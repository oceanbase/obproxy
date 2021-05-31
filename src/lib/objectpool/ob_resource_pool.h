/**
 * Copyright (c) 2021 OceanBase
 * OceanBase Database Proxy(ODP) is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef  OCEANBASE_UPDATESERVER_RESOURCE_POOL_H_
#define  OCEANBASE_UPDATESERVER_RESOURCE_POOL_H_
#include <pthread.h>
#include <typeinfo>
#include "lib/ob_define.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_id_map.h"
#include "lib/utility/utility.h"
#include "lib/thread_local/ob_tsi_factory.h"
#include "lib/profile/ob_resource_profile.h"

DEFINE_HAS_MEMBER(RP_LOCAL_NUM);
DEFINE_HAS_MEMBER(RP_TOTAL_NUM);
DEFINE_HAS_MEMBER(RP_RESERVE_NUM);

namespace oceanbase
{
namespace common
{
template <class T,
          int64_t MOD_ID = common::ObModIds::OB_UPS_RESOURCE_POOL_NODE,
          int64_t LOCAL_NUM = 4,
          int64_t TOTAL_NUM = 256,
          int64_t RESERVE_NUM = 64>
class ObResourcePool
{
  static const int64_t ALLOCATOR_PAGE_SIZE = OB_MALLOC_BIG_BLOCK_SIZE - 1024;
  static const int64_t MAX_THREAD_NUM = 1024;
  static const int64_t WARN_INTERVAL = 60000000L; //60s
  static const uint64_t ALLOC_MAGIC_NUM = 0x72737263706f6f6c; // rsrcpool
  static const uint64_t ALLOC_BY_PAGEARENA = 0x0;
  static const uint64_t ALLOC_BY_OBMALLOC = 0x1;
  struct Node
  {
    T data;
    int64_t mod_id;
    union
    {
      Node *next;
      uint64_t magic;
    };
    uint64_t flag;
    Node() : data(), mod_id(MOD_ID), next(NULL), flag(ALLOC_BY_PAGEARENA) {};
  };
  struct NodeArray
  {
    Node datas[LOCAL_NUM];
    int64_t index;
    Node *free_list_;
    NodeArray() : index(0), free_list_(NULL) {};
  };
  typedef common::ObFixedQueue<Node> NodeQueue;
  //typedef ObTCIDFreeList<Node,4> NodeQueue;
  typedef common::ObFixedQueue<NodeArray> ArrayQueue;
public:
  class Guard
  {
  public:
    explicit Guard(ObResourcePool &host)
        : host_(host),
          local_list_(NULL),
          list_(NULL)
    {
    };
    ~Guard()
    {
      reset();
    };
  public:
    void reset()
    {
      Node *iter = list_;
      while (NULL != iter) {
        Node *tmp = iter;
        iter = iter->next;
        host_.free_node_(tmp);
      }
      list_ = NULL;
      NodeArray *array = host_.get_node_array_();
      if (NULL != array) {
        iter = local_list_;
        while (NULL != iter) {
          iter->data.reset();
          iter->next = array->free_list_;
          array->free_list_ = iter;
        }
      }
      local_list_ = NULL;
    };
  public:
    void add_node(Node *node)
    {
      if (NULL != node) {
        node->next = list_;
        list_ = node;
      }
    };
    void add_local_node(Node *node)
    {
      if (NULL != node) {
        node->next = local_list_;
        local_list_ = node;
      }
    };
  private:
    ObResourcePool &host_;
    Node *local_list_;
    Node *list_;
  };
public:
  ObResourcePool() : mod_(MOD_ID),
                     allocator_(ALLOCATOR_PAGE_SIZE, mod_),
                     allocated_num_(0),
                     thread_key_(OB_INVALID_PTHREAD_KEY)
  {
    int ret = pthread_key_create(&thread_key_, NULL);
    if (0 != ret) {
      _OB_LOG(ERROR, "pthread_key_create fail ret=%d", ret);
    }
    array_list_.init(MAX_THREAD_NUM);
    free_list_.init(TOTAL_NUM);
    _OB_LOG(DEBUG,
              "RP init this=%p type=%s local_num=%ld total_num=%ld reserve_num=%ld allocator=%p free_list=%p bt=%s",
              this, typeid(T).name(), LOCAL_NUM, TOTAL_NUM, RESERVE_NUM, &allocator_, &free_list_, lbt());
  };
  ~ObResourcePool()
  {
    Node *node = NULL;
    while (common::OB_SUCCESS == free_list_.pop(node)) {
      node->~Node();
      //allocator_.free((char*)node);
    }
    NodeArray *array = NULL;
    while (common::OB_SUCCESS == array_list_.pop(array)) {
      free_array_(array);
    }
    (void)pthread_key_delete(thread_key_);
  };
public:
  T *get(Guard &guard)
  {
    T *data = NULL;
    NodeArray *array = get_node_array_();
    if (NULL != array) {
      if (array->index < LOCAL_NUM) {
        data = &(array->datas[array->index].data);
        array->index += 1;
      } else if (NULL != array->free_list_) {
        Node *node = array->free_list_;
        array->free_list_ = node->next;
        node->next = NULL;
        guard.add_local_node(node);
      }
    }
    if (NULL == data) {
      Node *node = alloc_node_();
      if (NULL != node) {
        data = &(node->data);
        guard.add_node(node);
      }
    }
    return data;
  };
  T *alloc()
  {
    T *ret = NULL;
    Node *node = alloc_node_();
    if (NULL != node) {
      ret = &(node->data);
      node->magic = ALLOC_MAGIC_NUM;
    }
    if (NULL != ret) {
      //RESP_ON_ALLOC(OB_RES::OB_RESOURCE_POOL, ret);
    }
    return ret;
  };
  void free(T *ptr)
  {
    if (NULL != ptr) {
      Node *node = (Node *)ptr;
      if (ALLOC_MAGIC_NUM != node->magic || node->mod_id != MOD_ID) {
        _OB_LOG(ERROR, "node=%p magic=%lx not match %lx mod_id=%ld expected_mod_id=%ld", node, node->magic, ALLOC_MAGIC_NUM, node->mod_id, MOD_ID);
      } else {
        //RESP_ON_REVERT(OB_RES::OB_RESOURCE_POOL, ptr);
        free_node_(node);
      }
    }
  };
  int64_t get_free_num()
  {
    return free_list_.get_total();
  }
private:
  Node *alloc_node_()
  {
    Node *ret = NULL;
    const int64_t free_num = get_free_num();
    if (RESERVE_NUM >= free_num
        || common::OB_SUCCESS != free_list_.pop(ret)
        || NULL == ret) {
      void *buffer = NULL;
      uint64_t flag = ALLOC_BY_PAGEARENA;
      int64_t allocated_num = ATOMIC_AAF(&allocated_num_, 1);
      if (TOTAL_NUM >= allocated_num) {
        flag = ALLOC_BY_PAGEARENA;
        common::ObSpinLockGuard guard(allocator_lock_);
        buffer = allocator_.alloc(sizeof(Node));
      } else {
        flag = ALLOC_BY_OBMALLOC;
        ObMemAttr memattr(OB_SERVER_TENANT_ID, MOD_ID);
        buffer = common::ob_malloc(sizeof(Node), memattr);
      }
      if (NULL != buffer) {
        ret = new(buffer) Node();
        ret->flag = flag;
      } else {
        (void)ATOMIC_AAF(&allocated_num_, -1);
        (void)free_list_.pop(ret);
      }
    }
    return ret;
  };
  void free_node_(Node *ptr)
  {
    if (NULL != ptr) {
      ptr->data.reset();
      ptr->next = NULL;
      if (ALLOC_BY_PAGEARENA == ptr->flag) {
        if (common::OB_SUCCESS != free_list_.push(ptr)) {
          _OB_LOG(ERROR, "free node to list fail, size=%ld ptr=%p", free_list_.get_total(), ptr);
        }
      } else if (ALLOC_BY_OBMALLOC == ptr->flag) {
        ptr->~Node();
        common::ob_free(ptr);
        (void)ATOMIC_AAF(&allocated_num_, -1);
      } else {
        _OB_LOG(ERROR, "invalid flag=%lu", ptr->flag);
      }
      //int64_t cnt = 0;
      //int64_t last_warn_time = 0;
      //while (common::OB_SUCCESS != free_list_.push(ptr))
      //{
      //  Node *node = NULL;
      //  if (common::OB_SUCCESS == free_list_.pop(node)
      //      && NULL != node)
      //  {
      //    node->~Node();
      //    common::ob_free(node);
      //  }
      //  if (TOTAL_NUM < cnt++
      //      && ::oceanbase::common::ObTimeUtility::current_time() > (WARN_INTERVAL + last_warn_time))
      //  {
      //    last_warn_time = ::oceanbase::common::ObTimeUtility::current_time();
      //    _OB_LOG(ERROR, "free node to list fail count to large %ld, free_list_size=%ld", cnt, free_list_.get_free());
      //  }
      //}
    }
  };

  NodeArray *alloc_array_()
  {
    NodeArray *ret = NULL;
    ObMemAttr memattr(OB_SERVER_TENANT_ID, common::ObModIds::OB_UPS_RESOURCE_POOL_ARRAY);
    void *buffer = common::ob_malloc(sizeof(NodeArray), memattr);
    if (NULL != buffer) {
      ret = new(buffer) NodeArray();
    }
    return ret;
  };

  void free_array_(NodeArray *ptr)
  {
    if (NULL != ptr) {
      ptr->~NodeArray();
      common::ob_free(ptr);
    }
  };

  NodeArray *get_node_array_()
  {
    NodeArray *ret = (NodeArray *)pthread_getspecific(thread_key_);
    if (NULL == ret) {
      if (NULL != (ret = alloc_array_())) {
        if (common::OB_SUCCESS != array_list_.push(ret)) {
          free_array_(ret);
          ret = NULL;
        } else if (0 != pthread_setspecific(thread_key_, ret)) {
          OB_LOG(ERROR, "failed to pthread_setspecific");
        } else {
          _OB_LOG(DEBUG, "alloc thread specific node_array=%p", ret);
        }
      }
    }
    return ret;
  };
private:
  common::ObSpinLock allocator_lock_;
  common::ModulePageAllocator mod_;
  common::ModuleArena allocator_;
  volatile int64_t allocated_num_;
  pthread_key_t thread_key_;
  ArrayQueue array_list_;
  NodeQueue free_list_;
};

template <class T, int64_t MOD_ID, int64_t LOCAL_NUM, int64_t TOTAL_NUM, int64_t RESERVE_NUM>
ObResourcePool<T, MOD_ID, LOCAL_NUM, TOTAL_NUM, RESERVE_NUM> &get_resource_pool()
{
  static ObResourcePool<T, MOD_ID, LOCAL_NUM, TOTAL_NUM, RESERVE_NUM> resource_pool;
  static __thread bool once = false;
  if (!once) {
    _OB_LOG(DEBUG, "get_resource_pool ptr=%p", &resource_pool);
    once = true;
  }
  return resource_pool;
}

template <class T, int64_t MOD_ID, int64_t LOCAL_NUM, int64_t TOTAL_NUM, int64_t RESERVE_NUM>
ObResourcePool<T, MOD_ID, LOCAL_NUM, TOTAL_NUM, RESERVE_NUM> &get_tc_resource_pool()
{
  typedef ObResourcePool<T, MOD_ID, LOCAL_NUM, TOTAL_NUM, RESERVE_NUM> RP;
  RP *resource_pool = GET_TSI_MULT(RP, 1);
  static __thread bool once = false;
  if (!once) {
    _OB_LOG(DEBUG, "get_tc_resource_pool ptr=%p", resource_pool);
    once = true;
  }
  return *resource_pool;
}

template <class T>
struct RP
{
  template <class Type, bool Cond = true>
  struct GetRPLocalNum
  {
    static const int64_t v = Type::RP_LOCAL_NUM;
  };

  template <class Type>
  struct GetRPLocalNum<Type, false>
  {
    static const int64_t v = 1;
  };

  template <class Type, bool Cond = true>
  struct GetRPTotalNum
  {
    static const int64_t v = Type::RP_TOTAL_NUM;
  };

  template <class Type>
  struct GetRPTotalNum<Type, false>
  {
    static const int64_t v = 256;
  };

  template <class Type, bool Cond = true>
  struct GetRPReserveNum
  {
    static const int64_t v = Type::RP_RESERVE_NUM;
  };

  template <class Type>
  struct GetRPReserveNum<Type, false>
  {
    static const int64_t v = 1;
  };

  static const int64_t LOCAL_NUM = GetRPLocalNum<T, HAS_MEMBER(T, RP_LOCAL_NUM)>::v;
  static const int64_t TOTAL_NUM = GetRPTotalNum<T, HAS_MEMBER(T, RP_TOTAL_NUM)>::v;
  static const int64_t RESERVE_NUM = GetRPReserveNum<T, HAS_MEMBER(T, RP_RESERVE_NUM)>::v;
};


#define rp_alloc(type, mod_id) common::get_resource_pool<type, mod_id, common::RP<type>::LOCAL_NUM, common::RP<type>::TOTAL_NUM, common::RP<type>::RESERVE_NUM>().alloc()
#define rp_free(ptr, mod_id) common::get_resource_pool<__typeof__(*ptr), mod_id, common::RP<__typeof__(*ptr)>::LOCAL_NUM, common::RP<__typeof__(*ptr)>::TOTAL_NUM, common::RP<__typeof__(*ptr)>::RESERVE_NUM>().free(ptr)

#define tc_rp_alloc(type) common::get_tc_resource_pool<type, common::RP<type>::LOCAL_NUM, common::RP<type>::TOTAL_NUM, common::RP<type>::RESERVE_NUM>().alloc()
#define tc_rp_free(ptr) common::get_tc_resource_pool<__typeof__(*ptr), common::RP<__typeof__(*ptr)>::LOCAL_NUM, common::RP<__typeof__(*ptr)>::TOTAL_NUM, common::RP<__typeof__(*ptr)>::RESERVE_NUM>().free(ptr)
}
}

#endif //OCEANBASE_UPDATESERVER_RESOURCE_POOL_H_
