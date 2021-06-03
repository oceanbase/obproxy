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

#ifndef  OCEANBASE_COMMON_KV_STORE_CACHE_H_
#define  OCEANBASE_COMMON_KV_STORE_CACHE_H_

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include <math.h>
#include <sys/time.h>
#include "lib/ob_define.h"
#include "lib/hash/ob_hashset.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/ob_lf_fifo_allocator.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/atomic/ob_atomic_reference.h"
#include "lib/container/ob_array.h"
#include "lib/objectpool/ob_obj_pool.h"
#include "lib/task/ob_timer.h"
#include "lib/lock/ob_bucket_lock.h"
#include "lib/lock/ob_drw_lock.h"
#include "lib/lock/ob_mutex.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/list/ob_list.h"
#include "lib/list/ob_dlist.h"
#include "common/ob_cost_consts_def.h"

namespace oceanbase
{
namespace common
{
static const int64_t MAX_CACHE_NUM             = 16;
static const int64_t MAX_TENANT_NUM_PER_SERVER = 1024;
static const int32_t MAX_CACHE_NAME_LENGTH     = 127;
static const double CACHE_SCORE_DECAY_FACTOR   = 0.9;

class ObIKVCacheKey
{
public:
  ObIKVCacheKey() {}
  virtual ~ObIKVCacheKey() {}
  virtual bool operator ==(const ObIKVCacheKey &other) const = 0;
  virtual uint64_t get_tenant_id() const = 0;
  virtual uint64_t hash() const = 0;
  virtual int64_t size() const = 0;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const = 0;
};

class ObIKVCacheValue
{
public:
  ObIKVCacheValue() {}
  virtual ~ObIKVCacheValue() {}
  virtual int64_t size() const = 0;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const = 0;
};

struct ObKVCachePair
{
  uint32_t magic_;
  int32_t size_;
  ObIKVCacheKey *key_;
  ObIKVCacheValue *value_;
  static const uint32_t KVPAIR_MAGIC_NUM = 0x4B564B56;  //"KVKV"
  ObKVCachePair()
      : magic_(KVPAIR_MAGIC_NUM), size_(0), key_(NULL), value_(NULL)
  {
  }
};

class ObKVCacheHandle;
class ObKVCacheIterator;

template <class Key, class Value>
class ObKVCache
{
public:
  ObKVCache();
  virtual ~ObKVCache();
  int init(const char *cache_name, const int64_t priority = 1);
  void destroy();
  int set_priority(const int64_t priority);
  int put(const Key &key, const Value &value, bool overwrite = true);
  int put_and_fetch(
    const Key &key,
    const Value &value,
    const Value *&pvalue,
    ObKVCacheHandle &handle,
    bool overwrite = true);
  int get(const Key &key, const Value *&pvalue, ObKVCacheHandle &handle);
  int get_iterator(ObKVCacheIterator &iter);
  int erase(const Key &key);
  int alloc(const Key &key, const Value &value, ObKVCachePair *&kvpair, ObKVCacheHandle &handle);
  int put(ObKVCachePair *kvpair, ObKVCacheHandle &handle, bool overwrite = true);
  int64_t size(const uint64_t tenant_id = OB_SYS_TENANT_ID) const;
  int64_t count(const uint64_t tenant_id = OB_SYS_TENANT_ID) const;
  int64_t get_hit_cnt(const uint64_t tenant_id = OB_SYS_TENANT_ID) const;
  int64_t get_miss_cnt(const uint64_t tenant_id = OB_SYS_TENANT_ID) const;
  double get_hit_rate(const uint64_t tenant_id = OB_SYS_TENANT_ID) const;
private:
  bool inited_;
  int64_t cache_id_;
};



////////////////////////////////////////////////////////////////////////////////////////////////////
enum ObKVCachePolicy
{
  LRU = 0,
  LFU = 1,
  MAX_POLICY = 2
};

struct ObKVCacheConfig
{
public:
  ObKVCacheConfig();
  void reset();
  bool is_valid_;
  int64_t priority_;
  char cache_name_[MAX_CACHE_NAME_LENGTH];
};

struct ObKVCacheStatus
{
public:
  ObKVCacheStatus();
  void refresh(const int64_t period_us);
  double get_hit_ratio() const;
  void reset();
  TO_STRING_KV(
      KP_(config),
      K_(kv_cnt),
      K_(store_size),
      K_(map_size),
      K_(lru_mb_cnt),
      K_(lfu_mb_cnt),
      K_(total_put_cnt),
      K_(total_miss_cnt),
      K_(total_hit_cnt),
      K_(base_mb_score));
  const ObKVCacheConfig *config_;
  int64_t kv_cnt_;
  int64_t store_size_;
  int64_t map_size_;
  int64_t lru_mb_cnt_;
  int64_t lfu_mb_cnt_;
  int64_t total_put_cnt_;
  int64_t total_miss_cnt_;
  int64_t total_hit_cnt_;
  int64_t last_hit_cnt_;
  double base_mb_score_;
};

class ObKVStoreMemBlock
{
public:
  ObKVStoreMemBlock(char *buffer, const int64_t size);
  virtual ~ObKVStoreMemBlock();
  static int64_t upper_align(int64_t input, int64_t align);
  static int64_t get_align_size(const ObIKVCacheKey &key, const ObIKVCacheValue &value);
  int store(const ObIKVCacheKey &key, const ObIKVCacheValue &value, ObKVCachePair *&kvpair);
  inline int64_t get_payload_size() const
  {
    return payload_size_;
  }
private:
  static const int64_t ALIGN_SIZE = sizeof(size_t);
  AtomicInt64 atomic_pos_;
  int64_t payload_size_;
  char *buffer_;
};

enum ObKVMBHandleStatus
{
  FREE = 0,
  USING = 1,
  FULL = 2
};

class ObKVCacheInst;
struct ObKVMemBlockHandle
{
  ObKVStoreMemBlock * volatile mem_block_;
  volatile enum ObKVMBHandleStatus status_;
  ObKVCacheInst *inst_;
  enum ObKVCachePolicy policy_;
  int64_t get_cnt_;
  int64_t recent_get_cnt_;
  double score_;
  int64_t kv_cnt_;
  ObAtomicReference handle_ref_;

  ObKVMemBlockHandle();
  virtual ~ObKVMemBlockHandle();
  void reset();
};

/*-----------------------------------------------------------------------*/
struct ObKVCacheInstKey
{
  ObKVCacheInstKey() : cache_id_(0), tenant_id_(0) {}
  ObKVCacheInstKey(const int64_t cache_id, const uint64_t tenant_id)
  : cache_id_(cache_id), tenant_id_(tenant_id) {}
  int64_t cache_id_;
  uint64_t tenant_id_;
  inline uint64_t hash() const { return cache_id_ + tenant_id_; }
  inline bool operator==(const ObKVCacheInstKey &other) const
  {
    return cache_id_ == other.cache_id_ && tenant_id_ == other.tenant_id_;
  }
  inline bool is_valid() const { return cache_id_ >= 0 && cache_id_ < MAX_CACHE_NUM
      && tenant_id_ < (uint64_t) OB_DEFAULT_TENANT_COUNT; }
  TO_STRING_KV(K_(cache_id), K_(tenant_id));
};

struct ObKVCacheInst
{
  int64_t cache_id_;
  uint64_t tenant_id_;
  ObKVMemBlockHandle *handles_[MAX_POLICY ];
  ObLfFIFOAllocator node_allocator_;
  ObKVCacheStatus status_;
  int64_t ref_cnt_;
  ObKVCacheInst()
    : cache_id_(0),
      tenant_id_(0),
      node_allocator_(),
      status_(),
      ref_cnt_(0) { MEMSET(handles_, 0, sizeof(handles_)); }
  bool can_destroy() {
    return 1 == ATOMIC_LOAD(&ref_cnt_)
        && 0 == ATOMIC_LOAD(&status_.kv_cnt_)
        && 0 == ATOMIC_LOAD(&status_.store_size_)
        && 0 == ATOMIC_LOAD(&status_.lru_mb_cnt_)
        && 0 == ATOMIC_LOAD(&status_.lfu_mb_cnt_);
  }
  void reset() {
    cache_id_ = 0;
    tenant_id_ = 0;
    node_allocator_.destroy();
    status_.reset();
    ref_cnt_ = 0;
    MEMSET(handles_, 0, sizeof(handles_));
  }
  bool is_valid() const { return ref_cnt_ > 0; }
};

class ObKVCacheInstMap;
class ObKVCacheInstHandle
{
public:
  ObKVCacheInstHandle();
  virtual ~ObKVCacheInstHandle();
  void reset();
  inline ObKVCacheInst *get_inst() { return inst_; }
private:
  friend class ObKVCacheInstMap;
  ObKVCacheInstMap *map_;
  ObKVCacheInst *inst_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObKVCacheInstHandle);
};


struct ObKVCacheInfo
{
  ObKVCacheInstKey inst_key_;
  ObKVCacheStatus status_;
  ObKVCacheInfo() : inst_key_(), status_() {}
  TO_STRING_KV(K_(inst_key), K_(status));
};

class ObKVCacheInstMap
{
public:
  ObKVCacheInstMap();
  virtual ~ObKVCacheInstMap();
  int init(const int64_t max_entry_cnt, const ObKVCacheConfig *configs);
  void destroy();
  int get_cache_inst(
      const ObKVCacheInstKey &inst_key,
      ObKVCacheInstHandle &inst_handle);
  int clean_garbage_inst();
  int refresh_score();
  int set_priority(const int64_t cache_id, const int64_t old_priority, const int64_t new_priority);
  int get_tenant_cache_info(const uint64_t tenant_id, ObIArray<ObKVCacheInfo> &infos);
  int get_all_cache_info(ObIArray<ObKVCacheInfo> &infos);
private:
  friend class ObKVCacheInstHandle;
  typedef hash::ObHashMap<ObKVCacheInstKey, ObKVCacheInst*, hash::NoPthreadDefendMode> KVCacheInstMap;
  void add_inst_ref(ObKVCacheInst *inst);
  void de_inst_ref(ObKVCacheInst *inst);
  DRWLock lock_;
  KVCacheInstMap  inst_map_;
  ObFixedQueue<ObKVCacheInst> inst_pool_;
  const ObKVCacheConfig *configs_;
  ObArenaAllocator allocator_;
  bool is_inited_;
};

class ObKVCacheStore
{
public:
  ObKVCacheStore();
  virtual ~ObKVCacheStore();
  int init(ObKVCacheInstMap &insts, const int64_t max_cache_size, const int64_t block_size);
  void destroy();
  int set_priority(const int64_t cache_id, const int64_t old_priority, const int64_t new_priority);
  int store(
    ObKVCacheInst &inst,
    const ObIKVCacheKey &key,
    const ObIKVCacheValue &value,
    ObKVCachePair *&kvpair,
    ObKVMemBlockHandle *&mb_handle,
    const enum ObKVCachePolicy policy = LRU);
  bool add_handle_ref(ObKVMemBlockHandle *mb_handle, const int32_t seq_num);
  bool add_handle_ref(ObKVMemBlockHandle *mb_handle);
  void de_handle_ref(ObKVMemBlockHandle *mb_handle);
  void refresh_score();
  void wash();
private:
  struct StoreMBHandleCmp
  {
    bool operator()(const ObKVMemBlockHandle *a, const ObKVMemBlockHandle *b) const;
  };
  struct WashHeap
  {
  public:
    WashHeap();
    virtual ~WashHeap();
    ObKVMemBlockHandle *add(ObKVMemBlockHandle *mb_handle);
    void reset();
    ObKVMemBlockHandle **heap_;
    int64_t heap_size_;
    int64_t mb_cnt_;
  };
  struct TenantWashInfo
  {
  public:
    TenantWashInfo();
    int64_t cache_size_;
    int64_t lower_limit_;
    int64_t upper_limit_;
    int64_t max_wash_size_;
    int64_t min_wash_size_;
    int64_t wash_size_;
    WashHeap wash_heap_;
  };
  typedef hash::ObHashMap<uint64_t, TenantWashInfo*, hash::NoPthreadDefendMode> WashMap;
private:
  ObKVMemBlockHandle *alloc_mbhandle(
    ObKVCacheInst &inst,
    const enum ObKVCachePolicy policy,
    const int64_t block_size);
  void compute_tenant_wash_size();
  void wash_mb(ObKVMemBlockHandle *mb_handle);
  void wash_mbs(WashHeap &heap);
  void init_wash_heap(WashHeap &heap, const int64_t heap_size);
private:
  bool inited_;
  ObKVCacheInstMap *insts_;
  //data structures for store
  int64_t max_mb_num_;
  int64_t block_size_;
  int64_t block_payload_size_;
  ObKVMemBlockHandle *mb_handles_;
  ObFixedQueue<ObKVMemBlockHandle> mb_handles_pool_;

  //data structures for wash
  lib::ObMutex wash_out_lock_;
  ObArenaAllocator wash_arena_;
  WashMap tenant_wash_map_;
  double tenant_reserve_mem_ratio_;
};

class ObKVCacheMap
{
public:
  ObKVCacheMap();
  virtual ~ObKVCacheMap();
  int init(const int64_t bucket_num, ObKVCacheStore *store);
  void destroy();
  int erase_all();
  int erase_all(const int64_t cache_id);
  int clean_garbage_node(int64_t &start_pos, const int64_t clean_num);
  int put(
    ObKVCacheInst &inst,
    const ObIKVCacheKey &key,
    const ObKVCachePair *kvpair,
    ObKVMemBlockHandle *mb_handle,
    bool overwrite = true);
  int get(
    ObKVCacheInst &inst,
    const ObIKVCacheKey &key,
    const ObIKVCacheValue *&pvalue,
    ObKVMemBlockHandle *&out_handle);
  int erase(ObKVCacheInst &inst, const ObIKVCacheKey &key);
private:
  struct Node
  {
    ObKVCacheInst *inst_;
    int32_t seq_num_;
    int64_t get_cnt_;
    ObKVMemBlockHandle *mb_handle_;
    const ObKVCachePair *kvpair_;
    uint64_t hash_code_;
    Node *next_;
    Node()
      : inst_(NULL),
        seq_num_(0),
        get_cnt_(0),
        mb_handle_(NULL),
        kvpair_(NULL),
        hash_code_(0),
        next_(NULL)
    {}
  };
private:
  friend class ObKVCacheIterator;
  int multi_get(const int64_t cache_id, const int64_t pos, common::ObList<Node> &list);
  void internal_map_erase(Node *&prev, Node *&iter, const uint64_t bucket_pos);
  void internal_map_replace(Node *&prev, Node *&iter, const uint64_t bucket_pos);
  void internal_data_move(Node *iter, const enum ObKVCachePolicy policy);
private:
  bool is_inited_;
  ObMalloc bucket_allocator_;
  int64_t bucket_num_;
  Node **buckets_;
  ObBucketLock bucket_lock_;
  ObKVCacheStore *store_;
};

class ObKVCacheHandle;
class ObKVGlobalCache
{
public:
  static ObKVGlobalCache &get_instance();
  int init(const int64_t bucket_num = DEFAULT_BUCKET_NUM,
      const int64_t max_cache_size = DEFAULT_MAX_CACHE_SIZE,
      const int64_t block_size = common::OB_MALLOC_BIG_BLOCK_SIZE);
  void destroy();
  void reload_priority();
  int get_tenant_cache_info(const uint64_t tenant_id, ObIArray<ObKVCacheInfo> &infos);
  int get_all_cache_info(ObIArray<ObKVCacheInfo> &infos);
private:
  template<class Key, class Value> friend class ObKVCache;
  friend class ObKVCacheHandle;
  ObKVGlobalCache();
  virtual ~ObKVGlobalCache();
  int register_cache(const char *cache_name, const int64_t priority, int64_t &cache_id);
  void deregister_cache(const int64_t cache_id);
  int set_priority(const int64_t cache_id, const int64_t priority);
  int put(
    const int64_t cache_id,
    const ObIKVCacheKey &key,
    const ObIKVCacheValue &value,
    const ObIKVCacheValue *&pvalue,
    ObKVMemBlockHandle *&mb_handle,
    bool overwrite = true);
  int get(
    const int64_t cache_id,
    const ObIKVCacheKey &key,
    const ObIKVCacheValue *&pvalue,
    ObKVMemBlockHandle *&mb_handle);
  int erase(const int64_t cache_id, const ObIKVCacheKey &key);
  void revert(ObKVMemBlockHandle *mb_handle);
  void wash();
private:
  static const int64_t DEFAULT_BUCKET_NUM = 10000000L;
  static const int64_t DEFAULT_MAX_CACHE_SIZE = 1024L * 1024L * 1024L * 1024L;  //1T
  static const int64_t MAP_ONCE_CLEAN_NUM = 500000;
  static const int64_t TIMER_SCHEDULE_INTERVAL_US = 1000 * 1000;
private:
  class KVStoreWashTask: public ObTimerTask
  {
  public:
    KVStoreWashTask()
    {
    }
    virtual ~KVStoreWashTask()
    {
    }
    void runTimerTask()
    {
      ObKVGlobalCache::get_instance().wash();
    }
  };
private:
  bool inited_;
  // map
  ObKVCacheMap map_;
  // store
  ObKVCacheStore store_;
  // cache instances
  ObKVCacheInstMap insts_;
  // cache configs
  ObKVCacheConfig configs_[MAX_CACHE_NUM];
  int64_t cache_num_;
  lib::ObMutex mutex_;
  // timer and task
  int64_t map_clean_pos_;
  KVStoreWashTask wash_task_;
  ObTimer wash_timer_;
};


class ObKVCacheHandle
{
public:
  ObKVCacheHandle();
  virtual ~ObKVCacheHandle();
  ObKVCacheHandle(const ObKVCacheHandle &other);
  ObKVCacheHandle &operator=(const ObKVCacheHandle &other);
  void reset();
  inline bool is_valid() const { return NULL != mb_handle_; }
  TO_STRING_KV(KP_(mb_handle));
private:
  template<class Key, class Value> friend class ObKVCache;
  friend class ObKVCacheIterator;
  ObKVMemBlockHandle *mb_handle_;
};

class ObKVCacheIterator
{
public:
  ObKVCacheIterator();
  virtual ~ObKVCacheIterator();
  int init(const int64_t cache_id, ObKVCacheMap *map);
  /**
   * get a kvpair from the kvcache, if return OB_SUCCESS, remember to call revert(handle)
   * to revert the handle.
   * @param key: out
   * @param value: out
   * @param handle: out
   * @return OB_SUCCESS or OB_ITER_END or other error code
   */
  template <class Key, class Value>
  int get_next_kvpair(const Key *&key, const Value *&value, ObKVCacheHandle &handle);
  void reset();
private:
  int64_t cache_id_;
  ObKVCacheMap *map_;
  int64_t pos_;
  common::ObList<ObKVCacheMap::Node> handle_list_;
  bool is_inited_;
};

//-------------------------------------------------------Template Methods----------------------------------------------------------
/*
 * ------------------------------------------------------------ObKVCache-----------------------------------------------------------------
 */
template <class Key, class Value>
ObKVCache<Key, Value>::ObKVCache()
    : inited_(false), cache_id_(-1)
{
}

template <class Key, class Value>
ObKVCache<Key, Value>::~ObKVCache()
{
  destroy();
}

template <class Key, class Value>
int ObKVCache<Key, Value>::init(const char *cache_name, const int64_t priority)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "The ObKVCache has been inited, ", K(ret));
  } else if (OB_UNLIKELY(NULL == cache_name)
      || OB_UNLIKELY(priority <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", KP(cache_name), K(priority), K(ret));
  } else if (OB_FAIL(ObKVGlobalCache::get_instance().register_cache(cache_name, priority, cache_id_))) {
    COMMON_LOG(WARN, "Fail to register cache, ", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

template <class Key, class Value>
void ObKVCache<Key, Value>::destroy()
{
  if (OB_LIKELY(inited_)) {
    ObKVGlobalCache::get_instance().deregister_cache(cache_id_);
    inited_ = false;
  }
}

template <class Key, class Value>
int ObKVCache<Key, Value>::set_priority(const int64_t priority)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCache has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(priority <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", K(priority), K(ret));
  } else if (OB_FAIL(ObKVGlobalCache::get_instance().set_priority(cache_id_, priority))) {
    COMMON_LOG(WARN, "Fail to set priority, ", K(ret));
  }
  return ret;
}

template <class Key, class Value>
int64_t ObKVCache<Key, Value>::size(const uint64_t tenant_id) const
{
  int64_t size = 0;
  if (OB_LIKELY(inited_)) {
    int ret = OB_SUCCESS;
    ObKVCacheInstKey inst_key(cache_id_, tenant_id);
    ObKVCacheInstHandle inst_handle;
    if (OB_SUCC(ObKVGlobalCache::get_instance().insts_.get_cache_inst(inst_key, inst_handle))) {
      if (NULL != inst_handle.get_inst()) {
        size += inst_handle.get_inst()->status_.store_size_;
        size += inst_handle.get_inst()->node_allocator_.allocated();
      }
    }
  }
  return size;
}

template <class Key, class Value>
int64_t ObKVCache<Key, Value>::count(const uint64_t tenant_id) const
{
  int64_t count = 0;
  if (OB_LIKELY(inited_)) {
    int ret = OB_SUCCESS;
    ObKVCacheInstKey inst_key(cache_id_, tenant_id);
    ObKVCacheInstHandle inst_handle;
    if (OB_SUCC(ObKVGlobalCache::get_instance().insts_.get_cache_inst(inst_key, inst_handle))) {
      if (NULL != inst_handle.get_inst()) {
        count = inst_handle.get_inst()->status_.kv_cnt_;
      }
    }
  }
  return count;
}

template <class Key, class Value>
int64_t ObKVCache<Key, Value>::get_hit_cnt(const uint64_t tenant_id) const
{
  int64_t hit_cnt = 0;
  if (OB_LIKELY(inited_)) {
    int ret = OB_SUCCESS;
    ObKVCacheInstKey inst_key(cache_id_, tenant_id);
    ObKVCacheInstHandle inst_handle;
    if (OB_SUCC(ObKVGlobalCache::get_instance().insts_.get_cache_inst(inst_key, inst_handle))) {
      if (NULL != inst_handle.get_inst()) {
        hit_cnt = inst_handle.get_inst()->status_.total_hit_cnt_;
      }
    }
  }
  return hit_cnt;
}

template <class Key, class Value>
int64_t ObKVCache<Key, Value>::get_miss_cnt(const uint64_t tenant_id) const
{
  int64_t miss_cnt = 0;
  if (OB_LIKELY(inited_)) {
    int ret = OB_SUCCESS;
    ObKVCacheInstKey inst_key(cache_id_, tenant_id);
    ObKVCacheInstHandle inst_handle;
    if (OB_SUCC(ObKVGlobalCache::get_instance().insts_.get_cache_inst(inst_key, inst_handle))) {
      if (NULL != inst_handle.get_inst()) {
        miss_cnt = inst_handle.get_inst()->status_.total_miss_cnt_;
      }
    }
  }
  return miss_cnt;
}

template <class Key, class Value>
double ObKVCache<Key, Value>::get_hit_rate(const uint64_t tenant_id) const
{
  UNUSED(tenant_id);
  return DEFAULT_CACHE_HIT_RATE;
}

template <class Key, class Value>
int ObKVCache<Key, Value>::get_iterator(ObKVCacheIterator &iter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCache has not been inited, ", K(ret));
  } else if (OB_FAIL(iter.init(cache_id_, &ObKVGlobalCache::get_instance().map_))) {
    COMMON_LOG(WARN, "Fail to init ObKVCacheIterator, ", K(ret));
  }
  return ret;
}

template <class Key, class Value>
int ObKVCache<Key, Value>::put(const Key &key, const Value &value, bool overwrite)
{
  int ret = OB_SUCCESS;
  ObKVCacheHandle handle;
  const ObIKVCacheValue *pvalue = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCache has not been inited, ", K(ret));
  } else if (OB_FAIL(ObKVGlobalCache::get_instance().put(cache_id_, key, value, pvalue,
      handle.mb_handle_, overwrite))) {
    if (OB_ENTRY_EXIST != ret) {
      COMMON_LOG(WARN, "Fail to put kv to ObKVGlobalCache, ", K_(cache_id), K(ret));
    }
  } else {
    handle.reset();
  }
  return ret;
}


template <class Key, class Value>
int ObKVCache<Key, Value>::put_and_fetch(
    const Key &key,
    const Value &value,
    const Value *&pvalue,
    ObKVCacheHandle &handle,
    bool overwrite)
{
  int ret = OB_SUCCESS;
  const ObIKVCacheValue *tmp = NULL;
  handle.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCache has not been inited, ", K(ret));
  } else if (OB_FAIL(ObKVGlobalCache::get_instance().put(cache_id_, key, value,
      tmp, handle.mb_handle_, overwrite))) {
    COMMON_LOG(WARN, "Fail to put kv to ObKVGlobalCache, ", K_(cache_id), K(ret));
  } else {
    pvalue = reinterpret_cast<const Value*>(tmp);
  }
  return ret;
}

template <class Key, class Value>
int ObKVCache<Key, Value>::get(const Key &key, const Value *&pvalue, ObKVCacheHandle &handle)
{
  int ret = OB_SUCCESS;
  const ObIKVCacheValue *value = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCache has not been inited, ", K(ret));
  } else {
    handle.reset();
    if (OB_FAIL(ObKVGlobalCache::get_instance().get(cache_id_, key, value, handle.mb_handle_))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        COMMON_LOG(WARN, "Fail to get value from ObKVGlobalCache, ", K(ret));
      }
    } else {
      pvalue = reinterpret_cast<const Value*> (value);
    }
  }
  return ret;
}

template <class Key, class Value>
int ObKVCache<Key, Value>::erase(const Key &key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCache has not been inited, ", K(ret));
  } else if (OB_FAIL(ObKVGlobalCache::get_instance().erase(cache_id_, key))) {
    COMMON_LOG(WARN, "Fail to erase key from ObKVGlobalCache, ", K_(cache_id), K(ret));
  }
  return ret;
}

template <class Key, class Value>
int ObKVCache<Key, Value>::alloc(const Key &key, const Value &value,
    ObKVCachePair *&kvpair, ObKVCacheHandle &handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCache has not been inited, ", K(ret));
  } else {
    ObKVCachePair *tmp_pair = NULL;
    ObKVMemBlockHandle *mb_handle = NULL;
    ObKVCacheInstKey inst_key(cache_id_, key.get_tenant_id());
    ObKVCacheInstHandle inst_handle;

    if (OB_FAIL(ObKVGlobalCache::get_instance().insts_.get_cache_inst(inst_key, inst_handle))) {
      COMMON_LOG(WARN, "Fail to get cache inst, ", K(ret));
    } else if (NULL == inst_handle.get_inst()) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "The inst is NULL, ", K(ret));
    } else if (OB_FAIL(ObKVGlobalCache::get_instance().store_.store(*inst_handle.get_inst(),
        key, value, tmp_pair, mb_handle))) {
      COMMON_LOG(WARN, "Fail to store kvpair, ", K(ret));
    } else {
      handle.reset();
      handle.mb_handle_ = mb_handle;
      kvpair = tmp_pair;
    }
  }
  return ret;
}

template <class Key, class Value>
int ObKVCache<Key, Value>::put(ObKVCachePair *kvpair, ObKVCacheHandle &handle, bool overwrite)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCache has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(NULL == kvpair)
      || OB_UNLIKELY(NULL == kvpair->key_)
      || OB_UNLIKELY(NULL == kvpair->value_)
      || OB_UNLIKELY(!handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", KP(kvpair), K(handle), K(ret));
  } else {
    ObKVCacheInstKey inst_key(cache_id_, kvpair->key_->get_tenant_id());
    ObKVCacheInstHandle inst_handle;
    if (OB_FAIL(ObKVGlobalCache::get_instance().insts_.get_cache_inst(inst_key, inst_handle))) {
      COMMON_LOG(WARN, "Fail to get inst, ", K(ret));
    } else if (NULL == inst_handle.get_inst()) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "The inst is NULL, ", K(ret));
    } else if (OB_FAIL(ObKVGlobalCache::get_instance().map_.put(*inst_handle.get_inst(),
        *kvpair->key_, kvpair, handle.mb_handle_, overwrite))) {
      if (OB_ENTRY_EXIST != ret) {
        COMMON_LOG(WARN, "Fail to put kvpair to map, ", K(ret));
      }
    }
  }
  return ret;
}

/*
 * ----------------------------------------------------ObKVCacheIterator---------------------------------------------
 */
template <class Key, class Value>
int ObKVCacheIterator::get_next_kvpair(
    const Key *&key,
    const Value *&value,
    ObKVCacheHandle &handle)
{
  int ret = OB_SUCCESS;
  ObKVCacheMap::Node node;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The ObKVCacheIterator has not been inited, ", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (pos_ >= map_->bucket_num_ && handle_list_.empty()) {
        ret = OB_ITER_END;
      } else if (OB_SUCC(handle_list_.pop_front(node))) {
        if (map_->store_->add_handle_ref(node.mb_handle_, node.seq_num_)) {
          break;
        }
      } else {
        if (common::OB_ENTRY_NOT_EXIST == ret) {
          if (pos_ >= map_->bucket_num_) {
            ret = OB_ITER_END;
          } else if (OB_FAIL(map_->multi_get(cache_id_, pos_++, handle_list_))) {
            COMMON_LOG(WARN, "Fail to multi get from map, ", K(ret));
          }
        } else {
          COMMON_LOG(WARN, "Unexpected error, ", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    handle.reset();
    key = reinterpret_cast<const Key*>(node.kvpair_->key_);
    value = reinterpret_cast<const Value*>(node.kvpair_->value_);
    handle.mb_handle_ = node.mb_handle_;
  }
  return ret;
}

}
}

#endif //OCEANBASE_COMMON_KV_STORE_CACHE_H_
