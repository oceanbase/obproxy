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

#ifndef OCEANBASE_HASH_OB_LINK_HASHMAP_H_
#define OCEANBASE_HASH_OB_LINK_HASHMAP_H_

#include <lib/allocator/ob_mod_define.h>
#include <lib/allocator/ob_malloc.h>
#include <lib/hash/ob_dchash.h>
#include "lib/lock/ob_tc_ref.h"
#include "lib/allocator/ob_retire_station.h"
#include "lib/objectpool/ob_concurrency_objpool.h"

namespace oceanbase
{
namespace common
{
class DCArrayAlloc: public IAlloc
{
public:
  DCArrayAlloc(): mod_id_(ObModIds::OB_CONCURRENT_HASH_MAP), tenant_id_(OB_SERVER_TENANT_ID) {}
  virtual ~DCArrayAlloc() {}
  int init(const int64_t mod_id, const uint64_t tenant_id)
  {
    int ret = OB_SUCCESS;
    if (!is_valid_tenant_id(tenant_id)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid argument", K(ret), K(mod_id), K(tenant_id));
    } else {
      mod_id_ = mod_id;
      tenant_id_ = tenant_id;
    }
    return ret;
  }

  void *alloc(const int64_t sz)
  {
    void *ptr = NULL;
    if (sz <= 0) {
      COMMON_LOG(WARN, "invalid argument", K(sz));
    } else {
      ObMemAttr attr(tenant_id_, mod_id_);
      ptr = ob_malloc(sz, attr);
    }
    return ptr;
  }

  void free(void *p)
  {
    if (p != NULL) {
      ob_free(p);
      p = NULL;
    }
  }
private:
  DISALLOW_COPY_AND_ASSIGN(DCArrayAlloc);
private:
  int64_t mod_id_;
  uint64_t tenant_id_;
};

inline TCRef& get_global_hash_tcref()
{
  static TCRef global_hash_tc_ref;
  return global_hash_tc_ref;
}

struct RefNode
{
  RefNode(): retire_link_(), uref_(0), href_(0) {}
  ~RefNode() {}
  int32_t xhref(int32_t x) { return ATOMIC_AAF(&href_, x); }
  ObLink retire_link_;
  int32_t uref_;
  int32_t href_;
private:
  DISALLOW_COPY_AND_ASSIGN(RefNode);
};

template<typename key_t>
struct LinkHashNode: RefNode
{
  typedef KeyHashNode<key_t> KeyNode;
  LinkHashNode(): RefNode(), hash_link_(), hash_val_(NULL), hash_node_(NULL) {}
  ~LinkHashNode() {}
  int32_t get_uref() const { return hash_node_->uref_; }
  int32_t get_href() const { return hash_node_->href_; }
  KeyNode hash_link_;
  void* hash_val_;
  LinkHashNode* hash_node_;
private:
  DISALLOW_COPY_AND_ASSIGN(LinkHashNode);
};

template<typename Key, typename Value>
class AllocHandle
{
public:
  typedef LinkHashNode<Key> Node;
  static Value* alloc_value() { return op_alloc(Value); }
  static void free_value(Value* val) { op_free(val); val = NULL; }
  static Node* alloc_node(Value* val) { UNUSED(val); return op_alloc(Node); }
  static void free_node(Node* node) { op_free(node); node = NULL; }
};

template<typename Key, typename Value>
class EmbedAllocHandle
{
public:
  typedef LinkHashNode<Key> Node;
  static Value* alloc_value() { return op_alloc(Value); }
  static void free_value(Value* value) {
    if (NULL != value) {
      value->destroy();
    }
  }
  static Node* alloc_node(Value* value) { return static_cast<Node*>(value); }
  static void free_node(Node* node)
  {
    if (NULL != node) {
      Value* value = node->hash_val_;
      op_free(value);
      value = NULL;
    }
  }
};

class CachedRefHandle
{
public:
  typedef RefNode Node;
  CachedRefHandle(RetireStation& retire_station): qclock_(get_global_qclock()), retire_station_(retire_station), tcref_(get_global_hash_tcref()) {}
  ~CachedRefHandle() {}
  void enter_critical() { qclock_.enter_critical(); }
  void leave_critical() { qclock_.leave_critical(); }
  static bool get_delay_end_uref() { return true; }
  void retire(Node* node, HazardList& reclaim_list)
  {
    HazardList retire_list;
    int64_t retire_limit = 1024;
    retire_list.push(&node->retire_link_);
    retire_station_.retire(reclaim_list, retire_list, retire_limit);
  }
  void sync() { tcref_.sync(); }
  void purge(HazardList& reclaim_list) { retire_station_.purge(reclaim_list); }
  void born(Node* node) { tcref_.born(&node->uref_); }
  int32_t end(Node* node) { return tcref_.end(&node->uref_); }
  bool inc(Node* node) { tcref_.inc_ref(&node->uref_); return true; }
  int32_t dec(Node* node) { return tcref_.dec_ref(&node->uref_); }
private:
  QClock& qclock_;
  RetireStation& retire_station_;
  TCRef& tcref_;
};

class RefHandle
{
public:
  typedef RefNode Node;
  explicit RefHandle(RetireStation& retire_station): qclock_(get_global_qclock()), retire_station_(retire_station) {}
  ~RefHandle() {}
  void enter_critical() { qclock_.enter_critical(); }
  void leave_critical() { qclock_.leave_critical(); }
  static bool get_delay_end_uref() { return false; }
  void retire(Node* node, HazardList& reclaim_list)
  {
    HazardList retire_list;
    int64_t retire_limit = 1024;
    retire_list.push(&node->retire_link_);
    retire_station_.retire(reclaim_list, retire_list, retire_limit);
  }
  void sync() {}
  void purge(HazardList& reclaim_list) { retire_station_.purge(reclaim_list); }
  void born(Node* node) { (void)ATOMIC_AAF(&node->uref_, INT32_MAX/2); }
  int32_t end(Node* node) { return ATOMIC_AAF(&node->uref_, -INT32_MAX/2); }
  bool inc(Node* node) { return ATOMIC_AAF(&node->uref_, 1) != 1; }
  int32_t dec(Node* node) { return ATOMIC_AAF(&node->uref_, -1); }
private:
  QClock& qclock_;
  RetireStation& retire_station_;
};

class DummyRefHandle
{
public:
  typedef RefNode Node;
  explicit DummyRefHandle(RetireStation& retire_station){ UNUSED(retire_station); }
  virtual ~DummyRefHandle() {}
  void enter_critical() {}
  void leave_critical() {}
  static bool get_delay_end_uref() { return false; }
  void sync() {}
  void retire(Node* node, HazardList& reclaim_list)
  {
    reclaim_list.push(&node->retire_link_);
  }
  void purge(HazardList& reclaim_list) { UNUSED(reclaim_list); }
  void born(Node* node) { UNUSED(node); }
  int32_t end(Node* node) { UNUSED(node); return 0; }
  bool inc(Node* node) { UNUSED(node); return true; }
  int32_t dec(Node* node) { UNUSED(node); return 1; }
};

class CountHandle
{
public:
  CountHandle(): count_(0) {}
  ~CountHandle() {}
  int64_t size() const { return ATOMIC_LOAD(&count_); }
  int64_t add(int64_t x) { return ATOMIC_FAA(&count_, x); }
private:
  int64_t count_;
};

class DummyCountHandle
{
public:
  DummyCountHandle() {}
  ~DummyCountHandle() {}
  int64_t size() const { return 1; }
  int64_t add(int64_t x) { UNUSED(x); return 1; }
};

template<typename Key, typename Value, typename AllocHandle=AllocHandle<Key, Value>, typename RefHandle=RefHandle>
class ObLinkHashMap
{
private:
  typedef DCArrayAlloc ArrayAlloc;
  typedef DCHash<Key>      Hash;
  typedef typename Hash::Node Node;
  typedef LinkHashNode<Key> HashNode;
  struct Guard
  {
    explicit Guard(RefHandle& ref_handle): ref_handle_(ref_handle) { ref_handle_.enter_critical(); }
    ~Guard() { ref_handle_.leave_critical(); }
    RefHandle& ref_handle_;
  };
  class Iterator
  {
  public:
    explicit Iterator(ObLinkHashMap& hash): hash_(hash), next_(hash_.next(NULL)) {}
    ~Iterator() { destroy(); }
    void destroy() {
      if (NULL != next_) {
        hash_.revert(next_);
        next_ = NULL;
      }
    }
    Value* next() {
      Value* ret = next_;
      if (NULL != next_) {
        next_ = hash_.next(next_);
      }
      return ret;
    }
  private:
    ObLinkHashMap& hash_;
    Value* next_;
  };
public:
  explicit ObLinkHashMap(int64_t min_size = 1<<16): ref_handle_(get_retire_station()), hash_(array_alloc_, min_size)
  {}
  ~ObLinkHashMap()
  {
    destroy();
  }
  int init(const int mod_id = ObModIds::OB_CONCURRENT_HASH_MAP,
      const uint64_t tenant_id = OB_SERVER_TENANT_ID)
  {
    int ret = OB_SUCCESS;
    if (!is_valid_tenant_id(tenant_id)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid argument", K(ret), K(mod_id), K(tenant_id));
    } else if (OB_FAIL(array_alloc_.init(mod_id, tenant_id))) {
      COMMON_LOG(ERROR, "array_alloc_ init error", K(ret), K(mod_id), K(tenant_id));
    }
    return ret;
  }

  void reset()
  {
    (void)remove_if(always_true);
  }

  void destroy()
  {
    reset();
    purge();
  }

  void purge()
  {
    HazardList reclaim_list;
    ref_handle_.purge(reclaim_list);
    reclaim_nodes(reclaim_list);
  }
  int64_t count() const { return hash_.count(); }
  Value* next(Value* value)
  {
    HashNode* node = NULL == value? NULL: value->hash_node_;
    Guard guard(ref_handle_);
    node = next_(node);
    return (Value*)(NULL == node? NULL: node->hash_val_);
  }
  int64_t size() const { return count_handle_.size(); }
  int create(const Key &key, Value *&value)
  {
    int ret = OB_SUCCESS;
    if (NULL == (value = alloc_handle_.alloc_value())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_SUCCESS != (ret = insert(key, value))) {
      alloc_handle_.free_value(value);
    }
    return ret;
  }
  int insert(const Key &key, Value* value)
  {
    int hash_ret = 0;
    Guard guard(ref_handle_);
    HashNode* node = NULL;
    if (NULL == (node = alloc_node(value))) {
      hash_ret = -ENOMEM;
    } else {
      ref_handle_.born(node);
      (void)ref_handle_.inc(node);
      node->xhref(2);
      node->hash_link_.set(key);
      while (-EAGAIN == (hash_ret = hash_.insert(key, &node->hash_link_)))
        ;
      if (0 != hash_ret) {
        node->xhref(-2);
        (void)ref_handle_.dec(node);
        (void)ref_handle_.end(node);
        alloc_handle_.free_node(node);
      }
    }
    if (0 == hash_ret) {
      count_handle_.add(1);
    }
    return err_code_map(hash_ret);
  }

  int del(const Key &key)
  {
    int hash_ret = 0;
    Node* hash_link = NULL;
    {
      Guard guard(ref_handle_);
      while (-EAGAIN == (hash_ret = hash_.del(key, hash_link))) 
        ;
    }
    if (0 == hash_ret) {
      HazardList reclaim_list;
      HashNode* node = CONTAINER_OF(hash_link, HashNode, hash_link_);
      if (!ref_handle_.get_delay_end_uref()) {
        end_uref(node);
      }
      ref_handle_.retire(node, reclaim_list);
      reclaim_nodes(reclaim_list);
    }
    if (0 == hash_ret) {
      count_handle_.add(-1);
    }
    return err_code_map(hash_ret);
  }

  int get(const Key &key, Value*& value)
  {
    int hash_ret = 0;
    Node* hash_link = NULL;
    Guard guard(ref_handle_);
    while (-EAGAIN == (hash_ret = hash_.get(key, hash_link)))
      ;
    if (0 == hash_ret) {
      HashNode* node = CONTAINER_OF(hash_link, HashNode, hash_link_);
      if (!try_inc_ref(node)) {
        hash_ret = -ENOENT;
      } else {
        value = (Value*)node->hash_val_;
      }
    }
    return err_code_map(hash_ret);
  }
  void revert(Value* value)
  {
    if (NULL != value) {
      HashNode* node = value->hash_node_;
      dec_uref(node);
    }
  }
  int contains_key(const Key &key) const
  {
    int hash_ret = 0;
    Node* hash_link = NULL;
    while (-EAGAIN == (hash_ret = const_cast<Hash&>(hash_).get(key, hash_link)))
      ;
    hash_ret = 0 == hash_ret ? -EEXIST : hash_ret;
    return err_code_map(hash_ret);
  }

  template <typename Function> int map(Function &fn)
  {
    int ret = OB_SUCCESS;
    if (0 != size()) {
      Value* value = NULL;
      Iterator iter(*this);
      while(OB_SUCCESS == ret && NULL != (value = iter.next())) {
        HashNode* node = value->hash_node_;
        if (NULL != node) {
          if (!fn(node->hash_link_.key_, value)) {
            ret = OB_EAGAIN;
          }
        }
      }
    }
    return ret;
  }

  template <typename Function> int for_each(Function &fn)
  {
    HandleOn<Function> handle_on(*this, fn);
    return map(handle_on);
  }
  template <typename Function> int remove_if(Function &fn)
  {
    RemoveIf<Function> remove_if(*this, fn);
    return map(remove_if);
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObLinkHashMap);
  static int err_code_map(int err)
  {
    int ret = OB_SUCCESS;
    switch (err) {
      case 0:           ret = OB_SUCCESS; break;
      case -ENOENT:     ret = OB_ENTRY_NOT_EXIST; break;
      case -EEXIST:     ret = OB_ENTRY_EXIST; break;
      case -ENOMEM:     ret = OB_ALLOCATE_MEMORY_FAILED; break;
      case -EOVERFLOW:  ret = OB_SIZE_OVERFLOW; break;
      default:          ret = OB_ERROR;
    }
    return ret;
  }

  HashNode* alloc_node(Value* value)
  {
    HashNode* node = NULL;
    if (NULL == value) {
    } else if (NULL == (node = alloc_handle_.alloc_node(value))) {
      value->hash_node_ = NULL;
    } else {
      value->hash_node_ = node;
      node->hash_val_ = value;
    }
    return node;
  }
  bool try_inc_ref(HashNode* node)
  {
    bool ret = true;
    if (!ref_handle_.inc(node)) {
      (void)ref_handle_.dec(node);
      ret = false;
    } else if(is_last_bit_set((uint64_t)node->hash_link_.next_)) {
      dec_uref(node);
      ret = false;
    }
    return ret;
  }
  void dec_uref(HashNode* node)
  {
    if (0 == ref_handle_.dec(node)) {
      on_uref_clean(node);
    }
  }
  void end_uref(HashNode* node)
  {
    if (0 == ref_handle_.end(node)) {
      on_uref_clean(node);
    }
  }
  void on_uref_clean(HashNode* node) {
    alloc_handle_.free_value((Value*)node->hash_val_);
    dec_href(node);
  }
  void dec_href(HashNode* node) {
    if (0 == node->xhref(-1)) {
      alloc_handle_.free_node(node);
      node = NULL;
    }
  }
  void reclaim_nodes(HazardList& list)
  {
    ObLink* p = NULL;
    if (list.size() > 0) {
      ref_handle_.sync();
      while(NULL != (p = list.pop())) {
        HashNode* node = CONTAINER_OF(p, HashNode, retire_link_);
        if (ref_handle_.get_delay_end_uref()) {
          end_uref(node);
        }
        dec_href(node);
      }
    }
  }
  HashNode* next_(HashNode* node) {
    Node* iter = NULL;
    HashNode* next_node = NULL;
    while (NULL != (iter = hash_.next(NULL != node? &node->hash_link_: NULL))
           && !try_inc_ref(next_node = CONTAINER_OF(iter, HashNode, hash_link_)))
      ;
    return NULL == iter? NULL: next_node;
  }

private:
  template <typename Function>
  class HandleOn
  {
  public:
    HandleOn(ObLinkHashMap &hash, Function &fn) : hash_(hash), handle_(fn) {}
    bool operator()(Key &key, Value* value)
    {
      bool need_continue = handle_(key, value);
      hash_.revert(value);
      return need_continue;
    }
  private:
    ObLinkHashMap &hash_;
    Function &handle_;
  };
  template <typename Function>
  class RemoveIf
  {
  public:
    RemoveIf(ObLinkHashMap &hash, Function &fn) : hash_(hash), predicate_(fn) {}
    bool operator()(Key &key, Value* value)
    {
      bool need_remove = predicate_(key, value);
      hash_.revert(value);
      if (need_remove) {
        (void)hash_.del(key);
      }
      // always return true
      return true;
    }
  private:
    ObLinkHashMap &hash_;
    Function &predicate_;
  };
  static bool always_true(Key &key, Value *value) { UNUSED(key); UNUSED(value); return true; }
private:
  static RetireStation& get_retire_station() {
    static RetireStation retire_station;
    return retire_station;
  }
private:
  CountHandle count_handle_;
  AllocHandle alloc_handle_;
  RefHandle ref_handle_;
  ArrayAlloc array_alloc_;
  Hash hash_;
};

} // namespace common
} // namespace oceanbase


#endif /* OCEANBASE_HASH_OB_LINK_HASHMAP_H_ */
