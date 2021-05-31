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

#ifndef OCEANBASE_CBTREE_BTREE_
#define OCEANBASE_CBTREE_BTREE_

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#ifndef __STDC_LIMIT_MACROS
#define __STDC_LIMIT_MACROS
#endif
#include <stdint.h>
#include <string.h>
#include <sys/time.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <new>
#include <unistd.h>
#include <utmpx.h>

#include "lib/oblog/ob_log.h"
#include "lib/allocator/ob_retire_station.h"
#include "lib/thread_local/ob_tsi_utils.h"

#define BTREE_LOG(level, format, args...) _OB_LOG(level, format, ## args)

#ifndef BTREE_LOG
#define BTREE_LOG(prefix, format, ...) { \
    int64_t cur_ts = cbtree::get_us(); \
    fprintf(stderr, "[%ld.%.6ld] %s %s:%d [%lx] " format "\n", \
            cur_ts/1000000, cur_ts%1000000, #prefix, __FILE__, __LINE__, pthread_self(), ##__VA_ARGS__); }
#endif

#ifndef CACHE_ALIGNED
#define CACHE_ALIGN_SIZE 64
#define CACHE_ALIGNED __attribute__((aligned(CACHE_ALIGN_SIZE)))
#endif

#define CBT_COMPILER_BARRIER() asm volatile("" ::: "memory")
#if (__GNUC__ * 10000 + __GNUC_MINOR__ * 100 + __GNUC_PATCHLEVEL__) > 40704
#define CBT_AL(x) __atomic_load_n((x), __ATOMIC_SEQ_CST)
#define CBT_AS(x, v) __atomic_store_n((x), (v), __ATOMIC_SEQ_CST)
#else
#define CBT_AL(x) ({CBT_COMPILER_BARRIER(); *(x);})
#define CBT_AS(x, v) ({CBT_COMPILER_BARRIER(); *(x) = v; __sync_synchronize(); })
#endif

#define CBT_CAS(x, ov, nv) __sync_bool_compare_and_swap((x), (ov), (nv))
#define CBT_FAA(x, i) __sync_fetch_and_add((x), (i))
#define CBT_AAF(x, i) __sync_add_and_fetch((x), (i))
#define CBT_RELAX() asm volatile ("pause;\n")
#define CHECK_POSITIVE(i) char __check_positive_array[i];
#define CBT_UNUSED(x) (void)(x)
namespace cbtree
{
enum
{
  BTREE_SUCCESS = 0,
  BTREE_ERROR = 1,
  BTREE_EAGAIN = 2,
  BTREE_DUPLICATE = 3,
  BTREE_NOENT = 4,
  BTREE_ITER_END = 5,
  BTREE_NOMEM = 6,
  BTREE_HANDLE_OVERFLOW = 7,
  BTREE_DEPTH_OVERFLOW = 8,
  BTREE_NOT_INIT = 21,
  BTREE_INIT_TWICE = 22,
  BTREE_INVAL = 23,
  BTREE_SIZE_OVERFLOW = 24,
};

enum
{
  MAX_THREAD_NUM = oceanbase::common::OB_MAX_THREAD_NUM,
  MAX_CPU_NUM = 64,
  RETIRE_LIMIT = 16,
};

typedef oceanbase::common::ObLink Link;
typedef oceanbase::common::RetireStation RetireStation;
typedef oceanbase::common::HazardList List;

struct BaseNode;
class INodeAllocator
{
  public:
  INodeAllocator() {}
  virtual ~INodeAllocator() {}
  virtual BaseNode *alloc_node(const bool is_emergency) = 0;
  virtual void free_node(BaseNode *p) = 0;
};

class IKVRCallback
{
public:
  IKVRCallback() {}
  virtual ~IKVRCallback() {}
  virtual void reclaim_key_value(void *key, void *value) = 0;
  virtual void ref_key_value(void *key, void *value)
  {
    CBT_UNUSED(key);
    CBT_UNUSED(value);
  }
  virtual void release_key_value(void *key, void *value)
  {
    CBT_UNUSED(key);
    CBT_UNUSED(value);
  }
};

class DummyKVRCallback: public IKVRCallback
{
public:
  DummyKVRCallback() {}
  virtual ~DummyKVRCallback() {}
  void reclaim_key_value(void *key, void *value)
  {
    (void)key;
    (void)value;
  }
};

inline int64_t get_us()
{
  struct timeval time_val;
  gettimeofday(&time_val, NULL);
  return time_val.tv_sec * 1000000 + time_val.tv_usec;
}

struct TCCounter
{
  struct Item
  {
    uint64_t value_;
  } CACHE_ALIGNED;
  TCCounter() { memset(items_, 0, sizeof(items_)); }
  ~TCCounter() {}
  void reset() { memset(items_, 0, sizeof(items_)); }
  int64_t inc(int64_t delta = 1) { return CBT_FAA(&items_[oceanbase::common::icpu_id() % MAX_CPU_NUM].value_, delta); }
  int64_t value() const
  {
    int64_t sum = 0;
    for (int64_t i = 0; i < MAX_CPU_NUM; i++) {
      sum += items_[i].value_;
    }
    return sum;
  }
  Item items_[MAX_CPU_NUM];
};

class RWLock
{
public:
  RWLock(): writer_id_(0), read_ref_(0) {}
  ~RWLock() {}
  bool try_rdlock()
  {
    bool lock_succ = true;
    if (CBT_AL(&writer_id_) != 0) {
      lock_succ = false;
    } else {
      (void)CBT_FAA(&read_ref_, 1);
      if (CBT_AL(&writer_id_) != 0) {
        (void)CBT_FAA(&read_ref_, -1);
        lock_succ = false;
      }
    }
    return lock_succ;
  }
  bool is_hold_wrlock(const uint32_t uid) const { return writer_id_ == uid; }
  bool try_wrlock(const uint32_t uid)
  {
    bool lock_succ = true;
    if (!CBT_CAS(&writer_id_, 0, uid)) {
      lock_succ = false;
    } else {
      while (CBT_AL(&read_ref_) > 0) {
        CBT_RELAX();
      }
    }
    return lock_succ;
  }
  void rdunlock() { (void)CBT_FAA(&read_ref_, -1); }
  void wrunlock() { CBT_AS(&writer_id_, 0); }
private:
  uint32_t writer_id_;
  int32_t read_ref_;
};

struct BaseNode: public Link
{
  BaseNode(): host_(NULL) {}
  ~BaseNode() {}
  void reset() { next_ = NULL; host_ = NULL; }
  void *host_;
};

template <typename key_t>
struct DummyCompHelper
{
  void reset_diff(key_t &key)
  {
    CBT_UNUSED(key);
  }
  void calc_diff(key_t &key1, key_t &key2)
  {
    CBT_UNUSED(key1);
    CBT_UNUSED(key2);
  }
  bool ideq(const key_t search_key, const key_t idx_key) const
  {
    return search_key.compare(idx_key) == 0;
  }
  bool veq(const key_t search_key, const key_t idx_key) const
  {
    return search_key.compare(idx_key) == 0;
  }
  int linear_search_compare(const key_t search_key, const key_t idx_key)
  {
    return search_key.compare(idx_key);
  }
  int compare(const key_t search_key, const key_t idx_key) const
  {
    return search_key.compare(idx_key);
  }
  void reset()
  {
    // do nothing
  }
};

template<typename T, int64_t capacity = 64>
class SimpleQueue
{
public:
  SimpleQueue(): push_(0), pop_(0) {}
  ~SimpleQueue() {}
  void reset()
  {
    push_ = 0;
    pop_ = 0;
  }
  int push(const T &data)
  {
    int ret = 0;
    if (push_ >= pop_ + capacity) {
      ret = BTREE_EAGAIN;
    } else {
      items_[idx(push_++)] = data;
    }
    return ret;
  }
  int pop(T &data)
  {
    int ret = 0;
    if (pop_ >= push_) {
      ret = BTREE_EAGAIN;
    } else {
      data = items_[idx(pop_++)];
    }
    return ret;
  }
  int64_t size() const { return push_ - pop_; }
private:
  int64_t idx(const int64_t x) { return x % capacity; }
private:
  int64_t push_;
  int64_t pop_;
  T items_[capacity];
};

template<typename key_t, typename comp_helper_t, int NODE_SIZE>
class BtreeBase
{
public:
  typedef void *val_t;
  typedef comp_helper_t CompHelper;
  struct kv_t
  {
    key_t key_;
    val_t val_;
  };
  enum
  {
    MAX_DEPTH = 32,
    NODE_KEY_COUNT = (NODE_SIZE - 32) / (sizeof(kv_t)),
  };

  struct Node: public BaseNode
  {
    public:
    typedef CompHelper NodeHelper;
    RWLock lock_;
    bool is_root_;
    bool is_leaf_;
    int16_t deleted_child_pos_;
    int32_t count_;
    kv_t kvs_[NODE_KEY_COUNT];
    public:
    Node(): is_root_(false), is_leaf_(false), deleted_child_pos_(-1), count_(0) {}
    ~Node() {}
    void reset()
    {
      BaseNode::reset();
      new(&lock_)RWLock();
      is_root_ = false;
      is_leaf_ = false;
      deleted_child_pos_ = -1;
      count_ = 0;
    }
    void set_deleted_pos(const int64_t pos) { deleted_child_pos_ = static_cast<int16_t>(pos); }
    void clear_deleted_pos() { deleted_child_pos_ = -1; }
    int64_t get_deleted_pos() { return deleted_child_pos_; }
    bool is_hold_wrlock(const uint32_t uid) const { return lock_.is_hold_wrlock(uid); }
    int try_rdlock() { return lock_.try_rdlock() ? BTREE_SUCCESS : BTREE_EAGAIN; }
    int try_wrlock(const uint32_t uid) { return lock_.try_wrlock(uid) ? BTREE_SUCCESS : BTREE_EAGAIN; }
    void rdunlock() { lock_.rdunlock(); }
    void wrunlock() { lock_.wrunlock();}
    bool is_leaf() const { return is_leaf_; }
    void set_is_root(const bool is_root) { is_root_ = is_root; }
    key_t &get_key(const int64_t pos) { return kvs_[pos].key_; }
    const key_t &get_key(const int64_t pos) const { return kvs_[pos].key_; }
    val_t get_val(const int64_t pos) const { return kvs_[pos].val_; }
    void set_val(const int64_t pos, val_t val) { kvs_[pos].val_ = val; }
    void make_new_leaf(NodeHelper &nh, key_t key, val_t val)
    {
      is_leaf_ = true;
      append_child(nh, key, val);
    }
    int make_new_root(NodeHelper &nh, Node *node_1, Node *node_2)
    {
      int ret = BTREE_SUCCESS;
      if (OB_ISNULL(node_1) || OB_ISNULL(node_2)) {
        ret = BTREE_INVAL;
      } else {
        is_root_ = true;
        append_child(nh, node_1->get_key(0), node_1);
        append_child(nh, node_2->get_key(0), node_2);
      }
      return ret;
    }
    int size() const { return count_; }
    bool is_overflow(const int64_t delta)
    {
      return count_ + delta > NODE_KEY_COUNT;
    }
    bool is_underflow(const int64_t delta)
    {
      return !is_root_ && (count_ + delta < NODE_KEY_COUNT / 2);
    }
    void print(FILE *file, const int depth) const
    {
      if (NULL == file) {
      } else if (NULL == this) {
        fprintf(file, "nil\n");
      } else {
        if (is_leaf_) {
          fprintf(file, "%dV: ", count_);
        } else {
          fprintf(file, "%*s %dC:\n", depth * 4, "|-", count_);
        }
        for (int64_t i = 0; i < count_; i++) {
          if (!is_leaf_) {
            fprintf(file, "%*s %s->%lx ", depth * 4, "|-",  get_key(i).repr(), (uint64_t)get_val(i));
            Node *child = (Node *)get_val(i);
            child->print(file, depth + 1);
          } else {
            fprintf(file, "%s->%lx ", get_key(i).repr(), (uint64_t)get_val(i));
          }
        }
        if (is_leaf_) {
          fprintf(file, "\n");
        }
      }
    }
    int find_pos(NodeHelper &nh, key_t key)
    {
      return binary_search_upper_bound(nh, key) - 1;
    }
  protected:
    void append_child(NodeHelper &nh, key_t key, val_t val)
    {
      if (count_ < NODE_KEY_COUNT) {
        set_key_value(nh, count_, key, val);
        count_++;
      }
    }
    int linear_search_upper_bound(NodeHelper &nh, key_t key)
    {
      // find first item > key
      int pos = 0;
      for (pos = 0; pos < count_; pos++) {
        if (nh.linear_search_compare(key, get_key(pos)) < 0) {
          break;
        }
      }
      return pos;
    }
    int binary_search_upper_bound(NodeHelper &nh, key_t key)
    {
      // find first item > key
      int start = 0;
      int end = count_;
      // valid value to compare is within [start, end)
      // return idx is within [start, end], return end if all valid value < key
      while (start < end) {
        int pos = start + (end - start) / 2;
        if (nh.compare(key, get_key(pos)) < 0) {
          end = pos;
        } else {
          start = pos + 1;
        }
      }
      return end;
    }
    void calc_part_key(NodeHelper &nh, const int idx)
    {
      if (0 == idx) {
        nh.reset_diff(get_key(idx));
      } else {
        nh.calc_diff(get_key(idx), get_key(idx - 1));
      }
    }
    void calc_follow_part_key(NodeHelper &nh, const int idx)
    {
      if (idx < count_) {
        calc_part_key(nh, idx);
      }
    }
    void set_key_value(NodeHelper &nh, const int idx, key_t key, val_t val)
    {
      kvs_[idx].key_ = key;
      kvs_[idx].val_ = val;
      calc_part_key(nh, idx);
    }
    //kmnc -> key may not change
    void set_key_value_kmnc(NodeHelper &nh, const int idx, key_t key, val_t val)
    {
      if (!nh.ideq(kvs_[idx].key_, key)) {
        kvs_[idx].key_ = key;
        calc_part_key(nh, idx);
      }
      kvs_[idx].val_ = val;
    }
    void copy(NodeHelper &nh, Node &dest, const int dest_start, const int start, const int end)
    {
      if (dest_start < dest.count_ && start < end) {
        MEMCPY(dest.kvs_ + dest_start, this->kvs_ + start, sizeof(kv_t) * (end - start));
        dest.calc_part_key(nh, dest_start);
      }
    }
    void copy_and_split_child(NodeHelper &nh, Node &dest_node, const int dest_start, const int start,
                              const int end,
                              int pos, key_t key_1,
                              val_t val_1, key_t key_2, val_t val_2)
    {
      if (pos - start >= 0) {
        copy(nh, dest_node, dest_start, start, pos + 1);
        dest_node.set_key_value_kmnc(nh, dest_start + pos - start, key_1, val_1);
      }
      dest_node.set_key_value(nh, dest_start + pos + 1 - start, key_2, val_2);
      copy(nh, dest_node, dest_start + (pos + 2) - start, pos + 1, end);
    }
    void copy_and_merge_child(NodeHelper &nh, Node &dest_node, const int dest_start, const int start,
                              const int end,
                              int pos, key_t key,
                              val_t val)
    {
      copy(nh, dest_node, dest_start, start, pos);
      dest_node.set_key_value_kmnc(nh, dest_start + pos - 1 - start, key, val);
      copy(nh, dest_node, dest_start + pos - start, pos + 1, end);
    }
    int get_merge_pos(const int pos) // delete key pos
    {
      return pos > 0 ? pos : pos + 1;
    }
  public:
    int replace_child_and_key(NodeHelper &nh, Node *new_node, const int pos, Node *child)
    {
      int ret = BTREE_SUCCESS;
      if (OB_ISNULL(new_node)) {
        ret = BTREE_INVAL;
      } else {
        new_node->is_leaf_ = is_leaf_;
        new_node->count_ = count_;
        copy(nh, *new_node, 0, 0, count_);
        new_node->set_key_value(nh, pos, child->get_key(0), child);
        new_node->calc_follow_part_key(nh, pos + 1);
      }
      return ret;
    }
    int split_child_no_overflow(NodeHelper &nh, Node *new_node, const int pos, key_t key_1, val_t val_1,
                                key_t key_2,
                                val_t val_2)
    {
      int ret = BTREE_SUCCESS;
      if (OB_ISNULL(new_node)) {
        ret = BTREE_INVAL;
      } else {
        new_node->is_leaf_ = is_leaf_;
        new_node->count_ = count_ + 1;
        copy_and_split_child(nh, *new_node, 0, 0, count_, pos, key_1, val_1, key_2, val_2);
      }
      return ret;
    }
    int split_child_cause_recursive_split(NodeHelper &nh, Node *new_node_1, Node *new_node_2,
                                          const int pos,
                                          key_t key_1,
                                          val_t val_1, key_t key_2, val_t val_2)
    {
      int ret = BTREE_SUCCESS;
      const int32_t half_limit = NODE_KEY_COUNT / 2;
      if (OB_ISNULL(new_node_1) || OB_ISNULL(new_node_2)) {
        ret = BTREE_INVAL;
      } else {
        new_node_1->is_leaf_ = is_leaf_;
        new_node_2->is_leaf_ = is_leaf_;

        if (pos < half_limit) {
          new_node_1->count_ = half_limit + 1;
          new_node_2->count_ = count_ - half_limit;
          copy_and_split_child(nh, *new_node_1, 0, 0, half_limit, pos, key_1, val_1, key_2, val_2);
          copy(nh, *new_node_2, 0, half_limit, count_);
        } else {
          new_node_1->count_ = half_limit;
          new_node_2->count_ = count_ + 1 - half_limit;
          copy(nh, *new_node_1, 0, 0, half_limit);
          copy_and_split_child(nh, *new_node_2, 0, half_limit, count_, pos, key_1, val_1, key_2, val_2);
        }
      }
      return ret;
    }
    int get_brother_pos(const int pos)
    {
      return pos > 0 ? pos - 1 : pos + 1;
    }
    int replace_child_and_brother(NodeHelper &nh, Node *new_node, const int pos, Node *child,
                                  Node *brother)
    {
      int ret = BTREE_SUCCESS;
      if (OB_ISNULL(new_node) || OB_ISNULL(child) || OB_ISNULL(brother)) {
        ret = BTREE_INVAL;
      } else {
        new_node->is_leaf_ = is_leaf_;
        new_node->count_ = count_;
        copy(nh, *new_node, 0, 0, count_);
        if (pos > 0) {
          new_node->set_key_value_kmnc(nh, pos - 1, brother->get_key(0), brother);
          new_node->set_key_value(nh, pos, child->get_key(0), child);
          new_node->calc_follow_part_key(nh, pos + 1);
        } else {
          new_node->set_key_value_kmnc(nh, pos, child->get_key(0), child);
          new_node->set_key_value(nh, pos + 1, brother->get_key(0), brother);
          new_node->calc_follow_part_key(nh, pos + 2);
        }
      }
      return ret;
    }
    int merge_child_no_underflow(NodeHelper &nh, Node *new_node, const int pos, key_t key, val_t val)
    {
      int ret = BTREE_SUCCESS;
      if (OB_ISNULL(new_node)) {
        ret = BTREE_INVAL;
      } else {
        new_node->is_leaf_ = is_leaf_;
        new_node->count_ = count_ - 1;
        copy_and_merge_child(nh, *new_node, 0, 0, count_, get_merge_pos(pos), key, val);
      }
      return ret;
    }
    int merge_child_cause_recursive_merge_with_right_brother(NodeHelper &nh, Node *new_node,
                                                             Node *brother, const int pos,
                                                             key_t key, val_t val)
    {
      int ret = BTREE_SUCCESS;
      if (OB_ISNULL(new_node) || OB_ISNULL(brother)) {
        ret = BTREE_INVAL;
      } else {
        new_node->is_leaf_ = is_leaf_;
        new_node->count_ = count_ + brother->size() - 1;
        copy_and_merge_child(nh, *new_node, 0, 0, count_, get_merge_pos(pos), key, val);
        brother->copy(nh, *new_node, count_ - 1, 0, brother->count_);
      }
      return ret;
    }
    int merge_child_cause_recursive_merge_with_left_brother(NodeHelper &nh, Node *new_node,
                                                            Node *brother, const int pos,
                                                            key_t key, val_t val)
    {
      int ret = BTREE_SUCCESS;
      if (OB_ISNULL(new_node) || OB_ISNULL(brother)) {
        ret = BTREE_INVAL;
      } else {
        new_node->is_leaf_ = is_leaf_;
        new_node->count_ = count_ + brother->size() - 1;
        brother->copy(nh, *new_node, 0, 0, brother->count_);
        copy_and_merge_child(nh, *new_node, brother->count_, 0, count_, get_merge_pos(pos), key, val);
      }
      return ret;
    }
    int merge_child_cause_rebalance_with_right_brother(NodeHelper &nh, Node *new_node_1,
                                                       Node *new_node_2,
                                                       Node *brother, const int pos, key_t key, val_t val)
    {
      int ret = BTREE_SUCCESS;
      if (OB_ISNULL(new_node_1) || OB_ISNULL(new_node_2) || OB_ISNULL(brother)) {
        ret = BTREE_INVAL;
      } else {
        new_node_1->is_leaf_ = is_leaf_;
        new_node_1->count_ = (count_ + brother->count_ - 1) / 2;
        new_node_2->is_leaf_ = is_leaf_;
        new_node_2->count_ = (count_ + brother->count_ - 1) - new_node_1->count_;
        copy_and_merge_child(nh, *new_node_1, 0, 0, count_, get_merge_pos(pos), key, val);
        brother->copy(nh, *new_node_1, count_ - 1, 0, new_node_1->count_ - count_ + 1);
        brother->copy(nh, *new_node_2, 0, new_node_1->count_ - count_ + 1, brother->count_);
      }
      return ret;
    }
    int merge_child_cause_rebalance_with_left_brother(NodeHelper &nh, Node *new_node_1,
                                                      Node *new_node_2, Node *brother,
                                                      int pos, key_t key, val_t val)
    {
      int ret = BTREE_SUCCESS;
      if (OB_ISNULL(new_node_1) || OB_ISNULL(new_node_2) || OB_ISNULL(brother)) {
        ret = BTREE_INVAL;
      } else {
        new_node_1->is_leaf_ = is_leaf_;
        new_node_1->count_ = (count_ + brother->count_ - 1) / 2;
        new_node_2->is_leaf_ = is_leaf_;
        new_node_2->count_ = (count_ + brother->count_ - 1) - new_node_1->count_;
        brother->copy(nh, *new_node_1, 0, 0, new_node_1->count_);
        brother->copy(nh, *new_node_2, 0, new_node_1->count_, brother->count_);
        copy_and_merge_child(nh, *new_node_2, brother->count_ - new_node_1->count_, 0, count_,
                             get_merge_pos(pos), key, val);
      }
      return ret;
    }
  };
  CHECK_POSITIVE(NODE_SIZE - (int)sizeof(Node));

  class Path
  {
  public:
    enum { CAPACITY = MAX_DEPTH };
    struct Item
    {
      Item(): node_(NULL), pos_(-1) {}
      ~Item() {}
      Node *node_;
      int pos_;
    };
    Path(): depth_(0) {}
    ~Path() {}
    void reset() { depth_ = 0; }
    int push(Node *node, const int pos)
    {
      int ret = BTREE_SUCCESS;
      if (depth_ >= CAPACITY) {
        ret = BTREE_DEPTH_OVERFLOW;
      } else {
        path_[depth_].node_ = node;
        path_[depth_].pos_ = pos;
        depth_++;
      }
      return ret;
    }
    int pop(Node *&node, int &pos)
    {
      int ret = BTREE_SUCCESS;
      if (depth_ <= 0) {
        ret = BTREE_DEPTH_OVERFLOW;
        node = NULL;
        pos = -1;
      } else {
        depth_--;
        node = path_[depth_].node_;
        pos = path_[depth_].pos_;
      }
      return ret;
    }
    int top(Node *&node, int &pos)
    {
      int ret = BTREE_SUCCESS;
      if (depth_ <= 0) {
        ret = BTREE_DEPTH_OVERFLOW;
        node = NULL;
        pos = -1;
      } else {
        node = path_[depth_ - 1].node_;
        pos = path_[depth_ - 1].pos_;
      }
      return ret;
    }
    bool is_empty() const { return 0 == depth_; }
  private:
    int64_t depth_;
    Item path_[CAPACITY];
  };

  class BaseHandle
  {
  public:
    BaseHandle(): is_hold_ref_(false), comp_() {}
    ~BaseHandle() { release_ref(); }
    int acquire_ref()
    {
      int ret = BTREE_SUCCESS;
      oceanbase::common::get_global_qclock().enter_critical();
      is_hold_ref_ = true;
      return ret;
    }
    void release_ref()
    {
      if (is_hold_ref_) {
        is_hold_ref_ = false;
        oceanbase::common::get_global_qclock().leave_critical();
      }
    }
    CompHelper &get_comp() { return comp_; }
  private:
    bool is_hold_ref_;
    CompHelper comp_;
  };

  class GetHandle: public BaseHandle
  {
  public:
    GetHandle(BtreeBase &tree): BaseHandle() {  CBT_UNUSED(tree); }
    ~GetHandle() {}
    int get(Node *root, key_t key, val_t &val)
    {
      int ret = BTREE_SUCCESS;
      Node *leaf = NULL;
      int64_t pos = -1;
      if (NULL == root) {
        ret = BTREE_NOENT;
      }
      while (BTREE_SUCCESS == ret) {
        if ((pos = root->find_pos(this->get_comp(), key)) < 0) {
          ret = BTREE_NOENT;
        } else if (root->is_leaf()) {
          leaf = root;
          break;
        } else {
          root = reinterpret_cast<Node *>(root->get_val(pos));
        }
      }
      if (NULL != leaf) {
        key_t prev_key = leaf->get_key(pos);
        if (!(this->get_comp().veq(prev_key, key))) {
          ret = BTREE_NOENT;
        } else {
          val = leaf->get_val(pos);
        }
      }
      return ret;
    }
  };

  class ScanHandle: public BaseHandle
  {
  private:
    Path path_;
  public:
    explicit ScanHandle(BtreeBase &tree): BaseHandle() { CBT_UNUSED(tree); }
    ~ScanHandle() {}
    void reset()
    {
      this->release_ref();
      path_.reset();
    }
    int get(key_t &key, val_t &val, bool is_backward, key_t*& last_key)
    {
      int ret = BTREE_SUCCESS;
      Node *leaf = NULL;
      int pos = 0;
      if (BTREE_SUCCESS != (ret = path_.top(leaf, pos))) {
        ret = BTREE_ITER_END;
      } else {
        key = leaf->get_key(pos);
        val = leaf->get_val(pos);
        last_key = is_backward? &leaf->get_key(0): &leaf->get_key(leaf->size() - 1);
      }
      return ret;
    }
    int find_path(Node *root, key_t key)
    {
      int ret = BTREE_SUCCESS;
      int pos = -1;
      bool may_exist = true;
      while (NULL != root && BTREE_SUCCESS == ret) {
        if (may_exist && (pos = root->find_pos(this->get_comp(), key)) < 0) {
          may_exist = false;
          pos = 0;
        }
        if (BTREE_SUCCESS != (ret = path_.push(root, pos)))
        {}
        else if (root->is_leaf()) {
          root = NULL;
        } else {
          root = (Node *)root->get_val(pos);
        }
      }
      return ret;
    }
    int scan_forward()
    {
      int ret = BTREE_SUCCESS;
      Node *node = NULL;
      int pos = 0;
      while (BTREE_SUCCESS == ret) {
        if (BTREE_SUCCESS != (ret = path_.pop(node, pos))) {
          ret = BTREE_ITER_END;
        } else if (++pos >= node->size())
        {}
        else if (BTREE_SUCCESS != (ret = path_.push(node, pos)))
        {}
        else {
          break;
        }
      }
      while (BTREE_SUCCESS == ret) {
        if (BTREE_SUCCESS != (ret = path_.top(node, pos)))
        {}
        else if (node->is_leaf()) {
          break;
        } else if (BTREE_SUCCESS != (ret = path_.push(reinterpret_cast<Node *>(node->get_val(pos)), 0))) {
          BTREE_LOG(ERROR, "push fail err=%d", ret);
        }
      }
      return ret;
    }
    int scan_backward()
    {
      int ret = BTREE_SUCCESS;
      Node *node = NULL;
      int pos = 0;
      while (BTREE_SUCCESS == ret) {
        if (BTREE_SUCCESS != (ret = path_.pop(node, pos))) {
          ret = BTREE_ITER_END;
        } else if (--pos < 0)
        {}
        else if (BTREE_SUCCESS != (ret = path_.push(node, pos)))
        {}
        else {
          break;
        }
      }
      while (BTREE_SUCCESS == ret) {
        if (BTREE_SUCCESS != (ret = path_.top(node, pos)))
        {}
        else if (node->is_leaf()) {
          break;
        } else if (BTREE_SUCCESS != (ret = path_.push(reinterpret_cast<Node *>(node->get_val(pos)),
                                                      (reinterpret_cast<Node *>(node->get_val(pos))->size() - 1))))
        {}
      }
      return ret;
    }
  };

  class OptimisticScanCompHelper
  {
  public:
    explicit OptimisticScanCompHelper(CompHelper& comp):
        enable_optimistic_comp_(true),
        comp_(comp), is_backward_(false), bound_key_(NULL), jump_key_(NULL), cmp_result_(0) {}
    ~OptimisticScanCompHelper() {}
    void reset()
    {
      is_backward_ = false;
      bound_key_ = NULL;
      jump_key_ = NULL;
      cmp_result_ = 0;
    }
    void set(bool is_backward, key_t* bound_key)
    {
      is_backward_ = is_backward;
      bound_key_ = bound_key;
      jump_key_ = NULL;
      cmp_result_ = 0;
    }
    // cmp < 0: iter end
    int comp(key_t& cur_key, key_t* jump_key)
    {
      int cmp = 0;
      if (NULL == bound_key_) {
      } else {
        if (!enable_optimistic_comp_) {
        } else if (NULL == jump_key) {
        } else if (jump_key_ == jump_key) {
          cmp = cmp_result_;
        } else {
          cmp_result_ = is_backward_? comp_.compare(*jump_key, *bound_key_) : comp_.compare(*bound_key_, *jump_key);
          jump_key_ = jump_key;
          cmp = cmp_result_;
        }
        if (cmp <= 0) {
          cmp = is_backward_? comp_.compare(cur_key, *bound_key_) : comp_.compare(*bound_key_, cur_key);
        }
      }
      return cmp;
    }
  private:
    bool enable_optimistic_comp_;
    CompHelper &comp_;
    bool is_backward_;
    key_t* bound_key_;
    key_t* jump_key_;
    int cmp_result_;
  };
  
  class Iterator
  {
  public:
    explicit Iterator(BtreeBase &btree): btree_(btree), scan_handle_(btree),
                                         optimistic_comp_helper_(scan_handle_.get_comp()),
                                         comp_(scan_handle_.get_comp()),
                                         start_key_(), end_key_(), start_exclude_(0), end_exclude_(0),
                                         scan_backward_(false), is_iter_end_(false), iter_count_(0) {}
    ~Iterator()
    {
      scan_handle_.reset();
    }
    void reset()
    {
      scan_handle_.reset();
      optimistic_comp_helper_.reset();
      new(&start_key_)key_t();
      new(&end_key_)key_t();
      start_exclude_ = 0;
      end_exclude_ = 0;
      scan_backward_ = false;
      is_iter_end_ = false;
      iter_count_ = 0;
    }
    int set_key_range(Node **root, const key_t min_key, const int start_exclude,
                      const key_t max_key, const int end_exclude)
    {
      int ret = BTREE_SUCCESS;
      if (OB_ISNULL(root)) {
        ret = BTREE_INVAL;
      } else if (BTREE_SUCCESS != (ret = btree_.search(root, scan_handle_, min_key)))
      {}
      else {
        scan_backward_ = (comp_.compare(max_key, min_key) < 0);
        start_key_ = min_key;
        end_key_ = max_key;
        start_exclude_ = start_exclude;
        end_exclude_ = end_exclude;
        optimistic_comp_helper_.set(scan_backward_, &end_key_);
      }
      return ret;
    }
    int get_next(key_t &key, val_t &value)
    {
      int ret = BTREE_SUCCESS;
      if (BTREE_SUCCESS != (ret = iter_next(key, value)))
      {}
      else if (0 == iter_count_) {
        int cmp = scan_backward_ ? comp_.compare(start_key_, key) : comp_.compare(key, start_key_);
        if (cmp < 0 || (0 != start_exclude_ && 0 == cmp)) {
          ret = iter_next(key, value);
        }
      }
      if (BTREE_SUCCESS == ret) {
        iter_count_++;
      } else {
        scan_handle_.release_ref();
      }
      return ret;
    }
  private:
    int iter_next(key_t &key, val_t &value)
    {
      int ret = BTREE_SUCCESS;
      key_t* jump_key = NULL;
      if (is_iter_end_) {
        ret = BTREE_ITER_END;
      } else if (BTREE_SUCCESS != (ret = scan_handle_.get(key, (void *&)value, scan_backward_, jump_key)))
      {}
      else {
        int cmp = optimistic_comp_helper_.comp(key, jump_key);
        if (cmp < 0) {
          ret = BTREE_ITER_END;
        } else if (cmp > 0) {
          if (BTREE_SUCCESS != (scan_backward_ ? scan_handle_.scan_backward() :
                                scan_handle_.scan_forward())) {
            is_iter_end_ = true;
          }
        } else if (cmp == 0) {
          is_iter_end_ = true;
          if (0 != end_exclude_) {
            ret = BTREE_ITER_END;
          }
        }
      }
      if (BTREE_SUCCESS != ret) {
        is_iter_end_ = true;
      }
      return ret;
    }
  private:
    BtreeBase &btree_;
    ScanHandle scan_handle_;
    OptimisticScanCompHelper optimistic_comp_helper_;
    CompHelper &comp_;
    key_t start_key_;
    key_t end_key_;
    int start_exclude_;
    int end_exclude_;
    bool scan_backward_;
    bool is_iter_end_;
    int64_t iter_count_;
  };

  class HazardLessIterator
  {
  public:
    struct KVPair
    {
      key_t key_;
      val_t val_;
    };
    typedef SimpleQueue<KVPair> KVQueue;
    explicit HazardLessIterator(BtreeBase &btree):
        kvr_callback_(btree.get_kvr_callback()),
        iter_(btree),
        root_(NULL),
        start_key_(),
        end_key_(),
        start_exclude_(0),
        end_exclude_(0),
        is_iter_end_(false)
    {}
    ~HazardLessIterator() { reset(); }
    void reset()
    {
      iter_.reset();
      new(&start_key_)key_t();
      new(&end_key_)key_t();
      start_exclude_ = 0;
      end_exclude_ = 0;
      is_iter_end_ = false;
    }
    int set_key_range(Node **root, const key_t min_key, const int start_exclude,
                      const key_t max_key, const int end_exclude)
    {
      int ret = BTREE_SUCCESS;
      if (NULL == root) {
        ret = BTREE_INVAL;
      } else {
        root_ = root;
        start_key_ = min_key;
        end_key_ = max_key;
        start_exclude_ = start_exclude;
        end_exclude_ = end_exclude;
        is_iter_end_ = false;
      }
      return ret;
    }
    int get_next(key_t &key, val_t &value)
    {
      int ret = BTREE_SUCCESS;
      KVPair item;
      if (BTREE_SUCCESS == (ret = kv_queue_.pop(item))) {
      } else if (BTREE_SUCCESS != (ret = scan_batch())) {
      } else if (BTREE_SUCCESS != (ret = kv_queue_.pop(item))) {
      }
      if (BTREE_SUCCESS == ret) {
        key = item.key_;
        value = item.val_;
      }
      return ret;
    }
    void revert(key_t &key, val_t &value)
    {
      kvr_callback_.release_key_value(&key, value);
    }
  private:
    int scan_batch()
    {
      int ret = BTREE_SUCCESS;
      if (is_iter_end_) {
        ret = BTREE_ITER_END;
      } else if (BTREE_SUCCESS != (ret = iter_.set_key_range(root_, start_key_, start_exclude_, end_key_,
                                                             end_exclude_))) {
      } else {
        KVPair item;
        while (BTREE_SUCCESS == ret) {
          if (BTREE_SUCCESS != (ret = iter_.get_next(item.key_, item.val_))) {
            is_iter_end_ = true;
            if (kv_queue_.size() > 0) {
              ret = BTREE_SUCCESS;
            }
            break;
          } else if (BTREE_SUCCESS != kv_queue_.push(item)) {
            break;
          } else {
            start_key_ = item.key_;
            start_exclude_ = 1;
            kvr_callback_.ref_key_value(&item.key_, item.val_);
          }
        }
        iter_.reset();
      }
      if (BTREE_SUCCESS != ret) {
        is_iter_end_ = true;
      }
      return ret;
    }
  private:
    IKVRCallback &kvr_callback_;
    Iterator iter_;
    Node **root_;
    key_t start_key_;
    key_t end_key_;
    int start_exclude_;
    int end_exclude_;
    bool is_iter_end_;
    KVQueue kv_queue_;
  };


  class WriteHandle: public BaseHandle
  {
  private:
    BtreeBase &base_;
    Path path_;
    List retire_list_;
    List alloc_list_;
  public:
    explicit WriteHandle(BtreeBase &tree): BaseHandle(), base_(tree) {}
    ~WriteHandle() {}
    bool &get_is_in_delete()
    {
      static __thread bool is_in_delete;
      return is_in_delete;
    }
    Node *alloc_node()
    {
      Node *p = NULL;
      if (NULL != (p = (Node *)base_.alloc_node(get_is_in_delete()))) {
        alloc_list_.push(p);
      }
      return p;
    }
    void free_node(Node *p)
    {
      if (NULL != p) {
        base_.free_node(p);
        p = NULL;
      }
    }
    void retire(const int btree_err)
    {
      if (BTREE_SUCCESS != btree_err) {
        Node *p = NULL;
        while (NULL != (p = (Node *)retire_list_.pop())) {
          p->clear_deleted_pos();
          p->wrunlock();
        }
        while (NULL != (p = (Node *)alloc_list_.pop())) {
          free_node(p);
        }
      } else {
        base_.retire(retire_list_, RETIRE_LIMIT);
      }
    }
    int find_path(Node *root, key_t key)
    {
      int ret = BTREE_SUCCESS;
      int pos = -1;
      bool may_exist = true;
      while (NULL != root && BTREE_SUCCESS == ret) {
        if (may_exist && (pos = root->find_pos(this->get_comp(), key)) < 0) {
          may_exist = false;
          if (get_is_in_delete()) {
            ret = BTREE_NOENT;
            break;
          }
        }
        if (BTREE_SUCCESS != (ret = path_.push(root, pos)))
        {}
        else if (root->is_leaf()) {
          root = NULL;
        } else {
          root = (Node *)root->get_val(pos >= 0 ? pos : 0);
        }
      }
      return ret;
    }
  public:
    int insert_and_split_upward(key_t key, val_t val, const bool overwrite, Node *&new_root)
    {
      int ret = BTREE_SUCCESS;
      Node *old_node = NULL;
      int pos = -1;
      Node *new_node_1 = NULL;
      Node *new_node_2 = NULL;
      this->path_.pop(old_node, pos); // old_node allowed be NULL, so ignore return value of pop func
      ret = insert_to_leaf(old_node, overwrite, pos, key, val, new_node_1, new_node_2);
      while (BTREE_SUCCESS == ret) {
        if (old_node == new_node_1) {
          break;
        } else if (NULL == new_node_2) {
          if (BTREE_SUCCESS != this->path_.pop(old_node, pos)) {
            old_node = new_node_1;
          } else if (pos < 0) {
            ret = replace_child_and_key(old_node, 0, new_node_1, new_node_1);
          } else {
            ret = replace_child(old_node, pos, new_node_1);
            new_node_1 = old_node;
          }
        } else {
          if (BTREE_SUCCESS != this->path_.pop(old_node, pos)) {
            ret = this->make_new_root(old_node, new_node_1, new_node_2);
            new_node_1 = old_node;
            new_node_2 = NULL;
          } else {
            ret = split_child(old_node, pos >= 0 ? pos : 0, new_node_1->get_key(0), new_node_1,
                              new_node_2->get_key(0), new_node_2, new_node_1, new_node_2);
          }
        }
      }
      if (BTREE_SUCCESS == ret && this->path_.is_empty()) {
        new_root = old_node;
      }
      return ret;
    }

    int delete_and_merge_upward(key_t key, val_t &val, Node *&new_root)
    {
      int ret = BTREE_SUCCESS;
      Node *old_node = NULL;
      int pos = -1;
      Node *new_node_1 = NULL;
      Node *new_node_2 = NULL;
      (void)this->path_.pop(old_node, pos); // old_node allowed be NULL, so ignore return value of pop func
      ret = delete_from_leaf(old_node, pos, key, val, new_node_1, new_node_2);
      while (BTREE_SUCCESS == ret) {
        if (old_node == new_node_1) {
          break;
        } else if (NULL == new_node_2) {
          if (BTREE_SUCCESS != this->path_.pop(old_node, pos)) {
            old_node = new_node_1;
          } else if (!(this->get_comp().ideq(old_node->get_key(pos), new_node_1->get_key(0)))) {
            ret = replace_child_and_key(old_node, pos, new_node_1, new_node_1);
          } else {
            ret = replace_child(old_node, pos, new_node_1);
            new_node_1 = old_node;
          }
        } else if (NULL == new_node_1) {
          if (BTREE_SUCCESS != (ret = this->path_.pop(old_node, pos)))
          {}
          else {
            ret = merge_child(old_node, pos, new_node_2->get_key(0), new_node_2, new_node_1, new_node_2);
          }
        } else {
          if (BTREE_SUCCESS != (ret = this->path_.pop(old_node, pos)))
          {}
          else if (BTREE_SUCCESS != (ret = replace_child_and_brother(old_node, pos, new_node_1, new_node_2,
                                                                     new_node_1)))
          {}
          else {
            new_node_2 = NULL;
          }
        }
      }
      if (BTREE_SUCCESS == ret && path_.is_empty()) {
        if (new_root == old_node)
        {}
        else if (NULL != old_node && !old_node->is_leaf() && old_node->size() == 1) {
          new_root = (Node *)old_node->get_val(0);
          retire_list_.push(old_node);
        } else {
          new_root = old_node;
        }
      }
      return ret;
    }
  private:
    int try_wrlock(Node *node)
    {
      int ret = BTREE_SUCCESS;
      uint32_t uid = (uint32_t)oceanbase::common::get_itid() + 1;
      if (OB_ISNULL(node)) {
        ret = BTREE_INVAL;
      } else if (node->is_hold_wrlock(uid))
      {}
      else if (0 != node->try_wrlock(uid)) {
        ret = BTREE_EAGAIN;
      } else {
        retire_list_.push(node);
      }
      return ret;
    }

    int replace_child(Node *old_node, const int64_t pos, val_t val)
    {
      int ret = BTREE_SUCCESS;
      if (OB_ISNULL(old_node)) {
        ret = BTREE_INVAL;
      } else if (0 != old_node->try_rdlock()) {
        ret = BTREE_EAGAIN;
      } else {
        old_node->set_val(pos, val);
        old_node->rdunlock();
      }
      return ret;
    }
    int replace_child_and_key(Node *old_node, const int pos, Node *child, Node *&new_node)
    {
      int ret = BTREE_SUCCESS;
      if (OB_ISNULL(old_node) || OB_ISNULL(child)) {
        ret = BTREE_INVAL;
      } else if (BTREE_SUCCESS != (ret = try_wrlock(old_node)))
      {}
      else if (NULL == (new_node = alloc_node())) {
        ret = BTREE_NOMEM;
      } else {
        ret = old_node->replace_child_and_key(this->get_comp(), new_node, pos, child);
      }
      return ret;
    }
    int replace_child_and_brother(Node *old_node, const int pos, Node *child, Node *brother,
                                  Node *&new_node)
    {
      int ret = BTREE_SUCCESS;
      if (OB_ISNULL(old_node) || OB_ISNULL(child) || OB_ISNULL(brother)) {
        ret = BTREE_INVAL;
      } else if (BTREE_SUCCESS != (ret = try_wrlock(old_node)))
      {}
      else if (NULL == (new_node = alloc_node())) {
        ret = BTREE_NOMEM;
      } else {
        ret = old_node->replace_child_and_brother(this->get_comp(), new_node, pos, child, brother);
      }
      return ret;
    }

    int make_new_leaf(Node *&leaf, key_t key, val_t val)
    {
      int ret = BTREE_SUCCESS;
      if (NULL == (leaf = alloc_node())) {
        ret = BTREE_NOMEM;
      } else {
        leaf->make_new_leaf(this->get_comp(), key, val);
      }
      return ret;
    }

    int make_new_root(Node *&root, Node *node_1, Node *node_2)
    {
      int ret = BTREE_SUCCESS;
      if (OB_ISNULL(node_1) || OB_ISNULL(node_2)) {
        ret = BTREE_INVAL;
      } else if (NULL == (root = alloc_node())) {
        ret = BTREE_NOMEM;
      } else {
        ret = root->make_new_root(this->get_comp(), node_1, node_2);
      }
      return ret;
    }

    int insert_to_leaf(Node *old_node, const bool overwrite, const int pos, key_t key, val_t val,
                       Node *&new_node_1,
                       Node *&new_node_2)
    {
      int ret = BTREE_SUCCESS;
      if (NULL == old_node) {
        ret = make_new_leaf(new_node_1, key, val);
        new_node_2 = NULL;
      } else if (pos < 0 || !(this->get_comp().veq(old_node->get_key(pos), key))) {
        key_t dummy_key;
        ret = split_child(old_node, pos, pos < 0 ? dummy_key : old_node->get_key(pos),
                          pos < 0 ? NULL : old_node->get_val(pos), key, val, new_node_1, new_node_2);
      } else if (overwrite) {
        ret = replace_child(old_node, pos, val);
        new_node_1 = old_node;
        new_node_2 = NULL;
      } else {
        ret = BTREE_DUPLICATE;
      }
      return ret;
    }
    int split_child(Node *old_node, const int pos, key_t key_1, val_t val_1, key_t key_2, val_t val_2,
                    Node *&new_node_1, Node *&new_node_2)
    {
      int ret = BTREE_SUCCESS;
      if (OB_ISNULL(old_node)) {
        ret = BTREE_INVAL;
      } else if (BTREE_SUCCESS != (ret = try_wrlock(old_node)))
      {}
      else if (old_node->is_overflow(1)) {
        if (NULL == (new_node_1 = alloc_node()) || NULL == (new_node_2 = alloc_node())) {
          ret = BTREE_NOMEM;
        } else {
          ret = old_node->split_child_cause_recursive_split(this->get_comp(), new_node_1, new_node_2, pos,
                                                            key_1, val_1, key_2,
                                                            val_2);
        }
      } else {
        if (NULL == (new_node_1 = alloc_node())) {
          ret = BTREE_NOMEM;
        } else {
          ret = old_node->split_child_no_overflow(this->get_comp(), new_node_1, pos, key_1, val_1, key_2,
                                                  val_2);
        }
        new_node_2 = NULL;
      }
      return ret;
    }

    int delete_from_leaf(Node *old_node, const int pos, key_t key, val_t &val, Node *&new_node_1,
                         Node *&new_node_2)
    {
      int ret = BTREE_SUCCESS;
      if (NULL == old_node || pos < 0 || !(this->get_comp().veq(old_node->get_key(pos), key))) {
        ret = BTREE_NOENT;
        BTREE_LOG(INFO, "delete_from_leaf: old_node=%p pos=%d", old_node, pos);
      } else if (BTREE_SUCCESS != (ret = try_wrlock(old_node)))
      {}
      else {
        val = old_node->get_val(pos);
        int brother_pos = old_node->get_brother_pos(pos);
        old_node->set_deleted_pos(pos);
        ret = merge_child(old_node, pos, old_node->get_key(brother_pos), old_node->get_val(brother_pos),
                          new_node_1, new_node_2);
      }
      return ret;
    }
    int merge_child(Node *old_node, const int pos, key_t key, val_t val, Node *&new_node_1,
                    Node *&new_node_2)
    {
      int ret = BTREE_SUCCESS;
      Node *parent_node = NULL;
      Node *brother_node = NULL;
      int parent_pos = -1;
      if (OB_ISNULL(old_node)) {
        ret = BTREE_INVAL;
      } else if (BTREE_SUCCESS != (ret = try_wrlock(old_node)))
      {}
      else if (!old_node->is_underflow(-1)) {
        if (old_node->size() <= 1) {
          new_node_1 = NULL;
        } else if (NULL == (new_node_1 = alloc_node())) {
          ret = BTREE_NOMEM;
        } else {
          ret = old_node->merge_child_no_underflow(this->get_comp(), new_node_1, pos, key, val);
        }
        new_node_2 = NULL;
      } else if (BTREE_SUCCESS != (ret = this->path_.top(parent_node, parent_pos)))
      {}
      else if (BTREE_SUCCESS != (ret = try_wrlock(parent_node)))
      {}
      else if (NULL == (brother_node = reinterpret_cast<Node *>(parent_node->get_val(
                                                                    parent_node->get_brother_pos(
                                                                        parent_pos))))) {
        ret = BTREE_ERROR;
      } else if (BTREE_SUCCESS != (ret = try_wrlock(brother_node)))
      {}
      else if (!brother_node->is_overflow(old_node->size())) {
        if (NULL == (new_node_2 = alloc_node())) {
          ret = BTREE_NOMEM;
        } else if (parent_pos > 0) {
          ret = old_node->merge_child_cause_recursive_merge_with_left_brother(this->get_comp(), new_node_2,
                                                                              brother_node, pos,
                                                                              key, val);
        } else {
          ret = old_node->merge_child_cause_recursive_merge_with_right_brother(this->get_comp(), new_node_2,
                                                                               brother_node, pos,
                                                                               key, val);
        }
        new_node_1 = NULL;
      } else {
        if (NULL == (new_node_1 = alloc_node()) || NULL == (new_node_2 = alloc_node())) {
          ret = BTREE_NOMEM;
        } else if (parent_pos > 0) {
          ret = old_node->merge_child_cause_rebalance_with_left_brother(this->get_comp(), new_node_2,
                                                                        new_node_1, brother_node,
                                                                        pos, key, val);
        } else {
          ret = old_node->merge_child_cause_rebalance_with_right_brother(this->get_comp(), new_node_1,
                                                                         new_node_2, brother_node,
                                                                         pos, key, val);
        }
      }
      return ret;
    }
  };

  BtreeBase(INodeAllocator &node_allocator,
            IKVRCallback &kvr_callback): node_allocator_(node_allocator), kvr_callback_(kvr_callback)
  {}
  ~BtreeBase()
  {}

  int64_t size() const
  {
    return size_.value();
  }

  IKVRCallback &get_kvr_callback() { return kvr_callback_; }
  void print(const Node *root, FILE *file) const
  {
    if (NULL != root && NULL != file) {
      fprintf(file, "--------------------------\n");
      fprintf(file, "|root=%p node_size=%ld node_key_count=%d total_size=%ld\n", root, sizeof(Node),
              NODE_KEY_COUNT, size());
      root->print(file, 0);
    }
  }

  void destroy(Node *root)
  {
    BtreeBase *host = NULL;
    if (NULL != root && NULL != (host = reinterpret_cast<BtreeBase *>(root->host_))) {
      for (int i = 0; i < root->size(); i++) {
        if (!root->is_leaf()) {
          destroy(reinterpret_cast<Node *>(root->get_val(i)));
        } else {
          host->kvr_callback_.reclaim_key_value(&root->get_key(i), root->get_val(i));
          size_.inc(-1);
        }
      }
      host->node_allocator_.free_node(root);
    }
  }

  void destroy()
  {
    List reclaim_list;
    Node *p = NULL;
    get_retire_station().purge(reclaim_list);
    while (NULL != (p = reinterpret_cast<Node *>(reclaim_list.pop()))) {
      free_node(p);
      p = NULL;
    }
  }

  int set(Node **root, WriteHandle &handle, key_t key, val_t val, const bool overwrite)
  {
    int ret = BTREE_SUCCESS;
    Node *old_root = NULL;
    Node *new_root = NULL;
    handle.get_is_in_delete() = false;
    if (OB_ISNULL(root)) {
      ret = BTREE_INVAL;
    } else if (BTREE_SUCCESS != (ret = handle.acquire_ref())) {
      BTREE_LOG(ERROR, "acquire_ref fail, err=%d", ret);
    } else if (BTREE_SUCCESS != (ret = handle.find_path(old_root = CBT_AL(root), key))) {
      BTREE_LOG(ERROR, "path.search(%p)=>%d", root, ret);
    } else if (BTREE_SUCCESS != (ret = handle.insert_and_split_upward(key, val, overwrite,
                                                                      new_root = old_root))) {
      if (BTREE_DUPLICATE != ret && BTREE_EAGAIN != ret && BTREE_NOMEM != ret) {
        BTREE_LOG(ERROR, "insert_upward(%p)=>%d", val, ret);
      }
    } else if (old_root != new_root) {
      if (NULL != new_root) {
        new_root->set_is_root(true);
      }
      if (!CBT_CAS(root, old_root, new_root)) {
        ret = BTREE_EAGAIN;
      }
    }
    if (BTREE_SUCCESS == ret) {
      size_.inc(1);
    }
    handle.release_ref();
    handle.retire(ret);
    return ret;
  }

  int del(Node **root, WriteHandle &handle, key_t key, val_t &val)
  {
    int ret = BTREE_SUCCESS;
    Node *old_root = NULL;
    Node *new_root = NULL;
    handle.get_is_in_delete() = true;
    if (OB_ISNULL(root)) {
      ret = BTREE_INVAL;
    } else if (BTREE_SUCCESS != (ret = handle.acquire_ref())) {
      BTREE_LOG(ERROR, "acquire_ref fail, err=%d", ret);
    } else if (BTREE_SUCCESS != (ret = handle.find_path(old_root = CBT_AL(root), key))) {
      if (BTREE_NOENT != ret) {
        BTREE_LOG(ERROR, "path.search(%p)=>%d", root, ret);
      }
    } else if (BTREE_SUCCESS != (ret = handle.delete_and_merge_upward(key, val, new_root = old_root))) {
      if (BTREE_NOENT != ret && BTREE_EAGAIN != ret && BTREE_NOMEM != ret) {
        BTREE_LOG(ERROR, "delete_upward(root=%p, %p)=>%d", old_root, val, ret);
      }
    } else if (old_root != new_root) {
      if (NULL != new_root) {
        new_root->set_is_root(true);
      }
      if (!CBT_CAS(root, old_root, new_root)) {
        ret = BTREE_EAGAIN;
      }
    }
    if (BTREE_SUCCESS == ret) {
      size_.inc(-1);
    }
    handle.release_ref();
    handle.retire(ret);
    return ret;
  }

  int search(Node **root, ScanHandle &handle, key_t key)
  {
    int ret = BTREE_SUCCESS;
    if (OB_ISNULL(root)) {
      ret = BTREE_INVAL;
    } else if (BTREE_SUCCESS != (ret = handle.acquire_ref())) {
      BTREE_LOG(ERROR, "acquire_ref fail, err=%d", ret);
    } else if (BTREE_SUCCESS != (ret = handle.find_path(CBT_AL(root), key)))
    {}
    return ret;
  }

  int get(Node **root, GetHandle &handle, key_t key, val_t &val)
  {
    int ret = BTREE_SUCCESS;
    if (OB_ISNULL(root)) {
      ret = BTREE_INVAL;
    } else if (BTREE_SUCCESS != (ret = handle.acquire_ref())) {
      BTREE_LOG(ERROR, "acquire_ref fail, err=%d", ret);
    } else if (BTREE_SUCCESS != (ret = handle.get(CBT_AL(root), key, val)))
    {}
    return ret;
  }

  Node *alloc_node(const bool emergency)
  {
    Node *p = NULL;
    if (NULL != (p = (Node *)node_allocator_.alloc_node(emergency))) {
      p->reset();
      p->host_ = (void *)this;
    }
    return p;
  }

  void free_node(Node *p)
  {
    if (NULL != p) {
      int64_t deleted_pos = p->get_deleted_pos();
      BtreeBase *host = (BtreeBase *)p->host_;
      if (NULL != host) {
        if (deleted_pos != -1) {
          host->kvr_callback_.reclaim_key_value(&p->get_key(deleted_pos), p->get_val(deleted_pos));
        }
        host->node_allocator_.free_node(p);
        p = NULL;
      }
    }
  }

  void retire(List &retire_list, const int64_t retire_limit)
  {
    List reclaim_list;
    BaseNode *p = NULL;
    get_retire_station().retire(reclaim_list, retire_list, retire_limit);
    while (NULL != (p = reinterpret_cast<BaseNode *>(reclaim_list.pop()))) {
      free_node(static_cast<Node *>(p));
      p = NULL;
    }
  }

  static RetireStation &get_retire_station()
  {
    static RetireStation retire_station_;
    return retire_station_;
  }
private:
  TCCounter size_;
  INodeAllocator &node_allocator_;
  IKVRCallback &kvr_callback_;
  DISALLOW_COPY_AND_ASSIGN(BtreeBase);
};
}

#endif // OCEANBASE_CBTREE_BTREE_
