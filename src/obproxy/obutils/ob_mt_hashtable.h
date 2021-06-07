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

#ifndef OBPROXY_MT_HASHTABLE_H
#define OBPROXY_MT_HASHTABLE_H

#include "lib/hash_func/ob_hash_func.h"
#include "utils/ob_proxy_lib.h"
#include "iocore/eventsystem/ob_buf_allocator.h"
#include "iocore/eventsystem/ob_event_system.h"
#include "lib/lock/ob_drw_lock.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

static const int64_t MT_HASHTABLE_PARTITION_BITS    = 6;
static const int64_t MT_HASHTABLE_PARTITIONS        = 1 << MT_HASHTABLE_PARTITION_BITS;
static const uint64_t MT_HASHTABLE_PARTITION_MASK   = MT_HASHTABLE_PARTITIONS - 1;
static const int64_t MT_HASHTABLE_MAX_CHAIN_AVG_LEN = 4;

template <class Key, class Value>
struct ObHashTableEntry
{
  static ObHashTableEntry *alloc()
  {
    return op_reclaim_alloc(ObHashTableEntry);
  }

  static void free(ObHashTableEntry *entry)
  {
    op_reclaim_free(entry);
    entry = NULL;
  }

  uint64_t hash_;
  Key key_;
  Value data_;
  ObHashTableEntry *next_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObHashTableEntry);
};

template <class Key, class Value>
class ObHashTableIteratorState
{
public:
  ObHashTableIteratorState() : cur_buck_(-1), ppcur_(NULL) { }
  ~ObHashTableIteratorState() { }

  int64_t cur_buck_;
  ObHashTableEntry<Key, Value> **ppcur_;
};

template <class Key, class Value>
class ObIMTHashTable
{
public:
  typedef ObHashTableIteratorState<Key, Value> IteratorState;
  typedef ObHashTableEntry<Key, Value> HashTableEntry;

  ObIMTHashTable(bool (*a_gc_func)(Value) = NULL,
                 void (*a_pre_gc_func)(void) = NULL)
  {
    gc_func = a_gc_func;
    pre_gc_func = a_pre_gc_func;
    buckets_ = NULL;
    cur_size_ = 0;
    bucket_num_ = 0;
  }

  ~ObIMTHashTable() { destroy(); }

  int init(const int64_t size)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(size <= 0)) {
      ret = common::OB_INVALID_ARGUMENT;
      PROXY_LOG(WARN, "invalid bucket_size", K(size), K(ret));
    } else if (NULL == buckets_) {
      bucket_num_ = size;
      buckets_ = static_cast<HashTableEntry **>(op_fixed_mem_alloc(bucket_num_ * sizeof(HashTableEntry *)));
      if (OB_ISNULL(buckets_)) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        PROXY_LOG(ERROR, "failed to allocate memory for hash table bucket", K(ret));
      } else {
        memset(buckets_, 0, bucket_num_ * sizeof(HashTableEntry *));
      }
    }
    return ret;
  }

  int64_t get_bucket_num() const
  {
    return bucket_num_;
  }

  int64_t get_cur_size() const
  {
    return cur_size_;
  }

  int64_t bucket_id(const uint64_t hash, const int64_t a_bucket_num) const
  {
    return ((hash >> MT_HASHTABLE_PARTITION_BITS) ^ hash) % a_bucket_num;
  }

  int64_t bucket_id(const uint64_t hash) const
  {
    return bucket_id(hash, bucket_num_);
  }

  void destroy()
  {
    HashTableEntry *tmp = NULL;
    if (NULL != buckets_) {
      for (int64_t i = 0; i < bucket_num_; ++i) {
        tmp = buckets_[i];
        while (NULL != tmp) {
          buckets_[i] = tmp->next_;
          HashTableEntry::free(tmp);
          tmp = buckets_[i];
        }
      }

      op_fixed_mem_free(buckets_, bucket_num_ * sizeof(HashTableEntry *));
      buckets_ = NULL;
    }
  }

  Value insert_entry(const uint64_t hash, const Key &key, Value data);
  Value remove_entry(const uint64_t hash, const Key &key);
  Value lookup_entry(const uint64_t hash, const Key &key);

  Value first_entry(const int64_t bucket_id, IteratorState &s);
  static Value next_entry(IteratorState &s);
  static Value cur_entry(IteratorState &s);
  Value remove_entry(IteratorState &s);

  void gc(void)
  {
    if (NULL != gc_func) {
      if ( NULL != pre_gc_func) {
        pre_gc_func();
      }

      HashTableEntry *cur = NULL;
      HashTableEntry *prev = NULL;
      HashTableEntry *next = NULL;
      for (int64_t i = 0; i < bucket_num_; ++i) {
        cur = buckets_[i];
        prev = NULL;
        next = NULL;
        while (NULL != cur) {
          next = cur->next_;
          if (gc_func(cur->data_)) {
            if (NULL != prev) {
              prev->next_ = next;
            } else {
              buckets_[i] = next;
            }

            HashTableEntry::free(cur);
            --cur_size_;
          } else {
            prev = cur;
          }

          cur = next;
        } // end while
      } // end for
    } // end if (NULL != gc_func)
  }

  int resize(const int64_t size)
  {
    int ret = common::OB_SUCCESS;
    int64_t new_bucket_num = size;
    HashTableEntry **new_buckets =
      static_cast<HashTableEntry **>(op_fixed_mem_alloc(new_bucket_num * sizeof(HashTableEntry *)));
    if (OB_ISNULL(new_buckets)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      PROXY_LOG(ERROR, "fail to alloc memory for HashTableEntry", K(ret));
    } else {
      memset(new_buckets, 0, new_bucket_num * sizeof(HashTableEntry *));

      HashTableEntry *cur = NULL;
      HashTableEntry *next = NULL;
      int64_t new_id = 0;
      for (int64_t i = 0; i < bucket_num_; ++i) {
        cur = buckets_[i];
        next = NULL;
        while (NULL != cur) {
          next = cur->next_;
          new_id = bucket_id(cur->hash_, new_bucket_num);
          cur->next_ = new_buckets[new_id];
          new_buckets[new_id] = cur;
          cur = next;
        }

        buckets_[i] = NULL;
      }

      op_fixed_mem_free(buckets_, bucket_num_ * sizeof(HashTableEntry *));
      buckets_ = new_buckets;
      bucket_num_ = new_bucket_num;
    }
    return ret;
  }

private:
  ObIMTHashTable();

  bool (*gc_func)(Value);
  void (*pre_gc_func)(void);

private:
  HashTableEntry **buckets_;
  int64_t cur_size_;
  int64_t bucket_num_;
  DISALLOW_COPY_AND_ASSIGN(ObIMTHashTable);
};

template <class Key, class Value>
inline Value ObIMTHashTable<Key, Value>::insert_entry(const uint64_t hash, const Key &key, Value data)
{
  Value ret = static_cast<Value>(0);
  int64_t id = bucket_id(hash);
  HashTableEntry *cur = buckets_[id];

  while (NULL != cur && (hash != cur->hash_ || cur->key_ != key)) {
    cur = cur->next_;
  }

  if (NULL != cur) {
    if (data == cur->data_) {
      // return NULL;
    } else {
      ret = cur->data_;
      cur->data_ = data;
      cur->key_ = key;
      // potential memory leak, need to check the return value by the caller
    }
  } else {
    HashTableEntry *new_entry = HashTableEntry::alloc();
    if (OB_LIKELY(NULL != new_entry)) {
      new_entry->hash_ = hash;
      new_entry->key_ = key;
      new_entry->data_ = data;
      new_entry->next_ = buckets_[id];
      buckets_[id] = new_entry;
      ++cur_size_;
      if (cur_size_ / bucket_num_ > MT_HASHTABLE_MAX_CHAIN_AVG_LEN) {
        gc();
        if (cur_size_ / bucket_num_ > MT_HASHTABLE_MAX_CHAIN_AVG_LEN) {
          if (OB_UNLIKELY(common::OB_SUCCESS != resize(bucket_num_ * 2))) {
            PROXY_LOG(WARN, "fail to resize buckets");
          }
        }
      }
    }
  }

  return ret;
}

template <class Key, class Value>
inline Value ObIMTHashTable<Key, Value>::remove_entry(const uint64_t hash, const Key &key)
{
  int64_t id = bucket_id(hash);
  Value ret = static_cast<Value>(0);
  HashTableEntry *cur = buckets_[id];
  HashTableEntry *prev = NULL;

  while (NULL != cur && (hash != cur->hash_ || cur->key_ != key)) {
    prev = cur;
    cur = cur->next_;
  }

  if (NULL != cur) {
    if (NULL != prev) {
      prev->next_ = cur->next_;
    } else {
      buckets_[id] = cur->next_;
    }

    ret = cur->data_;
    HashTableEntry::free(cur);
    cur = NULL;
    --cur_size_;
  }

  return ret;
}

template <class Key, class Value>
inline Value ObIMTHashTable<Key, Value>::lookup_entry(const uint64_t hash, const Key &key)
{
  int64_t id = bucket_id(hash);
  Value ret = static_cast<Value>(0);
  HashTableEntry *cur = buckets_[id];

  while (NULL != cur && (hash != cur->hash_ || cur->key_ != key)) {
    cur = cur->next_;
  }

  if (NULL != cur) {
    ret = cur->data_;
  }

  return ret;
}

template <class Key, class Value>
inline Value ObIMTHashTable<Key, Value>::first_entry(const int64_t bucket_id, IteratorState &s)
{
  Value ret = static_cast<Value>(0);
  if (OB_LIKELY(bucket_id < bucket_num_)) {
    s.cur_buck_ = bucket_id;
    s.ppcur_ = &(buckets_[bucket_id]);
    if (NULL != *(s.ppcur_)) {
      ret = (*(s.ppcur_))->data_;
    }
  }
  return ret;
}

template <class Key, class Value>
inline Value ObIMTHashTable<Key, Value>::next_entry(IteratorState &s)
{
  Value ret = static_cast<Value>(0);
  if (NULL != (*(s.ppcur_))) {
    s.ppcur_ = &((*(s.ppcur_))->next_);
    if (NULL != *(s.ppcur_)) {
      ret = (*(s.ppcur_))->data_;
    }
  }
  return ret;
}

template <class Key, class Value>
inline Value ObIMTHashTable<Key, Value>::cur_entry(IteratorState &s)
{
  Value ret = static_cast<Value>(0);
  if (NULL != *(s.ppcur_)) {
    ret = (*(s.ppcur_))->data_;
  }
  return ret;
}

template <class Key, class Value>
inline Value ObIMTHashTable<Key, Value>::remove_entry(IteratorState &s)
{
  Value ret = static_cast<Value>(0);
  HashTableEntry *entry = *(s.ppcur_);
  if (NULL != entry) {
    ret = entry->data_;
    *(s.ppcur_) = entry->next_;
    HashTableEntry::free(entry);
    entry = NULL;
    --cur_size_;
  }

  return ret;
}

template <class Key, class Value>
class ObMTHashTable
{
public:
  typedef ObHashTableIteratorState<Key, Value> IteratorState;
  typedef ObHashTableEntry<Key, Value> HashTableEntry;
  typedef ObIMTHashTable<Key, Value> IMTHashTable;

  ObMTHashTable() : is_inited_(false)
  {
    memset(&locks_, 0, sizeof(locks_));
    memset(&rw_locks_, 0, sizeof(rw_locks_));
    memset(&hash_tables_, 0, sizeof(hash_tables_));
  }

  ~ObMTHashTable()
  {
    if (OB_LIKELY(is_inited_)) {
      for (int64_t i = 0; i < MT_HASHTABLE_PARTITIONS; ++i) {
        locks_[i].release();
        if (NULL != hash_tables_[i]) {
          hash_tables_[i]->destroy();
          op_free(hash_tables_[i]);
          hash_tables_[i] = NULL;
        }
      }
    }
  }

  int init(const int64_t size, const event::ObLockStats lock_stats = event::COMMON_LOCK,
           bool (*gc_func)(Value) = NULL, void (*pre_gc_func)(void) = NULL)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(is_inited_)) {
      ret = common::OB_INIT_TWICE;
      PROXY_LOG(WARN, "init twice", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < MT_HASHTABLE_PARTITIONS; ++i) {
        if (OB_ISNULL(locks_[i] = event::new_proxy_mutex(lock_stats))) {
          ret = common::OB_ALLOCATE_MEMORY_FAILED;
          PROXY_LOG(ERROR, "fail to alloc mem for proxymutex", K(ret));
        } else if (OB_ISNULL(hash_tables_[i] = op_alloc_args(IMTHashTable, gc_func, pre_gc_func))) {
          ret = common::OB_ALLOCATE_MEMORY_FAILED;
          PROXY_LOG(ERROR, "fail to alloc mem for hash table", K(ret));
        } else if (OB_FAIL(hash_tables_[i]->init(size))) {
          PROXY_LOG(WARN, "failed to init hash table", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        is_inited_ = true;
      }
    }
    return ret;
  }

  event::ObProxyMutex *lock_for_key(const uint64_t hash)
  {
    return locks_[part_num(hash)];
  }

  event::ObProxyMutex *lock_for_part(const int64_t part_idx)
  {
    event::ObProxyMutex *mutex = NULL;
    if ((part_idx >= 0) && (part_idx < get_sub_part_count())) {
      mutex = locks_[part_idx];
    }
    return mutex;
  }

  common::DRWLock &rw_lock_for_key(const uint64_t hash)
  {
    return rw_locks_[part_num(hash)];
  }

  common::DRWLock *rw_lock_for_part(const int64_t part_idx)
  {
    common::DRWLock *ret = NULL;
    if ((part_idx >= 0) && (part_idx < get_sub_part_count())) {
      ret = &rw_locks_[part_idx];
    }
    return ret;
  }

  int64_t get_part_cur_size(const uint64_t hash)
  {
    return hash_tables_[part_num(hash)]->get_cur_size();
  }

  int64_t get_sub_part_count() const
  {
    return MT_HASHTABLE_PARTITIONS;
  }

  int64_t part_num(const uint64_t hash) const
  {
    return static_cast<int64_t>(hash & MT_HASHTABLE_PARTITION_MASK);
  }

  void gc(const uint64_t hash) { hash_tables_[part_num(hash)]->gc(); }

  //return old entry if existed
  Value insert_entry(const uint64_t hash, const Key &key, Value data)
  {
    return hash_tables_[part_num(hash)]->insert_entry(hash, key, data);
  }

  Value remove_entry(const uint64_t hash, const Key &key)
  {
    return hash_tables_[part_num(hash)]->remove_entry(hash, key);
  }

  Value lookup_entry(const uint64_t hash, const Key &key)
  {
    return hash_tables_[part_num(hash)]->lookup_entry(hash, key);
  }

  Value first_entry(const int64_t part_id, IteratorState &s)
  {
    Value ret = static_cast<Value>(0);
    bool found = false;
    for (int64_t i = 0; !found && i < hash_tables_[part_id]->get_bucket_num(); ++i) {
      ret = hash_tables_[part_id]->first_entry(i, s);
      if (static_cast<Value>(0) != ret) {
        found = true;
      }
    }

    return ret;
  }

  Value cur_entry(const int64_t part_id, IteratorState &s)
  {
    Value ret = IMTHashTable::cur_entry(s);
    if (static_cast<Value>(0) == ret) {
      ret = next_entry(part_id, s);
    }

    return ret;
  };

  Value next_entry(const int64_t part_id, IteratorState &s)
  {
    Value ret = IMTHashTable::next_entry(s);
    if (static_cast<Value>(0) == ret) {
      bool found = false;
      for (int64_t i = s.cur_buck_ + 1; !found && i < hash_tables_[part_id]->get_bucket_num(); ++i) {
        ret = hash_tables_[part_id]->first_entry(i, s);
        if (static_cast<Value>(0) != ret) {
          found = true;
        }
      }
    }

    return ret;
  }

  Value remove_entry(const int64_t part_id, IteratorState &s)
  {
    return hash_tables_[part_id]->remove_entry(s);
  }

private:
  bool is_inited_;
  IMTHashTable *hash_tables_[MT_HASHTABLE_PARTITIONS];
  common::ObPtr<event::ObProxyMutex> locks_[MT_HASHTABLE_PARTITIONS];
  common::DRWLock rw_locks_[MT_HASHTABLE_PARTITIONS];
};

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_MT_HASHTABLE_H
