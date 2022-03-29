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

#ifndef  OCEANBASE_COMMON_HASH_HASHMAP_
#define  OCEANBASE_COMMON_HASH_HASHMAP_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <new>
#include <pthread.h>
#include "lib/hash/ob_hashutils.h"
#include "lib/hash/ob_hashtable.h"
#include "lib/hash/ob_serialization.h"
#include "lib/allocator/ob_allocator.h"

namespace oceanbase
{
namespace common
{
namespace hash
{
template <class _key_type, class _value_type>
struct HashMapTypes
{
  typedef HashMapPair<_key_type, _value_type> pair_type;
  typedef typename HashTableTypes<pair_type>::AllocType AllocType;
};

template <class _key_type, class _value_type>
struct hashmap_preproc
{
  typedef typename HashMapTypes<_key_type, _value_type>::pair_type pair_type;
  _value_type &operator()(pair_type &pair) const
  {
    return pair.second;
  };
  char buf[0];
};

template <class _key_type,
          class _value_type,
          class _defendmode = ReadWriteDefendMode,
          class _hashfunc = hash_func<_key_type>,
          class _equal = equal_to<_key_type>,
          class _allocer = SimpleAllocer<typename HashMapTypes<_key_type, _value_type>::AllocType>,
          template <class> class _bucket_array = NormalPointer,
          class _bucket_allocer = oceanbase::common::ObMalloc>
class ObHashMap
{
  typedef typename HashMapTypes<_key_type, _value_type>::pair_type pair_type;
  typedef ObHashMap<_key_type, _value_type, _defendmode, _hashfunc, _equal, _allocer, _bucket_array, _bucket_allocer>
  hashmap;
  typedef ObHashTable<_key_type, pair_type, _hashfunc, _equal, pair_first<pair_type>, _allocer, _defendmode, _bucket_array, _bucket_allocer>
  hashtable;
  typedef hashmap_preproc<_key_type, _value_type> preproc;
public:
  typedef typename hashtable::iterator iterator;
  typedef typename hashtable::const_iterator const_iterator;
public:
  ObHashMap() : ht_()
  {
    // default
    ObMemAttr attr(OB_SERVER_TENANT_ID, ObModIds::OB_HASH_BUCKET);
    bucket_allocer_.set_attr(attr);
  };
  ~ObHashMap()
  {
  };
public:
  iterator begin()
  {
    return ht_.begin();
  };
  const_iterator begin() const
  {
    return ht_.begin();
  };
  iterator end()
  {
    return ht_.end();
  };
  const_iterator end() const
  {
    return ht_.end();
  };
  int64_t size() const
  {
    return ht_.size();
  };
  inline bool created() const
  {
    return ht_.created();
  }

  int create(int64_t bucket_num, int64_t bucket_mod_id, int64_t node_mod_id = ObModIds::OB_HASH_NODE)
  {
    allocer_.set_mod_id(node_mod_id);
    bucket_allocer_.set_mod_id(bucket_mod_id);
    return ht_.create(cal_next_prime(bucket_num), &allocer_, &bucket_allocer_);
  };
  int create(int64_t bucket_num, _allocer *allocer, int64_t bucket_mod_id,
             int64_t node_mod_id = ObModIds::OB_HASH_NODE)
  {
    allocer_.set_mod_id(node_mod_id);
    bucket_allocer_.set_mod_id(bucket_mod_id);
    return ht_.create(cal_next_prime(bucket_num), allocer, &bucket_allocer_);
  };
  int create(int64_t bucket_num, _allocer *allocer, _bucket_allocer *bucket_allocer)
  {
    return ht_.create(cal_next_prime(bucket_num), allocer, bucket_allocer);
  };
  int destroy()
  {
    int ret = ht_.destroy();
    allocer_.clear();
    return ret;
  };
  int clear()
  {
    int ret = ht_.clear();
    //allocer_.clear();
    return ret;
  };
  int reuse()
  {
    return clear();
  };
  _allocer &get_local_allocer() {return allocer_;};
  _bucket_allocer &get_local_bucket_allocer() {return bucket_allocer_;};
  inline int get_refactored(const _key_type &key, _value_type &value, const int64_t timeout_us = 0) const
  {
    int ret = OB_SUCCESS;
    pair_type pair(key, value);
    if (OB_SUCC(const_cast<hashtable &>(ht_).get_refactored(key, pair, timeout_us))) {
      value = pair.second;
    }
    return ret;
  };
  inline const _value_type *get(const _key_type &key) const
  {
    const _value_type *ret = NULL;
    const pair_type *pair = NULL;
    if (OB_SUCCESS == const_cast<hashtable &>(ht_).get_refactored(key, pair)
        && NULL != pair) {
      ret = &(pair->second);
    }
    return ret;
  };
   // flag: 0 shows that do not cover existing object
  inline int set_refactored(const _key_type &key, const _value_type &value, int flag = 0,
                 int broadcast = 0, int overwrite_key = 0)
  {
    pair_type pair(key, value);
    return ht_.set_refactored(key, pair, flag, broadcast, overwrite_key);
  };
  template <class _callback>
  int atomic_refactored(const _key_type &key, _callback &callback)
  {
    //return ht_.atomic(key, callback, preproc_);
    return ht_.atomic_refactored(key, callback);
  };

  // thread safe scan, will add read lock to the bucket, the modification to the value is forbidden
  //
  // @param callback
  // @return OB_SUCCESS for success, other for error
  template<class _callback>
  int foreach_refactored(_callback &callback) const
  {
    return ht_.foreach_refactored(callback);
  }
  int erase_refactored(const _key_type &key, _value_type *value = NULL)
  {
    int ret = OB_SUCCESS;
    pair_type pair;
    if (NULL != value) {
      ret = ht_.erase_refactored(key, &pair);
      *value = pair.second;
    } else {
      ret = ht_.erase_refactored(key);
    }
    return ret;
  };
  template <class _archive>
  int serialization(_archive &archive)
  {
    return ht_.serialization(archive);
  };
  template <class _archive>
  int deserialization(_archive &archive)
  {
    return ht_.deserialization(archive, &allocer_);
  };
private:
  preproc preproc_;
  _allocer allocer_;
  _bucket_allocer bucket_allocer_;
  hashtable ht_;

  DISALLOW_COPY_AND_ASSIGN(ObHashMap);
};

template <class _key_type,
         class _value_type,
         class _defendmode = ReadWriteDefendMode,
         class _hashfunc = hash_func<_key_type>,
         class _equal = equal_to<_key_type>,
         class _allocer = SimpleAllocer<typename HashMapTypes<_key_type, _value_type>::AllocType>,
         template <class> class _bucket_array = NormalPointer,
         class _bucket_allocer = oceanbase::common::ObMalloc>
class ObHashMapWrapper
{
  typedef ObHashMapWrapper<_key_type, _value_type, _defendmode, _hashfunc, _equal, _allocer, _bucket_array, _bucket_allocer>
    OB_HASH_MAP_WRAPPER;
  typedef ObHashMap<_key_type, _value_type, _defendmode, _hashfunc, _equal, _allocer, _bucket_array, _bucket_allocer>
    OB_HASH_MAP;
public:
  typedef typename OB_HASH_MAP::iterator iterator;
  typedef typename OB_HASH_MAP::const_iterator const_iterator;

public:
  ObHashMapWrapper() : is_inited_(false), bucket_num_(-1), bucket_mod_id_(0), node_mod_id_(0),
                       allocer_(NULL), bucket_allocer_(NULL), hash_map_() {}
  ~ObHashMapWrapper() {
    hash_map_.destroy();
  }

  int reuse()
  {
    return hash_map_.reuse();
  }

  int init(int64_t bucket_num, int64_t bucket_mod_id, int64_t node_mod_id = ObModIds::OB_HASH_NODE) {
    int ret = OB_SUCCESS;

    if (!is_inited_) {
      bucket_num_ = bucket_num;
      bucket_mod_id_ = bucket_mod_id;
      node_mod_id_ = node_mod_id;

      if (OB_SUCC(hash_map_.create(bucket_num, bucket_mod_id, node_mod_id))) {
        is_inited_ = true;
      }
    }

    return ret;
  }

  int init(int64_t bucket_num, _allocer *allocer, int64_t bucket_mod_id,
           int64_t node_mod_id = ObModIds::OB_HASH_NODE) {
    int ret = OB_SUCCESS;

    if (!is_inited_) {
      bucket_num_ = bucket_num;
      allocer_ = allocer;
      bucket_mod_id_ = bucket_mod_id;
      node_mod_id_ = node_mod_id;

      if (OB_SUCC(hash_map_.create(bucket_num, allocer, bucket_mod_id, node_mod_id))) {
        is_inited_ = true;
      }
    }

    return ret;
  }

  int init(int64_t bucket_num, _allocer *allocer, _bucket_allocer *bucket_allocer) {
    int ret = OB_SUCCESS;

    if (!is_inited_) {
      bucket_num_ = bucket_num;
      allocer_ = allocer;
      bucket_allocer_ = bucket_allocer;

      if (OB_SUCC(hash_map_.create(bucket_num, allocer, bucket_allocer))) {
        is_inited_ = true;
      }
    }

    return ret;
  }

  int init(const OB_HASH_MAP_WRAPPER &hash_map_wrapper) {
    int ret = OB_SUCCESS;

    if (!is_inited_) {
      bucket_num_ = hash_map_wrapper.get_bucket_num();
      bucket_mod_id_ = hash_map_wrapper.get_bucket_mod_id();
      node_mod_id_ = hash_map_wrapper.get_node_mod_id();
      allocer_ = hash_map_wrapper.get_allocer();
      bucket_allocer_ = hash_map_wrapper.get_bucket_allocer();

      if (NULL != allocer_ && NULL != bucket_allocer_) {
        ret = hash_map_.create(bucket_num_, allocer_, bucket_allocer_);
      } else if (NULL != allocer_) {
        ret = hash_map_.create(bucket_num_, allocer_, bucket_mod_id_, node_mod_id_);
      } else {
        ret = hash_map_.create(bucket_num_, bucket_mod_id_, node_mod_id_);
      }

      if (OB_SUCC(ret)) {
        is_inited_ = true;
      }
    }

    return ret;
  }

  int assign(const OB_HASH_MAP_WRAPPER &hash_map_wrapper) {
    int ret = OB_SUCCESS;
    if (!is_inited_) {
      ret = init(hash_map_wrapper);
    }

    if (OB_SUCC(ret)) {
      const OB_HASH_MAP &hash_map = hash_map_wrapper.get_hash_map();
      const_iterator iter = hash_map.begin();
      const_iterator end = hash_map.end();
      for (; OB_SUCC(ret) && iter !=end; iter++) {
        ret = hash_map_.set_refactored(iter->first, iter->second);
      }
    }

    return ret;
  }

  int64_t get_bucket_num() const { return bucket_num_; }
  int64_t get_bucket_mod_id() const { return bucket_mod_id_; }
  int64_t get_node_mod_id() const { return node_mod_id_; }
  _allocer *get_allocer() const { return allocer_; }
  _bucket_allocer *get_bucket_allocer() const { return bucket_allocer_; }
  const OB_HASH_MAP &get_hash_map() const { return hash_map_; }
  OB_HASH_MAP &get_hash_map() { return hash_map_; }

  TO_STRING_KV(K_(is_inited), K_(bucket_num), K_(bucket_mod_id),
               K_(node_mod_id), KP_(allocer), KP_(bucket_allocer));

private:
  bool is_inited_;
  int64_t bucket_num_;
  int64_t bucket_mod_id_;
  int64_t node_mod_id_;
  _allocer *allocer_;
  _bucket_allocer *bucket_allocer_;
  OB_HASH_MAP hash_map_;
};

}//namespace hash
}//namespace common
}//namespace oceanbase
#endif //OCEANBASE_COMMON_HASH_HASHMAP_
