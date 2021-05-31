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

#ifndef  OCEANBASE_COMMON_HASH_HASHSET_
#define  OCEANBASE_COMMON_HASH_HASHSET_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <new>
#include <pthread.h>
#include "lib/hash/ob_hashutils.h"
#include "lib/hash/ob_hashtable.h"
#include "lib/hash/ob_serialization.h"

namespace oceanbase
{
namespace common
{
namespace hash
{

template <class _key_type>
struct HashSetTypes
{
  typedef HashMapPair<_key_type, HashNullObj> pair_type;
  typedef typename HashTableTypes<pair_type>::AllocType AllocType;
};

template <class _key_type,
          class _defendmode = ReadWriteDefendMode,
          class _hashfunc = hash_func<_key_type>,
          class _equal = equal_to<_key_type>,
          class _allocer = SimpleAllocer<typename HashSetTypes<_key_type>::AllocType>,
          template <class> class _bucket_array = NormalPointer>
class ObHashSet
{
  typedef typename HashSetTypes<_key_type>::pair_type pair_type;
  typedef ObHashSet<_key_type, _hashfunc, _equal, _allocer, _defendmode> hashset;
  typedef ObHashTable<_key_type, pair_type, _hashfunc, _equal, pair_first<pair_type>, _allocer, _defendmode, _bucket_array, oceanbase::common::ObMalloc>
  hashtable;
public:
  typedef typename hashtable::iterator iterator;
  typedef typename hashtable::const_iterator const_iterator;
public:
  ObHashSet() : bucket_allocer_(ObModIds::OB_HASH_BUCKET), ht_()
  {
  };
  ~ObHashSet()
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
  bool created() const
  {
    return ht_.created();
  }
  int64_t size() const
  {
    return ht_.size();
  };
  int create(int64_t bucket_num)
  {
    return ht_.create(cal_next_prime(bucket_num), &allocer_, &bucket_allocer_);
  };
  int create(int64_t bucket_num, int64_t bucket_mod_id, int64_t node_mod_id)
  {
    allocer_.set_mod_id(node_mod_id);
    bucket_allocer_.set_mod_id(bucket_mod_id);
    return ht_.create(cal_next_prime(bucket_num), &allocer_, &bucket_allocer_);
  };
  int create(int64_t bucket_num, _allocer *allocer)
  {
    return ht_.create(cal_next_prime(bucket_num), allocer, &bucket_allocer_);
  };
  int destroy()
  {
    // ret should be handle by caller.
    int ret = ht_.destroy();
    allocer_.clear();
    return ret;
  };
  int clear()
  {
    return ht_.clear();
  };
  void reuse()
  {
    clear();
  }
  // return:
  //   OB_HASH_EXIST node exists
  //   OB_HASH_NOT_EXIST node not exists
  //   error ocurried
  int exist_refactored(const _key_type &key) const
  {
    int ret = OB_SUCCESS;
    pair_type pair;
    ret = const_cast<hashtable &>(ht_).get_refactored(key, pair);
    if (OB_SUCCESS == ret) {
      ret = OB_HASH_EXIST;
    }
    return ret;
  };
  // flag: 0--not cover exists object, 1--cover exists object.
  // return:
  //   OB_SUCCESS insert or cover successfully
  //   OB_HASH_EXIST object exists(flag = 0)
  //   others     other errors
  int set_refactored(const _key_type &key)
  {
    pair_type pair(key, HashNullObj());
    return ht_.set_refactored(key, pair, 1);
  };

  int set_refactored_1(const _key_type &key, bool overwrite_key)
  {
    pair_type pair(key, HashNullObj());
    return ht_.set_refactored(key, pair, 1, 0, overwrite_key);
  };
  int erase_refactored(const _key_type &key)
  {
    int ret = OB_SUCCESS;
    ret = ht_.erase_refactored(key);
    return ret;
  };

  int64_t to_string(char *buffer, const int64_t length) const
  {
    int64_t pos = 0;
    const_iterator iter;
    for (iter = begin(); iter != end(); iter++) {
      databuff_printf(buffer, length, pos, "%s,", to_cstring(iter->first));
    }
    return pos;
  }

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
  _allocer allocer_;
  oceanbase::common::ObMalloc bucket_allocer_;
  hashtable ht_;

  DISALLOW_COPY_AND_ASSIGN(ObHashSet);
};
}//namespace hash
}//namespace common
}//namespace oceanbase
#endif //OCEANBASE_COMMON_HASH_HASHSET_
