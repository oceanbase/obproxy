/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIB_HASH_OB_BUILD_IN_HASHMAP_FOR_REF_COUNT_
#define OCEANBASE_LIB_HASH_OB_BUILD_IN_HASHMAP_FOR_REF_COUNT_

#include "lib/atomic/ob_atomic.h"
#include "lib/hash_func/ob_hash_func.h"
#include "lib/hash/ob_hashutils.h"
#include "lib/list/ob_intrusive_list.h"
#include "lib/lock/ob_mutex.h"

namespace oceanbase
{
namespace common
{
namespace hash
{

// There may be objects with a reference count of 0
// and it is necessary to carefully check the concurrency status when modifying the code
template <typename H, int64_t bucket_num = 16>
class ObBuildInHashMapForRefCount
{
public:
  // Make embedded types easier to use by importing them to the class namespace.
  typedef H Hasher; // Rename and promote.
  typedef typename Hasher::Key Key; // Key type.
  typedef typename Hasher::Value Value; // Stored value (element) type.
  typedef typename Hasher::ListHead ListHead; // Anchor for value chain

  struct ObBuildInBucket
  {
    ListHead chain_; // Chain of elements.
    mutable lib::ObMutex lock_;
    ObBuildInBucket() : chain_(), lock_() {}
  } CACHE_ALIGNED;

  typedef int (*TRAVERSE_FUNC)(Value&, va_list args);
public:
  ObBuildInHashMapForRefCount() : buckets_(), count_(0) {}
  // Remove all values from the map.
  // The values are not cleaned up. The values are not touched in this method,
  // therefore it is safe to destroy them first and then @c clear this map.
  void reset()
  {
    memset(this, 0, sizeof(ObBuildInHashMapForRefCount));
  }

  // Get the number of elements in the map.
  int64_t count() const { return ATOMIC_LOAD64(&count_); }

  // put a key value pair into HashMap
  // @retval OB_SUCCESS for success
  // @retval OB_HASH_EXIST when the value's pointer already exist
  int set_refactored(Value *value)
  {
    int ret = OB_SUCCESS;
    Key key = Hasher::key(value);

    ObBuildInBucket &bucket = buckets_[Hasher::hash(key) % bucket_num];
    lib::ObMutexGuard guard(bucket.lock_);
    if (! bucket.chain_.in(value)) {
      bucket.chain_.push(value);
      ATOMIC_INC(&count_);
    } else {
      ret = OB_HASH_EXIST;
    }

    if (OB_SUCC(ret)) {
      Hasher::inc_ref(value);
    }
    return ret;
  }

  // put a key value pair into HashMap
  // @retval OB_SUCCESS for success
  // @retval OB_HASH_EXIST when the value's key already exist
  int unique_set(Value *value)
  {
    int ret = OB_SUCCESS;
    Key key = Hasher::key(value);
    ObBuildInBucket &bucket = buckets_[Hasher::hash(key) % bucket_num];
    lib::ObMutexGuard guard(bucket.lock_);
    if (!bucket.chain_.in(value)) {
      Value *v = bucket.chain_.head_;
      while (NULL != v && ((!Hasher::equal(key, Hasher::key(v)) && (v != value)) || 0 == Hasher::get_ref(v))) {
        v = ListHead::next(v);
      }
      if (NULL == v) {
        bucket.chain_.push(value);
        ATOMIC_INC(&count_);
      } else {
        ret = OB_HASH_EXIST;
      }
    } else {
      ret = OB_HASH_EXIST;
    }
    if (OB_SUCC(ret)) {
      Hasher::inc_ref(value);
    }
    return ret;
  }

  // @retval OB_SUCCESS for success
  // @retval OB_HASH_NOT_EXIST for key not exist
  int get_refactored(Key key, Value *&value)
  {
    int ret = OB_HASH_NOT_EXIST;

    const ObBuildInBucket &bucket = buckets_[Hasher::hash(key) % bucket_num];
    lib::ObMutexGuard guard(bucket.lock_);
    Value *v = bucket.chain_.head_;
    while (NULL != v && (!Hasher::equal(key, Hasher::key(v)) || 0 == Hasher::get_ref(v))) {
      v = ListHead::next(v);
    }
    if (NULL != (value = v)) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      while (true) {
        int64_t ref = Hasher::get_ref(v);
        if(ref > 0) {
          if (!Hasher::bcas_ref(value, ref, ref + 1)) {
            PAUSE();
          } else {
            break;
          }
        } else {
          ret = OB_HASH_NOT_EXIST;
          value = NULL;
          break;
        }
      }
    }
    return ret;
  }

  void remove(Value* value)
  {
    if (NULL != value) {
      Key key = Hasher::key(value);
      ObBuildInBucket &bucket = buckets_[Hasher::hash(key) % bucket_num];
      lib::ObMutexGuard guard(bucket.lock_);
      bucket.chain_.remove(value);
      ATOMIC_DEC(&count_);

      Hasher::destroy(value);
    }
  }

  int traverse_map(TRAVERSE_FUNC func, ...) const
  {
    int ret = OB_SUCCESS;

    va_list args, args_copy;
    va_start(args, func);
    for (int i = 0; OB_SUCC(ret) && i < bucket_num; ++i) {
      const ObBuildInBucket &bucket = buckets_[i];
      if (OB_NOT_NULL(bucket.chain_.head_)) {
        lib::ObMutexGuard guard(bucket.lock_);
        Value *v = bucket.chain_.head_;
        while (NULL != v) {
          va_copy(args_copy, args);
          if (OB_FAIL(func(*v, args_copy))) {
            OB_LOG(WDIAG, "fail to handle", KP(v), K(ret)); // if bucket is in chain, must be non-empty.
          }
          v = ListHead::next(v);
          va_end(args_copy);
        }
      }
    }
    va_end(args);
    return ret;
  }

private:
  ObBuildInBucket buckets_[bucket_num];
  int64_t count_; // of elements stored in the map.
};

} // namespace hash
} // namespace common
} // namespace oceanbase
#endif //OCEANBASE_LIB_HASH_OB_BUILD_IN_HASHMAP_FOR_REF_COUNT_
