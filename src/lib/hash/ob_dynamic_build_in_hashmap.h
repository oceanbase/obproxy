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

#ifndef OCEANBASE_LIB_HASH_OB_DYNAMIC_BUILD_IN_HASHMAP_
#define OCEANBASE_LIB_HASH_OB_DYNAMIC_BUILD_IN_HASHMAP_

#include "lib/hash_func/ob_hash_func.h"
#include "lib/hash/ob_hashutils.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{
namespace hash
{
// thread-unsafe
template <typename H,
          int64_t init_bucket_num = 8,
          int64_t expand_threshold = 2,  // (count / hash_bucket) >= expand_threshold, will expand
          int64_t shrink_threshold = 16>  // (count / hash_bucket) <= shrink_threshold, will shrink

class ObDynamicBuildInHashMap
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
    ObBuildInBucket() : chain_() {}
  };

  // Standard iterator for walking the map.
  // This iterates over all elements.
  // @internal Iterator is end if value_ is NULL.
  struct iterator
  {
    Value *value_; // Current location.
    ObBuildInBucket *bucket_; // Current bucket;
    const ObDynamicBuildInHashMap * map_;

    iterator() : value_(NULL), bucket_(NULL), map_(NULL) {}
    iterator &operator ++ ()
    {
      if (NULL != value_) {
        if (NULL == (value_ = ListHead::next(value_))) { // end of bucket, next bucket.
          for (; (bucket_ <=  map_->last_bucket()) && OB_ISNULL(value_);) {
            ++bucket_;
            if (bucket_ <=  map_->last_bucket()) {
              value_ = bucket_->chain_.head_;
            } else {
              bucket_ = NULL;
              value_ = NULL;
              break;
            }
          }
        }
      }
      return *this;
    }
    Value &operator * () { return *value_; }
    Value *operator -> () { return value_; }
    bool operator == (iterator const &that) { return map_ == that.map_
                                                     && bucket_ == that.bucket_
                                                     && value_ == that.value_; }
    bool operator != (iterator const &that) { return !(*this == that); }
    int64_t to_string(char *buf, const int64_t buf_len) const {
      int64_t pos = 0;
      J_OBJ_START();
      J_KV(KP_(value), KP_(bucket), KP_(map));
      J_OBJ_END();
      return pos;
    }
  protected:
    // Internal iterator constructor.
    iterator(ObBuildInBucket *b, Value *v, const ObDynamicBuildInHashMap *m) : value_(v), bucket_(b), map_(m) {}
    friend class ObDynamicBuildInHashMap;
  };

public:
  ObDynamicBuildInHashMap() : count_(0), bucket_num_(init_bucket_num), buckets_(default_buckets_) {
    MEMSET(default_buckets_, 0, sizeof(default_buckets_));
  }

  ~ObDynamicBuildInHashMap() {
    if (buckets_ != default_buckets_) {
      ob_free(buckets_);
    }
  }
  // Remove all values from the map.
  // The values are not cleaned up. The values are not touched in this method,
  // therefore it is safe to destroy them first and then @c clear this map.
  void reset()
  {
    // free pointer
    if (buckets_ != NULL
        && buckets_ != default_buckets_) {
      ob_free(buckets_);
    }
    MEMSET(this, 0, sizeof(ObDynamicBuildInHashMap));
    buckets_ = default_buckets_;
    bucket_num_ = init_bucket_num;
  }

  // Get the number of elements in the map.
  int64_t count() const { return count_; }
  ObBuildInBucket * last_bucket() const { return buckets_ + bucket_num_ - 1; }

  iterator begin()
  {
    // Get the first non-empty bucket, if any.
    ObBuildInBucket *bucket = buckets_;
    Value *value = bucket->chain_.head_;
    for (; (bucket <= last_bucket()) && OB_ISNULL(value);) {
      ++bucket;
      if (bucket <= last_bucket()) {
        value = bucket->chain_.head_;
      } else {
        bucket = NULL;
        value = NULL;
        break;
      }
    }
    return (bucket <= last_bucket() && OB_NOT_NULL(value))
           ? iterator(bucket, value, this) : end();
  }

  iterator end() { return iterator(NULL, NULL, this); }


  // put a key value pair into HashMap
  // @retval OB_SUCCESS for success
  // @retval OB_HASH_EXIST when key already exist
  int set_refactored(Value *value)
  {
    int ret = OB_SUCCESS;

    try_expand_before_insert();
    Key key = Hasher::key(value);
    ObBuildInBucket &bucket = buckets_[Hasher::hash(key) % bucket_num_];

    if (! bucket.chain_.in(value)) {
      bucket.chain_.push(value);
      ++count_;
    } else {
      ret = OB_HASH_EXIST;
    }

    return ret;
  }

  // put a key value pair into HashMap
  // @retval OB_SUCCESS for success
  // @retval OB_HASH_EXIST when the value's key already exist
  int unique_set(Value *value)
  {
    int ret = OB_SUCCESS;

    try_expand_before_insert();
    Key key = Hasher::key(value);
    ObBuildInBucket &bucket = buckets_[Hasher::hash(key) % bucket_num_];
    if (!bucket.chain_.in(value)) {
      Value *v = bucket.chain_.head_;
      while (NULL != v && !Hasher::equal(key, Hasher::key(v)) && (v != value)) {
        v = ListHead::next(v);
      }
      if (NULL == v) {
        bucket.chain_.push(value);
        ++count_;
      } else {
        ret = OB_HASH_EXIST;
      }
    } else {
      ret = OB_HASH_EXIST;
    }
    return ret;
  }

  // @retval OB_SUCCESS for success
  // @retval OB_HASH_NOT_EXIST for key not exist
  int get_refactored(Key key, Value *&value) const
  {
    int ret = OB_HASH_NOT_EXIST;
    const ObBuildInBucket &bucket = buckets_[Hasher::hash(key) % bucket_num_];
    Value *v = bucket.chain_.head_;
    while (NULL != v && ! Hasher::equal(key, Hasher::key(v))) {
      v = ListHead::next(v);
    }
    if (NULL != (value = v)) {
      ret = OB_SUCCESS;;
    }
    return ret;
  }

  Value *get(Key key) const
  {
    const ObBuildInBucket &bucket = buckets_[Hasher::hash(key) % bucket_num_];
    Value *v = bucket.chain_.head_;
    while (NULL != v && ! Hasher::equal(key, Hasher::key(v))) {
      v = ListHead::next(v);
    }
    return v;
  }

  Value *remove(const Key key)
  {
    try_shrink_after_erase();
    ObBuildInBucket &bucket = buckets_[Hasher::hash(key) % bucket_num_];
    Value *v = bucket.chain_.head_;
    while (NULL != v && ! Hasher::equal(key, Hasher::key(v))) {
      v = ListHead::next(v);
    }
    if (NULL != v) {
      bucket.chain_.remove(v);
      --count_;
    }
    return v;
  }

  void remove(Value *v)
  {
    if (NULL != v) {
      ObBuildInBucket &bucket = buckets_[Hasher::hash(Hasher::key(v)) % bucket_num_];
      bucket.chain_.remove(v);
      --count_;
      try_shrink_after_erase();
    }
  }

  // @retval OB_SUCCESS for success
  // @retval OB_HASH_NOT_EXIST for key not exist
  int erase_refactored(const Key key)
  {
    int ret = OB_SUCCESS;

    Value *v = remove(key);
    if (NULL == v) {
      ret = OB_HASH_NOT_EXIST;
    }
    return ret;
  }

  int64_t to_string(char *buf, const int64_t buf_len) const {
    int64_t pos = 0;
    J_KV(K_(count), K_(bucket_num), KP_(buckets), KP_(default_buckets));
    J_COMMA();
    J_OBJ_START();
    for (int64_t i = 0; i < bucket_num_; ++i) {
      J_KV(K(i));
      J_COMMA();
      {
        ObBuildInBucket &bucket = buckets_[i];
        Value *v = bucket.chain_.head_;
        while (NULL != v) {
          J_KV(KPC(v));
          v = ListHead::next(v);
          J_COMMA();
        }
      }
      if (i < bucket_num_ - 1) {
        J_COMMA();
      }
    }
    J_OBJ_END();
    return pos;
  }

private:
  inline void try_expand_before_insert() {
    int ret = OB_SUCCESS;

    if (OB_UNLIKELY(count_ >= bucket_num_ * expand_threshold)) {
      ObBuildInBucket *target_buckets = NULL;
      int64_t target_bucket_num = bucket_num_ * expand_threshold;
      if (OB_ISNULL(target_buckets =
                    static_cast<ObBuildInBucket*>(ob_malloc(target_bucket_num * sizeof(ObBuildInBucket))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(EDIAG, "fail to alloc mem for expand", K(target_bucket_num), K(sizeof(ObBuildInBucket)), K(ret));
      } else {
        if (OB_FAIL(rehash(target_buckets, target_bucket_num))) {
          // just log
          OB_LOG(EDIAG, "fail to rehash", K(target_buckets), K_(default_buckets), K(target_bucket_num), K(ret));
          if (target_buckets != default_buckets_) {
            ob_free(target_buckets);
            target_buckets = NULL;
          }

        } else {
          if (buckets_ != default_buckets_) {
            ob_free(buckets_);
          }
          buckets_ = target_buckets;
          bucket_num_ = target_bucket_num;
        }
      }
    }
    UNUSED(ret);
  }

  inline void try_shrink_after_erase() {
    int ret = OB_SUCCESS;

    if (OB_UNLIKELY((bucket_num_ >= init_bucket_num)
                     && (count_ <= bucket_num_ / shrink_threshold))) {
      ObBuildInBucket *target_buckets = NULL;
      int64_t target_bucket_num = bucket_num_ / shrink_threshold;
      if (target_bucket_num <= init_bucket_num) {
        target_bucket_num = init_bucket_num;
        target_buckets = default_buckets_;
      } else if (OB_ISNULL(target_buckets =
                 static_cast<ObBuildInBucket*>(ob_malloc(target_bucket_num * sizeof(ObBuildInBucket))))) {
        OB_LOG(EDIAG, "fail to alloc mem for expand", K(target_bucket_num), K(sizeof(ObBuildInBucket)));
      } else {
        // nothing
      }

      if (OB_NOT_NULL(target_buckets)) {
        if (OB_FAIL(rehash(target_buckets, target_bucket_num))) {
          // just log
          OB_LOG(EDIAG, "fail to rehash", K(target_buckets), K(target_bucket_num), K(ret));
          if (target_buckets != default_buckets_) {
            ob_free(target_buckets);
            target_buckets = NULL;
          }
        } else {
          if (buckets_ != default_buckets_) {
            ob_free(buckets_);
          }
          buckets_ = target_buckets;
          bucket_num_ = target_bucket_num;
        }
      }
    }
    UNUSED(ret);
  }

  inline int rehash(ObBuildInBucket* new_buckets, int64_t new_bucket_num) {
    int ret = OB_SUCCESS;

    MEMSET(new_buckets, 0, new_bucket_num * sizeof(ObBuildInBucket));
    for (int64_t i = 0; i < new_bucket_num; ++i) {
      new(&new_buckets[i]) ObBuildInBucket();
    }

    for (int64_t i = 0; i < bucket_num_; ++i) {
      ObBuildInBucket &old_bucket = buckets_[i];
      Value *value = old_bucket.chain_.head_;
      while (OB_NOT_NULL(value)) {
        Value *next_value = ListHead::next(value);
        old_bucket.chain_.remove(value);
        Key key = Hasher::key(value);
        ObBuildInBucket &bucket = new_buckets[Hasher::hash(key) % new_bucket_num];
#ifdef DEBUG
        if (!bucket.chain_.in(value)) {
          bucket.chain_.push(value);
        } else {
          ret = OB_HASH_EXIST;
          OB_LOG(EDIAG, "unexpect duplicate item", KP(value), K(key), K(*this));
        }
#else
        bucket.chain_.push(value);
#endif
        value = next_value;
      }
    }

    return ret;
  }

private:
  int64_t count_; // of elements stored in the map.
  int64_t bucket_num_;
  ObBuildInBucket* buckets_;
  ObBuildInBucket default_buckets_[init_bucket_num];

};

} // namespace hash
} // namespace common
} // namespace oceanbase
#endif //OCEANBASE_LIB_HASH_OB_BUILD_IN_HASHMAP_
