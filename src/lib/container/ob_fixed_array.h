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

#ifndef OCEANBASE_COMMON_OB_FIXED_ARRAY_H
#define OCEANBASE_COMMON_OB_FIXED_ARRAY_H
#include "lib/allocator/ob_malloc.h"
#include "lib/utility/utility.h"
#include "lib/container/ob_iarray.h"

namespace oceanbase
{
namespace common
{
template<typename T, typename AllocatorT = ObMalloc>
class ObFixedArray : public ObIArray<T>
{
public:
  ObFixedArray(AllocatorT *allocator = NULL, int64_t item_count = 0)
      : ObIArray<T>(),
      data_(NULL),
      allocator_(allocator),
      count_(0),
      init_cnt_(0),
      copy_assign_ret_(OB_SUCCESS)
  {
    capacity_ = static_cast<uint32_t>(item_count);
  }
  ObFixedArray(AllocatorT &allocator, int64_t item_count = 0)
      : ObIArray<T>(),
      data_(NULL),
      allocator_(&allocator),
      count_(0),
      init_cnt_(0),
      copy_assign_ret_(OB_SUCCESS)
  {
    capacity_ = static_cast<uint32_t>(item_count);
  }
  ~ObFixedArray() { destroy(); }
  void set_allocator(AllocatorT *alloc) { allocator_ = alloc; }
  inline int push_back(const T &obj)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(NULL == data_)) {
      if (capacity_ > 0) {
        if (OB_FAIL(reserve(capacity_))) {
          OB_LOG(WDIAG, "fail to reserve array", K(ret));
        }
      } else {
        ret = OB_NOT_INIT;
        OB_LOG(WDIAG, "array not init", K(ret), K_(capacity));
      }
    } else if (OB_UNLIKELY(count_ >= capacity_)) {
      ret = OB_SIZE_OVERFLOW;
      OB_LOG(WDIAG, "ob fixed array size overflow", K(ret), K(count_), K(capacity_));
    } else {}
    if (OB_SUCC(ret)) {
      if (OB_LIKELY(count_ >= init_cnt_)) {
        // current position not inited
        new(&data_[count_++]) T(obj);
        init_cnt_ = static_cast<uint32_t>(count_);
      } else {
        data_[count_++] = obj;
      }
    }
    return ret;
  }

  inline void pop_back()
  {
    if (count_ > 0) {
      --count_;
    }
  }

  inline int pop_back(T &obj)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(count_ <= 0)) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      obj = data_[--count_];
    }
    return ret;
  }

  inline int remove(int64_t idx) { UNUSED(idx); return OB_NOT_IMPLEMENT; }

  inline int at(int64_t idx, T &obj) const
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(idx < 0 || idx >= count_)) {
      ret = OB_ARRAY_OUT_OF_RANGE;
    } else {
      obj = data_[idx];
    }
    return ret;
  }

  inline T &at(int64_t idx)
  {
    if (0 > idx || idx >= count_) {
      right_to_die_or_duty_to_live();
    }
    return data_[idx];
  }

  inline const T &at(int64_t idx) const
  {
    if (0 > idx || idx >= count_) {
      right_to_die_or_duty_to_live();
    }
    return data_[idx];
  }
  inline T &operator[](const int64_t idx) {return at(idx);}
  inline const T &operator[](const int64_t idx) const {return at(idx);}

  inline int64_t count() const { return count_; }
  inline void reset() { destroy(); }
  inline void reuse() { destroy(); }

  inline void destroy()
  {
    if (OB_ISNULL(allocator_)) {
      OB_LOG(DEBUG, "fail to destory fixed array", K(allocator_));
    } else {
      if (NULL != data_) {
        for (uint32_t i = 0; i < init_cnt_; ++i) {
          data_[i].~T();
        }
        allocator_->free(data_);
        data_ = NULL;
      }
      count_ = 0;
      init_cnt_ = 0;
      capacity_ = 0;
      copy_assign_ret_ = OB_SUCCESS;
    }
  }

  inline void clear() { count_ = 0; }

  inline int reserve(int64_t capacity)
  {
    int ret = OB_SUCCESS;
    if (capacity < 0 || capacity > UINT32_MAX) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WDIAG, "invalid argument", K(ret), K(capacity));
    } else if (NULL == data_) {
      if (OB_FAIL(init(capacity))) {
        OB_LOG(WDIAG, "fail to init array", K(ret), K(capacity));
      }
    } else {}
    if (OB_SUCC(ret) && capacity > capacity_) {
      ret = OB_SIZE_OVERFLOW;
      OB_LOG(WDIAG, "fail to reserver capacity", K(ret), K(capacity), K(capacity_));
    } else {
      //nothing todo
    }
    return ret;
  }

  inline int init(int64_t capacity)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(capacity < 0) || OB_UNLIKELY(capacity > UINT32_MAX)) {
      ret = OB_INVALID_ARGUMENT;
      LIB_LOG(WDIAG, "invalid argument", K(capacity));
    } else if (OB_UNLIKELY(NULL != data_)) {
      ret = OB_INIT_TWICE;
      OB_LOG(WDIAG, "reserve array size", K(ret), K_(data), K_(capacity));
    } else if (OB_ISNULL(allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(EDIAG, "fail to init array", K(ret), K(allocator_));
    } else if (0 == capacity) {
      //nothing todo
    } else {
      int64_t real_capacity = capacity_ == 0 ? capacity : capacity_;
      data_ = static_cast<T *>(allocator_->alloc(real_capacity * sizeof(T)));
      if (OB_UNLIKELY(NULL == data_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LIB_LOG(WDIAG, "no memory", K(ret));
      } else {
        count_ = 0;
        init_cnt_ = 0;
        capacity_ = static_cast<uint32_t>(real_capacity);
      }
    }
    return ret;
  }

  ObFixedArray<T, AllocatorT>& operator=(const ObFixedArray<T, AllocatorT> &other)
  {
    int64_t other_cnt = other.count();
    if (this != &other) {
      if (NULL == allocator_) {
        allocator_ = other.allocator_;
      }
      if (OB_LIKELY(NULL == data_) && OB_LIKELY(other_cnt > 0)) {
        //need init capacity
        if (OB_UNLIKELY(OB_SUCCESS != (copy_assign_ret_ = init(other_cnt)))) {
          LIB_LOG(WDIAG, "init capacity failed", K(other_cnt));
        }
      } else {
        clear();
      }
      for (int64_t i = 0; OB_LIKELY(OB_SUCCESS == copy_assign_ret_) && i < other_cnt; ++i) {
        if (OB_UNLIKELY(OB_SUCCESS != (copy_assign_ret_ = push_back(other.at(i))))) {
          LIB_LOG(WDIAG, "push back other element failed", K(copy_assign_ret_), K(i));
        }
      }
    }
    return *this;
  }

  inline int assign(const ObIArray<T> &other)
  {
    int ret = OB_SUCCESS;
    int64_t other_cnt = other.count();
    if (this != &other) {
      if (OB_LIKELY(NULL == data_) && OB_LIKELY(other_cnt > 0)) {
        //need init capacity
        if (OB_FAIL(init(other_cnt))) {
          LIB_LOG(WDIAG, "init capacity failed", K(other_cnt));
        }
      } else {
        clear();
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < other_cnt; ++i) {
        if (OB_FAIL(push_back(other.at(i)))) {
          LIB_LOG(WDIAG, "push back other element failed", K(ret), K(i));
        }
      }
    }
    return ret;
  }

  inline int prepare_allocate(int64_t capacity)
  {
    int ret = OB_SUCCESS;
    if (capacity < 0 || capacity > UINT32_MAX) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WDIAG, "fail to preprare allocate array", K(ret), K(capacity));
    } else if (OB_FAIL(reserve(capacity))) {
      OB_LOG(WDIAG, "fail to reserver array", K(ret), K(capacity));
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = init_cnt_; i < capacity; i++) {
        new(&data_[i]) T();
      }
      count_ = static_cast<uint32_t>(capacity > count_ ? capacity : count_);
      init_cnt_ = static_cast<uint32_t>(capacity >init_cnt_ ? capacity : init_cnt_);
    }
    return ret;
  }
  inline ObFixedArray(const ObFixedArray<T> &other)
      :ObIArray<T>(), data_(NULL), allocator_(other.allocator_),
      count_(0), init_cnt_(0),
      capacity_(0), copy_assign_ret_(OB_SUCCESS)
  {
    *this = other;
  }

  inline int get_copy_assign_ret() const { return copy_assign_ret_; }

  NEED_SERIALIZE_AND_DESERIALIZE;

protected:
  T *data_;
  AllocatorT *allocator_;
  uint32_t count_;
  uint32_t init_cnt_;
  uint32_t capacity_;
private:
  int copy_assign_ret_;
};
} //end namespace common
} //end namespace oceanbase
#endif

