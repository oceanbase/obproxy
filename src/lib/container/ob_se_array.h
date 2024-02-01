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

#ifndef OCEANBASE_LIB_CONTAINER_SE_ARRAY_
#define OCEANBASE_LIB_CONTAINER_SE_ARRAY_ 1
#include "lib/container/ob_array.h"
#include "lib/allocator/page_arena.h"         // for ModulePageAllocator
#include "lib/container/ob_iarray.h"
#include "lib/utility/ob_template_utils.h"
namespace oceanbase
{
namespace common
{
namespace array
{
template <typename T, int64_t LOCAL_ARRAY_SIZE,
          typename BlockAllocatorT,
          typename CallBack,
          typename ItemEncode,
          typename VarArrayT >
class ObSEArrayIterator;
}

// ObNullAllocator cannot alloc buffer
class ObNullAllocator: public ObIAllocator
{
public:
  ObNullAllocator(int64_t mod_id = ObModIds::OB_MOD_DO_NOT_USE_ME,
      int64_t tenant_id = OB_SERVER_TENANT_ID)
  {
    UNUSED(mod_id);
    UNUSED(tenant_id);
  }
  virtual ~ObNullAllocator() {};
};

template <typename T, int64_t LOCAL_ARRAY_SIZE,
          typename BlockAllocatorT = ModulePageAllocator,
          typename CallBack = ObArrayDefaultCallBack<T>,
          typename ItemEncode = NotImplementItemEncode<T>,
          typename VarArrayT = ObArray<T, BlockAllocatorT, ObArrayDefaultCallBack<T>, ItemEncode> >
class ObSEArray : public ObIArray<T>
{
public:
  typedef array::ObSEArrayIterator<T,
                                   LOCAL_ARRAY_SIZE,
                                   BlockAllocatorT,
                                   CallBack,
                                   ItemEncode,
                                   VarArrayT> iterator;
  ObSEArray(int64_t block_size = OB_MALLOC_NORMAL_BLOCK_SIZE,
            const BlockAllocatorT &alloc = BlockAllocatorT(ObModIds::OB_SE_ARRAY));
  ObSEArray(int64_t mod_id, int64_t block_size);
  virtual ~ObSEArray();

  // deep copy
  ObSEArray(const ObSEArray &other);
  ObSEArray &operator=(const ObSEArray &other);
  int assign(const ObIArray<T> &other);

  int push_back(const T &obj);
  void pop_back();
  int pop_back(T &obj);
  int remove(int64_t idx);

  inline int at(int64_t idx, T &obj) const
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(0 > idx || idx >= count_)) {
      ret = OB_ARRAY_OUT_OF_RANGE;
    } else {
      if (OB_LIKELY(count_ <= LOCAL_ARRAY_SIZE)) {
        if (OB_FAIL(copy_assign(obj, local_data_[idx]))) {
          LIB_LOG(WDIAG, "failed to copy data", K(ret));
        }
      } else {
        if (OB_FAIL(copy_assign(obj, array_.at(idx)))) {
          LIB_LOG(WDIAG, "failed to copy data", K(ret));
        }
      }
    }
    return ret;
  }
  inline T &at(int64_t idx)     // dangerous
  {
    if (OB_UNLIKELY(0 > idx || idx >= count_)) {
      LIB_LOG(EDIAG, "idx out of range", K(idx), K_(count));
    }

    if (OB_LIKELY(count_ <= LOCAL_ARRAY_SIZE)) {
      return local_data_[idx];
    } else {
      return array_.at(idx);
    }
  }
  inline const T &at(int64_t idx) const // dangerous
  {
    if (OB_UNLIKELY(0 > idx || idx >= count_)) {
      LIB_LOG(EDIAG, "idx out of range", K(idx), K_(count));
    }

    if (OB_LIKELY(count_ <= LOCAL_ARRAY_SIZE)) {
      return local_data_[idx];
    } else {
      return array_.at(idx);
    }
  }
  inline const T &operator[](int64_t idx) const  // dangerous
  {
    if (OB_UNLIKELY(0 > idx || idx >= count_)) {
      LIB_LOG(EDIAG, "idx out of range", K(idx), K_(count));
    }

    if (OB_LIKELY(count_ <= LOCAL_ARRAY_SIZE)) {
      return local_data_[idx];
    } else {
      return array_.at(idx);
    }
  }
  T *alloc_place_holder();

  inline int64_t count() const { return count_; }

  // For test only!!!
  inline int64_t for_test_only_valid_count() const { return valid_count_; }

  void reuse()
  {
    CallBack cb;
    do_clear_func(cb);
  }
  inline void reset() { destroy(); }
  void destroy();
  inline int reserve(int64_t capacity)
  {
    int ret = OB_SUCCESS;
    if (capacity > LOCAL_ARRAY_SIZE) {
      ret = array_.reserve(capacity);
    }
    return ret;
  }
  //prepare allocate can avoid declaring local data
  int prepare_allocate(int64_t capacity);
  int64_t to_string(char *buffer, int64_t length) const;
  inline int64_t get_data_size() const
  {
    return (count_ <= LOCAL_ARRAY_SIZE) ? (LOCAL_ARRAY_SIZE * sizeof(T)) : (array_.get_data_size());
  }
  bool error() const {return array_.error();};
  inline int get_copy_assign_ret() const { return copy_assign_ret_; }
  int64_t get_capacity() const
  {
    return std::max(LOCAL_ARRAY_SIZE, array_.get_capacity());
  }
  NEED_SERIALIZE_AND_DESERIALIZE;

public:
  iterator begin();
  iterator end();
private:
  // types and constants
private:
  // function members
  inline int move_from_local();
  inline int move_to_local();
  template <class CB>
  inline void do_clear_func(CB &cb)
  {
    //reset all datamember
    if (count_ <= LOCAL_ARRAY_SIZE) {
      for (int64_t i = 0; i < count_; ++i) {
        cb(&local_data_[i]);
      }
    } else {
      array_.reset();
    }
    count_ = 0;
    copy_assign_ret_ = OB_SUCCESS;
  }
private:
  // data members
  T *local_data_;
  char local_data_buf_[LOCAL_ARRAY_SIZE * sizeof(T)];
  int64_t count_;
  int64_t valid_count_;
  VarArrayT array_;
  int copy_assign_ret_;
};

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack, typename ItemEncode, typename VarArrayT>
int ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack, ItemEncode, VarArrayT>::serialize(
    char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, count()))) {
    LIB_LOG(WDIAG, "fail to encode ob array count", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count(); i ++) {
    if (OB_SUCCESS != (ret = serialization::encode(buf, buf_len, pos, at(i)))) {
      LIB_LOG(WDIAG, "fail to encode item", K(i), K(ret));
    }
  }
  return ret;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack, typename ItemEncode, typename VarArrayT>
int ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack, ItemEncode, VarArrayT>::deserialize(
    const char *buf, int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  T item;
  reset();
  if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &count))) {
    LIB_LOG(WDIAG, "fail to decode ob array count", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; i ++) {
    if (OB_SUCCESS != (ret = serialization::decode(buf, data_len, pos, item))) {
      LIB_LOG(WDIAG, "fail to decode array item", K(ret));
    } else if (OB_SUCCESS != (ret = push_back(item))) {
      LIB_LOG(WDIAG, "fail to add item to array", K(ret));
    }
  }
  return ret;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack, typename ItemEncode, typename VarArrayT>
int64_t ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack, ItemEncode, VarArrayT>::get_serialize_size()
const
{
  int64_t size = 0;
  size += serialization::encoded_length_vi64(count());
  for (int64_t i = 0; i < count(); i ++) {
    size += serialization::encoded_length(at(i));
  }
  return size;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack, typename ItemEncode, typename VarArrayT>
ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack, ItemEncode, VarArrayT>::ObSEArray(
    int64_t block_size, const BlockAllocatorT &alloc)
    : ObIArray<T>(),
      local_data_(static_cast<T *>(static_cast<void *>(local_data_buf_))),
      count_(0),
      valid_count_(0),
      array_(block_size, alloc),
      copy_assign_ret_(OB_SUCCESS)
{
  STATIC_ASSERT(LOCAL_ARRAY_SIZE > 0, "se local array size invalid");
}
template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack, typename ItemEncode, typename VarArrayT>
ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack, ItemEncode, VarArrayT>::ObSEArray(
    int64_t mod_id, int64_t block_size)
    : ObIArray<T>(),
      local_data_(static_cast<T *>(static_cast<void *>(local_data_buf_))),
      count_(0),
      valid_count_(0),
      array_(block_size, BlockAllocatorT(mod_id)),
      copy_assign_ret_(OB_SUCCESS)
{
  STATIC_ASSERT(LOCAL_ARRAY_SIZE > 0, "se local array size invalid");
}
template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack, typename ItemEncode, typename VarArrayT>
ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack, ItemEncode, VarArrayT>::~ObSEArray()
{
  destroy();
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack, typename ItemEncode, typename VarArrayT>
inline int
ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack, ItemEncode, VarArrayT>::move_from_local()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(LOCAL_ARRAY_SIZE != count_ || 0 != array_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(EDIAG, "invalid count", K_(count), K(array_.count()), K(ret));
  } else {
    array_.reserve(count_);
    for (int64_t i = 0; OB_SUCC(ret) && i < count_; ++i) {
      ret = array_.push_back(local_data_[i]);
    }
  }
  return ret;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack, typename ItemEncode, typename VarArrayT>
int ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack, ItemEncode, VarArrayT>::push_back(
    const T &obj)
{
  int ret = OB_SUCCESS;
  if (count_ < LOCAL_ARRAY_SIZE) {
    if (count_ < valid_count_) {
      if (OB_FAIL(copy_assign(local_data_[count_], obj))) {
        LIB_LOG(WDIAG, "failed to copy data", K(ret));
      } else {
        ++count_;
      }
    } else {
      if (OB_FAIL(construct_assign(local_data_[count_], obj))) {
        LIB_LOG(WDIAG, "failed to copy data", K(ret));
      } else {
        ++valid_count_;
        ++count_;
      }
    }
  } else {
    if (OB_UNLIKELY(count_ == LOCAL_ARRAY_SIZE)) {
      ret = move_from_local();
    }
    if (OB_SUCC(ret)) {
      ret = array_.push_back(obj);
    }
    if (OB_SUCC(ret)) {
      ++count_;
    }
  }
  return ret;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack, typename ItemEncode, typename VarArrayT>
inline int
ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack, ItemEncode, VarArrayT>::move_to_local()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(array_.count() != LOCAL_ARRAY_SIZE)) {
    LIB_LOG(EDIAG, "invalid move_to_local action", K(LOCAL_ARRAY_SIZE), K(array_.count()));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < valid_count_; ++i) {
    if (OB_FAIL(copy_assign(local_data_[i], array_.at(i)))) {
      LIB_LOG(WDIAG, "failed to copy data", K(ret));
    }
  }
  for (int64_t i = valid_count_; OB_SUCC(ret) && i < LOCAL_ARRAY_SIZE; ++i) {
    if (OB_FAIL(construct_assign(local_data_[i], array_.at(i)))) {
      LIB_LOG(WDIAG, "failed to copy data", K(ret));
    }
  }
  array_.reset();
  return ret;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack, typename ItemEncode, typename VarArrayT>
void ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack, ItemEncode, VarArrayT>::pop_back()
{
  if (OB_UNLIKELY(count_ <= 0)) {
  } else if (count_ <= LOCAL_ARRAY_SIZE) {
    --count_;
  } else {
    array_.pop_back();
    if (array_.count() == LOCAL_ARRAY_SIZE) {
      if (OB_UNLIKELY(OB_SUCCESS != move_to_local())) {
        LIB_LOG(WDIAG, "failed to move_to_local()");
      }
    }
    --count_;
  }
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack, typename ItemEncode, typename VarArrayT>
int ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack, ItemEncode, VarArrayT>::pop_back(
    T &obj)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(count_ <= 0)) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (count_ <= LOCAL_ARRAY_SIZE) {
    if (OB_FAIL(copy_assign(obj, local_data_[--count_]))) {
      LIB_LOG(WDIAG, "failed to copy data", K(ret));
    }
  } else {
    ret = array_.pop_back(obj);
    if (OB_SUCC(ret)) {
      if (array_.count() == LOCAL_ARRAY_SIZE) {
        if (OB_FAIL(move_to_local())) {
          LIB_LOG(WDIAG, "failed to move_to_local()", K(ret));
        }
      }
      --count_;
    }
  }
  return ret;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack, typename ItemEncode, typename VarArrayT>
int ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack, ItemEncode, VarArrayT>::remove(
    int64_t idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 > idx || idx >= count_)) {
    ret = OB_ARRAY_OUT_OF_RANGE;
  } else if (count_ <= LOCAL_ARRAY_SIZE) {
    for (int64_t i = idx; OB_SUCC(ret) && i < count_ - 1; ++i) {
      if (OB_FAIL(copy_assign(local_data_[i], local_data_[i + 1]))) {
        LIB_LOG(WDIAG, "failed to copy data", K(ret));
      }
    }
    --count_;
  } else {
    ret = array_.remove(idx);
    if (OB_SUCC(ret)) {
      if (array_.count() == LOCAL_ARRAY_SIZE) {
        if (OB_FAIL(move_to_local())) {
          LIB_LOG(WDIAG, "failed to move_to_local()", K(ret));
        }
      }
      --count_;
    }
  }
  return ret;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack, typename ItemEncode, typename VarArrayT>
inline  T *ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack, ItemEncode, VarArrayT>::alloc_place_holder()
{
  int ret = OB_SUCCESS;
  T *ptr = NULL;
  if (count_ < LOCAL_ARRAY_SIZE) {
    if (OB_UNLIKELY(valid_count_ < count_)) {
      LIB_LOG(EDIAG, "ObSEArray inner state retor",
              K(LOCAL_ARRAY_SIZE), K_(valid_count), K_(count));
      ptr = NULL; // unexpected
    } else if (valid_count_ == count_) {
      ptr = new(&local_data_[count_]) T();
      count_++;
      valid_count_++;
    } else {
      ptr = &local_data_[count_++];
    }
  } else {
    if (OB_UNLIKELY(count_ == LOCAL_ARRAY_SIZE)) {
      ret = move_from_local();
    }
    if (OB_SUCC(ret)) {
      if ((ptr = array_.alloc_place_holder()) != NULL) {
        ++count_;
      }
    }
  }
  return ptr;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack, typename ItemEncode, typename VarArrayT>
void ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack, ItemEncode, VarArrayT>::destroy()
{
  if (OB_UNLIKELY(count_ > LOCAL_ARRAY_SIZE)) {
    array_.destroy();
  } else {
    CallBack cb;
    for (int64_t i = 0; i < count_; ++i) {
      cb(&local_data_[i]);
    }
  }
  for (int i = 0; i < valid_count_; ++i) {
    local_data_[i].~T();
  }
  local_data_ = static_cast<T *>(static_cast<void *>(local_data_buf_));
  count_ = 0;
  valid_count_ = 0;
  copy_assign_ret_ = OB_SUCCESS;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack, typename ItemEncode, typename VarArrayT>
inline int
ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack, ItemEncode, VarArrayT>::prepare_allocate(
    int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (capacity > LOCAL_ARRAY_SIZE) {
    ret = array_.prepare_allocate(capacity);
  } else {
    if (valid_count_ < capacity) {
      for (int64_t i = valid_count_; i < capacity; ++i) {
        new(&local_data_[i]) T();
      }
      valid_count_= capacity;
    }
  }
  count_ = capacity;
  return ret;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack, typename ItemEncode, typename VarArrayT>
int64_t ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack, ItemEncode, VarArrayT>::to_string(
    char *buf, int64_t buf_len) const
{
  int64_t pos = 0;
  const int64_t log_keep_size = (ObLogger::get_logger().is_in_async_logging()
                                 ? OB_ASYNC_LOG_KEEP_SIZE
                                 : OB_LOG_KEEP_SIZE);
  J_ARRAY_START();
  for (int64_t index = 0; index < count_ - 1; ++index) {
    if (pos + log_keep_size >= buf_len - 1) {
      BUF_PRINTF(OB_LOG_ELLIPSIS);
      J_COMMA();
      break;
    }
    databuff_printf(buf, buf_len, pos, "[%ld]", index);
    BUF_PRINTO(at(index));
    J_COMMA();
  }
  if (0 < count_) {
    databuff_printf(buf, buf_len, pos, "[%ld]", count_ - 1);
    BUF_PRINTO(at(count_ - 1));
  }
  J_ARRAY_END();
  return pos;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack, typename ItemEncode, typename VarArrayT>
ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack, ItemEncode, VarArrayT>
&ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack, ItemEncode, VarArrayT>::operator=
(const ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack, ItemEncode, VarArrayT> &other)
{
  if (OB_LIKELY(this != &other)) {
    this->reset();
    int64_t N = other.count();
    if (N <= LOCAL_ARRAY_SIZE) {
      int64_t i = 0;
      for (i = 0; OB_LIKELY(OB_SUCCESS == copy_assign_ret_) && i < N; ++i) {
        if (OB_UNLIKELY(OB_SUCCESS != (copy_assign_ret_ = construct_assign(local_data_[i],
                                                                          other.local_data_[i])))) {
          LIB_LOG(WDIAG, "failed to copy data", K(copy_assign_ret_));
          count_ = i;
        }
      }
      if (OB_LIKELY(OB_SUCCESS == copy_assign_ret_)) {
        count_ = N;
      }
      if (valid_count_ < count_) {
        valid_count_ = count_;
      }
    } else {
      this->array_ = other.array_;
      if (OB_UNLIKELY(OB_SUCCESS != (copy_assign_ret_ = this->array_.get_copy_assign_ret()))) {
        LIB_LOG(WDIAG, "error when assign array_", K(copy_assign_ret_));
        count_ = this->array_.count();
      } else {
        count_ = other.count_;
      }
    }
  }
  return *this;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack, typename ItemEncode, typename VarArrayT>
int ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack, ItemEncode, VarArrayT>::assign(const ObIArray<T> &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    this->reset();
    int64_t N = other.count();
    if (N <= LOCAL_ARRAY_SIZE) {
      const int64_t assign = std::min(valid_count_, N);
      for (int64_t i = 0; OB_SUCC(ret) && i < assign; ++i) {
        if (OB_FAIL(copy_assign(local_data_[i], other.at(i)))) {
          LIB_LOG(WDIAG, "failed to copy data", K(ret));
          count_ = i;
        }
      }
      for (int64_t i = assign; OB_SUCC(ret) && i < N; ++i) {
        if (OB_FAIL(construct_assign(local_data_[i], other.at(i)))) {
          LIB_LOG(WDIAG, "failed to copy data", K(ret));
          count_ = i;
        }
      }
      if (OB_SUCC(ret)) {
        count_ = N;
      }
      if (valid_count_ < count_) {
        valid_count_= count_;
      }
    } else {
      if (OB_FAIL(this->array_.assign(other))) {
        LIB_LOG(WDIAG, "error when assign array_", K(ret));
        count_ = this->array_.count();
      } else {
        this->count_ = N;
      }
    }
  }
  return ret;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, typename CallBack, typename ItemEncode, typename VarArrayT>
ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack, ItemEncode, VarArrayT>::ObSEArray(
    const ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, CallBack, ItemEncode, VarArrayT> &other)
    : local_data_(static_cast<T *>(static_cast<void *>(local_data_buf_))),
      count_(0),
      valid_count_(0),
      array_(other.array_.get_block_size(), other.array_.get_block_allocator()),
      copy_assign_ret_(OB_SUCCESS)
{
  *this = other;
}
} // end namespace common
} // end namespace oceanbase

#endif /* OCEANBASE_LIB_CONTAINER_SE_ARRAY_ */
