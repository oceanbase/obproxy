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

#ifndef _OB_2D_ARRAY_H
#define _OB_2D_ARRAY_H 1
#include "lib/container/ob_iarray.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
namespace common
{
template <typename T,
          typename BlockAllocatorT = ObMalloc,
          typename BlockPointerArrayT = ObSEArray<char *, 64> >
class Ob2DArray: public ObIArray<T>
{
public:
  Ob2DArray(int64_t block_size = OB_MALLOC_NORMAL_BLOCK_SIZE,
            const BlockAllocatorT &alloc = BlockAllocatorT(ObModIds::OB_2D_ARRAY));
  virtual ~Ob2DArray();
  // @note called before any other operation
  void set_block_allocator(const BlockAllocatorT &alloc) {block_alloc_ = alloc;};

  int assign(const ObIArray<T> &other);

  virtual int push_back(const T &obj);
  virtual void pop_back();
  virtual int pop_back(T &obj);
  virtual int remove(int64_t idx);

  virtual T &at(int64_t idx);
  virtual const T &at(int64_t idx) const;
  virtual int at(int64_t idx, T &obj) const;

  virtual int64_t count() const {return count_;};
  virtual void reuse();
  virtual void reset() {reuse();};
  virtual void destroy();
  virtual int reserve(int64_t capacity);
  int64_t get_capacity() const;
  int prepare_allocate(int64_t capacity) { UNUSED(capacity); return OB_NOT_SUPPORTED; }
  bool error() const { return error_; }
  virtual int64_t to_string(char *buffer, int64_t length) const;
  inline void set_block_size(const int64_t block_size) { block_size_ = block_size; }
  inline int64_t get_block_size() const { return block_size_; }
  inline const BlockAllocatorT &get_block_allocator() const { return block_alloc_; }
private:
  // function members
  char *get_obj_pos(int64_t i) const;
  int new_block();
  void destruct_objs();
  DISALLOW_COPY_AND_ASSIGN(Ob2DArray);
private:
  // data members
  int32_t magic_;
  BlockAllocatorT block_alloc_;
  BlockPointerArrayT blocks_;
  int64_t block_size_;
  int64_t count_;
  bool error_;
};

template <typename T,
          typename BlockAllocatorT,
          typename BlockPointerArrayT>
Ob2DArray<T, BlockAllocatorT, BlockPointerArrayT>::Ob2DArray(int64_t block_size,
                                                             const BlockAllocatorT &alloc)
    : magic_(0x2D2D2D2D),
      block_alloc_(alloc),
      blocks_(),
      block_size_(block_size),
      count_(0),
      error_(false)
{
  if (OB_UNLIKELY(static_cast<int64_t>(sizeof(T)) > block_size_)) {
    LIB_LOG(EDIAG, "invalid block size, smaller than T. Fatal!!!", K(sizeof(T)), K_(block_size));
    error_ = true;
  }
}
template <typename T,
          typename BlockAllocatorT,
          typename BlockPointerArrayT>
Ob2DArray<T, BlockAllocatorT, BlockPointerArrayT>::~Ob2DArray()
{
  destroy();
}

template <typename T,
          typename BlockAllocatorT,
          typename BlockPointerArrayT>
int Ob2DArray<T, BlockAllocatorT, BlockPointerArrayT>::assign(const ObIArray<T> &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reuse();
    int64_t N = other.count();
    reserve(N);
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      if (OB_FAIL(this->push_back(other.at(i)))) {
        LIB_LOG(WDIAG, "failed to push back item", K(ret));
        break;
      }
    }
  }
  return ret;
}

template <typename T,
          typename BlockAllocatorT,
          typename BlockPointerArrayT>
inline int64_t Ob2DArray<T, BlockAllocatorT, BlockPointerArrayT>::get_capacity() const
{
  return block_size_ / static_cast<int64_t>(sizeof(T)) * blocks_.count();
}
template <typename T,
          typename BlockAllocatorT,
          typename BlockPointerArrayT>
inline int Ob2DArray<T, BlockAllocatorT, BlockPointerArrayT>::new_block()
{
  int ret = OB_SUCCESS;
  char *blk = static_cast<char *>(block_alloc_.alloc(block_size_));
  if (OB_ISNULL(blk)) {
    LIB_LOG(WDIAG, "no memory");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(blocks_.push_back(blk))) {
    LIB_LOG(WDIAG, "failed to add block", K(ret));
    block_alloc_.free(blk);
    blk = NULL;
  }
  return ret;
}
template <typename T,
          typename BlockAllocatorT,
          typename BlockPointerArrayT>
inline char *Ob2DArray<T, BlockAllocatorT, BlockPointerArrayT>::get_obj_pos(int64_t i) const
{
  // KNOWN ISSUE: if block_obj_num = 0, would generate div by zero exception
  const int64_t block_obj_num = block_size_ / static_cast<int64_t>(sizeof(T));
  char *block = blocks_.at(i / block_obj_num);
  char *obj_buf = block + sizeof(T) * (i % block_obj_num);
  return obj_buf;
}
template <typename T,
          typename BlockAllocatorT,
          typename BlockPointerArrayT>
int Ob2DArray<T, BlockAllocatorT, BlockPointerArrayT>::push_back(const T &obj)
{
  int ret = OB_SUCCESS;
  if (count_ >= get_capacity()) {
    ret = new_block();
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(count_ >= get_capacity())) {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(WDIAG, "invalid param", K_(count), K(get_capacity()));
    } else {
      char *obj_buf = get_obj_pos(count_);
      if (OB_FAIL(construct_assign(*(reinterpret_cast<T *>(obj_buf)), obj))) {
        LIB_LOG(WDIAG, "failed to copy data", K(ret));
      } else {
        ++count_;
      }
    }
  }
  return ret;
}

template <typename T,
          typename BlockAllocatorT,
          typename BlockPointerArrayT>
void Ob2DArray<T, BlockAllocatorT, BlockPointerArrayT>::pop_back()
{
  if (OB_LIKELY(0 < count_)) {
    char *obj_buf = get_obj_pos(count_ - 1);
    reinterpret_cast<T *>(obj_buf)->~T();
    --count_;
  }
}

template <typename T,
          typename BlockAllocatorT,
          typename BlockPointerArrayT>
int Ob2DArray<T, BlockAllocatorT, BlockPointerArrayT>::pop_back(T &obj)
{
  int ret = OB_ENTRY_NOT_EXIST;
  if (OB_LIKELY(0 < count_)) {
    T *obj_ptr = reinterpret_cast<T *>(get_obj_pos(count_ - 1));
    // assign
    if (OB_FAIL(copy_assign(obj, *obj_ptr))) {
      LIB_LOG(WDIAG, "failed to copy data", K(ret));
    } else {
      obj_ptr->~T();
      --count_;
    }
  }
  return ret;
}

template <typename T,
          typename BlockAllocatorT,
          typename BlockPointerArrayT>
int Ob2DArray<T, BlockAllocatorT, BlockPointerArrayT>::remove(int64_t idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 > idx || idx >= count_)) {
    ret = OB_ARRAY_OUT_OF_RANGE;
  } else {
    T *obj_ptr = reinterpret_cast<T *>(get_obj_pos(idx));
    for (int64_t i = idx; OB_SUCC(ret) && i < count_ - 1; ++i) {
      if (OB_FAIL(copy_assign(at(i), at(i + 1)))) {
        LIB_LOG(WDIAG, "failed to copy data", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      // destruct the last object
      obj_ptr = reinterpret_cast<T *>(get_obj_pos(count_ - 1));
      obj_ptr->~T();
      --count_;
    }
  }
  return ret;
}

template <typename T,
          typename BlockAllocatorT,
          typename BlockPointerArrayT>
int Ob2DArray<T, BlockAllocatorT, BlockPointerArrayT>::at(int64_t idx, T &obj) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 > idx || idx >= count_)) {
    ret = OB_ARRAY_OUT_OF_RANGE;
  } else {
    T *obj_ptr = reinterpret_cast<T *>(get_obj_pos(idx));
    if (OB_FAIL(copy_assign(obj, *obj_ptr))) {
      LIB_LOG(WDIAG, "failed to copy data", K(ret));
    }
  }
  return ret;
}

template <typename T,
          typename BlockAllocatorT,
          typename BlockPointerArrayT>
inline T &Ob2DArray<T, BlockAllocatorT, BlockPointerArrayT>::at(int64_t idx)
{
  if (OB_UNLIKELY(0 > idx || idx >= count_)) {
    LIB_LOG(EDIAG, "invalid idx. Fatal!!!", K(idx), K_(count));
  }
  return *reinterpret_cast<T *>(get_obj_pos(idx));
}

template <typename T,
          typename BlockAllocatorT,
          typename BlockPointerArrayT>
inline const T &Ob2DArray<T, BlockAllocatorT, BlockPointerArrayT>::at(int64_t idx) const
{
  if (OB_UNLIKELY(0 > idx || idx >= count_)) {
    LIB_LOG(EDIAG, "invalid idx. Fatal!!!", K(idx), K_(count));
  }
  return *reinterpret_cast<T *>(get_obj_pos(idx));
}

template <typename T,
          typename BlockAllocatorT,
          typename BlockPointerArrayT>
void Ob2DArray<T, BlockAllocatorT, BlockPointerArrayT>::destruct_objs()
{
  // destruct all objects
  for (int64_t i = 0; i < count_; ++i) {
    char *obj_buf = get_obj_pos(i);
    reinterpret_cast<T *>(obj_buf)->~T();
  }
  count_ = 0;
}

template <typename T,
          typename BlockAllocatorT,
          typename BlockPointerArrayT>
void Ob2DArray<T, BlockAllocatorT, BlockPointerArrayT>::reuse()
{
  if (1 >= blocks_.count()) {
    destruct_objs();
  } else {
    destroy();
  }
}

template <typename T,
          typename BlockAllocatorT,
          typename BlockPointerArrayT>
void Ob2DArray<T, BlockAllocatorT, BlockPointerArrayT>::destroy()
{
  destruct_objs();
  int64_t block_count = blocks_.count();
  for (int64_t i = 0; i < block_count; ++i) {
    char *block = blocks_.at(i);
    block_alloc_.free(block);
    block = NULL;
  }
  blocks_.destroy();
}

template <typename T,
          typename BlockAllocatorT,
          typename BlockPointerArrayT>
int Ob2DArray<T, BlockAllocatorT, BlockPointerArrayT>::reserve(int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(true == error_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (capacity > get_capacity()) {
    const int64_t block_obj_num = block_size_ / static_cast<int64_t>(sizeof(T));
    int64_t need_blocks = (0 == capacity % block_obj_num) ?
                          capacity / block_obj_num :
                          1 + capacity / block_obj_num;
    const int64_t curr_blocks = blocks_.count();
    for (int64_t i = curr_blocks; OB_SUCC(ret) && i < need_blocks; ++i) {
      char *block = static_cast<char *>(block_alloc_.alloc(block_size_));
      if (OB_ISNULL(block)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LIB_LOG(WDIAG, "no memory", K(ret));
      } else if (OB_FAIL(blocks_.push_back(block))) {
        LIB_LOG(WDIAG, "failed to add new block", K(ret));
        block_alloc_.free(block);
        block = NULL;
      }
    }
  }
  return ret;
}

template <typename T,
          typename BlockAllocatorT,
          typename BlockPointerArrayT>
int64_t Ob2DArray<T, BlockAllocatorT, BlockPointerArrayT>::to_string(char *buf,
                                                                     int64_t buf_len) const
{
  int64_t pos = 0;
  J_ARRAY_START();
  for (int64_t index = 0; index < count_ - 1; ++index) {
    BUF_PRINTO(at(index));
    J_COMMA();
  }
  if (0 < count_) {
    BUF_PRINTO(at(count_ - 1));
  }
  J_ARRAY_END();
  return pos;
}

} // end namespace common
} // end namespace oceanbase

#endif /* _OB_2D_ARRAY_H */
