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

#ifndef OCEANBASE_LIB_CONTAINER_BITSET_
#define OCEANBASE_LIB_CONTAINER_BITSET_

#include <stdint.h>
#include "lib/utility/utility.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
namespace common
{
template <int64_t N = common::OB_DEFAULT_BITSET_SIZE, typename BlockAllocatorT = ModulePageAllocator>
class ObBitSet
{
public:
  typedef uint32_t BitSetWord;

  explicit ObBitSet(const BlockAllocatorT &alloc = BlockAllocatorT(ObModIds::OB_BIT_SET));
  virtual ~ObBitSet() {}

  ObBitSet(const ObBitSet &other);
  ObBitSet &operator=(const ObBitSet &other);
  bool operator==(const ObBitSet &other) const;
  bool equal(const ObBitSet &other) const;

  int reserve(int64_t bits)
  {
    return bitset_word_array_.reserve((bits / PER_BITSETWORD_BITS) + 1);
  }
  int add_member(int64_t index);
  int del_member(int64_t index);
  bool has_member(int64_t index) const;
  bool is_empty() const;
  bool is_subset(const ObBitSet &other) const;
  bool is_superset(const ObBitSet &other) const;
  bool overlap(const ObBitSet &other) const;
  template <int64_t M, typename AnotherAlloc>
  int add_members2(const ObBitSet<M, AnotherAlloc> &other)
  {
    int ret = OB_SUCCESS;
    int64_t this_count = bitset_word_array_.count();
    int64_t other_count = other.bitset_word_count();

    if (this_count < other_count) {
      //make up elements
      for (int64_t i = this_count; OB_SUCC(ret) && i < other_count; ++i) {
        if (OB_FAIL(bitset_word_array_.push_back(0))) {
          LIB_LOG(WDIAG, "fail to push back element into array", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < other_count; ++i) {
        bitset_word_array_.at(i) |= other.get_bitset_word(i);
      }
    }
    return ret;
  }
  int add_members(const ObBitSet &other);
  void del_members(const ObBitSet &other); //added by ryan.ly 20150105
  int do_mask(int64_t begin_index, int64_t end_index);
  void reuse();
  void reset();
  int64_t num_members() const;
  int to_array(ObIArray<int64_t> &arr) const;
  int64_t bitset_word_count() const {return bitset_word_array_.count();}
  int64_t bit_count() const {return bitset_word_count() * PER_BITSETWORD_BITS;}
  //return index:-1 if not have one, or return the index of postition of peticulia bit
  int64_t get_first_peculiar_bit(const ObBitSet &other) const;

  virtual int64_t to_string(char *buf, const int64_t buf_len) const;
  BitSetWord get_bitset_word(int64_t index) const;
  NEED_SERIALIZE_AND_DESERIALIZE;
private:
  static const int64_t PER_BITSETWORD_BITS = 32;
  static const int64_t PER_BITSETWORD_MOD_BITS = 5;
  static const int64_t PER_BITSETWORD_MASK = PER_BITSETWORD_BITS - 1;
  static const int64_t MAX_BITSETWORD = ((N <= 0 ? common::OB_DEFAULT_BITSET_SIZE : N) - 1) / PER_BITSETWORD_BITS + 1;
  ObSEArray<BitSetWord, MAX_BITSETWORD, BlockAllocatorT> bitset_word_array_;
};

template <int64_t N, typename BlockAllocatorT>
ObBitSet<N, BlockAllocatorT>::ObBitSet(const BlockAllocatorT &alloc)
  : bitset_word_array_(OB_MALLOC_NORMAL_BLOCK_SIZE, alloc)
{
}

//return value: true if succ, false if fail
template <int64_t N, typename BlockAllocatorT>
int ObBitSet<N, BlockAllocatorT>::add_member(int64_t index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(index < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WDIAG, "negative bitmapset member not allowed", K(index), K(ret));
    //just return false
  } else {
    int64_t pos = index >> PER_BITSETWORD_MOD_BITS;
    if (OB_UNLIKELY(pos >= bitset_word_array_.count())) {
      for (int64_t i = bitset_word_array_.count(); OB_SUCC(ret) && i <= pos; ++i) {
        if (OB_FAIL(bitset_word_array_.push_back(0))) {
          LIB_LOG(WDIAG, "fail to push back element into array", K(index), K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      BitSetWord &word = bitset_word_array_.at(pos);
      word |= ((BitSetWord) 1 << (index & PER_BITSETWORD_MASK));
    }
  }
  return ret;
}

template <int64_t N, typename BlockAllocatorT>
int ObBitSet<N, BlockAllocatorT>::del_member(int64_t index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(index < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WDIAG, "negative bitmapset member not allowed", K(index), K(ret));
    //just return false
  } else {
    int64_t pos = index >> PER_BITSETWORD_MOD_BITS;
    if (OB_UNLIKELY(pos >= bitset_word_array_.count())) {
    } else {
      BitSetWord &word = bitset_word_array_.at(pos);
      word &= ~((BitSetWord) 1 << (index & PER_BITSETWORD_MASK));
    }
  }
  return ret;
}

template <int64_t N, typename BlockAllocatorT>
typename ObBitSet<N, BlockAllocatorT>::BitSetWord ObBitSet<N, BlockAllocatorT>::get_bitset_word(
    int64_t index) const
{
  BitSetWord word = 0;
  if (index < 0 || index >= bitset_word_array_.count()) {
    LIB_LOG(INFO, "bitmap word index exceeds the scope", K(index), K(bitset_word_array_.count()));
    //indeed,as private function, index would never be negative
    //just return 0
  } else {
    word = bitset_word_array_.at(index);
  }
  return word;
}

template <int64_t N, typename BlockAllocatorT>
int ObBitSet<N, BlockAllocatorT>::add_members(const ObBitSet &other)
{
  int ret = OB_SUCCESS;
  int64_t this_count = bitset_word_array_.count();
  int64_t other_count = other.bitset_word_array_.count();

  if (this_count < other_count) {
    //make up elements
    for (int64_t i = this_count; OB_SUCC(ret) && i < other_count; ++i) {
      if (OB_FAIL(bitset_word_array_.push_back(0))) {
        LIB_LOG(WDIAG, "fail to push back element into array", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < other_count; ++i) {
      bitset_word_array_.at(i) |= other.get_bitset_word(i);
    }
  }
  return ret;
}

template <int64_t N, typename BlockAllocatorT>
void ObBitSet<N, BlockAllocatorT>::del_members(const ObBitSet &other)
{
  for (int64_t i = 0; i < bitset_word_array_.count(); i++) {
    bitset_word_array_.at(i) &= ~other.get_bitset_word(i);
  }
}

template <int64_t N, typename BlockAllocatorT>
int ObBitSet<N, BlockAllocatorT>::do_mask(int64_t begin_index, int64_t end_index)
{
  int64_t max_bit_count = bitset_word_array_.count() * PER_BITSETWORD_BITS;
  int ret = OB_SUCCESS;
  if (begin_index < 0 || begin_index >= max_bit_count || end_index < 0 || end_index >=max_bit_count
      || begin_index >= end_index) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WDIAG, "invalid arguments", K(begin_index), K(end_index), K(ret));
  } else {
    int64_t begin_word = begin_index / PER_BITSETWORD_BITS;
    int64_t end_word = end_index / PER_BITSETWORD_BITS;
    int64_t begin_pos = begin_index % PER_BITSETWORD_BITS;
    int64_t end_pos = end_index % PER_BITSETWORD_BITS;
    // clear words
    for (int64_t i = 0; i < begin_word; i++) {
      bitset_word_array_.at(i) = 0;
    }
    for (int64_t i = bitset_word_array_.count() - 1; i > end_word; i--) {
      bitset_word_array_.at(i) = 0;
    }
    // clear bits
    for (int64_t i = 0; i < begin_pos; ++i) {
      bitset_word_array_.at(begin_word) &= ~((BitSetWord) 1 << i);
    }
    for (int64_t i = 1 + end_pos; i < PER_BITSETWORD_BITS; ++i) {
      bitset_word_array_.at(end_word) &= ~((BitSetWord) 1 << i);
    }
  }
  return ret;
}

template <int64_t N, typename BlockAllocatorT>
inline bool ObBitSet<N, BlockAllocatorT>::has_member(int64_t index) const
{
  bool bool_ret = false;
  if (OB_UNLIKELY(index < 0)) {
    LIB_LOG(WDIAG, "negative bitmapset member not allowed", K(index));
    //just return false
  } else if (OB_UNLIKELY(index >= bit_count())) {
    //the bit is not set
  } else {
    bool_ret = ((bitset_word_array_.at(index >> PER_BITSETWORD_MOD_BITS)
                 & ((BitSetWord) 1 << (index & PER_BITSETWORD_MASK))) != 0);
  }
  return bool_ret;
}

template <int64_t N, typename BlockAllocatorT>
int64_t ObBitSet<N, BlockAllocatorT>::to_string(char *buf, const int64_t buf_len) const
{
  return bitset_word_array_.to_string(buf, buf_len);
}

template <int64_t N, typename BlockAllocatorT>
bool ObBitSet<N, BlockAllocatorT>::is_empty() const
{
  return (0 == bitset_word_array_.count());
}

template <int64_t N, typename BlockAllocatorT>
bool ObBitSet<N, BlockAllocatorT>::is_superset(const ObBitSet &other) const
{
  bool bool_ret = true;
  if (other.is_empty()) {
    bool_ret =  true;
  } else if (is_empty()) {
    bool_ret = false;
  } else if (bitset_word_array_.count() < other.bitset_word_array_.count()) {
    bool_ret = false;
  } else {
    for (int64_t i = 0; bool_ret && i < other.bitset_word_array_.count(); ++i) {
      if ((other.get_bitset_word(i)) & ~(get_bitset_word(i))) {
        bool_ret = false;
      }
    }
  }
  return bool_ret;
}

template <int64_t N, typename BlockAllocatorT>
bool ObBitSet<N, BlockAllocatorT>::is_subset(const ObBitSet &other) const
{
  bool bool_ret = true;
  if (is_empty()) {
    bool_ret = true;
  } else if (other.is_empty()) {
    bool_ret = false;
  } else if (bitset_word_array_.count() > other.bitset_word_array_.count()) {
    bool_ret = false;
  } else {
    for (int64_t i = 0; bool_ret && i < bitset_word_array_.count(); ++i) {
      if (get_bitset_word(i) & ~(other.get_bitset_word(i))) {
        bool_ret = false;
      }
    }
  }
  return bool_ret;
}

template <int64_t N, typename BlockAllocatorT>
bool ObBitSet<N, BlockAllocatorT>::overlap(const ObBitSet &other) const
{
  bool bool_ret = false;
  int64_t min_bitset_word_count = std::min(bitset_word_count(), other.bitset_word_count());
  for (int64_t i = 0; !bool_ret && i < min_bitset_word_count; ++i) {
    if (get_bitset_word(i) & other.get_bitset_word(i)) {
      bool_ret = true;
    }
  }
  return bool_ret;
}

template <int64_t N, typename BlockAllocatorT>
void ObBitSet<N, BlockAllocatorT>::reuse()
{
  bitset_word_array_.reuse();
}

template <int64_t N, typename BlockAllocatorT>
void ObBitSet<N, BlockAllocatorT>::reset()
{
  bitset_word_array_.reset();
}

template <int64_t N, typename BlockAllocatorT>
int64_t ObBitSet<N, BlockAllocatorT>::num_members() const
{
  int64_t num = 0;
  BitSetWord word = 0;

  for (int64_t i = 0; i < bitset_word_count(); i ++) {
    word = get_bitset_word(i);
    if (0 == word) {
      //do nothing
    } else {
      word = (word & UINT32_C(0x55555555)) + ((word >> 1) & UINT32_C(0x55555555));
      word = (word & UINT32_C(0x33333333)) + ((word >> 2) & UINT32_C(0x33333333));
      word = (word & UINT32_C(0x0f0f0f0f)) + ((word >> 4) & UINT32_C(0x0f0f0f0f));
      word = (word & UINT32_C(0x00ff00ff)) + ((word >> 8) & UINT32_C(0x00ff00ff));
      word = (word & UINT32_C(0x0000ffff)) + ((word >> 16) & UINT32_C(0x0000ffff));
      num += (int64_t)word;
    }
  }
  return num;
}

template <int64_t N, typename BlockAllocatorT>
int ObBitSet<N, BlockAllocatorT>::to_array(ObIArray<int64_t> &arr) const
{
  int ret = OB_SUCCESS;
  arr.reuse();
  int64_t num = num_members();
  int64_t count = 0;
  int64_t max_bit_count = bitset_word_array_.count() * PER_BITSETWORD_BITS;
  for (int64_t i = 0; OB_SUCC(ret) && count < num && i < max_bit_count; ++i) {
    if (has_member(i)) {
      if (OB_FAIL(arr.push_back(i))) {
        LIB_LOG(WDIAG, "failed to push back i onto array", K(i), K(ret));
      } else {
        ++count;
      }
    }
  }
  return ret;
}

template <int64_t N, typename BlockAllocatorT>
int64_t ObBitSet<N, BlockAllocatorT>::get_first_peculiar_bit(const ObBitSet &other) const
{
  int64_t index = -1;
  int64_t min_bit_count = std::min(bit_count(), other.bit_count());
  int64_t i = 0;
  for (i = 0; -1 == index && i < min_bit_count; ++i) {
    if (has_member(i) && !(other.has_member(i))) {
      index = i;
    }
  }
  if (-1 == index && i < bit_count()) {
    //not found upper, keep looking
    for (; -1 == index && i < bit_count(); ++i) {
      if (has_member(i)) {
        index = i;
      }
    }
  }
  return index;
}

template <int64_t N, typename BlockAllocatorT>
ObBitSet<N, BlockAllocatorT>::ObBitSet(const ObBitSet &other)
{
  *this = other;
}

template <int64_t N, typename BlockAllocatorT>
ObBitSet<N, BlockAllocatorT> &ObBitSet<N, BlockAllocatorT>::operator=(const ObBitSet &other)
{
  if (this != &other) {
    bitset_word_array_ = other.bitset_word_array_;
  }
  return *this;
}

template <int64_t N, typename BlockAllocatorT>
bool ObBitSet<N, BlockAllocatorT>::operator==(const ObBitSet &other) const
{
  bool bool_ret = true;
  int64_t max_bitset_word_count = std::max(bitset_word_count(), other.bitset_word_count());
  for (int64_t i = 0; bool_ret && i < max_bitset_word_count; ++i) {
    if (this->get_bitset_word(i) != other.get_bitset_word(i)) {
      bool_ret = false;
    }
  }
  return bool_ret;
}

template <int64_t N, typename BlockAllocatorT>
bool ObBitSet<N, BlockAllocatorT>::equal(const ObBitSet &other) const
{
  return *this == other;
}

template <int64_t N, typename BlockAllocatorT>
int ObBitSet<N, BlockAllocatorT>::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  ret = bitset_word_array_.serialize(buf, buf_len, pos);
  return ret;
}

template <int64_t N, typename BlockAllocatorT>
int ObBitSet<N, BlockAllocatorT>::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  ret = bitset_word_array_.deserialize(buf, data_len, pos);
  return ret;
}

template <int64_t N, typename BlockAllocatorT>
int64_t ObBitSet<N, BlockAllocatorT>::get_serialize_size() const
{
  return bitset_word_array_.get_serialize_size();
}

}//end of common
}//end of namespace

#endif // OCEANBASE_LIB_CONTAINER_BITSET_
