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

#ifndef  OCEANBASE_COMMON_BLOOM_FILTER_H_
#define  OCEANBASE_COMMON_BLOOM_FILTER_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <limits.h>
#include <cmath>
#include "lib/ob_define.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase
{
namespace common
{
template <class T, class HashFunc>
class ObBloomFilter
{
public:
  ObBloomFilter();
  ~ObBloomFilter();
  int init(int64_t element_count, double false_positive_prob = BLOOM_FILTER_FALSE_POSITIVE_PROB);
  void destroy();
  void clear();
  int deep_copy(const ObBloomFilter<T, HashFunc> &other);
  int deep_copy(const ObBloomFilter<T, HashFunc> &other, char *buf);
  int64_t get_deep_copy_size() const;
  int insert(const T &element);
  int may_contain(const T &element, bool &is_contain) const;
  int64_t calc_nbyte(const int64_t nbit) const;
  bool is_valid() const
  {
    return NULL != bits_ && nbit_ > 0 && nhash_ > 0;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObBloomFilter);
  static const double BLOOM_FILTER_FALSE_POSITIVE_PROB = 0.1;
  mutable HashFunc hash_func_;
  ObArenaAllocator allocator_;
  int64_t nhash_;
  int64_t nbit_;
  uint8_t *bits_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////

template <class T, class HashFunc>
ObBloomFilter<T, HashFunc>::ObBloomFilter() : allocator_(ObModIds::OB_BLOOM_FILTER), nhash_(0), nbit_(0), bits_(NULL)
{
}

template <class T, class HashFunc>
ObBloomFilter<T, HashFunc>::~ObBloomFilter()
{
  destroy();
}

template <class T, class HashFunc>
int ObBloomFilter<T, HashFunc>::deep_copy(const ObBloomFilter<T, HashFunc> &other)
{
  int ret = OB_SUCCESS;

  if (is_valid()) {
    ret = OB_INIT_TWICE;
    LIB_LOG(WARN, "The ObBloomFilter has data.", K(ret));
  } else if (NULL == (bits_ = allocator_.alloc(calc_nbyte(other.nbit_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(WARN, "Fail to allocate memory, ", K(ret));
  } else {
    nbit_ = other.nbit_;
    nhash_ = other.nhash_;
    MEMCPY(bits_, other.bits_, calc_nbyte(nbit_));
  }

  return ret;
}

template <class T, class HashFunc>
int ObBloomFilter<T, HashFunc>::deep_copy(const ObBloomFilter<T, HashFunc> &other, char *buffer)
{
  int ret = OB_SUCCESS;

  if (is_valid()) {
    ret = OB_INIT_TWICE;
    LIB_LOG(WARN, "The ObBloomFilter has data.", K(ret));
  } else {
    nbit_ = other.nbit_;
    nhash_ = other.nhash_;
    bits_ = reinterpret_cast<uint8_t*>(buffer);
    MEMCPY(bits_, other.bits_, calc_nbyte(nbit_));
  }

  return ret;
}


template <class T, class HashFunc>
int64_t ObBloomFilter<T, HashFunc>::get_deep_copy_size() const
{
  return calc_nbyte(nbit_);
}

template <class T, class HashFunc>
int64_t ObBloomFilter<T, HashFunc>::calc_nbyte(const int64_t nbit) const
{
  return (nbit / CHAR_BIT + (nbit % CHAR_BIT ? 1 : 0));
}

template <class T, class HashFunc>
int ObBloomFilter<T, HashFunc>::init(int64_t element_count, double false_positive_prob)
{
  int ret = common::OB_SUCCESS;
  if (element_count <= 0) {
    ret = common::OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "bloom filter element_count should be > 0 ",
            K(element_count), K(ret));
  } else if (!(false_positive_prob < 1.0 || false_positive_prob > 0.0)) {
    ret = common::OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "bloom filter false_positive_prob should be < 1.0 and > 0.0",
            K(false_positive_prob), K(ret));
  } else {
    double num_hashes = -std::log(false_positive_prob) / std::log(2);
    int64_t num_bits = static_cast<int64_t>((static_cast<double>(element_count)
                                             * num_hashes / static_cast<double>(std::log(2))));
    int64_t num_bytes = calc_nbyte(num_bits);

    bits_ = (uint8_t *)allocator_.alloc(static_cast<int32_t>(num_bytes));
    if (NULL == bits_) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(WARN, "bits_ null pointer, ", K_(nbit), K(ret));
    } else {
      memset(bits_, 0, num_bytes);
      nhash_ = static_cast<int64_t>(num_hashes);
      nbit_ = num_bits;
    }
  }
  return ret;
}

template <class T, class HashFunc>
void ObBloomFilter<T, HashFunc>::destroy()
{
  if (NULL != bits_) {
    allocator_.reset();
    bits_ = NULL;
    nhash_ = 0;
    nbit_ = 0;
  }
}

template <class T, class HashFunc>
void ObBloomFilter<T, HashFunc>::clear()
{
  if (NULL != bits_) {
    memset(bits_, 0, calc_nbyte(nbit_));
  }
}

template <class T, class HashFunc>
int ObBloomFilter<T, HashFunc>::insert(const T &element)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "bloom filter has not inited, ",
            K_(bits), K_(nbit), K_(nhash), K(ret));
  } else {
    uint64_t hash = 0;
    for (int64_t i = 0; i < nhash_; ++i) {
      hash = (hash_func_(element, hash) % nbit_);
      bits_[hash / CHAR_BIT] = static_cast<unsigned char>(bits_[hash / CHAR_BIT]
          | (1 << (hash % CHAR_BIT)));
    }
  }
  return ret;
}

template <class T, class HashFunc>
int ObBloomFilter<T, HashFunc>::may_contain(const T &element, bool &is_contain) const
{
  int ret = OB_SUCCESS;
  is_contain = true;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "bloom filter has not inited, ",
            K_(bits), K_(nbit), K_(nhash), K(ret));
  } else {
    uint64_t hash = 0;
    uint8_t byte_mask = 0;
    uint8_t byte = 0;
    for (int64_t i = 0; i < nhash_; ++i) {
      hash = static_cast<uint32_t>(hash_func_(element, hash) % nbit_);
      byte = bits_[hash / CHAR_BIT];
      byte_mask = static_cast<int8_t>(1 << (hash % CHAR_BIT));
      if (0 == (byte & byte_mask)) {
        is_contain = false;
        break;
      }
    }
  }
  return ret;
}

}//end namespace common
}//end namespace oceanbase

#endif //OCEANBASE_COMMON_BLOOM_FILTER_H_

