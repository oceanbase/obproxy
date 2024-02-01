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
 *
 * *************************************************************
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#ifndef OBPROXY_BUF_ALLOCATOR_H
#define OBPROXY_BUF_ALLOCATOR_H

#include "utils/ob_proxy_lib.h"
#include "iocore/eventsystem/ob_thread_allocator.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{

#define DEFAULT_BUFFER_NUMBER        64
#define DEFAULT_HUGE_BUFFER_NUMBER   32
#define MAX_MIOBUFFER_READERS        4
#define DEFAULT_BUFFER_ALIGNMENT     16
#define DEFAULT_BUFFER_BASE_SHIFT    7
#define DEFAULT_BUFFER_BASE_SIZE     (1 << DEFAULT_BUFFER_BASE_SHIFT)

/**
 * These are defines so that code that used 2
 * for buffer size index when 2 was 2K will
 * still work if it uses BUFFER_SIZE_INDEX_2K
 * instead.
 */
#define BUFFER_SIZE_INDEX_128           0
#define BUFFER_SIZE_INDEX_256           1
#define BUFFER_SIZE_INDEX_512           2
#define BUFFER_SIZE_INDEX_1K            3
#define BUFFER_SIZE_INDEX_2K            4
#define BUFFER_SIZE_INDEX_4K            5
#define BUFFER_SIZE_INDEX_8K            6
#define MAX_BUFFER_SIZE_INDEX           6
#define BUFFER_SIZE_INDEX_COUNT         (MAX_BUFFER_SIZE_INDEX + 1)

#define BUFFER_SIZE_FOR_INDEX(i)     (DEFAULT_BUFFER_BASE_SIZE * (1 << (i)))
#define DEFAULT_MAX_BUFFER_SIZE      BUFFER_SIZE_FOR_INDEX(MAX_BUFFER_SIZE_INDEX)
#define DEFAULT_SMALL_BUFFER_SIZE    BUFFER_SIZE_FOR_INDEX(BUFFER_SIZE_INDEX_512)
#define DEFAULT_LARGE_BUFFER_SIZE    BUFFER_SIZE_FOR_INDEX(BUFFER_SIZE_INDEX_8K)

class ObBufAllocator
{
public:
  ObBufAllocator() { }

  ~ObBufAllocator() { }

  void *alloc(const int64_t size)
  {
    void *ret = NULL;
    if (DEFAULT_MAX_BUFFER_SIZE == size) {
      ret = op_thread_fixed_mem_alloc(buf_allocator_[buffer_size_to_index(size)], get_8k_block_allocator());
    } else if (size < DEFAULT_MAX_BUFFER_SIZE) {
      ret = buf_allocator_[buffer_size_to_index(next_pow2(size))].alloc_void();
    } else {
      ret = common::ob_malloc_align(DEFAULT_BUFFER_ALIGNMENT, size,
                                    common::ObModIds::OB_LARGE_IO_BUFFER);
    }
    return ret;
  }

  void free(void *ptr, const int64_t size)
  {
    if (DEFAULT_MAX_BUFFER_SIZE == size) {
      op_thread_fixed_mem_free(buf_allocator_[buffer_size_to_index(size)], ptr, get_8k_block_allocator());
    } else if (size < DEFAULT_MAX_BUFFER_SIZE) {
      buf_allocator_[buffer_size_to_index(next_pow2(size))].free_void(ptr);
    } else {
      common::ob_free_align(ptr);
    }
  }

  static ObBufAllocator& get_buf_allocator()
  {
    int ret = common::OB_SUCCESS;
    static volatile int64_t once = 0;
    static ObBufAllocator g_buf_allocator;

    while (OB_UNLIKELY(once < 2) && OB_SUCC(ret)) {
      if (ATOMIC_BCAS(&once, 0, 1)) {
        if (OB_FAIL(g_buf_allocator.init_buffer_allocators())) {
          PROXY_EVENT_LOG(WDIAG, "failed to init buffer allocators", K(ret));
          ATOMIC_BCAS(&once, 1, 0);
        } else {
          ATOMIC_BCAS(&once, 1, 2);
        }
      }
    }

    return g_buf_allocator;
  }

private:
  int init_buffer_allocators()
  {
    int ret = common::OB_SUCCESS;
    int64_t slab_size = 0;
    int64_t alignment = 0;
    int64_t cache_count = 0;
    for (int64_t i = 0; i < BUFFER_SIZE_INDEX_COUNT && OB_SUCC(ret); ++i) {
      slab_size = DEFAULT_BUFFER_BASE_SIZE * ((1L) << i);
      alignment = DEFAULT_BUFFER_ALIGNMENT;
      cache_count = BUFFER_SIZE_FOR_INDEX(i) <= DEFAULT_LARGE_BUFFER_SIZE ?
          DEFAULT_BUFFER_NUMBER : DEFAULT_HUGE_BUFFER_NUMBER;

      if (slab_size < alignment) {
        alignment = slab_size;
      }

      if (OB_FAIL(buf_allocator_[i].init("ObBufAllocator", slab_size, cache_count, alignment))) {
        PROXY_EVENT_LOG(WDIAG, "failed to init buffer allocator",
                        K(i), K(slab_size), K(cache_count), K(alignment), K(ret));
      } else if (DEFAULT_MAX_BUFFER_SIZE == slab_size) {
        buf_allocator_[i].set_reclaim_opt(common::ENABLE_RECLAIM, 1);
        buf_allocator_[i].set_reclaim_opt(common::MAX_OVERAGE, 0);
        buf_allocator_[i].set_reclaim_opt(common::DEBUG_FILTER, 3);
        buf_allocator_[i].set_reclaim_opt(common::STEAL_THREAD_COUNT, 4);
      }
    }
    return ret;
  }

  int64_t next_pow2(const int64_t x)
  {
    return x ? (1ULL << (8 * sizeof(int64_t) - __builtin_clzll(x - 1))) : 1;
  }

  int64_t buffer_size_to_index(const int64_t size)
  {
    return size > DEFAULT_BUFFER_BASE_SIZE
           ? (8 * sizeof(int64_t) - __builtin_clzll(size - 1)) - DEFAULT_BUFFER_BASE_SHIFT : 0;
  }

private:
  common::ObFixedMemAllocator buf_allocator_[BUFFER_SIZE_INDEX_COUNT];

  DISALLOW_COPY_AND_ASSIGN(ObBufAllocator);
};

// public fixed memory allocate interface
#define op_fixed_mem_alloc(size) event::ObBufAllocator::get_buf_allocator().alloc(size)
#define op_fixed_mem_free(ptr, size) event::ObBufAllocator::get_buf_allocator().free(ptr, size)

template <const int64_t page_size>
struct ObFixedPageAllocator : public common::ObIAllocator
{
  ObFixedPageAllocator() {}

  virtual ~ObFixedPageAllocator() {}

  void *alloc(const int64_t sz) { return sz == page_size ?  op_fixed_mem_alloc(page_size) : NULL; }

  void free(void *p) { op_fixed_mem_free(p, page_size); }

  void freed(const int64_t sz) { UNUSED(sz); }
};

template <const int64_t page_size>
class ObFixedArenaAllocator : public common::ObIAllocator
{
public:
  ObFixedArenaAllocator()
      : arena_(page_size, ObFixedPageAllocator<page_size>()) { }

  virtual ~ObFixedArenaAllocator() { }

public:
  virtual void *alloc(const int64_t sz) { return arena_.alloc_aligned(sz); }

  virtual void free(void *ptr) { arena_.free(reinterpret_cast<char*>(ptr));  ptr = NULL; }

  virtual void clear() { arena_.free(); }

  int64_t used() const { return arena_.used(); }

  int64_t total() const { return arena_.total(); }

  void reset() { arena_.free(); }

  void reuse() { arena_.reuse(); }

private:
  common::PageArena<char, ObFixedPageAllocator<page_size> > arena_;
};

} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_BUF_ALLOCATOR_H
