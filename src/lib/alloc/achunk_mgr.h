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

#ifndef _OCEABASE_LIB_ALLOC_ACHUNK_MGR_H_
#define _OCEABASE_LIB_ALLOC_ACHUNK_MGR_H_

#include <stdint.h>
#include <stdlib.h>
#include <sys/mman.h>
#include "alloc_struct.h"
#include "abit_set.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/ob_define.h"

namespace oceanbase
{
namespace lib
{

class AChunk;

static const uint64_t MAXADDR           = (1L << 52);
static const uint64_t CHUNK_BITMAP_SIZE = MAXADDR / MEMCHK_CHUNK_ALIGN;

class AChunkMgr
{
#if MEMCHK_LEVEL >= 1
  static const uint64_t ALIGN_SIZE = MEMCHK_CHUNK_ALIGN;
#else
  static const uint64_t ALIGN_SIZE = INTACT_ACHUNK_SIZE;
#endif

  static const int64_t DEFAULT_LIMIT = INT64_MAX;
public:
  static AChunkMgr &instance();

public:
  AChunkMgr();

  AChunk *alloc_chunk(
      const uint64_t size = ACHUNK_SIZE,
      bool high_prio = false);
  void free_chunk(const AChunk *chunk);
  OB_INLINE uint64_t aligned(const uint64_t size);

#if MEMCHK_LEVEL >= 1
  inline const AChunk *ptr2chunk(const void *ptr);
#endif

  inline void set_limit(int64_t limit);
  inline int64_t get_limit() const;
  inline void set_urgent(int64_t limit);
  inline int64_t get_urgent() const;
  inline int64_t get_hold() const;
  inline void disable_mem_alloc() { disable_mem_alloc_ = true; }
  inline void enable_mem_alloc() { disable_mem_alloc_ = false; }

private:
  typedef ABitSet<CHUNK_BITMAP_SIZE> ChunkBitMap;

private:
  void *direct_alloc(const uint64_t size) const;
  void direct_free(const void *ptr, const uint64_t size) const;

  int update_hold(int64_t bytes, bool high_prio);

  bool disable_mem_alloc_;
  ChunkBitMap *chunk_bitmap_;

  int64_t limit_;
  int64_t urgent_;
  int64_t hold_bytes_;
}; // end of class AChunkMgr

#if MEMCHK_LEVEL >= 1
const AChunk *AChunkMgr::ptr2chunk(const void *ptr)
{
  AChunk *chunk = NULL;
  if (chunk_bitmap_ != NULL) {
    uint64_t aligned_addr = (uint64_t)(ptr) & ~(MEMCHK_CHUNK_ALIGN - 1);
    if (chunk_bitmap_->isset((int)(aligned_addr >> MEMCHK_CHUNK_ALIGN_BITS))) {
      chunk = (AChunk*)aligned_addr;
    }
  }
  return chunk;
}
#endif

OB_INLINE uint64_t AChunkMgr::aligned(const uint64_t size)
{
  const uint64_t all_size = align_up2(
      size + ACHUNK_HEADER_SIZE, ALIGN_SIZE);
  return all_size;
}

inline void AChunkMgr::set_limit(int64_t limit)
{
  limit_ = limit;
}

inline int64_t AChunkMgr::get_limit() const
{
  return limit_;
}

inline void AChunkMgr::set_urgent(int64_t urgent)
{
  urgent_ = urgent;
}

inline int64_t AChunkMgr::get_urgent() const
{
  return urgent_;
}

inline int64_t AChunkMgr::get_hold() const
{
  return hold_bytes_;
}

inline int AChunkMgr::update_hold(int64_t bytes, bool high_prio)
{
  int ret = common::OB_SUCCESS;
  const int64_t limit = high_prio ? limit_ + urgent_ : limit_;
  if (bytes <= 0) {
    IGNORE_RETURN ATOMIC_AAF(&hold_bytes_, bytes);
  } else if (hold_bytes_ + bytes <= limit) {
    const int64_t nvalue = ATOMIC_AAF(&hold_bytes_, bytes);
    if (nvalue > limit) {
      IGNORE_RETURN ATOMIC_AAF(&hold_bytes_, -bytes);
      ret = common::OB_EXCEED_MEM_LIMIT;
    }
  } else {
    ret = common::OB_EXCEED_MEM_LIMIT;
  }
  return ret;
}

} // end of namespace lib
} // end of namespace oceanbase

#define CHUNK_MGR (AChunkMgr::instance())

#endif /* _OCEABASE_LIB_ALLOC_ACHUNK_MGR_H_ */
