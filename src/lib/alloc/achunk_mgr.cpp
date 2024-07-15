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

#define USING_LOG_PREFIX LIB

#include <new>
#include <stdlib.h>
#include "achunk_mgr.h"
#include "lib/allocator/ob_tc_malloc.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/oblog/ob_log.h"
#include "alloc_struct.h"

using namespace oceanbase::lib;

AChunkMgr &AChunkMgr::instance()
{
  static AChunkMgr mgr;
  return mgr;
}

AChunkMgr::AChunkMgr()
    : disable_mem_alloc_(false), chunk_bitmap_(NULL),
      limit_(DEFAULT_LIMIT), urgent_(0), hold_bytes_(0)
{
#if MEMCHK_LEVEL >= 1
  uint64_t bmsize = sizeof (ChunkBitMap);
  AChunk *chunk = alloc_chunk(bmsize);
  if (NULL != chunk) {
    chunk_bitmap_ = new (chunk->data_) ChunkBitMap();
    chunk_bitmap_->set((int)((uint64_t)chunk >> MEMCHK_CHUNK_ALIGN_BITS));
  }
#endif
}

void *AChunkMgr::direct_alloc(const uint64_t size) const
{
  void *ptr = NULL;
  const int prot = PROT_READ | PROT_WRITE;
  const int flags = MAP_PRIVATE | MAP_ANONYMOUS;
  const int flags_hp = flags | MAP_HUGETLB;
  const int fd = -1;
  const int offset = 0;
  bool hp_on = true;

  if (OB_UNLIKELY(disable_mem_alloc_)) {
    // nothing
  } else {
    if (MAP_FAILED == (ptr = ::mmap(NULL, size, prot, flags_hp, fd, offset))) {
      hp_on = false;
      ptr = NULL;
    }

    if (!hp_on && MAP_FAILED == (ptr = ::mmap(NULL, size, prot, flags, fd, offset))) {
      ptr = NULL;
    } else {
#if MEMCHK_LEVEL >= 1
      if (((uint64_t)ptr & (ALIGN_SIZE - 1)) != 0) {
        // not aligned
        ::munmap(ptr, size);

        uint64_t new_size = size + ALIGN_SIZE;
        if (MAP_FAILED == (
              ptr = ::mmap(
                NULL, new_size, prot, flags, fd, offset))) {
          ptr = NULL;
        } else {
          const uint64_t addr = align_up2((uint64_t)ptr, ALIGN_SIZE);
          if (addr - (uint64_t)ptr > 0) {
            ::munmap(ptr, addr - (uint64_t)ptr);
          }
          if (ALIGN_SIZE - (addr - (uint64_t)ptr) > 0) {
            ::munmap((void*)(addr + size), ALIGN_SIZE - (addr - (uint64_t)ptr));
          }
          ptr = (void*)addr;
        }
      } else {
        // aligned address returned
      }

      if (NULL != chunk_bitmap_ && NULL != ptr) {
        chunk_bitmap_->set((int)((uint64_t)ptr >> MEMCHK_CHUNK_ALIGN_BITS));
      }
#endif
    }
  } 

  return ptr;
}

void AChunkMgr::direct_free(const void *ptr, const uint64_t size) const
{
#if MEMCHK_LEVEL >= 1
  const int idx = (int)((uint64_t)ptr >> MEMCHK_CHUNK_ALIGN_BITS);

  abort_unless(ptr);
  abort_unless(((uint64_t)ptr & (MEMCHK_CHUNK_ALIGN - 1)) == 0);
  abort_unless(chunk_bitmap_ != NULL);
  abort_unless(chunk_bitmap_->isset(idx));
  chunk_bitmap_->unset(idx);
#endif
  ::munmap((void*)ptr, size);
}

AChunk *AChunkMgr::alloc_chunk(const uint64_t size, bool high_prio)
{
  const uint64_t all_size = aligned(size);
  const uint64_t adj_size = all_size - ACHUNK_HEADER_SIZE;

  int ret = common::OB_SUCCESS;
  AChunk *chunk = NULL;
  if (OB_SUCC(update_hold(all_size, high_prio))) {
    void *ptr = direct_alloc(all_size);
    if (ptr != NULL) {
      chunk = new (ptr) AChunk(adj_size);
      chunk->alloc_bytes_ = size;
    } else {
      IGNORE_RETURN update_hold(-all_size, high_prio);
    }
  }

  return chunk;
}

void AChunkMgr::free_chunk(const AChunk *chunk)
{
  int ret = common::OB_SUCCESS;
  if (NULL != chunk) {
    const int64_t size = chunk->size_;
    const uint64_t all_size = aligned(size);
    direct_free(chunk, chunk->size_ + ACHUNK_HEADER_SIZE);
    if (OB_FAIL(update_hold(-all_size, false))) {
      // do nothing
    }
  }
}
