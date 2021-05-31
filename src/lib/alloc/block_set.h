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

#ifndef _OCEABASE_LIB_ALLOC_BLOCK_SET_H_
#define _OCEABASE_LIB_ALLOC_BLOCK_SET_H_

#include "lib/tbsys.h"
#include "lib/ob_define.h"
#include "alloc_struct.h"
#include "achunk_mgr.h"

namespace oceanbase
{
namespace lib
{

class ObTenantAllocator;
class BlockSet
{
public:
  BlockSet();
  ~BlockSet();

  ABlock *alloc_block(const uint64_t size, const ObMemAttr &attr);
  void free_block(ABlock * const block);

  inline void lock();
  inline void unlock();
  inline bool trylock();

  // tempory
  inline uint64_t get_hold(uint64_t size) const;
  inline uint64_t get_total_hold() const;

  void set_tenant_allocator(ObTenantAllocator &allocator);

private:
  DISALLOW_COPY_AND_ASSIGN(BlockSet);

  void add_free_block(ABlock *block);
  ABlock *get_free_block(const int cls, const ObMemAttr &attr);
  void take_off_free_block(ABlock *block);
  AChunk *alloc_chunk(const uint64_t size, const ObMemAttr &attr);
  bool add_chunk(const ObMemAttr &attr);
  void free_chunk(const AChunk *const chunk);
  void check_block(ABlock *block);

private:
  obsys::CThreadMutex mutex_;
  ABlock *block_list_[BLOCKS_PER_CHUNK+1];
  AChunk *clist_;  // using chunk list
  ABitSet<BLOCKS_PER_CHUNK+1> avail_bm_;
  uint64_t total_hold_;
  ObTenantAllocator *tallocator_;
}; // end of class BlockSet

void BlockSet::lock()
{
  mutex_.lock();
}

void BlockSet::unlock()
{
  mutex_.unlock();
}

bool BlockSet::trylock()
{
  return 0 == mutex_.trylock();
}

uint64_t BlockSet::get_hold(uint64_t size) const
{
  const uint32_t align_size = (uint32_t)align_up(size, ABLOCK_SIZE);
  uint64_t hold_size = align_size + ABLOCK_HEADER_SIZE;
  if (align_size > MAX_ABLOCK_SIZE) {
    hold_size += ACHUNK_HEADER_SIZE;
  }
  return hold_size;
}

uint64_t BlockSet::get_total_hold() const
{
  return total_hold_;
}

inline void BlockSet::set_tenant_allocator(ObTenantAllocator &allocator)
{
  tallocator_ = &allocator;
}

} // end of namespace lib
} // end of namespace oceanbase

#endif /* _OCEABASE_LIB_ALLOC_BLOCK_SET_H_ */
