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

#include "block_mgr.h"

using namespace oceanbase::lib;

ABlock *BlockMgr::alloc_block(uint64_t size, const ObMemAttr &attr)
{
  ABlock *block = NULL;
  bool found = false;
  const uint32_t rnd = rand_.get_int32(0, ALLOC_ABLOCK_CONCURRENCY - 1);
  for (int64_t i = 0; NULL == block && i < ALLOC_ABLOCK_CONCURRENCY && !found; i++) {
    const uint64_t idx = (rnd + i) % ALLOC_ABLOCK_CONCURRENCY;
    if (bs_[idx].trylock()) {
      block = bs_[idx].alloc_block(size, attr);
      bs_[idx].unlock();
      found = true;
      break;
    }
  }
  if (!found && NULL == block) {
    bs_[rnd].lock();
    block = bs_[rnd].alloc_block(size, attr);
    bs_[rnd].unlock();
  }
  return block;
}

void BlockMgr::free_block(ABlock * const block)
{
  if (NULL != block) {
    BlockSet *set = block->block_set_;
    abort_unless(NULL != set);

    if (NULL != set) {
      set->lock();
      set->free_block(block);
      set->unlock();
    }
  }
}

BlockMgr *oceanbase::lib::get_block_mgr()
{
  static BlockMgr *pmgr = NULL;
  static obsys::CThreadMutex mutex;

  if (NULL == pmgr) {
    obsys::CThreadGuard guard(&mutex);
    if (NULL == pmgr) {
      void *buf = AChunkMgr::instance().alloc_chunk(sizeof (BlockMgr));
      if (NULL != buf) {
        pmgr = new (buf) BlockMgr();
      }
    }
  }
  return pmgr;
}
