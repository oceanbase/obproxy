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

#include "block_set.h"

#include <sys/mman.h>
#include "lib/allocator/ob_mod_define.h"
#include "lib/allocator/ob_tc_malloc.h"
#include "lib/ob_define.h"
#include "lib/alloc/ob_tenant_allocator.h"

using namespace oceanbase;
using namespace oceanbase::lib;
using namespace obsys;

BlockSet::BlockSet()
    : mutex_(), block_list_(), clist_(NULL),
      avail_bm_(), total_hold_(0), tallocator_(NULL)
{

}

BlockSet::~BlockSet()
{

}

ABlock *BlockSet::alloc_block(const uint64_t size, const ObMemAttr &attr)
{
  const uint64_t alloc_size = size;
  const uint64_t all_size   = alloc_size + ABLOCK_HEADER_SIZE;
  const uint32_t cls        = (uint32_t)(1 + (all_size - 1) / INTACT_ABLOCK_SIZE);
  ABlock *block             = NULL;

  if (size >= UINT32_MAX) {
    // not support
  } else if (0 == size) {
    // size is zero
  } else if (cls <= BLOCKS_PER_CHUNK) {
    // can fit in a chunk
    block = get_free_block(cls, attr);
  } else {
    AChunk *chunk = alloc_chunk(all_size, attr);
    if (chunk) {
      block = new (chunk->data_) ABlock(this);
      block->in_use_ = true;
      block->is_large_ = true;
    }
  }

  if (NULL != block) {
    block->alloc_bytes_ = size;
  }

  return block;
}

void BlockSet::free_block(ABlock *const block)
{
  if (NULL == block) {
    // nothing
  } else {
    check_block(block);

    if (block->is_large_) {
      free_chunk(block->chunk());
    } else {
      check_block(block);

      ABlock *prev_block = NULL;
      ABlock *next_block = NULL;
      ABlock *next_next_block = NULL;

      if (block->block_offset_ != 0) {
        prev_block = block->phy_next(-block->nblocks_prev_);
        if (!prev_block->in_use_) {
          take_off_free_block(prev_block);

          block->clear_magic_code();
        }
      }

      if (!block->is_last()) {
        next_block = block->phy_next(block->nblocks_);
        if (!next_block->in_use_) {
          take_off_free_block(next_block);

          if (!next_block->is_last()) {
            next_next_block = next_block->phy_next(next_block->nblocks_);
          }

          next_block->clear_magic_code();
        }
      }

      ABlock *head = NULL != prev_block && !prev_block->in_use_ ? prev_block : block;
      ABlock *tail = NULL != next_block && !next_block->in_use_ ? next_next_block : next_block;

      // head won't been NULL,
      if (head != NULL) {
        if (tail != NULL) {
          head->nblocks_ = static_cast<uint8_t>(tail->block_offset_ - head->block_offset_);
          tail->nblocks_prev_ = head->nblocks_;
        } else {
          head->nblocks_ = static_cast<uint8_t>(BLOCKS_PER_CHUNK - head->block_offset_);
        }
        head->in_use_ = false;

        if (head->nblocks_ == BLOCKS_PER_CHUNK) {
          free_chunk(head->chunk());
        } else {
          add_free_block(head);
        }
      }
    }
  }
}

void BlockSet::add_free_block(ABlock *block)
{
  abort_unless(NULL != block && !block->in_use_);

  ABlock *&blist = block_list_[block->nblocks_];
  if (avail_bm_.isset(block->nblocks_)) {
    block->next_ = blist;
    block->prev_ = blist->prev_;
    blist->prev_->next_ = block;
    blist->prev_ = block;
    blist = block;
  } else {
    block->prev_ = block->next_ = block;
    blist = block;
    avail_bm_.set(block->nblocks_);
  }
}

ABlock* BlockSet::get_free_block(const int cls, const ObMemAttr &attr)
{
  ABlock *block = NULL;

  const int ffs = avail_bm_.find_first_significant(cls);
  if (ffs >= 0) {
    if (NULL != block_list_[ffs]) {  // exist
      block = block_list_[ffs];
      if (block->next_ != block) {  // not the only one
        block->prev_->next_ = block->next_;
        block->next_->prev_ = block->prev_;
        block_list_[ffs] = block->next_;
      } else {
        avail_bm_.unset(ffs);
        block_list_[ffs] = NULL;
      }
      block->in_use_ = true;
    }
  }

  // put back into another block list if need be.
  if (NULL != block && ffs > cls) {
    block->nblocks_ = static_cast<uint8_t>(cls);

    // contruct a new block at right position
    ABlock *next_block = new (block->phy_next(cls)) ABlock(this);
    next_block->nblocks_prev_ = static_cast<uint8_t>(cls);
    next_block->nblocks_      = (uint8_t)(ffs - cls);
    next_block->block_offset_ = (uint8_t)(block->block_offset_ + cls);

    add_free_block(next_block);

    if (next_block->nblocks_ + next_block->block_offset_ < BLOCKS_PER_CHUNK) {
      ABlock *next_next_block = next_block->phy_next(next_block->nblocks_);
      next_next_block->nblocks_prev_ = next_block->nblocks_;
    }
  }

  if (block == NULL && ffs < 0) {
    if (add_chunk(attr)) {
      block = get_free_block(cls, attr);
    }
  }

  return block;
}

void BlockSet::take_off_free_block(ABlock *block)
{
  abort_unless(NULL != block && !block->in_use_);

  if (block->next_ != block) {
    block->next_->prev_ = block->prev_;
    block->prev_->next_ = block->next_;
    if (block == block_list_[block->nblocks_]) {
      block_list_[block->nblocks_] = block->next_;
    }
  } else {
    avail_bm_.unset(block->nblocks_);
    block_list_[block->nblocks_] = NULL;
  }
}

AChunk *BlockSet::alloc_chunk(const uint64_t size, const ObMemAttr &attr)
{
  int ret = common::OB_SUCCESS;
  AChunk *chunk = NULL;
  if (!OB_ISNULL(tallocator_)) {
    const int64_t chunk_aligned_size = CHUNK_MGR.aligned(size);
    if (OB_SUCC(tallocator_->update_hold(chunk_aligned_size, attr))) {
      chunk = CHUNK_MGR.alloc_chunk(size, OB_HIGH_ALLOC == attr.prio_);
      if (NULL != chunk) {
        if (NULL != clist_) {
          chunk->prev_ = clist_->prev_;
          chunk->next_ = clist_;
          clist_->prev_->next_ = chunk;
          clist_->prev_ = chunk;
        } else {
          chunk->prev_ = chunk->next_ = chunk;
          clist_ = chunk;
        }
      } else {
        IGNORE_RETURN tallocator_->update_hold(-chunk_aligned_size, attr);
      }
    }
  }
  return chunk;
}

bool BlockSet::add_chunk(const ObMemAttr &attr)
{
  AChunk *chunk = alloc_chunk(ACHUNK_SIZE, attr);
  if (NULL != chunk) {
    ABlock *block = new (chunk->data_) ABlock(this);
    block->nblocks_ = BLOCKS_PER_CHUNK;
    add_free_block(block);
  }
  return NULL != chunk;
}

void BlockSet::free_chunk(const AChunk *const chunk)
{
  int ret = common::OB_SUCCESS;

  abort_unless(NULL != chunk);
  abort_unless(ACHUNK_MAGIC_CODE == chunk->MAGIC_CODE_);
  abort_unless(NULL != chunk->next_);
  abort_unless(NULL != chunk->prev_);
  abort_unless(NULL != clist_);

  if (chunk == clist_) {
    clist_ = clist_->next_;
  }

  if (chunk == clist_) {
    clist_ = NULL;
  } else {
    chunk->next_->prev_ = chunk->prev_;
    chunk->prev_->next_ = chunk->next_;
  }

  if (!OB_ISNULL(tallocator_)) {
    const int64_t chunk_aligned_size = chunk->hold();
    ObMemAttr attr;
    if (OB_FAIL(tallocator_->update_hold(-chunk_aligned_size, attr))) {
      // do nothing
    }
  }
  CHUNK_MGR.free_chunk(chunk);
}

void BlockSet::check_block(ABlock *block)
{
  abort_unless(block);
  abort_unless(block->check_magic_code());
}
