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

#include "object_set.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/allocator/ob_tc_malloc.h"
#include "lib/ob_define.h"
#include "block_mgr.h"

using namespace oceanbase::lib;

const double ObjectSet::FREE_LISTS_BUILD_RATIO = .1;
const double ObjectSet::BLOCK_CACHE_RATIO = .1;

ObjectSet::ObjectSet()
    : mutex_(), bs_(), mod_set_(), blist_(NULL), last_remainder_(NULL),
      bm_(NULL), free_lists_(NULL),
      alloc_bytes_(0), used_bytes_(0), hold_bytes_(0), allocs_(0),
      normal_alloc_bytes_(0), normal_used_bytes_(0),
      normal_hold_bytes_(0)
{

}

ObjectSet::~ObjectSet()
{
  reset();
  if (blist_ != NULL) {
    free_block(blist_);
    blist_ = NULL;
  }
}

AObject *ObjectSet::alloc_object(
    const uint64_t size, const ObMemAttr &attr)
{
  const uint64_t alloc_size = size;
  const uint64_t adj_size = MAX(alloc_size, MIN_AOBJECT_SIZE);
  const uint64_t all_size = adj_size + AOBJECT_META_SIZE;

  AObject *obj = NULL;

  if (alloc_size > UINT32_MAX || 0 == alloc_size) {
    // not support
  } else if (all_size <= INTACT_NORMAL_AOBJECT_SIZE) {
    const uint32_t cls = (uint32_t)(1+ ((all_size - 1) / AOBJECT_CELL_BYTES));
    obj = alloc_normal_object(cls, attr);
    if (NULL != obj) {
      normal_alloc_bytes_ += alloc_size;
      normal_used_bytes_ += all_size;
    }
  } else {
    obj = alloc_big_object(adj_size, attr);
    abort_unless(NULL == obj || obj->in_use_);
  }

  if (NULL != obj) {
    abort_unless(obj->in_use_);
    abort_unless(obj->is_valid());

    obj->mod_id_ = static_cast<int32_t>(attr.mod_id_);

    reinterpret_cast<uint64_t&>(obj->data_[alloc_size]) = AOBJECT_TAIL_MAGIC_CODE;
    obj->alloc_bytes_ = static_cast<uint32_t>(alloc_size);

    allocs_++;
    alloc_bytes_ += alloc_size;
    used_bytes_ += all_size;

    mod_set_.mod_update(
        attr.mod_id_,
        obj->hold(),
        static_cast<int64_t>(obj->alloc_bytes_));
  }

  return obj;
}

AObject *ObjectSet::realloc_object(
    AObject *obj, const uint64_t size, const ObMemAttr &attr)
{
  AObject *new_obj = NULL;
  uint64_t copy_size = 0;

  if (NULL == obj) {
    new_obj = alloc_object(size, attr);
  } else {
    abort_unless(obj->is_valid());
    if (obj->is_large_) {
      copy_size = MIN(obj->alloc_bytes_, size);
    } else {
      copy_size = MIN(
          size, (obj->nobjs_ - META_CELLS) * AOBJECT_CELL_BYTES);
    }

    new_obj = alloc_object(size, attr);
    if (NULL != new_obj && copy_size != 0) {
      memmove(new_obj->data_, obj->data_, copy_size);
    }

    free_object(obj);
  }

  return new_obj;
}

AObject *ObjectSet::alloc_normal_object(const uint32_t cls, const ObMemAttr &attr)
{
  AObject *obj = NULL;

  // best fit
  if (NULL != bm_
      && NULL != free_lists_
      && bm_->isset(cls)) {
    obj = (*free_lists_)[cls];
    take_off_free_object(obj);
    obj->in_use_ = true;
  }

  // last remainder
  if (NULL == obj && NULL != last_remainder_) {
    if (cls <= last_remainder_->nobjs_) {
      obj = split_obj(last_remainder_, cls, last_remainder_);
      obj->in_use_ = true;
    }
  }

  // next first fit
  if (NULL == obj && NULL != bm_ && NULL != free_lists_) {
    obj = get_free_object(cls);
    if (obj != NULL) {
      obj->in_use_ = true;
    }
  }

  // alloc new block, split into two part:
  //
  //   1. with cls cells that return to user
  //   2. the remainder as last remainder
  //
  if (NULL == obj) {
    if (NULL != bm_
        && NULL != free_lists_
        && NULL != last_remainder_) {
      AObject * const lrback = last_remainder_;
      last_remainder_ = NULL;
      add_free_object(merge_obj(lrback));
    }

    ABlock *block = alloc_block(ABLOCK_SIZE, attr);
    if (NULL != block) {
      normal_hold_bytes_ += ABLOCK_SIZE;

      AObject *remainder = NULL;
      obj = new (block->data_) AObject();
      obj->nobjs_ = static_cast<uint16_t>(CELLS_PER_BLOCK);
      obj = split_obj(obj, cls, remainder);
      obj->in_use_ = true;
      if (remainder != NULL) {
        last_remainder_ = remainder;
      }
    }
  }

  return obj;
}

AObject *ObjectSet::get_free_object(const uint32_t cls)
{
  AObject *obj = NULL;

  if (NULL != bm_ && NULL != free_lists_) {
    const int ffs = bm_->find_first_significant(cls);
    if (ffs >= 0) {
      if (free_lists_[ffs] != NULL) {
        obj = (*free_lists_)[ffs];
      }
    }

    if (ffs >= 0 && NULL != obj && ffs >= static_cast<int32_t>(cls)) {
      if (NULL != last_remainder_
          && obj->block() == last_remainder_->block()
          && (obj->phy_next(obj->nobjs_) == last_remainder_
              || last_remainder_->phy_next(last_remainder_->nobjs_) == obj)) {
        last_remainder_ = merge_obj(last_remainder_);
      } else {
        if (NULL != last_remainder_) {
          add_free_object(last_remainder_);
          last_remainder_ = NULL;
        }
        take_off_free_object(obj);
        last_remainder_ = obj;
      }
      obj = split_obj(last_remainder_, cls, last_remainder_);
    }
  }

  if (NULL != obj) {
    abort_unless(obj->is_valid());
  }

  return obj;
}

void ObjectSet::add_free_object(AObject *obj)
{
  abort_unless(NULL != obj);
  abort_unless(NULL != bm_ && NULL != free_lists_);
  abort_unless(obj->is_valid());

  if (obj->nobjs_ >= MIN_FREE_CELLS) {
    AObject *&head = (*free_lists_)[obj->nobjs_];
    if (bm_->isset(obj->nobjs_)) {
      obj->prev_ = head->prev_;
      obj->next_ = head;
      obj->prev_->next_ = obj;
      obj->next_->prev_ = obj;
      head = obj;
    } else {
      bm_->set(obj->nobjs_);
      obj->prev_ = obj;
      obj->next_ = obj;
      head = obj;
    }
  }
}

void ObjectSet::take_off_free_object(AObject *obj)
{
  abort_unless(NULL != obj);
  abort_unless(NULL != bm_ && NULL != free_lists_);
  abort_unless(obj->is_valid());

  if (obj->in_use_) {
  } else if (obj->nobjs_ < MIN_FREE_CELLS) {
  } else if (obj->next_ == obj) {
    bm_->unset(obj->nobjs_);
  } else {
    AObject *&head = (*free_lists_)[obj->nobjs_];
    if (head == obj) {
      head = head->next_;
    }
    obj->prev_->next_ = obj->next_;
    obj->next_->prev_ = obj->prev_;
  }
}

void ObjectSet::free_normal_object(AObject *obj)
{
  abort_unless(NULL != obj);
  abort_unless(obj->is_valid());

  normal_alloc_bytes_ -= obj->alloc_bytes_;
  normal_used_bytes_ -= obj->nobjs_ * AOBJECT_CELL_BYTES;

  if (NULL == bm_ || NULL == free_lists_) {
    obj->in_use_ = false;
  } else {
    AObject *newobj = merge_obj(obj);

    if (newobj->nobjs_ == CELLS_PER_BLOCK) {
      // we can cache `BLOCK_CACHE_RATIO` of hold blocks
      if (normal_used_bytes_ == 0 ||
          ((double)normal_used_bytes_
           > (double)normal_hold_bytes_ * (1. - BLOCK_CACHE_RATIO))) {
        add_free_object(newobj);
      } else {
        hold_bytes_ -= ABLOCK_SIZE;
        normal_hold_bytes_ -= ABLOCK_SIZE;
        free_block(newobj->block());
      }
    } else {
      add_free_object(newobj);
    }
  }
}

ABlock *ObjectSet::alloc_block(const uint64_t size, const ObMemAttr &attr)
{
  ABlock *block = bs_.alloc_block(size, attr);

  if (NULL != block) {
    if (NULL != blist_) {
      block->prev_ = blist_->prev_;
      block->next_ = blist_;
      blist_->prev_->next_ = block;
      blist_->prev_ = block;
    } else {
      block->prev_ = block->next_ = block;
      blist_ = block;
    }
    hold_bytes_ += size;
    block->obj_set_ = this;
  }

  return block;
}

void ObjectSet::free_block(ABlock *block)
{
  abort_unless(NULL != block);
  abort_unless(block->is_valid());

  if (block == blist_) {
    blist_ = blist_->next_;
    if (block == blist_) {
      blist_ = NULL;
    }
  }

  block->prev_->next_ = block->next_;
  block->next_->prev_ = block->prev_;

  // The pbmgr shouldn't be NULL or there'll be memory leak.
  bs_.free_block(block);
}

AObject *ObjectSet::alloc_big_object(const uint64_t size, const ObMemAttr &attr)
{
  AObject *obj = NULL;
  ABlock *block = alloc_block(size + AOBJECT_META_SIZE, attr);

  if (NULL != block) {
    obj = new (block->data_) AObject();
    obj->is_large_ = true;
    obj->in_use_ = true;
    obj->alloc_bytes_ = static_cast<uint32_t>(size);
  }

  return obj;
}

void ObjectSet::free_big_object(AObject *obj)
{
  abort_unless(NULL != obj);
  abort_unless(NULL != obj->block());
  abort_unless(obj->is_valid());

  hold_bytes_ -= obj->alloc_bytes_ + AOBJECT_META_SIZE;

  free_block(obj->block());
}

void ObjectSet::free_object(AObject *obj)
{
  abort_unless(obj != NULL);
  abort_unless(obj->is_valid());
  abort_unless(
      AOBJECT_TAIL_MAGIC_CODE
      == reinterpret_cast<uint64_t&>(obj->data_[obj->alloc_bytes_]));
//  OB_LOG(DEBUG, "free", "ptr", P(obj->data_));

  if (obj->MAGIC_CODE_ == AOBJECT_MAGIC_CODE
      || obj->MAGIC_CODE_ == BIG_AOBJECT_MAGIC_CODE) {
    abort_unless(obj->in_use_);

    const int64_t hold = obj->hold();
    const int64_t used = obj->alloc_bytes_;

    mod_set_.mod_update(obj->mod_id_, -hold, -used);

    alloc_bytes_ -= obj->alloc_bytes_;
    used_bytes_ -= hold;

    obj->in_use_ = false;
    if (!obj->is_large_) {
      free_normal_object(obj);
    } else {
      free_big_object(obj);
    }
  }

  // 1. havn't created free lists
  // 2. touch free lists build ratio
  if (NULL == bm_
      && NULL == free_lists_
      && normal_hold_bytes_ > ABLOCK_SIZE
      && (static_cast<double>(normal_used_bytes_)
          < static_cast<double>(normal_hold_bytes_)
          * (1. - FREE_LISTS_BUILD_RATIO))) {
    build_free_lists();
    last_remainder_ = NULL;
  }
}

void ObjectSet::reset()
{
  while (NULL != blist_) {
    if (blist_->next_ == blist_) {  // The only block
      last_remainder_ = new (blist_->data_) AObject();
      last_remainder_->nobjs_ = static_cast<uint16_t>(CELLS_PER_BLOCK);
      last_remainder_->in_use_ = false;
      normal_hold_bytes_ = hold_bytes_ = ABLOCK_SIZE;
      break;
    }
    free_block(blist_);
  }

  bm_ = NULL;
  free_lists_ = NULL;
  // last_remainder_ = NULL;

  alloc_bytes_ = 0;
  used_bytes_ = 0;
  allocs_ = 0;

  normal_alloc_bytes_ = 0;
  normal_used_bytes_ = 0;
  mod_set_.reset();
}

bool ObjectSet::build_free_lists()
{
  abort_unless(NULL == bm_ && NULL == free_lists_);
  ObMemAttr attr;
  attr.mod_id_ = common::ObModIds::OB_OBJ_FREELISTS;
  ABlock *new_block = alloc_block(sizeof (FreeLists) + sizeof (BitMap), attr);

  OB_LOG(INFO, "build free lists");

  if (NULL != new_block) {
    free_lists_ = new (new_block->data_) FreeLists[0];
    bm_ = new (&new_block->data_[sizeof (FreeLists)]) BitMap();

    // the new block is at tail of blist_, the BREAK and CONTINUE is
    // necessary otherwise the code will be very ugly and bug prone.
    for (ABlock *block = blist_;
         block != new_block;
         block = block->next_) {
      AObject *obj = reinterpret_cast<AObject*>(block->data_);
      abort_unless(obj->is_valid());

      // ignore large object
      if (!obj->is_large_) {
        for (;;) {
          while (obj->in_use_ && !obj->is_last()) {
            AObject *next_obj = obj->phy_next(obj->nobjs_);
            next_obj->nobjs_prev_ = obj->nobjs_;
            obj = next_obj;
            abort_unless(obj->is_valid());
          }
          if (obj->is_last()) {
            obj->nobjs_ = static_cast<uint16_t>(CELLS_PER_BLOCK - obj->obj_offset_);
            if (!obj->in_use_) {
              add_free_object(obj);
            }
            break;
          }
          // the first object not in use
          AObject *first = obj;
          obj = obj->phy_next(obj->nobjs_);
          abort_unless(obj->is_valid());
          while (!obj->in_use_ && !obj->is_last()) {
            obj = obj->phy_next(obj->nobjs_);
            abort_unless(obj->is_valid());
          }
          if (obj->is_last() && !obj->in_use_) {
            first->nobjs_ = static_cast<uint16_t>(CELLS_PER_BLOCK - first->obj_offset_);
            add_free_object(first);
            break;
          } else {
            first->nobjs_ = static_cast<uint16_t>(obj->obj_offset_ - first->obj_offset_);
            obj->nobjs_prev_ = first->nobjs_;
            add_free_object(first);
          }
        } // for one block
      } // if (!obj->is_large_)
    } // for all block
  }

  return new_block != NULL;
}
