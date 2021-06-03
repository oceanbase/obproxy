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

#ifndef _ALLOC_STRUCT_H_
#define _ALLOC_STRUCT_H_

#include <stdint.h>
#include <cstdlib>
#include <cstddef>
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace lib
{
#define ALIGN_UP(x, y) (((x) + ((y) - 1)) / (y) * (y))

// Align chunk we allocate by mmap function to MEMCHK_CHUNK_ALIGN if
// we want to coordinate the owner chunk of an arbitrary memory
// address. Notice size should large enough to hold most mmap
// allocates because we can only process with memory address past in
// ranged between 0 and this size.
static const uint64_t MEMCHK_CHUNK_ALIGN_BITS = 20;
static const uint64_t MEMCHK_CHUNK_ALIGN      = 4UL << MEMCHK_CHUNK_ALIGN_BITS;

static const uint32_t AOBJECT_TAIL_SIZE       = 8;
static const uint32_t MIN_AOBJECT_SIZE        = 16;      // 16 bytes, 2 pointers
static const uint32_t AOBJECT_CELL_BYTES      = 8;     // 8 bytes
static const uint32_t NORMAL_AOBJECT_SIZE     = 1 << 13;  // 8K
static const uint32_t INTACT_ACHUNK_SIZE      = 1 << 21;               // 2M

static const int64_t ALLOC_ABLOCK_CONCURRENCY = 4;

class BlockSet;
class ObjectSet;

enum ObAllocPrio
{
  OB_NORMAL_ALLOC,
  OB_HIGH_ALLOC
};

struct ObMemAttr
{
  uint64_t tenant_id_;
  int64_t mod_id_;
  ObAllocPrio prio_;

  explicit ObMemAttr(
      uint64_t tenant_id = 500,
      int64_t mod_id = 0,
      ObAllocPrio prio = OB_NORMAL_ALLOC)
      : tenant_id_(tenant_id),
        mod_id_(mod_id),
        prio_(prio)
  {}
};

struct AChunk {
  OB_INLINE explicit AChunk(uint64_t size);
  OB_INLINE uint64_t hold() const;

  const uint64_t MAGIC_CODE_;
  uint32_t nblocks_;
  uint32_t nblocks_used_;
  uint64_t size_;
  uint64_t alloc_bytes_;
  AChunk *prev_, *next_;
  char data_[0];
} __attribute__ ((aligned (8)));

struct ABlock {
  OB_INLINE explicit ABlock(BlockSet *block_set);

  OB_INLINE ABlock *phy_next(const int32_t offset) const;
  OB_INLINE AChunk *chunk() const;
  OB_INLINE void clear_magic_code();
  OB_INLINE bool is_first() const;
  OB_INLINE bool is_last() const;
  OB_INLINE bool check_magic_code() const;
  OB_INLINE bool is_valid() const;
  OB_INLINE uint64_t hold() const;

  union {
    uint32_t MAGIC_CODE_;
    struct {
      struct {
        uint8_t in_use_ : 1;
        uint8_t is_large_ : 1;
      };
    };
  };

  uint8_t nblocks_, nblocks_prev_;
  uint8_t block_offset_;
  uint64_t alloc_bytes_;
  BlockSet *block_set_;
  ObjectSet *obj_set_;
  ABlock *prev_, *next_;
  char data_[0];
} __attribute__ ((aligned (8)));

struct AObject {
  OB_INLINE AObject();
  OB_INLINE AObject *phy_next(int32_t steps) const;
  OB_INLINE bool is_valid() const;
  OB_INLINE bool is_first() const;
  OB_INLINE bool is_last() const;
  OB_INLINE ABlock *block();
  OB_INLINE const ABlock *block() const;
  OB_INLINE uint64_t hold() const;

  // members
  union {
    const uint16_t MAGIC_CODE_;
    struct {
      uint16_t in_use_ : 1;
      uint16_t is_large_ : 1;
    };
  } __attribute__((packed));

  uint16_t nobjs_;
  uint16_t nobjs_prev_;
  uint16_t obj_offset_;

  uint32_t mod_id_;
  uint32_t alloc_bytes_;
  uint64_t tenant_id_;

  union {
    struct {
      AObject *prev_, *next_;
    };
    char data_[0];
  };
} __attribute__((aligned (8)));

static const uint64_t AOBJECT_TAIL_MAGIC_CODE     = 0X8F8F7E7EE7E7F8F8;
static const uint32_t ACHUNK_MAGIC_CODE           = 0XCEDEECCC;
static const uint32_t ABLOCK_MAGIC_CODE_MASK      = 0XFFFFFF00;
static const uint32_t ABLOCK_MAGIC_CODE           = 0XCCEEDD00;
static const uint16_t AOBJECT_MAGIC_CODE_MASK     = 0XFFF0;
static const uint16_t FREE_AOBJECT_MAGIC_CODE     = 0XCED0;
static const uint16_t AOBJECT_MAGIC_CODE          = 0XCED1;
static const uint16_t FREE_BIG_AOBJECT_MAGIC_CODE = 0XCED2;
static const uint16_t BIG_AOBJECT_MAGIC_CODE      = 0XCED3;

static const uint32_t AOBJECT_HEADER_SIZE         = offsetof(AObject, data_);
static const uint32_t AOBJECT_META_SIZE           = AOBJECT_HEADER_SIZE + AOBJECT_TAIL_SIZE;
static const uint32_t INTACT_NORMAL_AOBJECT_SIZE  = NORMAL_AOBJECT_SIZE + AOBJECT_META_SIZE;

static const uint32_t ABLOCK_HEADER_SIZE          = offsetof(ABlock, data_);
static const uint32_t ABLOCK_SIZE                 = INTACT_NORMAL_AOBJECT_SIZE;
static const uint32_t INTACT_ABLOCK_SIZE          = ABLOCK_SIZE + ABLOCK_HEADER_SIZE;

static const uint32_t ACHUNK_HEADER_SIZE          = offsetof(AChunk, data_);
static const uint32_t ACHUNK_SIZE                 = INTACT_ACHUNK_SIZE - ACHUNK_HEADER_SIZE;
static const uint32_t INTACT_BIG_ABLOCK_SIZE      = ACHUNK_SIZE;
static const uint32_t BIG_ABLOCK_SIZE             = INTACT_BIG_ABLOCK_SIZE - ABLOCK_HEADER_SIZE;
static const uint32_t INTACT_BIG_AOBJECT_SIZE     = BIG_ABLOCK_SIZE;
static const uint32_t BIG_AOBJECT_SIZE            = INTACT_BIG_AOBJECT_SIZE - AOBJECT_META_SIZE;

static const uint32_t CELLS_PER_BLOCK             = ABLOCK_SIZE / AOBJECT_CELL_BYTES;
static const uint64_t BLOCKS_PER_CHUNK            = ACHUNK_SIZE / INTACT_ABLOCK_SIZE;

static const uint32_t MAX_ABLOCK_COUNT            = ACHUNK_SIZE / ABLOCK_SIZE - 1;
static const uint32_t MAX_ABLOCK_SIZE             = MAX_ABLOCK_COUNT * ABLOCK_SIZE;

inline uint64_t align_up(uint64_t x, uint64_t align)
{
  return (x + (align - 1)) / align * align;
}

inline uint64_t align_up2(uint64_t x, uint64_t align)
{
  return (x + (align - 1)) & ~(align - 1);
}

AChunk::AChunk(uint64_t size) :
    MAGIC_CODE_(ACHUNK_MAGIC_CODE),
    nblocks_(ACHUNK_SIZE / ABLOCK_SIZE - 1),
    nblocks_used_(0),
    size_(size), alloc_bytes_(0),
    prev_(this), next_(this)
{}

uint64_t AChunk::hold() const
{
  return size_ + ACHUNK_HEADER_SIZE;
}


ABlock::ABlock(BlockSet *block_set) :
    MAGIC_CODE_(ABLOCK_MAGIC_CODE),
    nblocks_(0), nblocks_prev_(0),
    block_offset_(0), alloc_bytes_(0),
    block_set_(block_set), obj_set_(NULL),
    prev_(this), next_(this)
{}

ABlock *ABlock::phy_next(const int32_t offset) const
{
  ABlock *block = (ABlock *)((char*)this + static_cast<int32_t>(offset * INTACT_ABLOCK_SIZE));
  return block;
}

AChunk *ABlock::chunk() const
{
  return (AChunk*)((char*)this
                   - block_offset_ * INTACT_ABLOCK_SIZE
                   - ACHUNK_HEADER_SIZE);
}

void ABlock::clear_magic_code()
{
  MAGIC_CODE_ = (~ABLOCK_MAGIC_CODE & ~((1 << 8) - 1)) | (MAGIC_CODE_ & ((1 << 8) - 1));
}

bool ABlock::is_first() const
{
  return 0 == block_offset_;
}

bool ABlock::is_last() const
{
  return block_offset_ + nblocks_ >= BLOCKS_PER_CHUNK;
}

bool ABlock::check_magic_code() const
{
  return MAGIC_CODE_ >> 8 == ABLOCK_MAGIC_CODE >> 8;
}

bool ABlock::is_valid() const
{
  return (MAGIC_CODE_ & ABLOCK_MAGIC_CODE_MASK) == ABLOCK_MAGIC_CODE;
}

uint64_t ABlock::hold() const
{
  return is_large_ || (is_first() && is_last())
      ? chunk()->hold()
      : nblocks_ * INTACT_ABLOCK_SIZE;
}


AObject::AObject()
    : MAGIC_CODE_(FREE_AOBJECT_MAGIC_CODE),
      nobjs_(0), nobjs_prev_(0), obj_offset_(0),
      mod_id_(0), alloc_bytes_(0), tenant_id_(0)
{
}

AObject *AObject::phy_next(int32_t steps) const
{
  AObject *obj = (AObject *)((char*)this + static_cast<int32_t>(steps * AOBJECT_CELL_BYTES));
  return obj;
}

bool AObject::is_valid() const
{
  return (MAGIC_CODE_ & AOBJECT_MAGIC_CODE_MASK) == FREE_AOBJECT_MAGIC_CODE;
}

bool AObject::is_first() const
{
  return 0 == obj_offset_;
}

bool AObject::is_last() const
{
  return obj_offset_ + nobjs_ >= CELLS_PER_BLOCK;
}

ABlock *AObject::block()
{
  return (ABlock*)((char*)this
                   - obj_offset_ * AOBJECT_CELL_BYTES
                   - ABLOCK_HEADER_SIZE);
}

const ABlock *AObject::block() const
{
  return (ABlock*)((char*)this
                   - obj_offset_ * AOBJECT_CELL_BYTES
                   - ABLOCK_HEADER_SIZE);
}

uint64_t AObject::hold() const
{
  return is_large_ || (is_last() && is_first())
      ? block()->hold()
      : nobjs_ * AOBJECT_CELL_BYTES;
}


} // end of namespace lib
} // end of namespace oceanbase

#endif /* _ALLOC_STRUCT_H_ */
