/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEABASE_LIB_ALLOC_BLOCK_MGR_H_
#define _OCEABASE_LIB_ALLOC_BLOCK_MGR_H_

#include "lib/random/ob_random.h"
#include "alloc_struct.h"
#include "block_set.h"

namespace oceanbase
{
namespace lib
{

class BlockMgr
{
public:
  ABlock *alloc_block(uint64_t size, const ObMemAttr &attr);
  void free_block(ABlock *block);

private:
  BlockSet bs_[ALLOC_ABLOCK_CONCURRENCY];
  common::ObRandom rand_;
}; // end of class BlockMgr

extern BlockMgr *get_block_mgr();

} // end of namespace lib
} // end of namespace oceanbase

#endif /* _OCEABASE_LIB_ALLOC_BLOCK_MGR_H_ */
