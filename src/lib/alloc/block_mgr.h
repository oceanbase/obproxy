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
