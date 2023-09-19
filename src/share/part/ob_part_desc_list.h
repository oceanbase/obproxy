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

#ifndef _OB_PART_DESC_LIST_H
#define _OB_PART_DESC_LIST_H 1

#include "share/part/ob_part_desc.h"
#include "common/ob_row.h"
#include "lib/container/ob_se_array.h"
#include "proxy/route/obproxy_part_mgr.h"

namespace oceanbase
{
namespace common
{
struct ListPartition
{
  int64_t part_id_;
  int64_t first_part_id_;
  ObSEArray<ObNewRow, 8> rows_;

  ListPartition();
  TO_STRING_KV(K_(part_id),
               K_(first_part_id),
               K_(rows));
};

class ObPartDescList : public ObPartDesc
{
public:
  ObPartDescList();
  virtual ~ObPartDescList();

  virtual int get_part(ObNewRange &range,
                       ObIAllocator &allocator,
                       ObIArray<int64_t> &part_ids,
                       ObPartDescCtx &ctx,
                       ObIArray<int64_t> &tablet_ids,
                       int64_t &part_idx);
  virtual int get_part_by_num(const int64_t num, common::ObIArray<int64_t> &part_ids, common::ObIArray<int64_t> &tablet_ids);
  void set_default_part_array_idx(int64_t idx) { default_part_array_idx_ = idx; }
  int64_t get_default_part_array_idx() const { return default_part_array_idx_; }
  ListPartition *get_part_array() { return part_array_; }
  int set_part_array(ListPartition *part_array, int64_t size) {
    part_array_ = part_array;
    part_array_size_ = size;
    return OB_SUCCESS;
  }

  int cast_row(ObNewRow &src_row,
               ObNewRow &target_row,
               ObIAllocator &allocator,
               ObPartDescCtx &ctx);

  DECLARE_VIRTUAL_TO_STRING;

private:
  ListPartition *part_array_;
  int64_t part_array_size_;
  int64_t default_part_array_idx_;
};

} // end common
} // end oceanbase

#endif /* _OB_PART_DESC_LIST_H */


