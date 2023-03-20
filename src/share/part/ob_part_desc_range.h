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

#ifndef _OB_PART_DESC_RANGE_H
#define _OB_PART_DESC_RANGE_H 1

#include "share/part/ob_part_desc.h"
#include "lib/container/ob_se_array.h"
#include "proxy/route/obproxy_part_mgr.h"

namespace oceanbase
{
namespace common
{
struct RangePartition
{
  ObRowkey high_bound_val_;
  bool is_max_value_;
  int64_t part_id_;
  int64_t first_part_id_; // sub partition will use this value

  RangePartition();
  static bool less_than(const RangePartition &a, const RangePartition &b);
  TO_STRING_KV(K_(high_bound_val),
               K_(is_max_value),
               K_(part_id),
               K_(first_part_id));
};

class ObPartDescRange : public ObPartDesc
{
public:
  ObPartDescRange();
  virtual ~ObPartDescRange();

  virtual int get_part(common::ObNewRange &range,
                       common::ObIAllocator &allocator,
                       ObIArray<int64_t> &part_ids,
                       ObPartDescCtx &ctx,
                       ObIArray<int64_t> &tablet_ids);
  virtual int get_part_by_num(const int64_t num, common::ObIArray<int64_t> &part_ids, common::ObIArray<int64_t> &tablet_ids);
  RangePartition* get_part_array() { return part_array_; }
  int set_part_array(RangePartition *part_array, int64_t size) {
    part_array_ = part_array;
    part_array_size_ = size;
    return common::OB_SUCCESS;
  }

  int cast_key(ObRowkey &src_key,
               ObRowkey &target_key,
               ObIAllocator &allocator,
               ObPartDescCtx &ctx);

  DECLARE_VIRTUAL_TO_STRING;
private:
  int64_t get_start(const RangePartition *part_array,
                    const int64_t size,
                    const ObNewRange &range);

  int64_t get_end(const RangePartition *part_array,
                  const int64_t size,
                  const ObNewRange &range);

private:
  RangePartition *part_array_;
  int64_t part_array_size_;
};
}
}

#endif /* _OB_PART_DESC_RANGE_H */


