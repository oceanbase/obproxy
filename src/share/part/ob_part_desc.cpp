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

#include "share/part/ob_part_desc.h"

namespace oceanbase
{
namespace common
{
int ObPartDesc::get_part(common::ObNewRange &range,
                         common::ObIAllocator &allocator,
                         common::ObIArray<int64_t> &part_ids)
{
  UNUSED(range);
  UNUSED(allocator);
  UNUSED(part_ids);
  return OB_NOT_IMPLEMENT;
}

int ObPartDesc::get_part_by_num(const int64_t num, common::ObIArray<int64_t> &part_ids)
{
  UNUSED(num);
  UNUSED(part_ids);
  return OB_NOT_IMPLEMENT;
}


} // end of common
} // end of oceanbase
