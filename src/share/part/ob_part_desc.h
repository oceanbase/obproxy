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

#ifndef _OB_PART_DESC_H
#define _OB_PART_DESC_H 1

#include "common/ob_range2.h"
#include "lib/container/ob_iarray.h"
#include "share/part/ob_part_mgr_util.h"

namespace oceanbase
{
namespace common
{
class ObPartDesc
{
public:
  /*
   * get partition id according to range
   * @in param  range
   * @in param  allocator: use to type conversion
   *
   * @out param part_ids: list of part id
   *
   */
  virtual int get_part(common::ObNewRange &range,
                       common::ObIAllocator &allocator,
                       common::ObIArray<int64_t> &part_ids);
  void set_part_level(share::schema::ObPartitionLevel part_level) { part_level_ = part_level; }
  share::schema::ObPartitionLevel get_part_level() { return part_level_; }
  void set_part_func_type(share::schema::ObPartitionFuncType part_func_type) { part_func_type_ = part_func_type; }
  share::schema::ObPartitionFuncType get_part_func_type() { return part_func_type_; }

  ObPartDesc() : part_level_(share::schema::PARTITION_LEVEL_ZERO) {};
  virtual ~ObPartDesc() {};

  DECLARE_VIRTUAL_TO_STRING = 0;
  share::schema::ObPartitionLevel part_level_;
  share::schema::ObPartitionFuncType part_func_type_;
};
}
}

#endif /* _OB_PART_DESC_H */


