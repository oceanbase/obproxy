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

#ifndef _OB_PART_MGR_H
#define _OB_PART_MGR_H 1

#include "common/ob_range.h"
#include "lib/container/ob_iarray.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace common
{
  class ObPartMgr
  {
    public:
      ObPartMgr() {}
      virtual ~ObPartMgr() {}

      /*
       * get first part or sub part
       * @in param tenant_id
       * @in param table_id
       * @in param part_level: 1 for first part; 2 for sub part. other value invalid
       * @in param part_id first part id
       * @in param range
       * @in param reverse
       * @out param part_ids: list partition id
       */
      virtual int get_part(const uint64_t table_id,
                           const share::schema::ObPartitionLevel part_level,
                           const int64_t part_id,
                           const common::ObNewRange &range,
                           bool reverse,
                           ObIArray<int64_t> &part_ids) = 0;

  };
}
}

#endif /* _OB_PART_MGR_H */


