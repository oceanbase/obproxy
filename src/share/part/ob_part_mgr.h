/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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


