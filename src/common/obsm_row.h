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

#ifndef _OCEABASE_COMMON_OBSM_ROW_H_
#define _OCEABASE_COMMON_OBSM_ROW_H_

#include "rpc/obmysql/ob_mysql_row.h"
#include "common/ob_row.h"
#include "common/ob_field.h"

namespace oceanbase
{
namespace common
{

class ObSMRow
    : public obmysql::ObMySQLRow
{
public:
  ObSMRow(obmysql::MYSQL_PROTOCOL_TYPE type,
          const ObNewRow &obrow,
          const ObTimeZoneInfo *tz_info = NULL,
          const ObIArray<ObField> *fields = NULL);

  virtual ~ObSMRow() {}

protected:
  virtual int64_t get_cells_cnt() const;
  virtual int encode_cell(
      int64_t idx, char *buf,
      int64_t len, int64_t &pos, char *bitmap) const;

private:
  const ObNewRow &obrow_;
  const ObTimeZoneInfo *tz_info_;
  const ObIArray<ObField> *fields_;

  DISALLOW_COPY_AND_ASSIGN(ObSMRow);
}; // end of class OBMP

} // end of namespace common
} // end of namespace oceanbase

#endif /* _OCEABASE_COMMON_OBSM_ROW_H_ */
