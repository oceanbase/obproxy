/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
