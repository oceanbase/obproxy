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

#ifndef _OB_MYSQL_ROW_H_
#define _OB_MYSQL_ROW_H_

#include "rpc/obmysql/ob_mysql_util.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obmysql
{

class ObMySQLRow
{
public:
  explicit ObMySQLRow(MYSQL_PROTOCOL_TYPE type);

public:
  /**
   * Serialize the row of data into a format recognized by MySQL, 
   * output position: buf + pos, after execution,
   * pos points to the first free position in buf.
   */
  int serialize(char *buf, const int64_t len, int64_t &pos) const;

protected:
  virtual int64_t get_cells_cnt() const = 0;
  virtual int encode_cell(
      int64_t idx, char *buf,
      int64_t len, int64_t &pos, char *bitmap) const = 0;

protected:
  const MYSQL_PROTOCOL_TYPE type_;
}; // end class ObMySQLRow

} // end of namespace obmysql
} // end of namespace oceanbase

#endif /* _OB_MYSQL_ROW_H_ */
