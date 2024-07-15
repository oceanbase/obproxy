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

#ifndef __OB_COMMON_SQLCLIENT_OB_MYSQL_RESULT__
#define __OB_COMMON_SQLCLIENT_OB_MYSQL_RESULT__
#include <mariadb/mysql.h>
#include "lib/mysqlclient/ob_mysql_result.h"


namespace oceanbase
{
namespace common
{
namespace sqlclient
{
class ObMySQLStatement;
class ObMySQLResultImpl : public ObMySQLResult
{
  friend class ObMySQLResultWriter;
  friend class ObMySQLResultHeader;
public:
  explicit ObMySQLResultImpl(ObMySQLStatement &stmt);
  ~ObMySQLResultImpl();
  int init();
  /*
   * close result
   */
  int close();
  /*
   * row count
   */
  // must using store mode after mysql_store_result()
  int64_t get_row_count(void) const;

  /*
   * move result cursor to next row
   */
  int next();
  /*
   * read int/str/TODO from result set
   * col_idx: indicate which column to read, [0, max_read_col)
   */
  int get_int(const int64_t col_idx, int64_t &int_val) const;
  int get_uint(const int64_t col_idx, uint64_t &int_val) const;
  int get_datetime(const int64_t col_idx, int64_t &datetime) const;
  int get_bool(const int64_t col_idx, bool &bool_val) const;
  int get_varchar(const int64_t col_idx, common::ObString &varchar_val) const;
  int get_float(const int64_t col_idx, float &float_val) const;
  int get_double(const int64_t col_idx, double &double_val) const;
  /*
   * read int/str/TODO from result set
   * col_name: indicate which column to read
   * @return  OB_INVALID_PARAM if col_name does not exsit
   */
  int get_int(const char *col_name, int64_t &int_val) const;
  int get_uint(const char *col_name, uint64_t &int_val) const;
  int get_datetime(const char *col_name, int64_t &datetime) const;
  int get_bool(const char *col_name, bool &bool_val) const;
  int get_varchar(const char *col_name, common::ObString &varchar_val) const;
  int get_float(const char *col_name, float &float_val) const;
  int get_double(const char *col_name, double &double_val) const;

  //debug function
  int print_info() const;
  int64_t get_column_count() const;

private:
  int get_column_index(const char *col_name, int64_t &index) const;
  int get_special_value(const common::ObString &varchar_val) const;
  int get_number(const int64_t col_idx, common::number::ObNumber &nmb_val,
                  IAllocator &allocator) const;
  int get_number(const char *col_name, common::number::ObNumber &nmb_val,
                  IAllocator &allocator) const;
private:
  ObMySQLStatement &stmt_;
  MYSQL_RES *result_;
  MYSQL_ROW cur_row_;
  int result_column_count_;
  unsigned long *cur_row_result_lengths_;
  hash::ObHashMap<ObString, int64_t, hash::NoPthreadDefendMode> column_map_;
  MYSQL_FIELD *fields_;
};

}
}
}
#endif
