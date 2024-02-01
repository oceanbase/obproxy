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

#ifndef OCEANBASE_MYSQL_PROXY_H_
#define OCEANBASE_MYSQL_PROXY_H_

#include "ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_statement.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"

namespace oceanbase
{
namespace common
{
namespace sqlclient
{
class ObISQLConnection;
class ObISQLConnectionPool;
}

// thread safe sql proxy
// TODO : implement retry logic by general method (macros e.t.)
class ObMySQLProxy : public ObISQLClient
{
public:
  // FIXME : remove this typedef?
  typedef ReadResult MySQLResult;

  ObMySQLProxy();
  virtual ~ObMySQLProxy();

  static int add_slashes(const char *from, const int64_t from_size,
                         char *to, const int64_t to_size, int64_t &out_size);

  // init the connection pool
  int init(sqlclient::ObISQLConnectionPool *pool);

  virtual int escape(const char *from, const int64_t from_size,
      char *to, const int64_t to_size, int64_t &out_size);

  // execute query and return data result
  virtual int read(ReadResult &res, const char *sql);
  virtual int read(ReadResult &res, const uint64_t tenant_id, const char *sql);
  // execute update sql
  virtual int write(const char *sql, int64_t &affected_rows);
  virtual int write(const uint64_t tenant_id, const char *sql, int64_t &affected_rows);

  bool is_inited() const { return NULL != pool_; }
  sqlclient::ObISQLConnectionPool *get_pool(void) { return pool_; }

  bool is_active() const { return active_; }
  void set_active() { active_ = true; }
  void set_inactive() { active_ = false; }

  // can only use assign() to copy to prevent passing ObMySQLProxy by value unintentionally.
  void assign(const ObMySQLProxy &proxy) { *this = proxy; }
private:
  // relase the connection
  void close(sqlclient::ObISQLConnection *conn, const int succ);
private:
  volatile bool active_;
  sqlclient::ObISQLConnectionPool *pool_;

  DISALLOW_COPY_AND_ASSIGN(ObMySQLProxy);
};

// SQLXXX_APPEND macros for appending class member to insert sql
#define SQL_APPEND_COLUMN_NAME(sql, values, column) \
    do { \
      if (OB_SUCC(ret)) { \
        if (OB_SUCCESS != (ret = sql.append_fmt("%s%s", \
            (values).empty() ? "" : ", ", (column)))) { \
          _OB_LOG(WDIAG, "sql append column %s failed, ret %d", (column), ret); \
        } \
      } \
    } while (false)

#define SQL_COL_APPEND_VALUE(sql, values, v, column, fmt) \
    do { \
      SQL_APPEND_COLUMN_NAME(sql, values, column); \
      if (OB_SUCC(ret)) { \
        if (OB_SUCCESS != (ret = (values).append_fmt( \
            "%s" fmt, (values).empty() ? "" : ", ", (v)))) { \
          _OB_LOG(WDIAG, "sql append value failed, ret %d, " #v " " fmt, ret, (v)); \
        } \
      } \
    } while (false)
#define SQL_COL_APPEND_STR_VALUE(sql, values, v, v_len, column) \
    do { \
      SQL_APPEND_COLUMN_NAME(sql, values, column); \
      if (OB_SUCC(ret)) { \
        if (OB_SUCCESS != (ret = (values).append_fmt( \
            "%s'%.*s'", (values).empty() ? "" : ", ", \
            static_cast<int32_t>(v_len), (v)))) { \
          _OB_LOG(WDIAG, "sql append value failed, ret %d, " #v " %.*s", \
              ret, static_cast<int32_t>(v_len), (v)); \
        } \
      } \
    } while (false)
#define SQL_COL_APPEND_CSTR_VALUE(sql, values, v, column) \
    SQL_COL_APPEND_STR_VALUE(sql, values, v, strlen(v), column)
#define SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, v, v_len, column) \
    do { \
      SQL_APPEND_COLUMN_NAME(sql, values, column); \
      if (OB_SUCC(ret)) { \
        if (OB_SUCCESS != (ret = (values).append((values).empty() ? "'" : ", "))) { \
          _OB_LOG(WDIAG, "sql append ', ' failed, ret %d", ret); \
        } else if (OB_SUCCESS != (ret = sql_append_hex_escape_str((v), v_len, (values)))) { \
          _OB_LOG(WDIAG, "sql append escaped value failed, ret %d, " #v " %.*s", \
              ret, static_cast<int32_t>((v_len)), (v)); \
        } \
      } \
    } while (false)

#define SQL_COL_APPEND_ESCAPE_CSTR_VALUE(sql, values, v, column) \
    SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, v, strlen(v), column)

#define SQL_APPEND_VALUE(sql, values, obj, member, fmt) \
    SQL_COL_APPEND_VALUE(sql, values, (obj).member##_, #member, fmt)

#define SQL_APPEND_INT_VALUE(sql, values, obj, member) \
    SQL_COL_APPEND_VALUE(sql, values, static_cast<int64_t>((obj).member##_), #member, "%ld")

#define SQL_APPEND_CSTR_VALUE(sql, values, obj, member) \
    SQL_COL_APPEND_CSTR_VALUE(sql, values, (obj).member##_, #member)

#define SQL_APPEND_STR_VALUE(sql, values, obj, member) \
    SQL_COL_APPEND_STR_VALUE( \
        sql, values, (obj).member##_.ptr(), (obj).member##_.length(), #member)

#define SQL_APPEND_ESCAPE_CSTR_VALUE(sql, values, obj, member) \
    SQL_COL_APPEND_ESCAPE_CSTR_VALUE(sql, values, (obj).member##_, #member)

#define SQL_APPEND_ESCAPE_STR_VALUE(sql, values, obj, member) \
    SQL_COL_APPEND_ESCAPE_STR_VALUE( \
        sql, values, (obj).member##_.ptr(), (obj).member##_.length(), #member)


}
}

#endif // OCEANBASE_MYSQL_PROXY_H_
