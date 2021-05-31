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

#ifndef OCEANBASE_MYSQL_PROXY_OB_ISQL_CLIENT_H_
#define OCEANBASE_MYSQL_PROXY_OB_ISQL_CLIENT_H_

#include "lib/allocator/page_arena.h"

namespace oceanbase
{
namespace common
{

namespace sqlclient
{
class ObISQLResultHandler;
class ObMySQLResult;
};

inline bool is_zero_row(const int64_t row_count) { return 0 == row_count; }
inline bool is_single_row(const int64_t row_count) { return 1 == row_count; }
inline bool is_double_row(const int64_t row_count) { return 2 == row_count; }


class ObISQLClient
{
public:
  class ReadResult;

  ObISQLClient() {}
  virtual ~ObISQLClient() {}

  // sql string escape
  virtual int escape(const char *from, const int64_t from_size,
      char *to, const int64_t to_size, int64_t &out_size) = 0;
  virtual int escape(const char *from, const int64_t from_size,
      char *to, const int64_t to_size);

  // FIXME : replace 'const char *' with 'const ObString &'
  // execute query and return data result
  virtual int read(ReadResult &res, const char *sql) = 0;
  virtual int read(ReadResult &res, const uint64_t tenant_id, const char *sql) = 0;
  // execute update sql
  virtual int write(const char *sql, int64_t &affected_rows) = 0;
  virtual int write(const uint64_t tenant_id, const char *sql, int64_t &affected_rows) = 0;

  class ReadResult
  {
  public:
    friend class ObISQLClient;

    ReadResult();
    virtual ~ReadResult();

    sqlclient::ObMySQLResult *mysql_result();
    // FIXME : remove
    sqlclient::ObMySQLResult *get_result() { return mysql_result(); }

    void reset();
    void reuse();
  private:
    sqlclient::ObISQLResultHandler *result_handler_;
    ObArenaAllocator allocator_;
  };

protected:
  sqlclient::ObISQLResultHandler *&get_result_handler(ReadResult &res)
  { return res.result_handler_; }
  ObIAllocator &get_allocator(ReadResult &res) { return res.allocator_; }

};

} // end namespace common
} // end namespace oceanbase
#endif // OCEANBASE_MYSQL_PROXY_OB_ISQL_CLIENT_H_
