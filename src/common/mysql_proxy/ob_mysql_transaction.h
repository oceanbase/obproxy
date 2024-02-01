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

#ifndef OCEANBASE_MYSQL_TRANSACTION_H_
#define OCEANBASE_MYSQL_TRANSACTION_H_

#include "lib/oblog/ob_log_module.h"
#include "ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_connection.h"

namespace oceanbase
{
namespace common
{
namespace sqlclient
{
class ObISQLConnection;
class ObISQLConnectionPool;
}
class ObMySQLProxy;

// not thread safe sql transaction execution
class ObMySQLTransaction : public ObISQLClient
{
public:
  ObMySQLTransaction();
  virtual ~ObMySQLTransaction();
public:
  // start transaction
  virtual int start(ObMySQLProxy *proxy, bool with_snapshot = false);
  // end the transaction
  virtual int end(const bool commit);
  virtual bool is_started() const { return started_; }

  virtual int escape(const char *from, const int64_t from_size,
      char *to, const int64_t to_size, int64_t &out_size);

  // %res should be destructed before execute other sql or end transaction.
  virtual int read(ReadResult &res, const char *sql);
  virtual int read(ReadResult &res, const uint64_t tenant_id, const char *sql);

  virtual int write(const char *sql, int64_t &affected_rows);
  virtual int write(const uint64_t tenant_id, const char *sql, int64_t &affected_rows);

protected:
  int connect(ObMySQLProxy *proxy);
  int start_transaction(bool with_snap_shot);
  int end_transaction(const bool commit);
  void close();
public:
  bool check_inner_stat(void) const;
protected:
  int errno_;
  int64_t start_time_;
  int64_t statement_count_;
  sqlclient::ObISQLConnection *conn_;
  sqlclient::ObISQLConnectionPool *pool_;
  ObMySQLProxy *sql_proxy_;
  bool started_;
};

inline bool ObMySQLTransaction::check_inner_stat(void) const
{
  bool bret = (OB_SUCCESS == errno_ && NULL != pool_ && NULL != conn_);
  if (!bret) {
    COMMON_MYSQLP_LOG(WDIAG, "invalid inner stat", "errno", errno_, K_(pool), K_(conn));
  }
  return bret;
}

} // end namespace commmon
} // end namespace oceanbase

#endif // OCEANBASE_MYSQL_TRANSACTION_H_
