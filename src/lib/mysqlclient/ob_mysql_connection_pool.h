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

#ifndef __COMMON_OB_MYSQL_CONNECTION_POOL__
#define __COMMON_OB_MYSQL_CONNECTION_POOL__

#include <mariadb/mysql.h>
#include "lib/tbsys.h"
#include "lib/container/ob_se_array.h"
#include "lib/task/ob_timer.h"
#include "lib/list/ob_list.h"
#include "lib/allocator/ob_cached_allocator.h"
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/net/ob_addr.h"
#include "ob_isql_connection_pool.h"

namespace oceanbase
{
namespace common
{

struct ObConnPoolConfigParam
{
  ObConnPoolConfigParam() { reset(); }
  ~ObConnPoolConfigParam() { }
  void reset() { memset(this,0, sizeof(ObConnPoolConfigParam)); }

  int64_t sqlclient_wait_timeout_;      // s
  int64_t connection_refresh_interval_; // us
  int64_t connection_pool_warn_time_;   // us
  int64_t long_query_timeout_;          // us
  int64_t sqlclient_per_observer_conn_limit_;
};

class ObMySQLProxy;
class ObMySQLProxyUtil;
class ObMySQLTransaction;
class ObCommonMySQLProvider;
namespace sqlclient
{
class ObServerConnectionPool;
class ObMySQLServerProvider;
class ObMySQLConnectionPool : public common::ObTimerTask, public ObISQLConnectionPool
{
public:
  friend class common::ObMySQLProxy;
  friend class common::ObMySQLProxyUtil;
  friend class common::ObMySQLTransaction;

  static const char *const DEFAULT_DB_USER;
  static const char *const DEFAULT_DB_PASS;
  static const char *const DEFAULT_DB_NAME;
  static const int64_t DEFAULT_TRANSACTION_TIMEOUT_US = 100 * 1000 * 1000;
public:
  ObMySQLConnectionPool();
  ~ObMySQLConnectionPool();

  void set_server_provider(ObMySQLServerProvider *provider);
  void update_config(const ObConnPoolConfigParam &config) { config_ = config; }
  const ObConnPoolConfigParam &get_config() const { return config_; }
  int set_db_param(const char *db_user = DEFAULT_DB_USER,
                    const char *db_pass = DEFAULT_DB_PASS, const char *db_name = DEFAULT_DB_NAME);
  int set_db_param(const ObString &db_user, const ObString &db_pass,
                    const ObString &db_name);
  int start(ObTimer &timer);
  void stop();
  void signal_refresh();
  void close_all_connection();
  bool is_updated() const { return is_updated_; }
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "connection pool task");
    return pos;
  }
  int64_t get_server_count() const;

  virtual int escape(const char *from, const int64_t from_size,
      char *to, const int64_t to_size, int64_t &out_size);

  virtual int acquire(ObISQLConnection *&conn);
  virtual int release(ObISQLConnection *conn, const bool success);

  void set_debug(const bool debug) { debug_ = debug; }
  bool get_debug() const { return debug_; }
protected:
  // update interval.
  // update ms list in backgroud thread and
  // recycle not-in-use unavaliable ms connections
  //virtual void run(obsys::CThread *thread, void *args);
  virtual void runTimerTask();
  int create_server_connection_pool(const common::ObAddr &server);

private:
  virtual int acquire(ObMySQLConnection *&connection);
  int do_acquire(ObMySQLConnection *&connection);
  int execute_init_sql(ObMySQLConnection *connection);
  int try_connect(ObMySQLConnection *connection);
  int release(ObMySQLConnection *connection, const bool succ);
  int get_pool(ObServerConnectionPool  *&pool);
  int purge_connection_pool();
  void mark_all_server_connection_gone();
  int renew_server_connection_pool(common::ObAddr &server);

private:
  static const int64_t OB_MAX_PASS_WORD_LENGTH = 64;
  static const int MAX_SERVER_GONE_INTERVAL = 1000 * 1000 * 1; // 1 sec

  bool is_updated_;
  bool is_stop_;
  bool debug_;

  ObTimer *timer_;
  ObMySQLServerProvider *server_provider_;
  volatile int64_t busy_conn_count_;

  char db_user_[OB_MAX_USER_NAME_LENGTH + 1];
  char db_pass_[OB_MAX_PASS_WORD_LENGTH + 1];
  char db_name_[OB_MAX_DATABASE_NAME_LENGTH + 1];
  char init_sql_[OB_MAX_SQL_LENGTH];
  ObConnPoolConfigParam config_;
  mutable obsys::CRWLock get_lock_;
  common::ObList<ObServerConnectionPool *> server_list_;
  common::ObCachedAllocator<ObServerConnectionPool> server_pool_;
};
}
}
}

#endif // __COMMON_OB_MYSQL_CONNECTION__
