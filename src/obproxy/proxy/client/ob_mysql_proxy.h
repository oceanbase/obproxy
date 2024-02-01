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

#ifndef OBPROXY_MYSQL_PROXY_H
#define OBPROXY_MYSQL_PROXY_H

#include "lib/ob_define.h"
#include "lib/container/ob_iarray.h"
#include "proxy/client/ob_raw_mysql_client.h"
#include "proxy/client/ob_mysql_client_pool.h"

#define MYSQL_PROXY_EXECUTE_REQUEST_EVENT (MYSQL_RPOXY_EVENT_EVENTS_START + 1)
#define MYSQL_PROXY_POST_REQUEST_EVENT (MYSQL_RPOXY_EVENT_EVENTS_START + 2)
#define MYSQL_PROXY_ACTIVE_TIMEOUT_EVENT (MYSQL_RPOXY_EVENT_EVENTS_START + 3)

namespace oceanbase
{
namespace common
{
class ObAddr;
}
namespace obproxy
{
namespace obutils
{
class ObClusterResource;
}
namespace event
{
class ObContinuation;
class ObAction;
}
namespace proxy
{
class ObClientMysqlResp;
class ObMysqlResultHandler;
class ObRawMysqlClient;
class ClientPoolOption;
class ObMysqlProxy
{
public:
  ObMysqlProxy()
    : is_inited_(false), stop_(false), is_raw_execute_(false), timeout_ms_(0),
      client_pool_(NULL), raw_mysql_client_(), rwlock_() {}
  ~ObMysqlProxy() { destroy(); }

  int init(const int64_t timeout_ms,
           const common::ObString &user_name,
           const common::ObString &password,
           const common::ObString &database,
           const common::ObString &cluster_name = "",
           const common::ObString &password1 = "");
  bool is_inited() const { return is_inited_; }
  void destroy();
  int set_timeout_ms(const int64_t timeout_ms);

  // if client pool is null, create it;
  // otherwise, we will clear current client pool and create a new one

  int rebuild_client_pool(dbconfig::ObShardConnector *shard_conn,
                          dbconfig::ObShardProp *shard_prop,
                          const bool is_meta_mysql_client,
                          const common::ObString &user_name,
                          const common::ObString &password,
                          const common::ObString &database,
                          const common::ObString &password1 = "",
                          ClientPoolOption* client_pool_option = NULL);

  int rebuild_client_pool(obutils::ObClusterResource *cr,
                          const bool is_meta_mysql_client,
                          const common::ObString &cluster_name,
                          const int64_t cluster_id,
                          const common::ObString &user_name,
                          const common::ObString &password,
                          const common::ObString &database,
                          const common::ObString &password1 = "",
                          ClientPoolOption* client_pool_option = NULL);

  void destroy_client_pool();

  // Attention!! sync_read or sync_write must not be called in working thread
  int read(const char *sql, ObMysqlResultHandler &result_handler);
  int write(const char *sql, int64_t &affected_rows);

  // Attention!! async_read or async_write must be called by ObEThread
  int async_read(event::ObContinuation *cont, const char *sql, event::ObAction *&action);
  int async_read(event::ObContinuation *cont, const ObMysqlRequestParam &request_param, event::ObAction *&action);
  int async_read(event::ObContinuation *cont, const ObMysqlRequestParam &request_param,
                 event::ObAction *&action, const int64_t timeout_ms);
  int async_write(event::ObContinuation *cont, const char *sql, event::ObAction *&action);

  // raw execute related
  int set_raw_execute(const common::ObIArray<proxy::ObProxyReplicaLocation> &addrs);
  int clear_raw_execute();

  DECLARE_TO_STRING;

private:
  void stop() { stop_ = true; }

  int async_execute(event::ObContinuation *cont,
                    const ObMysqlRequestParam &request_param,
                    const int64_t timeout_ms, event::ObAction *&action);
  int sync_execute(const char *sql, const int64_t timeout_ms, ObClientMysqlResp *&res);

  // client pool related
  int alloc_client_pool(const bool is_meta_mysql_client,
                        const common::ObString &user_name,
                        const common::ObString &password,
                        const common::ObString &database,
                        const common::ObString &cluster_name = "",
                        const common::ObString &password1 = "",
                        ClientPoolOption* client_pool_option = NULL);
public:
  ObMysqlClientPool *acquire_client_pool();
  void release_client_pool(ObMysqlClientPool *pool);
  // this function must be called under write lock
  void clear_client_pool();

private:
  bool is_inited_;
  bool stop_;
  bool is_raw_execute_;//if true, means no depend sm
  int64_t timeout_ms_;
  ObMysqlClientPool *client_pool_;
  ObRawMysqlClient raw_mysql_client_;
  mutable obsys::CRWLock rwlock_; // used for client pool
  DISALLOW_COPY_AND_ASSIGN(ObMysqlProxy);
};

inline int ObMysqlProxy::set_timeout_ms(const int64_t timeout_ms)
{
  int ret = common::OB_SUCCESS;
  if (timeout_ms <= 0) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_LOG(WDIAG, "invalid argument", K(timeout_ms), K(ret));
  } else {
    timeout_ms_ = timeout_ms;
  }
  return ret;
}

inline int ObMysqlProxy::clear_raw_execute()
{
  int ret = common::OB_SUCCESS;
  if (!is_inited_) {
    ret = common::OB_NOT_INIT;
    PROXY_LOG(WDIAG, "not init", K_(is_inited), K(ret));
  } else if (OB_FAIL(raw_mysql_client_.disconnect())) {
    PROXY_LOG(WDIAG, "fail to disconnect raw mysql client", K(ret));
  } else {
    is_raw_execute_ = false;
  }
  return ret;
}

inline void ObMysqlProxy::destroy_client_pool()
{
  if (OB_LIKELY(is_inited_)) {
    obsys::CWLockGuard guard(rwlock_);
    clear_client_pool();
  }
}

inline void ObMysqlProxy::clear_client_pool()
{
  if (OB_LIKELY(NULL != client_pool_)) {
    PROXY_LOG(INFO, "client pool will be destroyed", KPC_(client_pool));
    client_pool_->destroy();
    client_pool_->dec_ref();
    client_pool_ = NULL;
  }
}

inline ObMysqlClientPool *ObMysqlProxy::acquire_client_pool()
{
  ObMysqlClientPool *client_pool = NULL;
  obsys::CRLockGuard guard(rwlock_);
  if (OB_LIKELY(NULL != client_pool_)) {
    client_pool = client_pool_;
    client_pool->inc_ref();
  }
  return client_pool;
}

inline void ObMysqlProxy::release_client_pool(ObMysqlClientPool *pool)
{
  if (OB_LIKELY(NULL != pool)) {
    pool->dec_ref();
    pool = NULL;
  }
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_MYSQL_PROXY_H
