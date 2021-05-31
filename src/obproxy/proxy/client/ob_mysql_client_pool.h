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

#ifndef OBPROXY_MYSQL_CLIENT_POOL_H
#define OBPROXY_MYSQL_CLIENT_POOL_H

#include "lib/objectpool/ob_concurrency_objpool.h"
#include "dbconfig/ob_proxy_db_config_info.h"
#include "lib/ptr/ob_ptr.h"
#include "lib/list/ob_atomic_list.h"

namespace oceanbase
{
namespace common
{
class ObString;
}
namespace obproxy
{
namespace event
{
class ObContinuation;
class ObAction;
}
namespace obutils
{
class ObClusterResource;
}
namespace proxy
{
class ObMysqlClient;
class ClientPoolOption;
class ObMysqlClientPool : public common::ObSharedRefCount
{
public:
  ObMysqlClientPool()
    : is_inited_(false), stop_(false), mc_count_(0), cluster_resource_(NULL),
      shard_conn_(NULL), free_mc_list_(), is_cluster_param_(true) {}
  virtual ~ObMysqlClientPool() {}
  virtual void free();

  int init(const bool is_meta_mysql_client,
           const common::ObString &user_name,
           const common::ObString &password,
           const common::ObString &database,
           ClientPoolOption* client_pool_option = NULL);
  void destroy();
  // Attention!! must release, after required
  ObMysqlClient *acquire_mysql_client();
  void release_mysql_client(ObMysqlClient *mysql_client);

  void set_cluster_resource(obutils::ObClusterResource *cr);
  void set_shard_conn(dbconfig::ObShardConnector *shard_conn);
  common::ObSharedRefCount *acquire_connection_param();
  int64_t to_string(char *buf, const int64_t buf_len) const;

  static int64_t get_mysql_client_pool_count(const bool is_meta_mysql_client);

  int64_t count() const { return mc_count_; }
  common::ObAtomicList &get_free_mysql_client_list() { return free_mc_list_; }
  bool is_cluster_param() { return is_cluster_param_; }

private:
  bool is_inited_;
  bool stop_;
  int64_t mc_count_;  // mysql client count
  obutils::ObClusterResource *cluster_resource_;
  dbconfig::ObShardConnector *shard_conn_;
  common::ObAtomicList free_mc_list_;
  bool is_cluster_param_;

  DISALLOW_COPY_AND_ASSIGN(ObMysqlClientPool);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_MYSQL_CLIENT_POOL_H
