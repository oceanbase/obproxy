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

#define USING_LOG_PREFIX PROXY

#include "proxy/client/ob_mysql_client_pool.h"
#include "stat/ob_resource_pool_stats.h"
#include "obutils/ob_resource_pool_processor.h"
#include "proxy/client/ob_client_vc.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::dbconfig;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
//----------------------ObMysqlClientDestroyCont-------------------------//
class ObMysqlClientDestroyCont : public ObAsyncCommonTask
{
public:
  ObMysqlClientDestroyCont(ObProxyMutex *m, ObMysqlClientPool *pool)
    : ObAsyncCommonTask(m, "client_pool_destroy_task"),
      pool_(pool), deleted_count_(0) {}
  virtual ~ObMysqlClientDestroyCont() {}

  static ObMysqlClientDestroyCont *alloc(ObMysqlClientPool *pool);

  virtual int main_handler(int event, void *data);
  virtual void destroy()
  {
    if (OB_LIKELY(NULL != pool_)) {
      pool_->dec_ref();
      pool_ = NULL;
      ObAsyncCommonTask::destroy();
    }
  }

private:
  int handle_destroy_mysql_client();

private:
  static const int64_t RETRY_INTERVAL_MS = 100;

  ObMysqlClientPool *pool_;
  int64_t deleted_count_;
  DISALLOW_COPY_AND_ASSIGN(ObMysqlClientDestroyCont);
};

ObMysqlClientDestroyCont *ObMysqlClientDestroyCont::alloc(ObMysqlClientPool *pool)
{
  int ret = OB_SUCCESS;
  ObMysqlClientDestroyCont *cont = NULL;
  ObProxyMutex *mutex = NULL;
  if (OB_ISNULL(pool)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pool is null", K(ret));
  } else if (OB_ISNULL(mutex = new_proxy_mutex())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc memory for mutex", K(ret));
  } else {
    pool->inc_ref();
    if (OB_ISNULL(cont = new(std::nothrow) ObMysqlClientDestroyCont(mutex, pool))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to alloc memory for ObMysqlClientDestroyCont", K(ret));
      if (OB_LIKELY(NULL != mutex)) {
        mutex->free();
        mutex = NULL;
      }
      pool->dec_ref();
      pool = NULL;
    }
  }
  return cont;
}

int ObMysqlClientDestroyCont::main_handler(int event, void *data)
{
  UNUSED(data);
  int event_ret = EVENT_CONT;
  if (OB_LIKELY(CLIENT_DESTROY_SELF_EVENT == event)) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(handle_destroy_mysql_client())) {
      LOG_WARN("fail to handle destroy mysql client", K(ret));
    }
  } else {
    terminate_ = true;
    LOG_WARN("unknown event", K(event));
  }

  if (terminate_) {
    event_ret = EVENT_DONE;
    destroy();
  }
  return event_ret;
}

int ObMysqlClientDestroyCont::handle_destroy_mysql_client()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pool_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("client pool is null, ObMysqlClientDestroyCont will be destroyed", K(ret));
    terminate_ = true;
  } else {
    ObAtomicList &todo_list = pool_->get_free_mysql_client_list();
    ObMysqlClient *mysql_client = reinterpret_cast<ObMysqlClient *>(todo_list.popall());
    ObMysqlClient *next = NULL;
    while (NULL != mysql_client) {
      next = reinterpret_cast<ObMysqlClient *>(mysql_client->link_.next_);
      if (OB_ISNULL(self_ethread().schedule_imm(mysql_client, CLIENT_DESTROY_SELF_EVENT))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to schedule destroy mysql client event, memory will leak", K(ret));
      }
      // ignore ret, continue
      mysql_client = next;
      ++deleted_count_;
    }

    if (pool_->count() == deleted_count_) {
      terminate_ = true;
      LOG_INFO("all mysql client has been scheduled to destroy self", K_(deleted_count));
    } else if (deleted_count_ > pool_->count()) {
      terminate_ = true;
      LOG_ERROR("delete count must be less than total count",
                K_(deleted_count), "total_count", pool_->count());
    } else if (OB_ISNULL(self_ethread().schedule_in(this, HRTIME_MSECONDS(RETRY_INTERVAL_MS), CLIENT_DESTROY_SELF_EVENT))) {
      LOG_WARN("fail to reschedule destroy mysql client list event", K(this),
               LITERAL_K(RETRY_INTERVAL_MS));
      terminate_ = true;
    }
  }
  return ret;
}

//------------------------------ObMysqlClientPool------------------------------//

int64_t ObMysqlClientPool::get_mysql_client_pool_count(const bool is_meta_mysql_client)
{
  return (is_meta_mysql_client ? OB_META_MYSQL_CLIENT_COUNT : OB_NORMAL_MYSQL_CLIENT_COUNT);
}

int ObMysqlClientPool::init(
    const bool is_meta_mysql_client,
    const ObString &user_name,
    const ObString &password,
    const ObString &database,
    const ObString &password1,
    ClientPoolOption* client_pool_option)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(user_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(user_name), K(ret));
  } else if (OB_FAIL(free_mc_list_.init("mysql client list",
          reinterpret_cast<int64_t>(&(reinterpret_cast<ObMysqlClient *>(0))->link_)))) {
    LOG_WARN("fail to init mc_list", K(ret));
  } else {
    ObMysqlClient *mysql_client = NULL;
    const int64_t mc_count = get_mysql_client_pool_count(is_meta_mysql_client);
    for (int64_t i = 0; (i < mc_count && OB_SUCC(ret)); ++i) {
      if (OB_FAIL(ObMysqlClient::alloc(this, mysql_client, user_name, password, database, is_meta_mysql_client, password1, client_pool_option))) {
        LOG_WARN("fail to alloc mysql client", K(user_name), K(password),
                 K(database), "idx", i, K(is_meta_mysql_client), K(ret));
      } else if (OB_ISNULL(mysql_client)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mysql client can not be NULL here", K(mysql_client), K(ret));
      } else {
        free_mc_list_.push(mysql_client);
      }
    }
    if (OB_SUCC(ret)) {
      mc_count_ = mc_count;
      is_inited_ = true;
    }
  }

  if (OB_FAIL(ret)) {
    destroy();
  }

  return ret;
}

void ObMysqlClientPool::free()
{
  LOG_INFO("client pool will be free", KPC(this));
  if (OB_LIKELY(NULL != cluster_resource_)) {
    cluster_resource_->dec_ref();
    cluster_resource_ = NULL;
  }

  if (OB_LIKELY(NULL != shard_conn_)) {
    shard_conn_->dec_ref();
    shard_conn_ = NULL;
  }

  CLIENT_POOL_INCREMENT_DYN_STAT(FREE_CLUSTER_CLIENT_POOL_COUNT);

  op_free(this);
}

void ObMysqlClientPool::destroy()
{
  CLIENT_POOL_INCREMENT_DYN_STAT(DELETE_CLUSTER_CLIENT_POOL_COUNT);
  stop_ = true;
  if (is_inited_) {
    int ret = OB_SUCCESS;
    ObMysqlClientDestroyCont *cont = NULL;
    if (OB_ISNULL(cont = ObMysqlClientDestroyCont::alloc(this))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to alloc ObMysqlClientDestroyCont", K(ret));
    } else if (OB_ISNULL(g_event_processor.schedule_imm(cont, ET_CALL, CLIENT_DESTROY_SELF_EVENT))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to schedule ObMysqlClientDestroyCont", K(cont), K(ret));
    }
    if (OB_FAIL(ret) && OB_LIKELY(NULL != cont)) {
      cont->destroy();
      cont = NULL;
    }
    is_inited_ = false;
  }
}

int64_t ObMysqlClientPool::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this), K_(ref_count), K_(is_inited), K_(stop), K_(mc_count), KP_(cluster_resource));
  J_OBJ_END();
  return pos;
}

ObMysqlClient *ObMysqlClientPool::acquire_mysql_client()
{
  return stop_ ? NULL : reinterpret_cast<ObMysqlClient *>(free_mc_list_.pop());
}

void ObMysqlClientPool::release_mysql_client(ObMysqlClient *mysql_client)
{
  if (NULL != mysql_client) {
    free_mc_list_.push(mysql_client);
  }
}

void ObMysqlClientPool::set_cluster_resource(ObClusterResource *cr)
{
  if (NULL != cluster_resource_) {
    LOG_DEBUG("cluster_resource will dec ref", K_(cluster_resource), KPC_(cluster_resource));
    cluster_resource_->dec_ref();
    cluster_resource_ = NULL;
  }
  if (NULL != cr) {
    cr->inc_ref();
    cluster_resource_ = cr;
    cr = NULL;
    is_cluster_param_ = true;
  }
}

void ObMysqlClientPool::set_shard_conn(ObShardConnector *shard_conn)
{
  if (NULL != shard_conn_) {
    LOG_DEBUG("shard_conn will dec ref", K_(shard_conn), KPC_(shard_conn));
    shard_conn_->dec_ref();
    shard_conn_ = NULL;
  }
  if (NULL != shard_conn) {
    shard_conn->inc_ref();
    shard_conn_ = shard_conn;
    shard_conn = NULL;
    is_cluster_param_ = false;
  }
}

ObSharedRefCount *ObMysqlClientPool::acquire_connection_param()
{
  if (is_cluster_param_) {
    if (OB_NOT_NULL(cluster_resource_)) {
      cluster_resource_->inc_ref();
    }
    return cluster_resource_;
  } else {
    if (OB_NOT_NULL(shard_conn_)) {
      shard_conn_->inc_ref();
    }
    return shard_conn_;
  }
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
