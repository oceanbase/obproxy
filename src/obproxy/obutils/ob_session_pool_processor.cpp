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
#include "obutils/ob_session_pool_processor.h"
#include "utils/ob_proxy_utils.h"
#include "obutils/ob_proxy_refresh_server_addr_cont.h"
#include "obutils/ob_proxy_create_server_conn_cont.h"
#include "proxy/mysql/ob_mysql_global_session_utils.h"
#include "obutils/ob_proxy_create_server_conn_cont.h"
#include "dbconfig/ob_proxy_db_config_info.h"
#include "proxy/mysql/ob_mysql_global_session_manager.h"
#include "obutils/ob_proxy_conn_num_check_cont.h"
#include "iocore/eventsystem/ob_event_system.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy;
using namespace oceanbase::obproxy::dbconfig;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::proxy;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
ObSessionPoolProcessor g_session_pool_processor;
ObSessionPoolProcessor::ObSessionPoolProcessor(): pool_stat_dump_cont_(NULL)
{

}
ObSessionPoolProcessor::~ObSessionPoolProcessor()
{
  if (NULL != pool_stat_dump_cont_) {
    pool_stat_dump_cont_->destroy();
    pool_stat_dump_cont_ = NULL;
  }
}
int ObSessionPoolProcessor::create_refresh_server_session_cont()
{
  int ret = OB_SUCCESS;
  ObProxyRefreshServerAddrCont *cont = NULL;
  ObProxyMutex *mutex = NULL;
  int64_t interval_us = HRTIME_USECONDS(get_global_proxy_config().session_pool_cont_delay_interval);
  if (OB_ISNULL(mutex = new_proxy_mutex())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory for proxy mutex error", K(ret));
  } else if (OB_ISNULL(cont = new (std::nothrow) ObProxyRefreshServerAddrCont(mutex, interval_us))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc ObProxyRefreshServerAddrCont", K(ret));
  } else {
    cont->schedule_refresh_server_cont(true);
    LOG_INFO("create_refresh_server_session_cont", K(interval_us));
  }
  if (OB_FAIL(ret)) {
    if (NULL != cont) {
      cont->destroy();
      cont = NULL;
      mutex = NULL;
    } else {
      if (NULL != mutex) {
        mutex->free();
        mutex = NULL;
      }
    }
  }
  return ret;
}

int ObSessionPoolProcessor::create_server_conn_cont()
{
  int ret = OB_SUCCESS;
  ObProxyCreateServerConnCont *cont = NULL;
  ObProxyMutex *mutex = NULL;
  int64_t interval_us = HRTIME_USECONDS(get_global_proxy_config().session_pool_cont_delay_interval);
  if (OB_ISNULL(mutex = new_proxy_mutex())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory for proxy mutex error", K(ret));
  } else if (OB_ISNULL(cont = new (std::nothrow) ObProxyCreateServerConnCont(mutex, interval_us))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc ObProxyCreateServerConnCont", K(ret));
  } else {
    ret = cont->schedule_create_conn_cont(true);
    LOG_INFO("create_server_conn_cont", K(interval_us));
  }
  if (OB_FAIL(ret)) {
    if (NULL != cont) {
      cont->destroy();
      cont = NULL;
      mutex = NULL;
    } else {
      if (NULL != mutex) {
        mutex->free();
        mutex = NULL;
      }
    }
  }
  return ret;
}

int ObSessionPoolProcessor::create_conn_num_check_cont()
{
  int ret = OB_SUCCESS;
  ObProxyConnNumCheckCont *cont = NULL;
  ObProxyMutex *mutex = NULL;
  int64_t interval_us = HRTIME_USECONDS(get_global_proxy_config().session_pool_cont_delay_interval);
  if (OB_ISNULL(mutex = new_proxy_mutex())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory for proxy mutex error", K(ret));
  } else if (OB_ISNULL(cont = new (std::nothrow) ObProxyConnNumCheckCont(mutex, interval_us))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc ObProxyConnNumCheckCont", K(ret));
  } else {
    ret = cont->schedule_check_conn_num_cont(true);
    LOG_INFO("create_conn_num_check_cont", K(interval_us));
  }
  if (OB_FAIL(ret)) {
    if (NULL != cont) {
      cont->destroy();
      cont = NULL;
      mutex = NULL;
    } else {
      if (NULL != mutex) {
        mutex->free();
        mutex = NULL;
      }
    }
  }
  return ret;
}

int ObSessionPoolProcessor::handle_one_logic_tenent(ObDbConfigLogicTenant* logic_tenant,
  int32_t& logic_tenant_count,
  int32_t& logic_db_count,
  int32_t& shard_conn_count)
{
  int ret = OB_SUCCESS;
  ObProxySchemaKey* schema_key = NULL;
  ObDbConfigLogicDb* logic_db = NULL;
  ++logic_tenant_count;
  ObString& logic_tenant_name = logic_tenant->tenant_name_.config_string_;
  ObDbConfigLogicTenant::LDHashMap::iterator ld_spot = logic_tenant->ld_map_.begin();
  ObDbConfigLogicTenant::LDHashMap::iterator ld_last = logic_tenant->ld_map_.end();
  LOG_DEBUG("handle one logic_tenant", K(logic_tenant_name));
  for (; OB_SUCC(ret) && ld_spot != ld_last; ++ld_spot) {
    logic_db = &(*ld_spot);
    schema_key = NULL;
    if (OB_ISNULL(logic_db)) {
      LOG_WARN("logic_db should not null here", K(logic_tenant_name));
    } else {
      ++logic_db_count;
      ObString logic_db_name = logic_db->db_name_.config_string_;
      ObDbConfigChildArrayInfo<ObShardConnector>& shard_connector_array_info = logic_db->sc_array_;
      ObDbConfigChildArrayInfo<ObShardConnector>::CCRHashMap::iterator sc_spot = shard_connector_array_info.ccr_map_.begin();
      ObDbConfigChildArrayInfo<ObShardConnector>::CCRHashMap::iterator sc_last = shard_connector_array_info.ccr_map_.end();
      for (; OB_SUCC(ret) && sc_spot != sc_last; ++sc_spot) {
        ObShardConnector* shard_conn = &(*sc_spot);
        if (OB_ISNULL(shard_conn)) {
          LOG_WARN("shard_conn should not null here", K(logic_tenant_name), K(logic_db_name));
        } else {
          ++shard_conn_count;
          ObString shard_name = shard_conn->shard_name_.config_string_;
          ObString cluster_name = shard_conn->cluster_name_;
          ObShardProp* shard_prop = NULL;
          if (OB_FAIL(get_global_dbconfig_cache().get_shard_prop(logic_tenant_name,
              logic_db_name, shard_name, shard_prop))) {
            LOG_WARN("get_connector_prop faild", K(ret), K(logic_tenant_name), K(logic_db_name), K(shard_name));
          } else if (!shard_prop->get_need_prefill()) {
            LOG_DEBUG("no need prefill", K(logic_tenant_name), K(logic_db_name), K(shard_name));
          } else if (OB_ISNULL(schema_key = op_alloc(ObProxySchemaKey))) {
            LOG_WARN("alloc schema_key failed", K(shard_name), KPC(shard_conn));
          } else if (OB_FAIL(ObMysqlSessionUtils::init_schema_key_value(*schema_key, logic_tenant_name, logic_db_name, shard_conn))) {
            LOG_WARN("init_schema_key_value failed", K(ret), KPC(shard_conn));
          } else if (DB_MYSQL == shard_conn->server_type_) {
            // mysql add addr direct
            int64_t port = 0;
            if (OB_FAIL(get_int_value(shard_conn->physic_port_.config_string_, port))) {
              LOG_WARN("invalid int value", K(shard_conn->physic_port_.config_string_), K(ret));
            } else if (OB_FAIL(get_global_session_manager().add_server_addr_if_not_exist(*schema_key,
                         shard_conn->physic_addr_.config_string_, static_cast<int32_t>(port),
                         shard_conn->is_physic_ip_))) {
              LOG_WARN("add server addr failed", K(ret), KPC(shard_conn));
            } else {
              LOG_DEBUG("add server addr succ", K(ret), K(shard_conn->physic_addr_.config_string_), K(port));
            }
            op_free(schema_key);
            schema_key = NULL;
          } else {
            // ob need refresh addr
            LOG_DEBUG("will create server session", KPC(schema_key));
            get_global_schema_key_job_list().push(schema_key);
          }
          if (NULL != shard_prop) {
            shard_prop->dec_ref();
            shard_prop = NULL;
          }
        }
      }
    }
  }
  return ret;
}
int ObSessionPoolProcessor::do_pool_stat_dump()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObMysqlServerSessionListPool*, 1024> all_session_list_pool_array;
  get_global_session_manager().get_all_session_list_pool(all_session_list_pool_array);
  for (int64_t i = 0; i < all_session_list_pool_array.count(); i++) {
    ObMysqlServerSessionListPool* session_list_pool = all_session_list_pool_array.at(i);
    ObProxySchemaKey& schema_key = session_list_pool->schema_key_;
    DRWLock::RDLockGuard  guard(session_list_pool->rwlock_);
    ObMysqlServerSessionListPool::IPHashTable& session_pool = session_list_pool->server_session_list_pool_;
    ObMysqlServerSessionListPool::IPHashTable::iterator spot = session_pool.begin();
    ObMysqlServerSessionListPool::IPHashTable::iterator last = session_pool.end();
    for (int64_t j = 0; OB_SUCC(ret) && spot != last; ++spot, ++j) {
      spot->do_pool_log(schema_key, true);
    }
    session_list_pool->dec_ref();
    session_list_pool = NULL;
  }
  return ret;
}
void ObSessionPoolProcessor::update_pool_stat_dump_interval()
{
  ObAsyncCommonTask *cont = get_global_session_pool_processor().get_pool_stat_dump_cont();
  if (OB_LIKELY(cont)) {
    int64_t interval_us = ObRandomNumUtils::get_random_half_to_full(
                          get_global_proxy_config().session_pool_stat_log_interval);
    cont->set_interval(interval_us);
  }
}
int ObSessionPoolProcessor::start_pool_stat_dump_task()
{
  int ret = OB_SUCCESS;
  int64_t interval_us = get_global_proxy_config().session_pool_stat_log_interval;
  if (interval_us > 0) {
    if (OB_ISNULL(pool_stat_dump_cont_ = ObAsyncCommonTask::create_and_start_repeat_task(
            interval_us,
            "pool_stat_dump_task",
            ObSessionPoolProcessor::do_pool_stat_dump,
            ObSessionPoolProcessor::update_pool_stat_dump_interval, false, event::ET_TASK))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to create and start tenant_stat_dump_task task", K(ret));
    } else {
      LOG_DEBUG("start pool_stat_dump_task", K(interval_us));
    }
  }
  return ret;
}

int ObSessionPoolProcessor::start_session_pool_task()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("Enter start_session_pool_task");
  int64_t max_conn_cont_num = get_global_proxy_config().create_conn_cont_num;
  if (!get_global_proxy_config().enable_conn_precreate) {
    LOG_INFO("conn precreate is disabled");
  } else {
     // wait for dbconfig init finished
    common::ObSEArray<common::ObString, 32> all_tenant;
    ObDbConfigLogicTenant* logic_tenant = NULL;
    int32_t logic_tenant_count = 0;
    int32_t logic_db_count = 0;
    int32_t shard_conn_count = 0;
    if (OB_FAIL(get_global_dbconfig_cache().get_all_logic_tenant(all_tenant))) {
      LOG_WARN("failed to get all logic tenant", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < all_tenant.count(); i++) {
      ObString& tenant_name = all_tenant.at(i);
      if (OB_ISNULL(logic_tenant = get_global_dbconfig_cache().get_exist_tenant(tenant_name))) {
        LOG_WARN("tenant not exist", K(tenant_name));
      } else {
        if (OB_FAIL(handle_one_logic_tenent(logic_tenant, logic_tenant_count, logic_db_count, shard_conn_count))) {
          LOG_WARN("fail to handle_one_logic_tenent", K(ret));
        }
        logic_tenant->dec_ref();
        logic_tenant = NULL;
      }
    }
    int32_t count = get_global_schema_key_job_list().count();
    // after add all to pending list create cont to consume job
    int64_t max_refresh_cont_num = get_global_proxy_config().refresh_server_cont_num;
    LOG_INFO("now job list is ", K(count), K(max_refresh_cont_num), K(max_conn_cont_num),
      K(logic_db_count), K(logic_db_count), K(shard_conn_count));
    for (int64_t i = 0; OB_SUCC(ret) && i < max_refresh_cont_num; i++) {
      ret = create_refresh_server_session_cont();
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < max_conn_cont_num; i++) {
    ret = create_server_conn_cont();
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(create_conn_num_check_cont())) {
      LOG_WARN("create_conn_num_check_cont failed", K(ret));
    } else if (OB_FAIL(start_pool_stat_dump_task())) {
      LOG_WARN("start_pool_stat_dump_task failed", K(ret));
    }
  }
  return ret;
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase