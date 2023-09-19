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
#include "obutils/ob_proxy_refresh_server_addr_cont.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/encrypt/ob_encrypted_helper.h"
#include "obutils/ob_resource_pool_processor.h"
#include "obutils/ob_proxy_table_processor.h"
#include "obutils/ob_config_server_processor.h"
#include "stat/ob_stat_processor.h"
#include "proxy/client/ob_mysql_proxy.h"
#include "proxy/client/ob_client_vc.h"
#include "proxy/route/ob_route_utils.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "proxy/mysql/ob_mysql_transact.h"
#include "proxy/mysql/ob_mysql_global_session_utils.h"
#include "obutils/ob_proxy_create_server_conn_cont.h"
#include "dbconfig/ob_proxy_db_config_info.h"
#include "proxy/mysql/ob_mysql_global_session_manager.h"
#include "obutils/ob_proxy_conn_num_check_cont.h"
#include "obutils/ob_session_pool_processor.h"


using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::dbconfig;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
ObProxyRefreshServerAddrCont::ObProxyRefreshServerAddrCont(event::ObProxyMutex *m, int64_t interval_us)
  : obutils::ObAsyncCommonTask(m, "proxy_refresh_sever_task"), reentrancy_count_(0), retry_count_(INT32_MAX),
    succ_count_(0), total_count_(0), refresh_interval_us_(interval_us),
    schema_key_(NULL), cr_(NULL)
{
  SET_HANDLER(&ObProxyRefreshServerAddrCont::main_handler);
}
ObProxyRefreshServerAddrCont::~ObProxyRefreshServerAddrCont()
{
  if (NULL != schema_key_) {
    op_free(schema_key_);
    schema_key_ = NULL;
  }
  if (NULL != cr_) {
    cr_->dec_ref();
    cr_ = NULL;
  }
}
int ObProxyRefreshServerAddrCont::handle_get_one_schema_key()
{
  int ret = OB_SUCCESS;
  if (NULL != schema_key_) {
    op_free(schema_key_);
    schema_key_ = NULL;
  }
  schema_key_ = get_global_schema_key_job_list().pop();
  if (NULL == schema_key_) {
    ret = OB_ITER_END;
  } else {
    ++total_count_;
  }
  return ret;
}
int ObProxyRefreshServerAddrCont::schedule_refresh_server_cont(bool imm)
{
  int ret = OB_SUCCESS;
  if (get_global_hot_upgrade_info().is_graceful_exit_timeout(get_hrtime())) {
    ret = OB_SERVER_IS_STOPPING;
    terminate_ = true;
    LOG_WARN("proxy need exit now", K(ret));
  } else if (OB_UNLIKELY(NULL != pending_action_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pending action should be null here", K_(pending_action), K(ret));
  } else {
    int64_t delay_us = 0;
    if (imm) {
      // must be done in work thread
      if (OB_ISNULL(g_event_processor.schedule_imm(this, ET_CALL, REFRESH_SERVER_START_CONT_EVENT))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to schedule refresh_server event", K(ret));
      }
    } else {
      delay_us = ObRandomNumUtils::get_random_half_to_full(refresh_interval_us_);
      if (OB_UNLIKELY(delay_us <= 0)) {
        ret = OB_INNER_STAT_ERROR;
        LOG_WARN("delay must greater than zero", K(delay_us), K(ret));
      } else if (OB_ISNULL(pending_action_ = self_ethread().schedule_in(this,
                                             HRTIME_USECONDS(delay_us), REFRESH_SERVER_START_CONT_EVENT))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to schedule refresh_server cont", K(delay_us), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      LOG_DEBUG("schedule refresh_server cont succ", K(delay_us), K(imm));
    }
  }
  return ret;
}

int ObProxyRefreshServerAddrCont::main_handler(int event, void *data)
{
  int ret_event = EVENT_CONT;
  int ret = OB_SUCCESS;
  ++reentrancy_count_;
  LOG_DEBUG("main_handler receive event",
            "event", get_event_name(event), K(data), K_(reentrancy_count));
  switch (event) {
  case REFRESH_SERVER_START_CONT_EVENT:
    pending_action_ = NULL;
    if (OB_FAIL(handle_get_one_schema_key())) {
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
        terminate_ = true;
      } else {
        LOG_WARN("fail to get one schema_key");
      }
    } else if OB_FAIL(handle_create_cluster_resource()) {
      LOG_WARN("fail to handle_create_cluster_resource", K(ret));
    }
    break;
  case REFRESH_SERVER_CREATE_CLUSTER_RESOURCE_EVENT: {
    pending_action_ = NULL;
    if (OB_FAIL(handle_create_cluster_resource())) {
      LOG_WARN("fail to handle_create_cluster_resource", K(ret));
    }
    break;
  }
  case CLUSTER_RESOURCE_CREATE_COMPLETE_EVENT: {
    pending_action_ = NULL;
    if (OB_FAIL(handle_create_cluster_resource_complete(data))) {
      LOG_WARN("fail to handle creat complete", K(ret));
    }
    break;
  }
  case TABLE_ENTRY_EVENT_LOOKUP_DONE: {
    if (OB_FAIL(process_table_entry_lookup(data))) {
      LOG_WARN("fail to process_table_entry_lookup", K(ret));
    } else {
      ++succ_count_;
      if (OB_ISNULL(pending_action_ = self_ethread().schedule_imm(this, REFRESH_SERVER_START_CONT_EVENT, NULL))) {
        LOG_WARN("fail to schedule", K(ret));
      }
    }
    break;
  }
  default: {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unknown event", K(event), K(data), K(ret));
    break;
  }
  };
  if (!terminate_ && OB_FAIL(ret)) {
    // may be fail in handle, continue handle next one
    LOG_INFO("failed in handle one", K(ret));
    if (OB_FAIL(cancel_pending_action())) {
      LOG_WARN("fail to cancel_pending_action", K(ret));
    } else if (OB_ISNULL(pending_action_ =  self_ethread().schedule_imm(this, REFRESH_SERVER_START_CONT_EVENT, NULL))) {
      LOG_WARN("fail to schedule", K(ret));
    }
  }
  if (terminate_ && (1 == reentrancy_count_)) {
    ret_event = EVENT_DONE;
    LOG_INFO("will destroy now, total handle", K(succ_count_), K(total_count_));
    destroy();
  } else {
    --reentrancy_count_;
    if (OB_UNLIKELY(reentrancy_count_ < 0)) {
      LOG_ERROR("invalid reentrancy_count", K_(reentrancy_count));
    }
  }
  return ret_event;
}
const char *ObProxyRefreshServerAddrCont::get_event_name(int event)
{
  const char *name = "UNKNOWN_STATE";
  switch (event) {
  case REFRESH_SERVER_START_CONT_EVENT:
    name = "REFRESH_SERVER_START_CONT_EVENT";
    break;
  case REFRESH_SERVER_CREATE_CLUSTER_RESOURCE_EVENT:
    name = "REFRESH_SERVER_CREATE_CLUSTER_RESOURCE_EVENT";
    break;
  case CLUSTER_RESOURCE_CREATE_COMPLETE_EVENT:
    name = "CLUSTER_RESOURCE_CREATE_COMPLETE_EVENT";
    break;
  case TABLE_ENTRY_EVENT_LOOKUP_DONE:
    name = "TABLE_ENTRY_EVENT_LOOKUP_DONE";
    break;
  case REFRESH_SERVER_DONE_ENTRY_EVENT:
    name = "REFRESH_SERVER_DONE_ENTRY_EVENT";
    break;
  default:
    LOG_WARN("UNKNOWN_STATE", K(event));
    break;
  }
  return name;
}

int ObProxyRefreshServerAddrCont::process_table_entry_lookup(void* data)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg, data is null");
  } else {
    ObMysqlRouteResult *result = reinterpret_cast<ObMysqlRouteResult *>(data);
    ObTableEntry *table_entry = result->table_entry_;
    if ((NULL != table_entry) && table_entry->is_dummy_entry()) {
      proxy::ObTableEntry* dummy_entry = table_entry;
      ret = find_servers_from_dummy_entry(dummy_entry);
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("not dummy_entry", KPC(table_entry));
    }
    result->ref_reset();
  }
  return ret;
}

int ObProxyRefreshServerAddrCont::find_servers_from_dummy_entry(proxy::ObTableEntry *dummy_entry)
{
  int ret = OB_SUCCESS;
  ObClusterResource* cluster_resource_ = cr_;
  if (OB_ISNULL(cluster_resource_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cluster_resource is not avail", K(ret));
  } else if (OB_ISNULL(dummy_entry) || OB_UNLIKELY(!dummy_entry->is_tenant_servers_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dummy_entry is not avail", KPC(dummy_entry), K(ret));
  } else {
    ObSEArray<ObServerStateSimpleInfo, ObServerStateRefreshCont::DEFAULT_SERVER_COUNT> simple_servers_info(
          ObServerStateRefreshCont::DEFAULT_SERVER_COUNT);
    const uint64_t new_ss_version = cluster_resource_->server_state_version_;
    common::ObIArray<ObServerStateSimpleInfo> &server_state_info = cluster_resource_->get_server_state_info(new_ss_version);
    common::DRWLock &server_state_lock = cluster_resource_->get_server_state_lock(new_ss_version);
    if (OB_FAIL(server_state_lock.try_rdlock())) {
      LOG_ERROR("fail to tryrdlock server_state_lock", K(ret),
                     K(current_idc_name_), "new_ss_version", new_ss_version);
    } else {
      if (OB_FAIL(simple_servers_info.assign(server_state_info))) {
        LOG_WARN("fail to assign servers_info_", K(ret));
      }
      server_state_lock.rdunlock();
    }
    if (OB_SUCC(ret)) {
      const ObTenantServer * tenant_server = dummy_entry->get_tenant_servers();
      int32_t add_count = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < tenant_server->count(); i++) {
        const ObProxyReplicaLocation &replica = tenant_server->server_array_[i];
        for (int64_t j = 0; OB_SUCC(ret) && j < simple_servers_info.count(); ++j) {
          const ObServerStateSimpleInfo& ss = simple_servers_info.at(j);
          if (replica.server_  != ss.addr_) {
            // not same ,do nothin
          } else if (ss.is_force_congested_) {
            // skip it
          } else if (ss.idc_name_.case_compare(current_idc_name_.config_string_) == 0 ||
            ss.zone_name_.prefix_case_match(current_idc_name_.config_string_)) {
            char ip_buf[128];
            if (!ss.addr_.ip_to_string(ip_buf, 128)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("ip_to_string failed", K(ss));
            } else if (OB_FAIL(get_global_session_manager().add_server_addr_if_not_exist(*schema_key_,
                       ObString::make_string(ip_buf),
                       ss.addr_.get_port(), true))) {
              LOG_WARN("add to map failed", K(ret), K(ss.addr_));
            } else {
              ++add_count;
            }
          }
        }
      }
      LOG_DEBUG("find_servers_from_dummy_entry", K(add_count), KPC(schema_key_), K(current_idc_name_),
        KPC(tenant_server), K(simple_servers_info));
    }
  }
  return ret;
}
int ObProxyRefreshServerAddrCont::fetch_dummy_table_entry()
{
  int ret = OB_SUCCESS;
  int64_t short_async_task_timeout = 0;
  ObMysqlConfigParams *config = NULL;
  if (OB_ISNULL(config = get_global_mysql_config_processor().acquire())) {
    LOG_WARN("failed to acquire mysql config");
  } else {
    current_idc_name_.set_value(config->proxy_idc_name_);
    short_async_task_timeout = config->short_async_task_timeout_;
    ObRouteParam param;
    param.cont_ = this;
    param.force_renew_ = false;
    param.use_lower_case_name_ = false;
    param.mysql_proxy_ = &(cr_->mysql_proxy_);
    param.cr_version_ = cr_->version_;
    param.timeout_us_ = hrtime_to_usec(short_async_task_timeout);
    param.is_partition_table_route_supported_ = config->enable_partition_table_route_;
    param.client_request_ = NULL;
    param.client_info_ = NULL;
    param.current_idc_name_ = current_idc_name_.config_string_;//shallow copy
    param.need_pl_route_ = false;
    ObString cluster_name = schema_key_->get_cluster_name();
    ObString tenant_name = schema_key_->get_tenant_name();
    param.name_.shallow_copy(cluster_name, tenant_name,
                             ObString::make_string(OB_SYS_DATABASE_NAME),
                             ObString::make_string(OB_ALL_DUMMY_TNAME));
    if (OB_FAIL(ObMysqlRoute::get_route_entry(param, cr_, pending_action_))) {
      LOG_WARN("fail to get_route_entry", K(param), K(ret));
    } else {
      LOG_DEBUG("get_route_entry succ", K(param), K(ret), K(pending_action_));
    }
    config->dec_ref();
    config = NULL;
  }
  return ret;
}

int ObProxyRefreshServerAddrCont::handle_create_cluster_resource()
{
  int ret = OB_SUCCESS;
  ObResourcePoolProcessor &rp_processor = get_global_resource_pool_processor();
  ObString cluster_name = schema_key_->get_cluster_name();

  if (OB_FAIL(rp_processor.get_cluster_resource(*this,
              (process_async_task_pfn)&ObProxyRefreshServerAddrCont::handle_create_cluster_resource_complete,
              false, cluster_name, OB_DEFAULT_CLUSTER_ID, NULL, pending_action_))) {
    LOG_WARN("fail to get cluster resource", "cluster name", cluster_name, K(ret));
  } else if (NULL == pending_action_) { // created succ
    LOG_INFO("cluster resource was created by others, no need create again");
  }
  return ret;
}

int ObProxyRefreshServerAddrCont::handle_create_cluster_resource_complete(void *data)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data)) {
    if (retry_count_ > 0) {
      --retry_count_;
      LOG_INFO("fail to create cluste resource, will retry", "remain retry count", retry_count_,
               K(schema_key_->get_cluster_name()));
      if (OB_ISNULL(pending_action_ = g_event_processor.schedule_in(
                                        this, HRTIME_MSECONDS(RETRY_INTERVAL_MS), ET_CALL,
                                        REFRESH_SERVER_CREATE_CLUSTER_RESOURCE_EVENT))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to schedule fetch rslist task", K(ret));
      }
    } else {
      LOG_WARN("fail to create cluste resource, no chance retry", K(data),
               K(schema_key_->get_cluster_name()));
    }
  } else {
    ObClusterResource *cr = reinterpret_cast<ObClusterResource *>(data);
    LOG_INFO("cluster create succ", K(cr), KPC(cr), K(retry_count_));
    if (!OB_ISNULL(cr_)) {
      cr_->dec_ref();
      cr_ = NULL;
    }
    cr_ = cr;
    retry_count_ = INT32_MAX;
    if (OB_FAIL(fetch_dummy_table_entry())) {
      LOG_WARN("fail to fetch dummy table entry", K(ret));
    } else {
      LOG_DEBUG("fetch_dummy_table_entry succ");
    }
  }
  return ret;
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
