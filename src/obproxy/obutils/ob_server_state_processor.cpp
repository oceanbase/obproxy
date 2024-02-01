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
#include "obutils/ob_server_state_processor.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "utils/ob_proxy_utils.h"
#include "utils/ob_proxy_hot_upgrader.h"
#include "obutils/ob_congestion_manager.h"
#include "obutils/ob_config_server_processor.h"
#include "obutils/ob_resource_pool_processor.h"
#include "proxy/mysqllib/ob_resultset_fetcher.h"
#include "proxy/client/ob_mysql_proxy.h"
#include "proxy/client/ob_client_utils.h"
#include "proxy/client/ob_client_vc.h"
#include "proxy/route/ob_table_cache.h"
#include "proxy/route/ob_route_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::common::hash;
using namespace oceanbase::share;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

static const char *SELECT_ZONE_STATE_INFO_SQL                     =
    //zone, is_merging, status, region
    "SELECT /*+READ_CONSISTENCY(WEAK)*/ * "
    "FROM oceanbase.%s LIMIT %ld";

static const char *SELECT_ZONE_STATE_INFO_SQL_V4 =
    "SELECT /*+READ_CONSISTENCY(WEAK)*/ zone, status, "
    "0 AS is_merging, region, idc AS spare4, type AS spare5 "
    "FROM oceanbase.%s LIMIT %ld";

//when server fail to start, its status is inactive, but its port == 0.
//it is design defect, but proxy need compatible with it.
//so select svr_port > 0 one
const static char *SELECT_SERVER_STATE_INFO_SQL                   =
    "SELECT /*+READ_CONSISTENCY(WEAK)*/ svr_ip, svr_port, zone, status, start_service_time, stop_time "
    "FROM oceanbase.%s "
    "WHERE svr_port > 0 ORDER BY zone LIMIT %ld";

const static char *SELECT_SERVER_STATE_INFO_SQL_V4 =
    "SELECT /*+READ_CONSISTENCY(WEAK)*/ svr_ip, sql_port AS svr_port, zone, status, "
    "start_service_time is not null AS start_service_time, stop_time is not null as stop_time "
    "FROM oceanbase.%s "
    "WHERE svr_port > 0 ORDER BY zone LIMIT %ld";

const static char *SELECT_CLUSTER_ROEL_SQL                        =
    "SELECT /*+READ_CONSISTENCY(WEAK)*/ cluster_role, cluster_status, primary_cluster_id "
    "FROM oceanbase.v$ob_cluster LIMIT 1";

const static char *SYS_LDG_INFO_SQL                               =
    "SELECT TENANT_ID, TENANT_NAME, CLUSTER_ID, CLUSTER_NAME, LDG_ROLE "
    "FROM oceanbase.ldg_standby_status";
const static char *SELECT_ALL_TENANT_SQL                          =
    "SELECT /*+READ_CONSISTENCY(WEAK)*/ tenant_name, locality, previous_locality, primary_zone "
    "FROM oceanbase.%s where %s and tenant_id != 1";

class ObDetectOneServerStateCont : public obutils::ObAsyncCommonTask
{
public:
  ObDetectOneServerStateCont();
  virtual ~ObDetectOneServerStateCont() {}
  virtual void destroy() { kill_this(); }
  virtual int main_handler(int event, void *data);
  void kill_this();
  int init(ObClusterResource *cluster_resource, ObAddr addr);

private:
  int detect_server_state_by_sql();
  int handle_client_resp(void *data);
private:
  ObClusterResource *cluster_resource_;
  ObAddr addr_;
  ObMysqlClient *mysql_client_;
  ObMysqlProxy mysql_proxy_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDetectOneServerStateCont);
};

//-------------------------------ObServerStateRefreshCont----------------------------------------//
int ObServerStateRefreshCont::init(ObClusterResource *cr,
                                   int64_t ss_refresh_interval_us,
                                   uint64_t last_rs_list_hash)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(ss_refresh_interval_us <= 0 || NULL == cr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(ss_refresh_interval_us), K(ret));
  } else if (OB_ISNULL(mutex_ = new_proxy_mutex())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to allocate mutex", K(ret));
  } else {
    cr->inc_ref();
    cluster_resource_ = cr;
    mysql_proxy_ = &cluster_resource_->mysql_proxy_;
    congestion_manager_ = &cluster_resource_->congestion_manager_;
    ss_refresh_interval_us_ = ss_refresh_interval_us;
    cluster_name_.assign_ptr(cluster_resource_->get_cluster_name().ptr(), cluster_resource_->get_cluster_name().length());
    cluster_id_ = cluster_resource_->get_cluster_id();
    last_server_list_hash_ = last_rs_list_hash;
    is_inited_ = true;
  }
  return ret;
}

void ObServerStateRefreshCont::kill_this()
{
  if (is_inited_) {
    LOG_INFO("ObServerStateRefreshCont will kill self", KPC(this));
    int ret = OB_SUCCESS;
    // cancel pending action at first
    // ignore ret
    if (OB_FAIL(cancel_pending_action())) {
      LOG_WDIAG("fail to cancel pending action", K(ret));
    }

    if (OB_LIKELY(NULL != cluster_resource_)) {
      cluster_resource_->dec_ref();
      cluster_resource_ = NULL;
    }

    mysql_proxy_ = NULL;
    congestion_manager_ = NULL;
    need_reset_in_error_ = false;
    ss_refresh_interval_us_ = 0;
    cur_zones_state_.reset();
    last_servers_state_.reset();
    last_servers_state_hash_ = 0;
    last_server_list_hash_ = 0;
    last_zones_state_.reset();
    kill_this_ = false;
    is_inited_ = false;
  }
  mutex_.release();
  op_free(this);
}

DEF_TO_STRING(ObServerStateRefreshCont)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(is_inited), K_(kill_this), K_(need_reset_in_error),
       K_(cur_job_event), K_(ss_refresh_interval_us), K_(ss_refresh_failure), K_(set_interval_task_count),
       K_(cluster_name), K_(cluster_id), K_(last_zones_state_hash), K_(last_servers_state_hash),
       K_(last_servers_state), K_(last_zones_state), K_(last_server_list_hash),
       K_(last_zones_state), KP_(pending_action), KPC_(cluster_resource));
  J_OBJ_END();
  return pos;
}

int ObServerStateRefreshCont::schedule_refresh_server_state(const bool imm /*false*/)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else if (get_global_hot_upgrade_info().is_graceful_exit_timeout(get_hrtime())) {
    ret = OB_SERVER_IS_STOPPING;
    LOG_WDIAG("proxy need exit now", K(ret));
  } else if (OB_UNLIKELY(NULL != pending_action_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("pending action should be null here", K_(pending_action), K(ret));
  } else if (OB_UNLIKELY(!self_ethread().is_event_thread_type(ET_CALL))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("server state refresh cont must be scheduled in work thread", K(ret));
  } else {
    need_reset_in_error_ = false;
    int64_t delay_us = 0;
    bool need_refresh_cluster_role = get_global_proxy_config().with_config_server_
                                     && get_global_proxy_config().enable_standby
                                     && OB_DEFAULT_CLUSTER_ID == cluster_id_
                                     && IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_resource_->cluster_version_);
    if (imm) {
      if (OB_ISNULL(pending_action_ = self_ethread().schedule_imm(
              this,  need_refresh_cluster_role? REFRESH_CLUSTER_ROLE_EVENT : REFRESH_ZONE_STATE_EVENT))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to schedule refresh server state", K_(cluster_name), K_(cluster_id), K(imm), K(ret));
      }
    } else {
      delay_us = ObRandomNumUtils::get_random_half_to_full(ss_refresh_interval_us_);
      if (OB_UNLIKELY(delay_us <= 0)) {
        ret = OB_INNER_STAT_ERROR;
        LOG_WDIAG("delay must greater than zero", K(delay_us), K(ret));
      } else if (OB_ISNULL(pending_action_ = self_ethread().schedule_in(
              this, HRTIME_USECONDS(delay_us), need_refresh_cluster_role ? REFRESH_CLUSTER_ROLE_EVENT : REFRESH_ZONE_STATE_EVENT))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to schedule refresh server state", K_(cluster_name), K_(cluster_id), K(delay_us), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      LOG_DEBUG("schedule refresh server state succ", K(delay_us), K(imm), K_(cluster_name), K_(cluster_id));
    }
  }
  return ret;
}

int ObServerStateRefreshCont::main_handler(int event, void *data)
{
  UNUSED(data);
  int event_ret = EVENT_CONT;
  int ret = OB_SUCCESS;
  LOG_DEBUG("[ObServerStateRefreshCont::main_handler] ", K(event), K(cur_job_event_), K(kill_this_));

  switch (event) {
    case DESTROY_SERVER_STATE_EVENT: {
      // if set_interval_task_count_ != 0, we should reschedule
      if (0 == ATOMIC_CAS(&set_interval_task_count_, 0, 0)) {
        LOG_INFO("ObServerStateRefreshCont will terminate", K_(cluster_name), K_(cluster_id));
        kill_this_ = true;
      } else {
        LOG_INFO("there are still set_interval tasks which have been scheduled,"
                 " we should reschedule destroy event", KPC(this));
        if (OB_ISNULL(self_ethread().schedule_imm(this, DESTROY_SERVER_STATE_EVENT))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("fail to schedule DESTROY_SERVER_STATE_EVENT", KPC(this), K(ret));
        }
      }
      break;
    }
    case EVENT_IMMEDIATE: {
      // cancel pending action and reschedule task with newest interval
      ATOMIC_DEC(&set_interval_task_count_);
      if (OB_FAIL(cancel_pending_action())) {
        LOG_WDIAG("fail to cancel pending action", K(ret));
      } else if (OB_FAIL(schedule_refresh_server_state())) {
        LOG_WDIAG("fail to schedule refresh server state", K(ret));
      }
      break;
    }
    case REFRESH_CLUSTER_ROLE_EVENT: {
      pending_action_ = NULL;
      cur_job_event_ = event;
      if (OB_FAIL(refresh_cluster_role())) {
        LOG_WDIAG("fail to refresh cluster role", K(ret));
      }
      break;
    }
    case REFRESH_ZONE_STATE_EVENT: {
      pending_action_ = NULL;
      cur_job_event_ = event;
      if (OB_FAIL(refresh_zone_state())) {
        LOG_WDIAG("fail to refresh zone state", K(ret));
      }
      break;
    }
    case REFRESH_SERVER_STATE_EVENT: {
      pending_action_ = NULL;
      cur_job_event_ = event;
      if (OB_FAIL(refresh_server_state())) {
        LOG_WDIAG("fail to refresh server state", K(ret));
      }
      break;
    }
    case REFRESH_LDG_INFO_EVENT: {
      pending_action_ = NULL;
      cur_job_event_ = event;
      if (OB_FAIL(refresh_ldg_info())) {
        LOG_WDIAG("fail to refresh ldg info", K(ret));
      }
      break;
    }
    case REFRESH_ALL_TENANT_EVENT: {
      pending_action_ = NULL;
      cur_job_event_ = event;
      if (OB_FAIL(refresh_all_tenant())) {
        LOG_WDIAG("fail to refresh all tenant", K(ret));
      }
      break;
    }
    case CLIENT_TRANSPORT_MYSQL_RESP_EVENT: {
      pending_action_ = NULL;
      if (REFRESH_CLUSTER_ROLE_EVENT == cur_job_event_) {
        if (OB_FAIL(handle_cluster_role(data))) {
          LOG_WDIAG("fail to handle cluster role", K(data), K(ret));
        }
      } else if (REFRESH_ZONE_STATE_EVENT == cur_job_event_) {
        if (OB_FAIL(handle_zone_state(data))) {
          LOG_WDIAG("fail to handle zone state", K(data), K(ret));
        }
      } else if (REFRESH_SERVER_STATE_EVENT == cur_job_event_) {
        if (OB_FAIL(handle_server_state(data))) {
          LOG_WDIAG("fail to handle server state", K(data), K(ret));
        } else {
          ss_refresh_failure_ = 0;//reset
        }
      } else if (REFRESH_LDG_INFO_EVENT == cur_job_event_) {
        if (OB_FAIL(handle_ldg_info(data))) {
          LOG_WDIAG("fail to handle ldg info", K(data), K(ret));
        }
      } else if (REFRESH_ALL_TENANT_EVENT == cur_job_event_) {
        if (OB_FAIL(handle_all_tenant(data))) {
          LOG_WDIAG("fail to handle all tenant", K(data), K(ret));
        }
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unknown event", K(event), K(ret));
      break;
    }
  }

  if (OB_FAIL(ret)) {
    bool imm_reschedule = false;
    bool has_slave_clusters = get_global_config_server_processor().has_slave_clusters(cluster_name_);
    // primary-slave cluser mode, if primary cluster refresh failed count greater than three, need reschedule now
    if (ss_refresh_failure_ >= MIN_REFRESH_FAILURE
        && OB_DEFAULT_CLUSTER_ID == cluster_id_
        && has_slave_clusters) {
      imm_reschedule = true;
      LOG_DEBUG("primary cluster server state task has failed more than MIN_REFRESH_FAILURE times, will schedule immediately",
                K(ss_refresh_failure_), K(static_cast<int64_t>(MIN_REFRESH_FAILURE)), K(cluster_name_), K(cluster_id_));
    }
    if (-OB_CLUSTER_NO_MATCH == ret) {
      imm_reschedule = false;
      if (OB_FAIL(handle_delete_cluster_resource(OB_DEFAULT_CLUSTER_ID))) {
        LOG_WDIAG("fail to delete cluster resource", K(ret));
      }
    } else if (ss_refresh_failure_ >= MAX_REFRESH_FAILURE) {
      bool need_refresh_cluster_role = get_global_proxy_config().with_config_server_
                                       && get_global_proxy_config().enable_standby
                                       && IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_resource_->cluster_version_)
                                       && OB_DEFAULT_CLUSTER_ID == cluster_id_
                                       && has_slave_clusters;
      imm_reschedule = false;
      LOG_INFO("refresh server state has failed too many times, should refresh rslist or delete cluster resource",
               K_(ss_refresh_failure), K(static_cast<int64_t>(MAX_REFRESH_FAILURE)), K(cluster_name_), K(cluster_id_), K(need_refresh_cluster_role));

      if (need_refresh_cluster_role) {
        if (OB_FAIL(handle_delete_cluster_resource(OB_DEFAULT_CLUSTER_ID))) {
          LOG_WDIAG("fail to delete cluster resource", K(ret));
        }
      } else {
        bool need_update_dummy_entry = true;
        if (OB_FAIL(add_refresh_rslist_task(need_update_dummy_entry))) {
          LOG_WDIAG("fail to add refresh rslist task", K(ret));
        } else {
          ss_refresh_failure_ = 0;
        }
      }
    }
    // ignore ret, schedule next;
    ret = OB_SUCCESS;
    if (need_reset_in_error_) {
      cur_zones_state_.reset();
      last_zones_state_.reset();
      last_servers_state_.reset();
      last_zones_state_hash_ = 0;
      last_servers_state_hash_ = 0;
      last_server_list_hash_ = 0;
    }

    if (OB_FAIL(cancel_pending_action())) {
      LOG_WDIAG("fail to cancel pending action", K(ret));
    } else if (OB_FAIL(schedule_refresh_server_state(imm_reschedule))) {
      LOG_WDIAG("fail to schedule refresh server state", K(imm_reschedule), K(ret));
    }

    if (OB_FAIL(ret) && OB_SERVER_IS_STOPPING != ret) {
      LOG_EDIAG("ObServerStateRefreshCont will stop running", K_(cluster_name),
                K_(cluster_id), K_(ss_refresh_interval_us), K(ret));
    }
  }

  if (get_global_proxy_config().with_config_server_ && OB_FAIL(check_add_refresh_idc_list_task())) {
    LOG_WDIAG("fail to add refresh idc list task", K_(cluster_name), K_(cluster_id), K(ret));
  }

  if (kill_this_) {
    event_ret = EVENT_DONE;
    kill_this();
  }

  return event_ret;
}

int ObServerStateRefreshCont::cancel_pending_action()
{
  int ret = OB_SUCCESS;
  if (NULL != pending_action_) {
    if (OB_FAIL(pending_action_->cancel())) {
      LOG_WDIAG("fail to cancel pending action", K_(pending_action), K(ret));
    } else {
      pending_action_ = NULL;
    }
  }

  return ret;
}

int ObServerStateRefreshCont::refresh_cluster_role()
{
  int ret = OB_SUCCESS;
  char sql[OB_SHORT_SQL_LENGTH];
  sql[0] = '\0';
  int64_t len = snprintf(sql, OB_SHORT_SQL_LENGTH, SELECT_CLUSTER_ROEL_SQL);
  if (OB_UNLIKELY(len <= 0 || len >= OB_SHORT_SQL_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to fill sql", K(len), K(sql), K(ret));
  } else if (OB_UNLIKELY(NULL != pending_action_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("pending_action should be null here", K_(pending_action), K(ret));
  } else if (OB_FAIL(mysql_proxy_->async_read(this, sql, pending_action_))) {
    LOG_WDIAG("fail to sync read cluster role", K(sql), K(ret));
  } else if (OB_ISNULL(pending_action_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("pending_action can not be NULL", K_(pending_action), K(ret));
  }
  return ret;
}

int ObServerStateRefreshCont::refresh_zone_state()
{
  int ret = OB_SUCCESS;
  char sql[OB_SHORT_SQL_LENGTH];
  sql[0] = '\0';
  int64_t len = 0;
  if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_resource_->cluster_version_)) {
    len = snprintf(sql, OB_SHORT_SQL_LENGTH, SELECT_ZONE_STATE_INFO_SQL,
                   OB_ALL_VIRTUAL_ZONE_STAT_TNAME, INT64_MAX);
  } else {
    len = snprintf(sql, OB_SHORT_SQL_LENGTH, SELECT_ZONE_STATE_INFO_SQL_V4,
                   DBA_OB_ZONES_VNAME, INT64_MAX);
  }

  if (OB_UNLIKELY(len <= 0 || len >= OB_SHORT_SQL_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to fill sql", K(len), K(sql), K(ret));
  } else if (OB_UNLIKELY(NULL != pending_action_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("pending_action should be null here", K_(pending_action), K(ret));
  } else if (OB_FAIL(mysql_proxy_->async_read(this, sql, pending_action_))) {
    LOG_WDIAG("fail to syanc read zone state", K(sql), K(ret));
  } else if (OB_ISNULL(pending_action_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("pending_action can not be NULL", K_(pending_action), K(ret));
  }
  return ret;
}

int ObServerStateRefreshCont::refresh_server_state()
{
  int ret = OB_SUCCESS;
  char sql[OB_SHORT_SQL_LENGTH];
  sql[0] = '\0';
  int64_t len =0;
  if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_resource_->cluster_version_)) {
    len = static_cast<int64_t>(snprintf(sql, OB_SHORT_SQL_LENGTH, SELECT_SERVER_STATE_INFO_SQL,
                                        OB_ALL_VIRTUAL_PROXY_SERVER_STAT_TNAME, INT64_MAX));
  } else {
    len = static_cast<int64_t>(snprintf(sql, OB_SHORT_SQL_LENGTH, SELECT_SERVER_STATE_INFO_SQL_V4,
                                        DBA_OB_SERVERS_VNAME, INT64_MAX));
  }
  if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= OB_SHORT_SQL_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to fill sql", K(len), K(sql), K(ret));
  } else if (OB_UNLIKELY(NULL != pending_action_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("pending_action should be null here", K_(pending_action), K(ret));
  } else if (OB_FAIL(mysql_proxy_->async_read(this, sql, pending_action_))) {
    LOG_WDIAG("fail to syanc read zone state", K(sql), K(ret));
  } else if (OB_ISNULL(pending_action_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("pending_action can not be NULL", K_(pending_action), K(ret));
  }
  return ret;
}

int ObServerStateRefreshCont::handle_ldg_info(void *data)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid data, fail to handle ldg info", K(data), K_(cluster_name), K_(cluster_id),
        K_(ss_refresh_failure), K(ret));
    ++ss_refresh_failure_;
  } else {
    ObClientMysqlResp *resp = reinterpret_cast<ObClientMysqlResp *>(data);
    ObMysqlResultHandler result_handler;
    result_handler.set_resp(resp);
    int64_t tenant_id = 0;
    ObString tenant_name;
    int64_t cluster_id = 0;
    ObString cluster_name;
    ObString ldg_role;
    while (OB_SUCC(ret) && OB_SUCC(result_handler.next())) {
      tenant_name.reset();
      cluster_name.reset();
      ldg_role.reset();
      PROXY_EXTRACT_INT_FIELD_MYSQL(result_handler, "TENANT_ID", tenant_id, int64_t);
      PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(result_handler, "TENANT_NAME", tenant_name);
      PROXY_EXTRACT_INT_FIELD_MYSQL(result_handler, "CLUSTER_ID", cluster_id, int64_t);
      PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(result_handler, "CLUSTER_NAME", cluster_name);
      PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(result_handler, "LDG_ROLE", ldg_role);
      LOG_INFO("sys ldg info", K(tenant_id), K(tenant_name), K(cluster_name), K(cluster_id), K(ldg_role));
      if (OB_SUCC(ret)) {
        ObSysLdgInfo *sys_ldg_info = NULL;
        if (OB_ISNULL(sys_ldg_info = op_alloc(ObSysLdgInfo))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_EDIAG("fail to alloc memory for sys ldg info", K(ret));
        } else {
          sys_ldg_info->tenant_id_ = tenant_id;
          sys_ldg_info->set_tenant_name(tenant_name);
          sys_ldg_info->cluster_id_ = cluster_id;
          sys_ldg_info->set_cluster_name(cluster_name);
          sys_ldg_info->set_ldg_role(ldg_role);
          if (OB_FAIL(cluster_resource_->update_sys_ldg_info(sys_ldg_info))) {
            op_free(sys_ldg_info);
            sys_ldg_info = NULL;
            LOG_WDIAG("update sys ldg info failed", K(ret));
          }
        }
      }
    }
    if (ret != OB_ITER_END) {
      // some cluster do not support LDG
      ret = OB_SUCCESS;
      if (ER_TABLEACCESS_DENIED_ERROR == resp->get_err_code()) {
        LOG_DEBUG("access denied for ldg_standby_status");
      } else if (ER_NO_SUCH_TABLE == resp->get_err_code()) {
        LOG_DEBUG("table ldg_standby_status not exist");
      } else {
        LOG_WDIAG("fail to get all ldg info", K(ret), K(resp->get_err_code()));
      }
      LOG_DEBUG("fail to get ldg info", K(ret), K_(cluster_name));
    } else {
      ret = OB_SUCCESS;
      LOG_DEBUG("get ldg info succ", K(ret), K_(cluster_name));
    }
    if (OB_SUCC(ret) && OB_FAIL(schedule_imm(REFRESH_SERVER_STATE_EVENT))) {
      LOG_WDIAG("fail to schedule imm", K(ret));
    }
  }

  return ret;
}

int ObServerStateRefreshCont::handle_all_tenant(void *data)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid data, fail to refresh all tenant", K(data), K_(cluster_name), K_(cluster_id),
        K_(ss_refresh_failure), K(ret));
    ++ss_refresh_failure_;
  } else {
    ObClientMysqlResp *resp = reinterpret_cast<ObClientMysqlResp *>(data);
    ObMysqlResultHandler result_handler;
    result_handler.set_resp(resp);
    ObString tenant_name;
    ObString locality;
    ObString previous_locality;
    ObString primary_zone;
    ObSEArray<ObString, 4> tenant_array;
    ObSEArray<ObString, 4> locality_array;
    ObSEArray<ObString, 4> primary_zone_array;
    while (OB_SUCC(ret) && OB_SUCC(result_handler.next())) {
      tenant_name.reset();
      locality.reset();
      previous_locality.reset();
      primary_zone.reset();
      PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(result_handler, "tenant_name", tenant_name);
      PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(result_handler, "locality", locality);
      PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(result_handler, "previous_locality", previous_locality);
      PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(result_handler, "primary_zone", primary_zone);

      if (OB_FAIL(tenant_array.push_back(tenant_name))) {
        LOG_WDIAG("tenant array push back failed", K(ret));
      } else if (OB_FAIL(primary_zone_array.push_back(primary_zone))) {
        LOG_WDIAG("primary zone array push back failed", K(ret));
      } else {
        /*
         * in oceanbase.__all_tenant, the column of previous_locality not empty
         * means the locality is in changing state by the control of rootserver, otherwise the locality is stable.
         */
        if (previous_locality.empty()) {
          if (OB_FAIL(locality_array.push_back(locality))) {
            LOG_WDIAG("fail to push locality to array", K(ret));
          }
        } else {
          if (OB_FAIL(locality_array.push_back(previous_locality))) {
            LOG_WDIAG("fail to push previous locality to array", K(ret));
          }
        }
      }
    }

    if (ret != OB_ITER_END) {
      // handle case of fail to access __all_tenant on alipay main site: do not exec error handling process
      if (ER_TABLEACCESS_DENIED_ERROR == resp->get_err_code()) {
        LOG_DEBUG("access denied for __all_teannt");
        ret = OB_SUCCESS;
      } else {
        LOG_WDIAG("fail to get all tenant info", K(ret));
      }
    } else {
      if (OB_FAIL(cluster_resource_->update_location_tenant_info(tenant_array, locality_array, primary_zone_array))) {
        LOG_WDIAG("update location tenant info failed", K(ret));
      } else {
        LOG_DEBUG("update location tenant info succ");
      }
      ret = OB_SUCCESS;
    }

    if (OB_SUCC(ret) && OB_FAIL(schedule_refresh_server_state())) {
      LOG_WDIAG("fail to schedule refresh server state", K(ret));
    }
  }

  return ret;
}

int ObServerStateRefreshCont::handle_delete_cluster_resource(int64_t master_cluster_id)
{
  int ret = OB_SUCCESS;

  //1. if master cluster id changed, update master cluster id
  ObConfigServerProcessor &csp = get_global_config_server_processor();
  if (OB_DEFAULT_CLUSTER_ID == master_cluster_id) {
    LOG_INFO("current cluster has been switched to STANDBY or FailOver, but ob does not return new primary cluster id", K_(cluster_name));
  } else if (OB_FAIL(csp.set_master_cluster_id(cluster_name_, master_cluster_id))) {
    LOG_WDIAG("fail to set master cluster id", K_(cluster_name), K(master_cluster_id), K(ret));
    ret = OB_SUCCESS;
  }
  //2. delete cluster resource
  ObResourcePoolProcessor &rpp = get_global_resource_pool_processor();
  if (cluster_name_ == OB_META_DB_CLUSTER_NAME) {
    const bool ignore_cluster_not_exist = true;
    if (OB_FAIL(rpp.rebuild_metadb(ignore_cluster_not_exist))) {
      PROXY_CS_LOG(WDIAG, "fail to rebuild metadb cluster resource", K(ret));
    }
  } else if (OB_FAIL(rpp.delete_cluster_resource(cluster_name_, cluster_id_))) {
    LOG_WDIAG("fail to delete cluster resource", K_(cluster_name), K(OB_DEFAULT_CLUSTER_ID), K(ret));
  }

  return ret;
}

int ObServerStateRefreshCont::handle_cluster_role(void *data)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid data, fail to refresh cluster role",
             K(data), K_(cluster_name), K_(cluster_id), K_(ss_refresh_failure), K(ret));
    ++ss_refresh_failure_;
  } else {
    int64_t master_cluster_id = OB_DEFAULT_CLUSTER_ID;
    ObClientMysqlResp *resp = reinterpret_cast<ObClientMysqlResp *>(data);
    ObMysqlResultHandler handler;
    handler.set_resp(resp);
    if (OB_FAIL(ObServerStateRefreshUtils::check_cluster_role(handler, master_cluster_id))) {
      LOG_WDIAG("fail to check cluster role, will reschedule", K(ret));
      if (OB_NOT_MASTER == ret) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = handle_delete_cluster_resource(master_cluster_id))) {
          LOG_WDIAG("fail to delete cluster resource", K_(cluster_name), K(master_cluster_id), K(tmp_ret));
        }
        // cluster resouce delete succes, no need reschedule refresh task
        ret = OB_SUCC(tmp_ret) ? tmp_ret : ret;
      }
    } else if (OB_FAIL(schedule_imm(REFRESH_ZONE_STATE_EVENT))) {
      LOG_WDIAG("fail to schedule imm", K(ret));
    }
  }
  return ret;
}

int ObServerStateRefreshCont::handle_zone_state(void *data)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid data, fail to refresh zone state", K(data), K_(cluster_name), K_(cluster_id), K_(ss_refresh_failure), K(ret));
    ++ss_refresh_failure_;
  } else {
    ObClientMysqlResp *resp = reinterpret_cast<ObClientMysqlResp *>(data);
    ObMysqlResultHandler handler;
    handler.set_resp(resp);
    cur_zones_state_.reset();
    if (OB_FAIL(ObServerStateRefreshUtils::get_zone_state_info(handler, cur_zones_state_))) {
      LOG_WDIAG("fail to get zone state", K_(cur_zones_state), K(ret));
    } else {
      if (cur_zones_state_.empty()) {
        LOG_INFO("current zones are empty, unnormal state, reschedule");
        if (OB_FAIL(schedule_refresh_server_state())) {
          LOG_WDIAG("fail to schedule refresh server state", K(ret));
        }
      } else {
        bool is_metadb = (0 == cluster_resource_->cluster_info_key_.cluster_name_.get_string().case_compare(OB_META_DB_CLUSTER_NAME));
        if (get_global_proxy_config().enable_ldg && !is_metadb) {
          if (OB_FAIL(schedule_imm(REFRESH_LDG_INFO_EVENT))) {
            LOG_WDIAG("fail to schedule imm ldg info event", K(ret));
          }
        } else {
          if (OB_FAIL(schedule_imm(REFRESH_SERVER_STATE_EVENT))) {
            LOG_WDIAG("fail to schedule imm", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObServerStateRefreshCont::handle_server_state(void *data)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid data, fail to refresh server state", K(data), K_(ss_refresh_failure), K(ret));
    ++ss_refresh_failure_;
  } else {
    ObClientMysqlResp *resp = reinterpret_cast<ObClientMysqlResp *>(data);
    ObMysqlResultHandler handler;
    handler.set_resp(resp);
    ObSEArray<ObServerStateInfo, DEFAULT_SERVER_COUNT> servers_state;
    bool has_invalid_zone = false;
    bool has_invalid_server = false;
    bool is_zones_state_changed = false;
    bool is_servers_state_changed = false;

    if (OB_FAIL(ObServerStateRefreshUtils::get_server_state_info(
            handler, cur_zones_state_, servers_state, has_invalid_server))) {
      LOG_WDIAG("fail to get server state", K_(cur_zones_state), K(servers_state), K(ret));
    } else {
      // 1. handle zones
      if (OB_FAIL(handle_newest_zone(cur_zones_state_, is_zones_state_changed, has_invalid_zone))) {
        LOG_WDIAG("fail to handle newest zone", K_(cur_zones_state), K(ret));
        need_reset_in_error_ = true;
      }

      // 2. handle servers
      if (!has_invalid_zone && !has_invalid_server && OB_SUCC(ret)) {
        if (OB_FAIL(handle_newest_server(servers_state, is_servers_state_changed, has_invalid_server))) {
          LOG_WDIAG("fail to handle newest server", K(servers_state), K(ret));
          need_reset_in_error_ = true;
        }
      }

      if (!has_invalid_zone && !has_invalid_server && OB_SUCC(ret)) {
        // 3. update last zones state and servers state
        if (OB_FAIL(update_last_zs_state(is_zones_state_changed, is_servers_state_changed,
                                         cur_zones_state_, servers_state))) {
          LOG_WDIAG("fail to update last zone and server state", K(ret));
          need_reset_in_error_ = true;
        }

        // 4. update sys tenant' __all_dummy entry
        if (OB_SUCC(ret) && !servers_state.empty() && OB_LIKELY(cluster_resource_->is_avail())) {
          if (OB_FAIL(update_all_dummy_entry(servers_state))) {
            LOG_WDIAG("fail to update all dummy entry", K(servers_state), K(ret));
          }
        }

        // 5. update read snapshot entry
        if (OB_SUCC(ret) && !servers_state.empty() && OB_LIKELY(cluster_resource_->is_avail())) {
          if (OB_FAIL(update_safe_snapshot_manager(servers_state))) {
            LOG_WDIAG("fail to update snapshot manager", K(servers_state), K(ret));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (get_global_proxy_config().check_tenant_locality_change) {
      if (OB_FAIL(schedule_imm(REFRESH_ALL_TENANT_EVENT))) {
        LOG_WDIAG("fail to schedule imm all tenant event", K(ret));
      }
    } else {
      if (OB_FAIL(schedule_refresh_server_state())) {
        LOG_WDIAG("fail to schedule refresh server state", K(ret));
      }
    }
  }

  return ret;
}

int ObServerStateRefreshCont::refresh_ldg_info()
{
  int ret = OB_SUCCESS;
  char sql[OB_SHORT_SQL_LENGTH];
  sql[0] = '\0';
  const int64_t len = static_cast<int64_t>(snprintf(sql, OB_SHORT_SQL_LENGTH, SYS_LDG_INFO_SQL));
  if (OB_UNLIKELY(len < 0) || OB_UNLIKELY(len >= OB_SHORT_SQL_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to fill sql", K(len), K(sql), K(ret));
  } else if (OB_UNLIKELY(NULL != pending_action_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("pending_action should be null here", K_(pending_action), K(ret));
  } else if (OB_FAIL(mysql_proxy_->async_read(this, sql, pending_action_))) {
    LOG_WDIAG("fail to async read all tenant", K(sql), K(ret));
  } else if (OB_ISNULL(pending_action_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("pending_action can not be NULL", K_(pending_action), K(ret));
  }
  return ret;
}

int ObServerStateRefreshCont::refresh_all_tenant()
{
  int ret = OB_SUCCESS;
  char sql[OB_SHORT_SQL_LENGTH];
  sql[0] = '\0';
  const int64_t len = static_cast<int64_t>(snprintf(sql, OB_SHORT_SQL_LENGTH, SELECT_ALL_TENANT_SQL,
    IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_resource_->cluster_version_) ? OB_ALL_TENANT_TNAME : DBA_OB_TENANTS_VNAME,
    IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_resource_->cluster_version_) ? "previous_locality = ''" : "previous_locality is null"));
  if (OB_UNLIKELY(len <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to fill sql", K(len), K(sql), K(ret));
  } else if (OB_UNLIKELY(NULL != pending_action_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("pending_action should be null here", K_(pending_action), K(ret));
  } else if (OB_FAIL(mysql_proxy_->async_read(this, sql, pending_action_))) {
    LOG_WDIAG("fail to async read all tenant", K(sql), K(ret));
  } else if (OB_ISNULL(pending_action_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("pending_action can not be NULL", K_(pending_action), K(ret));
  }
  return ret;
}

int ObServerStateRefreshCont::update_last_zs_state(
    const bool is_zones_state_changed,
    const bool is_servers_state_changed,
    ObIArray<ObZoneStateInfo> &zones_state,
    ObIArray<ObServerStateInfo> &servers_state)
{
  int ret = OB_SUCCESS;
  if (!zones_state.empty() && !servers_state.empty()) {
    if (!is_zones_state_changed && !is_servers_state_changed) {
      // do nothing
    } else {
      if (OB_FAIL(last_zones_state_.assign(zones_state))) {
        LOG_WDIAG("fail to assign last zone state", K(zones_state), K(ret));
      } else {
        ObZoneStateInfo *tmp_zsi = NULL;
        ObString target_zname;
        for (int64_t i = 0; (i < servers_state.count()) && OB_SUCC(ret); ++i) {
          if (OB_ISNULL(servers_state.at(i).zone_state_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("zone state can not be NULL", K(servers_state), K(i), K(ret));
          } else {
            target_zname = servers_state.at(i).zone_state_->zone_name_;
            tmp_zsi = ObServerStateRefreshUtils::get_zone_info_ptr(last_zones_state_, target_zname);
            if (OB_ISNULL(tmp_zsi)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WDIAG("can not find zone info", K(target_zname), K(ret));
            } else {
              servers_state.at(i).zone_state_ = tmp_zsi;
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(last_servers_state_.assign(servers_state))) {
            LOG_WDIAG("fail to assign last servers state", K(servers_state), K(ret));
          } else {
            if (!congestion_manager_->is_base_servers_added()) {
              congestion_manager_->set_base_servers_added();
              LOG_INFO("congestion manager's base servers has added", K_(cluster_name), K_(cluster_id));
            }

            if (cluster_resource_->is_avail()) {
              const uint64_t new_ss_version = cluster_resource_->server_state_version_ + 1;
              common::DRWLock &server_state_lock = cluster_resource_->get_server_state_lock(new_ss_version);
              server_state_lock.wrlock();
              common::ObIArray<ObServerStateSimpleInfo> &server_state_info = cluster_resource_->get_server_state_info(new_ss_version);
              common::ObIArray<ObServerStateSimpleInfo> &old_server_state_info = cluster_resource_->get_server_state_info(new_ss_version - 1);
              server_state_info.reuse();
              ObServerStateSimpleInfo simple_server_info;
              for (int64_t i = 0; OB_SUCC(ret) && i < servers_state.count(); ++i) {
                ObServerStateInfo &server_state = servers_state.at(i);
                if (OB_ISNULL(server_state.zone_state_)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WDIAG("zone_state_ is NULL", "server_state", server_state, K(ret));
                } else {
                  simple_server_info.reset();
                  simple_server_info.is_merging_ = server_state.zone_state_->is_merging_;
                  simple_server_info.is_force_congested_ = server_state.is_treat_as_force_congested();
                  simple_server_info.zone_type_ = server_state.zone_state_->zone_type_;
                  if (OB_FAIL(simple_server_info.set_addr(server_state.replica_.server_))) {
                    LOG_WDIAG("fail to set addr", "addr", server_state.replica_.server_, K(ret));
                  } else if (OB_FAIL(simple_server_info.set_zone_name(server_state.zone_state_->zone_name_))) {
                    LOG_WDIAG("fail to set zone name", "zone_name", server_state.zone_state_->zone_name_, K(ret));
                  } else if (OB_FAIL(simple_server_info.set_region_name(server_state.zone_state_->region_name_))) {
                    LOG_WDIAG("fail to set region name", "region_name", server_state.zone_state_->region_name_, K(ret));
                  } else if (OB_FAIL(simple_server_info.set_idc_name(server_state.zone_state_->idc_name_))) {
                    LOG_WDIAG("fail to set idc name", "idc_name", server_state.zone_state_->idc_name_, K(ret));
                  }
                  if (OB_SUCC(ret)) {
                    for (int64_t i = 0; i < old_server_state_info.count(); i++) {
                      ObServerStateSimpleInfo &tmp_info = old_server_state_info.at(i);
                      if (simple_server_info.addr_ == tmp_info.addr_) {
                        simple_server_info.request_sql_cnt_ = tmp_info.request_sql_cnt_;
                        simple_server_info.last_response_time_ = tmp_info.last_response_time_;
                        simple_server_info.detect_fail_cnt_ = tmp_info.detect_fail_cnt_;
                        tmp_info.reset();
                        break;
                      }
                    }
                    LOG_DEBUG("update server state info", K(simple_server_info), K(new_ss_version));
                    if (OB_FAIL(server_state_info.push_back(simple_server_info))) {
                      LOG_WDIAG("fail to push back server_info", K(simple_server_info), K(ret));
                    }
                  }
                }
              }
              if (OB_SUCC(ret)) {
                ATOMIC_AAF(&(cluster_resource_->server_state_version_), 1);
                LOG_INFO("succ to update servers_info_version",
                         "cluster_info", cluster_name_,
                         "cluster_id", cluster_id_,
                         "server_state_version", cluster_resource_->server_state_version_,
                         K(cluster_resource_),
                         K(server_state_info));
              }
              server_state_lock.wrunlock();
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObServerStateRefreshCont::schedule_imm(const int event)
{
  int ret = OB_SUCCESS;
  if (NULL != pending_action_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("pending_action must be NULL", K_(pending_action), K(ret));
  } else if (get_global_hot_upgrade_info().is_graceful_exit_timeout(get_hrtime())) {
    ret = OB_SERVER_IS_STOPPING;
    LOG_WDIAG("proxy need exit now", K(ret));
  } else if (OB_ISNULL(pending_action_ = self_ethread().schedule_imm(this, event))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("fail to schedule imm", K(event), K(ret));
  }
  return ret;
}

int ObServerStateRefreshCont::handle_newest_zone(
    ObIArray<ObZoneStateInfo> &zones_state,
    bool &is_zones_state_changed,
    bool &has_invalid_zone)
{
  int ret = OB_SUCCESS;
  has_invalid_zone = false;
  const int64_t total_zone_count = zones_state.count();
  // update zones state info
  if (total_zone_count > 0) {
    int64_t upgrade_zone_count = 0;
    int64_t merge_zone_count = 0;
    int64_t active_zone_count = 0;
    ObCongestionZoneState::ObZoneState state = ObCongestionZoneState::ACTIVE;
    for (int64_t i = 0; OB_SUCC(ret) && (i < total_zone_count); ++i) {
      // do not call handle_deleted_zone if one of zone_state is not valid;
      if (!zones_state.at(i).is_valid()) {
        LOG_WDIAG("invalid zone state", K(zones_state.at(i)));
        has_invalid_zone = true;
        break; // no need continue
      } else {
        state = ObCongestionZoneState::ACTIVE;
        switch (zones_state.at(i).zone_status_) {
          case ObZoneStatus::INACTIVE:
            // now, the state INACTIVE of zone is treated as upgrade
            state = ObCongestionZoneState::UPGRADE;
            upgrade_zone_count++;
            break;
          case ObZoneStatus::ACTIVE:
            if (zones_state.at(i).is_merging_) {
              state = ObCongestionZoneState::MERGE;
              merge_zone_count++;
            } else {
              state = ObCongestionZoneState::ACTIVE;
              active_zone_count++;
            }
            break;
          default:
            break;
        }
        zones_state.at(i).cgt_zone_state_ = state;
      }
    }

    uint64_t cur_zones_state_hash = ObServerStateRefreshUtils::get_zones_state_hash(zones_state);
    LOG_DEBUG("zones state hash", K(cur_zones_state_hash), K_(last_zones_state_hash), K(has_invalid_zone));
    if (has_invalid_zone) {
      LOG_INFO("has invalid zone, do not update zone", K(cur_zones_state_hash),
               K_(last_zones_state_hash), K(has_invalid_zone));
    } else {
      // only when some zone state changed, we need update zone state
      if (cur_zones_state_hash != last_zones_state_hash_) {
        is_zones_state_changed = true;
        last_zones_state_hash_ = cur_zones_state_hash;
      } else {
        LOG_DEBUG("zones state has not changed, no need update",
                  K_(cur_zones_state), K(cur_zones_state_hash), K_(cluster_name), K_(cluster_id));
      }

      if (is_zones_state_changed) {
        if (total_zone_count != (active_zone_count + merge_zone_count + upgrade_zone_count)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("zone count missmatch", K(active_zone_count), K(merge_zone_count),
                   K(upgrade_zone_count), K(total_zone_count), K(zones_state), K(ret));
        } else {
          for (int64_t i = 0; (i < total_zone_count); ++i) {
            const ObZoneStateInfo &zs_info = zones_state.at(i);
            if (OB_FAIL(do_update_zone(zs_info))) {
              LOG_WDIAG("fail to update zone state", K(zs_info),  K(ret));
            }
          }
        }

        // handle deleted zone, if has
        if (OB_SUCC(ret)) {
          if (OB_FAIL(handle_deleted_zone(zones_state))) {
            LOG_WDIAG("fail to handle deleted zone", K(zones_state), K(ret));
          }
        }

        LOG_INFO("zone state info", K(has_invalid_zone), K(is_zones_state_changed),
                 K(ret), K(zones_state));
      }
    }
  }
  return ret;
}

int ObServerStateRefreshCont::do_update_zone(const ObZoneStateInfo &zs_info)
{
  int ret = OB_SUCCESS;
  bool need_update = true;
  bool found = false;
  const ObZoneStateInfo *last_zs_info = NULL;
  for (int64_t i = 0; (i < last_zones_state_.count()) && !found; ++i) {
    last_zs_info = &(last_zones_state_.at(i));
    if (zs_info.zone_name_ == last_zs_info->zone_name_) {
      found = true;
      if (zs_info.cgt_zone_state_ == last_zs_info->cgt_zone_state_
          && zs_info.region_name_ == last_zs_info->region_name_) {
        need_update = false;
      }
    }
  }

  if (need_update) {
    bool is_init = !congestion_manager_->is_base_servers_added();
    if (OB_FAIL(congestion_manager_->update_zone(zs_info.zone_name_, zs_info.region_name_, zs_info.cgt_zone_state_, is_init))) {
      LOG_WDIAG("fail to update zone state", KPC(last_zs_info), K(zs_info), K(is_init), K(ret));
    } else {
      LOG_INFO("zone state has changed, update it", KPC(last_zs_info), K(zs_info), K(is_init), K(ret));
    }
  }

  return ret;
}

int ObServerStateRefreshCont::handle_rs_changed(const ObIArray<ObServerStateInfo> &servers_state)
{
  int ret = OB_SUCCESS;
  bool found_rs = true;
  ObConfigServerProcessor &cs_processor = get_global_config_server_processor();
  ObProxyJsonConfigInfo *json_info = cs_processor.acquire();
  if (OB_ISNULL(json_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("json config info is null", K(ret));
  } else {
    ObProxySubClusterInfo *sub_cluster_info = NULL;
    if (OB_FAIL(json_info->get_sub_cluster_info(cluster_name_, cluster_id_, sub_cluster_info))) {
      LOG_WDIAG("fail to get cluster info", K_(cluster_name), K_(cluster_id), K(ret));
    } else if (OB_ISNULL(sub_cluster_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("cluster info is null", K_(cluster_name), K_(cluster_id), K(ret));
    } else {
      const LocationList &rslist = sub_cluster_info->web_rs_list_;
      for (int64_t i = 0; found_rs && i < rslist.count(); ++i) {
        found_rs = false;
        for (int64_t j = 0; !found_rs && j < servers_state.count(); ++j) {
          if (rslist.at(i).server_.is_ip_loopback() || rslist.at(i).server_ == servers_state.at(j).replica_.server_) {
            found_rs = true;
          }
        }
        if (!found_rs) {
          LOG_INFO("rs not in server list, will schedule task to refresh rslist", "rs server", rslist.at(i).server_);
        }
      }
    }
  }
  cs_processor.release(json_info);
  json_info = NULL;

  if (OB_SUCC(ret) && !found_rs) {
    //if only rs changed, we can do not refresh rslist here.
    //otherwise the sys dummy entry will come from rslist. it is dangerous in some case
    bool need_update_dummy_entry = false;
    if (OB_FAIL(add_refresh_rslist_task(need_update_dummy_entry))) {
      LOG_WDIAG("fail to add refresh rslist", K(ret));
    }
  }
  return ret;
}

int ObServerStateRefreshCont::handle_newest_server(
    ObIArray<ObServerStateInfo> &servers_state,
    bool &is_servers_state_changed,
    bool &has_invalid_server)
{
  int ret = OB_SUCCESS;
  if (!servers_state.empty()) {
    // update servers state info
    ObCongestionEntry::ObServerState state = ObCongestionEntry::ACTIVE;
    for (int64_t i = 0; OB_SUCC(ret) && (i < servers_state.count()); ++i) {
      if (!servers_state.at(i).is_valid()) {
        LOG_WDIAG("invalid server stat", K(servers_state.at(i)));
        has_invalid_server = true;
        break; // no need continue
      } else {
        state = ObCongestionEntry::ACTIVE;
        if (servers_state.at(i).is_upgrading()) {
          state = ObCongestionEntry::UPGRADE;
        } else if (!servers_state.at(i).is_in_service()) {
          if (ObServerStatus::OB_SERVER_ACTIVE == servers_state.at(i).server_status_) {
            state = ObCongestionEntry::REPLAY;
          } else {
            state = ObCongestionEntry::INACTIVE;
          }
        } else {
          switch (servers_state.at(i).server_status_) {
            case ObServerStatus::OB_SERVER_INACTIVE:
              state = ObCongestionEntry::INACTIVE;
              break;
            case ObServerStatus::OB_SERVER_ACTIVE:
              state = ObCongestionEntry::ACTIVE;
              break;
            case ObServerStatus::OB_SERVER_DELETING:
              state = ObCongestionEntry::DELETING;
              break;
            default:
              break;
          }
        }
        servers_state.at(i).cgt_server_state_ = state;
      }
    }

    uint64_t cur_servers_state_hash = ObServerStateRefreshUtils::get_servers_state_hash(servers_state);
    LOG_DEBUG("servers state hash", K(cur_servers_state_hash), K_(last_servers_state_hash), K(has_invalid_server));
    if (has_invalid_server) {
      LOG_INFO("has invalid server, do not update server",
               K(cur_servers_state_hash), K_(last_servers_state_hash), K(has_invalid_server));
    } else {
      // only when some server state changed, we need update server state
      if (cur_servers_state_hash != last_servers_state_hash_) {
        is_servers_state_changed = true;
        last_servers_state_hash_ = cur_servers_state_hash;
        LOG_INFO("servers state has been changed",
                 "old servers state", last_servers_state_,
                 "new servers state", servers_state, K_(cluster_name), K_(cluster_id));
      } else {
        LOG_DEBUG("server state has not changed, no need update",
                  K(servers_state), K(cur_servers_state_hash), K_(cluster_name), K_(cluster_id));
      }

      if (is_servers_state_changed || 0 != get_global_resource_pool_processor().ip_set_.size()) {
        // check rslist to see if need to update rslist, ignore ret
        if (OB_FAIL(handle_rs_changed(servers_state))) {
          LOG_WDIAG("fail to handle rslist changed", K(ret));
        }

        // update server
        for (int64_t i = 0; OB_SUCC(ret) && (i < servers_state.count()); ++i) {
          if (OB_FAIL(do_update_server(servers_state.at(i)))) {
            LOG_WDIAG("fail to update server state", K(servers_state), K(ret));
          }
        }
        // handle deleted server,if has
        if (OB_SUCC(ret)) {
          if (OB_FAIL(handle_deleted_server(servers_state))) {
            LOG_WDIAG("fail to handle deleted server", K(servers_state), K(ret));
          }
        }
        LOG_INFO("server state info", K(servers_state));
      }
    }
  }

  return ret;
}

int ObServerStateRefreshCont::do_update_server(const ObServerStateInfo &ss_info)
{
  int ret = OB_SUCCESS;
  bool need_update = true;
  bool found = false;
  const ObString &zone_name = ss_info.zone_state_->zone_name_;
  const ObString &region_name = ss_info.zone_state_->region_name_;
  const ObCongestionEntry::ObServerState cgt_server_state = ss_info.cgt_server_state_;
  ObServerStateInfo *last_ss_info = NULL;
  for (int64_t i = 0; (i < last_servers_state_.count()) && !found; ++i) {
    last_ss_info = &(last_servers_state_.at(i));
    if (last_ss_info->replica_.server_ == ss_info.replica_.server_) {
      found = true;
      if (zone_name == last_ss_info->zone_state_->zone_name_
          && cgt_server_state == last_ss_info->cgt_server_state_) {
        // no need check region name, zone update will do this job
        need_update = false;
      }
    }
  }

  ObIpEndpoint ip(ss_info.replica_.server_.get_sockaddr());
  if (!need_update && ObCongestionEntry::ACTIVE == ss_info.cgt_server_state_) {
    if (OB_HASH_EXIST == get_global_resource_pool_processor().ip_set_.exist_refactored(ip)) {
      need_update = true;
    }
  }

  if (need_update) {
    bool is_init = !congestion_manager_->is_base_servers_added();
    int64_t cr_version = cluster_resource_->version_;
    if (OB_FAIL(congestion_manager_->update_server(ip, cr_version, cgt_server_state, zone_name, region_name, is_init))) {
      LOG_WDIAG("fail to update update server", KPC(last_ss_info), K(cr_version),
               K(ss_info),  K(is_init), K(ret));
    } else {
      LOG_INFO("server state has changed, update it", KPC(last_ss_info), K(cr_version), K(ss_info), K(is_init), K(ret));
    }
  }

  return ret;
}

int ObServerStateRefreshCont::add_refresh_rslist_task(const bool need_update_dummy_entry)
{
  int ret = OB_SUCCESS;
  ObProxyMutex *mutex = NULL;
  ObRslistFetchCont *cont = NULL;
  if (0 == ATOMIC_CAS(&cluster_resource_->fetch_rslist_task_count_, 0, 1)) {
    if (OB_ISNULL(mutex = new_proxy_mutex())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_EDIAG("fail to alloc memory for mutex", K(ret));
    } else if (FALSE_IT(cluster_resource_->inc_ref())) {
      // impossible
    } else if (OB_ISNULL(cont = new(std::nothrow) ObRslistFetchCont(cluster_resource_, mutex, need_update_dummy_entry, NULL))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_EDIAG("fail to alloc memory for ObRslist", K(ret));
      if (OB_LIKELY(NULL != mutex)) {
        mutex->free();
        mutex = NULL;
      }
      cluster_resource_->dec_ref();
    } else if (OB_ISNULL(g_event_processor.schedule_imm(cont, ET_TASK))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to schedule refresh rslist task", K(ret));
      cluster_resource_->dec_ref();
    } else {
      cluster_resource_->last_rslist_refresh_time_ns_ = get_hrtime();
      LOG_INFO("succ to add refresh rslist task", K_(cluster_name), K_(cluster_id));
    }

    if (OB_FAIL(ret)) {
      (void)ATOMIC_FAA(&cluster_resource_->fetch_rslist_task_count_, -1);
      if (OB_LIKELY(NULL != cont)) {
        cont->destroy();
        cont = NULL;
      }
    }
  } else {
    LOG_DEBUG("refresh rslist task has been scheduled", K_(cluster_name), K_(cluster_id),
              "fetch_count", cluster_resource_->fetch_rslist_task_count_,
              "last_time", cluster_resource_->last_rslist_refresh_time_ns_);
  }

  return ret;
}

int ObServerStateRefreshCont::check_add_refresh_idc_list_task()
{
  int ret = OB_SUCCESS;
  ObProxyMutex *mutex = NULL;
  ObIDCListFetchCont *cont = NULL;
  const int64_t idc_list_refresh_interval_us = get_global_proxy_config().idc_list_refresh_interval.get();
  if (0 == cluster_resource_->last_idc_list_refresh_time_ns_
      || get_hrtime() - cluster_resource_->last_idc_list_refresh_time_ns_ > HRTIME_USECONDS(idc_list_refresh_interval_us)) {
    if (0 == ATOMIC_CAS(&cluster_resource_->fetch_idc_list_task_count_, 0, 1)) {
      if (OB_ISNULL(mutex = new_proxy_mutex())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("fail to alloc memory for mutex", K(ret));
      } else if (FALSE_IT(cluster_resource_->inc_ref())) {
        // impossible
      } else if (OB_ISNULL(cont = new(std::nothrow) ObIDCListFetchCont(cluster_resource_, mutex, NULL))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("fail to alloc memory for ObIDCListFetchCont", K(ret));
        if (OB_LIKELY(NULL != mutex)) {
          mutex->free();
          mutex = NULL;
        }
        cluster_resource_->dec_ref();
      } else if (OB_ISNULL(g_event_processor.schedule_imm(cont, ET_TASK))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to schedule refresh idc list task", K(ret));
        cluster_resource_->dec_ref();
      } else {
        cluster_resource_->last_idc_list_refresh_time_ns_ = get_hrtime();
        LOG_INFO("succ to add refresh idc list task", K_(cluster_name), K_(cluster_id), K(idc_list_refresh_interval_us));
      }

      if (OB_FAIL(ret)) {
        (void)ATOMIC_FAA(&cluster_resource_->fetch_idc_list_task_count_, -1);
        if (OB_LIKELY(NULL != cont)) {
          cont->destroy();
          cont = NULL;
        }
      }
    } else {
      LOG_DEBUG("refresh idc list task has been scheduled", K_(cluster_name), K_(cluster_id),
                "fetch_count", cluster_resource_->fetch_idc_list_task_count_,
                "last_time", cluster_resource_->last_idc_list_refresh_time_ns_);
    }
  } else {
    LOG_DEBUG("it is not time to refresh idc list", K_(cluster_name), K_(cluster_id),
              "fetch_count", cluster_resource_->fetch_idc_list_task_count_,
              "last_time", cluster_resource_->last_idc_list_refresh_time_ns_);
  }

  return ret;
}

//NOTE::Assume servers_state is order by zone
//for example: zone_name(server_count) -- A(2):B(2):C(2)
//input servers  : A1 A2 B1 B2 C1 C2
//output rs_list : A1 B1 C1 A2 B2 C2
int ObServerStateRefreshCont::update_all_dummy_entry(const ObIArray<ObServerStateInfo> &servers_state)
{
  int ret = OB_SUCCESS;
  LocationList server_list; //save the ordered servers

  if (OB_FAIL(ObServerStateRefreshUtils::order_servers_state(servers_state, server_list))) {
    LOG_WDIAG("fail to order servers state", K(ret));
  }

  // update rslist
  bool need_update_dummy_entry = true;
  bool is_rs_list_changed = false;
  if (OB_SUCC(ret) && OB_LIKELY(!server_list.empty())) {
    ObConfigServerProcessor &cs_processor = get_global_config_server_processor();
    uint64_t cur_server_list_hash = 0;
    uint64_t last_rs_list_hash = 0;
    // only when some server addr changed, we need update and dump rslist
    if (servers_state.count() != last_servers_state_.count()) {
      is_rs_list_changed = true;
    } else if (OB_FAIL(cs_processor.get_rs_list_hash(cluster_name_, cluster_id_, last_rs_list_hash))) {
      LOG_WDIAG("fail to get_last_rs_list_hash", K_(cluster_name), K_(cluster_id), K(ret));
    } else if (last_rs_list_hash != last_server_list_hash_) {
      LOG_INFO("rs list maybe update by rs refresh task, current used dummy entry maybe old one. "
               "We need stop to update server list and dummy entry this time",
               K(cluster_name_), K(last_rs_list_hash), K_(last_server_list_hash), K(server_list));
      last_server_list_hash_ = last_rs_list_hash;
      need_update_dummy_entry = false;
    } else if (last_rs_list_hash != (cur_server_list_hash = ObProxyClusterInfo::get_server_list_hash(server_list))) {
      is_rs_list_changed = true;
    }
    LOG_DEBUG("servers addr hash", K(cur_server_list_hash), K(last_rs_list_hash),
                                   K(is_rs_list_changed), K_(cluster_name), K_(cluster_id));
    if (is_rs_list_changed) {
      if (OB_FAIL(cs_processor.update_rslist(cluster_name_, cluster_id_, server_list, cur_server_list_hash))) {
        LOG_WDIAG("fail to update rslist", K(cluster_name_), K_(cluster_id), K(server_list), K(ret));
      } else {
        last_server_list_hash_ = cur_server_list_hash;
        LOG_DEBUG("succ to update rslist", K(cluster_name_), K_(cluster_id), K(server_list));
      }
    }
  }//end of update rslist

  // 4. add to table location
  if (OB_SUCC(ret) && OB_LIKELY(!server_list.empty()) && need_update_dummy_entry) {
    LOG_DEBUG("succ to get server list", K(server_list), "server_count", servers_state.count());
    ObTableEntry *entry = NULL;
    ObTableCache &table_cache = get_global_table_cache();
    const bool is_rslist = false;
    if (OB_FAIL(ObRouteUtils::build_sys_dummy_entry(cluster_name_, cluster_id_, server_list, is_rslist, entry))) {
      LOG_WDIAG("fail to build sys dummy entry", K(server_list), K_(cluster_name), K_(cluster_id), K(ret));
    } else if (OB_ISNULL(entry) || OB_UNLIKELY(!entry->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("entry can not be NULL here", K(entry), K(ret));
    } else {
      ObTableEntry *tmp_entry = NULL;
      tmp_entry = entry;
      tmp_entry->inc_ref();//just for print
      if (OB_FAIL(ObTableCache::add_table_entry(table_cache, *entry))) {
        LOG_WDIAG("fail to add table entry", K(*entry), K(ret));
      } else {
        obsys::CWLockGuard guard(cluster_resource_->dummy_entry_rwlock_);
        if (NULL != cluster_resource_->dummy_entry_) {
          cluster_resource_->dummy_entry_->dec_ref();
        }
        cluster_resource_->dummy_entry_ = tmp_entry;
        cluster_resource_->dummy_entry_->inc_ref();

        if (is_rs_list_changed) {
          LOG_INFO("ObServerStateRefreshCont, update sys tennant's __all_dummy succ", KPC(tmp_entry));
        } else {
          LOG_DEBUG("ObServerStateRefreshCont, update sys tennant's __all_dummy succ", KPC(tmp_entry));
        }
      }
      tmp_entry->dec_ref();
      tmp_entry = NULL;
    }

    if (OB_FAIL(ret)) {
      if (NULL != entry) {
        entry->dec_ref();
        entry = NULL;
      }
    }
  }//end of add to table location
  return ret;
}

int ObServerStateRefreshCont::update_safe_snapshot_manager(
    const ObIArray<ObServerStateInfo> &servers_state)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < servers_state.count(); ++i) {
    const ObServerStateInfo &server_state = servers_state.at(i);
    if (NULL == cluster_resource_->safe_snapshot_mgr_.get(server_state.replica_.server_)) {
      if (OB_FAIL(cluster_resource_->safe_snapshot_mgr_.add(server_state.replica_.server_))) {
        LOG_WDIAG("failed to add server", K(server_state.replica_.server_), K(ret));
        // ignore ret and go on
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObServerStateRefreshCont::handle_deleted_zone(ObIArray<ObZoneStateInfo> &zones_state)
{
  int ret = OB_SUCCESS;
  bool found = false;
  for (int64_t i = 0; OB_SUCC(ret) && (i < last_zones_state_.count()); ++i) {
    found = false;
    const ObZoneStateInfo &last_zs_info = last_zones_state_.at(i);
    for (int64_t j = 0; !found && j < zones_state.count(); ++j) {
      if (last_zs_info.zone_name_ == zones_state.at(j).zone_name_) {
        found = true;
      }
    }
    if (!found) { // the zone has been deleted
      LOG_INFO("deleted zone", K(last_zs_info));
      if (OB_FAIL(congestion_manager_->update_zone(last_zs_info.zone_name_,
                                                   last_zs_info.region_name_,
                                                   ObCongestionZoneState::DELETED))) {
        LOG_WDIAG("fail to delete zone", K(last_zs_info), K(ret));
      }
    }
  }
  return ret;
}

int ObServerStateRefreshCont::handle_deleted_server(ObIArray<ObServerStateInfo> &servers_state)
{
  int ret = OB_SUCCESS;
  // find the deleted server
  // simple, rude, but it works;
  bool found = false;
  ObIpEndpoint ip;
  bool is_init = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < last_servers_state_.count(); ++i) {
    found = false;
    const ObServerStateInfo &last_ss_info = last_servers_state_.at(i);
    for (int64_t j = 0; !found && j < servers_state.count(); ++j) {
      if (last_ss_info.replica_.server_ == servers_state.at(j).replica_.server_) {
        found = true;
      }
    }
    if (!found) {
      LOG_INFO("deleted server", "ss_info", last_ss_info);
      ip.assign(last_ss_info.replica_.server_.get_sockaddr());
      int64_t cr_version = cluster_resource_->version_;
      if (OB_FAIL(congestion_manager_->update_server(ip, cr_version, ObCongestionEntry::DELETED,
          last_ss_info.zone_state_->zone_name_,
          last_ss_info.zone_state_->region_name_, is_init))) {
        LOG_WDIAG("fail to delete server", KPC(last_ss_info.zone_state_), K(cr_version), K(ret));
      }
    }
  }
  return ret;
}

int ObServerStateRefreshCont::set_server_state_refresh_interval(const int64_t interval)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(interval <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid server state refresh interval", K(interval), K(ret));
  } else {
    // add set_interval_task_count_ before schedule_imm, and dec when failed
    ss_refresh_interval_us_ = interval;
    ATOMIC_INC(&set_interval_task_count_);
    if (OB_ISNULL(g_event_processor.schedule_imm(this, ET_CALL))) {
      ATOMIC_DEC(&set_interval_task_count_);
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("schedule imm ss_refresh task error", K(ret));
    }
  }
  return ret;
}

//----------------------------ObServerStateRefreshUtils------------------------------------//

// if current cluster is master, cluster_role = primary, primary_cluster_id = NULL
// if current cluster is slave, cluster_role = standby, primary_cluster_id is primary clusuter id
int ObServerStateRefreshUtils::check_cluster_role(ObMysqlResultHandler &handler, int64_t &master_cluster_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(handler.next())) {
    if (-ER_NO_SUCH_TABLE == ret) {
      LOG_DEBUG("v$ob_cluster is not exist, maybe ob version < 2.2.4", K(ret));
      ret = OB_SUCCESS;
    } else if (-ER_TABLEACCESS_DENIED_ERROR == ret) {
      LOG_DEBUG("v$ob_cluster is denied access, maybe revoke proxyro, no need check", K(ret));
      ret = OB_SUCCESS;
    } else {
      LOG_WDIAG("fail to get resultset row for cluster role", K(ret));
    }
  } else {
    ObString cluster_role;
    ObString cluster_status;
    PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(handler, "cluster_role", cluster_role);
    PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(handler, "cluster_status", cluster_status);

    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(0 != cluster_role.case_compare("PRIMARY"))
          || OB_UNLIKELY(0 != cluster_status.case_compare("VALID"))) {
        // if OBServer do not election primary cluster, primary_cluster_id is NULL. ignore return value
        PROXY_EXTRACT_INT_FIELD_MYSQL(handler, "primary_cluster_id", master_cluster_id, int64_t);
        ret = OB_NOT_MASTER;
        LOG_WDIAG("fail to check PRIMARY cluster role",
                 "remote cluster role", cluster_role,
                 "remote cluster status", cluster_status, K(master_cluster_id), K(ret));
      }
    }
  }
  return ret;
}


int ObServerStateRefreshUtils::get_zone_state_info(ObMysqlResultHandler &result_handler,
                                                   ObIArray<ObZoneStateInfo> &zone_info)
{
  int ret = OB_SUCCESS;

  ObZoneStateInfo zone_state;
  ObString zone_status;
  ObString zone_name;
  ObString region_name;
  ObString idc_name;
  ObString zone_type;

  while (OB_SUCC(ret) && OB_SUCC(result_handler.next())) {
    zone_status.reset();
    zone_state.reset();
    zone_name.reset();
    region_name.reset();
    idc_name.reset();
    zone_type.reset();
    PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(result_handler, "zone", zone_name);
    PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(result_handler, "status", zone_status);
    PROXY_EXTRACT_INT_FIELD_MYSQL(result_handler, "is_merging", zone_state.is_merging_, int64_t);

    if (OB_SUCC(ret)) {
      PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(result_handler, "region", region_name);
      if ((OB_ERR_COLUMN_NOT_FOUND == ret) || (OB_SUCC(ret) && region_name.empty())) {
        LOG_DEBUG("region_name is unexpected, maybe it is old server, continue", K(region_name), K(ret));
        ret = OB_SUCCESS;
        region_name.assign_ptr(DEFAULT_REGION_NAME, static_cast<int>(STRLEN(DEFAULT_REGION_NAME)));
      }
    }

    if (OB_SUCC(ret)) {
      PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(result_handler, "spare4", idc_name); // spare4 is idc name
      if ((OB_ERR_COLUMN_NOT_FOUND == ret) || (OB_SUCC(ret) && idc_name.empty())) {
        LOG_DEBUG("idc_name is unkonwn, maybe it is old server, continue", K(zone_name), K(idc_name), K(ret));
        ret = OB_SUCCESS;
        idc_name.assign_ptr(DEFAULT_PROXY_IDC_NAME, static_cast<int>(STRLEN(DEFAULT_PROXY_IDC_NAME)));
      }
    }

    if (OB_SUCC(ret)) {
      // use spare 5 as zone type
      PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(result_handler, "spare5", zone_type);
      if ((OB_ERR_COLUMN_NOT_FOUND == ret) || (OB_SUCC(ret) && zone_type.empty())) {
        LOG_DEBUG("zone_type is unkonwn, maybe it is old server", K(zone_name), K(zone_type), K(ret));
        ret = OB_SUCCESS;
        zone_state.zone_type_ = ZONE_TYPE_READWRITE;
      } else {
        zone_state.zone_type_ = str_to_zone_type(zone_type.ptr());
        if (ZONE_TYPE_INVALID == zone_state.zone_type_) {
          LOG_INFO("zone_type is unkonwn, maybe it is new type, treate it as ReadWrite", K(zone_name), K(zone_type), K(ret));
          zone_state.zone_type_ = ZONE_TYPE_READWRITE;
        }
      }
    }

    if (OB_SUCC(ret)) {
      zone_state.zone_status_ = ObZoneStatus::get_status(zone_status);
      if (OB_FAIL(zone_state.set_zone_name(zone_name))) {
        LOG_WDIAG("fail to set zone name", K(zone_name), K(ret));
      } else if (OB_FAIL(zone_state.set_region_name(region_name))) {
        LOG_WDIAG("fail to set region name", K(region_name), K(ret));
      } else if (OB_FAIL(zone_state.set_idc_name(idc_name))) {
        LOG_WDIAG("fail to set idc name", K(region_name), K(ret));
      } else if (OB_FAIL(zone_info.push_back(zone_state))) {
        LOG_WDIAG("fail to push back zone_info", K(zone_state), K(ret));
      } else {
        // do nothing
      }
    }
  }

  if (ret != OB_ITER_END) {
    LOG_WDIAG("fail to get zone state info", K(ret));
  } else {
    ret = OB_SUCCESS;
    LOG_DEBUG("get zone state info succ", K(zone_info));
  }

  return ret;
}

int ObServerStateRefreshUtils::get_server_state_info(
    ObMysqlResultHandler &result_handler,
    ObIArray<ObZoneStateInfo> &zones_state,
    ObIArray<ObServerStateInfo> &servers_state,
    bool &has_invalid_server)
{
  int ret = OB_SUCCESS;

  char ip_str[MAX_IP_ADDR_LENGTH];
  int64_t port = 0;
  ObServerStateInfo server_state;
  const int64_t MAX_DISPLAY_STATUS_LEN = 64;
  char display_status_str[MAX_DISPLAY_STATUS_LEN];
  char zone_name[MAX_ZONE_LENGTH + 1];
  int64_t zone_name_len = 0;
  int64_t tmp_real_str_len = 0;
  ObString zone_name_str;
  has_invalid_server = false;
  while (OB_SUCC(ret) && OB_SUCC(result_handler.next())) {
    server_state.reset();
    ip_str[0] = '\0';
    display_status_str[0] = '\0';
    zone_name[0] = '\0';
    PROXY_EXTRACT_STRBUF_FIELD_MYSQL(result_handler, "svr_ip", ip_str,
                                     MAX_IP_ADDR_LENGTH, tmp_real_str_len);
    PROXY_EXTRACT_INT_FIELD_MYSQL(result_handler, "svr_port", port, int64_t);
    PROXY_EXTRACT_STRBUF_FIELD_MYSQL(result_handler, "zone", zone_name,
                                     MAX_ZONE_LENGTH + 1, zone_name_len);
    PROXY_EXTRACT_STRBUF_FIELD_MYSQL(result_handler, "status", display_status_str,
                                     MAX_DISPLAY_STATUS_LEN, tmp_real_str_len);
    PROXY_EXTRACT_INT_FIELD_MYSQL(result_handler, "start_service_time",
                                  server_state.start_service_time_, int64_t);
    PROXY_EXTRACT_INT_FIELD_MYSQL(result_handler, "stop_time", server_state.stop_time_, int64_t);
    if (OB_SUCC(ret)) {
      zone_name_str = ObString(zone_name_len, zone_name);
      server_state.server_status_ = ObServerStatus::OB_DISPLAY_MAX;
      if (OB_FAIL(ObServerStatus::str2display_status(
          display_status_str, server_state.server_status_))) {
        LOG_WDIAG("display string to status failed", K(ret), K(display_status_str));
      } else if ((server_state.server_status_ < 0)
                 || (server_state.server_status_ >= ObServerStatus::OB_DISPLAY_MAX)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("invalid display status", "server_status", server_state.server_status_, K(ret));
      } else if (OB_FAIL(server_state.add_addr(ip_str, port))) {
        LOG_WDIAG("fail to add addr", K(ip_str), K(port), K(ret));
        has_invalid_server = true;
        //if svr_ip or svr_port in __all_virtual_proxy_server_stat is wrong,
        //we can skip over this server_state.
        ret = OB_SUCCESS;
        continue;
      } else if (OB_ISNULL(server_state.zone_state_ = get_zone_info_ptr(zones_state, zone_name_str))) {
        LOG_INFO("this server can not find it's zone, maybe it's zone has been "
                 "deleted, so treat this server as deleted also", K(server_state),
                 K(zone_name_str), K(zones_state));
        ret = OB_SUCCESS;
        continue;
      } else {
        if (server_state.zone_state_->is_readonly_zone()) {
          server_state.replica_.replica_type_ = REPLICA_TYPE_READONLY;
        } else if (server_state.zone_state_->is_encryption_zone()) {
          server_state.replica_.replica_type_ = REPLICA_TYPE_ENCRYPTION_LOGONLY;
        } else {
          server_state.replica_.replica_type_ = REPLICA_TYPE_FULL;
        }
        if (OB_FAIL(servers_state.push_back(server_state))) {
          LOG_WDIAG("fail to push back server_state", K(server_state), K(ret));
        }
      }
    }
  }

  if (OB_ITER_END != ret) {
    LOG_WDIAG("fail to get server state info", K(ret));
  } else {
    LOG_DEBUG("get server state info succ", K(servers_state));
    ret = OB_SUCCESS;
  }

  return ret;
}

ObZoneStateInfo *ObServerStateRefreshUtils::get_zone_info_ptr(
    ObIArray<ObZoneStateInfo> &zones_state,
    const ObString &zone_name)
{
  ObZoneStateInfo *tmp_zone_info = NULL;
  ObString orig_zname;
  for (int64_t i = 0; ((i < zones_state.count()) && (NULL == tmp_zone_info)); ++i) {
    orig_zname = zones_state.at(i).zone_name_;
    if (zone_name == orig_zname) {
      tmp_zone_info = &zones_state.at(i);
    }
  }
  return tmp_zone_info;
}

uint64_t ObServerStateRefreshUtils::get_zones_state_hash(ObIArray<ObZoneStateInfo> &zones_state)
{
  uint64_t hash = 0;
  if (!zones_state.empty()) {
    void *data = &zones_state.at(0);
    int32_t len = static_cast<int32_t>(zones_state.count() * sizeof(ObZoneStateInfo));
    hash = murmurhash(data, len, 0);
  }
  return hash;
}

uint64_t ObServerStateRefreshUtils::get_servers_state_hash(ObIArray<ObServerStateInfo> &servers_state)
{
  uint64_t hash = 0;
  void *data = NULL;
  int32_t len = 0;
  for (int64_t i = 0; i < servers_state.count(); ++i) {
    data = &servers_state.at(i);
    // skip zone_state_ at the end
    len = static_cast<int32_t>(sizeof(ObServerStateInfo) - sizeof(ObZoneStateInfo *));
    hash += murmurhash(data, len, 0);
  }
  return hash;
}

int ObServerStateRefreshUtils::order_servers_state(const ObIArray<ObServerStateInfo> &servers_state, LocationList &server_list)
{
  int ret = OB_SUCCESS;
  const int64_t server_count = servers_state.count();
  ObSEArray<int64_t, ObServerStateRefreshCont::DEFAULT_ZONE_COUNT> first_idx_in_zone;//save first svr_idx_idx in zone
  ObString last_zone_name;
  const ObServerStateInfo *server_info = NULL;

  // 1. fill first_idx_in_zone
  for (int64_t i = 0; i < server_count && OB_SUCC(ret); ++i) {
    server_info = &servers_state.at(i);
    if (last_zone_name != server_info->zone_state_->zone_name_) {
      // save first idx in zone
      if (OB_FAIL(first_idx_in_zone.push_back(i))) {
        LOG_WDIAG("fail to push back first_idx_in_zone", K(i), K(ret));
      } else {
        last_zone_name = server_info->zone_state_->zone_name_;
      }
    }//end of different zone
  }//end of for

  if (OB_SUCC(ret)) {
    //virtual invalid idx, just used for compute svr_count_in_zone
    if (OB_FAIL(first_idx_in_zone.push_back(server_count))) {
      LOG_WDIAG("fail to push back first_idx_in_zone", K(server_count), K(ret));
    }
  }

  LOG_DEBUG("current info", K(servers_state), K(server_count), K(first_idx_in_zone), K(ret));

  // 2. order servers
  if (OB_SUCC(ret)
      && OB_LIKELY(server_count > 0)
      && OB_LIKELY(first_idx_in_zone.count() > 1)) {
    const int64_t zone_count = first_idx_in_zone.count() - 1;//NOTE:: the last one is virtual invalid;
    const int64_t replica_count = zone_count;
    const int64_t partition_count = (server_count / replica_count + (0 == server_count % replica_count ? 0 : 1));
    ObSEArray<int64_t, ObServerStateRefreshCont::DEFAULT_ZONE_COUNT + 1> unused_server_count;
    int64_t svr_count_in_zone = 0;

    for (int64_t i = 0; i < zone_count && OB_SUCC(ret); ++i) {
      if (OB_UNLIKELY(0 >= (svr_count_in_zone = first_idx_in_zone.at(i + 1) - first_idx_in_zone.at(i)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("it should not happened", K(first_idx_in_zone), K(svr_count_in_zone), K(ret));
      } else if (OB_FAIL(unused_server_count.push_back(svr_count_in_zone))) {
        LOG_WDIAG("fail to push back unused_server_count", K(i), K(svr_count_in_zone), K(ret));
      } else {/*do nothing*/}
    }
    if (OB_SUCC(ret)) {
      // random to pick zone
      int64_t init_idx = (get_hrtime() + *(static_cast<const int64_t *>(&server_count))) % server_count;
      int64_t finish_count = 0;
      int64_t svr_idx = 0;
      LOG_DEBUG("begin to extract rs list", K(init_idx));
      while (finish_count < server_count && OB_SUCC(ret)) {
        ++init_idx;
        for (int64_t i = 0; i < zone_count && OB_SUCC(ret); ++i) {
          if (unused_server_count.at(i) > 0) {
            svr_count_in_zone = first_idx_in_zone.at(i + 1) - first_idx_in_zone.at(i);
            svr_idx = first_idx_in_zone.at(i) + (init_idx + i) % svr_count_in_zone;
            if (OB_FAIL(server_list.push_back(servers_state.at(svr_idx).replica_))) {
              LOG_WDIAG("fail to push back server_list", "replica", servers_state.at(svr_idx).replica_,
                       K(i), K(unused_server_count), K(ret));
            } else {
              ++finish_count;
              --unused_server_count.at(i);
            }
          } else {
            LOG_DEBUG("no need try", K(i), K(unused_server_count));
          } //end of unused_server_count
        } //end of for zone_count
      } //end of while server_count

      //make the first one leader
      if (OB_SUCC(ret)) {
        for (int64_t i = 0; i < partition_count && OB_SUCC(ret); ++i) {
          svr_idx = i * replica_count;
          server_list.at(svr_idx).role_ = LEADER;
        }
        LOG_DEBUG("succ to get rs list", K(server_list), K(partition_count), K(replica_count));
      }
    }
  } //end of order servers

  return ret;
}

int ObDetectServerStateCont::init(ObClusterResource *cr, int64_t server_detect_refresh_interval_us)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(server_detect_refresh_interval_us <= 0 || NULL == cr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(server_detect_refresh_interval_us), K(ret));
  } else if (OB_ISNULL(mutex_ = new_proxy_mutex())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to allocate mutex", K(ret));
  } else {
    cr->inc_ref();
    cluster_resource_ = cr;
    server_detect_state_interval_us_ = server_detect_refresh_interval_us;
    is_inited_ = true;
  }
  return ret;
}

void ObDetectServerStateCont::kill_this()
{
  if (is_inited_) {
    LOG_INFO("ObDetectServerStateCont will kill self", KPC(this));
    int ret = OB_SUCCESS;
    // cancel pending action at first
    // ignore ret
    if (OB_FAIL(cancel_pending_action())) {
      LOG_WDIAG("fail to cancel pending action", K(ret));
    }

    if (OB_LIKELY(NULL != cluster_resource_)) {
      cluster_resource_->dec_ref();
      cluster_resource_ = NULL;
    }

    server_detect_state_interval_us_ = 0;
    kill_this_ = false;
    is_inited_ = false;
  }
  mutex_.release();
  op_free(this);
}

DEF_TO_STRING(ObDetectServerStateCont)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(is_inited), K_(kill_this), K_(server_detect_state_interval_us), KP_(pending_action), KPC_(cluster_resource));
  J_OBJ_END();
  return pos;
}

int ObDetectServerStateCont::schedule_detect_server_state()
{
  int ret = OB_SUCCESS;
  const int64_t server_detect_mode = get_global_proxy_config().server_detect_mode;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else if (get_global_hot_upgrade_info().is_graceful_exit_timeout(get_hrtime())) {
    ret = OB_SERVER_IS_STOPPING;
    LOG_WDIAG("proxy need exit now", K(ret));
  } else if (OB_UNLIKELY(!self_ethread().is_event_thread_type(ET_CALL))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("server state refresh cont must be scheduled in work thread", K(ret));
  } else if (server_detect_mode > 0) {
    int64_t ss_version = cluster_resource_->server_state_version_;
    common::DRWLock &server_state_lock = cluster_resource_->get_server_state_lock(ss_version);
    server_state_lock.rdlock();
    common::ObIArray<ObServerStateSimpleInfo> &server_state_info =
        cluster_resource_->get_server_state_info(ss_version);
    ObHashSet<ObAddr> addr_set;
    addr_set.create(4);

    for (int i = 0; i < server_state_info.count(); i++) {
      ObServerStateSimpleInfo &info = server_state_info.at(i);
      bool check_pass = true;
      // Accurate detection method
      if (1 == server_detect_mode) {
        check_pass = (info.request_sql_cnt_ > 0);
        LOG_DEBUG("check detect one server", K(server_detect_mode), K_(info.request_sql_cnt), K(check_pass));
      }

      if (check_pass || info.detect_fail_cnt_ > 0) {
        // There is no need to judge whether the push_back is successful here
        ObDetectOneServerStateCont *cont = NULL;
        if (OB_ISNULL(cont = op_alloc(ObDetectOneServerStateCont))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WDIAG("fail to alloc ObDetectOneServerStateCont", K(ret));
        } else if (OB_FAIL(cont->init(cluster_resource_, info.addr_))) {
          LOG_WDIAG("fail to init detect server state cont", K(ret));
        } else if (OB_ISNULL(self_ethread().schedule_imm(cont, DETECT_SERVER_STATE_EVENT))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("schedule detect one server state failed", K(ret));
        }
        LOG_DEBUG("schedule detect one server state", K(info), K(ss_version));
      } else {
        ObIpEndpoint point(info.addr_.get_sockaddr());
        if (OB_HASH_EXIST == get_global_resource_pool_processor().ip_set_.exist_refactored(point)) {
          addr_set.set_refactored(info.addr_);
        } else if (OB_HASH_EXIST == cluster_resource_->alive_addr_set_.exist_refactored(point)) {
          addr_set.set_refactored(info.addr_);
        }
      }
    }

    ObHashSet<ObAddr>::iterator it = addr_set.begin();
    while (it != addr_set.end()) {
      ObAddr &addr = it->first;
      ObDetectOneServerStateCont *cont = NULL;
      if (OB_ISNULL(cont = op_alloc(ObDetectOneServerStateCont))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to alloc ObDetectOneServerStateCont", K(ret));
      } else if (OB_FAIL(cont->init(cluster_resource_, addr))) {
        LOG_WDIAG("fail to init detect server cont", K(ret));
      } else if (OB_ISNULL(self_ethread().schedule_imm(cont, DETECT_SERVER_STATE_EVENT))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("schedule detect one server state failed", K(ret));
      }
      it++;
      LOG_DEBUG("schedule detect one server state", K(addr), K(ss_version));
    }
    server_state_lock.rdunlock();
  }

  if (OB_UNLIKELY(server_detect_state_interval_us_ <= 0)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WDIAG("delay must greater than zero", K_(server_detect_state_interval_us), K(ret));
  } else if (OB_ISNULL(pending_action_ = self_ethread().schedule_in(this,
              HRTIME_USECONDS(server_detect_state_interval_us_), DETECT_SERVER_STATE_EVENT))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to schedule refresh server state", K_(server_detect_state_interval_us),
              K(server_detect_mode), KPC_(cluster_resource), K(ret));
  }

  LOG_DEBUG("schedule detect server state", K_(server_detect_state_interval_us),
             K(server_detect_mode), KPC_(cluster_resource), K(ret));

  return ret;
}

int ObDetectServerStateCont::main_handler(int event, void *data)
{
  UNUSED(data);
  int event_ret = EVENT_CONT;
  int ret = OB_SUCCESS;
  LOG_DEBUG("[ObDetectServerStateCont::main_handler] ", K(event), K(kill_this_));

  switch (event) {
    case DESTROY_SERVER_STATE_EVENT: {
      if (0 == ATOMIC_CAS(&set_interval_task_count_, 0, 0)) {
        LOG_INFO("ObDetectServerStateCont will terminate", KPC_(cluster_resource));
        kill_this_ = true;
      } else {
        LOG_INFO("there are still set_interval tasks which have been scheduled,"
            " we should reschedule destroy event", KPC(this));
        if (OB_ISNULL(self_ethread().schedule_imm(this, DESTROY_SERVER_STATE_EVENT))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("fail to schedule DESTROY_SERVER_STATE_EVENT", KPC(this), K(ret));
        }
      }
      break;
    }
    case EVENT_IMMEDIATE: {
      ATOMIC_DEC(&set_interval_task_count_);
      if (OB_FAIL(cancel_pending_action())) {
        LOG_WDIAG("cancel pending action failed", K(ret));
      } else if (OB_FAIL(schedule_detect_server_state())) {
        LOG_WDIAG("fail to schedule detect server state", K(ret));
      }
      break;
    }
    case DETECT_SERVER_STATE_EVENT: {
      pending_action_ = NULL;
      if (OB_FAIL(schedule_detect_server_state())) {
        LOG_WDIAG("fail to schedule detect server state", K(ret));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unknown event", K(event), K(ret));
      break;
    }
  }

  if (kill_this_) {
    event_ret = EVENT_DONE;
    kill_this();
  }

  return event_ret;
}

int ObDetectServerStateCont::cancel_pending_action()
{
  int ret = OB_SUCCESS;
  if (NULL != pending_action_) {
    if (OB_FAIL(pending_action_->cancel())) {
      LOG_WDIAG("fail to cancel pending action", K_(pending_action), K(ret));
    } else {
      pending_action_ = NULL;
    }
  }

  return ret;
}

int ObDetectServerStateCont::set_detect_server_state_interval(const int64_t refresh_interval)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(refresh_interval <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid detect server state interval", K(refresh_interval), K(ret));
  } else {
    server_detect_state_interval_us_ = refresh_interval;
    ATOMIC_INC(&set_interval_task_count_);
    if (OB_ISNULL(g_event_processor.schedule_imm(this, ET_CALL))) {
      ATOMIC_DEC(&set_interval_task_count_);
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("schedule imm detect_server_state task error", K(ret));
    }

  }
  LOG_DEBUG("set detect server state interval", K_(server_detect_state_interval_us), K(refresh_interval));

  return ret;
}

ObDetectOneServerStateCont::ObDetectOneServerStateCont()
      : ObAsyncCommonTask(NULL, "detect_server_state_task"),
        cluster_resource_(NULL), addr_(), mysql_client_(NULL),
        mysql_proxy_()
{
  SET_HANDLER(&ObDetectOneServerStateCont::main_handler);
}

int ObDetectOneServerStateCont::init(ObClusterResource *cluster_resource, ObAddr addr)
{
  int ret = OB_SUCCESS;
  const int64_t timeout_ms = usec_to_msec(get_global_proxy_config().detect_server_timeout);
  if (OB_UNLIKELY(NULL == cluster_resource || !addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("init obdetectserverstate cont failed", K(cluster_resource), K(ret));
    // The detection only depends on whether OBServer returns OB_MYSQL_COM_HANDSHAKE,
    // and will not log in. The following parameters will not be actually used,
    // only for the initialization of class objects
    // user_name : detect_username
    // password : detect_password
    // database : detect_database
  } else if (OB_FAIL(mysql_proxy_.init(timeout_ms, ObProxyTableInfo::DETECT_USERNAME_USER, "detect_password", "detect_database"))) {
    LOG_WDIAG("fail to init mysql proxy", K(ret));
  } else {
    cluster_resource->inc_ref();
    cluster_resource_ = cluster_resource;
    addr_ = addr;
    if (OB_ISNULL(mysql_client_ = op_alloc(ObMysqlClient))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to allocate ObMysqlClient", K(ret));
    } else if (OB_FAIL(mysql_client_->init_detect_client(cluster_resource_))) {
      LOG_WDIAG("fail to init detect client", K(ret));
    }

    if (OB_FAIL(ret) && (NULL != mysql_client_)) {
      mysql_client_->kill_this();
      mysql_client_ = NULL;
    }
  }

  return ret;
}

void ObDetectOneServerStateCont::kill_this()
{
  int ret = OB_SUCCESS;

  if (OB_LIKELY(NULL != cluster_resource_)) {
    cluster_resource_->dec_ref();
    cluster_resource_ = NULL;
  }
  if (OB_LIKELY(NULL != mysql_client_)) {
    mysql_client_->kill_this();
    mysql_client_ = NULL;
  }
  if (OB_FAIL(cancel_timeout_action())) {
    LOG_WDIAG("fail to cancel timeout action", K(ret));
  }

  if (OB_FAIL(cancel_pending_action())) {
    LOG_WDIAG("fail to cancel pending action", K(ret));
  }

  action_.set_continuation(NULL);
  op_free(this);
}

int ObDetectOneServerStateCont::main_handler(int event, void *data)
{
  int he_ret = EVENT_CONT;
  int ret = OB_SUCCESS;

  switch (event) {
    case EVENT_IMMEDIATE:
    case DETECT_SERVER_STATE_EVENT: {
      if (OB_FAIL(detect_server_state_by_sql())) {
        LOG_WDIAG("detect server by sql failed", K(ret));
      }
      break;
    }
    case CLIENT_TRANSPORT_MYSQL_RESP_EVENT: {
      if (OB_FAIL(handle_client_resp(data))) {
        LOG_WDIAG("fail to handle client resp", K(ret));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unknown event", K(event), K(data), K(ret));
      break;
    }
  }

  if (OB_FAIL(ret) || terminate_) {
    terminate_ = true;
    kill_this();
    he_ret = EVENT_DONE;
  }

  return he_ret;
}

int ObDetectOneServerStateCont::detect_server_state_by_sql()
{
  int ret = OB_SUCCESS;
  char sql[] = "select 'detect server' from dual";
  LOG_DEBUG("begin to detect server", K_(addr));
  ObMysqlRequestParam request_param(sql);
  request_param.set_target_addr(addr_);
  request_param.set_mysql_client(mysql_client_);
  request_param.set_client_vc_type(ObMysqlRequestParam::CLIENT_VC_TYPE_DETECT);
  const int64_t timeout_ms = usec_to_msec(get_global_proxy_config().detect_server_timeout);
  if (OB_FAIL(mysql_proxy_.async_read(this, request_param, pending_action_, timeout_ms))) {
    LOG_WDIAG("fail to async read", K(ret));
  }

  return ret;
}

int ObDetectOneServerStateCont::handle_client_resp(void *data)
{
  int ret = OB_SUCCESS;
  common::DRWLock &server_state_lock1 = cluster_resource_->get_server_state_lock(0);
  common::DRWLock &server_state_lock2 = cluster_resource_->get_server_state_lock(1);
  server_state_lock1.rdlock();
  server_state_lock2.rdlock();
  common::ObIArray<ObServerStateSimpleInfo> &server_state_info =
       cluster_resource_->get_server_state_info(cluster_resource_->server_state_version_);
  bool found = false;
  for (int i = 0; !found && i < server_state_info.count(); i++) {
    ObServerStateSimpleInfo &info = server_state_info.at(i);
    ObIpEndpoint ip(info.addr_.get_sockaddr());
    ObCongestionEntry::ObServerState state = ObCongestionEntry::ObServerState::ACTIVE;
    if (addr_ == info.addr_) {
      if (NULL != data) {
        (void)ATOMIC_SET(&info.detect_fail_cnt_, 0);
        state = ObCongestionEntry::ObServerState::DETECT_ALIVE;
        cluster_resource_->alive_addr_set_.erase_refactored(ip);
        get_global_resource_pool_processor().ip_set_.erase_refactored(ip);
        LOG_DEBUG("detect server alive", K(info));
      } else {
        int64_t fail_cnt = ATOMIC_AAF(&info.detect_fail_cnt_, 1);
        LOG_WDIAG("detect server dead", K(info));
        if (fail_cnt >= get_global_proxy_config().server_detect_fail_threshold) {
          // If the detection failure exceeds the number of retries, the server needs to be added to the blacklist
          (void)ATOMIC_SET(&info.detect_fail_cnt_, 0);
          state = ObCongestionEntry::ObServerState::DETECT_DEAD;
          get_global_resource_pool_processor().ip_set_.set_refactored(ip);
        }
      }

      if (ObCongestionEntry::ObServerState::DETECT_DEAD == state
          || ObCongestionEntry::ObServerState::DETECT_ALIVE == state) {
        ObCongestionManager &congestion_manager = cluster_resource_->congestion_manager_;
        bool is_init = !congestion_manager.is_base_servers_added();
        int64_t cr_version = cluster_resource_->version_;
        if (OB_FAIL(congestion_manager.update_server(ip, cr_version, state,
                info.zone_name_, info.region_name_, is_init))) {
          LOG_WDIAG("fail to update update server", K(cr_version), K(ip),  K(is_init), K(ret));
        }
      }
      found = true;
    }
  }

  if (!found) {
    ObIpEndpoint ip(addr_.get_sockaddr());
    get_global_resource_pool_processor().ip_set_.erase_refactored(ip);
    cluster_resource_->alive_addr_set_.erase_refactored(ip);
    LOG_WDIAG("server not in cluster", K(ip), KPC_(cluster_resource));
  }
  server_state_lock2.rdunlock();
  server_state_lock1.rdunlock();
  if (NULL != data) {
    ObClientMysqlResp *resp = reinterpret_cast<ObClientMysqlResp *>(data);
    op_free(resp);
    data = NULL;
  }
  terminate_ = true;

  return ret;
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
