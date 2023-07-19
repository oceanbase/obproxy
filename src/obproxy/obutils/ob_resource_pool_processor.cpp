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
#include "obutils/ob_resource_pool_processor.h"
#include "utils/ob_proxy_utils.h"
#include "iocore/eventsystem/ob_continuation.h"
#include "stat/ob_resource_pool_stats.h"
#include "obutils/ob_config_server_processor.h"
#include "obutils/ob_proxy_config.h"
#include "obutils/ob_metadb_create_cont.h"
#include "proxy/route/ob_table_cache.h"
#include "proxy/route/ob_route_utils.h"
#include "proxy/route/ob_cache_cleaner.h"
#include "proxy/mysqllib/ob_session_field_mgr.h"
#include "proxy/mysqllib/ob_proxy_auth_parser.h"
#include "proxy/client/ob_mysql_proxy.h"
#include "obutils/ob_async_common_task.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/encrypt/ob_encrypted_helper.h"

using namespace obsys;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::share;
using namespace oceanbase::obproxy;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
ObResourcePoolProcessor g_rp_processor;

const static char *CHECK_VERSION_SQL = "SELECT ob_version() AS cluster_version";
const static char *CHEK_CLUSTER_INFO_SQL    =
    "SELECT /*+READ_CONSISTENCY(WEAK)*/ cluster_role, cluster_status FROM oceanbase.%s LIMIT 1";
const static char *OBPROXY_V_DATABASE_TNAME = "v$ob_cluster";
const static char *INIT_SS_INFO_SQL         =
    "SELECT /*+READ_CONSISTENCY(WEAK)*/ *, zs.status AS zone_status, ss.status AS server_status "
    "FROM oceanbase.%s ss left join oceanbase.%s zs "
    "ON zs.zone = ss.zone "
    "WHERE ss.svr_port > 0 ORDER BY ss.zone LIMIT %ld;";
const static char *INIT_SS_INFO_SQL_V4 =
    "SELECT /*READ_CONSISTENCY(WEAK)*/ parameters.value as cluster, zs.zone AS zone, zs.status AS zone_status, ss.status AS server_status, "
    "zs.region AS region, zs.idc AS spare4, zs.type AS spare5, ss.svr_ip AS svr_ip, ss.sql_port AS svr_port, "
    "ss.start_service_time is not null AS start_service_time, ss.stop_time is not null as stop_time "
    "FROM oceanbase.%s ss left join oceanbase.%s zs "
    "ON zs.zone = ss.zone join V$OB_PARAMETERS parameters "
    "WHERE ss.svr_port > 0 and parameters.name = 'cluster' ORDER BY ss.zone LIMIT %ld;";

const static char *PRIMARY_ROLE             = "PRIMARY";
const static char *ROLE_VALID               = "VALID";

//-------ObClusterInfoKey------
DEF_TO_STRING(ObClusterInfoKey)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(cluster_name), K_(cluster_id));
  J_OBJ_END();
  return pos;
}

//-------ObClusterDeleteCont------
class ObClusterDeleteCont : public ObAsyncCommonTask
{
public:
  virtual ~ObClusterDeleteCont() {}

  virtual int main_handler(int event, void *data);
  static  int alloc(const ObString &cluster_name, const int64_t cluster_id, ObClusterDeleteCont *&cont);

private:
  explicit ObClusterDeleteCont(ObProxyMutex *m) : ObAsyncCommonTask(m, "delete cluster task"),
                                                  cluster_id_(OB_INVALID_CLUSTER_ID),
                                                  cluster_name_() {}
  int set_cluster_info(const ObString &cluster_name, const int64_t cluster_id);

private:
  int64_t cluster_id_;
  ObString cluster_name_;
  char cluster_name_str_[OB_MAX_USER_NAME_LENGTH_STORE];
  DISALLOW_COPY_AND_ASSIGN(ObClusterDeleteCont);
};

int ObClusterDeleteCont::alloc(const ObString &cluster_name, const int64_t cluster_id, ObClusterDeleteCont *&cont)
{
  int ret = OB_SUCCESS;
  ObProxyMutex *mutex = NULL;
  cont = NULL;
  if (OB_ISNULL(mutex = new_proxy_mutex())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to allocate mem for mutex", K(ret));
  } else if (OB_ISNULL(cont = new(std::nothrow) ObClusterDeleteCont(mutex))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to allocate mem for ObClusterDeleteCont", K(ret));
    if (OB_LIKELY(NULL != mutex)) {
      mutex->free();
      mutex = NULL;
    }
  } else if (OB_FAIL(cont->set_cluster_info(cluster_name, cluster_id))) {
    LOG_WARN("fail to set cluster name", K(cluster_name), K(cluster_id), K(ret));
  }
  if (OB_FAIL(ret) && OB_LIKELY(NULL != cont)) {
    cont->destroy();
    cont = NULL;
  }
  return ret;
}

int ObClusterDeleteCont::set_cluster_info(const ObString &cluster_name, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cluster_name.length() > OB_MAX_USER_NAME_LENGTH_STORE)
      || OB_UNLIKELY(cluster_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(cluster_name), K(ret));
  } else {
    MEMCPY(cluster_name_str_, cluster_name.ptr(), cluster_name.length());
    cluster_name_.assign_ptr(cluster_name_str_, cluster_name.length());
    cluster_id_ = cluster_id;
  }
  return ret;
}

int ObClusterDeleteCont::main_handler(int event, void *data)
{
  UNUSED(data);
  if (OB_LIKELY(EVENT_IMMEDIATE == event)) {
    int ret = OB_SUCCESS;
    if (cluster_name_ == OB_META_DB_CLUSTER_NAME) {
      if (OB_FAIL(get_global_resource_pool_processor().rebuild_metadb())) {
        LOG_WARN("fail to rebuild metadb", K(ret));
      }
    } else {
      if (OB_FAIL(get_global_resource_pool_processor().delete_cluster_resource(cluster_name_, cluster_id_))) {
        LOG_WARN("fail to delete cluster resource", K_(cluster_name), K_(cluster_id), K(ret));
      }
    }
  }
  destroy();
  return EVENT_DONE;
}

//---------------------ObRslistFetchCont---------------------//
void ObRslistFetchCont::destroy()
{
  (void)ATOMIC_FAA(&cr_->fetch_rslist_task_count_, -1);
  if (NULL != cr_) {
    // inc_ref() in add_async_task()
    cr_->dec_ref();
    cr_ = NULL;
  }
  ObAsyncCommonTask::destroy();
}

int ObRslistFetchCont::init_task()
{
  int ret = OB_SUCCESS;

  ObConfigServerProcessor &cs_processor = get_global_config_server_processor();

  if (OB_LIKELY(get_global_proxy_config().with_config_server_)) {
    ObSEArray<ObAddr, 5> rs_list;
    if (OB_FAIL(cs_processor.get_newest_cluster_rs_list(cr_->get_cluster_name(),
                                                        cr_->get_cluster_id(), rs_list, need_update_dummy_entry_))) {
      LOG_WARN("fail to get cluster rslist", K_(cr_->cluster_info_key), K(ret));
    } else {
      fetch_result_ = true;
    }
  }

  if (!fetch_result_) {
    // If rstlist is started, do not modify the rslist_hash value
    bool need_save_rslist_hash = get_global_proxy_config().with_config_server_;
    if (OB_FAIL(cs_processor.swap_origin_web_rslist_and_build_sys(cr_->get_cluster_name(), cr_->get_cluster_id(), need_save_rslist_hash))) {
      LOG_WARN("fail to swap origin web rslist", K_(cr_->cluster_info_key), K(ret));
    } else {
      fetch_result_ = true;
    }
  }

  LOG_DEBUG("finish to ObRslistFetchCont", K_(cr_->cluster_info_key), K(ret));

  need_callback_ = true;
  return ret;
}

//---------------------ObIDCListFetchCont---------------------//
void ObIDCListFetchCont::destroy()
{
  (void)ATOMIC_FAA(&cr_->fetch_idc_list_task_count_, -1);
  if (NULL != cr_) {
    // inc_ref() in add_async_task()
    cr_->dec_ref();
    cr_ = NULL;
  }
  ObAsyncCommonTask::destroy();
}

int ObIDCListFetchCont::init_task()
{
  int ret = OB_SUCCESS;
  ObProxyIDCList idc_list;
  ObConfigServerProcessor &cs_processor = get_global_config_server_processor();
  if (OB_FAIL(cs_processor.refresh_idc_list(cr_->get_cluster_name(), cr_->get_cluster_id(), idc_list))) {
    LOG_INFO("fail to refresh_idc_list", "cluster_name", cr_->get_cluster_name(),
             "cluster_id", cr_->get_cluster_id(), K(ret));
    fetch_result_ = true;
  } else {
    fetch_result_ = true;
  }
  need_callback_ = true;
  return ret;
}

//---------------------ObCheckVersionCont-------------------------//
class ObCheckVersionCont : public ObAsyncCommonTask
{
public:
  ObCheckVersionCont(ObClusterResource *cr, const int64_t cluster_id,
                       ObContinuation *cb_cont, ObEThread *submit_thread)
    : ObAsyncCommonTask(cb_cont->mutex_, "cluster_role_check_task", cb_cont, submit_thread),
      check_result_(false), cr_(cr), cluster_id_(cluster_id) {}
  virtual ~ObCheckVersionCont() {}

  virtual void destroy();
  virtual int init_task();
  virtual int finish_task(void *data);
  virtual void *get_callback_data();

private:
  bool check_result_;
  ObClusterResource *cr_;
  int64_t cluster_id_;
  DISALLOW_COPY_AND_ASSIGN(ObCheckVersionCont);
};

void ObCheckVersionCont::destroy()
{
  if (NULL != cr_) {
    // inc_ref() in add_async_task()
    cr_->dec_ref();
    cr_ = NULL;
  }
  ObAsyncCommonTask::destroy();
}

int ObCheckVersionCont::init_task()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cr_->mysql_proxy_.async_read(this, CHECK_VERSION_SQL, pending_action_))) {
    LOG_WARN("fail to async read", K(ret));
  } else if (OB_ISNULL(pending_action_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pending action can not be NULL", K_(pending_action), K(ret));
  }

  return ret;
}

int ObCheckVersionCont::finish_task(void *data)
{
  int ret = OB_SUCCESS;
  const ObString &cluster_name = cr_->cluster_info_key_.cluster_name_.config_string_;
  if (OB_ISNULL(data)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data is null, fail to check clsuter version", K(cluster_name), K(ret));
  } else {
    ObClientMysqlResp *resp = reinterpret_cast<ObClientMysqlResp *>(data);
    ObMysqlResultHandler handler;
    handler.set_resp(resp);
    if (OB_FAIL(handler.next())) {
      check_result_ = true;
      ret = OB_SUCCESS;
    } else {
      ObString cluster_version;
      PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(handler, "cluster_version", cluster_version);
      int64_t version = 0;
      for (int64_t i = 0; i < cluster_version.length() && cluster_version[i] != '.'; i++) {
        version = version * 10 + cluster_version[i] - '0';
      }

      cr_->cluster_version_ = version;

      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(OB_ITER_END != handler.next())) { // check if this is only one
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get cluster version, there is more than one record", K(ret));
        } else {
          check_result_ = true;
        }
      }
    }
  }

  return ret;
}

inline void *ObCheckVersionCont::get_callback_data()
{
  return static_cast<void *>(&check_result_);
}

//---------------------ObClusterCheckRoleCont---------------------//
class ObClusterInfoCheckCont : public ObAsyncCommonTask
{
public:
  ObClusterInfoCheckCont(ObClusterResource *cr, const int64_t cluster_id,
                       ObContinuation *cb_cont, ObEThread *submit_thread)
    : ObAsyncCommonTask(cb_cont->mutex_, "cluster_role_check_task", cb_cont, submit_thread),
      check_result_(false), cr_(cr), cluster_id_(cluster_id) {}
  virtual ~ObClusterInfoCheckCont() {}

  virtual void destroy();
  virtual int init_task();
  virtual int finish_task(void *data);
  virtual void *get_callback_data();

private:
  bool check_result_;
  ObClusterResource *cr_;
  int64_t cluster_id_;
  DISALLOW_COPY_AND_ASSIGN(ObClusterInfoCheckCont);
};

void ObClusterInfoCheckCont::destroy()
{
  if (NULL != cr_) {
    // inc_ref() in add_async_task()
    cr_->dec_ref();
    cr_ = NULL;
  }
  ObAsyncCommonTask::destroy();
}

int ObClusterInfoCheckCont::init_task()
{
  int ret = OB_SUCCESS;
  char sql[OB_SHORT_SQL_LENGTH];
  sql[0] = '\0';
  int64_t len = snprintf(sql, OB_SHORT_SQL_LENGTH, CHEK_CLUSTER_INFO_SQL, OBPROXY_V_DATABASE_TNAME);
  if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= OB_SHORT_SQL_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to fill sql", K(len), K(ret));
  } else if (OB_FAIL(cr_->mysql_proxy_.async_read(this, sql, pending_action_))) {
    LOG_WARN("fail to async read", K(ret));
  } else if (OB_ISNULL(pending_action_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pending action can not be NULL", K_(pending_action), K(ret));
  }

  return ret;
}

int ObClusterInfoCheckCont::finish_task(void *data)
{
  int ret = OB_SUCCESS;
  const ObString &cluster_name = cr_->cluster_info_key_.cluster_name_.config_string_;
  if (OB_ISNULL(data)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data is null, fail to check clsuter role", K(cluster_name), K(ret));
  } else {
    ObClientMysqlResp *resp = reinterpret_cast<ObClientMysqlResp *>(data);
    ObMysqlResultHandler handler;
    handler.set_resp(resp);
    if (OB_FAIL(handler.next())) {
      if (-ER_NO_SUCH_TABLE == ret) {
        LOG_DEBUG("v$ob_cluster is not exist, maybe ob version < 2.2.4, no need check", K(ret));
        check_result_ = true;
        ret = OB_SUCCESS;
      } else if (-ER_TABLEACCESS_DENIED_ERROR == ret) {
        LOG_DEBUG("v$ob_cluster is denied access, maybe revoke proxyro, no need check", K(ret));
        check_result_ = true;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get resultset for cluster role", K(data), K(ret));
      }
    } else {
      ObString cluster_role;
      ObString cluster_status;
      PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(handler, "cluster_role", cluster_role);
      PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(handler, "cluster_status", cluster_status);

      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(OB_ITER_END != handler.next())) { // check if this is only one
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get cluster role, there is more than one record", K(ret));
        } else if (OB_DEFAULT_CLUSTER_ID == cluster_id_
            && get_global_proxy_config().with_config_server_
            && get_global_proxy_config().enable_standby) {
          if (OB_UNLIKELY(0 != cluster_role.case_compare(PRIMARY_ROLE))
                    || OB_UNLIKELY(0 != cluster_status.case_compare(ROLE_VALID))) {
            ret = OB_OBCONFIG_APPNAME_MISMATCH;
            LOG_WARN("fail to check cluster role", "expected cluster role", PRIMARY_ROLE,
                    "remote cluster role", cluster_role,
                    "expected cluster status", ROLE_VALID,
                    "remote cluster status", cluster_status, K(ret));
          } else {
            check_result_ = true;
          }
        } else {
          check_result_ = true;
        }

      }
    }
  }

  return ret;
}

inline void *ObClusterInfoCheckCont::get_callback_data()
{
  return static_cast<void *>(&check_result_);
}

//---------------------ObServerStateInfoInitCont---------------------//
class ObServerStateInfoInitCont : public ObAsyncCommonTask
{
public:
  ObServerStateInfoInitCont(ObClusterResource *cr, ObContinuation *cb_cont, ObEThread *submit_thread)
    : ObAsyncCommonTask(cb_cont->mutex_, "server_state_info_init_task", cb_cont, submit_thread),
      ss_infos_(), check_result_(false), cr_(cr) {}
  virtual ~ObServerStateInfoInitCont() {}

  virtual void destroy();
  virtual int init_task();
  virtual int finish_task(void *data);
  virtual void *get_callback_data();

private:
  common::ObSEArray<ObServerStateSimpleInfo, ObServerStateRefreshCont::DEFAULT_SERVER_COUNT> ss_infos_;
  bool check_result_;
  ObClusterResource *cr_;
  DISALLOW_COPY_AND_ASSIGN(ObServerStateInfoInitCont);
};

void ObServerStateInfoInitCont::destroy()
{
  if (NULL != cr_) {
    // inc_ref() in add_async_task()
    cr_->dec_ref();
    cr_ = NULL;
  }
  ss_infos_.reset();
  ObAsyncCommonTask::destroy();
}

int ObServerStateInfoInitCont::init_task()
{
  int ret = OB_SUCCESS;
  char sql[OB_SHORT_SQL_LENGTH];
  sql[0] = '\0';
  int64_t len = 0;
  if (IS_CLUSTER_VERSION_LESS_THAN_V4(cr_->cluster_version_)) {
    len = snprintf(sql, OB_SHORT_SQL_LENGTH, INIT_SS_INFO_SQL,
                          OB_ALL_VIRTUAL_PROXY_SERVER_STAT_TNAME,
                          OB_ALL_VIRTUAL_ZONE_STAT_TNAME,
                          INT64_MAX);
  } else {
    len = snprintf(sql, OB_SHORT_SQL_LENGTH, INIT_SS_INFO_SQL_V4,
                          DBA_OB_SERVERS_VNAME,
                          DBA_OB_ZONES_VNAME,
                          INT64_MAX);
  }
  if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= OB_SHORT_SQL_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to fill sql", K(len), K(ret));
  } else if (OB_FAIL(cr_->mysql_proxy_.async_read(this, sql, pending_action_))) {
    LOG_WARN("fail to async read", K(ret));
  } else if (OB_ISNULL(pending_action_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pending action can not be NULL", K_(pending_action), K(ret));
  }

  return ret;
}

int ObServerStateInfoInitCont::finish_task(void *data)
{
  int ret = OB_SUCCESS;
  ObConfigServerProcessor &cs_processor = get_global_config_server_processor();
  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data", K(data), K(ret));
  } else {
    ObClientMysqlResp *resp = reinterpret_cast<ObClientMysqlResp *>(data);
    ObMysqlResultHandler result_handler;
    result_handler.set_resp(resp);
    ObServerStateSimpleInfo ss_info;
    ObString zone_status;
    ObString zone_name;
    ObString region_name;
    ObString idc_name;
    ObString zone_type;
    const int64_t MAX_DISPLAY_STATUS_LEN = 64;
    char display_status_str[MAX_DISPLAY_STATUS_LEN];
    ObServerStatus::DisplayStatus server_status = ObServerStatus::OB_DISPLAY_MAX;
    char ip_str[MAX_IP_ADDR_LENGTH];
    int64_t port = 0;
    int64_t tmp_real_str_len = 0;
    int64_t start_service_time = 0;
    int64_t stop_time = 0;
    ObString cluster_name;
    ObSEArray<ObServerStateInfo, ObServerStateRefreshCont::DEFAULT_SERVER_COUNT> servers_state;
    ObServerStateInfo server_state;
    ObSEArray<ObZoneStateInfo, ObServerStateRefreshCont::DEFAULT_ZONE_COUNT> zones_state;
    ObZoneStateInfo zone_state;
    ObSEArray<ObString, ObServerStateRefreshCont::DEFAULT_ZONE_COUNT> zone_names;

    while (OB_SUCC(ret) && OB_SUCC(result_handler.next())) {
      ss_info.reset();
      zone_status.reset();
      zone_name.reset();
      region_name.reset();
      idc_name.reset();
      zone_type.reset();
      ip_str[0] = '\0';
      display_status_str[0] = '\0';
      server_status = ObServerStatus::OB_DISPLAY_MAX;
      port = 0;
      start_service_time = 0;
      stop_time = 0;
      cluster_name.reset();
      server_state.reset();
      zone_state.reset();

      if (IS_CLUSTER_VERSION_LESS_THAN_V4(cr_->cluster_version_)) {
        PROXY_EXTRACT_INT_FIELD_MYSQL(result_handler, "is_merging", ss_info.is_merging_, int64_t);
      } else {
       ss_info.is_merging_ = 0;
      }

      PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(result_handler, "zone", zone_name);
      PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(result_handler, "zone_status", zone_status);
      PROXY_EXTRACT_STRBUF_FIELD_MYSQL(result_handler, "svr_ip", ip_str, MAX_IP_ADDR_LENGTH, tmp_real_str_len);
      PROXY_EXTRACT_INT_FIELD_MYSQL(result_handler, "svr_port", port, int64_t);
      PROXY_EXTRACT_INT_FIELD_MYSQL(result_handler, "start_service_time", start_service_time, int64_t);
      PROXY_EXTRACT_INT_FIELD_MYSQL(result_handler, "stop_time", stop_time, int64_t);
      PROXY_EXTRACT_STRBUF_FIELD_MYSQL(result_handler, "server_status", display_status_str,
                                       MAX_DISPLAY_STATUS_LEN, tmp_real_str_len);

      if (OB_SUCC(ret)) {
        if (OB_LIKELY(get_global_proxy_config().with_config_server_)
            && (cr_->get_cluster_name() != OB_META_DB_CLUSTER_NAME)) {
          bool is_cluster_alias = cs_processor.is_cluster_name_alias(cr_->get_cluster_name());
          ObString real_cluster_name;
          PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(result_handler, "cluster", cluster_name);
          if (!is_cluster_alias) {
            if (cr_->get_cluster_name() != cluster_name) {
              ret = OB_OBCONFIG_APPNAME_MISMATCH;
              LOG_WARN("fail to check cluster name", "local cluster name", cr_->get_cluster_name(),
                  "remote cluster name", cluster_name, K(ret));
            }
          } else {
            if (OB_FAIL(cs_processor.get_real_cluster_name(real_cluster_name, cr_->get_cluster_name()))) {
              LOG_WARN("fail to get main cluster name", "cluster_name", cr_->get_cluster_name(), "cluster_id", cr_->get_cluster_id());
            } else if (real_cluster_name != cluster_name) {
              ret = OB_OBCONFIG_APPNAME_MISMATCH;
              LOG_WARN("fail to check cluster name", "local cluster name", cr_->get_cluster_name(),
                  "remote cluster name", cluster_name, K(ret));
            }
          }
        }
      }

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
          ss_info.zone_type_ = ZONE_TYPE_READWRITE;
        } else {
          ss_info.zone_type_ = str_to_zone_type(zone_type.ptr());
          if (ZONE_TYPE_INVALID == ss_info.zone_type_) {
            LOG_INFO("zone_type is unkonwn, maybe it is new type, treat it as ReadWrite", K(zone_name), K(zone_type), K(ret));
            ss_info.zone_type_ = ZONE_TYPE_READWRITE;
          }
        }
      }

      if (OB_SUCC(ret)) {
        //accound to ObServerStateInfo::is_treat_as_force_congested()
        if (OB_FAIL(ObServerStatus::str2display_status(display_status_str, server_status))) {
          LOG_WARN("display string to status failed", K(ret), K(display_status_str));
        } else if (OB_FAIL(ss_info.set_addr(ip_str, port))) {
          LOG_WARN("fail to add addr", K(ip_str), K(port), K(ret));
        } else if (OB_FAIL(ss_info.set_zone_name(zone_name))) {
          LOG_WARN("fail to set zone name", K(zone_name), K(ret));
        } else if (OB_FAIL(ss_info.set_region_name(region_name))) {
          LOG_WARN("fail to set region name", K(region_name), K(ret));
        } else if (OB_FAIL(ss_info.set_idc_name(idc_name))) {
          LOG_WARN("fail to set idc name", K(idc_name), K(ret));
        } else {
          //According to ObServerStateInfo::is_treat_as_force_congested()
          const bool is_server_dead_congested = ((start_service_time <= 0 && ObServerStatus::OB_SERVER_ACTIVE != server_status)
                                                  || (ObServerStatus::OB_SERVER_INACTIVE == server_status));
          const bool is_zone_upgrade = (ObZoneStatus::INACTIVE == ObZoneStatus::get_status(zone_status));
          const bool is_server_upgrade = (stop_time > 0);
          ss_info.is_force_congested_ = (is_server_dead_congested || is_zone_upgrade || is_server_upgrade);
        }

        if (FAILEDx(ss_infos_.push_back(ss_info))) {
          LOG_WARN("fail to push back server_info", K(ss_info), K(ret));
        } else {
          LOG_DEBUG("succ to push back server_info", K(ss_info));
        }
      }

      if (OB_SUCC(ret)) {
        zone_state.is_merging_ = ss_info.is_merging_;
        zone_state.zone_type_ = ss_info.zone_type_;
        zone_state.zone_status_ = ObZoneStatus::get_status(zone_status);
        if (OB_FAIL(zone_state.set_zone_name(zone_name))) {
          LOG_WARN("fail to set zone name", K(zone_name), K(ret));
        } else if (OB_FAIL(zone_state.set_region_name(region_name))) {
          LOG_WARN("fail to set region name", K(region_name), K(ret));
        } else if (OB_FAIL(zone_state.set_idc_name(idc_name))) {
          LOG_WARN("fail to set idc name", K(region_name), K(ret));
        } else if (OB_FAIL(zones_state.push_back(zone_state))) {
          LOG_WARN("fail to push back zone_info", K(zone_state), K(ret));
        } else {
          server_state.start_service_time_ = start_service_time;
          server_state.stop_time_ = stop_time;
          server_state.server_status_ = server_status;

          if (OB_FAIL(server_state.add_addr(ip_str, port))) {
            LOG_WARN("fail to add addr", K(ip_str), K(port), K(ret));
            //if svr_ip or svr_port in __all_virtual_proxy_server_stat is wrong,
            //we can skip over this server_state.
            ret = OB_SUCCESS;
            continue;
          } else if (OB_ISNULL(ObServerStateRefreshUtils::get_zone_info_ptr(zones_state, zone_name))) {
            LOG_INFO("this server can not find it's zone, maybe it's zone has been "
                     "deleted, so treat this server as deleted also", K(server_state), K(zone_name), K(zones_state));
            ret = OB_SUCCESS;
            continue;
          } else {
            if (OB_FAIL(servers_state.push_back(server_state))) {
              LOG_WARN("fail to push back server_state", K(server_state), K(ret));
            } else if (OB_FAIL(zone_names.push_back(zone_name))) {
              LOG_WARN("fail to push back zone_name", K(ret), K(zone_name));
            } else {
              LOG_DEBUG("succ to push back server_state and zone_name", K(ret), K(server_state), K(zone_name));
            }
          }
        }
      }
    }//end of while

    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }

    /*
     * build zone_state_ for each server_state
     * pay attention to the validity of the ptr zone_state_
     * the above judgement of get_zone_info_ptr() will not set zone_state_, to avoid the ptr being invalid.
     */
    for (int i = 0; (OB_SUCC(ret)) && (i < zone_names.count()); ++i) {
      ObString &each_zone_name = zone_names.at(i);
      ObServerStateInfo &each_servers_state = servers_state.at(i);
      if (OB_ISNULL(each_servers_state.zone_state_ =
            ObServerStateRefreshUtils::get_zone_info_ptr(zones_state, each_zone_name))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unexpected situation, the corresponding zone_state has been searched before.",
                 K(ret), K(each_zone_name), K(zones_state));
      } else {
        if (each_servers_state.zone_state_->is_readonly_zone()) {
          each_servers_state.replica_.replica_type_ = REPLICA_TYPE_READONLY;
        } else if (each_servers_state.zone_state_->is_encryption_zone()) {
          each_servers_state.replica_.replica_type_ = REPLICA_TYPE_ENCRYPTION_LOGONLY;
        } else {
          each_servers_state.replica_.replica_type_ = REPLICA_TYPE_FULL;
        }
      }
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("fail to get server state info", K(ret));
    } else {
      LocationList server_list; //save the ordered servers
      const uint64_t new_ss_version = cr_->server_state_version_ + 1;
      common::ObIArray<ObServerStateSimpleInfo> &server_state_info = cr_->get_server_state_info(new_ss_version);
      common::DRWLock &server_state_lock = cr_->get_server_state_lock(new_ss_version);
      server_state_lock.wrlock();
      server_state_info.reuse();
      server_state_info.assign(ss_infos_);
      server_state_lock.wrunlock();
      ATOMIC_AAF(&(cr_->server_state_version_), 1);

      if (OB_FAIL(ObServerStateRefreshUtils::order_servers_state(servers_state, server_list))) {
        LOG_WARN("fail to order servers state", K(ret));
      // add to table location
      } else if (OB_LIKELY(!server_list.empty())) {
        LOG_DEBUG("succ to get server list", K(server_list), "server_count", servers_state.count());
        ObTableEntry *entry = NULL;
        ObTableCache &table_cache = get_global_table_cache();
        const bool is_rslist = false;
        if (OB_FAIL(ObRouteUtils::build_sys_dummy_entry(cr_->get_cluster_name(), cr_->get_cluster_id(), server_list, is_rslist, entry))) {
          LOG_WARN("fail to build sys dummy entry", K(server_list), "cluster_name", cr_->get_cluster_name(),
              "cluster_id", cr_->get_cluster_id(), K(ret));
        } else if (OB_ISNULL(entry) || OB_UNLIKELY(!entry->is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("entry can not be NULL here", K(entry), K(ret));
        } else {
          ObTableEntry *tmp_entry = NULL;
          tmp_entry = entry;
          tmp_entry->inc_ref();//just for print
          if (OB_FAIL(ObTableCache::add_table_entry(table_cache, *entry))) {
            LOG_WARN("fail to add table entry", K(*entry), K(ret));
          } else {
            obsys::CWLockGuard guard(cr_->dummy_entry_rwlock_);
            if (NULL != cr_->dummy_entry_) {
              cr_->dummy_entry_->dec_ref();
            }
            cr_->dummy_entry_ = tmp_entry;
            cr_->dummy_entry_->inc_ref();

            LOG_INFO("ObServerStateInfoInitCont, update sys tennant's __all_dummy succ", KPC(tmp_entry));
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

      if (OB_SUCC(ret)) {
        check_result_ = true;
        LOG_INFO("succ to init server state info", "cluster_info", cr_->cluster_info_key_,
                 "server_state_version_", cr_->server_state_version_,
                 K_(ss_infos), K(zones_state), K(servers_state));
      }
    }
  }

  return ret;
}

inline void *ObServerStateInfoInitCont::get_callback_data()
{
  return static_cast<void *>(&check_result_);
}

//---------------------ObClusterResourceCreateCont---------------------//
enum
{
  CR_CONT_MAGIC_ALIVE = 0xAABBCCDD,
  CR_CONT_MAGIC_DEAD = 0xDDCCBBAA
};

class ObClusterResourceCreateCont : public ObAsyncCommonTask
{
public:
  ObClusterResourceCreateCont(ObResourcePoolProcessor &rp_processor, ObContinuation *cb_cont, ObEThread *submit_thread)
    : ObAsyncCommonTask(cb_cont->mutex_, "cluster_resource_create_task", cb_cont, submit_thread),
      is_rslist_from_local_(true), magic_(CR_CONT_MAGIC_ALIVE), init_status_(INIT_BORN),
      rp_processor_(rp_processor), created_cr_(NULL), callback_cr_(NULL), cluster_id_(OB_INVALID_CLUSTER_ID), cluster_name_() {}

  ~ObClusterResourceCreateCont() {}

  int main_handler(int event, void *data);
  int create_cluster_resource(const ObString &cluster_name, const int64_t cluster_id, ObAction *&action);

  // unused functions
  virtual int init_task() { return OB_SUCCESS; }
  virtual int finish_task(void *data)
  {
    UNUSED(data);
    return OB_SUCCESS;
  }
  virtual void *get_callback_data() { return NULL; }
  virtual int schedule_timeout();
  virtual int handle_timeout();

  int set_cluster_info(const common::ObString &cluster_name, int64_t cluster_id);
  int add_async_task();
  int build();
  int check_real_meta_cluster();

  void destroy();
  static const char *get_cr_event_name(const int event);

private:
  int do_create_cluster_resource();
  int handle_inform_out_event();
  int handle_chain_inform_cont();
  int handle_add_pending_list();
  int handle_async_task_complete(void *data);
  int handle_async_task_succ();
  int handle_async_task_fail();

public:
  static const int64_t OP_LOCAL_NUM = 16;

private:
  bool is_rslist_from_local_;
  uint32_t magic_;
  ObClusterResourceInitState init_status_;
  ObResourcePoolProcessor &rp_processor_;
  // we should remember the created cr, no matter succ or not
  ObClusterResource *created_cr_;
  ObClusterResource *callback_cr_;

  int64_t cluster_id_;
  common::ObString cluster_name_;
  char cluster_name_str_[OB_MAX_USER_NAME_LENGTH_STORE];
  DISALLOW_COPY_AND_ASSIGN(ObClusterResourceCreateCont);
};

const char *ObClusterResourceCreateCont::get_cr_event_name(const int event)
{
  const char *name = NULL;
  switch (event) {
    case CLUSTER_RESOURCE_CREATE_EVENT:
      name = "CLUSTER_RESOURCE_CREATE_EVENT";
      break;
    case CLUSTER_RESOURCE_INFORM_OUT_EVENT:
      name = "CLUSTER_RESOURCE_INFORM_OUT_EVENT";
      break;
    case CLUSTER_RESOURCE_CREATE_COMPLETE_EVENT:
      name = "CLUSTER_RESOURCE_CREATE_COMPLETE_EVENT";
      break;
    case EVENT_INTERVAL:
      name = "CLUSTER_RESOURCE_CREATE_TIMEOUT_EVENT";
      break;
    case ASYNC_PROCESS_DONE_EVENT:
      name = "ASYNC_TASK_DONE_EVENT";
      break;
    default:
      name = "UNKNOWN_EVENT";
      break;
  }
  return name;
}

int ObClusterResourceCreateCont::create_cluster_resource(const ObString &cluster_name, const int64_t cluster_id, ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;
  if (OB_FAIL(set_cluster_info(cluster_name, cluster_id))) {
    LOG_WARN("fail to set cluster name", K(cluster_name), K(cluster_id), K(ret));
  } else if (OB_UNLIKELY(submit_thread_ != this_ethread())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("this thread must be equal with submit_thread",
              "this ethread", this_ethread(), K_(submit_thread),
              K(cluster_name), K(cluster_id), K(ret));
  } else if (OB_ISNULL(self_ethread().schedule_imm(this, CLUSTER_RESOURCE_CREATE_EVENT))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to schedule imm", K(ret));
  } else {
    action = &get_action();
  }
  return ret;
}

int ObClusterResourceCreateCont::schedule_timeout()
{
  int ret = OB_SUCCESS;
  int64_t timeout_us = get_global_proxy_config().short_async_task_timeout; // us
  if (OB_ISNULL(submit_thread_) || OB_UNLIKELY(timeout_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("submit thread can not be NULL", K_(submit_thread), K(timeout_us), K(ret));
  } else if (OB_UNLIKELY(NULL != timeout_action_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("timeout action must be NULL", K_(timeout_action), K(ret));
  } else if (OB_ISNULL(timeout_action_ = submit_thread_->schedule_in(this, HRTIME_USECONDS(timeout_us)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to schedule timeout", K_(timeout_action), K(ret));
  }
  return ret;
}

int ObClusterResourceCreateCont::handle_timeout()
{
  int ret = OB_SUCCESS;
  LOG_INFO("handle timeout", K_(created_cr), K_(is_rslist_from_local));
  if (OB_FAIL(cancel_pending_action())) {
    LOG_WARN("fail to cancel pending action", K(ret));
  } else if (NULL != created_cr_) {
    if (is_rslist_from_local_) {
      ObConfigServerProcessor &cs_processor = get_global_config_server_processor();
      int64_t new_failure_count = 0;
      if (OB_FAIL(cs_processor.inc_and_get_create_failure_count(cluster_name_, cluster_id_, new_failure_count))) {
        LOG_WARN("fail to inc_and_get_create_failure_count", K(ret));
      } else {
        LOG_INFO("timeout to create cluster with local rslist",
                 K_(cluster_name), K_(cluster_id), K_(init_status), K(new_failure_count));
      }
    }

    // we only handle timeout for the cont which is not in the pending list
    if (OB_FAIL(handle_async_task_fail())) {
      LOG_WARN("fail to handle async task fail", K(ret));
    }
  }
  return ret;
}

int ObClusterResourceCreateCont::add_async_task()
{
  int ret = OB_SUCCESS;
  ObAsyncCommonTask *async_cont = NULL;
  ObProxyMutex *mutex = NULL;
  if (OB_UNLIKELY(NULL != pending_action_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("pending action must be NULL here", K(ret));
  } else if (OB_ISNULL(created_cr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("created_cr_ can not be null", K_(created_cr), K(ret));
  } else {
    switch (init_status_) {
      case INIT_RS:
        LOG_INFO("will add INIT_RS task", K(this), K_(cluster_name), K_(cluster_id));
        // fetch rs will block creation, so use a new mutex instead of cb_cont's mutex
        is_rslist_from_local_ = false;
        if (OB_ISNULL(mutex = new_proxy_mutex())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("alloc memory for proxy mutex error", K(ret));
        } else {
          created_cr_->inc_ref();
          bool need_update_dummy_entry = true;
          async_cont = new(std::nothrow) ObRslistFetchCont(created_cr_, mutex,
                                                           need_update_dummy_entry,
                                                           this, &self_ethread());
        }
        break;
      case CHECK_VERSION:
        LOG_INFO("will add CHECK_VERSION task", K(this), K_(cluster_name), K_(cluster_id));
        created_cr_->inc_ref();
        async_cont = new(std::nothrow) ObCheckVersionCont(created_cr_, cluster_id_, this, &self_ethread());
        break;
      case CHECK_CLUSTER_INFO:
        LOG_INFO("will add CHECK_CLUSTER_INFO task", K(this), K_(cluster_name), K_(cluster_id));
        created_cr_->inc_ref();
        async_cont = new(std::nothrow) ObClusterInfoCheckCont(created_cr_, cluster_id_, this, &self_ethread());
        break;
      case INIT_SS_INFO:
        LOG_INFO("will add  INIT_SS_INFO task", K(this), K_(cluster_name), K_(cluster_id));
        created_cr_->inc_ref();
        async_cont = new(std::nothrow) ObServerStateInfoInitCont(created_cr_, this, &self_ethread());
        break;
      case INIT_IDC_LIST:
        LOG_INFO("will add  INIT_IDC_LIST task", K(this), K_(cluster_name), K_(cluster_id));
        // fetch idc will block creation, so use a new mutex instead of cb_cont's mutex
        if (OB_ISNULL(mutex = new_proxy_mutex())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("alloc memory for proxy mutex error", K(ret));
        } else {
          created_cr_->inc_ref();
          async_cont = new(std::nothrow) ObIDCListFetchCont(created_cr_, mutex,
                                                            this, &self_ethread(), true);
        }
        break;
      case INIT_SYSVAR:
        LOG_INFO("will add  INIT_SYSVAR task", K(this), K_(cluster_name), K_(cluster_id));
        created_cr_->inc_ref();
        async_cont = new(std::nothrow) ObSysVarFetchCont(created_cr_, mutex_, this, &self_ethread());
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown build state", K_(init_status), K(ret));
        break;
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(async_cont)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc mem for async task continuation", K_(init_status), K(ret));
        if (NULL != mutex) {
          mutex->free();
          mutex = NULL;
        }
        created_cr_->dec_ref();
      } else if (!self_ethread().is_event_thread_type(ET_CALL)) {
        ret = OB_INNER_STAT_ERROR;
        LOG_ERROR("schedule cluster build cont must be in work thread", K(&self_ethread()), K(ret));
      } else {
        if (INIT_RS == init_status_) {
          (void)ATOMIC_FAA(&created_cr_->fetch_rslist_task_count_, 1);
          if (OB_ISNULL(g_event_processor.schedule_imm(async_cont, ET_TASK))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("fail to schedule fetch rslist task", K(ret));
            (void)ATOMIC_FAA(&created_cr_->fetch_rslist_task_count_, -1);
          } else {
            created_cr_->last_rslist_refresh_time_ns_ = get_hrtime();
            pending_action_ = &async_cont->get_action();
          }
        } else if (INIT_IDC_LIST == init_status_) {
          (void)ATOMIC_FAA(&created_cr_->fetch_idc_list_task_count_, 1);
          if (OB_ISNULL(g_event_processor.schedule_imm(async_cont, ET_TASK))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("fail to schedule fetch idc list task", K(ret));
            (void)ATOMIC_FAA(&created_cr_->fetch_idc_list_task_count_, -1);
          } else {
            created_cr_->last_idc_list_refresh_time_ns_ = get_hrtime();
            pending_action_ = &async_cont->get_action();
          }
        } else {
          pending_action_ = &async_cont->get_action();
          async_cont->handle_event(EVENT_IMMEDIATE, NULL);
        }
      }
    }
  }

  if (OB_FAIL(ret) && OB_LIKELY(NULL != async_cont)) {
    async_cont->destroy();
    async_cont = NULL;
    if (NULL != mutex) {
      mutex->free();
      mutex = NULL;
    }
    created_cr_->dec_ref();
  }
  return ret;
}

int ObClusterResourceCreateCont::set_cluster_info(const ObString &cluster_name, int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cluster_name.empty())
      || OB_UNLIKELY(cluster_name.length() > OB_MAX_USER_NAME_LENGTH_STORE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(cluster_name), K(ret));
  } else {
    MEMCPY(cluster_name_str_, cluster_name.ptr(), cluster_name.length());
    cluster_name_.assign_ptr(cluster_name_str_, cluster_name.length());
    cluster_id_ = cluster_id;
  }
  return ret;
}

int ObClusterResourceCreateCont::do_create_cluster_resource()
{
  int ret = OB_SUCCESS;
  bool is_cr_exist = false;
  bool is_add_to_list = false;
  {
    ObClusterResource *cr = NULL;
    ObClusterInfoKey cluster_info_key(cluster_name_, cluster_id_);
    CRLockGuard guard(rp_processor_.cr_map_rwlock_);
    rp_processor_.cr_map_.get_refactored(cluster_info_key, cr);
    if (NULL != cr) {
      is_cr_exist = true;
      if (cr->is_avail()) {
        LOG_DEBUG("cluster resource has already create succ", KPC(cr));
        // cr is avail, just inform outer
        cr->inc_ref();
        if (NULL != callback_cr_) {
          callback_cr_->dec_ref();
        }
        callback_cr_ = cr;
      } else if (cr->is_initing()) {
        cr->pending_list_.push(this);
        is_add_to_list = true;
      } else {
        LOG_WARN("invalid cluster resource", KPC(cr), KPC_(callback_cr));
      }
    }
  }

  if (is_cr_exist) {
    if (!is_add_to_list) {
      if (OB_FAIL(handle_inform_out_event())) {
        LOG_ERROR("fail to schedule infom out cont", K(ret));
      }
    }
  } else { // cr is not exist, create and build
    ret = rp_processor_.create_cluster_resource(cluster_name_, cluster_id_, created_cr_, *this);
    // created_cr != null means new cr has done start build
    // we have 3 situations that created cr will be null
    // 1. new cr already exist in cr_map
    // 2. fail to alloc memory
    // 3. fail to init cluster resource
    if (NULL != created_cr_) {
      // if succ, it means cr is initing, we need do nothing
      // if fail, it means start_build fail, we should inform the chain to callback
      if (OB_SUCC(ret)) {
        // do nothing
      } else if (OB_FAIL(handle_chain_inform_cont())) {
        LOG_ERROR("fail to handle chain inform", K(ret));
      }
    } else if (OB_FAIL(handle_add_pending_list())) {
      LOG_ERROR("fail to handle add cont pending list", K(ret));
    }
  }

  return ret;
}

int ObClusterResourceCreateCont::main_handler(int event, void *data)
{
  int event_ret = EVENT_CONT;
  int ret = OB_SUCCESS;
  LOG_INFO("ObClusterResourceCreateCont::main_handler", "event",
           get_cr_event_name(event), K_(init_status), K_(cluster_name), K_(cluster_id), KP(data));

  if (OB_UNLIKELY(CR_CONT_MAGIC_ALIVE != magic_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("cluster resource cont is dead", K_(magic), K(ret));
  } else {
    switch (event) {
      case CLUSTER_RESOURCE_CREATE_EVENT: {
        pending_action_ = NULL;
        if (OB_FAIL(schedule_timeout())) {
          LOG_WARN("fail to schedule timeout action", K(ret));
        } else if (OB_FAIL(do_create_cluster_resource())) {
          LOG_WARN("fail to do create custer resource", K(ret));
        }
        break;
      }
      case CLUSTER_RESOURCE_INFORM_OUT_EVENT: {
        pending_action_ = NULL;
        if (OB_FAIL(handle_inform_out_event())) {
          LOG_WARN("fail to handle inform out event", K(ret));
        }
        break;
      }
      case EVENT_INTERVAL: {
        timeout_action_ = NULL;
        if (OB_FAIL(handle_timeout())) {
          LOG_WARN("fail to handle timeout event", K(ret));
        }
        break;
      }
      case ASYNC_PROCESS_DONE_EVENT: {
        pending_action_ = NULL;
        if (OB_FAIL(handle_async_task_complete(data))) {
          LOG_WARN("fail to handle async task complete", K_(init_status), K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected event", K(event), K(ret));
        break;
      }
    }
  }

  if (terminate_) {
    destroy();
    event_ret = EVENT_DONE;
  }

  return event_ret;
}

int ObClusterResourceCreateCont::handle_async_task_complete(void *data)
{
  int ret = OB_SUCCESS;
  bool result = false;
  if (OB_ISNULL(data)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid data", K(data), K(ret));
  } else {
    result = *(static_cast<bool *>(data));
    if (result) {
      if (INIT_RS == init_status_) {
        init_status_ = CHECK_VERSION;
      } else if (CHECK_VERSION == init_status_) {
        if (OB_DEFAULT_CLUSTER_ID == cluster_id_
            && get_global_proxy_config().with_config_server_
            && get_global_proxy_config().enable_standby
            && IS_CLUSTER_VERSION_LESS_THAN_V4(created_cr_->cluster_version_)) {
          init_status_ = CHECK_CLUSTER_INFO;
        } else {
          init_status_ = INIT_SS_INFO;
        }
      } else if (CHECK_CLUSTER_INFO == init_status_) {
        init_status_ = INIT_SS_INFO;
      } else if (INIT_SS_INFO == init_status_) {
        init_status_ = INIT_IDC_LIST;
      } else if (INIT_IDC_LIST == init_status_) {
        init_status_ = INIT_SYSVAR;
      } else if (INIT_SYSVAR == init_status_) {
        init_status_ = INIT_SERVER_STATE_PROCESSOR;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected build status", K_(init_status), K(ret));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(build())) {
          LOG_WARN("fail to build async task", K_(init_status), K(ret));
        } else if (INIT_SERVER_STATE_PROCESSOR == init_status_) {
          if (OB_FAIL(handle_async_task_succ())) {
            LOG_WARN("fail to handle async task succ");
          }
        }
      }
    }
  }

  if (OB_SUCC(ret) && OB_UNLIKELY(!result)) {
    ObClusterResourceInitState old_status = init_status_;
    // 1. if local rslist cannot work, we will try to get newest rslist
    if (is_rslist_from_local_) {
      LOG_INFO("local rs list cannot work, try to get newest rs list", K_(init_status));
      result = true;
      init_status_ = INIT_RS;
      if (OB_FAIL(add_async_task())) {
        LOG_WARN("fail to add rslist fetch task", K_(cluster_name), K(ret));
        init_status_ = old_status;
      }
    } else if (CHECK_CLUSTER_INFO == init_status_) {
      // 2. master cluster role is not primary, try to loop all sub cluster rs list
      result = true;
      const bool is_rslist = false;
      ObSEArray<ObAddr, 5> rs_list;
      ObConfigServerProcessor &cs_processor = get_global_config_server_processor();
      if (OB_FAIL(cs_processor.get_next_master_cluster_rslist(cluster_name_, rs_list))) {
        LOG_WARN("fail to get next master cluster rslist", K_(cluster_name));
      } else if (OB_FAIL(ObRouteUtils::build_and_add_sys_dummy_entry(cluster_name_, cluster_id_, rs_list, is_rslist))) {
        LOG_WARN("fail to build and add dummy entry", K_(cluster_name), K_(cluster_id), K(rs_list), K(ret));
      } else if (OB_FAIL(add_async_task())) {
        LOG_WARN("fail to add check cluster name task", K_(cluster_name), K(ret));
      }
    }
  }

  if (OB_FAIL(ret) || OB_UNLIKELY(!result)) {
    if (OB_FAIL(handle_async_task_fail())) {
      LOG_WARN("fail to handle async task", K(ret));
    }
  }
  return ret;
}

int ObClusterResourceCreateCont::handle_async_task_succ()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(created_cr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("created_cr_ can not be null here", K(ret));
  } else {
    CWLockGuard guard(rp_processor_.cr_map_rwlock_); // write lock need
    created_cr_->set_avail_state();
    created_cr_->renew_last_access_time(); // renew last access time when cr is successfully created
  }

  if (OB_FAIL(handle_chain_inform_cont())) {
    LOG_ERROR("fail to handle chain inform", K(ret));
  }
  return ret;
}

int ObClusterResourceCreateCont::handle_async_task_fail()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(created_cr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("created_cr_ can not be null here", K(ret));
  } else {
    ObClusterInfoKey cluster_info_key(cluster_name_, cluster_id_);
    CWLockGuard guard(rp_processor_.cr_map_rwlock_); // write lock need
    created_cr_->set_init_failed_state();
    ObClusterResource *tmp_cr = rp_processor_.cr_map_.remove(cluster_info_key);
    // do not dec_ref, handle_chain_inform_cont
    if (created_cr_ != tmp_cr) {
      // ignore ret
      LOG_ERROR("removed cluster resource must be equal with created cluster resource", K(tmp_cr), K_(created_cr));
    }
  }

  if (OB_FAIL(handle_chain_inform_cont())) {
    LOG_WARN("fail to handle chain inform cont", K(ret));
  }
  return ret;
}

int ObClusterResourceCreateCont::handle_chain_inform_cont()
{
  int ret = OB_SUCCESS;
  int pending_list_count = 0;

  if (NULL != created_cr_) {
    if (created_cr_->is_avail() || created_cr_->is_deleting()) {
      // avail cr may be deleted by other thread, but we should still increment the stats,
      // because they will be decreased when they are deleted
      RESOURCE_POOL_INCREMENT_DYN_STAT(CREATE_CLUSTER_RESOURCE_SUCC_COUNT);
      RESOURCE_POOL_INCREMENT_DYN_STAT(CURRENT_CLUSTER_RESOURCE_COUNT);

      if (created_cr_->is_avail()
          && OB_FAIL(created_cr_->rebuild_mysql_client_pool(created_cr_))) {
        LOG_WARN("fail to create mysql client pool after cluster resource is created", K(ret));
      }
    }

    ObEThread *submit_thread = NULL;
    created_cr_->pending_list_.push(this); // push self
    ObClusterResourceCreateCont *cr_cont = reinterpret_cast<ObClusterResourceCreateCont *>(created_cr_->pending_list_.popall());
    ObClusterResourceCreateCont *next_cr_cont = NULL;
    while (NULL != cr_cont) {
      next_cr_cont = reinterpret_cast<ObClusterResourceCreateCont *>(cr_cont->link_.next_);
      ++pending_list_count;
      submit_thread = cr_cont->submit_thread_;
      if (OB_ISNULL(submit_thread)) {
        LOG_ERROR("submit thread can not be NULL", K(cr_cont));
      } else {
        if (created_cr_->is_avail()) {
          created_cr_->inc_ref();
          if (NULL != cr_cont->callback_cr_) {
            cr_cont->callback_cr_->dec_ref();
          }
          cr_cont->callback_cr_ = created_cr_;
        } else {
          cr_cont->callback_cr_ = NULL;
        }
        if (OB_ISNULL(submit_thread->schedule_imm(cr_cont, CLUSTER_RESOURCE_INFORM_OUT_EVENT))) {
          LOG_ERROR("fail to schedule imm", K(cr_cont));
          if (NULL != cr_cont->callback_cr_) {
            cr_cont->callback_cr_->dec_ref();
            cr_cont->callback_cr_ = NULL;
          }
        }
      }
      // if failed, continue, do not break;
      cr_cont = next_cr_cont;
    }

    if (pending_list_count > 1) {
      RESOURCE_POOL_SUM_DYN_STAT(PARALLEL_CREATE_CLUSTER_RESOURCE_COUNT, (pending_list_count - 1));
    }

    LOG_INFO("cluster resource create complete", KPC_(created_cr),
             "pending_list_count", pending_list_count - 1);
    created_cr_->dec_ref(); // inc_ref() in create_cluster_resource() begin to init
    if (created_cr_->is_init_failed()) {
      created_cr_->dec_ref(); // inc_ref() in create_cluster_resource() after alloc
    }
    created_cr_ = NULL;
  } else {
    // never reach here
    LOG_ERROR("created cluster resource is unexist, unexpected state", K_(created_cr), K_(cluster_name));
  }

  return ret;
}

int ObClusterResourceCreateCont::handle_add_pending_list()
{
  int ret = OB_SUCCESS;
  bool inform_out = true;
  {
    ObClusterInfoKey cluster_info_key(cluster_name_, cluster_id_);
    ObClusterResource *cr = NULL;
    CRLockGuard guard(rp_processor_.cr_map_rwlock_);
    rp_processor_.cr_map_.get_refactored(cluster_info_key, cr);
    if (NULL != cr) {
      if (cr->is_initing()) {
        cr->pending_list_.push(this);
        inform_out = false;
      } else if (cr->is_avail()) {
        cr->inc_ref();
        if (NULL != callback_cr_) {
          callback_cr_->dec_ref();
        }
        callback_cr_ = cr;
      } else {
        callback_cr_ = NULL;
        LOG_ERROR("invalid cluster state", K(*cr));
      }
    }
  }

  if (inform_out) {
    if (OB_FAIL(handle_inform_out_event())) {
      LOG_WARN("fail to handle inform out event", K(ret));
    }
  }

  return ret;
}

void ObClusterResourceCreateCont::destroy()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cancel_pending_action())) {
    LOG_WARN("fail to cancel pending action", K(ret));
  }
  if (OB_FAIL(cancel_timeout_action())) {
    LOG_WARN("fail to cancel timeout action", K(ret));
  }
  cb_cont_ = NULL;
  submit_thread_ = NULL;
  cluster_name_.reset();
  cluster_id_ = OB_INVALID_CLUSTER_ID;
  magic_ = CR_CONT_MAGIC_DEAD;
  mutex_.release();

  op_free(this);
}

int ObClusterResourceCreateCont::handle_inform_out_event()
{
  int ret = OB_SUCCESS;
  if (this_ethread() != submit_thread_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("this thread must be equal with submit_thread",
              "this ethread", this_ethread(), K_(submit_thread),
              K_(cluster_name), K_(cluster_id), K(ret));
  } else {
    if (!action_.cancelled_) {
      if (OB_ISNULL(cb_cont_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cb_cont can not be NULL", K_(cb_cont), K(ret));
        if (NULL != callback_cr_) {
          callback_cr_->dec_ref();
          callback_cr_ = NULL;
        }
      } else {
        LOG_DEBUG("succ to get avail cluster resource in remote", K_(cluster_name), K_(cluster_id));
        cb_cont_->handle_event(CLUSTER_RESOURCE_CREATE_COMPLETE_EVENT, callback_cr_);
      }
    } else {
      LOG_INFO("ObClusterResourceCreateCont has been cancelled", K_(cluster_name), K_(cluster_id), K_(callback_cr));
      if (NULL != callback_cr_) {
        callback_cr_->dec_ref();
        callback_cr_ = NULL;
      }
    }
    terminate_ = true;
  }
  return ret;
}

int ObClusterResourceCreateCont::check_real_meta_cluster()
{
  int ret = OB_SUCCESS;
  // we need check real meta cluster when the follow happened:
  // 1. this is OB_META_DB_CLUSTER_NAME
  // 2. enable cluster checkout
  // 3. real meta cluster do not exist
  ObConfigServerProcessor &cs_processor = get_global_config_server_processor();
  if (OB_LIKELY(get_global_proxy_config().enable_cluster_checkout)
      && cluster_name_ == OB_META_DB_CLUSTER_NAME
      && !cs_processor.is_real_meta_cluster_exist()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("real meta cluster is not exist", K(ret));
  }
  return ret;
}

int ObClusterResourceCreateCont::build()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(created_cr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("created cr can not be null here", K(ret));
  }

  if (OB_SUCC(ret) && INIT_BORN == init_status_) {
    if (OB_FAIL(created_cr_->init_local_config(rp_processor_.config_))) {
      LOG_WARN("fail to build local", K(ret));
    } else {
      init_status_ = INIT_RS;
    }
  }

  if (OB_SUCC(ret) && INIT_RS == init_status_) {
    if (get_global_proxy_config().enable_get_rslist_remote) {
      if (OB_FAIL(add_async_task())) {
        LOG_WARN("fail to add async task", K_(init_status), K(ret));
      }
    } else {
      const bool is_rslist = false;
      ObSEArray<ObAddr, 5> rs_list;
      ObConfigServerProcessor &cs_processor = get_global_config_server_processor();
      int64_t new_failure_count = 0;
      if (OB_FAIL(check_real_meta_cluster())) {
        // if fail to check real meta cluster, try to get newest cluster rslist from config server
        if (OB_FAIL(add_async_task())) {
          LOG_WARN("fail to add async fetch rslist task", K_(cluster_name), K_(cluster_id), K(ret));
        }
      } else if (OB_FAIL(cs_processor.get_cluster_rs_list(cluster_name_, cluster_id_, rs_list))) {
        // if fail to get local cluster rslist, try to get newest cluster rslist from config server
        if (is_rslist_from_local_) {
          if (OB_FAIL(add_async_task())) {
            LOG_WARN("fail to add async fetch rslist task", K_(cluster_name), K_(cluster_id), K(ret));
          }
        } else {
          LOG_WARN("had already fetch rslist, no need try again", K_(cluster_name), K_(cluster_id), K(ret));
        }
      } else if (OB_FAIL(cs_processor.get_create_failure_count(cluster_name_, cluster_id_, new_failure_count))) {
        LOG_WARN("fail to get_create_failure_count", K_(cluster_name), K_(cluster_id), K(ret));
      } else {
        if (is_rslist_from_local_
            && new_failure_count >= ObProxyClusterInfo::RESET_RS_LIST_FAILURE_COUNT) {
          LOG_INFO("local rslist is unavailable many times, need get newest one",
                   K_(cluster_name), K_(cluster_id), K(new_failure_count), K(rs_list));
          if (OB_FAIL(add_async_task())) {
            LOG_WARN("fail to add async fetch rslist task", K_(cluster_name), K_(cluster_id), K(ret));
          }
        } else if (OB_FAIL(ObRouteUtils::build_and_add_sys_dummy_entry(cluster_name_, cluster_id_, rs_list, is_rslist))) {
          LOG_WARN("fail to build and add dummy entry", K_(cluster_name), K_(cluster_id), K(rs_list), K(ret));
        } else {
          init_status_ = CHECK_VERSION;
          LOG_DEBUG("try next status",
                   K_(cluster_name), K_(cluster_id), K(new_failure_count), K(rs_list), K(is_rslist_from_local_), K(init_status_));
        }
      }
    }
  }

  if (OB_SUCC(ret) && CHECK_CLUSTER_INFO == init_status_) {
    if (OB_DEFAULT_CLUSTER_ID == cluster_id_
        && get_global_proxy_config().with_config_server_
        && get_global_proxy_config().enable_standby) {
      int64_t cluster_id = OB_DEFAULT_CLUSTER_ID;
      ObConfigServerProcessor &cs_processor = get_global_config_server_processor();
      if (OB_FAIL(cs_processor.get_master_cluster_id(cluster_name_, cluster_id)
          || OB_DEFAULT_CLUSTER_ID == cluster_id)) {
        // mayby no master on config server, here try a cluster rs list by random
        const bool is_rslist = false;
        ObSEArray<ObAddr, 5> rs_list;
        if (OB_FAIL(cs_processor.get_next_master_cluster_rslist(cluster_name_, rs_list))) {
          LOG_WARN("fail to get next master cluster rslist", K_(cluster_name));
        } else if (OB_FAIL(ObRouteUtils::build_and_add_sys_dummy_entry(cluster_name_, cluster_id_, rs_list, is_rslist))) {
          LOG_WARN("fail to build and add dummy entry", K_(cluster_name), K_(cluster_id), K(rs_list), K(ret));
        }
      }
    }

    if (OB_SUCC(ret) && OB_FAIL(add_async_task())) {
      LOG_WARN("fail to add async task", K_(init_status), K(ret));
    }
  }

  if (OB_SUCC(ret)
      && (CHECK_VERSION == init_status_
          || INIT_SYSVAR == init_status_
          || INIT_SS_INFO == init_status_
          || INIT_IDC_LIST == init_status_)) {
    if (OB_FAIL(add_async_task())) {
      LOG_WARN("fail to add async task", K_(init_status), K(ret));
    }
  }

  if (OB_SUCC(ret) && INIT_SERVER_STATE_PROCESSOR == init_status_) {
    // init server state processor
    if (OB_FAIL(created_cr_->init_server_state_processor(rp_processor_.config_))) {
      LOG_WARN("fail to init server state refresh processor", K(ret));
    } else {
      LOG_DEBUG("finish build cluster resource", K_(cluster_name), K_(cluster_id), K(ret));
    }
  }

  return ret;
}


//-----------------------ObClusterResource-----------------------//
int ObClusterResource::init(const common::ObString &cluster_name, int64_t cluster_id, const int64_t version)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K_(is_inited), K(ret));
  } else if (cluster_name.empty() || version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(cluster_name), K(version), K(ret));
  } else if (OB_FAIL(set_cluster_info(cluster_name, cluster_id))) {
    LOG_WARN("fail to set cluster name", K(cluster_name), K(cluster_id), K(ret));
  } else if (OB_FAIL(pending_list_.init("cr init pending list",
          reinterpret_cast<int64_t>(&(reinterpret_cast<ObClusterResourceCreateCont *> (0))->link_)))) {
    LOG_WARN("fail to init pending list", K(ret));
  } else if (OB_FAIL(alive_addr_set_.create(4))) {
    LOG_WARN("alive_addr_set create failed", K(ret));
  } else {
    is_inited_ = true;
    version_ = version;
  }
  return ret;
}

DEF_TO_STRING(ObClusterResource)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this), K_(ref_count), K_(is_inited), K_(cluster_info_key),
       "cr_state", get_cr_state_str(cr_state_), K_(version),
       K_(last_access_time_ns), K_(deleting_completed_thread_num),
       K_(fetch_rslist_task_count), K_(fetch_idc_list_task_count),
       K_(last_idc_list_refresh_time_ns), K_(last_rslist_refresh_time_ns), K_(server_state_version));
  J_OBJ_END();
  return pos;
}

int ObClusterResource::init_local_config(const ObResourcePoolConfig &config)
{
  int ret = OB_SUCCESS;

  // init mysql client pool and mysql proxy
  int64_t timeout_ms = usec_to_msec(config.short_async_task_timeout_);
  const ObString user_name(ObProxyTableInfo::READ_ONLY_USERNAME);
  const ObString database(ObProxyTableInfo::READ_ONLY_DATABASE);
  ObString cluster_name = cluster_info_key_.cluster_name_.config_string_;
  ObConfigItem item;
  char password[ENC_STRING_BUF_LEN];
  char password1[ENC_STRING_BUF_LEN];
  memset(password, 0, sizeof (password));
  memset(password1, 0, sizeof (password1));

  ObVipAddr addr;
  if (OB_FAIL(get_global_config_processor().get_proxy_config(
          addr, cluster_name, "", ObProxyTableInfo::OBSERVER_SYS_PASSWORD, item))) {
    LOG_WARN("get observer_sys_password config failed", K(cluster_name), K(ret));
  } else {
    MEMCPY(password, item.str(), strlen(item.str()));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_global_config_processor().get_proxy_config(
            addr, cluster_name, "", ObProxyTableInfo::OBSERVER_SYS_PASSWORD1, item))) {
      LOG_WARN("get observer_sys_password1 config failed", K(cluster_name), K(ret));
    } else {
      MEMCPY(password1, item.str(), strlen(item.str()));
    }
  }


  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(mysql_proxy_.init(timeout_ms, user_name, password, database, cluster_name, password1))) {
    LOG_WARN("fail to init mysql proxy", K(ret));
  } else if (OB_FAIL(rebuild_mysql_client_pool(
             get_global_resource_pool_processor().get_default_cluster_resource()))) {
    LOG_WARN("fail t create mysql client pool for init cluster resource", K(ret));
  }

  // init sys var set processor
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sys_var_set_processor_.init())) {
      LOG_WARN("fail to init sys var set processor", K(ret));
    } else if (OB_FAIL(sys_var_set_processor_.renew_sys_var_set(get_global_resource_pool_processor().get_default_sysvar_set()))) {
      LOG_WARN("fail to renew sys var set", K(ret));
    }
  }

  // init congestion manager
  if (OB_SUCC(ret)) {
    if (OB_FAIL(congestion_manager_.init())) {
      LOG_WARN("failed to init congestion manager", K(ret));
    } else {
      ObCongestionControlConfig *control_config = NULL;
      if (OB_ISNULL(control_config = op_alloc(ObCongestionControlConfig))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("failed to allocate memory for congestion control config", K(ret));
      } else {
        control_config->fail_window_sec_ = usec_to_sec(config.congestion_fail_window_us_);
        control_config->conn_failure_threshold_ = config.congestion_failure_threshold_;
        control_config->alive_failure_threshold_ = config.congestion_failure_threshold_;
        control_config->min_keep_congestion_interval_sec_ = usec_to_sec(config.min_keep_congestion_interval_us_);
        control_config->retry_interval_sec_ = usec_to_sec(config.congestion_retry_interval_us_);
        control_config->inc_ref();
        if (OB_FAIL(congestion_manager_.update_congestion_config(control_config, true))) {
          LOG_WARN("failed to update congestion config", K(ret));
        }
        control_config->dec_ref();
        control_config = NULL;
      }
    }
  }

  return ret;
}

int ObClusterResource::init_server_state_processor(const ObResourcePoolConfig &config)
{
  int ret = OB_SUCCESS;
  ObConfigServerProcessor &cs_processor = get_global_config_server_processor();
  uint64_t last_rs_list_hash = 0;
  bool is_metadb = (0 == cluster_info_key_.cluster_name_.get_string().case_compare(OB_META_DB_CLUSTER_NAME));
  if (OB_FAIL(cs_processor.get_rs_list_hash(cluster_info_key_.cluster_name_.config_string_,
                                            cluster_info_key_.cluster_id_, last_rs_list_hash))) {
    LOG_WARN("fail to get_last_rs_list_hash", K_(cluster_info_key), K(ret));
  } else if (OB_ISNULL(ss_refresh_cont_ = op_alloc(ObServerStateRefreshCont))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ObServerStateRefreshCont", K(ret));
  } else if (!is_metadb && OB_FAIL(ss_refresh_cont_->init(this, config.server_state_refresh_interval_, last_rs_list_hash))) {
    LOG_WARN("fail to init server state processor", K_(cluster_info_key), K(ret));
  } else if (is_metadb && OB_FAIL(ss_refresh_cont_->init(this, config.metadb_server_state_refresh_interval_, last_rs_list_hash))) {
    LOG_WARN("fail to init metadb server state processor", K_(cluster_info_key), K(ret));
  } else if (!is_metadb && OB_ISNULL(detect_server_state_cont_ = op_alloc(ObDetectServerStateCont))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ObDetectServerStateCont", K(ret));
  } else if (!is_metadb && OB_FAIL(detect_server_state_cont_->init(this, config.server_detect_refresh_interval_))) {
    LOG_WARN("fail to init detect server state cont", K_(config.server_detect_refresh_interval), K(ret));
  } else {
    bool imm = true;
    if (OB_FAIL(ss_refresh_cont_->schedule_refresh_server_state(imm))) {
      LOG_WARN("fail to start schedule refresh server state", K(ret));
    } else if (!is_metadb && OB_FAIL(detect_server_state_cont_->schedule_detect_server_state())) {
      LOG_WARN("fail to start schedule detect server state", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    if (NULL != ss_refresh_cont_) {
      ss_refresh_cont_->kill_this();
      ss_refresh_cont_ = NULL;
    }
    if (NULL != detect_server_state_cont_) {
      detect_server_state_cont_->kill_this();
      detect_server_state_cont_ = NULL;
    }
  }
  return ret;
}

const char *ObClusterResource::get_cr_state_str(const ObCRState state)
{
  const char *state_str = NULL;
  switch (state) {
    case CR_BORN:
      state_str = "CR_BORN";
      break;
    case CR_INITING:
      state_str = "CR_INITING";
      break;
    case CR_INIT_FAILED:
      state_str = "CR_INIT_FAILED";
      break;
    case CR_AVAIL:
      state_str = "CR_AVAIL";
      break;
    case CR_DEAD:
      state_str = "CR_DEAD";
      break;
    case CR_DELETING:
      state_str = "CR_DELETING";
      break;
    default:
      state_str = "CR_UNKNOWN";
      break;
  }
  return state_str;
}

void ObClusterResource::free()
{
  destroy();
  // mysql_proxy and congestion manager must destroy here
  mysql_proxy_.destroy();
  congestion_manager_.destroy();
  cr_state_ = CR_DEAD;
  {
    obsys::CWLockGuard guard(dummy_entry_rwlock_);
    if (NULL != dummy_entry_) {
      dummy_entry_->dec_ref();
      dummy_entry_ = NULL;
    }
  }
  ObProxyMutex *mutex_ = &self_ethread().get_mutex();
  RESOURCE_POOL_INCREMENT_DYN_STAT(FREE_CLUSTER_RESOURCE_COUNT);
  LOG_INFO("the cluster resource will free", KPC(this));

  op_free(this);
}

int ObClusterResource::rebuild_mysql_client_pool(ObClusterResource *cr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(cr), K(ret));
  } else {
    const ObString user_name(ObProxyTableInfo::READ_ONLY_USERNAME);
    const ObString database(ObProxyTableInfo::READ_ONLY_DATABASE);
    ObString password(get_global_proxy_config().observer_sys_password.str());
    ObString password1(get_global_proxy_config().observer_sys_password1.str());
    const bool is_meta_mysql_client = (get_global_resource_pool_processor().get_default_cluster_resource() == cr);
    if (OB_FAIL(mysql_proxy_.rebuild_client_pool(cr, is_meta_mysql_client, get_cluster_name(), get_cluster_id(), user_name,
                password, database, password1))) {
      LOG_WARN("fail to create mysql client pool", K(ret));
    }
  }
  return ret;
}

int ObClusterResource::set_cluster_info(const ObString &cluster_name, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cluster_name.empty())
      || OB_UNLIKELY(cluster_name.length() > OB_MAX_USER_NAME_LENGTH_STORE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(cluster_name), K(ret));
  } else {
    cluster_info_key_.set_cluster_name(cluster_name);
    cluster_info_key_.set_cluster_id(cluster_id);
  }
  return ret;
}

void ObClusterResource::destroy()
{
  if (is_inited_) {
    LOG_INFO("ObClusterResource will destroy, and wait to be free", KPC(this));
    int ret = OB_SUCCESS;
    if (OB_FAIL(stop_refresh_server_state())) {
      LOG_WARN("fail to stop refresh server state", K(ret));
    } else if (OB_FAIL(stop_detect_server_state())) {
      LOG_WARN("fail to stop detect server state", K(ret));
    }
    destroy_location_tenant_info();

    {
      DRWLock::WRLockGuard lock(sys_ldg_info_lock_);
      ObSysLdgInfoMap::iterator tmp_it;
      for (ObSysLdgInfoMap::iterator it = sys_ldg_info_map_.begin(); it != sys_ldg_info_map_.end();) {
        tmp_it = it;
        ++it;
        op_free(&(*tmp_it));
      }
      sys_ldg_info_map_.reset();
    }

    mysql_proxy_.destroy_client_pool();
    sys_var_set_processor_.destroy();
    is_inited_ = false;
  }
}

bool ObClusterResource::inc_and_test_deleting_complete()
{
  bool complete = false;
  int64_t complete_count = ATOMIC_AAF(&deleting_completed_thread_num_, 1);
  int64_t thread_count = g_event_processor.thread_count_for_type_[ET_CALL];
  if (complete_count == thread_count) {
    complete = true;
  }
  return complete;
}

int ObClusterResource::stop_refresh_server_state()
{
  int ret = OB_SUCCESS;
  if (NULL != ss_refresh_cont_) {
    if (OB_ISNULL(g_event_processor.schedule_imm(ss_refresh_cont_, ET_CALL, DESTROY_SERVER_STATE_EVENT))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to schedule imm DESTROY_SERVER_STATE_EVENT", KPC_(ss_refresh_cont), K(ret));
    }
    ss_refresh_cont_ = NULL;
  }
  return ret;
}

int ObClusterResource::stop_detect_server_state()
{
  int ret = OB_SUCCESS;
  if (NULL != detect_server_state_cont_) {
    if (OB_ISNULL(g_event_processor.schedule_imm(detect_server_state_cont_, ET_CALL, DESTROY_SERVER_STATE_EVENT))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to schedule imm DESTROY_SERVER_STATE_EVENT", KPC_(detect_server_state_cont), K(ret));
    }
    LOG_DEBUG("stop detect server state", KPC(this));
    detect_server_state_cont_ = NULL;
  }
  return ret;
}

int ObClusterResource::update_sys_ldg_info(ObSysLdgInfo *sys_ldg_info)
{
  int ret = OB_SUCCESS;
  ObSysLdgInfo *tmp_info = NULL;
  DRWLock::WRLockGuard lock(sys_ldg_info_lock_);
  if (NULL != (tmp_info = sys_ldg_info_map_.remove(sys_ldg_info->get_hash_key()))) {
    op_free(tmp_info);
    tmp_info = NULL;
  }
  if (OB_SUCC(ret) && OB_FAIL(sys_ldg_info_map_.unique_set(sys_ldg_info))) {
    LOG_WARN("sys ldg info map add info failed", K(ret));
  }
  return ret;
}

void ObClusterResource::destroy_location_tenant_info()
{
  DRWLock::WRLockGuard lock(location_tenant_info_lock_);
  ObLocationTenantInfoHashMap::iterator tmp_iter;
  for (ObLocationTenantInfoHashMap::iterator iter = location_tenant_info_map_.begin();
      iter != location_tenant_info_map_.end();) {
    tmp_iter = iter;
    ++iter;
    ObLocationTenantInfo *tmp_ptr = &(*tmp_iter);
    tmp_ptr->dec_ref();
    tmp_ptr = NULL;
  }

  location_tenant_info_map_.reset();
}

int ObClusterResource::update_location_tenant_info(ObSEArray<ObString, 4> &tenant_array,
                                                   ObSEArray<ObString, 4> &locality_array,
                                                   ObSEArray<ObString, 4> &primary_zone_array)
{
  int ret = OB_SUCCESS;

  int64_t tenant_num = tenant_array.count();
  int64_t locality_num = locality_array.count();
  int64_t primary_zone_num = primary_zone_array.count();

  if (tenant_num != 0
      && (tenant_num == locality_num
          && tenant_num == primary_zone_num)) {
    DRWLock::WRLockGuard lock(location_tenant_info_lock_);
    ObLocationTenantInfoHashMap::iterator tmp_iter;
    for (ObLocationTenantInfoHashMap::iterator iter = location_tenant_info_map_.begin();
         iter != location_tenant_info_map_.end();) {
      tmp_iter = iter;
      ++iter;
      ObLocationTenantInfo *tmp_ptr = &(*tmp_iter);
      tmp_ptr->dec_ref();
      tmp_ptr = NULL;
    }

    location_tenant_info_map_.reset();
    LOG_DEBUG("reset location tenant info map done");

    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_array.count(); i++) {
      ObString &tenant_name = tenant_array.at(i);
      ObString &locality = locality_array.at(i);
      ObString &primary_zone = primary_zone_array.at(i);

      if (tenant_name.length() >= OB_MAX_TENANT_NAME_LENGTH
          || primary_zone.length() >= MAX_ZONE_LIST_LENGTH) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant name or primary zone string oversize", K(ret), K(tenant_name), K(primary_zone));
      } else {
        ObLocationTenantInfo *info = NULL;
        if (OB_ISNULL(info = op_alloc(ObLocationTenantInfo))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("update location tenant info alloc memory failed", K(ret));
        } else {
          info->inc_ref();
          MEMCPY(info->tenant_name_str_, tenant_name.ptr(), tenant_name.length());
          info->tenant_name_.assign_ptr(info->tenant_name_str_, tenant_name.length());
          info->version_ = locality.hash();

          // cut origin primary zone string to priority array & weight array
          if (OB_FAIL(info->resolve_location_tenant_info_primary_zone(primary_zone))) {
            LOG_ERROR("fail to resolve location tenant info primary zone", K(ret), K(primary_zone), K(tenant_name));
          } else if (OB_FAIL(location_tenant_info_map_.unique_set(info))) {
            LOG_ERROR("fail to add location tenant info, already exist, never happen",
                      K(tenant_name), K_(cluster_info_key));
          } else {
            LOG_DEBUG("succ to add tenant location info to map", KPC(info));
          }

          if (OB_FAIL(ret) && OB_NOT_NULL(info)) {
            info->dec_ref();
            info = NULL;
          }
        }
      }
    } // for
  } // if in wlock

  return ret;
}

bool ObClusterResource::check_tenant_valid(const common::ObString &tenant_name,
                                           const common::ObString &cluster_name)
{
  bool bret = false;
  int ret = OB_SUCCESS;
  char key_buf[OB_PROXY_FULL_USER_NAME_MAX_LEN];
  int32_t len = 0;
  char ch = '#';
  MEMCPY(key_buf, tenant_name.ptr(), tenant_name.length());
  len += tenant_name.length();
  MEMCPY(key_buf + len, &ch, 1);
  len += 1;
  MEMCPY(key_buf + len, cluster_name.ptr(), cluster_name.length());
  len += cluster_name.length();
  ObString key;
  key.assign_ptr(key_buf, len);
  ObSysLdgInfo *tmp_ldg_info = NULL;
  DRWLock::RDLockGuard lock(sys_ldg_info_lock_);
  if (OB_FAIL(sys_ldg_info_map_.get_refactored(key, tmp_ldg_info))) {
    LOG_WARN("get sys ldg info failed", K(ret), K(key));
  } else if (OB_ISNULL(tmp_ldg_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tmp ldg info is null", K(ret), K(key));
  } else {
    if (tmp_ldg_info->ldg_role_ != "LDG PRIMARY") {
      LOG_WARN("ldg role is not primary", KPC(tmp_ldg_info));
    } else {
      bret = true;
    }
  }
  return bret;
}

uint64_t ObClusterResource::get_location_tenant_version(const ObString &tenant_name)
{
  int64_t version = 0;
  int ret = OB_SUCCESS;
  if (!tenant_name.empty() && tenant_name != OB_SYS_TENANT_NAME) {
    ObLocationTenantInfo *info = NULL;
    DRWLock::RDLockGuard lock(location_tenant_info_lock_);
    if (OB_FAIL(location_tenant_info_map_.get_refactored(tenant_name, info))) {
      if (OB_LIKELY(ret == OB_HASH_NOT_EXIST)) {
        LOG_DEBUG("get tenant version failed", K(tenant_name));
      } else {
        LOG_WARN("get tenant version failed, please check", K(ret), K(tenant_name));
      }
    } else if (NULL != info) {
      version = info->version_;
    }
  }

  return version;
}

int ObClusterResource::get_location_tenant_info(const ObString &tenant_name, ObLocationTenantInfo *&info_out)
{
  int ret = OB_SUCCESS;

  if (!tenant_name.empty() && tenant_name != OB_SYS_TENANT_NAME) {
    ObLocationTenantInfo *info = NULL;
    DRWLock::RDLockGuard lock(location_tenant_info_lock_);
    if (OB_FAIL(location_tenant_info_map_.get_refactored(tenant_name, info))) {
      LOG_DEBUG("do not find location tenant info from map, maybe no right to visit table", K(ret), K(tenant_name));
    } else if (info != NULL) {
      info->inc_ref();
      info_out = info;
      LOG_DEBUG("find location tenant info from map", K(info));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, get null ptr from map", K(tenant_name));
    }
  }

  return ret;
}

void ObLocationTenantInfo::destroy()
{
  op_free(this);
}

/*
 * the primary_zone from __all_tenant of each tenant could be empty/RANDOM, do not save it
 * use the tenant location cache while no primary zone appropriately
 */
int ObLocationTenantInfo::resolve_location_tenant_info_primary_zone(ObString &primary_zone)
{
  int ret = OB_SUCCESS;

  if (!primary_zone.empty()
      && 0 != primary_zone.case_compare(OB_RANDOM_PRIMARY_ZONE)) {
    // save origin str
    MEMCPY(primary_zone_str_, primary_zone.ptr(), primary_zone.length());
    primary_zone_.assign_ptr(primary_zone_str_, primary_zone.length());

    // resolve to zone priority array
    // resolve to zone weight priority array
    if (OB_FAIL(split_weight_group(primary_zone_, primary_zone_prio_array_, primary_zone_prio_weight_array_))) {
      LOG_WARN("fail to resolve primary zone", K(ret), K(primary_zone_));
    } else if (primary_zone_prio_array_.count() != primary_zone_prio_weight_array_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected array count check", K(tenant_name_), K(version_), K(primary_zone_prio_array_.count()),
                                               K(primary_zone_prio_weight_array_.count()));
    } else {
      LOG_DEBUG("succ to resolve primary zone", KPC(this));
    }
  }

  return ret;
}

ObResourceDeleteActor *ObResourceDeleteActor::alloc(ObClusterResource *cr)
{
  int ret = OB_SUCCESS;
  ObResourceDeleteActor *actor = NULL;
  if (OB_ISNULL(cr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(cr), K(ret));
  } else if (OB_ISNULL(actor = op_alloc(ObResourceDeleteActor))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc ObResourceDeleteActor", K(ret));
  } else {
    cr->inc_ref();
    actor->cr_ = cr;
  }

  return actor;
}

void ObResourceDeleteActor::free()
{
  if (NULL != cr_) {
    cr_->dec_ref();
    cr_ = NULL;
  }
  retry_count_ = 0;
  op_free(this);
}

//----------------------ObResourcePoolConfig---------------------
bool ObResourcePoolConfig::update(const ObProxyConfig &config)
{
  bool bret = false;
  if (long_async_task_timeout_ != config.long_async_task_timeout.get()) {
    long_async_task_timeout_ = config.long_async_task_timeout;
    bret = true;
  }
  if (short_async_task_timeout_ != config.short_async_task_timeout.get()) {
    short_async_task_timeout_ = config.short_async_task_timeout;
    bret = true;
  }
  if (server_state_refresh_interval_ != config.server_state_refresh_interval.get()) {
    server_state_refresh_interval_ = config.server_state_refresh_interval;
    bret = true;
  }
  if (metadb_server_state_refresh_interval_ != config.metadb_server_state_refresh_interval.get()) {
    metadb_server_state_refresh_interval_ = config.metadb_server_state_refresh_interval;
    bret = true;
  }
  if (min_keep_congestion_interval_us_ != config.min_keep_congestion_interval.get()) {
    min_keep_congestion_interval_us_ = config.min_keep_congestion_interval;
    bret = true;
  }
  if (congestion_retry_interval_us_ != config.congestion_retry_interval.get()) {
    congestion_retry_interval_us_ = config.congestion_retry_interval;
    bret = true;
  }
  if (congestion_fail_window_us_ != config.congestion_fail_window.get()) {
    congestion_fail_window_us_ = config.congestion_fail_window;
    bret = true;
  }
  if (congestion_failure_threshold_ != config.congestion_failure_threshold.get()) {
    congestion_failure_threshold_ = config.congestion_failure_threshold;
    bret = true;
  }
  if (server_detect_refresh_interval_ != config.server_detect_refresh_interval.get()) {
    server_detect_refresh_interval_ = config.server_detect_refresh_interval;
    bret = true;
  }
  return bret;
}

//----------------------ObResourcePoolProcessor---------------------//
int ObResourcePoolProcessor::init(ObProxyConfig &config, proxy::ObMysqlProxy &meta_client_proxy)
{
  int ret = OB_SUCCESS;
  int64_t default_cr_version = acquire_cluster_resource_version();
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K_(is_inited), K(ret));
  } else if (OB_ISNULL(default_sysvar_set_ = new (std::nothrow) ObDefaultSysVarSet())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc mem for default sysvar set", K(ret));
  } else if (OB_FAIL(default_sysvar_set_->init())) {
    LOG_WARN("fail to init default sysvar set", K(ret));
  } else if (OB_FAIL(default_sysvar_set_->load_default_system_variable())) {
    LOG_WARN("fail to load default system variable", K(ret));
  } else if (OB_ISNULL(default_cr_ = op_alloc(ObClusterResource))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc mem for default cluster resource", K(ret));
  } else if (OB_FAIL(default_cr_->init(ObString::make_string(OB_PROXY_DEFAULT_CLUSTER_NAME), OB_DEFAULT_CLUSTER_ID, default_cr_version))) {
    LOG_WARN("fail to init default cluster resource", K(ret));
  } else if (FALSE_IT(config_.update(config))) {
    // impossible
  } else if (OB_FAIL(default_cr_->init_local_config(config_))) {
    LOG_WARN("fail to build local default cluster resource", K(ret));
  } else if (OB_FAIL(ip_set_.create(8))) {
    LOG_WARN("ip_set create failed", K(ret));
  } else {
    default_cr_->inc_ref();
    default_sysvar_set_->inc_ref();
    meta_client_proxy_ = &meta_client_proxy;
    is_inited_ = true;
  }

  if (OB_FAIL(ret)) {
    if (OB_LIKELY(NULL != default_cr_)) {
      default_cr_->dec_ref();
      default_cr_ = NULL;
    }
    if (OB_LIKELY(NULL != default_sysvar_set_)) {
      default_sysvar_set_->dec_ref();  // free
      default_sysvar_set_ = NULL;
    }
  }
  return ret;
}

void ObResourcePoolProcessor::destroy()
{
  if (OB_LIKELY(is_inited_)) {
    CWLockGuard guard(cr_map_rwlock_);
    ObCRHashMap::iterator last = cr_map_.end();
    for (ObCRHashMap::iterator cr_iter = cr_map_.begin(); (cr_iter != last); ++cr_iter) {
      cr_iter->dec_ref();
    }
    cr_map_.reset();

    if (NULL != default_cr_) {
      default_cr_->dec_ref();
      default_cr_ = NULL;
    }
    if (NULL != default_sysvar_set_) {
      default_sysvar_set_->dec_ref();
      default_sysvar_set_ = NULL;
    }
    is_inited_ = false;
  }
}

DEF_TO_STRING(ObResourcePoolProcessor)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(is_inited), K_(first_cluster_name_str),
       K_(newest_cluster_resource_version), K_(config),
       KPC_(default_sysvar_set), KPC_(meta_client_proxy), KPC_(default_cr));
  J_OBJ_END();
  return pos;
}

ObClusterResource *ObResourcePoolProcessor::acquire_cluster_resource(const ObString &cluster_name, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  ObClusterInfoKey cluster_info_key(cluster_name, cluster_id);
  CRLockGuard guard(cr_map_rwlock_);
  ObClusterResource *cr = NULL;
  if (!cluster_name.empty()) {
    if (OB_SUCCESS != cr_map_.get_refactored(cluster_info_key, cr)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_INFO("can not find cluster resource, maybe it has not been created yet", K(cluster_info_key), K(ret));
    } else if (OB_ISNULL(cr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cluster resource get from cr_map is null", K(cr), K(ret));
    } else {
      cr->inc_ref();
    }
  }
  return cr;
}

ObClusterResource *ObResourcePoolProcessor::acquire_avail_cluster_resource(const ObString &cluster_name, const int64_t cluster_id)
{
  ObClusterResource *cr = acquire_cluster_resource(cluster_name, cluster_id);
  if (NULL != cr) {
    if (!cr->is_avail()) {
      cr->dec_ref();
      cr = NULL;
    }
  }
  return cr;
}

void ObResourcePoolProcessor::release_cluster_resource(ObClusterResource *cr)
{
  if (NULL != cr) {
    cr->dec_ref();
    cr = NULL;
  }
}

bool ObResourcePoolProcessor::is_cluster_resource_avail(const ObString &cluster_name, const int64_t cluster_id/*0*/)
{
  bool bret = false;
  ObClusterResource *cr = acquire_cluster_resource(cluster_name, cluster_id);
  if (NULL != cr) {
    if (cr->is_avail()) {
      bret = true;
    }
    cr->dec_ref();
    cr = NULL;
  }
  return bret;
}

int ObResourcePoolProcessor::get_cluster_resource(
    ObContinuation &cont,
    process_cr_pfn process_resource_pool,
    const bool is_proxy_mysql_client,
    const ObString &cluster_name,
    const int64_t cluster_id,
    ObAction *&action)
{
  UNUSED(is_proxy_mysql_client);
  int ret = OB_SUCCESS;
  ObClusterResource *cr = NULL;
  action = NULL;
  if (OB_ISNULL(process_resource_pool) || OB_UNLIKELY(cluster_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(cluster_name), K(ret));
  } else if (OB_MYSQL_ROUTING_MODE == ObProxyConfig::get_routing_mode(get_global_proxy_config().server_routing_mode)) {
    default_cr_->inc_ref();
    cr = default_cr_;
  } else {
    cr = acquire_avail_cluster_resource(cluster_name, cluster_id);
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(cr)) {
      LOG_INFO("fail to acuqire avail cluster resource in local, will schedule task to"
               " create and init new cluster resource" , K(cluster_name), K(cluster_id));
      if (OB_FAIL(init_cluster_resource_cont(cont, cluster_name, cluster_id, action))) {
        LOG_WARN("fail to schedule cluster resource", K(&cont), K(cluster_name), K(cluster_id), K(ret));
      } else if (NULL == action) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("action must not be NULL", K(action), K(ret));
      }
    } else { // found in local
      LOG_DEBUG("succ to get avail cluster resource in local", KPC(cr));
      ret = (cont.*process_resource_pool)(cr);
    }
  }
  return ret;
}

int ObResourcePoolProcessor::init_cluster_resource_cont(
    ObContinuation &cont,
    const ObString &cluster_name,
    const int64_t cluster_id,
    ObAction *&action)
{
  int ret = OB_SUCCESS;
  ObClusterResourceCreateCont *cr_cont = NULL;
  if (OB_UNLIKELY(cluster_name.empty()) || OB_ISNULL(cont.mutex_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(&cont), K(cluster_name), K(ret));
  } else if (OB_UNLIKELY(&self_ethread() != cont.mutex_->thread_holding_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current thread is not equal with thread which holds sm mutex", K(ret));
  } else if (get_global_hot_upgrade_info().is_graceful_exit_timeout(get_hrtime())) {
    ret = OB_SERVER_IS_STOPPING;
    LOG_WARN("proxy need exit now", K(ret));
  } else if (OB_ISNULL(cr_cont = op_alloc_args(
          ObClusterResourceCreateCont, *this, &cont, &self_ethread()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc ObClusterResourceCreateCont", K(ret));
  } else if (OB_FAIL(cr_cont->create_cluster_resource(cluster_name, cluster_id, action))) {
    LOG_WARN("fail to create cluster resource", K(cluster_name), K(cluster_id), K(ret));
  }

  if (OB_FAIL(ret) && (NULL != cr_cont)) {
    cr_cont->destroy();
    cr_cont = NULL;
    action = NULL;
  }

  return ret;
}

int ObResourcePoolProcessor::create_cluster_resource(
    const ObString &cluster_name,
    const int64_t cluster_id,
    ObClusterResource *&created_cr,
    ObContinuation &cont)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(cluster_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(cluster_name), K(ret));
  } else {
    ObClusterResource *cr = NULL;
    ObClusterResource *new_cr = NULL;
    ObClusterInfoKey cluster_info_key(cluster_name, cluster_id);
    {
      CWLockGuard guard(cr_map_rwlock_); // write lock need
      cr_map_.get_refactored(cluster_info_key, cr); // check again
      if (NULL == cr) {
        int64_t cr_version = acquire_cluster_resource_version();
        LOG_INFO("will create new cluster resource", K(cluster_name), K(cluster_id), K(cr_version));
        if (OB_ISNULL(new_cr = op_alloc(ObClusterResource))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc mem for cluster resource", K(new_cr), K(ret));
        } else {
          new_cr->inc_ref();
          if (OB_FAIL(new_cr->init(cluster_name, cluster_id, cr_version))) {
            LOG_WARN("fail to init cluster resource", K(cluster_name), K(cluster_id), K(cr_version), K(ret));
          } else {
            if (OB_FAIL(cr_map_.unique_set(new_cr))) {
              LOG_ERROR("fail to add cluster resource, already exist, never happen",
                        K(cluster_name), K(cluster_id), K(ret));
            } else {
              // begin to init, and it will dec_ref in handle_chain_inform_cont();
              new_cr->inc_ref();
              new_cr->set_initing_state();
              cr = new_cr;
            }
          }

          if (OB_FAIL(ret) && (NULL != new_cr)) {
            new_cr->dec_ref();
            new_cr = NULL;
          }
        }
      } else { // this clsuter resource is building
        cr = NULL;
      }
    }

    // no need lock
    if (OB_SUCC(ret) && (NULL != cr)) {
      ObProxyMutex *mutex_ = &self_ethread().get_mutex();
      RESOURCE_POOL_INCREMENT_DYN_STAT(CREATE_CLUSTER_RESOURCE_COUNT);
      created_cr = cr;
      ObClusterResourceCreateCont *cr_cont = static_cast<ObClusterResourceCreateCont *>(&cont);
      if (OB_FAIL(cr_cont->build())) {
        // if build failed :
        // 1. set init failed state
        // 2. remove the cr from cr_map
        LOG_WARN("fail to begin build cluster resource", K(cluster_name), K(cluster_id), K(ret));
        CWLockGuard guard(cr_map_rwlock_); // write lock need
        cr->set_init_failed_state();
        // do not dec ref, leave it to handle_chain_inform_cont
        ObClusterResource *tmp_cr = cr_map_.remove(cluster_info_key);
        if (cr != tmp_cr) {
          LOG_ERROR("removed cluster resource must be equal with created cluster resource",
                    KPC(cr), KPC(tmp_cr), K(ret));
        }
      }
    }
  }

  return ret;
}

int ObResourcePoolProcessor::update_config_param()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("resource pool processor is not inited", K_(is_inited), K(ret));
  } else if (!config_.update(get_global_proxy_config())) {
    LOG_INFO("no need to update config param", K_(config), K(ret));
  } else {
    const int64_t server_state_refresh_interval = config_.server_state_refresh_interval_;
    const int64_t metadb_server_state_refresh_interval = config_.metadb_server_state_refresh_interval_;
    const int64_t mysql_client_timeout_ms = (config_.short_async_task_timeout_ / 1000);
    const int64_t detect_server_state_refresh_interval = config_.server_detect_refresh_interval_;
    ObCongestionControlConfig *control_config = NULL;
    if (OB_ISNULL(control_config = op_alloc(ObCongestionControlConfig))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate memory for congestion control config");
    } else {
      control_config->fail_window_sec_ = usec_to_sec(config_.congestion_fail_window_us_);
      control_config->conn_failure_threshold_ = config_.congestion_failure_threshold_;
      control_config->alive_failure_threshold_ = config_.congestion_failure_threshold_;
      control_config->min_keep_congestion_interval_sec_ = usec_to_sec(config_.min_keep_congestion_interval_us_);
      control_config->retry_interval_sec_ = usec_to_sec(config_.congestion_retry_interval_us_);
      control_config->inc_ref();

      CRLockGuard guard(cr_map_rwlock_); // read lock needed
      ObCRHashMap::iterator last = cr_map_.end();
      for (ObCRHashMap::iterator cr_iter = cr_map_.begin(); (cr_iter != last) && (OB_SUCC(ret)); ++cr_iter) {
        if (cr_iter->is_avail()) {
          bool is_metadb = (0 == cr_iter->cluster_info_key_.cluster_name_.get_string().case_compare(OB_META_DB_CLUSTER_NAME));
          if (OB_FAIL(cr_iter->congestion_manager_.update_congestion_config(control_config))) {
            LOG_WARN("fail to update congestion config", K(cr_iter->cluster_info_key_), K(ret));
          } else if ((NULL != cr_iter->ss_refresh_cont_)
            && !is_metadb
            && OB_FAIL(cr_iter->ss_refresh_cont_->set_server_state_refresh_interval(server_state_refresh_interval))) {
            LOG_WARN("fail to update server state refresh task interval", K(cr_iter->cluster_info_key_), K(ret));
          } else if ((NULL != cr_iter->ss_refresh_cont_)
            && is_metadb
            && OB_FAIL(cr_iter->ss_refresh_cont_->set_server_state_refresh_interval(metadb_server_state_refresh_interval))) {
            LOG_WARN("fail to update metadb state refresh task interval", K(cr_iter->cluster_info_key_), K(ret));
          } else if (OB_FAIL(cr_iter->mysql_proxy_.set_timeout_ms(mysql_client_timeout_ms))) {
            LOG_WARN("fail to update mysql proxy timeout", K(mysql_client_timeout_ms), K(ret));
          } else if ((NULL != cr_iter->ss_refresh_cont_)
            && !is_metadb
            && OB_FAIL(cr_iter->detect_server_state_cont_->set_detect_server_state_interval(detect_server_state_refresh_interval))) {
            LOG_WARN("fail to set detect server state interval", K(ret));
          }
        }
      } // end for
      control_config->dec_ref();
      control_config = NULL;
    }
  }
  return ret;
}

int ObResourcePoolProcessor::acquire_all_avail_cluster_resource(ObIArray<ObClusterResource *> &cr_array)
{
  int ret = OB_SUCCESS;
  cr_array.reset();
  {
    CRLockGuard guard(cr_map_rwlock_); // read lock needed
    ObCRHashMap::iterator last = cr_map_.end();
    for (ObCRHashMap::iterator cr_iter = cr_map_.begin(); (cr_iter != last) && (OB_SUCC(ret)); ++cr_iter) {
      if (cr_iter->is_avail()) {
        if (OB_FAIL(cr_array.push_back(cr_iter.value_))) {
          LOG_WARN("fail to push back", K(ret));
        } else {
          cr_iter->inc_ref();
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    for (int64_t i = 0; i < cr_array.count(); ++i) {
      cr_array.at(i)->dec_ref();
    }
    cr_array.reset();
  }

  return ret;
}

int ObResourcePoolProcessor::acquire_all_cluster_resource(common::ObIArray<ObClusterResource *> &cr_array)
{
  int ret = OB_SUCCESS;
  cr_array.reset();
  {
    CRLockGuard guard(cr_map_rwlock_); // read lock needed
    ObCRHashMap::iterator last = cr_map_.end();
    for (ObCRHashMap::iterator cr_iter = cr_map_.begin(); (cr_iter != last) && (OB_SUCC(ret)); ++cr_iter) {
      if (OB_FAIL(cr_array.push_back(cr_iter.value_))) {
        LOG_WARN("fail to push back", K(ret));
      } else {
        cr_iter->inc_ref();
      }
    }
  }

  if (OB_FAIL(ret)) {
    for (int64_t i = 0; i < cr_array.count(); ++i) {
      cr_array.at(i)->dec_ref();
    }
    cr_array.reset();
  }

  return ret;
}

int ObResourcePoolProcessor::get_recently_accessed_cluster_info(char *info_buf,
    const int64_t buf_len, int64_t &count)
{
  int ret = OB_SUCCESS;
  count = 0;
  static const int64_t MAX_RECENTLY_ACCESSED_MINUTES = 15;
  static const char COMMA_STR = ',';
  static const int64_t MARK_LENGTH = 1;

  if (OB_ISNULL(info_buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(info_buf), K(buf_len), K(ret));
  } else {
    int64_t name_length = 0;
    int64_t pos = 0;

    //1. append info stmt
    CRLockGuard guard(cr_map_rwlock_);
    ObCRHashMap::iterator last = cr_map_.end();
    for (ObCRHashMap::iterator cr_iter = cr_map_.begin(); (cr_iter != last) && (OB_SUCC(ret)); ++cr_iter) {
      if ((get_hrtime() - cr_iter->last_access_time_ns_) <= HRTIME_MINUTES(MAX_RECENTLY_ACCESSED_MINUTES)) {
        // when cluster is available && be accessed in 15min, we will report it
        name_length = cr_iter->get_cluster_name().length();
        if ((pos + MARK_LENGTH + name_length) >= buf_len) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("info buf is not enough to hold these cluster name", K(COMMA_STR), "cluster_name", cr_iter->get_cluster_name(),
                   K(pos), K(name_length), K(MARK_LENGTH), K(buf_len), K(ret));
        } else {
          if (0 != count) {// if not the first name, we need append ','
            info_buf[pos++] = COMMA_STR;
          }
          MEMCPY(info_buf + pos, cr_iter->get_cluster_name().ptr(), name_length);
          pos += name_length;
          ++count;
        }
      } else {}//do nothing
    }//end of for

    //2. append '\0'
    if (OB_SUCC(ret)) {
      info_buf[pos] = '\0';
    }
  }
  return ret;
}

int ObResourcePoolProcessor::set_first_cluster_name(const ObString &cluster_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cluster_name.empty())
      || OB_UNLIKELY(cluster_name.length() > OB_MAX_USER_NAME_LENGTH_STORE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(cluster_name), K(ret));
  } else {
    int64_t len = cluster_name.length();
    MEMCPY(first_cluster_name_, cluster_name.ptr(), len);
    first_cluster_name_str_.assign_ptr(first_cluster_name_,
                                       static_cast<ObString::obstr_size_t>(len));
  }
  return ret;
}

int ObResourcePoolProcessor::get_first_cluster_name(ObString &cluster_name) const
{
  int ret = OB_SUCCESS;
  cluster_name.reset();
  if (first_cluster_name_str_.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    cluster_name = first_cluster_name_str_;
  }
  return ret;
}

int ObResourcePoolProcessor::delete_cluster_resource(const ObString &cluster_name, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cluster_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("clsuter name can not be empty", K(cluster_name), K(ret));
  } else if (cluster_name == ObString::make_string(OB_PROXY_DEFAULT_CLUSTER_NAME)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("default cluster resource can not be deleted", K(cluster_name), K(ret));
  } else {
    ObClusterInfoKey cluster_info_key(cluster_name, cluster_id);
    ObClusterResource *cr = NULL;
    {
      CWLockGuard guard(cr_map_rwlock_); // write lock need
      cr = cr_map_.remove(cluster_info_key);
      if (NULL == cr) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("the cluster resource does not exist, can not delete", K(cluster_info_key), K(ret));
      } else if (OB_UNLIKELY(!cr->is_avail())) {
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("only avail cluster resource can be deleted, will set it back to map", KPC(cr), K(cr), K(ret));
        int tmp_ret = OB_SUCCESS;
        if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = cr_map_.unique_set(cr)))) {
          LOG_ERROR("fail to set cr back into map, memory will leak", KPC(cr), K(cr), K(tmp_ret));
          cr = NULL;
          ret = tmp_ret;
        } else {
          cr = NULL;
        }
      } else {
        cr->set_deleting_state();
      }
    }
    if (OB_SUCC(ret) && OB_LIKELY(NULL != cr)) {
      LOG_INFO("this cluster resource will be deleted, and wait to destroy", K(cr), KPC(cr));
      ObProxyMutex *mutex_ = &self_ethread().get_mutex();
      RESOURCE_POOL_DECREMENT_DYN_STAT(CURRENT_CLUSTER_RESOURCE_COUNT);
      RESOURCE_POOL_INCREMENT_DYN_STAT(DELETE_CLUSTER_RESOURCE_COUNT);
      if (OB_FAIL(cr->stop_refresh_server_state())) {
        LOG_WARN("fail to stop refresh server state", K(ret));
      } else if (OB_FAIL(cr->stop_detect_server_state())) {
        LOG_WARN("fail to stop detect server state", K(ret));
      } else {
        // push to every work thread
        int64_t thread_count = g_event_processor.thread_count_for_type_[ET_CALL];
        ObEThread **threads = g_event_processor.event_thread_[ET_CALL];
        ObCacheCleaner *cleaner = NULL;
        ObEThread *ethread = NULL;
        ObResourceDeleteActor *actor = NULL;
        for (int64_t i = 0; (i < thread_count) && OB_SUCC(ret); ++i) {
          if (OB_ISNULL(ethread = threads[i]) || OB_ISNULL(cleaner = threads[i]->cache_cleaner_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("ethread and cache cleaner can not be NULL", K(ethread), K(cleaner), K(ret));
          } else if (OB_ISNULL(actor = ObResourceDeleteActor::alloc(cr))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_ERROR("fail to alloc ObResourceDeleteActor", K(ret));
          } else if (OB_FAIL(cleaner->push_deleting_cr(actor))) {
            LOG_WARN("fail to push delete cr delete actor", K(actor), K(ret));
          } else if (OB_FAIL(cleaner->trigger())) { // trigger cleaner to wrok
            LOG_WARN("fail to trigger", K(ret));
          }
        }
      }
      cr->dec_ref();
      cr = NULL;
    }
  }
  return ret;
}

int ObResourcePoolProcessor::expire_cluster_resource()
{
  int ret = OB_SUCCESS;
  int64_t cr_count_water_mark = get_global_proxy_config().cluster_count_high_water_mark;
  int64_t expired_time_us = get_global_proxy_config().cluster_expire_time;  // us
  ObClusterResource *cr = NULL;

  // expire cluster resource by cr_expired_time
  int64_t max_expired_count = 10;  // max count to be expired by expire_time
  if (expired_time_us > 0) {
    while (OB_SUCC(ret)
           && max_expired_count > 0
           && NULL != (cr = acquire_last_used_cluster_resource())
           && cr->is_expired(HRTIME_USECONDS(expired_time_us))) {
      LOG_INFO("cluster resource has been idle for long time, will be deleted",
               "expire_time(us)", expired_time_us, KPC(cr), K(cr));
      if (OB_FAIL(delete_cluster_resource(cr->cluster_info_key_.cluster_name_.config_string_, cr->cluster_info_key_.cluster_id_))) {
        LOG_WARN("fail to delete cluster resource", K(cr), KPC(cr), K(ret));
      } else {
        --max_expired_count;
      }
      cr->dec_ref();
      cr = NULL;
    }

    if (NULL != cr) {
      cr->dec_ref();
      cr = NULL;
    }
  }

  // expire cluster resource by cr_count_water_mark
  int64_t deleting_cr_count = cr_map_.count() - cr_count_water_mark;
  for (int64_t i = 0; i < deleting_cr_count; ++i) {
    cr = acquire_last_used_cluster_resource();
    if (NULL != cr) {
      LOG_INFO("expire_cluster_resource, this cluster resourec will be deleted", KPC(cr), K(cr));
      if (OB_FAIL(delete_cluster_resource(cr->cluster_info_key_.cluster_name_.config_string_, cr->cluster_info_key_.cluster_id_))) {
        LOG_WARN("fail to delete cluster resource", K(cr), KPC(cr), K(ret));
        ret = OB_SUCCESS; // continue;
      }
      cr->dec_ref();
      cr = NULL;
    }
  }

  return ret;
}

ObClusterResource *ObResourcePoolProcessor::acquire_last_used_cluster_resource()
{
  CRLockGuard guard(cr_map_rwlock_);
  ObClusterResource *cr = NULL;
  ObCRHashMap::iterator last = cr_map_.end();
  int64_t last_access_time_ns = INT64_MAX;
  for (ObCRHashMap::iterator cr_iter = cr_map_.begin(); (cr_iter != last); ++cr_iter) {
    if ((cr_iter->last_access_time_ns_ < last_access_time_ns)
        && !cr_iter->is_metadb_cluster_resource()
        && !cr_iter->is_default_cluster_resource()
        && cr_iter->is_avail()) {
      cr = &(*cr_iter);
      last_access_time_ns = cr_iter->last_access_time_ns_;
    }
  }
  if (NULL != cr) {
    cr->inc_ref();
  }
  return cr;
}

int ObResourcePoolProcessor::rebuild_metadb(const bool ignore_cluster_not_exist/*false*/)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(delete_cluster_resource(ObString::make_string(OB_META_DB_CLUSTER_NAME)))) {
    LOG_WARN("fail to delete metadb", K(ret));
  }

  const bool enable_build_metadb = (NULL == meta_client_proxy_ || meta_client_proxy_->is_inited());
  if (enable_build_metadb && (OB_SUCC(ret) || (ignore_cluster_not_exist && OB_ENTRY_NOT_EXIST == ret))){
    if (NULL != meta_client_proxy_) {
      meta_client_proxy_->destroy_client_pool();
    }
    if (OB_FAIL(ObMetadbCreateCont::create_metadb(meta_client_proxy_))) {
      LOG_WARN("fail to create metadb", K(ret));
    }
  }
  return ret;
}

int ObResourcePoolProcessor::add_cluster_delete_task(const ObString &cluster_name, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  ObClusterDeleteCont *cont = NULL;
  if (OB_FAIL(ObClusterDeleteCont::alloc(cluster_name, cluster_id, cont))) {
    LOG_WARN("fail to alloc ObClusterDeleteCont", K(cluster_name), K(cluster_id), K(ret));
  } else if (OB_ISNULL(g_event_processor.schedule_imm(cont, ET_CALL))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to schedule metadb rebuild task", K(ret));
  }
  if (OB_FAIL(ret) && OB_LIKELY(NULL != cont)) {
    cont->destroy();
    cont = NULL;
  }
  return ret;
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
