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
#include "obutils/ob_proxy_create_server_conn_cont.h"
#include "lib/ob_errno.h"
#include "lib/encrypt/ob_encrypted_helper.h"
#include "obutils/ob_resource_pool_processor.h"
#include "obutils/ob_config_server_processor.h"
#include "proxy/client/ob_mysql_proxy.h"
#include "proxy/client/ob_client_vc.h"
#include "proxy/mysql/ob_mysql_global_session_manager.h"
#include "proxy/mysql/ob_mysql_transact.h"
#include "obutils/ob_session_pool_processor.h"


using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::dbconfig;
using namespace oceanbase::common;
using namespace oceanbase::share;
namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
ObProxyCreateServerConnCont::ObProxyCreateServerConnCont(event::ObProxyMutex *m, int64_t interval_us)
  : obutils::ObAsyncCommonTask(m, "proxy_conn_create_task"),
    reentrancy_count_(0), retry_count_(INT32_MAX), conn_count_(0), create_count_(0),
    refresh_interval_us_(interval_us), schema_key_conn_info_(NULL), cr_(NULL), proxy_(NULL)
{
  SET_HANDLER(&ObProxyCreateServerConnCont::main_handler);
}

ObProxyCreateServerConnCont::~ObProxyCreateServerConnCont()
{
  if (NULL != schema_key_conn_info_) {
    op_free(schema_key_conn_info_);
    schema_key_conn_info_ = NULL;
  }
  if (NULL != proxy_) {
    op_free(proxy_);
    proxy_ = NULL;
  }
  if (NULL != cr_) {
    cr_->dec_ref();
    cr_ = NULL;
  }
}
const char* ObProxyCreateServerConnCont::get_event_name(const int64_t event)
{
  const char *name = "UNKNOWN_EVENT";
  switch (event) {
  case CONN_ENTRY_GET_ONE_CONN_INFO_EVENT:
    name = "CONN_ENTRY_GET_ONE_CONN_INFO_EVENT";
    break;
  case CONN_ENTRY_CREATE_CLUSTER_RESOURCE_EVENT: {
    name = "CONN_ENTRY_CREATE_CLUSTER_RESOURCE_EVENT";
    break;
  }
  case CLUSTER_RESOURCE_CREATE_COMPLETE_EVENT: {
    name = "CLUSTER_RESOURCE_CREATE_COMPLETE_EVENT";
    break;
  }
  case CONN_ENTRY_CREATE_SERVER_SESSION_EVENT: {
    name = "CONN_ENTRY_CREATE_SERVER_SESSION_EVENT";
    break;
  }
  case CLIENT_TRANSPORT_MYSQL_RESP_EVENT: {
    name = "CLIENT_TRANSPORT_MYSQL_RESP_EVENT";
    break;
  }
  default: {
    break;
  }
  }
  return name;
}
int ObProxyCreateServerConnCont::handle_get_one_conn_info()
{
  int ret = OB_SUCCESS;
  while (true) {
    // free last schema_key_conn_info_
    if (NULL != schema_key_conn_info_) {
      op_free(schema_key_conn_info_);
      schema_key_conn_info_ = NULL;
    }
    if (!get_global_proxy_config().is_pool_mode) {
      ret = OB_ITER_END;
      break;
    } else if (NULL == (schema_key_conn_info_ = get_global_server_conn_job_list().pop())) {
      ret = OB_ITER_END;
      break;
    } else {
      int32_t list_count = get_global_server_conn_job_list().count();
      ObCommonAddr& addr = schema_key_conn_info_->addr_;
      ObProxySchemaKey& schema_key = schema_key_conn_info_->schema_key_;
      int64_t min_count = ObMysqlSessionUtils::get_session_min_conn(schema_key);
      int64_t cur_count = get_global_session_manager().get_current_session_conn_count(
                            schema_key.dbkey_.config_string_,
                            addr);
      create_count_ = 0;
      if (cur_count >= min_count) {
        // reach min no need create
        continue;
      }
      conn_count_ = min_count - cur_count;
      LOG_DEBUG("get one conn", K(schema_key.dbkey_), K(addr),
                K(list_count), K(create_count_), K(conn_count_), K(cur_count), K(min_count));
      break;
    }
  }
  return ret;
}

int ObProxyCreateServerConnCont::main_handler(int event, void *data)
{
  int ret = OB_SUCCESS;
  ++reentrancy_count_;
  ObProxySchemaKey schema_key;
  common::DBServerType server_type = DB_MAX;
  // LOG_DEBUG("main_handler receive event", "event", get_event_name(event), K(data), K_(reentrancy_count));
  switch (event) {
  case CONN_ENTRY_GET_ONE_CONN_INFO_EVENT:
    pending_action_ = NULL;
    ret = handle_get_one_conn_info();
    if (ret == OB_ITER_END) {
      // end one loop, schedule next loop
      ret = schedule_create_conn_cont();
    } else if (OB_FAIL(ret)) {
      LOG_WARN("handle_get_one_conn_info failed", K(ret));
    } else {
      schema_key = schema_key_conn_info_->schema_key_;
      server_type = schema_key.get_db_server_type();
      if (DB_MYSQL == server_type) {
        //mysql no need create cluster
        if (OB_ISNULL(pending_action_ = self_ethread().schedule_imm(this,
            CONN_ENTRY_CREATE_SERVER_SESSION_EVENT))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to schedule create session", K(schema_key));
        }
      } else if ((DB_OB_MYSQL == server_type || DB_OB_ORACLE == server_type)) { 
        if (OB_FAIL(handle_create_cluster_resource())) {
          LOG_WARN("fail to handle_create_cluster_resource", K(ret));
        }
      } else {
        LOG_WARN("unexpected type", K(server_type), K(schema_key_conn_info_));
        ret = OB_ERR_UNEXPECTED;
      }
    }
    break;
  case CONN_ENTRY_CREATE_CLUSTER_RESOURCE_EVENT: {
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
  case CONN_ENTRY_CREATE_SERVER_SESSION_EVENT: {
    pending_action_ = NULL;
    if (OB_FAIL(do_create_server_conn())) {
      LOG_WARN("fail to create server conn", K(ret));
    } else if (OB_FAIL(handle_create_session())) {
      LOG_WARN("handle create session fail", K(ret));
    } else {
      LOG_DEBUG("handle create session succ");
    }
    break;
  }
  case CLIENT_TRANSPORT_MYSQL_RESP_EVENT: {
    pending_action_ = NULL;
    schema_key = schema_key_conn_info_->schema_key_;
    if (OB_FAIL(handle_client_resp(data))) {
      LOG_WARN("fail to handle client resp", K(ret));
      get_global_session_manager().incr_fail_count(schema_key.dbkey_.config_string_,
          schema_key_conn_info_->addr_);
    } else {
      get_global_session_manager().reset_fail_count(schema_key.dbkey_.config_string_, schema_key_conn_info_->addr_);
      if (create_count_ < conn_count_) {
        if (OB_ISNULL(pending_action_ = self_ethread().schedule_imm(this, CONN_ENTRY_CREATE_SERVER_SESSION_EVENT, NULL))) {
          LOG_WARN("fail to schedule create conn", K(ret));
        } else {
          LOG_DEBUG("continue create server conn", K(create_count_), K(conn_count_),
            K(schema_key_conn_info_->addr_), K(schema_key.dbkey_.config_string_));
        }
      } else {
        // continue handle next one
        if (OB_ISNULL(pending_action_ = self_ethread().schedule_imm(this, CONN_ENTRY_GET_ONE_CONN_INFO_EVENT, NULL))) {
          LOG_WARN("fail to schedule get conn", K(ret));
        }
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
    // may be fail in handle one
    LOG_INFO("failed in handle one", K(ret));
    if (OB_FAIL(cancel_pending_action())) {
      LOG_WARN("fail to cancel_pending_action", K(ret));
    }
    if (OB_ISNULL(pending_action_ = self_ethread().schedule_imm(this, CONN_ENTRY_GET_ONE_CONN_INFO_EVENT, NULL))) {
      LOG_WARN("fail to schedule get conn", K(ret));
    }
  }
  if (terminate_ && (1 == reentrancy_count_)) {
    destroy();
  } else {
    --reentrancy_count_;
    if (OB_UNLIKELY(reentrancy_count_ < 0)) {
      LOG_ERROR("invalid reentrancy_count", K_(reentrancy_count));
    }
  }
  return ret;
}

int ObProxyCreateServerConnCont::handle_create_session() {
  int ret = OB_SUCCESS;
  char sql[OB_SHORT_SQL_LENGTH];
  sql[0] = '\0';
  snprintf(sql, OB_SHORT_SQL_LENGTH, "select 1 from dual");
  const ObMysqlRequestParam request_param(sql);
  LOG_DEBUG("handle create session sql is ", K(sql));
  if (OB_FAIL(proxy_->async_read(this, request_param, pending_action_))) {
    LOG_WARN("fail to nonblock read", K(sql), K(ret));
  }
  return ret;
}
int ObProxyCreateServerConnCont::handle_select_value_resp(ObResultSetFetcher &rs_fetcher) {
  int ret = OB_SUCCESS;
  if (OB_SUCC(rs_fetcher.next())) {
    int tmp_real_str_len = 0;
    char test_buf[10];
    PROXY_EXTRACT_STRBUF_FIELD_MYSQL(rs_fetcher, "1", test_buf, 10, tmp_real_str_len);
    LOG_DEBUG("handle select value", K(test_buf));
  }
  return ret;
}
int ObProxyCreateServerConnCont::handle_client_resp(void *data) {
  int ret = OB_SUCCESS;
  ++create_count_;
  if (NULL != data) {
    ObClientMysqlResp *resp = reinterpret_cast<ObClientMysqlResp *>(data);
    ObResultSetFetcher *rs_fetcher = NULL;
    if (resp->is_resultset_resp()) {
      if (OB_FAIL(resp->get_resultset_fetcher(rs_fetcher))) {
        LOG_WARN("fail to get resultset fetcher", K(ret));
      } else if (OB_ISNULL(rs_fetcher)) {
        LOG_WARN("resultset fetcher is NULL", K(ret));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(handle_select_value_resp(*rs_fetcher))) {
        LOG_WARN("fail to get value", K(ret));
      }
    } else {
      const int64_t error_code = resp->get_err_code();
      LOG_WARN("fail to get resp from remote", K(error_code));
      ret = OB_ERR_UNEXPECTED;
    }
    op_free(resp);
    resp = NULL;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("handle_client_resp fail ", K(ret));
  }
  if (NULL != proxy_) {
    op_free(proxy_);
    proxy_ = NULL;
  }
  return ret;
}

int ObProxyCreateServerConnCont::handle_create_cluster_resource()
{
  int ret = OB_SUCCESS;
  ObResourcePoolProcessor &rp_processor = get_global_resource_pool_processor();
  ObProxySchemaKey& schema_key = schema_key_conn_info_->schema_key_;
  ObString cluster_name = schema_key.get_cluster_name();
  if (OB_FAIL(rp_processor.get_cluster_resource(*this,
              (process_async_task_pfn)&ObProxyCreateServerConnCont::handle_create_cluster_resource_complete,
              false, cluster_name, OB_DEFAULT_CLUSTER_ID, pending_action_))) {
    LOG_WARN("fail to get cluster resource", "cluster name", cluster_name, K(ret));
  } else if (NULL == pending_action_) { // created succ
    LOG_INFO("cluster resource was created by others, no need create again");
  }
  return ret;
}

int ObProxyCreateServerConnCont::handle_create_cluster_resource_complete(void *data)
{
  int ret = OB_SUCCESS;
  ObProxySchemaKey& schema_key = schema_key_conn_info_->schema_key_;
  if (OB_ISNULL(data)) {
    if (retry_count_ > 0) {
      --retry_count_;
      LOG_INFO("fail to create cluste resource, will retry", "remain retry count", retry_count_,
               K(schema_key.get_cluster_name()));
      if (OB_ISNULL(pending_action_ = self_ethread().schedule_in(
                                        this, HRTIME_MSECONDS(RETRY_INTERVAL_MS),
                                        CONN_ENTRY_CREATE_CLUSTER_RESOURCE_EVENT))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to schedule fetch rslist task", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to create cluste resource, no chance retry", K(data),
               K(schema_key.get_cluster_name()));
    }
  } else {
    ObClusterResource *cr = reinterpret_cast<ObClusterResource *>(data);
    LOG_INFO("cluster create succ", K(cr), KPC(cr), K(retry_count_));
    cr_ = cr;
    retry_count_ = INT32_MAX;
  }
  if (OB_SUCC(ret) && OB_FAIL(handle_event(CONN_ENTRY_CREATE_SERVER_SESSION_EVENT))) {
    LOG_WARN("fail to schedule");
  }
  return ret;
}
int ObProxyCreateServerConnCont::schedule_create_conn_cont(bool imm)
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
      if (OB_ISNULL(g_event_processor.schedule_imm(this, ET_CALL, CONN_ENTRY_GET_ONE_CONN_INFO_EVENT))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to schedule create_server_conn event", K(ret));
      }
    } else {
      delay_us = ObRandomNumUtils::get_random_half_to_full(refresh_interval_us_);
      if (OB_UNLIKELY(delay_us <= 0)) {
        ret = OB_INNER_STAT_ERROR;
        LOG_WARN("delay must greater than zero", K(delay_us), K(ret));
      } else if (OB_ISNULL(pending_action_ = self_ethread().schedule_in(
          this, HRTIME_USECONDS(delay_us), CONN_ENTRY_GET_ONE_CONN_INFO_EVENT))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to schedule create_server_conn cont", K(delay_us), K(ret));
      }
    }
  }
  return ret;
}

int ObProxyCreateServerConnCont::do_create_server_conn()
{
  int ret = OB_SUCCESS;
  ObProxyLoginInfo login_info;
  ObProxySchemaKey& schema_key = schema_key_conn_info_->schema_key_;
  ObString user_name = schema_key.get_user_name();
  ObString tenant_name = schema_key.get_tenant_name();
  ObString cluster_name = schema_key.get_cluster_name();
  ObString database_name = schema_key.get_database_name();
  ObString password = schema_key.get_password();
  char user_tenant_name_buf[1024];
  common::DBServerType server_type = schema_key.get_db_server_type();
  if (DB_MYSQL == server_type) {
    login_info.username_.set_value(schema_key.get_full_user_name());
  } else if (DB_OB_ORACLE == server_type || DB_OB_MYSQL == server_type) {
    snprintf(user_tenant_name_buf, 1024, "%.*s@%.*s",
           user_name.length(), user_name.ptr(),
           tenant_name.length(), tenant_name.ptr());
    login_info.username_.set_value(user_tenant_name_buf);
  }
  login_info.password_.set_value(password);
  login_info.db_.set_value(database_name);
  char passwd_staged1_buf[ENC_STRING_BUF_LEN]; // 1B '*' + 40B octal num
  ObString passwd_string(ENC_STRING_BUF_LEN, passwd_staged1_buf);
  ClientPoolOption client_pool_option;
  client_pool_option.need_skip_stage2_ = false;
  client_pool_option.is_session_pool_client_ = true;
  client_pool_option.server_addr_ = schema_key_conn_info_->addr_;
  client_pool_option.schema_key_ = schema_key;
  int64_t timeout_ms = get_global_resource_pool_processor().config_.short_async_task_timeout_;
  if (OB_FAIL(ObEncryptedHelper::encrypt_passwd_to_stage1(login_info.password_, passwd_string))) {
    LOG_WARN("fail to encrypt_passwd_to_stage1", K(login_info), K(ret));
  } else {
    passwd_string += 1;//trim the head'*'
    const bool is_meta_mysql_client = true;
    ObMysqlProxy* proxy = op_alloc(ObMysqlProxy);
    if (proxy != NULL) {
      if (OB_FAIL(proxy->init(timeout_ms, login_info.username_, passwd_string, login_info.db_))) {
        LOG_WARN("fail to init proxy", K(login_info.username_), K(login_info.db_));
      } else if (DB_OB_ORACLE == server_type || DB_OB_MYSQL == server_type) {
        if (OB_FAIL(proxy->rebuild_client_pool(cr_, is_meta_mysql_client,
                         cluster_name, OB_DEFAULT_CLUSTER_ID, login_info.username_, passwd_string, login_info.db_, "", &client_pool_option))) {
          LOG_WARN("fail to create mysql client pool", K(login_info), K(ret));
        } else {
          LOG_DEBUG("succ to create ob client pool", K(login_info.username_), K(server_type),
            K(schema_key_conn_info_->addr_));
        }
      } else if (DB_MYSQL == server_type) {
        if (OB_FAIL(proxy->rebuild_client_pool(schema_key.shard_conn_,
              is_meta_mysql_client, user_name, passwd_string, database_name, "", &client_pool_option))) {
          LOG_WARN("fail to create mysql client pool", K(user_name), K(database_name), K(ret));
        } else {
          LOG_DEBUG("succ to create mysql clinet pool", K(login_info.username_), K(database_name), K(server_type),
            K(schema_key_conn_info_->addr_));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid type", K(server_type));
      }
    }
    if (OB_FAIL(ret)) {
      op_free(proxy);
      proxy = NULL;
    } else {
      if (proxy_ != NULL) {
        op_free(proxy_);
      }
      proxy_ = proxy;
    }
  }
  return ret;
}
} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
