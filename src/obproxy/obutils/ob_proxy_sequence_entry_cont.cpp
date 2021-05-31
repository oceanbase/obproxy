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
#include "obutils/ob_proxy_sequence_entry_cont.h"
#include "iocore/eventsystem/ob_thread.h"
#include "iocore/eventsystem/ob_continuation.h"
#include "lib/encrypt/ob_encrypted_helper.h"
#include "lib/time/ob_hrtime.h"
#include "obutils/ob_proxy_json_config_info.h"
#include "obutils/ob_proxy_sequence_utils.h"
#include "obutils/ob_metadb_create_cont.h"
#include "obutils/ob_resource_pool_processor.h"
#include "obutils/ob_config_server_processor.h"
#include "proxy/client/ob_mysql_proxy.h"
#include "proxy/client/ob_client_vc.h"
#include "utils/ob_ref_hash_map.h"
#include "utils/ob_proxy_utils.h"

using namespace obsys;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::dbconfig;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
#define MACRO_FOR_CASE_STATE(state) \
   case state: \
     name = #state; \
     break

ObProxySequenceEntryCont::ObProxySequenceEntryCont(ObContinuation *cb_cont, ObEThread* submit_thread)
  : ObAsyncCommonTask(cb_cont->mutex_, "sequence_entry_task", cb_cont, submit_thread),
    shard_conn_(NULL), mysql_proxy_entry_(NULL),  state_(LOOKUP_SEQUENCE_ENTRY_STATE),
    last_state_success_(true), need_insert_(false)
{
  magic_ = OB_SEQUENCE_ENTRY_CONT_MAGIC_ALIVE;
  SET_HANDLER(&ObProxySequenceEntryCont::main_handler);
}
int ObProxySequenceEntryCont::init(const ObSequenceRouteParam &param,
                                   ObSequenceEntry* sequence_entry,
                                   ObSequenceContType cont_type)
{
  int ret = OB_SUCCESS;
  action_.set_continuation(param.cont_);
  mysql_proxy_entry_ = NULL;
  mutex_ = param.cont_->mutex_;
  sequence_entry_ = sequence_entry;
  allow_insert_ = sequence_entry_->allow_insert_;
  sequence_info_ = sequence_entry->sequence_info_;
  only_need_db_timestamp_ = param.only_need_db_timestamp_;
  if (only_need_db_timestamp_) {
    state_ = LOOKUP_SEQUENCE_DBTIME_STATE;
  }
  need_db_timestamp_ = param.need_db_timestamp_;
  retry_time_ = 0;
  cont_type_ = cont_type;
  server_type_ = param.server_type_;
  if (NULL != param.shard_conn_) {
    param.shard_conn_->inc_ref();
    shard_conn_ = param.shard_conn_;
  }
  cluster_name_.set_value(param.cluster_name_);
  tenant_name_.set_value(param.tenant_name_);
  database_name_.set_value(param.database_name_);
  real_table_name_.set_value(param.table_name_);
  username_.set_value(param.username_);
  password_.set_value(param.password_);
  seq_name_.set_value(param.seq_name_);
  min_value_  = param.min_value_;
  max_value_ = param.max_value_;
  step_ = param.step_;
  max_retry_count_ = param.retry_count_;
  sequence_info_.is_over_ = false;
  return ret;
}

int ObProxySequenceEntryCont::do_create_oceanbase_sequence_entry()
{
  int ret = OB_SUCCESS;
  snprintf(proxy_id_buf_, 1024, "%.*s-@%.*s#%.*s-%.*s",
           username_.config_string_.length(), username_.config_string_.ptr(),
           tenant_name_.config_string_.length(), tenant_name_.config_string_.ptr(),
           cluster_name_.config_string_.length(), cluster_name_.config_string_.ptr(),
           database_name_.config_string_.length(), database_name_.config_string_.ptr());
  ObString proxy_id = ObString::make_string(proxy_id_buf_);
  if (get_global_proxy_config().enable_mysql_proxy_pool) {
    mysql_proxy_entry_ = get_global_mysql_proxy_cache().accquire_mysql_proxy_entry(proxy_id_buf_);
  }
  if (OB_ISNULL(mysql_proxy_entry_) || need_create_cluster_resource()) {
    LOG_DEBUG("need_create_cluster_resource, create now", K(proxy_id), K(sequence_info_.seq_id_));
    if (OB_ISNULL(pending_action_ = self_ethread().schedule_imm(this, SEQUENCE_ENTRY_CREATE_CLUSTER_RESOURCE_EVENT))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to schedule imm", K(ret), K(sequence_info_.seq_id_));
    }
  }
  return ret;
}

int ObProxySequenceEntryCont::do_create_normal_sequence_entry()
{
  int ret = OB_SUCCESS;

  snprintf(proxy_id_buf_, 1024, "%.*s-%.*s-%.*s-%.*s",
           username_.config_string_.length(), username_.config_string_.ptr(),
           shard_conn_->physic_addr_.config_string_.length(), shard_conn_->physic_addr_.config_string_.ptr(),
           shard_conn_->physic_port_.config_string_.length(), shard_conn_->physic_port_.config_string_.ptr(),
           database_name_.config_string_.length(), database_name_.config_string_.ptr());
  ObString proxy_id = ObString::make_string(proxy_id_buf_);
  if (get_global_proxy_config().enable_mysql_proxy_pool) {
    mysql_proxy_entry_ = get_global_mysql_proxy_cache().accquire_mysql_proxy_entry(proxy_id_buf_);
  }
  if (OB_ISNULL(mysql_proxy_entry_) && OB_FAIL(create_proxy(shard_conn_, username_))) {
    LOG_ERROR("fail to create proxy", K_(username), K(ret));
  }
  return ret;
}

int ObProxySequenceEntryCont::do_create_sequence_entry()
{
  int ret = OB_SUCCESS;

  if (DB_OB_MYSQL == server_type_ || DB_OB_ORACLE == server_type_) {
    if (OB_FAIL(do_create_oceanbase_sequence_entry())) {
      LOG_WARN("fail to create oceanbase sequence entry", K(ret));
    }
  } else {
    if (OB_FAIL(do_create_normal_sequence_entry())) {
      LOG_WARN("fail to create mysql sequence entry", K(ret));
    }
  }

  if (OB_SUCC(ret) && pending_action_ == NULL) {
    LOG_DEBUG("succ get proxy from cache", K(sequence_info_.seq_id_));
    if (OB_ISNULL(pending_action_ = self_ethread().schedule_imm(this, SEQUENCE_ENTRY_LOOKUP_REMOTE_EVENT))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to schedule imm", K(ret), K(sequence_info_.seq_id_));
    }
  }
  return ret;
}

bool ObProxySequenceEntryCont::need_create_cluster_resource() {
  bool need_create = true;
  if (OB_ISNULL(mysql_proxy_entry_)) {
    LOG_DEBUG("mysql_proxy_entry is NULL", K(sequence_info_.seq_id_));
  } else if (OB_UNLIKELY(OB_ISNULL(mysql_proxy_entry_->mysql_proxy_))) {
    LOG_WARN("mysql_proxy_entry_->mysql_proxy_ is NULL", K(sequence_info_.seq_id_));
  } else {
    ObMysqlClientPool* client_pool = mysql_proxy_entry_->mysql_proxy_-> acquire_client_pool();
    if (OB_UNLIKELY(OB_ISNULL(client_pool))) {
      LOG_WARN("client_pool is NULL", K(sequence_info_.seq_id_));
    } else {
      ObClusterResource *cluster_resource = dynamic_cast<ObClusterResource*>(client_pool->acquire_connection_param());
      if (OB_UNLIKELY(OB_ISNULL(cluster_resource))) {
        LOG_WARN("cluster_resource is NULL", K(sequence_info_.seq_id_));
      } else if (cluster_resource->is_avail()) {
        need_create = false;
        cluster_resource->dec_ref();
      }
      mysql_proxy_entry_->mysql_proxy_->release_client_pool(client_pool);
    }
  }
  LOG_DEBUG("left need_create_cluster_resource", K(need_create), K(sequence_info_.seq_id_));
  return need_create;
}

int ObProxySequenceEntryCont::handle_create_cluster_resouce()
{
  int ret = OB_SUCCESS;
  ObResourcePoolProcessor &rp_processor = get_global_resource_pool_processor();
  ObString cluster_name = cluster_name_.config_string_;

  if (OB_FAIL(rp_processor.get_cluster_resource(*this,
              (process_async_task_pfn)&ObProxySequenceEntryCont::handle_create_cluster_resource_complete,
              false, cluster_name, OB_DEFAULT_CLUSTER_ID, pending_action_))) {
    LOG_WARN("fail to get cluster resource", "cluster name", cluster_name, K(ret));
    last_state_success_ = false;
  } else if (NULL == pending_action_) { // created succ
    LOG_INFO("cluster  resource was created by others, no need create again", K(cluster_name));
  }
  return ret;
}

int ObProxySequenceEntryCont::handle_create_cluster_resource_complete(void *data)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("Entry handle_create_cluster_resource_complete()");
  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    last_state_success_ = false;
    LOG_WARN("handle_create_cluster_resource_complete get invalid argument", K(data), K(cluster_name_));
  } else {
    ObClusterResource *cr = reinterpret_cast<ObClusterResource *>(data);
    LOG_DEBUG("cluster create succ", K(cluster_name_), K(cr), KPC(cr));

    char user_tenant_name_buf[1024];
    snprintf(user_tenant_name_buf, 1024, "%.*s@%.*s",
             username_.config_string_.length(), username_.config_string_.ptr(),
             tenant_name_.config_string_.length(), tenant_name_.config_string_.ptr());
    ObString full_user_name(user_tenant_name_buf);
    if (OB_FAIL(create_proxy(cr, full_user_name))) {
      LOG_WARN("fail to create proxy", KPC(cr), K(full_user_name), K(ret));
    }

    cr->dec_ref(); // free
    cr = NULL;
  }
  if (OB_FAIL(ret)) {
    last_state_success_ = false;
    submit_thread_->schedule_imm(this, SEQUENCE_ENTRY_CHAIN_NOTIFY_CALLER_EVENT);
  } else {
    submit_thread_->schedule_imm(this, SEQUENCE_ENTRY_LOOKUP_REMOTE_EVENT);
  }
  return ret;
}

int ObProxySequenceEntryCont::create_proxy(ObSharedRefCount *param, const ObString &username)
{
  int ret = OB_SUCCESS;

  int64_t timeout_ms = get_global_resource_pool_processor().config_.short_async_task_timeout_;
  ObMysqlProxy* proxy = op_alloc(ObMysqlProxy);
  char passwd_staged1_buf[ENC_STRING_BUF_LEN]; // 1B '*' + 40B octal num
  ObString passwd_string(ENC_STRING_BUF_LEN, passwd_staged1_buf);
  if (OB_FAIL(ObEncryptedHelper::encrypt_passwd_to_stage1(password_.config_string_, passwd_string))) {
    LOG_WARN("fail to encrypt_passwd_to_stage1", K(ret));
  } else {
    passwd_string += 1;//trim the head'*'
    if (OB_ISNULL(proxy)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc ObMysqlProxy");
    } else if (OB_FAIL(proxy->init(timeout_ms, username, passwd_string, database_name_))) {
      LOG_WARN("fail to init proxy", K(username), K_(database_name));
    } else if (OB_FAIL(rebuild_proxy(proxy, param, username, passwd_string))) {
      LOG_WARN("fail to rebuild client pool", K(username), K(ret));
    } else {
      LOG_DEBUG("rebuild_client_pool success", K(username), K_(database_name));
    }
  }
  if (OB_FAIL(ret)) {
    last_state_success_ = false;
    op_free(proxy);
    proxy = NULL;
  } else {
    if (mysql_proxy_entry_ != NULL) {
      mysql_proxy_entry_->dec_ref();
      mysql_proxy_entry_ = NULL;
    }
    mysql_proxy_entry_ = op_alloc(ObMysqlProxyEntry);
    if (OB_ISNULL(mysql_proxy_entry_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc ObMysqlProxyEntry", K(ret));
    } else {
      mysql_proxy_entry_->mysql_proxy_ = proxy;
      mysql_proxy_entry_->proxy_id_.set_value(proxy_id_buf_);
      mysql_proxy_entry_->inc_ref();// for local use
      if (get_global_proxy_config().enable_mysql_proxy_pool) {
        mysql_proxy_entry_->inc_ref(); // for add_map
        get_global_mysql_proxy_cache().add_mysql_proxy_entry(mysql_proxy_entry_);
      }
    }
  }
  return ret;
}

int ObProxySequenceEntryCont::rebuild_proxy(ObMysqlProxy *proxy, ObSharedRefCount *param,
                                            const ObString &username, const ObString &passwd)
{
  int ret = OB_SUCCESS;

  const bool is_meta_mysql_client = false;
  if (DB_OB_MYSQL == server_type_ || DB_OB_ORACLE == server_type_) {
    if (OB_FAIL(proxy->rebuild_client_pool(dynamic_cast<ObClusterResource*>(param), is_meta_mysql_client,
                       cluster_name_, OB_DEFAULT_CLUSTER_ID, username, passwd, database_name_))) {
      LOG_WARN("fail to create oceanbase client pool", K_(cluster_name), K(username),
               K_(database_name), K(ret));
    }
  } else {
    if (OB_FAIL(proxy->rebuild_client_pool(dynamic_cast<ObShardConnector*>(param), is_meta_mysql_client,
                       username, passwd, database_name_))) {
      LOG_WARN("fail to create mysql client pool", K(username), K_(database_name), K(ret));
    }
  }

  return ret;
}

void ObProxySequenceEntryCont::kill_this()
{
  LOG_DEBUG("ObProxySequenceEntryCont will be free", K(this));
  int ret = OB_SUCCESS;
  if (OB_FAIL(cancel_timeout_action())) {
    LOG_WARN("fail to cancel timeout action", K(ret));
  }
  if (OB_FAIL(cancel_pending_action())) {
    LOG_WARN("fail to cancel pending action", K(ret));
  }
  if (mysql_proxy_entry_ != NULL) {
    mysql_proxy_entry_->dec_ref();
    mysql_proxy_entry_ = NULL;
  }
  if (NULL != shard_conn_) {
    shard_conn_->dec_ref();
    shard_conn_ = NULL;
  }
  action_.set_continuation(NULL);
  submit_thread_ = NULL;
  magic_ = OB_SEQUENCE_ENTRY_CONT_MAGIC_DEAD;
  mutex_.release();
  op_free(this);
}
int ObProxySequenceEntryCont::select_old_value_from_remote()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("Enter select_old_value_from_remote()");
  ObMysqlProxy* mysql_proxy = mysql_proxy_entry_->mysql_proxy_;
  char sql[OB_SHORT_SQL_LENGTH];
  sql[0] = '\0';
  ObString& seq_name = sequence_info_.seq_name_.config_string_;
  ObString& tnt_id = sequence_info_.tnt_id_.config_string_;
  ObString& tnt_col = sequence_info_.tnt_col_.config_string_;
  if (OB_FAIL(ObProxySequenceUtils::get_sequence_entry_sql(sql, OB_SHORT_SQL_LENGTH,
              database_name_.config_string_,
              real_table_name_.config_string_,
              seq_name,
              tnt_id,
              tnt_col))) {
    last_state_success_ = false;
    LOG_WARN("fail to get sequence entry sql", K(sql), K(sequence_info_), K(ret));
  } else {
    LOG_DEBUG("get_sequence_entry_sql ", K(sql));
    const ObMysqlRequestParam request_param(sql);
    if (OB_FAIL(mysql_proxy->async_read(this, request_param, pending_action_))) {
      LOG_WARN("fail to nonblock read", K(sql), K(cluster_name_), K(seq_name), K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    ret = OB_SUCCESS;
    last_state_success_ = false;
    if (OB_FAIL(schedule_imm(this, SEQUENCE_ENTRY_FAIL_SCHEDULE_LOOKUP_REMOTE_EVENT))) {
      LOG_WARN("fail to schedule in", K(ret));
    }
  }
  return ret;
}

int ObProxySequenceEntryCont::update_new_value_to_remote()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("Enter update_new_value_to_remote");
  // todo: add new_value sql
  ObMysqlProxy* mysql_proxy = mysql_proxy_entry_->mysql_proxy_;
  char sql[OB_SHORT_SQL_LENGTH];
  sql[0] = '\0';
  if (!last_state_success_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update_new_value_to_remote last_state_success_ should true");
    return ret;
  }
  ObString& seq_name = sequence_info_.seq_name_.config_string_;
  ObString& tnt_id = sequence_info_.tnt_id_.config_string_;
  ObString& tnt_col = sequence_info_.tnt_col_.config_string_;

  if (need_insert_) { // when not exist insert a record with init value
    char now_time_buf[1024];
    ObProxySequenceUtils::get_nowtime_string(now_time_buf, 1024);
    ObString time_now_str(now_time_buf);
    // do insert
    if (OB_FAIL(ObProxySequenceUtils::insert_sequence_entry_sql(sql, OB_SHORT_SQL_LENGTH,
                database_name_.config_string_,
                real_table_name_.config_string_,
                seq_name,
                tnt_id,
                tnt_col,
                min_value_,
                max_value_,
                step_,
                min_value_ + step_,
                time_now_str))) {
      LOG_WARN("fail to get sequence entry sql", K(sql), K(sequence_info_), K(ret));
    } else {
      LOG_DEBUG("insert_sequence_entry_sql ", K(sql));
      const ObMysqlRequestParam request_param(sql);
      if (OB_FAIL(mysql_proxy->async_read(this, request_param, pending_action_))) {
        LOG_WARN("fail to nonblock read", K(sql), K(cluster_name_), K(seq_name), K(ret));
      } else {
        // set init value when insert
        sequence_info_.value_ = min_value_;
        sequence_info_.min_value_ = min_value_;
        sequence_info_.max_value_ = max_value_;
        sequence_info_.step_ = step_;
        sequence_info_.local_value_ = sequence_info_.value_;
        sequence_info_.gmt_create_.set_value(time_now_str);
        sequence_info_.gmt_modified_.set_value(time_now_str);
      }
    }
  } else {
    // do update
    int64_t old_value = sequence_info_.value_;
    int64_t new_value = sequence_info_.value_ + sequence_info_.step_;
    if (new_value >= sequence_info_.max_value_) {
      LOG_DEBUG("reach max, reset to min now", K(new_value), K(sequence_info_.min_value_));
      new_value = sequence_info_.min_value_;
    }
    sequence_info_.local_value_ = sequence_info_.value_;
    if (OB_FAIL(ObProxySequenceUtils::update_sequence_entry_sql(sql, OB_SHORT_SQL_LENGTH,
                database_name_.config_string_,
                real_table_name_.config_string_,
                seq_name,
                tnt_id,
                tnt_col,
                new_value,
                old_value))) {
      LOG_WARN("fail to get sequence entry sql", K(sql), K(sequence_info_), K(ret));
    } else {
      LOG_DEBUG("update_sequence_entry_sql ", K(sql));
      const ObMysqlRequestParam request_param(sql);
      if (OB_FAIL(mysql_proxy->async_read(this, request_param, pending_action_))) {
        LOG_WARN("fail to nonblock read", K(sql), K(cluster_name_), K(seq_name), K(tnt_id), K(tnt_col), K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
    ret = OB_SUCCESS;
    if (OB_FAIL(schedule_imm(this, SEQUENCE_ENTRY_FAIL_SCHEDULE_LOOKUP_REMOTE_EVENT))) {
      LOG_WARN("fail to schedule_imm", K(ret), K(sequence_info_));
    }
  } else {
    LOG_INFO("after update", K(sequence_info_), K(need_insert_));
  }
  return ret;
}

inline int ObProxySequenceEntryCont::schedule_imm(ObContinuation * cont, const int event)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cont)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cont", K(cont), K(ret));
  } else if (OB_UNLIKELY(NULL != pending_action_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pending_action_ must be NULL here", K_(pending_action), K(ret));
  } else {
    pending_action_ = submit_thread_->schedule_imm(cont, event);
    if (OB_ISNULL(pending_action_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to schedule imm", K_(pending_action), K(event), K(ret));
    }
  }
  return ret;
}
const char* ObProxySequenceEntryCont::get_event_name(const int64_t event)
{
  const char *name = NULL;
  switch (event) {
    MACRO_FOR_CASE_STATE(SEQUENCE_ENTRY_CREATE_CLUSTER_RESOURCE_EVENT);
    MACRO_FOR_CASE_STATE(CLUSTER_RESOURCE_CREATE_COMPLETE_EVENT);
    MACRO_FOR_CASE_STATE(SEQUENCE_ENTRY_LOOKUP_REMOTE_EVENT);
    MACRO_FOR_CASE_STATE(SEQUENCE_ENTRY_CHAIN_NOTIFY_CALLER_EVENT);
    MACRO_FOR_CASE_STATE(SEQUENCE_ENTRY_NOTIFY_CALLER_EVENT);
    MACRO_FOR_CASE_STATE(CLIENT_TRANSPORT_MYSQL_RESP_EVENT);
    MACRO_FOR_CASE_STATE(SEQUENCE_ENTRY_CREATE_COMPLETE_EVENT);
    MACRO_FOR_CASE_STATE(SEQUENCE_ENTRY_FAIL_SCHEDULE_LOOKUP_REMOTE_EVENT);
  default:
    name = "unknown event name";
    break;
  }
  return name;
}
const char* ObProxySequenceEntryCont::get_state_name(const ObSequenceEntryLookupState state)
{
  const char *name = "Unknown State";
  switch (state) {
    MACRO_FOR_CASE_STATE(LOOKUP_SEQUENCE_ENTRY_STATE);
    MACRO_FOR_CASE_STATE(LOOKUP_SEQUENCE_DBTIME_STATE);
    MACRO_FOR_CASE_STATE(LOOKUP_SEQUENCE_UPDATE_STATE);
    MACRO_FOR_CASE_STATE(LOOKUP_SEQUENCE_DONE_STATE);
    MACRO_FOR_CASE_STATE(LOOKUP_SEQUENCE_RETRY_STATE);
  default:
    name = "Unknown State";
    LOG_WARN("Unknown State", K(state));
    break;
  }
  return name;
}
int ObProxySequenceEntryCont::set_next_state() {
  int ret = OB_SUCCESS;
  ObSequenceEntryLookupState next_state = LOOKUP_SEQUENCE_DONE_STATE;
  switch (state_) {
  case LOOKUP_SEQUENCE_ENTRY_STATE:
    next_state = LOOKUP_SEQUENCE_UPDATE_STATE;
    break;
  case LOOKUP_SEQUENCE_DBTIME_STATE:
    next_state = LOOKUP_SEQUENCE_DONE_STATE;
    break;
  case LOOKUP_SEQUENCE_UPDATE_STATE:
    if (need_insert_ && need_db_timestamp_) {
      next_state = LOOKUP_SEQUENCE_DBTIME_STATE;
    } else {
      next_state = LOOKUP_SEQUENCE_DONE_STATE;
    }
    break;
  case LOOKUP_SEQUENCE_RETRY_STATE:
    next_state = LOOKUP_SEQUENCE_ENTRY_STATE;
    break;
  case LOOKUP_SEQUENCE_DONE_STATE:
  default:
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected state", K_(state), K(ret));
    break;
  }
  LOG_DEBUG("sequence entry state changed", "state", get_state_name(state_),
            "next_state", get_state_name(next_state));
  state_ = next_state;
  return ret;
}
int ObProxySequenceEntryCont::handle_client_resp(void *data)
{
  int ret = OB_SUCCESS;
  int64_t affected_row = 0;
  bool need_clean_proxy = false;
  if (NULL != data) {
    ObClientMysqlResp *resp = reinterpret_cast<ObClientMysqlResp *>(data);
    ObResultSetFetcher *rs_fetcher = NULL;
    switch (state_) {
    case LOOKUP_SEQUENCE_ENTRY_STATE:
    case LOOKUP_SEQUENCE_DBTIME_STATE:
      if (resp->is_resultset_resp()) {
        if (OB_FAIL(resp->get_resultset_fetcher(rs_fetcher))) {
          last_state_success_ = false;
          LOG_WARN("fail to get resultset fetcher", K(ret), K(sequence_info_.seq_id_));
        } else if (OB_ISNULL(rs_fetcher)) {
          last_state_success_ = false;
          LOG_WARN("resultset fetcher is NULL", K(ret), K(sequence_info_.seq_id_));
          ret = OB_ERR_UNEXPECTED;
        } else {
          ret = handle_select_old_value_resp(*rs_fetcher);
        }
      } else {
        last_state_success_ = false;
        need_clean_proxy = true;
        const int64_t error_code = resp->get_err_code();
        ObString error_msg = resp->get_err_msg();
        sequence_info_.errno_ = error_code;
        sequence_info_.err_msg_.set_value(error_msg);
        LOG_WARN("fail to get table entry from remote", K(sequence_info_.seq_id_), K(error_code), K(error_msg));
      }
      break;
    case LOOKUP_SEQUENCE_UPDATE_STATE:
      if (OB_SUCC(resp->get_affected_rows(affected_row))) {
        LOG_DEBUG("affected_row is ", K(affected_row), K(sequence_info_.seq_id_));
        if (affected_row >= 1) { // allow same seq
          ret = handle_update_new_value_resp(*rs_fetcher);
        } else if (retry_time_ < max_retry_count_) {
          retry_time_++;
          LOG_INFO("update response failed retry now", K(affected_row), K(sequence_info_.seq_id_), K(retry_time_));
          state_ = LOOKUP_SEQUENCE_RETRY_STATE;
        } else {
          last_state_success_ = false;
          LOG_WARN("update failed after retry ", K(sequence_info_.seq_id_), K(retry_time_));
          ret = OB_ERR_UNEXPECTED;
        }
      } else {
        last_state_success_ = false;
        need_clean_proxy = true;
        const int64_t error_code = resp->get_err_code();
        ObString error_msg = resp->get_err_msg();
        sequence_info_.errno_ = error_code;
        sequence_info_.err_msg_.set_value(error_msg);
        LOG_WARN("fail to get table entry from remote", K(sequence_info_.seq_id_), K(error_code), K(error_msg), K(ret));
        ret = OB_ERR_UNEXPECTED;
      }
      break;
    default:
      LOG_WARN("handle_client_resp invalid state", K(state_), K(sequence_info_.seq_id_));
    }
    op_free(resp); // free the resp come from ObMysqlProxy
    resp = NULL;
  } else {
    ret = OB_ERR_UNEXPECTED;
    need_clean_proxy = true;
    last_state_success_ = false;
    LOG_WARN("handle_client_resp fail to get table entry from remote", K(ret), K(sequence_info_.seq_id_));
  }
  if (need_clean_proxy) {
    // when fail clean cache
    get_global_mysql_proxy_cache().remove_mysql_proxy_entry(mysql_proxy_entry_);
  }
  ret = OB_SUCCESS;
  if (OB_FAIL(set_next_state())) {
    LOG_WARN("handle_client_resp fail to set next state", "state", get_state_name(state_), K(sequence_info_.seq_id_));
  }
  return ret;
}
int ObProxySequenceEntryCont::handle_select_old_value_resp(ObResultSetFetcher &rs_fetcher)
{
  LOG_DEBUG("Enter handle_select_old_value_resp", K(sequence_info_.seq_id_));
  sequence_info_.db_timestamp_.reset(); // reset timestamp
  int ret = OB_SUCCESS;
  int32_t min_value = 0;
  int64_t max_value = 0;
  int32_t step = 0;
  int64_t value = 0;
#define TMP_STR_BUF 1024
  int tmp_real_str_len = 0;
  char gmt_create[TMP_STR_BUF];
  char gmt_modified[TMP_STR_BUF];
  char db_timestamp[TMP_STR_BUF];
  gmt_create[0] = '\0';
  gmt_modified[0] = '\0';
  db_timestamp[0] = '\0';
  if (OB_SUCC(rs_fetcher.next())) {
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "min_value", min_value, int32_t);
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "max_value", max_value, int64_t);
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "step", step, int32_t);
    PROXY_EXTRACT_INT_FIELD_MYSQL(rs_fetcher, "value", value, int64_t);
    PROXY_EXTRACT_STRBUF_FIELD_MYSQL(rs_fetcher, "gmt_create", gmt_create, TMP_STR_BUF, tmp_real_str_len);
    PROXY_EXTRACT_STRBUF_FIELD_MYSQL(rs_fetcher, "gmt_create", gmt_modified, TMP_STR_BUF, tmp_real_str_len);
    PROXY_EXTRACT_STRBUF_FIELD_MYSQL(rs_fetcher, "now", db_timestamp, TMP_STR_BUF, tmp_real_str_len);

    LOG_DEBUG("select_old_value:", K(min_value), K(max_value), K(step), K(value));
    sequence_info_.db_timestamp_.set_value(db_timestamp);
    if (!only_need_db_timestamp_) {
      sequence_info_.min_value_ = min_value;
      sequence_info_.max_value_ = max_value;
      sequence_info_.step_ = step;
      sequence_info_.value_ = value;
      sequence_info_.gmt_create_.set_value(gmt_create);
      sequence_info_.gmt_modified_.set_value(gmt_modified);
    }
    LOG_DEBUG("from old sequence_info now is:", K(sequence_info_));
  } else {
    if (allow_insert_) {
      need_insert_ = true;
      LOG_INFO("handle_select_old_value_resp fetch result fail, maybe need insert", K(sequence_info_.seq_id_));
    } else {
      last_state_success_ = false;
      ret = OB_ERR_UNEXPECTED;
      sequence_info_.errno_ = OB_SEQUENCE_ERROR;
      sequence_info_.err_msg_.set_value("sequence not exist");
      LOG_WARN("handle_select_old_value_resp fetch result fail, something worng", K(sequence_info_.seq_id_));
    }
  }
  return ret;
}
int ObProxySequenceEntryCont::handle_update_new_value_resp(ObResultSetFetcher &rs_fetcher)
{
  int ret = OB_SUCCESS;
  UNUSED(rs_fetcher);
  sequence_info_.last_renew_time_ = event::get_hrtime();//ObTimeUtility::current_time();
  bool need_set_seq2 = false;
  {
    // code block for lock
    obsys::CWLockGuard guard(sequence_entry_->rwlock_);
    if (cont_type_ == OB_SEQUENCE_CONT_ASYNC_TYPE) {
      if (sequence_entry_->sequence_info_.is_valid()) {
        sequence_entry_->sequence_info2_ = sequence_info_;
        need_set_seq2 = true;
      } else {
        sequence_entry_->sequence_info_ = sequence_info_;
      }
    } else if (cont_type_ == OB_SEQUENCE_CONT_SYNC_TYPE) {
      sequence_entry_->sequence_info_ = sequence_info_;
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
    sequence_entry_->set_avail_state();
  }
  // here log for reduce lock time
  LOG_INFO("update new value", K(cont_type_), K(need_set_seq2), K(ret), K(sequence_info_));
  return ret;
}

inline int ObProxySequenceEntryCont::handle_lookup_remote()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("handle lookup remote", "state", get_state_name(state_));
  switch (state_) {
  case LOOKUP_SEQUENCE_ENTRY_STATE:
  case LOOKUP_SEQUENCE_DBTIME_STATE:
    ret = select_old_value_from_remote();
    break;
  case LOOKUP_SEQUENCE_UPDATE_STATE:
    ret = update_new_value_to_remote();
    break;
  case LOOKUP_SEQUENCE_DONE_STATE:
    // do nothing here
    break;
  default:
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpectec state", K_(state), K(ret));
    break;
  }
  return ret;
}

int ObProxySequenceEntryCont::handle_lookup_remote_done()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(notify_caller())) {
    LOG_WARN("fail to notify_caller", K(ret));
  }
  return ret;
}

int ObProxySequenceEntryCont::notify_caller()
{
  LOG_DEBUG("Enter notify_caller()", K(sequence_info_.seq_id_));
  int ret = OB_SUCCESS;
  int pending_list_count = 0;
  ObEThread *submit_thread = NULL;
  ObProxySequenceEntryCont* cont = NULL;
  ObProxySequenceEntryCont* next_cont = NULL;
  if (only_need_db_timestamp_) {
    obsys::CWLockGuard guard(sequence_entry_->rwlock_);
    sequence_entry_->set_timestamp_fetching(false);
    sequence_entry_->timestamp_pending_list_.push(this);
    cont = reinterpret_cast<ObProxySequenceEntryCont *>(sequence_entry_->timestamp_pending_list_.popall());
  } else {
    obsys::CWLockGuard guard(sequence_entry_->rwlock_);
    if (sequence_entry_->is_remote_fetching_state()) {
      LOG_WARN("still remote fetching state, something wrong before", K(sequence_info_.seq_id_));
      sequence_entry_->set_dead_state();
    } else {
      // when start, not allow insert
      sequence_entry_->allow_insert_ = false;
    }
    sequence_entry_->pending_list_.push(this);
    cont = reinterpret_cast<ObProxySequenceEntryCont *>(sequence_entry_->pending_list_.popall());
  }
  while (!OB_ISNULL(cont)) {
    next_cont = reinterpret_cast<ObProxySequenceEntryCont *>(cont->link_.next_);
    ++pending_list_count;
    if (only_need_db_timestamp_) {
      cont->set_dbtimestamp(sequence_info_.db_timestamp_.config_string_);
    }
    submit_thread = cont->submit_thread_;
    if (OB_ISNULL(submit_thread)) {
      LOG_ERROR("submit_thread can not be null", K(cont), K(sequence_info_.seq_id_));
    } else {
      cont->set_errinfo(sequence_info_.err_msg_.config_string_);
      cont->set_errno(sequence_info_.errno_);
      if (OB_ISNULL(submit_thread->schedule_imm(cont, SEQUENCE_ENTRY_NOTIFY_CALLER_EVENT))) {
        LOG_ERROR("fail to schedule imm", K(cont), K(sequence_info_.seq_id_));
      } else {
        LOG_DEBUG("schedule SEQUENCE_ENTRY_NOTIFY_CALLER_EVENT succ", K(cont), K(sequence_info_.seq_id_));
      }
    }
    cont = next_cont;
  }
  LOG_DEBUG("notify caller complete, pending_list_count", K(sequence_info_.seq_id_),
    K(pending_list_count), K(only_need_db_timestamp_));
  return ret;
}

int ObProxySequenceEntryCont::handle_inform_out_event()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("Enter handle_inform_out_event", K(sequence_info_.seq_id_));
  if (this_ethread() != submit_thread_) {
    LOG_ERROR("this thread must be equal with submit_thread", "this ethread", this_ethread(), K_(submit_thread),
              K(sequence_info_.seq_id_), K(ret));
  } if (cont_type_ == OB_SEQUENCE_CONT_ASYNC_TYPE) {
    // async only need set sequence_info2 to entry
    LOG_DEBUG("async type here do nothing", K(sequence_info_.seq_id_));
  } else if (!action_.cancelled_) {
    ObSequenceInfo* seq_info = NULL;
    if (last_state_success_) {
      if (OB_ISNULL(seq_info = op_alloc(ObSequenceInfo))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("failt to allocate ObSequenceInfo", K(sequence_info_.seq_id_));
      } else if (only_need_db_timestamp_) {
        *seq_info = sequence_info_;
        LOG_DEBUG("only_need_db_timestamp_ just dispatch seq info", K(*seq_info));
      } else {
        obsys::CWLockGuard guard(sequence_entry_->rwlock_);
        if (get_global_proxy_config().enable_sequence_prefetch) {
          sequence_entry_->change_use_info2_if_need();
        }
        if (sequence_info_.errno_ != 0) {
          seq_info->err_msg_.set_value(sequence_info_.err_msg_.config_string_);
          seq_info->errno_  = sequence_info_.errno_;
          LOG_WARN("something wrong in fetch remote", K(sequence_info_), K(seq_info));
        } else if (!sequence_entry_->sequence_info_.is_valid()) {
          // maybe too much pending list local is over
          LOG_INFO("local is over, maybe too much pending list", K(sequence_entry_->sequence_info_));
          seq_info->is_over_ = true;
        } else {
          // set value
          *seq_info = sequence_entry_->sequence_info_;
          if (need_insert_ && need_db_timestamp_) {
            seq_info->db_timestamp_.set_value(sequence_info_.db_timestamp_.config_string_);
          }
          sequence_entry_->sequence_info_.local_value_++;
        }
      }
    } else if (sequence_info_.errno_ != 0) {
      if (OB_ISNULL(seq_info = op_alloc(ObSequenceInfo))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("failt to allocate ObSequenceInfo", K(sequence_info_.seq_id_));
      } else {
        seq_info->errno_ = sequence_info_.errno_;
        seq_info->err_msg_.set_value(sequence_info_.err_msg_.config_string_);
      }
    }
    if (OB_SUCC(ret) && !only_need_db_timestamp_) {
      // log only when fetch value
      LOG_INFO("sequence cont complete", KPC(seq_info));
    }
    action_.continuation_->handle_event(SEQUENCE_ENTRY_CREATE_COMPLETE_EVENT, seq_info);
  } else {
    LOG_INFO("ObProxySequenceEntryCont has been cancelled", K(sequence_info_.seq_id_));
  }
  terminate_ = true;
  return ret;
}

int ObProxySequenceEntryCont::main_handler(int event, void *data)
{
  int he_ret = EVENT_CONT;
  int ret = OB_SUCCESS;
  LOG_DEBUG("Enter main_handler, received event", "event", get_event_name(event), K(data));
  if (OB_UNLIKELY(OB_SEQUENCE_ENTRY_CONT_MAGIC_ALIVE != magic_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("this table entry cont is dead", K_(magic), K(ret));
  } else if (OB_UNLIKELY(this_ethread() != mutex_->thread_holding_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("this_ethread must be equal with thread_holding", "this_ethread",
              this_ethread(), "thread_holding", mutex_->thread_holding_, K(ret));
  } else {
    pending_action_ = NULL;
    switch (event) {
    case SEQUENCE_ENTRY_CREATE_CLUSTER_RESOURCE_EVENT: {
      if (OB_FAIL(handle_create_cluster_resouce())) {
        LOG_WARN("fail to handle create cluster resource", K(ret), K(sequence_info_.seq_id_));
      }
      break;
    }
    case CLUSTER_RESOURCE_CREATE_COMPLETE_EVENT: {
      if (OB_FAIL(handle_create_cluster_resource_complete(data))) {
        LOG_WARN("fail to handle create complete", K(ret), K(sequence_info_.seq_id_));
      }
      break;
    }
    case SEQUENCE_ENTRY_LOOKUP_REMOTE_EVENT: {
      if (OB_FAIL(select_old_value_from_remote())) {
        LOG_WARN("fail to lookup entry remote", K(ret), K(sequence_info_.seq_id_));
      }
      break;
    }
    case SEQUENCE_ENTRY_CHAIN_NOTIFY_CALLER_EVENT: {
      if (OB_FAIL(notify_caller())) {
        LOG_WARN("fail to notify caller", K(sequence_info_.seq_id_));
      }
      break;
    }
    case SEQUENCE_ENTRY_NOTIFY_CALLER_EVENT: {
      if (OB_FAIL(handle_inform_out_event())) {
        LOG_WARN("fail to inform out", K(ret), K(sequence_info_.seq_id_));
      }
      break;
    }
    case SEQUENCE_ENTRY_FAIL_SCHEDULE_LOOKUP_REMOTE_EVENT: {
      // if fail to schedule lookup remote, data must set to NULL
      data = NULL;
      // fail through, do not break
    }
    case CLIENT_TRANSPORT_MYSQL_RESP_EVENT: {
      if (OB_FAIL(handle_client_resp(data))) {
        LOG_WARN("fail to handle client resp", K(ret), K(sequence_info_.seq_id_));
      } else if (OB_FAIL(handle_lookup_remote())) {
        LOG_WARN("fail to handle lookup remote done", K(ret), K(sequence_info_.seq_id_));
      }
      // if failed, treat as lookup done and  will inform out
      if (LOOKUP_SEQUENCE_DONE_STATE == state_ || OB_FAIL(ret)) {
        ret = OB_SUCCESS;
        if (OB_FAIL(handle_lookup_remote_done())) {
          LOG_ERROR("fail to handle lookup remote done", K(ret), K(sequence_info_.seq_id_));
        }
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown event", K(event), K(data), K(ret), K(sequence_info_.seq_id_));
      break;
    }
    }
  }
  if (terminate_) {
    kill_this();
    he_ret = EVENT_DONE;
  }
  return he_ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
