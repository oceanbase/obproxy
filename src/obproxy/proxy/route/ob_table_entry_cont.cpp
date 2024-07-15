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
#include "proxy/route/ob_table_entry_cont.h"
#include "lib/profile/ob_trace_id.h"
#include "utils/ob_ref_hash_map.h"
#include "stat/ob_processor_stats.h"
#include "proxy/route/ob_route_utils.h"
#include "proxy/client/ob_mysql_proxy.h"
#include "proxy/client/ob_client_vc.h"
#include "proxy/route/ob_table_entry.h"
#include "proxy/route/ob_table_cache.h"
#include "proxy/route/ob_table_processor.h"
#include "obutils/ob_task_flow_controller.h"
#include "obutils/ob_async_common_task.h"
#include "obutils/ob_config_server_processor.h"
#include "proxy/mysqllib/ob_resultset_fetcher.h"
#include "prometheus/ob_route_prometheus.h"
#include "proxy/route/ob_route_diagnosis.h"
#include "obproxy/obutils/ob_async_common_task.h"
#include "obproxy/utils/ob_proxy_utils.h"
#include "obproxy/obutils/ob_hostname_ip_processor.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::obproxy;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::prometheus;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

static const int HOSTNAME_REFRESH_COMPLETE_EVENT = ASYNC_PROCESS_DONE_EVENT;

int64_t ObRouteResult::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KPC_(target_entry),
       KPC_(target_old_entry),
       K_(is_from_remote),
       K_(is_need_force_flush));
  J_OBJ_END();
  return pos;
}

void ObRouteResult::reset()
{
  target_entry_ = NULL;
  target_old_entry_ = NULL;
  is_from_remote_ = false;
  is_need_force_flush_ = false;
}

bool ObTableRouteParam::is_valid() const
{
  return (NULL != cont_)
         && (cr_version_ >= 0)
         && (cr_id_ >= 0)
         && (name_.is_valid())
         && (NULL != mysql_proxy_);
}

void ObTableRouteParam::reset()
{
  cont_ = NULL;
  name_.reset();
  result_.reset();
  is_partition_table_route_supported_ = false;
  force_renew_ = false;
  is_oracle_mode_ = false;
  mysql_proxy_ = NULL;
  cr_version_ = 0;
  cr_id_ = OB_INVALID_CLUSTER_ID;
  cluster_version_ = 0;
  tenant_version_ = 0;
  current_idc_name_.reset();
  is_need_force_flush_ = false;
  binlog_service_ip_.reset();
  set_route_diagnosis(NULL);
}

void ObTableRouteParam::set_route_diagnosis(ObRouteDiagnosis *route_diagnosis)
{
  if (OB_NOT_NULL(route_diagnosis_)) {
    route_diagnosis_->dec_ref();
    route_diagnosis_ = NULL;
  }
  if (OB_NOT_NULL(route_diagnosis)) {
    route_diagnosis_ = route_diagnosis;
    route_diagnosis_->inc_ref();
  }
}

int64_t ObTableRouteParam::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP_(cont),
       K_(cr_version),
       K_(name),
       K_(cr_id),
       K_(force_renew),
       K_(is_oracle_mode),
       K_(result),
       KP_(mysql_proxy),
       K_(current_idc_name),
       K_(is_need_force_flush));
  J_OBJ_END();
  return pos;
}

// --------- ObTableEntryCont
ObTableEntryCont::ObTableEntryCont()
    : ObAsyncCommonTask(NULL, "table_entry_build_task"), magic_(OB_TABLE_ENTRY_CONT_MAGIC_ALIVE),
      table_param_(),
      name_buf_(NULL), name_buf_len_(0), te_op_(LOOKUP_MIN_OP), state_(LOOKUP_TABLE_ENTRY_STATE),
      newest_table_entry_(NULL), table_entry_(NULL), table_cache_(NULL), mysql_client_(NULL), binlog_sql_(NULL),
      request_param_(), need_notify_(true), need_prepare_binlog_entry_param_(true),
      binlog_service_ip_(), binlog_service_hostname_ip_list_(), binlog_service_addr_list_(), used_hostname_ip_count_(0),
      used_addr_count_(0)
{
  SET_HANDLER(&ObTableEntryCont::main_handler);
}

int ObTableEntryCont::init(ObTableCache &table_cache, ObTableRouteParam &table_param,
                           ObTableEntry *table_entry)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!table_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(table_param), K(ret));
  } else if (OB_FAIL(deep_copy_table_param(table_param))) {
    LOG_WDIAG("fail to deep copy route param", K(ret));
  } else {
    table_cache_ = &table_cache;
    action_.set_continuation(table_param.cont_);
    mutex_ = table_param.cont_->mutex_;
    submit_thread_ = table_param.cont_->mutex_->thread_holding_;
    binlog_service_ip_ = table_param.binlog_service_ip_;
    if (NULL != table_entry) {
      table_entry->inc_ref();
      if (false == table_param_.is_need_force_flush_) {
        table_param_.is_need_force_flush_ = table_entry->is_need_force_flush();
        table_entry->set_need_force_flush(false);
      }
    }
    table_entry_ = table_entry;
  }

  return ret;
}

void ObTableEntryCont::kill_this()
{
  LOG_DEBUG("ObTableEntryCont will be free", K_(table_param_.name), K(this));
  table_param_.reset();
  if (NULL != name_buf_) {
    op_fixed_mem_free(name_buf_, name_buf_len_);
    name_buf_ = NULL;
    name_buf_len_ = 0;
  }

  if (OB_UNLIKELY(NULL != binlog_sql_)) {
    op_fixed_mem_free(binlog_sql_, OB_SHORT_SQL_LENGTH);
    binlog_sql_ = NULL;
  }

  int ret = OB_SUCCESS;
  if (OB_FAIL(cancel_timeout_action())) {
    LOG_WDIAG("fail to cancel timeout action", K(ret));
  }

  if (OB_FAIL(cancel_pending_action())) {
    LOG_WDIAG("fail to cancel pending action", K(ret));
  }

  if (NULL != table_entry_) {
    table_entry_->dec_ref();
    table_entry_ = NULL;
  }

  if (NULL != newest_table_entry_) {
    newest_table_entry_->dec_ref();
    newest_table_entry_ = NULL;
  }

  if (NULL != mysql_client_) {
    mysql_client_->kill_this();
    mysql_client_ = NULL;
  }

  table_cache_ = NULL;
  action_.set_continuation(NULL);
  submit_thread_ = NULL;
  magic_ = OB_TABLE_ENTRY_CONT_MAGIC_DEAD;
  mutex_.release();

  op_free(this);
}

const char *ObTableEntryCont::get_event_name(const int64_t event)
{
  const char *name = NULL;
  switch (event) {
    case TABLE_ENTRY_LOOKUP_CACHE_EVENT: {
      name = "TABLE_ENTRY_LOOKUP_CACHE_EVENT";
      break;
    }
    case TABLE_ENTRY_LOOKUP_REMOTE_EVENT: {
      name = "TABLE_ENTRY_LOOKUP_REMOTE_EVENT";
      break;
    }
    case CLIENT_TRANSPORT_MYSQL_RESP_EVENT: {
      name = "CLIENT_TRANSPORT_MYSQL_RESP_EVENT";
      break;
    }
    case TABLE_ENTRY_EVENT_LOOKUP_DONE: {
      name = "TABLE_ENTRY_EVENT_LOOKUP_DONE";
      break;
    }
    case TABLE_ENTRY_CHAIN_NOTIFY_CALLER_EVENT: {
      name = "TABLE_ENTRY_CHAIN_NOTIFY_CALLER_EVENT";
      break;
    }
    case TABLE_ENTRY_FAIL_SCHEDULE_LOOKUP_REMOTE_EVENT: {
      name = "TABLE_ENTRY_FAIL_SCHEDULE_LOOKUP_REMOTE_EVENT";
      break;
    }
    case TABLE_ENTRY_NOTIFY_CALLER_EVENT: {
      name = "TABLE_ENTRY_NOTIFY_CALLER_EVENT";
      break;
    }
    case HOSTNAME_REFRESH_COMPLETE_EVENT: {
      name = "HOSTNAME_REFRESH_COMPLETE_EVENT";
      break;
    }
    default: {
      name = "unknown event name";
      break;
    }
  }
  return name;
}

const char *ObTableEntryCont::get_state_name(const ObTableEntryLookupState state)
{
  const char *name = "Unknown State";
  switch (state) {
    case LOOKUP_TABLE_ENTRY_STATE:
      name = "LOOKUP_TABLE_ENTRY_STATE";
      break;
    case LOOKUP_PART_INFO_STATE:
      name = "LOOKUP_PART_INFO_STATE";
      break;
    case LOOKUP_FIRST_PART_STATE:
      name = "LOOKUP_FIRST_PART_STATE";
      break;
    case LOOKUP_SUB_PART_STATE:
      name = "LOOKUP_SUB_PART_STATE";
      break;
    case LOOKUP_DONE_STATE:
      name = "LOOKUP_DONE_STATE";
      break;
    case LOOKUP_BINLOG_ENTRY_STATE:
      name = "LOOKUP_BINLOG_ENTRY_STATE";
      break;
    case LOOKUP_BINLOG_HOSTNAME_STATE:
      name = "LOOKUP_BINLOG_HOSTNAME_STATE";
      break;
    default:
      name = "Unknown State";
      LOG_WDIAG("Unknown State", K(state));
      break;
  }
  return name;
}

inline int ObTableEntryCont::main_handler(int event, void *data)
{
  int he_ret = EVENT_CONT;
  int ret = OB_SUCCESS;
  LOG_DEBUG("ObTableEntryCont::main_handler, received event",
            "event", get_event_name(event), K(data));
  if (OB_UNLIKELY(OB_TABLE_ENTRY_CONT_MAGIC_ALIVE != magic_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("this table entry cont is dead", K_(magic), K(ret));
  } else if (OB_UNLIKELY(this_ethread() != mutex_->thread_holding_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("this_ethread must be equal with thread_holding", "this_ethread",
              this_ethread(), "thread_holding", mutex_->thread_holding_, K(ret));
  } else {
    pending_action_ = NULL;
    switch (event) {
      case TABLE_ENTRY_LOOKUP_CACHE_EVENT: {
        if (OB_FAIL(lookup_entry_in_cache())) {
          LOG_WDIAG("fail to lookup enty in cache", K(ret));
        }
        break;
      }
      case TABLE_ENTRY_LOOKUP_REMOTE_EVENT: {
        if (OB_FAIL(lookup_entry_remote())) {
          LOG_WDIAG("fail to lookup entry remote", K(ret));
        }
        break;
      }
      case TABLE_ENTRY_FAIL_SCHEDULE_LOOKUP_REMOTE_EVENT: {
        // if fail to schedule lookup remote, data must set to NULL
        data = NULL;
        // fail through, do not break
      }
      __attribute__ ((fallthrough));
      case CLIENT_TRANSPORT_MYSQL_RESP_EVENT: {
        if (OB_FAIL(handle_client_resp(data))) {
          LOG_WDIAG("fail to handle client resp", K(ret));
        } else if (OB_FAIL(handle_lookup_remote())) {
          LOG_WDIAG("fail to handle lookup remote done", K(ret));
        }
        // if failed, treat as lookup done and  will inform out
        if (LOOKUP_DONE_STATE == state_ || OB_FAIL(ret)) {
          ret = OB_SUCCESS;
          if (OB_FAIL(handle_lookup_remote_done())) {
            LOG_EDIAG("fail to handle lookup remote done", K(ret));
          }
        }
        if (OB_UNLIKELY(LOOKUP_RETRY_STATE == state_)) {
          if (OB_FAIL(do_lookup_binlog_entry_remote(true))) {
            LOG_WDIAG("fail to do lookup binlog service entry", K(ret));
          }
        }
        break;
      }
      case TABLE_ENTRY_CHAIN_NOTIFY_CALLER_EVENT: {
        bool is_replaced = false;
        if (OB_FAIL(replace_building_state_entry(is_replaced))) {
          LOG_WDIAG("fail to replace buding state entry", K(ret));
        } else {
          if (is_replaced) {
            if (OB_FAIL(handle_chain_notify_caller())) {
              LOG_WDIAG("fail to chain notify caller", K(ret));
            }
          }
        }
        break;
      }
      case TABLE_ENTRY_NOTIFY_CALLER_EVENT: {
        if (OB_FAIL(notify_caller())) {
          LOG_WDIAG("fail to notify caller", K(ret));
        }
        break;
      }
      case HOSTNAME_REFRESH_COMPLETE_EVENT: {
        int64_t async_task_ret = *static_cast<int64_t*>(data);
        bool need_use_next_hostname_ip = false;
        bool refresh_succ = (OB_SUCCESS == async_task_ret);
        LOG_DEBUG("get hostname refresh complete event", K(event), "state", get_state_name(state_), K(async_task_ret));
        if (LOOKUP_BINLOG_HOSTNAME_STATE != state_) {
          ret = OB_ERR_UNEXPECTED;
          terminate_ = true;
          LOG_WDIAG("unexpected state", K(event), K_(state), K(ret));
        } else {
          if (OB_FAIL(async_task_ret)) {
            need_use_next_hostname_ip = true;
            LOG_WDIAG("fail to hostname refresh, try next", K(event), K_(state), K(async_task_ret));
          }
          if (OB_FAIL(do_lookup_binlog_entry_remote(need_use_next_hostname_ip, refresh_succ))) {
            LOG_WDIAG("fail to do lookup binlog service entry", K(ret));
          }
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unknow event", K(event), K(data), K(ret));
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

inline int ObTableEntryCont::deep_copy_table_param(ObTableRouteParam &param)
{
  int ret = OB_SUCCESS;
  if (&table_param_ != &param) {
    if (!param.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("invalid input value", K(param), K(ret));
    } else {
      table_param_.cont_ = param.cont_;
      table_param_.force_renew_ = param.force_renew_;
      table_param_.mysql_proxy_ = param.mysql_proxy_;
      table_param_.cr_version_ = param.cr_version_;
      table_param_.cr_id_ = param.cr_id_;
      table_param_.tenant_version_ = param.tenant_version_;
      table_param_.cluster_version_ = param.cluster_version_;
      table_param_.is_partition_table_route_supported_ = param.is_partition_table_route_supported_;
      table_param_.is_oracle_mode_ = param.is_oracle_mode_;
      table_param_.is_need_force_flush_ = param.is_need_force_flush_;
      table_param_.set_route_diagnosis(param.route_diagnosis_);
      if (!param.current_idc_name_.empty()) {
        MEMCPY(table_param_.current_idc_name_buf_, param.current_idc_name_.ptr(), param.current_idc_name_.length());
        table_param_.current_idc_name_.assign_ptr(table_param_.current_idc_name_buf_, param.current_idc_name_.length());
      } else {
        table_param_.current_idc_name_.reset();
      }
      // no need assign result_

      if (OB_NOT_NULL(name_buf_) && name_buf_len_ > 0) {
        op_fixed_mem_free(name_buf_, name_buf_len_);
        name_buf_ = NULL;
        name_buf_len_ = 0;
      }
      name_buf_len_ = param.name_.get_total_str_len();
      name_buf_ = static_cast<char *>(op_fixed_mem_alloc(name_buf_len_));
      if (OB_ISNULL(name_buf_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to alloc mem", K_(name_buf_len), K(ret));
      } else if (OB_FAIL(table_param_.name_.deep_copy(param.name_, name_buf_, name_buf_len_))) {
        LOG_WDIAG("fail to deep copy table entry name", K(ret));
      }

      if (OB_FAIL(ret) && (NULL != name_buf_)) {
        op_fixed_mem_free(name_buf_, name_buf_len_);
        name_buf_ = NULL;
        name_buf_len_ = 0;
        table_param_.name_.reset();
      }
    }
  }

  return ret;
}

inline int ObTableEntryCont::set_next_state()
{
  int ret = OB_SUCCESS;
  ObTableEntryLookupState next_state = LOOKUP_DONE_STATE;
  bool is_part_table_route_supported = table_param_.is_partition_table_route_supported_;
  switch (state_) {
    case LOOKUP_TABLE_ENTRY_STATE:
      if (OB_ISNULL(newest_table_entry_)) {
        next_state = LOOKUP_DONE_STATE;
      } else if (newest_table_entry_->is_partition_table() && is_part_table_route_supported) {
        next_state = LOOKUP_PART_INFO_STATE;
      } else {
        next_state = LOOKUP_DONE_STATE;
      }
      break;
    case LOOKUP_BINLOG_ENTRY_STATE:
      if (OB_ISNULL(newest_table_entry_)) {
        next_state = LOOKUP_RETRY_STATE;
      } else {
        next_state = LOOKUP_DONE_STATE;
      }
      break;
    case LOOKUP_PART_INFO_STATE:
      if (OB_ISNULL(newest_table_entry_)) {
        next_state = LOOKUP_DONE_STATE;
        ret = OB_ERR_NULL_VALUE;
        LOG_WDIAG("newest_table_entry is null, maybe client_vc disconnect or timeout", K(ret));
      } else if (OB_ISNULL(newest_table_entry_->get_part_info())) {
        next_state = LOOKUP_DONE_STATE;
        ret = OB_ERR_NULL_VALUE;
        LOG_WDIAG("part info is null, maybe client_vc disconnect or timeout", K(ret));
      } else if (newest_table_entry_->get_part_info()->has_unknown_part_key()) {
        next_state = LOOKUP_DONE_STATE;
      } else if (newest_table_entry_->get_part_info()->has_first_part()) {
        next_state = LOOKUP_FIRST_PART_STATE;
      } else {
        next_state = LOOKUP_DONE_STATE;
      }
      break;

    case LOOKUP_FIRST_PART_STATE:
      if (OB_ISNULL(newest_table_entry_)) {
        next_state = LOOKUP_DONE_STATE;
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("newest_table_entry should not be null here", K(ret));
      } else if (OB_ISNULL(newest_table_entry_->get_part_info())) {
        next_state = LOOKUP_DONE_STATE;
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("part info should not be null here", K(ret));
      } else if (newest_table_entry_->get_part_info()->has_sub_part()) {
        next_state = LOOKUP_SUB_PART_STATE;
      } else {
        next_state = LOOKUP_DONE_STATE;
      }
      break;

    case LOOKUP_SUB_PART_STATE:
      next_state = LOOKUP_DONE_STATE;
      break;

    case LOOKUP_DONE_STATE:
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unexpected state", K_(state), K(ret));
      break;
  }

  LOG_DEBUG("table entry state changed", "state", get_state_name(state_),
                                         "next_state", get_state_name(next_state));
  state_ = next_state;

  return ret;
}

inline int ObTableEntryCont::handle_client_resp(void *data)
{
  int ret = OB_SUCCESS;
  int64_t error_code = 0;
  int tmp_ret = OB_SUCCESS;
  if (NULL != data) {
    ObClientMysqlResp *resp = reinterpret_cast<ObClientMysqlResp *>(data);
    ObResultSetFetcher *rs_fetcher = NULL;
    if (resp->is_resultset_resp()) {
      if (OB_FAIL(resp->get_resultset_fetcher(rs_fetcher))) {
        LOG_WDIAG("fail to get resultset fetcher", K(ret));
      } else if (OB_ISNULL(rs_fetcher)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        switch (state_) {
          case LOOKUP_TABLE_ENTRY_STATE:
            ret = handle_table_entry_resp(*rs_fetcher);
            break;
          case LOOKUP_PART_INFO_STATE:
            ret = handle_part_info_resp(*rs_fetcher);
            break;
          case LOOKUP_FIRST_PART_STATE:
            ret = handle_first_part_resp(*rs_fetcher);
            break;
          case LOOKUP_SUB_PART_STATE:
            ret = handle_sub_part_resp(*rs_fetcher);
            break;
          case LOOKUP_BINLOG_ENTRY_STATE:
            ret = handle_binlog_entry_resp(*rs_fetcher);
            break;
          case LOOKUP_DONE_STATE:
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("unexpect state", K_(state), K(ret));
            break;
        }
      }
    } else {
      error_code = resp->get_err_code();
      LOG_WDIAG("fail to get table entry from remote", "name", table_param_.name_, K(error_code));
      PROCESSOR_INCREMENT_DYN_STAT(GET_PL_FROM_REMOTE_FAIL);
      ROUTE_PROMETHEUS_STAT(table_param_.name_, PROMETHEUS_ENTRY_LOOKUP_COUNT, TBALE_ENTRY, false, false);
    }
    op_free(resp); // free the resp come from ObMysqlProxy
    resp = NULL;
  } else {
    // no resp, maybe client_vc disconnect
    PROCESSOR_INCREMENT_DYN_STAT(GET_PL_FROM_REMOTE_FAIL);
    ROUTE_PROMETHEUS_STAT(table_param_.name_, PROMETHEUS_ENTRY_LOOKUP_COUNT, TBALE_ENTRY, false, false);
    ret = OB_ERR_UNEXPECTED; // use to free newest_table_entry_
    LOG_WDIAG("fail to get table entry from remote", "name", table_param_.name_, K(ret));
  }
  if (ret != OB_SUCCESS || error_code != OB_SUCCESS) {
    tmp_ret = ret;
    if (!table_param_.name_.is_all_dummy_table()) {
      ROUTE_DIAGNOSIS(table_param_.route_diagnosis_,
                      FETCH_TABLE_RELATED_DATA,
                      fetch_table_related_data,
                      ret,
                      error_code,
                      state_,
                      newest_table_entry_);
    }
  }

  if (OB_FAIL(ret) && NULL != newest_table_entry_) {
    newest_table_entry_->dec_ref();
    newest_table_entry_ = NULL;
  } else {
    LOG_DEBUG("succ to get client resp", "state", get_state_name(state_));
  }

  ret = OB_SUCCESS;
  if (OB_FAIL(set_next_state())) {
    LOG_WDIAG("fail to set next state", "state", get_state_name(state_));
  }
  if (OB_SUCCESS == tmp_ret &&
      OB_SUCCESS == error_code && 
      (ObTableEntryLookupState::LOOKUP_DONE_STATE == state_
       || ObTableEntryLookupState::LOOKUP_RETRY_STATE == state_)) {
    if (!table_param_.name_.is_all_dummy_table()) {
      ROUTE_DIAGNOSIS(table_param_.route_diagnosis_,
                      FETCH_TABLE_RELATED_DATA,
                      fetch_table_related_data,
                      ret,
                      error_code,
                      state_,
                      newest_table_entry_);
    }
  }
  return ret;
}

inline int ObTableEntryCont::handle_table_entry_resp(ObResultSetFetcher &rs_fetcher)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableEntry::alloc_and_init_table_entry(table_param_.name_,
                                                       table_param_.cr_version_,
                                                       table_param_.cr_id_,
                                                       newest_table_entry_))) {
    LOG_WDIAG("fail to alloc and init table entry", "name", table_param_.name_, K(ret));
  } else if (OB_ISNULL(newest_table_entry_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("table entry should not be NULL", K_(newest_table_entry), K(ret));
  } else if (OB_FAIL(ObRouteUtils::fetch_table_entry(rs_fetcher,
                                                     *newest_table_entry_,
                                                     table_param_.cluster_version_))) {
    LOG_WDIAG("fail to fetch one table entry info", K(ret));
  } else {
    newest_table_entry_->set_tenant_version(table_param_.tenant_version_);
  }
  return ret;
}

int ObTableEntryCont::handle_binlog_entry_resp(ObResultSetFetcher &rs_fetcher)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableEntry::alloc_and_init_table_entry(table_param_.name_, 0, 0, newest_table_entry_))) {
    LOG_WDIAG("fail to alloc and init table entry", "name", table_param_.name_, K(ret));
  } else if (OB_ISNULL(newest_table_entry_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("table entry should not be NULL", K_(newest_table_entry), K(ret));
  } else if (OB_FAIL(ObRouteUtils::fetch_binlog_entry(rs_fetcher,
                                                      *newest_table_entry_))) {
    LOG_WDIAG("fail to fetch binlog entry info", K(ret));
  }

  return ret;
}

inline int ObTableEntryCont::handle_part_info_resp(ObResultSetFetcher &rs_fetcher)
{
  int ret = OB_SUCCESS;
  ObProxyPartInfo *part_info = NULL;
  if (OB_ISNULL(newest_table_entry_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("entry should not be null here", K(ret));
  } else if (OB_FAIL(newest_table_entry_->alloc_part_info())) {
    LOG_WDIAG("fail to alloc part info", K_(newest_table_entry), K(ret));
  } else if (OB_ISNULL(part_info = newest_table_entry_->get_part_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("part info should not be null here", K(ret));
  } else if (FALSE_IT(part_info->set_oracle_mode(table_param_.is_oracle_mode_))) {
    // do nothing
  } else if (OB_FAIL(ObRouteUtils::fetch_part_info(rs_fetcher, *part_info, table_param_.cluster_version_))) {
    PROCESSOR_INCREMENT_DYN_STAT(GET_PART_INFO_FROM_REMOTE_FAIL);
    ROUTE_PROMETHEUS_STAT(table_param_.name_, PROMETHEUS_ENTRY_LOOKUP_COUNT, PARTITION_INFO, false, false);
    LOG_WDIAG("fail to fetch part info", K(ret));
  } else {
    PROCESSOR_INCREMENT_DYN_STAT(GET_PART_INFO_FROM_REMOTE_SUCC);
    ROUTE_PROMETHEUS_STAT(table_param_.name_, PROMETHEUS_ENTRY_LOOKUP_COUNT, PARTITION_INFO, false, true);
  }
  return ret;
}

inline int ObTableEntryCont::handle_first_part_resp(ObResultSetFetcher &rs_fetcher)
{
  int ret = OB_SUCCESS;
  ObProxyPartInfo *part_info = NULL;
  if (OB_ISNULL(newest_table_entry_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("entry should not be null here", K(ret));
  } else if (OB_ISNULL(part_info = newest_table_entry_->get_part_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("part info should not be null here", K(ret));
  } else if (OB_FAIL(ObRouteUtils::fetch_first_part(rs_fetcher, *part_info, table_param_.cluster_version_))) {
    PROCESSOR_INCREMENT_DYN_STAT(GET_FIRST_PART_FROM_REMOTE_FAIL);
    ROUTE_PROMETHEUS_STAT(table_param_.name_, PROMETHEUS_ENTRY_LOOKUP_COUNT, PARTITION_INFO, false, false);
    LOG_WDIAG("fail to fetch part info", K(ret));
  } else {
    PROCESSOR_INCREMENT_DYN_STAT(GET_FIRST_PART_FROM_REMOTE_SUCC);
    ROUTE_PROMETHEUS_STAT(table_param_.name_, PROMETHEUS_ENTRY_LOOKUP_COUNT, PARTITION_INFO, false, true);
  }
  return ret;
}

inline int ObTableEntryCont::handle_sub_part_resp(ObResultSetFetcher &rs_fetcher)
{
  int ret = OB_SUCCESS;
  ObProxyPartInfo *part_info = NULL;
  if (OB_ISNULL(newest_table_entry_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("entry should not be null here", K(ret));
  } else if (OB_ISNULL(part_info = newest_table_entry_->get_part_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("part info should not be null here", K(ret));
  } else if (OB_FAIL(ObRouteUtils::fetch_sub_part(rs_fetcher, *part_info, table_param_.cluster_version_))) {
    PROCESSOR_INCREMENT_DYN_STAT(GET_SUB_PART_FROM_REMOTE_FAIL);
    ROUTE_PROMETHEUS_STAT(table_param_.name_, PROMETHEUS_ENTRY_LOOKUP_COUNT, PARTITION_INFO, false, false);
    LOG_WDIAG("fail to fetch part info", K(ret));
  } else {
    PROCESSOR_INCREMENT_DYN_STAT(GET_SUB_PART_FROM_REMOTE_SUCC);
    ROUTE_PROMETHEUS_STAT(table_param_.name_, PROMETHEUS_ENTRY_LOOKUP_COUNT, PARTITION_INFO, false, true);
  }
  return ret;
}

inline int ObTableEntryCont::handle_lookup_remote()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("handle lookup remote", "state", get_state_name(state_));
  switch (state_) {
    case LOOKUP_PART_INFO_STATE:
      ret = lookup_part_info_remote();
      break;

    case LOOKUP_FIRST_PART_STATE:
      ret = lookup_first_part_remote();
      break;

    case LOOKUP_SUB_PART_STATE:
      ret = lookup_sub_part_remote();
      break;

    case LOOKUP_DONE_STATE:
    case LOOKUP_RETRY_STATE:
      // do nothing here
      break;

    case LOOKUP_TABLE_ENTRY_STATE:
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unexpect state", K_(state), K(ret));
      break;
  }

  // if failed, treat as normal case and set LOOKUP_DONE_STATE
  if (OB_FAIL(ret)) {
    ret = OB_SUCCESS;
    state_ = LOOKUP_DONE_STATE;
  }

  if (LOOKUP_DONE_STATE == state_
      || LOOKUP_RETRY_STATE == state_) {
    if (OB_ISNULL(newest_table_entry_)) {
      PROCESSOR_INCREMENT_DYN_STAT(GET_PL_FROM_REMOTE_FAIL);
      ROUTE_PROMETHEUS_STAT(table_param_.name_, PROMETHEUS_ENTRY_LOOKUP_COUNT, TBALE_ENTRY, false, false);
      LOG_INFO("can not find table entry, empty result", "names", table_param_.name_, KPC_(newest_table_entry));
      // if table entry is part info entry, and part table route is NOT supported, we will treat this entry as valid
    } else if (!is_newest_table_entry_valid()) {
      PROCESSOR_INCREMENT_DYN_STAT(GET_PL_FROM_REMOTE_FAIL);
      ROUTE_PROMETHEUS_STAT(table_param_.name_, PROMETHEUS_ENTRY_LOOKUP_COUNT, TBALE_ENTRY, false, false);
      LOG_INFO("fail to get table entry", "names", table_param_.name_, KPC_(newest_table_entry));
      newest_table_entry_->dec_ref();
      newest_table_entry_ = NULL;
    } else {
      newest_table_entry_->set_avail_state();
      if (newest_table_entry_->is_non_partition_table()
          && !newest_table_entry_->exist_leader_server()) {
        // current the non partition table has no leader, avoid refequently updating
        newest_table_entry_->renew_last_update_time();
      }
      LOG_INFO("get table entry from remote succ", KPC(newest_table_entry_));
      PROCESSOR_INCREMENT_DYN_STAT(GET_PL_FROM_REMOTE_SUCC);
      ROUTE_PROMETHEUS_STAT(table_param_.name_, PROMETHEUS_ENTRY_LOOKUP_COUNT, TBALE_ENTRY, false, true);
      table_param_.result_.is_from_remote_ = true;
    }
  }
  return ret;
}

inline int ObTableEntryCont::handle_lookup_remote_done()
{
  int ret = OB_SUCCESS;
  switch (te_op_) {
    case LOOKUP_REMOTE_DIRECT_OP: {
      bool is_add_succ = false;
      if (OB_FAIL(add_to_global_cache(is_add_succ))) {
        LOG_WDIAG("fail to add to global cache", K(ret));
      } else if (notify_caller()) {
        LOG_WDIAG("fail to notify caller", K(ret));
      }
      break;
    }
    case LOOKUP_REMOTE_FOR_UPDATE_OP: {
      if (OB_FAIL(handle_lookup_remote_for_update())) {
        LOG_WDIAG("fail to handle lookup remote fro update", K(ret));
      }
      break;
    }
    case LOOKUP_REMOTE_WITH_BUILDING_ENTRY_OP: {
      bool is_replaced = false;
      if (OB_FAIL(replace_building_state_entry(is_replaced))) {
        LOG_WDIAG("fail to replace buding state entry", K(ret));
      } else {
        if (is_replaced) {
          if (OB_FAIL(handle_chain_notify_caller())) {
            LOG_WDIAG("fail to chain notify caller", K(ret));
          }
        }
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unexpected op", K_(te_op), K(ret));
    }
  }
  return ret;
}

inline int ObTableEntryCont::add_to_global_cache(bool &add_succ)
{
  int ret = OB_SUCCESS;
  add_succ = false;
  ObTableEntry *entry = newest_table_entry_;
  if (OB_LIKELY(NULL != entry) && OB_LIKELY(entry->is_valid())) {
    entry->inc_ref(); // Attention!! before add to table cache, must inc_ref
    if (OB_FAIL(table_cache_->add_table_entry(*entry, false))) {
      LOG_WDIAG("fail to add table entry", KPC(entry), K(ret));
      entry->dec_ref(); // paired the ref count above
    } else {
      add_succ = true;
    }
  }
  return ret;
}

inline int ObTableEntryCont::handle_lookup_remote_for_update()
{
  int ret = OB_SUCCESS;
  bool is_add_succ = false;
  if (OB_FAIL(add_to_global_cache(is_add_succ))) {
    LOG_WDIAG("fail to add to global cache", K(ret));
    ret = OB_SUCCESS; // ignore ret;
  }

  // if fail to update dirty table entry, must set entry state from UPDATING back to DIRTY,
  // or it will never be updated again
  if (NULL != table_entry_) {
    if (!is_add_succ) {
      if (table_entry_->is_updating_state()) {
        table_entry_->renew_last_update_time(); // avoid refequently updating
        // double check
        if (table_entry_->cas_compare_and_swap_state(ObTableEntry::UPDATING, ObTableEntry::DIRTY)) {
          LOG_INFO("fail to update dirty table entry, set state back to dirty", K_(*table_entry));
        }
      }
    } else {
      if (NULL != newest_table_entry_) {
        if (table_entry_->is_the_same_entry(*newest_table_entry_)) {
          // the newest_table_entry is the same with old table entry,
          // so renew last update time and avoid refequently updating;
          newest_table_entry_->renew_last_update_time();
          LOG_INFO("new table entry is the same with old one, will renew last_update_time "
                   "and avoid refequently updating", KPC_(table_entry), KPC_(newest_table_entry));
        }
        ObProxyPartitionLocation *this_pl = const_cast<ObProxyPartitionLocation *>(table_entry_->get_first_pl());
        ObProxyPartitionLocation *new_pl = const_cast<ObProxyPartitionLocation *>(newest_table_entry_->get_first_pl());
        if (this_pl != NULL && new_pl != NULL) {
          const bool is_server_changed = new_pl->check_and_update_server_changed(*this_pl);
          if (is_server_changed) {
            LOG_INFO("server is changed, ", "old_entry", PC(table_entry_),
                                            "new_entry", PC(newest_table_entry_));
          }
        }
      }
    }
    table_entry_->dec_ref();
    table_entry_ = NULL;
  }

  if (OB_FAIL(notify_caller())) {
    LOG_WDIAG("fail to notify caller", K(ret));
  }
  return ret;
}

inline int ObTableEntryCont::replace_building_state_entry(bool &is_replaced)
{
  int ret = OB_SUCCESS;
  is_replaced = false;
  ObTableEntryKey key(table_param_.name_, table_param_.cr_version_, table_param_.cr_id_);
  uint64_t hash = key.hash();
  ObProxyMutex *bucket_mutex = table_cache_->lock_for_key(hash);
  MUTEX_TRY_LOCK(lock_bucket, bucket_mutex, this_ethread()); // release lock as soon as possible
  if (lock_bucket.is_locked()) {
    if (OB_FAIL(table_cache_->run_todo_list(table_cache_->part_num(hash)))) {
      LOG_WDIAG("fail to run todo list", K(ret));
    } else if (OB_ISNULL(table_entry_)) { // building state table entry
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("table entry can not be NULL", K(ret));
    } else {
      is_replaced = true;
      bool is_add_succ = false;
      if (OB_FAIL(add_to_global_cache(is_add_succ))) {
        LOG_WDIAG("fail to add to global cache", K(ret));
        ret = OB_SUCCESS;
      }
      if (!is_add_succ) {
        // if fail to add, should remove the building state table entry
        if (OB_FAIL(table_cache_->remove_table_entry(key))) {
          LOG_WDIAG("fail to remote table entry", K(key), K(ret));
        }
      }
    }
  } else { // reschedule
    if (OB_FAIL(schedule_in(this, SCHEDULE_TABLE_ENTRY_LOOKUP_INTERVAL,
                            TABLE_ENTRY_CHAIN_NOTIFY_CALLER_EVENT))) {
      LOG_WDIAG("fail to schedule in", K(ret));
    }
  }
  return ret;
}

inline int ObTableEntryCont::handle_chain_notify_caller()
{
  int ret = OB_SUCCESS;
  int64_t pending_count = 0;
  ObTableEntryCont *cur_te_cont = reinterpret_cast<ObTableEntryCont *>
                                  (table_entry_->pending_queue_.pop());
  ObTableEntryCont *next_te_cont = NULL;
  bool need_schedule = false;
  // 1. first notify other conts
  while (NULL != cur_te_cont) {
    next_te_cont = reinterpret_cast<ObTableEntryCont *>(table_entry_->pending_queue_.pop());
    ++pending_count;
    ObEThread *submit_thread = cur_te_cont->submit_thread_;
    if (OB_ISNULL(submit_thread)) {
      LOG_EDIAG("submit thread can not be NULL", K(cur_te_cont));
    } else {
      if (NULL != newest_table_entry_) {
        newest_table_entry_->inc_ref();
      }
      cur_te_cont->newest_table_entry_ = newest_table_entry_;
      if (submit_thread == &self_ethread()) { // the same thread
        // try lock
        MUTEX_TRY_LOCK(lock, cur_te_cont->mutex_, &self_ethread());
        if (lock.is_locked()) {
          need_schedule = false;
          ObCurTraceId::set(reinterpret_cast<uint64_t>(cur_te_cont->mutex_.ptr_));
          cur_te_cont->handle_event(TABLE_ENTRY_NOTIFY_CALLER_EVENT, NULL);
          ObCurTraceId::set(reinterpret_cast<uint64_t>(mutex_.ptr_));
        } else {
          need_schedule = true;
        }
      } else {
        need_schedule = true;
      }
      if (need_schedule) {
        if (OB_ISNULL(submit_thread->schedule_imm(cur_te_cont, TABLE_ENTRY_NOTIFY_CALLER_EVENT))) {
          LOG_EDIAG("fail to schedule imm", K(cur_te_cont));
          if (NULL != cur_te_cont->newest_table_entry_) {
            cur_te_cont->newest_table_entry_->dec_ref();
            cur_te_cont->newest_table_entry_ = NULL;
          }
        }
      }
    }
    // if failed, continue, do not break;
    cur_te_cont = next_te_cont;
  }
  LOG_DEBUG("after handle chain notify caller", K(pending_count), KPC_(newest_table_entry));

  // 2. notify self cont
  if (OB_SUCC(ret)) {
    // no need inc newest_table_entry's ref
    LOG_DEBUG("will notify self caller directly", K(ret));
    if (OB_FAIL(notify_caller())) {
      LOG_WDIAG("fail to call notify caller", K(ret));
      ret = OB_SUCCESS; // ignore ret
    }
  }

  if (OB_SUCC(ret)) { // free the buinding state table entry
    table_entry_->dec_ref();
    table_entry_ = NULL;
  }

  return ret;
}

inline int ObTableEntryCont::lookup_entry_remote()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(table_param_.name_.table_name_ == OB_ALL_BINLOG_DUMMY_TNAME)) {
    if (OB_FAIL(do_lookup_binlog_entry_remote(false))) {
      LOG_WDIAG("fail to get binlog entry from remote", K(ret));
    } else {
      LOG_DEBUG("succ to get binlog entry or wait to hostname refresh and callback", K(ret));
    }
  } else {
    ObMysqlProxy *mysql_proxy = table_param_.mysql_proxy_;
    char sql[OB_SHORT_SQL_LENGTH];
    sql[0] = '\0';
    if (OB_FAIL(ObRouteUtils::get_table_entry_sql(sql, OB_SHORT_SQL_LENGTH, table_param_.name_,
                                                  table_param_.is_need_force_flush_, table_param_.cluster_version_))) {
      LOG_WDIAG("fail to get table entry sql", K(sql), K(ret));
    } else {
      const ObMysqlRequestParam request_param(sql, table_param_.current_idc_name_);
      if (OB_FAIL(mysql_proxy->async_read(this, request_param, pending_action_))) {
        LOG_WDIAG("fail to nonblock read", K(sql), K_(table_param), K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
    ret = OB_SUCCESS;
    if (OB_FAIL(schedule_imm(this, TABLE_ENTRY_FAIL_SCHEDULE_LOOKUP_REMOTE_EVENT))) {
      LOG_WDIAG("fail to schedule in", K(ret));
    }
  }
  return ret;
}

int ObTableEntryCont::do_lookup_binlog_entry_remote(bool need_use_next_hostname_ip,
                                                    bool hostname_refresh_succ /* false */)
{
  int ret = OB_SUCCESS;

  ObMysqlProxy *mysql_proxy = table_param_.mysql_proxy_;
  if (need_prepare_binlog_entry_param_) {
    if (OB_FAIL(obproxy::split_string_by_char(binlog_service_ip_, binlog_service_hostname_ip_list_, ';'))) {
      LOG_WDIAG("fail to split binlog_service_ip", K_(binlog_service_ip), K(ret));
    } else if (OB_ISNULL(binlog_sql_ = static_cast<char *>(op_fixed_mem_alloc(OB_SHORT_SQL_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc mem", K(ret));
    } else if (OB_FAIL(ObRouteUtils::get_binlog_entry_sql(binlog_sql_, OB_SHORT_SQL_LENGTH,
               table_param_.name_.cluster_name_, table_param_.name_.tenant_name_))) {
      LOG_WDIAG("fail to get binlog entry sql", K(ret));
    } else if (OB_ISNULL(mysql_client_)
               && OB_ISNULL(mysql_client_ = op_alloc(ObMysqlClient))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to allocate ObMysqlClient", K(ret));
    } else if (OB_FAIL(mysql_client_->init_binlog_client(table_param_.name_.cluster_name_,
                                                         table_param_.name_.tenant_name_))) {
      LOG_WDIAG("fail to init binlog client", K_(table_param), K(ret));  
    }

    request_param_.set_sql(binlog_sql_);
    request_param_.set_mysql_client(mysql_client_);
    request_param_.set_client_vc_type(ObMysqlRequestParam::CLIENT_VC_TYPE_BINLOG);
    request_param_.ob_client_flags_.client_flags_.OB_CLIENT_SKIP_AUTOCOMMIT = 1;
    need_prepare_binlog_entry_param_ = false;
  }

  ObAddr addr;

  if (OB_FAIL(ret)) {
    // nothing
  } else if (!binlog_service_addr_list_.empty()
             && used_addr_count_ < binlog_service_addr_list_.count()) {
    // cur hostname have multiple ip, use rest available one
    addr = binlog_service_addr_list_.at(used_addr_count_);
    used_addr_count_++;
  } else do {
    // need use new hostname or ip
    binlog_service_addr_list_.reset();
    used_addr_count_ = 0;
    ObString single_binlog_service_ip;
    ObString hostname;
    ObSEArray<ObIpAddr, 2> ip_list;

    if (need_use_next_hostname_ip) {
      used_hostname_ip_count_++;
    }

    while (used_hostname_ip_count_ < binlog_service_hostname_ip_list_.count()
           && binlog_service_hostname_ip_list_.at(used_hostname_ip_count_).empty()) {
      used_hostname_ip_count_++;
    }

    if (OB_UNLIKELY(used_hostname_ip_count_ == binlog_service_hostname_ip_list_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to find available hostname or ip", K_(binlog_service_ip), K_(used_hostname_ip_count), K(ret));
    } else if (FALSE_IT(single_binlog_service_ip = binlog_service_hostname_ip_list_.at(used_hostname_ip_count_))) {
      // impossible
    } else if (OB_FAIL(addr.parse_from_obtring(single_binlog_service_ip))) {
      LOG_DEBUG("parse from cstring failed, treat as hostname", K_(binlog_service_ip), K(ret));
      if (OB_FAIL(ObAddr::parse_hostname_port_from_obtring(single_binlog_service_ip, hostname, addr.port_))) {
        ret = OB_EAGAIN;
        LOG_WDIAG("parse from cstring failed", K_(binlog_service_ip), K(hostname), K(ret));
      } else if (OB_FAIL(get_binlog_service_hostname_ip_processor().sync_get_ip_by_hostname(hostname, ip_list))) {
        LOG_DEBUG("fail to get binlog service ip sync, maybe need refresh", K(hostname), K(ret));
        if (OB_UNLIKELY(hostname_refresh_succ)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("fail to async get hostname after refresh succ, cannot retry", K(hostname), K(ret));
        } else if (OB_EAGAIN == ret) {
          if (OB_NOT_NULL(pending_action_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("fail to do async task", K_(pending_action), K(ret));
          } else if (OB_FAIL(get_binlog_service_hostname_ip_processor().async_refresh_ip_by_hostname(hostname, this, pending_action_))) {
            LOG_WDIAG("fail to refresh hostname", K(ret));
          } else {
            ret = OB_NEED_WAIT;
            state_ = LOOKUP_BINLOG_HOSTNAME_STATE;
          }
        } else {
          LOG_WDIAG("fail to get binlog service ip", K(hostname), K(ret));
        }
      } else {
        // get hostname ip from local directly
        for (int64_t i = 0; OB_SUCC(ret) && i < ip_list.count(); ++i) {
          addr.set_ip_from_ip_addr(ip_list.at(i));
          LOG_DEBUG("succ to get ip from hostname", K(addr), K(i));
          if (OB_FAIL(binlog_service_addr_list_.push_back(addr))) {
            LOG_WDIAG("fail to push back binlog service addr", K(addr), K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          addr = binlog_service_addr_list_.at(used_addr_count_);
          used_addr_count_++;
        }
      }

      if (OB_FAIL(ret)
          && (OB_NEED_WAIT != ret)) {
        ret = OB_EAGAIN;
        need_use_next_hostname_ip = true;
      }
    } else {
      // get ip from binlog_service_ip
      // nothing
    }
  } while (OB_EAGAIN == ret);


  if (OB_UNLIKELY(OB_NEED_WAIT == ret)) {
    // nothing wait callback
    ret = OB_SUCCESS;
  } else if (addr.is_invalid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to get valid binlog service addr", K(addr), K(ret));
  } else if (OB_SUCC(ret)) {
    state_ = LOOKUP_BINLOG_ENTRY_STATE;
    request_param_.set_target_addr(addr);
    if (OB_FAIL(mysql_proxy->async_read(this, request_param_, pending_action_))) {
      LOG_WDIAG("fail to async read", K_(binlog_sql), K(addr), K(ret));
    }
    if (OB_FAIL(ret) && NULL != mysql_client_) {
      mysql_client_->kill_this();
      mysql_client_ = NULL;
    }
  }

  return ret;
}

inline int ObTableEntryCont::lookup_part_info_remote()
{
  int ret = OB_SUCCESS;
  PROCESSOR_INCREMENT_DYN_STAT(GET_PART_INFO_FROM_REMOTE);
  ObMysqlProxy *mysql_proxy = table_param_.mysql_proxy_;
  char sql[OB_SHORT_SQL_LENGTH];
  sql[0] = '\0';
  if (OB_ISNULL(newest_table_entry_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("table entry should not be null", K(ret));
  } else if (OB_FAIL(ObRouteUtils::get_part_info_sql(sql, OB_SHORT_SQL_LENGTH,
                                                          newest_table_entry_->get_table_id(),
                                                          table_param_.name_,
                                                          table_param_.cluster_version_))) {
    LOG_WDIAG("fail to get table entry sql", K(sql), K(ret));
  } else {
    const ObMysqlRequestParam request_param(sql, table_param_.current_idc_name_);
    if (OB_FAIL(mysql_proxy->async_read(this, request_param, pending_action_))) {
      LOG_WDIAG("fail to nonblock read", K(sql), K_(table_param), K(ret));
    }
  }
  return ret;
}

inline int ObTableEntryCont::lookup_first_part_remote()
{
  int ret = OB_SUCCESS;
  PROCESSOR_INCREMENT_DYN_STAT(GET_FIRST_PART_FROM_REMOTE);
  ObMysqlProxy *mysql_proxy = table_param_.mysql_proxy_;
  char sql[OB_SHORT_SQL_LENGTH];
  sql[0] = '\0';
  if (OB_ISNULL(newest_table_entry_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("table entry should not be null", K(ret));
  } else if (OB_FAIL(ObRouteUtils::get_first_part_sql(sql, OB_SHORT_SQL_LENGTH,
                                                      newest_table_entry_->get_table_id(),
                                                      (newest_table_entry_->get_part_info()->get_first_part_option().is_hash_part(table_param_.cluster_version_)
                                                       || newest_table_entry_->get_part_info()->get_first_part_option().is_key_part(table_param_.cluster_version_)),
                                                      table_param_.name_,
                                                      table_param_.cluster_version_))) {
    LOG_WDIAG("fail to get table entry sql", K(sql), K(ret));
  } else {
    const ObMysqlRequestParam request_param(sql, table_param_.current_idc_name_);
    if (OB_FAIL(mysql_proxy->async_read(this, request_param, pending_action_))) {
      LOG_WDIAG("fail to nonblock read", K(sql), K_(table_param), K(ret));
    }
  }
  return ret;
}

inline int ObTableEntryCont::lookup_sub_part_remote()
{
  int ret = OB_SUCCESS;
  PROCESSOR_INCREMENT_DYN_STAT(GET_SUB_PART_FROM_REMOTE);
  ObMysqlProxy *mysql_proxy = table_param_.mysql_proxy_;
  char sql[OB_SHORT_SQL_LENGTH];
  sql[0] = '\0';
  if (OB_ISNULL(newest_table_entry_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("table entry should not be null", K(ret));
  } else if (OB_FAIL(ObRouteUtils::get_sub_part_sql(sql, OB_SHORT_SQL_LENGTH,
                                                    newest_table_entry_->get_table_id(),
                                                    newest_table_entry_->get_part_info()->is_template_table(),
                                                    table_param_.name_,
                                                    table_param_.cluster_version_))) {
    LOG_WDIAG("fail to get table entry sql", K(sql), K(ret));
  } else {
    const ObMysqlRequestParam request_param(sql, table_param_.current_idc_name_);
    if (OB_FAIL(mysql_proxy->async_read(this, request_param, pending_action_))) {
      LOG_WDIAG("fail to nonblock read", K(sql), K_(table_param), K(ret));
    }
  }
  return ret;
}

inline int ObTableEntryCont::lookup_entry_in_cache()
{
  int ret = OB_SUCCESS;
  ObAction *action = NULL;
  ObTableEntryLookupOp op = LOOKUP_MIN_OP;
  ObTableEntry *entry = NULL;
  if (OB_FAIL(ObTableProcessor::get_table_entry_from_global_cache(
                  table_param_, *table_cache_, this, action, entry, op))) {
    LOG_WDIAG("fail to get table entry in global cache", K(ret));
  } else {
    switch (op) {
      case LOOKUP_PUSH_INTO_PENDING_LIST_OP: {
        if (NULL != action) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("action must be NULL here", K(action), K(ret));
        } else {
          table_entry_ = entry;
          entry = NULL;
        }
        break;
      }
      case LOOKUP_GLOBAL_CACHE_HIT_OP: {
        if (OB_ISNULL(entry)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("table entry must not be NULL here", K(entry), K(ret));
        } else {
          PROCESSOR_INCREMENT_DYN_STAT(GET_PL_FROM_GLOBAL_CACHE_HIT);
          ROUTE_PROMETHEUS_STAT(table_param_.name_, PROMETHEUS_ENTRY_LOOKUP_COUNT, TBALE_ENTRY, true, true);
          // entry has already inc_ref
          newest_table_entry_ = entry;
          entry = NULL;
          if (OB_FAIL(notify_caller())) {
            LOG_WDIAG("fail to notify caller", K(ret));
          }
        }
        break;
      }
      case RETRY_LOOKUP_GLOBAL_CACHE_OP: { // fail to lock, reschedule
        if (OB_FAIL(schedule_in(this, SCHEDULE_TABLE_ENTRY_LOOKUP_INTERVAL,
                                TABLE_ENTRY_LOOKUP_CACHE_EVENT))) {
          LOG_WDIAG("fail to schedule in", K(ret));
        }
        break;
      }
      case LOOKUP_REMOTE_DIRECT_OP:
      case LOOKUP_REMOTE_FOR_UPDATE_OP:
      case LOOKUP_REMOTE_WITH_BUILDING_ENTRY_OP: {
        if (LOOKUP_REMOTE_DIRECT_OP == op) {
          PROCESSOR_INCREMENT_DYN_STAT(GET_PL_FROM_REMOTE);
        } else if (LOOKUP_REMOTE_FOR_UPDATE_OP == op) {
          PROCESSOR_INCREMENT_DYN_STAT(GET_PL_FROM_GLOBAL_CACHE_DIRTY);
        } else if (LOOKUP_REMOTE_WITH_BUILDING_ENTRY_OP == op) {
          PROCESSOR_INCREMENT_DYN_STAT(GET_PL_FROM_REMOTE);
        }
        set_table_entry_op(op);
        table_entry_ = entry;
        entry = NULL;
        if (OB_FAIL(lookup_entry_remote())) {
          LOG_WDIAG("fail to lookup entry remote", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unknown op", K(op), K(ret));
        break;
      }
    }
  }
  return ret;
}

inline int ObTableEntryCont::schedule_in(ObContinuation *cont, const ObHRTime atimeout_in,
                                         const int event)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cont)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid cont", K(cont), K(ret));
  } else if (OB_UNLIKELY(NULL != pending_action_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("pending_action_ must be NULL here", K_(pending_action), K(ret));
  } else {
    pending_action_ = submit_thread_->schedule_in(cont, atimeout_in, event);
    if (OB_ISNULL(pending_action_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to schedule imm", K_(pending_action), K(event), K(ret));
    }
  }
  return ret;
}

inline int ObTableEntryCont::schedule_imm(ObContinuation *cont, const int event)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cont)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid cont", K(cont), K(ret));
  } else if (OB_UNLIKELY(NULL != pending_action_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("pending_action_ must be NULL here", K_(pending_action), K(ret));
  } else {
    pending_action_ = submit_thread_->schedule_imm(cont, event);
    if (OB_ISNULL(pending_action_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to schedule imm", K_(pending_action), K(event), K(ret));
    }
  }
  return ret;
}

inline int ObTableEntryCont::notify_caller()
{
  int ret = OB_SUCCESS;
  ObTableEntry *entry = newest_table_entry_;
  table_param_.result_.target_entry_ = entry;
  table_param_.result_.is_need_force_flush_ = table_param_.is_need_force_flush_;
  if (NULL != entry) {
    entry->renew_last_access_time();
  }
  // do not forget
  newest_table_entry_ = NULL;

  // update thread cache table entry
  if ((NULL != entry) && (entry->is_avail_state())) {
    ObTableRefHashMap &table_map = self_ethread().get_table_map();
    if (OB_FAIL(table_map.set(entry))) {
      LOG_WDIAG("fail to set table map", KPC(entry), K(ret));
      ret = OB_SUCCESS; // ignore ret
    }
  }

  if (action_.cancelled_) {
    // when cancelled, do no forget free the table entry
    if (NULL != entry) {
      entry->dec_ref();
      entry = NULL;
      table_param_.result_.target_entry_ = NULL;
    }
    LOG_DEBUG("ObTableEntryCont has been cancelled");
  } else if (need_notify_) {
    action_.continuation_->handle_event(TABLE_ENTRY_EVENT_LOOKUP_DONE, &table_param_.result_);
  } else {
    // enable async pull table entry, need dec_ref
    if (NULL != entry) {
      entry->dec_ref();
      entry = NULL;
      table_param_.result_.target_entry_ = NULL;
    }
  }

  if (OB_SUCC(ret)) {
    terminate_ = true;
  }
  return ret;
}

inline bool ObTableEntryCont::is_newest_table_entry_valid() const
{
  bool bret = false;
  if (NULL == newest_table_entry_) {
    bret = false;
  } else if (newest_table_entry_->is_valid()) {
    bret = true;
  } else if (newest_table_entry_->is_part_info_entry()
             && !table_param_.is_partition_table_route_supported_) {
    bret = true;
  } else {
    bret = false;
  }
  return bret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
