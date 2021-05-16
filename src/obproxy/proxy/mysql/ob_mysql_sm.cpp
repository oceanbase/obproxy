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
 *
 * *************************************************************
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX PROXY_SM
#include "proxy/mysql/ob_mysql_sm.h"
#include "rpc/obmysql/packet/ompk_handshake.h"
#include "obutils/ob_config_server_processor.h"
#include "proxy/shard/obproxy_shard_utils.h"
#include "stat/ob_processor_stats.h"
#include "stat/ob_resource_pool_stats.h"
#include "obutils/ob_resource_pool_processor.h"
#include "proxy/client/ob_client_vc.h"
#include "proxy/route/ob_mysql_route.h"
#include "proxy/mysqllib/ob_proxy_session_info_handler.h"
#include "proxy/mysqllib/ob_mysql_request_builder.h"
#include "proxy/mysqllib/ob_mysql_response_builder.h"
#include "proxy/api/ob_plugin_vc.h"
#include "proxy/mysql/ob_mysql_debug_names.h"
#include "proxy/mysql/ob_prepare_statement_struct.h"
#include "cmd/ob_show_sqlaudit_handler.h"
#include "cmd/ob_show_databases_handler.h"
#include "cmd/ob_show_tables_handler.h"
#include "cmd/ob_select_database_handler.h"
#include "cmd/ob_show_topology_handler.h"
#include "cmd/ob_show_db_version_handler.h"
#include "obutils/ob_tenant_stat_manager.h"
#include "prometheus/ob_net_prometheus.h"
#include "prometheus/ob_sql_prometheus.h"
#include "lib/profile/ob_trace_id.h"
#include "obutils/ob_proxy_config.h"
#include "qos/ob_proxy_qos_stat_processor.h"
#include "obutils/ob_proxy_config_processor.h"
#include "proxy/mysqllib/ob_mysql_packet_rewriter.h"
#include "iocore/net/ob_ssl_processor.h"
#include "packet/ob_mysql_packet_writer.h"
#include "dbconfig/ob_proxy_db_config_info.h"
#include "optimizer/ob_proxy_optimizer_processor.h"
#include "dbconfig/ob_proxy_pb_utils.h"
#include "proxy/mysql/ob_mysql_global_session_manager.h"

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::packet;
using namespace oceanbase::obproxy::prometheus;
using namespace oceanbase::obproxy::qos;
using namespace oceanbase::obproxy::dbconfig;
using namespace oceanbase::obproxy::engine;
using namespace oceanbase::obproxy::optimizer;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
using namespace obmysql;

#define MYSQL_INCREMENT_TRANS_STAT(X) ObMysqlTransact::update_stat(trans_state_, X, 1);
#define MYSQL_SUM_TRANS_STAT(X, S) ObMysqlTransact::update_stat(trans_state_, X, S);

// We have a debugging list that can use to find stuck
// state machines
DLL<ObMysqlSM> g_debug_sm_list;
ObMutex g_debug_sm_list_mutex;

// _instantiate_func is called from the fast allocator to initialize
// newly-allocated ObMysqlSM objects.  By default, the fast allocators
// just memcpys the entire prototype object, but this function does
// sparse initialization, not copying dead space for history.
//
// Most of the content of in the prototype object consists of zeroes.
// To take advantage of that, a "scatter list" is constructed of
// the non-zero words, and those values are scattered onto the
// new object after first zeroing out the object (except for dead space).
//
// make_scatter_list should be called only once (during static
// initialization, since it isn't thread safe).

const int64_t ObMysqlSM::MAX_SCATTER_LEN = (sizeof(ObMysqlSM) / sizeof(int64_t));
static int64_t val[ObMysqlSM::MAX_SCATTER_LEN];
static int16_t to[ObMysqlSM::MAX_SCATTER_LEN];
static int64_t scat_count = 0;

ObMysqlSMListBucket g_mysqlsm_list[MYSQL_SM_LIST_BUCKETS];

void ObMysqlSM::make_scatter_list(ObMysqlSM &prototype)
{
  int64_t *p = reinterpret_cast<int64_t *>(&prototype);

  for (int64_t i = 0; i < MAX_SCATTER_LEN; ++i) {
    if (0 != p[i]) {
      to[scat_count] = static_cast<int16_t>(i);
      val[scat_count] = p[i];
      ++scat_count;
    }
  }
}

void ObMysqlSM::instantiate_func(ObMysqlSM &prototype, ObMysqlSM &new_instance)
{
  const int64_t history_len = sizeof(prototype.history_);
  const int64_t total_len = sizeof(ObMysqlSM);
  const int64_t pre_history_len =
      reinterpret_cast<char *>(&(prototype.history_)) - reinterpret_cast<char *>(&prototype);
  const int64_t post_history_len = total_len - history_len - pre_history_len;
  const int64_t post_offset = pre_history_len + history_len;

  memset(reinterpret_cast<char *>(&new_instance), 0, pre_history_len);
  memset(reinterpret_cast<char *>(&new_instance) + post_offset, 0, post_history_len);

  int64_t *pd = reinterpret_cast<int64_t *>(&new_instance);

  for (int64_t i = 0; i < scat_count; ++i) {
    pd[to[i]] = val[i];
  }
}

#define MYSQL_INCREMENT_TRANS_STAT(X) ObMysqlTransact::update_stat(trans_state_, X, 1);

#define __REMEMBER(x)  #x
#define _REMEMBER(x)   __REMEMBER(x)

#define REMEMBER(e, r) {                                        \
    add_history_entry(__FILE__ ":" _REMEMBER (__LINE__), e, r); \
  }

#define STATE_ENTER(state_name, e) { \
  REMEMBER (e, reentrancy_count_); \
  int64_t stack_start = event::self_ethread().stack_start_; \
  _PROXY_SM_LOG(DEBUG, "sm_id=%u, stack_size=%ld, next_action=%s, event=%s", \
                sm_id_, stack_start - reinterpret_cast<int64_t>(&stack_start), \
                #state_name, ObMysqlDebugNames::get_event_name(e)); }

#define MYSQL_SM_SET_DEFAULT_HANDLER(h) {   \
  REMEMBER(-1,reentrancy_count_);         \
  default_handler_ = h;}

ObMysqlSM::ObMysqlSM()
    : ObContinuation(NULL), sm_id_(0), magic_(MYSQL_SM_MAGIC_DEAD),
      trans_state_(), client_session_(NULL), sm_cluster_resource_(NULL),
      is_updated_stat_(false), is_in_list_(false), hooks_set_(false), history_pos_(0),
      tunnel_(), client_entry_(NULL), client_buffer_reader_(NULL),
      server_entry_(NULL), server_session_(NULL),
      server_buffer_reader_(NULL),
      default_handler_(NULL), pending_action_(NULL), reentrancy_count_(0),
      terminate_sm_(false), kill_this_async_done_(false), handling_ssl_request_(false),
      need_renew_cluster_resource_(false), is_in_trans_(true),
      retry_acquire_server_session_count_(0), start_acquire_server_session_time_(0)
{
  static bool scatter_inited = false;

  memset(&history_, 0, sizeof(history_));
  memset(&vc_table_, 0, sizeof(vc_table_));

  if (!scatter_inited) {
    make_scatter_list(*this);
    scatter_inited = true;
  }
}

inline void ObMysqlSM::cleanup()
{
  //analyzer_.destroy(); // needn't destroy, it will be initialized when sm is initializing
  compress_analyzer_.reset(); // will free mem, do not forget
  compress_ob20_analyzer_.reset();
  //request_analyzer_.reset(); // no need
  api_.destroy();
  trans_state_.destroy();
  mutex_.release();
  tunnel_.mutex_.release();
  magic_ = MYSQL_SM_MAGIC_DEAD;
  if (NULL != sm_cluster_resource_) {
    LOG_DEBUG("sm cluser resouce will dec ref", K_(sm_cluster_resource), KPC_(sm_cluster_resource));
    sm_cluster_resource_->dec_ref();
    sm_cluster_resource_ = NULL;
  }
}

inline void ObMysqlSM::destroy()
{
  cleanup();
  op_thread_free(ObMysqlSM, this, get_sm_allocator());
}

int ObMysqlSM::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(trans_state_.init(this))){
    LOG_WARN("failed to init transaction state", K_(sm_id), K(ret));
  } else {
    magic_ = MYSQL_SM_MAGIC_ALIVE;
    sm_id_ = get_next_sm_id();
    api_.sm_ = this;

    SET_HANDLER(&ObMysqlSM::main_handler);

#ifdef USE_MYSQL_DEBUG_LISTS
    if (OB_SUCCESS == mutex_acquire(&g_debug_sm_list_mutex)) {
      g_debug_sm_list.push(this);
      if (OB_SUCCESS != mutex_release(&g_debug_sm_list_mutex)) {
        LOG_WARN("failed to release mutex", K_(sm_id));
      }
    }
#endif
  }
  return ret;
}

int ObMysqlSM::state_add_to_list(int event, void *data)
{
  UNUSED(data);
  // The list if for general debugging The config
  // variable exists mostly to allow us to
  // measure an performance drop during benchmark runs
  if (trans_state_.mysql_config_params_->enable_mysqlsm_info_) {
    if (is_in_list_) {
      terminate_sm_ = true;
      LOG_ERROR("state_add_to_list, it should not arrive here again", K_(is_in_list), K_(sm_id));
    } else if (OB_UNLIKELY(EVENT_NONE != event) && OB_UNLIKELY(EVENT_INTERVAL != event)) {
      terminate_sm_ = true;
      LOG_ERROR("state_add_to_list, unexpected event", K(event), K_(sm_id));
    } else {
      STATE_ENTER(ObMysqlSM::state_add_to_list, event);
      int64_t bucket = (sm_id_ % MYSQL_SM_LIST_BUCKETS);
      MUTEX_TRY_LOCK(lock, g_mysqlsm_list[bucket].mutex_, mutex_->thread_holding_);
      // the client_vc's timeout events can be triggered, so we should not
      // reschedule the mysql_sm when the lock is not acquired.
      // FIXME: the sm_list may miss some mysql_sm when the lock contention
      if (lock.is_locked()) {
        g_mysqlsm_list[bucket].sm_list_.push(this);
        is_in_list_ = true;
      }
    }
  }
  if (OB_LIKELY(!terminate_sm_)) {
    callout_api_and_start_next_action(ObMysqlTransact::SM_ACTION_API_SM_START);
  }
  return EVENT_DONE;
}

int ObMysqlSM::state_remove_from_list(int event, void *data)
{
  int ret = OB_SUCCESS;
  int event_ret = EVENT_DONE;
  bool is_done = false;
  UNUSED(data);
  // The config parameters are guaranteed maybe change because sm
  // across the life of a transaction
 if (is_in_list_) {
    STATE_ENTER(ObMysqlSM::state_remove_from_list, event);
    if (OB_UNLIKELY(EVENT_NONE != event) && OB_UNLIKELY(EVENT_INTERVAL != event)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("state_remove_from_list, unexpected event", K(event), K_(sm_id));
    } else {
      int64_t bucket = (sm_id_ % MYSQL_SM_LIST_BUCKETS);
      MUTEX_TRY_LOCK(lock, g_mysqlsm_list[bucket].mutex_, mutex_->thread_holding_);
      if (!lock.is_locked()) {
        MYSQL_SM_SET_DEFAULT_HANDLER(&ObMysqlSM::state_remove_from_list);
        if (OB_ISNULL(pending_action_ = mutex_->thread_holding_->schedule_in(this, HRTIME_MSECONDS(1)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to schedule in", K_(sm_id), K(ret));
        } else {
          LOG_DEBUG("fail to remove_from_list, reschedule it", K_(sm_id), K(bucket));
        }
      } else {
        g_mysqlsm_list[bucket].sm_list_.remove(this);
        is_in_list_ = false;
        pending_action_ = NULL; // clear the pending_action_(if has) assigned by schedule_in above
        is_done = true;
      }
    }
  } else {
    is_done = true;
  }

  if (is_done) {
    event_ret = kill_this_async_hook(EVENT_NONE, NULL);
  }
  return event_ret;
}

inline int ObMysqlSM::kill_this_async_hook(int event, void *data)
{
  UNUSED(event);
  UNUSED(data);
  // In the base ObMysqlSM, we don't have anything to
  // do here. subclasses can override this function
  // to do their own asynchronous cleanup
  // So We're now ready to finish off the state machine
  terminate_sm_ = true;
  kill_this_async_done_ = true;

  return EVENT_DONE;
}

inline void ObMysqlSM::refresh_cluster_resource()
{
  if (OB_LIKELY(NULL != client_session_)) {
    if (sm_cluster_resource_ != client_session_->cluster_resource_
        && OB_LIKELY(NULL != client_session_->cluster_resource_)) {
      client_session_->cluster_resource_->inc_ref();
      if (NULL != sm_cluster_resource_) {
        sm_cluster_resource_->dec_ref();
      }
      sm_cluster_resource_ = client_session_->cluster_resource_;
    }
    if (OB_LIKELY(NULL != sm_cluster_resource_)
        && OB_LIKELY(!client_session_->is_proxy_mysql_client_)) {
      sm_cluster_resource_->renew_last_access_time();
    }
  }
}

int ObMysqlSM::attach_client_session(
    ObMysqlClientSession &client_vc,
    ObIOBufferReader &buffer_reader,
    const bool is_new_conn /* = false */)
{
  int ret = OB_SUCCESS;
  ObNetVConnection *netvc = NULL;

  ++reentrancy_count_;

  if (client_vc.get_half_close_flag()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("client session is in half close state", K_(sm_id), K(ret));
  } else {
    milestones_.client_.client_begin_ = get_based_hrtime();
    client_session_ = &client_vc;
    mutex_ = client_vc.mutex_;

    refresh_cluster_resource();
    tunnel_.init(*this, *mutex_);

    // assign dummy entry valid time
    client_session_->dummy_entry_valid_time_ns_ = ObRandomNumUtils::get_random_half_to_full(
        trans_state_.mysql_config_params_->tenant_location_valid_time_);

    // Allocate a client entry in the state machine's vc table
    client_entry_ = vc_table_.new_entry();
    if (OB_ISNULL(client_entry_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to new client entry", K_(sm_id), K(ret));
    } else {
      client_entry_->vc_ = &client_vc;
      client_entry_->vc_type_ = MYSQL_CLIENT_VC;

      if (OB_ISNULL(netvc = client_vc.get_netvc())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("net vconnection of client session is NULL", K_(sm_id), K(ret));
      } else {
        trans_state_.client_info_.set_addr(netvc->get_remote_addr());
        trans_state_.is_auth_request_ = is_new_conn;
        trans_state_.is_trans_first_request_ = true;
        trans_state_.is_proxysys_tenant_ = client_vc.is_proxysys_tenant();

        // Record api hook set state
        hooks_set_ = client_vc.has_hooks();

        // Setup for parsing the header
        client_buffer_reader_ = &buffer_reader;
        client_entry_->vc_handler_ = &ObMysqlSM::state_client_request_read;

        // We first need to run the transaction start hook.  Since
        // this hook maybe asynchronous, we need to disable IO on
        // client but set the continuation to be the state machine
        // so if we get an timeout events the sm handles them
        client_entry_->read_vio_ = client_vc.do_io_read(this, 0, buffer_reader.mbuf_);
        if (OB_ISNULL(client_entry_->read_vio_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("client sesion failed to do io read", K_(client_entry_->read_vio), K_(sm_id), K(ret));
        } else {
          // set up timeouts
          // set inactive timeout to wait_timeout
          set_client_wait_timeout();

          // Add our state sm to the sm list
          state_add_to_list(EVENT_NONE, NULL);
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    terminate_sm_ = true;
  }

  // This is another external entry point and it is possible for the state
  // machine to get terminated while down the call chain from
  // state_add_to_list. So we need to use the reentrancy_count to prevent
  // cleanup there and do it here as we return to the external caller.
  if (terminate_sm_ && 1 == reentrancy_count_) {
    kill_this();
  } else {
    --reentrancy_count_;
    if (OB_UNLIKELY(reentrancy_count_ < 0)) {
      LOG_ERROR("invalid internal state", K_(reentrancy_count), K_(sm_id));
    }
  }
  return ret;
}

inline int ObMysqlSM::setup_client_request_read()
{
  int ret = OB_SUCCESS;

  if (client_entry_->vc_handler_ != &ObMysqlSM::state_client_request_read) {
    client_entry_->vc_handler_ = &ObMysqlSM::state_client_request_read;
  }

  //set read trigger and read_reschedule. sometimes the data already is in the io buffer
  if (!client_session_->is_proxy_mysql_client_) {
    static_cast<ObUnixNetVConnection *>(client_session_->get_netvc())->set_read_trigger();
  }

  // The request may already be in the buffer if
  // this a request from a keep-alive connection
  int64_t read_num = trans_state_.is_auth_request_ ? 36 : INT64_MAX;
  // can't distinguish first request is SSL Request or Login Request, so just read packet header
  if (OB_ISNULL(client_entry_->read_vio_ = client_session_->do_io_read(
              this, read_num, client_buffer_reader_->mbuf_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("client session failed to do_io_read", K_(sm_id), K(ret));
  } else {
    if (client_buffer_reader_->read_avail() > 0
        || (trans_state_.is_auth_request_ && trans_state_.is_trans_first_request_)) {
      LOG_INFO("the request already in buffer, continue to handle it",
               "buffer len", client_buffer_reader_->read_avail(),
               "is_auth_rquest", trans_state_.is_auth_request_);
      handle_event(VC_EVENT_READ_READY, client_entry_->read_vio_);
    }
  }
  return ret;
}

int ObMysqlSM::state_client_request_read(int event, void *data)
{
  int ret = OB_SUCCESS;
  int event_ret = VC_EVENT_NONE;

  STATE_ENTER(ObMysqlSM::state_client_request_read, event);

  // set net_read_timeout when client begin to read
  set_client_net_read_timeout();

  if (trans_state_.is_trans_first_request_) {
    if (NULL != client_session_) {
      client_session_->is_waiting_trans_first_request_ = false;
    }

    trans_state_.refresh_mysql_config();
    refresh_cluster_resource();
    if (0 == milestones_.trans_start_) {
      milestones_.trans_start_ = get_based_hrtime();
      MYSQL_INCREMENT_DYN_STAT(TOTAL_TRANSACTION_COUNT);
      MYSQL_INCREMENT_DYN_STAT(CURRENT_CLIENT_TRANSACTIONS);
    }
  }
  if (0 == milestones_.client_.client_begin_) {
    milestones_.client_.client_begin_ = get_based_hrtime();
  }

  if (client_entry_->read_vio_ != reinterpret_cast<ObVIO *>(data)
      || (NULL != server_entry_)
      || (NULL != server_session_)
      || (client_entry_->eos_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("invalid internal state", K_(client_entry_->read_vio), K(data), K_(server_entry),
             K_(server_session), K_(client_entry_->eos));
  } else {
    switch (event) {
      case VC_EVENT_READ_READY:
      case VC_EVENT_READ_COMPLETE:
        // More data to parse
        break;

      case VC_EVENT_EOS: {
        client_entry_->eos_ = true;
        LOG_INFO("ObMysqlSM::state_client_request_read", "event",
                 ObMysqlDebugNames::get_event_name(event), K_(sm_id),
                 "client_vc", client_session_ == NULL ? "NULL" : P(client_session_->get_netvc()));
        if (0 == cmd_size_stats_.client_request_bytes_) {
          ret = OB_CONNECT_ERROR;
          // The client is closed. Close it.
          trans_state_.client_info_.abort_ = ObMysqlTransact::ABORTED;
        }
        break;
      }

        // fall through
      case VC_EVENT_INACTIVITY_TIMEOUT: {
        // if proxy is doing gracful exit, no need print WARN
        if (get_global_hot_upgrade_info().is_graceful_exit_timeout(get_based_hrtime())) {
          LOG_INFO("ObMysqlSM::state_client_request_read", "event",
                   ObMysqlDebugNames::get_event_name(event), K_(sm_id),
                   "client_vc", client_session_ == NULL ? "NULL" : P(client_session_->get_netvc()));
        } else {
          LOG_WARN("ObMysqlSM::state_client_request_read", "event",
                   ObMysqlDebugNames::get_event_name(event), K_(sm_id),
                   "client_vc", client_session_ == NULL ? "NULL" : P(client_session_->get_netvc()));
        }
        ret = OB_CONNECT_ERROR;
        // The client is closed. Close it.
        trans_state_.client_info_.abort_ = ObMysqlTransact::ABORTED;
        break;
      }

      case VC_EVENT_ERROR:
      case VC_EVENT_ACTIVE_TIMEOUT: {
        LOG_WARN("ObMysqlSM::state_client_request_read", "event",
                 ObMysqlDebugNames::get_event_name(event), K_(sm_id),
                 "client_vc", client_session_ == NULL ? "NULL" : P(client_session_->get_netvc()));
        ret = OB_CONNECT_ERROR;
        // The client is closed. Close it.
        trans_state_.client_info_.abort_ = ObMysqlTransact::ABORTED;
        break;
      }

      default:
        ret = OB_INNER_STAT_ERROR;
        LOG_ERROR("unexpected event", K(event), K_(sm_id), K(ret));
        break;
    }
  }


  if (OB_SUCC(ret) && !client_session_->is_proxy_mysql_client_) {
    ObNetVConnection *vc = client_session_->get_netvc();

    if (OB_LIKELY(NULL != vc) && vc->options_.sockopt_flags_ != trans_state_.mysql_config_params_->client_sock_option_flag_out_) {
      vc->options_.sockopt_flags_ = static_cast<uint32_t>(trans_state_.mysql_config_params_->client_sock_option_flag_out_);
      if (vc->options_.sockopt_flags_ & ObNetVCOptions::SOCK_OPT_KEEP_ALIVE) {
        vc->options_.set_keepalive_param(static_cast<int32_t>(trans_state_.mysql_config_params_->client_tcp_keepidle_),
              static_cast<int32_t>(trans_state_.mysql_config_params_->client_tcp_keepintvl_),
              static_cast<int32_t>(trans_state_.mysql_config_params_->client_tcp_keepcnt_),
              static_cast<int32_t>(trans_state_.mysql_config_params_->client_tcp_user_timeout_));
      }
      if (OB_FAIL(vc->apply_options())) {
        LOG_WARN("client session failed to apply per-transaction socket options", K_(sm_id), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObMysqlAnalyzeStatus status = ANALYZE_CONT;
    analyze_mysql_request(status);

    cmd_size_stats_.client_request_bytes_ = client_buffer_reader_->read_avail();

    // Check to see if we are done parsing the whole request
    if ((ANALYZE_CONT != status
        || client_entry_->eos_
        || (ANALYZE_CONT == status && VC_EVENT_READ_COMPLETE == event && !trans_state_.is_auth_request_))
        && !handling_ssl_request_) {
      client_entry_->vc_handler_ = &ObMysqlSM::state_watch_for_client_abort;
      milestones_.client_.client_read_end_ = get_based_hrtime();
      cmd_time_stats_.client_request_read_time_ += (milestones_.client_.client_read_end_ - milestones_.client_.client_begin_);
    }

    switch (status) {
      case ANALYZE_OBPARSE_ERROR:
        //must read all data of the request, otherwise will read the remain request data when recv next request
        trans_state_.mysql_errcode_ = OB_ERR_PARSE_SQL;
        trans_state_.mysql_errmsg_ = ob_strerror(OB_ERR_PARSER_SYNTAX);
        if (OB_FAIL(ObMysqlTransact::build_error_packet(trans_state_))) {
          LOG_WARN("fail to build err resp", K(ret));
        } else if (OB_FAIL(client_buffer_reader_->consume_all())) {
          LOG_WARN("fail to consume all", K_(sm_id), K(ret));
        } else {
          trans_state_.next_action_ = ObMysqlTransact::SM_ACTION_INTERNAL_NOOP;
          callout_api_and_start_next_action(ObMysqlTransact::SM_ACTION_API_SEND_RESPONSE);
        }
        break;
      case ANALYZE_OBUNSUPPORT_ERROR:
        //must read all data of the request, otherwise will read the remain request data when recv next request
        trans_state_.mysql_errcode_ = OB_ERROR_UNSUPPORT_EXPR_TYPE;
        trans_state_.mysql_errmsg_ = ob_strerror(OB_ERROR_UNSUPPORT_EXPR_TYPE);
        if (OB_FAIL(ObMysqlTransact::build_error_packet(trans_state_))) {
          LOG_WARN("fail to build err resp", K(ret));
        } else if (OB_FAIL(client_buffer_reader_->consume_all())) {
            LOG_WARN("fail to consume all", K_(sm_id), K(ret));
        } else {
          trans_state_.next_action_ = ObMysqlTransact::SM_ACTION_INTERNAL_NOOP;
          callout_api_and_start_next_action(ObMysqlTransact::SM_ACTION_API_SEND_RESPONSE);
        }
        break;
      case ANALYZE_ERROR:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error parsing client request", K(ret), K_(sm_id));
        set_client_abort(ObMysqlTransact::ABORTED, event);

        // Disable further I/O on the client
        client_entry_->read_vio_->nbytes_ = client_entry_->read_vio_->ndone_;
        break;

      case ANALYZE_CONT:
        if (client_entry_->eos_) {
          ret = OB_CONNECT_ERROR;
          LOG_WARN("EOS before client request parsing finished", K_(sm_id), K(ret));
          set_client_abort(ObMysqlTransact::ABORTED, event);

          // Disable further I/O on the client
          client_entry_->read_vio_->nbytes_ = client_entry_->read_vio_->ndone_;
        } else if (VC_EVENT_READ_COMPLETE == event) {
          LOG_DEBUG("VC_EVENT_READ_COMPLETE and ANALYZE CONT status", K_(sm_id));
          if (trans_state_.is_auth_request_) {
            client_entry_->read_vio_->reenable();
            if (OB_ISNULL(client_entry_->read_vio_ = client_session_->do_io_read(this,
                    INT64_MAX, client_buffer_reader_->mbuf_))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("client session failed to do_io_read", K_(sm_id), K(ret));
            }
          }
        } else {
          MYSQL_INCREMENT_DYN_STAT(TOTAL_CLIENT_REQUEST_REREAD_COUNT);
          int64_t request_len = trans_state_.trans_info_.client_request_.get_packet_len();
          if (request_len > 0 && request_len > client_buffer_reader_->mbuf_->water_mark_) {
            // ensure the read buffer can cache the whole reqeust
            client_buffer_reader_->mbuf_->water_mark_ = request_len;
          }
          if (trans_state_.trans_info_.client_request_.is_large_request()) {
            // Disable further I/O on the client since there could
            // be rest request body that we are tunneling, and we can't issue
            // another IO later for the rest request body with a different buffer
            client_entry_->read_vio_->nbytes_ = client_entry_->read_vio_->ndone_;
            trans_state_.trans_info_.request_content_length_ = request_len;

            MYSQL_INCREMENT_TRANS_STAT(CLIENT_LARGE_REQUESTS);
            LOG_DEBUG("large request",
                      K_(sm_id), K(request_len),
                      "saved_request_len", client_buffer_reader_->read_avail());

            client_entry_->vc_handler_ = &ObMysqlSM::state_watch_for_client_abort;

            if (!client_session_->active_) {
              client_session_->active_ = true;
              MYSQL_INCREMENT_DYN_STAT(CURRENT_ACTIVE_CLIENT_CONNECTIONS);
            }
            // do not warry about cluster resource, large request will allways behind
            // the auth request.

            call_transact_and_set_next_state(ObMysqlTransact::modify_request);
          } else {
            client_entry_->read_vio_->reenable();
            event_ret = VC_EVENT_CONT;
          }
        }
        break;

      case ANALYZE_DONE: {
        ObServerRoutingMode mode = trans_state_.mysql_config_params_->server_routing_mode_;
        LOG_DEBUG("done parsing client request",
                  K_(sm_id), "routing_mode", ObProxyConfig::get_routing_mode_str(mode));

        if (!client_session_->active_) {
          client_session_->active_ = true;
          MYSQL_INCREMENT_DYN_STAT(CURRENT_ACTIVE_CLIENT_CONNECTIONS);
        }

        // We read the whole mysql request packet and then analyze it, so there is not
        // data from client after reading a whole mysql request packet. The packet
        // size is assured. Enable further IO to watch for client aborts
        if (handling_ssl_request_) {
          client_entry_->read_vio_->nbytes_ = INT64_MAX;
        }
        client_entry_->read_vio_->reenable();

        // cancel client net_read_timeout, set to wait_timeout
        set_client_wait_timeout();

        // no request data to read, reset read trigger and avoid unnecessary reading
        ObUnixNetVConnection* vc = static_cast<ObUnixNetVConnection *>(client_session_->get_netvc());
        if (!client_session_->is_proxy_mysql_client_) {
          if (!handling_ssl_request_) {
            vc->reset_read_trigger();
          }
        }

        if (!handling_ssl_request_) {
          bool need_direct_response_for_client = false;
          ObClientSessionInfo &session_info = client_session_->get_session_info();
          if (session_info.is_sharding_user()) {
            if (OB_FAIL(handle_shard_request(need_direct_response_for_client))) {
              LOG_ERROR("handle shard request failed", K(ret));
            } else if (need_direct_response_for_client) {
              if (OB_FAIL(client_buffer_reader_->consume_all())) {
                LOG_WARN("fail to consume all", K_(sm_id), K(ret));
              } else {
                trans_state_.next_action_ = ObMysqlTransact::SM_ACTION_INTERNAL_NOOP;
                callout_api_and_start_next_action(ObMysqlTransact::SM_ACTION_API_SEND_RESPONSE);
              }
            } else {
              if (NULL == client_session_->get_sharding_select_log_plan()) {
                if (client_session_->get_session_info().is_oceanbase_server()) {
                  setup_get_cluster_resource();
                } else {
                  setup_set_cached_variables();
                }
              } else {
                if (OB_FAIL(setup_handle_execute_plan())) {
                  LOG_WARN("fail to setup handle request shard scan", K_(sm_id), K(ret));
                }
              }
            }
          } else {
            if (get_global_proxy_config().enable_qos
                && !client_session_->is_proxy_mysql_client_
                && OB_FAIL(handle_limit(need_direct_response_for_client))) {
              LOG_WARN("fail to handle limit", K(ret));
            }

            if (OB_SUCC(ret) && !need_direct_response_for_client
                && !client_session_->is_proxy_mysql_client_
                && get_global_proxy_config().enable_ldg
                && OB_FAIL(handle_ldg(need_direct_response_for_client))) {
              LOG_WARN("fail to handle ldg", K(ret));
            }

            if (OB_SUCC(ret) && need_direct_response_for_client) {
              if (OB_FAIL(client_buffer_reader_->consume_all())) {
                LOG_WARN("fail to consume all", K_(sm_id), K(ret));
              } else {
                trans_state_.next_action_ = ObMysqlTransact::SM_ACTION_INTERNAL_NOOP;
                callout_api_and_start_next_action(ObMysqlTransact::SM_ACTION_API_SEND_RESPONSE);
              }
            }

            if (OB_SUCC(ret) && !need_direct_response_for_client) {
              if (client_session_->get_session_info().is_oceanbase_server()) {
                setup_get_cluster_resource();
              } else {
                setup_set_cached_variables();
              }
            }
          }
        } else {
          handling_ssl_request_ = false;
        }

        break;
      }
      default:
        ret = OB_INNER_STAT_ERROR;
        LOG_ERROR("unknown analyze mysql request status", K(status), K_(sm_id), K(ret));
        break;
    }
  }

  if (OB_FAIL(ret)) {
    trans_state_.inner_errcode_ = ret;
    call_transact_and_set_next_state(ObMysqlTransact::bad_request);
  }

  return event_ret;
}

int ObMysqlSM::setup_handle_execute_plan()
{
  int ret = OB_SUCCESS;

  LOG_DEBUG("setup handle execute plan");
  MYSQL_SM_SET_DEFAULT_HANDLER(&ObMysqlSM::state_handle_execute_plan);

  ObShardingSelectLogPlan* plan = NULL;
  ObProxyOperator* operator_root = NULL;

  int64_t total_len = client_buffer_reader_->read_avail();
  if (total_len > trans_state_.trans_info_.client_request_.get_packet_meta().pkt_len_) {
    total_len = trans_state_.trans_info_.client_request_.get_packet_meta().pkt_len_;
  }

  ObHRTime execute_timeout = client_session_->get_session_info().get_query_timeout();

  // consume data in client buffer reader
  if (OB_FAIL(client_buffer_reader_->consume(total_len))) {
    LOG_WARN("fail to consume all", K_(sm_id), K(ret));
  } else if (OB_ISNULL(plan = client_session_->get_sharding_select_log_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select log plan should not be null", K_(sm_id), K(ret));
  } else if (OB_ISNULL(operator_root = plan->get_plan_root())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator should not be null", K_(sm_id), K(ret));
  } else if (OB_NOT_NULL(pending_action_)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pending_action must be NULL here", K_(pending_action), K_(sm_id), K(ret));
  } else if (OB_FAIL(operator_root->open(this, pending_action_, hrtime_to_msec(execute_timeout)))) {
    LOG_WARN("fail to open operator", K_(sm_id), K(ret));
  } else if (OB_FAIL(operator_root->get_next_row())) {
    LOG_WARN("fail to get next row", K_(sm_id), K(ret));
  } else if (OB_ISNULL(pending_action_)) {
    LOG_WARN("pending action should not be null", K_(sm_id), K(ret));
  } else {
    client_session_->set_inactivity_timeout(execute_timeout);
  }

  return ret;
}

int ObMysqlSM::state_handle_execute_plan(int event, void *data)
{
  int ret = OB_SUCCESS;

  STATE_ENTER(ObMysqlSM::state_handle_execute_plan, event);

  ObShardingSelectLogPlan* plan = NULL;
  ObProxyOperator* operator_root = NULL;
  pending_action_ = NULL;

  switch (event) {
    case VC_EVENT_READ_COMPLETE:
      if (OB_ISNULL(data)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("data is NULL", K_(sm_id), K(ret));
      } else if (OB_FAIL(process_executor_result(reinterpret_cast<ObProxyResultResp*>(data)))) {
        LOG_WARN("fail to process executor result, will disconnect", K_(sm_id), K(ret));
      } else {
        trans_state_.next_action_ = ObMysqlTransact::SM_ACTION_INTERNAL_NOOP;
        callout_api_and_start_next_action(ObMysqlTransact::SM_ACTION_API_SEND_RESPONSE);
      }
      break;
    case VC_EVENT_EOS:
    case VC_EVENT_ACTIVE_TIMEOUT:
    case VC_EVENT_INACTIVITY_TIMEOUT:
      LOG_WARN("handle execute plan meet error, will disconnect", K_(sm_id),
              "event", ObMysqlDebugNames::get_event_name(event), K(ret));
      ret = OB_CONNECT_ERROR;
      break;
    case VC_EVENT_ERROR:
      LOG_WARN("handle execute plan meet error, will disconnect", K_(sm_id),
               "event", ObMysqlDebugNames::get_event_name(event), K(ret));
      ret = OB_ERR_UNEXPECTED;
      break;
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("Unexpected event", K_(sm_id), K(event), K(ret));
      break;
    }
  }

  if (OB_ISNULL(plan = client_session_->get_sharding_select_log_plan())) {
    LOG_WARN("select log plan should not be null", K_(sm_id), K(ret));
  } else if (OB_ISNULL(operator_root = plan->get_plan_root())) {
    LOG_WARN("operator should not be null", K_(sm_id), K(ret));
  } else {
    operator_root->close();
  }

  if (OB_FAIL(ret)) {
    trans_state_.free_internal_buffer();
    trans_state_.inner_errcode_ = ret;
    call_transact_and_set_next_state(ObMysqlTransact::handle_error_jump);
  }

  return VC_EVENT_NONE;
}

int ObMysqlSM::process_executor_result(ObProxyResultResp *result_resp)
{
  int ret = OB_SUCCESS;

  ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
  uint8_t seq = static_cast<uint8_t>(client_request.get_packet_meta().pkt_seq_ + 1);
  ObMIOBuffer *buf = NULL;

  if (NULL != trans_state_.internal_buffer_) {
    buf = trans_state_.internal_buffer_;
  } else {
    if (OB_FAIL(trans_state_.alloc_internal_buffer(MYSQL_BUFFER_SIZE))) {
      LOG_WARN("fail to allocate internal buffer", K(ret));
    } else {
      buf = trans_state_.internal_buffer_;
    }
  }

  if (OB_SUCC(ret)) {
    if (result_resp->is_resultset_resp()) {
      if (OB_FAIL(build_executor_resp(buf, seq, result_resp))) {
        LOG_WARN("fail to build shard scan resp", K(ret));
      }
    } else {
      if (OB_FAIL(ObMysqlPacketUtil::encode_err_packet_buf(*buf, seq, result_resp->get_err_code(), result_resp->get_err_msg()))) {
        LOG_WARN("fail to encode err pacekt buf", K(seq), "errmsg", result_resp->get_err_msg(),
                 "errcode", result_resp->get_err_code(), K(ret));
      }
    }
  }

  return ret;
}

int ObMysqlSM::build_executor_resp(ObMIOBuffer *write_buf, uint8_t &seq, ObProxyResultResp *result_resp)
{
  int ret = OB_SUCCESS;
  int64_t column_count = result_resp->get_column_count();

  // header , cols , first eof
  if (OB_SUCC(ret)) {
    ObSEArray<ObMySQLField, 1, common::ObIAllocator&> *fields = NULL;
    if (OB_FAIL(result_resp->get_fields(fields))) {
      LOG_WARN("faild to push field", K(fields), K(ret));
    } else if (OB_ISNULL(fields)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fields should not be null", K(ret));
    } else if (OB_UNLIKELY(fields->count() != column_count)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fields count should equal column_count", K(column_count), "fileds count", fields->count(), K(ret));
    } else if (OB_FAIL(ObMysqlPacketUtil::encode_header(*write_buf, seq, *fields))) {
      LOG_WARN("faild to encode header", K(fields), K(seq), K(ret));
    }
  }

  // rows
  if (OB_SUCC(ret)) {
    ObObj *objs = NULL;
    int64_t buf_len = sizeof(ObObj) * column_count;

    if (OB_ISNULL(objs = static_cast<ObObj *>(op_fixed_mem_alloc(buf_len)))) {
      LOG_WARN("faild to alloc obj array", K(column_count), K(ret));
    } else {
      ObNewRow row;
      //ObSEArray<common::ObObj, 4> *row_array;
      ObSEArray<common::ObObj*, 4, common::ObIAllocator&> *row_array;
      while ((OB_SUCC(ret)) && (OB_SUCC(result_resp->next(row_array)))) {
        if (OB_ISNULL(row_array)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("row_array should not be null", K(ret));
        } else if (OB_UNLIKELY(row_array->count() != column_count)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("row_array count should equal column_count", K(column_count), "row_array count", row_array->count(), K(ret));
        } else {
          for (int64_t i = 0; i < column_count; i++) {
            objs[i] = *(row_array->at(i));
          }

          row.cells_ = objs;
          row.count_ = column_count;
          if (OB_FAIL(ObMysqlPacketUtil::encode_row_packet(*write_buf, seq, row))) {
            LOG_WARN("faild to encode row", K(seq), K(row), K(ret));
          } else {
            row.reset();
            row_array = NULL;
          }
        }
      }

      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }

    if (OB_NOT_NULL(objs)) {
      op_fixed_mem_free(objs, buf_len);
      objs = NULL;
    }
  }

  // second eof
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObMysqlPacketUtil::encode_eof_packet(*write_buf, seq))) {
      LOG_WARN("faild to encode row", K(seq), K(ret));
    }
  }

  return ret;
}

int ObMysqlSM::handle_shard_request(bool &need_response_for_stmt)
{
  int ret = OB_SUCCESS;

  need_response_for_stmt = false;
  ObClientSessionInfo &session_info = client_session_->get_session_info();
  ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
  ObSqlParseResult &parse_result = client_request.get_parse_result();
  ObMySQLCmd &req_cmd = trans_state_.trans_info_.sql_cmd_;

  session_info.set_allow_use_last_session(true);

  ObHSRResult &hsr = session_info.get_login_req().get_hsr_result();
  ObDbConfigLogicDb *db_info = NULL;

  if (!session_info.is_sharding_user()
      || parse_result.is_shard_special_cmd()
      || ObProxyShardUtils::is_special_db(trans_state_)) {
    // do nothing
  } else {
    if (obmysql::OB_MYSQL_COM_FIELD_LIST == req_cmd) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("ddl stmt is unsupported for sharding table", K(ret));
    } else if (obmysql::OB_MYSQL_COM_QUERY != req_cmd) {
      //do nothing
    } else if (parse_result.is_invalid_stmt() && parse_result.has_shard_comment()) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("fail to parse shard sql with shard comment",
               "sql", client_request.get_sql(), K(ret));
    } else {
      //  get logic db and check auth
      if (OB_FAIL(ObProxyShardUtils::get_logic_db_info(trans_state_,
                      client_session_->get_session_info(),
                      db_info))) {
        LOG_WARN("fail to get logic database", K(ret));
      } else if (OB_ISNULL(db_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("db info is null", K(ret));
      } else if (OB_FAIL(ObProxyShardUtils::check_shard_request(*client_session_, parse_result,
                    *db_info))) {
        LOG_WARN("fail to check shard request", K(ret));
      } else if (parse_result.is_dual_request()) {
        //do nothing
      } else if (db_info->is_single_shard_db_table()) {
        // sindle db table
        if (OB_FAIL(ObProxyShardUtils::handle_single_shard_request(*client_session_,
                        trans_state_, *client_buffer_reader_, *db_info))) {
          LOG_WARN("fail to handle single shard request", K(ret));
        }
      } else if (OB_FAIL(ObProxyShardUtils::handle_shard_request(*client_session_,
                      trans_state_, *client_buffer_reader_, *db_info))) {
          LOG_WARN("fail to handle shard request", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
      int tmp_ret = ObProxyShardUtils::build_error_packet(ret, need_response_for_stmt, trans_state_);
      ret = OB_SUCC(tmp_ret) ? tmp_ret : ret;
    }
    if (OB_SUCC(ret) && !need_response_for_stmt) {
      if (OB_FAIL(save_user_login_info(session_info, hsr))) {
        LOG_WARN("fail to save user login info", K_(sm_id), K(ret));
      }
    }
    if (NULL != db_info) {
      db_info->dec_ref();
      db_info = NULL;
    }
  }
  return ret;
}

void ObMysqlSM::setup_set_cached_variables()
{
  int ret = OB_SUCCESS;

  if (OB_MYSQL_COM_LOGIN == trans_state_.trans_info_.sql_cmd_) {
    ObDefaultSysVarSet *default_sysvar_set = NULL;
    if (OB_ISNULL(default_sysvar_set = get_global_resource_pool_processor().get_default_sysvar_set())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("default_sysvar_set is null", K(ret));
    } else if (OB_FAIL(client_session_->get_session_info().add_sys_var_set(*default_sysvar_set))) {
      LOG_WARN("fail to add sys var set", K(ret));
    } else {
      LOG_DEBUG("succ to add sys var set", "cached_variables", client_session_->get_session_info().get_cached_variables());
    }
  }

  if (OB_FAIL(ret)) {
    // disconnect
    trans_state_.inner_errcode_ = ret;
    call_transact_and_set_next_state(ObMysqlTransact::handle_error_jump);
  } else {
    call_transact_and_set_next_state(ObMysqlTransact::modify_request);
  }
}

int ObMysqlSM::handle_limit(bool &need_response_for_client)
{
  int ret = OB_SUCCESS;
  bool is_pass = false;

  need_response_for_client = false;

  ObMySQLCmd &req_cmd = trans_state_.trans_info_.sql_cmd_;
  if (req_cmd == OB_MYSQL_COM_QUERY) {
    const ObString app_name(get_global_proxy_config().app_name_str_);
    ObProxyAppConfig *cur_config = get_global_proxy_config_processor().get_app_config(app_name);

    if (NULL != cur_config) {
      const ObClientSessionInfo &cs_info = client_session_->get_session_info();

      ObString limit_name;
      ObArenaAllocator calc_allocator;

      if (OB_FAIL(cur_config->calc_limit(trans_state_, cs_info,
                                         &calc_allocator, is_pass, limit_name))) {
        LOG_WARN("fail to calc limit", K(ret));
      }

      cur_config->dec_ref();
      cur_config = NULL;

      if (OB_SUCC(ret) && !is_pass) {
        char err_msg_buf[OB_MAX_ERROR_MSG_LEN] = "\0";
        int64_t pos = 0;
        if (OB_FAIL(databuff_printf(err_msg_buf, OB_MAX_ERROR_MSG_LEN, pos, ob_str_user_error(OB_ERR_LIMIT),
                                    limit_name.length(), limit_name.ptr()))) {
        } else {
          trans_state_.mysql_errcode_ = OB_ERR_LIMIT;
          trans_state_.mysql_errmsg_ = err_msg_buf;
          if (OB_FAIL(ObMysqlTransact::build_error_packet(trans_state_))) {
            LOG_WARN("fail to build err resp", K(ret));
          } else {
            need_response_for_client = true;
          }
        }
      }
    }
  }

  return ret;
}

int ObMysqlSM::handle_ldg(bool &need_response_for_client)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObProxyObInstance *instance = NULL;
  need_response_for_client = false;
  need_renew_cluster_resource_ = false;
  bool need_rewrite_login_req = false;
  ObString ldg_logical_cluster_name;
  ObString ldg_logical_tenant_name;
  ObString ldg_real_cluster_name;
  ObString ldg_real_tenant_name;
  ObClientSessionInfo &session_info = client_session_->get_session_info();
  ObHSRResult &hsr = session_info.get_login_req().get_hsr_result();

  if (OB_MYSQL_COM_LOGIN == trans_state_.trans_info_.sql_cmd_) {
    if (OB_FAIL(get_global_config_server_processor().get_ldg_primary_role_instance(
            hsr.tenant_name_, hsr.cluster_name_, instance))) {
      //normal tenant do not have LDC info, ignore
      if (OB_HASH_NOT_EXIST == ret) {
        LOG_DEBUG("get primary role instance failed", K(ret));
      } else {
        LOG_WARN("get primary role instance failed", K(ret));
      }
    } else if (OB_ISNULL(instance)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ob instance is null", K(ret));
    } else if (!instance->is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("instance is not valid", K(ret));
    } else {
      LOG_DEBUG("ldg login user", K(*instance), K(hsr.cluster_name_), K(hsr.tenant_name_));
      session_info.set_ldg_logical_cluster_name(hsr.cluster_name_);
      session_info.set_ldg_logical_tenant_name(hsr.tenant_name_);
      client_session_->set_using_ldg(true);
      need_rewrite_login_req = true;
    }
  } else if (client_session_->using_ldg()) {
    if (OB_FAIL(session_info.get_ldg_logical_cluster_name(ldg_logical_cluster_name))) {
      LOG_WARN("fail to get ldg logical cluster name", K(ret));
    } else if (OB_FAIL(session_info.get_ldg_logical_tenant_name(ldg_logical_tenant_name))) {
      LOG_WARN("fail to get ldg logical tenant name", K(ret));
    } else if (OB_FAIL(session_info.get_cluster_name(ldg_real_cluster_name))) {
      LOG_WARN("fail to get ldg real cluster name", K(ret));
    } else if (OB_FAIL(session_info.get_tenant_name(ldg_real_tenant_name))) {
      LOG_WARN("fail to get ldg real tenant name", K(ret));
    } else if (OB_FAIL(get_global_config_server_processor().get_ldg_primary_role_instance(
            ldg_logical_tenant_name, ldg_logical_cluster_name, instance))) {
      LOG_WARN("get primary role instance failed", K(ret));
    } else if (NULL == instance) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get ldg piramy role from ocp failed", K(ret));
    } else {
      if (!instance->is_valid() || OB_ISNULL(client_session_->cluster_resource_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ldg argument is not vaild", K(ret), KP(instance),
            KP(client_session_->cluster_resource_));
      } else if (!client_session_->cluster_resource_->check_tenant_valid(instance->ob_tenant_,
            instance->ob_cluster_)) {
        LOG_WARN("ldg check tenant valid failed", K_(instance->ob_tenant), K_(instance->ob_cluster));
      } else if (instance->ob_cluster_.get_string() != ldg_real_cluster_name
          || instance->ob_tenant_.get_string() != ldg_real_tenant_name) {
        if (ObMysqlTransact::is_in_trans(trans_state_)) {
          // do nothing, keep origin session
          LOG_WARN("in trans, keep origin session");
        } else {
          LOG_INFO("ldg change tenant ", K(ldg_real_cluster_name), K(ldg_real_tenant_name),
                    K(instance->ob_cluster_.get_string()), K(instance->ob_tenant_.get_string()));
          if (instance->ob_cluster_.get_string() != ldg_real_cluster_name) {
            need_renew_cluster_resource_ = true;
            client_session_->cluster_resource_->dec_ref();
            client_session_->cluster_resource_ = NULL;
          }

          client_session_->is_need_update_dummy_entry_ = true;
          need_rewrite_login_req =true;
        }
      }
    }
  }

  if (OB_SUCC(ret) && need_rewrite_login_req && NULL != instance) {
    if (OB_FAIL(ObProxySessionInfoHandler::rewrite_ldg_login_req(
            session_info, instance->ob_tenant_, instance->ob_cluster_))) {
      LOG_WARN("ldg rewrite ldg login req failed", K(ret));
      tmp_ret = ret;
    } else if (OB_FAIL(save_user_login_info(session_info, hsr))) {
      LOG_WARN("save user login info failed", K(ret), K(hsr), K(session_info));
      tmp_ret = ret;
    } else {
      LOG_INFO("handle ldg success", K(instance->ob_cluster_), K(instance->ob_tenant_));
    }
  }

  if (NULL != instance) {
    instance->dec_ref();
    instance = NULL;
  }

  if (OB_SUCCESS != ret) {
    // If there is a problem, connect to the previous cluster, otherwise the connection will fail and a business error will occur
    ret = tmp_ret;
    LOG_WARN("handle ldg failed, try to use old session according ret", K(ret));
  }

  return ret;
}

void ObMysqlSM::setup_get_cluster_resource()
{
  int ret = OB_SUCCESS;
  ObAction *cr_handler = NULL;

  if (OB_MYSQL_COM_LOGIN == trans_state_.trans_info_.sql_cmd_
      || need_renew_cluster_resource_
      || (client_session_->get_session_info().is_sharding_user() && NULL == client_session_->cluster_resource_)) {
    if (trans_state_.mysql_config_params_->is_mysql_routing_mode()) {
      // here must be in mysql routing mode
      trans_state_.current_.send_action_ = ObMysqlTransact::SERVER_SEND_LOGIN;
    } else if (!client_session_->is_proxysys_tenant() && (NULL != client_session_->cluster_resource_)) {
      if (client_session_->is_proxy_mysql_client_) {
        // normal, no need dec ref, hand to process_cluster_resource()
        ObClusterResource *cr = client_session_->cluster_resource_;
        client_session_->cluster_resource_ = NULL;
        ObClientSessionInfo &session_info = client_session_->get_session_info();
        const ObString &cluster_name = session_info.get_login_req().get_hsr_result().cluster_name_;
        const bool is_clustername_from_default = false;
        int64_t cluster_id = session_info.get_login_req().get_hsr_result().cluster_id_;
        ObProxyConfigString real_meta_cluster_name;
        ObConfigServerProcessor &csp = get_global_config_server_processor();
        if (OB_FAIL(csp.get_cluster_info(cluster_name, is_clustername_from_default,
                                         real_meta_cluster_name))) {
          LOG_WARN("fail to get cluster info, this connection will disconnect",
                   K_(sm_id), K(is_clustername_from_default), K(cluster_name), K(cluster_id), K(ret));
        } else if (OB_FAIL(session_info.set_cluster_info(get_global_proxy_config().enable_cluster_checkout,
                                                         cluster_name,
                                                         real_meta_cluster_name,
                                                         cluster_id,
                                                         client_session_->need_delete_cluster_))) {
          LOG_WARN("fail to set cluster info, this connection will disconnect",
                   K_(sm_id), K(cluster_name), K(cluster_id), K(ret));
        } else if (OB_FAIL(process_cluster_resource(cr))) {
          LOG_WARN("fail to process_cluster_resource", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("cluster resource must be NULL here", K(ret));
      }
    } else if (!client_session_->is_proxysys_tenant() && (NULL == client_session_->cluster_resource_)) {
      // if cluster don't exist, return err packet and disconnect
      // if config server has more than 1 cluster, in order to connect the correct cluster,
      // hsr must contain cluster name, otherwise return err packet and disconnect
      ObClientSessionInfo &session_info = client_session_->get_session_info();
      const ObHSRResult &hsr = session_info.get_login_req().get_hsr_result();
      const ObString &cluster_name = hsr.cluster_name_;

      // Attention! if login via vip and vip tenant cluster is valid, we think its cluster is not from default
      // and no need to tell whether it is multi clusters or not
      const bool is_clustername_from_default = (client_session_->is_need_convert_vip_to_tname()
                                                && client_session_->is_vip_lookup_success())
                                               ? false : hsr.is_clustername_from_default_;
      int64_t cluster_id = hsr.cluster_id_;
      ObProxyConfigString real_meta_cluster_name;
      ObConfigServerProcessor &csp = get_global_config_server_processor();
      if (OB_FAIL(csp.get_cluster_info(cluster_name, is_clustername_from_default,
                                       real_meta_cluster_name))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_CLUSTER_NOT_EXIST;
          LOG_WARN("cluster does not exist, this connection will disconnect",
                   K_(sm_id), K(is_clustername_from_default), K(cluster_name), K(ret));
          // return err packet to client "Access denied for ..."
          trans_state_.mysql_errcode_ = OB_PASSWORD_WRONG;
          int tmp_ret = OB_SUCCESS;
          if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = ObMysqlTransact::build_error_packet(trans_state_)))) {
            LOG_WARN("fail to build err packet", K(tmp_ret));
          } else {
            trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
          }
        } else {
          LOG_WARN("fail to get cluster info, this connection will disconnect",
                   K_(sm_id), K(is_clustername_from_default), K(cluster_name), K(ret));
        }
      } else if (OB_FAIL(session_info.set_cluster_info(get_global_proxy_config().enable_cluster_checkout,
                                                       cluster_name,
                                                       real_meta_cluster_name,
                                                       cluster_id,
                                                       client_session_->need_delete_cluster_))) {
        LOG_WARN("fail to set cluster info, this connection will disconnect",
                 K_(sm_id), K(cluster_name), K(cluster_id), K(ret));
      } else {
        MYSQL_SM_SET_DEFAULT_HANDLER(&ObMysqlSM::state_get_cluster_resource);
        milestones_.cluster_resource_create_begin_ = get_based_hrtime();

        ret = get_global_resource_pool_processor().get_cluster_resource(*this,
              (process_async_task_pfn)&ObMysqlSM::process_cluster_resource,
              client_session_->is_proxy_mysql_client_, cluster_name, cluster_id, cr_handler);
        if (OB_FAIL(ret)) {
          LOG_WARN("cluster_resource_handler is ACTION_RESULT_NONE, something is wrong",
                   K_(sm_id), K(cluster_name), K(cluster_id), K(ret));
        } else if (OB_SUCC(ret) && (NULL != cr_handler)) {
          LOG_DEBUG("should create and init cluster_resource and assign pending action",
                    K_(sm_id), K(cluster_name), K(cluster_id));
          if (OB_UNLIKELY(NULL != pending_action_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("pending_action must be NULL here", K_(pending_action), K_(sm_id), K(ret));
          } else {
            pending_action_ = cr_handler;
          }
        } else if (OB_SUCC(ret) && (NULL == cr_handler)) {
          // do nothing
        }
      }
    } else if (client_session_->is_proxysys_tenant()) {
      if (NULL == client_session_->cluster_resource_) {
        ObDefaultSysVarSet *default_sysvar_set = NULL;
        if (OB_FAIL(client_session_->fill_session_priv_info())) {
          LOG_WARN("fail to fill proxysys user session priv info", K(ret));
        } else if (OB_ISNULL(default_sysvar_set = get_global_resource_pool_processor().get_default_sysvar_set())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("default_sysvar_set is null", K(ret));
        } else if (OB_FAIL(client_session_->get_session_info().add_sys_var_set(*default_sysvar_set))) {
          LOG_WARN("fail to add sys var set", K(ret));
        } else {
          LOG_DEBUG("succ to add sys var set", "cached_variables", client_session_->get_session_info().get_cached_variables());
        }
      }
    } else {
      // do nothing
    }
  } else {
    // mainly for performance testing
    if (OB_MYSQL_COM_HANDSHAKE == trans_state_.trans_info_.sql_cmd_
        && trans_state_.mysql_config_params_->is_mysql_routing_mode()) {
      ObResourcePoolProcessor &rp_processor = get_global_resource_pool_processor();

      ObString cluster_name;
      if (OB_SUCC(rp_processor.get_first_cluster_name(cluster_name))) {
        LOG_DEBUG("get cluster resource in OB_MYSQL_COM_HANDSHAKE", K_(sm_id),
                  K(cluster_name), "tenant_name", OB_SYS_TENANT_NAME,
                  "user_name", OB_SYS_USER_NAME);

        ObClientSessionInfo &session_info = client_session_->get_session_info();
        if (OB_FAIL(session_info.set_user_name(ObString::make_string(OB_SYS_USER_NAME)))) {
          LOG_WARN("fail to set user name", K_(sm_id), K(ret));
        } else if (OB_FAIL(session_info.set_tenant_name(ObString::make_string(OB_SYS_TENANT_NAME)))) {
          LOG_WARN("fail to set tenant name", K_(sm_id), K(ret));
        } else if (OB_FAIL(session_info.set_cluster_name(cluster_name))) {
          // If there is no default cluster name, here maybe fail.
          LOG_WARN("fail to set cluster name", K_(sm_id), K(ret));
        } else if (OB_FAIL(session_info.set_database_name(ObString::make_string("test"), false))) {
          LOG_WARN("fail to set cluster name", K_(sm_id), K(ret));
        } else {
          // do nothing
        }

        if (OB_SUCC(ret)) {
          int64_t cluster_id = OB_INVALID_CLUSTER_ID;
          ret = get_global_resource_pool_processor().get_cluster_resource(*this,
                (process_async_task_pfn)&ObMysqlSM::process_cluster_resource,
                client_session_->is_proxy_mysql_client_, cluster_name, cluster_id, cr_handler);

          if (OB_SUCC(ret) && NULL != cr_handler) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("in mysql mode, we use default cluster resource, cr_handler must be null", K(cr_handler), K(ret));
            if (NULL != pending_action_) {
              LOG_WARN("pending_action must be NULL here", K_(pending_action), K_(sm_id), K(ret));
            } else {
              pending_action_ = cr_handler;
            }
          } else if (OB_FAIL(ret)) {
            LOG_WARN("fail to get cluster resource, will disconnect", K(ret));
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    // disconnect
    trans_state_.inner_errcode_ = ret;
    call_transact_and_set_next_state(ObMysqlTransact::handle_error_jump);
  } else if (NULL == pending_action_) {
    call_transact_and_set_next_state(ObMysqlTransact::modify_request);
  } else {
    // NULL != pending_action_, nothing, wait callback;
  }
}

int ObMysqlSM::process_cluster_resource(void *data)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("process cluster resource", K_(sm_id), K(data));
  if (OB_ISNULL(data) || OB_ISNULL(client_session_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(data), K_(client_session), K_(sm_id), K(ret));
  } else {
    ObClusterResource *cluster_resource = reinterpret_cast<ObClusterResource *>(data);
    if (cluster_resource->is_deleting()) { // maybe has already been deleted
      ret = OB_INNER_STAT_ERROR;
      LOG_WARN("this cluster resource has been deleted", K(ret), KPC(cluster_resource));
      cluster_resource->dec_ref();
      cluster_resource = NULL;
    } else {
      ObClientSessionInfo &session_info = client_session_->get_session_info();
      ObSysVarSetProcessor &sys_var_set_processor = cluster_resource->sys_var_set_processor_;
      if (OB_FAIL(session_info.revalidate_sys_var_set(sys_var_set_processor))) {
        LOG_WARN("fail to revalidate sys var set", K_(sm_id), K(ret));
        cluster_resource->dec_ref();
        cluster_resource = NULL;
      } else {
        if (!client_session_->is_proxy_mysql_client_) {
          cluster_resource->renew_last_access_time();
        }
        if (NULL != client_session_->cluster_resource_) {
          LOG_WARN("cluster resource must be NULl here, or will mem leak",
                   KPC(client_session_->cluster_resource_));
          client_session_->cluster_resource_->dec_ref();
          client_session_->cluster_resource_ = NULL;
        }

        // no need inc ref, outer has inc
        client_session_->cluster_resource_ = cluster_resource;
        if (NULL != sm_cluster_resource_) {
          sm_cluster_resource_->dec_ref();
          sm_cluster_resource_ = NULL;
        }
        client_session_->cluster_resource_->inc_ref();
        sm_cluster_resource_ = client_session_->cluster_resource_;
        if (OB_FAIL(client_session_->fill_session_priv_info())) {
          LOG_WARN("failed to fill session priv info", K_(sm_id), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObMysqlSM::state_get_cluster_resource(int event, void *data)
{
  int ret = OB_SUCCESS;
  milestones_.cluster_resource_create_end_ = get_based_hrtime();
  cmd_time_stats_.cluster_resource_create_time_ =
    milestone_diff(milestones_.cluster_resource_create_begin_, milestones_.cluster_resource_create_end_);
  switch (event) {
    case CLUSTER_RESOURCE_CREATE_COMPLETE_EVENT:
      pending_action_ = NULL;
      if (OB_ISNULL(data)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("data is NULL", K_(sm_id), K(ret));
      } else if (OB_FAIL(process_cluster_resource(data))) {
        LOG_WARN("fail to get cluster_resource, will disconnect", K_(sm_id), K(ret));
      } else {
        call_transact_and_set_next_state(ObMysqlTransact::modify_request);
      }
      break;
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("Unexpected event", K_(sm_id), K(event), K(ret));
      break;
    }
  }

  if (OB_FAIL(ret)) {
    trans_state_.inner_errcode_ = ret;
    RESOURCE_POOL_INCREMENT_DYN_STAT(GET_CLUSTER_RESOURCE_FAIL_COUNT);
    call_transact_and_set_next_state(ObMysqlTransact::handle_error_jump);
  }

  return VC_EVENT_NONE;
}

inline int ObMysqlSM::init_request_content(ObRequestAnalyzeCtx &ctx)
{
  int ret = OB_SUCCESS;

  if (trans_state_.is_auth_request_) {
    ObMysqlAuthRequest &orig_auth_req = client_session_->get_session_info().get_login_req();
    orig_auth_req.reset();
    if (client_session_->is_need_convert_vip_to_tname() && client_session_->is_vip_lookup_success()) {
      ctx.vip_tenant_name_ = client_session_->get_vip_tenant_name();
      ctx.vip_cluster_name_ = client_session_->get_vip_cluster_name();
    } else {
      ctx.vip_tenant_name_.assign_ptr(OB_SYS_TENANT_NAME,
                                      static_cast<int32_t>(STRLEN(OB_SYS_TENANT_NAME)));
      if (OB_FAIL(get_global_resource_pool_processor().get_first_cluster_name(ctx.vip_cluster_name_))) {
        LOG_WARN("fail to get first cluster name", K_(sm_id), K(ret));
      }
    }
  } else {
    trans_state_.trans_info_.client_request_.reuse();
    const bool eaic = client_session_->enable_analyze_internal_cmd();
    trans_state_.trans_info_.client_request_.set_enable_analyze_internal_cmd(eaic);
    trans_state_.trans_info_.client_request_.set_user_identity(client_session_->get_user_identity());

    ctx.is_sharding_mode_ = client_session_->get_session_info().is_sharding_user();
  }

  if (OB_SUCC(ret)) {
    ctx.reader_ = client_buffer_reader_;
    ctx.is_auth_ = trans_state_.is_auth_request_;

    ctx.parse_mode_ = NORMAL_PARSE_MODE;
    ctx.cached_variables_ = &client_session_->get_session_info().get_cached_variables();

    ctx.large_request_threshold_len_ = trans_state_.mysql_config_params_->tunnel_request_size_threshold_;
    ctx.request_buffer_length_ = trans_state_.mysql_config_params_->request_buffer_length_;

    ctx.using_ldg_ = client_session_->using_ldg();
  }
  return ret;
}

// chect weahter client connections reach throttle
inline bool ObMysqlSM::check_connection_throttle()
{
  bool throttle = false;
  bool enable_client_connection_lru_disconnect = trans_state_.mysql_config_params_->enable_client_connection_lru_disconnect_;
  if (OB_LIKELY(!client_session_->is_proxysys_tenant()) && !enable_client_connection_lru_disconnect) {
    int64_t currently_open = 0;
    NET_READ_GLOBAL_DYN_SUM(NET_GLOBAL_CLIENT_CONNECTIONS_CURRENTLY_OPEN, currently_open);
    int64_t max_client_connection = trans_state_.mysql_config_params_->client_max_connections_;
    if (max_client_connection > 0) { // if 0 == client_max connections means no limit
      throttle = (max_client_connection < currently_open);
      if (throttle) {
        LOG_WARN("client connections reach throttle",
                K(currently_open), K(max_client_connection),
                K(throttle), K(enable_client_connection_lru_disconnect), K_(sm_id));
      }
    }
  }
  return throttle;
}

inline int ObMysqlSM::encode_throttle_message()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(client_buffer_reader_->consume_all())) {
    LOG_WARN("client buffer reader fail to consume all", K(ret));
  } else {
    trans_state_.mysql_errcode_ = OB_ERR_TOO_MANY_SESSIONS;
    if (OB_FAIL(ObMysqlTransact::build_error_packet(trans_state_))) {
      LOG_WARN("[ObMysqlSM::encode_throttle_message] fail to encode error response",
               K_(sm_id), K(ret), "errcode", trans_state_.mysql_errcode_);
    }
  }

  return ret;
}

inline int ObMysqlSM::encode_unsupport_ps_message()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(client_buffer_reader_->consume_all())) {
    LOG_WARN("client buffer reader fail to consume all", K(ret));
  } else {
    trans_state_.mysql_errcode_ = OB_UNSUPPORTED_PS;
    if (OB_FAIL(ObMysqlTransact::build_error_packet(trans_state_))) {
      LOG_WARN("[ObMysqlSM::encode_unsupport_ps_message] fail to encode error response",
               K_(sm_id), K(ret), "errcode", trans_state_.mysql_errcode_);
    }
  }

  return ret;
}

inline bool ObMysqlSM::is_partition_table_route_supported()
{
  bool bret = false;
  if (trans_state_.mysql_config_params_->enable_partition_table_route_) {
    if (NULL != client_session_
        && client_session_->get_session_info().is_partition_table_supported()) {
      bret = true;
    }
  }
  return bret;
}

inline bool ObMysqlSM::is_pl_route_supported()
{
  bool bret = false;
  if (trans_state_.mysql_config_params_->enable_pl_route_) {
    if (NULL != client_session_
        && client_session_->get_session_info().is_pl_route_supported()) {
      bret = true;
    }
  }
  return bret;
}


// if authentication error, current session will destroy, so will not do anything
inline int ObMysqlSM::check_user_identity(const ObString &user_name,
                                          const ObString &tenant_name,
                                          const ObString &cluster_name)
{
  int ret = OB_SUCCESS;
  const ObHotUpgraderInfo &hu_info = get_global_hot_upgrade_info();
  ObHSRResult &hsr = client_session_->get_session_info().get_login_req().get_hsr_result();
  const bool is_current_cloud_user = is_cloud_user();
  if (need_reject_user_login(user_name, tenant_name, hsr.has_full_username_, is_current_cloud_user)) {
    ret = OB_USER_NOT_EXIST;
    LOG_WARN("access denied for this user", K(hsr), K(is_current_cloud_user), K(ret));
  } else if (tenant_name == OB_PROXYSYS_TENANT_NAME) {
  //1. "root@proxysys" user has super privilege, and it can only use proxy internal cmd without connect to observer
    if (user_name == OB_PROXYSYS_USER_NAME) {
      client_session_->set_user_identity(USER_TYPE_PROXYSYS);
      if (client_session_->is_authorised_proxysys(USER_TYPE_PROXYSYS)) {
        trans_state_.is_proxysys_tenant_ = true;
        _LOG_DEBUG("This is %s@%s, will not connect observer, enable use proxy internal cmd",
                   OB_PROXYSYS_USER_NAME, OB_PROXYSYS_TENANT_NAME);
      } else {
        ret = OB_USER_NOT_EXIST;
      }
    } else if (user_name == OB_INSPECTOR_USER_NAME) {
      //1.1 "inspector@proxysys" user can only use proxy ping cmd without connect to observer
      client_session_->set_user_identity(USER_TYPE_INSPECTOR);
      if (client_session_->is_authorised_proxysys(USER_TYPE_INSPECTOR)) {
        trans_state_.is_proxysys_tenant_ = true;
        _LOG_DEBUG("This is %s@%s, will not connect observer, enable use proxy ping cmd",
                   OB_INSPECTOR_USER_NAME, OB_PROXYSYS_TENANT_NAME);
      } else {
        ret = OB_USER_NOT_EXIST;
      }
    }

  //2."root@sys" user has super privilege, and it can use proxy internal cmd
  } else if (tenant_name == OB_SYS_TENANT_NAME && user_name == OB_SYS_USER_NAME) {
    client_session_->set_user_identity(USER_TYPE_ROOTSYS);
    _LOG_DEBUG("This is %s@%s user, enable use proxy internal cmd", OB_SYS_USER_NAME, OB_SYS_TENANT_NAME);

  //3."proxyro@sys" user has will disconect when is checking proxy
  } else if (tenant_name == OB_SYS_TENANT_NAME && user_name == ObProxyTableInfo::READ_ONLY_USERNAME_USER) {
    client_session_->set_user_identity(USER_TYPE_PROXYRO);
    _LOG_DEBUG("This is %s user", ObProxyTableInfo::READ_ONLY_USERNAME);

  //4. ConfigServer user is proxy internal user, and it can use proxy internal cmd
  // ConfigServer username format: "user@tenant" or "user"
  } else if (get_global_proxy_config().is_metadb_used() && cluster_name == OB_META_DB_CLUSTER_NAME) {
    ObProxyConfigString cfg_full_user_name;
    if (OB_FAIL(get_global_config_server_processor().get_proxy_meta_table_username(cfg_full_user_name))) {
      LOG_WARN("fail to get meta table info", K_(sm_id), K(ret));
    } else if (hsr.user_tenant_name_ == cfg_full_user_name
               || (user_name == cfg_full_user_name && tenant_name == OB_SYS_TENANT_NAME)) {
      client_session_->set_user_identity(USER_TYPE_METADB);
      LOG_DEBUG("This is metadb user, enable use proxy internal cmd");
    }
  }


  if (OB_SUCC(ret)
      && !client_session_->is_proxy_mysql_client_
      && hu_info.is_parent()
      && hu_info.need_reject_user()) {
    if ((hu_info.need_reject_metadb() && client_session_->is_metadb_user())
        || (hu_info.need_reject_proxysys() && client_session_->is_proxysys_user())
        || (hu_info.need_reject_proxyro() && client_session_->is_proxyro_user())) {
      ret = OB_ERR_USER_IS_LOCKED;
      LOG_INFO("this is parent proxy check subprocess available, current connect need rejected",
               K_(sm_id), K(ret));
    }
  }

  return ret;
}

// add tenant_name, username and cluster name to client_session_info
inline int ObMysqlSM::save_user_login_info(ObClientSessionInfo &session_info, ObHSRResult &hsr_result)
{
  int ret = OB_SUCCESS;

  LOG_DEBUG("orig login packet received complete and analyzed done",
            K_(sm_id), K_(hsr_result.cluster_name), K_(hsr_result.tenant_name),
            K_(hsr_result.user_name),
            "cs_id", client_session_->get_cs_id());

  if (OB_FAIL(session_info.set_user_name(hsr_result.user_name_))) {
    LOG_WARN("fail to set user name", K_(sm_id), K(ret));
  } else if (OB_FAIL(session_info.set_tenant_name(hsr_result.tenant_name_))) {
    LOG_WARN("fail to set tenant name", K_(sm_id), K(ret));
  } else if (OB_FAIL(session_info.set_cluster_name(hsr_result.cluster_name_))) {
    LOG_WARN("fail to set cluster name", K_(sm_id), K(ret));
  } else {
    // no need add database name to client session info here,
    // because if login request has -D database option, obproxy
    // will get changed database name from response ok packet;
  }
  return ret;
}

void ObMysqlSM::analyze_mysql_request(ObMysqlAnalyzeStatus &status)
{
  int ret = OB_SUCCESS;
  milestones_.client_.analyze_request_begin_ = get_based_hrtime();
  if (0 == cmd_size_stats_.client_request_bytes_ && 0 == milestones_.client_.analyze_request_end_) {
    cmd_time_stats_.client_request_read_time_ += (milestones_.client_.analyze_request_begin_ - milestones_.client_.client_begin_);
  } else if (cmd_size_stats_.client_request_bytes_ > 0 && milestones_.client_.analyze_request_end_ > 0) {
    cmd_time_stats_.client_request_read_time_ += (milestones_.client_.analyze_request_begin_ - milestones_.client_.analyze_request_end_);
  }

  ObRequestAnalyzeCtx ctx;
  if (OB_ISNULL(client_session_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("client session is NULL in init_request_content", K_(sm_id), K(ret));

  // 1. init request content at first
  } else if (OB_FAIL(init_request_content(ctx))) {
    LOG_WARN("fail to init requtest content", K_(sm_id), K(ret));
  } else {
    // 2. analyze mysql request(auth request or common mysql request)
    ObClientSessionInfo &session_info = client_session_->get_session_info();
    session_info.reset_text_ps_state();
    ObMysqlAuthRequest &orig_auth_req = session_info.get_login_req();
    ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
    ObMySQLCmd &req_cmd = trans_state_.trans_info_.sql_cmd_;

    ObMysqlRequestAnalyzer::analyze_request(ctx, orig_auth_req, client_request, req_cmd, status, session_info.is_oracle_mode());

    if (ANALYZE_DONE == status) {
      // 3. if BEGIN or START TRANSACTION, try to hold it
      if (OB_MYSQL_COM_QUERY == req_cmd) {
        MYSQL_INCREMENT_DYN_STAT(TOTAL_QUERY_COUNT);
        if (client_request.get_parse_result().need_hold_start_trans()) {
          if (OB_FAIL(session_info.set_start_trans_sql(client_request.get_sql()))) {
            LOG_WARN("fail to set start transaction sqld", K_(sm_id), K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (client_request.get_parse_result().is_text_ps_prepare_stmt()) {
            if (OB_FAIL(analyze_text_ps_prepare_request())) {
              LOG_WARN("analyze text ps prepare request failed", K(ret));
            }
          } else if (client_request.get_parse_result().is_text_ps_execute_stmt()) {
            if (OB_FAIL(analyze_text_ps_execute_request())) {
              LOG_WARN("analyze text ps execute request failed", K(ret));
            }
          }
        }
        // 4. if OB_MYSQL_COM_LOGIN, do some check
      } else if (OB_MYSQL_COM_LOGIN == req_cmd) {
        ObHSRResult &hsr = orig_auth_req.get_hsr_result();
        ObUnixNetVConnection* unix_vc = static_cast<ObUnixNetVConnection *>(client_session_->get_netvc());
        if (NULL == unix_vc) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("client entry vc is null", K(ret));
        } else if (hsr.response_.is_ssl_request() && !unix_vc->ssl_connected()) {
          if (OB_FAIL(unix_vc->ssl_init(ObUnixNetVConnection::SSL_SERVER))) {
            LOG_WARN("ssl start handshake failed", K(ret));
          } else {
            ctx.reader_->consume_all();
            handling_ssl_request_ = true;
          }
        } else {
          //save orig capability
          ObMySQLCapabilityFlags capability(session_info.get_orig_capability_flags().capability_ & hsr.response_.get_capability_flags().capability_);
          session_info.save_orig_capability_flags(capability);

          //login succ, we need desc cache miss stat as we has inc it when fetch_tenant_by_vip
          if (client_session_->is_need_convert_vip_to_tname()
              && !client_session_->is_vip_lookup_success()) {
            client_session_->get_session_stats().stats_[VIP_TO_TENANT_CACHE_MISS] -= 1;
            MYSQL_DECREMENT_DYN_STAT(VIP_TO_TENANT_CACHE_MISS);
          }

          // check whether the session uses logic tenant
          // invoke this func before save_user_login_info, so can rewrite real user into auth packet
          // all inner connections are not sharding connection
          if (!client_session_->is_proxy_mysql_client_ && OB_FAIL(ObProxyShardUtils::handle_shard_auth(*client_session_, hsr))) {
            LOG_WARN("fail to check user sharding", K_(sm_id), K(ret));
            // add some message for login error
            bool need_response_for_stmt = false;
            ObProxyShardUtils::build_error_packet(ret, need_response_for_stmt, trans_state_);
            // if check user failed, disconnect
            trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
          //save user name, tenant name, cluster name
          } else if (OB_FAIL(save_user_login_info(session_info, hsr))) {
            LOG_WARN("fail to save user login info", K_(sm_id), K(ret));

            //check user identity
          } else if (client_session_->get_session_info().is_oceanbase_server()
                     && OB_FAIL(check_user_identity(hsr.user_name_, hsr.tenant_name_, hsr.cluster_name_))) {
            LOG_WARN("fail to check user identity", K_(sm_id), K(ret));

          } else {
            if (client_session_->is_proxysys_tenant()) {
              //proxysys user no need check everything
            } else {
              if (!client_session_->is_proxy_mysql_client_ && !client_session_->is_proxysys_tenant()) {
                SESSION_PROMETHEUS_STAT(client_session_->get_session_info(), PROMETHEUS_CURRENT_SESSION, true, 1);
                client_session_->set_conn_prometheus_decrease(true);
              }
              if (check_connection_throttle()) {
                // check client connection throttle count
                if (OB_FAIL(encode_throttle_message())) {
                  LOG_WARN("fail to encode throttle message", K_(sm_id), K(ret));
                }
                status = ANALYZE_ERROR; // disconnect

              } else {
                // do nothing
              }

              // the data which remains in client_buffer_reader_ is orig auth request,
              // and will not send to observer;
              // later we will rewrite_first_login_req, send first_auth_req to observer
              // and consume the client_buffer_reader_;

            }//end if !proxysys
          }
        }
      } else if (OB_MYSQL_COM_STMT_PREPARE == req_cmd) {
        if (client_request.get_parse_result().is_start_trans_stmt() || session_info.is_sharding_user()) {
          if (OB_FAIL(encode_unsupport_ps_message())) {
            LOG_WARN("fail to encode unsupport ps error message", K(ret));
          } else {
            LOG_INFO("begin statement is not supported in prepare stament", K(ret));
          }

          status = ANALYZE_ERROR;
        } else if (OB_FAIL(analyze_ps_prepare_request())) {
          LOG_WARN("fail to analyze ps prepare request", K(ret));
        }
      } else if (OB_MYSQL_COM_STMT_EXECUTE == req_cmd
          || OB_MYSQL_COM_STMT_SEND_PIECE_DATA == req_cmd) {
        if (OB_FAIL(analyze_ps_execute_request())) {
          LOG_WARN("fail to analyze ps execute request", K(ret), K(req_cmd));
        }
      } else if (OB_MYSQL_COM_STMT_FETCH == req_cmd || OB_MYSQL_COM_STMT_GET_PIECE_DATA == req_cmd) {
        if (OB_FAIL(analyze_fetch_request())) {
          LOG_WARN("fail to analyze fetch request", K(ret));
        }
      } else if (OB_MYSQL_COM_STMT_CLOSE == req_cmd) {
        if (OB_FAIL(analyze_close_request())) {
          LOG_WARN("fail to analyze fetch request", K(ret));
        }
      } else if (OB_MYSQL_COM_STMT_PREPARE_EXECUTE == req_cmd) {
        if (OB_FAIL(analyze_ps_prepare_execute_request())) {
          LOG_WARN("fail to analyze ps prepare execute request", K(ret));
        }
      } else {
        // do nothing
      }
    } else if (ANALYZE_CONT == status)  {
      // large request means we have received enough packet(> request_buffer_len_)
      if ((OB_MYSQL_COM_STMT_EXECUTE == req_cmd || OB_MYSQL_COM_STMT_SEND_PIECE_DATA == req_cmd)
          && client_request.is_large_request()) {
        if (OB_FAIL(analyze_ps_execute_request())) {
          LOG_WARN("fail to analyze ps execute request", K(ret));
        }
      } else if (OB_MYSQL_COM_STMT_PREPARE_EXECUTE == req_cmd && client_request.is_large_request()) {
        if (OB_FAIL(analyze_ps_prepare_execute_request())) {
          LOG_WARN("fail to analyze ps prepare execute request", K(ret));
        }
      }
    } else {
      // is not ANALYZE_DONE, do nothing
    }
  }

  if (OB_FAIL(ret)) {
    status = ANALYZE_ERROR;
  }
  milestones_.client_.analyze_request_end_ = get_based_hrtime();

  cmd_time_stats_.client_request_analyze_time_ +=
    milestone_diff(milestones_.client_.analyze_request_begin_, milestones_.client_.analyze_request_end_);
}

int ObMysqlSM::do_analyze_ps_prepare_request(const ObString &ps_sql)
{
  int ret = OB_SUCCESS;

  // here we have the complete request in miobuffer, but client request buffer with
  // configured request_buffer_length may be not enough to copy all data,
  // we need copy complete ps sql
  ObClientSessionInfo &session_info = client_session_->get_session_info();
  ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;

  ObPsEntry *ps_entry = client_session_->get_ps_entry(ps_sql);
  ObPsIdEntry *ps_id_entry = NULL;
  bool is_new_ps_entry = false;

  if (NULL == ps_entry) {
    if (OB_FAIL(ObPsEntry::alloc_and_init_ps_entry(ps_sql, client_request.get_parse_result(), ps_entry))) {
      LOG_WARN("fail to alloc and init ps entry", K(ret));
    } else if (OB_FAIL(client_session_->add_ps_entry(ps_entry))) {
      LOG_WARN("fail to add ps entry to cache", K(ret));
      if (OB_LIKELY(NULL != ps_entry)) {
        ps_entry->destroy();
        ps_entry = NULL;
      }
    } else {
      ps_entry->inc_ref();
      is_new_ps_entry = true;
    }
  }

  if (OB_SUCC(ret)) {
    // every prepare request, use dirrerent ps id
    if (OB_FAIL(ObPsIdEntry::alloc_ps_id_entry(client_session_->inc_and_get_ps_id(), ps_entry, ps_id_entry))) {
      LOG_WARN("fail to alloc ps id entry", K(ret));
    } else if (OB_FAIL(session_info.add_ps_id_entry(ps_id_entry))) {
      LOG_WARN("fail to add ps id entry", KPC(ps_entry), K(ret));
      if (OB_LIKELY(NULL != ps_id_entry)) {
        ps_id_entry->destroy();
        ps_id_entry = NULL;
      }
    } else {
      // set current ps info
      session_info.set_ps_entry(ps_id_entry->ps_entry_);
      session_info.set_client_ps_id(ps_id_entry->ps_id_);
    }
  }

  if (is_new_ps_entry) {
    ps_entry->dec_ref();
    ps_entry = NULL;
  }
  return ret;
}

int ObMysqlSM::analyze_ps_prepare_request()
{
  int ret = OB_SUCCESS;

  ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
  ObString ps_sql = client_request.get_sql();
  char *ps_sql_buf = NULL;
  int64_t complete_sql_len = client_request.get_packet_len() - MYSQL_NET_META_LENGTH;
  if (complete_sql_len > ps_sql.length()) {
    if (OB_ISNULL(ps_sql_buf = static_cast<char *>(op_fixed_mem_alloc(complete_sql_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem for ps sql buf", K(complete_sql_len), K(ret));
    } else {
      client_buffer_reader_->copy(ps_sql_buf, complete_sql_len, MYSQL_NET_META_LENGTH);
      ps_sql.assign_ptr(ps_sql_buf, static_cast<int32_t>(complete_sql_len));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(do_analyze_ps_prepare_request(ps_sql))) {
      LOG_WARN("fail to do_analyze_ps_prepare_request", K(ps_sql), K(ret));
    }
  }

  if (NULL != ps_sql_buf) {
    op_fixed_mem_free(ps_sql_buf, complete_sql_len);
    ps_sql_buf = NULL;
  }
  return ret;
}

int ObMysqlSM::analyze_ps_execute_request()
{
  int ret = OB_SUCCESS;

  ObClientSessionInfo &session_info = client_session_->get_session_info();
  ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
  ObString data = client_request.get_req_pkt();
  if (OB_UNLIKELY(data.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("com_stmt_execute packet is empty", K(ret));
  } else {
    const char *pos = data.ptr() + MYSQL_NET_META_LENGTH;
    uint32_t ps_id = 0;
    ObMySQLUtil::get_uint4(pos, ps_id);
    ObPsEntry *entry = NULL;
    if (OB_ISNULL(entry = session_info.get_ps_entry(ps_id)) || !entry->is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ps entry does not exist", K(ps_id), KPC(entry), K(ret));
    } else {
      session_info.set_ps_entry(entry);
      session_info.set_client_ps_id(ps_id);
      client_request.set_ps_parse_result(&entry->get_base_ps_parse_result());
      // no need to analyze execute param value here,
      // will do analyze when needed before calculate partition id
    }
  }
  return ret;
}

int ObMysqlSM::analyze_text_ps_prepare_request()
{
  int ret = OB_SUCCESS;
  ObClientSessionInfo &session_info = client_session_->get_session_info();
  ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
  ObString text_ps_sql = client_request.get_sql();
  char *text_ps_sql_buf = NULL;
  int64_t complete_sql_len = client_request.get_packet_len() - MYSQL_NET_META_LENGTH;

  if (complete_sql_len > text_ps_sql.length()) {
    if (OB_ISNULL(text_ps_sql_buf = static_cast<char*>(op_fixed_mem_alloc(complete_sql_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem for text ps sql buf", K(complete_sql_len), K(ret));
    } else {
      client_buffer_reader_->copy(text_ps_sql_buf, complete_sql_len, MYSQL_NET_META_LENGTH);
      text_ps_sql.assign_ptr(text_ps_sql_buf, static_cast<int32_t>(complete_sql_len));
    }
  }

  ObTextPsNameEntry *text_ps_name_entry = session_info.get_text_ps_name_entry(client_request.get_parse_result().get_text_ps_name());

  if (OB_SUCC(ret) && NULL != text_ps_name_entry) {
    if (OB_FAIL(session_info.delete_text_ps_name_entry(text_ps_name_entry))) {
      LOG_WARN("delete text ps name entry failed", K(ret), KPC(text_ps_name_entry));
    } else {
      LOG_DEBUG("delete from text ps name entry", KPC(text_ps_name_entry));
    }
  }

  if (OB_SUCC(ret)) {
    ObTextPsEntry *text_ps_entry = NULL;
    if (OB_FAIL(ObTextPsEntry::alloc_and_init_text_ps_entry(
            text_ps_sql, client_request.get_parse_result(), text_ps_entry))) {
      LOG_WARN("fail to alloc and init text ps entry", K(ret));
    } else if (OB_FAIL(client_session_->add_text_ps_entry(text_ps_entry))) {
      LOG_WARN("fail to add text ps entry to cache", K(ret));
      if (OB_LIKELY(NULL != text_ps_entry)) {
        text_ps_entry->destroy();
        text_ps_entry = NULL;
      }
    } else if (OB_FAIL(ObTextPsNameEntry::alloc_text_ps_name_entry(
            client_request.get_parse_result().get_text_ps_name(),
            text_ps_entry, text_ps_name_entry))) {
      LOG_WARN("fail to alloc text ps name entry", K(ret));
    } else if (OB_FAIL(session_info.add_text_ps_name_entry(text_ps_name_entry))) {
      LOG_WARN("fail to add text ps name entry", KPC(text_ps_name_entry), K(ret));
      if (OB_LIKELY(NULL != text_ps_name_entry)) {
        text_ps_name_entry->destroy();
        text_ps_name_entry = NULL;
      }
    } else {
      text_ps_entry->set_version(client_session_->inc_and_get_ps_id());
    }
  }

  if (OB_SUCC(ret)) {
    session_info.set_text_ps_entry(text_ps_name_entry->text_ps_entry_);
    session_info.set_client_text_ps_name(client_request.get_parse_result().get_text_ps_name());
    LOG_DEBUG("session info add text ps name", K(client_request.get_parse_result().get_text_ps_name()));

    if (NULL != text_ps_sql_buf) {
      op_fixed_mem_free(text_ps_sql_buf, complete_sql_len);
      text_ps_sql_buf = NULL;
    }
  }

  return ret;
}

int ObMysqlSM::analyze_text_ps_execute_request()
{
  int ret = OB_SUCCESS;

  ObClientSessionInfo &session_info = client_session_->get_session_info();
  ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
  const ObString &text_ps_name = client_request.get_parse_result().get_text_ps_name();
  ObTextPsEntry *entry = NULL;

  if (OB_ISNULL(entry = session_info.get_text_ps_entry(text_ps_name)) || !entry->is_valid()) {
    LOG_WARN("text ps entry does not exist", K(text_ps_name), KPC(entry));
    // if not execute 'prepare stmt1', send 'execute stmt1' to server. server will return error msg
    client_request.get_parse_result().set_stmt_type(OBPROXY_T_OTHERS);
  } else {
    session_info.set_text_ps_entry(entry);
    session_info.set_client_text_ps_name(text_ps_name);
    session_info.set_text_ps_execute(true);
    client_request.set_text_ps_parse_result(&entry->get_base_ps_parse_result());
  }

  return ret;
}

int ObMysqlSM::analyze_fetch_request()
{
  int ret = OB_SUCCESS;

  ObClientSessionInfo &session_info = client_session_->get_session_info();
  ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
  ObString data = client_request.get_req_pkt();
  if (OB_UNLIKELY(data.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("com_stmt_fetch packet is empty", K(ret));
  } else {
    const char *pos = data.ptr() + MYSQL_NET_META_LENGTH;
    uint32_t cursor_id = 0;
    ObMySQLUtil::get_uint4(pos, cursor_id);
    session_info.set_client_cursor_id(cursor_id);
    LOG_DEBUG("fetch cursor id", K(cursor_id));
  }

  return ret;
}

int ObMysqlSM::analyze_close_request()
{
  int ret = OB_SUCCESS;

  ObClientSessionInfo &session_info = client_session_->get_session_info();
  ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
  ObString data = client_request.get_req_pkt();
  if (OB_UNLIKELY(data.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("com_stmt_fetch packet is empty", K(ret));
  } else {
    const char *pos = data.ptr() + MYSQL_NET_META_LENGTH;
    uint32_t ps_id = 0;
    ObMySQLUtil::get_uint4(pos, ps_id);
    session_info.set_client_ps_id(ps_id);
    LOG_DEBUG("close ps id", K(ps_id));
  }

  return ret;
}

int ObMysqlSM::analyze_ps_prepare_execute_request()
{
  int ret = OB_SUCCESS;

  ObClientSessionInfo &session_info = client_session_->get_session_info();
  ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
  ObString data = client_request.get_req_pkt();
  if (OB_UNLIKELY(data.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("com_stmt_execute packet is empty", K(ret));
  } else {
    const char *pos = data.ptr() + MYSQL_NET_META_LENGTH;
    uint32_t ps_id = 0;
    ObMySQLUtil::get_uint4(pos, ps_id);
    if (0 == ps_id) {
      session_info.set_client_ps_id(client_session_->inc_and_get_ps_id());
    } else {
      session_info.set_client_ps_id(ps_id);
      // no need to analyze execute param value here,
      // will do analyze when needed before calculate partition id
    }
    session_info.set_recv_client_ps_id(ps_id);
  }

  return ret;
}

int ObMysqlSM::state_watch_for_client_abort(int event, void *data)
{
  STATE_ENTER(ObMysqlSM::state_watch_for_client_abort, event);

  if (OB_ISNULL(client_entry_) || OB_ISNULL(client_session_)
      || OB_UNLIKELY(client_entry_->read_vio_ != reinterpret_cast<ObVIO *>(data))
      || OB_UNLIKELY(client_entry_->vc_ != client_session_)) {
    terminate_sm_ = true;
    LOG_ERROR("invalid internal state", K_(client_entry_->read_vio), K(data),
              K_(client_entry_->vc), K_(client_session), K_(sm_id));
  } else {
    switch (event) {
      /**
       * EOS means that the client has initiated the connection shut down.
       * Only half close the client connection so obproxy can read additional
       * data that may still be sent from the server and send it to the
       * client.
       */
      case VC_EVENT_EOS:
        static_cast<ObMysqlClientSession *>(client_entry_->vc_)->get_netvc()->do_io_shutdown(IO_SHUTDOWN_READ);
        client_entry_->eos_ = true;
      case VC_EVENT_ERROR:
      case VC_EVENT_ACTIVE_TIMEOUT:
      case VC_EVENT_INACTIVITY_TIMEOUT:
        if (tunnel_.is_tunnel_active()) {
          // Check to see if the client is part of the tunnel.
          // If so forward the event to the tunnel. Otherwise,
          // kill the tunnel and fall through to the case
          // where the tunnel is not active
          ObMysqlTunnelConsumer *c = tunnel_.get_consumer(client_session_);

          if (NULL != c && c->alive_) {
            LOG_DEBUG("[watch_for_client_abort] forwarding event to tunnel",
                      K_(sm_id), "event", ObMysqlDebugNames::get_event_name(event));
            tunnel_.handle_event(event, c->write_vio_);
            break;
          } else {
            tunnel_.kill_tunnel();
          }
        }

        trans_state_.current_.state_ = ObMysqlTransact::ACTIVE_TIMEOUT;
        client_session_->can_server_session_release_ = false;
        LOG_INFO("[watch_for_client_abort] connection close",
                 K_(sm_id), "event", ObMysqlDebugNames::get_event_name(event));

        // Disable further I/O on the client
        if (NULL != client_entry_->read_vio_) {
          client_entry_->read_vio_->nbytes_ = client_entry_->read_vio_->ndone_;
        }
        milestones_.client_.client_end_ = get_based_hrtime();
        set_client_abort(ObMysqlTransact::ABORTED, event);
        update_congestion_entry(event);
        terminate_sm_ = true;
        break;

      case VC_EVENT_READ_COMPLETE:
      case VC_EVENT_READ_READY:
        // Ignore. Could be a pipelined request. We'll get to it when we finish the
        // current transaction
        break;

      default:
        terminate_sm_ = true;
        LOG_ERROR("Unexpected event", K(event), K_(sm_id));
        break;
    }
  }

  return VC_EVENT_NONE;
}

// We've done a successful transform open and issued a do_io_write
// to the transform. We are now ready for the transform to tell us
// it is now ready to be read from and it done modifying the server
// request
int ObMysqlSM::state_request_wait_for_transform_read(int event, void *data)
{
  return api_.state_request_wait_for_transform_read(event, data);
}

// We've done a successful transform open and issued a do_io_write
// to the transform. We are now ready for the transform to tell us
// it is now ready to be read from and it done modifying the client
// response
int ObMysqlSM::state_response_wait_for_transform_read(int event, void *data)
{
  return api_.state_response_wait_for_transform_read(event, data);
}

// ob_api.cpp calls us directly here to avoid
// problems with setting and changing the
// default_handler function.  As such, this is an
// entry point and needs to handle the reentrancy
// counter and deallocation the state machine if
// necessary
int ObMysqlSM::state_api_callback(int event, void *data)
{
  if (OB_UNLIKELY(MYSQL_SM_MAGIC_ALIVE != magic_)
      || OB_UNLIKELY(reentrancy_count_ < 0)) {
    LOG_ERROR("invalid sm magic or reentrancy_count", K_(magic), K_(reentrancy_count), K_(sm_id));
  }
  ++reentrancy_count_;

  STATE_ENTER(ObMysqlSM::state_api_callback, event);

  api_.state_api_callout(event, data);

  // The sub-handler signals when it is time for the state
  // machine to exit. We can only exit if we are not reentrantly
  // called otherwise when the our call unwinds, we will be
  // running on a dead state machine
  //
  // Because of the need for an api shutdown hook, kill_this() is
  // also reentrant.  As such, we don't want to decrement
  // the reentrancy count until after we run kill_this()
  if (terminate_sm_ && 1 == reentrancy_count_) {
    kill_this();
  } else {
    --reentrancy_count_;
    if (OB_UNLIKELY(reentrancy_count_ < 0)) {
      LOG_ERROR("invalid reentrancy_count", K_(reentrancy_count), K_(sm_id));
    }
  }

  return VC_EVENT_CONT;
}

int ObMysqlSM::state_api_callout(int event, void *data)
{
  return api_.state_api_callout(event, data);
}

// Figures out what to do after calling api callouts
// have finished. This messy and I would like to
// come up with a cleaner way to handle the api
// return. The way we are doing things also makes a
// mess of set_next_state()
void ObMysqlSM::handle_api_return()
{
  int ret = OB_SUCCESS;
  switch (trans_state_.api_next_action_) {
    case ObMysqlTransact::SM_ACTION_API_SM_START:
      if (OB_FAIL(setup_client_request_read())) {
        LOG_WARN("failed to setup_client_request_read", K_(sm_id), K(ret));
      }
      break;

    case ObMysqlTransact::SM_ACTION_API_READ_REQUEST:
    case ObMysqlTransact::SM_ACTION_API_OBSERVER_PL:
    case ObMysqlTransact::SM_ACTION_API_READ_RESPONSE:
      call_transact_and_set_next_state(NULL);
      break;

    case ObMysqlTransact::SM_ACTION_API_SEND_REQUEST:
      if (OB_FAIL(setup_server_request_send())) {
        LOG_WARN("failed to setup_server_request_send", K_(sm_id), K(ret));
      }
      break;

    case ObMysqlTransact::SM_ACTION_API_SEND_RESPONSE:
      set_client_wait_timeout();
      milestones_.client_.client_write_begin_ = get_based_hrtime();
      // we have further processing to do
      // based on what trans_state_.next_action_ is
      switch (trans_state_.next_action_) {
        case ObMysqlTransact::SM_ACTION_TRANSFORM_READ:
          if (OB_FAIL(api_.setup_transfer_from_transform())) {
            LOG_WARN("failed to setup_transfer_from_transform", K_(sm_id), K(ret));
          }
          break;

        case ObMysqlTransact::SM_ACTION_SERVER_READ:
          if (OB_FAIL(setup_server_transfer())) {
            LOG_WARN("failed to setup_server_transfer", K_(sm_id), K(ret));
          }
          break;

        case ObMysqlTransact::SM_ACTION_INTERNAL_NOOP:
        case ObMysqlTransact::SM_ACTION_SEND_ERROR_NOOP:
          if (OB_FAIL(setup_internal_transfer(&ObMysqlSM::tunnel_handler_response_transfered))) {
            LOG_WARN("failed to setup_internal_transfer", K_(sm_id), K(ret));
          }
          break;

        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("Should not get here", K_(trans_state_.next_action), K_(sm_id), K(ret));
          break;
      }
      break;

    case ObMysqlTransact::SM_ACTION_API_CMD_COMPLETE:
      if (OB_FAIL(setup_cmd_complete())) {
        LOG_WARN("failed to setup_cmd_complete", K_(sm_id), K(ret));
      }
      break;

    case ObMysqlTransact::SM_ACTION_API_SM_SHUTDOWN:
      if (OB_FAIL(state_remove_from_list(EVENT_NONE, NULL))) {
        LOG_WARN("failed to state_remove_from_list", K_(sm_id), K(ret));
      }
      break;

    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("Should not get here", K_(trans_state_.api_next_action), K_(sm_id), K(ret));
      break;
  }

  if (OB_FAIL(ret)) {
    trans_state_.inner_errcode_ = ret;
    setup_error_transfer();
  }
}

int ObMysqlSM::state_execute_internal_cmd(int event, void *data)
{
  LOG_DEBUG("entered inside state_execute_internal_cmd", K_(sm_id));
  STATE_ENTER(ObMysqlSM::state_execute_internal_cmd, event);
  int ret = OB_SUCCESS;
  UNUSED(data);

  if (OB_ISNULL(client_entry_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("invalid internal state, client entry is NULL", K_(client_entry), K_(sm_id), K(ret));
  } else if (OB_ISNULL(client_entry_->vc_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("invalid internal state, client entry vc is NULL", K_(client_entry_->vc), K_(sm_id), K(ret));
  } else  if (OB_FAIL(client_buffer_reader_->consume_all())) { // consume the client buffer
    LOG_WARN("fail to consume all data in client buffer reader", K_(sm_id), K(ret));
  } else {
    pending_action_ = NULL;
    switch (event) {
      case INTERNAL_CMD_EVENTS_SUCCESS :
        trans_state_.next_action_ = ObMysqlTransact::SM_ACTION_INTERNAL_NOOP;
        callout_api_and_start_next_action(ObMysqlTransact::SM_ACTION_API_SEND_RESPONSE);
        break;

      case INTERNAL_CMD_EVENTS_FAILED :
        ret = OB_ERR_SYS;
        trans_state_.free_internal_buffer();
        LOG_DEBUG("failed to execute_internal_cmd, will close connection", K_(sm_id));
        break;

      default:
        ret = OB_INNER_STAT_ERROR;
        LOG_ERROR("Unexpected event", K(event), K_(sm_id), K(ret));
        break;
    }
  }

  if (OB_FAIL(ret)) {
    call_transact_and_set_next_state(ObMysqlTransact::handle_error_jump);
  }

  return VC_EVENT_NONE;
}

int ObMysqlSM::state_observer_open(int event, void *data)
{
  int ret = OB_SUCCESS;
  ObMysqlServerSession *session = NULL;

  STATE_ENTER(ObMysqlSM::state_observer_open, event);

  pending_action_ = NULL;
  milestones_.server_connect_end_ = get_based_hrtime();

  switch (event) {
    case NET_EVENT_OPEN:
    {
      session = op_reclaim_alloc(ObMysqlServerSession);
      if (OB_ISNULL(session)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("failed to allocate memory for ObMysqlServerSession", K(ret));
      } else if (OB_ISNULL(client_session_) || OB_ISNULL(data)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid client session or data", K_(client_session), K(data), K_(sm_id), K(ret));
      } else {
        ObNetVConnection *net_vc = reinterpret_cast<ObNetVConnection *>(data);
        ops_ip_copy(session->local_ip_, net_vc->get_local_addr());
        ops_ip_copy(session->server_ip_, trans_state_.server_info_.addr_);
        const ObString &full_username = client_session_->get_session_info().get_full_username();
        if (full_username.length() >= OB_PROXY_FULL_USER_NAME_MAX_LEN) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("full username is too long", K(ret), K(full_username));
        } else {
          MEMCPY(session->full_name_buf_, full_username.ptr(), full_username.length());
          session->auth_user_.assign_ptr(session->full_name_buf_, full_username.length());

          // if shard_conn is NULL, should set to client session. And later will use connection pool by the shard connector
          ObShardConnector *shard_conn = client_session_->get_session_info().get_shard_connector();
          if (OB_NOT_NULL(shard_conn)) {
            session->get_session_info().set_shard_connector(shard_conn);
          }

          if (OB_FAIL(session->new_connection(*client_session_, *(reinterpret_cast<ObNetVConnection *>(data))))) {
            LOG_WARN("fail to new server connection", K_(sm_id), K(ret));
          } else {
            session->state_ = MSS_ACTIVE;

            if (OB_FAIL(attach_server_session(*session))) {
              LOG_WARN("failed to attach server session", K_(sm_id), K(ret));
            } else {
              if (OB_UNLIKELY(MYSQL_PLUGIN_AS_INTERCEPT == api_.plugin_tunnel_type_)) {
                // intercept plugin can't support auth
                trans_state_.current_.send_action_ = ObMysqlTransact::SERVER_SEND_REQUEST;
              } else {
                trans_state_.current_.send_action_ = ObMysqlTransact::SERVER_SEND_HANDSHAKE;
              }
              LOG_DEBUG("succ to establish server sesssion", K_(sm_id));

              handle_observer_open();
            }
          }
        }
      }
      if (OB_FAIL(ret) && OB_LIKELY(NULL != session)) {
        op_reclaim_free(session);
        session = NULL;
      }
      break;
    }

    case NET_EVENT_OPEN_FAILED:
      LOG_DEBUG("[ObMysqlSM::state_observer_open] connect failed", K_(sm_id));
      // save the errno from the connect fail for future use
      // (passed as negative value, flip back)
      trans_state_.current_.state_ = ObMysqlTransact::CONNECT_ERROR;
      call_transact_and_set_next_state(ObMysqlTransact::handle_response);
      break;

    case VC_EVENT_ERROR:
      trans_state_.current_.state_ = ObMysqlTransact::CONNECTION_ERROR;
      call_transact_and_set_next_state(ObMysqlTransact::handle_response);
      break;

    default:
      ret = OB_INNER_STAT_ERROR;
      LOG_ERROR("[ObMysqlSM::state_observer_open] Unknown event:",
                K_(sm_id), K(ret), K(event));
      break;
  }

  if (OB_FAIL(ret)) {
    trans_state_.inner_errcode_ = ret;
    trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
    call_transact_and_set_next_state(ObMysqlTransact::handle_response);
  }

  return VC_EVENT_NONE;
}

int ObMysqlSM::state_server_response_read(int event, void *data)
{
  int ret = OB_SUCCESS;
  ObMysqlAnalyzeStatus state = ANALYZE_ERROR;
  ObVIO *vio = reinterpret_cast<ObVIO *>(data);

  STATE_ENTER(ObMysqlSM::state_server_response_read, event);

  if (OB_ISNULL(server_entry_) || OB_ISNULL(data)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("invalid internal state, server entry is NULL or data is NULL",
              K_(server_entry), K(data), K_(sm_id), K(ret));
  } else if (server_entry_->read_vio_ != reinterpret_cast<ObVIO *>(data)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("invalid internal state, server entry read vio is not the same as data",
              K_(server_entry_->read_vio), K(data), K_(sm_id), K(ret));
  } else {
    switch (event) {
      case VC_EVENT_EOS:
        server_entry_->eos_ = true;

        LOG_WARN("ObMysqlSM::state_server_response_read, recevied  VC_EVENT_EOS", K_(sm_id));
        // If no bytes were transmitted, maybe an
        // overloaded server closing the connection so
        // don't accept zero length responses
        if (0 == vio->ndone_) {
          ret = OB_CONNECT_ERROR;
          break;
        }

        // fall through
      case VC_EVENT_READ_READY:
      case VC_EVENT_READ_COMPLETE:
        // More data to parse
        break;

      case VC_EVENT_INACTIVITY_TIMEOUT:
        // if proxy timeout after forwarding request to observer,
        // we should set it alive congested
        trans_state_.set_alive_failed();
      // fall through
      case VC_EVENT_ERROR:
      case VC_EVENT_ACTIVE_TIMEOUT:
        LOG_WARN("ObMysqlSM::state_server_response_read", "event",
                 ObMysqlDebugNames::get_event_name(event), K_(sm_id));
        // Error handling function
        ret = OB_CONNECT_ERROR;
        break;
    }
  }

  // set server_read_begin_ no matter if error or timeout happen
  if (0 == milestones_.server_.server_read_begin_) {
    milestones_.server_.server_read_begin_ = get_based_hrtime();
    cmd_time_stats_.server_process_request_time_ =
        milestone_diff(milestones_.server_.server_write_end_, milestones_.server_.server_read_begin_);
  }

  if (OB_SUCC(ret)) {
    // Reset the inactivity timeout if this is the first
    // time we've been called. The timeout had been set to
    // the connect timeout when we set up to read the header
    if (0 == trans_stats_.server_response_bytes_) {
      if (-1 != trans_state_.api_txn_no_activity_timeout_value_) {
        server_session_->get_netvc()->set_inactivity_timeout(
            HRTIME_MSECONDS(trans_state_.api_txn_no_activity_timeout_value_));
      }
    }

    bool need_receive_completed = false;
    if ((OB_LIKELY(NULL != client_session_) && client_session_->is_proxy_mysql_client_)
        || ObMysqlTransact::SERVER_SEND_REQUEST != trans_state_.current_.send_action_) {
      need_receive_completed = true;
    }

    int64_t first_pkt_len = 0; // include packet header
    if (OB_FAIL(handle_first_response_packet(state, first_pkt_len, need_receive_completed))) {
      LOG_WARN("fail to handle first response packet", K(ret));
      state = ANALYZE_ERROR;
    }

    if (ANALYZE_CONT != state) {
      // Disable further I/O
      // Read the first packet, or command complete or transaction complete,
      // or error happen;
      // If read the first packet on the client since there could
      // be rest request body that we are tunneling, and we can't issue
      // another IO later for the rest request body with a different buffer
      server_entry_->read_vio_->nbytes_ = server_entry_->read_vio_->ndone_;
      if (0 == milestones_.server_.server_read_end_) {
        milestones_.server_.server_read_end_ = get_based_hrtime();
        cmd_time_stats_.server_response_read_time_ += (
          milestone_diff(milestones_.server_.server_read_begin_, milestones_.server_.server_read_end_)
          - cmd_time_stats_.plugin_decompress_response_time_
          - cmd_time_stats_.server_response_analyze_time_);
      }
    }

    switch (state) {
      case ANALYZE_ERROR:
      {
        LOG_WARN("Error parsing server response", K_(sm_id), K(ret));
        trans_state_.current_.state_ = ObMysqlTransact::ANALYZE_ERROR;

        // If the server closed prematurely on us, use the
        // server setup error routine since it will forward
        // error to a request tranfer tunnel if any
        if (VC_EVENT_EOS == event) {
          ret = OB_CONNECT_ERROR;
        } else {
          if (OB_SUCC(ret)) {
            // mark it error, will disconnect soon
            ret = OB_ERR_UNEXPECTED;
          }
        }
        break;
      }

      case ANALYZE_DONE:
      {
        // Now that we know that we have first packet of the observer
        // response, we can reset the client inactivity
        // timeout. This is unlikely to cause a recurrence of
        // old bug because there will be no more retries now that
        // the connection has been established. It is possible
        // however. We do not need to reset the inactivity timeout
        // if the request contains a body (noted by the
        // request_content_length field) because it was never
        // cancelled.

        // we now reset the client inactivity timeout only
        // when we are ready to send the response. In the
        // case of transform plugin, this is after the transform
        // outputs the 1st byte, which can take a long time if the
        // plugin buffers the whole response.
        // Also, if the request contains a body, we cancel the timeout
        // when we read the 1st byte of the observer response.
        if (ObMysqlTransact::SERVER_SEND_REQUEST == trans_state_.current_.send_action_) {
          set_client_wait_timeout();
        }

        trans_state_.current_.state_ = ObMysqlTransact::CONNECTION_ALIVE;
        trans_state_.transact_return_point = ObMysqlTransact::handle_response;
        callout_api_and_start_next_action(ObMysqlTransact::SM_ACTION_API_READ_RESPONSE);
        break;
      }

      case ANALYZE_CONT: {
        if (first_pkt_len > 0 && first_pkt_len > server_buffer_reader_->mbuf_->water_mark_) {
          // ensure the server read buffer can cache the first packet
          server_buffer_reader_->mbuf_->water_mark_ = first_pkt_len;
        }

        if (need_receive_completed) {
          // for mysql client or send_action_ is not SERVER_SEND_REQUEST, we must receive all response packets completely,
          // so set a larger water_mark to read more data
          server_buffer_reader_->mbuf_->water_mark_ =
            std::max(2 * server_buffer_reader_->read_avail(),
                     trans_state_.mysql_config_params_->default_buffer_water_mark_);
        }
        if (server_entry_->eos_) {
          // if obproxy just received part data of a mysql packet, then server session
          // disconect. at this time, we should consume the received data,
          // and disconnect client session.
          if (OB_FAIL(server_buffer_reader_->consume_all())) {
            LOG_WARN("fail to consume data in server buffer reader", K(ret));
          }
          ret = OB_CONNECT_ERROR;
        } else {
          server_entry_->read_vio_->reenable();
        }
        break;
      }

      default:
        ret = OB_INNER_STAT_ERROR;
        LOG_ERROR("not reached, unknown ayalyze status", K(state), K_(sm_id), K(ret));
        break;
    }
  }

  if (OB_FAIL(ret)) {
    trans_state_.inner_errcode_ = ret;
    if (0 == milestones_.server_.server_read_end_) {
      milestones_.server_.server_read_end_ = get_based_hrtime();
      cmd_time_stats_.server_response_read_time_ +=
        milestone_diff(milestones_.server_.server_read_begin_, milestones_.server_.server_read_end_);
    }
    handle_server_setup_error(event, data);
  }

  return (ANALYZE_CONT == state) ? VC_EVENT_CONT : VC_EVENT_NONE;
}

ObMysqlCompressAnalyzer& ObMysqlSM::get_compress_analyzer()
{
  if (PROTOCOL_OB20 == use_compression_protocol()) {
    return compress_ob20_analyzer_;
  } else {
    return compress_analyzer_;
  }
}

ObProxyProtocol ObMysqlSM::use_compression_protocol() const
{
  // in auth, do not use compress prototcol
  if (NULL == server_session_ || (!server_session_->is_checksum_supported() && !server_session_->is_ob_protocol_v2_supported())
          || ObMysqlTransact::SERVER_SEND_SAVED_LOGIN == trans_state_.current_.send_action_
          || ObMysqlTransact::SERVER_SEND_LOGIN == trans_state_.current_.send_action_
          || ObMysqlTransact::SERVER_SEND_HANDSHAKE == trans_state_.current_.send_action_) {
    return PROTOCOL_NORMAL;
  } else if (server_session_->is_ob_protocol_v2_supported()) {
    return PROTOCOL_OB20;
  } else {
    return PROTOCOL_CHECKSUM; // current checksum is supported by zlib
  }
}

bool ObMysqlSM::is_checksum_on() const
{
  return (NULL != server_session_ && server_session_->is_checksum_on());
}

bool ObMysqlSM::is_extra_ok_packet_for_stats_enabled() const
{
  return (NULL != server_session_
          && server_session_->is_extra_ok_packet_for_stats_enabled());
}

bool ObMysqlSM::is_cloud_user() const
{
  bool bret = false;
  if (OB_LIKELY(NULL != client_session_)) {
    bret = client_session_->is_need_convert_vip_to_tname()
           || client_session_->is_need_use_proxy_tenant_name();
  }
  return bret;
}

bool ObMysqlSM::need_reject_user_login(const ObString &user, const ObString &tenant,
                                       const bool has_full_username, const bool is_cloud_user) const
{
  // blew case need reject user login:
  //  1. user proxyro@sys user
  //  2. user LBACSYS
  //  3. user ORAAUDITOR
  //  4. cloud user with tenant or cluster
  bool bret = false;
  if (OB_UNLIKELY(NULL != client_session_)
      && !client_session_->is_proxy_mysql_client_
      && tenant.case_compare(OB_PROXYSYS_TENANT_NAME) != 0) {
    bret = (!get_global_proxy_config().skip_proxyro_check
            && tenant.case_compare(OB_SYS_TENANT_NAME) == 0
            && user.case_compare(ObProxyTableInfo::READ_ONLY_USERNAME_USER) == 0)
           || (is_cloud_user && !get_global_proxy_config().enable_cloud_full_username
               && has_full_username);
  }
  return bret;
}

uint8_t ObMysqlSM::get_request_seq()
{
  uint8_t seq = 0;

  if (use_compression_protocol() == PROTOCOL_CHECKSUM) {
    if (ObMysqlTransact::SERVER_SEND_REQUEST == trans_state_.current_.send_action_) {
      seq = trans_state_.trans_info_.client_request_.get_packet_meta().pkt_seq_;
    } else {
      // others like sync database, sync session variables seq num == 0
    }
  } else {
    seq = server_session_->get_compressed_seq();
  }
  return seq;
}

ObMySQLCmd ObMysqlSM::get_request_cmd()
{
  ObMySQLCmd cmd = OB_MYSQL_COM_QUERY;
  if (ObMysqlTransact::SERVER_SEND_REQUEST == trans_state_.current_.send_action_) {
    cmd = trans_state_.trans_info_.client_request_.get_packet_meta().cmd_;
  } else {
    // others like sync last insert id, sync session variables, cmd = OB_MYSQL_COM_QUERY
    cmd = trans_state_.trans_info_.sql_cmd_;
  }
  LOG_DEBUG("get_request_cmd", "request_cmd", ObProxyParserUtils::get_sql_cmd_name(cmd));
  return cmd;
}

inline void ObMysqlSM::check_update_checksum_switch(const bool is_compressed_payload)
{
  if (OB_LIKELY(NULL != server_session_)
      && server_session_->get_session_info().is_checksum_switch_supported()) {
    if (is_compressed_payload == server_session_->get_session_info().is_checksum_on()) {
      LOG_DEBUG("no need update checksum_switch",
                K(is_compressed_payload),
                "checksum_switch", server_session_->get_session_info().get_checksum_switch(),
                "server_ip", server_session_->server_ip_,
                "server_sessid", server_session_->get_server_sessid());
    } else {
      const ObProxyChecksumSwitch old_switch = server_session_->get_session_info().get_checksum_switch();
      server_session_->get_session_info().set_checksum_switch(is_compressed_payload);
      LOG_INFO("succ to update checksum_switch", K(old_switch),
               "new_switch", server_session_->get_session_info().get_checksum_switch(),
               "server_ip", server_session_->server_ip_,
               "server_sessid", server_session_->get_server_sessid());
    }
  }
}

// the first response packet (mysql protocol or compress protocol) from observer
inline int ObMysqlSM::handle_first_response_packet(ObMysqlAnalyzeStatus &state, int64_t &first_pkt_len, bool need_receive_completed)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("handle_first_response_packet", K(need_receive_completed));
  ObHRTime analyze_response_begin = get_based_hrtime();

  if (client_session_->get_session_info().is_oceanbase_server()) {
    if (OB_FAIL(handle_oceanbase_first_response_packet(state, need_receive_completed, first_pkt_len))) {
      LOG_WARN("fail to handle oceanbase first response packet", K(ret));
    }
  } else {
    if (OB_FAIL(handle_first_normal_response_packet(state, need_receive_completed, first_pkt_len))) {
      LOG_WARN("fail to handle normal first response packet", K(ret));
    }
  }

  cmd_time_stats_.server_response_analyze_time_ += milestone_diff(analyze_response_begin, get_based_hrtime());
  return ret;
}

inline int ObMysqlSM::handle_oceanbase_first_response_packet(ObMysqlAnalyzeStatus &state,
        const bool need_receive_completed, int64_t &first_pkt_len)
{
  int ret = OB_SUCCESS;

  ObProxyProtocol ob_proxy_protocol = use_compression_protocol();

  if (PROTOCOL_CHECKSUM == ob_proxy_protocol || PROTOCOL_OB20 == ob_proxy_protocol) { // compressed protocol
    if (OB_FAIL(handle_first_compress_response_packet(state, need_receive_completed, first_pkt_len))) {
      LOG_WARN("fail to handle_first_compress_response_packet", K(need_receive_completed), K(ret));
    }
  } else { // standard mysql protocol
    if (OB_FAIL(handle_first_normal_response_packet(state, need_receive_completed, first_pkt_len))) {
      LOG_WARN("fail to handle_first_normal_response_packet", K(need_receive_completed), K(ret));
    }
  }
  return ret;
}

inline int ObMysqlSM::handle_first_compress_response_packet(ObMysqlAnalyzeStatus &state,
    const bool need_receive_completed, int64_t &first_pkt_len)
{
  int ret = OB_SUCCESS;
  ObMysqlResp &server_response = trans_state_.trans_info_.server_response_;
  server_response.reset();

  ObMysqlCompressedOB20AnalyzeResult result;
  ObMysqlCompressAnalyzer *compress_analyzer = &get_compress_analyzer();
  compress_analyzer->reset();
  const uint8_t req_seq = get_request_seq();
  const ObMySQLCmd cmd = get_request_cmd();
  const bool enable_extra_ok_packet_for_stats = is_extra_ok_packet_for_stats_enabled();

  if (server_buffer_reader_->is_read_avail_more_than(MYSQL_COMPRESSED_HEALDER_LENGTH)) {
    ObMysqlCompressAnalyzer::AnalyzeMode mode;
    if (!need_receive_completed || ObMysqlTransact::SERVER_SEND_REQUEST != trans_state_.current_.send_action_) {
      mode = ObMysqlCompressAnalyzer::SIMPLE_MODE;
    } else {
      mode = ObMysqlCompressAnalyzer::DECOMPRESS_MODE;
    }

    if (OB_FAIL(compress_analyzer->init(req_seq, mode, cmd, enable_extra_ok_packet_for_stats,
                                        req_seq, server_session_->get_server_request_id(),
                                        server_session_->get_server_sessid()))) {
      LOG_WARN("fail to init compress_analyzer", K_(sm_id), K(req_seq), K(cmd), K(ret));
    } else if (OB_FAIL(compress_analyzer->analyze_first_response(
            *server_buffer_reader_, need_receive_completed, result, server_response))) {
      LOG_WARN("fail to analyze first response", K(server_buffer_reader_),
               K(need_receive_completed), K(server_response), K_(sm_id), K(ret));
    } else {
      LOG_DEBUG("after first response", K(req_seq), K(result), "is_resp_finished",
                server_response.get_analyze_result().is_resp_completed(),
                "is_trans_finished", server_response.get_analyze_result().is_trans_completed());
      if (ANALYZE_DONE == result.status_ && PROTOCOL_CHECKSUM == use_compression_protocol()) {
        check_update_checksum_switch(result.is_checksum_on_);
      }

      state = result.status_;
      first_pkt_len = result.header_.compressed_len_ + MYSQL_COMPRESSED_HEALDER_LENGTH;
      cmd_size_stats_.server_response_bytes_ = server_buffer_reader_->read_avail();
    }
  } else {
    state = ANALYZE_CONT;
  }
  return ret;
}

inline int ObMysqlSM::handle_first_normal_response_packet(ObMysqlAnalyzeStatus &state,
    const bool need_receive_completed, int64_t &first_pkt_len)
{
  int ret = OB_SUCCESS;
  ObMysqlResp &server_response = trans_state_.trans_info_.server_response_;
  server_response.reset();
  ObMysqlAnalyzeResult result;

  ObObj obj;
  bool is_autocommit_0 = false;
  if (OB_FAIL(client_session_->get_session_info().field_mgr_.get_common_sys_variable_value("autocommit", obj))) {
    LOG_DEBUG("fail to get autocommit val", K(ret));
  } else if (obj.get_int() == 0) {
    LOG_DEBUG("autocommit is 0");
    is_autocommit_0 = true;
  }

  if (server_buffer_reader_->is_read_avail_more_than(MYSQL_NET_META_LENGTH)) {
    if (!client_session_->get_session_info().is_oceanbase_server()
        || trans_state_.mysql_config_params_->is_mysql_routing_mode()) {
      LOG_DEBUG("handle_first_normal_response_packet", K(trans_state_.current_.state_),
                K(is_in_trans_), K(is_autocommit_0));
      analyzer_.set_server_cmd(trans_state_.trans_info_.sql_cmd_, STANDARD_MYSQL_PROTOCOL_MODE,
                               false, ObMysqlTransact::is_in_trans(trans_state_)|| is_in_trans_|| is_autocommit_0);
    } else {
      const bool enable_extra_ok_packet_for_stats = is_extra_ok_packet_for_stats_enabled();
      analyzer_.set_server_cmd(trans_state_.trans_info_.sql_cmd_,
          OCEANBASE_MYSQL_PROTOCOL_MODE, enable_extra_ok_packet_for_stats,
          ObMysqlTransact::is_in_trans(trans_state_) || is_in_trans_ || is_autocommit_0);
      LOG_DEBUG("handle_first_normal_response_packet", K(trans_state_.current_.state_),
                K(is_in_trans_), K(is_autocommit_0), K(enable_extra_ok_packet_for_stats));
    }
    if (OB_FAIL(analyzer_.analyze_response(
          *server_buffer_reader_, result, &server_response, need_receive_completed))) {
      LOG_WARN("fail to analyze response", K(server_buffer_reader_), K(server_response),
               K(need_receive_completed), K_(sm_id), K(ret));
    } else {
      state = result.status_;
      first_pkt_len = result.meta_.pkt_len_;
      cmd_size_stats_.server_response_bytes_ = server_buffer_reader_->read_avail();
    }
  } else {
    state = ANALYZE_CONT;
  }
  return ret;
}

int ObMysqlSM::state_server_request_send(int event, void *data)
{
  int ret = OB_SUCCESS;
  STATE_ENTER(ObMysqlSM::state_server_request_send, event);

  if (OB_ISNULL(server_entry_) || OB_ISNULL(data)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("invalid internal state, server entry is NULL or data is NULL",
              K_(server_entry), K(data), K_(sm_id), K(ret));
  } else if (server_entry_->read_vio_ != reinterpret_cast<ObVIO *>(data)
             && server_entry_->write_vio_ != reinterpret_cast<ObVIO *>(data)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("invalid internal state, server entry read vio isn't the same as data,"
              "and server entry write vio isn't the same as data",
              K_(server_entry_->read_vio),
              K_(server_entry_->write_vio), K(data), K_(sm_id), K(ret));
  } else {
    switch (event) {
      case VC_EVENT_WRITE_READY:
        server_entry_->write_vio_->reenable();
        break;

      case VC_EVENT_WRITE_COMPLETE: {
        milestones_.server_.server_write_end_ = get_based_hrtime();
        cmd_time_stats_.server_request_write_time_ += (milestones_.server_.server_write_end_ - milestones_.server_.server_write_begin_);
        // We are done sending the request, deallocate our
        // buffer and then decide what to do next
        if (NULL != server_entry_->write_buffer_) {
          free_miobuffer(server_entry_->write_buffer_);
          server_entry_->write_buffer_ = NULL;
        }
        ObMySQLCmd request_cmd = trans_state_.trans_info_.client_request_.get_packet_meta().cmd_;
        // before send quit cmd to observer, maybe we need send session vars first.
        // after send quit cmd to observer, this connection will disconnect soon.
        if ((OB_MYSQL_COM_QUIT == request_cmd) && (ObMysqlTransact::SERVER_SEND_REQUEST == trans_state_.current_.send_action_)) {
          // when receive 'quit' cmd, obproxy should disconnect after sending it to observer
          LOG_DEBUG("[setup_server_response_read] send quit to observer completed,"
                    " this connection will disconnect soon");
          ret = OB_CONNECT_ERROR;
        } else if ((OB_MYSQL_COM_STMT_CLOSE == request_cmd) && (ObMysqlTransact::SERVER_SEND_REQUEST == trans_state_.current_.send_action_)) {
          // remove ps_id_pair and cursor_id_pair
          ObClientSessionInfo &cs_info = client_session_->get_session_info();
          ObServerSessionInfo &ss_info = server_session_->get_session_info();
          uint32_t client_ps_id = cs_info.get_client_ps_id();
          ObPsIdAddrs *ps_id_addrs = cs_info.get_ps_id_addrs(client_ps_id);
          // remove directly
          ss_info.remove_ps_id_pair(client_ps_id);
          ss_info.remove_cursor_id_pair(client_ps_id);
          cs_info.remove_cursor_id_addr(client_ps_id);
          cs_info.remove_piece_info(client_ps_id);
          if (NULL != ps_id_addrs) {
            ps_id_addrs->remove_addr(server_session_->get_netvc()->get_remote_addr());
          }

          call_transact_and_set_next_state(ObMysqlTransact::handle_request);
        } else if (ObMysqlTransact::SERVER_SEND_SSL_REQUEST == trans_state_.current_.send_action_) {
          ObUnixNetVConnection *vc = static_cast<net::ObUnixNetVConnection *>(server_session_->get_netvc());
          if (OB_FAIL(vc->ssl_init(ObUnixNetVConnection::SSL_CLIENT))) {
            LOG_WARN("client ssl init failed", K(ret));
          } else  if (trans_state_.is_auth_request_) {
            trans_state_.current_.send_action_ = ObMysqlTransact::SERVER_SEND_LOGIN;
            trans_state_.next_action_ = ObMysqlTransact::SM_ACTION_API_SEND_REQUEST;
            if (OB_FAIL(setup_server_request_send())) {
              LOG_WARN("setup server request send failed", K(ret));
            }
          } else {
            trans_state_.current_.send_action_ = ObMysqlTransact::SERVER_SEND_SAVED_LOGIN;
            trans_state_.next_action_ = ObMysqlTransact::SM_ACTION_API_SEND_REQUEST;
            if (OB_FAIL(setup_server_request_send())) {
              LOG_WARN("setup server request send failed", K(ret));
            }
          }
        } else {
          if (!tunnel_.is_tunnel_active()) {
            // It's time to start reading the response
            if (OB_FAIL(setup_server_response_read())) {
              LOG_WARN("failed to setup_server_response_read", K_(sm_id), K(ret));
            }
          }
        }
        break;
      }
      case VC_EVENT_READ_READY:
        break;

      case VC_EVENT_EOS:
        server_entry_->eos_ = true;

        // if EOS is received on read and we are still in this state,
        // we must have not gotten WRITE_COMPLETE. With epoll we might
        // not receive EOS from both read and write sides of a connection
        // so it should be handled correctly (close tunnels, deallocate, etc)
        // here with handle_server_setup_error(). Otherwise we might hang
        // due to not shutting down and never receiving another event again.

        // fall through
      case VC_EVENT_ERROR:
      case VC_EVENT_ACTIVE_TIMEOUT:
      case VC_EVENT_INACTIVITY_TIMEOUT:
        LOG_WARN("ObMysqlSM::state_server_request_send", "event",
                 ObMysqlDebugNames::get_event_name(event), K_(sm_id));
        // if something unusual happened in sending request, also need to get request write time
        milestones_.server_.server_write_end_ = get_based_hrtime();
        cmd_time_stats_.server_request_write_time_ += (milestones_.server_.server_write_end_ - milestones_.server_.server_write_begin_);
        ret = OB_CONNECT_ERROR;
        break;

      case VC_EVENT_READ_COMPLETE:
        LOG_DEBUG("read complete due to 0 byte do_io_read");
        break;

      default:
        ret = OB_INNER_STAT_ERROR;
        LOG_ERROR("Unknown event", K(event), K_(sm_id), K(ret));
        break;
    }
  }

  if (OB_FAIL(ret)) {
    handle_server_setup_error(event, data);
  }

  return VC_EVENT_NONE;
}

int ObMysqlSM::process_partition_location(ObMysqlRouteResult &result)
{
  int ret = OB_SUCCESS;

  // update cached dummy entry first,
  update_cached_dummy_entry(result);

  if (trans_state_.pll_info_.is_cached_dummy_force_renew()) {
     trans_state_.pll_info_.set_cached_dummy_force_renew_done();
     // no need set route info, only to update cached dummy entry
     result.ref_reset();
  } else {
    trans_state_.pll_info_.set_route_info(result);
    // no table entry found from mysql route(both table entry and part entry are NULL)
    if (trans_state_.pll_info_.is_no_route_info_found() && !client_session_->is_need_update_dummy_entry_) {
      LOG_DEBUG("begin to process partition location, no avail route entry found,"
                " will use dummy entry", K_(trans_state_.pll_info));
      // if we can not find the certain table location, just use __all_dummy's partition location
      ObMysqlTransact::ObAttachDummyEntryType type = ObMysqlTransact::NO_TABLE_ENTRY_FOUND_ATTACH_TYPE;
      if (OB_FAIL(ObMysqlTransact::attach_cached_dummy_entry(trans_state_, type))) {
        LOG_WARN("can not get table entry, and dummy entry does not exist, will disconnect",
                 K_(sm_id), K(type), K(ret));
        // if failed, encode err packet and send to client, then disconnect
        int ret_tmp = OB_SUCCESS;
        trans_state_.mysql_errcode_ = OB_PASSWORD_WRONG;
        if (OB_UNLIKELY(OB_SUCCESS != (ret_tmp = ObMysqlTransact::build_error_packet(trans_state_)))) {
          LOG_WARN("fail to build err packet", K(ret_tmp));
        }
      }
    }
  }

  // update client session cached dummy entry, one transaction one shot at most
  if (OB_SUCC(ret) && trans_state_.pll_info_.is_cached_dummy_avail_force_renew()) {
    if (is_cached_dummy_entry_expired() || client_session_->is_need_update_dummy_entry_) {
      trans_state_.pll_info_.set_cached_dummy_force_renew();
    }
  }

  // only when dummy entry no need force renew, we can check_update_ldc;
  // or we should wait dummy entry force new done, then check_update_ldc;
  if (OB_SUCC(ret) && !trans_state_.pll_info_.is_cached_dummy_force_renew()) {

#if OB_DETAILED_SLOW_QUERY
    ObHRTime t1 = common::get_hrtime_internal();
#endif
    if (OB_FAIL(client_session_->check_update_ldc())) {
      LOG_WARN("fail to check_update_ldc", K(ret));
    }
#if OB_DETAILED_SLOW_QUERY
    ObHRTime t2 = common::get_hrtime_internal();
    cmd_time_stats_.debug_assign_time_ += (t2 - t1);
#endif

  }

  return ret;
}

void ObMysqlSM::update_cached_dummy_entry(ObMysqlRouteResult &result)
{
  ObTableEntry *table_entry = result.table_entry_;
  if ((NULL != table_entry) && table_entry->is_dummy_entry()) {
    // update client_session cached dummy entry
    if (OB_LIKELY(NULL != client_session_)) {
      client_session_->is_need_update_dummy_entry_ = false;
      if (OB_UNLIKELY(table_entry != client_session_->dummy_entry_)) {
        if (NULL != client_session_->dummy_entry_) {
          client_session_->dummy_entry_->dec_ref();
          client_session_->dummy_entry_ = NULL;
          //As dummy ldc has a tenant server ptr from dummy entry, we need reset it
          client_session_->dummy_ldc_.reset();
        }
        client_session_->dummy_entry_ = table_entry;
        client_session_->dummy_entry_->inc_ref();

        LOG_DEBUG("succ update cached dummy entry", KPC(table_entry));
      }
    }
  }
}

bool ObMysqlSM::is_cached_dummy_entry_expired()
{
  bool need_update = false;
  ObTableEntry *cached_dummy_entry = NULL;
  int64_t valid_ns = 0;
  if (NULL != client_session_) {
    cached_dummy_entry = client_session_->dummy_entry_;
    valid_ns = client_session_->dummy_entry_valid_time_ns_;
  }

  if (NULL != cached_dummy_entry) {
    if (cached_dummy_entry->is_deleted_state()) {
      need_update = true;
    } else if (cached_dummy_entry->is_need_update()) { // dirty, but not in punish time
      need_update = true;
    } else if (cached_dummy_entry->is_avail_state()) {
      if (valid_ns > 0) {
        bool expired = ((get_hrtime_internal() - hrtime_from_usec(cached_dummy_entry->get_create_time_us())) > valid_ns);
        if (expired
            && !cached_dummy_entry->is_sys_dummy_entry()
            && cached_dummy_entry->cas_set_dirty_state()) {
          LOG_INFO("this cached dummy entry is expired, set to dirty", KPC(cached_dummy_entry),
                   K(valid_ns));
          MYSQL_INCREMENT_TRANS_STAT(DUMMY_ENTRY_EXPIRED_COUNT);
          need_update = true;
        }
      }
    }
  }

  if (need_update) {
    LOG_INFO("this cached dummy entry need force renew", KPC(cached_dummy_entry));
  }

  return need_update;
}

int ObMysqlSM::process_server_addr_lookup(const ObProxyKillQueryInfo *query_info)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("begin to process_server_addr_lookup", K(*query_info));

  ObProxyKillQueryInfo *internal_query_info = trans_state_.trans_info_.client_request_.query_info_;
  if (OB_ISNULL(query_info) || OB_ISNULL(internal_query_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query_info is null, it should not happened",
             K(*query_info), K(*internal_query_info),  K_(sm_id), K(ret));
    trans_state_.pll_info_.lookup_success_ = false;
  } else {
    if(query_info->is_lookup_succ()) {
      LOG_DEBUG("succ to lookup server addr", K(*query_info));
      internal_query_info->real_conn_id_ = query_info->real_conn_id_;
      if (static_cast<int64_t>(query_info->real_conn_id_) != internal_query_info->cs_id_) {
        // we need reset req pkt
        if (OB_ISNULL(client_buffer_reader_) || OB_ISNULL(client_buffer_reader_->mbuf_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null client_buffer",  K_(sm_id), K(ret));
        //consume the client buffer
        } else if (OB_FAIL(client_buffer_reader_->consume_all())) {
          LOG_WARN("fail to consume request in buffer", K(ret));
        } else {
          ObSqlString sql;
          ObMIOBuffer *writer = client_buffer_reader_->mbuf_;
          ObMysqlAnalyzeResult result;
          const bool use_compress = false;
          const bool is_checksum_on = false;
          if (OB_ISNULL(writer)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null values ", K(writer), K_(sm_id), K(ret));
          } else if (OB_FAIL(ObServerAddrLookupHandler::build_kill_query_sql(query_info->real_conn_id_, sql))) {
            LOG_WARN("fail to build_kill_query_sql", K(*query_info), K_(sm_id), K(ret));
            // no need compress here, if server session support comrpess, it will compress later
          } else if (OB_FAIL(ObMysqlRequestBuilder::build_mysql_request(*writer, OB_MYSQL_COM_QUERY,
              sql.string(), use_compress, is_checksum_on))) {
            LOG_WARN("fail to build_mysql_request", K(*query_info), K_(sm_id), K(ret));
          } else if (OB_FAIL(ObProxyParserUtils::analyze_one_packet(*client_buffer_reader_, result))) {
            LOG_WARN("fail to analyze one packet", K(ret));
          } else if (ANALYZE_DONE != result.status_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to analyze one packet, error status_", K(result.status_), K(ret));
          } else {
            //update set_packet_meta as we had rewrite kill query cmd
            trans_state_.trans_info_.client_request_.set_packet_meta(result.meta_);
            LOG_DEBUG("succ to reset request packet", K(*query_info),
                      "total_len", trans_state_.trans_info_.client_request_.get_packet_len());
            internal_query_info->errcode_ = query_info->errcode_;
            trans_state_.server_info_.set_addr(query_info->server_addr_);
            trans_state_.pll_info_.lookup_success_ = true;
          }
        }

        if (OB_FAIL(ret)) {
          internal_query_info->errcode_ = OB_RESULT_UNKNOWN;
          trans_state_.pll_info_.lookup_success_ = false;
        }
      } else {
        // no need reset req pkt
        internal_query_info->errcode_ = query_info->errcode_;
        trans_state_.server_info_.set_addr(query_info->server_addr_);
        trans_state_.pll_info_.lookup_success_ = true;
      }
    } else {
      internal_query_info->errcode_ = query_info->errcode_;
      internal_query_info->priv_name_ = query_info->priv_name_;
      trans_state_.pll_info_.lookup_success_ = false;
    }
  }
  return ret;
}

int ObMysqlSM::state_server_addr_lookup(int event, void *data)
{
  int ret = OB_SUCCESS;
  STATE_ENTER(ObMysqlSM::state_server_addr_lookup, event);
  pending_action_ = NULL;

  if (OB_UNLIKELY(SERVER_ADDR_LOOKUP_EVENT_DONE != event)) {
    if (NULL != trans_state_.trans_info_.client_request_.query_info_) {
      trans_state_.trans_info_.client_request_.query_info_->errcode_ = OB_RESULT_UNKNOWN;
    }
    LOG_ERROR("unknown event type, it should not happen", K(event), K_(sm_id));
  } else if (OB_FAIL(process_server_addr_lookup(reinterpret_cast<ObProxyKillQueryInfo *>(data)))) {
    LOG_WARN("failed to process_server_addr_lookup", K_(sm_id), K(ret));
  }

  milestones_.pl_lookup_end_ = get_based_hrtime();
  cmd_time_stats_.pl_lookup_time_ += milestone_diff(milestones_.pl_lookup_begin_, milestones_.pl_lookup_end_);

  // call ObMysqlTransact::handle_server_addr_lookup() to handle fail / success
  call_transact_and_set_next_state(NULL);
  return EVENT_NONE;
}

int ObMysqlSM::state_partition_location_lookup(int event, void *data)
{
  int ret = OB_SUCCESS;
  STATE_ENTER(ObMysqlSM::state_partition_location_lookup, event);
  pending_action_ = NULL;

  milestones_.pl_lookup_end_ = get_based_hrtime();
  cmd_time_stats_.pl_lookup_time_ += milestone_diff(milestones_.pl_lookup_begin_, milestones_.pl_lookup_end_);

  milestones_.pl_process_begin_ = milestones_.pl_lookup_end_;

  if (OB_UNLIKELY(TABLE_ENTRY_EVENT_LOOKUP_DONE != event) || OB_ISNULL(data)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected event type, it should not happen", K(event), K(data), K(ret));
  } else {
    ObMysqlRouteResult *result = reinterpret_cast<ObMysqlRouteResult *>(data);

    trans_state_.pll_info_.set_need_force_flush(false);

    if (trans_state_.pll_info_.is_force_renew()) {
      trans_state_.pll_info_.set_force_renew_done();
    }

    if (OB_FAIL(process_partition_location(*result))) {
      LOG_WARN("fail to process partition location", K_(sm_id), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (trans_state_.pll_info_.is_cached_dummy_force_renew()) {
      trans_state_.pll_info_.lookup_success_ = false;
      milestones_.pl_process_end_ = get_based_hrtime();
      cmd_time_stats_.pl_process_time_ += milestone_diff(milestones_.pl_process_begin_, milestones_.pl_process_end_);
      // update dummy entry and do pl again
      call_transact_and_set_next_state(ObMysqlTransact::modify_pl_lookup);
    } else {
      trans_state_.pll_info_.lookup_success_ = true;
      // call ObMysqlTransact::handle_pl_lookup() to handle fail / success
      call_transact_and_set_next_state(NULL);
    }
  } else {
    trans_state_.inner_errcode_ = ret;
    // failed, disconnect
    trans_state_.pll_info_.lookup_success_ = false;
    trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
    // call ObMysqlTransact::handle_pl_lookup() to handle fail / success
    call_transact_and_set_next_state(NULL);
  }

  return EVENT_DONE;
}

int ObMysqlSM::main_handler(int event, void *data)
{
  MysqlSMHandler jump_point = NULL;
  ObMysqlVCTableEntry *vc_entry = NULL;

  if (OB_UNLIKELY(MYSQL_SM_MAGIC_ALIVE != magic_)
      || OB_UNLIKELY(reentrancy_count_ < 0)) {
    LOG_ERROR("invalid sm magic or reentrancy_count", K_(magic), K_(reentrancy_count), K_(sm_id), K(event));
  }
  ++reentrancy_count_;

  // Don't use the state enter macro since it uses history
  // space that we don't care about
  LOG_DEBUG("[ObMysqlSM::main_handler]",
           K_(sm_id), "event", ObMysqlDebugNames::get_event_name(event),
           "ethread", this_ethread());

  if (NULL != data) {
    // Only search the VC table if the event could have to
    // do with a ObVIO to save a few cycles
    if (event < VC_EVENT_EVENTS_START + 100) {
      vc_entry = vc_table_.find_entry(reinterpret_cast<ObVIO*>(data));
    }
  }

  if (NULL != vc_entry) {
    jump_point = vc_entry->vc_handler_;
    if (OB_ISNULL(jump_point) || (OB_ISNULL(vc_entry->vc_))) {
      LOG_ERROR("invalid internal state, vc handler is NULL or vc is NULL",
                K_(vc_entry->vc), K_(sm_id));
    } else {
      (this->*jump_point)(event, data);
    }
  } else {
    if (OB_ISNULL(default_handler_)) {
      LOG_ERROR("invalid internal state, default handler is NULL", K_(sm_id));
    } else {
      (this->*default_handler_)(event, data);
    }
  }

  // The sub-handler signals when it is time for the state machine
  // to exit. We can only exit if we are not reentrantly called
  // otherwise when the our call unwinds, we will be
  // running on a dead state machine
  //
  // Because of the need for an api shutdown hook, kill_this()
  // is also reentrant. As such, we don't want to decrement the
  // reentrancy count until after we run kill_this()
  if (terminate_sm_ && 1 == reentrancy_count_) {
    kill_this();
  } else {
    --reentrancy_count_;
    if (OB_UNLIKELY(reentrancy_count_ < 0)) {
      LOG_ERROR("invalid reentrancy_count", K_(reentrancy_count), K_(sm_id));
    }
  }

  return VC_EVENT_CONT;
}

// Handles completion of any mysql request tunnel
int ObMysqlSM::tunnel_handler_request_transfered(int event, void *data)
{
  int ret = OB_SUCCESS;
  STATE_ENTER(ObMysqlSM::tunnel_handler_request_transfered, event);

  if (OB_UNLIKELY(MYSQL_TUNNEL_EVENT_DONE != event) || OB_UNLIKELY(data != &tunnel_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("unexpected event type, or data isn't  the same as &tunnel_",
              K(event), K(data), K_(client_entry),
              K_(server_entry), K_(sm_id), K(ret));
  } else if (OB_ISNULL(client_entry_) || OB_ISNULL(server_entry_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("client_entry or server entry is null, disconnect",
             K_(server_entry), K_(client_entry), K(ret));
  } else {
    // The tunnel calls this when it is done
    ObMysqlTunnelProducer *p = tunnel_.get_producer(client_session_);
    ObMysqlTunnelConsumer *c = NULL;

    if (OB_ISNULL(p) || OB_UNLIKELY(MT_MYSQL_CLIENT != p->vc_type_)) {
      ret = OB_INNER_STAT_ERROR;
      LOG_ERROR("invalid producer vc type, should be MT_MYSQL_CLIENT",
                K_(p->vc_type), K_(sm_id), K(ret));
    } else {
      // If there is a request transform, remove it's entry from the State
      // Machine's VC table
      //
      // MUST NOT clear the vc pointer from request_transform_info
      // as this causes a double close of the transform vc in transform_cleanup
      if (NULL != api_.request_transform_info_.vc_) {
        if (!api_.request_transform_info_.entry_->in_tunnel_
            || api_.request_transform_info_.vc_ != api_.request_transform_info_.entry_->vc_) {
          ret = OB_INNER_STAT_ERROR;
          LOG_ERROR("invalid internal state, request transform entry must be in tunnel,"
                    "and the vc must be the same",
                    K_(api_.request_transform_info_.entry_->in_tunnel),
                    K_(api_.request_transform_info_.vc),
                    K_(api_.request_transform_info_.entry_->vc), K_(sm_id), K(ret));
        } else if (OB_FAIL(vc_table_.cleanup_entry(api_.request_transform_info_.entry_))) {
          LOG_WARN("vc table failed to cleanup server entry", K_(sm_id), K(ret));
        } else {
          api_.request_transform_info_.entry_ = NULL;
        }
      }
    }

    if (OB_SUCC(ret)) {
      switch (p->handler_state_) {
        case MYSQL_SM_REQUEST_TRANSFER_SERVER_FAIL:
          c = tunnel_.get_consumer(server_entry_->vc_);
          if (OB_ISNULL(c) || OB_UNLIKELY(c->write_success_)) {
            ret = OB_INNER_STAT_ERROR;
            LOG_ERROR("invalid internal state, consumer must be in write success state",
                      K(c), K_(sm_id), K(ret));
          } else {
            handle_request_transfer_failure();
          }
          break;

        case MYSQL_SM_REQUEST_TRANSFER_CLIENT_FAIL:
          // client quit - shutdown the SM
          if (p->read_success_) {
            ret = OB_INNER_STAT_ERROR;
            LOG_ERROR("invalid internal state, producer should not read success",
                      K_(p->read_success), K_(sm_id), K(ret));
          } else {
            terminate_sm_ = true;
          }
          break;

        case MYSQL_SM_REQUEST_TRANSFER_SUCCESS:
          // The request transfer succeeded
          if (!p->read_success_ || !p->consumer_list_.head_->write_success_) {
            ret = OB_INNER_STAT_ERROR;
            LOG_ERROR("invalid internal state, producer must read and write sucess",
                      K_(p->read_success), K_(p->consumer_list_.head_->write_success), K_(sm_id), K(ret));
          } else if (client_entry_->in_tunnel_) {
            // When the client completed sending it's data we must have
            // removed it from the tunnel
            ret = OB_INNER_STAT_ERROR;
            LOG_ERROR("invalid internal state, client entry should not in tunnel",
                      K_(client_entry_->in_tunnel), K_(sm_id), K(ret));
          } else {
            tunnel_.reset();
            server_entry_->in_tunnel_ = false;
            trans_state_.reset_internal_buffer();
            // It's time to start reading the response
            if (OB_FAIL(setup_server_response_read())) {
              LOG_WARN("failed to setup_server_response_read", K_(sm_id), K(ret));
            }
          }
          break;

        case MYSQL_SM_REQUEST_TRANSFER_TRANSFORM_FAIL: {
          LOG_WARN("MYSQL_SM_REQUEST_TRANSFER_TRANSFORM_FAIL", K_(sm_id), K(ret));
          terminate_sm_ = true;
          break;
        }

        default:
          terminate_sm_ = true;
          ret = OB_INNER_STAT_ERROR;
          LOG_ERROR("Unknown request transfer status", K_(p->handler_state), K_(sm_id), K(ret));
          break;
      }
    }
  }

  if (OB_FAIL(ret)) {
    trans_state_.inner_errcode_ = ret;
    trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
    call_transact_and_set_next_state(ObMysqlTransact::handle_response);
  }

  return EVENT_DONE;
}

int ObMysqlSM::tunnel_handler_response_transfered(int event, void *data)
{
  STATE_ENTER(ObMysqlSM::tunnel_handler_response_transfered, event);

  if (OB_UNLIKELY(MYSQL_TUNNEL_EVENT_DONE != event) || OB_UNLIKELY(data != &tunnel_)) {
    terminate_sm_ = true;
    LOG_ERROR("unexpected event type", K(event), K_(sm_id));
  } else {
    // only the first request in one transaction will need to update pl,
    // begin(start transaction), or set autocommit = 0 is not the first request;
    if (trans_state_.is_trans_first_request_) {
      ObMysqlTransact::handle_pl_update(trans_state_);
    }

    if (obmysql::COM_STMT_FETCH == trans_state_.trans_info_.sql_cmd_
        && client_session_->is_need_return_last_bound_ss()) {
      int ret = OB_SUCCESS;
      ObMysqlServerSession *last_bound_session = client_session_->get_last_bound_server_session();
      if (NULL != last_bound_session) {
        // 由于 tunnel_handler_server 中只有事务中才会释放 server_session,
        // 正常释放 server sssion 有两个地方:
        //   1. 事务中, tunnel_handler_server
        //   2. 事务结束, setup_cmd_complete
        // 对于事务中的 COM_STMT_FETCH, 如果需要切换到另外一台 Server:
        //   1. 在 tunnel_handler_server 时, 是认为事务结束了. 因为 in_trans = false;
        //   2. 由于这里修改了事务状态，在 setup_cmd_complete 中又认为是事务中
        // 所以上面两处都不会释放, 所以这里要释放一次
        release_server_session();
        if (OB_FAIL(ObMysqlTransact::return_last_bound_server_session(client_session_))) {
          LOG_WARN("fail to return last bound server session", K(ret));
        } else {
          trans_state_.current_.state_ = ObMysqlTransact::CMD_COMPLETE;
        }
      } else {
        trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
        LOG_WARN("need return last bound ss, but last bound ss is NULL", K(ret));
      }
    }

    // each sm will be destroyed after it runs 5 secondes.
    if (NULL != client_session_
        && (ObMysqlTransact::CMD_COMPLETE == trans_state_.current_.state_
            || ObMysqlTransact::TRANSACTION_COMPLETE == trans_state_.current_.state_)) {
      // FIXME: can't call api and release mutex, because no continuation
      // handle event from client session now
      callout_api_and_start_next_action(ObMysqlTransact::SM_ACTION_API_CMD_COMPLETE);
    } else {
      // The tunnel calls this when it is done
      terminate_sm_ = true;
    }
  }

  return EVENT_DONE;
}

int ObMysqlSM::tunnel_handler_server(int event, ObMysqlTunnelProducer &p)
{
  int ret = OB_SUCCESS;
  STATE_ENTER(ObMysqlSM::tunnel_handler_server, event);
  milestones_.server_.server_read_end_ = get_based_hrtime();
  cmd_time_stats_.server_response_read_time_ += p.cost_time_;

  bool close_connection = false;

  if (!server_entry_->eos_ && MYSQL_NO_PLUGIN_TUNNEL == api_.plugin_tunnel_type_) {
    close_connection = false;
  } else {
    close_connection = true;
  }

  if (OB_ISNULL(server_entry_) || OB_ISNULL(server_session_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("server entry and server session should not be NULL",
              K_(server_entry), K_(server_session), K_(sm_id), K(ret));
  } else if (OB_UNLIKELY(server_entry_->vc_ != p.vc_)
             || OB_UNLIKELY(MT_MYSQL_SERVER != p.vc_type_)
             || p.vc_ != server_session_) {
    ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("invalid internal state, server entry vc, producer vc, server session are different",
              K_(server_entry_->vc), K_(p.vc),
              K_(p.vc_type), K_(server_session), K_(sm_id), K(ret));
  } else {
    switch (event) {
      case VC_EVENT_INACTIVITY_TIMEOUT:
      case VC_EVENT_ACTIVE_TIMEOUT:
      case VC_EVENT_ERROR:
        // fall through
      case VC_EVENT_EOS:
        switch (event) {
          case VC_EVENT_INACTIVITY_TIMEOUT:
            trans_state_.current_.state_ = ObMysqlTransact::INACTIVE_TIMEOUT;
            break;
          case VC_EVENT_ACTIVE_TIMEOUT:
            trans_state_.current_.state_ = ObMysqlTransact::ACTIVE_TIMEOUT;
            break;
          case VC_EVENT_ERROR:
            trans_state_.current_.state_ = ObMysqlTransact::CONNECTION_ERROR;
            break;
          case VC_EVENT_EOS:
            // when event is VC_EVENT_EOS, there are two kinds of situations:
            // 1. ObMysqlTransact::TRANSACTION_COMPLETE:
            //    the current transaction on this connection is completed, but server session is close.
            // 2. ObMysqlTransact::CONNECTION_ERROR:
            //    this connection is handling transactions, but server session is close.
            if (NULL != p.packet_analyzer_.server_response_
             && p.packet_analyzer_.server_response_->get_analyze_result().is_trans_completed()) {
              trans_state_.current_.state_ = ObMysqlTransact::TRANSACTION_COMPLETE;
            } else {
              trans_state_.current_.state_ = ObMysqlTransact::CONNECTION_ERROR;
            }
            break;
          default:
            break;
        }

        MYSQL_INCREMENT_TRANS_STAT(BROKEN_SERVER_CONNECTIONS);

        LOG_WARN("[ObMysqlSM::tunnel_handler_server] finishing mysql tunnel", K_(sm_id),
                 "event", ObMysqlTransact::get_server_state_name(trans_state_.current_.state_));
        p.read_success_ = true;
        trans_state_.server_info_.abort_ = ObMysqlTransact::DIDNOT_ABORT;
        if (OB_FAIL(tunnel_.local_finish_all(p))) {
          LOG_ERROR("fail to do tunnel finish all", K_(sm_id),
                    "event", ObMysqlTransact::get_server_state_name(trans_state_.current_.state_),
                    K(ret));
        }

        close_connection = true;
        break;

      case MYSQL_TUNNEL_EVENT_PRECOMPLETE:
      case MYSQL_TUNNEL_EVENT_CMD_COMPLETE:
      case VC_EVENT_READ_COMPLETE:{
        p.read_success_ = true;

        if (MYSQL_TUNNEL_EVENT_CMD_COMPLETE == event) {
          // One command complete of the transaction
          trans_state_.current_.state_ = ObMysqlTransact::CMD_COMPLETE;
        } else {
          trans_state_.current_.state_ = ObMysqlTransact::TRANSACTION_COMPLETE;
        }

        if (OB_FAIL(tunnel_handler_server_cmd_complete(p))) {
          LOG_WARN("failed to tunnel_handler_server_cmd_complete", K_(sm_id), K(ret));
          close_connection = true;
        }
        // ignore ret, go on
        trans_state_.server_info_.abort_ = ObMysqlTransact::DIDNOT_ABORT;
        if (OB_FAIL(tunnel_.local_finish_all(p))) {
          LOG_ERROR("fail to do tunnel finish all", K_(sm_id),
                    "event", ObMysqlTransact::get_server_state_name(trans_state_.current_.state_),
                    K(ret));
          close_connection = true;
        }
        break;
      }
      case MYSQL_TUNNEL_EVENT_CONSUMER_DETACH:
        // All consumers are prematurely gone. Shutdown the server connection
        p.read_success_ = true;
        trans_state_.current_.state_ = ObMysqlTransact::TRANSACTION_COMPLETE;
        trans_state_.server_info_.abort_ = ObMysqlTransact::DIDNOT_ABORT;
        close_connection = true;
        break;

      case VC_EVENT_READ_READY:
      case VC_EVENT_WRITE_READY:
      case VC_EVENT_WRITE_COMPLETE:
      default:
        // None of these events should ever come our way
        ret = OB_INNER_STAT_ERROR;
        LOG_ERROR("unexpected event", K(event), K_(sm_id), K(ret));
        break;
    }
  }

  trans_state_.server_info_.state_ = trans_state_.current_.state_;

  // We handled the event. Now either shutdown the connection or
  // setup it up for keep-alive
  if (close_connection || OB_FAIL(ret)) {
    if (NULL != p.vc_) {
      p.vc_->do_io_close();
    }
    p.read_vio_ = NULL;
  } else {
    if (NULL != client_session_) {
      LOG_DEBUG("Attaching server session to the client", K_(sm_id));

      --(server_session_->server_trans_stat_);
      if (OB_FAIL(client_session_->attach_server_session(server_session_))) {
        LOG_WARN("client session failed to attach server session", K_(sm_id), K(ret));
      }
    } else {
      // if client session disconnect, just close the tunnel's vc
      p.vc_->do_io_close();
      p.read_vio_ = NULL;
    }
  }

  if (OB_SUCC(ret)) {
    if (MYSQL_TUNNEL_EVENT_CMD_COMPLETE != event) {
      MYSQL_DECREMENT_DYN_STAT(CURRENT_SERVER_TRANSACTIONS);
    } else {
      // If transaction doesn't complete, and command completes, we cleanup
      // server entry and store server session. Next command of the transaction
      // must use the same server session.
      // If transaction completes, the server entry will be cleaned up when
      // mysqlsm exit.
      if (OB_FAIL(vc_table_.cleanup_entry(server_entry_, true))) {
        LOG_WARN("vc table failed to cleanup server entry", K_(sm_id), K(ret));
      } else {
        server_entry_ = NULL;
        server_session_ = NULL;
      }
    }
  }

  if (OB_FAIL(ret)) {
    trans_state_.inner_errcode_ = ret;
    trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
  }

  return ret;
}

int ObMysqlSM::handle_saved_session_variables()
{
  int ret = OB_SUCCESS;
  ObRespAnalyzeResult &analyze_result = trans_state_.trans_info_.server_response_.get_analyze_result();
  // reset db if need
  if (ObMysqlTransact::is_db_reset(trans_state_)) {
    ObMysqlTransact::handle_db_reset(trans_state_);
  }

  // record last_insert_id
  if (!client_session_->get_session_info().is_sharding_user()) {
    if (analyze_result.is_last_insert_id_changed()) {
      client_session_->set_lii_server_session(server_session_);
      LOG_DEBUG("last_insert_id is changed, record last_insert_id server session");
    }
  }

  if (analyze_result.has_proxy_idc_name_user_var()) {
#if OB_DETAILED_SLOW_QUERY
    ObHRTime t1 = common::get_hrtime_internal();
#endif
    client_session_->check_update_ldc();
#if OB_DETAILED_SLOW_QUERY
    ObHRTime t2 = common::get_hrtime_internal();
    cmd_time_stats_.debug_assign_time_ += (t2 - t1);
#endif

  }

  // if has new sys var, we will try to add task
  if (analyze_result.has_new_sys_var()) {
    if (OB_ISNULL(sm_cluster_resource_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected cluster resource is null", K(ret));
    } else if (!sm_cluster_resource_->is_avail()) {
      LOG_DEBUG("cluster resource is not created completely");
    } else if (OB_FAIL(sm_cluster_resource_->sys_var_set_processor_.add_sys_var_renew_task(*sm_cluster_resource_))) {
      LOG_WARN("fail to add sys var renew task, we will retry at the next request",
               K(ret));
      // return OB_SUCCESS anyway, we will retry at the next request
      ret = OB_SUCCESS;
    } else {
      LOG_DEBUG("add sys var renew task successfully", K(ret));
    }
  }

  return ret;
}

void ObMysqlSM::print_mysql_complete_log(ObMysqlTunnelProducer *p)
{
  if (ObMysqlTransact::CMD_COMPLETE == trans_state_.current_.state_
     || ObMysqlTransact::TRANSACTION_COMPLETE == trans_state_.current_.state_) {
    // just print sql, observer addr, error code together for easy debugging
    ObRespAnalyzeResult &result = trans_state_.trans_info_.server_response_.get_analyze_result();
    bool print_warn_log = false;
    bool print_info_log = false;
    if (OB_LIKELY(NULL != client_session_)) {
      if (OB_UNLIKELY(result.is_error_resp())) {
        // filtering some error code later
        if (result.is_not_supported_error()) {
          print_info_log = true;
          OBPROXY_XF_LOG(INFO, XFH_SQL_NOT_SUPPORT,
                         "client_ip", trans_state_.client_info_.addr_,
                         "server_ip", trans_state_.server_info_.addr_,
                         "proxy_user_name", client_session_->get_session_info().get_priv_info().get_proxy_user_name(),
                         "database_name", client_session_->get_session_info().get_database_name(),
                         "sql", trans_state_.trans_info_.client_request_.get_print_sql(),
                         "sql_cmd", get_mysql_cmd_str(trans_state_.trans_info_.sql_cmd_));
        } else {
          print_warn_log = true;
          OBPROXY_XF_LOG(WARN, XFH_SQL_ERROR_RESP,
                         "client_ip", trans_state_.client_info_.addr_,
                         "server_ip", trans_state_.server_info_.addr_,
                         "proxy_user_name", client_session_->get_session_info().get_priv_info().get_proxy_user_name(),
                         "database_name", client_session_->get_session_info().get_database_name(),
                         "sql", trans_state_.trans_info_.client_request_.get_print_sql(),
                         "sql_cmd", get_mysql_cmd_str(trans_state_.trans_info_.sql_cmd_),
                         "sql_error", get_xflush_error_name(result.get_error_code()),
                         "msg", result);
        }
      } else if (!result.is_partition_hit()
                 && trans_state_.is_trans_first_request_
                 && !client_session_->is_proxyro_user()) {//proxyro user no need print partition miss xflush_log
        print_info_log = true;
        OBPROXY_XF_LOG(INFO, XFH_SQL_PARTITION_MISS,
                       "client_ip", trans_state_.client_info_.addr_,
                       "server_ip", trans_state_.server_info_.addr_,
                       "proxy_user_name", client_session_->get_session_info().get_priv_info().get_proxy_user_name(),
                       "database_name", client_session_->get_session_info().get_database_name(),
                       "sql", trans_state_.trans_info_.client_request_.get_print_sql(),
                       "sql_cmd", get_mysql_cmd_str(trans_state_.trans_info_.sql_cmd_));
      }

      if(NULL != p && p->is_flow_controlled()) {
        print_info_log = true;
        OBPROXY_XF_LOG(INFO, XFH_SQL_FLOW_CTL,
                       "client_ip", trans_state_.client_info_.addr_,
                       "server_ip", trans_state_.server_info_.addr_,
                       "proxy_user_name", client_session_->get_session_info().get_priv_info().get_proxy_user_name(),
                       "database_name", client_session_->get_session_info().get_database_name(),
                       "sql", trans_state_.trans_info_.client_request_.get_print_sql(),
                       "sql_cmd", get_mysql_cmd_str(trans_state_.trans_info_.sql_cmd_),
                       "cpu_ctl_count", p->cpu_flow_control_count_,
                       "mem_ctl_count", p->memory_flow_control_count_);
      }
    }

    bool is_flow_controlled = false;
    int64_t cpu_flow_control_count = 0;
    int64_t memory_flow_control_count = 0;
    if (NULL != p) {
      is_flow_controlled = p->is_flow_controlled();
      cpu_flow_control_count = p->cpu_flow_control_count_;
      memory_flow_control_count = p->memory_flow_control_count_;
    }

    uint64_t proxy_sessid = 0;
    uint32_t cs_id = 0;
    uint32_t server_sessid = 0;
    int64_t ss_id = 0;
    if (NULL != client_session_) {
      proxy_sessid = client_session_->get_proxy_sessid();
      cs_id = client_session_->get_cs_id();
    }
    if (NULL != server_session_) {
      server_sessid = server_session_->get_server_sessid();
      ss_id = server_session_->ss_id_;
    }

    if (OB_UNLIKELY(print_warn_log)) {
      LOG_WARN("finishing mysql tunnel",
               K_(sm_id),
               K(cs_id),
               K(proxy_sessid),
               K(ss_id),
               K(server_sessid),
               "client_ip", trans_state_.client_info_.addr_,
               "server_ip", trans_state_.server_info_.addr_,
               "server_trace_id", get_server_trace_id(),
               "proxy_user_name", client_session_->get_session_info().get_priv_info().get_proxy_user_name(),
               "database_name", client_session_->get_session_info().get_database_name(),
               K(is_flow_controlled),
               K(cpu_flow_control_count),
               K(memory_flow_control_count),
               "sql", trans_state_.trans_info_.client_request_.get_print_sql(),
               "sql_cmd", get_mysql_cmd_str(trans_state_.trans_info_.sql_cmd_),
               K(result));
    } else if (OB_UNLIKELY(print_info_log)) {
      LOG_INFO("finishing mysql tunnel",
               K_(sm_id),
               K(cs_id),
               K(proxy_sessid),
               K(ss_id),
               K(server_sessid),
               "client_ip", trans_state_.client_info_.addr_,
               "server_ip", trans_state_.server_info_.addr_,
               "server_trace_id", get_server_trace_id(),
               "proxy_user_name", client_session_->get_session_info().get_priv_info().get_proxy_user_name(),
               "database_name", client_session_->get_session_info().get_database_name(),
               K(is_flow_controlled),
               K(cpu_flow_control_count),
               K(memory_flow_control_count),
               "sql", trans_state_.trans_info_.client_request_.get_print_sql(),
               "sql_cmd", get_mysql_cmd_str(trans_state_.trans_info_.sql_cmd_),
               K(result));
    } else {
      LOG_DEBUG("finishing mysql tunnel",
                K_(sm_id),
                K(cs_id),
                K(proxy_sessid),
                K(ss_id),
                K(server_sessid),
                "client_ip", trans_state_.client_info_.addr_,
                "server_ip", trans_state_.server_info_.addr_,
                "server_trace_id", get_server_trace_id(),
                "proxy_user_name", client_session_->get_session_info().get_priv_info().get_proxy_user_name(),
                "database_name", client_session_->get_session_info().get_database_name(),
                K(is_flow_controlled),
                K(cpu_flow_control_count),
                K(memory_flow_control_count),
                "sql", trans_state_.trans_info_.client_request_.get_print_sql(),
                "sql_cmd", get_mysql_cmd_str(trans_state_.trans_info_.sql_cmd_),
                K(result));
    }
  }
}

void ObMysqlSM::update_safe_read_snapshot()
{
  // sm_cluster_resource_ and client_session_ is not null
  ObRespAnalyzeResult &result = trans_state_.trans_info_.server_response_.get_analyze_result();
  ObSafeSnapshotEntry *entry = NULL;
  bool is_need_force_sync = true;
  ObAddr last_addr;
  last_addr.set_ipv4_addr(trans_state_.server_info_.addr_.get_ip4_host_order(),
                          static_cast<int32_t>(trans_state_.server_info_.addr_.get_port_host_order()));

  if (result.is_partition_hit()) {
    entry = sm_cluster_resource_->safe_snapshot_mgr_.get(last_addr);
    if (OB_ISNULL(entry)) {
      if (sm_cluster_resource_->is_base_servers_added()) {
        LOG_WARN("entry not exist, which is unexpected", K(last_addr));
      }
    } else {
      // in this conditition, we no need to force sync
      is_need_force_sync = false;
      entry->update_safe_read_snapshot(client_session_->get_session_info().get_safe_read_snapshot(),
                                       is_need_force_sync);
      LOG_DEBUG("update read snapshot entry succ ", KPC(entry));
    }
  } else {
    int64_t count = client_session_->dummy_ldc_.count();
    const ObLDCItem *item_array = client_session_->dummy_ldc_.get_item_array();
    for (int64_t i = 0; i < count; ++i) {
      if (OB_ISNULL(item_array[i].replica_)) {
        LOG_WARN("item_array[i].replica_ is null, ignore it");
      } else {
        entry = sm_cluster_resource_->safe_snapshot_mgr_.get(item_array[i].replica_->server_);
        if (OB_ISNULL(entry)) {
          LOG_INFO("entry is null, maybe new server added", K(item_array[i].replica_->server_));
        } else {
          if (item_array[i].replica_->server_ != last_addr) {
            is_need_force_sync = true;
          } else {
            is_need_force_sync = false;
          }
          entry->update_safe_read_snapshot(
              client_session_->get_session_info().get_safe_read_snapshot(),
              is_need_force_sync);
          LOG_DEBUG("update read snapshot entry succ ", KPC(entry));
        }
      }
    }
  }

  ObProxyPartitionLocation *pl = const_cast<ObProxyPartitionLocation *>(trans_state_.pll_info_.route_.cur_chosen_pl_);
  if (pl != NULL && pl->is_server_changed()) {
    LOG_INFO("As we have visit this table entry, mark server unchanged", KPC(pl));
    pl->mark_server_unchanged();
  }
}

int ObMysqlSM::tunnel_handler_server_cmd_complete(ObMysqlTunnelProducer &p)
{
  int ret = OB_SUCCESS;
  bool found = false;

  // If the produce completed, the analyzer in tunnel filled the result in the server response.
  // If it's resultset protocol, there is an extra ok packet in the consumer's read buffer, and
  // the reserved_len in the analyze result is the length of extra ok packet and it is not 0.
  // Before the consumer consumes the data, we analyze the extra ok packet at the tail of
  // consumer's read buffer.
  ObRespAnalyzeResult &analyze_result = trans_state_.trans_info_.server_response_.get_analyze_result();
  ObHRTime trim_ok_packet_begin = 0;
  if (!analyze_result.is_last_ok_handled() && analyze_result.get_last_ok_pkt_len() > 0) {
    for (ObMysqlTunnelConsumer *c = p.consumer_list_.head_; NULL != c && !found && OB_SUCC(ret); c = c->link_.next_) {
      if (c->alive_ && NULL != c->buffer_reader_) {
        // 1. analyze ok packet
        trim_ok_packet_begin = get_based_hrtime();

        if (OB_FAIL(trim_ok_packet(*(c->buffer_reader_)))) {
          LOG_WARN("fail to trim ok packet",  K_(sm_id), K(ret));
        } else {
          analyze_result.is_last_ok_handled_ = true;
          // we change the writer_ in analyze_extra_ok_packet/rebuild_ok_packet
          // so we should set bytes_read_ to the corrent value
          p.bytes_read_ += (analyze_result.get_rewritten_last_ok_pkt_len()
                            - analyze_result.get_last_ok_pkt_len());
        }

        cmd_time_stats_.ok_packet_trim_time_ +=
          milestone_diff(trim_ok_packet_begin, get_based_hrtime());

        // break is we found an valid consumer
        found = true;
      }  // end of if (c->alive_ && NULL != c->buffer_reader_)
    } // end of for loop

    // print log
    print_mysql_complete_log(&p);
  } // end of analyze ok packet

  return ret;
}

int ObMysqlSM::trim_ok_packet(ObIOBufferReader &reader)
{
  int ret = OB_SUCCESS;
  ObMysqlClientSession *client_session = get_client_session();
  ObMysqlServerSession *server_session = get_server_session();
  if (NULL == server_session && NULL != client_session) {
    server_session = client_session->get_server_session();
  }

  if (NULL == client_session || NULL == server_session ) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid session", K(client_session), K(server_session));
  } else {
    // trim or rebuild ok packet
    bool need_handle_sysvar = sm_cluster_resource_->is_avail();
    ObRespAnalyzeResult &analyze_result =
      trans_state_.trans_info_.server_response_.get_analyze_result();
    LOG_DEBUG("handle last ok packet", K(need_handle_sysvar),
              "action type", analyze_result.get_ok_packet_action_type(),
              "lask ok pkt len", analyze_result.get_last_ok_pkt_len());

    if (OK_PACKET_ACTION_CONSUME == analyze_result.get_ok_packet_action_type()) {
      if (OB_FAIL(ObProxySessionInfoHandler::analyze_extra_ok_packet(
          reader, client_session->get_session_info(),
          server_session->get_session_info(), need_handle_sysvar, analyze_result))) {
        LOG_WARN("fail to analyze extra ok packet",  K_(sm_id), K(ret));
      }
    } else if (OK_PACKET_ACTION_REWRITE == analyze_result.get_ok_packet_action_type()) {
      ObClientSessionInfo &client_info = client_session->get_session_info();

      const ObProxyBasicStmtType type = trans_state_.trans_info_.client_request_.get_parse_result().get_stmt_type();
      bool is_save_to_common_sys = client_info.is_sharding_user()
                                   && (OBPROXY_T_SET == type || OBPROXY_T_SET_NAMES == type || OBPROXY_T_SET_CHARSET == type);
      if (OB_FAIL(ObProxySessionInfoHandler::rebuild_ok_packet(
          reader, client_session->get_session_info(),
          server_session->get_session_info(), trans_state_.is_auth_request_,
          need_handle_sysvar, analyze_result, is_save_to_common_sys))) {
        LOG_WARN("fail to analyze rewrite ok packet",  K_(sm_id), K(ret));
      }
    }

    // reset route_addr
    client_session->get_session_info().set_obproxy_route_addr(0);

    if (is_causal_order_read_enabled()) {
      // handle safe snapshot version
      if (WEAK == trans_state_.get_trans_consistency_level(client_session->get_session_info())
          && trans_state_.trans_info_.client_request_.get_parse_result().is_select_stmt()) {
        update_safe_read_snapshot();
      }
    }

    // handle other variables
    if (OB_SUCC(ret)) {
      if (OB_FAIL(handle_saved_session_variables())) {
        LOG_WARN("fail to handle saved session varialbes", K(ret));
      }
    }
  }
  return ret;
}

int ObMysqlSM::tunnel_handler_client(int event, ObMysqlTunnelConsumer &c)
{
  int ret = OB_SUCCESS;
  bool close_connection = true;
  ObMysqlTunnelProducer *p = NULL;
  ObMysqlTunnelConsumer *selfc = NULL;

  STATE_ENTER(ObMysqlSM::tunnel_handler_client, event);

  if (OB_ISNULL(client_entry_) || OB_ISNULL(client_session_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("client entry and client session should not be NULL",
              K_(client_entry), K_(client_session), K_(sm_id), K(ret));
  } else if (OB_UNLIKELY(client_entry_->vc_ != c.vc_)
             || c.vc_ != client_session_) {
    ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("invalid internal state, client entry vc, consumer vc and client session are different",
              K_(client_entry_->vc), K_(c.vc),
              K_(client_session), K_(sm_id), K(ret));
  } else {
    milestones_.client_.client_end_ = get_based_hrtime();
    cmd_time_stats_.client_response_write_time_ += c.cost_time_;
    client_entry_->in_tunnel_ = false;

    switch (event) {
      case VC_EVENT_EOS:
        client_entry_->eos_ = true;
        // fall through
      case VC_EVENT_INACTIVITY_TIMEOUT:
      case VC_EVENT_ACTIVE_TIMEOUT:
      case VC_EVENT_ERROR:
        LOG_WARN("ObMysqlSM::tunnel_handler_client", "event",
                 ObMysqlDebugNames::get_event_name(event), K_(sm_id));
        // The client died or aborted. Check to see
        // if we should setup a background fill
        set_client_abort(ObMysqlTransact::ABORTED, event);

        p = c.producer_;
        tunnel_.chain_abort_all(*c.producer_);
        selfc = p->self_consumer_;
        if (NULL != selfc) {
          // This is the case where there is a transformation between client and observer
          p = selfc->producer_;
          // if producer is the observer, close the producer. Otherwise in case of
          // large response, producer iobuffer gets filled up, waiting for a consumer
          // to consume data and the connection is never closed.
          if (p->alive_ && (MT_MYSQL_SERVER == p->vc_type_)) {
            tunnel_.chain_abort_all(*p);
          }
        }
        // In transaction, client session is error, close both server
        // session and client session
        close_connection = true;
        break;

      case VC_EVENT_WRITE_COMPLETE: {
        c.write_success_ = true;
        trans_state_.client_info_.abort_ = ObMysqlTransact::DIDNOT_ABORT;
        close_connection = false;

        ObRespAnalyzeResult &result = trans_state_.trans_info_.server_response_.get_analyze_result();
        if (result.is_error_resp()) {
          // filtering some error code, to disconnect client session
          if (result.is_net_packet_too_large_error()) {
            LOG_WARN("according to the errcode, client session should disconnect", K_(sm_id), K(result));
            close_connection = true;
            set_client_abort(ObMysqlTransact::ABORTED, event);
          }
        }
        break;
      }
      case VC_EVENT_WRITE_READY:
      case VC_EVENT_READ_READY:
      case VC_EVENT_READ_COMPLETE:
      default:
        // None of these events should ever come our way
        ret = OB_INNER_STAT_ERROR;
        LOG_ERROR("Unexpected event", K(event), K_(sm_id), K(ret));
        break;
    }
  }

  if (OB_SUCC(ret)) {
    cmd_size_stats_.client_response_bytes_ = c.bytes_written_;
    if (ObMysqlTransact::SOURCE_OBSERVER == trans_state_.source_) {
      cmd_size_stats_.server_response_bytes_ = cmd_size_stats_.client_response_bytes_;
    }

    if (close_connection) {
      trans_state_.current_.state_ = ObMysqlTransact::INACTIVE_TIMEOUT;
    } else if (client_session_->vc_ready_killed_) {
      //receiving VC_EVENT_WRITE_COMPLETE event means that the packet had sent successfully,
      //and we can do close session here if kill self
      client_session_->vc_ready_killed_ = false;
      clear_client_entry();
    } else if (ObMysqlTransact::TRANSACTION_COMPLETE == trans_state_.current_.state_) {
      // transaction complete, release client session
      client_session_->handle_transaction_complete(client_buffer_reader_, close_connection);
      if (close_connection) {
        trans_state_.current_.state_ = ObMysqlTransact::INACTIVE_TIMEOUT;
      }
    }
  }

  if (OB_FAIL(ret)) {
    trans_state_.inner_errcode_ = ret;
    trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
  }

  return ret;
}

int ObMysqlSM::tunnel_handler_request_transfer_client(int event, ObMysqlTunnelProducer &p)
{
  int ret = OB_SUCCESS;
  STATE_ENTER(ObMysqlSM::tunnel_handler_request_transfer_client, event);
  cmd_size_stats_.client_request_bytes_ += p.bytes_read_;
  milestones_.client_.client_read_end_ = get_based_hrtime();
  cmd_time_stats_.client_request_read_time_ += p.cost_time_;

  switch (event) {
    case VC_EVENT_EOS:
      // My reading of spec says that user clients can not terminate
      // request transfer with a half close so this is an error
      client_entry_->eos_ = true;
    case VC_EVENT_ERROR:
    case VC_EVENT_INACTIVITY_TIMEOUT:
    case VC_EVENT_ACTIVE_TIMEOUT:
      LOG_WARN("ObMysqlSM::tunnel_handler_request_transfer_client", "event",
               ObMysqlDebugNames::get_event_name(event), K_(sm_id));
      // Did not complete request transfer tunneling. Abort the
      // server and close the client
      trans_state_.current_.state_ = ObMysqlTransact::ACTIVE_TIMEOUT;
      p.handler_state_ = MYSQL_SM_REQUEST_TRANSFER_CLIENT_FAIL;
      set_client_abort(ObMysqlTransact::ABORTED, event);
      tunnel_.chain_abort_all(p);
      p.read_vio_ = NULL;
      if (p.vc_ != client_entry_->vc_) {
        p.vc_->do_io_close(EMYSQL_ERROR);
      }

      // the in_tunnel status on both the client & and
      // it's consumer must already be set to true. Previously we
      // were setting it again to true but incorrectly in the
    // case of a transform
      if (!client_entry_->in_tunnel_) {
        ret = OB_INNER_STAT_ERROR;
        LOG_ERROR("client entry must be in tunnel",
                  K_(client_entry_->in_tunnel), K_(sm_id), K(ret));
        dump_history_state();
      } else {
        client_entry_->in_tunnel_ = false;
        if (MT_TRANSFORM == p.consumer_list_.head_->vc_type_) {
          if (!api_.request_transform_info_.entry_->in_tunnel_) {
            ret = OB_INNER_STAT_ERROR;
            LOG_ERROR("request transform entry must be in tunnel",
                      K_(api_.request_transform_info_.entry_->in_tunnel), K_(sm_id), K(ret));
            dump_history_state();
          }
        } else if (NULL != server_entry_) {
          if (!server_entry_->in_tunnel_) {
            ret = OB_INNER_STAT_ERROR;
            LOG_ERROR("server entry must be in tunnel",
                      K_(server_entry_->in_tunnel), K_(sm_id), K(ret));
            dump_history_state();
          }
        }
      }
      break;

    case VC_EVENT_READ_COMPLETE:
    case MYSQL_TUNNEL_EVENT_PRECOMPLETE:
      // Completed successfully
      p.handler_state_ = MYSQL_SM_REQUEST_TRANSFER_SUCCESS;
      p.read_success_ = true;
      client_entry_->in_tunnel_ = false;

      tunnel_.local_finish_all(p);
      // Initiate another read to watch catch aborts and timeouts
      set_client_wait_timeout();
      client_entry_->vc_handler_ = &ObMysqlSM::state_watch_for_client_abort;
      client_entry_->read_vio_ = p.vc_->do_io_read(this, INT64_MAX, client_buffer_reader_->mbuf_);
      if (OB_ISNULL(client_entry_->read_vio_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("client entry failed to do_io_read", K_(sm_id), K(ret));
      }
      break;

    default:
      ret = OB_INNER_STAT_ERROR;
      LOG_ERROR("Unexpected event", K(event), K_(sm_id), K(ret));
      break;
  }

  if (OB_FAIL(ret)) {
    p.handler_state_ = MYSQL_SM_REQUEST_TRANSFER_CLIENT_FAIL;
  }

  return ret;
}

int ObMysqlSM::tunnel_handler_request_transfer_server(int event, ObMysqlTunnelConsumer &c)
{
  int ret = OB_SUCCESS;
  ObMysqlTunnelProducer *client_producer = NULL;

  STATE_ENTER(ObMysqlSM::tunnel_handler_request_transfer_server, event);
  cmd_size_stats_.server_request_bytes_ += c.bytes_written_;
  //server_request_write_time_ will be stated when WRITE_COMPLETE

  switch (event) {
    case VC_EVENT_EOS:
    case VC_EVENT_ERROR:
    case VC_EVENT_INACTIVITY_TIMEOUT:
    case VC_EVENT_ACTIVE_TIMEOUT:
    {
      LOG_WARN("ObMysqlSM::tunnel_handler_request_transfer_server", "event",
               ObMysqlDebugNames::get_event_name(event), K_(sm_id));
      // Did not complete request transfer tunneling
      //
      // In the mysql case, we don't want to close the connection because
      // the destroys the buffer which may a response even though the tunnel failed.

      // Shutdown both sides of the connection. This prevents us
      // from getting any further events and signals to client
      // that request data will not be forwarded to the server.
      // Doing shutdown on the write side will likely generate a
      // TCP reset to the client but if the proxy wasn't here this
      // is exactly what would happen.
      // we should wait to shutdown read side of the
      // client to prevent sending a reset
      server_entry_->eos_ = true;
      c.vc_->do_io_shutdown(IO_SHUTDOWN_WRITE);

      // We may be reading from a transform. In that case, we
      // want to close the transform
      if (MT_TRANSFORM == c.producer_->vc_type_) {
        if (MYSQL_SM_TRANSFORM_OPEN == c.producer_->handler_state_) {
          if (c.producer_->vc_ != api_.request_transform_info_.vc_) {
            ret = OB_INNER_STAT_ERROR;
            LOG_ERROR("invalid internal state, producer vc must be the same as request transform vc",
                      K_(c.producer_->vc), K_(api_.request_transform_info_.vc), K_(sm_id), K(ret));
          }
          c.producer_->vc_->do_io_close();
          c.producer_->alive_ = false;
          c.producer_->self_consumer_->alive_ = false;
        }
        client_producer = c.producer_->self_consumer_->producer_;
      } else {
        client_producer = c.producer_;
      }

      if (OB_SUCC(ret)) {
        if (OB_ISNULL(client_entry_) || OB_ISNULL(client_session_) || OB_ISNULL(client_producer)) {
          ret = OB_INNER_STAT_ERROR;
          LOG_ERROR("client entry and client session should not be NULL",
                    K_(client_entry), K_(client_session), K(client_producer), K_(sm_id), K(ret));
        } else if (OB_UNLIKELY(client_entry_->vc_ != client_producer->vc_)
                   || OB_UNLIKELY(MT_MYSQL_CLIENT != client_producer->vc_type_)
                   || client_producer->vc_ != client_session_) {
          ret = OB_INNER_STAT_ERROR;
          LOG_ERROR("invalid internal state, client entry vc, client producer vc and client session are different",
                    K_(client_entry_->vc), K_(client_producer->vc),
                    K_(client_producer->vc_type), K_(client_session), K_(sm_id), K(ret));
        } else {
          // Before shutting down, initiate another read on
          // the client in order to get timeouts
          // coming to the state machine and not the tunnel
          client_entry_->vc_handler_ = &ObMysqlSM::state_watch_for_client_abort;

          set_client_wait_timeout();
          client_entry_->read_vio_ = client_producer->vc_->do_io_read(this, INT64_MAX, c.producer_->read_buffer_);
          // we should not shutdown read side of the client here to prevent sending a reset
          //client_producer->vc->do_io_shutdown(IO_SHUTDOWN_READ);
          if (OB_ISNULL(client_entry_->read_vio_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("client session failed to do_io_read", K_(sm_id), K(ret));
          } else {
            // We want to shutdown the tunnel here and see if there
            // is a response on from the server. Mark the client
            // as down so that tunnel concludes.
            client_producer->alive_ = false;
            client_producer->handler_state_ = MYSQL_SM_REQUEST_TRANSFER_SERVER_FAIL;
            if (tunnel_.is_tunnel_alive()) {
              ret = OB_INNER_STAT_ERROR;
              LOG_ERROR("invalid internal state, tunnel should not be alive",
                        "tunne_alive", tunnel_.is_tunnel_alive(), K_(sm_id), K(ret));
            }
          }
        }
      }

      // we should disconnect in setup_error_transfer,
      // so we need consume all data in internal_reader.
      // Attention, here the data in internal_reader is the reqeust,
      // which will send to observer.
      if (NULL != trans_state_.internal_reader_) {
        trans_state_.internal_reader_->consume_all();
      }
      break;
    }

    case VC_EVENT_WRITE_COMPLETE:
      // Completed successfully
      c.write_success_ = true;
      break;

    default:
      ret = OB_INNER_STAT_ERROR;
      LOG_ERROR("Unexpected event", K(event), K_(sm_id), K(ret));
      break;
  }

  if (OB_FAIL(ret)) {
    if (NULL != client_producer) {
      // terminate sm
      client_producer->handler_state_ = MYSQL_SM_REQUEST_TRANSFER_CLIENT_FAIL;
    }
  }

  return ret;
}

int ObMysqlSM::tunnel_handler_transform_write(int event, ObMysqlTunnelConsumer &c)
{
  return api_.tunnel_handler_transform_write(event, c);
}

int ObMysqlSM::tunnel_handler_transform_read(int event, ObMysqlTunnelProducer &p)
{
  return api_.tunnel_handler_transform_read(event, p);
}

int ObMysqlSM::tunnel_handler_plugin_client(int event, ObMysqlTunnelConsumer &c)
{
  return api_.tunnel_handler_plugin_client(event, c);
}

void ObMysqlSM::do_congestion_control_lookup()
{
  milestones_.congestion_control_begin_ = get_based_hrtime();
  milestones_.congestion_control_end_ = 0;

  bool enable_congestion = trans_state_.mysql_config_params_->enable_congestion_;
  bool is_cur_congestion_avail = sm_cluster_resource_->is_congestion_avail();

  // we will lookup congestion, only the follow all happened
  // 1. enable_congestion from config
  // 2. cur congestion is avail(base servers has been added)
  // 3. need pl lookup
  // 4. not mysql route mode
  //
  // Attention! when force_retry_congested_, also need do congestion lookup
  if (enable_congestion
      && is_cur_congestion_avail
      && trans_state_.need_pl_lookup_
      && !trans_state_.mysql_config_params_->is_mysql_routing_mode()) {
    trans_state_.need_congestion_lookup_ = true;
  } else {
    LOG_DEBUG("no need do congestion lookup", K_(sm_id), K(enable_congestion),
              K(is_cur_congestion_avail), "need_pl_lookup", trans_state_.need_pl_lookup_,
              "force_retry_congestion", trans_state_.force_retry_congested_,
              "route_mode", trans_state_.mysql_config_params_->server_routing_mode_);
    trans_state_.need_congestion_lookup_ = false;
  }

  if (!trans_state_.need_congestion_lookup_) { // no need congestion lookup
    // call ObMysqlTransact::handle_congestion_control_lookup() to handle fail / success
    call_transact_and_set_next_state(NULL);
  } else {
    MYSQL_SM_SET_DEFAULT_HANDLER(&ObMysqlSM::state_congestion_control_lookup);
    int ret = OB_SUCCESS;
    if (NULL != pending_action_) {
      ret = OB_INNER_STAT_ERROR;
      LOG_ERROR("do_congestion_control_lookup, pending_action_ should be NULL",
                K_(pending_action), K_(trans_state_.congestion_entry), K_(sm_id));
    } else {
      if (NULL != trans_state_.congestion_entry_) {
        LOG_WARN("congestion entry must be NULL here",
                 KPC_(trans_state_.congestion_entry), K_(sm_id));
        trans_state_.congestion_entry_->dec_ref();
        trans_state_.congestion_entry_ = NULL;
      }
      trans_state_.is_congestion_entry_updated_ = false;

      ObAction *congestion_control_action_handle = NULL;
      ObCongestionManager &congestion_manager = sm_cluster_resource_->congestion_manager_;
      int64_t cr_version = sm_cluster_resource_->version_;
      ret = congestion_manager.get_congest_entry(this, trans_state_.server_info_.addr_, cr_version,
          &trans_state_.congestion_entry_, congestion_control_action_handle);
      if (OB_SUCC(ret)) {
        if (NULL != congestion_control_action_handle) {
          pending_action_ = congestion_control_action_handle;
        } else {
          trans_state_.congestion_lookup_success_ = true;
          // call ObMysqlTransact::handle_congestion_control_lookup() to handle fail / success
          call_transact_and_set_next_state(NULL);
        }
      } else {
        LOG_WARN("failed to get congest entry", K_(sm_id), K(ret));
      }
    }

    if (OB_FAIL(ret)) {
      trans_state_.inner_errcode_ = ret;
      trans_state_.congestion_lookup_success_ = false;
      // call ObMysqlTransact::handle_congestion_control_lookup() to handle fail / success
      call_transact_and_set_next_state(NULL);
    }
  }
}

void ObMysqlSM::do_partition_location_lookup()
{
  milestones_.pl_lookup_begin_ = get_based_hrtime();
  milestones_.pl_lookup_end_ = 0;

  if (trans_state_.api_server_addr_set_) {
    // If the API has set the server address before the partition
    // location lookup then we can skip the lookup
    LOG_DEBUG("[ObMysqlSM::do_partition_location_lookup] Skipping partition location "
              "lookup for API supplied target",
             K_(sm_id), K_(trans_state_.server_info_.addr));
    trans_state_.pll_info_.lookup_success_ = true;
    // call ObMysqlTransact::handle_pl_lookup() to handle fail / success
    call_transact_and_set_next_state(NULL);

  } else if (trans_state_.pll_info_.lookup_success_) {
    LOG_DEBUG("[ObMysqlSM::do_partition_location_lookup] Skipping partition "
              "location lookup, provided by plugin", K_(sm_id));
    // call ObMysqlTransact::handle_pl_lookup() to handle fail / success
    call_transact_and_set_next_state(NULL);

  } else {
    MYSQL_SM_SET_DEFAULT_HANDLER(&ObMysqlSM::state_partition_location_lookup);
    if (OB_UNLIKELY(NULL != pending_action_)) {
      trans_state_.pll_info_.lookup_success_ = false;
      LOG_ERROR("do_partition_location_lookup, pending_action_ should be NULL",
                K_(pending_action), K_(sm_id));
      // call ObMysqlTransact::handle_pl_lookup() to handle fail / success
      call_transact_and_set_next_state(NULL);

    } else {
      const ObTableEntryName &name = trans_state_.pll_info_.te_name_;

      ObRouteParam param;
      param.cont_ = this;
      param.force_renew_ = trans_state_.pll_info_.is_force_renew();
      param.is_need_force_flush_ = trans_state_.pll_info_.is_need_force_flush();
      param.use_lower_case_name_ = client_session_->get_session_info().need_use_lower_case_names();
      param.mysql_proxy_ = &sm_cluster_resource_->mysql_proxy_;
      param.cr_version_ = sm_cluster_resource_->version_;
      if (get_global_resource_pool_processor().get_default_cluster_resource() == sm_cluster_resource_) {
        // default cluster resource cluster id is always 0, and it is used only for building cluster resource
        param.cr_id_ = client_session_->get_session_info().get_cluster_id();
      } else {
        param.cr_id_ = sm_cluster_resource_->get_cluster_id();
      }
      param.tenant_version_ = sm_cluster_resource_->get_location_tenant_version(
          client_session_->get_session_info().get_priv_info().tenant_name_);
      param.timeout_us_ = hrtime_to_usec(trans_state_.mysql_config_params_->short_async_task_timeout_);
      param.is_partition_table_route_supported_ = is_partition_table_route_supported();
      param.is_oracle_mode_ = client_session_->get_session_info().is_oracle_mode();
      param.client_request_ = &trans_state_.trans_info_.client_request_; // priv parse result
      param.client_info_ = &client_session_->get_session_info();
      param.need_pl_route_ = (is_pl_route_supported() && (param.client_request_->get_parse_result().is_call_stmt()
            || param.client_request_->get_parse_result().is_text_ps_call_stmt()));
      param.current_idc_name_ = client_session_->get_current_idc_name();//shallow copy
      if (trans_state_.pll_info_.is_cached_dummy_force_renew()) {
        param.need_pl_route_ = false;
        param.name_.shallow_copy(name.cluster_name_, name.tenant_name_,
                                 ObString::make_string(OB_SYS_DATABASE_NAME),
                                 ObString::make_string(OB_ALL_DUMMY_TNAME));
      } else {
        param.name_.shallow_copy(name);
      }

      LOG_DEBUG("Doing partition location Lookup", K_(sm_id), K(param));
      ObAction *pl_lookup_action_handle = NULL;
      int ret = ObMysqlRoute::get_route_entry(param, sm_cluster_resource_, pl_lookup_action_handle);

      if (OB_SUCC(ret)) {
        if (NULL == pl_lookup_action_handle) {
          // cache hit and has called back, do nothing
        } else {
          pending_action_ = pl_lookup_action_handle;
        }
      } else {
        trans_state_.pll_info_.lookup_success_ = false;
        LOG_WARN("failed to get table entry", K_(sm_id));
        // call ObMysqlTransact::handle_pl_lookup() to handle fail / success
        call_transact_and_set_next_state(NULL);
      }
    }
  }
}

void ObMysqlSM::do_server_addr_lookup()
{
  ObProxyKillQueryInfo *query_info = trans_state_.trans_info_.client_request_.query_info_;
  if (OB_ISNULL(query_info)) {
    LOG_WARN("[ObMysqlSM::do_server_addr_lookup] query_info should not be null", K_(sm_id));
    // call ObMysqlTransact::handle_server_addr_lookup() to handle fail / success
    call_transact_and_set_next_state(NULL);

  } else if (!trans_state_.need_pl_lookup_) {
    // need use last server session
    query_info->errcode_ = OB_NOT_SUPPORTED;
    LOG_WARN("[ObMysqlSM::do_server_addr_lookup] As we need use last server session, "
            "we can not handle kill query, error request",
            K_(sm_id), K(*query_info));
    // call ObMysqlTransact::handle_server_addr_lookup() to handle fail / success
    call_transact_and_set_next_state(NULL);

  } else {
    LOG_DEBUG("[ObMysqlSM::do_server_addr_lookup] Doing kill query request analyze",
                K_(sm_id), K(*query_info));
    MYSQL_SM_SET_DEFAULT_HANDLER(&ObMysqlSM::state_server_addr_lookup);
    ObProxySessionPrivInfo &priv_info = client_session_->get_session_info().get_priv_info();
    ObAction *addr_lookup_action = NULL;
    milestones_.pl_lookup_begin_ = get_based_hrtime();
    milestones_.pl_lookup_end_ = 0;

    int ret = ObServerAddrLookupHandler::lookup_server_addr(*this, priv_info, *query_info, addr_lookup_action);
    if (OB_SUCC(ret)) {
      if (NULL == addr_lookup_action) {
        handle_event(SERVER_ADDR_LOOKUP_EVENT_DONE, query_info);
      } else {
        pending_action_ = addr_lookup_action;
      }
    } else {
      LOG_WARN("failed to lookup server addr", K_(sm_id), K(*query_info), K(addr_lookup_action), K(ret));
      // call ObMysqlTransact::handle_server_addr_lookup() to handle fail / success
      call_transact_and_set_next_state(NULL);
    }
  }//end of trans_state_.need_pl_lookup_
}

int ObMysqlSM::state_congestion_control_lookup(int event, void *data)
{
  STATE_ENTER(ObMysqlSM::state_congestion_control_lookup, event);
  if (CONGESTION_EVENT_CONTROL_LOOKUP_DONE == event) {
    trans_state_.congestion_lookup_success_ = true;
    if (OB_ISNULL(data)) {
      LOG_WARN("congestion entry is not found", "addr", trans_state_.server_info_.addr_, K_(sm_id));
    } else {
      trans_state_.congestion_entry_ = reinterpret_cast<ObCongestionEntry *>(data);
    }
  } else {
    trans_state_.congestion_lookup_success_ = false;
    LOG_ERROR("unexpected event type, it should not happen", K(event), K(data),
              "addr", trans_state_.server_info_.addr_, K_(sm_id));
  }

  pending_action_ = NULL;
  // call ObMysqlTransact::handle_congestion_control_lookup() to handle fail / success
  call_transact_and_set_next_state(NULL);

  return EVENT_DONE;
}

int ObMysqlSM::do_observer_open()
{
  int ret = OB_SUCCESS;
  MYSQL_SM_SET_DEFAULT_HANDLER(&ObMysqlSM::state_observer_open);

  milestones_.do_observer_open_begin_ = get_based_hrtime();
  milestones_.do_observer_open_end_ = 0;

  // if sync all variables completed, send request directly through handle_observer_open
  if (trans_state_.send_reqeust_direct_) {
    if (OB_ISNULL(server_session_) || ObMysqlTransact::SERVER_SEND_REQUEST != trans_state_.current_.send_action_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected state", K(server_session_), K(trans_state_.current_.send_action_), K(ret));
    } else {
      handle_observer_open();
    }
  } else {
    // We need to close the previous attempt, except reroute
    if (trans_state_.is_rerouted_) {
      release_server_session();
    }

    if (NULL != server_entry_) {
      if (MYSQL_SERVER_VC != server_entry_->vc_type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invlid internal state, server entry vc type is unexpected",
                  K_(server_entry_->vc_type), K_(sm_id), K(ret));
      } else if (OB_FAIL(vc_table_.cleanup_entry(server_entry_))) {
        LOG_WARN("vc table failed to cleanup server entry",
                 K_(server_entry), K_(sm_id), K(ret));
      } else {
        server_entry_ = NULL;
        server_session_ = NULL;
      }
    } else {
      // Now that we have gotten the client request, we can cancel
      // the inactivity timeout associated with it. Note, however, that we
      // must not cancel the inactivity timeout if the message contains a
      // body (as indicated by the non-zero request_content_length field).
      // This indicates that a POST operation is taking place and that the
      // client is still sending data to the observer. The observer cannot
      // reply until the entire request is received. In light of this
      // dependency, obproxy must ensure that the client finishes sending its
      // request and for this reason, the inactivity timeout cannot be
      // cancelled.
      if (NULL != client_session_) {
        client_session_->get_netvc()->cancel_inactivity_timeout();
      }
    }

    if (OB_UNLIKELY(NULL != server_entry_)
        || OB_ISNULL(client_entry_)
        || OB_ISNULL(client_session_)
        || OB_UNLIKELY(NULL != pending_action_)
        || OB_UNLIKELY(0 == trans_state_.server_info_.get_port())) {
      ret = OB_INNER_STAT_ERROR;
      LOG_ERROR("invalide internal state, server entry and pending action should be NULL,"
                "client entry and client session should not be NULL",
                K_(server_entry), K_(client_entry), K_(client_session), K_(pending_action),
                "server_port", trans_state_.server_info_.get_port(),
                K_(sm_id), K(ret));
    } else {
      LOG_DEBUG("will open connection to",
                "addr", trans_state_.server_info_.addr_, K_(sm_id),
                "force_retry_congested", trans_state_.force_retry_congested_);

      // we check this version only when safe_weak_read is enable
      if (client_session_->get_session_info().is_oceanbase_server()
          && is_causal_order_read_enabled()
          && NULL != client_session_
          && WEAK == trans_state_.get_trans_consistency_level(client_session_->get_session_info())
          && trans_state_.trans_info_.client_request_.get_parse_result().is_select_stmt()) {
        ObMysqlTransact::check_safe_read_snapshot(trans_state_);
      }

      if (NULL != api_.plugin_tunnel_) {
        ObPluginVCCore *t = api_.plugin_tunnel_;

        api_.plugin_tunnel_ = NULL;
        ObAction *pvc_action_handle = NULL;
        ret = t->connect_re(this, pvc_action_handle);
        if (OB_FAIL(ret) || NULL != pvc_action_handle) {
          LOG_WARN("failed plugin vc to connect_re", K(pvc_action_handle), K_(sm_id), K(ret));
        }
      } else if (MYSQL_NO_PLUGIN_TUNNEL != api_.plugin_tunnel_type_) {
        ret = OB_INNER_STAT_ERROR;
        LOG_ERROR("invalid tunnel type, should be MYSQL_NO_PLUGIN_TUNNEL",
                  K_(api_.plugin_tunnel_type), K_(sm_id), K(ret));
      } else if (OB_FAIL(do_internal_observer_open())) {
        LOG_WARN("failed to do_internal_observer_open", K_(sm_id), K(ret));
      }
    }
  }

  if (OB_SESSION_POOL_FULL_ERROR == ret) {
    ret = handle_retry_acquire_svr_session();
  }

  if (OB_FAIL(ret)) {
    trans_state_.inner_errcode_ = ret;
    trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
    call_transact_and_set_next_state(ObMysqlTransact::handle_response);
  }
  return ret;
}

inline int ObMysqlSM::do_internal_observer_open_event(int event, void* data)
{
  int ret = OB_SUCCESS;
  UNUSED(event);
  UNUSED(data);

  pending_action_ = NULL;
  MYSQL_SM_SET_DEFAULT_HANDLER(&ObMysqlSM::state_observer_open);

  if (OB_SUCC(do_internal_observer_open())) {
    retry_acquire_server_session_count_ = 0;
    start_acquire_server_session_time_ = 0;
  } else if (OB_SESSION_POOL_FULL_ERROR == ret) {
    ++retry_acquire_server_session_count_;
    ret = handle_retry_acquire_svr_session();
  }

  if (OB_FAIL(ret)) {
    trans_state_.inner_errcode_ = ret;
    trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
    call_transact_and_set_next_state(ObMysqlTransact::handle_response);
  }
  return ret;
}

inline int ObMysqlSM::handle_retry_acquire_svr_session() {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(client_session_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid client_session");
  } else {
    int64_t now_time = event::get_hrtime();
    if (start_acquire_server_session_time_ == 0) {
      start_acquire_server_session_time_ = now_time;
    }
    int64_t interval = HRTIME_USECONDS(get_global_proxy_config().session_pool_retry_interval);
    if (interval <= 0) {
      interval = HRTIME_MSECONDS(1); //should not be less zero
    }
    int64_t blocking_timeout_ms = ObMysqlSessionUtils::get_session_blocking_timeout_ms(client_session_->schema_key_);
    int64_t diff_time = now_time - start_acquire_server_session_time_;
    if (diff_time < blocking_timeout_ms) {
      MYSQL_SM_SET_DEFAULT_HANDLER(&ObMysqlSM::do_internal_observer_open_event);
      if (OB_ISNULL(pending_action_ = self_ethread().schedule_in(this, interval, CLIENT_SESSION_ACQUIRE_SERVER_SESSION_EVENT))) {
        LOG_WARN("fail to handle_retry_acquire_svr_session ", K(this), K(interval));
        ret = OB_ERR_UNEXPECTED;
      } else {
        LOG_DEBUG("succ to retry acquire svr session", K(diff_time), K(interval));
      }
    } else {
      ret = OB_SESSION_POOL_FULL_ERROR;
      trans_state_.mysql_errcode_ = OB_SESSION_POOL_FULL_ERROR;
      trans_state_.mysql_errmsg_ = "No empty server session, acquire session failed";
      if (OB_FAIL(ObMysqlTransact::build_error_packet(trans_state_))) {
        LOG_WARN("fail to encode err pacekt buf",
                 K_(sm_id), "errcode", trans_state_.mysql_errcode_,
                 "user_err_msg", trans_state_.mysql_errmsg_, K(ret));
      } else {
        LOG_WARN("fail to acquire svr session after retry", K(diff_time), K(retry_acquire_server_session_count_));
        retry_acquire_server_session_count_ = 0;
        start_acquire_server_session_time_ = 0;
        if (OB_FAIL(client_buffer_reader_->consume_all())) {
          LOG_WARN("fail to consume all", K_(sm_id), K(ret));
        }
        call_transact_and_set_next_state(ObMysqlTransact::handle_error_jump);
      }
    }
  }
  if (OB_FAIL(ret)) {
    trans_state_.inner_errcode_ = ret;
    trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
    call_transact_and_set_next_state(ObMysqlTransact::handle_response);
  }
  return ret;
}

inline int ObMysqlSM::use_set_pool_addr()
{
  int ret = OB_SUCCESS;
  ObCommonAddr& common_addr = client_session_->common_addr_;
  sockaddr sa;
  if (OB_FAIL(common_addr.get_sockaddr(sa))) {
    LOG_WARN("get_sockaddr failed", K(common_addr), K(ret));
  } else {
    trans_state_.server_info_.set_addr(sa);
    LOG_DEBUG("session pool client not use pool, will use set addr", K(client_session_->common_addr_),
            K_(sm_id));
  }
  return ret;
}

inline int ObMysqlSM::do_oceanbase_internal_observer_open(ObMysqlServerSession *&selected_session)
{
  int ret = OB_SUCCESS;
  ObMysqlServerSession *last_session = NULL;

  last_session = client_session_->get_server_session();
  // if need_pl_lookup is false, we must use last server session
  // allow no last server session when OB_MYSQL_COM_STMT_CLOSE/OB_MYSQL_COM_STMT_FETCH and need_pl_lookup_ = false
  if (!trans_state_.need_pl_lookup_
      && ((OB_MYSQL_COM_STMT_CLOSE != trans_state_.trans_info_.sql_cmd_ && OB_MYSQL_COM_STMT_FETCH != trans_state_.trans_info_.sql_cmd_)
           || !client_session_->is_need_return_last_bound_ss())) {
    if (OB_ISNULL(last_session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("last server session is NULL, disconnect", K_(sm_id), K(ret));
    } else if (OB_UNLIKELY(last_session->server_ip_ != trans_state_.server_info_.addr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("last server session ip is unexpected",
                K_(last_session->server_ip), K_(trans_state_.server_info_.addr), K_(sm_id), K(ret));
    } else {
      LOG_DEBUG("use last server session", K_(sm_id));
      selected_session = last_session;
    }
  } else {
    /*
    if ((trans_state_.mysql_config_params_->is_mock_routing_mode() && !client_session_->is_proxy_mysql_client_)
        || trans_state_.mysql_config_params_->is_mysql_routing_mode()) {
      if (client_session_->test_server_addr_.is_valid()) {
        trans_state_.server_info_.addr_ = client_session_->test_server_addr_;
        LOG_DEBUG("use hacked server addr", K_(sm_id), "server addr", trans_state_.server_info_.addr_);
      } else if (OB_FAIL(trans_state_.mysql_config_params_->get_one_test_server_addr(
              client_session_->test_server_addr_))) {
        LOG_INFO("mysql mode, but test server addr in not set, open conn to observer", K(ret));
        ret = OB_SUCCESS;
      } else {
        trans_state_.server_info_.addr_ = client_session_->test_server_addr_;
        LOG_DEBUG("use hacked server addr", K_(sm_id), "server addr", trans_state_.server_info_.addr_);
      }
    }
    */
    LOG_DEBUG("sql cmd", K(trans_state_.trans_info_.sql_cmd_), K(client_session_->is_session_pool_client()));
    if (trans_state_.trans_info_.sql_cmd_ == OB_MYSQL_COM_LOGIN && client_session_->is_session_pool_client() && !client_session_->can_direct_ok()) {
      LOG_DEBUG("OB_MYSQL_COM_LOGIN here not use pool");
      // only proxy_mysql_client for session pool use pool_sever_addr
      // this logic is for pre connection create
      if (client_session_->is_proxy_mysql_client_) {
        ret = use_set_pool_addr();
      }
    } else {
      const bool need_close_last_ss = need_close_last_used_ss();
      ret = client_session_->acquire_svr_session(trans_state_.server_info_.addr_.sa_, need_close_last_ss, selected_session);
      if ((OB_SUCCESS == ret && NULL != selected_session)
          || (OB_SESSION_NOT_FOUND == ret && NULL == selected_session)) {
        ret = OB_SUCCESS;
      } else if (OB_SESSION_POOL_FULL_ERROR == ret) {
        // now last is release
        client_session_->attach_server_session(NULL);
        server_entry_ = NULL;
        LOG_WARN("server session pool is full", K_(sm_id), K(selected_session),
                 "server_ip", trans_state_.server_info_.addr_, K(ret));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to acquire server session", K_(sm_id), K(selected_session),
                 "server_ip", trans_state_.server_info_.addr_, K(ret));
      }
    }
  }

  return ret;
}

inline int ObMysqlSM::do_normal_internal_observer_open(ObMysqlServerSession *&selected_session)
{
  int ret = OB_SUCCESS;
  if (trans_state_.trans_info_.sql_cmd_ == OB_MYSQL_COM_LOGIN && client_session_->is_session_pool_client() && !client_session_->can_direct_ok()) {
    LOG_DEBUG("OB_MYSQL_COM_LOGIN here not use pool");
    // only proxy_mysql_client for session pool use pool_sever_addr
    // this logic is for pre connection create
    if (client_session_->is_proxy_mysql_client_) {
      ret = use_set_pool_addr();
    }
    ret = OB_SESSION_NOT_FOUND;
  } else {
    // mayby some connections have same addr in connection pool
    if (ObMysqlTransact::is_in_trans(trans_state_)) {
      ObMysqlServerSession *last_session = NULL;
      last_session = client_session_->get_server_session();
      if (OB_ISNULL(last_session)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("last server session is NULL, disconnect", K_(sm_id), K(ret));
      } else if (OB_UNLIKELY(last_session->server_ip_ != trans_state_.server_info_.addr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("last server session ip is unexpected",
                  K_(last_session->server_ip), K_(trans_state_.server_info_.addr), K_(sm_id), K(ret));
      } else {
        LOG_DEBUG("use last server session", K_(sm_id));
        selected_session = last_session;
      }
    } else {
      ret = client_session_->acquire_svr_session(trans_state_.server_info_.addr_.sa_, false, selected_session);
    }
  }
  if ((OB_SUCCESS == ret && NULL != selected_session)
      || (OB_SESSION_NOT_FOUND == ret && NULL == selected_session)) {
    ret = OB_SUCCESS;
  } else if (OB_SESSION_POOL_FULL_ERROR == ret) {
    LOG_INFO("session pool is full", K(ret));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to acquire server session", K_(sm_id), K(selected_session),
             "server_ip", trans_state_.server_info_.addr_, K(ret));
  }
  return ret;
}

inline int ObMysqlSM::do_internal_observer_open()
{
  int ret = OB_SUCCESS;
  ObMysqlServerSession *selected_session = NULL;

  if (client_session_->get_session_info().is_oceanbase_server()) {
    if (OB_FAIL(do_oceanbase_internal_observer_open(selected_session))) {
      LOG_WARN("failed to do oceanbase internal observer open", K(ret));
    }
  } else if (OB_FAIL(do_normal_internal_observer_open(selected_session))) {
    LOG_WARN("failed to do normal internal observer open", K(ret));
  }

  // if we should force use last_session , but last_session is NULL,
  // we will close client session, and the client_session_ in sm will be reset to NULL.
  // so, in this condition, here we must judge client_session_ is NULL or not;
  if (OB_SUCC(ret)) {
    // clear client session's bound_ss_
    client_session_->attach_server_session(NULL);
    if (NULL != selected_session) {
      selected_session->state_ = MSS_ACTIVE;
      selected_session->set_client_session(*client_session_);
      if (client_session_->can_direct_ok()) {
        // sharding user maybe skip real connect, set flag now
        proxy::ObMysqlAuthRequest& login_req = client_session_->get_session_info().get_login_req();
        ObMySQLCapabilityFlags& cap_flag = login_req.get_hsr_result().response_.get_capability_flags();
        cap_flag.cap_flags_.OB_CLIENT_SESSION_TRACK = 1;
      }

      if (OB_FAIL(attach_server_session(*selected_session))) {
        LOG_WARN("failed to attach_server_session", K_(sm_id), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (NULL != server_session_) {
      ObClientSessionInfo &client_info = client_session_->get_session_info();
      ObServerSessionInfo &server_info = server_session_->get_session_info();
      // ObSqlParseResult &sql_result = trans_state_.trans_info_.client_request_.get_parse_result();
      // must in this order:
      // (1) reset database;
      // (2) reset session vars;
      // (3) reset last_insert_id(only when sql statement contain last_insert_id() function);
      // (4) send begin (start transaction);
      // (5) send prepare for ps execute;
      // (6) send text ps prepare for text ps execute
      // (7) send request(including normal request, ps execute, first prepare);

      LOG_DEBUG("get server session from connection pool", K_(sm_id),
                "server addr", server_session_->server_ip_,
                K(server_session_->ss_id_),
                "client_session version", client_info.get_session_version(),
                "server version", server_info.get_session_var_version());
      if (OB_MYSQL_COM_STMT_CLOSE == trans_state_.trans_info_.client_request_.get_packet_meta().cmd_) {
        // no need sync var on OB_MYSQL_COM_STMT_CLOSE
        trans_state_.current_.send_action_ = ObMysqlTransact::SERVER_SEND_REQUEST;
      } else if (client_info.need_reset_database(server_info)) {
        trans_state_.current_.send_action_ = ObMysqlTransact::SERVER_SEND_USE_DATABASE;
      } else if (client_info.need_reset_session_vars(server_info)) {
        trans_state_.current_.send_action_ = ObMysqlTransact::SERVER_SEND_SESSION_VARS;
      } else if (client_info.need_reset_last_insert_id(server_info)) {
        // TODO: current version proxy parse can't judge last_insert_id exactly,
        // so we do not judge, whether sql_reuslt has_last_insert_id here
        trans_state_.current_.send_action_ = ObMysqlTransact::SERVER_SEND_LAST_INSERT_ID;
      } else if (trans_state_.is_hold_start_trans_) {
        trans_state_.current_.send_action_ = ObMysqlTransact::SERVER_SEND_START_TRANS;
      } else if (((OB_MYSQL_COM_STMT_EXECUTE == trans_state_.trans_info_.client_request_.get_packet_meta().cmd_)
                  || (OB_MYSQL_COM_STMT_SEND_PIECE_DATA == trans_state_.trans_info_.client_request_.get_packet_meta().cmd_))
                 && client_info.need_do_prepare(server_info)) {
        trans_state_.current_.send_action_ = ObMysqlTransact::SERVER_SEND_PREPARE;
      } else if (client_info.is_text_ps_execute() && client_info.need_do_text_ps_prepare(server_info)) {
        trans_state_.current_.send_action_ = ObMysqlTransact::SERVER_SEND_TEXT_PS_PREPARE;
      } else {
        trans_state_.current_.send_action_ = ObMysqlTransact::SERVER_SEND_REQUEST;
      }
      handle_observer_open();
    } else {
      if (OB_FAIL(connect_observer())) {
        LOG_WARN("failed to connection observer", K_(sm_id), K(ret));
      }
    }
  }
  return ret;
}

inline int ObMysqlSM::connect_observer()
{
  // We did not manage to get an existing session
  // and need to open a new connection
  int ret = OB_SUCCESS;
  ObAction *connect_action_handle = NULL;
  ObNetVCOptions opt;

  milestones_.server_connect_begin_ = get_based_hrtime();
  milestones_.server_connect_end_ = 0;

  opt.f_blocking_connect_ = false;
  opt.set_sock_param(static_cast<int32_t>(trans_state_.mysql_config_params_->sock_recv_buffer_size_out_),
                     static_cast<int32_t>(trans_state_.mysql_config_params_->sock_send_buffer_size_out_),
                     static_cast<uint32_t>(trans_state_.mysql_config_params_->sock_option_flag_out_),
                     static_cast<uint32_t>(trans_state_.mysql_config_params_->sock_packet_mark_out_),
                     static_cast<uint32_t>(trans_state_.mysql_config_params_->sock_packet_tos_out_));
  if (opt.sockopt_flags_ & ObNetVCOptions::SOCK_OPT_KEEP_ALIVE) {
    opt.set_keepalive_param(static_cast<int32_t>(trans_state_.mysql_config_params_->server_tcp_keepidle_),
                            static_cast<int32_t>(trans_state_.mysql_config_params_->server_tcp_keepintvl_),
                            static_cast<int32_t>(trans_state_.mysql_config_params_->server_tcp_keepcnt_),
                            static_cast<int32_t>(trans_state_.mysql_config_params_->server_tcp_user_timeout_));
  }
  opt.ip_family_ = trans_state_.server_info_.addr_.sa_.sa_family;
  opt.is_inner_connect_ = client_session_->is_proxy_mysql_client_;
  opt.ethread_ = client_session_->is_proxy_mysql_client_ ? this_ethread() : client_session_->get_create_thread();

  // Set the inactivity timeout to the connect timeout so that we
  // we fail this server if it doesn't start sending the response
  // convert to ns
  const int64_t connect_timeout = trans_state_.mysql_config_params_->short_async_task_timeout_;

  LOG_DEBUG("calling g_net_processor.connect", K_(sm_id), K(trans_state_.server_info_.addr_));
  ret = g_net_processor.connect(*this, trans_state_.server_info_.addr_.sa_,
                                connect_action_handle, connect_timeout, &opt);
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to connect observer", K_(sm_id), K(ret));
  } else if (OB_ISNULL(connect_action_handle)) {
    // connect fail, net module has called back, do nothing
  } else if (NULL != pending_action_) {
    if (OB_SUCCESS != connect_action_handle->cancel()) {
      LOG_WARN("failed to cancel connect observer pending action", K_(sm_id), K(connect_action_handle));
    }
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid internal state, pending_action_ is not NULL", K_(pending_action), K_(sm_id), K(ret));
  } else {
    pending_action_ = connect_action_handle;
  }
  return ret;
}

void ObMysqlSM::do_internal_request()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("[ObMysqlSM::do_internal_request] Doing internal request", K_(sm_id));
  MYSQL_INCREMENT_TRANS_STAT(CLIENT_INTERNAL_REQUESTS);
  ObMIOBuffer *buf = NULL;
  bool send_response_direct = true;

  int64_t total_len = client_buffer_reader_->read_avail();
  if (OB_UNLIKELY(OB_MYSQL_COM_STMT_CLOSE == trans_state_.trans_info_.sql_cmd_)
          && OB_LIKELY(total_len > trans_state_.trans_info_.client_request_.get_packet_meta().pkt_len_)) {
    total_len = trans_state_.trans_info_.client_request_.get_packet_meta().pkt_len_;
  }

  // consume data in client buffer reader
  if (OB_UNLIKELY(NULL != pending_action_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[ObMysqlSM::do_internal_request]", K_(sm_id), K_(pending_action));
  } else if (OB_FAIL(client_buffer_reader_->consume(total_len))) {
    LOG_WARN("fail to consume all", K_(sm_id), K(ret));
  } else if (OB_FAIL(trans_state_.alloc_internal_buffer(MYSQL_BUFFER_SIZE))) {
    LOG_ERROR("[ObMysqlSM::do_internal_request] fail to allocate internal buffer,",
              K_(sm_id), K(ret));
  } else {
    buf = trans_state_.internal_buffer_;
    switch (trans_state_.trans_info_.sql_cmd_) {
      case OB_MYSQL_COM_HANDSHAKE: {
        OMPKHandshake handshake;
        if (g_ssl_processor.is_client_ssl_supported() && !client_session_->is_proxy_mysql_client_) {
          handshake.enable_ssl();
        }
        // current not support compress to client
        handshake.disable_use_compress();
        handshake.enable_session_track();
        handshake.enable_connection_attr();
        // use cs id (proxy connection id)
        // before receive ok pkt from observer, the conn_id_ is always 0, means has not set
        uint32_t conn_id = client_session_->get_cs_id();
        handshake.set_thread_id(conn_id);
        if (strlen(obproxy::obutils::get_global_proxy_config().mysql_version.str())) {
          ObString server_version = ObString::make_string(obproxy::obutils::get_global_proxy_config().mysql_version.str());
          handshake.set_server_version(server_version);
        }

        if (trans_state_.mysql_config_params_->enable_proxy_scramble_
            && client_session_->get_scramble_string().empty()) {
          if (OB_FAIL(client_session_->create_scramble())) {
            LOG_WARN("fail to create_scramble", K_(sm_id), K(ret));
          } else {
            ObString &scramble = client_session_->get_scramble_string();
            if (OB_FAIL(handshake.set_scramble(scramble.ptr(), scramble.length()))) {
              LOG_WARN("fail to set_scramble", K_(sm_id), K(scramble), K(ret));
            }
          }
        }

        if (FAILEDx(ObMysqlPacketWriter::write_packet(*buf, handshake))) {
          LOG_WARN("fail to serialize handshake packet", K_(sm_id), K(ret));
        } else {
          ObMySQLCapabilityFlags capability(handshake.get_server_capability());
          client_session_->get_session_info().save_orig_capability_flags(capability);
        }
        break;
      }
      case OB_MYSQL_COM_LOGIN: {
        if (!client_session_->is_proxysys_tenant() && !client_session_->can_direct_ok()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[ObMysqlSM::do_internal_request] it should never enter here",
                   K_(sm_id), "cs_id", client_session_->get_cs_id());
        } else {
          LOG_DEBUG("proxysys response login ok packet",
                   K_(sm_id), "cs_id", client_session_->get_cs_id());

          ObMysqlAuthRequest &orig_auth_req = client_session_->get_session_info().get_login_req();
          const ObMySQLCapabilityFlags &capability = client_session_->get_session_info().get_orig_capability_flags();
          uint8_t pkt_seq = static_cast<uint8_t>(orig_auth_req.get_packet_meta().pkt_seq_ + 1);

          if (OB_FAIL(ObMysqlPacketUtil::encode_ok_packet(*buf, pkt_seq, 0, capability))) {
            LOG_WARN("[ObMysqlSM::do_internal_request] fail to encode login response ok packet",
                     K_(sm_id), K(ret));
          }
        }
        break;
      }
      case OB_MYSQL_COM_PING: {
        LOG_DEBUG("proxy response OB_MYSQL_COM_PING ok packet", K_(sm_id), "cs_id", client_session_->get_cs_id());
        if (OB_LIKELY(get_global_hot_upgrade_info().need_conn_accept_)) {
          uint8_t pkt_seq = static_cast<uint8_t>(trans_state_.trans_info_.client_request_.get_packet_meta().pkt_seq_ + 1);
          const ObMySQLCapabilityFlags &capability = client_session_->get_session_info().get_orig_capability_flags();
          if (OB_FAIL(ObMysqlPacketUtil::encode_ok_packet(*buf, pkt_seq, 0, capability))) {
            LOG_WARN("[ObMysqlSM::do_internal_request] fail to encode OB_MYSQL_COM_PING response ok packet",
                     K_(sm_id), K(ret));
          }
        } else {
          ret = OB_SERVER_IS_STOPPING;
          LOG_INFO("proxy had been stop accepting new connection, "
                   "disconnect here for OB_MYSQL_COM_PING", K_(sm_id), "cs_id", client_session_->get_cs_id(), K(ret));
        }
        break;
      }
      case OB_MYSQL_COM_QUIT: {
        send_response_direct = false;
        LOG_DEBUG("proxy handle OB_MYSQL_COM_QUIT itself", K_(sm_id), "cs_id", client_session_->get_cs_id());
        // call ObMysqlTransact::handle_internal_request() to handle quit command, disconnect
        call_transact_and_set_next_state(NULL);
        break;
      }

      case OB_MYSQL_COM_STMT_CLOSE: {
        ObClientSessionInfo &client_info = client_session_->get_session_info();
        uint32_t client_ps_id = client_info.get_client_ps_id();
        // remove directly
        client_info.remove_ps_id_entry(client_ps_id);
        client_info.remove_ps_id_addrs(client_ps_id);
        send_response_direct = false;
        LOG_DEBUG("proxy no response OB_MYSQL_COM_STMT_CLOSE", K_(sm_id), "cs_id", client_session_->get_cs_id());
        callout_api_and_start_next_action(ObMysqlTransact::SM_ACTION_API_CMD_COMPLETE);
        break;
      }

      case OB_MYSQL_COM_QUERY:
      case OB_MYSQL_COM_SLEEP:
      case OB_MYSQL_COM_INIT_DB:
      case OB_MYSQL_COM_FIELD_LIST:
      case OB_MYSQL_COM_CREATE_DB:
      case OB_MYSQL_COM_DROP_DB:
      case OB_MYSQL_COM_REFRESH:
      case OB_MYSQL_COM_STATISTICS:
      case OB_MYSQL_COM_PROCESS_INFO:
      case OB_MYSQL_COM_CONNECT:
      case OB_MYSQL_COM_PROCESS_KILL:
      case OB_MYSQL_COM_DEBUG:
      case OB_MYSQL_COM_TIME:
      case OB_MYSQL_COM_DELAYED_INSERT:
      case OB_MYSQL_COM_CHANGE_USER:
      case OB_MYSQL_COM_SHUTDOWN:
      case OB_MYSQL_COM_DAEMON: {
        ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
        ObClientSessionInfo &client_info = client_session_->get_session_info();
        uint8_t next_seq = static_cast<uint8_t>(client_request.get_packet_meta().pkt_seq_ + 1);
        // 1. hold_start_trans
        if (client_request.get_parse_result().need_hold_start_trans()) {
          trans_state_.is_hold_start_trans_ = true;
          if (OB_FAIL(ObMysqlResponseBuilder::build_start_trans_resp(*buf, client_request, client_info))) {
            LOG_WARN("[ObMysqlSM::do_internal_request] fail to build start trans resp", K_(sm_id), K(ret));
          }

        // 2. bad_route_request
        } else if (ObMysqlTransact::is_bad_route_request(trans_state_)) {
          trans_state_.mysql_errcode_ = OB_NOT_SUPPORTED;
          trans_state_.mysql_errmsg_ = "Not supported, bad route request";
          if (OB_FAIL(ObMysqlTransact::build_error_packet(trans_state_))) {
            LOG_WARN("[ObMysqlSM::do_internal_request] fail to encode err pacekt buf",
                     K_(sm_id), K(next_seq), "errcode", trans_state_.mysql_errcode_,
                     "user_err_msg", trans_state_.mysql_errmsg_, K(ret));
          } else {
            // throw away the former 'begin' or 'start transaction'
            trans_state_.is_hold_start_trans_ = false;
          }

         // 3. not_supported
        } else if (client_request.get_parse_result().is_not_supported()) {
          trans_state_.mysql_errcode_ = OB_NOT_SUPPORTED;
          if (OB_FAIL(ObMysqlTransact::build_error_packet(trans_state_))) {
            LOG_WARN("[ObMysqlSM::do_internal_request] fail to build not supported err resp", K_(sm_id), K(ret));
          }

        // 4. select_tx_ro
        } else if (client_request.get_parse_result().is_select_tx_ro()) {
          // if start trans is hold, we treat this resp is in trans
          bool is_in_trans = (trans_state_.is_hold_start_trans_ || ObMysqlTransact::is_in_trans(trans_state_));
          if (OB_FAIL(ObMysqlResponseBuilder::build_select_tx_ro_resp(
                  *buf, client_request, client_info, is_in_trans))) {
            LOG_WARN("[ObMysqlSM::do_internal_request] fail to build select @@tx_read_only", K_(sm_id), K(ret));
          }

        // 5. set_autocommit_0
        } else if (client_request.get_parse_result().is_set_autocommit_0()) {
          // if start trans is hold, we treat this resp is in trans
          bool is_in_trans = (trans_state_.is_hold_start_trans_ || ObMysqlTransact::is_in_trans(trans_state_));
          ObObj value;
          value.set_int(0);
          client_info.set_need_sync_session_vars(true);
          if (client_info.update_common_sys_variable("autocommit", value, true, false)) {
            LOG_WARN("fail to update sys variable", K_(sm_id), K(ret));
          } else if (OB_FAIL(ObMysqlResponseBuilder::build_ok_resq_with_state_changed(
                  *buf, client_request, client_info, is_in_trans))) {
            LOG_WARN("[ObMysqlSM::do_internal_request] fail to build set autocommit = 0", K_(sm_id), K(ret));
          }

        // 6. ping_proxy
        } else if (client_request.get_parse_result().is_ping_proxy_cmd()
                   && !client_request.get_parse_result().is_internal_error_cmd()) {
          if (OB_LIKELY(get_global_hot_upgrade_info().need_conn_accept_)) {
            uint8_t pkt_seq = static_cast<uint8_t>(trans_state_.trans_info_.client_request_.get_packet_meta().pkt_seq_ + 1);
            const ObMySQLCapabilityFlags &capability = client_session_->get_session_info().get_orig_capability_flags();
            if (OB_FAIL(ObMysqlPacketUtil::encode_ok_packet(*buf, pkt_seq, 0, capability))) {
              LOG_WARN("[ObMysqlSM::do_internal_request] fail to encode 'ping proxy' response ok packet",
                       K_(sm_id), K(ret));
            }
          } else {
            ret = OB_SERVER_IS_STOPPING;
            LOG_INFO("proxy had been stop accepting new connection, "
                     "disconnect here for 'ping proxy' cmd", K_(sm_id), "cs_id", client_session_->get_cs_id(), K(ret));
          }

        // 7. error internal cmd
        } else if (OB_LIKELY(NULL != client_request.cmd_info_) && client_request.cmd_info_->is_error_cmd()) {
          if (client_request.cmd_info_->is_error_cmd_need_resp_ok()) {
            // python mysql will send 'SET autocommit=0' after connected, proxysys need resp ok packet
            uint8_t pkt_seq = static_cast<uint8_t>(trans_state_.trans_info_.client_request_.get_packet_meta().pkt_seq_ + 1);
            const ObMySQLCapabilityFlags &capability = client_session_->get_session_info().get_orig_capability_flags();
            if (OB_FAIL(ObMysqlPacketUtil::encode_ok_packet(*buf, pkt_seq, 0, capability))) {
              LOG_WARN("[ObMysqlSM::do_internal_request] fail to encode ok packet for internal cmd",
                       K_(sm_id), K(ret));
            }
          } else {
            trans_state_.mysql_errcode_ = OB_ERR_OPERATOR_UNKNOWN;
            trans_state_.mysql_errmsg_ = "Unknown operator, bad internal cmd";
            if (OB_FAIL(ObMysqlTransact::build_error_packet(trans_state_))) {
              LOG_WARN("[ObMysqlSM::do_internal_request] fail to build not err resp", K_(sm_id), K(ret));
            }
          }

        // 8. select route_addr
        } else if (client_request.get_parse_result().is_select_route_addr()) {
          ObMysqlServerSession *last_session = client_session_->get_server_session();
          int64_t addr = 0;
          if (NULL != last_session && NULL != last_session->get_netvc()) {
            addr = ObMysqlTransact::build_addr_from_ip_port(
                      ntohl(last_session->get_netvc()->get_remote_ip()),
                      last_session->get_netvc()->get_remote_port());
            // if start trans is hold, we treat this resp is in trans
            bool is_in_trans = (trans_state_.is_hold_start_trans_ || ObMysqlTransact::is_in_trans(trans_state_));
            if (OB_FAIL(ObMysqlResponseBuilder::build_select_route_addr_resp(
                    *buf, client_request, client_info, is_in_trans, addr))) {
              LOG_WARN("[ObMysqlSM::do_internal_request] fail to build select @obproxy_route_addr", K_(sm_id), K(ret));
            }
          } else {
            trans_state_.mysql_errcode_ = OB_SERVER_NOT_ACTIVE;
            trans_state_.mysql_errmsg_ = "last session is invalid";
            if (OB_FAIL(ObMysqlTransact::build_error_packet(trans_state_))) {
              LOG_WARN("[ObMysqlSM::do_internal_request] fail to build not err resp", K_(sm_id), K(ret));
            }
          }

        // 9. set route_addr
        } else if (client_request.get_parse_result().is_set_route_addr()) {
          client_info.set_obproxy_route_addr(client_request.get_parse_result().cmd_info_.integer_[0]);
          // if start trans is hold, we treat this resp is in trans
          bool is_in_trans = (trans_state_.is_hold_start_trans_ || ObMysqlTransact::is_in_trans(trans_state_));
          if (OB_FAIL(ObMysqlResponseBuilder::build_set_route_addr_resp(
                  *buf, client_request, client_info, is_in_trans))) {
            LOG_WARN("[ObMysqlSM::do_internal_request] fail to build set @obproxy_route_addr", K_(sm_id), K(ret));
          } else {
            LOG_DEBUG("@obproxy_route_addr is set", K(client_info.get_obproxy_route_addr()));
          }
        // 10. set ob_read_consistency
        } else if (client_request.get_parse_result().is_set_ob_read_consistency()) {
          if (client_session_->get_session_info().is_request_follower_user()) {
            trans_state_.mysql_errcode_ = OB_OP_NOT_ALLOW;
            trans_state_.mysql_errmsg_ = "set ob_read_consitency is not allowed";
            if (OB_FAIL(ObMysqlTransact::build_error_packet(trans_state_))) {
              LOG_WARN("[ObMysqlSM::do_internal_request] fail to encode err pacekt buf",
                     K_(sm_id), K(next_seq), "errcode", trans_state_.mysql_errcode_,
                     "user_err_msg", trans_state_.mysql_errmsg_, K(ret));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error", K(ret));
          }
        // 11. set tx_read_only
        } else if (client_request.get_parse_result().is_set_tx_read_only()) {
          if (client_session_->get_session_info().is_read_only_user()) {
            trans_state_.mysql_errcode_ = OB_OP_NOT_ALLOW;
            trans_state_.mysql_errmsg_ = "set ob_read_consitency is not allowed";
            if (OB_FAIL(ObMysqlTransact::build_error_packet(trans_state_))) {
              LOG_WARN("[ObMysqlSM::do_internal_request] fail to encode err pacekt buf",
                     K_(sm_id), K(next_seq), "errcode", trans_state_.mysql_errcode_,
                     "user_err_msg", trans_state_.mysql_errmsg_, K(ret));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error", K(ret));
          }

        // 12. handle some special stmt or cmd when sharding
        } else if (client_session_->get_session_info().is_sharding_user() &&
                   !trans_state_.trans_info_.client_request_.get_parse_result().is_dual_request()) {
          // handle use db stmt or OB_MYSQL_COM_INIT_DB cmd when sharding
          if (OB_MYSQL_COM_INIT_DB == trans_state_.trans_info_.sql_cmd_ || client_request.get_parse_result().is_use_db_stmt()) {
            LOG_DEBUG("init db");
            ObHSRResult &hsr = client_info.get_login_req().get_hsr_result();
            ObString db_name;
            if (client_request.get_parse_result().is_use_db_stmt()) {
              db_name = client_request.get_parse_result().get_database_name();
            } else {
              db_name = client_request.get_sql();
            }
            if (OB_FAIL(ObProxyShardUtils::handle_shard_use_db(trans_state_, *client_session_, db_name))) {
              if (OB_ENTRY_NOT_EXIST == ret) {
                // response unknown db
                trans_state_.mysql_errcode_ = OB_ERR_BAD_DATABASE;
                trans_state_.mysql_errmsg_ = ob_strerror(OB_ERR_BAD_DATABASE);
                if (OB_FAIL(ObMysqlTransact::build_error_packet(trans_state_))) {
                  LOG_WARN("[ObMysqlSM::do_internal_request] fail to build err resp", K_(sm_id), K(ret));
                }
              } else if (OB_ERR_NO_DB_PRIVILEGE == ret) {
                if (OB_FAIL(ObMysqlTransact::build_no_privilege_message(trans_state_, *client_session_, db_name))) {
                  LOG_WARN("fail to build_no_privilege_message", K(db_name), K(ret));
                }
              } else {
                LOG_WARN("handle shard use db failed", K(ret));
              }
            } else if (OB_FAIL(save_user_login_info(client_info, hsr))) {
              LOG_WARN("fail to save user login info", K_(sm_id), K(ret));
            } else {
              // if start trans is hold, we treat this resp is in trans
              bool is_in_trans = (trans_state_.is_hold_start_trans_ || ObMysqlTransact::is_in_trans(trans_state_));
              if (OB_FAIL(ObMysqlResponseBuilder::build_ok_resq_with_state_changed(
                          *buf, client_request, client_info, is_in_trans))) {
                LOG_WARN("[ObMysqlSM::do_internal_request] fail to build ok res", K_(sm_id), K(ret));
              }
            }

          } else if (client_request.get_parse_result().is_show_db_version_stmt()) {
            // handle show db version when sharding
            LOG_DEBUG("show db_version");
            ObString logic_tenant_name;
            ObString logic_database_name;
            if (OB_FAIL(client_info.get_logic_tenant_name(logic_tenant_name))) {
              LOG_WARN("fail to get_logic_tenant_name", K(ret));
            } else if (OB_FAIL(client_info.get_logic_database_name(logic_database_name))) {
              LOG_WARN("fail to get_logic_database_name", K(ret));
              trans_state_.mysql_errcode_ = OB_ERR_NO_DB_SELECTED;
              trans_state_.mysql_errmsg_ = ob_strerror(OB_ERR_NO_DB_SELECTED);
              if (OB_FAIL(ObMysqlTransact::build_error_packet(trans_state_))) {
                LOG_WARN("[ObMysqlSM::do_internal_request] fail to build not err resp", K_(sm_id), K(ret));
              }
            } else if (OB_FAIL(ObShowDBVersionHandler::show_db_version_cmd_callback(buf,
                               static_cast<uint8_t>(client_request.get_packet_meta().pkt_seq_ + 1),
                               trans_state_.mysql_config_params_->internal_cmd_mem_limited_,
                               logic_tenant_name, logic_database_name))) {
              LOG_WARN("[ObMysqlSM::do_internal_request] fail to handle show db version", K_(sm_id), K(ret));
            }
          } else if (client_request.get_parse_result().is_show_databases_stmt()) {
            // handle show databases when sharding
            LOG_DEBUG("show databases");
            ObString logic_tenant_name;
            if (OB_FAIL(client_info.get_logic_tenant_name(logic_tenant_name))) {
              LOG_WARN("fail to get_logic_tenant_name", K(ret));
            } else if (OB_FAIL(ObShowDatabasesHandler::show_databases_cmd_callback(buf,
                               static_cast<uint8_t>(client_request.get_packet_meta().pkt_seq_ + 1),
                               trans_state_.mysql_config_params_->internal_cmd_mem_limited_, logic_tenant_name))) {
              LOG_WARN("[ObMysqlSM::do_internal_request] fail to handle show databases", K_(sm_id), K(ret));
            }
          // handle show tables when sharding
          } else if (client_request.get_parse_result().is_show_tables_stmt()) {
            LOG_DEBUG("show tables");
            ObString logic_tenant_name;
            ObString logic_database_name;
            if (OB_FAIL(client_info.get_logic_tenant_name(logic_tenant_name))) {
              LOG_WARN("fail to get_logic_tenant_name", K(ret));
            } else if (OB_FAIL(client_info.get_logic_database_name(logic_database_name))) {
              LOG_WARN("fail to get_logic_database_name", K(ret));
              trans_state_.mysql_errcode_ = OB_ERR_NO_DB_SELECTED;
              trans_state_.mysql_errmsg_ = ob_strerror(OB_ERR_NO_DB_SELECTED);
              if (OB_FAIL(ObMysqlTransact::build_error_packet(trans_state_))) {
                LOG_WARN("[ObMysqlSM::do_internal_request] fail to build not err resp", K_(sm_id), K(ret));
              }
            } else if (OB_FAIL(ObShowTablesHandler::show_tables_cmd_callback(buf,
                        static_cast<uint8_t>(client_request.get_packet_meta().pkt_seq_ + 1),
                        trans_state_.mysql_config_params_->internal_cmd_mem_limited_,
                        logic_tenant_name, logic_database_name))) {
              LOG_WARN("[ObMysqlSM::do_internal_request] fail to handle show databases", K_(sm_id), K(ret));
            }
          } else if (client_request.get_parse_result().is_show_topology_stmt()) {
            // handle show topology when sharding
            ObInternalCmdInfo &cmd_info = *client_request.cmd_info_;
            ObString logic_db_name = cmd_info.get_like_string();
            ObString logic_tenant_name;
            ObString group_name = cmd_info.get_value_string();
            LOG_DEBUG("show topology", K(logic_db_name), K(group_name));
            if (logic_db_name.empty()) {
              if (OB_FAIL(client_info.get_logic_database_name(logic_db_name))) {
                LOG_WARN("fail to get_logic_db_name", K(ret));
              }
            }
            if (OB_FAIL(ret)) {
              // do nothing
            } if (OB_FAIL(client_info.get_logic_tenant_name(logic_tenant_name))) {
              LOG_WARN("fail to get_logic_tenant_name", K(ret));
            } else if (OB_FAIL(ObShowTopologyHandler::show_topology_cmd_callback(buf,
                               static_cast<uint8_t>(client_request.get_packet_meta().pkt_seq_ + 1),
                               trans_state_.mysql_config_params_->internal_cmd_mem_limited_,
                               logic_tenant_name, logic_db_name, group_name))) {
              LOG_WARN("[ObMysqlSM::do_internal_request] fail to handle show topology", K_(sm_id), K(ret));
            }
          } else if (client_request.get_parse_result().is_select_database_stmt()) {
            ObString logic_database_name;
            client_info.get_logic_database_name(logic_database_name);
            if (OB_FAIL(ObSelectDatabaseHandler::select_database_cmd_callback(buf,
                        static_cast<uint8_t>(client_request.get_packet_meta().pkt_seq_ + 1),
                        trans_state_.mysql_config_params_->internal_cmd_mem_limited_,
                        logic_database_name))) {
              LOG_WARN("[ObMysqlSM::do_internal_request] fail to handle select database()", K_(sm_id), K(ret));
            }
          }

        // 13. internal cmd
        } else {
          send_response_direct = false;
          if (OB_ISNULL(client_request.cmd_info_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("[ObMysqlSM::do_internal_request] cmd_info_ should not be null", K_(sm_id), K(ret));
          } else {
            MYSQL_SM_SET_DEFAULT_HANDLER(&ObMysqlSM::state_execute_internal_cmd);
            ObInternalCmdInfo &cmd_info = *client_request.cmd_info_;
            cmd_info.set_pkt_seq(static_cast<uint8_t>(client_request.get_packet_meta().pkt_seq_ + 1));
            cmd_info.set_capability(client_session_->get_session_info().get_orig_capability_flags());
            cmd_info.session_priv_ = &client_session_->get_session_info().get_priv_info();
            cmd_info.set_memory_limit(trans_state_.mysql_config_params_->internal_cmd_mem_limited_);
            cmd_info.set_internal_user(client_request.is_proxysys_user());
            ObAction *cmd_handler = NULL;
            if (OB_FAIL(get_global_internal_cmd_processor().execute_cmd(this, cmd_info, buf, cmd_handler))) {
              LOG_WARN("[ObMysqlSM::do_internal_request] fail to execute_cmd", K_(sm_id), K(ret));
            } else if (OB_ISNULL(cmd_handler)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("cmd_handler can not be NULL", K(cmd_handler), K(ret));
            } else {
              pending_action_ = cmd_handler;
              ObHRTime execute_timeout = 0;
              if (cmd_info.is_mysql_compatible_cmd()) {
                execute_timeout = client_session_->get_session_info().get_query_timeout();
              } else {
                execute_timeout = HRTIME_SECONDS(30);
              }
              set_internal_cmd_timeout(execute_timeout);
              LOG_DEBUG("assign pending_action", K_(sm_id), K_(pending_action), K(execute_timeout));
            }
          }
        }
        break;
      }
      default: {
        if (!is_supported_mysql_cmd(trans_state_.trans_info_.sql_cmd_)) {
          LOG_WARN("not supported mysql cmd", K_(sm_id), "cmd", get_mysql_cmd_str(trans_state_.trans_info_.sql_cmd_));
          trans_state_.mysql_errcode_ = OB_NOT_SUPPORTED;
          if (OB_FAIL(ObMysqlTransact::build_error_packet(trans_state_))) {
            LOG_WARN("[ObMysqlSM::do_internal_request] fail to build not supported err resp", K_(sm_id), K(ret));
          }
        } else {
          ret = OB_INNER_STAT_ERROR;
          LOG_ERROR("[ObMysqlSM::do_internal_request] it should not enter here ERROR", K_(sm_id),
                    "sql_cmd", ObProxyParserUtils::get_sql_cmd_name(trans_state_.trans_info_.sql_cmd_));
        }
        break;
      }
    }
  }

  if (OB_SUCC(ret) && send_response_direct) {
    trans_state_.next_action_ = ObMysqlTransact::SM_ACTION_INTERNAL_NOOP;
    callout_api_and_start_next_action(ObMysqlTransact::SM_ACTION_API_SEND_RESPONSE);
  }

  if (OB_FAIL(ret)) {
    trans_state_.inner_errcode_ = ret;
    // call ObMysqlTransact::handle_internal_request() to handle fail
    call_transact_and_set_next_state(NULL);
  }
}

inline void ObMysqlSM::set_client_abort(const ObMysqlTransact::ObAbortStateType client_abort, int event)
{
  trans_state_.client_info_.abort_ = client_abort;

  // Set the connection attribute code for the client so that
  // we log the client finish code correctly

  const char *xflush_head = NULL;
  switch (event) {
    case VC_EVENT_ACTIVE_TIMEOUT:
      trans_state_.client_info_.state_ = ObMysqlTransact::ACTIVE_TIMEOUT;
      xflush_head = XFH_CONNECTION_ACTIVE_TIMEOUT;
      break;

    case VC_EVENT_INACTIVITY_TIMEOUT:
      trans_state_.client_info_.state_ = ObMysqlTransact::INACTIVE_TIMEOUT;
      xflush_head = XFH_CONNECTION_INACTIVE_TIMEOUT;
      break;

    case VC_EVENT_ERROR:
      trans_state_.client_info_.state_ = ObMysqlTransact::CONNECTION_ERROR;
      xflush_head = XFH_CONNECTION_ERROR;
      break;

    case VC_EVENT_EOS:
      trans_state_.client_info_.state_ = ObMysqlTransact::CONNECTION_CLOSED;
      xflush_head = XFH_CONNECTION_CLOSED;
      break;

    default:
      xflush_head = XFH_CONNECTION_CLIENT_ABORT;
      break;
  }

  uint64_t proxy_sessid = 0;
  uint32_t cs_id = 0;
  uint32_t server_sessid = 0;
  int64_t ss_id = 0;
  if (NULL != client_session_) {
    proxy_sessid = client_session_->get_proxy_sessid();
    cs_id = client_session_->get_cs_id();
  }
  if (NULL != server_session_) {
    server_sessid = server_session_->get_server_sessid();
    ss_id = server_session_->ss_id_;
  }

  if (OB_ISNULL(client_session_)) {
    LOG_WARN("client will abort soon",
             K_(sm_id),
             K(cs_id),
             K(proxy_sessid),
             K(ss_id),
             K(server_sessid),
             "client_ip", trans_state_.client_info_.addr_,
             "server_ip", trans_state_.server_info_.addr_,
             "event", ObMysqlDebugNames::get_event_name(event),
             "request_cmd", get_mysql_cmd_str(trans_state_.trans_info_.client_request_.get_packet_meta().cmd_),
             "sql_cmd", get_mysql_cmd_str(trans_state_.trans_info_.sql_cmd_),
             "sql", trans_state_.trans_info_.get_print_sql());
  } else {
    LOG_WARN("client will abort soon",
             K_(sm_id),
             K(cs_id),
             K(proxy_sessid),
             K(ss_id),
             K(server_sessid),
             "client_ip", trans_state_.client_info_.addr_,
             "server_ip", trans_state_.server_info_.addr_,
             "cluster_name", client_session_->get_session_info().get_priv_info().cluster_name_,
             "tenant_name", client_session_->get_session_info().get_priv_info().tenant_name_,
             "user_name", client_session_->get_session_info().get_priv_info().user_name_,
             "db", client_session_->get_session_info().get_database_name(),
             "event", ObMysqlDebugNames::get_event_name(event),
             "request_cmd", get_mysql_cmd_str(trans_state_.trans_info_.client_request_.get_packet_meta().cmd_),
             "sql_cmd", get_mysql_cmd_str(trans_state_.trans_info_.sql_cmd_),
             "sql", trans_state_.trans_info_.get_print_sql());

    OBPROXY_XF_LOG(INFO, xflush_head,
                   "client_ip", trans_state_.client_info_.addr_,
                   "server_ip", trans_state_.server_info_.addr_,
                   "cluster_name", client_session_->get_session_info().get_priv_info().cluster_name_,
                   "tenant_name", client_session_->get_session_info().get_priv_info().tenant_name_,
                   "user_name", client_session_->get_session_info().get_priv_info().user_name_,
                   "db", client_session_->get_session_info().get_database_name(),
                   "sql", trans_state_.trans_info_.client_request_.get_print_sql(),
                   "sql_cmd", get_mysql_cmd_str(trans_state_.trans_info_.sql_cmd_),
                   "event", ObMysqlDebugNames::get_event_name(event));
  }
}

// Called when we are not tunneling a response from the server.
void ObMysqlSM::release_server_session()
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(NULL != server_session_) && OB_LIKELY(NULL != server_entry_)
      && OB_LIKELY(server_entry_->vc_ == server_session_)) {
    if (OB_LIKELY(NULL != client_session_)) {
      LOG_DEBUG("release_server_session", "in_tunnel", server_entry_->in_tunnel_,
                "eos", server_entry_->eos_, "vc_type", server_entry_->vc_type_,
                "vc", server_entry_->vc_);

      server_entry_->in_tunnel_ = true;
      if (OB_FAIL(vc_table_.cleanup_entry(server_entry_))) {
        LOG_WARN("failed to cleanup server entry", K_(sm_id), K(ret));
      }
      server_entry_ = NULL;
      server_session_ = NULL;

      //put curr ss into last
      ObMysqlServerSession *curr_ss = client_session_->get_cur_server_session();
      if (OB_LIKELY(NULL != curr_ss)) {
        --(curr_ss->server_trans_stat_);
        if (OB_FAIL(client_session_->attach_server_session(curr_ss))) {
          LOG_WARN("client session failed to attach server session", K_(sm_id), K(ret));
        }
      }
    } else {
      LOG_WARN("client_session is null, clear server_entry", "in_tunnel", server_entry_->in_tunnel_,
                "eos", server_entry_->eos_, "vc_type", server_entry_->vc_type_,
                "vc", server_entry_->vc_);
      clear_server_entry();
    }
  }
}

// We failed in our attempt transfer a request to the
// server. Two cases happen here. The normal one is the
// server died, in which case we ought to return an error
// to the client. The second one is stupid. The server
// returned a response without reading all the transfer data.
// In order to be as transparent as possible process the
// server's response
void ObMysqlSM::handle_request_transfer_failure()
{
  int ret = OB_SUCCESS;
  STATE_ENTER(ObMysqlSM::handle_request_transfer_failure, VC_EVENT_NONE);

  if (OB_ISNULL(server_session_) || OB_ISNULL(server_entry_)
      || OB_ISNULL(client_session_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("invalid internal state",
              K_(server_session), K_(server_entry), K_(client_session),
              K_(sm_id), K(ret));
  } else if (OB_UNLIKELY(!server_entry_->eos_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("invalid internal state, server entry must be eos state",
              K_(server_entry_->eos), K_(sm_id), K(ret));
  } else {
    client_entry_->in_tunnel_ = false;
    server_entry_->in_tunnel_ = false;

    if (server_buffer_reader_->read_avail() > 0) {
      tunnel_.reset();
      // There's data from the server so try to read the response
      if (OB_FAIL(setup_server_response_read())) {
        LOG_WARN("failed to setup_server_response_read", K_(sm_id), K(ret));
      }
    } else {
      tunnel_.reset();
      // Server died
      if (OB_FAIL(vc_table_.cleanup_entry(server_entry_))) {
        LOG_WARN("failed to cleanup server entry", K_(sm_id), K(ret));
      } else {
        server_entry_ = NULL;
        server_session_ = NULL;
        trans_state_.current_.state_ = ObMysqlTransact::CONNECTION_CLOSED;
        call_transact_and_set_next_state(ObMysqlTransact::handle_response);
      }
    }
  }

  if (OB_FAIL(ret)) {
    trans_state_.inner_errcode_ = ret;
    trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
    call_transact_and_set_next_state(ObMysqlTransact::handle_response);
  }
}

// The server connection is now open. If there is a client transformation,
// we need setup a transform is there is one otherwise we need
// to send the request
void ObMysqlSM::handle_observer_open()
{
  int ret = OB_SUCCESS;

  LOG_DEBUG("[ObMysqlSM::handle_observer_open]", K_(sm_id), K(trans_state_.send_reqeust_direct_),
            "server_ip", trans_state_.server_info_.addr_);

  milestones_.do_observer_open_end_ = get_based_hrtime();
  cmd_time_stats_.do_observer_open_time_ += milestone_diff(milestones_.do_observer_open_begin_,
                                                           milestones_.do_observer_open_end_);

  // applying per-transaction observer netVC options
  // here IFF they differ from the netVC's current
  // options. This should keep this from being
  // redundant on a server session's first
  // transaction.
  if (OB_LIKELY(NULL != server_session_)) {
    ObNetVConnection *vc = server_session_->get_netvc();

    if (OB_LIKELY(NULL != vc) &&
        (vc->options_.sockopt_flags_ != trans_state_.mysql_config_params_->sock_option_flag_out_
         || vc->options_.packet_mark_ != trans_state_.mysql_config_params_->sock_packet_mark_out_
         || vc->options_.packet_tos_ != trans_state_.mysql_config_params_->sock_packet_tos_out_)) {
      vc->options_.sockopt_flags_ = static_cast<uint32_t>(trans_state_.mysql_config_params_->sock_option_flag_out_);
      vc->options_.packet_mark_ = static_cast<uint32_t>(trans_state_.mysql_config_params_->sock_packet_mark_out_);
      vc->options_.packet_tos_ = static_cast<uint32_t>(trans_state_.mysql_config_params_->sock_packet_tos_out_);
      if (vc->options_.sockopt_flags_ & ObNetVCOptions::SOCK_OPT_KEEP_ALIVE) {
        vc->options_.set_keepalive_param(static_cast<int32_t>(trans_state_.mysql_config_params_->server_tcp_keepidle_),
                                         static_cast<int32_t>(trans_state_.mysql_config_params_->server_tcp_keepintvl_),
                                         static_cast<int32_t>(trans_state_.mysql_config_params_->server_tcp_keepcnt_),
                                         static_cast<int32_t>(trans_state_.mysql_config_params_->server_tcp_user_timeout_));
      }
      if (OB_FAIL(vc->apply_options())) {
        LOG_WARN("server session failed to apply per-transaction socket options", K_(sm_id), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObMysqlTransact::update_sql_cmd(trans_state_);

    switch (trans_state_.current_.send_action_) {
      case ObMysqlTransact::SERVER_SEND_HANDSHAKE:
        // normally, we will set query_timeout before we send request,
        // but in SERVER_SEND_HANDSHAK nothing to be sent to observer,
        // observer will send handshake packet to obproxy first,
        // so we must set timeout here
        set_server_query_timeout();
        if (OB_FAIL(setup_server_response_read())) {
          LOG_WARN("failed to setup_server_response_read", K_(sm_id), K(ret));
        }
        break;

      case ObMysqlTransact::SERVER_SEND_REQUEST:
        if (need_setup_client_transfer()) {
          if (OB_FAIL(setup_client_transfer(MYSQL_TRANSFORM_VC))) {
            LOG_WARN("failed to setup_client_transfer", K_(sm_id), K(ret));
          }
        } else {
          callout_api_and_start_next_action(ObMysqlTransact::SM_ACTION_API_SEND_REQUEST);
        }
        break;

      case ObMysqlTransact::SERVER_SEND_ALL_SESSION_VARS:
        //fall through
      case ObMysqlTransact::SERVER_SEND_USE_DATABASE:
        //fall through
      case ObMysqlTransact::SERVER_SEND_SESSION_VARS:
        //fall through
      case ObMysqlTransact::SERVER_SEND_LAST_INSERT_ID:
        //fall through
      case ObMysqlTransact::SERVER_SEND_START_TRANS:
        //fall through
      case ObMysqlTransact::SERVER_SEND_PREPARE:
      case ObMysqlTransact::SERVER_SEND_TEXT_PS_PREPARE:
        callout_api_and_start_next_action(ObMysqlTransact::SM_ACTION_API_SEND_REQUEST);
        break;

      case ObMysqlTransact::SERVER_SEND_NONE:
        //fall through
      case ObMysqlTransact::SERVER_SEND_SAVED_LOGIN:
        //fall through
      case ObMysqlTransact::SERVER_SEND_LOGIN:
        //fall through
      default:
        ret = OB_INNER_STAT_ERROR;
        LOG_ERROR("Unexpected send next action type",
                  "send_action", ObMysqlTransact::get_send_action_name(trans_state_.current_.send_action_));
        break;
    }
  }

  if (OB_FAIL(ret)) {
    trans_state_.inner_errcode_ = ret;
    trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
    call_transact_and_set_next_state(ObMysqlTransact::handle_response);
  }
}

inline bool ObMysqlSM::need_setup_client_transfer()
{
  bool need = false;
  if (client_session_->get_session_info().is_oceanbase_server()) {
    need = (!trans_state_.is_auth_request_
            && trans_state_.trans_info_.request_content_length_ > 0
            && (PROTOCOL_CHECKSUM == use_compression_protocol()
                || PROTOCOL_OB20 == use_compression_protocol()
                || obmysql::OB_MYSQL_COM_STMT_PREPARE == trans_state_.trans_info_.client_request_.get_packet_meta().cmd_)
            && NULL != api_.do_request_transform_open());
    if (need) {
      LOG_DEBUG("[need_setup_client_transfer] will setup client transfer", K_(sm_id));
    }
  }

  return need;
}

// Handles setting trans_state_.current_.state_ and
// calling Transact in between opening an observer
// connection and receiving the response (in the case of
// the request transfer, a request transfer tunnel happens
// in between the sending request and reading the response
void ObMysqlSM::handle_server_setup_error(int event, void *data)
{
  int ret = OB_SUCCESS;
  bool need_handle_response = true;

  STATE_ENTER(ObMysqlSM::handle_server_setup_error, event);

  if (OB_ISNULL(server_entry_) || OB_ISNULL(data)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("invalid internal state, server entry or data is NULL",
              K_(server_entry), K(data), K_(sm_id), K(ret));
  } else {
    // If there is request transfer tunnel wait for the tunnel
    // to figure out that things have gone to hell
    if (tunnel_.is_tunnel_active()) {
      LOG_DEBUG("[handle_server_setup_error] "
                "forwarding event to request transfer tunnel",
                K_(sm_id), "event", ObMysqlDebugNames::get_event_name(event));

      if (OB_UNLIKELY(server_entry_->read_vio_ != reinterpret_cast<ObVIO *>(data))) {
        trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
        LOG_ERROR("invalid internal state, srever entry vio is different with data",
                  K_(server_entry_->read_vio), K(data), K_(sm_id));
      } else {
        ObMysqlTunnelConsumer *c = tunnel_.get_consumer(server_entry_->vc_);

        // it is possible only client request transform is set up
        // this happened for Linux iocore where NET_EVENT_OPEN was returned
        // for a non-existing listening port. the hack is to pass the error
        // event for server connection to request_transform_info
        if (NULL == c && NULL != api_.request_transform_info_.vc_) {
          c = tunnel_.get_consumer(api_.request_transform_info_.vc_);

          ObMysqlTunnelProducer *client_producer = c->producer_;
          if (OB_ISNULL(client_producer) || OB_ISNULL(client_entry_)
              || OB_UNLIKELY(client_entry_->vc_ != client_producer->vc_)) {
            ret = OB_INNER_STAT_ERROR;
            LOG_ERROR("invalid internal state, client entry vc is different with client producer vc",
                      K_(client_entry_->vc), K_(client_producer->vc), K_(sm_id));
          } else {
            set_client_wait_timeout();
            client_entry_->vc_handler_ = &ObMysqlSM::state_watch_for_client_abort;
            client_entry_->read_vio_ =
                client_producer->vc_->do_io_read(this, INT64_MAX, c->producer_->read_buffer_);
            client_producer->vc_->do_io_shutdown(IO_SHUTDOWN_READ);

            if (OB_ISNULL(client_entry_->read_vio_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("client entry failed to do_io_read", K_(sm_id), K(ret));
            } else {
              client_producer->alive_ = false;
              client_producer->handler_state_ = MYSQL_SM_REQUEST_TRANSFER_SERVER_FAIL;
              tunnel_.handle_event(VC_EVENT_ERROR, c->write_vio_);
              need_handle_response = false;
            }
          }
        } else {
          tunnel_.handle_event(event, c->write_vio_);
          need_handle_response = false;
        }
      }
    } else {
      if (NULL != api_.request_transform_info_.vc_) {
        ObMysqlTunnelConsumer *c = tunnel_.get_consumer(api_.request_transform_info_.vc_);

        if (NULL != c && MYSQL_SM_TRANSFORM_OPEN == c->handler_state_) {
          if (OB_FAIL(vc_table_.cleanup_entry(api_.request_transform_info_.entry_))) {
            LOG_WARN("vc table failed to cleanup server entry", K_(sm_id), K(ret));
          } else {
            api_.request_transform_info_.entry_ = NULL;
            tunnel_.reset();
          }
        }
      }

      bool is_internal_send_process = ObMysqlTransact::is_in_internal_send_process(trans_state_);
      switch (event) {
        case VC_EVENT_EOS: {
          trans_state_.current_.state_ = (is_internal_send_process ? ObMysqlTransact::CONNECT_ERROR
                                          : ObMysqlTransact::CONNECTION_CLOSED);
          break;
        }
        case VC_EVENT_ERROR: {
          trans_state_.current_.state_ = ObMysqlTransact::CONNECTION_ERROR;
          break;
        }
        case VC_EVENT_ACTIVE_TIMEOUT: {
          trans_state_.current_.state_ = ObMysqlTransact::ACTIVE_TIMEOUT;
          break;
        }
        case VC_EVENT_INACTIVITY_TIMEOUT: {
          trans_state_.current_.state_ =  ObMysqlTransact::INACTIVE_TIMEOUT;
          break;
        }
        default: {
          trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
          break;
        }
      }

      // just print log
      if (is_internal_send_process
          && (ObMysqlTransact::CONNECT_ERROR == trans_state_.current_.state_)) {
        LOG_INFO("[ObMysqlSM::handle_server_setup_error] failed in internal"
                  " send process, will retry", K_(sm_id), "event",
                  ObMysqlDebugNames::get_event_name(event), "current_send_action",
                  ObMysqlTransact::get_send_action_name(trans_state_.current_.send_action_),
                  "state", ObMysqlTransact::get_server_state_name(trans_state_.current_.state_));
      }

      bool can_release = can_server_session_release();
      // Close down server connection and deallocate buffers
      if (OB_UNLIKELY(server_entry_->in_tunnel_)) {
        trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
        LOG_ERROR("invalid server entry state", K_(sm_id), K_(server_entry_->in_tunnel));
      } else if (OB_FAIL(vc_table_.cleanup_entry(server_entry_, !can_release))) {
        LOG_WARN("vc table failed to cleanup server entry", K_(sm_id), K(ret));
      } else {
        if (can_release) {
          release_server_session_to_pool();
        }
        server_entry_ = NULL;
        server_session_ = NULL;
      }

      // if we are waiting on a plugin callout for
      // MYSQL_API_SEND_REQUEST defer calling transact until
      // after we've finished processing the plugin callout
      switch (api_.callout_state_) {
        case MYSQL_API_NO_CALLOUT:
          // Normal fast path case, no api callouts in progress
          break;

        case MYSQL_API_IN_CALLOUT:
        case MYSQL_API_DEFERED_SERVER_ERROR:
          // Callout in progress note that we are in deferring
          // the server error
          api_.callout_state_ = MYSQL_API_DEFERED_SERVER_ERROR;
          need_handle_response = false;
          break;

        case MYSQL_API_DEFERED_CLOSE:
          // The client has shutdown killing the sm
          // but we are stuck waiting for the server callout
          // to finish so do nothing here. We don't care about
          // the server connection at this and are just
          // waiting till we can execute the close hook
          need_handle_response = false;
          break;

        default:
          ret = OB_INNER_STAT_ERROR;
          LOG_ERROR("Unknown api state type", K_(sm_id), K_(api_.callout_state));
          break;
      }
    }
  }

  if (OB_FAIL(ret)) {
    trans_state_.inner_errcode_ = ret;
    trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
  }

  if (need_handle_response) {
    call_transact_and_set_next_state(ObMysqlTransact::handle_response);
  }
}

int ObMysqlSM::setup_client_transfer(ObMysqlVCType to_vc_type)
{
  int ret = OB_SUCCESS;
  int64_t written_bytes = 0;
  ObMysqlTunnelProducer *p = NULL;
  ObMysqlTunnelConsumer *c = NULL;

  LOG_DEBUG("Setup Client Transfer", K_(sm_id), "to_vc_type", get_mysql_vc_type(to_vc_type),
            "sql_cmd", ObProxyParserUtils::get_sql_cmd_name(trans_state_.trans_info_.sql_cmd_));

  if (trans_state_.trans_info_.request_content_length_ <= 0 || OB_ISNULL(client_buffer_reader_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid request length",
             K(trans_state_.trans_info_.request_content_length_), K_(client_buffer_reader),
             K_(sm_id), K(ret));
  } else if (OB_FAIL(trans_state_.alloc_internal_buffer(MYSQL_BUFFER_SIZE))) {
    LOG_ERROR("fail to allocate internal buffer,", K_(sm_id), K(ret));
  } else if (OB_FAIL(ObMysqlTransact::rewrite_stmt_id(trans_state_, client_buffer_reader_ ))) {
    LOG_WARN("rewrite stmt id failed", K(ret));
  } else {
    // Next order of business if copy the remaining data from the
    // request buffer into new buffer
    if (OB_FAIL(trans_state_.internal_buffer_->remove_append(client_buffer_reader_, written_bytes))) {
      LOG_WARN("fail to remove block from client buffer reader to internal buffer",
               K(written_bytes), K_(sm_id), K(ret));
    } else {
      client_session_->reset_read_buffer();
      client_buffer_reader_ = client_session_->get_reader();
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(p = tunnel_.add_producer(client_entry_->vc_,
                                           -1, // need request analyzer to inform complete
                                           trans_state_.internal_reader_,
                                           &ObMysqlSM::tunnel_handler_request_transfer_client,
                                           MT_MYSQL_CLIENT,
                                           "client request transfer", false))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to add producer", K_(sm_id), K(ret));
    } else {
      client_entry_->in_tunnel_ = true;

      switch (to_vc_type) {
        case MYSQL_TRANSFORM_VC:
          MYSQL_SM_SET_DEFAULT_HANDLER(&ObMysqlSM::state_request_wait_for_transform_read);
          if (OB_ISNULL(api_.request_transform_info_.entry_)
              || api_.request_transform_info_.entry_->vc_ != api_.request_transform_info_.vc_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid internal state",
                     K_(api_.request_transform_info_.entry),
                     K_(api_.request_transform_info_.entry_->vc),
                     K_(api_.request_transform_info_.vc), K_(sm_id), K(ret));
          } else if (OB_ISNULL(c = tunnel_.add_consumer(api_.request_transform_info_.entry_->vc_,
                                                        client_entry_->vc_,
                                                        &ObMysqlSM::tunnel_handler_transform_write,
                                                        MT_TRANSFORM,
                                                        "client transform"))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add consumer", K_(sm_id), K(ret));
          } else {
            api_.request_transform_info_.entry_->in_tunnel_ = true;
          }
          break;

        case MYSQL_SERVER_VC:
          MYSQL_SM_SET_DEFAULT_HANDLER(&ObMysqlSM::tunnel_handler_request_transfered);
          if (OB_ISNULL(c = tunnel_.add_consumer(server_entry_->vc_,
                                                 client_entry_->vc_,
                                                 &ObMysqlSM::tunnel_handler_request_transfer_server,
                                                 MT_MYSQL_SERVER,
                                                 "observer request transfer"))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add consumer", K_(sm_id), K(ret));
          } else {
            server_entry_->in_tunnel_ = true;
          }
          break;

        default:
          ret = OB_INNER_STAT_ERROR;
          LOG_ERROR("Unknown vconnection type", K(to_vc_type),  K_(sm_id), K(ret));
          break;
      }
    }

    if (OB_SUCC(ret)) {
      request_analyzer_.reset();
      if (OB_FAIL(p->set_request_packet_analyzer(MYSQL_REQUEST, &request_analyzer_))) {
        LOG_WARN("failed to set_producer_packet_analyzer", K(p), K_(sm_id), K(ret));
      } else if (OB_FAIL(tunnel_.tunnel_run(p))) {
        LOG_WARN("failed to run tunnel", K(p), K_(sm_id), K(ret));
      } else {
        // If we're half closed, we got a FIN from the client. Forward it on to the observer
        // now that we have the tunnel operational.
        if (client_session_->get_half_close_flag()) {
          p->vc_->do_io_shutdown(IO_SHUTDOWN_READ);
        }
      }
    }
  }

  return ret;
}

// if NULL == s mean reuse the existent server session
int ObMysqlSM::attach_server_session(ObMysqlServerSession &s)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(NULL != server_session_) || OB_UNLIKELY(NULL != server_entry_)
      || OB_ISNULL(client_session_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("invalid internal state, server session and server entry should be NULL,"
              "client session should not be NULL",
              K_(server_session), K_(server_entry), K_(client_session),
              K_(sm_id), K(ret));
    dump_history_state();
  } else if (OB_UNLIKELY(MSS_ACTIVE != s.state_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("invalid server session state", K_(s.state), K_(sm_id), K(ret));
    dump_history_state();
  } else {
    if (trans_state_.server_info_.addr_ != s.server_ip_) {
      trans_state_.server_info_.set_addr(s.get_netvc()->get_remote_addr());
    }
    server_session_ = &s;
    ++(server_session_->transact_count_);

    // mark the cur session
    client_session_->set_cur_server_session(server_session_);

    // Set the mutex_ so that we have something to update stats with
    server_session_->mutex_ = mutex_;

    MYSQL_INCREMENT_DYN_STAT(CURRENT_SERVER_TRANSACTIONS);
    ++s.server_trans_stat_;

    // Record the VC in our table
    server_entry_ = vc_table_.new_entry();
    server_entry_->vc_ = server_session_;
    server_entry_->vc_type_ = MYSQL_SERVER_VC;
    server_entry_->vc_handler_ = &ObMysqlSM::state_server_request_send;

    if (client_session_->is_session_pool_client() && client_session_->is_proxy_mysql_client_) {
      if (server_session_->get_session_info().get_ob_capability() == 0) {
        server_session_->get_session_info().set_ob_capability(client_session_->get_session_info().get_ob_capability());
        LOG_DEBUG("set_ob_capability", K(client_session_->get_session_info().get_ob_capability()));
      }
    }

    // Initiate a read on the session so that the SM and not
    // session manager will get called back if the timeout occurs
    // or the server closes on us. The IO Core now requires us to
    // do the read with a buffer and a size so preallocate the
    // buffer
    server_buffer_reader_ = server_session_->get_reader();

    // We are only setting up an empty read at this point.  This
    // is suffient to have the timeout errors directed to the appropriate
    // SM handler, but we don't want to read any data until the tunnel has
    // been set up. Since if no tunnels are set up, there is no danger of
    // data being delivered to the wrong tunnel's consumer handler. But for
    // client transfer that send data after the request, two tunnels are
    // created in  series, and with a full read set up at this point, the
    // EOS from the  first tunnel was sometimes behind handled by the consumer
    // of the  first tunnel instead of the producer of the second tunnel.
    // The real read is setup in setup_server_response_read()
    server_entry_->read_vio_ = server_session_->do_io_read(
        this, 0, server_session_->read_buffer_);
    if (OB_ISNULL(server_entry_->read_vio_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("server entry failed to do_io_read", K_(sm_id), K(ret));
    } else {
      // Transfer control of the write side as well
      server_entry_->write_vio_ = server_session_->do_io_write(this, 0, NULL);
      if (OB_ISNULL(server_entry_->write_vio_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("server entry failed to do_io_write", K_(sm_id), K(ret));
      } else {
        // Setup the timeouts
        // Set the inactivity timeout to the query timeout so that we
        // we fail this server if it doesn't start sending the response
        if (-1 != trans_state_.api_txn_connect_timeout_value_) {
          server_session_->get_netvc()->set_inactivity_timeout(
              HRTIME_MSECONDS(trans_state_.api_txn_connect_timeout_value_));
        }
      }
    }
  }
  return ret;
}

int ObMysqlSM::setup_server_request_send()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(server_session_) || OB_ISNULL(server_entry_)
      || OB_ISNULL(client_session_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("invalid internal state, server session, or server entry or client session is NULL",
              K_(server_session), K_(server_entry), K_(client_session),
              K_(sm_id), K(ret));
    dump_history_state();
  } else if (OB_UNLIKELY(server_entry_->vc_ != server_session_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("server_entry_vc is different with server session",
              K_(server_entry_->vc), K_(server_session), K_(sm_id), K(ret));
    dump_history_state();
  } else if (OB_UNLIKELY(ObMysqlTransact::SERVER_SEND_NONE == trans_state_.current_.send_action_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("invalid send_action", "send_action", trans_state_.current_.send_action_, K_(sm_id), K(ret));
    dump_history_state();
  } else {
    LOG_DEBUG("[ObMysqlSM::setup_server_request_send] send request to observer", K_(sm_id));

    // Send the request header
    server_entry_->vc_handler_ = &ObMysqlSM::state_server_request_send;

    ObIOBufferReader *buf_start = NULL;
    int64_t request_len = 0;

    int64_t build_server_request_begin = get_based_hrtime();
    if (OB_FAIL(ObMysqlTransact::build_server_request(trans_state_, buf_start, request_len))) {
      LOG_WARN("failed to build server request", K(buf_start), K(ret));
    } else if (OB_ISNULL(buf_start) || OB_UNLIKELY(request_len <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid request buf", K(buf_start), K(request_len), K(ret));
    } else {
      int64_t build_server_request_end = get_based_hrtime();
      cmd_time_stats_.build_server_request_time_ += milestone_diff(build_server_request_begin, build_server_request_end);
      //reset before send request
      milestones_.server_.reset();
      cmd_time_stats_.server_request_write_time_ = 0;
      cmd_time_stats_.server_response_read_time_ = 0;
      cmd_time_stats_.plugin_decompress_response_time_ = 0;
      cmd_time_stats_.server_response_analyze_time_ = 0;
      cmd_time_stats_.ok_packet_trim_time_ = 0;
      cmd_time_stats_.client_response_write_time_ = 0;

      MYSQL_INCREMENT_TRANS_STAT(SERVER_REQUESTS);
      milestones_.server_.reset();
      cmd_size_stats_.server_request_bytes_ = request_len;
      milestones_.server_.server_write_begin_ = get_based_hrtime();

      if (0 == milestones_.server_first_write_begin_
          && (trans_state_.is_auth_request_
              || ObMysqlTransact::SERVER_SEND_REQUEST == trans_state_.current_.send_action_)) {
        milestones_.server_first_write_begin_ = milestones_.server_.server_write_begin_;
        cmd_time_stats_.prepare_send_request_to_server_time_ =
          milestone_diff(milestones_.client_.client_begin_, milestones_.server_first_write_begin_);
      }

      // if write_buffer_ is not NULL, when clean server entry, will free the write_buffer_.
      // so the mio_buffer we alloc in build_server_request will free evently.
      server_entry_->write_buffer_ = (buf_start != client_buffer_reader_) ? buf_start->writer() : NULL;

      // set query_timeout to each request, ddl stmt will never timeout
      if (trans_state_.trans_info_.client_request_.get_parse_result().is_ddl_stmt()) {
        cancel_server_query_timeout();
      // if PL/SQL:
      //  1. if in trans, use trx timeout
      //  2. if not in trans, no timeout
      // Attention:
      //  1. if send begin or start, think as in trans
      //  2. if autocommit = 0, first SQL think as not in trans
      } else if (trans_state_.trans_info_.client_request_.get_parse_result().is_call_stmt()
                 || trans_state_.trans_info_.client_request_.get_parse_result().has_anonymous_block()) {
        if (trans_state_.is_hold_start_trans_ || ObMysqlTransact::is_in_trans(trans_state_)) {
          set_server_trx_timeout();
        } else {
          cancel_server_query_timeout();
        }
      } else {
        set_server_query_timeout();
      }

      if (!trans_state_.is_auth_request_
          && ObMysqlTransact::SERVER_SEND_REQUEST == trans_state_.current_.send_action_
          && trans_state_.trans_info_.request_content_length_ > 0) {
        if (NULL != api_.request_transform_info_.vc_) {
          if (OB_FAIL(api_.setup_transform_to_server_transfer())) {
            LOG_WARN("failed to setup_transform_to_server_transfer", K_(sm_id), K(ret));
          }
        } else {
          if (OB_FAIL(setup_client_transfer(MYSQL_SERVER_VC))) {
            LOG_WARN("failed to setup_client_transfer", K_(sm_id), K(ret));
          }
        }
      } else {
        server_entry_->write_vio_ = server_entry_->vc_->do_io_write(this, request_len, buf_start);
        if (OB_ISNULL(server_entry_->write_vio_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("server entry failed to do_io_write", K_(sm_id), K(ret));
        }
      }
    }
  }

  return ret;
}

int ObMysqlSM::setup_server_response_read()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(server_session_) || OB_ISNULL(server_entry_)
      || OB_ISNULL(client_session_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("invalid internal state, server session, or server entry or cleint session is NULL",
              K_(server_session), K_(server_entry), K_(client_session),
              K_(sm_id), K(ret));
  } else if (OB_ISNULL(server_buffer_reader_)) {
    // We should have set the server_buffer_reader
    // we sent the request header
    ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("server_buffer_reader should not be NULL", K_(sm_id), K(ret));
  } else if (OB_ISNULL(server_entry_->read_vio_)) {
    // We already done the READ when we setup the connection to
    // read the request, attach_server_session()
    ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("server entry read vio should not be NULL", K_(sm_id), K(ret));
  } else {
    // Now that we've got the ability to read from the
    // server, setup to read the response header
    server_entry_->vc_handler_ = &ObMysqlSM::state_server_response_read;

    trans_state_.current_.state_ = ObMysqlTransact::STATE_UNDEFINED;
    trans_state_.server_info_.state_ = ObMysqlTransact::STATE_UNDEFINED;

    cmd_size_stats_.server_response_bytes_ = 0;
    milestones_.server_.server_read_end_ = 0;

    // The tunnel from observer to client is now setup. Ready to read the response
    if (NULL != trans_state_.cache_block_) {
      // use the cached block to read server response
      server_session_->read_buffer_->append_block_internal(trans_state_.cache_block_);
      trans_state_.cache_block_ = NULL;
    }
    server_entry_->read_vio_ = server_session_->do_io_read(
        this, INT64_MAX, server_session_->read_buffer_);
    if (OB_ISNULL(server_entry_->read_vio_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("server session failed to do_io_read", K_(sm_id), K(ret));
    } else {
      // If there is anything in the buffer call the parsing routines
      // since if the response is finished, we won't get any
      // additional callbacks
      if (server_buffer_reader_->read_avail() > 0) {
        state_server_response_read((server_entry_->eos_) ? VC_EVENT_EOS : VC_EVENT_READ_READY,
                                   server_entry_->read_vio_);
      }
    }
  }
  return ret;
}

// The proxy has generated an error message which it
// is sending to the client.
void ObMysqlSM::setup_error_transfer()
{
  if (tunnel_.is_tunnel_alive()) {
    // when tunnel is active, we can not close server entry and client entry here,
    // or it will double close in kill_this();
    // also we can not send internal data, if has, just treat as disconnect
    LOG_WARN("tunnel is alive, just disconnect", K_(sm_id));
    terminate_sm_ = true;
    trans_state_.source_ = ObMysqlTransact::SOURCE_INTERNAL;
  } else {
    //need clear server entry once arrive here
    if (NULL != server_entry_) {
      server_entry_->in_tunnel_ = false;
      clear_server_entry();
    }

    if ((NULL != trans_state_.internal_buffer_)
        && (NULL != trans_state_.internal_reader_)
        && (trans_state_.internal_reader_->read_avail() > 0)) {
      // Since we need to send the error message, call the API function
      callout_api_and_start_next_action(ObMysqlTransact::SM_ACTION_API_SEND_RESPONSE);
    } else {
      ObMySQLCmd request_cmd = trans_state_.trans_info_.client_request_.get_packet_meta().cmd_;
      if (OB_MYSQL_COM_QUIT == request_cmd) {
        LOG_INFO("[setup_error_transfer] Now closing connection caused by OB_MYSQL_COM_QUIT", K_(sm_id),
                 "request_cmd", get_mysql_cmd_str(request_cmd),
                 "sql_cmd", get_mysql_cmd_str(trans_state_.trans_info_.sql_cmd_),
                 "sql", trans_state_.trans_info_.get_print_sql());
      } else {
        if (OB_MYSQL_COM_LOGIN == trans_state_.trans_info_.sql_cmd_) {
          if (OB_ISNULL(client_session_)) {
            LOG_DEBUG("OB_MYSQL_COM_LOGIN close, should close");
            client_session_->can_server_session_release_ = false;
          }
        }
        client_session_->can_server_session_release_ = false;
        LOG_WARN("[setup_error_transfer] Now closing connection", K_(sm_id),
                 "request_cmd", get_mysql_cmd_str(request_cmd),
                 "sql_cmd", get_mysql_cmd_str(trans_state_.trans_info_.sql_cmd_),
                 "sql", trans_state_.trans_info_.get_print_sql());
      }

      if (OB_LIKELY(NULL != client_entry_)) {
        client_entry_->in_tunnel_ = false;
      } else {
        LOG_INFO("[setup_error_transfer] client_entry_ is NULL, no need cleanup", K_(sm_id));
      }

      terminate_sm_ = true;
      trans_state_.source_ = ObMysqlTransact::SOURCE_INTERNAL;
    }
  }
}

int ObMysqlSM::setup_internal_transfer(MysqlSMHandler handler_arg)
{
  int ret = OB_SUCCESS;
  ObMysqlTunnelProducer *p = NULL;
  ObMysqlTunnelConsumer *c = NULL;

  if (OB_ISNULL(trans_state_.internal_buffer_) || OB_ISNULL(trans_state_.internal_reader_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("invalid internal state, internal msg buffer or internal msg reaer are NULL",
             K_(trans_state_.internal_buffer), K_(trans_state_.internal_reader), K_(sm_id), K(ret));
  } else {
    LOG_DEBUG("[setup_internal_transfer] Now setup internal transfer", K_(sm_id));

    trans_state_.source_ = ObMysqlTransact::SOURCE_INTERNAL;
    MYSQL_SM_SET_DEFAULT_HANDLER(handler_arg);

    // Clear the decks before we setup the new producers
    // As things stand, we cannot have two static producers operating at once
    tunnel_.kill_tunnel();

    // Setup the tunnel to the client
    if (OB_ISNULL(p = tunnel_.add_producer(MYSQL_TUNNEL_STATIC_PRODUCER,
                                           trans_state_.internal_reader_->read_avail(),
                                           trans_state_.internal_reader_,
                                           NULL,
                                           MT_STATIC,
                                           "internal msg", false))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to add producer", K_(sm_id), K(ret));
    } else if (OB_ISNULL(c = tunnel_.add_consumer(client_entry_->vc_,
                                                  MYSQL_TUNNEL_STATIC_PRODUCER,
                                                  &ObMysqlSM::tunnel_handler_client,
                                                  MT_MYSQL_CLIENT,
                                                  "client"))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to add consumer", K_(sm_id), K(ret));
    } else {
      client_entry_->in_tunnel_ = true;

      if (OB_FAIL(p->set_response_packet_analyzer(0, MYSQL_RESPONSE, NULL, NULL))) {
        LOG_WARN("failed to set_producer_packet_analyzer", K(p), K_(sm_id), K(ret));
      } else if (OB_FAIL(tunnel_.tunnel_run(p))) {
        LOG_WARN("failed to run tunnel", K(p), K_(sm_id), K(ret));
      } else {
        if (ObMysqlTransact::INTERNAL_ERROR == trans_state_.current_.state_) {
          // when we need both send err packet to client and disconnect, will reach here;
          LOG_INFO("INTERNAL_ERROR, will disconnect", K_(sm_id));
        } else {
          // if the internal request is not the first request and not proxysys,
          // it means that it is in trans (or hold trans), in this case we do NOT change the trans_state_
          if (trans_state_.is_trans_first_request_) {
            if (trans_state_.is_auth_request_ || trans_state_.is_hold_start_trans_) {
              trans_state_.current_.state_ = ObMysqlTransact::CMD_COMPLETE;
            } else {
              // proxysys && !OB_MYSQL_COM_LOGIN &&!OB_MYSQL_COM_HANDSHAKE will also enter here
              trans_state_.current_.state_ = ObMysqlTransact::TRANSACTION_COMPLETE;
            }
          } else if ((client_session_->is_proxysys_tenant()
                      || (trans_state_.is_auth_request_ && client_session_->can_direct_ok()))
                     && OB_MYSQL_COM_LOGIN == trans_state_.trans_info_.sql_cmd_) {
            // proxysys && response OB_MYSQL_COM_LOGIN ok packet will enter here, we need set state_ TRANSACTION_COMPLETE
            trans_state_.current_.state_ = ObMysqlTransact::TRANSACTION_COMPLETE;
          } else if (ObMysqlTransact::CMD_COMPLETE != trans_state_.current_.state_) {
            LOG_WARN("unexpected current state, expected_state=CMD_COMPLETE",
                     "actual_state", ObMysqlTransact::get_server_state_name(trans_state_.current_.state_),
                      K_(sm_id));
          }
        }
      }
    }
  }

  return ret;
}

// Moves data from the header buffer into the reply buffer and return
// the number of bytes we should use for initiating the tunnel
int ObMysqlSM::server_transfer_init(ObMIOBuffer *buf, int64_t &nbytes)
{
  int ret = OB_SUCCESS;
  nbytes = 0;

  // should have been set up if we're doing a transfer.
  if (OB_ISNULL(server_buffer_reader_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("invalid internal state", K_(server_buffer_reader));
  } else {
    ObRespAnalyzeResult &resp = trans_state_.trans_info_.server_response_.get_analyze_result();
    if (server_entry_->eos_ || resp.is_resp_completed()) {
      // The server has shutdown on us already so the only data
      // we'll get is already in the buffer
      nbytes = server_buffer_reader_->read_avail();
      LOG_DEBUG("server_transfer_init",
                K_(sm_id), "is_resp_completed", resp.is_resp_completed());
    } else {
      nbytes = -1;
    }

    buf->water_mark_ = trans_state_.mysql_config_params_->default_buffer_water_mark_;

    // Next order of business if copy the remaining data from the
    // response buffer into new buffer.
    // relinquish the space in server_buffer and let
    // the tunnel use the trailing space
    int64_t server_response_pre_read_bytes = 0;
    if (OB_FAIL(buf->remove_append(server_buffer_reader_, server_response_pre_read_bytes))) {
      LOG_WARN("fail to remove block from server buffer reader to buf",
               K(server_response_pre_read_bytes), K(ret));
    } else {
      // reset the server session buffer
      server_session_->reset_read_buffer();
      server_buffer_reader_ = NULL;
    }
  }

  return ret;
}

int ObMysqlSM::setup_server_transfer()
{
  int ret = OB_SUCCESS;
  int64_t nbytes = 0;
  ObMysqlTunnelProducer *p = NULL;
  ObMysqlTunnelConsumer *c = NULL;

  LOG_DEBUG("Setup Server Transfer", K_(sm_id),
            "sql_cmd", ObProxyParserUtils::get_sql_cmd_name(trans_state_.trans_info_.sql_cmd_));

  if (OB_FAIL(trans_state_.alloc_internal_buffer(MYSQL_BUFFER_SIZE))) {
    LOG_ERROR("fail to allocate internal buffer,", K_(sm_id), K(ret));
  } else if (OB_FAIL(server_transfer_init(trans_state_.internal_buffer_, nbytes))) {
    LOG_WARN("failed to init server transfer", K_(sm_id), K(ret));
  } else if (OB_ISNULL(p = tunnel_.add_producer(server_entry_->vc_,
                                                nbytes,
                                                trans_state_.internal_reader_,
                                                &ObMysqlSM::tunnel_handler_server,
                                                MT_MYSQL_SERVER,
                                                "observer", false))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to add producer", K_(sm_id), K(p));
  } else if (OB_ISNULL(c = tunnel_.add_consumer(client_entry_->vc_,
                                                server_entry_->vc_,
                                                &ObMysqlSM::tunnel_handler_client,
                                                MT_MYSQL_CLIENT,
                                                "client"))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to add consumer", K_(sm_id), K(c));
  } else {
    MYSQL_SM_SET_DEFAULT_HANDLER(&ObMysqlSM::tunnel_handler_response_transfered);

    client_entry_->in_tunnel_ = true;
    server_entry_->in_tunnel_ = true;

    ObMysqlResp &server_response = trans_state_.trans_info_.server_response_;
    ObIMysqlRespAnalyzer *analyzer = NULL;
    bool is_resultset = server_response.get_analyze_result().is_resultset_resp();
    if ((PROTOCOL_CHECKSUM == use_compression_protocol())
        && (NULL != client_session_)
        // inner sql's compressed response has tranfer to normal mysql packet
        && (!client_session_->is_proxy_mysql_client_)) {
      if (is_resultset) {
        LOG_ERROR("compress protocol's never reach here", K(is_resultset));
      }
    } else {
      analyzer = is_resultset ? &analyzer_ : NULL;
    }

    if (OB_FAIL(p->set_response_packet_analyzer(0, MYSQL_RESPONSE, analyzer, &server_response))) {
      LOG_WARN("failed to set_producer_packet_analyzer", K(p), K_(sm_id), K(ret));
    } else if (OB_FAIL(tunnel_.tunnel_run(p))) {
      LOG_WARN("failed to run tunnel", K(p), K_(sm_id), K(ret));
    }
  }

  return ret;
}

int ObMysqlSM::setup_cmd_complete()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(client_session_) || OB_ISNULL(client_entry_) || OB_ISNULL(client_buffer_reader_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("invliad internal state, client session, or client entry, or client buffer reader is NULL",
              K_(client_session), K_(client_entry),
              K_(client_buffer_reader), K_(sm_id), K(ret));
  } else if (OB_UNLIKELY(client_entry_->vc_ != client_session_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("invliad internal state, client entry vc is different with client session",
              K_(client_session), K_(client_entry_->vc), K_(sm_id), K(ret));
  // for defence
  } else if (OB_UNLIKELY(NULL != client_session_->get_last_bound_server_session())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("invliad internal state, last bound server session is not null",
              K_(client_session), "last bound server session",
              reinterpret_cast<const void*>(client_session_->get_last_bound_server_session()),
              K_(sm_id), K(ret));
  } else {
    tunnel_.reset();
    client_entry_->in_tunnel_ = false;
    api_.reset();
    // reset client read buffer water mark
    client_buffer_reader_->mbuf_->water_mark_ = MYSQL_NET_META_LENGTH;

    if (client_session_->is_already_send_trace_info()) {
      client_session_->set_need_send_trace_info(false);
    }
    client_session_->set_first_handle_close_request(true);
    client_session_->set_in_trans_for_close_request(false);
    client_session_->set_sharding_select_log_plan(NULL);
    client_session_->set_need_return_last_bound_ss(false);

    if (OB_MYSQL_COM_HANDSHAKE == trans_state_.trans_info_.sql_cmd_) {
      // set inactivity timeout to connect_timeout after proxy send handshake
      // will be canceled when the login packet arrived
      set_client_connect_timeout();
    }

    // stat reset
    if (ObMysqlTransact::TRANSACTION_COMPLETE == trans_state_.current_.state_) {
      update_stats();
      client_session_->get_session_info().set_need_sync_session_vars(true);
      trans_stats_.reset();
      milestones_.trans_reset();
      set_client_wait_timeout();
      bool need_release = false;
      if ((NULL != server_session_ && server_entry_->in_tunnel_) || server_session_ == NULL) {
        need_release = true;
      }
      clear_server_entry();
      // using session pool, when not in trans can relase server session to pool
      if (get_global_proxy_config().need_release_after_tx) {
        if (need_release && can_server_session_release()) {
          ObMysqlServerSession * cur_ss = client_session_->get_server_session();
          // ObMysqlServerSession * lii_ss = client_session_->get_lii_server_session();
          if (!OB_ISNULL(cur_ss)) {
            cur_ss->release();
            client_session_->attach_server_session(NULL);
            LOG_DEBUG("transaction is complete, will release session now");
          }
        }
      }
      is_updated_stat_ = false;
      history_pos_ = 0;
      is_in_trans_ = false;
      MYSQL_SM_SET_DEFAULT_HANDLER(&ObMysqlSM::mysql_client_event_handler);
      LOG_DEBUG("transaction is complete, wait next transaction", K_(sm_id));
    } else {
      is_in_trans_ = true;
      update_cmd_stats();
      milestones_.cmd_reset();
      LOG_DEBUG("still in transaction, wait next request", K_(sm_id));
    }
    cmd_size_stats_.reset();
    cmd_time_stats_.reset();
    trans_state_.reset();
    // reset ps info
    if (NULL != server_session_) {
      server_session_->get_session_info().reset_server_ps_id();
    }
    if (NULL != client_session_) {
      client_session_->get_session_info().reset_recv_client_ps_id();
      client_session_->get_session_info().reset_client_ps_id();
      client_session_->get_session_info().reset_ps_entry();
      client_session_->get_session_info().reset_client_cursor_id();
    }

    // wait new client request
    if (OB_FAIL(setup_client_request_read())) {
      LOG_WARN("failed to setup_client_request_read", K_(sm_id), K(ret));
    }
  }
  return ret;
}

bool ObMysqlSM::need_close_last_used_ss()
{
  bool bret = false;
  const ObMysqlServerSession *last_ss = NULL;
  //when the follower match, server session is not available for readwrite
  //1. cs && ss is not null
  //2. server has readonly zone
  //3. user has set ob_read_consistency,
  //4. this is weak read, but ss is readwrite.
  //   OR this is strong read, but ss is readonly.
  if (OB_LIKELY(NULL != client_session_)
      && NULL != (last_ss = client_session_->get_server_session())
      && client_session_->get_session_info().is_read_consistency_set()
      && client_session_->dummy_ldc_.is_readonly_zone_exist()) {
    common::ObAddr tmp_addr;
    (void)tmp_addr.set_ipv4_addr(last_ss->server_ip_.get_ip4_host_order(), static_cast<int32_t>(last_ss->server_ip_.get_port_host_order()));
    const bool is_last_route_readonly_zone = client_session_->dummy_ldc_.is_readonly_zone(tmp_addr);
    const bool is_weak_read = (common::WEAK == static_cast<ObConsistencyLevel>(client_session_->get_session_info().get_read_consistency()));
    common::ObAddr current_addr;
    (void)current_addr.set_ipv4_addr(trans_state_.server_info_.addr_.get_ip4_host_order(),
        static_cast<int32_t>(trans_state_.server_info_.addr_.get_port_host_order()));
    const bool is_current_route_readonly_zone = client_session_->dummy_ldc_.is_readonly_zone(current_addr);
    // if readonly zone exist, should choose readonly zone when weak read and close connection which connect to readwrite zone
    if (is_last_route_readonly_zone != is_current_route_readonly_zone) {
      bret = true;
      LOG_INFO("last used server session not match readwrite policy, need close it", K_(sm_id),
               "server_sessid", last_ss->server_sessid_,
               "server_ip", last_ss->server_ip_, K(is_last_route_readonly_zone), K(is_weak_read), K(is_current_route_readonly_zone));
    }
  }
  return bret;
}

bool ObMysqlSM::can_server_session_release()
{
  bool result = false;
  bool is_in_trans = (trans_state_.is_hold_start_trans_ || ObMysqlTransact::is_in_trans(trans_state_));
  bool is_allowed_state_  = (ObMysqlTransact::STATE_UNDEFINED == trans_state_.current_.state_ ||
    ObMysqlTransact::TRANSACTION_COMPLETE == trans_state_.current_.state_);
  // here should handle some case before release
  if (client_session_ != NULL &&
      client_session_->is_session_pool_client() &&
      client_session_->can_server_session_release_ &&
      is_allowed_state_ &&
      !client_session_->get_session_info().is_trans_specified() && !is_in_trans) {
    result = true;
    LOG_DEBUG("can_server_session_release", K(result), K(trans_state_.current_.state_));
  }
  return result;
}

void ObMysqlSM::release_server_session_to_pool()
{
  if (OB_ISNULL(client_session_)) {
    LOG_WARN("invalid client_session");
  } else if (OB_ISNULL(server_session_)) {
    LOG_WARN("invalid server_session");
  } else {
    if (client_session_->get_lii_server_session() == server_session_) {
      client_session_->set_lii_server_session(NULL);
      LOG_DEBUG("lii same as server_session_");
    }
    if (MSS_ACTIVE == server_session_->state_) {
      --server_session_->server_trans_stat_;
    }
    server_session_->release();
    LOG_DEBUG("release server_session");
    server_session_ = NULL;
  }
}

int ObMysqlSM::mysql_client_event_handler(int event, void *data)
{
  int ret = OB_SUCCESS;
  STATE_ENTER(ObMysqlSM::mysql_client_event_handler, event);
  if (OB_LIKELY(NULL != client_session_)) {
    if (CLIENT_VC_SWAP_MUTEX_EVENT == event) { // from proxy client vc
      if (OB_FAIL(client_session_->swap_mutex(data))) {
        LOG_WARN("fail to swap mutex for mysql client", K_(sm_id), K(ret));
      }
    } else if (CLIENT_VC_DISCONNECT_LAST_USED_SS_EVENT == event && client_session_->is_proxy_mysql_client_) {
      LOG_DEBUG("CLIENT_VC_DISCONNECT_LAST_USED_SS_EVENT", K(client_session_->is_proxy_mysql_client_),
                K(client_session_->is_session_pool_client()), K_(sm_id));
      client_session_->close_last_used_ss();
      if (client_session_->is_session_pool_client()) {
        // for session pool client not close server session
        if (server_session_ != NULL) {
          LOG_DEBUG("is_session_pool_client here not close server session", K_(sm_id));
          vc_table_.cleanup_entry(server_entry_, false);
          server_entry_ = NULL;
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("invalid event or  internal state", K_(sm_id), K(event), K_(client_session));
  }
  return EVENT_DONE;
}

// This function has two phases. One before we call the
// asynchronous clean up routines (api and list removal)
// and one after. The state about which phase we are in is
// kept in ObMysqlSM::kill_this_async_done
void ObMysqlSM::kill_this()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(1 != reentrancy_count_)) {
    LOG_ERROR("invalid internal state, reentrancy_count should be 1",
              K_(reentrancy_count), K_(sm_id));
  }

  if (!kill_this_async_done_) {
    // cancel uncompleted actions
    // The action should be cancelled only if the
    // state machine is in MYSQL_API_NO_CALLOUT
    // state. This is because we are depending on the
    // callout to complete for the state machine to
    // get killed.

    LOG_DEBUG("will deallocate sm",
             K_(sm_id), K_(pending_action), K_(api_.callout_state));

    update_stats();

    if (MYSQL_API_NO_CALLOUT == api_.callout_state_ && NULL != pending_action_) {
      LOG_DEBUG("deallocating sm", K_(sm_id), K_(pending_action));
      if (OB_FAIL(pending_action_->cancel())) {
        LOG_WARN("failed to cancel pending action", K_(pending_action), K_(sm_id), K(ret));
      }
      pending_action_ = NULL;
    }

    // before close client_entry, must close server_entry_ firstly;
    if (NULL != server_entry_) {
      if (OB_FAIL(vc_table_.cleanup_entry(server_entry_))) {
        LOG_WARN("vc table failed to cleanup server entry", K_(sm_id), K_(server_entry), K(ret));
      } else {
        server_entry_ = NULL;
        server_session_ = NULL;
      }
    }

    if (OB_FAIL(vc_table_.cleanup_all())) {
      LOG_WARN("vc_table failed to cleanup_all", K_(sm_id), K(ret));
    }
    // Why don't we just kill the tunnel?  Might still be
    // active if the state machine is going down hard,
    // and we should clean it up.
    tunnel_.kill_tunnel();

    // It possible that a plugin added transform hook
    // but the hook never executed due to a client abort
    // In that case, we need to manually close all the
    // transforms to prevent memory leaks
    if (hooks_set_) {
      api_.transform_cleanup(OB_MYSQL_RESPONSE_TRANSFORM_HOOK, api_.response_transform_info_);
      api_.transform_cleanup(OB_MYSQL_REQUEST_TRANSFORM_HOOK, api_.request_transform_info_);
      api_.plugin_clients_cleanup();
    }

    // It's also possible that the plugin_tunnel vc was never
    // executed due to not contacting the server
    if (NULL != api_.plugin_tunnel_) {
      api_.plugin_tunnel_->kill_no_connect();
      api_.plugin_tunnel_ = NULL;
    }

    client_session_ = NULL;
    server_session_ = NULL;

    // So we don't try to nuke the state machine
    // if the plugin receives event we must reset
    // the terminate_flag
    terminate_sm_ = false;
    callout_api_and_start_next_action(ObMysqlTransact::SM_ACTION_API_SM_SHUTDOWN);
  }

  // The reentrancy_count is still valid up to this point since
  // the api shutdown hook is asynchronous and double frees can
  // happen if the reentrancy count is not still valid until
  // after all async callouts have completed
  //
  // Once we get to this point, we could be waiting for async
  // completion in which case we need to decrement the reentrancy
  // count since the entry points can't do it for us since they
  // don't know if the state machine has been destroyed.  In the
  // case we really are done with async callouts, decrement the
  // reentrancy count since it seems tacky to destruct a state
  // machine with non-zero count
  --reentrancy_count_;
  if (OB_UNLIKELY(0 != reentrancy_count_)) {
    LOG_ERROR("invalid internal state, reentrancy_count should be 0",
              K_(reentrancy_count), K_(sm_id));
  }

  // If the api shutdown & list removal was synchronous
  // then the value of kill_this_async_done has changed so
  // we must check it again
  if (kill_this_async_done_) {
    // In the async state, the plugin could have been
    // called resulting in the creation of a plugin_tunnel.
    // So it needs to be deleted now.
    if (NULL != api_.plugin_tunnel_) {
      api_.plugin_tunnel_->kill_no_connect();
      api_.plugin_tunnel_ = NULL;
    }

    if (NULL != trans_state_.congestion_entry_) {
      // if this trans succ, just set avlie this server;
      if (ObMysqlTransact::TRANSACTION_COMPLETE == trans_state_.current_.state_
          && !trans_state_.is_congestion_entry_updated_) {
        LOG_DEBUG("server is available, will set alive congested free", KPC_(trans_state_.congestion_entry));
        trans_state_.congestion_entry_->set_alive_congested_free();
      }
    }

    if (OB_UNLIKELY(NULL != pending_action_)
        || OB_UNLIKELY(!vc_table_.is_table_clear())
        || OB_UNLIKELY(tunnel_.is_tunnel_active())) {
      LOG_ERROR("after kill sm, invalid internal state",
                K_(pending_action), "is_table_clear", vc_table_.is_table_clear(),
                "is_tunnel_active", tunnel_.is_tunnel_active(), K_(sm_id));
    }

    MYSQL_SM_SET_DEFAULT_HANDLER(NULL);

#ifdef USE_MYSQL_DEBUG_LISTS
    if (OB_SUCC(mutex_acquire(&g_debug_sm_list_mutex))) {
      g_debug_sm_list.remove(this);
      if (OB_FAIL(mutex_release(&g_debug_sm_list_mutex))) {
        LOG_WARN("failed to release mutex", K_(sm_id));
      }
    }
#endif
    LOG_INFO("deallocating sm", K_(sm_id));

    destroy();
  }
}

inline void ObMysqlSM::update_congestion_entry(const int event)
{
  // if client connect timeout, proxy need set target server alive congested
  // the follow must matched:
  // 1. client is timeout
  // 2. proxy is connecting observer
  // 3. connect time >= min_congested_connect_timeout_
  int64_t client_connect_timeout = 0;
  if ((VC_EVENT_EOS == event
       || VC_EVENT_INACTIVITY_TIMEOUT == event
       || VC_EVENT_ACTIVE_TIMEOUT == event)
      && ObMysqlTransact::SM_ACTION_OBSERVER_OPEN == trans_state_.next_action_
      && 0 == milestones_.server_connect_end_
      && OB_LIKELY(NULL != pending_action_)
      && OB_LIKELY(NULL == server_session_)
      && OB_LIKELY(NULL != trans_state_.congestion_entry_)
      && OB_LIKELY(NULL != trans_state_.mysql_config_params_)
      && trans_state_.mysql_config_params_->min_congested_connect_timeout_
          <= (client_connect_timeout = milestone_diff(milestones_.server_connect_begin_, get_based_hrtime()))) {

    trans_state_.set_alive_failed();

    LOG_WARN("client connect timeout, proxy treat target server alive congested",
             K_(sm_id), "target server", trans_state_.congestion_entry_->server_ip_,
             K(client_connect_timeout),
             "min_congested_connect_timeout", trans_state_.mysql_config_params_->min_congested_connect_timeout_,
             "event", ObMysqlDebugNames::get_event_name(event));
  } else {
    LOG_DEBUG("no need update congestion entry",
             K_(sm_id), KPC(trans_state_.congestion_entry_),
             "next_action", ObMysqlTransact::get_action_name(trans_state_.next_action_),
             "server_connect_begin", milestones_.server_connect_begin_,
             "server_connect_end", milestones_.server_connect_end_,
             K(client_connect_timeout), KP(pending_action_), KP(server_session_),
             "event", ObMysqlDebugNames::get_event_name(event));
  }
}

inline void ObMysqlSM::get_server_session_ids(uint32_t &server_sessid, int64_t &ss_id)
{
  ObMysqlServerSession *tmp_ss = NULL;
  if (NULL != client_session_) {
    if (!ObMysqlTransact::is_internal_request(trans_state_)) {
      if (NULL != server_session_) {
        tmp_ss = server_session_;
      } else {
        if (NULL != client_session_) {
          tmp_ss = client_session_->get_server_session();
        }
      }
      if (NULL != tmp_ss) {
        server_sessid = tmp_ss->get_server_sessid();
        ss_id = tmp_ss->ss_id_;
      }
    }
  }
}

inline void ObMysqlSM::update_monitor_stats(const ObString &logic_tenant_name,
                                            const ObString &logic_database_name,
                                            const ObString &cluster_name,
                                            const ObString &tenant_name,
                                            const ObString &database_name,
                                            const DBServerType database_type,
                                            const ObProxyBasicStmtType stmt_type,
                                            char *error_code_str)
{
  int ret = OB_SUCCESS;

  ObString error_code(error_code_str);
  ObTenantStatItem *item = NULL;
  if (OB_FAIL(get_global_tenant_stat_mgr().get_or_create_item(logic_tenant_name,
                                                              logic_database_name,
                                                              cluster_name, tenant_name,
                                                              database_name, database_type,
                                                              stmt_type, error_code,
                                                              item))) {
    if (OB_EXCEED_MEM_LIMIT == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get or create tenant stat item", K(ret));
    }
  } else if (OB_ISNULL(item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant stat item is null, it should not happened", K(logic_tenant_name), K(logic_database_name),
             K(cluster_name), K(tenant_name), K(database_name), K(ret));
  } else {
    int64_t low_threshold = trans_state_.mysql_config_params_->monitor_stat_low_threshold_;
    int64_t middle_threshold = trans_state_.mysql_config_params_->monitor_stat_middle_threshold_;
    int64_t high_threshold = trans_state_.mysql_config_params_->monitor_stat_high_threshold_;

    DRWLock::RDLockGuard lock(item->get_lock());
    item->atomic_add(STAT_TOTAL_COUNT);
    item->atomic_add(STAT_TOTAL_TIME, hrtime_to_usec(cmd_time_stats_.request_total_time_));
    item->atomic_add(STAT_PREPARE_TIME, hrtime_to_usec(cmd_time_stats_.prepare_send_request_to_server_time_));
    item->atomic_add(STAT_SERVER_TIME, hrtime_to_usec(cmd_time_stats_.server_process_request_time_));

    if (cmd_time_stats_.request_total_time_ >= high_threshold) {
      item->atomic_add(STAT_HIGH_COUNT);
    } else if (cmd_time_stats_.request_total_time_ >= middle_threshold) {
      item->atomic_add(STAT_MIDDLE_COUNT);
    } else if (cmd_time_stats_.request_total_time_ >= low_threshold) {
      item->atomic_add(STAT_LOW_COUNT);
    }

    LOG_DEBUG("succ to update tenant stats", K(logic_tenant_name), K(logic_database_name),
              K(cluster_name), K(tenant_name), K(database_name),
              K(database_type), K(error_code), KPC(item));
  }

  get_global_tenant_stat_mgr().revert_item(item);
}

inline void ObMysqlSM::get_monitor_error_info(int32_t &error_code, ObString &error_msg, bool &is_error_resp)
{
  const char *msg = NULL;

  if (ObMysqlTransact::TRANSACTION_COMPLETE == trans_state_.current_.state_ || ObMysqlTransact::CMD_COMPLETE == trans_state_.current_.state_) {
    ObRespAnalyzeResult &resp = trans_state_.trans_info_.server_response_.get_analyze_result();
    if (resp.is_resp_completed_) {
      is_error_resp = resp.is_error_resp();
      if (is_error_resp) {
        error_code = resp.get_error_code();
        error_msg = resp.get_error_message();
      }
    } else if (trans_state_.inner_errcode_ != 0) {
      is_error_resp = true;
      error_code = trans_state_.inner_errcode_;
      msg = trans_state_.inner_errmsg_ != NULL ? trans_state_.inner_errmsg_ : ob_strerror(trans_state_.inner_errcode_);
      error_msg.assign_ptr(msg, static_cast<int32_t>(STRLEN(msg)));
    }
  } else if (OB_NOT_NULL(client_entry_) && client_entry_->eos_) {
    if (0 != cmd_size_stats_.client_request_bytes_) {
      is_error_resp = true;
      error_code = ER_QUERY_INTERRUPTED;
      msg = ob_strerror(OB_ERR_QUERY_INTERRUPTED);
      error_msg.assign_ptr(msg, static_cast<int32_t>(STRLEN(msg)));
    }
  } else if (OB_MYSQL_COM_QUIT != trans_state_.trans_info_.sql_cmd_) {
    is_error_resp = true;
    if (ObMysqlTransact::ACTIVE_TIMEOUT == trans_state_.current_.state_
            || ObMysqlTransact::INACTIVE_TIMEOUT == trans_state_.current_.state_) {
      error_code = ER_NET_READ_INTERRUPTED;
      msg = "Got timeout reading communication packets";
    } else {
      if (ObMysqlTransact::INTERNAL_ERROR == trans_state_.current_.state_) {
        error_code = trans_state_.inner_errcode_;
      } else if (ObMysqlTransact::CONNECTION_ERROR == trans_state_.current_.state_
              || ObMysqlTransact::CONNECT_ERROR == trans_state_.current_.state_) {
        error_code = OB_CONNECT_ERROR;
      } else {
        error_code = OB_ERR_UNEXPECTED;
      }
      msg = trans_state_.inner_errmsg_ != NULL ? trans_state_.inner_errmsg_ : ob_strerror(trans_state_.inner_errcode_);
    }
    error_msg.assign_ptr(msg, static_cast<int32_t>(STRLEN(msg)));
  }
}

#define MONITOR_LOG_FORMAT "%.*s,%.*s,%.*s," \
                        "%.*s,%.*s:%.*s:%.*s,%s,"  \
                        "%.*s,%.*s,%s,%s,%s,%s,%.*s,"  \
                        "%ldus,%ldus,%dus,%ldus," \
                        TRACE_ID_FORMAT ",%s,%s," \
                        "%.*s,%s,%s"

#define MONITOR_ERROR_LOG_FORMAT MONITOR_LOG_FORMAT ",%.*s"

#define MONITOR_LOG_PARAM \
          logic_tenant_name.length(), logic_tenant_name.ptr(),     \
          ant_trace_id.length(), ant_trace_id.ptr(),           \
          rpc_id.length(), rpc_id.ptr(),               \
                                                       \
          logic_database_name.length(), logic_database_name.ptr(), \
          cluster_name.length(), cluster_name.ptr(),   \
          tenant_name.length(), tenant_name.ptr(),     \
          database_name.length(), database_name.ptr(), \
          database_type_str,                           \
                                                       \
          logic_table_name.length(), logic_table_name.ptr(),     \
          table_name.length(), table_name.ptr(),       \
          sql_cmd,                                     \
          stmt_type_str,                               \
          is_error_resp? "failed" : "success",         \
          is_error_resp? error_code_str : "",          \
          new_sql.length(), new_sql.ptr(),             \
                                                       \
          hrtime_to_usec(cmd_time_stats_.request_total_time_),       \
          hrtime_to_usec(cmd_time_stats_.prepare_send_request_to_server_time_),  \
          0,                                           \
          hrtime_to_usec(cmd_time_stats_.server_process_request_time_),    \
                                                                           \
          trace_id_0, trace_id_1, "", "",              \
          shard_name.length(), shard_name.ptr(),       \
          is_enc_beyond_trust ? "1" : "0",             \
          ip_port_buff

#define MONITOR_ERROR_LOG_PARAM MONITOR_LOG_PARAM, error_msg.length(), error_msg.ptr()


inline void ObMysqlSM::update_monitor_log()
{
  if (OB_NOT_NULL(client_session_)
      && !client_session_->is_proxy_mysql_client_
      && !client_session_->is_proxysys_tenant()
      && OB_MYSQL_COM_HANDSHAKE != trans_state_.trans_info_.sql_cmd_
      && OB_MYSQL_COM_END != trans_state_.trans_info_.sql_cmd_
      && OB_MYSQL_COM_QUIT != trans_state_.trans_info_.sql_cmd_) {
    int64_t slow_time_threshold = trans_state_.mysql_config_params_->slow_query_time_threshold_;
    int64_t query_digest_time_threshold = trans_state_.mysql_config_params_->query_digest_time_threshold_;
    bool is_error_resp = false;
    int error_code = 0;
    ObString error_msg;
    get_monitor_error_info(error_code, error_msg, is_error_resp);

    if ((query_digest_time_threshold > 0 && query_digest_time_threshold < cmd_time_stats_.request_total_time_)
        || (slow_time_threshold > 0 && slow_time_threshold < cmd_time_stats_.request_total_time_)
        || is_error_resp
        || get_global_proxy_config().enable_monitor_stat
        || (get_global_proxy_config().enable_qos && OB_MYSQL_COM_QUERY == trans_state_.trans_info_.sql_cmd_)
        || (get_global_proxy_config().enable_prometheus && g_ob_prometheus_processor.is_inited())) {

      ObClientSessionInfo &cs_info = client_session_->get_session_info();
      ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
      ObSqlParseResult &parse_result = client_request.get_parse_result();

      ObString logic_tenant_name;
      ObString logic_database_name;
      cs_info.get_logic_tenant_name(logic_tenant_name);
      if (OB_UNLIKELY(logic_tenant_name.empty())) {
        logic_tenant_name.assign_ptr(get_global_proxy_config().app_name_str_,
                                     static_cast<int32_t>(STRLEN(get_global_proxy_config().app_name_str_)));
      }
      if (cs_info.is_sharding_user()) {
        if (OB_MYSQL_COM_INIT_DB == trans_state_.trans_info_.sql_cmd_) {
          logic_database_name = client_request.get_sql();
        } else {
          logic_database_name = parse_result.get_origin_database_name();
          if (OB_UNLIKELY(logic_database_name.empty())) {
            cs_info.get_logic_database_name(logic_database_name);
          }
        }
      }

      ObString cluster_name;
      ObString tenant_name;
      ObString database_name;
      ObString user_name;
      // in sharding mode, only need logic schema
      if (!cs_info.is_sharding_user() || !ObMysqlTransact::is_internal_request(trans_state_)) {
        cs_info.get_cluster_name(cluster_name);
        cs_info.get_tenant_name(tenant_name);
        cs_info.get_user_name(user_name);

        if (OB_MYSQL_COM_INIT_DB == trans_state_.trans_info_.sql_cmd_) {
          database_name = client_request.get_sql();
        } else {
          database_name = parse_result.get_database_name();
          if (OB_UNLIKELY(database_name.empty())) {
            cs_info.get_database_name(database_name);
          }
        }
      }

      const DBServerType database_type = cs_info.get_server_type();
      const ObString &table_name = parse_result.get_table_name();
      ObProxyBasicStmtType stmt_type = parse_result.get_stmt_type();
      bool is_slow_query = false;
      char error_code_str[OB_MAX_ERROR_CODE_LEN] = "\0";

      if ((query_digest_time_threshold > 0 && query_digest_time_threshold < cmd_time_stats_.request_total_time_)
          || (slow_time_threshold > 0 && slow_time_threshold < cmd_time_stats_.request_total_time_)
          || is_error_resp) {
        const ObString ant_trace_id = parse_result.get_trace_id();
        const ObString rpc_id = parse_result.get_rpc_id();
        const char *database_type_str = ObProxyMonitorUtils::get_database_type_name(database_type);
        const ObString &logic_table_name = parse_result.get_origin_table_name();
        const char *sql_cmd = ObProxyParserUtils::get_sql_cmd_name(trans_state_.trans_info_.sql_cmd_);

        // for compatible sharding mode
        bool is_enc_beyond_trust = false;
        ObString shard_name;
        if (NULL != cs_info.get_shard_connector()) {
          shard_name = cs_info.get_shard_connector()->shard_name_.config_string_;
          is_enc_beyond_trust = cs_info.get_shard_connector()->is_enc_beyond_trust();
        }

        const char *stmt_type_str = "";
        ObString new_sql;
        char new_sql_buf[PRINT_SQL_LEN] = "\0";
        if (OB_MYSQL_COM_QUERY == trans_state_.trans_info_.sql_cmd_
            || OB_MYSQL_COM_STMT_PREPARE == trans_state_.trans_info_.sql_cmd_
            || OB_MYSQL_COM_STMT_PREPARE_EXECUTE == trans_state_.trans_info_.sql_cmd_) {
          stmt_type_str = get_print_stmt_name(stmt_type);

          const ObString &origin_sql = trans_state_.trans_info_.get_print_sql();
          int32_t new_sql_len = 0;
          ObProxyMonitorUtils::sql_escape(origin_sql.ptr(), origin_sql.length(),
                                          new_sql_buf, PRINT_SQL_LEN, new_sql_len);
          new_sql.assign_ptr(new_sql_buf, new_sql_len);
        }

        const uint64_t *trace_id = ObCurTraceId::get();
        uint64_t trace_id_0 = (OB_ISNULL(trace_id)) ? OB_INVALID_ID : trace_id[0];
        uint64_t trace_id_1 = (OB_ISNULL(trace_id)) ? OB_INVALID_ID : trace_id[1];

        if (is_error_resp) {
          snprintf(error_code_str, OB_MAX_ERROR_CODE_LEN, "%d", error_code);
        }

        char ip_port_buff[INET6_ADDRPORTSTRLEN] = "\0";
        ObIpEndpoint &server_addr = trans_state_.server_info_.addr_;
        if (ops_is_ip(server_addr)) {
          char ip_buff_temp[INET6_ADDRSTRLEN] = "\0";
          snprintf(ip_port_buff, INET6_ADDRPORTSTRLEN, "%s:%u", ops_ip_ntop(server_addr, ip_buff_temp, INET6_ADDRSTRLEN), ops_ip_port_host_order(server_addr));
        }

        if ((slow_time_threshold > 0 && slow_time_threshold < cmd_time_stats_.request_total_time_)
            || is_error_resp) {
          _OBPROXY_DIGEST_LOG(WARN, MONITOR_LOG_FORMAT, MONITOR_LOG_PARAM);
        } else if (query_digest_time_threshold > 0 && query_digest_time_threshold < cmd_time_stats_.request_total_time_) {
          _OBPROXY_DIGEST_LOG(INFO, MONITOR_LOG_FORMAT, MONITOR_LOG_PARAM);
        }

        if (slow_time_threshold > 0 && slow_time_threshold < cmd_time_stats_.request_total_time_) {
          is_slow_query = true;
          _OBPROXY_SLOW_LOG(WARN, MONITOR_LOG_FORMAT, MONITOR_LOG_PARAM);
        }

        if (is_error_resp) {
          _OBPROXY_ERROR_LOG(WARN, MONITOR_ERROR_LOG_FORMAT, MONITOR_ERROR_LOG_PARAM);
        }
      }

      if (get_global_proxy_config().enable_monitor_stat) {
        if (OB_MYSQL_COM_LOGIN == trans_state_.trans_info_.sql_cmd_) {
          stmt_type = OBPROXY_T_LOGIN;
        }
        update_monitor_stats(logic_tenant_name, logic_database_name,
                             cluster_name, tenant_name, database_name,
                             database_type, stmt_type, error_code_str);
      }

      // Temporarily only supports OB_MYSQL_COM_QUERY
      // TODO: support ps sql
      if (get_global_proxy_config().enable_qos && OB_MYSQL_COM_QUERY == trans_state_.trans_info_.sql_cmd_) {
        int32_t table_name_length = table_name.length();
        ObString new_table_name;
        if (trans_stats_.is_in_testload_trans_ && !ObMysqlTransact::is_in_trans(trans_state_)) {
          new_table_name.assign_ptr(ObProxyQosStatProcessor::TESTLOAD_TABLE_NAME,
              static_cast<int32_t>(strlen(ObProxyQosStatProcessor::TESTLOAD_TABLE_NAME)));
          trans_stats_.is_in_testload_trans_ = false;
        } else if (table_name_length > 2) {
          if ((table_name[table_name_length - 1] == 't' || table_name[table_name_length - 1] == 'T')
              && (table_name[table_name_length - 2] == '_')) {
            new_table_name.assign_ptr(ObProxyQosStatProcessor::TESTLOAD_TABLE_NAME,
                              static_cast<int32_t>(strlen(ObProxyQosStatProcessor::TESTLOAD_TABLE_NAME)));
            if (ObMysqlTransact::is_in_trans(trans_state_)) {
              trans_stats_.is_in_testload_trans_ = true;
            }
          }
        }
        g_ob_qos_stat_processor.store_stat(cluster_name, tenant_name, database_name, user_name, new_table_name,
                                           hrtime_to_usec(cmd_time_stats_.request_total_time_));
      }

      SQL_PROMETHEUS_STAT(logic_tenant_name, logic_database_name,
                          cluster_name, tenant_name, database_name,
                          stmt_type, PROMETHEUS_REQUEST_COUNT, is_slow_query, is_error_resp);
      SQL_PROMETHEUS_STAT(logic_tenant_name, logic_database_name,
                          cluster_name, tenant_name, database_name,
                          stmt_type, PROMETHEUS_REQUEST_TOTAL_TIME,
                          hrtime_to_usec(cmd_time_stats_.request_total_time_));
      SQL_PROMETHEUS_STAT(logic_tenant_name, logic_database_name,
                          cluster_name, tenant_name, database_name,
                          stmt_type, PROMETHEUS_SERVER_PROCESS_REQUEST_TIME,
                          hrtime_to_usec(cmd_time_stats_.server_process_request_time_));
      SQL_PROMETHEUS_STAT(logic_tenant_name, logic_database_name,
                          cluster_name, tenant_name, database_name,
                          stmt_type, PROMETHEUS_PREPARE_SEND_REQUEST_TIME,
                          hrtime_to_usec(cmd_time_stats_.prepare_send_request_to_server_time_));

      NET_PROMETHEUS_STAT(logic_tenant_name, logic_database_name,
                          cluster_name, tenant_name, database_name,
                          PROMETHEUS_REQUEST_BYTE, true, true,
                          cmd_size_stats_.client_request_bytes_);
      NET_PROMETHEUS_STAT(logic_tenant_name, logic_database_name,
                          cluster_name, tenant_name, database_name,
                          PROMETHEUS_REQUEST_BYTE, true, false,
                          cmd_size_stats_.server_request_bytes_);
      NET_PROMETHEUS_STAT(logic_tenant_name, logic_database_name,
                          cluster_name, tenant_name, database_name,
                          PROMETHEUS_REQUEST_BYTE, false, true,
                          cmd_size_stats_.client_response_bytes_);
      NET_PROMETHEUS_STAT(logic_tenant_name, logic_database_name,
                          cluster_name, tenant_name, database_name,
                          PROMETHEUS_REQUEST_BYTE, false, false,
                          cmd_size_stats_.server_response_bytes_);
    }
  }
}

inline void ObMysqlSM::update_cmd_stats()
{
  trans_stats_.client_request_bytes_ += cmd_size_stats_.client_request_bytes_;
  trans_stats_.server_request_bytes_ += cmd_size_stats_.server_request_bytes_;
  trans_stats_.server_response_bytes_ += cmd_size_stats_.server_response_bytes_;
  trans_stats_.client_response_bytes_ += cmd_size_stats_.client_response_bytes_;

  // client_close will not be assigned properly in some exceptional situation.
  // TODO: Assign client_close with suitable value when ObMysqlTunnel terminates abnormally.
  if (0 == milestones_.client_.client_end_ && milestones_.client_.client_read_end_ > 0) {
    milestones_.client_.client_end_ = get_based_hrtime();
  }

  if (0 == milestones_.last_client_cmd_end_) {
    milestones_.last_client_cmd_end_ = milestones_.client_.client_end_;
  } else {
    cmd_time_stats_.client_transaction_idle_time_ =
      milestone_diff(milestones_.last_client_cmd_end_, milestones_.client_.client_begin_);
    milestones_.last_client_cmd_end_ = milestones_.client_.client_end_;
  }
  trans_stats_.client_transaction_idle_time_ += cmd_time_stats_.client_transaction_idle_time_;

  trans_stats_.client_request_read_time_ += cmd_time_stats_.client_request_read_time_;
  trans_stats_.server_request_write_time_ += cmd_time_stats_.server_request_write_time_;
  trans_stats_.client_process_request_time_ += (cmd_time_stats_.client_request_read_time_
                                                + cmd_time_stats_.server_request_write_time_);
  cmd_time_stats_.server_connect_time_ =
    milestone_diff(milestones_.server_connect_begin_, milestones_.server_connect_end_);

  trans_stats_.client_request_analyze_time_ += cmd_time_stats_.client_request_analyze_time_;

  trans_stats_.cluster_resource_create_time_ += cmd_time_stats_.cluster_resource_create_time_;

  trans_stats_.pl_lookup_time_ += cmd_time_stats_.pl_lookup_time_;

  trans_stats_.congestion_control_time_ += cmd_time_stats_.congestion_control_time_;

  trans_stats_.prepare_send_request_to_server_time_ += cmd_time_stats_.prepare_send_request_to_server_time_;

  trans_stats_.server_process_request_time_ += cmd_time_stats_.server_process_request_time_;

  trans_stats_.server_response_read_time_ += cmd_time_stats_.server_response_read_time_;

  trans_stats_.client_response_write_time_ += cmd_time_stats_.client_response_write_time_;

  trans_stats_.server_response_analyze_time_ += cmd_time_stats_.server_response_analyze_time_;
  trans_stats_.ok_packet_trim_time_ += cmd_time_stats_.ok_packet_trim_time_;

  cmd_time_stats_.request_total_time_ =
    milestone_diff(milestones_.client_.client_begin_, milestones_.client_.client_end_);

  trans_stats_.plugin_compress_request_time_ += cmd_time_stats_.plugin_compress_request_time_;
  trans_stats_.plugin_decompress_response_time_ += cmd_time_stats_.plugin_decompress_response_time_;
  trans_stats_.pl_process_time_ += cmd_time_stats_.pl_process_time_;
  trans_stats_.congestion_process_time_ += cmd_time_stats_.congestion_process_time_;
  trans_stats_.do_observer_open_time_ += cmd_time_stats_.do_observer_open_time_;
  trans_stats_.sync_session_variable_time_ += cmd_time_stats_.server_sync_session_variable_time_;
  trans_stats_.send_saved_login_time_ += cmd_time_stats_.server_send_saved_login_time_;
  trans_stats_.send_all_session_vars_time_ += cmd_time_stats_.server_send_all_session_variable_time_;
  trans_stats_.send_use_database_time_ += cmd_time_stats_.server_send_use_database_time_;
  trans_stats_.send_session_vars_time_ += cmd_time_stats_.server_send_session_variable_time_;
  trans_stats_.send_last_insert_id_time_ += cmd_time_stats_.server_send_last_insert_id_time_;
  trans_stats_.send_start_trans_time_ += cmd_time_stats_.server_send_start_trans_time_;
  trans_stats_.build_server_request_time_ += cmd_time_stats_.build_server_request_time_;

  int64_t slow_time_threshold = trans_state_.mysql_config_params_->slow_query_time_threshold_;
  int64_t proxy_process_time_threshold = trans_state_.mysql_config_params_->slow_proxy_process_time_threshold_;
  const char *SLOW_QUERY = "Slow Query: ";
  const char *TRACE_STAT = "Trace Stat: ";
  const char *xf_head = NULL;
  const char *log_head = NULL;
  bool print_info_log = false;

  if (slow_time_threshold > 0 && slow_time_threshold < cmd_time_stats_.request_total_time_) {
    xf_head = XFH_SQL_SLOW_QUERY;
    log_head = SLOW_QUERY;
  } else if (proxy_process_time_threshold > 0 && proxy_process_time_threshold < cmd_time_stats_.prepare_send_request_to_server_time_) {
    xf_head = XFH_SQL_SLOW_QUERY;
    log_head = SLOW_QUERY;
    print_info_log = true;
  } else if (need_print_trace_stat()) {
    log_head = TRACE_STAT;
    print_info_log = true;
  } else {
    //do nothing
  }
  if (NULL != log_head) {
    uint64_t proxy_sessid = 0;
    uint32_t cs_id = 0;
    uint32_t server_sessid = 0;
    int64_t ss_id = 0;
    ObProxyProtocol ob_proxy_protocol = use_compression_protocol();
    ObString user_name;
    ObString tenant_name;
    ObString cluster_name;
    ObString logic_tenant_name;
    ObString logic_database_name;
    if (NULL != client_session_) {
      proxy_sessid = client_session_->get_proxy_sessid();
      cs_id = client_session_->get_cs_id();
      const ObClientSessionInfo &cs_info = client_session_->get_session_info();
      cs_info.get_user_name(user_name);
      cs_info.get_tenant_name(tenant_name);
      cs_info.get_cluster_name(cluster_name);
      cs_info.get_logic_database_name(logic_database_name);
      cs_info.get_logic_tenant_name(logic_tenant_name);
    }
    get_server_session_ids(server_sessid, ss_id);

    if (print_info_log) {
      //print info
      LOG_INFO(log_head,
               "client_ip", trans_state_.client_info_.addr_,
               "server_ip", trans_state_.server_info_.addr_,
               "server_trace_id", get_server_trace_id(),
               "route_type", get_route_type_string(trans_state_.pll_info_.route_.cur_chosen_route_type_),
               K(user_name),
               K(tenant_name),
               K(cluster_name),
               K(logic_database_name),
               K(logic_tenant_name),
               K(ob_proxy_protocol),
               K(cs_id),
               K(proxy_sessid),
               K(ss_id),
               K(server_sessid),
               K_(sm_id),
               K_(cmd_size_stats),
               K_(cmd_time_stats),
               "sql", trans_state_.trans_info_.get_print_sql());
    } else {
      LOG_WARN(log_head,
               "client_ip", trans_state_.client_info_.addr_,
               "server_ip", trans_state_.server_info_.addr_,
               "server_trace_id", get_server_trace_id(),
               "route_type", get_route_type_string(trans_state_.pll_info_.route_.cur_chosen_route_type_),
               K(user_name),
               K(tenant_name),
               K(cluster_name),
               K(logic_database_name),
               K(logic_tenant_name),
               K(ob_proxy_protocol),
               K(cs_id),
               K(proxy_sessid),
               K(ss_id),
               K(server_sessid),
               K_(sm_id),
               K_(cmd_size_stats),
               K_(cmd_time_stats),
               "sql", trans_state_.trans_info_.get_print_sql());
    }

    if (NULL != xf_head) {
      OBPROXY_XF_LOG(INFO, xf_head,
                     "client_ip", trans_state_.client_info_.addr_,
                     "server_ip", trans_state_.server_info_.addr_,
                     "route_type", get_route_type_string(trans_state_.pll_info_.route_.cur_chosen_route_type_),
                     K(user_name),
                     K(tenant_name),
                     K(cluster_name),
                     K(logic_database_name),
                     K(logic_tenant_name),
                     K(ob_proxy_protocol),
                     "sql", trans_state_.trans_info_.get_print_sql(),
                     K_(cmd_size_stats),
                     K_(cmd_time_stats));
    }
  }

  update_monitor_log();

  if (trans_state_.need_sqlaudit()) {
    trans_state_.sqlaudit_record_queue_->enqueue(static_cast<int64_t>(sm_id_),
        milestones_.client_.client_begin_, cmd_time_stats_, trans_state_.server_info_.addr_,
        trans_state_.trans_info_.get_print_sql(), trans_state_.trans_info_.sql_cmd_);
  }

  ObMysqlTransact::client_result_stat(trans_state_);

  if (trans_state_.mysql_config_params_->enable_trans_detail_stats_) {
    ObMysqlTransact::histogram_request_size(trans_state_, cmd_size_stats_.client_request_bytes_);
    ObMysqlTransact::histogram_response_size(trans_state_, cmd_size_stats_.server_response_bytes_);
    if (trans_stats_.client_response_write_time_ > 0 && cmd_size_stats_.client_response_bytes_ > 0) {
      ObMysqlTransact::client_connection_speed(trans_state_, cmd_time_stats_.client_response_write_time_,
                                               cmd_size_stats_.client_response_bytes_);
    }

    if (trans_stats_.server_response_read_time_ > 0 && cmd_size_stats_.server_response_bytes_ > 0) {
      ObMysqlTransact::server_connection_speed(trans_state_, cmd_time_stats_.server_response_read_time_,
                                               cmd_size_stats_.server_response_bytes_);
    }
  }
}

inline void ObMysqlSM::update_stats()
{
  if (!is_updated_stat_) {
    update_cmd_stats();

    milestones_.trans_finish_ = get_based_hrtime();

    LOG_DEBUG("[ObMysqlSM::update_stats] Logging transaction", K_(sm_id));

    // count
    MYSQL_SUM_TRANS_STAT(CLIENT_REQUESTS, trans_stats_.client_requests_);
    MYSQL_SUM_TRANS_STAT(SERVER_RESPONSES, trans_stats_.server_responses_);
    MYSQL_SUM_TRANS_STAT(SERVER_PL_LOOKUP_RETRIES, trans_stats_.pl_lookup_retries_);
    MYSQL_SUM_TRANS_STAT(SERVER_CONNECT_RETRIES, trans_stats_.server_retries_);

    // sizes
    MYSQL_SUM_TRANS_STAT(CLIENT_REQUEST_TOTAL_SIZE, trans_stats_.client_request_bytes_);
    MYSQL_SUM_TRANS_STAT(CLIENT_RESPONSE_TOTAL_SIZE, trans_stats_.client_response_bytes_);
    if (trans_stats_.server_request_bytes_ > 0) {
      MYSQL_SUM_TRANS_STAT(SERVER_REQUEST_TOTAL_SIZE, trans_stats_.server_request_bytes_);
      MYSQL_SUM_TRANS_STAT(SERVER_RESPONSE_TOTAL_SIZE, trans_stats_.server_response_bytes_);
    }

    // time
    MYSQL_SUM_TRANS_STAT(TOTAL_CLIENT_REQUEST_READ_TIME, trans_stats_.client_process_request_time_);
    MYSQL_SUM_TRANS_STAT(TOTAL_CLIENT_RESPONSE_WRITE_TIME, trans_stats_.client_response_write_time_);
    MYSQL_SUM_TRANS_STAT(TOTAL_CLIENT_REQUEST_ANALYZE_TIME, trans_stats_.client_request_analyze_time_);
    MYSQL_SUM_TRANS_STAT(TOTAL_CLIENT_TRANSACTION_IDLE_TIME, trans_stats_.client_transaction_idle_time_);
    MYSQL_SUM_TRANS_STAT(TOTAL_OK_PACKET_TRIM_TIME, trans_stats_.ok_packet_trim_time_);
    MYSQL_SUM_TRANS_STAT(TOTAL_SERVER_PROCESS_REQUEST_TIME, trans_stats_.server_process_request_time_);
    MYSQL_SUM_TRANS_STAT(TOTAL_SERVER_RESPONSE_READ_TIME, trans_stats_.server_response_read_time_);
    MYSQL_SUM_TRANS_STAT(TOTAL_SERVER_RESPONSE_ANALYZE_TIME, trans_stats_.server_response_analyze_time_);

    trans_stats_.server_connect_time_ =
      milestone_diff(milestones_.server_connect_begin_, milestones_.server_connect_end_);
    trans_stats_.trans_time_ =
      milestone_diff(milestones_.trans_start_, milestones_.trans_finish_);

    if (trans_stats_.pl_lookup_time_ > 0) {
      MYSQL_SUM_TRANS_STAT(TOTAL_PL_LOOKUP_TIME, trans_stats_.pl_lookup_time_);
      MYSQL_INCREMENT_TRANS_STAT(SERVER_PL_LOOKUP_COUNT);
    }

    if (trans_stats_.congestion_control_time_ > 0) {
      MYSQL_SUM_TRANS_STAT(TOTAL_CONGESTION_CONTROL_LOOKUP_TIME, trans_stats_.congestion_control_time_);
    }

    if (trans_stats_.server_connect_time_ > 0) {
      MYSQL_SUM_TRANS_STAT(TOTAL_SERVER_CONNECT_TIME, trans_stats_.server_connect_time_);
      MYSQL_INCREMENT_TRANS_STAT(SERVER_CONNECT_COUNT);
    }

    if (trans_stats_.cluster_resource_create_time_ > 0) {
      RESOURCE_POOL_SUM_DYN_STAT(CREATE_CLUSTER_RESOURCE_TIME, trans_stats_.cluster_resource_create_time_);
    }

    MYSQL_SUM_TRANS_STAT(TOTAL_TRANSACTIONS_TIME, trans_stats_.trans_time_);

    if (ObMysqlTransact::is_user_trans_complete(trans_state_)) {
      MYSQL_SUM_TRANS_STAT(TOTAL_USER_TRANSACTIONS_TIME, trans_stats_.trans_time_);
      MYSQL_INCREMENT_DYN_STAT(TOTAL_USER_TRANSACTION_COUNT);
      if (client_session_ && !client_session_->is_proxy_mysql_client_ && !client_session_->is_proxysys_tenant()) {
        SESSION_PROMETHEUS_STAT(client_session_->get_session_info(), PROMETHEUS_TRANSACTION_COUNT);
      }
    }

    if (trans_state_.mysql_config_params_->slow_transaction_time_threshold_ > 0
        && trans_state_.mysql_config_params_->slow_transaction_time_threshold_ < trans_stats_.trans_time_) {
      uint32_t cs_id = (OB_ISNULL(client_session_) ? 0 : client_session_->get_cs_id());
      uint64_t proxy_sessid = (OB_ISNULL(client_session_) ? 0 : client_session_->get_proxy_sessid());
      ObProxyProtocol ob_proxy_protocol = use_compression_protocol();
      int64_t ss_id = 0;
      uint32_t server_sessid = 0;

      get_server_session_ids(server_sessid, ss_id);

      LOG_WARN("Slow transaction: ",
               "client_ip", trans_state_.client_info_.addr_,
               "server_ip", trans_state_.server_info_.addr_,
               "server_trace_id", get_server_trace_id(),
               "route_type", get_route_type_string(trans_state_.pll_info_.route_.cur_chosen_route_type_),
               K(ob_proxy_protocol),
               K(cs_id),
               K(proxy_sessid),
               K(ss_id),
               K(server_sessid),
               K_(sm_id),
               "state", trans_state_.current_.state_,
               K(trans_stats_),
               "last_sql", trans_state_.trans_info_.get_print_sql());

      OBPROXY_XF_LOG(INFO, XFH_SQL_SLOW_TRX,
                     "client_ip", trans_state_.client_info_.addr_,
                     "server_ip", trans_state_.server_info_.addr_,
                     "sql", trans_state_.trans_info_.get_print_sql(),
                     "sql_cmd", get_mysql_cmd_str(trans_state_.trans_info_.sql_cmd_),
                     "route_type", get_route_type_string(trans_state_.pll_info_.route_.cur_chosen_route_type_),
                     K(ob_proxy_protocol),
                     K(trans_stats_));
    }
    is_updated_stat_ = true;
  }
}

void ObMysqlSM::update_session_stats(int64_t *stats, const int64_t stats_size)
{
  update_stats();
  trans_state_.record_transaction_stats(stats, stats_size);
}

int ObMysqlSM::swap_mutex(event::ObProxyMutex *mutex)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(mutex)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arguement", K_(sm_id), K(mutex), K(ret));
  } else if (OB_ISNULL(client_entry_) || OB_ISNULL(client_session_)
             || OB_UNLIKELY(NULL != server_entry_) || OB_UNLIKELY(NULL != server_session_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("invalid internal state, client session mustn't be NULL, server session must be NULL,",
             K_(sm_id), K_(client_entry), K_(client_session), K_(server_entry), K_(server_session), K(ret));
  } else {
    mutex_ = mutex;
    tunnel_.init(*this, *mutex);
    if (OB_FAIL(setup_client_request_read())) {
      LOG_WARN("failed to setup_client_request_read", K_(sm_id), K(ret));
    }
  }

  return ret;
}

int64_t ObStreamSizeStat::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(client_request_bytes),
       K_(server_request_bytes),
       K_(server_response_bytes),
       K_(client_response_bytes));
  J_OBJ_END();
  return pos;
}

// Debugging routine to dump the state machine's history
// and other state on an assertion failure
void ObMysqlSM::dump_history_state()
{
  int64_t hist_size = history_pos_;

  LOG_ERROR("------- begin mysql state dump -------", K_(sm_id));
  if (history_pos_ > HISTORY_SIZE) {
    hist_size = HISTORY_SIZE;
    LOG_ERROR("   History Wrap around", K_(history_pos));
  }

  // Loop through the history and dump it
  for (int64_t i = 0; i < hist_size; ++i) {
    _LOG_ERROR("%d   %d   %s", history_[i].event_, history_[i].reentrancy_, history_[i].file_line_);
  }
}

// This routine takes an ObMysqlTransact function <f>, calls the
// function to perform some actions on the current
// ObMysqlTransact::ObTransState, and then uses the ObMysqlTransact return action
// code to set the next handler (state) for the state machine.
// ObMysqlTransact could have returned the handler directly, but returns
// action codes in hopes of making a cleaner separation between the
// state machine and the ObMysqlTransact logic.
inline void ObMysqlSM::call_transact_and_set_next_state(TransactEntryFunc f)
{
  ObMysqlTransact::ObStateMachineActionType last_action = trans_state_.next_action_; // remember where we were

  // The callee can either specify a method to call in to Transact,
  // or call with NULL which indicates that Transact should use
  // its stored entry point.
  if (NULL == f) {
    if (OB_ISNULL(trans_state_.transact_return_point)) {
      LOG_ERROR("invalid internal state, transact_return_point is NULL", K_(sm_id));
    } else {
      trans_state_.transact_return_point(trans_state_);
    }
  } else {
    f(trans_state_);
  }

  LOG_DEBUG("State Transition:", K_(sm_id),
            "last_action", ObMysqlTransact::get_action_name(last_action),
            "next_action", ObMysqlTransact::get_action_name(trans_state_.next_action_));

  set_next_state();
}

// call_transact_and_set_next_state() was broken into two parts, one
// which calls the ObMysqlTransact method and the second which sets
// the next state. In a case which set_next_state() was not
// completed, the state function calls set_next_state() to retry
// setting the state.
inline void ObMysqlSM::set_next_state()
{
  int ret = OB_SUCCESS;
  // Use the returned "next action_" code to set the next state handler
  switch (trans_state_.next_action_) {
    case ObMysqlTransact::SM_ACTION_API_READ_REQUEST:
    case ObMysqlTransact::SM_ACTION_API_OBSERVER_PL:
    case ObMysqlTransact::SM_ACTION_API_SEND_REQUEST:
    case ObMysqlTransact::SM_ACTION_API_READ_RESPONSE:
    case ObMysqlTransact::SM_ACTION_API_SEND_RESPONSE:
      callout_api_and_start_next_action(trans_state_.next_action_);
      break;

    case ObMysqlTransact::SM_ACTION_SERVER_ADDR_LOOKUP:
      do_server_addr_lookup();
      break;

    case ObMysqlTransact::SM_ACTION_PARTITION_LOCATION_LOOKUP:
      do_partition_location_lookup();
      break;

    case ObMysqlTransact::SM_ACTION_CONGESTION_CONTROL_LOOKUP:
      do_congestion_control_lookup();
      break;

    case ObMysqlTransact::SM_ACTION_OBSERVER_OPEN: {
      do_observer_open();
      break;
    }
    case ObMysqlTransact::SM_ACTION_INTERNAL_REQUEST:
      do_internal_request();
      break;

    case ObMysqlTransact::SM_ACTION_SERVER_READ: {
      trans_state_.source_ = ObMysqlTransact::SOURCE_OBSERVER;

      if (NULL != api_.response_transform_info_.vc_) {
        if (OB_FAIL(api_.setup_server_transfer_to_transform())) {
          LOG_WARN("failed to setup_server_transfer_to_transform", K_(sm_id), K(ret));
          setup_error_transfer();
        }
      } else {
        callout_api_and_start_next_action(ObMysqlTransact::SM_ACTION_API_SEND_RESPONSE);
      }
      break;
    }

    case ObMysqlTransact::SM_ACTION_INTERNAL_NOOP: {
      if (NULL == server_entry_ || !server_entry_->in_tunnel_) {
        release_server_session();
      }

      // If we're in state SEND_API_RESPONSE, it means functions
      // registered to hook SEND_RESPONSE have already been called. So we do not
      // need to call do_api_callout. Otherwise obproxy loops infinitely in this
      // state !
      if (ObMysqlTransact::SM_ACTION_API_SEND_RESPONSE == trans_state_.api_next_action_) {
        handle_api_return();
      } else {
        callout_api_and_start_next_action(ObMysqlTransact::SM_ACTION_API_SEND_RESPONSE);
      }
      break;
    }

    case ObMysqlTransact::SM_ACTION_SEND_ERROR_NOOP:
      setup_error_transfer();
      break;

    case ObMysqlTransact::SM_ACTION_TRANSFORM_READ:
      callout_api_and_start_next_action(ObMysqlTransact::SM_ACTION_API_SEND_RESPONSE);
      break;

    default:
      LOG_ERROR("Unknown next action", K_(trans_state_.next_action), K_(sm_id));
      setup_error_transfer();
      break;
  }
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
