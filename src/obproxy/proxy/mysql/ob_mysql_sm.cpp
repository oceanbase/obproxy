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
#include "cmd/ob_show_create_table_handler.h"
#include "cmd/ob_select_database_handler.h"
#include "cmd/ob_show_topology_handler.h"
#include "cmd/ob_show_db_version_handler.h"
#include "cmd/ob_show_table_status_handler.h"
#include "obutils/ob_tenant_stat_manager.h"
#include "prometheus/ob_net_prometheus.h"
#include "prometheus/ob_sql_prometheus.h"
#include "lib/profile/ob_trace_id.h"
#include "obutils/ob_proxy_config.h"
#include "qos/ob_proxy_qos_stat_processor.h"
#include "obutils/ob_proxy_config_processor.h"
#include "proxy/mysqllib/ob_mysql_packet_rewriter.h"
#include "packet/ob_mysql_packet_writer.h"
#include "dbconfig/ob_proxy_db_config_info.h"
#include "optimizer/ob_proxy_optimizer_processor.h"
#include "dbconfig/ob_proxy_pb_utils.h"
#include "proxy/mysql/ob_mysql_global_session_manager.h"
#include "omt/ob_white_list_table_processor.h"
#include "omt/ob_conn_table_processor.h"
#include "omt/ob_ssl_config_table_processor.h"
#include "iocore/net/ob_inet.h"
#include "iocore/eventsystem/ob_shard_scan_all_task.h"
#include "prometheus/ob_thread_prometheus.h"
#include "ob_proxy_init.h"
#include "common/obsm_utils.h"
#include "proxy/mysqllib/ob_proxy_ob20_request.h"
#include "proxy/mysqllib/ob_resp_packet_analyze_result.h"
#include "obproxy/packet/ob_proxy_packet_writer.h"
#include "obproxy/cmd/ob_internal_cmd_processor.h"
#include "engine/ob_proxy_operator_cont.h"
#include "proxy/mysqllib/ob_resp_analyzer.h"

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
using namespace oceanbase::obproxy::omt;
using namespace oceanbase::proxy_protocol_v2;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
using namespace obmysql;

#define MYSQL_SUM_TRANS_STAT(X, S) { \
  if (OB_UNLIKELY(get_global_performance_params().enable_stat_)) { \
    ObMysqlTransact::update_stat(trans_state_, X, S); \
  } \
}

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

#define MYSQL_INCREMENT_TRANS_STAT(X) { \
  if (OB_UNLIKELY(get_global_performance_params().enable_stat_)) { \
    ObMysqlTransact::update_stat(trans_state_, X, 1); \
  } \
}

#define __REMEMBER(x)  #x
#define _REMEMBER(x)   __REMEMBER(x)

#define REMEMBER(e, r) {                                        \
    add_history_entry(__FILE__ ":" _REMEMBER (__LINE__), e, r); \
  }

#define STATE_ENTER(state_name, e) { \
  if (OB_UNLIKELY(!get_global_performance_params().enable_performance_mode_)) { \
    REMEMBER (e, reentrancy_count_); \
    int64_t stack_start = event::self_ethread().stack_start_; \
    _PROXY_SM_LOG(DEBUG, "sm_id=%u, stack_size=%ld, next_action=%s, event=%s", \
                  sm_id_, stack_start - reinterpret_cast<int64_t>(&stack_start), \
                  #state_name, ObMysqlDebugNames::get_event_name(e)); } \
  }

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
      retry_acquire_server_session_count_(0), start_acquire_server_session_time_(0),
      skip_plugin_(false), add_detect_server_cnt_(false), proxy_protocol_v2_(),
      server_protocol_(ObProxyProtocol::PROTOCOL_NORMAL), need_update_non_login_config_(false),
      single_leader_version_(0),
      need_depend_last_session_(false), enable_full_link_trace_(false),
      kill_after_cmd_done_err_code_(0),
      multi_level_config_(NULL), route_diagnosis_(NULL), protocol_diagnosis_(NULL),
      connection_diagnosis_trace_(NULL), service_name_instance_(NULL)
{
  static bool scatter_inited = false;
  if (!scatter_inited) {
    make_scatter_list(*this);
    scatter_inited = true;
  }
  reset_single_leader();
}

inline void ObMysqlSM::cleanup()
{
  // release the memory refered by the members
  resp_analyzer_.reset();
  DEC_SHARED_REF(resp_analyzer_.get_protocol_diagnosis_ref());

  api_.destroy();
  trans_state_.destroy();
  mutex_.release();
  tunnel_.mutex_.release();
  magic_ = MYSQL_SM_MAGIC_DEAD;
  if (OB_NOT_NULL(target_db_server_)) {
    op_free(target_db_server_);
    target_db_server_ = NULL;
  }
  DEC_SHARED_REF(sm_cluster_resource_);
  DEC_SHARED_REF(route_diagnosis_);
  DEC_SHARED_REF(protocol_diagnosis_);
  if (OB_NOT_NULL(multi_level_config_)) {
    multi_level_config_->dec_ref();
  }
  DEC_SHARED_REF(service_name_instance_);
  flt_.reset();         // show trace mem managed by thread allocator, reset it!
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
    LOG_WDIAG("failed to init transaction state", K_(sm_id), K(ret));
  } else {
    magic_ = MYSQL_SM_MAGIC_ALIVE;
    sm_id_ = get_next_sm_id();
    api_.sm_ = this;
    SET_HANDLER(&ObMysqlSM::main_handler);
    if (ObConnectionDiagnosisTrace::is_enable_diagnosis_log(get_global_proxy_config().connection_diagnosis_option) && OB_ISNULL(connection_diagnosis_trace_)) {
      if (OB_ISNULL(connection_diagnosis_trace_ = op_alloc(ObConnectionDiagnosisTrace))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to alloc mem for diagnosis trace", K(ret));
      } else {
        connection_diagnosis_trace_->inc_ref();
      }
    }
#ifdef USE_MYSQL_DEBUG_LISTS
    if (OB_SUCCESS == mutex_acquire(&g_debug_sm_list_mutex)) {
      g_debug_sm_list.push(this);
      if (OB_SUCCESS != mutex_release(&g_debug_sm_list_mutex)) {
        LOG_WDIAG("failed to release mutex", K_(sm_id));
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
  if (get_global_proxy_config().enable_mysqlsm_info) {
    if (is_in_list_) {
      terminate_sm_ = true;
      LOG_EDIAG("state_add_to_list, it should not arrive here again", K_(is_in_list), K_(sm_id));
    } else if (OB_UNLIKELY(EVENT_NONE != event) && OB_UNLIKELY(EVENT_INTERVAL != event)) {
      terminate_sm_ = true;
      LOG_EDIAG("state_add_to_list, unexpected event", K(event), K_(sm_id));
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
      LOG_EDIAG("state_remove_from_list, unexpected event", K(event), K_(sm_id));
    } else {
      int64_t bucket = (sm_id_ % MYSQL_SM_LIST_BUCKETS);
      MUTEX_TRY_LOCK(lock, g_mysqlsm_list[bucket].mutex_, mutex_->thread_holding_);
      if (!lock.is_locked()) {
        MYSQL_SM_SET_DEFAULT_HANDLER(&ObMysqlSM::state_remove_from_list);
        if (OB_ISNULL(pending_action_ = mutex_->thread_holding_->schedule_in(this, HRTIME_MSECONDS(1)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("fail to schedule in", K_(sm_id), K(ret));
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
    if (OB_UNLIKELY(sm_cluster_resource_ != client_session_->cluster_resource_
        && NULL != client_session_->cluster_resource_)) {
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
    LOG_WDIAG("client session is in half close state", K_(sm_id), K(ret));
  } else {
    milestones_.client_.client_begin_ = get_based_hrtime();
    client_session_ = &client_vc;
    mutex_ = client_vc.mutex_;

    refresh_cluster_resource();
    tunnel_.init(*this, *mutex_);

    // assign dummy entry valid time
    client_session_->dummy_entry_valid_time_ns_ = ObRandomNumUtils::get_random_half_to_full(
        get_global_proxy_config().tenant_location_valid_time * 1000);
    if (connection_diagnosis_trace_ != NULL) {
      connection_diagnosis_trace_->set_user_client(!client_vc.is_proxy_mysql_client_);
      connection_diagnosis_trace_->set_is_detect_user(client_vc.get_session_info().get_priv_info().user_name_ == ObProxyTableInfo::DETECT_USERNAME_USER);
    }

    // Allocate a client entry in the state machine's vc table
    client_entry_ = vc_table_.new_entry();
    if (OB_ISNULL(client_entry_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("failed to new client entry", K_(sm_id), K(ret));
    } else {
      client_entry_->vc_ = &client_vc;
      client_entry_->vc_type_ = MYSQL_CLIENT_VC;

      if (OB_ISNULL(netvc = client_vc.get_netvc())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("net vconnection of client session is NULL", K_(sm_id), K(ret));
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
          LOG_WDIAG("client sesion failed to do io read", K_(client_entry_->read_vio), K_(sm_id), K(ret));
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
      LOG_EDIAG("invalid internal state", K_(reentrancy_count), K_(sm_id));
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
  } else if (trans_state_.is_auth_request_) {
    // is_proxy_mysql_client_ 情况下不走 ppv2 协议
    trans_state_.is_proxy_protocol_v2_request_ = false;
  }

  // The request may already be in the buffer if
  // this a request from a keep-alive connection
  // 无法区分客户端第一个报文是SSL request还是login，所以保守读取报头
  int64_t read_num = trans_state_.is_auth_request_ ? 36 : INT64_MAX;

  // 如果开启了 ppv2，先读取报头的 16 个字节
  if (trans_state_.is_proxy_protocol_v2_request_) {
    read_num = ProxyProtocolV2::PROXY_PROTOCOL_V2_HEADER_LEN;
    client_buffer_reader_->mbuf_->water_mark_ = ProxyProtocolV2::PROXY_PROTOCOL_V2_HEADER_LEN;
  }

  if (OB_ISNULL(client_entry_->read_vio_ = client_session_->do_io_read(
              this, read_num, client_buffer_reader_->mbuf_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("client session failed to do_io_read", K_(sm_id), K(ret));
  } else {
    if (OB_SUCC(ret)) {
      int64_t read_avail = client_buffer_reader_->read_avail();
      if (read_avail > 0
          || (trans_state_.is_auth_request_ && trans_state_.is_trans_first_request_)) {
        LOG_DEBUG("the request already in buffer, continue to handle it",
                 "buffer len", read_avail, "is_auth_rquest", trans_state_.is_auth_request_);
        handle_event(VC_EVENT_READ_READY, client_entry_->read_vio_);
      }
    }
  }
  return ret;
}

int ObMysqlSM::state_client_request_read(int event, void *data)
{
  int ret = OB_SUCCESS;
  int event_ret = VC_EVENT_NONE;

  STATE_ENTER(ObMysqlSM::state_client_request_read, event);

  if (event == VC_EVENT_INACTIVITY_TIMEOUT) {
    // if event == INACTIVITY_TIMEOUT, record timeout info before set_client_net_read_timeout
    COLLECT_TIMEOUT_DIAGNOSIS(
        connection_diagnosis_trace_, OB_TIMEOUT_DISCONNECT_TRACE,
        client_session_ == NULL
            ? obutils::OB_TIMEOUT_UNKNOWN_EVENT
            : client_session_->get_inactivity_timeout_event(),
        client_session_ == NULL ? 0 : client_session_->get_timeout_record(),
        OB_PROXY_INACTIVITY_TIMEOUT, NULL);
  }
  // set net_read_timeout when client begin to read
  set_client_net_read_timeout();
  client_session_->set_request_transferring(true);

  if (trans_state_.is_trans_first_request_) {
    if (OB_LIKELY(NULL != client_session_)) {
      client_session_->is_waiting_trans_first_request_ = false;
    }
    trans_state_.refresh_mysql_config();
    refresh_cluster_resource();
    if (OB_UNLIKELY(get_global_performance_params().enable_stat_)) {
      if (0 == milestones_.trans_start_) {
        MYSQL_INCREMENT_DYN_STAT(TOTAL_TRANSACTION_COUNT);
        MYSQL_INCREMENT_DYN_STAT(CURRENT_CLIENT_TRANSACTIONS);
      }
    }

    if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
      if (0 == milestones_.trans_start_) {
        milestones_.trans_start_ = get_based_hrtime();
      }
    }

    // record the config of enable_transaction_internal_routing
    client_session_->set_proxy_enable_trans_internal_routing(
        trans_state_.mysql_config_params_->enable_transaction_internal_routing_ &&
        (server_protocol_ == ObProxyProtocol::PROTOCOL_OB20));
    client_session_->set_proxy_enable_cross_shard_txn(
        get_global_proxy_config().enable_cross_shard_txn);
  }

  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
    if (0 == milestones_.client_.client_begin_) {
      milestones_.client_.client_begin_ = get_based_hrtime();
    }
  }

  if (OB_UNLIKELY(client_entry_->read_vio_ != reinterpret_cast<ObVIO *>(data)
      || (NULL != server_entry_)
      || (NULL != server_session_)
      || (client_entry_->eos_))) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WDIAG("invalid internal state", K_(client_entry_->read_vio), K(data), K_(server_entry),
             K_(server_session), K_(client_entry_->eos));
  } else {
    switch (event) {
      case VC_EVENT_READ_READY:
      case VC_EVENT_READ_COMPLETE:
        // More data to parse
        break;

      case VC_EVENT_EOS: {
        ObUnixNetVConnection* unix_vc = static_cast<ObUnixNetVConnection *>(client_session_->get_netvc());
        if (handling_ssl_request_ && unix_vc != NULL && unix_vc->get_ssl_err_code() != SSL_ERROR_NONE) {
          COLLECT_VC_DIAGNOSIS(
              connection_diagnosis_trace_, obutils::OB_CLIENT_VC_TRACE, event,
              OB_CLIENT_RECEIVING_PACKET_CONNECTION_ERROR,
              "ssl related error occured, error code:%d", unix_vc->get_ssl_err_code());
        } else if (OB_UNLIKELY(NULL != client_session_->get_closed_key_server_session())) {
          LOG_INFO("some key server session close, need close client session",
                   "key server session", client_session_->get_closed_key_server_session(), K_(kill_after_cmd_done_err_code));
          COLLECT_VC_DIAGNOSIS(
              connection_diagnosis_trace_, obutils::OB_SERVER_VC_TRACE, event,
              kill_after_cmd_done_err_code_, "key server session disconnect, will disconnect");
        } else {
          COLLECT_VC_DIAGNOSIS(
              connection_diagnosis_trace_, obutils::OB_CLIENT_VC_TRACE, event,
              OB_CLIENT_RECEIVING_PACKET_CONNECTION_ERROR, NULL);
        }

        client_entry_->eos_ = true;
        LOG_INFO("ObMysqlSM::state_client_request_read", "event",
                 ObMysqlDebugNames::get_event_name(event), K_(sm_id),
                 "client_vc", client_session_ == NULL ? "NULL" : P(client_session_->get_netvc()));
        if (0 == cmd_size_stats_.client_request_bytes_) {
          ret = OB_CONNECT_ERROR;
          // The client is closed. Close it.
          trans_state_.client_info_.abort_ = ObMysqlTransact::ABORTED;
          trans_state_.internal_error_op_for_diagnosis_ = ObMysqlTransact::PROXY_INTERNAL_ERROR_TRANSFER_DISCONNECT;
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
          LOG_WDIAG("ObMysqlSM::state_client_request_read", "event",
                   ObMysqlDebugNames::get_event_name(event), K_(sm_id),
                   "client_vc", client_session_ == NULL ? "NULL" : P(client_session_->get_netvc()));
        }
        ret = OB_CONNECT_ERROR;
        // The client is closed. Close it.
        trans_state_.client_info_.abort_ = ObMysqlTransact::ABORTED;
        break;
      }

      case VC_EVENT_ERROR:
      case VC_EVENT_ACTIVE_TIMEOUT:
      case VC_EVENT_DETECT_SERVER_DEAD: {
        COLLECT_VC_DIAGNOSIS(connection_diagnosis_trace_,
                             obutils::OB_CLIENT_VC_TRACE, event,
                             OB_CLIENT_RECEIVING_PACKET_CONNECTION_ERROR, NULL);
        LOG_WDIAG("ObMysqlSM::state_client_request_read", "event",
                 ObMysqlDebugNames::get_event_name(event), K_(sm_id),
                 "client_vc", client_session_ == NULL ? "NULL" : P(client_session_->get_netvc()));
        ret = OB_CONNECT_ERROR;
        // The client is closed. Close it.
        trans_state_.internal_error_op_for_diagnosis_ = ObMysqlTransact::PROXY_INTERNAL_ERROR_TRANSFER_DISCONNECT;
        trans_state_.client_info_.abort_ = ObMysqlTransact::ABORTED;
        break;
      }

      default:
        ret = OB_INNER_STAT_ERROR;
        LOG_EDIAG("unexpected event", K(event), K_(sm_id), K(ret));
        break;
    }
  }

  if (OB_SUCC(ret) && !client_session_->is_proxy_mysql_client_
      && RUN_MODE_PROXY == g_run_mode) {
    ObNetVConnection *vc = client_session_->get_netvc();
    if (OB_UNLIKELY(NULL != vc && vc->options_.sockopt_flags_ != trans_state_.mysql_config_params_->client_sock_option_flag_out_)) {
      vc->options_.sockopt_flags_ = static_cast<uint32_t>(trans_state_.mysql_config_params_->client_sock_option_flag_out_);
      if (vc->options_.sockopt_flags_ & ObNetVCOptions::SOCK_OPT_KEEP_ALIVE) {
        vc->options_.set_keepalive_param(static_cast<int32_t>(trans_state_.mysql_config_params_->client_tcp_keepidle_),
            static_cast<int32_t>(trans_state_.mysql_config_params_->client_tcp_keepintvl_),
            static_cast<int32_t>(trans_state_.mysql_config_params_->client_tcp_keepcnt_),
            static_cast<int32_t>(trans_state_.mysql_config_params_->client_tcp_user_timeout_));
      }
      if (OB_FAIL(vc->apply_options())) {
        LOG_WDIAG("client session failed to apply per-transaction socket options", K_(sm_id), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObMysqlAnalyzeStatus status = ANALYZE_CONT;
    int64_t first_packet_len = 0;       // the mysql packet total len or mysql compress packet total len
    bool is_proxy_protocol_v2_request = trans_state_.is_proxy_protocol_v2_request_ && client_buffer_reader_->read_avail() > 0;

    if (is_proxy_protocol_v2_request && client_buffer_reader_->read_avail() >= MYSQL_NET_HEADER_LENGTH) {
      char header[MYSQL_NET_HEADER_LENGTH];
      char *written_pos = client_buffer_reader_->copy(header, MYSQL_NET_HEADER_LENGTH, 0);
      if (written_pos != header + MYSQL_NET_HEADER_LENGTH) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("not copy completely", K(ret));
      } else if (!(header[0] == 0x0d && header[1] == 0x0a && header[2] == 0x0d && header[3] == 0x0a)) {
        is_proxy_protocol_v2_request = false;
        trans_state_.is_proxy_protocol_v2_request_ = false;
        event = VC_EVENT_READ_READY;
        if (OB_ISNULL(client_entry_->read_vio_ = client_session_->do_io_read(
                this, 36 - client_buffer_reader_->read_avail(), client_buffer_reader_->mbuf_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("client session failed to do_io_read", K_(sm_id), K(ret));
        }
      }
    }
    if (is_proxy_protocol_v2_request) {
      if (OB_FAIL(handle_proxy_protocol_v2_request(proxy_protocol_v2_, status))) {
        LOG_WDIAG("handle proxy protocol v2 request failed", K(ret));
      }
    } else {
      if (OB_SUCC(ret)) {
        if (OB_FAIL(handle_first_request_packet(status, first_packet_len))) {
          LOG_WDIAG("fail to handle first request packet", K(ret), K(status));
        }
      }
      cmd_size_stats_.client_request_bytes_ = client_buffer_reader_->read_avail();
    }

    if (connection_diagnosis_trace_ != NULL) {
      if (OB_UNLIKELY(trans_state_.trans_info_.sql_cmd_ == OB_MYSQL_COM_QUIT)) {
        connection_diagnosis_trace_->set_is_com_quit(true);
      }
    }

    // Check to see if we are done parsing the whole request
    if ((ANALYZE_CONT != status
          || client_entry_->eos_
          || (ANALYZE_CONT == status && VC_EVENT_READ_COMPLETE == event && !trans_state_.is_auth_request_))
        && !handling_ssl_request_
        && !is_proxy_protocol_v2_request) {
      client_entry_->vc_handler_ = &ObMysqlSM::state_watch_for_client_abort;
      milestones_.client_.client_read_end_ = get_based_hrtime();
      cmd_time_stats_.client_request_read_time_ += (milestones_.client_.client_read_end_ - milestones_.client_.client_begin_);
    }

    switch (__builtin_expect(status, ANALYZE_DONE)) {
      case ANALYZE_OBPARSE_ERROR:
        //must read all data of the request, otherwise will read the remain request data when recv next request
        if (trans_state_.trans_info_.client_request_.get_parse_result().is_internal_cmd()) {
          trans_state_.mysql_errcode_ = OB_INTERNAL_CMD_VALUE_TOO_LONG;
          trans_state_.mysql_errmsg_ = ob_strerror(OB_INTERNAL_CMD_VALUE_TOO_LONG);
        } else {
          trans_state_.mysql_errcode_ = OB_ERR_PARSE_SQL;
          trans_state_.mysql_errmsg_ = ob_strerror(OB_ERR_PARSER_SYNTAX);
        }
        if (OB_FAIL(ObMysqlTransact::encode_error_message(trans_state_))) {
          LOG_WDIAG("fail to build error resp", K(ret));
        } else {
          trans_state_.next_action_ = ObMysqlTransact::SM_ACTION_INTERNAL_NOOP;
          callout_api_and_start_next_action(ObMysqlTransact::SM_ACTION_API_SEND_RESPONSE);
        }
        break;
      case ANALYZE_NOT_SUPPORT:
        trans_state_.mysql_errcode_ = OB_NOT_SUPPORTED;
        trans_state_.mysql_errmsg_ = ob_strerror(OB_NOT_SUPPORTED);
        if (OB_FAIL(ObMysqlTransact::encode_error_message(trans_state_))) {
          LOG_WDIAG("fail to build error resp", K(ret));
        } else {
          trans_state_.next_action_ = ObMysqlTransact::SM_ACTION_INTERNAL_NOOP;
          callout_api_and_start_next_action(ObMysqlTransact::SM_ACTION_API_SEND_RESPONSE);
        }
        break;
      case ANALYZE_CAN_NOT_PASS_WHITE_LIST_ERROR:
        ret = OB_ERR_CAN_NOT_PASS_WHITELIST;
        COLLECT_INTERNAL_DIAGNOSIS(connection_diagnosis_trace_,
                                   OB_PROXY_INTERNAL_TRACE,
                                   OB_ERR_CAN_NOT_PASS_WHITELIST,
                                   "sql analyze result can't not pass white list");
        LOG_WDIAG("error not pass white list", K(ret), K_(sm_id));
        set_client_abort(ObMysqlTransact::ABORTED, event);

        // Disable further I/O on the client
        client_entry_->read_vio_->nbytes_ = client_entry_->read_vio_->ndone_;
        break;
      case ANALYZE_ERROR:
        ret = OB_ERR_UNEXPECTED;
        // if build error packet and current_state correct, won't disconnect
        if (trans_state_.mysql_errcode_ != 0 &&
            (trans_state_.current_.state_ == ObMysqlTransact::CMD_COMPLETE ||
             trans_state_.current_.state_ == ObMysqlTransact::TRANSACTION_COMPLETE)) {
          COLLECT_INTERNAL_DIAGNOSIS(
              connection_diagnosis_trace_, OB_PROXY_INTERNAL_TRACE,
              OB_PROXY_INTERNAL_ERROR, "sql analyze error, abort connection");
        }
        LOG_WDIAG("error parsing client request", K(ret), K_(sm_id));
        set_client_abort(ObMysqlTransact::ABORTED, event);

        // Disable further I/O on the client
        client_entry_->read_vio_->nbytes_ = client_entry_->read_vio_->ndone_;
        break;

      case ANALYZE_CONT:
        if (client_entry_->eos_) {
          ret = OB_CONNECT_ERROR;
          COLLECT_VC_DIAGNOSIS(
              connection_diagnosis_trace_, obutils::OB_CLIENT_VC_TRACE,
              VC_EVENT_EOS, OB_CLIENT_RECEIVING_PACKET_CONNECTION_ERROR, NULL);
          LOG_WDIAG("EOS before client request parsing finished", K_(sm_id), K(ret));
          set_client_abort(ObMysqlTransact::ABORTED, event);

          // Disable further I/O on the client
          client_entry_->read_vio_->nbytes_ = client_entry_->read_vio_->ndone_;
        } else if (is_proxy_protocol_v2_request) {
          if (VC_EVENT_READ_COMPLETE == event) {
            client_buffer_reader_->mbuf_->water_mark_ = proxy_protocol_v2_.get_total_len();
            if (OB_ISNULL(client_entry_->read_vio_ = client_session_->do_io_read(this, proxy_protocol_v2_.get_len(), client_buffer_reader_->mbuf_))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WDIAG("client session failed to do_io_read", K_(sm_id), K(ret));
            } else {
              event_ret = VC_EVENT_CONT;
            }
          } else {
            client_entry_->read_vio_->reenable();
            event_ret = VC_EVENT_CONT;
          }
        } else if (VC_EVENT_READ_COMPLETE == event) {
          LOG_DEBUG("VC_EVENT_READ_COMPLETE and ANALYZE CONT status", K_(sm_id));
          if (trans_state_.is_auth_request_) {
            client_entry_->read_vio_->reenable();
            if (OB_ISNULL(client_entry_->read_vio_ = client_session_->do_io_read(this,
                    INT64_MAX, client_buffer_reader_->mbuf_))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WDIAG("client session failed to do_io_read", K_(sm_id), K(ret));
            }
          }
        } else {
          MYSQL_INCREMENT_DYN_STAT(TOTAL_CLIENT_REQUEST_REREAD_COUNT);
          int64_t mysql_req_len = trans_state_.trans_info_.client_request_.get_packet_len();
          int64_t request_max = MAX(first_packet_len, mysql_req_len);
          request_max = MAX(request_max, MYSQL_COMPRESSED_OB20_HEALDER_LENGTH);
          if (request_max > 0 && request_max > client_buffer_reader_->mbuf_->water_mark_) {
            // ensure the read buffer can cache the whole reqeust
            client_buffer_reader_->mbuf_->water_mark_ = request_max;
            LOG_DEBUG("modify client buffer reader water mark", K(request_max));
          }
          if (ObMysqlTransact::is_large_request(trans_state_) ||
              ObMysqlTransact::is_transfer_content_of_file(trans_state_)) {
            // Disable further I/O on the client since there could
            // be rest request body that we are tunneling, and we can't issue
            // another IO later for the rest request body with a different buffer
            client_entry_->read_vio_->nbytes_ = client_entry_->read_vio_->ndone_;
            trans_state_.trans_info_.request_content_length_ = mysql_req_len;
            if (ObMysqlTransact::is_large_request(trans_state_)) {
              MYSQL_INCREMENT_TRANS_STAT(CLIENT_LARGE_REQUESTS);
              LOG_DEBUG("large request", K_(sm_id), K(mysql_req_len),
                        "saved_request_len", client_buffer_reader_->read_avail());
            }

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
        if (is_proxy_protocol_v2_request) {
          trans_state_.is_proxy_protocol_v2_request_ = false;
          client_buffer_reader_->mbuf_->water_mark_ = MYSQL_NET_META_LENGTH;
          if (OB_FAIL(client_buffer_reader_->consume(proxy_protocol_v2_.get_total_len()))) {
            LOG_WDIAG("analyze ppv2 done, consume buffer failed", K(ret));
          } else if (OB_ISNULL(client_entry_->read_vio_ = client_session_->do_io_read(
                                 this, 36, client_buffer_reader_->mbuf_))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("client session failed to do_io_read", K_(sm_id), K(ret));
          }
        } else {
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
          if (OB_UNLIKELY(handling_ssl_request_)) {
            client_entry_->read_vio_->nbytes_ = INT64_MAX;
          }
          client_entry_->read_vio_->reenable();

          // cancel client net_read_timeout, set to wait_timeout
          set_client_wait_timeout();

          // no request data to read, reset read trigger and avoid unnecessary reading
          if (!client_session_->is_proxy_mysql_client_) {
            ObUnixNetVConnection* vc = static_cast<ObUnixNetVConnection *>(client_session_->get_netvc());
            if (!handling_ssl_request_) {
              vc->reset_read_trigger();
            }
          }

          if (OB_LIKELY(!handling_ssl_request_)) {
            bool need_direct_response_for_client = false;
            bool need_wait_callback = false;
            ObClientSessionInfo &session_info = client_session_->get_session_info();
            if (OB_UNLIKELY(session_info.is_sharding_user())) {
              if (OB_FAIL(handle_shard_request(need_direct_response_for_client, need_wait_callback))) {
                LOG_EDIAG("handle shard request failed", K(ret));
              } else if (need_wait_callback) {
                // do nothing
              } else if (need_direct_response_for_client) {
                if (OB_FAIL(client_buffer_reader_->consume_all())) {
                  LOG_WDIAG("fail to consume all", K_(sm_id), K(ret));
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
                    LOG_WDIAG("fail to setup handle request shard scan", K_(sm_id), K(ret));
                  }
                }
              }
            } else {
              if (OB_UNLIKELY(get_global_proxy_config().enable_qos
                    && !client_session_->is_proxy_mysql_client_
                    && OB_FAIL(handle_limit(need_direct_response_for_client)))) {
                LOG_WDIAG("fail to handle limit", K(ret));
              }

              if (OB_SUCC(ret) && !need_direct_response_for_client
                  && !client_session_->is_proxy_mysql_client_
                  && OB_UNLIKELY(get_global_proxy_config().enable_ldg)
                  && OB_FAIL(handle_ldg(need_direct_response_for_client))) {
                LOG_WDIAG("fail to handle ldg", K(ret));
              }

              if (OB_SUCC(ret) && !need_direct_response_for_client
                  && !client_session_->is_proxy_mysql_client_
                  && get_global_proxy_config().enable_standby
                  && OB_FAIL(handle_service_name(need_direct_response_for_client))) {
                LOG_WDIAG("fail to handle service name", K(ret));
                if (OB_SERVICE_NAME_NOT_FOUND == ret) {
                  trans_state_.mysql_errcode_ = ret;
                  int tmp_ret = OB_SUCCESS;
                  if (OB_UNLIKELY(OB_SUCCESS !=
                                  (tmp_ret = ObMysqlTransact::encode_error_message(trans_state_)))) {
                    LOG_WDIAG("fail to build err packet", K(ret));
                  } else {
                    // do nothing
                  }
                }
              }

              if (OB_SUCC(ret) && OB_UNLIKELY(need_direct_response_for_client)) {
                if (OB_FAIL(client_buffer_reader_->consume_all())) {
                  LOG_WDIAG("fail to consume all", K_(sm_id), K(ret));
                } else {
                  trans_state_.next_action_ = ObMysqlTransact::SM_ACTION_INTERNAL_NOOP;
                  callout_api_and_start_next_action(ObMysqlTransact::SM_ACTION_API_SEND_RESPONSE);
                }
              }

              if (OB_SUCC(ret) && OB_LIKELY(!need_direct_response_for_client)) {
                if (OB_LIKELY(client_session_->get_session_info().is_oceanbase_server())) {
                  setup_get_cluster_resource();
                } else {
                  setup_set_cached_variables();
                }
              }
            }
          } else {
            handling_ssl_request_ = false;
          }
        }
        if (connection_diagnosis_trace_ != NULL) {
          if (OB_UNLIKELY(trans_state_.trans_info_.sql_cmd_ == OB_MYSQL_COM_LOGIN)) {
            connection_diagnosis_trace_->set_first_packet_received(true);
          }
        }
        break;
      }
      default:
        ret = OB_INNER_STAT_ERROR;
        LOG_EDIAG("unknown analyze mysql request status", K(status), K_(sm_id), K(ret));
        break;
    }
  }

  #ifdef ERRSIM
  if (OB_SUCC(ret) && trans_state_.trans_info_.sql_cmd_ == COM_LOGIN && OB_FAIL(OB_E(EventTable::EN_COM_LOGIN_FAIL) OB_SUCCESS)) {
    LOG_INFO("error inject succ", "sql_cmd:", trans_state_.trans_info_.sql_cmd_);
  }
  #endif

  #ifdef ERRSIM
  if (OB_SUCC(ret) && trans_state_.trans_info_.sql_cmd_ == COM_QUIT && OB_FAIL(OB_E(EventTable::EN_COM_QUIT_FAIL) OB_SUCCESS)) {
    LOG_INFO("error inject succ", "sql_cmd:", trans_state_.trans_info_.sql_cmd_);
  }
  #endif

  if (OB_FAIL(ret)) {
    trans_state_.inner_errcode_ = ret;
    call_transact_and_set_next_state(ObMysqlTransact::bad_request);
  }

  return event_ret;
}

int ObMysqlSM::setup_handle_shard_ddl(ObAction *action)
{
  int ret = OB_SUCCESS;

  LOG_DEBUG("setup handle shard ddl");
  MYSQL_SM_SET_DEFAULT_HANDLER(&ObMysqlSM::state_handle_shard_ddl);

  int64_t total_len = client_buffer_reader_->read_avail();
  if (total_len > trans_state_.trans_info_.client_request_.get_packet_meta().pkt_len_) {
    total_len = trans_state_.trans_info_.client_request_.get_packet_meta().pkt_len_;
  }

  // consume data in client buffer reader
  if (OB_FAIL(client_buffer_reader_->consume(total_len))) {
    LOG_WDIAG("fail to consume all", K_(sm_id), K(ret));
  } else if (OB_NOT_NULL(pending_action_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("pending_action must be NULL here", K_(pending_action), K_(sm_id), K(ret));
  } else {
    pending_action_ = action;
  }

  return ret;
}

int ObMysqlSM::state_handle_shard_ddl(int event, void *data)
{
  int ret = OB_SUCCESS;

  STATE_ENTER(ObMysqlSM::state_handle_shard_ddl, event);

  pending_action_ = NULL;

  switch (event) {
    case ASYNC_PROCESS_DONE_EVENT:
      if (OB_ISNULL(data)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WDIAG("data is NULL", K_(sm_id), K(ret));
      } else if (OB_FAIL(process_shard_ddl_result(reinterpret_cast<ObShardDDLStatus*>(data)))) {
        LOG_WDIAG("fail to process executor result, will disconnect", K_(sm_id), K(ret));
      } else {
        trans_state_.next_action_ = ObMysqlTransact::SM_ACTION_INTERNAL_NOOP;
        callout_api_and_start_next_action(ObMysqlTransact::SM_ACTION_API_SEND_RESPONSE);
      }
      break;
    case VC_EVENT_EOS:
    case VC_EVENT_ACTIVE_TIMEOUT:
    case VC_EVENT_INACTIVITY_TIMEOUT:
    case VC_EVENT_DETECT_SERVER_DEAD:
      LOG_WDIAG("handle shard ddl meet error, will disconnect", K_(sm_id),
               "event", ObMysqlDebugNames::get_event_name(event), K(ret));
      ret = OB_CONNECT_ERROR;
      break;
    case VC_EVENT_ERROR:
      LOG_WDIAG("handle shard ddl meet error, will disconnect", K_(sm_id),
               "event", ObMysqlDebugNames::get_event_name(event), K(ret));
      ret = OB_ERR_UNEXPECTED;
      break;
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("Unexpected event", K_(sm_id), K(event), K(ret));
      break;
    }
  }

  if (OB_FAIL(ret)) {
    // 如果内部处理有问题, 清空缓存, 直接断链接
    trans_state_.free_internal_buffer();
    trans_state_.inner_errcode_ = ret;
    call_transact_and_set_next_state(ObMysqlTransact::handle_error_jump);
  }

  return VC_EVENT_NONE;
}

int ObMysqlSM::process_shard_ddl_result(ObShardDDLStatus *ddl_status)
{
  int ret = OB_SUCCESS;

  ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
  uint8_t seq = static_cast<uint8_t>(client_request.get_packet_meta().pkt_seq_ + 1);
  ObMIOBuffer *buf = NULL;

  if (NULL != trans_state_.internal_buffer_) {
    buf = trans_state_.internal_buffer_;
  } else {
    if (OB_FAIL(trans_state_.alloc_internal_buffer(MYSQL_BUFFER_SIZE))) {
      LOG_WDIAG("fail to allocate internal buffer", K(ret));
    } else {
      buf = trans_state_.internal_buffer_;
    }
  }

  if (OB_SUCC(ret)) {
    ObProxyProtocol client_protocol = get_client_session_protocol();
    ObMysqlClientSession *client_session = get_client_session();

    if (ddl_status->is_success()) {
      const ObMySQLCapabilityFlags &capability = client_session->get_session_info().get_orig_capability_flags();
      if (OB_FAIL(ObProxyPacketWriter::write_ok_packet(*buf, *client_session_, client_protocol, seq, 0, capability))) {
        LOG_WDIAG("[ObMysqlSM::do_internal_request] fail to encode shard ddl ok packet",
                 K_(sm_id), K(ret));
      }
    } else {
      int64_t error_code = 0;
      if (OB_FAIL(get_int_value(ddl_status->get_error_code(), error_code))) {
        LOG_WDIAG("fail to get int error code", "errcode", ddl_status->get_error_code(), K(ret));
      } else if (OB_FAIL(ObProxyPacketWriter::write_error_packet(*buf, client_session, client_protocol,
                                                                 seq, static_cast<int>(-error_code),
                                                                 ddl_status->get_error_message()))) {
        LOG_WDIAG("fail to encode err pacekt buf", K(seq), "errmsg", ddl_status->get_error_message(),
                 "errcode", error_code, K(ret));
      } else {
        // nothing
      }
    }
  }

  return ret;
}

int ObMysqlSM::setup_handle_execute_plan()
{
  int ret = OB_SUCCESS;

  LOG_DEBUG("setup handle execute plan");
  MYSQL_SM_SET_DEFAULT_HANDLER(&ObMysqlSM::state_handle_execute_plan);

  ObShardingSelectLogPlan* plan = NULL;
  ObProxyOperator* operator_root = NULL;
  ObProxyOperatorCont *operator_cont = NULL;

  ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
  int64_t total_len = client_buffer_reader_->read_avail();
  if (total_len > client_request.get_packet_meta().pkt_len_) {
    total_len = client_request.get_packet_meta().pkt_len_;
  }

  uint8_t seq = static_cast<uint8_t>(client_request.get_packet_meta().pkt_seq_ + 1);

  ObHRTime execute_timeout = get_query_timeout();

  // consume data in client buffer reader
  if (OB_FAIL(client_buffer_reader_->consume(total_len))) {
    LOG_WDIAG("fail to consume all", K_(sm_id), K(ret));
  } else if (OB_ISNULL(plan = client_session_->get_sharding_select_log_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("select log plan should not be null", K_(sm_id), K(ret));
  } else if (OB_ISNULL(operator_root = plan->get_plan_root())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("operator should not be null", K_(sm_id), K(ret));
  } else if (OB_NOT_NULL(pending_action_)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("pending_action must be NULL here", K_(pending_action), K_(sm_id), K(ret));
  } else if (OB_ISNULL(operator_cont = new(std::nothrow) ObProxyOperatorCont(this, &self_ethread()))) {
    LOG_WDIAG("fail to alloc parallel execute cont", K(ret));
  } else if (OB_FAIL(operator_cont->init(operator_root, seq, hrtime_to_msec(execute_timeout)))) {
    LOG_WDIAG("fail to init execute cont", K(ret));
  } else if (OB_ISNULL(g_shard_scan_all_task_processor.schedule_imm(operator_cont, ET_SHARD_SCAN_ALL))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to schedule parallel execute cont", K(ret));
  } else {
    pending_action_ = &operator_cont->get_action();
    client_session_->set_inactivity_timeout(execute_timeout, obutils::OB_CLIENT_EXECUTE_PLAN_TIMEOUT);
    #ifdef ERRSIM
    int tmp_ret = OB_SUCCESS;
    if ((tmp_ret = OB_E(EventTable::EN_CLIENT_EXECUTE_PLAN_TIMEOUT) OB_SUCCESS) != OB_SUCCESS) {
      client_session_->set_inactivity_timeout(1, obutils::OB_CLIENT_EXECUTE_PLAN_TIMEOUT);
    }
    #endif
  }

  if (OB_FAIL(ret) && OB_LIKELY(NULL != operator_cont)) {
    operator_cont->destroy();
    operator_cont = NULL;
  }

  return ret;
}

int ObMysqlSM::state_handle_execute_plan(int event, void *data)
{
  int ret = OB_SUCCESS;

  STATE_ENTER(ObMysqlSM::state_handle_execute_plan, event);

  ObShardingSelectLogPlan* plan = NULL;
  ObProxyOperator* operator_root = NULL;

  switch (event) {
    case ASYNC_PROCESS_DONE_EVENT:
      pending_action_ = NULL;
      if (OB_ISNULL(data)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WDIAG("data is NULL", K_(sm_id), K(ret));
      } else if (OB_FAIL(process_executor_result(reinterpret_cast<ObIOBufferReader*>(data)))) {
        LOG_WDIAG("fail to process executor result, will disconnect", K_(sm_id), K(ret));
      } else {
        trans_state_.next_action_ = ObMysqlTransact::SM_ACTION_INTERNAL_NOOP;
        callout_api_and_start_next_action(ObMysqlTransact::SM_ACTION_API_SEND_RESPONSE);
      }
      break;
    case VC_EVENT_EOS:
    case VC_EVENT_ACTIVE_TIMEOUT:
    case VC_EVENT_INACTIVITY_TIMEOUT:
    case VC_EVENT_DETECT_SERVER_DEAD:
      LOG_WDIAG("handle execute plan meet error, will disconnect", K_(sm_id),
              "event", ObMysqlDebugNames::get_event_name(event), K(ret));
      ret = OB_CONNECT_ERROR;
      break;
    case VC_EVENT_ERROR:
      LOG_WDIAG("handle execute plan meet error, will disconnect", K_(sm_id),
               "event", ObMysqlDebugNames::get_event_name(event), K(ret));
      ret = OB_ERR_UNEXPECTED;
      break;
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("Unexpected event", K_(sm_id), K(event), K(ret));
      break;
    }
  }

  if (OB_NOT_NULL(pending_action_)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = pending_action_->cancel())) {
      LOG_WDIAG("failed to cancel pending action", K_(pending_action), K_(sm_id), K(tmp_ret));
    }

    if (OB_SUCC(ret)) {
      ret = tmp_ret;
    }
    pending_action_ = NULL;
  }

  if (OB_ISNULL(plan = client_session_->get_sharding_select_log_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("select log plan should not be null", K_(sm_id), K(ret));
  } else if (OB_ISNULL(operator_root = plan->get_plan_root())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("operator should not be null", K_(sm_id), K(ret));
  } else {
    operator_root->close();
  }

  if (OB_FAIL(ret)) {
    // 如果内部处理有问题, 清空缓存, 直接断链接
    trans_state_.free_internal_buffer();
    trans_state_.inner_errcode_ = ret;
    call_transact_and_set_next_state(ObMysqlTransact::handle_error_jump);
  }

  return VC_EVENT_NONE;
}

int ObMysqlSM::process_executor_result(ObIOBufferReader *resp_reader)
{
  int ret = OB_SUCCESS;

  ObMIOBuffer *buf = NULL;

  if (NULL != trans_state_.internal_buffer_) {
    buf = trans_state_.internal_buffer_;
  } else {
    if (OB_FAIL(trans_state_.alloc_internal_buffer(MYSQL_BUFFER_SIZE))) {
      LOG_WDIAG("fail to allocate internal buffer", K(ret));
    } else {
      buf = trans_state_.internal_buffer_;
    }
  }

  if (OB_SUCC(ret)) {
    ObMysqlClientSession *client_session = get_client_session();
    ObProxyProtocol client_protocol = get_client_session_protocol();

    if (client_protocol == ObProxyProtocol::PROTOCOL_OB20) {
      Ob20ProtocolHeader &ob20_head = client_session->get_session_info().ob20_request_.ob20_header_;
      uint8_t compressed_seq = static_cast<uint8_t>(client_session->get_compressed_seq() + 1);
      Ob20HeaderParam ob20_head_param(client_session->get_cs_id(), ob20_head.request_id_, compressed_seq,
                                      compressed_seq, true, false, false,
                                      client_session->is_client_support_new_extra_info(),
                                      client_session->is_trans_internal_routing(), false);
      if (OB_FAIL(ObProto20Utils::consume_and_compress_data(resp_reader, buf,
                                                            resp_reader->read_avail(), ob20_head_param))) {
        LOG_WDIAG("fail to consume and compress data for executor response packet in ob20", K(ret));
      } else {
        LOG_DEBUG("succ to executor response in ob20 packet");
      }
    } else {
      int64_t data_size = resp_reader->read_avail();
      int64_t bytes_written = 0;
      if (OB_FAIL(buf->remove_append(resp_reader, bytes_written))) {
        LOG_EDIAG("Error while remove_append to buf", "Attempted size", data_size,
                  "wrote size", bytes_written, K(ret));
      } else if (OB_UNLIKELY(bytes_written != data_size)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unexpected result", "Attempted size", data_size,
                 "wrote size", bytes_written, K(ret));
      } else {
        LOG_DEBUG("succ to write to client", "Attempted size", bytes_written);
      }
    }
  }

  return ret;
}

int ObMysqlSM::handle_shard_request(bool &need_response_for_stmt, bool &need_wait_callback)
{
  int ret = OB_SUCCESS;

  need_response_for_stmt = false;
  need_wait_callback = false;
  ObClientSessionInfo &session_info = client_session_->get_session_info();
  ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
  ObSqlParseResult &parse_result = client_request.get_parse_result();
  ObMySQLCmd &req_cmd = trans_state_.trans_info_.sql_cmd_;

  session_info.set_allow_use_last_session(true);

  ObHSRResult &hsr = session_info.get_login_req().get_hsr_result();
  ObDbConfigLogicDb *db_info = NULL;

  if (!session_info.is_sharding_user()
      || parse_result.is_shard_special_cmd()) {
    // do nothing
  } else {
    if (obmysql::OB_MYSQL_COM_FIELD_LIST == req_cmd) {
      ret = OB_NOT_SUPPORTED;
      LOG_WDIAG("com_field_list cmd is unsupported for sharding table", K(ret));
    } else if (obmysql::OB_MYSQL_COM_QUERY != req_cmd) {
      //do nothing
    } else if (ObProxyShardUtils::is_special_db(parse_result)) {
      if (OB_FAIL(ObProxyShardUtils::handle_information_schema_request(*client_session_,
                                                                       trans_state_,
                                                                       *client_buffer_reader_))) {
        LOG_WDIAG("fail to handle information schema request", K(ret));
      }
    } else {
      //  get logic db and check auth
      if (OB_FAIL(ObProxyShardUtils::get_logic_db_info(trans_state_,
                      client_session_->get_session_info(),
                      db_info))) {
        LOG_WDIAG("fail to get logic database", K(ret));
      } else if (OB_ISNULL(db_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("db info is null", K(ret));
      } else if (OB_FAIL(ObProxyShardUtils::check_shard_request(*client_session_, parse_result,
                    *db_info))) {
        LOG_WDIAG("fail to check shard request", K(ret));
      } else if (OB_FAIL(ObProxyShardUtils::handle_possible_probing_stmt(client_request.get_sql(), parse_result))) {
        // disconnect and not return internal error packet
        // check proxy alive state, if internal error, disconnect directly
        trans_state_.internal_error_op_for_diagnosis_ = ObMysqlTransact::PROXY_INTERNAL_ERROR_TRANSFER_DISCONNECT;
        LOG_WDIAG("fail to handle prob stmt, obproxy is pretending sleeping", K(ret));
      } else if (parse_result.is_dual_request()) {
        //do nothing
      } else if (parse_result.is_ddl_stmt()) {
        const ObString runtime_env = get_global_proxy_config().runtime_env.str();
        if (0 == runtime_env.case_compare(OB_PROXY_DBP_RUNTIME_ENV)) {
          if (OB_FAIL(ObProxyShardUtils::handle_ddl_request(this, *client_session_, trans_state_, *db_info, need_wait_callback))) {
            LOG_WDIAG("fail to handle ddl request", K(ret));
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WDIAG("ddl stmt is unsupported for sharding table", K(ret));
        }
      } else if (db_info->is_single_shard_db_table()) {
        // sindle db table
        if (OB_FAIL(ObProxyShardUtils::handle_single_shard_request(*client_session_,
                        trans_state_, *client_buffer_reader_, *db_info))) {
          LOG_WDIAG("fail to handle single shard request", K(ret));
        }
      } else if (OB_FAIL(ObProxyShardUtils::handle_shard_request(*client_session_,
                      trans_state_, *client_buffer_reader_, *db_info))) {
          LOG_WDIAG("fail to handle shard request", K(ret));
      } else if (OB_UNLIKELY(session_info.need_record_shard_txn_server())) {
        session_info.set_need_record_shard_txn_server(false);
        if (OB_ISNULL(client_session_->get_server_session())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("fail to find txn server session", K(ret));
        } else {
          client_session_->set_sharding_txn_ss_addr(
                          client_session_->get_server_session()->get_netvc()->get_remote_addr());
          LOG_DEBUG("set shard txn shard", "remote addr",
                    client_session_->get_sharding_txn_ss_addr(), "txn shard name",
                    client_session_->get_session_info().get_txn_shard_connector()->shard_name_.config_string_, "server shard conn",
                    client_session_->get_server_session()->get_session_info().get_shard_connector()->shard_name_.config_string_);
        }
      }
    }

    if (OB_FAIL(ret)) {
      int tmp_ret = ObProxyShardUtils::build_error_packet(ret, need_response_for_stmt, trans_state_, client_session_);
      ret = OB_SUCC(tmp_ret) ? tmp_ret : ret;
    }
    if (OB_SUCC(ret) && !need_response_for_stmt && !need_wait_callback) {
      if (OB_FAIL(save_user_login_info(session_info, hsr))) {
        LOG_WDIAG("fail to save user login info", K_(sm_id), K(ret));
      }
    }
    if (NULL != db_info) {
      db_info->dec_ref();
      db_info = NULL;
    }
  }

  if (OB_NOT_NULL(session_info.get_txn_shard_connector())
      && *session_info.get_shard_connector() != *session_info.get_txn_shard_connector()) {
    session_info.set_allow_use_last_session(false);
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
      LOG_WDIAG("default_sysvar_set is null", K(ret));
    } else if (OB_FAIL(client_session_->get_session_info().add_sys_var_set(*default_sysvar_set))) {
      LOG_WDIAG("fail to add sys var set", K(ret));
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
        LOG_WDIAG("fail to calc limit", K(ret));
      }

      cur_config->dec_ref();
      cur_config = NULL;

      if (OB_SUCC(ret) && !is_pass) {
        char err_msg_buf[OB_MAX_ERROR_MSG_LEN] = "\0";
        int64_t pos = 0;
        if (OB_FAIL(databuff_printf(err_msg_buf, OB_MAX_ERROR_MSG_LEN, pos, ob_str_user_error(OB_ERR_LIMIT),
                                    limit_name.length(), limit_name.ptr()))) {
          LOG_WDIAG("build error msg for limit failed", K(limit_name), K(ret));
        } else {
          trans_state_.mysql_errcode_ = OB_ERR_LIMIT;
          trans_state_.mysql_errmsg_ = err_msg_buf;
          if (OB_FAIL(ObMysqlTransact::encode_error_message(trans_state_))) {
            LOG_WDIAG("fail to build err resp", K(ret));
          } else {
            need_response_for_client = true;
          }
        }
      }
    }
  }

  return ret;
}

int ObMysqlSM::handle_service_name(bool &need_response_for_client)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  need_response_for_client = false;
  bool need_rewrite_login_req = false;
  need_renew_cluster_resource_ = false;
  ObProxyObInstance* obinstance = NULL;
  ObClientSessionInfo &cs_info = client_session_->get_session_info();
  ObHSRResult &hsr = cs_info.get_login_req().get_hsr_result();
  ObServiceNameInstance *instance = NULL;
  ObString service_name;
  // 对service name连接串登录时，会把service name当作tenant_name
  if (OB_MYSQL_COM_LOGIN == trans_state_.trans_info_.sql_cmd_) {
    const int64_t service_name_prefix_len = strlen(OB_SERVICE_NAME_PRIFIX);
    if (hsr.tenant_name_.prefix_case_match(OB_SERVICE_NAME_PRIFIX)) {
      need_renew_cluster_resource_ = false;
      // service name为空，或长度超过限制（64），返回service name不存在;
      if (OB_UNLIKELY(hsr.tenant_name_.length() == service_name_prefix_len
          || hsr.tenant_name_.length() - service_name_prefix_len > OB_MAX_SERVICE_NAME_LENGTH)) {
        ret = OB_SERVICE_NAME_NOT_FOUND;
        LOG_WDIAG("service name is empty or length greater than OB_MAX_SERVICE_NAME_LENGTH",
                  K(OB_MAX_SERVICE_NAME_LENGTH), K(hsr.tenant_name_.length()), K_(hsr.tenant_name), K_(hsr.cluster_name), K(ret));
      } else {
        ObString tmp_service_name(hsr.tenant_name_.length() - service_name_prefix_len, hsr.tenant_name_.ptr() + service_name_prefix_len);
        // service name名字大小写不敏感
        string_to_lower_case(tmp_service_name.ptr(), tmp_service_name.length());
        LOG_DEBUG("service name is login", K(tmp_service_name));
        // 获取service name，如果找不到，会拉取ocp一次;
        if (OB_FAIL(get_global_config_server_processor().get_service_name_with_fetch(
                                                            tmp_service_name, instance))) {
          LOG_WDIAG("fail to get service name instance when login", K(tmp_service_name), K(ret));
        } else if (OB_ISNULL(instance) || OB_UNLIKELY(instance->instance_array_.empty())) {
          ret = OB_SERVICE_NAME_NOT_FOUND;
          LOG_WDIAG("service name is not exist when login", K(tmp_service_name), K(instance), K(ret));
        } else {
          obinstance = instance->instance_array_.at(instance->primary_index_);
          if (OB_ISNULL(obinstance) || !obinstance->is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("service name argument is not vaild", KP(obinstance), K(instance->primary_index_), K(ret));
          } else if (OB_FAIL(cs_info.set_service_name(tmp_service_name))) {
            LOG_WDIAG("fail to set service name to client session info", K(tmp_service_name), K(ret));
          } else {
            // 注册service name成功
            need_rewrite_login_req = true;
            client_session_->set_using_service_name(true);
            DEC_SHARED_REF(service_name_instance_);
            service_name_instance_ = instance;
            instance = NULL;
            LOG_DEBUG("succ to convert login service name",
                     K(tmp_service_name), "tenant_name", obinstance->ob_tenant_, "cluster_name", obinstance->ob_cluster_);
          }
        }
      }
    } else {
      // do nothing
    }
  } else if (!client_session_->using_service_name()) {
    // 非service name session, do nothing
  } else if (OB_FAIL(cs_info.get_service_name(service_name))) {
    LOG_WDIAG("fail to get service name from session info", K(ret));
  } else {
    need_renew_cluster_resource_ = false;
    // 强路由不转换
    need_depend_last_session_ = ObMysqlTransact::is_depend_last_session(trans_state_);
    ObString cluster_name;
    ObString tenant_name;

    // 如果出现失败，会复用上一次的cluster_name、tenant_name
    if (need_depend_last_session_) {
      LOG_DEBUG("need depend last session, don't change cluster/tenant", K(service_name), K(ret));
    } else if (OB_FAIL(cs_info.get_cluster_name(cluster_name))) {
      LOG_WDIAG("fail to get cluster name", K(ret));
    } else if (OB_FAIL(cs_info.get_tenant_name(tenant_name))) {
      LOG_WDIAG("fail to get tenant name", K(ret));
    } else if (OB_FAIL(get_global_config_server_processor().get_service_name_without_fetch(
service_name, instance))) {
      LOG_DEBUG("fail to get service name instance", K(service_name), K(ret));
    } else if (OB_ISNULL(instance) || OB_UNLIKELY(instance->instance_array_.empty())) {
      // service name可能被删除了，复用old session
      ret = OB_SERVICE_NAME_NOT_FOUND;
      LOG_WDIAG("service name is not exist, maybe is deleted. Will use last session", K(service_name), K(instance), K(ret));
    } else {
      // 获取主租户，解析时会将首个租户设置为主租户
      obinstance = instance->instance_array_.at(instance->primary_index_);
      if (OB_ISNULL(obinstance) || !obinstance->is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("service name argument is not vaild", KP(obinstance), K(ret));
      } else if (obinstance->ob_cluster_.get_string() != cluster_name
                 || obinstance->ob_tenant_.get_string() != tenant_name) {
        client_session_->is_need_update_dummy_entry_ = true;
        need_rewrite_login_req = true;
        // 释放旧service_name，移交所有权
        DEC_SHARED_REF(service_name_instance_);
        service_name_instance_ = instance;
        instance = NULL;
        if (obinstance->ob_cluster_.get_string() != cluster_name) {
          need_renew_cluster_resource_ = true;
          DEC_SHARED_REF(client_session_->cluster_resource_);
        }
      }
    }
  }

  // 前面移交所有权后，一定会把instance置为NULL，否则需要引用计数-1
  DEC_SHARED_REF(instance);

  // 复用LDG的方法，重写loging req
  if (OB_SUCC(ret) && need_rewrite_login_req && OB_NOT_NULL(obinstance)) {
    if (OB_FAIL(ObProxySessionInfoHandler::rewrite_ldg_login_req(
            cs_info, obinstance->ob_tenant_, obinstance->ob_cluster_))) {
      LOG_WDIAG("fail to ldg rewrite service name login req", K(ret));
      tmp_ret = ret;
    } else if (OB_FAIL(save_user_login_info(cs_info, hsr))) {
      LOG_WDIAG("fail to save user login info failed", K(ret), K(hsr), K(cs_info));
      tmp_ret = ret;
    // service name兼容租户的白名单校验
    } else if (OB_MYSQL_COM_LOGIN == trans_state_.trans_info_.sql_cmd_ && !can_pass_white_list()) {
      tmp_ret = ret = OB_ERR_CAN_NOT_PASS_WHITELIST;
      LOG_WDIAG("service name tenant can not pass white_list", K_(obinstance->ob_cluster),
                K_(obinstance->ob_tenant), K(ret));
    } else if (OB_MYSQL_COM_LOGIN != trans_state_.trans_info_.sql_cmd_) {
      LOG_INFO("handle service name success", K(service_name), K(obinstance->ob_cluster_), K(obinstance->ob_tenant_));
    }
  }

  if (OB_FAIL(ret) && OB_MYSQL_COM_LOGIN != trans_state_.trans_info_.sql_cmd_) {
    // 如果非登录失败，连接之前的集群，减少业务报错
    LOG_WDIAG("handle service name failed, try to use old session according ret", K(tmp_ret), K(ret));
    ret = tmp_ret;
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
      //ignore
      //对于普通租户是查不到LDG的相关信息的
      if (OB_HASH_NOT_EXIST == ret) {
        LOG_DEBUG("get primary role instance failed", K(ret));
      } else {
        LOG_WDIAG("get primary role instance failed", K(ret));
      }
    } else if (OB_ISNULL(instance)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("ob instance is null", K(ret));
    } else if (!instance->is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("instance is not valid", K(ret));
    } else {
      LOG_DEBUG("ldg login user", K(*instance), K(hsr.cluster_name_), K(hsr.tenant_name_));
      session_info.set_ldg_logical_cluster_name(hsr.cluster_name_);
      session_info.set_ldg_logical_tenant_name(hsr.tenant_name_);
      client_session_->set_using_ldg(true);
      need_rewrite_login_req = true;
    }
  } else if (client_session_->using_ldg()) {
    if (OB_FAIL(session_info.get_ldg_logical_cluster_name(ldg_logical_cluster_name))) {
      LOG_WDIAG("fail to get ldg logical cluster name", K(ret));
    } else if (OB_FAIL(session_info.get_ldg_logical_tenant_name(ldg_logical_tenant_name))) {
      LOG_WDIAG("fail to get ldg logical tenant name", K(ret));
    } else if (OB_FAIL(session_info.get_cluster_name(ldg_real_cluster_name))) {
      LOG_WDIAG("fail to get ldg real cluster name", K(ret));
    } else if (OB_FAIL(session_info.get_tenant_name(ldg_real_tenant_name))) {
      LOG_WDIAG("fail to get ldg real tenant name", K(ret));
    } else if (OB_FAIL(get_global_config_server_processor().get_ldg_primary_role_instance(
            ldg_logical_tenant_name, ldg_logical_cluster_name, instance))) {
      LOG_WDIAG("get primary role instance failed", K(ret));
    } else if (NULL == instance) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("get ldg piramy role from ocp failed", K(ret));
    } else {
      if (!instance->is_valid() || OB_ISNULL(client_session_->cluster_resource_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("ldg argument is not vaild", K(ret), KP(instance),
            KP(client_session_->cluster_resource_));
      } else if (!client_session_->cluster_resource_->check_tenant_valid(instance->ob_tenant_,
            instance->ob_cluster_)) {
        LOG_WDIAG("ldg check tenant valid failed", K_(instance->ob_tenant), K_(instance->ob_cluster));
      } else if (instance->ob_cluster_.get_string() != ldg_real_cluster_name
          || instance->ob_tenant_.get_string() != ldg_real_tenant_name) {
        if (ObMysqlTransact::is_in_trans(trans_state_)) {
          // do nothing, keep origin session
          LOG_WDIAG("in trans, keep origin session");
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
      LOG_WDIAG("ldg rewrite ldg login req failed", K(ret));
      tmp_ret = ret;
    } else if (OB_FAIL(save_user_login_info(session_info, hsr))) {
      LOG_WDIAG("save user login info failed", K(ret), K(hsr), K(session_info));
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
    // 如果出现问题，连接之前的集群，否则会连接失败，出现业务报错
    ret = tmp_ret;
    LOG_WDIAG("handle ldg failed, try to use old session according ret", K(ret));
  }

  return ret;
}

void ObMysqlSM::setup_get_cluster_resource()
{
  int ret = OB_SUCCESS;
  ObAction *cr_handler = NULL;

  if (OB_UNLIKELY(OB_MYSQL_COM_LOGIN == trans_state_.trans_info_.sql_cmd_
      || need_renew_cluster_resource_
      || (client_session_->get_session_info().is_sharding_user() && NULL == client_session_->cluster_resource_))) {
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
          LOG_WDIAG("fail to get cluster info, this connection will disconnect",
                   K_(sm_id), K(is_clustername_from_default), K(cluster_name), K(cluster_id), K(ret));
        } else if (OB_FAIL(session_info.set_cluster_info(get_global_proxy_config().enable_cluster_checkout,
                                                         cluster_name,
                                                         real_meta_cluster_name,
                                                         cluster_id,
                                                         client_session_->need_delete_cluster_))) {
          LOG_WDIAG("fail to set cluster info, this connection will disconnect",
                   K_(sm_id), K(cluster_name), K(cluster_id), K(ret));
        } else if (OB_FAIL(process_cluster_resource(cr))) {
          LOG_WDIAG("fail to process_cluster_resource", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_EDIAG("cluster resource must be NULL here", K(ret));
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
          if (csp.is_cluster_array_empty()) {
            COLLECT_LOGIN_DIAGNOSIS(
                connection_diagnosis_trace_, OB_LOGIN_DISCONNECT_TRACE, "",
                OB_CLUSTER_NOT_EXIST,
                "all cluster info is empty, please check the config server");
          } else {
            COLLECT_LOGIN_DIAGNOSIS(
                connection_diagnosis_trace_, OB_LOGIN_DISCONNECT_TRACE, "",
                OB_CLUSTER_NOT_EXIST, "cluster does not exist");
          }
          ret = OB_CLUSTER_NOT_EXIST;
          LOG_WDIAG("cluster does not exist, this connection will disconnect",
                   K_(sm_id), K(is_clustername_from_default), K(cluster_name), K(ret));
          // return err packet to client "Access denied for ..."
          trans_state_.mysql_errcode_ = OB_CLUSTER_NOT_EXIST;
          int tmp_ret = OB_SUCCESS;
          if (OB_UNLIKELY(OB_SUCCESS !=
                          (tmp_ret = ObMysqlTransact::encode_error_message(trans_state_)))) {
            LOG_WDIAG("fail to build err packet", K(tmp_ret));
          } else {
            trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
          }
        } else {
          LOG_WDIAG("fail to get cluster info, this connection will disconnect",
                   K_(sm_id), K(is_clustername_from_default), K(cluster_name), K(ret));
        }
      } else if (OB_FAIL(session_info.set_cluster_info(get_global_proxy_config().enable_cluster_checkout,
                                                       cluster_name,
                                                       real_meta_cluster_name,
                                                       cluster_id,
                                                       client_session_->need_delete_cluster_))) {
        LOG_WDIAG("fail to set cluster info, this connection will disconnect",
                 K_(sm_id), K(cluster_name), K(cluster_id), K(ret));
      } else {
        MYSQL_SM_SET_DEFAULT_HANDLER(&ObMysqlSM::state_get_cluster_resource);
        milestones_.cluster_resource_create_begin_ = get_based_hrtime();

        if (enable_record_full_link_trace_info()) {
          if (flt_.trace_log_info_.cluster_resource_create_ctx_ == NULL) {
            SET_TRACE_BUFFER(flt_trace_buffer_, MAX_TRACE_LOG_SIZE);
            trace::ObSpanCtx *ctx = FLT_BEGIN_SPAN(ob_proxy_cluster_resource_create);
            if (OB_NOT_NULL(ctx)) {
              flt_.trace_log_info_.cluster_resource_create_ctx_ = ctx;
              LOG_DEBUG("begin span ob_proxy_cluster_resource_create", K(ctx->span_id_));
            }
          }
        }
        // collect basic info before get cluster resource
        ret = get_global_resource_pool_processor().get_cluster_resource(*this,
              (process_async_task_pfn)&ObMysqlSM::process_cluster_resource,
              client_session_->is_proxy_mysql_client_, cluster_name, cluster_id, connection_diagnosis_trace_, cr_handler);
        if (OB_FAIL(ret)) {
          LOG_WDIAG("cluster_resource_handler is ACTION_RESULT_NONE, something is wrong",
                   K_(sm_id), K(cluster_name), K(cluster_id), K(ret));
        } else if (OB_SUCC(ret) && (NULL != cr_handler)) {
          LOG_DEBUG("should create and init cluster_resource and assign pending action",
                    K_(sm_id), K(cluster_name), K(cluster_id));
          if (OB_UNLIKELY(NULL != pending_action_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("pending_action must be NULL here", K_(pending_action), K_(sm_id), K(ret));
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
          LOG_WDIAG("fail to fill proxysys user session priv info", K(ret));
        } else if (OB_ISNULL(default_sysvar_set = get_global_resource_pool_processor().get_default_sysvar_set())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("default_sysvar_set is null", K(ret));
        } else if (OB_FAIL(client_session_->get_session_info().add_sys_var_set(*default_sysvar_set))) {
          LOG_WDIAG("fail to add sys var set", K(ret));
        } else {
          LOG_DEBUG("succ to add sys var set", "cached_variables", client_session_->get_session_info().get_cached_variables());
        }
      }
    } else {
      // do nothing
    }
  } else {
    // mainly for performance testing
    if (OB_UNLIKELY(OB_MYSQL_COM_HANDSHAKE == trans_state_.trans_info_.sql_cmd_
        && trans_state_.mysql_config_params_->is_mysql_routing_mode())) {
      ObResourcePoolProcessor &rp_processor = get_global_resource_pool_processor();

      ObString cluster_name;
      if (OB_SUCC(rp_processor.get_first_cluster_name(cluster_name))) {
        LOG_DEBUG("get cluster resource in COM_HANDSHAKE", K_(sm_id),
                  K(cluster_name), "tenant_name", OB_SYS_TENANT_NAME,
                  "user_name", OB_SYS_USER_NAME);

        ObClientSessionInfo &session_info = client_session_->get_session_info();
        if (OB_FAIL(session_info.set_user_name(ObString::make_string(OB_SYS_USER_NAME)))) {
          LOG_WDIAG("fail to set user name", K_(sm_id), K(ret));
        } else if (OB_FAIL(session_info.set_tenant_name(ObString::make_string(OB_SYS_TENANT_NAME)))) {
          LOG_WDIAG("fail to set tenant name", K_(sm_id), K(ret));
        } else if (OB_FAIL(session_info.set_cluster_name(cluster_name))) {
          // If there is no default cluster name, here maybe fail.
          LOG_WDIAG("fail to set cluster name", K_(sm_id), K(ret));
        } else if (OB_FAIL(session_info.set_database_name(ObString::make_string("test"), false))) {
          LOG_WDIAG("fail to set cluster name", K_(sm_id), K(ret));
        } else {
          // do nothing
        }

        if (OB_SUCC(ret)) {
          int64_t cluster_id = OB_INVALID_CLUSTER_ID;
          // collect basic info before get cluster resource
          ret = get_global_resource_pool_processor().get_cluster_resource(*this,
                (process_async_task_pfn)&ObMysqlSM::process_cluster_resource,
                client_session_->is_proxy_mysql_client_, cluster_name, cluster_id, connection_diagnosis_trace_, cr_handler);

          if (OB_SUCC(ret) && NULL != cr_handler) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("in mysql mode, we use default cluster resource, cr_handler must be null", K(cr_handler), K(ret));
            if (NULL != pending_action_) {
              LOG_WDIAG("pending_action must be NULL here", K_(pending_action), K_(sm_id), K(ret));
            } else {
              pending_action_ = cr_handler;
            }
          } else if (OB_FAIL(ret)) {
            LOG_WDIAG("fail to get cluster resource, will disconnect", K(ret));
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
    COLLECT_LOGIN_DIAGNOSIS(connection_diagnosis_trace_,
                            OB_LOGIN_DISCONNECT_TRACE, "", OB_CLUSTER_NOT_EXIST,
                            "cluster info is empty or client session closed");
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(data), K_(client_session), K_(sm_id), K(ret));
  } else {
    ObClusterResource *cluster_resource = reinterpret_cast<ObClusterResource *>(data);
    if (cluster_resource->is_deleting()) { // maybe has already been deleted
      ret = OB_INNER_STAT_ERROR;
      LOG_WDIAG("this cluster resource has been deleted", K(ret), KPC(cluster_resource));
      cluster_resource->dec_ref();
      cluster_resource = NULL;
    } else {
      ObClientSessionInfo &session_info = client_session_->get_session_info();
      ObSysVarSetProcessor &sys_var_set_processor = cluster_resource->sys_var_set_processor_;
      if (OB_FAIL(session_info.revalidate_sys_var_set(sys_var_set_processor))) {
        LOG_WDIAG("fail to revalidate sys var set", K_(sm_id), K(ret));
        cluster_resource->dec_ref();
        cluster_resource = NULL;
      } else {
        if (!client_session_->is_proxy_mysql_client_) {
          cluster_resource->renew_last_access_time();
        }
        if (NULL != client_session_->cluster_resource_) {
          LOG_WDIAG("cluster resource must be NULl here, or will mem leak",
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
          LOG_WDIAG("failed to fill session priv info", K_(sm_id), K(ret));
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

  if (enable_record_full_link_trace_info()) {
    trace::ObSpanCtx *ctx = flt_.trace_log_info_.cluster_resource_create_ctx_;
    if (OB_NOT_NULL(ctx)) {
      // set show trace buffer before flush trace
      if (flt_.control_info_.is_show_trace_enable()) {
        SET_SHOW_TRACE_INFO(&flt_.show_trace_json_info_.curr_sql_json_span_array_);
      }
      LOG_DEBUG("end span ob_proxy_cluster_resource_create", K(ctx->span_id_));
      SET_TRACE_BUFFER(flt_trace_buffer_, MAX_TRACE_LOG_SIZE);
      FLT_END_SPAN(ctx);
      flt_.trace_log_info_.cluster_resource_create_ctx_ = NULL;   // logically free, mem still in OBTRACE
    }
  }

  switch (event) {
    case CLUSTER_RESOURCE_CREATE_COMPLETE_EVENT:
      pending_action_ = NULL;
      if (OB_ISNULL(data)) {
        ret = OB_ERR_NULL_VALUE;
        COLLECT_LOGIN_DIAGNOSIS(connection_diagnosis_trace_,
                        OB_LOGIN_DISCONNECT_TRACE, "", OB_CLUSTER_NOT_EXIST,
                        "cluster info is empty");
        LOG_WDIAG("data is NULL", K_(sm_id), K(ret));
      } else if (OB_FAIL(process_cluster_resource(data))) {
        LOG_WDIAG("fail to get cluster_resource, will disconnect", K_(sm_id), K(ret));
      } else {
        call_transact_and_set_next_state(ObMysqlTransact::modify_request);
      }
      break;
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("Unexpected event", K_(sm_id), K(event), K(ret));
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

inline int ObMysqlSM::init_request_content(ObRequestAnalyzeCtx &ctx, const bool is_mysql_req_in_ob20)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(trans_state_.is_auth_request_)) {
    ObMysqlAuthRequest &orig_auth_req = client_session_->get_session_info().get_login_req();
    orig_auth_req.reset();
    if (client_session_->is_need_convert_vip_to_tname() && client_session_->is_vip_lookup_success()) {
      ctx.vip_tenant_name_ = client_session_->get_vip_tenant_name();
      ctx.vip_cluster_name_ = client_session_->get_vip_cluster_name();
    } else {
      ctx.vip_tenant_name_.assign_ptr(OB_SYS_TENANT_NAME,
                                      static_cast<int32_t>(STRLEN(OB_SYS_TENANT_NAME)));
      if (OB_FAIL(get_global_resource_pool_processor().get_first_cluster_name(ctx.vip_cluster_name_))) {
        LOG_WDIAG("fail to get first cluster name", K_(sm_id), K(ret));
      }
    }
  } else {
    ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
    client_request.reuse();
    const bool eaic = client_session_->enable_analyze_internal_cmd();
    client_request.set_enable_analyze_internal_cmd(eaic);
    client_request.set_user_identity(client_session_->get_user_identity());
    client_request.set_mysql_req_in_ob20_payload(is_mysql_req_in_ob20);
    client_request.set_enable_server_kill_connection(client_session_->get_session_info().is_server_support_cs_id_v2() && client_session_->is_cs_id_v2());

    ctx.is_sharding_mode_ = client_session_->get_session_info().is_sharding_user();
    ctx.connection_collation_ = static_cast<common::ObCollationType>(client_session_->get_session_info().get_collation_connection());
  }

  if (OB_SUCC(ret)) {
    ctx.reader_ = client_buffer_reader_;
    ctx.is_auth_ = trans_state_.is_auth_request_;

    ctx.parse_mode_ = NORMAL_PARSE_MODE;
    ctx.cached_variables_ = &client_session_->get_session_info().get_cached_variables();

    ctx.large_request_threshold_len_ = trans_state_.mysql_config_params_->tunnel_request_size_threshold_;
    ctx.request_buffer_length_ = trans_state_.mysql_config_params_->request_buffer_length_;

    ctx.using_ldg_ = client_session_->using_ldg();
    ctx.using_service_name_ = client_session_->using_service_name();
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
        LOG_WDIAG("client connections reach throttle",
                K(currently_open), K(max_client_connection),
                K(throttle), K(enable_client_connection_lru_disconnect), K_(sm_id));
      }
    }
  }
  return throttle;
}

bool ObMysqlSM::can_pass_white_list()
{
  bool can_pass = false;
  if (RUN_MODE_CLIENT == g_run_mode) {
    can_pass = true;
  } else {
    ObStringKV string_kv;
    ObClientSessionInfo &session_info = client_session_->get_session_info();
    ObHSRResult &hsr = session_info.get_login_req().get_hsr_result();
    ObUnixNetVConnection* unix_vc = static_cast<ObUnixNetVConnection *>(client_session_->get_netvc());
    if (OB_UNLIKELY(NULL == unix_vc)) {
        LOG_WDIAG("invalid unix_vc");
    } else if (get_global_white_list_table_processor().can_ip_pass(hsr.cluster_name_, hsr.tenant_name_, hsr.user_name_, unix_vc->get_real_client_addr())) {
        can_pass = true;
    }
  }

  return can_pass;
}

/*
 * support ob2.0 protocol between client and proxy
 * client will send cap through CLIENT_CONNECT_ATTRS in mysql handshake response packet.
 * key: "__proxy_capability_flag"
 *
 * client supported ob2.0 protocol or not
 * while negotiate with client, not only the cap shift OB_CAP_OB_PROTOCOL_V2 should be set,
   but also the CONN_ATTR kv of key:"__mysql_client_type" and value "__ob_libobclient"/"__ob_jdbc_client"
   should be set, to indicate that client support ob2.0 protocol transfer.
 */
int ObMysqlSM::analyze_capacity_flag_from_client()
{
  int ret = OB_SUCCESS;

  ObClientSessionInfo &session_info = client_session_->get_session_info();
  ObHSRResult &hsr = session_info.get_login_req().get_hsr_result();
  int64_t total_conn_attrs = hsr.response_.get_connect_attrs().count();
  ObStringKV string_kv;
  bool is_client_send_client_mode = false;

  for (int64_t i = 0; OB_SUCC(ret) && i < total_conn_attrs; ++i) {
    string_kv = hsr.response_.get_connect_attrs().at(i);
    if (0 == string_kv.key_.case_compare(OB_MYSQL_CAPABILITY_FLAG)) {
      int64_t orig_client_cap = 0;
      if (OB_FAIL(get_int_value(string_kv.value_, orig_client_cap))) {
        LOG_WDIAG("fail to get int value from cap flag", K(ret), K(string_kv.key_), K(string_kv.value_));
      } else {
        uint64_t set_client_cap = orig_client_cap & OBPROXY_DEFAULT_CAPABILITY_FLAG;
        session_info.set_client_ob_capability(set_client_cap);
        LOG_DEBUG("succ to analyze origin capability flag from client", K(orig_client_cap), K(set_client_cap));
      }
    } else if (0 == string_kv.key_.case_compare(OB_MYSQL_CLIENT_MODE)) {
      // another tag for client to judge the support of ob2.0
      if (0 == string_kv.value_.compare(OB_MYSQL_CLIENT_LIBOBCLIENT_MODE)
          || 0 == string_kv.value_.compare(OB_MYSQL_CLIENT_JDBC_CLIENT_MODE)) {
        is_client_send_client_mode = true;
        LOG_DEBUG("client transfered mysql client mode", K(string_kv.value_));
      }
    } else {
      continue;
    }
  }

  // whether client truely support ob2.0 or not
  // whether proxy config flag set or not
  if (OB_SUCC(ret)) {
    uint64_t origin_client_cap = session_info.get_client_ob_capability();
    if (!session_info.is_client_support_ob20_protocol()
        || !is_client_send_client_mode
        || !get_global_proxy_config().enable_ob_protocol_v2_with_client) {
      origin_client_cap &= ~(OB_CAP_OB_PROTOCOL_V2);
      session_info.set_client_ob_capability(origin_client_cap);
    }
  }

  if (OB_SUCC(ret)) {
    const uint64_t last_client_cap = session_info.get_client_ob_capability();
    const bool is_client_support_ob20 = session_info.is_client_support_ob20_protocol();
    LOG_INFO("final client capability flag in negotiation", K(last_client_cap), K(is_client_support_ob20));
  }

  return ret;
}

// chect weahter connections reach throttle
inline bool ObMysqlSM::check_vt_connection_throttle()
{
  #ifdef ERRSIM
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_E(EventTable::EN_CHECK_TENANT_MAX_CONNECTION_FAIL) OB_SUCCESS)) {
    return true;
  }
  #endif
  ObString cluster_name;
  ObString tenant_name;
  ObString ip_name;
  ObClientSessionInfo &cs_info = client_session_->get_session_info();
  if (client_session_->is_need_convert_vip_to_tname() &&
    client_session_->is_vip_lookup_success()) {
    // 处理公有云vip信息
    cluster_name = client_session_->get_vip_cluster_name();
    tenant_name  = client_session_->get_vip_tenant_name();
    cs_info.get_vip_addr_name(ip_name);
  } else {
    // 拿不到vip信息的私有云场景: 没有vip的概念，只有集群和租户
    cs_info.get_cluster_name(cluster_name);
    cs_info.get_tenant_name(tenant_name);
  }

  return get_global_conn_table_processor().check_and_inc_conn(
    cluster_name, tenant_name, ip_name);
}

/*
 * proxy will do parition route with observer, the cap is determined by proxy config
 */
inline bool ObMysqlSM::is_partition_table_route_supported()
{
  bool bret = false;
  if (trans_state_.mysql_config_params_->enable_partition_table_route_) {
    if (NULL != client_session_
        && client_session_->get_session_info().is_server_support_partition_table()) {
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
        && client_session_->get_session_info().is_server_support_pl_route()) {
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
  if (need_reject_user_login(user_name, tenant_name, hsr.has_tenant_username_, hsr.has_cluster_username_, is_current_cloud_user)) {
    ret = OB_USER_NOT_EXIST;
    LOG_WDIAG("access denied for this user", K(hsr), K(is_current_cloud_user), K(ret));
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
      LOG_WDIAG("fail to get meta table info", K_(sm_id), K(ret));
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
    LOG_WDIAG("fail to set user name", K_(sm_id), K(ret));
  } else if (OB_FAIL(session_info.set_tenant_name(hsr_result.tenant_name_))) {
    LOG_WDIAG("fail to set tenant name", K_(sm_id), K(ret));
  } else if (OB_FAIL(session_info.set_cluster_name(hsr_result.cluster_name_))) {
    LOG_WDIAG("fail to set cluster name", K_(sm_id), K(ret));
  } else {
    // no need add database name to client session info here,
    // because if login request has -D database option, obproxy
    // will get changed database name from response ok packet;
    trans_state_.refresh_mysql_config();
  }
  return ret;
}

void ObMysqlSM::analyze_mysql_request(ObMysqlAnalyzeStatus &status, const bool is_mysql_req_in_ob20)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
    milestones_.client_.analyze_request_begin_ = get_based_hrtime();
  }

  if (0 == cmd_size_stats_.client_request_bytes_ && 0 == milestones_.client_.analyze_request_end_) {
    cmd_time_stats_.client_request_read_time_ += (milestones_.client_.analyze_request_begin_ - milestones_.client_.client_begin_);
  } else if (cmd_size_stats_.client_request_bytes_ > 0 && milestones_.client_.analyze_request_end_ > 0) {
    cmd_time_stats_.client_request_read_time_ += (milestones_.client_.analyze_request_begin_ - milestones_.client_.analyze_request_end_);
  }

  ObRequestAnalyzeCtx ctx;
  if (OB_ISNULL(client_session_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("client session is NULL in init_request_content", K_(sm_id), K(ret));

  // 1. init request content at first
  } else if (OB_FAIL(init_request_content(ctx, is_mysql_req_in_ob20))) {
    LOG_WDIAG("fail to init requtest content", K_(sm_id), K(ret));
  } else {
    // 2. analyze mysql request(auth request or common mysql request)
    ObClientSessionInfo &session_info = client_session_->get_session_info();
    ObMysqlAuthRequest &orig_auth_req = session_info.get_login_req();
    ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
    ObMySQLCmd &req_cmd = trans_state_.trans_info_.sql_cmd_;

    // for load content of file we couldn't parse cmd from request pkt
    // so we set req_cmd according to the last 0xfb resp
    if (OB_UNLIKELY(trans_state_.trans_info_.resp_result_.is_local_infile_0xfb_resp())) {
      req_cmd = OB_MYSQL_COM_LOAD_DATA_TRANSFER_CONTENT;
      LOG_DEBUG("transferring content of file request", K(req_cmd));
    }

    ObMysqlRequestAnalyzer::analyze_request(ctx, orig_auth_req, client_request, req_cmd, status,
                                            session_info.is_oracle_mode(),
                                            session_info.is_client_support_ob20_protocol());

    // get last server response
    if (trans_state_.trans_info_.resp_result_.is_auth_switch_req()) {
      req_cmd = OB_MYSQL_COM_AUTH_SWITCH_RESP;
      LOG_DEBUG("analyzed auth switch response", K(trans_state_.trans_info_.sql_cmd_));
    }

    if (OB_NOT_NULL(protocol_diagnosis_)) {
      // protocol diagnosis needs current request cmd type
      protocol_diagnosis_->set_sql_cmd(req_cmd);
    }

    if (OB_LIKELY(ANALYZE_DONE == status)) {
      if (client_request.get_parse_result().has_explain_route()) {
        if (OB_ISNULL(route_diagnosis_)) {
          if (OB_ISNULL(route_diagnosis_ = op_alloc(ObRouteDiagnosis))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WDIAG("fail to alloc memory to for route diagnosis for explain route", K(ret));
          } else {
            route_diagnosis_->inc_ref();
            LOG_DEBUG("succ to init route diagnosis for explain route");
          }
        }
      }
      if (OB_NOT_NULL(route_diagnosis_)) {
        route_diagnosis_->set_is_support_explain_route(client_request);
        route_diagnosis_->set_is_request_diagnostic(client_request);
      }
      // 3. if BEGIN or START TRANSACTION, try to hold it
      if (OB_LIKELY(OB_MYSQL_COM_QUERY == req_cmd)) {
        MYSQL_INCREMENT_DYN_STAT(TOTAL_QUERY_COUNT);
        // Not set start trans sql
        // 1. already in transaction
        // 2. already hold xa start
        if (OB_UNLIKELY(client_request.get_parse_result().need_hold_start_trans()
                        && trans_state_.is_trans_first_request_
                        && !trans_state_.is_hold_xa_start_)) {
          if (OB_FAIL(session_info.set_start_trans_sql(client_request.get_sql()))) {
            LOG_WDIAG("fail to set start transaction sqld", K_(sm_id), K(ret));
          }
        }
        //处理文本ps功能
        if (OB_SUCC(ret)) {
          if (OB_UNLIKELY(client_request.get_parse_result().is_text_ps_prepare_stmt())) {
            if (OB_FAIL(analyze_text_ps_prepare_request(ctx))) {
              LOG_WDIAG("analyze text ps prepare request failed", K(ret));
            }
          } else if (OB_UNLIKELY(client_request.get_parse_result().is_text_ps_execute_stmt())) {
            if (OB_FAIL(analyze_text_ps_execute_request())) {
              LOG_WDIAG("analyze text ps execute request failed", K(ret));
            }
          } else if (OB_UNLIKELY(client_request.get_parse_result().is_text_ps_drop_stmt())) {
            if (OB_FAIL(analyze_text_ps_drop_request())) {
              LOG_WDIAG("analyze text ps drop request failed", K(ret));
            }
          }
        }
        // 4. if COM_LOGIN, do some check
      } else if (OB_UNLIKELY(OB_MYSQL_COM_LOGIN == req_cmd)) {
        if (OB_FAIL(analyze_login_request(ctx, status))) {
          LOG_WDIAG("fail to analyze login request", K(ret));
        }
      } else if (OB_MYSQL_COM_RESET_CONNECTION == req_cmd) {
        if (session_info.is_sharding_user() || session_info.is_session_pool_client_) {
          if (OB_FAIL(ObMysqlTransact::encode_error_message(trans_state_, OB_NOT_SUPPORTED))) {
            LOG_WDIAG("fail to encode unsupport change user error message", K(ret));
          } else {
            LOG_INFO("not support change user");
          }
          status = ANALYZE_ERROR;
        }
      } else if (OB_MYSQL_COM_STMT_PREPARE == req_cmd) {
        if (client_request.get_parse_result().is_start_trans_stmt() || session_info.is_sharding_user()) {
          if (OB_FAIL(ObMysqlTransact::encode_error_message(trans_state_, OB_UNSUPPORTED_PS))) {
            LOG_WDIAG("fail to encode unsupport ps error message", K(ret));
          } else {
            if (client_request.get_parse_result().is_start_trans_stmt()) {
              LOG_WDIAG("begin statement is not supported in prepare stament", K(ret));
            } else {
              LOG_WDIAG("sharding user is not supported for prepare stament", K(ret));
            }
          }

          status = ANALYZE_ERROR;
        } else if (OB_FAIL(analyze_ps_prepare_request())) {
          LOG_WDIAG("fail to analyze ps prepare request", K(ret));
        }
      } else if (OB_MYSQL_COM_STMT_EXECUTE == req_cmd
                 || OB_MYSQL_COM_STMT_SEND_PIECE_DATA == req_cmd
                 || OB_MYSQL_COM_STMT_SEND_LONG_DATA == req_cmd) {
        if (OB_FAIL(analyze_ps_execute_request())) {
          if (ret == OB_ERR_PREPARE_STMT_NOT_FOUND) {
            if (OB_FAIL(ObMysqlTransact::encode_error_message(trans_state_, OB_ERR_PREPARE_STMT_NOT_FOUND))) {
              LOG_WDIAG("fail to encode error of stmt not found will disconnect", K(ret));
            } else {
              LOG_DEBUG("succ to encode error of prepare stmt not found");
              status = ANALYZE_ERROR;
            }
          } else {
            LOG_WDIAG("fail to analyze ps request will disconnect", K(ret), K(req_cmd));
          }
        }
      } else if (OB_MYSQL_COM_STMT_FETCH == req_cmd || OB_MYSQL_COM_STMT_GET_PIECE_DATA == req_cmd) {
        // 每次执行COM_STMT_GET_PIECE_DATA也需要设置下cursor id，因为
        // 行为相同，所以此处复用了analyze_fetch_request函数
        if (OB_FAIL(analyze_fetch_request())) {
          LOG_WDIAG("fail to analyze fetch request", K(ret));
        }
      } else if (OB_MYSQL_COM_STMT_CLOSE == req_cmd || OB_MYSQL_COM_STMT_RESET == req_cmd) {
        if (OB_FAIL(analyze_close_reset_request())) {
          LOG_WDIAG("fail to analyze fetch request", K(ret));
        }
      } else if (OB_MYSQL_COM_STMT_PREPARE_EXECUTE == req_cmd) {
        if (client_request.get_parse_result().is_start_trans_stmt()) {
          if (OB_FAIL(ObMysqlTransact::encode_error_message(trans_state_, OB_UNSUPPORTED_PS))) {
            LOG_WDIAG("fail to encode unsupport ps error message", K(ret));
          } else {
            LOG_WDIAG("begin statement is not supported in prepare execute stament", K(ret));
          }
        } else if (OB_FAIL(analyze_ps_prepare_execute_request())) {
          LOG_WDIAG("fail to analyze ps prepare execute request", K(ret));
        // Not set start trans sql
        // 1. already in transaction
        // 2. already hold xa start
        // 3. already hold begin
        } else if (get_global_proxy_config().enable_xa_route
                   && client_request.get_parse_result().need_hold_xa_start()
                   && trans_state_.is_trans_first_request_
                   && !trans_state_.is_hold_start_trans_
                   && !trans_state_.is_hold_xa_start_) {
          LOG_DEBUG("[ObMysqlSM::analyze_mysql_request] save xa start request packet");
          session_info.set_start_trans_sql(client_request.get_req_pkt());
          // save the xa start ps id, session_info.ps_id_ will be reset at ObMysqlSM::setup_cmd_complete()
          session_info.set_xa_start_ps_id(session_info.get_client_ps_id());
        }
      } else if (OB_MYSQL_COM_CHANGE_USER == req_cmd) {
        if (OB_UNLIKELY(session_info.is_sharding_user()
            || client_session_->using_ldg()
            || client_session_->using_service_name())) {
          if (OB_FAIL(ObMysqlTransact::encode_error_message(trans_state_, OB_NOT_SUPPORTED))) {
            LOG_WDIAG("fail to encode unsupport change user error message", K(ret));
          } else {
            LOG_WDIAG("sharding or ldg not support change user", K(session_info.is_sharding_user()),
                KPC_(client_session), K(ret));
          }
          status = ANALYZE_ERROR;
        } else if (OB_FAIL(analyze_change_user_request())) {
          if (OB_LIKELY(ret == OB_NOT_SUPPORTED)) {
            ret = OB_SUCCESS;
            status = ANALYZE_NOT_SUPPORT;
            LOG_INFO("user try to change tenant", K(ret));
          } else {
            LOG_WDIAG("fail to analyze change user request", K(ret));
          }
        }
      } else {
        // do nothing
      }
    } else if (ANALYZE_CONT == status)  {
      // large request means we have received enough packet(> request_buffer_len_)
      if ((OB_MYSQL_COM_STMT_EXECUTE == req_cmd
           || OB_MYSQL_COM_STMT_SEND_PIECE_DATA == req_cmd
           || OB_MYSQL_COM_STMT_SEND_LONG_DATA == req_cmd)
          && client_request.is_large_request()) {
        if (OB_FAIL(analyze_ps_execute_request(client_request.is_large_request()))) {
          LOG_WDIAG("fail to analyze ps execute request", K(ret));
        }
      } else if (OB_MYSQL_COM_STMT_PREPARE_EXECUTE == req_cmd && client_request.is_large_request()) {
        if (OB_FAIL(analyze_ps_prepare_execute_request())) {
          LOG_WDIAG("fail to analyze ps prepare execute request", K(ret));
        }
      } else if (client_request.get_parse_result().is_text_ps_execute_stmt() &&
        client_request.is_large_request()) {
        if (OB_FAIL(analyze_text_ps_execute_request())) {
          LOG_WDIAG("fail to analyze text ps execute request", K(ret));
        }
      }
    } else {
      // is not ANALYZE_DONE, do nothing
    }
  }

  if (OB_FAIL(ret)) {
    status = ANALYZE_ERROR;
  }

  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
    milestones_.client_.analyze_request_end_ = get_based_hrtime();
  }
  cmd_time_stats_.client_request_analyze_time_ +=
    milestone_diff(milestones_.client_.analyze_request_begin_, milestones_.client_.analyze_request_end_);
}

int ObMysqlSM::analyze_change_user_request()
{
  int ret = OB_SUCCESS;
  ObClientSessionInfo& client_info = client_session_->get_session_info();
  // len 表示整个 COM_CHANGE_USER 报文的大小
  int64_t len = trans_state_.trans_info_.client_request_.get_packet_meta().pkt_len_;
  if (OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid len", K(len), K(ret));
  } else {
    ObVariableLenBuffer<128> user_buffer;
    if (OB_FAIL(user_buffer.init(len))) {
      LOG_WDIAG("fail to init user buffer", K(ret));
    } else {
      char *start = const_cast<char *>(user_buffer.ptr());
      char *written_pos = client_buffer_reader_->copy(start, len, 0);
      if (OB_UNLIKELY(written_pos != start + len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("write pos not expected", K(written_pos), K(start), K(len), K(ret));
      } else if (OB_FAIL(client_buffer_reader_->consume_all())) {
        LOG_WDIAG("client_buffer_reader consume_all failed", K(ret));
      } else {
        ObMIOBuffer *write_buffer = client_buffer_reader_->writer();
        // buf 指向 COM_CHANGE_USER的username部分
        char *buf = start + MYSQL_NET_META_LENGTH;
        ObString username = ObString::make_string(buf);
        int64_t name_len = strlen(buf);
        ObHSRResult result;
        int64_t written_len = 0;
        ObString& tenant_name = client_info.get_login_req().get_hsr_result().tenant_name_;
        ObString& cluster_name = client_info.get_login_req().get_hsr_result().cluster_name_;
        if (OB_FAIL(ObProxyAuthParser::parse_full_user_name(username, tenant_name, cluster_name, result))) {
          LOG_WDIAG("parse full user name failed", K(ret));
        } else if (OB_FAIL(write_buffer->write(start, MYSQL_NET_META_LENGTH, written_len))) {
          LOG_WDIAG("fail to write header", K(ret));
        } else if (OB_UNLIKELY(MYSQL_NET_META_LENGTH != written_len)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("not write completely", K(written_len), K(ret));
        }
        // 写入用户名
        if (OB_SUCC(ret)) {
          if (OB_FAIL(write_buffer->write(result.user_name_.ptr(), result.user_name_.length(), written_len))) {
            LOG_WDIAG("fail to write username", K(ret));
          } else if (OB_UNLIKELY(result.user_name_.length() != written_len)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("not write completely", K(written_len), K(ret));
          }
        }
        // 写入租户名
        int64_t new_name_len = 0;
        if (OB_SUCC(ret)) {
          if (result.has_tenant_username_ && result.has_cluster_username_) {
            if (OB_UNLIKELY(tenant_name != result.tenant_name_ || cluster_name != result.cluster_name_)) {
              ret = OB_NOT_SUPPORTED;
              LOG_WDIAG("name not match", K(result), K(tenant_name), K(cluster_name), K(ret));
            }
          } else if (result.has_tenant_username_ && !result.has_cluster_username_) {
            if (OB_UNLIKELY(tenant_name != result.tenant_name_)) {
              ret = OB_NOT_SUPPORTED;
              LOG_WDIAG("name not match", K(result), K(tenant_name), K(ret));
            }
          } else if (!result.has_tenant_username_ && !result.has_cluster_username_) {
            // do nothing
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("should not come here", K(result), K(ret));
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(write_buffer->write("@", 1, written_len))) {
              LOG_WDIAG("fail to write @", K(ret));
            } else if (1 != written_len) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WDIAG("not write completely", K(written_len), K(ret));
            } else if (OB_FAIL(write_buffer->write(tenant_name.ptr(), tenant_name.length(), written_len))) {
              LOG_WDIAG("fail to write tenant_name", K(ret));
            } else if (tenant_name.length() != written_len) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WDIAG("not write completely", K(written_len), K(ret));
            } else {
              new_name_len = tenant_name.length() + result.user_name_.length() + 1;
            }
          }
          LOG_DEBUG("change user name", K(result), K(tenant_name));
        }
        // 写入报文剩余内容
        if (OB_SUCC(ret)) {
          if (OB_FAIL(write_buffer->write("\0", 1, written_len))) {
            LOG_WDIAG("fail to write string null", K(ret));
          } else if (1 != written_len) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("not write completely", K(written_len), K(ret));
          } else if (OB_FAIL(write_buffer->write(buf + name_len + 1, len - MYSQL_NET_META_LENGTH - name_len - 1, written_len))) {
            LOG_WDIAG("fail to write left buffer", K(ret));
          } else if (written_len != len - MYSQL_NET_META_LENGTH - name_len - 1) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("not write completely", K(written_len), K(ret));
          } else if (get_client_session_protocol() == ObProxyProtocol::PROTOCOL_OB20) {
            char crc_buf[4];
            memset(crc_buf, 0, sizeof(crc_buf));
            if (OB_FAIL(write_buffer->write(crc_buf, 4, written_len))) {
              LOG_WDIAG("fail to write crc_buf", K(ret));
            } else if (4 != written_len) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WDIAG("not write completely", K(written_len), K(ret));
            }
          }
        }
        // 重新修改报文长度
        if (OB_SUCC(ret)) {
          uint32_t new_len = static_cast<uint32_t>(len - MYSQL_NET_HEADER_LENGTH + new_name_len - name_len);
          client_buffer_reader_->replace(reinterpret_cast<const char*>(&new_len), 3, 0);
          ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
          client_request.get_packet_meta().pkt_len_ =  new_len + MYSQL_NET_HEADER_LENGTH;

          // rewrite remain payload len while consume buffer all and rewrite mysql request to buffer
          if (get_client_session_protocol() == ObProxyProtocol::PROTOCOL_OB20) {
            client_info.ob20_request_.remain_payload_len_ = client_request.get_packet_meta().pkt_len_;
          }

          if (OB_FAIL(client_request.add_request(client_buffer_reader_, trans_state_.mysql_config_params_->request_buffer_length_))) {
            LOG_WDIAG("fail to add com request", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

// 用户名|客户端地址|登录结果|失败原因
#define LOGIN_LOG_FORMAT "%.*s,%s,%s,%.*s"

#define LOGIN_LOG_PARAM \
           hsr.full_name_.length(), hsr.full_name_.ptr(), \
           client_ip_buf, \
           is_error_resp ? "failed" : "success", \
           error_msg.length(), error_msg.ptr()

int ObMysqlSM::analyze_login_request(ObRequestAnalyzeCtx &ctx, ObMysqlAnalyzeStatus &status)
{
  int ret = OB_SUCCESS;

  ObClientSessionInfo &session_info = client_session_->get_session_info();
  ObMysqlAuthRequest &orig_auth_req = session_info.get_login_req();
  ObHSRResult &hsr = orig_auth_req.get_hsr_result();
  // TODO: 需要考虑富客户端情况
  ObUnixNetVConnection* unix_vc = static_cast<ObUnixNetVConnection *>(client_session_->get_netvc());
  if (NULL == unix_vc) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("client entry vc is null", K(ret));
  } else if (!client_session_->is_proxy_mysql_client_ && hsr.response_.is_ssl_request() && !unix_vc->ssl_connected()) {
    if (OB_ISNULL(multi_level_config_)) {
      PROXY_LOG(WDIAG, "fail to init ssl, multi level config is null", K(ret));
    } else if (OB_FAIL(unix_vc->ssl_init(ObUnixNetVConnection::SSL_SERVER,
                                  client_session_->get_vip_cluster_name(),
                                  client_session_->get_vip_tenant_name(),
                                  multi_level_config_->ssl_attributes_.options_))) {
      LOG_WDIAG("ssl start handshake failed", K(ret));
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
      LOG_WDIAG("fail to check user sharding", K_(sm_id), K(ret));
      // add some message for login error
      bool need_response_for_stmt = false;
      ObProxyShardUtils::build_error_packet(ret, need_response_for_stmt, trans_state_, client_session_);
      //只要 check user 失败, 就断链接
      trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
      //save user name, tenant name, cluster name
    } else if (OB_FAIL(save_user_login_info(session_info, hsr))) {
      LOG_WDIAG("fail to save user login info", K_(sm_id), K(ret));
      //check user identity
    } else if (client_session_->get_session_info().is_oceanbase_server()
        && !session_info.is_sharding_user()
        && OB_FAIL(check_user_identity(hsr.user_name_, hsr.tenant_name_, hsr.cluster_name_))) {
      LOG_WDIAG("fail to check user identity", K_(sm_id), K(ret));
      if (OB_SUCCESS != ObMysqlTransact::encode_error_message(trans_state_, OB_PASSWORD_WRONG)) {
        LOG_WDIAG("fail to encode throttle message", K_(sm_id), K(ret));
      }
      trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
    } else {
      if (client_session_->is_proxysys_tenant()) {
        //proxysys user no need check everything
      } else {
        if (!client_session_->is_proxy_mysql_client_) {
          SESSION_PROMETHEUS_STAT(client_session_->get_session_info(), PROMETHEUS_CURRENT_SESSION, true, 1);
          client_session_->set_conn_prometheus_decrease(true);
          // 私有云场景is_vip_lookup_success 为flase
          // 公有云is_vip_lookup_success 为 false时也需要走一遍白名单流程
          if (!client_session_->is_vip_lookup_success() && !can_pass_white_list()) {
            COLLECT_LOGIN_DIAGNOSIS(
                connection_diagnosis_trace_, obutils::OB_LOGIN_DISCONNECT_TRACE,
                "", OB_ERR_CAN_NOT_PASS_WHITELIST,
                "user %.*s@%.*s can not pass white list",
                hsr.user_name_.length(), hsr.user_name_.ptr(), hsr.tenant_name_.length(), hsr.tenant_name_.ptr());
            status = ANALYZE_CAN_NOT_PASS_WHITE_LIST_ERROR;
          } else if (check_connection_throttle()) {
            trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
            // check client connection throttle count
            if (OB_FAIL(ObMysqlTransact::encode_error_message(trans_state_, OB_ERR_TOO_MANY_SESSIONS))) {
              LOG_WDIAG("fail to encode throttle message", K_(sm_id), K(ret));
            }

            COLLECT_LOGIN_DIAGNOSIS(connection_diagnosis_trace_,
                                    obutils::OB_LOGIN_DISCONNECT_TRACE, "",
                                    OB_ERR_TOO_MANY_SESSIONS, NULL);
            status = ANALYZE_ERROR; // disconnect
          } else if (!client_session_->is_proxy_mysql_client_
                 && !unix_vc->using_ssl()
                 && OB_NOT_NULL(multi_level_config_)
                 && multi_level_config_->ssl_attributes_.force_using_ssl_
                 && multi_level_config_->enable_client_ssl_
                 && !hsr.response_.is_ssl_request()) {
            ret = OB_ERR_UNEXPECTED;
            COLLECT_LOGIN_DIAGNOSIS(
                connection_diagnosis_trace_, obutils::OB_LOGIN_DISCONNECT_TRACE,
                "", OB_SSL_ERROR,
                "obproxy is configured to force ssl connection");
            LOG_WDIAG("not using ssl unexpected", K(ret));
          } else if (OB_FAIL(analyze_capacity_flag_from_client())) {
            LOG_WDIAG("fail to analyze capacity flag from client", K(ret));
            status = ANALYZE_ERROR;
          } else {
            if (check_vt_connection_throttle()) {
              // check cloud vip connection throttle count
              if (OB_FAIL(ObMysqlTransact::encode_error_message(trans_state_, OB_ERR_CON_COUNT_ERROR))) {
                LOG_WDIAG("fail to encode vip throttle message", K_(sm_id), K(ret));
              }
              COLLECT_LOGIN_DIAGNOSIS(connection_diagnosis_trace_,
                                      obutils::OB_LOGIN_DISCONNECT_TRACE, "",
                                      OB_ERR_TOO_MANY_SESSIONS,
                                      "tenant %.*s hold too many connections",
                                      hsr.tenant_name_.length(), hsr.tenant_name_.ptr());
              status = ANALYZE_ERROR; // disconnect
            } else {
              client_session_->set_vip_connection_decrease(true);
            }
          }
        }

        // the data which remains in client_buffer_reader_ is orig auth request,
        // and will not send to observer;
        // later we will rewrite_first_login_req, send first_auth_req to observer
        // and consume the client_buffer_reader_;

      }//end if !proxysys
    }
  }
  return ret;
}

int ObMysqlSM::do_analyze_ps_prepare_request(const ObString &ps_sql)
{
  int ret = OB_SUCCESS;

  // here we have the complete request in miobuffer, but client request buffer with
  // configured request_buffer_length may be not enough to copy all data,
  // we need copy complete ps sql
  ObClientSessionInfo &session_info = client_session_->get_session_info();
  ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;

  ObPsEntry *ps_entry = NULL;
  ObGlobalPsEntry *global_ps_entry = NULL;
  ObPsIdEntry *ps_id_entry = NULL;
  if (get_global_proxy_config().enable_global_ps_cache) {
    ObBasePsEntryGlobalCache& ps_entry_global_cache = get_global_ps_entry_cache();
    if (OB_FAIL(ps_entry_global_cache.acquire_or_create_ps_entry(ps_sql, client_request.get_parse_result(), global_ps_entry))) {
      LOG_WDIAG("create ps entry failed", K(ps_sql), K(ret));
    } else {
      ps_entry = global_ps_entry;
    }
  } else {
    ObBasePsEntryThreadCache &ps_entry_thread_cache = self_ethread().get_ps_entry_cache();
    if (OB_FAIL(ps_entry_thread_cache.acquire_or_create_ps_entry(ps_sql, client_request.get_parse_result(), ps_entry))) {
      LOG_WDIAG("create ps entry failed", K(ps_sql), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    /* 每次 preapre 请求都使用不同的 ps id */
    if (OB_FAIL(ObPsIdEntry::alloc_ps_id_entry(client_session_->inc_and_get_ps_id(), ps_entry, ps_id_entry))) {
      LOG_WDIAG("fail to alloc ps id entry", K(ret));
    } else if (OB_FAIL(session_info.add_ps_id_entry(ps_id_entry))) {
      LOG_WDIAG("fail to add ps id entry", KPC(ps_entry), K(ret));
    } else {
      // set current ps info
      session_info.set_ps_entry(ps_id_entry->ps_entry_);
      session_info.set_client_ps_id(ps_id_entry->ps_id_);
      session_info.set_ps_id_entry(ps_id_entry);
    }
    if (OB_FAIL(ret)) {
      if (OB_LIKELY(NULL != ps_id_entry)) {
        ps_id_entry->destroy();
        ps_id_entry = NULL;
      } else if (OB_LIKELY(NULL == ps_id_entry && NULL != ps_entry)) {
        ps_entry->dec_ref();
      }
    }
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
  LOG_DEBUG("analyze ps prepare req", K(complete_sql_len), K(ps_sql.length()), K(ps_sql));

  if (complete_sql_len > ps_sql.length()) {
    if (OB_ISNULL(ps_sql_buf = static_cast<char *>(op_fixed_mem_alloc(complete_sql_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc mem for ps sql buf", K(complete_sql_len), K(ret));
    } else {
      client_buffer_reader_->copy(ps_sql_buf, complete_sql_len, MYSQL_NET_META_LENGTH);
      ps_sql.assign_ptr(ps_sql_buf, static_cast<int32_t>(complete_sql_len));
      LOG_DEBUG("after ps sql", K(ps_sql), K(complete_sql_len));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(do_analyze_ps_prepare_request(ps_sql))) {
      LOG_WDIAG("fail to do_analyze_ps_prepare_request", K(ps_sql), K(ret));
    }
  }

  if (NULL != ps_sql_buf) {
    op_fixed_mem_free(ps_sql_buf, complete_sql_len);
    ps_sql_buf = NULL;
  }
  return ret;
}

int ObMysqlSM::do_analyze_ps_execute_request_with_flag(ObPsIdEntry *ps_id_entry)
{
  int ret = OB_SUCCESS;

  int64_t param_num = ps_id_entry->get_param_count();
  ObIOBufferReader *param_type_reader = NULL;
  char *param_type_buf = NULL;
  int64_t param_type_pos = MYSQL_NET_META_LENGTH + MYSQL_PS_EXECUTE_HEADER_LENGTH + ((param_num + 7) /8) + 1;
  int64_t param_type_len = 0;
  int64_t param_offset = 0;
  bool is_finish = false;
  ObIArray<EMySQLFieldType> &param_types = ps_id_entry->get_ps_sql_meta().get_param_types();
  param_types.reset();

  if (OB_ISNULL(param_type_reader = client_buffer_reader_->clone())) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_API_LOG(WDIAG, "fail to alloc param_type_reader ", KP_(client_buffer_reader), K(ret));
  } else if (OB_FAIL(param_type_reader->consume(param_type_pos))) {
    PROXY_API_LOG(WDIAG, "failed to consume param_type_reader", K(param_type_pos), K(ret));
  } else if (OB_FAIL(ObMysqlRequestAnalyzer::parse_param_type_from_reader(param_offset, param_num, param_types,
                                                                          param_type_reader,
                                                                          param_type_len, is_finish))) {
    LOG_WDIAG("fail to parse param type", K(param_offset), K(param_num), K(ret));
  } else if (OB_UNLIKELY(!is_finish)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("param type is not finish, somethins is wrong", K(is_finish), K(param_type_len), K(ret));
  } else if (OB_ISNULL(param_type_buf = static_cast<char *>(op_fixed_mem_alloc(param_type_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc param type buf", K(param_type_len), K(ret));
  } else {
    param_type_reader->copy(param_type_buf, param_type_len, 0);
    if (OB_FAIL(ps_id_entry->get_ps_sql_meta().set_param_type(param_type_buf, param_type_len))) {
      LOG_WDIAG("fail to set param type", K(param_type_len), K(ret));
    } else {
      LOG_DEBUG("set param type success", KPC(ps_id_entry), K(param_types), K(param_type_len), K(ret));
    }
  }

  if (NULL != param_type_reader) {
    param_type_reader->dealloc();
    param_type_reader = NULL;
  }

  if (NULL != param_type_buf) {
    op_fixed_mem_free(param_type_buf, param_type_len);
  }
  return ret;
}

int ObMysqlSM::do_analyze_ps_execute_request_without_flag(ObPsIdEntry *ps_id_entry)
{
  int ret = OB_SUCCESS;

  int64_t param_num = ps_id_entry->get_param_count();
  uint64_t read_avail = client_buffer_reader_->read_avail();
  const ObString& param_type = ps_id_entry->get_ps_sql_meta().get_param_type();
  int64_t param_type_pos = MYSQL_NET_META_LENGTH + MYSQL_PS_EXECUTE_HEADER_LENGTH + ((param_num + 7) /8) + 1;
  // decode execute packet to old execute obj
  if (OB_ISNULL(client_buffer_reader_) || OB_UNLIKELY(param_type.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("reader is null or param_type is emptry, which is unexpected", K(param_type), KPC(ps_id_entry), K(ret));
  } else {
    int32_t pkt_length = static_cast<int32_t>(read_avail - 4 + param_type.length());

    char header[MYSQL_PAYLOAD_LENGTH_LENGTH];
    int64_t pos = 0;
    if (OB_FAIL(ObMySQLUtil::store_int3(header, MYSQL_PAYLOAD_LENGTH_LENGTH, pkt_length, pos))) {
      LOG_WDIAG("fail to store pkg meta header", K(ret));
    } else {
      int64_t new_param_bound_flag_pos = param_type_pos - 1;
      int8_t new_param_bound_flag = 1;
      client_buffer_reader_->replace(header, MYSQL_PAYLOAD_LENGTH_LENGTH, 0);
      client_buffer_reader_->replace(reinterpret_cast<char*>(&new_param_bound_flag), 1, new_param_bound_flag_pos);
    }
  }

  if (OB_SUCC(ret)) {
    int64_t written_len = 0;
    ObMIOBuffer *writer = client_buffer_reader_->writer();
    ObMysqlAnalyzeResult result;
    if (OB_ISNULL(writer)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unexpected null values ", KP(writer), K_(sm_id), K(ret));

      // The previous content is sent out first
    } else if(OB_FAIL(writer->write(client_buffer_reader_, param_type_pos, written_len))) {
      LOG_WDIAG("fail to write execute header", KP_(client_buffer_reader), K(param_type_pos), K_(sm_id), K(ret));
    } else if (OB_UNLIKELY(written_len != param_type_pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to write to writer", "expected size", param_type_pos,
               "actual size", written_len, K(ret));

      // Output type content
    } else if (OB_FAIL(writer->write(param_type.ptr(), param_type.length(), written_len))) {
      LOG_WDIAG("fail to write param type", "length", param_type.length(), K_(sm_id), K(ret));
    } else if (OB_UNLIKELY(written_len != param_type.length())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to write to writer", "expected size", param_type.length(),
               "actual size", written_len, K(ret));

      // Output the remaining content of this time
    } else if (OB_FAIL(do_analyze_ps_execute_request_with_remain_value(writer, read_avail, param_type_pos))) {
      LOG_WDIAG("fail to analyze ps execute request with remain value", K(ret), K(read_avail), K(param_type_pos));

      // consume the previous request packet
    } else if (OB_FAIL(client_buffer_reader_->consume(read_avail))) {
      LOG_WDIAG("fail to consume client_buffer_reader", K(read_avail), K(ret));
      // Check the integrity of the package
    } else if (OB_FAIL(ObProxyParserUtils::analyze_one_packet(*client_buffer_reader_, result))) {
      LOG_WDIAG("fail to analyze one packet", K(ret));
    } else if (OB_UNLIKELY(ANALYZE_DONE != result.status_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("analyze_one_packet do not return ANALYZE_DONE, which is unexpected", K(result.status_), K(ret));
    } else {
      ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
      client_request.add_request(client_buffer_reader_, trans_state_.mysql_config_params_->request_buffer_length_);
      client_request.get_packet_meta().pkt_len_ = static_cast<uint32_t>(client_buffer_reader_->read_avail());

      // rewrite remain payload len while consume buffer all and rewrite mysql request to buffer
      if (get_client_session_protocol() == ObProxyProtocol::PROTOCOL_OB20) {
        ObClientSessionInfo &client_session_info = get_client_session()->get_session_info();
        client_session_info.ob20_request_.remain_payload_len_ = client_request.get_packet_meta().pkt_len_;
      }
    }
  }

  return ret;
}

int ObMysqlSM::do_analyze_ps_execute_request_with_remain_value(ObMIOBuffer *writer,
                                                               int64_t read_avail,
                                                               int64_t param_type_pos)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(read_avail == param_type_pos)) {
    /* no error here, please check the format of COM_STMT_EXECUTE, data could be sent by COM_STMT_SEND_LONG_DATA */
    LOG_DEBUG("The value of each param from package COM_STMT_EXECUTE is null, data maybe sent by COM_STMT_SEND_LONG_DATA");
  } else if (OB_UNLIKELY(read_avail < param_type_pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("Invalid param", K(ret), K(read_avail), K(param_type_pos));
  } else {
    int64_t written_len = 0;
    if (OB_FAIL(writer->write(client_buffer_reader_, read_avail - param_type_pos, written_len, param_type_pos))) {
      LOG_WDIAG("fail to write param value", KP_(client_buffer_reader),
               "length", read_avail - param_type_pos, K_(sm_id), K(ret));
    } else if (OB_UNLIKELY(written_len != (read_avail - param_type_pos))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to write to writer", "expected size", read_avail - param_type_pos,
               "actual size", written_len, K(ret));
    } else {
      /* do nothing here */
    }
  }

  return ret;
}

int ObMysqlSM::do_analyze_ps_execute_request(ObPsIdEntry *ps_id_entry, bool is_large_request)
{
  int ret = OB_SUCCESS;

  // large request will handled by ObMysqlRequestExecuteTransformPlugin
  int64_t param_num = ps_id_entry->get_param_count();
  if (!is_large_request && param_num > 0) {
    int64_t local_read_avail = client_buffer_reader_->read_avail();
    int8_t new_param_bound_flag = 0;
    int64_t new_param_bound_flag_pos = MYSQL_NET_META_LENGTH + MYSQL_PS_EXECUTE_HEADER_LENGTH + ((param_num + 7) /8);

    // Because the data has not been consumed, local_read_avail here is the data from the very beginning
    if (OB_UNLIKELY(local_read_avail <= new_param_bound_flag_pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("new_param_bound_flag is not 1 or 0, which is unexpected", K(ret));
    } else if (FALSE_IT(client_buffer_reader_->copy(reinterpret_cast<char *>(&new_param_bound_flag), 1, new_param_bound_flag_pos))) {
      // must not be here
    } else if (1 == new_param_bound_flag) {
      if (OB_FAIL(do_analyze_ps_execute_request_with_flag(ps_id_entry))) {
        LOG_WDIAG("fail to analyze execute request with flag", KPC(ps_id_entry), K(ret));
      }
    } else if (0 == new_param_bound_flag) {
      if (OB_FAIL(do_analyze_ps_execute_request_without_flag(ps_id_entry))) {
        LOG_WDIAG("fail to analyze execute request without flag", KPC(ps_id_entry), K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("new_param_bound_flag is not 1 or 0, which is unexpected", K(ret));
    }
  }

  return ret;
}

int ObMysqlSM::analyze_ps_execute_request(bool is_large_request)
{
  int ret = OB_SUCCESS;

  ObClientSessionInfo &session_info = client_session_->get_session_info();
  ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
  ObString data = client_request.get_req_pkt();
  if (OB_UNLIKELY(data.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("com_stmt_execute packet is empty", K(ret));
  } else {
    const char *pos = data.ptr() + MYSQL_NET_META_LENGTH;
    uint32_t ps_id = 0;
    ObMySQLUtil::get_uint4(pos, ps_id);
    ObPsIdEntry *ps_id_entry = NULL;
    if (OB_ISNULL(ps_id_entry = session_info.get_ps_id_entry(ps_id)) || !ps_id_entry->is_valid()) {
      ret = OB_ERR_PREPARE_STMT_NOT_FOUND;
      LOG_WDIAG("ps id entry does not exist", K(ps_id), KPC(ps_id_entry), K(ret));
    } else {
      session_info.set_ps_entry(ps_id_entry->get_ps_entry());
      session_info.set_client_ps_id(ps_id);
      session_info.set_ps_id_entry(ps_id_entry);
      client_request.set_ps_parse_result(&ps_id_entry->get_ps_entry()->get_base_ps_parse_result());
      // no need to analyze execute param value here,
      // will do analyze when needed before calculate partition id

      if (OB_MYSQL_COM_STMT_EXECUTE == trans_state_.trans_info_.sql_cmd_) {
        if (OB_FAIL(do_analyze_ps_execute_request(ps_id_entry, is_large_request))) {
          LOG_WDIAG("fail to do analyze ps execute request", KPC(ps_id_entry), K(is_large_request), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObMysqlSM::analyze_text_ps_prepare_request(const ObRequestAnalyzeCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
  ObString text_ps_sql = client_request.get_sql();
  char *text_ps_sql_buf = NULL;
  char *text_ps_prepare_buf = NULL;
  int64_t text_ps_prepare_buf_len = 0;

  int64_t complete_sql_len = client_request.get_packet_len() - MYSQL_NET_META_LENGTH;
  if (complete_sql_len > text_ps_sql.length()) {
    if (OB_ISNULL(text_ps_sql_buf = static_cast<char*>(op_fixed_mem_alloc(complete_sql_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc mem for text ps sql buf", K(complete_sql_len), K(ret));
    } else {
      client_buffer_reader_->copy(text_ps_sql_buf, complete_sql_len, MYSQL_NET_META_LENGTH);
      text_ps_sql.assign_ptr(text_ps_sql_buf, static_cast<int32_t>(complete_sql_len));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(do_parse_text_ps_prepare_sql(text_ps_prepare_buf, text_ps_prepare_buf_len, text_ps_sql, ctx))) {
      LOG_WDIAG("fail to do_parse_text_ps_prepare_sql and transfer the query directly", K(ret));
      ret = OB_SUCCESS;
    } else if (OB_FAIL(do_analyze_text_ps_prepare_request(text_ps_sql))) {
      LOG_WDIAG("fail to do_analyze_text_ps_prepare_request", K(text_ps_sql), K(ret));
    }
  }

  if (NULL != text_ps_sql_buf) {
    op_fixed_mem_free(text_ps_sql_buf, complete_sql_len);
  }
  if (NULL != text_ps_prepare_buf) {
    op_fixed_mem_free(text_ps_prepare_buf, text_ps_prepare_buf_len);
  }
  return ret;
}

int ObMysqlSM::do_parse_text_ps_prepare_sql(char*& text_ps_prepare_buf,
    int64_t& text_ps_prepare_buf_len,
    ObString& text_ps_sql, const ObRequestAnalyzeCtx& ctx)
{
  int ret = OB_SUCCESS;

  ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
  ObClientSessionInfo &cs_info = client_session_->get_session_info();

  // 如果是 SET @s = CONCAT('SELECT * FROM ', 't1'); PREPARE stmt3 FROM @s; 这种prepare语句还需要解析一次
  ObProxyTextPsInfo prepare_info = client_request.get_parse_result().text_ps_info_;
  if (1 == prepare_info.params_.count()) {
    ObProxyTextPsParam* param = prepare_info.params_.at(0);
    ObString user_name = param->str_value_.config_string_;
    if (cs_info.need_use_lower_case_names()) {
      string_to_lower_case(user_name.ptr(), user_name.length());
    }
    ObObj user_value;
    if (OB_FAIL(cs_info.get_user_variable_value(user_name, user_value))) {
      LOG_WDIAG("get user variable failed", K(ret), K(user_name));
    } else if (!user_value.is_varchar()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("invalid extra info value type", K(ret), K(user_value));
    } else {
      const char* prepare_pos = text_ps_sql.find('@');
      ObString prepare = text_ps_sql.split_on(prepare_pos);
      text_ps_prepare_buf_len = prepare.length() + user_value.get_string().length() + 2;
      text_ps_prepare_buf = reinterpret_cast<char *>(op_fixed_mem_alloc(text_ps_prepare_buf_len));
      if (OB_UNLIKELY(NULL == text_ps_prepare_buf)) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        PROXY_LOG(EDIAG, "fail to alloc mem", K(text_ps_prepare_buf_len), K(ret));
      } else {
        MEMCPY(text_ps_prepare_buf, prepare.ptr(), prepare.length());
        MEMCPY(text_ps_prepare_buf + prepare.length(), user_value.get_string().ptr(), user_value.get_string().length());
        text_ps_sql.reset();
        text_ps_sql.assign_ptr(text_ps_prepare_buf, static_cast<int32_t>(text_ps_prepare_buf_len));
        int64_t pos = prepare.length() + user_value.get_string().length();
        MEMSET(text_ps_prepare_buf + pos, 0, 2);
        ObProxySqlParser sql_parser;
        ObSqlParseResult &sql_parse_result = client_request.get_parse_result();
        sql_parse_result.reset();
        bool use_lower_case_name = false;
        if (!ctx.is_sharding_mode_ && !cs_info.is_oracle_mode())  {
          use_lower_case_name = ctx.cached_variables_->need_use_lower_case_names();
        }
        if (OB_FAIL(sql_parser.parse_sql(text_ps_sql, ctx.parse_mode_, sql_parse_result,
                                         use_lower_case_name,
                                         ctx.connection_collation_,
                                         ctx.drop_origin_db_table_name_,
                                         client_request.is_sharding_user()))) {
          LOG_WDIAG("fail to parse sql", K(text_ps_sql), K(ret));
        } else if (!sql_parse_result.is_text_ps_stmt()) {
          ret = common::OB_NOT_SUPPORTED;
          LOG_DEBUG("not support to process text ps prepare sql like this", K(text_ps_sql), K(ret));
        } else {
          LOG_DEBUG("succ to parse sql", K(text_ps_sql));
        }
      }
    }
  }
  return ret;
}

int ObMysqlSM::do_analyze_text_ps_prepare_request(const ObString& text_ps_sql)
{
  int ret = OB_SUCCESS;

  // here we have the complete request in miobuffer, but client request buffer with
  // configured request_buffer_length may be not enough to copy all data,
  // we need copy complete ps sql
  ObClientSessionInfo &session_info = client_session_->get_session_info();
  ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;

  ObTextPsEntry *text_ps_entry = NULL;
  ObGlobalTextPsEntry *global_text_ps_entry = NULL;
  ObString text_ps_name = client_request.get_parse_result().get_text_ps_name();
  ObTextPsNameEntry *text_ps_name_entry = session_info.get_text_ps_name_entry(text_ps_name);
  LOG_DEBUG("prepare text ps name", K(client_request.get_parse_result().get_text_ps_name()));
  if (OB_SUCC(ret) && NULL != text_ps_name_entry) {
    uint32_t client_ps_id = text_ps_name_entry->version_;
    if (OB_FAIL(session_info.delete_text_ps_name_entry(text_ps_name))) {
      LOG_WDIAG("delete text ps name entry failed", K(ret), KPC(text_ps_name_entry));
    } else {
      session_info.remove_ps_id_addrs(client_ps_id);
      ObMysqlServerSession* server_session = NULL;
      server_session = client_session_->get_server_session();
      if (OB_NOT_NULL(server_session)) {
        server_session->get_session_info().remove_text_ps_version(client_ps_id);
      }
      int64_t svr_session_count = client_session_->get_session_manager().get_svr_session_count();
      for (int64_t i = 0; i < svr_session_count; ++i) {
        server_session = client_session_->get_session_manager().get_server_session(i);
        if (OB_NOT_NULL(server_session)) {
          server_session->get_session_info().remove_text_ps_version(client_ps_id);
        }
      }
      LOG_DEBUG("delete text ps name entry succed", K(client_ps_id), KPC(text_ps_name_entry));
    }
  }

  if (get_global_proxy_config().enable_global_ps_cache) {
    ObBasePsEntryGlobalCache& text_ps_entry_global_cache = get_global_text_ps_entry_cache();
    if (OB_FAIL(text_ps_entry_global_cache.acquire_or_create_ps_entry(text_ps_sql, client_request.get_parse_result(), global_text_ps_entry))) {
      LOG_WDIAG("create text ps entry failed", K(text_ps_sql), K(ret));
    } else {
      text_ps_entry = global_text_ps_entry;
    }
  } else {
    ObBasePsEntryThreadCache &text_ps_entry_thread_cache = self_ethread().get_text_ps_entry_cache();
    if (OB_FAIL(text_ps_entry_thread_cache.acquire_or_create_ps_entry(text_ps_sql, client_request.get_parse_result(), text_ps_entry))) {
      LOG_WDIAG("create text ps entry failed", K(text_ps_sql), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObTextPsNameEntry::alloc_text_ps_name_entry(
      client_request.get_parse_result().get_text_ps_name(), text_ps_entry, text_ps_name_entry))) {
      LOG_WDIAG("fail to alloc text ps name entry", K(ret));
    } else if (OB_FAIL(session_info.add_text_ps_name_entry(text_ps_name_entry))) {
      LOG_WDIAG("fail to add text ps name entry", KPC(text_ps_name_entry), K(ret));
    } else {
      text_ps_name_entry->version_ = client_session_->inc_and_get_ps_id();
      session_info.set_client_ps_id(text_ps_name_entry->version_);
      session_info.set_text_ps_name_entry(text_ps_name_entry);
      LOG_DEBUG("session info add text ps name", K(client_request.get_parse_result().get_text_ps_name()));
    }
    if (OB_FAIL(ret)) {
      if (OB_LIKELY(NULL != text_ps_name_entry)) {
        text_ps_name_entry->destroy();
        text_ps_name_entry = NULL;
      } else if (OB_LIKELY(NULL == text_ps_name_entry && NULL != text_ps_entry)) {
        text_ps_entry->dec_ref();
      }
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
  ObTextPsEntry* text_ps_entry = NULL;
  ObTextPsNameEntry* text_ps_name_entry = NULL;
  LOG_DEBUG("execute text ps name", K(text_ps_name));
  if (OB_ISNULL(text_ps_name_entry = session_info.get_text_ps_name_entry(text_ps_name))) {
    LOG_WDIAG("text ps name entry does not exist", K(text_ps_name), KPC(text_ps_entry));
    // 如果execute stmt1, stmt1 没有prepare，直接发给server，让server返回错误信息
    client_request.get_parse_result().set_stmt_type(OBPROXY_T_OTHERS);
  } else if (OB_ISNULL(text_ps_entry = text_ps_name_entry->text_ps_entry_) || !text_ps_entry->is_valid()) {
    LOG_WDIAG("text ps entry does not exist", K(text_ps_name), KPC(text_ps_entry));
    // 如果execute stmt1, stmt1 没有prepare，直接发给server，让server返回错误信息
    client_request.get_parse_result().set_stmt_type(OBPROXY_T_OTHERS);
  } else {
    session_info.set_client_ps_id(text_ps_name_entry->version_);
    session_info.set_text_ps_name_entry(text_ps_name_entry);
    // 这里需要仔细看下具体实现
    client_request.set_text_ps_parse_result(&text_ps_entry->get_base_ps_parse_result());
  }

  return ret;
}

int ObMysqlSM::analyze_text_ps_drop_request()
{
  int ret = OB_SUCCESS;

  ObClientSessionInfo &session_info = client_session_->get_session_info();
  ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
  const ObString &text_ps_name = client_request.get_parse_result().get_text_ps_name();
  ObTextPsEntry* text_ps_entry = NULL;
  ObTextPsNameEntry* text_ps_name_entry = NULL;
  LOG_DEBUG("drop text ps name", K(text_ps_name));
  if (OB_ISNULL(text_ps_name_entry = session_info.get_text_ps_name_entry(text_ps_name))) {
    ret = OB_ERR_PREPARE_STMT_NOT_FOUND;
    LOG_WDIAG("text ps name entry does not exist", K(text_ps_name), KPC(text_ps_entry));
    if (OB_SUCCESS != ObMysqlTransact::encode_error_message(trans_state_, OB_ERR_PREPARE_STMT_NOT_FOUND)) {
      LOG_WDIAG("fail to encode error message", K(ret));
    }
  } else if (OB_ISNULL(text_ps_entry = text_ps_name_entry->text_ps_entry_) || !text_ps_entry->is_valid()) {
    LOG_WDIAG("text ps entry does not exist", K(text_ps_name), KPC(text_ps_entry));
    // 如果drop stmt1, stmt1 没有prepare和execute，直接发给server，让server返回错误信息
    client_request.get_parse_result().set_stmt_type(OBPROXY_T_OTHERS);
  } else {
    session_info.set_client_ps_id(text_ps_name_entry->version_);
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
    LOG_WDIAG("com_stmt_fetch packet is empty", K(ret));
  } else {
    const char *pos = data.ptr() + MYSQL_NET_META_LENGTH;
    uint32_t cursor_id = 0;
    ObMySQLUtil::get_uint4(pos, cursor_id);
    session_info.set_client_cursor_id(cursor_id);
    LOG_DEBUG("fetch cursor id", K(cursor_id));
  }

  return ret;
}

int ObMysqlSM::analyze_close_reset_request()
{
  int ret = OB_SUCCESS;

  ObClientSessionInfo &session_info = client_session_->get_session_info();
  ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
  ObString data = client_request.get_req_pkt();
  if (OB_UNLIKELY(data.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("com_stmt_fetch packet is empty", K(ret));
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
    LOG_WDIAG("com_stmt_execute packet is empty", K(ret));
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
    LOG_EDIAG("invalid internal state", K_(client_entry_->read_vio), K(data),
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
        __attribute__ ((fallthrough));
      case VC_EVENT_ERROR:
      case VC_EVENT_ACTIVE_TIMEOUT:
      case VC_EVENT_INACTIVITY_TIMEOUT:
      case VC_EVENT_DETECT_SERVER_DEAD:
        if (event == VC_EVENT_INACTIVITY_TIMEOUT) {
          COLLECT_TIMEOUT_DIAGNOSIS(connection_diagnosis_trace_,
                                    OB_TIMEOUT_DISCONNECT_TRACE,
                                    client_session_ == NULL ? obutils::OB_TIMEOUT_UNKNOWN_EVENT : client_session_->get_inactivity_timeout_event(),
                                    client_session_ == NULL ? 0 : client_session_->get_timeout_record(),
                                    OB_PROXY_INACTIVITY_TIMEOUT,
                                    NULL);
        } else {
          COLLECT_VC_DIAGNOSIS(connection_diagnosis_trace_,
                                obutils::OB_CLIENT_VC_TRACE,
                                event,
                                OB_CLIENT_HANDLING_REQUEST_CONNECTION_ERROR,
                                NULL);
        }
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
        if (OB_MYSQL_COM_QUIT != trans_state_.trans_info_.client_request_.get_packet_meta().cmd_) {
          if (NULL != trans_state_.congestion_entry_) {
            trans_state_.congestion_entry_->set_client_feedback_failed_at(event::get_hrtime());
            if (trans_state_.congestion_entry_->client_feedback_congested_) {
              if (NULL != sm_cluster_resource_) {
                sm_cluster_resource_->alive_addr_set_.set_refactored(trans_state_.congestion_entry_->server_ip_);
              }
            }
          }
        }
        terminate_sm_ = true;
        break;

      case VC_EVENT_READ_COMPLETE:
      case VC_EVENT_READ_READY:
        // Ignore. Could be a pipelined request. We'll get to it when we finish the
        // current transaction
        break;

      default:
        terminate_sm_ = true;
        LOG_EDIAG("Unexpected event", K(event), K_(sm_id));
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
    LOG_EDIAG("invalid sm magic or reentrancy_count", K_(magic), K_(reentrancy_count), K_(sm_id));
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
      LOG_EDIAG("invalid reentrancy_count", K_(reentrancy_count), K_(sm_id));
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
  skip_plugin_ = false;
  switch (trans_state_.api_next_action_) {
    case ObMysqlTransact::SM_ACTION_API_SM_START:
      if (OB_FAIL(setup_client_request_read())) {
        LOG_WDIAG("failed to setup_client_request_read", K_(sm_id), K(ret));
      }
      break;

    case ObMysqlTransact::SM_ACTION_API_READ_REQUEST:
    case ObMysqlTransact::SM_ACTION_API_OBSERVER_PL:
    case ObMysqlTransact::SM_ACTION_API_READ_RESPONSE:
      call_transact_and_set_next_state(NULL);
      break;

    case ObMysqlTransact::SM_ACTION_API_SEND_REQUEST:
      if (OB_FAIL(setup_server_request_send())) {
        LOG_WDIAG("failed to setup_server_request_send", K_(sm_id), K(ret));
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
            LOG_WDIAG("failed to setup_transfer_from_transform", K_(sm_id), K(ret));
          }
          break;

        case ObMysqlTransact::SM_ACTION_SERVER_READ:
          if (OB_FAIL(setup_server_transfer())) {
            LOG_WDIAG("failed to setup_server_transfer", K_(sm_id), K(ret));
          }
          break;

        case ObMysqlTransact::SM_ACTION_INTERNAL_NOOP:
        case ObMysqlTransact::SM_ACTION_SEND_ERROR_NOOP:
          if (OB_FAIL(setup_internal_transfer(&ObMysqlSM::tunnel_handler_response_transfered))) {
            LOG_WDIAG("failed to setup_internal_transfer", K_(sm_id), K(ret));
          }
          break;

        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_EDIAG("Should not get here", K_(trans_state_.next_action), K_(sm_id), K(ret));
          break;
      }
      break;

    case ObMysqlTransact::SM_ACTION_API_CMD_COMPLETE:
      if (OB_FAIL(setup_cmd_complete())) {
        LOG_WDIAG("failed to setup_cmd_complete", K_(sm_id), K(ret));
      }
      break;

    case ObMysqlTransact::SM_ACTION_API_SM_SHUTDOWN:
      if (OB_FAIL(state_remove_from_list(EVENT_NONE, NULL))) {
        LOG_WDIAG("failed to state_remove_from_list", K_(sm_id), K(ret));
      }
      break;

    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("Should not get here", K_(trans_state_.api_next_action), K_(sm_id), K(ret));
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
    LOG_EDIAG("invalid internal state, client entry is NULL", K_(client_entry), K_(sm_id), K(ret));
  } else if (OB_ISNULL(client_entry_->vc_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_EDIAG("invalid internal state, client entry vc is NULL", K_(client_entry_->vc), K_(sm_id), K(ret));
  } else  if (OB_FAIL(client_buffer_reader_->consume_all())) { // consume the client buffer
    LOG_WDIAG("fail to consume all data in client buffer reader", K_(sm_id), K(ret));
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
        LOG_EDIAG("Unexpected event", K(event), K_(sm_id), K(ret));
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
        LOG_EDIAG("failed to allocate memory for ObMysqlServerSession", K(ret));
      } else if (OB_ISNULL(client_session_) || OB_ISNULL(data)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("invalid client session or data", K_(client_session), K(data), K_(sm_id), K(ret));
      } else {
        ObNetVConnection *net_vc = reinterpret_cast<ObNetVConnection *>(data);
        ops_ip_copy(session->local_ip_, net_vc->get_local_addr());
        ops_ip_copy(session->server_ip_, trans_state_.server_info_.addr_);
        const ObString &full_username = client_session_->get_session_info().get_full_username();
        if (full_username.length() >= OB_PROXY_FULL_USER_NAME_MAX_LEN) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("full username is too long", K(ret), K(full_username));
        } else {
          MEMCPY(session->full_name_buf_, full_username.ptr(), full_username.length());
          session->auth_user_.assign_ptr(session->full_name_buf_, full_username.length());

          // if shard_conn is NULL, should set to client session. And later will use connection pool by the shard connector
          ObShardConnector *shard_conn = client_session_->get_session_info().get_shard_connector();
          if (OB_NOT_NULL(shard_conn)) {
            session->get_session_info().set_shard_connector(shard_conn);
            LOG_DEBUG("open server session for this shard_connector", "shard_name", shard_conn->shard_name_.config_string_);
          }

          if (OB_FAIL(session->new_connection(*client_session_, *(reinterpret_cast<ObNetVConnection *>(data))))) {
            LOG_WDIAG("fail to new server connection", K_(sm_id), K(ret));
          } else {
            session->state_ = MSS_ACTIVE;

            if (OB_FAIL(attach_server_session(*session))) {
              LOG_WDIAG("failed to attach server session", K_(sm_id), K(ret));
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
    {
      char addr_buf[MAX_IP_PORT_LENGTH];
      int port = ops_ip_port_host_order(trans_state_.server_info_.addr_);
      MEMSET(addr_buf, 0, MAX_IP_PORT_LENGTH);
      net::ops_ip_ntop(trans_state_.server_info_.addr_, addr_buf,
                       MAX_IP_PORT_LENGTH);
      LOG_DEBUG("[ObMysqlSM::state_observer_open] connect failed", K_(sm_id));
      COLLECT_VC_DIAGNOSIS(connection_diagnosis_trace_,
                           obutils::OB_SERVER_VC_TRACE, event,
                           OB_SERVER_BUILD_CONNECTION_ERROR,
                           "fail to build connection to observer, %.*s:%d",
                           static_cast<uint32_t>(MAX_IP_PORT_LENGTH), addr_buf, port);
      // save the errno from the connect fail for future use
      // (passed as negative value, flip back)
      trans_state_.current_.state_ = ObMysqlTransact::CONNECT_ERROR;
      call_transact_and_set_next_state(ObMysqlTransact::handle_response);
      break;
    }
    case VC_EVENT_ERROR:
    {
      char addr_buf[MAX_IP_PORT_LENGTH];
      int port = ops_ip_port_host_order(trans_state_.server_info_.addr_);
      MEMSET(addr_buf, 0, MAX_IP_PORT_LENGTH);
      net::ops_ip_ntop(trans_state_.server_info_.addr_, addr_buf,
                       MAX_IP_PORT_LENGTH);
      COLLECT_VC_DIAGNOSIS(connection_diagnosis_trace_,
                           obutils::OB_SERVER_VC_TRACE, event,
                           OB_SERVER_BUILD_CONNECTION_ERROR,
                           "fail to build connection to observer, %.*s:%d",
                           static_cast<uint32_t>(MAX_IP_PORT_LENGTH), addr_buf, port);
      trans_state_.current_.state_ = ObMysqlTransact::CONNECTION_ERROR;
      call_transact_and_set_next_state(ObMysqlTransact::handle_response);
      break;
    }
    default:
      ret = OB_INNER_STAT_ERROR;
      LOG_EDIAG("[ObMysqlSM::state_observer_open] Unknown event:",
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
    LOG_EDIAG("invalid internal state, server entry is NULL or data is NULL",
              K_(server_entry), K(data), K_(sm_id), K(ret));
  } else if (OB_UNLIKELY(server_entry_->read_vio_ != reinterpret_cast<ObVIO *>(data))) {
    ret = OB_INNER_STAT_ERROR;
    LOG_EDIAG("invalid internal state, server entry read vio is not the same as data",
              K_(server_entry_->read_vio), K(data), K_(sm_id), K(ret));
  } else {
    switch (event) {
      case VC_EVENT_EOS:
        server_entry_->eos_ = true;
        COLLECT_VC_DIAGNOSIS(connection_diagnosis_trace_,
                             obutils::OB_SERVER_VC_TRACE, event,
                             OB_SERVER_RECEIVING_PACKET_CONNECTION_ERROR, NULL);
        LOG_WDIAG("ObMysqlSM::state_server_response_read, recevied  VC_EVENT_EOS", K_(sm_id));
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
      case VC_EVENT_DETECT_SERVER_DEAD:
        if (event == VC_EVENT_INACTIVITY_TIMEOUT) {
          COLLECT_TIMEOUT_DIAGNOSIS(connection_diagnosis_trace_,
                                    OB_TIMEOUT_DISCONNECT_TRACE,
                                    server_session_ == NULL ? obutils::OB_TIMEOUT_UNKNOWN_EVENT : server_session_->get_inactivity_timeout_event(),
                                    server_session_ == NULL ? 0 :server_session_->get_timeout_record(),
                                    OB_PROXY_INACTIVITY_TIMEOUT,
                                    NULL);
        } else {
          COLLECT_VC_DIAGNOSIS(connection_diagnosis_trace_,
                      obutils::OB_SERVER_VC_TRACE,
                      event,
                      OB_SERVER_RECEIVING_PACKET_CONNECTION_ERROR,
                      NULL);
        }
        LOG_WDIAG("ObMysqlSM::state_server_response_read", "event",
                 ObMysqlDebugNames::get_event_name(event), K_(sm_id));
        // Error handling function
        ret = OB_CONNECT_ERROR;
        break;
    }
  }

  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
    // set server_read_begin_ no matter if error or timeout happen
    if (0 == milestones_.server_.server_read_begin_) {
      milestones_.server_.server_read_begin_ = get_based_hrtime();
      cmd_time_stats_.server_process_request_time_ =
          milestone_diff(milestones_.server_.server_write_end_, milestones_.server_.server_read_begin_);
    }
  }

  if (enable_record_full_link_trace_info()) {
    SET_TRACE_BUFFER(flt_trace_buffer_, MAX_TRACE_LOG_SIZE);
    LOG_DEBUG("state server response read begin.");

    // server process req end
    trace::ObSpanCtx *server_process_req_ctx = flt_.trace_log_info_.server_process_req_ctx_;
    if (OB_NOT_NULL(server_process_req_ctx)) {
      // set show trace buffer before flush trace
      if (flt_.control_info_.is_show_trace_enable()) {
        SET_SHOW_TRACE_INFO(&flt_.show_trace_json_info_.curr_sql_json_span_array_);
      }
      LOG_DEBUG("end span ob_proxy_server_process_req", K(server_process_req_ctx->span_id_));
      FLT_END_SPAN(server_process_req_ctx);
      flt_.trace_log_info_.server_process_req_ctx_ = NULL;
    }

    // client response write begin, check inited or not, only init it once
    if (flt_.trace_log_info_.client_response_write_ctx_ == NULL) {
      trace::ObSpanCtx *ctx = FLT_BEGIN_SPAN(ob_proxy_client_response_write);
      if (OB_NOT_NULL(ctx)) {
        flt_.trace_log_info_.client_response_write_ctx_ = ctx;
        LOG_DEBUG("begin span ob_proxy_client_response_write", K(ctx->span_id_));
      }
    }
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
    if (ObMysqlTransact::SERVER_SEND_REQUEST != trans_state_.current_.send_action_) {
      need_receive_completed = true;
    }

    int64_t first_pkt_len = 0; // include packet header
    if (OB_FAIL(handle_first_response_packet(state, first_pkt_len, need_receive_completed))) {
      LOG_WDIAG("fail to handle first response packet", K(ret));
      state = ANALYZE_ERROR;
    }

    if (OB_UNLIKELY(ANALYZE_CONT != state)) {
      // Disable further I/O
      // Read the first packet, or command complete or transaction complete,
      // or error happen;
      // If read the first packet on the client since there could
      // be rest request body that we are tunneling, and we can't issue
      // another IO later for the rest request body with a different buffer
      server_entry_->read_vio_->nbytes_ = server_entry_->read_vio_->ndone_;
      if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
        if (0 == milestones_.server_.server_read_end_) {
          milestones_.server_.server_read_end_ = get_based_hrtime();
          cmd_time_stats_.server_response_read_time_ += (
            milestone_diff(milestones_.server_.server_read_begin_, milestones_.server_.server_read_end_)
            - cmd_time_stats_.plugin_decompress_response_time_
            - cmd_time_stats_.server_response_analyze_time_);
        }
      }

      ObRespAnalyzeResult &resp_result = trans_state_.trans_info_.resp_result_;
      if (enable_record_full_link_trace_info()
          && (resp_result.is_resp_completed()
              || resp_result.is_trans_completed())) {
        trace::ObSpanCtx *ctx = flt_.trace_log_info_.server_response_read_ctx_;
        if (OB_NOT_NULL(ctx)) {
          // set show trace buffer before flush trace
          if (flt_.control_info_.is_show_trace_enable()) {
            SET_SHOW_TRACE_INFO(&flt_.show_trace_json_info_.curr_sql_json_span_array_);
          }
          LOG_DEBUG("end span ob_proxy_server_response_read", K(ctx->span_id_));
          SET_TRACE_BUFFER(flt_trace_buffer_, MAX_TRACE_LOG_SIZE);
          FLT_END_SPAN(ctx);
          flt_.trace_log_info_.server_response_read_ctx_ = NULL;
        }
      }
    }

    switch (state) {
      case ANALYZE_ERROR:
      {
        LOG_WDIAG("Error parsing server response", K_(sm_id), K(ret));
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

        // skip the response handling plugins or not
        trans_state_.current_.state_ = ObMysqlTransact::CONNECTION_ALIVE;
        trans_state_.transact_return_point = ObMysqlTransact::handle_response;
        if (OB_MYSQL_COM_QUERY == trans_state_.trans_info_.sql_cmd_
            && (ObProxyProtocol::PROTOCOL_OB20 != get_client_session_protocol()
                || ObMysqlTransact::SERVER_SEND_REQUEST != trans_state_.current_.send_action_)
            && (trans_state_.trans_info_.resp_result_.is_decompressed()
                || ObProxyProtocol::PROTOCOL_NORMAL == get_server_session_protocol())) {
          skip_plugin_ = true;
        }
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
            LOG_WDIAG("fail to consume data in server buffer reader", K(ret));
          }
          ret = OB_CONNECT_ERROR;
        } else {
          server_entry_->read_vio_->reenable();
        }
        break;
      }

      default:
        ret = OB_INNER_STAT_ERROR;
        LOG_EDIAG("not reached, unknown ayalyze status", K(state), K_(sm_id), K(ret));
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

ObProxyProtocol ObMysqlSM::get_server_session_protocol() const
{
  // in auth, do not use compress prototcol
  if (NULL == server_session_ || (!server_session_->is_checksum_supported() && !server_session_->is_ob_protocol_v2_supported())
          || ObMysqlTransact::SERVER_SEND_SAVED_LOGIN == trans_state_.current_.send_action_
          || ObMysqlTransact::SERVER_SEND_LOGIN == trans_state_.current_.send_action_
          || ObMysqlTransact::SERVER_SEND_HANDSHAKE == trans_state_.current_.send_action_
          || ObMysqlTransact::is_binlog_request(trans_state_)) {
    return ObProxyProtocol::PROTOCOL_NORMAL;
  } else if (server_session_->is_ob_protocol_v2_supported()) {
    return ObProxyProtocol::PROTOCOL_OB20;
  } else {
    return ObProxyProtocol::PROTOCOL_CHECKSUM; // current checksum is supported by zlib
  }
}

ObProxyProtocol ObMysqlSM::get_client_session_protocol() const
{
  if (client_session_ == NULL
      || !client_session_->get_session_info().is_client_support_ob20_protocol()
      || trans_state_.is_auth_request_) {
    return ObProxyProtocol::PROTOCOL_NORMAL;
  } else {
    return ObProxyProtocol::PROTOCOL_OB20;
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
    bret = client_session_->is_need_convert_vip_to_tname();
  }
  return bret;
}

bool ObMysqlSM::need_reject_user_login(const ObString &user, const ObString &tenant,
                                       const bool has_tenant_username, const bool has_cluster_username,
                                       const bool is_cloud_user) const
{
  // 以下几种场景需要拒绝用户连接
  // proxysys 为proxy内部租户，不会访问 ob
  // 1. 外部用户使用 proxyro@sys
  // 2. 云上用户带有 tenant 或者 cluster
  // 3. 非云用户没有带 cluster
  bool bret = false;
  // service name登录，没有集群名，不做登录校验
  if (OB_UNLIKELY(NULL != client_session_)
      && !client_session_->is_proxy_mysql_client_
      && tenant.case_compare(OB_PROXYSYS_TENANT_NAME) != 0
      && !(get_global_proxy_config().enable_standby
           && tenant.prefix_case_match(OB_SERVICE_NAME_PRIFIX))) {
    if (!get_global_proxy_config().skip_proxyro_check &&
        tenant.case_compare(OB_SYS_TENANT_NAME) == 0 &&
        user.case_compare(ObProxyTableInfo::READ_ONLY_USERNAME_USER) == 0) {
      bret = true;
      COLLECT_LOGIN_DIAGNOSIS(
          connection_diagnosis_trace_, OB_LOGIN_DISCONNECT_TRACE, "",
          OB_PROXY_INVALID_USER,
          "user proxyro is rejected while proxyro_check on");
    } else if (is_cloud_user
               && (OB_ISNULL(multi_level_config_)
                  || !multi_level_config_->enable_cloud_full_username_)
               && (has_tenant_username || has_cluster_username)) {
      bret = true;
      COLLECT_LOGIN_DIAGNOSIS(connection_diagnosis_trace_,
                              OB_LOGIN_DISCONNECT_TRACE, "",
                              OB_PROXY_INVALID_USER,
                              "connection with cluster name and tenant name is "
                              "rejected while cloud_full_user_name_check off");
    } else if (!is_cloud_user &&
               get_global_proxy_config().enable_full_username &&
               (!has_cluster_username || !has_tenant_username)) {
      COLLECT_LOGIN_DIAGNOSIS(connection_diagnosis_trace_,
                              OB_LOGIN_DISCONNECT_TRACE, "",
                              OB_PROXY_INVALID_USER,
                              "cluster name and tenant name is required while "
                              "full_username_check on");
      bret = true;
    } else {
      // do nothing
    }
  }

  return bret;
}

uint8_t ObMysqlSM::get_compressed_or_ob20_request_seq()
{
  uint8_t seq = 0;
  ObProxyProtocol server_protocol = get_server_session_protocol();
  if (ObProxyProtocol::PROTOCOL_CHECKSUM == server_protocol) {
    // load content of file's response use compressed seq
    if (OB_MYSQL_COM_LOAD_DATA_TRANSFER_CONTENT == trans_state_.trans_info_.sql_cmd_) {
      seq = server_session_->get_cur_compressed_seq();
    } else if (ObMysqlTransact::SERVER_SEND_REQUEST == trans_state_.current_.send_action_) {
      seq = trans_state_.trans_info_.client_request_.get_packet_meta().pkt_seq_;
    } else {
      // others like sync database, sync session variables seq num == 0
      seq = 0;
    }
  // ob20 pkt seq is identical with compressed seq
  } else if (ObProxyProtocol::PROTOCOL_OB20 == server_protocol) {
    seq = server_session_->get_cur_compressed_seq();
  } else {
    LOG_WDIAG("unexpect proxy protocol type", K(server_protocol));
  }
  return seq;
}

ObMySQLCmd ObMysqlSM::get_request_cmd()
{
  ObMySQLCmd cmd = OB_MYSQL_COM_QUERY;
  if (OB_UNLIKELY(OB_MYSQL_COM_LOAD_DATA_TRANSFER_CONTENT == trans_state_.trans_info_.sql_cmd_)) {
    cmd = OB_MYSQL_COM_LOAD_DATA_TRANSFER_CONTENT;
  } else if (OB_UNLIKELY(OB_MYSQL_COM_AUTH_SWITCH_RESP == trans_state_.trans_info_.sql_cmd_)) {
    cmd = OB_MYSQL_COM_AUTH_SWITCH_RESP;
  } else {
    if (ObMysqlTransact::SERVER_SEND_REQUEST == trans_state_.current_.send_action_) {
      cmd = trans_state_.trans_info_.client_request_.get_packet_meta().cmd_;
    } else {
      // others like sync last insert id, sync session variables, cmd = COM_QUERY
      cmd = trans_state_.trans_info_.sql_cmd_;
    }
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

  if (OB_LIKELY(client_session_->get_session_info().is_oceanbase_server())) {
    if (OB_FAIL(handle_oceanbase_first_response_packet(state, need_receive_completed, first_pkt_len))) {
      LOG_WDIAG("fail to handle oceanbase first response packet", K(ret));
    }
  } else {
    if (OB_FAIL(handle_first_normal_response_packet(state, need_receive_completed, first_pkt_len))) {
      LOG_WDIAG("fail to handle normal first response packet", K(ret));
    }
  }

  cmd_time_stats_.server_response_analyze_time_ += milestone_diff(analyze_response_begin, get_based_hrtime());
  return ret;
}

inline int ObMysqlSM::handle_oceanbase_first_response_packet(ObMysqlAnalyzeStatus &state,
        const bool need_receive_completed, int64_t &first_pkt_len)
{
  int ret = OB_SUCCESS;

  ObProxyProtocol ob_proxy_protocol = get_server_session_protocol();

  if (OB_UNLIKELY(ObProxyProtocol::PROTOCOL_CHECKSUM == ob_proxy_protocol
                  || ObProxyProtocol::PROTOCOL_OB20 == ob_proxy_protocol)) { // compressed protocol
    if (OB_FAIL(handle_first_compress_response_packet(state, need_receive_completed, first_pkt_len))) {
      LOG_WDIAG("fail to handle_first_compress_response_packet", K(need_receive_completed), K(ret));
    }
  } else { // standard mysql protocol
    if (OB_FAIL(handle_first_normal_response_packet(state, need_receive_completed, first_pkt_len))) {
      LOG_WDIAG("fail to handle_first_normal_response_packet", K(need_receive_completed), K(ret));
    }
  }
  return ret;
}

inline int ObMysqlSM::handle_first_compress_response_packet(ObMysqlAnalyzeStatus &state,
    const bool need_receive_completed, int64_t &first_pkt_len)
{
  int ret = OB_SUCCESS;
  ObRespAnalyzeResult &resp_result = trans_state_.trans_info_.resp_result_;
  resp_result.reset();

  ObAnalyzeHeaderResult result;
  const uint8_t req_seq = get_compressed_or_ob20_request_seq();
  const ObMySQLCmd cmd = get_request_cmd();
  const ObMysqlProtocolMode mysql_mode = client_session_->get_session_info().is_oracle_mode() ? OCEANBASE_ORACLE_PROTOCOL_MODE : OCEANBASE_MYSQL_PROTOCOL_MODE;
  const bool is_extra_ok_for_stats = is_extra_ok_packet_for_stats_enabled();

  if (server_buffer_reader_->is_read_avail_more_than(MYSQL_COMPRESSED_HEALDER_LENGTH)) {
    ObRespAnalyzeMode analyze_mode;
    if (ObMysqlTransact::SERVER_SEND_XA_START == trans_state_.current_.send_action_ ||
        ObMysqlTransact::SERVER_SEND_INIT_SQL == trans_state_.current_.send_action_ ||
        (need_receive_completed && ObMysqlTransact::SERVER_SEND_REQUEST == trans_state_.current_.send_action_)) {
      analyze_mode = ObRespAnalyzeMode::DECOMPRESS_MODE;
    } else {
      analyze_mode = ObRespAnalyzeMode::SIMPLE_MODE;
    }

    const bool is_compressed_ob20 = server_session_->get_session_info().is_server_ob20_compress_supported()
                                    && 0 != compression_algorithm_.level_;
    const bool enable_transmission_checksum = client_session_->get_session_info().get_enable_transmission_checksum();

    if (OB_FAIL(resp_analyzer_.init(get_server_session_protocol(),
                                    cmd, mysql_mode, analyze_mode,
                                    is_extra_ok_for_stats,
                                    is_compressed_ob20,
                                    req_seq, req_seq,
                                    server_session_->get_server_request_id(),
                                    server_session_->get_server_sessid(),
                                    enable_transmission_checksum))) {
      LOG_WDIAG("fail to init resp_analyzer", K_(sm_id), K(req_seq), K(cmd), K(ret));
    } else if (OB_FAIL(resp_analyzer_.analyze_response(*server_buffer_reader_,
                                                       need_receive_completed,
                                                       result, resp_result))) {
      LOG_WDIAG("fail to analyze response",
                K(server_buffer_reader_), K(need_receive_completed),
                K(resp_result), K_(sm_id), K(ret));
    }

    LOG_DEBUG("handle first compress resp, print tag",
              K(analyze_mode), K(need_receive_completed), K(trans_state_.current_.send_action_));

    if (OB_FAIL(ret)) {
    } else {
      LOG_DEBUG("after first response", K(req_seq),
                "is_resp_finished", resp_result.is_resp_completed(),
                "is_trans_finished", resp_result.is_trans_completed(),
                "is_local_infile_0xfb_resp", resp_result.is_local_infile_0xfb_resp(),
                K(result), K(resp_result));
      if (ANALYZE_DONE == result.status_ && ObProxyProtocol::PROTOCOL_CHECKSUM == get_server_session_protocol()) {
        check_update_checksum_switch(result.compressed_mysql_header_.is_compressed_payload());
      }

      state = result.status_;
      first_pkt_len = result.compressed_mysql_header_.compressed_len_ + MYSQL_COMPRESSED_HEALDER_LENGTH;
      cmd_size_stats_.server_response_bytes_ = server_buffer_reader_->read_avail();

      // save flt from response analyze result to sm
      save_response_flt_result_to_sm(resp_result.flt_);
      if (OB_FAIL(handle_feedback_proxy_info(resp_result.extra_info_))) {
        LOG_WDIAG("fail to handle feedback proxy info", K_(sm_id), K(result), K(ret));
      }
    }
  } else {
    state = ANALYZE_CONT;
  }
  return ret;
}

void ObMysqlSM::save_response_flt_result_to_sm(common::FLTObjManage &flt)
{
  if (OB_UNLIKELY(enable_full_link_trace_)) {
    // control info
    // deserialized from server, it should be sent to client whichi is valid or invalid
    if (flt.control_info_.is_need_send()) {
      flt_.saved_control_info_ = flt.control_info_;
      flt.control_info_.reset();
      LOG_DEBUG("update control info from server response", K(flt_.saved_control_info_));
    }

    // query info
    if (flt.query_info_.is_valid()) {
      flt_.query_info_ = flt.query_info_;
      flt.query_info_.reset();
      LOG_DEBUG("update query info from server response", K(flt_.query_info_));
    }

    if (flt.span_info_.is_valid()) {
      LOG_WDIAG("attention: span info in rsp valid!");
    }
    if (flt.app_info_.is_valid()) {
      LOG_WDIAG("attention: app info in rsp valid!");
    }
    if (flt.driver_span_info_.is_valid()) {
      LOG_WDIAG("attention: driver span info in rsp valid!");
    }
  }
}

bool ObMysqlSM::enable_record_full_link_trace_info()
{
  return enable_full_link_trace_ && flt_.span_info_.trace_enable_ && flt_.span_info_.is_valid();
}

bool ObMysqlSM::is_proxy_init_trace_log_info()
{
  return flt_.trace_log_info_.is_inited_;
}

int ObMysqlSM::handle_resp_for_end_proxy_root_span(trace::UUID &trace_id, bool is_in_trans)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(enable_full_link_trace_)) {
    LOG_DEBUG("end proxy root span begin", K(flt_.span_info_), K(is_in_trans));

    if (enable_record_full_link_trace_info()) {
      SET_TRACE_BUFFER(flt_trace_buffer_, MAX_TRACE_LOG_SIZE);
      trace_id = flt_.span_info_.trace_id_;

      if (flt_.query_info_.is_valid()) {
        // set tag to proxy root span and print with it
        FLT_SET_TAG(query_start_ts, flt_.query_info_.query_start_ts_, query_end_ts, flt_.query_info_.query_end_ts_);
        LOG_DEBUG("set observer query info tag to proxy root span", K(flt_.query_info_));
        flt_.query_info_.reset();
      }

      ObMysqlClientSession *client_session = get_client_session();
      if (!client_session->is_client_support_full_link_trace()) {
        // add client ip and port to tag, inorder to classify the different io from client
        net::ObIpEndpoint client_ip_point;
        net::ops_ip_copy(client_ip_point, client_session->get_netvc()->get_real_client_addr());
        char client_addr_buf[INET6_ADDRSTRLEN] = {0};
        if (OB_FAIL(ops_ip_nptop(client_ip_point, client_addr_buf, INET6_ADDRSTRLEN))) {
          LOG_WDIAG("fail to ops ip nptop", K(ret));
        } else {
          const char *client_addr_ptr = client_addr_buf;
          FLT_SET_TAG(client_host, client_addr_ptr);
          LOG_DEBUG("set client host tag to proxy root span", K(client_addr_ptr));
        }
      }

      // accord to record policy and slow query, print the log
      trace::ObSpanCtx *ctx = flt_.trace_log_info_.proxy_root_span_ctx_;
      if (OB_NOT_NULL(ctx)) {
        // set show trace buffer before flush trace
        if (flt_.control_info_.is_show_trace_enable()) {
          SET_SHOW_TRACE_INFO(&flt_.show_trace_json_info_.curr_sql_json_span_array_);
        }
        LOG_DEBUG("end span ob proxy root", K(trace_id), K(ctx->span_id_), K(OBTRACE->is_auto_flush()),
                  K(flt_.control_info_.is_show_trace_enable()));
        FLT_END_SPAN(ctx);        // if auto flush, FLUSH_TRACE()
        flt_.trace_log_info_.proxy_root_span_ctx_ = NULL;
      }

      // slow query check
      int64_t slow_query_thres_in_control_info = flt_.control_info_.slow_query_threshold_;
      if (!flt_.span_info_.force_print_                           // do not print repeatly
          && flt_.trace_log_info_.proxy_root_span_begin_time_ > 0
          && slow_query_thres_in_control_info > 0
          && (flt_.control_info_.record_policy_ == RP_ONLY_SLOW_QUERY
              || flt_.control_info_.record_policy_ == RP_SAMPLE_AND_SLOW_QUERY)) {
        int64_t proxy_root_span_end_time = ObTimeUtility::current_time();
        if (milestone_diff(flt_.trace_log_info_.proxy_root_span_begin_time_, proxy_root_span_end_time)
                           > slow_query_thres_in_control_info) {
          // slow query need to print
          LOG_DEBUG("succ to print slow query trace log", K(slow_query_thres_in_control_info),
                    K(proxy_root_span_end_time), K(flt_.trace_log_info_.proxy_root_span_begin_time_));
          // set show trace buffer before flush trace
          if (flt_.control_info_.is_show_trace_enable()) {
            SET_SHOW_TRACE_INFO(&flt_.show_trace_json_info_.curr_sql_json_span_array_);
          }
          FLUSH_TRACE();
          flt_.trace_log_info_.proxy_root_span_begin_time_ = -1;
        }
      }

      // if show trace enable, record json, mv to last
      if (flt_.control_info_.is_show_trace_enable()) {
        SET_TRACE_BUFFER(flt_trace_buffer_, MAX_TRACE_LOG_SIZE);
        SET_SHOW_TRACE_INFO(&flt_.show_trace_json_info_.curr_sql_json_span_array_);
        // force to flush to curr json array
        FLUSH_TRACE();
        flt_.show_trace_json_info_.move_curr_to_last_span_array();
        LOG_DEBUG("succ to flush trace and move json", KP(&flt_));
      } else {
        flt_.show_trace_json_info_.reset();
        LOG_DEBUG("reset show trace json info while show trace disable", KP(&flt_));
      }

      /*
       * clear obtrace instance, and re-init
       * if it is not force print policy, only record the current SQL span in obtrace memory
       * do not print other sql span info in obtrace
       * for only slow query policy, only print slow query, do not print other sql in obtrace instance
       * for sample and slow query policy, force print will print all sqls, while others only print slow query
       */
      if (flt_.span_info_.force_print_ == false) {
        trace::UUID trace_id = OBTRACE->get_trace_id();
        trace::UUID root_span_id = OBTRACE->get_root_span_id();
        uint8_t policy = OBTRACE->get_policy();
        OBTRACE->init(trace_id, root_span_id, policy);
      }

      // reset app info, after succeed to send request to observer
      if (flt_.app_info_.is_valid()) {
        flt_.app_info_.reset();
      }

      // reset span info, root obproxy span end, clear span info
      bool trace_enable = flt_.span_info_.trace_enable_;
      bool force_print = flt_.span_info_.force_print_;
      flt_.span_info_.reset();
      if (is_in_trans) {
        flt_.span_info_.trace_id_ = trace_id;  // in trans, keep the trace id as the same before
        if (is_proxy_init_trace_log_info()) {
          // proxy generate trace, still in trans, save bool, save trace id, regenerate span id.
          flt_.span_info_.trace_enable_ = trace_enable;
          flt_.span_info_.force_print_ = force_print;
        }
      }
    } else {
      // disable flt, or do not target flt, means trace_enable=false
      // we need free show trace json memory for last sql
      flt_.show_trace_json_info_.reset();
      LOG_DEBUG("reset show trace json info while flt trace disable", KP(&flt_));
    }

    // save client control info for proxy session at last
    if (flt_.saved_control_info_.is_need_send()) {
      flt_.control_info_ = flt_.saved_control_info_;
      flt_.control_info_.set_need_send(false);
      flt_.saved_control_info_.reset();
      LOG_DEBUG("update proxy session control info", K(flt_.control_info_));
    }
  }

  return ret;
}

int ObMysqlSM::handle_for_end_proxy_trace(trace::UUID &trace_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(enable_full_link_trace_)) {
    if (is_proxy_init_trace_log_info()) {
      LOG_DEBUG("end trace generated by proxy", K(trace_id));
      SET_TRACE_BUFFER(flt_trace_buffer_, MAX_TRACE_LOG_SIZE);
      FLT_END_TRACE();
      flt_.trace_log_info_.reset();
    }
  }

  return ret;
}

int ObMysqlSM::handle_resp_for_end_flt_trace(bool is_trans_completed)
{
  int ret = OB_SUCCESS;
  trace::UUID trace_id;

  if (is_trans_completed) {
    if (OB_FAIL(handle_resp_for_end_proxy_root_span(trace_id, false))) {
      LOG_WDIAG("fail to handle resp for end span", K(ret));
    } else if (OB_FAIL(handle_for_end_proxy_trace(trace_id))) {
      LOG_WDIAG("fail to handle resp for end trace", K(ret));
    } else {
      // nothing
    }
  } else {
    if (OB_FAIL(handle_resp_for_end_proxy_root_span(trace_id, true))) {
      LOG_WDIAG("fail to handle resp for end span in plugin", K(ret));
    } else {
      // nothing
      // proxy generate trace & still in trans, do not clear the OBTRACE memory
    }
  }

  return ret;
}

inline int ObMysqlSM::handle_first_normal_response_packet(ObMysqlAnalyzeStatus &state,
    const bool need_receive_completed, int64_t &first_pkt_len)
{
  int ret = OB_SUCCESS;
  ObRespAnalyzeResult &resp_result = trans_state_.trans_info_.resp_result_;
  resp_result.reset();
  ObAnalyzeHeaderResult result;

  ObObj obj;
  bool is_autocommit_0 = false;
  if (OB_FAIL(client_session_->get_session_info().field_mgr_.get_common_sys_variable_value("autocommit", obj))) {
    LOG_DEBUG("fail to get autocommit val", K(ret));
    ret = OB_SUCCESS;
  } else if (obj.get_int() == 0) {
    LOG_DEBUG("autocommit is 0");
    is_autocommit_0 = true;
  }
  const int64_t autocommit = client_session_->get_session_info().get_cached_variables().get_autocommit();
  bool is_in_trans = ObMysqlTransact::is_in_trans(trans_state_) || is_in_trans_ || is_autocommit_0;
  ObMySQLCmd req_cmd = trans_state_.trans_info_.sql_cmd_;

  if (OB_LIKELY(server_buffer_reader_->is_read_avail_more_than(MYSQL_NET_META_LENGTH))) {
    if (OB_UNLIKELY(!client_session_->get_session_info().is_oceanbase_server()
        || trans_state_.mysql_config_params_->is_mysql_routing_mode()
        || ObMysqlTransact::is_binlog_request(trans_state_))) {
      LOG_DEBUG("handle_first_normal_response_packet for binlog", K(trans_state_.current_.state_),
                K(is_in_trans_), K(is_autocommit_0));
      bool is_binlog_related = ObMysqlTransact::is_binlog_request(trans_state_) &&
                               OBPROXY_T_SHOW_BINLOG_SERVER_FOR_TENANT != trans_state_.trans_info_.client_request_.get_parse_result().get_stmt_type();

      resp_analyzer_.init(req_cmd, STANDARD_MYSQL_PROTOCOL_MODE, false,
                          is_in_trans, autocommit, is_binlog_related);

    } else {
      const bool is_extra_ok_for_stats = is_extra_ok_packet_for_stats_enabled();
      const ObMysqlProtocolMode mysql_mode = client_session_->get_session_info().is_oracle_mode() ?
          OCEANBASE_ORACLE_PROTOCOL_MODE : OCEANBASE_MYSQL_PROTOCOL_MODE;
      resp_analyzer_.init(req_cmd, mysql_mode, is_extra_ok_for_stats, is_in_trans, false, false);
      LOG_DEBUG("handle_first_normal_response_packet", K(trans_state_.current_.state_),
                K(is_in_trans_), K(is_autocommit_0), K(is_extra_ok_for_stats));
    }

    if (OB_FAIL(resp_analyzer_.analyze_response(*server_buffer_reader_,
                                                need_receive_completed,
                                                result, resp_result))) {
      LOG_WDIAG("fail to analyze response", K(server_buffer_reader_), K(resp_result),
               K(need_receive_completed), K_(sm_id), K(ret));
    } else {
      state = result.status_;
      first_pkt_len = result.mysql_header_.pkt_len_;
      cmd_size_stats_.server_response_bytes_ = server_buffer_reader_->read_avail();
    }
  } else {
    state = ANALYZE_CONT;
  }
  return ret;
}

int ObMysqlSM::handle_first_request_packet(ObMysqlAnalyzeStatus &status, int64_t &first_packet_len)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("handle first request packet begin");

  if (get_client_session_protocol() != ObProxyProtocol::PROTOCOL_OB20) {
    analyze_mysql_request(status);
  } else {
    ObClientSessionInfo &client_session_info = get_client_session()->get_session_info();
    if (client_session_info.ob20_request_.remain_payload_len_ == 0) {
      LOG_DEBUG("handle ob20 req begin", K(client_buffer_reader_->read_avail()));
      // analyze ob20 request here, do not copy mysql packet
      // ensure the whole ob20 request received complete is enough
      if (OB_FAIL(handle_first_compress_request_packet(status, first_packet_len))) {
        LOG_WDIAG("fail to handle first compress request from client", K(ret), K(status), K(first_packet_len));
      } else if (status == ANALYZE_DONE) {
        analyze_mysql_request(status, true);
        analyze_status_after_analyze_mysql_in_ob20_payload(status, client_session_info);
      } else if (status == ANALYZE_CONT) {
        LOG_DEBUG("handle first compress request continue.", K(first_packet_len));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unexpected ret status, handle ob20 first compress req", K(ret), K(status), K(first_packet_len));
      }
    } else {
      // more than one mysql packet in ob20 payload, handle it one by one
      // at last, consume the tail crc of ob20 in client_buffer_reader_
      LOG_DEBUG("handle other mysql req in ob20 payload begin");
      analyze_mysql_request(status, true);
      analyze_status_after_analyze_mysql_in_ob20_payload(status, client_session_info);
    }
  }

  LOG_DEBUG("after handle first request packet", K(status));

  return ret;
}

/*
 * analyze ob20 request from client
 * all the reqs are saved in src: client_buffer_reader_, there could be more than one mysql packet in ob20 payload data
 * the strategy to avoid copy buffer is:
 * 1: resolve the necessary compressed head and ob20 head info from the begging of src buffer (7 + 24)
 * 2: get extra len if extra info flag is set in ob20 head flag
 * 3: remain payload len = ob20.head.payload_len - extra len - 4(extra len)
 * 4: consume src buffer, len = compressed head + ob20 head (7 + 24) + 4(extra len) + extra len, make sure the ptr
      point to basic info in ob20 payload data(several mysql packets)
 * 5: analyze mysql packet
 *
 * the procedure of this is compatible with the current mysql buffer consume
 * take care of the end crc judgement, by remain payload len
 */
int ObMysqlSM::handle_first_compress_request_packet(ObMysqlAnalyzeStatus &status, int64_t &first_packet_len)
{
  int ret = OB_SUCCESS;

  if (client_buffer_reader_->is_read_avail_more_than(MYSQL_COMPRESSED_HEALDER_LENGTH)) {
    ObAnalyzeHeaderResult ob20_result;
    Ob20ExtraInfo extra_info;
    common::FLTObjManage flt;
    ObOceanBase20RequestAnalyzer ob20_req_analyzer;

    if (OB_FAIL(ob20_req_analyzer.analyze_first_request(*client_buffer_reader_,
                                                         ob20_result,
                                                         trans_state_.trans_info_.client_request_,
                                                         status, extra_info, flt))) {
    	LOG_WDIAG("fail to analyze first compress request", K(ret));
    } else if (status == ANALYZE_DONE) {
      if (OB_FAIL(handle_compress_request_analyze_done(ob20_result, first_packet_len, status, extra_info, flt))) {
        LOG_WDIAG("fail to handle compress request analyze done", K(ret));
      }
    } else if (status == ANALYZE_CONT) {
      first_packet_len = ob20_result.compressed_mysql_header_.compressed_len_ > 0
                         ? ob20_result.compressed_mysql_header_.compressed_len_ + MYSQL_COMPRESSED_HEALDER_LENGTH
                         : 0;
      LOG_DEBUG("analyze first compress request continue.", K(status), K(first_packet_len));
    }
  } else {
    status = ANALYZE_CONT;
  }

  return ret;
}

int ObMysqlSM::handle_compress_request_analyze_done(ObAnalyzeHeaderResult &ob20_result,
                                                    int64_t &first_packet_len,
                                                    ObMysqlAnalyzeStatus &status,
                                                    Ob20ExtraInfo &extra_info,
                                                    common::FLTObjManage &flt)
{
  int ret = OB_SUCCESS;

  // total mysql packet len in ob20 payload = payload_len - extra_len(4) - extra_info_len (if extra exist)
  int64_t remain_mysql_packets_len = 0;
  if (ob20_result.ob20_header_.flag_.is_extra_info_exist()) {
    remain_mysql_packets_len = static_cast<int64_t>(ob20_result.ob20_header_.payload_len_
                                                    - extra_info.extra_len_
                                                    - OB20_PROTOCOL_EXTRA_INFO_LENGTH);
  } else {
    remain_mysql_packets_len = static_cast<int64_t>(ob20_result.ob20_header_.payload_len_);
  }

  if (remain_mysql_packets_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    status = ANALYZE_ERROR;
    LOG_WDIAG("receive err mysql packet len in ob20 payload", K(ret), K(remain_mysql_packets_len), K(ob20_result));
  } else {
    first_packet_len = ob20_result.ob20_header_.cp_hdr_.compressed_len_ + MYSQL_COMPRESSED_HEALDER_LENGTH;
    ObClientSessionInfo &client_session_info = get_client_session()->get_session_info();
    client_session_info.ob20_request_.remain_payload_len_ = remain_mysql_packets_len;
    client_session_info.ob20_request_.ob20_request_received_done_ = true;
    client_session_info.ob20_request_.ob20_header_ = ob20_result.ob20_header_;
    get_client_session()->set_compressed_seq(ob20_result.ob20_header_.cp_hdr_.seq_);
    LOG_DEBUG("analyze first ob20 packet finish", K(status), K(ob20_result), K(remain_mysql_packets_len),
              "req compressed seq", get_client_session()->get_compressed_seq());

    // save FLT to sm after analyze ob20 req done
    if (OB_FAIL(save_request_flt_result_to_sm(flt))) {
      LOG_WDIAG("fail to save req flt result to sm", K(ret));
    } else {
      // consume, make sure the beginning in reader_ is mysql packet
      int64_t offset = MYSQL_COMPRESSED_OB20_HEALDER_LENGTH;
      if (ob20_result.ob20_header_.flag_.is_extra_info_exist()) {
        offset += OB20_PROTOCOL_EXTRA_INFO_LENGTH + extra_info.extra_len_;
      }

      if (OB_FAIL(client_buffer_reader_->consume(offset))) {
        status = ANALYZE_ERROR;
        LOG_WDIAG("fail to consume buffer", K(ret));
      } else {
        LOG_DEBUG("consume the compress head and ob20 head in client buffer",
                  K(offset), K(client_buffer_reader_->read_avail()));
      }
    }
  }

  return ret;
}

void ObMysqlSM::analyze_status_after_analyze_mysql_in_ob20_payload(ObMysqlAnalyzeStatus &status,
                                                                   ObClientSessionInfo &client_session_info)
{
  int64_t mysql_packet_len = trans_state_.trans_info_.client_request_.get_packet_len();
  int ret = OB_SUCCESS;
  if (OB_LIKELY(status == ANALYZE_DONE)) {
    if (OB_UNLIKELY(mysql_packet_len == MYSQL_PACKET_MAX_LENGTH)) {
      status = ANALYZE_NOT_SUPPORT;
      LOG_WDIAG("do not support full mysql packet in ob2.0 payload now", K(status));
    }
  } else if (OB_UNLIKELY(status == ANALYZE_CONT)) {
    status = ANALYZE_NOT_SUPPORT;
    int64_t total_avail = client_buffer_reader_->read_avail();
    LOG_WDIAG("compress packet received done, mysql packet not received done yet, or unexpected analyze status",
             K(status), K(total_avail), K(mysql_packet_len));
  } else {
    LOG_WDIAG("unexpected status after analyze mysql in ob20 payload", K(status));
  }

  if (OB_FAIL(analyze_ob20_remain_after_analyze_mysql_request_done(client_session_info))) {
    if (OB_LIKELY(status == ANALYZE_DONE)) {
      if (ret == OB_NOT_SUPPORTED) {
        status = ANALYZE_NOT_SUPPORT;
      } else {
        status = ANALYZE_ERROR;
        LOG_WDIAG("fail to analyze ob20 remain after mysql req done", K(ret), K(status));
      }
    }
  }
}

int ObMysqlSM::analyze_ob20_remain_after_analyze_mysql_request_done(ObClientSessionInfo &client_session_info)
{
  int ret = OB_SUCCESS;

  int64_t mysql_packet_len = trans_state_.trans_info_.client_request_.get_packet_len();
  int64_t remain_payload_len = client_session_info.ob20_request_.remain_payload_len_;
  if (remain_payload_len < mysql_packet_len) {
    ret = OB_NOT_SUPPORTED;
    client_session_info.ob20_request_.remain_payload_len_ = 0;
    LOG_WDIAG("unexpected situation after received total compress packet and total mysql packet",
             K(ret), K(remain_payload_len), K(mysql_packet_len));
  } else {
    client_session_info.ob20_request_.remain_payload_len_ -= mysql_packet_len;
    LOG_DEBUG("analyze other mysql reqs in compress payload", K(remain_payload_len), K(mysql_packet_len));
  }

  return ret;
}

int ObMysqlSM::handle_req_for_begin_proxy_root_span()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(enable_full_link_trace_)) {
    // according to client capability
    if (get_client_session()->is_client_support_full_link_trace()) {
      handle_req_to_generate_root_span_from_client();
    } else {
      // client do not support flt, proxy generate obtrace
      if (OB_FAIL(handle_req_to_generate_root_span_by_proxy())) {
        LOG_WDIAG("fail to do proxy generate proxy root span", K(ret));
      }
    }
  }

  return ret;
}

void ObMysqlSM::handle_req_to_generate_root_span_from_client()
{
  if (OB_UNLIKELY(enable_full_link_trace_)) {
    // client pass span info to proxy, proxy use it directly
    if (flt_.span_info_.is_valid() && flt_.control_info_.is_valid()) {
      SET_TRACE_BUFFER(flt_trace_buffer_, MAX_TRACE_LOG_SIZE);
      if (enable_record_full_link_trace_info()) {
        OBTRACE->init(flt_.span_info_.trace_id_, flt_.span_info_.span_id_, flt_.control_info_.level_);
        if (flt_.span_info_.force_print_) {
          FLT_SET_AUTO_FLUSH(true);
        } else {
          FLT_SET_AUTO_FLUSH(false);
        }

        if (flt_.trace_log_info_.proxy_root_span_ctx_ == NULL) {
          trace::ObSpanCtx *ctx = FLT_BEGIN_SPAN(ob_proxy);
          if (OB_NOT_NULL(ctx)) {
            // save driver span for show trace
            if (flt_.control_info_.is_show_trace_enable()) {
              flt_.show_trace_json_info_.drv_show_by_proxy_.curr_drv_span_info_ = flt_.span_info_;
              flt_.show_trace_json_info_.drv_show_by_proxy_.curr_drv_span_start_ts_ = ObTimeUtility::current_time();
            }
            flt_.trace_log_info_.proxy_root_span_ctx_ = ctx;
            flt_.span_info_.span_id_ = ctx->span_id_;
            flt_.span_info_.ref_type_ = common::SYNC;
            flt_.trace_log_info_.proxy_root_span_begin_time_ = ObTimeUtility::current_time();
            LOG_DEBUG("client gen, proxy use, begin span ob proxy root based on client trace",
                      K(ctx->span_id_), K(flt_.span_info_), K(OBTRACE->is_auto_flush()));
          }
        }
      } else {
        FLT_SET_AUTO_FLUSH(false);
        LOG_DEBUG("req flt trace not enable in this req, auto flush false.", K(flt_.span_info_));
      }
    }
  }

}

/*
 * global.enable_trace_ as the total switch to decide whether proxy will monitor or not
 * including xflush, full link trace
 */
int ObMysqlSM::handle_req_to_generate_root_span_by_proxy()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(enable_full_link_trace_)
      && flt_.control_info_.is_valid()
      && get_global_performance_params().enable_trace_
      && !client_session_->is_proxy_mysql_client_
      && client_session_->get_session_info().is_server_support_full_link_trace()) {
    if (!is_proxy_init_trace_log_info()) {
      double pct = trace::get_random_percentage();
      if (pct < flt_.control_info_.sample_percentage_
          || flt_.control_info_.show_trace_enable_) {
        flt_.span_info_.trace_enable_ = true;
      } else {
        flt_.span_info_.trace_enable_ = false;
      }

      SET_TRACE_BUFFER(flt_trace_buffer_, MAX_TRACE_LOG_SIZE);
      if (flt_.span_info_.trace_enable_) {
        flt_.span_info_.trace_id_ = FLT_BEGIN_TRACE();
        FLT_SET_TRACE_LEVEL(flt_.control_info_.level_);   // need set trace level after BEGIN TRACE
        LOG_DEBUG("begin trace generated by proxy", K(flt_.span_info_.trace_id_), K(flt_.control_info_.level_));

        if (flt_.control_info_.record_policy_ == RP_ALL) {
          flt_.span_info_.force_print_ = true;
        } else if (flt_.control_info_.record_policy_ == RP_SAMPLE_AND_SLOW_QUERY) {
          double print_pct = trace::get_random_percentage();
          if (print_pct < flt_.control_info_.print_sample_percentage_) {
            flt_.span_info_.force_print_ = true;
          } else {
            flt_.span_info_.force_print_ = false;
          }
        } else if (flt_.control_info_.record_policy_ == RP_ONLY_SLOW_QUERY) {
          flt_.span_info_.force_print_ = false;
        } else {
          flt_.span_info_.force_print_ = false;
        }

        // generate ob proxy root span
        if (flt_.trace_log_info_.proxy_root_span_ctx_ == NULL) {
          trace::ObSpanCtx *ctx = FLT_BEGIN_SPAN(ob_proxy);
          if (OB_NOT_NULL(ctx)) {
            flt_.trace_log_info_.proxy_root_span_ctx_ = ctx;
            flt_.span_info_.span_id_ = ctx->span_id_;
            flt_.span_info_.ref_type_ = common::SYNC;
            flt_.trace_log_info_.proxy_root_span_begin_time_ = ObTimeUtility::current_time();
            LOG_DEBUG("proxy gen, begin trace, begin span ob proxy root.",
                      K(flt_.span_info_), K(flt_.trace_log_info_), K(flt_.control_info_));
          } else {
            LOG_DEBUG("proxy gen, begin, empty span ctx!");
          }
        }
      } else {
        flt_.span_info_.force_print_ = false;
      }

      if (flt_.span_info_.force_print_) {
        FLT_SET_AUTO_FLUSH(true);
      } else {
        FLT_SET_AUTO_FLUSH(false);
      }

      flt_.trace_log_info_.is_inited_ = true;
      LOG_DEBUG("proxy init trace log info done", K(flt_.span_info_), K(flt_.trace_log_info_));
    } else if (is_in_trans_) {
      // still in trans, proxy has initede trace log info before.
      // nothing, use the span generated before, regenerate root span id
      if (flt_.span_info_.trace_enable_) {
        SET_TRACE_BUFFER(flt_trace_buffer_, MAX_TRACE_LOG_SIZE);
        if (flt_.trace_log_info_.proxy_root_span_ctx_ == NULL) {
          trace::ObSpanCtx *ctx = FLT_BEGIN_SPAN(ob_proxy);
          if (OB_NOT_NULL(ctx)) {
            flt_.trace_log_info_.proxy_root_span_ctx_ = ctx;
            flt_.span_info_.span_id_ = ctx->span_id_;
            flt_.span_info_.ref_type_ = common::SYNC;
            flt_.trace_log_info_.proxy_root_span_begin_time_ = ObTimeUtility::current_time();
            LOG_DEBUG("proxy gen trace before, still in trans, begin span ob proxy root",
                      K(flt_.span_info_), K(flt_.trace_log_info_));
          } else {
            LOG_DEBUG("proxy gen, in trans, empty span ctx!");
          }
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unexpected error, proxy has generate trace&span id at req, but not in trans",
               K(ret), K(flt_.trace_log_info_));
    }
  }

  return ret;
}

int ObMysqlSM::save_request_flt_result_to_sm(common::FLTObjManage &flt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(enable_full_link_trace_)) {
    // span info from client, save as we need
    if (flt.span_info_.is_valid()) {
      flt_.span_info_ = flt.span_info_;
      LOG_DEBUG("succ to save req span info to sm", K(flt_.span_info_));
    }

    // app info, pass it to server only
    if (flt.app_info_.is_valid()) {
      flt_.app_info_ = flt.app_info_;
      LOG_DEBUG("succ to save req app info to sm", K(flt_.app_info_));
    }

    // driver span info, print right now
    if (flt.driver_span_info_.is_valid()) {
      SET_TRACE_BUFFER(flt_trace_buffer_, MAX_TRACE_LOG_SIZE);
      _OBPROXY_TRACE_LOG(INFO, "%s", flt.driver_span_info_.curr_driver_span_.ptr());
      LOG_DEBUG("succ get driver span info from req", K(flt.driver_span_info_.curr_driver_span_));
    }

    // driver show trace info, copy to our thread buffer, and manage by ourselves
    // driver should send this with "show trace" cmd, not every SQL
    ObMysqlClientSession *client_session = get_client_session();
    if (!flt.show_trace_json_info_.flt_drv_show_trace_span_.empty()
        && OB_NOT_NULL(client_session)) {
      if (OB_FAIL(flt_.show_trace_json_info_.deep_copy_drv_show_trace_span(flt.show_trace_json_info_))) {
        LOG_WDIAG("fail to deep copy driver show trace json info", K(ret));
      } else {
        LOG_DEBUG("save show trace json info from driver", K(flt_.show_trace_json_info_));
      }
    }
  }

  return ret;
}

int ObMysqlSM::state_server_request_send(int event, void *data)
{
  int ret = OB_SUCCESS;
  STATE_ENTER(ObMysqlSM::state_server_request_send, event);

  if (OB_ISNULL(server_entry_) || OB_ISNULL(data)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_EDIAG("invalid internal state, server entry is NULL or data is NULL",
              K_(server_entry), K(data), K_(sm_id), K(ret));
  } else if (OB_UNLIKELY(server_entry_->read_vio_ != reinterpret_cast<ObVIO *>(data)
             && server_entry_->write_vio_ != reinterpret_cast<ObVIO *>(data))) {
    ret = OB_INNER_STAT_ERROR;
    LOG_EDIAG("invalid internal state, server entry read vio isn't the same as data,"
              "and server entry write vio isn't the same as data",
              K_(server_entry_->read_vio),
              K_(server_entry_->write_vio), K(data), K_(sm_id), K(ret));
  } else {
    switch (event) {
      case VC_EVENT_WRITE_READY:
        server_entry_->write_vio_->reenable();
        break;

      case VC_EVENT_WRITE_COMPLETE: {
        if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
          milestones_.server_.server_write_end_ = get_based_hrtime();
          cmd_time_stats_.server_request_write_time_ += (milestones_.server_.server_write_end_ - milestones_.server_.server_write_begin_);
        }

        if (enable_record_full_link_trace_info()) {
          trace::ObSpanCtx *ctx = flt_.trace_log_info_.server_request_write_ctx_;
          if (OB_NOT_NULL(ctx)) {
            // set show trace buffer before flush trace
            if (flt_.control_info_.is_show_trace_enable()) {
              SET_SHOW_TRACE_INFO(&flt_.show_trace_json_info_.curr_sql_json_span_array_);
            }
            LOG_DEBUG("end span ob_proxy_server_request_write", K(ctx->span_id_));
            SET_TRACE_BUFFER(flt_trace_buffer_, MAX_TRACE_LOG_SIZE);
            FLT_END_SPAN(ctx);
            flt_.trace_log_info_.server_request_write_ctx_ = NULL;
          }
        }

        // We are done sending the request, deallocate our
        // buffer and then decide what to do next
        if (OB_UNLIKELY(NULL != server_entry_->write_buffer_)) {
          free_miobuffer(server_entry_->write_buffer_);
          server_entry_->write_buffer_ = NULL;
        }
        ObMySQLCmd request_cmd = trans_state_.trans_info_.client_request_.get_packet_meta().cmd_;
        // before send quit cmd to observer, maybe we need send session vars first.
        // after send quit cmd to observer, this connection will disconnect soon.
        if (OB_UNLIKELY(OB_MYSQL_COM_QUIT == request_cmd
                        && ObMysqlTransact::SERVER_SEND_REQUEST == trans_state_.current_.send_action_)) {
          // when receive 'quit' cmd, obproxy should disconnect after sending it to observer
          LOG_DEBUG("[setup_server_response_read] send quit to observer completed,"
                    " this connection will disconnect soon");
          ret = OB_CONNECT_ERROR;
        } else if (OB_UNLIKELY(OB_MYSQL_COM_STMT_CLOSE == request_cmd
                               && ObMysqlTransact::SERVER_SEND_REQUEST == trans_state_.current_.send_action_)) {
          /* 把 ps_id_pair 和 cursor_id_pair 清除掉 */
          ObClientSessionInfo &cs_info = client_session_->get_session_info();
          ObServerSessionInfo &ss_info = server_session_->get_session_info();
          uint32_t client_ps_id = cs_info.get_client_ps_id();
          /* 无论是 ps id 还是 cursor id, 都可以直接删除, 有就删除, 没有就算了 */
          ss_info.remove_ps_id_pair(client_ps_id);
          ss_info.remove_cursor_id_pair(client_ps_id);
          cs_info.remove_cursor_id_addr(client_ps_id);
          cs_info.remove_piece_info(client_ps_id);
          call_transact_and_set_next_state(ObMysqlTransact::handle_request);
        } else if (OB_UNLIKELY(ObMysqlTransact::SERVER_SEND_SSL_REQUEST == trans_state_.current_.send_action_)) {
          ObUnixNetVConnection *vc = static_cast<net::ObUnixNetVConnection *>(server_session_->get_netvc());
          if (OB_FAIL(vc->ssl_init(ObUnixNetVConnection::SSL_CLIENT,
                                   client_session_->get_vip_cluster_name(),
                                   client_session_->get_vip_tenant_name()))) {
            LOG_WDIAG("client ssl init failed", K(ret));
          } else  if (trans_state_.is_auth_request_) {
            trans_state_.current_.send_action_ = ObMysqlTransact::SERVER_SEND_LOGIN;
            trans_state_.next_action_ = ObMysqlTransact::SM_ACTION_API_SEND_REQUEST;
            if (OB_FAIL(setup_server_request_send())) {
              LOG_WDIAG("setup server request send failed", K(ret));
            }
          } else {
            trans_state_.current_.send_action_ = ObMysqlTransact::SERVER_SEND_SAVED_LOGIN;
            trans_state_.next_action_ = ObMysqlTransact::SM_ACTION_API_SEND_REQUEST;
            if (OB_FAIL(setup_server_request_send())) {
              LOG_WDIAG("setup server request send failed", K(ret));
            }
          }
        } else if (OB_UNLIKELY(OB_MYSQL_COM_STMT_SEND_LONG_DATA == request_cmd
                               && ObMysqlTransact::SERVER_SEND_REQUEST == trans_state_.current_.send_action_)) {
          if (OB_FAIL(handle_server_request_send_long_data())) {
            LOG_WDIAG("fail to handle state server request send long data", K(ret));
          } else {
            LOG_DEBUG("succ to handle state server request send long data");
          }
        } else {
          if (OB_LIKELY(!tunnel_.is_tunnel_active())) {
            // It's time to start reading the response
            if (OB_FAIL(setup_server_response_read())) {
              LOG_WDIAG("failed to setup_server_response_read", K_(sm_id), K(ret));
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
      case VC_EVENT_DETECT_SERVER_DEAD:
        if (event == VC_EVENT_INACTIVITY_TIMEOUT) {
          COLLECT_TIMEOUT_DIAGNOSIS(connection_diagnosis_trace_,
                                    OB_TIMEOUT_DISCONNECT_TRACE,
                                    server_session_ == NULL ? obutils::OB_TIMEOUT_UNKNOWN_EVENT : server_session_->get_inactivity_timeout_event(),
                                    server_session_ == NULL ? 0 : server_session_->get_timeout_record(),
                                    OB_PROXY_INACTIVITY_TIMEOUT,
                                    NULL);
        } else {
          COLLECT_VC_DIAGNOSIS(
              connection_diagnosis_trace_, obutils::OB_SERVER_VC_TRACE, event,
              OB_SERVER_TRANSFERRING_PACKET_CONNECTION_ERROR, NULL);
        }
        LOG_WDIAG("ObMysqlSM::state_server_request_send", "event",
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
        LOG_EDIAG("Unknown event", K(event), K_(sm_id), K(ret));
        break;
    }
  }

  if (OB_FAIL(ret)) {
    handle_server_setup_error(event, data);
  }

  return VC_EVENT_NONE;
}

/*
 * COM_STMT_SEND_LONG_DATA has no rsp from server, trans to handle request
 * use piece_info_map to record the server addr info
 * only record after first send_long_data, remove after execute/close
 */
int ObMysqlSM::handle_server_request_send_long_data()
{
  int ret = OB_SUCCESS;

  ObPieceInfo *info = NULL;
  ObClientSessionInfo &cs_info = client_session_->get_session_info();
  if (OB_FAIL(cs_info.get_piece_info(info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      if (OB_ISNULL(info = op_alloc(ObPieceInfo))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to alloc mem for piece info", K(ret));
      } else {
        info->set_ps_id(cs_info.get_client_ps_id());
        info->set_addr(server_session_->get_netvc()->get_remote_addr());
        if (OB_FAIL(cs_info.add_piece_info(info))) {
          LOG_WDIAG("fail to add piece info", K(ret));
          op_free(info);
          info = NULL;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (ObMysqlTransact::is_in_trans(trans_state_)) {
      trans_state_.current_.state_ = ObMysqlTransact::CMD_COMPLETE;
    } else {
      trans_state_.current_.state_ = ObMysqlTransact::TRANSACTION_COMPLETE;
    }

    LOG_DEBUG("send_long_data send finish, trans to handle request");
    release_server_session();
    callout_api_and_start_next_action(ObMysqlTransact::SM_ACTION_API_CMD_COMPLETE);
  }

  return ret;
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
  } else if (client_session_ == NULL) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("client session is closed", K(ret));
  } else {
    trans_state_.pll_info_.set_route_info(result);
    // no table entry found from mysql route(both table entry and part entry are NULL)
    if (trans_state_.pll_info_.is_no_route_info_found() && !client_session_->is_need_update_dummy_entry_) {
      LOG_DEBUG("begin to process partition location, no avail route entry found,"
                " will use dummy entry", K_(trans_state_.pll_info));
      // if we can not find the certain table location, just use __all_dummy's partition location
      ObMysqlTransact::ObAttachDummyEntryType type = ObMysqlTransact::NO_TABLE_ENTRY_FOUND_ATTACH_TYPE;
      if (OB_FAIL(ObMysqlTransact::attach_cached_dummy_entry(trans_state_, type))) {
        ObMysqlClientSession *client_session = get_client_session();
        LOG_WDIAG("can not get table entry, and dummy entry does not exist, will disconnect",
                 K_(sm_id), K(type), K(ret));

        // if failed, encode err packet and send to client, then disconnect
        // if it's COM_LOGIN req and failed to acquire dummy_entry, probably because the tenant does not exist
        if (trans_state_.trans_info_.sql_cmd_ == obmysql::OB_MYSQL_COM_LOGIN) {
          ObString tenant_name = client_session->get_session_info().get_login_req().get_hsr_result().tenant_name_;
          LOG_WDIAG("fail to acquire dummy_entry on COM_LOGIN, maybe tenant not exist", K(tenant_name));
          COLLECT_LOGIN_DIAGNOSIS(
              connection_diagnosis_trace_, OB_LOGIN_DISCONNECT_TRACE, "",
              OB_PASSWORD_WRONG,
              "dummy entry is empty, please check if the tenant exists");
          trans_state_.mysql_errcode_ = OB_PASSWORD_WRONG;
        } else {
          COLLECT_INTERNAL_DIAGNOSIS(
              connection_diagnosis_trace_, OB_PROXY_INTERNAL_TRACE,
              OB_GET_LOCATION_TIME_OUT, "dummy entry is empty, disconnect");
          trans_state_.mysql_errcode_ = OB_GET_LOCATION_TIME_OUT;
        }

        int ret_tmp = OB_SUCCESS;
        if (OB_UNLIKELY(OB_SUCCESS !=
             (ret_tmp = ObMysqlTransact::encode_error_message(trans_state_)))) {
          LOG_WDIAG("fail to build err packet", K(ret_tmp));
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
      LOG_WDIAG("fail to check_update_ldc", K(ret));
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
      bool expired = false;
      if (valid_ns > 0) {
        expired = ((get_hrtime_internal() - hrtime_from_usec(cached_dummy_entry->get_create_time_us()))> valid_ns);
      }

      if (!expired) {
        int64_t tenant_version = 0;
        if (OB_UNLIKELY(get_global_proxy_config().check_tenant_locality_change)) {
          tenant_version = sm_cluster_resource_->get_location_tenant_version(
            client_session_->get_session_info().get_priv_info().tenant_name_);
        }

        if (!(expired = get_global_table_cache().is_table_entry_expired(*cached_dummy_entry))) {
          cached_dummy_entry->check_and_set_expire_time(tenant_version, cached_dummy_entry->is_sys_dummy_entry());
          LOG_DEBUG("dummy entry expired", KPC(cached_dummy_entry));
        }
      }

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
    LOG_WDIAG("query_info is null, it should not happened",
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
          LOG_WDIAG("unexpect null client_buffer",  K_(sm_id), K(ret));
        //consume the client buffer
        } else if (OB_FAIL(client_buffer_reader_->consume_all())) {
          LOG_WDIAG("fail to consume request in buffer", K(ret));
        } else {
          ObSqlString sql;
          ObMIOBuffer *writer = client_buffer_reader_->mbuf_;
          ObMysqlAnalyzeResult result;
          const bool use_compress = false;
          const bool is_checksum_on = false;
          if (OB_ISNULL(writer)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("unexpected null values ", K(writer), K_(sm_id), K(ret));
          } else if (OB_FAIL(ObServerAddrLookupHandler::build_kill_query_sql(query_info->real_conn_id_, sql))) {
            LOG_WDIAG("fail to build_kill_query_sql", K(*query_info), K_(sm_id), K(ret));
            // no need compress here, if server session support compress, it will compress later
          } else if (OB_FAIL(ObMysqlRequestBuilder::build_mysql_request(*writer, OB_MYSQL_COM_QUERY,
              sql.string(), use_compress, is_checksum_on, 0))) {
            LOG_WDIAG("fail to build_mysql_request", K(*query_info), K_(sm_id), K(ret));
          } else if (OB_FAIL(ObProxyParserUtils::analyze_one_packet(*client_buffer_reader_, result))) {
            LOG_WDIAG("fail to analyze one packet", K(ret));
          } else if (ANALYZE_DONE != result.status_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("fail to analyze one packet, error status_", K(result.status_), K(ret));
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
    LOG_EDIAG("unknown event type, it should not happen", K(event), K_(sm_id));
  } else if (OB_FAIL(process_server_addr_lookup(reinterpret_cast<ObProxyKillQueryInfo *>(data)))) {
    LOG_WDIAG("failed to process_server_addr_lookup", K_(sm_id), K(ret));
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

  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
    milestones_.pl_lookup_end_ = get_based_hrtime();
    cmd_time_stats_.pl_lookup_time_ += milestone_diff(milestones_.pl_lookup_begin_, milestones_.pl_lookup_end_);
    milestones_.pl_process_begin_ = milestones_.pl_lookup_end_;
  }

  if (OB_UNLIKELY(TABLE_ENTRY_EVENT_LOOKUP_DONE != event) || OB_ISNULL(data)) {
    ret = OB_ERR_UNEXPECTED;
    COLLECT_INTERNAL_DIAGNOSIS(
        connection_diagnosis_trace_, OB_PROXY_INTERNAL_TRACE,
        OB_PROXY_INTERNAL_ERROR,
        "partition location lookup result is unexpected, event:%d", event);
    LOG_EDIAG("unexpected event type, it should not happen", K(event), K(data), K(ret));
  } else {
    ObMysqlRouteResult *result = reinterpret_cast<ObMysqlRouteResult *>(data);

    trans_state_.pll_info_.set_need_force_flush(false);

    if (trans_state_.pll_info_.is_force_renew()) {
      trans_state_.pll_info_.set_force_renew_done();
    }

    if (OB_FAIL(process_partition_location(*result))) {
      LOG_WDIAG("fail to process partition location", K_(sm_id), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(trans_state_.pll_info_.is_cached_dummy_force_renew())) {
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
    LOG_EDIAG("invalid sm magic or reentrancy_count", K_(magic), K_(reentrancy_count), K_(sm_id), K(event));
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
      LOG_EDIAG("invalid internal state, vc handler is NULL or vc is NULL",
                K_(vc_entry->vc), K_(sm_id));
    } else {
      (this->*jump_point)(event, data);
    }
  } else {
    if (OB_ISNULL(default_handler_)) {
      LOG_EDIAG("invalid internal state, default handler is NULL", K_(sm_id));
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
      LOG_EDIAG("invalid reentrancy_count", K_(reentrancy_count), K_(sm_id));
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
    LOG_EDIAG("unexpected event type, or data isn't  the same as &tunnel_",
              K(event), K(data), K_(client_entry),
              K_(server_entry), K_(sm_id), K(ret));
  } else if (OB_ISNULL(client_entry_) || OB_ISNULL(server_entry_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WDIAG("client_entry or server entry is null, disconnect",
             K_(server_entry), K_(client_entry), K(ret));
  } else {
    // The tunnel calls this when it is done
    ObMysqlTunnelProducer *p = tunnel_.get_producer(client_session_);
    ObMysqlTunnelConsumer *c = NULL;

    if (OB_ISNULL(p) || OB_UNLIKELY(MT_MYSQL_CLIENT != p->vc_type_)) {
      ret = OB_INNER_STAT_ERROR;
      LOG_EDIAG("invalid producer vc type, should be MT_MYSQL_CLIENT",
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
          LOG_EDIAG("invalid internal state, request transform entry must be in tunnel,"
                    "and the vc must be the same",
                    K_(api_.request_transform_info_.entry_->in_tunnel),
                    K_(api_.request_transform_info_.vc),
                    K_(api_.request_transform_info_.entry_->vc), K_(sm_id), K(ret));
        } else if (OB_FAIL(vc_table_.cleanup_entry(api_.request_transform_info_.entry_))) {
          LOG_WDIAG("vc table failed to cleanup server entry", K_(sm_id), K(ret));
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
            LOG_EDIAG("invalid internal state, consumer must be in write success state",
                      K(c), K_(sm_id), K(ret));
          } else {
            handle_request_transfer_failure();
          }
          break;

        case MYSQL_SM_REQUEST_TRANSFER_CLIENT_FAIL:
          // client quit - shutdown the SM
          if (p->read_success_) {
            ret = OB_INNER_STAT_ERROR;
            LOG_EDIAG("invalid internal state, producer should not read success",
                      K_(p->read_success), K_(sm_id), K(ret));
          } else {
            terminate_sm_ = true;
          }
          break;

        case MYSQL_SM_REQUEST_TRANSFER_SUCCESS:
          // The request transfer succeeded
          if (!p->read_success_ || !p->consumer_list_.head_->write_success_) {
            ret = OB_INNER_STAT_ERROR;
            LOG_EDIAG("invalid internal state, producer must read and write sucess",
                      K_(p->read_success), K_(p->consumer_list_.head_->write_success), K_(sm_id), K(ret));
          } else if (client_entry_->in_tunnel_) {
            // When the client completed sending it's data we must have
            // removed it from the tunnel
            ret = OB_INNER_STAT_ERROR;
            LOG_EDIAG("invalid internal state, client entry should not in tunnel",
                      K_(client_entry_->in_tunnel), K_(sm_id), K(ret));
          } else {
            tunnel_.reset();
            server_entry_->in_tunnel_ = false;
            trans_state_.reset_internal_buffer();
            // It's time to start reading the response
            if (OB_FAIL(setup_server_response_read())) {
              LOG_WDIAG("failed to setup_server_response_read", K_(sm_id), K(ret));
            }
          }
          break;

        case MYSQL_SM_REQUEST_TRANSFER_TRANSFORM_FAIL: {
          LOG_WDIAG("MYSQL_SM_REQUEST_TRANSFER_TRANSFORM_FAIL", K_(sm_id), K(ret));
          terminate_sm_ = true;
          break;
        }

        default:
          terminate_sm_ = true;
          ret = OB_INNER_STAT_ERROR;
          LOG_EDIAG("Unknown request transfer status", K_(p->handler_state), K_(sm_id), K(ret));
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
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(MYSQL_TUNNEL_EVENT_DONE != event) || OB_UNLIKELY(data != &tunnel_)) {
    terminate_sm_ = true;
    LOG_EDIAG("unexpected event type", K(event), K_(sm_id));
  } else if (OB_ISNULL(client_entry_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WDIAG("client_entry or server entry is null, disconnect", K_(client_entry), K(ret));
  } else {
    // only the first request in one transaction or internal routing transaction will need to update pl,
    // begin(start transaction), or set autocommit = 0 is not the first request;
    if ((trans_state_.is_trans_first_request_ || client_session_->is_trans_internal_routing())
        && !ObMysqlTransact::is_binlog_request(trans_state_)) {
      ObMysqlTransact::handle_pl_update(trans_state_);
    }

    if (client_session_->is_need_return_last_bound_ss() &&
        (obmysql::OB_MYSQL_COM_STMT_FETCH == trans_state_.trans_info_.sql_cmd_
         || ObMysqlTransact::is_binlog_request(trans_state_))) {
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
          LOG_WDIAG("fail to return last bound server session", K(ret));
        } else {
          trans_state_.current_.state_ = ObMysqlTransact::CMD_COMPLETE;
        }
      } else {
        trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
        LOG_WDIAG("need return last bound ss, but last bound ss is NULL", K(ret));
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

    if (IS_DEBUG_ENABLED() &&
        trans_state_.trans_info_.resp_result_.is_local_infile_0xfb_resp()) {
      LOG_DEBUG("[tunnel_handler_response_transfered] sm starts to transfer content of file request");
    }
  }

  if (OB_FAIL(ret)) {
    trans_state_.inner_errcode_ = ret;
    trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
    call_transact_and_set_next_state(ObMysqlTransact::handle_response);
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
    LOG_EDIAG("server entry and server session should not be NULL",
              K_(server_entry), K_(server_session), K_(sm_id), K(ret));
  } else if (OB_UNLIKELY(server_entry_->vc_ != p.vc_)
             || OB_UNLIKELY(MT_MYSQL_SERVER != p.vc_type_)
             || p.vc_ != server_session_) {
    ret = OB_INNER_STAT_ERROR;
    LOG_EDIAG("invalid internal state, server entry vc, producer vc, server session are different",
              K_(server_entry_->vc), K_(p.vc),
              K_(p.vc_type), K_(server_session), K_(sm_id), K(ret));
  } else {
    switch (event) {
      case VC_EVENT_INACTIVITY_TIMEOUT:
      case VC_EVENT_ACTIVE_TIMEOUT:
      case VC_EVENT_DETECT_SERVER_DEAD:
      case VC_EVENT_ERROR:
        // fall through
      case VC_EVENT_EOS:
        if (event == VC_EVENT_INACTIVITY_TIMEOUT) {
          COLLECT_TIMEOUT_DIAGNOSIS(connection_diagnosis_trace_,
                                    OB_TIMEOUT_DISCONNECT_TRACE,
                                    server_session_ == NULL ? obutils::OB_TIMEOUT_UNKNOWN_EVENT : server_session_->get_inactivity_timeout_event(),
                                    server_session_ == NULL ? 0 : server_session_->get_timeout_record(),
                                    OB_PROXY_INACTIVITY_TIMEOUT,
                                    NULL);
        } else {
          COLLECT_VC_DIAGNOSIS(
              connection_diagnosis_trace_, obutils::OB_SERVER_VC_TRACE, event,
              OB_SERVER_TRANSFERRING_PACKET_CONNECTION_ERROR, NULL);
        }
        switch (event) {
          case VC_EVENT_INACTIVITY_TIMEOUT:
            trans_state_.current_.state_ = ObMysqlTransact::INACTIVE_TIMEOUT;
            break;
          case VC_EVENT_ACTIVE_TIMEOUT:
            trans_state_.current_.state_ = ObMysqlTransact::ACTIVE_TIMEOUT;
            break;
          case VC_EVENT_ERROR:
          case VC_EVENT_DETECT_SERVER_DEAD:
            trans_state_.current_.state_ = ObMysqlTransact::CONNECTION_ERROR;
            break;
          case VC_EVENT_EOS:
            // when event is VC_EVENT_EOS, there are two kinds of situations:
            // 1. ObMysqlTransact::TRANSACTION_COMPLETE:
            //    the current transaction on this connection is completed, but server session is close.
            // 2. ObMysqlTransact::CONNECTION_ERROR:
            //    this connection is handling transactions, but server session is close.
            if (NULL != p.packet_analyzer_.resp_result_
             && p.packet_analyzer_.resp_result_->is_trans_completed()) {
              trans_state_.current_.state_ = ObMysqlTransact::TRANSACTION_COMPLETE;
            } else {
              trans_state_.current_.state_ = ObMysqlTransact::CONNECTION_ERROR;
            }
            break;
          default:
            break;
        }

        MYSQL_INCREMENT_TRANS_STAT(BROKEN_SERVER_CONNECTIONS);

        LOG_WDIAG("[ObMysqlSM::tunnel_handler_server] finishing mysql tunnel", K_(sm_id),
                 "event", ObMysqlTransact::get_server_state_name(trans_state_.current_.state_));
        p.read_success_ = true;
        trans_state_.server_info_.abort_ = ObMysqlTransact::DIDNOT_ABORT;
        if (OB_FAIL(tunnel_.local_finish_all(p))) {
          LOG_EDIAG("fail to do tunnel finish all", K_(sm_id),
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
          LOG_WDIAG("failed to tunnel_handler_server_cmd_complete", K_(sm_id), K(ret));
          close_connection = true;
        }
        // ignore ret, go on
        trans_state_.server_info_.abort_ = ObMysqlTransact::DIDNOT_ABORT;
        if (OB_FAIL(tunnel_.local_finish_all(p))) {
          LOG_EDIAG("fail to do tunnel finish all", K_(sm_id),
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
        LOG_EDIAG("unexpected event", K(event), K_(sm_id), K(ret));
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
        LOG_WDIAG("client session failed to attach server session", K_(sm_id), K(ret));
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
        LOG_WDIAG("vc table failed to cleanup server entry", K_(sm_id), K(ret));
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
  ObRespAnalyzeResult &resp_result = trans_state_.trans_info_.resp_result_;
  // reset db if need
  if (ObMysqlTransact::is_db_reset(trans_state_)) {
    ObMysqlTransact::handle_db_reset(trans_state_);
  }

  // record last_insert_id
  if (!client_session_->get_session_info().is_sharding_user()) {
    if (resp_result.is_last_insert_id_changed()) {
      client_session_->set_lii_server_session(server_session_);
      LOG_DEBUG("last_insert_id is changed, record last_insert_id server session");
    }
  }

  if (resp_result.has_proxy_idc_name_user_var()) {
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
  if (resp_result.has_new_sys_var()) {
    if (OB_ISNULL(sm_cluster_resource_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unexpected cluster resource is null", K(ret));
    } else if (!sm_cluster_resource_->is_avail()) {
      LOG_DEBUG("cluster resource is not created completely");
    } else if (OB_FAIL(sm_cluster_resource_->sys_var_set_processor_.add_sys_var_renew_task(*sm_cluster_resource_))) {
      LOG_WDIAG("fail to add sys var renew task, we will retry at the next request",
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
    ObRespAnalyzeResult &result = trans_state_.trans_info_.resp_result_;
    ROUTE_DIAGNOSIS(route_diagnosis_,
                    HANDLE_RESPONSE,
                    handle_response,
                    OB_SUCCESS,
                    result.is_partition_hit(),
                    trans_state_.current_.send_action_,
                    trans_state_.current_.state_,
                    trans_state_.current_.error_type_);
    bool print_warn_log = false;
    bool print_info_log = false;
    if (OB_LIKELY(NULL != client_session_)) {
      if (OB_UNLIKELY(result.is_error_resp() && client_session_->is_proxy_mysql_client_
          && client_session_->get_session_info().get_priv_info().user_name_ != ObProxyTableInfo::DETECT_USERNAME_USER)) {
        if (result.is_not_supported_error()) {
          print_info_log = true;
        } else {
          print_warn_log = true;
        }
      } else if (!result.is_partition_hit()
                 && trans_state_.is_trans_first_request_
                 && !client_session_->is_proxyro_user()) {//proxyro user no need print partition miss xflush_log
        print_info_log = true;
      }

      if(NULL != p && p->is_flow_controlled()) {
        print_info_log = true;
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
      LOG_WDIAG("finishing mysql tunnel",
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
  ObRespAnalyzeResult &result = trans_state_.trans_info_.resp_result_;
  ObSafeSnapshotEntry *entry = NULL;
  bool is_need_force_sync = true;
  ObAddr last_addr;
  last_addr.set_sockaddr(trans_state_.server_info_.addr_.sa_);

  // partition hit时，我们只更新我们上一次访问的server
  if (result.is_partition_hit()) {
    entry = sm_cluster_resource_->safe_snapshot_mgr_.get(last_addr);
    if (OB_ISNULL(entry)) {
      if (sm_cluster_resource_->is_base_servers_added()) {
        LOG_WDIAG("entry not exist, which is unexpected", K(last_addr));
      }
    } else {
      // in this conditition, we no need to force sync
      is_need_force_sync = false;
      entry->update_safe_read_snapshot(client_session_->get_session_info().get_safe_read_snapshot(),
                                       is_need_force_sync);
      LOG_DEBUG("update read snapshot entry succ ", KPC(entry));
    }
  // partition miss时，我们会同时更新所有的server，并且要求除了本次访问的server的其他server在
  // 下一个请求时进行一次强制同步
  } else {
    int64_t count = client_session_->dummy_ldc_.count();
    const ObLDCItem *item_array = client_session_->dummy_ldc_.get_item_array();
    for (int64_t i = 0; i < count; ++i) {
      if (OB_ISNULL(item_array[i].replica_)) {
        LOG_WDIAG("item_array[i].replica_ is null, ignore it");
      } else {
        entry = sm_cluster_resource_->safe_snapshot_mgr_.get(item_array[i].replica_->server_);
        if (OB_ISNULL(entry)) {
          LOG_INFO("entry is null, maybe new server added", K(item_array[i].replica_->server_));
        } else {
          // 当发生partition miss时，除了本条语句访问的server，
          // 其他server都需要下一次访问时进行一次强制同步
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
  ObRespAnalyzeResult &resp_result = trans_state_.trans_info_.resp_result_;
  ObHRTime trim_ok_packet_begin = 0;
  if (!resp_result.is_last_ok_handled() && resp_result.get_last_ok_pkt_len() > 0) {
    for (ObMysqlTunnelConsumer *c = p.consumer_list_.head_; NULL != c && !found && OB_SUCC(ret); c = c->link_.next_) {
      if (c->alive_ && NULL != c->buffer_reader_) {
        // 1. analyze ok packet
        trim_ok_packet_begin = get_based_hrtime();

        if (OB_FAIL(trim_ok_packet(*(c->buffer_reader_)))) {
          LOG_WDIAG("fail to trim ok packet",  K_(sm_id), K(ret));
        } else {
          resp_result.is_last_ok_handled_ = true;
          // we change the writer_ in analyze_extra_ok_packet/rebuild_ok_packet
          // so we should set bytes_read_ to the corrent value
          p.bytes_read_ += (resp_result.get_rewritten_last_ok_pkt_len()
                            - resp_result.get_last_ok_pkt_len());
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
    LOG_WDIAG("invalid session", K(client_session), K(server_session));
  } else {
    // trim or rebuild ok packet
    bool need_handle_sysvar = sm_cluster_resource_->is_avail();
    ObRespAnalyzeResult &resp_result =
      trans_state_.trans_info_.resp_result_;
    LOG_DEBUG("handle last ok packet", K(need_handle_sysvar),
              "action type", resp_result.get_ok_packet_action_type(),
              "lask ok pkt len", resp_result.get_last_ok_pkt_len());

    if (OK_PACKET_ACTION_CONSUME == resp_result.get_ok_packet_action_type()) {
      if (OB_FAIL(ObProxySessionInfoHandler::analyze_extra_ok_packet(
          reader, client_session->get_session_info(),
          server_session->get_session_info(), need_handle_sysvar, resp_result, trans_state_.trace_log_))) {
        LOG_WDIAG("fail to analyze extra ok packet",  K_(sm_id), K(ret));
      }
    } else if (OK_PACKET_ACTION_REWRITE == resp_result.get_ok_packet_action_type()) {
      ObClientSessionInfo &client_info = client_session->get_session_info();

      const ObProxyBasicStmtType type = trans_state_.trans_info_.client_request_.get_parse_result().get_stmt_type();
      bool is_save_to_common_sys = client_info.is_sharding_user()
                                   && (OBPROXY_T_SET == type || OBPROXY_T_SET_NAMES == type || OBPROXY_T_SET_CHARSET == type);
      if (OB_FAIL(ObProxySessionInfoHandler::rebuild_ok_packet(reader,
                                                               client_session->get_session_info(),
                                                               server_session->get_session_info(),
                                                               trans_state_.is_auth_request_,
                                                               need_handle_sysvar,
                                                               resp_result,
                                                               trans_state_.trace_log_,
                                                               is_save_to_common_sys))) {
        LOG_WDIAG("fail to analyze rewrite ok packet",  K_(sm_id), K(ret));
      }
    }

    // reset route_addr
    client_session->get_session_info().set_obproxy_route_addr(0);

    if (OB_UNLIKELY(is_causal_order_read_enabled())) {
      // handle safe snapshot version
      if (WEAK == trans_state_.get_trans_consistency_level(client_session->get_session_info())
          && trans_state_.trans_info_.client_request_.get_parse_result().is_select_stmt()) {
        update_safe_read_snapshot();
      }
    }

    // handle other variables
    if (OB_SUCC(ret)) {
      bool is_only_sync_trans_sess = trans_state_.trans_info_.resp_result_.is_error_resp();
      if (OB_FAIL(handle_saved_session_variables())) {
        LOG_WDIAG("fail to handle saved session varialbes", K(ret));
      } else if (OB_FAIL(ObProxySessionInfoHandler::save_changed_sess_info(client_session->get_session_info(),
                                                                           server_session->get_session_info(),
                                                                           resp_result.get_extra_info(),
                                                                           trans_state_.trace_log_,
                                                                           is_only_sync_trans_sess))) {
        LOG_WDIAG("fail to save changed session info", K(ret));
      }
    }
  }
  return ret;
}

int ObMysqlSM::handle_feedback_proxy_info(const Ob20ExtraInfo& extra_info)
{
  int ret = OB_SUCCESS;

  if (OB_LIKELY(!extra_info.exist_feedback_proxy_info())) {
    //nothing
  } else if (OB_ISNULL(client_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unknown client session, ignore feedback info", K(sm_id_), K(ret));
  } else {
    LOG_DEBUG("handle feedback proxy info", K(extra_info), K(lbt()));

    // extra_info may be analyzed after last sever session send all data
    // then attach to client_session_ or close direct
    ObMysqlServerSession* last_sever_session = (NULL != server_session_) ?
                                               server_session_ : client_session_->get_server_session();
    ObClientSessionInfo& client_info = client_session_->get_session_info();

    // subtype IS_LOCK_SESSION = 0
    if (OB_ISNULL(last_sever_session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unknown server session, ignore feedback info", K(sm_id_), K(ret));
    } else {
      ObServerSessionInfo& server_info = last_sever_session->get_session_info();
      const Ob20FeedbackProxyInfo& feedback_proxy_info = extra_info.feedback_proxy_info_;

      bool is_lock_session = feedback_proxy_info.is_lock_session_;
      if (is_lock_session == server_info.is_lock_session()) {
        LOG_WDIAG("lock_session should not equal to local", K(is_lock_session));
      } else {
        client_info.lock_session_num_ += is_lock_session ? 1 : -1;
      }

      if (OB_UNLIKELY(client_info.is_session_pool_client_)) {
        LOG_EDIAG("Should not set lock session for pool session!", K(is_lock_session), "local lock session", server_info.is_lock_session());
      } else {
        LOG_DEBUG("set lock session", K(is_lock_session), "local lock session", server_info.is_lock_session());
      }
      server_info.set_lock_session(is_lock_session);
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
  #ifdef ERRSIM
  if (OB_FAIL(OB_E(EventTable::EN_TUNNEL_CLIENT_TIMEOUT) OB_SUCCESS)) {
    ret = OB_SUCCESS;     // verifiy timeout, reset ret
    event = VC_EVENT_INACTIVITY_TIMEOUT;
  }
  #endif
  if (OB_ISNULL(client_entry_) || OB_ISNULL(client_session_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_EDIAG("client entry and client session should not be NULL",
              K_(client_entry), K_(client_session), K_(sm_id), K(ret));
  } else if (OB_UNLIKELY(client_entry_->vc_ != c.vc_)
             || c.vc_ != client_session_) {
    ret = OB_INNER_STAT_ERROR;
    LOG_EDIAG("invalid internal state, client entry vc, consumer vc and client session are different",
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
      case VC_EVENT_DETECT_SERVER_DEAD: {
        if (event == VC_EVENT_INACTIVITY_TIMEOUT) {
          COLLECT_TIMEOUT_DIAGNOSIS(connection_diagnosis_trace_,
                                    OB_TIMEOUT_DISCONNECT_TRACE,
                                    client_session_ == NULL ? obutils::OB_TIMEOUT_UNKNOWN_EVENT : client_session_->get_inactivity_timeout_event(),
                                    client_session_ == NULL ? 0 : client_session_->get_timeout_record(),
                                    OB_PROXY_INACTIVITY_TIMEOUT,
                                    NULL);
        } else {
          COLLECT_VC_DIAGNOSIS(
              connection_diagnosis_trace_, obutils::OB_CLIENT_VC_TRACE, event,
              OB_CLIENT_TRANSFERRING_PACKET_CONNECTION_ERROR, NULL);
        }
        // 探测用户不打印断连接日志
        bool is_detect_user = false;
        if (NULL != client_session_ && client_session_->get_session_info().get_priv_info().user_name_ == ObProxyTableInfo::DETECT_USERNAME_USER) {
          is_detect_user = true;
        }

        if (!is_detect_user) {
          LOG_WDIAG("ObMysqlSM::tunnel_handler_client", "event",
              ObMysqlDebugNames::get_event_name(event), K_(sm_id));
        }
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
      }
      case VC_EVENT_WRITE_COMPLETE: {
        c.write_success_ = true;
        trans_state_.client_info_.abort_ = ObMysqlTransact::DIDNOT_ABORT;
        close_connection = false;

        ObRespAnalyzeResult &result = trans_state_.trans_info_.resp_result_;
        if (result.is_error_resp()) {
          // filtering some error code, to disconnect client session
          if (result.is_net_packet_too_large_error()) {
            COLLECT_INTERNAL_DIAGNOSIS(
                connection_diagnosis_trace_, OB_PROXY_INTERNAL_TRACE,
                result.get_error_pkt().get_err_code(), "%.*s",
                result.get_error_pkt().get_message().length(),
                result.get_error_pkt().get_message().ptr());
            LOG_WDIAG("according to the errcode, client session should disconnect", K_(sm_id), K(result));
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
        COLLECT_INTERNAL_DIAGNOSIS(connection_diagnosis_trace_, OB_PROXY_INTERNAL_TRACE, OB_PROXY_INTERNAL_ERROR, "unexpected vc event reveived while transferring pkg to client");
        LOG_EDIAG("Unexpected event", K(event), K_(sm_id), K(ret));
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
      __attribute__ ((fallthrough));
    case VC_EVENT_ERROR:
    case VC_EVENT_INACTIVITY_TIMEOUT:
    case VC_EVENT_ACTIVE_TIMEOUT:
    case VC_EVENT_DETECT_SERVER_DEAD:
      if (event == VC_EVENT_INACTIVITY_TIMEOUT) {
        COLLECT_TIMEOUT_DIAGNOSIS(connection_diagnosis_trace_,
                                  OB_TIMEOUT_DISCONNECT_TRACE,
                                  client_session_ == NULL ? obutils::OB_TIMEOUT_UNKNOWN_EVENT : client_session_->get_inactivity_timeout_event(),
                                  client_session_ == NULL ? 0 : client_session_->get_timeout_record(),
                                  OB_PROXY_INACTIVITY_TIMEOUT,
                                  NULL);
      } else {
        COLLECT_VC_DIAGNOSIS(
            connection_diagnosis_trace_, obutils::OB_SERVER_VC_TRACE, event,
            OB_CLIENT_TRANSFERRING_PACKET_CONNECTION_ERROR, NULL);
      }
      LOG_WDIAG("ObMysqlSM::tunnel_handler_request_transfer_client", "event",
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
        LOG_EDIAG("client entry must be in tunnel",
                  K_(client_entry_->in_tunnel), K_(sm_id), K(ret));
        dump_history_state();
      } else {
        client_entry_->in_tunnel_ = false;
        if (MT_TRANSFORM == p.consumer_list_.head_->vc_type_) {
          if (!api_.request_transform_info_.entry_->in_tunnel_) {
            ret = OB_INNER_STAT_ERROR;
            LOG_EDIAG("request transform entry must be in tunnel",
                      K_(api_.request_transform_info_.entry_->in_tunnel), K_(sm_id), K(ret));
            dump_history_state();
          }
        } else if (NULL != server_entry_) {
          if (!server_entry_->in_tunnel_) {
            ret = OB_INNER_STAT_ERROR;
            LOG_EDIAG("server entry must be in tunnel",
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
        LOG_WDIAG("client entry failed to do_io_read", K_(sm_id), K(ret));
      }
      break;

    default:
      ret = OB_INNER_STAT_ERROR;
      LOG_EDIAG("Unexpected event", K(event), K_(sm_id), K(ret));
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
    case VC_EVENT_DETECT_SERVER_DEAD:
    {
      if (event == VC_EVENT_INACTIVITY_TIMEOUT) {
        COLLECT_TIMEOUT_DIAGNOSIS(connection_diagnosis_trace_,
                                  OB_TIMEOUT_DISCONNECT_TRACE,
                                  server_session_ == NULL ? obutils::OB_TIMEOUT_UNKNOWN_EVENT : server_session_->get_inactivity_timeout_event(),
                                  server_session_ == NULL ? 0 : server_session_->get_timeout_record(),
                                  OB_PROXY_INACTIVITY_TIMEOUT,
                                  NULL);
      } else {
        COLLECT_VC_DIAGNOSIS(
            connection_diagnosis_trace_, obutils::OB_SERVER_VC_TRACE, event,
            OB_SERVER_TRANSFERRING_PACKET_CONNECTION_ERROR, NULL);
      }
      LOG_WDIAG("ObMysqlSM::tunnel_handler_request_transfer_server", "event",
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
            LOG_EDIAG("invalid internal state, producer vc must be the same as request transform vc",
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
          LOG_EDIAG("client entry and client session should not be NULL",
                    K_(client_entry), K_(client_session), K(client_producer), K_(sm_id), K(ret));
        } else if (OB_UNLIKELY(client_entry_->vc_ != client_producer->vc_)
                   || OB_UNLIKELY(MT_MYSQL_CLIENT != client_producer->vc_type_)
                   || client_producer->vc_ != client_session_) {
          ret = OB_INNER_STAT_ERROR;
          LOG_EDIAG("invalid internal state, client entry vc, client producer vc and client session are different",
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
            LOG_WDIAG("client session failed to do_io_read", K_(sm_id), K(ret));
          } else {
            // We want to shutdown the tunnel here and see if there
            // is a response on from the server. Mark the client
            // as down so that tunnel concludes.
            client_producer->alive_ = false;
            client_producer->handler_state_ = MYSQL_SM_REQUEST_TRANSFER_SERVER_FAIL;
            if (tunnel_.is_tunnel_alive()) {
              ret = OB_INNER_STAT_ERROR;
              LOG_EDIAG("invalid internal state, tunnel should not be alive",
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
      LOG_EDIAG("Unexpected event", K(event), K_(sm_id), K(ret));
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
  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
    milestones_.congestion_control_begin_ = get_based_hrtime();
    milestones_.congestion_control_end_ = 0;
  }

  bool enable_congestion = trans_state_.mysql_config_params_->enable_congestion_;

  // we will lookup congestion, only the follow all happened
  // 1. enable_congestion from config
  // 2. cur congestion is avail(base servers has been added)
  // 3. need pl lookup
  // 4. not mysql route mode
  // 5. need use coordinator session in transaction (included in 'need pl lookup')
  // Attention! when force_retry_congested_, also need do congestion lookup
  if (OB_UNLIKELY(enable_congestion
      && sm_cluster_resource_->is_congestion_avail()
      && trans_state_.is_need_pl_lookup()
      && !trans_state_.mysql_config_params_->is_mysql_routing_mode()
      && (!trans_state_.mysql_config_params_->is_mock_routing_mode() ||
           trans_state_.use_conf_target_db_server_ ||
           trans_state_.use_cmnt_target_db_server_)
      // 注意：目前探测请求和binlog请求使用clientvc时会满足下面条件
      && !(trans_state_.server_info_.addr_.is_valid() && !trans_state_.need_retry_)
      && !ops_is_ip_loopback(trans_state_.server_info_.addr_))) {
    trans_state_.need_congestion_lookup_ = true;
    LOG_DEBUG("need to do congestion lookup",
              K(trans_state_.is_need_pl_lookup()),
              K(trans_state_.pl_lookup_state_));
  } else {
    LOG_DEBUG("no need do congestion lookup", K_(sm_id), K(enable_congestion),
              K(sm_cluster_resource_->is_congestion_avail()),
              "pl_lookup_state", ObMysqlTransact::get_pl_lookup_state_string(trans_state_.pl_lookup_state_),
              "force_retry_congestion", trans_state_.force_retry_congested_,
              "route_mode", trans_state_.mysql_config_params_->server_routing_mode_);
    trans_state_.need_congestion_lookup_ = false;
  }

  if (OB_LIKELY(!trans_state_.need_congestion_lookup_)) { // no need congestion lookup
    // call ObMysqlTransact::handle_congestion_control_lookup() to handle fail / success
    call_transact_and_set_next_state(NULL);
  } else {
    MYSQL_SM_SET_DEFAULT_HANDLER(&ObMysqlSM::state_congestion_control_lookup);
    int ret = OB_SUCCESS;
    if (NULL != pending_action_) {
      ret = OB_INNER_STAT_ERROR;
      LOG_EDIAG("do_congestion_control_lookup, pending_action_ should be NULL",
                K_(pending_action), K_(trans_state_.congestion_entry), K_(sm_id));
    } else {
      if (NULL != trans_state_.congestion_entry_) {
        LOG_WDIAG("congestion entry must be NULL here",
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
        LOG_WDIAG("failed to get congest entry", K_(sm_id), K(ret));
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
  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
    milestones_.pl_lookup_begin_ = get_based_hrtime();
    milestones_.pl_lookup_end_ = 0;
  }

  if (enable_record_full_link_trace_info()) {
    if (flt_.trace_log_info_.partition_location_lookup_ctx_ == NULL) {
      SET_TRACE_BUFFER(flt_trace_buffer_, MAX_TRACE_LOG_SIZE);
      trace::ObSpanCtx *ctx = FLT_BEGIN_SPAN(ob_proxy_partition_location_lookup);
      if (OB_NOT_NULL(ctx)) {
        flt_.trace_log_info_.partition_location_lookup_ctx_ = ctx;
        LOG_DEBUG("begin span ob_proxy_partition_location_lookup", K(ctx->span_id_), K(flt_.span_info_.trace_id_));
      }
    }
  }

  if (OB_UNLIKELY(trans_state_.api_server_addr_set_)) {
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
      LOG_EDIAG("do_partition_location_lookup, pending_action_ should be NULL",
                K_(pending_action), K_(sm_id));
      // call ObMysqlTransact::handle_pl_lookup() to handle fail / success
      call_transact_and_set_next_state(NULL);

    } else {
      const ObTableEntryName &name = trans_state_.pll_info_.te_name_;
      int ret = OB_SUCCESS;
      ObAction *pl_lookup_action_handle = NULL;
      bool find_entry = false;
      int64_t tenant_version = 0;
      bool is_random_routing_mode = trans_state_.mysql_config_params_->is_random_routing_mode();
      bool need_pl_route = (is_pl_route_supported() && (trans_state_.trans_info_.client_request_.get_parse_result().is_call_stmt()
              || trans_state_.trans_info_.client_request_.get_parse_result().is_text_ps_call_stmt()));

      if (OB_UNLIKELY(get_global_proxy_config().check_tenant_locality_change)) {
        tenant_version = sm_cluster_resource_->get_location_tenant_version(
            client_session_->get_session_info().get_priv_info().tenant_name_);
      }
      // 先从table_map中获取
      if (OB_LIKELY(!trans_state_.pll_info_.is_force_renew() && !name.is_all_dummy_table()
                    && !trans_state_.pll_info_.is_cached_dummy_force_renew())
                    && !need_pl_route
                    && !is_random_routing_mode) {
        ObTableRefHashMap &table_map = self_ethread().get_table_map();
        ObTableEntry *tmp_entry = NULL;
        int64_t cr_id = 0;
        if (get_global_resource_pool_processor().get_default_cluster_resource() == sm_cluster_resource_) {
          // default cluster resource cluster id is always 0, and it is used only for building cluster resource
          cr_id = client_session_->get_session_info().get_cluster_id();
        } else {
          cr_id = sm_cluster_resource_->get_cluster_id();
        }
        ObTableEntryKey key(name, sm_cluster_resource_->version_, cr_id);
        tmp_entry = table_map.get(key);
        if (NULL != tmp_entry && !tmp_entry->is_partition_table()
            && (tmp_entry->is_avail_state() || tmp_entry->is_updating_state())
            && !(get_global_table_cache().is_table_entry_expired(*tmp_entry))) {
          tmp_entry->check_and_set_expire_time(tenant_version, tmp_entry->is_sys_dummy_entry());

          tmp_entry->renew_last_access_time();
          ObMysqlRouteResult result;
          result.table_entry_ = tmp_entry;
          result.is_table_entry_from_remote_ = false;
          result.has_dup_replica_ = tmp_entry->has_dup_replica();
          tmp_entry->set_need_force_flush(false);
          find_entry = true;
          bool is_table_entry_from_remote = false;
          LOG_DEBUG("ObMysqlRoute get table entry succ", KPC(tmp_entry), K(is_table_entry_from_remote));
          if (!tmp_entry->is_dummy_entry()) {
            ROUTE_DIAGNOSIS(route_diagnosis_,
                            TABLE_ENTRY_LOOKUP_DONE,
                            table_entry_lookup_done,
                            ret,
                            tmp_entry,
                            is_table_entry_from_remote,
                            true);
          }
          state_partition_location_lookup(TABLE_ENTRY_EVENT_LOOKUP_DONE, &result);
        } else if (NULL != tmp_entry) {
          tmp_entry->dec_ref();
        }
      }

      if (OB_UNLIKELY(!find_entry)) {
        ObRouteParam param;
        param.cont_ = this;
        param.force_renew_ = trans_state_.pll_info_.is_force_renew();
        param.is_need_force_flush_ = trans_state_.pll_info_.is_need_force_flush();
        param.use_lower_case_name_ = client_session_->get_session_info().need_use_lower_case_names();
        param.mysql_proxy_ = &sm_cluster_resource_->mysql_proxy_;
        param.cr_version_ = sm_cluster_resource_->version_;
        param.cluster_version_ = sm_cluster_resource_->cluster_version_;
        if (get_global_resource_pool_processor().get_default_cluster_resource() == sm_cluster_resource_) {
          // default cluster resource cluster id is always 0, and it is used only for building cluster resource
          param.cr_id_ = client_session_->get_session_info().get_cluster_id();
        } else {
          param.cr_id_ = sm_cluster_resource_->get_cluster_id();
        }
        param.tenant_version_ = tenant_version;
        param.timeout_us_ = hrtime_to_usec(trans_state_.mysql_config_params_->short_async_task_timeout_);
        param.is_partition_table_route_supported_ = is_partition_table_route_supported();
        param.is_oracle_mode_ = client_session_->get_session_info().is_oracle_mode();
        param.client_request_ = &trans_state_.trans_info_.client_request_; // priv parse result
        param.client_info_ = &client_session_->get_session_info();
        param.route_ = &trans_state_.pll_info_.route_;
        param.need_pl_route_ = need_pl_route;
        param.current_idc_name_ = client_session_->get_current_idc_name();//shallow copy
        param.set_route_diagnosis(route_diagnosis_);
        if (trans_state_.pll_info_.is_cached_dummy_force_renew() || is_random_routing_mode) {
          param.need_pl_route_ = false;
          param.name_.shallow_copy(name.cluster_name_, name.tenant_name_,
                                  ObString::make_string(OB_SYS_DATABASE_NAME),
                                  ObString::make_string(OB_ALL_DUMMY_TNAME));
        } else {
          param.name_.shallow_copy(name);
        }
      if (!param.name_.is_all_dummy_table()) {
        ROUTE_DIAGNOSIS(trans_state_.sm_->route_diagnosis_,
                        LOCATION_CACHE_LOOKUP,
                        location_cache_lookup,
                        OB_SUCCESS,
                        trans_state_.mysql_config_params_->server_routing_mode_,
                        !trans_state_.pll_info_.lookup_success_ && !trans_state_.api_server_addr_set_);
        }
        LOG_DEBUG("Doing partition location Lookup", K_(sm_id), K(param));

        pl_lookup_action_handle = NULL;
        ret = ObMysqlRoute::get_route_entry(param, sm_cluster_resource_, pl_lookup_action_handle);
      }

      if (OB_SUCC(ret)) {
        if (OB_LIKELY(NULL == pl_lookup_action_handle)) {
          // cache hit and has called back, do nothing
        } else {
          pending_action_ = pl_lookup_action_handle;
        }
      } else {
        trans_state_.pll_info_.lookup_success_ = false;
        LOG_WDIAG("failed to get table entry", K_(sm_id));
        // call ObMysqlTransact::handle_pl_lookup() to handle fail / success
        call_transact_and_set_next_state(NULL);
      }
    }
  }
}

void ObMysqlSM::do_binlog_location_lookup()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
    milestones_.bl_lookup_begin_ = get_based_hrtime();
    milestones_.bl_lookup_end_ = 0;
  }

  LOG_DEBUG("do binlog location lookup");
  MYSQL_SM_SET_DEFAULT_HANDLER(&ObMysqlSM::state_binlog_location_lookup);
  if (OB_UNLIKELY(NULL != pending_action_)) {
    trans_state_.pll_info_.lookup_success_ = false;
    LOG_EDIAG("do_binlog_location_lookup, pending_action_ should be NULL",
              K_(pending_action), K_(sm_id));
    call_transact_and_set_next_state(NULL);
  } else if (trans_state_.trans_info_.client_request_.get_parse_result().is_show_binlog_server_for_tenant_stmt()
             && trans_state_.server_info_.addr_.is_valid()) {
    trans_state_.pll_info_.lookup_success_ = true;
    call_transact_and_set_next_state(NULL);
  } else {
    const ObTableEntryName &name = trans_state_.pll_info_.te_name_;
    ObAction *bl_lookup_action_handle = NULL;

    ObRouteParam param;
    param.cont_ = this;
    param.need_pl_route_ = false;
    param.force_renew_ = true;
    param.is_need_force_flush_ = trans_state_.pll_info_.is_need_force_flush();
    param.mysql_proxy_ = &sm_cluster_resource_->mysql_proxy_;
    param.timeout_us_ = hrtime_to_usec(trans_state_.mysql_config_params_->short_async_task_timeout_);
    param.name_.shallow_copy(name);
    param.cr_version_ = sm_cluster_resource_->version_;
    param.cr_id_ = 0;
    if (OB_NOT_NULL(multi_level_config_)) {
      param.binlog_service_ip_ = multi_level_config_->binlog_service_ip_;
    }
    LOG_DEBUG("Doing binlog location lookup", K_(sm_id), K(param));
    ret = ObMysqlRoute::get_route_entry(param, sm_cluster_resource_, bl_lookup_action_handle);

    if (OB_SUCC(ret)) {
      if (NULL == bl_lookup_action_handle) {
        // do nothing
      } else {
        pending_action_ = bl_lookup_action_handle;
      }
    } else {
      trans_state_.pll_info_.lookup_success_ = false;
      LOG_WDIAG("fail to get binlog table entry", K_(sm_id), K(name), K(ret));
      call_transact_and_set_next_state(NULL);
    }
  }
}

void ObMysqlSM::do_server_addr_lookup()
{
  ObProxyKillQueryInfo *query_info = trans_state_.trans_info_.client_request_.query_info_;
  if (OB_ISNULL(query_info)) {
    LOG_WDIAG("[ObMysqlSM::do_server_addr_lookup] query_info should not be null", K_(sm_id));
    // call ObMysqlTransact::handle_server_addr_lookup() to handle fail / success
    call_transact_and_set_next_state(NULL);

  } else if (!trans_state_.is_need_pl_lookup()) {
    // need use last server session
    query_info->errcode_ = OB_NOT_SUPPORTED;
    LOG_WDIAG("[ObMysqlSM::do_server_addr_lookup] As we need use last server session, "
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
      LOG_WDIAG("failed to lookup server addr", K_(sm_id), K(*query_info), K(addr_lookup_action), K(ret));
      // call ObMysqlTransact::handle_server_addr_lookup() to handle fail / success
      call_transact_and_set_next_state(NULL);
    }
  }//end of is_need_pl_lookup
}

int ObMysqlSM::state_congestion_control_lookup(int event, void *data)
{
  STATE_ENTER(ObMysqlSM::state_congestion_control_lookup, event);
  if (CONGESTION_EVENT_CONTROL_LOOKUP_DONE == event) {
    trans_state_.congestion_lookup_success_ = true;
    if (OB_ISNULL(data)) {
      LOG_WDIAG("congestion entry is not found", "addr", trans_state_.server_info_.addr_, K_(sm_id));
    } else {
      trans_state_.congestion_entry_ = reinterpret_cast<ObCongestionEntry *>(data);
    }
  } else {
    trans_state_.congestion_lookup_success_ = false;
    LOG_EDIAG("unexpected event type, it should not happen", K(event), K(data),
              "addr", trans_state_.server_info_.addr_, K_(sm_id));
  }

  pending_action_ = NULL;
  // call ObMysqlTransact::handle_congestion_control_lookup() to handle fail / success
  call_transact_and_set_next_state(NULL);

  return EVENT_DONE;
}

void ObMysqlSM::set_detect_server_info(net::ObIpEndpoint target_addr, int cnt, int64_t time)
{
  if (!ObMysqlTransact::is_binlog_request(trans_state_)
      && OB_LIKELY(target_addr.is_valid()
      && NULL != sm_cluster_resource_
      && NULL != client_session_
      && !client_session_->is_proxy_mysql_client_
      //优化关闭场景下的性能，1表示精准探测
      && (1 == get_global_proxy_config().server_detect_mode || add_detect_server_cnt_))) {
    bool found = false;
    common::DRWLock &server_state_lock1 = sm_cluster_resource_->get_server_state_lock(0);
    common::DRWLock &server_state_lock2 = sm_cluster_resource_->get_server_state_lock(1);
    server_state_lock1.rdlock();
    server_state_lock2.rdlock();
    common::ObIArray<ObServerStateSimpleInfo> &server_state_info =
      sm_cluster_resource_->get_server_state_info(sm_cluster_resource_->server_state_version_);
    for (int i = 0; !found && i < server_state_info.count(); i++) {
      ObServerStateSimpleInfo &info = server_state_info.at(i);
      ObIpEndpoint addr(info.addr_.get_sockaddr());
      if (addr == target_addr) {
        // add_detect_server_cnt_防止do_observer_open之前失败，对cnt减1操作
        if (cnt > 0 && 1 == get_global_proxy_config().server_detect_mode) {
          add_detect_server_cnt_ = true;
          (void)ATOMIC_AAF(&info.request_sql_cnt_, cnt);
        } else if (cnt < 0) {
          if (add_detect_server_cnt_) {
            add_detect_server_cnt_ = false;
            (void)ATOMIC_AAF(&info.request_sql_cnt_, cnt);
          }
        }
        if (0 != time) {
          (void)ATOMIC_SET(&info.last_response_time_, time);
          (void)ATOMIC_SET(&info.detect_fail_cnt_, 0);
        }
        LOG_DEBUG("set detect server info", K(addr), K(cnt), K(time), K(info), K(sm_cluster_resource_->server_state_version_));
        found = true;
      }
    }
    server_state_lock2.rdunlock();
    server_state_lock1.rdunlock();

    if (!found) {
      LOG_WDIAG("not found server", K(target_addr), K(cnt), KPC(sm_cluster_resource_));
    }
  }
}

int ObMysqlSM::do_observer_open()
{
  int ret = OB_SUCCESS;
  MYSQL_SM_SET_DEFAULT_HANDLER(&ObMysqlSM::state_observer_open);

  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
    milestones_.do_observer_open_begin_ = get_based_hrtime();
    milestones_.do_observer_open_end_ = 0;
  }

  if (enable_record_full_link_trace_info()) {
    if (flt_.trace_log_info_.do_observer_open_ctx_ == NULL) {
      SET_TRACE_BUFFER(flt_trace_buffer_, MAX_TRACE_LOG_SIZE);
      trace::ObSpanCtx *ctx = FLT_BEGIN_SPAN(ob_proxy_do_observer_open);
      if (OB_NOT_NULL(ctx)) {
        flt_.trace_log_info_.do_observer_open_ctx_ = ctx;
        LOG_DEBUG("begin span ob_proxy_do_observer_open", K(ctx->span_id_));
      }
    }
  }

  // if sync all variables completed, send request directly through handle_observer_open
  if (OB_UNLIKELY(trans_state_.send_reqeust_direct_)) {
    if (OB_ISNULL(server_session_) || ObMysqlTransact::SERVER_SEND_REQUEST != trans_state_.current_.send_action_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unexpected state", K(server_session_), K(trans_state_.current_.send_action_), K(ret));
    } else {
      handle_observer_open();
    }
  } else {
    // We need to close the previous attempt, except reroute
    if (OB_UNLIKELY(trans_state_.is_rerouted_)) {
      release_server_session();
    }

    if (add_detect_server_cnt_) {
      set_detect_server_info(trans_state_.pre_server_info_.addr_, -1, 0);
    }
    // 大请求会进入do_observer_open两次，第二次会设置send_request_direct为true
    set_detect_server_info(trans_state_.server_info_.addr_, 1, 0);
    trans_state_.pre_server_info_.addr_ = trans_state_.server_info_.addr_;

    if (OB_UNLIKELY(NULL != server_entry_)) {
      if (MYSQL_SERVER_VC != server_entry_->vc_type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_EDIAG("invlid internal state, server entry vc type is unexpected",
                  K_(server_entry_->vc_type), K_(sm_id), K(ret));
      } else if (OB_FAIL(vc_table_.cleanup_entry(server_entry_))) {
        LOG_WDIAG("vc table failed to cleanup server entry",
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
      if (OB_LIKELY(NULL != client_session_)) {
        client_session_->get_netvc()->cancel_inactivity_timeout();
      }
    }

    if (OB_UNLIKELY(NULL != server_entry_)
        || OB_ISNULL(client_entry_)
        || OB_ISNULL(client_session_)
        || OB_UNLIKELY(NULL != pending_action_)
        || OB_UNLIKELY(0 == trans_state_.server_info_.get_port())) {
      ret = OB_INNER_STAT_ERROR;
      LOG_EDIAG("invalide internal state, server entry and pending action should be NULL,"
                "client entry and client session should not be NULL",
                K_(server_entry), K_(client_entry), K_(client_session), K_(pending_action),
                "server_port", trans_state_.server_info_.get_port(),
                K_(sm_id), K(ret));
    } else {
      LOG_DEBUG("will open connection to",
                "addr", trans_state_.server_info_.addr_, K_(sm_id),
                "force_retry_congested", trans_state_.force_retry_congested_);

      // we check this version only when safe_weak_read is enable
      if (OB_UNLIKELY(client_session_->get_session_info().is_oceanbase_server()
          && is_causal_order_read_enabled()
          && NULL != client_session_
          && WEAK == trans_state_.get_trans_consistency_level(client_session_->get_session_info())
          && trans_state_.trans_info_.client_request_.get_parse_result().is_select_stmt())) {
        ObMysqlTransact::check_safe_read_snapshot(trans_state_);
      }

      if (OB_UNLIKELY(NULL != api_.plugin_tunnel_)) {
        ObPluginVCCore *t = api_.plugin_tunnel_;

        api_.plugin_tunnel_ = NULL;
        ObAction *pvc_action_handle = NULL;
        ret = t->connect_re(this, pvc_action_handle);
        if (OB_FAIL(ret) || NULL != pvc_action_handle) {
          LOG_WDIAG("failed plugin vc to connect_re", K(pvc_action_handle), K_(sm_id), K(ret));
        }
      } else if (OB_UNLIKELY(MYSQL_NO_PLUGIN_TUNNEL != api_.plugin_tunnel_type_)) {
        ret = OB_INNER_STAT_ERROR;
        LOG_EDIAG("invalid tunnel type, should be MYSQL_NO_PLUGIN_TUNNEL",
                  K_(api_.plugin_tunnel_type), K_(sm_id), K(ret));
      } else if (OB_FAIL(do_internal_observer_open())) {
        LOG_WDIAG("failed to do_internal_observer_open", K_(sm_id), K(ret));
      }
    }
  }

  if (OB_UNLIKELY(OB_SESSION_POOL_FULL_ERROR == ret)) {
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
    LOG_WDIAG("invalid client_session");
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
        LOG_WDIAG("fail to handle_retry_acquire_svr_session ", K(this), K(interval));
        ret = OB_ERR_UNEXPECTED;
      } else {
        LOG_DEBUG("succ to retry acquire svr session", K(diff_time), K(interval));
      }
    } else {
      ret = OB_SESSION_POOL_FULL_ERROR;
      trans_state_.mysql_errcode_ = OB_SESSION_POOL_FULL_ERROR;
      trans_state_.mysql_errmsg_ = "No empty server session, acquire session failed";
      if (OB_FAIL(ObMysqlTransact::encode_error_message(trans_state_))) {
        LOG_WDIAG("fail to encode err pacekt buf",
                 K_(sm_id), "errcode", trans_state_.mysql_errcode_,
                 "user_err_msg", trans_state_.mysql_errmsg_, K(ret));
      } else {
        LOG_WDIAG("fail to acquire svr session after retry", K(diff_time), K(retry_acquire_server_session_count_));
        retry_acquire_server_session_count_ = 0;
        start_acquire_server_session_time_ = 0;
        if (OB_FAIL(client_buffer_reader_->consume_all())) {
          LOG_WDIAG("fail to consume all", K_(sm_id), K(ret));
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
    LOG_WDIAG("get_sockaddr failed", K(common_addr), K(ret));
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
  // if is_need_pl_lookup is false, we must use last_server_session/coordinator_sesssion(internal routing transaction)
  // COM_STMT_CLOSE/COM_STMT_FETCH 命令在与事务解绑时允许在NEED_PL_LOOKUP的情况下没有 last server session
  obmysql::ObMySQLCmd cmd = trans_state_.trans_info_.sql_cmd_;
  bool is_text_ps_close = trans_state_.trans_info_.client_request_.get_parse_result().is_text_ps_drop_stmt();
  if (!trans_state_.is_need_pl_lookup()
      && !ObMysqlTransact::is_binlog_request(trans_state_)
      && ((OB_MYSQL_COM_STMT_CLOSE != cmd
           && OB_MYSQL_COM_STMT_FETCH != cmd
           && OB_MYSQL_COM_STMT_RESET != cmd
           && OB_MYSQL_COM_STMT_GET_PIECE_DATA != cmd
           && !is_text_ps_close)
          || !client_session_->is_need_return_last_bound_ss())) {
    ObMysqlServerSession *target_session = NULL;
    if (trans_state_.pl_lookup_state_ == ObMysqlTransact::USE_COORDINATOR_SESSION
        || trans_state_.pl_lookup_state_ == ObMysqlTransact::USE_SHARD_TXN_SESSION) {
      if (OB_FAIL(client_session_->acquire_svr_session(trans_state_.server_info_.addr_.sa_, false, target_session))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("acquire coordinator session fail, disconnect", K_(sm_id), K(ret));
      }
      LOG_DEBUG("use coordinator server session", K_(sm_id));
    } else if (trans_state_.pl_lookup_state_ == ObMysqlTransact::USE_LAST_SERVER_SESSION){
      target_session = last_session;
      LOG_DEBUG("use last server session", K_(sm_id));
    } else if (trans_state_.pl_lookup_state_ == ObMysqlTransact::USE_LOCK_SERVER_SESSION) {
      // lock_ss_ don't hold server session always
      // now target_session hold it, need release from pool
      target_session = client_session_->get_lock_server_session();
      client_session_->set_lock_server_session(NULL);
      if (OB_ISNULL(target_session)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_EDIAG("fail to set lock session", K(ret));
      } else if (target_session == last_session) {
        LOG_DEBUG("use last lock server session");
      } else if (OB_FAIL(client_session_->acquire_svr_session(target_session->server_ip_.sa_, false, target_session))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to acquire lock session", K_(sm_id), K(ret));
      } else {
        // nothing
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unexpected pl lookup state", K(trans_state_.pl_lookup_state_));
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(target_session)) {
        ret = OB_ERR_UNEXPECTED;
        COLLECT_INTERNAL_DIAGNOSIS(connection_diagnosis_trace_, OB_PROXY_INTERNAL_TRACE, OB_PROXY_INTERNAL_ERROR, "target session is closed, disconnect") ;
        LOG_WDIAG("target server session is NULL, disconnect", K_(sm_id), K(ret));
      } else if (OB_UNLIKELY(target_session->server_ip_ != trans_state_.server_info_.addr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_EDIAG("target session ip is unexpected",
                  K_(target_session->server_ip), K_(trans_state_.server_info_.addr), K_(sm_id), K(ret));
      } else {
        selected_session = target_session;
      }
    }
  } else {
    LOG_DEBUG("sql cmd", K(cmd), K(client_session_->is_session_pool_client()));
    if (OB_UNLIKELY(OB_MYSQL_COM_LOGIN == cmd
                    && client_session_->is_session_pool_client()
                    && !client_session_->can_direct_ok())) {
      LOG_DEBUG("COM_LOGIN here not use pool");
      // only proxy_mysql_client for session pool use pool_sever_addr
      // this logic is for pre connection create
      if (client_session_->is_proxy_mysql_client_) {
        ret = use_set_pool_addr();
      }
    } else {
      bool need_close_last_ss = false;
      if (OB_UNLIKELY(client_session_->get_session_info().is_read_consistency_set())) {
        need_close_last_ss = need_close_last_used_ss();
      }
      ret = client_session_->acquire_svr_session(trans_state_.server_info_.addr_.sa_, need_close_last_ss, selected_session);
      if (OB_LIKELY((OB_SUCCESS == ret && NULL != selected_session)
          || (OB_SESSION_NOT_FOUND == ret && NULL == selected_session))) {
        ret = OB_SUCCESS;
      } else if (OB_UNLIKELY(OB_SESSION_POOL_FULL_ERROR == ret)) {
        // now last is release
        client_session_->attach_server_session(NULL);
        server_entry_ = NULL;
        LOG_WDIAG("server session pool is full", K_(sm_id), K(selected_session),
                 "server_ip", trans_state_.server_info_.addr_, K(ret));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("failed to acquire server session", K_(sm_id), K(selected_session),
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
    LOG_DEBUG("COM_LOGIN here not use pool");
    // only proxy_mysql_client for session pool use pool_sever_addr
    // this logic is for pre connection create
    if (client_session_->is_proxy_mysql_client_) {
      ret = use_set_pool_addr();
    }
    ret = OB_SESSION_NOT_FOUND;
  } else {
    //这里有连接池可能同一个地址多个连接，事务中不能直接acquire
    if (ObMysqlTransact::is_in_trans(trans_state_)) {
      ObMysqlServerSession *last_session = NULL;
      last_session = client_session_->get_server_session();
      if (OB_ISNULL(last_session)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("last server session is NULL, disconnect", K_(sm_id), K(ret));
      } else if (OB_UNLIKELY(last_session->server_ip_ != trans_state_.server_info_.addr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_EDIAG("last server session ip is unexpected",
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
    LOG_WDIAG("failed to acquire server session", K_(sm_id), K(selected_session),
             "server_ip", trans_state_.server_info_.addr_, K(ret));
  }
  return ret;
}

inline int ObMysqlSM::do_internal_observer_open()
{
  int ret = OB_SUCCESS;
  ObMysqlServerSession *selected_session = NULL;
  ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
  bool is_in_trans = ObMysqlTransact::is_in_trans(trans_state_);

  #ifdef ERRSIM
  if (OB_SUCC(ret) && OB_FAIL(OB_E(EventTable::EN_SERVER_OPEN_FAIL) OB_SUCCESS)) {
    ret = OB_SUCCESS;
    ops_ip_pton("2.2.2.2", trans_state_.server_info_.addr_);
  }
  #endif

  if (OB_LIKELY(client_session_->get_session_info().is_oceanbase_server())) {
    if (OB_FAIL(do_oceanbase_internal_observer_open(selected_session))) {
      LOG_WDIAG("failed to do oceanbase internal observer open", K(ret));
    }
  } else if (OB_FAIL(do_normal_internal_observer_open(selected_session))) {
    LOG_WDIAG("failed to do normal internal observer open", K(ret));
  }

  // if we should force use last_session , but last_session is NULL,
  // we will close client session, and the client_session_ in sm will be reset to NULL.
  // so, in this condition, here we must judge client_session_ is NULL or not;
  if (OB_SUCC(ret)) {
    // clear client session's bound_ss_
    client_session_->attach_server_session(NULL);
    if (OB_LIKELY(NULL != selected_session)) {
      selected_session->state_ = MSS_ACTIVE;
      selected_session->set_client_session(*client_session_);
      if (OB_UNLIKELY(client_session_->can_direct_ok())) {
        // sharding user maybe skip real connect, set flag now
        proxy::ObMysqlAuthRequest& login_req = client_session_->get_session_info().get_login_req();
        ObMySQLCapabilityFlags& cap_flag = login_req.get_hsr_result().response_.get_capability_flags();
        cap_flag.cap_flags_.OB_CLIENT_SESSION_TRACK = 1;
      }

      if (OB_FAIL(attach_server_session(*selected_session))) {
        LOG_WDIAG("failed to attach_server_session", K_(sm_id), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_LIKELY(NULL != server_session_)) {
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

      obmysql::ObMySQLCmd cmd = trans_state_.trans_info_.client_request_.get_packet_meta().cmd_;
      LOG_DEBUG("get server session from connection pool", K_(sm_id),
                "server addr", server_session_->server_ip_,
                K(server_session_->ss_id_),
                "client_session version", client_info.get_session_version(),
                "server version", server_info.get_session_var_version(),
                "cmd", cmd);

      trans_state_.trace_log_.log_it("[get_conn]",
                                     "time", get_based_hrtime(),
                                     "lookup", static_cast<int64_t>(trans_state_.pl_lookup_state_),
                                     "sql_cmd", static_cast<int64_t>(trans_state_.trans_info_.sql_cmd_),
                                     "stmt_type", static_cast<int64_t>(trans_state_.trans_info_.client_request_.get_parse_result().get_stmt_type()),
                                     "in_trans", is_in_trans,
                                     "internal_route", get_client_session()->is_trans_internal_routing() && is_in_trans,
                                     "svr", server_session_->server_ip_,
                                     "sessid", static_cast<int64_t>(server_session_->get_server_sessid()));

      if (OB_UNLIKELY(OB_MYSQL_COM_STMT_CLOSE == cmd
                      || OB_MYSQL_COM_STMT_RESET == cmd
                      || client_session_->can_direct_send_request_)) {
        /* CLOSE/RESET 请求不同步任何变量 */
        trans_state_.current_.send_action_ = ObMysqlTransact::SERVER_SEND_REQUEST;
      // 跟 logproxy 开发确认，只对 session 变量有同步要求，其它都不需要同步
      } else if (OB_UNLIKELY(ObMysqlTransact::is_binlog_request(trans_state_))) {
        if (client_info.need_reset_session_vars(server_info)) {
          trans_state_.current_.send_action_ = ObMysqlTransact::SERVER_SEND_SESSION_VARS;
        } else {
          trans_state_.current_.send_action_ = ObMysqlTransact::SERVER_SEND_REQUEST;
        }
      } else if (OB_UNLIKELY(client_info.need_reset_database(server_info))) {
        trans_state_.current_.send_action_ = ObMysqlTransact::SERVER_SEND_USE_DATABASE;
      // sync the changed sys vars by config
      } else if (OB_UNLIKELY((!client_info.is_server_support_session_var_sync() ||
                               client_info.need_reset_conf_sys_vars()) &&
                               client_info.need_reset_session_vars(server_info))) {
        trans_state_.current_.send_action_ = ObMysqlTransact::SERVER_SEND_SESSION_VARS;
      } else if (OB_UNLIKELY(client_info.is_server_support_session_var_sync() &&
                             client_info.need_reset_user_session_vars(server_info))) {
        trans_state_.current_.send_action_ = ObMysqlTransact::SERVER_SEND_SESSION_USER_VARS;
      } else if (OB_UNLIKELY(trans_state_.is_hold_start_trans_)) {
        trans_state_.current_.send_action_ = ObMysqlTransact::SERVER_SEND_START_TRANS;
      } else if (OB_UNLIKELY(trans_state_.is_hold_xa_start_)) {
        trans_state_.current_.send_action_ = ObMysqlTransact::SERVER_SEND_XA_START;
        LOG_DEBUG("[ObMysqlSM::do_internal_observer_open] set send action SERVER_SEND_XA_START to sync xa start");
      } else if (OB_UNLIKELY(((OB_MYSQL_COM_STMT_EXECUTE == cmd)
                              || (OB_MYSQL_COM_STMT_SEND_PIECE_DATA == cmd)
                              || (OB_MYSQL_COM_STMT_SEND_LONG_DATA == cmd))
                             && client_info.need_do_prepare(server_info))) {
        trans_state_.current_.send_action_ = ObMysqlTransact::SERVER_SEND_PREPARE;
      } else if (OB_UNLIKELY(client_request.get_parse_result().is_text_ps_execute_stmt() &&
        client_info.need_do_text_ps_prepare(server_info))) {
        trans_state_.current_.send_action_ = ObMysqlTransact::SERVER_SEND_TEXT_PS_PREPARE;
      } else if (!trans_state_.is_proxysys_tenant_
                && !client_session_->is_proxy_mysql_client_
                && !trans_state_.is_auth_request_
                && !client_info.get_init_sql().empty()) {
        trans_state_.current_.send_action_ = ObMysqlTransact::SERVER_SEND_INIT_SQL;
      } else {
        trans_state_.current_.send_action_ = ObMysqlTransact::SERVER_SEND_REQUEST;
      }
      handle_observer_open();
    } else {
      if (ObMysqlTransact::is_in_trans(trans_state_)
          && client_session_->is_proxy_enable_trans_internal_routing()
          && trans_state_.server_info_.addr_ == client_session_->get_trans_coordinator_ss_addr()) {
        ret = OB_PROXY_RECONNECT_COORDINATOR;
        LOG_WDIAG("try to repeatedly connect to coordinator in transaction, disconnect", K(ret));
      } else if (OB_FAIL(connect_observer())) {
        LOG_WDIAG("failed to connection observer", K_(sm_id), K(ret));
      } else {

        trans_state_.trace_log_.log_it("[create_conn]",
                                       "time", get_based_hrtime(),
                                       "lookup", static_cast<int64_t>(trans_state_.pl_lookup_state_),
                                       "sql_cmd", static_cast<int64_t>(trans_state_.trans_info_.sql_cmd_),
                                       "stmt_type", static_cast<int64_t>(trans_state_.trans_info_.client_request_.get_parse_result().get_stmt_type()),
                                       "in_trans", is_in_trans,
                                       "internal_route", get_client_session()->is_trans_internal_routing() && is_in_trans,
                                       "svr", trans_state_.server_info_.addr_);
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
  ObShardProp *shard_prop = client_session_->get_session_info().get_shard_prop();
  int64_t connect_timeout = 0;
  if (OB_NOT_NULL(shard_prop)) {
    connect_timeout = HRTIME_MSECONDS(shard_prop->get_connect_timeout());
  } else {
    connect_timeout = trans_state_.mysql_config_params_->short_async_task_timeout_;
  }

  LOG_DEBUG("calling g_net_processor.connect", K_(sm_id), K(trans_state_.server_info_.addr_));
  ret = g_net_processor.connect(*this, trans_state_.server_info_.addr_.sa_,
                                connect_action_handle, connect_timeout, &opt);
  if (OB_FAIL(ret)) {
    LOG_WDIAG("failed to connect observer", K_(sm_id), K(ret));
  } else if (OB_ISNULL(connect_action_handle)) {
    // connect fail, net module has called back, do nothing
  } else if (NULL != pending_action_) {
    if (OB_SUCCESS != connect_action_handle->cancel()) {
      LOG_WDIAG("failed to cancel connect observer pending action", K_(sm_id), K(connect_action_handle));
    }
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid internal state, pending_action_ is not NULL", K_(pending_action), K_(sm_id), K(ret));
  } else {
    pending_action_ = connect_action_handle;
  }
  return ret;
}

int ObMysqlSM::do_internal_request_for_sharding_init_db(ObMIOBuffer *buf)
{
  int ret = OB_SUCCESS;

  ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
  ObMysqlClientSession *client_session = get_client_session();
  ObClientSessionInfo &client_info = client_session->get_session_info();
  ObProxyProtocol client_procotol = get_client_session_protocol();

  // handle use db stmt or COM_INIT_DB cmd when sharding
  LOG_DEBUG("sharding init db");
  ObHSRResult &hsr = client_info.get_login_req().get_hsr_result();
  ObString db_name;
  if (client_request.get_parse_result().is_use_db_stmt()) {
    db_name = client_request.get_parse_result().get_database_name();
  } else {
    db_name = client_request.get_sql();
  }
  if (OB_FAIL(ObProxyShardUtils::handle_shard_use_db(trans_state_, *client_session, db_name))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // response unknown db
      trans_state_.mysql_errcode_ = OB_ERR_BAD_DATABASE;
      trans_state_.mysql_errmsg_ = ob_strerror(OB_ERR_BAD_DATABASE);
      if (OB_FAIL(ObMysqlTransact::encode_error_message(trans_state_))) {
        LOG_WDIAG("[ObMysqlSM::do_internal_request] fail to build err resp", K_(sm_id), K(ret));
      }
    } else if (OB_ERR_NO_DB_PRIVILEGE == ret) {
      if (OB_FAIL(ObMysqlTransact::build_no_privilege_message(trans_state_, *client_session, db_name))) {
        LOG_WDIAG("fail to build_no_privilege_message", K(db_name), K(ret));
      }
    } else {
      LOG_WDIAG("handle shard use db failed", K(ret));
    }
  } else if (OB_FAIL(save_user_login_info(client_info, hsr))) {
    LOG_WDIAG("fail to save user login info", K_(sm_id), K(ret));
  } else {
    // if start trans is hold, we treat this resp is in trans
    bool is_in_trans = (trans_state_.is_hold_xa_start_
                        || trans_state_.is_hold_start_trans_
                        || ObMysqlTransact::is_in_trans(trans_state_));
    if (OB_FAIL(ObMysqlResponseBuilder::build_ok_resq_with_state_changed(*buf, client_request, *client_session,
                                                                         client_procotol, is_in_trans))) {
      LOG_WDIAG("[ObMysqlSM::do_internal_request] fail to build ok res", K_(sm_id), K(ret));
    }
  }

  return ret;
}

int ObMysqlSM::do_internal_request_for_sharding_show_db_version(ObMIOBuffer *buf)
{
  int ret = OB_SUCCESS;

  ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
  ObMysqlClientSession *client_session = get_client_session();
  ObClientSessionInfo &client_info = client_session->get_session_info();
  ObProxyProtocol client_protocol = get_client_session_protocol();
  Ob20ProtocolHeader &ob20_head = client_session->get_session_info().ob20_request_.ob20_header_;
  uint8_t compressed_seq = static_cast<uint8_t>(client_session->get_compressed_seq() + 1);
  Ob20HeaderParam ob20_head_param(client_session->get_cs_id(), ob20_head.request_id_, compressed_seq,
                                  compressed_seq, true, false, false,
                                  client_session->is_client_support_new_extra_info(),
                                  client_session->is_trans_internal_routing(), false);
  ObCmdInfo info(static_cast<uint8_t>(client_request.get_packet_meta().pkt_seq_ + 1),
                 trans_state_.mysql_config_params_->internal_cmd_mem_limited_, client_protocol, ob20_head_param);
  // handle show db version when sharding
  LOG_DEBUG("sharding show db_version");
  ObString logic_tenant_name;
  ObString logic_database_name;
  if (OB_FAIL(client_info.get_logic_tenant_name(logic_tenant_name))) {
    LOG_WDIAG("fail to get_logic_tenant_name", K(ret));
  } else if (OB_FAIL(client_info.get_logic_database_name(logic_database_name))) {
    LOG_WDIAG("fail to get_logic_database_name", K(ret));
    trans_state_.mysql_errcode_ = OB_ERR_NO_DB_SELECTED;
    trans_state_.mysql_errmsg_ = ob_strerror(OB_ERR_NO_DB_SELECTED);
    if (OB_FAIL(ObMysqlTransact::encode_error_message(trans_state_))) {
      LOG_WDIAG("[ObMysqlSM::do_internal_request] fail to build not err resp", K_(sm_id), K(ret));
    }
  } else if (OB_FAIL(ObShowDBVersionHandler::show_db_version_cmd_callback(buf, info, logic_tenant_name,
                                                                          logic_database_name))) {
    LOG_WDIAG("[ObMysqlSM::do_internal_request] fail to handle show db version", K_(sm_id), K(ret));
  }

  return ret;
}

int ObMysqlSM::do_internal_request_for_sharding_show_db(ObMIOBuffer *buf)
{
  int ret = OB_SUCCESS;

  ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
  ObMysqlClientSession *client_session = get_client_session();
  ObProxyProtocol client_protocol = get_client_session_protocol();
  Ob20ProtocolHeader &ob20_head = client_session->get_session_info().ob20_request_.ob20_header_;
  uint8_t compressed_seq = static_cast<uint8_t>(client_session->get_compressed_seq() + 1);
  Ob20HeaderParam ob20_head_param(client_session->get_cs_id(), ob20_head.request_id_, compressed_seq,
                                  compressed_seq, true, false, false,
                                  client_session->is_client_support_new_extra_info(),
                                  client_session->is_trans_internal_routing(), false);
  ObCmdInfo info(static_cast<uint8_t>(client_request.get_packet_meta().pkt_seq_ + 1),
                 trans_state_.mysql_config_params_->internal_cmd_mem_limited_, client_protocol, ob20_head_param);

  // handle show databases when sharding
  LOG_DEBUG("sharding show databases");
  ObString logic_tenant_name;
  ObClientSessionInfo &session_info = client_session_->get_session_info();

  if (OB_FAIL(session_info.get_logic_tenant_name(logic_tenant_name))) {
    LOG_WDIAG("fail to get_logic_tenant_name", K(ret));
  } else if (OB_FAIL(ObShardingShowDatabasesHandler::show_databases_cmd_callback(buf, info, logic_tenant_name, *client_session_))) {
    LOG_WDIAG("[ObMysqlSM::do_internal_request] fail to handle show databases", K_(sm_id), K(ret));
  }

  return ret;
}

int ObMysqlSM::do_internal_request_for_sharding_show_table(ObMIOBuffer *buf)
{
  int ret = OB_SUCCESS;

  ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
  ObMysqlClientSession *client_session = get_client_session();
  ObClientSessionInfo &client_info = client_session->get_session_info();
  ObProxyProtocol client_protocol = get_client_session_protocol();
  Ob20ProtocolHeader &ob20_head = client_session->get_session_info().ob20_request_.ob20_header_;
  uint8_t compressed_seq = static_cast<uint8_t>(client_session->get_compressed_seq() + 1);
  Ob20HeaderParam ob20_head_param(client_session->get_cs_id(), ob20_head.request_id_, compressed_seq,
                                  compressed_seq, true, false, false,
                                  client_session->is_client_support_new_extra_info(),
                                  client_session->is_trans_internal_routing(), false);
  ObCmdInfo info(static_cast<uint8_t>(client_request.get_packet_meta().pkt_seq_ + 1),
                 trans_state_.mysql_config_params_->internal_cmd_mem_limited_, client_protocol, ob20_head_param);
  // handle show tables when sharding
  LOG_DEBUG("sharding show tables");
  ObString logic_tenant_name;
  ObString logic_database_name = client_request.get_parse_result().get_database_name();
  ObString logic_table_name = client_request.get_parse_result().get_table_name();

  if (OB_FAIL(client_info.get_logic_tenant_name(logic_tenant_name))) {
    LOG_WDIAG("fail to get_logic_tenant_name", K(ret));
  } else if (logic_database_name.empty() &&
             OB_FAIL(client_info.get_logic_database_name(logic_database_name))) {
    LOG_WDIAG("fail to get_logic_database_name", K(ret));
    trans_state_.mysql_errcode_ = OB_ERR_NO_DB_SELECTED;
    trans_state_.mysql_errmsg_ = ob_strerror(OB_ERR_NO_DB_SELECTED);
    if (OB_FAIL(ObMysqlTransact::encode_error_message(trans_state_))) {
      LOG_WDIAG("[ObMysqlSM::do_internal_request] fail to build not err resp", K_(sm_id), K(ret));
    }
  } else if (OB_FAIL(ObShardingShowTablesHandler::show_tables_cmd_callback(buf, info, client_request.get_parse_result().get_cmd_sub_type(),
                                                                   logic_tenant_name, logic_database_name, logic_table_name))) {
    LOG_WDIAG("[ObMysqlSM::do_internal_request] fail to handle show databases", K_(sm_id), K(ret));
  }

  return ret;
}

int ObMysqlSM::do_internal_request_for_sharding_show_table_status(ObMIOBuffer *buf)
{
  int ret = OB_SUCCESS;
  ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
  ObMysqlClientSession *client_session = get_client_session();
  ObClientSessionInfo &client_info = client_session->get_session_info();
  ObProxyProtocol client_protocol = get_client_session_protocol();
  Ob20ProtocolHeader &ob20_head = client_session->get_session_info().ob20_request_.ob20_header_;
  uint8_t compressed_seq = static_cast<uint8_t>(client_session->get_compressed_seq() + 1);

  // handle show table status when sharding
  LOG_DEBUG("sharding show table status");
  ObString logic_database_name = client_request.get_parse_result().get_database_name();
  ObString logic_table_name = client_request.get_parse_result().get_table_name();
  ObString logic_tenant_name;

  Ob20HeaderParam ob20_head_param(client_session->get_cs_id(), ob20_head.request_id_, compressed_seq,
                                  compressed_seq, true, false, false,
                                  client_session->is_client_support_new_extra_info(),
                                  client_session->is_trans_internal_routing(), false);
  ObCmdInfo info(static_cast<uint8_t>(client_request.get_packet_meta().pkt_seq_ + 1),
                 trans_state_.mysql_config_params_->internal_cmd_mem_limited_, client_protocol, ob20_head_param);

  if (OB_FAIL(client_info.get_logic_tenant_name(logic_tenant_name))) {
    LOG_WDIAG("fail to get_logic_tenant_name", K(ret));
  } else if (logic_database_name.empty() &&
             OB_FAIL(client_info.get_logic_database_name(logic_database_name))) {
    LOG_WDIAG("fail to get_logic_database_name", K(ret));
    trans_state_.mysql_errcode_ = OB_ERR_NO_DB_SELECTED;
    trans_state_.mysql_errmsg_ = ob_strerror(OB_ERR_NO_DB_SELECTED);
    if (OB_FAIL(ObMysqlTransact::encode_error_message(trans_state_))) {
      LOG_WDIAG("[ObMysqlSM::do_internal_request] fail to build not err resp", K_(sm_id), K(ret));
    }
  } else if (OB_FAIL(ObShardingShowTableStatusHandler::show_table_status_cmd_callback(buf, info,
                     logic_tenant_name, logic_database_name, logic_table_name))) {
    LOG_WDIAG("[ObMysqlSM::do_internal_request] fail to handle show table status", K_(sm_id), K(ret));
  }

  return ret;
}

int ObMysqlSM::do_internal_request_for_sharding_show_create_table(ObMIOBuffer *buf)
{
  int ret = OB_SUCCESS;

  ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
  ObMysqlClientSession *client_session = get_client_session();
  ObClientSessionInfo &client_info = client_session->get_session_info();
  ObProxyProtocol client_protocol = get_client_session_protocol();
  Ob20ProtocolHeader &ob20_head = client_session->get_session_info().ob20_request_.ob20_header_;
  uint8_t compressed_seq = static_cast<uint8_t>(client_session->get_compressed_seq() + 1);

  Ob20HeaderParam ob20_head_param(client_session->get_cs_id(), ob20_head.request_id_, compressed_seq,
                                  compressed_seq, true, false, false,
                                  client_session->is_client_support_new_extra_info(),
                                  client_session->is_trans_internal_routing(), false);
  ObCmdInfo info(static_cast<uint8_t>(client_request.get_packet_meta().pkt_seq_ + 1),
                 trans_state_.mysql_config_params_->internal_cmd_mem_limited_, client_protocol, ob20_head_param);

  // handle show create table when sharding
  ObString logic_tenant_name;
  ObString logic_db_name = client_request.get_parse_result().get_database_name();;
  ObString logic_table_name = client_request.get_parse_result().get_table_name();
  LOG_DEBUG("sharding show create table", K(logic_db_name));

  if (logic_db_name.empty() && OB_FAIL(client_info.get_logic_database_name(logic_db_name))) {
    LOG_WDIAG("fail to get_logic_db_name", K(ret));
  } else if (OB_FAIL(client_info.get_logic_tenant_name(logic_tenant_name))) {
    LOG_WDIAG("fail to get_logic_tenant_name", K(ret));
  } else if (OB_FAIL(ObShardingShowCreateTableHandler::show_create_table_cmd_callback(buf, info,
                     logic_tenant_name, logic_db_name, logic_table_name))) {
    LOG_WDIAG("[ObMysqlSM::do_internal_request] fail to handle show create table", K_(sm_id), K(ret));
  }
  return ret;
}

int ObMysqlSM::do_internal_request_for_sharding_show_elastic_id(ObMIOBuffer *buf)
{
  int ret = OB_SUCCESS;

  ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
  ObMysqlClientSession *client_session = get_client_session();
  ObClientSessionInfo &client_info = client_session->get_session_info();
  ObProxyProtocol client_protocol = get_client_session_protocol();
  Ob20ProtocolHeader &ob20_head = client_session->get_session_info().ob20_request_.ob20_header_;
  uint8_t compressed_seq = static_cast<uint8_t>(client_session->get_compressed_seq() + 1);
  Ob20HeaderParam ob20_head_param(client_session->get_cs_id(), ob20_head.request_id_, compressed_seq,
                                  compressed_seq, true, false, false,
                                  client_session->is_client_support_new_extra_info(),
                                  client_session->is_trans_internal_routing(), false);
  ObCmdInfo info(static_cast<uint8_t>(client_request.get_packet_meta().pkt_seq_ + 1),
                 trans_state_.mysql_config_params_->internal_cmd_mem_limited_, client_protocol, ob20_head_param);

  // handle show elastic id when sharding
  ObInternalCmdInfo &cmd_info = *client_request.cmd_info_;
  ObString logic_db_name = cmd_info.get_like_string();
  ObString logic_tenant_name;
  ObString group_name = cmd_info.get_value_string();
  LOG_DEBUG("sharding show elastic id", K(logic_db_name), K(group_name));
  if (logic_db_name.empty()) {
    if (OB_FAIL(client_info.get_logic_database_name(logic_db_name))) {
      LOG_WDIAG("fail to get_logic_db_name", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(client_info.get_logic_tenant_name(logic_tenant_name))) {
    LOG_WDIAG("fail to get_logic_tenant_name", K(ret));
  } else if (OB_FAIL(ObShowTopologyHandler::show_elastic_id_cmd_callback(buf, info,
                      logic_tenant_name, logic_db_name, group_name))) {
    LOG_WDIAG("[ObMysqlSM::do_internal_request] fail to handle show elastic id", K_(sm_id), K(ret));
  }
  return ret;
}


int ObMysqlSM::do_internal_request_for_sharding_show_topology(ObMIOBuffer *buf)
{
  int ret = OB_SUCCESS;

  ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
  ObMysqlClientSession *client_session = get_client_session();
  ObClientSessionInfo &client_info = client_session->get_session_info();
  ObProxyProtocol client_protocol = get_client_session_protocol();
  Ob20ProtocolHeader &ob20_head = client_session->get_session_info().ob20_request_.ob20_header_;
  uint8_t compressed_seq = static_cast<uint8_t>(client_session->get_compressed_seq() + 1);
  Ob20HeaderParam ob20_head_param(client_session->get_cs_id(), ob20_head.request_id_, compressed_seq,
                                  compressed_seq, true, false, false,
                                  client_session->is_client_support_new_extra_info(),
                                  client_session->is_proxy_enable_trans_internal_routing(),
                                  false);
  ObCmdInfo info(static_cast<uint8_t>(client_request.get_packet_meta().pkt_seq_ + 1),
                 trans_state_.mysql_config_params_->internal_cmd_mem_limited_, client_protocol, ob20_head_param);

  // handle show topology when sharding
  ObString logic_tenant_name;
  ObString logic_db_name = client_request.get_parse_result().get_database_name();
  ObString logic_table_name = client_request.get_parse_result().get_table_name();
  LOG_DEBUG("sharding show topology", K(logic_db_name));
  if (logic_db_name.empty()) {
    if (OB_FAIL(client_info.get_logic_database_name(logic_db_name))) {
      LOG_WDIAG("fail to get_logic_db_name", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(client_info.get_logic_tenant_name(logic_tenant_name))) {
    LOG_WDIAG("fail to get_logic_tenant_name", K(ret));
  } else if (OB_FAIL(ObShowTopologyHandler::show_topology_cmd_callback(buf, info,
                      logic_tenant_name, logic_db_name, logic_table_name))) {
    LOG_WDIAG("[ObMysqlSM::do_internal_request] fail to handle show topology", K_(sm_id), K(ret));
  }
  return ret;
}

int ObMysqlSM::do_internal_request_for_sharding_select_db(ObMIOBuffer *buf)
{
  int ret = OB_SUCCESS;

  ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
  ObMysqlClientSession *client_session = get_client_session();
  ObClientSessionInfo &client_info = client_session->get_session_info();
  ObProxyProtocol client_protocol = get_client_session_protocol();
  Ob20ProtocolHeader &ob20_head = client_session->get_session_info().ob20_request_.ob20_header_;
  uint8_t compressed_seq = static_cast<uint8_t>(client_session->get_compressed_seq() + 1);
  Ob20HeaderParam ob20_head_param(client_session->get_cs_id(), ob20_head.request_id_, compressed_seq,
                                  compressed_seq, true, false, false,
                                  client_session->is_client_support_new_extra_info(),
                                  client_session->is_trans_internal_routing(), false);
  ObCmdInfo info(static_cast<uint8_t>(client_request.get_packet_meta().pkt_seq_ + 1),
                 trans_state_.mysql_config_params_->internal_cmd_mem_limited_, client_protocol, ob20_head_param);

  ObString logic_database_name;
  client_info.get_logic_database_name(logic_database_name);
  if (OB_FAIL(ObSelectDatabaseHandler::select_database_cmd_callback(buf, info, logic_database_name))) {
    LOG_WDIAG("[ObMysqlSM::do_internal_request] fail to handle select database()", K_(sm_id), K(ret));
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
    LOG_WDIAG("[ObMysqlSM::do_internal_request]", K_(sm_id), K_(pending_action));
  } else if (OB_FAIL(client_buffer_reader_->consume(total_len))) {
    LOG_WDIAG("fail to consume all", K_(sm_id), K(ret));
  } else if (OB_FAIL(trans_state_.alloc_internal_buffer(MYSQL_BUFFER_SIZE))) {
    LOG_EDIAG("[ObMysqlSM::do_internal_request] fail to allocate internal buffer,",
              K_(sm_id), K(ret));
  } else {
    buf = trans_state_.internal_buffer_;
    ObProxyProtocol client_protocol = get_client_session_protocol();

    switch (trans_state_.trans_info_.sql_cmd_) {
      case OB_MYSQL_COM_HANDSHAKE: {
        OMPKHandshake handshake;
        if (OB_NOT_NULL(multi_level_config_)
            && multi_level_config_->enable_client_ssl_
            && !client_session_->is_proxy_mysql_client_ &&
             get_global_ssl_config_table_processor().is_ssl_key_info_valid(
                              client_session_->get_vip_cluster_name(),
                              client_session_->get_vip_tenant_name())) {
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
        if (OB_NOT_NULL(multi_level_config_)) {
          ObString server_version = multi_level_config_->mysql_version_;
          handshake.set_server_version(server_version);
        }

        if (trans_state_.mysql_config_params_->enable_proxy_scramble_) {
          if (client_session_->get_scramble_string().empty()
              && OB_FAIL(client_session_->create_scramble())) {
            LOG_WDIAG("fail to create_scramble", K_(sm_id), K(ret));
          } else {
            ObString &scramble = client_session_->get_scramble_string();
            if (OB_FAIL(handshake.set_scramble(scramble.ptr(), scramble.length()))) {
              LOG_WDIAG("fail to set_scramble", K_(sm_id), K(scramble), K(ret));
            }
          }
        }

        if (FAILEDx(ObProxyPacketWriter::write_packet(*buf, *client_session_, client_protocol, handshake))) {
          LOG_WDIAG("fail to build handshake packet", K_(sm_id), K(ret));
        } else {
          ObMySQLCapabilityFlags capability(handshake.get_server_capability());
          client_session_->get_session_info().save_orig_capability_flags(capability);
        }
        #ifdef ERRSIM
        // check ret OB_SUCCESS\ to avoid ret covered
        if (OB_SUCC(ret) && OB_FAIL(OB_E(EventTable::EN_COM_HANDSHAKE_FAIL) OB_SUCCESS)) {
          LOG_INFO("error inject succ", "sql_cmd:", trans_state_.trans_info_.sql_cmd_);
        }
        #endif
        break;
      }
      case OB_MYSQL_COM_LOGIN: {
        if (!client_session_->is_proxysys_tenant() && !client_session_->can_direct_ok()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("[ObMysqlSM::do_internal_request] it should never enter here",
                   K_(sm_id), "cs_id", client_session_->get_cs_id());
        } else {
          LOG_DEBUG("proxysys response login ok packet",
                   K_(sm_id), "cs_id", client_session_->get_cs_id());

          ObMysqlAuthRequest &orig_auth_req = client_session_->get_session_info().get_login_req();
          const ObMySQLCapabilityFlags &capability = client_session_->get_session_info().get_orig_capability_flags();
          uint8_t pkt_seq = static_cast<uint8_t>(orig_auth_req.get_packet_meta().pkt_seq_ + 1);
          if (OB_FAIL(ObProxyPacketWriter::write_ok_packet(*buf, *client_session_, client_protocol,
                                                           pkt_seq, 0, capability))) {
            LOG_WDIAG("fail to write ok packet", K(ret));
          }
        }
        break;
      }
      case OB_MYSQL_COM_PING: {
        LOG_DEBUG("proxy response COM_PING ok packet", K_(sm_id), "cs_id", client_session_->get_cs_id());
        uint8_t pkt_seq = static_cast<uint8_t>(trans_state_.trans_info_.client_request_.get_packet_meta().pkt_seq_ + 1);
        const ObMySQLCapabilityFlags &capability = client_session_->get_session_info().get_orig_capability_flags();
        if (OB_FAIL(ObProxyPacketWriter::write_ok_packet(*buf, *client_session_, client_protocol,
                                                         pkt_seq, 0, capability))) {
          LOG_WDIAG("fail to encode COM_PING response ok packet", K(ret));
        }
        break;
      }
      case OB_MYSQL_COM_QUIT: {
        send_response_direct = false;
        LOG_DEBUG("proxy handle COM_QUIT itself", K_(sm_id), "cs_id", client_session_->get_cs_id());
        // call ObMysqlTransact::handle_internal_request() to handle quit command, disconnect
        call_transact_and_set_next_state(NULL);
        break;
      }

      case OB_MYSQL_COM_STMT_CLOSE: {
        ObClientSessionInfo &client_info = client_session_->get_session_info();
        uint32_t client_ps_id = client_info.get_client_ps_id();
        bool is_over_flow = false;
        int64_t read_avail = 0;
        #ifdef ERRSIM
        if (OB_SUCC(ret) && OB_FAIL(OB_E(EventTable::EN_COM_STMT_CLOSE_STACK_OVERFLOW) OB_SUCCESS)) {
          // try to check_stack_overflow and set over_flow true
          if (OB_FAIL(check_stack_overflow(is_over_flow))) {
            LOG_WDIAG("fail to check stack overflow", K(ret));
          } else {
            is_over_flow = true;
          }
        }
        #endif
        if (client_buffer_reader_ != NULL) {
          read_avail = client_buffer_reader_->read_avail();
          if (read_avail > COM_STMT_CLOSE_REQUEST_BUFFER_WATER_MARK && OB_FAIL(check_stack_overflow(is_over_flow))) {
            LOG_WDIAG("fail to check stack over flow", K(ret));
          }
        }
        if (is_over_flow) {
          ret = OB_ERR_UNEXPECTED;
          trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR; // disconnect directly
          LOG_WDIAG("executing COM_STMT_CLOSE, obproxy going to stack over flow", K(read_avail));
        } else {
          /* 无论是 ps id 还是 cursor id, 都可以直接删除, 有就删除, 没有就算了 */
          client_info.remove_ps_id_entry(client_ps_id);
          client_info.remove_ps_id_addrs(client_ps_id);
          send_response_direct = false;
          LOG_DEBUG("proxy no response COM_STMT_CLOSE", K_(sm_id), "cs_id", client_session_->get_cs_id());
          callout_api_and_start_next_action(ObMysqlTransact::SM_ACTION_API_CMD_COMPLETE);
        }
        break;
      }

      case OB_MYSQL_COM_STMT_RESET: {
        ObClientSessionInfo &client_info = client_session_->get_session_info();
        uint32_t client_ps_id = client_info.get_client_ps_id();

        uint8_t pkt_seq = static_cast<uint8_t>(trans_state_.trans_info_.client_request_.get_packet_meta().pkt_seq_ + 1);
        const ObMySQLCapabilityFlags &capability = client_info.get_orig_capability_flags();
        if (OB_FAIL(ObProxyPacketWriter::write_ok_packet(*buf, *client_session_, client_protocol,
                                                         pkt_seq, 0, capability))) {
          LOG_WDIAG("[ObMysqlSM::do_internal_request] fail to build COM_STMT_RESET response ok packet",
                   K_(sm_id), K(client_ps_id), K(ret));
        }
        LOG_DEBUG("proxy no response COM_STMT_RESET", K_(sm_id), "cs_id", client_session_->get_cs_id());
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
      case OB_MYSQL_COM_STMT_PREPARE_EXECUTE:
      case OB_MYSQL_COM_DAEMON: {
        ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
        ObClientSessionInfo &client_info = client_session_->get_session_info();
        uint8_t next_seq = static_cast<uint8_t>(client_request.get_packet_meta().pkt_seq_ + 1);
        // 1. hold_start_trans
        if (client_request.get_parse_result().need_hold_start_trans()) {
          // hold begin reset hold xa start
          trans_state_.is_hold_start_trans_ = true;
          if (OB_FAIL(ObMysqlResponseBuilder::build_start_trans_resp(*buf, client_request,
                                                                     *client_session_, client_protocol))) {
            LOG_WDIAG("[ObMysqlSM::do_internal_request] fail to build start trans resp", K_(sm_id), K(ret));
          }
        // hold_xa_start
        } else if (client_request.get_parse_result().need_hold_xa_start()) {
          LOG_DEBUG("[ObMysqlSM::do_internal_request] to build xa start resp");
          // hold xa start reset hold begin
          trans_state_.is_hold_xa_start_ = true;
          if (OB_FAIL(ObMysqlResponseBuilder::build_prepare_execute_xa_start_resp(*buf, client_request,
                                                                                  *client_session_, client_protocol))) {
            LOG_WDIAG("[ObMysqlSM::do_internal_request] fail to build xa start resp", K_(sm_id), K(ret));
          }
        // 2. bad_route_request
        } else if (ObMysqlTransact::is_bad_route_request(trans_state_)) {
          trans_state_.mysql_errcode_ = OB_NOT_SUPPORTED;
          trans_state_.mysql_errmsg_ = "Not supported, bad route request";
          if (OB_FAIL(ObMysqlTransact::encode_error_message(trans_state_))) {
            LOG_WDIAG("[ObMysqlSM::do_internal_request] fail to encode err pacekt buf",
                     K_(sm_id), K(next_seq), "errcode", trans_state_.mysql_errcode_,
                     "user_err_msg", trans_state_.mysql_errmsg_, K(ret));
          } else {
            // throw away the former 'begin' or 'start transaction'
            trans_state_.is_hold_start_trans_ = false;
            trans_state_.is_hold_xa_start_ = false;
          }

         // 3. not_supported
        } else if (client_request.get_parse_result().is_not_supported()) {
          trans_state_.mysql_errcode_ = OB_NOT_SUPPORTED;
          if (OB_FAIL(ObMysqlTransact::encode_error_message(trans_state_))) {
            LOG_WDIAG("[ObMysqlSM::do_internal_request] fail to build not supported err resp", K_(sm_id), K(ret));
          }

        // 4. select_tx_ro
        } else if (client_request.get_parse_result().is_select_tx_ro()) {
          // if start trans is hold, we treat this resp is in trans
          bool is_in_trans = (trans_state_.is_hold_xa_start_
                              || trans_state_.is_hold_start_trans_
                              || ObMysqlTransact::is_in_trans(trans_state_));
          if (OB_FAIL(ObMysqlResponseBuilder::build_select_tx_ro_resp(*buf, client_request, *client_session_,
                                                                      client_protocol, is_in_trans))) {
            LOG_WDIAG("[ObMysqlSM::do_internal_request] fail to build select @@tx_read_only", K_(sm_id), K(ret));
          }
        // 6. ping_proxy
        } else if (client_request.get_parse_result().is_ping_proxy_cmd()
                   && !client_request.get_parse_result().is_internal_error_cmd()) {
          if (OB_LIKELY(get_global_hot_upgrade_info().need_conn_accept_)) {
            uint8_t pkt_seq = static_cast<uint8_t>(trans_state_.trans_info_.client_request_.get_packet_meta().pkt_seq_ + 1);
            const ObMySQLCapabilityFlags &capability = client_session_->get_session_info().get_orig_capability_flags();
            if (OB_FAIL(ObProxyPacketWriter::write_ok_packet(*buf, *client_session_, client_protocol,
                                                             pkt_seq, 0, capability))) {
              LOG_WDIAG("fail to write ok packet", K(ret));
            }
          } else {
            ret = OB_SERVER_IS_STOPPING;
            LOG_INFO("proxy had been stop accepting new connection, "
                     "disconnect here for 'ping proxy' cmd", K_(sm_id), "cs_id", client_session_->get_cs_id(), K(ret));
          }

        // 7. error internal cmd
        } else if (OB_LIKELY(NULL != client_request.cmd_info_) && client_request.cmd_info_->is_error_cmd()) {
          LOG_DEBUG("unsupportde internal cmd", K(client_request.get_print_sql()));
          if (client_request.cmd_info_->is_error_cmd_need_resp_ok()) {
            // python mysql will send 'SET autocommit=0' after connected, proxysys need resp ok packet
            uint8_t pkt_seq = static_cast<uint8_t>(trans_state_.trans_info_.client_request_.get_packet_meta().pkt_seq_ + 1);
            const ObMySQLCapabilityFlags &capability = client_session_->get_session_info().get_orig_capability_flags();
            if (OB_FAIL(ObProxyPacketWriter::write_ok_packet(*buf, *client_session_, client_protocol,
                                                             pkt_seq, 0, capability))) {
              LOG_WDIAG("fail to build ok packet", K(ret));
            }
          } else {
            trans_state_.mysql_errcode_ = OB_ERR_OPERATOR_UNKNOWN;
            trans_state_.mysql_errmsg_ = "Unknown operator, bad internal cmd";
            if (OB_FAIL(ObMysqlTransact::encode_error_message(trans_state_))) {
              LOG_WDIAG("[ObMysqlSM::do_internal_request] fail to build not err resp", K_(sm_id), K(ret));
            }
          }

        // 8. select route_addr
        } else if (client_request.get_parse_result().is_select_route_addr()) {
          ObMysqlServerSession *last_session = client_session_->get_server_session();
          if (NULL != last_session && NULL != last_session->get_netvc()) {
            // if start trans is hold, we treat this resp is in trans
            bool is_in_trans = (trans_state_.is_hold_xa_start_
                                || trans_state_.is_hold_start_trans_
                                || ObMysqlTransact::is_in_trans(trans_state_));
            if (OB_FAIL(ObMysqlResponseBuilder::build_select_route_addr_resp(*buf, client_request,
                                                        *client_session_, client_protocol, is_in_trans,
                                                        last_session->get_netvc()->get_remote_addr()))) {
              LOG_WDIAG("[ObMysqlSM::do_internal_request] fail to build select @obproxy_route_addr", K_(sm_id), K(ret));
            }
          } else {
            trans_state_.mysql_errcode_ = OB_SERVER_NOT_ACTIVE;
            trans_state_.mysql_errmsg_ = "last session is invalid";
            if (OB_FAIL(ObMysqlTransact::encode_error_message(trans_state_))) {
              LOG_WDIAG("[ObMysqlSM::do_internal_request] fail to build not err resp", K_(sm_id), K(ret));
            }
          }

        // 9. set route_addr
        } else if (client_request.get_parse_result().is_set_route_addr()) {
          client_info.set_obproxy_route_addr(client_request.get_parse_result().cmd_info_.integer_[0]);
          // if start trans is hold, we treat this resp is in trans
          bool is_in_trans = (trans_state_.is_hold_xa_start_
                              || trans_state_.is_hold_start_trans_
                              || ObMysqlTransact::is_in_trans(trans_state_));
          if (OB_FAIL(ObMysqlResponseBuilder::build_set_route_addr_resp(*buf, client_request, *client_session_,
                                                                        client_protocol, is_in_trans))) {
            LOG_WDIAG("[ObMysqlSM::do_internal_request] fail to build set @obproxy_route_addr", K_(sm_id), K(ret));
          } else {
            LOG_DEBUG("@obproxy_route_addr is set", K(client_info.get_obproxy_route_addr()));
          }
        // 10. set ob_read_consistency
        } else if (client_request.get_parse_result().is_set_ob_read_consistency()) {
          if (client_session_->get_session_info().is_request_follower_user()) {
            trans_state_.mysql_errcode_ = OB_OP_NOT_ALLOW;
            trans_state_.mysql_errmsg_ = "set ob_read_consitency is not allowed";
            if (OB_FAIL(ObMysqlTransact::encode_error_message(trans_state_))) {
              LOG_WDIAG("[ObMysqlSM::do_internal_request] fail to encode err pacekt buf",
                     K_(sm_id), K(next_seq), "errcode", trans_state_.mysql_errcode_,
                     "user_err_msg", trans_state_.mysql_errmsg_, K(ret));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("unexpected error", K(ret));
          }
        // 11. set tx_read_only
        } else if (client_request.get_parse_result().is_set_tx_read_only()) {
          if (client_session_->get_session_info().is_read_only_user()) {
            trans_state_.mysql_errcode_ = OB_OP_NOT_ALLOW;
            trans_state_.mysql_errmsg_ = "set tx_read_only/transaction_read_only is not allowed";
            if (OB_FAIL(ObMysqlTransact::encode_error_message(trans_state_))) {
              LOG_WDIAG("[ObMysqlSM::do_internal_request] fail to encode err pacekt buf",
                     K_(sm_id), K(next_seq), "errcode", trans_state_.mysql_errcode_,
                     "user_err_msg", trans_state_.mysql_errmsg_, K(ret));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("unexpected error", K(ret));
          }

        // 12. handle some special stmt or cmd when sharding
        // only "show databases" do extra privilege check
        // the others do privilege check in check_shard_request
        } else if (client_session_->get_session_info().is_sharding_user()
                   && (OB_MYSQL_COM_INIT_DB == trans_state_.trans_info_.sql_cmd_
                       || client_request.get_parse_result().is_use_db_stmt())) {
          if (OB_FAIL(do_internal_request_for_sharding_init_db(buf))) {
            LOG_WDIAG("fail to do internal request for sharding init db", K_(sm_id), K(ret));
          }
        } else if (client_session_->get_session_info().is_sharding_user()
                   && client_request.get_parse_result().is_show_db_version_stmt()) {
          if (OB_FAIL(do_internal_request_for_sharding_show_db_version(buf))) {
            LOG_WDIAG("fail to do internal request for sharding show db version", K_(sm_id), K(ret));
          }
        } else if (client_session_->get_session_info().is_sharding_user()
                   && client_request.get_parse_result().is_show_databases_stmt()) {
          if (OB_FAIL(do_internal_request_for_sharding_show_db(buf))) {
            LOG_WDIAG("fail to do internal request for sharding show db", K_(sm_id), K(ret));
          }
        } else if (client_session_->get_session_info().is_sharding_user()
                   && (client_request.get_parse_result().is_show_tables_stmt()
                       || client_request.get_parse_result().is_show_full_tables_stmt())) {
          if (OB_FAIL(do_internal_request_for_sharding_show_table(buf))) {
            LOG_WDIAG("fail to do internal request for sharding show db", K_(sm_id), K(ret));
          }
        } else if (client_session_->get_session_info().is_sharding_user()
                   && (client_request.get_parse_result().is_show_table_status_stmt())) {
          if (OB_FAIL(do_internal_request_for_sharding_show_table_status(buf))) {
            LOG_WDIAG("fail to do internal request for sharding show db", K_(sm_id), K(ret));
          }
        } else if (client_session_->get_session_info().is_sharding_user()
                   && client_request.get_parse_result().is_show_create_table_stmt()) {
          if (OB_FAIL(do_internal_request_for_sharding_show_create_table(buf))) {
            LOG_WDIAG("fail to do internal request for sharding show create table", K_(sm_id), K(ret));
          }
        } else if (client_session_->get_session_info().is_sharding_user()
                   && client_request.get_parse_result().is_show_elastic_id_stmt()) {
          if (OB_FAIL(do_internal_request_for_sharding_show_elastic_id(buf))) {
            LOG_WDIAG("fail to do internal request for sharding show db", K_(sm_id), K(ret));
          }
        } else if (client_session_->get_session_info().is_sharding_user()
                   && client_request.get_parse_result().is_show_topology_stmt()) {
          if (OB_FAIL(do_internal_request_for_sharding_show_topology(buf))) {
            LOG_WDIAG("fail to do internal request for sharding show db", K_(sm_id), K(ret));
          }
        } else if (client_session_->get_session_info().is_sharding_user()
                   && client_request.get_parse_result().is_select_database_stmt()) {
          if (OB_FAIL(do_internal_request_for_sharding_select_db(buf))) {
            LOG_WDIAG("fail to do internal request for sharding show db", K_(sm_id), K(ret));
          }

        // 13. select_proxy_version
        } else if (client_request.get_parse_result().is_select_proxy_version()) {
          // if start trans is hold, we treat this resp is in trans
          bool is_in_trans = (trans_state_.is_hold_xa_start_
                              || trans_state_.is_hold_start_trans_
                              || ObMysqlTransact::is_in_trans(trans_state_));
          if (OB_FAIL(ObMysqlResponseBuilder::build_select_proxy_version_resp(*buf, client_request, *client_session_,
                                                                              client_protocol, is_in_trans))) {
            LOG_WDIAG("[ObMysqlSM::do_internal_request] fail to build select proxy_version", K_(sm_id), K(ret));
          }
        // 14. select_proxy_status
        } else if (client_request.get_parse_result().is_select_proxy_status_stmt()) {
          // if start trans is hold, we treat this resp is in trans
          bool is_in_trans = (trans_state_.is_hold_start_trans_ || ObMysqlTransact::is_in_trans(trans_state_));
          if (OB_FAIL(ObMysqlResponseBuilder::build_select_proxy_status_resp(
                  *buf, client_request, client_info, is_in_trans))) {
            LOG_WDIAG("[ObMysqlSM::do_internal_request] fail to build select proxy_status", K_(sm_id), K(ret));
          }
        // 15. drop prepare stmt
        } else if (client_request.get_parse_result().is_text_ps_drop_stmt()) {
          ObString text_ps_name = client_request.get_parse_result().get_text_ps_name();
          uint32_t client_ps_id = client_info.get_client_ps_id();
          uint8_t pkt_seq = static_cast<uint8_t>(trans_state_.trans_info_.client_request_.get_packet_meta().pkt_seq_ + 1);
          const ObMySQLCapabilityFlags &capability = client_info.get_orig_capability_flags();
          if (OB_FAIL(ObProxyPacketWriter::write_ok_packet(*buf, *client_session_, client_protocol,
                                                           pkt_seq, 0, capability))) {
            LOG_WDIAG("fail to write ok packet", K(ret));
          } else {
            client_info.delete_text_ps_name_entry(text_ps_name);
            client_info.remove_ps_id_addrs(client_ps_id);
            LOG_DEBUG("proxy no response text ps drop", K_(sm_id), "cs_id", client_session_->get_cs_id());
          }
        } else if (client_request.get_parse_result().is_show_slave_hosts() ||
                   client_request.get_parse_result().is_show_relaylog_events()) {
          if (OB_FAIL(ObMysqlResponseBuilder::build_empty_resultset(*buf, client_request, *client_session_,
                                                                    client_protocol, is_in_trans_))) {
            LOG_WDIAG("[ObMysqlSM::do_internal_request] fail to build_empty_resultset", K_(sm_id), K(ret));
          }
        } else if (client_request.get_parse_result().is_binlog_str()) {
          if (OB_FAIL(ObMysqlResponseBuilder::build_ok_resp(*buf, client_request, *client_session_,
                                                            client_protocol, is_in_trans_, false))) {
            LOG_WDIAG("[ObMysqlSM::do_internal_request] fail to build_ok_resp", K_(sm_id), K(ret));
          }
        } else {
          send_response_direct = false;
          if (OB_ISNULL(client_request.cmd_info_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("[ObMysqlSM::do_internal_request] cmd_info_ should not be null", K_(sm_id), K(ret));
          } else {
            MYSQL_SM_SET_DEFAULT_HANDLER(&ObMysqlSM::state_execute_internal_cmd);
            ObInternalCmdInfo &cmd_info = *client_request.cmd_info_;
            cmd_info.set_pkt_seq(static_cast<uint8_t>(client_request.get_packet_meta().pkt_seq_ + 1));
            cmd_info.set_capability(client_session_->get_session_info().get_orig_capability_flags());
            cmd_info.session_priv_ = &client_session_->get_session_info().get_priv_info();
            cmd_info.set_memory_limit(trans_state_.mysql_config_params_->internal_cmd_mem_limited_);
            cmd_info.set_internal_user(client_request.is_proxysys_user());

            Ob20ProtocolHeader &ob20_head = client_session_->get_session_info().ob20_request_.ob20_header_;
            uint8_t compressed_seq = static_cast<uint8_t>(client_session_->get_compressed_seq() + 1);
            Ob20HeaderParam ob20_head_param(client_session_->get_cs_id(), ob20_head.request_id_, compressed_seq,
                                                    compressed_seq, true, false, false,
                                                    client_session_->is_client_support_new_extra_info(),
                                                    client_session_->is_trans_internal_routing(),
                                                    false);
            cmd_info.set_protocol(get_client_session_protocol());
            cmd_info.set_ob20_head_param(ob20_head_param);

            ObAction *cmd_handler = NULL;
            if (OB_FAIL(get_global_internal_cmd_processor().execute_cmd(this, cmd_info, buf, cmd_handler))) {
              LOG_WDIAG("[ObMysqlSM::do_internal_request] fail to execute_cmd", K_(sm_id), K(ret));
            } else if (OB_ISNULL(cmd_handler)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WDIAG("cmd_handler can not be NULL", K(cmd_handler), K(ret));
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
          LOG_WDIAG("not supported mysql cmd", K_(sm_id), "sql_cmd", trans_state_.trans_info_.sql_cmd_,
                   "cmd", get_mysql_cmd_str(trans_state_.trans_info_.sql_cmd_));
          trans_state_.mysql_errcode_ = OB_NOT_SUPPORTED;
          if (OB_FAIL(ObMysqlTransact::encode_error_message(trans_state_))) {
            LOG_WDIAG("[ObMysqlSM::do_internal_request] fail to build not supported err resp", K_(sm_id), K(ret));
          }
        } else {
          ret = OB_INNER_STAT_ERROR;
          LOG_EDIAG("[ObMysqlSM::do_internal_request] it should not enter here ERROR", K_(sm_id),
                    "sql", trans_state_.trans_info_.client_request_.get_print_sql(),
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
  if (client_abort == ObMysqlTransact::ABORTED) {
    trans_state_.internal_error_op_for_diagnosis_ = ObMysqlTransact::PROXY_INTERNAL_ERROR_TRANSFER_DISCONNECT;
  }
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
    case VC_EVENT_DETECT_SERVER_DEAD:
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
    LOG_WDIAG("client will abort soon",
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
  } else if (client_session_->get_session_info().get_priv_info().user_name_ != ObProxyTableInfo::DETECT_USERNAME_USER) {
    LOG_WDIAG("client will abort soon",
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
        LOG_WDIAG("failed to cleanup server entry", K_(sm_id), K(ret));
      }
      server_entry_ = NULL;
      LOG_DEBUG("[tunnel_handle_server] reset server_session_", KP(server_session_), KP(this));
      server_session_ = NULL;

      //put curr ss into last
      ObMysqlServerSession *curr_ss = client_session_->get_cur_server_session();
      if (OB_LIKELY(NULL != curr_ss)) {
        --(curr_ss->server_trans_stat_);
        if (OB_FAIL(client_session_->attach_server_session(curr_ss))) {
          LOG_WDIAG("client session failed to attach server session", K_(sm_id), K(ret));
        }
      }
    } else {
      LOG_WDIAG("client_session is null, clear server_entry", "in_tunnel", server_entry_->in_tunnel_,
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
    LOG_EDIAG("invalid internal state",
              K_(server_session), K_(server_entry), K_(client_session),
              K_(sm_id), K(ret));
  } else if (OB_UNLIKELY(!server_entry_->eos_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_EDIAG("invalid internal state, server entry must be eos state",
              K_(server_entry_->eos), K_(sm_id), K(ret));
  } else {
    client_entry_->in_tunnel_ = false;
    server_entry_->in_tunnel_ = false;

    if (server_buffer_reader_->read_avail() > 0) {
      tunnel_.reset();
      // There's data from the server so try to read the response
      if (OB_FAIL(setup_server_response_read())) {
        LOG_WDIAG("failed to setup_server_response_read", K_(sm_id), K(ret));
      }
    } else {
      tunnel_.reset();
      // Server died
      if (OB_FAIL(vc_table_.cleanup_entry(server_entry_))) {
        LOG_WDIAG("failed to cleanup server entry", K_(sm_id), K(ret));
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

  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
    milestones_.do_observer_open_end_ = get_based_hrtime();
    cmd_time_stats_.do_observer_open_time_ += milestone_diff(milestones_.do_observer_open_begin_,
        milestones_.do_observer_open_end_);
  }

  if (enable_record_full_link_trace_info()) {
    trace::ObSpanCtx *ctx = flt_.trace_log_info_.do_observer_open_ctx_;
    if (OB_NOT_NULL(ctx)) {
      // set show trace buffer before flush trace
      if (flt_.control_info_.is_show_trace_enable()) {
        SET_SHOW_TRACE_INFO(&flt_.show_trace_json_info_.curr_sql_json_span_array_);
      }
      LOG_DEBUG("end span ob_proxy_do_observer_open", K(ctx->span_id_));
      SET_TRACE_BUFFER(flt_trace_buffer_, MAX_TRACE_LOG_SIZE);
      FLT_END_SPAN(ctx);
      flt_.trace_log_info_.do_observer_open_ctx_ = NULL;
    }
  }

  // applying per-transaction observer netVC options
  // here IFF they differ from the netVC's current
  // options. This should keep this from being
  // redundant on a server session's first
  // transaction.
  if (OB_LIKELY(NULL != server_session_)) {
    ObNetVConnection *vc = server_session_->get_netvc();
    trans_state_.server_info_.set_obproxy_addr(server_session_->get_netvc()->get_local_addr());

    if (OB_UNLIKELY(NULL != vc &&
        (vc->options_.sockopt_flags_ != trans_state_.mysql_config_params_->sock_option_flag_out_
         || vc->options_.packet_mark_ != trans_state_.mysql_config_params_->sock_packet_mark_out_
         || vc->options_.packet_tos_ != trans_state_.mysql_config_params_->sock_packet_tos_out_))) {
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
        LOG_WDIAG("server session failed to apply per-transaction socket options", K_(sm_id), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {

    ObMysqlTransact::update_sql_cmd(trans_state_);

    switch (trans_state_.current_.send_action_) {
      case ObMysqlTransact::SERVER_SEND_REQUEST:
      {
        bool need = false;
        if (OB_FAIL(need_setup_client_transform_transfer(need))) {
          LOG_WDIAG("fail to check need setup client transfer", K(ret));
        } else if (OB_UNLIKELY(need)) {
          if (OB_FAIL(setup_client_transfer(MYSQL_TRANSFORM_VC))) {
            LOG_WDIAG("failed to setup_client_transfer", K_(sm_id), K(ret));
          }
        } else {
          skip_plugin_ = true;
          callout_api_and_start_next_action(ObMysqlTransact::SM_ACTION_API_SEND_REQUEST);
        }
        break;
      }

      case ObMysqlTransact::SERVER_SEND_HANDSHAKE:
        // normally, we will set query_timeout before we send request,
        // but in SERVER_SEND_HANDSHAK nothing to be sent to observer,
        // observer will send handshake packet to obproxy first,
        // so we must set timeout here
        set_server_query_timeout();
        if (OB_FAIL(setup_server_response_read())) {
          LOG_WDIAG("failed to setup_server_response_read", K_(sm_id), K(ret));
        }
        break;

      case ObMysqlTransact::SERVER_SEND_INIT_SQL:
        //fall through
      case ObMysqlTransact::SERVER_SEND_ALL_SESSION_VARS:
        //fall through
      case ObMysqlTransact::SERVER_SEND_USE_DATABASE:
        //fall through
      case ObMysqlTransact::SERVER_SEND_SESSION_VARS:
        //fall through
      case ObMysqlTransact::SERVER_SEND_SESSION_USER_VARS:
        //fall through
      case ObMysqlTransact::SERVER_SEND_START_TRANS:
      case ObMysqlTransact::SERVER_SEND_XA_START:
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
        LOG_EDIAG("Unexpected send next action type",
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

inline int ObMysqlSM::need_setup_client_transform_transfer(bool &need)
{
  int ret = OB_SUCCESS;
  need = false;
  ObProxyProtocol server_protocol = get_server_session_protocol();

  if (OB_LIKELY(client_session_->get_session_info().is_oceanbase_server())) {
    need = (!trans_state_.is_auth_request_
            && ObMysqlTransact::need_use_tunnel(trans_state_)
            && (ObProxyProtocol::PROTOCOL_CHECKSUM == server_protocol
                || ObProxyProtocol::PROTOCOL_OB20 == server_protocol
                || obmysql::OB_MYSQL_COM_STMT_PREPARE == trans_state_.trans_info_.client_request_.get_packet_meta().cmd_
                || obmysql::OB_MYSQL_COM_STMT_EXECUTE == trans_state_.trans_info_.client_request_.get_packet_meta().cmd_
                || trans_state_.trans_info_.client_request_.get_parse_result().is_text_ps_prepare_stmt()));
    ObVConnection *vc = NULL;
    if (need) {
      if (OB_FAIL(api_.do_request_transform_open(vc))) {
        LOG_WDIAG("do request transform open failed");
      } else if (OB_NOT_NULL(vc)) {
        LOG_DEBUG("[need_setup_client_transform_transfer] will setup client transform transfer", K_(sm_id));
      } else  {
        // vc not opened
        need = false;
      }
    }
  }

  return ret;
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
    LOG_EDIAG("invalid internal state, server entry or data is NULL",
              K_(server_entry), K(data), K_(sm_id), K(ret));
  } else {
    if (OB_MYSQL_COM_QUIT != trans_state_.trans_info_.sql_cmd_) {
      LOG_WDIAG("trace_log", K(trans_state_.trace_log_));
    }
    // If there is request transfer tunnel wait for the tunnel
    // to figure out that things have gone to hell
    if (tunnel_.is_tunnel_active()) {
      LOG_DEBUG("[handle_server_setup_error] "
                "forwarding event to request transfer tunnel",
                K_(sm_id), "event", ObMysqlDebugNames::get_event_name(event));

      if (OB_UNLIKELY(server_entry_->read_vio_ != reinterpret_cast<ObVIO *>(data))) {
        trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
        LOG_EDIAG("invalid internal state, srever entry vio is different with data",
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
            LOG_EDIAG("invalid internal state, client entry vc is different with client producer vc",
                      K_(client_entry_->vc), K_(client_producer->vc), K_(sm_id));
          } else {
            set_client_wait_timeout();
            client_entry_->vc_handler_ = &ObMysqlSM::state_watch_for_client_abort;
            client_entry_->read_vio_ =
                client_producer->vc_->do_io_read(this, INT64_MAX, c->producer_->read_buffer_);
            client_producer->vc_->do_io_shutdown(IO_SHUTDOWN_READ);

            if (OB_ISNULL(client_entry_->read_vio_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WDIAG("client entry failed to do_io_read", K_(sm_id), K(ret));
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
            LOG_WDIAG("vc table failed to cleanup server entry", K_(sm_id), K(ret));
          } else {
            api_.request_transform_info_.entry_ = NULL;
            tunnel_.reset();
          }
        }
      }

      // some sepcial server session with the follwing attributes connot retry
      // and need close client session.
      // - is_lock_session
      // - is_sharding_txn_session_
      bool is_internal_send_process = ObMysqlTransact::is_in_internal_send_process(trans_state_);
      bool need_close_client_session = (OB_NOT_NULL(server_session_)
                                          && (server_session_->get_session_info().is_sharding_txn_session()
                                              || server_session_->get_session_info().is_lock_session()))
                                       || !is_internal_send_process;
      switch (event) {
        case VC_EVENT_EOS: {
          if (OB_UNLIKELY(need_close_client_session)) {
            trans_state_.current_.state_ = ObMysqlTransact::CONNECTION_CLOSED;
          } else {
            trans_state_.current_.state_ = ObMysqlTransact::CONNECT_ERROR;
          }
          break;
        }
        case VC_EVENT_ERROR:
        case VC_EVENT_DETECT_SERVER_DEAD: {
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
      if (!need_close_client_session
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
        LOG_EDIAG("invalid server entry state", K_(sm_id), K_(server_entry_->in_tunnel));
      } else if (OB_FAIL(vc_table_.cleanup_entry(server_entry_, !can_release))) {
        LOG_WDIAG("vc table failed to cleanup server entry", K_(sm_id), K(ret));
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
          LOG_EDIAG("Unknown api state type", K_(sm_id), K_(api_.callout_state));
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
  // use tunnel to transfer content of file request and large request
  if (!ObMysqlTransact::need_use_tunnel(trans_state_) ||
      OB_ISNULL(client_buffer_reader_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid request length",
             K(trans_state_.trans_info_.sql_cmd_),
             K(trans_state_.trans_info_.request_content_length_),
             K_(client_buffer_reader),
             K_(sm_id), K(ret));
  } else if (OB_FAIL(trans_state_.alloc_internal_buffer(MYSQL_BUFFER_SIZE))) {
    LOG_EDIAG("fail to allocate internal buffer,", K_(sm_id), K(ret));
  } else if (OB_FAIL(ObMysqlTransact::rewrite_stmt_id(trans_state_, client_buffer_reader_ ))) {
    LOG_WDIAG("rewrite stmt id failed", K(ret));
  } else {
    // Next order of business if copy the remaining data from the
    // request buffer into new buffer
    if (OB_FAIL(trans_state_.internal_buffer_->remove_append(client_buffer_reader_, written_bytes))) {
      LOG_WDIAG("fail to remove block from client buffer reader to internal buffer",
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
      LOG_WDIAG("failed to add producer", K_(sm_id), K(ret));
    } else {
      client_entry_->in_tunnel_ = true;

      switch (to_vc_type) {
        case MYSQL_TRANSFORM_VC:
          MYSQL_SM_SET_DEFAULT_HANDLER(&ObMysqlSM::state_request_wait_for_transform_read);
          if (OB_ISNULL(api_.request_transform_info_.entry_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("invalid internal state", KP_(api_.request_transform_info_.vc), K_(sm_id), K(ret));
          } else if (api_.request_transform_info_.entry_->vc_ != api_.request_transform_info_.vc_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("invalid internal state",
                     KP_(api_.request_transform_info_.entry),
                     KP_(api_.request_transform_info_.entry_->vc),
                     KP_(api_.request_transform_info_.vc), K_(sm_id), K(ret));
          } else if (OB_ISNULL(c = tunnel_.add_consumer(api_.request_transform_info_.entry_->vc_,
                                                        client_entry_->vc_,
                                                        &ObMysqlSM::tunnel_handler_transform_write,
                                                        MT_TRANSFORM,
                                                        "client transform"))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("failed to add consumer", K_(sm_id), K(ret));
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
            LOG_WDIAG("failed to add consumer", K_(sm_id), K(ret));
          } else {
            server_entry_->in_tunnel_ = true;
          }
          break;

        default:
          ret = OB_INNER_STAT_ERROR;
          LOG_EDIAG("Unknown vconnection type", K(to_vc_type),  K_(sm_id), K(ret));
          break;
      }
    }

    if (OB_SUCC(ret)) {
      request_analyzer_.reset();
      if (OB_FAIL(p->set_request_packet_analyzer(MYSQL_REQUEST, &request_analyzer_,
                                                 // client - mysql -> server: need protocol diagnosis
                                                 MYSQL_SERVER_VC == to_vc_type ? protocol_diagnosis_ : NULL))) {
        LOG_WDIAG("failed to set_producer_packet_analyzer", K(p), K_(sm_id), K(ret));
      } else if (OB_FAIL(tunnel_.tunnel_run(p))) {
        LOG_WDIAG("failed to run tunnel", K(p), K_(sm_id), K(ret));
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
    LOG_EDIAG("invalid internal state, server session and server entry should be NULL,"
              "client session should not be NULL",
              K_(server_session), K_(server_entry), K_(client_session),
              K_(sm_id), K(ret));
    dump_history_state();
  } else if (OB_UNLIKELY(MSS_ACTIVE != s.state_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_EDIAG("invalid server session state", K_(s.state), K_(sm_id), K(ret));
    dump_history_state();
  } else {
    if (OB_UNLIKELY(trans_state_.server_info_.addr_ != s.server_ip_)) {
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

    if (OB_UNLIKELY(client_session_->is_session_pool_client() && client_session_->is_proxy_mysql_client_)) {
      if (server_session_->get_session_info().get_server_ob_capability() == 0) {
        uint64_t ob_server_cap = client_session_->get_session_info().get_server_ob_capability();
        server_session_->get_session_info().set_server_ob_capability(ob_server_cap);
        LOG_DEBUG("set server session capability", K(ob_server_cap));
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
      LOG_WDIAG("server entry failed to do_io_read", K_(sm_id), K(ret));
    } else {
      // Transfer control of the write side as well
      server_entry_->write_vio_ = server_session_->do_io_write(this, 0, NULL);
      if (OB_ISNULL(server_entry_->write_vio_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("server entry failed to do_io_write", K_(sm_id), K(ret));
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
    LOG_EDIAG("invalid internal state, server session, or server entry or client session is NULL",
              K_(server_session), K_(server_entry), K_(client_session),
              K_(sm_id), K(ret));
    dump_history_state();
  } else if (OB_UNLIKELY(server_entry_->vc_ != server_session_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_EDIAG("server_entry_vc is different with server session",
              K_(server_entry_->vc), K_(server_session), K_(sm_id), K(ret));
    dump_history_state();
  } else if (OB_UNLIKELY(ObMysqlTransact::SERVER_SEND_NONE == trans_state_.current_.send_action_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_EDIAG("invalid send_action", "send_action", trans_state_.current_.send_action_, K_(sm_id), K(ret));
    dump_history_state();
  } else {
    LOG_DEBUG("[ObMysqlSM::setup_server_request_send] send request to observer", K_(sm_id),
              K(trans_state_.current_.send_action_));

    // Send the request header
    server_entry_->vc_handler_ = &ObMysqlSM::state_server_request_send;

    ObIOBufferReader *buf_start = NULL;
    int64_t request_len = 0;

    int64_t build_server_request_begin = 0;
    if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
      build_server_request_begin = get_based_hrtime();
    }

    if (OB_FAIL(ObMysqlTransact::build_server_request(trans_state_, buf_start, request_len))) {
      LOG_WDIAG("failed to build server request", K(buf_start), K(ret));
    } else if (OB_ISNULL(buf_start) || OB_UNLIKELY(request_len <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("invalid request buf", K(buf_start), K(request_len), K(ret));
    } else {
      if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
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
      }

      if (enable_record_full_link_trace_info()) {
        if (flt_.trace_log_info_.server_request_write_ctx_ == NULL) {
          SET_TRACE_BUFFER(flt_trace_buffer_, MAX_TRACE_LOG_SIZE);
          trace::ObSpanCtx *ctx = FLT_BEGIN_SPAN(ob_proxy_server_request_write);
          if (OB_NOT_NULL(ctx)) {
            flt_.trace_log_info_.server_request_write_ctx_ = ctx;
            LOG_DEBUG("begin span ob_proxy_server_request_write", K(ctx->span_id_));
          }
        }
      }
      LOG_DEBUG("build server request finish", K(flt_.span_info_));

      // if write_buffer_ is not NULL, when clean server entry, will free the write_buffer_.
      // so the mio_buffer we alloc in build_server_request will free evently.
      server_entry_->write_buffer_ = (buf_start != client_buffer_reader_) ? buf_start->writer() : NULL;
      // oracle模式下，begin是匿名块开启，所以设置过has_set_anonymous_block，则认为在pl中
      ObClientSessionInfo &session_info = client_session_->get_session_info();
      if (session_info.is_oracle_mode() && trans_state_.trans_info_.client_request_.get_parse_result().has_ever_set_anonymous_block()) {
        trans_state_.trans_info_.client_request_.get_parse_result().set_anonymous_block(true);
      }
      // set query_timeout to each request, ddl stmt will never timeout
      if (OB_UNLIKELY(trans_state_.trans_info_.client_request_.get_parse_result().is_ddl_stmt()
                      || ObMysqlTransact::is_binlog_request(trans_state_))) {
        cancel_server_query_timeout();
      // 如果是存储过程调用：
      //  1. 如果是在事务中, 用事务超时时间.
      //  2. 如果在事务外, 不设置超时时间
      // 注意:
      //  1 如果发送了 begin/start, 认为处于事务中
      //  1 如果是 autocommit = 0, 第一条语句认为不在事务中
      } else if (OB_UNLIKELY(trans_state_.trans_info_.client_request_.get_parse_result().is_call_stmt()
                 || trans_state_.trans_info_.client_request_.get_parse_result().is_text_ps_call_stmt()
                 || trans_state_.trans_info_.client_request_.get_parse_result().has_anonymous_block())) {
        if (trans_state_.is_hold_xa_start_
            || trans_state_.is_hold_start_trans_
            || ObMysqlTransact::is_in_trans(trans_state_)) {
          set_server_trx_timeout();
        } else {
          cancel_server_query_timeout();
        }
      } else {
        set_server_query_timeout();
      }

      if (OB_UNLIKELY(!trans_state_.is_auth_request_ &&
          ObMysqlTransact::SERVER_SEND_REQUEST == trans_state_.current_.send_action_ &&
          ObMysqlTransact::need_use_tunnel(trans_state_))) {
        if (api_.is_request_transform_opened()) { // vc_ was inited in `need_setup_client_transform_transfer()`
          if (OB_FAIL(api_.setup_transform_to_server_transfer())) {
            LOG_WDIAG("failed to setup_transform_to_server_transfer", K_(sm_id), K(ret));
          }
        } else {
          if (OB_FAIL(setup_client_transfer(MYSQL_SERVER_VC))) {
            LOG_WDIAG("failed to setup_client_transfer", K_(sm_id), K(ret));
          }
        }
      } else {
        server_entry_->write_vio_ = server_entry_->vc_->do_io_write(this, request_len, buf_start);
        if (OB_ISNULL(server_entry_->write_vio_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("server entry failed to do_io_write", K_(sm_id), K(ret));
        }
      }
    }
  }

  return ret;
}

int ObMysqlSM::setup_server_response_read()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(protocol_diagnosis_ != NULL)) {
    if (OB_UNLIKELY(get_global_proxy_config().protocol_diagnosis_level == 2)) {
      LOG_INFO("protocol diagnosis (send request)", "packet_records", *protocol_diagnosis_);
    }
  }

  if (OB_ISNULL(server_session_) || OB_ISNULL(server_entry_)
      || OB_ISNULL(client_session_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_EDIAG("invalid internal state, server session, or server entry or cleint session is NULL",
              K_(server_session), K_(server_entry), K_(client_session),
              K_(sm_id), K(ret));
  } else if (OB_ISNULL(server_buffer_reader_)) {
    // We should have set the server_buffer_reader
    // we sent the request header
    ret = OB_INNER_STAT_ERROR;
    LOG_EDIAG("server_buffer_reader should not be NULL", K_(sm_id), K(ret));
  } else if (OB_ISNULL(server_entry_->read_vio_)) {
    // We already done the READ when we setup the connection to
    // read the request, attach_server_session()
    ret = OB_INNER_STAT_ERROR;
    LOG_EDIAG("server entry read vio should not be NULL", K_(sm_id), K(ret));
  } else {
    // Now that we've got the ability to read from the
    // server, setup to read the response header
    server_entry_->vc_handler_ = &ObMysqlSM::state_server_response_read;

    trans_state_.current_.state_ = ObMysqlTransact::STATE_UNDEFINED;
    trans_state_.server_info_.state_ = ObMysqlTransact::STATE_UNDEFINED;

    cmd_size_stats_.server_response_bytes_ = 0;
    milestones_.server_.server_read_end_ = 0;

    LOG_DEBUG("setup server response read begin.");

    // full link trace
    if (enable_record_full_link_trace_info()) {
      SET_TRACE_BUFFER(flt_trace_buffer_, MAX_TRACE_LOG_SIZE);
      if (flt_.trace_log_info_.server_response_read_ctx_ == NULL) {
        trace::ObSpanCtx *server_response_read_ctx = FLT_BEGIN_SPAN(ob_proxy_server_response_read);
        if (OB_NOT_NULL(server_response_read_ctx)) {
          flt_.trace_log_info_.server_response_read_ctx_ = server_response_read_ctx;
          LOG_DEBUG("begin span ob_proxy_server_response_read", K(server_response_read_ctx->span_id_));
        }
      }

      if (flt_.trace_log_info_.server_process_req_ctx_ == NULL) {
        trace::ObSpanCtx *server_process_req_ctx = FLT_BEGIN_SPAN(ob_proxy_server_process_req);
        if (OB_NOT_NULL(server_process_req_ctx)) {
          flt_.trace_log_info_.server_process_req_ctx_ = server_process_req_ctx;
          LOG_DEBUG("begin span ob_proxy_server_process_req", K(server_process_req_ctx->span_id_));
        }
      }
    }

    // The tunnel from observer to client is now setup. Ready to read the response
    if (OB_UNLIKELY(NULL != trans_state_.cache_block_)) {
      // use the cached block to read server response
      server_session_->read_buffer_->append_block_internal(trans_state_.cache_block_);
      trans_state_.cache_block_ = NULL;
    }
    server_entry_->read_vio_ = server_session_->do_io_read(
        this, INT64_MAX, server_session_->read_buffer_);
    if (OB_ISNULL(server_entry_->read_vio_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("server session failed to do_io_read", K_(sm_id), K(ret));
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
    LOG_WDIAG("tunnel is alive, just disconnect", K_(sm_id));
    terminate_sm_ = true;
    trans_state_.source_ = ObMysqlTransact::SOURCE_INTERNAL;
  } else {
    //need clear server entry once arrive here
    if (NULL != server_entry_) {
      server_entry_->in_tunnel_ = false;
      clear_server_entry();
    }
    bool enable_keep_conn = ObConnectionDiagnosisTrace::is_enable_keep_connection(get_global_proxy_config().connection_diagnosis_option);
    ObMySQLCmd sql_cmd = trans_state_.trans_info_.sql_cmd_;

    if (OB_UNLIKELY(enable_keep_conn)) {
      // try to keep connection
      ObSqlParseResult &parse_result = trans_state_.trans_info_.client_request_.get_parse_result();
      bool is_client_vc_related_err = false;
      ObUnixNetVConnection* vc = NULL;

      if (client_session_ != NULL) {
        vc = static_cast<ObUnixNetVConnection *>(client_session_->get_netvc());
        if (vc != NULL) {
          switch (vc->event_record_)
          {
            case VC_EVENT_EOS:
            case VC_EVENT_ERROR:
            case VC_EVENT_ACTIVE_TIMEOUT:
            case VC_EVENT_INACTIVITY_TIMEOUT:
            case VC_EVENT_DETECT_SERVER_DEAD:
              is_client_vc_related_err = true;
              break;
          }
        }

        // these cases don't try to build error packet for diagnosis
        // if internal buffer is empty, will disconnect directly
        // if INTERNAL_ERROR == current_.state_, will disconnect in internal_transfer
        // vc->closed_ = -1 means connection closed by do_io_close, 1 means connection call back cont is null, 0 means not closed
        if (ObMysqlTransact::is_trans_specified(trans_state_) ||
            ObMysqlTransact::is_in_trans(trans_state_) ||
            OB_MYSQL_COM_QUERY != sql_cmd ||
            parse_result.is_text_ps_prepare_stmt() ||
            client_session_->is_proxy_mysql_client_ ||
            client_entry_ == NULL ||
            vc == NULL || is_client_vc_related_err ||
            (vc != NULL && vc->closed_ != 0) ||
            (connection_diagnosis_trace_ != NULL && connection_diagnosis_trace_->trace_type_ != OB_PROXY_INTERNAL_TRACE)) {
          trans_state_.internal_error_op_for_diagnosis_ = ObMysqlTransact::PROXY_INTERNAL_ERROR_TRANSFER_DISCONNECT;
        }
      } else {
        trans_state_.internal_error_op_for_diagnosis_ = ObMysqlTransact::PROXY_INTERNAL_ERROR_TRANSFER_DISCONNECT;
      }
    } else {
      // original error transfer process
      trans_state_.internal_error_op_for_diagnosis_ = ObMysqlTransact::PROXY_INTERNAL_ERROR_TRANSFER_DISCONNECT;
    }

    switch (trans_state_.internal_error_op_for_diagnosis_) {
      case ObMysqlTransact::PROXY_INTERNAL_ERROR_TRANSFER_WITH_DIAGNOSIS:
        LOG_INFO("setup_error_transfer, try to build error packet for diagnosis", K(sql_cmd), "internal_state", trans_state_.current_.state_);
        handle_obproxy_internal_error();
        break;
      case ObMysqlTransact::PROXY_INTERNAL_ERROR_TRANSFER_DISCONNECT:
        handle_obproxy_error_transfer();
        LOG_INFO("setup_error_transfer, transfer internal buffer and try to disconnect", K(sql_cmd), "internal_state", trans_state_.current_.state_);
        break;
      default:
        LOG_WDIAG("unexpected internal error operation, transfer internal buffer", K(sql_cmd), "internal_state", trans_state_.current_.state_);
        handle_obproxy_error_transfer();
        break;
    }
  }
}

int ObMysqlSM::build_error_packet_for_connection_diagnosis(bool &is_packet_build)
{
  int ret = OB_SUCCESS;
  is_packet_build = false;
    // internal buffer empty
  if (NULL == trans_state_.internal_buffer_ ||
      (trans_state_.internal_reader_ != NULL &&
      trans_state_.internal_reader_->read_avail() <= 0)) {
    // consume user request
    if (OB_NOT_NULL(client_buffer_reader_) && OB_FAIL(client_buffer_reader_->consume_all())) {
      LOG_WDIAG("fail to consume request", K_(sm_id), K(ret));
    } else {
      int error_code = 0;
      const char *error_msg = NULL;
      if (connection_diagnosis_trace_ != NULL && connection_diagnosis_trace_->diagnosis_info_ != NULL && connection_diagnosis_trace_->diagnosis_info_->error_code_ < 0 ) {
        error_code = static_cast<int>(connection_diagnosis_trace_->diagnosis_info_->error_code_);
        if (STRLEN(connection_diagnosis_trace_->diagnosis_info_->error_msg_) > 0) {
          error_msg = connection_diagnosis_trace_->diagnosis_info_->error_msg_;
        } else {
          error_msg = ob_strerror(error_code);
        }
      }
      if (error_code == 0 || error_code == OB_PROXY_INTERNAL_ERROR || error_msg == NULL || STRLEN(error_msg) <= 0) {
        // do nothing
      } else if (FALSE_IT(trans_state_.mysql_errcode_ = error_code)) {
      } else if (FALSE_IT(trans_state_.mysql_errmsg_ = error_msg)) {
      } else if (OB_FAIL(ObMysqlTransact::encode_error_message(trans_state_))) {
        LOG_WDIAG("fail to encode error message, disconnect directly", K(error_code), K(ret));
      } else {
        is_packet_build = true;
      }
    }
  }
  return ret;
}

// handle internal error, encode error packet and not disconnection
void ObMysqlSM::handle_obproxy_internal_error()
{
  int ret = OB_SUCCESS;
  bool is_packet_build = false;
  if (OB_FAIL(build_error_packet_for_connection_diagnosis(is_packet_build))) {
    LOG_WDIAG("fail to build error packet", K(ret));
    handle_disconnect_directly();
  } else if ((NULL != trans_state_.internal_buffer_)
    && (NULL != trans_state_.internal_reader_)
    && (trans_state_.internal_reader_->read_avail() > 0)) {
    // transfer error packet
    trans_state_.source_ = ObMysqlTransact::SOURCE_INTERNAL;
    if (is_packet_build) {
      // disconnect in setup_internal_transfer
      trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
    }
    trans_state_.next_action_ = ObMysqlTransact::SM_ACTION_INTERNAL_NOOP;
    callout_api_and_start_next_action(ObMysqlTransact::SM_ACTION_API_SEND_RESPONSE);
  } else {
    LOG_WDIAG("unexpected internal buffer state, empty internal packet");
    handle_disconnect_directly();
  }
}

// transfer internal buffer to client
void ObMysqlSM::handle_obproxy_error_transfer()
{
  if ((NULL != trans_state_.internal_buffer_)
      && (NULL != trans_state_.internal_reader_)
      && (trans_state_.internal_reader_->read_avail() > 0)) {
    // Since we need to send the error message, call the API function
    if (trans_state_.current_.state_ == ObMysqlTransact::INTERNAL_ERROR) {
      LOG_INFO("transferring internal packet and disconnect", "current:", trans_state_.current_.state_);
    }
    callout_api_and_start_next_action(ObMysqlTransact::SM_ACTION_API_SEND_RESPONSE);
  } else {
    if (OB_LIKELY(NULL != client_entry_)) {
      client_entry_->in_tunnel_ = false;
    } else {
      LOG_INFO("[setup_error_transfer] client_entry_ is NULL, no need cleanup", K_(sm_id));
    }

    trans_state_.source_ = ObMysqlTransact::SOURCE_INTERNAL;

    ObMySQLCmd request_cmd = trans_state_.trans_info_.client_request_.get_packet_meta().cmd_;
    if (OB_MYSQL_COM_QUIT == request_cmd || (NULL != client_session_ && client_session_->is_proxy_mysql_client_)) {
      LOG_INFO("[setup_error_transfer] Now closing connection caused by COM_QUIT", K_(sm_id),
                "request_cmd", get_mysql_cmd_str(request_cmd),
                "sql_cmd", get_mysql_cmd_str(trans_state_.trans_info_.sql_cmd_),
                "sql", trans_state_.trans_info_.get_print_sql());
      terminate_sm_ = true;
    } else {
      if (OB_NOT_NULL(client_session_)) {
        client_session_->can_server_session_release_ = false;
      }
      LOG_WDIAG("[setup_error_transfer] Now closing connection", K_(sm_id),
          "client_ip", trans_state_.client_info_.addr_,
          "request_cmd", get_mysql_cmd_str(request_cmd),
          "sql_cmd", get_mysql_cmd_str(trans_state_.trans_info_.sql_cmd_),
          "sql", trans_state_.trans_info_.get_print_sql());
      terminate_sm_ = true;
    }
  }
}

void ObMysqlSM::handle_disconnect_directly()
{
  if (OB_LIKELY(NULL != client_entry_)) {
    client_entry_->in_tunnel_ = false;
  } else {
    LOG_INFO("[setup_error_transfer] client_entry_ is NULL, no need cleanup", K_(sm_id));
  }
  trans_state_.source_ = ObMysqlTransact::SOURCE_INTERNAL;
  terminate_sm_ = true;
}

int ObMysqlSM::setup_internal_transfer(MysqlSMHandler handler_arg)
{
  int ret = OB_SUCCESS;
  ObMysqlTunnelProducer *p = NULL;
  ObMysqlTunnelConsumer *c = NULL;

  if (OB_ISNULL(trans_state_.internal_buffer_) || OB_ISNULL(trans_state_.internal_reader_)) {
    ret = OB_ERR_SYS;
    LOG_WDIAG("invalid internal state, internal msg buffer or internal msg reaer are NULL",
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
      LOG_WDIAG("failed to add producer", K_(sm_id), K(ret));
    } else if (OB_ISNULL(c = tunnel_.add_consumer(client_entry_->vc_,
                                                  MYSQL_TUNNEL_STATIC_PRODUCER,
                                                  &ObMysqlSM::tunnel_handler_client,
                                                  MT_MYSQL_CLIENT,
                                                  "client"))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("failed to add consumer", K_(sm_id), K(ret));
    } else {
      client_entry_->in_tunnel_ = true;

      if (OB_FAIL(p->set_response_packet_analyzer(0, MYSQL_RESPONSE, NULL, NULL))) {
        LOG_WDIAG("failed to set_producer_packet_analyzer", K(p), K_(sm_id), K(ret));
      } else if (OB_FAIL(tunnel_.tunnel_run(p))) {
        LOG_WDIAG("failed to run tunnel", K(p), K_(sm_id), K(ret));
      } else {
        if (ObMysqlTransact::INTERNAL_ERROR == trans_state_.current_.state_) {
          // when we need both send err packet to client and disconnect, will reach here;
          if (NULL != client_session_ && client_session_->get_session_info().get_priv_info().user_name_ != ObProxyTableInfo::DETECT_USERNAME_USER) {
            LOG_INFO("INTERNAL_ERROR, will disconnect", K_(sm_id));
          }
        } else {
          // if the internal request is not the first request and not proxysys,
          // it means that it is in trans (or hold trans), in this case we do NOT change the trans_state_
          if (trans_state_.is_trans_first_request_) {
            if (trans_state_.is_auth_request_
                || trans_state_.is_hold_start_trans_
                || trans_state_.is_hold_xa_start_) {
              trans_state_.current_.state_ = ObMysqlTransact::CMD_COMPLETE;
            } else {
              // proxysys && !COM_LOGIN &&!COM_HANDSHAKE will also enter here
              trans_state_.current_.state_ = ObMysqlTransact::TRANSACTION_COMPLETE;
            }
          } else if ((client_session_->is_proxysys_tenant()
                      || (trans_state_.is_auth_request_ && client_session_->can_direct_ok()))
                     && OB_MYSQL_COM_LOGIN == trans_state_.trans_info_.sql_cmd_) {
            // proxysys && response COM_LOGIN ok packet will enter here, we need set state_ TRANSACTION_COMPLETE
            trans_state_.current_.state_ = ObMysqlTransact::TRANSACTION_COMPLETE;
          } else if (ObMysqlTransact::CMD_COMPLETE != trans_state_.current_.state_) {
            LOG_WDIAG("unexpected current state, expected_state=CMD_COMPLETE",
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
    LOG_EDIAG("invalid internal state", K_(server_buffer_reader));
  } else {
    ObRespAnalyzeResult &resp_result = trans_state_.trans_info_.resp_result_;
    if (server_entry_->eos_ || resp_result.is_resp_completed()) {
      // The server has shutdown on us already so the only data
      // we'll get is already in the buffer
      nbytes = server_buffer_reader_->read_avail();
      LOG_DEBUG("server_transfer_init",
                K_(sm_id), "is_resp_completed", resp_result.is_resp_completed());
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
      LOG_WDIAG("fail to remove block from server buffer reader to buf",
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
    LOG_EDIAG("fail to allocate internal buffer,", K_(sm_id), K(ret));
  } else if (OB_FAIL(server_transfer_init(trans_state_.internal_buffer_, nbytes))) {
    LOG_WDIAG("failed to init server transfer", K_(sm_id), K(ret));
  } else if (OB_ISNULL(p = tunnel_.add_producer(server_entry_->vc_,
                                                nbytes,
                                                trans_state_.internal_reader_,
                                                &ObMysqlSM::tunnel_handler_server,
                                                MT_MYSQL_SERVER,
                                                "observer", false))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("failed to add producer", K_(sm_id), K(p));
  } else if (OB_ISNULL(c = tunnel_.add_consumer(client_entry_->vc_,
                                                server_entry_->vc_,
                                                &ObMysqlSM::tunnel_handler_client,
                                                MT_MYSQL_CLIENT,
                                                "client"))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("failed to add consumer", K_(sm_id), K(c));
  } else {
    MYSQL_SM_SET_DEFAULT_HANDLER(&ObMysqlSM::tunnel_handler_response_transfered);

    client_entry_->in_tunnel_ = true;
    server_entry_->in_tunnel_ = true;

    ObRespAnalyzeResult &resp_result = trans_state_.trans_info_.resp_result_;
    ObRespAnalyzer *resp_analyzer = NULL;
    bool is_resultset = resp_result.is_resultset_resp();
    ObMySQLCmd req_cmd = trans_state_.trans_info_.sql_cmd_;
    if (OB_UNLIKELY((ObProxyProtocol::PROTOCOL_CHECKSUM == get_server_session_protocol())
        && (NULL != client_session_)
        // inner sql's compressed response has tranfer to normal mysql packet
        && (!client_session_->is_proxy_mysql_client_))) {
      if (is_resultset) {
        LOG_EDIAG("compress protocol's never reach here", K(is_resultset));
      }
    } else if ((OB_MYSQL_COM_REGISTER_SLAVE == req_cmd
                || OB_MYSQL_COM_BINLOG_DUMP == req_cmd
                || OB_MYSQL_COM_BINLOG_DUMP_GTID == req_cmd)
               && OB_NOT_NULL(multi_level_config_)
               && !multi_level_config_->binlog_service_ip_.is_empty()) {
      resp_analyzer = &resp_analyzer_;
    } else {
      resp_analyzer = is_resultset ? &resp_analyzer_ : NULL;
    }

    if (resp_analyzer != NULL) {
      // resp_analyzer has been inited in handle_first_normal_response_packet() and analyze_response() be called
      // analyze_response() will changes mysql packets related data in resp_analyzer
      // so, to reset the changed mysql related data to analyze response again in the tunnel
      resp_analyzer->reset_for_mysql_tunnel();
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(p->set_response_packet_analyzer(0, MYSQL_RESPONSE, resp_analyzer, &resp_result))) {
        LOG_WDIAG("failed to set_producer_packet_analyzer", K(p), K_(sm_id), K(ret));
      } else if (OB_FAIL(tunnel_.tunnel_run(p))) {
        LOG_WDIAG("failed to run tunnel", K(p), K_(sm_id), K(ret));
      }
    }
  }

  return ret;
}

int ObMysqlSM::setup_cmd_complete()
{
  int ret = OB_SUCCESS;
  // DEBUG 该日志表示 SQL 执行结束
  LOG_DEBUG("handle sql finished, setup cmd complete");
  // make it easy to check protocol:
  // grep "protocol diagnosis" obproxy.log | sed 's/#/\n/g'
  if (OB_UNLIKELY(protocol_diagnosis_ != NULL) &&
      OB_UNLIKELY(get_global_proxy_config().protocol_diagnosis_level == 2)) {
    LOG_INFO("protocol diagnosis (recv response)", "packet_records", *protocol_diagnosis_);
  }
  if (OB_ISNULL(client_session_) || OB_ISNULL(client_entry_) || OB_ISNULL(client_buffer_reader_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_EDIAG("invliad internal state, client session, or client entry, or client buffer reader is NULL",
              K_(client_session), K_(client_entry),
              K_(client_buffer_reader), K_(sm_id), K(ret));
  } else if (OB_UNLIKELY(client_entry_->vc_ != client_session_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_EDIAG("invliad internal state, client entry vc is different with client session",
              K_(client_session), K_(client_entry_->vc), K_(sm_id), K(ret));
  /* 做一层防御, 在请求结束时就及时发现, 避免等到下一次 close 才会发现, 中间已经出现 session 错乱 */
  } else if (OB_UNLIKELY(NULL != client_session_->get_last_bound_server_session())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_EDIAG("invliad internal state, last bound server session is not null",
              K_(client_session), "last bound server session",
              reinterpret_cast<const void*>(client_session_->get_last_bound_server_session()),
              K_(sm_id), K(ret));
  } else {
    ObMysqlTransact::record_trans_state(trans_state_, ObMysqlTransact::is_in_trans(trans_state_));
    set_detect_server_info(trans_state_.server_info_.addr_, -1, hrtime_to_nsec(ObThread::cur_time_));
    if (OB_UNLIKELY(add_detect_server_cnt_)) {
      LOG_EDIAG("setup_cmd_complete, add_detect_server_cnt_ should be false");
      add_detect_server_cnt_ = false;
      // abort();
    }
    tunnel_.reset();
    client_entry_->in_tunnel_ = false;
    api_.reset();
    // reset client read buffer water mark
    client_buffer_reader_->mbuf_->water_mark_ = MYSQL_NET_META_LENGTH;

    if (client_session_->is_already_send_trace_info()) {
      client_session_->set_need_send_trace_info(false);
    }
    client_session_->set_first_handle_request(true);
    client_session_->set_in_trans_for_close_request(false);
    client_session_->set_sharding_select_log_plan(NULL);
    client_session_->set_need_return_last_bound_ss(false);
    client_session_->set_request_transferring(false);

    if (OB_MYSQL_COM_HANDSHAKE == trans_state_.trans_info_.sql_cmd_) {
      // set inactivity timeout to connect_timeout after proxy send handshake
      // will be canceled when the login packet arrived
      set_client_connect_timeout();
    }

    // sharding request on read shard connector may return TRANSACTION_COMPLETE
    // we need treat it as CMD_COMPLETE
    if (ObMysqlTransact::TRANSACTION_COMPLETE == trans_state_.current_.state_
        && OB_NOT_NULL(client_session_->get_session_info().get_txn_shard_connector())
        && OB_NOT_NULL(client_session_->get_session_info().get_shard_connector())
        && *(client_session_->get_session_info().get_shard_connector())
           != *(client_session_->get_session_info().get_txn_shard_connector())) {
      trans_state_.current_.state_ = ObMysqlTransact::CMD_COMPLETE;
    }

    // stat reset
    if (ObMysqlTransact::TRANSACTION_COMPLETE == trans_state_.current_.state_) {
      update_stats();
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
      if (OB_NOT_NULL(route_diagnosis_)) {
        // complete transaction diagnosis
        route_diagnosis_->trans_diagnosis_completed();
      }
      client_session_->get_session_info().set_txn_shard_connector(NULL);
      client_session_->get_sharding_txn_ss_addr().reset();
    } else {
      is_in_trans_ = true;
      if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
        update_cmd_stats();
        milestones_.cmd_reset();
      }
      LOG_DEBUG("still in transaction, wait next request", K_(sm_id));
      if (OB_NOT_NULL(route_diagnosis_)) {
        if (trans_state_.is_trans_first_request_) {
          // complete transaction's first sql diagnosis
          route_diagnosis_->trans_first_query_diagnosed(trans_state_.trans_info_.client_request_);
          LOG_DEBUG("transaction first sql has been diagnosed");
        } else {
          // complete transaction's current sql diagnosis
          route_diagnosis_->trans_cur_query_diagnosed();
          LOG_DEBUG("transaction cur sql has been diagnosed (clear cur sql diagnosis info)");
        }
      }
    }

    if (OB_UNLIKELY(client_session_->get_session_info().need_close_last_server_session())) {
      if (OB_NOT_NULL(client_session_->get_server_session())
         && !client_session_->get_server_session()->get_session_info().is_sharding_txn_session()) {
        client_session_->close_last_used_ss();
      }
      clear_server_entry();
    }
    // end client response write span after cmd complete
    if (enable_record_full_link_trace_info()) {
      trace::ObSpanCtx *ctx = flt_.trace_log_info_.client_response_write_ctx_;
      if (OB_NOT_NULL(ctx)) {
        // set show trace buffer before flush trace
        if (flt_.control_info_.is_show_trace_enable()) {
          SET_SHOW_TRACE_INFO(&flt_.show_trace_json_info_.curr_sql_json_span_array_);
        }
        LOG_DEBUG("end span ob_proxy_client_response_write", K(ctx->span_id_));
        SET_TRACE_BUFFER(flt_trace_buffer_, MAX_TRACE_LOG_SIZE);
        FLT_END_SPAN(ctx);
        flt_.trace_log_info_.client_response_write_ctx_ = NULL;
      }
    }

    // consume the ob20 tail crc(4) in client buf reader while analyzed all the mysql packet in ob20 payload
    ObClientSessionInfo &client_session_info = get_client_session()->get_session_info();
    ObProxyProtocol client_proto = get_client_session_protocol();
    bool ob20_req_received_done = client_session_info.ob20_request_.ob20_request_received_done_;
    int64_t ob20_req_remain_payload_len = client_session_info.ob20_request_.remain_payload_len_;
    if (client_proto == ObProxyProtocol::PROTOCOL_OB20
        && ob20_req_received_done
        && ob20_req_remain_payload_len == 0) {
      client_session_info.ob20_request_.reset();
      int64_t read_avail = client_buffer_reader_->read_avail();
      LOG_DEBUG("before handle tail crc in setup cmd complete", K(read_avail));
      if (OB_LIKELY(read_avail >= OB20_PROTOCOL_TAILER_LENGTH)) {
        if (OB_FAIL(client_buffer_reader_->consume(OB20_PROTOCOL_TAILER_LENGTH))) {
          LOG_WDIAG("fail to consume the last crc buffer in client request buffer", K(ret));
        }
      } else {
        // nothing
        // cause buffer could be consumed all before, eg: handle internal request
      }
    }

    // it's necessary to end full link trace resource after trans/resp finished
    if (OB_SUCC(ret)) {
      if (OB_FAIL(handle_resp_for_end_flt_trace(ObMysqlTransact::TRANSACTION_COMPLETE
                                                == trans_state_.current_.state_))) {
        LOG_WDIAG("fail to handle resp for end flt trace", K(ret));
      } else {
        cmd_size_stats_.reset();
        if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
          cmd_time_stats_.reset();
        }
        if (ObMysqlTransact::is_in_trans(trans_state_)
            && (client_session_->is_trans_internal_routing())) {
          trans_state_.server_info_.reset();
          trans_state_.pll_info_.reset();
          trans_state_.current_.attempts_ = 1;
          trans_state_.reset_congestion_entry();
        }
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
          if (OB_LIKELY(ObConnectionDiagnosisTrace::is_enable_diagnosis_log(get_global_proxy_config().connection_diagnosis_option))) {
            client_session_->conn_record_.last_cmd_complete_ = get_based_hrtime();
          };
        }

        // wait new client request
        if (OB_SUCC(kill_after_cmd_done_err_code_)
            && OB_FAIL(setup_client_request_read())) {
          LOG_WDIAG("failed to setup_client_request_read", K_(sm_id), K(ret));
        }
      }
    }

    if (OB_FAIL(kill_after_cmd_done_err_code_)) {
      kill_after_cmd_done_err_code_ = OB_SUCCESS;
      COLLECT_VC_DIAGNOSIS(
              connection_diagnosis_trace_, obutils::OB_SERVER_VC_TRACE, VC_EVENT_EOS,
              kill_after_cmd_done_err_code_, "key server session disconnect, will disconnect");
      terminate_sm_ = true;
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
      && client_session_->dummy_ldc_.is_readonly_zone_exist()
      && !ObMysqlTransact::is_in_trans(trans_state_)) {
    common::ObAddr tmp_addr;
    (void)tmp_addr.set_sockaddr(last_ss->server_ip_.sa_);
    const bool is_last_route_readonly_zone = client_session_->dummy_ldc_.is_readonly_zone(tmp_addr);
    const bool is_weak_read = (common::WEAK == static_cast<ObConsistencyLevel>(client_session_->get_session_info().get_read_consistency()));
    common::ObAddr current_addr;
    (void)current_addr.set_sockaddr(trans_state_.server_info_.addr_.sa_);
    const bool is_current_route_readonly_zone = client_session_->dummy_ldc_.is_readonly_zone(current_addr);
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
  bool is_in_trans = (trans_state_.is_hold_start_trans_
                      || trans_state_.is_hold_xa_start_
                      || ObMysqlTransact::is_in_trans(trans_state_));
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
    LOG_WDIAG("invalid client_session");
  } else if (OB_ISNULL(server_session_)) {
    LOG_WDIAG("invalid server_session");
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
        LOG_WDIAG("fail to swap mutex for mysql client", K_(sm_id), K(ret));
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
    LOG_WDIAG("invalid event or  internal state", K_(sm_id), K(event), K_(client_session));
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

  if (OB_NOT_NULL(connection_diagnosis_trace_)) {
    if (ObConnectionDiagnosisTrace::is_enable_diagnosis_log(get_global_proxy_config().connection_diagnosis_option)) {
      build_basic_connection_diagnosis_info();
    }
    DEC_SHARED_REF(connection_diagnosis_trace_);
  }

  if (OB_UNLIKELY(1 != reentrancy_count_)) {
    LOG_EDIAG("invalid internal state, reentrancy_count should be 1",
              K_(reentrancy_count), K_(sm_id));
  }

  if (!kill_this_async_done_) {
    // cancel uncompleted actions
    // The action should be cancelled only if the
    // state machine is in MYSQL_API_NO_CALLOUT
    // state. This is because we are depending on the
    // callout to complete for the state machine to
    // get killed.

    if (OB_MYSQL_COM_QUIT != trans_state_.trans_info_.client_request_.get_packet_meta().cmd_
        && OB_MYSQL_COM_HANDSHAKE != trans_state_.trans_info_.client_request_.get_packet_meta().cmd_
        && OB_MYSQL_COM_LOGIN != trans_state_.trans_info_.client_request_.get_packet_meta().cmd_) {
      LOG_INFO("will deallocate sm", K_(sm_id), K_(pending_action),
               K_(api_.callout_state), K_(trans_state_.trace_log));
    } else {
      LOG_DEBUG("will deallocate sm",
          K_(sm_id), K_(pending_action), K_(api_.callout_state));
    }

    update_stats();

    if (MYSQL_API_NO_CALLOUT == api_.callout_state_ && NULL != pending_action_) {
      LOG_DEBUG("deallocating sm", K_(sm_id), K_(pending_action));
      if (OB_FAIL(pending_action_->cancel())) {
        LOG_WDIAG("failed to cancel pending action", K_(pending_action), K_(sm_id), K(ret));
      }
      pending_action_ = NULL;
    }

    // before close client_entry, must close server_entry_ firstly;
    if (NULL != server_entry_) {
      if (OB_FAIL(vc_table_.cleanup_entry(server_entry_))) {
        LOG_WDIAG("vc table failed to cleanup server entry", K_(sm_id), K_(server_entry), K(ret));
      } else {
        server_entry_ = NULL;
        server_session_ = NULL;
      }
    }

    if (OB_FAIL(vc_table_.cleanup_all())) {
      LOG_WDIAG("vc_table failed to cleanup_all", K_(sm_id), K(ret));
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
    LOG_EDIAG("invalid internal state, reentrancy_count should be 0",
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
      LOG_EDIAG("after kill sm, invalid internal state",
                K_(pending_action), "is_table_clear", vc_table_.is_table_clear(),
                "is_tunnel_active", tunnel_.is_tunnel_active(), K_(sm_id));
    }

    MYSQL_SM_SET_DEFAULT_HANDLER(NULL);

#ifdef USE_MYSQL_DEBUG_LISTS
    if (OB_SUCC(mutex_acquire(&g_debug_sm_list_mutex))) {
      g_debug_sm_list.remove(this);
      if (OB_FAIL(mutex_release(&g_debug_sm_list_mutex))) {
        LOG_WDIAG("failed to release mutex", K_(sm_id));
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
       || VC_EVENT_ACTIVE_TIMEOUT == event
       || VC_EVENT_DETECT_SERVER_DEAD)
      && ObMysqlTransact::SM_ACTION_OBSERVER_OPEN == trans_state_.next_action_
      && 0 == milestones_.server_connect_end_
      && OB_LIKELY(NULL != pending_action_)
      && OB_LIKELY(NULL == server_session_)
      && OB_LIKELY(NULL != trans_state_.congestion_entry_)
      && OB_LIKELY(NULL != trans_state_.mysql_config_params_)
      && trans_state_.mysql_config_params_->min_congested_connect_timeout_
          <= (client_connect_timeout = milestone_diff(milestones_.server_connect_begin_, get_based_hrtime()))) {

    trans_state_.set_alive_failed();

    LOG_WDIAG("client connect timeout, proxy treat target server alive congested",
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
      LOG_WDIAG("fail to get or create tenant stat item", K(ret));
    }
  } else if (OB_ISNULL(item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("tenant stat item is null, it should not happened", K(logic_tenant_name), K(logic_database_name),
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
    get_global_tenant_stat_mgr().revert_item(item);
  }
}

void ObMysqlSM::get_monitor_error_info(int32_t &error_code, ObString &error_msg, bool &is_error_resp, bool &is_database_error)
{
  const char *msg = NULL;

  if (ObMysqlTransact::TRANSACTION_COMPLETE == trans_state_.current_.state_ || ObMysqlTransact::CMD_COMPLETE == trans_state_.current_.state_) {
    bool enable_diagnosis_trace = ObConnectionDiagnosisTrace::is_enable_diagnosis_log(get_global_proxy_config().connection_diagnosis_option);
    ObRespAnalyzeResult &resp_result = trans_state_.trans_info_.resp_result_;
    // if enable connection diagnosis and disconnect by client, just record in diagnosis log
    if (enable_diagnosis_trace &&
        (client_entry_ != NULL && client_entry_->eos_) &&
        trans_state_.inner_errcode_ == OB_CONNECT_ERROR) {
      // do nothing
    } else if (trans_state_.inner_errcode_ != 0) {
      is_error_resp = true;
      error_code = trans_state_.inner_errcode_;
      msg = trans_state_.inner_errmsg_ != NULL ? trans_state_.inner_errmsg_ : ob_strerror(trans_state_.inner_errcode_);
      error_msg.assign_ptr(msg, static_cast<int32_t>(STRLEN(msg)));
    } else if (resp_result.is_resp_completed_) {
      is_error_resp = resp_result.is_error_resp();
      if (is_error_resp) {
        error_code = resp_result.get_error_code();
        error_msg = resp_result.get_error_message();
        is_database_error = true;
      }
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
                        TRACE_ID_FORMAT ",%.*s,%s," \
                        "%.*s,%s,%s"

#define MONITOR_ERROR_LOG_FORMAT MONITOR_LOG_FORMAT ",%.*s,%.*s"

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
          trace_id_0, trace_id_1, server_trace_id.length(), server_trace_id.ptr(), \
          client_ip_port_buff, shard_name.length(), shard_name.ptr(),       \
          is_enc_beyond_trust ? "1" : "0",             \
          server_ip_port_buff

#define MONITOR_ERROR_LOG_PARAM MONITOR_LOG_PARAM, error_msg.length(), error_msg.ptr(),\
                                server_trace_id.length(), server_trace_id.ptr()

inline void ObMysqlSM::update_monitor_log()
{
  ObMySQLCmd request_cmd = OB_MYSQL_COM_END;
  if (trans_state_.is_auth_request_) {
    request_cmd = trans_state_.trans_info_.sql_cmd_;
  } else {
    request_cmd = trans_state_.trans_info_.client_request_.get_packet_meta().cmd_;
  }

  // only output to file when partition not hit
  if (OB_NOT_NULL(route_diagnosis_)
      && route_diagnosis_->need_log_to_file(trans_state_.trans_info_.resp_result_)){
    if (route_diagnosis_->need_log_warn(trans_state_.is_trans_first_request_,
                                        ObMysqlTransact::is_in_trans(trans_state_),
                                        client_session_ == NULL ? false : client_session_->is_trans_internal_routing())){
      OBPROXY_DIAGNOSIS_LOG(INFO, "[ROUTE]", K_(*route_diagnosis));
    } else {
      OBPROXY_DIAGNOSIS_LOG(TRACE, "[ROUTE]", K_(*route_diagnosis));

    }
  }

  if (OB_NOT_NULL(client_session_)
      && !client_session_->is_proxy_mysql_client_
      && !client_session_->is_proxysys_tenant()
      && OB_MYSQL_COM_HANDSHAKE != request_cmd
      && OB_MYSQL_COM_END != request_cmd
      && OB_MYSQL_COM_QUIT != request_cmd) {

    ObClientSessionInfo &cs_info = client_session_->get_session_info();
    ObProxyMysqlRequest &client_request = trans_state_.trans_info_.client_request_;
    ObSqlParseResult &parse_result = client_request.get_parse_result();

    int64_t slow_time_threshold = trans_state_.mysql_config_params_->slow_query_time_threshold_;
    int64_t query_digest_time_threshold = trans_state_.mysql_config_params_->query_digest_time_threshold_;
    if (OB_NOT_NULL(multi_level_config_)) {
      slow_time_threshold = multi_level_config_->slow_query_time_threshold_;
      query_digest_time_threshold = multi_level_config_->query_digest_time_threshold_;
    }
    bool is_error_resp = false;
    int error_code = 0;
    ObString error_msg;
    bool is_database_error = false;
    bool is_partition_hit = trans_state_.trans_info_.resp_result_.is_partition_hit();
    get_monitor_error_info(error_code, error_msg, is_error_resp, is_database_error);

    if ((query_digest_time_threshold > 0 && query_digest_time_threshold < cmd_time_stats_.request_total_time_)
        || (slow_time_threshold > 0 && slow_time_threshold < cmd_time_stats_.request_total_time_)
        || is_error_resp || parse_result.has_trace_log_hint()
        || get_global_proxy_config().enable_monitor_stat
        || (get_global_proxy_config().enable_qos && OB_MYSQL_COM_QUERY == request_cmd)
        || (get_global_proxy_config().enable_prometheus && g_ob_prometheus_processor.is_inited())) {

      ObString logic_tenant_name;
      ObString logic_database_name;
      cs_info.get_logic_tenant_name(logic_tenant_name);
      if (OB_UNLIKELY(logic_tenant_name.empty())) {
        logic_tenant_name.assign_ptr(get_global_proxy_config().app_name_str_,
                                     static_cast<int32_t>(STRLEN(get_global_proxy_config().app_name_str_)));
      }
      if (cs_info.is_sharding_user()) {
        if (OB_MYSQL_COM_INIT_DB == trans_state_.trans_info_.client_request_.get_packet_meta().cmd_) {
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
      // sharding 模式下, 如果是内部请求, 均只涉及到逻辑库, 无需获取物理库
      if (!cs_info.is_sharding_user() || !ObMysqlTransact::is_internal_request(trans_state_)) {
        cs_info.get_cluster_name(cluster_name);
        cs_info.get_tenant_name(tenant_name);
        cs_info.get_user_name(user_name);

        if (OB_MYSQL_COM_INIT_DB == trans_state_.trans_info_.client_request_.get_packet_meta().cmd_) {
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
          || is_error_resp || parse_result.has_trace_log_hint()) {
        const ObString ant_trace_id = parse_result.get_trace_id();
        const ObString rpc_id = parse_result.get_rpc_id();
        const char *database_type_str = ObProxyMonitorUtils::get_database_type_name(database_type);
        const ObString &logic_table_name = parse_result.get_origin_table_name();
        const char *sql_cmd = ObProxyParserUtils::get_sql_cmd_name(request_cmd);

        // for compatible sharding mode
        bool is_enc_beyond_trust = false;
        ObString shard_name;
        if (NULL != cs_info.get_shard_connector()) {
          shard_name = cs_info.get_shard_connector()->shard_name_.config_string_;
          is_enc_beyond_trust = cs_info.get_shard_connector()->is_enc_beyond_trust();
        }

        const char *stmt_type_str = "";
        ObString new_sql;
        const int32_t print_len = static_cast<int32_t>(get_global_proxy_config().digest_sql_length);
        char new_sql_buf[print_len] = "\0";
        int32_t new_sql_len = 0;
        if (OB_MYSQL_COM_QUERY == request_cmd
            || OB_MYSQL_COM_STMT_PREPARE == request_cmd
            || OB_MYSQL_COM_STMT_PREPARE_EXECUTE == request_cmd) {
          stmt_type_str = get_print_stmt_name(stmt_type);

          const ObString &origin_sql = trans_state_.trans_info_.get_print_sql(print_len);
          ObProxyMonitorUtils::sql_escape(origin_sql.ptr(), origin_sql.length(),
                                          new_sql_buf, print_len, new_sql_len);
          new_sql.assign_ptr(new_sql_buf, new_sql_len);
        }

        const uint64_t *trace_id = ObCurTraceId::get();
        uint64_t trace_id_0 = (OB_ISNULL(trace_id)) ? OB_INVALID_ID : trace_id[0];
        uint64_t trace_id_1 = (OB_ISNULL(trace_id)) ? OB_INVALID_ID : trace_id[1];

        if (is_error_resp) {
          snprintf(error_code_str, OB_MAX_ERROR_CODE_LEN, "%d", error_code);
        }

        char client_ip_port_buff[INET6_ADDRPORTSTRLEN] = "\0";
        ObIpEndpoint &client_addr = trans_state_.client_info_.addr_;
        if (ops_is_ip(client_addr)) {
          char ip_buff_temp[INET6_ADDRSTRLEN] = "\0";
          snprintf(client_ip_port_buff, INET6_ADDRPORTSTRLEN, "%s:%u", ops_ip_ntop(client_addr, ip_buff_temp, INET6_ADDRSTRLEN), ops_ip_port_host_order(client_addr));
        }
        char server_ip_port_buff[INET6_ADDRPORTSTRLEN] = "\0";
        ObIpEndpoint &server_addr = trans_state_.server_info_.addr_;
        if (ops_is_ip(server_addr)) {
          char ip_buff_temp[INET6_ADDRSTRLEN] = "\0";
          snprintf(server_ip_port_buff, INET6_ADDRPORTSTRLEN, "%s:%u", ops_ip_ntop(server_addr, ip_buff_temp, INET6_ADDRSTRLEN), ops_ip_port_host_order(server_addr));
        }
        const ObString &server_trace_id = get_server_trace_id();

        if ((slow_time_threshold > 0 && slow_time_threshold < cmd_time_stats_.request_total_time_)
            || (query_digest_time_threshold > 0 && query_digest_time_threshold < cmd_time_stats_.request_total_time_)
            || OB_UNLIKELY(parse_result.has_trace_log_hint())) {
          //使用 INFO 减少等待异步日志的时间
          _OBPROXY_DIGEST_LOG(INFO, MONITOR_LOG_FORMAT, MONITOR_LOG_PARAM);
        }

        if (slow_time_threshold > 0 && slow_time_threshold < cmd_time_stats_.request_total_time_) {
          is_slow_query = true;
          _OBPROXY_SLOW_LOG(INFO, MONITOR_LOG_FORMAT, MONITOR_LOG_PARAM);
        }

        if (is_error_resp) {
          const ObString &server_trace_id = get_server_trace_id();
          // 对于数据库的错误码，允许触发限流，如主键冲突故障
          if (is_database_error) {
            _OBPROXY_ERROR_LOG(INFO, MONITOR_ERROR_LOG_FORMAT, MONITOR_ERROR_LOG_PARAM);
          } else {
            _OBPROXY_ERROR_LOG(ERROR, MONITOR_ERROR_LOG_FORMAT, MONITOR_ERROR_LOG_PARAM);
          }
        }
      }

      if (!client_session_->is_proxy_mysql_client_ && OB_MYSQL_COM_LOGIN == request_cmd) {
        const ObHSRResult &hsr = client_session_->get_session_info().get_login_req().get_hsr_result();
        const ObAddr &addr = client_session_->get_real_client_addr();
        if (OB_LIKELY(!is_error_resp)) {
          OBPROXY_DIAGNOSIS_LOG(INFO, "[LOGIN]", K(hsr), K(addr), "login_result", "success");
          SESSION_PROMETHEUS_STAT(client_session_->get_session_info(), PROMETHEUS_NEW_CLIENT_CONNECTIONS, 1, 1);
        } else {
          SESSION_PROMETHEUS_STAT(client_session_->get_session_info(), PROMETHEUS_NEW_CLIENT_CONNECTIONS, 1, 0);
        }
      }

      if (get_global_proxy_config().enable_monitor_stat) {
        if (OB_MYSQL_COM_LOGIN == request_cmd) {
          stmt_type = OBPROXY_T_LOGIN;
        }
        update_monitor_stats(logic_tenant_name, logic_database_name,
                             cluster_name, tenant_name, database_name,
                             database_type, stmt_type, error_code_str);
      }

      // 本期只统计 COM_QUERY 的
      if (get_global_proxy_config().enable_qos && OB_MYSQL_COM_QUERY == request_cmd) {
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

      if (client_session_->is_need_convert_vip_to_tname()
          && !cs_info.is_sharding_user()
          && !is_slow_query
          && !is_error_resp
          && is_partition_hit) {
        SQLMonitorInfo monitor_info;
        SQLstatInfo& stat_info = monitor_info.sql_request_stat_count_;
        switch (stmt_type) {
          case OBPROXY_T_SELECT:
            monitor_info.select_count_ = 1;
            stat_info.select_request_total_time_ = cmd_time_stats_.request_total_time_;
            stat_info.select_process_request_time_ = cmd_time_stats_.server_process_request_time_;
            stat_info.select_prepare_send_request_to_server_time_ = cmd_time_stats_.prepare_send_request_to_server_time_;
          break;
          case OBPROXY_T_UPDATE:
            monitor_info.update_count_ = 1;
            stat_info.update_request_total_time_ = cmd_time_stats_.request_total_time_;
            stat_info.update_process_request_time_ = cmd_time_stats_.server_process_request_time_;
            stat_info.update_prepare_send_request_to_server_time_ = cmd_time_stats_.prepare_send_request_to_server_time_;
          break;
          case OBPROXY_T_INSERT:
          case OBPROXY_T_REPLACE:
            monitor_info.insert_count_ = 1;
            stat_info.insert_request_total_time_ = cmd_time_stats_.request_total_time_;
            stat_info.insert_process_request_time_ = cmd_time_stats_.server_process_request_time_;
            stat_info.insert_prepare_send_request_to_server_time_ = cmd_time_stats_.prepare_send_request_to_server_time_;
          break;
          case OBPROXY_T_DELETE:
            monitor_info.delete_count_ = 1;
            stat_info.delete_request_total_time_ = cmd_time_stats_.request_total_time_;
            stat_info.delete_process_request_time_ = cmd_time_stats_.server_process_request_time_;
            stat_info.delete_prepare_send_request_to_server_time_ = cmd_time_stats_.prepare_send_request_to_server_time_;
          break;
          default:
            monitor_info.other_count_ = 1;
            stat_info.select_request_total_time_ = cmd_time_stats_.request_total_time_;
            stat_info.select_process_request_time_ = cmd_time_stats_.server_process_request_time_;
            stat_info.select_prepare_send_request_to_server_time_ = cmd_time_stats_.prepare_send_request_to_server_time_;
          break;
        }
        monitor_info.request_count_ = 1;
        monitor_info.request_total_time_ = cmd_time_stats_.request_total_time_;
        monitor_info.server_process_request_time_ = cmd_time_stats_.server_process_request_time_;
        monitor_info.prepare_send_request_to_server_time_ = cmd_time_stats_.prepare_send_request_to_server_time_;
        self_ethread().thread_prometheus_->set_sql_monitor_info(tenant_name, cluster_name, monitor_info);
      } else {
        if (!client_session_->is_need_convert_vip_to_tname()) {
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

        SQL_PROMETHEUS_STAT(logic_tenant_name, logic_database_name,
                            cluster_name, tenant_name, database_name,
                            stmt_type, PROMETHEUS_REQUEST_COUNT,
                            is_slow_query, is_error_resp, is_partition_hit, static_cast<int64_t> (1));
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
      }
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
  trans_stats_.send_session_user_vars_time_ += cmd_time_stats_.server_send_session_user_variable_time_;
  trans_stats_.send_start_trans_time_ += cmd_time_stats_.server_send_start_trans_time_;
  trans_stats_.send_xa_start_time_ += cmd_time_stats_.server_send_xa_start_time_;
  trans_stats_.build_server_request_time_ += cmd_time_stats_.build_server_request_time_;

  int64_t slow_time_threshold = trans_state_.mysql_config_params_->slow_query_time_threshold_;
  int64_t proxy_process_time_threshold = trans_state_.mysql_config_params_->slow_proxy_process_time_threshold_;
  const char *SLOW_QUERY = "Slow Query: ";
  const char *TRACE_STAT = "Trace Stat: ";
  const char *log_head = NULL;
  bool print_info_log = false;

  if (OB_NOT_NULL(multi_level_config_)) {
    slow_time_threshold = multi_level_config_->slow_query_time_threshold_;
  }
  if (slow_time_threshold > 0 && slow_time_threshold < cmd_time_stats_.request_total_time_) {
    log_head = SLOW_QUERY;
  } else if (proxy_process_time_threshold > 0 && proxy_process_time_threshold < cmd_time_stats_.prepare_send_request_to_server_time_) {
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
    ObProxyProtocol ob_proxy_protocol = get_server_session_protocol();
    ObString user_name;
    ObString tenant_name;
    ObString cluster_name;
    ObString logic_tenant_name;
    ObString logic_database_name;
    ObString trans_internal_routing_state;
    bool is_in_trans = ObMysqlTransact::is_in_trans(trans_state_);
    bool is_trans_internal_routing = false;
    if (NULL != client_session_) {
      proxy_sessid = client_session_->get_proxy_sessid();
      cs_id = client_session_->get_cs_id();
      is_trans_internal_routing = client_session_->is_trans_internal_routing();
      const ObClientSessionInfo &cs_info = client_session_->get_session_info();
      cs_info.get_user_name(user_name);
      cs_info.get_tenant_name(tenant_name);
      cs_info.get_cluster_name(cluster_name);
      cs_info.get_logic_database_name(logic_database_name);
      cs_info.get_logic_tenant_name(logic_tenant_name);
    }
    trans_internal_routing_state = is_in_trans ? (is_trans_internal_routing ? "trans internal routing" : "trans not internal routing") : "not in trans";
    get_server_session_ids(server_sessid, ss_id);

    if (print_info_log) {
      //print info
      LOG_INFO(log_head,
               "client_ip", trans_state_.client_info_.addr_,
               "server_ip", trans_state_.server_info_.addr_,
               "obproxy_client_port", trans_state_.server_info_.obproxy_addr_,
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
               "sql", trans_state_.trans_info_.get_print_sql(),
               K(trans_internal_routing_state));
    } else {
      LOG_WDIAG(log_head,
               "client_ip", trans_state_.client_info_.addr_,
               "server_ip", trans_state_.server_info_.addr_,
               "obproxy_client_port", trans_state_.server_info_.obproxy_addr_,
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
               "sql", trans_state_.trans_info_.get_print_sql(),
               K(trans_internal_routing_state));
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
    if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
      update_cmd_stats();
    }

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
    LOG_WDIAG("invalid arguement", K_(sm_id), K(mutex), K(ret));
  } else if (OB_ISNULL(client_entry_) || OB_ISNULL(client_session_)
             || OB_UNLIKELY(NULL != server_entry_) || OB_UNLIKELY(NULL != server_session_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WDIAG("invalid internal state, client session mustn't be NULL, server session must be NULL,",
             K_(sm_id), K_(client_entry), K_(client_session), K_(server_entry), K_(server_session), K(ret));
  } else {
    mutex_ = mutex;
    tunnel_.init(*this, *mutex);
    if (OB_FAIL(setup_client_request_read())) {
      LOG_WDIAG("failed to setup_client_request_read", K_(sm_id), K(ret));
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

  LOG_EDIAG("------- begin mysql state dump -------", K_(sm_id));
  if (history_pos_ > HISTORY_SIZE) {
    hist_size = HISTORY_SIZE;
    LOG_EDIAG("   History Wrap around", K_(history_pos));
  }

  // Loop through the history and dump it
  for (int64_t i = 0; i < hist_size; ++i) {
    _LOG_EDIAG("%d   %d   %s", history_[i].event_, history_[i].reentrancy_, history_[i].file_line_);
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
      LOG_EDIAG("invalid internal state, transact_return_point is NULL", K_(sm_id));
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

    case ObMysqlTransact::SM_ACTION_BINLOG_LOCATION_LOOKUP:
      do_binlog_location_lookup();
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

      if (OB_UNLIKELY(NULL != api_.response_transform_info_.vc_)) {
        if (OB_FAIL(api_.setup_server_transfer_to_transform())) {
          LOG_WDIAG("failed to setup_server_transfer_to_transform", K_(sm_id), K(ret));
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

    case ObMysqlTransact::SM_ACTION_SETUP_GET_CLUSTER_RESOURCE:
      setup_get_cluster_resource();
    break;

    default:
      LOG_EDIAG("Unknown next action", K_(trans_state_.next_action), K_(sm_id));
      setup_error_transfer();
      break;
  }
}

int ObMysqlSM::state_binlog_location_lookup(int event, void *data)
{
  int ret = OB_SUCCESS;
  STATE_ENTER(ObMysqlSM::state_binlog_location_lookup, event);
  pending_action_ = NULL;
  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
    milestones_.bl_lookup_end_ = get_based_hrtime();
    cmd_time_stats_.bl_lookup_time_ += milestone_diff(milestones_.bl_lookup_begin_, milestones_.bl_lookup_end_);
    milestones_.bl_process_begin_ = milestones_.bl_lookup_end_;
  }

  if (OB_UNLIKELY(TABLE_ENTRY_EVENT_LOOKUP_DONE != event) || OB_ISNULL(data)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("unexpected event type, it should not happen", K(event), K(data), K(ret));
  } else {
    ObMysqlRouteResult *result = reinterpret_cast<ObMysqlRouteResult *>(data);
    if (OB_UNLIKELY(NULL == result)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("get binlog entry failed", K(result), K(ret));
    } else {
      if (NULL == result->table_entry_) {
        const ObTableEntryName &name = trans_state_.pll_info_.te_name_;
        ObTableRefHashMap &table_map = self_ethread().get_table_map();
        ObTableEntry *tmp_entry = NULL;
        ObTableEntryKey key(name, 0, 0);
        tmp_entry = table_map.get(key);
        if (OB_LIKELY(NULL != tmp_entry)) {
          tmp_entry->renew_last_access_time();
          result->table_entry_ = tmp_entry;
          result->is_table_entry_from_remote_ = false;
          tmp_entry->set_need_force_flush(false);
          LOG_DEBUG("get binlog entry succ", KPC(tmp_entry));
        }
      }
      if (NULL != result->table_entry_) {
        trans_state_.pll_info_.set_route_info(*result);
        trans_state_.pll_info_.lookup_success_ = true;
        call_transact_and_set_next_state(NULL);
      } else {
        trans_state_.mysql_errcode_ = OB_CONNECT_BINLOG_ERROR;
        if (OB_FAIL(ObMysqlTransact::encode_error_message(trans_state_))) {
          LOG_WDIAG("fail to build err resp", K(ret));
        }
        ret = OB_CONNECT_BINLOG_ERROR;
        LOG_WDIAG("cannot get binlog service ip", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
    trans_state_.inner_errcode_ = ret;
    // failed, disconnect
    trans_state_.pll_info_.lookup_success_ = false;
    trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
    // call ObMysqlTransact::handle_bl_lookup() to handle fail / success
    call_transact_and_set_next_state(NULL);
  }

  return EVENT_DONE;
}

int ObMysqlSM::handle_proxy_protocol_v2_request(ProxyProtocolV2 &v2, ObMysqlAnalyzeStatus &status)
{
  int ret = OB_SUCCESS;
  int64_t len = client_buffer_reader_->read_avail();
  status = ANALYZE_CONT;
  LOG_DEBUG("handle proxy protocol v2 request", K(len));
  if (OB_LIKELY(len >= ProxyProtocolV2::PROXY_PROTOCOL_V2_HEADER_LEN)) {
    if (OB_SUCC(ret)) {
      char packet[len];
      char *written_pos = client_buffer_reader_->copy(packet, len, 0);
      if (written_pos != (packet + len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("not copy completely", K(ret));
      } else if (OB_FAIL(v2.analyze_packet(packet, len))) {
        LOG_WDIAG("proxy protocol v2 analyze packet failed", K(ret));
      } else if (v2.is_finished()) {
        // 根据 ppv2 信息填充 ObConnTenantInfo
        status = ANALYZE_DONE;
        LOG_DEBUG("handle proxy protocol v2 success", K(v2));

        if (OB_FAIL(client_session_->fill_tenant_info_with_ppv2(v2))) {
          LOG_WDIAG("fail to fill tenant info with ppv2", K(v2), K(ret));
        } else {
          ObString user_name;
          if (!get_global_white_list_table_processor().can_ip_pass(
                client_session_->get_vip_cluster_name(), client_session_->get_vip_tenant_name(),
                user_name, ops_ip_sa_cast(v2.src_addr_.get_sockaddr()))) {
            COLLECT_LOGIN_DIAGNOSIS(
                (connection_diagnosis_trace_), OB_LOGIN_DISCONNECT_TRACE, "",
                OB_ERR_CAN_NOT_PASS_WHITELIST,
                "virtual ip user %.*s@%.*s#%.*s can not pass whitelist",
                user_name.length(), user_name.ptr(), client_session_->get_vip_tenant_name().length(),
                client_session_->get_vip_tenant_name().ptr(), client_session_->get_vip_cluster_name().length(),
                client_session_->get_vip_cluster_name().ptr());
            ret = OB_ERR_CAN_NOT_PASS_WHITELIST;
            status = ANALYZE_CAN_NOT_PASS_WHITE_LIST_ERROR;
            PROXY_CS_LOG(WDIAG, "can not pass white_list", K(client_session_->get_vip_cluster_name()),
                K(client_session_->get_vip_tenant_name()), K(v2), K(ret));
          }
        }
      }
    }
  }

  return ret;
}

bool ObMysqlSM::is_proxy_switch_route() const
{
  bool bret = false;
  ObMysqlClientSession *client_session = get_client_session();
  if (OB_NOT_NULL(client_session)
      && !client_session_->is_proxy_mysql_client_) {
    ObMysqlServerSession *cur_server_session = client_session->get_cur_server_session();
    if (OB_NOT_NULL(cur_server_session)) {
      const net::ObIpEndpoint &last_server_addr = client_session->get_session_info().get_last_server_addr();
      if (last_server_addr.is_valid()
          && cur_server_session->server_ip_.is_valid()
          && last_server_addr != cur_server_session->server_ip_) {
        bret = true;
      }
    }
  }
  return bret;
}

void ObMysqlSM::build_basic_connection_diagnosis_info()
{
  ObMysqlClientSession *client_session = get_client_session();
  if (connection_diagnosis_trace_ != NULL && connection_diagnosis_trace_->diagnosis_info_ != NULL) {
    // fill basic info
    if (client_session != NULL) {
      const ObHSRResult &hsr = client_session->get_session_info().get_login_req().get_hsr_result();
      const ObConnDiagRecord &conn_record = client_session->conn_record_;
      connection_diagnosis_trace_->diagnosis_info_->cs_id_ = client_session->get_cs_id();
      connection_diagnosis_trace_->diagnosis_info_->proxy_session_id_ = client_session->get_proxy_sessid();
      client_session_->get_real_client_addr().ip_port_to_string(connection_diagnosis_trace_->diagnosis_info_->client_addr_, MAX_IP_ADDR_LENGTH);
      MEMCPY(connection_diagnosis_trace_->diagnosis_info_->cluster_name_, hsr.cluster_name_.ptr(), MIN(hsr.cluster_name_.length(), OB_PROXY_MAX_CLUSTER_NAME_LENGTH));
      MEMCPY(connection_diagnosis_trace_->diagnosis_info_->tenant_name_, hsr.tenant_name_.ptr(), MIN(hsr.tenant_name_.length(), OB_MAX_TENANT_NAME_LENGTH));
      MEMCPY(connection_diagnosis_trace_->diagnosis_info_->user_name_, hsr.user_name_.ptr(), MIN(hsr.user_name_.length(), OB_MAX_USER_NAME_LEN));
      connection_diagnosis_trace_->diagnosis_info_->sql_cmd_ = trans_state_.trans_info_.sql_cmd_;
      connection_diagnosis_trace_->diagnosis_info_->request_cmd_ = trans_state_.trans_info_.client_request_.get_packet_meta().cmd_;
      if (obmysql::OB_MYSQL_COM_LOGIN == connection_diagnosis_trace_->diagnosis_info_->sql_cmd_) {
        connection_diagnosis_trace_->diagnosis_info_->request_cmd_ = obmysql::OB_MYSQL_COM_LOGIN;
      }

      // if server return error packet for COM_LOGIN, client session will disconnect and request_cmd has been reset
      if (obmysql::OB_MYSQL_COM_SLEEP ==connection_diagnosis_trace_->diagnosis_info_->request_cmd_ &&
          connection_diagnosis_trace_->trace_type_ == OB_LOGIN_DISCONNECT_TRACE) {
        connection_diagnosis_trace_->diagnosis_info_->request_cmd_ = obmysql::OB_MYSQL_COM_LOGIN;
      }

      if (obmysql::OB_MYSQL_COM_SLEEP == connection_diagnosis_trace_->diagnosis_info_->request_cmd_) {
        // record client session idle time
        connection_diagnosis_trace_->diagnosis_info_->req_total_time_ = milestone_diff(conn_record.last_cmd_complete_, get_based_hrtime()) / 1000;
      } else {
        connection_diagnosis_trace_->diagnosis_info_->req_total_time_ = milestone_diff(milestones_.client_.client_begin_, get_based_hrtime()) / 1000;
      }

      ObMysqlServerSession *server_session = client_session->get_cur_server_session();
      if (OB_UNLIKELY(NULL != client_session->get_closed_key_server_session())) {
        server_session = client_session->get_closed_key_server_session();
      }

      if (server_session != NULL) {
        connection_diagnosis_trace_->diagnosis_info_->ss_id_ = server_session->ss_id_;
        connection_diagnosis_trace_->diagnosis_info_->server_session_id_ = server_session->server_sessid_;
        ops_ip_nptop(server_session->server_ip_, connection_diagnosis_trace_->diagnosis_info_->server_addr_, MAX_IP_ADDR_LENGTH);
        if (server_session->get_netvc() != NULL) {
          ops_ip_nptop(server_session->get_netvc()->get_local_addr(), connection_diagnosis_trace_->diagnosis_info_->proxy_server_addr_, MAX_IP_ADDR_LENGTH);
         }
      } else {
        connection_diagnosis_trace_->diagnosis_info_->server_session_id_ = conn_record.cur_server_sess_id_;
        ops_ip_nptop(conn_record.cur_server_sess_dst_addr_, connection_diagnosis_trace_->diagnosis_info_->server_addr_, MAX_IP_ADDR_LENGTH);
        ops_ip_nptop(conn_record.cur_server_sess_src_addr_, connection_diagnosis_trace_->diagnosis_info_->proxy_server_addr_, MAX_IP_ADDR_LENGTH);
      }
    }
    ObConnectionDiagnosisInfo *diagnosis_info = connection_diagnosis_trace_->diagnosis_info_;
    ObString user_sql = trans_state_.trans_info_.client_request_.get_print_sql();
    // fill common info
    switch (connection_diagnosis_trace_->trace_type_) {
      case obutils::OB_LOGIN_DISCONNECT_TRACE:
        break;
      case obutils::OB_PROXY_INTERNAL_TRACE:
      case obutils::OB_SERVER_INTERNAL_TRACE: {
        ObProxyInternalDiagnosisInfo *internal_info = static_cast<ObProxyInternalDiagnosisInfo*>(diagnosis_info);
        if (internal_info != NULL) {
          int32_t new_sql_len = 0;
          ObProxyMonitorUtils::sql_escape(user_sql.ptr(), user_sql.length(), internal_info->user_sql_, OB_SHORT_SQL_LENGTH, new_sql_len);
        }
        break;
      }
      case obutils::OB_CLIENT_VC_TRACE:
      case obutils::OB_SERVER_VC_TRACE: {
        ObVCDiagnosisInfo *vc_info = static_cast<ObVCDiagnosisInfo*>(diagnosis_info);
        if (vc_info != NULL) {
          int32_t new_sql_len = 0;
          ObProxyMonitorUtils::sql_escape(user_sql.ptr(), user_sql.length(), vc_info->user_sql_, OB_SHORT_SQL_LENGTH, new_sql_len);
        }
        break;
      }
      case obutils::OB_TIMEOUT_DISCONNECT_TRACE: {
        // do nothing
        break;
      }
      default:
        break;
    }
    fill_disconnect_message();
    // to print protocol diagnosis info with connection diagnosis
    INC_SHARED_REF(connection_diagnosis_trace_->protocol_diagnosis_, protocol_diagnosis_);
  } else if (connection_diagnosis_trace_ == NULL) {
    LOG_WDIAG("unexpected connection diagnosis trace state", K(sm_id_));
  }
}


void ObMysqlSM::fill_disconnect_message() {
  if (connection_diagnosis_trace_ != NULL
      && connection_diagnosis_trace_->diagnosis_info_ != NULL
      && (STRLEN(connection_diagnosis_trace_->diagnosis_info_->error_msg_) == 0)) {
    ObString error_msg;
    int32_t error_code = static_cast<int32_t>(connection_diagnosis_trace_->diagnosis_info_->error_code_);
    if (error_code == OB_PROXY_INTERNAL_ERROR || error_code == 0) {
      bool is_error_resp = false;
      bool is_database_error = false;
      get_monitor_error_info(error_code, error_msg, is_error_resp, is_database_error);
    } else {
      error_msg = ob_strerror(error_code);
    }
    connection_diagnosis_trace_->diagnosis_info_->set_error_msg(error_msg);
  }
}

void ObMysqlSM::refresh_single_leader()
{
  int ret = OB_SUCCESS;
  int64_t version = sm_cluster_resource_ == NULL ? single_leader_version_
                : sm_cluster_resource_->get_single_leader_map_version();
  bool enable_single_leader = OB_NOT_NULL(multi_level_config_)
                              && multi_level_config_->enable_single_leader_node_routing_;
  bool old_is_single_leader_vaild = is_vaild_single_leader();
  // 1. version changed
  if (OB_UNLIKELY(version != single_leader_version_)) {
    single_leader_version_ = version;
    if (OB_NOT_NULL(client_session_)
        && OB_NOT_NULL(sm_cluster_resource_)
        && enable_single_leader
        && !client_session_->is_proxy_mysql_client_) {
      ObString &t = client_session_->get_session_info().get_priv_info().tenant_name_;
      if (OB_FAIL(sm_cluster_resource_->get_single_leader_info(t, single_leader_addr_))) {
        LOG_DEBUG("fail to get single leader", K(t), K(ret));
        reset_single_leader();
      } else {
        LOG_DEBUG("mysql sm has single leader", K(t));
      }
    } else {
      reset_single_leader();
    }
  }

  // 2. config changed
  if (!enable_single_leader) {
    if (old_is_single_leader_vaild) {
      reset_single_leader();
    }
  }
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

