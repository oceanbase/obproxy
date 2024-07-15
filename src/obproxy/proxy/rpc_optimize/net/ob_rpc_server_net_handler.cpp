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
#include "lib/encrypt/ob_encrypted_helper.h"
#include "proxy/rpc_optimize/ob_rpc_req_debug_names.h"
#include "proxy/rpc_optimize/net/ob_rpc_net_handler.h"
#include "proxy/rpc_optimize/net/ob_rpc_server_net_handler.h"
#include "obutils/ob_config_server_processor.h"
#include "dbconfig/ob_proxy_pb_utils.h"
#include "proxy/mysql/ob_mysql_global_session_manager.h"
#include "omt/ob_conn_table_processor.h"
#include "stat/ob_rpc_stats.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::prometheus;
using namespace oceanbase::obproxy::dbconfig;
using namespace oceanbase::obproxy::omt;
using namespace oceanbase::obproxy::obkv;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

#define STATE_ENTER(state_name, event, vio) do { \
  PROXY_SS_LOG(DEBUG, "ENTER STATE "#state_name"", "event", ObRpcReqDebugNames::get_event_name(event), K_(ss_id), K_(server_ip), K_(local_ip)); \
} while(0)

#define RPC_SERVER_ENTRY_SET_DEFAULT_HANDLER(h) {   \
  default_handler_ = h;}

/*
#define RPC_SSN_INCREMENT_DYN_STAT(x)     \
  session_stats_.stats_[x] += 1;            \
  RPC_INCREMENT_DYN_STAT(x)
  */

// We have debugging list that we can use to find stuck
// client sessions
#ifdef USE_MYSQL_DEBUG_LISTS
DLL<ObRpcServerNetHandler> g_debug_rpc_cs_list;
// ObMutex g_debug_rpc_cs_list_mutex;
ObMutex g_debug_rpc_cs_list_mutex;
#endif

static int64_t const MYSQL_BUFFER_SIZE = BUFFER_SIZE_FOR_INDEX(BUFFER_SIZE_INDEX_8K);
static int64_t const RPC_PKT_PAYLOAD_SIZE_POS = 4;

ObRpcServerNetHandler::ObRpcServerNetHandler()
    : ObRpcNetHandler(),
      read_state_(MCS_INIT),
      conn_decrease_(false),
      active_(true),
      server_addr_(),
      vc_ready_killed_(false),
      need_send_req_list_(),
      sending_req_list_(),
      tcp_init_cwnd_set_(false),
      half_close_(false),
      state_(OB_RPC_SERVER_ENTRY_INIT),
      server_connect_retry_times_(0),
      first_send_time_us_(0),
      last_send_time_us_(0),
      last_recv_time_us_(0),
      last_active_time_us_(0),
      request_sent_(0),
      request_pending_(0),
      current_need_read_len_(RPC_NET_HEADER_LENGTH),
      current_ez_header_(),
      server_table_entry_(NULL),
      create_thread_(NULL),
      current_tid_(-1),
      s_id_(0),
      cs_id_(0),
      proxy_sessid_(0),
      atomic_channel_id_(1),
      timeout_us_(0),
      pending_action_(NULL),
      timeout_action_(NULL),
      period_task_action_(NULL),
      cid_to_req_map_(),
      net_head_buf_()
{
  cid_to_req_map_.create(OB_RPC_PARALLE_REQUEST_MAP_MAX_BUCKET_NUM, ObModIds::OB_RPC);
  SET_HANDLER(&ObRpcServerNetHandler::main_handler);
}

void ObRpcServerNetHandler::destroy()
{
  PROXY_SS_LOG(INFO, "rpc server net handler destroy", K_(ss_id), K_(server_ip), K_(local_ip), K_(proxy_sessid),
               KP_(rpc_net_vc));

  if (OB_UNLIKELY(NULL != rpc_net_vc_)
      || OB_ISNULL(read_buffer_)) {
    PROXY_SS_LOG(WDIAG, "invalid rpc server net handler session", K_(ss_id), K_(server_ip), K_(local_ip),
                 K(rpc_net_vc_), K(read_buffer_));
  }

  magic_ = RPC_NET_SS_MAGIC_DEAD;
  if (OB_LIKELY(NULL != read_buffer_)) {
    free_miobuffer(read_buffer_);
    read_buffer_ = NULL;
  }
  buf_reader_ = NULL;

#ifdef USE_MYSQL_DEBUG_LISTS
  mutex_acquire(&g_debug_rpc_cs_list_mutex);
  g_debug_rpc_cs_list.remove(this);
  mutex_release(&g_debug_rpc_cs_list_mutex);
#endif

  create_thread_ = NULL;

  op_reclaim_free(this);
}

void ObRpcServerNetHandler::init_server_addr(ObAddr server_addr)
{
  UNUSED(server_addr);
}

void ObRpcServerNetHandler::init_server_ip(net::ObIpEndpoint &server_ip)
{
  server_ip_ = server_ip;
}

inline int64_t ObRpcServerNetHandler::get_next_ss_id()
{
  static int64_t next_ss_id = 1;
  int64_t ret = 0;
  do {
    ret = ATOMIC_FAA(&next_ss_id, 1);
  } while (0 == ret);
  return ret;
}

int ObRpcServerNetHandler::new_connection(net::ObNetVConnection &new_vc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    PROXY_SS_LOG(WDIAG, "init twice", K_(ss_id), K_(server_ip), K_(local_ip), K(is_inited_), K(ret));
  } else {
    // server_vc_ = &new_vc;
    rpc_net_vc_ = &new_vc;
    mutex_ = new_vc.mutex_;

    // Unique server session identifier.
    ss_id_ = get_next_ss_id();
    magic_ = RPC_NET_SS_MAGIC_ALIVE;
    //TODO check need it convert to CURRENT_SERVER_RPC_CONNECTIONS or not
    RPC_SUM_GLOBAL_DYN_STAT(CURRENT_SERVER_CONNECTIONS, 1); // Update the true global stat
    RPC_INCREMENT_DYN_STAT(TOTAL_SERVER_CONNECTIONS); //TODO need check not have RPC_DECREMENT_DYN_STAT for TOTAL_SERVER_CONNECTIONS
    //TODO need add it later after assign cluster and tenant info for server_net_entry
    //RPC_SESSION_PROMETHEUS_STAT(, PROMETHEUS_CURRENT_SESSION, false, 1);

    read_buffer_ = new_empty_miobuffer(MYSQL_BUFFER_SIZE);
    if (OB_LIKELY(NULL != read_buffer_)) {
      buf_reader_ = read_buffer_->alloc_reader();
      /**
       * we cache the response in the read io buffer, so the water mark of
       * read io buffer must be larger than the response packet size. we set
       * RPC_NET_HEADER_LENGTH as the default water mark, when we read
       * the header of response, we reset the water mark.
       */
      read_buffer_->water_mark_ = MYSQL_BUFFER_SIZE;  // 默认水位调整为8192
      // if (OB_FAIL(session_info_.init())) {
      //   PROXY_SS_LOG(WDIAG, "fail to init session_info", K_(ss_id), K(ret));
      // } else {
        // DBServerType server_type = client_session.get_session_info().get_server_type();
        // session_info_.set_server_type(server_type);
      PROXY_SS_LOG(INFO, "server net handler born", K_(ss_id), K_(server_ip), K_(local_ip));

      state_ = OB_RPC_SERVER_ENTRY_INIT;
      is_inited_ = true;
        // if (is_pool_session_) {
        //   ret = get_global_session_manager().add_server_session(*this);
        //   create_time_ = ObTimeUtility::current_time();
        // }
      // }

      if (OB_FAIL(schedule_period_task())) {
        PROXY_LOG(WDIAG, "fail to call schedule_period_task", K(ret), K_(ss_id), K_(server_ip), K_(local_ip));
      }
    } else {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      PROXY_SS_LOG(WDIAG, "alloc mem for read_buffer_ error", K(ret), K_(ss_id), K_(server_ip), K_(local_ip));
    }
  }
  return ret;
}

int ObRpcServerNetHandler::handle_new_connection()
{
  // Use a local pointer to the mutex as when we return from do_api_callout,
  // the ClientSession may have already been deallocated.
  ObEThread &ethread = self_ethread();
  ObPtr<ObProxyMutex> lmutex = mutex_;
  int ret = OB_SUCCESS;
  {
    MUTEX_LOCK(lock, lmutex, &ethread);
    ObAction *connect_action_handle = NULL;
    ObNetVCOptions opt;

    opt.f_blocking_connect_ = false;
    //TODO need add it as global config
    // opt.set_sock_param(static_cast<int32_t>(trans_state_.mysql_config_params_->sock_recv_buffer_size_out_),
    //                    static_cast<int32_t>(trans_state_.mysql_config_params_->sock_send_buffer_size_out_),
    //                    static_cast<uint32_t>(trans_state_.mysql_config_params_->sock_option_flag_out_),
    //                    static_cast<uint32_t>(trans_state_.mysql_config_params_->sock_packet_mark_out_),
    //                    static_cast<uint32_t>(trans_state_.mysql_config_params_->sock_packet_tos_out_));
    if (opt.sockopt_flags_ & ObNetVCOptions::SOCK_OPT_KEEP_ALIVE) {
      // opt.set_keepalive_param(static_cast<int32_t>(trans_state_.mysql_config_params_->server_tcp_keepidle_),
      //                         static_cast<int32_t>(trans_state_.mysql_config_params_->server_tcp_keepintvl_),
      //                         static_cast<int32_t>(trans_state_.mysql_config_params_->server_tcp_keepcnt_),
      //                         static_cast<int32_t>(trans_state_.mysql_config_params_->server_tcp_user_timeout_));
    }
    opt.ip_family_ = server_ip_.sa_.sa_family;//server_addr_.sa_.sa_family;
    // opt.is_inner_connect_ = client_session_->is_proxy_rpc_client_;
    // opt.ethread_ = client_session_->is_proxy_rpc_client_ ? this_ethread() : client_session_->get_create_thread();
    // opt.ethread_ = this_ethread();
    opt.ethread_ = &ethread;

    // Set the inactivity timeout to the connect timeout so that we
    // we fail this server if it doesn't start sending the response
    // convert to ns
    //ObShardProp *shard_prop = client_session_->get_session_info().get_shard_prop();
    int64_t connect_timeout = get_global_proxy_config().short_async_task_timeout; //set it to global config info

    // PROXY_SS_LOG(DEBUG, "calling g_net_processor.connect", K_(s_id), K_(trans_state_.server_info_.addr_));
    PROXY_SS_LOG(DEBUG, "calling g_net_processor.connect", K_(server_addr), K_(ss_id), K_(server_ip), K_(local_ip), K(this));
    ret = g_net_processor.connect(*this, server_ip_.sa_, connect_action_handle, connect_timeout, &opt);
    if (OB_FAIL(ret)) {
      PROXY_SS_LOG(WDIAG, "failed to connect observer", K_(ss_id), K_(server_ip), K_(local_ip), K(ret));
    } else if (OB_ISNULL(connect_action_handle)) {
      // connect fail, net module has called back, do nothing
    } else if (NULL != pending_action_) {
      if (OB_SUCCESS != connect_action_handle->cancel()) {
        PROXY_SS_LOG(WDIAG, "failed to cancel connect observer pending action", K_(ss_id),
                     K_(server_ip), K_(local_ip), K(connect_action_handle));
      }
      ret = OB_ERR_UNEXPECTED;
      PROXY_SS_LOG(WDIAG, "invalid internal state, pending_action_ is not NULL", K_(pending_action),
                   K_(ss_id), K_(server_ip), K_(local_ip), K(ret));
    } else {
      //not set pending_action any more, maybe we need init a async task to build a new connection
      pending_action_ = connect_action_handle;
    }
  }
  if (OB_FAIL(ret)) {
    do_io_close();
  }

  return ret;
}

uint64_t ObRpcServerNetHandler::get_next_proxy_sessid()
{
  static uint64_t next_proxy_sessid = 1;
  const ObAddr &addr = get_global_hot_upgrade_info().local_addr_;
  int64_t ipv4 = static_cast<int64_t>(addr.get_ipv4());
  int64_t port = static_cast<int64_t>(addr.get_port());
  port &= 0xFFFF;

  uint64_t ret = ATOMIC_FAA((&next_proxy_sessid), 1);
  ret &= 0xFFFF;
  ret |= (port << 16);
  ret |= (ipv4 << 32);
  return ret;
}

ObVIO *ObRpcServerNetHandler::do_io_write(
    ObContinuation *c, const int64_t nbytes, ObIOBufferReader *buf)
{
  // conditionally set the tcp initial congestion window
  // before our first write.
  if (!tcp_init_cwnd_set_) {
    tcp_init_cwnd_set_ = true;
    set_tcp_init_cwnd();
  }
  return rpc_net_vc_->do_io_write(c, nbytes, buf);
}

void ObRpcServerNetHandler::set_tcp_init_cwnd()
{
  int32_t desired_tcp_init_cwnd = static_cast<int32_t>(get_global_proxy_config().server_tcp_init_cwnd);

  if (0 != desired_tcp_init_cwnd) {
    if (0 != rpc_net_vc_->set_tcp_init_cwnd(desired_tcp_init_cwnd)) {
      PROXY_SS_LOG(WDIAG, "set_tcp_init_cwnd failed", K_(ss_id), K_(server_ip), K_(local_ip), K(desired_tcp_init_cwnd));
    }
  }
}

void ObRpcServerNetHandler::do_io_close(const int alerrno)
{
  int ret = OB_SUCCESS;
  // Prevent double closing
  PROXY_SS_LOG(INFO, "server net handle do_io_close", K_(ss_id), K_(server_ip), K_(local_ip), KP(rpc_net_vc_), KP(this));
  clean_all_pending_request();
  if (OB_FAIL(cancel_period_task())) {
    PROXY_LOG(WDIAG, "fail to call cancel_period_task", K_(ss_id), K_(server_ip), K_(local_ip), K(ret));
  }

  if (OB_RPC_SERVER_ENTRY_ACTIVE <= state_) {
    //TODO need update stat_info for rpc server net for thread
      //  RPC_DECREMENT_DYN_STAT(CURRENT_SERVER_TRANSACTIONS);
    // --server_trans_stat_;
  }

  if (OB_NOT_NULL(rpc_net_vc_)) {
    rpc_net_vc_->do_io_close(alerrno);
    rpc_net_vc_ = NULL;
  }
  if (OB_NOT_NULL(server_table_entry_)) {
    server_table_entry_->remove_server_entry(this); /* remove it from server_table_entry */
  }
  if (OB_FAIL(cancel_timeout_action())) {
    PROXY_LOG(WDIAG, "fail to call cancel_timeout_action", K_(ss_id), K_(server_ip), K_(local_ip), K(ret));
  }
  if (OB_FAIL(cancel_pending_action())) {
    PROXY_LOG(WDIAG, "fail to call cancel_pending_action", K_(ss_id), K_(server_ip), K_(local_ip), K(ret));
  }

  state_ = OB_RPC_SERVER_ENTRY_DESTROY;

  RPC_SUM_GLOBAL_DYN_STAT(CURRENT_SERVER_CONNECTIONS, -1); // Make sure to work on the global stat
  //   RPC_SUM_GLOBAL_DYN_STAT(CURRENT_SERVER_CONNECTIONS, -1); // Make sure to work on the global stat
  destroy();
}

int ObRpcServerNetHandler::handle_other_event(int event, void *data)
{
  UNUSED(data);
  switch (event) {
    case CLIENT_SESSION_ERASE_FROM_MAP_EVENT: {
      do_io_close();
      break;
    }
    default:
      PROXY_SS_LOG(WDIAG, "unknown event", K(event), K_(ss_id), K_(server_ip), K_(local_ip));
      break;
  }

  return VC_EVENT_NONE;
}

int ObRpcServerNetHandler::state_server_new_connection(int event, void *data)
{
  int ret = OB_SUCCESS;

  STATE_ENTER(ObRpcServerNetHandler::state_server_new_connection, event, data);
  pending_action_ = NULL;
  switch (event) {
    case NET_EVENT_OPEN:
    {
      //init server_entry with net_vc
      if (OB_ISNULL(data)) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_SS_LOG(WDIAG, "invalid server entry data", K_(ss_id), K_(server_ip), K_(local_ip), K(data), K(ret));
      } else {
        ObNetVConnection *net_vc = reinterpret_cast<ObNetVConnection *>(data);
        ops_ip_copy(local_ip_, net_vc->get_local_addr());
        if (OB_NOT_NULL(server_table_entry_)) {
          server_table_entry_->cleanup_server_connector_error();
        }
        if (OB_FAIL(new_connection(*(reinterpret_cast<ObNetVConnection *>(data))))) {
          PROXY_SS_LOG(WDIAG, "fail to new server connection", K_(ss_id), K_(server_ip), K_(local_ip), KPC(this), K(ret));
        } else {
          state_ = OB_RPC_SERVER_ENTRY_ACTIVE;
        }
        //TODO if need do something when server tcp handle in here
        //   cmd_time_stats_.do_observer_open_time_ += milestone_diff(milestones_.do_observer_open_begin_,
        //   milestones_.do_observer_open_end_);)

        //set keep alived
        if (OB_UNLIKELY(NULL != net_vc &&
            (net_vc->options_.sockopt_flags_ != get_global_proxy_config().sock_option_flag_out
             || net_vc->options_.packet_mark_ != get_global_proxy_config().sock_packet_mark_out
             || net_vc->options_.packet_tos_ != get_global_proxy_config().sock_packet_tos_out))) {
              net_vc->options_.sockopt_flags_ = static_cast<uint32_t>(get_global_proxy_config().sock_option_flag_out);
              net_vc->options_.packet_mark_ = static_cast<uint32_t>(get_global_proxy_config().sock_packet_mark_out);
              net_vc->options_.packet_tos_ = static_cast<uint32_t>(get_global_proxy_config().sock_packet_tos_out);
          if (net_vc->options_.sockopt_flags_ & ObNetVCOptions::SOCK_OPT_KEEP_ALIVE) {
            net_vc->options_.set_keepalive_param(static_cast<int32_t>(get_global_proxy_config().server_tcp_keepidle),
                                         static_cast<int32_t>(get_global_proxy_config().server_tcp_keepintvl),
                                         static_cast<int32_t>(get_global_proxy_config().server_tcp_keepcnt),
                                         static_cast<int32_t>(get_global_proxy_config().server_tcp_user_timeout));
          }
          if (OB_FAIL(net_vc->apply_options())) {
            PROXY_SS_LOG(WDIAG, "server session failed to apply per-transaction socket options", K_(ss_id),
                         K_(server_ip), K_(local_ip), K(ret));
          }
        }
        if (OB_ISNULL(net_entry_.write_buffer_ = new_miobuffer(MYSQL_BUFFER_SIZE))) {
          ret = OB_ERR_UNEXPECTED;
          PROXY_SS_LOG(WDIAG, "[ObRpcServerNetHandler::state_server_new_connection] write_buffer is null",
                       K_(ss_id), K_(server_ip), K_(local_ip));
        }
      }
      // int64_t write_len = 0;
      if (OB_ISNULL(net_entry_.read_vio_ = do_io_read(this, INT64_MAX, read_buffer_))) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_SS_LOG(WDIAG, "server entry failed to do_io_read", K_(ss_id), K_(server_ip), K_(local_ip), K(ret));
      } else if (OB_ISNULL(net_entry_.write_vio_ = do_io_write(this, 0, NULL))) {
        PROXY_SS_LOG(WDIAG, "server entry failed to do_io_write", K_(ss_id), K_(server_ip), K_(local_ip), K(ret));
      } else {
        //has init a empty server net entry
        server_connect_retry_times_ = 0;
      }
      break;
    }
    case NET_EVENT_OPEN_FAILED: //maybe need retry, or re-excute rpc
    {
      PROXY_SS_LOG(DEBUG, "[ObRpcServerNetHandler::state_server_new_connection] connect failed",
                   K_(server_connect_retry_times), K_(ss_id), K_(server_ip), K_(local_ip));
      state_ = OB_RPC_SERVER_ENTRY_INIT; //TODO need add retry
      // call_transact_and_set_next_state(ObMysqlTransact::handle_response);
      //TODO handle the retry
      break;
    }
    case VC_EVENT_ERROR:
    {
      state_ = OB_RPC_SERVER_ENTRY_INIT; //TODO need add retry
      break;
    }
    default:
      ret = OB_INNER_STAT_ERROR;
      PROXY_SS_LOG(EDIAG, "[ObRpcServerNetHandler::state_server_new_connection] Unknown event:", K(event), K_(ss_id),
                   K_(server_ip), K_(local_ip), K(ret), K_(server_connect_retry_times));
      break;
  }

  if (OB_FAIL(ret) || state_ != OB_RPC_SERVER_ENTRY_ACTIVE) {

    PROXY_SS_LOG(WDIAG, "ObRpcServerNetHandler connect failed", K_(ss_id), K_(server_ip), K_(local_ip), K(ret),
                 K_(server_connect_retry_times), K_(server_addr));
    // To avoid set server fail, if init local socket error, not have set server_table_entry_ for server_net_entry_
    // vc->handle_connect, schedule it by function mode, not to use schedule_imm. if fail in local proxy, we need not
    // to set server_table_entry_ status any more.
    if (OB_NOT_NULL(server_table_entry_)) { 
      //pending request need to retry other server addr
      server_table_entry_->set_server_connector_error();
    }

    if (server_connect_retry_times_ < 3) {
      server_connect_retry_times_++;
      if (OB_FAIL(handle_new_connection())) {
        PROXY_SS_LOG(WDIAG, "[ObRpcServerNetHandler::state_server_new_connection] retry handle new connection failed",
                     K_(ss_id), K_(server_ip), K_(local_ip), K(ret));
      }
    } else {
      // remove from server table entry
      // set magic to dead to avoid double free
      magic_ = RPC_NET_SS_MAGIC_DEAD;
      do_io_close();
      //TODO need put all request to retry
    }
  } else {
    if (OB_FAIL(setup_server_request_send())) {
      PROXY_LOG(WDIAG, "fail to call setup_server_request_send", K_(ss_id), K_(server_ip), K_(local_ip), K(ret));
    }
  }

  return VC_EVENT_NONE;
}

int ObRpcServerNetHandler::setup_retry_request(ObRpcReqList &requests)
{
  int ret = OB_SUCCESS;
  ObRpcReq *request = NULL;
  int64_t server_entry_retry_max_times = get_global_proxy_config().rpc_request_server_entry_max_retries;

  PROXY_SS_LOG(INFO, "setup_retry_request will retry", K_(ss_id), K_(server_ip), K_(local_ip), K(requests),
               K(server_entry_retry_max_times));

  if (OB_ISNULL(server_table_entry_)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_SS_LOG(WDIAG, "server_table_entry_ is NULL", K_(ss_id), K_(server_ip), K_(local_ip), K(ret));
  } else {
    while (OB_SUCC(ret) && !requests.empty()) {
      if (OB_FAIL(requests.pop_front(request))) {
        PROXY_SS_LOG(WDIAG, "fail to pop request", K_(ss_id), K_(server_ip), K_(local_ip), K(ret));
      } else if (OB_ISNULL(request)) {
        // invalid request, retry next
        PROXY_SS_LOG(WDIAG, "invalid retry request", K_(ss_id), K_(server_ip), K_(local_ip), K(ret));
      } else if (request->server_entry_send_retry_times_++ < server_entry_retry_max_times) {
        const ObRpcReqTraceId &rpc_trace_id = request->get_trace_id();
        PROXY_SS_LOG(DEBUG, "setup_retry_request will retry in waiting req arr",
                            K_(ss_id), K_(server_ip), K_(local_ip), K(request), 
                            "retry_times", request->server_entry_send_retry_times_,
                            "retry_max_times", server_entry_retry_max_times, K(rpc_trace_id));
        if (OB_FAIL(server_table_entry_->waiting_req_list_.push_back(request))) {
          PROXY_SS_LOG(WDIAG, "fail to push_back request to waiting_req_list", K_(ss_id), K_(server_ip),
                       K_(local_ip), K(request), K(ret), K(rpc_trace_id));
        }
      } else {
        const ObRpcReqTraceId &rpc_trace_id = request->get_trace_id();
        PROXY_SS_LOG(DEBUG, "setup_retry_request will retry pl lookup", K_(ss_id), K_(server_ip),
                     K_(local_ip), K(request), K(rpc_trace_id));
        ObRpcRequestSM *request_sm = NULL;
        request->set_server_failed(true);
        if (OB_ISNULL(request_sm = request->get_request_sm())) {
          ret = OB_ERR_UNEXPECTED;
          PROXY_SS_LOG(WDIAG, "invalid request sm", K_(ss_id), K_(server_ip), K_(local_ip), K(request),
                       K(ret), K(rpc_trace_id));
        } else {
          request_sm->set_retry_need_update_pl(true); // need set target dirty
          if (OB_FAIL(request_sm->schedule_call_next_action(RPC_REQ_REQUEST_RETRY))) {
            PROXY_SS_LOG(WDIAG, "fail to call schedule_call_next_action", K(ret), K(request_sm), K_(ss_id), K_(server_ip), K_(local_ip), K(rpc_trace_id));
          }
        }
      }
    }
    requests.reset();
  }

  return ret;
}

int ObRpcServerNetHandler::setup_server_request_send()
{
  int ret = OB_SUCCESS;
  bool need_add_free_server_entry = true;
  ObRpcReqList retry_request_list;
  ObRpcReqList &waiting_req_list = server_table_entry_->waiting_req_list_;
  pending_action_ = NULL;
  int64_t send_timeout = get_global_proxy_config().rpc_request_timeout;
  if (send_timeout == 0) {
    send_timeout = 5000000; //us, default 5s
  }
  PROXY_LOG(DEBUG, "ObRpcServerNetHandler::setup_server_request_send", K_(ss_id), K_(server_ip), K_(local_ip), K(this));

  if (OB_UNLIKELY(!sending_req_list_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_SS_LOG(INFO, "last setup_server_request_send is not complete, will do nothing "
                 "and recall setup_server_request_send", K_(ss_id), K_(server_ip), K_(local_ip), K_(sending_req_list), K(ret));
  } else if (OB_FAIL(calc_request_need_send(retry_request_list))) {
    PROXY_SS_LOG(WDIAG, "fail to calc request need send", K_(ss_id), K_(server_ip), K_(local_ip), K(ret));
  }

  if (OB_SUCC(ret)) {
    PROXY_SS_LOG(DEBUG, "ObRpcServerNetHandler::setup_server_request_send", "request_count", need_send_req_list_.size(),
                 K_(ss_id), K_(server_ip), K_(local_ip));
    if (OB_UNLIKELY(need_send_req_list_.empty())) {
      PROXY_SS_LOG(DEBUG, "ObRpcServerNetHandler::setup_server_request_send no request need to send",
                   K_(ss_id), K_(server_ip), K_(local_ip));
    } else {
      if (OB_ISNULL(net_entry_.write_buffer_)) {
        net_entry_.write_buffer_ = new_empty_miobuffer(MYSQL_BUFFER_SIZE);
      } else {
        net_entry_.write_buffer_->reset(); //cleanup
        net_entry_.write_buffer_->dealloc_all_readers();
      }
      ObIOBufferReader *buf_start = net_entry_.write_buffer_->alloc_reader();
      int64_t total_request_len = 0;
      int64_t send_request = 0;
      //init buf_start to mio_buffer
      // net_entry_.write_buffer_ = (buf_start != buf_reader_) ? buf_start->writer() : NULL;
      if (OB_UNLIKELY(OB_ISNULL(buf_start))) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_SS_LOG(WDIAG, "setup_server_request_send failed to allocate iobuffer reader", K_(ss_id),
                     K_(server_ip), K_(local_ip), K(ret));
      }
      while (OB_SUCC(ret) && !need_send_req_list_.empty()) {
        int64_t request_len = 0;
        char *buf = NULL;
        int64_t buf_len = 0;
        ObRpcReq *request = NULL;
        if (OB_FAIL(need_send_req_list_.pop_front(request))) {
          request = NULL;
          PROXY_SS_LOG(WDIAG, "fail to pop need send request", K_(ss_id), K_(server_ip), K_(local_ip), K(ret));
        } else if (OB_UNLIKELY(OB_ISNULL(request))) { //request has cancled() or invalid, not send again
          //invalid request, not need send it next
          PROXY_SS_LOG(INFO, "request which send server is invalid ", K_(ss_id), K_(server_ip), K_(local_ip), K(request),
                       "rpc_trace_id", request->get_trace_id());
        } else if (request->canceled()) {
          PROXY_SS_LOG(INFO, "request which send server is canced", K_(ss_id), K_(server_ip), K_(local_ip), K(request),
          "rpc_trace_id", request->get_trace_id());
          ObRpcReq::ObRpcReqCleanupParams cleanup_params(ObRpcReq::ServerNetState::RPC_REQ_SERVER_CANCLED);
          request->cleanup(cleanup_params);
        } else {
          const ObRpcReqTraceId &rpc_trace_id = request->get_trace_id();
          if (!request->is_use_request_inner_buf()) {
            buf = request->get_request_buf();
            buf_len = request->get_request_buf_len();
          } else {
            buf = request->get_request_inner_buf();
            buf_len = request->get_request_inner_buf_len();
          }
          PROXY_SS_LOG(DEBUG, "[RPC_REQUEST]sending request...", K_(ss_id), K_(server_ip), K_(local_ip), KPC(request),
                         K(buf), K(buf_len), "use_inner_buf", request->is_use_request_inner_buf(),
                        K(this), "server_addr", server_ip_, "local_addr", local_ip_, K(rpc_trace_id));
          if (OB_ISNULL(buf) || buf_len < 0) {
            ret = OB_ERR_UNEXPECTED;
            PROXY_SS_LOG(WDIAG, "request buf is invalid", K_(ss_id), K_(server_ip), K_(local_ip), K(buf), K(buf_len), K(rpc_trace_id));
          } else if (OB_FAIL(handle_request_rewrite_channel_id(request))) {
            PROXY_SS_LOG(WDIAG, "request convert channel_id failed", K_(ss_id), K_(server_ip), K_(local_ip), K(request), K(rpc_trace_id));
          } else {
            int64_t written_len = 0;
            ObRpcOBKVInfo &obkv_info = request->get_obkv_info();
            request_len = request->get_request_len();//request->get_request_buf_len();
            // todo: record cmd_time_stats and cmd_size_stats

            obkv_info.server_info_.set_addr(rpc_net_vc_->get_remote_addr());
            obkv_info.server_info_.set_obproxy_addr(rpc_net_vc_->get_local_addr());
            if (send_timeout > request->get_rpc_request()->get_packet_meta().rpc_header_.timeout_ 
                  && request->get_rpc_request()->get_packet_meta().rpc_header_.timeout_ > 0) {
              //update min send_timeout value
              send_timeout = request->get_rpc_request()->get_packet_meta().rpc_header_.timeout_;
            }

            if (OB_FAIL(net_entry_.write_buffer_->write(buf, request->get_request_len(), written_len))) {
              PROXY_SS_LOG(WDIAG, "request is not written completely, request handle failed", K_(ss_id), K_(server_ip), K_(local_ip),
                           K(written_len), K(request_len), K(request), "to_write", request->get_request_len(), K(rpc_trace_id));
            } else if (OB_UNLIKELY(request_len != written_len)) {
              ret = OB_ERR_UNEXPECTED;
              PROXY_SS_LOG(WDIAG, "request is not written completely, all request handle failed", K_(ss_id),
                           K_(server_ip), K_(local_ip), K(written_len), K(request_len), K(rpc_trace_id));
            } else if (OB_FAIL(sending_req_list_.push_back(request))) {
              //don't enqueue the request to the retry list in case duplicate execution.
              PROXY_SS_LOG(WDIAG, "request failed to push back into sending_request_list", K_(ss_id),
                           K_(server_ip), K_(local_ip), K(ret), "rpc_req", *request, K(rpc_trace_id));
            } else {
              RPC_REQ_SNET_ENTER_STATE(request, ObRpcReq::ServerNetState::RPC_REQ_SERVER_REQUST_SENDING);
              send_request++;
              total_request_len += request_len;
              PROXY_SS_LOG(DEBUG, "server need to send", K_(ss_id), K_(server_ip), K_(local_ip), K(send_request),
                           K(total_request_len), K(this), K(written_len), K(rpc_trace_id));
            }
          }
        }
        if (OB_FAIL(ret) && OB_NOT_NULL(request)) {
          retry_request_list.push_back(request);
        }
      }
      
      // push need send requests back to waiting list 
      if (OB_FAIL(ret) && !need_send_req_list_.empty()) {
        PROXY_SS_LOG(WDIAG, "need send request list is not empty", K_(ss_id), K_(server_ip), K_(local_ip), K_(need_send_req_list), K(ret));
        while (OB_SUCC(ret) && !need_send_req_list_.empty()) {
          ObRpcReq *need_send_req = NULL;
          if (OB_FAIL(need_send_req_list_.pop_front(need_send_req))) {
            PROXY_SS_LOG(WDIAG, "fail to pop need send req list", K_(ss_id), K_(server_ip), K_(local_ip), K(ret));
          } else if (OB_ISNULL(need_send_req)) {
            PROXY_SS_LOG(WDIAG, "invalid need send request", K_(ss_id), K_(server_ip), K_(local_ip), K(ret));
          } else if (OB_FAIL(retry_request_list.push_back(need_send_req))) {
            PROXY_SS_LOG(WDIAG, "fail to push  need send req to waiting list", K_(ss_id), K_(server_ip), K_(local_ip),
                         K(ret), "rpc_trace_id", need_send_req->get_trace_id());
          }
        }
      }
      if (OB_SUCC(ret)) {
        MUTEX_TRY_LOCK(lock, rpc_net_vc_->mutex_, create_thread_);
        if (!lock.is_locked()) {
          ret = OB_ERR_UNEXPECTED;
          PROXY_SS_LOG(WDIAG, "fail to lock server rpc vconnection, need retry", K_(ss_id), K_(server_ip), K_(local_ip), K(ret));
        } else if (send_request > 0) {
          if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
            write_begin_ = ObRpcRequestSM::static_get_based_hrtime(); /* record begin time to write */
          }
          if (send_timeout > 0) {
            send_timeout >>= 1; //send_timeout / 2
            send_timeout = HRTIME_USECONDS(send_timeout);
            rpc_net_vc_->set_inactivity_timeout(send_timeout);
          }
          timeout_us_ = get_global_proxy_config().rpc_net_timeout_base * total_request_len * 1000;
          if ((0 == 1) && OB_FAIL(schedule_timeout_action())) { //TODO disable to schedule timeout at here, maybe repeate handle with
                                                                // rpc_net_vc_->set_inactivity_timeout, to remove code in future(and 
                                                                // cancel_timeout_action).
            PROXY_SS_LOG(WDIAG, "fail to schedule_timeout_action", K_(ss_id), K_(server_ip), K_(local_ip), K(send_request),
                         K(total_request_len), K(ret));
          } else if (create_thread_ != mutex_->thread_holding_ 
                     || OB_ISNULL(net_entry_.write_vio_ = do_io_write(this, total_request_len, buf_start))) {
            ret = OB_ERR_UNEXPECTED;
            PROXY_SS_LOG(WDIAG, "server entry failed to do_io_write", K_(ss_id), K_(server_ip), K_(local_ip),
                         K(send_request), K(total_request_len));
          } else {
            need_add_free_server_entry = false;
            request_sent_ += send_request;
            request_pending_ += send_request;
            renew_last_send_time();
            PROXY_SS_LOG(DEBUG, "server has send", K_(ss_id), K_(server_ip), K_(local_ip), K(send_request),
                         K(total_request_len), K(this), K_(timeout_us));
          }
        } else {
          PROXY_SS_LOG(INFO, "ObRpcServerNetHandler::setup_server_request_send send request is zero",
                       K_(ss_id), K_(server_ip), K_(local_ip), K(send_request));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    // retry request
    if (retry_request_list.size() > 0 && OB_FAIL(setup_retry_request(retry_request_list))) {
      PROXY_SS_LOG(WDIAG, "fail to call setup_retry_request", K_(ss_id), K_(server_ip), K_(local_ip),
                   "waiting_req_len", waiting_req_list.size(), K(retry_request_list), K(ret));
    }
  }

  // add server entry
  if (need_add_free_server_entry) {
    if (OB_NOT_NULL(server_table_entry_)) {
      PROXY_SS_LOG(DEBUG, "server_table_entry waiting count is", K_(ss_id), K_(server_ip), K_(local_ip),
                   K(waiting_req_list.size()), K(need_send_req_list_.size()));
      if (waiting_req_list.empty() && need_send_req_list_.empty()) {
        PROXY_LOG(DEBUG, "ObRpcServerNetHandler::setup_server_request_send done, add_free_server_entry",
                  K_(ss_id), K_(server_ip), K_(local_ip), K(this));
        //not have any waiting request, just put it to free list
        server_table_entry_->add_free_server_entry(this);
      } else {
        if (OB_ISNULL(pending_action_)) {
          PROXY_LOG(DEBUG, "ObRpcServerNetHandler::setup_server_request_send done, schedule"
                    " RPC_SERVER_NET_REQUEST_SEND", K_(ss_id), K_(server_ip), K_(local_ip), K(this));
          pending_action_ = self_ethread().schedule_imm(this, RPC_SERVER_NET_REQUEST_SEND);
        } else {
          PROXY_LOG(DEBUG, "ObRpcServerNetHandler::setup_server_request_send done, do nothing",
                    K_(ss_id), K_(server_ip), K_(local_ip), K(this));
          // do nothing
        }
      }
    }
  } else {
    PROXY_LOG(DEBUG, "ObRpcServerNetHandler::setup_server_request_send done, wait write succeed",
              K_(ss_id), K_(server_ip), K_(local_ip), K(this));
  }

  return ret;
}

int ObRpcServerNetHandler::calc_request_need_send(ObRpcReqList &retry_list)
{
  int ret = OB_SUCCESS;
  // fill need_send_req_list_ by waiting_req_list or put excess reqs to waiting_req_list
  int64_t need_send_req_len = need_send_req_list_.size();
  int64_t max_request_count = get_global_proxy_config().rpc_max_request_batch_size;
  ObRpcReq *request = NULL;

  //TODO : add request bytes limite
  if (need_send_req_len > max_request_count) {
    while (OB_SUCC(ret) && need_send_req_list_.size() > max_request_count) {
      // pop back last request to waiting list
      if (OB_FAIL(need_send_req_list_.pop_back(request))) {
        PROXY_SS_LOG(WDIAG, "fail to pop need send request", K_(ss_id), K_(server_ip), K_(local_ip), K(ret));
      } else if (OB_UNLIKELY(OB_ISNULL(request))) {
        PROXY_SS_LOG(WDIAG, "request in waiting request list is invalid ", K_(ss_id), K_(server_ip));
      } else if (OB_FAIL(server_table_entry_->waiting_req_list_.push_back(request))) {
        PROXY_SS_LOG(WDIAG, "fail to enqueue request need send", K_(ss_id), K_(server_ip), K_(local_ip), K(ret),
                     "rpc_trace_id", request->get_trace_id());
        retry_list.push_back(request);
      }
    }
  } else {
    while (OB_SUCC(ret) 
             && need_send_req_list_.size() < max_request_count 
             && !server_table_entry_->waiting_req_list_.empty()) {
      if (OB_FAIL(server_table_entry_->waiting_req_list_.pop_front(request))) {
        PROXY_SS_LOG(WDIAG, "fail to pop need send request", K_(ss_id), K_(server_ip), K_(local_ip), K(ret));
      } else if (OB_UNLIKELY(OB_ISNULL(request))) {
        PROXY_SS_LOG(WDIAG, "request which send server is invalid ", K_(ss_id), K_(server_ip), K_(local_ip));
      } else if (OB_FAIL(need_send_req_list_.push_back(request))) {
        PROXY_SS_LOG(WDIAG, "fail to enqueue request need send", K_(ss_id), K_(server_ip), K_(local_ip), K(ret),
                     "rpc_trace_id", request->get_trace_id());
        retry_list.push_back(request);
      }
    }
  }
  
  if (OB_FAIL(ret)) {
    PROXY_SS_LOG(INFO, "will retry fail request", K_(ss_id), K_(server_ip), K_(local_ip),
                  "waiting_count", server_table_entry_->waiting_req_list_.size(),
                  "need_send_count", need_send_req_list_.size(), "retry_count", retry_list.size());
  }
  return ret;
}

int ObRpcServerNetHandler::save_rpc_response(ObRpcReq *rpc_req)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf_reader_) || OB_ISNULL(rpc_req)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_SS_LOG(WDIAG, "get invalid buf_reader or rpc_req", K_(ss_id), K_(server_ip), K_(local_ip), K_(buf_reader), KP(rpc_req), K(ret));
  } else {
    const ObRpcReqTraceId &rpc_trace_id = rpc_req->get_trace_id();
    int64_t response_len = current_ez_header_.ez_payload_size_ + RPC_NET_HEADER_LENGTH;

    if (OB_NOT_NULL(rpc_req->get_response_buf())
          && rpc_req->get_response_buf_len() > response_len + ObProxyRpcReqAnalyzer::OB_RPC_ANALYZE_MORE_BUFF_LEN) {
      rpc_req->inc_req_buf_repeat_times();
    } else if (OB_NOT_NULL(rpc_req->get_response_buf()) && OB_FAIL(rpc_req->free_response_buf())) {
      PROXY_SS_LOG(WDIAG, "fail to free rpc reponse object buffer, could not to be here", K_(ss_id), K_(server_ip),
                    K_(local_ip), K(ret), K(rpc_trace_id));
    } else if (OB_FAIL(rpc_req->alloc_response_buf(response_len + ObProxyRpcReqAnalyzer::OB_RPC_ANALYZE_MORE_BUFF_LEN))) {
      PROXY_SS_LOG(WDIAG, "fail to allocate rpc request object", K_(ss_id), K_(server_ip), K_(local_ip), K(ret), K(rpc_trace_id));
    }
    if (OB_SUCC(ret)) {
      // success we need copy net data to request buffer
      buf_reader_->copy(rpc_req->get_response_buf(), response_len);
      rpc_req->set_response_len(response_len);
    }
  }

  return ret;
}

int ObRpcServerNetHandler::setup_server_response_read()
{
  int ret = OB_SUCCESS;
  // if (OB_ISNULL(net_entry_.read_buffer_))
  if (OB_ISNULL(read_buffer_)) {
    ret = OB_INNER_STAT_ERROR;
    PROXY_SS_LOG(EDIAG, "invalid server response entry to handle", K_(ss_id), K_(server_ip), K_(local_ip), K(ret));
  } else if (OB_ISNULL(net_entry_.read_vio_)) {
    ret = OB_INNER_STAT_ERROR;
    PROXY_SS_LOG(EDIAG, "net entry read vio should not be NULL", K_(ss_id), K_(server_ip), K_(local_ip), K(ret));
  } else if (OB_ISNULL(net_entry_.read_vio_ = do_io_read(this, INT64_MAX, read_buffer_))) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_SS_LOG(WDIAG, "server entry failed to do_io_read", K_(ss_id), K_(server_ip), K_(local_ip), K(this), K(ret));
  } else {
    if (buf_reader_->read_avail() > 0) {
      // TODO: check level
      PROXY_SS_LOG(DEBUG, "the response already in buffer, continue to handle it", K_(ss_id), K_(server_ip), K_(local_ip),
               "buffer len", buf_reader_->read_avail());
      // handle_event(VC_EVENT_READ_READY, net_entry_.read_vio_);
      state_server_response_read((net_entry_.eos_) ? VC_EVENT_EOS : VC_EVENT_READ_READY, net_entry_.read_vio_);
    }
  }
  return ret;
}

int ObRpcServerNetHandler::state_server_response_read(int event, void *data)
{
  int ret = OB_SUCCESS;
  int event_ret = VC_EVENT_NONE;
  ObRpcReq *rpc_req = NULL;
  ObRpcRequestSM *request_sm = NULL;
  bool server_failed = false;

  STATE_ENTER(ObRpcServerNetHandler::state_server_response_read, event, data);

  if (OB_UNLIKELY(net_entry_.read_vio_ != reinterpret_cast<ObVIO *>(data))
      || (net_entry_.eos_)) {
    ret = OB_INNER_STAT_ERROR;
    PROXY_SS_LOG(WDIAG, "invalid internal state", K_(ss_id), K_(server_ip), K_(local_ip), K_(net_entry_.read_vio),
                 K(data), K_(net_entry_.eos));
  } else {
    switch (event) {
      case VC_EVENT_READ_READY:
      case VC_EVENT_READ_COMPLETE:
        // More data to fill request
        break;
      case VC_EVENT_EOS: {
        net_entry_.eos_ = true;
        PROXY_SS_LOG(INFO, "ObRpcServerNetHandler::state_server_response_read",
                 "event", ObRpcReqDebugNames::get_event_name(event),
                 K_(ss_id), K_(server_ip), K_(local_ip), "server_vc", P(this->get_netvc()));
        break;
      }
      case VC_EVENT_INACTIVITY_TIMEOUT:
      case VC_EVENT_ACTIVE_TIMEOUT:
      case VC_EVENT_ERROR: {
        PROXY_SS_LOG(WDIAG, "ObRpcServerNetHandler::state_server_response_read",
                 "event", ObRpcReqDebugNames::get_event_name(event),
                 K_(ss_id), K_(server_ip), K_(local_ip), "server_vc", P(this->get_netvc()));
        server_failed = true;
        ret = OB_CONNECT_ERROR;
        // The server is closed. Close it.
        break;
      }
      default:
        ret = OB_INNER_STAT_ERROR;
        PROXY_SS_LOG(EDIAG, "unexpected event", K_(ss_id), K_(server_ip), K_(local_ip), K(event), K(ret));
        break;
    }

    /* 5. read data from vc buffer and init ObRpcReq */
    if (OB_SUCC(ret) && !net_entry_.eos_) {
      if (OB_ISNULL(get_reader())) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_SS_LOG(WDIAG, "reader is NULL", K_(ss_id), K_(server_ip), K_(local_ip), K(ret));
      } else {
        // ObRpcReq *request = NULL;
        ObRpcReqReadStatus status = RPC_REQUEST_READ_CONT;
        ObIOBufferReader &buffer_reader = *get_reader();
        int64_t pos = 0;
        int64_t request_len = 0;
        uint32_t request_id = 0;
        char *written_pos = NULL;
        obkv::ObProxyRpcType rpc_type = obkv::OBPROXY_RPC_UNKOWN;

        if (read_begin_ == 0 && OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
          read_begin_ = ObRpcRequestSM::static_get_based_hrtime();
        }

        if (buffer_reader.read_avail() < current_need_read_len_) { //need handle the rpc header
          PROXY_SS_LOG(DEBUG, "response not ready, need get next", K_(ss_id), K_(server_ip), K_(local_ip), K_(current_need_read_len),
                          "data_len", buffer_reader.read_avail());
          status = RPC_REQUEST_READ_CONT;
        } else {
          if (RPC_NET_HEADER_LENGTH == current_need_read_len_) {
            written_pos = buffer_reader.copy(net_head_buf_, RPC_NET_HEADER_LENGTH, 0);
            if (OB_UNLIKELY(written_pos != net_head_buf_ + RPC_NET_HEADER_LENGTH)) {
              ret = OB_ERR_UNEXPECTED;
              PROXY_SS_LOG(WDIAG, "not copy completely", K_(ss_id), K_(server_ip), K_(local_ip),
                      K(written_pos), K(net_head_buf_),
                      "meta_length", RPC_NET_HEADER_LENGTH, K(ret));
            } else if (OB_FAIL(current_ez_header_.deserialize(net_head_buf_, RPC_NET_HEADER_LENGTH, pos))) {
              PROXY_SS_LOG(WDIAG, "fail to deserialize ObRpcEzHeader", K_(ss_id), K_(server_ip), K_(local_ip),
                      K(written_pos), K(net_head_buf_),
                      "meta_length", RPC_NET_HEADER_LENGTH, K(ret));
            } else if (obkv::OBPROXY_RPC_OBRPC != (rpc_type = current_ez_header_.get_rpc_magic_type())) {
              ret = OB_ERR_UNEXPECTED;
              PROXY_SS_LOG(WDIAG, "get an unsupported rpc type", K_(ss_id), K_(server_ip), K_(local_ip),
                      K(rpc_type), K(ret));
            } else {
              current_need_read_len_ = current_ez_header_.ez_payload_size_ + RPC_NET_HEADER_LENGTH;
            }
          }
          if (OB_SUCC(ret) && RPC_NET_HEADER_LENGTH != current_need_read_len_) {
            request_len = current_need_read_len_;
            request_id = current_ez_header_.chid_;

            if (buffer_reader.read_avail() < request_len) {
              // request not completely got from net, next to read it when meet condition
              PROXY_SS_LOG(DEBUG, "response not ready, need get next", K_(ss_id), K_(server_ip), K_(local_ip), K(request_len),
                          "data_len", buffer_reader.read_avail());
              status = RPC_REQUEST_READ_CONT;
            } else {
              PROXY_SS_LOG(DEBUG, "client request need to handle", K_(ss_id), K_(server_ip), K_(local_ip), K(this), K(cid_to_req_map_.size()),
                                  "data_len", buffer_reader.read_avail(), "request_len", request_len);

              if (OB_FAIL(cid_to_req_map_.erase_refactored(request_id, &rpc_req))) {
                if (OB_HASH_NOT_EXIST == ret) {
                  status = RPC_REQUEST_READ_DONE;
                  PROXY_SS_LOG(DEBUG, "client request has been delete maybe timeout", K_(ss_id), K_(server_ip), K_(local_ip), K(this), K(rpc_req),
                              K(ret), K(request_id), K(cid_to_req_map_.size()));
                  ret = OB_SUCCESS;
                } else {
                  PROXY_SS_LOG(WDIAG, "fail to call erase_refactored", K_(ss_id), K_(server_ip), K_(local_ip), K(this), K(ret), K(request_id),
                              K(cid_to_req_map_.size()));
                }
              } else if (rpc_req->canceled()) {
                status = RPC_REQUEST_READ_DONE;
              } else {
                const ObRpcReqTraceId &rpc_trace_id = rpc_req->get_trace_id();
                PROXY_SS_LOG(DEBUG, "[RPC_REQUEST]client request has found to handle", K_(ss_id), K_(server_ip), K_(local_ip), KPC(rpc_req),
                              "count", cid_to_req_map_.size(), K(this), "server_addr", server_ip_, K(rpc_trace_id));

                if (OB_FAIL(save_rpc_response(rpc_req))) {
                  PROXY_SS_LOG(WDIAG, "fail to call save_rpc_response", K_(ss_id),
                              K_(server_ip), K_(local_ip), K(rpc_req), K(ret), K(rpc_trace_id));
                } else {
                  // success we need copy net data to request buffer
                  status = RPC_REQUEST_READ_DONE;
                  RPC_REQ_SNET_ENTER_STATE(rpc_req, ObRpcReq::ServerNetState::RPC_REQ_SERVER_RESPONSE_READED);
                  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
                    rpc_req->server_timestamp_.server_read_begin_ = read_begin_;
                    rpc_req->server_timestamp_.server_read_end_ = ObRpcRequestSM::static_get_based_hrtime();
                  }
                  PROXY_SS_LOG(DEBUG, "has init response of rpc response ", K_(ss_id), K_(server_ip), K_(local_ip), K(ret), K(rpc_trace_id));
                }
              }
              buffer_reader.consume(request_len);   //clean the net data of response no matter it is has rpc_req
            }
          }
        }

        if (OB_FAIL(ret)) {
          status = RPC_REQUEST_READ_ERROR;
        }

        switch (__builtin_expect(status, RPC_REQUEST_READ_DONE)) {
          case RPC_REQUEST_READ_DONE:
            // handle server net
            net_entry_.read_vio_->nbytes_ = INT64_MAX;
            // buffer_reader.mbuf_->water_mark_ = RPC_NET_HEADER_LENGTH; //do next response
            net_entry_.read_vio_->reenable(); //need check next data
            renew_last_recv_time();
            renew_handler_last_active_time();
            request_pending_--;
            first_send_time_us_ = 0;
            current_need_read_len_ = RPC_NET_HEADER_LENGTH;
            current_ez_header_.reset();
            if (request_pending_ == 0) {
              last_recv_time_us_ = 0;
              last_send_time_us_ = 0;
              PROXY_SS_LOG(DEBUG, "all pending request handled, update recv_time and send_time", K_(ss_id), K_(server_ip));
            }
            if (buffer_reader.read_avail() > 0) {
              PROXY_SS_LOG(DEBUG, "need read next result immediately when resultset waiting", K_(ss_id), K_(server_ip), K_(local_ip),
                           "net_len", buffer_reader.read_avail());
              self_ethread().schedule_imm(this, RPC_SERVER_NET_RESPONSE_RECVD); //handle the next result
            }
            if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
              /* maybe request retry, just calc last request handle */
              read_begin_ = 0; //reset it to read next
            }

            // handle rpc req
            if (OB_ISNULL(rpc_req)) {
              // can not be here, not return error ,just report
              PROXY_SS_LOG(WDIAG, "rpc_req is NULL", K_(ss_id), K_(server_ip), K_(local_ip), K(ret), KPC(rpc_req));
            } else {
              const ObRpcReqTraceId &rpc_trace_id = rpc_req->get_trace_id();
              if (rpc_req->canceled()) {
                PROXY_SS_LOG(INFO, "client request has been canceled", K_(ss_id), K_(server_ip), K_(local_ip), K(this), "rpc_req", *rpc_req,
                            K(request_id), K(rpc_trace_id));
                ObRpcReq::ObRpcReqCleanupParams cleanup_params(ObRpcReq::ServerNetState::RPC_REQ_SERVER_CANCLED);
                rpc_req->cleanup(cleanup_params);
              } else {
                if (OB_NOT_NULL(request_sm = reinterpret_cast<ObRpcRequestSM *>(rpc_req->sm_))
                    && rpc_req->get_origin_channel_id() == request_sm->get_rpc_req_origin_channel_id()) {
                  RPC_REQ_SNET_ENTER_STATE(rpc_req, ObRpcReq::ServerNetState::RPC_REQ_SERVER_DONE);
                  // Cancel timeout events in time to prevent timeouts from triggering when processing return packets
                  request_sm->cancel_timeout_action();
                  if (OB_FAIL(request_sm->schedule_call_next_action(RPC_REQ_PROCESS_RESPONSE))) {
                    PROXY_SS_LOG(WDIAG, "fail to call schedule_call_next_action", K(ret), K(request_sm), K_(ss_id), K_(server_ip), K_(local_ip), K(rpc_trace_id));
                  }
                } else {
                  PROXY_SS_LOG(WDIAG, "client request is null or maybe be cleaned due to canceled", K_(ss_id),
                           K_(server_ip), K_(local_ip), K(ret), KPC(rpc_req));
                }
              }
            }
            break;
          case RPC_REQUEST_READ_CONT:
            renew_handler_last_active_time();
            if (net_entry_.eos_) {
              ret = OB_CONNECT_ERROR;
              PROXY_SS_LOG(WDIAG, "EOS before client request parsing finished", K_(ss_id), K_(server_ip), K_(local_ip), K(ret));
              //TODO PRPC client need abort and broken connection
              net_entry_.read_vio_->nbytes_ = net_entry_.read_vio_->ndone_;
            } else {
              if (request_len > 0 && request_len > buffer_reader.mbuf_->water_mark_) {
                buffer_reader.mbuf_->water_mark_ = request_len;
              }

              net_entry_.read_vio_->reenable(); //need more data
              event_ret = VC_EVENT_CONT;
            }
            break;
          case RPC_REQUEST_READ_ERROR:
            ret = OB_ERR_UNEXPECTED;
            PROXY_SS_LOG(WDIAG, "error parsing client request", K_(ss_id), K_(server_ip), K_(local_ip), K(ret));
            // set_client_abort(ObRpcTransact::ABORTED, event);

            // Disable further I/O on the client
            net_entry_.read_vio_->nbytes_ = net_entry_.read_vio_->ndone_;
            break;
          default:
            ret = OB_INNER_STAT_ERROR;
            PROXY_SS_LOG(EDIAG, "unknown analyze mysql request status", K_(ss_id), K_(server_ip), K_(local_ip), K(status), K(ret));
            break;
        }
      }
    }
  }

  if (OB_FAIL(ret) || net_entry_.eos_) {
    PROXY_SS_LOG(WDIAG, "rpc server net receieve error enent", K_(ss_id), K_(server_ip), K_(local_ip),
                 K(ret), K(server_failed), "eos", net_entry_.eos_, K(event));
    // If an error is reported, no longer clear the rpc req, wait for timeout or server entry disconnection processing
    do_io_close();
  }

  return event_ret;
}


int ObRpcServerNetHandler::handle_net_event(int event, void *data)
{
  int ret = OB_SUCCESS;
  int event_ret = VC_EVENT_NONE;
  PROXY_SS_LOG(DEBUG, "ObRpcServerNetHandler::handle_net_event", K_(ss_id), K_(server_ip), K_(local_ip), K(event), K(data));
  if (data == this->net_entry_.read_vio_) {
    PROXY_SS_LOG(DEBUG, "ObRpcServerNetHandler::handle_net_event handle response read", K_(ss_id), K_(server_ip), K_(local_ip),
                 K(event), K(data));
    event_ret = state_server_response_read(event, data);
  } else if (data == this->net_entry_.write_vio_) {
    PROXY_SS_LOG(DEBUG, "ObRpcServerNetHandler::handle_net_event handle request send success", K_(ss_id),
                 K_(server_ip), K_(local_ip), K(event), K(data));
    event_ret = state_server_request_send(event, data); //request has end server success
  } else {
    ret = OB_INNER_STAT_ERROR;
    PROXY_SS_LOG(WDIAG, "invalid internal state", K_(ss_id), K_(server_ip), K_(local_ip), K(event), K(data), K(this), K(ret));
  }

  return event_ret;
}

int ObRpcServerNetHandler::state_server_request_send(int event, void *data)
{
  int ret = OB_SUCCESS;
  uint32_t key = 0;
  ObRpcReq *request = NULL;
  bool need_add_free_server_entry = true;
  bool server_failed = false;
  PROXY_SS_LOG(DEBUG, "ObRpcServerNetHandler::state_server_request_send handle event", K_(ss_id),
               K_(server_ip), K_(local_ip), K(event), K(data), K(this));

  if (OB_FAIL(cancel_timeout_action())) {
    PROXY_SS_LOG(WDIAG, "fail to cancel_timeout_action", K_(ss_id), K_(server_ip), K_(local_ip), K(ret));
  } else {
    switch (event) {
      case VC_EVENT_WRITE_READY:
      {
        need_add_free_server_entry = false;
        net_entry_.write_vio_->reenable();
        break;
      }
      case VC_EVENT_WRITE_COMPLETE:
      {
        /* buffer write os net buffer successfully will return this event, not means endpoint could 
         * receive data success, maybe endpoint not receive and return any data for isolated.
         * so will need to find this situation. it meet the condition of 'is_invalid_server_net_handle(),
         * will fix this question, so need send 10 rpc request default to find server is down */
        renew_handler_last_active_time();
        renew_first_send_time();
        ObHRTime write_done = 0;
        if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
          write_done = ObRpcRequestSM::static_get_based_hrtime();
        }
        rpc_net_vc_->set_inactivity_timeout(0); //cancel sending error
        while (OB_SUCC(ret) && !sending_req_list_.empty()) {
          if (OB_FAIL(sending_req_list_.pop_front(request))) {
            PROXY_SS_LOG(WDIAG, "fail to pop sending request", K_(ss_id), K_(server_ip), K_(local_ip), K(ret));
          } else if (OB_ISNULL(request)) {
            PROXY_SS_LOG(DEBUG, "ObRpcServerNetHandler::state_server_request_send request is NULL",
                         K_(ss_id), K_(server_ip), K_(local_ip), K(event), K(data), K(ret));
          } else {
            const ObRpcReqTraceId &rpc_trace_id = request->get_trace_id();            
            if (OB_UNLIKELY(request->canceled())) {
              PROXY_SS_LOG(INFO, "ObRpcServerNetHandler::state_server_request_send request has canceled",
                           K_(ss_id), K_(server_ip), K_(local_ip), K(event), K(data), K(rpc_trace_id));
              ObRpcReq::ObRpcReqCleanupParams cleanup_params(ObRpcReq::ServerNetState::RPC_REQ_SERVER_CANCLED);
              request->cleanup(cleanup_params);
            } else {
              RPC_REQ_SNET_ENTER_STATE(request, ObRpcReq::ServerNetState::RPC_REQ_SERVER_REQUST_SENDED);
              key = request->get_server_channel_id();
              request->server_timestamp_.server_write_begin_ = write_begin_;
              request->server_timestamp_.server_write_end_ = write_done;
              if (OB_FAIL(cid_to_req_map_.set_refactored(key, request))) {
                  // cancel this request and try to handle next request
                  PROXY_SS_LOG(WDIAG, "fail to call set_refactored", K_(ss_id), K_(server_ip), K_(local_ip), K(request),
                               K(key), K(rpc_trace_id));
                  request->server_net_cancel_request();
                  ObRpcReq::ObRpcReqCleanupParams cleanup_params(ObRpcReq::ServerNetState::RPC_REQ_SERVER_CANCLED);
                  request->cleanup(cleanup_params);
                  ret = OB_SUCCESS;
              } else {
                PROXY_SS_LOG(DEBUG, "ObRpcServerNetHandler::state_server_request_send put it to cid_ot_req_map",
                             K_(ss_id), K_(server_ip), K_(local_ip), K(this), K(event), 
                             K(data), K(cid_to_req_map_.size()), K(request), K(key), K(rpc_trace_id));
              }
            }
          }
        }
        break;
      }
      case VC_EVENT_INACTIVITY_TIMEOUT:
      case VC_EVENT_ACTIVE_TIMEOUT:
      case VC_EVENT_ERROR: {
        PROXY_SS_LOG(WDIAG, "ObRpcServerNetHandler::state_server_request_send",
                 K_(ss_id), K_(server_ip), K_(local_ip), "server_vc", P(this->get_netvc()));
        server_failed = true;
        ret = OB_CONNECT_ERROR;
        // The server is closed. Close it.
        break;
      }
      case VC_EVENT_EOS: { //maybe server net has broken in any state(building connection / send / receive)
        // TODO: handle eos, clean and destroy entry
        need_add_free_server_entry = false;
        eos_ = true;
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        PROXY_SS_LOG(WDIAG, "ObRpcServerNetHandler::state_server_request_send unknown event", K_(ss_id),
                     K_(server_ip), K_(local_ip), K(event), K(data), K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    PROXY_SS_LOG(WDIAG, "ObRpcServerNetHandler::state_server_request_send write complete but error, clean requests and map",
                K_(ss_id), K_(server_ip), K_(local_ip), K_(sending_req_list), K(event), K(data));
    int tmp_ret = ret;
    ret = OB_SUCCESS;
    // clean rpc_req and map
    while (OB_SUCC(ret) && !sending_req_list_.empty()) {
      if (OB_FAIL(sending_req_list_.pop_front(request))) {
        PROXY_SS_LOG(WDIAG, "fail to pop request from sending request list", K_(ss_id), K_(server_ip), K_(local_ip), K(ret));
      } else if (OB_NOT_NULL(request)) {
        PROXY_SS_LOG(INFO, "ObRpcServerNetHandler::state_server_request_send write error cleanup", KPC(request),
                           K_(ss_id), K_(server_ip), K_(local_ip), K(event), K(data));
        request->server_net_cancel_request();
        ObRpcReq::ObRpcReqCleanupParams cleanup_params(ObRpcReq::ServerNetState::RPC_REQ_SERVER_CANCLED);
        request->cleanup(cleanup_params);
      }
    }
    sending_req_list_.reset();
    ret = tmp_ret;
  } else {
    // for VC_EVENT_WRITE_READY, reenable and wait WRITE COMPLETE
    if (VC_EVENT_WRITE_READY != event) {
      sending_req_list_.reset();
    }
  }

  // Some error situations also need to be fully released
  if (need_add_free_server_entry) {
    if (OB_ISNULL(server_table_entry_)) {
      //could not be here, need to free server table entry
      ret = OB_ERR_UNEXPECTED;
      PROXY_SS_LOG(WDIAG, "server_table_entry is NULL could not be here", K_(ss_id), K_(server_ip), K_(local_ip), K(ret));
    } else {
      PROXY_SS_LOG(DEBUG, "server_table_entry waiting count is", K_(ss_id), K_(server_ip), K_(local_ip),
                  "waiting_request_list_size", server_table_entry_->waiting_req_list_.size());
      if (server_table_entry_->waiting_req_list_.empty() && need_send_req_list_.empty()) {
        PROXY_LOG(DEBUG, "ObRpcServerNetHandler::state_server_request_send done, add_free_server_entry",
                  K_(ss_id), K_(server_ip), K_(local_ip), K(this));
        server_table_entry_->add_free_server_entry(this);
      } else {
        //schdulue it to send next request, not direct to send them to avoid thread handle it all the time
        if (OB_ISNULL(pending_action_)) {
          PROXY_LOG(DEBUG, "ObRpcServerNetHandler::state_server_request_send done, schedule"
                    " RPC_SERVER_NET_REQUEST_SEND", K_(ss_id), K_(server_ip), K_(local_ip), K(this));
          pending_action_ = self_ethread().schedule_imm(this, RPC_SERVER_NET_REQUEST_SEND);
        } else {
          PROXY_LOG(DEBUG, "ObRpcServerNetHandler::state_server_request_send done, do nothing",
                    K_(ss_id), K_(server_ip), K_(local_ip), K(this));
          // do nothing
        }
      }
    }
  } else {
    PROXY_LOG(DEBUG, "ObRpcServerNetHandler::state_server_request_send done, need_add_free_server_entry",
              K_(ss_id), K_(server_ip), K_(local_ip), K(this));
  }

  if (OB_FAIL(ret) || net_entry_.eos_) {
    PROXY_SS_LOG(WDIAG, "rpc server net receieve error event",  K_(ss_id), K_(server_ip), K_(local_ip),
                 K(ret), K(server_failed), "eos", net_entry_.eos_, K(event));
    // If an error is reported, no longer clear the rpc req, wait for timeout or server entry disconnection processing
    do_io_close();
  }

  return ret;
}

int ObRpcServerNetHandler::handle_common_event(int event, void *data)
{
  // int ret = OB_SUCCESS;
  PROXY_SS_LOG(DEBUG, "[ObRpcServerNetHandler::handle_common_event]",
               K_(ss_id), K_(server_ip), K_(local_ip), K(event), K(data));
  int event_ret = VC_EVENT_NONE;
  UNUSED(data); //data maybe NULL
  switch(event) {
    case RPC_SERVER_NET_REQUEST_SEND:
    {
      event_ret = setup_server_request_send();
      break;
    }
    case RPC_SERVER_NET_RESPONSE_RECVD:
    {
      event_ret = setup_server_response_read();
      break;
    }
    case RPC_SERVER_NET_PERIOD_TASK:
    {
      event_ret = handle_period_task();
      break;
    }
    default:
      break;
  }

  return event_ret;
}

void ObRpcServerNetHandler::reenable(ObVIO *vio)
{
  rpc_net_vc_->reenable(vio);
}

int ObRpcServerNetHandler::main_handler(int event, void *data)
{
  int event_ret = VC_EVENT_CONT;
  PROXY_SS_LOG(DEBUG, "[ObRpcServerNetHandler::main_handler]",
            K_(ss_id), K_(server_ip), K_(local_ip),
            K(event),
            "event_name", ObRpcReqDebugNames::get_event_name(event),
            "read_state", get_read_state_str(), K(data), K(this),
            "this_thread", this_ethread());

  if (OB_LIKELY(RPC_NET_SS_MAGIC_ALIVE == magic_)) {
    if (EVENT_INTERVAL == event) {/* schedule timeout*/
      event_ret = handle_timeout();
    } else if (event < VC_EVENT_EVENTS_START + 100) {
      //handle net event
      event_ret = handle_net_event(event, data);
    } else if (event < NET_EVENT_OPEN + 100) {
      event_ret = state_server_new_connection(event, data);
    } else {
      event_ret = handle_common_event(event, data);
    }
  } else {
    PROXY_SS_LOG(WDIAG, "unexpected magic, expected RPC_NET_SS_MAGIC_ALIVE",
                 K_(ss_id), K_(server_ip), K_(local_ip), K(magic_));
  }

  return event_ret;
}

int ObRpcServerNetHandler::handle_server_entry_setup_error(int event, void *data)
{
  int ret = OB_SUCCESS;
  UNUSED(event);
  UNUSED(data);

  return ret;
}

int ObRpcServerNetHandler::handle_request_rewrite_channel_id(ObRpcReq *request)
{
  int ret = OB_SUCCESS;
  uint32_t channel_id = atomic_channel_id_++;

  int64_t pos = ObRpcEzHeader::RPC_PKT_CHANNEL_ID_POS;
  char *buf = request->get_request_buf();
  int64_t buf_len = request->get_request_buf_len();
  if (request->is_use_request_inner_buf()) {
    buf = request->get_request_inner_buf();
    buf_len = request->get_request_inner_buf_len();
  }

  PROXY_SS_LOG(DEBUG, "ObRpcServerNetHandler handle_request_rewrite_channel_id ", K_(ss_id), K_(server_ip),
               K_(local_ip), K(channel_id), K_(cs_id), K(this), "rpc_trace_id", request->get_trace_id());
  if (OB_FAIL(common::serialization::encode_i32(buf, buf_len, pos, channel_id))) {
    PROXY_SS_LOG(WDIAG, "ObRpcServerNetHandler handle ", K_(ss_id), K_(server_ip), K_(local_ip),
                "rpc_trace_id", request->get_trace_id());
  } else {
    request->get_obkv_info().server_request_id_ = channel_id;
    request->set_server_channel_id(channel_id);
  }

  return ret;
}

int ObRpcServerNetHandler::handle_response_recover_channel_id(ObRpcReq *request, uint32_t origin_id)
{
  int ret = OB_SUCCESS;
  int64_t pos = ObRpcEzHeader::RPC_PKT_CHANNEL_ID_POS;
  //ASSERT request not NULL 

  //response not use inner_req_buf any more
  if (OB_FAIL(common::serialization::encode_i32(request->get_request_buf(), request->get_request_buf_len(),
                                                  pos, origin_id))) {
    PROXY_SS_LOG(WDIAG, "ObRpcServerNetHandler handle ", K_(ss_id), K_(server_ip), K_(local_ip),
                 "rpc_trace_id", request->get_trace_id());
  } else {
    request->set_origin_channel_id(origin_id);
  }

  return ret;
}

const char *ObRpcServerNetHandler::get_read_state_str() const
{
  const char *states[MCS_MAX + 1] = {"MCS_INIT",
                                     "MCS_ACTIVE_READER",
                                     "MCS_KEEP_ALIVE",
                                     "MCS_HALF_CLOSED",
                                     "MCS_CLOSED",
                                     "MCS_MAX"};
  return states[read_state_];
}

int64_t ObRpcServerNetHandler::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this),
       K_(vc_ready_killed),
       K_(active),
       K_(magic),
       K_(conn_decrease),
       K_(current_tid),
       K_(ss_id),
       K_(server_ip),
       K_(local_ip),
       K_(proxy_sessid),
       K_(need_send_req_list),
       K_(sending_req_list),
       K_(server_connect_retry_times),
       K_(first_send_time_us),
       K_(last_send_time_us),
       K_(last_recv_time_us),
       K_(last_active_time_us),
       K_(request_sent),
       K_(request_pending),
       KP_(rpc_net_vc)
       );
  J_OBJ_END();
  return pos;
}

inline bool ObRpcServerNetHandler::need_close() const
{
  bool bret = false;
  const ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
  if (OB_UNLIKELY(info.graceful_exit_start_time_ > 0)
      && OB_LIKELY(!info.need_conn_accept_)
      && info.active_client_vc_count_ > 0
      && OB_LIKELY(info.graceful_exit_end_time_ > info.graceful_exit_start_time_)) {
    int64_t current_active_count = 0;
    NET_READ_GLOBAL_DYN_SUM(NET_GLOBAL_CLIENT_CONNECTIONS_CURRENTLY_OPEN, current_active_count);
    const ObHRTime remain_time = info.graceful_exit_end_time_ - get_hrtime();
    const ObHRTime total_time = info.graceful_exit_end_time_ - info.graceful_exit_start_time_;
    //use CEIL way
    const int64_t need_active_count = 
      static_cast<int64_t>((remain_time * info.active_client_vc_count_ + total_time - 1) / total_time);
    if (remain_time < 0) {
      bret = true;
      PROXY_SS_LOG(INFO, "client need force close", K_(ss_id), K_(server_ip), K_(local_ip), K(current_active_count),
                   "remain_time(ms)", hrtime_to_msec(remain_time));
    } else if (current_active_count > need_active_count) {
      bret = true;
      PROXY_SS_LOG(INFO, "client need orderly close", K_(ss_id), K_(server_ip), K_(local_ip), K(current_active_count),
                   K(need_active_count), "remain_time(ms)", hrtime_to_msec(remain_time));
    } else {/*do nothing*/}
  }
  return bret;
}

int ObRpcServerNetHandler::cancel_timeout_action()
{
  int ret = common::OB_SUCCESS;
  if (NULL != timeout_action_) {
    if (OB_FAIL(timeout_action_->cancel())) {
      PROXY_LOG(WDIAG, "fail to cancel timeout action", K_(ss_id), K_(server_ip), K_(local_ip), K_(timeout_action), K(ret));
    } else {
      timeout_action_ = NULL;
    }
  }
  return ret;
}

int ObRpcServerNetHandler::cancel_pending_action()
{
  int ret = common::OB_SUCCESS;
  if (NULL != pending_action_) {
    if (OB_FAIL(pending_action_->cancel())) {
      PROXY_LOG(WDIAG, "fail to cancel pending action", K_(ss_id), K_(server_ip), K_(local_ip), K_(pending_action), K(ret));
    } else {
      pending_action_ = NULL;
    }
  }
  return ret;
}

int ObRpcServerNetHandler::schedule_timeout_action()
{
  int ret = OB_SUCCESS;
  int64_t timeout_ns = HRTIME_USECONDS(timeout_us_);

  if (OB_UNLIKELY(NULL != timeout_action_)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_SS_LOG(WDIAG, "timeout_action must be NULL here", K_(ss_id), K_(server_ip), K_(local_ip),
                 K_(timeout_action), K(ret));
  } else if (OB_UNLIKELY(0 == timeout_ns)) {
    // do nothing
  } else if (OB_ISNULL(timeout_action_ = self_ethread().schedule_in(this, timeout_ns))) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_SS_LOG(EDIAG, "fail to schedule timeout", K_(ss_id), K_(server_ip), K_(local_ip), K(timeout_action_), K(ret));
  }

  return ret;
}

int ObRpcServerNetHandler::handle_timeout()
{
  // timeout, clear and free this server net handler  
  int ret = OB_SUCCESS;

  PROXY_SS_LOG(WDIAG, "ObRpcServerNetHandler get timeout event", K_(ss_id), K_(server_ip), K_(local_ip), K_(timeout_us),
               "send_request_size", timeout_us_ / get_global_proxy_config().rpc_net_timeout_base);

  // 1. retry all request
  if (!sending_req_list_.empty()) {
    if (OB_FAIL(setup_retry_request(sending_req_list_))) {
      PROXY_SS_LOG(WDIAG, "fail to call setup_retry_request", K_(ss_id), K_(server_ip), K_(local_ip), K(ret),
                   K_(sending_req_list));
    } else if (OB_NOT_NULL(server_table_entry_) && !server_table_entry_->waiting_req_list_.empty()) {
      // schdule retry
      if (OB_FAIL(server_table_entry_->schedule_send_request_action())) {
        PROXY_SS_LOG(WDIAG, "ObRpcServerNetHandler handle timeout to schedule server table entry failed",
          K_(ss_id), K_(server_ip), K_(local_ip), K(ret), KP(this));
      }
    }
  }

  // clear
  do_io_close();
  return ret;
}

int ObRpcServerNetHandler::schedule_period_task()
{
  int ret = OB_SUCCESS;
  ObHRTime period_task_time = HRTIME_USECONDS(get_global_proxy_config().rpc_period_task_interval);

  if (OB_UNLIKELY(NULL != period_task_action_)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_LOG(WDIAG, "period_task_action must be NULL here", K_(ss_id), K_(server_ip), K_(local_ip),
              K_(period_task_action), K(ret));
  } else if (OB_ISNULL(create_thread_) || create_thread_ != this_ethread()) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_LOG(WDIAG, "ObRpcServerNetHandler::schedule_period_task get wrong thread", K_(ss_id),
              K_(server_ip), K_(local_ip), KP_(create_thread), KP(this_ethread()));
  } else if (OB_ISNULL(period_task_action_ = 
                       self_ethread().schedule_every(this, period_task_time, RPC_SERVER_NET_PERIOD_TASK))) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_LOG(EDIAG, "fail to schedule timeout", K_(ss_id), K_(server_ip), K_(local_ip), K(period_task_action_), K(ret));
  } else {
    PROXY_LOG(DEBUG, "succ to schedule repeat task for ObRpcServerNetHandler", K_(ss_id), K_(server_ip),
              K_(local_ip), K(period_task_time));
  }

  return ret;
}

int ObRpcServerNetHandler::cancel_period_task()
{
  int ret = OB_SUCCESS;

  if (NULL != period_task_action_) {
    if (OB_FAIL(period_task_action_->cancel())) {
      PROXY_LOG(WDIAG, "fail to cancel repeat task", K_(ss_id), K_(server_ip), K_(local_ip), K_(period_task_action), K(ret));
    } else {
      period_task_action_ = NULL;
    }
  }

  return ret;
}

int ObRpcServerNetHandler::handle_period_task()
{
  int ret = OB_SUCCESS;
  PROXY_LOG(DEBUG, "ObRpcServerNetHandler::handle_period_task",
            K_(ss_id), K_(server_ip), K_(local_ip),
            "need_send_count", need_send_req_list_.size(),
            "sending_count", sending_req_list_.size(), K(last_active_time_us_));
  // 1. 清理超时请求
  clean_all_timeout_request();
  // 2. 检查server net handler 的过期状态
  int64_t expire_time = get_global_proxy_config().rpc_server_net_handler_expire_time;
  if (is_server_net_handler_expired(expire_time)) {
    PROXY_SS_LOG(INFO, "server net handler is expired, close it", K_(ss_id),
                 K_(server_ip), K_(local_ip), KPC(this));
    do_io_close();
  }
  return ret;
}

void ObRpcServerNetHandler::clean_all_pending_request()
{
  ObRpcReq *rpc_req = NULL;

  if (!need_send_req_list_.empty()) {
    PROXY_LOG(DEBUG, "ObRpcServerNetHandler::clean_all_pending_request clean request_need_send", K_(ss_id),
              K_(server_ip), K_(local_ip), "need_send_count", need_send_req_list_.size());
    ObRpcReqList::iterator iter = need_send_req_list_.begin();
    ObRpcReqList::iterator end = need_send_req_list_.end();
    for (; iter != end; iter ++) {    
      rpc_req = *iter;
      if (OB_NOT_NULL(rpc_req)) {
        PROXY_LOG(INFO, "ObRpcServerNetHandler::clean_all_pending_request clean request", K_(ss_id),
              KPC(rpc_req), K_(server_ip), K_(local_ip), "need_send_count", need_send_req_list_.size());
        rpc_req->set_server_failed(true);
        rpc_req->server_net_cancel_request();  // server net done
        ObRpcReq::ObRpcReqCleanupParams cleanup_params(ObRpcReq::ServerNetState::RPC_REQ_SERVER_CANCLED);
        rpc_req->cleanup(cleanup_params);
        rpc_req = NULL;
      }
    }
  }
  need_send_req_list_.reset();

  if (!sending_req_list_.empty()) {
    PROXY_LOG(DEBUG, "ObRpcServerNetHandler::clean_all_pending_request clean request_is_sending", K_(ss_id),
              K_(server_ip), K_(local_ip), "sending_count", sending_req_list_.size());
    ObRpcReqList::iterator iter = sending_req_list_.begin();
    ObRpcReqList::iterator end = sending_req_list_.end();
    for (; iter != end; iter ++) {
      rpc_req = *iter;
      if (OB_NOT_NULL(rpc_req)) {
        PROXY_LOG(INFO, "ObRpcServerNetHandler::clean_all_pending_request clean request", K_(ss_id),
            KPC(rpc_req), K_(server_ip), K_(local_ip), "need_send_count", sending_req_list_.size());
        rpc_req->set_server_failed(true);
        rpc_req->server_net_cancel_request();  // server net done
        ObRpcReq::ObRpcReqCleanupParams cleanup_params(ObRpcReq::ServerNetState::RPC_REQ_SERVER_CANCLED);
        rpc_req->cleanup(cleanup_params);
        rpc_req = NULL;
      }
    }
  }
  sending_req_list_.reset();

  // clean map
  if (0 != cid_to_req_map_.size()) {
    PROXY_LOG(DEBUG, "ObRpcServerNetHandler::clean_all_pending_request clean cid map", K_(ss_id),
              K_(server_ip), K_(local_ip), "map size", cid_to_req_map_.size());
    common::hash::ObHashMap<int32_t, ObRpcReq*, hash::NoPthreadDefendMode>::iterator iter = cid_to_req_map_.begin();
    rpc_req = NULL;
    for (; iter != cid_to_req_map_.end(); iter++) {
      if (OB_NOT_NULL(rpc_req = iter->second)) {
        rpc_req->set_server_failed(true);
        PROXY_SS_LOG(INFO, "server net handle do io close, clean rpc req", K_(ss_id), K_(server_ip),
                     K_(local_ip), KPC(rpc_req), "rpc_treace_id", rpc_req->get_trace_id());
        rpc_req->server_net_cancel_request();  // server net done
        ObRpcReq::ObRpcReqCleanupParams cleanup_params(ObRpcReq::ServerNetState::RPC_REQ_SERVER_CANCLED);
        rpc_req->cleanup(cleanup_params);
        rpc_req = NULL;
      }
    }
  }
  cid_to_req_map_.destroy();
}

void ObRpcServerNetHandler::clean_all_timeout_request()
{
  int ret = OB_SUCCESS;
  common::hash::ObHashMap<int32_t, ObRpcReq*, hash::NoPthreadDefendMode>::iterator iter = cid_to_req_map_.begin();
  ObRpcReq *rpc_req = NULL;
  int64_t current_time_us = common::ObTimeUtility::current_time();
  ObRpcReqList clean_list;
  uint32_t key = 0;
  
  PROXY_LOG(DEBUG, "ObRpcServerNetHandler::clean_all_timeout_request", K_(ss_id), K_(server_ip), K_(local_ip),
            "need_send_count", need_send_req_list_.size(), "sending_count", sending_req_list_.size());

  if (OB_FAIL(check_and_clean_server_request_list(need_send_req_list_))) {
    PROXY_SS_LOG(WDIAG, "fail to check an clean need send req list", K_(ss_id), K_(server_ip), K_(local_ip), K(ret));
  } else if (OB_FAIL(check_and_clean_server_request_list(sending_req_list_))) {
    PROXY_SS_LOG(WDIAG, "fail to check an clean sending  req list", K_(ss_id), K_(server_ip), K_(local_ip), K(ret));
  }

  for (; OB_SUCC(ret) && iter != cid_to_req_map_.end(); iter++) {
    if (OB_NOT_NULL(rpc_req = iter->second)) {
      const ObRpcReqTraceId &rpc_trace_id = rpc_req->get_trace_id();
      if ((0 != rpc_req->get_server_net_timeout_us() &&
           rpc_req->get_server_net_timeout_us() < current_time_us) ||
          rpc_req->canceled()) {
        if (OB_FAIL(clean_list.push_back(rpc_req))) {
          PROXY_LOG(WDIAG, "fail to push rpc request to clean list", K_(ss_id), K_(server_ip), K_(local_ip), K(ret),
                    K(rpc_req), K(rpc_trace_id));
        }
        rpc_req = NULL;
      }
    } else {
      // do nothing
    }
  }

  while (OB_SUCC(ret) && !clean_list.empty()) {
    if (OB_FAIL(clean_list.pop_front(rpc_req))) {
      PROXY_LOG(WDIAG, "fail to pop need clean request", K_(ss_id), K_(server_ip), K_(local_ip), K(ret));
    } else if (OB_NOT_NULL(rpc_req)) {
      const ObRpcReqTraceId &rpc_trace_id = rpc_req->get_trace_id();
      key = rpc_req->get_server_channel_id();
      if (OB_FAIL(cid_to_req_map_.erase_refactored(key))) {
        PROXY_LOG(WDIAG, "fail to call erase_refactored for req_map, do nothing", K_(ss_id),
                  K_(server_ip), K_(local_ip), K(ret), KPC(rpc_req), K(rpc_trace_id));
      } else {
        PROXY_LOG(INFO, "ObRpcServerNetHandler::clean_all_timeout_request clean rpc_req", K_(ss_id),
                  K_(server_ip), K_(local_ip), KPC(rpc_req), K(rpc_trace_id));
        rpc_req->server_net_cancel_request();  // server net done
        ObRpcReq::ObRpcReqCleanupParams cleanup_params(ObRpcReq::ServerNetState::RPC_REQ_SERVER_CANCLED);
        rpc_req->cleanup(cleanup_params);
      }
      rpc_req = NULL;
    }
  }
}

int ObRpcServerNetTableEntry::schedule_period_task()
{
  int ret = OB_SUCCESS;
  ObHRTime period_task_time = HRTIME_USECONDS(get_global_proxy_config().rpc_period_task_interval);

  if (OB_UNLIKELY(NULL != period_task_action_)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_LOG(WDIAG, "period_task_action must be NULL here", K(this), K_(server_ip),
              K_(period_task_action), K(ret));
  } else if (OB_ISNULL(create_thread_) || create_thread_ != this_ethread()) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_LOG(WDIAG, "ObRpcServerNetTableEntry::schedule_period_task get wrong thread", K(this),
              K_(server_ip), KP_(create_thread), KP(this_ethread()));
  } else if (OB_ISNULL(period_task_action_ =
             self_ethread().schedule_every(this, period_task_time, RPC_SERVER_NET_TABLE_ENTRY_PERIOD_TASK))) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_LOG(EDIAG, "fail to schedule timeout", K(this), K_(server_ip), K(period_task_action_), K(ret));
  } else {
    PROXY_LOG(DEBUG, "succ to schedule repeat task for ObRpcServerNetTableEntry", K(this), K_(server_ip),
              K(period_task_time));
  }

  return ret;
}

int ObRpcServerNetTableEntry::cancel_period_task()
{
  int ret = OB_SUCCESS;

  if (NULL != period_task_action_) {
    if (OB_FAIL(period_task_action_->cancel())) {
      PROXY_LOG(WDIAG, "fail to cancel repeat task", K(this), K_(server_ip),
                K_(period_task_action), K(ret));
    } else {
      period_task_action_ = NULL;
    }
  }

  return ret;
}

int ObRpcServerNetTableEntry::handle_period_task()
{
  int ret = OB_SUCCESS;

  PROXY_LOG(DEBUG, "ObRpcServerNetTableEntry::handle_period_task", K(this), K_(server_ip),
            K(cur_server_entry_count_),
            "is_server_conn_empty", all_server_list_.empty(), K(last_access_timestamp_));
  // 1. 清理withing list
  clean_all_timeout_request();
  // 2. 清理过期的或者异常的server net handle
  int64_t expire_time = get_global_proxy_config().rpc_server_entry_expire_time;
  if (is_server_table_entry_expired(expire_time)) {
    PROXY_SS_LOG(INFO, "server table entry is expired, clean this table entry, close it",
                 K(this), K_(server_ip));
    do_entry_close();
  }

  // TODO
  return ret;
}


void ObRpcServerNetTableEntry::clean_all_timeout_request()
{
  int ret = OB_SUCCESS;
  PROXY_LOG(DEBUG, "ObRpcServerNetTableEntry::clean_all_timeout_request", K(this), K_(server_ip),
            "waiting_list_count", waiting_req_list_.size());
  if (OB_FAIL(check_and_clean_server_request_list(waiting_req_list_))) {
    PROXY_SS_LOG(WDIAG, "fail to check and clean request list", K(this), K_(server_ip), K(ret));
  }
}

/* ObRpcServerNetTableEntry */
int check_and_clean_server_request_list(ObRpcReqList &req_list)
{
  int ret = OB_SUCCESS;
  ObRpcReq *rpc_req = NULL;
  int64_t current_time_us = common::ObTimeUtility::current_time();
  ObRpcReqList clean_list;
  
  PROXY_LOG(DEBUG, "ObRpcServerNetTableEntry::clean req_list", "list_count", req_list.size());
  ObRpcReqList::iterator iter = req_list.begin();
  ObRpcReqList::iterator end = req_list.end();

  // record canceled request
  for (; OB_SUCC(ret) && iter != end; iter++) {
    rpc_req = *iter;
    if (OB_ISNULL(rpc_req)) {
      PROXY_LOG(WDIAG, "rpc_req in waiting_req_list is NULL");
    } else if ((0 != rpc_req->get_server_net_timeout_us() &&
                rpc_req->get_server_net_timeout_us() < current_time_us) ||
               rpc_req->canceled()) {
      if (OB_FAIL(clean_list.push_back(rpc_req))) {
        PROXY_LOG(WDIAG, "fail to enqueue req to clean_list", K(ret), K(rpc_req),
                  "rpc_trace_id", rpc_req->get_trace_id());
      }
      rpc_req = NULL;
    }
  }

  // clean canceled request
  while (OB_SUCC(ret) && !clean_list.empty()) {
    if (OB_FAIL(clean_list.pop_front(rpc_req))) {
      PROXY_LOG(WDIAG, "fail to pop need clean request", K(ret));
    } else if (OB_NOT_NULL(rpc_req)) {
      if (OB_FAIL(req_list.erase(rpc_req))) {
        PROXY_LOG(WDIAG, "fail to erase rpc req", K(ret), K(rpc_req));
      } else {
        PROXY_LOG(INFO, "ObRpcServerNetTableEntry::clean_all_timeout_request clean rpc_req",
                  KPC(rpc_req), "rpc_trace_id", rpc_req->get_trace_id());
        rpc_req->server_net_cancel_request();  // server net done
        ObRpcReq::ObRpcReqCleanupParams cleanup_params(ObRpcReq::ServerNetState::RPC_REQ_SERVER_CANCLED);
        rpc_req->cleanup(cleanup_params);
        rpc_req = NULL;
      }
    }
  }
  return ret;
}
/* ObRpcServerNetTableEntry */
void ObRpcServerNetTableEntry::destroy()
{
  int ret = OB_SUCCESS;
  PROXY_SS_LOG(INFO, "server table entry destroying", K(this), K_(server_ip), K(this));
  if (OB_NOT_NULL(create_thread_)) {
    ObRpcServerNetTableEntryPool &entry_pool = get_rpc_server_net_handler_map(*create_thread_);
    // remove table entry from pool
    entry_pool.ip_pool_.remove(this);
    create_thread_ = NULL;
  } else {
    PROXY_SS_LOG(WDIAG, "create thread of this table entry is NULL", K(this), K_(server_ip),
                 K(ret), K(this));
  }
  op_reclaim_free(this);
}

int ObRpcServerNetTableEntry::add_req_to_waiting_link(ObRpcReq *rpc_req)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(rpc_req)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_SS_LOG(WDIAG, "rpc_req is NULL to add waiting list", K(this), K_(server_ip), K(ret));
  } else if (OB_FAIL(waiting_req_list_.push_back(rpc_req))) {
    PROXY_SS_LOG(WDIAG, "fail to enqueue request to waiting list", K(this), K_(server_ip), K(ret),
                 KP(rpc_req));
  } else {
    RPC_REQ_SNET_ENTER_STATE(rpc_req, ObRpcReq::ServerNetState::RPC_REQ_SERVER_ENTRY_WARTING);
    PROXY_SS_LOG(DEBUG, "succ to add_req_to_waiting_link", K(this), K_(server_ip), KP(rpc_req),
                 "waiting count", waiting_req_list_.size());
  }

  return ret;
}

int ObRpcServerNetTableEntry::pop_back_req_from_waiting_link(ObRpcReq *&rpc_req)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(waiting_req_list_.pop_back(rpc_req))) {
    PROXY_SS_LOG(WDIAG, "fail pop request from waiting list", K(this), K_(server_ip), K(ret));
  } else {
    RPC_REQ_SNET_ENTER_STATE(rpc_req, ObRpcReq::ServerNetState::RPC_REQ_SERVER_INIT);
  }

  return ret;
}

ObRpcServerNetHandler *ObRpcServerNetTableEntry::pop_free_server_entry()
{
  ObRpcServerNetHandler *server_handler = NULL;
  int64_t rpc_server_net_invalid_time_us = get_global_proxy_config().rpc_server_net_invalid_time_us;
  int64_t rpc_server_net_max_pending_request = get_global_proxy_config().rpc_server_net_max_pending_request;

  while(OB_ISNULL(server_handler) && OB_LIKELY(!free_server_list_.empty())) {
    server_handler = free_server_list_.pop();

    if (server_handler->is_invalid_server_net_handle(rpc_server_net_invalid_time_us, rpc_server_net_max_pending_request)) {
      // invalid, close net handle
      PROXY_SS_LOG(INFO, "pick an invalid server_net_handle, close", K(this), K_(server_ip), KPC(server_handler));
      server_handler->do_io_close();
      server_handler = NULL;
    } else {
      PROXY_SS_LOG(DEBUG, "need schedule server to handle the request", K(this), K_(server_ip), KPC(server_handler));
    }
  }

  return server_handler;
}

void ObRpcServerNetTableEntry::add_free_server_entry(ObRpcServerNetHandler *entry)
{
  if (OB_LIKELY(entry)) {
    PROXY_SS_LOG(DEBUG, "need put server entry to free entry table", K(this), K_(server_ip), K(entry));
    free_server_list_.push(entry);
    // cur_server_entry_count_++;
    // last_access_timestamp_ TODO need update access_times_ at here
  }
}

int ObRpcServerNetTableEntry::connect_to_observer() //init a new server entry handler, to build a new server connection
{
  int ret = OB_SUCCESS;
  ObRpcServerNetHandler *server_entry = NULL;
  if (OB_ISNULL(server_entry = op_reclaim_alloc(ObRpcServerNetHandler))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_SS_LOG(EDIAG, "failed to allocate memory for ObRpcServerNetHandler", K(this), K_(server_ip), K(ret));
  } else {
    server_entry->init_server_addr(server_addr_);
    server_entry->init_server_ip(server_ip_);
    server_entry->mutex_ = this_ethread()->mutex_;
    last_build_connect_timestamp_ = event::get_hrtime();

    if (OB_FAIL(server_entry->handle_new_connection())) { //init a new net_vc to connect to server
      PROXY_SS_LOG(WDIAG, "server entry handle new connection failed in local", K(this), K_(server_ip));
    } else {
      if (OB_FAIL(add_connect_server_entry(server_entry))) {
        server_entry->destroy();
      } else {
        server_entry->server_table_entry_ = this;
        server_entry->create_thread_ = this_ethread();
      }
      PROXY_SS_LOG(DEBUG, "init new server entry", K(this), K_(server_ip), KPC(server_entry));
    }
  }
  return ret;
}

int ObRpcServerNetTableEntry::add_connect_server_entry(ObRpcServerNetHandler *entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry)) {
    //invalid server net entry
    ret = OB_ERR_UNEXPECTED;
    PROXY_SS_LOG(WDIAG, "invalid server entry add", K(this), K_(server_ip), KPC(entry));
  } else {
    entry->renew_handler_last_active_time();
    all_server_list_.push(entry);
    in_connect_server_list_.push(entry);
    cur_server_entry_count_++; //maybe in connecting procedure
  }
  return ret;
}

int ObRpcServerNetTableEntry::remove_server_entry(ObRpcServerNetHandler *entry)
{
  int ret = OB_SUCCESS;
  ObRpcServerNetHandler *tmp = NULL;

  if (OB_ISNULL(entry) || OB_ISNULL(tmp = all_server_list_.remove(entry))) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_SS_LOG(WDIAG, "invalid server entry add", K(this), K_(server_ip), KPC(entry));
  } else {
    free_server_list_.remove(entry);
    in_connect_server_list_.remove(entry);
    cur_server_entry_count_--; //maybe in connecting procedure
    PROXY_SS_LOG(DEBUG, "remove server entry in server table entry object", K(this), K_(server_ip),
                KP(entry), K_(cur_server_entry_count));
  }
  //TODO maybe need schedule server net table entry to init connection to handle the waiting requests
  return ret;
}

int ObRpcServerNetTableEntry::handle_rpc_request(ObRpcReq &request)
{
  int ret = OB_SUCCESS;
  ObRpcReq *req = &request;
  const ObRpcReqTraceId &rpc_trace_id = request.get_trace_id();
  renew_last_access_time();
  if (OB_UNLIKELY(request.canceled())) {
    PROXY_SS_LOG(INFO, "ObRpcServerNetTableEntry::handle_rpc_request get a canceled request", K(this),
                 K_(server_ip), K(request), K(rpc_trace_id));
    ObRpcReq::ObRpcReqCleanupParams cleanup_params(ObRpcReq::ServerNetState::RPC_REQ_SERVER_CANCLED);
    request.cleanup(cleanup_params);
  } else {
    PROXY_SS_LOG(DEBUG, "handle the new rpc request in server table entry", K(this), K_(server_ip), K(request),
                 "addr", request.get_server_addr().addr_, K(rpc_trace_id));
    ObRpcServerNetHandler *server_net_handler = NULL;
    // TODO: 实时生效，缩放
    max_server_entry_count_ = get_global_proxy_config().rpc_max_server_table_entry_num;

    if (OB_ISNULL(server_net_handler = pop_free_server_entry())) {
      PROXY_SS_LOG(DEBUG, "no free server net handler to get", K(this), K_(server_ip), K(request),
                   K_(cur_server_entry_count), K_(max_server_entry_count), K(rpc_trace_id));
      if (OB_FAIL(add_req_to_waiting_link(&request))) {
        PROXY_SS_LOG(WDIAG, "server request push it to the waiting list failed", K(this), K_(server_ip),
                    K(request), K(rpc_trace_id));
      } else if (cur_server_entry_count_ < max_server_entry_count_) {
        if (OB_FAIL(connect_to_observer())) { //add one server entry to this
          PROXY_SS_LOG(WDIAG, "failed to  call connect_to_observer", K(this), K_(server_ip), K(request),
                       K(ret), K(rpc_trace_id));
        }
      } else {
        //TODO need calc pending request, and init time
        PROXY_SS_LOG(DEBUG, "server is busy handing the request, and need to wait",
            K(this), K_(server_ip), K(request), K_(cur_server_entry_count), K_(max_server_entry_count),
            "waiting_count", waiting_req_list_.size(), K(rpc_trace_id));
      }
    } else if (OB_FAIL(server_net_handler->need_send_req_list_.push_back(&request))) { //put it to waiting list
      PROXY_SS_LOG(WDIAG, "fail to push back requset", K(this), K_(server_ip), K(request),
                   K(ret), K(rpc_trace_id));
    } else {
      RPC_REQ_SNET_ENTER_STATE(req, ObRpcReq::ServerNetState::RPC_REQ_SERVER_ENTRY_LOOKUP_DONE);
      server_net_handler->handle_event(RPC_SERVER_NET_REQUEST_SEND);
    }

    if (OB_FAIL(ret)) {
      // clean waiting link
      PROXY_SS_LOG(WDIAG, "error in ObRpcServerNetTableEntry::handle_rpc_request, clean waiting link",
                   K(this), K_(server_ip), K(request), K(ret), K(rpc_trace_id));
      if (ObRpcReq::ServerNetState::RPC_REQ_SERVER_ENTRY_WARTING == request.get_snet_state()) {
        int tmp_ret = ret;
        ObRpcReq *tmp_req = NULL;
        if (OB_FAIL(pop_back_req_from_waiting_link(tmp_req))) {
          PROXY_SS_LOG(WDIAG, "fail to pop req from waiting link", K(this), K_(server_ip), K(tmp_req),
                       K(ret), K(rpc_trace_id));
        } else if (tmp_req != req) {
          ret = OB_ERR_UNEXPECTED;
          PROXY_SS_LOG(WDIAG, "req is not equal to tmp_req", K(this), K_(server_ip), K(tmp_req), K(req),
                       K(ret), K(rpc_trace_id));
        }
        ret = tmp_ret;
      }
    }
  }
  return ret;
}

int ObRpcServerNetTableEntry::handle_send_request() /* need to reschedule rpc_requests in waiting list */
{
  int ret = OB_SUCCESS;
  renew_last_access_time();
  // PROXY_SS_LOG(DEBUG, "handle the new rpc request in server table entry", K(request.req_addr_));
  PROXY_SS_LOG(DEBUG, "handle the waiting rpc request in server table entry", K(this), K_(server_ip),
              "waiting_req_count", waiting_req_list_.size(), K_(send_request_action));
  send_request_action_ = NULL;
  if (!waiting_req_list_.empty()) {
    ObRpcServerNetHandler *server_net_handler = NULL;
    // max_server_entry_count_ = 1; //default value
    max_server_entry_count_ = get_global_proxy_config().rpc_max_server_table_entry_num;

    if (OB_ISNULL(server_net_handler = pop_free_server_entry())) {
      PROXY_SS_LOG(DEBUG, "no free server net handler to get", K(this), K_(server_ip), K_(cur_server_entry_count),
                   K_(max_server_entry_count));
      if (cur_server_entry_count_ < max_server_entry_count_) {
        if (OB_FAIL(connect_to_observer())) { //add one server entry to this
          PROXY_SS_LOG(WDIAG, "failed to  call connect_to_observer", K(this), K_(server_ip), K(ret));
        }
      } else {
        //TODO need calc pending request, and init time
        PROXY_SS_LOG(DEBUG, "server is busy handing the request, and need to wait", K(this), K_(server_ip),
                     K_(cur_server_entry_count), K_(max_server_entry_count),
                     "waiting_count", waiting_req_list_.size());
      }
    } else {
      server_net_handler->handle_event(RPC_SERVER_NET_REQUEST_SEND);
    }
  }

  if (OB_FAIL(ret)) {
    PROXY_SS_LOG(WDIAG, "fail to call ObRpcServerNetTableEntry::handle_rpc_request, need handle waiting request",
                 K(this), K_(server_ip), K(ret));
    // TODO add retry
  }

  return ret;
}

void ObRpcServerNetTableEntry::set_server_connector_error() 
{
  last_connect_failed_timestamp_ = event::get_hrtime();
  server_connect_error_conut_++;
  if (!waiting_req_list_.empty()) {
    force_retry_waiting_link(true);
  }
}

void ObRpcServerNetTableEntry::cleanup_server_connector_error() 
{
  last_connect_success_timestamp_ = event::get_hrtime();
  server_connect_error_conut_ = 0;
}

void ObRpcServerNetTableEntry::force_retry_waiting_link(bool server_failed)
{
  int ret = OB_SUCCESS;
  //only will be called when server is failed
  // int64_t MAX_HANDLE_BATCH_SIZE = 1024;
  ObRpcReq *request = NULL;
  while(OB_SUCC(waiting_req_list_.pop_front(request))) {
    if (OB_LIKELY(OB_NOT_NULL(request))) {
      const ObRpcReqTraceId &rpc_trace_id = request->get_trace_id();
      const ObRpcOBKVInfo &obkv_info = request->get_obkv_info();
      request->set_server_failed(server_failed);
      if ((request->canceled() || !obkv_info.is_rpc_req_can_retry())) {
        ObRpcReq::ObRpcReqCleanupParams cleanup_params(ObRpcReq::ServerNetState::RPC_REQ_SERVER_CANCLED);
        request->cleanup(cleanup_params);
        PROXY_SS_LOG(INFO, "inner request can not retry, cancel directly", K(request), K(rpc_trace_id));
      } else {
        PROXY_SS_LOG(DEBUG, "setup_retry_request will retry pl lookup", K(this), K(request->get_cnet_state()), K(request->get_snet_state()), K_(server_ip),  K(request), K(rpc_trace_id));
        ObRpcRequestSM *request_sm = NULL;
        request->set_server_failed(server_failed);
        if (OB_ISNULL(request_sm = request->get_request_sm())) {
          ret = OB_ERR_UNEXPECTED;
          PROXY_SS_LOG(WDIAG, "invalid request sm", K(this), K_(server_ip), K(request), K(ret), K(rpc_trace_id));
        } else {
          request_sm->set_retry_need_update_pl(true); // need set target dirty
          if (OB_FAIL(request_sm->schedule_call_next_action(RPC_REQ_REQUEST_RETRY))) {
            PROXY_SS_LOG(WDIAG, "fail to call schedule_call_next_action", K(ret), K(request_sm), K_(server_ip), K_(local_ip), K(rpc_trace_id));
          }
        }
        request = NULL;
      }
    }
  }
}

void ObRpcServerNetTableEntry::do_entry_close()
{
  int ret = OB_SUCCESS;
  PROXY_SS_LOG(INFO, "server table entry handle do_entry_close", K(this), K_(server_ip), K(*this));
  ObRpcServerNetHandler *server_net_handler = NULL;

  while (!all_server_list_.empty()) {
    server_net_handler = all_server_list_.pop();
    if (OB_NOT_NULL(server_net_handler)) {
      in_connect_server_list_.remove(server_net_handler);
      free_server_list_.remove(server_net_handler);
      cur_server_entry_count_--; //maybe in connecting procedure
      server_net_handler->do_io_close(); //release server entry and pending request
    }
  }

  if (cur_server_entry_count_ > 0 || !free_server_list_.empty() || !in_connect_server_list_.empty()) {
    PROXY_SS_LOG(WDIAG, "cur server net handler count is not zero, it should not happen", K(this),
                 K_(server_ip), K(cur_server_entry_count_));
  }

  if (OB_SUCC(ret)) {
    PROXY_SS_LOG(INFO, "try to clean all waiting req", K(this), K_(server_ip), K_(waiting_req_list));
    if (OB_FAIL(clean_all_pending_request())) {
      PROXY_SS_LOG(WDIAG, "fail to clean up all waiting request", K(this), K_(server_ip), K(ret));
    } if (OB_FAIL(cancel_period_task())) {
      PROXY_SS_LOG(WDIAG, "fail to cancel period task", K(this), K_(server_ip), K(ret));
    } else if (OB_FAIL(cancel_send_request_action())) {
      PROXY_SS_LOG(WDIAG, "fail to cancel pending action", K(this), K_(server_ip), K(ret));
    }
  }
  destroy();
}

int64_t ObRpcServerNetTableEntry::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  // int ret = OB_SUCCESS;
  J_OBJ_START();
  J_KV(KP(this), K_(server_ip), K_(server_addr), K_(cur_server_entry_count), K_(max_server_entry_count),
       K_(last_access_timestamp));
  J_OBJ_END();
  return pos;
}

int ObRpcServerNetTableEntry::main_handler(int event, void *data)
{
  int ret = OB_SUCCESS;
  int event_ret = EVENT_NONE;
  //do nothing
  PROXY_SS_LOG(DEBUG, "[ObRpcServerNetTableEntry::main_handler]",
            K(this), K_(server_ip),
            "event_name", ObRpcReqDebugNames::get_event_name(event),
            K(data));
  switch(event) {
    case RPC_SERVER_NET_TABLE_ENTRY_PERIOD_TASK:
      ret = handle_period_task();
      break;
    case RPC_SERVER_NET_TABLE_ENTRY_SEND_REQUEST:
      ret = handle_send_request(); /* handle the waiting request when server net entry changed */
      break;
    case RPC_SERVER_NET_TABLE_ENTRY_DESTROY:
     // TODO : add destroy func
    default:
      PROXY_SS_LOG(WDIAG, "ObRpcServerNetTableEntry meet unknow event to destroy, need to exit", K(this),
                   K_(server_ip), K(event), K(data));
      do_entry_close();
      break;
  }

  if (OB_FAIL(ret)) {
    //just print
    PROXY_SS_LOG(WDIAG, "ObRpcServerNetTableEntry handle event failed", K(this),
                 K_(server_ip), K(event));
  }

  return event_ret;
}

int ObRpcServerNetTableEntry::schedule_send_request_action()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(NULL != send_request_action_)) {
    // do nothing
    PROXY_LOG(DEBUG, "pending send_request_action, do nothing", K(this), K_(server_ip),
              K_(send_request_action), K(ret));
  } else if (OB_ISNULL(send_request_action_ =
                       self_ethread().schedule_imm(this, RPC_SERVER_NET_TABLE_ENTRY_SEND_REQUEST))) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_LOG(EDIAG, "fail to schedule send_request", K(this), K_(server_ip),
              K_(send_request_action), K(ret));
  } else {
    PROXY_LOG(DEBUG, "succ to schedule send_request for ObRpcServerNetTableEntry", K(this),
              K_(server_ip), K_(send_request_action));
  }

  return ret;
}
int ObRpcServerNetTableEntry::cancel_send_request_action()
{
  int ret = common::OB_SUCCESS;
  if (NULL != send_request_action_) {
    if (OB_FAIL(send_request_action_->cancel())) {
      PROXY_LOG(WDIAG, "ObRpcServerNetTableEntry fail to cancel send_request_action", K(this), K_(server_ip),
                K_(send_request_action), K(ret));
    } else {
      send_request_action_ = NULL;
    }
  }
  return ret;
}

int ObRpcServerNetTableEntry::clean_all_pending_request()
{
  int ret = OB_SUCCESS;
  ObRpcReq *rpc_req = NULL;

  if (!waiting_req_list_.empty()) {
    PROXY_LOG(WDIAG, "ObRpcServerNetTableEntry::clean_all_pending_request clean waiting req", K(this),
              K_(server_ip), "waiting_req_count", waiting_req_list_.size());
    ObRpcReqList::iterator iter = waiting_req_list_.begin();
    ObRpcReqList::iterator end = waiting_req_list_.end();
    for (; iter != end; iter ++) {    
      rpc_req = *iter;
      if (OB_NOT_NULL(rpc_req)) {
        PROXY_SS_LOG(INFO, "server net table entry to clean pending request", K(this), K_(server_ip),
                     "rpc_trace_id", rpc_req->get_trace_id());
        rpc_req->server_net_cancel_request();  // server net done
        ObRpcReq::ObRpcReqCleanupParams cleanup_params(ObRpcReq::ServerNetState::RPC_REQ_SERVER_CANCLED);
        rpc_req->cleanup(cleanup_params);
        rpc_req = NULL;
      }
    }
  }
  waiting_req_list_.reset();
  return ret;
}

int ObRpcServerNetTableEntry::init()
{
  int ret = common::OB_SUCCESS;

  create_thread_ = this_ethread();

  if (OB_FAIL(schedule_period_task())) {
    PROXY_SS_LOG(WDIAG, "fail to call schedule_period_task for server_table_entry",
                 K_(server_ip), K(ret), KP(this));
  } else {
    // do nothing
  }
  
  return ret;
}
/* ObRpcServerNetTableEntry */

/* ObRpcServerNetTableEntryPool */
ObRpcServerNetTableEntryPool::ObRpcServerNetTableEntryPool(ObProxyMutex *mutex)
   : ObContinuation(mutex)
{
  SET_HANDLER(&ObRpcServerNetTableEntryPool::event_handler);
}

void ObRpcServerNetTableEntryPool::purge()
{
  RpcIPHashTable::iterator last = ip_pool_.end();
  RpcIPHashTable::iterator tmp_iter;
  for (RpcIPHashTable::iterator spot = ip_pool_.begin(); spot != last;) {
    tmp_iter = spot;
    ++spot;
    tmp_iter->do_entry_close();
  }
  ip_pool_.reset();

  // if (is_delete_when_empty_) {
  //   destroy();
  // }
}

void ObRpcServerNetTableEntryPool::destroy()
{
  // // session_manager_ = NULL;
  // hash_key_.reset();
  op_free(this);
}

ObRpcServerNetTableEntry *ObRpcServerNetTableEntryPool::acquire_server_table_entry(ObRpcReq &request)
{
  int ret = OB_SUCCESS;
  ObRpcServerNetTableEntry *server_table_entry = NULL;
  ObRpcReq *req = &request;
  ObRpcServerNetHandleHashKey key;
  const ObRpcReqTraceId &rpc_trace_id = request.get_trace_id();
  ObIpEndpoint server_ip;
  ops_ip_copy(server_ip, request.get_server_addr().addr_);
  key.server_ip_ = &server_ip;
  ObString &name = request.get_full_username();
  key.auth_user_ = &name;

  RPC_REQ_SNET_ENTER_STATE(req, ObRpcReq::ServerNetState::RPC_REQ_SERVER_ENTRY_LOOKUP);
  if (OB_ISNULL(server_table_entry = acquire_server_table_entry(key))) {
    PROXY_SS_LOG(DEBUG, "new server table entry need to init", K(this), K(server_ip), K(request),
                 K(key), "thread", this_ethread(), K(rpc_trace_id));
    server_table_entry = op_reclaim_alloc(ObRpcServerNetTableEntry);
    if (OB_ISNULL(server_table_entry)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      PROXY_SS_LOG(EDIAG, "failed to allocate memory for ObRpcServerNetTableEntry", K(this),
                   K(server_ip), K(ret), K(rpc_trace_id));
    } else if (FALSE_IT(server_table_entry->server_ip_.assign(request.get_server_addr().addr_.sa_))) {
      // not here
    } else if (OB_FAIL(server_table_entry->init())) {
      PROXY_SS_LOG(WDIAG, "fail to init server_table_entry", K(this), K(server_ip), K(ret),
                   KP(server_table_entry), K(rpc_trace_id));
    } else if (OB_FAIL(put_server_table_entry(server_table_entry, key))) {
      PROXY_SS_LOG(WDIAG, "fail to call put_server_table_entry", K(this), K(server_ip), K(ret),
                   K(key), KP(server_table_entry), K(rpc_trace_id));
    } else {
      // success
    }
  }

  if (OB_FAIL(ret)) {
    op_reclaim_free(server_table_entry);
    server_table_entry = NULL;
  }

  return server_table_entry;
}

ObRpcServerNetTableEntry *ObRpcServerNetTableEntryPool::acquire_server_table_entry(
  const ObRpcServerNetHandleHashKey &key)
{
  // return ip_pool_.remove(key);
  return ip_pool_.get(key);
}

ObRpcServerNetTableEntry *ObRpcServerNetTableEntryPool::acquire_server_table_entry(
  const sockaddr &addr, const ObString &auth_user)
{
  ObRpcServerNetHandleHashKey key;
  ObIpEndpoint server_ip;
  ops_ip_copy(server_ip, addr);
  key.server_ip_ = &server_ip;
  key.auth_user_ = &auth_user;

  return acquire_server_table_entry(key);
}

int ObRpcServerNetTableEntryPool::put_server_table_entry(
  ObRpcServerNetTableEntry *table_entry,
  const ObRpcServerNetHandleHashKey &key)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(table_entry)) {
    ret = ip_pool_.set_refactored(table_entry);
    if (OB_SUCCESS == ret || OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
      PROXY_SS_LOG(DEBUG, "[free server table entry] server session placed into shared pool",
                   K(this), K(key));
    } else {
      PROXY_SS_LOG(WDIAG, "fail to put server table entry into pool", K(this), K(ret), K(key));
    }
  } else {
    //do nothing
  }
  return ret;
}

int ObRpcServerNetTableEntryPool::release_server_entry(ObRpcServerNetTableEntry &rsh)
{
  int ret = OB_SUCCESS;
  // rsh.state_ = OB_RPC_SERVER_ENTRY_SHARED;

  if (FALSE_IT(rsh.do_entry_close())) { //it will release all server entry from server
    PROXY_SS_LOG(WDIAG, "release server entry failed", K(this), K(rsh), K(ret));
  } else {
    //clear server net entry from hash pool
    ip_pool_.remove(&rsh);
  }

  return ret;
}

// Called from the ObNetProcessor to let us know that a
// connection has closed down, init or closed for server entry
int ObRpcServerNetTableEntryPool::event_handler(int event, void *data)
{
  UNUSED(event);
  UNUSED(data);
  return OB_NOT_SUPPORTED;
}

int ObRpcServerNetTableEntryPool::handle_rpc_request(ObRpcReq &request)
{
  int ret = OB_SUCCESS;
  ObRpcReq *req = &request;
  const ObRpcReqTraceId &rpc_trace_id = request.get_trace_id();
  PROXY_SS_LOG(DEBUG, "handle the new rpc request", K(this),  "thread", this_ethread(),
               K(request), K(rpc_trace_id));
  if (OB_LIKELY(request.canceled())) {
    PROXY_SS_LOG(INFO, "ObRpcServerNetTableEntryPool::handle_rpc_request get a canceled request",
                 K(this), K(request), K(rpc_trace_id));
    ObRpcReq::ObRpcReqCleanupParams cleanup_params(ObRpcReq::ServerNetState::RPC_REQ_SERVER_CANCLED);
    request.cleanup(cleanup_params);
  } else {
    ObRpcServerNetTableEntry *server_table_entry = NULL;
    ObRpcServerNetHandleHashKey key;
    ObIpEndpoint server_ip;
    ops_ip_copy(server_ip, request.get_server_addr().addr_);
    key.server_ip_ = &server_ip;
    ObString &name = request.get_full_username();
    key.auth_user_ = &name;

    RPC_REQ_SNET_ENTER_STATE(req, ObRpcReq::ServerNetState::RPC_REQ_SERVER_ENTRY_LOOKUP);
    if (OB_ISNULL(server_table_entry = acquire_server_table_entry(key))) {
      PROXY_SS_LOG(DEBUG, "new server table entry need to init", K(this), K(request), K(key),
                   "thread", this_ethread(), K(rpc_trace_id));
      // TODO: 考虑释放
      server_table_entry = op_reclaim_alloc(ObRpcServerNetTableEntry);
      if (OB_ISNULL(server_table_entry)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        PROXY_SS_LOG(EDIAG, "failed to allocate memory for ObRpcServerNetTableEntry", K(this),
                     "thread", this_ethread(), K(ret), K(rpc_trace_id));
      } else if (FALSE_IT(server_table_entry->server_ip_.assign(request.get_server_addr().addr_.sa_))) {
        // not here
      } else if (put_server_table_entry(server_table_entry, key)) {
        PROXY_SS_LOG(WDIAG, "fail to call put_server_table_entry", K(this), "thread", this_ethread(),
                     K(ret), K(key), KP(server_table_entry), K(rpc_trace_id));
      } else {
        // success
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(server_table_entry)) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_SS_LOG(WDIAG, "server table entry got is NULL", K(this), "thread", this_ethread(),
                     K(request), K(rpc_trace_id));
      } else {
        PROXY_SS_LOG(DEBUG, "get a server table entry to handle request", K(this), "thread", this_ethread(),
                     K(request), K(server_table_entry), "thread", this_ethread(),
                     "addr", request.get_server_addr().addr_, K(rpc_trace_id));
        if (OB_FAIL(server_table_entry->handle_rpc_request(request))) {
          PROXY_SS_LOG(WDIAG, "server table entry handle rpc request failed", K(this),
                       "thread", this_ethread(), K(request), K(rpc_trace_id));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    // TODO: add return error to client
    PROXY_SS_LOG(INFO, "ObRpcServerNetTableEntryPool::handle_rpc_request get error request",
                 K(this), K(request), K(rpc_trace_id));
    request.server_net_cancel_request();
    ObRpcReq::ObRpcReqCleanupParams cleanup_params(ObRpcReq::ServerNetState::RPC_REQ_SERVER_CANCLED);
    request.cleanup(cleanup_params);
  }

  return ret;
}

int init_rpc_net_ss_map_for_thread()
{
 int ret = OB_SUCCESS;
 const int64_t event_thread_count = g_event_processor.thread_count_for_type_[ET_CALL];
 for (int64_t i = 0; i < event_thread_count && OB_SUCC(ret); ++i) {
   if (OB_ISNULL(g_event_processor.event_thread_[ET_CALL][i]->rpc_net_ss_map_ = 
                  new (std::nothrow) ObRpcServerNetTableEntryPool())) {
     ret = OB_ALLOCATE_MEMORY_FAILED;
     PROXY_NET_LOG(WDIAG, "fail to new ObInactivityCop", K(i), K(ret));
   }
 }
 return ret;
}

/* ObRpcServerNetTableEntryPool */



} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
