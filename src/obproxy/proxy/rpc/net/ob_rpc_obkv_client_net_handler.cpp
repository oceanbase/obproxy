/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include "lib/encrypt/ob_encrypted_helper.h"
#include "lib/utility/ob_print_utils.h"
#include "proxy/api/ob_plugin.h"
#include "obutils/ob_resource_pool_processor.h"
#include "obutils/ob_config_server_processor.h"
#include "dbconfig/ob_proxy_pb_utils.h"
#include "proxy/mysql/ob_mysql_global_session_manager.h"
#include "omt/ob_conn_table_processor.h"
#include "omt/ob_white_list_table_processor.h"
#include "proxy/rpc/net/ob_rpc_client_net_handler.h"
#include "proxy/rpc/net/ob_rpc_server_net_handler.h"
#include "proxy/rpc/net/ob_rpc_obkv_client_net_handler.h"
#include "proxy/rpc/ob_rpc_req_debug_names.h"
#include "proxy/rpc/ob_rpc_request_sm.h"
#include "obkv/table/ob_rpc_struct.h"
#include "prometheus/ob_prometheus_info.h"
#include "prometheus/ob_rpc_prometheus.h"
#include "obproxy/stat/ob_rpc_stats.h"

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
  PROXY_CS_LOG(DEBUG, "ENTER STATE "#state_name"", "event", ObRpcReqDebugNames::get_event_name(event), K_(cs_id)); \
} while(0)

static int64_t const MYSQL_BUFFER_SIZE = BUFFER_SIZE_FOR_INDEX(BUFFER_SIZE_INDEX_8K);

// We have debugging list that we can use to find stuck
// client sessions
#ifdef USE_MYSQL_DEBUG_LISTS
DLL<ObRpcOBKVClientNetHandler> g_debug_rpc_cs_list;
// ObMutex g_debug_rpc_cs_list_mutex;
ObMutex g_debug_rpc_cs_list_mutex;
#endif

ObRpcOBKVClientNetHandler::ObRpcOBKVClientNetHandler()
    : ObRpcClientNetHandler(),
      atomic_channel_id_(0), is_sending_response_(false), has_inited_(false),
      ct_info_(), last_server_ip_(),
      need_send_response_list_(), sending_response_list_(), period_task_action_(NULL), timeout_action_(NULL),
      current_need_read_len_(RPC_NET_HEADER_LENGTH), current_ez_header_()
{
  cid_to_req_map_.create(OB_RPC_PARALLE_REQUEST_MAP_MAX_BUCKET_NUM, ObModIds::OB_RPC);
  SET_HANDLER(&ObRpcOBKVClientNetHandler::main_handler);
}

void ObRpcOBKVClientNetHandler::destroy()
{
  PROXY_CS_LOG(INFO, "rpc obkv client session destroy", K_(cs_id), K_(proxy_sessid), KP_(rpc_net_vc));

  ObRpcClientNetHandler::cleanup();

  cid_to_req_map_.destroy();
  op_reclaim_free(this);
}

void ObRpcOBKVClientNetHandler::do_io_close(const int alerrno)
{
  int ret = OB_SUCCESS;
  // Prevent double closing
  PROXY_CS_LOG(INFO, "ObRpcOBKVClientNetHandler do_io_close", K_(cs_id), K_(pending_action), K(this));

  if (MCS_CLOSED != read_state_) {
    // clean all rpc req
    if (OB_FAIL(cancel_period_task())) {
      //to ignore ret
      PROXY_CS_LOG(WDIAG, "fail to call cancel_period_task", K_(cs_id), K(ret));
    }
    if (OB_FAIL(cancel_pending_action())) {
      PROXY_CS_LOG(WDIAG, "fail to call cancel_pending_action", K_(cs_id), K(ret));
    }
    clean_all_pending_request();

    if (MCS_ACTIVE_READER == read_state_) { // now not enter
      if (LIST_ADDED == in_list_stat_) {
        RPC_INCREMENT_DYN_STAT(CURRENT_CLIENT_TRANSACTIONS);
      }
      if (active_) {
        active_ = false;
        RPC_INCREMENT_DYN_STAT(CURRENT_ACTIVE_CLIENT_CONNECTIONS);
      }
    }

    if (half_close_) { // now not enter
      read_state_ = MCS_HALF_CLOSED;
      PROXY_CS_LOG(DEBUG, "session half close", K_(cs_id));

      // We want the client to know that that we're finished writing. The
      // write shutdown accomplishes this. Unfortunately, the IO Core
      // semantics don't stop us from getting events on the write side of
      // the connection like timeouts so we need to zero out the write of
      // the continuation with the do_io_write() call
      rpc_net_vc_->do_io_shutdown(IO_SHUTDOWN_WRITE);

      if (OB_ISNULL(net_entry_.read_vio_ = rpc_net_vc_->do_io_read(this, INT64_MAX, read_buffer_))) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_CS_LOG(WDIAG, "read_vio_ is null", K_(cs_id), K(ret));
      }

      // Drain any data read.
      // If the buffer is full and the client writes again, we will not receive a
      // READ_READY event.
      if (OB_FAIL(buf_reader_->consume(buf_reader_->read_avail()))) {
        PROXY_CS_LOG(WDIAG, "fail to consume ", K_(cs_id), K(ret));
      }
    }

    if (!half_close_ && NULL != rpc_net_vc_) {
      rpc_net_vc_->do_io_close(alerrno);
      PROXY_CS_LOG(DEBUG, "session closed, session stats", K_(cs_id));
      rpc_net_vc_ = NULL;
      //RPC_SUM_DYN_STAT(TRANSACTIONS_PER_CLIENT_CON, get_transact_count());
      RPC_DECREMENT_DYN_STAT(CURRENT_CLIENT_CONNECTIONS);
    }

    if (!half_close_ && LIST_ADDED == in_list_stat_) {
      // proxy_mysql_client is not in map, no need to erase
      if (this_ethread() != create_thread_) {
        PROXY_CS_LOG(DEBUG, "current thread is not create thread, should schedule",
                     "current thread", this_ethread(), "create thread", create_thread_, K_(cs_id));
        CLIENT_NET_SET_DEFAULT_HANDLER(&ObRpcClientNetHandler::handle_other_event);
        if (OB_ISNULL(create_thread_->schedule_imm(this, CLIENT_SESSION_ERASE_FROM_MAP_EVENT))) {
          ret = OB_ERR_UNEXPECTED;
          PROXY_CS_LOG(WDIAG, "fail to schedule switch thread", K_(cs_id), K(ret));
        }
      } else {
        ObRpcClientNetHandlerMap &cs_map = get_rpc_client_net_handler_map(*create_thread_);
        if (OB_FAIL(cs_map.erase(cs_id_))) {
          PROXY_CS_LOG(WDIAG, "current client session is not in table, no need to erase", K_(cs_id), K(ret));
        }
        in_list_stat_ = LIST_REMOVED;
      }
    }

    // in 2 situations we will delete cluster (cluster rslist and resource)
    // 1. all servers of table entry which comes from rslist are not in congestion list
    // 2. fail to verify cluster name in login step, user cluster and server cluster are not the same
    //
    // if we only delete resource, loacl cluster rslist still exist, so we must delete both of them
    if (OB_UNLIKELY(need_delete_cluster_)) { // now not enter
      if (OB_FAIL(handle_delete_cluster())) {
        PROXY_CS_LOG(WDIAG, "fail to handle delete cluster", K_(cs_id), K(ret));
      }
      need_delete_cluster_ = false;
    }

    if (this_ethread() == create_thread_) {
      read_state_ = MCS_CLOSED;
      destroy(); // clean
    }
  }
}

int ObRpcOBKVClientNetHandler::handle_other_event(int event, void *data)
{
  UNUSED(data);
  switch (event) {
    case CLIENT_SESSION_ERASE_FROM_MAP_EVENT: {
      do_io_close();
      break;
    }
    default:
      PROXY_CS_LOG(WDIAG, "unknown event", K_(cs_id), K(event));
      break;
  }

  return VC_EVENT_NONE;
}

int ObRpcOBKVClientNetHandler::main_handler(int event, void *data)
{
  int event_ret = VC_EVENT_CONT;
  PROXY_CS_LOG(DEBUG, "[ObRpcOBKVClientNetHandler::main_handler]",
            K_(cs_id),
            "event_name", ObRpcReqDebugNames::get_event_name(event),
            "read_state", get_read_state_str(), K(data));

  if (OB_LIKELY(RPC_C_NET_MAGIC_ALIVE == magic_)) {
    if (RPC_CLIENT_NET_PERIOD_TASK == event) {
      event_ret = handle_period_task();
    } else if (RPC_CLIENT_NET_SEND_RESPONSE == event) {
      pending_action_ = NULL;
      event_ret = setup_client_response_send();
    } else {
      if (NULL != data && data == net_entry_.read_vio_) { // from client vc
        event_ret = state_keep_alive(event, data);
      } else if (NULL != data && data == net_entry_.write_vio_) { // from client vc
        event_ret = state_client_response_send(event, data);
      } else {
        event_ret = (this->*cs_default_handler_)(event, data); // others
      }
    }
  } else {
    PROXY_CS_LOG(WDIAG, "unexpected magic, expected RPC_C_NET_MAGIC_ALIVE", K_(cs_id), K(magic_));
  }

  return event_ret;
}

// int64_t ObRpcOBKVClientNetHandler::to_string(char *buf, const int64_t buf_len) const
// {
//   int64_t pos = 0;
//   J_OBJ_START();
//   J_KV(KP(this),
//        K_(vc_ready_killed),
//        K_(active),
//        K_(magic),
//        K_(current_tid),
//        K_(cs_id),
//        K_(proxy_sessid),
//        K_(session_info),
//        K_(dummy_ldc),
//        KP_(dummy_entry),
//        K_(server_state_version),
//        KPC_(cluster_resource),
//        KP_(rpc_net_vc),
//        K_(using_ldg),
//       //  KPC_(trace_stats));
//       K_(active));
//   J_OBJ_END();
//   return pos;
// }



int ObRpcOBKVClientNetHandler::setup_client_request_read()
{
  int ret = OB_SUCCESS;
  //set read trigger and read_reschedule. sometimes the data already is in the io buffer
  static_cast<ObUnixNetVConnection *>(this->get_netvc())->set_read_trigger();

  // int64_t read_num = 16; //RPC header len
  int64_t read_num = INT64_MAX; //just header for RPC service

  if (!has_inited_) {
    if (OB_FAIL(schedule_period_task())) { //obkv need init period task
      PROXY_CS_LOG(WDIAG, "fail to call schedule_period_task", K_(cs_id), K(ret));
    }
    has_inited_ = true;
  }
  // set net_read_timeout when client begin to read
  set_client_net_read_timeout();

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(net_entry_.read_vio_ = this->do_io_read(this, read_num, buf_reader_->mbuf_))) {
      ret = OB_ERR_UNEXPECTED;
      PROXY_CS_LOG(WDIAG, "rpc client net handler failed to do_io_read", K_(cs_id), K(ret));
    } else {
      //TODO ZDW schedule period task at first
      if (buf_reader_->read_avail() > 0) {
        PROXY_CS_LOG(DEBUG, "the request already in buffer, continue to handle it",
                K_(cs_id), "buffer len", buf_reader_->read_avail());
        state_client_request_read(VC_EVENT_READ_READY, net_entry_.read_vio_);
      } else {
        cancel_net_read_timeout();
      }
    }
  }

  return ret;
}

int ObRpcOBKVClientNetHandler::state_client_request_read(int event, void *data)
{
  int ret = OB_SUCCESS;
  int event_ret = VC_EVENT_NONE;
  UNUSED(event_ret);

  STATE_ENTER(ObRpcOBKVClientNetHandler::state_client_request_read, event, data);

  /* 1. check need update cluster resource */
  /* 2. check and add trace info */
  /* 3. check event info */
  if (OB_UNLIKELY(NULL != net_entry_.read_vio_ && net_entry_.read_vio_ != reinterpret_cast<ObVIO *>(data))
      || (net_entry_.eos_)) {
    ret = OB_INNER_STAT_ERROR;
    PROXY_CS_LOG(WDIAG, "invalid internal state", K_(cs_id), K_(net_entry_.read_vio), K(data), K_(net_entry_.eos));
  } else {
    switch (event) {
      case VC_EVENT_READ_READY:
      case VC_EVENT_READ_COMPLETE:
        // More data to fill request
        break;
      case VC_EVENT_EOS: {
        net_entry_.eos_ = true;
        PROXY_CS_LOG(INFO, "ObRpcOBKVClientNetHandler::state_client_request_read", "event", "set event name",
                 K_(cs_id), "client_vc", P(this->get_netvc()));
        break;
      }
      case VC_EVENT_ACTIVE_TIMEOUT:
      case VC_EVENT_ERROR: {
        PROXY_CS_LOG(WDIAG, "ObRpcOBKVClientNetHandler::state_client_request_read", "event",
                 ObRpcReqDebugNames::get_event_name(event), K_(cs_id), "client_vc", P(this->get_netvc()));
        ret = OB_CONNECT_ERROR;
        // The client is closed. Close it.
        // trans_state_.client_info_.abort_ = ObRpcTransact::ABORTED; //TODO need check queueing RPC and broken connection
        break;
      }
      default:
        ret = OB_INNER_STAT_ERROR;
        PROXY_CS_LOG(EDIAG, "unexpected event", K_(cs_id), K(event), K(ret));
        break;
    }

    /* 4. set keep alive base on config */
    ObNetVConnection *vc = this->get_netvc();
    //TODO PRPC need update trans_state_.mysql_config_params_ info to set keep alive opt
    if (OB_UNLIKELY(NULL != vc && vc->options_.sockopt_flags_ != get_global_proxy_config().client_sock_option_flag_out)) {
      vc->options_.sockopt_flags_ = static_cast<uint32_t>(get_global_proxy_config().client_sock_option_flag_out);
      if (vc->options_.sockopt_flags_ & ObNetVCOptions::SOCK_OPT_KEEP_ALIVE) {
        vc->options_.set_keepalive_param(static_cast<int32_t>(get_global_proxy_config().client_tcp_keepidle),
              static_cast<int32_t>(get_global_proxy_config().client_tcp_keepintvl),
              static_cast<int32_t>(get_global_proxy_config().client_tcp_keepcnt),
              static_cast<int32_t>(get_global_proxy_config().client_tcp_user_timeout));
      }
      if (OB_FAIL(vc->apply_options())) {
        PROXY_CS_LOG(WDIAG,"client session failed to apply per-transaction socket options", K_(cs_id), K(ret));
      }
    }

    /* 5. read data from vc buffer and init ObReq */
    if (OB_SUCC(ret) && OB_NOT_NULL(get_reader())) {
      ObRpcReq *rpc_req = NULL;
      ObRpcRequestSM *request_sm = NULL;
      ObRpcReqReadStatus status = RPC_REQUEST_READ_CONT;
      obkv::ObProxyRpcType rpc_type = obkv::OBPROXY_RPC_UNKOWN;
      ObIOBufferReader &buffer_reader = *get_reader();
      ObRpcEzHeader ez_header;
      ObRpcReqTraceId rpc_trace_id;
      uint32_t request_id = 0;
      uint32_t client_channel_id = 0;
      int64_t request_len = 0;
      int64_t trace_id1;
      int64_t trace_id2;
      bool is_set_cid_to_req_map = false;
      int64_t pos = 0;

      if (read_begin_ == 0 && OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
        read_begin_ = ObRpcRequestSM::static_get_based_hrtime();
      }

      if (buffer_reader.read_avail() < current_need_read_len_) { //need handle the rpc header
        PROXY_CS_LOG(DEBUG, "data not meet need", K_(cs_id), "avail_len", buffer_reader.read_avail(), K_(current_need_read_len));
      } else {
        if (RPC_NET_HEADER_LENGTH == current_need_read_len_) {
          char *written_pos = buffer_reader.copy(net_head_buf_, RPC_NET_HEADER_LENGTH);
          if (OB_UNLIKELY(written_pos != net_head_buf_ + RPC_NET_HEADER_LENGTH)) {
            ret = OB_ERR_UNEXPECTED;
            PROXY_CS_LOG(WDIAG, "not copy completely", K_(cs_id), K(written_pos), K(net_head_buf_), "meta_length", RPC_NET_HEADER_LENGTH, K(ret));
          } else if (OB_FAIL(current_ez_header_.deserialize(net_head_buf_, RPC_NET_HEADER_LENGTH, pos))) {
            PROXY_CS_LOG(WDIAG, "fail to deserialize ObRpcEzHeader", K_(cs_id), K(written_pos), K(net_head_buf_), "meta_length", RPC_NET_HEADER_LENGTH, K(ret));
          } else {
            current_need_read_len_ = current_ez_header_.ez_payload_size_ + RPC_NET_HEADER_LENGTH;
          }
        }
        if (OB_SUCC(ret) && RPC_NET_HEADER_LENGTH != current_need_read_len_) {
          request_len = current_need_read_len_;

          if (buffer_reader.read_avail() < request_len) {
            // request not completely got from net, next to read it when meet condition
            PROXY_CS_LOG(DEBUG, "response not ready, need get next", K_(cs_id), K(request_len), "data_len", buffer_reader.read_avail());
            status = RPC_REQUEST_READ_CONT;
          } else {
            if (obkv::OBPROXY_RPC_OBRPC != (rpc_type = current_ez_header_.get_rpc_magic_type())) {
              ret = OB_ERR_UNEXPECTED;
              int x1 = current_ez_header_.magic_header_flag_[0];
              int x2 = current_ez_header_.magic_header_flag_[1];
              int x3 = current_ez_header_.magic_header_flag_[2];
              int x4 = current_ez_header_.magic_header_flag_[3];
              PROXY_CS_LOG(WDIAG, "get an unsupported rpc type", K_(cs_id), K(rpc_type), K(ret), K(x1), K(x2), K(x3), K(x4));
            } else if (OB_ISNULL(rpc_req = ObRpcReq::allocate())) {
              //TODO handle the error, maybe need broken connection
              ret = OB_ERR_UNEXPECTED;
              PROXY_CS_LOG(WDIAG,"could not allocate request, abort connection if need", K_(cs_id), K(ret));
            } else if (OB_FAIL(rpc_req->alloc_request_buf(request_len + ObProxyRpcReqAnalyzer::OB_RPC_ANALYZE_MORE_BUFF_LEN))) {
              PROXY_CS_LOG(WDIAG,"fail to allocate rpc request object", K_(cs_id), K(ret));
            } else if (OB_ISNULL(request_sm = ObRpcRequestSM::allocate())) {
              ret = OB_ERR_UNEXPECTED;
              PROXY_CS_LOG(WDIAG, "could not allocate request sm, net need abort connection", K_(cs_id), K(ret));
            } else {
              //init rpc request trace id
              int64_t pos = RPC_NET_HEADER_LENGTH + obrpc::ObRpcPacketHeader::RPC_REQ_TRACE_ID_POS;
              char *request_buf = rpc_req->get_request_buf();
              PROXY_CS_LOG(DEBUG, "receive one rpc request has init rpc request done", K_(cs_id), K(ret), K(this));
              // success we need copy net data to request buffer
              buffer_reader.copy(request_buf, request_len);
              buffer_reader.consume(request_len); //clean the net data of the request
              if (OB_UNLIKELY(OB_FAIL(serialization::decode_i64(request_buf, request_len, pos, &trace_id1))
                    || OB_FAIL(serialization::decode_i64(request_buf, request_len, pos, &trace_id2)))) {
                PROXY_CS_LOG(WDIAG, "fail to retrive trace id from request", K_(cs_id), K(ret), K(request_len), K(pos));
              } else {
                PROXY_CS_LOG(DEBUG, "retrive trace id from request", K_(cs_id), K(pos), K(trace_id1), K(trace_id2));
                rpc_trace_id.set_rpc_trace_id(trace_id2, trace_id1);
              }
              status = RPC_REQUEST_READ_DONE;
              PROXY_CS_LOG(DEBUG, "request has read to buffer, consume it", K_(cs_id), K(request_len),
                  "block_count", buffer_reader.get_block_count(),
                  "block_addr", buffer_reader.get_current_block(),
                  "buffer", buffer_reader.mbuf_,
                  "read_avail", buffer_reader.read_avail()
                  );
              RPC_REQ_CNET_ENTER_STATE(rpc_req, ObRpcReq::ClientNetState::RPC_REQ_CLIENT_REQUEST_READ);
              rpc_req->client_timestamp_.client_begin_ = read_begin_;
              rpc_req->client_timestamp_.client_read_end_ = ObRpcRequestSM::static_get_based_hrtime();
              read_begin_ = 0;
            }
          }
        }
      }

      if (OB_FAIL(ret)) {
        status = RPC_REQUEST_READ_ERROR;
      }

      switch (__builtin_expect(status, RPC_REQUEST_READ_DONE)) {
        case RPC_REQUEST_READ_DONE:
          if (OB_NOT_NULL(rpc_req) && OB_NOT_NULL(request_sm)) {
            request_id = current_ez_header_.chid_;
            client_channel_id = atomic_channel_id_++;

            PROXY_CS_LOG(DEBUG, "[RPC_REQUEST]recv a new rpc_req, to handle", K_(cs_id), K(rpc_trace_id), K(ret), KPC(rpc_req));

            if (OB_FAIL(rpc_req->init(rpc_type, request_sm, this, request_len, cluster_version_,
                                      request_id, client_channel_id, cs_id_, rpc_net_vc_, trace_id1, trace_id2))) {
              PROXY_CS_LOG(WDIAG, "failed to init rpc_req", K_(cs_id), K(rpc_trace_id), K(ret), K(this));
            } else if (OB_FAIL(request_sm->init(rpc_req, mutex_))) {
              PROXY_CS_LOG(WDIAG, "failed to init request_sm", K_(cs_id), K(rpc_trace_id), K(ret), K(this));
            } else if (OB_FAIL(request_sm->state_add_to_list(EVENT_NONE, NULL))) {
              PROXY_CS_LOG(WDIAG, "failed to add request_sm to list", K_(cs_id), K(rpc_trace_id), K(ret), K(this));
            } else if (OB_FAIL(cid_to_req_map_.set_refactored(client_channel_id, rpc_req))) {
              PROXY_CS_LOG(WDIAG, "failed to set_refactored", K_(cs_id), K(rpc_trace_id), K(ret), K(this));
            } else {
              RPC_REQ_CNET_ENTER_STATE(rpc_req, ObRpcReq::ClientNetState::RPC_REQ_CLIENT_REQUEST_HANDLING);
              is_set_cid_to_req_map = true;
              if (OB_FAIL(request_sm->schedule_call_next_action(RPC_REQ_NEW_REQUEST))) {
                PROXY_CS_LOG(WDIAG, "fail to call schedule_call_next_action", K(ret), K_(cs_id), K(request_sm));
              }
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            PROXY_CS_LOG(WDIAG," handle rpc request expected null request", K_(cs_id), K(ret));
          }

          net_entry_.read_vio_->nbytes_ = INT64_MAX;
          net_entry_.read_vio_->reenable(); //need check next data
          current_need_read_len_ = RPC_NET_HEADER_LENGTH;
          current_ez_header_.reset();
          cancel_net_read_timeout();
          if (OB_SUCC(ret)) {
            PROXY_CS_LOG(DEBUG, "need read next request immediately when request waiting", K_(cs_id), "net_len", buffer_reader.read_avail());
            if (OB_FAIL(setup_client_request_read())) {
              PROXY_CS_LOG(WDIAG, "fail to call setup_client_request_read", K_(cs_id), K(ret));
            }
          }
          break;
        case RPC_REQUEST_READ_CONT:
          if (net_entry_.eos_) {
            ret = OB_CONNECT_ERROR;
            PROXY_CS_LOG(WDIAG,"EOS before client request parsing finished", K_(cs_id), K(ret));
            //TODO PRPC client need abort and broken connection
            net_entry_.read_vio_->nbytes_ = net_entry_.read_vio_->ndone_;//client_entry_->read_vio_->ndone_;
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
          PROXY_CS_LOG(WDIAG,"error parsing client request", K_(cs_id), K(ret));
          net_entry_.read_vio_->nbytes_ = net_entry_.read_vio_->ndone_;
          break;
        default:
          ret = OB_INNER_STAT_ERROR;
          PROXY_CS_LOG(EDIAG,"unknown analyze mysql request status", K_(cs_id), K(status), K(ret));
          break;
      }

      if (OB_FAIL(ret)) { // clear all buffer
        int tmp_ret = ret;
        if (is_set_cid_to_req_map && OB_FAIL(cid_to_req_map_.erase_refactored(client_channel_id))) {
          PROXY_CS_LOG(WDIAG, "fail to call erase_refactored", K_(cs_id), K(ret), K(client_channel_id));
        }
        if (OB_NOT_NULL(rpc_req)) {
          PROXY_CS_LOG(INFO, "state_client_request_read error, cleanup rpc_req", K_(cs_id), KPC(rpc_req));
          ObRpcReq::ObRpcReqCleanupParams cleanup_params(ObRpcReq::ClientNetState::RPC_REQ_CLIENT_DONE);
          rpc_req->cleanup(cleanup_params);
        }
        ret = tmp_ret;
      }
    }
  }

  if (OB_FAIL(ret)) {
    //error state, need abort client session
    do_io_close();
  }

  return ret;
}

int ObRpcOBKVClientNetHandler::calc_response_need_send(int64_t &count)
{
  int ret = OB_SUCCESS;
  int64_t need_send_response_count = need_send_response_list_.size();
  int64_t max_response_count = get_global_proxy_config().rpc_max_response_batch_size;

  //TODO : add response bytes limite

  if (need_send_response_count > max_response_count) {
    need_send_response_count = max_response_count;
  }

  count = need_send_response_count;
  PROXY_LOG(DEBUG, "ObRpcOBKVClientNetHandler::calc_response_need_send done", K(count));

  return ret;
}

int ObRpcOBKVClientNetHandler::store_rpc_req_into_response_buffer(int64_t need_send_resp_count, int64_t &send_response, int64_t &total_response_len)
{
  int ret = OB_SUCCESS;
  ObRpcReq *rpc_req = NULL;
  ObMIOBuffer *response_buffer = net_entry_.write_buffer_;
  int64_t written_len = 0;
  int64_t response_len = 0;
  char *buf = NULL;

  while (OB_SUCC(ret) && !need_send_response_list_.empty() && send_response < need_send_resp_count) {
    written_len = 0;

    if (OB_FAIL(need_send_response_list_.pop_front(rpc_req))) {
      PROXY_CS_LOG(WDIAG, "fail to pop need send response", K_(cs_id), K(ret));
    } else if (OB_ISNULL(rpc_req)) {
      PROXY_CS_LOG(WDIAG, "need send response is invalid", K_(cs_id), K(ret));
    } else if (OB_FAIL(handle_response_rewrite_channel_id(rpc_req))) {
      PROXY_CS_LOG(WDIAG, "rpc_req convert channel_id failed", K_(cs_id), K(rpc_req));
    } else {
      const ObRpcReqTraceId &rpc_trace_id = rpc_req->get_trace_id();
      RPC_REQ_CNET_ENTER_STATE(rpc_req, ObRpcReq::ClientNetState::RPC_REQ_CLIENT_RESPONSE_HANDLING);

      response_len = rpc_req->get_response_len();
      if (!rpc_req->is_use_response_inner_buf()) {
        buf = rpc_req->get_response_buf();
      } else {
        buf = rpc_req->get_response_inner_buf();
      }

      if (OB_FAIL(response_buffer->write(buf, response_len, written_len))) {
        PROXY_CS_LOG(WDIAG, "response is not written completely, all rpc_req handle failed", K_(cs_id), K(written_len), K(response_len), K(rpc_trace_id));
      } else if (OB_UNLIKELY(response_len != written_len)) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_CS_LOG(WDIAG, "response is not written completely, all rpc_req handle failed", K_(cs_id), K(written_len), K(response_len), K(rpc_trace_id));
      } else if (OB_FAIL(sending_response_list_.push_back(rpc_req))) {
        PROXY_CS_LOG(WDIAG, "response push back into sending_response_list failed", K_(cs_id), K(response_len), K(rpc_trace_id));
      } else {
        PROXY_CS_LOG(DEBUG, "[RPC_REQUEST]sending response...", K_(cs_id), KPC(rpc_req), K(this), K(rpc_trace_id));
        RPC_REQ_CNET_ENTER_STATE(rpc_req, ObRpcReq::ClientNetState::RPC_REQ_CLIENT_RESPONSE_SEND);
        send_response++;
        total_response_len += response_len;
      }
    }
  }

  return ret;
}

int ObRpcOBKVClientNetHandler::setup_client_response_send()
{
  int ret = OB_SUCCESS;
  //set read trigger and read_reschedule. sometimes the data already is in the io buffer
  PROXY_CS_LOG(DEBUG, "ObRpcServerNetHandler::setup_client_response send", K_(cs_id), "request_count", need_send_response_list_.size());
  static_cast<ObUnixNetVConnection *>(this->get_netvc())->set_read_trigger();

  if (OB_LIKELY(!need_send_response_list_.empty()) && !is_sending_response_) {
    int64_t send_response = 0;
    int64_t total_response_len = 0;
    ObIOBufferReader *buf_start = NULL;
    int64_t need_send_resp_count = 0;

    if (OB_ISNULL(net_entry_.write_buffer_)) {
      net_entry_.write_buffer_ = new_empty_miobuffer(MYSQL_BUFFER_SIZE);
    } else {
      net_entry_.write_buffer_->reset(); //cleanup
      net_entry_.write_buffer_->dealloc_all_readers();
    }

    if (OB_ISNULL(buf_start = net_entry_.write_buffer_->alloc_reader())) {
      ret = OB_ERR_UNEXPECTED;
      PROXY_CS_LOG(WDIAG, "setup_client_response_send failed to allocate iobuffer reader", K_(cs_id), K(ret));
    } else if (OB_FAIL(calc_response_need_send(need_send_resp_count))) {
      PROXY_CS_LOG(WDIAG, "fail to call calc_response_need_send", K(ret), K_(cs_id), K(ret));
    } else if (OB_FAIL(store_rpc_req_into_response_buffer(need_send_resp_count, send_response, total_response_len))) {
      PROXY_CS_LOG(WDIAG, "fail to call store_rpc_req_into_response_buffer", K(ret), K_(cs_id), K(ret));
    } else if (send_response > 0) {
      if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
        write_begin_ = ObRpcRequestSM::static_get_based_hrtime(); /* record begin time to write */
      }
      set_client_net_write_timeout();
      // MUTEX_TRY_LOCK(lock, rpc_net_vc_->mutex_, create_thread_);
      // TODO: Using MUTEX_ LOCK may affect performance. In the future, consider using different mutexes for asynchronous tasks within RPC requests
      // compared to client VC to prevent race conditions
      MUTEX_LOCK(lock, rpc_net_vc_->mutex_, create_thread_);    // TODO: check CLIENT_VC_SWAP_MUTEX_EVENT event
      /* check mutex_->thread_holding_ is same with create_thread_ to avoid
        writing failed with net_entry_.write_vio_ in not null */
      if (create_thread_ != mutex_->thread_holding_ || OB_ISNULL(net_entry_.write_vio_ = do_io_write(this, total_response_len, buf_start))) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_CS_LOG(WDIAG, "client entry failed to do_io_write", K_(cs_id), K(send_response), K(total_response_len), KP_(create_thread), KP(this_ethread()),
                    KP(mutex_->thread_holding_), K(mutex_.ptr_));
      } else {
        is_sending_response_ = true;
      }
    }
  } else {
    //do nothing
    PROXY_CS_LOG(DEBUG, "client net is in sending state, need to wait complete for last", K_(cs_id), "waiting_count", need_send_response_list_.size());
  }

  if (OB_FAIL(ret)) {
    do_io_close();
  }

  return ret;
}

int ObRpcOBKVClientNetHandler::state_client_response_send(int event, void *data)
{
  int ret = OB_SUCCESS;
  bool need_terminal = false;
  ObRpcReq *rpc_req = NULL;
  if (OB_ISNULL(data)) {
    ret = OB_INNER_STAT_ERROR;
    PROXY_CS_LOG(EDIAG,"invalid internal state, server entry is NULL or data is NULL",
            /* K_(net_entry), */
            K_(cs_id), K(data), K(ret));
  } else if (OB_UNLIKELY(net_entry_.read_vio_ != reinterpret_cast<ObVIO *>(data)
             && net_entry_.write_vio_ != reinterpret_cast<ObVIO *>(data))) {
    ret = OB_INNER_STAT_ERROR;
    PROXY_CS_LOG(EDIAG,"invalid internal state, server entry read vio isn't the same as data,"
              "and server entry write vio isn't the same as data",
              K_(cs_id), K_(net_entry_.read_vio),
              K_(net_entry_.write_vio), K(data), K(ret));

  } else {
    ObHRTime write_done = 0;
    switch (event) {
      case VC_EVENT_WRITE_READY:
        net_entry_.write_vio_->reenable();
        break;
      case VC_EVENT_WRITE_COMPLETE:
        is_sending_response_ = false;
        cancel_net_write_timeout();
        if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
          write_done = ObRpcRequestSM::static_get_based_hrtime();
        }
        while (OB_SUCC(ret) && !sending_response_list_.empty()) {
          if (OB_FAIL(sending_response_list_.pop_front(rpc_req))) {
            ret = OB_ERR_UNEXPECTED;
            PROXY_CS_LOG(WDIAG, "fail to pop sending response", K_(cs_id), K(ret));
          } else if (OB_NOT_NULL(rpc_req)) {
            uint32_t key = rpc_req->get_client_channel_id();
            const ObRpcReqTraceId &rpc_trace_id = rpc_req->get_trace_id();

            if (OB_FAIL(cid_to_req_map_.erase_refactored(key))) { //remove it
              // do nothing
              if (OB_HASH_NOT_EXIST == ret) {
                ret = OB_SUCCESS;
                PROXY_CS_LOG(INFO, "rpc_req is not in cid_to_req_map, just destory", K_(cs_id), "rpc_req", *rpc_req, K(ret), K(key), K(rpc_trace_id));
              } else {
                PROXY_CS_LOG(WDIAG, "fail to call erase_refactored", K_(cs_id), "rpc_req", *rpc_req, K(ret), K(key), K(rpc_trace_id));
              }
            } else {
              rpc_req->client_timestamp_.client_write_begin_ = write_begin_;
              rpc_req->client_timestamp_.client_end_ = write_done;
            }
            rpc_req->cnet_sm_ = NULL; // client has done to clean it to avoid coredump when client exit
            if (OB_SUCC(ret)) {
              if (rpc_req->is_need_terminal_client_net()) {
                need_terminal = true;
              }
              ObRpcReq::ObRpcReqCleanupParams cleanup_params(ObRpcReq::ClientNetState::RPC_REQ_CLIENT_DONE);
              rpc_req->cleanup(cleanup_params);
              rpc_req = NULL;
            }
          } else {
            //do nothing
            PROXY_CS_LOG(DEBUG, "ObRpcServerNetHandler::state_client_response_send rpc_req is NULL", K_(cs_id), K(event), K(data));
          }
        }
        sending_response_list_.reset();
        write_begin_ = 0;

        break;
      case VC_EVENT_READ_READY:
      case VC_EVENT_READ_COMPLETE:
        //do nothing, not to be here
        break;
      case VC_EVENT_EOS:
        net_entry_.eos_ = true;
        break;
      case VC_EVENT_ERROR:
        ret = OB_CONNECT_ERROR;
        //may entry if broken, request need retry
        break;
      default:
        ret = OB_INNER_STAT_ERROR;
        PROXY_CS_LOG(EDIAG,"Unknown event", K_(cs_id), K(event), K(ret));
        break;
    }
  }

  if (OB_FAIL(ret) || need_terminal || net_entry_.eos_) {
    PROXY_CS_LOG(WDIAG, "state_client_response_send failed or get need_terminal", K_(cs_id), K(ret), K(need_terminal), K(event));
    do_io_close();
  } else if (OB_SUCC(ret) && need_send_response_list_.size() > 0) {
    // Responses are returned concurrently on a single connection. Here you need to check whether there are any responses that have not been returned.
    PROXY_CS_LOG(DEBUG, "state_client_response_send need_send_response is not empty, send again", K_(cs_id), K(ret), K_(need_send_response_list));
    if (OB_FAIL(setup_client_response_send())) {
      PROXY_CS_LOG(WDIAG, "fail to send again", K_(cs_id), K(ret), K_(need_send_response_list));
    }
  }

  return ret;
}

int ObRpcOBKVClientNetHandler::handle_response_rewrite_channel_id(ObRpcReq *rpc_req)
{
  int ret = OB_SUCCESS;
  const ObRpcReqTraceId &rpc_trace_id = rpc_req->get_trace_id();
  uint32_t request_id = rpc_req->get_origin_channel_id();
  char *buf = NULL;
  int64_t buf_len = 0;
  int64_t pos = ObRpcEzHeader::RPC_PKT_CHANNEL_ID_POS;

  if (rpc_req->is_use_response_inner_buf()) {
    buf = rpc_req->get_response_inner_buf();
    buf_len = rpc_req->get_response_inner_buf_len();
  } else {
    buf = rpc_req->get_response_buf();
    buf_len = rpc_req->get_response_buf_len();
  }

  if (OB_ISNULL(buf) || 0 == buf_len) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_CS_LOG(WDIAG, "rpc req response buf is invalid", K_(cs_id), K(rpc_trace_id));
  } else if (OB_FAIL(common::serialization::encode_i32(buf, buf_len, pos, request_id))) {
    PROXY_CS_LOG(WDIAG, "fail to encode response channel id", K_(cs_id), K(rpc_trace_id));
  }

  return ret;
}

int ObRpcOBKVClientNetHandler::schedule_send_response_action()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(NULL != pending_action_)) {
    // do nothing
    PROXY_LOG(DEBUG, "pending send_response_action, do nothing", K_(cs_id), K_(pending_action), K(ret));
  } else if (OB_UNLIKELY(create_thread_ != NULL && this_ethread() != create_thread_)) {
    //need to check it
    // there are many sub module in rpc process, but notify caller is not check if the sub module is in the same thread
    // for example, the table entry cont, so here need to check and adjust log level to WDIAG
    PROXY_CS_LOG(EDIAG, "fail to schedule_send_response_action for client, need scheduled by created_ethread",
                K_(cs_id), K_(create_thread), "current_thread", self_ethread());
    if (OB_ISNULL(pending_action_ = create_thread_->schedule_imm(this, RPC_CLIENT_NET_SEND_RESPONSE))) {
      ret = OB_ERR_UNEXPECTED;
      PROXY_LOG(EDIAG, "fail to schedule send_response", K_(cs_id), K_(pending_action), K(ret));
    } else {
      PROXY_LOG(DEBUG, "succ to schedule send_response for ObRpcOBKVClientNetHandler", K_(cs_id), K(pending_action_), K(this));
    }
  } else if (OB_ISNULL(pending_action_ = self_ethread().schedule_imm(this, RPC_CLIENT_NET_SEND_RESPONSE))) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_LOG(EDIAG, "fail to schedule send_response", K_(cs_id), K_(pending_action), K(ret));
  } else {
    PROXY_LOG(DEBUG, "succ to schedule send_response for ObRpcOBKVClientNetHandler", K_(cs_id), K(pending_action_), K(this));
  }
  return ret;
}

int ObRpcOBKVClientNetHandler::cancel_pending_action()
{
  int ret = OB_SUCCESS;

  if (NULL != pending_action_) {
    if (OB_FAIL(pending_action_->cancel())) {
      PROXY_LOG(WDIAG, "fail to cancel pending task", K_(cs_id), K_(pending_action), K(ret));
    } else {
      pending_action_ = NULL;
    }
  }

  return ret;
}

void ObRpcOBKVClientNetHandler::clean_all_pending_request()
{
  RPC_PKT_REQ_MAP::iterator iter = cid_to_req_map_.begin();
  ObRpcReq *rpc_req = NULL;
  for (; iter != cid_to_req_map_.end(); iter++) {
    if (OB_NOT_NULL(rpc_req = iter->second)) {
      PROXY_CS_LOG(INFO, "client net handle do io close, clean pending rpc req", K_(cs_id), KPC(rpc_req));
      rpc_req->client_net_cancel_request();  // client net done
      ObRpcReq::ObRpcReqCleanupParams cleanup_params(ObRpcReq::ClientNetState::RPC_REQ_CLIENT_CANCLED);
      rpc_req->cleanup(cleanup_params);
      rpc_req = NULL;
    }
  }
  cid_to_req_map_.destroy();
}

void ObRpcOBKVClientNetHandler::clean_all_timeout_request()
{
  int ret = OB_SUCCESS;
  RPC_PKT_REQ_MAP::iterator iter = cid_to_req_map_.begin();
  ObRpcReq *rpc_req = NULL;
  int64_t current_time_us = common::ObTimeUtility::current_time();
  ObRpcReqList clean_list;
  uint32_t key = 0;

  PROXY_LOG(DEBUG, "ObRpcOBKVClientNetHandler::clean_all_timeout_request", K_(cs_id));

  for (;OB_SUCC(ret) && iter != cid_to_req_map_.end(); iter++) {
    if (OB_NOT_NULL(rpc_req = iter->second) && rpc_req->get_cnet_state() < ObRpcReq::ClientNetState::RPC_REQ_CLIENT_RESPONSE_HANDLING) {
      if ((0 != rpc_req->get_client_net_timeout_us() &&
           rpc_req->get_client_net_timeout_us() < current_time_us) ||
          rpc_req->canceled()) {
        if (OB_FAIL(clean_list.push_back(rpc_req))) {
          PROXY_CS_LOG(WDIAG, "fail to push rpc request to clean list", K_(cs_id), K(ret), K(rpc_req));
        }
        rpc_req = NULL;
      }
    }
  }

  while (OB_SUCC(ret) && !clean_list.empty()) {
    if (OB_FAIL(clean_list.pop_front(rpc_req))) {
      PROXY_LOG(WDIAG, "fail to pop need clean request", K_(cs_id), K(ret));
    } else if (OB_NOT_NULL(rpc_req)) {
      key = rpc_req->get_client_channel_id();
      if (OB_FAIL(cid_to_req_map_.erase_refactored(key))) {
        PROXY_LOG(WDIAG, "fail to call erase_refactored for clean_list, do nothing", K_(cs_id), K(ret), KPC(rpc_req));
      } else {
        const ObRpcReqTraceId &rpc_trace_id = rpc_req->get_trace_id();
        PROXY_LOG(INFO, "ObRpcOBKVClientNetHandler::clean_all_timeout_request clean rpc_req", K_(cs_id), KPC(rpc_req), K(rpc_trace_id));
        rpc_req->client_net_cancel_request();  // client net done
        ObRpcReq::ObRpcReqCleanupParams cleanup_params(ObRpcReq::ClientNetState::RPC_REQ_CLIENT_CANCLED);
        rpc_req->cleanup(cleanup_params);
      }
      rpc_req = NULL;
    }
  }
}

int ObRpcOBKVClientNetHandler::schedule_period_task()
{
  int ret = OB_SUCCESS;
  ObHRTime period_task_time = HRTIME_USECONDS(get_global_proxy_config().rpc_period_task_interval);

  if (OB_UNLIKELY(NULL != period_task_action_)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_LOG(WDIAG, "period_task_action must be NULL here", K_(cs_id), K_(period_task_action), K(ret));
  } else if (OB_ISNULL(create_thread_) || create_thread_ != this_ethread()) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_LOG(WDIAG, "ObRpcOBKVClientNetHandler::schedule_period_task get wrong thread", K_(cs_id), KP_(create_thread), KP(this_ethread()));
  } else if (OB_ISNULL(period_task_action_ = self_ethread().schedule_every(this, period_task_time, RPC_CLIENT_NET_PERIOD_TASK))) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_LOG(EDIAG, "fail to schedule timeout", K_(cs_id), K(period_task_action_), K(ret));
  } else {
    PROXY_LOG(DEBUG, "succ to schedule repeat task for ObRpcOBKVClientNetHandler", K_(cs_id), K(period_task_time));
  }

  return ret;
}

int ObRpcOBKVClientNetHandler::cancel_period_task()
{
  int ret = OB_SUCCESS;

  if (NULL != period_task_action_) {
    if (OB_FAIL(period_task_action_->cancel())) {
      PROXY_LOG(WDIAG, "fail to cancel repeat task", K_(cs_id), K_(period_task_action), K(ret));
    } else {
      period_task_action_ = NULL;
    }
  }

  return ret;
}

int ObRpcOBKVClientNetHandler::handle_period_task()
{
  int ret = OB_SUCCESS;

  PROXY_LOG(DEBUG, "ObRpcOBKVClientNetHandler::handle_period_task", K_(cs_id));
  // 1. clean timeout request
  clean_all_timeout_request();

  return ret;
}

int ObRpcOBKVClientNetHandler::handle_client_entry_setup_error(int event, void *data)
{
  int ret = OB_SUCCESS;
  UNUSED(event);
  UNUSED(data);
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
