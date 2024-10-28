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
#include "lib/utility/ob_print_utils.h"
#include "proxy/api/ob_plugin.h"
#include "obutils/ob_resource_pool_processor.h"
#include "obutils/ob_config_server_processor.h"
#include "dbconfig/ob_proxy_pb_utils.h"
#include "proxy/mysql/ob_mysql_global_session_manager.h"
#include "omt/ob_conn_table_processor.h"
#include "omt/ob_white_list_table_processor.h"
#include "proxy/rpc/net/ob_rpc_redis_client_net_handler.h"
#include "proxy/rpc/net/ob_rpc_server_net_handler.h"
#include "proxy/rpc/ob_rpc_req_debug_names.h"
#include "proxy/rpc/ob_rpc_request_sm.h"
#include "proxy/rpc/ob_rpc_req.h"
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

ObRpcRedisClientNetHandler::ObRpcRedisClientNetHandler()
    : ObRpcClientNetHandler(), cur_rpc_request_(NULL), is_in_handling_request_(false),
      redis_db_(0), rpc_credential_(), credential_()
{
  SET_HANDLER(&ObRpcRedisClientNetHandler::main_handler);
}

void ObRpcRedisClientNetHandler::destroy()
{
  PROXY_CS_LOG(INFO, "rpc client session destroy", K_(cs_id), K_(proxy_sessid), KP_(rpc_net_vc));

  ObRpcRedisClientNetHandler::cleanup();

  op_reclaim_free(this);
}

void ObRpcRedisClientNetHandler::do_io_close(const int alerrno)
{
  if (OB_NOT_NULL(cur_rpc_request_)) {
    cur_rpc_request_->client_net_cancel_request();
    if (OB_NOT_NULL(cur_rpc_request_->get_request_sm())) {
      ObRpcReq::ObRpcReqCleanupParams cleanup_params(ObRpcReq::ClientNetState::RPC_REQ_CLIENT_CANCLED);
      cur_rpc_request_->cleanup(cleanup_params);
    } else {
      cur_rpc_request_->destroy();
    }
    cur_rpc_request_ = NULL;
  }
  ObRpcClientNetHandler::do_io_close(alerrno);

}

int ObRpcRedisClientNetHandler::main_handler(int event, void *data)
{
  int event_ret = VC_EVENT_CONT;
  PROXY_CS_LOG(DEBUG, "[ObRpcRedisClientNetHandler::main_handler]",
            K_(cs_id),
            "event_name", ObRpcReqDebugNames::get_event_name(event),
            "read_state", get_read_state_str(), K(data));

  if (OB_LIKELY(RPC_C_NET_MAGIC_ALIVE == magic_)) {
    if (RPC_CLIENT_NET_PERIOD_TASK == event) {
      // event_ret = handle_period_task();
      //do nothing now
    } else if (RPC_CLIENT_NET_SEND_RESPONSE == event) {
      event_ret = setup_client_response_send();
    } else {
      if (NULL != data && data == net_entry_.read_vio_) { // from client vc
        if (!(is_in_handling_request_ && (VC_EVENT_READ_COMPLETE == event || VC_EVENT_READ_READY == event))) {
          event_ret = state_keep_alive(event, data);
        }
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

int ObRpcRedisClientNetHandler::setup_client_request_read()
{
  int ret = OB_SUCCESS;
  //set read trigger and read_reschedule. sometimes the data already is in the io buffer
  static_cast<ObUnixNetVConnection *>(this->get_netvc())->set_read_trigger();

  // int64_t read_num = 16; //RPC header len
  int64_t read_num = INT64_MAX; //just header for RPC service

  if (OB_ISNULL(net_entry_.read_vio_ = this->do_io_read(this, read_num, buf_reader_->mbuf_))) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_CS_LOG(WDIAG, "rpc client net handler failed to do_io_read", K_(cs_id), K(ret));
  } else {
    if (buf_reader_->read_avail() > 0) {
      PROXY_CS_LOG(DEBUG, "the request already in buffer, continue to handle it",
              K_(cs_id), "buffer len", buf_reader_->read_avail());
      state_client_request_read(VC_EVENT_READ_READY, net_entry_.read_vio_);
    }
  }

  return ret;
}

int ObRpcRedisClientNetHandler::state_client_request_read(int event, void *data)
{
  int ret = OB_SUCCESS;
  int event_ret = VC_EVENT_NONE;
  obkv::ObProxyRpcType rpc_type = obkv::OBPROXY_RPC_UNKOWN;
  UNUSED(event_ret);

  STATE_ENTER(ObRpcRedisClientNetHandler::state_client_request_read, event, data);

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
        PROXY_CS_LOG(INFO, "ObRpcRedisClientNetHandler::state_client_request_read", "event", "set event name",
                 K_(cs_id), "client_vc", P(this->get_netvc()));
        break;
      }
      case VC_EVENT_ACTIVE_TIMEOUT:
      case VC_EVENT_ERROR: {
        PROXY_CS_LOG(WDIAG, "ObRpcRedisClientNetHandler::state_client_request_read", "event",
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

    //check the buffer
    if (OB_SUCC(ret) && OB_NOT_NULL(get_reader())) {
      ObRpcRequestSM *request_sm = NULL;
      ObRpcReqReadStatus status = RPC_REQUEST_READ_CONT;
      ObIOBufferReader &buffer_reader = *get_reader();
      ObRpcReqTraceId rpc_trace_id;
      int64_t trace_id1;
      int64_t trace_id2;
      int64_t current_need_read_len = 4;
      int64_t read_len = buffer_reader.read_avail();
      int64_t origin_pos = 0;
      uint64_t request_len = 0;
      uint32_t client_channel_id = 0;
      // const char * pos_ptr = NULL;
      /*tmp*/
      // int32_t data_pos_ = 0;
      // int32_t cmd_arr_len_ = 0;
      // int32_t  data_length_= 0;
      // int32_t  valid_data_length_ = 0;
      // int32_t next_vec_idx_ = 0;
      bool need_to_parse_bulk_str = false;
      /*end tmp*/
      read_len = buffer_reader.read_avail();
      ObCurNewTraceId::NewTraceId new_trace_id;
      const ObAddr &client_addr = get_real_client_addr();
      new_trace_id.init(client_addr);
      trace_id1 = *(new_trace_id.get());

      if (OB_ISNULL(cur_rpc_request_) && OB_ISNULL(cur_rpc_request_ = ObRpcReq::allocate())) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_CS_LOG(WDIAG, "could not allocate request, abort connection if need", K_(cs_id), K(ret));
      }

      if (OB_SUCC(ret)
          && OB_ISNULL(cur_rpc_request_->get_redis_info())
          && OB_FAIL(cur_rpc_request_->init_rpc_redis_info())) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_CS_LOG(WDIAG, "could not init request redis info, abort connection if need", K_(cs_id), K(ret));
      }

      if (OB_SUCC(ret)) {
        //0. check ArrayLength.inited
        ObRpcRedisCmdInfo &redis_cmd_info = cur_rpc_request_->get_redis_info()->redis_cmd_info_;
        ObRpcRedisInfo *redis_info = cur_rpc_request_->get_redis_info();
        ARR_ARGS *redis_arr_args = NULL;
        if (OB_ISNULL(redis_arr_args = redis_info->get_redis_args())) {
          ret = common::OB_ERR_UNEXPECTED;
          PROXY_CS_LOG(WDIAG, "could not get redis info args, abort connection if need", K_(cs_id), K(ret));
        } else {
          char *request_buf = redis_info->get_request_buf();//cur_rpc_request_->get_request_buf();
          if (redis_info->get_request_buf_len() <= redis_cmd_info.data_len_ + read_len) {
            redis_info->realloc_request_buf(redis_cmd_info.data_len_ + read_len + 1024); // alloc more buffer to read data
            request_buf = redis_info->get_request_buf();
          }

          if (redis_cmd_info.redis_cmd_arr_len_ == 0) { //TODO update
            if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
              // read_begin_ = ObRpcRequestSM::static_get_based_hrtime();
              cur_rpc_request_->client_timestamp_.client_begin_ = ObRpcRequestSM::static_get_based_hrtime();
            }
            //1.init length of Array
            if (buffer_reader.read_avail() < current_need_read_len) { //*${x}\r\n
              status = RPC_REQUEST_READ_CONT;
              PROXY_CS_LOG(DEBUG, "data not meet need", K_(cs_id), "avail_len", buffer_reader.read_avail());
            } else {
              const char *tmp_str = NULL;
              buffer_reader.copy(request_buf + redis_cmd_info.data_len_, read_len);
              origin_pos = redis_cmd_info.data_len_;
              redis_cmd_info.data_len_ += read_len;
              while (OB_NOT_NULL(tmp_str = STRSTR(request_buf + redis_cmd_info.data_pos_, "\r\n")) && !need_to_parse_bulk_str) {
                if (tmp_str == request_buf + redis_cmd_info.data_pos_) {
                  //skip '\r\n' line for first command like '\r\n\r\n*2\r\n$4\r\nAUTH\r\n$3\r\nABC\r\n
                  redis_cmd_info.data_pos_ += 2;
                } else {
                  if (sscanf(request_buf + redis_cmd_info.data_pos_,"*%d", &redis_cmd_info.redis_cmd_arr_len_) != 1) {
                    ret = OB_INVALID_ARGUMENT; // invalid protocol to parser
                    PROXY_CS_LOG(WDIAG, "invalid to to fetch argc of parameter", K(ret), "str_buf", request_buf + redis_cmd_info.data_pos_);
                  }
                  /* to init argc */
                  redis_cmd_info.data_pos_ = tmp_str - request_buf + 2;
                  need_to_parse_bulk_str = true;
                }
              }
              if (!need_to_parse_bulk_str) {
               current_need_read_len += 1;
               status = RPC_REQUEST_READ_CONT;
               PROXY_CS_LOG(DEBUG, "data not meet need", K_(cs_id), "avail_len", buffer_reader.read_avail());
              }
            }
            // redis_request:
            //  cmd_arr_len_        int32_t  /*length of Array*/
            //  bulk_str_vec_       vector<int32_t>
            //  bulk_str_len_vec_   vector<int32_t>
            //  next_vec_idx_        int32_t  /* init */
            //  buffer_length_
            //  data_length_
            //  valid_data_length_
            //  data_pos_
            //    buffer: [ * 2 \r \n $ 4 \r \n a u t h \r \n $ 1 0 \r \n a b c \r \n] //such as not any date has read to buffer
            /**    cmd_arr_len_ = 2
             *     bulk_str_vec_      [ 9 ]
             *     bulk_str_len_vec_  [ 4 ]
             *     next_vec_idx_  = 1
             *     buffer_length_ = 1024 [default]
             *     data_length_ = 24
             *     valid_data_length_ = 19  #valid data has parsed which could consume from net
             *     data_pos_ = 19           #parsed position
             */
          } else {
            buffer_reader.copy(request_buf + redis_cmd_info.data_len_, read_len);
            origin_pos = redis_cmd_info.data_len_;
            redis_cmd_info.data_len_ += read_len;
            need_to_parse_bulk_str = true;
          }

          while (OB_SUCC(ret) && need_to_parse_bulk_str
                    && (redis_cmd_info.next_bulk_str_idx_ < redis_cmd_info.redis_cmd_arr_len_)
                    && (redis_cmd_info.data_pos_ < redis_cmd_info.data_len_)) {
            int32_t data_len = 0;
            int sret = 0;
            const char *tmp_str = NULL;
            // PROXY_CS_LOG(DEBUG, "tmp to to fetch argc of parameter - -", K(ret), K(data_len), K(redis_cmd_info.next_bulk_str_idx_), K(redis_cmd_info.redis_cmd_arr_len_), K(sret), K(request_buf), K(request_buf + redis_cmd_info.data_pos_), K(redis_cmd_info.data_pos_));
            if (OB_NOT_NULL(tmp_str = STRSTR(request_buf + redis_cmd_info.data_pos_, "\r\n"))) {
              int bulk_num_len = tmp_str - request_buf - redis_cmd_info.data_pos_ + 2;
              if (OB_UNLIKELY(bulk_num_len == 2)) { // tmp_str == (request_buf + redis_cmd_info.data_pos_)
                //to skip '\r\n' line
                redis_cmd_info.data_pos_ += 2;
              } else {
                if ((sret = sscanf(request_buf + redis_cmd_info.data_pos_,"$%d", &data_len)) != 1) {
                  ret = OB_INVALID_ARGUMENT; // invalid protocol to parser
                  PROXY_CS_LOG(WDIAG, "invalid to to fetch argc of parameter", K(ret), K(data_len), K(sret), K(request_buf), K(request_buf + redis_cmd_info.data_pos_), K(redis_cmd_info.data_pos_));
                } else {
                  if (redis_cmd_info.data_len_  >= redis_cmd_info.data_pos_ + bulk_num_len + data_len + 2) { //TODO read_len need rewrite to redis_cmd_info.data_len_
                    // read a new str bulk string
                    // ObString args;
                    // args.assign((char *)(request_buf + bulk_num_len + redis_cmd_info.data_pos_), data_len);
                    // redis_arr_args->push_back(args);
                    redis_cmd_info.redis_bulk_str_arr_.push_back(bulk_num_len + redis_cmd_info.data_pos_); //str pos
                    redis_cmd_info.redis_bulk_str_len_arr_.push_back(data_len);                            //str len

                    redis_cmd_info.next_bulk_str_idx_ += 1;
                    redis_cmd_info.data_pos_ += bulk_num_len + data_len + 2;
                    //TODO update each
                  } else {
                    current_need_read_len = redis_cmd_info.data_pos_ + bulk_num_len + data_len + 2 - redis_cmd_info.data_len_ ; //$x\r\n
                    need_to_parse_bulk_str = false;
                    PROXY_CS_LOG(DEBUG, "not read enough data to handle", K(redis_cmd_info.data_len_ ), K(redis_cmd_info.data_pos_), K(data_len));
                  }
                }
              }
            } else {
              current_need_read_len = 4; //$x\r\n
              need_to_parse_bulk_str = false;
              PROXY_CS_LOG(DEBUG, "not read meet data");
              //TODO consume data has parsed
            }
          }

          if (redis_cmd_info.next_bulk_str_idx_ == redis_cmd_info.redis_cmd_arr_len_
                && redis_cmd_info.redis_cmd_arr_len_ != 0) {
            status = RPC_REQUEST_READ_DONE;
            rpc_type = obkv::OBPROXY_RPC_REDIS;
          } else if (redis_cmd_info.data_pos_ == redis_cmd_info.data_len_) {
              current_need_read_len = 4; //$x\r\n
              PROXY_CS_LOG(DEBUG, "not read meet data");
          }

          if (status == RPC_REQUEST_READ_DONE) {
            if (OB_UNLIKELY(redis_cmd_info.redis_bulk_str_arr_.count() != redis_cmd_info.redis_bulk_str_len_arr_.count())) {
              ret = OB_ERR_UNEXPECTED;
              PROXY_CS_LOG(WDIAG, "invalid redis cmd handle", "redis_cmd_info_str_pos", redis_cmd_info.redis_bulk_str_arr_,
                           "redis_cmd_info_str_len", redis_cmd_info.redis_bulk_str_len_arr_, K(ret));
            } else {
              int64_t i = 0;
              for (i = 0; i < redis_cmd_info.redis_bulk_str_arr_.count(); i++) {
                ObString args;
                args.assign((char *)(request_buf + redis_cmd_info.redis_bulk_str_arr_.at(i)), redis_cmd_info.redis_bulk_str_len_arr_.at(i));
                redis_arr_args->push_back(args);
              }
              // buffer_reader.consume(redis_cmd_info.data_pos_);
              // request_len = redis_cmd_info.data_len_;
              request_len = redis_cmd_info.data_pos_;
              buffer_reader.consume(redis_cmd_info.data_pos_ - origin_pos);
              redis_info->set_request_len(request_len);
              if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
                // read_begin_ = ObRpcRequestSM::static_get_based_hrtime();
                cur_rpc_request_->client_timestamp_.client_read_end_ = ObRpcRequestSM::static_get_based_hrtime();
              }
            }

          } else {
            // buffer_reader.consume(redis_cmd_info.data_len_);
            buffer_reader.consume(read_len);
          }
        }
      }

      if (OB_FAIL(ret)) {
        status = RPC_REQUEST_READ_ERROR;
      }

      switch (__builtin_expect(status, RPC_REQUEST_READ_DONE)) {
        case RPC_REQUEST_READ_DONE:
          if (OB_NOT_NULL(cur_rpc_request_)) {
            // request_id = current_ez_header_.chid_;
            client_channel_id = atomic_channel_id_++;
            trace_id2 = client_channel_id;

            PROXY_CS_LOG(DEBUG, "[RPC_REQUEST][OB_REDIS]recv a new rpc_req, to handle", K_(cs_id), K(rpc_trace_id), K(client_addr), K(new_trace_id), K(ret), KPC_(cur_rpc_request));
            if (OB_ISNULL(cur_rpc_request_->get_request_sm()) && OB_ISNULL(request_sm = ObRpcRequestSM::allocate())) {
              ret = OB_ERR_UNEXPECTED;
              PROXY_CS_LOG(WDIAG, "could not allocate request sm, net need abort connection", K_(cs_id), K(ret));
            } else if (OB_FAIL(cur_rpc_request_->init(rpc_type, request_sm, this, request_len, cluster_version_,
                                      client_channel_id, client_channel_id, cs_id_, rpc_net_vc_, trace_id1, trace_id2))) {
              PROXY_CS_LOG(WDIAG, "failed to init rpc_req", K_(cs_id), K(rpc_trace_id), K(ret), K(this));
            } else if (OB_FAIL(request_sm->init(cur_rpc_request_, mutex_))) {
              PROXY_CS_LOG(WDIAG, "failed to init request_sm", K_(cs_id), K(rpc_trace_id), K(ret), K(this));
            } else {
              ObRpcRedisInfo *redis_info = cur_rpc_request_->get_redis_info();
              RPC_REQ_CNET_ENTER_STATE(cur_rpc_request_, ObRpcReq::ClientNetState::RPC_REQ_CLIENT_REQUEST_HANDLING);
              is_in_handling_request_ = true;
              if (OB_NOT_NULL(redis_info)) {
                redis_info->set_rpc_credential(get_rpc_credential());
                redis_info->set_redis_db(redis_db_);
              }
              if (OB_FAIL(request_sm->schedule_call_next_action(RPC_REQ_NEW_REDIS_REQUEST))) {
                PROXY_CS_LOG(WDIAG, "fail to call schedule_call_next_action", K(ret), K(request_sm));
              }
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            PROXY_CS_LOG(WDIAG," handle rpc request expected null request", K_(cs_id), K(ret));
          }

          net_entry_.read_vio_->nbytes_ = INT64_MAX;
          net_entry_.read_vio_->reenable(); //need check next data
          current_need_read_len = RPC_NET_HEADER_LENGTH;
          // if (OB_SUCC(ret)) {
          //   PROXY_CS_LOG(DEBUG, "need read next request immediately when request waiting", K_(cs_id), "net_len", buffer_reader.read_avail());
          //   if (OB_FAIL(setup_client_request_read())) {
          //     PROXY_CS_LOG(WDIAG, "fail to call setup_client_request_read", K_(cs_id), K(ret));
          //   }
          // }
          break;
        break;
        case RPC_REQUEST_READ_CONT:
          if (net_entry_.eos_) {
            ret = OB_CONNECT_ERROR;
            PROXY_CS_LOG(WDIAG,"EOS before client request parsing finished", K_(cs_id), K(ret));
            //TODO PRPC client need abort and broken connection
            net_entry_.read_vio_->nbytes_ = net_entry_.read_vio_->ndone_;//client_entry_->read_vio_->ndone_;
          } else {
            if (current_need_read_len > 0 && current_need_read_len > buffer_reader.mbuf_->water_mark_) {
              buffer_reader.mbuf_->water_mark_ = current_need_read_len;
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
    }
  }

  if (OB_FAIL(ret)) {
    //error state, need abort client session
    do_io_close();
  }
  return ret;
}

int ObRpcRedisClientNetHandler::setup_client_response_send()
{
  int ret = OB_SUCCESS;
  bool is_need_quit = false;
  //set read trigger and read_reschedule. sometimes the data already is in the io buffer
  PROXY_CS_LOG(DEBUG, "ObRpcServerNetHandler::setup_client_response send", K_(cs_id));
  static_cast<ObUnixNetVConnection *>(this->get_netvc())->set_read_trigger();
  pending_action_ = NULL;

  if (!is_sending_response_ && OB_NOT_NULL(cur_rpc_request_)) {
    int64_t send_response = 0;
    ObIOBufferReader *buf_start = NULL;
    int64_t written_len = 0;
    const ObRpcReqTraceId &rpc_trace_id = cur_rpc_request_->get_trace_id();
    ObRpcRedisInfo *redis_info = cur_rpc_request_->get_redis_info();

    if (OB_ISNULL(net_entry_.write_buffer_)) {
      net_entry_.write_buffer_ = new_empty_miobuffer(MYSQL_BUFFER_SIZE);
    } else {
      net_entry_.write_buffer_->reset(); //cleanup
      net_entry_.write_buffer_->dealloc_all_readers();
    }

    if (OB_ISNULL(redis_info)) {
      ret = OB_INVALID_ARGUMENT;
      PROXY_CS_LOG(WDIAG, "invalid to handle rpc request as redis info", K(rpc_trace_id), K_(cs_id), K(ret));
    } else if (redis_info->is_need_quit()) {
      //
      is_need_quit = true;
      PROXY_CS_LOG(INFO, "redis client net will be quit", K(rpc_trace_id), K_(cs_id), K(ret));
    } else if (OB_ISNULL(buf_start = net_entry_.write_buffer_->alloc_reader())) {
      ret = OB_ERR_UNEXPECTED;
      PROXY_CS_LOG(WDIAG, "setup_client_response_send failed to allocate iobuffer reader", K_(cs_id), K(ret));
    // } else if (cur_rpc_request_->get_redis_info()->get_response_len() > 0) { //TODO need to update
    } else if (cur_rpc_request_->get_response_len() > 0) { //TODO need to update
      char *buf = NULL;
      // int64_t buf_len = 0;
      if (OB_NOT_NULL(redis_info->get_response_server_ptr()) && redis_info->get_response_len()) {
        //response from server
        buf = redis_info->get_response_server_ptr();
        cur_rpc_request_->set_response_len(redis_info->get_response_len());
      } else if (redis_info->is_use_response_inner_buf()){
        //response from obproxy build
        buf = redis_info->get_response_inner_buf();
      } else {
        buf = redis_info->get_response_buf();
      }

      //for auth request, need save credential info in clientNet
      ObString rpc_credential = redis_info->get_rpc_credential();
      if (redis_info->is_auth_request()) {
        set_rpc_credential(rpc_credential); //set only credential.length > 0
      } else if (redis_info->get_redis_db() != redis_db_) {
        redis_db_ = redis_info->get_redis_db(); //select db executed
      }

      if (OB_FAIL(net_entry_.write_buffer_->write(buf, cur_rpc_request_->get_response_len(), written_len))) {
        PROXY_CS_LOG(WDIAG, "response is not written completely, all rpc_req handle failed", K_(cs_id), K(written_len), "response_len", cur_rpc_request_->get_response_len(), K(rpc_trace_id));
      } else if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
        write_begin_ = ObRpcRequestSM::static_get_based_hrtime(); /* record begin time to write */
      }
      // MUTEX_TRY_LOCK(lock, rpc_net_vc_->mutex_, create_thread_);
      // TODO: Using MUTEX_ LOCK may affect performance. In the future, consider using different mutexes for asynchronous tasks within RPC requests
      // compared to client VC to prevent race conditions
      if (OB_SUCC(ret)) {
        MUTEX_LOCK(lock, rpc_net_vc_->mutex_, create_thread_);    // TODO: check CLIENT_VC_SWAP_MUTEX_EVENT event
        /* check mutex_->thread_holding_ is same with create_thread_ to avoid
          writing failed with net_entry_.write_vio_ in not null */
        if (create_thread_ != mutex_->thread_holding_ || OB_ISNULL(net_entry_.write_vio_ = do_io_write(this, written_len, buf_start))) {
          ret = OB_ERR_UNEXPECTED;
          PROXY_CS_LOG(WDIAG, "client entry failed to do_io_write", K_(cs_id), K(send_response), K(written_len), KP_(create_thread), KP(this_ethread()),
                      KP(mutex_->thread_holding_), K(mutex_.ptr_));
        } else {
          is_sending_response_ = true;
        }
      }
    }
  } else {
    //do nothing
    PROXY_CS_LOG(DEBUG, "client net is in sending state, need to wait complete for last", K_(cs_id));
  }

  if (OB_FAIL(ret) || is_need_quit) {
    do_io_close();
  }

  return ret;
}

int ObRpcRedisClientNetHandler::state_client_response_send(int event, void *data)
{
  int ret = OB_SUCCESS;
  bool need_terminal = false;
  ObRpcReq *rpc_req = cur_rpc_request_;
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
        if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
          write_done = ObRpcRequestSM::static_get_based_hrtime();
        }
        if (OB_NOT_NULL(rpc_req)) {
          rpc_req->client_timestamp_.client_write_begin_ = write_begin_;
          rpc_req->client_timestamp_.client_end_ = write_done;
          if (rpc_req->is_need_terminal_client_net()) {
            need_terminal = true;
          }
          ObRpcReq::ObRpcReqCleanupParams cleanup_params(ObRpcReq::ClientNetState::RPC_REQ_CLIENT_DONE);
          rpc_req->cleanup(cleanup_params); //TODO just need stat and reset
          rpc_req = NULL;
        }
        write_begin_ = 0;
        cur_rpc_request_ = NULL;
        is_in_handling_request_ = false;

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
  } else {
    if (OB_FAIL(setup_client_request_read())) {
      PROXY_CS_LOG(WDIAG, "fail to read next request", K_(cs_id), K(ret));
    }
  }

  return ret;

}

void ObRpcRedisClientNetHandler::add_client_response_request(ObRpcReq *request) {
  // int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_rpc_request_)) {
    cur_rpc_request_ = request;
  } else if (request != cur_rpc_request_) {
    // ret = OB_ERR_UNEXPECTED;
    cur_rpc_request_ = request;
    PROXY_CS_LOG(WDIAG, "invalid to handle other redis request in client net", K_(cs_id));
  }
}

int ObRpcRedisClientNetHandler::schedule_send_response_action()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(NULL != pending_action_)) {
    // do nothing
    PROXY_LOG(DEBUG, "pending send_response_action, do nothing", K_(cs_id), K_(pending_action), K(ret));
  } else if (OB_ISNULL(pending_action_ = self_ethread().schedule_imm(this, RPC_CLIENT_NET_SEND_RESPONSE))) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_LOG(EDIAG, "fail to schedule send_response", K_(cs_id), K_(pending_action), K(ret));
  } else {
    PROXY_LOG(DEBUG, "succ to schedule send_response for ObRpcRedisClientNetHandler", K_(cs_id), K(pending_action_));
  }
  return ret;
}

void ObRpcRedisClientNetHandler::set_rpc_credential(const common::ObString &credential)
{
  if (credential.length() > 0 && credential.length() < 50) {
    MEMCPY(rpc_credential_, credential.ptr(), credential.length());
    credential_.assign(rpc_credential_, credential.length());
  }
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase