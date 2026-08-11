/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_mysql_response_compress_transform_plugin.h"
#include "obproxy/obutils/ob_resource_pool_processor.h"
#include "proxy/mysqllib/ob_protocol_diagnosis.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

ObMysqlResponseCompressTransformPlugin *ObMysqlResponseCompressTransformPlugin::alloc(ObApiTransaction &transaction)
{
  return op_reclaim_alloc_args(ObMysqlResponseCompressTransformPlugin, transaction);
}

ObMysqlResponseCompressTransformPlugin::ObMysqlResponseCompressTransformPlugin(ObApiTransaction &transaction)
  : ObTransformationPlugin(transaction, ObTransformationPlugin::RESPONSE_TRANSFORMATION),
    req_seq_(0), request_id_(0), server_sessid_(0),
    local_reader_(NULL), local_transfer_reader_(NULL)
{
  protocol_ = sm_->get_server_session_protocol();
  // get request seq
  req_seq_ = sm_->get_compressed_or_ob20_request_seq();
   // save sm_->server_session info now otherwise in `consume()` sm_->server_session has been reset
  ObMysqlServerSession *server_session = sm_->get_server_session();
  request_id_ = server_session->get_server_request_id();
  server_sessid_ = server_session->get_server_sessid();
  is_compressed_ob20_ = server_session->get_session_info().is_server_ob20_compress_supported()
                        && 0 != sm_->compression_algorithm_.level_;
  DEC_AND_INC_SHARED_REF(resp_analyzer_.get_protocol_diagnosis_ref(), sm_->protocol_diagnosis_);
  PROXY_API_LOG(DEBUG, "ObMysqlResponseCompressTransformPlugin born", K(protocol_), K_(req_seq),
                K_(request_id), K_(server_sessid), K(this));
}

void ObMysqlResponseCompressTransformPlugin::destroy()
{
  PROXY_API_LOG(DEBUG, "ObMysqlResponseCompressTransformPlugin destroy", K(this));
  ObTransformationPlugin::destroy();
  op_reclaim_free(this);
}

int ObMysqlResponseCompressTransformPlugin::consume(event::ObIOBufferReader *reader)
{
  PROXY_API_LOG(DEBUG, "ObMysqlResponseCompressTransformPlugin::consume happen");
  int ret = OB_SUCCESS;
  int64_t consume_size = 0;
  int64_t produce_size = 0;
  int64_t read_avail = 0;

  if (NULL == local_reader_) {
    local_reader_ = reader->clone();
  }

  if (OB_NOT_NULL(sm_->protocol_diagnosis_)) {
    sm_->protocol_diagnosis_->resp_forward_data_flow_.plugin_decompress_read_ += reader->read_avail();
    PROTOCOL_FORWARD_LOG(TRACE, "plugin_decompress read compressed response",
      "plugin_decompress_read",
      sm_->protocol_diagnosis_->resp_forward_data_flow_.plugin_decompress_read_);
  }

  const obmysql::ObMySQLCmd cmd = sm_->trans_state_.trans_info_.client_request_.get_packet_meta().cmd_;
  const ObMysqlProtocolMode protocol_mode = sm_->client_session_->get_session_info().is_oracle_mode() ?
                                            OCEANBASE_ORACLE_PROTOCOL_MODE : OCEANBASE_MYSQL_PROTOCOL_MODE;
  const bool enable_trans_checksum = sm_->client_session_->get_session_info().get_enable_transmission_checksum();
  if (resp_analyzer_.is_inited()) {
  } else if (OB_FAIL(resp_analyzer_.init(protocol_, cmd, protocol_mode, ObRespAnalyzeMode::DECOMPRESS_MODE,
                                         sm_->is_extra_ok_packet_for_stats_enabled(), is_compressed_ob20_,
                                         req_seq_, req_seq_, request_id_, server_sessid_,
                                         enable_trans_checksum))) {
    PROXY_API_LOG(EDIAG, "fail to init resp analyzer", K(ret));
  }


  ObRespAnalyzeResult &resp_result = sm_->trans_state_.trans_info_.resp_result_;
  if (OB_SUCC(ret)) {
    if (NULL == local_transfer_reader_) {
      local_transfer_reader_ = resp_analyzer_.alloc_mysql_pkt_reader();
    }

    // [TODO] 这里 Tunnel 和 Plugin 共用一个数据结构存在潜在风险
    // the ObRespAnalyzeResult will be use both by tunnel and this class, and if tunnel analyze finished,
    // is_resp_completed_ will set true, here we set back to false to ensure compress_analyzer
    // work happy.
    resp_result.set_is_resp_completed(false);

    int64_t plugin_decompress_response_begin = sm_->get_based_hrtime();
    read_avail = local_reader_->read_avail();
    PROXY_API_LOG(DEBUG, "[ObMysqlResponseCompressTransformPlugin] local_reader_", K(read_avail));
    if (OB_FAIL(resp_analyzer_.analyze_response(*local_reader_, resp_result))) {
      PROXY_API_LOG(EDIAG, "fail to analyze mysql compress", K_(local_reader), K(ret));
    } else if (OB_FAIL(local_reader_->consume(read_avail))) {// consume all input data anyway
      PROXY_API_LOG(WDIAG, "fail to consume", K(consume_size), K(ret));
    } else {
      consume_size = local_transfer_reader_->read_avail();

      int64_t plugin_decompress_response_end = sm_->get_based_hrtime();
      sm_->cmd_time_stats_.plugin_decompress_response_time_ +=
        milestone_diff(plugin_decompress_response_begin, plugin_decompress_response_end);

      // save flt from response analyze result to sm
      sm_->save_response_flt_result_to_sm(resp_result.get_flt());
      if (OB_FAIL(sm_->handle_feedback_proxy_info(resp_result.get_extra_info()))) {
        PROXY_API_LOG(WDIAG, "fail to handle feedback proxy info", "sm_id", sm_->sm_id_,
                      "result", resp_result, K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && consume_size > 0) {
    // all data has recevied complete, and decompress totally
    if (!resp_result.is_last_ok_handled()
        && resp_result.get_last_ok_pkt_len() > 0) {
      // only resultset will use transform to decompress, so we just trim last ok packet
      if (OB_FAIL(sm_->trim_ok_packet(*local_transfer_reader_))) {
        PROXY_API_LOG(WDIAG, "fail to trim last ok packet", K(ret));
      } else {
        resp_result.set_is_last_ok_handled(true);
        if (OB_NOT_NULL(sm_->protocol_diagnosis_)) {
          if (resp_result.get_ok_packet_action_type() == OK_PACKET_ACTION_CONSUME) {
            sm_->protocol_diagnosis_->record_resp_forward_ctrl_flow(ObRespForwardCtrlFlow::PLUGIN_DECOMPRESS_TRIM_EXTRA_OK);
            sm_->protocol_diagnosis_->resp_forward_data_flow_.plugin_trim_ok_ = resp_result.get_last_ok_pkt_len();
            PROTOCOL_FORWARD_LOG(TRACE, "plugin trim response extra ok",
              "plugin_trim_ok", sm_->protocol_diagnosis_->resp_forward_data_flow_.plugin_trim_ok_);
          } else if (resp_result.get_ok_packet_action_type() == OK_PACKET_ACTION_REWRITE) {
            sm_->protocol_diagnosis_->record_resp_forward_ctrl_flow(ObRespForwardCtrlFlow::PLUGIN_DECOMPRESS_REWRITE_LAST_OK);
            sm_->protocol_diagnosis_->resp_forward_data_flow_.plugin_rewrite_ok_delta_ =
              (resp_result.get_rewritten_last_ok_pkt_len() - resp_result.get_last_ok_pkt_len());
            PROTOCOL_FORWARD_LOG(TRACE, "plugin rewrite response last ok",
              "plugin_rewrite_last_ok_delta", sm_->protocol_diagnosis_->resp_forward_data_flow_.plugin_rewrite_ok_delta_);
          }
        }
      }
      sm_->print_mysql_complete_log(NULL);
    }

    if (OB_SUCC(ret)) {
      // the current state may be has already set to CMD_COMPLETE in tunnel by thre
      // producer of server session, when received complete. but it not decompressed,
      // so we should set TRANSACTION_COMPLETE here.
      if (resp_result.is_trans_completed()) {
        sm_->trans_state_.current_.state_ = ObMysqlTransact::TRANSACTION_COMPLETE;
      } else if (resp_result.is_resp_completed()) {
        sm_->trans_state_.current_.state_ = ObMysqlTransact::CMD_COMPLETE;
      }
      if (sm_->enable_record_full_link_trace_info()) {
        if (resp_result.is_trans_completed() || resp_result.is_resp_completed()) {
          trace::ObSpanCtx *ctx = sm_->flt_.trace_log_info_.server_response_read_ctx_;
          if (OB_NOT_NULL(ctx)) {
            // set show trace buffer before flush trace
            if (sm_->flt_.control_info_.is_show_trace_enable()) {
              SET_SHOW_TRACE_INFO(&sm_->flt_.show_trace_json_info_.curr_sql_json_span_array_);
            }
            PROXY_API_LOG(DEBUG, "end span ob_proxy_server_response_read", K(ctx->span_id_));
            SET_TRACE_BUFFER(sm_->flt_trace_buffer_, MAX_TRACE_LOG_SIZE);
            FLT_END_SPAN(ctx);
            sm_->flt_.trace_log_info_.server_response_read_ctx_ = NULL;
          }
        }
      }

      // get consume size again, for trim the last packet
      consume_size = local_transfer_reader_->read_avail() - resp_result.get_reserved_ok_len_of_compressed();

      // Here is a situation:
      //   under the 2.0 protocol, the last 4 tail checksum bytes were not read,
      //   but the Tunnel sent all the MySQL content.
      //   For ClientVC, after receiving all MySQL content,
      //   it does not judge the end of the response based on the content,
      //   but judges based on write_state_.vio_.ntodo(),
      //   but because Tunnel has not read the last 4 tail checksum bytes,
      //   Tunnel will not modify nbytes in ntodo,
      //   causing ClientVC to think Have not finished receiving, continue to wait, do not continue to process.
      //   After the Tunnel receives the last 4 tail checksum bytes,
      //   since these 4 bytes are not sent to the Client,
      //   the Client VC will not be triggered again.
      //   As a result, the Tunnel ends directly, and the ClientVC How Hung lives
      //
      // Therefore, it is modified here that if the entire Tunnel is not over,
      //   the last bit of MySQL packet content will not be sent, and will not be sent until the entire Tunnel is over
      //   And with consume_size > 0 or resp_result.get_reserved_ok_len_of_compressed() == 0

      // 原判断为 (!resp_result.is_last_ok_handled() || resp_analyzer_.is_stream_end()) 这个条件可能两个值均为 False,
      // 最后一个 OK Packet 已经重写或者裁剪掉了,is_last_ok_handled() == True,
      // 但是 checksum tailer 并没有读取解析, is_stream_end() == False
      // 这样整个表达式就为 False, 就不会调用到 produce 往下游写入数据
      // 改为 is_stream_end -> is_resp_completed, 这里可以调用到 produce 写入数据, 可以确保将 MySQL Packet 写入到下游
      if ((!resp_result.is_last_ok_handled() || resp_result.is_resp_completed())
          && (consume_size > 0 || resp_result.get_reserved_ok_len_of_compressed() == 0)) {
        // just send all data in local_transfer_reader_
        if (consume_size != (produce_size = produce(local_transfer_reader_, consume_size))) {
          ret = OB_ERR_UNEXPECTED;
          PROXY_API_LOG(WDIAG, "fail to produce", "expected size", consume_size,
                        "actual size", produce_size, K(ret));
        } else if (OB_FAIL(local_transfer_reader_->consume(consume_size))) {
          PROXY_API_LOG(WDIAG, "fail to consume local transfer reader", K(consume_size), K(ret));
        } else {
          if (OB_NOT_NULL(sm_->protocol_diagnosis_)) {
            sm_->protocol_diagnosis_->resp_forward_data_flow_.plugin_decompress_write_ += consume_size;
            sm_->protocol_diagnosis_->resp_forward_data_flow_.plugin_decompress_decrease_ += (
              sm_->protocol_diagnosis_->resp_forward_data_flow_.plugin_decompress_read_ -
              sm_->protocol_diagnosis_->resp_forward_data_flow_.plugin_decompress_write_
            );
            PROTOCOL_FORWARD_LOG(TRACE, "plugin_decompress write mysql response",
              "plugin_decompress_write", sm_->protocol_diagnosis_->resp_forward_data_flow_.plugin_decompress_write_,
              "write_delta", consume_size);
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    sm_->trans_state_.inner_errcode_ = ret;
    // if failed, set state to INTERNAL_ERROR
    sm_->trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
  }

  return ret;
}

void ObMysqlResponseCompressTransformPlugin::handle_input_complete()
{
  PROXY_API_LOG(DEBUG, "ObMysqlResponseCompressTransformPlugin::handle_input_complete happen");

  if (OB_NOT_NULL(sm_->protocol_diagnosis_)) {
    sm_->protocol_diagnosis_->record_resp_forward_ctrl_flow(ObRespForwardCtrlFlow::PLUGIN_DECOMPRESS_FINISH);
    PROTOCOL_FORWARD_LOG(TRACE, "plugin_decompress process response finish");
  }

  if (NULL != local_reader_) {
    local_reader_->dealloc();
    local_reader_ = NULL;
  }
  if (NULL != local_transfer_reader_) {
    local_transfer_reader_->dealloc();
    local_transfer_reader_ = NULL;
  }
  set_output_complete();
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
