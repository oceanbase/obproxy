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

#include "ob_mysql_response_prepare_execute_transform_plugin.h"
#include "ob_mysql_response_cursor_transform_plugin.h"
#include "rpc/obmysql/packet/ompk_prepare.h"
#include "rpc/obmysql/packet/ompk_field.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

ObMysqlResponsePrepareExecuteTransformPlugin *ObMysqlResponsePrepareExecuteTransformPlugin::alloc(ObApiTransaction &transaction)
{
  return op_reclaim_alloc_args(ObMysqlResponsePrepareExecuteTransformPlugin, transaction);
}

ObMysqlResponsePrepareExecuteTransformPlugin::ObMysqlResponsePrepareExecuteTransformPlugin(ObApiTransaction &transaction)
  : ObTransformationPlugin(transaction, ObTransformationPlugin::RESPONSE_TRANSFORMATION),
    local_produce_reader_(NULL), local_analyze_reader_(NULL), local_buffer_(NULL), pkt_reader_(),
    prepare_execute_state_(PREPARE_EXECUTE_HEADER), num_columns_(0), num_params_(0), pkt_count_(0),
    have_cursor_(false), field_types_()
{
  PROXY_API_LOG(DEBUG, "ObMysqlResponsePrepareExecuteTransformPlugin born", K(this));
  // local_buffer_ 的使用目的换一个 ObMIOBuffer 暂存上游过来的数据, 缓急上游 ObMIOBuffer 数据压力
  // 因为 consume() 的参数 ObMIOBuffer 对应的 buffer 如果数据量太大将无法扩容, 继续读取数据会造成数据流 Hung
  if (OB_ISNULL(local_buffer_ = new_empty_miobuffer())) {
    PROXY_API_LOG(EDIAG, "fail to alloc memory for local_buffer_");
  } else if (OB_ISNULL(local_analyze_reader_ = local_buffer_->alloc_reader())) {
    PROXY_API_LOG(EDIAG, "fail to alloc reader of local_buffer_");
  } else if (OB_ISNULL(local_produce_reader_ = local_buffer_->alloc_reader())) {
    PROXY_API_LOG(EDIAG, "fail to alloc reader of local_buffer_");
  }
}

void ObMysqlResponsePrepareExecuteTransformPlugin::free_local_buffer()
{
  if (NULL != local_analyze_reader_) {
    local_analyze_reader_->dealloc();
    local_analyze_reader_ = NULL;
  }

  if (NULL != local_produce_reader_) {
    local_produce_reader_->dealloc();
    local_produce_reader_ = NULL;
  }

  if (NULL != local_buffer_) {
    free_miobuffer(local_buffer_);
    local_buffer_ = NULL;
  }
}
void ObMysqlResponsePrepareExecuteTransformPlugin::destroy()
{
  PROXY_API_LOG(DEBUG, "ObMysqlResponsePrepareExecuteTransformPlugin destroy", K(this));
  ObTransformationPlugin::destroy();
  pkt_reader_.reset();
  free_local_buffer();
  op_reclaim_free(this);
}

int ObMysqlResponsePrepareExecuteTransformPlugin::consume(event::ObIOBufferReader *reader)
{
  PROXY_API_LOG(DEBUG, "ObMysqlResponsePrepareExecuteTransformPlugin::consume happen");
  int ret = OB_SUCCESS;

  int64_t forward_len = 0;   // 往下游 produce 的数据大小
  ObMysqlAnalyzeResult result;
  event::ObIOBufferReader *produce_reader = NULL;

  if (OB_NOT_NULL(sm_->protocol_diagnosis_) && reader != NULL) {
    sm_->protocol_diagnosis_->resp_forward_data_flow_.plugin_prepare_execute_read_ += reader->read_avail();
    PROTOCOL_FORWARD_LOG(TRACE, "plugin_prepare_execute read response",
      "plugin_prepare_execute_read", sm_->protocol_diagnosis_->resp_forward_data_flow_.plugin_prepare_execute_read_);
  }

  if (local_analyze_reader_ == NULL || local_buffer_ == NULL) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_API_LOG(EDIAG, "unexpected null ptr", KP(local_analyze_reader_), KP(local_buffer_), K(ret));
  } else {
    if (PREPARE_EXECUTE_END != prepare_execute_state_) {
      int64_t written = 0;
      produce_reader = local_produce_reader_;
      if (OB_FAIL(local_buffer_->write(reader, reader->read_avail(), written))) {  // 并没有真正拷贝内存, 仅克隆 block
        PROXY_API_LOG(EDIAG, "fail to alloc reader of local_buffer_", K(ret));
      } else if (reader->read_avail() != written) {
        PROXY_API_LOG(EDIAG, "fail to write all data to local_buffer_", K(written), K(reader->read_avail()));
      } else {
        while (OB_SUCC(ret) && local_analyze_reader_->read_avail() > 0) {
          if (OB_FAIL(ObProxyParserUtils::analyze_one_packet(*local_analyze_reader_, result))) {
            PROXY_API_LOG(EDIAG, "fail to analyze one packet", K(local_analyze_reader_), K(ret));
          } else {
            if (ANALYZE_DONE == result.status_) {
              if (result.is_error_packet()) {
                prepare_execute_state_ = PREPARE_EXECUTE_END;
              }

              switch(prepare_execute_state_) {
              case PREPARE_EXECUTE_HEADER :
                if (MYSQL_OK_PACKET_TYPE == result.meta_.pkt_type_) {
                  ret = handle_prepare_execute_ok(local_analyze_reader_);
                } else {
                  ret = OB_ERR_UNEXPECTED;
                  PROXY_API_LOG(EDIAG, "the type of first packet is impossible", K(ret));
                }
                break;
              case PREPARE_EXECUTE_PARAM :
                ret = handle_prepare_param();
                break;
              case PREPARE_EXECUTE_COLUMN :
                ret = handle_prepare_column(local_analyze_reader_, result);
                break;
              case PREPARE_EXECUTE_ROW :
                if (result.is_eof_packet()) {
                  ret = handle_prepare_execute_eof(local_analyze_reader_);
                } else if (OB_FAIL(ObMysqlResponseCursorTransformPlugin::handle_resultset_row(local_analyze_reader_, sm_, field_types_,
                                                                                              have_cursor_, num_columns_))) {
                  PROXY_API_LOG(EDIAG, "fail to consume local analyze reader", K(result.meta_.pkt_len_), K(ret));
                }
                break;
              default :
                break;
              }

              if (OB_FAIL(ret)) {
              } else if (PREPARE_EXECUTE_END == prepare_execute_state_) {
                forward_len += local_analyze_reader_->read_avail();
                local_analyze_reader_->consume_all();
                break;
              } else if (OB_FAIL(local_analyze_reader_->consume(result.meta_.pkt_len_))) {
                PROXY_API_LOG(WDIAG, "fail to consume local analyze reader", K(result.meta_.pkt_len_), K(ret));
              } else {
                forward_len += result.meta_.pkt_len_;
              }
            } else {
              break;
            }
          }
        }
      }
    } else {
      // PREPARE_EXECUTE_END 状态, 说明已经不需要读 reader 里面的 packet 内容了
      // 直接将 reader 的数据全部写入下游
      forward_len = reader->read_avail();
      produce_reader = reader;
    }

    int64_t actual_size = 0;
    if (OB_SUCC(ret) && forward_len > 0) {
      if (forward_len != (actual_size = produce(produce_reader, forward_len))) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_API_LOG(EDIAG, "fail to produce", "expected size", forward_len, "actual size", actual_size, K(ret));
      // local_produce_reader_ 中已经被解析的数据写入下游后要及时 consume, 因为下次进入这个函数时会有新的数据写入到 local_buffer_
      // reader 中的数据写入下游后不用管,因为在外部会调用 reader->consume_all()
      } else if (produce_reader == local_produce_reader_) {
        if (OB_FAIL(local_produce_reader_->consume(forward_len))) {
          ret = OB_ERR_UNEXPECTED;
          PROXY_API_LOG(EDIAG, "fail to consume data from local_produce_reader", K(ret), K(forward_len));
        }
      }
      if (OB_NOT_NULL(sm_->protocol_diagnosis_)) {
        sm_->protocol_diagnosis_->resp_forward_data_flow_.plugin_prepare_execute_write_ += forward_len;
        PROTOCOL_FORWARD_LOG(TRACE, "plugin_prepare_execute write response",
          "plugin_prepare_execute_write", sm_->protocol_diagnosis_->resp_forward_data_flow_.plugin_prepare_execute_write_,
          "write_delta", forward_len);
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

int ObMysqlResponsePrepareExecuteTransformPlugin::handle_prepare_execute_eof(event::ObIOBufferReader *reader)
{
  int ret = OB_SUCCESS;

  obmysql::OMPKEOF eof_packet;
  pkt_reader_.reset();
  if (OB_FAIL(pkt_reader_.get_packet(*reader, eof_packet))) {
    PROXY_API_LOG(EDIAG, "fail to get preaprea ok packet from reader", K(ret));
  } else {
    if (eof_packet.get_server_status().status_flags_.OB_SERVER_MORE_RESULTS_EXISTS) {
      prepare_execute_state_ = PREPARE_EXECUTE_HEADER;
      num_columns_ = 0;
      num_params_ = 0;
      pkt_count_ = 0;
      have_cursor_ = false;
      field_types_.reset();
      pkt_reader_.reset();
    } else {
      prepare_execute_state_ = PREPARE_EXECUTE_END;
    }
  }

  return ret;
}

int ObMysqlResponsePrepareExecuteTransformPlugin::handle_prepare_column_eof()
{
  int ret = OB_SUCCESS;
  // col def packet 已经全部收到
  if (pkt_count_ == num_columns_) {
    if (have_cursor_) {
      prepare_execute_state_ = PREPARE_EXECUTE_ROW;
    } else {
      prepare_execute_state_ = PREPARE_EXECUTE_END;
    }
    pkt_count_ = 0;
  } else if (pkt_count_ < num_columns_) {
    ret = OB_UNKNOWN_PACKET;
    PROXY_API_LOG(EDIAG, "unexpected eof packet, need more colunm def packet",
                         K(ret),
                         "col_def_pkt_processed", pkt_count_,
                         "col_def_pkt_total", num_columns_);
  } else {
    ret = OB_ERR_UNEXPECTED;
    PROXY_API_LOG(EDIAG, "unexpected branch", K(ret), K(pkt_count_), K(num_columns_));
  }

  return ret;
}

int ObMysqlResponsePrepareExecuteTransformPlugin::handle_prepare_column_def(event::ObIOBufferReader *reader)
{
  int ret = OB_SUCCESS;
  if (pkt_count_ < num_columns_) {
    ObMySQLField field;
    OMPKField field_packet(field);

    pkt_reader_.reset();
    if (OB_FAIL(pkt_reader_.get_packet(*reader, field_packet))) {
      PROXY_API_LOG(EDIAG, "fail to get filed packet from reader", K(ret));
    } else {
      field_types_.push_back(field.type_);
      if (OB_MYSQL_TYPE_CURSOR == field.type_) {
        have_cursor_ = true;
      }
    }
    pkt_count_++;
  } else {
    ret = OB_ERR_UNEXPECTED;
    PROXY_API_LOG(EDIAG, "unexpected branch", K(ret), K(pkt_count_), K(num_columns_));
  }

  return ret;
}
int ObMysqlResponsePrepareExecuteTransformPlugin::handle_prepare_column(event::ObIOBufferReader *reader, ObMysqlAnalyzeResult& result)
{
  int ret = OB_SUCCESS;
  if (result.is_eof_packet()) {
    ret = handle_prepare_column_eof();
  } else {
    ret = handle_prepare_column_def(reader);
  }

  return ret;
}

int ObMysqlResponsePrepareExecuteTransformPlugin::handle_prepare_param()
{
  int ret = OB_SUCCESS;

  pkt_count_++;
  if (pkt_count_ > num_params_) {
    if (num_columns_ > 0) {
      prepare_execute_state_ = PREPARE_EXECUTE_COLUMN;
    } else {
      prepare_execute_state_ = PREPARE_EXECUTE_END;
    }

    pkt_count_ = 0;
  }

  return ret;
}

int ObMysqlResponsePrepareExecuteTransformPlugin::handle_prepare_execute_ok(event::ObIOBufferReader *reader)
{
  int ret = OB_SUCCESS;

  obmysql::OMPKPrepare prepare_packet;
  pkt_reader_.reset();
  if (OB_FAIL(pkt_reader_.get_packet(*reader, prepare_packet))) {
    PROXY_API_LOG(EDIAG, "fail to get preaprea ok packet from reader", K(ret));
  } else {
    num_columns_ = prepare_packet.get_column_num();
    num_params_ = prepare_packet.get_param_num();

    uint32_t client_ps_id = sm_->get_client_session()->get_session_info().get_client_ps_id();
    reader->replace(reinterpret_cast<const char*>(&client_ps_id), sizeof(client_ps_id), MYSQL_NET_META_LENGTH);

    /* 只有有 column 信息, 并且当前有结果集才需要分析 */
    if (num_columns_ > 0 && 1 == prepare_packet.has_result_set()) {
      if (num_params_ > 0) {
        prepare_execute_state_ = PREPARE_EXECUTE_PARAM;
      } else {
        prepare_execute_state_ = PREPARE_EXECUTE_COLUMN;
      }
    } else {
      prepare_execute_state_ = PREPARE_EXECUTE_END;
    }

    pkt_count_ = 0;
  }

  return ret;
}

void ObMysqlResponsePrepareExecuteTransformPlugin::handle_input_complete()
{
  PROXY_API_LOG(DEBUG, "ObMysqlResponsePrepareExecuteTransformPlugin::handle_input_complete happen");
  if (OB_NOT_NULL(sm_->protocol_diagnosis_)) {
    sm_->protocol_diagnosis_->record_resp_forward_ctrl_flow(ObRespForwardCtrlFlow::PLUGIN_PREPARE_EXECUTE_FINISH);
    PROTOCOL_FORWARD_LOG(TRACE, "plugin_prepare_execute process response finish");
  }
  free_local_buffer();
  set_output_complete();
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
