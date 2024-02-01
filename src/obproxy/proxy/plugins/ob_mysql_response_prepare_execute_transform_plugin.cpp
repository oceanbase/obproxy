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
    local_reader_(NULL), local_analyze_reader_(NULL), pkt_reader_(), prepare_execute_state_(PREPARE_EXECUTE_HEADER),
    num_columns_(0), num_params_(0), pkt_count_(0), hava_cursor_(false), field_types_()
{
  PROXY_API_LOG(DEBUG, "ObMysqlResponsePrepareExecuteTransformPlugin born", K(this));
}

void ObMysqlResponsePrepareExecuteTransformPlugin::destroy()
{
  PROXY_API_LOG(DEBUG, "ObMysqlResponsePrepareExecuteTransformPlugin destroy", K(this));
  ObTransformationPlugin::destroy();
  pkt_reader_.reset();
  op_reclaim_free(this);
}

int ObMysqlResponsePrepareExecuteTransformPlugin::consume(event::ObIOBufferReader *reader)
{
  PROXY_API_LOG(DEBUG, "ObMysqlResponsePrepareExecuteTransformPlugin::consume happen");
  int ret = OB_SUCCESS;

  int64_t write_size = 0;
  ObMysqlAnalyzeResult result;

  // 这里为什么要 clone 两个 reader，是因为:
  // local_analyze_reader 用于分析, 当分析完一个 mysql 包，就要往前移动到下一个 mysql 包;
  // local_reader 用于把数据输出给tunnel，这里需要从开始的位置输出;
  // 这里也可以clone一个reader，使用start_pos_ 来移动
  if (NULL == local_reader_) {
    local_reader_ = reader->clone();
    local_analyze_reader_ = local_reader_->clone();
  } else {
    local_reader_->reserved_size_ = reader->reserved_size_;
    local_analyze_reader_->reserved_size_ = reader->reserved_size_;
  }

  if (PREPARE_EXECUTE_END != prepare_execute_state_) {
    while (OB_SUCC(ret) && local_analyze_reader_->read_avail()) {
      if (OB_FAIL(ObProxyParserUtils::analyze_one_packet(*local_analyze_reader_, result))) {
        PROXY_API_LOG(EDIAG, "fail to analyze one packet", K(local_analyze_reader_), K(ret));
      } else {
        if (ANALYZE_DONE == result.status_) {
          if (MYSQL_ERR_PACKET_TYPE == result.meta_.pkt_type_) {
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
            ret = handle_prepare_column(local_analyze_reader_);
            break;
          case PREPARE_EXECUTE_ROW :
            if (MYSQL_EOF_PACKET_TYPE == result.meta_.pkt_type_) {
              ret = handle_prepare_execute_eof(local_analyze_reader_);
            } else if (MYSQL_ERR_PACKET_TYPE == result.meta_.pkt_type_) {
              prepare_execute_state_ = PREPARE_EXECUTE_END;
            } else if (OB_FAIL(ObMysqlResponseCursorTransformPlugin::handle_resultset_row(local_analyze_reader_, sm_, field_types_,
                                                                                          hava_cursor_, num_columns_))) {
              PROXY_API_LOG(EDIAG, "fail to consume local analyze reader", K(result.meta_.pkt_len_), K(ret));
            }
            break;
          default :
            break;
          }

          if (PREPARE_EXECUTE_END == prepare_execute_state_) {
            write_size += local_analyze_reader_->read_avail();
            break;
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(local_analyze_reader_->consume(result.meta_.pkt_len_))) {
              PROXY_API_LOG(WDIAG, "fail to consume local analyze reader", K(result.meta_.pkt_len_), K(ret));
            } else {
              write_size += result.meta_.pkt_len_;
            }
          }
        } else {
          break;
        }
      }
    }
  } else {
    write_size = local_analyze_reader_->read_avail();
  }

  if (OB_SUCC(ret) && write_size > 0) {
    int64_t actual_size = 0;
    if (write_size != (actual_size = produce(local_reader_, write_size))) {
      ret = OB_ERR_UNEXPECTED;
      PROXY_API_LOG(WDIAG, "fail to produce", "expected size", write_size,
                    "actual size", actual_size, K(ret));
    } else if (write_size == local_reader_->read_avail() && OB_FAIL(local_analyze_reader_->consume_all())) {
      PROXY_API_LOG(WDIAG, "fail to consume all local analyze reader", K(ret));
    } else if (OB_FAIL(local_reader_->consume(write_size))) {
      PROXY_API_LOG(WDIAG, "fail to consume local reader", K(write_size), K(ret));
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
      hava_cursor_ = false;
      field_types_.reset();
      pkt_reader_.reset();
    } else {
      prepare_execute_state_ = PREPARE_EXECUTE_END;
    }
  }

  return ret;
}

int ObMysqlResponsePrepareExecuteTransformPlugin::handle_prepare_column(event::ObIOBufferReader *reader)
{
  int ret = OB_SUCCESS;

  pkt_count_++;
  if (pkt_count_ <= num_columns_) {
    ObMySQLField field;
    OMPKField field_packet(field);

    pkt_reader_.reset();
    if (OB_FAIL(pkt_reader_.get_packet(*reader, field_packet))) {
      PROXY_API_LOG(EDIAG, "fail to get filed packet from reader", K(ret));
    } else {
      field_types_.push_back(field.type_);
      if (OB_MYSQL_TYPE_CURSOR == field.type_) {
        hava_cursor_ = true;
      }
    }
  } else {
    if (hava_cursor_) {
      prepare_execute_state_ = PREPARE_EXECUTE_ROW;
    } else {
      prepare_execute_state_ = PREPARE_EXECUTE_END;
    }
    pkt_count_ = 0;
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
  if (NULL != local_reader_) {
    local_reader_->dealloc();
    local_reader_ = NULL;
  }

  if (NULL != local_analyze_reader_) {
    local_analyze_reader_->dealloc();
    local_analyze_reader_ = NULL;
  }

  set_output_complete();
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
