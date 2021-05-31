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

#include "ob_mysql_response_prepare_transform_plugin.h"
#include "rpc/obmysql/packet/ompk_prepare.h"
#include "rpc/obmysql/packet/ompk_field.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

ObMysqlResponsePrepareTransformPlugin *ObMysqlResponsePrepareTransformPlugin::alloc(ObApiTransaction &transaction)
{
  return op_reclaim_alloc_args(ObMysqlResponsePrepareTransformPlugin, transaction);
}

ObMysqlResponsePrepareTransformPlugin::ObMysqlResponsePrepareTransformPlugin(ObApiTransaction &transaction)
  : ObTransformationPlugin(transaction, ObTransformationPlugin::RESPONSE_TRANSFORMATION),
    local_reader_(NULL), local_analyze_reader_(NULL), pkt_reader_(), prepare_state_(PREPARE_OK), num_columns_(0), num_params_(0), pkt_count_(0)
{
  PROXY_API_LOG(DEBUG, "ObMysqlResponsePrepareTransformPlugin born", K(this));
}

void ObMysqlResponsePrepareTransformPlugin::destroy()
{
  PROXY_API_LOG(DEBUG, "ObMysqlResponsePrepareTransformPlugin destroy", K(this));
  ObTransformationPlugin::destroy();
  pkt_reader_.reset();
  op_reclaim_free(this);
}

int ObMysqlResponsePrepareTransformPlugin::consume(event::ObIOBufferReader *reader)
{
  PROXY_API_LOG(DEBUG, "ObMysqlResponsePrepareTransformPlugin::consume happen");
  int ret = OB_SUCCESS;

  int64_t write_size = 0;
  ObMysqlAnalyzeResult result;

  // why use two reader?
  // local_analyze_reader for analyze. after analyze one mysql packet, will move to next mysql packet
  // local_reader for output data to tunnel, need from start pos
  if (NULL == local_reader_) {
    local_reader_ = reader->clone();
    local_analyze_reader_ = local_reader_->clone();
  } else {
    local_reader_->reserved_size_ = reader->reserved_size_;
    local_analyze_reader_->reserved_size_ = reader->reserved_size_;
  }

  if (PREPARE_END != prepare_state_) {
    while (OB_SUCC(ret) && local_analyze_reader_->read_avail()) {
      if (OB_FAIL(ObProxyParserUtils::analyze_one_packet(*local_analyze_reader_, result))) {
        PROXY_API_LOG(ERROR, "fail to analyze one packet", K(local_analyze_reader_), K(ret));
      } else {
        if (ANALYZE_DONE == result.status_) {
          switch(prepare_state_) {
          case PREPARE_OK :
            if (MYSQL_ERR_PACKET_TYPE == result.meta_.pkt_type_) {
              prepare_state_ = PREPARE_END;
            } else if (MYSQL_OK_PACKET_TYPE == result.meta_.pkt_type_) {
              ret = handle_prepare_ok(local_analyze_reader_);
            } else {
              ret = OB_ERR_UNEXPECTED;
              PROXY_API_LOG(ERROR, "the type of first packet is impossible", K(ret));
            }
            break;
          case PREPARE_PARAM :
            ret = handle_prepare_param(local_analyze_reader_);
            break;
          case PREPARE_COLUMN :
            ret = handle_prepare_column();
            break;
          default :
            break;
          }

          if (PREPARE_END == prepare_state_) {
            write_size += local_analyze_reader_->read_avail();
            break;
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(local_analyze_reader_->consume(result.meta_.pkt_len_))) {
              PROXY_API_LOG(WARN, "fail to consume local analyze reader", K(result.meta_.pkt_len_), K(ret));
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
      PROXY_API_LOG(WARN, "fail to produce", "expected size", write_size,
                    "actual size", actual_size, K(ret));
    } else if (write_size == local_reader_->read_avail() && OB_FAIL(local_analyze_reader_->consume_all())) {
      PROXY_API_LOG(WARN, "fail to consume all local analyze reader", K(ret));
    } else if (OB_FAIL(local_reader_->consume(write_size))) {
      PROXY_API_LOG(WARN, "fail to consume local reader", K(write_size), K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    sm_->trans_state_.inner_errcode_ = ret;
    // if failed, set state to INTERNAL_ERROR
    sm_->trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
  }

  return ret;
}

int ObMysqlResponsePrepareTransformPlugin::handle_prepare_column()
{
  int ret = OB_SUCCESS;

  pkt_count_++;
  if (pkt_count_ <= num_columns_) {
    // no need to parse columns, do nothing
  } else {
    prepare_state_ = PREPARE_END;
    pkt_count_ = 0;
  }

  return ret;
}

int ObMysqlResponsePrepareTransformPlugin::handle_prepare_param(event::ObIOBufferReader *reader)
{
  int ret = OB_SUCCESS;
  obmysql::ObMySQLField field;
  obmysql::OMPKField field_packet(field);

  pkt_count_++;
  if (pkt_count_ > num_params_) {
    if (num_columns_ > 0) {
      prepare_state_ = PREPARE_COLUMN;
    } else {
      prepare_state_ = PREPARE_END;
    }

    pkt_count_ = 0;
  } else {
    pkt_reader_.reset();
    if (OB_FAIL(pkt_reader_.get_packet(*reader, field_packet))) {
      PROXY_API_LOG(ERROR, "fail to get filed packet from reader", K(ret));
    } else {
      ObClientSessionInfo &cs_info = sm_->get_client_session()->get_session_info();
      ObIArray<obmysql::EMySQLFieldType> &param_types = cs_info.get_ps_entry()->get_ps_sql_meta().get_param_types();
      param_types.push_back(field.type_);
    }
  }

  return ret;
}

int ObMysqlResponsePrepareTransformPlugin::handle_prepare_ok(event::ObIOBufferReader *reader)
{
  int ret = OB_SUCCESS;

  obmysql::OMPKPrepare prepare_packet;
  pkt_reader_.reset();
  if (OB_FAIL(pkt_reader_.get_packet(*reader, prepare_packet))) {
    PROXY_API_LOG(ERROR, "fail to get preaprea ok packet from reader", K(ret));
  } else {
    num_columns_ = prepare_packet.get_column_num();
    num_params_ = prepare_packet.get_param_num();
    uint32_t client_ps_id = sm_->get_client_session()->get_session_info().get_client_ps_id();

    reader->replace(reinterpret_cast<const char*>(&client_ps_id), sizeof(client_ps_id), MYSQL_NET_META_LENGTH);
    ObClientSessionInfo &cs_info = sm_->get_client_session()->get_session_info();
    int64_t origin_num_params = cs_info.get_ps_entry()->get_ps_sql_meta().get_param_count();
    int64_t origin_num_columns = cs_info.get_ps_entry()->get_ps_sql_meta().get_column_count();

    if ((origin_num_params > 0 && OB_UNLIKELY(origin_num_params != num_params_))
        || (origin_num_columns > 0 && OB_UNLIKELY(origin_num_columns != num_columns_))) {
      ret = OB_ERR_UNEXPECTED;
      PROXY_API_LOG(ERROR, "prepare response ok returns different param count or param count",
                    K_(num_columns), K(origin_num_columns),
                    K_(num_params), K(origin_num_params), KPC(cs_info.get_ps_entry()), K(ret));
    } else if (num_params_ > 0) {
      cs_info.get_ps_entry()->get_ps_sql_meta().set_param_count(num_params_);
      prepare_state_ = PREPARE_PARAM;
      ObClientSessionInfo &cs_info = sm_->get_client_session()->get_session_info();
      ObIArray<obmysql::EMySQLFieldType> &param_types = cs_info.get_ps_entry()->get_ps_sql_meta().get_param_types();
      param_types.reset();
    } else if (num_columns_ > 0) {
      cs_info.get_ps_entry()->get_ps_sql_meta().set_column_count(num_columns_);
      prepare_state_ = PREPARE_COLUMN;
    } else {
      prepare_state_ = PREPARE_END;
    }

    pkt_count_ = 0;
  }

  return ret;
}

void ObMysqlResponsePrepareTransformPlugin::handle_input_complete()
{
  PROXY_API_LOG(DEBUG, "ObMysqlResponsePrepareTransformPlugin::handle_input_complete happen");
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
