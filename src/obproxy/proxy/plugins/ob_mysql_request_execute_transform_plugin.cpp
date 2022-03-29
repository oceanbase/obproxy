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
#include "ob_mysql_request_execute_transform_plugin.h"
#include "proxy/mysqllib/ob_mysql_analyzer_utils.h"
#include "rpc/obmysql/ob_mysql_util.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

ObMysqlRequestExecuteTransformPlugin *ObMysqlRequestExecuteTransformPlugin::alloc(ObApiTransaction &transaction, int64_t req_pkt_len)
{
  return op_reclaim_alloc_args(ObMysqlRequestExecuteTransformPlugin, transaction, req_pkt_len);
}

ObMysqlRequestExecuteTransformPlugin::ObMysqlRequestExecuteTransformPlugin(ObApiTransaction &transaction, int64_t req_pkt_len)
  : ObTransformationPlugin(transaction, ObTransformationPlugin::REQUEST_TRANSFORMATION),
    ps_pkt_len_(req_pkt_len), is_handle_finished_(false), param_type_pos_(-1), param_num_(-1), param_offset_(0),
    new_param_bound_flag_(-1), param_type_buf_(), ps_id_entry_(NULL), local_reader_(NULL), is_first_analyze_with_flag_(true)
{
  PROXY_API_LOG(DEBUG, "ObMysqlRequestExecuteTransformPlugin born", K(this));
}

void ObMysqlRequestExecuteTransformPlugin::destroy()
{
  PROXY_API_LOG(DEBUG, "ObMysqlRequestExecuteTransformPlugin destroy", K(this));
  ObTransformationPlugin::destroy();
  op_reclaim_free(this);
}

int ObMysqlRequestExecuteTransformPlugin::consume(event::ObIOBufferReader *reader)
{
  PROXY_API_LOG(DEBUG, "ObMysqlRequestExecuteTransformPlugin::consume happen");
  int ret = OB_SUCCESS;
  if (OB_ISNULL(reader)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_API_LOG(WARN, "invalid argument", K(reader), K(ret));
  } else {
    int64_t read_avail = 0;

    if (NULL == local_reader_) {
      local_reader_ = reader->clone();
      ObClientSessionInfo &session_info = sm_->get_client_session()->get_session_info();
      if (OB_UNLIKELY(ps_pkt_len_ >= MYSQL_PACKET_MAX_LENGTH)) {
        ret = OB_NOT_SUPPORTED;
        PROXY_API_LOG(WARN, "we cannot support packet which is larger than 16MB", K_(ps_pkt_len),
                      K(MYSQL_PACKET_MAX_LENGTH), K(ret));
      } else if (OB_ISNULL(ps_id_entry_ = session_info.get_ps_id_entry()) || !ps_id_entry_->is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_API_LOG(WARN, "ps id entry does not exist", KPC(ps_id_entry_), K(ret));
      } else {
        if (OB_LIKELY((param_num_ = ps_id_entry_->get_param_count()) > 0)) {
          param_type_pos_ = MYSQL_NET_META_LENGTH + MYSQL_PS_EXECUTE_HEADER_LENGTH + ((param_num_ + 7) /8) + 1;
        }
      }
    }

    if (OB_SUCC(ret)) {
      read_avail = local_reader_->read_avail();
      if (read_avail > 0) {
        PROXY_API_LOG(DEBUG, "ObMysqlRequestExecuteTransformPlugin::save_rewrite_ps_execute::consume", K(read_avail));
        // If there are no parameters, you can directly transfer
        if (param_num_ == 0 || is_handle_finished_) {
          if (OB_FAIL(produce_data(local_reader_, read_avail))) {
            PROXY_API_LOG(WARN, "fail to consume local transfer reader", K(param_num_), K(ret));
          }
        } else if (OB_FAIL(analyze_ps_execute_request(read_avail))) {
          PROXY_API_LOG(WARN, "fail to analyze ps execute request", K(read_avail),  K(ret));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    sm_->trans_state_.inner_errcode_ = ret;
    // if failed set to INTERNAL_ERROR
    sm_->trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
  }

  return ret;
}

int ObMysqlRequestExecuteTransformPlugin::rewrite_pkt_length_and_flag()
{
  int ret = OB_SUCCESS;

  int32_t pkt_length = static_cast<int32_t>(ps_pkt_len_ - 4); // packet length without header
  const ObString& param_type = ps_id_entry_->get_ps_sql_meta().get_param_type();
  if (OB_UNLIKELY(pkt_length <= 0) || OB_UNLIKELY(param_type.empty())) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_API_LOG(WARN, "invalid pkt_length or param_type is empty", K(pkt_length), K(param_type), K(ret));
  } else {
    pkt_length += param_type.length();

    char header[MYSQL_PAYLOAD_LENGTH_LENGTH];
    int64_t pos = 0;
    if (OB_FAIL(obmysql::ObMySQLUtil::store_int3(header, MYSQL_PAYLOAD_LENGTH_LENGTH, pkt_length, pos))) {
      PROXY_API_LOG(WARN, "fail to store pkg meta header", K(ret));
    } else {
      int64_t new_param_bound_flag_pos = param_type_pos_ - 1;
      int8_t new_param_bound_flag = 1;
      local_reader_->replace(header, MYSQL_PAYLOAD_LENGTH_LENGTH, 0);
      local_reader_->replace(reinterpret_cast<char*>(&new_param_bound_flag), 1, new_param_bound_flag_pos);

      sm_->trans_state_.trans_info_.client_request_.get_packet_meta().pkt_len_ = static_cast<uint32_t>(ps_pkt_len_ + (param_type.length()));
      sm_->trans_state_.trans_info_.request_content_length_ = ps_pkt_len_ + (param_type.length());
    }
  }

  return ret;
}

int ObMysqlRequestExecuteTransformPlugin::produce_param_type()
{
  int ret = OB_SUCCESS;

  const ObString& param_type = ps_id_entry_->get_ps_sql_meta().get_param_type();
  if (OB_UNLIKELY(param_type.empty())) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_API_LOG(WARN, "invalid param_type", K(param_type), K(ret));
  } else {
    ObMIOBuffer* mio_new = NULL;
    int64_t written_len = 0;

    event::ObIOBufferReader* reader_tmp = NULL;

    if (OB_ISNULL(mio_new = new_miobuffer(param_type.length()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      PROXY_API_LOG(WARN, "fail to alloc miobuffer for reader_buffer_tmp", K(ret));
    } else if (OB_ISNULL(reader_tmp = mio_new->alloc_reader())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      PROXY_API_LOG(WARN, "fail to alloc reader for reader_tmp", K(ret));
    } else if (OB_FAIL(mio_new->write(param_type.ptr(), param_type.length(), written_len))) {
      PROXY_API_LOG(WARN, "fail to write to reader_tmp", K(ret));
    } else if (OB_UNLIKELY(written_len != (param_type.length()))) {
      ret = OB_ERR_UNEXPECTED;
      PROXY_API_LOG(WARN, "fail to write to reader_tmp", K(ret));
    } else if (OB_FAIL(produce_data(reader_tmp, written_len))) {
      PROXY_API_LOG(WARN, "fail to produce param type", K(ret));
    }

    if (NULL != mio_new) {
      free_miobuffer(mio_new);
    }
  }

  return ret;
}

int ObMysqlRequestExecuteTransformPlugin::analyze_without_flag(const int64_t local_read_avail)
{
  int ret = OB_SUCCESS;
  // At this point, the packet length and new_param_bound_flag_ fields are already there,
  // you can directly rewrite them, and append the type content
  if (OB_FAIL(rewrite_pkt_length_and_flag())) {
    PROXY_API_LOG(WARN, "fail to rewrite header length", K(ret));
    // Send out the previous content first
  } else if (OB_FAIL(produce_data(local_reader_, param_type_pos_))) {
    PROXY_API_LOG(WARN, "fail to produce execute", K(ret));
    // Output type content
  } else if (OB_FAIL(produce_param_type())) {
    PROXY_API_LOG(WARN, "fail to consume reader_tmp transfer reader", K(ret));
    // Output the remaining content of this time
  } else if (local_read_avail > param_type_pos_ && OB_FAIL(produce_data(local_reader_, local_read_avail - param_type_pos_))) {
    PROXY_API_LOG(WARN, "fail to consume rewrite transfer reader", K(ret));
  } else {
    is_handle_finished_ = true;
  }

  return ret;
}

int ObMysqlRequestExecuteTransformPlugin::analyze_with_flag(const int64_t local_read_avail)
{
  int ret = OB_SUCCESS;
  int64_t consume_len = local_read_avail;
  ObIArray<obmysql::EMySQLFieldType> &param_types = ps_id_entry_->get_ps_sql_meta().get_param_types();

  // At this point, the packet length and new_param_bound_flag_ fields are already there,
  // and the previous content can be consumed directly
  if (is_first_analyze_with_flag_) {
    if (OB_FAIL(produce_data(local_reader_, param_type_pos_))) {
      PROXY_API_LOG(WARN, "fail to produce local reader", K_(param_type_pos), K(ret));
    } else if (OB_FAIL(param_type_buf_.reserve(2 * param_num_))) {
      PROXY_API_LOG(WARN, "fail to reserve param type buf", "param_type_len", 2 * param_num_, K(ret));
    } else {
      is_first_analyze_with_flag_ = false;
      consume_len -= param_type_pos_;
      param_types.reset();
    }
  }

  if (OB_SUCC(ret)) {
    int64_t analyzed_len = 0;
    int64_t param_type_len = 0;
    if (OB_FAIL(ObMysqlRequestAnalyzer::parse_param_type_from_reader(param_offset_, param_num_,
                                                                     param_types,
                                                                     local_reader_,
                                                                     analyzed_len, is_handle_finished_))) {
      if (OB_LIKELY(ret == OB_SIZE_OVERFLOW)) {
        ret = OB_SUCCESS;
      } else {
        PROXY_API_LOG(WARN, "fail to parse param type from reader", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      param_type_len = param_type_buf_.length() + analyzed_len;
      if (OB_UNLIKELY(param_type_len > param_type_buf_.capacity())) {
        if (OB_FAIL(param_type_buf_.reserve(param_type_len))) {
          PROXY_API_LOG(WARN, "fail to reserve param type buf", K(param_type_len), K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      local_reader_->copy(param_type_buf_.ptr() + param_type_buf_.length(), analyzed_len, 0);
      if (OB_FAIL(param_type_buf_.set_length(param_type_len))) {
        PROXY_API_LOG(WARN, "fail to set param type len", K(param_type_len), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (is_handle_finished_) {
        if (OB_FAIL(ps_id_entry_->get_ps_sql_meta().set_param_type(param_type_buf_.ptr(), param_type_len))) {
          PROXY_API_LOG(WARN, "fail to set param type", K(param_type_len), K(ret));
        }
      } else {
        consume_len = analyzed_len;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(produce_data(local_reader_, consume_len))) {
      PROXY_API_LOG(WARN, "fail to produce local reader", K(local_read_avail), K(consume_len), K(ret));
    }
  }

  return ret;
}

int ObMysqlRequestExecuteTransformPlugin::analyze_ps_execute_request(const int64_t local_read_avail)
{
  int ret = OB_SUCCESS;
  // You must wait until new_param_bound_flag_ to send data,
  // otherwise it is not clear whether to rewrite the header length field
  if (-1 == new_param_bound_flag_) {
    int64_t new_param_bound_flag_pos = param_type_pos_ - 1;
    // Because the data has not been consumed, local_read_avail here is the data from the very beginning
    if (local_read_avail > new_param_bound_flag_pos) {
      local_reader_->copy(reinterpret_cast<char *>(&new_param_bound_flag_), 1, new_param_bound_flag_pos);
      if (1 != new_param_bound_flag_ && 0 != new_param_bound_flag_) {
        ret = OB_DATA_OUT_OF_RANGE;
        PROXY_API_LOG(WARN, "fail to get new_param_bound_flag", K_(new_param_bound_flag), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (1 == new_param_bound_flag_) {
      if (OB_FAIL(analyze_with_flag(local_read_avail))) {
        PROXY_API_LOG(WARN, "fail to analyze with flag", K(ret));
      }
    } else if (0 == new_param_bound_flag_) {
      if (OB_FAIL(analyze_without_flag(local_read_avail))) {
        PROXY_API_LOG(WARN, "fail to analyze without flag", K(ret));
      }
    }
  }

  return ret;
}

int ObMysqlRequestExecuteTransformPlugin::produce_data(event::ObIOBufferReader *reader,
                                                       const int64_t produce_size)
{
  int ret = OB_SUCCESS;

  PROXY_API_LOG(DEBUG, "ObMysqlRequestExecuteTransformPlugin::produce_data", K(produce_size));

  int64_t produce_length = 0;
  if (OB_ISNULL(reader)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_API_LOG(WARN, "invalid argument", K(reader), K(ret));
  } else if (OB_UNLIKELY(produce_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_API_LOG(WARN, "invalid argument", K(produce_size), K(ret));
  } else if (OB_UNLIKELY((produce_size > reader->read_avail()))){
    ret = OB_ERR_UNEXPECTED;
    PROXY_API_LOG(WARN, "produce_size is larger than read_avail", K(produce_size), K(reader->read_avail()), K(ret));
  } else if (OB_UNLIKELY(produce_size != (produce_length = produce(reader, produce_size)))) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_API_LOG(WARN, "fail to produce", "expected size", produce_size,
                  "actual size", produce_length, K(ret));
  } else if (OB_FAIL(reader->consume(produce_size))) {
    PROXY_API_LOG(WARN, "fail to consume local transfer reader", K(produce_size), K(ret));
  }

  return ret;
}

void ObMysqlRequestExecuteTransformPlugin::handle_input_complete()
{
  PROXY_API_LOG(DEBUG, "ObMysqlRequestExecuteTransformPlugin::handle_input_complete happen");
  if (NULL != local_reader_) {
    local_reader_->dealloc();
    local_reader_ = NULL;
  }

  set_output_complete();
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
