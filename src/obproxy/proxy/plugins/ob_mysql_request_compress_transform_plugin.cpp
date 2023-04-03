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

#include "ob_mysql_request_compress_transform_plugin.h"
#include "proxy/mysqllib/ob_mysql_analyzer_utils.h"
#include "proxy/mysqllib/ob_2_0_protocol_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

ObMysqlRequestCompressTransformPlugin *ObMysqlRequestCompressTransformPlugin::alloc(ObApiTransaction &transaction)
{
  return op_reclaim_alloc_args(ObMysqlRequestCompressTransformPlugin, transaction);
}

ObMysqlRequestCompressTransformPlugin::ObMysqlRequestCompressTransformPlugin(ObApiTransaction &transaction)
  : ObTransformationPlugin(transaction, ObTransformationPlugin::REQUEST_TRANSFORMATION),
    local_reader_(NULL), local_transfer_reader_(NULL), mio_buffer_(NULL), compressed_seq_(0),
    request_id_(UINT24_MAX + 1)
{
  PROXY_API_LOG(DEBUG, "ObMysqlRequestCompressTransformPlugin born", K(this));
}

void ObMysqlRequestCompressTransformPlugin::destroy()
{
  PROXY_API_LOG(DEBUG, "ObMysqlRequestCompressTransformPlugin destroy", K(this));
  ObTransformationPlugin::destroy();
  if (NULL != mio_buffer_) {
    free_miobuffer(mio_buffer_);
    mio_buffer_ = NULL;
  }
  op_reclaim_free(this);
}

int ObMysqlRequestCompressTransformPlugin::consume(event::ObIOBufferReader *reader)
{
  PROXY_API_LOG(DEBUG, "ObMysqlRequestCompressTransformPlugin::consume happen");
  int ret = OB_SUCCESS;
  if (OB_ISNULL(reader)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_API_LOG(WARN, "invalid argument", K(reader), K(ret));
  } else {
    int64_t consume_size = 0;
    int64_t produce_size = 0;
    int64_t local_read_avail = 0;
    const int64_t newest_read_avail = reader->read_avail();

    if (NULL == local_reader_) {
      local_reader_ = reader->clone();
      compressed_seq_ = 0;
    }

    if (NULL == local_transfer_reader_) {
      if (NULL != mio_buffer_) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_API_LOG(ERROR, "mio_buffer_ must be NULL here", K_(mio_buffer), K(ret));
      } else if (OB_ISNULL(mio_buffer_ = new_miobuffer(MYSQL_BUFFER_SIZE))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        PROXY_API_LOG(WARN, "fail to new miobuffer", K_(mio_buffer), K(ret));
      } else if (OB_ISNULL(local_transfer_reader_ = mio_buffer_->alloc_reader())) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_API_LOG(WARN, "fail to allocate iobuffer reader", K_(local_transfer_reader), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      local_read_avail = local_reader_->read_avail();
      bool is_last_data_segment = false;
      if (OB_FAIL(check_last_data_segment(*reader, is_last_data_segment))) {
        PROXY_API_LOG(WARN, "fail to check last data segment", K(ret));
      } else if ((local_read_avail >= MIN_COMPRESS_DATA_SIZE) || is_last_data_segment) {
        int64_t plugin_compress_request_begin = sm_->get_based_hrtime();
        if (OB_FAIL(build_compressed_packet(is_last_data_segment))) {
          PROXY_API_LOG(WARN, "fail to build compressed packet", K(ret));
        } else {
          int64_t plugin_compress_request_end = sm_->get_based_hrtime();
          sm_->cmd_time_stats_.plugin_compress_request_time_ +=
            milestone_diff(plugin_compress_request_begin, plugin_compress_request_end);

          // send the compressed packet in local_transfer_reader_
          consume_size = local_transfer_reader_->read_avail();
          if (consume_size > 0) {
            if (consume_size != (produce_size = produce(local_transfer_reader_, consume_size))) {
              ret = OB_ERR_UNEXPECTED;
              PROXY_API_LOG(WARN, "fail to produce", "expected size", consume_size,
                            "actual size", produce_size, K(ret));
            } else if (OB_FAIL(local_transfer_reader_->consume(consume_size))) {
              PROXY_API_LOG(WARN, "fail to consume local transfer reader", K(consume_size), K(ret));
            }
          }
        }
      } else {
        // contiune to receive
        PROXY_API_LOG(DEBUG, "not received enough data to compress", K(local_read_avail),
                      K(newest_read_avail), LITERAL_K(MIN_COMPRESS_DATA_SIZE));
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

int ObMysqlRequestCompressTransformPlugin::build_compressed_packet(bool is_last_segment)
{
  int ret = OB_SUCCESS;
  int64_t read_avail = local_reader_->read_avail() ;
  const bool use_fast_compress = true;
  const bool is_checksum_on = true;
  const bool is_need_reroute = false; //large request don't save, so can't reroute;
  // local_reader_ will consume in consume_and_compress_data(),
  //  compressed_seq_ will inc in consume_and_compress_data
  ObProxyProtocol ob_proxy_protocol = sm_->get_server_session_protocol();
  if (ObProxyProtocol::PROTOCOL_OB20 == ob_proxy_protocol) {
    if (request_id_ > UINT24_MAX) {
      request_id_ = sm_->get_server_session()->get_next_server_request_id();
    }

    ObSEArray<ObObJKV, 3> extra_info;
    ObSqlString sess_info_value;
    char client_ip_buf[MAX_IP_BUFFER_LEN] = "\0";
    char server_extra_info_buf[SERVER_EXTRA_INFO_BUF_MAX_LEN] = "\0";

    if (OB_FAIL(ObProxyTraceUtils::build_related_extra_info_all(extra_info, sm_,
                                                                client_ip_buf, MAX_IP_BUFFER_LEN,
                                                                server_extra_info_buf, SERVER_EXTRA_INFO_BUF_MAX_LEN,
                                                                sess_info_value, is_last_segment))) {
      PROXY_API_LOG(WARN, "fail to build related extra info all", K(ret));
    } else {
      ObMysqlServerSession *server_session = sm_->get_server_session();
      ObMysqlClientSession *client_session = sm_->get_client_session();
      const bool is_weak_read = (WEAK == sm_->trans_state_.get_trans_consistency_level(client_session->get_session_info()));
      Ob20ProtocolHeaderParam ob20_head_param(server_session->get_server_sessid(), request_id_, compressed_seq_,
                                              compressed_seq_, is_last_segment, is_weak_read, is_need_reroute,
                                              server_session->get_session_info().is_new_extra_info_supported(),
                                              client_session->is_trans_internal_routing());
      if (OB_FAIL(ObProto20Utils::consume_and_compress_data(local_reader_, mio_buffer_, local_reader_->read_avail(),
                                                            ob20_head_param, &extra_info))) {
        PROXY_API_LOG(WARN, "fail to consume and compress data with OB20", K(ret));
      }
    }
  } else {
    if (OB_FAIL(ObMysqlAnalyzerUtils::consume_and_compress_data(
                local_reader_, mio_buffer_, local_reader_->read_avail(),
                use_fast_compress, compressed_seq_, is_checksum_on))) {
      PROXY_API_LOG(WARN, "fail to consume and compress data", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    int64_t compressed_len = local_transfer_reader_->read_avail();
    sm_->get_server_session()->set_compressed_seq(compressed_seq_++);

    PROXY_API_LOG(DEBUG, "build compressed packet succ", "origin len", read_avail,
                  "compressed len(include header)", compressed_len, K(ob_proxy_protocol), K(is_checksum_on));
  }

  return ret;
}

void ObMysqlRequestCompressTransformPlugin::handle_input_complete()
{
  PROXY_API_LOG(DEBUG, "ObMysqlRequestCompressTransformPlugin::handle_input_complete happen");
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

int ObMysqlRequestCompressTransformPlugin::check_last_data_segment(
    event::ObIOBufferReader &reader,
    bool &is_last_segment)
{
  int ret = OB_SUCCESS;
  const int64_t read_avail = reader.read_avail();
  int64_t ntodo = -1;
  if (OB_FAIL(get_write_ntodo(ntodo))) {
    PROXY_API_LOG(ERROR, "fail to get write ntodo", K(ret));
  } else if (OB_UNLIKELY(ntodo <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_API_LOG(ERROR, "get_data_to_read must > 0", K(ntodo), K(ret));
  } else if (OB_UNLIKELY(read_avail > ntodo)) { // just defense
    if (read_avail >= (ntodo + MYSQL_NET_META_LENGTH)) {
      char tmp_buff[MYSQL_NET_META_LENGTH]; // print the next packet's meta
      reader.copy(tmp_buff, MYSQL_NET_META_LENGTH, ntodo);
      int64_t payload_len = uint3korr(tmp_buff);
      int64_t seq = uint1korr(tmp_buff + 3);
      int64_t cmd = static_cast<uint8_t>(tmp_buff[4]);
      PROXY_API_LOG(ERROR, "next packet meta is", K(payload_len), K(seq), K(cmd));
    }
    ret = OB_ERR_UNEXPECTED;
    PROXY_API_LOG(ERROR, "invalid data, maybe client received more than one mysql packet,"
                  " will disconnect", K(read_avail), K(ntodo), K(ret));
  } else {
    is_last_segment = (read_avail == ntodo);
  }

  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
