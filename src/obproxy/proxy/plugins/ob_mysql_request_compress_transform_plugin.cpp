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
#include "lib/utility/ob_2_0_sess_veri.h"
#include "proxy/mysqllib/ob_protocol_diagnosis.h"


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
  MEMSET(&content_of_file_, 0, sizeof(content_of_file_));
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
    PROXY_API_LOG(WDIAG, "invalid argument", K(reader), K(ret));
  } else {
    if (NULL == local_reader_) {
      local_reader_ = reader->clone();
      compressed_seq_ = 0;
    }

    if (NULL == local_transfer_reader_) {
      if (NULL != mio_buffer_) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_API_LOG(EDIAG, "mio_buffer_ must be NULL here", K_(mio_buffer), K(ret));
      } else if (OB_ISNULL(mio_buffer_ = new_miobuffer(MYSQL_BUFFER_SIZE))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        PROXY_API_LOG(WDIAG, "fail to new miobuffer", K_(mio_buffer), K(ret));
      } else if (OB_ISNULL(local_transfer_reader_ = mio_buffer_->alloc_reader())) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_API_LOG(WDIAG, "fail to allocate iobuffer reader", K_(local_transfer_reader), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(obmysql::OB_MYSQL_COM_LOAD_DATA_TRANSFER_CONTENT == sm_->trans_state_.trans_info_.sql_cmd_)) {
        if (OB_FAIL(consume_content_of_file_compress_packet())) {
          PROXY_API_LOG(WDIAG, "fail to consume_content_of_file_compress_packet", K(ret));
        }
      } else {
        if (OB_FAIL(consume_normal_compress_packet(*reader))) {
          PROXY_API_LOG(WDIAG, "fail to consume_normal_compress_packet", K(ret));
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

int ObMysqlRequestCompressTransformPlugin::consume_normal_compress_packet(event::ObIOBufferReader &reader)
{
  int ret = OB_SUCCESS;
  const int64_t newest_read_avail = reader.read_avail();
  int64_t local_read_avail = (local_reader_ == NULL) ? 0 : local_reader_->read_avail();
  bool is_last_segment = false;
  if (OB_FAIL(check_last_data_segment(reader, is_last_segment))) {
    PROXY_API_LOG(WDIAG, "fail to check last data segment", K(ret));
  } else if ((local_read_avail >= MIN_COMPRESS_DATA_SIZE) || is_last_segment) {
    int64_t plugin_compress_request_begin = sm_->get_based_hrtime();
    if (OB_FAIL(build_compressed_packet(is_last_segment, local_read_avail))) {
      PROXY_API_LOG(WDIAG, "fail to build compressed packet", K(ret));
    } else {
      int64_t plugin_compress_request_end = sm_->get_based_hrtime();
      sm_->cmd_time_stats_.plugin_compress_request_time_ +=
        milestone_diff(plugin_compress_request_begin, plugin_compress_request_end);
      int64_t consume_size = local_transfer_reader_->read_avail();
      int64_t produce_size = 0;
      // send the compressed packet in local_transfer_reader_
      if (consume_size > 0) {
        if (consume_size != (produce_size = produce(local_transfer_reader_, consume_size))) {
          ret = OB_ERR_UNEXPECTED;
          PROXY_API_LOG(WDIAG, "fail to produce", "expected size", consume_size,
                        "actual size", produce_size, K(ret));
        } else if (OB_FAIL(local_transfer_reader_->consume(consume_size))) {
          PROXY_API_LOG(WDIAG, "fail to consume local transfer reader", K(consume_size), K(ret));
        }
      }
    }
  } else {
    // contiune to receive
    PROXY_API_LOG(DEBUG, "not received enough data to compress", K(local_read_avail),
                  K(newest_read_avail), LITERAL_K(MIN_COMPRESS_DATA_SIZE));
  }
  return ret;
}

int ObMysqlRequestCompressTransformPlugin::consume_content_of_file_compress_packet()
{
  int ret = OB_SUCCESS;
  int64_t local_read_avail = local_reader_->read_avail();
  ObIOBufferBlock *block = NULL;
  int64_t offset = 0;
  // get avail data and size
  char *block_buf = NULL;
  int64_t block_length = 0;
  if (NULL != local_reader_->block_) {
    local_reader_->skip_empty_blocks();
    block = local_reader_->block_;
    offset = local_reader_->start_offset_;
    if (OB_NOT_NULL(block)) {
      block_buf = block->start() + offset;
      block_length = block->read_avail() - offset;
    }

    if (block_length < 0) {
      ret = OB_ERR_UNEXPECTED;
      PROXY_API_LOG(WDIAG, "the first block block_length in reader is less than 0",
                    K(offset), K(block->read_avail()), K(ret));
    }
  }

  // before start to analyze the reader we need to clear the last header length buf and offset
  // because if we just read part of the header in last calling of this func we would't consume
  // the data of part of the header from reader but we consume them util read completed packet header
  MEMSET(content_of_file_.header_length_buffer_, 0, MYSQL_NET_HEADER_LENGTH);
  content_of_file_.header_content_offset_ = 0;

  int64_t to_consume_length = 0;
  while (OB_SUCC(ret) && 0 < block_length && OB_NOT_NULL(block) && OB_NOT_NULL(block_buf)) {
    // 1. if current block buffer started with an uncompressed packet
    if (content_of_file_.last_packet_remain_ == 0) {
      // consider the packet header across tow block
      int64_t header_need_length = MYSQL_NET_HEADER_LENGTH - content_of_file_.header_content_offset_;
      // without complete mysql packet header we wouldn't consume the reader
      if (block_length < header_need_length) {
        for (int i = 0; i < block_length; ++i, ++content_of_file_.header_content_offset_) {
          content_of_file_.header_length_buffer_[content_of_file_.header_content_offset_] = *(block_buf + i);
        }
        to_consume_length = 0;
      } else {
        for (int i = 0; i < header_need_length; ++i, ++content_of_file_.header_content_offset_) {
          content_of_file_.header_length_buffer_[content_of_file_.header_content_offset_] = *(block_buf + i);
        }
        // we have read the completed packet header
        // then we can do compress
        int64_t payload_length = uint3korr(content_of_file_.header_length_buffer_);
        int64_t seq = uint1korr(content_of_file_.header_length_buffer_ + MYSQL_PAYLOAD_LENGTH_LENGTH);
        MEMSET(content_of_file_.header_length_buffer_, 0, MYSQL_NET_HEADER_LENGTH);
        content_of_file_.header_content_offset_ = 0;
        content_of_file_.is_last_packet_ = (payload_length == 0);
        int64_t complete_packet_length = payload_length + MYSQL_NET_HEADER_LENGTH;
        int64_t consume_length = complete_packet_length;
        // if reader buffer not holds the completed packet
        // then we set the content_of_file_.last_packet_remain_ for next loop to process
        if (complete_packet_length > local_read_avail) {
          consume_length = local_read_avail;
          content_of_file_.last_packet_remain_ = complete_packet_length - local_read_avail;
        }
        int64_t plugin_compress_request_begin = sm_->get_based_hrtime();
        if (OB_FAIL(build_compressed_packet(content_of_file_.is_last_packet_, consume_length))) {
          PROXY_API_LOG(WDIAG, "fail to build_compressed_packet");
        } else {
          to_consume_length = consume_length;
          PROXY_API_LOG(DEBUG, "succ to compress and consume new packet",
                        "len", payload_length, "seq", seq,
                        "compressed_length", consume_length,
                        "still_remain", content_of_file_.last_packet_remain_);
        }
        sm_->cmd_time_stats_.plugin_compress_request_time_
          += milestone_diff(plugin_compress_request_begin, sm_->get_based_hrtime());
      }
    // 2. if reader buffer still holds part of content of last packet
    //    then compress them to a new compressed
    } else if (content_of_file_.last_packet_remain_ > 0) {
      int64_t plugin_compress_request_begin = sm_->get_based_hrtime();
      // if reader buffer holds all the remain of last packet
      if (local_read_avail >= content_of_file_.last_packet_remain_) {
        // compress and consume content_of_file_.last_packet_remain
        if (OB_FAIL(build_compressed_packet(content_of_file_.is_last_packet_, content_of_file_.last_packet_remain_))) {
          PROXY_API_LOG(WDIAG, "fail to build_compressed_packet");
        } else {
          to_consume_length = content_of_file_.last_packet_remain_;
          PROXY_API_LOG(DEBUG, "succ to compress and consume previous packet remain",
                        "consumed_length", content_of_file_.last_packet_remain_);
          content_of_file_.last_packet_remain_ = 0;
        }
      // if read buffer holds part of the remain of last packet
      } else {
        // compress and consume local_read_avail
        if (OB_FAIL(build_compressed_packet(content_of_file_.is_last_packet_, local_read_avail))) {
          PROXY_API_LOG(WDIAG, "fail to build_compressed_packet");
        } else {
          to_consume_length = local_read_avail;
          content_of_file_.last_packet_remain_ -= local_read_avail;
          PROXY_API_LOG(DEBUG, "succ to compress and consume part of previous packet remain",
                        "consumed_length", local_read_avail,
                        "still_remain", content_of_file_.last_packet_remain_);
        }
      }
      sm_->cmd_time_stats_.plugin_compress_request_time_
        += milestone_diff(plugin_compress_request_begin, sm_->get_based_hrtime());
    } else {
      ret = OB_ERR_UNEXPECTED;
      PROXY_API_LOG(WDIAG, "content_of_file_.last_packet_remain is unexpectedly be less then 0");
    }
    if (OB_SUCC(ret)) {
      // we consume the data from the transfer reader that we just compressed
      if (to_consume_length > 0) {
        int64_t consume_size = local_transfer_reader_->read_avail();
        int64_t produce_size = 0;
        // send the compressed packet in local_transfer_reader_
        if (consume_size > 0) {
          if (consume_size != (produce_size = produce(local_transfer_reader_, consume_size))) {
            ret = OB_ERR_UNEXPECTED;
            PROXY_API_LOG(WDIAG, "fail to produce", "expected size", consume_size,
                          "actual size", produce_size, K(ret));
          } else if (OB_FAIL(local_transfer_reader_->consume(consume_size))) {
            PROXY_API_LOG(WDIAG, "fail to consume local transfer reader", K(consume_size), K(ret));
          } else {
            PROXY_API_LOG(DEBUG, "succ to consume local transfer reader", K(consume_size));
          }
        }
        // we have consumed local reader so block has changed
        if (OB_NOT_NULL(block = local_reader_->block_)) {
          block_buf = block->start() + local_reader_->start_offset_;
          block_length = block->read_avail() - local_reader_->start_offset_;
          local_read_avail = local_reader_->read_avail();
          PROXY_API_LOG(DEBUG, "after consumed from local reader and remain", K(local_read_avail));
        } else {
          PROXY_API_LOG(DEBUG, "local_reader has been consumed all");
        }

      // if we didn't read completed mysql packet header
      // then try to get complete mysql packet header from next block
      } else if (to_consume_length == 0) {
        if (OB_NOT_NULL(block = block->next_)) {
          block_buf = block->start();
          block_length = block->read_avail();
        }
      } else {
        // never be here
        ret = OB_ERR_UNEXPECTED;
        PROXY_API_LOG(WDIAG, "unexpected", K(to_consume_length), K(ret));
      }
    }
  }
  return ret;
}

int ObMysqlRequestCompressTransformPlugin::build_compressed_packet(bool is_last_segment, int64_t to_compress_len)
{
  int ret = OB_SUCCESS;
  const int64_t compression_level = sm_->compression_algorithm_.level_;
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

    char flt_info_buf[SERVER_FLT_INFO_BUF_MAX_LEN] = "\0";
    char sess_info_veri_buf[OB_SESS_INFO_VERI_BUF_MAX] = "\0";
    const bool is_proxy_switch_route = sm_->is_proxy_switch_route();

    // buf to fill extra info
    char *total_flt_info_buf = flt_info_buf;
    int64_t total_flt_info_buf_len = SERVER_FLT_INFO_BUF_MAX_LEN;

    // alloc show trace buffer
    if (OB_FAIL(ObProxyTraceUtils::build_show_trace_info_buffer(sm_, is_last_segment, total_flt_info_buf,
                                                                total_flt_info_buf_len))) {
      PROXY_API_LOG(WDIAG, "fail to build show trace info buffer", K(ret));
    } else if (OB_FAIL(ObProxyTraceUtils::build_related_extra_info_all(extra_info, sm_,
                                                                       client_ip_buf, MAX_IP_BUFFER_LEN,
                                                                       total_flt_info_buf, total_flt_info_buf_len,
                                                                       sess_info_veri_buf, OB_SESS_INFO_VERI_BUF_MAX,
                                                                       sess_info_value, is_last_segment,
                                                                       is_proxy_switch_route))) {
      PROXY_API_LOG(WDIAG, "fail to build related extra info all", K(ret));
    } else {
      ObMysqlServerSession *server_session = sm_->get_server_session();
      ObMysqlClientSession *client_session = sm_->get_client_session();
      const bool is_weak_read = (WEAK == sm_->trans_state_.get_trans_consistency_level(client_session->get_session_info()));
      const int64_t compression_level = sm_->compression_algorithm_.level_;
      const bool is_compressed_ob20 = (compression_level != 0 && server_session->get_session_info().is_server_ob20_compress_supported());
      Ob20HeaderParam ob20_head_param(server_session->get_server_sessid(), request_id_, compressed_seq_,
                                      compressed_seq_, is_last_segment, is_weak_read, is_need_reroute,
                                      server_session->get_session_info().is_new_extra_info_supported(),
                                      client_session->is_trans_internal_routing(), is_proxy_switch_route,
                                      is_compressed_ob20, compression_level);
      INC_SHARED_REF(ob20_head_param.get_protocol_diagnosis_ref(), sm_->protocol_diagnosis_);
      if (OB_FAIL(ObProto20Utils::consume_and_compress_data(local_reader_, mio_buffer_, to_compress_len,
                                                            ob20_head_param, &extra_info))) {
        PROXY_API_LOG(WDIAG, "fail to consume and compress data with OB20", K(ret));
      }
    }

    // free show trace buffer after use, in both succ and fail
    if (total_flt_info_buf_len > SERVER_FLT_INFO_BUF_MAX_LEN
        && total_flt_info_buf != NULL) {
      ob_free(total_flt_info_buf);
      total_flt_info_buf = NULL;
    }
  } else {
    ObCmpHeaderParam param(compressed_seq_, is_checksum_on, compression_level);
    INC_SHARED_REF(param.get_protocol_diagnosis_ref(), sm_->protocol_diagnosis_);
    if (OB_FAIL(ObMysqlAnalyzerUtils::consume_and_compress_data(local_reader_, mio_buffer_, to_compress_len, param))) {
      PROXY_API_LOG(WDIAG, "fail to consume and compress data", K(ret));
    } else {
      compressed_seq_ = param.get_compressed_seq();
    }
  }

  if (OB_SUCC(ret)) {
    int64_t compressed_len = local_transfer_reader_->read_avail();
    sm_->get_server_session()->set_compressed_seq(compressed_seq_++);

    PROXY_API_LOG(DEBUG, "build compressed packet succ", "origin len", to_compress_len,
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
    PROXY_API_LOG(EDIAG, "fail to get write ntodo", K(ret));
  } else if (OB_UNLIKELY(ntodo <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_API_LOG(EDIAG, "get_data_to_read must > 0", K(ntodo), K(ret));
  } else if (OB_UNLIKELY(read_avail > ntodo)) { // just defense
    if (read_avail >= (ntodo + MYSQL_NET_META_LENGTH)) {
      char tmp_buff[MYSQL_NET_META_LENGTH]; // print the next packet's meta
      reader.copy(tmp_buff, MYSQL_NET_META_LENGTH, ntodo);
      int64_t payload_len = uint3korr(tmp_buff);
      int64_t seq = uint1korr(tmp_buff + 3);
      int64_t cmd = static_cast<uint8_t>(tmp_buff[4]);
      PROXY_API_LOG(EDIAG, "next packet meta is", K(payload_len), K(seq), K(cmd));
    }
    ret = OB_ERR_UNEXPECTED;
    PROXY_API_LOG(EDIAG, "invalid data, maybe client received more than one mysql packet,"
                  " will disconnect", K(read_avail), K(ntodo), K(ret));
  } else {
    is_last_segment = (read_avail == ntodo);
  }

  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
