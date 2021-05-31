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

#define USING_LOG_PREFIX PROXY
#include "rpc/obmysql/ob_mysql_util.h"
#include "proxy/mysqllib/ob_mysql_compress_analyzer.h"
#include "proxy/mysqllib/ob_mysql_response.h"
#include "proxy/mysqllib/ob_mysql_analyzer_utils.h"
#include "proxy/mysqllib/ob_proxy_parser_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

static int64_t const MYSQL_BUFFER_SIZE = BUFFER_SIZE_FOR_INDEX(BUFFER_SIZE_INDEX_8K);

int ObMysqlCompressAnalyzer::init(
    const uint8_t last_seq, const AnalyzeMode mode,
    const obmysql::ObMySQLCmd mysql_cmd,
    const bool enable_extra_ok_packet_for_stats,
    const uint8_t last_ob20_seq,
    const uint32_t request_id,
    const uint32_t sessid)
{
  int ret = OB_SUCCESS;

  UNUSED(last_ob20_seq);
  UNUSED(request_id);
  UNUSED(sessid);

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(is_inited_), K(ret));
  } else {
    last_seq_ = last_seq;
    mode_ = mode;
    mysql_cmd_ = mysql_cmd;
    enable_extra_ok_packet_for_stats_ = enable_extra_ok_packet_for_stats;
    reset_out_buffer();
    if (DECOMPRESS_MODE == mode_) {
      if (OB_ISNULL(out_buffer_ = new_miobuffer(MYSQL_BUFFER_SIZE))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to new miobuffer", K(ret));
      }
    }
    is_inited_ = true;
  }
  return ret;
}

void ObMysqlCompressAnalyzer::reset_out_buffer()
{
  if (NULL != out_buffer_) {
    free_miobuffer(out_buffer_);
  }
  out_buffer_ = NULL;
}

void ObMysqlCompressAnalyzer::reset()
{
  is_inited_ = false;
  last_seq_ = 0;
  mysql_cmd_ = OB_MYSQL_COM_MAX_NUM;
  enable_extra_ok_packet_for_stats_ = false;
  is_last_packet_ = false;
  is_stream_finished_ = false;
  mode_ = SIMPLE_MODE;
  remain_len_ = 0;
  header_valid_len_ = 0;
  curr_compressed_header_.reset();
  reset_out_buffer();
  if (NULL != last_packet_buffer_ && last_packet_len_ > 0) {
    op_fixed_mem_free(last_packet_buffer_, last_packet_len_);
  }
  last_packet_buffer_ = NULL;
  last_packet_len_ = 0;
  last_packet_filled_len_ = 0;
  compressor_.reset();
}

int ObMysqlCompressAnalyzer::analyze_compressed_response_with_length(event::ObIOBufferReader &reader, uint64_t decompress_size)
{
  int ret = OB_SUCCESS;
  ObIOBufferBlock *block = NULL;
  int64_t offset = 0;
  char *data = NULL;
  int64_t data_size = 0;
  if (NULL != reader.block_) {
    reader.skip_empty_blocks();
    block = reader.block_;
    offset = reader.start_offset_;
    data = block->start() + offset;
    data_size = block->read_avail() - offset;
  }

  if (data_size <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the first block data_size in reader is less than 0",
             K(offset), K(block->read_avail()), K(ret));
  } else if (data_size > decompress_size) {
    data_size = decompress_size;
  }

  ObString resp_buf;
  ObMysqlResp resp;
  while (OB_SUCC(ret) && NULL != block && decompress_size > 0) {
    resp_buf.assign_ptr(data, static_cast<int32_t>(data_size));
    if (OB_FAIL(analyze_compressed_response(resp_buf, resp))) {
      LOG_WARN("fail to analyze compressed response", K(ret));
    } else {
      decompress_size -= data_size;
      if (decompress_size > 0) {
        // on to the next block
        offset = 0;
        block = block->next_;
        if (NULL != block) {
          data = block->start();
          data_size = block->read_avail();
          if (data_size > decompress_size) {
            data_size = decompress_size;
          }
        }
      }
    }
  }
  LOG_DEBUG("analyze compressed response with length finished", "decompress_size", decompress_size, K(resp));

  return ret;
}

int ObMysqlCompressAnalyzer::analyze_compressed_response(event::ObIOBufferReader &reader, ObMysqlResp &resp)
{
  int ret = OB_SUCCESS;
  ObIOBufferBlock *block = NULL;
  int64_t offset = 0;
  char *data = NULL;
  int64_t data_size = 0;
  if (NULL != reader.block_) {
    reader.skip_empty_blocks();
    block = reader.block_;
    offset = reader.start_offset_;
    data = block->start() + offset;
    data_size = block->read_avail() - offset;
  }

  if (data_size <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the first block data_size in reader is less than 0",
             K(offset), K(block->read_avail()), K(ret));
  }

  ObString resp_buf;
  while (OB_SUCC(ret) && NULL != block && data_size > 0 && !resp.get_analyze_result().is_resp_completed()) {
    resp_buf.assign_ptr(data, static_cast<int32_t>(data_size));
    if (OB_FAIL(analyze_compressed_response(resp_buf, resp))) {
      LOG_WARN("fail to analyze compressed response", K(ret));
    } else {
      // on to the next block
      offset = 0;
      block = block->next_;
      if (NULL != block) {
        data = block->start();
        data_size = block->read_avail();
        // just for defense
        if (resp.get_analyze_result().is_resp_completed()) {
          if (OB_UNLIKELY(0 != data_size)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("compressed resp completed, but data_size != 0", K(data_size), K(ret));
          }
        }
      }
    }
  }
  LOG_DEBUG("analyze compressed response finished", "data_size", reader.read_avail(), K(resp));

  return ret;
}

int ObMysqlCompressAnalyzer::analyze_compressed_response(const ObString &compressed_data, ObMysqlResp &resp)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(is_inited_), K(ret));
  } else if (OB_UNLIKELY(compressed_data.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty compressed data", K(compressed_data), K(ret));
  } else {
    int64_t origin_len = compressed_data.length();
    int64_t avail_len = origin_len;
    const char *start = compressed_data.ptr();

    while (OB_SUCC(ret) && avail_len > 0) {
      if (0 == remain_len_) { // next compressed packet start
        if (OB_FAIL(decode_compressed_header(compressed_data, avail_len))) {
          LOG_WARN("fail to decode compressed header", K(ret));
        }
      } else if (remain_len_ > 0) { // just consume
        const char *buf_start = start + origin_len - avail_len;
        int64_t buf_len = 0;
        if (remain_len_ > avail_len) {
          buf_len = avail_len;
          remain_len_ -= avail_len;
          avail_len = 0; // need more data
        } else {
          buf_len = remain_len_;
          avail_len -= remain_len_;
          remain_len_ = 0;
          header_valid_len_ = 0; // prepare next compressed packet
        }

        // whether this is last part of one compressed packet
        bool is_last_data = (0 == remain_len_);
        if (SIMPLE_MODE == mode_) {
          if (is_last_packet_ && is_last_data) {
            // stream completed
            ObRespAnalyzeResult &analyze_result = resp.get_analyze_result();
            analyze_result.is_resp_completed_ = true;
            is_stream_finished_ = true;
            LOG_DEBUG("simple mode, compressed stream complete", K(mode_));
          }
        } else if (DECOMPRESS_MODE == mode_) {
          if (is_last_packet_) {
            if (OB_FAIL(analyze_last_compress_packet(buf_start, buf_len, is_last_data, resp))) {
              LOG_WARN("fail to analyze last compress packet", KP(buf_start),
                       K(buf_len), K(is_last_data), K(ret));
            }
          } else if (OB_FAIL(decompress_data(buf_start, buf_len, resp))) {
            LOG_WARN("fail to decompress data", KP(buf_start), K(buf_len), K(ret));
          } else {
            resp.get_analyze_result().has_more_compress_packet_ = true;
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid analyze mode", K_(mode), K(ret));
        }
      } else { // remain_len_ < 0
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid last remain len", K_(remain_len), K(ret));
      }
    }
  }
  return ret;
}

int ObMysqlCompressAnalyzer::decode_compressed_header(
    const ObString &compressed_data,
    int64_t &avail_len)
{
  int ret = OB_SUCCESS;
  int64_t origin_len = compressed_data.length();
  const char *start = compressed_data.ptr();
  const char *header_buffer_start = NULL;

  // an optimization, if header all in one buf, no need copy
  if ((0 == header_valid_len_) && (avail_len >= MYSQL_COMPRESSED_HEALDER_LENGTH)) {
    header_buffer_start = start + origin_len - avail_len;
    avail_len -= MYSQL_COMPRESSED_HEALDER_LENGTH;
    header_valid_len_ = MYSQL_COMPRESSED_HEALDER_LENGTH;
  } else {
    if (header_valid_len_ < MYSQL_COMPRESSED_HEALDER_LENGTH) {
      int64_t need_len = MYSQL_COMPRESSED_HEALDER_LENGTH - header_valid_len_;
      int64_t copy_len = (avail_len >= need_len) ? (need_len) : (avail_len);
      MEMCPY(header_buf_ + header_valid_len_, (start + origin_len - avail_len), copy_len);
      avail_len -= copy_len;
      header_valid_len_ += copy_len;
      header_buffer_start = header_buf_;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("never happen", K_(header_valid_len), K(curr_compressed_header_),
               K(avail_len), K(is_last_packet_), K(ret));
      abort();
    }
  }

  if (OB_SUCC(ret) && (header_valid_len_ == MYSQL_COMPRESSED_HEALDER_LENGTH)) {
    // compress packet header received completely, and start to analyze
    curr_compressed_header_.reset();
    if (OB_FAIL(ObMysqlAnalyzerUtils::analyze_compressed_packet_header(
            header_buffer_start, MYSQL_COMPRESSED_HEALDER_LENGTH, curr_compressed_header_))) {
      LOG_WARN("fail to analyze compressed packet header", K(ret));
    } else {
      if (last_seq_ == curr_compressed_header_.seq_) {
        is_last_packet_ = true;
      }
      last_seq_ = curr_compressed_header_.seq_;
      remain_len_ = curr_compressed_header_.compressed_len_;
      LOG_DEBUG("decode compressed header succ", K_(curr_compressed_header));
    }
  }

  return ret;
}

int ObMysqlCompressAnalyzer::analyze_last_compress_packet(
    const char *start, const int64_t len,
    const bool is_last_data, ObMysqlResp &resp)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(start) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", KP(start), K(len), K(ret));
  } else {
    // alloc buff if needed
    if (NULL == last_packet_buffer_) {
      //NOTE::add 1 is used to decide whether decompress compelete easy
      if (is_compressed_payload()) {
        last_packet_len_ = curr_compressed_header_.non_compressed_len_ + 1;
      } else {
        last_packet_len_ = curr_compressed_header_.compressed_len_ + 1;
      }
      if (OB_ISNULL(last_packet_buffer_ = static_cast<char *>(op_fixed_mem_alloc(last_packet_len_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc mem for last_packet_buffer_", "alloc_size", last_packet_len_, K(ret));
      }
    }

    // decopress data
    if (OB_SUCC(ret)) {
      if (OB_FAIL(decompress_last_mysql_packet(start, len))) {
        LOG_WARN("fail to decompress last mysql packet", K(ret));
      }
    }

    // analyze the last compress packet
    if (OB_SUCC(ret) && is_last_data) {
      if (OB_FAIL(do_analyze_last_compress_packet(resp))) {
        LOG_WARN("fail to do analyze last compress packet", K(ret));
      }
    }
  }

  return ret;
}

int ObMysqlCompressAnalyzer::decompress_last_mysql_packet(const char *compressed_data, const int64_t len)
{
  int ret = OB_SUCCESS;
  int64_t filled_len = 0;
  int64_t free_len = last_packet_len_ - last_packet_filled_len_;
  char *buf_start = last_packet_buffer_ + last_packet_filled_len_;
  if (OB_ISNULL(compressed_data) || OB_ISNULL(last_packet_buffer_)
      || OB_UNLIKELY(free_len <= 0) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", KP(compressed_data), K(len), K_(last_packet_buffer),
             K(free_len), K(ret));
  } else {
    if (is_compressed_payload()) {
      if (OB_FAIL(compressor_.add_decompress_data(compressed_data, len))) {
        LOG_WARN("fail to add decompress data", K(ret));
      } else if (OB_FAIL(compressor_.decompress(buf_start, free_len, filled_len))) {
        LOG_WARN("fail to decompress", KP(buf_start), K(free_len), K(filled_len), K(ret));
      } else if (filled_len > free_len) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the packet buff is enough, should not happen", K(filled_len), K(free_len), K(ret));
      } else {
        last_packet_filled_len_ += filled_len;
      }
    } else {
      //uncompressed payload, just copy
      MEMCPY(buf_start, compressed_data, len);
      last_packet_filled_len_ += len;
    }
  }
  return ret;
}

int ObMysqlCompressAnalyzer::do_analyze_last_compress_packet(ObMysqlResp &resp)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(last_packet_buffer_) || OB_UNLIKELY(last_packet_len_ != (last_packet_filled_len_ + 1))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP_(last_packet_buffer), K_(last_packet_len),
             K_(last_packet_filled_len), K(ret));
  } else {
    // the last compress packet only has four types:
    // 1. ok
    // 2. eof + ok
    // 3. error + ok
    // 4. string<EOF> Human readable string, just for OB_MYSQL_COM_STATISTICS
    ObMysqlPacketMeta meta1; // the fist packet's meta
    char *body_start = NULL; // the start pos of mysql packet body
    int64_t body_len = 0;    // the len of mysql packet body
    ObMysqlRespEndingType ending_type = MAX_PACKET_ENDING_TYPE;

    if (OB_MYSQL_COM_STATISTICS == mysql_cmd_) {
      ending_type = STRING_EOF_ENDING_TYPE;
      if (OB_FAIL(analyze_string_eof_packet(resp))) {
        LOG_WARN("fail to analyze string eof packet", K(meta1),
                 "request cmd", ObProxyParserUtils::get_sql_cmd_name(mysql_cmd_), K(ret));
      }
    } else {
      if (OB_FAIL(ObProxyParserUtils::analyze_mysql_packet_meta(
              last_packet_buffer_, last_packet_filled_len_, meta1))) {
        LOG_WARN("fail to analyze mysql packet meta",
                 "request cmd", ObProxyParserUtils::get_sql_cmd_name(mysql_cmd_), K(ret));
      } else {
        switch (meta1.pkt_type_) {
          case MYSQL_ERR_PACKET_TYPE : {
            // only error packet is error resp, other is result set
            if (OB_MYSQL_COM_STMT_PREPARE_EXECUTE == mysql_cmd_ && resp.get_analyze_result().has_more_compress_packet()) {
              ending_type = EOF_PACKET_ENDING_TYPE;
            } else {
              ending_type = ERROR_PACKET_ENDING_TYPE;
            }
            body_start = last_packet_buffer_ + MYSQL_NET_HEADER_LENGTH;
            body_len = meta1.pkt_len_ - MYSQL_NET_HEADER_LENGTH;
            if (OB_FAIL(analyze_error_packet(body_start, body_len, resp))) {
              LOG_WARN("fail to analyze error packet", K(ret));
            } else if (OB_FAIL(analyze_last_ok_packet(meta1.pkt_len_, resp))) {
              LOG_WARN("fail to analyze last ok packet", K(meta1), K(ret));
            }
            break;
          }
          case MYSQL_OK_PACKET_TYPE : {
            ending_type = OK_PACKET_ENDING_TYPE;
            body_start = last_packet_buffer_ + MYSQL_NET_HEADER_LENGTH;
            body_len = meta1.pkt_len_ - MYSQL_NET_HEADER_LENGTH;
            // only one ok packet, we need rewrite later
            if (OB_MYSQL_COM_FIELD_LIST == mysql_cmd_|| OB_MYSQL_COM_STMT_PREPARE == mysql_cmd_) {
              resp.get_analyze_result().ok_packet_action_type_ = OK_PACKET_ACTION_CONSUME;
            } else {
              resp.get_analyze_result().ok_packet_action_type_ = OK_PACKET_ACTION_REWRITE;
            }
            resp.get_analyze_result().last_ok_pkt_len_ = meta1.pkt_len_; // include header
            if (OB_FAIL(analyze_ok_packet(body_start, body_len, resp))) {
              LOG_WARN("fail to analyze ok packet", K(body_len), KP(body_start), K(ret));
            }
            break;
          }
          case MYSQL_EOF_PACKET_TYPE : {
            ending_type = EOF_PACKET_ENDING_TYPE;
            // just skip analyze eof packet len, just analyze the last ok packet
            if (OB_FAIL(analyze_last_ok_packet(meta1.pkt_len_, resp))) {
              LOG_WARN("fail to analyze last ok packet", K(meta1), K(ret));
            }
            break;
          }
          default : {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid packet type", K(meta1),
                     "request cmd", ObProxyParserUtils::get_sql_cmd_name(mysql_cmd_), K(ret));
            break;
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (NULL != out_buffer_) {
        // copy the last packet data(non-compressed) to out_buffer_
        int64_t written_len = 0;
        int64_t packet_len = last_packet_len_ - 1;
        out_buffer_->write(last_packet_buffer_, packet_len, written_len);
        if (OB_UNLIKELY(written_len != packet_len)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to write", K(mode_), K(written_len), K(packet_len), K(ret));
        }
      }

      ObRespAnalyzeResult &analyze_result = resp.get_analyze_result();
      analyze_result.is_resp_completed_ = true;
      analyze_result.ending_type_ = ending_type;
      is_stream_finished_ = true;
      LOG_DEBUG("the last compressed analyze complete",
                "request cmd", ObProxyParserUtils::get_sql_cmd_name(mysql_cmd_), K(resp));
    }
  }

  return ret;
}

inline int ObMysqlCompressAnalyzer::analyze_string_eof_packet(ObMysqlResp &resp)
{
  int ret = OB_SUCCESS;
  ObMySQLPacketHeader header;
  if (OB_FAIL(ObProxyParserUtils::analyze_mysql_packet_header(
              last_packet_buffer_, last_packet_filled_len_, header))) {
    LOG_WARN("fail to analyze mysql packet header",
             "request cmd", ObProxyParserUtils::get_sql_cmd_name(mysql_cmd_), K(ret));
  } else {
    LOG_DEBUG("string<EOF> packet", K(header), K_(enable_extra_ok_packet_for_stats),
              "request cmd", ObProxyParserUtils::get_sql_cmd_name(mysql_cmd_));
    if (enable_extra_ok_packet_for_stats_) {
      if ((header.len_ + MYSQL_NET_HEADER_LENGTH) >= last_packet_filled_len_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid string<EOF> Human readable string", K(header),
                 K(last_packet_filled_len_), K(header), K(ret));
      } else {
        // analyze last ok packet
        if (OB_FAIL(analyze_last_ok_packet(header.len_ + MYSQL_NET_HEADER_LENGTH, resp))) {
          LOG_WARN("fail to analyze ok packet", K(header), K(ret));
        }
      }
    } else {
      if ((header.len_ + MYSQL_NET_HEADER_LENGTH) != last_packet_filled_len_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid string<EOF> Human readable string", K(header),
                 K(last_packet_filled_len_), K(header), K(ret));
      } else {
        // if server not support extra ok packet, treat it as in trans
        ObRespAnalyzeResult &analyze_result = resp.get_analyze_result();
        analyze_result.is_trans_completed_ = false;
        resp.get_analyze_result().ok_packet_action_type_ = OK_PACKET_ACTION_SEND;
      }
    }
  }

  return ret;
}

int ObMysqlCompressAnalyzer::analyze_last_ok_packet(
    const int64_t last_pkt_total_len, // include header
    ObMysqlResp &resp)
{
  int ret = OB_SUCCESS;
  ObMysqlPacketMeta ok_pkt_meta;
  // the total buffer must > first paket len
  if (last_packet_filled_len_ <= last_pkt_total_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("there must have ok packet after first packet", K(last_pkt_total_len),
             K(last_packet_filled_len_), K(ret));
  // get ok packet meta
  } else if (OB_FAIL(ObProxyParserUtils::analyze_mysql_packet_meta(
          last_packet_buffer_ + last_pkt_total_len,
          last_packet_filled_len_ - last_pkt_total_len, ok_pkt_meta))) {
    LOG_WARN("fail to analyze mysql packet meta", K(ret));
  } else if (last_pkt_total_len + ok_pkt_meta.pkt_len_ != last_packet_filled_len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the last compressed packet must be contain two packet",
             K(last_pkt_total_len), K(ok_pkt_meta), K(last_packet_filled_len_), K(ret));
  } else {
    resp.get_analyze_result().reserved_len_ = 0; // the last packet is received, no need reserve
    resp.get_analyze_result().ok_packet_action_type_ = OK_PACKET_ACTION_CONSUME;
    resp.get_analyze_result().last_ok_pkt_len_ = ok_pkt_meta.pkt_len_; // include header

    char *body_start = last_packet_buffer_ + last_pkt_total_len + MYSQL_NET_HEADER_LENGTH;
    int64_t body_len = ok_pkt_meta.pkt_len_ - MYSQL_NET_HEADER_LENGTH;
    if (OB_FAIL(analyze_ok_packet(body_start, body_len, resp))) {
      LOG_WARN("fail to analyze ok packet", K(body_len), KP(body_start), K(ret));
    }
  }

  return ret;
}

int ObMysqlCompressAnalyzer::analyze_error_packet(const char *ptr, const int64_t len, ObMysqlResp &resp)
{
  int ret = OB_SUCCESS;
  ObVariableLenBuffer<FIXED_MEMORY_BUFFER_SIZE> &content_buf = resp.get_analyze_result().error_pkt_buf_;
  if (OB_FAIL(content_buf.init(len))) {
    LOG_WARN("fail int alloc mem", K(len), K(ret));
  } else {
    char *pos = content_buf.pos();
    MEMCPY(pos, ptr, len); // len is packet body len, not include packet header
    if (OB_FAIL(content_buf.consume(len))) {
      LOG_WARN("fail to consume", K(len), K(ret));
    } else {
      OMPKError &err_pkt = resp.get_analyze_result().get_error_pkt();
      err_pkt.set_content(pos, static_cast<uint32_t>(len));
      if (OB_FAIL(err_pkt.decode())) {
        LOG_WARN("fail to decode error packet", K(ret));
      }
    }
  }

  return ret;
}

int ObMysqlCompressAnalyzer::analyze_ok_packet(const char *ptr, const int64_t len, ObMysqlResp &resp)
{
  int ret = OB_SUCCESS;
  // only need to get the OB_SERVER_STATUS_IN_TRANS bit
  const char *pos = ptr;
  uint64_t affected_rows = 0;
  uint64_t last_insert_id = 0;
  uint8_t ok_packet_type = 0;
  ObServerStatusFlags server_status;

  ObMySQLUtil::get_uint1(pos, ok_packet_type);
  if (OB_FAIL(ObMySQLUtil::get_length(pos, affected_rows))) {
    LOG_WARN("get length failed", K(ptr), K(pos), K(affected_rows), K(ret));
  } else if (OB_FAIL(ObMySQLUtil::get_length(pos, last_insert_id))) {
    LOG_WARN("get length failed", K(ptr), K(pos), K(last_insert_id), K(ret));
  } else if (FALSE_IT(ObMySQLUtil::get_uint2(pos, server_status.flags_))) {
    // impossible
  } else if (OB_UNLIKELY((pos - ptr) > len)) { // just defnese
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ptr pos", KP(ptr), KP(pos), K(len), K(ret));
  } else {
    ObRespAnalyzeResult &analyze_result = resp.get_analyze_result();
    if (server_status.status_flags_.OB_SERVER_STATUS_IN_TRANS) {
      analyze_result.is_trans_completed_ = false;
    } else {
      analyze_result.is_trans_completed_ = true;
    }
  }
  return ret;
}

int ObMysqlCompressAnalyzer::decompress_data(const char *zprt, const int64_t zlen, ObMysqlResp &resp)
{
  int ret = OB_SUCCESS;
  UNUSED(resp);
  if (OB_ISNULL(out_buffer_) || OB_ISNULL(zprt) || OB_UNLIKELY(zlen < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K_(out_buffer), KP(zprt), K(zlen), K(ret));
  } else {
    int64_t filled_len = 0;
    if (is_compressed_payload()) {
      char *buf = NULL;
      int64_t len = 0;
      bool stop = false;
      if (OB_FAIL(compressor_.add_decompress_data(zprt, zlen))) {
        LOG_WARN("fail to add decompress data", K(ret));
      }
      while (OB_SUCC(ret) && !stop) {
        if (OB_FAIL(out_buffer_->get_write_avail_buf(buf, len))) {
          LOG_WARN("fail to get successive vuf", K(len), K(ret));
        } else if (OB_ISNULL(buf) || (0 == len)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid argument", KP(buf), K(len), K(ret));
        } else if (OB_FAIL(compressor_.decompress(buf, len, filled_len))) {
          LOG_WARN("fail to decompress", KP(buf), K(len), K(ret));
        } else {
          if (filled_len > 0) {
            out_buffer_->fill(filled_len);
          }
          if (len > filled_len) { // complete
            stop = true;
          }
        }
      }
    } else {
      //uncompressed payload, just copy
      //TODO:: this can do better, remove_append
      if (OB_FAIL(out_buffer_->write(zprt, zlen, filled_len))) {
        LOG_WARN("fail to write uncompressed payload", K(zlen), K(filled_len), K(ret));
      } else if (OB_UNLIKELY(zlen != filled_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to write uncompressed payload", K(zlen), K(filled_len), K(ret));
      }
    }
  }
  return ret;
}

int ObMysqlCompressAnalyzer::analyze_one_compressed_packet(ObIOBufferReader &reader,
                                                           ObMysqlCompressedAnalyzeResult &result)
{
  return ObMysqlAnalyzerUtils::analyze_one_compressed_packet(reader, result);
}

bool ObMysqlCompressAnalyzer::is_last_packet(const ObMysqlCompressedAnalyzeResult &result)
{
  return result.header_.seq_ == last_seq_;
}

int ObMysqlCompressAnalyzer::analyze_first_response(
    ObIOBufferReader &reader,
    const bool need_receive_completed,
    ObMysqlCompressedAnalyzeResult &result,
    ObMysqlResp &resp)
{
  int ret = OB_SUCCESS;
  result.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(is_inited_), K(ret));
  } else if (OB_FAIL(analyze_one_compressed_packet(reader, result))) {
    LOG_WARN("fail to analyze one compressed packet", K(ret));
  } else if (ANALYZE_DONE == result.status_) { // one compressed packet received completely
    if (is_last_packet(result)) {    // only has one compressed packet
      resp.get_analyze_result().is_resultset_resp_ = false;
      mode_ = DECOMPRESS_MODE;
    } else {
      resp.get_analyze_result().is_resultset_resp_ = true;
    }

    if (mode_ == DECOMPRESS_MODE || need_receive_completed) {
      ObIOBufferReader *transfer_reader = NULL;
      if (mode_ == DECOMPRESS_MODE) {
        if (NULL == out_buffer_ && OB_ISNULL(out_buffer_ = new_miobuffer(MYSQL_BUFFER_SIZE))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to new miobuffer", K(ret));
        } else if (OB_ISNULL(transfer_reader = get_transfer_reader())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get transfer reader", K(transfer_reader), K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(analyze_compressed_response(reader, resp))) { // analyze it
          LOG_WARN("fail to analyze compressed response", K(ret));
        } else {
          if (resp.get_analyze_result().is_resp_completed()) {
            result.is_checksum_on_ = curr_compressed_header_.is_compressed_payload();
            result.status_ = ANALYZE_DONE;
            if (mode_ == DECOMPRESS_MODE) {
              int64_t written_len = 0;
              int64_t tmp_read_avail = transfer_reader->read_avail();
              // consume all compressed data, and write uncompressed data to server miobuffer
              if (OB_FAIL(reader.consume_all())) {
                LOG_WARN("fail to consume all", K(ret));
              } else if (OB_FAIL(reader.mbuf_->write(transfer_reader, tmp_read_avail, written_len))) {
                LOG_WARN("fail to write", K(tmp_read_avail), K(ret));
              } else if (OB_UNLIKELY(written_len != tmp_read_avail)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("written_len is not expected", K(written_len), K(tmp_read_avail), K(ret));
              } else {
                resp.get_analyze_result().is_decompressed_ = true;
                LOG_DEBUG("receive completed", K(need_receive_completed), K(result));
              }
            }
          } else {
            result.status_ = ANALYZE_CONT;
            LOG_DEBUG("not receive completed", K(result));
          }
        }
      }

      if (NULL != transfer_reader) {
        if (OB_FAIL(transfer_reader->consume_all())) {
          LOG_WARN("fail to consume all", K(transfer_reader), K(ret));
        }
        transfer_reader->dealloc();
        transfer_reader = NULL;
      }
    }
  }
  return ret;
}

int ObMysqlCompressAnalyzer::analyze_first_response(
    ObIOBufferReader &reader,
    ObMysqlCompressedAnalyzeResult &result)
{
  int ret = OB_SUCCESS;
  result.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(is_inited_), K(ret));
  } else if (OB_FAIL(analyze_one_compressed_packet(reader, result))) {
    LOG_WARN("fail to analyze one compressed packet", K(ret));
  } else if (ANALYZE_DONE == result.status_) { // one compressed packet received completely
    if (OB_FAIL(analyze_compressed_response_with_length(reader, result.header_.compressed_len_ + MYSQL_COMPRESSED_HEALDER_LENGTH))) { // analyze it
      LOG_WARN("fail to analyze compressed response with length", K(ret));
    }
  }

  return ret;
}

int ObMysqlCompressAnalyzer::analyze_response(event::ObIOBufferReader &reader, ObMysqlResp *resp)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(resp)) {
    int ret = OB_INVALID_ARGUMENT;
    LOG_WARN("resp can not be NULL here", K(resp), K(ret));
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(is_inited_), K(ret));
  } else if (OB_FAIL(analyze_compressed_response(reader, *resp))) {
    LOG_WARN("fail to analyze compressed response", K(ret));
  }

  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
