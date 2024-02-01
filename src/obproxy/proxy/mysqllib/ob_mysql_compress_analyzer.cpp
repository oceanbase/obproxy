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
#include "proxy/mysqllib/ob_protocol_diagnosis.h"

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

ObMysqlCompressAnalyzer::~ObMysqlCompressAnalyzer()
{
  DEC_SHARED_REF(protocol_diagnosis_);
  delloc_tmp_used_buffer();
}

int ObMysqlCompressAnalyzer::init(
    const uint8_t last_seq, const AnalyzeMode mode,
    const obmysql::ObMySQLCmd mysql_cmd,
    const ObMysqlProtocolMode mysql_mode,
    const bool enable_extra_ok_packet_for_stats,
    const uint8_t last_ob20_seq,
    const uint32_t request_id,
    const uint32_t sessid,
    const bool is_analyze_compressed_ob20)
{
  int ret = OB_SUCCESS;

  UNUSED(mysql_mode);
  UNUSED(last_ob20_seq);
  UNUSED(request_id);
  UNUSED(sessid);

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WDIAG("init twice", K(is_inited_), K(ret));
  } else {
    last_seq_ = last_seq;
    mode_ = mode;
    mysql_cmd_ = mysql_cmd;
    enable_extra_ok_packet_for_stats_ = enable_extra_ok_packet_for_stats;
    delloc_tmp_used_buffer();
    if (DECOMPRESS_MODE == mode_ && OB_FAIL(alloc_tmp_used_buffer())) {
      LOG_WDIAG("fail to do alloc_tmp_used_buffer", K(ret));
    } else if (is_analyze_compressed_ob20 && OB_ISNULL(ob20_decompress_buffer_ = new_miobuffer(MYSQL_BUFFER_SIZE))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to new miobuffer", K(ret));
    } else {
      is_inited_ = true;
      analyzer_.set_mysql_mode(mysql_mode);
      result_.set_cmd(mysql_cmd);
      result_.set_mysql_mode(mysql_mode);
      result_.set_enable_extra_ok_packet_for_stats(enable_extra_ok_packet_for_stats);
      LOG_DEBUG("compress analyzer init", K(last_seq), K(mode), K(mysql_cmd), K(enable_extra_ok_packet_for_stats));
    }
  }
  return ret;
}

void ObMysqlCompressAnalyzer::delloc_tmp_used_buffer()
{
  if (NULL != mysql_decompress_buffer_reader_) {
    mysql_decompress_buffer_reader_->dealloc();
    mysql_decompress_buffer_reader_ = NULL;
  }
  if (NULL != mysql_decompress_buffer_) {
    free_miobuffer(mysql_decompress_buffer_);
    mysql_decompress_buffer_ = NULL;
  }
  if (NULL != ob20_decompress_buffer_) {
    free_miobuffer(ob20_decompress_buffer_);
    ob20_decompress_buffer_ = NULL;
  }
}

int ObMysqlCompressAnalyzer::alloc_tmp_used_buffer()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(mysql_decompress_buffer_) &&
      OB_ISNULL(mysql_decompress_buffer_ = new_miobuffer(MYSQL_BUFFER_SIZE))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to do new_miobuffer", K(mysql_decompress_buffer_), K(ret));
  } else if (OB_ISNULL(mysql_decompress_buffer_reader_) &&
             OB_ISNULL(mysql_decompress_buffer_reader_ = mysql_decompress_buffer_->alloc_reader())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to do alloc_reader", K(mysql_decompress_buffer_), K(ret));
  } else {
    /* do nothing */
  }

  return ret;
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
  last_compressed_pkt_remain_len_ = 0;
  header_valid_len_ = 0;
  curr_compressed_header_.reset();
  delloc_tmp_used_buffer();
  compressor_.reset();
  is_analyze_compressed_ob20_ = false;
  result_.reset();
  analyzer_.reset();
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
    LOG_WDIAG("the first block data_size in reader is less than 0",
             K(offset), K(block->read_avail()), K(ret));
  } else if (data_size > decompress_size) {
    data_size = decompress_size;
  }

  ObString resp_buf;
  ObMysqlResp resp;
  while (OB_SUCC(ret) && NULL != block && decompress_size > 0) {
    resp_buf.assign_ptr(data, static_cast<int32_t>(data_size));
    if (OB_FAIL(analyze_compressed_response(resp_buf, resp))) {
      LOG_WDIAG("fail to analyze compressed response", K(ret));
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

/*
  ATTENTION!!!
  1. reader should contain all compressed packets
  2. reader.mio_buf_ shouldn't be modified
*/
int ObMysqlCompressAnalyzer::analyze_compressed_response(
    event::ObIOBufferReader &reader,
    ObMysqlResp &resp)
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
    LOG_WDIAG("the first block data_size in reader is less than 0",
             K(offset), K(block->read_avail()), K(ret));
  }

  ObString resp_buf;
  while (OB_SUCC(ret) && NULL != block && data_size > 0 && !resp.get_analyze_result().is_resp_completed()) {
    resp_buf.assign_ptr(data, static_cast<int32_t>(data_size));
    if (OB_FAIL(analyze_compressed_response(resp_buf, resp))) {
      LOG_WDIAG("fail to analyze compressed response", K(ret));
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
            LOG_EDIAG("compressed resp completed, but data_size != 0", K(data_size), K(ret));
          }
        }
      }
    }
  }
  LOG_DEBUG("analyze compressed response finished", "data_size", reader.read_avail(), K(mode_), K(resp));

  return ret;
}

int ObMysqlCompressAnalyzer::analyze_compressed_response(const ObString &compressed_data, ObMysqlResp &resp)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K(is_inited_), K(ret));
  } else if (OB_UNLIKELY(compressed_data.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("empty compressed data", K(compressed_data), K(ret));
  } else {
    int64_t origin_len = compressed_data.length();
    int64_t avail_len = origin_len;
    const char *start = compressed_data.ptr();

    while (OB_SUCC(ret) && avail_len > 0) {
      // 1. last compressed packet decompressed completely
      //    so read header to prepare decompressed next compressed packet
      if (0 == last_compressed_pkt_remain_len_) { // next compressed packet start
        if (OB_FAIL(decode_compressed_header(compressed_data, avail_len))) {
          LOG_WDIAG("fail to decode compressed header", K(ret));
        }
      // 2. last compressed packet's remain
      //    directly add these data to decompress
      } else if (last_compressed_pkt_remain_len_ > 0) { // just consume
        const char *buf_start = start + origin_len - avail_len;
        int64_t buf_len = 0;
        // the remaining of the one compressed packet across two blocks
        // so we just decompress the first part
        if (last_compressed_pkt_remain_len_ > avail_len) {
          buf_len = avail_len;
          last_compressed_pkt_remain_len_ -= avail_len;
          avail_len = 0;
        // the remaining of the one compressed packet in this block
        // so we finish to decompress this packet
        // and make header_valid_len to 0 to prepare decompress the next one
        } else {
          buf_len = last_compressed_pkt_remain_len_;
          avail_len -= last_compressed_pkt_remain_len_;
          last_compressed_pkt_remain_len_ = 0;
          header_valid_len_ = 0; // prepare next compressed packet
        }

        // whether this is last part of one compressed packet
        bool is_last_data = (0 == last_compressed_pkt_remain_len_);
        if (SIMPLE_MODE == mode_) {
          if (is_last_packet_ && is_last_data) {
            // stream completed
            ObRespAnalyzeResult &analyze_result = resp.get_analyze_result();
            analyze_result.is_resp_completed_ = true;
            is_stream_finished_ = true;
            LOG_DEBUG("simple mode, compressed stream complete", K(mode_));
          }
          LOG_DEBUG("print analyzer in simple mode", K(is_last_packet_), K(is_last_data), K(resp));
        } else if (DECOMPRESS_MODE == mode_) {
          if (is_last_packet_ && is_last_data) {
            if (OB_FAIL(analyze_last_compress_packet(buf_start, buf_len, is_last_data, resp))) {
              LOG_WDIAG("fail to analyze last compress packet", KP(buf_start),
                       K(buf_len), K(is_last_data), K(ret));
            }
          } else if (OB_FAIL(decompress_data(buf_start, buf_len, resp))) {
            LOG_WDIAG("fail to decompress data", KP(buf_start), K(buf_len), K(ret));
          } else {
            resp.get_analyze_result().has_more_compress_packet_ = true;
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("invalid analyze mode", K_(mode), K(ret));
        }
      } else { // last_compressed_pkt_remain_len_ < 0
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("invalid last remain len", K_(last_compressed_pkt_remain_len), K(ret));
      }
    }
  }
  return ret;
}

int ObMysqlCompressAnalyzer::decode_compressed_header(const ObString &compressed_data,
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
      LOG_WDIAG("never happen", K_(header_valid_len), K(curr_compressed_header_),
               K(avail_len), K(is_last_packet_), K(ret));
      abort();
    }
  }

  if (OB_SUCC(ret) && (header_valid_len_ == MYSQL_COMPRESSED_HEALDER_LENGTH)) {
    // compress packet header received completely, and start to analyze
    curr_compressed_header_.reset();
    if (OB_FAIL(ObMysqlAnalyzerUtils::analyze_compressed_packet_header(
            header_buffer_start, MYSQL_COMPRESSED_HEALDER_LENGTH, curr_compressed_header_))) {
      LOG_WDIAG("fail to analyze compressed packet header", K(ret));
    } else {
      if (mode_ == DECOMPRESS_MODE) {
        PROTOCOL_DIAGNOSIS(COMPRESSED_MYSQL, recv, protocol_diagnosis_,
                           curr_compressed_header_.compressed_len_,
                           curr_compressed_header_.seq_,
                           curr_compressed_header_.non_compressed_len_);
      }
      if (last_seq_ == curr_compressed_header_.seq_) {
        is_last_packet_ = true;
      }
      last_seq_ = curr_compressed_header_.seq_;
      last_compressed_pkt_remain_len_ = curr_compressed_header_.compressed_len_;
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
  UNUSED(is_last_data);
  if (OB_ISNULL(start) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", KP(start), K(len), K(ret));
  } else if (OB_FAIL(decompress_data(start, len, resp))) {
    LOG_WDIAG("fail to decompress last mysql packet", K(ret));
  } else if (OB_FAIL(analyze_mysql_packet_end(resp))) {
    LOG_WDIAG("fail to analyze_mysql_packet_end", K(ret));
  }
  return ret;
}

int ObMysqlCompressAnalyzer::decompress_data(const char *zprt, const int64_t zlen, ObMysqlResp &resp)
{
  int ret = OB_SUCCESS;
  UNUSED(resp);
  if (OB_ISNULL(mysql_decompress_buffer_) || OB_ISNULL(zprt) || OB_UNLIKELY(zlen < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K_(mysql_decompress_buffer), KP(zprt), K(zlen), K(ret));
  } else {
    int64_t filled_len = 0;
    if (is_compressed_payload()) {
      char *buf = NULL;
      int64_t len = 0;
      bool stop = false;
      if (OB_FAIL(compressor_.add_decompress_data(zprt, zlen))) {
        LOG_WDIAG("fail to add decompress data", K(ret));
      }
      while (OB_SUCC(ret) && !stop) {
        if (OB_FAIL(mysql_decompress_buffer_->get_write_avail_buf(buf, len))) {
          LOG_WDIAG("fail to get successive vuf", K(len), K(ret));
        } else if (OB_ISNULL(buf) || (0 == len)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("invalid argument", KP(buf), K(len), K(ret));
        } else if (OB_FAIL(compressor_.decompress(buf, len, filled_len))) {
          LOG_WDIAG("fail to decompress", KP(buf), K(len), K(ret));
        } else {
          if (filled_len > 0) {
            mysql_decompress_buffer_->fill(filled_len);
          }
          if (len > filled_len) { // complete
            stop = true;
          }
        }
      }
    } else {
      //uncompressed payload, just copy
      //TODO:: this can do better, remove_append
      if (OB_FAIL(mysql_decompress_buffer_->write(zprt, zlen, filled_len))) {
        LOG_WDIAG("fail to write uncompressed payload", K(zlen), K(filled_len), K(ret));
      } else if (OB_UNLIKELY(zlen != filled_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to write uncompressed payload", K(zlen), K(filled_len), K(ret));
      }
    }
    if (OB_NOT_NULL(mysql_decompress_buffer_reader_)) {
      LOG_DEBUG("start analyze mysql packet");
      mysql_decompress_buffer_reader_->skip_empty_blocks();
      ObIOBufferBlock *block = mysql_decompress_buffer_reader_->block_;
      int64_t offset = mysql_decompress_buffer_reader_->start_offset_;
      char *data = block->start() + offset;
      int64_t data_size = block->read_avail() - offset;
      int64_t analyzed_len = 0;
      while (OB_NOT_NULL(block) && data_size > 0) {
        ObString buf;
        buf.assign_ptr(data, static_cast<int>(data_size));
        ObBufferReader r(buf);
        if (OB_FAIL(analyzer_.analyze_mysql_resp(r, result_, &resp, protocol_diagnosis_))) {
          LOG_WDIAG("fail to analyze mysql packets", K(ret), K(resp));
        } else {
          analyzed_len += data_size;
          if (OB_NOT_NULL(block = block->next_)) {
            data = block->start();
            data_size = block->read_avail();
          }
          resp.get_analyze_result().reserved_len_of_last_ok_ = result_.get_reserved_len();
        }
      }
      mysql_decompress_buffer_reader_->consume(analyzed_len);
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
    LOG_WDIAG("not inited", K(is_inited_), K(ret));
  } else if (OB_FAIL(analyze_one_compressed_packet(reader, result))) {
    LOG_WDIAG("fail to analyze one compressed packet", K(ret));
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
        if (OB_FAIL(alloc_tmp_used_buffer())) {
          LOG_WDIAG("fail to do alloc_tmp_used_buffer", K(ret));
        } else if (OB_ISNULL(transfer_reader = mysql_decompress_buffer_reader_->clone())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("fail to clone mysql_decompress_buffer_reader", K(transfer_reader), K(ret));
        } else {
          /* do nothing */
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObMysqlCompressAnalyzer::analyze_compressed_response(reader, resp))) { // analyze it
          LOG_WDIAG("fail to analyze compressed response", K(ret));
        } else {
          if (resp.get_analyze_result().is_resp_completed()) {
            result.is_checksum_on_ = curr_compressed_header_.is_compressed_payload();
            result.status_ = ANALYZE_DONE;
            if (mode_ == DECOMPRESS_MODE) {
              int64_t written_len = 0;
              int64_t tmp_read_avail = transfer_reader->read_avail();
              // reader.mio_buf -- decompress --> transfer_reader.mio_buf ----> reader.mio_buf
              //  we have decompress all data from reader to transfer_reader.mio_buf
              //  then we write all data in transfer_reader.mio_buf back to reader.mio_buf
              if (OB_FAIL(reader.consume_all())) {
                LOG_WDIAG("fail to consume all", K(ret));
              } else if (OB_FAIL(reader.mbuf_->write(transfer_reader, tmp_read_avail, written_len))) {
                LOG_WDIAG("fail to write", K(tmp_read_avail), K(ret));
              } else if (OB_UNLIKELY(written_len != tmp_read_avail)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WDIAG("written_len is not expected", K(written_len), K(tmp_read_avail), K(ret));
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

      // 这里 transfer_reader 不清理缓存可能会导致内存泄漏
      int tmp_ret = OB_SUCCESS;
      if (NULL != transfer_reader) {
        transfer_reader->dealloc();
        transfer_reader = NULL;
      }

      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    }
  }
  return ret;
}

// called by ObMysqlTransact::do_handle_prepare_response or ObMysqlCompressOB20Analyzer::analyze_first_response
int ObMysqlCompressAnalyzer::analyze_first_response(
    ObIOBufferReader &reader,
    ObMysqlCompressedAnalyzeResult &result)
{
  int ret = OB_SUCCESS;
  result.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(is_inited_), K(ret));
  } else if (OB_FAIL(analyze_one_compressed_packet(reader, result))) {
    LOG_WDIAG("fail to analyze one compressed packet", K(ret));
  } else if (ANALYZE_DONE == result.status_) { // one compressed packet received completely
    if (OB_FAIL(analyze_compressed_response_with_length(reader, result.header_.compressed_len_ + MYSQL_COMPRESSED_HEALDER_LENGTH))) { // analyze it
      LOG_WDIAG("fail to analyze compressed response with length", K(ret));
    }
  }

  return ret;
}

// reader was not modified!!!
int ObMysqlCompressAnalyzer::analyze_response(event::ObIOBufferReader &reader, ObMysqlResp *resp)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(resp)) {
    int ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("resp can not be NULL here", K(resp), K(ret));
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K(is_inited_), K(ret));
  } else if (OB_FAIL(analyze_compressed_response(reader, *resp))) {
    LOG_WDIAG("fail to analyze compressed response", K(ret));
  }
  LOG_DEBUG("[ObMysqlCompressAnalyzer::analyze_response] analyzed reader", K(reader.read_avail()));
  return ret;
}

int ObMysqlCompressAnalyzer::analyze_first_request(ObIOBufferReader &reader,
                                                   ObMysqlCompressedAnalyzeResult &result,
                                                   ObProxyMysqlRequest &req,
                                                   ObMysqlAnalyzeStatus &status)
{
  UNUSED(req);

  int ret = OB_SUCCESS;
  result.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("analyzer not init", K(ret));
  } else if (OB_FAIL(analyze_one_compressed_packet(reader, result))) { // 7 + 24 header decode finish
    LOG_WDIAG("fail to analyze one compressed packet", K(ret));
  } else if (ANALYZE_DONE == result.status_) {
    status = result.status_;

    // total ob20 request received complete here, total_len = ob20.head.compressed_len + mysql compress head(3+1+3)
    // get extra info from ob20 payload
    if (OB_FAIL(analyze_compress_packet_payload(reader, result))) {
      LOG_WDIAG("fail to analyze compress packet payload", K(ret));
      status = ANALYZE_ERROR;
    }
  }

  return ret;
}

int ObMysqlCompressAnalyzer::analyze_compress_packet_payload(event::ObIOBufferReader &reader,
                                                             ObMysqlCompressedAnalyzeResult &result)
{
  UNUSED(reader);
  UNUSED(result);

  int ret = OB_INVALID_ARGUMENT;
  LOG_WDIAG("unexpected protocol type handle.");
  return ret;
}

int ObMysqlCompressAnalyzer::analyze_mysql_packet_end(ObMysqlResp& resp)
{
  int ret = OB_SUCCESS;
  bool is_trans_completed = false;
  bool is_resp_completed = false;
  ObMysqlRespEndingType ending_type = MAX_PACKET_ENDING_TYPE;
  if (!analyzer_.need_wait_more_data()
        && OB_FAIL(result_.is_resp_finished(is_resp_completed, ending_type))) {
      LOG_WDIAG("fail to check is resp finished", K(ending_type), K(ret));
  } else {
    // this mysql response is complete
    if (is_resp_completed) {
      ObRespTransState state = result_.get_trans_state();
      if (NOT_IN_TRANS_STATE_BY_PARSE == state) {
        is_trans_completed = true;
      }
    }

    // just defense
    if (OB_UNLIKELY(is_trans_completed) && OB_UNLIKELY(!is_resp_completed)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("state error", K(is_trans_completed), K(is_resp_completed), K(ret));
    } else if (OB_UNLIKELY(!is_resp_completed) && OB_UNLIKELY(is_trans_completed)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("state error", K(is_trans_completed), K(is_resp_completed), K(ret));
    }

    LOG_DEBUG("analyze trans response succ", K(is_trans_completed), K(is_resp_completed),
              "mode", result_.get_mysql_mode(), "ObMysqlRespEndingType", ending_type);
    ObRespAnalyzeResult &analyze_result = resp.get_analyze_result();
    if (is_resp_completed) {
      analyze_result.is_trans_completed_ = is_trans_completed;
      analyze_result.is_resp_completed_ = is_resp_completed;
      analyze_result.ending_type_ = ending_type;
      is_stream_finished_ = true;
      LOG_DEBUG("analyze result", K(resp));
    }
  }

  return ret;
}
} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
