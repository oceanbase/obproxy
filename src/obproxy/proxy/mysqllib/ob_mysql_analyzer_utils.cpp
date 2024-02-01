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
#include "proxy/mysqllib/ob_mysql_analyzer_utils.h"
#include "iocore/eventsystem/ob_io_buffer.h"
#include "proxy/mysqllib/ob_proxy_parser_utils.h"
#include "proxy/mysqllib/ob_mysql_compressed_packet.h"
#include "obproxy/utils/ob_zlib_stream_compressor.h"
#include "obproxy/utils/ob_fast_zlib_stream_compressor.h"
#include "rpc/obmysql/ob_mysql_util.h"
#include "proxy/mysqllib/ob_mysql_request_analyzer.h"
#include "proxy/mysqllib/ob_protocol_diagnosis.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
ObCmpHeaderParam::ObCmpHeaderParam(const ObCmpHeaderParam &param) {
  compressed_seq_ = param.compressed_seq_;
  compression_level_ = param.compression_level_;
  is_checksum_on_ = param.is_checksum_on_;
  protocol_diagnosis_ = NULL;
  INC_SHARED_REF(protocol_diagnosis_, const_cast<ObProtocolDiagnosis*>(param.get_protocol_diagnosis()));
}

ObCmpHeaderParam &ObCmpHeaderParam::operator=(const ObCmpHeaderParam &param) {
  if (this != &param) {
    compressed_seq_ = param.compressed_seq_;
    compression_level_ = param.compression_level_;
    is_checksum_on_ = param.is_checksum_on_;
    INC_SHARED_REF(protocol_diagnosis_, const_cast<ObProtocolDiagnosis*>(param.get_protocol_diagnosis()));
  }
  return *this;
}

ObCmpHeaderParam::~ObCmpHeaderParam() {
  DEC_SHARED_REF(protocol_diagnosis_);
}

ObProtocolDiagnosis *&ObCmpHeaderParam::get_protocol_diagnosis_ref() {
return protocol_diagnosis_;
}

ObProtocolDiagnosis *ObCmpHeaderParam::get_protocol_diagnosis() {
  return protocol_diagnosis_;
}

const ObProtocolDiagnosis *ObCmpHeaderParam::get_protocol_diagnosis() const{
  return protocol_diagnosis_;
}

int ObMysqlAnalyzerUtils::analyze_one_compressed_packet(
    ObIOBufferReader &reader,
    ObMysqlCompressedAnalyzeResult &result)
{
  int ret = OB_SUCCESS;
  int64_t len = reader.read_avail();

  result.status_ = ANALYZE_CONT;
  // just consider the condition the compressed mysql header cross two buffer block
  if (OB_LIKELY(len >= MYSQL_COMPRESSED_HEALDER_LENGTH)) {
    int64_t block_len = reader.block_read_avail();
    char *buf_start = reader.start();

    if (OB_UNLIKELY(block_len < MYSQL_COMPRESSED_HEALDER_LENGTH)) {
      char mysql_hdr[MYSQL_COMPRESSED_HEALDER_LENGTH];
      char *written_pos = reader.copy(mysql_hdr, MYSQL_COMPRESSED_HEALDER_LENGTH, 0);
      if (OB_UNLIKELY(written_pos != mysql_hdr + MYSQL_COMPRESSED_HEALDER_LENGTH)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("not copy completely", KP(written_pos), K(mysql_hdr),
                 "header_length", MYSQL_COMPRESSED_HEALDER_LENGTH, K(ret));
      } else {
        buf_start = mysql_hdr;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(analyze_compressed_packet_header(buf_start, MYSQL_COMPRESSED_HEALDER_LENGTH, result.header_))) {
        LOG_WDIAG("fail to analyze compressed packet header", K(ret));
      } else {
        if (len >= (result.header_.compressed_len_ + MYSQL_COMPRESSED_HEALDER_LENGTH)) {
          result.status_ = ANALYZE_DONE;
          result.is_checksum_on_ = result.header_.is_compressed_payload();
          LOG_DEBUG("analyze one compressed packet succ", "data len", len, K(result));
        }
      }
    }
  }
  return ret;
}

int ObMysqlAnalyzerUtils::analyze_compressed_packet_header(
    const char *start, const int64_t len,
    ObMysqlCompressedPacketHeader &header)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(start) || len < MYSQL_COMPRESSED_HEALDER_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", KP(start), K(len), K(ret));
  } else {
    header.compressed_len_ = uint3korr(start);
    header.seq_ = static_cast<uint8_t>(start[3]);
    header.non_compressed_len_ = uint3korr(start + 4);
  }
  return ret;
}

int ObMysqlAnalyzerUtils::do_zlib_compress(
    ObIOBufferReader *src_reader,
    const int64_t src_data_len,
    ObMIOBuffer *des_buf,
    const bool is_checksum_on,
    const int64_t compression_level,
    int64_t &compressed_len,
    int64_t &uncompressed_len)
{
  int ret = OB_SUCCESS;
  compressed_len = 0;
  uncompressed_len = 0;
  if (OB_ISNULL(src_reader) || OB_ISNULL(des_buf) || src_data_len > src_reader->read_avail()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(src_reader), K(des_buf), K(src_data_len), K(ret));
  } else if (is_checksum_on) {
    ObZlibStreamCompressor compressor(compression_level);
    int64_t remain_len = src_data_len;
    char *start = NULL;
    int64_t buf_len = 0;
    int64_t block_read_avail = 0;
    bool is_last_data = false;
    int64_t block_compressed_len = 0;
    uncompressed_len = src_data_len;

    while (remain_len > 0 && OB_SUCC(ret)) {
      start = src_reader->start();
      block_read_avail = src_reader->block_read_avail();
      buf_len = (block_read_avail >= remain_len ? remain_len : block_read_avail);
      remain_len -= buf_len;
      is_last_data = (0 == remain_len);
      block_compressed_len = 0;

      if (OB_FAIL(stream_compress_data(compressor, des_buf, start, buf_len, is_last_data, block_compressed_len))) {
        LOG_WDIAG("fail to stream compress data", K(des_buf), K(start), K(buf_len),
                 K(is_last_data), K(ret));
      } else {
        compressed_len += block_compressed_len;
        // consume used data
        if (OB_FAIL(src_reader->consume(buf_len))) {
          LOG_WDIAG("fail to consume", K(buf_len), K(ret));
        }
      }
    }
  } else {
    //checksum off, just copy
    int64_t written_len = 0;
    /* 由于存在 COM_STMT_CLOSE/COM_STMT_RESET 这类命令, 不能直接移动 block, 需要把数据拷贝出来 */
    if (OB_FAIL(des_buf->write(src_reader, src_data_len, written_len))) {
      LOG_WDIAG("fail to write uncompress data", K(des_buf), K(src_data_len), K(ret));
    } else if (OB_UNLIKELY(written_len != src_data_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to write uncompress data", K(des_buf), K(written_len), K(src_data_len), K(ret));
    } else if (OB_FAIL(src_reader->consume(src_data_len))) {
      LOG_WDIAG("fail to consume", K(src_data_len), K(ret));
    } else {
      compressed_len = src_data_len;
      uncompressed_len = 0;
    }
  }
  return ret;
}

// compressed all data in reader into 1 compressed mysql packet
int ObMysqlAnalyzerUtils::consume_and_normal_compress_data(
    ObIOBufferReader *reader,
    ObMIOBuffer *write_buf,
    const int64_t data_len,
    ObCmpHeaderParam &param)
{
  int ret = OB_SUCCESS;
  uint8_t &compressed_seq = param.get_compressed_seq();
  const bool is_checksum_on = param.is_checksum_on();
  const int64_t compression_level = param.get_compression_level();
  char *mio_hdr_buf_start = NULL;
  int64_t total_compressed_len = 0;
  int64_t total_uncompressed_len = 0;

  ObProtocolDiagnosis *protocol_diagnosis = param.get_protocol_diagnosis();
  ObIOBufferReader *diagnosis_used_reader = (protocol_diagnosis == NULL ? NULL : reader->clone());
  
  if (OB_FAIL(reserve_compressed_hdr(write_buf, mio_hdr_buf_start))) {
    LOG_WDIAG("fail to reserve compressed hdr", K(ret));
  } else if (OB_FAIL(do_zlib_compress(reader, data_len, write_buf, is_checksum_on, compression_level,
                                      total_compressed_len, total_uncompressed_len))) {
    LOG_WDIAG("fail to do zlib compress", K(data_len), K(is_checksum_on), K(total_compressed_len), K(total_uncompressed_len), K(ret));
  } else if (OB_FAIL(fill_compressed_header(total_uncompressed_len, compressed_seq, total_compressed_len, mio_hdr_buf_start))) {
    LOG_WDIAG("fail to fill compressed hdr", K(ret));
  } else {
    if (OB_UNLIKELY(diagnosis_used_reader != NULL)) {
      PROTOCOL_DIAGNOSIS(COMPRESSED_MYSQL, send, protocol_diagnosis,
                         static_cast<int32_t>(total_compressed_len),
                         compressed_seq,
                         static_cast<int32_t>(total_uncompressed_len));
      PROTOCOL_DIAGNOSIS(MULTI_MYSQL, send, protocol_diagnosis,
                         *diagnosis_used_reader, data_len);
      diagnosis_used_reader->dealloc();
      diagnosis_used_reader = NULL;
    }
    LOG_DEBUG("build mysql compress packet succ", "origin len", data_len, K(total_compressed_len),
              K(total_uncompressed_len), K(compressed_seq), K(is_checksum_on));
  }

  return ret;
}

int ObMysqlAnalyzerUtils::stream_compress_data(
    ObZlibStreamCompressor &compressor,
    event::ObMIOBuffer *write_buf, const char *buf,
    const int64_t len, const bool is_last_data,
    int64_t &compressed_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(write_buf) || OB_ISNULL(buf) || len <= 0) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    compressed_len = 0;
    if (OB_FAIL(compressor.add_compress_data(buf, len, is_last_data))) {
      LOG_WDIAG("fail to add compress data", KP(buf), K(len), K(ret));
    } else {
      bool stop = false;
      char *write_buf_start = NULL;
      int64_t write_buf_len = 0;
      int64_t filled_len = 0;
      while (OB_SUCC(ret) && !stop) {
        if (OB_FAIL(write_buf->get_write_avail_buf(write_buf_start, write_buf_len))) {
          LOG_WDIAG("fail to get avail buf", K(ret));
        } else {
          if (OB_ISNULL(write_buf_start) || (write_buf_len <= 0)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("invalid argument", KP(write_buf_start), K(write_buf_len), K(ret));
          } else if (OB_FAIL(compressor.compress(write_buf_start, write_buf_len, filled_len))) {
            LOG_WDIAG("fail to compress", KP(write_buf_start), K(write_buf_len), K(filled_len), K(ret));
          } else {
            if (filled_len > 0) {
              write_buf->fill(filled_len);
              compressed_len += filled_len;
            }
            if (write_buf_len > filled_len) { // compress complete
              stop = true;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObMysqlAnalyzerUtils::reserve_compressed_hdr(event::ObMIOBuffer *write_buf, char *&hdr_start)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(write_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("write buf is NULL", K(ret));
  } else {
    char *mio_hdr_buf_start = NULL;
    if (OB_FAIL(write_buf->reserve_successive_buf(MYSQL_COMPRESSED_HEALDER_LENGTH))) {
      LOG_WDIAG("fail to reserve successive buf", K(write_buf), K(MYSQL_COMPRESSED_HEALDER_LENGTH), K(ret));
    } else if (write_buf->block_write_avail() < MYSQL_COMPRESSED_HEALDER_LENGTH) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("after reserve successive buf, must has enough space", K(MYSQL_COMPRESSED_HEALDER_LENGTH),
               "block_write_avail", write_buf->block_write_avail(), "current_write_avail",
               write_buf->current_write_avail(), K(ret));
    } else {
      mio_hdr_buf_start = write_buf->end();
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(mio_hdr_buf_start)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("buf is NULL", K(ret));
      // just fill and reserve
      } else if(OB_FAIL(write_buf->fill(MYSQL_COMPRESSED_HEALDER_LENGTH))) {
        LOG_WDIAG("fail to fill write buf", K(ret));
      } else {
        hdr_start = mio_hdr_buf_start;
      }
    }
  }
  return ret;
}

int ObMysqlAnalyzerUtils::fill_compressed_header(
    const int64_t uncompress_len, const uint8_t seq, const int64_t compressed_len,
    char *compressed_hdr_buf_start)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(compressed_hdr_buf_start) || uncompress_len < 0 || compressed_len < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", KP(compressed_hdr_buf_start), K(uncompress_len), K(compressed_len));
  } else {
    ObMysqlCompressedPacketHeader hdr;
    hdr.non_compressed_len_ = static_cast<uint32_t>(uncompress_len);
    hdr.seq_ = seq;
    hdr.compressed_len_ = static_cast<uint32_t>(compressed_len);
    int64_t hdr_pos = 0;
    int64_t hdr_len = MYSQL_COMPRESSED_HEALDER_LENGTH;
    char hdr_buf[MYSQL_COMPRESSED_HEALDER_LENGTH]; // no need memset
    if (OB_FAIL(ObMysqlCompressedPacket::encode_compressed_header(hdr_buf, hdr_len, hdr_pos, hdr))) {
      LOG_WDIAG("fail to encode compress header", K(ret));
    } else {
      MEMCPY(compressed_hdr_buf_start, hdr_buf, MYSQL_COMPRESSED_HEALDER_LENGTH);
    }
  }
  return ret;
}

int ObMysqlAnalyzerUtils::consume_and_fast_compress_data(
    ObIOBufferReader *reader,
    ObMIOBuffer *write_buf,
    const int64_t data_len,
    ObCmpHeaderParam &param)
{
  int ret = OB_SUCCESS;
  uint8_t &compressed_seq = param.get_compressed_seq();
  ObProtocolDiagnosis *protocol_diagnosis = param.get_protocol_diagnosis();

  if (OB_ISNULL(reader) || OB_ISNULL(write_buf) || data_len > reader->read_avail()) {
    ret = OB_INVALID_ARGUMENT;
    int64_t tmp_read_avail = ((NULL == reader) ? 0 : reader->read_avail());
    LOG_WDIAG("invalid input value", K(reader), K(write_buf), K(data_len), K(compressed_seq),
             "read_avail", tmp_read_avail, K(ret));
  } else {
    int64_t remain_len = data_len;
    int64_t compressed_len = 0;
    char *mio_hdr_buf_start = NULL;
    char *start = NULL;
    int64_t block_read_avail = 0;
    int64_t buf_len = 0;
    // for every block
    while (remain_len > 0 && OB_SUCC(ret)) {
      // 1. reserved compressed header
      mio_hdr_buf_start = NULL;
      if (OB_FAIL(reserve_compressed_hdr(write_buf, mio_hdr_buf_start))) {
        LOG_WDIAG("fail to reserve compressed hdr", K(ret));
      } else if (OB_ISNULL(mio_hdr_buf_start)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("buf is NULL", K(ret));
      }
      if (OB_SUCC(ret)) {
        // 2. add compressed data
        compressed_len = 0;
        start = reader->start();
        block_read_avail = reader->block_read_avail();
        buf_len = (block_read_avail >= remain_len ? remain_len : block_read_avail);
        remain_len -= buf_len;

        if (OB_FAIL(stream_compress_data(write_buf, start, buf_len, compressed_len))) {
          LOG_WDIAG("fail to stream compress data", K(write_buf), K(start), K(buf_len), K(ret));
        } else {
          PROTOCOL_DIAGNOSIS(COMPRESSED_MYSQL, send, protocol_diagnosis,
                             static_cast<int32_t>(compressed_len),
                             compressed_seq,
                             static_cast<int32_t>(buf_len));
          PROTOCOL_DIAGNOSIS(MULTI_MYSQL, send, protocol_diagnosis, *reader, buf_len);
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(reader->consume(buf_len))) { // consume used data
          LOG_WDIAG("fail to consume", K(buf_len), K(ret));
        // 3. fill the reserved compressed hdr
        } else if (OB_FAIL(fill_compressed_header(buf_len, compressed_seq, compressed_len, mio_hdr_buf_start))) {
          LOG_WDIAG("fail to fill compressed header", K(buf_len), K(compressed_seq), K(compressed_len), K(ret));
        } else {
          LOG_DEBUG("build mysql compress packet succ", "origin len", buf_len, K(data_len),
                    K(compressed_len), K(compressed_seq));
          ++compressed_seq;
        }
      }
    }

    if (OB_SUCC(ret)) {
      --compressed_seq;
    }
  }
  return ret;
}

int ObMysqlAnalyzerUtils::stream_compress_data(
    event::ObMIOBuffer *write_buf, const char *buf,
    const int64_t len, int64_t &compressed_len)
{
  ObFastZlibStreamCompressor compressor;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(write_buf) || OB_ISNULL(buf) || len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", KP(write_buf), KP(buf), K(len), K(ret));
  } else {
    compressed_len = 0;
    if (OB_FAIL(compressor.add_compress_data(buf, len))) {
      LOG_WDIAG("fail to add compress data", KP(buf), K(len), K(ret));
    } else {
      bool stop = false;
      char *write_buf_start = NULL;
      int64_t write_buf_len = 0;
      int64_t filled_len = 0;
      while (OB_SUCC(ret) && !stop) {
        if (OB_FAIL(write_buf->get_write_avail_buf(write_buf_start, write_buf_len))) {
          LOG_WDIAG("fail to get avail buf", K(ret));
        } else {
          if (OB_ISNULL(write_buf_start) || (write_buf_len <= 0)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("invalid argument", KP(write_buf_start), K(write_buf_len), K(ret));
          } else if (OB_FAIL(compressor.compress(write_buf_start, write_buf_len, filled_len))) {
            LOG_WDIAG("fail to compress", KP(write_buf_start), K(write_buf_len), K(filled_len), K(ret));
          } else {
            if (filled_len > 0) {
              write_buf->fill(filled_len);
              compressed_len += filled_len;
            }
            if (write_buf_len > filled_len) { // compress complete
              stop = true;
            }
          }
        }
      }
    }
  }
  return ret;
}

// we will compress one block's data as one compressed packet
int ObMysqlAnalyzerUtils::consume_and_compress_data(
    ObIOBufferReader *reader,
    ObMIOBuffer *write_buf,
    const int64_t data_len,
    ObCmpHeaderParam &param)
{
  int ret = OB_SUCCESS;
  if (0 == param.get_compression_level() && param.is_checksum_on()) {
    LOG_DEBUG("[obmysqlanalyzerutils::consume_and_compress_data] do fast compress", K(param));
    ret = consume_and_fast_compress_data(reader, write_buf, data_len, param);
  } else {
    LOG_DEBUG("[ObMysqlAnalyzerUtils::consume_and_compress_data] do normal compress", K(param));
    ret = consume_and_normal_compress_data(reader, write_buf, data_len, param);
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
