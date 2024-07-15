/**
 * Copyright (c) 2024 OceanBase
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
#include "proxy/mysqllib/ob_proxy_parser_utils.h"
#include "proxy/mysqllib/ob_oceanbase_20_pkt_analyzer.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/checksum/ob_crc16.h"


namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
using namespace oceanbase::obmysql;

int ObOceanBase20PktAnalyzer::decode_header(const char *buf, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("unexpected buf", KP(buf), K(len), K(ret));
  } else if (len < OB20_PROTOCOL_HEADER_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("unexpected len", K(len), K(ret));
  } else {
    char *start = const_cast<char*>(buf);
    obmysql::ObMySQLUtil::get_uint2(start, header_.magic_num_);
    obmysql::ObMySQLUtil::get_uint2(start, header_.version_);
    obmysql::ObMySQLUtil::get_uint4(start, header_.connection_id_);
    obmysql::ObMySQLUtil::get_uint3(start, header_.request_id_);
    obmysql::ObMySQLUtil::get_uint1(start, header_.pkt_seq_);
    obmysql::ObMySQLUtil::get_uint4(start, header_.payload_len_);
    obmysql::ObMySQLUtil::get_uint4(start, header_.flag_.flags_);
    obmysql::ObMySQLUtil::get_uint2(start, header_.reserved_);
    obmysql::ObMySQLUtil::get_uint2(start, header_.header_checksum_);
    LOG_DEBUG("decode oceanbase 2.0 header succ", K(header_));
  }
  return ret;
}

int ObOceanBase20PktAnalyzer::stream_analyze_header(
    const char *buf,
    const int64_t len,
    int64_t &analyzed,
    ObRespPktAnalyzerState &state)
{
  // check parameters in `stream_analyze_data`
  int ret = OB_SUCCESS;
  char *header_buf_start = NULL;
  if (OB_FAIL(ObRespAnalyzerUtil::stream_analyze_data(buf, len, analyzed, header_buf_, header_buf_start,
                                                      header_to_read_, OB20_PROTOCOL_HEADER_LENGTH))) {
    LOG_WDIAG("fail to stream_analyze_data", K(buf), K(len), K(analyzed),
                                             KP(header_buf_start), K(header_to_read_), K(ret));
  } else if(OB_LIKELY(header_to_read_ == 0)) {
    if (OB_LIKELY(header_buf_start != NULL)) {
      if (OB_FAIL(decode_header(header_buf_start, OB20_PROTOCOL_HEADER_LENGTH))) {
        LOG_WDIAG("fail to decode_header", K(ret));
      } else {
        if (header_.header_checksum_ != 0) {
          const char * header_buf = NULL;
          if (analyzed == OB20_PROTOCOL_HEADER_LENGTH) {
            header_buf = buf;
          } else {
            header_buf = header_buf_;
          }
          local_header_checksum_ = ob_crc16(local_header_checksum_, 
                                            reinterpret_cast<const uint8_t *>(header_buf),
                                            OB20_PROTOCOL_HEADER_LENGTH - 2);
          if (local_header_checksum_ != header_.header_checksum_) {
            ret = OB_CHECKSUM_ERROR;
            LOG_EDIAG("fail to validate oceanbase 2.0 header checksum", K(*this));
          } else {
            LOG_DEBUG("succ to validate oceanbase 2.0 header checksum", K(local_header_checksum_));
          }
        }
        if (OB_SUCC(ret)) {
          if (header_.flag_.is_extra_info_exist()) {
            if (extra_info_to_read_ == 0) {
              state = STREAM_OCEANBASE20_EXTRA_INFO_HEADER;
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WDIAG("unexpected extra_info_to_read_", K(extra_info_to_read_), K(state));
            }
          } else {
            LOG_DEBUG("extra info not exists");
            state = STREAM_OCEANBASE20_MYSQL_PAYLOAD;
          }
          stream_analyze_header_done();
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("header_buf_start is NULL", KP(header_buf_start), K(ret));
    }
  }

  return ret;
}

int ObOceanBase20PktAnalyzer::stream_analyze_extra_info_header(
    const char *buf,
    const int64_t len,
    int64_t &analyzed,
    ObRespPktAnalyzerState &state)
{
  // check parameters in `stream_analyze_data`
  int ret = OB_SUCCESS;
  analyzed = 0;
  char *header_buf_start = NULL;
  if (OB_FAIL(ObRespAnalyzerUtil::stream_analyze_data(buf, len, analyzed, tmp_buf_, header_buf_start,
                                                      extra_info_hdr_to_read_, OB20_PROTOCOL_EXTRA_INFO_LENGTH))) {
      LOG_WDIAG("fail to stream_analyze_data", K(buf), K(len), K(analyzed),
                                               KP(header_buf_start), K(extra_info_hdr_to_read_), K(ret));
  } else {
    if (enable_transmission_checksum_) {
      local_payload_checksum_ = ob_crc64(local_payload_checksum_, buf, analyzed);
    }
    if (OB_LIKELY(extra_info_hdr_to_read_ == 0)) { 
      if (OB_LIKELY(header_buf_start != NULL)) {
        ObMySQLUtil::get_uint4(header_buf_start, extra_info_len_);
        state = STREAM_OCEANBASE20_EXTRA_INFO;
        LOG_DEBUG("got extra info", "len", extra_info_len_);
        stream_analyze_extra_info_header_done();
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("header_buf_start is NULL", KP(header_buf_start), K(ret));
      }
    }
  }

  return ret;
}

int ObOceanBase20PktAnalyzer::stream_analyze_extra_info(
    const char *buf,
    const int64_t len,
    int64_t &analyzed,
    ObRespPktAnalyzerState &state)
{
  // check parameters in `stream_read_data`
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRespAnalyzerUtil::stream_read_data(buf, len, analyzed, extra_info_to_read_))){
    LOG_WDIAG("fail to stream_analyze_data", KP(buf), K(len), K(analyzed), K(extra_info_to_read_), K(ret));
  } else {
    if (enable_transmission_checksum_) {
      local_payload_checksum_ = ob_crc64(local_payload_checksum_, buf, analyzed);
    }
    mysql_payload_to_read_ -= analyzed;
    if (mysql_payload_to_read_ == 0) {
      state = STREAM_OCEANBASE20_TAILER; // extra info acrosses two oceanbase 2.0
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("not support extra info acrosses two oceanbase 2.0 packets", K(*this), K(ret));
    } else if (extra_info_to_read_ == 0) {
      state = STREAM_OCEANBASE20_MYSQL_PAYLOAD;
    }
  }
  return ret;
}

int ObOceanBase20PktAnalyzer::stream_analyze_mysql_payload(
    const char *buf,
    const int64_t len,
    int64_t &analyzed,
    ObRespPktAnalyzerState &state)
{
  // check parameters in `stream_read_data`
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRespAnalyzerUtil::stream_read_data(buf, len, analyzed, mysql_payload_to_read_))){
    LOG_WDIAG("fail to stream_analyze_data", KP(buf), K(len), K(analyzed), K(mysql_payload_to_read_), K(ret));
  } else {
    if (enable_transmission_checksum_) {
      local_payload_checksum_ = ob_crc64(local_payload_checksum_, buf, analyzed);
    }
    if (mysql_payload_to_read_ == 0) {
      state = STREAM_OCEANBASE20_TAILER;
    }
  }
  return ret;
}

int ObOceanBase20PktAnalyzer::stream_analyze_tailer(
    const char *buf,
    const int64_t len,
    int64_t &analyzed,
    ObRespPktAnalyzerState &state)
{
  // check parameters in `stream_analyze_data`
  int ret = OB_SUCCESS;
  char *tailer_buf_start = NULL;
  if (OB_FAIL(ObRespAnalyzerUtil::stream_analyze_data(buf, len, analyzed, tmp_buf_, tailer_buf_start,
                                                      tailer_to_read_, OB20_PROTOCOL_TAILER_LENGTH))){
    LOG_WDIAG("fail to stream_analyze_data", KP(buf), K(len), KP(tailer_buf_start), K(analyzed), K(tailer_to_read_), K(ret));
  } else if (OB_LIKELY(tailer_to_read_ == 0)) {
    if (OB_LIKELY(tailer_buf_start != NULL)) {
      uint32_t payload_checksum = 0;
      ObMySQLUtil::get_uint4(tailer_buf_start, payload_checksum);
      if (enable_transmission_checksum_) {
        if (payload_checksum == 0) {
          LOG_DEBUG("observer not calc payload checksum");
        } else if (OB_UNLIKELY(local_payload_checksum_ != payload_checksum))  {
          ret = OB_CHECKSUM_ERROR;
          LOG_EDIAG("payload checksum dismatch", K_(local_payload_checksum), K(payload_checksum), K(ret));
        }
      } else {
        LOG_DEBUG("skip payload checksum", K_(local_payload_checksum), K(enable_transmission_checksum_));
      }
      state = STREAM_OCEANBASE20_END;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("tailer_buf_start is NULL", KP(tailer_buf_start));
    }
  }
  return ret;
}

int64_t ObOceanBase20PktAnalyzer::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K(header_), K(header_to_read_), K(local_payload_checksum_));
  J_COMMA();
  BUF_PRINTF("header_buf");
  J_COLON();
  J_ARRAY_START();
  BUF_PRINTF("%02X", (unsigned char) header_buf_[0]);
  for (int i = 1; i < OB20_PROTOCOL_HEADER_LENGTH; i++) {
    BUF_PRINTF(" %02X", (unsigned char) header_buf_[i]);
  }
  J_ARRAY_END();
  J_COMMA();
  J_KV(K(extra_info_hdr_to_read_), K(extra_info_len_), K(extra_info_to_read_));
  J_COMMA();
  BUF_PRINTF("tmp_buf");
  J_COLON();
  J_ARRAY_START();
  BUF_PRINTF("%02X", (unsigned char) header_buf_[0]);
  for (int i = 1; i < OB20_PROTOCOL_HEADER_LENGTH; i++) {
    BUF_PRINTF(" %02X", (unsigned char) header_buf_[i]);
  }
  J_ARRAY_END();
  J_COMMA();
  J_KV(K(tailer_to_read_));
  J_OBJ_END();

  return pos;
}

void ObOceanBase20PktAnalyzer::reset()
{
  local_payload_checksum_ = 0;
  mysql_payload_to_read_ = 0;
  extra_info_hdr_to_read_ = OB20_PROTOCOL_EXTRA_INFO_LENGTH;
  extra_info_len_ = 0;
  extra_info_to_read_ = 0;
  header_.reset();
  header_to_read_ = OB20_PROTOCOL_HEADER_LENGTH;
  tailer_to_read_ = OB20_PROTOCOL_TAILER_LENGTH;
}

} // end of namespace proxy
} // end of naemspace obproxy
} // end of namespace oceanbase
