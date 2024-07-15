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
#include "proxy/mysqllib/ob_protocol_diagnosis.h"
#include "proxy/mysqllib/ob_proxy_parser_utils.h"
#include "proxy/mysqllib/ob_compressed_pkt_analyzer.h"
#include "proxy/mysqllib/ob_mysql_common_define.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

int ObCompressedPktAnalyzer::decode_header(const char *buf, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (len < MYSQL_COMPRESSED_HEALDER_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("fail to decode_header", K(len), K(ret));
  } else {
    obmysql::ObMySQLUtil::get_uint3(buf, header_.compressed_len_);
    obmysql::ObMySQLUtil::get_uint1(buf, header_.seq_);
    obmysql::ObMySQLUtil::get_uint3(buf, header_.non_compressed_len_);
    LOG_DEBUG("decode compressed mysql header succ", K(header_));
  }
  return ret;
}

int ObCompressedPktAnalyzer::stream_analyze_payload(
    const char *buf,
    const int64_t len,
    int64_t &analyzed,
    ObRespPktAnalyzerState &state)
{
  // check parameters in `stream_read_data`
  int ret = OB_SUCCESS;
  analyzed = 0;
  if (OB_FAIL(ObRespAnalyzerUtil::stream_read_data(buf, len, analyzed, payload_to_read_))) {
    LOG_WDIAG("fail to stream_analyze_data", K(buf), K(len), K(analyzed),
                                             KP(header_buf_), K(header_to_read_), K(ret));
  } else if (payload_to_read_ == 0) {
    state = STREAM_COMPRESSED_END;
  }

  return ret;
}

int ObCompressedPktAnalyzer::stream_analyze_header(
    const char *buf,
    const int64_t len,
    int64_t &analyzed,
    ObRespPktAnalyzerState &state)
{
  // check parameters in `stream_analyze_data`
  int ret = OB_SUCCESS;
  analyzed = 0;
  char *header_buf_start = NULL;
  if (OB_FAIL(ObRespAnalyzerUtil::stream_analyze_data(buf, len, analyzed, header_buf_, header_buf_start,
                                                      header_to_read_, MYSQL_COMPRESSED_HEALDER_LENGTH))) {
    LOG_WDIAG("fail to stream_analyze_data", K(buf), K(len), K(analyzed), KP(header_buf_),
                                             KP(header_buf_start), K(header_to_read_), K(ret));
  } else if (header_to_read_ == 0) {
    if (OB_LIKELY(header_buf_start != NULL)) {
      if (OB_FAIL(decode_header(header_buf_start, MYSQL_COMPRESSED_HEALDER_LENGTH))) {
        LOG_WDIAG("fail to analyze compressed packet header", K(ret));
      } else {
        stream_analyze_header_done();
        state = STREAM_COMPRESSED_MYSQL_PAYLOAD;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("header_buf_start is NULL", KP(header_buf_start), K(ret));
    }
  }

  return ret;
}

void ObCompressedPktAnalyzer::reset()
{
  header_.reset();
  header_to_read_ = MYSQL_COMPRESSED_HEALDER_LENGTH;
  payload_to_read_ = 0;
}

int64_t ObCompressedPktAnalyzer::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K(header_), K(header_to_read_));
  J_COMMA();
  BUF_PRINTF("header_buf");
  J_COLON();
  J_ARRAY_START();
  BUF_PRINTF("%02X", (unsigned) header_buf_[0]);
  for (int i = 1; i < MYSQL_COMPRESSED_HEALDER_LENGTH; i++) {
    BUF_PRINTF(" %02X", (unsigned) header_buf_[i]);
  }
  J_ARRAY_END();
  J_OBJ_END();
  return pos;
}

} // end of namespace proxy
} // end of naemspace obproxy
} // end of namespace oceanbase