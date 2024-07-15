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
#include "proxy/mysqllib/ob_mysql_pkt_analyzer.h"
#include "proxy/mysqllib/ob_resp_analyzer.h"


namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

int ObMysqlPktAnalyzer::stream_analyze_header(
    const char *buf,
    const int64_t buf_len,
    int64_t &analyzed,
    ObRespPktAnalyzerState &state)
{
  // check parameters in `stream_analyze_data`
  int ret = OB_SUCCESS;
  char *header_buf_start = NULL;
  if (OB_FAIL(ObRespAnalyzerUtil::stream_analyze_data(buf, buf_len, analyzed, header_buf_, header_buf_start,
                                                      header_to_read_, MYSQL_NET_HEADER_LENGTH))) {
    LOG_WDIAG("fail to stream_analyze_data", K(buf), K(buf_len), K(analyzed), K(header_to_read_), K(ret));
  } else {
    if (OB_LIKELY(header_to_read_ == 0)) {
      if (OB_LIKELY(header_buf_start != NULL)) {
        obmysql::ObMySQLUtil::get_uint3(header_buf_start, pkt_len_);
        obmysql::ObMySQLUtil::get_uint1(header_buf_start, pkt_seq_);
        if (pkt_len_ == 0) {
          state = STREAM_MYSQL_END; // current empty packet, next
        } else {
          state = STREAM_MYSQL_TYPE;
        }
        LOG_DEBUG("analyze mysql header", K(*this));
        stream_analyze_header_done();
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("header_buf_start is NULL", KP(header_buf_start), K(ret));
      }
    }
  }
  return ret;
}

int ObMysqlPktAnalyzer::stream_analyze_type(
    const char *buf,
    const int64_t buf_len,
    int64_t &analyzed,
    ObRespPktAnalyzerState &state)
{
  int ret = OB_SUCCESS;
  analyzed = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected null ptr or buf_len", KP(buf), K(buf_len), K(ret));
  } else {
    pkt_type_ = static_cast<uint8_t>(*buf);
    analyzed = 1;
    body_to_read_ -= analyzed;
    if (body_to_read_ == 0) {
      state = STREAM_MYSQL_END;
    } else {
      state = STREAM_MYSQL_BODY;
    }
  }

  return ret;
}

int ObMysqlPktAnalyzer::stream_analyze_body(
    const char *buf,
    const int64_t buf_len,
    int64_t &analyzed,
    ObRespPktAnalyzerState &state)
{
  // check parameters in `stream_read_data`
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObRespAnalyzerUtil::stream_read_data(buf, buf_len, analyzed, body_to_read_))) {
    LOG_WDIAG("fail to stream_analyze_data", K(buf), K(buf_len), K(analyzed), K(body_to_read_), K(ret));
  } else {
    if (body_to_read_ == 0) {
      state = STREAM_MYSQL_END;
    }
  }

  return ret;
}

int64_t ObMysqlPktAnalyzer::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K(pkt_len_), K(pkt_seq_), K(pkt_type_), K(header_to_read_), K(body_to_read_));
  J_COMMA();
  BUF_PRINTF("header_buf");
  J_COLON();
  J_ARRAY_START();
  BUF_PRINTF("%02X", header_buf_[0]);
  for (int i = 1; i < MYSQL_NET_HEADER_LENGTH; i++) {
    BUF_PRINTF(" %02X", header_buf_[i]);
  }
  J_ARRAY_END();
  J_OBJ_END();
  return pos;
}

void ObMysqlPktAnalyzer::reset()
{
  pkt_len_ = 0;
  pkt_seq_ = 0;
  data_ = 0;
  body_to_read_ = 0;
  header_to_read_ = MYSQL_NET_HEADER_LENGTH;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase