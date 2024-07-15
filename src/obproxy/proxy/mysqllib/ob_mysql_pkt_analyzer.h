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
#ifndef OBPROXY_MYSQL_PKT_ANALYZER
#define OBPROXY_MYSQL_PKT_ANALYZER
#include "obproxy/iocore/eventsystem/ob_io_buffer.h"
#include "proxy/mysqllib/ob_proxy_parser_utils.h"
#include "proxy/mysqllib/ob_resp_analyzer_util.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObMysqlPktAnalyzer
{
public:
  ObMysqlPktAnalyzer() : pkt_len_(0), pkt_seq_(0), data_(0), body_to_read_(0), header_to_read_(MYSQL_NET_HEADER_LENGTH) {}
  int stream_analyze_header(const char *buf, const int64_t buf_len, int64_t &analyzed, ObRespPktAnalyzerState &state);
  int stream_analyze_type(const char *buf, const int64_t buf_len, int64_t &analyzed, ObRespPktAnalyzerState &state);
  int stream_analyze_body(const char *buf, const int64_t buf_len, int64_t &analyzed, ObRespPktAnalyzerState &state);
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();
public: // getter
  inline uint32_t get_body_to_read() { return body_to_read_; }
  inline uint32_t get_header_to_read() { return header_to_read_; }
  inline uint32_t get_pkt_len() { return pkt_len_; }
  inline uint8_t get_pkt_seq() { return pkt_seq_; }
  inline uint8_t get_pkt_type() { return pkt_type_; }

private:
  inline void stream_analyze_header_done() { body_to_read_ = pkt_len_; }


private:
  uint32_t pkt_len_;
  uint8_t pkt_seq_;
  union
  {
    obmysql::ObMySQLCmd cmd_; // mysql cmd for mysql request
    uint8_t pkt_type_; // packet type for mysql response
    uint8_t data_;  // for data
  };
  // stream read body
  uint32_t body_to_read_;
  // stream read header
  uint32_t header_to_read_;
  char header_buf_[MYSQL_NET_HEADER_LENGTH];
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif