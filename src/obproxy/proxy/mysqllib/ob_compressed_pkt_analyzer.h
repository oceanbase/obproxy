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

#ifndef OBPROXY_COMPRESSED_MYSQL_PKT_ANALYZER
#define OBPROXY_COMPRESSED_MYSQL_PKT_ANALYZER

#include "obproxy/iocore/eventsystem/ob_io_buffer.h"
#include "proxy/mysqllib/ob_proxy_parser_utils.h"
#include "utils/ob_zlib_stream_compressor.h"
#include "proxy/mysqllib/ob_resp_analyzer_util.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObCompressedPktAnalyzer
{
public:
  ObCompressedPktAnalyzer()
    : payload_to_read_(0),
      header_to_read_(MYSQL_COMPRESSED_HEALDER_LENGTH) {}
  int stream_analyze_header(const char *buf, const int64_t len, int64_t &analyze_len, ObRespPktAnalyzerState &state);
  inline void stream_analyze_header_done() { payload_to_read_ = header_.compressed_len_; }
  int stream_analyze_payload(const char *buf, const int64_t len, int64_t &analyze_len, ObRespPktAnalyzerState &state);
  void reset();
  int64_t to_string(char *buf, const int64_t buf_len) const;
public: // getter
  inline const char *get_header_buf() { return header_buf_; }
  inline ObMysqlCompressedPacketHeader get_header() { return header_; }
  inline bool is_compressed_payload() { return header_.non_compressed_len_ != 0; }
  inline uint32_t get_payload_to_read() { return payload_to_read_; }
  inline uint32_t get_header_to_read() { return header_to_read_; }
private:
  int decode_header(const char *buf, const int64_t len);
private:
  ObMysqlCompressedPacketHeader header_;
  uint32_t payload_to_read_;
  uint32_t header_to_read_;
  char header_buf_[MYSQL_COMPRESSED_HEALDER_LENGTH];
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif