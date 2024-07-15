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

#ifndef OBPROXY_OCEANBASE20_PKT_ANALYZER
#define OBPROXY_OCEANBASE20_PKT_ANALYZER

#include "proxy/mysqllib/ob_resp_analyzer_util.h"
namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObOceanBase20PktHeader
{
public:
  ObOceanBase20PktHeader()
    : magic_num_(0), header_checksum_(0),
    connection_id_(0), request_id_(0), pkt_seq_(0), payload_len_(0),
    flag_(0), version_(0), reserved_(0) {}
  ~ObOceanBase20PktHeader() {}
  void reset()
  {
    MEMSET(this, 0, sizeof(ObOceanBase20PktHeader));
  }
  TO_STRING_KV(K_(magic_num),
               K_(header_checksum),
               K_(connection_id),
               K_(request_id),
               K_(pkt_seq),
               K_(payload_len),
               K_(version),
               K_(flag_.flags),
               K_(reserved));
public:
  uint16_t magic_num_;
  uint16_t header_checksum_;
  uint32_t connection_id_;
  uint32_t request_id_;
  uint8_t pkt_seq_;
  uint32_t payload_len_;
  Ob20ProtocolFlags flag_;
  uint16_t version_;
  uint16_t reserved_;
};
class ObOceanBase20PktAnalyzer
{
public: // streaming
  ObOceanBase20PktAnalyzer()
    : enable_transmission_checksum_(true),
      local_payload_checksum_(0), mysql_payload_to_read_(0), 
      extra_info_hdr_to_read_(OB20_PROTOCOL_EXTRA_INFO_LENGTH),
      extra_info_len_(0), extra_info_to_read_(0),
      header_to_read_(OB20_PROTOCOL_HEADER_LENGTH),
      tailer_to_read_(OB20_PROTOCOL_TAILER_LENGTH) {}
  int stream_analyze_header(const char *buf, const int64_t len, int64_t &analyzed, ObRespPktAnalyzerState &state);
  int stream_analyze_extra_info_header(const char *buf, const int64_t len, int64_t &analyzed, ObRespPktAnalyzerState &state);
  int stream_analyze_extra_info(const char *buf, const int64_t len, int64_t &analyzed, ObRespPktAnalyzerState &state);
  int stream_analyze_mysql_payload(const char *buf, const int64_t len, int64_t &analyzed, ObRespPktAnalyzerState &state);
  int stream_analyze_tailer(const char *buf, const int64_t len, int64_t &analyzed, ObRespPktAnalyzerState &state);
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();

public: // getter and setter
  inline void set_local_header_checksum(int64_t checksum) { local_header_checksum_ = checksum; }
  inline int64_t get_local_header_checksum() { return local_header_checksum_; }
  inline int64_t get_local_payload_checksum() { return local_payload_checksum_; }
  inline int64_t get_header_to_read() { return header_to_read_; }
  inline const ObOceanBase20PktHeader get_header() { return header_; }
  inline const char *get_header_buf() { return header_buf_; }
  inline const uint32_t get_mysql_payload_to_read() { return mysql_payload_to_read_; }
  inline const uint32_t get_extra_info_len() { return extra_info_len_; }
  inline const uint32_t get_extra_info_to_read()  { return extra_info_to_read_; }
  inline const uint32_t get_tailer_to_read()  { return tailer_to_read_; }
  inline void set_enable_transmission_checksum(bool enable) { enable_transmission_checksum_ = enable; }
private:
  int decode_header(const char *buf, const int64_t len);
  inline void stream_analyze_header_done() {
    mysql_payload_to_read_ = header_.payload_len_;
    local_header_checksum_ = 0;
  }
  inline void stream_analyze_extra_info_header_done() 
  {
    extra_info_to_read_ = extra_info_len_;
    mysql_payload_to_read_ -= OB20_PROTOCOL_EXTRA_INFO_LENGTH;
  }
private:
  bool enable_transmission_checksum_;
  union {
    int64_t local_payload_checksum_;
    int64_t local_header_checksum_;
  };
  // mysql payload
  uint32_t mysql_payload_to_read_;
  // extra info
  char tmp_buf_[OB20_PROTOCOL_EXTRA_INFO_LENGTH];
  uint32_t extra_info_hdr_to_read_;
  uint32_t extra_info_len_;
  uint32_t extra_info_to_read_;

  // header
  ObOceanBase20PktHeader header_;
  char header_buf_[OB20_PROTOCOL_HEADER_LENGTH];
  uint32_t header_to_read_;
  // tailer
  uint32_t tailer_to_read_;

};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif