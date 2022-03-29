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

#ifndef OBPROXY_MYSQL_COMPRESS_ANALYZER_H
#define OBPROXY_MYSQL_COMPRESS_ANALYZER_H
#include "proxy/mysqllib/ob_mysql_common_define.h"
#include "proxy/mysqllib/ob_mysql_response.h"
#include "proxy/mysqllib/ob_i_mysql_respone_analyzer.h"
#include "proxy/mysqllib/ob_proxy_parser_utils.h"
#include "utils/ob_zlib_stream_compressor.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{
class ObMIOBuffer;
}
namespace proxy
{

class ObMysqlCompressAnalyzer : public ObIMysqlRespAnalyzer
{
public:
  enum AnalyzeMode
  {
    SIMPLE_MODE = 0, // only judge whether the stream completed, do not decompress any data
    DECOMPRESS_MODE, // decompress all the data received
  };

  ObMysqlCompressAnalyzer()
    : is_inited_(false), last_seq_(0), mysql_cmd_(obmysql::OB_MYSQL_COM_MAX_NUM),
      enable_extra_ok_packet_for_stats_(false), is_last_packet_(false),
      is_stream_finished_(false), mode_(SIMPLE_MODE), remain_len_(0), header_buf_(),
      header_valid_len_(0), curr_compressed_header_(), out_buffer_(NULL), last_packet_buffer_(NULL),
      last_packet_len_(0), last_packet_filled_len_(0), compressor_()
      {}
  virtual ~ObMysqlCompressAnalyzer() { reset(); }

  virtual int init(const uint8_t last_seq, const AnalyzeMode mode,
                   const obmysql::ObMySQLCmd mysql_cmd,
                   const ObMysqlProtocolMode mysql_mode,
                   const bool enable_extra_ok_packet_for_stats,
                   const uint8_t last_ob20_seq,
                   const uint32_t request_id,
                   const uint32_t sessid);

  virtual void reset();
  bool is_inited() const { return is_inited_; }

  bool is_stream_finished() const { return is_stream_finished_; }
  virtual bool is_compressed_payload() const { return curr_compressed_header_.is_compressed_payload(); }

  int analyze_compressed_response(const common::ObString &compressed_data,
                                  ObMysqlResp &resp);
  int analyze_compressed_response(event::ObIOBufferReader &reader, ObMysqlResp &resp);
  int analyze_compressed_response_with_length(event::ObIOBufferReader &reader, uint64_t decompress_size);

  virtual int analyze_response(event::ObIOBufferReader &reader, ObMysqlResp *resp = NULL);
  event::ObMIOBuffer *get_transfer_miobuf() { return out_buffer_; }
  event::ObIOBufferReader *get_transfer_reader() { return ((NULL == out_buffer_) ? NULL : out_buffer_->alloc_reader()); }

  virtual int analyze_first_response(event::ObIOBufferReader &reader,
                                     const bool need_receive_completed,
                                     ObMysqlCompressedAnalyzeResult &result,
                                     ObMysqlResp &resp);
  int analyze_first_response(event::ObIOBufferReader &reader,
                             ObMysqlCompressedAnalyzeResult &result);

  common::ObString get_last_packet_string();


protected:
  // attention!! the buf can not cross two compressed packets
  virtual int decompress_data(const char *zprt, const int64_t zlen, ObMysqlResp &resp);
  virtual int decode_compressed_header(const common::ObString &compressed_data, int64_t &avail_len);
  virtual int analyze_last_compress_packet(const char *start, const int64_t len,
                                   const bool is_last_data, ObMysqlResp &resp);
  virtual int analyze_one_compressed_packet(event::ObIOBufferReader &reader,
                                            ObMysqlCompressedAnalyzeResult &result);
  virtual bool is_last_packet(const ObMysqlCompressedAnalyzeResult &result);

private:
  int do_analyze_last_compress_packet(ObMysqlResp &resp);
  int decompress_last_mysql_packet(const char *compressed_data, const int64_t len);
  int analyze_last_ok_packet(const int64_t last_pkt_total_len, ObMysqlResp &resp);

  int analyze_error_packet(const char *ptr, const int64_t len, ObMysqlResp &resp);
  int analyze_ok_packet(const char *ptr, const int64_t len, ObMysqlResp &resp);
  int analyze_string_eof_packet(ObMysqlResp &resp);

  void reset_out_buffer();

protected:
  bool is_inited_;
  // last seq num
  // if curr seq == last seq, it means this is the last compressed packet,
  // which contain ok or err + ok or eof + ok
  uint8_t last_seq_;
  obmysql::ObMySQLCmd mysql_cmd_;
  bool enable_extra_ok_packet_for_stats_;
  bool is_last_packet_; // whether current analyzed packet is the last one
  bool is_stream_finished_;
  AnalyzeMode mode_;
  int64_t remain_len_;
  char header_buf_[MYSQL_COMPRESSED_HEALDER_LENGTH];
  int64_t header_valid_len_;
  ObMysqlCompressedPacketHeader curr_compressed_header_;
  // if not null, need decompress
  // Attention!, this mio_buffer_ do not contain the last control packet
  event::ObMIOBuffer *out_buffer_;
  char *last_packet_buffer_; // for decompress control packet
  int64_t last_packet_len_;
  int64_t last_packet_filled_len_;
  ObZlibStreamCompressor compressor_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMysqlCompressAnalyzer);
};

inline common::ObString ObMysqlCompressAnalyzer::get_last_packet_string()
{
  // the last char not actual data, only use to judge decompress complete easily
  common::ObString last_str(last_packet_len_ - 1, last_packet_buffer_);
  return last_str;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OBPROXY_MYSQL_COMPRESS_ANALYZER_H */
