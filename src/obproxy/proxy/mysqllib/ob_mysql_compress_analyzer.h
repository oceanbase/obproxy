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
#include "proxy/mysqllib/ob_i_mysql_response_analyzer.h"
#include "proxy/mysqllib/ob_proxy_parser_utils.h"
#include "utils/ob_zlib_stream_compressor.h"
#include "obproxy/iocore/eventsystem/ob_io_buffer.h"

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
class ObProxyMysqlRequest;
enum ObCompressionAlgorithm {
  OB_COMPRESSION_ALGORITHM_NONE,
  OB_COMPRESSION_ALGORITHM_ZLIB,
  OB_COMPRESSION_ALGORITHM_ZSTD
};
const char* get_compression_algorithm_name(ObCompressionAlgorithm algro) {
  const char *ret = "";
  switch (algro) {
    case OB_COMPRESSION_ALGORITHM_NONE:
      ret = "none";
      break;

    case OB_COMPRESSION_ALGORITHM_ZLIB:
      ret = "zlib";
      break;

    case OB_COMPRESSION_ALGORITHM_ZSTD:
      ret = "zstd";
      break;

    default:
      break;
  }
  return ret;
}
const ObCompressionAlgorithm get_compression_algorithm_by_name(const common::ObString &algro_name) {
  ObCompressionAlgorithm ret = OB_COMPRESSION_ALGORITHM_NONE;
  if (0 == algro_name.case_compare(get_compression_algorithm_name(OB_COMPRESSION_ALGORITHM_ZLIB))) {
    ret = OB_COMPRESSION_ALGORITHM_ZLIB;
  } else if (0 == algro_name.case_compare(get_compression_algorithm_name(OB_COMPRESSION_ALGORITHM_ZSTD))) {
    ret = OB_COMPRESSION_ALGORITHM_ZSTD;
  } else {
    /* do nothing */
  }
  return ret;
}
const int64_t get_min_compression_level(ObCompressionAlgorithm algro) {
  int64_t ret = 0;

  switch (algro) {
    case OB_COMPRESSION_ALGORITHM_NONE:
    case OB_COMPRESSION_ALGORITHM_ZLIB:
    case OB_COMPRESSION_ALGORITHM_ZSTD:
      ret = 0;
    break;

    default:
      ret = 0;
      break;
  }

  return ret;
}
const int64_t get_max_compression_level(ObCompressionAlgorithm algro) {
  int64_t ret = 0;

  switch (algro) {
    case OB_COMPRESSION_ALGORITHM_ZLIB:
      ret = 9;
      break;

    case OB_COMPRESSION_ALGORITHM_NONE:
    case OB_COMPRESSION_ALGORITHM_ZSTD:
      ret = 0;
    break;

    default:
      ret = 0;
      break;
  }

  return ret;
}


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
      is_stream_finished_(false), mode_(SIMPLE_MODE), last_compressed_pkt_remain_len_(0), header_buf_(),
      header_valid_len_(0), curr_compressed_header_(), mysql_decompress_buffer_(NULL), ob20_decompress_buffer_(NULL), mysql_decompress_buffer_reader_(NULL),
      compressor_(), is_analyze_compressed_ob20_(false),
      result_()
      {}
  virtual ~ObMysqlCompressAnalyzer();

  virtual int init(const uint8_t last_seq, const AnalyzeMode mode,
                   const obmysql::ObMySQLCmd mysql_cmd,
                   const ObMysqlProtocolMode mysql_mode,
                   const bool enable_extra_ok_packet_for_stats,
                   const uint8_t last_ob20_seq,
                   const uint32_t request_id,
                   const uint32_t sessid,
                   const bool is_analyze_compressed_ob20);

  virtual void reset();
  bool is_inited() const { return is_inited_; }

  bool is_stream_finished() const { return is_stream_finished_; }
  virtual bool is_compressed_payload() const { return curr_compressed_header_.is_compressed_payload(); }
  virtual int analyze_compressed_response(const common::ObString &compressed_data, ObMysqlResp &resp);
  virtual int analyze_compressed_response(event::ObIOBufferReader &reader, ObMysqlResp &resp);
  int analyze_compressed_response_with_length(event::ObIOBufferReader &reader, uint64_t decompress_size);
  virtual int analyze_response(event::ObIOBufferReader &reader, ObMysqlResp *resp = NULL);
  event::ObMIOBuffer *get_transfer_miobuf() { return mysql_decompress_buffer_; }
  event::ObIOBufferReader *get_transfer_reader() { return ((NULL == mysql_decompress_buffer_) ? NULL : mysql_decompress_buffer_->alloc_reader()); }
  event::ObMIOBuffer *get_ob20_decompressed_miobuf() { return ob20_decompress_buffer_; }
  event::ObIOBufferReader *get_ob20_decompressed_reader() { return ((NULL == ob20_decompress_buffer_) ? NULL : ob20_decompress_buffer_->alloc_reader()); }
  virtual int analyze_first_response(event::ObIOBufferReader &reader,
                                     const bool need_receive_completed,
                                     ObMysqlCompressedAnalyzeResult &result,
                                     ObMysqlResp &resp);
  virtual int analyze_first_response(event::ObIOBufferReader &reader,
                                     ObMysqlCompressedAnalyzeResult &result);

  int analyze_first_request(event::ObIOBufferReader &reader,
                            ObMysqlCompressedAnalyzeResult &result,
                            ObProxyMysqlRequest &req,
                            ObMysqlAnalyzeStatus &status);
  virtual int analyze_compress_packet_payload(event::ObIOBufferReader &reader,
                                              ObMysqlCompressedAnalyzeResult &result);

  int analyze_mysql_packet_end(ObMysqlResp &resp);

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
  int alloc_tmp_used_buffer();
  void delloc_tmp_used_buffer();

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
  int64_t last_compressed_pkt_remain_len_;
  char header_buf_[MYSQL_COMPRESSED_HEALDER_LENGTH];
  int64_t header_valid_len_;
  ObMysqlCompressedPacketHeader curr_compressed_header_;
  // if not null, need decompress
  // Attention!, this mio_buffer_ do not contain the last control packet
  event::ObMIOBuffer *mysql_decompress_buffer_;
  // tmp buffer for decompress ob20
  event::ObMIOBuffer *ob20_decompress_buffer_;
  // reader of mysql_decompress_buffer_ when analyzed N bytes mysql packets from mysql_decompress_buffer_
  // then need to consume N bytes from mysql_decompress_buffer_reader_
  // otherwise, it will cause a memory leak.
  event::ObIOBufferReader *mysql_decompress_buffer_reader_;
  ObZlibStreamCompressor compressor_;
  bool is_analyze_compressed_ob20_;
  ObMysqlRespAnalyzer analyzer_;
  ObRespResult result_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMysqlCompressAnalyzer);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OBPROXY_MYSQL_COMPRESS_ANALYZER_H */
