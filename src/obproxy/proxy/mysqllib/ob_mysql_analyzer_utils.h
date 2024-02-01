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

#ifndef OBPROXY_MYSQL_ANALYZER_UTILS_H
#define OBPROXY_MYSQL_ANALYZER_UTILS_H

#include "lib/ob_define.h"
#include "proxy/mysqllib/ob_mysql_common_define.h"

namespace oceanbase
{
namespace obproxy
{
class ObZlibStreamCompressor;
namespace event
{
class ObIOBufferReader;
class ObMIOBuffer;
}
namespace proxy
{
class ObProtocolDiagnosis;
class ObMysqlCompressedAnalyzeResult;
class ObCmpHeaderParam
{
public:
  ObCmpHeaderParam() :
    compressed_seq_(0),
    is_checksum_on_(false),
    compression_level_(0),
    protocol_diagnosis_(NULL) {}

  ObCmpHeaderParam(
    uint8_t compressed_seq,
    bool is_checksum_on,
    int64_t compression_level)
    : compressed_seq_(compressed_seq),
      is_checksum_on_(is_checksum_on),
      compression_level_(compression_level),
      protocol_diagnosis_(NULL){}

  ~ObCmpHeaderParam();
  ObCmpHeaderParam(const ObCmpHeaderParam &param);
  ObCmpHeaderParam &operator=(const ObCmpHeaderParam &param);
  inline uint8_t &get_compressed_seq() { return compressed_seq_; }
  inline const bool is_checksum_on() { return is_checksum_on_; }
  inline const int64_t get_compression_level() { return compression_level_; }

  ObProtocolDiagnosis *&get_protocol_diagnosis_ref();
  ObProtocolDiagnosis *get_protocol_diagnosis();
  const ObProtocolDiagnosis *get_protocol_diagnosis() const;

  TO_STRING_KV(K_(compressed_seq), K_(is_checksum_on), K_(compression_level), KP_(protocol_diagnosis));

private:
  uint8_t compressed_seq_;
  bool is_checksum_on_;
  int64_t compression_level_;
  ObProtocolDiagnosis *protocol_diagnosis_;
};

class ObMysqlAnalyzerUtils
{
public:
  // judge whether one mysql compressed packet has been received complete, and get packt len
  // if completed, return ANALYZE_DONE
  // if not,       return ANALYZE_CONT
  static int analyze_one_compressed_packet(event::ObIOBufferReader &reader,
                                           ObMysqlCompressedAnalyzeResult &result);

  static int analyze_compressed_packet_header(const char *start, const int64_t len,
                                              ObMysqlCompressedPacketHeader &header);
  static int do_zlib_compress(event::ObIOBufferReader *src_reader,
                              const int64_t src_data_len,
                              event::ObMIOBuffer *des_buf,
                              const bool is_checksum_on,
                              const int64_t compression_level,
                              int64_t &compressed_len,
                              int64_t &uncompressed_len);
  // @reader, contain the standard mysql data;
  // @data_len, the len of standard mysql data to be compressed
  // @write_buf, the compressed data will be written to
  // if succ, reader will consume data_len
  static int consume_and_normal_compress_data(event::ObIOBufferReader *reader,
                                              event::ObMIOBuffer *write_buf,
                                              const int64_t data_len,
                                              ObCmpHeaderParam &param);

  // @reader, contain the standard mysql data;
  // @data_len, the len of standard mysql data to be compressed
  // @write_buf, the compressed data will be written to
  // if succ, reader will consume data_len
  static int consume_and_fast_compress_data(event::ObIOBufferReader *reader,
                                            event::ObMIOBuffer *write_buf,
                                            const int64_t data_len,
                                            ObCmpHeaderParam &param);

  static int consume_and_compress_data(event::ObIOBufferReader *reader,
                                       event::ObMIOBuffer *write_buf,
                                       const int64_t data_len,
                                       ObCmpHeaderParam &param);

  static int stream_compress_data(ObZlibStreamCompressor &compressor,
                                  event::ObMIOBuffer *write_buf, const char *buf,
                                  const int64_t len, const bool is_last_data,
                                  int64_t &compressed_len);

  static int stream_compress_data(event::ObMIOBuffer *write_buf, const char *buf,
                                  const int64_t len, int64_t &compressed_len);


  static int reserve_compressed_hdr(event::ObMIOBuffer *write_buf, char *&hdr_start);

  static int fill_compressed_header(const int64_t uncompress_len, const uint8_t seq,
                                    const int64_t compressed_len,
                                    char *compressed_hdr_buf_start);

};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OBPROXY_MYSQL_ANALYZER_UTILS_H */
