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

#ifndef OBPROXY_ZLIB_STREAM_COMPRESSOR_H
#define OBPROXY_ZLIB_STREAM_COMPRESSOR_H

#include "lib/ob_define.h"
#include "lib/compress/zlib/zlib.h"

namespace oceanbase
{
namespace obproxy
{

class ObZlibStreamCompressor
{
enum ObCompressType
{
  NONE_TYPE = 0,
  COMPRESS_TYPE,
  DECOMPRESS_TYPE,
};

public:
  explicit ObZlibStreamCompressor(int64_t compress_level = 6)
    : is_finished_(false), type_(NONE_TYPE), compress_level_(compress_level),
      compress_flush_type_(Z_NO_FLUSH), stream_() {}
  ~ObZlibStreamCompressor();
  int reset();

  // stream decompress
  // usage:
  //  while (has_more_data to decompress) {
  //   add_decomress_data()
  //   do {
  //     decompress(buf, len, filled_len);
  //   } while (filled_len == len)
  // }
  int add_decompress_data(const char *src_buf, const int64_t len);
  int decompress(char *dest_start, const int64_t len, int64_t &filled_len);

  // stream compress
  // usage:
  //  while (has_more_data to compress) {
  //   add_comress_data()
  //   do {
  //     compress(buf, len, filled_len);
  //   } while (filled_len == len)
  // }
  int add_compress_data(const char *src_start, const int64_t len, const bool is_last_data);
  int compress(char *dest_start, const int64_t len, int64_t &filled_len);
  bool is_inited() { return NONE_TYPE != type_; }

public:
  static int get_max_overflow_size(const int64_t src_data_size, int64_t &max_overflow_size);

private:
  int decompress_init();
  int decompress_close();

  int compress_init();
  int compress_close();

private:
  bool is_finished_; // whether the compress is finished
  ObCompressType type_;
  int64_t compress_level_; // 0 ~ 9, 6 is default
  // Z_FINISH :
  //    which is later passed to deflate() to indicate that this is the
  //    last chunk of input data to compress.
  // Z_NO_FLUSH :
  //    If we are not yet at the end of the input, then the zlib constant
  //    Z_NO_FLUSH will be passed to deflate to indicate that we are still
  //    in the middle of the uncompressed data.
  int64_t compress_flush_type_;
  z_stream stream_; // zlib stream
  DISALLOW_COPY_AND_ASSIGN(ObZlibStreamCompressor);
};

} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OBPROXY_ZLIB_STREAM_COMPRESSOR_H */
