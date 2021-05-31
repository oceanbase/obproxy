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

#ifndef OBPROXY_FAST_ZLIB_STREAM_COMPRESSOR_H
#define OBPROXY_FAST_ZLIB_STREAM_COMPRESSOR_H

#include "lib/ob_define.h"
#include "lib/compress/zlib/zlib.h"

namespace oceanbase
{
namespace obproxy
{

class ObFastZlibStreamCompressor
{
enum ObCompressType
{
  NONE_TYPE = 0,
  COMPRESS_TYPE,
  DECOMPRESS_TYPE,
};

enum ObCompressState
{
  COMPRESS_FILL_HEADER = 0,
  COMPRESS_FILL_DATA,
  COMPRESS_FILL_TAILER,
  COMPRESS_FILL_COMPLETE
};

public:
  ObFastZlibStreamCompressor() { reset(); }
  ~ObFastZlibStreamCompressor() {}
  void reset() { compress_init(); }

  // stream compress, compress data must < 64KB
  // usage:
  //   add_comress_data()
  //   do {
  //     compress(buf, len, filled_len);
  //   } while (filled_len == len)
  //
  int add_compress_data(const char *src_start, const int64_t len);
  int compress(char *dest_start, const int64_t len, int64_t &filled_len);

private:
  void compress_init();
  void compress_close() { compress_init(); }
  void calc_level0_compress_meta(const Byte *source, uLong sourceLen);
  void copy_compress_data(Byte *dest, int64_t &dest_len, const Byte *src, int64_t src_len, int64_t &filled_len);

private:
  ObCompressType type_;

  static const uLong HEADER_LEN = 7;
  static const uLong TAILER_LEN = 4;
  Byte header_[HEADER_LEN];
  Byte tailer_[TAILER_LEN];
  uLong adler_;
  const Byte *src_start_;
  int64_t src_len_;
  ObCompressState compress_state_;
  int64_t state_filled_len_;

  DISALLOW_COPY_AND_ASSIGN(ObFastZlibStreamCompressor);
};

inline void ObFastZlibStreamCompressor::compress_init()
{
  adler_ = 0;
  src_start_ = NULL;
  src_len_ = 0;
  compress_state_ = COMPRESS_FILL_HEADER;
  state_filled_len_ = 0;
  type_ = NONE_TYPE;
}

inline int ObFastZlibStreamCompressor::add_compress_data(const char *src_buf, const int64_t len)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(src_buf) || len <= 0 || len >= UINT16_MAX) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_LOG(WARN, "invalid input value", KP(src_buf), K(len), K(ret));
  } else {
    compress_init();
    type_ = COMPRESS_TYPE;
    src_start_ = reinterpret_cast<const Byte *>(src_buf);
    src_len_ = len;
    calc_level0_compress_meta(src_start_, static_cast<ulong>(len));
  }
  return ret;
}

inline void ObFastZlibStreamCompressor::calc_level0_compress_meta(const unsigned char *source, ulong sourceLen)
{
  //https://tools.ietf.org/html/rfc1950
  //https://tools.ietf.org/html/rfc1952
  header_[0] = (Byte)(0x78);
  header_[1] = (Byte)(0x01);
  header_[2] = (Byte)(0x01);
  header_[3] = (Byte)(sourceLen & 0x000000ff);
  header_[4] = (Byte)((sourceLen & 0x0000ff00) >> 8);
  header_[5] = (Byte)((~sourceLen) & 0x000000ff);
  header_[6] = (Byte)(((~sourceLen) & 0x0000ff00) >> 8);

  adler_ = adler32(1L, source, static_cast<uInt>(sourceLen));

  tailer_[0] = (Byte)((adler_ & 0xff000000) >> 24);
  tailer_[1] = (Byte)((adler_ & 0x00ff0000) >> 16);
  tailer_[2] = (Byte)((adler_ & 0x0000ff00) >> 8);
  tailer_[3] = (Byte)((adler_ & 0x000000ff));
}

} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OBPROXY_FAST_ZLIB_STREAM_COMPRESSOR_H */
