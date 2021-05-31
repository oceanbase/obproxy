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

#include "ob_zlib_compressor.h"
#include "zlib.h"
#include "lib/ob_errno.h"

namespace oceanbase
{
namespace common
{
const char *ObZlibCompressor::compressor_name = "zlib_1.0";
int ObZlibCompressor::compress(const char *src_buffer,
                               const int64_t src_data_size,
                               char *dst_buffer,
                               const int64_t dst_buffer_size,
                               int64_t &dst_data_size)
{
  int ret = OB_SUCCESS;
  int zlib_errno = Z_OK;
  int64_t compress_ret_size = dst_buffer_size;
  int64_t max_overflow_size = 0;
  if ( NULL == src_buffer
       || 0 >= src_data_size
       || NULL == dst_buffer
       || 0 >= dst_buffer_size ) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid compress argument, ",
        K(ret), KP(src_buffer), K(src_data_size), KP(dst_buffer), K(dst_buffer_size));
  } else if (OB_FAIL(get_max_overflow_size(src_data_size, max_overflow_size))) {
    LIB_LOG(WARN, "fail to get max_overflow_size, ", K(ret), K(src_data_size));
  } else if ((src_data_size + max_overflow_size) > dst_buffer_size) {
    ret = OB_BUF_NOT_ENOUGH;
    LIB_LOG(WARN, "dst buffer not enough, ",
        K(ret), K(src_data_size), K(max_overflow_size), K(dst_buffer_size));
  } else if (Z_OK != (zlib_errno = compress2(reinterpret_cast<Bytef*>(dst_buffer),
					                                   reinterpret_cast<uLongf*>(&compress_ret_size),
					                                   reinterpret_cast<const Bytef*>(src_buffer),
					                                   static_cast<uLong>(src_data_size),
					                                   static_cast<int>(compress_level_)))) {
    ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
    LIB_LOG(WARN, "fail to compress data by zlib, ",
        K(ret), "zlib_errno", zlib_errno, KP(src_buffer), K(src_data_size), K_(compress_level));
  } else {
    dst_data_size = compress_ret_size;
  }

  return ret;
}

int ObZlibCompressor::decompress(const char *src_buffer,
			                           const int64_t src_data_size,
			                           char *dst_buffer,
			                           const int64_t dst_buffer_size,
			                           int64_t &dst_data_size)
{
  int ret = OB_SUCCESS;
  int zlib_errno = Z_OK;
  int64_t decompress_ret_size = dst_buffer_size;
  if (NULL == src_buffer
      || 0 >= src_data_size
      || NULL == dst_buffer
      || 0 >= dst_buffer_size) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid decompress argument, ",
        K(ret), KP(src_buffer), K(src_data_size), KP(dst_buffer), K(dst_buffer_size));
  } else if (Z_OK != (zlib_errno = ::uncompress(reinterpret_cast<Bytef*>(dst_buffer),
                                                reinterpret_cast<uLongf*>(&decompress_ret_size),
                                                reinterpret_cast<const Byte*>(src_buffer),
                                                static_cast<uLong>(src_data_size)))) {
    ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
    LIB_LOG(WARN, "fail to decompress data by zlib, ",
        K(ret), "zlib_errno", zlib_errno, KP(src_buffer), K(src_data_size));
  } else {
    dst_data_size = decompress_ret_size;
  }

  return ret;
}

int ObZlibCompressor::set_compress_level(const int64_t compress_level)
{
  int ret = OB_SUCCESS;

  if (compress_level < -1 || compress_level > 9) {
     ret = OB_INVALID_ARGUMENT;
     LIB_LOG(WARN, "invalid argument, ", K(ret), K(compress_level));
  } else {
     compress_level_ = compress_level;
  }
  return ret;
}
const char *ObZlibCompressor::get_compressor_name() const
{
  return compressor_name;
}

int ObZlibCompressor::get_max_overflow_size(const int64_t src_data_size,
                                            int64_t &max_overflow_size) const
{
  int ret = OB_SUCCESS;
  if (src_data_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid argument, ", K(ret), K(src_data_size));
  } else {
    max_overflow_size = (src_data_size >> 12) + (src_data_size >> 14) + (src_data_size >> 25 ) + 13;
  }
  return ret;
}

}//namespace common
}//namespace oceanbase
