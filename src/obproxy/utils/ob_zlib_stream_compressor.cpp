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

#define USING_LOG_PREFIX PROXY
#include "utils/ob_zlib_stream_compressor.h"
#include "lib/alloc/malloc_hook.h"

namespace oceanbase
{
namespace obproxy
{

using namespace oceanbase::common;

int ObZlibStreamCompressor::reset()
{
  int ret = OB_SUCCESS;
  is_finished_ = false;
  if (COMPRESS_TYPE == type_) {
    if (OB_FAIL(compress_close())) {
      LOG_WDIAG("fail to close zlib", K(ret));
    }
  } else if (DECOMPRESS_TYPE == type_) {
    if (OB_FAIL(decompress_close())) {
      LOG_WDIAG("fail to close zlib", K(ret));
    }
  }
  type_ = NONE_TYPE;
  compress_flush_type_ = Z_NO_FLUSH;
  return ret;
}

ObZlibStreamCompressor::~ObZlibStreamCompressor()
{
  if (is_inited()) { // just for defense
    LOG_EDIAG("never happen", K_(type), K_(is_finished));
  }
}

int ObZlibStreamCompressor::add_decompress_data(const char *src_buf, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_buf) || len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", KP(src_buf), K(len), K(ret));
  } else if (is_inited()) {
    if (DECOMPRESS_TYPE != type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("invalid compress type, DECOMPRESS_TYPE expected", K_(type), K(ret));
    }
  } else if (!is_inited()) {
    if (OB_FAIL(decompress_init())) {
      LOG_WDIAG("fail to decompress init", K(ret));
    }
  }


  if (OB_SUCC(ret)) {
    stream_.next_in = reinterpret_cast<Bytef *>(const_cast<char *>(src_buf));
    stream_.avail_in = static_cast<uInt>(len);
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = decompress_close())) {
      LOG_WDIAG("fail to close zlib", K(tmp_ret));
    }
  }
  return ret;
}

int ObZlibStreamCompressor::add_compress_data(
    const char *src_buf,
    const int64_t len,
    const bool is_last_data)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_buf) || len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", KP(src_buf), K(len), K(ret));
  } else if (is_inited()) {
    if (COMPRESS_TYPE != type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("invalid compress type, COMPRESS_TYPE expected", K_(type), K(ret));
    }
  } else if (!is_inited()) {
    if (OB_FAIL(compress_init())) {
      LOG_WDIAG("fail to compress init", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    stream_.next_in = reinterpret_cast<Bytef *>(const_cast<char *>(src_buf));
    stream_.avail_in = static_cast<uInt>(len);
    if (is_last_data) {
      compress_flush_type_ = Z_FINISH;
    } else {
      compress_flush_type_ = Z_NO_FLUSH;
    }
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = compress_close())) {
      LOG_WDIAG("fail to close zlib", K(tmp_ret));
    }
  }
  return ret;
}

int ObZlibStreamCompressor::decompress_init()
{
  int ret = OB_SUCCESS;
  int zlib_ret = Z_OK;

  stream_.zalloc = Z_NULL;
  stream_.zfree = Z_NULL;
  stream_.opaque = Z_NULL;
  stream_.avail_in = 0;
  stream_.next_in = Z_NULL;
  lib::glibc_hook_opt = lib::GHO_HOOK;
  if (Z_OK != (zlib_ret = ::inflateInit(&stream_))) {
    ret = OB_INIT_FAIL;
    LOG_WDIAG("fail to init zlib", K(zlib_ret), K(ret));
  } else {
    type_ = DECOMPRESS_TYPE;
    is_finished_ = false;
  }
  lib::glibc_hook_opt = lib::GHO_NOHOOK;

  return ret;
}

int ObZlibStreamCompressor::compress_init()
{
  int ret = OB_SUCCESS;
  int zlib_ret = Z_OK;

  stream_.zalloc = Z_NULL;
  stream_.zfree = Z_NULL;
  stream_.opaque = Z_NULL;
  lib::glibc_hook_opt = lib::GHO_HOOK;
  if (Z_OK != (zlib_ret = ::deflateInit(&stream_, static_cast<int>(compress_level_)))) {
    ret = OB_INIT_FAIL;
    LOG_WDIAG("fail to init zlib", K(zlib_ret), K(ret));
  } else {
    type_ = COMPRESS_TYPE;
    is_finished_ = false;
  }
  lib::glibc_hook_opt = lib::GHO_NOHOOK;

  return ret;
}

int ObZlibStreamCompressor::decompress(char *dest_start, const int64_t len, int64_t &filled_len)
{
  int ret = OB_SUCCESS;
  int zlib_ret = Z_OK;
  filled_len = 0;
  bool has_closed = false;

  if (OB_ISNULL(dest_start) || (len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", KP(dest_start), K(len), K(ret));
  } else if (is_finished_) { // if has finished, just return
    filled_len = 0;
  } else if (DECOMPRESS_TYPE != type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid compress type, DECOMPRESS_TYPE expected", K_(type), K(ret));
  } else {
    stream_.avail_out = static_cast<uInt>(len);
    stream_.next_out = reinterpret_cast<Bytef *>(dest_start);

    lib::glibc_hook_opt = lib::GHO_HOOK;
    zlib_ret = ::inflate(&stream_, Z_NO_FLUSH);
    lib::glibc_hook_opt = lib::GHO_NOHOOK;

    LOG_DEBUG("zlib decomress complete", K(zlib_ret));
    if (Z_STREAM_END == zlib_ret) { // this means the total decompres is finished
      LOG_DEBUG("zlib decomress stream end");
      filled_len = len - stream_.avail_out;
      is_finished_ = true;
      has_closed = true;
      if (OB_FAIL(decompress_close())) {
        LOG_WDIAG("fail to close zlib", K(ret));
      }
    } else if (Z_OK == zlib_ret) {
      filled_len = len - stream_.avail_out;
    } else {
      // Z_BUF_ERROR if no progress is possible (for example avail_in or avail_out was zero).
      // Note that Z_BUF_ERROR is not fatal, and deflate() can be called again with more
      // input and more output space to continue compressing.
      //
      // means that if the decompressed data has no output data, Z_BUF_ERROR will be returned.
      // happen when:
      // add_decompress_data();
      // decompress(dest_ptr, len, filled_len);
      // when len == filled_len and all data has decompressed complete,
      // then invoke decompress again, here no avail_in, Z_BUF_ERROR will returen;
      if ((Z_BUF_ERROR == zlib_ret) && (stream_.avail_out == len)) {
        LOG_DEBUG("the compressed data has no output data", K(zlib_ret),
                  K(stream_.avail_out), K(len), K(zlib_ret));
        filled_len = 0;
        ret = OB_SUCCESS;
      } else {
        ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
        LOG_WDIAG("fail to decpmress", K(zlib_ret), K(stream_.avail_out), K(ret));
      }
    }
  }

  if (OB_FAIL(ret) && !has_closed) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = decompress_close())) {
      LOG_WDIAG("fail to close zlib", K(tmp_ret));
    }
  }

  return ret;
}

int ObZlibStreamCompressor::compress(char *dest_start, const int64_t len, int64_t &filled_len)
{
  int ret = OB_SUCCESS;
  int zlib_ret = Z_OK;
  filled_len = 0;
  bool has_closed = false;

  if (OB_ISNULL(dest_start) || (len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", KP(dest_start), K(len), K(ret));
  } else if (is_finished_) {
    filled_len = 0;
  } else if (COMPRESS_TYPE != type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid compress type, COMPRESS_TYPE expected", K_(type), K(ret));
  } else {
    stream_.avail_out = static_cast<uInt>(len);
    stream_.next_out = reinterpret_cast<Bytef *>(dest_start);

    lib::glibc_hook_opt = lib::GHO_HOOK;
    zlib_ret = ::deflate(&stream_, static_cast<int>(compress_flush_type_));
    lib::glibc_hook_opt = lib::GHO_NOHOOK;
    LOG_DEBUG("zlib compress complete", K(zlib_ret), K_(compress_flush_type));
    if (Z_STREAM_END == zlib_ret) { // this means the total decompres is finished
      LOG_DEBUG("zlib comress stream end");
      filled_len = len - stream_.avail_out;
      is_finished_ = true;
      has_closed = true;
      if (OB_FAIL(compress_close())) {
        LOG_WDIAG("fail to close zlib", K(ret));
      }
    } else if (Z_OK == zlib_ret) {
      filled_len = len - stream_.avail_out;
    } else {
      ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
      LOG_WDIAG("fail to decpmress", K(zlib_ret), K(ret));
    }
  }

  if (OB_FAIL(ret) && !has_closed) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = compress_close())) {
      LOG_WDIAG("fail to close zlib", K(tmp_ret));
    }
  }

  return ret;
}

int ObZlibStreamCompressor::get_max_overflow_size(const int64_t src_data_size, int64_t &max_overflow_size)
{
  int ret = OB_SUCCESS;
  if (src_data_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument, ", K(src_data_size), K(ret));
  } else {
    max_overflow_size = (src_data_size >> 12) + (src_data_size >> 14) + (src_data_size >> 25) + 13;
  }
  return ret;
}

int ObZlibStreamCompressor::decompress_close()
{
  int ret = OB_SUCCESS;
  if (DECOMPRESS_TYPE == type_) {
    lib::glibc_hook_opt = lib::GHO_HOOK;
    int zlib_ret = ::inflateEnd(&stream_);
    lib::glibc_hook_opt = lib::GHO_NOHOOK;
    if (Z_OK != zlib_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("fail to decompress close", K(zlib_ret), K(ret));
    }
    type_ = NONE_TYPE;
  }
  return ret;
}

int ObZlibStreamCompressor::compress_close()
{
  int ret = OB_SUCCESS;
  if (COMPRESS_TYPE == type_) {
    lib::glibc_hook_opt = lib::GHO_HOOK;
    int zlib_ret = ::deflateEnd(&stream_);
    lib::glibc_hook_opt = lib::GHO_NOHOOK;
    if (Z_OK != zlib_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("fail to compress_close", K(zlib_ret), K(ret));
    }
    type_ = NONE_TYPE;
  }
  return ret;
}

} // end of namespace obproxy
} // end of namespace oceanbase
