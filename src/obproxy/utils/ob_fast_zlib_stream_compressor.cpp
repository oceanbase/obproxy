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
#include "utils/ob_fast_zlib_stream_compressor.h"

namespace oceanbase
{
namespace obproxy
{
using namespace oceanbase::common;

int ObFastZlibStreamCompressor::compress(char *dest_start, const int64_t len, int64_t &filled_len)
{
  int ret = OB_SUCCESS;
  filled_len = 0;

  if (OB_ISNULL(dest_start) || (len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", KP(dest_start), K(len), K(ret));
  } else if (COMPRESS_FILL_COMPLETE == compress_state_) {
    filled_len = 0;
  } else if (COMPRESS_TYPE != type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid compress type, COMPRESS_TYPE expected", K_(type), K(ret));
  } else {
    Byte *dest = reinterpret_cast<Byte *>(dest_start);
    int64_t curr_dest_len = len;

    while ((filled_len < len) && (COMPRESS_FILL_COMPLETE != compress_state_)) {
      switch (compress_state_) {
        case COMPRESS_FILL_HEADER: {
          copy_compress_data(dest + filled_len, curr_dest_len, header_, HEADER_LEN, filled_len);
          if (HEADER_LEN == state_filled_len_) {
            compress_state_ = COMPRESS_FILL_DATA;
            state_filled_len_ = 0;
          }
          break;
        }
        case COMPRESS_FILL_DATA: {
          copy_compress_data(dest + filled_len, curr_dest_len, src_start_, src_len_, filled_len);
          if (state_filled_len_ == src_len_) {
            compress_state_ = COMPRESS_FILL_TAILER;
            state_filled_len_ = 0;
          }
          break;
        }
        case COMPRESS_FILL_TAILER: {
          copy_compress_data(dest + filled_len, curr_dest_len, tailer_, TAILER_LEN, filled_len);
          if (TAILER_LEN == state_filled_len_) {
            compress_state_ = COMPRESS_FILL_COMPLETE;
            state_filled_len_ = 0;
          }
          break;
        }
        case COMPRESS_FILL_COMPLETE: {
          LOG_DEBUG("compress complete", K_(adler), KP(src_start_),
                    K_(src_len), K_(compress_state));
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("invalid sate", K(compress_state_), K(ret));
        }
      }
    }
  }

  return ret;
}

inline void ObFastZlibStreamCompressor::copy_compress_data(
    Byte *dest, int64_t &dest_len, const Byte *src,
    int64_t src_len, int64_t &filled_len)
{
  int64_t current_src_len = src_len - state_filled_len_;
  const Byte *current_src_start = src + state_filled_len_;
  int64_t remain_len = ((dest_len > current_src_len) ? current_src_len : dest_len);
  MEMCPY(dest, current_src_start, remain_len);
  filled_len += remain_len;
  dest_len -= remain_len;
  state_filled_len_ += remain_len;
}

} // end of namespace obproxy
} // end of namespace oceanbase
