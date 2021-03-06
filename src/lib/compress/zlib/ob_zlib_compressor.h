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

#ifndef OCEANBASE_COMMOM_COMPRESS_ZLIB_COMPRESSOR_
#define OCEANBASE_COMMOM_COMPRESS_ZLIB_COMPRESSOR_
#include "lib/compress/ob_compressor.h"

namespace oceanbase
{
namespace common
{
class ObZlibCompressor : public ObCompressor
{
public:
  const static char *NAME;
public:
  explicit ObZlibCompressor(int64_t compress_level = 6) : compress_level_(compress_level) {}
  ~ObZlibCompressor() {}
  int compress(const char *src_buffer,
	             const int64_t src_data_size,
	             char *dst_buffer,
	             const int64_t dst_buffer_size,
	             int64_t &dst_data_size);
  int decompress(const char *src_buffer,
	               const int64_t src_data_size,
	               char *dst_buffer,
	               const int64_t dst_buffer_size,
	               int64_t &dst_data_size);
  int set_compress_level(const int64_t compress_level);
  const char *get_compressor_name() const;
  int get_max_overflow_size(const int64_t src_data_size,
                            int64_t &max_overflow_size) const;
private:
  int64_t compress_level_;
  static const char *compressor_name;
};

}//namespace common
}//namespace oceanbase
#endif //OCEANBASE_COMMOM_COMPRESS_ZLIB_COMPRESSOR_
