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

#ifndef OB_COMPRESSOR_POOL_H_
#define OB_COMPRESSOR_POOL_H_

#include "lib/compress/ob_compressor.h"
#include "none/ob_none_compressor.h"
#include "lz4/ob_lz4_compressor.h"
#include "snappy/ob_snappy_compressor.h"
#include "zlib/ob_zlib_compressor.h"
#include "zstd_1_3_8/ob_zstd_compressor_1_3_8.h"

namespace oceanbase
{
namespace common
{
enum ObCompressorType {
  INVALID_COMPRESSOR = 0 ,
  NONE_COMPRESSOR ,
  LZ4_COMPRESSOR ,
  SNAPPY_COMPRESSOR ,
  ZLIB_COMPRESSOR ,
  ZSTD_1_3_8_COMPRESSOR
};

class ObCompressorPool
{
public:
  static ObCompressorPool &get_instance();
  int get_compressor(const char *compressor_name, ObCompressor *&compressor);
  int get_compressor(const ObCompressorType& compressor_type, ObCompressor*& compressor);
  int get_compressor_type(const char *compressor_name, ObCompressorType &compressor_type) const;
private:
  ObCompressorPool();
  virtual ~ObCompressorPool() {}

  ObNoneCompressor none_compressor;
  ObLZ4Compressor lz4_compressor;
  ObSnappyCompressor snappy_compressor;
  ObZlibCompressor zlib_compressor;
  zstd_1_3_8::ObZstdCompressor_1_3_8 zstd_compressor_1_3_8;
};

} /* namespace common */
} /* namespace oceanbase */

#endif /* OB_COMPRESSOR_POOL_H_ */
