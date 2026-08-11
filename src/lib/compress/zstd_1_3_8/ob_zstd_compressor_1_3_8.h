/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_COMMON_COMPRESS_ZSTD_1_3_8_COMPRESSOR_
#define OCEANBASE_COMMON_COMPRESS_ZSTD_1_3_8_COMPRESSOR_
#include "lib/compress/ob_compressor.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase {
namespace common {

namespace zstd_1_3_8 {

class ObZstdCtxAllocator {
public:
  ObZstdCtxAllocator();
  virtual ~ObZstdCtxAllocator();
  void* alloc(size_t size);
  void free(void* addr);
  void reuse();

private:
  ObArenaAllocator allocator_;
};

class __attribute__((visibility("default"))) ObZstdCompressor_1_3_8 : public ObCompressor {
public:
  explicit ObZstdCompressor_1_3_8()
  {}
  virtual ~ObZstdCompressor_1_3_8()
  {}
  int compress(const char* src_buffer, const int64_t src_data_size, char* dst_buffer, const int64_t dst_buffer_size,
      int64_t& dst_data_size);
  int decompress(const char* src_buffer, const int64_t src_data_size, char* dst_buffer, const int64_t dst_buffer_size,
      int64_t& dst_data_size);
  const char* get_compressor_name() const;
  int get_max_overflow_size(const int64_t src_data_size, int64_t& max_overflow_size) const;

private:
  static const char* compressor_name;
};
}  // namespace zstd_1_3_8
}  // namespace common
}  // namespace oceanbase
#endif  // OCEANBASE_COMMON_COMPRESS_ZSTD_1_3_8_COMPRESSOR_
