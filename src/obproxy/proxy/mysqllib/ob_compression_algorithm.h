/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_COMPRESSION_ALGORITHM_H
#define OBPROXY_COMPRESSION_ALGORITHM_H
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
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
} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OBPROXY_MYSQL_COMPRESS_ANALYZER_H */
