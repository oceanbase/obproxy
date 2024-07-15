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
