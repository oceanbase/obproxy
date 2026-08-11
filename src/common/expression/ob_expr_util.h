/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_ENGINE_EXPR_EXPR_UTIL_H_
#define _OB_ENGINE_EXPR_EXPR_UTIL_H_

#include "lib/string/ob_string.h"
#include "lib/charset/ob_charset.h"
#include "lib/container/ob_iarray.h"

namespace oceanbase
{
namespace common
{
class ObExprUtil
{
public:
  static int get_mb_str_info(const common::ObString &str,
                             common::ObCollationType cs_type,
                             common::ObIArray<size_t> &byte_num,
                             common::ObIArray<size_t> &byte_offset);

  // This function relies on `kmp_next` to do the calculation of next array
  static int kmp(const char *pattern,
                 const int64_t pattern_len,
                 const char *text,
                 const int64_t text_len,
                 const int64_t nth_appearance,
                 const int32_t *next, /* calculated, size same with pattern */
                 int64_t &result);
  static int kmp_next(const char *pattern, const int64_t pattern_len, int32_t *next);

  // This function relies on `kmp_next_reverse` to do the calculation of next array
  static int kmp_reverse(const char *pattern,
                         const int64_t pattern_len,
                         const char *text,
                         const int64_t text_len,
                         const int64_t nth_appearance,
                         const int32_t *next, /* calculated, size same with pattern */
                         int64_t &result);
  static int kmp_next_reverse(const char *pattern,
                              const int64_t pattern_len,
                              int32_t *next);

  DISALLOW_COPY_AND_ASSIGN(ObExprUtil);
};
}
}
#endif  /* _OB_ENGINE_EXPR_EXPR_UTIL_H_ */
