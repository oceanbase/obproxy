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

  static int kmp_reverse(const char *x, int64_t m, const char *y, int64_t n, int64_t count, int64_t &pos);
  static int kmp(const char *x, int64_t m, const char *y, int64_t n, int64_t count, int64_t &pos);

  DISALLOW_COPY_AND_ASSIGN(ObExprUtil);
};
}
}
#endif  /* _OB_ENGINE_EXPR_EXPR_UTIL_H_ */
