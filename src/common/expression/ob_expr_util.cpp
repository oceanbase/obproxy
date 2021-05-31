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

#define USING_LOG_PREFIX SQL_ENG
#include "common/expression/ob_expr_util.h"
#include "lib/container/ob_array.h"

using namespace oceanbase::common;

// Get the byte offset of each character in the multiple bytes string
// And the number of bytes occupied by each mb character
// The first element in byte_offsets is the starting byte position of 
// the second character, and the last element is the length of the string
// Each element in byte_num corresponds to the 
// number of bytes occupied by each character of input str
// ori_str: abc
// ori_str_byte_offsets: [0, 1, 2, 3]
// ori_str_byte_num: [1, 1, 1]
int ObExprUtil::get_mb_str_info(const ObString &str,
                                ObCollationType cs_type,
                                ObIArray<size_t> &byte_num,
                                ObIArray<size_t> &byte_offsets)
{
  int ret = OB_SUCCESS;
  int64_t well_formed_len = 0;
  if (CS_TYPE_INVALID == cs_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cs_type", K(ret), K(cs_type));
  } else if (OB_ISNULL(str.ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("str.ptr is null", K(ret));
  } else if (0 == str.length()) {
    // do nothing
  } else if (OB_FAIL(ObCharset::well_formed_len(cs_type,
                                                str.ptr(),
                                                str.length(),
                                                well_formed_len))) {
    LOG_WARN("invalid string for charset", K(str), K(well_formed_len), K(cs_type));
  } else {
    size_t byte_index = 0;
    while (OB_SUCC(ret) && byte_index < str.length()) {
      if (OB_FAIL(byte_offsets.push_back(byte_index))) {
        LOG_WARN("byte_offsets.push_back failed", K(ret), K(byte_offsets));
      } else {
        size_t tmp_off = ObCharset::charpos(cs_type, str.ptr() + byte_index,
                                            str.length() - byte_index, 1);
        byte_index += tmp_off;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(byte_index != str.length())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected byte_index", K(ret), K(byte_index), K(str), K(byte_offsets));
      } else if (OB_FAIL(byte_offsets.push_back(byte_index))) {
        LOG_WARN("byte_offsets.push_back failed", K(ret), K(byte_offsets));
      }
    }

    // Get the number of bytes occupied by each character
    for (size_t i = 1; OB_SUCC(ret) && (i < byte_offsets.count()); ++i) {
      if (OB_FAIL(byte_num.push_back(byte_offsets.at(i) - byte_offsets.at(i-1)))) {
        LOG_WARN("byte_num.push_back failed", K(ret), K(byte_num));
      }
    }
    LOG_DEBUG("get_mb_str_info done", K(ret), K(str), K(cs_type), K(byte_offsets), K(byte_num));
  }
  return ret;
}

int kmp_next(const char *x, int64_t m, ObArray<int64_t> &next)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(x) || OB_UNLIKELY(m <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg", K(m), K(ret));
  } else {
    int64_t i = 0;
    int64_t j = -1;
    next[0] = -1;
    while (i < m) {
      while (-1 < j && x[i] != x[j]) {
        j = next[j];
      }
      i++;
      j++;
      if (x[i] == x[j]) {
        next[i] = next[j];
      } else {
        next[i] = j;
      }
    }
  }
  return ret;
}

int ObExprUtil::kmp(const char *x, 
                    int64_t m, 
                    const char *y, 
                    int64_t n, 
                    int64_t count, 
                    int64_t &result)
{
  int ret = OB_SUCCESS;
  int64_t i = 0;
  int64_t j = 0;
  int64_t t = 0;
  ObArray<int64_t> next;
  result = -1;

  if (OB_ISNULL(x) || OB_ISNULL(y) || OB_UNLIKELY(m <= 0) || OB_UNLIKELY(n <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ptr", K(ret));
  } else if (OB_FAIL(next.prepare_allocate(m + 1))) {
    LOG_WARN("allocate fail", K(m), K(ret));
  } else if (m <= n) {
    // preprocessing
    if (OB_SUCC(kmp_next(x, m, next))) {
      // searching
      i = j = t = 0;
      while (j < n && -1 == result) {
        while (-1 < i && x[i] != y[j]) {
          i = next[i];
        }
        i++;
        j++;
        if (i >= m) {
          t++;
          // find nth apperance
          if (t == count) {
            result = j - i;
          }
          i = 0;
        }
      }
    }
  }
  return ret;
}

/**
 * next array is reversed, for example
 * [2, 2, 2, 3] to [3, 2, 2, 2]
 * kmp_reverse is changed according to next array
 * because next array needs one more space than delim.length
 */
int kmp_next_reverse(const char *x, int64_t m, ObArray<int64_t> &next)
{
  int ret = OB_SUCCESS;
  int64_t i = m - 1;
  int64_t j = m;

  if (OB_ISNULL(x) || OB_UNLIKELY(m <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ptr", K(ret));
  } else {
    next[0] = m;
    while (0 <= i) {
      while (j < m && x[i] != x[j]) {
        j = next[m - 1 - j];
      }
      i--;
      j--;
      if (x[i] == x[j]) {
        next[m - 1 - i] = next[m - 1 - j];
      } else {
        next[m - 1 - i] = j;
      }
    }
  }

  return ret;
}

/**
 * read reversed next array
 * next[i] to next[m-1-i]
 */
int ObExprUtil::kmp_reverse(const char *x, 
                            int64_t m, 
                            const char *y, 
                            int64_t n, 
                            int64_t count, 
                            int64_t &result)
{
  int ret = OB_SUCCESS;
  int64_t i = 0;
  int64_t j = 0;
  int64_t t = 0;
  ObArray<int64_t> next;
  result = -1;

  if (OB_ISNULL(x) || OB_ISNULL(y) || OB_UNLIKELY(m <= 0) || OB_UNLIKELY(n <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ptr", K(m), K(n), K(ret));
  } else if (OB_FAIL(next.prepare_allocate(m + 1))) {
    LOG_WARN("allocate fail", K(m), K(ret));
  } else if (m <= n) {
    // preprocessing
    ret = kmp_next_reverse(x, m, next);

    // searching from back to front
    i = m - 1;
    j = n - 1;
    t = 0;
    while (0 <= j && -1 == result) {
      while (i < m && x[i] != y[j]) {
        i = next[m - 1 - i];
      }
      i--;
      j--;
      if (0 > i) {
        t--;
        // find nth apperance
        if (t == count) {
          result = j + 1;
        }
        i = m - 1;
      }
    }
  }
  return ret;
}

