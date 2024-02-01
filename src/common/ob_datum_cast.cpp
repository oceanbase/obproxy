/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX COMMON
#include "ob_datum_cast.h"
#include "lib/wide_integer/ob_wide_integer.h"
#include "lib/wide_integer/ob_wide_integer_cmp_funcs.h"
namespace oceanbase
{
namespace common
{

int ObDatumCast::align_decint_precision_unsafe(const ObDecimalInt *decint, const int32_t int_bytes,
                                               const int32_t expected_int_bytes,
                                               ObDecimalIntBuilder &res)
{
  int ret = OB_SUCCESS;
  res.from(decint, int_bytes);
  if (int_bytes > expected_int_bytes) {
    res.truncate(expected_int_bytes);
  } else if (int_bytes < expected_int_bytes) {
    res.extend(expected_int_bytes);
  } else {
    // do nothing
  }
  return ret;
}

template <typename T>
static int scale_down_decimalint(const T &x, unsigned scale, ObDecimalIntBuilder &res,
                                 const ObCastMode cm, bool &has_extra_decimals)
{
  static const int64_t pows[5] = {10, 100, 10000, 100000000, 10000000000000000};
  int ret = OB_SUCCESS;
  T result = x;
  bool is_neg = (x < 0);
  if (is_neg) {
    result = -result;
  }
  T remain;
  while (scale != 0 && result != 0) {
    for (int i = ARRAYSIZEOF(pows) - 1; scale != 0 && result != 0 && i >= 0; i--) {
      if (scale & (1 << i)) {
        if (!has_extra_decimals) {
          remain = result % pows[i];
          has_extra_decimals = (remain > 0);
        }
        result = result / pows[i];
        scale -= (1<<i);
      }
    }
    if (scale != 0) {
      if (!has_extra_decimals) {
        remain = result % 10;
        has_extra_decimals = (remain > 0);
      }
      result = result / 10;
      scale -= 1;
    }
  }
  if (is_neg) {
    result = -result;
  }
  if (has_extra_decimals) {
    if ((cm & CM_CONST_TO_DECIMAL_INT_UP) != 0) {
      if (!is_neg) { result = result + 1; }
    } else if ((cm & CM_CONST_TO_DECIMAL_INT_DOWN) != 0) {
      if (is_neg) { result = result - 1; }
    }
  }
  res.from(result);
  return ret;
}

static int scale_const_decimalint_expr(const ObDecimalInt *decint, const int32_t int_bytes,
                                       const ObScale in_scale, const ObScale out_scale,
                                       const ObPrecision out_prec, const ObCastMode cast_mode,
                                       ObDecimalIntBuilder &res)
{
#define DO_SCALE(int_type)                                                                         \
  const int_type *v = reinterpret_cast<const int_type *>(decint);                                  \
  if (in_scale < out_scale) {                                                                      \
    ret = wide::scale_up_decimalint(*v, out_scale - in_scale, res);                                \
  } else if (OB_FAIL(scale_down_decimalint(*v, in_scale - out_scale, res,                          \
                                           cast_mode, has_extra_decimal))) {                       \
    LOG_WDIAG("scale down decimal int failed", K(ret));                                             \
  }

  int ret = OB_SUCCESS;
  bool has_extra_decimal = false;
  ObDecimalIntBuilder max_v, min_v;
  int32_t expected_int_bytes =
        wide::ObDecimalIntConstValue::get_int_bytes_by_precision(out_prec);
  min_v.from(wide::ObDecimalIntConstValue::get_min_lower(out_prec), expected_int_bytes);
  max_v.from(wide::ObDecimalIntConstValue::get_max_upper(out_prec), expected_int_bytes);
 if (in_scale != out_scale) {
    DISPATCH_WIDTH_TASK(int_bytes, DO_SCALE);
  } else {
    res.from(decint, int_bytes);
  }
  if (OB_FAIL(ret)) {
  } else if ((cast_mode & CM_CONST_TO_DECIMAL_INT_EQ) != 0 && has_extra_decimal) {
    res.from(max_v);
  } else {
    int cmp_max = 0, cmp_min = 0;
    if (OB_FAIL(wide::compare(res, min_v, cmp_min))) {
      LOG_WDIAG("compare failed", K(ret));
    } else if (OB_FAIL(wide::compare(res, max_v, cmp_max))) {
      LOG_WDIAG("compare failed", K(ret));
    } else if (cmp_max < 0 && cmp_min > 0) { // max(P, S) >= res >= min(P, S)
      if (expected_int_bytes > res.get_int_bytes()) {
        res.extend(expected_int_bytes);
      } else if (expected_int_bytes < res.get_int_bytes()) {
        res.truncate(expected_int_bytes);
      }
    } else if (cmp_max >= 0) {
      res.from(max_v);
    } else if (cmp_min <= 0) {
      res.from(min_v);
    }
  }

  return ret;
#undef DO_SCALE
}

int ObDatumCast::common_scale_decimalint(const ObDecimalInt *decint, const int32_t int_bytes,
                                         const ObScale in_scale, const ObScale out_scale,
                                         const ObPrecision out_prec, const ObCastMode cast_mode,
                                         ObDecimalIntBuilder &val)
{
  int ret = OB_SUCCESS;
  ObDecimalIntBuilder max_v, min_v;
  ObDecimalIntBuilder scaled_val;
  int cmp_min = 0, cmp_max = 0;
  if (OB_ISNULL(decint)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid null decimal int", K(ret), K(decint));
  } else if (CM_IS_CONST_TO_DECIMAL_INT(cast_mode)) {
    ret = scale_const_decimalint_expr(decint, int_bytes, in_scale, out_scale, out_prec, cast_mode, val);
  } else if (CM_IS_COLUMN_CONVERT(cast_mode) || CM_IS_EXPLICIT_CAST(cast_mode)) {
    int32_t check_int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(out_prec);
    max_v.from(wide::ObDecimalIntConstValue::get_max_upper(out_prec), check_int_bytes);
    min_v.from(wide::ObDecimalIntConstValue::get_min_lower(out_prec), check_int_bytes);
    if (OB_FAIL(
          wide::common_scale_decimalint(decint, int_bytes, in_scale, out_scale, scaled_val))) {
      LOG_WDIAG("scale decimal int failed", K(ret));
    } else if (OB_FAIL(wide::compare(scaled_val, min_v, cmp_min))) {
      LOG_WDIAG("compare failed", K(ret));
    } else if (OB_FAIL(wide::compare(scaled_val, max_v, cmp_max))) {
      LOG_WDIAG("compare failed", K(ret));
    } else if (cmp_min > 0 && cmp_max < 0) {
      ret = ObDatumCast::align_decint_precision_unsafe(scaled_val.get_decimal_int(), scaled_val.get_int_bytes(),
                                          check_int_bytes, val);
    } else if (cmp_min <= 0) {
      val.from(min_v);
    } else if (cmp_max >= 0) {
      val.from(max_v);
    } else {
      // do nothing
    }
  } else {
    if (OB_FAIL(
          wide::common_scale_decimalint(decint, int_bytes, in_scale, out_scale, scaled_val))) {
      LOG_WDIAG("scale decimal int failed", K(ret));
    }
    int32_t expected_int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(out_prec);
    if (OB_UNLIKELY(out_prec == PRECISION_UNKNOWN_YET)) {
      // tempory value may have unknown precision(-1), just set expected int bytes to input int_bytes
      expected_int_bytes = scaled_val.get_int_bytes();
      LOG_WDIAG("invalid out precision", K(out_prec), K(lbt()));
    }
    if (OB_FAIL(ret)) { // do nothing
    } else if (OB_FAIL(align_decint_precision_unsafe(scaled_val.get_decimal_int(),
                                                     scaled_val.get_int_bytes(), expected_int_bytes,
                                                     val))) {
      LOG_WDIAG("align decimal int precision failed", K(ret));
    }
  }
  return ret;
}

} //end namespace common
} //end namespace oceanbase