/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#include "lib/ob_errno.h"
#include "lib/charset/ob_dtoa.h"
#include "common/ob_object.h"
#include "lib/wide_integer/ob_wide_integer.h"
#include "lib/wide_integer/ob_wide_integer_str_funcs.h"

namespace oceanbase
{
namespace common
{
namespace wide
{
#define DEF_MIN(B, Signed, ...)                                                \
  const ObWideInteger<B, Signed>                                               \
      Limits<ObWideInteger<B, Signed>>::Signed##_min_v{__VA_ARGS__}

#define DEF_MAX(B, Signed, ...)                                                \
  const ObWideInteger<B, Signed>                                               \
      Limits<ObWideInteger<B, Signed>>::Signed##_max_v{__VA_ARGS__}

#define DEF_NATIVE_LIMITS(int_type, Signed)                                                        \
  const int_type Limits<int_type>::Signed##_min_v = std::numeric_limits<int_type>::min();          \
  const int_type Limits<int_type>::Signed##_max_v = std::numeric_limits<int_type>::max()

static const constexpr uint64_t S_MIN_V = static_cast<uint64_t>(INT64_MIN);
static const constexpr uint64_t S_MAX_V = static_cast<uint64_t>(INT64_MAX);
static const constexpr uint64_t US_MAX_V = UINT64_MAX;

DEF_MIN(128, signed, 0, S_MIN_V);
DEF_MAX(128, signed, US_MAX_V, S_MAX_V);

DEF_MIN(128, unsigned, 0, 0);
DEF_MAX(128, unsigned, US_MAX_V, US_MAX_V);

DEF_MIN(256, signed, 0, 0, 0, S_MIN_V);
DEF_MAX(256, signed, US_MAX_V, US_MAX_V, US_MAX_V, S_MAX_V);

DEF_MIN(256, unsigned, 0, 0, 0, 0);
DEF_MAX(256, unsigned, US_MAX_V, US_MAX_V, US_MAX_V, US_MAX_V);

DEF_MIN(512, signed, 0, 0, 0, 0, 0, 0, 0, S_MIN_V);
DEF_MAX(512, signed, US_MAX_V, US_MAX_V, US_MAX_V, US_MAX_V,
                     US_MAX_V, US_MAX_V, US_MAX_V, S_MAX_V);

DEF_MIN(1024, signed, 0, 0, 0, 0, 0, 0, 0, 0,
                      0, 0, 0, 0, 0, 0, 0, S_MIN_V);
DEF_MAX(1024, signed, US_MAX_V, US_MAX_V, US_MAX_V, US_MAX_V,
                      US_MAX_V, US_MAX_V, US_MAX_V, US_MAX_V,
                      US_MAX_V, US_MAX_V, US_MAX_V, US_MAX_V,
                      US_MAX_V, US_MAX_V, US_MAX_V, S_MAX_V);

DEF_MIN(1024, unsigned, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0);
DEF_MAX(1024, unsigned, US_MAX_V, US_MAX_V, US_MAX_V, US_MAX_V,
                        US_MAX_V, US_MAX_V, US_MAX_V, US_MAX_V,
                        US_MAX_V, US_MAX_V, US_MAX_V, US_MAX_V,
                        US_MAX_V, US_MAX_V, US_MAX_V, US_MAX_V);

DEF_NATIVE_LIMITS(int32_t, signed);
DEF_NATIVE_LIMITS(int64_t, signed);

#undef S_MAX_V
#undef S_MIN_V
#undef US_MAX_V
#undef DEF_MIN
#undef DEF_MAX

const ObDecimalInt *ObDecimalIntConstValue::MIN_DECINT[OB_MAX_DECIMAL_POSSIBLE_PRECISION + 1] = {nullptr};
const ObDecimalInt *ObDecimalIntConstValue::MAX_DECINT[OB_MAX_DECIMAL_POSSIBLE_PRECISION + 1] = {nullptr};

const ObDecimalInt *ObDecimalIntConstValue::MAX_UPPER[OB_MAX_DECIMAL_POSSIBLE_PRECISION + 1] = {nullptr};
const ObDecimalInt *ObDecimalIntConstValue::MIN_LOWER[OB_MAX_DECIMAL_POSSIBLE_PRECISION + 1] = {nullptr};

// init ObDecimalIntConstValue
int ObDecimalIntConstValue::init_const_values()
{
  int ret = OB_SUCCESS;
  static ObArenaAllocator alloctor(ObModIds::OB_PROXY_DECIMAL_INT_CONST);
  ObIAllocator *alloc = reinterpret_cast<ObIAllocator *>(&alloctor);

  // init mysql const values
  char buf[128] = {0};
  // mysql min/max
  for (int16_t precision = 1; OB_SUCC(ret) && precision <= OB_MAX_DECIMAL_POSSIBLE_PRECISION;
       precision++) {
    buf[0] = '-';
    for (int i = 1; i <= precision; i++) { buf[i] = '9'; }
    ObDecimalInt *min_decint = nullptr, *max_decint = nullptr;
    int32_t int_bytes = 0;
    int16_t calc_scale = 0, calc_precision = 0;
    // parse mysql_min
    if (OB_FAIL(wide::from_string(buf, precision + 1, *alloc, calc_scale, calc_precision,
                                  int_bytes, min_decint))) {
      COMMON_LOG(WDIAG, "failed to parse MYSQL_MIN", K(ret), K(precision));
    } else {
      OB_ASSERT(int_bytes == get_int_bytes_by_precision(precision));
      MIN_DECINT[precision] = min_decint;
    }
    // parse mysql_max
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(wide::from_string(buf + 1, precision, *alloc, calc_scale, calc_precision,
                                         int_bytes, max_decint))) {
      COMMON_LOG(WDIAG, "failed to parse MYSQL_MAX", K(ret), K(precision));
    } else {
      OB_ASSERT(int_bytes == get_int_bytes_by_precision(precision));
      MAX_DECINT[precision] = max_decint;
    }
  } // for end

  // mysql max + 1, min - 1
  for (int16_t precision = 1; OB_SUCC(ret) && precision <= OB_MAX_DECIMAL_POSSIBLE_PRECISION;
       precision++) {
    buf[0] = '-';
    buf[1] = '1';
    for (int i = 2; i <= precision + 1; i++) { buf[i] = '0'; }
    ObDecimalInt *min_decint = nullptr, *max_decint = nullptr;
    int32_t int_bytes = 0;
    int16_t calc_scale = 0, calc_precision = 0;
    // parse mysql_min_lower
    if (OB_FAIL(wide::from_string(buf, precision + 2, *alloc, calc_scale, calc_precision,
                                  int_bytes, min_decint))) {
      COMMON_LOG(WDIAG, "failed to parse MYSQL_MIN", K(ret), K(precision));
    } else {
      MIN_LOWER[precision] = min_decint;
    }
    // parse mysql_max
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(wide::from_string(buf + 1, precision + 1, *alloc, calc_scale,
                                         calc_precision, int_bytes, max_decint))) {
      COMMON_LOG(WDIAG, "failed to parse MYSQL_MAX", K(ret), K(precision));
    } else {
      MAX_UPPER[precision] = max_decint;
    }
  } // for end

  return ret;
}
const int512_t ObDecimalIntConstValue::MYSQL_DEC_INT_MIN =
  -get_scale_factor<int512_t>(OB_MAX_DECIMAL_POSSIBLE_PRECISION);
const int512_t ObDecimalIntConstValue::MYSQL_DEC_INT_MAX =
  get_scale_factor<int512_t>(OB_MAX_DECIMAL_POSSIBLE_PRECISION);
const int512_t ObDecimalIntConstValue::MYSQL_DEC_INT_MAX_AVAILABLE = MYSQL_DEC_INT_MAX - 1;
int common_scale_decimalint(const ObDecimalInt *decint, const int32_t int_bytes,
                            const ObScale in_scale, const ObScale out_scale,
                            ObDecimalIntBuilder &val, const bool is_trunc)
{
#define DO_SCALE(int_type)                                                                         \
  const int_type *v = reinterpret_cast<const int_type *>(decint);                                  \
  if (in_scale < out_scale) {                                                                      \
    ret = scale_up_decimalint(*v, out_scale - in_scale, val);                                      \
  } else {                                                                                         \
    ret = scale_down_decimalint(*v, in_scale - out_scale, is_trunc, val);                          \
  }
  int ret = OB_SUCCESS;
  COMMON_LOG(DEBUG, "scale decimalint", K(int_bytes), K(in_scale), K(out_scale), K(lbt()));
  if (OB_ISNULL(decint)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "invalid null decimal int", K(ret), K(decint));
  } else {
    DISPATCH_WIDTH_TASK(int_bytes, DO_SCALE)
  }
  return ret;
#undef DO_SCALE
}

int from_double(const double x, ObIAllocator &allocator, ObDecimalInt *&decint, int32_t &int_bytes,
                int16_t &scale)
{
  int ret = OB_SUCCESS;
  char buf[MAX_DOUBLE_PRINT_SIZE] = {0};
  int64_t length = 0;
  int16_t precision = 0; // useless
  if (isnan(x) || (x == -INFINITY) || (x == INFINITY)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "invalid input", K(ret), K(x));
  } else {
    length = ob_gcvt(x, OB_GCVT_ARG_DOUBLE, sizeof(buf) - 1, buf, NULL);
    ret = wide::from_string(buf, length, allocator, scale, precision, int_bytes, decint);
  }
  return ret;
}

#define CHECK_RANGE_IS_VALID_INT64(TYPE)       \
  case sizeof(TYPE##_t): {                     \
    if (*(decint->TYPE##_v_) < INT64_MIN       \
        || *(decint->TYPE##_v_) > INT64_MAX) { \
      is_valid_int64 = false;                  \
    } else {                                   \
      res_val = *(decint->int64_v_);           \
    }                                          \
    break;                                     \
  }

int check_range_valid_int64(
    const ObDecimalInt *decint, const int32_t int_bytes,
    bool &is_valid_int64, int64_t &res_val) // scale is regarded as 0
{
  int ret = OB_SUCCESS;
  is_valid_int64 = true;
  if (0 == int_bytes) {
    res_val = 0;
  } else if (sizeof(int32_t) == int_bytes) {
    res_val = *(decint->int32_v_);
  } else if (sizeof(int64_t) == int_bytes) {
    res_val = *(decint->int64_v_);
  } else {
    switch (int_bytes) {
      CHECK_RANGE_IS_VALID_INT64(int128)
      CHECK_RANGE_IS_VALID_INT64(int256)
      CHECK_RANGE_IS_VALID_INT64(int512)
      default: {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WDIAG, "int_bytes is unexpected", K(ret), K(int_bytes));
        break;
      }
    }
  }
  return ret;
}

#define CHECK_RANGE_IS_VALID_UINT64(TYPE)      \
  case sizeof(TYPE##_t): {                     \
    if (*(decint->TYPE##_v_) > UINT64_MAX) {   \
      is_valid_uint64 = false;                 \
    } else {                                   \
      res_val = *(decint->int64_v_);           \
    }                                          \
    break;                                     \
  }

int check_range_valid_uint64(
    const ObDecimalInt *decint, const int32_t int_bytes,
    bool &is_valid_uint64, uint64_t &res_val) // scale is regarded as 0
{
  int ret = OB_SUCCESS;
  is_valid_uint64 = true;
  if (is_negative(decint, int_bytes)) {
    is_valid_uint64 = false;
  } else {
    if (0 == int_bytes) {
      res_val = 0;
    } else if (sizeof(int32_t) == int_bytes) {
      res_val = *(decint->int32_v_);
    } else if (sizeof(int64_t) == int_bytes) {
      res_val = *(decint->int64_v_);
    } else {
      switch (int_bytes) {
        CHECK_RANGE_IS_VALID_UINT64(int128)
        CHECK_RANGE_IS_VALID_UINT64(int256)
        CHECK_RANGE_IS_VALID_UINT64(int512)
        default: {
          ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(WDIAG, "int_bytes is unexpected", K(ret), K(int_bytes));
          break;
        }
      }
    }
  }
  return ret;
}

} // end namespace wide
} // end namespace common
} // end namespace oceanbase
