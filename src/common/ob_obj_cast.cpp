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

#define USING_LOG_PREFIX COMMON

#include "common/ob_obj_cast.h"
#include <math.h>
#include "lib/charset/ob_dtoa.h"


namespace oceanbase
{
namespace common
{

static const int64_t MAX_FLOAT_PRINT_SIZE = 64;
static const int64_t MAX_DOUBLE_PRINT_SIZE = 64;

static int identity(const ObObjType expect_type,
                    ObObjCastParams &params,
                    const ObObj &in,
                    ObObj &out,
                    const ObCastMode cast_mode)
{
  UNUSED(expect_type);
  UNUSED(params);
  UNUSED(cast_mode);
  if (&in != &out) {
    out = in;
  }
  return OB_SUCCESS;
}

static int not_support(const ObObjType expect_type,
                       ObObjCastParams &params,
                       const ObObj &in,
                       ObObj &out,
                       const ObCastMode cast_mode)
{
  UNUSED(params);
  LOG_WARN("not supported obj type convert" , K(expect_type), K(in), K(out), K(cast_mode));
  return OB_NOT_SUPPORTED;
}

static int not_expected(const ObObjType expect_type,
                        ObObjCastParams &params,
                        const ObObj& in,
                        ObObj& out,
                        const ObCastMode cast_mode)
{
  UNUSED(params);
  LOG_WARN("not expected obj type convert", K(expect_type), K(in), K(out), K(cast_mode));
  return OB_ERR_UNEXPECTED;
}

static int unknown_other(const ObObjType expect_type,
                         ObObjCastParams &params,
                         const ObObj &in,
                         ObObj &out,
                         const ObCastMode cast_mode)
{
  return not_support(expect_type, params, in, out, cast_mode);
}

////////////////////////////////////////////////////////////////
// Utility func

static int print_varchar(ObString &str, const char *format, ...)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str.ptr()) ||
      OB_UNLIKELY(str.size() <= 0)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("output buffer for varchar not enough",
        K(ret), K(str.ptr()), K(str.size()));
  } else {
    va_list args;
    va_start(args, format);
    int32_t length = vsnprintf(str.ptr(), str.size(), format, args);
    va_end(args);
    if (OB_UNLIKELY(length < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to snprintf string", K(ret), K(length));
    } else if (OB_UNLIKELY(length >= str.size())) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("output buffer for varchar not enough", K(str.size()), K(length));
    } else {
      // need not care the result, we have judged the length above.
      str.set_length(length);
    }
  }
  return ret;
}

static int copy_string(const ObObjCastParams &params,
                       const ObObjType type,
                       const char *str,
                       int64_t len,
                       ObObj &obj)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  if (OB_LIKELY(len > 0 && NULL != str)) {
    if (OB_LIKELY(NULL != params.zf_info_) && params.zf_info_->need_zerofill_) {
      int64_t str_len = std::max(len, static_cast<int64_t>(params.zf_info_->max_length_));
      if (OB_UNLIKELY(NULL == (buf = static_cast<char*>(params.alloc(str_len))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        int64_t zf = params.zf_info_->max_length_ - len;
        if (zf > 0) {
          MEMSET(buf, '0', zf);
          MEMCPY(buf + zf, str, len);
          len = str_len; // set string length
        } else {
          MEMCPY(buf, str, len);
        }
      }
    } else {
      if (OB_UNLIKELY(NULL == (buf = static_cast<char*>(params.alloc(len))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        MEMCPY(buf, str, len);
      }
    }
  }
  obj.set_string(type, buf, static_cast<int32_t>(len));
  return ret;
}

static int copy_string(const ObObjCastParams &params,
                       const ObObjType type,
                       const ObString &str,
                       ObObj &obj)
{
  return copy_string(params, type, str.ptr(), str.length(), obj);
}


/*
 * check err when a string cast to int/uint/double/float according to endptr
 *@str input string
 *@endptr result pointer to end of converted string
 *@len length of str
 *@err
 * incorrect -> "a232"->int or "a2323.23"->double
 * truncated -> "23as" -> int or "as2332.a"->double
 * @note
 *  This is called after one has called strntoull10rnd() or strntod function.
 */
static int check_convert_str_err(const char *str,
                                 const char *endptr,
                                 const int32_t len,
                                 const int err)
{
  int ret = OB_SUCCESS;
  // 1. only one of str and endptr is null, it is invalid input.
  if ((OB_ISNULL(str) || OB_ISNULL(endptr)) && str != endptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null pointer(s)", K(ret), K(str), K(endptr));
  } else
  // 2. str == endptr include NULL == NULL.
  if (OB_UNLIKELY(str == endptr) || OB_UNLIKELY(EDOM == err)) {
    ret = OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD; //1366
  } else {
    // 3. so here we are sure that both str and endptr are not NULL.
    endptr += ObCharset::scan_str(endptr, str + len, OB_SEQ_SPACES);
    endptr += ObCharset::scan_str(endptr, str + len, OB_SEQ_INTTAIL);
    if (endptr < str + len) {
      ret = OB_ERR_DATA_TRUNCATED; //1265
    }
  }
  return ret;
}

static int convert_string_collation(const ObString &in,
                                    const ObCollationType in_collation,
                                    ObString &out,
                                    const ObCollationType out_collation,
                                    ObObjCastParams &params)
{
  int ret = OB_SUCCESS;
  
  if (!ObCharset::is_valid_collation(in_collation)
      || !ObCharset::is_valid_collation(out_collation)
      || ObCharset::charset_type_by_coll(in_collation) == CHARSET_BINARY
      || ObCharset::charset_type_by_coll(out_collation) == CHARSET_BINARY
      || (ObCharset::charset_type_by_coll(in_collation) == ObCharset::charset_type_by_coll(out_collation))) {
    out = in;
  } else if (in.empty()) {
    out.reset();
  } else {
    char* buf = NULL;
    const int32_t CharConvertFactorNum = 4;
    int32_t buf_len = in.length() * CharConvertFactorNum;
    uint32_t result_len = 0;
    if (OB_ISNULL(buf = static_cast<char*>(params.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc memory failed", K(ret));
    } else if (OB_FAIL(ObCharset::charset_convert(in_collation,
                                                  in.ptr(),
                                                  in.length(),
                                                  out_collation,
                                                  buf,
                                                  buf_len,
                                                  result_len))) {
      LOG_WARN("charset convert failed", K(ret));
    } else {
      out.assign_ptr(buf, result_len);
    }
  }
  LOG_DEBUG("convert_string_collation", K(in.length()), K(in_collation), K(out.length()), K(out_collation));
  
  return ret;
}

////////////////////////////////////////////////////////////////

OB_INLINE int get_cast_ret(const ObCastMode cast_mode,
                           int ret,
                           int &warning)
{
  if (OB_SUCCESS != ret &&
      //OB_ERR_UNEXPECTED != ret &&
      CM_IS_WARN_ON_FAIL(cast_mode)) {
    warning = ret;
    ret = OB_SUCCESS;
  }
  return ret;
}

#define CAST_FAIL(stmt) \
  (OB_UNLIKELY((OB_SUCCESS != (ret = get_cast_ret(cast_mode, (stmt), params.warning_)))))

#define SET_RES_OBJ(res, func_val, obj_type, comma, val, zero_val)      \
  do {                                                                  \
    if (OB_SUCC(ret)) {                                            \
      if (OB_SUCCESS == params.warning_                                 \
          || OB_ERR_TRUNCATED_WRONG_VALUE == params.warning_            \
          || OB_DATA_OUT_OF_RANGE == params.warning_                    \
          || OB_ERR_DATA_TRUNCATED == params.warning_                   \
          || OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD == params.warning_) { \
        res.set_##func_val(obj_type comma val);                         \
      } else if (CM_IS_ZERO_ON_WARN(cast_mode)) {                       \
        res.set_##func_val(obj_type comma zero_val);                    \
      } else {                                                          \
        res.set_null();                                                 \
      }                                                                 \
    } else {                                                            \
      res.set_##func_val(obj_type comma val);                           \
    }                                                                   \
  } while (0)

#define COMMA ,
#define SET_RES_INT(res)        SET_RES_OBJ(res, int, expect_type, COMMA, value, 0)
#define SET_RES_UINT(res)       SET_RES_OBJ(res, uint, expect_type, COMMA, value, 0)
#define SET_RES_FLOAT(res)      SET_RES_OBJ(res, float, expect_type, COMMA, value, 0.0)
#define SET_RES_DOUBLE(res)     SET_RES_OBJ(res, double, expect_type, COMMA, value, 0.0)
#define SET_RES_NUMBER(res)     SET_RES_OBJ(res, number, expect_type, COMMA, value, (value.set_zero(), value))
#define SET_RES_DATETIME(res)   SET_RES_OBJ(res, datetime, expect_type, COMMA, value, ObTimeConverter::ZERO_DATETIME)
#define SET_RES_DATE(res)       SET_RES_OBJ(res, date, , , value, ObTimeConverter::ZERO_DATE)
#define SET_RES_TIME(res)       SET_RES_OBJ(res, time, , , value, ObTimeConverter::ZERO_TIME)
#define SET_RES_YEAR(res)       SET_RES_OBJ(res, year, , , value, ObTimeConverter::ZERO_YEAR)
#define SET_RES_OTIMESTAMP(res) SET_RES_OBJ(res, otimestamp_value, expect_type, COMMA, value, ObOTimestampData())


#define SET_RES_ACCURACY(res_precision, res_scale, res_length) \
  if (params.res_accuracy_ != NULL && OB_SUCCESS == ret) {\
    params.res_accuracy_->set_scale(res_scale);\
    params.res_accuracy_->set_precision(res_precision);\
    params.res_accuracy_->set_length(res_length);\
  }

#define SET_RES_ACCURACY_STRING(type, res_precision, res_length) \
  if (params.res_accuracy_ != NULL && OB_SUCCESS == ret) {       \
    params.res_accuracy_->set_precision(res_precision);          \
    params.res_accuracy_->set_length(res_length);                \
    if (ob_is_text_tc(type)) {                                   \
      params.res_accuracy_->set_scale(DEFAULT_SCALE_FOR_TEXT);   \
    } else {                                                     \
      params.res_accuracy_->set_scale(DEFAULT_SCALE_FOR_STRING); \
    }                                                            \
  }
  
////////////////////////////////////////////////////////////////
// range check function templates.

// check with given lower and upper limit.
template <typename InType, typename OutType>
OB_INLINE int numeric_range_check(const InType in_val,
                                  const OutType min_out_val,
                                  const OutType max_out_val,
                                  OutType &out_val)
{
  int ret = OB_SUCCESS;
  if (in_val < static_cast<InType>(min_out_val)) {
    ret = OB_DATA_OUT_OF_RANGE;
    out_val = min_out_val;
  } else if (in_val > static_cast<InType>(max_out_val)) {
    ret = OB_DATA_OUT_OF_RANGE;
    out_val = max_out_val;
  }
  return ret;
}

// explicit for int_uint check, because we need use out_val to compare with max_out_val instead
// of in_val, since we can't cast UINT64_MAX to int64.
template <>
OB_INLINE int numeric_range_check<int64_t, uint64_t>(const int64_t in_val,
                                                     const uint64_t min_out_val,
                                                     const uint64_t max_out_val,
                                                     uint64_t &out_val)
{
  int ret = OB_SUCCESS;
  if (in_val < 0) {
    ret = OB_DATA_OUT_OF_RANGE;
    out_val = 0;
  } else if (out_val > max_out_val) {
    ret = OB_DATA_OUT_OF_RANGE;
    out_val = max_out_val;
  }
  UNUSED(min_out_val);
  return ret;
}

// check if is negative only.
template <typename OutType>
OB_INLINE int numeric_negative_check(OutType &out_val)
{
  int ret = OB_SUCCESS;
  if (out_val < static_cast<OutType>(0)) {
    ret = OB_DATA_OUT_OF_RANGE;
    out_val = static_cast<OutType>(0);
  }
  return ret;
}

// explicit for number check.
template <>
OB_INLINE int numeric_negative_check<number::ObNumber>(number::ObNumber &out_val)
{
  int ret = OB_SUCCESS;
  if (out_val.is_negative()) {
    ret = OB_DATA_OUT_OF_RANGE;
    out_val.set_zero();
  }
  return ret;
}

// check upper limit only.
template <typename InType, typename OutType>
OB_INLINE int numeric_upper_check(const InType in_val,
                                  const OutType max_out_val,
                                  OutType &out_val)
{
  int ret = OB_SUCCESS;
  if (in_val > static_cast<InType>(max_out_val)) {
    ret = OB_DATA_OUT_OF_RANGE;
    out_val = max_out_val;
  }
  return ret;
}

template <typename InType>
OB_INLINE int int_range_check(const ObObjType out_type,
                              const InType in_val,
                              int64_t &out_val)
{
  return numeric_range_check(in_val, INT_MIN_VAL[out_type], INT_MAX_VAL[out_type], out_val);
}

template <typename InType>
OB_INLINE int int_upper_check(const ObObjType out_type,
                              InType in_val,
                              int64_t &out_val)
{
  return numeric_upper_check(in_val, INT_MAX_VAL[out_type], out_val);
}

OB_INLINE int uint_upper_check(const ObObjType out_type, uint64_t &out_val)
{
  return numeric_upper_check(out_val, UINT_MAX_VAL[out_type], out_val);
}

template <typename InType>
OB_INLINE int uint_range_check(const ObObjType out_type,
                               const InType in_val,
                               uint64_t &out_val)
{
  return numeric_range_check(in_val, static_cast<uint64_t>(0),
                             UINT_MAX_VAL[out_type], out_val);
}

template <typename InType, typename OutType>
OB_INLINE int real_range_check(const ObObjType out_type,
                               const InType in_val,
                               OutType &out_val)
{
  return numeric_range_check(in_val, static_cast<OutType>(REAL_MIN_VAL[out_type]),
                             static_cast<OutType>(REAL_MAX_VAL[out_type]), out_val);
}

template <typename Type>
int real_range_check(const ObAccuracy &accuracy, Type &value)
{
  int ret = OB_SUCCESS;
  const ObPrecision precision = accuracy.get_precision();
  const ObScale scale = accuracy.get_scale();
  if (OB_LIKELY(precision > 0) &&
      OB_LIKELY(scale >= 0) &&
      OB_LIKELY(precision >= scale)) {
    Type integer_part = static_cast<Type>(pow(10.0, static_cast<double>(precision - scale)));
    Type decimal_part = static_cast<Type>(pow(10.0, static_cast<double>(scale)));
    Type max_value = integer_part - 1 / decimal_part;
    Type min_value = -max_value;
    if (OB_FAIL(numeric_range_check(value, min_value, max_value, value))) {
    } else {
      value = static_cast<Type>(round(value * decimal_part) / decimal_part);
    }
  }
  return ret;
}

#define BOUND_INFO_START_POS 18

template<typename T>
ObPrecision get_precision_for_integer(T value)
{
  static const uint64_t bound_info[] = {
      INT64_MAX + 1ULL,
      999999999999999999ULL,
      99999999999999999ULL,
      9999999999999999ULL,
      999999999999999ULL,
      99999999999999ULL,
      9999999999999ULL,
      999999999999ULL,
      99999999999ULL,
      9999999999ULL,
      999999999ULL,
      99999999ULL,
      9999999ULL,
      999999ULL,
      99999ULL,
      9999ULL,
      999ULL,
      99ULL,
      9ULL,
      99ULL,
      999ULL,
      9999ULL,
      99999ULL,
      999999ULL,
      9999999ULL,
      99999999ULL,
      999999999ULL,
      9999999999ULL,
      99999999999ULL,
      999999999999ULL,
      9999999999999ULL,
      99999999999999ULL,
      999999999999999ULL,
      9999999999999999ULL,
      99999999999999999ULL,
      999999999999999999ULL,
      9999999999999999999ULL,
      UINT64_MAX,
    };
  int64_t flag = std::less<T>()(value, 0) ? -1 : 1;
  uint64_t abs_value = value * flag;
  const uint64_t *iter = bound_info + BOUND_INFO_START_POS;
  while(abs_value > *iter) {
    iter += flag;
  }
  // *(iter - 1) < abs <= *iter
  return static_cast<ObPrecision>((iter - (bound_info + BOUND_INFO_START_POS)) * flag + 1);
}

int ObHexUtils::unhex(const ObString &text, ObCastCtx &cast_ctx, ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString str_result;
  char *buf = NULL;
  const bool need_fill_zero = (1 == text.length() % 2);
  const int32_t tmp_length = text.length() / 2 + need_fill_zero;
  int32_t alloc_length = (0 == tmp_length ? 1 : tmp_length);
  if (OB_ISNULL(cast_ctx.allocator_v2_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator in cast ctx is NULL", K(ret), K(text));
  } else if (OB_ISNULL(buf = static_cast<char *>(cast_ctx.allocator_v2_->alloc(alloc_length)))) {
    result.set_null();
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed", K(alloc_length), K(ret));
  } else {
    int32_t i = 0;
    char c1 = 0;
    char c2 = 0;
    if (text.length() > 0) {
      if (need_fill_zero) {
        c1 = '0';
        c2 = text[0];
        i = 0;
      } else {
        c1 = text[0];
        c2 = text[1];
        i = 1;
      }
    }
    while (OB_SUCC(ret) && i < text.length()) {
      if (isxdigit(c1) && isxdigit(c2)) {
        buf[i / 2] = (char)((get_xdigit(c1) << 4) | get_xdigit(c2));
        c1 = text[++i];
        c2 = text[++i];
      } else {
        ret = OB_ERR_INVALID_HEX_NUMBER;
        LOG_WARN("invalid hex number", K(ret), K(c1), K(c2), K(text));
      }
    }

    if (OB_SUCC(ret)) {
      str_result.assign_ptr(buf, tmp_length);
      result.set_varchar(str_result);
    }
  }
  return ret;
}

int ObHexUtils::hex(const ObString &text, ObCastCtx &cast_ctx, ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString str_result;
  char* buf = NULL;
  const int32_t alloc_length = text.empty() ? 1 : text.length() * 2;
  if (OB_ISNULL(cast_ctx.allocator_v2_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator in cast ctx is NULL", K(ret), K(text));
  } else if (OB_ISNULL(buf = static_cast<char*>(cast_ctx.allocator_v2_->alloc(alloc_length)))) {
    result.set_null();
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed", K(ret), K(alloc_length));
  } else {
    static const char* HEXCHARS = "0123456789ABCDEF";
    int32_t pos = 0;
    for (int32_t i = 0; i < text.length(); ++i) {
      buf[pos++] = HEXCHARS[text[i] >> 4 & 0xF];
      buf[pos++] = HEXCHARS[text[i] & 0xF];
    }
    str_result.assign_ptr(buf, pos);
    result.set_varchar(str_result);
    LOG_DEBUG("succ to hex", K(text), "length", text.length(), K(str_result));
  }
  return ret;
}

int ObHexUtils::hex_for_mysql(const uint64_t uint_val, common::ObCastCtx &cast_ctx, common::ObObj &result)
{

  int ret = OB_SUCCESS;
  char* buf = NULL;
  const int32_t MAX_INT64_LEN = 20;
  if (OB_ISNULL(cast_ctx.allocator_v2_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator in cast ctx is NULL", K(ret), K(uint_val));
  } else if (OB_ISNULL(buf = static_cast<char*>(cast_ctx.allocator_v2_->alloc(MAX_INT64_LEN)))) {
    result.set_null();
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed", K(ret));
  } else {
    int pos = snprintf(buf, MAX_INT64_LEN, "%lX", uint_val);
    if (OB_UNLIKELY(pos <= 0) || OB_UNLIKELY(pos >= MAX_INT64_LEN)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_ERROR("size is overflow", K(ret), K(uint_val));
    } else {
      ObString str_result(pos, buf);
      result.set_varchar(str_result);
    }
  }
  return ret;
}

// https://docs.oracle.com/en/database/oracle/oracle-database/12.2/sqlrf/RAWTOHEX.html
// As a SQL built-in function, RAWTOHEX accepts an argument of any scalar data type other than LONG,
// LONG RAW, CLOB, NCLOB, BLOB, or BFILE. If the argument is of a data type other than RAW,
// then this function converts the argument value, which is represented using some number of data bytes,
// into a RAW value with the same number of data bytes. The data itself is not modified in any way,
// but the data type is recast to a RAW data type.
int ObHexUtils::rawtohex(const ObObj &text, ObCastCtx &cast_ctx, ObObj &result)
{
  int ret = OB_SUCCESS;

  if (text.is_null()) {
    result.set_null();
  } else {
    ObString str;
    ObObj num_obj;
    char* splice_num_str = NULL;  // for splice Desc and degits_ of number.
    ObOTimestampData time_value;
    switch (text.get_type()) {
      // TODO::this should same as oracle, and support dump func
      case ObTinyIntType:
      case ObSmallIntType:
      case ObInt32Type:
      case ObIntType: {
        int64_t int_value = text.get_int();
        number::ObNumber nmb;
        if (OB_FAIL(nmb.from(int_value, cast_ctx))) {
          LOG_WARN("fail to int_number", K(ret), K(int_value), "type", text.get_type());
        } else {
          num_obj.set_number(ObNumberType, nmb);
          int32_t alloc_len =
              static_cast<int32_t>(sizeof(num_obj.get_number_desc()) + num_obj.get_number_byte_length());
          if (OB_ISNULL(cast_ctx.allocator_v2_)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("allocator in cast ctx is NULL", K(ret));
          } else if (OB_ISNULL(splice_num_str = static_cast<char*>(cast_ctx.allocator_v2_->alloc(alloc_len)))) {
            result.set_null();
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_ERROR("alloc memory failed", K(ret), K(alloc_len));
          } else {
            MEMCPY(splice_num_str, &(num_obj.get_number_desc()), sizeof(num_obj.get_number_desc()));
            MEMCPY(splice_num_str + sizeof(num_obj.get_number_desc()),
                num_obj.get_data_ptr(),
                num_obj.get_number_byte_length());
            str.assign_ptr(static_cast<const char*>(splice_num_str), alloc_len);
          }
          LOG_DEBUG("succ to int_number", K(ret), K(int_value), "type", num_obj.get_type(), K(nmb), K(str));
        }
        break;
      }
      case ObNumberFloatType:
      case ObNumberType: {
        int32_t alloc_len = static_cast<int32_t>(sizeof(text.get_number_desc()) + text.get_number_byte_length());
        if (OB_ISNULL(cast_ctx.allocator_v2_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("allocator in cast ctx is NULL", K(ret));
        } else if (OB_ISNULL(splice_num_str = static_cast<char*>(cast_ctx.allocator_v2_->alloc(alloc_len)))) {
          result.set_null();
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("alloc memory failed", K(ret), K(alloc_len));
        } else {
          MEMCPY(splice_num_str, &(text.get_number_desc()), sizeof(text.get_number_desc()));
          MEMCPY(splice_num_str + sizeof(text.get_number_desc()), text.get_data_ptr(), text.get_number_byte_length());
          str.assign_ptr(static_cast<const char*>(splice_num_str), alloc_len);
        }
        break;
      }
      case ObDateTimeType: {
        str.assign_ptr(static_cast<const char*>(text.get_data_ptr()), static_cast<int32_t>(sizeof(int64_t)));
        break;
      }
      case ObNVarchar2Type:
      case ObNCharType:
      case ObVarcharType:
      case ObCharType:
      case ObLongTextType:
      case ObRawType: {
        // https://www.techonthenet.com/oracle/functions/rawtohex.php
        // NOTE:: when convert string to raw, Oracle use utl_raw.cast_to_raw(), while PL/SQL use hextoraw()
        //       here we use utl_raw.cast_to_raw(), as we can not distinguish in which SQL
        str = text.get_varbinary();
        break;
      }
      case ObTimestampTZType:
      case ObTimestampLTZType:
      case ObTimestampNanoType: {
        time_value = text.get_otimestamp_value();
        str.assign_ptr(reinterpret_cast<char*>(&time_value), static_cast<int32_t>(text.get_otimestamp_store_size()));
        break;
      }
      default: {
        ret = OB_ERR_INVALID_HEX_NUMBER;
        LOG_WARN("invalid hex number", K(ret), K(text), "type", text.get_type());
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(hex(str, cast_ctx, result))) {
        LOG_WARN("fail to convert to hex", K(ret), K(str));
      } else {
        result.set_default_collation_type();
        LOG_DEBUG("succ to rawtohex", "type", text.get_type(), K(text), K(result), K(lbt()));
      }
    }
  }
  
  return ret;
}

int ObHexUtils::hextoraw(const ObObj &text, ObCastCtx &cast_ctx, ObObj &result)
{
  int ret = OB_SUCCESS;
  if (text.is_null()) {
    result.set_null();
  } else if (text.is_numeric_type()) {
    number::ObNumber nmb_val;
    if (OB_FAIL(get_uint(text, cast_ctx, nmb_val))) {
      LOG_WARN("fail to get uint64", K(ret), K(text));
    } else if (OB_FAIL(uint_to_raw(nmb_val, cast_ctx, result))) {
      LOG_WARN("fail to convert to hex", K(ret), K(nmb_val));
    }
  } else if (text.is_raw()) {
    // fast path
    if (OB_FAIL(copy_raw(text, cast_ctx, result))) {
      LOG_WARN("fail to convert to hex", K(ret), K(text));
    }
  } else if (text.is_character_type() || text.is_varbinary_or_binary()) {
    ObString utf8_string;
    if (OB_FAIL(convert_string_collation(text.get_string(),
                                         text.get_collation_type(),
                                         utf8_string,
                                         ObCharset::get_system_collation(),
                                         cast_ctx))) {
      LOG_WARN("convert_string_collation", K(ret));
    } else if (OB_FAIL(unhex(utf8_string, cast_ctx, result))) {
      LOG_WARN("fail to convert to hex", K(ret), K(text));
    } else {
      result.set_raw(result.get_raw());
    }
  } else {
    ret = OB_ERR_INVALID_HEX_NUMBER;
    LOG_WARN("invalid hex number", K(ret), K(text));
  }
  
  return ret;
}

int ObHexUtils::get_uint(const ObObj &obj, ObCastCtx &cast_ctx, number::ObNumber &out)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ob_is_accurate_numeric_type(obj.get_type()))) {
    ret = OB_ERR_INVALID_HEX_NUMBER;
    LOG_WARN("invalid hex number", K(ret), K(obj));
  } else if (obj.is_number() || obj.is_unumber()) {
    const number::ObNumber& value = obj.get_number();
    if (OB_FAIL(out.from(value, cast_ctx))) {
      LOG_WARN("deep copy failed", K(ret), K(obj));
    } else if (OB_UNLIKELY(!out.is_integer()) || OB_UNLIKELY(out.is_negative())) {
      ret = OB_ERR_INVALID_HEX_NUMBER;
      LOG_WARN("invalid hex number", K(ret), K(out));
    } else if (OB_FAIL(out.round(0))) {
      LOG_WARN("round failed", K(ret), K(out));
    }
  } else {
    if (OB_UNLIKELY(obj.get_int() < 0)) {
      ret = OB_ERR_INVALID_HEX_NUMBER;
      LOG_WARN("invalid hex number", K(ret), K(obj));
    } else if (OB_FAIL(out.from(obj.get_int(), cast_ctx))) {
      LOG_WARN("deep copy failed", K(ret), K(obj));
    }
  }
  return ret;
}

int ObHexUtils::uint_to_raw(const number::ObNumber &uint_num, ObCastCtx &cast_ctx, ObObj &result)
{
  int ret = OB_SUCCESS;
  const int64_t oracle_max_avail_len = 40;
  char uint_buf[number::ObNumber::MAX_TOTAL_SCALE] = {0};
  int64_t uint_pos = 0;
  ObString uint_str;
  if (OB_FAIL(uint_num.format(uint_buf, number::ObNumber::MAX_TOTAL_SCALE, uint_pos, 0))) {
    LOG_WARN("fail to format ", K(ret), K(uint_num));
  } else if (uint_pos > oracle_max_avail_len) {
    ret = OB_ERR_INVALID_HEX_NUMBER;
    LOG_WARN("invalid hex number", K(ret), K(uint_pos), K(oracle_max_avail_len), K(uint_num));
  } else {
    uint_str.assign_ptr(uint_buf, static_cast<int32_t>(uint_pos));
    if (OB_FAIL(unhex(uint_str, cast_ctx, result))) {
      LOG_WARN("fail to str_to_raw", K(ret), K(result));
    } else {
      result.set_raw(result.get_raw());
    }
  }
  return ret;
}

int ObHexUtils::copy_raw(const common::ObObj &obj, common::ObCastCtx &cast_ctx, common::ObObj &result)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  const ObString& value = obj.get_raw();
  const int32_t alloc_length = value.empty() ? 1 : value.length();
  if (OB_ISNULL(cast_ctx.allocator_v2_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator in cast ctx is NULL", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char*>(cast_ctx.allocator_v2_->alloc(alloc_length)))) {
    result.set_null();
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed", K(ret), K(alloc_length));
  } else {
    MEMCPY(buf, value.ptr(), value.length());
    result.set_raw(buf, value.length());
  }
  return ret;
}

static int check_convert_string(const ObObjType expect_type,
                                ObObjCastParams &params,
                                const ObObj &in,
                                ObObj &out)
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode() && ob_is_blob(expect_type, params.expect_obj_collation_) && !in.is_blob() && !in.is_raw()) {
    if (in.is_varchar_or_char()) {
      if (OB_FAIL(ObHexUtils::hextoraw(in, params, out))) {
        LOG_WARN("fail to hextoraw for blob", K(ret), K(in));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_ERROR("Invalid use of blob type", K(ret), K(in), K(expect_type));
    }
  } else {
    out = in;
  }
  return ret;
}

static int check_convert_string(const ObObjType expect_type,
                                ObObjCastParams &params,
                                const ObString &in_string,
                                ObObj &out)
{
  ObObj tmp_obj;
  tmp_obj.set_varchar(in_string);
  return check_convert_string(expect_type, params, tmp_obj, out);
}

////////////////////////////////////////////////////////////////
// Int -> XXX

static const double ROUND_DOUBLE = 0.5;

static int int_int(const ObObjType expect_type, ObObjCastParams &params, const ObObj &in,
                   ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObIntTC != in.get_type_class()
                  || ObIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    int64_t value = in.get_int();
    if (in.get_type() > expect_type && CAST_FAIL(int_range_check(expect_type, value, value))) {
    } else {
      out.set_int(expect_type, value);
      res_precision = get_precision_for_integer(value);
    }
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int int_uint(const ObObjType expect_type, ObObjCastParams &params, const ObObj &in,
                    ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObIntTC != in.get_type_class()
                  || ObUIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (CM_SKIP_CAST_INT_UINT(cast_mode)) {
    out = in;
    res_precision = get_precision_for_integer(out.get_uint64());
  } else {
    uint64_t value = static_cast<uint64_t>(in.get_int());
    if (CM_NEED_RANGE_CHECK(cast_mode)
        && CAST_FAIL(uint_range_check(expect_type, in.get_int(), value))) {
    } else {
      out.set_uint(expect_type, value);
      res_precision = get_precision_for_integer(out.get_uint64());
    }
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int int_float(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObIntTC != in.get_type_class()
                  || ObFloatTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    float value = static_cast<float>(in.get_int());
    if (ObUFloatType == expect_type && CAST_FAIL(numeric_negative_check(value))) {
    } else {
      out.set_float(expect_type, value);
    }
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  return ret;
}

static int int_double(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObIntTC != in.get_type_class()
                  || ObDoubleTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    double value = static_cast<double>(in.get_int());
    if (ObUDoubleType == expect_type && CAST_FAIL(numeric_negative_check(value))) {
    } else {
      out.set_double(expect_type, value);
    }
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  return ret;
}

static int int_number(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObIntTC != in.get_type_class()
                  || ObNumberTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    int64_t value = in.get_int();
    number::ObNumber nmb;
    if (ObUNumberType == expect_type && CAST_FAIL(numeric_negative_check(value))) {
    } else if (OB_FAIL(nmb.from(value, params))) {
    } else {
      out.set_number(expect_type, nmb);
      res_precision = get_precision_for_integer(in.get_int());
    }
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int int_datetime(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObIntTC != in.get_type_class()
                  || ObDateTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const ObTimeZoneInfo *tz_info = (ObTimestampType == expect_type) ? params.dtc_params_.tz_info_ : NULL;
    int64_t value = 0;
    if (in.get_int() < 0) {
      CAST_FAIL(OB_INVALID_DATE_FORMAT);
    } else if (CAST_FAIL(ObTimeConverter::int_to_datetime(in.get_int(), 0, tz_info, value))) {
    } else {
      SET_RES_DATETIME(out);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, MIN_SCALE_FOR_TEMPORAL, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int int_date(const ObObjType expect_type, ObObjCastParams &params,
                    const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObIntTC != in.get_type_class()
                  || ObDateTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    int32_t value = 0;
    if (CAST_FAIL(ObTimeConverter::int_to_date(in.get_int(), value))) {
    } else {
      SET_RES_DATE(out);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, DEFAULT_SCALE_FOR_DATE, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int int_time(const ObObjType expect_type, ObObjCastParams &params,
                    const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObIntTC != in.get_type_class()
                  || ObTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    int64_t value = 0;
    if (CAST_FAIL(ObTimeConverter::int_to_time(in.get_int(), value))) {
    } else {
      SET_RES_TIME(out);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, MIN_SCALE_FOR_TEMPORAL, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int int_year(const ObObjType expect_type, ObObjCastParams &params,
                    const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObIntTC != in.get_type_class()
                  || ObYearTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    uint8_t value = 0;
    if (CAST_FAIL(ObTimeConverter::int_to_year(in.get_int(), value))) {
    } else {
      SET_RES_YEAR(out);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, DEFAULT_SCALE_FOR_YEAR, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int int_string(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  if (OB_UNLIKELY(ObIntTC != in.get_type_class()
                  || (ObStringTC != ob_obj_type_class(expect_type)
                  && ObTextTC != ob_obj_type_class(expect_type)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH];
    MEMSET(buf, 0, OB_CAST_TO_VARCHAR_MAX_LENGTH);
    ObString str(sizeof(buf), 0, buf);
    if (OB_FAIL(print_varchar(str, "%ld", in.get_int()))) {
    } else {
      ret = copy_string(params, expect_type, str, out);
      if (OB_SUCC(ret)) {
        res_length = static_cast<ObLength>(out.get_string_len());
      }
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_STRING, DEFAULT_SCALE_FOR_STRING, res_length);
  UNUSED(cast_mode);
  return ret;
}

////////////////////////////////////////////////////////////////
// UInt -> XXX

static int uint_int(const ObObjType expect_type, ObObjCastParams &params,
                    const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObUIntTC != in.get_type_class()
                  || ObIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (CM_IS_EXTERNAL_CALL(cast_mode) && CM_SKIP_CAST_INT_UINT(cast_mode)) {
    out = in;
    res_precision = get_precision_for_integer(out.get_int());
  } else {
    int64_t value = static_cast<int64_t>(in.get_uint64());
    if (CM_NEED_RANGE_CHECK(cast_mode)
        && CAST_FAIL(int_upper_check(expect_type, in.get_uint64(), value))) {
    } else {
      out.set_int(expect_type, value);
      res_precision = get_precision_for_integer(value);
    }
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int uint_uint(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObUIntTC != in.get_type_class()
                  || ObUIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    uint64_t value = in.get_uint64();
    if (in.get_type() > expect_type && CAST_FAIL(uint_upper_check(expect_type, value))) {
    } else {
      out.set_uint(expect_type, value);
      res_precision = get_precision_for_integer(value);
    }
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int uint_float(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObUIntTC != in.get_type_class()
                  || ObFloatTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    out.set_float(expect_type, static_cast<float>(in.get_uint64()));
  }
  UNUSED(cast_mode);
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  return ret;
}

static int uint_double(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObUIntTC != in.get_type_class()
                  || ObDoubleTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    out.set_double(expect_type, static_cast<double>(in.get_uint64()));
  }
  UNUSED(cast_mode);
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  return ret;
}

static int uint_number(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  number::ObNumber nmb;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObUIntTC != in.get_type_class()
                  || ObNumberTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(nmb.from(in.get_uint64(), params))) {
  } else {
    out.set_number(expect_type, nmb);
    res_precision = get_precision_for_integer(in.get_uint64());
  }
  UNUSED(cast_mode);
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int uint_datetime(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj int64;
  if (OB_UNLIKELY(ObUIntTC != in.get_type_class()
                  || ObDateTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(uint_int(ObIntType, params, in, int64, CM_UNSET_NO_CAST_INT_UINT(cast_mode)))) {
  } else if (OB_FAIL(int_datetime(expect_type, params, int64, out, cast_mode))) {
  }
  //has set the accuracy in prev int_datetime call
  return ret;
}

static int uint_date(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj int64;
  if (OB_UNLIKELY(ObUIntTC != in.get_type_class()
                  || ObDateTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(uint_int(ObIntType, params, in, int64, CM_UNSET_NO_CAST_INT_UINT(cast_mode)))) {
  } else if (OB_FAIL(int_date(expect_type, params, int64, out, cast_mode))) {
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, DEFAULT_SCALE_FOR_DATE, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int uint_time(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj int64;
  if (OB_UNLIKELY(ObUIntTC != in.get_type_class()
                  || ObTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(uint_int(ObIntType, params, in, int64, CM_UNSET_NO_CAST_INT_UINT(cast_mode)))) {
  } else if (OB_FAIL(int_time(expect_type, params, int64, out, cast_mode))) {
  }
  //accuracy has been set in prev int_time call
  return ret;
}

static int uint_year(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj int64;
  if (OB_UNLIKELY(ObUIntTC != in.get_type_class()
                  || ObYearTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(uint_int(ObIntType, params, in, int64, CM_UNSET_NO_CAST_INT_UINT(cast_mode)))) {
  } else if (OB_FAIL(int_year(expect_type, params, int64, out, cast_mode))) {
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, DEFAULT_SCALE_FOR_YEAR, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int uint_string(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH];
  MEMSET(buf, 0, OB_CAST_TO_VARCHAR_MAX_LENGTH);
  ObLength res_length = -1;
  ObString str(sizeof(buf), 0, buf);
  if (OB_UNLIKELY(ObUIntTC != in.get_type_class()
                  || (ObStringTC != ob_obj_type_class(expect_type)
                  && ObTextTC != ob_obj_type_class(expect_type)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(print_varchar(str, "%lu", in.get_uint64()))) {
  } else {
    ret = copy_string(params, expect_type, str, out);
    if (OB_SUCC(ret)) {
      res_length = static_cast<ObLength>(out.get_string_len());
    }
  }
  UNUSED(cast_mode);
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_STRING, DEFAULT_SCALE_FOR_STRING, res_length);
  return ret;
}

////////////////////////////////////////////////////////////////
// Float -> XXX

// directly stacit cast from float to int or uint:
// case 1: float = -99999896450971467776.000000, int64 = -9223372036854775808, uint64 = 9223372036854775808.
// case 2: float = 99999896450971467776.000000, int64 = -9223372036854775808, uint64 = 0.
// case 3: float = -99999904.000000, int64 = -99999904, uint64 = 18446744073609551712.
// case 4: float = 99999904.000000, int64 = 99999904, uint64 = 99999904.
// we can see that if float value is out of range of int or uint value, the casted int or uint
// value can't be used to compare with INT64_MAX and so on, see case 2.
// so we should use float value to determine weither it is in range of int or uint.

static int float_int(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  int64_t value = 0;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObFloatTC != in.get_type_class()
                  || ObIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    if (in.get_float() < 0) {
      value = static_cast<int64_t>(in.get_float() - ROUND_DOUBLE);
    } else if (in.get_float() > 0) {
      value = static_cast<int64_t>(in.get_float() + ROUND_DOUBLE);
    } else {
      value = static_cast<int64_t>(in.get_float());
    }
    if (CAST_FAIL(int_range_check(expect_type, in.get_float(), value))) {
    } else {
      out.set_int(expect_type, value);
    }
    if (OB_SUCC(ret)) {
      res_precision = get_precision_for_integer(value);
    }
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int float_uint(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  uint64_t value = 0;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObFloatTC != in.get_type_class()
                  || ObUIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    if (in.get_float() < 0) {
      value = static_cast<uint64_t>(in.get_float() - ROUND_DOUBLE);
    } else if (in.get_float() > 0) {
      value = static_cast<uint64_t>(in.get_float() + ROUND_DOUBLE);
    } else {
      value = static_cast<uint64_t>(in.get_float());
    }
    if (CM_NEED_RANGE_CHECK(cast_mode) &&
            CAST_FAIL(uint_range_check(expect_type, in.get_float(), value))) {
    } else {
      out.set_uint(expect_type, value);
    }
    if (OB_SUCC(ret)) {
      res_precision = get_precision_for_integer(value);
    }
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int float_float(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  float value = in.get_float();
  if (OB_UNLIKELY(ObFloatTC != in.get_type_class()
                  || ObFloatTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (ObUFloatType == expect_type && CAST_FAIL(numeric_negative_check(value))) {
  } else {
    out.set_float(expect_type, value);
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  return ret;
}

static int float_double(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  double value = static_cast<double>(in.get_float());
  if (OB_UNLIKELY(ObFloatTC != in.get_type_class()
                  || ObDoubleTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (ObUDoubleType == expect_type && CAST_FAIL(numeric_negative_check(value))) {
  } else {
    out.set_double(expect_type, value);
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  return ret;
}

static int float_number(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  float value = in.get_float();
  ObPrecision res_precision = -1;
  ObScale res_scale = -1;
  if (OB_UNLIKELY(ObFloatTC != in.get_type_class()
                  || ObNumberTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (ObUNumberType == expect_type && CAST_FAIL(numeric_negative_check(value))) {
  } else {
    char buf[MAX_FLOAT_PRINT_SIZE];
    MEMSET(buf, 0, MAX_FLOAT_PRINT_SIZE);
    snprintf(buf, MAX_FLOAT_PRINT_SIZE, "%f", value);
    number::ObNumber nmb;
    if (OB_FAIL(nmb.from(buf, params, &res_precision, &res_scale))) {
    } else {
      out.set_number(expect_type, nmb);
    }
  }
  SET_RES_ACCURACY(res_precision, res_scale, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int double_datetime(const ObObjType expect_type, ObObjCastParams &params,
                           const ObObj &in, ObObj &out, const ObCastMode cast_mode);
static int float_datetime(const ObObjType expect_type, ObObjCastParams &params,
                          const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj dbl;
  if (OB_UNLIKELY(ObFloatTC != in.get_type_class()
                  || ObDateTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(float_double(ObDoubleType, params, in, dbl, cast_mode))) {
  } else if (OB_FAIL(double_datetime(expect_type, params, dbl, out, cast_mode))) {
  }
  //has set accuracy in prev double_datetime call
  return ret;
}

static int double_date(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode);
static int float_date(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj dbl;
  if (OB_UNLIKELY(ObFloatTC != in.get_type_class()
                  || ObDateTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(float_double(ObDoubleType, params, in, dbl, cast_mode))) {
  } else if (OB_FAIL(double_date(expect_type, params, dbl, out, cast_mode))) {
  }
  //has set accuracy in prev double_date call
  return ret;
}

static int double_time(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode);
static int float_time(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj dbl;
  if (OB_UNLIKELY(ObFloatTC != in.get_type_class()
                  || ObTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(float_double(ObDoubleType, params, in, dbl, cast_mode))) {
  } else if (OB_FAIL(double_time(expect_type, params, dbl, out, cast_mode))) {
  }
  //has set accuracy in prev double_time call
  return ret;
}

static int float_string(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH];
  MEMSET(buf, 0, OB_CAST_TO_VARCHAR_MAX_LENGTH);
  ObScale scale = in.get_scale();
  int64_t length = 0;
  ObLength res_length = -1;
  if (OB_UNLIKELY(ObFloatTC != in.get_type_class()
                  || (ObStringTC != ob_obj_type_class(expect_type)
                  && ObTextTC != ob_obj_type_class(expect_type)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    if (0 <= scale) {
      length = ob_fcvt(in.get_float(), scale, sizeof(buf) - 1, buf, NULL);
    } else {
      length = ob_gcvt_opt(in.get_float(), OB_GCVT_ARG_FLOAT, (int)sizeof(buf) - 1, buf, NULL, lib::is_oracle_mode());
    }
    ObString str(sizeof(buf), static_cast<int32_t>(length), buf);
    if (OB_FAIL(copy_string(params, expect_type, str, out))) {
    } else {
      res_length = static_cast<ObLength>(out.get_string_len());
    }
  }
  UNUSED(cast_mode);
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_STRING, DEFAULT_SCALE_FOR_STRING, res_length);
  return ret;
}

////////////////////////////////////////////////////////////////
// Double -> XXX

static int double_int(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  int64_t value = 0;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObDoubleTC != in.get_type_class()
                  || ObIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    if (in.get_double() < 0) {
      value = static_cast<int64_t>(in.get_double() - ROUND_DOUBLE);
    } else if (in.get_double() > 0) {
      value = static_cast<int64_t>(in.get_double() + ROUND_DOUBLE);
    } else {
      value = static_cast<int64_t>(in.get_double());
    }
    if (CAST_FAIL(int_range_check(expect_type, in.get_double(), value))) {
    } else {
      out.set_int(expect_type, value);
    }
    if (OB_SUCC(ret)) {
      res_precision = get_precision_for_integer(value);
    }
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int double_uint(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  uint64_t value = 0;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObDoubleTC != in.get_type_class()
                  || ObUIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    if (in.get_double() < 0) {
      value = static_cast<uint64_t>(in.get_double() - ROUND_DOUBLE);
    } else if (in.get_double() > 0) {
      value = static_cast<uint64_t>(in.get_double() + ROUND_DOUBLE);
    } else {
      value = static_cast<uint64_t>(in.get_double());
    }
    if (CM_NEED_RANGE_CHECK(cast_mode)
        && CAST_FAIL(uint_range_check(expect_type, in.get_double(), value))) {
    } else {
      out.set_uint(expect_type, value);
    }
    if (OB_SUCC(ret)) {
      res_precision = get_precision_for_integer(value);
    }
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int double_float(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  float value = static_cast<float>(in.get_double());
  if (OB_UNLIKELY(ObDoubleTC != in.get_type_class()
                  || ObFloatTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (CAST_FAIL(real_range_check(expect_type, in.get_double(), value))) {
  } else {
    out.set_float(expect_type, value);
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  return ret;
}

static int double_double(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  double value = in.get_double();
  if (OB_UNLIKELY(ObDoubleTC != in.get_type_class()
                  || ObDoubleTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (ObUDoubleType == expect_type && CAST_FAIL(numeric_negative_check(value))) {
  } else {
    out.set_double(expect_type, value);
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  return ret;
}

static int double_number(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  double value = in.get_double();
  ObPrecision res_precision = -1;
  ObScale res_scale = -1;
  if (OB_UNLIKELY(ObDoubleTC != in.get_type_class()
                  || ObNumberTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (ObUNumberType == expect_type && CAST_FAIL(numeric_negative_check(value))) {
  } else {
    char buf[MAX_DOUBLE_PRINT_SIZE];
    MEMSET(buf, 0, MAX_DOUBLE_PRINT_SIZE);
    snprintf(buf, MAX_DOUBLE_PRINT_SIZE, "%lf", value);
    number::ObNumber nmb;
    if (OB_FAIL(nmb.from(buf, params, &res_precision, &res_scale))) {
    } else {
      out.set_number(expect_type, nmb);
    }
  }
  SET_RES_ACCURACY(res_precision, res_scale, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int double_datetime(const ObObjType expect_type, ObObjCastParams &params,
                           const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  // double to datetime must cast to string first, then cast string to datetime.
  // because double 20151016153421.8 may actually 20151016153421.801 in memory,
  // so we will get '2015-10-16 15:34:21.801' instead of '2015-10-16 15:34:21.8'.
  // this problem can be resolved by cast double to string using ob_gcvt_opt().
  int ret = OB_SUCCESS;
  ObScale res_scale = -1;
  if (OB_UNLIKELY(ObDoubleTC != in.get_type_class()
                  || ObDateTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    int64_t value = 0;
    const ObTimeZoneInfo *tz_info = (ObTimestampType == expect_type) ? params.dtc_params_.tz_info_ : NULL;
    char buf[MAX_DOUBLE_PRINT_SIZE];
    MEMSET(buf, 0, MAX_DOUBLE_PRINT_SIZE);
    int64_t length = ob_gcvt_opt(in.get_double(), OB_GCVT_ARG_DOUBLE, (int)sizeof(buf) - 1, buf, NULL, lib::is_oracle_mode());
    ObString str(sizeof(buf), static_cast<int32_t>(length), buf);
    if (CAST_FAIL(ObTimeConverter::str_to_datetime(str, tz_info, value, &res_scale))) {
    } else {
      SET_RES_DATETIME(out);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, res_scale, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int double_date(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  //TODO In datediff,mysql does truncate NOT round
  //So,we have to revise double 2 int which act as round (right now) to truncation
  int ret = OB_SUCCESS;
  ObObj int64;
  if (OB_UNLIKELY(ObDoubleTC != in.get_type_class()
                  || ObDateTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(double_int(ObIntType, params, in, int64, cast_mode))) {
  } else if (OB_FAIL(int_date(expect_type, params, int64, out, cast_mode))) {
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, DEFAULT_SCALE_FOR_DATE, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int double_time(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  // double to time must cast to string first, then cast string to time.
  // see comment in double_datetime.
  int ret = OB_SUCCESS;
  ObScale res_scale = -1;
  if (OB_UNLIKELY(ObDoubleTC != in.get_type_class()
                  || ObTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    int64_t value = 0;
    char buf[MAX_DOUBLE_PRINT_SIZE];
    MEMSET(buf, 0, MAX_DOUBLE_PRINT_SIZE);
    int64_t length = ob_gcvt_opt(in.get_double(), OB_GCVT_ARG_DOUBLE, (int)sizeof(buf) - 1, buf, NULL, lib::is_oracle_mode());
    ObString str(sizeof(buf), static_cast<int32_t>(length), buf);
    if (CAST_FAIL(ObTimeConverter::str_to_time(str, value, &res_scale))) {
    } else {
      SET_RES_TIME(out);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, res_scale, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int double_string(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  if (OB_UNLIKELY(ObDoubleTC != in.get_type_class()
                  || (ObStringTC != ob_obj_type_class(expect_type)
                  && ObTextTC != ob_obj_type_class(expect_type)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH];
    MEMSET(buf, 0, OB_CAST_TO_VARCHAR_MAX_LENGTH);
    ObScale scale = in.get_scale();
    int64_t length = 0;
    if (0 <= scale) {
      length = ob_fcvt(in.get_double(), scale, sizeof(buf) - 1, buf, NULL);
    } else {
      length = ob_gcvt_opt(in.get_double(), OB_GCVT_ARG_DOUBLE, (int)sizeof(buf) - 1, buf, NULL, lib::is_oracle_mode());
    }
    ObString str(sizeof(buf), static_cast<int32_t>(length), buf);
    if (OB_FAIL(copy_string(params, expect_type, str, out))) {
    } else {
      res_length = static_cast<ObLength>(out.get_string_len());
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_STRING, DEFAULT_SCALE_FOR_STRING, res_length);
  UNUSED(cast_mode);
  return ret;
}


////////////////////////////////////////////////////////////////
// Number -> XXX

static int string_int(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode);
static int number_int(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObNumberTC != in.get_type_class()
                  || ObIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    number::ObNumber nmb = in.get_number();
    const char *value = nmb.format();
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null pointer",
               K(ret), K(value));
    } else {
      ObObj from;
      from.set_varchar(value, static_cast<ObString::obstr_size_t>(strlen(value)));
      ret = string_int(expect_type, params, from, out, cast_mode);
    }
  }
  if (OB_SUCC(ret)) {
    res_precision = get_precision_for_integer(out.get_int());
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int string_uint(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode);
static int number_uint(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObNumberTC != in.get_type_class()
                  || ObUIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    number::ObNumber nmb = in.get_number();
    const char *value = nmb.format();
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null pointer",
               K(ret), K(value));
    } else {
      ObObj from;
      from.set_varchar(value, static_cast<ObString::obstr_size_t>(strlen(value)));
      ret = string_uint(expect_type, params, from, out, cast_mode);
    }
  }
  if (OB_SUCC(ret)) {
    res_precision = get_precision_for_integer(out.get_uint64());
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int string_float(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode);
static int number_float(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObNumberTC != in.get_type_class()
                  || ObFloatTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const char *value = in.get_number().format();
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null pointer",
          K(ret), K(value));
    } else {
      ObObj from;
      from.set_varchar(value, static_cast<ObString::obstr_size_t>(strlen(value)));
      ret = string_float(expect_type, params, from, out, cast_mode);
    }
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  return ret;
}

static int string_double(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode);
static int number_double(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObNumberTC != in.get_type_class()
                  || ObDoubleTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const char *value = in.get_number().format();
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null pointer",
          K(ret), K(value));
    } else {
      ObObj from;
      from.set_varchar(value, static_cast<ObString::obstr_size_t>(strlen(value)));
      ret = string_double(expect_type, params, from, out, cast_mode);
    }
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  return ret;
}

static int number_number(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObNumberTC != in.get_type_class()
                  || ObNumberTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    number::ObNumber nmb = in.get_number();
    number::ObNumber value;
    if (OB_FAIL(value.from(nmb, params))) {
    } else if (ObUNumberType == expect_type && CAST_FAIL(numeric_negative_check(value))) {
    } else {
      out.set_number(expect_type, value);
    }
  }
  //todo maybe we can do some dirty work to get better result.
  //for now, just set it to unknown....
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  return ret;
}

static int string_datetime(const ObObjType expect_type, ObObjCastParams &params,
                           const ObObj &in, ObObj &out, const ObCastMode cast_mode);
static int number_datetime(const ObObjType expect_type, ObObjCastParams &params,
                           const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObNumberTC != in.get_type_class()
                  || ObDateTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const ObTimeZoneInfo *tz_info = (ObTimestampType == expect_type) ? params.dtc_params_.tz_info_ : NULL;
    int64_t value = 0;
    int64_t int_part = 0;
    int64_t dec_part = 0;
    if (in.get_number().is_negative()) {
      CAST_FAIL(OB_INVALID_DATE_FORMAT);
    } else if ((ObTimestampType == expect_type && in.get_number().is_decimal())) {
      CAST_FAIL(OB_INVALID_DATE_FORMAT);
    } else if (!in.get_number().is_int_parts_valid_int64(int_part,dec_part)) {
      LOG_WARN("failed to convert number to int");
    } else if (CAST_FAIL(ObTimeConverter::int_to_datetime(int_part, dec_part, tz_info, value))) {
    } else {
      SET_RES_DATETIME(out);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, MIN_SCALE_FOR_TEMPORAL, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int string_date(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode);
static int number_date(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObNumberTC != in.get_type_class()
                  || ObDateTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const char *value = in.get_number().format();
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null pointer",
          K(ret), K(value));
    } else {
      ObObj from;
      from.set_varchar(value, static_cast<ObString::obstr_size_t>(strlen(value)));
      ret = string_date(expect_type, params, from, out, cast_mode);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, DEFAULT_SCALE_FOR_DATE, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int string_time(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode);
static int number_time(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObNumberTC != in.get_type_class()
                  || ObTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const char *value = in.get_number().format();
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null pointer",
          K(ret), K(value));
    } else {
      ObObj from;
      from.set_varchar(value, static_cast<ObString::obstr_size_t>(strlen(value)));
      ret = string_time(expect_type, params, from, out, cast_mode);
    }
  }
  //has set accuracy in prev string_time call
  return ret;
}

static int string_year(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode);
static int number_year(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObNumberTC != in.get_type_class()
                  || ObYearTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const char *value = in.get_number().format();
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null pointer",
          K(ret), K(value));
    } else {
      ObObj from;
      from.set_varchar(value, static_cast<ObString::obstr_size_t>(strlen(value)));
      ret = string_year(expect_type, params, from, out, cast_mode);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, DEFAULT_SCALE_FOR_YEAR, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int number_string(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH];
  MEMSET(buf, 0, OB_CAST_TO_VARCHAR_MAX_LENGTH);
  int64_t len = 0;
  ObLength res_length = -1;
  if (OB_UNLIKELY(ObNumberTC != in.get_type_class()
                  || (ObStringTC != ob_obj_type_class(expect_type)
                  && ObTextTC != ob_obj_type_class(expect_type)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_SUCC(in.get_number().format(buf, sizeof(buf), len, in.get_scale()))) {
    ret = copy_string(params, expect_type, buf, len, out);
    if (OB_SUCC(ret)) {
      res_length = static_cast<ObLength>(out.get_string_len());
    }
  }
  UNUSED(cast_mode);
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_STRING, DEFAULT_SCALE_FOR_STRING, res_length);
  return ret;
}

////////////////////////////////////////////////////////////
// Datetime -> XXX

static int datetime_int(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObDateTimeTC != in.get_type_class()
                  || ObIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const ObTimeZoneInfo *tz_info = (ObTimestampType == in.get_type()) ? params.dtc_params_.tz_info_ : NULL;
    int64_t value = 0;
    if (OB_FAIL(ObTimeConverter::datetime_to_int(in.get_datetime(), tz_info, value))) {
    } else if (expect_type < ObIntType && CAST_FAIL(int_range_check(expect_type, value, value))) {
    } else {
      out.set_int(expect_type, value);
      res_precision = get_precision_for_integer(value);
    }
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int datetime_uint(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObDateTimeTC != in.get_type_class()
                  || ObUIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const ObTimeZoneInfo *tz_info = (ObTimestampType == in.get_type()) ? params.dtc_params_.tz_info_ : NULL;
    int64_t int64 = 0;
    if (OB_FAIL(ObTimeConverter::datetime_to_int(in.get_datetime(), tz_info, int64))) {
    } else {
      uint64_t value = static_cast<uint64_t>(int64);
      if (expect_type < ObUInt64Type && CAST_FAIL(uint_range_check(expect_type, int64, value))) {
      } else {
        out.set_uint(expect_type, value);
        res_precision = get_precision_for_integer(value);
      }
    }
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int datetime_float(const ObObjType expect_type, ObObjCastParams &params,
                          const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObDateTimeTC != in.get_type_class()
                  || ObFloatTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const ObTimeZoneInfo *tz_info = (ObTimestampType == in.get_type()) ? params.dtc_params_.tz_info_ : NULL;
    double value = 0.0;
    if (OB_FAIL(ObTimeConverter::datetime_to_double(in.get_datetime(), tz_info, value))) {
    } else {
      // if datetime_to_double return OB_SUCCESS, double value must be in (0, INT64_MAX),
      // so we can static_cast to float without range check.
      out.set_float(expect_type, static_cast<float>(value));
    }
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  UNUSED(cast_mode);
  return ret;
}

static int datetime_double(const ObObjType expect_type, ObObjCastParams &params,
                           const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObDateTimeTC != in.get_type_class()
                  || ObDoubleTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const ObTimeZoneInfo *tz_info = (ObTimestampType == in.get_type()) ? params.dtc_params_.tz_info_ : NULL;
    double value = 0.0;
    if (OB_FAIL(ObTimeConverter::datetime_to_double(in.get_datetime(), tz_info, value))) {
    } else {
      out.set_double(expect_type, value);
    }
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  UNUSED(cast_mode);
  return ret;
}

static int datetime_number(const ObObjType expect_type, ObObjCastParams &params,
                           const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  ObScale res_scale = -1;
  if (OB_UNLIKELY(ObDateTimeTC != in.get_type_class()
                  || ObNumberTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const ObTimeZoneInfo *tz_info = (ObTimestampType == in.get_type()) ? params.dtc_params_.tz_info_ : NULL;
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH];
    MEMSET(buf, 0, OB_CAST_TO_VARCHAR_MAX_LENGTH);
    int64_t len = 0;
    number::ObNumber value;
    if (OB_FAIL(ObTimeConverter::datetime_to_str(in.get_datetime(), tz_info, in.get_scale(), buf, sizeof(buf), len, false))) {
      LOG_WARN("failed to convert datetime to string", K(ret));
    } else if (CAST_FAIL(value.from(buf, len, params, &res_precision, &res_scale))) {
      LOG_WARN("failed to convert string to number", K(ret));
    } else {
      out.set_number(expect_type, value);
    }
  }
  SET_RES_ACCURACY(res_precision, res_scale, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int datetime_datetime(const ObObjType expect_type, ObObjCastParams &params,
                             const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObDateTimeTC != in.get_type_class()
                  || ObDateTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    int64_t value = in.get_datetime();
    if (ObDateTimeType == in.get_type() && ObTimestampType == expect_type) {
      ret = ObTimeConverter::datetime_to_timestamp(in.get_datetime(), params.dtc_params_.tz_info_, value);
    } else if (ObTimestampType == in.get_type() && ObDateTimeType == expect_type) {
      ret = ObTimeConverter::timestamp_to_datetime(in.get_datetime(), params.dtc_params_.tz_info_, value);
    }

    if (OB_FAIL(ret)) {
    } else {
      out.set_datetime(expect_type, value);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, in.get_scale(), DEFAULT_LENGTH_FOR_TEMPORAL);
  UNUSED(cast_mode);
  return ret;
}

static int datetime_date(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObDateTimeTC != in.get_type_class()
                  || ObDateTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const ObTimeZoneInfo *tz_info = (ObTimestampType == in.get_type()) ? params.dtc_params_.tz_info_ : NULL;
    int32_t value = 0;
    if (OB_FAIL(ObTimeConverter::datetime_to_date(in.get_datetime(), tz_info, value))) {
    } else {
      out.set_date(value);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, DEFAULT_SCALE_FOR_DATE, DEFAULT_LENGTH_FOR_TEMPORAL);
  UNUSED(cast_mode);
  return ret;
}

static int datetime_time(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObDateTimeTC != in.get_type_class()
                  || ObTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const ObTimeZoneInfo *tz_info = (ObTimestampType == in.get_type()) ? params.dtc_params_.tz_info_ : NULL;
    int64_t value = 0;
    if (OB_FAIL(ObTimeConverter::datetime_to_time(in.get_datetime(), tz_info, value))) {
    } else {
      out.set_time(value);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, in.get_scale(), DEFAULT_LENGTH_FOR_TEMPORAL);
  UNUSED(cast_mode);
  return ret;
}

static int datetime_year(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObDateTimeTC != in.get_type_class()
                  || ObYearTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const ObTimeZoneInfo *tz_info = (ObTimestampType == in.get_type()) ? params.dtc_params_.tz_info_ : NULL;
    uint8_t value = 0;
    if (CAST_FAIL(ObTimeConverter::datetime_to_year(in.get_datetime(), tz_info, value))) {
    } else {
      SET_RES_YEAR(out);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, DEFAULT_SCALE_FOR_YEAR, DEFAULT_LENGTH_FOR_TEMPORAL);
  UNUSED(cast_mode);
  return ret;
}

static int datetime_string(const ObObjType expect_type, ObObjCastParams &params,
                           const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  if (OB_UNLIKELY(ObDateTimeTC != in.get_type_class()
                  || (ObStringTC != ob_obj_type_class(expect_type)
                  && ObTextTC != ob_obj_type_class(expect_type)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const ObTimeZoneInfo *tz_info = (ObTimestampType == in.get_type()) ? params.dtc_params_.tz_info_ : NULL;
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH];
    MEMSET(buf, 0, OB_CAST_TO_VARCHAR_MAX_LENGTH);
    int64_t len = 0;
    if (OB_FAIL(ObTimeConverter::datetime_to_str(in.get_datetime(), tz_info, in.get_scale(),
                                                 buf, sizeof(buf), len))) {
      LOG_WARN("failed to convert datetime to string", K(ret));
    } else {
      out.set_type(expect_type);
      ret = copy_string(params, expect_type, buf, len, out);
      if (OB_SUCC(ret)) {
        res_length = static_cast<ObLength>(out.get_string_len());
      }
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_STRING, DEFAULT_SCALE_FOR_STRING, res_length);
  UNUSED(cast_mode);
  return ret;
}

static int datetime_otimestamp(const ObObjType expect_type,
                               ObObjCastParams &params,
                               const ObObj &in,
                               ObObj &out,
                               const ObCastMode cast_mode)
{
  UNUSED(cast_mode);
  int ret = OB_SUCCESS;
  
  if (OB_UNLIKELY(ObDateTimeTC != in.get_type_class()) 
      || OB_UNLIKELY(ObOTimestampTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else {
    int64_t dt_value = 0;
    if (ObTimestampType == in.get_type()) {
      int64_t utc_value = in.get_timestamp();
      if (OB_FAIL(ObTimeConverter::timestamp_to_datetime(utc_value, params.dtc_params_.tz_info_, dt_value))) {
        LOG_WARN("fail to convert timestamp to datetime", K(ret));
      }
    } else {
      dt_value = in.get_datetime();
    }
    if (OB_SUCC(ret)) {
      int64_t odate_value = 0;
      ObOTimestampData value;
      ObTimeConverter::datetime_to_odate(dt_value, odate_value);
      if (OB_FAIL(ObTimeConverter::odate_to_otimestamp(odate_value, params.dtc_params_.tz_info_, expect_type, value))) {
        LOG_WARN("fail to odate to otimestamp", K(ret), K(in), K(expect_type));
      } else {
        SET_RES_OTIMESTAMP(out);
      }
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, in.get_scale(), DEFAULT_LENGTH_FOR_TEMPORAL);
  
  return ret;
}


////////////////////////////////////////////////////////////
// Date -> XXX

static int date_int(const ObObjType expect_type, ObObjCastParams &params,
                    const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  int64_t value = 0;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObDateTC != in.get_type_class()
                  || ObIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::date_to_int(in.get_date(), value))) {
  } else if (expect_type < ObInt32Type && CAST_FAIL(int_range_check(expect_type, value, value))) {
  } else {
    out.set_int(expect_type, value);
    res_precision = get_precision_for_integer(value);
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int date_uint(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  int64_t int64 = 0;
  uint64_t value = 0;
  if (OB_UNLIKELY(ObDateTC != in.get_type_class()
                  || ObUIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::date_to_int(in.get_date(), int64))) {
  } else {
    value = static_cast<uint64_t>(int64);
    if (expect_type < ObUInt32Type && CAST_FAIL(uint_range_check(expect_type, int64, value))) {
    } else {
      out.set_uint(expect_type, value);
      res_precision = get_precision_for_integer(value);
    }
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int date_float(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  int64_t value = 0;
  if (OB_UNLIKELY(ObDateTC != in.get_type_class()
                  || ObFloatTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::date_to_int(in.get_date(), value))) {
  } else {
    out.set_float(expect_type, static_cast<float>(value));
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  UNUSED(cast_mode);
  return ret;
}

static int date_double(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  int64_t value = 0;
  if (OB_UNLIKELY(ObDateTC != in.get_type_class()
                  || ObDoubleTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::date_to_int(in.get_date(), value))) {
  } else {
    out.set_double(expect_type, static_cast<double>(value));
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  UNUSED(cast_mode);
  return ret;
}

static int date_number(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj obj_int;
  if (OB_UNLIKELY(ObDateTC != in.get_type_class()
                  || ObNumberTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(date_int(ObIntType, params, in, obj_int, cast_mode))) {
  } else if (OB_FAIL(int_number(expect_type, params, obj_int, out, cast_mode))) {
  }
  //has set accuracy in prev int_number
  return ret;
}

static int date_datetime(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObDateTC != in.get_type_class()
                  || ObDateTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const ObTimeZoneInfo *tz_info = (ObTimestampType == expect_type) ? params.dtc_params_.tz_info_ : NULL;
    int64_t value = 0;
    if (OB_FAIL(ObTimeConverter::date_to_datetime(in.get_date(), tz_info, value))) {
    } else {
      out.set_datetime(expect_type, value);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, MIN_SCALE_FOR_TEMPORAL, DEFAULT_LENGTH_FOR_TEMPORAL);
  UNUSED(cast_mode);
  return ret;
}

static int date_time(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObDateTC != in.get_type_class()
                  || ObTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    out.set_time(ObTimeConverter::ZERO_TIME);
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, MIN_SCALE_FOR_TEMPORAL, DEFAULT_LENGTH_FOR_TEMPORAL);
  UNUSED(cast_mode);
  return OB_SUCCESS;
}

static int date_year(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  uint8_t value = 0;
  if (OB_UNLIKELY(ObDateTC != in.get_type_class()
                  || ObYearTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (CAST_FAIL(ObTimeConverter::date_to_year(in.get_date(), value))) {
  } else {
    SET_RES_YEAR(out);
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, DEFAULT_SCALE_FOR_YEAR, DEFAULT_LENGTH_FOR_TEMPORAL);
  UNUSED(cast_mode);
  return ret;
}

static int date_string(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH];
  MEMSET(buf, 0, OB_CAST_TO_VARCHAR_MAX_LENGTH);
  int64_t len = 0;
  ObLength res_length = -1;
  if (OB_UNLIKELY(ObDateTC != in.get_type_class()
                  || (ObStringTC != ob_obj_type_class(expect_type)
                  && ObTextTC != ob_obj_type_class(expect_type)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::date_to_str(in.get_date(), buf, sizeof(buf), len))) {
  } else {
    out.set_type(expect_type);
    ret = copy_string(params, expect_type, buf, len, out);
    if (OB_SUCC(ret)) {
      res_length = static_cast<ObLength>(out.get_string_len());
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_STRING, DEFAULT_SCALE_FOR_STRING, res_length);
  UNUSED(cast_mode);
  return ret;
}

////////////////////////////////////////////////////////////
// Time -> XXX

static int time_int(const ObObjType expect_type, ObObjCastParams &params,
                    const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  int64_t value = 0;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObTimeTC != in.get_type_class()
                  || ObIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::time_to_int(in.get_time(), value))) {
  } else if (expect_type < ObInt32Type && CAST_FAIL(int_range_check(expect_type, value, value))) {
  } else {
    out.set_int(expect_type, value);
    res_precision = get_precision_for_integer(value);
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int time_uint(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  int64_t int64 = 0;
  uint64_t value = 0;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObTimeTC != in.get_type_class()
                  || ObUIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::time_to_int(in.get_time(), int64))) {
  } else {
    value = static_cast<uint64_t>(int64);
    if (CM_NEED_RANGE_CHECK(cast_mode) && CAST_FAIL(uint_range_check(expect_type, int64, value))) {
    } else {
      out.set_uint(expect_type, value);
      res_precision = get_precision_for_integer(value);
    }
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int time_float(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  double value = 0.0;
  if (OB_UNLIKELY(ObTimeTC != in.get_type_class()
                  || ObFloatTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::time_to_double(in.get_time(), value))) {
  } else if (ObUFloatType == expect_type && CAST_FAIL(numeric_negative_check(value))) {
  } else {
    // if time_to_double return OB_SUCCESS, double value must be in (INT64_MIN, INT64_MAX),
    // so we can static_cast to float without range check.
    out.set_float(expect_type, static_cast<float>(value));
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  return ret;
}

static int time_double(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  double value = 0.0;
  if (OB_UNLIKELY(ObTimeTC != in.get_type_class()
                  || ObDoubleTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::time_to_double(in.get_time(), value))) {
  } else if (ObUDoubleType == expect_type && CAST_FAIL(numeric_negative_check(value))) {
  } else {
    out.set_double(expect_type, value);
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  UNUSED(cast_mode);
  return ret;
}

static int time_number(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH];
  MEMSET(buf, 0, OB_CAST_TO_VARCHAR_MAX_LENGTH);
  int64_t len = 0;
  number::ObNumber value;
  ObPrecision res_precision = -1;
  ObScale res_scale = -1;
  if (OB_UNLIKELY(ObTimeTC != in.get_type_class()
                  || ObNumberTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::time_to_str(in.get_time(), in.get_scale(), buf, sizeof(buf), len, false))) {
  } else if (CAST_FAIL(value.from(buf, len, params, &res_precision, &res_scale))) {
  } else {
    out.set_number(expect_type, value);
  }
  SET_RES_ACCURACY(res_precision, res_scale, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int time_datetime(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  const ObTimeZoneInfo *tz_info = params.dtc_params_.tz_info_;
  int64_t value = 0;
  if (OB_UNLIKELY(ObTimeTC != in.get_type_class()
                  || ObDateTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::time_to_datetime(in.get_time(), params.cur_time_, tz_info, value, expect_type))) {
  } else {
    out.set_datetime(expect_type, value);
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, in.get_scale(), DEFAULT_LENGTH_FOR_TEMPORAL);
  UNUSED(cast_mode);
  return ret;
}

static int time_string(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH];
  MEMSET(buf, 0, OB_CAST_TO_VARCHAR_MAX_LENGTH);
  int64_t len = 0;
  ObLength res_length = -1;
  if (OB_UNLIKELY(ObTimeTC != in.get_type_class()
                  || (ObStringTC != ob_obj_type_class(expect_type)
                  && ObTextTC != ob_obj_type_class(expect_type)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::time_to_str(in.get_time(), in.get_scale(), buf, sizeof(buf), len))) {
  } else {
    out.set_type(expect_type);
    ret = copy_string(params, expect_type, buf, len, out);
    if (OB_SUCC(ret)) {
      res_length = static_cast<ObLength>(out.get_string_len());
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_STRING, DEFAULT_SCALE_FOR_STRING, res_length);
  UNUSED(cast_mode);
  return ret;
}

////////////////////////////////////////////////////////////
// Year -> XXX

static int year_int(const ObObjType expect_type, ObObjCastParams &params,
                    const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  int64_t value = 0;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObYearTC != in.get_type_class()
                  || ObIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::year_to_int(in.get_year(), value))) {
  } else if (expect_type < ObSmallIntType && CAST_FAIL(int_range_check(expect_type, value, value))) {
  } else {
    out.set_int(expect_type, value);
    res_precision = get_precision_for_integer(value);
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int year_uint(const ObObjType expect_type, ObObjCastParams &params,
                     const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  int64_t int64 = 0;
  uint64_t value = 0;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY(ObYearTC != in.get_type_class()
                  || ObUIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::year_to_int(in.get_year(), int64))) {
  } else {
    value = static_cast<uint64_t>(int64);
    if (expect_type < ObUSmallIntType && CAST_FAIL(uint_range_check(expect_type, int64, value))) {
    } else {
      out.set_uint(expect_type, value);
      res_precision = get_precision_for_integer(value);
    }
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int year_float(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  int64_t value = 0;
  if (OB_UNLIKELY(ObYearTC != in.get_type_class()
                  || ObFloatTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::year_to_int(in.get_year(), value))) {
  } else {
    out.set_float(expect_type, static_cast<float>(value));
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  UNUSED(cast_mode);
  return ret;
}

static int year_double(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  int64_t value = 0;
  if (OB_UNLIKELY(ObYearTC != in.get_type_class()
                  || ObDoubleTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::year_to_int(in.get_year(), value))) {
  } else {
    out.set_double(expect_type, static_cast<double>(value));
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  UNUSED(cast_mode);
  return ret;
}

static int year_number(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj obj_int;
  if (OB_UNLIKELY(ObYearTC != in.get_type_class()
                  || ObNumberTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(year_int(ObIntType, params, in, obj_int, cast_mode))) {
  } else if (OB_FAIL(int_number(expect_type, params, obj_int, out, cast_mode))) {
  }
  //has set accuracy in prev int_number
  return ret;
}


static int year_string(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH];
  MEMSET(buf, 0, OB_CAST_TO_VARCHAR_MAX_LENGTH);
  int64_t len = 0;
  ObLength res_length = -1;
  if (OB_UNLIKELY(ObYearTC != in.get_type_class()
                  || (ObStringTC != ob_obj_type_class(expect_type)
                  && ObTextTC != ob_obj_type_class(expect_type)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::year_to_str(in.get_year(), buf, sizeof(buf), len))) {
  } else {
    out.set_type(expect_type);
    ret = copy_string(params, expect_type, buf, len, out);
    if (OB_SUCC(ret)) {
      res_length = static_cast<ObLength>(out.get_string_len());
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_STRING, DEFAULT_SCALE_FOR_STRING, res_length);
  UNUSED(cast_mode);
  return ret;
}

////////////////////////////////////////////////////////////
// String -> XXX

inline uint64_t hex_to_uint64(const ObString &str)
{
  int32_t N = str.length();
  const uint8_t *p = reinterpret_cast<const uint8_t*>(str.ptr());
  uint64_t value = 0;
  if (OB_LIKELY(NULL != p)) {
    for (int32_t i = 0; i < N; ++i, ++p) {
      value = value * 256 + *p;
    }
  }
  return value;
}

static int string_int(const ObObjType expect_type, ObObjCastParams &params,
                      const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY((ObStringTC != in.get_type_class()
                  && ObTextTC != in.get_type_class())
                  || ObIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const ObString &str = in.get_string();
    int64_t value = 0;
    if (ObHexStringType == in.get_type()) {
      value = static_cast<int64_t>(hex_to_uint64(str));
    } else {
      int err = 0;
      char *endptr = NULL;
      value = static_cast<int64_t>(ObCharset::strntoullrnd(str.ptr(), str.length(), false, &endptr, &err));
      if (ERANGE == err && (INT64_MIN == value || INT64_MAX == value)) {
        ret = OB_DATA_OUT_OF_RANGE;
      } else {
        ret = check_convert_str_err(str.ptr(), endptr, str.length(), err);
      }
    }

    if (CAST_FAIL(ret)) {
    } else if (expect_type < ObIntType && CAST_FAIL(int_range_check(expect_type, value, value))) {
    } else {
      SET_RES_INT(out);
    }
  }
  if (OB_SUCC(ret)) {
    res_precision = get_precision_for_integer(out.get_int());
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int string_uint(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  if (OB_UNLIKELY((ObStringTC != in.get_type_class()
                  && ObTextTC != in.get_type_class())
                  || ObUIntTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else {
    const ObString &str = in.get_string();
    uint64_t value = 0;
    if (ObHexStringType == in.get_type()) {
      value = hex_to_uint64(str);
    } else {
      int err = 0;
      char *endptr = NULL;
      value = ObCharset::strntoullrnd(str.ptr(), str.length(), true, &endptr, &err);
      if (ERANGE == err && (0 == value || UINT64_MAX == value)) {
        ret = OB_DATA_OUT_OF_RANGE;
      } else {
        ret = check_convert_str_err(str.ptr(), endptr, str.length(), err);
      }
    }

    if (CAST_FAIL(ret)) {
    } else if (expect_type < ObUInt64Type && CM_NEED_RANGE_CHECK(cast_mode)
               && CAST_FAIL(uint_upper_check(expect_type, value))) {
    } else {
      SET_RES_UINT(out);
    }
  }
  if (OB_SUCC(ret)) {
    res_precision = get_precision_for_integer(out.get_uint64());
  }
  SET_RES_ACCURACY(res_precision, DEFAULT_SCALE_FOR_INTEGER, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int string_float(const ObObjType expect_type, ObObjCastParams &params,
                        const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj dbl;
  if (OB_UNLIKELY((ObStringTC != in.get_type_class()
                  && ObTextTC != in.get_type_class())
                  || ObFloatTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type",
        K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(string_double(ObDoubleType, params, in, dbl, cast_mode))) {
  } else if (OB_FAIL(double_float(expect_type, params, dbl, out, cast_mode))) {
  }
  //has set accuracy in prev double_float call
  return ret;
}

static int string_double(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((ObStringTC != in.get_type_class()
                  && ObTextTC != in.get_type_class())
                  || ObDoubleTC != ob_obj_type_class(expect_type))) {
     ret = OB_ERR_UNEXPECTED;
     LOG_ERROR("invalid input type",
         K(ret), K(in), K(expect_type));
  } else {
    double value = 0.0;
    const ObString &str = in.get_string();
    if (ObHexStringType == in.get_type()) {
      value = static_cast<double>(hex_to_uint64(str));
    } else {
      int err = 0;
      char *endptr = NULL;
      value = ObCharset::strntod(str.ptr(), str.length(), &endptr, &err);
      if (EOVERFLOW == err && (-DBL_MAX == value || DBL_MAX == value)) {
        ret = OB_DATA_OUT_OF_RANGE;
      } else {
        if (OB_SUCCESS != (ret = check_convert_str_err(str.ptr(), endptr, str.length(), err))) {
          ret = OB_ERR_DATA_TRUNCATED;
        }
      }
    }

    if (CAST_FAIL(ret)) {
    } else if (ObUDoubleType == expect_type && CAST_FAIL(numeric_negative_check(value))) {
    } else {
      SET_RES_DOUBLE(out);
    }
  }
  SET_RES_ACCURACY(PRECISION_UNKNOWN_YET, SCALE_UNKNOWN_YET, LENGTH_UNKNOWN_YET);
  return ret;
}

static int string_number(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  ObScale res_scale = -1;
  if (OB_UNLIKELY((ObStringTC != in.get_type_class()
                  && ObTextTC != in.get_type_class())
                  || ObNumberTC != ob_obj_type_class(expect_type))) {
     ret = OB_ERR_UNEXPECTED;
     LOG_ERROR("invalid input type",
         K(ret), K(in), K(expect_type));
  } else {
    const ObString &str = in.get_string();
    number::ObNumber value;
    if (ObHexStringType == in.get_type()) {
      ret = value.from(hex_to_uint64(str), params);
    } else if (OB_UNLIKELY(0 == str.length())) {
      value.set_zero();
      ret = OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD;
    } else {
      ret = value.from(str.ptr(), str.length(), params, &res_precision, &res_scale);
    }

    if (CAST_FAIL(ret)) {
    } else if (ObUNumberType == expect_type && CAST_FAIL(numeric_negative_check(value))) {
    } else {
      out.set_number(expect_type, value);
    }
  }
  SET_RES_ACCURACY(res_precision, res_scale, DEFAULT_LENGTH_FOR_NUMERIC);
  return ret;
}

static int string_datetime(const ObObjType expect_type, ObObjCastParams &params,
                           const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObScale res_scale = -1;
  if (OB_UNLIKELY((ObStringTC != in.get_type_class()
                  && ObTextTC != in.get_type_class())
                  || ObDateTimeTC != ob_obj_type_class(expect_type))) {
     ret = OB_ERR_UNEXPECTED;
     LOG_ERROR("invalid input type",
         K(ret), K(in), K(expect_type));
  } else {
    const ObTimeZoneInfo *tz_info = (ObTimestampType == expect_type) ? params.dtc_params_.tz_info_ : NULL;
    int64_t value = 0;
    if (CAST_FAIL(ObTimeConverter::str_to_datetime(in.get_string(), tz_info, value, &res_scale))) {
    } else {
      SET_RES_DATETIME(out);
    }
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, res_scale, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int string_date(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  int32_t value = 0;
  if (OB_UNLIKELY((ObStringTC != in.get_type_class()
                  && ObTextTC != in.get_type_class())
                  || ObDateTC != ob_obj_type_class(expect_type))) {
     ret = OB_ERR_UNEXPECTED;
     LOG_ERROR("invalid input type",
         K(ret), K(in), K(expect_type));
  } else if (CAST_FAIL(ObTimeConverter::str_to_date(in.get_string(), value))) {
  } else {
    SET_RES_DATE(out);
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, DEFAULT_SCALE_FOR_DATE, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int string_time(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  int64_t value = 0;
  ObScale res_scale = -1;
  if (OB_UNLIKELY((ObStringTC != in.get_type_class()
                  && ObTextTC != in.get_type_class())
                  || ObTimeTC != ob_obj_type_class(expect_type))) {
     ret = OB_ERR_UNEXPECTED;
     LOG_ERROR("invalid input type",
         K(ret), K(in), K(expect_type));
  } else if (CAST_FAIL(ObTimeConverter::str_to_time(in.get_string(), value, &res_scale))) {
  } else {
    SET_RES_TIME(out);
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, res_scale, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int string_year(const ObObjType expect_type, ObObjCastParams &params,
                       const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObObj int64;
  if (OB_UNLIKELY((ObStringTC != in.get_type_class()
                  && ObTextTC != in.get_type_class())
                  || ObYearTC != ob_obj_type_class(expect_type))) {
     ret = OB_ERR_UNEXPECTED;
     LOG_ERROR("invalid input type",
         K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(string_int(ObIntType, params, in, int64, CM_SET_WARN_ON_FAIL(cast_mode)))) {
  } else if (CAST_FAIL(int_year(ObYearType, params, int64, out, cast_mode))) {
  } else if (CAST_FAIL(params.warning_)) {
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, DEFAULT_SCALE_FOR_YEAR, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

static int string_string(const ObObjType expect_type, ObObjCastParams &params,
                         const ObObj &in, ObObj &out, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  if (OB_UNLIKELY((ObStringTC != in.get_type_class()
                  && ObTextTC != in.get_type_class())
                  || (ObStringTC != ob_obj_type_class(expect_type)
                  && ObTextTC != ob_obj_type_class(expect_type)))) {
     ret = OB_ERR_UNEXPECTED;
     LOG_ERROR("invalid input type",
         K(ret), K(in), K(expect_type));
  } else {
    ObString str;
    in.get_string(str);
    if (0 != str.length()
        && CS_TYPE_BINARY != in.get_collation_type()
        && CS_TYPE_BINARY != params.dest_collation_
        && CS_TYPE_INVALID != in.get_collation_type()
        && CS_TYPE_INVALID != params.dest_collation_
        && (ObCharset::charset_type_by_coll(in.get_collation_type())
            != ObCharset::charset_type_by_coll(params.dest_collation_))) {
      char *buf = NULL;
      // buf_len is related to the encoding length, gbk uses 2 bytes to encode a character, utf8mb4 uses 1 to 4 bytes
      // CharConvertFactorNum is a multiple of the requested memory size
      const int32_t CharConvertFactorNum = 2;
      int32_t buf_len = str.length() * CharConvertFactorNum;
      uint32_t result_len = 0;
      if (OB_UNLIKELY(NULL == (buf = static_cast<char*>(params.alloc(buf_len))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc memory failed", K(ret));
      } else if (OB_FAIL(ObCharset::charset_convert(in.get_collation_type(),
                                                    str.ptr(),
                                                    str.length(),
                                                    params.dest_collation_,
                                                    buf,
                                                    buf_len,
                                                    result_len))) {
        LOG_WARN("charset convert failed", K(ret), K(in.get_collation_type()), K(params.dest_collation_));
      }

      LOG_DEBUG("convert result", K(str), "result", ObHexEscapeSqlStr(ObString(result_len, buf)));

      if (OB_SUCC(ret)) {
        out.set_string(expect_type, buf, static_cast<int32_t>(result_len));
        if (CS_TYPE_INVALID != in.get_collation_type()) {
          out.set_collation_type(params.dest_collation_);
        }
      }
    } else {
      ret = copy_string(params, expect_type, str, out);
      if (CS_TYPE_INVALID != in.get_collation_type()) {
        out.set_collation_type(in.get_collation_type());
      }
    }
  }
  if (OB_SUCC(ret)) {
    res_length = static_cast<ObLength>(out.get_string_len());
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_STRING, DEFAULT_SCALE_FOR_STRING, res_length);
  UNUSED(cast_mode);
  return ret;
}

static int string_otimestamp(const ObObjType expect_type,
                             ObObjCastParams &params,
                             const ObObj &in,
                             ObObj &out,
                             const ObCastMode cast_mode)
{
  UNUSED(cast_mode);
  int ret = OB_SUCCESS;
  ObScale res_scale = -1;
  ObString utf8_string;

  if (OB_UNLIKELY(ObStringTC != in.get_type_class() && ObTextTC != in.get_type_class())
      || OB_UNLIKELY(ObOTimestampTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (lib::is_oracle_mode() && in.is_blob()) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("invalid use of blob type", K(ret), K(in), K(expect_type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Cast to blob type");
  } else if (OB_FAIL(convert_string_collation(in.get_string(),
                                              in.get_collation_type(),
                                              utf8_string,
                                              ObCharset::get_system_collation(),
                                              params))) {
    LOG_WARN("convert_string_collation", K(ret));
  } else {
    ObOTimestampData value;
    ObTimeConvertCtx cvrt_ctx(params.dtc_params_.tz_info_, true);
    cvrt_ctx.oracle_nls_format_ = params.dtc_params_.get_nls_format(expect_type);
    if (CAST_FAIL(ObTimeConverter::str_to_otimestamp(utf8_string, cvrt_ctx, expect_type, value, res_scale))) {
    } else {
      SET_RES_OTIMESTAMP(out);
    }
  }

  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, res_scale, DEFAULT_LENGTH_FOR_TEMPORAL);
  return ret;
}

////////////////////////////////////////////////////////////
// OTimestamp -> XXX

static int otimestamp_datetime(const ObObjType expect_type,
                               ObObjCastParams &params,
                               const ObObj &in,
                               ObObj &out,
                               const ObCastMode cast_mode)
{
  UNUSED(cast_mode);
  int ret = OB_SUCCESS;
  
  int64_t usec = 0;
  if (OB_UNLIKELY(ObOTimestampTC != in.get_type_class())
      || OB_UNLIKELY(ObDateTimeTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (OB_FAIL(ObTimeConverter::otimestamp_to_odate(in.get_type(),
                                                          in.get_otimestamp_value(),
                                                          params.dtc_params_.tz_info_,
                                                          usec))) {
    LOG_WARN("fail to timestamp_tz_to_timestamp", K(ret), K(in), K(expect_type));
  } else {
    ObTimeConverter::trunc_datetime(OB_MAX_DATE_PRECISION, usec);
    out.set_datetime(expect_type, usec);
    out.set_scale(OB_MAX_DATE_PRECISION);
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, in.get_scale(), DEFAULT_LENGTH_FOR_TEMPORAL);
  
  return ret;
}

static int otimestamp_string(const ObObjType expect_type,
                             ObObjCastParams &params,
                             const ObObj &in,
                             ObObj &out,
                             const ObCastMode cast_mode)
{
  UNUSED(cast_mode);
  int ret = OB_SUCCESS;
  ObLength res_length = -1;
  
  if (OB_UNLIKELY(ObOTimestampTC != in.get_type_class())
      || OB_UNLIKELY(ObStringTC != ob_obj_type_class(expect_type) && ObTextTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else {
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t len = 0;
    if (OB_FAIL(ObTimeConverter::otimestamp_to_str(in.get_otimestamp_value(),
                                                   params.dtc_params_,
                                                   in.get_scale(),
                                                   in.get_type(),
                                                   buf,
                                                   OB_CAST_TO_VARCHAR_MAX_LENGTH,
                                                   len))) {
      LOG_WARN("failed to convert otimestamp to string", K(ret));
    } else {
      ObString tmp_str;
      ObObj tmp_out;
      if (OB_FAIL(convert_string_collation(ObString(len, buf), 
                                           ObCharset::get_system_collation(),
                                           tmp_str,
                                           params.dest_collation_,
                                           params))) {
        LOG_WARN("fail to convert string collation", K(ret));
      } else if (OB_FAIL(check_convert_string(expect_type, params, tmp_str, tmp_out))) {
        LOG_WARN("fail to check_convert_string", K(ret), K(in), K(expect_type));
      } else if (OB_FAIL(copy_string(params, expect_type, tmp_out.get_string(), out))) {
        LOG_WARN("failed to copy_string", K(ret), K(expect_type), K(len));
      } else {
        out.set_type(expect_type);
        res_length = static_cast<ObLength>(out.get_string_len());
      }
    }
  }
  SET_RES_ACCURACY_STRING(expect_type, DEFAULT_PRECISION_FOR_STRING, res_length);
  
  return ret;
}

static int otimestamp_otimestamp(const ObObjType expect_type,
                                 ObObjCastParams &params,
                                 const ObObj &in,
                                 ObObj &out,
                                 const ObCastMode cast_mode)
{
  UNUSED(cast_mode);
  int ret = OB_SUCCESS;
  ObOTimestampData value;
  
  if (OB_UNLIKELY(ObOTimestampTC != in.get_type_class())
      || OB_UNLIKELY(ObOTimestampTC != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid input type", K(ret), K(in), K(expect_type));
  } else if (ObTimestampNanoType == in.get_type()) {
    if (OB_FAIL(ObTimeConverter::odate_to_otimestamp(in.get_otimestamp_value().time_us_,
                                                     params.dtc_params_.tz_info_,
                                                     expect_type,
                                                     value))) {
      LOG_WARN("fail to odate_to_otimestamp", K(ret), K(expect_type));
    } else {
      value.time_ctx_.tail_nsec_ = in.get_otimestamp_value().time_ctx_.tail_nsec_;
    }
  } else if (ObTimestampNanoType == expect_type) {
    if (OB_FAIL(ObTimeConverter::otimestamp_to_odate(in.get_type(),
                                                     in.get_otimestamp_value(),
                                                     params.dtc_params_.tz_info_,
                                                     *(int64_t*)&value.time_us_))) {
      LOG_WARN("fail to otimestamp_to_odate", K(ret), K(expect_type));
    } else {
      value.time_ctx_.tail_nsec_ = in.get_otimestamp_value().time_ctx_.tail_nsec_;
    }
  } else {
    if (OB_FAIL(ObTimeConverter::otimestamp_to_otimestamp(in.get_type(),
                                                          in.get_otimestamp_value(),
                                                          params.dtc_params_.tz_info_,
                                                          expect_type,
                                                          value))) {
      LOG_WARN("fail to otimestamp_to_otimestamp", K(ret), K(expect_type));
    }
  }

  if (OB_SUCC(ret)) {
    SET_RES_OTIMESTAMP(out);
  }
  SET_RES_ACCURACY(DEFAULT_PRECISION_FOR_TEMPORAL, in.get_scale(), DEFAULT_LENGTH_FOR_TEMPORAL);
  
  return ret;
}


ObObjCastFunc OB_OBJ_CAST[ObMaxTC][ObMaxTC] =
{
  {
    /*null -> XXX*/
    identity,           /*null*/
    identity,           /*int*/
    identity,           /*uint*/
    identity,           /*float*/
    identity,           /*double*/
    identity,           /*number*/
    identity,           /*datetime*/
    identity,           /*date*/
    identity,           /*time*/
    identity,           /*year*/
    identity,           /*string*/
    identity,           /*extend*/
    identity,           /*unknown*/
    identity,           /*text*/
    not_support,        /*bit*/
    not_support,        /*enumset*/
    not_support,        /*enumsetInner*/
    identity,           /*otimestamp*/
    not_expected,       /*raw*/
    not_expected,       /*interval*/
    not_expected,       /*rowid*/
    not_support,        /*lob*/
  },
  {
    /*int -> XXX*/
    not_support,        /*null*/
    int_int,            /*int*/
    int_uint,           /*uint*/
    int_float,          /*float*/
    int_double,         /*double*/
    int_number,         /*number*/
    int_datetime,       /*datetime*/
    int_date,           /*date*/
    int_time,           /*time*/
    int_year,           /*year*/
    int_string,         /*string*/
    not_support,        /*extend*/
    not_support,        /*unknown*/
    int_string,         /*text*/
    not_support,        /*bit*/
    not_expected,       /*enumset*/
    not_expected,       /*enumset_inner*/
    not_support,        /*otimestamp*/
    not_support,        /*raw*/
    not_expected,       /*interval*/
    not_expected,       /*rowid*/
    not_support,        /*lob*/
  },
  {
    /*uint -> XXX*/
    not_support,        /*null*/
    uint_int,           /*int*/
    uint_uint,          /*uint*/
    uint_float,         /*float*/
    uint_double,        /*double*/
    uint_number,        /*number*/
    uint_datetime,      /*datetime*/
    uint_date,          /*date*/
    uint_time,          /*time*/
    uint_year,          /*year*/
    uint_string,        /*string*/
    not_support,        /*extend*/
    not_support,        /*unknown*/
    uint_string,        /*text*/
    not_support,        /*bit*/
    not_expected,       /*enumset*/
    not_expected,       /*enumset_inner*/
    not_support,        /*otimestamp*/
    not_support,        /*raw*/
    not_expected,       /*interval*/
    not_expected,       /*rowid*/
    not_support,        /*lob*/
  },
  {
    /*float -> XXX*/
    not_support,        /*null*/
    float_int,          /*int*/
    float_uint,         /*uint*/
    float_float,        /*float*/
    float_double,       /*double*/
    float_number,       /*number*/
    float_datetime,     /*datetime*/
    float_date,         /*date*/
    float_time,         /*time*/
    not_support,        /*year*/
    float_string,       /*string*/
    not_support,        /*extend*/
    not_support,        /*unknown*/
    float_string,       /*text*/
    not_support,        /*bit*/
    not_expected,       /*enumset*/
    not_expected,       /*enumset_inner*/
    not_support,        /*otimestamp*/
    not_support,        /*raw*/
    not_expected,       /*interval*/
    not_expected,       /*rowid*/
    not_support,        /*lob*/
  },
  {
    /*double -> XXX*/
    not_support,        /*null*/
    double_int,         /*int*/
    double_uint,        /*uint*/
    double_float,       /*float*/
    double_double,      /*double*/
    double_number,      /*number*/
    double_datetime,    /*datetime*/
    double_date,        /*date*/
    double_time,        /*time*/
    not_support,        /*year*/
    double_string,      /*string*/
    not_support,        /*extend*/
    not_support,        /*unknown*/
    double_string,      /*text*/
    not_support,        /*bit*/
    not_expected,       /*enumset*/
    not_expected,       /*enumset_inner*/
    not_support,        /*otimestamp*/
    not_support,        /*raw*/
    not_expected,       /*interval*/
    not_expected,       /*rowid*/
    not_support,        /*lob*/
  },
  {
    /*number -> XXX*/
    not_support,        /*null*/
    number_int,         /*int*/
    number_uint,        /*uint*/
    number_float,       /*float*/
    number_double,      /*double*/
    number_number,      /*number*/
    number_datetime,    /*datetime*/
    number_date,        /*date*/
    number_time,        /*time*/
    number_year,        /*year*/
    number_string,      /*string*/
    not_support,        /*extend*/
    not_support,        /*unknown*/
    number_string,      /*text*/
    not_support,        /*bit*/
    not_expected,       /*enumset*/
    not_expected,       /*enumset_inner*/
    not_support,        /*otimestamp*/
    not_support,        /*raw*/
    not_expected,       /*interval*/
    not_expected,       /*rowid*/
    not_support,        /*lob*/
  },
  {
    /*datetime -> XXX*/
    not_support,          /*null*/
    datetime_int,         /*int*/
    datetime_uint,        /*uint*/
    datetime_float,       /*float*/
    datetime_double,      /*double*/
    datetime_number,      /*number*/
    datetime_datetime,    /*datetime*/
    datetime_date,        /*date*/
    datetime_time,        /*time*/
    datetime_year,        /*year*/
    datetime_string,      /*string*/
    not_support,          /*extend*/
    not_support,          /*unknown*/
    datetime_string,      /*text*/
    not_support,          /*bit*/
    not_expected,         /*enumset*/
    not_expected,         /*enumset_inner*/
    datetime_otimestamp,  /*otimestamp*/
    not_support,          /*raw*/
    not_expected,         /*interval*/
    not_expected,         /*rowid*/
    not_support,          /*lob*/
  },
  {
    /*date -> XXX*/
    not_support,        /*null*/
    date_int,           /*int*/
    date_uint,          /*uint*/
    date_float,         /*float*/
    date_double,        /*double*/
    date_number,        /*number*/
    date_datetime,      /*datetime*/
    identity,           /*date*/
    date_time,          /*time*/
    date_year,          /*year*/
    date_string,        /*string*/
    not_support,        /*extend*/
    not_support,        /*unknown*/
    date_string,        /*text*/
    not_support,        /*bit*/
    not_expected,       /*enumset*/
    not_expected,       /*enumset_inner*/
    not_support,        /*otimestamp*/
    not_support,        /*raw*/
    not_expected,       /*interval*/
    not_expected,       /*rowid*/
    not_support,        /*lob*/
  },
  {
    /*time -> XXX*/
    not_support,        /*null*/
    time_int,           /*int*/
    time_uint,          /*uint*/
    time_float,         /*float*/
    time_double,        /*double*/
    time_number,        /*number*/
    time_datetime,      /*datetime*/
    not_support,        /*date*/
    identity,           /*time*/
    not_support,        /*year*/
    time_string,        /*string*/
    not_support,        /*extend*/
    not_support,        /*unknown*/
    time_string,        /*text*/
    not_support,        /*bit*/
    not_expected,       /*enumset*/
    not_expected,       /*enumset_inner*/
    not_support,        /*otimestamp*/
    not_support,        /*raw*/
    not_expected,       /*interval*/
    not_expected,       /*rowid*/
    not_support,        /*lob*/
  },
  {
    /*year -> XXX*/
    not_support,        /*null*/
    year_int,           /*int*/
    year_uint,          /*uint*/
    year_float,         /*float*/
    year_double,        /*double*/
    year_number,        /*number*/
    not_support,        /*datetime*/
    not_support,        /*date*/
    not_support,        /*time*/
    identity,           /*year*/
    year_string,        /*string*/
    not_support,        /*extend*/
    not_support,        /*unknown*/
    year_string,        /*text*/
    not_support,        /*bit*/
    not_expected,       /*enumset*/
    not_expected,       /*enumset_inner*/
    not_support,        /*otimestamp*/
    not_support,        /*raw*/
    not_expected,       /*interval*/
    not_expected,       /*rowid*/
    not_support,        /*lob*/
  },
  {
    /*string -> XXX*/
    not_support,        /*null*/
    string_int,         /*int*/
    string_uint,        /*uint*/
    string_float,       /*float*/
    string_double,      /*double*/
    string_number,      /*number*/
    string_datetime,    /*datetime*/
    string_date,        /*date*/
    string_time,        /*time*/
    string_year,        /*year*/
    string_string,      /*string*/
    not_support,        /*extend*/
    not_support,        /*unknown*/
    string_string,      /*text*/
    not_support,        /*bit*/
    not_expected,       /*enumset*/
    not_expected,       /*enumset_inner*/
    string_otimestamp,  /*otimestamp*/
    not_support,        /*raw*/
    not_expected,       /*interval*/
    not_expected,       /*rowid*/
    not_support,        /*lob*/
  },
  {
    /*extend -> XXX*/
    not_support,        /*null*/
    not_support,        /*int*/
    not_support,        /*uint*/
    not_support,        /*float*/
    not_support,        /*double*/
    not_support,        /*number*/
    not_support,        /*datetime*/
    not_support,        /*date*/
    not_support,        /*time*/
    not_support,        /*year*/
    not_support,        /*string*/
    identity,           /*extend*/
    not_support,        /*unknown*/
    not_support,        /*text*/
    not_support,        /*bit*/
    not_support,        /*enumset*/
    not_support,        /*enumset_inner*/
    not_support,        /*otimestamp*/
    not_support,        /*raw*/
    not_expected,       /*interval*/
    not_expected,       /*rowid*/
    not_support,        /*lob*/
  },
  {
    /*unknown -> XXX*/
    unknown_other,      /*null*/
    unknown_other,      /*int*/
    unknown_other,      /*uint*/
    unknown_other,      /*float*/
    unknown_other,      /*double*/
    unknown_other,      /*number*/
    unknown_other,      /*datetime*/
    unknown_other,      /*date*/
    unknown_other,      /*time*/
    unknown_other,      /*year*/
    unknown_other,      /*string*/
    unknown_other,      /*extend*/
    identity,           /*unknown*/
    not_support,        /*text*/
    not_support,        /*bit*/
    not_support,        /*enumset*/
    not_support,        /*enumset_inner*/
    not_support,        /*otimestamp*/
    not_support,        /*raw*/
    not_expected,       /*interval*/
    not_expected,       /*rowid*/
    not_support,        /*lob*/
  },
  {
    /*text -> XXX*/
    not_support,        /*null*/
    string_int,         /*int*/
    string_uint,        /*uint*/
    string_float,       /*float*/
    string_double,      /*double*/
    string_number,      /*number*/
    string_datetime,    /*datetime*/
    string_date,        /*date*/
    string_time,        /*time*/
    string_year,        /*year*/
    string_string,      /*string*/
    not_support,        /*extend*/
    not_support,        /*unknown*/
    string_string,      /*text*/
    not_support,        /*bit*/
    not_expected,       /*enumset*/
    not_expected,       /*enumset_inner*/
    string_otimestamp,  /*otimestamp*/
    not_support,        /*raw*/
    not_expected,       /*interval*/
    not_expected,       /*rowid*/
    not_support,        /*lob*/
  },
  {
    /*bit -> XXX*/
    not_support,        /*null*/
    not_support,        /*int*/
    not_support,        /*uint*/
    not_support,        /*float*/
    not_support,        /*double*/
    not_support,        /*number*/
    not_support,        /*datetime*/
    not_support,        /*date*/
    not_support,        /*time*/
    not_support,        /*year*/
    not_support,        /*string*/
    not_support,        /*extend*/
    not_support,        /*unknown*/
    not_support,        /*text*/
    not_support,        /*bit*/
    not_expected,       /*enumset*/
    not_expected,       /*enumset_inner*/
    not_support,        /*otimestamp*/
    not_support,        /*raw*/
    not_expected,       /*interval*/
    not_expected,       /*rowid*/
    not_support,        /*lob*/
  },
  {
    /*enumset -> XXX*/
    not_support,        /*null*/
    not_support,        /*int*/
    not_support,        /*uint*/
    not_support,        /*float*/
    not_support,        /*double*/
    not_support,        /*number*/
    not_expected,       /*datetime*/
    not_expected,       /*date*/
    not_expected,       /*time*/
    not_support,        /*year*/
    not_expected,       /*string*/
    not_support,        /*extend*/
    not_support,        /*unknown*/
    not_expected,       /*text*/
    not_support,        /*bit*/
    not_expected,       /*enumset*/
    not_expected,       /*enumset_inner*/
    not_support,        /*otimestamp*/
    not_support,        /*raw*/
    not_expected,       /*interval*/
    not_expected,       /*rowid*/
    not_expected,       /*lob*/
  },
  {
    /*enumset_inner -> XXX*/
    not_support,        /*null*/
    not_support,        /*int*/
    not_support,        /*uint*/
    not_support,        /*float*/
    not_support,        /*double*/
    not_support,        /*number*/
    not_support,        /*datetime*/
    not_support,        /*date*/
    not_support,        /*time*/
    not_support,        /*year*/
    not_support,        /*string*/
    not_support,        /*extend*/
    not_support,        /*unknown*/
    not_support,        /*text*/
    not_support,        /*bit*/
    not_expected,       /*enumset*/
    not_expected,       /*enumset_inner*/
    not_support,        /*otimestamp*/
    not_support,        /*raw*/
    not_expected,       /*interval*/
    not_expected,       /*rowid*/
    not_support,        /*lob*/
  },
  {
    /*otimestamp -> XXX*/
    not_support,            /*null*/
    not_support,            /*int*/
    not_support,            /*uint*/
    not_support,            /*float*/
    not_support,            /*double*/
    not_support,            /*number*/
    otimestamp_datetime,    /*datetime*/
    not_expected,           /*date*/
    not_expected,           /*time*/
    not_expected,           /*year*/
    otimestamp_string,      /*string*/
    not_support,            /*extend*/
    not_support,            /*unknown*/
    not_support,            /*text*/
    not_support,            /*bit*/
    not_expected,           /*enumset*/
    not_expected,           /*enumset_inner*/
    otimestamp_otimestamp,  /*otimestamp*/
    not_support,            /*raw*/
    not_support,            /*interval*/
    not_support,            /*rowid*/
    not_support,            /*lob*/
  },
  {
    /*raw -> XXX*/
    not_support,        /*null*/
    not_support,        /*int*/
    not_support,        /*uint*/
    not_support,        /*float*/
    not_support,        /*double*/
    not_support,        /*number*/
    not_support,        /*datetime*/
    not_expected,       /*date*/
    not_expected,       /*time*/
    not_expected,       /*year*/
    not_support,        /*string*/
    not_support,        /*extend*/
    not_support,        /*unknown*/
    not_support,        /*text*/
    not_support,        /*bit*/
    not_expected,       /*enumset*/
    not_expected,       /*enumset_inner*/
    not_support,        /*otimestamp*/
    not_support,        /*raw*/
    not_support,        /*interval*/
    not_support,        /*rowid*/
    not_support,        /*lob*/
  },
  {
    /*interval -> XXX*/
    not_expected,       /*null*/
    not_expected,       /*int*/
    not_expected,       /*uint*/
    not_expected,       /*float*/
    not_expected,       /*double*/
    not_expected,       /*number*/
    not_expected,       /*datetime*/
    not_expected,       /*date*/
    not_expected,       /*time*/
    not_expected,       /*year*/
    not_support,        /*string*/
    not_expected,       /*extend*/
    not_expected,       /*unknown*/
    not_expected,       /*text*/
    not_expected,       /*bit*/
    not_expected,       /*enumset*/
    not_expected,       /*enumset_inner*/
    not_expected,       /*otimestamp*/
    not_expected,       /*raw*/
    not_support,        /*interval*/
    not_support,        /*rowid*/
    not_support,        /*lob*/
  },
  {
    /*rowid -> XXX*/
    not_support,        /*null*/
    not_support,        /*int*/
    not_support,        /*uint*/
    not_support,        /*float*/
    not_support,        /*double*/
    not_support,        /*number*/
    not_support,        /*datetime*/
    not_expected,       /*date*/
    not_expected,       /*time*/
    not_expected,       /*year*/
    not_support,        /*string*/
    not_expected,       /*extend*/
    not_expected,       /*unknown*/
    not_expected,       /*text*/
    not_expected,       /*bit*/
    not_expected,       /*enumset*/
    not_expected,       /*enumset_inner*/
    not_support,        /*otimestamp*/
    not_support,        /*raw*/
    not_support,        /*interval*/
    not_support,        /*rowid*/
    not_support,        /*lob*/
  },
  {
    /*lob -> XXX*/
    not_support,        /*null*/
    not_support,        /*int*/
    not_support,        /*uint*/
    not_support,        /*float*/
    not_support,        /*double*/
    not_support,        /*number*/
    not_support,        /*datetime*/
    not_expected,       /*date*/
    not_expected,       /*time*/
    not_expected,       /*year*/
    not_support,        /*string*/
    not_support,        /*extend*/
    not_support,        /*unknown*/
    not_support,        /*text*/
    not_support,        /*bit*/
    not_expected,       /*enumset*/
    not_expected,       /*enumset_inner*/
    not_support,        /*otimestamp*/
    not_support,        /*raw*/
    not_support,        /*interval*/
    not_support,        /*rowid*/
    not_support,        /*lob*/
  },
};

////////////////////////////////////////////////////////////////
bool cast_supported(const ObObjTypeClass orig_tc,
                    const ObObjTypeClass expect_tc)
{
  bool bret = false;
  if (OB_UNLIKELY(ob_is_invalid_obj_tc(orig_tc) ||
                  ob_is_invalid_obj_tc(expect_tc))) {
    LOG_WARN("invalid cast type", K(orig_tc), K(expect_tc));
  } else {
    bret = (OB_OBJ_CAST[orig_tc][expect_tc] != not_support);
  }
  return bret;
}

int float_range_check(ObObjCastParams &params, const ObAccuracy &accuracy,
                      const ObObj &obj, ObObj &buf_obj, const ObObj *&res_obj, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  float value = obj.get_float();
  res_obj = &obj;
  if (CAST_FAIL(real_range_check(accuracy, value))) {
  } else if (obj.get_float() != value) {
    buf_obj.set_float(obj.get_type(), value);
    res_obj = &buf_obj;
  }
  return ret;
}

int double_check_precision(ObObjCastParams &params, const ObAccuracy &accuracy,
                           const ObObj &obj, ObObj &buf_obj, const ObObj *&res_obj, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  double value = obj.get_double();
  res_obj = &obj;
  if (CAST_FAIL(real_range_check(accuracy, value))) {
  } else if (obj.get_double() != value) {
    buf_obj.set_double(obj.get_type(), value);
    res_obj = &buf_obj;
  }
  return ret;
}

int number_range_check(ObObjCastParams &params, const ObAccuracy &accuracy,
                       const ObObj &obj, ObObj &buf_obj, const ObObj *&res_obj, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  res_obj = NULL;
  static const int64_t BUFFER_SIZE = 2 * (number::ObNumber::MAX_SCALE +
                                          number::ObNumber::MAX_PRECISION);
  if (OB_ISNULL(params.allocator_v2_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid allocator",
        K(ret), K(params.allocator_v2_), K(obj));
  } else {
    int &cast_ret = CM_IS_ERROR_ON_FAIL(cast_mode) ? ret : params.warning_;
    ObPrecision precision = accuracy.get_precision();
    ObScale scale = accuracy.get_scale();
    ObIAllocator &allocator = *params.allocator_v2_;
    res_obj = &obj;
    if (OB_UNLIKELY(precision < scale)) {
      ret = OB_ERR_M_BIGGER_THAN_D;
    } else if (precision < 0 && scale < 0) {
      /* do nothing */
    } else if (number::ObNumber::MAX_PRECISION >= precision &&
               number::ObNumber::MAX_SCALE >= scale &&
               precision >= 0 &&
               scale >= 0) {
      // prepare string like "99.999".
      char buf[BUFFER_SIZE] = {0};
      int pos = 0;
      buf[pos++] = '-';
      if (precision == scale) {
        buf[pos++] = '0';
      }
      MEMSET(buf + pos, '9', precision + 1);
      buf[pos + precision - scale] = '.';
      // make min and max numbers.
      number::ObNumber min_num;
      number::ObNumber max_num;
      number::ObNumber in_val = obj.get_number();
      number::ObNumber out_val;
      if (OB_FAIL(min_num.from(buf, allocator))) {
      } else if (OB_FAIL(max_num.from(buf + 1, allocator))) {
      } else if (in_val < min_num) {
        cast_ret = OB_DATA_OUT_OF_RANGE;
        buf_obj.set_number(obj.get_type(), min_num);
      } else if (in_val > max_num) {
        cast_ret = OB_DATA_OUT_OF_RANGE;
        buf_obj.set_number(obj.get_type(), max_num);
      } else if (OB_FAIL(out_val.from(in_val, allocator))) {
      } else if (OB_FAIL(out_val.round(scale))) {
      } else {
        buf_obj.set_number(obj.get_type(), out_val);
      }
      res_obj = &buf_obj;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments",
          K(ret), K(precision), K(scale));
    }
  }
  return ret;
}

int datetime_scale_check(ObObjCastParams &params, const ObAccuracy &accuracy,
                         const ObObj &obj, ObObj &buf_obj, const ObObj *&res_obj, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  res_obj = NULL;
  ObScale scale = accuracy.get_scale();
  if (OB_UNLIKELY(scale > MAX_SCALE_FOR_TEMPORAL)) {
    ret = OB_ERR_TOO_BIG_PRECISION;
  } else if (OB_UNLIKELY(0 <= scale && scale < MAX_SCALE_FOR_TEMPORAL)) {
    int64_t value = obj.get_datetime();
    ObTimeConverter::round_datetime(scale, value);
    buf_obj.set_datetime(obj.get_type(), value);
    res_obj = &buf_obj;
  } else {
    res_obj = &obj;
  }
  UNUSED(params);
  UNUSED(cast_mode);
  return ret;
}

int otimestamp_scale_check(ObObjCastParams &params, const ObAccuracy &accuracy, const ObObj &obj,
                           ObObj &buf_obj, const ObObj *&res_obj, const ObCastMode cast_mode)
{
  UNUSED(params);
  UNUSED(cast_mode);

  int ret = OB_SUCCESS;
  res_obj = NULL;
  ObScale scale = accuracy.get_scale();

  if (OB_UNLIKELY(scale > MAX_SCALE_FOR_ORACLE_TEMPORAL)) {
    ret = OB_ERR_TOO_BIG_PRECISION;
    LOG_WARN("fail to scale check", K(ret), K(MAX_SCALE_FOR_ORACLE_TEMPORAL));
  } else if (0 <= scale && scale < MAX_SCALE_FOR_ORACLE_TEMPORAL) {
    ObOTimestampData ot_data = ObTimeConverter::round_otimestamp(scale, obj.get_otimestamp_value());
    if (ObTimeConverter::is_valid_otimestamp(ot_data.time_us_, static_cast<int32_t>(ot_data.time_ctx_.tail_nsec_))) {
      buf_obj.set_otimestamp_value(obj.get_type(), ot_data);
      buf_obj.set_scale(scale);
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid otimestamp, set it null", K(ret), K(ot_data), K(scale));
      buf_obj.set_null();
    }
    res_obj = &buf_obj;
  } else {
    res_obj = &obj;
  }

  return ret;
}

int time_scale_check(ObObjCastParams &params, const ObAccuracy &accuracy,
                     const ObObj &obj, ObObj &buf_obj, const ObObj *&res_obj, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  res_obj = NULL;
  ObScale scale = accuracy.get_scale();
  if (OB_LIKELY(0 <= scale && scale < MAX_SCALE_FOR_TEMPORAL)) {
    int64_t value = obj.get_time();
    ObTimeConverter::round_datetime(scale, value);
    buf_obj.set_time(value);
    res_obj = &buf_obj;
  } else {
    res_obj = &obj;
  }
  UNUSED(params);
  UNUSED(cast_mode);
  return ret;
}

int string_length_check(ObObjCastParams &params, const ObAccuracy &accuracy, const ObCollationType cs_type,
                        const ObObj &obj, ObObj &buf_obj, const ObObj *&res_obj, const ObCastMode cast_mode)
{
  int ret = OB_SUCCESS;
  int &cast_ret = CM_IS_ERROR_ON_FAIL(cast_mode) ? ret : params.warning_;
  const ObLength max_len_char = accuracy.get_length();
  const char *str = obj.get_string_ptr();
  const int32_t str_len_byte = obj.get_string_len();
  const int32_t str_len_char = static_cast<int32_t>(ObCharset::strlen_char(cs_type, str, str_len_byte));
  if (OB_UNLIKELY(max_len_char <= 0)) {
    buf_obj.set_string(obj.get_type(), NULL, 0);
    buf_obj.set_collation_level(obj.get_collation_level());
    buf_obj.set_collation_type(obj.get_collation_type());
    res_obj = &buf_obj;
    if (OB_UNLIKELY(0 == max_len_char && str_len_byte > 0)) {
      cast_ret = OB_ERR_DATA_TOO_LONG;
      OB_LOG(WARN, "char type length is too long", K(obj), K(max_len_char), K(str_len_char));
    }
  } else {
    int32_t trunc_len_byte = -1;
    int32_t trunc_len_char = -1;
    if (obj.is_varbinary() || obj.is_binary()) {
      // str_len_char > max_len_char means an error or warning, no matter tail ' ' or '\0'.
      if (OB_UNLIKELY(str_len_char > max_len_char)) {
        cast_ret = OB_ERR_DATA_TOO_LONG;
        LOG_WARN("binary type length is too long", K(obj), K(max_len_char), K(str_len_char));
      }
    } else {
      // trunc_len_char > max_len_char means an error or warning, without tail ' ', otherwise
      // str_len_char > max_len_char means only warning, even in strict mode.
      trunc_len_byte = static_cast<int32_t>(ObCharset::strlen_byte_no_sp(cs_type, str, str_len_byte));
      trunc_len_char = static_cast<int32_t>(ObCharset::strlen_char(cs_type, str, trunc_len_byte));
      if (OB_UNLIKELY(trunc_len_char > max_len_char)) {
        cast_ret = OB_ERR_DATA_TOO_LONG;
        LOG_WARN("char type length is too long", K(obj), K(max_len_char), K(trunc_len_char));
      } else if (OB_UNLIKELY(str_len_char > max_len_char)) {
        params.warning_ = OB_ERR_DATA_TOO_LONG;
        LOG_WARN("char type length is too long", K(obj), K(max_len_char), K(str_len_char));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(OB_ERR_DATA_TOO_LONG == params.warning_)) {
        // when warning, always trunc to max_len_char first.
        // besides, if char (not binary), trunc to trunc_len_char again, trim tail ' ' after first trunc.
        // the reason of two-trunc for char (not binary):
        // insert 'ab  ! ' to char(3), we get an 'ab' in column, not 'ab ':
        // first trunc: 'ab  ! ' to 'ab ',
        // second trunc: 'ab ' to 'ab'.
        trunc_len_byte = static_cast<int32_t>(ObCharset::charpos(cs_type, str, str_len_byte, max_len_char));
        if (obj.is_char() && !obj.is_binary()) {
          trunc_len_byte = static_cast<int32_t>(ObCharset::strlen_byte_no_sp(cs_type, str, trunc_len_byte));
        }
        if (OB_FAIL(copy_string(params, obj.get_type(), str, trunc_len_byte, buf_obj))) {
        } else {
          buf_obj.set_collation_level(obj.get_collation_level());
          buf_obj.set_collation_type(obj.get_collation_type());
          res_obj = &buf_obj;
        }
      } else {
        res_obj = &obj;
      }
    }
  }
  return ret;
}

int obj_collation_check(const bool is_strict_mode, const ObCollationType cs_type, ObObj &obj)
{
  int ret = OB_SUCCESS;
  if (!ob_is_string_tc(obj.get_type())
      || (cs_type == obj.get_collation_type())) {
    //nothing to do
  } else if (cs_type == CS_TYPE_BINARY) {
    obj.set_collation_type(cs_type);
  } else {
    ObString str;
    int64_t well_formed_len = 0;
    obj.get_string(str);
    if (OB_FAIL(ObCharset::well_formed_len(cs_type,
                                           str.ptr(),
                                           str.length(),
                                           well_formed_len))) {
      LOG_WARN("invalid string for charset",
                K(ret), K(cs_type), K(str), K(well_formed_len));
      if (is_strict_mode) {
        ret = OB_ERR_INCORRECT_STRING_VALUE;
        LOG_USER_ERROR(ret, str.length(), str.ptr());
      } else {
        ret = OB_SUCCESS;
        obj.set_collation_type(cs_type);
        str.assign_ptr(str.ptr(), static_cast<ObString::obstr_size_t>(well_formed_len));
        obj.set_string(obj.get_type(), str.ptr(), str.length());
        LOG_WARN("invalid string for charset",
                  K(ret), K(cs_type), K(str), K(well_formed_len), K(str.length()));
      }
    } else {
      obj.set_collation_type(cs_type);
    }
  }
  return ret;
}

int obj_accuracy_check(ObCastCtx &cast_ctx,
                       const ObAccuracy &accuracy,
                       const ObCollationType cs_type,
                       const ObObj &obj,
                       ObObj &buf_obj, 
                       const ObObj *&res_obj)
{
  int ret = OB_SUCCESS;

  if (accuracy.is_valid()) {
    LOG_DEBUG("obj_accuracy_check before", K(obj), K(accuracy), K(cs_type));
    switch (obj.get_type_class()) {
      case ObFloatTC: {
        ret = float_range_check(cast_ctx, accuracy, obj, buf_obj, res_obj, cast_ctx.cast_mode_);
        break;
      }
      case ObDoubleTC: {
        ret = double_check_precision(cast_ctx, accuracy, obj, buf_obj, res_obj, cast_ctx.cast_mode_);
        break;
      }
      case ObNumberTC: {
        ret = number_range_check(cast_ctx, accuracy, obj, buf_obj, res_obj, cast_ctx.cast_mode_);
        break;
      }
      case ObDateTimeTC: {
        ret = datetime_scale_check(cast_ctx, accuracy, obj, buf_obj, res_obj, cast_ctx.cast_mode_);
        break;
      }
      case ObOTimestampTC: {
        ret = otimestamp_scale_check(cast_ctx, accuracy, obj, buf_obj, res_obj, cast_ctx.cast_mode_);
        break;
      }
      case ObTimeTC: {
        ret = time_scale_check(cast_ctx, accuracy, obj, buf_obj, res_obj, cast_ctx.cast_mode_);
        break;
      }
      default: {
        break;
      }
    }
  }

  return ret;
}

int ob_obj_to_ob_time_with_date(const ObObj &obj, const ObTimeZoneInfo *tz_info, ObTime &ob_time)
{
  int ret = OB_SUCCESS;
  switch (obj.get_type_class()) {
  case ObIntTC:
  // fallthrough.
  case ObUIntTC: {
      ret = ObTimeConverter::int_to_ob_time_with_date(obj.get_int(), ob_time);
      break;
    }
  case ObDateTimeTC: {
      ret = ObTimeConverter::datetime_to_ob_time(obj.get_datetime(),
                            (ObTimestampType == obj.get_type()) ? tz_info : NULL, ob_time);
      break;
    }
  case ObDateTC: {
      ret = ObTimeConverter::date_to_ob_time(obj.get_date(), ob_time);
      break;
    }
  case ObTimeTC: {
      ret = ObTimeConverter::time_to_ob_time(obj.get_time(), ob_time);
      break;
    }
  case ObStringTC: {
      ret = ObTimeConverter::str_to_ob_time_with_date(obj.get_string(), ob_time);
      break;
    }
  default: {
      ret = OB_NOT_SUPPORTED;
    }
  }
  return ret;
}

int ob_obj_to_ob_time_without_date(const ObObj &obj, const ObTimeZoneInfo *tz_info, ObTime &ob_time)
{
  int ret = OB_SUCCESS;
  switch (obj.get_type_class()) {
  case ObIntTC:
    // fallthrough.
  case ObUIntTC: {
      ret = ObTimeConverter::int_to_ob_time_without_date(obj.get_int(), ob_time);
      break;
    }
  case ObDateTimeTC: {
      ret = ObTimeConverter::datetime_to_ob_time(obj.get_datetime(),  (ObTimestampType == obj.get_type()) ? tz_info : NULL, ob_time);
      break;
    }
  case ObDateTC: {
      ret = ObTimeConverter::date_to_ob_time(obj.get_date(), ob_time);
      break;
    }
  case ObTimeTC: {
      ret = ObTimeConverter::time_to_ob_time(obj.get_time(), ob_time);
      break;
    }
  case ObStringTC: {
      ret = ObTimeConverter::str_to_ob_time_without_date(obj.get_string(), ob_time);
      break;
    }
  default: {
      ret = OB_NOT_SUPPORTED;
    }
  }
  return ret;
}

int ObObjCasterV2::to_type(const ObObjType expect_type,
                           ObCastCtx &cast_ctx,
                           const ObObj &in_obj,
                           ObObj &buf_obj,
                           const ObObj *&res_obj)
{
  int ret = OB_SUCCESS;
  res_obj = NULL;
  cast_ctx.warning_ = OB_SUCCESS;
  if (OB_UNLIKELY(expect_type == in_obj.get_type() || ObNullType == in_obj.get_type())) {
    res_obj = &in_obj;
  } else if (OB_FAIL(to_type(expect_type, CS_TYPE_INVALID, cast_ctx, in_obj, buf_obj))) {
    LOG_WARN("failed to cast obj", K(ret), K(in_obj), K(expect_type), K(cast_ctx.cast_mode_));
  } else {
    res_obj = &buf_obj;
  }
  return ret;
}

int ObObjCasterV2::to_type(const ObObjType expect_type,
                           ObCastCtx &cast_ctx,
                           const ObObj &in_obj,
                           ObObj &out_obj)
{
  return to_type(expect_type, CS_TYPE_INVALID, cast_ctx, in_obj, out_obj);
}

int ObObjCasterV2::to_type(const ObObjType expect_type,
                           ObCollationType expect_cs_type,
                           ObCastCtx &cast_ctx,
                           const ObObj &in_obj,
                           ObObj &out_obj)
{
  int ret = OB_SUCCESS;
  const ObObjTypeClass in_tc = in_obj.get_type_class();
  const ObObjTypeClass out_tc = ob_obj_type_class(expect_type);
  cast_ctx.warning_ = OB_SUCCESS;
  if (CS_TYPE_INVALID != expect_cs_type) {
    cast_ctx.dest_collation_ = expect_cs_type;
  } else {
    cast_ctx.dest_collation_ = cast_ctx.dtc_params_.connection_collation_;
  }
  if (OB_UNLIKELY(ob_is_invalid_obj_tc(in_tc) || ob_is_invalid_obj_tc(out_tc))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected type", K(ret), K(in_obj), K(expect_type));
  } else if (OB_FAIL(OB_OBJ_CAST[in_tc][out_tc](expect_type, cast_ctx, in_obj, out_obj, cast_ctx.cast_mode_))) {
    LOG_WARN("failed to cast obj", K(ret), K(in_obj), K(in_tc), K(out_tc), K(expect_type), K(cast_ctx.cast_mode_));
  }

  if (OB_SUCC(ret)) {
    if (ObStringTC == out_tc || ObTextTC == out_tc || ObLobTC == out_tc) {
      if (ObStringTC == in_tc || ObTextTC == in_tc || ObLobTC == out_tc) {
        out_obj.set_collation_level(in_obj.get_collation_level());
      } else {
        out_obj.set_collation_level(CS_LEVEL_COERCIBLE);
      }
      if (OB_LIKELY(expect_cs_type != CS_TYPE_INVALID)) {
        out_obj.set_collation_type(expect_cs_type);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected collation type", K(ret), K(in_obj), K(out_obj), K(expect_cs_type), K(common::lbt()));
      }
    }
  }
  LOG_DEBUG("process to_type", K(ret), "in_type", in_obj.get_type(), K(in_obj), K(expect_type), K(out_obj), K(lbt()));
  return ret;
}

// given two objects with type A: a1 and a2, cast them to type B: b1 and b2,
// if in any case:
// a1 > a2 means b1 > b2, and
// a1 < a2 means b1 < b2, and
// a1 = a2 means b1 = b2,
// then type A and B is cast monotonic.
int ObObjCasterV2::is_cast_monotonic(ObObjType t1, ObObjType t2, bool &is_monotonic)
{
  int ret = OB_SUCCESS;
  ObObjTypeClass tc1 = ob_obj_type_class(t1);
  ObObjTypeClass tc2 = ob_obj_type_class(t2);
  if (OB_UNLIKELY(ob_is_invalid_obj_tc(tc1) || ob_is_invalid_obj_tc(tc2))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected type", K(ret), K(t1), K(t2), K(tc1), K(tc2));
  } else {
    is_monotonic = CAST_MONOTONIC[tc1][tc2];
  }
  return ret;
}

/* make sure that you have read the doc before you call these functions !
 *
 * doc:  http://www.atatech.org/articles/56575
 */

int ObObjCasterV2::is_order_consistent(const ObObjMeta &from,
                                       const ObObjMeta &to,
                                       bool &result)
{
  int ret = OB_SUCCESS;
  result = false;
  ObObjTypeClass tc1 = from.get_type_class();
  ObObjTypeClass tc2 = to.get_type_class();
  if (OB_UNLIKELY(ob_is_invalid_obj_tc(tc1) || ob_is_invalid_obj_tc(tc2))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected obj type class", K(ret), K(from), K(to));
  } else if (from.is_string_type() && to.is_string_type()) {
    ObCollationType res_cs_type = CS_TYPE_INVALID;
    ObCollationLevel res_cs_level = CS_LEVEL_INVALID;
    ObCollationType from_cs_type = from.get_collation_type();
    ObCollationType to_cs_type = to.get_collation_type();
    if (OB_FAIL(ObCharset::aggregate_collation(from.get_collation_level(),
                                               from_cs_type,
                                               to.get_collation_level(),
                                               to_cs_type,
                                               res_cs_level,
                                               res_cs_type))) {
      LOG_WARN("fail to aggregate collation", K(ret), K(from), K(to));
    } else {
      int64_t idx_from = get_idx_of_collate(from_cs_type);
      int64_t idx_to = get_idx_of_collate(to_cs_type);
      int64_t idx_res = get_idx_of_collate(res_cs_type);
      if (OB_UNLIKELY(idx_from < 0 || idx_to < 0 || idx_res < 0
        ||idx_from >= ObCharset::VALID_COLLATION_TYPES
        ||idx_to >= ObCharset::VALID_COLLATION_TYPES
        ||idx_res >= ObCharset::VALID_COLLATION_TYPES)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected collation type", K(ret), K(from), K(to));
      } else {
        result = ORDER_CONSISTENT_WITH_BOTH_STRING[idx_from][idx_to][idx_res];
      }
    }
  } else {
    result = ORDER_CONSISTENT[tc1][tc2];
  }
  return ret;
}

/* make sure that you have read the doc before you call these functions !
 *
 * doc:  http://www.atatech.org/articles/56575
 */

int ObObjCasterV2::is_injection(const ObObjMeta &from,
                                const ObObjMeta &to,
                                bool &result)
{
  int ret = OB_SUCCESS;
  result = false;
  ObObjTypeClass tc1 = from.get_type_class();
  ObObjTypeClass tc2 = to.get_type_class();
  if (OB_UNLIKELY(ob_is_invalid_obj_tc(tc1) || ob_is_invalid_obj_tc(tc2))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected obj type class", K(ret), K(from), K(to));
  } else if (from.is_string_type() && to.is_string_type()) {
    ObCollationType res_cs_type = CS_TYPE_INVALID;
    ObCollationLevel res_cs_level = CS_LEVEL_INVALID;
    ObCollationType from_cs_type = from.get_collation_type();
    ObCollationType to_cs_type = to.get_collation_type();
    if (OB_FAIL(ObCharset::aggregate_collation(from.get_collation_level(),
                                               from_cs_type,
                                               to.get_collation_level(),
                                               to_cs_type,
                                               res_cs_level,
                                               res_cs_type))) {
      LOG_WARN("fail to aggregate collation", K(ret), K(from), K(to));
    } else {
      int64_t idx_from = get_idx_of_collate(from_cs_type);
      int64_t idx_to = get_idx_of_collate(to_cs_type);
      int64_t idx_res = get_idx_of_collate(res_cs_type);
      if (OB_UNLIKELY(idx_from < 0 || idx_to < 0 || idx_res < 0
        ||idx_from >= ObCharset::VALID_COLLATION_TYPES
        ||idx_to >= ObCharset::VALID_COLLATION_TYPES
        ||idx_res >= ObCharset::VALID_COLLATION_TYPES)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected collation type", K(ret), K(from), K(to));
      } else {
        result = INJECTION_WITH_BOTH_STRING[idx_from][idx_to][idx_res];
      }
    }
  } else {
    result = INJECTION[tc1][tc2];
  }
  return ret;
}

const bool ObObjCasterV2::CAST_MONOTONIC[ObMaxTC][ObMaxTC] =
{
  // null
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
  },
  // int
  {
    false,  // null
    true,   // int
    true,   // uint
    true,   // float
    true,   // double
    true,   // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
  },
  // uint
  {
    false,  // null
    true,   // int
    true,   // uint
    true,   // float
    true,   // double
    true,   // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
  },
  // float
  {
    false,  // null
    true,   // int
    true,   // uint
    true,   // float
    true,   // double
    true,   // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
  },
  // double
  {
    false,  // null
    true,   // int
    true,   // uint
    true,   // float
    true,   // double
    true,   // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
  },
  // number
  {
    false,  // null
    true,   // int
    true,   // uint
    true,   // float
    true,   // double
    true,   // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
  },
  // datetime
  {
    false,  // null
    true,   // int
    true,   // uint
    true,   // float
    true,   // double
    true,   // number
    true,   // datetime
    true,   // date
    false,  // time
    true,   // year
    true,   // string
    false,  // extend
    false,  // unknown
    true,   // lob
  },
  // date
  {
    false,  // null
    true,   // int
    true,   // uint
    true,   // float
    true,   // double
    true,   // number
    true,   // datetime
    true,   // date
    false,  // time
    true,   // year
    true,   // string
    false,  // extend
    false,  // unknown
    true,   // lob
  },
  // time
  {
    false,  // null
    true,   // int
    true,   // uint
    true,   // float
    true,   // double
    true,   // number
    true,   // datetime
    false,  // date
    true,   // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    true,   // lob
  },
  // year
  {
    false,  // null
    true,   // int
    true,   // uint
    true,   // float
    true,   // double
    true,   // number
    true,   // datetime
    true,   // date
    false,  // time
    true ,  // year
    true,   // string
    false,  // extend
    false,  // unknown
    true,   // lob
  },
  // string
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    true,   // string
    false,  // extend
    false,  // unknown
    true,   // lob
  },
  // extend
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
  },
  // unknown
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
  },
  // lob
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    true,   // string
    false,  // extend
    false,  // unknown
    true,   // lob
  },
};

const bool ObObjCasterV2::ORDER_CONSISTENT[ObMaxTC][ObMaxTC] =
{
  // null
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
  },
  // int
  {
    false,  // null
    true,   // int
    true,   // uint
    true,   // float
    true,   // double
    true,   // number
    false,  // datetime
    false,  // date
    false,  // time
    true,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
  },
  // uint
  {
    false,  // null
    true,   // int
    true,   // uint
    true,   // float
    true,   // double
    true,   // number
    false,  // datetime
    false,  // date
    false,  // time
    true,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
  },
  // float
  {
    false,  // null
    false,   // int
    false,   // uint
    true,   // float
    true,   // double
    false,   // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
  },
  // double
  {
    false,  // null
    false,   // int
    false,   // uint
    true,   // float
    true,   // double
    false,   // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
  },
  // number
  {
    false,  // null
    true,   // int
    true,   // uint
    true,   // float
    true,   // double
    true,   // number
    false,  // datetime
    false,  // date
    false,  // time
    true,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
  },
  // datetime
  {
    false,  // null
    false,   // int
    false,   // uint
    false,   // float
    false,   // double
    false,   // number
    true,   // datetime
    true,   // date
    true,  // time
    true,   // year
    false,   // string
    false,  // extend
    false,  // unknown
    false,   // lob
  },
  // date
  {
    false,  // null
    false,   // int
    false,   // uint
    false,   // float
    false,   // double
    false,   // number
    true,   // datetime
    true,   // date
    true,  // time
    true,   // year
    false,   // string
    false,  // extend
    false,  // unknown
    false,   // lob
  },
  // time
  {
    false,  // null
    false,   // int
    false,   // uint
    false,   // float
    false,   // double
    false,   // number
    true,   // datetime
    true,  // date
    true,   // time
    true,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,   // lob
  },
  // year //0000-9999
  {
    false,  // null
    true,   // int
    true,   // uint
    true,   // float
    true,   // double
    true,   // number
    false,   // datetime
    false,   // date
    false,  // time
    true ,  // year
    false,   // string
    false,  // extend
    false,  // unknown
    false,   // lob
  },
  // string
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,   // string
    false,  // extend
    false,  // unknown
    false,   // lob
  },
  // extend
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
  },
  // unknown
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
  },
  // lob
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,   // string
    false,  // extend
    false,  // unknown
    false,   // lob
  },
};

const bool ObObjCasterV2::ORDER_CONSISTENT_WITH_BOTH_STRING[ObCharset::VALID_COLLATION_TYPES][ObCharset::VALID_COLLATION_TYPES][ObCharset::VALID_COLLATION_TYPES] =
{
  //CS_TYPE_UTF8MB4_GENERAL_CI
  {
      //ci    //utf8bin //bin
      {true, true, true},//CS_TYPE_UTF8MB4_GENERAL_CI
      {false, false , false},//CS_TYPE_UTF8MB4_BIN
      {false, false , false},//CS_TYPE_BINARY
  },
  //CS_TYPE_UTF8MB4_BIN
  {
      //ci    //utf8bin //bin
      {true, true , true},//CS_TYPE_UTF8MB4_GENERAL_CI
      {false, true , true},//CS_TYPE_UTF8MB4_BIN
      {false, true , true},//CS_TYPE_BINARY
  },
  //CS_TYPE_BINARY
  {
      //ci    //utf8bin //bin
      {true, true , true},//CS_TYPE_UTF8MB4_GENERAL_CI
      {false, true , true},//CS_TYPE_UTF8MB4_BIN
      {false, true , true},//CS_TYPE_BINARY
  }
};

const bool ObObjCasterV2::INJECTION[ObMaxTC][ObMaxTC] =
{
  // null
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
  },
  // int
  {
    false,  // null
    true,   // int
    true,   // uint
    true,   // float
    true,   // double
    true,   // number
    true,  // datetime
    true,  // date
    true,  // time
    true,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
  },
  // uint
  {
    false,  // null
    true,   // int
    true,   // uint
    true,   // float
    true,   // double
    true,   // number
    true,  // datetime
    true,  // date
    true,  // time
    true,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
  },
  // float
  {
    false,  // null
    false,   // int
    false,   // uint
    true,   // float
    true,   // double
    false,   // number
    true,  // datetime
    true,  // date
    true,  // time
    true,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
  },
  // double
  {
    false,  // null
    false,   // int
    false,   // uint
    true,   // float
    true,   // double
    false,   // number
    true,  // datetime
    true,  // date
    true,  // time
    true,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
  },
  // number
  {
    false,  // null
    true,   // int
    true,   // uint
    true,   // float
    true,   // double
    true,   // number
    true,  // datetime
    true,  // date
    true,  // time
    true,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
  },
  // datetime
  {
    false,  // null
    false,   // int
    false,   // uint
    false,   // float
    false,   // double
    false,   // number //2010-01-01 12:34:56.12345 = 20100101123456.1234520  and 2010-01-01 12:34:56.12345 = 20100101123456.1234530
    true,   // datetime
    true,   // date
    true,  // time
    true,   // year
    false,   // string
    false,  // extend
    false,  // unknown
    false,   // lob
  },
  // date
  {
    false,  // null
    false,   // int //think about 0000-00-00
    false,   // uint
    false,   // float
    false,   // double
    false,   // number
    true,   // datetime
    true,   // date
    true,  // time
    true,   // year
    false,   // string
    false,  // extend
    false,  // unknown
    false,   // lob
  },
  // time
  {
    false,  // null
    false,   // int
    false,   // uint
    false,   // float
    false,   // double
    false,   // number //think about time(5) = decimal(40,7)
    true,   // datetime
    true,  // date
    true,   // time
    true,  // year
    false,  // string //00:12:34 = "00:12:34" and 00:12:34 = "00:12:34.000"
    false,  // extend
    false,  // unknown
    false,   // lob
  },
  // year //0000-9999
  {
    false,  // null
    true,   // int
    true,   // uint
    true,   // float
    true,   // double
    true,   // number
    true,   // datetime //1999 = 1999-00-00 00:00:00
    true,   // date //1999 = 1999-00-00
    true,  // time
    true ,  // year
    false,   // string //1999 = "99" and 1999 = "1999"
    false,  // extend
    false,  // unknown
    false,   // lob
  },
  // string
  {
    false,  // null
    true,  // int
    true,  // uint
    true,  // float
    true,  // double
    true,  // number
    true,  // datetime
    true,  // date
    true,  // time
    true,  // year
    false,   // string
    false,  // extend
    false,  // unknown
    false,   // lob
  },
  // extend
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
  },
  // unknown
  {
    false,  // null
    false,  // int
    false,  // uint
    false,  // float
    false,  // double
    false,  // number
    false,  // datetime
    false,  // date
    false,  // time
    false,  // year
    false,  // string
    false,  // extend
    false,  // unknown
    false,  // lob
  },
  // lob
  {
    false,  // null
    true,  // int
    true,  // uint
    true,  // float
    true,  // double
    true,  // number
    true,  // datetime
    true,  // date
    true,  // time
    true,  // year
    false,   // string
    false,  // extend
    false,  // unknown
    false,   // lob
  },
};

const bool ObObjCasterV2::INJECTION_WITH_BOTH_STRING[ObCharset::VALID_COLLATION_TYPES][ObCharset::VALID_COLLATION_TYPES][ObCharset::VALID_COLLATION_TYPES] =
{
  //CS_TYPE_UTF8MB4_GENERAL_CI
  {
      //ci    //utf8bin //bin
      {true, true, true},//CS_TYPE_UTF8MB4_GENERAL_CI
      {false, true , true},//CS_TYPE_UTF8MB4_BIN
      {false, true , true},//CS_TYPE_BINARY
  },
  //CS_TYPE_UTF8MB4_BIN
  {
      //ci    //utf8bin //bin
      {true, true , true},//CS_TYPE_UTF8MB4_GENERAL_CI
      {false, true , true},//CS_TYPE_UTF8MB4_BIN
      {false, true , true},//CS_TYPE_BINARY
  },
  //CS_TYPE_BINARY
  {
      //ci    //utf8bin //bin
      {true, true , true},//CS_TYPE_UTF8MB4_GENERAL_CI
      {false, true , true},//CS_TYPE_UTF8MB4_BIN
      {false, true , true},//CS_TYPE_BINARY
  }
};




} // end namespace common
} // end namespace oceanbase
