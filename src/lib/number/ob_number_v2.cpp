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

#define USING_LOG_PREFIX LIB
#include "lib/number/ob_number_v2.h"
#include "lib/ob_define.h"
#include "lib/utility/utility.h"
#include "lib/charset/ob_dtoa.h"

namespace oceanbase
{
namespace common
{
namespace number
{
const char *ObNumber::DIGIT_FORMAT = "%09u";
const char *ObNumber::BACK_DIGIT_FORMAT[DIGIT_LEN] =
{
  "%09u",
  "%08u",
  "%07u",
  "%06u",
  "%05u",
  "%04u",
  "%03u",
  "%02u",
  "%01u",
};

ObNumber::ObNumber()
{
  desc_ = 0;
  exp_ = EXP_ZERO;
  sign_ = POSITIVE;
  digits_ = NULL;
}

ObNumber::ObNumber(const uint32_t desc, uint32_t *digits)
    :desc_(desc),
     digits_(digits)
{
}

ObNumber::~ObNumber()
{
  // do nothing
}

const ObNumber &ObNumber::get_positive_one()
{
  struct Init
  {
    explicit Init(ObNumber &v)
    {
      static StackAllocator sa;
      v.from("1", sa);
    };
  };
  static ObNumber one;
  static Init init(one);
  return one;
}

const ObNumber &ObNumber::get_zero()
{
  struct Init
  {
    explicit Init(ObNumber &v)
    {
      v.set_zero();
    };
  };
  static ObNumber one;
  static Init init(one);
  return one;
}

uint32_t *ObNumber::alloc_(IAllocator &allocator, const int64_t num)
{
  if (0 >= num) {
    digits_ = NULL;
    cap_ = 0;
  } else if (NULL == (digits_ = allocator.alloc(num * sizeof(uint32_t)))) {
    cap_ = 0;
  } else {
    cap_ = (uint8_t)num;
  }
  return digits_;
}

int ObNumber::from_(const int64_t value, IAllocator &allocator)
{
  const int64_t INT_BUF_SIZE = 32;
  char buf[INT_BUF_SIZE];
  (void)snprintf(buf, INT_BUF_SIZE, "%ld", value);
  return from_(buf, allocator);
}

int ObNumber::from_(const uint64_t value, IAllocator &allocator)
{
  const int64_t INT_BUF_SIZE = 32;
  char buf[INT_BUF_SIZE];
  (void)snprintf(buf, INT_BUF_SIZE, "%lu", value);
  return from_(buf, allocator);
}

int ObNumber::from_(const char *str, IAllocator &allocator, int16_t *precision, int16_t *scale)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  int64_t length = (NULL == str) ? 0 : strlen(str);
  ret = from_(str, length, allocator, warning, precision, scale);
  if (OB_SUCCESS == ret && OB_SUCCESS != warning) {
    ret = warning;
  }
  return ret;
}

int ObNumber::from_(const char *str, const int64_t length, IAllocator &allocator, int &warning,
                    int16_t *precision, int16_t *scale)
{
  int ret = OB_SUCCESS;
  ObTRecover<ObNumber> recover_guard(*this, OB_SUCCESS, ret);

  uint32_t digits[MAX_CALC_LEN];
  ObNumberBuilder nb;
  nb.digits_ = digits;
  nb.cap_ = MAX_CALC_LEN;

  if (OB_UNLIKELY(NULL == str || length <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid param", K(length), K(str), K(ret));
  } else if (OB_FAIL(nb.build(str, length, warning, precision, scale))) {
    _OB_LOG(WDIAG, "number build from fail, ret=%d str=[%.*s]", ret, (int)length, str);
  } else if (OB_FAIL(nb.round_scale_(FLOATING_SCALE, true))) {
    _OB_LOG(WDIAG, "round scale fail, ret=%d str=[%.*s]", ret, (int)length, str);
  } else if (OB_FAIL(exp_check_(nb.get_desc()))) {
    LOG_WDIAG("exponent precision check fail", K(ret));
    if (OB_DECIMAL_PRECISION_OVERFLOW == ret) {
      set_zero();
      ret = OB_SUCCESS;
    }
  } else if (0 == nb.len_) {
    set_zero();
  } else if (OB_ISNULL(digits_ = alloc_(allocator, nb.len_))) {
    _OB_LOG(WDIAG, "alloc digits fail, length=%hhu", nb.len_);
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMCPY(digits_, nb.digits_, nb.len_ * ITEM_SIZE(digits_));
    uint8_t tmp = cap_;
    desc_ = nb.desc_;
    cap_ = tmp;
  }
  if (OB_FAIL(ret)) {
    set_zero();
  }
  return ret;
}

int ObNumber::from_(const uint32_t desc, const ObCalcVector &vector, IAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObTRecover<ObNumber> recover_guard(*this, OB_SUCCESS, ret);

  ObCalcVector normalized_vector = vector;
  if (OB_FAIL(normalized_vector.normalize())) {
    LOG_WDIAG("normalized_vector.normalize() fails", K(ret));
  }
  Desc d;
  d.desc_ = desc;
  d.len_ = (uint8_t)std::min(+MAX_STORE_LEN, normalized_vector.size());
  if (OB_FAIL(ret)) {
    LOG_WDIAG("Previous normalize() fails", K(ret));
  } else if (0 == d.len_) {
    set_zero();
  } else if (OB_ISNULL(digits_ = alloc_(allocator, d.len_))) {
    LOG_WDIAG("alloc digits fail", K(d.len_));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMCPY(digits_, normalized_vector.get_digits(), d.len_ * ITEM_SIZE(digits_));
    sign_ = d.sign_;
    exp_ = d.exp_;
    if (OB_FAIL(normalize_(digits_, d.len_))) {
      _OB_LOG(WDIAG, "normalize [%s] fail, ret=%d", to_cstring(*this), ret);
    } else if (OB_FAIL(round_scale_(FLOATING_SCALE, true))) {
      LOG_WDIAG("round scale fail", K(ret), K(*this));
    } else if (OB_FAIL(exp_check_(desc_))) {
      LOG_WDIAG("exponent precision check fail", K(ret), K(*this));
      if (OB_DECIMAL_PRECISION_OVERFLOW == ret) {
        set_zero();
        ret = OB_SUCCESS;
      }
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObNumber::from_(
    const int16_t precision,
    const int16_t scale,
    const char *str,
    const int64_t length,
    IAllocator &allocator)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  ObTRecover<ObNumber> recover_guard(*this, OB_SUCCESS, ret);

  if (OB_UNLIKELY(MIN_PRECISION > precision
      || MAX_PRECISION < precision
      || MIN_SCALE > scale
      || MAX_SCALE < scale
      || NULL == str
      || 0 >= length)) {
    LOG_WDIAG("invalid param",
             K(precision), K(scale), K(str), K(length));
    ret = OB_INVALID_ARGUMENT;
  } else {
    uint32_t digits[MAX_CALC_LEN];
    ObNumberBuilder nb;
    nb.digits_ = digits;
    nb.cap_ = MAX_CALC_LEN;

    if (OB_FAIL(nb.build(str, length, warning))) {
      _OB_LOG(WDIAG, "number build from fail, ret=%d str=[%.*s]", ret, (int)length, str);
    } else if (OB_FAIL(nb.check_and_round(precision, scale))) {
      _OB_LOG(WDIAG, "check and round fail, ret=%d str=[%.*s]", ret, (int)length, str);
    } else if (0 == nb.len_) {
      set_zero();
    } else if (OB_ISNULL(digits_ = alloc_(allocator, nb.len_))) {
      LOG_WDIAG("alloc digits fail", K(nb.len_));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(digits_, nb.digits_, nb.len_ * ITEM_SIZE(digits_));
      uint8_t tmp = cap_;
      desc_ = nb.desc_;
      cap_ = tmp;
    }
  }
  if (OB_FAIL(ret)) {
    set_zero();
  } else if (OB_SUCCESS != warning) {
    ret = warning;
  } else { /* Do nothing */ }
  return ret;
}

int ObNumber::from_(const ObNumber &other, IAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObTRecover<ObNumber> recover_guard(*this, OB_SUCCESS, ret);

  if (&other == this) {
    // assign to self
  } else if (0 == other.len_) {
    set_zero();
  } else if (OB_ISNULL(digits_ = alloc_(allocator, other.len_))) {
    _OB_LOG(DEBUG, "alloc digits fail, length=%hhu", other.len_);
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMCPY(digits_, other.digits_, other.len_ * ITEM_SIZE(digits_));
    uint8_t tmp = cap_;
    desc_ = other.desc_;
    cap_ = tmp;
  }
  return ret;
}

int ObNumber::from_(const ObNumber &other, uint32_t *digits, uint8_t cap)
{
  int ret = OB_SUCCESS;
  if (&other == this) {
    // assign to self
  } else if (0 == other.len_) {
    set_zero();
  } else if (OB_UNLIKELY(NULL == digits || cap < other.len_)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    digits_ = digits;
    MEMCPY(digits_, other.digits_, other.len_ * ITEM_SIZE(digits_));
    desc_ = other.desc_;
    cap_ = cap;
  }
  return ret;
}

int ObNumber::floor(const int64_t scale)
{
  int ret = OB_SUCCESS;
  int int_len = 0;
  ObTRecover<ObNumber> recover_guard(*this, OB_SUCCESS, ret);
  if (OB_UNLIKELY(0 != scale)) {
    LOG_WDIAG("invalid param", K(scale));
    ret = OB_INVALID_ARGUMENT;
  } else if (is_zero()) {
    ret = OB_SUCCESS;
  } else if (OB_UNLIKELY(NULL == digits_
             || 0 >= len_)) {
    ret = OB_NOT_INIT;
  } else {
    if (POSITIVE == sign_) {
      int_len = se_ - POSITIVE_EXP_BOUNDARY;
      if (int_len >= 0) {
        int_len += 1;
      } else {
        int_len = -1;
      }
    } else if (NEGATIVE == sign_) {
      int_len = NEGATIVE_EXP_BOUNDARY - se_;
      if (int_len >= 0) {
        int_len += 1;
      } else {
        int_len = -1;
      }
    } else { /* Do nothing */ }
    if (int_len > 0 && int_len < len_) {
      if (POSITIVE == sign_) {
        for (int64_t i = int_len; i < len_; ++i) {
          digits_[i] = 0;
        }
      } else if (NEGATIVE == sign_) {
        for (int64_t i = int_len; i < len_; ++i) {
          digits_[i] = 0;
        }
        for (int64_t i = int_len - 1; i >= 0; --i) {
          if ((BASE - 1) != digits_[i]) {
            digits_[i] += 1;
            break;
          } else {
            digits_[i] = 0;
          }
        }
        if (0 == digits_[0] && NEGATIVE == sign_) {
          for (int64_t i = (int_len - 1); i >= 0; --i) {
            digits_[i + 1] = digits_[i];
          }
          digits_[0] = 1;
          --exp_;
        }
      } else { /* Do nothing */ }
      if (int_len < MAX_STORE_LEN) {
        len_ = static_cast<unsigned char>(int_len);
      }
    } else if (-1 == int_len && OB_SUCCESS == ret) {
      for (int64_t i = 0; i < len_; ++i) {
        digits_[i] = 0;
      }
      if (POSITIVE == sign_) {
        set_zero();
      } else if (NEGATIVE == sign_) {
        digits_[0] = 1;
        se_ = 0x40;
        len_ = 1;
      }
    } else { /* Do nothing */ }
  }
  return ret;
}

int ObNumber::ceil(const int64_t scale)
{
  int ret = OB_SUCCESS;
  int int_len = 0;
  ObTRecover<ObNumber> recover_guard(*this, OB_SUCCESS, ret);
  if (OB_UNLIKELY(0 != scale)) {
    LOG_WDIAG("invalid param", K(scale));
    ret = OB_INVALID_ARGUMENT;
  } else if (is_zero()) {
    ret = OB_SUCCESS;
  } else if (OB_UNLIKELY(NULL == digits_
             || 0 >= len_)) {
    ret = OB_NOT_INIT;
  } else {
    if (POSITIVE == sign_) {
      int_len = se_ - POSITIVE_EXP_BOUNDARY;
      if (int_len >= 0) {
        int_len += 1;
      } else {
        int_len = -1;
      }
    } else if (NEGATIVE == sign_) {
      int_len = NEGATIVE_EXP_BOUNDARY - se_;
      if (int_len >= 0) {
        int_len += 1;
      } else {
        int_len = -1;
      }
    } else { /* Do nothing */ }
    if (int_len > 0 && int_len < len_) {
      for (int64_t i = int_len; i < len_; ++i) {
        digits_[i] = 0;
      }
      len_ = static_cast<uint8_t>(int_len);
      if (POSITIVE == sign_) {
        for (int64_t i = int_len - 1; i >= 0; --i) {
          if ((BASE - 1) != digits_[i]) {
            digits_[i] += 1;
            break;
          } else {
            digits_[i] = 0;
          }
        }
        if (0 == digits_[0]) {
          // since int_len is less than len_, so the loop below is safe,
          // we won't get a buffer overflow problem.
          for (int64_t i = (int_len - 1); i >= 0; --i) {
            digits_[i + 1] = digits_[i];
          }
          digits_[0] = 1;
          ++len_;
          ++exp_;
        }
      }
    } else if (int_len < 0) {
      for (int64_t i = 0; i < len_; ++i) {
        digits_[i] = 0;
      }
      if (POSITIVE == sign_) {
        digits_[0] = 1;
        se_ = 0xc0;
        len_ = 1;
      } else if (NEGATIVE == sign_) {
        set_zero();
      }
    } else { /* Do nothing */ }
  }
  return ret;
}

int ObNumber::trunc(const int64_t scale)
{
  int ret = OB_SUCCESS;
  ObTRecover<ObNumber> recover_guard(*this, OB_SUCCESS, ret);

  if (OB_UNLIKELY(MIN_SCALE > scale
      || MAX_SCALE < scale)) {
    LOG_WDIAG("invalid param", K(scale));
    ret = OB_INVALID_ARGUMENT;
  } else if (is_zero()) {
    ret = OB_SUCCESS;
  } else if (OB_UNLIKELY(NULL == digits_
             || 0 >= len_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(trunc_scale_(scale, false))) {
    LOG_WDIAG("trunc scale failed", K(*this), K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObNumber::round(const int64_t scale)
{
  int ret = OB_SUCCESS;
  ObTRecover<ObNumber> recover_guard(*this, OB_SUCCESS, ret);

  if (OB_UNLIKELY(MIN_SCALE > scale
      || MAX_SCALE < scale)) {
    LOG_WDIAG("invalid param", K(scale));
    ret = OB_INVALID_ARGUMENT;
  } else if (is_zero()) {
    ret = OB_SUCCESS;
  } else if (OB_UNLIKELY(NULL == digits_
             || 0 >= len_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(round_scale_(scale, false))) {
    //_OB_LOG(WDIAG, "Buffer overflow, %s", to_cstring(*this));
  } else {
    // do nothing
  }
  return ret;
}

int ObNumber::check_and_round(const int64_t precision, const int64_t scale)
{
  int ret = OB_SUCCESS;
  ObTRecover<ObNumber> recover_guard(*this, OB_SUCCESS, ret);

  if (is_zero()) {
    ret = OB_SUCCESS;
  } else if (OB_UNLIKELY(NULL == digits_
             || 0 >= len_)) {
    ret = OB_NOT_INIT;
  } else if (INT64_MAX != precision
             && OB_FAIL(round_scale_(scale, false))) {
    //_OB_LOG(WDIAG, "Buffer overflow, %s", to_cstring(*this));
  } else if (INT64_MAX != precision && INT64_MAX != scale
             && OB_FAIL(check_precision_(precision, scale))) {
    //_OB_LOG(WDIAG, "Precision overflow, %s", to_cstring(*this));
  } else {
    // do nothing
  }
  return ret;
}

void ObNumber::assign(const uint32_t desc, uint32_t *digits)
{
  // no need check whether digits is null. 0 is null
  desc_ = desc;
  digits_ = digits;
}

int64_t ObNumber::to_string(char *buffer, const int64_t length) const
{
  int64_t pos = 0;
  if (len_ > 0 && OB_ISNULL(digits_)) {
    LOG_EDIAG("the pointer is null");
  } else {
    databuff_printf(buffer, length, pos, "\"sign=%hhu exp=%hhu se=0x%hhx len=%hhu digits=[", sign_,
                    exp_, se_, len_);
    for (uint8_t i = 0; i < len_; ++i) {
      databuff_printf(buffer, length, pos, "%u,", digits_[i]);
    }
    databuff_printf(buffer, length, pos, "]\"");
  }

  return pos;
}

void ObNumber::set_zero()
{
  len_ = 0;
  sign_ = POSITIVE;
  exp_ = 0;
}

bool ObNumber::is_negative() const
{
  return (NEGATIVE == sign_);
}

bool ObNumber::is_zero() const
{
  return (0 == len_);
}

bool ObNumber::is_decimal() const
{
  bool bret = false;
  if (POSITIVE == sign_) {
    bret = (0 > (exp_ - EXP_ZERO));
  } else {
    bret = (0 < (exp_ - EXP_ZERO));
  }
  return bret;
}

bool ObNumber::is_integer() const
{
  bool bret = false;
  int int_len = 0;
  if (is_zero()) {
    bret = true;
  } else if (OB_UNLIKELY(NULL == digits_ || 0 >= len_)) {
    bret = false;
    _OB_LOG(EDIAG, "not init");
  } else {
    if (POSITIVE == sign_) {
      int_len = se_ - POSITIVE_EXP_BOUNDARY;
      if (int_len >= 0) {
        int_len += 1;
      } else {
        int_len = -1;
      }
    } else if (NEGATIVE == sign_) {
      int_len = NEGATIVE_EXP_BOUNDARY - se_;
      if (int_len >= 0) {
        int_len += 1;
      } else {
        int_len = -1;
      }
    } else { /* Do nothing */ }
    if (int_len < 0 || (int_len > 0 && int_len < len_)) {
      bret = false;
    } else {
      bret = true;
    }
  }
  return bret;
}

uint32_t ObNumber::get_desc() const
{
  return desc_;
}

ObNumberDesc ObNumber::get_number_desc() const
{
  ObNumberDesc d;
  d.desc_ = desc_;
  return d;
}

int64_t ObNumber::get_cap() const
{
  return cap_;
}

uint32_t *ObNumber::get_digits() const
{
  return digits_;
}

int64_t ObNumber::get_length() const
{
  return len_;
}

int64_t ObNumber::get_scale() const
{
  int64_t decimal_count = 0;
  if (!is_zero()) {
    const int64_t expr_value = get_decode_exp(desc_);
    //xxx_length means xx digit array length
    const int64_t integer_length = (expr_value >= 0 ? (expr_value + 1) : 0);
    //e.g. 1,000000000,000000000,   digits=[1], expr_value=2, len_=1,
    //integer_length=3, valid_integer_length=1
    // const int64_t valid_integer_length = (d_.len_ > integer_length ? integer_length : d_.len_);

    //len_ > integer_length means have decimal
    const int64_t valid_decimal_length = (len_ > integer_length ? (len_ - integer_length) : 0);
    //e.g. 0.000000000 000000001, digits=[1], expr_value=-2, len_=1, decimal_length=2,
    //valid_decimal_length=1
    const int64_t decimal_length = valid_decimal_length
                                   + (0 == integer_length ? (0 - expr_value - 1) : 0);

    int64_t tail_decimal_zero_count = 0;
    if (valid_decimal_length > 0) {
      remove_back_zero(digits_[len_ - 1], tail_decimal_zero_count);
    }
    decimal_count = decimal_length * DIGIT_LEN - tail_decimal_zero_count;
    LIB_LOG(DEBUG, "get_scale", KPC(this), K(expr_value),
            K(integer_length), K(valid_decimal_length), K(decimal_length),
            K(tail_decimal_zero_count), K(decimal_count));
  }
  return decimal_count;
}

bool ObNumber::is_valid_uint64(uint64_t &uint64)
{
  bool bret = false;
  uint64_t tmp_int_parts = 0;
  uint64_t tmp_decimal_parts = 0;
  if (OB_UNLIKELY(OB_SUCCESS != check_range(&bret, NULL, tmp_int_parts, tmp_decimal_parts))) {
    LOG_WDIAG("can't to check the param range", K(bret));
  } else {
    uint64 = tmp_int_parts;
    bret = bret && (0 == tmp_decimal_parts);//Should not use if-test.
  }

  return bret;
}

bool ObNumber::is_valid_int64(int64_t &int64)
{
  bool bret = false;
  uint64_t tmp_int_parts = 0;
  uint64_t tmp_decimal_parts = 0;
  if (OB_UNLIKELY(OB_SUCCESS != check_range(NULL, &bret, tmp_int_parts, tmp_decimal_parts))) {
    LOG_WDIAG("can't to check the param range", K(bret));
  } else {
    int64 = is_negative() ? (-1 * tmp_int_parts) : tmp_int_parts;
    bret = bret && (0 == tmp_decimal_parts);
  }

  return bret;
}

bool ObNumber::is_int_parts_valid_int64(int64_t &int_parts, int64_t &decimal_parts)
{
  bool bret = false;
  uint64_t tmp_int_parts = 0;
  uint64_t tmp_decimal_parts = 0;
  if (OB_UNLIKELY(OB_SUCCESS != check_range(NULL, &bret, tmp_int_parts, tmp_decimal_parts))) {
    LOG_WDIAG("can't to check the param range", K(bret));
  } else {
    decimal_parts = tmp_decimal_parts;
    int_parts = is_negative() ? (-1 * tmp_int_parts) : tmp_int_parts;
  }

  return bret;
}

int ObNumber::check_range(bool *is_valid_uint64, bool *is_valid_int64,
                           uint64_t &int_parts, uint64_t &decimal_parts) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((NULL == is_valid_uint64) && (NULL == is_valid_int64))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_EDIAG("the param is invalid", K(ret));
  } else {
    bool *is_valid = NULL;
    static const int FLAG_INT64 = 0;
    static const int FLAG_UINT64 = 1;
    static const int SIGN_NEG = 0;
    static const int SIGN_POS = 1;
    int flag = FLAG_INT64;
    if (NULL != is_valid_uint64) {
      is_valid = is_valid_uint64;
      flag = FLAG_UINT64;
    } else {
      is_valid = is_valid_int64;
    }
    *is_valid = true;
    if (OB_UNLIKELY(is_zero())) {
      //do nothing
    } else {
      int sign = is_negative() ? SIGN_NEG : SIGN_POS;
      if ((SIGN_NEG == sign) && (FLAG_UINT64 == flag)) {
        *is_valid = false; //there are no neg guys in uint64 !
      }
      uint64_t value = 0;
      static uint64_t ABS_INT64_MIN = 9223372036854775808ULL;
                                           //int64        uint64
      static uint64_t THRESHOLD[2][2] = {{ABS_INT64_MIN, 0}, //neg
                                         {INT64_MAX,     UINT64_MAX}}; //pos
      ObDigitIterator di;
      di.assign(desc_, digits_);
      uint32_t digit = 0;
      bool from_integer = false;
      bool last_decimal = false;
      while (OB_SUCCESS == di.get_next_digit(digit, from_integer, last_decimal)) {
        if (from_integer) {
          //In case of overflow we can not write "value * BASE + digit <= THRESHOLD[index]"
          if (*is_valid && value <= (THRESHOLD[sign][flag] - digit) / BASE) {
            value = value * BASE + digit;
          } else {
            *is_valid = false; //no break
          }
        } else {
          decimal_parts = digit;
          break;
        }
      }
      int_parts = value; //no matter valid or not, we set int_parts always here.
    }
  }
  return ret;
}

int ObNumber::check_precision_(const int64_t precision, const int64_t scale)
{
  int ret = OB_SUCCESS;
  int64_t limit = precision - scale;
  ObDigitIterator di;
  di.assign(desc_, digits_);
  int64_t integer_counter = 0;
  int64_t decimal_zero_counter = 0;
  uint32_t digit = 0;
  bool head_flag = true;
  bool from_integer = false;
  bool last_decimal = false;
  while (OB_SUCCESS == (ret = di.get_next_digit(digit, from_integer, last_decimal))) {
    if (from_integer) {
      if (head_flag) {
        integer_counter += get_digit_len(digit);
      } else {
        integer_counter += DIGIT_LEN;
      }
      if (OB_UNLIKELY(integer_counter > limit)) {
        _OB_LOG(WDIAG, "Precision=%ld scale=%ld integer_number=%ld precision overflow %s",
                  precision, scale, integer_counter, to_cstring(*this));
        ret = OB_INTEGER_PRECISION_OVERFLOW;
        break;
      }
    } else {
      if (0 != digit) {
        decimal_zero_counter += (DIGIT_LEN - get_digit_len(digit));
      } else {
        decimal_zero_counter += DIGIT_LEN;
      }
      if (decimal_zero_counter >= -limit) {
        break;
      }
    }
    head_flag = false;
  }
  ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
  if (OB_UNLIKELY(OB_SUCCESS == ret
      && decimal_zero_counter < -limit)) {
    LOG_WDIAG("precision overflow ",
             K(precision), K(scale), K(decimal_zero_counter), K(*this));
    ret = OB_DECIMAL_PRECISION_OVERFLOW;
  }
  return ret;
}

int ObNumber::round_scale_(const int64_t scale, const bool using_floating_scale)
{
  int ret = OB_SUCCESS;
  ObDigitIterator di;
  di.assign(desc_, digits_);

  uint32_t digit = 0;
  bool from_integer = false;
  bool last_decimal = false;
  uint32_t integer_digits[MAX_CALC_LEN];
  int64_t integer_length = 0;
  int64_t integer_counter = 0;
  uint32_t decimal_digits[MAX_CALC_LEN];
  int64_t decimal_length = 0;
  while (OB_SUCCESS == di.get_next_digit(digit, from_integer, last_decimal)) {
    if (OB_UNLIKELY(MAX_CALC_LEN <= integer_length
        || MAX_CALC_LEN <= decimal_length)) {
      LOG_WDIAG("buffer size overflow",
                K(integer_length), K(decimal_length));
      ret = OB_NUMERIC_OVERFLOW;
      break;
    }
    if (from_integer) {
      if (0 == integer_length) {
        integer_counter += get_digit_len(digit);
      } else {
        integer_counter += DIGIT_LEN;
      }

      integer_digits[integer_length++] = digit;
    } else if (0 <= scale) {
      if (using_floating_scale
          && 0 == digit
          && 0 == integer_length
          && 0 == decimal_length) {
        continue;
      }
      decimal_digits[decimal_length++] = digit;
      if ((decimal_length * DIGIT_LEN) > scale) {
        break;
      }
    } else {
      break;
    }
  }
  int64_t floating_scale = scale;
  if (OB_SUCCESS == ret
      && using_floating_scale) {
    floating_scale -= integer_counter;
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (0 < floating_scale) {
    if (OB_FAIL(round_decimal_(floating_scale, decimal_digits, decimal_length))) {
      LOG_EDIAG("fail to get round_decimal");
    } else {
      ret = rebuild_digits_(integer_digits, integer_length, decimal_digits, decimal_length);
    }
  } else if (0 > floating_scale) {
    if (OB_FAIL(round_integer_(-floating_scale, integer_digits, integer_length))) {
      LOG_EDIAG("fail to get round_integer");
    } else {
      ret = rebuild_digits_(integer_digits, integer_length, NULL, 0);
    }

  } else if (0 == floating_scale) {
    if (0 < decimal_length) {
      if ((5 * BASE / 10) <= decimal_digits[0]) {
        decimal_digits[0] = BASE;
      } else {
        decimal_digits[0] = 0;
      }
      ret = rebuild_digits_(integer_digits, integer_length, decimal_digits, 1);
    } else {
      ret = rebuild_digits_(integer_digits, integer_length, NULL, 0);
    }
  } else { /* Do nothing */ }
  return ret;
}

int ObNumber::round_integer_(
    const int64_t scale, // >= 1
    uint32_t *integer_digits,
    int64_t &integer_length)
{
  static const uint64_t ROUND_POWS[] = {0, 10, 100, 1000, 10000, 100000,
                                        1000000, 10000000, 100000000};
  int ret = OB_SUCCESS;
  if (OB_ISNULL(integer_digits) || OB_UNLIKELY(0 == DIGIT_LEN || scale < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_EDIAG("the pointer is null or the DIGIT_LEN IS ZERO or the scale < 1", K(ret));
  } else {
    int64_t round_length = scale / DIGIT_LEN;
    int64_t round = scale % DIGIT_LEN;
    if (integer_length < round_length) {
      integer_length = 0;
    } else {
      if (0 == round) { // scale >= 9, round_length >= 1
        integer_length -= (round_length - 1);
        if ((5 * BASE / 10) <= integer_digits[integer_length - 1]) {
          integer_digits[integer_length - 1] = BASE;
        } else {
          integer_digits[integer_length - 1] = 0;
        }
      } else {
        integer_length -= round_length;
        uint64_t roundv = (uint64_t)integer_digits[integer_length - 1]
                          + 5 * ROUND_POWS[round] / 10;
        roundv /= ROUND_POWS[round];
        roundv *= ROUND_POWS[round];
        integer_digits[integer_length - 1] = (uint32_t)roundv;
      }
    }
  }
  return ret;
}

int ObNumber::round_decimal_(
    const int64_t scale, // >= 1
    uint32_t *decimal_digits,
    int64_t &decimal_length)
{
  static const uint64_t ROUND_POWS[] = {0, 100000000, 10000000, 1000000,
                                        100000, 10000, 1000, 100, 10};
  int ret = OB_SUCCESS;
  if (OB_ISNULL(decimal_digits) || OB_UNLIKELY(0 == DIGIT_LEN || scale < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_EDIAG("the pointer is null or the DIGIT_LEN IS ZERO or the scale < 1", K(ret));
  } else {
    int64_t round_length = scale / DIGIT_LEN;
    int64_t round = scale % DIGIT_LEN;

    if (decimal_length <= round_length) {
      // do nothing
    } else {
      decimal_length = std::min(decimal_length, round_length + 1);
      if (0 == round) { // scale >= 9, decimal_length >= 2
        if ((5 * BASE / 10) <= decimal_digits[decimal_length - 1]) {
          decimal_digits[decimal_length - 1] = BASE;
        } else {
          decimal_digits[decimal_length - 1] = 0;
        }
      } else {
        uint64_t roundv = (uint64_t)decimal_digits[decimal_length - 1] + 5 * ROUND_POWS[round] / 10;
        roundv /= ROUND_POWS[round];
        roundv *= ROUND_POWS[round];
        decimal_digits[decimal_length - 1] = (uint32_t)roundv;
      }
    }
  }

  return ret;
}

int ObNumber::trunc_scale_(int64_t scale, bool using_floating_scale)
{
  int ret = OB_SUCCESS;
  ObDigitIterator di;
  di.assign(desc_, digits_);
  uint32_t digit = 0;
  bool from_integer = false;
  bool last_decimal = false;
  uint32_t integer_digits[MAX_CALC_LEN];
  int64_t integer_length = 0;
  int64_t integer_counter = 0;
  uint32_t decimal_digits[MAX_CALC_LEN];
  int64_t decimal_length = 0;
  while (OB_SUCCESS == di.get_next_digit(digit, from_integer, last_decimal)) {
    if (OB_UNLIKELY(MAX_CALC_LEN <= integer_length
        || MAX_CALC_LEN <= decimal_length)) {
      LOG_WDIAG("buffer size overflow", K(integer_length), K(decimal_length));
      ret = OB_NUMERIC_OVERFLOW;
      break;
    }
    if (from_integer) {
      if (0 == integer_length) {
        integer_counter += get_digit_len(digit);
      } else {
        integer_counter += DIGIT_LEN;
      }
      integer_digits[integer_length ++] = digit;
    } else if (0 <= scale) {
      if (using_floating_scale
          && 0 == digit
          && 0 == integer_length
          && 0 == decimal_length) {
        continue;
      }
      decimal_digits[decimal_length ++] = digit;
      if ((decimal_length * DIGIT_LEN) > scale) {
        break;
      }
    } else {
      break;
    }
  }

  int64_t floating_scale = scale;
  if (OB_SUCCESS == ret
      && using_floating_scale) {
    floating_scale -= integer_counter;
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (0 < floating_scale) {
    if (OB_FAIL(trunc_decimal_(floating_scale, decimal_digits, decimal_length))) {
      LOG_EDIAG("fail to get trunc decimal", K(ret));
    } else {
      ret = rebuild_digits_(integer_digits, integer_length, decimal_digits, decimal_length);
    }
  } else if (0 > floating_scale) {
    if (OB_FAIL(trunc_integer_(-floating_scale, integer_digits, integer_length))) {
      LOG_EDIAG("fail to get trunc integer", K(ret));
    } else {
      ret = rebuild_digits_(integer_digits, integer_length, NULL, 0);
    }

  } else if (0 == floating_scale) {
    ret = rebuild_digits_(integer_digits, integer_length, NULL, 0);
  } else { /* Do nothing */ }
  return ret;
}

int ObNumber::trunc_integer_(
    const int64_t scale,
    uint32_t *integer_digits,
    int64_t &integer_length)
{
  int ret = OB_SUCCESS;
  static const uint64_t TRUNC_POWS[] = {0, 10, 100, 1000, 10000, 100000,
                                        1000000, 10000000, 100000000};
  if (OB_UNLIKELY(0 == DIGIT_LEN || scale <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(EDIAG, "DIGIT_LEN is zero or scale is not positive", K(ret), K(scale));
  } else if (integer_length > 0) {
    //tips: in terms of "3333123456789555555555.123"
    //integer_length = 3 and integer_digits[0]=3333, integer_digits[1] = 123456789 integer_digits[2] = 555555555
    int64_t trunc_length = (scale - 1) / DIGIT_LEN + 1;
    int64_t trunc = scale % DIGIT_LEN;
    if (integer_length < trunc_length) {
      integer_length = 0;
    } else {
      if (0 == trunc) { //such as "3333123456789555555555.123" and scale = 9
        integer_length -= trunc_length;
      } else { //such as "3333123456789555555555.123" and scale = 3, change 555555555 to 555555000
        //integer_length >= trunc_length and trunc_length > 0 , so it holds that
        //integer_length - trunc_length + 1 - 1 = integer_length - trunc_length >= 0 and
        //integer_length - trunc_length + 1 - 1 = integer_length - trunc_length <  integer_length
        //we need to check the bound here and can use integer_digits[integer_length - 1] safety
        integer_length -= (trunc_length - 1);
        uint64 truncdv = static_cast<uint64_t>(integer_digits[integer_length - 1]);
        truncdv /= TRUNC_POWS[trunc];
        truncdv *= TRUNC_POWS[trunc];
        integer_digits[integer_length - 1] = static_cast<uint32_t>(truncdv);
      }
    }
  }
  return ret;
}

int ObNumber::trunc_decimal_(
    const int64_t scale,
    uint32_t *decimal_digits,
    int64_t &decimal_length)
{
  int ret = OB_SUCCESS;
  static const uint64_t TRUNC_POWS[] = {0, 100000000, 10000000, 1000000,
                                        100000, 10000, 1000, 100, 10};
  if (OB_UNLIKELY(0 == DIGIT_LEN || scale <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_EDIAG("the DIGIT_LEN is zero or scale is not positive", K(ret), K(scale));
  } else if (decimal_length > 0) {
    int64_t trunc_length = (scale - 1) / DIGIT_LEN + 1;
    int64_t trunc = scale % DIGIT_LEN;
    if (decimal_length < trunc_length) {
      // do nothing
    } else {
      decimal_length = trunc_length;
      if (0 == trunc) {
        //such as : "1.123456789123456789" when scale = 9 trunc will be 0
        //decimal_length will be changed from 2 to 1 (trunc_length).
        //and, we do nothing here.
      } else {
        uint64 truncdv = static_cast<uint64_t>(decimal_digits[decimal_length - 1]);
        truncdv /= TRUNC_POWS[trunc];
        truncdv *= TRUNC_POWS[trunc];
        decimal_digits[decimal_length - 1] = static_cast<uint32_t>(truncdv);
      }
    }
  }
  return ret;
}

// handle decoded digits without tail flag
int ObNumber::normalize_(uint32_t *digits, const int64_t length)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(digits)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_EDIAG("the pointer is null", K(ret));
  } else {
    int64_t tmp_length = length;
    int64_t start = 0;
    for (int64_t i = 0; i < tmp_length; ++i) {
      if (0 != digits[i]) {
        break;
      }
      ++start;
    }

    uint32_t carry = 0;
    for (int64_t i = tmp_length - 1; i >= start; --i) {
      digits[i] += carry;
      if (BASE <= digits[i]) {
        digits[i] -= (uint32_t)BASE;
        carry = 1;
      } else {
        carry = 0;
      }
    }
    // carry may generate extra zero, so reduce zero on tail, must after handle carry
    int64_t end = tmp_length - 1;
    for (int64_t i = tmp_length - 1; i >= start; --i) {
      if (0 != digits[i]
          && BASE != digits[i]) {
        break;
      }
      --end;
    }
    tmp_length = end - start + 1;
    tmp_length = (0 < tmp_length) ? tmp_length : 0;

    if (0 < carry) {
      if (OB_UNLIKELY(0 != tmp_length)) {
        LOG_WDIAG("unexpected length", K(tmp_length));
        ret = OB_ERR_UNEXPECTED;
      } else {
        digits_[0] = carry;
        len_ = 1;
        if (NEGATIVE == sign_) {
          --exp_;
        } else {
          ++exp_;
        }
      }
    } else {
      if (OB_UNLIKELY(cap_ < tmp_length)) {
        LOG_WDIAG("unexpected length cap", K(tmp_length), K(cap_));
        ret = OB_ERR_UNEXPECTED;
      } else {
        if (digits_ != &digits[start]) {
          memmove(digits_, &digits[start], tmp_length * ITEM_SIZE(digits_));
        }
        len_ = (uint8_t)tmp_length;
        if (0 == len_) {
          set_zero();
        }
      }
    }
  }

  return ret;
}

int ObNumber::rebuild_digits_(
    const uint32_t *integer_digits,
    const int64_t integer_length,
    const uint32_t *decimal_digits,
    const int64_t decimal_length)
{
  int ret = OB_SUCCESS;
  uint32_t digits[MAX_CALC_LEN];
  int64_t length = 0;
  memset(digits, 0, sizeof(digits));
  if (NULL != integer_digits
      && 0 < integer_length) {
    if (OB_UNLIKELY(MAX_CALC_LEN <= (length + integer_length))) {
      _OB_LOG(WDIAG, "buffer size overflow, cap=%ld length=%ld integer_length=%ld",
                MAX_CALC_LEN, length, integer_length);
      ret = OB_NUMERIC_OVERFLOW;
    } else {
      MEMCPY(digits, integer_digits, integer_length * ITEM_SIZE(digits_));
      length = integer_length;
    }
  }
  if (OB_SUCCESS == ret
      && NULL != decimal_digits
      && 0 < decimal_length) {
    if (OB_UNLIKELY(MAX_CALC_LEN <= (length + decimal_length))) {
      _OB_LOG(WDIAG, "buffer size overflow, cap=%ld length=%ld decimal_length=%ld",
                MAX_CALC_LEN, length, decimal_length);
      ret = OB_NUMERIC_OVERFLOW;
    } else {
      MEMCPY(&digits[length], decimal_digits, decimal_length * ITEM_SIZE(digits));
      length += decimal_length;
    }
  }
  if (OB_SUCC(ret)) {
    ret = normalize_(digits, length);
  }
  return ret;
}

const char *ObNumber::format() const
{
  static const int64_t BUFFER_NUM = 64;
  static const int64_t BUFFER_SIZE = MAX_PRINTABLE_SIZE;
  static __thread char buffers[BUFFER_NUM][BUFFER_SIZE];
  static __thread uint64_t i = 0;
  char *buffer = buffers[i++ % BUFFER_NUM];
  int64_t length = 0;
  if(OB_UNLIKELY(OB_SUCCESS != format(buffer, BUFFER_SIZE, length, -1))) {
    LOG_EDIAG("fail to format buffer");
  }
  return buffer;
}

int ObNumber::format(char *buf, const int64_t buf_len, int64_t &pos, int16_t scale) const
{
  int ret = OB_SUCCESS;
  uint32_t digits[MAX_STORE_LEN] = {0};
  bool prev_from_integer = true;
  int64_t pad_zero_count = scale;
  ObNumber buf_nmb;
  const ObNumber *nmb = this;
  if (scale >= 0) {
    if (OB_FAIL(buf_nmb.from_(*this, digits, MAX_STORE_LEN))) {
    } else if (OB_FAIL(buf_nmb.round(scale))) {
    } else {
      nmb = &buf_nmb;
    }
  }
  if (OB_SUCC(ret) && 0 == nmb->len_) {
    ret = databuff_printf(buf, buf_len, pos, "0");
  } else {
    if (OB_SUCC(ret)) {
      if (nmb->is_negative()) {
        ret = databuff_printf(buf, buf_len, pos, "-");
      }
      if (OB_SUCCESS == ret && nmb->is_decimal()) {
        ret = databuff_printf(buf, buf_len, pos, "0");
      }
      if (OB_SUCC(ret)) {
        ObDigitIterator di;
        di.assign(nmb->get_desc(), nmb->get_digits());
        uint32_t digit = 0;
        bool head_flag = true;
        bool from_integer = false;
        bool last_decimal = false;
        while (OB_SUCCESS == ret
               && OB_SUCCESS == di.get_next_digit(digit, from_integer, last_decimal)) {
          if (prev_from_integer && !from_integer) {
            // dot
            ret = databuff_printf(buf, buf_len, pos, ".");
          }
          if (OB_SUCC(ret)) {
            if (!from_integer && !last_decimal) {
              pad_zero_count -= DIGIT_LEN;
            }
            if (head_flag && from_integer) {
              // first integer
              ret = databuff_printf(buf, buf_len, pos, "%u", digit);
            } else if (last_decimal) {
              // last decimal
              int64_t counter = 0;
              uint32_t tmp = remove_back_zero(digit, counter);
              ret = databuff_printf(buf, buf_len, pos, ObNumber::BACK_DIGIT_FORMAT[counter], tmp);
              pad_zero_count -= (DIGIT_LEN - counter);
            } else {
              // normal digit
              ret = databuff_printf(buf, buf_len, pos, DIGIT_FORMAT, digit);
            }
          }

          if (OB_SUCC(ret)) {
            prev_from_integer = from_integer;
            head_flag = false;
          }
        }
      }
    }
  }
  // pad zero.
  if (OB_SUCCESS == ret && pad_zero_count > 0) {
    if (prev_from_integer) {
      ret = databuff_printf(buf, buf_len, pos, ".");
    }
    if (OB_SUCC(ret)) {
      // 60 '0' is enough, because ObNumber can contains 9 * 6 = 54 digits.
      if (OB_UNLIKELY(pad_zero_count > 60)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_EDIAG("the param is invalid", K(ret), K(pad_zero_count));
      } else {
        char zeros[] = "000000000000000000000000000000000000000000000000000000000000";
        zeros[pad_zero_count] = 0;
        ret = databuff_printf(buf, buf_len, pos, zeros);
      }
    }
  }
  return ret;
}

/* from_sci -- from scientific notation
 *
 * str -- IN, scientific notation string representation
 * length -- IN, the length of the input string
 * allocator -- IN, used to alloc ObNumber
 * precision -- OUT, precision of the converted result
 * scale -- OUT, scale of the converted result
 * This function converts a scientific notation string into the internal number format
 * It's more efficient to call from_ if the string is a normal numeric string */
int ObNumber::from_sci_(const char *str, const int64_t length, IAllocator &allocator, int &warning,
     int16_t *precision, int16_t *scale, const bool do_rounding)
{
  UNUSED(do_rounding);
  int ret = OB_SUCCESS;
  char full_str[MAX_PRINTABLE_SIZE] = {0};
  char digit_str[MAX_PRINTABLE_SIZE] = {0};
  bool is_neg = false;
  int32_t nth = 0;
  int32_t i_nth = 0;
  int32_t i = 0;
  int32_t e_value = 0;
  int32_t valid_len = 0;
  int32_t dec_n_zero = 0;
  bool e_neg = false;
  bool as_zero = false;
  bool has_digit = false;
  char tmpstr[MAX_PRINTABLE_SIZE] = {0};
  char cur = '\0';
  if (OB_UNLIKELY(NULL == str || length <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WDIAG, "invalid param", K(length), KP(str), K(ret));
  } else {
    /* parse str like 1.2e3
     * part 1:allow str like 000123=> 123(valid length is 3); if valid length >126, ignore the rest
     * part 2:if '.' exists, part 2 must not be empty: str like 1.e3 is illegal
     * part 3:part 3's value + length of part1 <= 126
     * */
    for (i = 0; i < length && isspace(str[i]); ++i);
    if ('-' == str[i]) {
      is_neg = true;
      full_str[nth++] = '-';
      i++;
    } else if ('+' == str[i]) {
      i++;
    }
    /* 000123 -> 123 */
    while ('0' == str[i] && i + 1 < length) {
      i++;
      has_digit = true;
    }
    cur = str[i];
    while(i + 1 < length && cur <= '9' && cur >= '0') {
      if (i_nth < MAX_PRECISION) {
        digit_str[i_nth++] = cur;
        cur = str[++i];
        has_digit = true;
      } else {
        /* ignore the rest */
        i++;
      }
      valid_len++;
    }
  }

  if (OB_SUCC(ret) && cur == '.' && valid_len < MAX_PRECISION && i + 1 < length) {
    cur = str[++i];
    if (0 == valid_len) {
      /* 0.000123 -> dec_n_zero = 3 */
      while ('0' == cur && i + 1 < length) {
        valid_len--;
        dec_n_zero++;
        cur = str[++i];
        has_digit = true;
      }
    }
    /* 1.23  0.0123 0.123 -> digit_str:'123' */
    while(i + 1 < length && cur <= '9' && cur >= '0') {
      if (i_nth < MAX_PRECISION) {
        digit_str[i_nth++] = cur;
      } else {
        /* ignore the rest */
      }
      cur = str[++i];
      if (0 >= valid_len) {
        valid_len--;
      }
    }
  }

  if (OB_SUCC(ret) && (has_digit || 0 < i_nth) 
                   && ('e' == cur || 'E' == cur) 
                   && is_valid_sci_tail_(str, length, i)) {
    LOG_DEBUG("ObNumber from sci", K(ret), K(i), K(cur), K(is_neg), K(nth), K(digit_str), K(i_nth),
      K(valid_len), K(dec_n_zero));
    if (0 == i || i >= length - 1) {
      if (i_nth > 0) {
        memcpy(full_str + nth, digit_str, i_nth);
        nth += i_nth;
      }
      warning = OB_INVALID_NUMERIC;
    } else if (0 == valid_len || 0 == i_nth) {
      // `i_nth = 0` means all digits are zero.
      /* ignore e's value; only do the check*/
      cur = str[++i];
      if ('-' == cur || '+' == cur) {
        if (i < length - 1) {
          cur = str[++i];
        }
      }
      if (cur <= '9' && cur >= '0') {
        while (cur <= '9' && cur >= '0' && (++i < length)) {
          cur = str[i];
        }
      } else {
        /* 0e */
        ret = OB_INVALID_NUMERIC;
        LOG_WDIAG("Number from sci invalid exponent", K(ret), K(cur), K(i));
      }
    } else {
      cur = str[++i];
      switch (cur) {
      case '-':
        e_neg = true;
        /* fall through */
      case '+':
        if (i < length - 1) {
          cur = str[++i];
        }
        break;
      }
      /* Oracle max valid length of string is 255(exponent of the value is [-253, 255])
       * exponent of number's legal range is [-130, 125]
       * so e_value's valid range can't larger than 3 digits */
      int e_cnt = 0;
      while (i < length && cur <= '9' && cur >= '0') {
        if (e_cnt < 4) {
          e_value = e_neg ? (e_value * 10 - (cur - '0')) : (e_value * 10 + cur - '0');
        }
        e_cnt++;
        if (++i >= length) {
          break;
        }
        cur = str[i];
      }

      LOG_DEBUG("ObNumber from sci E", K(warning), K(e_neg), K(e_cnt), K(e_value), K(valid_len), K(i));
      if (0 == e_cnt) {
        warning = OB_INVALID_NUMERIC;
        e_value = 0;
      }
      if ((valid_len >= 0 && (e_value + valid_len <= MIN_SCI_SIZE))
          ||(valid_len < 0 && (e_value - dec_n_zero <= MIN_SCI_SIZE))) {
        as_zero = true;
      } else if (e_value + valid_len > MAX_SCI_SIZE) {
        ret = OB_NUMERIC_OVERFLOW;
      } else if (valid_len <= 0) {
        /* 0.01234e-5 */
        if (e_value < 0) {
          nth += snprintf(full_str + nth, MAX_PRINTABLE_SIZE - nth, "0.%0*d%s", 0 - e_value + dec_n_zero, 0, digit_str);
          LOG_DEBUG("ObNumber sci", K(tmpstr), K(nth), K(full_str), K(e_value), K(dec_n_zero), K(digit_str));
        } else {
          if (dec_n_zero - e_value > 0) {
            /* 0.00012e2 -> 0.012 */
            nth += snprintf(full_str + nth, MAX_PRINTABLE_SIZE - nth, "0.%0*d%s", dec_n_zero - e_value, 0, digit_str);
            LOG_DEBUG("ObNumber sci", K(tmpstr), K(e_value), K(dec_n_zero));
          } else if (e_value < dec_n_zero + i_nth) {
            /* 0.001234e4 -> 12.34
             * e_value - dec_n_zero = 4 - 2 = 2
             * fmt str: %2.s%s, digit_str, digit_str + 2*/
            nth += snprintf(full_str + nth, MAX_PRINTABLE_SIZE - nth, "%.*s.%s", e_value - dec_n_zero, digit_str, digit_str + e_value - dec_n_zero);
            LOG_DEBUG("ObNumber sci", K(tmpstr), K(full_str), K(nth), K(e_value), K(dec_n_zero), K(digit_str));
          } else {
            /* 0.001234e8 -> 123400
             * e_value - dec_n_zero - i_nth = 8 - 2 - 4 = 2 */
            if (e_value - dec_n_zero - i_nth > 0) {
              snprintf(tmpstr, MAX_PRINTABLE_SIZE, "%0*d", e_value - dec_n_zero - i_nth, 0);
            }
            nth += snprintf(full_str + nth, MAX_PRINTABLE_SIZE - nth, "%s%s", digit_str, tmpstr);
            LOG_DEBUG("ObNumber sci", K(tmpstr), K(digit_str), K(full_str), K(nth), K(e_value), K(i_nth), K(dec_n_zero));
          }
        }
      } else {
        if (e_value >= 0) {
          /* 12.34e5 -> 1234000
           * e_value - (i_nth - valid_len) = 5 - (4 - 2) = 0*/
          if (e_value - (i_nth - valid_len) > 0) {
            nth += snprintf(full_str + nth, MAX_PRINTABLE_SIZE - nth, "%s%0*d",
                digit_str, e_value - (i_nth - valid_len), 0);
            LOG_DEBUG("ObNumber sci", K(tmpstr), K(full_str), K(nth), K(e_value), K(i_nth), K(valid_len));
          } else if (e_value - (i_nth - valid_len) == 0) {
            nth += snprintf(full_str + nth, MAX_PRINTABLE_SIZE - nth, "%s", digit_str);
            LOG_DEBUG("ObNumber sci", K(full_str), K(nth), K(e_value), K(i_nth), K(valid_len));
          } else {
            /* 12.345e2 -> 1234.5
             * valid_len + e_value = 2 + 2 = 4
             * fmt_str: %4.s, digit_str, digit_str + 4*/
            nth += snprintf(full_str + nth, MAX_PRINTABLE_SIZE - nth, "%.*s.%s",
                valid_len + e_value, digit_str, digit_str + valid_len + e_value);
            LOG_DEBUG("ObNumber sci", K(valid_len + e_value), K(full_str), K(nth), K(e_value), K(digit_str), K(valid_len));
          }
        } else {
          if (valid_len + e_value > 0)
          {
            /* 12.34e-1 -> 1.234
             * valid_len + e_value = 2 - 1 = 1
             * fmt_str: %1.s, digit_str, digit_str + 1*/
            //sprintf(tmpstr, "%%%d.s.%%s", valid_len + e_value);
            nth += snprintf(full_str + nth, MAX_PRINTABLE_SIZE - nth, "%.*s.%s",
                valid_len + e_value, digit_str, digit_str + valid_len + e_value);
            LOG_DEBUG("ObNumber sci", K(valid_len + e_value), K(full_str), K(nth), K(e_value), K(digit_str), K(valid_len));
          } else {
            /* 12.34e-4 -> 0.001234
             * 0 - (valid_len + e_value) = 0 - (2 - 4) = 2 */
            if (valid_len + e_value < 0) {
              snprintf(tmpstr, MAX_PRINTABLE_SIZE, "%0*d", 0 - (valid_len + e_value), 0);
            }
            nth += snprintf(full_str + nth, MAX_PRINTABLE_SIZE - nth, "0.%s%s", tmpstr, digit_str);
            LOG_DEBUG("ObNumber sci", K(tmpstr), K(full_str), K(nth), K(e_value), K(digit_str), K(valid_len));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      LOG_DEBUG("Number from sci last ", K(cur), K(i), "str", ObString(length, str),
               K(length), K(valid_len), K(ret), K(warning));
      while (cur == ' ' && i < length - 1) {
        cur = str[++i];
      }
      if (cur != ' ' && i <= length - 1) {
        warning = OB_INVALID_NUMERIC;
        LOG_WDIAG("invalid numeric string", K(ret), K(i), K(length), K(cur), "str", ObString(length, str));
      }
      if ((OB_SUCCESS != warning) && OB_SUCC(ret)) {
        ret = warning;
      } else if (OB_FAIL(ret)) {
        as_zero = true;
//        warning = OB_ERR_DOUBLE_TRUNCATED;
        warning = ret;
        ret = OB_SUCCESS;
      }
      if (OB_SUCC(ret)) {
        LOG_DEBUG("ObNumber sci final", K(ret), K(warning), K(full_str), K(nth), K(as_zero), K(e_neg), K(e_value), K(valid_len), K(i), K(i_nth));
        if (as_zero || 0 == valid_len || 0 == i_nth) {
          full_str[0] = '0';
          nth = 1;
          set_zero();
        } else {
          int tmp_warning = OB_SUCCESS;
          // ret = from_(full_str, nth, allocator, tmp_warning, NULL, precision, scale, NULL, do_rounding);
          ret = from_(full_str, nth, allocator, tmp_warning, precision, scale);
          if (OB_SUCC(ret) && OB_SUCCESS != warning) {
            warning = tmp_warning;
          }
        }
      }
    }
  } else {
    // ret = from_(str, length, allocator, warning, NULL, precision, scale, NULL, do_rounding);
    ret = from_(full_str, nth, allocator, warning, precision, scale);
  }
  return ret;
}

int ObNumber::add_(const ObNumber &other, ObNumber &value, IAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObNumber res;
  Desc augend_desc;
  Desc addend_desc;
  augend_desc.desc_ = desc_;
  addend_desc.desc_ = other.desc_;
  if (is_zero()) {
    ret = res.from_(other, allocator);
  } else if (other.is_zero()) {
    ret = res.from_(*this, allocator);
  } else if (augend_desc.sign_ == addend_desc.sign_) {
    ObCalcVector augend;
    ObCalcVector addend;
    int64_t shift = exp_integer_align_(augend_desc, addend_desc);
    exp_shift_(shift, augend_desc);
    exp_shift_(shift, addend_desc);
    if (OB_FAIL(augend.init(augend_desc.desc_, digits_))) {
      LOG_WDIAG("fail to assign values", K(ret));
    } else if (OB_FAIL(addend.init(addend_desc.desc_, other.digits_))) {
      LOG_WDIAG("fail to assign values", K(ret));
    } else {
      ObCalcVector sum;
      int64_t sum_size = std::max(augend.size(), addend.size()) + 1;
      if (OB_FAIL(sum.ensure(sum_size))) {
        LOG_WDIAG("Fail to ensure sum_size", K(ret));
      } else if (OB_FAIL(poly_poly_add(augend, addend, sum))) {
        _OB_LOG(WDIAG, "[%s] add [%s] fail ret=%d", to_cstring(*this), to_cstring(other), ret);
      } else {
        Desc res_desc = exp_max_(augend_desc, addend_desc);
        bool carried = (0 != sum.at(0));
        exp_shift_(-shift + (carried ? 1 : 0), res_desc);
        ret = res.from_(res_desc.desc_, sum, allocator);
      }
    }
  } else {
    ObNumber subtrahend;
    StackAllocator stack_allocator;
    if (OB_FAIL(other.negate(subtrahend, stack_allocator))) {
      _OB_LOG(WDIAG, "nagate [%s] fail, ret=%d", to_cstring(other), ret);
    } else {
      ret = sub_(subtrahend, res, allocator);
    }
  }
  if (OB_SUCC(ret)) {
    value = res;
  }
  _OB_LOG(DEBUG, "[%s] + [%s], ret=%d [%s]", this->format(), other.format(), ret, res.format());
  return ret;
}

int ObNumber::sub_(const ObNumber &other, ObNumber &value, IAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObNumber res;
  Desc minuend_desc;
  Desc subtrahend_desc;
  minuend_desc.desc_ = desc_;
  subtrahend_desc.desc_ = other.desc_;
  if (is_zero()) {
    ret = other.negate_(res, allocator);
  } else if (other.is_zero()) {
    ret = res.from_(*this, allocator);
  } else if (minuend_desc.sign_ == subtrahend_desc.sign_) {
    ObCalcVector minuend;
    ObCalcVector subtrahend;
    int64_t shift = exp_integer_align_(minuend_desc, subtrahend_desc);
    exp_shift_(shift, minuend_desc);
    exp_shift_(shift, subtrahend_desc);
    if (OB_FAIL(minuend.init(minuend_desc.desc_, digits_))) {
      LOG_WDIAG("fail to assign values", K(ret));
    } else if (OB_FAIL(subtrahend.init(subtrahend_desc.desc_, other.digits_))){
      LOG_WDIAG("fail to assign values", K(ret));
    } else {
      ObCalcVector remainder;
      int64_t remainder_size = std::max(minuend.size(), subtrahend.size());

      bool sub_negative = false;
      if (OB_FAIL(remainder.ensure(remainder_size))) {
        LOG_WDIAG("remainder.ensure(remainder_size) fails", K(ret));
      } else if (OB_FAIL(poly_poly_sub(minuend, subtrahend, remainder, sub_negative))) {
        _OB_LOG(WDIAG, "[%s] sub [%s] fail ret=%d", to_cstring(*this), to_cstring(other), ret);
      } else {
        Desc res_desc = exp_max_(minuend_desc, subtrahend_desc);
        for (int64_t i = 0; i < remainder.size() - 1; ++i) {
          if (0 != remainder.at(i)) {
            break;
          }
          ++shift;
        }
        exp_shift_(-shift, res_desc);

        bool arg_negative = (NEGATIVE == minuend_desc.sign_);
        if (sub_negative == arg_negative) {
          res_desc.sign_ = POSITIVE;
        } else {
          res_desc.sign_ = NEGATIVE;
        }

        if (res_desc.sign_ != minuend_desc.sign_) {
          res_desc.exp_ = (0x7f & ~res_desc.exp_);
          ++res_desc.exp_;
        }

        ret = res.from_(res_desc.desc_, remainder, allocator);
      }
    }

  } else {
    ObNumber addend;
    StackAllocator stack_allocator;
    if (OB_FAIL(other.negate(addend, stack_allocator))) {
      _OB_LOG(WDIAG, "nagate [%s] fail, ret=%d", to_cstring(other), ret);
    } else {
      ret = add_(addend, res, allocator);
    }
  }
  if (OB_SUCC(ret)) {
    value = res;
  }
  _OB_LOG(DEBUG, "[%s] - [%s], ret=%d [%s]", this->format(), other.format(), ret, res.format());
  return ret;
}

int ObNumber::negate_(ObNumber &value, IAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObNumber res;
  if (is_zero()) {
    res.set_zero();
  } else {
    ObCalcVector cv;
    if (OB_FAIL(cv.init(desc_, digits_))) {
      LOG_WDIAG("fail to assign values", K(ret));
    } else {
      int64_t size2alloc = len_;
      if (POSITIVE == sign_) {
        res.sign_ = NEGATIVE;
      } else {
        res.sign_ = POSITIVE;
      }
      res.exp_ = (0x7f & ~exp_);
      ++res.exp_;

      if (OB_ISNULL(res.digits_ = res.alloc_(allocator, size2alloc))) {
        _OB_LOG(WDIAG, "alloc digits fail, length=%ld", size2alloc);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(res.normalize_(cv.get_digits(), cv.size()))) {
        _OB_LOG(WDIAG, "normalize [%s] fail ret=%d", to_cstring(res), ret);
      } else {
        // do nothing
      }
    }
  }
  if (OB_SUCC(ret)) {
    value = res;
  }
  _OB_LOG(DEBUG, "negate [%s], ret=%d [%s]", this->format(), ret, res.format());
  return ret;
}

int ObNumber::mul_(const ObNumber &other, ObNumber &value, IAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObNumber res;
  Desc multiplicand_desc;
  Desc multiplier_desc;
  multiplicand_desc.desc_ = desc_;
  multiplier_desc.desc_ = other.desc_;
  if (is_zero() || other.is_zero()) {
    res.set_zero();
  } else {
    ObCalcVector multiplicand;
    ObCalcVector multiplier;
    if (OB_FAIL(multiplicand.init(multiplicand_desc.desc_, digits_))) {
      LOG_WDIAG("fail to assign values", K(ret));
    } else if (OB_FAIL(multiplier.init(multiplier_desc.desc_, other.digits_))) {
      LOG_WDIAG("fail to assign values", K(ret));
    } else {
      ObCalcVector product;
      int64_t product_size = multiplicand.size() + multiplier.size();
      if (OB_FAIL(product.ensure(product_size))) {
        LOG_WDIAG("product.ensure(product_size) fails", K(ret));
      } else if (OB_FAIL(poly_poly_mul(multiplicand, multiplier, product))) {
        _OB_LOG(WDIAG, "[%s] mul [%s] fail, ret=%d", to_cstring(*this), to_cstring(other), ret);
      } else {
        Desc res_desc = exp_mul_(multiplicand_desc, multiplier_desc);
        bool carried = (0 != product.at(0));
        exp_shift_((carried ? 1 : 0), res_desc);
        ret = res.from_(res_desc.desc_, product, allocator);
      }
    }
  }
  if (OB_SUCC(ret)) {
    value = res;
  }
  _OB_LOG(DEBUG, "[%s] * [%s], ret=%d [%s]", this->format(), other.format(), ret, res.format());
  return ret;
}

int ObNumber::div_(const ObNumber &other, ObNumber &value, IAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObNumber res;
  Desc dividend_desc;
  Desc divisor_desc;
  dividend_desc.desc_ = desc_;
  divisor_desc.desc_ = other.desc_;
  if (OB_UNLIKELY(other.is_zero())) {
    _OB_LOG(EDIAG, "[%s] div zero [%s]", to_cstring(*this), to_cstring(other));
    ret = OB_DIVISION_BY_ZERO;
  } else if (is_zero()) {
    res.set_zero();
  } else {
    ObCalcVector dividend;
    ObCalcVector divisor;
    int64_t shift = get_extend_length_remainder_(dividend_desc, divisor_desc);
    shift = (MAX_STORE_LEN <= shift) ? shift : (MAX_STORE_LEN - shift);
    shift += get_decimal_extend_length_(dividend_desc);
    exp_shift_(shift, dividend_desc);
    if (OB_FAIL(dividend.init(dividend_desc.desc_, digits_))) {
      LOG_WDIAG("fail to assign values", K(ret));
    } else if (OB_FAIL(divisor.init(divisor_desc.desc_, other.digits_))) {
      LOG_WDIAG("fail to assign values", K(ret));
    } else {
      ObCalcVector quotient;
      ObCalcVector remainder;
      int64_t quotient_size = dividend.size() - divisor.size() + 1;
      int64_t remainder_size = divisor.size();
      if (OB_FAIL(quotient.ensure(quotient_size))) {
        LOG_WDIAG("quotient.ensure(quotient_size) fails", K(ret));
      } else if (OB_FAIL(remainder.ensure(remainder_size))) {
        LOG_WDIAG("remainder.ensure(remainder_size) fails", K(ret));
      } else if (OB_FAIL(poly_poly_div(dividend, divisor, quotient, remainder))) {
        _OB_LOG(WDIAG, "[%s] div [%s] fail ret=%d", to_cstring(*this), to_cstring(other), ret);
      } else {
        Desc res_desc = exp_div_(dividend_desc, divisor_desc);
        for (int64_t i = 0; i < quotient.size(); ++i) {
          if (0 != quotient.at(i)) {
            break;
          }
          ++shift;
        }
        exp_shift_(-shift, res_desc);
        ret = res.from_(res_desc.desc_, quotient, allocator);
      }
    }
  }
  if (OB_SUCC(ret)) {
    value = res;
  }
  _OB_LOG(DEBUG, "[%s] / [%s], ret=%d [%s]", this->format(), other.format(), ret, res.format());
  return ret;
}

int ObNumber::rem_(const ObNumber &other, ObNumber &value, IAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObNumber res;
  Desc dividend_desc;
  Desc divisor_desc;
  dividend_desc.desc_ = desc_;
  divisor_desc.desc_ = other.desc_;
  int cmp_ret = 0;
  if (OB_UNLIKELY(other.is_zero())) {
    _OB_LOG(EDIAG, "[%s] div zero [%s]", to_cstring(*this), to_cstring(other));
    ret = OB_DIVISION_BY_ZERO;
  } else if (is_zero()) {
    res.set_zero();
  } else if (0 >= (cmp_ret = abs_compare(other))) {
    if (0 == cmp_ret) {
      res.set_zero();
    } else {
      res.from(*this, allocator);
    }
  } else {
    int64_t shift = std::max(get_decimal_extend_length_(dividend_desc),
                             get_decimal_extend_length_(divisor_desc));
    exp_shift_(shift, dividend_desc);
    exp_shift_(shift, divisor_desc);

    ObCalcVector dividend;
    ObCalcVector divisor;
    if (OB_FAIL(dividend.init(dividend_desc.desc_, digits_))) {
      LOG_WDIAG("fail to assign values", K(ret));
    } else if (OB_FAIL(divisor.init(divisor_desc.desc_, other.digits_))){
      LOG_WDIAG("fail to assign values", K(ret));
    } else {
      ObCalcVector dividend_amplify;
      ObCalcVector *dividend_ptr = NULL;
      if (dividend.size() < divisor.size()) {
        _OB_LOG(WDIAG, "dividend_size=%ld must not less than divisor_size=%ld", dividend.size(),
                divisor.size());
        ret = OB_ERR_UNEXPECTED;
      } else if (dividend.size() > divisor.size()) {
        dividend_ptr = &dividend;
      } else {
        ObCalcVector divisor_amplify;
        if (OB_FAIL(divisor_amplify.ensure(divisor.size() + 1))) {
          LOG_WDIAG("divisor_amplify.ensure() fails", K(ret));
        } else if (OB_FAIL(poly_mono_mul(divisor, BASE - 1, divisor_amplify))) {
          _OB_LOG(WDIAG, "[%s] mul [%lu] fail, ret=%d", to_cstring(divisor), BASE - 1, ret);
        } else {
          int64_t sum_size = std::max(dividend.size(), divisor_amplify.size()) + 1;
          if (OB_FAIL(dividend_amplify.ensure(sum_size))) {
            LOG_WDIAG("ensure() fails", K(ret));
          } else if (OB_FAIL(poly_poly_add(dividend, divisor_amplify, dividend_amplify))) {
            _OB_LOG(WDIAG, "[%s] add [%s] fail, ret=%d",
                    to_cstring(dividend), to_cstring(divisor_amplify), ret);
          } else {
            dividend_ptr = &dividend_amplify;
          }
        }
      }
      if (OB_SUCC(ret)) {
        ObCalcVector quotient;
        ObCalcVector remainder;
        int64_t quotient_size = dividend_ptr->size() - divisor.size() + 1;
        int64_t remainder_size = divisor.size();
        if (OB_FAIL(quotient.ensure(quotient_size))) {
          LOG_WDIAG("ensure() fails", K(ret));
        } else if (OB_FAIL(remainder.ensure(remainder_size))) {
          LOG_WDIAG("ensure() fails", K(ret));
        } else if (OB_FAIL(poly_poly_div(*dividend_ptr, divisor, quotient, remainder))) {
          _OB_LOG(WDIAG, "[%s] div [%s] fail ret=%d",
                  to_cstring(*dividend_ptr), to_cstring(divisor), ret);
        } else {
          Desc res_desc = exp_rem_(dividend_desc, divisor_desc);
          for (int64_t i = 0; i < remainder.size() - 1; ++i) {
            if (0 != remainder.at(i)) {
              break;
            }
            ++shift;
          }
          exp_shift_(-shift, res_desc);
          ret = res.from_(res_desc.desc_, remainder, allocator);
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    value = res;
  }
  _OB_LOG(DEBUG, "[%s] %% [%s], ret=%d [%s]", this->format(), other.format(), ret, res.format());
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

ObIntegerBuilder::ObIntegerBuilder() : exp_(0),
                                       digit_pos_(ObNumber::MAX_CALC_LEN - 1),
                                       digit_idx_(0)
{
}

ObIntegerBuilder::~ObIntegerBuilder()
{
}

int ObIntegerBuilder::build(const char *str, const int64_t integer_start,
                            const int64_t integer_end, const bool reduce_zero)
{
  int ret = OB_SUCCESS;
  reset();
  int64_t skiped_zero_counter = 0;
  if (OB_UNLIKELY(integer_start <= integer_end && NULL == str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_EDIAG("the pointer is null");
  } else if (integer_start >= 0 && integer_end >= 0) {
    bool is_exist_non_zero = false;
    for (int64_t i = integer_end; i >= integer_start; --i) {
      char c = str[i];
      if ('0' == c) {
        if (is_exist_non_zero) {
          ++skiped_zero_counter;
          continue;
        }
      } else {
        for (int64_t j = 0; j < skiped_zero_counter; ++j) {
          if (OB_FAIL(push(0, reduce_zero))) {
            LOG_WDIAG("push to integer builder fail", K(ret), K(j));
            break;
          }
        }
        if (OB_FAIL(ret)) {
          break;
        }
        is_exist_non_zero = true;
        skiped_zero_counter = 0;
      }
      if (OB_FAIL(push((uint8_t)(c - '0'), reduce_zero))) {
        LOG_WDIAG("push to integer builder fail", K(ret), K(c));
        break;
      }
    }
  } else { /* Do nothing */ }
  return ret;
}

int ObIntegerBuilder::push(const uint8_t d, const bool reduce_zero)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(digits_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_EDIAG("the pointer is null");
  } else if (OB_UNLIKELY(get_length() > ObNumber::MAX_CALC_LEN)) {
    ret = OB_NUMERIC_OVERFLOW;
  } else {
    if (0 == digit_idx_) {
      // Init current digit on first use
      digits_[digit_pos_] = 0;
    }

    // 1234
    // push [4]: 4 = 4
    // push [3]: 3*10 + 4= 34
    // push [2]: 2*100 + 34 = 234
    // push [1]: 1*1000 + 234 = 1234
    static const uint32_t POWS[ObNumber::DIGIT_LEN] = {1, 10, 100, 1000, 10000, 100000,
                                                       1000000, 10000000, 100000000};
    digits_[digit_pos_] += d * POWS[digit_idx_];
    ++digit_idx_;

    if (ObNumber::DIGIT_LEN <= digit_idx_) {
      if (!reduce_zero
          || 0 != digits_[ObNumber::MAX_CALC_LEN - 1]) {
        digit_pos_ -= 1;
      }
      digit_idx_ = 0;
      ++exp_;
    }
  }
  return ret;
}

int64_t ObIntegerBuilder::get_exp() const
{
  return (0 == digit_idx_) ? (exp_ - 1) : exp_;
}

int64_t ObIntegerBuilder::get_length() const
{
  return (0 == digit_idx_) ? (ObNumber::MAX_CALC_LEN - digit_pos_ - 1) :
         (ObNumber::MAX_CALC_LEN - digit_pos_);
}

const uint32_t *ObIntegerBuilder::get_digits() const
{
  return (0 == get_length()) ? NULL : &digits_[ObNumber::MAX_CALC_LEN - get_length()];
}

void ObIntegerBuilder::reset()
{
  exp_ = 0;
  digit_pos_ = ObNumber::MAX_CALC_LEN - 1;
  digit_idx_ = 0;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

ObDecimalBuilder::ObDecimalBuilder() : exp_(-1),
                                       digit_pos_(0),
                                       digit_idx_(0)
{
}

ObDecimalBuilder::~ObDecimalBuilder()
{
}

int ObDecimalBuilder::push(const uint8_t d, const bool reduce_zero)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(get_length() >= ObNumber::MAX_CALC_LEN)) {
    ret = OB_NUMERIC_OVERFLOW;
  } else {
    if (0 == digit_idx_) {
      // Init current digit on first use
      digits_[digit_pos_] = 0;
    }

    // 0.1234
    // push [1]: 0 + 1*100000000 = 100000000
    // push [2]: 100000000 + 2*10000000 = 120000000
    // push [3]: 120000000 + 3*1000000 = 123000000
    // push [4]: 123000000 + 4*100000 = 123400000
    static const uint32_t POWS[ObNumber::DIGIT_LEN] = {100000000, 10000000, 1000000,
                                                       100000, 10000, 1000, 100, 10, 1};
    digits_[digit_pos_] += d * POWS[digit_idx_];
    ++digit_idx_;

    if (ObNumber::DIGIT_LEN <= digit_idx_) {
      if (!reduce_zero
          || 0 != digits_[0]) {
        digit_pos_ += 1;
      } else {
        --exp_;
      }
      digit_idx_ = 0;
    }
  }
  return ret;
}

int64_t ObDecimalBuilder::get_exp() const
{
  return exp_;
}

int64_t ObDecimalBuilder::get_length() const
{
  return (0 == digit_idx_) ? digit_pos_ : (digit_pos_ + 1);
}

const uint32_t *ObDecimalBuilder::get_digits() const
{
  return (0 == get_length()) ? NULL : &digits_[0];
}

void ObDecimalBuilder::reset()
{
  exp_ = -1;
  digit_pos_ = 0;
  digit_idx_ = 0;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

ObNumberBuilder::ObNumberBuilder()
{
}

ObNumberBuilder::~ObNumberBuilder()
{
}

void ObNumberBuilder::reset()
{
  len_ = 0;
  sign_ = POSITIVE;
  exp_ = EXP_ZERO;
}

int ObNumberBuilder::build(const char *str,
                           int64_t length,
                           int &warning,
                           int16_t *precision,
                           int16_t *scale)
{
  int ret = OB_SUCCESS;
  reset();
  bool negative = false;
  int64_t integer_start = -1;
  int64_t integer_end = -1;
  int64_t decimal_start = -1;
  bool integer_zero = false;
  bool decimal_zero = false;
  if (OB_ISNULL(digits_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("digits_ should not be null when this func is invoked", K(ret));
  } else if (OB_FAIL(find_point_(str, length, integer_start, integer_end, decimal_start,
                                       negative, integer_zero, decimal_zero, warning))) {
    _OB_LOG(WDIAG, "lookup fail ret=%d str=[%.*s]", ret, (int)length, str);
  } else if (OB_FAIL(build_integer_(str, integer_start, integer_end, decimal_zero))) {
    _OB_LOG(WDIAG, "build integer fail, ret=%d str=[%.*s]", ret, (int)length, str);
  } else if (OB_FAIL(build_decimal_(str, length, decimal_start, decimal_zero))) {
    _OB_LOG(WDIAG, "build decimal fail, ret=%d str=[%.*s]", ret, (int)length, str);
  } else {
    if (!negative) {
      sign_ = POSITIVE;
    } else {
      sign_ = NEGATIVE;
    }

    if (0 != ib_.get_length()) {
      exp_ = 0x7f & (uint8_t)(EXP_ZERO + ib_.get_exp());
    } else {
      exp_ = 0x7f & (uint8_t)(EXP_ZERO + db_.get_exp());
    }

    len_ = (uint8_t)(ib_.get_length() + db_.get_length());
    if (negative) {
      exp_ = (0x7f & ~exp_);
      ++exp_;
    }

    if (NULL != digits_
        && 0 < ib_.get_length()) {
      MEMCPY(digits_, ib_.get_digits(), ib_.get_length() * ITEM_SIZE(digits_));
    }
    if (NULL != digits_
        && 0 < db_.get_length()) {
      MEMCPY(&digits_[ib_.get_length()], db_.get_digits(), db_.get_length() * ITEM_SIZE(digits_));
    }

    if (NULL != precision && NULL != scale) {
      *scale = static_cast<int16_t>((length - decimal_start == -1) ? 0 : (length - decimal_start));
      if (*scale > ObNumber::FLOATING_SCALE) {
        *scale = ObNumber::FLOATING_SCALE;
      }
      *precision = static_cast<int16_t>(integer_end - integer_start + 1 + *scale);
    }
    ret = normalize_(digits_, len_);
  }
  return ret;
}

int ObNumberBuilder::build_integer_(const char *str, const int64_t integer_start,
                                    const int64_t integer_end, const bool reduce_zero)
{
  int ret = OB_SUCCESS;
  ib_.reset();
  int64_t skiped_zero_counter = 0;
  if (OB_UNLIKELY(integer_start <= integer_end && NULL == str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_EDIAG("the pointer is null");
  } else if (integer_start >= 0 && integer_end >= 0) {
    for (int64_t i = integer_end; i >= integer_start; --i) {
      char c = str[i];
      if ('0' == c) {
        ++skiped_zero_counter;
        continue;
      } else {
        for (int64_t j = 0; j < skiped_zero_counter; ++j) {
          if (OB_FAIL(ib_.push(0, reduce_zero))) {
            LOG_WDIAG("push to integer builder fail", K(ret), K(j));
            break;
          }
        }
        if (OB_FAIL(ret)) {
          break;
        }
        skiped_zero_counter = 0;
      }
      if (OB_FAIL(ib_.push((uint8_t)(c - '0'), reduce_zero))) {
        LOG_WDIAG("push to integer builder fail", K(ret), K(c));
        break;
      }
    }
  } else { /* Do nothing */ }
  return ret;
}

int ObNumberBuilder::build_decimal_(const char *str, const int64_t length,
                                    const int64_t decimal_start, const bool reduce_zero)
{
  int ret = OB_SUCCESS;
  db_.reset();
  int64_t skiped_zero_counter = 0;
  if (decimal_start < length && OB_ISNULL(str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_EDIAG("the pointer is null");
  } else {
    for (int64_t i = decimal_start; i < length; ++i) {
      char c = str[i];
      if ('0' == c) {
        ++skiped_zero_counter;
        continue;
      } else {
        for (int64_t j = 0; j < skiped_zero_counter; ++j) {
          if (OB_FAIL(db_.push(0, reduce_zero))) {
            LOG_WDIAG("push to decimal builder fail", K(ret), K(j));
            break;
          }
        }
        if (OB_FAIL(ret)) {
          break;
        }
        skiped_zero_counter = 0;
      }
      if (OB_FAIL(db_.push((uint8_t)(c - '0'), reduce_zero))) {
        LOG_WDIAG("push to decimal builder fail", K(ret), K(c));
        break;
      }
    }
  }

  return ret;
}

int ObNumberBuilder::find_point_(
    const char *str,
    int64_t &length,
    int64_t &integer_start,
    int64_t &integer_end,
    int64_t &decimal_start,
    bool &negative,
    bool &integer_zero,
    bool &decimal_zero,
    int &warning)
{
  int ret = OB_SUCCESS;
  int64_t i_integer_start = -2;
  int64_t dot_idx = -1;
  bool b_negative = false;
  bool b_integer_zero = true;
  bool b_decimal_zero = true;
  bool sign_appeared = false;
  bool digit_appeared = false;
  bool dot_appeared = false;
  int64_t i = 0;
  if (OB_ISNULL(str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_EDIAG("the str pointer is null", K(ret));
  } else {
    for (i = 0; i < length && isspace(str[i]); ++i);
    if (OB_UNLIKELY(i == length)) {
      ret = OB_INVALID_NUMERIC;
    } else {
      for (; OB_SUCCESS == warning && i < length; ++i) {
        char c = str[i];
        switch (c) {
          case '-':
          case '+':
            if (sign_appeared || digit_appeared || dot_appeared) {
              warning = OB_INVALID_NUMERIC;
            } else {
              if ('-' == c) {
                b_negative = true;
              }
              sign_appeared = true;
            }
            break;
          case '1':
          case '2':
          case '3':
          case '4':
          case '5':
          case '6':
          case '7':
          case '8':
          case '9':
            if (!dot_appeared) {
              b_integer_zero = false;
            } else {
              b_decimal_zero = false;
            }
            __attribute__ ((fallthrough));
            /* no break. */
          case '0':
            if (-2 == i_integer_start) {
              i_integer_start = i;
            }
            digit_appeared = true;
            break;
          case '.':
            if (dot_appeared) {
              warning = OB_INVALID_NUMERIC;
            } else {
              if (!digit_appeared) {
                //".95" means "0.95"
                // i_integer_start and i_integer_end both will be -1
                i_integer_start = -1;
              }
              dot_appeared = true;
              dot_idx = i;
            }
            break;
          default:
            warning = OB_INVALID_NUMERIC;
            break;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_SUCCESS != warning) {
        length = i - 1;
      }
      if (!dot_appeared) {
        dot_idx = length;
      }
      negative = b_negative;
      integer_zero = b_integer_zero;
      decimal_zero = b_decimal_zero;
      integer_start = i_integer_start;
      integer_end   = dot_idx - 1;
      decimal_start = dot_idx + 1;
    }
  }

  return ret;
}

bool ObNumberBuilder::negative() const
{
  return (NEGATIVE == sign_);
}

int64_t ObNumberBuilder::get_exp() const
{
  return exp_;
}

int64_t ObNumberBuilder::get_length() const
{
  return len_;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

ObDigitIterator::ObDigitIterator() : iter_idx_(0),
                                     iter_len_(0),
                                     iter_exp_(0)
{
}

ObDigitIterator::~ObDigitIterator()
{
}

int ObDigitIterator::get_next_digit(uint32_t &digit, bool &from_integer, bool &last_decimal)
{
  int ret = OB_SUCCESS;
  if (0 <= iter_exp_) {
    from_integer = true;
    if (iter_idx_ < iter_len_) {
      digit = get_digit(iter_idx_ ++);
    } else {
      digit = 0;
    }
    --iter_exp_;
  } else {
    from_integer = false;
    if (-1 == iter_exp_) {
      if (iter_idx_ >= iter_len_) {
        ret = OB_ITER_END;
      } else {
        digit = get_digit(iter_idx_ ++);
        last_decimal = (iter_idx_ == iter_len_);
      }
    } else {
      digit = 0;
      ++iter_exp_;
    }
  }
  return ret;
}

uint32_t ObDigitIterator::get_digit(const int64_t idx) const
{
  uint32_t ret_digit = 0;
  if (OB_ISNULL(digits_)) {
    LOG_EDIAG("the pointer is null", K(digits_));
  } else {
    ret_digit = digits_[idx];
    if (BASE <= ret_digit) {
      LOG_EDIAG("the param is invalid", K(ret_digit));
    }
  }

  return ret_digit;
}

void ObDigitIterator::reset_iter()
{
  iter_idx_ = 0;
  iter_len_ = len_;
  iter_exp_ = get_decode_exp(desc_);
}

void ObDigitIterator::assign(const uint32_t desc, uint32_t *digits)
{
  ObNumber::assign(desc, digits);
  reset_iter();
}

////////////////////////////////////////////////////////////////////////////////////////////////////

ObCalcVector::ObCalcVector() : base_(ObNumber::BASE),
                               length_(0),
                               digits_(buffer_)
{
}

ObCalcVector::~ObCalcVector()
{
}

ObCalcVector::ObCalcVector(const ObCalcVector &other)
{
  LIB_LOG(DEBUG, "copy assignment invoked");
  *this = other;
}

ObCalcVector &ObCalcVector::operator =(const ObCalcVector &other)
{
  LIB_LOG(DEBUG, "operator = invoked");
  if (this != &other) {
    base_ = other.base_;
    length_ = other.length_;
    digits_ = other.digits_;
  }
  return *this;
}

int ObCalcVector::init(const uint32_t desc, uint32_t *digits)
{
  ObDigitIterator di;
  int ret = OB_SUCCESS;
  di.assign(desc, digits);
  bool head_zero = true;
  uint32_t digit = 0;
  bool from_integer = false;
  bool last_decimal = false;
  length_ = 0;
  digits_ = buffer_;
  while (OB_SUCCESS == (ret = di.get_next_digit(digit, from_integer, last_decimal))) {
    if (head_zero) {
      if (0 == digit) {
        continue;
      } else {
        head_zero = false;
      }
    }
    digits_[length_ ++] = digit;
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

uint64_t ObCalcVector::at(const int64_t idx) const
{
  uint64_t ret_digit = 0;
  if (OB_ISNULL(digits_)) {
    LOG_EDIAG("the pointer is null");
  } else if (OB_UNLIKELY(idx <0 || idx > length_)) {
    LOG_EDIAG("the param is invalid");
  } else {
    ret_digit = digits_[idx];
  }
  return ret_digit;
}

uint64_t ObCalcVector::base() const
{
  return base_;
}

void ObCalcVector::set_base(const uint64_t base)
{
  base_ = base;
}

int64_t ObCalcVector::size() const
{
  return length_;
}

uint32_t *ObCalcVector::get_digits()
{
  return digits_;
}

int ObCalcVector::normalize()
{
  int64_t i = 0;
  int ret = OB_SUCCESS;
  if (length_ > 0 && OB_ISNULL(digits_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_EDIAG("the pointer is null");
  } else {
    for (; i < length_; ++i) {
      if (0 != digits_[i]) {
        break;
      }
    }
    length_ = length_ - i;
    if (0 == length_) {
      digits_ = NULL;
    } else {
      digits_ = &digits_[i];
    }
  }

  return ret;
}

int ObCalcVector::set(const int64_t idx, const uint64_t digit)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 > idx
      || idx >= length_
      || base_ <= digit
      || NULL == digits_)) {
    LOG_EDIAG("invalid param ",
              K(idx), K(length_), K(digit), K(base_));
    ret = OB_INVALID_ARGUMENT;
  } else {
    digits_[idx] = (uint32_t)digit;
  }
  return ret;
}

int ObCalcVector::ensure(const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buffer_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_EDIAG("the pointer is null");
  } else if (OB_UNLIKELY(ObNumber::MAX_CALC_LEN < size)) {
    ret = OB_NUMERIC_OVERFLOW;
  } else if (OB_UNLIKELY(digits_ < &buffer_[0]
             || digits_ > &buffer_[ObNumber::MAX_CALC_LEN - 1])) {
    LOG_EDIAG("digits is read only ", K(digits_), K(buffer_));
    ret = OB_ERR_READ_ONLY;
  } else {
    length_ = size;
  }
  return ret;
}

int ObCalcVector::resize(const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buffer_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_EDIAG("the pointer is null");
  } else if (OB_UNLIKELY(ObNumber::MAX_CALC_LEN < size)) {
    ret = OB_NUMERIC_OVERFLOW;
  } else if (OB_UNLIKELY(digits_ < &buffer_[0]
             || digits_ > &buffer_[ObNumber::MAX_CALC_LEN - 1])) {
    LOG_EDIAG("digits is read only", K(digits_), K(buffer_));
    ret = OB_ERR_READ_ONLY;
  } else {
    memset(digits_, 0, size * ITEM_SIZE(digits_));
    length_ = size;
  }
  return ret;
}

ObCalcVector ObCalcVector::ref(const int64_t start, const int64_t end) const
{
  ObCalcVector ret_calc_vec;
  if (OB_ISNULL(digits_)) {
    LOG_EDIAG("the pinter is null");
  } else {
    ret_calc_vec.length_ = end - start + 1;
    ret_calc_vec.digits_ = &digits_[start];
    ret_calc_vec.set_base(this->base());
  }

  return ret_calc_vec;
}

int ObCalcVector::assign(const ObCalcVector &other, const int64_t start, const int64_t end)
{
  int ret = OB_SUCCESS;
  if (0 < (end - start + 1)) {
    if (OB_ISNULL(digits_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_EDIAG( "the pointer is null");
    } else {
      MEMCPY(&digits_[start], other.digits_, (end - start + 1) * ITEM_SIZE(digits_));
    }
  }
  return ret;
}

int64_t ObCalcVector::to_string(char *buffer, const int64_t length) const
{
  int64_t pos = 0;

  if ((length_ > 0 && OB_ISNULL(digits_)) || length_ < 0) {
    LOG_EDIAG("the value is invalid");
  } else {
    databuff_printf(buffer, length, pos, "\"{length=%ld digits_ptr=%p buffer_ptr=%p digits=[",
                    length_, digits_, buffer_);
    for (int64_t i = 0; i < length_; ++i) {
      databuff_printf(buffer, length, pos, "%u,", digits_[i]);
    }
    databuff_printf(buffer, length, pos, "]}\"");
  }

  return pos;
}
}
}
}
