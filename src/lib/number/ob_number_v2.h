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

#ifndef OCEANBASE_LIB_NUMBER_OB_NUMBER_V2_H_
#define OCEANBASE_LIB_NUMBER_OB_NUMBER_V2_H_
#include <stdint.h>
#include <algorithm>
#include "common/ob_accuracy.h"
#include "lib/ob_define.h"
#include "lib/utility/utility.h"
#include "lib/utility/ob_print_utils.h"

#define ITEM_SIZE(ptr) sizeof((ptr)[0])

namespace oceanbase
{
namespace common
{
namespace number
{
// Poly: [a, b, c, d] = a * Base^3
//                    + b * Base^2
//                    + c * Base^1
//                    + d * Base^0;
// T should implement:
// uint64_t at(const int64_t idx) const;
// uint64_t base() const;
// int64_t size() const;
// int set(const int64_t idx, const uint64_t digit);
// int ensure(const int64_t size);
// T ref(const int64_t start, const int64_t end) const;
// int assign(const T &other, const int64_t start, const int64_t end);
// int64_t to_string(char* buffer, const int64_t length) const;
//
////////////////////////////////////////////////////////////////////////////////////////////////////

static const uint64_t MAX_BASE = 0x0000000100000000;

////////////////////////////////////////////////////////////////////////////////////////////////////

template <class T>
int poly_mono_mul(
    const T &multiplicand,
    const uint64_t multiplier,
    T &product);

template <class T>
int poly_mono_div(
    const T &dividend,
    const uint64_t divisor,
    T &quotient,
    uint64_t &remainder);

template <class T>
int poly_poly_add(
    const T &augend,
    const T &addend,
    T &sum);

template <class T>
int poly_poly_sub(
    const T &minuend,
    const T &subtrahend,
    T &remainder,
    bool &negative,
    const bool truevalue = true);

template <class T>
int poly_poly_mul(
    const T &multiplicand,
    const T &multiplier,
    T &product);

template <class T>
int poly_poly_div(
    const T &dividend,
    const T &divisor,
    T &quotient,
    T &remainder);

////////////////////////////////////////////////////////////////////////////////////////////////////

class ObCalcVector;
class ObNumber
{
  class IAllocator
  {
  public:
    virtual ~IAllocator() {PAUSE();};
    virtual uint32_t *alloc(const int64_t num) = 0;
  };
  template <class T>
  class TAllocator : public IAllocator
  {
  public:
    explicit TAllocator(T &allocator) __attribute__((noinline)) : allocator_(allocator) {};
    uint32_t *alloc(const int64_t num) {return (uint32_t *)allocator_.alloc(num);};
  private:
    T &allocator_;
  };
public:
  typedef ObNumberDesc Desc;
  static const uint64_t BASE = 1000000000;
  static const uint64_t BASE2 = 1000000000000000000;
  static const int64_t DIGIT_LEN = 9;
  static const char *DIGIT_FORMAT;
  static const char *BACK_DIGIT_FORMAT[DIGIT_LEN];
  static const ObPrecision MIN_PRECISION = 0;
  static const ObPrecision MAX_PRECISION = 65;
  static const ObScale MIN_SCALE = -84;
  static const ObScale MAX_SCALE = 127;
  static const ObScale MAX_TOTAL_SCALE = MAX_SCALE + MAX_PRECISION + 1;
  // 5 valid digits, another: 1 for round, 1 for negative end flag
  static const int64_t MAX_STORE_LEN = 9;
  static const int64_t MAX_BYTE_LEN = sizeof(uint32_t) * MAX_STORE_LEN + sizeof(Desc::desc_);
  static const ObScale FLOATING_SCALE = 72;  // 8 valid digits
  static const int64_t MAX_INTEGER_EXP = 14; // {9.999...999,count=38}e134
  static const int64_t MAX_DECIMAL_EXP = 15; // {1}e-135
  static const int64_t MAX_CALC_LEN = 32;
  static const uint8_t NEGATIVE = 0;
  static const uint8_t POSITIVE = 1;
  static const uint8_t EXP_ZERO = 0x40;
  static const int64_t MAX_PRINTABLE_SIZE = 256;
  static const int POSITIVE_EXP_BOUNDARY = 0xc0;
  static const int NEGATIVE_EXP_BOUNDARY = 0x40;
  static const ObNumber &get_positive_one();
  static const ObNumber &get_zero();
public:
  ObNumber();
  ObNumber(const uint32_t desc, uint32_t *digits);
  ~ObNumber();
public:
  template <class T>
  int from(const int64_t value, T &allocator);
  template <class T>
  int from(const uint64_t value, T &allocator);
  template <class T>
  int from(const char *str, T &allocator, int16_t *precision = NULL, int16_t *scale = NULL);
  template <class T>
  int from(const char *str, const int64_t length, T &allocator, int16_t *precision = NULL, int16_t *scale = NULL);
  template <class T>
  int from(const int16_t precision, const int16_t scale, const char *str, const int64_t length,
           T &allocator);
  template <class T>
  int from(const uint32_t desc, const ObCalcVector &vector, T &allocator);
  template <class T>
  int from(const ObNumber &other, T &allocator);

  void assign(const uint32_t desc, uint32_t *digits);
  int round(const int64_t scale);
  int floor(const int64_t scale);
  int ceil(const int64_t scale);
  int check_and_round(const int64_t precision, const int64_t scale);
  int trunc(const int64_t scale);
//  int to_mysql_binary(char *buffer, const int64_t length, int64_t &pos);

  template <class T>
  int add(const ObNumber &other, ObNumber &value, T &allocator) const;
  template <class T>
  int sub(const ObNumber &other, ObNumber &value, T &allocator) const;
  template <class T>
  int mul(const ObNumber &other, ObNumber &value, T &allocator) const;
  template <class T>
  int div(const ObNumber &other, ObNumber &value, T &allocator) const;
  template <class T>
  int rem(const ObNumber &other, ObNumber &value, T &allocator) const;
  template <class T>
  int negate(ObNumber &value, T &allocator) const;
  inline ObNumber negate() const __attribute__((always_inline));

  inline int compare(const ObNumber &other) const __attribute__((always_inline));
  inline bool is_equal(const ObNumber &other) const __attribute__((always_inline));
  inline bool operator<(const ObNumber &other) const __attribute__((always_inline));
  inline bool operator<=(const ObNumber &other) const __attribute__((always_inline));
  inline bool operator>(const ObNumber &other) const __attribute__((always_inline));
  inline bool operator>=(const ObNumber &other) const __attribute__((always_inline));
  inline bool operator==(const ObNumber &other) const __attribute__((always_inline));
  inline bool operator!=(const ObNumber &other) const __attribute__((always_inline));
  inline int abs_compare(const ObNumber &other) const __attribute__((always_inline));

  inline int compare(const int64_t &other) const __attribute__((always_inline));
  inline bool is_equal(const int64_t &other) const __attribute__((always_inline));
  inline bool operator<(const int64_t &other) const __attribute__((always_inline));
  inline bool operator<=(const int64_t &other) const __attribute__((always_inline));
  inline bool operator>(const int64_t &other) const __attribute__((always_inline));
  inline bool operator>=(const int64_t &other) const __attribute__((always_inline));
  inline bool operator==(const int64_t &other) const __attribute__((always_inline));
  inline bool operator!=(const int64_t &other) const __attribute__((always_inline));
  inline int abs_compare_sys(const uint64_t &other,
                             int64_t exp) const  __attribute__((always_inline));
  inline int is_in_uint(int64_t exp,
                        bool &is_uint,
                        uint64_t &num) const __attribute__((always_inline));;

  inline int compare(const uint64_t &other) const __attribute__((always_inline));
  inline bool is_equal(const uint64_t &other) const __attribute__((always_inline));
  inline bool operator<(const uint64_t &other) const __attribute__((always_inline));
  inline bool operator<=(const uint64_t &other) const __attribute__((always_inline));
  inline bool operator>(const uint64_t &other) const __attribute__((always_inline));
  inline bool operator>=(const uint64_t &other) const __attribute__((always_inline));
  inline bool operator==(const uint64_t &other) const __attribute__((always_inline));
  inline bool operator!=(const uint64_t &other) const __attribute__((always_inline));

  void set_zero();
  bool is_negative() const;
  bool is_zero() const;
  bool is_decimal() const;
  bool is_integer() const;

  inline static int uint32cmp(const uint32_t *s1, const uint32_t *s2,
                              const int64_t n) __attribute__((always_inline));
  inline static bool uint32equal(const uint32_t *s1, const uint32_t *s2,
                                 const int64_t n) __attribute__((always_inline));
  inline static uint32_t remove_back_zero(const uint32_t digit,
                                          int64_t &count) __attribute__((always_inline));
  inline static int64_t get_digit_len(const uint32_t v) __attribute__((always_inline));

  int64_t to_string(char *buffer, const int64_t length) const;
  int format(char *buf, const int64_t buf_len, int64_t &pos, int16_t scale) const;
  const char *format() const;

  OB_INLINE uint32_t get_desc_value() const;
  uint32_t get_desc() const;
  uint32_t *get_digits() const;
  int64_t get_length() const;
  int64_t get_cap() const;
  bool is_valid_uint64(uint64_t &uint64);
  bool is_valid_int64(int64_t &int64);
  bool is_int_parts_valid_int64(int64_t &int_parts, int64_t &decimal_parts);

protected:
  int from_(const int64_t value, IAllocator &allocator);
  int from_(const uint64_t value, IAllocator &allocator);
  int from_(const char *str,
            IAllocator &allocator,
            int16_t *precision = NULL,
            int16_t *scale = NULL);
  int from_(const char *str, const int64_t length, IAllocator &allocator, int &warning,
            int16_t *precision = NULL, int16_t *scale = NULL);
  int from_(const int16_t precision, const int16_t scale, const char *str, const int64_t length,
            IAllocator &allocator);
  int from_(const uint32_t desc, const ObCalcVector &vector, IAllocator &allocator);
  int from_(const ObNumber &other, IAllocator &allocator);
  int from_(const ObNumber &other, uint32_t *digits, uint8_t cap);
  int add_(const ObNumber &other, ObNumber &value, IAllocator &allocator) const;
  int sub_(const ObNumber &other, ObNumber &value, IAllocator &allocator) const;
  int mul_(const ObNumber &other, ObNumber &value, IAllocator &allocator) const;
  int div_(const ObNumber &other, ObNumber &value, IAllocator &allocator) const;
  int rem_(const ObNumber &other, ObNumber &value, IAllocator &allocator) const;
  int negate_(ObNumber &value, IAllocator &allocator) const;
protected:
  uint32_t *alloc_(IAllocator &allocator, const int64_t num);
  int check_precision_(const int64_t precision, const int64_t scale);
  int round_scale_(const int64_t scale, const bool using_floating_scale);
  int round_integer_(
      const int64_t scale,
      uint32_t *integer_digits,
      int64_t &integer_length);
  int round_decimal_(
      const int64_t scale,
      uint32_t *decimal_digits,
      int64_t &decimal_length);
  int trunc_scale_(const int64_t scale, const bool using_floating_scale);
  int trunc_integer_(const int64_t scale, uint32_t *integer_digits, int64_t &integer_length);
  int trunc_decimal_(const int64_t scale, uint32_t *decimal_digits, int64_t &decimal_length);
  int rebuild_digits_(
      const uint32_t *integer_digits,
      const int64_t integer_length,
      const uint32_t *decimal_digits,
      const int64_t decimal_length);
  int normalize_(uint32_t *digits, const int64_t length);
  inline static void exp_shift_(const int64_t shift, Desc &desc) __attribute__((always_inline));
  inline static int64_t exp_integer_align_(const Desc d1,
                                           const Desc d2) __attribute__((always_inline));
  inline static int64_t get_extend_length_remainder_(const Desc d1,
                                                     const Desc d2) __attribute__((always_inline));
  inline static int64_t get_decimal_extend_length_(const Desc d) __attribute__((always_inline));
  inline static int exp_cmp_(const Desc d1, const Desc d2) __attribute__((always_inline));
  inline static Desc exp_max_(const Desc d1, const Desc d2) __attribute__((always_inline));
  inline static Desc exp_mul_(const Desc d1, const Desc d2) __attribute__((always_inline));
  inline static Desc exp_div_(const Desc d1, const Desc d2) __attribute__((always_inline));
  inline static Desc exp_rem_(const Desc d1, const Desc d2) __attribute__((always_inline));
  inline static bool is_ge_1_(const Desc d) __attribute__((always_inline));
  inline static bool is_lt_1_(const Desc d) __attribute__((always_inline));
  inline static int exp_check_(const uint32_t desc) __attribute__((always_inline));
  inline static int64_t get_decode_exp(const uint32_t desc) __attribute__((always_inline));
public:
  struct \
  { \
    union \
    { \
      uint32_t desc_; \
        struct \
        { \
          uint8_t len_; \
            uint8_t cap_; \
            uint8_t flag_; \
            union \
            { \
              uint8_t se_; \
                struct \
                { \
                  uint8_t exp_:7; \
                    uint8_t sign_:1; \
                }; \
            }; \
        }; \
    }; \
  };
protected:
  uint32_t *digits_;
private:
  int check_range(bool *is_valid_uint64, bool *is_valid_int64,
                   uint64_t &int_parts, uint64_t &decimal_parts) const;
};

////////////////////////////////////////////////////////////////////////////////////////////////////

class StackAllocator
{
public:
  StackAllocator() : is_using_(false) {};
public:
  uint32_t *alloc(const int64_t num)
  {
    uint32_t *ret = NULL;
    if (!is_using_
        && num <= (int64_t)(ObNumber::MAX_STORE_LEN * ITEM_SIZE(buffer_))) {
      ret = buffer_;
      is_using_ = true;
    }
    return ret;
  };
private:
  uint32_t buffer_[ObNumber::MAX_STORE_LEN];
  bool is_using_;
};

class ObIntegerBuilder
{
public:
  ObIntegerBuilder();
  ~ObIntegerBuilder();
public:
  int push(const uint8_t d, const bool reduce_zero);
  int build(const char *str, const int64_t integer_start,
            const int64_t integer_end, const bool reduce_zero);
  int64_t get_exp() const;
  int64_t get_length() const;
  const uint32_t *get_digits() const;
  int64_t length() const;
  void reset();
private:
  int64_t exp_;
  int64_t digit_pos_;
  int64_t digit_idx_;
  uint32_t digits_[ObNumber::MAX_CALC_LEN];
};

class ObDecimalBuilder
{
public:
  ObDecimalBuilder();
  ~ObDecimalBuilder();
public:
  int push(const uint8_t d, const bool reduce_zero);
  int64_t get_exp() const;
  int64_t get_length() const;
  const uint32_t *get_digits() const;
  int64_t length() const;
  void reset();
private:
  int64_t exp_;
  int64_t digit_pos_;
  int64_t digit_idx_;
  uint32_t digits_[ObNumber::MAX_CALC_LEN];
};

class ObNumberBuilder : public ObNumber
{
public:
  ObNumberBuilder();
  ~ObNumberBuilder();
public:
  int build(const char *str,
            int64_t length,
            int &warning,
            int16_t *precision = NULL,
            int16_t *scale = NULL);
  bool negative() const;
  int64_t get_exp() const;
  int64_t get_length() const;
  void reset();
private:
  int find_point_(
      const char *str,
      int64_t &length,
      int64_t &integer_start,
      int64_t &integer_end,
      int64_t &decimal_start,
      bool &negative,
      bool &integer_zero,
      bool &decimal_zero,
      int &warning);
  int build_integer_(const char *str, const int64_t integer_start, const int64_t integer_end,
                     const bool reduce_zero);
  int build_decimal_(const char *str, const int64_t length, const int64_t decimal_start,
                     const bool reduce_zero);
private:
  ObIntegerBuilder ib_;
  ObDecimalBuilder db_;
};

class ObDigitIterator : public ObNumber
{
public:
  ObDigitIterator();
  ~ObDigitIterator();
public:
  void assign(const uint32_t desc, uint32_t *digits);
  int get_next_digit(uint32_t &digit, bool &from_integer, bool &last_decimal);
  void reset_iter();
  uint32_t get_digit(const int64_t idx) const;
private:
  int64_t iter_idx_;
  int64_t iter_len_;
  int64_t iter_exp_;
};

class ObCalcVector
{
public:
  ObCalcVector();
  ~ObCalcVector();
  ObCalcVector(const ObCalcVector &other);
  ObCalcVector &operator =(const ObCalcVector &other);
public:
  int init(const uint32_t desc, uint32_t *digits);
public:
  uint64_t at(const int64_t idx) const;
  uint64_t base() const;
  void set_base(const uint64_t base);
  int64_t size() const;
  uint32_t *get_digits();
  int set(const int64_t idx, const uint64_t digit);
  int ensure(const int64_t size);
  int resize(const int64_t size);
  ObCalcVector ref(const int64_t start, const int64_t end) const;
  int assign(const ObCalcVector &other, const int64_t start, const int64_t end);
  int64_t to_string(char *buffer, const int64_t length) const;
  int normalize();
private:
  uint64_t base_;
  int64_t length_;
  uint32_t *digits_;
  uint32_t buffer_[ObNumber::MAX_CALC_LEN];
};

template <class T>
class ObTRecover
{
public:
  ObTRecover(T &orig,
             int cond,
             int &observer) : orig_(orig),
                              recover_(orig),
                              cond_(cond),
                              observer_(observer)
  {
  };
  ~ObTRecover()
  {
    if (cond_ != observer_) {
      recover_ = orig_;
    }
  };
private:
  const T orig_;
  T &recover_;
  const int cond_;
  int &observer_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////

ObNumber ObNumber::negate() const
{
  ObNumber ret = *this;
  if (POSITIVE == ret.sign_) {
    ret.sign_ = NEGATIVE;
  } else {
    ret.sign_ = POSITIVE;
  }
  ret.exp_ = (0x7f & ~ret.exp_);
  ++ret.exp_;
  return ret;
}

int ObNumber::abs_compare(const ObNumber &other) const
{
  int ret = 0;
  if (!is_negative()) {
    if (!other.is_negative()) {
      ret = compare(other);
    } else {
      ret = compare(other.negate());
    }
  } else {
    if (other.is_negative()) {
      ret = -compare(other);
    } else {
      ret = negate().compare(other);
    }
  }
  return ret;
}

bool ObNumber::is_equal(const ObNumber &other) const
{
  bool bret = false;
  if (this->se_ == other.se_ && this->len_ == other.len_) {
    bret = uint32equal(this->digits_, other.digits_, this->len_) ;
  }
  return bret;
}

int ObNumber::compare(const ObNumber &other) const
{
  int ret = 0;
  if (this->se_ == other.se_) {
    if (this->len_ == other.len_) {
      ret = uint32cmp(this->digits_, other.digits_, this->len_);
    } else if (this->len_ < other.len_) {
      if (0 == (ret = uint32cmp(this->digits_, other.digits_, this->len_))) {
        ret = -1;
      }
    } else {
      if (0 == (ret = uint32cmp(this->digits_, other.digits_, other.len_))) {
        ret = 1;
      }
    }
    if (NEGATIVE == sign_) {
      ret = -ret;
    }
  } else if (this->se_ < other.se_) {
    ret = -1;
  } else {
    ret = 1;
  }
  OB_LOG(DEBUG, "current info", "this", *this, "other", other, K(ret));
  return ret;
}

template <class T>
int ObNumber::from(const int64_t value, T &allocator)
{
  TAllocator<T> ta(allocator);
  return from_(value, ta);
}

template <class T>
int ObNumber::from(const uint64_t value, T &allocator)
{
  TAllocator<T> ta(allocator);
  return from_(value, ta);
}

template <class T>
int ObNumber::from(const char *str, T &allocator, int16_t *precision, int16_t *scale)
{
  TAllocator<T> ta(allocator);
  return from_(str, ta, precision, scale);
}

template <class T>
int ObNumber::from(const char *str,
                   const int64_t length,
                   T &allocator,
                   int16_t *precision,
                   int16_t *scale)
{
  int ret = OB_SUCCESS;
  int warning = OB_SUCCESS;
  TAllocator<T> ta(allocator);
  ret = from_(str, length, ta, warning, precision, scale);
  if (OB_SUCCESS == ret && OB_SUCCESS != warning) {
    ret = warning;
  }
  return ret;
}

template <class T>
int ObNumber::from(const int16_t precision, const int16_t scale, const char *str,
                   const int64_t length, T &allocator)
{
  TAllocator<T> ta(allocator);
  return from_(precision, scale, str, length, ta);
}

template <class T>
int ObNumber::from(const uint32_t desc, const ObCalcVector &vector, T &allocator)
{
  TAllocator<T> ta(allocator);
  return from_(desc, vector, ta);
}

template <class T>
int ObNumber::from(const ObNumber &other, T &allocator)
{
  TAllocator<T> ta(allocator);
  return from_(other, ta);
}

template <class T>
int ObNumber::add(const ObNumber &other, ObNumber &value, T &allocator) const
{
  TAllocator<T> ta(allocator);
  return add_(other, value, ta);
}

template <class T>
int ObNumber::sub(const ObNumber &other, ObNumber &value, T &allocator) const
{
  TAllocator<T> ta(allocator);
  return sub_(other, value, ta);
}

template <class T>
int ObNumber::mul(const ObNumber &other, ObNumber &value, T &allocator) const
{
  TAllocator<T> ta(allocator);
  return mul_(other, value, ta);
}

template <class T>
int ObNumber::div(const ObNumber &other, ObNumber &value, T &allocator) const
{
  TAllocator<T> ta(allocator);
  return div_(other, value, ta);
}

template <class T>
int ObNumber::rem(const ObNumber &other, ObNumber &value, T &allocator) const
{
  TAllocator<T> ta(allocator);
  return rem_(other, value, ta);
}

template <class T>
int ObNumber::negate(ObNumber &value, T &allocator) const
{
  TAllocator<T> ta(allocator);
  return negate_(value, ta);
}

bool ObNumber::operator<(const ObNumber &other) const
{
  return (compare(other) < 0);
}

bool ObNumber::operator<=(const ObNumber &other) const
{
  return (compare(other) <= 0);
}

bool ObNumber::operator>(const ObNumber &other) const
{
  return (compare(other) > 0);
}

bool ObNumber::operator>=(const ObNumber &other) const
{
  return (compare(other) >= 0);
}

bool ObNumber::operator==(const ObNumber &other) const
{
  return  is_equal(other);
}

bool ObNumber::operator!=(const ObNumber &other) const
{
  return !is_equal(other);
}

int ObNumber::abs_compare_sys(const uint64_t &other, int64_t exp) const
{
  int cmp = 0;
  if (exp < 0) {
    cmp = (0 == other) ? 1 : -1;
  } else {
   /* uint64_t int_part = static_cast<uint64_t>(digits_[0]);
    switch (exp) {
      case 0 : cmp = (int_part >= other) ? ((int_part > other) ? 1 : ((len_ > 1) ? 1 : 0)) : -1; break;
      case 1 : {
        int_part = int_part * BASE + digits_[1];
        cmp = (int_part >= other) ? ((int_part > other) ? 1 : ((len_ > 2) ? 1 : 0)) : -1; break;
      }
      case 2 : {
        uint64_t part = other / (BASE * BASE);
        int64_t value = digits_[0] - part;
        if (0 == value) {
          int_part = digits_[1] * BASE + digits_[2];
          value = int_part - (other % (BASE * BASE));
          cmp = (value >= 0) ? ((value > 0) ? 1 : ((len_ > 3) ? 1 : 0)) : -1; break;
        } else {
          cmp = (value < 0) ? -1 : 1;
          break;
        }
      }
      default : cmp = -1;
    }   */
    // Optimization: use uint to compare
    int64_t exp_u = (other >= BASE2) ? 2 : (other >= BASE) ? 1 : 0;
    if (exp != exp_u) {
      cmp = exp > exp_u ? 1 : -1;
    } else {
      bool is_uint = true;
      uint64_t num = 0;
      if (OB_SUCCESS == is_in_uint(exp, is_uint, num)) {
        if (is_uint) {
           cmp = num > other ? 1 : num == other ? 0 : -1;
          if (0 == cmp) {
            cmp = len_ > exp + 1 ? 1 : 0;
          }
        } else {
          cmp = 1;
        }
      }
    }
  }
  return cmp;
}

int ObNumber::is_in_uint(int64_t exp, bool &is_uint, uint64_t &num) const
{
  //18  446744073   709551615
  int ret = OB_SUCCESS;
  int64_t len = len_;
  is_uint = true;
  num = 0;
  if (exp > 2) {
    is_uint = false;
  } else {
    if (OB_ISNULL(digits_)) {
      ret = OB_INVALID_ARGUMENT;
      LIB_LOG(ERROR, "the pointer is null");
    } else {
      if (2 == exp) {
        static const uint32_t unum[3] = {18, 446744073, 709551615};
        for (int i = 0 ; i < min(len , exp + 1); ++i) {
          if (digits_[i] != unum[i]) {
            is_uint = digits_[i] < unum[i];
            break;
          }
        }
      }
      if (is_uint) {
        for (int i = 0; exp >= 0 ; --exp, ++i, --len) {
          if (len > 0) {
            num = num * BASE + digits_[i];
          } else {
            num = num * BASE ; // zero padding
          }
        }
      }
    }
  }
  return ret;
}


bool ObNumber::is_equal(const int64_t &other) const
{
  return (0 == compare(other));
}

int ObNumber::compare(const int64_t &other) const
{
  int cmp = 0;
  if (is_zero()) {
    cmp = (other < 0) ? 1 : ((0 == other) ? 0 : -1);
  } else {
    if (is_negative()) {
      cmp = (other >= 0) ? -1 : - abs_compare_sys(static_cast<uint64_t>((other + 1)* (-1))
                                                  + static_cast<uint64_t>(1), EXP_ZERO - exp_);
    } else {
      cmp = (other <= 0) ? 1 : abs_compare_sys(static_cast<uint64_t>(other), exp_ - EXP_ZERO);
    }
  }
  return cmp;
}

bool ObNumber::operator<(const int64_t &other) const
{
  return (compare(other) < 0);
}

bool ObNumber::operator<=(const int64_t &other) const
{
  return (compare(other) <= 0);
}

bool ObNumber::operator>(const int64_t &other) const
{
  return (compare(other) > 0);
}

bool ObNumber::operator>=(const int64_t &other) const
{
  return (compare(other) >= 0);
}

bool ObNumber::operator==(const int64_t &other) const
{
  return (0 == compare(other));
}

bool ObNumber::operator!=(const int64_t &other) const
{
  return (0 != compare(other));
}

bool ObNumber::is_equal(const uint64_t &other) const
{
  return (0 == compare(other));
}

int ObNumber::compare(const uint64_t &other) const
{
  int cmp = 0;
  if (is_zero()) {
    cmp = (0 == other) ? 0 : -1;
  } else {
    if (is_negative()) {
      cmp = -1;
    } else {
      cmp = abs_compare_sys(other, exp_ - EXP_ZERO);
    }
  }
  return cmp;
}

bool ObNumber::operator<(const uint64_t &other) const
{
  return (compare(other) < 0);
}

bool ObNumber::operator<=(const uint64_t &other) const
{
  return (compare(other) <= 0);
}

bool ObNumber::operator>(const uint64_t &other) const
{
  return (compare(other) > 0);
}

bool ObNumber::operator>=(const uint64_t &other) const
{
  return (compare(other) >= 0);
}

bool ObNumber::operator==(const uint64_t &other) const
{
  return (0 == compare(other));
}

bool ObNumber::operator!=(const uint64_t &other) const
{
  return (0 != compare(other));
}


inline int ObNumber::uint32cmp(const uint32_t *s1, const uint32_t *s2, const int64_t n)
{
  int cret = 0;
  if (n > 0 && (OB_ISNULL(s1) || OB_ISNULL(s2))) {
    LIB_LOG(ERROR, "the poniter is null");
  } else {
    for (int64_t i = 0; i < n; ++i) {
      if (s1[i] < s2[i]) {
        cret = -1;
        break;
      } else if (s1[i] > s2[i]) {
        cret = 1;
        break;
      }
    }
  }
  return cret;
}

inline bool ObNumber::uint32equal(const uint32_t *s1, const uint32_t *s2, const int64_t n)
{
  bool is_equal = true;
  if (n > 0 && (OB_ISNULL(s1) || OB_ISNULL(s2))) {
    LIB_LOG(ERROR, "the poniter is null");
  } else {
    for (int64_t i = 0; i < n && is_equal; ++i) {
      if (s1[i] != s2[i]) {
        is_equal = false;
      }
    }
  }
  return is_equal;
}

inline uint32_t ObNumber::remove_back_zero(const uint32_t digit, int64_t &count)
{
  uint32_t ret = digit;
  count = 0;
  while (0 != ret
         && 0 == ret % 10) {
    ret = ret / 10;
    ++count;
  }
  return ret;
}

inline int64_t ObNumber::get_digit_len(const uint32_t d)
{
  int64_t ret = 0;
  if (BASE <= d) {
    LIB_LOG(ERROR, "d is out of range");
  } else {
    ret = (d >= 100000000) ? 9 :
          (d >= 10000000) ? 8 :
          (d >= 1000000) ? 7 :
          (d >= 100000) ? 6 :
          (d >= 10000) ? 5 :
          (d >= 1000) ? 4 :
          (d >= 100) ? 3 :
          (d >= 10) ? 2 :
          1;
  }

  return ret;
}

inline void ObNumber::exp_shift_(const int64_t shift, Desc &desc)
{
  int64_t exp = desc.exp_;
  if (NEGATIVE == desc.sign_) {
    exp = (0x7f & ~exp);
    exp += 1;
    exp += shift;
    exp = (0x7f & ~exp);
    exp += 1;
  } else {
    if (0 != exp) {
      exp += shift;
    }
  }
  desc.exp_ = (0x7f) & exp;
}

inline int64_t ObNumber::exp_integer_align_(const Desc d1, const Desc d2)
{
  int64_t ret = 0;

  if (d1.desc_ != d2.desc_) {
    int64_t exp1 = d1.exp_;
    int64_t exp2 = d2.exp_;
    int64_t len1 = d1.len_;
    int64_t len2 = d2.len_;
    if (NEGATIVE == d1.sign_) {
      exp1 = (0x7f & ~exp1);
      exp1 += 1;
    }
    if (NEGATIVE == d2.sign_) {
      exp2 = (0x7f & ~exp2);
      exp2 += 1;
    }
    if (0 != exp1) {
      exp1 -= EXP_ZERO;
    }
    if (0 != exp2) {
      exp2 -= EXP_ZERO;
    }
    int64_t min_exp1 = labs(std::min(exp1 - len1 + 1, (int64_t)0));
    int64_t min_exp2 = labs(std::min(exp2 - len2 + 1, (int64_t)0));
    ret = std::max(min_exp1, min_exp2);
  }

  return ret;
}

inline int64_t ObNumber::get_extend_length_remainder_(const Desc d1, const Desc d2)
{
  int64_t ret = 0;

  if (d1.desc_ != d2.desc_) {
    int64_t exp1 = d1.exp_;
    int64_t exp2 = d2.exp_;
    int64_t len1 = d1.len_;
    int64_t len2 = d2.len_;
    if (NEGATIVE == d1.sign_) {
      exp1 = (0x7f & ~exp1);
      exp1 += 1;
    }
    if (NEGATIVE == d2.sign_) {
      exp2 = (0x7f & ~exp2);
      exp2 += 1;
    }
    if (0 != exp1) {
      exp1 -= EXP_ZERO;
    }
    if (0 != exp2) {
      exp2 -= EXP_ZERO;
    }

    int64_t ext_len1 = (0 <= (exp1 - len1 + 1)) ? (exp1 + 1) : len1;
    int64_t ext_len2 = (0 <= (exp2 - len2 + 1)) ? (exp2 + 1) : len2;
    ret = ext_len1 - ext_len2;
  }

  return ret;
}

inline int64_t ObNumber::get_decimal_extend_length_(const Desc d)
{
  int64_t ret = 0;

  int64_t exp = d.exp_;
  int64_t len = d.len_;
  if (NEGATIVE == d.sign_) {
    exp = (0x7f & ~exp);
    exp += 1;
  }
  if (0 != exp) {
    exp -= EXP_ZERO;
  }
  ret = exp - len + 1;
  ret = (0 <= ret) ? 0 : -ret;

  return ret;
}

inline int ObNumber::exp_cmp_(const Desc d1, const Desc d2)
{
  int ret = 0;

  if (d1.desc_ != d2.desc_) {
    int64_t exp1 = d1.exp_;
    int64_t exp2 = d2.exp_;
    if (NEGATIVE == d1.sign_) {
      exp1 = (0x7f & ~exp1);
      exp1 += 1;
    }
    if (NEGATIVE == d2.sign_) {
      exp2 = (0x7f & ~exp2);
      exp2 += 1;
    }
    if (0 != exp1) {
      exp1 = labs(exp1 - EXP_ZERO);
    }
    if (0 != exp2) {
      exp2 = labs(exp2 - EXP_ZERO);
    }

    ret = (int)(exp1 - exp2);
  }

  return ret;
}

inline ObNumber::Desc ObNumber::exp_max_(const Desc d1, const Desc d2)
{
  Desc ret;
  int cmp_ret = exp_cmp_(d1, d2);
  if (0 <= cmp_ret) {
    ret = d1;
  } else {
    ret = d2;
  }
  return ret;
}

inline ObNumber::Desc ObNumber::exp_mul_(const Desc d1, const Desc d2)
{
  Desc ret;
  ret.desc_ = 0;

  int64_t exp1 = d1.exp_;
  int64_t exp2 = d2.exp_;
  if (NEGATIVE == d1.sign_) {
    exp1 = (0x7f & ~exp1);
    exp1 += 1;
  }
  exp1 -= EXP_ZERO;
  if (NEGATIVE == d2.sign_) {
    exp2 = (0x7f & ~exp2);
    exp2 += 1;
  }
  exp2 -= EXP_ZERO;

  ret.exp_ = 0x7f & (uint8_t)(exp1 + exp2 + EXP_ZERO);
  if (d1.sign_ != d2.sign_) {
    ret.sign_ = NEGATIVE;
    ret.exp_ = (0x7f & ~ret.exp_);
    ++ret.exp_;
  } else {
    ret.sign_ = POSITIVE;
  }
  return ret;
}

inline ObNumber::Desc ObNumber::exp_div_(const Desc d1, const Desc d2)
{
  Desc ret;
  ret.desc_ = 0;

  int64_t exp1 = d1.exp_;
  int64_t exp2 = d2.exp_;
  if (NEGATIVE == d1.sign_) {
    exp1 = (0x7f & ~exp1);
    exp1 += 1;
  }
  exp1 -= EXP_ZERO;
  if (NEGATIVE == d2.sign_) {
    exp2 = (0x7f & ~exp2);
    exp2 += 1;
  }
  exp2 -= EXP_ZERO;

  ret.exp_ = 0x7f & (uint8_t)(exp1 - exp2 + EXP_ZERO);
  if (d1.sign_ != d2.sign_) {
    ret.sign_ = NEGATIVE;
    ret.exp_ = (0x7f & ~ret.exp_);
    ++ret.exp_;
  } else {
    ret.sign_ = POSITIVE;
  }
  return ret;
}

inline ObNumber::Desc ObNumber::exp_rem_(const Desc d1, const Desc d2)
{
  Desc ret = d2;
  ret.sign_ = d1.sign_;
  if (d1.sign_ != d2.sign_) {
    ret.exp_ = (0x7f & ~ret.exp_);
    ++ret.exp_;
  }
  return ret;
}

inline bool ObNumber::is_lt_1_(const Desc d)
{
  bool bret = false;
  if ((NEGATIVE == d.sign_ && EXP_ZERO < d.exp_)
      || (POSITIVE == d.sign_ && 0 < d.exp_ && EXP_ZERO > d.exp_)) {
    bret = true;
  }
  return bret;
}

inline bool ObNumber::is_ge_1_(const Desc d)
{
  bool bret = false;
  if ((NEGATIVE == d.sign_ && EXP_ZERO >= d.exp_)
      || (POSITIVE == d.sign_ && EXP_ZERO <= d.exp_)) {
    bret = true;
  }
  return bret;
}

inline int ObNumber::exp_check_(const uint32_t desc)
{
  int ret = OB_SUCCESS;
  Desc d;
  d.desc_ = desc;
  int64_t exp = d.exp_;
  exp = labs(exp - EXP_ZERO);
  if (OB_UNLIKELY(MAX_INTEGER_EXP < exp
      && is_ge_1_(d))) {
    ret = OB_INTEGER_PRECISION_OVERFLOW;
  } else if (OB_UNLIKELY(MAX_DECIMAL_EXP < exp
      && is_lt_1_(d))) {
    ret = OB_DECIMAL_PRECISION_OVERFLOW;
  }
  return ret;
}

inline int64_t ObNumber::get_decode_exp(const uint32_t desc)
{
  Desc d;
  d.desc_ = desc;
  int64_t e = d.exp_;
  if (NEGATIVE == d.sign_
      && EXP_ZERO != d.exp_) {
    e = (0x7f & ~e);
    e += 1;
  }
  e = e - EXP_ZERO;
  return e;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

template <class T>
int poly_mono_mul(
    const T &multiplicand,
    const uint64_t multiplier,
    T &product)
{
  int ret = OB_SUCCESS;

  uint64_t base = multiplicand.base();
  int64_t multiplicand_size = multiplicand.size();
  int64_t product_size = product.size();

  if (OB_UNLIKELY(MAX_BASE < base
      || base <= multiplier
      || base != product.base()
      || 0 >= multiplicand_size
      || (multiplicand_size + 1) != product_size)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "the input param is invalid");
  } else {
    uint64_t carry = 0;
    int64_t multiplicand_idx = multiplicand_size - 1;
    int64_t product_idx = product_size - 1;
    while (product_idx >= 0) {
      uint64_t tmp = carry;
      if (multiplicand_idx >= 0) {
        tmp += (multiplicand.at(multiplicand_idx) * multiplier);
        --multiplicand_idx;
      }
      if (base <= tmp) {
        carry = tmp / base;
        tmp = tmp % base;
      } else {
        carry = 0;
      }
      if (OB_FAIL(product.set(product_idx, tmp))) {
        LIB_LOG(WARN, "set to product fail", K(ret), K(product_idx));
        break;
      }
      --product_idx;
    }
  }
  return ret;
}

template <class T>
int poly_mono_div(
    const T &dividend,
    const uint64_t divisor,
    T &quotient,
    uint64_t &remainder)
{
  int ret = OB_SUCCESS;

  uint64_t base = dividend.base();
  int64_t dividend_size = dividend.size();
  int64_t quotient_size = quotient.size();

  if (OB_UNLIKELY(MAX_BASE < base
      || base <= divisor
      || base != quotient.base()
      || 0 >= dividend_size
      || dividend_size != quotient_size)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "the input param is invalid");
  } else {
    uint64_t carry = 0;
    int64_t dividend_idx = 0;
    int64_t quotient_idx = 0;
    while (dividend_idx < dividend_size) {
      carry = carry * base + dividend.at(dividend_idx);
      ++dividend_idx;

      uint64_t tmp = carry / divisor;
      carry = carry % divisor;
      if (OB_FAIL(quotient.set(quotient_idx, tmp))) {
        LIB_LOG(WARN, "set to quotient fail", K(ret), K(quotient_idx));
        break;
      }
      ++quotient_idx;
    }
    remainder = carry;
  }
  return ret;
}

template <class T>
int poly_poly_add(
    const T &augend,
    const T &addend,
    T &sum)
{
  int ret = OB_SUCCESS;

  uint64_t base = augend.base();
  int64_t augend_size = augend.size();
  int64_t addend_size = addend.size();
  int64_t add_size = std::max(augend_size, addend_size) + 1;
  int64_t sum_size = sum.size();

  if (OB_UNLIKELY(MAX_BASE < base
      || base != addend.base()
      || base != sum.base()
      || 0 >= augend_size
      || 0 >= addend_size
      || add_size != sum_size)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "the input param is invalid");
  } else {
    uint64_t carry = 0;
    int64_t augend_idx = augend_size - 1;
    int64_t addend_idx = addend_size - 1;
    int64_t sum_idx = sum_size - 1;
    while (sum_idx >= 0) {
      uint64_t tmp = carry;
      if (augend_idx >= 0) {
        tmp += augend.at(augend_idx);
        --augend_idx;
      }
      if (addend_idx >= 0) {
        tmp += addend.at(addend_idx);
        --addend_idx;
      }
      if (base <= tmp) {
        tmp -= base;
        carry = 1;
      } else {
        carry = 0;
      }
      if (OB_FAIL(sum.set(sum_idx, tmp))) {
        LIB_LOG(WARN, "set to sum fail", K(ret), K(sum_idx));
        break;
      }
      --sum_idx;
    }
  }
  return ret;
}

template <class T>
int poly_poly_sub(
    const T &minuend,
    const T &subtrahend,
    T &remainder,
    bool &negative,
    const bool truevalue/* = true*/)
{
  int ret = OB_SUCCESS;

  uint64_t base = minuend.base();
  int64_t minuend_size = minuend.size();
  int64_t subtrahend_size = subtrahend.size();
  int64_t sub_size = std::max(minuend_size, subtrahend_size);
  int64_t remainder_size = remainder.size();

  if (OB_UNLIKELY(MAX_BASE < base
      || base != subtrahend.base()
      || base != remainder.base()
      || 0 >= minuend_size
      || 0 >= subtrahend_size
      || sub_size != remainder_size)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "the input param is invalid");
  } else {
    uint64_t borrow = 0;
    int64_t minuend_idx = minuend_size - 1;
    int64_t subtrahend_idx = subtrahend_size - 1;
    int64_t remainder_idx = remainder_size - 1;
    while (remainder_idx >= 0) {
      uint64_t tmp_subtrahend = borrow;
      if (subtrahend_idx >= 0) {
        tmp_subtrahend += subtrahend.at(subtrahend_idx);
        --subtrahend_idx;
      }
      uint64_t tmp_minuend = 0;
      if (minuend_idx >= 0) {
        tmp_minuend += minuend.at(minuend_idx);
        --minuend_idx;
      }
      if (tmp_minuend < tmp_subtrahend) {
        tmp_minuend += base;
        borrow = 1;
      } else {
        borrow = 0;
      }
      tmp_minuend -= tmp_subtrahend;
      if (OB_FAIL(remainder.set(remainder_idx, tmp_minuend))) {
        LIB_LOG(WARN, "set to remainder fail", K(ret), K(tmp_minuend));
        break;
      }
      --remainder_idx;
    }

    negative = (0 != borrow);
    if (OB_SUCCESS == ret
        && negative
        && truevalue) {
      uint64_t borrow = 0;
      for (int64_t i = remainder_size - 1; i >= 0; --i) {
        uint64_t tmp = borrow + remainder.at(i);
        if (0 < tmp) {
          tmp = base - tmp;
          borrow = 1;
        } else {
          borrow = 0;
        }
        remainder.set(i, tmp);
      }
    }
  }
  return ret;
}

template <class T>
int recursion_set_product(
    const uint64_t base,
    const uint64_t value,
    const int64_t start_idx,
    T &product)
{
  int ret = OB_SUCCESS;

  uint64_t carry = value;
  int64_t carry_idx = 0;
  while (0 != carry) {
    carry += product.at(start_idx - carry_idx);
    uint64_t tmp = carry % base;
    carry = carry / base;

    if (OB_FAIL(product.set(start_idx - carry_idx, tmp))) {
      LIB_LOG(WARN, "set to product fail",
              K(ret), K(start_idx), K(carry_idx), K(tmp), K(carry));
      break;
    }
    ++carry_idx;
  }
  return ret;
}

template <class T>
int poly_poly_mul(
    const T &multiplicand,
    const T &multiplier,
    T &product)
{
  int ret = OB_SUCCESS;

  uint64_t base = multiplicand.base();
  int64_t multiplicand_size = multiplicand.size();
  int64_t multiplier_size = multiplier.size();
  int64_t mul_size = multiplicand_size + multiplier_size;
  int64_t product_size = product.size();

  if (OB_UNLIKELY(MAX_BASE < base
      || base != multiplier.base()
      || base != product.base()
      || 0 >= multiplicand_size
      || 0 >= multiplier_size
      || mul_size != product_size)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "the input param is invalid");
  } else {
    // [a, b, c] * [d, e, f]
    // = (a*B^2 + b*B^1 + c*B^0) * (d*B^2 + e^B^1 + f*B^0)
    // = a*d*B^4 + (a*e+b*d)*B^3 + (a*f+b*e+c*d)*B^2 + (b*f+c*e)*B^1 + c*f*B^0
    for (int64_t i = 0; i < product_size; ++i) {
      product.set(i, 0);
    }
    for (int64_t multiplicand_idx = multiplicand_size - 1, product_idx = product_size - 1;
         OB_SUCCESS == ret && multiplicand_idx >= 0;
         --multiplicand_idx, --product_idx) {
      for (int64_t multiplier_idx = multiplier_size - 1; multiplier_idx >= 0; --multiplier_idx) {
        int64_t cur_product_idx = product_idx - (multiplier_size - 1 - multiplier_idx);
        uint64_t tmp_product = multiplicand.at(multiplicand_idx) * multiplier.at(multiplier_idx);

        if (OB_FAIL(recursion_set_product(base, tmp_product, cur_product_idx, product))) {
          LIB_LOG(WARN, "set product fail", K(tmp_product), K(product_idx));
          break;
        }
      }
    }
  }
  return ret;
}

template <class T>
uint64_t knuth_calc_delta(
    const int64_t j,
    T &u,
    T &v)
{
  uint64_t qq = 0;
  uint64_t base = u.base();

  if (u.at(j) == v.at(1)) {
    qq = base - 1;
  } else {
    qq = (u.at(j) * base + u.at(j + 1)) / v.at(1);
  }

  while (2 < v.size()) {
    uint64_t left = v.at(2) * qq;
    uint64_t right = (u.at(j) * base + u.at(j + 1) - qq * v.at(1)) * base + u.at(j + 2);
    if (left > right) {
      qq -= 1;
    } else {
      break;
    }
  }
  LIB_LOG(DEBUG, "after D3 ",
          K(j), K(qq));
  return qq;
}

template <class T>
int knuth_probe_quotient(
    const int64_t j,
    T &u,
    const T &v,
    uint64_t &qq)
{
  int ret = OB_SUCCESS;
  uint64_t base = u.base();
  int64_t n = v.size() - 1;

  // D4
  T qq_mul_v;
  qq_mul_v.set_base(base);
  T u_sub;
  u_sub.set_base(base);
  bool negative = false;
  bool truevalue = false;
  if (OB_FAIL(qq_mul_v.ensure(v.size() + 1))) {
    _OB_LOG(WARN, "ensure qq_mul_v fail, ret=%d size=%ld", ret, v.size() + 1);
  } else if (OB_FAIL(poly_mono_mul(v, qq, qq_mul_v))) {
    _OB_LOG(WARN, "%lu mul %s u=%s fail, ret=%d", qq, to_cstring(v), to_cstring(u), ret);
  } else if (OB_FAIL(u_sub.ensure(n + 1))) {
    _OB_LOG(WARN, "ensure u_sub fail, ret=%d size=%ld", ret, n + 1);
  } else if (OB_FAIL(poly_poly_sub(u.ref(j, j + n), qq_mul_v.ref(1, n + 1), u_sub,
                                                negative, truevalue))) {
    _OB_LOG(WARN, "%s sub %s fail, ret=%d", to_cstring(u.ref(j, j + n)),
            to_cstring(qq_mul_v), ret);
  } else if (!negative
             && OB_FAIL(u.assign(u_sub, j, j + n))) {
    _OB_LOG(WARN, "assign %s to u[%ld,%ld] fail, ret=%d", to_cstring(u_sub), j, j + n, ret);
  } else {
    // do nothing
  }
  LIB_LOG(DEBUG, "after D4",
          K(j), K(u), K(v), K(qq), K(negative),
          K(qq_mul_v), K(u_sub), K(ret));


  // D6
  if (negative
      && OB_SUCCESS == ret) {
    qq -= 1;
    T u_add;
    u_add.set_base(base);
    if (OB_FAIL(u_add.ensure(n + 2))) {
      _OB_LOG(WARN, "ensure u_add fail, ret=%d size=%ld", ret, n + 2);
    } else if (OB_FAIL(poly_poly_add(u_sub, v, u_add))) {
      _OB_LOG(WARN, "%s add %s fail, ret=%d", to_cstring(u_sub), to_cstring(v), ret);
    } else if (OB_FAIL(u.assign(u_add.ref(1, n + 1), j, j + n))) {
      _OB_LOG(WARN, "assign %s to u[%ld,%ld] fail, ret=%d",
              to_cstring(u_add.ref(1, n + 1)), j, j + n, ret);
    } else {
      // do nothing
    }
    LIB_LOG(DEBUG, "after D6 ",
            K(j), K(u), K(v), K(qq), K(u_add), K(ret));
  }

  return ret;
}

template <class T>
int poly_poly_div(
    const T &dividend,
    const T &divisor,
    T &quotient,
    T &remainder)
{
  int ret = OB_SUCCESS;

  uint64_t base = dividend.base();
  int64_t dividend_size = dividend.size();
  int64_t divisor_size = divisor.size();
  int64_t div_size = dividend_size - divisor_size + 1;
  int64_t quotient_size = quotient.size();
  int64_t remainder_size = remainder.size();

  int64_t n = divisor_size;
  int64_t m = dividend_size - n;
  T u;
  T v;
  T &q = quotient;
  u.set_base(base);
  v.set_base(base);

  if (OB_UNLIKELY(MAX_BASE < base
      || base != divisor.base()
      || base != quotient.base()
      || base != remainder.base()
      || 0 >= divisor_size
      || divisor_size >= dividend_size
      || div_size != quotient_size
      || divisor_size != remainder_size)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "the input param is invalid");
  } else {
    // Knuth Algo:
    // D1
    uint64_t d = base / (divisor.at(0) + 1);

    if (OB_FAIL(u.ensure(dividend_size + 1))) {
      _OB_LOG(WARN, "ensure dividend fail ret=%d size=%ld", ret, dividend_size + 1);
    } else if (OB_FAIL(v.ensure(divisor_size + 1))) {
      _OB_LOG(WARN, "ensure divisor fail ret=%d size=%ld", ret, divisor_size + 1);
    }
    if (OB_FAIL(poly_mono_mul(dividend, d, u))) {
      _OB_LOG(WARN, "%s mul %lu fail, ret=%d", to_cstring(dividend), d, ret);
    } else if (OB_FAIL(poly_mono_mul(divisor, d, v))) {
      _OB_LOG(WARN, "%s mul %lu fail, ret=%d", to_cstring(divisor), d, ret);
    } else {
      LIB_LOG(DEBUG, "Knuth Algo start",
              K(dividend), K(divisor), K(d), K(u), K(v), K(n), K(m));

      // D2
      int64_t j = 0;
      while (true) {
        // D3
        uint64_t qq = knuth_calc_delta(j, u, v);

        // D4, D6
        if (OB_FAIL(knuth_probe_quotient(j, u, v, qq))) {
          break;
        }

        // D5
        q.set(j, qq);
        LIB_LOG(DEBUG, "after D5",
                K(j), K(q));

        // D7
        j += 1;
        if (m < j) {
          break;
        }
      }

      if (OB_SUCC(ret)) {
        uint64_t r = 0;
        if (OB_FAIL(poly_mono_div(u.ref(m + 1, m + n), d, remainder, r))) {
          _OB_LOG(WARN, "%s div %lu fail, ret=%d", to_cstring(u.ref(m + 1, m + n)), d, ret);
        }
      }

      _OB_LOG(DEBUG, "Knuth Algo end, %s / %s = %s ... %s",
              to_cstring(dividend), to_cstring(divisor),
              to_cstring(quotient), to_cstring(remainder));
    }
  }
  return ret;
}

inline uint32_t ObNumber::get_desc_value() const
{
  return desc_;
}

}
} // end namespace common
} // end namespace oceanbase

#endif //OCEANBASE_LIB_NUMBER_OB_NUMBER_V2_H_
