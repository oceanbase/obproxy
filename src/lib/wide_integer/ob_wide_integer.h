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

#ifndef OB_WIDE_INTEGER_H_
#define OB_WIDE_INTEGER_H_

#include <limits.h>
#include <type_traits>
#include <initializer_list>

#include "lib/alloc/alloc_assist.h"
#include "common/ob_obj_type.h"
#include "lib/number/ob_number_v2.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace lib
{
  struct ObMemAttr;
} // end namespace lib
namespace common
{
struct ObObj;
// decimal int
namespace wide {
enum
{
  IgnoreOverFlow = 0,
  CheckOverFlow   = 1,
};
template<typename T> struct Limits;
template<typename T1, typename T2> struct CommonType;
template<typename Integer> struct PromotedInteger;
template <typename T> struct IsWideInteger;

template <unsigned Bits, typename Signed = signed>
struct ObWideInteger
{
  using dw_type          = unsigned __int128;

  constexpr static const unsigned   BYTE_COUNT          = Bits / CHAR_BIT;
  constexpr static const unsigned   ITEM_COUNT          = BYTE_COUNT / sizeof(uint64_t);
  constexpr static const unsigned   BASE_BITS           = sizeof(uint64_t) * CHAR_BIT;
  constexpr static const unsigned   BITS                = Bits;
  constexpr static const uint64_t   SIGN_BIT_MASK       = static_cast<uint64_t>(INT64_MIN);
  constexpr static const uint64_t   BASE_MAX            = UINT64_MAX;

  ObWideInteger() {
    MEMSET(items_, 0, sizeof(uint64_t) * ITEM_COUNT);
  }
  ObWideInteger(const ObWideInteger<Bits, Signed> &lhs) {
    MEMCPY(items_, lhs.items_, ITEM_COUNT * sizeof(uint64_t));
  }
  ObWideInteger(std::initializer_list<uint64_t> items)
  {
    int copy_items = items.size() < ITEM_COUNT ? items.size() : ITEM_COUNT;
    MEMCPY(items_, items.begin(), sizeof(uint64_t) * copy_items);
  }
  template<typename T>
  ObWideInteger(const T &rhs)
  {
    *this = rhs;
  }

  template <unsigned Bits2, typename Signed2>
  ObWideInteger<Bits, Signed> &operator=(const ObWideInteger<Bits2, Signed2> &rhs) {
    constexpr const unsigned copy_bits = (Bits < Bits2 ? Bits : Bits2);
    constexpr const unsigned copy_bytes = copy_bits / CHAR_BIT;
    constexpr const unsigned copy_items = copy_bytes / sizeof(uint64_t);

    for (unsigned i = 0; i < copy_items; i++) {
      items_[i] = rhs.items_[i];
    }
    if (Bits > Bits2) {
      if (_impl::is_negative(rhs)) {
        for (unsigned i = copy_items; i < ITEM_COUNT; i++) {
          items_[i] = UINT64_MAX;
        }
      } else {
        for (unsigned i = copy_items; i < ITEM_COUNT; i++) {
          items_[i] = 0;
        }
      }
    }
    return *this;
  }

  template<typename T>
  ObWideInteger<Bits, Signed> &operator=(const T &rhs)
  {
    if (sizeof(T) < BYTE_COUNT) {
      items_[0] = (uint64_t) rhs;
      if (_impl::is_negative(rhs)) {
        for (unsigned i = 1; i < ITEM_COUNT; i++) {
          items_[i] = UINT64_MAX;
        }
      } else {
        for (unsigned i = 1; i < ITEM_COUNT; i++) {
          items_[i] = 0;
        }
      }
    } else {
      for (unsigned i = 0; i < sizeof(T) / sizeof(uint64_t); i++) {
        items_[i] = _impl::get_item(rhs, i);
      }
      if (_impl::is_negative(rhs)) {
        for (unsigned i = sizeof(T) / sizeof(uint64_t); i < ITEM_COUNT; i++) {
          items_[i] = UINT64_MAX;
        }
      } else {
        for (unsigned i = sizeof(T) / sizeof(uint64_t); i < ITEM_COUNT; i++) {
          items_[i] = 0;
        }
      }
    }
    return *this;
  }

  operator int64_t() const
  {
    return static_cast<int64_t>(items_[0]);
  }

  template<int check_overflow, typename T>
  int multiply(const T &rhs, ObWideInteger<Bits, Signed> &res) const
  {
    return _impl::template multiply<check_overflow>(*this, rhs, res);
  }

  template<int check_overflow, typename T>
  int divide(const T &rhs, ObWideInteger<Bits, Signed> &res) const
  {
    ObWideInteger<Bits, Signed> rem;
    return _impl::template divide<check_overflow>(*this, rhs, res, rem);
  }

  template<typename T>
  int bitwise_and(const T &rhs, ObWideInteger<Bits, Signed> &res) const
  {
    return _impl::template bitwise_and(*this, rhs, res);
  }

  template<int check_overflow, typename T>
  int percent(const T &rhs, ObWideInteger<Bits, Signed> &rem) const
  {
    ObWideInteger<Bits, Signed> res;
    return _impl::template divide<check_overflow>(*this, rhs, res, rem);
  }

  template<int check_overflow, typename T>
  int add(const T &rhs, ObWideInteger<Bits, Signed> &res) const
  {
    if (_impl::is_negative(rhs)) {
      return _impl::template sub<check_overflow>(*this, -rhs, res);
    } else {
      return _impl::template add<check_overflow>(*this, rhs, res);
    }
  }

  template<int check_overflow, typename T>
  int sub(const T &rhs, ObWideInteger<Bits, Signed> &res) const
  {
    if (_impl::is_negative(rhs)) {
      return _impl::template add<check_overflow>(*this, -rhs, res);
    } else {
      return _impl::template sub<check_overflow>(*this, rhs, res);
    }
  }

  template<typename T>
  int cmp(const T &rhs) const
  {
    return _impl::cmp(*this, rhs);
  }

  ObWideInteger<Bits, Signed> &shift_left(unsigned n) const
  {
    _impl::shift_left(*this, *this, n);
    return *this;
  }

  ObWideInteger<Bits, Signed> &shift_right(unsigned n) const
  {
    _impl::shift_right(*this, *this, n);
    return *this;
  }

  ObWideInteger<Bits, Signed>& operator++()
  {
    _impl::template add<IgnoreOverFlow>(*this, 1, *this);
    return *this;
  }

  template<typename T>
  ObWideInteger<Bits, Signed>& operator+=(const T &rhs)
  {
    if (ObWideInteger<Bits, Signed>::_impl::is_negative(rhs)) {
      ObWideInteger<Bits, Signed>::_impl::template sub<IgnoreOverFlow>(*this, -rhs, *this);
    } else {
      ObWideInteger<Bits, Signed>::_impl::template add<IgnoreOverFlow>(*this, rhs, *this);
    }
    return *this;
  }

  ObWideInteger<Bits, Signed>& operator--()
  {
    _impl::template sub<IgnoreOverFlow>(*this, 1, *this);
    return *this;
  }

  template<typename T>
  ObWideInteger<Bits, Signed>& operator-=(const T &rhs)
  {
    if (ObWideInteger<Bits, Signed>::_impl::is_negative(rhs)) {
      ObWideInteger<Bits, Signed>::_impl::template add<IgnoreOverFlow>(*this, -rhs, *this);
    } else {
      ObWideInteger<Bits, Signed>::_impl::template sub<IgnoreOverFlow>(*this, rhs, *this);
    }
    return *this;
  }

  ObWideInteger<Bits, Signed> operator++(int)
  {
    ObWideInteger<Bits, Signed> ret = *this;
    _impl::template add<IgnoreOverFlow>(*this, 1, *this);
    return ret;
  }

  ObWideInteger<Bits, Signed> operator--(int)
  {
    ObWideInteger<Bits, Signed> ret = *this;
    _impl::template sub<IgnoreOverFlow>(*this, 1, *this);
    return ret;
  }

  // helpers
  bool is_negative() const
  {
    return _impl::is_negative(*this);
  }

  uint64_t items_[ITEM_COUNT];

  struct _impl
{
  static unsigned big(unsigned i)
  {
    return ITEM_COUNT - i - 1;
  }

  template<typename T>
  static bool is_negative(const T &x)
  {
    if (std::is_signed<T>::value) {
      return x < 0;
    }
    return false;
  }

  template<unsigned B, typename S>
  static bool is_negative(const ObWideInteger<B, S> &x)
  {
    if (std::is_same<S, signed>::value) {
      if (x.items_[ObWideInteger<B, S>::_impl::big(0)] & SIGN_BIT_MASK) {
        return true;
      }
    }
    return false;
  }

  template<unsigned B, typename S>
  static bool is_zero(const ObWideInteger<B, S> &x)
  {
    for (unsigned i = ObWideInteger<B, S>::ITEM_COUNT; i > 0; --i) {
      if (0 != x.items_[i - 1]) {
        return false;
      }
    }
    return true;
  }

  template<typename T>
  static T make_positive(const T &x)
  {
    if (std::is_signed<T>::value && x < 0) {
      return -x;
    }
    return x;
  }

  template<unsigned B, typename S>
  static ObWideInteger<B, S> make_positive(const ObWideInteger<B, S> &x)
  {
    if (is_negative(x)) {
      ObWideInteger<B, S> res;
      ObWideInteger<B, S>::_impl::template unary_minus<IgnoreOverFlow>(x, res);
      return res;
    }
    return x;
  }

  template<typename T>
  static bool should_keep_size()
  {
    return sizeof(T) <= BYTE_COUNT;
  }

  template<typename T>
  static int nlz(const T &x)
  {
    int cnt_zero = 0;
    if (sizeof(T) <= 4) { // uint32
      cnt_zero = __builtin_clz(x);
    } else if (sizeof(T) <= 8) { // uint64
      cnt_zero = __builtin_clzll(x);
    }
    return cnt_zero;
  }

  template<unsigned B, typename S>
  static int nlz(const ObWideInteger<B, S> &x)
  {
    int cnt_zero = 0;
    for (unsigned i = 0; i < ObWideInteger<B, S>::ITEM_COUNT; i++) {
      unsigned idx = ObWideInteger<B, S>::_impl::big(i);
      if (x.items_[idx]) {
        cnt_zero += __builtin_clzll(x.items_[idx]);
        break;
      } else {
        cnt_zero += ObWideInteger<B, S>::BASE_BITS;
      }
    }
    return cnt_zero;
  }

  template<unsigned B, typename S>
  static unsigned item_size(const ObWideInteger<B, S> &x)
  {
    unsigned i = ObWideInteger<B, S>::ITEM_COUNT;
    for (; i > 0; i--) {
      if (x.items_[i-1] != 0) {
        break;
      }
    }
    return i;
  }
  // check if x > abs(ObWideInteger<Bits, Signed>::MIN_VAL)
  // x is treated as unsigned value
  template<unsigned B, typename S>
  static bool overflow_negative_range(const ObWideInteger<B, S> &x)
  {
    uint64_t big0 = x.items_[ObWideInteger<B, S>::_impl::big(0)];
    if (big0 > ObWideInteger<B, S>::SIGN_BIT_MASK) {
      return true;
    } else if (big0 == ObWideInteger<B, unsigned>::SIGN_BIT_MASK) {
      for (unsigned i = 0; i < ObWideInteger<B, unsigned>::ITEM_COUNT - 1; i++) {
        if (x.items_[i] != 0) {
          return true;
        }
      }
    }
    return false;
  }

  // check if x > ObWideInteger<Bits, Signed>::MAX_VAL
  // x is treated as unsigned value
  template<unsigned B, typename S>
  static bool overflow_positive_range(const ObWideInteger<B, S> &x)
  {
    uint64_t big0 = x.items_[ObWideInteger<B, S>::_impl::big(0)];
    if (big0 >= ObWideInteger<B, unsigned>::SIGN_BIT_MASK) {
      return true;
    }
    return false;
  }

  static bool add_sub_overflow(bool l_neg, bool r_neg, bool res_neg)
  {
    // positive + positive = negative => overflow
    // negative + negative = positive => overflow
    if (l_neg && r_neg) {
      return !res_neg;
    } else if (!l_neg && !r_neg) {
      return res_neg;
    }
    return false;
  }

  template <unsigned Bits2, typename Signed2>
  static bool within_limits(const ObWideInteger<Bits2, Signed2> &x)
  {
    return ObWideInteger<Bits2, Signed2>::_impl::cmp(
               x, Limits<ObWideInteger<Bits, Signed>>::max()) <= 0 &&
           ObWideInteger<Bits2, Signed2>::_impl::cmp(
               x, Limits<ObWideInteger<Bits, Signed>>::min()) >= 0;
  }

  template<typename T, class = typename std::enable_if<std::is_integral<T>::value>::type>
  static uint64_t get_item(const T &val, unsigned idx)
  {
    if (sizeof(T) <= sizeof(uint64_t)) {
      if (idx == 0) {
        return static_cast<uint64_t>(val);
      }
      return 0;
    } else if (idx * sizeof(uint64_t) < sizeof(T)) {
      return static_cast<uint64_t>(val >> (idx * BASE_BITS));
    } else {
      return 0;
    }
  }

  template<unsigned B, typename S>
  static uint64_t get_item(const ObWideInteger<B, S> &x, unsigned idx)
  {
    if (idx < ObWideInteger<B, S>::ITEM_COUNT) {
      return x.items_[idx];
    }
    return 0;
  }

  static int shift_left(
    const ObWideInteger<Bits, Signed> &self,
    ObWideInteger<Bits, Signed> &res,
    unsigned n)
  {
    int ret = OB_SUCCESS;
    if ((void *)&res != (void *)&self) {
      res = self;
    }
    if (n >= Bits) {
      MEMSET(res.items_, 0, sizeof(uint64_t) * ITEM_COUNT);
    } else {
      unsigned item_shift = n / BASE_BITS;
      unsigned bits_shift = n % BASE_BITS;
      if (bits_shift) {
        unsigned right_bits = BASE_BITS - bits_shift;
        res.items_[big(0)] = res.items_[big(item_shift)] << bits_shift;
        for (unsigned i = 1; i < ITEM_COUNT - item_shift; i++) {
          res.items_[big(i - 1)] |= res.items_[big(item_shift + i)] >> right_bits;
          res.items_[big(i)] = res.items_[big(i + item_shift)] << bits_shift;
        }
      } else {
        for (unsigned i = 0; i < ITEM_COUNT - item_shift; i++) {
          res.items_[big(i)] = res.items_[big(i + item_shift)];
        }
      }
      for (unsigned i = 0; i < item_shift; i++) {
        res.items_[i] = 0;
      }
    }
    return ret;
  }

  static int shift_right(
    const ObWideInteger<Bits, Signed> &self,
    ObWideInteger<Bits, Signed> &res,
    unsigned n)
  {
    int ret = OB_SUCCESS;
    if ((void *)&res != (void *)&self) {
      res = self;
    }
    bool is_neg = is_negative(self);
    if (n >= Bits) {
      if (is_neg) {
        for (unsigned i = 0; i < ITEM_COUNT; i++) {
          res.items_[i] = UINT64_MAX;
        }
      } else {
        MEMSET(res.items_, 0, sizeof(uint64_t) * ITEM_COUNT);
      }
    } else {
      unsigned item_shift = n / BASE_BITS;
      unsigned bits_shift = n % BASE_BITS;
      if (bits_shift) {
        unsigned left_bits = BASE_BITS - bits_shift;
        res.items_[0] = res.items_[item_shift] >> bits_shift;
        for (unsigned i = 1; i < ITEM_COUNT - item_shift; i++) {
          res.items_[i - 1] |=
              (res.items_[i + item_shift] << left_bits);
          res.items_[i] =
              res.items_[i + item_shift] >> bits_shift;
        }
      } else {
        for (unsigned i = 0; i < ITEM_COUNT - item_shift; i++) {
          res.items_[i] = res.items_[i + item_shift];
        }
      }
      if (is_neg) {
        if (bits_shift) {
          res.items_[big(item_shift)] |= (BASE_MAX << (BASE_BITS - bits_shift));
        }
        for (unsigned i = 0; i < item_shift; i++) {
          res.items_[big(i)] = BASE_MAX;
        }
      } else {
        for (unsigned i = 0; i < item_shift; i++) {
          res.items_[big(i)] = 0;
        }
      }
    }
    return ret;
  }

  template <int check_overflow, unsigned Bits2, typename Signed2>
  static int add(
    const ObWideInteger<Bits, Signed> &lhs,
    const ObWideInteger<Bits2, Signed2> &rhs,
    ObWideInteger<Bits, Signed> &res
  )
  {
    int ret = OB_SUCCESS;
    if (Bits >= Bits2) {
      constexpr const unsigned op_items = ObWideInteger<Bits2, Signed2>::ITEM_COUNT;
      if ((void *)&res != (void *)&lhs) {
        res = lhs;
      }
      bool l_neg = is_negative(lhs);
      bool r_neg = is_negative(rhs);
      bool overflow = false;
      for (unsigned i = 0; i < op_items; i++) {
        uint64_t r_val = rhs.items_[i];
        if (overflow) {
          res.items_[i]++;
          overflow = (res.items_[i] == 0);
        }
        res.items_[i] += r_val;
        overflow = (overflow || (res.items_[i] < r_val));
      }
      for (unsigned i = op_items; overflow && i < ITEM_COUNT; i++) {
        res.items_[i]++;
        overflow = (res.items_[i] == 0);
      }
      if (check_overflow && add_sub_overflow(l_neg, r_neg, is_negative(res))) {
        ret = OB_OPERATE_OVERFLOW;
      }
    } else {
      using calc_type =
          typename CommonType<ObWideInteger<Bits, Signed>,
                              ObWideInteger<Bits2, Signed2>>::type;
      calc_type xres;
      ret = calc_type::_impl::template add<check_overflow>(calc_type(lhs), rhs, xres);
      res = xres;
      if (check_overflow && OB_SUCCESS == ret) {
        if (!within_limits(xres)) {
          ret = OB_OPERATE_OVERFLOW;
        }
      }
    }
    return ret;
  }
  template <int check_overflow, typename T>
  static int add(
    const ObWideInteger<Bits> &self, const T &rhs,
    ObWideInteger<Bits, Signed> &res)
  {
    int ret = OB_SUCCESS;
    if (should_keep_size<T>()) {
      constexpr const unsigned r_items =
          (sizeof(T) < sizeof(uint64_t)) ? 1 : sizeof(T) / sizeof(uint64_t);
      constexpr const unsigned op_items = (r_items < ITEM_COUNT) ? r_items : ITEM_COUNT;

      if ((void *)&res != (void *)&self) {
        res = self;
      }
      bool l_neg = is_negative(self);
      bool r_neg = is_negative(rhs);
      bool overflow = false;
      for (unsigned i = 0; i < op_items; i++) {
        uint64_t r_val = get_item(rhs, i);
        if (overflow) {
          res.items_[i]++;
          overflow = (res.items_[i] == 0);
        }
        res.items_[i] += r_val;
        overflow = (overflow || (res.items_[i] < r_val));
      }
      for (unsigned i = op_items; overflow && i < ITEM_COUNT; i++) {
        res.items_[i]++;
        overflow = (res.items_[i] == 0);
      }
      if (check_overflow && add_sub_overflow(l_neg, r_neg, is_negative(res))) {
        ret = OB_OPERATE_OVERFLOW;
      }
    } else {
      using calc_type = typename CommonType<ObWideInteger<Bits, Signed>, T>::type;
      calc_type xres;
      ret = calc_type::_impl::template add<check_overflow>(calc_type(self), rhs, xres);
      res = xres;
      if (check_overflow && OB_SUCCESS == ret) {
        if (!within_limits(xres)) {
          ret = OB_OPERATE_OVERFLOW;
        }
      }
    }
    return ret;
  }

  template<int check_overflow, unsigned Bits2, typename Signed2>
  static int sub(
    const ObWideInteger<Bits, Signed> &lhs, const ObWideInteger<Bits2, Signed2> &rhs,
    ObWideInteger<Bits, Signed> &res
  )
  {
    int ret = OB_SUCCESS;
    if (Bits >= Bits2) {
      constexpr const unsigned op_items = ObWideInteger<Bits2, Signed2>::ITEM_COUNT;
      if ((void *)&res != (void *)&lhs) {
        res = lhs;
      }
      bool l_neg = is_negative(lhs);
      bool r_neg = is_negative(rhs);
      bool borrow = false;
      for (unsigned i = 0; i < op_items; i++) {
        uint64_t r_val = rhs.items_[i];
        if (borrow) {
          res.items_[i]--;
          borrow = (res.items_[i] == BASE_MAX);
        }
        borrow = (borrow || (res.items_[i] < r_val));
        res.items_[i] -= r_val;
      }
      for (unsigned i = op_items; borrow && i < ITEM_COUNT; i++) {
        res.items_[i]--;
        borrow = (res.items_[i] == BASE_MAX);
      }
      if (check_overflow && add_sub_overflow(l_neg, r_neg, is_negative(res))) {
        ret = OB_OPERATE_OVERFLOW;
      }
    } else {
      using calc_type =
          typename CommonType<ObWideInteger<Bits, Signed>,
                              ObWideInteger<Bits2, Signed2>>::type;
      calc_type xres;
      ret = calc_type::_impl::template sub<check_overflow>(calc_type(lhs), rhs, xres);
      res = xres;
      if (check_overflow && OB_SUCCESS == ret) {
        if (!within_limits(xres)) {
          ret = OB_OPERATE_OVERFLOW;
        }
      }
    }
    return ret;
  }
  template<int check_overflow, typename T>
  static int sub(
    const ObWideInteger<Bits, Signed> &self,const T &rhs,ObWideInteger<Bits, Signed> &res
  )
  {
    int ret = OB_SUCCESS;
    if (should_keep_size<T>()) {
      constexpr const unsigned r_items =
        (sizeof(T) < sizeof(uint64_t)) ? 1 : sizeof(T) / sizeof(uint64_t);
      constexpr const unsigned op_items = (r_items < ITEM_COUNT) ? r_items : ITEM_COUNT;

      if ((void *)&res != (void *)&self) {
        res = self;
      }
      bool l_neg = is_negative(self);
      bool r_neg = is_negative(rhs);
      bool borrow = false;
      for (unsigned i = 0; i < op_items; i++) {
        uint64_t r_val = get_item(rhs, i);
        if (borrow) {
          res.items_[i]--;
          borrow = (res.items_[i] == BASE_MAX);
        }
        borrow = (borrow || (res.items_[i] < r_val));
        res.items_[i] -= r_val;
      }
      for (unsigned i = op_items; borrow && i < ITEM_COUNT; i++) {
        res.items_[i]--;
        borrow = (res.items_[i] == BASE_MAX);
      }
      if (check_overflow) {
        bool res_neg = is_negative(res);
        // positive - negative = negative => overflow
        // negative - positive = positive => overflow
        if (!l_neg && r_neg) {
          if (res_neg) {
            ret = OB_OPERATE_OVERFLOW;
          }
        } else if (l_neg && !r_neg) {
          if (!res_neg) {
            ret = OB_OPERATE_OVERFLOW;
          }
        }
      }
    } else {
      using calc_type = typename CommonType<ObWideInteger<Bits, Signed>, T>::type;
      calc_type xres;
      ret = calc_type::_impl::template sub<check_overflow>(calc_type(self), rhs, xres);
      res = xres;
      if (check_overflow && OB_SUCCESS == ret) {
        if (!within_limits(xres)) {
          ret = OB_OPERATE_OVERFLOW;
        }
      }
    }
    return ret;
  }

  template<int check_overflow>
  static int unary_minus(const ObWideInteger<Bits, Signed> &self, ObWideInteger<Bits, Signed> &res)
  {
    int ret = OB_SUCCESS;

    if ((void *)&res != (void *)&self) {
      res = self;
    }
    bool is_neg = is_negative(res);
    for (unsigned i = 0; i < ITEM_COUNT; i++) {
      res.items_[i] = ~res.items_[i];
    }
    add<IgnoreOverFlow>(res, 1, res);
    if (check_overflow) {
      // abs(min_val) overflow max_val
      if (is_neg && is_negative(res)) {
        ret = OB_OPERATE_OVERFLOW;
      }
    }
    return ret;
  }

  template <int check_overflow, typename T>
  static int multiply(
    const ObWideInteger<Bits, Signed> &lhs, const T &rhs,
    ObWideInteger<Bits, Signed> &res)
  {
    int ret = OB_SUCCESS;
    if (rhs == 0) {
      res = 0;
    } else if (rhs == 1) {
      res = lhs;
    } else if (sizeof(T) <= sizeof(uint64_t)) {
      res = make_positive(lhs);
      dw_type rval = make_positive(rhs);
      dw_type carrier = 0;
      for (unsigned i = 0; i < ITEM_COUNT; i++) {
        carrier += static_cast<dw_type>(res.items_[i]) * rval;
        res.items_[i] = static_cast<uint64_t>(carrier);
        carrier >>= 64;
      }
      bool l_neg = is_negative(lhs);
      bool r_neg = is_negative(rhs);
      if (check_overflow) {
        // if carrier != 0 => must overflow
        // else if result is negative, check if abs(res) >= abs(min_val)
        // else check if abs(res) >= abs(max_val)
        if (carrier) {
          ret = OB_OPERATE_OVERFLOW;
        } else if (l_neg != r_neg && overflow_negative_range(res)) {
          ret = OB_OPERATE_OVERFLOW;
        } else if (l_neg == r_neg && overflow_positive_range(res)) {
          ret = OB_OPERATE_OVERFLOW;
        }
      }
      if (l_neg != r_neg) {
        unary_minus<IgnoreOverFlow>(res, res);
      }
    } else {
      using calc_type = typename CommonType<ObWideInteger<Bits, Signed>, T>::type;
      calc_type xres;
      ret = calc_type::_impl::template multiply<check_overflow>(
        calc_type(lhs), calc_type(rhs), xres);
      res = xres;
      if (check_overflow && ret == OB_SUCCESS) {
        if (!within_limits(xres)) {
          ret = OB_OPERATE_OVERFLOW;
        }
      }
    }
    return ret;
  }


  template <int check_overflow, unsigned Bits2, typename Signed2>
  static int multiply(
    const ObWideInteger<Bits, Signed> &lhs, const ObWideInteger<Bits2, Signed2> &rhs,
    ObWideInteger<Bits, Signed> &res)
  {
    int ret = OB_SUCCESS;
    if (Bits >= Bits2) {
      if (Bits == 128) {
        dw_type lop = *(reinterpret_cast<const dw_type *>(lhs.items_));
        dw_type rop = *(reinterpret_cast<const dw_type *>(rhs.items_));
        dw_type result = lop * rop;
        res.items_[1] = static_cast<uint64_t>(result>>BASE_BITS);
        res.items_[0] = static_cast<uint64_t>(result);
        if (check_overflow) {
          ObWideInteger<Bits, unsigned> tmpl = make_positive(lhs);
          ObWideInteger<Bits, unsigned> tmpr = make_positive(rhs);
          uint64_t h0 = get_item(tmpl, 1);
          uint64_t h1 = get_item(tmpr, 1);
          uint64_t l0 = get_item(tmpl, 0);
          uint64_t l1 = get_item(tmpr, 0);
          uint64_t tmp = 0;
          // when int128 multiplies int128, we have
          //           h0    l0
          //  *        h1    l1
          // ---------------------
          //         h0*l1  l0*l1
          //  h0*h1  h1*l0
          //
          //  if h0 > 0 & h1 > 0 => must overflow
          //  else if h0*l1 overflow uint64 or h1*l0 overflow uint64 => must overflow
          //  otherwise cast to int256 to calculate result and check overflow
          if (h0 > 0 && h1 > 0) {
            ret = OB_OPERATE_OVERFLOW;
          } else if (__builtin_mul_overflow(h0, l1, &tmp) || __builtin_mul_overflow(h1, l0, &tmp)) {
            ret = OB_OPERATE_OVERFLOW;
          } else {
            using calc_type = typename PromotedInteger<ObWideInteger<Bits, Signed>>::type;
            calc_type xres;
            calc_type::_impl::template multiply<IgnoreOverFlow>(calc_type(lhs), rhs, xres);
            if (!within_limits(xres)) {
              ret = OB_OPERATE_OVERFLOW;
            }
          }
        }
      } else {
        ObWideInteger<Bits, unsigned> lval = make_positive(lhs);
        ObWideInteger<Bits2, unsigned> rval = make_positive(rhs);

        unsigned outter_limit = item_size(lval);
        unsigned inner_limit = item_size(rval);
        res = 0;
        dw_type carrier = 0;
        bool overflow = false;
        for (unsigned j = 0; j < inner_limit; j++) {
          for (unsigned i = 0; i < outter_limit; i++) {
            carrier += res.items_[i + j] + static_cast<dw_type>(lval.items_[i]) *
                                           static_cast<dw_type>(rval.items_[j]);
            if (i + j < ITEM_COUNT) {
              res.items_[i+j] = static_cast<uint64_t>(carrier);
              carrier >>= 64;
            } else {
              overflow = true;
              break;
            }
          }
          if (carrier) {
            if (j + outter_limit < ITEM_COUNT) {
              res.items_[j + outter_limit] = static_cast<uint64_t>(carrier);
            } else {
              overflow = true;
            }
            carrier = 0;
          }
        }
        bool l_neg = is_negative(lhs);
        bool r_neg = is_negative(rhs);
        if (check_overflow) {
          // for example int256 * int256, we have
          //
          //                               l3       l2      l1      l0
          //    *                          r3       r2      r1      r0
          //   -----------------------------------------------------------
          //                             l3*r0     l2*r0   l1*r0  l0*r0
          //                   l3*r1     l2*r1     r1*l1   r1*l0
          //...
          // if any(res_items[i] > 0, i in [item_count, 2*item_count]) => must overflow
          // else if result is positive, check if abs(result) overflow max_val
          // else if result is negative, check if abs(result) overflow abs(min_val)
          // else not overflow
          if (overflow) {
            ret = OB_OPERATE_OVERFLOW;
          } else if (l_neg != r_neg && overflow_negative_range(res)) {
            ret = OB_OPERATE_OVERFLOW;
          } else if (l_neg == r_neg && overflow_positive_range(res)) {
            ret = OB_OPERATE_OVERFLOW;
          }
        }
        if(l_neg != r_neg) {
          unary_minus<IgnoreOverFlow>(res, res);
        }
      }
    } else {
      using calc_type = typename CommonType<ObWideInteger<Bits>, ObWideInteger<Bits2>>::type;
      calc_type xres;
      ret = calc_type::_impl::template multiply<check_overflow>(calc_type(lhs), rhs, xres);
      res = xres;
      if (check_overflow && OB_SUCCESS == ret) {
        if (!within_limits(xres)) {
          ret = OB_OPERATE_OVERFLOW;
        }
      }
    }
    return ret;
  }

  // divide by native integer
  template <int check_overflow, typename T>
  static int divide(
    const ObWideInteger<Bits, Signed> &lhs, const T &rhs,
    ObWideInteger<Bits, Signed> &quotient,
    ObWideInteger<Bits, Signed> &remain)
  {
    int ret = OB_SUCCESS;
    if (check_overflow && rhs == 0) {
      ret = OB_OPERATE_OVERFLOW;
    } else if (lhs == 0) {
      quotient = 0;
      remain = lhs;
    } else {
      ObWideInteger<Bits, unsigned> numerator = make_positive(lhs);
      dw_type denominator;
      if (rhs < 0) {
        denominator = -rhs;
      } else {
        denominator = rhs;
      }
      if (numerator < denominator) {
        quotient = 0;
        remain = lhs;
      } else {
        bool r_neg = is_negative(rhs);
        bool l_neg = is_negative(lhs);
        if (denominator == 1) {
          quotient = numerator;
          remain = 0;
          if (check_overflow) {
            // min_val / -1 => overflow
            if (l_neg && l_neg == r_neg && overflow_positive_range(quotient)) {
              ret = OB_OPERATE_OVERFLOW;
            }
          }
        } else if (Bits == 128) {
          dw_type num = *(reinterpret_cast<const dw_type *>(numerator.items_));
          dw_type den = static_cast<dw_type>(denominator);
          dw_type quo = num / den;
          dw_type rem = num % den;

          quotient.items_[1] = static_cast<uint64_t>(quo >> BASE_BITS);
          quotient.items_[0] = static_cast<uint64_t>(quo);
          remain.items_[1] = static_cast<uint64_t>(rem >> BASE_BITS);
          remain.items_[0] = static_cast<uint64_t>(rem);
        } else {
          dw_type num = 0;
          dw_type den = static_cast<dw_type>(denominator);
          quotient = 0;
          for (unsigned i = 0; i < ITEM_COUNT; i++) {
            num = (num << BASE_BITS) + static_cast<dw_type>(numerator.items_[big(i)]);
            if (num < den) {
              quotient.items_[big(i)] = 0;
            } else {
              quotient.items_[big(i)] = static_cast<uint64_t>(num / den);
              num = num % den;
            }
          }
          remain = 0;
          remain.items_[0] = static_cast<uint64_t>(num);
        }
        if (r_neg != l_neg) {
            unary_minus<IgnoreOverFlow>(quotient, quotient);
        }
        if (l_neg) {
          unary_minus<IgnoreOverFlow>(remain, remain);
        }
      }
    }
    return ret;
  }

  template <int check_overflow, unsigned Bits2, typename Signed2>
  static int divide(
    const ObWideInteger<Bits, Signed> &lhs, const ObWideInteger<Bits2, Signed2> &rhs,
    ObWideInteger<Bits, Signed> &quotient,
    ObWideInteger<Bits, Signed> &remain)
  {
    int ret = OB_SUCCESS;
    if (check_overflow && rhs == 0) {
      ret = OB_OPERATE_OVERFLOW;
    } else if (lhs == 0) {
      quotient = 0;
      remain = lhs;
    } else if (Bits >= Bits2) {
      ObWideInteger<Bits, unsigned> numerator = make_positive(lhs);
      ObWideInteger<Bits, unsigned> denominator = make_positive(rhs);
      if (numerator < denominator) {
        quotient = 0;
        remain = lhs;
      } else if (denominator == 1) {
        bool lhs_neg = is_negative(lhs);
        bool rhs_neg = is_negative(rhs);
        quotient = numerator;
        remain = 0;
        if (check_overflow) {
          // min_val / -1 => overflow
          if (lhs_neg && lhs_neg == rhs_neg && overflow_positive_range(quotient)) {
            ret = OB_OPERATE_OVERFLOW;
          }
        }
        if (lhs_neg != rhs_neg) {
          unary_minus<IgnoreOverFlow>(quotient, quotient);
        }
      } else {
        int nlz_num = nlz(numerator);
        int shift_bits = nlz(denominator) % BASE_BITS;
        /* knuth division algorithm:
         *
         * we have
         *   non negative value `u[m+n-1, ...n-1...0]`(B based number) and
         *   non negative value `v[n-1...0]` (B based number)
         * while v[n-1] != 0 and n > 1 and m >= 1
         *
         * let `quo[m...0]` be quotient, `rem[n-1..0]` be remain
         *
         * Step1. [normalize]
         *   d = nlz(v[n-1]), v <<= d,  u <<= d;
         *
         * Step2.
         *   set j = m;
         *
         * Step3.
         *   qq = (u[j + n] * B + u[j + n - 1]) / v[n-1]
         *   rr =  (u[j + n] * B + u[j + n - 1]) % v[n-1]
         *   if qq == B || qq * v[n-2] > (B * rr + u[j + n - 2]) {
         *     qq -= 1;
         *     rr += v[n-1];
         *   }
         *
         * Step4.
         *   u[j+n..j] = u[j+n..j] - qq*v[n-1..0];
         *   if u[j+n..j] < 0 {
         *     u[j+n..j] += v[n-1..0];
         *     qq -= 1;
         *   }
         *   quo[j] = q;
         *
         * Step5.
         *   j = j - 1;
         *   if j >= 0, goto Step3
         *   else goto Step6
         *
         * Step6.
         *   remain = u[m+n-1..0] >> d;
         */
        if (shift_bits + BASE_BITS > nlz_num) {
          using cast_type = typename PromotedInteger<ObWideInteger<Bits, Signed>>::type;
          cast_type xquo;
          cast_type xrem;
          ret = cast_type::_impl::template divide<check_overflow>(cast_type(lhs), rhs, xquo, xrem);
          quotient = xquo;
          remain = xrem;
        } else {
          ObWideInteger<Bits, unsigned>::_impl::shift_left(numerator, numerator, shift_bits);
          ObWideInteger<Bits, unsigned>::_impl::shift_left(denominator, denominator, shift_bits);

          unsigned xlen = item_size(numerator);
          unsigned ylen = item_size(denominator);
          dw_type base = dw_type(1) << 64;
          unsigned n = ylen, m = xlen - ylen;
          ObWideInteger<Bits, Signed> tmp_ct = 0;
          ObWideInteger<Bits, Signed> tmp_cf = 0;
          ObWideInteger<Bits, unsigned> tmp_vt = 0;

          for (int j = m; j >= 0; j--) {
            dw_type tmp_value =
                static_cast<dw_type>(numerator.items_[j + n]) * base +
                static_cast<dw_type>(numerator.items_[j + n - 1]);
            dw_type qq =
                tmp_value / static_cast<dw_type>(denominator.items_[n - 1]);
            dw_type rr =
                tmp_value % static_cast<dw_type>(denominator.items_[n - 1]);
            if (qq == base || (n >= 2 &&
                              (qq * static_cast<dw_type>(denominator.items_[n - 2]) >
                               base * rr + static_cast<dw_type>(numerator.items_[j + n - 2])))) {
              qq--;
              rr += static_cast<dw_type>(denominator.items_[n - 1]);
            }
            MEMCPY(tmp_ct.items_, numerator.items_ + j, sizeof(uint64_t) * (n + 1));
            ObWideInteger<Bits, unsigned>::_impl::template multiply<IgnoreOverFlow>(
              denominator, static_cast<uint64_t>(qq), tmp_vt);
            sub<IgnoreOverFlow>(tmp_ct, ObWideInteger<Bits, Signed>(tmp_vt), tmp_cf);
            if (is_negative(tmp_cf)) {
              add<IgnoreOverFlow>(tmp_cf, ObWideInteger<Bits, Signed>(denominator), tmp_cf);
              qq--;
            }
            MEMCPY(numerator.items_ + j, tmp_cf.items_,
                   sizeof(uint64_t) * (n + 1));
            quotient.items_[j] = static_cast<uint64_t>(qq);
          }
          // calc remain
          ObWideInteger<Bits, unsigned>::_impl::shift_right(numerator, numerator, shift_bits);
          remain = numerator;
          bool lhs_neg = is_negative(lhs);
          bool rhs_neg = is_negative(rhs);
          if (lhs_neg != rhs_neg) {
            unary_minus<IgnoreOverFlow>(quotient, quotient);
          }
          if (lhs_neg) {
            unary_minus<IgnoreOverFlow>(remain, remain);
          }
        }
      }
    } else {
      using calc_type = typename CommonType<ObWideInteger<Bits>, ObWideInteger<Bits2>>::type;
      calc_type xquo, xrem;
      ret = calc_type::_impl::template divide<check_overflow>(calc_type(lhs), rhs, xquo, xrem);
      quotient = xquo;
      remain = xrem;
    }
    return ret;
  }

  template<typename T>
  static int cmp(const ObWideInteger<Bits, Signed> &lhs, const T &rhs
  )
  {
    int ret = 0;
    bool l_neg = is_negative(lhs);
    bool r_neg = is_negative(rhs);
    if (l_neg != r_neg) {
      ret = (l_neg ? -1 : 1);
    } else if (IsWideInteger<T>::value){
      ret = cmp(lhs, rhs);
    } else {
      ret = cmp(lhs, ObWideInteger<Bits, Signed>(rhs));
    }
    return ret;
  }

  template<unsigned Bits2, typename Signed2>
  static int cmp(const ObWideInteger<Bits, Signed> &lhs, const ObWideInteger<Bits2, Signed2> &rhs
  )
  {
    int ret = 0;
    bool l_neg = is_negative(lhs);
    bool r_neg = is_negative(rhs);
    if (l_neg != r_neg) {
      ret = (l_neg ? -1 : 1);
    } else if (Bits == Bits2) {
      for (unsigned i = 0; (ret == 0) && i < ITEM_COUNT; i++) {
        if (lhs.items_[big(i)] > rhs.items_[big(i)]) {
          ret = 1;
        } else if (lhs.items_[big(i)] < rhs.items_[big(i)]) {
          ret = -1;
        }
      }
    } else {
      using calc_type = typename CommonType<ObWideInteger<Bits, Signed>,
                                            ObWideInteger<Bits2, Signed2>>::type;
      ret = calc_type::_impl::cmp(calc_type(lhs), calc_type(rhs));
    }
    return ret;
  }

  template <unsigned Bits2, typename Signed2>
  static int bitwise_and(
      const ObWideInteger<Bits, Signed> &lhs,
      const ObWideInteger<Bits2, Signed2> &rhs,
      ObWideInteger<Bits, Signed> &res)
  {
    int ret = OB_SUCCESS;
    if (Bits >= Bits2) {
      constexpr const unsigned op_items = ObWideInteger<Bits2, Signed2>::ITEM_COUNT;
      constexpr const unsigned rest_items = ObWideInteger<Bits, Signed>::ITEM_COUNT;
      if ((void *)&res != (void *)&lhs) {
        res = lhs;
      }
      unsigned i = 0;
      for (; i < op_items; i++) {
        res.items_[i] &= rhs.items_[i];
      }
      if (!is_negative(rhs)) {
        // for positive integer, set all rest items to 0 
        for (; i < rest_items; i++) {
          res.items_[i] = 0;
        }
      }
    } else {
      using calc_type =
          typename CommonType<ObWideInteger<Bits, Signed>,
                              ObWideInteger<Bits2, Signed2>>::type;
      calc_type xres;
      ret = calc_type::_impl::template bitwise_and(calc_type(lhs), rhs, xres);
      res = xres;
    }
    return ret;
  }

  template <typename T>
  static int bitwise_and(
      const ObWideInteger<Bits> &self, 
      const T &rhs, 
      ObWideInteger<Bits, Signed> &res)
  {
    int ret = OB_SUCCESS;
    if (should_keep_size<T>()) {
      constexpr const unsigned r_items =
          (sizeof(T) < sizeof(uint64_t)) ? 1 : sizeof(T) / sizeof(uint64_t);
      constexpr const unsigned op_items = (r_items < ITEM_COUNT) ? r_items : ITEM_COUNT;

      if ((void *)&res != (void *)&self) {
        res = self;
      }
      unsigned i = 0;
      for (; i < op_items; i++) {
        uint64_t r_val = get_item(rhs, i);
        res.items_[i] &= r_val;
      }
      if (!is_negative(rhs)) {
        // for positive integer, set all rest items to 0 
        for (; i < ITEM_COUNT; i++) {
          res.items_[i] = 0;
        }
      }
    } else {
      using calc_type = typename CommonType<ObWideInteger<Bits, Signed>, T>::type;
      calc_type xres;
      ret = calc_type::_impl::template bitwise_and(calc_type(self), rhs, xres);
      res = xres;
    }
    return ret;
  }
};
private:
  template <unsigned Bits2, typename Signed2>
  friend class ObWideInteger;
};
}
using int128_t  = wide::ObWideInteger<128u, signed>;
using int256_t  = wide::ObWideInteger<256u, signed>;
using int512_t  = wide::ObWideInteger<512u, signed>;
using int1024_t = wide::ObWideInteger<1024u, signed>;
#define DISPATCH_WIDTH_TASK(width, task)                                                           \
  switch ((width)) {                                                                               \
  case sizeof(int32_t): {                                                                          \
    task(int32_t);                                                                                 \
    break;                                                                                         \
  }                                                                                                \
  case sizeof(int64_t): {                                                                          \
    task(int64_t);                                                                                 \
    break;                                                                                         \
  }                                                                                                \
  case sizeof(int128_t): {                                                                         \
    task(int128_t);                                                                                \
    break;                                                                                         \
  }                                                                                                \
  case sizeof(int256_t): {                                                                         \
    task(int256_t);                                                                                \
    break;                                                                                         \
  }                                                                                                \
  case sizeof(int512_t): {                                                                         \
    task(int512_t);                                                                                \
    break;                                                                                         \
  }                                                                                                \
  default: {                                                                                       \
    ret = OB_ERR_UNEXPECTED;                                                                       \
    COMMON_LOG(WARN, "invalid int bytes", K((width)));                                             \
  }                                                                                                \
  }

#define DISPATCH_INOUT_WIDTH_TASK(in_width, out_width, task)                                       \
  switch ((in_width)) {                                                                            \
  case sizeof(int32_t): {                                                                          \
    switch ((out_width)) {                                                                         \
    case sizeof(int32_t): {                                                                        \
      task(int32_t, int32_t);                                                                      \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int64_t): {                                                                        \
      task(int32_t, int64_t);                                                                      \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int128_t): {                                                                       \
      task(int32_t, int128_t);                                                                     \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int256_t): {                                                                       \
      task(int32_t, int256_t);                                                                     \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int512_t): {                                                                       \
      task(int32_t, int512_t);                                                                     \
      break;                                                                                       \
    }                                                                                              \
    default: {                                                                                     \
      ret = OB_ERR_UNEXPECTED;                                                                     \
      COMMON_LOG(WARN, "invalid int bytes", K((out_width)));                                       \
      break;                                                                                       \
    }                                                                                              \
    }                                                                                              \
    break;                                                                                         \
  }                                                                                                \
  case sizeof(int64_t): {                                                                          \
    switch ((out_width)) {                                                                         \
    case sizeof(int32_t): {                                                                        \
      task(int64_t, int32_t);                                                                      \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int64_t): {                                                                        \
      task(int64_t, int64_t);                                                                      \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int128_t): {                                                                       \
      task(int64_t, int128_t);                                                                     \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int256_t): {                                                                       \
      task(int64_t, int256_t);                                                                     \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int512_t): {                                                                       \
      task(int64_t, int512_t);                                                                     \
      break;                                                                                       \
    }                                                                                              \
    default: {                                                                                     \
      ret = OB_ERR_UNEXPECTED;                                                                     \
      COMMON_LOG(WARN, "invalid int bytes", K((out_width)));                                       \
      break;                                                                                       \
    }                                                                                              \
    }                                                                                              \
    break;                                                                                         \
  }                                                                                                \
  case sizeof(int128_t): {                                                                         \
    switch ((out_width)) {                                                                         \
    case sizeof(int32_t): {                                                                        \
      task(int128_t, int32_t);                                                                     \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int64_t): {                                                                        \
      task(int128_t, int64_t);                                                                     \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int128_t): {                                                                       \
      task(int128_t, int128_t);                                                                    \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int256_t): {                                                                       \
      task(int128_t, int256_t);                                                                    \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int512_t): {                                                                       \
      task(int128_t, int512_t);                                                                    \
      break;                                                                                       \
    }                                                                                              \
    default: {                                                                                     \
      ret = OB_ERR_UNEXPECTED;                                                                     \
      COMMON_LOG(WARN, "invalid int bytes", K((out_width)));                                       \
      break;                                                                                       \
    }                                                                                              \
    }                                                                                              \
    break;                                                                                         \
  }                                                                                                \
  case sizeof(int256_t): {                                                                         \
    switch ((out_width)) {                                                                         \
    case sizeof(int32_t): {                                                                        \
      task(int256_t, int32_t);                                                                     \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int64_t): {                                                                        \
      task(int256_t, int64_t);                                                                     \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int128_t): {                                                                       \
      task(int256_t, int128_t);                                                                    \
    }                                                                                              \
    case sizeof(int256_t): {                                                                       \
      task(int256_t, int256_t);                                                                    \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int512_t): {                                                                       \
      task(int256_t, int512_t);                                                                    \
      break;                                                                                       \
    }                                                                                              \
    default: {                                                                                     \
      ret = OB_ERR_UNEXPECTED;                                                                     \
      COMMON_LOG(WARN, "invalid int bytes", K((out_width)));                                       \
      break;                                                                                       \
    }                                                                                              \
    }                                                                                              \
    break;                                                                                         \
  }                                                                                                \
  case sizeof(int512_t): {                                                                         \
    switch ((out_width)) {                                                                         \
    case sizeof(int32_t): {                                                                        \
      task(int512_t, int32_t);                                                                     \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int64_t): {                                                                        \
      task(int512_t, int64_t);                                                                     \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int128_t): {                                                                       \
      task(int512_t, int128_t);                                                                    \
    }                                                                                              \
    case sizeof(int256_t): {                                                                       \
      task(int512_t, int256_t);                                                                    \
      break;                                                                                       \
    }                                                                                              \
    case sizeof(int512_t): {                                                                       \
      task(int512_t, int512_t);                                                                    \
      break;                                                                                       \
    }                                                                                              \
    default: {                                                                                     \
      ret = OB_ERR_UNEXPECTED;                                                                     \
      COMMON_LOG(WARN, "invalid int bytes", K((out_width)));                                       \
      break;                                                                                       \
    }                                                                                              \
    }                                                                                              \
    break;                                                                                         \
  }                                                                                                \
  default: {                                                                                       \
    ret = OB_ERR_UNEXPECTED;                                                                       \
    COMMON_LOG(WARN, "invalid int bytes", K((in_width)));                                          \
  }                                                                                                \
  }

struct ObDecimalInt
{
  union
  {
    int32_t int32_v_[0];
    int64_t int64_v_[0];
    int128_t int128_v_[0];
    int256_t int256_v_[0];
    int512_t int512_v_[0];
  };
};

// helper struct, used to store data for algorithm operations
struct ObDecimalIntBuilder: public common::ObDataBuffer
{
  ObDecimalIntBuilder(): ObDataBuffer(buffer_, sizeof(buffer_)) {}
  template<typename T>
  inline void from(const T &v)
  {
    static_assert(sizeof(T) <= sizeof(int512_t), "");
    int_bytes_ = sizeof(T);
    MEMCPY(buffer_, v.items_, sizeof(T));
  }

  template<unsigned Bits, typename Signed>
  inline void from(const wide::ObWideInteger<Bits, Signed> &v, const int32_t copy_size)
  {
    static_assert(Bits <= 512, "");
    int_bytes_ = MIN(copy_size, sizeof(v));
    MEMCPY(buffer_, v.items_, int_bytes_);
  }

  inline void from(int32_t v)
  {
    int_bytes_ = sizeof(int32_t);
    MEMCPY(buffer_, &v, int_bytes_);
  }

  inline void from(int64_t v)
  {
    int_bytes_ = sizeof(int64_t);
    MEMCPY(buffer_, &v, int_bytes_);
  }

  inline void from(const ObDecimalIntBuilder &other)
  {
    int_bytes_ = other.get_int_bytes();
    MEMCPY(buffer_, other.get_decimal_int(), (size_t) int_bytes_);
  }

  inline void from(const ObDecimalInt *decint, int32_t int_bytes)
  {
    OB_ASSERT(decint != NULL);
    int_bytes_ = MIN((int32_t) sizeof(buffer_), int_bytes);
    MEMCPY(buffer_, decint, (size_t) int_bytes_);
  }

  inline void build(ObDecimalInt *&decint, int32_t &int_bytes)
  {
    decint = reinterpret_cast<ObDecimalInt*>(buffer_);
    int_bytes = int_bytes_;
  }

  inline void build(const ObDecimalInt *&decint, int32_t &int_bytes)
  {
    decint = reinterpret_cast<const ObDecimalInt*>(buffer_);
    int_bytes = int_bytes_;
  }

  inline int32_t get_int_bytes() const
  {
    return int_bytes_;
  }

  inline const ObDecimalInt *get_decimal_int() const
  {
    return reinterpret_cast<const ObDecimalInt *>(buffer_);
  }

  inline void truncate(const int32_t expected_int_bytes)
  {
    if (expected_int_bytes >= int_bytes_) {
      // do nothing
    } else {
      int_bytes_ = expected_int_bytes;
    }
  }

  inline void extend(const int32_t expected_int_bytes)
  {
    bool is_neg = false;
    if (int_bytes_ > 0) {
      is_neg = (buffer_[int_bytes_- 1] & 0x80) > 0;
    }
    int32_t extending = expected_int_bytes - int_bytes_;
    if (extending <= 0) {
      // do nothing
    } else if (is_neg) {
      MEMSET(buffer_ + int_bytes_, 0xFF, extending);
      int_bytes_ = expected_int_bytes;
    } else {
      MEMSET(buffer_ + int_bytes_, 0, extending);
      int_bytes_ = expected_int_bytes;
    }
  }

  inline void set_zero(const int32_t int_bytes)
  {
    int_bytes_ = int_bytes;
    MEMSET(buffer_, 0, int_bytes);
  }

  inline char *get_buffer()
  {
    return buffer_;
  }
private:
  int32_t int_bytes_;
  char buffer_[sizeof(int512_t)];
};

namespace wide
{
// IsWideInteger
template <typename T>
struct IsWideInteger
{
  constexpr static const bool value = false;
};

template <unsigned Bits, typename Signed>
struct IsWideInteger<ObWideInteger<Bits, Signed>>
{
  constexpr static const bool value = true;
};

template <typename T>
struct IsIntegral
{
  constexpr static const bool value = std::is_integral<T>::value;
};

template <unsigned Bits, typename Signed>
struct IsIntegral<ObWideInteger<Bits, Signed>>
{
  constexpr static const bool value = true;
};

template<typename T>
struct SignedConcept
{
  constexpr static const bool value = std::is_signed<T>::value;
  using type = typename std::conditional<std::is_signed<T>::value, signed, unsigned>::type;
};

template<unsigned Bits, typename Signed>
struct SignedConcept<ObWideInteger<Bits, Signed>>
{
  constexpr static const bool value = std::is_same<Signed, signed>::value;
  using type = signed;
};

template<unsigned Bits, unsigned Bits2>
struct BiggerBits
{
  static unsigned constexpr value()
  {
    return (Bits > Bits2 ? Bits : Bits2);
  }
};

template <typename T>
struct Limits
{
  static_assert(IsIntegral<T>::value, "");
  static T min()
  {
    return std::numeric_limits<T>::min();
  }
  static T max()
  {
    return std::numeric_limits<T>::max();
  }
};

template<unsigned Bits, typename Signed>
struct Limits<ObWideInteger<Bits, Signed>>
{
  static  ObWideInteger<Bits, Signed> min()
  {
    const unsigned constexpr item_count = ObWideInteger<Bits, Signed>::ITEM_COUNT;
    ObWideInteger<Bits, Signed> res;
    if (std::is_same<Signed, signed>::value) {
      res.items_[item_count - 1] = std::numeric_limits<int64_t>::min();
    }
    return res;
  }
  static  ObWideInteger<Bits, Signed> max()
  {
    const unsigned constexpr item_count = ObWideInteger<Bits, Signed>::ITEM_COUNT;
    ObWideInteger<Bits, Signed> res;
    if (std::is_same<Signed, signed>::value) {
      res.items_[item_count - 1] = std::numeric_limits<int64_t>::max();
    } else {
      res.items_[item_count - 1] = std::numeric_limits<uint64_t>::max();
    }
    for (unsigned i = 0; i < item_count - 1; i++) {
      res.items_[i] = std::numeric_limits<uint64_t>::max();
    }
    return res;
  }
};

#define SPECIALIZE_NATIVE_LIMITS(int_type, Signed)                                                 \
  template <>                                                                                      \
  struct Limits<int_type>                                                                          \
  {                                                                                                \
    static const int_type Signed##_min_v;                                                          \
    static const int_type Signed##_max_v;                                                          \
    static int_type min()                                                                          \
    {                                                                                              \
      return Signed##_min_v;                                                                       \
    }                                                                                              \
    static int_type max()                                                                          \
    {                                                                                              \
      return Signed##_max_v;                                                                       \
    }                                                                                              \
  }

#define SPECIALIZE_LIMITS(B, Signed)                                                               \
  template <>                                                                                      \
  struct Limits<ObWideInteger<B, Signed>>                                                          \
  {                                                                                                \
    static const ObWideInteger<B, Signed> Signed##_min_v;                                          \
    static const ObWideInteger<B, Signed> Signed##_max_v;                                          \
    static ObWideInteger<B, Signed> min()                                                          \
    {                                                                                              \
      return Signed##_min_v;                                                                       \
    }                                                                                              \
    static ObWideInteger<B, Signed> max()                                                          \
    {                                                                                              \
      return Signed##_max_v;                                                                       \
    }                                                                                              \
  }

SPECIALIZE_LIMITS(128, signed);
SPECIALIZE_LIMITS(128, unsigned);
SPECIALIZE_LIMITS(256, signed);
SPECIALIZE_LIMITS(256, unsigned);
SPECIALIZE_LIMITS(512, signed);
SPECIALIZE_LIMITS(512, unsigned);
SPECIALIZE_LIMITS(1024, signed);
SPECIALIZE_LIMITS(1024, unsigned);

SPECIALIZE_NATIVE_LIMITS(int32_t, signed);
SPECIALIZE_NATIVE_LIMITS(int64_t, signed);

#undef SPECIALIZE_LIMITS

template<typename T1, typename T2>
struct CommonType
{
  template<bool B, class T, class F>
  using conditional_t = typename std::conditional<B, T, F>::type;

  using type =
      conditional_t<(sizeof(T1) < sizeof(T2)), T2,
                    conditional_t<(sizeof(T1) > sizeof(T2)), T1,
                                  conditional_t<(SignedConcept<T1>::value ||
                                                !SignedConcept<T2>::value),
                                                T2, T1>>>;
};

template<typename Integer>
struct PromotedInteger {};

template<>
struct PromotedInteger<int32_t>
{
  using type = int64_t;
};

template<>
struct PromotedInteger<int64_t>
{
  using type = ObWideInteger<128, signed>;
};

template <unsigned Bits, typename Signed>
struct PromotedInteger<ObWideInteger<Bits, Signed>> {
  static const constexpr unsigned max_bits = (1U<<10);
  using type = typename std::conditional<(Bits < max_bits / 2),
                                         ObWideInteger<Bits * 2, Signed>,
                                         ObWideInteger<max_bits, Signed>>::type;
};

class ObDecimalIntConstValue
{
public:
  // for mysql mode
  static const int512_t MYSQL_DEC_INT_MIN;
  static const int512_t MYSQL_DEC_INT_MAX;
  static const int512_t MYSQL_DEC_INT_MAX_AVAILABLE;
  static const int16_t MAX_ORACLE_SCALE_DELTA = 0 - number::ObNumber::MIN_SCALE;
  static const int16_t MAX_ORACLE_SCALE_SIZE =
    number::ObNumber::MAX_SCALE - number::ObNumber::MIN_SCALE;
  static int init_const_values();
  inline static int16_t oracle_delta_scale(const int16_t in_scale)
  {
    return (int16_t) (in_scale + MAX_ORACLE_SCALE_DELTA);
  }
  inline static int get_int_bytes_by_precision(int16_t precision)
  {
    if (precision <= 0) {
      return 0;
    } else if (precision <= MAX_PRECISION_DECIMAL_INT_32) {
      return sizeof(int32_t);
    } else if (precision <= MAX_PRECISION_DECIMAL_INT_64) {
      return sizeof(int64_t);
    } else if (precision <= MAX_PRECISION_DECIMAL_INT_128) {
      return sizeof(int128_t);
    } else if (precision <= MAX_PRECISION_DECIMAL_INT_256) {
      return sizeof(int256_t);
    } else {
      return sizeof(int512_t);
    }
  }

  inline static int get_max_precision_by_int_bytes(int int_bytes)
  {
    if (int_bytes <= 0) {
      return 0;
    } else if (int_bytes <= sizeof(int32_t)) {
      return MAX_PRECISION_DECIMAL_INT_32;
    } else if (int_bytes <= sizeof(int64_t)) {
      return MAX_PRECISION_DECIMAL_INT_64;
    } else if (int_bytes <= sizeof(int128_t)) {
      return MAX_PRECISION_DECIMAL_INT_128;
    } else if (int_bytes <= sizeof(int256_t)) {
      return MAX_PRECISION_DECIMAL_INT_256;
    } else {
      return MAX_PRECISION_DECIMAL_INT_512;
    }
  }

  inline static const ObDecimalInt* get_max_upper(ObPrecision prec)
  {
    OB_ASSERT(prec <= OB_MAX_DECIMAL_POSSIBLE_PRECISION);
    return MAX_UPPER[prec];
  }

  inline static const ObDecimalInt* get_min_lower(ObPrecision prec)
  {
    OB_ASSERT(prec <= OB_MAX_DECIMAL_POSSIBLE_PRECISION);
    return MIN_LOWER[prec];
  }
  inline static const ObDecimalInt* get_max_value(ObPrecision prec)
  {
    OB_ASSERT(prec <= OB_MAX_DECIMAL_POSSIBLE_PRECISION);
    return MAX_DECINT[prec];
  }

  inline static const ObDecimalInt* get_min_value(ObPrecision prec)
  {
    OB_ASSERT(prec <= OB_MAX_DECIMAL_POSSIBLE_PRECISION);
    return MIN_DECINT[prec];
  }
private:
  // MYSQL_MIN
  static const ObDecimalInt *MIN_DECINT[OB_MAX_DECIMAL_POSSIBLE_PRECISION + 1];

  // MYSQL_MAX
  static const ObDecimalInt *MAX_DECINT[OB_MAX_DECIMAL_POSSIBLE_PRECISION + 1];
  // MYSQL_MAX_UPPER
  static const ObDecimalInt *MAX_UPPER[OB_MAX_DECIMAL_POSSIBLE_PRECISION + 1];
  // MYSQL_MIN_LOWER
  static const ObDecimalInt *MIN_LOWER[OB_MAX_DECIMAL_POSSIBLE_PRECISION + 1];
};

// helper functions
inline bool is_negative(const ObDecimalInt *decint, int32_t int_bytes)
{
  switch (int_bytes) {
  case sizeof(int32_t): return (*(decint->int32_v_)) < 0;
  case sizeof(int64_t): return (*(decint->int64_v_)) < 0;
  case sizeof(int128_t): return decint->int128_v_->is_negative();
  case sizeof(int256_t): return decint->int256_v_->is_negative();
  case sizeof(int512_t): return decint->int512_v_->is_negative();
  default: return false;
  }
}
template <typename T>
int scale_up_decimalint(const T &x, unsigned scale, ObDecimalIntBuilder &res)
{
  static const int64_t pows[5] = {10, 100, 10000, 100000000, 10000000000000000};
  // here we use int512 to store tmp result, because scale up may overflow, and then we need copy
  // current result to bigger integer, and then continue calculating. use int512 to avoid copy and
  // complicated overflow calculating
  int ret = OB_SUCCESS;
  int512_t result = x;
  while (scale != 0 && result != 0) {
    for (int i = ARRAYSIZEOF(pows) - 1; scale != 0 && i >= 0; i--) {
      if (scale & (1 << i)) {
        result = result * pows[i]; // this may also overflow, ignore it
        scale -= (1<<i);
      }
    }
    if (scale != 0) {
      result = result * 10;
      scale -= 1;
    }
  }
  if (result <= wide::Limits<int32_t>::max() && result >= wide::Limits<int32_t>::min()) {
    res.from(result, sizeof(int32_t));
  } else if (result <= wide::Limits<int64_t>::max() && result >= wide::Limits<int64_t>::min()) {
    res.from(result, sizeof(int64_t));
  } else if (result <= wide::Limits<int128_t>::max() && result >= wide::Limits<int128_t>::min()) {
    res.from(result, sizeof(int128_t));
  } else if (result <= wide::Limits<int256_t>::max() && result >= wide::Limits<int256_t>::min()) {
    res.from(result, sizeof(int256_t));
  } else {
    res.from(result);
  }
  return ret;
}

template <typename T>
int scale_down_decimalint_for_round(const T &x, unsigned scale, ObDecimalIntBuilder &res)
{
  static const int64_t pows[5] = {10, 100, 10000, 100000000, 10000000000000000};
  static const int64_t carry_threshholds[5] = {5, 50, 5000, 50000000, 5000000000000000};
  int ret = OB_SUCCESS;
  T result = x;
  if (x < 0) { result = -result; }
  T remain;
  bool last_carry = false;
  while (scale != 0 && result != 0) {
    for (int i = ARRAYSIZEOF(pows) - 1; scale != 0 && result != 0 && i >= 0; i--) {
      if (scale & (1 << i)) {
        remain = result % pows[i];
        result = result / pows[i];
        last_carry = (remain >= carry_threshholds[i]);
        scale -= (1 << i);
      }
    }
    if (scale != 0) {
      remain = result % 10;
      result = result / 10;
      last_carry = (remain >= 5);
      scale -= 1;
    }
  }
  if (scale != 0) { last_carry = false; }
  if (last_carry) { result++; }
  if (x < 0) { result = -result; }
  res.from(result);
  return ret;
}

template <typename T>
int scale_down_decimalint_for_trunc(const T &x, unsigned scale, ObDecimalIntBuilder &res)
{
  static const int64_t pows[5] = {10, 100, 10000, 100000000, 10000000000000000};
  T result = x;
  if (x < 0) { result = -result; }
  while (scale != 0 && result != 0) {
    for (int i = ARRAYSIZEOF(pows) - 1; scale != 0 && result != 0 && i >= 0; i--) {
      if (scale & (1 << i)) {
        result = result / pows[i];
        scale -= (1 << i);
      }
    }
    if (scale != 0) {
      result = result / 10;
      scale -= 1;
    }
  }
  if (x < 0) { result = -result; }
  res.from(result);
  return OB_SUCCESS;
}

template <typename T>
inline int scale_down_decimalint(const T &x, unsigned scale, const bool is_trunc, ObDecimalIntBuilder &res)
{
  if (is_trunc) {
    scale_down_decimalint_for_trunc(x, scale, res);
  } else {
    scale_down_decimalint_for_round(x, scale, res);
  }

  return OB_SUCCESS;
}

int common_scale_decimalint(const ObDecimalInt *decint, const int32_t int_bytes,
                            const ObScale in_scale, const ObScale out_scale,
                            ObDecimalIntBuilder &res, const bool is_trunc = false);

int check_range_valid_int64(
    const ObDecimalInt *decint, const int32_t int_bytes,
    bool &is_valid_int64, int64_t &res_val); // scale is regarded as 0

int check_range_valid_uint64(
    const ObDecimalInt *decint, const int32_t int_bytes,
    bool &is_valid_uint64, uint64_t &res_val); // scale is regarded as 0

// unary operators
template<unsigned Bits, typename Signed, typename T>
bool operator==(const ObWideInteger<Bits, Signed> &lhs, const T &rhs)
{
  return ObWideInteger<Bits, Signed>::_impl::cmp(lhs, rhs) == 0;
}

template<unsigned Bits, typename Signed, typename T>
bool operator>(const ObWideInteger<Bits, Signed> &lhs, const T &rhs)
{
  return ObWideInteger<Bits, Signed>::_impl::cmp(lhs, rhs) > 0;
}

template<unsigned Bits, typename Signed, typename T>
bool operator<(const ObWideInteger<Bits, Signed> &lhs, const T &rhs)
{
  return ObWideInteger<Bits, Signed>::_impl::cmp(lhs, rhs) < 0;
}

template<unsigned Bits, typename Signed, typename T>
bool operator!=(const ObWideInteger<Bits, Signed> &lhs, const T &rhs)
{
  return ObWideInteger<Bits, Signed>::_impl::cmp(lhs, rhs) != 0;
}

template<unsigned Bits, typename Signed, typename T>
bool operator<=(const ObWideInteger<Bits, Signed> &lhs, const T &rhs)
{
  int ret = ObWideInteger<Bits, Signed>::_impl::cmp(lhs, rhs);
  return ret <= 0;
}

template<unsigned Bits, typename Signed, typename T>
bool operator>=(const ObWideInteger<Bits, Signed> &lhs, const T &rhs)
{
  int ret = ObWideInteger<Bits, Signed>::_impl::cmp(lhs, rhs);
  return ret >= 0;
}

template<unsigned Bits, typename Signed, typename T>
ObWideInteger<Bits, Signed> operator+(const ObWideInteger<Bits, Signed> &lhs, const T &rhs)
{
  ObWideInteger<Bits, Signed> res;
  if (ObWideInteger<Bits, Signed>::_impl::is_negative(rhs)) {
    ObWideInteger<Bits, Signed>::_impl::template sub<IgnoreOverFlow>(lhs, -rhs, res);
  } else {
    ObWideInteger<Bits, Signed>::_impl::template add<IgnoreOverFlow>(lhs, rhs, res);
  }
  return res;
}

template<unsigned Bits, typename Signed, typename T>
ObWideInteger<Bits, Signed> operator-(const ObWideInteger<Bits, Signed> &lhs, const T &rhs)
{
  ObWideInteger<Bits, Signed> res;
  if (ObWideInteger<Bits, Signed>::_impl::is_negative(rhs)) {
    ObWideInteger<Bits, Signed>::_impl::template add<IgnoreOverFlow>(lhs, -rhs, res);
  } else {
    ObWideInteger<Bits, Signed>::_impl::template sub<IgnoreOverFlow>(lhs, rhs, res);
  }
  return res;
}

template<unsigned Bits, typename Signed, typename T>
ObWideInteger<Bits, Signed> operator*(const ObWideInteger<Bits, Signed> &lhs, const T &rhs)
{
  ObWideInteger<Bits, Signed> res;
  ObWideInteger<Bits, Signed>::_impl::template multiply<IgnoreOverFlow>(lhs, rhs, res);
  return res;
}

template<unsigned Bits, typename Signed, typename T>
ObWideInteger<Bits, Signed> operator/(const ObWideInteger<Bits, Signed> &lhs, const T &rhs)
{
  ObWideInteger<Bits, Signed> res;
  ObWideInteger<Bits, Signed> rem;
  ObWideInteger<Bits, Signed>::_impl::template divide<IgnoreOverFlow>(lhs, rhs, res, rem);
  return res;
}

template<unsigned Bits, typename Signed, typename T>
ObWideInteger<Bits, Signed> operator%(const ObWideInteger<Bits, Signed> &lhs, const T &rhs)
{
  ObWideInteger<Bits, Signed> res;
  ObWideInteger<Bits, Signed> rem;
  ObWideInteger<Bits, Signed>::_impl::template divide<IgnoreOverFlow>(lhs, rhs, res, rem);
  return rem;
}

template<unsigned Bits, typename Signed, typename T>
ObWideInteger<Bits, Signed> operator&(const ObWideInteger<Bits, Signed> &lhs, const T &rhs)
{
  ObWideInteger<Bits, Signed> res;
  ObWideInteger<Bits, Signed>::_impl::template bitwise_and(lhs, rhs, res);
  return res;
}

template<unsigned Bits, typename Signed>
ObWideInteger<Bits, Signed> operator>>(const ObWideInteger<Bits, Signed> &lhs, int n)
{
  ObWideInteger<Bits, Signed> res;
  ObWideInteger<Bits, Signed>::_impl::shift_right(lhs, res, static_cast<unsigned>(n));
  return res;
}

template<unsigned Bits, typename Signed>
ObWideInteger<Bits, Signed> operator<<(const ObWideInteger<Bits, Signed> &lhs, int n)
{
  ObWideInteger<Bits, Signed> res;
  ObWideInteger<Bits, Signed>::_impl::shift_left(lhs, res, static_cast<unsigned>(n));
  return res;
}

// unary operators
template<unsigned Bits, typename Signed>
ObWideInteger<Bits, Signed> operator-(const ObWideInteger<Bits, Signed> &x)
{
  ObWideInteger<Bits, Signed> res;
  ObWideInteger<Bits, Signed>::_impl::template unary_minus<IgnoreOverFlow>(x, res);
  return res;
}
template <typename Allocator>
int from_number(const number::ObNumber &nmb, Allocator &allocator, const int16_t scale,
                ObDecimalInt *&decint, int32_t &int_bytes)
{
  static const int64_t pows[5] = {
    10,
    100,
    10000,
    100000000,
    10000000000000000,
  };
  int ret = OB_SUCCESS;
  int512_t res = 0;
  bool is_neg = nmb.is_negative();
  int64_t in_scale = nmb.get_scale();
  uint32_t nmb_base = number::ObNumber::BASE;
  uint32_t *digits = nmb.get_digits();
  uint32_t last_digit = 0, last_base = nmb_base;
  void *cp_data = nullptr;
  if (nmb.is_zero()) {
    if (OB_ISNULL(decint = (ObDecimalInt *)allocator.alloc(sizeof(int32_t)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "allocate memory failed", K(ret));
    } else {
      *(reinterpret_cast<int32_t *>(decint)) = 0;
      int_bytes = sizeof(int32_t);
    }
  } else {
    if (OB_ISNULL(digits)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "invalid null digits");
    } else {
      last_digit = digits[nmb.get_length() - 1];
      if (in_scale > 0) { // remove trailing zeros for decimal part
        while (last_digit != 0 && last_digit % 10 == 0) { last_digit /= 10; }
        int cnt = (int) (in_scale % number::ObNumber::DIGIT_LEN);
        if (cnt != 0) { last_base = 1; }
        for (int i = 0; i < cnt; i++) { last_base *= 10; }
      }
    }
    for (int i = 0; OB_SUCC(ret) && i < nmb.get_length(); i++) {
      if (i == nmb.get_length() - 1) {
        res = res * last_base + last_digit;
      } else {
        res = res * nmb_base + digits[i];
      }
    }
    if (in_scale <= 0) {
      ObNumberDesc desc = static_cast<ObNumberDesc>(nmb.get_number_desc());
      int32 nmb_exp = (number::ObNumber::POSITIVE == desc.sign_ ?
                         (desc.se_ - number::ObNumber::POSITIVE_EXP_BOUNDARY) :
                         (number::ObNumber::NEGATIVE_EXP_BOUNDARY - desc.se_));
      for (int i = 0; i < nmb_exp - desc.len_ + 1; i++) {
        res = res * nmb_base;
      }
    }
    if (OB_SUCC(ret)) {
      if (in_scale > scale) {
        int64_t scale_down = in_scale - scale;
        while (scale_down > 0 && res != 0) {
          for (int i = ARRAYSIZEOF(pows) - 1; scale_down > 0 && res != 0 && i >= 0; i--) {
            if (scale_down & (1 << i)) {
              res = res / pows[i];
              scale_down -= (1 << i);
            }
          } // for end
          if (scale_down > 0) {
            scale_down -= 1;
            res = res / 10;
          }
        }
      } else if (in_scale < scale) {
        int64_t scale_up = scale - in_scale;
        while (scale_up > 0 && res != 0) {
          for (int i = ARRAYSIZEOF(pows) - 1; scale_up > 0 && res != 0 && i >= 0; i--) {
            if (scale_up & (1 << i)) {
              res = res * pows[i];
              scale_up -= (1 << i);
            }
          } // for end
          if (scale_up > 0) {
            scale_up -= 1;
            res = res * 10;
          }
        }
      }
      if (is_neg) { res = -res; }

      ObDecimalIntBuilder bld;
      if (res <= wide::Limits<int32_t>::max() && res >= wide::Limits<int32_t>::min()) {
        int_bytes = sizeof(int32_t);
      } else if (res <= wide::Limits<int64_t>::max() && res >= wide::Limits<int64_t>::min()) {
        int_bytes = sizeof(int64_t);
      } else if (res <= wide::Limits<int128_t>::max() && res >= wide::Limits<int128_t>::min()) {
        int_bytes = sizeof(int128_t);
      } else if (res <= wide::Limits<int256_t>::max() && res >= wide::Limits<int256_t>::min()) {
        int_bytes = sizeof(int256_t);
      } else {
        int_bytes = sizeof(int512_t);
      }
      if (OB_ISNULL(cp_data = allocator.alloc(int_bytes))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(WARN, "allocate memory failed", K(ret));
      } else {
        MEMCPY(cp_data, res.items_, int_bytes);
        decint = reinterpret_cast<ObDecimalInt *>(cp_data);
      }
    }
  }
  return ret;
}

template <unsigned Bits, typename Signed, typename Allocator>
int to_number(const ObWideInteger<Bits, Signed> &x, int16_t scale, Allocator &allocator,
              number::ObNumber &numb)
{
  int ret = OB_SUCCESS;
  static const uint64_t pows[] = {
    10,
    100,
    1000,
    10000,
    100000,
    1000000,
    10000000,
    100000000,
    1000000000,
  };
  bool is_neg = ObWideInteger<Bits, Signed>::_impl::is_negative(x);
  ObWideInteger<Bits, unsigned> numerator = ObWideInteger<Bits, unsigned>::_impl::make_positive(x);
  ObWideInteger<Bits, unsigned> quo;
  ObWideInteger<Bits, unsigned> rem;
  uint32_t digits[number::ObNumber::MAX_STORE_LEN];
  int idx = number::ObNumber::MAX_STORE_LEN;
  uint32_t nmb_base = number::ObNumber::BASE;
  uint8_t exp, digit_len;
  if (scale >= 0) {
    int64_t calc_exp = -1;
    if ((scale % number::ObNumber::DIGIT_LEN) != 0) {
      int rem_scale = static_cast<int>(scale % (number::ObNumber::DIGIT_LEN));
      ObWideInteger<Bits, unsigned>::_impl::template divide<IgnoreOverFlow>(
        numerator, pows[rem_scale - 1], quo, rem);
      digits[--idx] = static_cast<uint32_t>(
        static_cast<uint32_t>(rem.items_[0]) * pows[number::ObNumber::DIGIT_LEN - rem_scale - 1]);
      numerator = quo;
      scale = static_cast<int16_t>(scale - rem_scale);
    }
    for (; OB_SUCC(ret) && scale > 0 && numerator != 0;) {
      if (OB_UNLIKELY(idx <= 0)) {
        ret = OB_ERROR_OUT_OF_RANGE;
        COMMON_LOG(WARN, "out of number range", K(ret));
      } else {
        ObWideInteger<Bits, unsigned>::_impl::template divide<IgnoreOverFlow>(numerator, nmb_base,
                                                                              quo, rem);
        digits[--idx] = (uint32_t)rem.items_[0];
        numerator = quo;
        scale = static_cast<int16_t>(scale - number::ObNumber::DIGIT_LEN);
      }
    }
    while (OB_SUCC(ret) && scale > 0) {
      scale = static_cast<int16_t>(scale - number::ObNumber::DIGIT_LEN);
      calc_exp--;
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (numerator == 0) {
      exp = 0x7F & (uint8_t)(number::ObNumber::EXP_ZERO + calc_exp);
      digit_len = (uint8_t)(number::ObNumber::MAX_STORE_LEN - idx);
    } else {
      calc_exp = -1;
      for (; OB_SUCC(ret) && numerator != 0;) {
        if (OB_UNLIKELY(idx <= 0)) {
          ret = OB_ERROR_OUT_OF_RANGE;
          COMMON_LOG(WARN, "out of number range", K(ret));
        } else {
          ObWideInteger<Bits, unsigned>::_impl::template divide<IgnoreOverFlow>(numerator, nmb_base,
                                                                                quo, rem);
          digits[--idx] = (uint32_t)rem.items_[0];
          numerator = quo;
          calc_exp++;
        }
      }
      if(OB_SUCC(ret)) {
        exp = 0x7F & (uint8_t)(number::ObNumber::EXP_ZERO + calc_exp);
        digit_len = (uint8_t)(number::ObNumber::MAX_STORE_LEN - idx);
      }
    }
  } else {
    int64_t calc_exp = 0;
    scale = static_cast<int16_t>(-scale);
    for (; scale >= number::ObNumber::DIGIT_LEN;) {
      calc_exp += 1;
      scale = static_cast<int16_t>(scale - number::ObNumber::DIGIT_LEN);
    }
    if (scale > 0) {
      ObWideInteger<Bits, unsigned>::_impl::template divide<IgnoreOverFlow>(
        numerator, pows[number::ObNumber::DIGIT_LEN - scale - 1], quo, rem);
      digits[--idx] = (uint32_t)rem.items_[0];
      numerator = quo;
      calc_exp++;
    }
    while (OB_SUCC(ret) && numerator != 0) {
      if (OB_UNLIKELY(idx <= 0)) {
        ret = OB_ERROR_OUT_OF_RANGE;
        COMMON_LOG(WARN, "out of number range", K(ret));
      } else {
        ObWideInteger<Bits, unsigned>::_impl::template divide<IgnoreOverFlow>(numerator, nmb_base,
                                                                              quo, rem);
        digits[--idx] = (uint32_t)rem.items_[0];
        numerator = quo;
        calc_exp++;
      }
    }
    if (OB_SUCC(ret)) {
      exp = 0x7F & (uint8_t)(number::ObNumber::EXP_ZERO + calc_exp);
      digit_len = (uint8_t)(number::ObNumber::MAX_STORE_LEN - idx);
    }
  }
  if (OB_SUCC(ret)) {
    ObNumberDesc desc;
    desc.len_ = digit_len;
    desc.exp_ = exp;
    desc.sign_ = is_neg ? number::ObNumber::NEGATIVE : number::ObNumber::POSITIVE;
    if (is_neg) {
      desc.exp_ = 0x7F & (~desc.exp_);
      desc.exp_++;
    }
    uint32_t *copy_digits = nullptr;
    if (digit_len > 0) {
      if (OB_ISNULL(copy_digits = (uint32_t *)allocator.alloc(digit_len * sizeof(uint32_t)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(WARN, "allocate memory failed", K(ret));
      } else {
        numb.assign(desc.desc_, copy_digits);
        if (OB_FAIL(numb.normalize_(digits + idx, digit_len))) {
          COMMON_LOG(WARN, "normalize number failed", K(ret));
        }
      }
    } else if (digit_len == 0) {
      numb.set_zero();
    }
  }
  return ret;
}

template<typename Allocator>
int to_number(const int64_t v, int16_t scale, Allocator &allocator, number::ObNumber &nmb)
{
  int ret = OB_SUCCESS;
  static const uint64_t pows[] = {
    10,
    100,
    1000,
    10000,
    100000,
    1000000,
    10000000,
    100000000,
    1000000000,
  };
  bool is_neg = (v < 0);
  int64_t numerator = std::abs(v);
  uint32_t digits[number::ObNumber::MAX_STORE_LEN];
  int idx = number::ObNumber::MAX_STORE_LEN;
  uint32_t nmb_base = number::ObNumber::BASE;
  uint8_t exp, digit_len;

  if (scale >= 0) {
    int64_t calc_exp = -1;
    if ((scale % number::ObNumber::DIGIT_LEN) != 0) {
      int16_t rem_scale = scale % number::ObNumber::DIGIT_LEN;
      digits[--idx] = static_cast<uint32_t>(
        (numerator % pows[rem_scale - 1]) * pows[number::ObNumber::DIGIT_LEN - rem_scale - 1]);
      numerator /= pows[rem_scale - 1];
      scale -= rem_scale;
    }
    for (; OB_SUCC(ret) && scale > 0 && numerator > 0;) {
      digits[--idx] = (uint32_t)(numerator) % nmb_base;
      numerator /= nmb_base;
      scale -= number::ObNumber::DIGIT_LEN;
    }
    while (OB_SUCC(ret) && scale > 0) {
      scale -= number::ObNumber::DIGIT_LEN;
      calc_exp--;
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (numerator == 0) {
      exp = 0x7F & (uint8_t)(number::ObNumber::EXP_ZERO + calc_exp);
      digit_len = (uint8_t)(number::ObNumber::MAX_STORE_LEN - idx);
    } else {
      calc_exp = -1;
      while (OB_SUCC(ret) && numerator != 0) {
        digits[--idx] = (uint32_t)(numerator) % nmb_base;
        numerator /= nmb_base;
        calc_exp++;
      }
      exp = 0x7F & (uint8_t)(number::ObNumber::EXP_ZERO + calc_exp);
      digit_len = (uint8_t)(number::ObNumber::MAX_STORE_LEN - idx);
    }
  } else {
    int64_t calc_exp = 0;
    scale = -scale;
    while (scale >= number::ObNumber::DIGIT_LEN) {
      calc_exp += 1;
      scale -= number::ObNumber::DIGIT_LEN;
    }
    if (scale > 0) {
      digits[--idx] =
        static_cast<uint32_t>(numerator % (pows[number::ObNumber::DIGIT_LEN - scale - 1]));
      numerator /= pows[number::ObNumber::DIGIT_LEN - scale - 1];
      calc_exp++;
    }
    while (numerator > 0) {
      digits[--idx] = (uint32_t) (numerator % nmb_base);
      numerator /= nmb_base;
      calc_exp++;
    }
    exp = 0x7F & (uint8_t)(number::ObNumber::EXP_ZERO + calc_exp);
    digit_len = (uint8_t)(number::ObNumber::MAX_STORE_LEN - idx);
  }
  if (OB_SUCC(ret)) {
    ObNumberDesc desc;
    desc.len_ = digit_len;
    desc.exp_ = exp;
    desc.sign_ = is_neg ? number::ObNumber::NEGATIVE : number::ObNumber::POSITIVE;
    if (is_neg) {
      desc.exp_ = 0x7F & (~desc.exp_);
      desc.exp_++;
    }
    uint32_t *copy_digits = nullptr;
    if (digit_len > 0) {
      if (OB_ISNULL(copy_digits = (uint32_t *)allocator.alloc(digit_len * sizeof(uint32_t)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(WARN, "allocate memory failed", K(ret));
      } else {
        nmb.assign(desc.desc_, copy_digits);
        if (OB_FAIL(nmb.normalize_(digits + idx, digit_len))) {
          COMMON_LOG(WARN, "normalize number failed", K(ret));
        }
      }
    } else if (digit_len == 0) {
      nmb.set_zero();
    }
  }
  return ret;
}

template <typename Allocator>
int to_number(const ObDecimalInt *decint, const int32_t int_bytes, int16_t scale,
              Allocator &allocator, number::ObNumber &numb)
{
#define ASSIGN_NMB(int_type)\
  const int_type *v = reinterpret_cast<const int_type *>(decint);\
  ret = to_number(*v, scale, allocator, numb);\

  int ret = OB_SUCCESS;
  if (OB_ISNULL(decint) || OB_UNLIKELY(int_bytes <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid decimal int", K(decint), K(int_bytes));
  } else if (OB_UNLIKELY(scale == NUMBER_SCALE_UNKNOWN_YET)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "invalid scale", K(ret), K(scale), K(lib::is_oracle_mode()));
  } else {
    DISPATCH_WIDTH_TASK(int_bytes, ASSIGN_NMB);
  }
  return ret;
#undef ASSIGN_NMB
}

template<typename T, typename Allocator>
int from_integer(const T v, Allocator &allocator, ObDecimalInt *&decint, int32_t &int_bytes)
{
  int ret = OB_SUCCESS;
  static_assert(std::is_integral<T>::value && sizeof(T) <= sizeof(int64_t), "");
  void *data = nullptr;
  UNUSED(data);
  if (std::is_signed<T>::value || (sizeof(T) == sizeof(int32_t) && v <= INT32_MAX)
      || (sizeof(T) == sizeof(int64_t) && v <= INT64_MAX)) {
    int32_t alloc_size = sizeof(T);
    T *data = (T *)allocator.alloc(alloc_size);
    if (OB_ISNULL(data)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "allocate memory failed", K(ret), K(alloc_size));
    } else {
      *data = v;
      decint = (ObDecimalInt *)data;
      int_bytes = alloc_size;
    }
  } else if (sizeof(T) == sizeof(int32_t)) {
    int_bytes = sizeof(int64_t);
    int64_t *data = (int64_t *)allocator.alloc(int_bytes);
    if (OB_ISNULL(data)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "allocate memory failed", K(ret), K(int_bytes));
    } else {
      *data = v;
      decint = (ObDecimalInt *)data;
    }
  } else {
    int128_t *data = (int128_t *)allocator.alloc(sizeof(int128_t));
    if (OB_ISNULL(data)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "allocate memory failed", K(ret), K(sizeof(int128_t)));
    } else {
      data->items_[0] = v;
      data->items_[1] = 0;
      decint = (ObDecimalInt *)data;
      int_bytes = sizeof(int128_t);
    }
  }
  return ret;
}

template <typename T, typename Allocator>
int from_integer(const T v, Allocator &allocator, ObDecimalInt *&decint, int32_t &int_bytes,
                 const int16_t in_prec)
{
  int ret = OB_SUCCESS;
  static_assert(std::is_integral<T>::value && sizeof(T) <= sizeof(int64_t), "");
  int_bytes = ObDecimalIntConstValue::get_int_bytes_by_precision(in_prec);
  void *data = nullptr;
  if (OB_UNLIKELY(int_bytes <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "invalid input precision", K(ret), K(in_prec));
  } else if (OB_ISNULL(data = allocator.alloc(int_bytes))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "allocate memory failed", K(ret), K(int_bytes));
  } else {
    decint = reinterpret_cast<ObDecimalInt *>(data);
    switch (int_bytes) {
    case sizeof(int32_t):
      *decint->int32_v_ = static_cast<const int32_t>(v);
      break;
    case sizeof(int64_t):
      *decint->int64_v_ = v;
      break;
    case sizeof(int128_t):
      *decint->int128_v_ = v;
      break;
    case sizeof(int256_t):
      *decint->int256_v_ = v;
      break;
    case sizeof(int512_t):
      *decint->int512_v_ = v;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "invalid int bytes for integer", K(ret), K(int_bytes));
      break;
    }
  }
  return ret;
}

template <typename T, typename Allocator>
int batch_from_integer(const T *val_arr, Allocator &allocator, const ObDecimalInt **decint_arr,
                       int32_t &int_bytes, const int16_t in_prec, const int64_t batch_size)
{
#define ASSIGN_VAL(int_type)                                                                       \
  for (int i = 0; i < batch_size; i++) {                                                           \
    int_type *vals = reinterpret_cast<int_type *>(data);                                           \
    *(vals + i) = val_arr[i];                                                                      \
    decint_arr[i] = reinterpret_cast<const ObDecimalInt *>(vals + i);                              \
  }

  int ret = OB_SUCCESS;
  static_assert(std::is_integral<T>::value && sizeof(T) <= sizeof(int64_t), "");
  int_bytes = ObDecimalIntConstValue::get_int_bytes_by_precision(in_prec);
  void *data = nullptr;
  if (OB_UNLIKELY(int_bytes <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "invalid input precision", K(ret), K(in_prec));
  } else if (OB_ISNULL(data = allocator.alloc(int_bytes * batch_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "allocate memory failed", K(ret));
  } else {
    DISPATCH_WIDTH_TASK(int_bytes, ASSIGN_VAL);
  }
  return ret;
#undef ASSIGN_VAL
}

int from_double(const double x, ObIAllocator &allocator, ObDecimalInt *&decint, int32_t &int_bytes,
                int16_t &scale);

template<typename T, class = typename std::enable_if<sizeof(T) <= sizeof(int64_t)>::type>
bool mul_overflow(const T &lhs, const T &rhs, T &res)
{
  return __builtin_mul_overflow(lhs, rhs, &res);
}

template <unsigned Bits, typename Signed>
bool mul_overflow(const ObWideInteger<Bits, Signed> &lhs, const ObWideInteger<Bits, Signed> &rhs,
                  ObWideInteger<Bits, Signed> &res)
{
  int ret = ObWideInteger<Bits, Signed>::_impl::template multiply<CheckOverFlow>(lhs, rhs, res);
  return (ret == OB_OPERATE_OVERFLOW);
}

} // end wide namespace

template<typename T>
T get_scale_factor(int16_t scale)
{
  const static int64_t POWERS_OF_TEN[MAX_PRECISION_DECIMAL_INT_64 + 1] =
  {1,
   10,
   100,
   1000,
   10000,
   100000,
   1000000,
   10000000,
   100000000,
   1000000000,
   10000000000,
   100000000000,
   1000000000000,
   10000000000000,
   100000000000000,
   1000000000000000,
   10000000000000000,
   100000000000000000,
   1000000000000000000};

  T sf = 1;
  while (scale > 0) {
    const int16_t step = MIN(scale, MAX_PRECISION_DECIMAL_INT_64);
    sf = static_cast<T>(sf * POWERS_OF_TEN[step]);
    scale -= step;
  }
  return sf;
}

} // end namespace common
} // end namespace oceanbase
#endif // !OB_WIDE_INTEGER_