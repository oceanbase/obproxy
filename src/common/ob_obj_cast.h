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

#ifndef OCEANBASE_COMMON_OB_OBJ_CAST_
#define OCEANBASE_COMMON_OB_OBJ_CAST_

#include "common/ob_object.h"
#include "common/ob_accuracy.h"
#include "common/ob_zerofill_info.h"
#include "lib/timezone/ob_timezone_info.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/charset/ob_charset.h"

namespace oceanbase
{
namespace common
{

#define DOUBLE_TRUE_VALUE_THRESHOLD (1e-50)

#define OB_IS_DOUBLE_ZERO(d)  (d < DOUBLE_TRUE_VALUE_THRESHOLD && d > -DOUBLE_TRUE_VALUE_THRESHOLD)

#define OB_IS_DOUBLE_NOT_ZERO(d)  (d >= DOUBLE_TRUE_VALUE_THRESHOLD || d <= -DOUBLE_TRUE_VALUE_THRESHOLD)

#define CM_NONE                       (0ULL)
#define CM_WARN_ON_FAIL               (1ULL << 0)   // treat fail as warn, otherwise
                                                    // treat fail as error (set to ret).
#define CM_NULL_ON_WARN               (1ULL << 1)   // return null value when warned, otherwise
                                                    // return zero value.
                                                    // only for non-numeric types.
#define CM_NO_RANGE_CHECK             (1ULL << 2)   // no range check when cast to numeric type, otherwise
                                                    // do range check.
#define CM_NO_CAST_INT_UINT           (1ULL << 3)   // no cast between int and uint, otherwise
                                                    // do cast between int and uint.
#define CM_CONST_TO_DECIMAL_INT_UP    (1ULL << 17)
#define CM_CONST_TO_DECIMAL_INT_DOWN  (1ULL << 18)
#define CM_CONST_TO_DECIMAL_INT_EQ    (1ULL << 19)
#define CM_BY_TRANSFORMER             (1ULL << 20)
// string->integer(int/uint)时默认进行round(round to nearest)，
// 如果设置该标记，则会进行trunc(round to zero)
// ceil(round to +inf)以及floor(round to -inf)暂时没有支持
#define CM_STRING_INTEGER_TRUNC       (1ULL << 57)
#define CM_COLUMN_CONVERT             (1ULL << 58)
#define CM_ENABLE_BLOB_CAST           (1ULL << 59)
#define CM_EXPLICIT_CAST              (1ULL << 60)
#define CM_ORACLE_MODE (1ULL << 61)

#define CM_INSERT_UPDATE_SCOPE        (1ULL << 62)  // affect calculate values() function. return the insert values
                                                    // otherwise return NULL;
#define CM_INTERNAL_CALL              (1ULL << 63)  // is internal call, otherwise
                                                    // is external call.
typedef uint64_t ObCastMode;

#define CM_IS_WARN_ON_FAIL(mode)              (CM_WARN_ON_FAIL & (mode))
#define CM_IS_ERROR_ON_FAIL(mode)             (!CM_IS_WARN_ON_FAIL(mode))
#define CM_SET_WARN_ON_FAIL(mode)             (CM_WARN_ON_FAIL | (mode))
#define CM_IS_NULL_ON_WARN(mode)              (CM_NULL_ON_WARN & (mode))
#define CM_IS_ZERO_ON_WARN(mode)              (!CM_IS_NULL_ON_WARN(mode))
#define CM_SKIP_RANGE_CHECK(mode)             (CM_NO_RANGE_CHECK & (mode))
#define CM_NEED_RANGE_CHECK(mode)             (!CM_SKIP_RANGE_CHECK(mode))
#define CM_SKIP_CAST_INT_UINT(mode)           (CM_NO_CAST_INT_UINT & (mode))
#define CM_NEED_CAST_INT_UINT(mode)           (!CM_SKIP_CAST_INT_UINT(mode))
#define CM_UNSET_NO_CAST_INT_UINT(mode)       (~CM_NO_CAST_INT_UINT & (mode))
#define CM_IS_INTERNAL_CALL(mode)             (CM_INTERNAL_CALL & (mode))
#define CM_IS_EXTERNAL_CALL(mode)             (!CM_IS_INTERNAL_CALL(mode))
#define CM_IS_CONST_TO_DECIMAL_INT(mode)                                                           \
  ((((mode)&CM_CONST_TO_DECIMAL_INT_UP) != 0) || (((mode)&CM_CONST_TO_DECIMAL_INT_DOWN) != 0)      \
   || (((mode)&CM_CONST_TO_DECIMAL_INT_EQ) != 0))

#define CM_IS_COLUMN_CONVERT(mode)            ((CM_COLUMN_CONVERT & (mode)) != 0)
#define CM_IS_EXPLICIT_CAST(mode)             ((CM_EXPLICIT_CAST & (mode)) != 0)
#define CM_IS_IMPLICIT_CAST(mode)             (!CM_IS_EXPLICIT_CAST(mode))

struct ObObjCastParams
{
  // add params when necessary
  DEFINE_ALLOCATOR_WRAPPER
  ObObjCastParams()
    : allocator_(NULL),
      allocator_v2_(NULL),
      cur_time_(0),
      cast_mode_(CM_NONE),
      warning_(OB_SUCCESS),
      zf_info_(NULL),
      dest_collation_(CS_TYPE_INVALID),
      expect_obj_collation_(CS_TYPE_INVALID),
      res_accuracy_(NULL),
      dtc_params_(),
      format_number_with_limit_(true)
  {
    set_compatible_cast_mode();
  }

  ObObjCastParams(ObIAllocator* allocator_v2, const ObDataTypeCastParams* dtc_params, ObCastMode cast_mode,
                  ObCollationType dest_collation, ObAccuracy* res_accuracy = NULL)
      //ObIAllocator *allocator_v2, ObCastMode cast_mode, ObCollationType dest_collation, ObAccuracy *res_accuracy = NULL)
    : allocator_(NULL),
      allocator_v2_(allocator_v2),
      cur_time_(0),
      cast_mode_(cast_mode),
      warning_(OB_SUCCESS),
      zf_info_(NULL),
      dest_collation_(dest_collation),
      expect_obj_collation_(dest_collation),
      res_accuracy_(res_accuracy),
      dtc_params_(),
      format_number_with_limit_(true)
  {
    set_compatible_cast_mode();
    if (NULL != dtc_params) {
      dtc_params_ = *dtc_params;
    }
  }

  ObObjCastParams(ObIAllocator* allocator_v2, const ObDataTypeCastParams* dtc_params, int64_t cur_time,
                  ObCastMode cast_mode, ObCollationType dest_collation, const ObZerofillInfo* zf_info = NULL,
                  ObAccuracy* res_accuracy = NULL)
    : allocator_(NULL),
      allocator_v2_(allocator_v2),
      cur_time_(cur_time),
      cast_mode_(cast_mode),
      warning_(OB_SUCCESS),
      zf_info_(zf_info),
      dest_collation_(dest_collation),
      expect_obj_collation_(dest_collation),
      res_accuracy_(res_accuracy),
      dtc_params_(),
      format_number_with_limit_(true)
  {
    set_compatible_cast_mode();
    if (NULL != dtc_params) {
      dtc_params_ = *dtc_params;
    }
  }
      
  void *alloc(const int64_t size) const
  {
    void *ret = NULL;
    if (NULL != allocator_v2_) {
      ret = allocator_v2_->alloc(size);
    } else if (NULL != allocator_) {
      ret = allocator_->alloc(size);
    }
    return ret;
  }

  void set_compatible_cast_mode()
  {
    if (lib::is_oracle_mode()) {
      cast_mode_ &= ~CM_WARN_ON_FAIL;
      cast_mode_ |= CM_ORACLE_MODE;
    } else {
      cast_mode_ &= ~CM_ORACLE_MODE;
    }
    return;
  }

  TO_STRING_KV(K(cur_time_), KP(cast_mode_), K(warning_), K(dest_collation_),
               K(expect_obj_collation_), K(res_accuracy_), K(format_number_with_limit_));
  
  IAllocator *allocator_;
  ObIAllocator *allocator_v2_;
  int64_t cur_time_;
  ObCastMode cast_mode_;
  int warning_;
  const ObZerofillInfo *zf_info_;
  ObCollationType dest_collation_;
  ObCollationType expect_obj_collation_;  // for each column obj
  ObAccuracy *res_accuracy_;
  ObDataTypeCastParams dtc_params_;
  bool format_number_with_limit_;
};


typedef ObObjCastParams ObCastCtx;

class ObHexUtils {
public:
  // text can be odd number, like 'aaa', treat as '0aaa'
  static int unhex(const common::ObString &text, common::ObCastCtx &cast_ctx, common::ObObj &result);
  static int hex(const common::ObString &text, common::ObCastCtx &cast_ctx, common::ObObj &result);
  static int hex_for_mysql(const uint64_t uint_val, common::ObCastCtx &cast_ctx, common::ObObj &result);
  static int rawtohex(const common::ObObj &text, common::ObCastCtx &cast_ctx, common::ObObj &result);
  static int hextoraw(const common::ObObj &text, common::ObCastCtx &cast_ctx, common::ObObj &result);
  static int get_uint(const common::ObObj &obj, common::ObCastCtx &cast_ctx, common::number::ObNumber &out);
  static int copy_raw(const common::ObObj &obj, common::ObCastCtx &cast_ctx, common::ObObj &result);

private:
  static int uint_to_raw(const common::number::ObNumber &text, common::ObCastCtx &cast_ctx, common::ObObj &result);
};


/**
 * cast functions to do the real work
 * cast the input object to the type specified and store the result in out_obj
 *
 * @note in_obj and out_obj could be the same object, the cast function should consider and support it.
 * @param params [in] other cast params, e.g. allocator
 * @param in_obj [in] input object
 * @param out_tpye [in] output object tyoe
 * @param out_obj [out] output object
 *
 * @return ob errno
 */
typedef int (*ObObjCastFunc)(ObObjType expect_type, ObObjCastParams &params,
                             const ObObj &in_obj, ObObj &out_obj, const ObCastMode cast_mode);

////////////////////////////////////////////////////////////////
class ObBufAllocator : public ObObjCastParams::IAllocator
{
public:
  ObBufAllocator() : buf_(NULL),
                     buf_size_(0),
                     used_buf_len_(const_cast<int64_t &>(buf_size_)) {}
  ObBufAllocator(char *buf,
                 const int64_t buf_size,
                 int64_t &used_buf_len) : buf_(buf),
                                          buf_size_(buf_size),
                                          used_buf_len_(used_buf_len) {}
  void *alloc(const int64_t size)
  {
    char *ret = NULL;
    if (OB_LIKELY((NULL != buf_) &&
                 (used_buf_len_ + size) <= buf_size_)) {
      ret = buf_ + used_buf_len_;
      used_buf_len_ += size;
    }
    return ret;
  }
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObBufAllocator);
private:
  char *const buf_;
  const int64_t buf_size_;
  int64_t &used_buf_len_;
};
class ObStrAllocator : public ObObjCastParams::IAllocator
{
public:
  ObStrAllocator() : pos_(0),
                     ba_() {}
  explicit ObStrAllocator(ObString &str) : pos_(0),
                                           ba_(str.ptr(), (0 != str.length()) ? str.length() : str.size(), pos_)
  {}

  void *alloc(const int64_t size)
  {
    return ba_.alloc(size);
  }
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObStrAllocator);
private:
  int64_t pos_;
  ObBufAllocator ba_;
};
class ObDummyAllocator : public ObObjCastParams::IAllocator
{
public:
  ObDummyAllocator() {}
  void *alloc(const int64_t size)
  {
    UNUSED(size);
    return NULL;
  }
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObDummyAllocator);
};

enum AllocatorSource
{
  FromNull = 0,
  FromBuffer = 1,
  FromString = 2,
  FromAllocator = 3,
};

template <AllocatorSource Source, class AllocatorTmpl = void>
class ObObjCaster
{
  template <AllocatorSource v, class AllocatorTmpl_ = void>
  struct AllocatorTraits
  {
  };
  template <class AllocatorTmpl_>
  struct AllocatorTraits<FromNull, AllocatorTmpl_>
  {
    typedef ObDummyAllocator AllocatorType;
  };
  template <class AllocatorTmpl_>
  struct AllocatorTraits<FromBuffer, AllocatorTmpl_>
  {
    typedef ObBufAllocator AllocatorType;
  };
  template <class AllocatorTmpl_>
  struct AllocatorTraits<FromString, AllocatorTmpl_>
  {
    typedef ObStrAllocator AllocatorType;
  };
  template <class AllocatorTmpl_>
  struct AllocatorTraits<FromAllocator, AllocatorTmpl_>
  {
    typedef AllocatorTmpl_ AllocatorType;
  };
  typedef typename AllocatorTraits<Source, AllocatorTmpl>::AllocatorType Allocator;
public:
  ObObjCaster(char *buf,
              const int64_t buf_size,
              int64_t &used_buf_len) : ba_(buf, buf_size, used_buf_len),
                                       sa_(),
                                       da_(),
                                       allocator_(ba_) {}

  explicit ObObjCaster(ObString &str) : ba_(),
                                        sa_(str),
                                        da_(),
                                        allocator_(sa_) {}

  ObObjCaster() : ba_(),
                  sa_(),
                  da_(),
                  allocator_(da_) {}

  explicit ObObjCaster(Allocator &allocator) : ba_(),
                                               sa_(),
                                               da_(),
                                               allocator_(allocator) {}

  ~ObObjCaster() {}
public:
  int expr_obj_cast(const ObObjType orig_type,
                    const ObObjType expect_type,
                    const ObTimeZoneInfo *tz_info,
                    const ObObj &in,
                    ObObj &out);
  int obj_cast(const ObObj &orig_cell,
               const ObObjType expect_type,
               const ObTimeZoneInfo *tz_info,
               ObObj &casted_cell,
               const ObObj *&res_cell);
public:
  int expr_obj_cast(const ObObjTypeClass orig_tc,
                    ObObjTypeClass expect_tc,
                    const ObObjType expect_type,
                    const ObTimeZoneInfo *tz_info,
                    const ObObj &in,
                    ObObj &out);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObObjCaster);
private:
  ObBufAllocator ba_;
  ObStrAllocator sa_;
  ObDummyAllocator da_;
  Allocator &allocator_;
};

template <AllocatorSource Source, class AllocatorTmpl>
int ObObjCaster<Source, AllocatorTmpl>::expr_obj_cast(const ObObjType orig_type,
                                                      ObObjType expect_type,
                                                      const ObTimeZoneInfo *tz_info,
                                                      const ObObj &in,
                                                      ObObj &out)
{
  const ObObjTypeClass orig_tc = ob_obj_type_class(orig_type);
  ObObjTypeClass expect_tc = ob_obj_type_class(expect_type);
  return expr_obj_cast(orig_tc, expect_tc, expect_type, tz_info, in, out);
}

template <AllocatorSource Source, class AllocatorTmpl>
int ObObjCaster<Source, AllocatorTmpl>::expr_obj_cast(const ObObjTypeClass orig_tc,
                                                      ObObjTypeClass expect_tc,
                                                      // maybe we have only tc, no type, like relation promotion.
                                                      ObObjType expect_type,
                                                      const ObTimeZoneInfo *tz_info,
                                                      const ObObj &in,
                                                      ObObj &out)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(expect_tc >= ObMaxTC)) {
    expect_tc = ob_obj_type_class(expect_type);
  } else if (OB_UNLIKELY(expect_type >= ObMaxType)) {
    expect_type = ob_obj_default_type(expect_tc);
  }
  if (OB_UNLIKELY(in.is_null())) {
    out.set_null();
  } else if (OB_UNLIKELY(ob_is_invalid_obj_tc(orig_tc) ||
             ob_is_invalid_obj_tc(expect_tc) ||
             ob_is_invalid_obj_type(expect_type) ||
             expect_tc != ob_obj_type_class(expect_type))) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WDIAG, "invalid argument(s)",
               K(ret), K(orig_tc), K(expect_tc), K(expect_type));
  } else {
    char varchar_buf[OB_CAST_TO_VARCHAR_MAX_LENGTH];
    ObObj to;
    if (OB_UNLIKELY(ObStringTC == expect_tc)) {
      to.set_varchar(varchar_buf, OB_CAST_TO_VARCHAR_MAX_LENGTH);
    }

    ObObjCastParams::TAllocator<Allocator> ta(allocator_);
    ObObjCastParams params;
    params.allocator_ = &ta;
    params.dtc_params_.tz_info_ = tz_info;
    params.cast_mode_ = CM_WARN_ON_FAIL;
    int warning = OB_SUCCESS;
    extern ObObjCastFunc OB_OBJ_CAST[ObMaxTC][ObMaxTC];
    if (OB_FAIL(OB_OBJ_CAST[orig_tc][expect_tc](expect_type, params, in, to, warning))) {
      COMMON_LOG(WDIAG, "failed to type cast obj",
                 K(ret), K(in), K(orig_tc), K(expect_tc));
    } else if (OB_SUCCESS != warning) {
      ret = warning;
      COMMON_LOG(WDIAG, "failed to cast obj to expect type",
                 K(ret), K(in), K(expect_type));
    }
    out = to;
    if (ObStringTC == orig_tc &&
        ObStringTC == expect_tc &&
        in.get_collation_type() != CS_TYPE_INVALID) {
      out.set_collation_type(in.get_collation_type()); //set same collation type
    } else if (ObStringTC == expect_tc) {
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
        BACKTRACE(WDIAG, true, "invalid collation type: %s", to_cstring(in));
      }
      out.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
    }
  }
  return ret;
}

template <AllocatorSource Source, class AllocatorTmpl>
int ObObjCaster<Source, AllocatorTmpl>::obj_cast(
    const ObObj &orig_cell,
    const ObObjType expect_type,
    const ObTimeZoneInfo *tz_info,
    ObObj &casted_cell,
    const ObObj *&res_cell)
{
  int ret = OB_SUCCESS;
  res_cell = NULL;
  if (OB_LIKELY(orig_cell.get_type() != expect_type)) {
    ret = expr_obj_cast(orig_cell.get_type(), expect_type, tz_info, orig_cell, casted_cell);
    res_cell = &casted_cell;
    // we need not see whether ret is not OB_SUCCESS, because without strict sql mode,
    // ER_WARN_DATA_OUT_OF_RANGE is allowed, and there is no following operation in
    // this func.
  } else {
    res_cell = &orig_cell;
  }
  return ret;
}

/**
 * cast obj to the type
 * utility functions which wrapper ObObjCaster
 * @param from [in]
 * @param to_type [in] expected type
 * @param to [out]
 * @param allocator [in]
 *
 * @return
 */
template <typename Allocator>
int ob_obj_cast_to(const ObObj &from, const ObObjType to_type, ObObj &to, const ObTimeZoneInfo *tz_info, Allocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(to_type == from.get_type())) {
    if (&from != &to) {
      to = from;
    }
  } else {
    ObObjCaster<FromAllocator, Allocator> caster(allocator);
    ret = caster.expr_obj_cast(from.get_type(), to_type, tz_info, from, to);
  }
  return ret;
}

template <typename Allocator>
int ob_obj_cast_to(ObObj &obj, const ObObjType to_type, const ObTimeZoneInfo *tz_info, Allocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(to_type != obj.get_type())) {
    ObObjCaster<FromAllocator, Allocator> caster(allocator);
    ret = caster.expr_obj_cast(obj.get_type(), to_type, tz_info, obj, obj);
  }
  return ret;
}

// whether the cast is supported
bool cast_supported(const ObObjTypeClass orig_td, const ObObjTypeClass expect_td);
int ob_obj_to_ob_time_with_date(const ObObj &obj, const ObTimeZoneInfo *tz_info, ObTime &ob_time);
int ob_obj_to_ob_time_without_date(const ObObj &obj, const ObTimeZoneInfo *tz_info, ObTime &ob_time);

int obj_collation_check(const bool is_strict_mode, const ObCollationType cs_type, ObObj &obj);
int obj_accuracy_check(ObCastCtx &cast_ctx, const ObAccuracy &accuracy,
                       const ObCollationType cs_type, const ObObj &obj, ObObj &buf_obj, const ObObj *&res_obj);

class ObObjCasterV2
{
public:
  static int to_type(const ObObjType expect_type, ObCastCtx &cast_ctx,
                     const ObObj &in_obj, ObObj &buf_obj, const ObObj *&out_obj);
  static int to_type(const ObObjType expect_type, ObCastCtx &cast_ctx,
                     const ObObj &in_obj, ObObj &out_obj);
  static int to_type(const ObObjType expect_type, ObCollationType expect_cs_type,
                     ObCastCtx &cast_ctx, const ObObj &in_obj, ObObj &out_obj);
  static int is_cast_monotonic(ObObjType t1, ObObjType t2, bool &is_monotonic);
  static int is_order_consistent(const ObObjMeta &from,
                                 const ObObjMeta &to,
                                 bool &result);
  static int is_injection(const ObObjMeta &from,
                          const ObObjMeta &to,
                          bool &result);
private:
  inline static int64_t get_idx_of_collate(ObCollationType cs_type)
  {
    int64_t idx = -1;
    if (CS_TYPE_UTF8MB4_GENERAL_CI == cs_type) {
      idx = 0;
    } else if (CS_TYPE_UTF8MB4_BIN == cs_type) {
      idx = 1;
    } else if (CS_TYPE_BINARY == cs_type) {
      idx = 2;
    }
    return idx;
  }
private:
  static const bool CAST_MONOTONIC[ObMaxTC][ObMaxTC];
  static const bool ORDER_CONSISTENT[ObMaxTC][ObMaxTC];
  static const bool ORDER_CONSISTENT_WITH_BOTH_STRING[ObCharset::VALID_COLLATION_TYPES][ObCharset::VALID_COLLATION_TYPES][ObCharset::VALID_COLLATION_TYPES];
  static const bool INJECTION[ObMaxTC][ObMaxTC];
  static const bool INJECTION_WITH_BOTH_STRING[ObCharset::VALID_COLLATION_TYPES][ObCharset::VALID_COLLATION_TYPES][ObCharset::VALID_COLLATION_TYPES];
};


} // end namespace common
} // end namespace oceanbase

#endif //OCEANBASE_COMMON_OB_OBJ_CAST_
