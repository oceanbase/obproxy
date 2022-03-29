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

#include "lib/utility/utility.h"
#include "common/ob_obj_compare.h"

namespace oceanbase
{
namespace common
{

#define OBJ_TYPE_CLASS_CHECK(obj, tc)                                               \
  if (OB_UNLIKELY(obj.get_type_class() != tc)) {                                    \
    LOG_ERROR("unexpected error. mismatch function for comparison", K(obj), K(tc)); \
    right_to_die_or_duty_to_live();                                                 \
  }

#define DEFINE_CMP_OP_FUNC(tc, type, op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<tc, tc, op>(const ObObj &obj1, \
                                             const ObObj &obj2, \
                                             const ObCompareCtx &/*cmp_ctx*/) \
  { \
    return obj1.get_##type() op_str obj2.get_##type(); \
  }

#define DEFINE_CMP_FUNC(tc, type) \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<tc, tc>(const ObObj &obj1, \
                                      const ObObj &obj2, \
                                      const ObCompareCtx &/*cmp_ctx*/) \
  { \
    return obj1.get_##type() < obj2.get_##type() \
           ? CR_LT \
           : obj1.get_##type() > obj2.get_##type() \
             ? CR_GT \
             : CR_EQ; \
  }

#define DEFINE_CMP_OP_FUNC_NULL_NULL(op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObNullTC, ObNullTC, op>(const ObObj &/*obj1*/, \
                                                         const ObObj &/*obj2*/, \
                                                         const ObCompareCtx &cmp_ctx) \
  { \
    return cmp_ctx.is_null_safe_ ? static_cast<int>(0 op_str 0) : CR_NULL; \
  }

#define DEFINE_CMP_FUNC_NULL_NULL() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObNullTC, ObNullTC>(const ObObj &/*obj1*/, \
                                                  const ObObj &/*obj2*/, \
                                                  const ObCompareCtx &cmp_ctx) \
  { \
    return cmp_ctx.is_null_safe_ ? CR_EQ : CR_NULL; \
  }

#define DEFINE_CMP_OP_FUNC_EXT_EXT(op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObExtendTC, ObExtendTC, op>(const ObObj &obj1, \
                                                             const ObObj &obj2, \
                                                             const ObCompareCtx &/*cmp_ctx*/) \
  { \
    return (obj1.is_min_value() && obj2.is_min_value()) || (obj1.is_max_value() && obj2.is_max_value()) \
           ? static_cast<int>(0 op_str 0) \
           : obj1.is_min_value() || obj2.is_max_value() \
             ? static_cast<int>(-1 op_str 1) \
             : obj1.is_max_value() || obj2.is_min_value() \
               ? static_cast<int>(1 op_str -1) \
               : CR_ERROR; \
  }

#define DEFINE_CMP_FUNC_EXT_EXT() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObExtendTC, ObExtendTC>(const ObObj &obj1, \
                                                      const ObObj &obj2, \
                                                      const ObCompareCtx &/*cmp_ctx*/) \
  { \
    return (obj1.is_min_value() && obj2.is_min_value()) || (obj1.is_max_value() && obj2.is_max_value()) \
           ? CR_EQ \
           : obj1.is_min_value() || obj2.is_max_value() \
             ? CR_LT \
             : obj1.is_max_value() || obj2.is_min_value() \
               ? CR_GT \
               : CR_ERROR; \
  }

#define DEFINE_CMP_OP_FUNC_NULL_EXT(op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObNullTC, ObExtendTC, op>(const ObObj &/*obj1*/, \
                                                           const ObObj &obj2, \
                                                           const ObCompareCtx &cmp_ctx) \
  { \
    return cmp_ctx.is_null_safe_ \
           ? obj2.is_min_value() \
             ? static_cast<int>(0 op_str -1) \
             : obj2.is_max_value() \
               ? static_cast<int>(0 op_str 1) \
               : CR_ERROR \
           : CR_NULL; \
  }

#define DEFINE_CMP_FUNC_NULL_EXT() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObNullTC, ObExtendTC>(const ObObj &/*obj1*/, \
                                                    const ObObj &obj2, \
                                                    const ObCompareCtx &cmp_ctx) \
  { \
    return cmp_ctx.is_null_safe_ \
           ? obj2.is_min_value() \
             ? CR_GT \
             : obj2.is_max_value() \
               ? CR_LT \
               : CR_ERROR \
           : CR_NULL; \
  }

#define DEFINE_CMP_OP_FUNC_EXT_NULL(op, sym_op) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObExtendTC, ObNullTC, op>(const ObObj &obj1, \
                                                           const ObObj &obj2, \
                                                           const ObCompareCtx &cmp_ctx) \
  { \
    return ObObjCmpFuncs::cmp_op_func<ObNullTC, ObExtendTC, sym_op>(obj2, obj1, cmp_ctx); \
  }

#define DEFINE_CMP_FUNC_EXT_NULL() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObExtendTC, ObNullTC>(const ObObj &obj1, \
                                                    const ObObj &/*obj2*/, \
                                                    const ObCompareCtx &cmp_ctx) \
  { \
    return cmp_ctx.is_null_safe_ \
           ? obj1.is_min_value() \
             ? CR_LT \
             : obj1.is_max_value() \
               ? CR_GT \
               : CR_ERROR \
           : CR_NULL; \
  }

#define DEFINE_CMP_OP_FUNC_NULL_XXX(op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObNullTC, ObMaxTC, op>(const ObObj &/*obj1*/, \
                                                        const ObObj &/*obj2*/, \
                                                        const ObCompareCtx &cmp_ctx) \
  { \
    return cmp_ctx.is_null_safe_ ? static_cast<int>(0 op_str 1) : CR_NULL; \
  }

#define DEFINE_CMP_FUNC_NULL_XXX() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObNullTC, ObMaxTC>(const ObObj &/*obj1*/, \
                                                 const ObObj &/*obj2*/, \
                                                 const ObCompareCtx &cmp_ctx) \
  { \
    return cmp_ctx.is_null_safe_ ? CR_LT : CR_NULL; \
  }

#define DEFINE_CMP_OP_FUNC_XXX_NULL(op, sym_op) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObMaxTC, ObNullTC, op>(const ObObj &obj1, \
                                                        const ObObj &obj2, \
                                                        const ObCompareCtx &cmp_ctx) \
  { \
    return ObObjCmpFuncs::cmp_op_func<ObNullTC, ObMaxTC, sym_op>(obj2, obj1, cmp_ctx); \
  }

#define DEFINE_CMP_FUNC_XXX_NULL(tc) \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObMaxTC, ObNullTC>(const ObObj &/*obj1*/, \
                                                 const ObObj &/*obj2*/, \
                                                 const ObCompareCtx &cmp_ctx) \
  { \
    return cmp_ctx.is_null_safe_ ? CR_GT : CR_NULL; \
  }

#define DEFINE_CMP_OP_FUNC_XXX_EXT(op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObMaxTC, ObExtendTC, op>(const ObObj &/*obj1*/, \
                                                          const ObObj &obj2, \
                                                          const ObCompareCtx &/*cmp_ctx*/) \
  { \
    return obj2.is_min_value() \
           ? static_cast<int>(0 op_str -1) \
           : obj2.is_max_value() \
             ? static_cast<int>(0 op_str 1) \
             : CR_ERROR; \
  }

#define DEFINE_CMP_FUNC_XXX_EXT() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObMaxTC, ObExtendTC>(const ObObj &/*obj1*/, \
                                                   const ObObj &obj2, \
                                                   const ObCompareCtx &/*cmp_ctx*/) \
  { \
    return obj2.is_min_value() \
           ? CR_GT \
           : obj2.is_max_value() \
             ? CR_LT \
             : CR_ERROR; \
  }

#define DEFINE_CMP_OP_FUNC_EXT_XXX(op, sym_op) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObExtendTC, ObMaxTC, op>(const ObObj &obj1, \
                                                          const ObObj &obj2, \
                                                          const ObCompareCtx &cmp_ctx) \
  { \
    return ObObjCmpFuncs::cmp_op_func<ObMaxTC, ObExtendTC, sym_op>(obj2, obj1, cmp_ctx); \
  }

#define DEFINE_CMP_FUNC_EXT_XXX() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObExtendTC, ObMaxTC>(const ObObj &obj1, \
                                                   const ObObj &/*obj2*/, \
                                                   const ObCompareCtx &/*cmp_ctx*/) \
  { \
    return obj1.is_min_value() \
           ? CR_LT \
           : obj1.is_max_value() \
             ? CR_GT \
             : CR_ERROR; \
  }

#define DEFINE_CMP_OP_FUNC_INT_UINT(op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObIntTC, ObUIntTC, op>(const ObObj &obj1, \
                                                        const ObObj &obj2, \
                                                        const ObCompareCtx &/*cmp_ctx*/) \
  { \
    return obj1.get_int() < 0 \
           ? obj1.get_int() op_str 0 \
           : obj1.get_uint64() op_str obj2.get_uint64(); \
  }

#define DEFINE_CMP_FUNC_INT_UINT() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObIntTC, ObUIntTC>(const ObObj &obj1, \
                                                 const ObObj &obj2, \
                                                 const ObCompareCtx &/*cmp_ctx*/) \
  { \
    return (obj1.get_int() < 0 || obj1.get_uint64() < obj2.get_uint64()) \
           ? CR_LT \
           : obj1.get_uint64() > obj2.get_uint64() \
             ? CR_GT \
             : CR_EQ; \
  }

// obj1 LE obj2 is equal to obj2 GE obj1, we say that LE and GE is symmetric.
// so sym_op is short for symmetric operator, which is used for reuse other functions.
#define DEFINE_CMP_OP_FUNC_UINT_INT(op, sym_op) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObUIntTC, ObIntTC, op>(const ObObj &obj1, \
                                                        const ObObj &obj2, \
                                                        const ObCompareCtx &cmp_ctx) \
  { \
    return ObObjCmpFuncs::cmp_op_func<ObIntTC, ObUIntTC, sym_op>(obj2, obj1, cmp_ctx); \
  }

#define DEFINE_CMP_FUNC_UINT_INT() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObUIntTC, ObIntTC>(const ObObj &obj1, \
                                                 const ObObj &obj2, \
                                                 const ObCompareCtx &cmp_ctx) \
  { \
    return -ObObjCmpFuncs::cmp_func<ObIntTC, ObUIntTC>(obj2, obj1, cmp_ctx); \
  }

#define DEFINE_CMP_OP_FUNC_XXX_REAL(tc, type, real_tc, real_type, op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<tc, real_tc, op>(const ObObj &obj1, \
                                                  const ObObj &obj2, \
                                                  const ObCompareCtx &/*cmp_ctx*/) \
  { \
    return static_cast<double>(obj1.get_##type()) op_str static_cast<double>(obj2.get_##real_type()); \
  }

#define DEFINE_CMP_FUNC_XXX_REAL(tc, type, real_tc, real_type) \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<tc, real_tc>(const ObObj &obj1, \
                                           const ObObj &obj2, \
                                           const ObCompareCtx &/*cmp_ctx*/) \
  { \
    return static_cast<double>(obj1.get_##type()) < static_cast<double>(obj2.get_##real_type()) \
           ? CR_LT \
           : static_cast<double>(obj1.get_##type()) > static_cast<double>(obj2.get_##real_type()) \
             ? CR_GT \
             : CR_EQ; \
  }

#define DEFINE_CMP_OP_FUNC_REAL_XXX(real_tc, real_type, tc, type, op, sym_op) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<real_tc, tc, op>(const ObObj &obj1, \
                                               const ObObj &obj2, \
                                               const ObCompareCtx &cmp_ctx) \
  { \
    return ObObjCmpFuncs::cmp_op_func<tc, real_tc, sym_op>(obj2, obj1, cmp_ctx); \
  }

#define DEFINE_CMP_FUNC_REAL_XXX(real_tc, real_type, tc, type) \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<real_tc, tc>(const ObObj &obj1, \
                                           const ObObj &obj2, \
                                           const ObCompareCtx &cmp_ctx) \
  { \
    return -ObObjCmpFuncs::cmp_func<tc, real_tc>(obj2, obj1, cmp_ctx); \
  }

#define DEFINE_CMP_OP_FUNC_XXX_NUMBER(tc, type, op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<tc, ObNumberTC, op>(const ObObj &obj1, \
                                                     const ObObj &obj2, \
                                                     const ObCompareCtx &/*cmp_ctx*/) \
  { \
    int val = 0 ; \
    if (CO_EQ == op) { \
      val = obj2.get_number().is_equal(obj1.get_##type()); \
    } else { \
      val = 0 op_str obj2.get_number().compare(obj1.get_##type()); \
    } \
    return val; \
  }

#define DEFINE_CMP_FUNC_XXX_NUMBER(tc, type) \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<tc, ObNumberTC>(const ObObj &obj1, \
                                              const ObObj &obj2, \
                                              const ObCompareCtx &/*cmp_ctx*/) \
  { \
    return -INT_TO_CR(obj2.get_number().compare(obj1.get_##type())); \
  }

#define DEFINE_CMP_OP_FUNC_NUMBER_XXX(tc, type, op, sys_op) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObNumberTC, tc, op>(const ObObj &obj1, \
                                                     const ObObj &obj2, \
                                                     const ObCompareCtx &cmp_ctx) \
  { \
    return ObObjCmpFuncs::cmp_op_func<tc, ObNumberTC, sys_op>(obj2, obj1, cmp_ctx); \
  }

#define DEFINE_CMP_FUNC_NUMBER_XXX(tc, type) \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObNumberTC, tc>(const ObObj &obj1, \
                                              const ObObj &obj2, \
                                              const ObCompareCtx &cmp_ctx) \
  { \
    return -ObObjCmpFuncs::cmp_func<tc, ObNumberTC>(obj2, obj1, cmp_ctx); \
  }

#define DEFINE_CMP_OP_FUNC_STRING_STRING(op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObStringTC, ObStringTC, op>(const ObObj &obj1, \
                                                             const ObObj &obj2, \
                                                             const ObCompareCtx &cmp_ctx) \
  { \
    ObCollationType cs_type = cmp_ctx.cmp_cs_type_; \
    if (CS_TYPE_INVALID == cs_type) { \
      if (obj1.get_collation_type() != obj2.get_collation_type() \
          || CS_TYPE_INVALID == obj1.get_collation_type()) { \
        LOG_ERROR("invalid collation", K(obj1), K(obj2)); \
      } else { \
        cs_type = obj1.get_collation_type(); \
      } \
    } \
    return CS_TYPE_INVALID != cs_type \
           ? static_cast<int>(ObCharset::strcmpsp(cs_type, obj1.v_.string_, obj1.val_len_, \
                                                  obj2.v_.string_, obj2.val_len_, true) op_str 0) \
           : CR_ERROR; \
  }

#define DEFINE_CMP_FUNC_STRING_STRING() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObStringTC, ObStringTC>(const ObObj &obj1, \
                                                      const ObObj &obj2, \
                                                      const ObCompareCtx &cmp_ctx) \
  { \
    ObCollationType cs_type = cmp_ctx.cmp_cs_type_; \
    if (CS_TYPE_INVALID == cs_type) { \
      if (obj1.get_collation_type() != obj2.get_collation_type() \
          || CS_TYPE_INVALID == obj1.get_collation_type()) { \
        LOG_ERROR("invalid collation", K(obj1), K(obj2)); \
      } else { \
        cs_type = obj1.get_collation_type(); \
      } \
    } \
    return CS_TYPE_INVALID != cs_type \
           ? INT_TO_CR(ObCharset::strcmpsp(cs_type, obj1.v_.string_, obj1.val_len_, \
                                           obj2.v_.string_, obj2.val_len_, true)) \
           : CR_ERROR; \
  }

//datetimetc VS datetimetc
#define DEFINE_CMP_OP_FUNC_DT_DT(op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObDateTimeTC, ObDateTimeTC, op>(const ObObj &obj1, \
                                                                 const ObObj &obj2, \
                                                                 const ObCompareCtx &cmp_ctx) \
  { \
    ObCmpRes ret = CR_FALSE; \
    int64_t v1 = obj1.get_datetime();\
    int64_t v2 = obj2.get_datetime();\
    if (obj1.get_type() != obj2.get_type()) { \
      if (OB_UNLIKELY(INVALID_TZ_OFF == cmp_ctx.tz_off_)) { \
        LOG_ERROR("invalid timezone offset", K(obj1), K(obj2)); \
        ret = CR_ERROR; \
      } else { \
        /*same tc while not same type*/ \
        if (ObDateTimeType == obj1.get_type()) { \
          v1 -= cmp_ctx.tz_off_; \
        } else { \
          v2 -= cmp_ctx.tz_off_; \
        } \
      } \
    } else { \
      /*same tc and same type. do nothing*/ \
    } \
    return CR_ERROR != ret ? static_cast<int>(v1 op_str v2) : CR_ERROR; \
  }

//datetimetc VS datetimetc
#define DEFINE_CMP_FUNC_DT_DT() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObDateTimeTC, ObDateTimeTC>(const ObObj &obj1, \
                                                          const ObObj &obj2, \
                                                          const ObCompareCtx &cmp_ctx) \
  { \
    ObCmpRes ret = CR_FALSE; \
    int64_t v1 = obj1.get_datetime();\
    int64_t v2 = obj2.get_datetime();\
    if (obj1.get_type() != obj2.get_type()) { \
      if (OB_UNLIKELY(INVALID_TZ_OFF == cmp_ctx.tz_off_)) { \
        LOG_ERROR("invalid timezone offset", K(obj1), K(obj2)); \
        ret = CR_ERROR; \
      } else { \
        /*same tc while not same type*/ \
        if (ObDateTimeType == obj1.get_type()) { \
          v1 -= cmp_ctx.tz_off_; \
        } else { \
          v2 -= cmp_ctx.tz_off_; \
        } \
      } \
    } else { \
      /*same tc and same type. do nothing*/ \
    }\
    return CR_ERROR != ret \
           ? v1 < v2 \
             ? CR_LT \
             : v1 > v2 \
               ? CR_GT \
               : CR_EQ \
           : CR_ERROR; \
  }

// type            storedtime
// data            local
// timestamp nano  local
// timestamptz     utc + tzid
// timestampltz    utc + tzid
//datetimetc VS otimestamptc
#define DEFINE_CMP_OP_FUNC_DT_OT(op, op_str) \
  template <> inline \
  int ObObjCmpFuncs::cmp_op_func<ObDateTimeTC, ObOTimestampTC, op>(const ObObj &obj1, \
                                                                   const ObObj &obj2, \
                                                                   const ObCompareCtx &cmp_ctx) \
  { \
    UNUSED(cmp_ctx); \
    OBJ_TYPE_CLASS_CHECK(obj1, ObDateTimeTC);\
    OBJ_TYPE_CLASS_CHECK(obj2, ObOTimestampTC); \
    ObCmpRes ret = CR_FALSE; \
    ObOTimestampData v1; \
    v1.time_us_ = obj1.get_datetime();\
    ObOTimestampData v2 = obj2.get_otimestamp_value();\
    if (!obj2.is_timestamp_nano()) { \
      if (OB_UNLIKELY(INVALID_TZ_OFF == cmp_ctx.tz_off_)) {\
        LOG_ERROR("invalid timezone offset", K(obj1), K(obj2)); \
        ret = CR_ERROR; \
      } else {\
        v1.time_us_ -= cmp_ctx.tz_off_;\
      }\
    }\
    return (CR_ERROR != ret ? static_cast<int>(v1 op_str v2) : CR_ERROR); \
  }

//datetimetc VS otimestamptc
#define DEFINE_CMP_FUNC_DT_OT() \
  template <> inline \
  int ObObjCmpFuncs::cmp_func<ObDateTimeTC, ObOTimestampTC>(const ObObj &obj1, \
                                                            const ObObj &obj2, \
                                                            const ObCompareCtx &cmp_ctx) \
  { \
    OBJ_TYPE_CLASS_CHECK(obj1, ObDateTimeTC);\
    OBJ_TYPE_CLASS_CHECK(obj2, ObOTimestampTC);\
    ObCmpRes ret = CR_FALSE;\
    ObOTimestampData v1; \
    v1.time_us_ = obj1.get_datetime();\
    ObOTimestampData v2 = obj2.get_otimestamp_value(); \
    if (!obj2.is_timestamp_nano()) { \
      if (OB_UNLIKELY(INVALID_TZ_OFF == cmp_ctx.tz_off_)) {\
        LOG_ERROR("invalid timezone offset", K(obj1), K(obj2)); \
        ret = CR_ERROR; \
      } else {\
        v1.time_us_ -= cmp_ctx.tz_off_;\
      }\
    }\
    return (CR_ERROR != ret \
            ? (v1 < v2 \
                ? CR_LT \
                : (v1 > v2 \
                   ? CR_GT \
                   : CR_EQ))\
            : CR_ERROR);\
  }

// otimestamptc VS datetimetc
#define DEFINE_CMP_OP_FUNC_OT_DT(op, op_str)                                    \
  template <>                                                                   \
  inline int ObObjCmpFuncs::cmp_op_func<ObOTimestampTC, ObDateTimeTC, op>(      \
      const ObObj& obj1, const ObObj& obj2, const ObCompareCtx& cmp_ctx)        \
  {                                                                             \
    UNUSED(cmp_ctx);                                                            \
    OBJ_TYPE_CLASS_CHECK(obj1, ObOTimestampTC);                                 \
    OBJ_TYPE_CLASS_CHECK(obj2, ObDateTimeTC);                                   \
    ObCmpRes ret = CR_FALSE;                                                    \
    ObOTimestampData v1 = obj1.get_otimestamp_value();                          \
    ObOTimestampData v2;                                                        \
    v2.time_us_ = obj2.get_datetime();                                          \
    if (!obj1.is_timestamp_nano()) {                                            \
      if (OB_UNLIKELY(INVALID_TZ_OFF == cmp_ctx.tz_off_)) {                     \
        LOG_ERROR("invalid timezone offset", K(obj1), K(obj2));                 \
        ret = CR_ERROR;                                                         \
      } else {                                                                  \
        v2.time_us_ -= cmp_ctx.tz_off_;                                         \
      }                                                                         \
    }                                                                           \
    return (CR_ERROR != ret ? static_cast<int>(v1 op_str v2) : CR_ERROR); \
  }

// otimestamptc VS datetimetc
#define DEFINE_CMP_FUNC_OT_DT()                                                                \
  template <>                                                                                  \
  inline int ObObjCmpFuncs::cmp_func<ObOTimestampTC, ObDateTimeTC>(                            \
      const ObObj& obj1, const ObObj& obj2, const ObCompareCtx& cmp_ctx)                       \
  {                                                                                            \
    OBJ_TYPE_CLASS_CHECK(obj1, ObOTimestampTC);                                                \
    OBJ_TYPE_CLASS_CHECK(obj2, ObDateTimeTC);                                                  \
    ObCmpRes ret = CR_FALSE;                                                                   \
    ObOTimestampData v1 = obj1.get_otimestamp_value();                                         \
    ObOTimestampData v2;                                                                       \
    v2.time_us_ = obj2.get_datetime();                                                         \
    if (!obj1.is_timestamp_nano()) {                                                           \
      if (OB_UNLIKELY(INVALID_TZ_OFF == cmp_ctx.tz_off_)) {                                    \
        LOG_ERROR("invalid timezone offset", K(obj1), K(obj2));                                \
        ret = CR_ERROR;                                                                        \
      } else {                                                                                 \
        v2.time_us_ -= cmp_ctx.tz_off_;                                                        \
      }                                                                                        \
    }                                                                                          \
    return (CR_ERROR != ret ? (v1 < v2 ? CR_LT : (v1 > v2 ? CR_GT : CR_EQ)) : CR_ERROR); \
  }

// otimestamptc VS otimestamptc
#define DEFINE_CMP_OP_FUNC_OT_OT(op, op_str)                                    \
  template <>                                                                   \
  inline int ObObjCmpFuncs::cmp_op_func<ObOTimestampTC, ObOTimestampTC, op>(    \
      const ObObj& obj1, const ObObj& obj2, const ObCompareCtx& cmp_ctx)        \
  {                                                                             \
    UNUSED(cmp_ctx);                                                            \
    OBJ_TYPE_CLASS_CHECK(obj1, ObOTimestampTC);                                 \
    OBJ_TYPE_CLASS_CHECK(obj2, ObOTimestampTC);                                 \
    ObCmpRes ret = CR_FALSE;                                                    \
    ObOTimestampData v1 = obj1.get_otimestamp_value();                          \
    ObOTimestampData v2 = obj2.get_otimestamp_value();                          \
    if (obj1.is_timestamp_nano() != obj2.is_timestamp_nano()) {                 \
      if (OB_UNLIKELY(INVALID_TZ_OFF == cmp_ctx.tz_off_)) {                     \
        LOG_ERROR("invalid timezone offset", K(obj1), K(obj2));                 \
        ret = CR_ERROR;                                                         \
      } else {                                                                  \
        if (obj1.is_timestamp_nano()) {                                         \
          v1.time_us_ -= cmp_ctx.tz_off_;                                       \
        } else {                                                                \
          v2.time_us_ -= cmp_ctx.tz_off_;                                       \
        }                                                                       \
      }                                                                         \
    }                                                                           \
    return (CR_ERROR != ret ? static_cast<int>(v1 op_str v2) : CR_ERROR); \
  }

// otimestamptc VS otimestamptc
#define DEFINE_CMP_FUNC_OT_OT()                                                                \
  template <>                                                                                  \
  inline int ObObjCmpFuncs::cmp_func<ObOTimestampTC, ObOTimestampTC>(                          \
      const ObObj& obj1, const ObObj& obj2, const ObCompareCtx& cmp_ctx)                       \
  {                                                                                            \
    OBJ_TYPE_CLASS_CHECK(obj1, ObOTimestampTC);                                                \
    OBJ_TYPE_CLASS_CHECK(obj2, ObOTimestampTC);                                                \
    ObCmpRes ret = CR_FALSE;                                                                   \
    ObOTimestampData v1 = obj1.get_otimestamp_value();                                         \
    ObOTimestampData v2 = obj2.get_otimestamp_value();                                         \
    if (obj1.is_timestamp_nano() != obj2.is_timestamp_nano()) {                                \
      if (OB_UNLIKELY(INVALID_TZ_OFF == cmp_ctx.tz_off_)) {                                    \
        LOG_ERROR("invalid timezone offset", K(obj1), K(obj2));                                \
        ret = CR_ERROR;                                                                        \
      } else if (obj1.is_timestamp_nano()) {                                                   \
        v1.time_us_ -= cmp_ctx.tz_off_;                                                        \
      } else {                                                                                 \
        v2.time_us_ -= cmp_ctx.tz_off_;                                                        \
      }                                                                                        \
    }                                                                                          \
    return (CR_ERROR != ret ? (v1 < v2 ? CR_LT : (v1 > v2 ? CR_GT : CR_EQ)) : CR_ERROR); \
  }

//==============================

#define DEFINE_CMP_FUNCS(tc, type) \
  DEFINE_CMP_OP_FUNC(tc, type, CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC(tc, type, CO_LE, <=); \
  DEFINE_CMP_OP_FUNC(tc, type, CO_LT, < ); \
  DEFINE_CMP_OP_FUNC(tc, type, CO_GE, >=); \
  DEFINE_CMP_OP_FUNC(tc, type, CO_GT, > ); \
  DEFINE_CMP_OP_FUNC(tc, type, CO_NE, !=); \
  DEFINE_CMP_FUNC(tc, type)

#define DEFINE_CMP_FUNCS_XXX_REAL(tc, type, real_tc, real_type) \
  DEFINE_CMP_OP_FUNC_XXX_REAL(tc, type, real_tc, real_type, CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_XXX_REAL(tc, type, real_tc, real_type, CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_XXX_REAL(tc, type, real_tc, real_type, CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_XXX_REAL(tc, type, real_tc, real_type, CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_XXX_REAL(tc, type, real_tc, real_type, CO_GT, > ); \
  DEFINE_CMP_OP_FUNC_XXX_REAL(tc, type, real_tc, real_type, CO_NE, !=); \
  DEFINE_CMP_FUNC_XXX_REAL(tc, type, real_tc, real_type); \

#define DEFINE_CMP_FUNCS_REAL_XXX(real_tc, real_type, tc, type) \
  DEFINE_CMP_OP_FUNC_REAL_XXX(real_tc, real_type, tc, type, CO_EQ, CO_EQ); \
  DEFINE_CMP_OP_FUNC_REAL_XXX(real_tc, real_type, tc, type, CO_LE, CO_GE); \
  DEFINE_CMP_OP_FUNC_REAL_XXX(real_tc, real_type, tc, type, CO_LT, CO_GT); \
  DEFINE_CMP_OP_FUNC_REAL_XXX(real_tc, real_type, tc, type, CO_GE, CO_LE); \
  DEFINE_CMP_OP_FUNC_REAL_XXX(real_tc, real_type, tc, type, CO_GT, CO_LT); \
  DEFINE_CMP_OP_FUNC_REAL_XXX(real_tc, real_type, tc, type, CO_NE, CO_NE); \
  DEFINE_CMP_FUNC_REAL_XXX(real_tc, real_type, tc, type); \

#define DEFINE_CMP_FUNCS_XXX_NUMBER(tc, type) \
  DEFINE_CMP_OP_FUNC_XXX_NUMBER(tc, type, CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_XXX_NUMBER(tc, type, CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_XXX_NUMBER(tc, type, CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_XXX_NUMBER(tc, type, CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_XXX_NUMBER(tc, type, CO_GT, > ); \
  DEFINE_CMP_OP_FUNC_XXX_NUMBER(tc, type, CO_NE, !=); \
  DEFINE_CMP_FUNC_XXX_NUMBER(tc, type); \

#define DEFINE_CMP_FUNCS_NUMBER_XXX(tc, type) \
  DEFINE_CMP_OP_FUNC_NUMBER_XXX(tc, type, CO_EQ, CO_EQ); \
  DEFINE_CMP_OP_FUNC_NUMBER_XXX(tc, type, CO_LE, CO_GE); \
  DEFINE_CMP_OP_FUNC_NUMBER_XXX(tc, type, CO_LT, CO_GT); \
  DEFINE_CMP_OP_FUNC_NUMBER_XXX(tc, type, CO_GE, CO_LE); \
  DEFINE_CMP_OP_FUNC_NUMBER_XXX(tc, type, CO_GT, CO_LT); \
  DEFINE_CMP_OP_FUNC_NUMBER_XXX(tc, type, CO_NE, CO_NE); \
  DEFINE_CMP_FUNC_NUMBER_XXX(tc, type); \

//==============================

#define DEFINE_CMP_FUNCS_NULL_NULL() \
  DEFINE_CMP_OP_FUNC_NULL_NULL(CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_NULL_NULL(CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_NULL_NULL(CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_NULL_NULL(CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_NULL_NULL(CO_GT, > ); \
  DEFINE_CMP_OP_FUNC_NULL_NULL(CO_NE, !=); \
  DEFINE_CMP_FUNC_NULL_NULL()

#define DEFINE_CMP_FUNCS_NULL_EXT() \
  DEFINE_CMP_OP_FUNC_NULL_EXT(CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_NULL_EXT(CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_NULL_EXT(CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_NULL_EXT(CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_NULL_EXT(CO_GT, > ); \
  DEFINE_CMP_OP_FUNC_NULL_EXT(CO_NE, !=); \
  DEFINE_CMP_FUNC_NULL_EXT()

#define DEFINE_CMP_FUNCS_INT_INT() \
  DEFINE_CMP_FUNCS(ObIntTC, int);

#define DEFINE_CMP_FUNCS_INT_UINT() \
  DEFINE_CMP_OP_FUNC_INT_UINT(CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_INT_UINT(CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_INT_UINT(CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_INT_UINT(CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_INT_UINT(CO_GT, > ); \
  DEFINE_CMP_OP_FUNC_INT_UINT(CO_NE, !=); \
  DEFINE_CMP_FUNC_INT_UINT()

#define DEFINE_CMP_FUNCS_INT_FLOAT() \
  DEFINE_CMP_FUNCS_XXX_REAL(ObIntTC, int, ObFloatTC, float);

#define DEFINE_CMP_FUNCS_INT_DOUBLE() \
  DEFINE_CMP_FUNCS_XXX_REAL(ObIntTC, int, ObDoubleTC, double);

#define DEFINE_CMP_FUNCS_INT_NUMBER() \
  DEFINE_CMP_FUNCS_XXX_NUMBER(ObIntTC, int);

#define DEFINE_CMP_FUNCS_UINT_INT() \
  DEFINE_CMP_OP_FUNC_UINT_INT(CO_EQ, CO_EQ); \
  DEFINE_CMP_OP_FUNC_UINT_INT(CO_LE, CO_GE); \
  DEFINE_CMP_OP_FUNC_UINT_INT(CO_LT, CO_GT); \
  DEFINE_CMP_OP_FUNC_UINT_INT(CO_GE, CO_LE); \
  DEFINE_CMP_OP_FUNC_UINT_INT(CO_GT, CO_LT); \
  DEFINE_CMP_OP_FUNC_UINT_INT(CO_NE, CO_NE); \
  DEFINE_CMP_FUNC_UINT_INT()

#define DEFINE_CMP_FUNCS_UINT_UINT() \
  DEFINE_CMP_FUNCS(ObUIntTC, uint64);

#define DEFINE_CMP_FUNCS_UINT_FLOAT() \
  DEFINE_CMP_FUNCS_XXX_REAL(ObUIntTC, uint64, ObFloatTC, float);

#define DEFINE_CMP_FUNCS_UINT_DOUBLE() \
  DEFINE_CMP_FUNCS_XXX_REAL(ObUIntTC, uint64, ObDoubleTC, double);

#define DEFINE_CMP_FUNCS_UINT_NUMBER() \
  DEFINE_CMP_FUNCS_XXX_NUMBER(ObUIntTC, uint64);

#define DEFINE_CMP_FUNCS_FLOAT_INT() \
  DEFINE_CMP_FUNCS_REAL_XXX(ObFloatTC, float, ObIntTC, int);

#define DEFINE_CMP_FUNCS_FLOAT_UINT() \
  DEFINE_CMP_FUNCS_REAL_XXX(ObFloatTC, float, ObUIntTC, uint64);

#define DEFINE_CMP_FUNCS_FLOAT_FLOAT() \
  DEFINE_CMP_FUNCS(ObFloatTC, float);

#define DEFINE_CMP_FUNCS_FLOAT_DOUBLE() \
  DEFINE_CMP_FUNCS_XXX_REAL(ObFloatTC, float, ObDoubleTC, double);

#define DEFINE_CMP_FUNCS_DOUBLE_INT() \
  DEFINE_CMP_FUNCS_REAL_XXX(ObDoubleTC, double, ObIntTC, int);

#define DEFINE_CMP_FUNCS_DOUBLE_UINT() \
  DEFINE_CMP_FUNCS_REAL_XXX(ObDoubleTC, double, ObUIntTC, uint64);

#define DEFINE_CMP_FUNCS_DOUBLE_FLOAT() \
  DEFINE_CMP_FUNCS_REAL_XXX(ObDoubleTC, double, ObFloatTC, float);

#define DEFINE_CMP_FUNCS_DOUBLE_DOUBLE() \
  DEFINE_CMP_FUNCS(ObDoubleTC, double);

#define DEFINE_CMP_FUNCS_NUMBER_INT() \
  DEFINE_CMP_FUNCS_NUMBER_XXX(ObIntTC, int);

#define DEFINE_CMP_FUNCS_NUMBER_UINT() \
  DEFINE_CMP_FUNCS_NUMBER_XXX(ObUIntTC, uint64);

#define DEFINE_CMP_FUNCS_NUMBER_NUMBER() \
  DEFINE_CMP_FUNCS(ObNumberTC, number);

#define DEFINE_CMP_FUNCS_DATETIME_DATETIME() \
    DEFINE_CMP_OP_FUNC_DT_DT(CO_EQ, ==); \
    DEFINE_CMP_OP_FUNC_DT_DT(CO_LE, <=); \
    DEFINE_CMP_OP_FUNC_DT_DT(CO_LT, < ); \
    DEFINE_CMP_OP_FUNC_DT_DT(CO_GE, >=); \
    DEFINE_CMP_OP_FUNC_DT_DT(CO_GT, > ); \
    DEFINE_CMP_OP_FUNC_DT_DT(CO_NE, !=); \
    DEFINE_CMP_FUNC_DT_DT(); \

#define DEFINE_CMP_FUNCS_DATETIME_OTIMESTAMP() \
    DEFINE_CMP_OP_FUNC_DT_OT(CO_EQ, ==); \
    DEFINE_CMP_OP_FUNC_DT_OT(CO_LE, <=); \
    DEFINE_CMP_OP_FUNC_DT_OT(CO_LT, < ); \
    DEFINE_CMP_OP_FUNC_DT_OT(CO_GE, >=); \
    DEFINE_CMP_OP_FUNC_DT_OT(CO_GT, > ); \
    DEFINE_CMP_OP_FUNC_DT_OT(CO_NE, !=); \
    DEFINE_CMP_FUNC_DT_OT(); \

#define DEFINE_CMP_FUNCS_DATE_DATE() \
  DEFINE_CMP_FUNCS(ObDateTC, date);

#define DEFINE_CMP_FUNCS_TIME_TIME() \
  DEFINE_CMP_FUNCS(ObTimeTC, time);

#define DEFINE_CMP_FUNCS_YEAR_YEAR() \
  DEFINE_CMP_FUNCS(ObYearTC, year);

  
#define DEFINE_CMP_FUNCS_OTIMESTAMP_DATETIME() \
    DEFINE_CMP_OP_FUNC_OT_DT(CO_EQ, ==);         \
    DEFINE_CMP_OP_FUNC_OT_DT(CO_LE, <=);         \
    DEFINE_CMP_OP_FUNC_OT_DT(CO_LT, <);          \
    DEFINE_CMP_OP_FUNC_OT_DT(CO_GE, >=);         \
    DEFINE_CMP_OP_FUNC_OT_DT(CO_GT, >);          \
    DEFINE_CMP_OP_FUNC_OT_DT(CO_NE, !=);         \
    DEFINE_CMP_FUNC_OT_DT();
  
#define DEFINE_CMP_FUNCS_OTIMESTAMP_OTIMESTAMP() \
    DEFINE_CMP_OP_FUNC_OT_OT(CO_EQ, ==);           \
    DEFINE_CMP_OP_FUNC_OT_OT(CO_LE, <=);           \
    DEFINE_CMP_OP_FUNC_OT_OT(CO_LT, <);            \
    DEFINE_CMP_OP_FUNC_OT_OT(CO_GE, >=);           \
    DEFINE_CMP_OP_FUNC_OT_OT(CO_GT, >);            \
    DEFINE_CMP_OP_FUNC_OT_OT(CO_NE, !=);           \
    DEFINE_CMP_FUNC_OT_OT();

#define DEFINE_CMP_FUNCS_STRING_STRING() \
  DEFINE_CMP_OP_FUNC_STRING_STRING(CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_STRING_STRING(CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_STRING_STRING(CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_STRING_STRING(CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_STRING_STRING(CO_GT, > ); \
  DEFINE_CMP_OP_FUNC_STRING_STRING(CO_NE, !=); \
  DEFINE_CMP_FUNC_STRING_STRING()

#define DEFINE_CMP_FUNCS_EXT_NULL() \
  DEFINE_CMP_OP_FUNC_EXT_NULL(CO_EQ, CO_EQ); \
  DEFINE_CMP_OP_FUNC_EXT_NULL(CO_LE, CO_GE); \
  DEFINE_CMP_OP_FUNC_EXT_NULL(CO_LT, CO_GT); \
  DEFINE_CMP_OP_FUNC_EXT_NULL(CO_GE, CO_LE); \
  DEFINE_CMP_OP_FUNC_EXT_NULL(CO_GT, CO_LT); \
  DEFINE_CMP_OP_FUNC_EXT_NULL(CO_NE, CO_NE); \
  DEFINE_CMP_FUNC_EXT_NULL()

#define DEFINE_CMP_FUNCS_EXT_EXT() \
  DEFINE_CMP_OP_FUNC(ObExtendTC, ext, CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_EXT_EXT(CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_EXT_EXT(CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_EXT_EXT(CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_EXT_EXT(CO_GT, > ); \
  DEFINE_CMP_OP_FUNC(ObExtendTC, ext, CO_NE, !=); \
  DEFINE_CMP_FUNC_EXT_EXT()

#define DEFINE_CMP_FUNCS_NULL_XXX() \
  DEFINE_CMP_OP_FUNC_NULL_XXX(CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_NULL_XXX(CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_NULL_XXX(CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_NULL_XXX(CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_NULL_XXX(CO_GT, > ); \
  DEFINE_CMP_OP_FUNC_NULL_XXX(CO_NE, !=); \
  DEFINE_CMP_FUNC_NULL_XXX()

#define DEFINE_CMP_FUNCS_XXX_NULL() \
  DEFINE_CMP_OP_FUNC_XXX_NULL(CO_EQ, CO_EQ); \
  DEFINE_CMP_OP_FUNC_XXX_NULL(CO_LE, CO_GE); \
  DEFINE_CMP_OP_FUNC_XXX_NULL(CO_LT, CO_GT); \
  DEFINE_CMP_OP_FUNC_XXX_NULL(CO_GE, CO_LE); \
  DEFINE_CMP_OP_FUNC_XXX_NULL(CO_GT, CO_LT); \
  DEFINE_CMP_OP_FUNC_XXX_NULL(CO_NE, CO_NE); \
  DEFINE_CMP_FUNC_XXX_NULL()

#define DEFINE_CMP_FUNCS_XXX_EXT() \
  DEFINE_CMP_OP_FUNC_XXX_EXT(CO_EQ, ==); \
  DEFINE_CMP_OP_FUNC_XXX_EXT(CO_LE, <=); \
  DEFINE_CMP_OP_FUNC_XXX_EXT(CO_LT, < ); \
  DEFINE_CMP_OP_FUNC_XXX_EXT(CO_GE, >=); \
  DEFINE_CMP_OP_FUNC_XXX_EXT(CO_GT, > ); \
  DEFINE_CMP_OP_FUNC_XXX_EXT(CO_NE, !=); \
  DEFINE_CMP_FUNC_XXX_EXT()

#define DEFINE_CMP_FUNCS_EXT_XXX() \
  DEFINE_CMP_OP_FUNC_EXT_XXX(CO_EQ, CO_EQ); \
  DEFINE_CMP_OP_FUNC_EXT_XXX(CO_LE, CO_GE); \
  DEFINE_CMP_OP_FUNC_EXT_XXX(CO_LT, CO_GT); \
  DEFINE_CMP_OP_FUNC_EXT_XXX(CO_GE, CO_LE); \
  DEFINE_CMP_OP_FUNC_EXT_XXX(CO_GT, CO_LT); \
  DEFINE_CMP_OP_FUNC_EXT_XXX(CO_NE, CO_NE); \
  DEFINE_CMP_FUNC_EXT_XXX()

#define DEFINE_CMP_FUNCS_UNKNOWN_UNKNOWN() \
  DEFINE_CMP_FUNCS(ObUnknownTC, unknown);

//==============================

DEFINE_CMP_FUNCS_NULL_NULL();
DEFINE_CMP_FUNCS_NULL_EXT();

DEFINE_CMP_FUNCS_INT_INT();
DEFINE_CMP_FUNCS_INT_UINT();
DEFINE_CMP_FUNCS_INT_FLOAT();
DEFINE_CMP_FUNCS_INT_DOUBLE();
DEFINE_CMP_FUNCS_INT_NUMBER();

DEFINE_CMP_FUNCS_UINT_INT();
DEFINE_CMP_FUNCS_UINT_UINT();
DEFINE_CMP_FUNCS_UINT_FLOAT();
DEFINE_CMP_FUNCS_UINT_DOUBLE();
DEFINE_CMP_FUNCS_UINT_NUMBER();

DEFINE_CMP_FUNCS_FLOAT_INT();
DEFINE_CMP_FUNCS_FLOAT_UINT();
DEFINE_CMP_FUNCS_FLOAT_FLOAT();
DEFINE_CMP_FUNCS_FLOAT_DOUBLE();

DEFINE_CMP_FUNCS_DOUBLE_INT();
DEFINE_CMP_FUNCS_DOUBLE_UINT();
DEFINE_CMP_FUNCS_DOUBLE_FLOAT();
DEFINE_CMP_FUNCS_DOUBLE_DOUBLE();

DEFINE_CMP_FUNCS_NUMBER_INT();
DEFINE_CMP_FUNCS_NUMBER_UINT();
DEFINE_CMP_FUNCS_NUMBER_NUMBER();

DEFINE_CMP_FUNCS_DATETIME_DATETIME();
DEFINE_CMP_FUNCS_DATETIME_OTIMESTAMP();

DEFINE_CMP_FUNCS_OTIMESTAMP_DATETIME();
DEFINE_CMP_FUNCS_OTIMESTAMP_OTIMESTAMP();

DEFINE_CMP_FUNCS_DATE_DATE();
DEFINE_CMP_FUNCS_TIME_TIME();
DEFINE_CMP_FUNCS_YEAR_YEAR();
DEFINE_CMP_FUNCS_STRING_STRING();

DEFINE_CMP_FUNCS_EXT_NULL();
DEFINE_CMP_FUNCS_EXT_EXT();

DEFINE_CMP_FUNCS_UNKNOWN_UNKNOWN();

DEFINE_CMP_FUNCS_NULL_XXX();
DEFINE_CMP_FUNCS_XXX_NULL();
DEFINE_CMP_FUNCS_XXX_EXT();
DEFINE_CMP_FUNCS_EXT_XXX();

#define DEFINE_CMP_FUNCS_ENTRY(tc1, tc2) \
{ \
  ObObjCmpFuncs::cmp_op_func<tc1, tc2, CO_EQ>, \
  ObObjCmpFuncs::cmp_op_func<tc1, tc2, CO_LE>, \
  ObObjCmpFuncs::cmp_op_func<tc1, tc2, CO_LT>, \
  ObObjCmpFuncs::cmp_op_func<tc1, tc2, CO_GE>, \
  ObObjCmpFuncs::cmp_op_func<tc1, tc2, CO_GT>, \
  ObObjCmpFuncs::cmp_op_func<tc1, tc2, CO_NE>, \
  ObObjCmpFuncs::cmp_func<tc1, tc2> \
}

#define DEFINE_CMP_FUNCS_ENTRY_NULL   {NULL, NULL, NULL, NULL, NULL, NULL, NULL}

const obj_cmp_func ObObjCmpFuncs::cmp_funcs[ObMaxTC][ObMaxTC][CO_MAX] =
{
  { // null
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObExtendTC),
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),  //text
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),  //bit
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),  //setenun
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),  //setenuninner
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),  //otimestamp
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),  //raw
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),  //interval
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),  //rowid
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),  //lob
  },
  { // int
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY(ObIntTC, ObIntTC),
    DEFINE_CMP_FUNCS_ENTRY(ObIntTC, ObUIntTC),
    DEFINE_CMP_FUNCS_ENTRY(ObIntTC, ObFloatTC),
    DEFINE_CMP_FUNCS_ENTRY(ObIntTC, ObDoubleTC),
    DEFINE_CMP_FUNCS_ENTRY(ObIntTC, ObNumberTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
  },
  { // uint
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY(ObUIntTC, ObIntTC),
    DEFINE_CMP_FUNCS_ENTRY(ObUIntTC, ObUIntTC),
    DEFINE_CMP_FUNCS_ENTRY(ObUIntTC, ObFloatTC),
    DEFINE_CMP_FUNCS_ENTRY(ObUIntTC, ObDoubleTC),
    DEFINE_CMP_FUNCS_ENTRY(ObUIntTC, ObNumberTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
  },
  { // float
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY(ObFloatTC, ObIntTC),
    DEFINE_CMP_FUNCS_ENTRY(ObFloatTC, ObUIntTC),
    DEFINE_CMP_FUNCS_ENTRY(ObFloatTC, ObFloatTC),
    DEFINE_CMP_FUNCS_ENTRY(ObFloatTC, ObDoubleTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL, //text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
  },
  { // double
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY(ObDoubleTC, ObIntTC),
    DEFINE_CMP_FUNCS_ENTRY(ObDoubleTC, ObUIntTC),
    DEFINE_CMP_FUNCS_ENTRY(ObDoubleTC, ObFloatTC),
    DEFINE_CMP_FUNCS_ENTRY(ObDoubleTC, ObDoubleTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL, //text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
  },
  { // number
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY(ObNumberTC, ObIntTC),
    DEFINE_CMP_FUNCS_ENTRY(ObNumberTC, ObUIntTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY(ObNumberTC, ObNumberTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL, //text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,//enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
  },
  { // datetime
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY(ObDateTimeTC, ObDateTimeTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL, //text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY(ObDateTimeTC, ObOTimestampTC),//otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
  },
  { // date
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY(ObDateTC, ObDateTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL, //text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
  },
  { // time
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY(ObTimeTC, ObTimeTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL, //text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
  },
  { // year
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY(ObYearTC, ObYearTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL, //text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
  },
  { // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY(ObStringTC, ObStringTC),
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY(ObStringTC, ObStringTC), //text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
  },
  { // extend
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObExtendTC),
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC), //text
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),//enumset
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),//enumsetInner
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),//otimestamp
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),//raw
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),//interval
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC),//rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
  },
  { // unknown
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC),
    DEFINE_CMP_FUNCS_ENTRY(ObUnknownTC, ObUnknownTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL, //text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumsetInner
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
  },
  { // text
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY(ObStringTC, ObStringTC),  // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC),  // extend
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY(ObStringTC, ObStringTC), //  text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
  },
  {
    // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL, //null
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //extend
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
  },
  {
    // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL, //null
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //extend
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
  },
  {
    // enumsetinner
    DEFINE_CMP_FUNCS_ENTRY_NULL, //null
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //extend
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
  },
  {
    // otimestamp
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC),
    DEFINE_CMP_FUNCS_ENTRY_NULL,                           // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,                           // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,                           // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,                           // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,                           // number
    DEFINE_CMP_FUNCS_ENTRY(ObOTimestampTC, ObDateTimeTC),  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,                           // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,                           // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,                           // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,                           // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC),           // extend
    DEFINE_CMP_FUNCS_ENTRY_NULL,                           // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL,                           // text
    DEFINE_CMP_FUNCS_ENTRY_NULL,                           // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,                           // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,                           // enumsetinner
    DEFINE_CMP_FUNCS_ENTRY(ObOTimestampTC, ObOTimestampTC),// otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
  },
  {
    // raw
    DEFINE_CMP_FUNCS_ENTRY_NULL, //null
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //extend
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
  },
  {
    // interval
    DEFINE_CMP_FUNCS_ENTRY_NULL, //null
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //extend
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
  },
  {
    // rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL, //null
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //extend
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
  },
  {
    // lob
    DEFINE_CMP_FUNCS_ENTRY_NULL, //null
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // int
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // float
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // double
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //extend
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
  },
};

const ObObj ObObjCmpFuncs::cmp_res_objs_bool[CR_BOOL_CNT] =
{
  ObObj(false),
  ObObj(true),
  ObObj(ObNullType),
};

const ObObj ObObjCmpFuncs::cmp_res_objs_int[CR_INT_CNT] =
{
  ObObj(static_cast<int64_t>(-1)),
  ObObj(static_cast<int64_t>(0)),
  ObObj(static_cast<int64_t>(1)),
  ObObj(ObNullType)
};

bool ObObjCmpFuncs::can_compare_directly(ObObjType type1, ObObjType type2)
{
  ObObjTypeClass tc1 = ob_obj_type_class(type1);
  ObObjTypeClass tc2 = ob_obj_type_class(type2);
  return (tc1 == tc2 && !(ObDateTimeTC == tc1 && type1 != type2));
}

bool ObObjCmpFuncs::compare_oper_nullsafe(const ObObj &obj1,
                                          const ObObj &obj2,
                                          ObCollationType cs_type,
                                          ObCmpOp cmp_op)
{
  int cmp = CR_FALSE;
  ObObjTypeClass tc1 = obj1.get_type_class();
  ObObjTypeClass tc2 = obj2.get_type_class();
  // maybe we should not check tc1, tc2 and cmp_op,
  // because this function is so fundamental and performance related.
  if (OB_UNLIKELY(ob_is_invalid_obj_tc(tc1)
                  || ob_is_invalid_obj_tc(tc2)
                  || ob_is_invalid_cmp_op_bool(cmp_op))) {
    LOG_ERROR("invalid obj1 or obj2 or cmp_op", K(obj1), K(obj2), K(cmp_op));
    right_to_die_or_duty_to_live();
  } else {
    obj_cmp_func cmp_op_func = cmp_funcs[tc1][tc2][cmp_op];
    if (OB_ISNULL(cmp_op_func)) {
      LOG_ERROR("obj1 and obj2 can't compare", K(obj1), K(obj2), K(cmp_op));
      right_to_die_or_duty_to_live();
    } else {
      ObCompareCtx cmp_ctx(ObMaxType, cs_type, true, INVALID_TZ_OFF);
      if (OB_UNLIKELY(CR_ERROR == (cmp = cmp_op_func(obj1, obj2, cmp_ctx)))) {
        LOG_ERROR("failed to compare obj1 and obj2", K(obj1), K(obj2), K(cmp_op));
        right_to_die_or_duty_to_live();
      }
    }
  }
  return static_cast<bool>(cmp);
}

int ObObjCmpFuncs::compare_nullsafe(const ObObj &obj1,
                                    const ObObj &obj2,
                                    ObCollationType cs_type)
{
  int cmp = CR_EQ;
  ObObjTypeClass tc1 = obj1.get_type_class();
  ObObjTypeClass tc2 = obj2.get_type_class();
  // maybe we should not check tc1 and tc2,
  // because this function is so fundamental and performance related.
  if (OB_UNLIKELY(ob_is_invalid_obj_tc(tc1) || ob_is_invalid_obj_tc(tc2))) {
    LOG_ERROR("invalid obj1 or obj2", K(obj1), K(obj2));
    right_to_die_or_duty_to_live();
  } else {
    obj_cmp_func cmp_func = cmp_funcs[tc1][tc2][CO_CMP];
    if (OB_ISNULL(cmp_func)) {
      LOG_ERROR("obj1 and obj2 can't compare", K(obj1), K(obj2));
      right_to_die_or_duty_to_live();
    } else {
      ObCompareCtx cmp_ctx(ObMaxType, cs_type, true, INVALID_TZ_OFF);
      if (OB_UNLIKELY(CR_ERROR == (cmp = cmp_func(obj1, obj2, cmp_ctx)))) {
        LOG_ERROR("failed to compare obj1 and obj2", K(obj1), K(obj2));
        right_to_die_or_duty_to_live();
      }
    }
  }
  return cmp;
}

int ObObjCmpFuncs::compare(ObObj &result,
                           const ObObj &obj1,
                           const ObObj &obj2,
                           const ObCompareCtx &cmp_ctx,
                           ObCmpOp cmp_op,
                           bool &need_cast)
{
  int ret = OB_SUCCESS;
  ObObjTypeClass tc1 = obj1.get_type_class();
  ObObjTypeClass tc2 = obj2.get_type_class();
  obj_cmp_func cmp_op_func = NULL;
  need_cast = false;
  if (OB_UNLIKELY(ob_is_invalid_obj_tc(tc1) ||
                  ob_is_invalid_obj_tc(tc2) ||
                  ob_is_invalid_cmp_op(cmp_op))) {
    ret = OB_ERR_UNEXPECTED;
  } else if (NULL == (cmp_op_func = cmp_funcs[tc1][tc2][cmp_op])) {
    need_cast = true;
  } else {
    int cmp = cmp_op_func(obj1, obj2, cmp_ctx);
    if (OB_UNLIKELY(CR_ERROR == cmp)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to compare obj1 and obj2", K(ret), K(obj1), K(obj2), K(cmp_op));
    } else {
      // CR_LT is -1, CR_EQ is 0, so we add 1 to cmp_res_objs_int.
      result = (CO_CMP == cmp_op) ? (cmp_res_objs_int + 1)[cmp] : cmp_res_objs_bool[cmp];
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
