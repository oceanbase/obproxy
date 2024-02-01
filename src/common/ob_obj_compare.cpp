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
#include "lib/wide_integer/ob_wide_integer.h"
#include "lib/wide_integer/ob_wide_integer_cmp_funcs.h"

namespace oceanbase
{
namespace common
{

#define OBJ_TYPE_CLASS_CHECK(obj, tc)                                               \
  if (OB_UNLIKELY(obj.get_type_class() != tc)) {                                    \
    LOG_EDIAG("unexpected error. mismatch function for comparison", K(obj), K(tc)); \
    ret = CR_ERROR;                                                                 \
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

#define DEFINE_CMP_OP_FUNC_NUMBER_DECIMALINT(op, op_str)                                           \
  template <>                                                                                      \
  inline int ObObjCmpFuncs::cmp_op_func<ObNumberTC, ObDecimalIntTC, op>(                           \
    const ObObj &obj1, const ObObj &obj2, const ObCompareCtx &ctx)                                 \
  {                                                                                                \
    int ret = ObObjCmpFuncs::cmp_op_func<ObDecimalIntTC, ObNumberTC, op>(obj2, obj1, ctx);         \
    return -ret;                                                                                   \
  }

#define DEFINE_CMP_FUNC_DECIMALINT_NUMBER()                                                        \
  template <>                                                                                      \
  inline int ObObjCmpFuncs::cmp_func<ObDecimalIntTC, ObNumberTC>(                                  \
    const ObObj &obj1, const ObObj &obj2, const ObCompareCtx &)                                    \
  {                                                                                                \
    int ret = OB_SUCCESS;                                                                          \
    int cmp_res = 0;                                                                               \
    OBJ_TYPE_CLASS_CHECK(obj1, ObDecimalIntTC);                                                    \
    OBJ_TYPE_CLASS_CHECK(obj2, ObNumberTC);                                                        \
    ObDecimalInt *decint = nullptr;                                                                \
    int32_t int_bytes = 0;                                                                         \
    ObDecimalIntBuilder lh_val, rh_val;                                                            \
    number::ObNumber nmb = obj2.get_number();                                                      \
    ObScale lh_scale = obj1.get_scale(), rh_scale = static_cast<int16_t>(nmb.get_scale());           \
    ObDecimalIntBuilder tmp_alloc;                                                                 \
    if (OB_FAIL(wide::from_number(nmb, tmp_alloc, rh_scale, decint, int_bytes))) {                 \
      LOG_EDIAG("scale decimal int failed", K(ret));                                               \
    } else if (lh_scale < rh_scale) {                                                              \
      if (OB_FAIL(wide::common_scale_decimalint(obj1.get_decimal_int(), obj1.get_int_bytes(),      \
                                                lh_scale, rh_scale, lh_val))) {                    \
        LOG_EDIAG("scale decimal int failed", K(ret), K(lh_scale), K(rh_scale));                   \
      } else if (FALSE_IT(rh_val.from(decint, int_bytes))) {                                       \
      }                                                                                            \
    } else if (lh_scale > rh_scale) {                                                              \
      if (OB_FAIL(wide::common_scale_decimalint(decint, int_bytes, rh_scale, lh_scale, rh_val))) { \
        LOG_EDIAG("scale decimal int failed", K(ret), K(rh_scale), K(lh_scale));                   \
      } else if (FALSE_IT(lh_val.from(obj1.get_decimal_int(), obj1.get_int_bytes()))) {            \
      }                                                                                            \
    } else {                                                                                       \
      lh_val.from(obj1.get_decimal_int(), obj1.get_int_bytes());                                   \
      rh_val.from(decint, int_bytes);                                                              \
    }                                                                                              \
    if (OB_FAIL(ret)) {                                                                            \
    } else if (OB_FAIL(wide::compare(lh_val, rh_val, cmp_res))) {                                  \
      LOG_EDIAG("compare failed", K(ret));                                                         \
    }                                                                                              \
    return cmp_res;                                                                                \
  }

#define DEFINE_CMP_FUNC_NUMBER_DECIMALINT()                                                        \
  template <>                                                                                      \
  inline int ObObjCmpFuncs::cmp_func<ObNumberTC, ObDecimalIntTC>(                                  \
    const ObObj &obj1, const ObObj &obj2, const ObCompareCtx &ctx)                                 \
  {                                                                                                \
    return -ObObjCmpFuncs::cmp_func<ObDecimalIntTC, ObNumberTC>(obj2, obj1, ctx);                  \
  }

#define DEFINE_CMP_OP_FUNC_DECIMALINT_DECIMALINT(op, op_str)                                       \
  template <>                                                                                      \
  inline int ObObjCmpFuncs::cmp_op_func<ObDecimalIntTC, ObDecimalIntTC, op>(                       \
    const ObObj &obj1, const ObObj &obj2, const ObCompareCtx &)                                    \
  {                                                                                                \
    int ret = OB_SUCCESS;                                                                          \
    int val = 0;                                                                                   \
    int cmp_res = 0;                                                                               \
    OBJ_TYPE_CLASS_CHECK(obj1, ObDecimalIntTC);                                                    \
    OBJ_TYPE_CLASS_CHECK(obj2, ObDecimalIntTC);                                                    \
    int16_t lh_scale = obj1.get_scale(), rh_scale = obj2.get_scale();                              \
    ObDecimalIntBuilder lh_val, rh_val;                                                            \
    if (lh_scale < rh_scale) {                                                                     \
      if (OB_FAIL(wide::common_scale_decimalint(obj1.get_decimal_int(), obj1.get_int_bytes(),      \
                                                lh_scale, rh_scale, lh_val))) {                    \
        LOG_WDIAG("scale decimal int failed", K(ret));                                              \
      } else {                                                                                     \
        rh_val.from(obj2.get_decimal_int(), obj2.get_int_bytes());                                 \
      }                                                                                            \
    } else if (lh_scale > rh_scale) {                                                              \
      if (OB_FAIL(wide::common_scale_decimalint(obj2.get_decimal_int(), obj2.get_int_bytes(),      \
                                                rh_scale, lh_scale, rh_val))) {                    \
        LOG_WDIAG("scale decimal int failed", K(ret));                                              \
      } else {                                                                                     \
        lh_val.from(obj1.get_decimal_int(), obj1.get_int_bytes());                                 \
      }                                                                                            \
    } else {                                                                                       \
      lh_val.from(obj1.get_decimal_int(), obj1.get_int_bytes());                                   \
      rh_val.from(obj2.get_decimal_int(), obj2.get_int_bytes());                                   \
    }                                                                                              \
    if (OB_FAIL(ret)) {                                                                            \
    } else if (OB_FAIL(wide::compare(lh_val, rh_val, cmp_res))) {                                  \
      LOG_EDIAG("compare error", K(ret));                                                          \
    } else if (op == CO_CMP) {                                                                     \
      val = cmp_res;                                                                               \
    } else {                                                                                       \
      val = (cmp_res op_str 0);                                                                    \
    }                                                                                              \
    return val;                                                                                    \
  }

#define DEFINE_CMP_FUNC_DECIMALINT_DECIMALINT()                                                    \
  template <>                                                                                      \
  inline int ObObjCmpFuncs::cmp_func<ObDecimalIntTC, ObDecimalIntTC>(                              \
    const ObObj &obj1, const ObObj &obj2, const ObCompareCtx &ctx)                                 \
  {                                                                                                \
    int cmp_res =                                                                                  \
      ObObjCmpFuncs::cmp_op_func<ObDecimalIntTC, ObDecimalIntTC, CO_CMP>(obj1, obj2, ctx);         \
    return INT_TO_CR(cmp_res);                                                                     \
  }

#define DEFINE_CMP_OP_FUNC_DECIMALINT_INTEGER(op, op_str, tc, val_type, val_func)                  \
  template <>                                                                                      \
  inline int ObObjCmpFuncs::cmp_op_func<ObDecimalIntTC, tc, op>(                                   \
    const ObObj &obj1, const ObObj &obj2, const ObCompareCtx &)                                    \
  {                                                                                                \
    int ret = OB_SUCCESS;                                                                          \
    OBJ_TYPE_CLASS_CHECK(obj1, ObDecimalIntTC);                                                    \
    OBJ_TYPE_CLASS_CHECK(obj2, tc);                                                                \
    int cmp_res = 0;                                                                               \
    int ret_val = 0;                                                                               \
    val_type val = obj2.get_##val_func();                                                          \
    ObDecimalIntBuilder tmp_alloc;                                                                 \
    ObDecimalInt *rh_decint = nullptr;                                                             \
    int32_t rh_int_bytes = 0;                                                                      \
    int16_t lh_scale = obj1.get_scale(), rh_scale = 0;                                             \
    ObDecimalIntBuilder lh_val, rh_val;                                                            \
    if (OB_FAIL(wide::from_integer(val, tmp_alloc, rh_decint, rh_int_bytes))) {                    \
      LOG_EDIAG("from_integer failed", K(ret));                                                    \
    } else if (lh_scale < rh_scale) {                                                              \
      if (OB_FAIL(wide::common_scale_decimalint(obj1.get_decimal_int(), obj1.get_int_bytes(),      \
                                                lh_scale, rh_scale, lh_val))) {                    \
        LOG_WDIAG("scale decimal int failed", K(ret));                                              \
      } else {                                                                                     \
        rh_val.from(rh_decint, rh_int_bytes);                                                      \
      }                                                                                            \
    } else if (lh_scale > rh_scale) {                                                              \
      if (OB_FAIL(                                                                                 \
            wide::common_scale_decimalint(rh_decint, rh_int_bytes, rh_scale, lh_scale, rh_val))) { \
        LOG_WDIAG("scale decimal int failed", K(ret));                                              \
      } else {                                                                                     \
        lh_val.from(obj1.get_decimal_int(), obj1.get_int_bytes());                                 \
      }                                                                                            \
    } else {                                                                                       \
      lh_val.from(obj1.get_decimal_int(), obj1.get_int_bytes());                                   \
      rh_val.from(rh_decint, rh_int_bytes);                                                        \
    }                                                                                              \
    if (OB_FAIL(ret)) {                                                                            \
    } else if (OB_FAIL(wide::compare(lh_val, rh_val, cmp_res))) {                                  \
      LOG_WDIAG("compare failed", K(ret));                                                          \
    } else if (op == CO_CMP) {                                                                     \
      ret_val = cmp_res;                                                                           \
    } else {                                                                                       \
      ret_val = (cmp_res op_str 0);                                                                \
    }                                                                                              \
    return ret_val;                                                                                \
  }

#define DEFINE_CMP_FUNC_DECIMALINT_INTEGER(tc, val_type, val_func)                                 \
  template <>                                                                                      \
  inline int ObObjCmpFuncs::cmp_func<ObDecimalIntTC, tc>(const ObObj &obj1, const ObObj &obj2,     \
                                                         const ObCompareCtx &ctx)                  \
  {                                                                                                \
    int cmp_ret = ObObjCmpFuncs::cmp_op_func<ObDecimalIntTC, tc, CO_CMP>(obj1, obj2, ctx);         \
    return cmp_ret;                                                                                \
  }

#define DEFINE_CMP_OP_FUNC_DECIMALINT_NUMBER(op, op_str)                                           \
  template <>                                                                                      \
  inline int ObObjCmpFuncs::cmp_op_func<ObDecimalIntTC, ObNumberTC, op>(                           \
    const ObObj &obj1, const ObObj &obj2, const ObCompareCtx &)                                    \
  {                                                                                                \
    int ret = OB_SUCCESS;                                                                          \
    OBJ_TYPE_CLASS_CHECK(obj1, ObDecimalIntTC);                                                    \
    OBJ_TYPE_CLASS_CHECK(obj2, ObNumberTC);                                                        \
    int cmp_res = 0;                                                                               \
    int ret_val = 0;                                                                               \
    ObDecimalInt *decint = nullptr;                                                                \
    int32_t int_bytes = 0;                                                                         \
    number::ObNumber nmb = obj2.get_number();                                                      \
    int16_t lh_scale = obj1.get_scale(), rh_scale = (int16_t) nmb.get_scale();                               \
    ObDecimalIntBuilder tmp_alloc;                                                                 \
    ObDecimalIntBuilder lh_val;                                                                    \
    ObDecimalIntBuilder rh_val;                                                                    \
    if (OB_FAIL(wide::from_number(nmb, tmp_alloc, rh_scale, decint, int_bytes))) {                 \
      LOG_EDIAG("cast number to decimal int failed", K(ret));                                      \
    } else if (lh_scale < rh_scale) {                                                              \
      if (OB_FAIL(wide::common_scale_decimalint(obj1.get_decimal_int(), obj1.get_int_bytes(),      \
                                                lh_scale, rh_scale, lh_val))) {                    \
        LOG_EDIAG("scale decimal int failed", K(ret), K(lh_scale), K(rh_scale));                   \
      } else if (FALSE_IT(rh_val.from(decint, int_bytes))) {                                       \
      }                                                                                            \
    } else if (lh_scale > rh_scale) {                                                              \
      if (OB_FAIL(wide::common_scale_decimalint(decint, int_bytes, rh_scale, lh_scale, rh_val))) { \
        LOG_EDIAG("scale decimal int failed", K(ret), K(rh_scale), K(lh_scale));                   \
      } else if (FALSE_IT(lh_val.from(obj1.get_decimal_int(), obj1.get_int_bytes()))) {            \
      }                                                                                            \
    } else {                                                                                       \
      lh_val.from(obj1.get_decimal_int(), obj1.get_int_bytes());                                   \
      rh_val.from(decint, int_bytes);                                                              \
    }                                                                                              \
    if (OB_SUCC(ret)) {                                                                            \
      if (OB_FAIL(wide::compare(lh_val, rh_val, cmp_res))) {                                       \
        LOG_EDIAG("compare failed", K(ret));                                                       \
      } else {                                                                                     \
        ret_val = (cmp_res op_str 0);                                                              \
      }                                                                                            \
    }                                                                                              \
    return ret_val;                                                                                \
  }

#define DEFINE_CMP_OP_FUNC_INTEGER_DECIMALINT(op, op_str, tc)                                      \
  template <>                                                                                      \
  inline int ObObjCmpFuncs::cmp_op_func<tc, ObDecimalIntTC, op>(                                   \
    const ObObj &obj1, const ObObj &obj2, const ObCompareCtx &ctx)                                 \
  {                                                                                                \
    int ret = ObObjCmpFuncs::cmp_op_func<ObDecimalIntTC, tc, op>(obj2, obj1, ctx);                 \
    return -ret;                                                                                   \
  }

#define DEFINE_CMP_FUNC_INTEGER_DECIMALINT(tc)                                                     \
  template <>                                                                                      \
  inline int ObObjCmpFuncs::cmp_func<tc, ObDecimalIntTC>(const ObObj &obj1, const ObObj &obj2,     \
                                                         const ObCompareCtx &ctx)                  \
  {                                                                                                \
    return -ObObjCmpFuncs::cmp_func<ObDecimalIntTC, tc>(obj2, obj1, ctx);                          \
  }

#define DEFINE_CMP_OP_FUNC_ENUMSETINNER_DECIMALINT(op, op_str)                                     \
  template <>                                                                                      \
  int ObObjCmpFuncs::cmp_op_func<ObEnumSetInnerTC, ObDecimalIntTC, op>(                            \
    const ObObj &obj1, const ObObj &obj2, const ObCompareCtx &)                                    \
  {                                                                                                \
    ObEnumSetInnerValue inner_value;                                                               \
    int cmp_res = 0;                                                                               \
    int ret_val = 0;                                                                               \
    int ret = OB_SUCCESS;                                                                          \
    OBJ_TYPE_CLASS_CHECK(obj1, ObEnumSetInnerTC);                                                  \
    OBJ_TYPE_CLASS_CHECK(obj2, ObDecimalIntTC);                                                    \
    ObScale lh_scale = 0, rh_scale = obj2.get_scale();                                             \
    ObDecimalIntBuilder lh_val, rh_val;                                                            \
    ObDecimalIntBuilder tmp_alloc;                                                                 \
    ObDecimalInt *decint = nullptr;                                                                \
    uint64_t obj1_value = 0;                                                                       \
    int32_t int_bytes = 0;                                                                         \
    if (OB_FAIL(obj1.get_enumset_inner_value(inner_value))) {                                      \
      ret_val = CR_OB_ERROR;                                                                       \
    } else if (FALSE_IT(obj1_value = inner_value.numberic_value_)) {                               \
    } else if (OB_FAIL(wide::from_integer(obj1_value, tmp_alloc, decint, int_bytes))) {            \
      LOG_EDIAG("cast integer to decimal int failed", K(ret));                                     \
      ret_val = CR_OB_ERROR;                                                                       \
    } else if (lh_scale > rh_scale) {                                                              \
      if (OB_FAIL(wide::common_scale_decimalint(obj2.get_decimal_int(), obj2.get_int_bytes(),      \
                                                rh_scale, lh_scale, rh_val))) {                    \
        LOG_EDIAG("scale decimal int failed", K(ret));                                             \
        ret_val = CR_OB_ERROR;                                                                     \
      } else {                                                                                     \
        rh_val.from(decint, int_bytes);                                                            \
      }                                                                                            \
    } else if (lh_scale < rh_scale) {                                                              \
      if (OB_FAIL(wide::common_scale_decimalint(decint, int_bytes, lh_scale, rh_scale, lh_val))) { \
        LOG_EDIAG("scale decimal int failed", K(ret));                                             \
      } else {                                                                                     \
        rh_val.from(obj2.get_decimal_int(), obj2.get_int_bytes());                                 \
      }                                                                                            \
    } else {                                                                                       \
      lh_val.from(decint, int_bytes);                                                              \
      rh_val.from(obj2.get_decimal_int(), obj2.get_int_bytes());                                   \
    }                                                                                              \
    if (OB_SUCC(ret)) {                                                                            \
      if (OB_FAIL(wide::compare(lh_val, rh_val, cmp_res))) {                                       \
        LOG_EDIAG("compare failed", K(ret));                                                       \
        ret_val = CR_OB_ERROR;                                                                     \
      } else {                                                                                     \
        ret_val = (cmp_res op_str 0);                                                              \
      }                                                                                            \
    }                                                                                              \
    return ret_val;                                                                                \
  }

#define DEFINE_CMP_OP_FUNC_JSON_JSON(op, op_str)                                                \
  template <> inline                                                                            \
  int ObObjCmpFuncs::cmp_op_func<ObJsonTC, ObJsonTC, op>(const ObObj &obj1,                     \
                                                         const ObObj &obj2,                     \
                                                         const ObCompareCtx &cmp_ctx)           \
  {                                                                                             \
    int cmp_ret = CR_OB_ERROR;                                                                  \
    int ret = OB_SUCCESS;                                                                       \
    int result = 0;                                                                             \
    OBJ_TYPE_CLASS_CHECK(obj1, ObJsonTC);                                                       \
    OBJ_TYPE_CLASS_CHECK(obj2, ObJsonTC);                                                       \
    UNUSED(cmp_ctx);                                                                            \
    ObString data_str1;                                                                         \
    ObString data_str2;                                                                         \
    if (obj1.is_outrow_lob() || obj2.is_outrow_lob()) {                                         \
      ret = OB_NOT_SUPPORTED;                                                                   \
      LOG_EDIAG("not support outrow json lobs", K(ret), K(obj1), K(obj2));                      \
    } else if (OB_FAIL(obj1.get_string(data_str1))) {                                           \
      ret = OB_ERR_UNEXPECTED;                                                                  \
      LOG_EDIAG("invalid json lob object1", K(ret),                                             \
                K(obj1.get_collation_type()), K(obj2.get_collation_type()),                     \
                K(obj1), K(obj2));                                                              \
    } else if (OB_FAIL(obj2.get_string(data_str2))) {                                           \
      ret = OB_ERR_UNEXPECTED;                                                                  \
      LOG_EDIAG("invalid json lob object2", K(ret),                                             \
                K(obj1.get_collation_type()), K(obj2.get_collation_type()),                     \
                K(obj1), K(obj2));                                                              \
    } else {                                                                                    \
      ObJsonBin j_bin1(data_str1.ptr(), data_str1.length());                                    \
      ObJsonBin j_bin2(data_str2.ptr(), data_str2.length());                                    \
      ObIJsonBase *j_base1 = &j_bin1;                                                           \
      ObIJsonBase *j_base2 = &j_bin2;                                                           \
      if (OB_FAIL(j_bin1.reset_iter())) {                                                       \
        LOG_WDIAG("fail to reset json bin1 iter", K(ret), K(data_str1.length()));                \
      } else if (OB_FAIL(j_bin2.reset_iter())) {                                                \
        LOG_WDIAG("fail to reset json bin2 iter", K(ret), K(data_str2.length()));                \
      } else if (OB_FAIL(j_base1->compare(*j_base2, result))) {                                 \
        LOG_WDIAG("fail to compare json", K(ret), K(data_str1.length()), K(data_str2.length())); \
      } else {                                                                                  \
        cmp_ret = result op_str 0;                                                              \
      }                                                                                         \
    }                                                                                           \
    return cmp_ret;                                                                             \
  }

#define DEFINE_CMP_FUNC_JSON_JSON()                                                             \
  template <> inline                                                                            \
  int ObObjCmpFuncs::cmp_func<ObJsonTC, ObJsonTC>(const ObObj &obj1,                            \
                                                const ObObj &obj2,                              \
                                                const ObCompareCtx &cmp_ctx)                    \
  {                                                                                             \
    int ret = OB_SUCCESS;                                                                       \
    int result = CR_OB_ERROR;                                                                   \
    OBJ_TYPE_CLASS_CHECK(obj1, ObJsonTC);                                                       \
    OBJ_TYPE_CLASS_CHECK(obj2, ObJsonTC);                                                       \
    UNUSED(cmp_ctx);                                                                            \
    ObString data_str1;                                                                         \
    ObString data_str2;                                                                         \
    if (obj1.is_outrow_lob() || obj2.is_outrow_lob()) {                                         \
      LOG_WDIAG("not support outrow json lobs", K(obj1), K(obj2));                               \
      ret = CR_OB_ERROR;                                                                        \
    } else if (OB_FAIL(obj1.get_string(data_str1))) {                                           \
      LOG_WDIAG("invalid json lob object1",                                                      \
                K(obj1.get_collation_type()), K(obj2.get_collation_type()),                     \
                K(obj1), K(obj2));                                                              \
      ret = CR_OB_ERROR;                                                                        \
    } else if (OB_FAIL(obj2.get_string(data_str2))) {                                           \
      LOG_WDIAG("invalid json lob object2",                                                      \
                K(obj1.get_collation_type()), K(obj2.get_collation_type()),                     \
                K(obj1), K(obj2));                                                              \
      ret = CR_OB_ERROR;                                                                        \
    } else {                                                                                    \
      ObJsonBin j_bin1(data_str1.ptr(), data_str1.length());                                    \
      ObJsonBin j_bin2(data_str2.ptr(), data_str2.length());                                    \
      ObIJsonBase *j_base1 = &j_bin1;                                                           \
      ObIJsonBase *j_base2 = &j_bin2;                                                           \
      if (OB_FAIL(j_bin1.reset_iter())) {                                                       \
        LOG_WDIAG("fail to reset json bin1 iter", K(ret), K(data_str1.length()));                \
      } else if (OB_FAIL(j_bin2.reset_iter())) {                                                \
        LOG_WDIAG("fail to reset json bin2 iter", K(ret), K(data_str2.length()));                \
      } else if (OB_FAIL(j_base1->compare(*j_base2, result))) {                                 \
        LOG_WDIAG("fail to compare json", K(ret), K(data_str1.length()), K(data_str2.length())); \
      } else {                                                                                  \
        result = INT_TO_CR(result);                                                             \
      }                                                                                         \
    }                                                                                           \
                                                                                                \
    return result;                                                                              \
  }

#define DEFINE_CMP_FUNC_ENUMSETINNER_DECIMALINT()                                                  \
  template <>                                                                                      \
  int ObObjCmpFuncs::cmp_func<ObEnumSetInnerTC, ObDecimalIntTC>(                                   \
    const ObObj &obj1, const ObObj &obj2, const ObCompareCtx &)                                    \
  {                                                                                                \
    int cmp_res = 0;                                                                               \
    int ret = OB_SUCCESS;                                                                          \
    OBJ_TYPE_CLASS_CHECK(obj1, ObEnumSetInnerTC);                                                  \
    OBJ_TYPE_CLASS_CHECK(obj2, ObDecimalIntTC);                                                    \
    ObEnumSetInnerValue inner_value;                                                               \
    ObScale lh_scale = 0, rh_scale = obj2.get_scale();                                             \
    ObDecimalIntBuilder lh_val, rh_val;                                                            \
    ObDecimalIntBuilder tmp_alloc;                                                                 \
    ObDecimalInt *decint = nullptr;                                                                \
    uint64_t obj1_value = 0;                                                                       \
    int32_t int_bytes = 0;                                                                         \
    if (OB_FAIL(obj1.get_enumset_inner_value(inner_value))) {                                      \
      LOG_EDIAG("get enumset inner value failed", K(ret));                                         \
    } else if (FALSE_IT(obj1_value = inner_value.numberic_value_)) {                               \
    } else if (OB_FAIL(wide::from_integer(obj1_value, tmp_alloc, decint, int_bytes))) {            \
      LOG_EDIAG("cast integer to decimal int failed", K(ret));                                     \
    } else if (lh_scale > rh_scale) {                                                              \
      if (OB_FAIL(wide::common_scale_decimalint(obj2.get_decimal_int(), obj2.get_int_bytes(),      \
                                                rh_scale, lh_scale, rh_val))) {                    \
        LOG_EDIAG("scale decimal int failed", K(ret));                                             \
      } else {                                                                                     \
        rh_val.from(decint, int_bytes);                                                            \
      }                                                                                            \
    } else if (lh_scale < rh_scale) {                                                              \
      if (OB_FAIL(wide::common_scale_decimalint(decint, int_bytes, lh_scale, rh_scale, lh_val))) { \
        LOG_EDIAG("scale decimal int failed", K(ret));                                             \
      } else {                                                                                     \
        rh_val.from(obj2.get_decimal_int(), obj2.get_int_bytes());                                 \
      }                                                                                            \
    } else {                                                                                       \
      lh_val.from(decint, int_bytes);                                                              \
      rh_val.from(obj2.get_decimal_int(), obj2.get_int_bytes());                                   \
    }                                                                                              \
    if (OB_SUCC(ret)) {                                                                            \
      if (OB_FAIL(wide::compare(lh_val, rh_val, cmp_res))) {                                       \
        LOG_EDIAG("compare failed", K(ret));                                                       \
      }                                                                                            \
    }                                                                                              \
    return cmp_res;                                                                                \
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
        LOG_EDIAG("invalid collation", K(obj1), K(obj2)); \
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
        LOG_EDIAG("invalid collation", K(obj1), K(obj2)); \
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
        LOG_EDIAG("invalid timezone offset", K(obj1), K(obj2)); \
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
        LOG_EDIAG("invalid timezone offset", K(obj1), K(obj2)); \
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
    ObCmpRes ret = CR_FALSE; \
    OBJ_TYPE_CLASS_CHECK(obj1, ObDateTimeTC);\
    OBJ_TYPE_CLASS_CHECK(obj2, ObOTimestampTC); \
    ObOTimestampData v1; \
    v1.time_us_ = obj1.get_datetime();\
    ObOTimestampData v2 = obj2.get_otimestamp_value();\
    if (!obj2.is_timestamp_nano()) { \
      if (OB_UNLIKELY(INVALID_TZ_OFF == cmp_ctx.tz_off_)) {\
        LOG_EDIAG("invalid timezone offset", K(obj1), K(obj2)); \
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
    ObCmpRes ret = CR_FALSE;\
    OBJ_TYPE_CLASS_CHECK(obj1, ObDateTimeTC);\
    OBJ_TYPE_CLASS_CHECK(obj2, ObOTimestampTC);\
    ObOTimestampData v1; \
    v1.time_us_ = obj1.get_datetime();\
    ObOTimestampData v2 = obj2.get_otimestamp_value(); \
    if (!obj2.is_timestamp_nano()) { \
      if (OB_UNLIKELY(INVALID_TZ_OFF == cmp_ctx.tz_off_)) {\
        LOG_EDIAG("invalid timezone offset", K(obj1), K(obj2)); \
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
    ObCmpRes ret = CR_FALSE;                                                    \
    OBJ_TYPE_CLASS_CHECK(obj1, ObOTimestampTC);                                 \
    OBJ_TYPE_CLASS_CHECK(obj2, ObDateTimeTC);                                   \
    ObOTimestampData v1 = obj1.get_otimestamp_value();                          \
    ObOTimestampData v2;                                                        \
    v2.time_us_ = obj2.get_datetime();                                          \
    if (!obj1.is_timestamp_nano()) {                                            \
      if (OB_UNLIKELY(INVALID_TZ_OFF == cmp_ctx.tz_off_)) {                     \
        LOG_EDIAG("invalid timezone offset", K(obj1), K(obj2));                 \
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
    ObCmpRes ret = CR_FALSE;                                                                   \
    OBJ_TYPE_CLASS_CHECK(obj1, ObOTimestampTC);                                                \
    OBJ_TYPE_CLASS_CHECK(obj2, ObDateTimeTC);                                                  \
    ObOTimestampData v1 = obj1.get_otimestamp_value();                                         \
    ObOTimestampData v2;                                                                       \
    v2.time_us_ = obj2.get_datetime();                                                         \
    if (!obj1.is_timestamp_nano()) {                                                           \
      if (OB_UNLIKELY(INVALID_TZ_OFF == cmp_ctx.tz_off_)) {                                    \
        LOG_EDIAG("invalid timezone offset", K(obj1), K(obj2));                                \
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
    ObCmpRes ret = CR_FALSE;                                                    \
    OBJ_TYPE_CLASS_CHECK(obj1, ObOTimestampTC);                                 \
    OBJ_TYPE_CLASS_CHECK(obj2, ObOTimestampTC);                                 \
    ObOTimestampData v1 = obj1.get_otimestamp_value();                          \
    ObOTimestampData v2 = obj2.get_otimestamp_value();                          \
    if (obj1.is_timestamp_nano() != obj2.is_timestamp_nano()) {                 \
      if (OB_UNLIKELY(INVALID_TZ_OFF == cmp_ctx.tz_off_)) {                     \
        LOG_EDIAG("invalid timezone offset", K(obj1), K(obj2));                 \
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
    ObCmpRes ret = CR_FALSE;                                                                   \
    OBJ_TYPE_CLASS_CHECK(obj1, ObOTimestampTC);                                                \
    OBJ_TYPE_CLASS_CHECK(obj2, ObOTimestampTC);                                                \
    ObOTimestampData v1 = obj1.get_otimestamp_value();                                         \
    ObOTimestampData v2 = obj2.get_otimestamp_value();                                         \
    if (obj1.is_timestamp_nano() != obj2.is_timestamp_nano()) {                                \
      if (OB_UNLIKELY(INVALID_TZ_OFF == cmp_ctx.tz_off_)) {                                    \
        LOG_EDIAG("invalid timezone offset", K(obj1), K(obj2));                                \
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

#define DEFINE_CMP_FUNCS_DECIMALINT_DECIMALINT()                                                   \
  DEFINE_CMP_OP_FUNC_DECIMALINT_DECIMALINT(CO_EQ, ==);                                             \
  DEFINE_CMP_OP_FUNC_DECIMALINT_DECIMALINT(CO_LE, <=);                                             \
  DEFINE_CMP_OP_FUNC_DECIMALINT_DECIMALINT(CO_LT, <);                                              \
  DEFINE_CMP_OP_FUNC_DECIMALINT_DECIMALINT(CO_GE, >=);                                             \
  DEFINE_CMP_OP_FUNC_DECIMALINT_DECIMALINT(CO_GT, >);                                              \
  DEFINE_CMP_OP_FUNC_DECIMALINT_DECIMALINT(CO_NE, !=);                                             \
  DEFINE_CMP_OP_FUNC_DECIMALINT_DECIMALINT(CO_CMP, =);                                             \
  DEFINE_CMP_FUNC_DECIMALINT_DECIMALINT()

#define DEFINE_CMP_FUNCS_DECIMALINT_INTEGER(int_tc, val_type, val_func)                            \
  DEFINE_CMP_OP_FUNC_DECIMALINT_INTEGER(CO_EQ, ==, int_tc, val_type, val_func);                    \
  DEFINE_CMP_OP_FUNC_DECIMALINT_INTEGER(CO_LE, <=, int_tc, val_type, val_func);                    \
  DEFINE_CMP_OP_FUNC_DECIMALINT_INTEGER(CO_LT, <, int_tc, val_type, val_func);                     \
  DEFINE_CMP_OP_FUNC_DECIMALINT_INTEGER(CO_GE, >=, int_tc, val_type, val_func);                    \
  DEFINE_CMP_OP_FUNC_DECIMALINT_INTEGER(CO_GT, >, int_tc, val_type, val_func);                     \
  DEFINE_CMP_OP_FUNC_DECIMALINT_INTEGER(CO_NE, !=, int_tc, val_type, val_func);                    \
  DEFINE_CMP_OP_FUNC_DECIMALINT_INTEGER(CO_CMP, =, int_tc, val_type, val_func);                    \
  DEFINE_CMP_FUNC_DECIMALINT_INTEGER(int_tc, val_type, val_func)

#define DEFINE_CMP_FUNCS_INTEGER_DECIMALINT(int_tc)                                                \
  DEFINE_CMP_OP_FUNC_INTEGER_DECIMALINT(CO_EQ, ==, int_tc);                                        \
  DEFINE_CMP_OP_FUNC_INTEGER_DECIMALINT(CO_LE, <=, int_tc);                                        \
  DEFINE_CMP_OP_FUNC_INTEGER_DECIMALINT(CO_LT, <, int_tc);                                         \
  DEFINE_CMP_OP_FUNC_INTEGER_DECIMALINT(CO_GE, >=, int_tc);                                        \
  DEFINE_CMP_OP_FUNC_INTEGER_DECIMALINT(CO_GT, >, int_tc);                                         \
  DEFINE_CMP_OP_FUNC_INTEGER_DECIMALINT(CO_NE, !=, int_tc);                                        \
  DEFINE_CMP_OP_FUNC_INTEGER_DECIMALINT(CO_CMP, =, int_tc);                                        \
  DEFINE_CMP_FUNC_INTEGER_DECIMALINT(int_tc)

#define DEFINE_CMP_FUNCS_DECIMALINT_NUMBER()                                                       \
  DEFINE_CMP_OP_FUNC_DECIMALINT_NUMBER(CO_EQ, ==);                                                 \
  DEFINE_CMP_OP_FUNC_DECIMALINT_NUMBER(CO_LE, <=);                                                 \
  DEFINE_CMP_OP_FUNC_DECIMALINT_NUMBER(CO_LT, <);                                                  \
  DEFINE_CMP_OP_FUNC_DECIMALINT_NUMBER(CO_GE, >=);                                                 \
  DEFINE_CMP_OP_FUNC_DECIMALINT_NUMBER(CO_GT, >);                                                  \
  DEFINE_CMP_OP_FUNC_DECIMALINT_NUMBER(CO_NE, !=);                                                 \
  DEFINE_CMP_FUNC_DECIMALINT_NUMBER()

#define DEFINE_CMP_FUNCS_NUMBER_DECIMALINT()                                                       \
  DEFINE_CMP_OP_FUNC_NUMBER_DECIMALINT(CO_EQ, ==);                                                 \
  DEFINE_CMP_OP_FUNC_NUMBER_DECIMALINT(CO_LE, <=);                                                 \
  DEFINE_CMP_OP_FUNC_NUMBER_DECIMALINT(CO_LT, <);                                                  \
  DEFINE_CMP_OP_FUNC_NUMBER_DECIMALINT(CO_GE, >=);                                                 \
  DEFINE_CMP_OP_FUNC_NUMBER_DECIMALINT(CO_GT, >);                                                  \
  DEFINE_CMP_OP_FUNC_NUMBER_DECIMALINT(CO_NE, !=);                                                 \
  DEFINE_CMP_FUNC_NUMBER_DECIMALINT()

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

#define DEFINE_CMP_FUNCS_NUMBER_DECIMALINT()                                                       \
  DEFINE_CMP_OP_FUNC_NUMBER_DECIMALINT(CO_EQ, ==);                                                 \
  DEFINE_CMP_OP_FUNC_NUMBER_DECIMALINT(CO_LE, <=);                                                 \
  DEFINE_CMP_OP_FUNC_NUMBER_DECIMALINT(CO_LT, <);                                                  \
  DEFINE_CMP_OP_FUNC_NUMBER_DECIMALINT(CO_GE, >=);                                                 \
  DEFINE_CMP_OP_FUNC_NUMBER_DECIMALINT(CO_GT, >);                                                  \
  DEFINE_CMP_OP_FUNC_NUMBER_DECIMALINT(CO_NE, !=);                                                 \
  DEFINE_CMP_FUNC_NUMBER_DECIMALINT()

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

DEFINE_CMP_FUNCS_DECIMALINT_DECIMALINT();
DEFINE_CMP_FUNCS_DECIMALINT_INTEGER(ObIntTC, int64_t, int);
DEFINE_CMP_FUNCS_DECIMALINT_INTEGER(ObUIntTC, uint64_t, uint64);
DEFINE_CMP_FUNCS_INTEGER_DECIMALINT(ObIntTC);
DEFINE_CMP_FUNCS_INTEGER_DECIMALINT(ObUIntTC);
DEFINE_CMP_FUNCS_DECIMALINT_INTEGER(ObEnumSetTC, uint64_t, uint64);
DEFINE_CMP_FUNCS_INTEGER_DECIMALINT(ObEnumSetTC);
DEFINE_CMP_FUNCS_DECIMALINT_NUMBER();
DEFINE_CMP_FUNCS_NUMBER_DECIMALINT();

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
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),//json
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),//geometry
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC),//udt
    DEFINE_CMP_FUNCS_ENTRY(ObNullTC, ObMaxTC), // decimal int
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
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY(ObIntTC, ObDecimalIntTC), // decimal int
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
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY(ObUIntTC, ObDecimalIntTC), // decimal int
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
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL, // decimal int
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
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // decimal int
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
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY(ObNumberTC, ObDecimalIntTC), // decimal int
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
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // decimal int
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
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // decimal int
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
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // decimal int
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
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // decimal int
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
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // decimal int
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
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY(ObExtendTC, ObMaxTC), // decimal int
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
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //decimal int
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
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //decimal int
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
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // decimal int
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
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY(ObEnumSetTC, ObDecimalIntTC), // decimal int
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
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL, // decimal int
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
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // decimal int
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
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // decimal int
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
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // decimal int
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
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // decimal int
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
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // decimal int
  },
  { // json
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC), //null
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
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL, // decimal int
  },
  { // geometry
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC), //null
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
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // decimal int
  },
  { // udt
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC), //null
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
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //udt
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // decimal int
  },
  { // decimalint
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObNullTC), //null
    DEFINE_CMP_FUNCS_ENTRY(ObDecimalIntTC, ObIntTC), // int
    DEFINE_CMP_FUNCS_ENTRY(ObDecimalIntTC, ObUIntTC), // uint
    DEFINE_CMP_FUNCS_ENTRY_NULL, // float
    DEFINE_CMP_FUNCS_ENTRY_NULL, // double
    DEFINE_CMP_FUNCS_ENTRY(ObDecimalIntTC, ObNumberTC), // number
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // datetime
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // date
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // time
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // year
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // string
    DEFINE_CMP_FUNCS_ENTRY(ObMaxTC, ObExtendTC), // extend
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // unknown
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // text
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // bit
    DEFINE_CMP_FUNCS_ENTRY(ObDecimalIntTC, ObEnumSetTC), // enumset
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // enumsetInner will not go here
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // otimestamp
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // raw
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // interval
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //rowid
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //lob
    DEFINE_CMP_FUNCS_ENTRY_NULL, // json
    DEFINE_CMP_FUNCS_ENTRY_NULL,  //geometry
    DEFINE_CMP_FUNCS_ENTRY_NULL,  // udt
    DEFINE_CMP_FUNCS_ENTRY(ObDecimalIntTC, ObDecimalIntTC), // decimal int
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
    LOG_EDIAG("invalid obj1 or obj2 or cmp_op", K(obj1), K(obj2), K(cmp_op));
    cmp = CR_ERROR;
  } else {
    obj_cmp_func cmp_op_func = cmp_funcs[tc1][tc2][cmp_op];
    if (OB_ISNULL(cmp_op_func)) {
      LOG_EDIAG("obj1 and obj2 can't compare", K(obj1), K(obj2), K(cmp_op));
      cmp = CR_ERROR;
    } else {
      ObCompareCtx cmp_ctx(ObMaxType, cs_type, true, INVALID_TZ_OFF);
      if (OB_UNLIKELY(CR_ERROR == (cmp = cmp_op_func(obj1, obj2, cmp_ctx)))) {
        LOG_EDIAG("failed to compare obj1 and obj2", K(obj1), K(obj2), K(cmp_op));
        cmp = CR_ERROR;
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
    LOG_EDIAG("invalid obj1 or obj2", K(obj1), K(obj2));
    cmp = CR_ERROR;
  } else {
    obj_cmp_func cmp_func = cmp_funcs[tc1][tc2][CO_CMP];
    if (OB_ISNULL(cmp_func)) {
      LOG_EDIAG("obj1 and obj2 can't compare", K(obj1), K(obj2));
      cmp = CR_ERROR;
    } else {
      ObCompareCtx cmp_ctx(ObMaxType, cs_type, true, INVALID_TZ_OFF);
      if (OB_UNLIKELY(CR_ERROR == (cmp = cmp_func(obj1, obj2, cmp_ctx)))) {
        LOG_EDIAG("failed to compare obj1 and obj2", K(obj1), K(obj2));
        cmp = CR_ERROR;
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
      LOG_WDIAG("failed to compare obj1 and obj2", K(ret), K(obj1), K(obj2), K(cmp_op));
    } else {
      // CR_LT is -1, CR_EQ is 0, so we add 1 to cmp_res_objs_int.
      result = (CO_CMP == cmp_op) ? (cmp_res_objs_int + 1)[cmp] : cmp_res_objs_bool[cmp];
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
