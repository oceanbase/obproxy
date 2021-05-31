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

#ifndef OCEANBASE_COMMON_OB_OBJ_COMPARE_
#define OCEANBASE_COMMON_OB_OBJ_COMPARE_

#include "lib/timezone/ob_timezone_info.h"
#include "common/ob_object.h"

namespace oceanbase
{
namespace common
{

enum ObCmpOp
{
  CO_EQ = 0,
  CO_LE,
  CO_LT,
  CO_GE,
  CO_GT,
  CO_NE,
  CO_CMP,
  CO_MAX
};

OB_INLINE bool ob_is_valid_cmp_op(ObCmpOp cmp_op) { return 0 <= cmp_op && cmp_op < CO_MAX; }
OB_INLINE bool ob_is_invalid_cmp_op(ObCmpOp cmp_op) { return !ob_is_valid_cmp_op(cmp_op); }
OB_INLINE bool ob_is_valid_cmp_op_bool(ObCmpOp cmp_op) { return 0 <= cmp_op && cmp_op < CO_CMP; }
OB_INLINE bool ob_is_invalid_cmp_op_bool(ObCmpOp cmp_op) { return !ob_is_valid_cmp_op_bool(cmp_op); }

struct ObCompareCtx
{
  //member functions
  ObCompareCtx(const ObObjType cmp_type,
               const ObCollationType cmp_cs_type,
               const bool is_null_safe,
               const int64_t tz_off)
    : cmp_type_(cmp_type),
      cmp_cs_type_(cmp_cs_type),
      is_null_safe_(is_null_safe),
      tz_off_(tz_off)
  {}
  ObCompareCtx()
    : cmp_type_(ObMaxType),
      cmp_cs_type_(CS_TYPE_INVALID),
      is_null_safe_(false),
      tz_off_(INVALID_TZ_OFF)
  {}
  //data members
  ObObjType cmp_type_;    // used by upper functions, not in these compare functions.
  ObCollationType cmp_cs_type_;
  bool is_null_safe_;
  int64_t tz_off_;
};

typedef int (*obj_cmp_func)(const ObObj &obj1,
                            const ObObj &obj2,
                            const ObCompareCtx &cmp_ctx);

/**
 * at the very beginning, all compare functions in this class should be used to compare objects
 * with same type ONLY, because we can't do any cast operation here.
 * but after some performance problems, we realized that some cast operations can be skipped even
 * if objects have different types, such as int VS number, int VS float / double.
 * so actually we can do any compare operations which NEED NOT cast.
 */
class ObObjCmpFuncs
{
public:
  static bool can_compare_directly(ObObjType type1, ObObjType type2);
  /**
   * null safe compare.
   * will HANG (right_to_die_or_duty_to_live) if error, such as can't compare without cast.
   * @param[in] obj1
   * @param[in] obj2
   * @param[in] cs_type: compare collation.
   * @param[in] cmp_op: CO_EQ / CO_LE / CO_LT / CO_GE / CO_GT / CO_NE. CO_CMP is not allowed.
   * @return bool.
   */
  static bool compare_oper_nullsafe(const ObObj &obj1,
                                    const ObObj &obj2,
                                    ObCollationType cs_type,
                                    ObCmpOp cmp_op);
  /**
   * null safe compare.
   * will HANG (right_to_die_or_duty_to_live) if error, such as can't compare without cast.
   * @param[in] obj1
   * @param[in] obj2
   * @param[in] cs_type: compare collation.
   * @return -1: obj1 < obj2.
   *          0: obj1 = obj2.
   *          1: obj1 > obj2.
   */
  static int compare_nullsafe(const ObObj &obj1,
                              const ObObj &obj2,
                              ObCollationType cs_type);
  /**
   * compare.
   * @param[out] result: true / false / -1 / 0 / 1 / null.
   * @param[in] obj1
   * @param[in] obj2
   * @param[in] cmp_ctx
   * @param[in] cmp_op: CO_EQ / CO_LE / CO_LT / CO_GE / CO_GT / CO_NE / CO_CMP.
   * @param[out] need_cast: set to true if can't compare without cast, otherwise false.
   * @return ob error code.
   */
  static int compare(ObObj &result,
                     const ObObj &obj1,
                     const ObObj &obj2,
                     const ObCompareCtx &cmp_ctx,
                     ObCmpOp cmp_op,
                     bool &need_cast);

private:
  enum ObCmpRes
  {
    // for bool.
    CR_FALSE = 0,
    CR_TRUE = 1,
    // for int.
    CR_LT = -1,
    CR_EQ = 0,
    CR_GT = 1,
    // other.
    CR_NULL = 2,
    CR_ERROR = 3,
    // count.
    CR_BOOL_CNT = 3,
    CR_INT_CNT = 4
  };

private:
  // return CR_FALSE / CR_TRUE / CR_NULL / CR_ERROR.
  template <ObObjTypeClass tc1, ObObjTypeClass tc2, ObCmpOp op>
  static int cmp_op_func(const ObObj &Obj1, const ObObj &obj2,
                         const ObCompareCtx &cmp_ctx);
  // return CR_LT / CR_EQ / CR_GT / CR_NULL / CR_ERROR.
  template <ObObjTypeClass tc1, ObObjTypeClass tc2>
  static int cmp_func(const ObObj &Obj1, const ObObj &obj2,
                      const ObCompareCtx &cmp_ctx);
private:
  OB_INLINE static int INT_TO_CR(int val) { return val < 0 ? CR_LT : val > 0 ? CR_GT : CR_EQ; }
private:
  static const obj_cmp_func cmp_funcs[ObMaxTC][ObMaxTC][CO_MAX];
  static const ObObj cmp_res_objs_bool[CR_BOOL_CNT];
  static const ObObj cmp_res_objs_int[CR_INT_CNT];
};

} // namespace common
} // namespace oceanbase

#endif /* OCEANBASE_COMMON_OB_OBJ_COMPARE_ */
