/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PROXY

#include "opsql/func_expr_resolver/proxy_expr/ob_proxy_expr_factory.h"
#include "lib/ob_errno.h"

namespace oceanbase
{
namespace obproxy
{
namespace opsql
{

typedef hash::ObHashMap<ObString, ObProxyExprType, hash::NoPthreadDefendMode> ExprNameTypeMap;
typedef int (ObProxyExprFactory::*ExprAllocFunc) (const ObProxyExprType type, ObProxyFuncExpr *&func_expr);
static ExprAllocFunc TYPE_ALLOC[EXPR_NUM];
static ExprNameTypeMap g_expr_name_type_map;

#define REG_EXPR(name, type, ExprClass, target_type)                                        \
  do {                                                                                      \
    ObString store_name(name);                                                              \
    if (OB_FAIL(g_expr_name_type_map.set_refactored(store_name, type))) {                   \
      LOG_EDIAG("fail to register expr funx", K(store_name), K(type), K(ret));              \
    } else {                                                                                \
      TYPE_ALLOC[type] = &ObProxyExprFactory::alloc_func_expr<ExprClass, target_type>;      \
    }                                                                                       \
    i++;                                                                                    \
  } while (0)

void ObProxyExprFactory::str_toupper(char *upper_buf, const char *str, const int32_t str_len)
{
  if (OB_LIKELY(OB_NOT_NULL(upper_buf) && OB_NOT_NULL(str)) && OB_LIKELY(str_len > 0)) {
    for (int32_t i = 0; i < str_len; ++i) {
      if ((str[i]) >= 'a' && (str[i]) <= 'z') {
        upper_buf[i] = static_cast<char>(str[i] - 32);
      }
    }
  }
}

int ObProxyExprFactory::get_type_by_name(const ObString &name, ObProxyExprType &type) {
  int ret = OB_SUCCESS;
  type = OB_PROXY_EXPR_TYPE_NONE;
  char *upper_buf = NULL;
  int buf_len = name.length();
  if (OB_ISNULL(upper_buf = static_cast<char*>(op_fixed_mem_alloc(name.length())))) {
    LOG_WDIAG("fail to alloc mem", K(ret));
  } else {
    MEMCPY(upper_buf, name.ptr(), buf_len);
    str_toupper(upper_buf, name.ptr(), buf_len);
    ObString upper_str(buf_len, upper_buf);
    if (!g_expr_name_type_map.created()) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(g_expr_name_type_map.get_refactored(upper_str, type))) {
      if (ret == OB_HASH_NOT_EXIST) {
        ret = OB_SUCCESS;
      } else {
        LOG_WDIAG("fail to get func type by name", K(name), K(ret));
      }
      type = OB_PROXY_EXPR_TYPE_NONE;
    }
  }
  if (upper_buf != NULL) {
    op_fixed_mem_free(upper_buf, buf_len);
  }
  LOG_DEBUG("the result of get type by name:", K(name), K(type));
  return ret;
}

int ObProxyExprFactory::create_func_expr(const ObProxyExprType type, ObProxyFuncExpr *&func_expr)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(OB_PROXY_EXPR_TYPE_NONE >= type || OB_PROXY_EXPR_TYPE_MAX <= type)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_DEBUG("unsupported function type", K(type), K(ret)); // DEBUG for simplify meaningless and futile log
  } else if (OB_ISNULL(TYPE_ALLOC[type])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected type_alloc func_expr is NULL", K(type), K(ret));
  } else if (OB_FAIL((this->*TYPE_ALLOC[type])(type, func_expr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to call alloc, maybe out of memory", K(type), K(ret));
  } else if (lib::is_oracle_mode) { // oracle的时间类型和mysql不同，需要特殊处理
    if (OB_PROXY_EXPR_TYPE_FUNC_TIMESTAMP == type) {
      func_expr->set_target_type(ObTimestampNanoType);
    }
  }
  return ret;
}

int ObProxyExprFactory::register_proxy_expr()
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(g_expr_name_type_map.create(EXPR_NUM, ObModIds::OB_HASH_BUCKET, ObModIds::OB_HASH_NODE))) {
    LOG_WDIAG("init name_type hashmap failed",K(ret));
  } else {
    // start from hash,front expr is not used for func
    int64_t i = 6;

    REG_EXPR("+", OB_PROXY_EXPR_TYPE_FUNC_ADD, ObProxyExprAdd, ObNullType);
    REG_EXPR("-", OB_PROXY_EXPR_TYPE_FUNC_SUB, ObProxyExprSub, ObNullType);
    REG_EXPR("*", OB_PROXY_EXPR_TYPE_FUNC_MUL, ObProxyExprMul, ObNullType);
    REG_EXPR("/", OB_PROXY_EXPR_TYPE_FUNC_DIV, ObProxyExprDiv, ObNullType);
    REG_EXPR("%", OB_PROXY_EXPR_TYPE_FUNC_MOD, ObProxyExprMod, ObNullType);


    REG_EXPR("HASH", OB_PROXY_EXPR_TYPE_FUNC_HASH, ObProxyExprHash, ObNullType);
    REG_EXPR("SUBSTR", OB_PROXY_EXPR_TYPE_FUNC_SUBSTR, ObProxyExprSubStr, ObNullType);
    REG_EXPR("CONCAT", OB_PROXY_EXPR_TYPE_FUNC_CONCAT, ObProxyExprConcat, ObNullType);
    REG_EXPR("TOINT", OB_PROXY_EXPR_TYPE_FUNC_TOINT, ObProxyExprToInt, ObNullType);
    REG_EXPR("DIV", OB_PROXY_EXPR_TYPE_FUNC_DIV, ObProxyExprDiv, ObNullType);
    REG_EXPR("ADD", OB_PROXY_EXPR_TYPE_FUNC_ADD, ObProxyExprAdd, ObNullType);
    REG_EXPR("SUB", OB_PROXY_EXPR_TYPE_FUNC_SUB, ObProxyExprSub, ObNullType);
    REG_EXPR("MUL", OB_PROXY_EXPR_TYPE_FUNC_MUL, ObProxyExprMul, ObNullType);

    /*
     * these are agg function expr, func expr will not call 
     */
    REG_EXPR("SUM", OB_PROXY_EXPR_TYPE_FUNC_SUM, ObProxyExprSum, ObNullType);
    REG_EXPR("COUNT", OB_PROXY_EXPR_TYPE_FUNC_COUNT, ObProxyExprCount, ObNullType);
    REG_EXPR("MAX", OB_PROXY_EXPR_TYPE_FUNC_MAX, ObProxyExprMax, ObNullType);
    REG_EXPR("MIN", OB_PROXY_EXPR_TYPE_FUNC_MIN, ObProxyExprMin, ObNullType);
    REG_EXPR("AVG", OB_PROXY_EXPR_TYPE_FUNC_AVG, ObProxyExprAvg, ObNullType);
    REG_EXPR("GROUP", OB_PROXY_EXPR_TYPE_FUNC_GROUP, ObProxyFuncExpr, ObNullType);
    REG_EXPR("ORDER", OB_PROXY_EXPR_TYPE_FUNC_ORDER, ObProxyFuncExpr, ObNullType);

    REG_EXPR("TESTLOAD", OB_PROXY_EXPR_TYPE_FUNC_TESTLOAD, ObProxyExprTestLoad, ObNullType);
    REG_EXPR("SPLIT", OB_PROXY_EXPR_TYPE_FUNC_SPLIT, ObProxyExprSplit, ObNullType);
    REG_EXPR("YEAR", OB_PROXY_EXPR_TYPE_FUNC_YEAR, ObProxyExprYear, ObIntType);
    REG_EXPR("MONTH", OB_PROXY_EXPR_TYPE_FUNC_MONTH, ObProxyExprMonth, ObIntType);
    REG_EXPR("TO_DAYS", OB_PROXY_EXPR_TYPE_FUNC_TO_DAYS, ObProxyExprToDays, ObIntType);
    REG_EXPR("TO_DATE", OB_PROXY_EXPR_TYPE_FUNC_TO_DATE, ObProxyExprToTime, ObDateTimeType); // special case will not call
    REG_EXPR("TO_TIMESTAMP", OB_PROXY_EXPR_TYPE_FUNC_TO_TIMESTAMP, ObProxyExprToTime, ObTimestampNanoType); // special case will not call
    REG_EXPR("TIMESTAMP", OB_PROXY_EXPR_TYPE_FUNC_TIMESTAMP, ObProxyExprToTime, ObTimestampType);
    REG_EXPR("DATE", OB_PROXY_EXPR_TYPE_FUNC_DATE, ObProxyExprToTime, ObDateTimeType);
    REG_EXPR("TIME", OB_PROXY_EXPR_TYPE_FUNC_TIME, ObProxyExprToTime, ObTimeType);
    REG_EXPR("NVL", OB_PROXY_EXPR_TYPE_FUNC_NVL, ObProxyExprNvl, ObNullType);
    REG_EXPR("TO_CHAR", OB_PROXY_EXPR_TYPE_FUNC_TO_CHAR, ObProxyExprToChar, ObNullType);
    REG_EXPR("SYSDATE", OB_PROXY_EXPR_TYPE_FUNC_SYSDATE, ObProxyExprSysdate, ObNullType);
    REG_EXPR("MOD", OB_PROXY_EXPR_TYPE_FUNC_MOD, ObProxyExprMod, ObNullType);
    REG_EXPR("ISNULL", OB_PROXY_EXPR_TYPE_FUNC_ISNULL, ObProxyExprIsnull, ObNullType);
    REG_EXPR("FLOOR", OB_PROXY_EXPR_TYPE_FUNC_FLOOR, ObProxyExprFloor, ObNullType);
    REG_EXPR("CEIL", OB_PROXY_EXPR_TYPE_FUNC_CEIL, ObProxyExprCeil, ObNullType);
    REG_EXPR("CEILING", OB_PROXY_EXPR_TYPE_FUNC_CEIL, ObProxyExprCeil, ObNullType);
    REG_EXPR("ROUND", OB_PROXY_EXPR_TYPE_FUNC_ROUND, ObProxyExprRound, ObNullType);
    REG_EXPR("TRUNCATE", OB_PROXY_EXPR_TYPE_FUNC_TRUNCATE, ObProxyExprTruncate, ObNullType);
    REG_EXPR("TRUNC", OB_PROXY_EXPR_TYPE_FUNC_TRUNCATE, ObProxyExprTruncate, ObNullType);
    REG_EXPR("ABS", OB_PROXY_EXPR_TYPE_FUNC_ABS, ObProxyExprAbs, ObNullType);
    REG_EXPR("SYSTIMESTAMP", OB_PROXY_EXPR_TYPE_FUNC_SYSTIMESTAMP, ObProxyExprSystimestamp, ObNullType);
    REG_EXPR("CURRENT_DATE", OB_PROXY_EXPR_TYPE_FUNC_CURRENT_DATE, ObProxyExprCurrentdate, ObNullType);
    REG_EXPR("CURDATE", OB_PROXY_EXPR_TYPE_FUNC_CURRENT_DATE, ObProxyExprCurrentdate, ObNullType);
    REG_EXPR("CURRENT_TIME", OB_PROXY_EXPR_TYPE_FUNC_CURRENT_TIME, ObProxyExprCurrenttime, ObNullType);
    REG_EXPR("CURTIME", OB_PROXY_EXPR_TYPE_FUNC_CURRENT_TIME, ObProxyExprCurrenttime, ObNullType);
    REG_EXPR("CURRENT_TIMESTAMP", OB_PROXY_EXPR_TYPE_FUNC_CURRENT_TIMESTAMP, ObProxyExprCurrenttimestamp, ObNullType);
    REG_EXPR("NOW", OB_PROXY_EXPR_TYPE_FUNC_CURRENT_TIMESTAMP, ObProxyExprCurrenttimestamp, ObNullType);

    REG_EXPR("TRIM", OB_PROXY_EXPR_TYPE_FUNC_TRIM, ObProxyExprTrim, ObNullType);
    REG_EXPR("LTRIM", OB_PROXY_EXPR_TYPE_FUNC_LTRIM, ObProxyExprLtrim, ObNullType);
    REG_EXPR("RTRIM", OB_PROXY_EXPR_TYPE_FUNC_RTRIM, ObProxyExprRtrim, ObNullType);
    REG_EXPR("SUBSTRING", OB_PROXY_EXPR_TYPE_FUNC_SUBSTR, ObProxyExprSubStr, ObNullType);
    REG_EXPR("REPLACE", OB_PROXY_EXPR_TYPE_FUNC_REPLACE, ObProxyExprReplace, ObNullType);
    REG_EXPR("LENGTH", OB_PROXY_EXPR_TYPE_FUNC_LENGTH, ObProxyExprLength, ObNullType);
    REG_EXPR("LOWER", OB_PROXY_EXPR_TYPE_FUNC_LOWER, ObProxyExprLower, ObNullType);
    REG_EXPR("LCASE", OB_PROXY_EXPR_TYPE_FUNC_LOWER, ObProxyExprLower, ObNullType);
    REG_EXPR("UPPER", OB_PROXY_EXPR_TYPE_FUNC_UPPER, ObProxyExprUpper, ObNullType);
    REG_EXPR("UCASE", OB_PROXY_EXPR_TYPE_FUNC_UPPER, ObProxyExprUpper, ObNullType);
    REG_EXPR("TO_NUMBER", OB_PROXY_EXPR_TYPE_FUNC_TO_NUMBER, ObProxyExprToNumber, ObNullType);
    REG_EXPR("SUBSTRING_INDEX",OB_PROXY_EXPR_TYPE_FUNC_SUBSTR_INDEX, ObProxyExprNotSupport, ObNullType);

    // add new function above, this is the last
    // REG_EXPR("", OB_PROXY_EXPR_TYPE_MAX, );
  }
  return ret;
}

} // end opsql
} // end obproxy
} // end oceanbase