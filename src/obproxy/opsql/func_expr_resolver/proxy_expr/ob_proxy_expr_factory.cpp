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

#define REG_EXPR(name, type, ExprClass)                                        \
  do {                                                                         \
    ObString store_name(name);                                                 \
    if (OB_FAIL(g_expr_name_type_map.set_refactored(store_name, type))) {      \
      LOG_EDIAG("fail to register expr funx", K(store_name), K(type), K(ret)); \
    } else {                                                                   \
      TYPE_ALLOC[type] = &ObProxyExprFactory::alloc_func_expr<ExprClass>;      \
    }                                                                          \
    i++;                                                                       \
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
  } else if (OB_ISNULL(TYPE_ALLOC[type])) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL((this->*TYPE_ALLOC[type])(type, func_expr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
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

    REG_EXPR("+", OB_PROXY_EXPR_TYPE_FUNC_ADD, ObProxyExprAdd);
    REG_EXPR("-", OB_PROXY_EXPR_TYPE_FUNC_SUB, ObProxyExprSub);
    REG_EXPR("*", OB_PROXY_EXPR_TYPE_FUNC_MUL, ObProxyExprMul);
    REG_EXPR("/", OB_PROXY_EXPR_TYPE_FUNC_DIV, ObProxyExprDiv);
    REG_EXPR("%", OB_PROXY_EXPR_TYPE_FUNC_MOD, ObProxyExprMod);


    REG_EXPR("HASH", OB_PROXY_EXPR_TYPE_FUNC_HASH, ObProxyExprHash);
    REG_EXPR("SUBSTR", OB_PROXY_EXPR_TYPE_FUNC_SUBSTR, ObProxyExprSubStr);
    REG_EXPR("CONCAT", OB_PROXY_EXPR_TYPE_FUNC_CONCAT, ObProxyExprConcat);
    REG_EXPR("TOINT", OB_PROXY_EXPR_TYPE_FUNC_TOINT, ObProxyExprToInt);
    REG_EXPR("DIV", OB_PROXY_EXPR_TYPE_FUNC_DIV, ObProxyExprDiv);
    REG_EXPR("ADD", OB_PROXY_EXPR_TYPE_FUNC_ADD, ObProxyExprAdd);
    REG_EXPR("SUB", OB_PROXY_EXPR_TYPE_FUNC_SUB, ObProxyExprSub);
    REG_EXPR("MUL", OB_PROXY_EXPR_TYPE_FUNC_MUL, ObProxyExprMul);

    /*
     * these are agg function expr, func expr will not call 
     */
    REG_EXPR("SUM", OB_PROXY_EXPR_TYPE_FUNC_SUM, ObProxyExprSum);
    REG_EXPR("COUNT", OB_PROXY_EXPR_TYPE_FUNC_COUNT, ObProxyExprCount); 
    REG_EXPR("MAX", OB_PROXY_EXPR_TYPE_FUNC_MAX, ObProxyExprMax); 
    REG_EXPR("MIN", OB_PROXY_EXPR_TYPE_FUNC_MIN, ObProxyExprMin);  
    REG_EXPR("AVG", OB_PROXY_EXPR_TYPE_FUNC_AVG, ObProxyExprAvg);
    REG_EXPR("GROUP", OB_PROXY_EXPR_TYPE_FUNC_GROUP, ObProxyFuncExpr); 
    REG_EXPR("ORDER", OB_PROXY_EXPR_TYPE_FUNC_ORDER, ObProxyFuncExpr);

    REG_EXPR("TESTLOAD", OB_PROXY_EXPR_TYPE_FUNC_TESTLOAD, ObProxyExprTestLoad);
    REG_EXPR("SPLIT", OB_PROXY_EXPR_TYPE_FUNC_SPLIT, ObProxyExprSplit);
    REG_EXPR("TO_DATE", OB_PROXY_EXPR_TYPE_FUNC_TO_DATE, ObProxyExprToTimeHandler); // special case will not call
    REG_EXPR("TO_TIMESTAMP", OB_PROXY_EXPR_TYPE_FUNC_TO_TIMESTAMP, ObProxyExprToTimeHandler); // special case will not call
    REG_EXPR("NVL", OB_PROXY_EXPR_TYPE_FUNC_NVL, ObProxyExprNvl);
    REG_EXPR("TO_CHAR", OB_PROXY_EXPR_TYPE_FUNC_TO_CHAR, ObProxyExprToChar);
    REG_EXPR("SYSDATE", OB_PROXY_EXPR_TYPE_FUNC_SYSDATE, ObProxyExprSysdate);
    REG_EXPR("MOD", OB_PROXY_EXPR_TYPE_FUNC_MOD, ObProxyExprMod);
    REG_EXPR("ISNULL", OB_PROXY_EXPR_TYPE_FUNC_ISNULL, ObProxyExprIsnull);
    REG_EXPR("FLOOR", OB_PROXY_EXPR_TYPE_FUNC_FLOOR, ObProxyExprFloor);
    REG_EXPR("CEIL", OB_PROXY_EXPR_TYPE_FUNC_CEIL, ObProxyExprCeil);
    REG_EXPR("CEILING", OB_PROXY_EXPR_TYPE_FUNC_CEIL, ObProxyExprCeil);
    REG_EXPR("ROUND", OB_PROXY_EXPR_TYPE_FUNC_ROUND, ObProxyExprRound);
    REG_EXPR("TRUNCATE", OB_PROXY_EXPR_TYPE_FUNC_TRUNCATE, ObProxyExprTruncate);
    REG_EXPR("TRUNC", OB_PROXY_EXPR_TYPE_FUNC_TRUNCATE, ObProxyExprTruncate);
    REG_EXPR("ABS", OB_PROXY_EXPR_TYPE_FUNC_ABS, ObProxyExprAbs);
    REG_EXPR("SYSTIMESTAMP", OB_PROXY_EXPR_TYPE_FUNC_SYSTIMESTAMP, ObProxyExprSystimestamp);
    REG_EXPR("CURRENT_DATE", OB_PROXY_EXPR_TYPE_FUNC_CURRENT_DATE, ObProxyExprCurrentdate);
    REG_EXPR("CURDATE", OB_PROXY_EXPR_TYPE_FUNC_CURRENT_DATE, ObProxyExprCurrentdate);
    REG_EXPR("CURRENT_TIME", OB_PROXY_EXPR_TYPE_FUNC_CURRENT_TIME, ObProxyExprCurrenttime);
    REG_EXPR("CURTIME", OB_PROXY_EXPR_TYPE_FUNC_CURRENT_TIME, ObProxyExprCurrenttime);
    REG_EXPR("CURRENT_TIMESTAMP", OB_PROXY_EXPR_TYPE_FUNC_CURRENT_TIMESTAMP, ObProxyExprCurrenttimestamp);
    REG_EXPR("NOW", OB_PROXY_EXPR_TYPE_FUNC_CURRENT_TIMESTAMP, ObProxyExprCurrenttimestamp);

    REG_EXPR("TRIM", OB_PROXY_EXPR_TYPE_FUNC_TRIM, ObProxyExprTrim);
    REG_EXPR("LTRIM", OB_PROXY_EXPR_TYPE_FUNC_LTRIM, ObProxyExprLtrim);
    REG_EXPR("RTRIM", OB_PROXY_EXPR_TYPE_FUNC_RTRIM, ObProxyExprRtrim);
    REG_EXPR("SUBSTRING", OB_PROXY_EXPR_TYPE_FUNC_SUBSTR, ObProxyExprSubStr);
    REG_EXPR("REPLACE", OB_PROXY_EXPR_TYPE_FUNC_REPLACE, ObProxyExprReplace);
    REG_EXPR("LENGTH", OB_PROXY_EXPR_TYPE_FUNC_LENGTH, ObProxyExprLength);
    REG_EXPR("LOWER", OB_PROXY_EXPR_TYPE_FUNC_LOWER, ObProxyExprLower);
    REG_EXPR("LCASE", OB_PROXY_EXPR_TYPE_FUNC_LOWER, ObProxyExprLower);
    REG_EXPR("UPPER", OB_PROXY_EXPR_TYPE_FUNC_UPPER, ObProxyExprUpper);
    REG_EXPR("UCASE", OB_PROXY_EXPR_TYPE_FUNC_UPPER, ObProxyExprUpper);
    REG_EXPR("TO_NUMBER", OB_PROXY_EXPR_TYPE_FUNC_TO_NUMBER, ObProxyExprToNumber);
    REG_EXPR("NULL", OB_PROXY_EXPR_TYPE_FUNC_NULL, ObProxyExprNULL);

    // add new function above, this is the last
    // REG_EXPR("", OB_PROXY_EXPR_TYPE_MAX, );
  }
  return ret;
}

} // end opsql
} // end obproxy
} // end oceanbase