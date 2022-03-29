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

#include "opsql/func_expr_parser/ob_func_expr_parse_result.h"

const char* get_func_expr_parse_mode(const ObFuncExprParseMode mode)
{
  const char *str_ret = "invalid mode";
  switch (mode) {
    case GENERATE_FUNC_PARSE_MODE:
      str_ret = "GENERATE_FUNC_PARSE_MODE";
      break;
    default:
      break;
  }
  return str_ret;
}

const char* get_generate_function_type(const ObProxyExprType type)
{
  const char *str_ret = "invalid function type";
  switch (type) {
    case OB_PROXY_EXPR_TYPE_NONE:
      str_ret = "OB_PROXY_EXPR_TYPE_NONE";
      break;
    case OB_PROXY_EXPR_TYPE_CONST:
      str_ret = "OB_PROXY_EXPR_TYPE_CONST";
      break;
    case OB_PROXY_EXPR_TYPE_SHARDING_CONST:
      str_ret = "OB_PROXY_EXPR_TYPE_SHARDING_CONST";
      break;
    case OB_PROXY_EXPR_TYPE_TABLE:
      str_ret = "OB_PROXY_EXPR_TYPE_TABLE";
      break;
    case OB_PROXY_EXPR_TYPE_COLUMN:
      str_ret = "OB_PROXY_EXPR_TYPE_COLUMN";
      break;
    case OB_PROXY_EXPR_TYPE_STAR:
      str_ret = "OB_PROXY_EXPR_TYPE_STAR";
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_HASH:
      str_ret = "OB_PROXY_EXPR_TYPE_FUNC_HASH";
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_SUBSTR:
      str_ret = "OB_PROXY_EXPR_TYPE_FUNC_SUBSTR";
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_CONCAT:
      str_ret = "OB_PROXY_EXPR_TYPE_FUNC_CONCAT";
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_TOINT:
      str_ret = "OB_PROXY_EXPR_TYPE_FUNC_TOINT";
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_DIV:
      str_ret = "OB_PROXY_EXPR_TYPE_FUNC_DIV";
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_ADD:
      str_ret = "OB_PROXY_EXPR_TYPE_FUNC_ADD";
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_SUB:
      str_ret = "OB_PROXY_EXPR_TYPE_FUNC_SUB";
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_MUL:
      str_ret = "OB_PROXY_EXPR_TYPE_FUNC_MUL";
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_SUM:
      str_ret = "OB_PROXY_EXPR_TYPE_FUNC_SUM";
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_COUNT:
      str_ret = "OB_PROXY_EXPR_TYPE_FUNC_COUNT";
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_MAX:
      str_ret = "OB_PROXY_EXPR_TYPE_FUNC_MAX";
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_MIN:
      str_ret = "OB_PROXY_EXPR_TYPE_FUNC_MIN";
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_AVG:
      str_ret = "OB_PROXY_EXPR_TYPE_FUNC_AVG";
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_ORDER:
      str_ret = "OB_PROXY_EXPR_TYPE_FUNC_ORDER";
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_GROUP:
      str_ret = "OB_PROXY_EXPR_TYPE_FUNC_GROUP";
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_TESTLOAD:
      str_ret = "OB_PROXY_EXPR_TYPE_FUNC_TESTLOAD";
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_SPLIT:
      str_ret = "OB_PROXY_EXPR_TYPE_FUNC_SPLIT";
      break;
    default:
      break;
  }
  return str_ret;
}

const char* get_func_param_type(const ObProxyParamType type)
{
  const char *str_ret = "invalid param type";
  switch (type) {
    case PARAM_NONE:
      str_ret = "PARAM_NONE";
      break;
    case PARAM_STR_VAL:
      str_ret = "PARAM_STR_VAL";
      break;
    case PARAM_INT_VAL:
      str_ret = "PARAM_INT_VAL";
      break;
    case PARAM_COLUMN:
      str_ret = "PARAM_COLUMN";
      break;
    case PARAM_FUNC:
      str_ret = "PARAM_FUNC";
    default:
      break;
  }
  return str_ret;
}

