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

#ifndef OB_FUNC_EXPR_PROXY_EXPR_TYPE_H
#define OB_FUNC_EXPR_PROXY_EXPR_TYPE_H

typedef enum ObProxyExprType
{
  OB_PROXY_EXPR_TYPE_NONE = 0,
  OB_PROXY_EXPR_TYPE_CONST,
  OB_PROXY_EXPR_TYPE_SHARDING_CONST,
  OB_PROXY_EXPR_TYPE_SHARDING_ALIAS,
  OB_PROXY_EXPR_TYPE_COLUMN,
  OB_PROXY_EXPR_TYPE_FUNC_HASH,
  OB_PROXY_EXPR_TYPE_FUNC_SUBSTR,
  OB_PROXY_EXPR_TYPE_FUNC_CONCAT,
  OB_PROXY_EXPR_TYPE_FUNC_TOINT,
  OB_PROXY_EXPR_TYPE_FUNC_DIV,
  OB_PROXY_EXPR_TYPE_FUNC_ADD,
  OB_PROXY_EXPR_TYPE_FUNC_SUB,
  OB_PROXY_EXPR_TYPE_FUNC_MUL,
  OB_PROXY_EXPR_TYPE_FUNC_SUM,
  OB_PROXY_EXPR_TYPE_FUNC_COUNT,
  OB_PROXY_EXPR_TYPE_FUNC_MAX,
  OB_PROXY_EXPR_TYPE_FUNC_MIN,
  OB_PROXY_EXPR_TYPE_FUNC_AVG,
  OB_PROXY_EXPR_TYPE_FUNC_ORDER,
  OB_PROXY_EXPR_TYPE_FUNC_TESTLOAD,
  OB_PROXY_EXPR_TYPE_FUNC_SPLIT,

  OB_PROXY_EXPR_TYPE_MAX,
}ObProxyExprType;

const char* get_expr_type_name(int expr_type)
{
  const char* type_name = "OB_PROXY_EXPR_TYPE_NONE";
  switch(expr_type) {
    case OB_PROXY_EXPR_TYPE_NONE:
      type_name = "OB_PROXY_EXPR_TYPE_NONE";
      break;
    case OB_PROXY_EXPR_TYPE_CONST:
      type_name = "OB_PROXY_EXPR_TYPE_CONST";
      break;
    case OB_PROXY_EXPR_TYPE_SHARDING_CONST:
      type_name = "OB_PROXY_EXPR_TYPE_SHARDING_CONST";
      break;
    case OB_PROXY_EXPR_TYPE_SHARDING_ALIAS:
      type_name = "OB_PROXY_EXPR_TYPE_SHARDING_ALIAS";
      break;
    case OB_PROXY_EXPR_TYPE_COLUMN:
      type_name = "OB_PROXY_EXPR_TYPE_COLUMN";
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_HASH:
      type_name = "OB_PROXY_EXPR_TYPE_FUNC_HASH";
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_SUBSTR:
      type_name = "OB_PROXY_EXPR_TYPE_FUNC_SUBSTR";
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_CONCAT:
      type_name = "OB_PROXY_EXPR_TYPE_FUNC_CONCAT";
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_TOINT:
      type_name = "OB_PROXY_EXPR_TYPE_FUNC_TOINT";
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_DIV:
      type_name = "OB_PROXY_EXPR_TYPE_FUNC_DIV";
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_ADD:
      type_name = "OB_PROXY_EXPR_TYPE_FUNC_ADD";
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_SUB:
      type_name = "OB_PROXY_EXPR_TYPE_FUNC_SUB";
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_MUL:
      type_name = "OB_PROXY_EXPR_TYPE_FUNC_MUL";
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_SUM:
      type_name = "OB_PROXY_EXPR_TYPE_FUNC_SUM";
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_COUNT:
      type_name = "OB_PROXY_EXPR_TYPE_FUNC_COUNT";
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_MAX:
      type_name = "OB_PROXY_EXPR_TYPE_FUNC_MAX";
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_MIN:
      type_name = "OB_PROXY_EXPR_TYPE_FUNC_MIN";
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_AVG:
      type_name = "OB_PROXY_EXPR_TYPE_FUNC_AVG";
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_ORDER:
      type_name = "OB_PROXY_EXPR_TYPE_FUNC_ORDER";
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_TESTLOAD:
      type_name = "OB_PROXY_EXPR_TYPE_FUNC_TESTLOAD";
      break;
    case OB_PROXY_EXPR_TYPE_MAX:
      type_name = "OB_PROXY_EXPR_TYPE_MAX";
      break;
    default:
      break;
  }
  return type_name;
}
#endif