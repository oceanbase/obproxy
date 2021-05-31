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

#include "opsql/expr_parser/ob_expr_parse_result.h"

const char* get_expr_parse_mode(const ObExprParseMode mode)
{
  const char *str_ret = "invalid mode";
  switch (mode) {
    case SELECT_STMT_PARSE_MODE:
      str_ret = "SELECT_STMT_PARSE_MODE";
      break;
    case INSERT_STMT_PARSE_MODE:
      str_ret = "INSERT_STMT_PARSE_MODE";
      break;
    default:
      break;
  }
  return str_ret;
}

const char* get_obproxy_function_type(const ObProxyFunctionType type)
{
  const char *str_ret = "invalid function type";
  switch (type) {
    case F_NONE:
      str_ret = "F_NONE";
      break;
    case F_OP_AND:
      str_ret = "F_OP_AND";
      break;
    case F_OP_OR:
      str_ret = "F_OP_OR";
      break;
    case F_COMP_START:
      str_ret = "F_COMP_START";
      break;
    case F_COMP_EQ:
      str_ret = "F_COMP_EQ";
      break;
    case F_COMP_NSEQ:
      str_ret = "F_COMP_NSEQ";
      break;
    case F_COMP_GE:
      str_ret = "F_COMP_GE";
      break;
    case F_COMP_GT:
      str_ret = "F_COMP_GT";
      break;
    case F_COMP_LE:
      str_ret = "F_COMP_LE";
      break;
    case F_COMP_LT:
      str_ret = "F_COMP_LT";
      break;
    case F_COMP_NE:
      str_ret = "F_COMP_NE";
      break;
    default:
      break;
  }
  return str_ret;
}

const char* get_obproxy_operator_type(const ObProxyOperatorType type)
{
  const char *str_ret = "invalid operator type";
  switch (type) {
    case OPT_NONE:
      str_ret = "OPT_NONE";
      break;
    case OPT_ADD:
      str_ret = "OPT_ADD";
      break;
    case OPT_MINUS:
      str_ret = "OPT_MINUS";
      break;
    case OPT_MUL:
      str_ret = "OPT_MUL";
      break;
    case OPT_DIV:
      str_ret = "OPT_DIV";
      break;
    case OPT_MOD:
      str_ret = "OPT_MOD";
      break;
    case OPT_AND:
      str_ret = "OPT_AND";
      break;
    case OPT_NOT:
      str_ret = "OPT_NOT";
      break;
    case OPT_COMMA:
      str_ret = "OPT_COMMA";
      break;
    default:
      break;
  }
  return str_ret;
}

const char* get_obproxy_token_type(const ObProxyTokenType type)
{
  const char *str_ret = "invalid token type";
  switch (type) {
    case TOKEN_NONE:
      str_ret = "TOKEN_NONE";
      break;
    case TOKEN_FUNC:
      str_ret = "TOKEN_FUNC";
      break;
    case TOKEN_OPERATOR:
      str_ret = "TOKEN_OPERATOR";
      break;
    case TOKEN_STR_VAL:
      str_ret = "TOKEN_STR_VAL";
      break;
    case TOKEN_INT_VAL:
      str_ret = "TOKEN_INT_VAL";
      break;
    case TOKEN_COLUMN:
      str_ret = "TOKEN_COLUMN";
      break;
    case TOKEN_PLACE_HOLDER:
      str_ret = "TOKEN_PLACE_HOLDER";
      break;
    default:
      break;
  }
  return str_ret;
}

const char* get_obproxy_part_key_level(const ObProxyPartKeyLevel level)
{
  const char *str_ret = "invalid part key level";
  switch (level) {
    case PART_KEY_LEVEL_ZERO:
      str_ret = "PART_KEY_LEVEL_ZERO";
      break;
    case PART_KEY_LEVEL_ONE:
      str_ret = "PART_KEY_LEVEL_ONE";
      break;
    case PART_KEY_LEVEL_TWO:
      str_ret = "PART_KEY_LEVEL_TWO";
      break;
    case PART_KEY_LEVEL_BOTH:
      str_ret = "PART_KEY_LEVEL_BOTH";
      break;
    default:
      break;
  }
  return str_ret;
}


