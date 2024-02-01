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

#ifndef OB_FUNC_EXPR_PARSE_RESULT_H
#define OB_FUNC_EXPR_PARSE_RESULT_H

#include <stdint.h>
#include <stdbool.h>
#include <setjmp.h>
#include "opsql/ob_proxy_parse_type.h"
#include "opsql/func_expr_resolver/proxy_expr/ob_proxy_expr_type.h"

#define OBPROXY_MAX_PARAM_NUM 3
#define OBPROXY_MAX_NAME_LENGTH 128

typedef enum ObFuncExprParseMode
{
  INVLIAD_FUNC_PARSE_MODE = 0,
  GENERATE_FUNC_PARSE_MODE, // generated function
  SHARDING_EXPR_FUNC_PARSE_MODE, // sharding expr function
} ObFuncExprParseMode;

typedef enum ObProxyParamType
{
  PARAM_NONE = 0,
  PARAM_STR_VAL,
  PARAM_INT_VAL,
  PARAM_COLUMN,
  PARAM_FUNC,
  PARAM_NULL,
} ObProxyParamType;

struct _ObProxyParamNodeList;

typedef struct _ObFuncExprNode
{
  ObProxyParseString func_name_;
  struct _ObProxyParamNodeList *child_;
} ObFuncExprNode;

typedef struct _ObProxyParamNode
{
  ObProxyParamType type_;
  union
  {
    int64_t             int_value_;
    ObProxyParseString  col_name_;
    ObProxyParseString  str_value_;
    ObFuncExprNode *func_expr_node_;
  };
  struct _ObProxyParamNode *next_;
} ObProxyParamNode;

typedef struct _ObProxyParamNodeList
{
  ObProxyParamNode *head_;
  ObProxyParamNode *tail_;
  int64_t child_num_;
} ObProxyParamNodeList;

typedef struct _ObFuncExprParseResult
{
  // input argument
  void *malloc_pool_; // ObIAllocator
  ObFuncExprParseMode parse_mode_;

  // scanner buffer
  void *yyscan_info_; // yy_scan_t
  char *tmp_buf_;
  char *tmp_start_ptr_;
  int32_t tmp_len_;
  jmp_buf jmp_buf_; // handle fatal error
  const char *start_pos_;
  const char *end_pos_;

  // expr info
  ObProxyParamNode *param_node_;
} ObFuncExprParseResult;

#ifdef __cplusplus
extern "C" const char* get_func_expr_parse_mode(const ObFuncExprParseMode);
extern "C" const char* get_generate_function_type(const ObProxyExprType type);
extern "C" const char* get_func_param_type(const ObProxyParamType type);
#else
const char* get_func_expr_parse_mode(const ObFuncExprParseMode);
const char* get_generate_function_type(const ObProxyExprType type);
const char* get_func_param_type(const ObProxyParamType type);
#endif

#endif // end of OB_FUNC_EXPR_PARSE_RESULT_H
