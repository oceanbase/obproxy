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

#ifndef OB_EXPR_PARSE_RESULT_H
#define OB_EXPR_PARSE_RESULT_H

#include <stdint.h>
#include <stdbool.h>
#include <setjmp.h>
#include <strings.h>
#include <stddef.h>
#include "opsql/ob_proxy_parse_type.h"
#include "opsql/func_expr_parser/ob_func_expr_parse_result.h"

#define OBPROXY_MAX_NAME_LENGTH 128
#define OBPROXY_MAX_PART_LEVEL 16
#define OBPROXY_MAX_RELATION_NUM 64
#define OBPROXY_MAX_PART_KEY_NUM 16

#define NO_BOUND_FLAG 0
#define LOW_BOUND_FLAG 1
#define HIGH_BOUND_FLAG (1 << 1)
#define BOTH_BOUND_FLAG (LOW_BOUND_FLAG | HIGH_BOUND_FLAG)
#define MASK_OFFSET 2
#define FIRST_PART_LOW_BOUND_MASK 1
#define FIRST_PART_HIGH_BOUND_MASK (1 << 1)
#define SUB_PART_LOW_BOUND_MASK (1 << 2)
#define SUB_PART_HIGH_BOUND_MASK (1 << 3)
#define FIRST_PART_MASK (FIRST_PART_LOW_BOUND_MASK | FIRST_PART_HIGH_BOUND_MASK )
#define SUB_PART_MASK (SUB_PART_LOW_BOUND_MASK | SUB_PART_HIGH_BOUND_MASK)
#define BOTH_PART_MASK (FIRST_PART_MASK | SUB_PART_MASK)
#define GET_FIRST_PART_MASK(flag) (flag)
#define GET_SUB_PART_MASK(flag) (flag << MASK_OFFSET)

#define IDX_NO_PART_KEY_COLUMN -1

typedef enum ObExprParseMode
{
  INVALID_PARSE_MODE = 0,
  SELECT_STMT_PARSE_MODE, // select/delete stmt
  INSERT_STMT_PARSE_MODE, // insert/replace/update stmt
} ObExprParseMode;

typedef enum ObProxyFunctionType
{
  F_NONE,

  F_OP_AND,
  F_OP_OR,

  F_COMP_START,
  F_COMP_EQ,
  F_COMP_NSEQ,
  F_COMP_GE,
  F_COMP_GT,
  F_COMP_LE,
  F_COMP_LT,
  F_COMP_NE,
} ObProxyFunctionType;

typedef enum ObProxyOperatorType
{
  OPT_NONE,
  OPT_ADD,
  OPT_MINUS,
  OPT_MUL,
  OPT_DIV,
  OPT_MOD,
  OPT_AND,
  OPT_NOT,
  OPT_COMMA,
} ObProxyOperatorType;

typedef enum ObProxyTokenType
{
  TOKEN_NONE = 0,
  TOKEN_FUNC,
  TOKEN_OPERATOR,
  TOKEN_STR_VAL,
  TOKEN_INT_VAL,
  TOKEN_COLUMN,
  TOKEN_PLACE_HOLDER,
  TOKEN_HEX_VAL,
} ObProxyTokenType;

typedef enum ObProxyPartKeyLevel
{
  PART_KEY_LEVEL_ZERO = 0,
  PART_KEY_LEVEL_ONE,
  PART_KEY_LEVEL_TWO,
  PART_KEY_LEVEL_BOTH, // a part key is in both first partition expr and sub partition expr
} ObProxyPartKeyLevel;

struct _ObProxyTokenList;
typedef struct _ObProxyTokenNode
{
  ObProxyTokenType type_;
  union
  {
    int64_t             int_value_;
    int64_t             part_key_idx_;
    int64_t             placeholder_idx_;
    ObProxyParseString  str_value_;
    ObProxyOperatorType operator_;
  };

  ObProxyParseString  column_name_;
  struct _ObProxyTokenList *child_;
  struct _ObProxyTokenNode *next_;
} ObProxyTokenNode;

typedef struct _ObProxyTokenList
{
  ObProxyTokenNode *column_node_;
  ObProxyTokenNode *head_;
  ObProxyTokenNode *tail_;
} ObProxyTokenList;

/**
 * @brief If level_ == PART_KEY_LEVEL_ONE, first_part_column_idx_ will be set.
 *        If level_ == PART_KEY_LEVEL_TWO, second_part_column_idx_ will be set.
 *        If level_ == PART_KEY_LEVEL_BOTH, both of them will be set.
 *        first_part_column_idx_: the column's idx in partition expression
 *        second_part_column_idx_: the column's idx in subpartition expression
 */
typedef struct _ObProxyRelationExpr
{
  int64_t column_idx_;
  int64_t first_part_column_idx_;
  int64_t second_part_column_idx_;
  ObProxyTokenList *left_value_;
  ObProxyTokenList *right_value_;
  ObProxyFunctionType type_;
  ObProxyPartKeyLevel level_;
} ObProxyRelationExpr;

/*
 * according to observer rs colleague, we use the triple to describe all type of the accuracy
 * which is necessary to describe the accuracy of each part key type
 * table: oceanbase.__all_virtual_proxy_partition_info
 *   -> column spare5: VARCHAR, format: "int32_t,int16_t,int16_t"
 * which means "length,precision/length_semantics,scale"
 */
typedef struct _ObProxyPartKeyAccuracy {
  int8_t valid_;         // default is 0, means not valid
  int32_t length_;
  int16_t precision_;    // the same as length_semantics
  int16_t scale_;
} ObProxyPartKeyAccuracy;

typedef struct _ObProxyPartKey
{
  ObProxyParseString name_;
  ObProxyParseString default_value_; // serialized value
  ObProxyPartKeyLevel level_;
  int64_t idx_; // pos in schema columns 
  int64_t obj_type_; // ObObjType
  int64_t cs_type_; // ObCollationType 
  bool is_exist_in_sql_;  // is part key exist in sql

  bool is_generated_;
  int64_t generated_col_idx_;
  int64_t param_num_;
  ObProxyExprType func_type_;
  ObProxyParamNode *params_[OBPROXY_MAX_PARAM_NUM]; // used to store generated func param
  int64_t real_source_idx_; // the real source idx of generated key, have no params_
  int64_t idx_in_rowid_;        // pos in rowid
  int64_t idx_in_part_columns_; // pos in part expr columns
  ObProxyPartKeyAccuracy accuracy_;
} ObProxyPartKey;

typedef struct _ObProxyPartKeyInfo
{
  ObProxyPartKey part_keys_[OBPROXY_MAX_PART_KEY_NUM];
  int64_t key_num_;
} ObProxyPartKeyInfo;

typedef struct _ObProxyRelationInfo
{
  ObProxyRelationExpr *relations_[OBPROXY_MAX_RELATION_NUM];
  int64_t relation_num_;
  int64_t right_value_num_;
} ObProxyRelationInfo;

typedef struct _ObExprParseResult
{
  // input argument
  void *malloc_pool_; // ObIAllocator
  bool is_oracle_mode_;
  ObExprParseMode parse_mode_;
  ObProxyTableInfo table_info_;
  ObProxyPartKeyInfo part_key_info_;

  // scanner buffer
  void *yyscan_info_; // yy_scan_t
  char *tmp_buf_;
  char *tmp_start_ptr_;
  int32_t tmp_len_;
  jmp_buf jmp_buf_; // handle fatal error
  const char *start_pos_;
  const char *end_pos_;
  int64_t column_idx_;
  int64_t values_list_idx_;
  int64_t multi_param_values_;
  int64_t placeholder_list_idx_;
  bool need_parse_token_list_;

  // result argument
  ObProxyRelationInfo relation_info_;
  ObProxyRelationInfo all_relation_info_;

  // hash rowid or not
  bool has_rowid_;
} ObExprParseResult;

static const char *g_ROWID = "ROWID";
static inline bool is_equal_to_rowid(ObProxyParseString *str)
{
  bool bret = false;
  if (str == NULL || str->str_ == NULL || str->str_len_ != 5) {
    // ret
  } else {
    bret = (strncasecmp(str->str_, g_ROWID, 5) == 0) ? true : false;
  }
  return bret;
}

#ifdef __cplusplus
extern "C" const char* get_expr_parse_mode(const ObExprParseMode mode);
extern "C" const char* get_obproxy_function_type(const ObProxyFunctionType type);
extern "C" const char* get_obproxy_operator_type(const ObProxyOperatorType type);
extern "C" const char* get_obproxy_token_type(const ObProxyTokenType type);
extern "C" const char* get_obproxy_part_key_level(const ObProxyPartKeyLevel level);
#else
const char* get_expr_parse_mode(const ObExprParseMode mode);
const char* get_obproxy_function_type(const ObProxyFunctionType type);
const char* get_obproxy_operator_type(const ObProxyOperatorType type);
const char* get_obproxy_token_type(const ObProxyTokenType type);
const char* get_obproxy_part_key_level(const ObProxyPartKeyLevel level);
#endif

#endif // end of OB_EXPR_PARSE_RESULT_H
