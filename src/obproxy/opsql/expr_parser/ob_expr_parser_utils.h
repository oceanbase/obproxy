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

#ifndef OB_EXPR_PARSER_UTILS_H
#define OB_EXPR_PARSER_UTILS_H
#include "lib/utility/ob_print_utils.h"
#include "opsql/expr_parser/ob_expr_parse_result.h"
#define VBUF_PRINTF(args...) (void)::oceanbase::common::databuff_printf(buf, buf_len, pos, ##args)

namespace oceanbase
{
namespace obproxy
{
namespace opsql
{
inline void print_indent(char *buf, const int64_t buf_len, int64_t &pos, const int64_t level)
{
  // ignore return here
  for (int64_t i = 0; i < level; ++i) {
    VBUF_PRINTF("  ");
  }
}

void print_token_list(char *buf, const int64_t buf_len, int64_t &pos, const int64_t level,
                      const ObProxyTokenList *list);
void print_token_node(char *buf, const int64_t buf_len, int64_t &pos, const int64_t level,
                      const ObProxyTokenNode *node)
{
  print_indent(buf, buf_len, pos, level);
  if (NULL != node) {
    VBUF_PRINTF("type:%s,", get_obproxy_token_type(node->type_));
    switch (node->type_) {
      case TOKEN_FUNC:
        VBUF_PRINTF(" name:%.*s, childs: \n", node->str_value_.str_len_, node->str_value_.str_);
        print_token_list(buf, buf_len, pos, level + 1, node->child_);
        break;

      case TOKEN_OPERATOR:
        VBUF_PRINTF(" value:%s,\n", get_obproxy_operator_type(node->operator_));
        break;

      case TOKEN_STR_VAL:
        VBUF_PRINTF(" value:%.*s,\n", node->str_value_.str_len_, node->str_value_.str_);
        break;

      case TOKEN_INT_VAL:
        VBUF_PRINTF(" value:%ld,\n", node->int_value_);
        break;

      case TOKEN_COLUMN:
        VBUF_PRINTF(" value:%ld,\n", node->part_key_idx_);
        break;

      case TOKEN_PLACE_HOLDER:
        VBUF_PRINTF(" value:%ld,\n", node->placeholder_idx_);
        break;

      case TOKEN_HEX_VAL:
        VBUF_PRINTF(" value:%.*s,\n", node->str_value_.str_len_, node->str_value_.str_);
        break;

      case TOKEN_NONE:
      default:
        break;
    }
  }
}

void print_token_list(char *buf, const int64_t buf_len, int64_t &pos, const int64_t level,
                      const ObProxyTokenList *list)
{
  print_indent(buf, buf_len, pos, level);
  VBUF_PRINTF("{\n");
  if (NULL != list) {
    ObProxyTokenNode *node = list->head_;
    while (NULL != node) {
      print_token_node(buf, buf_len, pos, level + 1, node);
      node = node->next_;
    }
  }
  print_indent(buf, buf_len, pos, level);
  VBUF_PRINTF("}\n");
}

void print_relation(char *buf, const int64_t buf_len, int64_t &pos, const int64_t level,
                    const ObProxyRelationExpr *relation)
{
  print_indent(buf, buf_len, pos, level);
  VBUF_PRINTF("{\n");
  if (NULL != relation) {
    print_indent(buf, buf_len, pos, level + 1);
    VBUF_PRINTF("level:%s\n", get_obproxy_part_key_level(relation->level_));

    print_indent(buf, buf_len, pos, level + 1);
    VBUF_PRINTF("type:%s\n", get_obproxy_function_type(relation->type_));

    print_indent(buf, buf_len, pos, level + 1);
    VBUF_PRINTF("right_value:\n");
    print_token_list(buf, buf_len, pos, level + 1, relation->right_value_);
  }
  print_indent(buf, buf_len, pos, level);
  VBUF_PRINTF("}\n");
}

class ObExprParseResultPrintWrapper
{
public:
  explicit ObExprParseResultPrintWrapper(const ObExprParseResult &result) :result_(result) {}

  int64_t to_string(char* buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_OBJ_START();
    VBUF_PRINTF("has_rowid_: %d\n", result_.has_rowid_);
    for (int64_t i = 0; i < result_.relation_info_.relation_num_; ++i) {
      print_relation(buf, buf_len, pos, 0, result_.relation_info_.relations_[i]);
    }
    J_OBJ_END();
    return pos;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprParseResultPrintWrapper);
private:
  const ObExprParseResult &result_;
};

class ObProxyPartKeyInfoPrintWrapper
{
public:
  explicit ObProxyPartKeyInfoPrintWrapper(const ObProxyPartKeyInfo &key_info)
    : part_key_info_(key_info)
  {}

  int64_t to_string(char* buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_OBJ_START();
    for (int64_t i = 0; i < part_key_info_.key_num_; ++i) {
      VBUF_PRINTF("key[%ld]:[name:%.*s, level:%s],", i,
                  part_key_info_.part_keys_[i].name_.str_len_,
                  part_key_info_.part_keys_[i].name_.str_,
                  get_obproxy_part_key_level(part_key_info_.part_keys_[i].level_));
    }
    J_OBJ_END();
    return pos;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObProxyPartKeyInfoPrintWrapper);
private:
  const ObProxyPartKeyInfo &part_key_info_;
};

} // end namespace opsql
} // end namespace obproxy
} // end namespace oceanbase

#endif /* OB_EXPR_PARSER_UTILS_H */
