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

#ifndef OB_FUNC_EXPR_PARSER_UTILS_H
#define OB_FUNC_EXPR_PARSER_UTILS_H
#include "lib/utility/ob_print_utils.h"
#include "opsql/func_expr_parser/ob_func_expr_parse_result.h"
#define VBUF_PRINTF(args...) (void)::oceanbase::common::databuff_printf(buf, buf_len, pos, ##args)

namespace oceanbase
{
namespace obproxy
{
namespace opsql
{

class ObFuncExprParseResultPrintWrapper
{
public:
  explicit ObFuncExprParseResultPrintWrapper(const ObFuncExprParseResult &result) :result_(result) {}

  inline void print_indent(char *buf, const int64_t buf_len, int64_t &pos, const int64_t level) const
  {
    // ignore return here
    for (int64_t i = 0; i < level; ++i) {
      VBUF_PRINTF("  ");
    }
  }

  inline bool is_valid_print_func_node(const ObProxyParamNode *node) const
  {
    return OB_NOT_NULL(node) &&
           PARAM_FUNC == node->type_ &&
          
           OB_NOT_NULL(node->func_expr_node_) &&
           OB_NOT_NULL(node->func_expr_node_->func_name_.str_) &&
           node->func_expr_node_->func_name_.str_len_ >= 0 &&
           OB_NOT_NULL(node->func_expr_node_->child_);
  }

  int64_t to_string(char* buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    VBUF_PRINTF("{\n");
    if (OB_NOT_NULL(result_.param_node_)) {
      print_func_param(buf, buf_len, pos, 0, 0, result_.param_node_, 1);
    }
    VBUF_PRINTF("}\n");
    return pos;
  }
  void print_func_param(char *buf, const int64_t buf_len, int64_t &pos,
                       const int64_t level, const int64_t param_index,
                       const ObProxyParamNode *node,
                       const int64_t child_num) const {
    if (OB_NOT_NULL(node) && child_num > 0) {
      print_indent(buf, buf_len, pos, level);
      if (PARAM_COLUMN == node->type_ && node->col_name_.str_len_ > 0 && OB_NOT_NULL(node->col_name_.str_)) {
        VBUF_PRINTF("param[%ld]:[type:%s, value:%.*s], \n", param_index, get_func_param_type(node->type_), node->col_name_.str_len_, node->col_name_.str_);
      } else if (PARAM_INT_VAL == node->type_) {
        VBUF_PRINTF("param[%ld]:[type:%s, value:%ld], \n", param_index, get_func_param_type(node->type_), node->int_value_);
      } else if (PARAM_STR_VAL == node->type_ && node->str_value_.str_len_ > 0 && OB_NOT_NULL(node->str_value_.str_)) {
        VBUF_PRINTF("param[%ld]:[type:%s, value:%.*s], \n", param_index, get_func_param_type(node->type_),node->str_value_.str_len_, node->str_value_.str_);
      } else if (PARAM_FUNC == node->type_) {
        if (is_valid_print_func_node(node)) {
          const ObProxyParamNodeList *child = node->func_expr_node_->child_;
          const ObProxyParamNode *param_node = child->head_;
          VBUF_PRINTF("param[%ld]:[func:%.*s], param_num:%ld\n", param_index,
                      node->func_expr_node_->func_name_.str_len_,
                      node->func_expr_node_->func_name_.str_,
                      child->child_num_);
          print_indent(buf, buf_len, pos, level);
          VBUF_PRINTF("{\n");
          for (int64_t i = 0; i < child->child_num_ && OB_NOT_NULL(param_node); i++, param_node = param_node->next_) {
            print_func_param(buf, buf_len, pos, level + 1, i, param_node, 1);
          }
          print_indent(buf, buf_len, pos, level);
          VBUF_PRINTF("}\n");
        }
      } else if (PARAM_NULL == node->type_) {
        VBUF_PRINTF("param[%ld]:[type:%s, value: NULL], \n", param_index, get_func_param_type(node->type_));
      }
      print_func_param(buf, buf_len, pos, level, param_index + 1, node->next_, child_num - 1);
    }
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObFuncExprParseResultPrintWrapper);
private:
  const ObFuncExprParseResult &result_;
};


} // end namespace opsql
} // end namespace obproxy
} // end namespace oceanbase

#endif /* OB_FUNC_EXPR_PARSER_UTILS_H */
