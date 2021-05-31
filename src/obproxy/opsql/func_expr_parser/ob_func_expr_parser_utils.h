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

  int64_t to_string(char* buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    ObFuncExprNode *func_expr_node = result_.param_node_->func_expr_node_;
    ObProxyParamNodeList *child = func_expr_node->child_;
    J_OBJ_START();
    VBUF_PRINTF("function_type:%s, param_num:%ld, ", get_generate_function_type(func_expr_node->func_type_), child->child_num_);
    ObProxyParamNode *param = child->head_;
    for (int64_t i = 0; i < child->child_num_; ++i) {
      if (NULL != param) {
        if (PARAM_COLUMN == param->type_) {
          VBUF_PRINTF("param[%ld]:[type:%s, value:%.*s],", i,
                      get_func_param_type(param->type_),
                      param->col_name_.str_len_, param->col_name_.str_);
        } else if (PARAM_INT_VAL == param->type_) {
          VBUF_PRINTF("param[%ld]:[type:%s, value:%ld],", i,
                      get_func_param_type(param->type_),
                      param->int_value_);
        } else if (PARAM_STR_VAL == param->type_) {
          VBUF_PRINTF("param[%ld]:[type:%s, value:%.*s],", i,
                      get_func_param_type(param->type_),
                      param->str_value_.str_len_, param->str_value_.str_);
        }
      }
      param = param->next_;
    }
    J_OBJ_END();
    return pos;
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
