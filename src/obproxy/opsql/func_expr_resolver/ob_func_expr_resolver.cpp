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

#include "opsql/func_expr_resolver/ob_func_expr_resolver.h"
#include "common/ob_object.h"

namespace oceanbase
{
using namespace common;

namespace obproxy
{
namespace opsql
{

int ObFuncExprResolver::resolve(const ObProxyParamNode *node, ObProxyExpr *&expr)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(recursive_resolve_proxy_expr(node, expr))) {
    LOG_WARN("resursive resolve proxy expr failed", K(ret));
  }

  return ret;
}

int ObFuncExprResolver::recursive_resolve_proxy_expr(const ObProxyParamNode *node, ObProxyExpr *&expr)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    expr = NULL;
    switch (node->type_) {
      case PARAM_STR_VAL:
      {
        ObProxyExprConst *str_expr = NULL;
        if(OB_FAIL(ctx_.expr_factory_->create_proxy_expr(OB_PROXY_EXPR_TYPE_CONST, str_expr))) {
          LOG_WARN("create proxy expr failed", K(ret));
        } else {
          ObIAllocator &allocator = *ctx_.allocator_;
          char *buf = NULL;
          if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(node->str_value_.str_len_)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc failed", K(node->str_value_.str_len_), K(ret));
          } else {
            MEMCPY(buf, node->str_value_.str_, node->str_value_.str_len_);
            ObObj &obj = str_expr->get_object();
            obj.set_varchar(buf, node->str_value_.str_len_);
            obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
            expr = str_expr;
          }
        }
      }
      break;
      case PARAM_INT_VAL:
      {
        ObProxyExprConst *int_expr = NULL;
        if (OB_FAIL(ctx_.expr_factory_->create_proxy_expr(OB_PROXY_EXPR_TYPE_CONST, int_expr))) {
          LOG_WARN("create proxy expr failed", K(ret));
        } else {
          ObObj &obj = int_expr->get_object();
          obj.set_int(node->int_value_);
          expr = int_expr;
        }
      }
      break;
      case PARAM_COLUMN:
      {
        ObProxyExprColumn *column_expr = NULL;
        if (OB_FAIL(ctx_.expr_factory_->create_proxy_expr(OB_PROXY_EXPR_TYPE_COLUMN, column_expr))) {
          LOG_WARN("create proxy expr failed", K(ret));
        } else {
          ObIAllocator &allocator = *ctx_.allocator_;
          char *buf = NULL;
          if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(node->col_name_.str_len_)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc failed", K(node->col_name_.str_len_), K(ret));
          } else {
            MEMCPY(buf, node->col_name_.str_, node->col_name_.str_len_);
            column_expr->set_column_name(buf, node->col_name_.str_len_);
            expr = column_expr;
          }
        }
      }
      break;
      case PARAM_FUNC:
      {
        ObFuncExprNode *func_expr_node = node->func_expr_node_;
        ObProxyFuncExpr *func_expr = NULL;
        if (OB_FAIL(create_func_expr_by_type(func_expr_node, func_expr))) {
          LOG_WARN("create func expr by type failed", K(ret));
        } else {
          ObProxyParamNodeList *child = func_expr_node->child_;
          ObProxyParamNode* tmp_param_node = child->head_;

          for (int64_t i = 0; OB_SUCC(ret) && i < child->child_num_; i++) {
            ObProxyExpr *proxy_expr = NULL;
            if (OB_FAIL(recursive_resolve_proxy_expr(tmp_param_node, proxy_expr))) {
              LOG_WARN("recursive resolve proxy failed", K(ret));
            } else if (OB_FAIL(func_expr->add_param_expr(proxy_expr))) {
              LOG_WARN("add parma expr failed", K(ret));
            } else {
              tmp_param_node = tmp_param_node->next_;
            }
          }
        }

        if (OB_SUCC(ret)) {
          expr = func_expr;
        }
      }
      break;
      default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected type", K(node->type_), K(ret));
    }
  }

  return ret;
}

int ObFuncExprResolver::create_func_expr_by_type(const ObFuncExprNode *func_expr_node,
                                                 ObProxyFuncExpr *&func_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(func_expr_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    switch(func_expr_node->func_type_) {
      case OB_PROXY_EXPR_TYPE_FUNC_CONCAT:
      {
        ObProxyExprConcat *concat_expr = NULL;
        if (OB_FAIL(ctx_.expr_factory_->create_proxy_expr(OB_PROXY_EXPR_TYPE_FUNC_CONCAT, concat_expr))) {
          LOG_WARN("create proxy concat expr failed", K(ret));
        } else {
          func_expr = concat_expr;
        }
      }
      break;
      case OB_PROXY_EXPR_TYPE_FUNC_SUBSTR:
      {
        ObProxyExprSubStr *substr_expr = NULL;
        if (OB_FAIL(ctx_.expr_factory_->create_proxy_expr(OB_PROXY_EXPR_TYPE_FUNC_SUBSTR, substr_expr))) {
          LOG_WARN("create proxy substr expr failed", K(ret));
        } else {
          func_expr = substr_expr;
        }
      }
      break;
      case OB_PROXY_EXPR_TYPE_FUNC_HASH:
      {
        ObProxyExprHash *hash_expr = NULL;
        if (OB_FAIL(ctx_.expr_factory_->create_proxy_expr(OB_PROXY_EXPR_TYPE_FUNC_HASH, hash_expr))) {
          LOG_WARN("create proxy hash expr failed", K(ret));
        } else {
          func_expr = hash_expr;
        }
      }
      break;
      case OB_PROXY_EXPR_TYPE_FUNC_TOINT:
      {
        ObProxyExprToInt *toint_expr = NULL;
        if (OB_FAIL(ctx_.expr_factory_->create_proxy_expr(OB_PROXY_EXPR_TYPE_FUNC_TOINT, toint_expr))) {
          LOG_WARN("create proxy toint expr failed", K(ret));
        } else {
          func_expr = toint_expr;
        }
      }
      break;
      case OB_PROXY_EXPR_TYPE_FUNC_DIV:
      {
        ObProxyExprDiv *div_expr = NULL;
        if OB_FAIL(ctx_.expr_factory_->create_proxy_expr(OB_PROXY_EXPR_TYPE_FUNC_DIV, div_expr)) {
          LOG_WARN("create proxy div expr failed", K(ret));
        } else {
          func_expr = div_expr;
        }
      }
      break;
      case OB_PROXY_EXPR_TYPE_FUNC_ADD:
      {
        ObProxyExprAdd *add_expr = NULL;
        if OB_FAIL(ctx_.expr_factory_->create_proxy_expr(OB_PROXY_EXPR_TYPE_FUNC_ADD, add_expr)) {
          LOG_WARN("create proxy add expr failed", K(ret));
        } else {
          func_expr = add_expr;
        }
      }
      break;
      case OB_PROXY_EXPR_TYPE_FUNC_SUB:
      {
        ObProxyExprSub *sub_expr = NULL;
        if OB_FAIL(ctx_.expr_factory_->create_proxy_expr(OB_PROXY_EXPR_TYPE_FUNC_SUB, sub_expr)) {
          LOG_WARN("create proxy sub expr failed", K(ret));
        } else {
          func_expr = sub_expr;
        }
      }
      break;
      case OB_PROXY_EXPR_TYPE_FUNC_MUL:
      {
        ObProxyExprMul *mul_expr = NULL;
        if OB_FAIL(ctx_.expr_factory_->create_proxy_expr(OB_PROXY_EXPR_TYPE_FUNC_MUL, mul_expr)) {
          LOG_WARN("create proxy mul expr failed", K(ret));
        } else {
          func_expr = mul_expr;
        }
      }
      break;
      case OB_PROXY_EXPR_TYPE_FUNC_TESTLOAD:
      {
        ObProxyExprTestLoad *testload_expr = NULL;
        if OB_FAIL(ctx_.expr_factory_->create_proxy_expr(OB_PROXY_EXPR_TYPE_FUNC_TESTLOAD, testload_expr)) {
          LOG_WARN("create proxy testload expr failed", K(ret));
        } else {
          func_expr = testload_expr;
        }
      }
      break;
      case OB_PROXY_EXPR_TYPE_FUNC_SPLIT:
      {
        ObProxyExprSplit *split_expr = NULL;
        if (OB_FAIL(ctx_.expr_factory_->create_proxy_expr(OB_PROXY_EXPR_TYPE_FUNC_SPLIT, split_expr))) {
          LOG_WARN("create proxy split expr failed", K(ret));
        } else {
          func_expr = split_expr;
        }
      }
      break;
      default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected type", K(func_expr_node->func_type_), K(ret));
    }
  }

  return ret;
}

} // end opsql
} // end obproxy
} // end oceanbase
