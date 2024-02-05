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
    LOG_WDIAG("resursive resolve proxy expr failed", K(ret));
  }

  return ret;
}

int ObFuncExprResolver::recursive_resolve_proxy_expr(const ObProxyParamNode *node, ObProxyExpr *&expr)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(ret));
  } else {
    expr = NULL;
    switch (node->type_) {
      case PARAM_STR_VAL:
      {
        ObProxyExprConst *str_expr = NULL;
        if(OB_FAIL(ctx_.expr_factory_->create_proxy_expr(OB_PROXY_EXPR_TYPE_CONST, str_expr))) {
          LOG_WDIAG("create proxy expr failed", K(ret));
        } else if (0 == node->str_value_.str_len_) {
          ObObj &obj = str_expr->get_object();
          obj.set_varchar(NULL, 0);
          obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
          expr = str_expr;
        } else if (OB_ISNULL(node->str_value_.str_)) {
          // handle the case where input is null, for example isnull(null) = 1
          ObObj &obj = str_expr->get_object();
          obj.set_null();
          expr = str_expr;
        } else {
          ObIAllocator &allocator = *ctx_.allocator_;
          char *buf = NULL;
          if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(node->str_value_.str_len_)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WDIAG("alloc failed", K(node->str_value_.str_len_), K(ret));
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
          LOG_WDIAG("create proxy expr failed", K(ret));
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
          LOG_WDIAG("create proxy expr failed", K(ret));
        } else {
          ObIAllocator &allocator = *ctx_.allocator_;
          char *buf = NULL;
          if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(node->col_name_.str_len_)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WDIAG("alloc failed", K(node->col_name_.str_len_), K(ret));
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
          LOG_WDIAG("create func expr by type failed", K(ret));
        } else if (OB_ISNULL(func_expr_node->child_)) {
          // do nothing 
        } else {
          ObProxyParamNodeList *child = func_expr_node->child_;
          ObProxyParamNode* tmp_param_node = child->head_;

          for (int64_t i = 0; OB_SUCC(ret) && i < child->child_num_; i++) {
            ObProxyExpr *proxy_expr = NULL;
            if (OB_FAIL(recursive_resolve_proxy_expr(tmp_param_node, proxy_expr))) {
              LOG_WDIAG("recursive resolve proxy failed", K(ret));
            } else if (OB_FAIL(func_expr->add_param_expr(proxy_expr))) {
              LOG_WDIAG("add parma expr failed", K(ret));
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
      case PARAM_NULL:
      {
        ObProxyExprConst *null_expr = NULL;
        if (OB_FAIL(ctx_.expr_factory_->create_proxy_expr(OB_PROXY_EXPR_TYPE_CONST, null_expr))) {
          LOG_WDIAG("create proxy expr failed", K(ret));
        } else {
          ObObj &obj = null_expr->get_object();
          obj.set_null();
          expr = null_expr;
        }
        break;
      }
      default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unexpected type", K(node->type_), K(ret));
    }
  }

  return ret;
}

int ObFuncExprResolver::create_func_expr_by_type(const ObFuncExprNode *func_expr_node,
                                                 ObProxyFuncExpr *&func_expr)
{
  int ret = OB_SUCCESS;
  ObString func_name;
  ObProxyExprType type = OB_PROXY_EXPR_TYPE_NONE;
  if (OB_ISNULL(func_expr_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(ret));
  } else if (OB_ISNULL(func_expr_node->func_name_.str_) || func_expr_node->func_name_.str_len_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (FALSE_IT(func_name = ObString(func_expr_node->func_name_.str_len_, func_expr_node->func_name_.str_))) {
  } else if (OB_FAIL(ObProxyExprFactory::get_type_by_name(func_name, type))) {
    LOG_WDIAG("fail to get func type by name", K(func_name), K(type), K(ret));
  } else if (type == OB_PROXY_EXPR_TYPE_NONE) {
    LOG_INFO("unsupported function", K(func_name));
    ret = OB_ERR_FUNCTION_UNKNOWN;
  } else if (OB_PROXY_EXPR_TYPE_FUNC_TO_DATE == type) {
    ObProxyExprToTimeHandler *todate_expr = NULL;
    if (OB_FAIL(ctx_.expr_factory_->create_proxy_expr(OB_PROXY_EXPR_TYPE_FUNC_TO_DATE, todate_expr))) {
      LOG_WDIAG("create proxy to_date failed", K(ret));
    } else {
      todate_expr->set_target_type(ObDateTimeType);
      func_expr = todate_expr;
    }
  } else if (OB_PROXY_EXPR_TYPE_FUNC_TO_TIMESTAMP == type) {
    ObProxyExprToTimeHandler *totimestamp_expr = NULL;
    if (OB_FAIL(ctx_.expr_factory_->create_proxy_expr(OB_PROXY_EXPR_TYPE_FUNC_TO_TIMESTAMP, totimestamp_expr))) {
      LOG_WDIAG("create proxy to_timestamp failed", K(ret));
    } else {
      totimestamp_expr->set_target_type(ObTimestampNanoType);
      func_expr = totimestamp_expr;
    }
  } else if (OB_FAIL(ctx_.expr_factory_->create_func_expr(type, func_expr))) {
    LOG_WDIAG("create func expr failed", K(ret));
  }
  return ret;
}

} // end opsql
} // end obproxy
} // end oceanbase
