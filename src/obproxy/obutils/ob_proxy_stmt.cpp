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
#include "obutils/ob_proxy_stmt.h"
#include "utils/ob_proxy_utils.h"

using namespace oceanbase::common;
static const int BUCKET_SIZE = 64;
namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
int ObProxyDMLStmt::condition_exprs_to_sql_string(common::ObSqlString& sql_string)
{
  int ret = OB_SUCCESS;
  if (condition_exprs_.count() <= 0) {
    // do nothing
  } else if (condition_exprs_.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected count", K(condition_exprs_.count()));
  } else if (OB_FAIL(sql_string.append(" WHERE "))) {
    LOG_WARN("append failed", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < condition_exprs_.count(); i++) {
      if (OB_FAIL(condition_exprs_.at(i)->to_sql_string(sql_string))) {
        LOG_WARN("to sql_string failed", K(i), K(ret));
      }
    }
  }
  return ret;
}

int ObProxyDMLStmt::limit_to_sql_string(common::ObSqlString& sql_string)
{
  int ret = OB_SUCCESS;
  if (limit_offset_ > 0) {
    if (OB_FAIL(sql_string.append_fmt(" LIMIT %d, %d", limit_start_, limit_offset_))) {
      LOG_WARN("fail to append", K(ret));
    }
  }
  return ret;
}
ObProxySelectStmt::ObProxySelectStmt() : is_inited_(false), parse_phase_(SELECT_FIELD_PHASE), has_rollup_(false)
{

}

ObProxySelectStmt::~ObProxySelectStmt()
{
  alias_col_map_.destroy();
}
int ObProxySelectStmt::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(alias_col_map_.create(BUCKET_SIZE, ObModIds::OB_HASH_BUCKET_PROXY_MAP))) {
    LOG_WARN("hash map init failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObProxySelectStmt::handle_where_end_pos(ParseNode* node)
{
  int ret = OB_SUCCESS;
  where_end_pos_ = sql_string_.length();
  if (node->num_child_ > PARSE_SELECT_GROUP && node->children_[PARSE_SELECT_GROUP] != NULL) {
    where_end_pos_ = node->children_[PARSE_SELECT_GROUP]->token_off_;
  } else if (node->num_child_ > PARSE_SELECT_HAVING && node->children_[PARSE_SELECT_HAVING] != NULL) {
    where_end_pos_ = node->children_[PARSE_SELECT_HAVING]->token_off_;
  } else if (node->num_child_ > PARSE_SELECT_ORDER && node->children_[PARSE_SELECT_ORDER] != NULL) {
    where_end_pos_ = node->children_[PARSE_SELECT_ORDER]->token_off_;
  } else if (node->num_child_ >PARSE_SELECT_LIMIT && node->children_[PARSE_SELECT_LIMIT] != NULL) {
    where_end_pos_ = node->children_[PARSE_SELECT_LIMIT]->token_off_;
  }
  return ret;
}
int ObProxySelectStmt::handle_parse_result(const ParseResult &parse_result)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  ParseNode* node = parse_result.result_tree_->children_[0];
  if (OB_FAIL(handle_where_end_pos(node))) {
    LOG_WARN("handle_where_end_pos failed", K(ret), K(sql_string_));
  }
  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    tmp_node = node->children_[i];
    if (NULL == tmp_node) {
      // do nothing
    } else if (i == PARSE_SELECT_HAVING) {
      ret = OB_ERROR_UNSUPPORT_EXPR_TYPE;
      LOG_WARN("having not support", K(ret), K(sql_string_));
    } else {
      switch(tmp_node->type_) {
        case T_HINT_OPTION_LIST:
          ret = handle_hint_clause(tmp_node);
          break;
        case T_PROJECT_LIST:
          ret = handle_project_list(tmp_node);
          break;
        case T_FROM_LIST:
          ret = handle_from_list(tmp_node);
          break;
        case T_WHERE_CLAUSE:
          ret = handle_where_clause(tmp_node);
          break;
        case T_GROUPBY_CLAUSE:
          ret = handle_groupby_clause(tmp_node);
          break;
        case T_ORDER_BY:
          ret = handle_orderby_clause(tmp_node);
          break;
        case T_COMMA_LIMIT_CLAUSE:
        case T_LIMIT_CLAUSE:
          ret = handle_limit_clause(tmp_node);
          break;
        case T_QEURY_EXPRESSION_LIST: //distinct not support now
        default:
          ret = OB_ERROR_UNSUPPORT_EXPR_TYPE;
          LOG_WARN("unsupport type", "node_type", get_type_name(tmp_node->type_), K(sql_string_), K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(handle_comment_list(parse_result))) {
    LOG_WARN("handle_comment_list failed", K(ret), K(sql_string_));
  }
  return ret;
}
int ObProxySelectStmt::handle_comment_list(const ParseResult &parse_result)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < parse_result.comment_cnt_; i++) {
    TokenPosInfo& token_info = parse_result.comment_list_[i];
    ObString comment(token_info.token_len_, sql_string_.ptr() + token_info.token_off_);
    if (OB_FAIL(comments_.push_back(comment))) {
      LOG_WARN("push_back failed", K(ret), K(comment), K(sql_string_));
    } else {
      LOG_DEBUG("push_back succ", K(comment));
    }
  }
  return ret;
}
int ObProxySelectStmt::comments_to_sql_string(common::ObSqlString& sql_string)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < comments_.count(); i++) {
    if (OB_FAIL(sql_string.append(comments_.at(i)))) {
      LOG_WARN("fail to append", K(ret), K(i), K(comments_.at(i)), K(sql_string_));
    }
  }
  return ret;
}

int ObProxySelectStmt::hint_exprs_to_sql_string(common::ObSqlString& sql_string)
{
  int ret = OB_SUCCESS;
  if (hint_string_.empty()) {
    // do nothing
  } else if (OB_FAIL(sql_string.append("/*"))) {
    LOG_WARN("fail to append", K(ret), K(sql_string_));
  } else if (OB_FAIL(sql_string.append(hint_string_))) {
    LOG_WARN("fail to append", K(ret), K(hint_string_), K(sql_string_));
  }
  return ret;
}
int ObProxySelectStmt::select_exprs_to_sql_string(common::ObSqlString& sql_string)
{
  int ret = OB_SUCCESS;
  if (select_exprs_.count() < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid select_exprs_ count", K(select_exprs_.count()), K(sql_string_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs_.count(); i++) {
      if (OB_FAIL(select_exprs_.at(i)->to_sql_string(sql_string))) {
        LOG_WARN("to sql_string failed", K(i), K(ret), K(sql_string_));
      } else if (i >= select_exprs_.count() - 1) {
        // no need add , do nothing
      } else if (OB_FAIL(sql_string.append(", "))) {
        LOG_WARN("fail to append", K(i), K(ret), K(sql_string_));
      }
    }
  }
  return ret;
}
int ObProxySelectStmt::table_exprs_to_sql_string(common::ObSqlString& sql_string)
{
  int ret = OB_SUCCESS;
  if (table_exprs_.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected count ,now only support 1", K(table_exprs_.count()), K(sql_string_));
  } else if (OB_FAIL(sql_string.append(" FROM "))) {
    LOG_WARN("fail to append", K(ret), K(sql_string_));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < table_exprs_.count(); i++) {
      if (OB_FAIL(table_exprs_.at(i)->to_sql_string(sql_string))) {
        LOG_WARN("to sql_string failed", K(i), K(ret), K(sql_string_));
      } else if (i >= table_exprs_.count() - 1) {
        // no need add , do nothing
      } else if (OB_FAIL(sql_string.append(", "))) {
        LOG_WARN("fail to append", K(i), K(ret), K(sql_string_));
      }
    }
  }
  return ret;
}
int ObProxySelectStmt::group_by_exprs_to_sql_string(common::ObSqlString& sql_string)
{
  int ret = OB_SUCCESS;
  if (group_by_exprs_.count() <= 0) {
    // do nothing
  } else if (OB_FAIL(sql_string.append(" GROUP BY "))) {
    LOG_WARN("fail to append", K(ret), K(sql_string_));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < group_by_exprs_.count(); i++) {
      if (OB_FAIL(group_by_exprs_.at(i)->to_sql_string(sql_string))) {
        LOG_WARN("to sql_string failed", K(i), K(ret), K(sql_string_));
      } else if (i >= group_by_exprs_.count() - 1) {
        // no need add , do nothing
      } else if (OB_FAIL(sql_string.append(", "))) {
        LOG_WARN("append failed", K(i), K(ret), K(sql_string_));
      }
    }
    if (OB_SUCC(ret) && has_rollup_) {
      if (OB_FAIL(sql_string.append(" WITH ROLLUP"))) {
        LOG_WARN("append failed", K(ret), K(sql_string_));
      }
    }
  }
  return ret;
}
int ObProxySelectStmt::order_by_exprs_to_sql_string(common::ObSqlString& sql_string)
{
  int ret = OB_SUCCESS;
  if (order_by_exprs_.count() <= 0) {
    // do nothing
  } else if (OB_FAIL(sql_string.append(" ORDER BY "))) {
    LOG_WARN("fail to append", K(ret), K(sql_string_));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < order_by_exprs_.count(); i++) {
      if (OB_FAIL(order_by_exprs_.at(i)->to_sql_string(sql_string))) {
        LOG_WARN("to sql_string failed", K(i), K(ret), K(sql_string_));
      } else if (i >= order_by_exprs_.count() - 1) {
        // no need add , do nothing
      } else if (OB_FAIL(sql_string.append(", "))) {
        LOG_WARN("append failed", K(i), K(ret), K(sql_string_));
      }
    }
  }
  return ret;
}

//REFER:https://dev.mysql.com/doc/refman/8.0/en/select.html
int ObProxySelectStmt::to_sql_string(common::ObSqlString& sql_string)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(comments_to_sql_string(sql_string))) {
    LOG_WARN("comments_to_sql_string failed", K(ret), K(sql_string_));
  } else if (OB_FAIL(sql_string.append("SELECT "))) {
    LOG_WARN("fail to append", K(ret), K(sql_string_));
  } else if (OB_FAIL(hint_exprs_to_sql_string(sql_string))) {
    LOG_WARN("hint_exprs_to_sql_string failed", K(ret), K(sql_string_));
  } else if (OB_FAIL(select_exprs_to_sql_string(sql_string))) {
    LOG_WARN("select_exprs_to_sql_string failed", K(ret), K(sql_string_));
  } else if (OB_FAIL(table_exprs_to_sql_string(sql_string))) {
    LOG_WARN("table_exprs_to_sql_string failed", K(ret), K(sql_string_));
  } else if (OB_FAIL(condition_exprs_to_sql_string(sql_string))) {
    LOG_WARN("condition_exprs_to_sql_string faield", K(ret), K(sql_string_));
  } else if (OB_FAIL(group_by_exprs_to_sql_string(sql_string))) {
    LOG_WARN("group_by_exprs_to_sql_string failed", K(ret), K(sql_string_));
  } else if (OB_FAIL(order_by_exprs_to_sql_string(sql_string))) {
    LOG_WARN("order_by_exprs_to_sql_string failed", K(ret), K(sql_string_));
  } else if (OB_FAIL(limit_to_sql_string(sql_string))) {
    LOG_WARN("limit_to_sql_string", K(ret), K(sql_string_));
  } else {
    LOG_DEBUG("sql_string is", K(sql_string));
  }
  return ret;
}

int ObProxySelectStmt::handle_hint_clause(ParseNode* node)
{
  int ret = OB_SUCCESS;
  hint_string_.assign_ptr(sql_string_.ptr() + node->token_off_, node->token_len_);
  LOG_DEBUG("handle_hint_clause", K(hint_string_));
  return ret;
}

int ObProxySelectStmt::handle_limit_clause(ParseNode* node)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  int64_t tmp_val = 0;
  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    tmp_node = node->children_[i];
    if (NULL == tmp_node) {
      //do nothing
    } else {
      switch(tmp_node->type_) {
        case T_INT:
          if(OB_FAIL(get_int_value(tmp_node->str_value_, tmp_val))) {
            LOG_WARN("get_int_value failed", K(ret), K(tmp_node->str_value_), K(sql_string_));
          } else {
            limit_start_ = static_cast<int>(tmp_val);
          }
          break;
        case T_LIMIT_INT:
          if(OB_FAIL(get_int_value(tmp_node->str_value_, tmp_val))) {
            LOG_WARN("get_int_value failed", K(ret), K(tmp_node->str_value_), K(sql_string_));
          } else {
            limit_offset_ = static_cast<int>(tmp_val);
          }
          break;
        default:
          ret = OB_ERROR_UNSUPPORT_EXPR_TYPE;
          LOG_WARN("unsupport type", "node_type", get_type_name(tmp_node->type_), K(sql_string_));
      }
    }
  }
  return ret;
}

int ObProxySelectStmt::handle_project_list(ParseNode* node)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  ObProxyExpr* expr = NULL;
  parse_phase_ = SELECT_FIELD_PHASE;
  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    tmp_node = node->children_[i];
    expr = NULL;
    if (NULL == tmp_node) {
      // do nothing
    } else {
      switch(tmp_node->type_) {
        case T_PROJECT_STRING:
          if (OB_FAIL(project_string_to_expr(tmp_node, expr))) {
            LOG_WARN("project_string_to_expr failed", K(ret), K(sql_string_));
          } else if (OB_FAIL(select_exprs_.push_back(expr))) {
            LOG_WARN("push to array failed", K(ret), K(sql_string_));
          } else {
            LOG_DEBUG("add expr succ", K(tmp_node->str_value_));
          }
          break;
        default:
          ret = OB_ERROR_UNSUPPORT_EXPR_TYPE;
          LOG_WARN("unsupport type", "node_type", get_type_name(tmp_node->type_), K(sql_string_));
      }
    }
  }
  return ret;
}

int ObProxySelectStmt::get_sharding_const_expr(ParseNode* node, ObProxyExpr* &expr, bool is_column_ref)
{
  int ret = OB_SUCCESS;
  ObProxyExpr* real_alias_col_expr = NULL;
  ObProxyExprShardingConst* expr_const = NULL;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected null", K(sql_string_), K(ret));
  } else if (OB_FAIL(get_expr_by_type(expr, OB_PROXY_EXPR_TYPE_SHARDING_CONST))) {
    LOG_WARN("get_expr_by_type failed", K(ret), K(sql_string_));
  } else if (OB_ISNULL(expr_const = dynamic_cast<ObProxyExprShardingConst*>(expr))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dynamic_cast failed", K(ret));
  } else {
    ObObj obj;
    if (node->type_ == T_INT) {
      int64_t value = 0;
      if (OB_FAIL(get_int_value(node->str_value_, value, 10))) {
        LOG_WARN("fail to int value", K(ret), K(node->str_value_), K(sql_string_));
      } else {
        obj.set_int(value);
        expr_const->set_object(obj);
        LOG_DEBUG("add int value", K(obj));
      }
    } else {
      obj.set_varchar(node->str_value_);
      expr_const->set_object(obj);
      expr_const->set_is_column(is_column_ref);
      LOG_DEBUG("add varchar value", K(obj));
    }
    if (parse_phase_ >= GROUP_BY_PHASE) {
      ObString col_name(node->str_value_);
      if (OB_SUCCESS == alias_col_map_.get_refactored(col_name, real_alias_col_expr)) {
        // is alias
        expr_const->expr_ = real_alias_col_expr;
        expr_const->has_alias_ = 1;
        expr_const->set_is_alias(true);
        LOG_DEBUG("succ set expr for alias", K(col_name));
      }
    }
    expr = expr_const;
  }
  return ret;
}
ObProxyExprType ObProxySelectStmt::get_expr_type_by_node_type(const ObItemType& item_type)
{
  ObProxyExprType expr_type = OB_PROXY_EXPR_TYPE_NONE;
  switch(item_type) {
    case T_OP_ADD:
      expr_type = OB_PROXY_EXPR_TYPE_FUNC_ADD;
      break;
    case T_OP_MINUS:
      expr_type = OB_PROXY_EXPR_TYPE_FUNC_SUB;
      break;
    case T_OP_MUL:
      expr_type = OB_PROXY_EXPR_TYPE_FUNC_MUL;
      break;
    case T_OP_DIV:
      expr_type = OB_PROXY_EXPR_TYPE_FUNC_DIV;
      break;
    case T_FUN_SUM:
      expr_type = OB_PROXY_EXPR_TYPE_FUNC_SUM;
      break;
    case T_FUN_MAX:
      expr_type = OB_PROXY_EXPR_TYPE_FUNC_MAX;
      break;
    case T_FUN_MIN:
      expr_type = OB_PROXY_EXPR_TYPE_FUNC_MIN;
      break;
    case T_FUN_COUNT:
      expr_type = OB_PROXY_EXPR_TYPE_FUNC_COUNT;
      break;
    case T_FUN_AVG:
      expr_type = OB_PROXY_EXPR_TYPE_FUNC_AVG;
      break;
    default:
      break;
  }
  return expr_type;
}

int ObProxySelectStmt::get_expr_by_type(ObProxyExpr* &expr, ObProxyExprType type)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
#define ALLOC_PROXY_EXPR_BY_TYPE(ExprClass) \
    if (OB_ISNULL(buf = (char*)(allocator_->alloc(sizeof(ExprClass))))) { \
      ret = OB_ALLOCATE_MEMORY_FAILED; \
      LOG_WARN("fail to alloc mem", K(ret)); \
    } else if (OB_ISNULL(expr = new (buf)ExprClass())) { \
      ret = OB_ALLOCATE_MEMORY_FAILED; \
      LOG_WARN("fail to new expr", K(ret)); \
    } else { \
      expr->set_expr_type(type);\
    }

  switch(type) {
  case OB_PROXY_EXPR_TYPE_SHARDING_CONST:
    ALLOC_PROXY_EXPR_BY_TYPE(ObProxyExprShardingConst);
    break;
  case OB_PROXY_EXPR_TYPE_SHARDING_ALIAS:
    ALLOC_PROXY_EXPR_BY_TYPE(ObProxyShardingAliasExpr);
    break;
  case OB_PROXY_EXPR_TYPE_FUNC_ADD:
    ALLOC_PROXY_EXPR_BY_TYPE(ObProxyExprAdd);
    break;
  case OB_PROXY_EXPR_TYPE_FUNC_SUB:
    ALLOC_PROXY_EXPR_BY_TYPE(ObProxyExprSub);
    break;
  case OB_PROXY_EXPR_TYPE_FUNC_MUL:
    ALLOC_PROXY_EXPR_BY_TYPE(ObProxyExprMul);
    break;
  case OB_PROXY_EXPR_TYPE_FUNC_DIV:
    ALLOC_PROXY_EXPR_BY_TYPE(ObProxyExprDiv);
    break;
  case OB_PROXY_EXPR_TYPE_FUNC_SUM:
    ALLOC_PROXY_EXPR_BY_TYPE(ObProxyExprSum);
    break;
  case OB_PROXY_EXPR_TYPE_FUNC_COUNT:
    ALLOC_PROXY_EXPR_BY_TYPE(ObProxyExprCount);
    break;
  case OB_PROXY_EXPR_TYPE_FUNC_AVG:
    ALLOC_PROXY_EXPR_BY_TYPE(ObProxyExprAvg);
    break;
  case OB_PROXY_EXPR_TYPE_FUNC_MAX:
    ALLOC_PROXY_EXPR_BY_TYPE(ObProxyExprMax);
    break;
  case OB_PROXY_EXPR_TYPE_FUNC_MIN:
    ALLOC_PROXY_EXPR_BY_TYPE(ObProxyExprMin);
    break;
  case OB_PROXY_EXPR_TYPE_FUNC_ORDER:
    ALLOC_PROXY_EXPR_BY_TYPE(ObProxyOrderItem);
    break;
  default:
    ret = OB_ERROR_UNSUPPORT_EXPR_TYPE;
    LOG_WARN("unexpected type", K(type));
    break;
  }
  return ret;
}

int ObProxySelectStmt::column_ref_to_expr(ParseNode* node, ObProxyExpr* &expr)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    tmp_node = node->children_[i];
    if (NULL == tmp_node) {
    } else if (T_IDENT != tmp_node->type_) {
      // now column_ref child should be T_IDENT
      ret = OB_ERROR_UNSUPPORT_EXPR_TYPE;
      LOG_WARN("unsupport type", "node_type", get_type_name(tmp_node->type_), K(i), K(tmp_node->str_value_), K(sql_string_));
    } else if (NULL != expr) {
      //column_ref should have only one T_IDENT
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr_const already allocate, unexpected", K(i), K(tmp_node->str_value_), K(node->num_child_));
    } else if (OB_FAIL(get_sharding_const_expr(tmp_node, expr, true))) {
      LOG_WARN("get_sharding_const_expr failed", K(ret), K(sql_string_));
    }
  }
  return ret;
}
int ObProxySelectStmt::alias_node_to_expr(ParseNode* node, ObProxyExpr* &expr, ParseNode* string_node)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  ObProxyShardingAliasExpr* alias_expr = NULL;
  ObProxyExpr* tmp_expr = NULL;
  if (OB_FAIL(get_expr_by_type(expr, OB_PROXY_EXPR_TYPE_SHARDING_ALIAS))) {
    LOG_WARN("get_expr_by_type failed", K(ret));
  } else if (OB_ISNULL(alias_expr = dynamic_cast<ObProxyShardingAliasExpr*>(expr))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dynamic_cast failed", K(ret));
  }
  ObString alias_col;
  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    tmp_node = node->children_[i];
    tmp_expr = NULL;
    if (NULL == tmp_node) {
      //do nothing
    } else {
      switch(tmp_node->type_) {
        case T_IDENT:
          // here is alias node
          if (OB_FAIL(get_sharding_const_expr(tmp_node, tmp_expr, true))) { //here is_column true
            LOG_WARN("get_sharding_const_expr failed", K(i), K(ret));
          } else if (parse_phase_ == SELECT_FIELD_PHASE) {
            // in select record alias
            alias_col.assign_ptr(tmp_node->str_value_, static_cast<ObString::obstr_size_t>(strlen(tmp_node->str_value_)));
            if (OB_FAIL(alias_col_map_.set_refactored(alias_col, alias_expr))) { //put origin expr into map
              LOG_WARN("set set_refactored failed", K(alias_col), K(ret));
            } else {
              LOG_DEBUG("add alias to map", K(alias_col));
            }
          }
          break;
        default:
          if (OB_FAIL(string_node_to_expr(tmp_node, tmp_expr, string_node))) {
            LOG_WARN("string_node_to_expr failed", K(i), K(ret));
          }
      }
      if (OB_FAIL(ret)) {
        // already log before, do nothing
      } else if (OB_FAIL(alias_expr->add_param_expr(tmp_expr))) {
        LOG_WARN("add_param_expr failed", K(ret), K(sql_string_));
      } else {
        if (tmp_expr->has_agg_) {
          alias_expr->has_agg_ = 1;
        }
        LOG_DEBUG("add alias node success", K(tmp_node->str_value_), K(alias_expr->has_agg_));
      }
    }
  }
  return ret;
}

int ObProxySelectStmt::func_node_to_expr(ParseNode* node, ObProxyExpr* &expr)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  ObProxyFuncExpr* func_expr = NULL;
  ObProxyExpr* tmp_expr = NULL;
  ObProxyExprType expr_type = get_expr_type_by_node_type(node->type_);
  if (OB_FAIL(get_expr_by_type(expr, expr_type))) {
    LOG_WARN("get_expr_by_type failed", K(ret));
  } else if (OB_ISNULL(func_expr = dynamic_cast<ObProxyFuncExpr*>(expr))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dynamic_cast failed", K(ret));
  } else {
    switch(node->type_) {
    case T_FUN_SUM:
    case T_FUN_COUNT:
    case T_FUN_MAX:
    case T_FUN_MIN:
    case T_FUN_AVG:
      func_expr->has_agg_ = 1;
      break;
    default:
      func_expr->has_agg_ = 0;
      break;
    }
  }
  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    tmp_node = node->children_[i];
    tmp_expr = NULL;
    if (NULL == tmp_node) {
      // do nothing
    } else if (tmp_node->type_ == T_ALL) {
      // count func have T_ALL node, no need handle
    } else if (OB_FAIL(string_node_to_expr(tmp_node, tmp_expr))){
      LOG_WARN("string_node_to_expr failed", K(ret));
    } else if (OB_FAIL(func_expr->add_param_expr(tmp_expr))) {
      LOG_WARN("add_param_expr failed", K(ret), K(sql_string_));
    } else {
      if (tmp_expr->has_agg_) {
        func_expr->has_agg_ = 1;
      }
      if (tmp_expr->has_alias_) {
        func_expr->has_alias_ = 1;
      }
      LOG_DEBUG("add node success", K(get_expr_type_name(expr_type)), K(func_expr->has_agg_), K(func_expr->has_alias_));
    }
  }
  return ret;
}
int ObProxySelectStmt::check_node_has_agg(ParseNode* node)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(sql_string_));
  }
  for(int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    tmp_node = node->children_[i];
    if (NULL == tmp_node) {
      //do nothing
    } else {
      switch(tmp_node->type_) {
        case T_FUN_SUM:
        case T_FUN_COUNT:
        case T_FUN_MAX:
        case T_FUN_MIN:
        case T_FUN_AVG:
          ret = OB_ERROR_UNSUPPORT_HAS_AGG_EXPR_TYPE;
          LOG_WARN("node has agg", "node_type", get_type_name(tmp_node->type_), K(ret), K(sql_string_));
          break;
        default:
          if (OB_FAIL(check_node_has_agg(tmp_node))) {
            LOG_WARN("check_node_has_agg failed", K(ret));
          }
          break;
      }
    }
  }
  return ret;
}
//we do not cover all sys func. if sys func do not have agg func, pass it to server as string
int ObProxySelectStmt::func_sys_node_to_expr(ParseNode* node, ObProxyExpr* &expr,  ParseNode* string_node)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_node_has_agg(node))) {
    LOG_WARN("check_node_has_agg failed", K(ret));
  } else if (string_node == NULL) {
    if (OB_FAIL(get_sharding_const_expr(node, expr, true))) {
      LOG_WARN("get_sharding_const_expr faield", K(ret));
    }
  } else if (OB_FAIL(get_sharding_const_expr(string_node, expr, true))) {
    LOG_WARN("get_sharding_const_expr faield", K(ret));
  }
  return ret;
}

int ObProxySelectStmt::string_node_to_expr(ParseNode* node, ObProxyExpr* &expr,  ParseNode* string_node)
{
  int ret = OB_SUCCESS;
  int i = 0;
  switch(node->type_) {
    case T_STAR:
      node->str_value_ = "*";
      node->str_len_ = 1;
      if (OB_SUCC(get_sharding_const_expr(node, expr, true))) {
        LOG_DEBUG("get sharding const expr succ", "node_str", node->str_value_);
      }
      break;
    case T_COLUMN_REF:
      if (OB_SUCC(column_ref_to_expr(node, expr))) {
        LOG_DEBUG("column_ref to expr succ", "node_str", node->str_value_);
      }
      break;
    case T_ALL:
      break;
    case T_ALIAS:
      if (OB_SUCC(alias_node_to_expr(node, expr, string_node))) {
        LOG_DEBUG("alias_node to expr succ", "node_str", node->str_value_);
      }
      break;
    case T_FUN_SYS: // not support fun sys, as string_node
      if (OB_SUCC(func_sys_node_to_expr(node, expr, string_node))) {
        LOG_DEBUG("func_sys node to expr succ", "node_str", node->str_value_);
      }
      break;
    case T_RELATION_FACTOR:
      if (OB_SUCC(get_sharding_const_expr(node, expr, true))) {
        LOG_DEBUG("get sharding const expr succ", "node_str", node->str_value_);
      }
      break;
    case T_IDENT:
    case T_INT:
      if (OB_SUCC(get_sharding_const_expr(node, expr, false))) {
        LOG_DEBUG("get sharding const expr succ", "node_str", node->str_value_);
      }
      break;
    case T_VARCHAR:
      //varchar has varchar child, if child is 0 not have child
      if (node->num_child_ == 0) {
        if (OB_SUCC(get_sharding_const_expr(node, expr, false))) {
          LOG_DEBUG("get sharding const expr succ", "node_str", node->str_value_);
        }
      } else {
        for (i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
          if (node->children_[i] == NULL) {
            //do nothing
          } else if (expr != NULL) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr should null", K(ret), K(i), K(node->str_value_), K(sql_string_));
          } else  if (OB_SUCC(get_sharding_const_expr(node->children_[i], expr, false))) {
            LOG_DEBUG("get sharding const expr succ", "node_str", node->children_[i]->str_value_);
          }
        }
      }
      break;
    case T_OP_ADD:
    case T_OP_MINUS:
    case T_OP_MUL:
    case T_OP_DIV:
    case T_FUN_SUM:
    case T_FUN_COUNT:
    case T_FUN_MAX:
    case T_FUN_MIN:
    case T_FUN_AVG:
      if (OB_SUCC(func_node_to_expr(node, expr))) {
        LOG_DEBUG("add add node expr succ", "node_type", get_type_name(node->type_));
      }
      break;
    default:
      if (OB_FAIL(check_node_has_agg(node))) {
        LOG_WARN("unsupport type", "node_type", get_type_name(node->type_), K(node->str_value_));
      } else if (string_node == NULL) {
        ret = OB_ERROR_UNSUPPORT_EXPR_TYPE;
        LOG_WARN("unsupport type", "node_type", get_type_name(node->type_), K(node->str_value_), K(sql_string_));
      } else if (OB_FAIL(get_sharding_const_expr(string_node, expr, true))) {
        LOG_WARN("get_sharding_const_expr failed", K(ret));
      }
  }
  return ret;
}
int ObProxySelectStmt::project_string_to_expr(ParseNode* node, ObProxyExpr* &expr)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    tmp_node = node->children_[i];
    if (NULL == tmp_node) {
      // do nothing
    } else if (expr != NULL) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr not null", K(node->str_value_), K(node->num_child_), K(sql_string_), K(ret));
    } else if (OB_FAIL(string_node_to_expr(tmp_node, expr, node))) {
      LOG_WARN("string_node_to_expr failed", K(sql_string_), K(ret));
    }
  }
  return ret;
}

int ObProxySelectStmt::org_node_to_expr(ParseNode* node, ObProxyExpr* &expr)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    tmp_node = node->children_[i];
    if (NULL == tmp_node) {
      // do nothing
    }  else {
      switch(tmp_node->type_) {
        case T_RELATION_FACTOR:
          if (OB_FAIL(get_sharding_const_expr(tmp_node, expr, true))) {
            LOG_WARN("get_sharding_const_expr", K(sql_string_), K(ret));
          }
          break;
        default:
          ret = OB_ERROR_UNSUPPORT_EXPR_TYPE;
          LOG_WARN("unsupport type", "node_type", get_type_name(node->type_),  K(node->str_value_));
      }
    }
  }
  return ret;
}
int ObProxySelectStmt::handle_from_list(ParseNode* node)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  LOG_DEBUG("handle_from_list");
  parse_phase_ = FROM_TABLE_PHASE;
  ObProxyExpr* expr = NULL;
  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    tmp_node = node->children_[i];
    expr = NULL;
    if (NULL == tmp_node) {
      // do nothing
    }  else {
      switch(tmp_node->type_) {
        case T_ORG:
          if (OB_FAIL(org_node_to_expr(tmp_node, expr))) {
            LOG_WARN("org_node_to_expr failed", K(ret), K(sql_string_));
          }
          break;
        case T_ALIAS:
          if (OB_FAIL(alias_node_to_expr(tmp_node, expr))) {
            LOG_WARN("alias_node_to_expr failed", K(ret), K(sql_string_));
          }
          break;
        default:
          ret = OB_ERROR_UNSUPPORT_EXPR_TYPE;
          LOG_WARN("unsupport type", "node_type", get_type_name(tmp_node->type_),  K(tmp_node->str_value_));
      }
      if (OB_SUCC(ret) && OB_FAIL(table_exprs_.push_back(expr))) {
        LOG_WARN("push_back failed", K(ret), K(sql_string_));
      } else if (table_exprs_.count() > 1) {
        ret = OB_ERROR_UNSUPPORT_EXPR_TYPE;
        LOG_WARN("unexpected count ,now only support 1", K(table_exprs_.count()), K(sql_string_));
      }
    }
  }
  return ret;
}

int ObProxySelectStmt::handle_varchar_node_in_where_condition(ParseNode* node, SqlField& sql_field)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_varchar_node = NULL;
  SqlColumnValue column_value;
  int i = 0;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (node->num_child_ == 0) {
    column_value.value_type_ = TOKEN_STR_VAL;
    column_value.column_value_.set(node->str_value_);
    sql_field.column_values_.push_back(column_value);
  } else {
    tmp_varchar_node = NULL;
    for (i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
      if (node->children_[i] == NULL) {
        //do nothing
      } else if (tmp_varchar_node != NULL) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("should null here", K(ret), K(node->str_value_), K(sql_string_));
      } else {
        tmp_varchar_node = node->children_[i];
      }
    }
    if (tmp_varchar_node == NULL) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("shoule not null here", K(ret), K(node->str_value_), K(sql_string_));
    } else {
      column_value.value_type_ = TOKEN_STR_VAL;
      column_value.column_value_.set(tmp_varchar_node->str_value_);
      sql_field.column_values_.push_back(column_value);
    }
  }
  return ret;
}

int ObProxySelectStmt::handle_where_eq_node(ParseNode* node)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  SqlField sql_field;
  SqlColumnValue column_value;
  if (OB_ISNULL(field_results_)) {
    LOG_WARN("unexpected null");
    ret = OB_ERR_UNEXPECTED;
  } else {
    for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
      tmp_node = node->children_[i];
      if (NULL == tmp_node) {
        //do nothing
      } else {
        switch(tmp_node->type_) {
          case T_COLUMN_REF:
            sql_field.column_name_.set(tmp_node->str_value_);
            break;
          case T_INT:
          case T_NUMBER:
            column_value.value_type_ = TOKEN_STR_VAL;
            column_value.column_value_.set(tmp_node->str_value_);
            sql_field.column_values_.push_back(column_value);
            break;
          case T_VARCHAR:
            ret = handle_varchar_node_in_where_condition(tmp_node, sql_field);
            break;
          default:
            ret = OB_ERROR_UNSUPPORT_EXPR_TYPE;
            LOG_WARN("unexpected expr type", "node_type", get_type_name(tmp_node->type_), K(sql_string_));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(field_results_->fields_.push_back(sql_field))) {
        LOG_WARN("push_back failed", K(ret), K(sql_string_));
      } else {
        ++field_results_->field_num_;
        LOG_DEBUG("add sql_field", K(sql_field), K(field_results_->field_num_));
      }
    }
  }
  return ret;
}
int ObProxySelectStmt::handle_where_expr_list_node(ParseNode* node, SqlField& sql_field)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  SqlColumnValue column_value;
  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    tmp_node = node->children_[i];
    if (NULL == tmp_node) {
      //do nothing
    } else {
      switch(tmp_node->type_) {
        case T_VARCHAR:
          ret = handle_varchar_node_in_where_condition(tmp_node, sql_field);
          break;
        case T_INT:
        default:
          column_value.value_type_ = TOKEN_STR_VAL;
          column_value.column_value_.set(tmp_node->str_value_);
          if (OB_FAIL(sql_field.column_values_.push_back(column_value))) {
            LOG_WARN("push_back failed", K(i), K(sql_string_), K(ret));
          }
          break;
      }
    }
  }
  return ret;
}
int ObProxySelectStmt::handle_where_in_node(ParseNode* node)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  SqlField sql_field;
  SqlColumnValue column_value;
  if (OB_ISNULL(field_results_)) {
    LOG_WARN("unexpected null");
    ret = OB_ERR_UNEXPECTED;
  } else {
    for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
      tmp_node = node->children_[i];
      if (NULL == tmp_node) {
        //do nothing
      } else {
        switch(tmp_node->type_) {
          case T_COLUMN_REF:
            sql_field.column_name_.set(tmp_node->str_value_);
            break;
          case T_EXPR_LIST:
            if (OB_FAIL(handle_where_expr_list_node(tmp_node, sql_field))) {
              LOG_WARN("handle_where_expr_list_node failed", K(ret), K(i), K(sql_string_));
            }
            break;
          default:
            ret = OB_ERROR_UNSUPPORT_EXPR_TYPE;
            LOG_WARN("unsupport expr type", "node_type", get_type_name(tmp_node->type_), K(sql_string_));
            break;
        }
      }
    }
    if (OB_FAIL(field_results_->fields_.push_back(sql_field))) {
      LOG_WARN("push_back failed", K(ret));
    } else {
      LOG_DEBUG("succ add sql_field", K(sql_field));
    }
  }
  return ret;
}
int ObProxySelectStmt::handle_where_condition_node(ParseNode* node)
{
  int ret = OB_SUCCESS;
  switch(node->type_) {
    case T_OP_OR:
    case T_OP_AND:
      if (OB_FAIL(handle_where_nodes(node))) {
        LOG_WARN("handle_where_nodes failed", K(ret), K(sql_string_));
      }
      break;
    case T_OP_EQ:
      if (OB_FAIL(handle_where_eq_node(node))) {
        LOG_WARN("handle_where_eq_node failed", K(ret), K(sql_string_));
      }
      break;
    case T_OP_IN:
      if (OB_FAIL(handle_where_in_node(node))) {
        LOG_WARN("handle_where_in_node failed", K(ret), K(sql_string_));
      }
      break;
    default:
      LOG_DEBUG("type not handle", "node_type", get_type_name(node->type_), K(sql_string_));
  }
  return ret;
}
int ObProxySelectStmt::handle_where_clause(ParseNode* node)
{
  int ret = OB_SUCCESS;
  parse_phase_ = WHERE_CONDTION_PHASE;
  int offset = node->token_off_ + node->token_len_ + 1;
  ObString where_str(where_end_pos_ - offset, sql_string_.ptr() + offset);
  ObObj obj;
  obj.set_varchar(where_str);
  LOG_DEBUG("handle_where_clause", K(where_str));
  ObProxyExpr* expr = NULL;
  ObProxyExprShardingConst* sharding_expr = NULL;
  if (OB_FAIL(get_expr_by_type(expr, OB_PROXY_EXPR_TYPE_SHARDING_CONST))) {
    LOG_WARN("get_expr_by_type failed", K(ret), K(sql_string_));
  } else if (OB_ISNULL(sharding_expr = dynamic_cast<ObProxyExprShardingConst*>(expr))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dynamic_cast failed", K(ret));
  } else if (FALSE_IT(sharding_expr->set_is_column(true))) {
    // never here
  } else if (FALSE_IT(sharding_expr->set_object(obj))) {
    // never here
  } else if (OB_FAIL(condition_exprs_.push_back(sharding_expr))){
    LOG_WARN("push_back failed", K(ret), K(sql_string_));
  } else if (OB_FAIL(handle_where_nodes(node))) {
    LOG_WARN("handle_where_nodes failed", K(ret), K(sql_string_));
  } else {
    LOG_DEBUG("after handle_where_clause", KPC(field_results_), K(condition_exprs_.count()));
  }
  return ret;
}
int ObProxySelectStmt::handle_where_nodes(ParseNode* node)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  if (OB_ISNULL(field_results_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null");
  } else {
    for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
      tmp_node = node->children_[i];
      if (NULL == tmp_node) {
        // do nothing
      } else if (OB_FAIL(handle_where_condition_node(tmp_node))) {
        LOG_WARN("handle_where_condition_node", K(ret), K(sql_string_));
      }
    }
  }
  return ret;
}
int ObProxySelectStmt::handle_sort_key_node(ParseNode* node, ObProxyExpr* &expr, const SortListType& sort_list_type)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  ObProxyOrderItem* order_item_expr = NULL;
  ObProxyExpr* tmp_expr = NULL;
  ObProxyOrderDirection order_direction = NULLS_FIRST_ASC;
  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    tmp_node = node->children_[i];
    if (NULL == tmp_node) {
      //do nothing
    } else {
      switch(tmp_node->type_) {
      case T_SORT_ASC:
        order_direction = NULLS_FIRST_ASC;
        break;
      case T_SORT_DESC:
        order_direction = NULLS_LAST_DESC;
        break;
      default:
        if (OB_FAIL(string_node_to_expr(tmp_node, expr))) {
          LOG_WARN("string_node_to_expr failed", K(ret), K(sql_string_));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (sort_list_type != SORT_LSIT_IN_ORDER_BY) {
      // do nothing
    } else if (OB_FAIL(get_expr_by_type(tmp_expr, OB_PROXY_EXPR_TYPE_FUNC_ORDER))) {
      LOG_WARN("get_expr_by_type", K(ret), K(sql_string_));
    } else if (OB_ISNULL(order_item_expr = dynamic_cast<ObProxyOrderItem*>(tmp_expr))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dynamic_cast to ObProxyOrderItem failed", K(ret));
    } else {
      if (expr->has_agg_) {
        order_item_expr->has_agg_ = 1;
      }
      if (expr->has_alias_) {
        order_item_expr->has_alias_ = 1;
      }
      order_item_expr->order_direction_ = order_direction;
      order_item_expr->expr_ = expr;
      expr = order_item_expr;
      LOG_DEBUG("add order by sort key", K(order_direction), K(expr->has_agg_), K(expr->has_alias_));
    }
  }
  return ret;
}
int ObProxySelectStmt::handle_sort_list_node(ParseNode* node, const SortListType& sort_list_type)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  ObProxyExpr* expr = NULL;
  ObProxyOrderItem* order_item_expr = NULL;
  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    tmp_node = node->children_[i];
    expr = NULL;
    if (NULL == tmp_node) {
      //do nothing
    } else {
      switch(tmp_node->type_) {
        case T_SORT_KEY:
          if (OB_FAIL(handle_sort_key_node(tmp_node, expr, sort_list_type))) {
            LOG_WARN("handle_sort_key_node", K(ret), K(sql_string_));
          } else {
            switch(sort_list_type) {
              case SORT_LIST_IN_GROUP_BY:
                if (OB_FAIL(group_by_exprs_.push_back(expr))) {
                  LOG_WARN("push_back failed", K(ret), K(sql_string_));
                }
                break;
              case SORT_LSIT_IN_ORDER_BY:
                if (OB_ISNULL(order_item_expr = dynamic_cast<ObProxyOrderItem*>(expr))) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("dynamic_cast to ObProxyOrderItem failed", K(ret));
                } else if (OB_FAIL(order_by_exprs_.push_back(order_item_expr))) {
                  LOG_WARN("push_back failed", K(ret), K(sql_string_));
                }
                break;
              default:
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("invalid sort_list_type", K(sort_list_type), K(sql_string_));
            }
          }
          break;
        default:
        ret = OB_ERROR_UNSUPPORT_EXPR_TYPE;
        LOG_WARN("unsupport expr type", "node_type", get_type_name(tmp_node->type_), K(sql_string_));
      }
    }
  }
  return ret;
}
int ObProxySelectStmt::handle_with_rollup_in_groupby(ParseNode* node)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    tmp_node = node->children_[i];
    if (NULL == tmp_node) {
      //do nothing
    } else {
      switch(tmp_node->type_) {
      case T_ROLLUP:
        has_rollup_ = true;
        break;
      case T_SORT_LIST:
        if (OB_FAIL(handle_sort_list_node(tmp_node, SORT_LIST_IN_GROUP_BY))) {
          LOG_WARN("handle_sort_list_node failed", K(ret), K(sql_string_));
        }
        break;
      default:
        ret = OB_ERROR_UNSUPPORT_EXPR_TYPE;
        LOG_WARN("unsupport expr type", "node_type", get_type_name(tmp_node->type_), K(sql_string_));
      }
    }
  }
  return ret;
}

int ObProxySelectStmt::handle_groupby_clause(ParseNode* node)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  parse_phase_ = GROUP_BY_PHASE;
  LOG_DEBUG("handle_groupby_clause");
  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    tmp_node = node->children_[i];
    if (NULL == tmp_node) {
      //do nothing
    } else if (T_WITH_ROLLUP_CLAUSE != tmp_node->type_) {
      ret = OB_ERROR_UNSUPPORT_EXPR_TYPE;
      LOG_WARN("unsupport expr type", "node_type", get_type_name(tmp_node->type_), K(sql_string_));
    } else if (OB_FAIL(handle_with_rollup_in_groupby(tmp_node))) {
      LOG_WARN("handle_with_rollup_in_groupby failed", K(ret), K(i), K(sql_string_));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("after handle_groupby_clause", K(group_by_exprs_.count()));
  }
  return ret;
}

int ObProxySelectStmt::handle_orderby_clause(ParseNode* node)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  parse_phase_ = ORDER_BY_PHASE;
  LOG_DEBUG("handle_orderby_clause");
  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    tmp_node = node->children_[i];
    if (NULL == tmp_node) {
      //do nothing
    } else if (T_SORT_LIST != tmp_node->type_) {
      ret = OB_ERROR_UNSUPPORT_EXPR_TYPE;
      LOG_WARN("unsupport expr type", "node_type", get_type_name(tmp_node->type_), K(sql_string_));
    } else if (OB_FAIL(handle_sort_list_node(tmp_node, SORT_LSIT_IN_ORDER_BY))) {
      LOG_WARN("handle_sort_list_node", K(ret), K(i), K(sql_string_));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("after handle_orderby_clause", K(order_by_exprs_.count()));
  }
  return ret;
}


} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
