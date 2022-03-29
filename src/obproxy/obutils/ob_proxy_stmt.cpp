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
  UNUSED(sql_string);
  int ret = OB_SUCCESS;
  return ret;
}

int ObProxyDMLStmt::limit_to_sql_string(common::ObSqlString& sql_string)
{
  int ret = OB_SUCCESS;
  if (limit_size_ > 0) {
    if (OB_FAIL(sql_string.append_fmt(" LIMIT %d, %d", limit_offset_, limit_size_))) {
      LOG_WARN("fail to append", K(ret));
    }
  }
  return ret;
}
ObProxySelectStmt::ObProxySelectStmt() : is_inited_(false), has_rollup_(false),
                                         has_for_update_(false), from_token_off_(-1)
{

}

ObProxySelectStmt::~ObProxySelectStmt()
{
  for (int64_t i = 0; i < select_exprs_.count(); i++) {
    ObProxyExpr *expr = select_exprs_.at(i);
    expr->~ObProxyExpr();
  }

  for (int64_t i = 0; i < group_by_exprs_.count(); i++) {
    ObProxyGroupItem *group_expr = group_by_exprs_.at(i);
    group_expr->~ObProxyGroupItem();
  }

  for (int64_t i = 0; i < order_by_exprs_.count(); i++) {
    ObProxyOrderItem *order_expr = order_by_exprs_.at(i);
    order_expr->~ObProxyOrderItem();
  }

  ExprMap::iterator iter = table_exprs_map_.begin();
  ExprMap::iterator end = table_exprs_map_.end();
  for (; iter != end; iter++) {
    ObProxyExpr *expr = iter->second;
    expr->~ObProxyExpr();
  }

  table_exprs_map_.destroy();
  alias_table_map_.destroy();
}

int ObProxySelectStmt::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(table_exprs_map_.create(BUCKET_SIZE, ObModIds::OB_HASH_BUCKET_PROXY_MAP))) {
    LOG_WARN("fail to init table expr map", K(ret));
  } else if (OB_FAIL(alias_table_map_.create(BUCKET_SIZE, ObModIds::OB_HASH_BUCKET_PROXY_MAP))) {
    LOG_WARN("fail to init alias table set", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObProxySelectStmt::handle_parse_result(const ParseResult &parse_result)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  ParseNode* node = parse_result.result_tree_->children_[0];

  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    tmp_node = node->children_[i];
    if (NULL == tmp_node) {
      // do nothing
    } else if (i == PARSE_SELECT_HAVING) {
      ret = OB_ERROR_UNSUPPORT_EXPR_TYPE;
      LOG_WARN("having not support", K(ret), K(sql_string_));
    } else {
      switch(tmp_node->type_) {
        case T_FROM_LIST:
          from_token_off_ = tmp_node->token_off_;
          ret = handle_from_list(tmp_node);
          break;
        default:
          break;
      }
    }
  }

  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    tmp_node = node->children_[i];
    if (NULL == tmp_node) {
      // do nothing
    } else {
      switch(tmp_node->type_) {
        case T_HINT_OPTION_LIST:
          ret = handle_hint_clause(tmp_node);
          break;
        case T_PROJECT_LIST:
          ret = handle_project_list(tmp_node);
          break;
        case T_FROM_LIST:
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
        case T_SFU_INT:
          has_for_update_ = true;
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
  UNUSED(sql_string);
  int ret = OB_SUCCESS;
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
  UNUSED(sql_string);
  int ret = OB_SUCCESS;
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
  ParseNode* tmp_node = NULL;

  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    tmp_node = node->children_[i];
    if (NULL == tmp_node) {
      // do nothing
    }  else {
      switch(tmp_node->type_) {
        case T_RELATION_FACTOR:
          if (OB_FAIL(handle_table_and_db_in_hint(tmp_node))) {
            LOG_WARN("fail to handle table and db in hint", K(ret));
          }
          break;
        default:
          ret = handle_hint_clause(tmp_node);
          break;
      }
    }
  }

  return ret;
}

int ObProxySelectStmt::handle_limit_clause(ParseNode* node)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  int64_t tmp_val = 0;
  limit_token_off_ = node->token_off_;
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
            limit_offset_ = static_cast<int>(tmp_val);
          }
          break;
        case T_LIMIT_INT:
          if(OB_FAIL(get_int_value(tmp_node->str_value_, tmp_val))) {
            LOG_WARN("get_int_value failed", K(ret), K(tmp_node->str_value_), K(sql_string_));
          } else {
            limit_size_ = static_cast<int>(tmp_val);
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
  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    tmp_node = node->children_[i];
    expr = NULL;
    if (NULL == tmp_node) {
      // do nothing
    } else {
      switch(tmp_node->type_) {
        case T_PROJECT_STRING:
          if (OB_FAIL(project_string_to_expr(tmp_node, expr))) {
            LOG_WARN("project_string_to_expr failed", K(sql_string_), K(ret));
          } else if (OB_FAIL(select_exprs_.push_back(expr))) {
            LOG_WARN("push to array failed", K(sql_string_), K(ret));
          }
          break;
        default:
          ret = OB_ERROR_UNSUPPORT_EXPR_TYPE;
          LOG_WARN("unsupport type", "node_type", get_type_name(tmp_node->type_), K(sql_string_));
      }

      if (OB_FAIL(ret) && OB_NOT_NULL(expr)) {
        expr->~ObProxyExpr();
      }
    }
  }
  return ret;
}

int ObProxySelectStmt::get_const_expr(ParseNode* node, ObProxyExpr* &expr)
{
  int ret = OB_SUCCESS;
  ObProxyExprConst* expr_const = NULL;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected null", K(sql_string_), K(ret));
  } else if (OB_FAIL(get_expr_by_type(expr, OB_PROXY_EXPR_TYPE_CONST))) {
    LOG_WARN("get_expr_by_type failed", K(ret), K(sql_string_));
  } else if (OB_ISNULL(expr_const = dynamic_cast<ObProxyExprConst*>(expr))) {
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
      ObString var(node->token_len_, node->str_value_);
      obj.set_varchar(var);
      expr_const->set_object(obj);
      LOG_DEBUG("add varchar value", K(obj));
    }
  }
  return ret;
}

int ObProxySelectStmt::get_sharding_const_expr(ParseNode* node, ObProxyExpr* &expr)
{
  int ret = OB_SUCCESS;
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
    expr_const->set_expr_name(node->str_value_, node->token_len_);
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
  case OB_PROXY_EXPR_TYPE_CONST:
    ALLOC_PROXY_EXPR_BY_TYPE(ObProxyExprConst);
    break;
  case OB_PROXY_EXPR_TYPE_SHARDING_CONST:
    ALLOC_PROXY_EXPR_BY_TYPE(ObProxyExprShardingConst);
    break;
  case OB_PROXY_EXPR_TYPE_TABLE:
    ALLOC_PROXY_EXPR_BY_TYPE(ObProxyExprTable);
    break;
  case OB_PROXY_EXPR_TYPE_COLUMN:
    ALLOC_PROXY_EXPR_BY_TYPE(ObProxyExprColumn);
    break;
  case OB_PROXY_EXPR_TYPE_STAR:
    ALLOC_PROXY_EXPR_BY_TYPE(ObProxyExprStar);
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
  case OB_PROXY_EXPR_TYPE_FUNC_GROUP:
    ALLOC_PROXY_EXPR_BY_TYPE(ObProxyGroupItem);
    break;
  default:
    ret = OB_ERROR_UNSUPPORT_EXPR_TYPE;
    LOG_WARN("unexpected type", K(type));
    break;
  }
  return ret;
}

int ObProxySelectStmt::handle_table_and_db_node(ParseNode* node, ObProxyExprTable* &expr_table)
{
  int ret = OB_SUCCESS;

  ParseNode* db_node = node->children_[0];
  ParseNode* table_node = node->children_[1];
  if (OB_ISNULL(table_node)) {
    // do nothing
  } else if (OB_UNLIKELY(T_IDENT != table_node->type_ || NULL == table_node->str_value_ || 0 >= table_node->token_len_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table node", "node type", table_node->type_, "token len", table_node->token_len_, K(ret));
  } else if (NULL != db_node
             && OB_UNLIKELY(T_IDENT != db_node->type_ || NULL == db_node->str_value_ || 0 >= db_node->token_len_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected db node", "node type", db_node->type_, "token len", db_node->token_len_, K(ret));
  } else {
    ObProxyExpr* expr = NULL;
    ObString table_name = ObString::make_string(table_node->str_value_);
    if (OB_SUCCESS == alias_table_map_.get_refactored(table_name, expr)) {
      if (OB_ISNULL(expr_table = dynamic_cast<ObProxyExprTable*>(expr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dynamic_cast failed", K(ret));
      }
    } else if (OB_SUCCESS == table_exprs_map_.get_refactored(table_name, expr)) {
      if (OB_ISNULL(expr_table = dynamic_cast<ObProxyExprTable*>(expr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dynamic_cast failed", K(ret));
      } else {
        // Only the real table name needs to be rewritten, not the alias
        ObProxyExprTablePos expr_table_pos;
        if (NULL != db_node) {
          expr_table_pos.set_database_pos(db_node->token_off_);
        }
        expr_table_pos.set_table_pos(table_node->token_off_);
        expr_table_pos.set_table_expr(expr_table);
        if (OB_FAIL(table_pos_array_.push_back(expr_table_pos))) {
          LOG_WARN("fail to push expr table pos", K(ret));
        }
      }
    } else {
      ret = OB_ERR_BAD_FIELD_ERROR;
      LOG_WARN("table name of column is not alias name or real table name", K(table_name), K(ret));
    }
  }

  return ret;
}

int ObProxySelectStmt::handle_table_and_db_in_hint(ParseNode* node)
{
  int ret = OB_SUCCESS;

  ObProxyExprTable* expr_table = NULL;

  if (OB_T_RELATION_FACTOR_NUM_CHILD /* 2 */ != node->num_child_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children node is not 2", "child num", node->num_child_, K(ret));
  } else if (OB_FAIL(handle_table_and_db_node(node, expr_table))) {
    LOG_WARN("fail to handle table and db node", K(ret));
  }

  return ret;
}

int ObProxySelectStmt::column_ref_to_expr(ParseNode* node, ObProxyExpr* &expr, ObProxyExprTable* &expr_table)
{
  int ret = OB_SUCCESS;

  if (OB_T_COLUMN_REF_NUM_CHILD /* 3 */ != node->num_child_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("T_COLUMN_REF children node is not 3", "child num", node->num_child_, K(ret));
  } else if (OB_FAIL(handle_table_and_db_node(node, expr_table))) {
    LOG_WARN("fail to handle table and db node", K(ret));
  }

  if (OB_SUCC(ret)) {
    ParseNode* table_node = node->children_[1];
    ParseNode* column_node = node->children_[2];
    if (OB_ISNULL(column_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("T_COLUMN_REF unexpected column entry", K(ret));
    } else if (T_IDENT != column_node->type_ && T_STAR != column_node->type_) {
      // now column_ref child should be T_IDENT
      ret = OB_ERROR_UNSUPPORT_EXPR_TYPE;
      LOG_WARN("T_COLUMN_REF unexpected column entry", "node_type", get_type_name(column_node->type_), K(ret));
    } else if (T_IDENT == column_node->type_) {
      ObProxyExprColumn* expr_column = NULL;
      if (OB_FAIL(get_expr_by_type(expr, OB_PROXY_EXPR_TYPE_COLUMN))) {
        LOG_WARN("get_expr_by_type failed", K(ret), K(sql_string_));
      } else if (OB_ISNULL(expr_column = dynamic_cast<ObProxyExprColumn*>(expr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dynamic_cast failed", K(ret));
      } else {
        expr_column->set_column_name(column_node->str_value_, column_node->token_len_);
        if (OB_NOT_NULL(table_node)) {
          expr_column->set_table_name(table_node->str_value_, table_node->token_len_);
          if (NULL != expr_table) {
            expr_column->set_real_table_name(expr_table->get_table_name());
          }
        }
      }
    } else if (T_STAR == column_node->type_) {
      ObProxyExprStar* expr_star = NULL;
      if (OB_FAIL(get_expr_by_type(expr, OB_PROXY_EXPR_TYPE_STAR))) {
        LOG_WARN("get_expr_by_type failed", K(ret), K(sql_string_));
      } else if (OB_ISNULL(expr_star = dynamic_cast<ObProxyExprStar*>(expr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dynamic_cast failed", K(ret));
      } else {
        if (OB_NOT_NULL(table_node)) {
          expr_star->set_table_name(table_node->str_value_, table_node->token_len_);
        }
      }
    }
  }

  return ret;
}

int ObProxySelectStmt::alias_node_to_expr(ParseNode* node, ObProxyExpr* &expr, ParseNode* string_node)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  expr = NULL;
  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    tmp_node = node->children_[i];
    if (NULL == tmp_node) {
      //do nothing
    } else {
      switch(tmp_node->type_) {
        case T_IDENT:
          if (OB_ISNULL(expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get expr table", K(ret));
          } else if (OB_UNLIKELY(NULL == tmp_node->str_value_ || 0 >= tmp_node->token_len_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("alias token meet some wrong", "token len", tmp_node->token_len_, K(ret));
          } else {
            expr->set_alias_name(tmp_node->str_value_, tmp_node->token_len_);
          }
          break;
        default:
          if (OB_FAIL(string_node_to_expr(tmp_node, expr, string_node))) {
            LOG_WARN("string_node_to_expr failed", K(i), K(ret));
          }
      }
    }
  }
  return ret;
}

int ObProxySelectStmt::func_node_to_expr(ParseNode* node, ObProxyExpr* &expr)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  ObProxyExpr* tmp_expr = NULL;
  ObProxyFuncExpr* func_expr = NULL;
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
    }
  }

  if (OB_SUCC(ret)) {
    func_expr->set_expr_name(node->str_value_, node->token_len_);
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
          ret = OB_ERROR_UNSUPPORT_EXPR_TYPE;
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
    if (OB_FAIL(get_sharding_const_expr(node, expr))) {
      LOG_WARN("get_sharding_const_expr faield", K(ret));
    }
  } else if (OB_FAIL(get_sharding_const_expr(string_node, expr))) {
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
      if (OB_FAIL(get_expr_by_type(expr, OB_PROXY_EXPR_TYPE_STAR))) {
        LOG_WARN("get_expr_by_type failed", K(ret), K(sql_string_));
      }
      break;
    case T_COLUMN_REF: {
      ObProxyExprTable* expr_table = NULL;
      if (OB_FAIL(column_ref_to_expr(node, expr, expr_table))) {
        LOG_WARN("fail to column ref to expr", K(ret));
      }
      break;
    }
    case T_ALL:
      break;
    case T_ALIAS:
      if (OB_FAIL(alias_node_to_expr(node, expr, string_node))) {
        LOG_WARN("fail to alias node to expr", K(ret));
      }
      break;
    case T_FUN_SYS: // not support fun sys, as string_node
      if (OB_FAIL(func_sys_node_to_expr(node, expr, string_node))) {
        LOG_WARN("fail to func sys node to expr", K(ret));
      }
      break;
    case T_IDENT:
    case T_INT:
      if (OB_FAIL(get_const_expr(node, expr))) {
        LOG_WARN("fail to get sharding const expr", K(ret));
      }
      break;
    case T_VARCHAR:
      //varchar has varchar child, if child is 0 not have child
      if (node->num_child_ == 0) {
        if (OB_FAIL(get_const_expr(node, expr))) {
          LOG_WARN("fail to get sharding const expr succ", K(ret));
        }
      } else {
        for (i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
          if (node->children_[i] == NULL) {
            //do nothing
          } else if (expr != NULL) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr should null", K(ret), K(i), K(node->str_value_), K(sql_string_));
          } else  if (OB_FAIL(get_const_expr(node->children_[i], expr))) {
            LOG_WARN("fail to get sharding const expr succ", K(ret));
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
      if (OB_FAIL(func_node_to_expr(node, expr))) {
        LOG_WARN("fail to add func node expr succ", K(ret));
      }
      break;
    default:
      if (OB_FAIL(check_node_has_agg(node))) {
        LOG_WARN("unsupport type", "node_type", get_type_name(node->type_), K(node->str_value_));
      } else if (string_node == NULL) {
        ret = OB_ERROR_UNSUPPORT_EXPR_TYPE;
        LOG_WARN("unsupport type", "node_type", get_type_name(node->type_), K(node->str_value_), K(sql_string_));
      } else if (OB_FAIL(get_sharding_const_expr(string_node, expr))) {
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

int ObProxySelectStmt::get_table_and_db_expr(ParseNode* node, ObProxyExprTable* &expr_table)
{
  int ret = OB_SUCCESS;

  ObObj obj;
  ParseNode* table_node = NULL;
  ParseNode* db_node = NULL;
  ObProxyExpr* expr = NULL;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_T_RELATION_FACTOR_NUM_CHILD /* 2 */ != node->num_child_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("T_RELATION_FACTOR children node is not 2", K(node->num_child_), K(ret));
  } else if (FALSE_IT(db_node = node->children_[0])) {
  } else if (NULL != db_node
             && OB_UNLIKELY(T_IDENT != db_node->type_ || NULL == db_node->str_value_ || 0 >= db_node->token_len_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("T_RELATION_FACTOR unexpected table entry", "node type", db_node->type_,
             "token len", db_node->token_len_, K(ret));
  } else if (OB_ISNULL(table_node = node->children_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_UNLIKELY(T_IDENT != table_node->type_ || NULL == table_node->str_value_ || 0 >= table_node->token_len_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("T_RELATION_FACTOR unexpected table entry", "node type", table_node->type_,
             "token len", table_node->token_len_, K(ret));
  } else {
    ObString table_name(table_node->token_len_, table_node->str_value_);
    if (OB_FAIL(table_exprs_map_.get_refactored(table_name, expr))) { /* same table keep last one. */
      if (OB_HASH_NOT_EXIST == ret) {
        if (OB_FAIL(get_expr_by_type(expr, OB_PROXY_EXPR_TYPE_TABLE))) {
          LOG_WARN("get_expr_by_type failed", K(ret));
        } else if (OB_ISNULL(expr_table = dynamic_cast<ObProxyExprTable*>(expr))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dynamic_cast failed", K(ret));
        } else {
          if (NULL != db_node) {
            expr_table->set_database_name(db_node->str_value_, db_node->token_len_);
          }
          expr_table->set_table_name(table_node->str_value_, table_node->token_len_);

          if (OB_FAIL(table_exprs_map_.set_refactored(table_name, expr_table))) { /* same table keep last one. */
            LOG_WARN("fail to add table expr", K(table_name), K(ret));
          }
        }
      } else {
        LOG_WARN("fail to get table expr", K(table_name), K(ret));
      }
    } else {
      if (OB_ISNULL(expr_table = dynamic_cast<ObProxyExprTable*>(expr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dynamic_cast failed", K(ret));
      } else if (NULL != db_node && expr_table->get_database_name().empty()) {
        expr_table->set_database_name(db_node->str_value_, db_node->token_len_);
      }
    }
  }


  if (OB_SUCC(ret)) {
    ObProxyExprTablePos expr_table_pos;
    if (NULL != db_node) {
      expr_table_pos.set_database_pos(db_node->token_off_);
    }
    expr_table_pos.set_table_pos(table_node->token_off_);
    expr_table_pos.set_table_expr(expr_table);
    if (OB_FAIL(table_pos_array_.push_back(expr_table_pos))) {
      LOG_WARN("fail to push expr table pos", K(ret));
    }
  }

  return ret;
}

int ObProxySelectStmt::handle_table_node_to_expr(ParseNode* node)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  ObProxyExprTable* expr_table = NULL;
  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    tmp_node = node->children_[i];
    if (NULL == tmp_node) {
      //do nothing
    } else {
      switch(tmp_node->type_) {
        case T_RELATION_FACTOR:
          if (OB_FAIL(get_table_and_db_expr(tmp_node, expr_table))) {
            LOG_WARN("fail to get table expr", K(ret));
          }
          break;
        case T_IDENT:
          if (OB_ISNULL(expr_table)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get expr table", K(ret));
          } else if (OB_UNLIKELY(NULL == tmp_node->str_value_ || 0 >= tmp_node->token_len_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("alias token meet some wrong", "token len", tmp_node->token_len_, K(ret));
          } else {
            ObString alias_table = ObString::make_string(tmp_node->str_value_);
            if (OB_FAIL(alias_table_map_.set_refactored(alias_table, expr_table))) {
              LOG_WARN("fail to add alias table set", K(alias_table), K(ret));
            }
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

  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    tmp_node = node->children_[i];
    if (NULL == tmp_node) {
      // do nothing
    }  else {
      switch(tmp_node->type_) {
        case T_ORG:
        case T_ALIAS:
          if (OB_FAIL(handle_table_node_to_expr(tmp_node))) {
            LOG_WARN("fail to handle table node to expr", K(sql_string_), K(ret));
          }
          break;
        case T_OP_EQ:
        case T_OP_IN:
          if (OB_FAIL(handle_where_node(tmp_node))) {
            LOG_WARN("fail to handle where node", K(sql_string_), K(ret));
          }
          break;
        case T_COLUMN_REF: {
          ObProxyExprTable* expr_table = NULL;
          ObProxyExpr* expr = NULL;
          if (OB_FAIL(column_ref_to_expr(tmp_node, expr, expr_table))) {
            LOG_WARN("fail to column ref to expr", K(ret));
          }
          if (OB_NOT_NULL(expr)) {
            expr->~ObProxyExpr();
          }
          break;
        }
        default:
          if (OB_FAIL(handle_from_list(tmp_node))) {
            LOG_WARN("fail to handle from list", K(sql_string_), K(ret));
          }
          break;
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

int ObProxySelectStmt::handle_where_node(ParseNode* node)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  ObProxyExpr* expr = NULL;
  bool have_column = false;
  SqlField sql_field;
  SqlColumnValue column_value;
  bool is_skip_field = false;
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
          case T_COLUMN_REF: {
            ObProxyExprColumn* expr_column = NULL;
            ObProxyExprTable* expr_table = NULL;
            if (OB_FAIL(column_ref_to_expr(tmp_node, expr, expr_table))) {
              LOG_WARN("fal to get column expr", K(ret));
            } else if (OB_ISNULL(expr_column = dynamic_cast<ObProxyExprColumn*>(expr))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("dynamic_cast failed", K(ret));
            } else if (have_column) {
                // where t1.c1 = t2.c2
                is_skip_field = true;
            } else {
              if (NULL == expr_table
                  || 0 == expr_table->get_table_name().case_compare(table_name_)) {
                sql_field.column_name_.set(expr_column->get_column_name());
              } else {
                is_skip_field = true;
              }
              have_column = true;
            }

            if (OB_NOT_NULL(expr)) {
              expr->~ObProxyExpr();
            }
            break;
          }
          case T_INT:
          case T_NUMBER:
            column_value.value_type_ = TOKEN_STR_VAL;
            column_value.column_value_.set(tmp_node->str_value_);
            sql_field.column_values_.push_back(column_value);
            break;
          case T_VARCHAR:
            ret = handle_varchar_node_in_where_condition(tmp_node, sql_field);
            break;
          case T_EXPR_LIST:
            if (OB_FAIL(handle_where_expr_list_node(tmp_node, sql_field))) {
              LOG_WARN("handle_where_expr_list_node failed", K(ret), K(i), K(sql_string_));
            }
            break;
          default:
            ret = OB_ERROR_UNSUPPORT_EXPR_TYPE;
            LOG_WARN("unexpected expr type", "node_type", get_type_name(tmp_node->type_), K(sql_string_));
        }
      }
    }

    if (OB_SUCC(ret) && !is_skip_field) {
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

int ObProxySelectStmt::handle_where_clause(ParseNode* node)
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
      } else {
        switch(tmp_node->type_) {
          case T_OP_EQ:
          case T_OP_IN:
            if (OB_FAIL(handle_where_node(tmp_node))) {
              LOG_WARN("fail to handle where node", K(sql_string_), K(ret));
            }
            break;
          case T_COLUMN_REF: {
            ObProxyExprTable* expr_table = NULL;
            ObProxyExpr* expr = NULL;
            if (OB_FAIL(column_ref_to_expr(tmp_node, expr, expr_table))) {
              LOG_WARN("fail to column ref to expr", K(ret));
            }
            if (OB_NOT_NULL(expr)) {
              expr->~ObProxyExpr();
            }
            break;
          }
          default:
            if (OB_FAIL(handle_where_clause(tmp_node))) {
              LOG_WARN("handle_where_nodes failed", K(sql_string_), K(ret));
            }
            break;
        }
      }
    }
  }
  return ret;
}

int ObProxySelectStmt::handle_sort_key_node(ParseNode* node, ObProxyExpr* &expr, const SortListType& sort_list_type)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
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
        if (OB_FAIL(string_node_to_expr(tmp_node, tmp_expr))) {
          LOG_WARN("string_node_to_expr failed", K(ret), K(sql_string_));
        } else if (T_INT == tmp_node->type_) {
          ObProxyExprConst* expr_const = NULL;
          if (OB_ISNULL(expr_const = dynamic_cast<ObProxyExprConst*>(tmp_expr))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("dynamic_cast failed", K(ret));
          } else {
            int64_t index = expr_const->get_object().get_int();
            if (index > 0) {
              tmp_expr->set_index(index - 1);
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObProxyGroupItem* group_item_expr = NULL;
    ObProxyExprType expr_type;
    if (sort_list_type == SORT_LSIT_IN_ORDER_BY) {
      expr_type = OB_PROXY_EXPR_TYPE_FUNC_ORDER;
    } else {
      expr_type = OB_PROXY_EXPR_TYPE_FUNC_GROUP;
    }
      // do nothing
    if (OB_FAIL(get_expr_by_type(expr, expr_type))) {
      LOG_WARN("get_expr_by_type", K(expr_type), K(sql_string_), K(ret));
    } else if (OB_ISNULL(group_item_expr = dynamic_cast<ObProxyGroupItem*>(expr))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dynamic_cast to ObProxyGroupItem failed", K(ret));
    } else {
      if (tmp_expr->has_agg_) {
        group_item_expr->has_agg_ = 1;
      }
      group_item_expr->set_expr(tmp_expr);
      if (sort_list_type == SORT_LSIT_IN_ORDER_BY) {
        ObProxyOrderItem* order_item_expr = NULL;
        if (OB_ISNULL(order_item_expr = dynamic_cast<ObProxyOrderItem*>(expr))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dynamic_cast to ObProxyOrderItem failed", K(ret));
        } else {
          order_item_expr->order_direction_ = order_direction;
        }
      }
    }
  }
  return ret;
}
int ObProxySelectStmt::handle_sort_list_node(ParseNode* node, const SortListType& sort_list_type)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  ObProxyExpr* expr = NULL;
  ObProxyGroupItem* group_item_expr = NULL;
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
            LOG_WARN("handle_sort_key_node", K(sql_string_), K(ret));
          } else {
            switch(sort_list_type) {
              case SORT_LIST_IN_GROUP_BY:
                if (OB_ISNULL(group_item_expr = dynamic_cast<ObProxyGroupItem*>(expr))) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("dynamic_cast to ObProxyGroupItem failed", K(ret));
                } else if (OB_FAIL(group_by_exprs_.push_back(group_item_expr))) {
                  LOG_WARN("push_back failed", K(sql_string_), K(ret));
                }
                break;
              case SORT_LSIT_IN_ORDER_BY:
                if (OB_ISNULL(order_item_expr = dynamic_cast<ObProxyOrderItem*>(expr))) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("dynamic_cast to ObProxyOrderItem failed", K(ret));
                } else if (OB_FAIL(order_by_exprs_.push_back(order_item_expr))) {
                  LOG_WARN("push_back failed", K(sql_string_), K(ret));
                }
                break;
              default:
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("invalid sort_list_type", K(sort_list_type), K(sql_string_), K(ret));
            }
          }
          break;
        default:
          ret = OB_ERROR_UNSUPPORT_EXPR_TYPE;
          LOG_WARN("unsupport expr type", "node_type", get_type_name(tmp_node->type_), K(sql_string_), K(ret));
      }
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(expr)) {
    expr->~ObProxyExpr();
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

  return ret;
}

int ObProxySelectStmt::handle_orderby_clause(ParseNode* node)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
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

  return ret;
}


} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
