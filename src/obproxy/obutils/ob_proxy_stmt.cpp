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

ObProxyDMLStmt::ObProxyDMLStmt(common::ObIAllocator& allocator): ObProxyStmt(allocator), limit_offset_(0), limit_size_(-1),
                limit_token_off_(-1), dml_field_results_(), comments_(), table_name_(), is_inited_(false),
                has_unsupport_expr_type_(false), has_unsupport_expr_type_for_config_(false), has_sub_select_(false), use_column_value_from_hint_(false),
                table_pos_array_(), has_rollup_(false), has_for_update_(false), from_token_off_(-1), t_case_level_(0)
{
  field_results_ = &dml_field_results_;
}

ObProxyDMLStmt::~ObProxyDMLStmt()
{
  ExprMap::iterator iter = table_exprs_map_.begin();
  ExprMap::iterator end = table_exprs_map_.end();
  for (; iter != end; iter++) {
    ObProxyExpr *expr = iter->second;
    expr->~ObProxyExpr();
  }

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

  table_exprs_map_.destroy();
  alias_table_map_.destroy();
}

int ObProxyDMLStmt::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("init twice", K(ret));
  } else if (OB_FAIL(table_exprs_map_.create(BUCKET_SIZE, ObModIds::OB_HASH_BUCKET_PROXY_MAP))) {
    LOG_WDIAG("fail to init table expr map", K(ret));
  } else if (OB_FAIL(alias_table_map_.create(BUCKET_SIZE, ObModIds::OB_HASH_BUCKET_PROXY_MAP))) {
    LOG_WDIAG("fail to init alias table set", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

ObProxyExprType ObProxyDMLStmt::get_expr_type_by_node_type(const ObItemType& item_type)
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

int ObProxyDMLStmt::get_expr_by_type(ObProxyExpr* &expr, ObProxyExprType type)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
#define ALLOC_PROXY_EXPR_BY_TYPE(ExprClass) \
    if (OB_ISNULL(buf = (char*)(allocator_.alloc(sizeof(ExprClass))))) { \
      ret = OB_ALLOCATE_MEMORY_FAILED; \
      LOG_WDIAG("fail to alloc mem", K(ret)); \
    } else if (OB_ISNULL(expr = new (buf)ExprClass())) { \
      ret = OB_ALLOCATE_MEMORY_FAILED; \
      LOG_WDIAG("fail to new expr", K(ret)); \
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
    has_unsupport_expr_type_ = true;
    LOG_DEBUG("unexpected type", K(type));
    break;
  }
  return ret;
}

int ObProxyDMLStmt::handle_from_list(ParseNode* node)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;

  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    tmp_node = node->children_[i];
    if (NULL == tmp_node) {
      // do nothing
    } else {
      switch(tmp_node->type_) {
        case T_ORG:
        case T_ALIAS:
          if (OB_FAIL(handle_table_node_to_expr(tmp_node))) {
            LOG_WDIAG("fail to handle table node to expr", K(sql_string_), K(ret));
          }
          break;
        case T_OP_EQ:
        case T_OP_IN:
          if (OB_FAIL(handle_column_and_value(tmp_node))) {
            LOG_WDIAG("fail to handle where node", K(sql_string_), K(ret));
          }
          break;
        case T_COLUMN_REF: {
          ObProxyExprTable* expr_table = NULL;
          ObProxyExpr* expr = NULL;
          if (OB_FAIL(column_ref_to_expr(tmp_node, expr, expr_table))) {
            LOG_WDIAG("fail to column ref to expr", K(ret));
          }
          if (OB_NOT_NULL(expr)) {
            expr->~ObProxyExpr();
          }
          break;
        }
        default:
          if (OB_FAIL(handle_from_list(tmp_node))) {
            LOG_WDIAG("fail to handle from list", K(sql_string_), K(ret));
          }
          break;
      }
    }
  }
  return ret;
}

int ObProxyDMLStmt::handle_table_node_to_expr(ParseNode* node)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  ObProxyExprTable* expr_table = NULL;
  bool is_sub_query = false;
  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    tmp_node = node->children_[i];
    if (NULL == tmp_node) {
      //do nothing
    } else {
      switch(tmp_node->type_) {
        case T_RELATION_FACTOR:
          if (OB_FAIL(get_table_and_db_expr(tmp_node, expr_table))) {
            LOG_WDIAG("fail to get table expr", K(ret));
          }
          break;
        case T_IDENT:
          if (is_sub_query) {
            // skip alias
          } else if (OB_ISNULL(expr_table)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("fail to get expr table", K(ret));
          } else if (OB_UNLIKELY(NULL == tmp_node->str_value_ || 0 >= tmp_node->str_len_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("alias token meet some wrong", "str len", tmp_node->str_len_, K(ret));
          } else {
            ObString alias_table(static_cast<int32_t>(tmp_node->str_len_), tmp_node->str_value_);
            if (OB_FAIL(alias_table_map_.set_refactored(alias_table, expr_table))) {
              LOG_WDIAG("fail to add alias table set", K(alias_table), K(ret));
            }
          }
          break;
        case T_SELECT:
          has_sub_select_ = true;
          if (OB_FAIL(do_handle_parse_result(tmp_node))) {
            LOG_WDIAG("fail to do handle parse result", "node_type", get_type_name(tmp_node->type_), K(ret));
          } else {
            is_sub_query = true;
          }
          break;
        default:
          has_unsupport_expr_type_ = true;
          LOG_DEBUG("unsupport type", "node_type", get_type_name(node->type_),  K(node->str_value_));
      }
    }
  }
  return ret;
}

int ObProxyDMLStmt::get_table_and_db_expr(ParseNode* node, ObProxyExprTable* &expr_table)
{
  int ret = OB_SUCCESS;

  ObObj obj;
  ParseNode* table_node = NULL;
  ParseNode* db_node = NULL;
  ObProxyExpr* expr = NULL;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("unexpected null", K(ret));
  } else if (OB_T_RELATION_FACTOR_NUM_CHILD /* 2 */ != node->num_child_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("T_RELATION_FACTOR children node is not 2", K(node->num_child_), K(ret));
  } else if (FALSE_IT(db_node = node->children_[0])) {
  } else if (NULL != db_node
             && OB_UNLIKELY(T_IDENT != db_node->type_ || NULL == db_node->str_value_ || 0 >= db_node->str_len_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("T_RELATION_FACTOR unexpected table entry", "node type", db_node->type_,
             "str len", db_node->str_len_, K(ret));
  } else if (OB_ISNULL(table_node = node->children_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected null", K(ret));
  } else if (OB_UNLIKELY(T_IDENT != table_node->type_ || NULL == table_node->str_value_ || 0 >= table_node->str_len_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("T_RELATION_FACTOR unexpected table entry", "node type", table_node->type_,
             "str len", table_node->str_len_, K(ret));
  } else {
    ObString table_name(static_cast<int32_t>(table_node->str_len_), table_node->str_value_);
    string_to_upper_case(table_name.ptr(), table_name.length()); // change all to upper to store
    if (OB_FAIL(table_exprs_map_.get_refactored(table_name, expr))) { /* same table keep last one. */
      if (OB_HASH_NOT_EXIST == ret) {
        if(table_name_.empty())
          table_name_ = table_name; // set 'table_name_' be the first table name in SQL, and convert to UPPER CASE
        if (OB_FAIL(get_expr_by_type(expr, OB_PROXY_EXPR_TYPE_TABLE))) {
          LOG_WDIAG("get_expr_by_type failed", K(ret));
        } else if (OB_ISNULL(expr_table = dynamic_cast<ObProxyExprTable*>(expr))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("dynamic_cast failed", K(ret));
        } else {
          if (NULL != db_node) {
            ObString database_name(static_cast<int32_t>(db_node->str_len_), db_node->str_value_);
            string_to_upper_case(database_name.ptr(), database_name.length()); // change all to upper to store
            expr_table->set_database_name(db_node->str_value_, static_cast<int32_t>(db_node->str_len_));
          }
          expr_table->set_table_name(table_node->str_value_, static_cast<int32_t>(table_node->str_len_));

          if (OB_FAIL(table_exprs_map_.set_refactored(table_name, expr_table))) { /* same table keep last one. */
            LOG_WDIAG("fail to add table expr", K(table_name), K(ret));
          }
        }
      } else {
        LOG_WDIAG("fail to get table expr", K(table_name), K(ret));
      }
    } else {
      if (OB_ISNULL(expr_table = dynamic_cast<ObProxyExprTable*>(expr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("dynamic_cast failed", K(ret));
      } else if (NULL != db_node && expr_table->get_database_name().empty()) {
        ObString database_name(static_cast<int32_t>(db_node->str_len_), db_node->str_value_);
        string_to_upper_case(database_name.ptr(), database_name.length()); // change all to upper to store
        expr_table->set_database_name(db_node->str_value_, static_cast<int32_t>(db_node->str_len_));
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
      LOG_WDIAG("fail to push expr table pos", K(ret));
    }
  }

  return ret;
}

int ObProxyDMLStmt::handle_table_and_db_node(ParseNode* node, ObProxyExprTable* &expr_table)
{
  int ret = OB_SUCCESS;

  ParseNode* db_node = node->children_[0];
  ParseNode* table_node = node->children_[1];
  if (OB_ISNULL(table_node)) {
    // do nothing
  } else if (OB_UNLIKELY(T_IDENT != table_node->type_ || NULL == table_node->str_value_ || 0 >= table_node->str_len_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected table node", "node type", table_node->type_, "str len", table_node->str_len_, K(ret));
  } else if (NULL != db_node
             && OB_UNLIKELY(T_IDENT != db_node->type_ || NULL == db_node->str_value_ || 0 >= db_node->str_len_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected db node", "node type", db_node->type_, "str len", db_node->str_len_, K(ret));
  } else {
    ObProxyExpr* expr = NULL;
    ObString table_name = ObString::make_string(table_node->str_value_);
    if (OB_SUCCESS == alias_table_map_.get_refactored(table_name, expr)) {
      if (OB_ISNULL(expr_table = dynamic_cast<ObProxyExprTable*>(expr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("dynamic_cast failed", K(ret));
      }
    } else {
      string_to_upper_case(table_name.ptr(), table_name.length()); // change all to upper to store
      if (OB_SUCCESS == table_exprs_map_.get_refactored(table_name, expr)) {
        if (OB_ISNULL(expr_table = dynamic_cast<ObProxyExprTable*>(expr))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("dynamic_cast failed", K(ret));
        } else {
          // Only the real table name needs to be rewritten, not the alias
          ObProxyExprTablePos expr_table_pos;
          if (NULL != db_node) {
            expr_table_pos.set_database_pos(db_node->token_off_);
          }
          expr_table_pos.set_table_pos(table_node->token_off_);
          expr_table_pos.set_table_expr(expr_table);
          if (OB_FAIL(table_pos_array_.push_back(expr_table_pos))) {
            LOG_WDIAG("fail to push expr table pos", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObProxyDMLStmt::handle_table_references(ParseNode *node)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    ParseNode *tmp_node = node->children_[i];
    if (NULL == tmp_node) {
      //do nothing
    } else {
      switch (tmp_node->type_) {
        case T_ORG:
        case T_ALIAS:
          if (OB_FAIL(handle_table_node_to_expr(tmp_node))) {
            LOG_WDIAG("fail to handle table node to expr", K(sql_string_), K(ret));
          }
          break;
        //sql: delete tbl_name1,tbl_name2 ...
        //MULTI DELETE STMT is not support now
        case T_RELATION_FACTOR:
          break;
        default:
          has_unsupport_expr_type_ = true;
          LOG_DEBUG("unknown node type", K_(tmp_node->type), K(ret));
      }
    }
  }
  return ret;
}

int ObProxyDMLStmt::handle_where_clause(ParseNode* node)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  if (OB_ISNULL(field_results_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected null");
  } else {
    for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
      tmp_node = node->children_[i];
      if (NULL == tmp_node) {
        // do nothing
      } else {
        switch(tmp_node->type_) {
          case T_OP_EQ:
          case T_OP_IN:
            if (OB_FAIL(handle_column_and_value(tmp_node))) {
              LOG_WDIAG("fail to handle where node", K(sql_string_), K(ret));
            }
            break;
          case T_COLUMN_REF: {
            ObProxyExprTable* expr_table = NULL;
            ObProxyExpr* expr = NULL;
            if (OB_FAIL(column_ref_to_expr(tmp_node, expr, expr_table))) {
              LOG_WDIAG("fail to column ref to expr", K(ret));
            }
            if (OB_NOT_NULL(expr)) {
              expr->~ObProxyExpr();
            }
            break;
          }
          case T_CASE:
            t_case_level_++;
            if (OB_FAIL(handle_where_clause(tmp_node))) {
              LOG_WDIAG("handle_where_nodes failed", K(sql_string_), K(ret));
            }
            t_case_level_--;
            break;
          case T_NULL: { // ignore NULL in where clause
            break;
          }
          default:
            if (OB_FAIL(handle_where_clause(tmp_node))) {
              LOG_WDIAG("handle_where_nodes failed", K(sql_string_), K(ret));
            }
            break;
        }
        //Intercept unsupported expr for config management
        switch(tmp_node->type_) {
          case T_OP_EQ:
          case T_OP_AND:
            break;
          default:
            has_unsupport_expr_type_for_config_ = true;
        }
      }
    }
  }
  return ret;
}

int ObProxyDMLStmt::handle_column_and_value(ParseNode* node)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  ObProxyExpr* expr = NULL;
  bool have_column = false;
  SqlField sql_field;
  SqlColumnValue column_value;
  bool is_skip_field = use_column_value_from_hint_;
  if (t_case_level_ > 0) {
    is_skip_field = true;
  }
  if (OB_ISNULL(field_results_)) {
    LOG_WDIAG("unexpected null");
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
              LOG_WDIAG("fal to get column expr", K(ret));
            } else if (OB_ISNULL(expr_column = dynamic_cast<ObProxyExprColumn*>(expr))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WDIAG("dynamic_cast failed", K(ret));
            } else if (have_column) {
                // where t1.c1 = t2.c2
                is_skip_field = true;
            } else {
              if (NULL == expr_table
                  || 0 == expr_table->get_table_name().case_compare(table_name_)) {
                sql_field.column_name_.set_value(expr_column->get_column_name());
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
            if (T_INT == tmp_node->type_) {
              sql_field.value_type_ = TOKEN_INT_VAL;
              sql_field.column_int_value_ = tmp_node->value_;
            }
            column_value.value_type_ = TOKEN_STR_VAL;
            column_value.column_value_.set_value(tmp_node->str_value_);
            ret = sql_field.column_values_.push_back(column_value);
            break;
          case T_VARCHAR:
            ret = handle_varchar_node_in_column_value(tmp_node, sql_field);
            break;
          case T_EXPR_LIST:
            if (OB_FAIL(handle_expr_list_node_in_column_value(tmp_node, sql_field))) {
              LOG_WDIAG("handle_expr_list_node_in_column_value failed", K(ret), K(i), K(sql_string_));
            }
            break;
          case T_NULL: { // ignore NULL in where clause
            is_skip_field = true;
            has_unsupport_expr_type_for_config_ = true;
            break;
          }
          case T_SELECT: {
            is_skip_field = true;
            has_unsupport_expr_type_for_config_ = true;
            has_sub_select_ = true;
            if (OB_FAIL(do_handle_parse_result(tmp_node))) {
              LOG_WDIAG("fail to do handle parse result", "node_type", get_type_name(tmp_node->type_), K(ret));
            }
            break;
          }
          default:
            is_skip_field = true;
            has_unsupport_expr_type_ = true;
            LOG_DEBUG("unexpected expr type", "node_type", get_type_name(tmp_node->type_), K(sql_string_));
        }
      }
    }

    if (OB_SUCC(ret) && !is_skip_field) {
      int duplicate_column_idx = -1;
      for (int i = 0; i < field_results_->fields_.count(); i++) {
        if (0 == sql_field.column_name_.config_string_.case_compare(field_results_->fields_[i]->column_name_.config_string_)) {
          duplicate_column_idx = i;
          break;
        }
      }
      if (-1 != duplicate_column_idx) {
        for (int i = 0; OB_SUCC(ret) && i < sql_field.column_values_.count(); i++) {
          SqlColumnValue tmp_column_value = sql_field.column_values_[i];
          if (OB_FAIL(field_results_->fields_.at(duplicate_column_idx)->column_values_.push_back(tmp_column_value))) {
            LOG_WDIAG("push_back failed", K(ret), K(sql_string_));
          }
        }
      } else {
        SqlField *tmp_field = NULL;
        if (OB_FAIL(SqlField::alloc_sql_field(tmp_field))) {
          LOG_WDIAG("fail to allocate memory for sqlfield", K(ret));
        } else {
          *tmp_field = sql_field;
          if (OB_FAIL(field_results_->fields_.push_back(tmp_field))) {
            tmp_field->reset();
            tmp_field = NULL;
            LOG_WDIAG("push_back failed", K(ret), K(sql_string_));
          } else {
            ++field_results_->field_num_;
            LOG_DEBUG("add sql_field", KPC(tmp_field), K(field_results_->field_num_));
          }
        }
      }
    }
  }
  return ret;
}

int ObProxyDMLStmt::handle_varchar_node_in_column_value(ParseNode* node, SqlField& sql_field)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_varchar_node = NULL;
  SqlColumnValue column_value;
  int i = 0;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected null", K(ret));
  } else if (node->num_child_ == 0) {
    //there is implicit type conversion: const char* -> ObString
    sql_field.column_value_.set_value(node->str_value_);
    column_value.value_type_ = TOKEN_STR_VAL;
    column_value.column_value_.set_value(node->str_value_);
    ret = sql_field.column_values_.push_back(column_value);
  } else {
    tmp_varchar_node = NULL;
    for (i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
      if ((node->children_[i] == NULL) || (node->children_[i]->type_ != T_VARCHAR)) {
        //do nothing
      } else {
        tmp_varchar_node = node->children_[i];
        break;
      }
    }
    if (tmp_varchar_node == NULL) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("shoule not null here", K(ret), K(node->str_value_), K(sql_string_));
    } else {
      //there is implicit type conversion: const char* -> ObString
      sql_field.column_value_.set_value(tmp_varchar_node->str_value_);
      sql_field.value_type_ = TOKEN_STR_VAL;
      column_value.value_type_ = TOKEN_STR_VAL;
      column_value.column_value_.set_value(tmp_varchar_node->str_value_);
      if (OB_FAIL(sql_field.column_values_.push_back(column_value))) {
        LOG_WDIAG("fail to push column_value", K(ret));
      }

      bool need_save = false;
      ShardingPosType type = SHARDING_POS_NONE;
      if (0 == sql_field.column_name_.config_string_.case_compare("SCHEMA_NAME")
          || 0 == sql_field.column_name_.config_string_.case_compare("TABLE_SCHEMA")) {
        need_save = true;
        type = SHARDING_POS_DB;
      } else if (0 == sql_field.column_name_.config_string_.case_compare("TABLE_NAME")) {
        need_save = true;
        type = SHARDING_POS_TABLE;
      }

      if (need_save) {
        ObProxyDbTablePos db_table_pos;
        db_table_pos.set_pos(tmp_varchar_node->token_off_);
        db_table_pos.set_name(tmp_varchar_node->str_value_);
        db_table_pos.set_type(type);
        if (OB_FAIL(db_table_pos_array_.push_back(db_table_pos))) {
          LOG_WDIAG("fail to push expr table pos", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObProxyDMLStmt::handle_expr_list_node_in_column_value(ParseNode* node, SqlField& sql_field)
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
          ret = handle_varchar_node_in_column_value(tmp_node, sql_field);
          break;
        case T_INT:
        default:
          column_value.value_type_ = TOKEN_STR_VAL;
          column_value.column_value_.set_value(tmp_node->str_value_);
          if (OB_FAIL(sql_field.column_values_.push_back(column_value))) {
            LOG_WDIAG("push_back failed", K(i), K(sql_string_), K(ret));
          }
          break;
      }
    }
  }
  return ret;
}

int ObProxyDMLStmt::handle_assign_list(ParseNode *node)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    ParseNode *tmp_node = node->children_[i];
    if (NULL == tmp_node) {
      //do nothing
    } else {
      switch (tmp_node->type_) {
        case T_ASSIGN_ITEM:
          if (OB_FAIL(handle_column_and_value(tmp_node))) {
            LOG_WDIAG("handle op and failed", K(ret));
          }
          break;
        default:
          has_unsupport_expr_type_ = true;
          LOG_DEBUG("unknown node type", K_(tmp_node->type), K(ret));
          break;
      }
    }
  }
  return ret;
}

int ObProxyDMLStmt::handle_column_ref_in_list(ParseNode *node)
{
  int ret = OB_SUCCESS;
  ObProxyExpr* expr = NULL;
  ObProxyExprColumn* expr_column = NULL;
  ObProxyExprTable* expr_table = NULL;
  ObString column_name;
  if (OB_FAIL(column_ref_to_expr(node, expr, expr_table))) {
    LOG_WDIAG("fal to get column expr", K(ret));
  } else if (OB_ISNULL(expr_column = dynamic_cast<ObProxyExprColumn*>(expr))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("dynamic_cast failed", K(ret));
  } else if ((NULL != expr_table)
              && (0 != expr_table->get_table_name().case_compare(table_name_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("table name is wrong in assign list", K(ret));
  } else {
    column_name = expr_column->get_column_name();
  }

  if(OB_SUCC(ret)) {
    if (OB_UNLIKELY(use_column_value_from_hint_)) {
      // nothing
    } else {
      SqlField* tmp_sql_field = NULL;
      if (OB_FAIL(SqlField::alloc_sql_field(tmp_sql_field))) {
        LOG_WDIAG("fail to allocate memory for sqlfield", K(ret));
      } else {
        tmp_sql_field->column_name_.set_value(column_name);
        if (OB_FAIL(field_results_->fields_.push_back(tmp_sql_field))) {
          tmp_sql_field->reset();
          tmp_sql_field = NULL;
          LOG_WDIAG("column name push back failed", K(ret));
        } else {
          ++field_results_->field_num_;
          LOG_DEBUG("add sql_field", K(tmp_sql_field), K(field_results_->field_num_));
        }
      }
    }
  }
  return ret;
}

int ObProxyDMLStmt::column_ref_to_expr(ParseNode* node, ObProxyExpr* &expr, ObProxyExprTable* &expr_table)
{
  int ret = OB_SUCCESS;

  if (OB_T_COLUMN_REF_NUM_CHILD /* 3 */ != node->num_child_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("T_COLUMN_REF children node is not 3", "child num", node->num_child_, K(ret));
  } else if (OB_FAIL(handle_table_and_db_node(node, expr_table))) {
    LOG_WDIAG("fail to handle table and db node", K(ret));
  }

  if (OB_SUCC(ret)) {
    ParseNode* table_node = node->children_[1];
    ParseNode* column_node = node->children_[2];
    if (OB_ISNULL(column_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("T_COLUMN_REF unexpected column entry", K(ret));
    } else if (T_IDENT != column_node->type_ && T_STAR != column_node->type_) {
      // now column_ref child should be T_IDENT
      has_unsupport_expr_type_ = true;
      LOG_DEBUG("T_COLUMN_REF unexpected column entry", "node_type", get_type_name(column_node->type_), K(ret));
    } else if (T_IDENT == column_node->type_) {
      ObProxyExprColumn* expr_column = NULL;
      if (OB_FAIL(get_expr_by_type(expr, OB_PROXY_EXPR_TYPE_COLUMN))) {
        LOG_WDIAG("get_expr_by_type failed", K(ret), K(sql_string_));
      } else if (OB_ISNULL(expr_column = dynamic_cast<ObProxyExprColumn*>(expr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("dynamic_cast failed", K(ret));
      } else {
        expr_column->set_column_name(column_node->str_value_, static_cast<int32_t>(column_node->str_len_));
        if (OB_NOT_NULL(table_node)) {
          expr_column->set_table_name(table_node->str_value_, static_cast<int32_t>(table_node->str_len_));
          if (NULL != expr_table) {
            expr_column->set_real_table_name(expr_table->get_table_name());
          }
        }
      }
    } else if (T_STAR == column_node->type_) {
      ObProxyExprStar* expr_star = NULL;
      if (OB_FAIL(get_expr_by_type(expr, OB_PROXY_EXPR_TYPE_STAR))) {
        LOG_WDIAG("get_expr_by_type failed", K(ret), K(sql_string_));
      } else if (OB_ISNULL(expr_star = dynamic_cast<ObProxyExprStar*>(expr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("dynamic_cast failed", K(ret));
      } else {
        if (OB_NOT_NULL(table_node)) {
          expr_star->set_table_name(table_node->str_value_, static_cast<int32_t>(table_node->str_len_));
        }
      }
    }
  }

  return ret;
}

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
      LOG_WDIAG("fail to append", K(ret));
    }
  }
  return ret;
}

int ObProxyDMLStmt::handle_explain_node(const ParseResult &parse_result, ParseNode*& node)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(parse_result.result_tree_) || OB_ISNULL(parse_result.result_tree_->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("parse result info is null unexpected", K(ret));
  } else {
    node = parse_result.result_tree_->children_[0];
    if (OB_UNLIKELY(node->type_ == T_EXPLAIN)) {
      if ((node->num_child_ >= 2) && OB_NOT_NULL(node->children_[1])) {
        node = node->children_[1];
        LOG_DEBUG("succ to parse explain stmt", K(node->type_), K(ret));
      } else {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WDIAG("fail to parse explain stmt", K(node->type_), K(ret));
      }
    }
  }

  return ret;
}

int ObProxyDMLStmt::do_handle_parse_result(ParseNode* node)
{
  int ret = OB_SUCCESS;

  ParseNode* tmp_node = NULL;

  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    tmp_node = node->children_[i];
    if (NULL == tmp_node) {
      // do nothing
    } else if (i == PARSE_SELECT_HAVING) {
      has_unsupport_expr_type_ = true;
      LOG_DEBUG("having not support", K(ret), K(sql_string_));
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
        case T_SET_UNION:
          ret = handle_union_clause(tmp_node);
          break;
        case T_SELECT:
          has_sub_select_ = true;
          if (OB_FAIL(do_handle_parse_result(tmp_node))) {
            LOG_WDIAG("fail to do handle parse result", "node_type", get_type_name(tmp_node->type_), K(ret));
          }
          break;
        case T_QEURY_EXPRESSION_LIST: //distinct not support now
        default:
          has_unsupport_expr_type_ = true;
          LOG_DEBUG("unsupport type", "node_type", get_type_name(tmp_node->type_), K(sql_string_), K(ret));
      }
    }
  }

  return ret;
}

int ObProxyDMLStmt::handle_comment_list(const ParseResult &parse_result)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < parse_result.comment_cnt_; i++) {
    TokenPosInfo& token_info = parse_result.comment_list_[i];
    ObString comment(token_info.token_len_, sql_string_.ptr() + token_info.token_off_);
    if (OB_FAIL(comments_.push_back(comment))) {
      LOG_WDIAG("push_back failed", K(ret), K(comment), K(sql_string_));
    } else {
      LOG_DEBUG("push_back succ", K(comment));
    }
  }
  return ret;
}

int ObProxyDMLStmt::comments_to_sql_string(common::ObSqlString& sql_string)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < comments_.count(); i++) {
    if (OB_FAIL(sql_string.append(comments_.at(i)))) {
      LOG_WDIAG("fail to append", K(ret), K(i), K(comments_.at(i)), K(sql_string_));
    }
  }
  return ret;
}

int ObProxyDMLStmt::hint_exprs_to_sql_string(common::ObSqlString& sql_string)
{
  UNUSED(sql_string);
  int ret = OB_SUCCESS;
  return ret;
}
int ObProxyDMLStmt::select_exprs_to_sql_string(common::ObSqlString& sql_string)
{
  int ret = OB_SUCCESS;
  if (select_exprs_.count() < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid select_exprs_ count", K(select_exprs_.count()), K(sql_string_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs_.count(); i++) {
      if (OB_FAIL(select_exprs_.at(i)->to_sql_string(sql_string))) {
        LOG_WDIAG("to sql_string failed", K(i), K(ret), K(sql_string_));
      } else if (i >= select_exprs_.count() - 1) {
        // no need add , do nothing
      } else if (OB_FAIL(sql_string.append(", "))) {
        LOG_WDIAG("fail to append", K(i), K(ret), K(sql_string_));
      }
    }
  }
  return ret;
}

int ObProxyDMLStmt::table_exprs_to_sql_string(common::ObSqlString& sql_string)
{
  UNUSED(sql_string);
  int ret = OB_SUCCESS;
  return ret;
}

int ObProxyDMLStmt::group_by_exprs_to_sql_string(common::ObSqlString& sql_string)
{
  int ret = OB_SUCCESS;
  if (group_by_exprs_.count() <= 0) {
    // do nothing
  } else if (OB_FAIL(sql_string.append(" GROUP BY "))) {
    LOG_WDIAG("fail to append", K(ret), K(sql_string_));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < group_by_exprs_.count(); i++) {
      if (OB_FAIL(group_by_exprs_.at(i)->to_sql_string(sql_string))) {
        LOG_WDIAG("to sql_string failed", K(i), K(ret), K(sql_string_));
      } else if (i >= group_by_exprs_.count() - 1) {
        // no need add , do nothing
      } else if (OB_FAIL(sql_string.append(", "))) {
        LOG_WDIAG("append failed", K(i), K(ret), K(sql_string_));
      }
    }
    if (OB_SUCC(ret) && has_rollup_) {
      if (OB_FAIL(sql_string.append(" WITH ROLLUP"))) {
        LOG_WDIAG("append failed", K(ret), K(sql_string_));
      }
    }
  }
  return ret;
}

int ObProxyDMLStmt::order_by_exprs_to_sql_string(common::ObSqlString& sql_string)
{
  int ret = OB_SUCCESS;
  if (order_by_exprs_.count() <= 0) {
    // do nothing
  } else if (OB_FAIL(sql_string.append(" ORDER BY "))) {
    LOG_WDIAG("fail to append", K(ret), K(sql_string_));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < order_by_exprs_.count(); i++) {
      if (OB_FAIL(order_by_exprs_.at(i)->to_sql_string(sql_string))) {
        LOG_WDIAG("to sql_string failed", K(i), K(ret), K(sql_string_));
      } else if (i >= order_by_exprs_.count() - 1) {
        // no need add , do nothing
      } else if (OB_FAIL(sql_string.append(", "))) {
        LOG_WDIAG("append failed", K(i), K(ret), K(sql_string_));
      }
    }
  }
  return ret;
}

//REFER:https://dev.mysql.com/doc/refman/8.0/en/select.html
int ObProxyDMLStmt::to_sql_string(common::ObSqlString& sql_string)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(comments_to_sql_string(sql_string))) {
    LOG_WDIAG("comments_to_sql_string failed", K(ret), K(sql_string_));
  } else if (OB_FAIL(sql_string.append("SELECT "))) {
    LOG_WDIAG("fail to append", K(ret), K(sql_string_));
  } else if (OB_FAIL(hint_exprs_to_sql_string(sql_string))) {
    LOG_WDIAG("hint_exprs_to_sql_string failed", K(ret), K(sql_string_));
  } else if (OB_FAIL(select_exprs_to_sql_string(sql_string))) {
    LOG_WDIAG("select_exprs_to_sql_string failed", K(ret), K(sql_string_));
  } else if (OB_FAIL(table_exprs_to_sql_string(sql_string))) {
    LOG_WDIAG("table_exprs_to_sql_string failed", K(ret), K(sql_string_));
  } else if (OB_FAIL(condition_exprs_to_sql_string(sql_string))) {
    LOG_WDIAG("condition_exprs_to_sql_string faield", K(ret), K(sql_string_));
  } else if (OB_FAIL(group_by_exprs_to_sql_string(sql_string))) {
    LOG_WDIAG("group_by_exprs_to_sql_string failed", K(ret), K(sql_string_));
  } else if (OB_FAIL(order_by_exprs_to_sql_string(sql_string))) {
    LOG_WDIAG("order_by_exprs_to_sql_string failed", K(ret), K(sql_string_));
  } else if (OB_FAIL(limit_to_sql_string(sql_string))) {
    LOG_WDIAG("limit_to_sql_string", K(ret), K(sql_string_));
  } else {
    LOG_DEBUG("sql_string is", K(sql_string));
  }
  return ret;
}

int ObProxyDMLStmt::handle_union_clause(ParseNode* node)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  bool has_unsupport_expr_type = true;

  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    tmp_node = node->children_[i];
    if (NULL == tmp_node) {
      // do nothing
    } else {
      switch(tmp_node->type_) {
        case T_ALL:
          has_unsupport_expr_type = false;
          break;
        default:
          LOG_WDIAG("unsupport type", "node_type", get_type_name(tmp_node->type_), K(ret));
          break;
      }
    }
  }

  if (has_unsupport_expr_type) {
    has_unsupport_expr_type_ = true;
  }

  return ret;
}

int ObProxyDMLStmt::handle_hint_clause(ParseNode* node)
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
            LOG_WDIAG("fail to handle table and db in hint", K(ret));
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

int ObProxyDMLStmt::handle_limit_clause(ParseNode* node)
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
            LOG_WDIAG("get_int_value failed", K(ret), K(tmp_node->str_value_), K(sql_string_));
          } else {
            limit_offset_ = static_cast<int>(tmp_val);
          }
          break;
        case T_LIMIT_INT:
          if(OB_FAIL(get_int_value(tmp_node->str_value_, tmp_val))) {
            LOG_WDIAG("get_int_value failed", K(ret), K(tmp_node->str_value_), K(sql_string_));
          } else {
            limit_size_ = static_cast<int>(tmp_val);
          }
          break;
        default:
          has_unsupport_expr_type_ = true;
          LOG_DEBUG("unsupport type", "node_type", get_type_name(tmp_node->type_), K(sql_string_));
      }
    }
  }
  return ret;
}

int ObProxyDMLStmt::handle_project_list(ParseNode* node)
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
            LOG_WDIAG("project_string_to_expr failed", K(sql_string_), K(ret));
          } else if (OB_FAIL(select_exprs_.push_back(expr))) {
            LOG_WDIAG("push to array failed", K(sql_string_), K(ret));
          }
          break;
        default:
          has_unsupport_expr_type_ = true;
          LOG_DEBUG("unsupport type", "node_type", get_type_name(tmp_node->type_), K(sql_string_));
      }

      if (OB_FAIL(ret) && OB_NOT_NULL(expr)) {
        expr->~ObProxyExpr();
      }
    }
  }
  return ret;
}

int ObProxyDMLStmt::get_const_expr(ParseNode* node, ObProxyExpr* &expr)
{
  int ret = OB_SUCCESS;
  ObProxyExprConst* expr_const = NULL;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("unexpected null", K(sql_string_), K(ret));
  } else if (OB_FAIL(get_expr_by_type(expr, OB_PROXY_EXPR_TYPE_CONST))) {
    LOG_WDIAG("get_expr_by_type failed", K(ret), K(sql_string_));
  } else if (OB_ISNULL(expr_const = dynamic_cast<ObProxyExprConst*>(expr))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("dynamic_cast failed", K(ret));
  } else {
    ObObj obj;
    if (node->type_ == T_INT) {
      int64_t value = 0;
      if (OB_FAIL(get_int_value(node->str_value_, value, 10))) {
        LOG_WDIAG("fail to int value", K(ret), K(node->str_value_), K(sql_string_));
      } else {
        obj.set_int(value);
        expr_const->set_object(obj);
        LOG_DEBUG("add int value", K(obj));
      }
    } else {
      ObString var(static_cast<int32_t>(node->str_len_), node->str_value_);
      obj.set_varchar(var);
      expr_const->set_object(obj);
      LOG_DEBUG("add varchar value", K(obj));
    }
  }
  return ret;
}

int ObProxyDMLStmt::get_sharding_const_expr(ParseNode* node, ObProxyExpr* &expr)
{
  int ret = OB_SUCCESS;
  ObProxyExprShardingConst* expr_const = NULL;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("unexpected null", K(sql_string_), K(ret));
  } else if (OB_FAIL(get_expr_by_type(expr, OB_PROXY_EXPR_TYPE_SHARDING_CONST))) {
    LOG_WDIAG("get_expr_by_type failed", K(ret), K(sql_string_));
  } else if (OB_ISNULL(expr_const = dynamic_cast<ObProxyExprShardingConst*>(expr))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("dynamic_cast failed", K(ret));
  } else {
    expr_const->set_expr_name(node->str_value_, static_cast<int32_t>(node->str_len_));
  }
  return ret;
}

int ObProxyDMLStmt::handle_table_and_db_in_hint(ParseNode* node)
{
  int ret = OB_SUCCESS;

  ObProxyExprTable* expr_table = NULL;

  if (OB_T_RELATION_FACTOR_NUM_CHILD /* 2 */ != node->num_child_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("children node is not 2", "child num", node->num_child_, K(ret));
  } else if (OB_FAIL(handle_table_and_db_node(node, expr_table))) {
    LOG_WDIAG("fail to handle table and db node", K(ret));
  }

  return ret;
}


int ObProxyDMLStmt::alias_node_to_expr(ParseNode* node, ObProxyExpr* &expr, ParseNode* string_node)
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
            LOG_WDIAG("fail to get expr table", K(ret));
          } else if (OB_UNLIKELY(NULL == tmp_node->str_value_ || 0 >= tmp_node->str_len_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("alias token meet some wrong", "str len", tmp_node->str_len_, K(ret));
          } else {
            expr->set_alias_name(tmp_node->str_value_, static_cast<int32_t>(tmp_node->str_len_));
          }
          break;
        default:
          if (OB_FAIL(string_node_to_expr(tmp_node, expr, string_node))) {
            LOG_WDIAG("string_node_to_expr failed", K(i), K(ret));
          }
      }
    }
  }
  return ret;
}

int ObProxyDMLStmt::func_node_to_expr(ParseNode* node, ObProxyExpr* &expr)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  ObProxyExpr* tmp_expr = NULL;
  ObProxyFuncExpr* func_expr = NULL;
  ObProxyExprType expr_type = get_expr_type_by_node_type(node->type_);
  if (OB_FAIL(get_expr_by_type(expr, expr_type))) {
    LOG_WDIAG("get_expr_by_type failed", K(ret));
  } else if (OB_ISNULL(func_expr = dynamic_cast<ObProxyFuncExpr*>(expr))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("dynamic_cast failed", K(ret));
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
      // There are T_ALL nodes under the COUNT function, do not need to be processed, do nothing
    } else if (OB_FAIL(string_node_to_expr(tmp_node, tmp_expr))){
      LOG_WDIAG("string_node_to_expr failed", K(ret));
    } else if (NULL == tmp_expr) {
      // Argument in func is of unsupported type
      LOG_WDIAG("tmp_expr is empty", "type", tmp_node->type_, K(ret));
    } else if (OB_FAIL(func_expr->add_param_expr(tmp_expr))) {
      LOG_WDIAG("add_param_expr failed", K(ret), K(sql_string_));
    } else {
      if (tmp_expr->has_agg_) {
        func_expr->has_agg_ = 1;
      }
    }
  }

  if (OB_SUCC(ret)) {
    func_expr->set_expr_name(node->str_value_, static_cast<int32_t>(node->str_len_));
  }

  return ret;
}
int ObProxyDMLStmt::check_node_has_agg(ParseNode* node)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected null", K(sql_string_));
  }
  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
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
          has_unsupport_expr_type_ = true;
          LOG_DEBUG("node has agg", "node_type", get_type_name(tmp_node->type_), K(ret), K(sql_string_));
          break;
        default:
          if (OB_FAIL(check_node_has_agg(tmp_node))) {
            LOG_WDIAG("check_node_has_agg failed", K(ret));
          }
          break;
      }
    }
  }
  return ret;
}
//we do not cover all sys func. if sys func do not have agg func, pass it to server as string
int ObProxyDMLStmt::func_sys_node_to_expr(ParseNode* node, ObProxyExpr* &expr,  ParseNode* string_node)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_node_has_agg(node))) {
    LOG_WDIAG("check_node_has_agg failed", K(ret));
  } else if (string_node == NULL) {
    if (OB_FAIL(get_sharding_const_expr(node, expr))) {
      LOG_WDIAG("get_sharding_const_expr faield", K(ret));
    }
  } else if (OB_FAIL(get_sharding_const_expr(string_node, expr))) {
    LOG_WDIAG("get_sharding_const_expr faield", K(ret));
  }
  return ret;
}

int ObProxyDMLStmt::string_node_to_expr(ParseNode* node, ObProxyExpr* &expr,  ParseNode* string_node)
{
  int ret = OB_SUCCESS;
  int i = 0;
  switch(node->type_) {
    case T_STAR:
      if (OB_FAIL(get_expr_by_type(expr, OB_PROXY_EXPR_TYPE_STAR))) {
        LOG_WDIAG("get_expr_by_type failed", K(ret), K(sql_string_));
      }
      break;
    case T_COLUMN_REF: {
      ObProxyExprTable* expr_table = NULL;
      if (OB_FAIL(column_ref_to_expr(node, expr, expr_table))) {
        LOG_WDIAG("fail to column ref to expr", K(ret));
      }
      break;
    }
    case T_ALL:
      break;
    case T_ALIAS:
      if (OB_FAIL(alias_node_to_expr(node, expr, string_node))) {
        LOG_WDIAG("fail to alias node to expr", K(ret));
      }
      break;
    case T_FUN_SYS: // not support fun sys, as string_node
      if (OB_FAIL(func_sys_node_to_expr(node, expr, string_node))) {
        LOG_WDIAG("fail to func sys node to expr", K(ret));
      }
      break;
    case T_IDENT:
    case T_INT:
      if (OB_FAIL(get_const_expr(node, expr))) {
        LOG_WDIAG("fail to get sharding const expr", K(ret));
      }
      break;
    case T_VARCHAR:
      //varchar has varchar child, if child is 0 not have child
      if (node->num_child_ == 0) {
        if (OB_FAIL(get_const_expr(node, expr))) {
          LOG_WDIAG("fail to get sharding const expr succ", K(ret));
        }
      } else {
        for (i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
          if (node->children_[i] == NULL) {
            //do nothing
          } else if (expr != NULL) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("expr should null", K(ret), K(i), K(node->str_value_), K(sql_string_));
          } else  if (OB_FAIL(get_const_expr(node->children_[i], expr))) {
            LOG_WDIAG("fail to get sharding const expr succ", K(ret));
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
        LOG_WDIAG("fail to add func node expr succ", K(ret));
      }
      break;
    default:
      //Does not support type detection whether there is agg,
      //if not, use the string of string_node to construct transparent transmission
      if (OB_FAIL(check_node_has_agg(node))) {
        LOG_WDIAG("unsupport type", "node_type", get_type_name(node->type_), K(node->str_value_));
      } else if (string_node == NULL) {
        has_unsupport_expr_type_ = true;
        LOG_DEBUG("unsupport type", "node_type", get_type_name(node->type_), K(node->str_value_), K(sql_string_));
      } else if (OB_FAIL(get_sharding_const_expr(string_node, expr))) {
        LOG_WDIAG("get_sharding_const_expr failed", K(ret));
      }
  }
  return ret;
}
int ObProxyDMLStmt::project_string_to_expr(ParseNode* node, ObProxyExpr* &expr)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    tmp_node = node->children_[i];
    if (NULL == tmp_node) {
      // do nothing
    } else if (expr != NULL) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("expr not null", K(node->str_value_), K(node->num_child_), K(sql_string_), K(ret));
    } else if (OB_FAIL(string_node_to_expr(tmp_node, expr, node))) {
      LOG_WDIAG("string_node_to_expr failed", K(sql_string_), K(ret));
    }
  }
  return ret;
}


int ObProxyDMLStmt::handle_sort_key_node(ParseNode* node, ObProxyExpr* &expr, const SortListType& sort_list_type)
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
        if (OB_FAIL(string_node_to_expr(tmp_node, tmp_expr, tmp_node))) {
          LOG_WDIAG("string_node_to_expr failed", K(ret), K(sql_string_));
        } else if (T_INT == tmp_node->type_) {
          ObProxyExprConst* expr_const = NULL;
          if (OB_ISNULL(expr_const = dynamic_cast<ObProxyExprConst*>(tmp_expr))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("dynamic_cast failed", K(ret));
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
      LOG_WDIAG("get_expr_by_type", K(expr_type), K(sql_string_), K(ret));
    } else if (OB_ISNULL(group_item_expr = dynamic_cast<ObProxyGroupItem*>(expr))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("dynamic_cast to ObProxyGroupItem failed", K(ret));
    } else {
      if (tmp_expr->has_agg_) {
        group_item_expr->has_agg_ = 1;
      }
      group_item_expr->set_expr(tmp_expr);
      if (sort_list_type == SORT_LSIT_IN_ORDER_BY) {
        ObProxyOrderItem* order_item_expr = NULL;
        if (OB_ISNULL(order_item_expr = dynamic_cast<ObProxyOrderItem*>(expr))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("dynamic_cast to ObProxyOrderItem failed", K(ret));
        } else {
          order_item_expr->order_direction_ = order_direction;
        }
      }
    }
  }
  return ret;
}
int ObProxyDMLStmt::handle_sort_list_node(ParseNode* node, const SortListType& sort_list_type)
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
            LOG_WDIAG("handle_sort_key_node", K(sql_string_), K(ret));
          } else {
            switch(sort_list_type) {
              case SORT_LIST_IN_GROUP_BY:
                if (OB_ISNULL(group_item_expr = dynamic_cast<ObProxyGroupItem*>(expr))) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WDIAG("dynamic_cast to ObProxyGroupItem failed", K(ret));
                } else if (OB_FAIL(group_by_exprs_.push_back(group_item_expr))) {
                  LOG_WDIAG("push_back failed", K(sql_string_), K(ret));
                }
                break;
              case SORT_LSIT_IN_ORDER_BY:
                if (OB_ISNULL(order_item_expr = dynamic_cast<ObProxyOrderItem*>(expr))) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WDIAG("dynamic_cast to ObProxyOrderItem failed", K(ret));
                } else if (OB_FAIL(order_by_exprs_.push_back(order_item_expr))) {
                  LOG_WDIAG("push_back failed", K(sql_string_), K(ret));
                }
                break;
              default:
                has_unsupport_expr_type_ = true;
                LOG_DEBUG("invalid sort_list_type", K(sort_list_type), K(sql_string_), K(ret));
            }
          }
          break;
        default:
          has_unsupport_expr_type_ = true;
          LOG_DEBUG("unsupport expr type", "node_type", get_type_name(tmp_node->type_), K(sql_string_), K(ret));
      }
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(expr)) {
    expr->~ObProxyExpr();
  }
  return ret;
}
int ObProxyDMLStmt::handle_with_rollup_in_groupby(ParseNode* node)
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
          LOG_WDIAG("handle_sort_list_node failed", K(ret), K(sql_string_));
        }
        break;
      default:
        has_unsupport_expr_type_ = true;
        LOG_DEBUG("unsupport expr type", "node_type", get_type_name(tmp_node->type_), K(sql_string_));
      }
    }
  }
  return ret;
}

int ObProxyDMLStmt::handle_groupby_clause(ParseNode* node)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    tmp_node = node->children_[i];
    if (NULL == tmp_node) {
      //do nothing
    } else if (T_WITH_ROLLUP_CLAUSE != tmp_node->type_) {
      has_unsupport_expr_type_ = true;
      LOG_DEBUG("unsupport expr type", "node_type", get_type_name(tmp_node->type_), K(sql_string_));
    } else if (OB_FAIL(handle_with_rollup_in_groupby(tmp_node))) {
      LOG_WDIAG("handle_with_rollup_in_groupby failed", K(ret), K(i), K(sql_string_));
    }
  }

  return ret;
}

int ObProxyDMLStmt::handle_orderby_clause(ParseNode* node)
{
  int ret = OB_SUCCESS;
  ParseNode* tmp_node = NULL;
  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    tmp_node = node->children_[i];
    if (NULL == tmp_node) {
      //do nothing
    } else if (T_SORT_LIST != tmp_node->type_) {
      has_unsupport_expr_type_ = true;
      LOG_DEBUG("unsupport expr type", "node_type", get_type_name(tmp_node->type_), K(sql_string_));
    } else if (OB_FAIL(handle_sort_list_node(tmp_node, SORT_LSIT_IN_ORDER_BY))) {
      LOG_WDIAG("handle_sort_list_node", K(ret), K(i), K(sql_string_));
    }
  }

  return ret;
}

int ObProxySelectStmt::handle_parse_result(const ParseResult &parse_result)
{
  int ret = OB_SUCCESS;

  ParseNode* node = NULL;
  if (OB_FAIL(handle_explain_node(parse_result, node))) {
    LOG_WDIAG("fail to handle explain node", K(ret));
  } else if (OB_FAIL(do_handle_parse_result(node))) {
    LOG_WDIAG("fail to do handle parse result", K(sql_string_), "node_type", get_type_name(node->type_), K(ret));
  } else if (OB_FAIL(handle_comment_list(parse_result))) {
    LOG_WDIAG("handle_comment_list failed", K(ret), K(sql_string_));
  }

  return ret;
}

int ObProxyInsertStmt::handle_parse_result(const ParseResult &parse_result)
{
  int ret = OB_SUCCESS;

  ParseNode* node = NULL;
  if (OB_FAIL(handle_explain_node(parse_result, node))) {
    LOG_WDIAG("fail to handle explain node", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
      ParseNode* tmp_node = node->children_[i];
      if (NULL == tmp_node) {
        //do nothing
      } else {
        switch (tmp_node->type_) {
          case T_REPLACE:
            stmt_type_ = OBPROXY_T_REPLACE;
            break;
          case T_HINT_OPTION_LIST:
            ret = handle_hint_clause(tmp_node);
            break;
          case T_SINGLE_TABLE_INSERT:
            if (OB_FAIL(handle_single_table_insert(tmp_node))) {
              LOG_WDIAG("handle single table insert failed", K(ret));
            }
            break;
          case T_INSERT:
            //nothing
            break;
          default:
            has_unsupport_expr_type_ = true;
            LOG_DEBUG("unknown node type", K_(tmp_node->type), K(ret));
            break;
        }
      }
    }
  }

  if(OB_SUCC(ret)) {
    if (OB_FAIL(handle_comment_list(parse_result))) {
      LOG_WDIAG("handle_comment_list failed", K(ret), K(sql_string_));
    }
  }
  return ret;
}

int ObProxyInsertStmt::handle_single_table_insert(ParseNode *node)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    ParseNode* tmp_node = node->children_[i];
    if (NULL == tmp_node) {
      ///do nothing
    } else {
      switch (tmp_node->type_) {
        case T_INSERT_INTO_CLAUSE:
          if (OB_FAIL(handle_insert_into(tmp_node))) {
            LOG_WDIAG("fail to handle insert into", K(ret));
          }
          break;
        case T_VALUE_LIST:
          if (OB_FAIL(handle_value_list(tmp_node))) {
            LOG_WDIAG("fail to handle value list", K(ret));
          }
          break;
        case T_ASSIGN_LIST:
          if (OB_FAIL(handle_assign_list(tmp_node))) {
            LOG_WDIAG("fail to handle assign list", K(ret));
          }
          break;
        case T_SELECT:
          has_unsupport_expr_type_for_config_ = true;
          has_sub_select_ = true;
          if (OB_FAIL(do_handle_parse_result(tmp_node))) {
            LOG_WDIAG("fail to do handle parse result", "node_type", get_type_name(tmp_node->type_), K(ret));
          }
          break;
        default:
          has_unsupport_expr_type_ = true;
          LOG_DEBUG("unknown node type", K_(tmp_node->type), K(ret));
      }
    }
  }

  return ret;
}

int ObProxyInsertStmt::handle_insert_into(ParseNode *node)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    ParseNode* tmp_node = node->children_[i];
    if (NULL == tmp_node) {
      //do nothing
    } else {
      switch (tmp_node->type_) {
        case T_ORG:
          if (OB_FAIL(handle_table_node_to_expr(tmp_node))) {
            LOG_WDIAG("handle org failed", K(ret));
          }
          break;
        case T_COLUMN_LIST:
          if (OB_FAIL(handle_column_list(tmp_node))) {
            LOG_WDIAG("handle column list failed", K(ret));
          }
          break;
        default:
          has_unsupport_expr_type_ = true;
          LOG_DEBUG("unknown node type", K_(tmp_node->type), K(ret));
          break;
      }
    }
  }

  return ret;
}

int ObProxyInsertStmt::handle_value_list(ParseNode *node)
{
  int ret = OB_SUCCESS;
  row_count_ = node->num_child_;
  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    ParseNode *tmp_node = node->children_[i];
    if (NULL == tmp_node) {
      //do nothing
    } else {
      switch (tmp_node->type_) {
        case T_VALUE_VECTOR:
          if (OB_FAIL(handle_value_vector(tmp_node))) {
            LOG_WDIAG("handle value vector failed", K(ret));
          }
          break;
        default:
          has_unsupport_expr_type_ = true;
          LOG_DEBUG("unknown node type", K_(tmp_node->type), K(ret));
          break;
      }
    }
  }

  return ret;
}

int ObProxyInsertStmt::handle_column_list(ParseNode *node)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    ParseNode *tmp_node = node->children_[i];
    if (NULL == tmp_node) {
      //do nothing
    } else {
      switch (tmp_node->type_) {
        case T_COLUMN_REF:
          if (OB_FAIL(handle_column_ref_in_list(tmp_node))) {
            LOG_WDIAG("handle column ref failed", K(ret));
          }
          break;
        default:
          has_unsupport_expr_type_ = true;
          LOG_DEBUG("unknown ndoe type", K_(tmp_node->type), K(ret));
      }
    }
  }
  return ret;
}

int ObProxyInsertStmt::handle_value_vector(ParseNode *node)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(use_column_value_from_hint_
                  || field_results_->fields_.empty())) {
    ret = OB_SUCCESS;
    LOG_DEBUG("use hint fields or no column name, skip fields for insert/replace", K(use_column_value_from_hint_));
  } else if (OB_UNLIKELY(node->num_child_ != field_results_->fields_.count())){
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WDIAG("the num of column name do not match value vector", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
      ParseNode *tmp_node = node->children_[i];
      if (NULL == tmp_node) {
        // do nothing
      } else if (OB_ISNULL(field_results_->fields_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("null sql field", K(i), K(ret));
      } else {
        SqlField& sql_field = *(field_results_->fields_.at(i));
        SqlColumnValue column_value;
        switch (tmp_node->type_) {
          case T_INT:
            {
              sql_field.value_type_ = TOKEN_INT_VAL;
              sql_field.column_int_value_ = tmp_node->value_;
              column_value.value_type_ = TOKEN_STR_VAL;
              column_value.column_value_.set_value(tmp_node->str_value_);
              ret = sql_field.column_values_.push_back(column_value);
              break;
            }
          case T_VARCHAR:
            {
              ret = handle_varchar_node_in_column_value(tmp_node, sql_field);
              break;
            }
          case T_OP_NEG:
            {
              ParseNode *child_node = tmp_node->children_[0];
              if (child_node->type_ == T_INT) {
                sql_field.value_type_ = TOKEN_INT_VAL;
                sql_field.column_int_value_ = -1 * (child_node->value_);
                column_value.value_type_ = TOKEN_INT_VAL;
                column_value.column_int_value_ = -1 * (child_node->value_);
                ret = sql_field.column_values_.push_back(column_value);
              } else {
                has_unsupport_expr_type_for_config_ = true;
                LOG_DEBUG("unknown node type fro config", K_(child_node->type), K(ret));
              }
              break;
            }
          case T_NULL:
          {
            column_value.value_type_ = TOKEN_NULL;
            ret = sql_field.column_values_.push_back(column_value);
            has_unsupport_expr_type_for_config_ = true;
            break;
          }
          default:
            column_value.value_type_ = TOKEN_NONE;
            ret = sql_field.column_values_.push_back(column_value);
            has_unsupport_expr_type_ = true;
            LOG_DEBUG("unknown node type", "node type", tmp_node->type_, K(ret));
            break;
        }
      }
    }
  }

  return ret;
}

int ObProxyDeleteStmt::handle_parse_result(const ParseResult &parse_result)
{
  int ret = OB_SUCCESS;

  ParseNode* node = NULL;
  if (OB_FAIL(handle_explain_node(parse_result, node))) {
    LOG_WDIAG("fail to handle explain node", K(ret));
  } else {
    if (node->type_ == T_DELETE) {
      stmt_type_ = OBPROXY_T_DELETE;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unknown node type", K(node->type_), K(ret));
    }

    for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
      ParseNode* tmp_node = node->children_[i];
      if (NULL == tmp_node) {
        //do nothing
      } else {
        switch (tmp_node->type_) {
          case T_DELETE_TABLE_NODE:
            if (OB_FAIL(handle_delete_table_node(tmp_node))) {
              LOG_WDIAG("handle delete table node failed", K(ret));
            }
            break;
          case T_HINT_OPTION_LIST:
            ret = handle_hint_clause(tmp_node);
            break;
          case T_WHERE_CLAUSE:
            if (OB_FAIL(handle_where_clause(tmp_node))) {
              LOG_WDIAG("handle where clause failed", K(ret));
            }
            break;
          default:
            has_unsupport_expr_type_ = true;
            LOG_DEBUG("unknown node type", K_(tmp_node->type), K(ret));
            break;
        }
      }
    }
  }

  return ret;
}

int ObProxyDeleteStmt::handle_delete_table_node(ParseNode *node)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
    ParseNode *tmp_node = node->children_[i];
    if (NULL == tmp_node) {
      //do nothing
    } else {
      switch (tmp_node->type_) {
        case T_TABLE_REFERENCES:
          if (OB_FAIL(handle_table_references(tmp_node))) {
            LOG_WDIAG("handle table references failed", K(ret));
          }
          break;
        default:
          has_unsupport_expr_type_ = true;
          LOG_DEBUG("unknown node type", K_(tmp_node->type), K(ret));
      }
    }
  }
  return ret;
}

int ObProxyUpdateStmt::handle_parse_result(const ParseResult &parse_result)
{
  int ret = OB_SUCCESS;

  ParseNode* node = NULL;
  if (OB_FAIL(handle_explain_node(parse_result, node))) {
    LOG_WDIAG("fail to handle explain node", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
      ParseNode* tmp_node = node->children_[i];
      if (NULL == tmp_node) {
        //do nothing
      } else {
        switch (tmp_node->type_) {
          case T_TABLE_REFERENCES:
            if (OB_FAIL(handle_table_references(tmp_node))) {
              LOG_WDIAG("handle where clause failed", K(ret));
            }
            break;
          case T_HINT_OPTION_LIST:
            ret = handle_hint_clause(tmp_node);
            break;
          case T_ASSIGN_LIST:
            if (OB_FAIL(handle_assign_list(tmp_node))) {
              LOG_WDIAG("handle where clause failed", K(ret));
            }
            break;
          case T_WHERE_CLAUSE:
            if (OB_FAIL(handle_where_clause(tmp_node))) {
              LOG_WDIAG("handle where clause failed", K(ret));
            }
            break;
          default:
            has_unsupport_expr_type_ = true;
            LOG_DEBUG("unknown node type", K_(tmp_node->type), K(ret));
            break;
        }
      }
    }
  }
  return ret;
}


} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
