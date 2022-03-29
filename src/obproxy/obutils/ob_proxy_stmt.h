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

#ifndef OBPROXY_STMT_H
#define OBPROXY_STMT_H
#include "lib/ob_define.h"
#include "opsql/parser/ob_proxy_parse_result.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/string/ob_string.h"
#include "lib/string/ob_sql_string.h"
#include "lib/container/ob_se_array.h"
#include "opsql/func_expr_resolver/proxy_expr/ob_proxy_expr.h"

namespace oceanbase
{
namespace common {
class ObIAllocator;
}
namespace obproxy
{
namespace obutils
{

class ObProxyExprTablePos {
public:
  ObProxyExprTablePos() : database_pos_(-1), table_pos_(-1), table_expr_(NULL) {}
  virtual ~ObProxyExprTablePos() {}

  void set_database_pos(const int32_t database_pos) { database_pos_ = database_pos; }
  int32_t get_database_pos() { return database_pos_; }
  void set_table_pos(const int32_t table_pos) { table_pos_ = table_pos; }
  int32_t get_table_pos() { return table_pos_; }
  void set_table_expr(ObProxyExprTable* table_expr) { table_expr_ = table_expr; }
  ObProxyExprTable *get_table_expr() { return table_expr_; }

  bool operator<(ObProxyExprTablePos &expr_table_pos)
  {
    return table_pos_ < expr_table_pos.get_table_pos();
  }

  TO_STRING_KV("database_pos", database_pos_,
               "table_pos", table_pos_);

private:
  int32_t database_pos_;
  int32_t table_pos_;
  ObProxyExprTable* table_expr_;
};

class ObProxyStmt {
public:
  ObProxyStmt() : stmt_type_(OBPROXY_T_INVALID), allocator_(NULL) {}
  virtual ~ObProxyStmt() {}
  virtual int handle_parse_result(const ParseResult &parse_result) = 0;
  void set_stmt_type(ObProxyBasicStmtType stmt_type) { stmt_type_ = stmt_type; }
  void set_allocator(common::ObIAllocator* allocator) { allocator_ = allocator; }
  void set_sql_string(const common::ObString& sql_string) { sql_string_ = sql_string; }
  ObProxyBasicStmtType get_stmt_type() { return stmt_type_; }
  virtual int to_sql_string(common::ObSqlString& sql_string) = 0;
protected:
  ObProxyBasicStmtType stmt_type_;
  common::ObIAllocator* allocator_;
  common::ObString sql_string_;
};

class ObProxyDMLStmt : public ObProxyStmt
{
public:
  ObProxyDMLStmt(): limit_offset_(0), limit_size_(-1), limit_token_off_(-1), field_results_(NULL) {}
  virtual ~ObProxyDMLStmt() {}
protected:
  int condition_exprs_to_sql_string(common::ObSqlString& sql_string);
  int limit_to_sql_string(common::ObSqlString& sql_string);
public:
  int limit_offset_;
  int limit_size_;
  int64_t limit_token_off_;
  SqlFieldResult* field_results_; // pointer to parseResult
  common::ObSEArray<common::ObString,4> comments_;
};

enum SortListType {
  SORT_LIST_NONE = 0,
  SORT_LIST_IN_GROUP_BY,
  SORT_LSIT_IN_ORDER_BY,
  SORT_LIST_TYPE_MAX,
};

class ObProxySelectStmt : public ObProxyDMLStmt
{
public:
  ObProxySelectStmt();
  virtual ~ObProxySelectStmt();
  int init();
  virtual int handle_parse_result(const ParseResult &parse_result);
  int handle_project_list(ParseNode* node);
  int handle_project_string(ParseNode* node);
  int handle_from_list(ParseNode* node);
  int handle_hint_clause(ParseNode* node);
  int handle_where_clause(ParseNode* node);
  int handle_groupby_clause(ParseNode* node);
  int handle_orderby_clause(ParseNode* node);
  int handle_limit_clause(ParseNode* node);
  int handle_comment_list(const ParseResult &parse_result);
  virtual int to_sql_string(common::ObSqlString& sql_string);
  bool has_for_update() const { return has_for_update_; }
  void set_table_name(const common::ObString& table_name) { table_name_ = table_name; }
  int64_t get_from_token_off() { return from_token_off_; }

public:
  typedef common::hash::ObHashMap<common::ObString, opsql::ObProxyExpr *> ExprMap;
  typedef common::ObSEArray<ObProxyExprTablePos, 4> TablePosArray;

public:
  ExprMap& get_table_exprs_map() { return table_exprs_map_; }
  TablePosArray& get_table_pos_array() { return table_pos_array_; }

private:
  int project_string_to_expr(ParseNode* node, ObProxyExpr* &expr);
  int string_node_to_expr(ParseNode* node, ObProxyExpr* &expr,  ParseNode* string_node = NULL);
  int alias_node_to_expr(ParseNode* node, ObProxyExpr* &expr, ParseNode* string_node = NULL);
  //used for func
  int func_node_to_expr(ParseNode* node, ObProxyExpr* &expr);
  int func_sys_node_to_expr(ParseNode* node, ObProxyExpr* &expr,  ParseNode* string_node);
  int sum_node_to_expr(ParseNode* node, ObProxyExpr* &expr);
  int count_node_to_expr(ParseNode* node, ObProxyExpr* &expr);
  int avg_node_to_expr(ParseNode* node, ObProxyExpr* &expr);
  int max_node_to_expr(ParseNode* node, ObProxyExpr* &expr);
  int min_node_to_expr(ParseNode* node, ObProxyExpr* &expr);

  int column_ref_to_expr(ParseNode* node, ObProxyExpr* &expr, ObProxyExprTable* &expr_table);
  int get_const_expr(ParseNode* node, ObProxyExpr* &expr);
  int get_sharding_const_expr(ParseNode* node, ObProxyExpr* &expr);
  int get_expr_by_type(ObProxyExpr* &expr, ObProxyExprType type);

  ObProxyExprType get_expr_type_by_node_type(const ObItemType& item_type);

  int get_table_and_db_expr(ParseNode* node, ObProxyExprTable* &expr_table);
  int handle_table_node_to_expr(ParseNode* node);
  int handle_table_and_db_node(ParseNode* node, ObProxyExprTable* &expr_table);
  int handle_table_and_db_in_hint(ParseNode* node);
  //for where
  int handle_where_node(ParseNode* node);
  int handle_where_expr_list_node(ParseNode* node, SqlField& sql_field);
  int handle_varchar_node_in_where_condition(ParseNode* node, SqlField& sql_field);
  //for group by
  int handle_with_rollup_in_groupby(ParseNode* node);
  int handle_sort_list_node(ParseNode* node, const SortListType& sort_list_type);
  int handle_sort_key_node(ParseNode* node, ObProxyExpr* &expr, const SortListType& sort_list_type);

private:
  int comments_to_sql_string(common::ObSqlString& sql_string);
  int hint_exprs_to_sql_string(common::ObSqlString& sql_string);
  int select_exprs_to_sql_string(common::ObSqlString& sql_string);
  int table_exprs_to_sql_string(common::ObSqlString& sql_string);
  int group_by_exprs_to_sql_string(common::ObSqlString& sql_string);
  int order_by_exprs_to_sql_string(common::ObSqlString& sql_string);
private:
  int check_node_has_agg(ParseNode* node);
public:
  common::ObSEArray<opsql::ObProxyExpr*, 4> select_exprs_;
  common::ObSEArray<opsql::ObProxyGroupItem*, 4> group_by_exprs_; //select groupby expression
  common::ObSEArray<opsql::ObProxyOrderItem*, 4> order_by_exprs_;
private:
  common::ObString table_name_;
  bool is_inited_;
  bool has_rollup_;
  bool has_for_update_;
  int64_t from_token_off_;
  ExprMap table_exprs_map_;
  ExprMap alias_table_map_;
  common::ObSEArray<ObProxyExprTablePos, 4> table_pos_array_;
};
} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
#endif //
