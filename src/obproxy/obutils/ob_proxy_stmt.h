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
  ObProxyDMLStmt(): limit_start_(0), limit_offset_(-1), field_results_(NULL) {}
  virtual ~ObProxyDMLStmt() {}
protected:
  int condition_exprs_to_sql_string(common::ObSqlString& sql_string);
  int limit_to_sql_string(common::ObSqlString& sql_string);
public:
  common::ObSEArray<opsql::ObProxyExpr*, 4> condition_exprs_; //where condition expr
  int limit_start_;
  int limit_offset_;
  SqlFieldResult* field_results_; // pointer to parseResult
  common::ObString hint_string_;
  common::ObSEArray<common::ObString,4> comments_;
};

enum SortListType {
  SORT_LIST_NONE = 0,
  SORT_LIST_IN_GROUP_BY,
  SORT_LSIT_IN_ORDER_BY,
  SORT_LIST_TYPE_MAX,
};
enum ParsePhase {
  SELECT_FIELD_PHASE = 0,
  FROM_TABLE_PHASE = 1,
  WHERE_CONDTION_PHASE = 2,
  GROUP_BY_PHASE = 3,
  ORDER_BY_PHASE = 4,
  PHASE_MAX,
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

  int column_ref_to_expr(ParseNode* node, ObProxyExpr* &expr);
  int get_sharding_const_expr(ParseNode* node, ObProxyExpr* &expr, bool is_column_ref = true);
  int get_expr_by_type(ObProxyExpr* &expr, ObProxyExprType type);

  ObProxyExprType get_expr_type_by_node_type(const ObItemType& item_type);

  int org_node_to_expr(ParseNode* node, ObProxyExpr* &expr);
  //for where
  int handle_where_nodes(ParseNode* node);
  int handle_where_condition_node(ParseNode* node);
  int handle_where_eq_node(ParseNode* node);
  int handle_where_in_node(ParseNode* node);
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
  int handle_where_end_pos(ParseNode* node);
  int check_node_has_agg(ParseNode* node);
  int where_end_pos_;
public:
  common::ObSEArray<opsql::ObProxyExpr*, 4> select_exprs_;
  common::ObSEArray<opsql::ObProxyExpr*, 4> table_exprs_;
  common::ObSEArray<opsql::ObProxyExpr*, 4> group_by_exprs_;
  common::ObSEArray<opsql::ObProxyOrderItem*, 4> order_by_exprs_;
private:
  bool is_inited_;
  ParsePhase parse_phase_;
  bool has_rollup_;
  typedef common::hash::ObHashMap<common::ObString, opsql::ObProxyExpr *> AliasExprMap;
  AliasExprMap alias_col_map_;
};
} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
#endif //
