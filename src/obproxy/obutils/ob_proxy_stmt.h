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

enum ShardingPosType {
  SHARDING_POS_NONE = 0,
  SHARDING_POS_DB,
  SHARDING_POS_TABLE,
};

class ObProxyDbTablePos {
public:
  ObProxyDbTablePos() : pos_(-1), name_(), type_(SHARDING_POS_NONE) {}
  virtual ~ObProxyDbTablePos() {}

  void set_pos(const int32_t pos) { pos_ = pos; }
  int32_t get_pos() { return pos_; }

  void set_type(const ShardingPosType type) { type_ = type; }
  ShardingPosType get_type() { return type_; }

  void set_name(const ObString &name) { name_ = name; }
  ObString &get_name() { return name_; }

  bool operator<(ObProxyDbTablePos &db_table_pos)
  {
    return pos_ < db_table_pos.get_pos();
  }

  TO_STRING_KV(K_(pos), K_(name), K_(type));

private:
  int32_t pos_;
  ObString name_;
  ShardingPosType type_;
};

class ObProxyStmt {
public:
  ObProxyStmt(common::ObIAllocator& allocator) : stmt_type_(OBPROXY_T_INVALID), allocator_(allocator) {}
  virtual ~ObProxyStmt() {}
  virtual int handle_parse_result(const ParseResult &parse_result) = 0;
  void set_stmt_type(ObProxyBasicStmtType stmt_type) { stmt_type_ = stmt_type; }
  void set_sql_string(const common::ObString& sql_string) { sql_string_ = sql_string; }
  ObProxyBasicStmtType get_stmt_type() { return stmt_type_; }
  virtual int to_sql_string(common::ObSqlString& sql_string) = 0;
protected:
  ObProxyBasicStmtType stmt_type_;
  common::ObIAllocator& allocator_;
  common::ObString sql_string_;
};

enum SortListType {
  SORT_LIST_NONE = 0,
  SORT_LIST_IN_GROUP_BY,
  SORT_LSIT_IN_ORDER_BY,
  SORT_LIST_TYPE_MAX,
};

class ObProxyDMLStmt : public ObProxyStmt
{
public:
  typedef common::hash::ObHashMap<common::ObString, opsql::ObProxyExpr *, common::hash::NoPthreadDefendMode> ExprMap;
  typedef common::ObSEArray<ObProxyExprTablePos, 4> TablePosArray;
  typedef common::ObSEArray<ObProxyDbTablePos, 4> DbTablePosArray;

public:
  ObProxyDMLStmt(common::ObIAllocator& allocator);
  virtual ~ObProxyDMLStmt();
  int init();
  virtual int handle_parse_result(const ParseResult &parse_result)
  {
    UNUSED(parse_result);
    return common::OB_SUCCESS;
  }
  void set_table_name(const common::ObString& table_name) { table_name_ = table_name; }
  void set_field_results(SqlFieldResult* real_field_results_ptr) { field_results_ = real_field_results_ptr; }
  void set_use_column_value_from_hint(bool use_column_value_from_hint) { use_column_value_from_hint_ = use_column_value_from_hint; }

  void set_stmt_property(const common::ObString& sql_string,
                         ObProxyBasicStmtType stmt_type,
                         const common::ObString& table_name,
                         SqlFieldResult* real_field_results_ptr,
                         bool use_column_value_from_hint) {
    set_sql_string(sql_string);
    set_stmt_type(stmt_type);
    set_table_name(table_name);
    set_field_results(real_field_results_ptr);
    set_use_column_value_from_hint(use_column_value_from_hint);
  }
  //if 'table_name_' is not set, 'table_name_' means the first table name in SQL and is converted to UPPER CASE
  const common::ObString& get_table_name() const { return table_name_; }
  SqlFieldResult& get_dml_field_result() { return dml_field_results_; }

  bool has_unsupport_expr_type() const { return has_unsupport_expr_type_; }
  bool has_unsupport_expr_type_for_config() const { return has_unsupport_expr_type_for_config_; }
  bool has_sub_select() const { return has_sub_select_; }
  ExprMap& get_table_exprs_map() { return table_exprs_map_; }
  TablePosArray& get_table_pos_array() { return table_pos_array_; }
  DbTablePosArray& get_db_table_pos_array() { return db_table_pos_array_; }

protected:
  ObProxyExprType get_expr_type_by_node_type(const ObItemType& item_type);
  int get_expr_by_type(ObProxyExpr* &expr, ObProxyExprType type);

  //for from
  int handle_from_list(ParseNode* node);
  int handle_table_node_to_expr(ParseNode* node);
  int get_table_and_db_expr(ParseNode* node, ObProxyExprTable* &expr_table);
  int handle_table_and_db_node(ParseNode* node, ObProxyExprTable* &expr_table);
  //for from in delete and update
  int handle_table_references(ParseNode *node);

  //for where
  int handle_where_clause(ParseNode* node);

  //for set
  int handle_assign_list(ParseNode *node);

  //for column
  int handle_column_and_value(ParseNode* node);
  int handle_varchar_node_in_column_value(ParseNode* node, SqlField& sql_field);
  int handle_expr_list_node_in_column_value(ParseNode* node, SqlField& sql_field);
  int handle_column_ref_in_list(ParseNode *node);
  int column_ref_to_expr(ParseNode* node, ObProxyExpr* &expr, ObProxyExprTable* &expr_table);

  int condition_exprs_to_sql_string(common::ObSqlString& sql_string);
  int limit_to_sql_string(common::ObSqlString& sql_string);

  //for explain
  int handle_explain_node(const ParseResult &parse_result, ParseNode*& node);
public:
  int limit_offset_;
  int limit_size_;
  int64_t limit_token_off_;

  //pointer to parseResult, default point to 'dml_field_results_'
  //if 'field_results_' is not set, ObProxyDMLStmt save the parseResult in 'dml_field_results_'
  //if 'field_results_' is set by caller, caller need save the parseResult
  SqlFieldResult* field_results_;
  SqlFieldResult dml_field_results_;

  common::ObSEArray<common::ObString,4> comments_;
  // Store dml related information
  common::ObString table_name_;

protected:
  bool is_inited_;
  bool has_unsupport_expr_type_;
  bool has_unsupport_expr_type_for_config_;
  bool has_sub_select_;
  bool use_column_value_from_hint_;
  ExprMap table_exprs_map_;
  ExprMap alias_table_map_;
  common::ObSEArray<ObProxyExprTablePos, 4> table_pos_array_;
  common::ObSEArray<ObProxyDbTablePos, 4> db_table_pos_array_;

/*for sub select*/
public:
  int handle_project_list(ParseNode* node);
  int handle_project_string(ParseNode* node);
  int handle_hint_clause(ParseNode* node);
  int handle_groupby_clause(ParseNode* node);
  int handle_orderby_clause(ParseNode* node);
  int handle_limit_clause(ParseNode* node);
  int handle_union_clause(ParseNode* node);
  int handle_comment_list(const ParseResult &parse_result);
  virtual int to_sql_string(common::ObSqlString& sql_string);
  bool has_for_update() const { return has_for_update_; }
  int64_t get_from_token_off() { return from_token_off_; }

protected:
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

  int get_const_expr(ParseNode* node, ObProxyExpr* &expr);
  int get_sharding_const_expr(ParseNode* node, ObProxyExpr* &expr);

  int handle_table_and_db_in_hint(ParseNode* node);

  //for group by
  int handle_with_rollup_in_groupby(ParseNode* node);
  int handle_sort_list_node(ParseNode* node, const SortListType& sort_list_type);
  int handle_sort_key_node(ParseNode* node, ObProxyExpr* &expr, const SortListType& sort_list_type);

protected:
  int do_handle_parse_result(ParseNode* node);
  int comments_to_sql_string(common::ObSqlString& sql_string);
  int hint_exprs_to_sql_string(common::ObSqlString& sql_string);
  int select_exprs_to_sql_string(common::ObSqlString& sql_string);
  int table_exprs_to_sql_string(common::ObSqlString& sql_string);
  int group_by_exprs_to_sql_string(common::ObSqlString& sql_string);
  int order_by_exprs_to_sql_string(common::ObSqlString& sql_string);

  int check_node_has_agg(ParseNode* node);
public:
  common::ObSEArray<opsql::ObProxyExpr*, 4> select_exprs_;
  common::ObSEArray<opsql::ObProxyGroupItem*, 4> group_by_exprs_; //select groupby expression
  common::ObSEArray<opsql::ObProxyOrderItem*, 4> order_by_exprs_;
protected:
  bool has_rollup_;
  bool has_for_update_;
  int64_t from_token_off_;
  int64_t t_case_level_;
};

class ObProxySelectStmt : public ObProxyDMLStmt
{
public:
  ObProxySelectStmt(common::ObIAllocator& allocator) : ObProxyDMLStmt(allocator) {}
  virtual ~ObProxySelectStmt() {}
  virtual int handle_parse_result(const ParseResult &parse_result);
};

class ObProxyInsertStmt : public ObProxyDMLStmt
{
public:
  ObProxyInsertStmt(common::ObIAllocator& allocator) : ObProxyDMLStmt(allocator), row_count_(0) {}
  ~ObProxyInsertStmt() {}
  int handle_parse_result(const ParseResult &parse_result);
  int to_sql_string(common::ObSqlString& sql_string)
  {
    UNUSED(sql_string);
    return common::OB_SUCCESS;
  }
  int64_t get_row_count() const { return row_count_; }
private:
  int handle_single_table_insert(ParseNode *node);
  int handle_insert_into(ParseNode *node);
  int handle_value_list(ParseNode *node);
  int handle_column_list(ParseNode *node);
  int handle_value_vector(ParseNode *node);
private:
  int64_t row_count_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObProxyInsertStmt);
};

class ObProxyDeleteStmt : public ObProxyDMLStmt
{
public:
  ObProxyDeleteStmt(common::ObIAllocator& allocator) : ObProxyDMLStmt(allocator) {}
  ~ObProxyDeleteStmt() {}
  int handle_parse_result(const ParseResult &parse_result);
  int to_sql_string(common::ObSqlString& sql_string)
  {
    UNUSED(sql_string);
    return common::OB_SUCCESS;
  }
private:
  int handle_delete_table_node(ParseNode *node);
private:
  DISALLOW_COPY_AND_ASSIGN(ObProxyDeleteStmt);
};


class ObProxyUpdateStmt : public ObProxyDMLStmt
{
public:
  ObProxyUpdateStmt(common::ObIAllocator& allocator) : ObProxyDMLStmt(allocator) {}
  ~ObProxyUpdateStmt() {}
  int handle_parse_result(const ParseResult &parse_result);
  int to_sql_string(common::ObSqlString& sql_string)
  {
    UNUSED(sql_string);
    return common::OB_SUCCESS;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObProxyUpdateStmt);
};

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
#endif //
