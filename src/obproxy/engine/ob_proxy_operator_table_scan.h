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

#ifndef OBPROXY_OB_PROXY_OPERATOR_TABLE_SCAN_H
#define OBPROXY_OB_PROXY_OPERATOR_TABLE_SCAN_H

#include "opsql/func_expr_resolver/proxy_expr/ob_proxy_expr.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"

#include "ob_proxy_operator.h"
#include "ob_proxy_operator_async_task.h"

namespace oceanbase {
namespace obproxy {
namespace engine {
class ObProxyTableScanOp : public ObProxyOperator
{
public:
  ObProxyTableScanOp(ObProxyOpInput *input, common::ObIAllocator &allocator)
    : ObProxyOperator(input, allocator), sub_sql_count_(0) {
    set_op_type(PHY_TABLE_SCAN);
  }

  virtual ~ObProxyTableScanOp() {};

  virtual int open(event::ObContinuation *cont, event::ObAction *&action, const int64_t timeout_ms = 0);
  virtual int get_next_row();

  virtual ObProxyOperator* get_child(const uint32_t idx);// { return NULL; }

  int rewrite_hint_table(const common::ObString &hint_string, common::ObSqlString &obj_hint_string,
      const common::ObString &table_name, const common::ObString &database_name,
      const common::ObString &real_table_name, const common::ObString &real_database_name,
      bool is_oracle_mode);

  virtual int format_sql_header(ObSqlString &sql_header); /* sql: SELECT ... FROM */
  virtual int format_sql_tailer(ObSqlString &sql_header, bool is_oracle_mode); /* sql: WHERE ... GROUP BY ORDER BY LIMIT */

  virtual int handle_result(void *src, bool is_final, ObProxyResultResp *&result);
  virtual int handle_response_result(void *src, bool is_final, ObProxyResultResp *&result);
  virtual int process_ready_data(void *data, int &event);
  virtual int process_complete_data(void *data);

  int64_t get_sub_sql_count() { return sub_sql_count_; }
  void set_sub_sql_count(int64_t count) { sub_sql_count_ = count; }
protected:
  int64_t sub_sql_count_;
};

class ObProxyTableScanInput : public ObProxyOpInput
{
public:
  ObProxyTableScanInput()
    : ObProxyOpInput(), // { }
      logical_table_name_(),
      logical_database_name_(),
      phy_db_table_names_(ObModIds::OB_SE_ARRAY_ENGINE, array_new_alloc_size),
      db_key_names_(ObModIds::OB_SE_ARRAY_ENGINE, array_new_alloc_size),
      condition_exprs_(ObModIds::OB_SE_ARRAY_ENGINE, array_new_alloc_size),
      group_by_exprs_(ObModIds::OB_SE_ARRAY_ENGINE, array_new_alloc_size),
      having_exprs_(NULL),
      order_by_exprs_(ObModIds::OB_SE_ARRAY_ENGINE, array_new_alloc_size),
      hint_string_(),
      normal_annotation_string_()
  {}

  ObProxyTableScanInput(const common::ObString &logical_table_name,
              const common::ObString &logical_database_name,
              //const common::ObSEArray<std::pair<common::ObString, common::ObString>, 1>
              //  &phy_db_table_names,
              const common::ObSEArray<common::ObString, 4> &phy_db_table_names,
              const common::ObSEArray<dbKeyName*, 4> &db_key_names,
              const common::ObSEArray<ObProxyExpr*, 4> &select_exprs,
              const common::ObSEArray<ObProxyExpr*, 4> &condition_exprs,
              const common::ObSEArray<ObProxyExpr*, 4> &group_by_exprs,
              ObProxyExpr* having_exprs,
              const common::ObSEArray<ObProxyExpr*, 4> &order_by_exprs,
              const common::ObString &hint_string,
              const common::ObString &normal_annotation_string)
    : ObProxyOpInput(select_exprs),
        logical_table_name_(logical_table_name),
        logical_database_name_(logical_database_name),
        phy_db_table_names_(phy_db_table_names),
        db_key_names_(db_key_names),
        condition_exprs_(condition_exprs),
        group_by_exprs_(group_by_exprs),
        having_exprs_(having_exprs),
        order_by_exprs_(order_by_exprs),
        hint_string_(hint_string),
        normal_annotation_string_(normal_annotation_string)
  {
  }

  ~ObProxyTableScanInput() {}

  void set_logical_table_name(const common::ObString &logical_table_name) {
    logical_table_name_ = logical_table_name;
  }
  common::ObString& get_logical_table_name() {
    return logical_table_name_;
  }

  void set_logical_database_name(const common::ObString &logical_database_name) {
    logical_database_name_ = logical_database_name;
  }
  common::ObString& get_logical_database_name() {
    return logical_database_name_;
  }

  void set_db_key_names(const common::ObIArray<dbKeyName*> &db_key_names) {
    db_key_names_.assign(db_key_names);
  }
  const common::ObSEArray<dbKeyName*, 4>& get_db_key_names() {
    return db_key_names_;
  }

  void set_phy_db_table_names(
    //const common::ObSEArray<std::pair<common::ObString, common::ObString>, 1>
    const common::ObIArray<common::ObString> &phy_db_table_names) {
    phy_db_table_names_.assign(phy_db_table_names);
  }
  //common::ObSEArray<std::pair<common::ObString, common::ObString>, 1>&
  common::ObSEArray<common::ObString, 4>& get_phy_db_table_names() {
    return phy_db_table_names_;
  }

  void set_condition_exprs(const common::ObSEArray<ObProxyExpr*, 4> &condition_exprs) {
    condition_exprs_ = condition_exprs;
  }
  common::ObSEArray<ObProxyExpr*, 4>& get_condition_exprs() {
    return condition_exprs_;
  }

  void set_group_by_exprs(const common::ObSEArray<ObProxyExpr*, 4> &group_by_exprs) {
    group_by_exprs_ = group_by_exprs;
  }
  common::ObSEArray<ObProxyExpr*, 4>& get_group_by_exprs() {
    return group_by_exprs_;
  }

  void set_order_by_exprs(const common::ObSEArray<ObProxyExpr*, 4> &order_by_exprs) {
    order_by_exprs_ = order_by_exprs;
  }
  common::ObSEArray<ObProxyExpr*, 4>& get_order_by_exprs() {
    return order_by_exprs_;
  }

  void set_hint_string(const common::ObString &hint_string) {
    hint_string_ = hint_string;
  }
  common::ObString& get_hint_string() {
    return hint_string_;
  }

  void set_normal_annotation_string(const common::ObString &normal_annotation_string) {
    normal_annotation_string_ = normal_annotation_string;
  }
  common::ObString& get_normal_annotation_string() {
    return normal_annotation_string_;
  }

protected:
  common::ObString logical_table_name_;
  common::ObString logical_database_name_;
  common::ObSEArray<common::ObString, 4> phy_db_table_names_;
  common::ObSEArray<dbKeyName*, 4> db_key_names_;
  //common::ObSEArray<ObProxyExpr*, 4> select_exprs_;
  common::ObSEArray<ObProxyExpr*, 4> condition_exprs_;
  common::ObSEArray<ObProxyExpr*, 4> group_by_exprs_;
  ObProxyExpr* having_exprs_;
  common::ObSEArray<ObProxyExpr*, 4> order_by_exprs_;
  common::ObString hint_string_;
  common::ObString normal_annotation_string_;
};

}
}
}

#endif //OBPROXY_OB_PROXY_OPERATOR_TABLE_SCAN_H
