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

#ifndef OBPROXY_SHARDING_SELECT_LOG_PLAN_H_
#define OBPROXY_SHARDING_SELECT_LOG_PLAN_H_

#include "engine/ob_proxy_operator.h"
#include "engine/ob_proxy_operator_table_scan.h"
#include "opsql/func_expr_resolver/proxy_expr/ob_proxy_expr.h"

namespace oceanbase
{
namespace obproxy
{

namespace optimizer
{

class ObShardingSelectLogPlan
{
public:
  ObShardingSelectLogPlan(proxy::ObProxyMysqlRequest &client_request,
                          common::ObIAllocator *allocator);
  ~ObShardingSelectLogPlan();

  int generate_plan(common::ObIArray<dbconfig::ObShardConnector*> &shard_connector_array,
                    common::ObIArray<dbconfig::ObShardProp*> &shard_prop_array,
                    common::ObIArray<hash::ObHashMapWrapper<common::ObString, common::ObString> > &table_name_map_array);
  int analyze_select_clause();
  int analyze_group_by_clause();
  int analyze_order_by_clause();
  int rewrite_sql(common::ObSqlString &new_sql);

  int add_table_scan_operator(common::ObIArray<dbconfig::ObShardConnector*> &shard_connector_array,
                              common::ObIArray<dbconfig::ObShardProp*> &shard_prop_array,
                              common::ObIArray<hash::ObHashMapWrapper<common::ObString, common::ObString> > &table_name_map_array, 
                              ObSqlString &new_sql);
  int add_mem_merge_agg_operator(bool is_set_limit);
  int add_stream_agg_operator(bool is_set_limit);
  int add_mem_merge_sort_operator(bool is_set_limit);
  int add_stream_sort_operator(bool is_set_limit);
  int add_agg_and_sort_operator(bool is_same_group_and_order);
  int add_projection_operator();
  int append_derived_order_by(bool &is_same_group_and_order);
  int is_need_derived(opsql::ObProxyExpr *expr, opsql::ObProxyExpr *&exist_expr);
  int add_derived_column(opsql::ObProxyExpr *expr);
  void print_plan_info();

  common::ObIAllocator* get_allocator() const { return allocator_; }
  engine::ObProxyOperator* get_plan_root() const { return plan_root_; }

private:
  template <typename T>
  int add_agg_operator(bool is_set_limit);
  template <typename T>
  int add_sort_operator(bool need_set_limit);
  int handle_avg_expr(opsql::ObProxyExprAvg *expr);
  int handle_agg_expr(opsql::ObProxyExpr *expr, bool need_add_calc = false);
  int compare_group_and_order(bool &is_same_group_and_order);
  int handle_select_derived(opsql::ObProxyExpr *expr);
  int do_handle_select_derived(opsql::ObProxyExpr *expr, bool &bret, bool is_root = false);
  int handle_derived(opsql::ObProxyExpr *expr, bool column_first = false);
  int do_need_derived(common::ObIArray<opsql::ObProxyExpr*> &expr_array, opsql::ObProxyExpr *expr,
                      bool column_first, bool &bret);
  int do_other_need_derived(opsql::ObProxyExpr *tmp_expr, opsql::ObProxyExpr *expr, bool &bret);
  int do_other_need_derived_for_avg(opsql::ObProxyExpr *tmp_expr, opsql::ObProxyExpr *expr);
  int do_column_need_derived(common::ObIArray<opsql::ObProxyExpr*> &expr_array, opsql::ObProxyExpr *expr, bool &bret);
  int do_column_need_derived_with_alias(common::ObIArray<opsql::ObProxyExpr*> &expr_array, opsql::ObProxyExpr *expr, bool &bret);
  int do_column_need_derived_with_star(common::ObIArray<opsql::ObProxyExpr*> &expr_array, opsql::ObProxyExpr *expr, bool &bret);
  template <typename T>
  int do_handle_avg_expr(opsql::ObProxyExprAvg *agg_expr, T *&expr,
                         const char* op, ObProxyExprType expr_type);

private:
  proxy::ObProxyMysqlRequest &client_request_;
  common::ObIAllocator *allocator_;
  engine::ObProxyOperator *plan_root_;
  int64_t derived_column_count_;
  bool is_set_limit_;
  common::ObSEArray<opsql::ObProxyExpr*, 4> agg_exprs_;
  common::ObSEArray<opsql::ObProxyExpr*, 4> calc_exprs_;
  common::ObSEArray<opsql::ObProxyExpr*, 4> derived_exprs_;
  common::ObSEArray<common::ObString, 4> derived_columns_;
  common::ObSEArray<common::ObString, 4> derived_orders_;
};

} // end optimizer
} // end obproxy
} // end oceanbase

#endif
