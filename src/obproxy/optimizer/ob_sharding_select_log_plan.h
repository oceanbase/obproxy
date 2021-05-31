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
  ObShardingSelectLogPlan(obutils::ObSqlParseResult &parse_result,
                          common::ObIAllocator *allocator,
                          common::ObIArray<common::ObString> &physical_table_name_array);
  ~ObShardingSelectLogPlan();

  int generate_plan();
  int analyze_from_clause();
  int analyze_where_clause();
  int analyze_group_by_clause();
  int analyze_order_by_clause();
  int analyze_select_clause();
  int add_projection_operator();
  int traverse_plan_tree(engine::ObProxyOperator *op);
  void print_plan_info();
  int set_shard_connector_array(const ObIArray<dbconfig::ObShardConnector*> & array);

  common::ObIAllocator* get_allocator() const { return allocator_; }
  engine::ObProxyOperator* get_plan_root() const { return plan_root_; }

private:
  int get_agg_related_expr(opsql::ObProxyExpr* expr, common::ObIArray<opsql::ObProxyExpr*> &array);
  int handle_avg_expr(opsql::ObProxyExprAvg *expr);

private:
  obutils::ObSqlParseResult &parse_result_;
  common::ObIAllocator *allocator_;
  engine::ObProxyOperator *plan_root_;
  common::ObSEArray<dbconfig::ObShardConnector*, 4> shard_connector_array_;
  common::ObIArray<common::ObString> &physical_table_name_array_;
  engine::ObProxyTableScanOp *table_scan_;
  common::ObSEArray<opsql::ObProxyExpr*, 4> output_expr_array_;
};

} // end optimizer
} // end obproxy
} // end oceanbase

#endif
