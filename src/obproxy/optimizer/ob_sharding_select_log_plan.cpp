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

#include "optimizer/ob_sharding_select_log_plan.h"
#include "engine/ob_proxy_operator_sort.h"
#include "engine/ob_proxy_operator_agg.h"
#include "engine/ob_proxy_operator_table_scan.h"
#include "engine/ob_proxy_operator_projection.h"
#include "obutils/ob_proxy_stmt.h"

using namespace oceanbase::obproxy::engine;
using namespace oceanbase::common;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace optimizer
{

template <typename O, typename I>
int create_operator_and_input(ObIAllocator *allocator, O *&op, I *&input)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  op = NULL;
  input = NULL;
  if (NULL == (ptr = allocator->alloc(sizeof(I)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc input failed", K(ret));
  } else {
    input = new (ptr) I();
  }

  if (OB_SUCC(ret) && (NULL == (ptr = allocator->alloc(sizeof(O))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc operator failed", K(ret));
  } else {
    op = new (ptr) O(input, *allocator);
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(op->init())) {
      LOG_WARN("op init failed", K(ret));
    }
  }
  return ret;
}

ObShardingSelectLogPlan::ObShardingSelectLogPlan(obutils::ObSqlParseResult &parse_result,
                                                 ObIAllocator *allocator,
                                                 ObIArray<ObString> &physical_table_name_array)
       : parse_result_(parse_result), allocator_(allocator), plan_root_(NULL),
         shard_connector_array_(ObModIds::OB_PROXY_SHARDING_OPTIMIZER, OB_MALLOC_NORMAL_BLOCK_SIZE),
         physical_table_name_array_(physical_table_name_array),
         table_scan_(NULL), output_expr_array_(ObModIds::OB_PROXY_SHARDING_OPTIMIZER, OB_MALLOC_NORMAL_BLOCK_SIZE)
{
}

ObShardingSelectLogPlan::~ObShardingSelectLogPlan() {
  if (NULL != plan_root_) {
    plan_root_->~ObProxyOperator();
  }

  plan_root_ = NULL;
  table_scan_ = NULL;
}

int ObShardingSelectLogPlan::generate_plan()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("begin to generate plan");

  if (OB_FAIL(analyze_from_clause())) {
    LOG_WARN("analyze from clause failed", K(ret));
  } else if (OB_FAIL(analyze_where_clause())) {
    LOG_WARN("analyze where clause failed", K(ret));
  } else if (OB_FAIL(analyze_group_by_clause())) {
    LOG_WARN("analyze group by clause failed", K(ret));
  } else if (OB_FAIL(analyze_order_by_clause())) {
    LOG_WARN("analyze order by clause failed", K(ret));
  }  else if (OB_FAIL(analyze_select_clause())) {
    LOG_WARN("analyze select clause failed", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(add_projection_operator())) {
      LOG_WARN("add projection operator failed", K(ret));
    } else if (OB_FAIL(traverse_plan_tree(plan_root_))) {
      LOG_WARN("traverse plan tree failed", K(ret));
    } else {
      print_plan_info();
    }
  }

  return ret;
}

int ObShardingSelectLogPlan::analyze_from_clause()
{
  int ret = OB_SUCCESS;
  ObProxySelectStmt *select_stmt = static_cast<ObProxySelectStmt*>(parse_result_.get_proxy_stmt());

  // generate table scan operator
  ObProxyTableScanOp *table_scan_op = NULL;
  ObProxyTableScanInput *table_scan_input = NULL;
  if (OB_FAIL(create_operator_and_input(allocator_, table_scan_op, table_scan_input))) {
    LOG_WARN("create operator and input failed", K(ret));
  } else {
    table_scan_ = table_scan_op;
    table_scan_input->set_logical_table_name(parse_result_.get_origin_table_name());
    table_scan_input->set_logical_database_name(parse_result_.get_origin_database_name());
    table_scan_input->set_db_key_names(shard_connector_array_);
    table_scan_input->set_phy_db_table_names(physical_table_name_array_);
    table_scan_input->set_hint_string(select_stmt->hint_string_);

    plan_root_ = table_scan_op;
  }

  return ret;
}

int ObShardingSelectLogPlan::analyze_where_clause() {
  int ret = OB_SUCCESS;

  ObProxySelectStmt *select_stmt = static_cast<ObProxySelectStmt*>(parse_result_.get_proxy_stmt());
  ObIArray<ObProxyExpr*> &condition_expr_array = select_stmt->condition_exprs_;

  if (condition_expr_array.count() > 0) {
    if (1 != condition_expr_array.count()) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ObProxyExpr* condition_expr = condition_expr_array.at(0);
      if (condition_expr->has_agg()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("where expr has agg unexpected", K(ret));
      } else {
        ObProxyTableScanInput *table_scan_input = static_cast<ObProxyTableScanInput*>(table_scan_->get_input());
        if (OB_FAIL(table_scan_input->get_condition_exprs().push_back(condition_expr))) {
          LOG_WARN("push back condition expr failed", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObShardingSelectLogPlan::analyze_group_by_clause()
{
  int ret = OB_SUCCESS;
  ObProxySelectStmt *select_stmt = static_cast<ObProxySelectStmt*>(parse_result_.get_proxy_stmt());
  ObIArray<ObProxyExpr*> &group_by_expr_array = select_stmt->group_by_exprs_;
  bool has_agg = false;
  ObProxyAggOp *agg_op = NULL;
  ObProxyAggInput *agg_input = NULL;

  if (group_by_expr_array.count() > 0) {
    has_agg = true;

    ObProxyMergeAggOp *merge_agg = NULL;
    if (OB_FAIL(create_operator_and_input(allocator_, merge_agg, agg_input))) {
      LOG_WARN("create operator and input for agg failed", K(ret));
    } else {
      agg_op = merge_agg;
      ObProxyTableScanInput *table_scan_input = static_cast<ObProxyTableScanInput*>(table_scan_->get_input());
      for (int64_t i = 0; OB_SUCC(ret) && i < group_by_expr_array.count(); i++) {
        ObProxyExpr* expr = group_by_expr_array.at(i);
        // group by must not follow agg func
        if (expr->has_agg()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("group by expr is agg unexpected", K(ret));
        } else if (expr->has_alias()) {
          if (!expr->is_alias()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("has alias but alias is a sub expr", K(ret));
          } else if (OB_FAIL(table_scan_input->get_group_by_exprs().push_back(expr))) {
            LOG_WARN("table scan add group by expr failed", K(ret));
          } else if (OB_FAIL(agg_input->get_group_by_exprs().push_back(expr))) {
            LOG_WARN("agg add group by expr failed", K(ret));
          } else {
            //expr->set_index();
            //do nothing, expr->index will be updated in ObProxyAggOp::init_group_by_columns
          }
        } else if (OB_FAIL(output_expr_array_.push_back(expr))) {
          LOG_WARN("output expr array push back failed", K(ret));
        } else if (OB_FAIL(table_scan_input->get_group_by_exprs().push_back(expr))) {
          LOG_WARN("table scan add group by expr failed", K(ret));
        } else if (OB_FAIL(agg_input->get_group_by_exprs().push_back(expr))) {
          LOG_WARN("agg add group by expr failed", K(ret));
        } else {
          expr->set_index(output_expr_array_.count() - 1);
        }
      }
    }
  } else {
    for (int64_t i = 0; !has_agg && i < select_stmt->order_by_exprs_.count(); i++) {
      if (select_stmt->order_by_exprs_.at(i)->has_agg()) {
        has_agg = true;
      }
    }

    for (int64_t i = 0; !has_agg && i < select_stmt->select_exprs_.count(); i++) {
      if (select_stmt->select_exprs_.at(i)->has_agg()) {
        has_agg = true;
      }
    }

    if (has_agg && OB_FAIL(create_operator_and_input(allocator_, agg_op, agg_input))) {
      LOG_WARN("create operator and input for agg failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && has_agg) {
    if (OB_FAIL(agg_op->set_child(0, plan_root_))) {
      LOG_WARN("set child failed", K(ret));
    } else {
      plan_root_ = agg_op;
    }
  }

  return ret;
}

int ObShardingSelectLogPlan::handle_avg_expr(opsql::ObProxyExprAvg *agg_expr)
{
  int ret = OB_SUCCESS;

  ObProxyExprSum *sum_expr = NULL;
  void *ptr = NULL;

  if (OB_ISNULL(ptr = allocator_->alloc(sizeof(ObProxyExprSum)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
  } else {
    sum_expr = new (ptr) ObProxyExprSum();
    sum_expr->set_expr_type(OB_PROXY_EXPR_TYPE_FUNC_SUM);
    sum_expr->set_param_array(agg_expr->get_param_array());
    sum_expr->has_agg_ = 1;
    if (OB_FAIL(output_expr_array_.push_back(sum_expr))) {
      LOG_WARN("output expr push back failed", K(ret));
    } else {
      agg_expr->set_sum_index(output_expr_array_.count() - 1);
      sum_expr->set_index(output_expr_array_.count() - 1);
    }
  }

  if (OB_SUCC(ret))
  {
    ObProxyExprCount *count_expr = NULL;
    void *ptr = allocator_->alloc(sizeof(ObProxyExprCount));

    if (OB_UNLIKELY(NULL == ptr)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
    } else {
      count_expr = new (ptr) ObProxyExprCount();
      count_expr->set_expr_type(OB_PROXY_EXPR_TYPE_FUNC_COUNT);
      count_expr->set_param_array(agg_expr->get_param_array());
      count_expr->has_agg_ = 1;
      if (OB_FAIL(output_expr_array_.push_back(count_expr))) {
        LOG_WARN("output expr push back failed", K(ret));
      } else {
        agg_expr->set_count_index(output_expr_array_.count() - 1);
        count_expr->set_index(output_expr_array_.count() - 1);
      }
    }
  }

  return ret;
}

int ObShardingSelectLogPlan::analyze_order_by_clause()
{
  int ret = OB_SUCCESS;

  ObProxySelectStmt *select_stmt = static_cast<ObProxySelectStmt*>(parse_result_.get_proxy_stmt());
  ObIArray<ObProxyOrderItem*> *order_by_expr_array = &select_stmt->order_by_exprs_;
  ObProxyOperator *sort_op = NULL;
  ObProxySortInput *sort_input = NULL;
  ObProxyTableScanInput *table_scan_input = static_cast<ObProxyTableScanInput*>(table_scan_->get_input());

  if (order_by_expr_array->count() > 0) {
    if (select_stmt->limit_start_ > 0) {
      ObProxyTopKOp *topK = NULL;
      if (OB_FAIL(create_operator_and_input(allocator_, topK, sort_input))) {
        LOG_WARN("create operator and input for topk failed", K(ret));
      } else {
        sort_op = topK;
      }
    } else {
      ObProxyMemSortOp *mem_sort = NULL;
      if (OB_FAIL(create_operator_and_input(allocator_, mem_sort, sort_input))) {
        LOG_WARN("create operator and input for mem sort failed", K(ret));
      } else {
        sort_op = mem_sort;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(sort_op->set_child(0, plan_root_))) {
        LOG_WARN("set child failed", K(ret));
      } else {
        plan_root_ = sort_op;
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < order_by_expr_array->count(); i++) {
      ObProxyOrderItem *order_item = order_by_expr_array->at(i);
      ObProxyExpr *expr = order_item->expr_;
      if ((!expr->has_agg() && !expr->has_alias()) || expr->is_agg()) {
        if (OB_FAIL(output_expr_array_.push_back(expr))) {
          LOG_WARN("output expr push failed", K(ret));
        } else if (OB_FAIL(sort_input->get_order_by_expr().push_back(order_item))) {
          LOG_WARN("add order by expr failed", K(ret));
        } else if (OB_FAIL(table_scan_input->get_order_by_exprs().push_back(order_item))) {
          LOG_WARN("table scan add order by expr failed", K(ret));
        } else {
          expr->set_index(output_expr_array_.count() - 1);
        }
      } else if (expr->is_alias()) {
        if (expr->has_agg() && !expr->is_agg()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("has agg not support", K(ret), K(expr));
        } else if (OB_FAIL(sort_input->get_order_by_expr().push_back(order_item))) {
          LOG_WARN("add order by expr failed", K(ret));
        } else if (OB_FAIL(table_scan_input->get_order_by_exprs().push_back(order_item))) {
          LOG_WARN("table scan add order by expr failed", K(ret));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        ObProxyExpr::print_proxy_expr(expr);
        LOG_WARN("not support", K(ret), K(*expr));
      }
    }
  }
  return ret;
}

int ObShardingSelectLogPlan::analyze_select_clause()
{
  int ret = OB_SUCCESS;
  ObProxySelectStmt *select_stmt = static_cast<ObProxySelectStmt*>(parse_result_.get_proxy_stmt());
  ObIArray<ObProxyExpr*> &select_expr_array = select_stmt->select_exprs_;

  for (int64_t i = 0; OB_SUCC(ret) && i < select_expr_array.count(); i++) {
    ObProxyExpr *expr = select_expr_array.at(i);
    if (expr->has_agg()) {
      ObSEArray<ObProxyExpr*, 4> agg_expr_array;
      if (OB_FAIL(get_agg_related_expr(expr, agg_expr_array))) {
        LOG_WARN("get agg related expr failed", K(ret), K(expr));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < agg_expr_array.count(); j++) {
          ObProxyExpr *agg_expr = agg_expr_array.at(j);
          if (OB_PROXY_EXPR_TYPE_FUNC_AVG == agg_expr->get_expr_type()) {
            if (OB_FAIL(handle_avg_expr(static_cast<ObProxyExprAvg *>(agg_expr)))) {
              LOG_WARN("handle avg expr failed", K(ret));
            }
          } else {
            if (OB_FAIL(output_expr_array_.push_back(agg_expr))) {
              LOG_WARN("output expr push back failed", K(ret));
            } else {
              agg_expr->set_index(output_expr_array_.count() - 1);
            }
          }
        }
      }
    }
  }

  for (int64_t i = select_expr_array.count() - 1; OB_SUCC(ret) && i >=0; i--) {
    ObProxyExpr *expr = select_expr_array.at(i);
    if (OB_FAIL(output_expr_array_.push_back(expr))) {
      LOG_WARN("output expr push failed", K(ret));
    } else if (!expr->has_agg() && !expr->is_star_expr()) {
      expr->set_index(output_expr_array_.count() - 1);
    }

    if (expr->is_star_expr() && i != 0) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("* is not fisrt expr in select", K(ret));
    }
  }

  for (int64_t i = 0, j = output_expr_array_.count() - 1; OB_SUCC(ret) && i < j; i++, j--) {
    ObProxyExpr *tmp_expr = output_expr_array_.at(i);
    output_expr_array_.at(i) = output_expr_array_.at(j);
    output_expr_array_.at(j) = tmp_expr;
  }

  return ret;
}

int ObShardingSelectLogPlan::add_projection_operator() {
  int ret = OB_SUCCESS;
  ObProxySelectStmt *select_stmt = static_cast<ObProxySelectStmt*>(parse_result_.get_proxy_stmt());

  ObProxyProOp *op = NULL;
  ObProxyProInput *input = NULL;

  if (OB_FAIL(create_operator_and_input(allocator_, op, input))) {
    LOG_WARN("create projection failed", K(ret));
  } else {
    input->set_select_exprs(select_stmt->select_exprs_);
    if (OB_FAIL(op->set_child(0, plan_root_))) {
      LOG_WARN("set child failed", K(ret));
    } else {
      plan_root_ = op;
    }
  }

  return ret;
}

int ObShardingSelectLogPlan::get_agg_related_expr(ObProxyExpr* expr, ObIArray<ObProxyExpr*> &array)
{
  int ret = OB_SUCCESS;
  if (NULL != expr && OB_SUCC(ret)) {
    if (expr->is_alias()) {
     ObProxyExprShardingConst *const_expr = static_cast<ObProxyExprShardingConst*>(expr);
     if (OB_FAIL(get_agg_related_expr(const_expr->expr_, array))) {
       LOG_WARN("get agg related expr failed", K(ret));
     }
    } else if (!expr->has_agg() || expr->is_agg()) {
      if(OB_FAIL(array.push_back(expr))) {
        LOG_WARN("push back expr failed", K(ret));
      }
    } else if (!expr->is_func_expr()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(ret), K(*expr));
    } else {
      ObProxyFuncExpr *func_expr = static_cast<ObProxyFuncExpr*>(expr);
      int64_t count = func_expr->get_param_array().count();
      if (OB_PROXY_EXPR_TYPE_SHARDING_ALIAS == func_expr->get_expr_type()) {
        count--;
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
        ObProxyExpr* param_expr = func_expr->get_param_array().at(i);
        if (OB_FAIL(get_agg_related_expr(param_expr, array))) {
          LOG_WARN("get agg related expr failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObShardingSelectLogPlan::traverse_plan_tree(ObProxyOperator *op)
{
  int ret = OB_SUCCESS;
  if (NULL != op) {
    ObProxySelectStmt *select_stmt = static_cast<ObProxySelectStmt *>(parse_result_.get_proxy_stmt());
    ObProxyOpInput* input = op->get_input();
    input->set_op_limit(select_stmt->limit_start_, select_stmt->limit_offset_);
    input->set_select_exprs(output_expr_array_);
    input->set_added_row_count(output_expr_array_.count() - select_stmt->select_exprs_.count());
    if (OB_FAIL(traverse_plan_tree(op->get_child(0)))) {
      LOG_WARN("traverse plan tree failed", K(ret));
    }
  }
  return ret;
}

void ObShardingSelectLogPlan::print_plan_info()
{
  LOG_DEBUG("plan output info", K(output_expr_array_.count()));
  for (int64_t i = 0; i < output_expr_array_.count(); i++) {
    ObProxyExpr::print_proxy_expr(output_expr_array_.at(i));
  }
  ObProxyOperator::print_execute_plan_info(plan_root_);
}

int ObShardingSelectLogPlan::set_shard_connector_array(const ObIArray<dbconfig::ObShardConnector*> & array)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < array.count(); i++) {
    dbconfig::ObShardConnector *connector = array.at(i);
    if (OB_FAIL(shard_connector_array_.push_back(connector))) {
      LOG_WARN("shard connector push back failed", K(ret));
    }
  }
  return ret;
}

} // end optimizer
} // end obproxy
} // end oceanbase
