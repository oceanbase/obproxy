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
using namespace oceanbase::obproxy::opsql;

namespace oceanbase
{
namespace obproxy
{
namespace optimizer
{

const static char DERIVED_COLUMN[] = "DERIVED_COLUMN";

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

ObShardingSelectLogPlan::ObShardingSelectLogPlan(proxy::ObProxyMysqlRequest &client_request,
                                                 ObIAllocator *allocator)
       : client_request_(client_request), allocator_(allocator), plan_root_(NULL),
         derived_column_count_(0), is_set_limit_(false),
         agg_exprs_(ObModIds::OB_PROXY_SHARDING_OPTIMIZER, OB_MALLOC_NORMAL_BLOCK_SIZE),
         calc_exprs_(ObModIds::OB_PROXY_SHARDING_OPTIMIZER, OB_MALLOC_NORMAL_BLOCK_SIZE),
         derived_exprs_(ObModIds::OB_PROXY_SHARDING_OPTIMIZER, OB_MALLOC_NORMAL_BLOCK_SIZE),
         derived_columns_(ObModIds::OB_PROXY_SHARDING_OPTIMIZER, OB_MALLOC_NORMAL_BLOCK_SIZE),
         derived_orders_(ObModIds::OB_PROXY_SHARDING_OPTIMIZER, OB_MALLOC_NORMAL_BLOCK_SIZE)
{
}

ObShardingSelectLogPlan::~ObShardingSelectLogPlan()
{
  if (NULL != plan_root_) {
    plan_root_->~ObProxyOperator();
  }

  plan_root_ = NULL;
}

int ObShardingSelectLogPlan::generate_plan(ObIArray<dbconfig::ObShardConnector*> &shard_connector_array,
                                           ObIArray<dbconfig::ObShardProp*> &shard_prop_array,
                                           ObIArray<hash::ObHashMapWrapper<ObString, ObString> > &table_name_map_array)
{
  int ret = OB_SUCCESS;
  ObSqlString new_sql;
  bool is_same_group_and_order = false;
  LOG_DEBUG("begin to generate plan");

  if (OB_FAIL(analyze_select_clause())) {
    LOG_WARN("analyze select clause failed", K(ret));
  } else if (OB_FAIL(analyze_group_by_clause())) {
    LOG_WARN("analyze order by clause failed", K(ret));
  } else if (OB_FAIL(analyze_order_by_clause())) {
    LOG_WARN("analyze group by clause failed", K(ret));
  } else if (OB_FAIL(append_derived_order_by(is_same_group_and_order))) {
    LOG_WARN("fail to append derived order by", K(ret));
  } else if (OB_FAIL(rewrite_sql(new_sql))) {
    LOG_WARN("fail to rewrite sql", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(add_table_scan_operator(shard_connector_array,
                                        shard_prop_array,
                                        table_name_map_array,
                                        new_sql))) {
      LOG_WARN("fail to add table scan operator", K(ret));
    } else if (OB_FAIL(add_agg_and_sort_operator(is_same_group_and_order))) {
      LOG_WARN("fail to add agg operator", K(ret));
    } else if (OB_FAIL(add_projection_operator())) {
      LOG_WARN("fail to add projection operator", K(ret));
    } else {
      print_plan_info();
    }
  }

  return ret;
}

template <typename T>
int ObShardingSelectLogPlan::do_handle_avg_expr(ObProxyExprAvg *agg_expr, T *&expr,
                                                const char* op, ObProxyExprType expr_type)
{
  int ret = OB_SUCCESS;

  void *ptr = NULL;
  if (OB_ISNULL(ptr = allocator_->alloc(sizeof(T)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc sum expr buf", K(ret));
  } else {
    expr = new (ptr) T();
    expr->set_expr_type(expr_type);
    expr->has_agg_ = 1;
    expr->set_param_array(agg_expr->get_param_array());
    ObSqlString sql_string;
    char *buf = NULL;
    if (OB_FAIL(sql_string.append(op))) {
      LOG_WARN("append failed", K(ret));
    } else if (OB_FAIL(sql_string.append("("))) {
      LOG_WARN("append failed", K(ret));
    } else if (OB_FAIL(agg_expr->get_param_array().at(0)->to_column_string(sql_string))) {
      LOG_WARN("to sql_string failed", K(ret));
    } else if (OB_FAIL(sql_string.append(")"))) {
      LOG_WARN("append failed", K(ret));
    } else if (OB_ISNULL(buf = (char*)allocator_->alloc(sql_string.length()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc sum expr buf", K(ret));
    } else {
      MEMCPY(buf, sql_string.ptr(), sql_string.length());
      expr->set_expr_name(buf, sql_string.length());
      if (OB_FAIL(handle_derived(expr))) {
       LOG_WARN("fail to handle derived", K(ret));
      }
    }
  }

  return ret;
}

int ObShardingSelectLogPlan::handle_avg_expr(ObProxyExprAvg *agg_expr)
{
  int ret = OB_SUCCESS;

  ObProxyExprSum *sum_expr = NULL;
  ObProxyExprCount *count_expr = NULL;
  if (OB_FAIL(do_handle_avg_expr(agg_expr, sum_expr, "SUM", OB_PROXY_EXPR_TYPE_FUNC_SUM))) {
    LOG_WARN("fail to do handle avg expr", K(ret));
  } else if (OB_FAIL(do_handle_avg_expr(agg_expr, count_expr, "COUNT", OB_PROXY_EXPR_TYPE_FUNC_COUNT))) {
    LOG_WARN("fail to do handle avg expr", K(ret));
  } else {
    agg_expr->set_sum_expr(sum_expr);
    agg_expr->set_count_expr(count_expr);
  }

  return ret;
}

int ObShardingSelectLogPlan::handle_agg_expr(ObProxyExpr *expr, bool need_add_calc)
{
  int ret = OB_SUCCESS;

  if (expr->has_agg()) {
    // If itself is an aggregate function
    if (expr->is_agg()) {
      // If it is an avg aggregate function, it is actually a sum/count calculation function
      if (OB_PROXY_EXPR_TYPE_FUNC_AVG == expr->get_expr_type()) {
        ObProxyExprAvg *avg_expr = NULL;
        if (OB_ISNULL(avg_expr = dynamic_cast<ObProxyExprAvg *>(expr))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to dynamic cast", K(expr), K(ret));
        } else if (OB_FAIL(handle_avg_expr(avg_expr))) {
          LOG_WARN("handle avg expr failed", KPC(expr), K(ret));
        } else if (need_add_calc && OB_FAIL(calc_exprs_.push_back(avg_expr))) {
          LOG_WARN("fail to push back to calc expr", KP(avg_expr), K(ret));
        }
      } else {
        // Other aggregate functions, put into the aggregate function array
        if (OB_FAIL(agg_exprs_.push_back(expr))) {
          LOG_WARN("fail to add agg expr to array", K(ret));
        }
      }
    } else if (!expr->is_func_expr()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", KPC(expr), K(ret));
    } else {
      ObProxyFuncExpr *func_expr = NULL;
      if (OB_ISNULL(func_expr = dynamic_cast<ObProxyFuncExpr*>(expr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to dynamic cast", K(expr), K(ret));
      } else {
        ObSEArray<ObProxyExpr*, 4>& param_array = func_expr->get_param_array();

        // Parse each parameter for further processing
        for (int64_t i = 0; OB_SUCC(ret) && i < param_array.count(); i++) {
          ObProxyExpr* param_expr = param_array.at(i);
          if (OB_FAIL(handle_derived(param_expr))) {
            LOG_WARN("fail to handle derived", K(ret));
          }
        }

        if (OB_SUCC(ret) && need_add_calc) {
          if (OB_FAIL(calc_exprs_.push_back(expr))) {
            LOG_WARN("fail to push back calc expr", KP(expr), K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObShardingSelectLogPlan::add_derived_column(ObProxyExpr *expr)
{
  int ret = OB_SUCCESS;
  ObSqlString sql_string;
  char *buf = NULL;
  if (OB_FAIL(sql_string.append_fmt("%s_%ld", DERIVED_COLUMN, derived_column_count_++))) {
    LOG_WARN("fail to append derived column name", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(sql_string.length())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc alias name buf", "len", sql_string.length(), K(ret));
  } else {
    MEMCPY(buf, sql_string.ptr(), sql_string.length());
    expr->set_alias_name(buf, sql_string.length());
    buf = NULL;
    sql_string.reuse();
    if (OB_FAIL(expr->to_sql_string(sql_string))) {
      LOG_WARN("fail to get sql string", K(ret));
    } else if (OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(sql_string.length())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc sql string buf", K(ret));
    } else {
      MEMCPY(buf, sql_string.ptr(), sql_string.length());
      ObString derived_column(sql_string.length(), buf);
      if (OB_FAIL(derived_columns_.push_back(derived_column))) {
        LOG_WARN("fail to add derived column", K(ret));
      } else if (OB_FAIL(derived_exprs_.push_back(expr))) {
        LOG_WARN("fail to add derived expr", K(ret));
      }
    }
  }

  return ret;
}

int ObShardingSelectLogPlan::do_column_need_derived_with_star(ObIArray<ObProxyExpr*> &expr_array, ObProxyExpr *expr, bool &bret)
{
  int ret = OB_SUCCESS;
  bret = true;

  ObProxyExprColumn* expr_column = dynamic_cast<ObProxyExprColumn*>(expr);
  if (OB_ISNULL(expr_column)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to dynamic cast", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && bret && i < expr_array.count(); i++) {
    ObProxyExpr *tmp_expr = expr_array.at(i);
    ObProxyExprType tmp_expr_type = tmp_expr->get_expr_type();
    if (OB_PROXY_EXPR_TYPE_STAR == tmp_expr_type) {
      ObString &table_name = expr_column->get_table_name();
      ObProxyExprStar* expr_star = dynamic_cast<ObProxyExprStar*>(tmp_expr);
      if (OB_ISNULL(expr_star)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to dynamic cast", K(ret));
      } else {
        ObString &tmp_table_name = expr_star->get_table_name();
        if (tmp_table_name.empty()) {
          // select * from t1, t3 order by c3;
          bret = false;
        } else if (tmp_table_name == table_name) {
          // bad case: select t1.*  from t1, t3 order by c3;
          // c3 may be a column in t3, so the table must be the same
          bret = false;
        }
      }
    }
  }

  return ret;
}

int ObShardingSelectLogPlan::do_column_need_derived_with_alias(ObIArray<ObProxyExpr*> &expr_array, ObProxyExpr *expr, bool &bret)
{
  int ret = OB_SUCCESS;
  bret = true;

  ObProxyExprColumn* expr_column = dynamic_cast<ObProxyExprColumn*>(expr);
  if (OB_ISNULL(expr_column)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to dynamic cast", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && bret && i < expr_array.count(); i++) {
    ObProxyExpr *tmp_expr = expr_array.at(i);
    ObString &column_name = expr_column->get_column_name();
    ObString &tmp_alias_name = tmp_expr->get_alias_name();
    if (!tmp_alias_name.empty()) {
      if (0 == tmp_alias_name.case_compare(column_name)) {
        bret = false;
        expr->set_alias_name(tmp_alias_name);
      }
    }
  }

  return ret;
}

int ObShardingSelectLogPlan::do_column_need_derived(ObIArray<ObProxyExpr*> &expr_array, ObProxyExpr *expr, bool &bret)
{
  int ret = OB_SUCCESS;
  bret = true;

  ObProxyExprColumn* expr_column = dynamic_cast<ObProxyExprColumn*>(expr);
  if (OB_ISNULL(expr_column)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to dynamic cast", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && bret && i < expr_array.count(); i++) {
    ObProxyExpr *tmp_expr = expr_array.at(i);
    ObProxyExprType tmp_expr_type = tmp_expr->get_expr_type();
    if (OB_PROXY_EXPR_TYPE_COLUMN == tmp_expr_type) {
      ObString &table_name = expr_column->get_table_name();
      ObString &column_name = expr_column->get_column_name();
      ObString &tmp_alias_name = tmp_expr->get_alias_name();
      ObProxyExprColumn* tmp_expr_column = dynamic_cast<ObProxyExprColumn*>(tmp_expr);
      if (OB_ISNULL(tmp_expr_column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to dynamic cast", K(ret));
      } else {
        ObString &tmp_column_name = tmp_expr_column->get_column_name();
        ObString &tmp_table_name = tmp_expr_column->get_table_name();
        if (tmp_table_name.empty() || table_name.empty()) {
          // When there is no table name in the select column:
          //   select c1 from t1, t3 order by t1.c1;
          //   select c1 from t1, t3 order by c1;
          //   If both t1 and t3 have c1 column, the execution will report an error
          // When there is no table name in the order column:
          //   select t1.c1 from t1, t3 order by c1
          //   This can be executed correctly, and it is also sorted according to the t1.c1 column
          if (0 == tmp_column_name.case_compare(column_name)) {
            bret = false;
            expr->set_alias_name(tmp_alias_name);
          }
        } else if ((tmp_table_name == table_name)
                   && (0 == tmp_column_name.case_compare(column_name))) {
          bret = false;
          expr->set_alias_name(tmp_alias_name);
        }
      }
    }
  }

  return ret;
}

int ObShardingSelectLogPlan::do_other_need_derived_for_avg(ObProxyExpr *tmp_expr, ObProxyExpr *expr)
{
  int ret = OB_SUCCESS;
  ObProxyExprType tmp_expr_type = tmp_expr->get_expr_type();

  if (OB_PROXY_EXPR_TYPE_FUNC_AVG != tmp_expr_type) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("avg expr match non avg expr", K(tmp_expr), K(expr), K(ret));
  } else {
    ObProxyExprAvg *avg_expr = NULL;
    ObProxyExprAvg *tmp_avg_expr = NULL;
    if (OB_ISNULL(avg_expr = dynamic_cast<ObProxyExprAvg *>(expr))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to dynamic cast", K(expr), K(ret));
    } else if (OB_ISNULL(tmp_avg_expr = dynamic_cast<ObProxyExprAvg *>(tmp_expr))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to dynamic cast", K(tmp_expr), K(ret));
    } else {
      avg_expr->set_sum_expr(tmp_avg_expr->get_sum_expr());
      avg_expr->set_count_expr(tmp_avg_expr->get_count_expr());
    }
  }
  return ret;
}

int ObShardingSelectLogPlan::do_other_need_derived(ObProxyExpr *tmp_expr, ObProxyExpr *expr, bool &bret)
{
  int ret = OB_SUCCESS;
  bret = true;

  ObString &expr_name = expr->get_expr_name();
  ObProxyExprType expr_type = expr->get_expr_type();
  ObProxyExprType tmp_expr_type = tmp_expr->get_expr_type();
  ObString &tmp_alias_name = tmp_expr->get_alias_name();

  if (0 == tmp_alias_name.case_compare(expr_name)) {
    bret = false;
    expr->set_alias_name(tmp_alias_name);
    if (OB_PROXY_EXPR_TYPE_FUNC_AVG == expr_type) {
      if (OB_FAIL(do_other_need_derived_for_avg(tmp_expr, expr))) {
        LOG_WARN("fail to do other need derived for avg", K(tmp_expr), K(expr), K(ret));
      }
    }
  } else if (OB_PROXY_EXPR_TYPE_SHARDING_CONST == tmp_expr_type || tmp_expr->is_func_expr()) {
    ObString &tmp_expr_name = tmp_expr->get_expr_name();
    if (0 == tmp_expr_name.case_compare(expr_name)) {
      bret = false;
      expr->set_alias_name(tmp_alias_name);
      if (OB_PROXY_EXPR_TYPE_FUNC_AVG == expr_type) {
        if (OB_FAIL(do_other_need_derived_for_avg(tmp_expr, expr))) {
          LOG_WARN("fail to do other need derived for avg", K(tmp_expr), K(expr), K(ret));
        }
      }
    }
  }

  return ret;
}

int ObShardingSelectLogPlan::do_need_derived(ObIArray<ObProxyExpr*> &expr_array, ObProxyExpr *expr, bool column_first, bool &bret)
{
  int ret = OB_SUCCESS;

  ObProxyExprType expr_type = expr->get_expr_type();
  bret = true;

  if (OB_PROXY_EXPR_TYPE_COLUMN == expr_type) {
    if (column_first) {
      if (OB_FAIL(do_column_need_derived(expr_array, expr, bret))) {
        LOG_WARN("fail to do column need derived", K(ret));
      } else if (bret && OB_FAIL(do_column_need_derived_with_alias(expr_array, expr, bret))) {
        LOG_WARN("fail to do column need derived with alias", K(ret));
      }
    } else {
      if (OB_FAIL(do_column_need_derived_with_alias(expr_array, expr, bret))) {
        LOG_WARN("fail to do column need derived with alias", K(ret));
      } else if (bret && OB_FAIL(do_column_need_derived(expr_array, expr, bret))) {
        LOG_WARN("fail to do column need derived", K(ret));
      }
    }

    if (bret && OB_FAIL(do_column_need_derived_with_star(expr_array, expr, bret))) {
      LOG_WARN("fail to do column need derived with star", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && bret && i < expr_array.count(); i++) {
      ObProxyExpr *tmp_expr = expr_array.at(i);
      if (OB_FAIL(do_other_need_derived(tmp_expr, expr, bret))) {
        LOG_WARN("fail to do other need derived", K(ret));
      }
    }
  }

  return ret;
}

int ObShardingSelectLogPlan::handle_derived(ObProxyExpr *expr, bool column_first /*false*/)
{
  int ret = OB_SUCCESS;
  bool bret = true;

  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr is NULL", K(ret));
  } else {
    ObProxyExprType expr_type = expr->get_expr_type();

    if (OB_PROXY_EXPR_TYPE_COLUMN == expr_type || OB_PROXY_EXPR_TYPE_SHARDING_CONST == expr_type || expr->is_func_expr()) {
      ObProxySelectStmt *select_stmt = static_cast<ObProxySelectStmt*>(client_request_.get_parse_result().get_proxy_stmt());
      ObIArray<ObProxyExpr*> &select_expr_array = select_stmt->select_exprs_;

      // Priority is given to finding derived columns, all derived columns have aliases and can be uniquely located
      if (bret && do_need_derived(derived_exprs_, expr, column_first, bret)) {
        LOG_WARN("fail to do need derived from select expr", K(ret));
      } else if (OB_FAIL(do_need_derived(select_expr_array, expr, column_first, bret))) {
        LOG_WARN("fail to do need derived from select expr", K(ret));
      } else if (bret) {
        if (OB_FAIL(add_derived_column(expr))) {
          LOG_WARN("fail to add derived column", K(ret));
        } else if (OB_FAIL(handle_agg_expr(expr))) {
          LOG_WARN("fail to handle agg expr", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObShardingSelectLogPlan::do_handle_select_derived(ObProxyExpr *expr, bool &bret, bool is_root)
{
  int ret = OB_SUCCESS;
  bret = false;

  if (expr->get_alias_name().empty()) {
    ObProxyExprType expr_type = expr->get_expr_type();
    if (expr->is_func_expr()) {
      ObProxyFuncExpr *func_expr = NULL;
      if (OB_ISNULL(func_expr = dynamic_cast<ObProxyFuncExpr*>(expr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to dynamic cast", K(expr), K(ret));
      } else {
        ObSEArray<ObProxyExpr*, 4>& param_array = func_expr->get_param_array();
        for (int64_t i = 0; OB_SUCC(ret) && !bret && i < param_array.count(); i++) {
          ObProxyExpr* param_expr = param_array.at(i);
          if (OB_FAIL(do_handle_select_derived(param_expr, bret))) {
            LOG_WARN("fail to handle derived", K(ret));
          }
        }
      }
    } else if (!is_root && OB_PROXY_EXPR_TYPE_COLUMN == expr_type) {
      ObProxyExprColumn* expr_column = dynamic_cast<ObProxyExprColumn*>(expr);
      if (OB_ISNULL(expr_column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to dynamic cast", K(expr), K(ret));
      } else {
        if (!expr_column->get_table_name().empty()
            && expr_column->get_table_name() == expr_column->get_real_table_name()) {
          bret = true;
        }
      }
    }
  }

  return ret;
}

int ObShardingSelectLogPlan::handle_select_derived(ObProxyExpr *expr)
{
  int ret = OB_SUCCESS;
  bool bret = false;

  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr is NULL", K(ret));
  } else if (OB_FAIL(do_handle_select_derived(expr, bret, true))) {
    LOG_WARN("fail to do handle select derived", K(ret));
  } else if (bret && OB_FAIL(add_derived_column(expr))) {
    LOG_WARN("fail to add derived column", K(ret));
  } else if (OB_FAIL(handle_agg_expr(expr, true))) {
    LOG_WARN("fail to handle agg expr", K(ret));
  }

  return ret;
}

int ObShardingSelectLogPlan::analyze_group_by_clause()
{
  int ret = OB_SUCCESS;

  ObProxySelectStmt *select_stmt = static_cast<ObProxySelectStmt*>(client_request_.get_parse_result().get_proxy_stmt());
  ObIArray<ObProxyGroupItem*> &group_by_exprs = select_stmt->group_by_exprs_;

  for (int64_t i = 0; OB_SUCC(ret) && i < group_by_exprs.count(); i++) {
    ObProxyGroupItem *group_item = dynamic_cast<ObProxyGroupItem *>(group_by_exprs.at(i));
    ObProxyExpr *expr = NULL;
    if (OB_ISNULL(group_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to dynamic cast", "expr", group_by_exprs.at(i), K(ret));
    } else if (OB_ISNULL(expr = group_item->get_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("group item expr is NULL", K(ret));
    } else if (OB_FAIL(handle_derived(expr, true))) {
      LOG_WARN("fail to handle derived", K(ret));
    }
  }

  return ret;
}

int ObShardingSelectLogPlan::analyze_order_by_clause()
{
  int ret = OB_SUCCESS;

  ObProxySelectStmt *select_stmt = static_cast<ObProxySelectStmt*>(client_request_.get_parse_result().get_proxy_stmt());
  ObIArray<ObProxyOrderItem*> &order_by_exprs = select_stmt->order_by_exprs_;

  for (int64_t i = 0; OB_SUCC(ret) && i < order_by_exprs.count(); i++) {
    ObProxyOrderItem *order_item = dynamic_cast<ObProxyOrderItem *>(order_by_exprs.at(i));
    ObProxyExpr *expr = NULL;
    if (OB_ISNULL(order_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to dynamic cast", "expr", order_by_exprs.at(i), K(ret));
    } else if (OB_ISNULL(expr = order_item->get_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("order item expr is NULL", K(ret));
    } else if (OB_FAIL(handle_derived(expr))) {
      LOG_WARN("fail to handle derived", K(ret));
    }
  }

  return ret;
}

int ObShardingSelectLogPlan::analyze_select_clause()
{
  int ret = OB_SUCCESS;
  ObProxySelectStmt *select_stmt = static_cast<ObProxySelectStmt*>(client_request_.get_parse_result().get_proxy_stmt());
  ObIArray<ObProxyExpr*> &select_expr_array = select_stmt->select_exprs_;

  for (int64_t i = 0; OB_SUCC(ret) && i < select_expr_array.count(); i++) {
    ObProxyExpr *select_expr = select_expr_array.at(i);
    if (OB_FAIL(handle_select_derived(select_expr))) {
      LOG_WARN("fail to handle select derived", K(ret));
    }
  }

  return ret;
}

int ObShardingSelectLogPlan::append_derived_order_by(bool &is_same_group_and_order)
{
  int ret = OB_SUCCESS;

  ObProxySelectStmt *select_stmt = static_cast<ObProxySelectStmt*>(client_request_.get_parse_result().get_proxy_stmt());
  ObIArray<ObProxyGroupItem*> &group_by_exprs = select_stmt->group_by_exprs_;
  ObIArray<ObProxyOrderItem*> &order_by_exprs = select_stmt->order_by_exprs_;

  if (!group_by_exprs.empty() && order_by_exprs.empty()) {
    is_same_group_and_order = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < group_by_exprs.count(); i++) {
      ObProxyGroupItem* group_expr = group_by_exprs.at(i);
      void *ptr = NULL;
      if (OB_ISNULL(ptr = allocator_->alloc(sizeof(ObProxyOrderItem)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc order expr buf", K(ret));
      } else {
        ObProxyExpr *expr = group_expr->get_expr();
        ObSqlString sql_string;
        char *buf = NULL;
        ObProxyOrderItem *order_expr = new (ptr) ObProxyOrderItem();
        order_expr->set_expr_type(OB_PROXY_EXPR_TYPE_FUNC_ORDER);
        order_expr->set_expr(expr);
        if (OB_FAIL(order_expr->to_sql_string(sql_string))) {
          LOG_WARN("fail to get sql string", K(ret));
        } else if (OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(sql_string.length())))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc sql string buf", K(ret));
        } else {
          MEMCPY(buf, sql_string.ptr(), sql_string.length());
          ObString derived_order(sql_string.length(), buf);

          if (OB_FAIL(order_by_exprs.push_back(order_expr))) {
            LOG_WARN("fail to push back order expr", K(ret));
          } else if (OB_FAIL(derived_orders_.push_back(derived_order))) {
            LOG_WARN("fail to add derived order by", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObShardingSelectLogPlan::rewrite_sql(ObSqlString &new_sql)
{
  int ret = OB_SUCCESS;
  ObProxySelectStmt *select_stmt = static_cast<ObProxySelectStmt*>(client_request_.get_parse_result().get_proxy_stmt());
  ObString sql = client_request_.get_sql();

  if (derived_columns_.empty() && derived_orders_.empty()
      && (select_stmt->limit_size_ == -1 || select_stmt->limit_size_ == 0)) {
    new_sql.append(sql);
  } else {
    const char *sql_ptr = sql.ptr();
    int64_t sql_len = sql.length();

    if (derived_columns_.empty() && derived_orders_.empty()) {
      int64_t limit_position = select_stmt->limit_token_off_;
      new_sql.append(sql_ptr, limit_position);
      new_sql.append("LIMIT ");
      if (select_stmt->limit_size_ == 0) {
        new_sql.append_fmt("%d", select_stmt->limit_size_);
      } else {
        new_sql.append_fmt("%d", select_stmt->limit_offset_ + select_stmt->limit_size_);
      }
    } else {
      int64_t from_position = select_stmt->get_from_token_off();
      int64_t limit_position = select_stmt->limit_token_off_;
      new_sql.append(sql_ptr, from_position);

      if (!derived_columns_.empty()) {
        new_sql.append(", ");
        int64_t derived_column_count = derived_columns_.count();
        for (int64_t i = 0; i < derived_column_count; i++) {
          ObString derived_column = derived_columns_.at(i);
          if (i == derived_column_count - 1) {
            new_sql.append(derived_column);
            new_sql.append(" ");
          } else {
            new_sql.append(derived_column);
            new_sql.append(", ");
          }
        }
      }

      if (limit_position > 0) {
        new_sql.append(sql_ptr + from_position, limit_position - from_position);
      } else {
        new_sql.append(sql_ptr + from_position, sql_len - from_position);
      }

      if (!derived_orders_.empty()) {
        // Bottom group by id; scenes with semicolons
        while (!new_sql.empty() && ';' == new_sql.ptr()[new_sql.length() - 1]) {
          new_sql.set_length(new_sql.length() - 1);
        }

        new_sql.append(" ORDER BY ");
        int64_t derived_order_by_count = derived_orders_.count();
        for (int64_t i = 0; i < derived_order_by_count; i++) {
          ObString derived_order_by = derived_orders_.at(i);
          if (i == derived_order_by_count - 1) {
            new_sql.append(derived_order_by);
            new_sql.append(" ");
          } else {
            new_sql.append(derived_order_by);
            new_sql.append(",");
          }
        }
      }

      if (limit_position > 0) {
        new_sql.append("LIMIT ");
        if (select_stmt->limit_size_ == 0) {
          new_sql.append_fmt("%d", select_stmt->limit_size_);
        } else {
          new_sql.append_fmt("%d", select_stmt->limit_offset_ + select_stmt->limit_size_);
        }
      }
    }
  }

  return ret;
}

template <typename T>
int ObShardingSelectLogPlan::add_agg_operator(bool need_set_limit)
{
  int ret = OB_SUCCESS;

  ObProxySelectStmt *select_stmt = static_cast<ObProxySelectStmt*>(client_request_.get_parse_result().get_proxy_stmt());
  ObIArray<ObProxyGroupItem*> &group_by_exprs = select_stmt->group_by_exprs_;

  T *agg_operator = NULL;
  ObProxyAggInput *agg_input = NULL;
  if (OB_FAIL(create_operator_and_input(allocator_, agg_operator, agg_input))) {
    LOG_WARN("create operator and input for agg failed", K(ret));
  } else if (OB_FAIL(agg_input->set_group_by_exprs(group_by_exprs))) {
    LOG_WARN("fail to set group exprs", K(ret));
  } else if (OB_FAIL(agg_input->set_agg_exprs(agg_exprs_))) {
    LOG_WARN("fail to set agg exprs", K(ret));
  } else if (OB_FAIL(agg_operator->set_child(0, plan_root_))) {
    LOG_WARN("set child failed", K(ret));
  } else {
    if (need_set_limit) {
      agg_input->set_limit_offset(select_stmt->limit_offset_);
      agg_input->set_limit_size(select_stmt->limit_size_);
      is_set_limit_ = true;
    }
    plan_root_ = agg_operator;
  }

  return ret;
}

int ObShardingSelectLogPlan::add_stream_agg_operator(bool need_set_limit)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(add_agg_operator<ObProxyStreamAggOp>(need_set_limit))) {
    LOG_WARN("fail to add stream agg", K(need_set_limit), K(ret));
  }

  return ret;
}

int ObShardingSelectLogPlan::add_mem_merge_agg_operator(bool need_set_limit)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(add_agg_operator<ObProxyMemMergeAggOp>(need_set_limit))) {
    LOG_WARN("fail to add mem merge agg", K(ret));
  }

  return ret;
}

template <typename T>
int ObShardingSelectLogPlan::add_sort_operator(bool need_set_limit)
{
  int ret = OB_SUCCESS;

  ObProxySelectStmt *select_stmt = static_cast<ObProxySelectStmt*>(client_request_.get_parse_result().get_proxy_stmt());
  ObIArray<ObProxyOrderItem*> &order_by_exprs = select_stmt->order_by_exprs_;

  T *sort_operator = NULL;
  ObProxySortInput *sort_input = NULL;
  if (OB_FAIL(create_operator_and_input(allocator_, sort_operator, sort_input))) {
    LOG_WARN("create operator and input for agg failed", K(ret));
  } else if (OB_FAIL(sort_input->set_order_by_exprs(order_by_exprs))) {
    LOG_WARN("fail to set order exprs", K(ret));
  } else if (OB_FAIL(sort_operator->set_child(0, plan_root_))) {
    LOG_WARN("set child failed", K(ret));
  } else {
    if (need_set_limit) {
      sort_input->set_limit_offset(select_stmt->limit_offset_);
      sort_input->set_limit_size(select_stmt->limit_size_);
      is_set_limit_ = true;
    }
    plan_root_ = sort_operator;
  }

  return ret;
}

int ObShardingSelectLogPlan::add_stream_sort_operator(bool need_set_limit)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(add_sort_operator<ObProxyStreamSortOp>(need_set_limit))) {
    LOG_WARN("fail to add mem merge agg", K(ret));
  }

  return ret;
}

int ObShardingSelectLogPlan::add_mem_merge_sort_operator(bool need_set_limit)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(add_sort_operator<ObProxyMemMergeSortOp>(need_set_limit))) {
    LOG_WARN("fail to add mem merge agg", K(ret));
  }

  return ret;
}

int ObShardingSelectLogPlan::add_agg_and_sort_operator(bool is_same_group_and_order)
{
  int ret = OB_SUCCESS;

  ObProxySelectStmt *select_stmt = static_cast<ObProxySelectStmt*>(client_request_.get_parse_result().get_proxy_stmt());
  ObIArray<ObProxyGroupItem*> &group_by_exprs = select_stmt->group_by_exprs_;
  ObIArray<ObProxyOrderItem*> &order_by_exprs = select_stmt->order_by_exprs_;

  if (!group_by_exprs.empty() || !agg_exprs_.empty()) {
    if (!is_same_group_and_order) {
      if (OB_FAIL(compare_group_and_order(is_same_group_and_order))) {
        LOG_WARN("fail to compare group and order", K(is_same_group_and_order), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (is_same_group_and_order) {
        if (OB_FAIL(add_stream_sort_operator(false))) {
          LOG_WARN("fail to add stream sort operator", K(ret));
        } else if (OB_FAIL(add_stream_agg_operator(true))) {
          LOG_WARN("fail to add stream agg operator", K(ret));
        }
      } else {
        if (OB_FAIL(add_mem_merge_agg_operator(false))) {
          LOG_WARN("fail to add mem merge agg operator", K(ret));
        } else if (OB_FAIL(add_mem_merge_sort_operator(true))) {
          LOG_WARN("fail to add mem merge sort operator", K(ret));
        }
      }
    }
  } else if (!order_by_exprs.empty()) {
    if (OB_FAIL(add_stream_sort_operator(true))) {
      LOG_WARN("fail to add stream sort operator", K(ret));
    }
  }

  return ret;
}

int ObShardingSelectLogPlan::add_table_scan_operator(ObIArray<dbconfig::ObShardConnector*> &shard_connector_array,
                                                     ObIArray<dbconfig::ObShardProp*> &shard_prop_array,
                                                     ObIArray<hash::ObHashMapWrapper<ObString, ObString> > &table_name_map_array,
                                                     ObSqlString &new_sql)
{
  int ret = OB_SUCCESS;
  const uint32_t PARSE_EXTRA_CHAR_NUM = 2;

  ObProxySelectStmt *select_stmt = static_cast<ObProxySelectStmt*>(client_request_.get_parse_result().get_proxy_stmt());
  ObProxyTableScanOp *table_scan_op = NULL;
  ObProxyTableScanInput *table_scan_input = NULL;
  char *buf = NULL;
  if (OB_FAIL(create_operator_and_input(allocator_, table_scan_op, table_scan_input))) {
    LOG_WARN("create operator and input failed", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(new_sql.length() + PARSE_EXTRA_CHAR_NUM)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc column string buf", K(ret));
  } else {
    MEMCPY(buf, new_sql.ptr(), new_sql.length());
    MEMSET(buf + new_sql.length(), '\0', PARSE_EXTRA_CHAR_NUM);
    ObString request_sql(new_sql.length() + PARSE_EXTRA_CHAR_NUM, buf);
    if (OB_FAIL(table_scan_input->set_db_key_names(shard_connector_array))) {
      LOG_WARN("fail to set db key name", K(ret));
    } else if (OB_FAIL(table_scan_input->set_shard_props(shard_prop_array))) {
      LOG_WARN("fail to set db key name", K(ret));
    } else if (OB_FAIL(table_scan_input->set_table_name_maps(table_name_map_array))) {
      LOG_WARN("fail to set table name", K(ret));
    } else if (OB_FAIL(table_scan_input->set_calc_exprs(calc_exprs_))) {
      LOG_WARN("fail to set calc expr", K(ret));
    } else if (OB_FAIL(table_scan_input->set_agg_exprs(agg_exprs_))) {
      LOG_WARN("fail to set agg expr", K(ret));
    } else if (OB_FAIL(table_scan_input->set_group_exprs(select_stmt->group_by_exprs_))) {
      LOG_WARN("fail to set group exprs", K(ret));
    } else if (OB_FAIL(table_scan_input->set_order_exprs(select_stmt->order_by_exprs_))) {
      LOG_WARN("fail to set order exprs", K(ret));
    } else {
      table_scan_input->set_request_sql(request_sql);
    }

    plan_root_ = table_scan_op;
  }

  return ret;
}

int ObShardingSelectLogPlan::add_projection_operator()
{
  int ret = OB_SUCCESS;
  ObProxySelectStmt *select_stmt = static_cast<ObProxySelectStmt*>(client_request_.get_parse_result().get_proxy_stmt());

  ObProxyProOp *op = NULL;
  ObProxyProInput *input = NULL;

  if (OB_FAIL(create_operator_and_input(allocator_, op, input))) {
    LOG_WARN("create projection failed", K(ret));
  } else {
    input->set_calc_exprs(calc_exprs_);
    input->set_derived_column_count(derived_column_count_);
    if (!is_set_limit_) {
      input->set_limit_offset(select_stmt->limit_offset_);
      input->set_limit_size(select_stmt->limit_size_);
      is_set_limit_ = true;
    }
    if (OB_FAIL(op->set_child(0, plan_root_))) {
      LOG_WARN("set child failed", K(ret));
    } else {
      plan_root_ = op;
    }
  }

  return ret;
}

int ObShardingSelectLogPlan::compare_group_and_order(bool &is_same_group_and_order)
{
  int ret = OB_SUCCESS;

  ObProxySelectStmt *select_stmt = static_cast<ObProxySelectStmt*>(client_request_.get_parse_result().get_proxy_stmt());
  ObIArray<ObProxyGroupItem*> &group_by_exprs = select_stmt->group_by_exprs_;
  ObIArray<ObProxyOrderItem*> &order_by_exprs = select_stmt->order_by_exprs_;

  if (!group_by_exprs.empty() && !order_by_exprs.empty()
      && group_by_exprs.count() == order_by_exprs.count()) {
    is_same_group_and_order = true;
    for (int64_t i = 0; is_same_group_and_order && i < group_by_exprs.count(); i++) {
      ObProxyExpr *group_expr = group_by_exprs.at(i)->get_expr();
      ObProxyExpr *order_expr = order_by_exprs.at(i)->get_expr();
      if (OB_ISNULL(group_expr) || OB_ISNULL(order_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("group child or order child is NULL", KP(group_expr), KP(order_expr), K(ret));
      } else {
        ObProxyExprType group_expr_type = group_expr->get_expr_type();
        ObProxyExprType order_expr_type = order_expr->get_expr_type();
        if (group_expr_type != order_expr_type) {
          is_same_group_and_order = false;
        } else if (OB_PROXY_EXPR_TYPE_COLUMN == group_expr_type) {
          ObProxyExprColumn* group_expr_column = dynamic_cast<ObProxyExprColumn*>(group_expr);
          ObProxyExprColumn* order_expr_column = dynamic_cast<ObProxyExprColumn*>(order_expr);
          if (OB_ISNULL(group_expr_column) || OB_ISNULL(order_expr_column)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to dynamic cast", KP(group_expr), KP(order_expr), K(ret));
          } else {
            ObString &group_column_name = group_expr_column->get_column_name();
            ObString &group_alias_name = group_expr_column->get_alias_name();
            ObString &order_column_name = order_expr_column->get_column_name();
            ObString &order_alias_name = group_expr_column->get_alias_name();
            if (0 != group_column_name.case_compare(order_column_name)
                || 0 != group_alias_name.case_compare(order_alias_name)) {
              is_same_group_and_order = false;
            }
          }
        } else if (OB_PROXY_EXPR_TYPE_SHARDING_CONST == group_expr_type
                   || group_expr->is_func_expr()) {
          ObString &group_expr_name = group_expr->get_expr_name();
          ObString &order_expr_name = order_expr->get_expr_name();
          if (0 != group_expr_name.case_compare(order_expr_name)) {
            is_same_group_and_order = false;
          }
        }
      }
    }
  } else {
    is_same_group_and_order = false;
  }

  return ret;
}

void ObShardingSelectLogPlan::print_plan_info()
{
  ObProxyOperator::print_execute_plan_info(plan_root_);
}

} // end optimizer
} // end obproxy
} // end oceanbase
