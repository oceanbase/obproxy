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

#include "lib/oblog/ob_log_module.h"
#include "ob_proxy_operator_table_scan.h"
#include "lib/string/ob_sql_string.h"
#include "utils/ob_proxy_utils.h" /* string_to_upper_case */
#include "iocore/eventsystem/ob_ethread.h"
#include "executor/ob_proxy_parallel_cont.h"
#include "executor/ob_proxy_parallel_execute_cont.h"
#include "proxy/shard/obproxy_shard_utils.h"
#include "proxy/mysqllib/ob_proxy_mysql_request.h"

using namespace oceanbase::obproxy::executor;
using namespace oceanbase::common;

namespace oceanbase {
namespace obproxy {
namespace engine {

ObProxyTableScanOp::~ObProxyTableScanOp()
{
  for (int64_t i = 0; i < pres_array_.count(); i++) {
    executor::ObProxyParallelResp *pres = pres_array_.at(i);
    op_free(pres);
    pres = NULL;
  }
}

int ObProxyTableScanOp::open(event::ObContinuation *cont, event::ObAction *&action, const int64_t timeout_ms)
{
  child_cnt_ = 1; //fake child
  int ret = ObProxyOperator::open(cont, action, timeout_ms);
  return ret;
}

ObProxyOperator* ObProxyTableScanOp::get_child(const uint32_t idx)
{
  int ret = common::OB_SUCCESS;
  if (child_cnt_ > 0) {
    ret = common::OB_ERROR;
    LOG_WDIAG("invalid TABLE_SCAN operator which has children operator", K(ret), K(idx),
        K(child_cnt_));
  }
  return NULL;
}

int ObProxyTableScanOp::get_next_row()
{
  int ret = OB_SUCCESS;

  ObProxyTableScanInput* input = NULL;
  common::ObSEArray<ObProxyParallelParam, 4> parallel_param;

  if (OB_ISNULL(ObProxyOperator::get_input())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("not have any input for table_scan", K(ret), KP(ObProxyOperator::get_input()));
  } else if (OB_ISNULL(input = dynamic_cast<ObProxyTableScanInput*>(ObProxyOperator::get_input()))) { 
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input for ObProxyTableScanOp", K(ret));
  } else if (OB_ISNULL(operator_async_task_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input for ObProxyTableScanOp", K(ret));
  } else {
    ObIArray<hash::ObHashMapWrapper<ObString, ObString> > &table_name_maps = input->get_table_name_maps();
    ObIArray<dbconfig::ObShardConnector*> &db_key_names = input->get_db_key_names();
    ObIArray<dbconfig::ObShardProp*> &shard_props = input->get_shard_props();
    ObString &request_sql = input->get_request_sql();
    obutils::ObProxySqlParser sql_parser;
    obutils::ObSqlParseResult parse_result;

    if (table_name_maps.count() != db_key_names.count()
        || shard_props.count() != db_key_names.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("inner error for sharding info",
               "phy table count", table_name_maps.count(),
               "shard prop count", shard_props.count(),
               "dbkey count", db_key_names.count(), K(ret));
    } else if (OB_FAIL(sql_parser.parse_sql_by_obparser(proxy::ObProxyMysqlRequest::get_parse_sql(request_sql),
                                                        NORMAL_PARSE_MODE, parse_result, true))) {
      LOG_WDIAG("parse_sql_by_obparser failed", K(request_sql), K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < db_key_names.count(); i++) {
      dbconfig::ObShardConnector *db_key_name = db_key_names.at(i);
      dbconfig::ObShardProp *shard_prop =  shard_props.at(i);
      hash::ObHashMapWrapper<ObString, ObString> &table_name_map_warraper = table_name_maps.at(i);
      ObSqlString new_sql;
      char *tmp_buf = NULL;
      bool is_oracle_mode = db_key_name->server_type_ == common::DB_OB_ORACLE;
      if (OB_FAIL(proxy::ObProxyShardUtils::rewrite_shard_dml_request(request_sql, new_sql, parse_result,
                                            is_oracle_mode, table_name_map_warraper.get_hash_map(),
                                            db_key_name->database_name_, false))) {
        LOG_WDIAG("fail to rewrite shard request", K(request_sql), K(is_oracle_mode), K(ret));
      } else if (OB_ISNULL(tmp_buf = (char *)allocator_.alloc(new_sql.length()))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("no have enough memory to init", K(op_name()), "sql len", new_sql.length(), K(ret));
      } else {
        MEMCPY(tmp_buf, new_sql.ptr(), new_sql.length());
        ObProxyParallelParam param;

        param.shard_conn_ = db_key_name;
        param.shard_prop_ = shard_prop;
        param.request_sql_.assign(tmp_buf, static_cast<ObString::obstr_size_t>(new_sql.length()));
        parallel_param.push_back(param);
      }
    }
  }

  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < parallel_param.count(); i++) {
      LOG_DEBUG("sub_sql before send", K(i), "sql", parallel_param.at(i).request_sql_,
                "database", parallel_param.at(i).shard_conn_->database_name_);
    }
    if (OB_FAIL(get_global_parallel_processor().open(*operator_async_task_, operator_async_task_->parallel_action_array_[0],
                                                     parallel_param, &allocator_, timeout_ms_))) {
      LOG_WDIAG("fail to op parallel processor", K(parallel_param));
    } else {
      set_sub_sql_count(parallel_param.count());
    }
  }

  return ret;
}

int ObProxyTableScanOp::handle_result(void *data, bool &is_final, ObProxyResultResp *&result)
{
  int ret = OB_SUCCESS;

  executor::ObProxyParallelResp *pres = NULL;
  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input, data is NULL", K(ret));
  } else if (OB_ISNULL(pres = reinterpret_cast<executor::ObProxyParallelResp*>(data))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input, pres type is not match", K(ret));
  } else if (OB_FAIL(pres_array_.push_back(pres))) {
    LOG_WDIAG("fail to push result resp to array", K(ret));
  } else {
    if (pres->is_ok_resp()) { // it is the OK packet
      // For the OK package, it is only necessary to construct an OK package in the case of
      // the last package, and the others can be swallowed.
      if (is_final) {
        if (OB_FAIL(build_ok_packet(result))) {
          LOG_WDIAG("fail to build_ok_packet", K(ret));
        }
      }
    } else if (pres->is_resultset_resp()) {
      if (OB_FAIL(handle_response_result(pres, is_final, result))) {
        LOG_WDIAG("failed to handle resultset_resp", K(ret));
      }
    } else if (pres->is_error_resp()) {
      if (OB_FAIL(packet_error_info(result, pres->get_err_msg().ptr(),
                                    pres->get_err_msg().length(), pres->get_err_code()))) {
        LOG_WDIAG("fail to error packet error", K(ret), K(result));
      }
    }
    LOG_DEBUG("handle_result success", K(ret), K(pres));
  }
  return ret;
}

int ObProxyTableScanOp::handle_response_result(void *data, bool &is_final, ObProxyResultResp *&result)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("Enter ObProxyTableScanOp::handle_response_result", K(op_name()), K(data));
  int64_t columns_length = 0;
  void *tmp_buf = NULL;
  executor::ObProxyParallelResp *pres = NULL;

  ObMysqlField *fields = NULL; //_array = NULL;
  ResultRows *rows = NULL;
  int64_t result_sum = 0;
  ResultRow *row = NULL;
  common::ObObj *row_ptr = NULL;

  if (OB_ISNULL(data)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input", K(ret), K(data));
  } else if (OB_ISNULL(pres = reinterpret_cast<executor::ObProxyParallelResp*> (data))) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input", K(ret), K(data));
  } else if (!pres->is_resultset_resp()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("ObProxyTableScanOp::handle_response_result not response result", K(pres), KP(pres), K(pres->is_resultset_resp()));
  } else if (OB_UNLIKELY((columns_length = pres->get_column_count()) <= 0)) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WDIAG("columns_length less than 0", K(columns_length), K(pres), K(ret));
  } else if (OB_ISNULL(fields = pres->get_field())) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WDIAG("fields is null", K(pres), K(ret));
  }

  if (OB_SUCC(ret) && OB_ISNULL(result_fields_)) {
    if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ResultFields)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("no have enough memory to init", K(ret), K(op_name()), K(sizeof(ResultFields)));
    } else if (OB_ISNULL(result_fields_ = new (tmp_buf) ResultFields(ENGINE_ARRAY_NEW_ALLOC_SIZE, allocator_))) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WDIAG("no have enough memory to init", K(ret), K(op_name()), K(sizeof(ResultFields)));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < columns_length; i++) {
        LOG_DEBUG("change field info", K(i), K(result_fields_));
        obmysql::ObMySQLField *obj_field = NULL;
        if (OB_FAIL(change_sql_field(fields, obj_field, allocator_))) {
          LOG_WDIAG("change field info failed", K(ret));
        } else {
          result_fields_->push_back(*obj_field);
          fields++;
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(set_index())) {
        LOG_WDIAG("fail to set index", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ResultRows)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("no have enough memory to init", K(ret), K(op_name()), K(sizeof(ResultRows)));
    } else {
      rows = new (tmp_buf) ResultRows(ENGINE_ARRAY_NEW_ALLOC_SIZE, allocator_);
    }
  }

  while(OB_SUCC(ret) && OB_SUCC(pres->next(row_ptr))) {
    if (NULL == row_ptr) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WDIAG("row prt is NULL", K(ret));
    } else if (OB_FAIL(init_row(row))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("no have enough memory to init", K(ret), K(op_name()), K(row));
    } else {
      result_sum++;

      row->reserve(columns_length);
      for (int64_t i = 0; i < columns_length; i++) {
        LOG_DEBUG("row object info--------------->", K(i), KPC(row_ptr), K(pres->get_cont_index()));
        row->push_back(row_ptr);
        row_ptr++;
      }
      rows->push_back(row);
      LOG_DEBUG("process_ready_data: get one result from server", K(op_name()), K(row), KPC(row));
    }
  }

  if (common::OB_ITER_END == ret) {
    ret = common::OB_SUCCESS;
  }

  LOG_DEBUG("ObProxyTableScanOp::process_ready_data get all rows", K(ret), K(result_sum),
            K(pres->get_cont_index()));
  all_resultset_rows_sum_ += result_sum;
  if (OB_UNLIKELY(is_final 
      && (all_resultset_rows_sum_ > obproxy::obutils::get_global_proxy_config().scan_buffered_rows_warning_threshold))) {
    int64_t all_resultset_rows_threshold = obproxy::obutils::get_global_proxy_config().scan_buffered_rows_warning_threshold;
    ObProxyTableScanInput* input = dynamic_cast<ObProxyTableScanInput*>(get_input());
    LOG_WDIAG("scan all request get resultset rows more than threshold", K_(all_resultset_rows_sum), K(all_resultset_rows_threshold), 
              K_(sub_sql_count), "request sql", input? input->request_sql_ : "NULL", K(ret));
  }

  ObProxyResultResp *res = NULL;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(packet_result_set(res, rows, get_result_fields()))) {
      LOG_WDIAG("process_ready_data:failed to packet resultset", K(op_name()), K(ret));
    } else if (OB_ISNULL(res)) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WDIAG("process_ready_data::packet_result_set success but res is NULL", K(ret), K(res));
    } else {
      res->set_result_sum(get_sub_sql_count());
      res->set_result_idx(pres->get_cont_index());
      LOG_DEBUG("ObProxyTableScanOp::process_ready_data sub_sql_count", K(ret), K(res->get_result_sum()), K(res));
    }
    result = res;
  }

  return ret;
}

int ObProxyTableScanOp::set_index()
{
  int ret = OB_SUCCESS;

  ObProxyTableScanInput* input = dynamic_cast<ObProxyTableScanInput*>(get_input());
  if (OB_FAIL(set_index_for_exprs(input->get_calc_exprs()))) {
    LOG_WDIAG("fail to set index for calc exprs", K(ret));
  } else if (OB_FAIL(set_index_for_exprs(input->get_agg_exprs()))) {
    LOG_WDIAG("fail to set index for agg exprs", K(ret));
  } else if (OB_FAIL(set_index_for_exprs(input->get_group_exprs()))) {
    LOG_WDIAG("fail to set index for group exprs", K(ret));
  } else if (OB_FAIL(set_index_for_exprs(input->get_order_exprs()))) {
    LOG_WDIAG("fail to set index for order exprs", K(ret));
  }

  return ret;
}

template <typename T>
int ObProxyTableScanOp::set_index_for_exprs(ObIArray<T*> &expr_array)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < expr_array.count(); i++) {
    T *expr = expr_array.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unexpected expr is null", K(ret));
    } else if (OB_FAIL(set_index_for_expr(expr))) {
      LOG_WDIAG("fail to set index", K(ret));
    }
  }

  return ret;
}

int ObProxyTableScanOp::set_index_for_expr(ObProxyExpr *expr)
{
  int ret = OB_SUCCESS;

  ObProxyExprType expr_type = expr->get_expr_type();

  if (OB_PROXY_EXPR_TYPE_FUNC_GROUP == expr_type || OB_PROXY_EXPR_TYPE_FUNC_ORDER == expr_type) {
    ObProxyGroupItem *group_expr = dynamic_cast<ObProxyGroupItem*>(expr);
    if (OB_ISNULL(group_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to dynamic cast", K(expr), K(ret));
    } else if (OB_ISNULL(expr = group_expr->get_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("group expr or order expr do not have child expr", KPC(group_expr), K(ret));
    } else {
      expr_type = expr->get_expr_type();
    }
  }

  if (OB_SUCC(ret) && -1 == expr->get_index()
      && (OB_PROXY_EXPR_TYPE_COLUMN == expr_type
          || OB_PROXY_EXPR_TYPE_SHARDING_CONST == expr_type
          || expr->is_func_expr())) {
    int64_t i = 0;
    for (; OB_SUCC(ret) && i < result_fields_->count(); ++i) {
      obmysql::ObMySQLField &field = result_fields_->at(i);
      ObString &alias_name = expr->get_alias_name();
      if (!alias_name.empty()) {
        if (0 == alias_name.case_compare(field.cname_)) {
          expr->set_index(i);
          expr->set_accuracy(field.accuracy_);
          break;
        }
      } else if (OB_PROXY_EXPR_TYPE_SHARDING_CONST == expr_type || expr->is_func_expr()) {
        ObString &expr_name = expr->get_expr_name();
        if (0 == expr_name.case_compare(field.cname_)) {
          expr->set_index(i);
          expr->set_accuracy(field.accuracy_);
          break;
        }
      } else if (OB_PROXY_EXPR_TYPE_COLUMN == expr_type) {
        ObProxyExprColumn *expr_column = dynamic_cast<ObProxyExprColumn*>(expr);
        if (OB_ISNULL(expr_column)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("fail to dynamic cast", K(expr), K(ret));
        } else {
          ObString &table_name = expr_column->get_table_name();
          ObString &column_name = expr_column->get_column_name();
          if ((table_name.empty() || field.tname_.prefix_case_match(table_name))
              && 0 == column_name.case_compare(field.cname_)) {
            expr->set_index(i);
            expr->set_accuracy(field.accuracy_);
            break;
          }
        }
      }
    }

    if (i == result_fields_->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to get column from field, mayby something error", K(expr), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_PROXY_EXPR_TYPE_FUNC_AVG == expr_type) {
      // Avg-dependent count and sum expressions also set index
      ObProxyExprAvg *avg_expr = dynamic_cast<ObProxyExprAvg*>(expr);
      ObProxyExprSum *sum_expr = NULL;
      ObProxyExprCount *count_expr = NULL;
      if (OB_ISNULL(avg_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to dynamic cast", K(expr), K(ret));
      } else if (OB_ISNULL(sum_expr = avg_expr->get_sum_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("avg expr don't have sum expr", K(avg_expr), K(ret));
      } else if (OB_ISNULL(count_expr = avg_expr->get_count_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("avg expr don't have count expr", K(avg_expr), K(ret));
      } else if (OB_FAIL(set_index_for_expr(sum_expr))) {
        LOG_WDIAG("fail to set index for sum expr", K(ret));
      } else if (OB_FAIL(set_index_for_expr(count_expr))) {
        LOG_WDIAG("fail to set index for count expr", K(ret));
      }
    } else if (expr->has_agg() && !expr->is_agg()) {
      // If the expression contains an aggregate, but it is not an aggregate function, 
      // set an index for its parameter, which is used to calculate
      ObProxyFuncExpr *func_expr = dynamic_cast<ObProxyFuncExpr*>(expr);
      if (OB_ISNULL(func_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to dynamic cast", K(expr), K(ret));
      } else {
        ObSEArray<ObProxyExpr*, 4>& param_array = func_expr->get_param_array();
        for (int64_t i = 0; OB_SUCC(ret) && i < param_array.count(); i++) {
          ObProxyExpr* param_expr = param_array.at(i);
          if (OB_FAIL(set_index_for_expr(param_expr))) {
            LOG_WDIAG("fail to set index for expr", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

ObProxyTableScanInput::~ObProxyTableScanInput()
{
  // free reference count
  for (int64_t i = 0; i < db_key_names_.count(); i++) {
    dbconfig::ObShardConnector *db_key_name = db_key_names_.at(i);
    db_key_name->dec_ref();
  }

  for (int64_t i = 0; i < shard_props_.count(); i++) {
    dbconfig::ObShardProp *shard_prop = shard_props_.at(i);
    shard_prop->dec_ref();
  }
}

}
}
}
