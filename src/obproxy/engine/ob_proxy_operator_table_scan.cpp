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

using namespace oceanbase::obproxy::executor;

namespace oceanbase {
namespace obproxy {
namespace engine {

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
    LOG_WARN("invalid TABLE_SCAN operator which has children operator", K(ret), K(idx),
        K(child_cnt_));
  }
  return NULL;
}

int ObProxyTableScanOp::rewrite_hint_table(const common::ObString &hint_string,
      common::ObSqlString &obj_hint_string,
      const common::ObString &table_name, const common::ObString &database_name,
      const common::ObString &real_table_name, const common::ObString &real_database_name,
      bool is_oracle_mode) {
  char* hint_buf = NULL;
  char* tmp_buf = NULL;
  int32_t db_table_len = table_name.length() > database_name.length()
                                ? table_name.length() : database_name.length();
  int32_t hint_len = hint_string.length();
  db_table_len++;
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(hint_string.ptr())) {
    //ret = common::OB_INVALID_ARGUMENT;
    LOG_DEBUG("not have any hint_stirng, not need to rewrite_hint_table.");
    //LOG_WARN("invalid input", K(hint_string));
  } else if (OB_ISNULL(hint_buf
            = reinterpret_cast<char*>(allocator_.alloc(hint_len + 1)))
           ||(OB_ISNULL(tmp_buf
            = reinterpret_cast<char*>(allocator_.alloc(db_table_len))))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("memory alloc error", K(ret), K(hint_string));
  } else {
    char* table_pos = NULL;
    char* db_pos = NULL;
    memcpy(hint_buf, hint_string.ptr(), hint_len);
    hint_buf[hint_string.length()] = '\0';
    string_to_upper_case(hint_buf, hint_len);
    memcpy(tmp_buf, table_name.ptr(), table_name.length());
    tmp_buf[table_name.length()] = '\0';
    table_pos = strstr(hint_buf, tmp_buf);
    if (OB_NOT_NULL(table_pos)) {
      LOG_DEBUG("found the table_name in hint_string", K(hint_string), K(table_name));
      int64_t pos = table_pos - hint_buf;
      // has db_name.`table_name` or db_name.table_name
      if (pos > 2 && (hint_buf[pos-1] == '.' || hint_buf[pos-2] == '.')) {
        memcpy(tmp_buf, database_name.ptr(), database_name.length());
        tmp_buf[database_name.length()] = '\0';
        db_pos = strstr(hint_buf, tmp_buf);
      }
      if (OB_NOT_NULL(db_pos)) {
        obj_hint_string.append(hint_string.ptr(), db_pos - hint_buf);
      }else {
        obj_hint_string.append(hint_string.ptr(), *(table_pos - 1) == '"' ? table_pos - 1 - hint_buf
            : table_pos - hint_buf);
      }
      if (is_oracle_mode) {
        obj_hint_string.append("\"", 1);
        obj_hint_string.append(real_database_name.ptr(), real_database_name.length());
        obj_hint_string.append("\"", 1);
      } else {
        obj_hint_string.append(real_database_name.ptr(), real_database_name.length());
      }

      obj_hint_string.append(".", 1);
      if (is_oracle_mode) {
        obj_hint_string.append("\"", 1);
        obj_hint_string.append(real_table_name.ptr(), real_table_name.length());
        obj_hint_string.append("\"", 1);
      } else {
        obj_hint_string.append(real_table_name.ptr(), real_table_name.length());
      }
      if (*(table_pos + table_name.length()) == '\"') {
        obj_hint_string.append(hint_string.ptr() + (table_pos + 1 - hint_buf), 
            hint_string.length() - (table_pos + 1 - hint_buf)); 
      } else {
        obj_hint_string.append(hint_string.ptr() + (table_pos - hint_buf), 
            hint_string.length() - (table_pos - hint_buf)); 
      }
    }
  }
  return ret;
}

int ObProxyTableScanOp::format_sql_header(ObSqlString &obj_sql_head)
{
  int ret = common::OB_SUCCESS;
  ObProxyTableScanInput* input = NULL;

  OB_ASSERT(OB_NOT_NULL(ObProxyOperator::get_input()));

  if (OB_ISNULL(input
        =  dynamic_cast<ObProxyTableScanInput*>(ObProxyOperator::get_input()))) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input for ObProxyTableScanOp");
  }

  if (OB_SUCC(ret)) {
    LOG_DEBUG("SELECT EXPR count", K(ret), K(input->get_select_exprs().count()));
    for (int64_t i = 0; i < input->get_select_exprs().count(); i++) {
      if (OB_ISNULL(input->get_select_exprs()[i])
          || OB_FAIL((int)(input->get_select_exprs()[i]->to_sql_string(obj_sql_head)))) {
        LOG_WARN("inner error when select exprs to sql_string", K(ret),
            K(input->get_select_exprs()[i]));
      }
      if (i + 1 != input->get_select_exprs().count()) {
        obj_sql_head.append(", ");
      }
    }

    obj_sql_head.append(" FROM ");
  }
  return ret;
}

int ObProxyTableScanOp::format_sql_tailer(ObSqlString &obj_sql_tail, bool is_oracle_mode)
{
  int ret = common::OB_SUCCESS;
  ObProxyTableScanInput* input = NULL;
  bool has_group_by = false;

  OB_ASSERT(OB_NOT_NULL(ObProxyOperator::get_input()));

  if (OB_ISNULL(input 
        =  dynamic_cast<ObProxyTableScanInput*>(ObProxyOperator::get_input()))) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input for ObProxyTableScanOp");
  }

  if(OB_SUCC(ret)) {
    if (OB_SUCC(ret) && input->get_condition_exprs().count() > 0) {
      obj_sql_tail.append(" WHERE ");
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < input->get_condition_exprs().count(); i++) {
      if (OB_ISNULL(input->get_condition_exprs()[i])
          || OB_FAIL((int)(input->get_condition_exprs()[i]->to_sql_string(obj_sql_tail)))) {
        LOG_WARN("inner error when condition exprs to sql_string",
            K(input->get_condition_exprs()[i]));
        break;
      }
    }

    /* GROUP BY col_a, col_b ORDER BY col_c, col_d */
    if (OB_SUCC(ret) && input->get_group_by_exprs().count() > 0) {
      obj_sql_tail.append(" GROUP BY ");
      has_group_by = true;
      for (int64_t i = 0; OB_SUCC(ret) && i < input->get_group_by_exprs().count(); i++) {
        if (OB_ISNULL(input->get_group_by_exprs()[i])
            || OB_FAIL((int)(input->get_group_by_exprs()[i]->to_sql_string(obj_sql_tail)))) {
          LOG_WARN("inner error when groupby exprs to sql_string",
              K(input->get_group_by_exprs()[i]));
        }
        if (i + 1 != input->get_group_by_exprs().count()) {
          obj_sql_tail.append(", ");
        }
      }
      if (!is_oracle_mode) {
        /* Add order by for GROUP BY columns for ObServer not sort by group by columns. */
        obj_sql_tail.append(" ORDER BY ");
        for (int64_t i = 0; OB_SUCC(ret) && i < input->get_group_by_exprs().count(); i++) {
          if (OB_ISNULL(input->get_group_by_exprs()[i])
              || OB_FAIL((int)(input->get_group_by_exprs()[i]->to_sql_string(obj_sql_tail)))) {
            LOG_WARN("inner error when groupby exprs to sql_string",
                K(input->get_group_by_exprs()[i]));
          }
          if (i + 1 != input->get_group_by_exprs().count()) {
            obj_sql_tail.append(", ");
          }
        }
      }
    } else if (OB_SUCC(ret) && input->get_order_by_exprs().count() > 0) {
      obj_sql_tail.append(" ORDER BY ");
      for (int64_t i = 0; OB_SUCC(ret) && i < input->get_order_by_exprs().count(); i++) {
        if (OB_ISNULL(input->get_order_by_exprs()[i])
            || OB_FAIL((int)(input->get_order_by_exprs()[i]->to_sql_string(obj_sql_tail)))) {
          LOG_WARN("inner error when order by exprs to sql_string", K(ret),
              K(input->get_order_by_exprs()[i]));
        }
        if (i + 1 != input->get_order_by_exprs().count()) {
          obj_sql_tail.append(", ");
        }
        LOG_DEBUG("ORDER BY add expr", K(ret), K(obj_sql_tail));
      }
    }

    int64_t value = -1;
    if (OB_SUCC(ret) && !has_group_by && -1 != (value = input->get_op_top_value())) {
      obj_sql_tail.append_fmt(" LIMIT %ld", value);
      int64_t start = input->get_op_limit_value();
      int64_t offset = input->get_op_limit_value();
      LOG_DEBUG("add LIMIT base on limit ", K(start), K(offset), K(value));
    }

  }

  return ret;
}

int ObProxyTableScanOp::get_next_row()
{
  int ret = common::OB_SUCCESS;

  ObProxyTableScanInput* input = NULL;

  if (OB_ISNULL(ObProxyOperator::get_input())) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("not have any input for table_scan", K(ret), KP(ObProxyOperator::get_input()));
  } else if (OB_ISNULL(input 
        =  dynamic_cast<ObProxyTableScanInput*>(ObProxyOperator::get_input()))) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input for ObProxyTableScanOp", K(ret));
  } else if (OB_ISNULL(operator_async_task_)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input for ObProxyTableScanOp", K(ret));
  }

  LOG_DEBUG("input limit value:", K(input->get_op_limit_value()), K(input->get_op_offset_value()));

  if (OB_SUCC(ret)) {
    /* init object sql for select */
    ObSqlString obj_sql_head;
    ObSqlString obj_sql_tail;

    if (OB_FAIL(format_sql_header(obj_sql_head))) {
      LOG_WARN("inner error to format sql base on exprs");
    } else if (input->get_phy_db_table_names().count()
        != input->get_db_key_names().count()) {
      ret = common::OB_ERROR;
      LOG_WARN("inner error for sharding info", K(input->get_phy_db_table_names().count()),
           K(input->get_db_key_names().count()));
    } else {
      common::ObSEArray<ObProxyParallelParam, 4> parallel_param;
      //void *tmp_buf = NULL;

      int64_t count = input->get_db_key_names().count();
      char *tmp_buf = NULL;
      ObSqlString obj_sql; // a temp ObSqlString object to use
      for (int64_t i = 0; i < count; i++) {
        bool is_oracle_mode = input->get_db_key_names().at(i)->server_type_ == common::DB_OB_ORACLE;
        obj_sql.reset();
        obj_sql.append(input->get_normal_annotation_string());
        /* Rewrite and add hint string */
        obj_sql.append("SELECT ");
        obj_sql.append(obj_sql_head.ptr());
        obj_sql_tail.reuse();
        if (OB_FAIL(format_sql_tailer(obj_sql_tail, is_oracle_mode))) {
          LOG_WARN("inner error to format sql base on exprs");
        } else if (OB_FAIL(rewrite_hint_table(input->get_hint_string(), obj_sql,
            //TODO get origin table_name and db_name
            input->get_logical_table_name(), input->get_logical_database_name(),
            input->get_phy_db_table_names().at(i), input->get_db_key_names().at(i)->database_name_,
            is_oracle_mode))) {
          LOG_WARN("internal error in rewrite_hint_table", K(ret),
              K(input->get_hint_string()));
        } else if (input->get_db_key_names().at(i)->database_name_.length() <= 0
                      || input->get_phy_db_table_names().at(i).length() <= 0) {
          ret = common::OB_INVALID_ARGUMENT;
          LOG_WARN("invilid table name or db_name", K(ret),
                   K(input->get_db_key_names().at(i)->database_name_),
                   K(input->get_phy_db_table_names().at(i)));
        } else {
          obj_sql.append(input->get_db_key_names().at(i)->database_name_.ptr(),
                         input->get_db_key_names().at(i)->database_name_.length());
          obj_sql.append(".");
          obj_sql.append(input->get_phy_db_table_names().at(i).ptr(),
                         input->get_phy_db_table_names().at(i).length());
          LOG_DEBUG("sub_sql table info", K(input->get_db_key_names().at(i)->database_name_.ptr()),
                     K(input->get_phy_db_table_names().at(i).ptr()));
          obj_sql.append(obj_sql_tail.ptr(), obj_sql_tail.length());

          if (OB_ISNULL(tmp_buf = (char *)allocator_.alloc(obj_sql.length() + 1))) {
            ret = common::OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("no have enough memory to init", K(ret), K(op_name()), K(obj_sql.length() + 1));
          }

          MEMCPY(tmp_buf, obj_sql.ptr(), obj_sql.length());
          tmp_buf[obj_sql.length()] = '\0';

          ObProxyParallelParam param;
          param.shard_conn_ = input->get_db_key_names().at(i);
          //char* tmp_p = obj_sql.ptr();
          param.request_sql_.assign(tmp_buf, static_cast<ObString::obstr_size_t>(obj_sql.length()));
          LOG_DEBUG("sub_sql and len:", K(param.request_sql_), K(obj_sql.length()));
          parallel_param.push_back(param);
        }
      }

      if (OB_SUCC(ret)) {
        for (int64_t i = 0; i < count; i++) {
          LOG_DEBUG("sub_sql before send", K(i), K(&parallel_param.at(i).request_sql_),
                    K(parallel_param.at(i).request_sql_), K(parallel_param.at(i).shard_conn_->database_name_));
        }
        if (OB_FAIL(get_global_parallel_processor().open(*operator_async_task_, operator_async_task_->parallel_action_array_[0],
                  parallel_param, &allocator_, timeout_ms_))) {
          LOG_WARN("fail to op parallel processor", K(parallel_param));
        } else {
          set_sub_sql_count(count);
        }
      }
    }
  }
  return ret;
}

int ObProxyTableScanOp::handle_result(void *data, bool is_final, ObProxyResultResp *&result)
{
  int ret = OB_SUCCESS;

  executor::ObProxyParallelResp *pres = NULL;
  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input, data is NULL", K(ret));
  } else if (OB_ISNULL(pres = reinterpret_cast<executor::ObProxyParallelResp*>(data))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input, pres type is not match", K(ret));
  } else {
    if (pres->is_ok_resp()) { // it is the OK packet
      // For the OK package, it is only necessary to construct an OK package in the case of
      // the last package, and the others can be swallowed.
      if (is_final) {
        if (OB_FAIL(build_ok_packet(result))) {
          LOG_WARN("fail to build_ok_packet", K(ret));
        }
      }
    } else if (pres->is_resultset_resp()) {
      if (OB_FAIL(handle_response_result(pres, is_final, result))) {
        LOG_WARN("failed to handle resultset_resp", K(ret));
      }
    } else if (pres->is_error_resp()) {
      if (OB_FAIL(packet_error_info(result, pres->get_err_msg().ptr(),
                                    pres->get_err_msg().length(), pres->get_err_code()))) {
        LOG_WARN("fail to error packet error", K(ret), K(result));
      }
    }
    LOG_DEBUG("handle_result success", K(ret), K(pres));
  }
  if (OB_NOT_NULL(pres)) {
    op_free(pres);
    pres = NULL;
  }
  return ret;
}

int ObProxyTableScanOp::handle_response_result(void *data, bool is_final, ObProxyResultResp *&result)
{
  UNUSED(is_final);
  int ret = OB_SUCCESS;
  LOG_DEBUG("Enter ObProxyTableScanOp::handle_response_result", K(op_name()), K(data));
  int64_t columns_length = 0;
  void *tmp_buf = NULL;
  executor::ObProxyParallelResp *pres = NULL;

  ObMysqlField *fields = NULL; //_array = NULL;
  UNUSED(data);
  UNUSED(is_final);

  if (OB_ISNULL(data)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input", K(ret), K(data));
  } else if (OB_ISNULL(pres = reinterpret_cast<executor::ObProxyParallelResp*> (data))) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input", K(ret), K(data));
  } else if (!pres->is_resultset_resp()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObProxyTableScanOp::handle_response_result not response result", K(pres), KP(pres), K(pres->is_resultset_resp()));
  } else {
    LOG_DEBUG("ObProxyTableScanOp::process_ready_data:resultset_resp", K(pres), KP(pres));

    common::ObObj *row_ptr = NULL;
    ResultRows *rows = NULL;
    if (OB_UNLIKELY((columns_length = pres->get_column_count()) <= 0)) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WARN("columns_length less than 0", K(columns_length), K(pres), K(ret));
    } else if (OB_ISNULL(fields = pres->get_field())) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WARN("fields is null", K(pres), K(ret));
    }

    if (OB_SUCC(ret) && OB_ISNULL(result_fields_)) {
      if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ResultFields)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("no have enough memory to init", K(ret), K(op_name()), K(sizeof(ResultFields)));
      } else if (OB_ISNULL(result_fields_ = new (tmp_buf) ResultFields(array_new_alloc_size, allocator_))) {
        ret = common::OB_ERR_UNEXPECTED;
        LOG_WARN("no have enough memory to init", K(ret), K(op_name()), K(sizeof(ResultFields)));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < columns_length; i++) {
          LOG_DEBUG("change field info", K(i), K(result_fields_));
          obmysql::ObMySQLField *obj_field = NULL;
          if (OB_FAIL(change_sql_field(fields, obj_field, allocator_))) {
            LOG_WARN("change field info failed", K(ret));
          } else {
            result_fields_->push_back(*obj_field);
            fields++;
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ResultRows)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("no have enough memory to init", K(ret), K(op_name()), K(sizeof(ResultRows)));
      } else {
        rows = new (tmp_buf) ResultRows(array_new_alloc_size, allocator_);
      }
    }

    int64_t result_sum = 0;
    ResultRow *row = NULL;

    while(OB_SUCC(ret) && OB_SUCC(pres->next(row_ptr))) {
      if (NULL == row_ptr) {
        ret = common::OB_ERR_UNEXPECTED;
        LOG_WARN("row prt is NULL", K(ret));
      } else if (OB_FAIL(init_row(row))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("no have enough memory to init", K(ret), K(op_name()), K(row));
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

    ObProxyResultResp *res = NULL;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(packet_result_set(res, rows, get_result_fields()))) {
        LOG_WARN("process_ready_data:failed to packet resultset", K(op_name()), K(ret));
      } else if (OB_ISNULL(res)) {
        LOG_WARN("process_ready_data::packet_result_set success but res is NULL", K(ret), K(res));
        ret = common::OB_ERR_UNEXPECTED;
      } else {
        res->set_result_sum(get_sub_sql_count());
        res->set_result_idx(pres->get_cont_index());
        LOG_DEBUG("ObProxyTableScanOp::process_ready_data sub_sql_count", K(ret), K(res->get_result_sum()), K(res));
      }
      result = res;
    }
  }
  LOG_DEBUG("Exit ObProxyTableScanOp::handle_result", K(ret), K(pres));
  return ret;
}

int ObProxyTableScanOp::process_ready_data(void *data, int &event)
{
  int ret = OB_SUCCESS;
  ObProxyResultResp *result = NULL;
  executor::ObProxyParallelResp *res = NULL;
  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param, data is null", "op_name", op_name(), K(ret));
  } else if (OB_ISNULL(res = reinterpret_cast<executor::ObProxyParallelResp*>(data))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input, pres type is not match", K(ret), KP(data));
  } else if (OB_FAIL(handle_result(res, false, result))) {
    LOG_WARN("fail to handle result", "op_name", op_name(), K(ret));
  } else if (OB_ISNULL(result)) {
    event = VC_EVENT_CONT;
  } else if (result->is_error_resp()) {
    result_ = result;
    event = VC_EVENT_READ_COMPLETE;
  } else {
    result_ = result;
    event = VC_EVENT_READ_READY;
  }
  LOG_DEBUG("finish process_ready_data", K(event), K(ret));
  return ret;
}

int ObProxyTableScanOp::process_complete_data(void *data)
{
  int ret = OB_SUCCESS;
  ObProxyResultResp *result = NULL;
  executor::ObProxyParallelResp *res = NULL;
  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param, data is null", K(ret));
  } else if (OB_ISNULL(res = reinterpret_cast<executor::ObProxyParallelResp*>(data))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input, pres type is not match", K(ret));
  } else if (OB_FAIL(handle_result(res, true, result))) {
    LOG_WARN("fail to handle result", K(ret));
  } else if (OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("reulst is NULL, something error", K(ret));
  } else {
    result_ = result;
  }

  LOG_DEBUG("finish process_complete_data", K(ret));
  return ret;
}

}
}
}
