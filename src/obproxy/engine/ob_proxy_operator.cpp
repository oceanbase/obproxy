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
#include "iocore/eventsystem/ob_vconnection.h"
#include "iocore/eventsystem/ob_ethread.h"

#include "ob_proxy_operator.h"
#include "ob_proxy_operator_table_scan.h"
#include "ob_proxy_operator_async_task.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace engine
{

ObProxyOperator::ObProxyOperator(ObProxyOpInput *input, common::ObIAllocator &allocator)
          : input_(input), allocator_(allocator), parent_(NULL), children_(NULL),
            child_cnt_(0), child_max_cnt_(0), left_(NULL), right_(NULL),
            is_phy_operator_(false), column_count_(0),
            row_count_(0), projector_(NULL), type_(PHY_INVALID), cur_result_rows_(NULL),
            result_fields_(NULL), operator_async_task_(NULL), timeout_ms_(0),
            expr_has_calced_(false), result_(NULL)
{}

ObProxyOperator::~ObProxyOperator()
{
  LOG_DEBUG("Will Destruct Operator", K(op_name()));
  destruct_children();
  destruct_input();

  if (OB_NOT_NULL(operator_async_task_)) {
    operator_async_task_->destroy();
    operator_async_task_ = NULL;
  }
}

void ObProxyOperator::destruct_children()
{
  if (get_op_type() != PHY_TABLE_SCAN) { //skip TABLE_SCAN
    for (int64_t i = 0; i < child_cnt_; ++i) {
      if (OB_NOT_NULL(children_[i])) {
        children_[i]->~ObProxyOperator();
        children_[i] = NULL;
      }
    }
    child_cnt_ = 0;
  }
}

void ObProxyOperator::destruct_input()
{
  if (OB_NOT_NULL(input_)) {
    input_->~ObProxyOpInput();
    input_ = NULL;
  }
}

void ObProxyOperator::print_execute_plan_info(ObProxyOperator *op)
{
  char buf[1024 * 256];
  int32_t pos = 0;
  ObProxyOperator::print_execute_plan(op, 0, buf, pos, 256 * 1024);
  ObString trace_info(16 * 1024, buf);
  LOG_DEBUG("execute_plan_info: \n", K(trace_info));
}

int ObProxyOperator::set_children_pointer(ObProxyOperator **children, uint32_t child_cnt)
{
  int ret = common::OB_SUCCESS;
  if (child_cnt > 0 && NULL == children) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(op_name()), K(child_cnt), KP(children));
  } else {
    children_ = children;
    child_cnt_ = child_cnt;
    child_max_cnt_ = child_cnt;
    if (child_cnt > 0) {
      child_ = children[0];
    } else {
      child_ = NULL;
    }
    if (child_cnt > 1) {
      right_ = children[1];
    } else {
      right_ = NULL;
    }
  }
  return ret;
}

int ObProxyOperator::set_child(const uint32_t idx, ObProxyOperator *child)
{
  int ret = common::OB_SUCCESS;
  if (idx >= child_max_cnt_ || OB_ISNULL(child)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(op_name()), K(idx), K(child_max_cnt_), KP(child));
  } else if (idx > child_cnt_) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(op_name()), K(idx), K(child_cnt_), KP(child));
  } else {
    children_[idx] = child;
    if (0 == idx) {
      child_ = child;
    }
    if (1 == idx) {
      right_ = child;
    }
    child_->parent_ = this;
    child_cnt_ = (idx == child_cnt_) ? child_cnt_ + 1 : child_cnt_;
  }
  return ret;
}

ObProxyOperator* ObProxyOperator::get_child(const uint32_t idx)
{
  ObProxyOperator *op = NULL;
  int ret = common::OB_SUCCESS;
  if (idx >= child_cnt_ &&  PHY_TABLE_SCAN != type_) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(op_name()), K(idx), K(child_cnt_));
  } else if (PHY_TABLE_SCAN != type_) { //TableScan not has valid child
    op = children_[idx];
  }
  return op;
}

int ObProxyOperator::init()
{
  LOG_DEBUG("ObProxyOperator::init start", K(op_name()));
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(children_)) {
    if (0 >= child_max_cnt_) {
      child_max_cnt_ = OP_MAX_CHILDREN_NUM;
    }
    const int64_t alloc_size = child_max_cnt_ * sizeof(ObProxyOperator*);
    children_ = reinterpret_cast<ObProxyOperator **>(allocator_.alloc(alloc_size));
    if (OB_ISNULL(children_)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("no have enough memory to init", K(ret), K(op_name()), K(child_max_cnt_),
          K(alloc_size));
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(init_row_set(cur_result_rows_))) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WARN("init result_rows error", K(ret), K(op_name()));
  }

  LOG_DEBUG("ObProxyOperator::init end", K(ret), K(op_name()));
  return ret;
}

const char* ObProxyOperator::op_name()
{
  return get_op_name(get_op_type());
}

int ObProxyOperator::open(event::ObContinuation *cont, event::ObAction *&action, const int64_t timeout_ms)
{
  LOG_DEBUG("ObProxyOperator::open enter", K(op_name()), K(cont), K(timeout_ms));
  int ret = common::OB_SUCCESS;
  void *tmp_buf = NULL;
  action  = NULL;
  if (OB_ISNULL(cont)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input", K(ret));
  } else if (OB_ISNULL(tmp_buf = static_cast<char *>(allocator_.alloc(sizeof(ObOperatorAsyncCommonTask))))){
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem", K(ret));
  } else if (OB_ISNULL(operator_async_task_ = new (tmp_buf) ObOperatorAsyncCommonTask(child_cnt_, this))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to init ObOperatorAsyncCommonTask object", K(ret));
  } else {
    action = &operator_async_task_->get_action();
    if (OB_FAIL(operator_async_task_->init_async_task(cont, &self_ethread()))) {
      LOG_WARN("failed to init init ObOperatorAsyncCommonTask task", K(ret));
    } else if (get_op_type() != PHY_TABLE_SCAN) { //TABLE_SCAN operator not need to init child
      for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt_; i++) {
        if (OB_ISNULL(children_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid child to open", K(ret));
        } else if (OB_FAIL(children_[i]->open(operator_async_task_, operator_async_task_->parallel_action_array_[i], timeout_ms))) {
          LOG_WARN("open child operator failed", K(ret), K(children_[i]));
        } else if (OB_ISNULL(operator_async_task_->parallel_action_array_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("not get the action from child", K(ret), K(children_[i]), K(i)); 
        } else {
          operator_async_task_->add_target_task_count();
        }
      }
    }
  }
  timeout_ms_ = timeout_ms;
  return ret;
}

void ObProxyOperator::close()
{
  LOG_DEBUG("ObProxyOperator::close enter", K(op_name()));

  if (get_op_type() != PHY_TABLE_SCAN) { //skip PHY_TABLE_SCAN
    for (int64_t i = 0; i < child_cnt_; ++i) {
       if (OB_NOT_NULL(children_[i])) {
         children_[i]->close();
       }
    }
  }

  if (OB_NOT_NULL(operator_async_task_)) {
    operator_async_task_->cancel_all_pending_action();
  }

  LOG_DEBUG("ObProxyOperator::close exit", K(op_name()));
}

int ObProxyOperator::get_next_row() 
{
  LOG_DEBUG("ObProxyOperator::get_next_row enter", K(op_name()));
  int ret = common::OB_SUCCESS;
  /* children_ pointer is checked bofore operator open, no need check it again. */

  for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt_; ++i) {
    if (OB_FAIL(children_[i]->get_next_row())) {
      LOG_WARN("ObProxyProOp::get_next_row failed", K(ret));
      break;
    }
  }
  LOG_DEBUG("ObProxyOperator::get_next_row exit", K(ret), K(op_name()));
  return ret;
}

int ObProxyOperator::calc_result(ResultRow &row, ResultRow &result,
        common::ObIArray<ObProxyExpr*> &exprs, const int64_t added_row_count)
{
  int ret = common::OB_SUCCESS;
  if (result.count() > 0) {
    result.reset();
  }
  if (OB_ISNULL(&row) || row.count() == 0 || added_row_count < 0
         || row.count() <= added_row_count) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input for ObProxyOperator::cal_result", K(ret), K(row));
  } else if (expr_has_calced_) {
    int64_t expect_row_count = row.count() - added_row_count;
    for (int i = 0; i < expect_row_count; i++) {
      result.push_back(row.at(i));
    }
  } else {

    int64_t exprs_count = exprs.count();
    int64_t row_count = row.count();
    int64_t expect_row_count = row_count - added_row_count;
    int64_t star_row_count = row_count - exprs_count + 1;
    void *tmp_buf = NULL;
    ObObj *obj_array = NULL;
    LOG_DEBUG("calc_result info:", K(exprs_count), K(row_count), K(expect_row_count),
                      K(star_row_count));

    ObProxyExprCtx ctx(0, dbconfig::TESTLOAD_NON, false, &allocator_);
    ObProxyExprCalcItem calc_item(&row);
    ObProxyExpr *expr_ptr = NULL;
    ResultFields *field_info = NULL;
    if (OB_ISNULL(field_info = get_result_fields())) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WARN("ObProxyOperator::calc_result not have field info", K(ret));
    } else if (field_info->count() != row_count) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WARN("ObProxyOperator::calc_result have invalid field info", K(ret), K(row_count),
                      K(field_info->count()));
    }

    LOG_DEBUG("row display before calc:", K(row));
    if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(common::ObObj) * exprs_count))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret), K(sizeof(common::ObObj) * exprs_count));
    } else if (OB_ISNULL(obj_array = new (tmp_buf) common::ObObj[exprs_count])) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WARN("init ObObj Array error", K(ret));
    }

    int64_t k = 0;
    for (int64_t i = 0, j = 0; OB_SUCC(ret) && i < exprs_count && j < expect_row_count; j++) {
      OB_ASSERT(OB_NOT_NULL(expr_ptr = exprs.at(i)));
      ObSqlString str;
      ObProxyExpr::print_proxy_expr(expr_ptr); //DEBUG print
      expr_ptr->to_sql_string(str); //info for pint
      if (expr_ptr->is_star_expr()) {
        result.push_back(row.at(j));
        k++;
        if (k == star_row_count) { //Had push all ObObj of start_expr
          i++; //change to next expr
        }
      } else { //need calc result from optmizer
        ctx.set_scale(field_info->at(j).accuracy_.get_accuracy());
        result.push_back(obj_array + i); // result will be put into *(obj_array + i)
        if (OB_FAIL(expr_ptr->calc(ctx, calc_item, result))) {
          LOG_WARN("internal error when calc the value for the expr", K(ret), K(i), K(str));
        } else {
          i++;//to next expr
        }
        ctx.set_scale(-1); //set default for next
      }
      LOG_DEBUG("row display in calc:", K(i), K(j), K(result), K(str));
    }
    LOG_DEBUG("row display after calc:", K(result));
    LOG_DEBUG("success cal value of ObProxyOperator::cal_result", K(ret),
               K(result.count()));
    if (OB_SUCC(ret) && result.count() != expect_row_count) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WARN("ObProxyOperator::calc_result fail not get all result from expr->calc",
                   K(ret), K(result.count()), K(expect_row_count));
    }
  }
  return ret;
}

int ObProxyOperator::put_result_row(ResultRow *row)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input for ObProxyOperator::put_result_row", K(row));
  } else {
    cur_result_rows_->push_back(row);
  }
  return ret;
}

int ObProxyOperator::process_ready_data(void *data, int &event)
{
  int ret = OB_SUCCESS;
  ObProxyResultResp *result = NULL;
  ObProxyResultResp *res = NULL;
  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param, data is null", "op_name", op_name(), K(ret));
  } else if (OB_ISNULL(res = reinterpret_cast<ObProxyResultResp*>(data))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input, pres type is not match", K(ret), KP(data));
  } else if (OB_FAIL(handle_result(res, false, result))) {
    LOG_WARN("fail to handle_result", "op_name", op_name(), K(ret));
  } else if (OB_NOT_NULL(result) && result->is_error_resp()) {
    result_ = result;
    event = VC_EVENT_READ_COMPLETE;
  }

  LOG_DEBUG("finish process_ready_data", K(event), K(ret));
  return ret;
}

int ObProxyOperator::handle_result(void *data, bool is_final, ObProxyResultResp *&result)
{
  int ret = OB_SUCCESS;

  ObProxyResultResp *pres = NULL;
  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input, data is NULL", K(ret));
  } else if (OB_ISNULL(pres = reinterpret_cast<ObProxyResultResp*>(data))) { 
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input, pres type is not match", K(ret));
  } else {
    if (pres->is_resultset_resp()) {
      if (OB_FAIL(handle_response_result(pres, is_final, result))) {
        LOG_WARN("failed to handle resultset_resp", K(ret));
      }
    } else if (pres->is_error_resp() || pres->is_ok_resp()) {
      // The ok/error package only needs to be processed in the table_scan operator
      // In the non-table_scan operator, if the ok/error packet is received, it must be complete, just pass it through
      result = pres;
    }
    LOG_DEBUG("handle_result success", K(ret), K(pres));
  }
  return ret; 
}

int ObProxyOperator::handle_response_result(void *src, bool is_final, ObProxyResultResp *&result)
{
  UNUSED(src);
  UNUSED(is_final);
  UNUSED(result);
  return OB_SUCCESS;
}

int ObProxyOperator::process_complete_data(void *data)
{
  int ret = OB_SUCCESS;
  ObProxyResultResp *result = NULL;
  ObProxyResultResp *res = NULL;
  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param, data is null", K(ret));
  } else if (OB_ISNULL(res = reinterpret_cast<ObProxyResultResp*>(data))) {
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

inline void print_error_info(void *data, const char *name)
{
  ObProxyResultResp *result = NULL;
  if (OB_NOT_NULL(data) && OB_NOT_NULL(result = static_cast<ObProxyResultResp*>(data))) {
    LOG_WARN("handle error info", K(name), K(result->get_err_code()), K(result->is_error_resp()),
             K(result->get_err_msg()), K(result->get_err_msg().length()));
  }
}

int ObProxyOperator::put_row(ResultRow *&row)
{
  int ret = common::OB_SUCCESS;
  if (NULL == row) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invaild argument", K(row));
  } else {
    cur_result_rows_->push_back(row);
  }
  return ret;
}

int ObProxyOperator::init_row(ResultRow *&row)
{
  int ret = common::OB_SUCCESS;
  void *buf = NULL;
  row = NULL;
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ResultRow)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("no have enough memory to init", K(ret), K(op_name()), K(sizeof(ResultRow)));
  } else {
    row = new (buf) ResultRow(array_new_alloc_size, allocator_);
  }
  return ret;
}

int ObProxyOperator::init_row_set(ResultRows *&rows)
{
  int ret = common::OB_SUCCESS;
  void *buf = NULL;
  rows = NULL;
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ResultRows)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("no have enough memory to init", K(ret), K(op_name()), K(sizeof(ResultRows)));
  } else {
    rows = new (buf) ResultRows(array_new_alloc_size, allocator_);
  }
  return ret;
}

int ObProxyOperator::packet_result_set(ObProxyResultResp *&res, ResultRows *rows,
        ResultFields *fields)
{
  LOG_DEBUG("ObProxyOperator::packet_result_set enter", K(res), K(rows), K(fields));
  int ret = common::OB_SUCCESS;
  void *tmp_buf = NULL;
  if (OB_ISNULL(rows) || OB_ISNULL(fields)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("pack_result_set error", K(ret), K(op_name()), K(rows), K(fields));
  } else if(OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObProxyResultResp)))) {
    res = NULL;
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("no have enough memory to init", K(ret), K(op_name()), K(sizeof(ObProxyResultResp)));
  } else {
    res = new (tmp_buf) ObProxyResultResp(allocator_, get_cont_index());
    if (OB_FAIL(res->init_result(rows, fields))) {
          LOG_WARN("packet resultset packet error", K(ret));
      res->set_packet_flag(PCK_ERR_RESPONSE);
    } else {
      res->set_has_calc_exprs(expr_has_calced_);
    }
  }
  LOG_DEBUG("ObProxyOperator::packet_result_set exit", K(op_name()), K(ret), K(res->get_packet_flag()),
              K(expr_has_calced_));
  return ret;
}

int ObProxyOperator::packet_result_set_eof(ObProxyResultResp *&res)
{
  LOG_DEBUG("ObProxyOperator::packet_result_set_eof enter", K(res));
  int ret = common::OB_SUCCESS;
  void *tmp_buf = NULL;
  if(OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObProxyResultResp)))) {
    res = NULL;
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("no have enough memory to init", K(ret), K(op_name()), K(sizeof(ObProxyResultResp)));
  } else {
    res = new (tmp_buf) ObProxyResultResp(allocator_, get_cont_index());
    res->set_packet_flag(PCK_RESULTSET_EOF_RESPONSE);
    res->set_has_calc_exprs(expr_has_calced_);
  }
  LOG_DEBUG("ObProxyOperator::packet_result_set_eof exit", K(op_name()), K(ret), K(res),
             K(res->get_packet_flag()));
  return ret;
}

int ObProxyOperator::packet_error_info(ObProxyResultResp *&res, PacketErrInfo *err)
{
  int ret = common::OB_SUCCESS;
  void *tmp_buf = NULL;
  if (OB_ISNULL(err)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("pack_result_set error", K(ret), K(op_name()), K(err));
  } else if(OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObProxyResultResp)))) {
    res = NULL;
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("no have enough memory to init", K(ret), K(op_name()), K(sizeof(ObProxyResultResp)));
  } else {
    res = new (tmp_buf) ObProxyResultResp(allocator_, get_cont_index());
    res->set_err_info(err);
    LOG_WARN("MEM_MSG:", K(res));
    res->set_packet_flag(PCK_ERR_RESPONSE);
  }
  LOG_DEBUG("ObProxyOperator::packet_error_info exit", K(op_name()), K(ret), K(res->get_packet_flag()));
  return ret;
}

int ObProxyOperator::packet_error_info(ObProxyResultResp *&res, char *err_msg, int64_t err_msg_len,
                      uint16_t error_no)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(err_msg)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(err_msg), K(err_msg_len), K(error_no));
  } else {
    LOG_DEBUG("will be packet error packet:", K(err_msg), K(err_msg_len), K(error_no));
    res = NULL;
    void *err_buf = NULL;
    char *info_buf = NULL;
    err_buf = allocator_.alloc(sizeof(PacketErrorInfo));
    info_buf = static_cast<char*>(allocator_.alloc(err_msg_len + 1));

    if (OB_ISNULL(err_buf) || OB_ISNULL(info_buf)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("not allocat enougth memeory", K(sizeof(PacketErrorInfo)),
               K(err_msg_len + 1));
    } else {
      PacketErrorInfo *err_info = new (err_buf) PacketErrorInfo();
      err_info->error_code_ = error_no;

      MEMCPY(info_buf, err_msg, err_msg_len);
      info_buf[err_msg_len] = '\0';
      err_info->error_msg_.assign(info_buf, static_cast<ObString::obstr_size_t>(err_msg_len));
      ret = packet_error_info(res, err_info);
    }
  }
  return ret;
}

int ObProxyOperator::build_ok_packet(ObProxyResultResp *&res)
{
  int ret = OB_SUCCESS;
  void *tmp_buf = NULL;
  res = NULL;
  if(OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObProxyResultResp)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("no have enough memory to init", K(ret), "op_name", op_name());
  } else {
    res = new (tmp_buf) ObProxyResultResp(allocator_, get_cont_index());
    res->set_packet_flag(PCK_OK_RESPONSE);
  }
  LOG_DEBUG("ObProxyOperator::packet_result_set_eof exit",
            "op_name", op_name(), "packet flag", res->get_packet_flag(), K(ret));
  return ret;
}

void ObProxyOperator::set_op_type(ObPhyOperatorType type)
{
  type_ = type;
}

void ObProxyOpInput::set_op_limit(int64_t limit, int64_t offset)
{
  LOG_DEBUG("set_op_limit:", K(limit), K(offset));
  limit_ = limit;
  offset_ = offset;
}

}
}
}
