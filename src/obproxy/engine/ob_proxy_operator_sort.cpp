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
#include "common/ob_obj_compare.h"
#include "ob_proxy_operator_sort.h"
#include "lib/container/ob_se_array_iterator.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;

namespace oceanbase {
namespace obproxy {
namespace engine {

ObProxySortOp::~ObProxySortOp()
{
  if (OB_NOT_NULL(sort_imp_)) {
    sort_imp_->~ObBaseSort();
    sort_imp_ = NULL;
  }
  if (OB_NOT_NULL(sort_columns_)) {
    sort_columns_->~SortColumnArray();
    sort_columns_ = NULL;
  }
}

int ObProxySortOp::init()
{
/* put init_sort_columns() to get_next_row() for input(exprs) maybe not set
 * when run the ObProxyAggOp::init by Optmizer.*/
  int ret = common::OB_SUCCESS;
  char *tmp_buf = NULL;
  ret = ObProxyOperator::init();
  if (OB_ISNULL(tmp_buf = (char *)allocator_.alloc(sizeof(SortColumnArray)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("init error for no memory to alloc", K(ret), K(sizeof(SortColumnArray)));
  } else if (OB_ISNULL(sort_columns_ = new (tmp_buf) SortColumnArray(ENGINE_ARRAY_NEW_ALLOC_SIZE, allocator_))) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WDIAG("init error for no memory to construct", K(ret), K(sort_columns_));
  }
  return ret;
}

/* For support ORDER BY complex expressions, we will calc and get new ObObj for
 * each expr in ORDER BY Exp. */
int ObProxySortOp::init_sort_columns()
{
  int ret = common::OB_SUCCESS;
  ObProxySortInput *input =
    dynamic_cast<ObProxySortInput*>(ObProxyOperator::get_input());

  if (OB_ISNULL(input)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WDIAG("input is invalid in ObProxySortOp", K(ret), K(input_));
  } else {
    const ObSEArray<ObProxyOrderItem*, 4> &order_by_expr = input->get_order_exprs();
    ObProxyOrderItem* expr_ptr = NULL;
    void *tmp_ptr = NULL;

    int64_t order_by_count = order_by_expr.count();

    LOG_DEBUG("ObProxySortOp::init_sort_columns", K(order_by_count), K(order_by_expr));
    for (int64_t i = 0; OB_SUCC(ret) && i < order_by_count; i++) {
      if (OB_ISNULL(expr_ptr = order_by_expr.at(i))) {
        ret = common::OB_ERROR;
        LOG_WDIAG("internal error in ObProxySortOp::init_sort_columns", K(ret), K(expr_ptr));
      } else {
        tmp_ptr = allocator_.alloc(sizeof(ObSortColumn));
        ObSortColumn *new_col= new (tmp_ptr) ObSortColumn();
        new_col->index_ = order_by_count - 1 - i;
        new_col->is_ascending_ = expr_ptr->order_direction_ <= NULLS_LAST_ASC;
        new_col->order_by_expr_ = expr_ptr;
        sort_columns_->push_back(new_col);
      }
    }
  }
  return ret;
}

int ObProxySortOp::add_order_by_obj(ResultRow *row)
{
  int ret = common::OB_SUCCESS;
  ResultRow *order_by_row = NULL;
  void *tmp_buf = NULL;
  common::ObObj *obj_array = NULL;
  if (OB_ISNULL(row)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WDIAG("ObProxySortOp::add_order_by_obj invalid input", K(ret));
  } else if (OB_FAIL(init_row(order_by_row))) {
      LOG_WDIAG("ObProxyProOp::add_order_by_obj init row error", K(ret));
  } else {
    int64_t sort_col_count = sort_columns_->count();

    if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(common::ObObj) * sort_col_count))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("alloc memory failed", K(ret), K(sizeof(common::ObObj) * sort_col_count));
    } else if (OB_ISNULL(obj_array = new (tmp_buf) common::ObObj[sort_col_count])) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WDIAG("init ObObj Array error", K(ret));
    }

    ObProxyExprCtx ctx(0, dbconfig::TESTLOAD_NON, false, &allocator_);
    ObProxyExprCalcItem calc_item(row);

    for (int64_t i = 0; OB_SUCC(ret) && i < sort_columns_->count(); i++) {
      order_by_row->push_back(obj_array + i); //will be push into *(obj_array + i)
      if (OB_ISNULL(sort_columns_->at(i))
              || OB_ISNULL(sort_columns_->at(i)->order_by_expr_)) {
        ret = common::OB_ERR_UNEXPECTED;
        LOG_WDIAG("ObProxyProOp::add_order_by_obj error", K(ret));
      } else if (OB_FAIL(sort_columns_->at(i)->order_by_expr_->calc(ctx, calc_item, *order_by_row))) {
        LOG_WDIAG("ObProxySortOp::add_order_by_obj calc order by expr error", K(ret), KP(row));
        ObProxyExpr::print_proxy_expr(sort_columns_->at(i)->order_by_expr_);
      }
    }
    if (order_by_row->count() != sort_columns_->count()) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WDIAG("ObProxySortOp::add_order_by_obj");
    }

    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < order_by_row->count(); i++) {
        row->push_back(order_by_row->at(i));
      }
      LOG_DEBUG("ObProxySortOp::add_order_by_obj has success add order by row", KP(row));
    } else {
      LOG_DEBUG("ObProxySortOp::add_order_by_obj has failed add order by row", K(ret), KP(row));
    }
  }
  return ret;
}

int ObProxySortOp::remove_all_order_by_objs(ResultRows &rows)
{
  int ret = common::OB_SUCCESS;
  ResultRow *row = NULL;
  for (int64_t j = 0; j < rows.count(); j++) {
    if (OB_ISNULL(row = rows.at(j))) {
      ret = common::OB_INVALID_ARGUMENT;
      LOG_WDIAG("ObProxySortOp::remove_order_by_obj invalid input", K(ret));
    } else if (row->count() < sort_columns_->count()) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WDIAG("ObProxySortOp::remove_order_by_obj error", K(ret), K(row->count()),
                   K(sort_columns_->count()));
    } else {
      LOG_DEBUG("ObProxySortOp::remove_order_by_obj before remove", KP(row), K(sort_columns_->count()));
      for (int64_t i = 0; OB_SUCC(ret) && i < sort_columns_->count(); i++) {
        if (OB_FAIL(row->remove(row->count() - 1))) {
          LOG_WDIAG("ObProxySortOp::remove_order_by_obj failed", K(row->count()), K(i));
        }
      }
      LOG_DEBUG("ObProxySortOp::remove_order_by_obj after remove", KP(row), K(sort_columns_->count()));
    }
  }
  return ret;
}

int ObProxySortOp::get_next_row()
{
  return ObProxyOperator::get_next_row();
}

int ObProxyMemSortOp::get_next_row()
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(init_sort_columns())) {
    LOG_WDIAG("ObProxyMemSortOp::get_next_row failed", K(ret));
  } else {
    void *tmp_buf = NULL;
    ResultRows *rows = NULL;
    if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ResultRows)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("no have enough memory to init", K(ret), K(op_name()), K(sizeof(ResultRows)));
    } else if (OB_ISNULL(rows = new (tmp_buf) ResultRows(ENGINE_ARRAY_NEW_ALLOC_SIZE, allocator_))) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WDIAG("init ResultRows failed", K(ret), K(op_name()), K(rows));
    } else if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObMemorySort)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("no have enough memory to init", K(ret), K(op_name()), K(sizeof(ObMemorySort)));
    } else {
      sort_imp_ = new (tmp_buf) ObMemorySort(*sort_columns_, allocator_, *rows);
      sort_imp_->set_topn_cnt(get_input()->get_op_top_value());
    }
  }

  if (OB_SUCC(ret)) {
    ret = ObProxySortOp::get_next_row();
  }

  return ret;
}

int ObProxyMemSortOp::handle_response_result(void *data, bool &is_final, ObProxyResultResp *&result)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("Enter ObProxyMemSortOp::handle_response_result", K(op_name()), K(data));

  ObProxyResultResp *opres = NULL;

  if (OB_ISNULL(data)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input", K(ret), K(data));
  } else if (OB_ISNULL(sort_imp_)) {
    ret = common::OB_ERROR;
    LOG_WDIAG("inner error sort_imp_ not init before to used.", K(ret));
  } else if (OB_ISNULL(opres = reinterpret_cast<ObProxyResultResp*>(data))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("ObProxyMemSortOp::handle_response_result not response result", K(data), KP(data));
  } else if (!opres->is_resultset_resp()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("ObProxyMemSortOp::handle_response_result not response result", K(opres), KP(opres), K(opres->is_resultset_resp()));
  } else {
    LOG_DEBUG("ObProxyMemSortOp::process_ready_data:resultset_resp", K(opres), KP(opres));
    expr_has_calced_ = opres->get_has_calc_exprs();

    if (OB_ISNULL(get_result_fields())) {
      result_fields_ = opres->get_fields();
    }
    ResultRow *row = NULL;
    int64_t i = 0;
    while (OB_SUCC(opres->next(row))) {
      if (OB_ISNULL(row)) {
        LOG_DEBUG("ObProxyMemSortOp::process_ready_data: handle all row", K(i));
        break;
      } else if (OB_FAIL(add_order_by_obj(row))) { // add obj for order by at backend
        LOG_WDIAG("ObProxyMemSortOp::process_ready_data add order by row error", K(ret), K(i));
        break;
      } else if (OB_FAIL(sort_imp_->add_row(row))) {
        LOG_WDIAG("inner error to put rows", K(ret), K(op_name()));
        break;
      }
      i++;
    }
    if (ret == common::OB_ITER_END) {
      ret = common::OB_SUCCESS;
    }
    LOG_DEBUG("ObProxyMemSortOp::process_ready_data: handle all row", K(i));
    if (is_final) {
      ObProxyResultResp *res = NULL;
      if (OB_FAIL(sort_imp_->sort_rows())) {
        LOG_WDIAG("memory sort error in ObProxySortOp", K(ret));
      } else if (OB_FAIL(sort_imp_->fetch_final_results(*cur_result_rows_))) {
        LOG_WDIAG("memory sort error in ObProxySortOp", K(ret));
      } else if (OB_FAIL(remove_all_order_by_objs(*cur_result_rows_))) {
        LOG_WDIAG("memory sort error in ObProxySortOp", K(ret));
      } else if (OB_FAIL(packet_result_set(res, cur_result_rows_, get_result_fields()))) {
        LOG_WDIAG("packet resultset packet error", K(ret));
      }
      LOG_DEBUG("ObProxyMemSortOp::process_complete_data end mem sort", K(sort_imp_->get_sort_rows()), K(sort_imp_->get_sort_rows().count()));
      LOG_DEBUG("ObProxyMemSortOp::process_complete_data", K(cur_result_rows_));

      if (OB_FAIL(ret) && OB_NOT_NULL(res)) {
        res->set_packet_flag(PCK_ERR_RESPONSE);
      }
      result = res;
    }
  }
  return ret;
}

int ObProxyTopKOp::get_next_row()
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(init_sort_columns())) {
    LOG_WDIAG("ObProxyTopKOp::get_next_row failed", K(ret));
  } else {
    void *tmp_buf = NULL;
    ResultRows *rows = NULL;
    if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ResultRows)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("no have enough memory to init", K(ret), K(op_name()), K(sizeof(ResultRows)));
    } else if (OB_ISNULL(rows = new (tmp_buf) ResultRows(ENGINE_ARRAY_NEW_ALLOC_SIZE, allocator_))) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WDIAG("init ResultRows failed", K(ret), K(op_name()), K(rows));
    } else if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObTopKSort)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("no have enough memory to init", K(ret), K(op_name()),
            K(sizeof(ObTopKSort)));
    } else {
      sort_imp_ = new (tmp_buf) ObTopKSort(*sort_columns_, allocator_, *rows,  *cur_result_rows_);
      sort_imp_->set_topn_cnt(get_input()->get_op_top_value());
    }
  }
  return ObProxySortOp::get_next_row();
}

int ObProxyTopKOp::handle_response_result(void *data, bool &is_final, ObProxyResultResp *&result)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("Enter ObProxyMemSortOp::handle_response_result", K(op_name()), K(data));

  ObProxyResultResp *opres = NULL;

  if (OB_ISNULL(data)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input", K(ret), K(data));
  } else if (OB_ISNULL(sort_imp_)) {
    ret = common::OB_ERROR;
    LOG_WDIAG("inner error sort_imp_ not init before to used.", K(ret));
  } else if (OB_ISNULL(opres = reinterpret_cast<ObProxyResultResp*>(data))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("ObProxyMemSortOp::handle_response_result not response result", K(opres), KP(opres));
  } else if (!opres->is_resultset_resp()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("ObProxyMemSortOp::handle_response_result not response result", K(opres), KP(opres), K(opres->is_resultset_resp()));
  } else {
    LOG_DEBUG("ObProxyMemSortOp::process_ready_data:resultset_resp", K(opres), KP(opres));
    expr_has_calced_ = opres->get_has_calc_exprs();

    if (OB_ISNULL(get_result_fields())) {
      result_fields_ = opres->get_fields();
    }
    ResultRow *row = NULL;
    int64_t i = 0;
    while (OB_SUCC(opres->next(row))) {
      if (OB_ISNULL(row)) {
        LOG_DEBUG("ObProxyMemSortOp::process_ready_data: handle all row", K(i));
        break;
      } else if (OB_FAIL(add_order_by_obj(row))) { // add obj for order by at backend
        LOG_WDIAG("ObProxyMemSortOp::process_ready_data add order by row error", K(ret), K(i));
        break;
      } else if (OB_FAIL((dynamic_cast<ObTopKSort*>(get_sort_impl()))->sort_rows(*row))) {
        LOG_WDIAG("inner error to put rows", K(ret), K(op_name()));
        break;
      }
      i++;
    }
    if (ret == common::OB_ITER_END) {
      ret = common::OB_SUCCESS;
    }
    LOG_DEBUG("ObProxyMemSortOp::process_ready_data: handle all row", K(i));
    if (is_final) {
      ObProxyResultResp *res = NULL;
      if (OB_FAIL(sort_imp_->sort_rows())) {
        LOG_WDIAG("topK sort error in ObProxySortOp", K(ret));
      } else if (OB_FAIL(dynamic_cast<ObTopKSort*>(sort_imp_)->fetch_final_results())) {
        LOG_WDIAG("topK sort error in ObProxySortOp", K(ret));
      } else if (OB_FAIL(remove_all_order_by_objs(*cur_result_rows_))) {
        LOG_WDIAG("memory sort error in ObProxySortOp", K(ret));
      } else if (OB_FAIL(packet_result_set(res, cur_result_rows_, get_result_fields()))) {
        LOG_WDIAG("packet resultset packet error", K(ret));
      }

      if (OB_FAIL(ret) && OB_NOT_NULL(res)) {
        res->set_packet_flag(PCK_ERR_RESPONSE);
      }
      result = res;
    }
  }
  return ret;
}

ObProxyMemMergeSortOp::~ObProxyMemMergeSortOp()
{
  for (int64_t i = 0; i < sort_units_.count(); i++) {
    ObProxyMemMergeSortUnit *sort_unit = sort_units_.at(i);
    sort_unit->~ObProxyMemMergeSortUnit();
    allocator_.free(sort_unit);
  }
}

int ObProxyMemMergeSortOp::handle_response_result(void *data, bool &is_final, ObProxyResultResp *&result)
{
  int ret = OB_SUCCESS;

  ObProxyResultResp *opres = NULL;
  ObProxySortInput *input = NULL;

  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input, data is NULL", K(ret));
  } else if (OB_ISNULL(opres = reinterpret_cast<ObProxyResultResp*>(data))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid input, opres type is not match", K(ret));
  } else if (!opres->is_resultset_resp()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("resp is not resultset", K(opres), K(ret));
  } else if (OB_ISNULL(input = dynamic_cast<ObProxySortInput*>(get_input()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("input is invalid", K(ret));
  } else {
    if (OB_ISNULL(get_result_fields())) {
      result_fields_ = opres->get_fields();
    }

    ResultRow *row = NULL;
    while (OB_SUCC(ret) && OB_SUCC(opres->next(row))) {
      void *tmp_buf = NULL;
      ObProxyMemMergeSortUnit *sort_unit = NULL;
      if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObProxyMemMergeSortUnit)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("no have enough memory to init", "size", sizeof(ObProxyMemMergeSortUnit), K(ret));
      } else if (OB_ISNULL(sort_unit = new (tmp_buf) ObProxyMemMergeSortUnit(allocator_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to new merge sort unit", K(ret));
      } else if (OB_FAIL(sort_unit->init(row, result_fields_, input->get_order_exprs()))) {
        LOG_WDIAG("fail to init sort unit", K(ret));
      } else if (OB_FAIL(sort_units_.push_back(sort_unit))) {
        LOG_WDIAG("fail to push back sort unit", K(ret));
      }
    }

    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }
  }

  if (OB_SUCC(ret) && is_final) {
    int64_t limit_offset = get_input()->get_limit_offset();
    int64_t limit_offset_size = get_input()->get_limit_size() + limit_offset;
    ObProxyResultResp *res = NULL;
    ResultRows *rows = NULL;
    void *tmp_buf = NULL;
    if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ResultRows)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("no have enough memory to init", "size", sizeof(ResultRows), K(ret));
    } else {
      rows = new (tmp_buf) ResultRows(ENGINE_ARRAY_NEW_ALLOC_SIZE, allocator_);
      std::sort(sort_units_.begin(), sort_units_.end(), ObProxySortUnitCompare<ObProxyMemMergeSortUnit>());
    }

    int64_t count = sort_units_.count();
    if (limit_offset_size > 0) {
      count = count > limit_offset_size ? limit_offset_size : count;
    }

    for (int64_t i = limit_offset; OB_SUCC(ret) && i < count; i++) {
      ObProxyMemMergeSortUnit *&sort_unit = sort_units_.at(i);

      ResultRow *row = sort_unit->get_row();
      if (OB_FAIL(rows->push_back(row))) {
        LOG_WDIAG("fail to push back row", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(packet_result_set(res, rows, get_result_fields()))) {
        LOG_WDIAG("fail to packet resultset", K(op_name()), K(ret));
      }
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(res)) {
      res->set_packet_flag(PCK_ERR_RESPONSE);
    }
    result = res;
  }

  return ret;
}

int ObProxyMemMergeSortUnit::init(ResultRow *row, ResultFields *result_fields, ObIArray<ObProxyOrderItem*> &order_exprs)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(order_exprs_.assign(order_exprs))) {
    LOG_WDIAG("fail to assign order exprs", K(ret));
  } else {
    row_ = row;
    result_fields_ = result_fields;
    if (OB_FAIL(calc_order_values())) {
      LOG_WDIAG("fail to get order value", K(ret));
    }
  }

  return ret;
}

int ObProxyMemMergeSortUnit::calc_order_values()
{
  int ret = OB_SUCCESS;

  ObProxyExprCtx ctx(0, dbconfig::TESTLOAD_NON, false, &allocator_);
  ObProxyExprCalcItem calc_item(row_);
  order_values_.reuse();

  for (int64_t i = 0; OB_SUCC(ret) && i < order_exprs_.count(); i++) {
    if (OB_FAIL(order_exprs_.at(i)->calc(ctx, calc_item, order_values_))) {
      LOG_WDIAG("fail to calc order exprs", K(ret));
    } else {
      int64_t index = order_exprs_.at(i)->get_expr()->get_index();
      if (-1 != index) {
        ObObj &value = order_values_.at(i);
        if (OB_FAIL(change_sql_value(value, result_fields_->at(index), &allocator_))) {
          LOG_WDIAG("fail to change sql value", K(value),
                   "filed", result_fields_->at(index), K(ret));
        }
      }
    }
  }

  return ret;
}

// Return true to come first
bool ObProxyMemMergeSortUnit::compare(const ObProxyMemMergeSortUnit* sort_unit) const
{
  bool bret = true;
  const ObIArray<ObObj> &order_values = sort_unit->get_order_values();
  int i = 0;
  int64_t count_this = order_values_.count();
  int64_t count_other = order_values.count();
  int64_t count = count_this <= count_other ? count_this : count_other;

  for (; i < count; i++) {
    int cmp = order_values_.at(i).compare(order_values.at(i));
    ObProxyOrderItem *order_expr = order_exprs_.at(i);
    if (0 != cmp) {
      if (order_expr->order_direction_ == NULLS_FIRST_ASC) {
        bret = cmp < 0;
      } else {
        bret = cmp > 0;
      }
      break;
    }
  }

  if (i == count) {
    bret = count_this <= count_other;
  }

  return bret;
}

ObProxyStreamSortOp::~ObProxyStreamSortOp()
{
  for (int64_t i = 0; i < sort_units_.count(); i++) {
    ObProxyStreamSortUnit *sort_unit = sort_units_.at(i);
    sort_unit->~ObProxyStreamSortUnit();
    allocator_.free(sort_unit);
  }
}

int ObProxyStreamSortOp::handle_response_result(void *data, bool &is_final, ObProxyResultResp *&result)
{
  int ret = OB_SUCCESS;

  ObProxyResultResp *opres = NULL;
  ObProxySortInput *input = NULL;

  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input, data is NULL", K(ret));
  } else if (OB_ISNULL(opres = reinterpret_cast<ObProxyResultResp*>(data))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid input, opres type is not match", K(ret));
  } else if (!opres->is_resultset_resp()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("resp is not resultset", K(opres), K(ret));
  } else if (OB_ISNULL(input = dynamic_cast<ObProxySortInput*>(get_input()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("input is invalid", K(ret));
  } else {
    if (OB_ISNULL(get_result_fields())) {
      result_fields_ = opres->get_fields();
    }

    ResultRows &result_rows = opres->get_result_rows();
    if (!result_rows.empty()) {
      void *tmp_buf = NULL;
      ObProxyStreamSortUnit *sort_unit = NULL;
      if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObProxyStreamSortUnit)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("no have enough memory to init", "size", sizeof(ObProxyStreamSortUnit), K(ret));
      } else if (OB_ISNULL(sort_unit = new (tmp_buf) ObProxyStreamSortUnit(allocator_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to new merge sort unit", K(ret));
      } else if (OB_FAIL(sort_unit->init(opres, result_fields_, input->get_order_exprs()))) {
        LOG_WDIAG("fail to init sort unit", K(ret));
      } else if (OB_FAIL(sort_units_.push_back(sort_unit))) {
        LOG_WDIAG("fail to push back sort unit", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && is_final) {
    int64_t limit_offset = input->get_limit_offset();
    int64_t limit_size = input->get_limit_size();
    ObProxyResultResp *res = NULL;
    ResultRows *rows = NULL;
    void *tmp_buf = NULL;
    if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ResultRows)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("no have enough memory to init", "size", sizeof(ResultRows), K(ret));
    } else {
      rows = new (tmp_buf) ResultRows(ENGINE_ARRAY_NEW_ALLOC_SIZE, allocator_);
    }

    while (OB_SUCC(ret) && !sort_units_.empty()) {
      // sort
      // Flashback according to expectations, such as positive order,
      // put the minimum value at the end, which is convenient for pop_back
      std::sort(sort_units_.begin(), sort_units_.end(), ObProxySortUnitCompare<ObProxyStreamSortUnit>());
      ObProxyStreamSortUnit *sort_unit = sort_units_.at(sort_units_.count() - 1);

      ResultRow *row = sort_unit->get_row();

      if (limit_offset > 0) {
        limit_offset--;
      } else {
        if (OB_FAIL(rows->push_back(row))) {
          LOG_WDIAG("failed to push back row", K(ret));
        } else if (limit_size > 0 && rows->count() == limit_size) {
          break;
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(sort_unit->next())) {
          if (OB_ITER_END == ret) {
            if (OB_FAIL(sort_units_.pop_back(sort_unit))) {
              LOG_WDIAG("fail to pop back sort unit", K(ret));
            } else {
              sort_unit->~ObProxyStreamSortUnit();
              allocator_.free(sort_unit);
            }
          } else {
            LOG_WDIAG("fail to exec next", K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(packet_result_set(res, rows, get_result_fields()))) {
        LOG_WDIAG("fail to packet resultset", K(op_name()), K(ret));
      }
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(res)) {
      res->set_packet_flag(PCK_ERR_RESPONSE);
    }

    result = res;
  }

  return ret;
}

int ObProxyStreamSortUnit::init(ObProxyResultResp* result_set, ResultFields *result_fields,
                                common::ObIArray<ObProxyOrderItem*> &order_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(order_exprs_.assign((order_exprs)))) {
    LOG_WDIAG("fail to assign order expr", K(ret));
  } else if (OB_ISNULL(result_set_ = result_set)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("result set should not NULL", K(ret));
  } else if (OB_ISNULL(result_fields_ = result_fields)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("result fields should not NULL", K(ret));
  } else if (OB_FAIL(next())) {
    // The first time, there must be a result, so it must not be OB_ITER_END
    LOG_WDIAG("fail to exec next", K(ret));
  }
  return ret;
}

bool ObProxyStreamSortUnit::compare(const ObProxyStreamSortUnit* sort_unit) const
{
  bool bret = ObProxyMemMergeSortUnit::compare(sort_unit);
  return !bret;
}

int ObProxyStreamSortUnit::next()
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(result_set_->next(row_))) {
    if (OB_FAIL(calc_order_values())) {
      LOG_WDIAG("fail to get order value", K(ret));
    }
  }

  return ret;
}

ObBaseSort::ObBaseSort(SortColumnArray &sort_columns, common::ObIAllocator &allocator, ResultRows &sort_rows)
               : allocator_(allocator), sort_columns_(sort_columns),
                 sort_rows_(sort_rows), topn_cnt_(0), row_count_(0),
                 sorted_(false), err_(new int()), sort_err_(new int())
{
  *err_ = common::OB_SUCCESS;
  *sort_err_ = common::OB_SUCCESS;
}
ObBaseSort::~ObBaseSort()
{
  if (err_ != NULL) {
    delete err_;
    err_ = NULL;
  }
  if (sort_err_ != NULL) {
    delete sort_err_;
    sort_err_ = NULL;
  }
}
bool ObBaseSort::compare_row(ResultRow &row1, ResultRow &row2, int &ret) //row1 <= row2 true, row1 > row2 false
{
  *err_ = common::OB_SUCCESS;
  bool bret = false;
  int cmp = 0;
  if (OB_ISNULL(&row1) || OB_ISNULL(&row2) || (row1.count() != row2.count())) {
    LOG_WDIAG("ObBaseSort::compare_row is error", K(&row1), K(&row2));
    *err_ = common::OB_ERR_UNEXPECTED;
  } else {
    LOG_DEBUG("ObBaseSort::compare_row start", K(sort_columns_.count()), K(row1), K(row2));

    int64_t cnt = sort_columns_.count();

    for (uint64_t i = 0; OB_SUCCESS == *err_ && 0 == cmp && i < cnt; i++) {

      ObSortColumn *sort_column = sort_columns_.at(i);
     if (OB_ISNULL(sort_column)) {
        *err_ = OB_ERR_UNEXPECTED;
      } else if (row1.count() < sort_column->index_
                   || row2.count() < sort_column->index_) {
        *err_ = OB_ERR_UNEXPECTED;
        LOG_WDIAG("error rows to sort", K(row1.count()), K(row2.count()), K(sort_column->index_));
      } else {
        ObSortColumn *sort_column = sort_columns_.at(i);
        int64_t cur_index = row1.count() - 1 - sort_column->index_; // get index that location in SELECT expr
        if (cur_index < 0 || cur_index >= row1.count()) {
          *err_ = OB_ERR_UNEXPECTED;
           LOG_WDIAG("error index to sort", K(sort_column->index_), K(cur_index), K(row1.count()));
        } else {
          cmp =
            row1.at(cur_index)->compare(*row2.at(cur_index), row1.at(cur_index)->get_collation_type());
          LOG_DEBUG("ObBaseSort::compare_row start", K(cur_index), K(row1.at(cur_index)), K(row2.at(cur_index)));
          if (cmp < 0) {
            bret = sort_column->is_ascending_;
          } else if (cmp > 0) {
            bret = !sort_column->is_ascending_;
          } else { //cmp == 0
            //do nothing
            //bret = true;
          }
        }
      }
    }
    if (*err_ == common::OB_SUCCESS && cmp == 0) {
      bret = true;
    }
  }

  if (*err_ != common::OB_SUCCESS) {
    ret = *err_;
  }

  LOG_DEBUG("ObBaseSort::compare_row end", K(bret), K(cmp), K(*err_));
  return bret;
}

int ObBaseSort::add_row(ResultRow *row)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = common::OB_INVALID_ARGUMENT;
    //LOG_WDIAG("inner error add_row");
  } else {
    sort_rows_.push_back(row);
    row_count_++;
  }
  return ret;
}

void ObBaseSort::swap_index(int64_t *l, int64_t *r)
{
  int64_t loc = *r;
  *r = *l;
  *l = loc;
}

void ObBaseSort::swap_row(ResultRow *&l, ResultRow *&r)
{
  ResultRow* tmp = l;
  l = r;
  r = tmp;
}

int ObBaseSort::fetch_final_results(ResultRows &rows)
{
  LOG_DEBUG("ObBaseSort::fetch_final_results", K(sort_rows_.count()));
  int ret = common::OB_SUCCESS;
  if (sorted_) {
    rows = sort_rows_;
  } else {
    ret = common::OB_ERROR;
    LOG_WDIAG("not have sorted, couldn't fetch the final result", K(ret));
  }
  return ret;
}

int ObMemorySort::sort_rows()
{
  int ret = common::OB_SUCCESS;
  if(OB_SUCC(quick_sort(0, get_row_count() - 1))) {
    sorted_ = true;
  }
  return ret;
}

int64_t ObMemorySort::partition(int64_t low, int64_t height)
{
  ResultRows &rows = get_sort_rows();

  int ret = common::OB_SUCCESS;

  ResultRow &key_row = *rows.at(low);
  int64_t i = low;
  int64_t j = height;
  LOG_DEBUG("memsort begin", K(i), K(j), K(key_row));
  while(i < j) {
    while (OB_SUCC(ret)
        && i < j
        && compare_row(key_row, *rows.at(j), ret)) {
        //&& !compare_row(*rows.at(j), key_row)) {
      LOG_DEBUG("memsort begin j", K(i), K(j) , K(key_row), K(*rows.at(j)));
      j--;
    }
    while (OB_SUCC(ret)
        && i < j
        && compare_row(*rows.at(i), key_row, ret)) {
      LOG_DEBUG("memsort begin i", K(i), K(j), K(*rows.at(i)), K(key_row));
      i++;
    }
    if (OB_SUCC(ret) && i < j) {
        swap_row(rows.at(i), rows.at(j));
        LOG_DEBUG("memsort begin sw", K(i), K(j) , K(*rows.at(i)), K(*rows.at(j)));
        //++i;
        //--j;
    }
  }

  LOG_DEBUG("memsort end one value", K(i), K(j) , K(*rows.at(low)), K(*rows.at(j)));
  if (OB_SUCC(ret)) {
    swap_row(rows.at(low), rows.at(j));
  } else {
    *sort_err_ = ret;
    LOG_WDIAG("ObMemorySort::partition failed", K(ret));
  }
  return j;
}

int ObMemorySort::quick_sort(int64_t low, int64_t height)
{
  int ret = common::OB_SUCCESS;
  if (low >= height)
    return ret;
  int64_t loc = partition(low, height);
  ret = *sort_err_; //Get sort_err_ from partition
  if (OB_FAIL(ret)) {
    LOG_WDIAG("ObMemorySort::quick_sort partition failed, not need sort any more", K(ret));
  } else if (OB_FAIL(quick_sort(low, loc - 1))) {
    LOG_WDIAG("internal error in ObMemorySort::quick_sort", K(low), K(height), K(loc));
  } else if (OB_FAIL(quick_sort(loc + 1, height))) {
    LOG_WDIAG("internal error in ObMemorySort::quick_sort", K(low), K(height), K(loc));
  }
  return ret;
}


int ObTopKSort::sort_rows()
{
  int ret = common::OB_SUCCESS;
  if (OB_SUCC(heap_sort())) {
    sorted_ = true;
  }
  return ret;
}

int ObTopKSort::sort_rows(ResultRow &new_row)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(&new_row)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid parament", K(ret));
  } else if (row_count_ < get_topn_cnt()) {
    sort_rows_heap_.push_back(&new_row);
  }
  if (OB_SUCC(ret)) {
    row_count_++;
  }

  if (OB_SUCC(ret) && row_count_ > get_topn_cnt()) {
    if (compare_row(new_row, *sort_rows_heap_.at(0), ret)) {
      if (OB_FAIL(ret)) {
        LOG_WDIAG("ObTopKSort::sort_rows compare_row fail", K(ret), K(new_row), K(*sort_rows_heap_.at(0)));
        return ret;
      }
      sort_rows_heap_.at(0) = &new_row;
    } else {
      LOG_DEBUG("ObTopKSort::sort_rows invalid rows to return", K(new_row));
      return ret;
    }

    if (OB_SUCC(ret) && OB_FAIL(build_heap())) {
      LOG_WDIAG("ObTopKSort::sort_rows build heap failed", K(ret));
    }

  } else if (OB_SUCC(ret) && row_count_ == get_topn_cnt()) {
    if (OB_FAIL(build_heap())) {
      LOG_WDIAG("ObTopKSort::sort_rows build heap failed", K(ret));
    }
  }

  return ret;
}

int ObTopKSort::build_heap() //build max heap
{
  int ret = common::OB_SUCCESS;
  int64_t topn_value = sort_rows_heap_.count();//get_topn_cnt();

  for (int64_t i = topn_value / 2 - 1; i >= 0; i--) {
    if (OB_FAIL(heap_adjust(i, topn_value))) {
      LOG_WDIAG(" ObTopKSort::build_heap heap_adjust fail", K(ret), K(i), K(topn_value));
      break;
    }
  }

  return ret;
}

int ObTopKSort::heap_adjust(int64_t p, int64_t len)
{
  int ret = common::OB_SUCCESS;
  if (p >= len) {
    ret = common::OB_INVALID_ARGUMENT;
  }

  ResultRow *row = sort_rows_heap_.at(p);
  int64_t child = (p * 2) + 1;
  ResultRows &rows = sort_rows_heap_;
  while (child < len && OB_SUCC(ret)) {
    if (OB_SUCC(ret)
      && child + 1 < len
      && (compare_row(*rows.at(child), *rows.at(child+1), ret))) {
      child = child + 1;
    }
    if (OB_SUCC(ret)
        && compare_row(*row, *rows.at(child), ret)) {
      swap_row(sort_rows_heap_.at(child), sort_rows_heap_.at(p));
      p = child;
      child = (p * 2) + 1;
    } else {
      break;
    }
  }
  return ret;
}

int ObTopKSort::heap_sort()
{
  int ret = common::OB_SUCCESS;
  int64_t len = sort_rows_heap_.count();
  OB_ASSERT(len <= get_topn_cnt());
  if (OB_FAIL(build_heap())) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WDIAG("ObTopKSort::heap_sort error");
  }
  for (int64_t i = len -1; OB_SUCC(ret) && i > 0; i--) {
    swap_row(sort_rows_heap_.at(0), sort_rows_heap_.at(i));
    if (OB_FAIL(heap_adjust(0, i))) {
      LOG_WDIAG(" ObTopKSort::build_heap heap_adjust fail", K(ret), K(i), K(i));
    }
  }
  return ret;
}

int ObTopKSort::fetch_final_results()
{
  int ret = common::OB_SUCCESS;
  LOG_DEBUG("ObBaseSort::fetch_final_results", K(sort_rows_heap_.count()));
  if (sorted_) {
    //not need do anything
    for (int64_t i = 0, j = sort_rows_heap_.count() - 1; i < j; i++, j--) {
      LOG_DEBUG("topk sort row-f", K(*sort_rows_heap_.at(i)));
      //swap_row(sort_rows_heap_.at(i), sort_rows_heap_.at(j));
    }
  } else {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WDIAG("ObBaseSort::fetch_final_results for not sorted rows");
  }
  return ret;
}

/*
int ObMergeSort::sort_rows()
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(&merge_sort_rows_array_)) {
    ret = common::OB_ERROR;
    LOG_WDIAG("invalid input for merge sort", K(ret));
  }

  get_sort_rows().reset();

  bool left_records = false;
  ResultRow *key_row = NULL;
  int64_t cur_loc = -1;
  while (true) {
    for (int64_t i = 0; i < sort_regions_; i++) {
      ObRowTrunk &sort_rows =
               *merge_sort_rows_array_.at(i);//->get_chrunk_rows();
      if (merge_rows_index_.at(i) >= sort_rows.get_row_count()) {
        continue;
      }
      left_records = true;
      if (OB_ISNULL(key_row)) {
        key_row = sort_rows.get_chrunk_rows().at(merge_rows_index_.at(i));
        cur_loc = i;
      } else if (compare_row(*sort_rows.get_chrunk_rows().at(merge_rows_index_.at(i)), *key_row)) {
        key_row = sort_rows.get_chrunk_rows().at(merge_rows_index_.at(i));
        cur_loc = i;
      }
    }

    if (!left_records) {
      break;
    }
    get_sort_rows().push_back(key_row);
    merge_rows_index_.at(cur_loc)++;
  }
  return ret;
}

int ObRowTrunk::put_row(ResultRow& row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(&row)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid parament", K(ret));
  } else {
    chrunk_rows_.push_back(&row);
  }
  return ret;
}
*/

}
}
}
