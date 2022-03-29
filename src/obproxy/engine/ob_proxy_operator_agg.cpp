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
#include "ob_proxy_operator_agg.h"
#include "lib/charset/ob_charset.h"
#include "common/ob_obj_compare.h" //ObObjCmpFuncs

static const int64_t MAX_CALC_BYTE_LEN = sizeof(uint32_t) * 10;

using namespace oceanbase::sql;
using namespace oceanbase::common;
namespace oceanbase {
namespace obproxy {
namespace engine {

ObProxyAggOp::~ObProxyAggOp()
{
  if (OB_NOT_NULL(ob_agg_func_)) {
    ob_agg_func_->~ObAggregateFunction();
    ob_agg_func_ = NULL;
  }

  if (OB_NOT_NULL(hash_col_idxs_)) {
    hash_col_idxs_->~ObColInfoArray();
    hash_col_idxs_ = NULL;
  }

  if (OB_NOT_NULL(sort_columns_)) {
    sort_columns_->~SortColumnArray();
    sort_columns_ = NULL;
  }
}


int ObProxyAggOp::init()
{
/* put ob_agg_func_ init and init_group_by_columns() to get_next_row() for
 * input(exprs) maybe not set when run the ObProxyAggOp::init by Optmizer.*/

  int ret = common::OB_SUCCESS;
  char *tmp_buf = NULL;
  if (OB_ISNULL(tmp_buf = (char *)allocator_.alloc(sizeof(ObColInfoArray)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("init not have enough memory", K(ret), K(sizeof(ObColInfoArray)));
  } else if (OB_ISNULL(hash_col_idxs_ = new (tmp_buf) ObColInfoArray(ENGINE_ARRAY_NEW_ALLOC_SIZE, allocator_))) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WARN("init construct error", K(ret), K(hash_col_idxs_));
  } else if (OB_ISNULL(tmp_buf = (char *)allocator_.alloc(sizeof(SortColumnArray)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("init not have enough memory", K(ret), K(sizeof(SortColumnArray)));
  } else if (OB_ISNULL(sort_columns_ = new (tmp_buf) SortColumnArray(ENGINE_ARRAY_NEW_ALLOC_SIZE, allocator_))) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WARN("init construct error", K(ret), K(sort_columns_));
  } else {
    ret = ObProxyOperator::init();
  }

  return ret;
}

int ObProxyAggOp::get_next_row()
{
  int ret = common::OB_SUCCESS;

  void *tmp_buf = NULL;
  tmp_buf = allocator_.alloc(sizeof(ObAggregateFunction));
  ob_agg_func_ = new (tmp_buf)ObAggregateFunction(allocator_, input_->get_select_exprs());
  if (OB_ISNULL(ob_agg_func_)) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", K(ret), K(ob_agg_func_));
  } else if (OB_FAIL(init_group_by_columns())) {
    LOG_WARN("init group by columns error", K(ret));
  } else if (OB_FAIL(ob_agg_func_->init(*hash_col_idxs_))) {
    LOG_WARN("init ob_agg_func_ failed", K(ret));
  } else {
    ret = ObProxyOperator::get_next_row();
  }

  return ret;
}

int ObProxyAggOp::init_group_by_columns()
{
  int ret = common::OB_SUCCESS;
  ObProxyAggInput *input = dynamic_cast<ObProxyAggInput*>(get_input());

  if (OB_ISNULL(input)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("input is invalid in ObProxyAggOp", K(ret), K(input_));
  } else {
    const ObSEArray<ObProxyGroupItem*, 4>& group_by_expr = input->get_group_by_exprs();
    ObProxyExpr* expr_ptr = NULL;
    void *tmp_ptr = NULL;

    LOG_DEBUG("ObProxyAggOp::init_group_by_columns init begin", K(group_by_expr.count()));

    for (int64_t i = 0; OB_SUCC(ret) && i < group_by_expr.count(); i++) {
      if (OB_ISNULL(expr_ptr = group_by_expr.at(i))) {
        ret = common::OB_ERROR;
        LOG_WARN("internal error in ObProxyAggOp::init_group_by_columns", K(ret), K(expr_ptr));
      }
      LOG_DEBUG("ObProxyAggOp::init_group_by_columns init begin", K(i), K(*expr_ptr));
      tmp_ptr = allocator_.alloc(sizeof(ObSortColumn));
      ObSortColumn *new_col= new (tmp_ptr) ObSortColumn();
      if (expr_ptr->index_ >= 0) {
        new_col->index_ = expr_ptr->index_;
      } else {
        ret = common::OB_ERR_UNEXPECTED;
        LOG_WARN("ObProxyAggOp::init_group_by_columns error group by expression", K(ret), KP(expr_ptr));
      }
      //use the type of the result returned, and cannot get type only by expr
      //new_col->cs_type_ = expr_ptr->get_expr_type();
      new_col->is_ascending_ = true; //TODO update
      sort_columns_->push_back(new_col);
      tmp_ptr = allocator_.alloc(sizeof(common::ObColumnInfo));
      common::ObColumnInfo *col_info = new (tmp_ptr) common::ObColumnInfo();
      col_info->index_ = expr_ptr->index_;
      //col_info->cs_type_ = expr_ptr->get_expr_type();
      hash_col_idxs_->push_back(*col_info);
    }

    LOG_DEBUG("ObProxyAggOp::init_group_by_columns init end", K(group_by_expr.count()),
                   K(sort_columns_->count()), K(hash_col_idxs_->count()));

  }
  return ret;
}

int ObProxyAggOp::process_exprs_in_agg(ResultRows *src_rows, ResultRows *obj_rows)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(src_rows) || OB_ISNULL(obj_rows)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN(" ObProxyAggOp::process_exprs_in_agg invalid input", K(ret), K(src_rows), K(obj_rows));
    return ret;
  }
  expr_has_calced_ = false;
  int64_t row_count = src_rows->count();
  ResultRow* row = NULL;
  int64_t added_row_count = 0;

  common::ObSEArray<ObProxyExpr*, 4>& select_exprs =
        get_input()->get_select_exprs();

  for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
    ResultRow *new_row = NULL;
    row = src_rows->at(i);
    if (OB_ISNULL(row)) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WARN("ObProxyAggOp::process_exprs_in_agg invalid row", K(ret));
      break;
    } else if (OB_FAIL(init_row(new_row))) {
      LOG_WARN("ObProxyProOp::process_ready_data init row error", K(ret));
      ret = common::OB_ERROR;
      break;
    }
    LOG_DEBUG("get one recored from res", K(ret), K(row));
    if (OB_FAIL(ObProxyOperator::calc_result(*row, *new_row, select_exprs, added_row_count))) {
      LOG_WARN("ObProxyProOp::process_ready_data calc result error", K(ret));
    } else if (OB_FAIL(obj_rows->push_back(new_row))) {
      LOG_WARN("ObProxyProOp::process_ready_data put row error", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    expr_has_calced_ = true;
  }

  return ret;
}

int ObProxyAggOp::handle_response_result(void *data, bool &is_final, ObProxyResultResp *&result)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("Enter ObProxyAggOp::handle_response_result", K(op_name()), K(data));

  ObProxyResultResp *opres = NULL;

  if (OB_ISNULL(data)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input", K(ret), K(data));
  } else if (OB_ISNULL(opres = reinterpret_cast<ObProxyResultResp*>(data))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObProxyMemSortOp::handle_response_result not response result", K(opres), KP(opres));
  } else if (!opres->is_resultset_resp()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObProxyAggOp::handle_response_result not response result", K(opres), KP(opres), K(opres->is_resultset_resp()));
  } else {
    LOG_DEBUG("ObProxyAggOp::::handle_response_result result set", K(opres));
    if (OB_ISNULL(get_result_fields())) {
      result_fields_ = opres->get_fields();
    }
    ResultRow *row = NULL;
    int64_t sum = 0;
    while (OB_SUCC(opres->next(row))) {
      if (OB_ISNULL(row) || row == NULL) {
        break;
        LOG_DEBUG("ObProxyAggOp::handle_response_result fetched all rows", K(sum), K(ret));
      }
      LOG_DEBUG("ObProxyAggOp::handle_response_result fetch rows", K(row), K(*row), K(ret));
      if (OB_FAIL(ob_agg_func_->add_row(row))) {
        LOG_WARN("inner error to put rows", K(ret), K(op_name()));
        break;
      }
      sum++;
    }
    if (ret == common::OB_ITER_END) {
      LOG_DEBUG("ObProxyAggOp::process_ready_data fetch rows over", K(sum), K(ret));
      ret = common::OB_SUCCESS;
    }
    if (OB_SUCC(ret) && is_final) {
      ResultRow *row = NULL;
      ObProxyResultResp *res = NULL;
      ResultFields *origin_fields = NULL;

      if (OB_ISNULL(origin_fields = get_result_fields())) {
        ret = common::OB_ERROR;
        LOG_WARN("invalid field info", K(ret), KP(origin_fields));
      } else if (OB_ISNULL(ob_agg_func_)) {
        ret = common::OB_ERROR;
        LOG_WARN("not init ob_agg_func_ before used", K(ret));
      } else {
        if (OB_FAIL(ob_agg_func_->handle_all_result(row))) {
          ret = common::OB_ERROR;
          LOG_WARN("handle all agg failed.", K(ret));
        } else if (OB_FAIL(put_row(row))) {
          ret = common::OB_ERROR;
          LOG_WARN("put result failed.", K(ret));
        } else if (OB_FAIL(packet_result_set(res, cur_result_rows_, origin_fields))) {
            res->set_packet_flag(PCK_ERR_RESPONSE);
            LOG_WARN("packet resultset packet error", K(ret));
        }

        if (OB_FAIL(ret)) {
          if (OB_NOT_NULL(res)) {
            res->set_packet_flag(PCK_ERR_RESPONSE);
          }
          LOG_DEBUG("ObProxyAggOp::process_complete_data packet err", K(opres));
        } else {
          LOG_DEBUG("ObProxyAggOp::process_complete_data packet result set", K(opres));
        }
      }
      result = res;
    }
  }
  return ret;
}

int ObProxyHashAggOp::init() {
  int ret = common::OB_SUCCESS;
  result_rows_->nbuckets_ = BUCKET_BUF_SIZE;
  return ret;
}

int ObProxyHashAggOp::handle_response_result(void *data, bool &is_final, ObProxyResultResp *&result)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("Enter ObProxyHashAggOp::handle_response_result", K(op_name()), K(data));
  ObProxyResultResp *opres = NULL;
  ResultFields *origin_fields = NULL;

  if (OB_ISNULL(data)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input", K(ret), K(data));
  } else if (OB_ISNULL(opres = reinterpret_cast<ObProxyResultResp*>(data))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObProxyMemSortOp::handle_response_result not response result", K(opres), KP(opres));
  } else if (!opres->is_resultset_resp()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObProxyHashAggOp::handle_response_result not response result", K(opres), KP(opres), K(opres->is_resultset_resp()));
  } else {
    LOG_DEBUG("ObProxyAggOp::::handle_response_result result set", K(opres));
    if (OB_ISNULL(get_result_fields())) {
      result_fields_ = opres->get_fields();
    }
    ResultRow *row = NULL;
    int64_t sum = 0;
    while (OB_SUCC(opres->next(row))) {
      if (OB_ISNULL(row) || row == NULL) {
        break;
        LOG_DEBUG("ObProxyAggOp::handle_response_result fetched all rows", K(sum), K(ret));
      }
      LOG_DEBUG("ObProxyAggOp::handle_response_result fetch rows", K(row), K(*row), K(ret));
      if (OB_FAIL(ob_agg_func_->add_row(row))) {
        LOG_WARN("inner error to put rows", K(ret), K(op_name()));
        break;
      }
      sum++;
    }
    if (ret == common::OB_ITER_END) {
      LOG_DEBUG("ObProxyAggOp::process_ready_data fetch rows over", K(sum), K(ret));
      ret = common::OB_SUCCESS;
    }
    if (OB_SUCC(ret) && is_final) {
      ObProxyResultResp *res = NULL;
      if (OB_ISNULL(origin_fields = get_result_fields())) {
        ret = common::OB_ERROR;
        LOG_WARN("invalid field info", K(ret), KP(origin_fields));
      } else if (OB_ISNULL(ob_agg_func_)) {
        ret = common::OB_ERROR;
        LOG_WARN("not init ob_agg_func_ before used", K(ret));
      } else {
        if (OB_FAIL(ob_agg_func_->handle_all_hash_result(cur_result_rows_))) {
          ret = common::OB_ERROR;
          LOG_WARN("handle all agg failed.", K(ret));
        } else if (OB_FAIL(packet_result_set(res, cur_result_rows_, origin_fields))) {
            res->set_packet_flag(PCK_ERR_RESPONSE);
            LOG_WARN("packet resultset packet error", K(ret));
        }

        if (OB_FAIL(ret)) {
          if (OB_NOT_NULL(res)) {
            res->set_packet_flag(PCK_ERR_RESPONSE);
          }
          LOG_DEBUG("ObProxyHashAggOp::process_complete_data packet err", K(opres));
        } else {
          LOG_DEBUG("ObProxyHashAggOp::process_complete_data packet result set", K(opres));
        }
      }
      result = res;
    }
  }
  return ret;
}

int ObProxyMergeAggOp::init()
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(ObProxyAggOp::init())) {
    LOG_WARN("ObProxyAggOp::init error", K(ret));
  } else if (OB_FAIL(init_row_set(result_rows_array_))) {
    LOG_WARN("ObProxyAggOp::init result_rows_array_ error", K(ret));
  } else {
    char *tmp_buf = NULL;
    if (OB_ISNULL(tmp_buf = (char*)allocator_.alloc(sizeof(ResultFlagArray)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("no have enough memory to init", K(ret), K(tmp_buf), K(sizeof(ResultFlagArray)));
    } else if (OB_ISNULL(result_rows_flag_array_
        = new (tmp_buf) ResultFlagArray(ENGINE_ARRAY_NEW_ALLOC_SIZE, allocator_))) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WARN("ObProxyAggOp::init error for construct", K(ret), K(result_rows_flag_array_));
    } else if (OB_ISNULL(tmp_buf = (char*)allocator_.alloc(sizeof(ResultRespArray)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("no have enough memory to init", K(ret), K(tmp_buf), K(sizeof(ResultRespArray)));
    } else if (OB_ISNULL(regions_results_
        = new (tmp_buf) ResultRespArray(ENGINE_ARRAY_NEW_ALLOC_SIZE, allocator_))) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WARN("ObProxyAggOp::init error for construct", K(ret), K(regions_results_));
    }
  }
  return ret;
}

int ObProxyMergeAggOp::handle_response_result(void *data, bool &is_final, ObProxyResultResp *&result)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("Enter ObProxyMergeAggOp::handle_response_result", K(op_name()), K(data));

  ObProxyResultResp *opres = NULL;

  if (OB_ISNULL(data)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input", K(ret), K(data));
  } else if (OB_ISNULL(opres = reinterpret_cast<ObProxyResultResp*>(data))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObProxyMemSortOp::handle_response_result not response result", K(opres), KP(opres));
  } else if (!opres->is_resultset_resp()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObProxyMergeAggOp::handle_response_result not response result", K(opres), KP(opres), K(opres->is_resultset_resp()));
  } else {
    if (OB_ISNULL(get_result_fields())) {
      result_fields_ = opres->get_fields();
    }
    if (regions_ == 0) {
      regions_ = opres->get_result_sum();
      if (OB_FAIL(init_result_rows_array(regions_))) {
        LOG_WARN("init regions to be merege error", K(ret), K(regions_));
      }
    }
    regions_results_->push_back(opres);
    LOG_DEBUG("ObProxyMergeAggOp::handle_response_result put all result to regions_results", K(ret), K(opres));
    if (OB_SUCC(ret) && is_final) {
      ObProxyResultResp *res = NULL;
      ResultFields *origin_fields = NULL;
      if (OB_ISNULL(origin_fields = get_result_fields())) {
        ret = common::OB_ERROR;
        LOG_WARN("invalid field info", K(ret), KP(origin_fields));
      } else if (OB_ISNULL(ob_agg_func_)) {
        ret = common::OB_ERROR;
        LOG_WARN("not init ob_agg_func_ before used", K(ret));
      } else {
        if (OB_FAIL(fetch_all_result(cur_result_rows_))) {
          ret = common::OB_ERROR;
          LOG_WARN("handle all agg failed.", K(ret));
        } else if (OB_FAIL(packet_result_set(res, cur_result_rows_, get_result_fields()))) {
            res->set_packet_flag(PCK_ERR_RESPONSE);
            LOG_WARN("packet resultset packet error", K(ret));
        }

        if (OB_FAIL(ret)) {
          if (OB_NOT_NULL(res)) {
            res->set_packet_flag(PCK_ERR_RESPONSE);
          }
          LOG_DEBUG("ObProxyMergeAggOp::process_complete_data packet err", K(ret), K(opres));
        } else {
          LOG_DEBUG("ObProxyMergeAggOp::process_complete_data packet result set", K(opres));
        }
        result = res;
      }
      has_done_agg_ = true;
    }
  }
  return ret;
}

int ObProxyMergeAggOp::init_result_rows_array(int64_t regions)
{
  int ret = common::OB_SUCCESS;
  if (regions <= 0) {
    ret =  common::OB_INVALID_ARGUMENT;
    LOG_WARN("error regions to store", K(ret));
  }

  for (int64_t i = 0; i < regions; i++) {
    ResultRow *row = NULL;//new ResultRows;
    result_rows_array_->push_back(row);
    result_rows_flag_array_->push_back(true);
  }
  return ret;
}

int ObProxyMergeAggOp::fetch_all_result(ResultRows *rows)
{
  bool has_inited_normal_cell = true;
  int ret = common::OB_SUCCESS;
  ObProxyResultResp *res = NULL;
  if (OB_ISNULL(rows)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input", K(ret));
    return ret;
  }
  if (regions_ < 0 || regions_ != regions_results_->count()) {
    ret = common::OB_ERROR;
    LOG_WARN("invalid result to merge", K(ret), K(regions_), K(regions_results_->count()));
  } else {
    void *tmp_buf = NULL;
    ResultRows *new_rows = NULL;
    ObBaseSort *sort_imp = NULL;
    if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ResultRows)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("no have enough memory to init", K(ret), K(op_name()), K(sizeof(ResultRows)));
    } else if (OB_ISNULL(new_rows = new (tmp_buf) ResultRows(ENGINE_ARRAY_NEW_ALLOC_SIZE, allocator_))) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WARN("init ResultRows failed", K(ret), K(op_name()), K(new_rows));
    } else if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObMemorySort)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("no have enough memory to init", K(ret), K(op_name()), K(sizeof(ObMemorySort)));
    } else {
      sort_imp = new (tmp_buf) ObMemorySort(*sort_columns_, allocator_, *new_rows);
    }

    while (OB_SUCC(ret)){
      //find the min row
      ResultRow *row = NULL;//result_rows_array_.at(0).at(0);
      ResultRow *cur_row = NULL;
      int64_t cur_loc = 0;
      bool exist_row = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < regions_; i++) {
        /* 1.find the smallest the row in the resultset. */
        if (!result_rows_flag_array_->at(i)) { // server_$i has no any result
          continue;
        } else if (OB_ISNULL(result_rows_array_->at(i))){ //temperory stored all regions
          /* find the next from res */
          if (OB_ISNULL(res = regions_results_->at(i)) || !res->is_resultset_resp()) {
            ret = common::OB_ERROR;
            LOG_WARN("invalid result to merge", K(ret), K(ret));
            continue;
          } else {
            if (OB_FAIL(res->next(row))) {
              if (ret == common::OB_ITER_END) {
                ret = common::OB_SUCCESS;
                result_rows_flag_array_->at(i) = false;
              } else {
                ret = common::OB_ERROR;
                LOG_WARN("invalid result to merge", K(ret), K(row));
              }
              continue;
            } else if (OB_ISNULL(row)) {
              ret = common::OB_ERROR;
              LOG_WARN("invalid result to merge", K(ret), K(row));
              continue;
            } else {
              result_rows_array_->at(i) = row;
              LOG_DEBUG("ObProxyMergeAggOp::fetch_all_result row new row", K(res->get_result_idx()), K(*row));
            }
          }
        } else {
          row = result_rows_array_->at(i);
        }
        exist_row = true;
        if (OB_ISNULL(cur_row)) {
          cur_row = row;
          cur_loc = i;
          continue;
        } else {
          if (sort_imp->compare_row(*row, *cur_row, ret) && OB_SUCC(ret)) {
            cur_row = row;
            cur_loc = i;
          }
        }
      }

      if (!exist_row || OB_FAIL(ret)) { //find not have any row in the result sets.
        break;
      } else if (OB_ISNULL(cur_row)) {
        ret = common::OB_ERROR;
        LOG_WARN("inner error in HashAGG", K(ret));
        break;
      }

      for (int64_t i = 0; i < cur_loc; i++) {

        if (OB_ISNULL(result_rows_array_->at(i))) {
          continue;
        }
        LOG_DEBUG("ObProxyMergeAggOp::fetch_all_result rows", K(i), K(*cur_row), K(*(result_rows_array_->at(i))));
        if (ob_agg_func_->is_same_group(*cur_row, *(result_rows_array_->at(i)))) {
          LOG_DEBUG("ObProxyMergeAggOp::fetch_all_result rows is_same_group", K(*cur_row), K(*(result_rows_array_->at(i))));
          if (OB_FAIL(ob_agg_func_->cal_row_agg(*cur_row, *(result_rows_array_->at(i)),
                  has_inited_normal_cell))) {
            LOG_WARN("ObProxyMergeAggOp::fetch_all_result, calc_row_agg fail", K(ret), K(cur_row));
          } else {
            result_rows_array_->at(i) = NULL;
          }
          //break;
          continue;
        }
      }
      if (OB_SUCC(ret)) {
        rows->push_back(cur_row);
        LOG_DEBUG("ObProxyMergeAggOp::fetch_all_result row done row", K(*cur_row));
        result_rows_array_->at(cur_loc) = NULL; //find the next
      }
      //ret = push_row(row);
    }
  }
  LOG_DEBUG("ObProxyMergeAggOp::fetch_all_result end", K(ret));
  return ret;
}

ObProxyStreamAggOp::~ObProxyStreamAggOp()
{
  if (NULL != current_group_unit_) {
    ObProxyGroupUnit::destroy_group_unit(allocator_, current_group_unit_);
    current_group_unit_ = NULL;
  }
}

int ObProxyStreamAggOp::handle_response_result(void *data, bool &is_final, ObProxyResultResp *&result)
{
  int ret = OB_SUCCESS;

  ObProxyResultResp *opres = NULL;
  ObProxyAggInput *input = NULL;

  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input, data is NULL", K(ret));
  } else if (OB_ISNULL(opres = reinterpret_cast<ObProxyResultResp*>(data))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input, opres type is not match", K(ret));
  } else if (!opres->is_resultset_resp()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("resp is not resultset", K(opres), K(ret));
  } else if (OB_ISNULL(input = dynamic_cast<ObProxyAggInput*>(get_input()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input is invalid", K(ret));
  } else {
    const ObSEArray<ObProxyGroupItem*, 4>& group_exprs = input->get_group_by_exprs();
    const ObSEArray<ObProxyExpr*, 4>& agg_exprs = input->get_agg_exprs();
    int64_t limit_offset = get_input()->get_limit_offset();
    int64_t limit_offset_size = get_input()->get_limit_size() + limit_offset;

    if (OB_ISNULL(get_result_fields())) {
      result_fields_ = opres->get_fields();
      if (OB_ISNULL(result_fields_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("no result field, unexpected", K(ret));
      }
    }

    ResultRow *row = NULL;
    while (OB_SUCC(ret) && OB_SUCC(opres->next(row))) {
      ObProxyGroupUnit group_unit(allocator_);
      if (OB_FAIL(group_unit.init(row, group_exprs))) {
        LOG_WARN("fail to init group unit", K(ret));
      } else if (NULL == current_group_unit_) {
        if (OB_FAIL(ObProxyGroupUnit::create_group_unit(allocator_, current_group_unit_, group_unit))) {
          LOG_WARN("fail to create group unit", K(ret));
        }
      } else if (*current_group_unit_ == group_unit) {
        if (OB_FAIL(current_group_unit_->aggregate(group_unit, agg_exprs))) {
          LOG_WARN("fail to aggregate", K(ret));
        }
      } else {
        if (OB_FAIL(current_group_unit_->set_agg_value())) {
          LOG_WARN("fail to set agg value", K(ret));
        } else if (OB_FAIL(current_rows_.push_back(current_group_unit_->get_row()))) {
          LOG_WARN("fail to push back row", K(ret));
        } else {
          ObProxyGroupUnit::destroy_group_unit(allocator_, current_group_unit_);
          current_group_unit_ = NULL;
          if (limit_offset_size > 0 && current_rows_.count() == limit_offset_size) {
            is_final = true;
            break;
          }

          if (OB_FAIL(ObProxyGroupUnit::create_group_unit(allocator_, current_group_unit_, group_unit))) {
            LOG_WARN("fail to create group unit", K(ret));
          }
        }
      }
    }

    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
      if (is_final && NULL != current_group_unit_) {
        if (OB_FAIL(current_group_unit_->set_agg_value())) {
          LOG_WARN("fail to set agg value", K(ret));
        } else if (OB_FAIL(current_rows_.push_back(current_group_unit_->get_row()))) {
          LOG_WARN("fail to push back row", K(ret));
        } else {
          ObProxyGroupUnit::destroy_group_unit(allocator_, current_group_unit_);
          current_group_unit_ = NULL;
        }
      }
    }

    if (OB_SUCC(ret) && (!current_rows_.empty() || is_final)) {
      ObProxyResultResp *res = NULL;
      ResultRows *rows = NULL;
      void *tmp_buf = NULL;
      if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ResultRows)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("no have enough memory to init", "size", sizeof(ResultRows), K(ret));
      } else if (FALSE_IT(rows = new (tmp_buf) ResultRows(ENGINE_ARRAY_NEW_ALLOC_SIZE, allocator_))) {
        // impossible
      } else {
        int64_t count = current_rows_.count();
        if (limit_offset_size > 0) {
          count = count > limit_offset_size ? limit_offset_size : count;
        }

        for (int64_t i = limit_offset; OB_SUCC(ret) && i < count; i++) {
          if (OB_FAIL(rows->push_back(current_rows_.at(i)))) {
            LOG_WARN("fail to push back row", K(i), K(count), K(limit_offset), K(limit_offset_size), K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (FALSE_IT(current_rows_.reuse())) {
          // impossible
        } else if (OB_FAIL(packet_result_set(res, rows, get_result_fields()))) {
          LOG_WARN("fail to packet resultset", K(op_name()), K(ret));
        }
      }

      result = res;
    }
  }

  return ret;
}

ObProxyMemMergeAggOp::~ObProxyMemMergeAggOp()
{
  GroupUnitHashMap::iterator it = group_unit_map_.begin();
  GroupUnitHashMap::iterator tmp_it;
  GroupUnitHashMap::iterator end = group_unit_map_.end();
  for (; it != end;) {
    tmp_it = it;
    ++it;
    ObProxyGroupUnit::destroy_group_unit(allocator_, &(*tmp_it));
  }
}

int ObProxyMemMergeAggOp::handle_response_result(void *data, bool &is_final, ObProxyResultResp *&result)
{
  int ret = OB_SUCCESS;

  ObProxyResultResp *opres = NULL;
  ObProxyAggInput *input = NULL;

  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input, data is NULL", K(ret));
  } else if (OB_ISNULL(opres = reinterpret_cast<ObProxyResultResp*>(data))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input, opres type is not match", K(ret));
  } else if (!opres->is_resultset_resp()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("resp is not resultset", K(opres), K(ret));
  } else if (OB_ISNULL(input = dynamic_cast<ObProxyAggInput*>(get_input()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input is invalid", K(ret));
  } else {
    const ObSEArray<ObProxyGroupItem*, 4>& group_exprs = input->get_group_by_exprs();
    const ObSEArray<ObProxyExpr*, 4>& agg_exprs = input->get_agg_exprs();

    if (OB_ISNULL(get_result_fields())) {
      result_fields_ = opres->get_fields();
      if (OB_ISNULL(result_fields_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("no result field, unexpected", K(ret));
      }
    }

    ResultRow *row = NULL;
    while (OB_SUCC(ret) && OB_SUCC(opres->next(row))) {
      ObProxyGroupUnit group_unit(allocator_);
      ObProxyGroupUnit *current_group_unit = NULL;
      if (OB_FAIL(group_unit.init(row, group_exprs))) {
        LOG_WARN("fail to init group unit", K(ret));
      } else if (OB_FAIL(group_unit_map_.get_refactored(group_unit, current_group_unit))) {
        if (OB_HASH_NOT_EXIST == ret) {
          if (OB_FAIL(ObProxyGroupUnit::create_group_unit(allocator_, current_group_unit, group_unit))) {
            LOG_WARN("fail to create group unit", K(ret));
          } else if (OB_FAIL(group_unit_map_.set_refactored(current_group_unit))) {
            LOG_WARN("fail to set group unit", K(ret));
          }
        } else {
          LOG_WARN("fail to get group unit", K(ret));
        }
      } else if (OB_FAIL(current_group_unit->aggregate(group_unit, agg_exprs))) {
        LOG_WARN("fail to aggregate", K(ret));
      }
    }

    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }

    if (OB_SUCC(ret) && is_final) {
      ObProxyResultResp *res = NULL;
      ResultRows *rows = NULL;
      void *tmp_buf = NULL;
      if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ResultRows)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("no have enough memory to init", "size", sizeof(ResultRows), K(ret));
      } else if (FALSE_IT(rows = new (tmp_buf) ResultRows(ENGINE_ARRAY_NEW_ALLOC_SIZE, allocator_))) {
        // impossible
      } else {
        GroupUnitHashMap::iterator it = group_unit_map_.begin();
        GroupUnitHashMap::iterator tmp_it;
        GroupUnitHashMap::iterator end = group_unit_map_.end();
        for (; OB_SUCC(ret) && it != end; ) {
          if (OB_FAIL(it->set_agg_value())) {
            LOG_WARN("fail to set agg value", K(ret));
          } else if (OB_FAIL(rows->push_back(it->get_row()))) {
            LOG_WARN("fail to push back row", K(ret));
          } else {
            tmp_it = it;
            ++it;
            ObProxyGroupUnit::destroy_group_unit(allocator_, &(*tmp_it));
          }
        }
        group_unit_map_.reset();
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(packet_result_set(res, rows, get_result_fields()))) {
          LOG_WARN("fail to packet resultset", K(op_name()), K(ret));
        }
      }
      result = res;
    }
  }

  return ret;
}

int ObProxyGroupUnit::create_group_unit(common::ObIAllocator &allocator,
                                        ObProxyGroupUnit* &current_group_unit,
                                        ObProxyGroupUnit &group_unit)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  current_group_unit = NULL;
  if (OB_ISNULL(buf = (char*)(allocator.alloc(sizeof(ObProxyGroupUnit))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc group unit buf", K(ret));
  } else if (OB_ISNULL(current_group_unit = new (buf)ObProxyGroupUnit(allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new group unit", K(ret));
  } else if (OB_FAIL(current_group_unit->assign(group_unit))) {
    LOG_WARN("fail to assign group unit", K(ret));
  }

  return ret;
}

void ObProxyGroupUnit::destroy_group_unit(common::ObIAllocator &allocator,
                                          ObProxyGroupUnit* group_unit)
{
  group_unit->~ObProxyGroupUnit();
  allocator.free(group_unit);
}

ObProxyGroupUnit::~ObProxyGroupUnit()
{
  for (int64_t i = 0; i < agg_units_.count(); i++) {
    ObProxyAggUnit *agg_unit = agg_units_.at(i);
    ObProxyAggUnit::destroy_agg_unit(allocator_, agg_unit);
  }
}

int ObProxyGroupUnit::init(ResultRow *row, const ObIArray<ObProxyGroupItem*>& group_exprs)
{
  int ret = OB_SUCCESS;

  ObProxyExprCtx ctx(0, dbconfig::TESTLOAD_NON, false, &allocator_);
  ObProxyExprCalcItem calc_item(row);

  for (int64_t i = 0; OB_SUCC(ret) && i < group_exprs.count(); i++) {
    if (OB_FAIL(group_exprs.at(i)->calc(ctx, calc_item, group_values_))) {
      LOG_WARN("fail to calc group exprs", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    row_ = row;
  }

  return ret;
}

uint64_t ObProxyGroupUnit::hash() const
{
  uint64_t hash = 0;
  for (int64_t i = 0; i < group_values_.count(); i++) {
    hash = group_values_.at(i).hash(hash);
  }

  return hash;
}

bool ObProxyGroupUnit::operator==(const ObProxyGroupUnit &group_unit) const
{
  bool bret = true;
  const ObIArray<ObObj> &group_values = group_unit.get_group_values();
  if (group_values_.count() != group_values.count()) {
    bret = false;
  } else {
    for (int64_t i = 0; bret && i < group_values_.count(); i++) {
      bret = (group_values_.at(i) == group_values.at(i));
    }
  }

  return bret;
}

int ObProxyGroupUnit::assign(const ObProxyGroupUnit &group_unit)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(group_values_.assign(group_unit.get_group_values()))) {
    LOG_WARN("fail to assign group value", K(ret));
  } else {
    row_ = group_unit.get_row();
  }

  return ret;
}

int ObProxyGroupUnit::do_aggregate(ResultRow *row)
{
  int ret = OB_SUCCESS;

  ObProxyExprCtx ctx(0, dbconfig::TESTLOAD_NON, false, &allocator_);
  ObProxyExprCalcItem calc_item(row);
  common::ObSEArray<ObObj, 4> agg_values;

  for (int64_t i = 0; OB_SUCC(ret) && i < agg_units_.count(); i++) {
    ObProxyAggUnit *agg_unit = agg_units_.at(i);
    agg_values.reuse();
    if (OB_FAIL(agg_unit->get_agg_expr()->calc(ctx, calc_item, agg_values))) {
      LOG_WARN("fail to calc agg expr", K(ret));
    } else if (OB_FAIL(agg_unit->merge(agg_values))) {
      LOG_WARN("fail to merge agg value", K(ret));
    }
  }

  return ret;
}

int ObProxyGroupUnit::aggregate(const ObProxyGroupUnit &group_unit, const ObIArray<ObProxyExpr*>& agg_exprs)
{
  int ret = OB_SUCCESS;

  if (agg_units_.empty() && !agg_exprs.empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < agg_exprs.count(); i++) {
      ObProxyExpr *agg_expr = agg_exprs.at(i);
      ObProxyAggUnit *agg_unit = NULL;
      if (OB_FAIL(ObProxyAggUnit::create_agg_unit(allocator_, agg_expr, agg_unit))) {
        LOG_WARN("fail to create agg unit", "agg type", agg_expr->get_expr_type(), K(ret));
      } else if (FALSE_IT(agg_unit->set_agg_expr(agg_expr))) {
        LOG_WARN("fail to set agg expr", K(ret));
      } else if (agg_units_.push_back(agg_unit)) {
        LOG_WARN("fail to push back agg unit", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      // Process the first row of data
      if (OB_FAIL(do_aggregate(row_))) {
        LOG_WARN("fail to do aggregate", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    // Aggregate subsequent data
    if (OB_FAIL(do_aggregate(group_unit.get_row()))) {
      LOG_WARN("fail to do aggregate", K(ret));
    }
  }

  return ret;
}

int ObProxyGroupUnit::set_agg_value()
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < agg_units_.count(); i++) {
    ObProxyAggUnit *agg_unit = agg_units_.at(i);
    int64_t index = agg_unit->get_agg_expr()->get_index();
    *(row_->at(index)) = agg_unit->get_result();
  }

  return ret;
}

int ObProxyAggUnit::create_agg_unit(ObIAllocator &allocator,
                                    ObProxyExpr *expr,
                                    ObProxyAggUnit* &agg_unit)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  ObProxyExprType expr_type = expr->get_expr_type();
#define ALLOC_AGG_UNIT_BY_TYPE(ExprClass, args) \
  if (OB_ISNULL(buf = (allocator.alloc(sizeof(ExprClass))))) { \
    ret = OB_ALLOCATE_MEMORY_FAILED; \
    LOG_WARN("fail to alloc mem", K(ret)); \
  } else if (OB_ISNULL(agg_unit = new (buf)ExprClass(allocator, args))) { \
    ret = OB_ALLOCATE_MEMORY_FAILED; \
    LOG_WARN("fail to new expr", K(ret)); \
  }

  switch(expr_type) {
    case OB_PROXY_EXPR_TYPE_FUNC_MAX:
      ALLOC_AGG_UNIT_BY_TYPE(ObProxyComparableAggUnit, false);
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_MIN:
      ALLOC_AGG_UNIT_BY_TYPE(ObProxyComparableAggUnit, true);
      break;
    case OB_PROXY_EXPR_TYPE_FUNC_COUNT:
    case OB_PROXY_EXPR_TYPE_FUNC_SUM:
      ALLOC_AGG_UNIT_BY_TYPE(ObProxyAccumulationAggUnit, expr->get_accuracy().get_scale());
      break;
    default:
      ret = OB_ERROR_UNSUPPORT_EXPR_TYPE;
      LOG_WARN("unexpected type", K(expr_type));
      break;
  }

  return ret;
}

void ObProxyAggUnit::destroy_agg_unit(ObIAllocator &allocator,
                                      ObProxyAggUnit *agg_unit)
{
  agg_unit->~ObProxyAggUnit();
  allocator.free(agg_unit);
}

int ObProxyComparableAggUnit::merge(common::ObIArray<ObObj> &agg_values)
{
  int ret = OB_SUCCESS;

  if (!agg_values.empty()) {
    ObObj obj = agg_values.at(0);
    if (is_first_) {
      obj_ = obj;
      is_first_ = false;
    } else if (!obj.is_null() && obj_.is_null()) {
      obj_ = obj;
    } else if (!obj.is_null()) {
      int cmp = obj.compare(obj_);
      if ((asc_ && cmp < 0) || (!asc_ && cmp > 0)) {
        obj_ = obj;
      }
    }
  }

  return ret;
}

int ObProxyAccumulationAggUnit::merge(common::ObIArray<ObObj> &agg_values)
{
  int ret = OB_SUCCESS;

  if (!agg_values.empty()) {
    ObObj obj = agg_values.at(0);
    if (is_first_) {
      obj_ = obj;
      is_first_ = false;
    } else if (!obj.is_null() && obj_.is_null()) {
      obj_ = obj;
    } else if (!obj.is_null()) {
      ObProxyExprCtx ctx(0, dbconfig::TESTLOAD_NON, false, &allocator_);
      ObProxyExprCalcItem calc_item;
      ObProxyExprAdd add_expr;
      ObProxyExprConst first_expr;
      ObProxyExprConst second_expr;
      ObSEArray<ObObj, 4> result_obj;

      ctx.set_scale(scale_);
      first_expr.set_object(obj_);
      second_expr.set_object(agg_values.at(0));
      if (OB_FAIL(add_expr.add_param_expr(&first_expr))) {
        LOG_WARN("fail to add first expr", K(ret));
      } else if (OB_FAIL(add_expr.add_param_expr(&second_expr))) {
        LOG_WARN("fail to add second expr", K(ret));
      } else if (add_expr.calc(ctx, calc_item, result_obj)) {
        LOG_WARN("fail to calc", K(ret));
      } else {
        obj_ = result_obj.at(0);
      }
    }
  }

  return ret;
}

ObAggregateFunction::ObAggregateFunction(common::ObIAllocator &allocator,
                                common::ObSEArray<ObProxyExpr*, 4> &select_exprs)
                             : //expr_ctx_(NULL, NULL, NULL, &allocator, NULL),
                               expr_ctx_(NULL),
                               is_sort_based_gby_(false),
                               select_exprs_(select_exprs),
                               allocator_(allocator),
                               group_col_idxs_(NULL),
                               sort_columns_(NULL),
                               agg_rows_(NULL),
                               result_rows_(NULL)
                               //agg_rows_(*(new ResultRows(ENGINE_ARRAY_NEW_ALLOC_SIZE, allocator)))
                               //result_rows(*(new ResultRows(ENGINE_ARRAY_NEW_ALLOC_SIZE, allocator_)))
{}

ObAggregateFunction::~ObAggregateFunction()
{
  if (OB_NOT_NULL(expr_ctx_)) {
    expr_ctx_->~ObExprCtx();
    expr_ctx_ = NULL;
  }
  if (OB_NOT_NULL(sort_columns_)) {
    sort_columns_->~SortColumnArray();
    sort_columns_ = NULL;
  }
  if (OB_NOT_NULL(agg_rows_)) {
    agg_rows_->~ResultRows();
    agg_rows_ = NULL;
  }
  if (OB_NOT_NULL(result_rows_)) {
    result_rows_->~HashTable();
    result_rows_ = NULL;
  }
  group_col_idxs_ = NULL;
}
//ObAggregateFunction::ObAggregateFunction()
//  : allocator_(*(dynamic_cast<common::ObIAllocator*>(new ObArenaAllocator())))
//{}

int ObAggregateFunction::handle_all_result(ResultRow *&row)
{
  bool has_inited_normal_cell = false;
  //bool inited_normal_cells = false;
  int ret = common::OB_SUCCESS;
  void *buf = NULL;
  buf = allocator_.alloc(sizeof(ResultRow));
  if (OB_ISNULL(buf)) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("handle_all_result not have enough memory", K(ret), K(sizeof(sizeof(ResultRow))));
  } else if (OB_ISNULL(row = new (buf) ResultRow(ENGINE_ARRAY_NEW_ALLOC_SIZE, allocator_))) {
    ret = common::OB_ERROR;
    LOG_WARN("handle_all_result init ResultRow error", K(ret));
  } else if (agg_rows_->count() > 0
              && OB_NOT_NULL(agg_rows_->at(0))) {
             // && agg_rows_.at(0)->count() == result_fields_->count())) {
    ResultRow &o_row = *agg_rows_->at(0);
    int64_t column_count = o_row.count();//result_fields_->count();
    for (int64_t i = 0; i < column_count; i++) {
      row->push_back(o_row.at(i));
    }
    int64_t rows_count = agg_rows_->count();
    for (int64_t i = 1; OB_SUCC(ret) && i < rows_count; i++) {
      if (OB_ISNULL(agg_rows_->at(i))) {
        ret = common::OB_ERROR;
        LOG_WARN("invalid row to calc agg", K(ret));
      } else if (OB_FAIL(cal_row_agg(*row, *agg_rows_->at(i), has_inited_normal_cell))) {
        LOG_WARN("calc agg error", K(ret));
      }
    }
  } else {
    ret = common::OB_ERROR;
    LOG_WARN("inner error when calc agg", K(ret));
  }
  return ret;
}

int ObAggregateFunction::handle_all_hash_result(ResultRows *rows)
{
  int ret = common::OB_SUCCESS;
  bool has_inited_normal_cell = true;
  void *buf = NULL;
  if (OB_ISNULL(rows)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input", K(rows));
  } else if (agg_rows_->count() > 0
              && OB_NOT_NULL(agg_rows_->at(0))) {
              //&& agg_rows_.at(0)->count == select_exprs_->count())) {
    ObGbyHashCols *col = NULL;
    int64_t rows_count = agg_rows_->count();
    ResultRow *row = NULL;
    for (int64_t i = 1; OB_SUCC(ret) && i < rows_count; i++) {
      if (OB_ISNULL(row = rows->at(i))) {
        ret = common::OB_ERROR;
        LOG_WARN("invalid row to calc agg", K(ret));
      } else {
        buf = static_cast<ObGbyHashCols*>(allocator_.alloc(sizeof(ObGbyHashCols)));
        col = new (buf) ObGbyHashCols();
        if (OB_FAIL(col->init(row, group_col_idxs_, 0))) {
          LOG_WARN("inner error to init hash_col", K(ret));
        } else if (FALSE_IT(col->inner_hash())) {
          // never be here
        } else {
          int loc = col->hash_val_ & BUCKET_MASK;
          ObGbyHashCols* bucket = result_rows_->buckets_.at(loc);
          if (OB_ISNULL(bucket)) {
            result_rows_->buckets_.at(loc) = col;
          } else {
            ObGbyHashCols *head = result_rows_->buckets_[loc];
            while (OB_NOT_NULL(head)) {
              if (is_same_group(*head->row_, *col->row_)) {
                if (OB_FAIL(cal_row_agg(*head->row_, *col->row_, has_inited_normal_cell))) { //TODO free col
                  LOG_WARN("inner error to calc agg", K(ret));
                }
                break;
              }
              head = static_cast<ObGbyHashCols*>(head->next_);
            }
          }

        }
      }
    }

    /* Get all result */
    if (OB_SUCC(ret)) {
      for (int i = 0; i < BUCKET_BUF_SIZE; i++) {
        ObGbyHashCols *bucket = result_rows_->buckets_.at(i);
        while (OB_NOT_NULL(bucket)) {
            rows->push_back(bucket->row_);
            bucket = static_cast<ObGbyHashCols*>(bucket->next_);
        }
      }
      ObMemorySort *mem_sort_impl = NULL;//new ObMemorySort(sort_columns_);
      void *tmp_buf = NULL;
      ResultRows *new_rows = NULL;
      if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ResultRows)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("no have enough memory to init", K(ret), K(sizeof(ResultRows)));
      } else if (OB_ISNULL(new_rows = new (tmp_buf) ResultRows(ENGINE_ARRAY_NEW_ALLOC_SIZE, allocator_))) {
        ret = common::OB_ERR_UNEXPECTED;
        LOG_WARN("init ResultRows failed", K(ret), K(new_rows));
      } else if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObMemorySort)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("no have enough memory to init", K(ret), K(sizeof(ObMemorySort)));
      } else {
        mem_sort_impl = new (tmp_buf) ObMemorySort(*sort_columns_, allocator_, *new_rows);
      }
      if (OB_ISNULL(mem_sort_impl)) {
        ret = common::OB_ERROR;
        LOG_WARN("inner error to init memory sort", K(ret));
      } else {
        mem_sort_impl->set_sort_rows(*rows);
        if (OB_FAIL(mem_sort_impl->sort_rows())) {
          LOG_WARN("memory sort error", K(ret));
        } else if (OB_FAIL(mem_sort_impl->fetch_final_results(*rows))) {
          LOG_WARN("memory sort error in ObProxySortOp", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAggregateFunction::cal_row_agg(ResultRow &obj_row, ResultRow &src_row, bool &has_inited_normal_cell)
{
  //has_inited_normal_cell = false;
  bool is_null_value = false;
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(&select_exprs_)
        || OB_ISNULL(&obj_row)
        || OB_ISNULL(&src_row)
        || obj_row.count() != src_row.count()) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("failed to call the aggregate the result row", K(ret));
  } else {
    int64_t row_count = obj_row.count();
    int64_t sel_count = select_exprs_.count();
    int64_t row_loc = 0;
    for (int64_t i = sel_count - 1; i >= 0; i--) {
      if (OB_ISNULL(select_exprs_[i])) {
        ret = common::OB_ERROR;
        LOG_WARN("erro to call thre cell agg", K(ret));
        break;
      } else if (select_exprs_[i]->has_agg()) {
        row_loc = row_count - sel_count + i;
        if (src_row.at(row_loc)->is_null()) { //MAX/MIN/SUM is NULL if not have any row /0 for COUNT
          is_null_value = true;
          break; //it is the NULL ObObj, not calc any more
        }
        if (OB_FAIL(calc_aggr_cell(select_exprs_[i]->get_expr_type(),
                    *obj_row.at(row_loc), *src_row.at(row_loc)))) {
          ret = common::OB_ERROR;
          LOG_WARN("error to call thre cell agg", K(ret));
          break;
        }
      }
    }
    if (OB_SUCC(ret) && !is_null_value && !has_inited_normal_cell) {
      LOG_DEBUG(" ObAggregateFunction::cal_row_agg init normal cell", K(ret), K(obj_row), K(src_row));
      bool all_is_null = true;
      for (int64_t i = sel_count - 1, j = row_count - 1; i >= 0 && j >= 0 ;) {
        //ObProxyExpr::print_proxy_expr(select_exprs_[i]);
        if (select_exprs_[i]->has_agg()) {
          i--;
          j--;
          continue;
        } else {
          //if calc COUNT for the field is not null but all other fields is null
          if (!select_exprs_[i]->is_star_expr()) {
            i--;
          }
          if (src_row.at(j)->is_null()) {
            j--;
            continue;
          }
          all_is_null = false;
          obj_row.at(j) = src_row.at(j);
          j--;
        }
      }
      has_inited_normal_cell = !(all_is_null);
    }
  }
  return ret;
}

int ObAggregateFunction::init(ObColInfoArray &group_col_idxs)
{
  int ret = common::OB_SUCCESS;
  char *tmp_buf = NULL;
  if (OB_ISNULL(tmp_buf = (char *)allocator_.alloc(sizeof(common::ObExprCtx)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("init not have enough memory", K(ret), K(sizeof(common::ObExprCtx)));
  } else if (OB_ISNULL(expr_ctx_ = new (tmp_buf) common::ObExprCtx(NULL, NULL, NULL, &allocator_, NULL))) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WARN("init construct error", K(ret), K(expr_ctx_));
//  } else if (OB_ISNULL(tmp_buf = (char *)allocator_.alloc(sizeof(ObColInfoArray)))) {
//    ret = common::OB_ALLOCATE_MEMORY_FAILED;
//    LOG_WARN("init not have enough memory", K(ret), K(sizeof(ObColInfoArray)));
//  } else if (OB_ISNULL(group_col_idxs_ = new (tmp_buf) ObColInfoArray(ENGINE_ARRAY_NEW_ALLOC_SIZE, allocator_))) {
//    ret = common::OB_ERR_UNEXPECTED;
//    LOG_WARN("init construct error", K(ret), K(group_col_idxs_));
  } else if (OB_ISNULL(tmp_buf = (char *)allocator_.alloc(sizeof(SortColumnArray)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("init not have enough memory", K(ret), K(sizeof(SortColumnArray)));
  } else if (OB_ISNULL(sort_columns_ = new (tmp_buf) SortColumnArray(ENGINE_ARRAY_NEW_ALLOC_SIZE, allocator_))) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WARN("init construct error", K(ret), K(sort_columns_));
  } else if (OB_ISNULL(tmp_buf = (char *)allocator_.alloc(sizeof(ResultRows)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("init not have enough memory", K(ret), K(sizeof(ResultRows)));
  } else if (OB_ISNULL(agg_rows_ = new (tmp_buf) ResultRows(ENGINE_ARRAY_NEW_ALLOC_SIZE, allocator_))) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WARN("init construct error", K(ret), K(agg_rows_));
  } else if (OB_ISNULL(tmp_buf = (char *)allocator_.alloc(sizeof(HashTable)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("init not have enough memory", K(ret), K(sizeof(HashTable)));
  } else if (OB_ISNULL(result_rows_ = new (tmp_buf) HashTable(allocator_))) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WARN("init construct error", K(ret), K(result_rows_));
  }

  group_col_idxs_ = &group_col_idxs;
  return ret;
}

int ObAggregateFunction::clone_cell(const common::ObObj &src_cell, common::ObObj &target_cell)
{
  int ret = common::OB_SUCCESS;
  if (ObNumberTC == src_cell.get_type_class()) {
    ret  = clone_number_cell(src_cell, target_cell);
  } else if (OB_UNLIKELY(src_cell.need_deep_copy())) {
//    target_cell = src_cell;
    char *buf = NULL;
    int64_t size = sizeof(int64_t) + src_cell.get_deep_copy_size();
    int64_t pos = 0;
    if (target_cell.get_type() == src_cell.get_type() && (NULL != target_cell.get_data_ptr()
        && 0 != target_cell.get_data_length())) {
      buf = (char *)((int64_t *)(target_cell.get_data_ptr()) - 1);
      if (size > *(int64_t *)buf) {
        size = size * 2;
        if (NULL != (buf = static_cast<char*>(allocator_.alloc(size)))) {
          *((int64_t *)buf) = size;
        } else {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
      }
    } else {
      if (NULL != (buf = static_cast<char*>(allocator_.alloc(size)))) {
        *((int64_t *)buf) = size;
      } else {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
    }

    LOG_DEBUG("ObAggregateFunction::clone_cell clone new cell", K(ret), K(size));

    if (OB_SUCC(ret)) {
      pos += sizeof(int64_t);
      ret = target_cell.deep_copy(src_cell, buf, size, pos);
    }

  } else {
    target_cell = src_cell;
  }
  return ret;
}

int ObAggregateFunction::clone_number_cell(const common::ObObj &src_cell, common::ObObj &target_cell)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(ObNumberTC != src_cell.get_type_class())) {
    //|| OB_UNLIKELY(src_cell.get_number_byte_length() > MAX_CALC_BYTE_LEN)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid", K(src_cell.get_type()), K(target_cell.get_type()),
         K(src_cell), K(ret));
  } else {
    target_cell = src_cell;
  }

  return ret;
}

int ObAggregateFunction::max_calc(common::ObObj &base,
                  const common::ObObj &other,
                  common::ObCollationType cs_type)
{
  int ret = common::OB_SUCCESS;
  if (!base.is_null() && !other.is_null()) {
    if (ObObjCmpFuncs::compare_oper_nullsafe(base, other, cs_type, CO_LT)) {
      // common::
      ret = clone_cell(other, base);
    }
  } else if (/*base.is_null() &&*/ !other.is_null()) {
    // base must be null!
    // if base is not null, the first 'if' will be match, not this 'else if'.
    ret = clone_cell(other, base);
  } else {
    // nothing.
  }
  return ret;
}

int ObAggregateFunction::min_calc(common::ObObj &base,
                  const common::ObObj &other,
                  common::ObCollationType cs_type)
{
  int ret = common::OB_SUCCESS;
  if (!base.is_null() && !other.is_null()) {
    if (ObObjCmpFuncs::compare_oper_nullsafe(base, other, cs_type, CO_GT)) {
      ret = clone_cell(other, base);
    }
  } else if (/*base.is_null() &&*/ !other.is_null()) {
    // base must be null!
    // if base is not null, the first 'if' will be match, not this 'else if'.
    ret = clone_cell(other, base);
  } else {
    // nothing.
  }
  return ret;
}

int ObAggregateFunction::add_calc(common::ObObj &res,
                  const common::ObObj &left,
                  const common::ObObj &right,
                  const common::ObTimeZoneInfo *tz_info)
{
  UNUSED(tz_info);
  int ret = common::OB_SUCCESS;
  bool need_expr_calc = true;
  if (OB_ISNULL(expr_ctx_->calc_buf_)) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WARN("expr context calc buf is null");
  } else if (ObNumberType == left.get_type() && ObNumberType == right.get_type()) {
    number::ObNumber sum;
    char buf_alloc[MAX_CALC_BYTE_LEN];
    ObDataBuffer allocator(buf_alloc, MAX_CALC_BYTE_LEN);

    if (OB_FAIL(left.get_number().add(right.get_number(), sum, *(expr_ctx_->calc_buf_)))) {
      LOG_WARN("number add failed", K(ret), K(left), K(right));
    } else {
      ObObj sum_obj;
      sum_obj.set_number(sum);
      ret = clone_number_cell(sum_obj, res); //or clone_cell
      need_expr_calc = false;
    }
  } else if (ob_is_int_tc(left.get_type()) && ob_is_int_tc(right.get_type())) {
    int64_t left_int = left.get_int();
    int64_t right_int = right.get_int();
    int64_t sum_int = left_int + right_int;
    if (ObAggregateFunction::is_int_int_out_of_range(left_int, right_int, sum_int)) {
      LOG_DEBUG("int64_t add overflow, will use number", K(left_int), K(right_int));
      need_expr_calc = true;
    } else {
      LOG_DEBUG("int64_t add does not overflow", K(left_int), K(right_int), K(sum_int));
      res.set_int(sum_int);
      need_expr_calc = false;
    }
  } else if (ob_is_uint_tc(left.get_type()) && ob_is_uint_tc(right.get_type())) {
    uint64_t left_uint = left.get_uint64();
    uint64_t right_uint = right.get_uint64();
    uint64_t sum_uint = left_uint + right_uint;
    if (ObAggregateFunction::is_uint_uint_out_of_range(left_uint, right_uint, sum_uint)) {
      LOG_DEBUG("uint64_t add overflow, will use number", K(left_uint), K(right_uint));
      need_expr_calc = true;
    } else {
      LOG_DEBUG("uint64_t add does not overflow", K(left_uint), K(right_uint), K(sum_uint));
      res.set_uint64(sum_uint);
      need_expr_calc = false;
    }
  } else if (ob_is_double_tc(left.get_type()) &&  ob_is_double_tc(right.get_type())) {
    double left_double = left.get_double();
    double right_double = right.get_double();
    double sum_double = left_double + right_double;
    //TODO not need to check exceed the range of double
    LOG_DEBUG("double add does not overflor", K(left_double), K(right_double), K(sum_double));
    res.set_double(sum_double);
    need_expr_calc = false;
  } else if (ob_is_float_tc(left.get_type()) &&  ob_is_float_tc(right.get_type())) {
    float left_float = left.get_float();
    float right_float = right.get_float();
    float sum_float = left_float + right_float;
    //TODO not need to check exceed the range of double
    LOG_DEBUG("double add does not overflor", K(left_float), K(right_float), K(sum_float));
    res.set_float(sum_float);
    need_expr_calc = false;
  }

  if (OB_SUCC(ret) && need_expr_calc) {
    LOG_WARN("ObObj type not supported or overflow when calc", K(left.get_type()), K(right.get_type()));
    LOG_DEBUG("ObObj type not supported", K(left), K(right));
    ret = common::OB_ERR_UNEXPECTED;
  }
  return ret;
}

int ObAggregateFunction::calc_aggr_cell(const ObProxyExprType aggr_fun,
                    common::ObObj &res1, //target_obj
                    common::ObObj &res2) //source_obj
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(&res1) || OB_ISNULL(&res2)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("oprands is invalid");
  }
  common::ObCollationType cs_type = res2.get_collation_type();
  LOG_DEBUG("ObAggregateFunction::calc_aggr_cell", K(cs_type), K(res1), K(res2));

  if (OB_SUCC(ret)) {
    switch(aggr_fun) {
      case OB_PROXY_EXPR_TYPE_FUNC_MAX: {
        ret = max_calc(res1, res2, cs_type);
        break;
      }
      case OB_PROXY_EXPR_TYPE_FUNC_MIN: {
        ret = min_calc(res1, res2, cs_type);
        break;
      }
      case OB_PROXY_EXPR_TYPE_FUNC_COUNT:
      case OB_PROXY_EXPR_TYPE_FUNC_SUM: {
        if (res1.is_null()) {
          res1 = res2; //call ObObj copy construct
          //ret = clone_cell(res2, res1);
        } else if (res2.is_null()) {
          // not need do anything
        } else {
          ret = add_calc(res1, res1, res2, NULL);
        }
        break;
      }
      case OB_PROXY_EXPR_TYPE_FUNC_AVG: {
        LOG_DEBUG("IS AVG Function do nothing", K(get_expr_type_name(aggr_fun)));
        break;
      }
      case OB_PROXY_EXPR_TYPE_NONE:
      case OB_PROXY_EXPR_TYPE_CONST:
      case OB_PROXY_EXPR_TYPE_SHARDING_CONST:
      case OB_PROXY_EXPR_TYPE_COLUMN:
      case OB_PROXY_EXPR_TYPE_FUNC_HASH:
      case OB_PROXY_EXPR_TYPE_FUNC_SUBSTR:
      case OB_PROXY_EXPR_TYPE_FUNC_CONCAT:
      case OB_PROXY_EXPR_TYPE_FUNC_TOINT:
      case OB_PROXY_EXPR_TYPE_FUNC_DIV:
      case OB_PROXY_EXPR_TYPE_FUNC_ADD:
      case OB_PROXY_EXPR_TYPE_FUNC_SUB:
      case OB_PROXY_EXPR_TYPE_FUNC_MUL: {
        LOG_DEBUG("NOT AGG Function do nothing", K(get_expr_type_name(aggr_fun)));
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown aggr function type", K(aggr_fun));
        break;
    }
  }
  return ret;
}

bool ObAggregateFunction::is_same_group(const ResultRow &row1, const ResultRow &row2)
{
  int ret = common::OB_SUCCESS;
  bool is_same = false;
  int64_t diff_loc = 0;
  if (OB_FAIL(is_same_group(row1, row2, is_same, diff_loc))) {
    LOG_DEBUG("internal error", K(ret), K(diff_loc));
    is_same = false;
  }
  return is_same;
}

int ObAggregateFunction::is_same_group(const ResultRow &row1, const ResultRow &row2,
    bool &result, int64_t &first_diff_pos)
{
  int ret = common::OB_SUCCESS;

  if (row1.count() != row2.count()) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input", K(ret), K(row1.count()), K(row2.count()));
  }

  const ObObj *lcell = NULL;
  const ObObj *rcell = NULL;
  LOG_DEBUG("ObAggregateFunction::is_same_group begin", K(row1), K(row2), K(group_col_idxs_->count()));

  result = true;
  for (int64_t i = 0; OB_SUCC(ret) && result && i < group_col_idxs_->count(); ++i) {
    int64_t group_idx = (row1.count() - 1 - group_col_idxs_->at(i).index_);
    if (OB_UNLIKELY(group_idx >= row1.count() || group_idx < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(row1), K(row2), K(group_idx));
    } else {
      lcell = row1.at(group_idx);//&row1.reserved_cells_[group_idx];
      rcell = row2.at(group_idx);//&row2.get_cell(group_idx); // read through projector
      LOG_DEBUG("ObAggregateFunction::is_same_group obj", K(*lcell), K(*rcell), K(group_idx));
      if (lcell->compare(*rcell, lcell->get_collation_type()) != 0) {
        first_diff_pos = i;
        result = false;
      }
    }
  }

  LOG_DEBUG("ObAggregateFunction::is_same_group end", K(row1), K(row2), K(ret), K(result));

  LOG_DEBUG("");
  return ret;
}

int ObAggregateFunction::add_row(ResultRow *row)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("inner error add_row", K(row));
  } else {
    agg_rows_->push_back(row);
  }
  return ret;
}

//When there's stored_row_ and reserved_cells_, use store_row's reserved_cells_ for calc hash.
//Other, use row_ for calc hash
uint64_t ObHashCols::inner_hash() const
{
  uint64_t result = 99194853094755497L;
  if (hash_col_idx_ != NULL) {
    int64_t group_col_count = hash_col_idx_->count();
    if (stored_row_ != NULL
      && stored_row_->reserved_cells_count_ > 0) {
      //not to used
      const ObObj *cells = stored_row_->reserved_cells_;
      for (int32_t i = 0; i < group_col_count; ++i) {
        if (hash_col_idx_->at(i).index_ < stored_row_->reserved_cells_count_) {
          const ObObj &cell = cells[hash_col_idx_->at(i).index_];
          result = cell.is_string_type() ?
               cell.varchar_hash(cell.get_collation_type(), result) :
               //cell.hash(result) :
               cell.hash(result);
        }
      }
    } else if (row_ != NULL && row_->count() > 0) {

      for (int64_t i = 0; i < group_col_count; ++i) {

        int64_t real_index = row_->count() - 1 - hash_col_idx_->at(i).index_;
        const ObObj &cell = *(row_->at(real_index));
        result = cell.is_string_type() ?
             cell.varchar_hash(cell.get_collation_type(), result) :
             //cell.varchar_murmur_hash(hash_col_idx_->at(i).cs_type_, result) :
             cell.hash(result);
      }
    }
  }
  return result;
}

//When there is stored_row_ reserved_cells, use stored_row_'s reserved_cells_ for calc equal.
//Other use row_.
bool ObHashCols::operator ==(const ObHashCols &other) const
{
  bool result = true;
  const ObObj *lcell = NULL;
  const ObObj *rcell = NULL;

  if (OB_ISNULL(hash_col_idx_)) {
    result = false;
  } else {
    int64_t group_col_count = hash_col_idx_->count();

    for (int32_t i = 0; i < group_col_count && result; ++i) {
      int64_t group_idx = row_->count() - 1 - hash_col_idx_->at(i).index_;
      if (stored_row_ != NULL) {
        if (group_idx < stored_row_->reserved_cells_count_) {
          lcell = &stored_row_->reserved_cells_[group_idx];
        }
      } else if (row_ != NULL) {
        if (row_->count() >= group_idx) {
          lcell = row_->at(group_idx);
        }
      }

      if (other.stored_row_ != NULL) {
        if (group_idx < other.stored_row_->reserved_cells_count_) {
          rcell = &other.stored_row_->reserved_cells_[group_idx];
        }
      } else if (other.row_ != NULL) {
        if (other.row_->count() >= group_idx) {
          rcell = other.row_->at(group_idx);
        }
      }
      if (NULL == lcell || NULL == rcell) {
        result = false;
      } else {
        result = lcell->is_equal(*rcell, hash_col_idx_->at(i).cs_type_);
      }
    }
  }
  return result;
}

void ObHashCols::set_stored_row(const common::ObRowStore::StoredRow *stored_row)
{
  stored_row_ = stored_row;
  row_ = NULL;
}

}
}
}
