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
#include "ob_proxy_operator_projection.h"

namespace oceanbase {
namespace obproxy {
namespace engine {
int ObProxyProOp::get_next_row()
{
  return ObProxyOperator::get_next_row();
}

//int ObProxyProOp::handle_response_result(executor::ObProxyParallelResp *pres, bool is_final,  ObProxyResultResp *&result)
int ObProxyProOp::handle_response_result(void *data, bool is_final,  ObProxyResultResp *&result)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("Enter ObProxyProOp::handle_response_result", K(op_name()), K(data));

  ObProxyResultResp *opres = NULL;
  ResultFields *origin_fields = NULL;
  ObProxyResultResp *res = NULL;
  void *tmp_buf = NULL;

  if (OB_ISNULL(data)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input", K(ret), K(data));
  } else if (OB_ISNULL(opres = reinterpret_cast<ObProxyResultResp*>(data))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObProxyProOp::handle_response_result not response result", K(data), KP(data));
  } else if (!opres->is_resultset_resp()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObProxyProOp::handle_response_result not response result", K(data), KP(opres), K(opres->is_resultset_resp()));
  } else {
    LOG_DEBUG("ObProxyProOp::process_ready_data:resultset_resp", K(opres), KP(opres));
    expr_has_calced_ = opres->get_has_calc_exprs();
    LOG_DEBUG("ObProxyProOp::process_ready_data resultset", K(opres), K(opres->get_fields()));
    if (OB_ISNULL(get_result_fields())) {
      result_fields_ = opres->get_fields();
    }


    ResultRow *row = NULL;

    common::ObSEArray<ObProxyExpr*, 4>& select_exprs =
      get_input()->get_select_exprs();

    int64_t limit_start = get_input()->get_op_limit_value();
    int64_t limit_offset = get_input()->get_op_offset_value();
    int64_t limit_topv = get_input()->get_op_top_value();

    int64_t added_row_count = get_input()->get_added_row_count();
    LOG_DEBUG("get all recored from res", K(ret), K(opres), K(opres->get_result_rows().count()),
               K(select_exprs.count()), K(added_row_count), K(limit_start), K(limit_offset), K(limit_topv));
    if (limit_topv != -1 && cur_result_rows_->count() >= limit_topv) {
      //reach up limit in SELECT
      LOG_DEBUG("not need to projection result any more, for reached the limit", K(ret), K(limit_topv));
    } else {
      while (OB_SUCC(ret) && (OB_SUCC(opres->next(row)))) {
        ResultRow *new_row = NULL;
        if (OB_ISNULL(row)) {
          LOG_DEBUG("ObProxyProOp::process_ready_data, handle all data", K(ret));
          break;
        } else if (OB_FAIL(init_row(new_row))) {
          LOG_WARN("ObProxyProOp::process_ready_data init row error", K(ret));
          ret = common::OB_ERROR;
          break;
        }

        LOG_DEBUG("get one recored from res", K(ret), K(opres), K(row));
        if (OB_FAIL(ObProxyOperator::calc_result(*row, *new_row, select_exprs, added_row_count))) {
          LOG_WARN("ObProxyProOp::process_ready_data calc result error", K(ret));
        } else if (OB_FAIL(put_result_row(new_row))) {
          LOG_WARN("ObProxyProOp::process_ready_data put row error", K(ret));
        }
      }
    }
    if (ret == common::OB_ITER_END) {
      ret = common::OB_SUCCESS;
      LOG_DEBUG("ObProxyProOp::process_ready_data, handle all data", K(ret));
    }
    if (OB_SUCC(ret)) {
      expr_has_calced_ = true;
    }
    if (is_final) {
      LOG_DEBUG("ObProxyOperator::process_complete_data get fields", K(ret),
                K(get_input()->get_added_row_count()));
      if (OB_ISNULL(origin_fields = get_result_fields())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ObProxyProOp::handle_response_result not result field", K(data), KP(result_fields_));
      } else if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ResultFields)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("no have enough memory to init", K(ret), K(op_name()), K(sizeof(ResultFields)));
      } else {
        ResultFields &new_fields = *(new (tmp_buf) ResultFields(array_new_alloc_size, allocator_));
        for (int64_t i = 0; i < origin_fields->count() - get_input()->get_added_row_count(); i++) {
          new_fields.push_back(origin_fields->at(i));
        }
   
        LOG_DEBUG("ObProxyOperator::process_complete_data packet final resultset", K(ret));
        if (OB_FAIL(get_limit_result(get_input()->get_op_limit_value(), get_input()->get_op_offset_value(),
                          cur_result_rows_))) {
          LOG_WARN("packet resultset packet limit error", K(ret));
        } else if (OB_FAIL(packet_result_set(res, cur_result_rows_, &new_fields))) {
          LOG_WARN("packet resultset packet error", K(ret));
        }
        res->set_column_count(new_fields.count());
        LOG_DEBUG("ObProxyOperator::process_complete_data packet final resultset over", K(ret), K(res),
                    K(res->get_column_count()), K(cur_result_rows_), KPC(cur_result_rows_),  K(new_fields));
      }
    }
    result = res;
  }
  return ret;
}

int ObProxyProOp::get_limit_result(int64_t start, int64_t offset,  ResultRows *rows)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(rows)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("ObProxyProOp::get_limit_result invalid input", K(ret), K(rows));
  } else {
    LOG_DEBUG("ObProxyProOp::get_limit_result remove rows by limit start", K(ret), K(start), K(offset),
                     K(rows->count()), K(*rows));
    if (start + offset < 0) { //no limit do nothing
      LOG_DEBUG("ObProxyProOp::get_limit_result not value to limit",
                        K(ret), K(start), K(offset), K(rows->count()));
    } else if (offset == -1) { // limt start, such as limit 10
      if (rows->count() > start) {
        while (rows->count() > start) {
          if (OB_FAIL(rows->remove(rows->count() - 1))) { //delete one record from backend and count_ will be minus
            LOG_WARN("ObProxyProOp::get_limit_result remove rows failed", K(ret), K(start), K(offset));
            break;
          }
        }
      }
    } else { //limit start, offset, such as limit 10, 2
      int64_t i = 0;
      for (i = 0; i < offset; i++) {
        if (i + start >= rows->count()) {
          break;
        }
        rows->at(i) = rows->at(i + start);
      }
      while (rows->count() > i) {
        if (OB_FAIL(rows->remove(rows->count() - 1))) { //delete one record from backend and count_ will be minus
          LOG_WARN("ObProxyProOp::get_limit_result remove rows failed", K(ret), K(start), K(offset));
          break;
        }
      }
    }
    LOG_DEBUG("ObProxyProOp::get_limit_result remove rows by limit end", K(ret), K(start), K(offset),
                  K(rows->count()), K(*rows));
  }

  return ret;
}

}
}
}
