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

int ObProxyProOp::handle_response_result(void *data, bool &is_final, ObProxyResultResp *&result)
{
  int ret = OB_SUCCESS;

  ObProxyResultResp *opres = NULL;
  ObProxyResultResp *res = NULL;
  ObProxyProInput *input = NULL;

  if (OB_ISNULL(data)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input", K(ret), K(data));
  } else if (OB_ISNULL(opres = reinterpret_cast<ObProxyResultResp*>(data))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObProxyProOp::handle_response_result not response result", K(data), KP(data));
  } else if (!opres->is_resultset_resp()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObProxyProOp::handle_response_result not response result", K(data), KP(opres), K(opres->is_resultset_resp()));
  } else if (OB_ISNULL(input = dynamic_cast<ObProxyProInput*>(get_input()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input is invalid", K(ret));
  } else {
    if (OB_ISNULL(get_result_fields())) {
      result_fields_ = opres->get_fields();
    }

    common::ObIArray<ObProxyExpr*> &calc_exprs = input->get_calc_exprs();
    int64_t derived_column_count = input->get_derived_column_count();
    int64_t real_column_count = opres->get_fields()->count() - derived_column_count;
    int64_t limit_offset = input->get_limit_offset();
    int64_t limit_offset_size = input->get_limit_size() + limit_offset;

    ResultRow *row = NULL;
    while (OB_SUCC(ret) && OB_SUCC(opres->next(row))) {
      ObProxyExprCtx ctx(0, dbconfig::TESTLOAD_NON, false, &allocator_);
      ObProxyExprCalcItem calc_item(row);
      ObSEArray<ObObj, 4> result_obj_array;

      for (int64_t i = 0; OB_SUCC(ret) && i < calc_exprs.count(); i++) {
        result_obj_array.reuse();
        ObProxyExpr *calc_expr = calc_exprs.at(i);
        if (OB_FAIL(calc_expr->calc(ctx, calc_item, result_obj_array))) {
          LOG_WARN("fail to get next row", K(i), K(ret));
        } else {
          *row->at(calc_expr->get_index()) = result_obj_array.at(0);
        }
      }

      if (OB_SUCC(ret)) {
        for (int64_t i = 0; OB_SUCC(ret) && i < derived_column_count; i++) {
          row->pop_back();
        }
      }
 
      if (OB_SUCC(ret)) {
        if (OB_FAIL(current_rows_.push_back(row))) {
          LOG_WARN("fail to push back row", K(ret));
        } else if (limit_offset_size > 0 && current_rows_.count() == limit_offset_size) {
          is_final = true;
          break;
        }
      }
    }

    if (ret == common::OB_ITER_END) {
      ret = common::OB_SUCCESS;
    }

    if (is_final) {
      void *tmp_buf_rows = NULL;
      void *tmp_buf_fields = NULL;
      if (OB_ISNULL(result_fields_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ObProxyProOp::handle_response_result not result field", K(data), KP(result_fields_));
      } else if (OB_ISNULL(tmp_buf_rows = allocator_.alloc(sizeof(ResultRows)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("no have enough memory to init", "size", sizeof(ResultRows), K(ret));
      } else if (OB_ISNULL(tmp_buf_fields = allocator_.alloc(sizeof(ResultFields)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("no have enough memory to init", K(op_name()), K(sizeof(ResultFields)), K(ret));
      } else {
        ResultRows *rows = new (tmp_buf_rows) ResultRows(ENGINE_ARRAY_NEW_ALLOC_SIZE, allocator_);
        ResultFields *new_fields = (new (tmp_buf_fields) ResultFields(ENGINE_ARRAY_NEW_ALLOC_SIZE, allocator_));

        for (int64_t i = 0; i < real_column_count; i++) {
          new_fields->push_back(result_fields_->at(i));
        }

        int64_t count = current_rows_.count();
        if (limit_offset_size > 0) {
          count = count > limit_offset_size ? limit_offset_size : count;
        }

        for (int64_t i = limit_offset; OB_SUCC(ret) && i < count; i++) {
          if (OB_FAIL(rows->push_back(current_rows_.at(i)))) {
            LOG_WARN("fail to push back row", K(i), K(count), K(limit_offset), K(limit_offset_size), K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(packet_result_set(res, rows, new_fields))) {
            LOG_WARN("packet resultset packet error", K(ret));
          } else if (OB_NOT_NULL(res)) {
            res->set_column_count(new_fields->count());
          }
        }
      }
    }
    result = res;
  }
  return ret;
}

}
}
}
