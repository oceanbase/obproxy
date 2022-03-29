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

#ifndef OBPROXY_OB_PROXY_OPERATOR_PROJECTION_H
#define OBPROXY_OB_PROXY_OPERATOR_PROJECTION_H

#include "ob_proxy_operator.h"

namespace oceanbase {
namespace obproxy {
namespace engine {

class ObProxyOpCallback;
class ObProxyProOp : public ObProxyOperator
{
public:
  ObProxyProOp(ObProxyOpInput *input, common::ObIAllocator &allocator)
    : ObProxyOperator(input, allocator), current_rows_(ENGINE_ARRAY_NEW_ALLOC_SIZE, allocator_) {
    set_op_type(PHY_PROJECTION);
  }

  ~ObProxyProOp() {}
  virtual int get_next_row();
  virtual int handle_response_result(void *src, bool &is_final, ObProxyResultResp *&result);

private:
  ResultRows current_rows_;
};

class ObProxyProInput : public ObProxyOpInput {
public:
  ObProxyProInput() : ObProxyOpInput(), derived_column_count_(0),
                      calc_exprs_(ObModIds::OB_SE_ARRAY_ENGINE, ENGINE_ARRAY_NEW_ALLOC_SIZE) {}
  ~ObProxyProInput() {}

  void set_derived_column_count(int64_t derived_column_count) {
    derived_column_count_ = derived_column_count;
  }

  int64_t get_derived_column_count() { return derived_column_count_; }

  int set_calc_exprs(common::ObIArray<opsql::ObProxyExpr*> &calc_exprs) {
    return calc_exprs_.assign(calc_exprs);
  }
  common::ObIArray<opsql::ObProxyExpr*> &get_calc_exprs() { return calc_exprs_; }

private:
  int64_t derived_column_count_;
  common::ObSEArray<opsql::ObProxyExpr*, 4> calc_exprs_;
};

}
}
}

#endif //OBPROXY_OB_PROXY_OPERATOR_PROJECTION_H
