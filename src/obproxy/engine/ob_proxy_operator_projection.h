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
    : ObProxyOperator(input, allocator) {
    set_op_type(PHY_PROJECTION);
  }

  ~ObProxyProOp() {};
  virtual int get_limit_result(int64_t start, int64_t offset,  ResultRows *rows);
  virtual int get_next_row();
  virtual int handle_response_result(void *src, bool is_final, ObProxyResultResp *&result);

protected:
  /* To temp preserve bulk records */
};

class ObProxyProInput : public ObProxyOpInput {
public:
  ObProxyProInput() : ObProxyOpInput() {}
  ObProxyProInput(const common::ObSEArray<ObProxyExpr*, 4> &select_exprs) 
    : ObProxyOpInput(select_exprs) {}
  ~ObProxyProInput() {}
  // set method use ObProxyOpInput::set_xx_xx(..)
};

}
}
}

#endif //OBPROXY_OB_PROXY_OPERATOR_PROJECTION_H
