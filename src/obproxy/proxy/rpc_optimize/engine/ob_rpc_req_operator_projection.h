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

#ifndef OBPROXY_OB_PROXY_RPC_REQ_OPERATOR_PROJECTION_H
#define OBPROXY_OB_PROXY_RPC_REQ_OPERATOR_PROJECTION_H

#include "ob_rpc_req_operator.h"

namespace oceanbase {
namespace obproxy {
namespace engine {

class ObProxyRpcReqProOp : public ObProxyRpcReqOperator
{
public:
  ObProxyRpcReqProOp(proxy::ObRpcReq *input, common::ObIAllocator &allocator)
    : ObProxyRpcReqOperator(input, allocator) {
    set_rpc_op_type(PHY_RPC_PROJECTION);
  }

  ~ObProxyRpcReqProOp() {}
  virtual int handle_response_result(void *src, bool &is_final, proxy::ObRpcReq *&result);
};

}
}
}

#endif //OBPROXY_OB_PROXY_RPC_REQ_OPERATOR_PROJECTION_H
