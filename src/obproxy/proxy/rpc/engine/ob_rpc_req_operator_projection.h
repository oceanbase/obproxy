/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
