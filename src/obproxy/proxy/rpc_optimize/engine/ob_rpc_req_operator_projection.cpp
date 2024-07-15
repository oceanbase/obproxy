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
#include "ob_rpc_req_operator_projection.h"
#include "proxy/rpc_optimize/executor/ob_rpc_req_parallel_execute_cont.h" //proxy::ObRpcReq

using namespace oceanbase::obproxy::proxy;

namespace oceanbase {
namespace obproxy {
namespace engine {

int ObProxyRpcReqProOp::handle_response_result(void *data, bool &is_final, proxy::ObRpcReq *&result)
{
  int ret = OB_SUCCESS;
  UNUSED(is_final);

  proxy::ObRpcReq *opres = NULL;

  if (OB_ISNULL(data)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input", K(ret), K(data));
  } else if (OB_ISNULL(opres = reinterpret_cast<proxy::ObRpcReq*>(data))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("ObProxyRpcReqProOp::handle_response_result not response result", K(data), KP(data), K_(rpc_trace_id));
  } else if (!opres->has_resultset_resp()) {
    ret = OB_ERR_UNEXPECTED;
    // LOG_WDIAG("ObProxyRpcReqProOp::handle_response_result not response result", K(data), KP(opres), K(opres->is_resultset_resp()));
    LOG_WDIAG("input is invalid", K(ret), K_(rpc_trace_id));
  } else {
    result = opres;
    LOG_DEBUG("handle response result", K(result), K_(rpc_trace_id));
  }
  return ret;
}

}
}
}
