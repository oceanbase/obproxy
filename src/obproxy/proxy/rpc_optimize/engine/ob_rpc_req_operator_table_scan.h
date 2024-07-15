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

#ifndef OBPROXY_OB_PROXY_RPC_REQ_OPERATOR_TABLE_SCAN_H
#define OBPROXY_OB_PROXY_RPC_REQ_OPERATOR_TABLE_SCAN_H

#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"
#include "ob_rpc_req_operator.h"
#include "ob_rpc_req_operator_async_task.h"
#include "proxy/rpc_optimize/executor/ob_rpc_req_parallel_processor.h"

namespace oceanbase {
namespace obproxy {
namespace proxy {
class ObRpcReq;
class ObProxyRpcAuthRequest;
}
namespace executor {
class ObProxyRpcParallelResp;
}
namespace engine {

class ObProxyRpcReqTableScanOp : public ObProxyRpcReqOperator
{
public:
  ObProxyRpcReqTableScanOp(proxy::ObRpcReq *input, common::ObIAllocator &allocator)
    : ObProxyRpcReqOperator(input, allocator), parallel_param_(),
      handle_stream_request_(false), rpc_parallel_cont_ptr_(NULL)
  {
    set_rpc_op_type(PHY_RPC_TABLE_SCAN);
  }

  virtual ~ObProxyRpcReqTableScanOp() {
    // 所有子任务都通过add_sub_rpc_req接口加入到了父请求的成员变量中，父请求释放时，会调用所有子请求的释放逻辑，等子请求全部释放完毕，最后回调父请求，最后释放
    parallel_param_.reset();
  };

  virtual int open(event::ObContinuation *cont, event::ObAction *&action, const int64_t timeout_ms = 0);
  virtual int execute_rpc_request();

  virtual ObProxyRpcReqOperator* get_child(const uint32_t idx);// { return NULL; }

  int handle_shard_rpc_request(proxy::ObRpcReq* rpc_request, common::ObSEArray<executor::ObProxyRpcParallelParam, 4> &parallel_param);
  int handle_shard_rpc_obkv_batch_request(proxy::ObRpcReq* rpc_request, common::ObSEArray<executor::ObProxyRpcParallelParam, 4> &parallel_param);
  int handle_shard_rpc_ls_request(proxy::ObRpcReq* rpc_request, common::ObSEArray<executor::ObProxyRpcParallelParam, 4> &parallel_param);
  int handle_shard_rpc_obkv_query_request(proxy::ObRpcReq* rpc_request, common::ObSEArray<executor::ObProxyRpcParallelParam, 4> &parallel_param);

  virtual int fetch_stream_response(int64_t cont_index);

protected:
  common::ObSEArray<executor::ObProxyRpcParallelParam, 4> parallel_param_;
  bool handle_stream_request_;
  void *rpc_parallel_cont_ptr_; //only used in fetch
};


}
}
}

#endif //OBPROXY_OB_PROXY_RPC_REQ_OPERATOR_TABLE_SCAN_H
