/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OBPROXY_RPC_REQ_PARALLEL_EXECUTE_CONT_H
#define OBPROXY_RPC_REQ_PARALLEL_EXECUTE_CONT_H

#include "obutils/ob_async_common_task.h"
#include "ob_rpc_req_parallel_processor.h"

namespace oceanbase
{
namespace obproxy
{
namespace executor
{

class ObProxyRpcReqParallelExecuteCont : public obutils::ObAsyncCommonTask
{
public:
  ObProxyRpcReqParallelExecuteCont(event::ObProxyMutex *m, event::ObContinuation *cb_cont, event::ObEThread *submit_thread)
      : ObAsyncCommonTask(m, "rpc parallel execute cont", cb_cont, submit_thread),
        rpc_request_(NULL),
        cont_index_(-1), allocator_(NULL) {}
  ~ObProxyRpcReqParallelExecuteCont() {}

  int init(const ObProxyRpcParallelParam &parallel_param, const int64_t cont_index,
           ObIAllocator *allocator, const int64_t timeout_ms);
  void destroy();
  void free_sub_rpc_request();
  virtual int init_task();
  virtual int finish_task(void *data);
  virtual void *get_callback_data() {
    return static_cast<void *>(rpc_request_);
  };
  virtual void free_holding_object() {
    free_sub_rpc_request();
  }

private:
  proxy::ObRpcReq *rpc_request_;
  int64_t cont_index_;
  common::ObIAllocator *allocator_;
};

} // end of namespace executor
} // end of namespace obproxy
} // end of namespace oceanbase

#endif //OBPROXY_RPC_REQ_PARALLEL_EXECUTE_CONT_H
