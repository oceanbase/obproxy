/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_RPC_REQ_ANALYZE_CONT_H
#define OBPROXY_RPC_REQ_ANALYZE_CONT_H

#include "obutils/ob_async_common_task.h"
#include "proxy/rpc/rpclib/ob_rpc_req_analyzer.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

class ObRpcReqAnalyzerCont : public obutils::ObAsyncCommonTask
{
public:
  ObRpcReqAnalyzerCont(event::ObContinuation *cb_cont, event::ObEThread *submit_thread,
    const ObProxyRpcReqAnalyzeCtx &ctx, ObRpcReq *rpc_req)
      : ObAsyncCommonTask(cb_cont->mutex_, "operator cont", cb_cont, submit_thread),
        execute_thread_(NULL), ctx_(ctx), rpc_req_(rpc_req)
  {
    SET_HANDLER(&ObRpcReqAnalyzerCont::main_handler);
  }

  ~ObRpcReqAnalyzerCont() {}

  int main_handler(int event, void *data);
  virtual int init_task();
  virtual void *get_callback_data() { return &ctx_; }
  virtual void destroy();

private:
  event::ObEThread *execute_thread_;
  ObProxyRpcReqAnalyzeCtx ctx_;
  ObRpcReq *rpc_req_;
  DISALLOW_COPY_AND_ASSIGN(ObRpcReqAnalyzerCont);
};

} // end of namespace engine
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_RPC_REQ_ANALYZE_CONT_H
