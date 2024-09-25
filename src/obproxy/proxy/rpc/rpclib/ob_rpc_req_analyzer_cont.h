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
