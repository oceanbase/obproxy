/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OBPROXY_RPC_PARALLEL_PROCESSOR_H
#define OBPROXY_RPC_PARALLEL_PROCESSOR_H

#include "iocore/eventsystem/ob_action.h"
#include "lib/container/ob_iarray.h"
#include "proxy/rpc/ob_rpc_req.h"

namespace oceanbase
{
namespace obproxy
{
namespace executor
{

class ObProxyRpcParallelParam
{
public:
  ObProxyRpcParallelParam() : request_(NULL), partition_id_(-1),
                              save_session_mode_(false), need_cancel_(false), need_retry_(false) {}
  ~ObProxyRpcParallelParam() {}

  TO_STRING_KV(K_(request), K_(partition_id));

public:
  proxy::ObRpcReq *request_;
  int64_t partition_id_;
  bool save_session_mode_;
  bool need_cancel_;
  bool need_retry_;
};

class ObProxyRpcReqParallelProcessor
{
public:
  ObProxyRpcReqParallelProcessor() {}
  ~ObProxyRpcReqParallelProcessor() {}

  int open(event::ObContinuation &cont, event::ObAction *&action,
           common::ObIArray<ObProxyRpcParallelParam> &parallel_param,
           common::ObIAllocator *allocator,
           bool is_steam_fetch,
           void *&rpc_paralle_cont_ptr,
           const int64_t timeout_ms = 0
           );

private:
  DISALLOW_COPY_AND_ASSIGN(ObProxyRpcReqParallelProcessor);
};

extern ObProxyRpcReqParallelProcessor g_ob_proxy_rpc_parallel_processor;
inline ObProxyRpcReqParallelProcessor &get_global_rpc_parallel_processor()
{
  return g_ob_proxy_rpc_parallel_processor;
}

} // end of namespace executor
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_RPC_PARALLEL_PROCESSOR_H
