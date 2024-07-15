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
#ifndef OBPROXY_RPC_PARALLEL_PROCESSOR_H
#define OBPROXY_RPC_PARALLEL_PROCESSOR_H

#include "iocore/eventsystem/ob_action.h"
#include "lib/container/ob_iarray.h"
#include "proxy/rpc_optimize/ob_rpc_req.h"

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
                              use_last_rpc_proxy_(false), save_session_mode_(false) {}
  ~ObProxyRpcParallelParam() {}

  TO_STRING_KV(K_(request), K_(partition_id));

public:
  proxy::ObRpcReq *request_;
  int64_t partition_id_;
  bool use_last_rpc_proxy_;
  bool save_session_mode_;
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
