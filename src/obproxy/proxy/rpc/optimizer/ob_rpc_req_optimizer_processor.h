/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_RPC_REQ_OPTIMIZER_PROCESSOR_H
#define OBPROXY_RPC_REQ_OPTIMIZER_PROCESSOR_H

#include "lib/allocator/page_arena.h"

namespace oceanbase
{
namespace obproxy
{
namespace optimizer
{

class ObProxyRpcReqOptimizerProcessor
{
public:
  ObProxyRpcReqOptimizerProcessor() {}
  ~ObProxyRpcReqOptimizerProcessor() {}

  int alloc_allocator(common::ObIAllocator *&allocator);
  int free_allocator(common::ObIAllocator *allocator);

private:
  common::ObArenaAllocator allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObProxyRpcReqOptimizerProcessor);
};

static inline ObProxyRpcReqOptimizerProcessor &get_global_optimizer_rpc_req_processor()
{
  static ObProxyRpcReqOptimizerProcessor instance;
  return instance;
}

} // end of namespace optimizer
} // end of namespace obproxy
} // end of namespace oceanbase

#endif //OBPROXY_RPC_REQ_OPTIMIZER_PROCESSOR_H
