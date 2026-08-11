/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PROXY

#include "ob_rpc_req_optimizer_processor.h"
#include "lib/objectpool/ob_concurrency_objpool.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace optimizer
{


int ObProxyRpcReqOptimizerProcessor::alloc_allocator(ObIAllocator *&allocator)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator = op_alloc_args(ObArenaAllocator, ObModIds::OB_PLAN_EXECUTE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc ObArenaAllocator", K(ret));
  }

  return ret;
}

int ObProxyRpcReqOptimizerProcessor::free_allocator(ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("allocator is null", K(ret));
  } else {
    LOG_DEBUG("MEM TOTAL temp before free:", K(allocator->total()));//, K(allocator->used()));
    op_free(reinterpret_cast<ObArenaAllocator*>(allocator));
    allocator = NULL;
  }

  return ret;
}

} // end of namespace optimizer
} // end of namespace obproxy
} // end of namespace oceanbase
