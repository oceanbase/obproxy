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

#include "ob_rpc_req_optimizer_processor.h"
#include "lib/objectpool/ob_concurrency_objpool.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace optimizer
{

ObProxyRpcReqOptimizerProcessor g_ob_proxy_rpc_req_optimizer_processor;

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
