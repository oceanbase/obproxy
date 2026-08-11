/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "rpc/ob_request.h"
using namespace oceanbase::common;

namespace oceanbase
{
namespace rpc
{

char *ObRequest::easy_alloc(int64_t size) const
{
  void *buf = NULL;
  if (NULL == ez_req_ || NULL == ez_req_->ms) {
    RPC_LOG(EDIAG, "ez_req_ is not corret");
  } else {
  /* this function is defined for c driver client compile */
    UNUSED(size);
    buf = NULL;
    // buf = easy_pool_alloc(
    //     ez_req_->ms->pool, static_cast<uint32_t>(size));
  }
  return static_cast<char*>(buf);
}

} //end of namespace rpc
} //end of namespace oceanbase
