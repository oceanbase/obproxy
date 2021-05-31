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

#include "ob_proxy_parallel_processor.h"
#include "ob_proxy_parallel_cont.h"
#include "utils/ob_proxy_hot_upgrader.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace executor
{

int ObProxyParallelProcessor::open(ObContinuation &cont, ObAction *&action, ObIArray<ObProxyParallelParam> &parallel_param,
                                   ObIAllocator *allocator, const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  ObProxyParallelCont *parallel_cont = NULL;
  if (OB_ISNULL(cont.mutex_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(&cont), K(ret));
  } else if (OB_UNLIKELY(&self_ethread() != cont.mutex_->thread_holding_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current thread is not equal with thread which holds sm mutex", K(ret));
  } else if (get_global_hot_upgrade_info().is_graceful_exit_timeout(get_hrtime())) {
    ret = OB_SERVER_IS_STOPPING;
    LOG_WARN("proxy need exit now", K(ret));
  } else if (OB_ISNULL(parallel_cont = op_alloc_args(ObProxyParallelCont, &cont, &self_ethread()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ObProxyParallelCont", K(ret));
  } else if (OB_FAIL(parallel_cont->do_open(action, parallel_param, allocator, timeout_ms))) {
    LOG_WARN("fail to open ObProxyParallelCont", K(ret));
  }

  if (OB_FAIL(ret) && (NULL != parallel_cont)) {
    parallel_cont->destroy();
    parallel_cont = NULL;
    action = NULL;
  }

  return ret;
}

ObProxyParallelProcessor g_ob_proxy_parallel_processor;

} // end of namespace executor
} // end of namespace obproxy
} // end of namespace oceanbase
