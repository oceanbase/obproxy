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

#include "ob_worker.h"

#include <stdlib.h>
#include "lib/ob_define.h"
#include "lib/time/ob_time_utility.h"
#include "lib/allocator/ob_malloc.h"

using namespace oceanbase::common;

// static variables
__thread char oceanbase::common::wbuf[sizeof (ObWorker)];
__thread ObWorker *ObWorker::self_ = NULL;

ObWorker::ObWorker()
    : run_status_(RS_PAUSED),
      timeout_ts_(INT64_MAX),
      rpc_tenant_id_(0),
      ssstore_allocator_(),
      allocator_(ObModIds::OB_WORKER_EXECUTION, OB_MALLOC_NORMAL_BLOCK_SIZE * 8),
      tidx_(-1)
{
  ssstore_allocator_.init(global_default_allocator, ObModIds::OB_SSSTORE);
}

bool ObWorker::sched_wait()
{
  run_status_ = RS_WAIT;
  return true;
}

bool ObWorker::sched_run(int64_t waittime)
{
  UNUSED(waittime);
  run_status_ = RS_RUN;
  check_wait();
  return true;
}

void ObWorker::set_timeout_ts(int64_t timeout_ts)
{
  timeout_ts_ = timeout_ts;
}

int64_t ObWorker::get_timeout_ts() const
{
  return timeout_ts_;
}

int64_t ObWorker::get_timeout_remain() const
{
  return timeout_ts_ - ObTimeUtility::current_time();
}

bool ObWorker::is_timeout() const
{
  return ObTimeUtility::current_time() >= timeout_ts_;
}

void ObWorker::set_rpc_tenant(uint64_t tenant_id)
{
  rpc_tenant_id_ = tenant_id;
}

void ObWorker::reset_rpc_tenant()
{
  rpc_tenant_id_ = 0;
}

uint64_t ObWorker::get_rpc_tenant() const
{
  return rpc_tenant_id_;
}

int oceanbase::common::worker_th_event(bool init)
{
  UNUSED(init);
  int ret = OB_SUCCESS;
  return ret;
}
