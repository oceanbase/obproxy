/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PROXY_EVENT

#include "iocore/eventsystem/ob_shard_watch_task.h"
#include "iocore/eventsystem/ob_event_system.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace event
{
ObEventThreadType ET_SHARD_WATCH = ET_NET;
ObShardWatchProcessor g_shard_watch_task_processor;

// Note that if the number of task_threads is 0, all continuations scheduled for
// ET_GRPC ends up running on ET_NET (which is the net-threads).
int ObShardWatchProcessor::start(const int64_t shard_watch_threads, const int64_t stacksize)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(g_event_processor.spawn_event_threads(shard_watch_threads, "ET_SHARD_WATCH", stacksize, ET_SHARD_WATCH))) {
    LOG_WDIAG("fail to spawn event threads for ET_SHARD_WATCH", K(ret));
  } else {
    LOG_INFO("succ to start shard watch threads", K(shard_watch_threads));
  }
  return ret;
}
} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase
