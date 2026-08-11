/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_SHARD_WATCH_TASK_H
#define OBPROXY_SHARD_WATCH_TASK_H

#include "iocore/eventsystem/ob_processor.h"
#include "iocore/eventsystem/ob_event.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{
extern ObEventThreadType ET_SHARD_WATCH;

class ObShardWatchProcessor : public ObProcessor
{
public:
  int start(const int64_t shard_watch_threads, const int64_t stacksize = DEFAULT_STACKSIZE);
};

extern ObShardWatchProcessor g_shard_watch_task_processor;
} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_GRPC_PARENT_TASK_H
