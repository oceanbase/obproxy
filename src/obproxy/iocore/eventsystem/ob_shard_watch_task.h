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
