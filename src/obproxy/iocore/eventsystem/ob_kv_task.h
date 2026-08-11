/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_KV_TASK_H
#define OBPROXY_KV_TASK_H

#include "iocore/eventsystem/ob_event_processor.h"
#include "iocore/eventsystem/ob_event.h"
#include "iocore/eventsystem/ob_protected_queue_thread_pool.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{
extern ObEventThreadType ET_OBKV;

enum ObRpcSubReqIsolateMode : int64_t {
  NOT_ISOLAEION = 0,
  ISOLATE_TO_ALL_ASYNC_THREAD,
  ISOLATE_TO_PART_ASYNC_THREAD
};

class ObKvTaskProcessor : public ObProcessor
{
public:
  ObKvTaskProcessor() : ObProcessor(), thread_pool_event_queue_(NULL) {}
  virtual ~ObKvTaskProcessor() {}
  int start(const int64_t obkv_task_all_threads, const int64_t stacksize = DEFAULT_STACKSIZE);

private:
  ObProtectedQueueThreadPool *thread_pool_event_queue_;
};

extern ObKvTaskProcessor g_obkv_task_processor;
} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_KV_TASK_H