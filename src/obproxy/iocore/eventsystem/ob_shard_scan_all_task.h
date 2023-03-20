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

#ifndef OBPROXY_SHARD_SCAN_ALL_TASK_H
#define OBPROXY_SHARD_SCAN_ALL_TASK_H

#include "iocore/eventsystem/ob_event_processor.h"
#include "iocore/eventsystem/ob_event.h"
#include "iocore/eventsystem/ob_protected_queue_thread_pool.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{
extern ObEventThreadType ET_SHARD_SCAN_ALL;

class ObShardScanAllTaskProcessor : public ObEventProcessor
{
public:
  ObShardScanAllTaskProcessor() : ObEventProcessor(), thread_pool_event_queue_(NULL) {}
  virtual ~ObShardScanAllTaskProcessor() {}
  int start(const int64_t shard_scan_all_threads, const int64_t stacksize = DEFAULT_STACKSIZE);

  virtual int init_thread(ObEThread *&t);
  virtual ObEvent *schedule(ObEvent *event, const ObEventThreadType etype, const bool fast_signal);

private:
  ObProtectedQueueThreadPool *thread_pool_event_queue_;
};

extern ObShardScanAllTaskProcessor g_shard_scan_all_task_processor;
} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_SHARD_SCAN_ALL_TASK_H
