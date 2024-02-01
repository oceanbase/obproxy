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

#define USING_LOG_PREFIX PROXY_EVENT

#include "iocore/eventsystem/ob_shard_scan_all_task.h"
#include "iocore/eventsystem/ob_event_system.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace event
{
ObEventThreadType ET_SHARD_SCAN_ALL = ET_CALL;
ObShardScanAllTaskProcessor g_shard_scan_all_task_processor;

// Note that if the number of task_threads is 0, all continuations scheduled for
// ET_SHARD_SCAN_ALL ends up running on ET_CALL (which is the net-threads).
int ObShardScanAllTaskProcessor::start(const int64_t shard_scan_all_threads, const int64_t stacksize)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(thread_pool_event_queue_ = new (std::nothrow) ObProtectedQueueThreadPool())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to new ObProtectedQueueThreadPool", K(ret));
  } else if (OB_FAIL(thread_pool_event_queue_->init())) {
    LOG_WDIAG("fail to init thread_pool_event_queue", K(ret));
  } else if (OB_FAIL(spawn_event_threads(shard_scan_all_threads, "ET_SHARD_SCAN_ALL", stacksize, ET_SHARD_SCAN_ALL))) {
    LOG_WDIAG("fail to spawn event threads for ET_SHARD_SCAN_ALL", K(ret));
  }

  return ret;
}

int ObShardScanAllTaskProcessor::init_thread(ObEThread *&t)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(t->init())) {
    LOG_WDIAG("fail to init event", K(ret));
    delete t;
    t = NULL;
  } else {
    t->thread_pool_event_queue_ = thread_pool_event_queue_;
    t->is_need_thread_pool_event_ = true;
  }

  return ret;
}

ObEvent *ObShardScanAllTaskProcessor::schedule(ObEvent *event, const ObEventThreadType etype, const bool fast_signal)
{
  UNUSED(fast_signal);
  UNUSED(etype);

  if (NULL != event->continuation_->mutex_) {
    event->mutex_ = event->continuation_->mutex_;
  } else {
    event->continuation_->mutex_ = event->ethread_->mutex_;
    event->mutex_ = event->continuation_->mutex_;
  }
  event->is_thread_pool_event_ = true;
  thread_pool_event_queue_->enqueue(event);
  return event;
}

} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase
