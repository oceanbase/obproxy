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

#include "iocore/eventsystem/ob_event_system.h"
#include "iocore/net/ob_unix_net.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace event
{

void ObProtectedQueueThreadPool::enqueue(ObEvent *e)
{
  if (OB_ISNULL(e)) {
    LOG_WARN("event NULL, it should not happened");
  } else if (OB_UNLIKELY(e->in_the_prot_queue_) || OB_UNLIKELY(e->in_the_priority_queue_)) {
    LOG_WARN("event has already in queue, it should not happened", K(*e));
  } else {
    int ret = OB_SUCCESS;
    e->in_the_prot_queue_ = 1;
    atomic_list_.push(e);

    if (OB_FAIL(signal())) {
      LOG_WARN("fail to do signal, it should not happened", K(ret));
    }
  }
}

int ObProtectedQueueThreadPool::dequeue_timed(const ObHRTime timeout, ObEvent *&event)
{
  int ret = OB_SUCCESS;

  if (NULL != (event= (ObEvent *)(atomic_list_.pop()))) {
    event->in_the_prot_queue_ = 0;
  } else {
    if (OB_FAIL(mutex_acquire(&lock_))) {
      LOG_ERROR("fail to acquire mutex", K(ret));
    } else {
      if (atomic_list_.empty()) {
        timespec ts = hrtime_to_timespec(timeout);
        cond_timedwait(&might_have_data_, &lock_, &ts);
      }

      if (NULL != (event= (ObEvent *)(atomic_list_.pop()))) {
        event->in_the_prot_queue_ = 0;
      }

      int tmp_ret = OB_SUCCESS;
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = mutex_release(&lock_)))) {
        LOG_ERROR("fail to release mutex", K(tmp_ret));
      }
    }
  }

  return ret;
}

} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase
