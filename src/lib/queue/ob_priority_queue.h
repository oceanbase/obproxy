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

#ifndef OCEANBASE_QUEUE_OB_PRIORITY_QUEUE_
#define OCEANBASE_QUEUE_OB_PRIORITY_QUEUE_

#include "lib/lock/ob_seq_sem.h"
#include "lib/queue/ob_link_queue.h"

namespace oceanbase
{
namespace common
{
template <int PRIOS>
class ObPriorityQueue
{
public:
  enum { PRIO_CNT = PRIOS };

  ObPriorityQueue() : sem_(), queue_(), size_(0), limit_(INT64_MAX) {}
  ~ObPriorityQueue() {}

  void set_limit(int64_t limit) { limit_ = limit; }
  inline int64_t size() const { return ATOMIC_LOAD(&size_); }

  int push(ObLink* data, int priority)
  {
    int ret = OB_SUCCESS;
    if (ATOMIC_FAA(&size_, 1) > limit_) {
      ret = OB_SIZE_OVERFLOW;
    } else if (OB_UNLIKELY(NULL == data) || OB_UNLIKELY(priority < 0) || OB_UNLIKELY(priority >= PRIO_CNT)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WDIAG, "push error, invalid argument", KP(data), K(priority));
    } else if (OB_FAIL(queue_[priority].push(data))) {
      // do nothing
    } else {
      ret = sem_.post();
    }
    if (OB_FAIL(ret)) {
      (void)ATOMIC_FAA(&size_, -1);
    }
    return ret;
  }

  int pop(ObLink*& data, int64_t timeout_us, int prio_limit=PRIO_CNT)
  {
    int ret = OB_ENTRY_NOT_EXIST;
    if (OB_UNLIKELY(timeout_us < 0)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(EDIAG, "timeout is invalid", K(ret), K(timeout_us));
    } else {
      if (0 == sem_.wait(timeout_us)) {
        for(int i = 0; OB_ENTRY_NOT_EXIST == ret  && i < prio_limit; i++) {
          if (OB_SUCCESS == queue_[i].pop(data)) {
            ret = OB_SUCCESS;
          }
        }
        if (OB_FAIL(ret)) {
          // In case of some threads maybe don't check all queues but
          // cost semaphore that lead to semaphore lost, we post a new
          // one to wakeup another thread checking these queues. It's
          // not a graceful way but works.
          if (PRIO_CNT != prio_limit) {
            sem_.post();
          }
        }
      }
      if (OB_FAIL(ret)) {
        data = NULL;
      } else {
        (void)ATOMIC_FAA(&size_, -1);
      }
    }
    return ret;
  }

private:
  ObSeqSem sem_;
  ObLinkQueue queue_[PRIO_CNT];
  int64_t size_ CACHE_ALIGNED;
  int64_t limit_ CACHE_ALIGNED;
  DISALLOW_COPY_AND_ASSIGN(ObPriorityQueue);
};
} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_QUEUE_OB_PRIORITY_QUEUE_
