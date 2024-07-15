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

#ifndef OB_COMMON_OB_LIGHTY_QUEUE_
#define OB_COMMON_OB_LIGHTY_QUEUE_

#include <stdint.h>
#include <stddef.h>
#include "lib/allocator/ob_malloc.h"
#include "lib/ob_define.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/lock/ob_futex.h"

namespace oceanbase
{
namespace common
{
struct Cond
{
  public:
  Cond(): seq_(0), n_waiters_(0) {}
  ~Cond() {}

  void signal()
  {
    (void)ATOMIC_FAA(&seq_, 1);
    if (ATOMIC_LOAD(&n_waiters_) > 0) {
      futex_wake((int32_t *)&seq_, INT32_MAX);
    }
  }
  uint32_t get_seq()
  {
    return ATOMIC_LOAD(&seq_);
  }
  void wait(const uint32_t cmp, const int64_t timeout)
  {
    if (timeout > 0) {
      struct timespec ts;
      make_timespec(&ts, timeout);
      (void)ATOMIC_FAA(&n_waiters_, 1);
      (void)futex_wait((int32_t *)&seq_, cmp, &ts);
      (void)ATOMIC_FAA(&n_waiters_, -1);
    }
  }
private:
  static struct timespec *make_timespec(struct timespec *ts, int64_t us)
  {
    if (OB_LIKELY(NULL != ts)) {
      ts->tv_sec = us / 1000000;
      ts->tv_nsec = 1000 * (us % 1000000);
    }
    return ts;
  }
private:
  uint32_t seq_;
  uint32_t n_waiters_;
private:
  DISALLOW_COPY_AND_ASSIGN(Cond);
};

class LightyQueue
{
public:
  LightyQueue() {}
  ~LightyQueue() { destroy(); }
public:
  int init(const uint64_t capacity, const uint32_t mod_id = ObModIds::OB_LIGHTY_QUEUE);
  void destroy() { queue_.destroy(); }
  void reset();
  int64_t size() const { return queue_.get_total(); }
  int64_t curr_size() const { return queue_.get_total(); }
  int64_t max_size() const { return queue_.capacity(); }
  bool is_inited() const { return queue_.is_inited(); }
  int push(void *data, const int64_t timeout = 0);
  int pop(void *&data, const int64_t timeout = 0);
  int pop_with_priority(void *&data, const int64_t timeout_us, uint32_t remain_limit);
private:
  typedef ObFixedQueue<void> Queue;
  Queue queue_;
  Cond cond_;
private:
  DISALLOW_COPY_AND_ASSIGN(LightyQueue);
};
}; // end namespace common
}; // end namespace oceanbase

#endif /* __OB_COMMON_OB_LIGHTY_QUEUE_H__ */
