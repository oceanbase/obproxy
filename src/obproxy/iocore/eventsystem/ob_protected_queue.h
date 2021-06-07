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
 *
 * *************************************************************
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * **************************************************************
 *
 * Protected Queue, a FIFO queue with the following functionality:
 * (1). Multiple threads could be simultaneously trying to enqueue
 *      and dequeue. Hence the queue needs to be protected with mutex.
 * (2). In case the queue is empty, dequeue() sleeps for a specified
 *      amount of time, or until a new element is inserted, whichever
 *      is earlier
 */

#ifndef OBPROXY_PROTECTED_QUEUE_H
#define OBPROXY_PROTECTED_QUEUE_H

#include "iocore/eventsystem/ob_event.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{

class ObProtectedQueue
{
public:
  ObProtectedQueue() : is_inited_(false), atomic_list_size_(0), local_queue_size_(0) {}
  ~ObProtectedQueue() { }

  int init();
  void enqueue(ObEvent *e, const bool fast_signal = false);
  void enqueue_local(ObEvent *e);        // Safe when called from the same thread
  void remove(ObEvent *e);
  ObEvent *dequeue_local();
  int dequeue_timed(const ObHRTime timeout, const bool need_sleep);
  int signal();
  int try_signal();             // Use non blocking lock and if acquired, signal
  int64_t get_atomic_list_size() const { return atomic_list_size_; };
  int64_t get_local_queue_size() const { return local_queue_size_; };

public:
  bool is_inited_;
  common::ObAtomicList atomic_list_;
  ObMutex lock_;
  ObProxyThreadCond might_have_data_;
  Que(ObEvent, link_) local_queue_;

  int64_t atomic_list_size_;
  int64_t local_queue_size_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObProtectedQueue);
};

inline int ObProtectedQueue::init()
{
  int ret = common::OB_SUCCESS;
  ObEvent e;
  if (OB_FAIL(common::mutex_init(&lock_))) {
    PROXY_EVENT_LOG(WARN, "failed to init mutex", K(ret));
  } else if (OB_FAIL(atomic_list_.init("ObProtectedQueue", reinterpret_cast<char *>(&e.link_.next_) - reinterpret_cast<char *>(&e)))) {
    PROXY_EVENT_LOG(WARN, "failed to init atomic_list_", K(ret));
  } else if (OB_FAIL(cond_init(&might_have_data_))) {
    PROXY_EVENT_LOG(WARN, "failed to init ObProxyThreadCond", K(ret));
  } else {
    is_inited_ = 0;
  }
  return ret;
}

inline int ObProtectedQueue::signal()
{
  int ret = common::OB_SUCCESS;
  // Need to get the lock before you can signal the thread
  if (OB_FAIL(common::mutex_acquire(&lock_))) {
    PROXY_EVENT_LOG(ERROR, "failed to acquire lock", K(ret));
  } else {
    if (OB_FAIL(cond_signal(&might_have_data_))) {
      PROXY_EVENT_LOG(WARN, "failed to call cond_signal", K(ret));
    }

    int tmp_ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(common::OB_SUCCESS != ( tmp_ret = common::mutex_release(&lock_)))) {
      PROXY_EVENT_LOG(WARN, "fail to release mutex", K(tmp_ret));
    }
  }
  return ret;
}

inline int ObProtectedQueue::try_signal()
{
  int ret = common::OB_SUCCESS;
  // Need to get the lock before you can signal the thread
  if (common::mutex_try_acquire(&lock_)) {
    if (OB_FAIL(cond_signal(&might_have_data_))) {
      PROXY_EVENT_LOG(WARN, "failed to call cond_signal",  K(ret));
    }

    int tmp_ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(common::OB_SUCCESS != ( tmp_ret = common::mutex_release(&lock_)))) {
      PROXY_EVENT_LOG(WARN, "fail to release mutex", K(tmp_ret));
    }
  } else {
    ret = common::OB_ERR_SYS;
  }
  return ret;
}

// Called from the same thread (don't need to signal)
inline void ObProtectedQueue::enqueue_local(ObEvent *e)
{
  if (OB_ISNULL(e)) {
    PROXY_EVENT_LOG(WARN, "event NULL, it should not happened");
  } else if (OB_UNLIKELY(e->in_the_prot_queue_) || OB_UNLIKELY(e->in_the_priority_queue_)) {
    PROXY_EVENT_LOG(WARN, "event has already in queue, it should not happened", K(*e));
  } else {
    e->in_the_prot_queue_ = 1;
    local_queue_.enqueue(e);
    ++local_queue_size_;
  }
}

inline void ObProtectedQueue::remove(ObEvent *e)
{
  if (OB_ISNULL(e)) {
    PROXY_EVENT_LOG(WARN, "event NULL, it should not happened");
  } else if (OB_UNLIKELY(!e->in_the_prot_queue_)) {
    PROXY_EVENT_LOG(WARN, "event is not in_the_prot_queue_, it should not happened", K(*e));
  } else if (OB_UNLIKELY(e->in_the_priority_queue_)) {
    PROXY_EVENT_LOG(WARN, "event has already in queue, it should not happened", K(*e));
  } else {
    if (NULL == atomic_list_.remove(e)) {
      // Attention! if remove it from local queue, we will do not reduce atomic_list_size_
      local_queue_.remove(e);
      --local_queue_size_;
    } else {
      --atomic_list_size_;
    }
    e->in_the_prot_queue_ = 0;
  }
}

inline ObEvent *ObProtectedQueue::dequeue_local()
{
  ObEvent *event_ret = local_queue_.dequeue();
  if (NULL != event_ret) {
    if (OB_UNLIKELY(!event_ret->in_the_prot_queue_)) {
      PROXY_EVENT_LOG(WARN, "event is not in_the_prot_queue_, it should not happened", K(*event_ret));
    } else if (OB_UNLIKELY(event_ret->in_the_priority_queue_)) {
      PROXY_EVENT_LOG(WARN, "event has already in queue, it should not happened", K(*event_ret));
    } else {
      event_ret->in_the_prot_queue_ = 0;
      --local_queue_size_;
    }
  }
  return event_ret;
}

void flush_signals(ObEThread *t);

} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_PROTECTED_QUEUE_H
