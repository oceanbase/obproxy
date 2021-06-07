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
 */

#ifndef OBPROXY_PRIORITY_EVENT_QUEUE_H
#define OBPROXY_PRIORITY_EVENT_QUEUE_H

#include "iocore/eventsystem/ob_event.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{
// <1ms, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024
#define PQ_BUCKET_TIME(i) (HRTIME_MSECONDS(1) << (i))

class ObEThread;

class ObPriorityEventQueue
{
public:
  ObPriorityEventQueue();
  ~ObPriorityEventQueue() { }

  void enqueue(ObEvent *e, const ObHRTime now);
  void remove(ObEvent *e);
  void check_ready(const ObHRTime now);
  ObEvent *dequeue_ready();
  ObHRTime earliest_timeout();
  int64_t get_queue_size() const { return after_queue_size_; };

public:
  static const int64_t PQ_LIST_COUNT = 10;
  Que(ObEvent, link_) after_queue_[PQ_LIST_COUNT];

  ObHRTime last_check_time_;
  uint64_t last_check_buckets_;
  int64_t after_queue_size_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPriorityEventQueue);
};

inline ObPriorityEventQueue::ObPriorityEventQueue()
{
  last_check_time_ = common::get_hrtime_internal();
  last_check_buckets_ = static_cast<uint64_t>(last_check_time_ / PQ_BUCKET_TIME(0));
  after_queue_size_ = 0;
}

inline void ObPriorityEventQueue::enqueue(ObEvent *e, const ObHRTime now)
{
  if (OB_ISNULL(e)) {
    PROXY_EVENT_LOG(WARN, "event NULL, it should not happened");
  } else if (OB_UNLIKELY(e->in_the_priority_queue_) || OB_UNLIKELY(e->in_the_prot_queue_)) {
    PROXY_EVENT_LOG(WARN, "event has already in queue, it should not happened", K(*e));
  } else {
    ObHRTime t = e->timeout_at_ - now;
    uint32_t i = 0;

    // equivalent but faster
    if (t <= PQ_BUCKET_TIME(3)) {
      if (t <= PQ_BUCKET_TIME(1)) {
        if (t <= PQ_BUCKET_TIME(0)) {
          i = 0;
        } else {
          i = 1;
        }
      } else {
        if (t <= PQ_BUCKET_TIME(2)) {
          i = 2;
        } else {
          i = 3;
        }
      }
    } else {
      if (t <= PQ_BUCKET_TIME(7)) {
        if (t <= PQ_BUCKET_TIME(5)) {
          if (t <= PQ_BUCKET_TIME(4)) {
            i = 4;
          } else {
            i = 5;
          }
        } else {
          if (t <= PQ_BUCKET_TIME(6)) {
            i = 6;
          } else {
            i = 7;
          }
        }
      } else {
        if (t <= PQ_BUCKET_TIME(8)) {
          i = 8;
        } else {
          i = 9;
        }
      }
    }
    e->in_the_priority_queue_ = 1;
    e->in_heap_ = i & 0x0F;
    after_queue_[i].enqueue(e);
    ++after_queue_size_;
  }
}

inline void ObPriorityEventQueue::remove(ObEvent *e)
{
  if (OB_ISNULL(e)) {
    PROXY_EVENT_LOG(WARN, "event NULL, it should not happened");
  } else if (OB_UNLIKELY(!e->in_the_priority_queue_)) {
    PROXY_EVENT_LOG(WARN, "event is not in_the_priority_queue, it should not happened", K(*e));
  } else if (OB_UNLIKELY(OB_UNLIKELY(e->in_the_prot_queue_))) {
    PROXY_EVENT_LOG(WARN, "event has already in in_the_prot_queue_, it should not happened", K(*e));
  } else {
    e->in_the_priority_queue_ = 0;
    after_queue_[e->in_heap_].remove(e);
    --after_queue_size_;
  }
}

inline ObEvent *ObPriorityEventQueue::dequeue_ready()
{
  ObEvent *event_ret = after_queue_[0].dequeue();
  if (NULL != event_ret) {
    if (OB_UNLIKELY(!event_ret->in_the_priority_queue_)) {
      PROXY_EVENT_LOG(WARN, "event is not in_the_priority_queue, it should not happened", K(*event_ret));
    } else if (OB_UNLIKELY(OB_UNLIKELY(event_ret->in_the_prot_queue_))) {
      PROXY_EVENT_LOG(WARN, "event has already in in_the_prot_queue_, it should not happened", K(*event_ret));
    } else {
      event_ret->in_the_priority_queue_ = 0;
      --after_queue_size_;
    }
  }
  return event_ret;
}

inline void ObPriorityEventQueue::check_ready(const ObHRTime now)
{
  const uint64_t check_buckets = static_cast<uint64_t>(now / PQ_BUCKET_TIME(0));
  uint64_t todo_buckets = check_buckets ^ last_check_buckets_;//pass N times of 5ms
  last_check_time_ = now;
  last_check_buckets_ = check_buckets;
  todo_buckets &= ((1 << (PQ_LIST_COUNT - 1)) - 1);//limit to 0x1FF times, 2^0~2^9

  int64_t k = 0;
  while (0 != todo_buckets) {
    ++k;
    todo_buckets >>= 1;//get the highest bit, 0~9
  }

  ObEvent *e = NULL;
  ObHRTime offset = 0;
  int64_t j = 0;
  for (int64_t i = 1; i <= k; ++i) {
    Que(ObEvent, link_) q = after_queue_[i];
    after_queue_[i].reset();
    while (NULL != (e = q.dequeue())) {
      if (e->cancelled_) {
        e->in_the_priority_queue_ = 0;
        e->free();
        --after_queue_size_;
      } else {
        offset = e->timeout_at_ - now;
        for (j = i; j > 0 && offset <= PQ_BUCKET_TIME(j - 1);) {
          --j;
        }
        e->in_heap_ = j & 0x0F;
        after_queue_[j].enqueue(e);
      }
    }//end of while
  }//end of for
}

inline ObHRTime ObPriorityEventQueue::earliest_timeout()
{
  ObHRTime ret = last_check_time_ + HRTIME_FOREVER;
  bool find_head = false;
  for (int64_t i = 0; i < PQ_LIST_COUNT && !find_head; ++i) {
    if (NULL != after_queue_[i].head_) {
      ret = last_check_time_ + (PQ_BUCKET_TIME(i) / 2);
      find_head = true;
    }
  }
  return ret;
}

} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase

#endif
