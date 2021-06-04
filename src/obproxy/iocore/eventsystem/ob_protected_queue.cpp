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

#define USING_LOG_PREFIX PROXY_EVENT

#include "iocore/eventsystem/ob_event_system.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace event
{
// The protected queue is designed to delay signaling of threads
// until some amount of work has been completed on the current thread
// in order to prevent excess context switches.
//
// Defining EAGER_SIGNALLING disables this behavior and causes
// threads to be made runnable immediately.
//
// #define EAGER_SIGNALLING

void ObProtectedQueue::enqueue(ObEvent *e, const bool fast_signal)
{
  if (OB_ISNULL(e)) {
    LOG_WARN("event NULL, it should not happened");
  } else if (OB_UNLIKELY(e->in_the_prot_queue_) || OB_UNLIKELY(e->in_the_priority_queue_)) {
    LOG_WARN("event has already in queue, it should not happened", K(*e));
  } else {
    int ret = OB_SUCCESS;
    ObEThread *e_ethread = e->ethread_;
    e->in_the_prot_queue_ = 1;
    bool was_empty = (NULL == atomic_list_.push(e));
    ++atomic_list_size_;
    ObEThread *inserting_thread = this_ethread();

    if (was_empty && inserting_thread != e_ethread) {
      // queue e->ethread in the list of threads to be signalled
      // inserting_thread == NULL means it is not a regular ObEThread
      if (NULL == inserting_thread || NULL == inserting_thread->ethreads_to_be_signalled_) {
        if (OB_FAIL(signal())) {
          LOG_WARN("fail to do signal, it should not happened", K(ret));
        }
        if (fast_signal) {
          if (NULL != e_ethread->signal_hook_) {
            e_ethread->signal_hook_(*e_ethread);
          }
        }
      } else {
        bool need_break = false;
#ifdef EAGER_SIGNALLING
        // Try to signal now and avoid deferred posting.
        if (OB_SUCC(e_ethread->event_queue_external_.try_signal())) {
          need_break = true;
        }
#endif
        if (!need_break) {
          if (fast_signal) {
            if (NULL != e_ethread->signal_hook_) {
              e_ethread->signal_hook_(*e_ethread);
            }
          }

          int64_t &count = inserting_thread->ethreads_to_be_signalled_count_;
          ObEThread **sig_e = inserting_thread->ethreads_to_be_signalled_;

          if ((count + 1) >= g_event_processor.event_thread_count_) {
            // we have run out of room
            if ((count + 1) == g_event_processor.event_thread_count_) {
              // convert to direct map, put each ethread (sig_e[i]) into
              // the direct map location: sig_e[sig_e[i]->id]
              ObEThread *cur = NULL;
              ObEThread *next = NULL;
              for (int64_t i = 0; i < count; ++i) {
                cur = sig_e[i];  // put this ethread
                while (NULL != cur && cur != (next = sig_e[cur->id_])) { // into this location
                  sig_e[cur->id_] = cur;
                  cur = next;
                }

                // if not overwritten
                if (NULL != sig_e[i] && sig_e[i]->id_ != i) {
                  sig_e[i] = NULL;
                }
              }
              ++count;
            }
            // we have a direct map, insert this ObEThread
            sig_e[e_ethread->id_] = e_ethread;
          } else {
            // insert into vector
            sig_e[count++] = e_ethread;
          }
        }//else need break
      }//end of else
    }//!was_empty || inserting_thread == e_ethread
  }//end of else
}

void flush_signals(ObEThread *thr)
{
  if (OB_ISNULL(thr) || OB_UNLIKELY(this_ethread() != thr)) {
    LOG_WARN("argument is error", K(thr), "this_ethread", this_ethread());
  } else {
    int ret = OB_SUCCESS;
    int64_t count = thr->ethreads_to_be_signalled_count_;
    if (count > g_event_processor.event_thread_count_) {
      count = g_event_processor.event_thread_count_;      // MAX
    }

    // Since the lock is only there to prevent a race in cond_timedwait
    // the lock is taken only for a short time, thus it is unlikely that
    // this code has any effect.
#ifdef EAGER_SIGNALLING
    for (int64_t i = 0; i < count; ++i) {
      // Try to signal as many threads as possible without blocking.
      if (NULL != thr->ethreads_to_be_signalled_[i]) {
        if (thr->ethreads_to_be_signalled_[i]->event_queue_external_.try_signal()) {
          thr->ethreads_to_be_signalled_[i] = 0;
        }
      }
    }
#endif
    for (int64_t i = 0; i < count; ++i) {
      if (NULL != thr->ethreads_to_be_signalled_[i]) {
        if (OB_FAIL(thr->ethreads_to_be_signalled_[i]->event_queue_external_.signal())) {
          LOG_WARN("failed to do signal, it should not happened", K(ret));
        }
        if (NULL != thr->ethreads_to_be_signalled_[i]->signal_hook_) {
          thr->ethreads_to_be_signalled_[i]->signal_hook_(*(thr->ethreads_to_be_signalled_[i]));
        }
        thr->ethreads_to_be_signalled_[i] = NULL;
      }
    }
    thr->ethreads_to_be_signalled_count_ = 0;
  }
}

int ObProtectedQueue::dequeue_timed(const ObHRTime timeout, const bool need_sleep)
{
  ObEvent *e = NULL;
  int ret = OB_SUCCESS;
  if (need_sleep) {
    if (OB_FAIL(mutex_acquire(&lock_))) {
      LOG_ERROR("failed to acquire mutex", K(ret));
    } else {
      if (atomic_list_.empty()) {
        timespec ts = hrtime_to_timespec(timeout);
        cond_timedwait(&might_have_data_, &lock_, &ts);
      }
      int tmp_ret = OB_SUCCESS;
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = mutex_release(&lock_)))) {
        LOG_ERROR("failed to release mutex", K(tmp_ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    e = static_cast<ObEvent *>(atomic_list_.popall());
    atomic_list_size_ = 0;

    // invert the list, to preserve order
    SLL<ObEvent, ObEvent::Link_link_> l;
    SLL<ObEvent, ObEvent::Link_link_> t;

    t.head_ = e;
    while (NULL != (e = t.pop())) {
      l.push(e);
    }
    // insert into localQueue
    while (NULL != (e = l.pop())) {
      if (!e->cancelled_) {
        local_queue_.enqueue(e);
        ++local_queue_size_;
      } else {
        e->in_the_prot_queue_ = 0;
        e->free();
      }
    }
  }
  return ret;
}

} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase
