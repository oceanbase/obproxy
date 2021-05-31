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
#if OB_HAVE_EVENTFD
#include <sys/eventfd.h>
#endif
#include "lib/profile/ob_trace_id.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace event
{
ObEThread::ObEThread()
    : ethreads_to_be_signalled_(NULL),
      ethreads_to_be_signalled_count_(0),
      id_(NO_ETHREAD_ID),
      event_types_(0),
      stack_start_(0),
      signal_hook_(NULL),
      ep_(NULL),
      net_handler_(NULL),
      net_poll_(NULL),
      inactivity_cop_(NULL),
      cs_map_(NULL),
      table_map_(NULL),
      partition_map_(NULL),
      routine_map_(NULL),
      congestion_map_(NULL),
      cache_cleaner_(NULL),
      sql_table_map_(NULL),
      random_seed_(NULL),
      warn_log_buf_(NULL),
      warn_log_buf_start_(NULL),
      tt_(REGULAR),
      pending_event_(NULL)
{
#if OB_HAVE_EVENTFD
  evfd_ = -1;
#else
  evpipe_[0] = -1;
  evpipe_[1] = -1;
#endif
  memset(thread_private_, 0, sizeof(thread_private_));
}

ObEThread::ObEThread(const ObThreadType att, const int64_t anid)
    : ethreads_to_be_signalled_(NULL),
      ethreads_to_be_signalled_count_(0),
      id_(anid),
      event_types_(0),
      stack_start_(0),
      signal_hook_(NULL),
      ep_(NULL),
      net_handler_(NULL),
      net_poll_(NULL),
      inactivity_cop_(NULL),
      cs_map_(NULL),
      table_map_(NULL),
      partition_map_(NULL),
      routine_map_(NULL),
      congestion_map_(NULL),
      cache_cleaner_(NULL),
      sql_table_map_(NULL),
      random_seed_(NULL),
      warn_log_buf_(NULL),
      warn_log_buf_start_(NULL),
      tt_(att),
      pending_event_(NULL)
{
#if OB_HAVE_EVENTFD
  evfd_ = -1;
#else
  evpipe_[0] = -1;
  evpipe_[1] = -1;
#endif
  memset(thread_private_, 0, sizeof(thread_private_));
}

ObEThread::ObEThread(const ObThreadType att, ObEvent *e)
    : ethreads_to_be_signalled_(NULL),
      ethreads_to_be_signalled_count_(0),
      id_(NO_ETHREAD_ID),
      event_types_(0),
      stack_start_(0),
      signal_hook_(NULL),
      ep_(NULL),
      net_handler_(NULL),
      net_poll_(NULL),
      inactivity_cop_(NULL),
      cs_map_(NULL),
      table_map_(NULL),
      partition_map_(NULL),
      routine_map_(NULL),
      congestion_map_(NULL),
      cache_cleaner_(NULL),
      sql_table_map_(NULL),
      random_seed_(NULL),
      warn_log_buf_(NULL),
      warn_log_buf_start_(NULL),
      tt_(att),
      pending_event_(e)
{
#if OB_HAVE_EVENTFD
  evfd_ = -1;
#else
  evpipe_[0] = -1;
  evpipe_[1] = -1;
#endif
  memset(thread_private_, 0, sizeof(thread_private_));
}

// Provide a destructor so that SDK functions which create and destroy
// threads won't have to deal with ObEThread memory deallocation.
ObEThread::~ObEThread()
{
  if (ethreads_to_be_signalled_count_ > 0) {
    flush_signals(this);
  }
  if (NULL != ethreads_to_be_signalled_) {
    delete[] ethreads_to_be_signalled_;
  }
}

int ObEThread::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(event_queue_external_.init())) {
    LOG_ERROR("fail to init event_queue_external_", K(ret));
  } else if (OB_ISNULL(mutex_ = new_proxy_mutex(ETHREAD_LOCK))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to allocator memory for ObProxyMutex", K(ret));
  } else if (OB_ISNULL(warn_log_buf_ =
      reinterpret_cast<char *>(ob_malloc(OB_PROXY_WARN_LOG_BUF_LENGTH, ObModIds::OB_PROXY_WARN_LOG_BUF)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc memory for warn log buf", K(ret));
  } else {
    mutex_ptr_ = mutex_;

    //when ObEThread was born, it must bound to current thread
    //and only be used in current thread
#ifdef OB_HAS_EVENT_DEBUG
    mutex_lock(MAKE_LOCATION(), reinterpret_cast<char*>(NULL), mutex_, this);
#else //OB_HAS_EVENT_DEBUG
    mutex_lock(mutex_, this);
#endif //OB_HAS_EVENT_DEBUG
    mutex_->thread_holding_count_ = OB_PROXY_INIT_MUTEX_THREAD_HOLDING_COUNT;
  }

  if (OB_SUCC(ret)) {
    if (DEDICATED == tt_) {
      if (OB_ISNULL(pending_event_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid parameters, pending_event_ must be specified", K(tt_), K(ret));
      }

    } else if (REGULAR == tt_) {
      if (OB_UNLIKELY(NO_ETHREAD_ID == id_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid parameters", K(tt_), K(id_), K(ret));
      } else if (OB_ISNULL(ethreads_to_be_signalled_ = new(std::nothrow) ObEThread *[MAX_EVENT_THREADS])) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to allocate memory for ethreads_to_be_signalled_", K(ret));
      } else {
        memset((char *)ethreads_to_be_signalled_, 0, MAX_EVENT_THREADS * sizeof(ObEThread *));
#if OB_HAVE_EVENTFD
        evfd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (OB_UNLIKELY(evfd_ < 0)) {
          if (EINVAL == errno) { // flags invalid for kernel <= 2.6.26
            evfd_ = eventfd(0, 0);
            if (OB_UNLIKELY(evfd_ < 0)) {
              ret = OB_ERR_SYS;
              LOG_ERROR("fail to create eventfd", K(evfd_), KERRMSGS, K(ret));
            } else if (OB_UNLIKELY(fcntl(evfd_, F_SETFD, FD_CLOEXEC) < 0)) {
              ret = OB_ERR_SYS;
              LOG_ERROR("fail to set event fd FD_CLOEXEC", KERRMSGS, K(ret));
            } else if (OB_UNLIKELY(fcntl(evfd_, F_SETFL, O_NONBLOCK) < 0)) {
              ret = OB_ERR_SYS;
              LOG_ERROR("fail to set event fd O_NONBLOCK", KERRMSGS, K(ret));
            } else {
              //do nothing
            }
          } else {
            ret = OB_ERR_SYS;
            LOG_ERROR("fail to create eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)", K(evfd_), KERRMSGS, K(ret));
          }
        } else {
          //do nothing
        }
#else
        if (OB_UNLIKELY(pipe(evpipe_) < 0)) {
          ret = OB_ERR_SYS;
          LOG_ERROR("fail to create pipe", KERRMSGS, K(ret));
        } else if (OB_UNLIKELY(fcntl(evpipe_[0], F_SETFD, FD_CLOEXEC) < 0)) {
          ret = OB_ERR_SYS;
          LOG_ERROR("fail to set evpipe_[0] FD_CLOEXEC", KERRMSGS, K(ret));
        } else if (OB_UNLIKELY(fcntl(evpipe_[0], F_SETFL, O_NONBLOCK) < 0)) {
          ret = OB_ERR_SYS;
          LOG_ERROR("fail to set evpipe_[0] O_NONBLOCK", KERRMSGS, K(ret));
        } else if (OB_UNLIKELY(fcntl(evpipe_[1], F_SETFD, FD_CLOEXEC) < 0)) {
          ret = OB_ERR_SYS;
          LOG_ERROR("fail to set evpipe_[1] FD_CLOEXEC", KERRMSGS, K(ret));
        } else if (OB_UNLIKELY(fcntl(evpipe_[1], F_SETFL, O_NONBLOCK) < 0)) {
          ret = OB_ERR_SYS;
          LOG_ERROR("fail to set evpipe_[1] O_NONBLOCK", KERRMSGS, K(ret));
        } else {
          //do nothing
        }
#endif
      }
    } else {
      //others == tt_
    }
  }
  return ret;
}

inline void ObEThread::process_event(ObEvent *e, const int calling_code)
{
  if (OB_UNLIKELY(e->in_the_prot_queue_) || OB_UNLIKELY(e->in_the_priority_queue_)) {
    LOG_WARN("event should not in queue here", K(*e));
  } else {
    MUTEX_TRY_LOCK(lock, e->mutex_, this);
    if (!lock.is_locked()) {
      e->timeout_at_ = cur_time_ + DELAY_FOR_RETRY;
      event_queue_external_.enqueue_local(e);
    } else {
      if (e->cancelled_) {
        free_event(*e);
      } else {
        const ObContinuation *c_temp = e->continuation_;

        // set trace id
        ObCurTraceId::set(reinterpret_cast<uint64_t>(e->continuation_->mutex_.ptr_));
        // set stack start;
        stack_start_ = reinterpret_cast<int64_t>(&c_temp);
        // handle event
        e->continuation_->handle_event(calling_code, e);

        if (OB_UNLIKELY(e->in_the_priority_queue_)) {
          LOG_WARN("event should not in in_the_priority_queue here", K(*e));
        } else if (OB_UNLIKELY(c_temp != e->continuation_)) {
          LOG_WARN("event should not in in_the_priority_queue here", K(*e));
        } else {/*do nothing*/}
        MUTEX_RELEASE(lock);

        if (0 != e->period_) {
          if (!e->in_the_prot_queue_ && !e->in_the_priority_queue_) {
            if (e->period_ < 0) {
              e->timeout_at_ = e->period_;
            } else {
              cur_time_ = get_hrtime();
              e->timeout_at_ = cur_time_ + e->period_;
            }
            event_queue_external_.enqueue_local(e);
          }
        } else if (!e->in_the_prot_queue_ && !e->in_the_priority_queue_) {
          free_event(*e);
        }
      }
    }
  }
}

inline void ObEThread::dequeue_local_event(Que(ObEvent, link_) &negative_queue)
{
  ObEvent *e = NULL;
  ObEvent *e_prev = NULL;
  ObEvent *e_next = NULL;
  while (NULL != (e = event_queue_external_.dequeue_local())) {
    if (e->cancelled_) {
      free_event(*e);
    } else if (0 == e->timeout_at_) { // IMMEDIATE
      // give priority to immediate events
      if (OB_UNLIKELY(0 != e->period_)) {
        LOG_WARN("period should be zero when in event_queue_external_", K(*e));
      }
      process_event(e, e->callback_event_);
    } else if (e->timeout_at_ > 0) { // INTERVAL
      event_queue_.enqueue(e, cur_time_);
    } else { // NEGATIVE
      // If its a negative event, it must be a result of
      // a negative event, which has been turned into a
      // timed-event (because of a missed lock), executed
      // before the poll. So, it must
      // be executed in this round (because you can't have
      // more than one poll between two executions of a
      // negative event)
      e_prev = NULL;
      e_next = negative_queue.head_;

      while (NULL != e_next && e_next->timeout_at_ > e->timeout_at_) {
        e_prev = e_next;
        e_next = e_next->link_.next_;
      }
      if (NULL == e_next) {
        negative_queue.enqueue(e);
      } else {
        negative_queue.insert(e, e_prev);
      }
    }
  }
}

// Execute loops forever on:
// Find the earliest event.
// Sleep until the event time or until an earlier event is inserted
// When its time for the event, try to get the appropriate continuation lock.
// If successful, call the continuation, otherwise put the event back into the queue.
void ObEThread::execute()
{
  switch (tt_) {
    case REGULAR: {
      Que(ObEvent, link_) negative_queue;
      ObEvent *e = NULL;
      ObHRTime next_time = 0;
      ObHRTime sleep_time = 0;
      bool done_one = false;

      //NOTE:: the precision of schedule in ObEThread is [-5ms, 60ms)
      while(true) {
        //1. execute all the available external events that have already been dequeued
        cur_time_ = get_hrtime_internal();
        dequeue_local_event(negative_queue);

        //2. execute all the eligible internal events
        do {
          done_one = false;
          event_queue_.check_ready(cur_time_);
          while (NULL != (e = event_queue_.dequeue_ready())) {
            if (e->cancelled_) {
              free_event(*e);
            } else {
              if (OB_UNLIKELY(e->timeout_at_ <= 0)) {
                LOG_WARN("timeout_at_ should bigger then zero when in event_queue_", K(*e));
              }
              done_one = true;
              process_event(e, e->callback_event_);
            }
          }
        } while (done_one);

        //3. execute any negative (poll) events
        if (NULL != negative_queue.head_) {
          if (ethreads_to_be_signalled_count_ > 0) {
            flush_signals(this);
          }
          // dequeue all the external events and put them in a local queue.
          // sleep until the event time or until an earlier event is inserted
          // If there are no external events available, don't do a cond_timedwait.
          if (!event_queue_external_.atomic_list_.empty()) {
            if (OB_UNLIKELY(OB_SUCCESS != event_queue_external_.dequeue_timed(next_time, false))) {
              LOG_WARN("fail to dequeue time in event_queue_external_");
            }
          }
          //3.1 execute all the available external events that have already been dequeued
          dequeue_local_event(negative_queue);

          // execute poll events
          while (NULL != (e = negative_queue.dequeue())) {
            process_event(e, EVENT_POLL);
          }
          if (!event_queue_external_.atomic_list_.empty()) {
            if (OB_UNLIKELY(OB_SUCCESS != event_queue_external_.dequeue_timed(next_time, false))) {
              LOG_WARN("fail to dequeue time in event_queue_external_");
            }
          }

        //4. wait for the appropriate event
        } else {
          next_time = event_queue_.earliest_timeout();
          sleep_time = next_time - cur_time_;

          if (sleep_time > THREAD_MAX_HEARTBEAT_MSECONDS * HRTIME_MSECOND) {
            next_time = cur_time_ + THREAD_MAX_HEARTBEAT_MSECONDS * HRTIME_MSECOND;
          }
          // dequeue all the external events and put them in a local queue.
          // If there are no external events available, do a cond_timedwait.
          if (ethreads_to_be_signalled_count_ > 0) {
            flush_signals(this);
          }
          if (OB_UNLIKELY(OB_SUCCESS != event_queue_external_.dequeue_timed(next_time, true))) {
            LOG_WARN("fail to dequeue time in event_queue_external_");
          }
        }
      }
      break;//in fact, it will never break
    }

    case DEDICATED: {
      {
        MUTEX_LOCK(lock, pending_event_->mutex_, this);
        pending_event_->continuation_->handle_event(EVENT_IMMEDIATE, pending_event_);
      }
      free_event(*pending_event_);
      pending_event_ = NULL;
      break;
    }

    default: {
      LOG_ERROR("it should not arrive here", K(tt_));
      break;
    }
  } // End switch
}

} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase
