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
 */

#ifndef OBPROXY_ETHREAD_H
#define OBPROXY_ETHREAD_H

#include "iocore/eventsystem/ob_thread.h"
#include "iocore/eventsystem/ob_priority_event_queue.h"
#include "iocore/eventsystem/ob_protected_queue.h"
#include "iocore/eventsystem/ob_protected_queue_thread_pool.h"
#include "lib/container/ob_vector.h"

namespace oceanbase
{
namespace common
{
class ObMysqlRandom;
}
namespace obproxy
{
namespace obutils
{
class ObCongestionRefHashMap;
}
namespace proxy
{
class ObMysqlClientSessionMap;
class ObTableRefHashMap;
class ObPartitionRefHashMap;
class ObRoutineRefHashMap;
class ObSqlTableRefHashMap;
class ObCacheCleaner;
class ObBasePsEntryThreadCache;
}
namespace net
{
class ObEventIO;
class ObNetHandler;
class ObNetPoll;
class ObInactivityCop;
}

namespace prometheus
{
class ObThreadPrometheus;
}

namespace event
{
class ObEvent;
class ObContinuation;

typedef int (ObEThread::*schedule_handler_func) (ObEvent &event, const bool fast_signal);

enum ObThreadType
{
  REGULAR = 0,
  MONITOR,
  DEDICATED
};

enum ObDedicateThreadType
{
  DEDICATE_THREAD_NONE = 0,
  DEDICATE_THREAD_ACCEPT,
};

/**
 * ObEvent System specific type of thread.
 * The ObEThread class is the type of thread created and managed by
 * the ObEvent System. It is one of the available interfaces for
 * schedulling events in the event system (another two are the ObEvent
 * and ObEventProcessor classes).
 * In order to handle events, each ObEThread object has two event
 * queues, one external and one internal. The external queue is
 * provided for users of the ObEThread (clients) to append events to
 * that particular thread. Since it can be accessed by other threads
 * at the same time, operations using it must proceed in an atomic
 * fashion.
 * The internal queue, in the other hand, is used exclusively by the
 * ObEThread to process timed events within a certain time frame. These
 * events are queued internally and they may come from the external
 * queue as well.
 * Scheduling Interface:
 * There are eight schedulling functions provided by ObEThread and
 * they are a wrapper around their counterparts in ObEventProcessor.
 *
 * @see ObEventProcessor
 * @see ObEvent
 */
class ObEThread : public ObThread
{
public:
  ObEThread();
  ObEThread(const ObThreadType att, const int64_t anid);
  ObEThread(const ObThreadType att, ObEvent *e);
  virtual ~ObEThread();

  int init();

  int schedule(ObEvent &e, const bool fast_signal = false);

  /**
   * Schedules the continuation on this ObEThread to receive an event
   * as soon as possible.
   * Forwards to the ObEventProcessor the schedule of the callback to
   * the continuation 'c' as soon as possible. The event is assigned
   * to ObEThread.
   *
   * @param c      ObContinuation to be called back as soon as possible.
   * @param callback_event
   *               ObEvent code to be passed back to the
   *               continuation's handler. See the the ObEventProcessor class.
   * @param cookie User-defined value or pointer to be passed back
   *               in the ObEvent's object cookie field.
   *
   * @return Reference to an ObEvent object representing the schedulling
   *         of this callback.
   */
  ObEvent *schedule_imm(ObContinuation *c, const int callback_event = EVENT_IMMEDIATE,
                        void *cookie = NULL);
  ObEvent *schedule_imm_signal(ObContinuation *c, const int callback_event = EVENT_IMMEDIATE,
                               void *cookie = NULL);

  /**
   * Schedules the continuation on this ObEThread to receive an event
   * at the given timeout.
   * Forwards the request to the ObEventProcessor to schedule the
   * callback to the continuation 'c' at the time specified in
   * 'atimeout_at'. The event is assigned to this ObEThread.
   *
   * @param c      ObContinuation to be called back at the time specified
   *               in 'atimeout_at'.
   * @param atimeout_at
   *               Time value at which to callback.
   * @param callback_event
   *               ObEvent code to be passed back to the
   *               continuation's handler. See the ObEventProcessor class.
   * @param cookie User-defined value or pointer to be passed back
   *               in the ObEvent's object cookie field.
   *
   * @return A reference to an ObEvent object representing the schedulling
   *         of this callback.
   */
  ObEvent *schedule_at(ObContinuation *c, const ObHRTime atimeout_at,
                       const int callback_event = EVENT_INTERVAL, void *cookie = NULL);

  /**
   * Schedules the continuation on this ObEThread to receive an event
   * after the timeout elapses.
   * Instructs the ObEventProcessor to schedule the callback to the
   * continuation 'c' after the time specified in atimeout_in elapses.
   * The event is assigned to this ObEThread.
   *
   * @param c      ObContinuation to be called back after the timeout elapses.
   * @param atimeout_in
   *               Amount of time after which to callback.
   * @param callback_event
   *               ObEvent code to be passed back to the
   *               continuation's handler. See the ObEventProcessor class.
   * @param cookie User-defined value or pointer to be passed back
   *               in the ObEvent's object cookie field.
   *
   * @return A reference to an ObEvent object representing the schedulling
   *         of this callback.
   */
  ObEvent *schedule_in(ObContinuation *c, const ObHRTime atimeout_in,
                       const int callback_event = EVENT_INTERVAL, void *cookie = NULL);

  /**
   * Schedules the continuation on this ObEThread to receive an event
   * periodically.
   * Schedules the callback to the continuation 'c' in the ObEventProcessor
   * to occur every time 'aperiod' elapses. It is scheduled on this
   * ObEThread.
   *
   * @param c       ObContinuation to call back everytime 'aperiod' elapses.
   * @param aperiod Duration of the time period between callbacks.
   * @param callback_event
   *                ObEvent code to be passed back to the
   *                continuation's handler. See the Remarks section in the
   *                ObEventProcessor class.
   * @param cookie  User-defined value or pointer to be passed back
   *                in the ObEvent's object cookie field.
   *
   * @return A reference to an ObEvent object representing the schedulling
   *         of this callback.
   */
  ObEvent *schedule_every(ObContinuation *c, const ObHRTime aperiod,
                          const int callback_event = EVENT_INTERVAL, void *cookie = NULL);

  /**
   * Schedules the continuation on this ObEThread to receive an event
   * as soon as possible.
   * Schedules the callback to the continuation 'c' as soon as
   * possible. The event is assigned to this ObEThread.
   *
   * @param c      ObContinuation to be called back as soon as possible.
   * @param callback_event
   *               ObEvent code to be passed back to the
   *               continuation's handler. See the ObEventProcessor class.
   * @param cookie User-defined value or pointer to be passed back
   *               in the ObEvent's object cookie field.
   *
   * @return A reference to an ObEvent object representing the schedulling
   *         of this callback.
   */
  ObEvent *schedule_imm_local(ObContinuation *c, const int callback_event = EVENT_IMMEDIATE,
                              void *cookie = NULL);

  /**
   * Schedules the continuation on this ObEThread to receive an event
   * at the given timeout.
   * Schedules the callback to the continuation 'c' at the time
   * specified in 'atimeout_at'. The event is assigned to this
   * ObEThread.
   *
   * @param c      ObContinuation to be called back at the time specified
   *               in 'atimeout_at'.
   * @param atimeout_at
   *               Time value at which to callback.
   * @param callback_event
   *               ObEvent code to be passed back to the
   *               continuation's handler. See the ObEventProcessor class.
   * @param cookie User-defined value or pointer to be passed back
   *               in the ObEvent's object cookie field.
   *
   * @return A reference to an ObEvent object representing the schedulling
   *         of this callback.
   */
  ObEvent *schedule_at_local(ObContinuation *c, const ObHRTime atimeout_at,
                             const int callback_event = EVENT_INTERVAL, void *cookie = NULL);

  /**
   * Schedules the continuation on this ObEThread to receive an event
   * after the timeout elapses.
   * Schedules the callback to the continuation 'c' after the time
   * specified in atimeout_in elapses. The event is assigned to this
   * ObEThread.
   *
   * @param c      ObContinuation to be called back after the timeout elapses.
   * @param atimeout_in
   *               Amount of time after which to callback.
   * @param callback_event
   *               ObEvent code to be passed back to the
   *               continuation's handler. See the Remarks section in the
   *               ObEventProcessor class.
   * @param cookie User-defined value or pointer to be passed back
   *               in the ObEvent's object cookie field.
   *
   * @return A reference to an ObEvent object representing the schedulling
   *         of this callback.
   */
  ObEvent *schedule_in_local(ObContinuation *c, const ObHRTime atimeout_in,
                             const int callback_event = EVENT_INTERVAL, void *cookie = NULL);

  /**
   * Schedules the continuation on this ObEThread to receive an event
   * periodically.
   * Schedules the callback to the continuation 'c' to occur every
   * time 'aperiod' elapses. It is scheduled on this ObEThread.
   *
   * @param c       ObContinuation to call back everytime 'aperiod' elapses.
   * @param aperiod Duration of the time period between callbacks.
   * @param callback_event
   *                ObEvent code to be passed back to the
   *                continuation's handler. See the Remarks section in the
   *                ObEventProcessor class.
   * @param cookie  User-defined value or pointer to be passed back
   *                in the ObEvent's object cookie field.
   *
   * @return A reference to an ObEvent object representing the schedulling
   *         of this callback.
   */
  ObEvent *schedule_every_local(ObContinuation *c, const ObHRTime aperiod,
                                const int callback_event = EVENT_INTERVAL, void *cookie = NULL);

  int schedule_local(ObEvent &e, const bool fast_signal = false);

  ObEvent *schedule_common(ObContinuation &cont, const ObHRTime atimeout_at,
                           const ObHRTime aperiod, const int callback_event,
                           void *cookie, schedule_handler_func schedule_handler,
                           const bool fast_signal = false);

  void execute();
  void free_event(ObEvent &e);

  bool is_event_thread_type(const ObEventThreadType et) { return !!(event_types_ & (1 << et)); }
  void set_event_thread_type(const ObEventThreadType et) { event_types_ |= (1 << et); }
  ObDedicateThreadType get_dedicate_type() { return dedicate_thread_type_; }
  void set_dedicate_type(const ObDedicateThreadType dedicate_thread_type) { dedicate_thread_type_ = dedicate_thread_type; }

  net::ObNetHandler &get_net_handler() { return *net_handler_; }
  net::ObNetPoll &get_net_poll() { return *net_poll_; }
  net::ObInactivityCop &get_inactivity_cop() { return *inactivity_cop_; }
  proxy::ObMysqlClientSessionMap &get_client_session_map() { return *cs_map_; }
  proxy::ObTableRefHashMap &get_table_map() { return *table_map_; }
  proxy::ObSqlTableRefHashMap &get_sql_table_map() { return *sql_table_map_; }
  proxy::ObPartitionRefHashMap &get_partition_map() { return *partition_map_; }
  proxy::ObRoutineRefHashMap &get_routine_map() { return *routine_map_; }
  proxy::ObBasePsEntryThreadCache &get_ps_entry_cache() { return *ps_entry_cache_; }
  proxy::ObBasePsEntryThreadCache &get_text_ps_entry_cache() { return *text_ps_entry_cache_; }

  obutils::ObCongestionRefHashMap &get_cgt_map() { return *congestion_map_; }
  common::ObMysqlRandom &get_random_seed() { return *random_seed_; }
  ObProxyMutex &get_mutex() { return *mutex_; }
  TO_STRING_KV(K_(id), K_(tid), K_(thread_id), K_(event_types), K_(tt), K_(stack_start),
               K_(ethreads_to_be_signalled_count), K_(cur_time), K_(use_status));

private:
  void process_event(ObEvent *e, const int calling_code);
  void dequeue_local_event(Que(ObEvent, link_) &negative_queue);

public:
  // TODO: This would be much nicer to have "run-time" configurable
  // when add new local thread, need
  static const int64_t MAX_THREAD_DATA_SIZE = 8096;//8 * 1024
  static const int64_t THREAD_MAX_HEARTBEAT_MSECONDS = 30;
  static const int64_t NO_ETHREAD_ID = -1;
  static const int64_t DELAY_FOR_RETRY = HRTIME_MSECONDS(1);

  // Block of memory to allocate thread specific data e.g. stat system arrays.
  char thread_private_[MAX_THREAD_DATA_SIZE];

  ObProtectedQueue event_queue_external_;
  ObPriorityEventQueue event_queue_;

  ObHRTime sleep_time_;

  ObEThread **ethreads_to_be_signalled_;
  int64_t ethreads_to_be_signalled_count_;

  int64_t id_;
  int64_t event_types_;
  int64_t stack_start_; // statck start pos, used to minitor stack size
  ObDedicateThreadType dedicate_thread_type_;

  int (*signal_hook_)(ObEThread &);

#if OB_HAVE_EVENTFD
  int evfd_;
#else
  int evpipe_[2];
#endif
  net::ObEventIO *ep_;

  net::ObNetHandler *net_handler_;
  net::ObNetPoll *net_poll_;
  net::ObInactivityCop *inactivity_cop_;
  proxy::ObMysqlClientSessionMap *cs_map_;
  proxy::ObTableRefHashMap *table_map_;
  proxy::ObPartitionRefHashMap *partition_map_;
  proxy::ObRoutineRefHashMap *routine_map_;
  obutils::ObCongestionRefHashMap *congestion_map_;
  proxy::ObCacheCleaner *cache_cleaner_;
  proxy::ObSqlTableRefHashMap *sql_table_map_;
  proxy::ObBasePsEntryThreadCache *ps_entry_cache_;
  proxy::ObBasePsEntryThreadCache *text_ps_entry_cache_;
  common::ObMysqlRandom *random_seed_;

  bool is_need_thread_pool_event_;
  ObProtectedQueueThreadPool *thread_pool_event_queue_;

  char *warn_log_buf_;
  char *warn_log_buf_start_;

  ObThreadType tt_;
  ObEvent *pending_event_; // For dedicated event thread
  prometheus::ObThreadPrometheus *thread_prometheus_;
  bool use_status_;

private:
  // prevent unauthorized copies (Not implemented)
  DISALLOW_COPY_AND_ASSIGN(ObEThread);
};

inline ObEvent *ObEThread::schedule_common(ObContinuation &cont, const ObHRTime atimeout_at,
    const ObHRTime aperiod, const int callback_event, void *cookie,
    schedule_handler_func schedule_handler, const bool fast_signal/*false*/)
{
  int ret = common::OB_SUCCESS;
  ObEvent *event = NULL;
  if (OB_ISNULL(event = op_reclaim_alloc(ObEvent))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    PROXY_EVENT_LOG(ERROR, "fail to alloc mem for ObEvent", K(ret));
  } else if (OB_FAIL(event->init(cont, atimeout_at, aperiod))) {
    PROXY_EVENT_LOG(WARN, "fail to init ObEvent", K(atimeout_at), K(aperiod), K(ret));
  } else {
    event->callback_event_ = callback_event;
    event->cookie_ = cookie;
    if (OB_FAIL((this->*schedule_handler)(*event, fast_signal))) {
      PROXY_EVENT_LOG(WARN, "fail to schedule ObEvent", K(event), K(ret));
    }
  }

  if (OB_FAIL(ret) && NULL != event) {
    op_reclaim_free(event);
    event = NULL;
  }
  return event;
}

inline ObEvent *ObEThread::schedule_imm(
    ObContinuation *cont, const int callback_event, void *cookie)
{
  ObEvent *event = NULL;
  if (OB_ISNULL(cont)) {
    PROXY_EVENT_LOG(WARN, "argument is invalid", K(cont));
  } else if (OB_ISNULL(event = schedule_common(*cont, 0, 0, callback_event, cookie,
      &ObEThread::schedule))) {
    PROXY_EVENT_LOG(WARN, "fail to schedule_common for schedule_imm");
  } else {/*do nothing*/}

  return event;
}

inline ObEvent *ObEThread::schedule_imm_signal(
    ObContinuation *cont, const int callback_event, void *cookie)
{
  ObEvent *event = NULL;
  if (OB_ISNULL(cont)) {
    PROXY_EVENT_LOG(WARN, "argument is invalid", K(cont));
  } else if (OB_ISNULL(event = schedule_common(*cont, 0, 0, callback_event, cookie,
      &ObEThread::schedule, true))) {
    PROXY_EVENT_LOG(WARN, "fail to schedule_common for schedule_imm_signal");
  } else {/*do nothing*/}

  return event;
}

inline ObEvent *ObEThread::schedule_at(
    ObContinuation *cont, const ObHRTime t, const int callback_event, void *cookie)
{
  ObEvent *event = NULL;
  if (OB_ISNULL(cont)) {
    PROXY_EVENT_LOG(WARN, "argument is invalid", K(cont));
  } else if (OB_ISNULL(event = schedule_common(*cont, t, 0, callback_event, cookie,
      &ObEThread::schedule))) {
    PROXY_EVENT_LOG(WARN, "fail to schedule_common for schedule_at");
  } else {/*do nothing*/}

  return event;
}

inline ObEvent *ObEThread::schedule_in(
    ObContinuation *cont, const ObHRTime t, const int callback_event, void *cookie)
{
  ObEvent *event = NULL;
  if (OB_ISNULL(cont)) {
    PROXY_EVENT_LOG(WARN, "argument is invalid", K(cont));
  } else if (OB_ISNULL(event = schedule_common(*cont, get_hrtime() + t, 0, callback_event,
      cookie, &ObEThread::schedule))) {
    PROXY_EVENT_LOG(WARN, "fail to schedule_common for schedule_in");
  } else {/*do nothing*/}

  return event;
}

inline ObEvent *ObEThread::schedule_every(
    ObContinuation *cont, const ObHRTime t, const int callback_event, void *cookie)
{
  ObEvent *event = NULL;
  const ObHRTime aperiod = (t < 0 ? t : (get_hrtime() + t));
  if (OB_ISNULL(cont)) {
    PROXY_EVENT_LOG(WARN, "argument is invalid", K(cont));
  } else if (OB_ISNULL(event = schedule_common(*cont, aperiod, t, callback_event,
      cookie, &ObEThread::schedule))) {
    PROXY_EVENT_LOG(WARN, "fail to schedule_common for schedule_every");
  } else {/*do nothing*/}

  return event;
}

inline int ObEThread::schedule(ObEvent &event, const bool fast_signal)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(REGULAR != tt_)) {
    ret = common::OB_ERR_UNEXPECTED;
    PROXY_EVENT_LOG(WARN, "only REGULAR ethread can arrive here", K(tt_), K(ret));
  } else if (NULL == event.continuation_->mutex_ && OB_ISNULL(this->mutex_)) {
    ret = common::OB_ERR_UNEXPECTED;
    PROXY_EVENT_LOG(WARN, "mutex_ is null, it should not happened", "this.mutex", this->mutex_,
                    "continuation.mutex", event.continuation_->mutex_.get_ptr(), K(ret));
  } else {
    event.ethread_ = this;
    if (NULL != event.continuation_->mutex_) {
      event.mutex_ = event.continuation_->mutex_;
    } else {
      event.mutex_ = event.ethread_->mutex_;
      event.continuation_->mutex_ = event.ethread_->mutex_;
    }
    event_queue_external_.enqueue(&event, fast_signal);
  }
  return ret;
}

inline ObEvent *ObEThread::schedule_imm_local(
    ObContinuation *cont, const int callback_event, void *cookie)
{
  ObEvent *event = NULL;
  if (OB_ISNULL(cont)) {
    PROXY_EVENT_LOG(WARN, "argument is invalid", K(cont));
  } else if (OB_ISNULL(event = schedule_common(*cont, 0, 0, callback_event, cookie,
      &ObEThread::schedule_local))) {
    PROXY_EVENT_LOG(WARN, "fail to schedule_common for schedule_imm_local");
  } else {/*do nothing*/}

  return event;
}

inline ObEvent *ObEThread::schedule_at_local(
    ObContinuation *cont, const ObHRTime t, const int callback_event, void *cookie)
{
  ObEvent *event = NULL;
  if (OB_ISNULL(cont)) {
    PROXY_EVENT_LOG(WARN, "argument is invalid", K(cont));
  } else if (OB_ISNULL(event = schedule_common(*cont, t, 0, callback_event, cookie,
      &ObEThread::schedule_local))) {
    PROXY_EVENT_LOG(WARN, "fail to schedule_common for schedule_at_local");
  } else {/*do nothing*/}

  return event;
}

inline ObEvent *ObEThread::schedule_in_local(
    ObContinuation *cont, const ObHRTime t, const int callback_event, void *cookie)
{
  ObEvent *event = NULL;
  if (OB_ISNULL(cont)) {
    PROXY_EVENT_LOG(WARN, "argument is invalid", K(cont));
  } else if (OB_ISNULL(event = schedule_common(*cont, get_hrtime() + t, 0, callback_event,
      cookie, &ObEThread::schedule_local))) {
    PROXY_EVENT_LOG(WARN, "fail to schedule_common for schedule_in_local");
  } else {/*do nothing*/}

  return event;
}

inline ObEvent *ObEThread::schedule_every_local(
    ObContinuation *cont, const ObHRTime t, const int callback_event, void *cookie)
{
  ObEvent *event = NULL;
  const ObHRTime aperiod = (t < 0 ? t : (get_hrtime() + t));
  if (OB_ISNULL(cont)) {
    PROXY_EVENT_LOG(WARN, "argument is invalid", K(cont));
  } else if (OB_ISNULL(event = schedule_common(*cont, aperiod, t, callback_event,
      cookie, &ObEThread::schedule_local))) {
    PROXY_EVENT_LOG(WARN, "fail to schedule_common for schedule_every_local");
  } else {/*do nothing*/}

  return event;
}

inline int ObEThread::schedule_local(ObEvent &event, const bool fast_signal)
{
  UNUSED(fast_signal);
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(REGULAR != tt_)) {
    ret = common::OB_ERR_UNEXPECTED;
    PROXY_EVENT_LOG(WARN, "only REGULAR ethread can arrive here", K(tt_), K(ret));
  } else {
    if (NULL == event.mutex_) {
      event.ethread_ = this;
      event.mutex_ = event.continuation_->mutex_;
      event_queue_external_.enqueue_local(&event);
    } else {
      if (event.ethread_ != this) {
        ret = common::OB_ERR_UNEXPECTED;
        PROXY_EVENT_LOG(WARN, "event.ethread_ do not band curr ethread", K(event.ethread_), K(ret));
      } else {
        event_queue_external_.enqueue_local(&event);
      }
    }
  }
  return ret;
}

inline void ObEThread::free_event(ObEvent &event)
{
  if (OB_UNLIKELY(event.in_the_priority_queue_) || OB_UNLIKELY(event.in_the_prot_queue_)) {
    PROXY_EVENT_LOG(ERROR, "current event is in queue, it should not happened", K(event));
  } else {
    event.free();
  }
}

inline ObEThread *this_ethread()
{
  return reinterpret_cast<ObEThread *>(this_thread());
}

inline ObEThread &self_ethread()
{
  return *reinterpret_cast<ObEThread *>(this_thread());
}

} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_ETHREAD_H
