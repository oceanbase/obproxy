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
 * **************************************************************
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef OBPROXY_EVENT_PROCESSOR_H
#define OBPROXY_EVENT_PROCESSOR_H

#include "iocore/eventsystem/ob_processor.h"
#include "iocore/eventsystem/ob_ethread.h"
#include "iocore/eventsystem/ob_event.h"
#include "iocore/eventsystem/ob_io_buffer.h"
#include "lib/lock/ob_drw_lock.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{

#ifdef OB_MAX_THREADS_IN_EACH_THREAD_TYPE
const int64_t MAX_THREADS_IN_EACH_TYPE = OB_MAX_THREADS_IN_EACH_THREAD_TYPE;
#else
const int64_t MAX_THREADS_IN_EACH_TYPE = 256;
#endif

#ifdef OB_MAX_NUMBER_EVENT_THREADS
const int64_t MAX_EVENT_THREADS        = OB_MAX_NUMBER_EVENT_THREADS;
#else
const int64_t MAX_EVENT_THREADS        = 512;
#endif

const int64_t MAX_OTHER_GROUP_NET_THREADS = 8;

#ifndef offsetof
#define offsetof(TYPE, MEMBER) ((size_t) &((TYPE *)0)->MEMBER)
#endif

// OB_ALIGN() is only to be used to align on a power of 2 boundary
#define OB_ALIGN(size, boundary) \
  (((size) + ((boundary) - 1)) & ~((boundary) - 1))

class ObEThread;

/**
 * Main processor for the ObEvent System. The ObEventProcessor is the core
 * component of the ObEvent System. Once started, it is responsible for
 * creating and managing groups of threads that execute user-defined
 * tasks asynchronously at a given time or periodically.
 * The ObEventProcessor provides a set of scheduling functions through
 * which you can specify continuations to be called back by one of its
 * threads. These function calls do not block. Instead they return an
 * ObEvent object and schedule the callback to the continuation passed in at
 * a later or specific time, as soon as possible or at certain intervals.
 * Singleton model:
 * Every executable that imports and statically links against the
 * EventSystem library is provided with a global instance of the
 * ObEventProcessor called eventProcessor. Therefore, it is not necessary to
 * create instances of the ObEventProcessor class because it was designed
 * as a singleton. It is important to note that none of its functions
 * are reentrant.
 * ObThread Groups (ObEvent types):
 * When the ObEventProcessor is started, the first group of threads is
 * spawned and it is assigned the special id ET_CALL. Depending on the
 * complexity of the state machine or protocol, you may be interested
 * in creating additional threads and the ObEventProcessor gives you the
 * ability to create a single thread or an entire group of threads. In
 * the former case, you call spawn_thread and the thread is independent
 * of the thread groups and it exists as long as your continuation handle
 * executes and there are events to process. In the latter, you call
 * spawn_event_theads which creates a new thread group and you get an id
 * or event type with wich you must keep for use later on when scheduling
 * continuations on that group.
 * Callback event codes:
 *
 * @b UNIX: For all of the scheduling functions, the callback_event
 * parameter is not used. On a callback, the event code passed in to
 * the continuation handler is always EVENT_IMMEDIATE.
 * @b NT: The value of the event code passed in to the continuation
 * handler is the value provided in the callback_event parameter.
 *
 * ObEvent allocation policy:
 *
 * Events are allocated and deallocated by the ObEventProcessor. A state
 * machine may access the returned, non-recurring event until it is
 * cancelled or the callback from the event is complete. For recurring
 * events, the ObEvent may be accessed until it is cancelled. Once the event
 * is complete or cancelled, it's the eventProcessor's responsibility to
 * deallocate it.
 */
class ObEventProcessor : public ObProcessor
{
public:
  ObEventProcessor();
  virtual ~ObEventProcessor() { }
  static int64_t get_cpu_count();

  /**
   * Spawn an additional thread for calling back the continuation. Spawns
   * a dedicated thread (ObEThread) that calls back the continuation passed
   * in as soon as possible.
   *
   * @param cont      continuation that the spawn thread will call back
   *                  immediately.
   * @param thr_name
   * @param stacksize
   *
   * @return event object representing the start of the thread.
   */
  ObEvent *spawn_thread(ObContinuation *cont, const char *thr_name,
                        const int64_t stacksize = 0,
                        ObDedicateThreadType dedicate_thread_type = DEDICATE_THREAD_NONE);

  /**
   * Spawns a group of threads for an event type. Spawns the number of
   * event threads passed in (thread_count) creating a thread group and
   * returns the thread group id (or ObEventThreadType). See the remarks section
   * for ObThread Groups.
   *
   * @param thread_count
   * @param et_name
   * @param stacksize
   * @param etype [out] return thread id for the new group of threads
   *
   * @return OB_SUCCESS or OB_ERROR.
   */
  int spawn_event_threads(const int64_t thread_count, const char *et_name,
                          const int64_t stacksize, ObEventThreadType &etype);
  int spawn_net_threads(const int64_t thread_count, const char *et_name,
                           const int64_t stacksize);

  /**
   * Schedules the continuation on a specific ObEThread to receive an event
   * at the given timeout.  Requests the ObEventProcessor to schedule
   * the callback to the continuation 'c' at the time specified in
   * 'atimeout_at'. The event is assigned to the specified ObEThread.
   *
   * @param c          ObContinuation to be called back at the time specified in
   *                   'atimeout_at'.
   * @param event_type
   * @param callback_event
   *                   code to be passed back to the continuation's
   *                   handler. See the Remarks section.
   * @param cookie     user-defined value or pointer to be passed back in
   *                   the ObEvent's object cookie field.
   *
   * @return reference to an ObEvent object representing the scheduling
   *         of this callback.
   */
  ObEvent *schedule_imm(ObContinuation *c,
                        const ObEventThreadType event_type = ET_CALL,
                        const int callback_event = EVENT_IMMEDIATE,
                        void *cookie = NULL);

  // provides the same functionality as schedule_imm and also signals the
  // thread immediately
  ObEvent *schedule_imm_signal(ObContinuation *cont,
                               const ObEventThreadType event_type = ET_CALL,
                               const int callback_event = EVENT_IMMEDIATE,
                               void *cookie = NULL);
  /**
   * Schedules the continuation on a specific thread group to receive an
   * event at the given timeout. Requests the ObEventProcessor to schedule
   * the callback to the continuation 'c' at the time specified in
   * 'atimeout_at'. The callback is handled by a thread in the specified
   * thread group (event_type).
   *
   * @param c          ObContinuation to be called back at the time specified in
   *                   'atimeout_at'.
   * @param atimeout_at
   *                   Time value at which to callback.
   * @param event_type thread group id (or event type) specifying the
   *                   group of threads on which to schedule the callback.
   * @param callback_event
   *                   code to be passed back to the continuation's
   *                   handler. See the Remarks section.
   * @param cookie     user-defined value or pointer to be passed back in
   *                   the ObEvent's object cookie field.
   *
   * @return reference to an ObEvent object representing the scheduling of
   *         this callback.
   */
  ObEvent *schedule_at(ObContinuation *cont,
                       const ObHRTime atimeout_at,
                       const ObEventThreadType event_type = ET_CALL,
                       const int callback_event = EVENT_INTERVAL,
                       void *cookie = NULL);

  /**
   * Schedules the continuation on a specific thread group to receive an
   * event after the specified timeout elapses. Requests the ObEventProcessor
   * to schedule the callback to the continuation 'c' after the time
   * specified in 'atimeout_in' elapses. The callback is handled by a
   * thread in the specified thread group (event_type).
   *
   * @param c          ObContinuation to call back after the timeout elapses.
   * @param atimeout_in
   *                   amount of time after which to callback.
   * @param event_type ObThread group id (or event type) specifying the
   *                   group of threads on which to schedule the callback.
   * @param callback_event
   *                   code to be passed back to the continuation's
   *                   handler. See the Remarks section.
   * @param cookie     user-defined value or pointer to be passed back in
   *                   the ObEvent's object cookie field.
   *
   * @return reference to an ObEvent object representing the scheduling of
   *         this callback.
   */
  ObEvent *schedule_in(ObContinuation *cont,
                       const ObHRTime atimeout_in,
                       const ObEventThreadType event_type = ET_CALL,
                       const int callback_event = EVENT_INTERVAL,
                       void *cookie = NULL);

  /**
   * Schedules the continuation on a specific thread group to receive
   * an event periodically. Requests the ObEventProcessor to schedule the
   * callback to the continuation 'c' everytime 'aperiod' elapses. The
   * callback is handled by a thread in the specified thread group
   * (event_type).
   *
   * @param c          ObContinuation to call back everytime 'aperiod' elapses.
   * @param aperiod    duration of the time period between callbacks.
   * @param event_type thread group id (or event type) specifying the
   *                   group of threads on which to schedule the callback.
   * @param callback_event
   *                   code to be passed back to the continuation's
   *                   handler. See the Remarks section.
   * @param cookie     user-defined value or pointer to be passed back in
   *                   the ObEvent's object cookie field.
   *
   * @return reference to an ObEvent object representing the scheduling of
   *         this callback.
   */
  ObEvent *schedule_every(ObContinuation *cont,
                          const ObHRTime aperiod,
                          const ObEventThreadType event_type = ET_CALL,
                          const int callback_event = EVENT_INTERVAL,
                          void *cookie = NULL);

  ObEvent *prepare_schedule_imm(ObContinuation *cont,
                                const ObEventThreadType event_type = ET_CALL,
                                const int callback_event = EVENT_IMMEDIATE,
                                void *cookie = NULL);

  void do_schedule(ObEvent *e, const bool fast_signal);

  /**
   * Initializes the ObEventProcessor and its associated threads. Spawns the
   * specified number of threads, initializes their state information and
   * sets them running. It creates the initial thread group, represented
   * by the event type ET_CALL.
   *
   * @param net_thread_count
   * @param stacksize
   *
   * @return 0 if successful, and a negative value otherwise.
   */
  virtual int start(const int64_t net_thread_count, const int64_t stacksize = DEFAULT_STACKSIZE,
                    const bool enable_cpu_topology = false, const bool automatic_match_work_thread = true,
                    const bool enable_cpu_isolate = false);

  /**
   * Stop the ObEventProcessor. Attempts to stop the ObEventProcessor and
   * all of the threads in each of the thread groups.
   */
  virtual void shutdown() { }

  /**
   * Allocates size bytes on the event threads. This function is thread
   * safe.
   *
   * @param size   bytes to be allocated.
   *
   * @return
   */
  int64_t allocate(const int64_t size);

  ObEvent *schedule(ObEvent *e, const ObEventThreadType etype, const bool fast_signal = false);

  ObEThread *assign_thread(const ObEventThreadType etype);

private:
  /**
   * Common function. Check the input parameter of schedule_x, such as schedule_imm,
   * schedule_imm_signal, schedule_at, schedule_in, schedule_every, prepare_schedule_imm.
   *
   * @param c          ObContinuation to call back everytime 'aperiod' elapses.
   * @param event_type thread group id (or event type) specifying the
   *                   group of threads on which to schedule the callback.
   *
   * @return return the result of schedule_x input parameter OB_SUCCESS
   *         or OB_INVALID_ARGUMENT.
   */
  int check_schedule_input(ObContinuation *cont, const ObEventThreadType event_type);

  int init_one_event_thread(const int64_t index);

public:
  /**
   * An array of pointers to all of the ObEThreads handled by the
   * ObEventProcessor. An array of pointers to all of the ObEThreads    created
   * throughout the existence of the ObEventProcessor instance.
   */
  ObEThread *all_event_threads_[MAX_EVENT_THREADS];

  /**
   * An array of pointers, organized by thread group, to all of the
   * ObEThreads handled by the ObEventProcessor. An array of pointers to all of
   * the ObEThreads created throughout the existence of the ObEventProcessor
   * instance. It is a two-dimensional array whose first dimension is the
   * thread group id and the second the ObEThread pointers for that group.
   */
  ObEThread *event_thread_[MAX_EVENT_TYPES][MAX_THREADS_IN_EACH_TYPE];

  uint32_t next_thread_for_type_[MAX_EVENT_TYPES];
  int64_t thread_count_for_type_[MAX_EVENT_TYPES];

  /**
   * Total number of threads controlled by this ObEventProcessor.  This is
   * the count of all the ObEThreads spawn by this ObEventProcessor, excluding
   * those created by spawn_thread
   */
  int64_t event_thread_count_;

  /**
   * Total number of thread groups created so far. This is the count of
   * all the thread groups (event types) created for this ObEventProcessor.
   */
  int64_t thread_group_count_;

  ObEThread *all_dedicate_threads_[MAX_EVENT_THREADS];
  int64_t dedicate_thread_count_;               // No. of dedicated threads
  volatile int64_t thread_data_used_;
  common::DRWLock lock_;

private:
  bool started_;
  DISALLOW_COPY_AND_ASSIGN(ObEventProcessor);
};

inline ObEventProcessor::ObEventProcessor()
    : event_thread_count_(0),
      thread_group_count_(0),
      dedicate_thread_count_(0),
      thread_data_used_(0),
      lock_(),
      started_(false)
{
  memset(all_event_threads_, 0, sizeof(all_event_threads_));
  memset(all_dedicate_threads_, 0, sizeof(all_dedicate_threads_));
  memset(event_thread_, 0, sizeof(event_thread_));
  memset(thread_count_for_type_, 0, sizeof(thread_count_for_type_));
  memset(next_thread_for_type_, 0, sizeof(next_thread_for_type_));
}

inline int64_t ObEventProcessor::allocate(const int64_t size)
{
  int64_t ret = -1;
  static int64_t start = OB_ALIGN(offsetof(ObEThread, thread_private_), 16);
  static int64_t loss = start - offsetof(ObEThread, thread_private_);

  if (OB_UNLIKELY(size < 0)) {
    PROXY_EVENT_LOG(ERROR, "invalid parameters", K(size));
  } else {
    int64_t alloc_size = OB_ALIGN(size, 16);       // 16 byte alignment
    int64_t old = 0;
    do {
      old = thread_data_used_;
      if ((old + loss + alloc_size) > ObEThread::MAX_THREAD_DATA_SIZE) {
        break;
      } else {
        ret = old + start;
      }
    } while (!ATOMIC_BCAS(&thread_data_used_, old, old + alloc_size));
  }
  PROXY_EVENT_LOG(INFO, "allocate memory from thread local data buffer", K(size),
                        "cur_offset", thread_data_used_, LITERAL_K(ObEThread::MAX_THREAD_DATA_SIZE));
  return ret;
}

inline ObEThread *ObEventProcessor::assign_thread(const ObEventThreadType etype)
{
  int64_t next = 0;
  if (OB_LIKELY(thread_count_for_type_[etype] > 1)) {
    next = static_cast<int64_t>(next_thread_for_type_[etype]++) % thread_count_for_type_[etype];
  } else {
    next = 0;
  }
  return (event_thread_[etype][next]);
}

inline ObEvent *ObEventProcessor::schedule(
    ObEvent *event, const ObEventThreadType etype, const bool fast_signal)
{
  event->ethread_ = assign_thread(etype);
  if (NULL != event->continuation_->mutex_) {
    event->mutex_ = event->continuation_->mutex_;
  } else {
    event->continuation_->mutex_ = event->ethread_->mutex_;
    event->mutex_ = event->continuation_->mutex_;
  }
  event->ethread_->event_queue_external_.enqueue(event, fast_signal);
  return event;
}

inline int ObEventProcessor::check_schedule_input(ObContinuation *cont,
                                                  const ObEventThreadType event_type)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(cont)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_EVENT_LOG(ERROR, "invalid parameters, ObContinuation is NULL", K(ret));
  } else if (OB_UNLIKELY(event_type >= MAX_EVENT_TYPES) || OB_UNLIKELY(event_type < ET_CALL)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_EVENT_LOG(ERROR, "invalid parameters", K(event_type), K(ret));
  } else if (OB_UNLIKELY(0 == thread_count_for_type_[event_type])) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_EVENT_LOG(ERROR, "threads count for this type is zero", K(event_type), K(ret));
  } else {
    //do nothing
  }
  return ret;
}

inline ObEvent *ObEventProcessor::schedule_imm_signal(
    ObContinuation *cont, const ObEventThreadType etype,
    const int callback_event, void *cookie)
{
  ObEvent *event = NULL;
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(check_schedule_input(cont, etype))) {
    PROXY_EVENT_LOG(ERROR, "fail to check_schedule_input", K(ret));
  } else if (OB_ISNULL(event = op_reclaim_alloc(ObEvent))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    PROXY_EVENT_LOG(ERROR, "fail to alloc mem for schedule_imm_signal", K(ret));
  } else if (OB_FAIL(event->init(*cont, 0, 0))) {
    PROXY_EVENT_LOG(WARN, "fail init ObEvent", K(ret));
  } else {
#ifdef ENABLE_TIME_TRACE
    event->start_time_ = get_hrtime();
#endif
    event->callback_event_ = callback_event;
    event->cookie_ = cookie;
    event = schedule(event, etype, true);
  }

  if (OB_FAIL(ret) && NULL != event) {
    op_reclaim_free(event);
    event = NULL;
  }
  return event;
}

inline ObEvent *ObEventProcessor::schedule_imm(
    ObContinuation *cont, const ObEventThreadType etype,
    const int callback_event, void *cookie)
{
  ObEvent *event = NULL;
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(check_schedule_input(cont, etype))) {
    PROXY_EVENT_LOG(ERROR, "fail to check_schedule_input", K(ret));
  } else if (OB_ISNULL(event = op_reclaim_alloc(ObEvent))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    PROXY_EVENT_LOG(ERROR, "fail to alloc mem for schedule_imm", K(ret));
  } else if (OB_FAIL(event->init(*cont, 0, 0))) {
    PROXY_EVENT_LOG(WARN, "fail init ObEvent", K(ret));
  } else {
#ifdef ENABLE_TIME_TRACE
    event->start_time_ = get_hrtime();
#endif
    event->callback_event_ = callback_event;
    event->cookie_ = cookie;
    event = schedule(event, etype);
  }

  if (OB_FAIL(ret) && NULL != event) {
    op_reclaim_free(event);
    event = NULL;
  }
  return event;
}

inline ObEvent *ObEventProcessor::schedule_at(
    ObContinuation *cont, const ObHRTime t, const ObEventThreadType etype,
    const int callback_event, void *cookie)
{
  ObEvent *event = NULL;
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(check_schedule_input(cont, etype))) {
    PROXY_EVENT_LOG(ERROR, "fail to check_schedule_input", K(ret));
  } else if (OB_UNLIKELY(t <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_EVENT_LOG(ERROR, "invalid parameters", K(t), K(ret));
  } else if (OB_ISNULL(event = op_reclaim_alloc(ObEvent))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    PROXY_EVENT_LOG(ERROR, "fail to alloc mem for schedule_at", K(ret));
  } else if (OB_FAIL(event->init(*cont, t, 0))) {
    PROXY_EVENT_LOG(WARN, "fail init ObEvent", K(ret));
  } else {
    event->callback_event_ = callback_event;
    event->cookie_ = cookie;
    event = schedule(event, etype);
  }

  if (OB_FAIL(ret) && NULL != event) {
    op_reclaim_free(event);
    event = NULL;
  }
  return event;
}

inline ObEvent *ObEventProcessor::schedule_in(
    ObContinuation *cont, const ObHRTime t, const ObEventThreadType etype,
    const int callback_event, void *cookie)
{
  ObEvent *event = NULL;
  int ret = common::OB_SUCCESS;

  if (OB_FAIL(check_schedule_input(cont, etype))) {
    PROXY_EVENT_LOG(ERROR, "fail to check_schedule_input", K(ret));
    // check_schedule_input had been printed
  } else if (OB_ISNULL(event = op_reclaim_alloc(ObEvent))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    PROXY_EVENT_LOG(ERROR, "fail to alloc mem for schedule_in", K(ret));
  } else if (OB_FAIL(event->init(*cont, get_hrtime() + t, 0))) {
    PROXY_EVENT_LOG(WARN, "fail init ObEvent", K(ret));
  } else {
    event->callback_event_ = callback_event;
    event->cookie_ = cookie;
    event = schedule(event, etype);
  }

  if (OB_FAIL(ret) && NULL != event) {
    op_reclaim_free(event);
    event = NULL;
  }
  return event;
}

inline ObEvent *ObEventProcessor::schedule_every(
    ObContinuation *cont, const ObHRTime t, const ObEventThreadType etype,
    const int callback_event, void *cookie)
{
  ObEvent *event = NULL;
  int ret = common::OB_SUCCESS;

  if (OB_FAIL(check_schedule_input(cont, etype))) {
    PROXY_EVENT_LOG(ERROR, "fail to check_schedule_input", K(ret));
  } else if (OB_UNLIKELY(0 == t)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_EVENT_LOG(ERROR, "invalid parameters", K(t), K(ret));
  } else if (OB_ISNULL(event = op_reclaim_alloc(ObEvent))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    PROXY_EVENT_LOG(ERROR, "fail to alloc mem for schedule_every", K(ret));
  } else {
    if (t < 0) {
      if (OB_FAIL(event->init(*cont, t, t))) {
        PROXY_EVENT_LOG(WARN, "fail init ObEvent", K(ret));
      }
    } else {
      if (OB_FAIL(event->init(*cont, get_hrtime() + t, t))) {
        PROXY_EVENT_LOG(WARN, "fail init ObEvent", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    event->callback_event_ = callback_event;
    event->cookie_ = cookie;
    event = schedule(event, etype);
  } else {
    if (NULL != event) {
      op_reclaim_free(event);
      event = NULL;
    }
  }
  return event;
}

inline ObEvent *ObEventProcessor::prepare_schedule_imm(
    ObContinuation *cont, const ObEventThreadType etype,
    const int callback_event, void *cookie)
{
  ObEvent *event = NULL;
  int ret = common::OB_SUCCESS;

  if (OB_FAIL(check_schedule_input(cont, etype))) {
    PROXY_EVENT_LOG(ERROR, "fail to check_schedule_input", K(ret));
  } else if (OB_ISNULL(event = op_reclaim_alloc(ObEvent))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    PROXY_EVENT_LOG(ERROR, "fail to alloc mem for prepare_schedule_imm", K(ret));
  } else if (OB_FAIL(event->init(*cont, 0, 0))) {
    PROXY_EVENT_LOG(WARN, "fail init ObEvent", K(ret));
  } else {
#ifdef ENABLE_TIME_TRACE
    event->start_time_ = get_hrtime();
#endif
    event->callback_event_ = callback_event;
    event->cookie_ = cookie;
    event->ethread_ = assign_thread(etype);
    if (NULL != event->continuation_->mutex_) {
      event->mutex_ = event->continuation_->mutex_;
    } else {
      event->continuation_->mutex_ = event->ethread_->mutex_;
      event->mutex_ = event->continuation_->mutex_;
    }
  }

  if (OB_FAIL(ret) && NULL != event) {
    op_reclaim_free(event);
    event = NULL;
  }
  return event;
}

inline void ObEventProcessor::do_schedule(ObEvent *event, const bool fast_signal)
{
  if (OB_ISNULL(event)) {
    PROXY_EVENT_LOG(ERROR, "invalid parameters, ObEvent is NULL");
  } else {
    event->ethread_->event_queue_external_.enqueue(event, fast_signal);
  }
}

extern class ObEventProcessor g_event_processor;

} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_EVENT_PROCESSOR_H
