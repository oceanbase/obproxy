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

#ifndef OBPROXY_EVENT_H
#define OBPROXY_EVENT_H

#include "iocore/eventsystem/ob_action.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{

#define MAX_EVENTS_PER_THREAD     100000

// Events
#define EVENT_NONE                CONTINUATION_NONE       // 0
#define EVENT_IMMEDIATE           1
#define EVENT_INTERVAL            2
#define EVENT_ERROR               3
#define EVENT_CALL                4     // used internally in state machines
#define EVENT_POLL                5     // negative event; activated on poll or epoll

// ObEvent callback return functions
#define EVENT_DONE                CONTINUATION_DONE     // 0
#define EVENT_CONT                CONTINUATION_CONT     // 1
#define EVENT_RETURN              5
#define EVENT_RESTART             6
#define EVENT_RESTART_DELAYED     7

// ObEvent numbers block allocation
// ALL NEW EVENT TYPES SHOULD BE ALLOCATED FROM BLOCKS LISTED HERE!
#define VC_EVENT_EVENTS_START                     100
#define NET_EVENT_EVENTS_START                    200
#define TRANSFORM_EVENTS_START                    300
#define MYSQL_TUNNEL_EVENTS_START                 400
#define EVENT_ASYNC_PROCESS_START                 500
#define INTERNAL_CMD_EVENTS_START                 600
#define CONGESTION_EVENT_EVENTS_START             700
#define LOCATION_EVENT_EVENTS_START               800
#define CLIENT_EVENT_EVENTS_START                 900
#define ROUTE_EVENT_EVENTS_START                  1000
#define TABLE_ENTRY_EVENT_EVENTS_START            1100
#define MYSQL_RPOXY_EVENT_EVENTS_START            1200
#define CLUSTER_RESOURCE_EVENT_EVENTS_START       1300
#define SERVER_STATE_EVENT_EVENTS_START           1400
#define METADB_CREATE_EVENT_EVENTS_START          1500
#define CLIENT_SESSION_EVENT_EVENTS_START         1600
#define CACHE_CLEANER_EVENT_EVENTS_START          1700
#define PARTITION_ENTRY_EVENT_EVENTS_START        1800
#define ROUTINE_ENTRY_EVENT_EVENTS_START          1900
#define SHARD_EVENT_EVENTS_START                  2000
#define SQUENCE_ENTRY_EVENT_EVENTS_START          2100
#define REFRESH_SERVER_ADDR_EVENT_START           2200
#define CONN_ENTRY_EVENT_EVENTS_START             2300
#define CONN_NUM_CHECK_EVENT_EVENTS_START         2400
#define API_EVENT_EVENTS_START                    60000

typedef int32_t ObEventThreadType;
const int32_t ET_CALL         = 0;
const int32_t MAX_EVENT_TYPES = 6; // conservative, these are dynamically allocated

class ObEThread;

/**
 * A type of ObAction returned by the ObEventProcessor. The ObEvent class
 * is the type of ObAction returned by the ObEventProcessor as a result
 * of scheduling an operation. Unlike asynchronous operations
 * represented by actions, events never call reentrantly.
 *
 * Besides being able to cancel an event (because it is an action),
 * you can also reschedule it once received.
 *
 * Remarks
 *
 * When reschedulling an event through any of the ObEvent class
 * schedulling fuctions, state machines must not make these calls
 * in other thread other than the one that called them back. They
 * also must have acquired the continuation's lock before calling
 * any of the schedulling functions.
 *
 * The rules for cancelling an event are the same as those for
 * actions:
 *
 * The canceller of an event must be the state machine that will be
 * called back by the task and that state machine's lock must be
 * held while calling cancel. Any reference to that event object
 * (ie. pointer) held by the state machine must not be used after
 * the cancellation.
 *
 * ObEvent Codes:
 *
 * At the completion of an event, state machines use the event code
 * passed in through the ObContinuation's handler function to distinguish
 * the type of event and handle the data parameter accordingly. State
 * machine implementers should be careful when defining the event
 * codes since they can impact on other state machines presents. For
 * this reason, this numbers are usually allocated from a common
 * pool.
 *
 * Time values:
 *
 * The schedulling functions use a time parameter typed as HRTime
 * for specifying the timeouts or periods. This is a nanosecond value
 * supported by libts and you should use the time functions and
 * macros defined in ob_hrime.h.
 *
 * The difference between the timeout specified for schedule_at and
 * schedule_in is that in the former it is an absolute value of time
 * that is expected to be in the future where in the latter it is
 * an amount of time to add to the current time (obtained with
 * get_hrtime).
 */
class ObEvent : public ObAction
{
public:
  ObEvent();
  virtual ~ObEvent() { }

  int init(ObContinuation &cont, const ObHRTime atimeout_at = 0, const ObHRTime aperiod = 0);

  /**
   * Reschedules this event immediately. Instructs the event object
   * to reschedule itself as soon as possible in the ObEventProcessor.
   *
   * @param callback_event
   *               ObEvent code to return at the completion
   *               of this event. See the Remarks section.
   */
  int schedule_imm(const int32_t callback_event = EVENT_IMMEDIATE);

  /**
   * Reschedules this event to callback at time 'atimeout_at'.
   * Instructs the event object to reschedule itself at the time
   * specified in atimeout_at on the ObEventProcessor.
   *
   * @param atimeout_at
   *               Time at which to callcallback. See the Remarks section.
   * @param callback_event
   *               ObEvent code to return at the completion of this event.
   *               See the Remarks section.
   */
  int schedule_at(const ObHRTime atimeout_at, const int32_t callback_event = EVENT_INTERVAL);

  /**
   * Reschedules this event to callback at time 'atimeout_at'.
   * Instructs the event object to reschedule itself at the time
   * specified in atimeout_at on the ObEventProcessor.
   *
   * @param atimeout_in
   *               Time at which to callcallback. See the Remarks section.
   * @param callback_event
   *               ObEvent code to return at the completion of this event.
   *               See the Remarks section.
   */
  int schedule_in(const ObHRTime atimeout_in, const int32_t callback_event = EVENT_INTERVAL);

  /**
   * Reschedules this event to callback every 'aperiod'. Instructs
   * the event object to reschedule itself to callback every 'aperiod'
   * from now.
   *
   * @param aperiod Time period at which to callcallback. See the Remarks section.
   * @param callback_event
   *                ObEvent code to return at the completion of this event.
   *                See the Remarks section.
   */
  int schedule_every(const ObHRTime aperiod, const int32_t callback_event = EVENT_INTERVAL);

  int check_schedule_common();

  void free();
  int64_t to_string(char *buf, const int64_t buf_len) const;

  ObEThread &get_ethread() { return *ethread_; }

public:
  uint32_t is_inited_:1;
  uint32_t in_the_prot_queue_:1;
  uint32_t in_the_priority_queue_:1;
  uint32_t in_heap_:4;
  uint32_t is_thread_pool_event_;
  int32_t callback_event_;

  ObHRTime timeout_at_;
  ObHRTime period_;

  ObEThread *ethread_;
  /**
   * This field can be set when an event is created. It is returned
   * as part of the ObEvent structure to the continuation when handleEvent
   * is called.
   */
  void *cookie_;

#ifdef ENABLE_TIME_TRACE
  ObHRTime start_time_;
#endif
  LINK(ObEvent, link_);

private:
  DISALLOW_COPY_AND_ASSIGN(ObEvent);
};

inline ObEvent::ObEvent()
    : is_inited_(false),
      in_the_prot_queue_(false),
      in_the_priority_queue_(false),
      in_heap_(0),
      is_thread_pool_event_(false),
      callback_event_(EVENT_NONE),
      timeout_at_(0),
      period_(0),
      ethread_(NULL),
      cookie_(NULL)
{
#ifdef ENABLE_TIME_TRACE
  start_time_ = 0;
#endif
}

inline int ObEvent::init(ObContinuation &cont, const ObHRTime atimeout_at, const ObHRTime aperiod)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = common::OB_INIT_TWICE;
    PROXY_EVENT_LOG(WARN, "it has been inited, it should not happened", K(is_inited_), K(*this), K(ret));
  } else {
    continuation_ = &cont;
    timeout_at_ = atimeout_at;
    period_ = aperiod;
    cancelled_ = false;
    is_inited_ = true;
  }
  return ret;
}

inline void ObEvent::free()
{
  mutex_.release();
  op_reclaim_free(this);
}

} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_EVENT_H
