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
 * ***************************************************************
 *
 * Continuations have a handle_event() method to invoke them. Userscan
 * determine the behavior of a ObContinuation by suppling a
 * "ContinuationHandler" (member function name) which is invoked when
 * events arrive. This function can be changed with the "setHandler"
 * method.
 *
 * Continuations can be subclassed to add additional state and
 * methods.
 */

#ifndef OBPROXY_CONTINUATION_H
#define OBPROXY_CONTINUATION_H

#include "iocore/eventsystem/ob_lock.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{

class ObContinuation;
class ContinuationQueue;
class ObProcessor;
class ObProxyMutex;
class ObEThread;

#define CONTINUATION_NONE            0
#define CONTINUATION_DONE            0
#define CONTINUATION_CONT            1

typedef int (ObContinuation::*ContinuationHandler)(int event, void *data);

/**
 * Base class for all state machines to receive notification of
 * events.
 *
 * The ObContinuation class represents the main abstraction mechanism
 * used throughout the IO Core ObEvent System to communicate its users
 * the occurrence of an event. A ObContinuation is a lightweight data
 * structure that implements a single method with which the user is
 * called back.
 *
 * Continuations are typically subclassed in order to implement
 * event-driven state machines. By including additional state and
 * methods, continuations can combine state with control flow, and
 * they are generally used to support split-phase, event-driven
 * control flow.
 *
 * Given the multithreaded nature of the ObEvent System, every
 * continuation carries a reference to a ObProxyMutex object to protect
 * its state and ensure atomic operations. This ObProxyMutex object
 * must be allocated by continuation-derived classes or by clients
 * of the IO Core ObEvent System and it is required as a parameter to
 * the ObContinuation's class constructor.
 */
class ObContinuation : private common::ObForceVFPTToTop
{
public:
  /**
   * Constructor of the ObContinuation object. It should not be used
   * directly. Instead create an object of a derived type.
   *
   * @param mutex  Lock to be set for this ObContinuation.
   */
  explicit ObContinuation(ObProxyMutex *mutex = NULL);

  virtual ~ObContinuation() { mutex_.release(); }

  /**
   * Receives the event code and data for an ObEvent.<p>
   * This function receives the event code and data for an event and
   * forwards them to the current continuation handler. The processor
   * calling back the continuation is responsible for acquiring its
   * lock.
   *
   * @param event  ObEvent code to be passed at callback (ObProcessor specific).
   * @param data   General purpose data related to the event code (ObProcessor specific).
   *
   * @return State machine and processor specific return code.
   */
  int handle_event(int32_t event = CONTINUATION_NONE, void *data = 0)
  {
    return (this->*handler_)(event, data);
  }

public:
  /**
   * The current continuation handler function.
   *
   * The current handler should not be set directly. In order to
   * change it, first acquire the ObContinuation's lock and then use
   * the SET_HANDLER macro which takes care of the type casting
   * issues.
   */
  ContinuationHandler handler_;

#ifdef OB_HAS_EVENT_DEBUG
  const char *handler_name_;
#endif

  /**
   * The Continuation's lock.
   *
   * A reference counted pointer to the ObContinuation's lock. This
   * lock is initialized in the constructor and should not be set
   * directly.
   */
  common::ObPtr<ObProxyMutex> mutex_;

  /**
    Link to other continuations.

    A doubly-linked element to allow Lists of Continuations to be
    assembled.
  */
  LINK(ObContinuation, link_);
};

inline ObContinuation::ObContinuation(ObProxyMutex *mutex)
    : handler_(NULL),
#ifdef OB_HAS_EVENT_DEBUG
      handler_name_(NULL),
#endif
      mutex_(mutex)
{
}

} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase

/**
 * Sets the ObContinuation's handler. The preferred mechanism for
 * setting the ObContinuation's handler.
 *
 * @param h Pointer to the function used to callback with events.
 */
#ifdef OB_HAS_EVENT_DEBUG
#define SET_HANDLER(h) \
  (handler_ = (reinterpret_cast<oceanbase::obproxy::event::ContinuationHandler>(h)),handler_name_ = #h)
#else
#define SET_HANDLER(h) \
  (handler_ = (reinterpret_cast<oceanbase::obproxy::event::ContinuationHandler>(h)))
#endif

/**
 * Sets a ObContinuation's handler.
 * The preferred mechanism for setting the ObContinuation's handler.
 *
 * @param c Pointer to a ObContinuation whose handler is being set.
 * @param h Pointer to the function used to callback with events.
 */
#ifdef OB_HAS_EVENT_DEBUG
#define SET_CONTINUATION_HANDLER(c,h) \
  (c->handler_ = (reinterpret_cast<oceanbase::obproxy::event::ContinuationHandler>(h)),c->handler_name_ = #h)
#else

#define SET_CONTINUATION_HANDLER(c,h) \
  (c->handler_ = (reinterpret_cast<oceanbase::obproxy::event::ContinuationHandler>(h)))
#endif

#endif // OBPROXY_CONTINUATION_H
