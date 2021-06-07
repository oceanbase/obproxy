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

#ifndef OBPROXY_ACTION_H
#define OBPROXY_ACTION_H

#include "iocore/eventsystem/ob_continuation.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{

/**
 * Represents an operation initiated on a ObProcessor.
 *
 * The ObAction class is an abstract representation of an operation
 * being executed by some ObProcessor. A reference to an ObAction object
 * allows you to cancel an ongoing asynchronous operation before it
 * completes. This means that the ObContinuation specified for the
 * operation will not be called back.
 *
 * Actions or classes derived from ObAction are the typical return
 * type of methods exposed by Processors in the ObEvent System and
 * throughout the IO Core libraries.
 *
 * The canceller of an action must be the state machine that will
 * be called back by the task and that state machine's lock must be
 * held while calling cancel.
 *
 * ObProcessor implementers:
 *
 * You must ensure that no events are sent to the state machine after
 * the operation has been cancelled appropriately.
 *
 * Returning an ObAction:
 *
 * ObProcessor functions that are asynchronous must return actions to
 * allow the calling state machine to cancel the task before completion.
 * Because some processor functions are reentrant, they can call
 * back the state machine before the returning from the call that
 * creates the actions. To handle this case, special values are
 * returned in place of an action to indicate to the state machine
 * that the action is already completed.
 *
 *   - @b (ret = OB_SUCCESS && NULL == action) The processor has completed
 *     the task and called the state machine back inline.
 *   - @b (ret = OB_SUCCESS && NULL != action) asynchronous task.
 *   - @b (ret != OB_SUCCESS) failed.
 *
 * To make matters more complicated, it's possible if the result is
 * (ret = OB_SUCCESS && NULL == action)that state machine deallocated itself on the
 * reentrant callback. Thus, state machine implementers MUST either
 * use a scheme to never deallocate their machines on reentrant
 * callbacks OR immediately check the returned action when creating
 * an asynchronous task and if it is (ret = OB_SUCCESS && NULL == action) neither read
 * nor write any state variables. With either method, it's imperative
 * that the returned action always be checked for special values and
 * the value handled accordingly.
 *
 * Allocation policy:
 *
 * Actions are allocated by the ObProcessor performing the actions.
 * It is the processor's responsibility to handle deallocation once
 * the action is complete or cancelled. A state machine MUST NOT
 * access an action once the operation that returned the ObAction has
 * completed or it has cancelled the ObAction.
 */
class ObAction
{
public:
  /**
   * Constructor of the ObAction object. ObProcessor implementers are
   * responsible for associating this action with the proper
   * ObContinuation.
   */
  ObAction() : continuation_(NULL), mutex_(NULL), cancelled_(false) { }

  virtual ~ObAction() { }

  /**
   * Cancels the asynchronous operation represented by this action.<p>
   * This method is called by state machines willing to cancel an
   * ongoing asynchronous operation. Classes derived from ObAction may
   * perform additional steps before flagging this action as cancelled.
   * There are certain rules that must be followed in order to cancel
   * an action (see the Remarks section).
   *
   * @param c      ObContinuation associated with this ObAction.
   */
  virtual int cancel(const ObContinuation *cont = NULL)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(NULL != cont) && OB_UNLIKELY(cont != continuation_)) {
      ret = common::OB_INVALID_ARGUMENT;
      PROXY_EVENT_LOG(WARN, "invalid argument, it should not happened", K(cont), K_(continuation), K(this), K(ret));
    } else if (OB_UNLIKELY(cancelled_)) {
      ret = common::OB_ERR_UNEXPECTED;
      PROXY_EVENT_LOG(WARN, "it has already be cancelled, it should not happened", K_(cancelled), K(this), K(ret));
    } else {
      cancelled_ = true;
    }
    return ret;
  }

  virtual ObContinuation *set_continuation(ObContinuation *cont)
  {
    continuation_ = cont;
    if (OB_LIKELY(NULL != cont)) {
      mutex_ = cont->mutex_;
    } else {
      mutex_ = NULL;
    }
    return cont;
  }

  int copy(ObAction &action)
  {
    int ret = common::OB_SUCCESS;
    continuation_ = action.continuation_;
    mutex_ = action.mutex_;
    cancelled_ = action.cancelled_;
    return ret;
  }

public:
  /**
   * Continuation that initiated this action.
   *
   * The reference to the initiating continuation is only used to
   * verify that the action is being cancelled by the correct
   * continuation.  This field should not be accessed or modified
   * directly by the state machine.
   */
  ObContinuation *continuation_;

  /**
   * Reference to the ObContinuation's lock.
   *
   * Keeps a reference to the ObContinuation's lock to preserve the
   * access to the cancelled field valid even when the state machine
   * has been deallocated. This field should not be accessed or
   * modified directly by the state machine.
   */
  common::ObPtr<ObProxyMutex> mutex_;

  /**
   * Internal flag used to indicate whether the action has been
   * cancelled.
   *
   * This flag is set after a call to cancel or cancel_action and
   * it should not be accessed or modified directly by the state
   * machine.
   */
  volatile bool cancelled_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAction);
};

} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_ACTION_H
