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

#ifndef OBPROXY_API_ASYNC_H
#define OBPROXY_API_ASYNC_H

#include "lib/oblog/ob_log.h"
#include "utils/ob_proxy_lib.h"
#include "iocore/eventsystem/ob_lock.h"
#include "iocore/eventsystem/ob_ethread.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

/**
 * @brief This class represents the interface of a dispatch controller. A dispatch controller
 * is used to dispatch an event to a receiver. This interface exists so that the types in this
 * header file can be defined.
 */
class ObAsyncDispatchControllerBase : public common::ObRefCountObj
{
public:
  ObAsyncDispatchControllerBase() { }
  virtual ~ObAsyncDispatchControllerBase() { }

  /**
   * Dispatches an async event to a receiver.
   *
   * @return True if the receiver was still alive.
   */
  virtual bool dispatch() = 0;

  // Renders dispatch unusable to communicate to receiver
  virtual void disable() = 0;

  // Returns true if receiver can be communicated with
  virtual bool is_enabled() = 0;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAsyncDispatchControllerBase);
};

/**
 * @brief ObAsyncProvider is the interface that providers of async operations must implement.
 * The system allows decoupling of the lifetime/scope of provider and receiver objects. The
 * receiver object might have expired before the async operation is complete and the system
 * handles this case. Because of this decoupling, it is the responsibility of the provider
 * to manage it's expiration - self-destruct on completion is a good option.
 */
class ObAsyncProvider
{
  friend class ObAsync;

public:
  virtual void destroy() { cancel(); }

  /**
   * This method is invoked when the async operation is requested. This call should be used
   * to just start the async operation and *not* block this thread. On completion,
   * get_dispatch_controller() can be used to invoke the receiver.
   */
  virtual void run() = 0;

  /**
   * Base implementation just breaks communication channel with receiver. Implementations
   * should add business logic here.
   */
  virtual void cancel()
  {
    if (NULL != dispatch_controller_) {
      dispatch_controller_->disable();
      dispatch_controller_ = NULL; // decrease reference count
    }
  }

protected:
  ObAsyncProvider() { }
  common::ObPtr<ObAsyncDispatchControllerBase> &get_dispatch_controller() { return dispatch_controller_; }
  virtual ~ObAsyncProvider() { }

private:
  void do_run(ObAsyncDispatchControllerBase *dispatch_controller)
  {
    dispatch_controller_ = dispatch_controller;
    run();
  }

  DISALLOW_COPY_AND_ASSIGN(ObAsyncProvider);
  common::ObPtr<ObAsyncDispatchControllerBase> dispatch_controller_;
};

/**
 * @brief ObAsyncReceiver is the interface that receivers of async operations must implement. It is
 * templated on the type of the async operation provider.
 */
class ObAsyncReceiver
{
  friend class ObAsync;
public:
  virtual void destroy() { cancel(); }

  /**
   * This method is invoked when the async operation is completed. The
   * mutex provided during the creation of the async operation will be
   * automatically locked during the invocation of this method.
   *
   * @param provider A reference to the provider which completed the async operation.
   */
  virtual void handle_async_complete(ObAsyncProvider &provider) = 0;

  /**
   * Base implementation just breaks communication channel with provider. Implementations
   * should add business logic here.
   *
   * @note If receiver don't need data from provider, must call cancel()
   */
  virtual void cancel()
  {
    if (NULL != dispatch_controller_) {
      dispatch_controller_->disable();
      dispatch_controller_ = NULL; // decrease reference count
    }
  }

protected:
  ObAsyncReceiver() { }
  common::ObPtr<ObAsyncDispatchControllerBase> &get_dispatch_controller() { return dispatch_controller_; }
  virtual ~ObAsyncReceiver() { }

private:
  void set_dispatcher(ObAsyncDispatchControllerBase *dispatch_controller)
  {
    dispatch_controller_ = dispatch_controller;
  }

  DISALLOW_COPY_AND_ASSIGN(ObAsyncReceiver);
  common::ObPtr<ObAsyncDispatchControllerBase> dispatch_controller_;
};

/**
 * @brief Dispatch controller implementation. When invoking the receiver, it verifies that the
 * receiver is still alive, locks the mutex and then invokes handle_async_complete ().
 */
class ObAsyncDispatchController : public ObAsyncDispatchControllerBase
{
public:
  static ObAsyncDispatchController *alloc(ObAsyncReceiver *event_receiver, ObAsyncProvider *provider,
                                          common::ObPtr<event::ObProxyMutex> &mutex)
  {
    return op_reclaim_alloc_args(ObAsyncDispatchController, event_receiver, provider, mutex);
  }

  bool dispatch()
  {
    bool ret = false;
    MUTEX_LOCK(lock, dispatch_mutex_, event::this_ethread());
    if (NULL != event_receiver_) {
      event_receiver_->handle_async_complete(*provider_);
      ret = true;
    }

    return ret;
  }

  void disable()
  {
    MUTEX_LOCK(lock, dispatch_mutex_, event::this_ethread());
    event_receiver_ = NULL;
  }

  bool is_enabled()
  {
    return (NULL != event_receiver_);
  }

  virtual void free()
  {
    dispatch_mutex_.release();
    op_reclaim_free(this);
  }

public:
  ObAsyncReceiver *event_receiver_;
  common::ObPtr<event::ObProxyMutex> dispatch_mutex_;
  virtual ~ObAsyncDispatchController() { }

private:
  /**
   * @param event_receiver The async complete event will be dispatched to this receiver.
   * @param provider ObAsync operation provider that is passed to the receiver on dispatch.
   * @param mutex Mutex of the receiver that is locked during the dispatch
   */
  ObAsyncDispatchController(ObAsyncReceiver *event_receiver,
                            ObAsyncProvider *provider,
                            common::ObPtr<event::ObProxyMutex> &mutex)
      : event_receiver_(event_receiver), dispatch_mutex_(mutex), provider_(provider)
  {
  }

  DISALLOW_COPY_AND_ASSIGN(ObAsyncDispatchController);
  ObAsyncProvider *provider_;
};

/**
 * @brief This class provides a method to create an async operation.
 */
class ObAsync
{
public:
  /**
   * This method sets up the dispatch controller to link the async operation provider and
   * receiver and then initiates the operation by invoking the provider.
   *
   * @param event_receiver The receiver of the async complete dispatch.
   * @param provider The provider of the async operation.
   * @param mutex The mutex that is locked during the dispatch of the async event complete.
   *              One will be created if nothing is passed in. ObApiTransaction plugins should use
   *              ObTransactionPlugin::get_mutex() here and global plugins can pass an appropriate
   *              or NULL mutex.
   */
  static void execute(ObAsyncReceiver *event_receiver,
                      ObAsyncProvider *provider, common::ObPtr<event::ObProxyMutex> mutex)
  {
    if (NULL == mutex) {
      mutex = event::new_proxy_mutex();
    }

    ObAsyncDispatchController *dispatcher = ObAsyncDispatchController::alloc(event_receiver, provider, mutex);
    if (NULL == dispatcher) {
      _OB_LOG(WDIAG, "failed to allocate memory for async dispatcher");
    } else {
      event_receiver->set_dispatcher(dispatcher);
      provider->do_run(dispatcher);
    }
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObAsync);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_API_ASYNC_H
