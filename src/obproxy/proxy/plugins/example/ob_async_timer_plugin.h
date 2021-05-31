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

#ifndef OBPROXY_ASYNC_TIMER_PLUGIN_H
#define OBPROXY_ASYNC_TIMER_PLUGIN_H

#include "lib/oblog/ob_log.h"
#include "ob_proxy_lib.h"
#include "ob_lock.h"
#include "ob_api_async.h"
#include "ob_api_async_timer.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

class ObTimerEventReceiver : public ObAsyncReceiver
{
public:
  static ObTimerEventReceiver *alloc(ObAsyncTimer::ObAsyncTimerType type,
                                     const int64_t period_in_ms,
                                     const int64_t initial_period_in_ms = 0,
                                     const int64_t max_instances = 0,
                                     const bool cancel = false)
  {
    return op_reclaim_alloc_args(ObTimerEventReceiver, type, period_in_ms,
                                 initial_period_in_ms, max_instances, cancel);
  }

  ObTimerEventReceiver(ObAsyncTimer::ObAsyncTimerType type,
                       const int64_t period_in_ms,
                       const int64_t initial_period_in_ms = 0,
                       const int64_t max_instances = 0,
                       const bool cancel = false)
      : max_instances_(max_instances), instance_count_(0), type_(type), cancel_(cancel)
  {
    timer_ = ObAsyncTimer::alloc(type, period_in_ms, initial_period_in_ms);
    ObAsync::execute(this, timer_, common::ObPtr<event::ObProxyMutex>(event::new_proxy_mutex()));
  }

  virtual void destroy()
  {
    ObAsyncReceiver::destroy();
    timer_->destroy();
    op_reclaim_free(this);
  }

  virtual void handle_async_complete(ObAsyncProvider &timer)
  {
    UNUSED(timer);
    _OB_LOG(DEBUG, "Got timer event in object %p!", this);
    if ((ObAsyncTimer::TYPE_ONE_OFF == type_)
        || (max_instances_ && (++instance_count_ == max_instances_))) {
      _OB_LOG(DEBUG, "Stopping timer in object %p!", this);
      cancel_ ? timer_->cancel() : destroy();
    }
  }

private:
  int64_t max_instances_;
  int64_t instance_count_;
  ObAsyncTimer::ObAsyncTimerType type_;
  ObAsyncTimer *timer_;
  bool cancel_;
};

static inline void init_async_timer()
{
  int64_t period_in_ms = 1000;
  ObTimerEventReceiver *timer1 = ObTimerEventReceiver::alloc(ObAsyncTimer::TYPE_PERIODIC, period_in_ms);
  _OB_LOG(DEBUG, "Created periodic timer %p with initial period 0, regular period %ld and max instances 0",
          timer1, period_in_ms);

  int64_t initial_period_in_ms = 100;
  ObTimerEventReceiver *timer2 = ObTimerEventReceiver::alloc(
      ObAsyncTimer::TYPE_PERIODIC, period_in_ms, initial_period_in_ms);
  _OB_LOG(DEBUG, "Created periodic timer %p with initial period %ld, regular period %ld and max instances 0",
          timer2, initial_period_in_ms, period_in_ms);

  initial_period_in_ms = 200;
  int64_t max_instances = 10;
  ObTimerEventReceiver *timer3 = ObTimerEventReceiver::alloc(
      ObAsyncTimer::TYPE_PERIODIC, period_in_ms, initial_period_in_ms, max_instances);
  _OB_LOG(DEBUG, "Created periodic timer %p with initial period %ld, regular period %ld and max instances %ld",
          timer3, initial_period_in_ms, period_in_ms, max_instances);

  ObTimerEventReceiver *timer4 = ObTimerEventReceiver::alloc(ObAsyncTimer::TYPE_ONE_OFF, period_in_ms);
  _OB_LOG(DEBUG, "Created one-off timer %p with period %ld", timer4, period_in_ms);

  initial_period_in_ms = 0;
  max_instances = 5;
  ObTimerEventReceiver *timer5 = ObTimerEventReceiver::alloc(
      ObAsyncTimer::TYPE_PERIODIC, period_in_ms, initial_period_in_ms, max_instances, true);
  _OB_LOG(DEBUG, "Created canceling timer %p with initial period %ld, regular period %ld and max instances %ld",
          timer5, initial_period_in_ms, period_in_ms, max_instances);
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_ASYNC_TIMER_PLUGIN_H
