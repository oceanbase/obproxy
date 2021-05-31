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

#include "proxy/api/ob_api_async_timer.h"
#include "proxy/api/ob_api_utils_internal.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
using namespace common;
using namespace event;

int handle_timer_event(ObContInternal *cont, ObEventType event, void *edata);

ObAsyncTimer::ObAsyncTimer(ObAsyncTimerType type, const int64_t period_in_ms,
                           const int64_t initial_period_in_ms)
    : cont_(NULL), type_(type), period_in_ms_(period_in_ms),
      initial_period_in_ms_(initial_period_in_ms),
      initial_timer_action_(NULL), periodic_timer_action_(NULL)
{
  cont_ = ObContInternal::alloc(handle_timer_event, new_proxy_mutex());
  cont_->data_ = static_cast<void *>(this);
}

void ObAsyncTimer::destroy()
{
  cancel();
  ObAsyncProvider::destroy();
  op_reclaim_free(this);
}

void ObAsyncTimer::run()
{
  int64_t one_off_timeout_in_ms = 0;
  int64_t regular_timeout_in_ms = 0;
  if (ObAsyncTimer::TYPE_ONE_OFF == type_) {
    one_off_timeout_in_ms = period_in_ms_;
  } else {
    one_off_timeout_in_ms = initial_period_in_ms_;
    regular_timeout_in_ms = period_in_ms_;
  }

  if (one_off_timeout_in_ms > 0) {
    DEBUG_API("Scheduling initial/one-off event");
    initial_timer_action_ = ObApiUtilsInternal::cont_schedule(
        cont_, one_off_timeout_in_ms, OB_THREAD_POOL_DEFAULT);
  } else if (regular_timeout_in_ms) {
    DEBUG_API("Scheduling regular timer events");
    periodic_timer_action_ = ObApiUtilsInternal::cont_schedule_every(
        cont_, regular_timeout_in_ms, OB_THREAD_POOL_DEFAULT);
  }
}

void ObAsyncTimer::cancel()
{
  if (NULL == cont_) {
    DEBUG_API("Already canceled");
  } else {
    MUTEX_LOCK(lock, cont_->mutex_, this_ethread());
    if (NULL != initial_timer_action_) {
      DEBUG_API("Canceling initial timer action");
      ObApiUtilsInternal::action_cancel(initial_timer_action_);
    }

    if (NULL != periodic_timer_action_) {
      DEBUG_API("Canceling periodic timer action");
      ObApiUtilsInternal::action_cancel(periodic_timer_action_);
    }

    DEBUG_API("Destroying cont");
    cont_->destroy();
    cont_ = NULL;
  }
}

int ObAsyncTimer::handle_event(ObEventType event, void *edata)
{
  UNUSED(event);
  UNUSED(edata);
  if (NULL != initial_timer_action_) {
    DEBUG_API("Received initial timer event.");
    initial_timer_action_ = NULL; // mark it so that it won't be canceled later on
    if (ObAsyncTimer::TYPE_PERIODIC == type_) {
      DEBUG_API("Scheduling periodic event now");
      periodic_timer_action_ = ObApiUtilsInternal::cont_schedule(
          cont_, period_in_ms_, OB_THREAD_POOL_DEFAULT);
    }
  }

  if (!get_dispatch_controller()->dispatch()) {
    DEBUG_API("Receiver has died. Destroying timer");
    destroy(); // auto-destruct only in this case
  }

  return EVENT_DONE;
}

int handle_timer_event(ObContInternal *cont, ObEventType event, void *edata)
{
  ObAsyncTimer *timer = static_cast<ObAsyncTimer *>(cont->data_);
  if (NULL != timer) {
    timer->handle_event(event, edata);
  }
  return EVENT_DONE;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
