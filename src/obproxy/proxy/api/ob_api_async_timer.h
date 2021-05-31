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

#ifndef OBPROXY_API_ASYNC_TIMER_H
#define OBPROXY_API_ASYNC_TIMER_H

#include "proxy/api/ob_api_async.h"
#include "proxy/api/ob_api_defs.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

class ObContInternal;

/**
 * @brief This class provides an implementation of ObAsyncProvider that
 * acts as a timer. It sends events at the set frequency. Calling the
 * destructor will stop the events. A one-off timer just sends one
 * event. Calling the destructor before this event will cancel the timer.
 *
 * For either type, user must delete the timer.
 *
 * See example async_timer for sample usage.
 */
class ObAsyncTimer : public ObAsyncProvider
{
public:
  enum ObAsyncTimerType
  {
    TYPE_ONE_OFF = 0,
    TYPE_PERIODIC
  };

  static ObAsyncTimer *alloc(ObAsyncTimerType type, const int64_t period_in_ms,
                             const int64_t initial_period_in_ms = 0)
  {
    return op_reclaim_alloc_args(ObAsyncTimer, type, period_in_ms, initial_period_in_ms);
  }

  virtual void destroy();

  /**
   * Starts the timer.
   */
  void run();

  int handle_event(ObEventType event, void *edata);

  void cancel();

private:
  /**
   * @param type A one-off timer fires only once and a periodic timer fires periodically.
   * @param period_in_ms The receiver will receive an event every this many milliseconds.
   * @param initial_period_in_ms The first event will arrive after this many milliseconds. Subsequent
   *                             events will have "regular" cadence. This is useful if the timer is
   *                             set for a long period of time (1hr etc.), but an initial event is
   *                             required. Value of 0 (default) indicates no initial event is desired.
   */
  ObAsyncTimer(ObAsyncTimerType type, const int64_t period_in_ms,
               const int64_t initial_period_in_ms = 0);

  DISALLOW_COPY_AND_ASSIGN(ObAsyncTimer);
  ObContInternal *cont_;
  ObAsyncTimerType type_;
  int64_t period_in_ms_;
  int64_t initial_period_in_ms_;
  event::ObAction *initial_timer_action_;
  event::ObAction *periodic_timer_action_;
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_API_ASYNC_TIMER_H
