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

#ifndef OBPROXY_TASK_FLOW_CONTROLLER_H
#define OBPROXY_TASK_FLOW_CONTROLLER_H
#include "lib/time/ob_hrtime.h"
namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
class ObTaskFlowController
{
public:
  static const ObHRTime TASK_STAT_PERIOD = HRTIME_SECONDS(1); // new task window size
  static const ObHRTime DELIVERY_STAT_PERIOD = HRTIME_SECONDS(1); // delivery window size

  ObTaskFlowController();
  ~ObTaskFlowController() {}

  void set_normal_threshold(const int64_t threshold) { normal_threshold_ = threshold; }
  void set_limited_threshold(const int64_t threshold) { limited_threshold_ = threshold; }
  void set_limited_period(const ObHRTime limited_period) { limited_period_ = limited_period; }

  void handle_new_task();
  bool can_deliver_task();

  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  // conf value
  int64_t normal_threshold_; // max delivery task per second
  int64_t limited_threshold_; // in limited state, max delivery task per second
  ObHRTime limited_period_; // the time period of limited delivery state

  // variables
  int64_t task_count_; // task count of current task stat period
  ObHRTime task_stat_end_time_; // end time of current stat period
  int64_t delivery_count_; // delivered task count of current delivered task stat period
  ObHRTime delivery_stat_end_time_; // end time of current delivered task stat period
  ObHRTime limited_end_time_; // the end time of current limited period
};

extern ObTaskFlowController &get_pl_task_flow_controller();
} // end of obutils
} // end of obproxy
} // end of oceanbase
#endif // OBPROXY_TASK_FLOW_CONTROLLER_H
