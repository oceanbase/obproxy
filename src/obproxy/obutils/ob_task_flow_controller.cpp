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

#define USING_LOG_PREFIX PROXY
#include "obutils/ob_task_flow_controller.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "utils/ob_proxy_utils.h"
using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
ObTaskFlowController::ObTaskFlowController()
    : normal_threshold_(100), limited_threshold_(10),
      limited_period_(HRTIME_SECONDS(20)),
      task_count_(0), task_stat_end_time_(0),
      delivery_count_(0), delivery_stat_end_time_(0),
      limited_end_time_(0)
{
}

void ObTaskFlowController::handle_new_task()
{
  ObHRTime cur_time = get_hrtime_internal();
  LOG_DEBUG("handle new task", K(cur_time), K(*this));

  if (cur_time > limited_end_time_) { // is not in limited state
    // set task_stat_end_time if in a new task stat period
    ObHRTime task_stat_end_time_snapshot = task_stat_end_time_;
    if (cur_time > task_stat_end_time_snapshot) {
      ObHRTime new_task_stat_end_time = cur_time + TASK_STAT_PERIOD;
      if (ATOMIC_BCAS(&task_stat_end_time_, task_stat_end_time_snapshot, new_task_stat_end_time)) {
        // set successfully, reset task count to 0
        task_count_ = 0;
      } else {
        // do nothing
      }
    }

    // increase task_count
    ATOMIC_INC(&task_count_);
    if (task_count_ > normal_threshold_) {
      // task_count more than normal_threshold, enter limited state
      ObHRTime new_limited_end_time =
        cur_time +ObRandomNumUtils::get_random_half_to_full(limited_period_);
      ATOMIC_SET(&limited_end_time_, new_limited_end_time);
    }
  } else {
    // do nothing in limited state
  }
}

bool ObTaskFlowController::can_deliver_task()
{
  bool bret = true;
  ObHRTime cur_time = get_hrtime_internal();
  LOG_DEBUG("check whether we are able to delivery this task", K(cur_time), K(*this));

  // set delivery_stat_end_time if in a new delivery stat period
  ObHRTime delivery_stat_end_time_snapshot = delivery_stat_end_time_;
  if (cur_time > delivery_stat_end_time_snapshot) {
    ObHRTime new_delivery_stat_end_time = cur_time + DELIVERY_STAT_PERIOD;
    if (ATOMIC_BCAS(&delivery_stat_end_time_,
                    delivery_stat_end_time_snapshot, new_delivery_stat_end_time)) {
      // set successfully, reset task count to 0
      delivery_count_ = 0;
    } else {
      // do nothing
    }
  }

  // set threshold according delivery state
  int64_t cur_threshold = normal_threshold_;
  if (OB_UNLIKELY(cur_time < limited_end_time_)) {
    cur_threshold = limited_threshold_;
  }

  if (delivery_count_ < cur_threshold) {
    // able to deliver, increase count
    ATOMIC_INC(&delivery_count_);
  } else {
    // disable to deliver
    bret = false;
  }

  return bret;
}

int64_t ObTaskFlowController::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(normal_threshold),
       K_(limited_threshold),
       K_(limited_period),
       K_(task_count),
       K_(task_stat_end_time),
       K_(delivery_count),
       K_(delivery_stat_end_time),
       K_(limited_end_time)
       );
  J_OBJ_END();
  return pos;
}

ObTaskFlowController &get_pl_task_flow_controller()
{
  static ObTaskFlowController pl_task_flow_controller;
  return pl_task_flow_controller;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
