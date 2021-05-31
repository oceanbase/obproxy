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

#include "lib/wait_event/ob_wait_event.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{
ObWaitEventDesc::ObWaitEventDesc()
  : event_no_(0),
    p1_(0),
    p2_(0),
    p3_(0),
    wait_begin_time_(0),
    wait_end_time_(0),
    wait_time_(0),
    timeout_ms_(0),
    level_(0),
    parent_(0),
    is_phy_(false)
{
}

void ObWaitEventDesc::reset()
{
  event_no_ = 0;
  p1_ = 0;
  p2_ = 0;
  p3_ = 0;
  wait_begin_time_ = 0;
  wait_end_time_ = 0;
  wait_time_ = 0;
  timeout_ms_ = 0;
  level_ = 0;
  parent_ = 0;
  is_phy_ = false;
}

int64_t ObWaitEventDesc::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", KP(buf), K(buf_len), K(ret));
  } else if (event_no_ >= 0 && event_no_ < ObWaitEventIds::WAIT_EVENT_END) {
    J_KV(K_(event_no), K_(p1), K_(p2), K_(p3), K_(wait_begin_time), K_(wait_end_time), K_(wait_time), K_(timeout_ms), K_(level), K_(parent),
        "event_id", OB_WAIT_EVENTS[event_no_].event_id_,
        "event_name", OB_WAIT_EVENTS[event_no_].event_name_,
        "param1", OB_WAIT_EVENTS[event_no_].param1_,
        "param2", OB_WAIT_EVENTS[event_no_].param2_,
        "param3", OB_WAIT_EVENTS[event_no_].param3_);
  } else {
    J_KV(K_(event_no), K_(p1), K_(p2), K_(p3), K_(wait_begin_time), K_(wait_end_time), K_(wait_time), K_(timeout_ms), K_(level), K_(parent));
  }
  return pos;
}

ObWaitEventStat::ObWaitEventStat()
  : total_waits_(0),
    total_timeouts_(0),
    time_waited_(0),
    max_wait_(0)
{
}

int ObWaitEventStat::add(const ObWaitEventStat &other)
{
  int ret = OB_SUCCESS;
  if (other.is_valid()) {
    if (is_valid()) {
      total_waits_ += other.total_waits_;
      total_timeouts_ += other.total_timeouts_;
      time_waited_ += other.time_waited_;
      max_wait_ = std::max(max_wait_, other.max_wait_);
    } else {
      *this = other;
    }
  }
  return ret;
}

void ObWaitEventStat::reset()
{
  total_waits_ = 0;
  total_timeouts_ = 0;
  time_waited_ =0;
  max_wait_ =0;
}

}
}

