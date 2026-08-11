/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/statistic_event/ob_stat_event.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{


ObStatEventAddStat::ObStatEventAddStat()
  : stat_no_(0),
    stat_value_(0)
{
}

int ObStatEventAddStat::add(const ObStatEventAddStat &other)
{
  int ret = OB_SUCCESS;
  if (other.is_valid()) {
    if (is_valid()) {
      stat_value_ += other.stat_value_;
    } else {
      *this = other;
    }
  }
  return ret;
}

int ObStatEventAddStat::add(int64_t value)
{
  int ret = OB_SUCCESS;
  stat_value_ += value;
  return ret;
}

void ObStatEventAddStat::reset()
{
  stat_no_ = 0;
  stat_value_ = 0;
}

ObStatEventSetStat::ObStatEventSetStat()
  : stat_no_(0),
    stat_value_(0),
    set_time_(0)
{
}

int ObStatEventSetStat::add(const ObStatEventSetStat &other)
{
  int ret = OB_SUCCESS;
  if (other.is_valid()) {
    if (is_valid()) {
      if (set_time_ < other.set_time_) {
        *this = other;
      }
    } else {
      *this = other;
    }
  }
  return ret;
}

void ObStatEventSetStat::reset()
{
  stat_no_ = 0;
  stat_value_ = 0;
  set_time_ = 0;
}


}
}

