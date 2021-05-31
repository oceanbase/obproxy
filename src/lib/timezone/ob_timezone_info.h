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

#ifndef OCEANBASE_LIB_TIMEZONE_INFO_
#define OCEANBASE_LIB_TIMEZONE_INFO_

#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{

struct ObTimeZoneName
{
  ObString  name_;
  int32_t   lower_idx_;   // index of ObTimeZoneTrans array.
  int32_t   upper_idx_;   // index of ObTimeZoneTrans array.
  int32_t   tz_id;        // tmp members, will be removed later.
};

struct ObTimeZoneTrans
{
  int64_t   trans_;
  int32_t   offset_;
  int32_t   tz_id;        // tmp members, will be removed later.
};

static const int32_t INVALID_TZ_OFF = INT32_MAX;

class ObTimeZoneInfo
{
  OB_UNIS_VERSION(1);
public:
  ObTimeZoneInfo()
    : tz_id_(0), offset_(0)
  {}
  int set_timezone(const ObString &str);
  int get_timezone_offset(int64_t value, int32_t &offset) const;
  void reset()
  {
    tz_id_ = 0;
    offset_ = 0;
  }
  TO_STRING_KV(N_ID, tz_id_, N_OFFSET, offset_);

private:
  static ObTimeZoneName TIME_ZONE_NAMES[];
  static ObTimeZoneTrans TIME_ZONE_TRANS[];

private:
  static int get_timezone_id(const ObString &str, int32_t &tz_id);

private:
  int32_t tz_id_;
  int32_t offset_;
};

} // end of common
} // end of oceanbase

#endif // OCEANBASE_LIB_TIMEZONE_INFO_
