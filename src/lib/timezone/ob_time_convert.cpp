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

#define USING_LOG_PREFIX  LIB_TIME

#include "lib/timezone/ob_time_convert.h"

#include <ctype.h>
#include <math.h>
#include "lib/ob_define.h"
#include "lib/oblog/ob_log.h"
//#include "lib/timezone/ob_timezone_util.h"

namespace oceanbase
{
namespace common
{

ObTimeConverter::ObTimeConverter()
{
}

ObTimeConverter::~ObTimeConverter()
{
}

const int32_t DT_PART_BASE[DATETIME_PART_CNT] = { 100, 12, -1, 24, 60, 60, 1000000};
const int32_t DT_PART_MIN[DATETIME_PART_CNT]  = {   0,  1,  1,  0,  0,  0, 0};
const int32_t DT_PART_MAX[DATETIME_PART_CNT]  = {9999, 12, 31, 23, 59, 59, 1000000};
// 1000000 for usecond, because sometimes we round .9999999 to  .1000000

static const int8_t DAYS_PER_MON[2][12 + 1] = {
  {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
  {0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
};

static const int32_t DAYS_UNTIL_MON[2][12 + 1] = {
  {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365},
  {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366}
};

/**
 * 3 days after wday 5 is wday 1, so [3][5] = 1.
 * 5 days before wday 2 is wday 4, so [-5][2] = 4.
 * and so on, max wday is 7, min offset is -6, max offset days is 6.
 */
static const int8_t WDAY_OFFSET_ARR[DAYS_PER_WEEK * 2 - 1][DAYS_PER_WEEK + 1] = {
  {0, 2, 3, 4, 5, 6, 7, 1},
  {0, 3, 4, 5, 6, 7, 1, 2},
  {0, 4, 5, 6, 7, 1, 2, 3},
  {0, 5, 6, 7, 1, 2, 3, 4},
  {0, 6, 7, 1, 2, 3, 4, 5},
  {0, 7, 1, 2, 3, 4, 5, 6},   // offset = -1, wday = 1/2/3/4/5/6/7.
  {0, 1, 2, 3, 4, 5, 6, 7},   // offset =  0, wday = 1/2/3/4/5/6/7.
  {0, 2, 3, 4, 5, 6, 7, 1},   // offset =  1, wday = 1/2/3/4/5/6/7.
  {0, 3, 4, 5, 6, 7, 1, 2},
  {0, 4, 5, 6, 7, 1, 2, 3},
  {0, 5, 6, 7, 1, 2, 3, 4},
  {0, 6, 7, 1, 2, 3, 4, 5},
  {0, 7, 1, 2, 3, 4, 5, 6}
};

static const int8_t (*WDAY_OFFSET)[DAYS_PER_WEEK + 1] = &WDAY_OFFSET_ARR[6];

/*
 * if wday of yday 1 is 4, not SUN_BEGIN, not GE_4_BEGIN, yday of fitst day of week 1 is 5, so [4][0][0] = 5.
 * if wday of yday 1 is 2,     SUN_BEGIN,     GE_4_BEGIN, yday of fitst day of week 1 is 5, so [2][1][1] = 1.
 * and so on, max wday is 7, and other two is 1.
 * ps: if the first week is not full week(GE_4_BEGIN), the yday maybe zero or neg, such as 0 means
 *     the last day of prev year, and so on.
 */
static const int8_t YDAY_WEEK1[DAYS_PER_WEEK + 1][2][2] = {
  {{0,  0}, {0,  0}},
  {{1,  1}, {7,  0}},  // wday of day 1 is 1.
  {{7,  0}, {6, -1}},  // 2.
  {{6, -1}, {5, -2}},  // 3.
  {{5, -2}, {4,  4}},  // 4.
  {{4,  4}, {3,  3}},  // 5.
  {{3,  3}, {2,  2}},  // 6.
  {{2,  2}, {1,  1}}   // 7.
};

#define WEEK_MODE_CNT   8
static const ObDTMode WEEK_MODE[WEEK_MODE_CNT] = {
  DT_WEEK_SUN_BEGIN | DT_WEEK_ZERO_BEGIN                       ,
                      DT_WEEK_ZERO_BEGIN | DT_WEEK_GE_4_BEGIN  ,
  DT_WEEK_SUN_BEGIN                                            ,
                                           DT_WEEK_GE_4_BEGIN  ,
  DT_WEEK_SUN_BEGIN | DT_WEEK_ZERO_BEGIN | DT_WEEK_GE_4_BEGIN  ,
                      DT_WEEK_ZERO_BEGIN                       ,
  DT_WEEK_SUN_BEGIN |                      DT_WEEK_GE_4_BEGIN  ,
                                                              0
};

static const int32_t DAYS_PER_YEAR[2]=
{
  DAYS_PER_NYEAR, DAYS_PER_LYEAR
};

#define EPOCH_YEAR4   1970
#define EPOCH_YEAR2   70
#define EPOCH_WDAY    4       // 1970-1-1 is thursday.
#define LEAP_YEAR_COUNT(y)  ((y) / 4 - (y) / 100 + (y) / 400)
#define IS_LEAP_YEAR(y) ((((y) % 4) == 0 && (((y) % 100) != 0 || ((y) % 400) == 0)) ? 1 : 0)
#define TIME_MAX_HOUR 838
#define TIME_MAX_VAL (3020399 * 1000000LL)    // 838:59:59 .
#define YEAR_MAX_YEAR 2155
#define YEAR_MIN_YEAR 1901
#define YEAR_BASE_YEAR 1900

#define INT32_MAX_DIGITS_LEN   10
static const int32_t power_of_10[INT32_MAX_DIGITS_LEN] = {
  1L,
  10L,
  100L,
  1000L,
  10000L,
  100000L,
  1000000L,
  10000000L,
  100000000L,
  1000000000L,
//2147483647
};

struct ObIntervalIndex {
  int32_t begin_;
  int32_t end_;
  int32_t count_;
  bool calc_with_usecond_;
  // in some cases we can trans the inteval to usecond exactly,
  // in other cases we calc with ob_time, like month, or quarter, or year, because we are not sure
  // how many days should be added exactly in simple way.
};

static const ObIntervalIndex INTERVAL_INDEX[DATE_UNIT_MAX] = {
  {DT_USEC, DT_USEC, 1, true},    // DATE_UNIT_MICROSECOND.
  {DT_SEC,  DT_SEC,  1, true},    // DATE_UNIT_SECOND.
  {DT_MIN,  DT_MIN,  1, true},    // DATE_UNIT_MINUTE.
  {DT_HOUR, DT_HOUR, 1, true},    // DATE_UNIT_HOUR.
  {DT_MDAY,  DT_MDAY,  1, true},    // DATE_UNIT_DAY.
  {DT_MDAY,  DT_MDAY,  1, true},    // DATE_UNIT_WEEK.
  {DT_MON,  DT_MON,  1, false},   // DATE_UNIT_MONTH.
  {DT_MON,  DT_MON,  1, false},   // DATE_UNIT_QUARTER.
  {DT_YEAR, DT_YEAR, 1, false},   // DATE_UNIT_YEAR.
  {DT_SEC,  DT_USEC, 2, true},    // DATE_UNIT_SECOND_MICROSECOND.
  {DT_MIN,  DT_USEC, 3, true},    // DATE_UNIT_MINUTE_MICROSECOND.
  {DT_MIN,  DT_SEC,  2, true},    // DATE_UNIT_MINUTE_SECOND.
  {DT_HOUR, DT_USEC, 4, true},    // DATE_UNIT_HOUR_MICROSECOND.
  {DT_HOUR, DT_SEC,  3, true},    // DATE_UNIT_HOUR_SECOND.
  {DT_HOUR, DT_MIN,  2, true},    // DATE_UNIT_HOUR_MINUTE.
  {DT_MDAY,  DT_USEC, 5, true},    // DATE_UNIT_DAY_MICROSECOND.
  {DT_MDAY,  DT_SEC,  4, true},    // DATE_UNIT_DAY_SECOND.
  {DT_MDAY,  DT_MIN,  3, true},    // DATE_UNIT_DAY_MINUTE.
  {DT_MDAY,  DT_HOUR, 2, true},    // DATE_UNIT_DAY_HOUR.
  {DT_YEAR, DT_MON,  2, false},   // DATE_UNIT_YEAR_MONTH.
};

static const ObTimeConverter::ObTimeConstStr MDAY_NAMES[31 + 1] = {
  {"null", 4},
  {"1st", 3}, {"2nd", 3}, {"3rd", 3}, {"4th", 3}, {"5th", 3}, {"6th", 3}, {"7th", 3},
  {"8th", 3}, {"9th", 3}, {"10th", 4}, {"11th", 4}, {"12th", 4}, {"13th", 4}, {"14th", 4},
  {"15th", 4}, {"16th", 4}, {"17th", 4}, {"18th", 4}, {"19th", 4}, {"20th", 4}, {"21st", 4},
  {"22nd", 4}, {"23rd", 4}, {"24th", 4}, {"25th", 4}, {"26th", 4}, {"27th", 4}, {"28th", 4},
  {"29th", 4}, {"30th", 4}, {"31st", 4}
};

static const char *DAY_NAME[31 + 1] = {
  "", "1st", "2nd", "3rd", "4th", "5th", "6th", "7th", "8th", "9th", "10th",
  "11th", "12th", "13th", "14th", "15th", "16th", "17th", "18th", "19th", "20th",
  "21st", "22nd", "23rd", "24th", "25th", "26th", "27th", "28th", "29th", "30th",
  "31st"
};

static const ObTimeConverter::ObTimeConstStr WDAY_NAMES[DAYS_PER_WEEK + 1] = {
  {"null", 4},
  {"Monday", 6}, {"Tuesday", 7}, {"Wednesday", 9}, {"Thursday", 8}, {"Friday", 6}, {"Saturday", 8}, {"Sunday", 6}
};

static const ObTimeConverter::ObTimeConstStr WDAY_ABBR_NAMES[DAYS_PER_WEEK + 1] = {
  {"null", 4},
  {"Mon", 3}, {"Tue", 3}, {"Wed", 3}, {"Thu", 3}, {"Fri", 3}, {"Sat", 3}, {"Sun", 3}
};

static const ObTimeConverter::ObTimeConstStr MON_NAMES[12 + 1] = {
  {"null", 4},
  {"January", 7}, {"February", 8}, {"March", 5}, {"April", 5}, {"May", 3}, {"June", 4},
  {"July", 4}, {"August", 6}, {"September", 9}, {"October", 7}, {"November", 8}, {"December", 8}
};

static const ObTimeConverter::ObTimeConstStr MON_ABBR_NAMES[12 + 1] = {
  {"null", 4},
  {"Jan", 3}, {"Feb", 3}, {"Mar", 3}, {"Apr", 3}, {"May", 3}, {"Jun", 3},
  {"Jul", 3}, {"Aug", 3}, {"Sep", 3}, {"Oct", 3}, {"Nov", 3}, {"Dec", 3}
};

#define START_WITH_SUNDAY       0x04
#define WEEK_FIRST_WEEKDAY      0x02
#define INCLUDE_CRITICAL_WEEK   0x01

const int64_t SECS_PER_HOUR = (SECS_PER_MIN * MINS_PER_HOUR);
const int64_t SECS_PER_DAY = (SECS_PER_HOUR * HOURS_PER_DAY);
const int64_t USECS_PER_DAY = (static_cast<int64_t>(USECS_PER_SEC) * SECS_PER_DAY);

////////////////////////////////
// int / double / string -> datetime / date / time / year.
int ObTimeConverter::int_to_datetime(int64_t int_part, int64_t dec_part,
                                     const ObTimeZoneInfo *tz_info, int64_t &value)
{
  int ret = OB_SUCCESS;
  dec_part = (dec_part + 500) / 1000;
  if (0 == int_part) {
    value = ZERO_DATETIME;
  } else {
    ObTime ob_time(DT_TYPE_DATETIME);
    if (OB_FAIL(int_to_ob_time_with_date(int_part, ob_time))) {
      LOG_WARN("failed to convert integer to datetime", K(ret));
    } else if (OB_FAIL(ob_time_to_datetime(ob_time, tz_info, value))) {
      LOG_WARN("failed to convert datetime to seconds", K(ret));
    }
  }
  value += dec_part;
  return ret;
}

int ObTimeConverter::int_to_date(int64_t int64, int32_t &value)
{
  int ret = OB_SUCCESS;
  if (0 == int64) {
      value = ZERO_DATE;
    } else {
    ObTime ob_time(DT_TYPE_DATE);
    if (OB_FAIL(int_to_ob_time_with_date(int64, ob_time))) {
      LOG_WARN("failed to convert integer to date", K(ret));
    } else {
      value = ob_time.parts_[DT_DATE]; //value = int32_min when all parts are zero
    }
  }
  return ret;
}

int ObTimeConverter::int_to_time(int64_t int64, int64_t &value)
{
  int ret = OB_SUCCESS;
  ObTime ob_time(DT_TYPE_TIME);
  if (OB_FAIL(int_to_ob_time_without_date(int64, ob_time))) {
    LOG_WARN("failed to convert integer to time", K(ret));
  } else {
    value = ob_time_to_time(ob_time);
    ret = time_overflow_trunc(value);
    if (DT_MODE_NEG & ob_time.mode_) {
      value = -value;
    }
  }
  return ret;
}

int ObTimeConverter::int_to_year(int64_t int64, uint8_t &value)
{
  int ret = OB_SUCCESS;
  if (0 == int64) {
    value = ZERO_YEAR;
  } else {
    apply_date_year2_rule(int64);
    if (OB_FAIL(validate_year(int64))) {
      LOG_WARN("year integer is invalid or out of range", K(ret));
    } else {
      value = static_cast<uint8_t>(int64 - YEAR_BASE_YEAR);
    }
  }
  return ret;
}

int ObTimeConverter::str_to_datetime(const ObString &str, const ObTimeZoneInfo *tz_info,
                                     int64_t &value, int16_t *scale)
{
  int ret = OB_SUCCESS;
  ObTime ob_time(DT_TYPE_DATETIME);
  if (OB_FAIL(str_to_ob_time_with_date(str, ob_time, scale))) {
    LOG_WARN("failed to convert string to datetime", K(ret));
  } else if (OB_FAIL(ob_time_to_datetime(ob_time, tz_info, value))) {
    LOG_WARN("failed to convert datetime to seconds", K(ret));
  }
  return ret;
}

int ObTimeConverter::str_to_datetime_format(const ObString &str, const ObString &fmt,
                                            const ObTimeZoneInfo *tz_info, int64_t &value, int16_t *scale)
{
  int ret = OB_SUCCESS;
  ObTime ob_time(DT_TYPE_DATETIME);
  if (OB_FAIL(str_to_ob_time_format(str, fmt, ob_time, scale))) {
    LOG_WARN("failed to convert string to datetime", K(ret));
  } else if (OB_FAIL(ob_time_to_datetime(ob_time, tz_info, value))) {
    LOG_WARN("failed to convert datetime to seconds", K(ret));
  }
  return ret;
}

int ObTimeConverter::str_to_date(const ObString &str, int32_t &value)
{
  int ret = OB_SUCCESS;
  ObTime ob_time(DT_TYPE_DATE);
  if (OB_FAIL(str_to_ob_time_with_date(str, ob_time))) {
    LOG_WARN("failed to convert string to date", K(ret));
  } else {
    value = ob_time.parts_[DT_DATE];
  }
  return ret;
}

int ObTimeConverter::str_to_time(const ObString &str, int64_t &value, int16_t *scale)
{
  int ret = OB_SUCCESS;
  ObTime ob_time(DT_TYPE_TIME);
  if (OB_FAIL(str_to_ob_time_without_date(str, ob_time, scale))) {
    LOG_WARN("failed to convert string to time", K(ret));
  } else {
    value = ob_time_to_time(ob_time);
    ret = time_overflow_trunc(value);
    if (DT_MODE_NEG & ob_time.mode_) {
      value = -value;
    }
  }
  return ret;
}

int ObTimeConverter::str_to_year(const ObString &str, uint8_t &value)
{
  int ret = OB_SUCCESS;
  if (NULL == str.ptr() || 0 == str.length()) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else {
    const char *pos = str.ptr();
    const char *end = pos + str.length();
    ObTimeDigits digits;
    for (; !isdigit(*pos) && pos < end; ++pos) {}
    if (OB_FAIL(get_datetime_digits(pos, end, INT32_MAX, digits))) {
      LOG_WARN("failed to get digits from year string", K(ret));
    } else {
      apply_date_year2_rule(digits);
      if (OB_FAIL(validate_year(digits.value_))) {
        LOG_WARN("year integer is invalid or out of range", K(ret));
      } else {
        value = (0 == digits.value_) ? ZERO_YEAR : static_cast<uint8_t>(digits.value_ - YEAR_BASE_YEAR);
      }
    }
  }
  return ret;
}

int ObTimeConverter::str_to_interval(const ObString &str, ObDateUnitType unit_type, int64_t &value)
{
  int ret = OB_SUCCESS;
  ObInterval ob_interval;
  if (OB_FAIL(str_to_ob_interval(str, unit_type, ob_interval))) {
    LOG_WARN("failed to convert string to ob interval", K(ret));
  } else if (OB_FAIL(ob_interval_to_interval(ob_interval, value))) {
    LOG_WARN("failed to convert ob interval to interval", K(ret));
  }
  return ret;
}

////////////////////////////////
// int / double / uint / string <- datetime / date / time / year.

int ObTimeConverter::datetime_to_int(int64_t value, const ObTimeZoneInfo *tz_info, int64_t &int64)
{
  int ret = OB_SUCCESS;
  ObTime ob_time;
  if (OB_FAIL(datetime_to_ob_time(value, tz_info, ob_time))) {
    LOG_WARN("failed to convert seconds to ob time", K(ret));
  } else {
    int64 = ob_time_to_int(ob_time, DT_TYPE_DATETIME);
  }
  return ret;
}

int ObTimeConverter::datetime_to_double(int64_t value, const ObTimeZoneInfo *tz_info, double &dbl)
{
  int ret = OB_SUCCESS;
  ObTime ob_time;
  if (OB_FAIL(datetime_to_ob_time(value, tz_info, ob_time))) {
    LOG_WARN("failed to convert seconds to ob time", K(ret));
  } else {
    dbl = static_cast<double>(ob_time_to_int(ob_time, DT_TYPE_DATETIME))
          + ob_time.parts_[DT_USEC] / static_cast<double>(USECS_PER_SEC);
  }
  return ret;
}

int ObTimeConverter::datetime_to_str(int64_t value, const ObTimeZoneInfo *tz_info, int16_t scale,
                                     char *buf, int64_t buf_len, int64_t &pos, bool with_delim)
{
  int ret = OB_SUCCESS;
  ObTime ob_time;
  round_datetime(scale, value);
  if (OB_FAIL(datetime_to_ob_time(value, tz_info, ob_time))) {
    LOG_WARN("failed to convert seconds to ob time", K(ret));
  } else if (OB_FAIL(ob_time_to_str(ob_time, DT_TYPE_DATETIME, scale, buf, buf_len, pos, with_delim))) {
    LOG_WARN("failed to convert ob time to string", K(ret));
  }
  return ret;
}

int ObTimeConverter::date_to_int(int32_t value, int64_t &int64)
{
  int ret = OB_SUCCESS;
  ObTime ob_time;
  if (OB_FAIL(date_to_ob_time(value, ob_time))) {
    LOG_WARN("failed to convert days to ob time", K(ret));
  } else {
    int64 = ob_time_to_int(ob_time, DT_TYPE_DATE);
  }
  return ret;
}

int ObTimeConverter::date_to_str(int32_t value, char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObTime ob_time;
  if (OB_FAIL(date_to_ob_time(value, ob_time))) {
    LOG_WARN("failed to convert days to ob time", K(ret));
  } else if (OB_FAIL(ob_time_to_str(ob_time, DT_TYPE_DATE, 0, buf, buf_len, pos, true))) {
    LOG_WARN("failed to convert ob time to string", K(ret));
  }
  return ret;
}

int ObTimeConverter::time_to_int(int64_t value, int64_t &int64)
{
  int ret = OB_SUCCESS;
  ObTime ob_time;
  if (OB_FAIL(time_to_ob_time(value, ob_time))) {
    LOG_WARN("failed to convert seconds to ob time", K(ret));
  } else {
    int64 = ob_time_to_int(ob_time, DT_TYPE_TIME);
    if (DT_MODE_NEG & ob_time.mode_) {
      int64 = -int64;
    }
  }
  return ret;
}

int ObTimeConverter::time_to_double(int64_t value, double &dbl)
{
  int ret = OB_SUCCESS;
  ObTime ob_time;
  if (OB_FAIL(time_to_ob_time(value, ob_time))) {
    LOG_WARN("failed to convert seconds to ob time", K(ret));
  } else {
    dbl = static_cast<double>(ob_time_to_int(ob_time, DT_TYPE_TIME)) +
          ob_time.parts_[DT_USEC] / static_cast<double>(USECS_PER_SEC);
    if (DT_MODE_NEG & ob_time.mode_) {
      dbl = -dbl;
    }
  }
  return ret;
}
int ObTimeConverter::time_to_datetime(int64_t t_value, int64_t cur_dt_value,
                              const ObTimeZoneInfo *tz_info, int64_t &dt_value, const ObObjType expect_type)
{
  int ret = OB_SUCCESS;
  if (ObTimestampType == expect_type) {
    if (OB_FAIL(add_timezone_offset(tz_info, cur_dt_value))) {
        LOG_WARN("failed to adjust value with time zone offset", K(ret));
      }
    int64_t days = cur_dt_value / USECS_PER_DAY;
    if (days < 0) {
      dt_value = (--days) * USECS_PER_DAY + t_value;
    } else {
      dt_value = days * USECS_PER_DAY + t_value;
    }
    if (OB_FAIL(sub_timezone_offset(tz_info, dt_value))) {
        LOG_WARN("failed to adjust value with time zone offset", K(ret));
      }
  } else {
    if (OB_FAIL(add_timezone_offset(tz_info, cur_dt_value))) {
      LOG_WARN("failed to adjust value with time zone offset", K(ret));
    } else {
      int64_t days = cur_dt_value / USECS_PER_DAY;
      if (days < 0) {
        dt_value = (--days) * USECS_PER_DAY + t_value;
      } else {
        dt_value = days * USECS_PER_DAY + t_value;
      }
    }
  }
  return ret;
}

int ObTimeConverter::time_to_str(int64_t value, int16_t scale,
                                 char *buf, int64_t buf_len, int64_t &pos, bool with_delim)
{
  int ret = OB_SUCCESS;
  ObTime ob_time;
  round_datetime(scale, value);
  if (OB_FAIL(time_to_ob_time(value, ob_time))) {
    LOG_WARN("failed to convert seconds to ob time", K(ret));
  } else if (OB_FAIL(ob_time_to_str(ob_time, DT_TYPE_TIME, scale, buf, buf_len, pos, with_delim))) {
    LOG_WARN("failed to convert ob time to string", K(ret));
  }
  return ret;
}

int ObTimeConverter::year_to_int(uint8_t value, int64_t &int64)
{
  int64 = (ZERO_YEAR == value) ? 0 : value + YEAR_BASE_YEAR;
  return OB_SUCCESS;
}

#define YEAR_STR_FMT  "%.4d"
#define YEAR_STR_LEN  4

int ObTimeConverter::year_to_str(uint8_t value, char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0 || pos < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer is invalid", K(ret), K(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, YEAR_STR_FMT, (value > 0) ? value + YEAR_BASE_YEAR : 0))) {
    LOG_WARN("failed to print year str", K(ret));
  }
  return ret;
}

////////////////////////////////
// inner cast between datetime, timestamp, date, time, year.

int ObTimeConverter::datetime_to_timestamp(int64_t dt_value, const ObTimeZoneInfo *tz_info, int64_t &ts_value)
{
  int ret = OB_SUCCESS;
  ts_value = dt_value;
  if (OB_FAIL(sub_timezone_offset(tz_info, ts_value))) {
    LOG_WARN("failed to adjust value with time zone offset", K(ret));
  }
  return ret;
}

int ObTimeConverter::timestamp_to_datetime(int64_t ts_value, const ObTimeZoneInfo *tz_info, int64_t &dt_value)
{
  int ret = OB_SUCCESS;
  dt_value = ts_value;
  if (OB_FAIL(add_timezone_offset(tz_info, dt_value))) {
    LOG_WARN("failed to adjust value with time zone offset", K(ret));
  }
  return ret;
}

int ObTimeConverter::datetime_to_date(int64_t dt_value, const ObTimeZoneInfo *tz_info, int32_t &d_value)
{
  int ret = OB_SUCCESS;
  if (ZERO_DATETIME == dt_value) {
    d_value = ZERO_DATE;
  } else if (OB_FAIL(add_timezone_offset(tz_info, dt_value))) {
    LOG_WARN("failed to adjust value with time zone offset", K(ret));
  } else {
    d_value = static_cast<int32_t>(dt_value / USECS_PER_DAY);
    if ((dt_value % USECS_PER_DAY) < 0) {
      --d_value;
    }
  }
  return ret;
}

int ObTimeConverter::datetime_to_time(int64_t dt_value, const ObTimeZoneInfo *tz_info, int64_t &t_value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(add_timezone_offset(tz_info, dt_value))) {
    LOG_WARN("failed to adjust value with time zone offset", K(ret));
  } else {
    t_value = dt_value % USECS_PER_DAY;
    if (t_value < 0) {
      t_value += USECS_PER_DAY;
    }
  }
  return ret;
}

int ObTimeConverter::datetime_to_year(int64_t dt_value, const ObTimeZoneInfo *tz_info, uint8_t &y_value)
{
  int ret = OB_SUCCESS;
  ObTime ob_time;
  if (ZERO_DATETIME == dt_value) {
    y_value = ZERO_YEAR;
  } else if (OB_FAIL(datetime_to_ob_time(dt_value, tz_info, ob_time))) {
    LOG_WARN("failed to convert datetime to ob time", K(ret));
  } else if (OB_FAIL(validate_year(ob_time.parts_[DT_YEAR]))) {
    LOG_WARN("year integer is invalid or out of range", K(ret));
  } else {
    y_value = static_cast<uint8_t>(ob_time.parts_[DT_YEAR] - YEAR_BASE_YEAR);
  }
  return ret;
}

int ObTimeConverter::date_to_datetime(int32_t d_value, const ObTimeZoneInfo *tz_info, int64_t &dt_value)
{
  int ret = OB_SUCCESS;
  ObTime ob_time;
  if (ZERO_DATE == d_value) {
    dt_value = ZERO_DATETIME;
  } else if (OB_FAIL(date_to_ob_time(d_value, ob_time))) {
    LOG_WARN("failed to convert date to ob time", K(ret));
  } else if (OB_FAIL(ob_time_to_datetime(ob_time, tz_info, dt_value))) {
    LOG_WARN("failed to convert ob time to datetime", K(ret));
  }
  return ret;
}

int ObTimeConverter::date_to_year(int32_t d_value, uint8_t &y_value)
{
  int ret = OB_SUCCESS;
  ObTime ob_time;
  if (ZERO_DATE == d_value) {
    y_value = ZERO_YEAR;
  } else if (OB_FAIL(date_to_ob_time(d_value, ob_time))) {
    LOG_WARN("failed to convert date to ob time", K(ret));
  } else if (OB_FAIL(validate_year(ob_time.parts_[DT_YEAR]))) {
    LOG_WARN("year integer is invalid or out of range", K(ret));
  } else {
    y_value = static_cast<uint8_t>(ob_time.parts_[DT_YEAR] - YEAR_BASE_YEAR);
  }
  return ret;
}

////////////////////////////////
// string -> offset.

#define OFFSET_MIN  static_cast<int32_t>(-(12 * MINS_PER_HOUR + 59) * SECS_PER_MIN)   // -12:59 .
#define OFFSET_MAX  static_cast<int32_t>(13 * SECS_PER_HOUR)                          // +13:00 .
int ObTimeConverter::str_to_offset(const ObString &str, int32_t &value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str.ptr()) || OB_UNLIKELY(str.length() <= 0)) {
    ret = OB_ERR_UNKNOWN_TIME_ZONE;
    LOG_WARN("invalid time zone offset", K(ret), K(str));
  } else {
    const char *pos = str.ptr();
    const char *end = pos + str.length();
    char sign = *pos++;
    ObTimeDigits hour;
    ObTimeDigits minute;
    ObTimeDelims colon;
    ObTimeDelims none;
    if (OB_FAIL(get_datetime_digits_delims(pos, end, INT32_MAX, hour, colon))) {
      LOG_WARN("failed to get offset", K(ret), K(str));
    } else if (OB_FAIL(get_datetime_digits_delims(pos, end, INT32_MAX, minute, none))) {
      LOG_WARN("failed to get offset", K(ret), K(str));
    }
    if (!('+' == sign || '-' == sign)
        || 0 == hour.len_ || 0 == minute.len_
        || !is_single_colon(colon) || none.len_ > 0) {
      ret = OB_ERR_UNKNOWN_TIME_ZONE;
      LOG_WARN("invalid time zone offset", K(ret), K(str));
    } else {
      value = ((hour.value_ * MINS_PER_HOUR) + minute.value_) * SECS_PER_MIN;
      if ('-' == sign) {
        value = -value;
      }
      if (!(OFFSET_MIN <= value && value <= OFFSET_MAX)) {
        ret = OB_ERR_UNKNOWN_TIME_ZONE;
        LOG_WARN("invalid time zone offset", K(ret), K(str));
      }
    }
  }
  return ret;
}

////////////////////////////////
// year / month / day / quarter / week / hour / minite / second / microsecond.

int ObTimeConverter::int_to_week(int64_t int64, int64_t mode, int32_t &value)
{
  int ret = OB_SUCCESS;
  ObTime ob_time;
  if (OB_FAIL(int_to_ob_time_with_date(int64, ob_time))) {
    LOG_WARN("failed to convert integer to datetime", K(ret));
  } else {
    value = ob_time_to_week(ob_time, WEEK_MODE[mode % WEEK_MODE_CNT]);
  }
  return ret;
}

////////////////////////////////
// other functions.

void ObTimeConverter::round_datetime(int16_t scale, int64_t &value)
{
  if (0 <= scale && scale < 6) {
    int32_t power_of_precision = power_of_10[6 - scale];
    int32_t usec = static_cast<int32_t>(value % power_of_precision);
    int32_t ceil_diff = (usec < 0) ? (-usec) : (power_of_precision - usec);
    if (ceil_diff <= power_of_precision / 2) {
      value += ceil_diff;
    } else {
      value -= (power_of_precision - ceil_diff);
    }
  } //others, just return the original value.
}

void ObTimeConverter::trunc_datetime(int16_t scale, int64_t &value)
{
  if (0 <= scale && scale <= 6) {
    int32_t power_of_precision = power_of_10[6 - scale];
    int32_t usec = static_cast<int32_t>(value % power_of_precision);
    value -= usec;
  } //others, just return the original value.
  //todo: test the date before 1970
}

////////////////////////////////
// date add / sub / diff.

int ObTimeConverter::date_adjust(const int64_t base_value, const ObString &interval_str,
                                 ObDateUnitType unit_type, int64_t &value, bool is_add)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(unit_type < 0 || unit_type >= DATE_UNIT_MAX)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit type is invalid", K(ret), K(unit_type));
  } else if (INTERVAL_INDEX[unit_type].calc_with_usecond_) {
    if (OB_FAIL(merge_date_interval(base_value, interval_str, unit_type, value, is_add))) {
      LOG_WARN("failed to merge date and interval", K(ret));
    }
  } else {
    ObTime base_time;
    if (OB_FAIL(datetime_to_ob_time(base_value, NULL, base_time))) {
      LOG_WARN("failed to convert datetime to ob time", K(ret));
    } else if (OB_FAIL(merge_date_interval(base_time, interval_str, unit_type, value, is_add))) {
      LOG_WARN("failed to merge date and interval", K(ret));
    }
  }
  return ret;
}

int ObTimeConverter::date_adjust(const ObString &base_str, const ObString &interval_str,
                                 ObDateUnitType unit_type, int64_t &value, bool is_add)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(unit_type < 0 || unit_type >= DATE_UNIT_MAX)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit type is invalid", K(ret), K(unit_type));
  } else if (INTERVAL_INDEX[unit_type].calc_with_usecond_) {
    int64_t base_value = 0;
    if (OB_FAIL(str_to_datetime(base_str, NULL, base_value))) {
      LOG_WARN("failed to convert string to datetime", K(ret));
    } else if (OB_FAIL(merge_date_interval(base_value, interval_str, unit_type, value, is_add))) {
      LOG_WARN("failed to merge date and interval", K(ret));
    }
  } else {
    ObTime base_time;
    if (OB_FAIL(str_to_ob_time_with_date(base_str, base_time))) {
      LOG_WARN("failed to convert string to ob time", K(ret));
    } else if (OB_FAIL(merge_date_interval(base_time, interval_str, unit_type, value, is_add))) {
      LOG_WARN("failed to merge date and interval", K(ret));
    }
  }
  return ret;
}

int ObTimeConverter::merge_date_interval(int64_t base_value, const ObString &interval_str,
                                         ObDateUnitType unit_type, int64_t &value, bool is_add)
{
  int ret = OB_SUCCESS;
  int64_t interval_value = 0;
  if (OB_FAIL(str_to_interval(interval_str, unit_type, interval_value))) {
    LOG_WARN("failed to convert string to interval", K(ret));
  } else {
    value = base_value + (is_add ? interval_value : -interval_value);
  }
  return ret;
}

int ObTimeConverter::merge_date_interval(/*const*/ ObTime &base_time, const ObString &interval_str,
                                         ObDateUnitType unit_type, int64_t &value, bool is_add)
{
  int ret = OB_SUCCESS;
  ObInterval interval_time;
  if (OB_FAIL(str_to_ob_interval(interval_str, unit_type, interval_time))) {
    LOG_WARN("failed to convert string to ob interval", K(ret));
  } else if (INTERVAL_INDEX[unit_type].end_ > DT_MON) {
    // we use this function only when can't convert ob_interval to useconds exactly,
    // so unit must be year / quarter / month / year_month.
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit type is invalid", K(ret), K(unit_type));
  } else {
    int64_t year = 0;
    int64_t month = 0;
    if (is_add == static_cast<bool>(DT_MODE_NEG & interval_time.mode_)) {
      year = static_cast<int64_t>(base_time.parts_[DT_YEAR]) - interval_time.parts_[DT_YEAR];
      month = static_cast<int64_t>(base_time.parts_[DT_MON]) - interval_time.parts_[DT_MON];
    } else {
      year = static_cast<int64_t>(base_time.parts_[DT_YEAR]) + interval_time.parts_[DT_YEAR];
      month = static_cast<int64_t>(base_time.parts_[DT_MON]) + interval_time.parts_[DT_MON];
    }
    if (DT_MON == INTERVAL_INDEX[unit_type].end_) {
      if (month <= 0) {
        // 0 => 12, -1 => 11 ... -11 => 1, -12 => 12, -13 => 11 ... -23 => 1.
        // | borrow = 1                  | borrow = 2                       |
        int32_t borrow = static_cast<int32_t>(-month / MONS_PER_YEAR) + 1;
        year -= borrow;
        month += (MONS_PER_YEAR * borrow);
      } else if (month > MONS_PER_YEAR) {
        // 13 => 1, 14 => 2 ... 24 => 12, 25 => 1, 26 => 2 ... 36 => 12.
        // | carry = 1                  | carry = 2                    |
        int32_t carry = static_cast<int32_t>(month - 1) / MONS_PER_YEAR;
        year += carry;
        month -= (MONS_PER_YEAR * carry);
      }
    }
    ObTime res_time;
    res_time.parts_[DT_YEAR] = static_cast<int32_t>(year);
    res_time.parts_[DT_MON] = static_cast<int32_t>(month);
    res_time.parts_[DT_MDAY] = base_time.parts_[DT_MDAY];
    res_time.parts_[DT_HOUR] = base_time.parts_[DT_HOUR];
    res_time.parts_[DT_MIN] = base_time.parts_[DT_MIN];
    res_time.parts_[DT_SEC] = base_time.parts_[DT_SEC];
    res_time.parts_[DT_USEC] = base_time.parts_[DT_USEC];
    // date_add('2000-2-29', interval '1' year) => 2001-02-28.
    int32_t days = DAYS_PER_MON[IS_LEAP_YEAR(year)][month];
    if (res_time.parts_[DT_MDAY] > days) {
      res_time.parts_[DT_MDAY] = days;
    }
    res_time.parts_[DT_DATE] = ob_time_to_date(res_time);
    if (OB_FAIL(validate_datetime(res_time))) {
      LOG_WARN("invalid datetime", K(ret));
    } else if (OB_FAIL(ob_time_to_datetime(res_time, NULL, value))) {
      LOG_WARN("failed to convert ob time to datetime", K(ret));
    }
  }
  return ret;
}

////////////////////////////////
// int / uint / string -> ObTime / ObInterval <- datetime / date / time.

int ObTimeConverter::int_to_ob_time_with_date(int64_t int64, ObTime &ob_time)
{
  int ret = OB_SUCCESS;
  int32_t *parts = ob_time.parts_;
  if (int64 < power_of_10[2]) {
    ret = OB_INVALID_DATE_VALUE;
    LOG_WARN("datetime integer is out of range", K(ret), K(int64));
  } else if (int64 < power_of_10[8]) {
    // YYYYMMDD.
    parts[DT_MDAY]  = static_cast<int32_t>(int64 % power_of_10[2]); int64 /= power_of_10[2];
    parts[DT_MON]  = static_cast<int32_t>(int64 % power_of_10[2]); int64 /= power_of_10[2];
    parts[DT_YEAR] = static_cast<int32_t>(int64 % power_of_10[4]);
  } else if (int64 / power_of_10[6] < power_of_10[8]) {
    // YYYYMMDDHHMMSS.
    parts[DT_SEC]  = static_cast<int32_t>(int64 % power_of_10[2]); int64 /= power_of_10[2];
    parts[DT_MIN]  = static_cast<int32_t>(int64 % power_of_10[2]); int64 /= power_of_10[2];
    parts[DT_HOUR] = static_cast<int32_t>(int64 % power_of_10[2]); int64 /= power_of_10[2];
    parts[DT_MDAY] = static_cast<int32_t>(int64 % power_of_10[2]); int64 /= power_of_10[2];
    parts[DT_MON]  = static_cast<int32_t>(int64 % power_of_10[2]); int64 /= power_of_10[2];
    parts[DT_YEAR] = static_cast<int32_t>(int64 % power_of_10[4]);
  } else {
    ret = OB_INVALID_DATE_VALUE;
    LOG_WARN("datetime integer is out of range", K(ret), K(int64));
  }
  if (OB_SUCC(ret)) {
    apply_date_year2_rule(parts[0]);
    if (OB_FAIL(validate_datetime(ob_time))) {
      LOG_WARN("datetime is invalid or out of range", K(ret), K(int64));
    } else if (ZERO_DATE != parts[DT_DATE]) {
      parts[DT_DATE] = ob_time_to_date(ob_time);
    }
  }
  return ret;
}

int ObTimeConverter::int_to_ob_time_without_date(int64_t int64, ObTime &ob_time)
{
  int ret = OB_SUCCESS;
  int32_t *parts = ob_time.parts_;
  ObDTMode mode = DT_TYPE_TIME;
  if (int64 < 0) {
    ob_time.mode_ |= DT_MODE_NEG;
    int64 = -int64;
  }
  if (int64 / power_of_10[4] < power_of_10[6]) {
    // [H]HHMMSS, like 123:45:56.
    parts[DT_SEC]  = static_cast<int32_t>(int64 % power_of_10[2]); int64 /= power_of_10[2];
    parts[DT_MIN]  = static_cast<int32_t>(int64 % power_of_10[2]); int64 /= power_of_10[2];
    parts[DT_HOUR] = static_cast<int32_t>(int64 % power_of_10[6]);
    if (OB_FAIL(validate_time(ob_time))) {
      LOG_WARN("time integer is invalid", K(ret), K(int64));
    }
  } else if (int64 / power_of_10[6] < power_of_10[8]) {
    // HHHHMMDDHHMMSS.
    mode = DT_TYPE_DATETIME;
    parts[DT_SEC]  = static_cast<int32_t>(int64 % power_of_10[2]); int64 /= power_of_10[2];
    parts[DT_MIN]  = static_cast<int32_t>(int64 % power_of_10[2]); int64 /= power_of_10[2];
    parts[DT_HOUR] = static_cast<int32_t>(int64 % power_of_10[2]); int64 /= power_of_10[2];
    parts[DT_MDAY]  = static_cast<int32_t>(int64 % power_of_10[2]); int64 /= power_of_10[2];
    parts[DT_MON]  = static_cast<int32_t>(int64 % power_of_10[2]); int64 /= power_of_10[2];
    parts[DT_YEAR] = static_cast<int32_t>(int64 % power_of_10[4]);
    apply_date_year2_rule(parts[DT_YEAR]);
    if (OB_FAIL(validate_datetime(ob_time))) {
      LOG_WARN("datetime is invalid or out of range", K(ret), K(int64));
    } else if (ZERO_DATE != parts[DT_DATE]) {
      parts[DT_DATE] = ob_time_to_date(ob_time);
    }
  } else {
    parts[DT_HOUR] = TIME_MAX_HOUR + 1;
  }
  UNUSED(mode);
  return ret;
}

// these 2 functions are in .h file.
//int ObTimeConverter::uint_to_ob_time_with_date(int64_t int64, ObTime &ob_time)
//int ObTimeConverter::uint_to_ob_time_without_date(int64_t int64, ObTime &ob_time)

static const int32_t DATETIME_PART_LENS_MAX[] = {
  INT32_MAX, INT32_MAX, INT32_MAX, INT32_MAX, INT32_MAX, INT32_MAX, INT32_MAX, 7};
static const int32_t DATETIME_PART_LENS_YEAR4[] = {4, 2, 2, 2, 2, 2, 7};
static const int32_t DATETIME_PART_LENS_YEAR2[] = {2, 2, 2, 2, 2, 2, 7};

int ObTimeConverter::str_to_digit_with_date(const ObString &str, ObTimeDigits *digits, ObTime &ob_time)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str.ptr()) || OB_UNLIKELY(str.length() <= 0) || OB_ISNULL(digits)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  } else {
    const char *pos = str.ptr();
    const char *end = pos + str.length();
    const int32_t *expect_lens = NULL;
    ObTimeDelims delims[DATETIME_PART_CNT];
    // find first digit and delimiter.
    for (; isspace(*pos) && pos < end; ++pos) {}
    if (!isdigit(*pos)) {
      ret = OB_INVALID_DATE_FORMAT;
    } else {
      const char *first_digit = pos;
      for (; isdigit(*pos) && pos < end; ++pos) {}
      const char *first_delim = pos;
       // year is separated by delimiter, or 4 digits, or 2?
      if ('.' != *first_delim && first_delim < end) {
        expect_lens = DATETIME_PART_LENS_MAX;
      } else {
        expect_lens = is_year4(first_delim - first_digit)
                      ? DATETIME_PART_LENS_YEAR4
                      : DATETIME_PART_LENS_YEAR2;
      }
       // parse datetime parts.
      pos = first_digit;
      for (int i = 0; OB_SUCC(ret) && i < DATETIME_PART_CNT && pos < end; ++i) {
        if (OB_FAIL(get_datetime_digits_delims(pos, end, expect_lens[i], digits[i], delims[i]))) {
          LOG_WARN("failed to get datetime digits from string");
        }
      }
       // apply all kinds of rules of mysql.
      if (OB_SUCC(ret)) {
        if (OB_FAIL(apply_date_space_rule(delims))) {
          LOG_WARN("invalid datetime string", K(ret), K(str));
        } else {
          if (0 == digits[DT_YEAR].value_ && 0 == digits[DT_MON].value_ && 0 == digits[DT_MDAY].value_) {
            // zero date, do nothing
          } else {
            apply_date_year2_rule(digits[DT_YEAR]);
          }
          if (OB_FAIL(apply_usecond_delim_rule(delims[DT_SEC], digits[DT_USEC]))) {
            LOG_WARN("failed to apply rule", K(ret));
          } else {
            if (!(DT_TYPE_DATE & ob_time.mode_) && (DT_TYPE_TIME & ob_time.mode_)) {
              if (OB_FAIL(apply_datetime_for_time_rule(ob_time, digits, delims))) {
                LOG_WARN("failed to apply rule", K(ret));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTimeConverter::str_to_ob_time_with_date(const ObString &str, ObTime &ob_time, int16_t *scale)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str.ptr()) || OB_UNLIKELY(str.length() <= 0)) {
    ret = OB_INVALID_DATE_VALUE;
    LOG_WARN("datetime string is invalid", K(ret), K(str));
  } else {
    ObTimeDigits digits[DATETIME_PART_CNT];
    if (OB_FAIL(str_to_digit_with_date(str, digits, ob_time))) {
      LOG_WARN("failed to get digits", K(ret), K(str));
    } else {
      // OK, it seems like a valid format, now we need check its value.
      for (int i = 0; i < DATETIME_PART_CNT; ++i) {
        ob_time.parts_[i] = digits[i].value_;
      }
      if (OB_FAIL(validate_datetime(ob_time))) {
        LOG_WARN("datetime is invalid or out of range", K(ret), K(str));
      } else if (ZERO_DATE != ob_time.parts_[DT_DATE]) {
        ob_time.parts_[DT_DATE] = ob_time_to_date(ob_time);
      }
      if (NULL != scale) {
        *scale = static_cast<int16_t>(MIN(digits[DT_USEC].len_, 6));
      }
    }
  }
  return ret;
}

int ObTimeConverter::str_is_date_format(const ObString &str, bool &date_flag)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str.ptr()) || OB_UNLIKELY(str.length() <= 0)) {
    ret = OB_INVALID_DATE_VALUE;
    LOG_WARN("datetime string is invalid", K(ret), K(str));
  } else {
    ObTimeDigits digits[DATETIME_PART_CNT];
    ObTime ob_time(DT_TYPE_DATE);
    if (OB_FAIL(str_to_digit_with_date(str, digits, ob_time))) {
      LOG_WARN("failed to get digits", K(ret), K(str));
    } else {
       // OK, it seems like a valid format, now we need check its value.
      if((MIN(digits[DT_HOUR].len_, 6)) > 0) {
        date_flag = false;
      } else {  // is date type
        date_flag = true;
      }
    }
  }
  return ret;
}

int ObTimeConverter::str_to_ob_time_without_date(const ObString &str, ObTime &ob_time, int16_t *scale)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str.ptr()) || OB_UNLIKELY(str.length() <= 0)) {
    ret = OB_INVALID_DATE_VALUE;
    LOG_WARN("time string is invalid", K(ret), K(str));
  } else {
    if (NULL != scale) {
      *scale = 0;
    }
    const char *pos = str.ptr();
    const char *end = pos + str.length();
    // find first digit.
    for (; isspace(*pos) && pos < end; ++pos) {}
    const char *first_digit = pos;
    if (!('-' == *pos || isdigit(*pos))){
      ret = OB_INVALID_DATE_VALUE;
      LOG_WARN("time string is invalid", K(ret), K(str));
    } else {
      if ('-' == *pos) {
        ++pos;
      }
      bool has_done = false;
      // maybe it is an whole datetime string.
      if (end - first_digit >= 12) {
        ret = str_to_ob_time_with_date(str, ob_time);
        if (DT_TYPE_NONE & ob_time.mode_ || OB_INVALID_DATE_FORMAT == ret) {
          ob_time.mode_ &= (~DT_TYPE_NONE);
          for (int i = 0; i < DATETIME_PART_CNT; ++i) {
            ob_time.parts_[i] = 0;
          }
          ret = OB_SUCCESS;
        } else if (OB_FAIL(ret)) {
          LOG_WARN("failed to convert string to datetime", K(ret));
        } else {
          has_done = true;
        }
      }
      // otherwise it is an time string.
      if (OB_SUCC(ret) && !has_done) {
        pos = str.ptr();
        if (is_negative(pos, end)) {
          ob_time.mode_ |= DT_MODE_NEG;
        }
        ObTimeDigits digits;
        ObTimeDelims delims;
        int32_t idx = -1;
        bool has_day = false;
        // first part, maybe day, or hour, or hour and minute and second.
        if (OB_FAIL(get_datetime_digits_delims(pos, end, INT32_MAX, digits, delims))) {
          LOG_WARN("failed to get digits and delims from datetime string", K(ret));
        } else if (is_all_spaces(delims)) {
          // digits are day.
          idx = DT_MDAY;
          ob_time.parts_[idx++] = digits.value_;
          has_day = true;
          // next part.
          if (OB_FAIL(get_datetime_digits_delims(pos, end, INT32_MAX, digits, delims))) {
            LOG_WARN("failed to get digits and delims from datetime string", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          bool end_with_single_colon = is_space_end_with_single_colon(delims);
          if (has_day || end_with_single_colon) {
            // digits are hour.
            // '11 12:34:56.789' => 276:34:56.789 .
            // '11 12 :34:56.789' => 276:00:00 .
            // '200 : 59 : 59' => 00:02:00 .
            // '200 :59 :59' => 200:59:00 .
            // '200: 59: 59' => 00:02:00 .
            // '200 ::59 :59' => 00:02:00 .
            idx = DT_HOUR;
            ob_time.parts_[idx++] = digits.value_;        // hour.
            // in any case, a single colon will be fine, if there is no day part,
            // spaces end with single colon will be fine too. see cases above.
            if (is_single_colon(delims) || (!has_day && end_with_single_colon)) {
              if (OB_FAIL(get_datetime_digits_delims(pos, end, INT32_MAX, digits, delims))) {
                LOG_WARN("failed to get digits and delims from datetime string", K(ret));
              } else {
                ob_time.parts_[idx++] = digits.value_;      // minute.
                if (is_single_colon(delims)) {
                  if (OB_FAIL(get_datetime_digits_delims(pos, end, INT32_MAX, digits, delims))) {
                    LOG_WARN("failed to get digits and delims from datetime string", K(ret));
                  } else {
                    ob_time.parts_[idx++] = digits.value_;  // second.
                  }
                }
              }
            }
          } else {
            // digits contain hour, minute, second.
            idx = DT_HOUR;
            ob_time.parts_[idx++] = digits.value_ / power_of_10[4];
            digits.value_ %= power_of_10[4];
            ob_time.parts_[idx++] = digits.value_ / power_of_10[2];
            digits.value_ %= power_of_10[2];
            ob_time.parts_[idx++] = digits.value_;
          }
          // usecond.
          if (OB_SUCC(ret) && is_single_dot(delims)) {
            // 7 is used for rounding to 6 digits.
            if (OB_FAIL(get_datetime_digits(pos, end, 7, digits))) {
              LOG_WARN("failed to get digits from datetime string", K(ret));
            } else if (OB_FAIL(normalize_usecond_round(digits))) {
              LOG_WARN("failed to round usecond", K(ret));
            } else {
              ob_time.parts_[DT_USEC] = digits.value_;    // usecond.
              if (NULL != scale) {
                *scale = static_cast<int16_t>(MIN(digits.len_, 6));
              }
            }
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(validate_time(ob_time))) {
          LOG_WARN("time value is invalid or out of range", K(ret), K(str));
        }
      }
    }
  }
  return ret;
}

int ObTimeConverter::str_to_ob_time_format(const ObString &str, const ObString &fmt, ObTime &ob_time, int16_t *scale)
{
  int ret = OB_SUCCESS;
//  bool is_minus = false;
  bool only_white_space = true;
  ObHourFlag hour_flag = HOUR_UNUSE;
  if (OB_ISNULL(str.ptr()) || OB_UNLIKELY(str.length() <= 0)) {
    // no error or warning even in strict mode.
    for (int i = 0; OB_SUCC(ret) && i < TOTAL_PART_CNT; ++i) {
      ob_time.parts_[i] = 0;
    }
    ob_time.parts_[DT_DATE] = ZERO_DATE;
  } else if (OB_ISNULL(fmt.ptr()) || fmt.length() <= 0) {
    ret = OB_INVALID_DATE_FORMAT;
    LOG_WARN("datetime format is invalid", K(ret), K(fmt));
  } else {
    const char *str_pos = str.ptr();
    const char *str_end = str.ptr() + str.length();
    const char *fmt_pos = fmt.ptr();
    const char *fmt_end = fmt.ptr() + fmt.length();
    ObString name;
    ObTimeDigits digits;
    ObTimeDelims delims;
    if (NULL != scale) {
      *scale = 0;
    }
    while (OB_SUCC(ret) && fmt_pos < fmt_end) {
      for (; isspace(*fmt_pos) && fmt_pos < fmt_end; ++fmt_pos);
      for (; isspace(*str_pos) && str_pos < str_end; ++str_pos);
      if (fmt_pos == fmt_end || str_pos == str_end) {
        break;
      }
      only_white_space = false;
      if ('%' == *fmt_pos) {
        fmt_pos++;
        if (fmt_pos >= fmt_end) {
          ret = OB_INVALID_DATE_VALUE;
          break;
        }
        switch (*fmt_pos) {
          case 'a': {
            name.assign_ptr(const_cast<char *>(str_pos), static_cast<int32_t>(str_end - str_pos));
            if (OB_SUCC(get_str_array_idx(name, WDAY_ABBR_NAMES, DAYS_PER_WEEK, ob_time.parts_[DT_WDAY]))) {
              str_pos += WDAY_ABBR_NAMES[ob_time.parts_[DT_WDAY]].len_;
            }
            break;
          }
          case 'b': {
            name.assign_ptr(const_cast<char *>(str_pos), static_cast<int32_t>(str_end - str_pos));
            if (OB_SUCC(get_str_array_idx(name, MON_ABBR_NAMES, MONS_PER_YEAR, ob_time.parts_[DT_MON]))) {
              str_pos += MON_ABBR_NAMES[ob_time.parts_[DT_MON]].len_;
            }
            break;
          }
          case 'c':
          case 'm': {
            if (OB_SUCC(get_datetime_digits(str_pos, str_end, 2, digits))) {
              ob_time.parts_[DT_MON] = digits.value_;
            }
            break;
          }
          case 'D': {
            name.assign_ptr(const_cast<char *>(str_pos), static_cast<int32_t>(str_end - str_pos));
            if (OB_SUCC(get_str_array_idx(name, MDAY_NAMES, 31, ob_time.parts_[DT_MDAY]))) {
              str_pos += MDAY_NAMES[ob_time.parts_[DT_MDAY]].len_;
            }
            break;
          }
          case 'd':
          case 'e': {
            if (OB_SUCC(get_datetime_digits(str_pos, str_end, 2, digits))) {
              ob_time.parts_[DT_MDAY] = digits.value_;
            }
            break;
          }
          case 'f': {
            if (str_pos == str_end || isdigit(*str_pos)) {
              if (OB_SUCC(get_datetime_digits(str_pos, str_end, 6, digits))
                  && OB_SUCC(normalize_usecond_trunc(digits, true))) {
                ob_time.parts_[DT_USEC] = digits.value_;
                if (NULL != scale) {
                  *scale = static_cast<int16_t>(MIN(digits.len_, 6));
                }
              }
            } else {
              ret = OB_INVALID_ARGUMENT;
            }
            break;
          }
          case 'H':
          case 'k':
          case 'h':
          case 'I':
          case 'l': {
            if (OB_SUCC(get_datetime_digits(str_pos, str_end, 2, digits))) {
              ob_time.parts_[DT_HOUR] = digits.value_;
            }
            break;
          }
          case 'i': {
            if (OB_SUCC(get_datetime_digits(str_pos, str_end, 2, digits))) {
              ob_time.parts_[DT_MIN] = digits.value_;
            }
            break;
          }
          case 'j': {
            if (OB_SUCC(get_datetime_digits(str_pos, str_end, 3, digits))) {
              ob_time.parts_[DT_YDAY] = digits.value_;
            }
            break;
          }
          case 'M': {
            name.assign_ptr(const_cast<char *>(str_pos), static_cast<int32_t>(str_end - str_pos));
            if (OB_SUCC(get_str_array_idx(name, MON_NAMES, MONS_PER_YEAR, ob_time.parts_[DT_MON]))) {
              str_pos += MON_NAMES[ob_time.parts_[DT_MON]].len_;
            }
            break;
          }
          case 'r':
          case 'T': {
            // HOUR, MINUTE
            for (int i = DT_HOUR; OB_SUCC(ret) && i <= DT_MIN; ++i) {
              if (OB_SUCC(get_datetime_digits_delims(str_pos, str_end, 2, digits, delims))) {
                if (!is_single_colon(delims)) {
                  ret = OB_INVALID_DATE_VALUE;
                } else {
                  ob_time.parts_[i] = digits.value_;
                }
              }
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(get_datetime_digits(str_pos, str_end, 2, digits))) {
                LOG_WARN("failed to get digits from datetime string");
              } else {
                ob_time.parts_[DT_SEC] = digits.value_;
              }
            }
            break;
          }
          case 'p': {
            if (HOUR_UNUSE != hour_flag || ob_time.parts_[DT_HOUR] > 12) {
              ret = OB_INVALID_DATE_VALUE;
            } else if (0 == strncasecmp(str_pos, "AM", strlen("AM"))) {
              hour_flag = HOUR_AM;
              str_pos += strlen("AM");
            } else if (0 == strncasecmp(str_pos, "PM", strlen("PM"))) {
              hour_flag = HOUR_PM;
              str_pos += strlen("PM");
            } else { //invalid date format
              ret = OB_INVALID_DATE_VALUE;
            }
            break;
          }
          case 'S':
          case 's': {
            if (OB_SUCC(get_datetime_digits(str_pos, str_end, 2, digits))) {
              ob_time.parts_[DT_SEC] = digits.value_;
            }
            break;
          }
          case 'U':
          case 'u':
          case 'V':
          case 'v':
          case 'W':
          case 'w':
          case 'X':
          case 'x': {
            ret = OB_NOT_SUPPORTED; //not use in str_to_date()
            break;
          }
          case 'Y': {
            if (OB_SUCC(get_datetime_digits(str_pos, str_end, 4, digits))) {
              ob_time.parts_[DT_YEAR] = digits.value_;
            }
            break;
          }
          case 'y': {
            if (OB_SUCC(get_datetime_digits(str_pos, str_end, 2, digits))) {
              apply_date_year2_rule(digits);
              ob_time.parts_[DT_YEAR] = digits.value_;
            }
            break;
          }
          case '%':
          default:
            ret = OB_NOT_SUPPORTED;
            break;
        }
        if (OB_SUCC(ret) && fmt_pos < fmt_end) {
          fmt_pos++;
        }
      } else if (*(fmt_pos++) != *(str_pos++)) {
        ret = OB_INVALID_DATE_VALUE;
        break;
      }
    }
    if (OB_SUCC(ret) && only_white_space) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("only white space in format argument", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (HOUR_AM == hour_flag && 12 == ob_time.parts_[DT_HOUR]) {
        ob_time.parts_[DT_HOUR] = 0;
      } else if (HOUR_PM == hour_flag && ob_time.parts_[DT_HOUR] > 0 && ob_time.parts_[DT_HOUR] < 12) {
        ob_time.parts_[DT_HOUR] += 12;
      }
      if (0 == ob_time.parts_[DT_MON] && 0 == ob_time.parts_[DT_MDAY] && 0 == ob_time.parts_[DT_YEAR]) {
        if (OB_FAIL(validate_time(ob_time))) {
          LOG_WARN("time value is invalid or out of range", K(ret), K(str));
        }
      } else {
        if (OB_FAIL(validate_datetime(ob_time))) {
          LOG_WARN("datetime is invalid or out of range", K(ret), K(str));
        } else if (ZERO_DATE != ob_time.parts_[DT_DATE]) {
          ob_time.parts_[DT_DATE] = ob_time_to_date(ob_time);
        }
      }
    }
  }
  return ret;
}

int ObTimeConverter::str_to_ob_interval(const ObString &str, ObDateUnitType unit_type, ObInterval &ob_interval)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str.ptr()) || OB_UNLIKELY(str.length() <= 0)) {
     for(int i = 0; OB_SUCC(ret) && i < TOTAL_PART_CNT ; i++) {
       if (OB_UNLIKELY(0 != ob_interval.parts_[i])) {
         ret = OB_ERR_UNEXPECTED;
         LIB_TIME_LOG(ERROR, "the part of ob_interval is 0", K(ret));
       }
     }
  } else {
    const char *pos = str.ptr();
    const char *end = pos + str.length();
    if (is_negative(pos, end)) {
      ob_interval.mode_ |= DT_MODE_NEG;
    }
    ObTimeDigits digits[DATETIME_PART_CNT + 1];
    ObTimeDelims delims[DATETIME_PART_CNT + 1];
    int32_t expect_cnt = INTERVAL_INDEX[unit_type].count_;
    int32_t i = 0;
    for (; OB_SUCC(ret) && i < expect_cnt + 1 && pos < end; ++i) {
      if (OB_FAIL(get_datetime_digits_delims(pos, end, INT32_MAX, digits[i], delims[i]))) {
        LOG_WARN("failed to get part value of interval", K(ret), K(str));
      }
    }
    if (OB_SUCC(ret)) {
      // date_add('2012-1-1', interval '1-2' hour) => 2012-1-1 01:00:00 .
      // date_add('2012-1-1', interval '1-2-3' hour) => 2012-1-1 01:00:00 .
      // date_add('2012-1-1', interval '1-2-3' day_hour) => NULL.
      // date_add('2012-1-1', interval '1:2.3' minute_second) => NULL.
      if (i == expect_cnt + 1 && digits[expect_cnt].len_ > 0 && expect_cnt > 1) {
        ret = OB_TOO_MANY_DATETIME_PARTS;
        LOG_WARN("interval has too many datetime parts", K(ret));
      } else {
        if (DATE_UNIT_WEEK == unit_type) {
          digits[0].value_ *= DAYS_PER_WEEK;
        } else if (DATE_UNIT_QUARTER == unit_type) {
          digits[0].value_ *= MONS_PER_QUAR;
        }
        // date_add('2012-1-1', interval '1' second_microsecond) => 2012-01-01 00:00:00.100000 .
        // date_add('2012-1-1', interval '1-2' hour) => 2012-1-1 01:00:00 .
        int32_t actual_cnt = i;
        int32_t idx_diff = actual_cnt < expect_cnt
                           ? INTERVAL_INDEX[unit_type].begin_ + expect_cnt - actual_cnt
                           : INTERVAL_INDEX[unit_type].begin_;
        int32_t assign_cnt = actual_cnt < expect_cnt ? actual_cnt : expect_cnt;
        for (i = 0; OB_SUCC(ret) && i < assign_cnt; ++i) {
          // date_add('2012-1-1', interval '1' microsecond) => 2012-01-01 00:00:00.000001 .
          // date_add('2012-1-1', interval '1.234' second_microsecond) => 2012-01-01 00:00:01.234000 .
          // date_add('2012-1-1', interval '1.2345678' second_microsecond) => 2012-01-01 00:00:03.345678 .
          // do not trunc or round.
          if (DT_USEC == i + idx_diff && expect_cnt > 1) {
            ret = normalize_usecond_trunc(digits[i], false);
          }
          ob_interval.parts_[i + idx_diff] = digits[i].value_;
        }
        // date_add('2012-1-1', interval '1.2.3' second) => 2012-01-01 00:00:01.200000 .
        // date_add('2012-1-1', interval '1:2.3' second) => 2012-01-01 00:00:01 .
        // date_add('2012-1-1', interval '1 .2' second) => 2012-01-01 00:00:01 .
        // date_add('2012-1-1', interval '1. 2' second) => 2012-01-01 00:00:01 .
        // date_add('2012-1-1', interval '1..2' second) => 2012-01-01 00:00:01 .
        // date_add('2012-1-1', interval '1.2345678' second) => 2012-01-01 00:00:01.234567 .
        // trunc, don't round.
        if (DATE_UNIT_SECOND == unit_type && actual_cnt > 1 && is_single_dot(delims[0])) {
          ret = normalize_usecond_trunc(digits[1], true);
          ob_interval.parts_[DT_USEC] = digits[1].value_;
        }
      }
    }
  }
  return ret;
}
int ObTimeConverter::datetime_to_ob_time(int64_t value, const ObTimeZoneInfo *tz_info, ObTime &ob_time)
{
  int ret = OB_SUCCESS;
  int64_t usec = value;
  if (OB_UNLIKELY(ZERO_DATETIME == usec)) {
    MEMSET(ob_time.parts_, 0, sizeof(*ob_time.parts_) * TOTAL_PART_CNT);
    ob_time.parts_[DT_DATE] = ZERO_DATE;
  } else if (OB_FAIL(add_timezone_offset(tz_info, usec))) {
    LOG_WARN("failed to adjust value with time zone offset", K(ret));
  } else {
    int32_t days = static_cast<int32_t>(usec / USECS_PER_DAY);
    usec %= USECS_PER_DAY;
    if (OB_UNLIKELY(usec < 0)) {
      --days;
      usec += USECS_PER_DAY;
    }
    if (OB_FAIL(date_to_ob_time(days, ob_time))) {
      LOG_WARN("failed to convert date part to obtime", K(ret), K(usec));
    } else if (OB_FAIL(time_to_ob_time(usec, ob_time))) {
      LOG_WARN("failed to convert time part to obtime", K(ret), K(usec));
    }
  }
  return ret;
}

int ObTimeConverter::date_to_ob_time(int32_t value, ObTime &ob_time)
{
  int ret = OB_SUCCESS;
  int32_t *parts = ob_time.parts_;
  if (OB_UNLIKELY(ZERO_DATE == value)) {
    memset(parts, 0, sizeof(*parts) * DATETIME_PART_CNT);
    parts[DT_DATE] = ZERO_DATE;
  } else {
    int32_t days = value;
    int32_t leap_year = 0;
    int32_t year = EPOCH_YEAR4;
    parts[DT_DATE] = value;
    // year.
    while (days < 0 || days >= DAYS_PER_YEAR[leap_year = IS_LEAP_YEAR(year)]) {
      int32_t new_year = year + days / DAYS_PER_NYEAR;
      new_year -= (days < 0);
      days -= (new_year - year) * DAYS_PER_NYEAR + LEAP_YEAR_COUNT(new_year - 1) - LEAP_YEAR_COUNT(year - 1);
      year = new_year;
    }
    parts[DT_YEAR] = year;
    parts[DT_YDAY] = days + 1;
    parts[DT_WDAY] = WDAY_OFFSET[value % DAYS_PER_WEEK][EPOCH_WDAY];
    // month.
    const int32_t *cur_days_until_mon = DAYS_UNTIL_MON[leap_year];
    int32_t month = 1;
    for (; month < MONS_PER_YEAR && days >= cur_days_until_mon[month]; ++month) {}
    parts[DT_MON] = month;
    days -= cur_days_until_mon[month - 1];
    // day.
    parts[DT_MDAY] = days + 1;
  }
  return ret;
}

int ObTimeConverter::time_to_ob_time(int64_t value, ObTime &ob_time)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(value < 0)) {
    ob_time.mode_ |= DT_MODE_NEG;
    value = -value;
  }
  int64_t secs = USEC_TO_SEC(value);
  ob_time.parts_[DT_HOUR] = static_cast<int32_t>(secs / SECS_PER_HOUR);
  secs %= SECS_PER_HOUR;
  ob_time.parts_[DT_MIN] = static_cast<int32_t>(secs / SECS_PER_MIN);
  ob_time.parts_[DT_SEC] = static_cast<int32_t>(secs % SECS_PER_MIN);
  ob_time.parts_[DT_USEC] = static_cast<int32_t>(value % USECS_PER_SEC);
  return ret;
}

////////////////////////////////
// int / uint / string <- ObTime -> datetime / date / time.

#define DTAE_DIGIT_LEN  8
#define TIME_DIGIT_LEN  6

OB_INLINE int64_t ObTimeConverter::ob_time_to_int(const ObTime &ob_time, ObDTMode mode)
{
  int64_t value = 0;
  if (DT_TYPE_DATE & mode) {
    value = ob_time.parts_[DT_YEAR] * static_cast<int64_t>(power_of_10[4])
            + ob_time.parts_[DT_MON] * static_cast<int64_t>(power_of_10[2])
            + ob_time.parts_[DT_MDAY];
  }
  if (DT_TYPE_TIME & mode) {
    value *= static_cast<int64_t>(power_of_10[6]);
    value += (ob_time.parts_[DT_HOUR] * static_cast<int64_t>(power_of_10[4])
              + ob_time.parts_[DT_MIN] * static_cast<int64_t>(power_of_10[2])
              + ob_time.parts_[DT_SEC]);
  }
  return value;
}

static const int32_t DT_PART_MUL[DATETIME_PART_CNT] = {100, 100, 100, 100, 100, 1000000, 1};
static const int64_t ZERO_DATE_WEEK = 613566757;    // for 0000-00-00 00:00:00 .
int64_t ObTimeConverter::ob_time_to_int_extract(const ObTime &ob_time, ObDateUnitType unit_type)
{
  int64_t value = 0;
  switch (unit_type) {
    case DATE_UNIT_WEEK: {
      value = ZERO_DATE == ob_time.parts_[DT_DATE]
              ? ZERO_DATE_WEEK
              : ObTimeConverter::ob_time_to_week(ob_time, DT_WEEK_SUN_BEGIN | DT_WEEK_ZERO_BEGIN);
      break;
    }
    case DATE_UNIT_QUARTER: {
      value = (ob_time.parts_[DT_MON] + 2) / 3;
      break;
    }
    default: {
      int32_t idx_begin = INTERVAL_INDEX[unit_type].begin_;
      int32_t idx_end = INTERVAL_INDEX[unit_type].end_;
      for (int32_t i = idx_begin; i < idx_end; ++i) {
        value += ob_time.parts_[i];
        value *= DT_PART_MUL[i];
      }
      value += ob_time.parts_[idx_end];
      if (DT_MODE_NEG & ob_time.mode_) {
        value = - value;
      }
      break;
    }
  }
  return value;
}

#define DATE_FMT_WITH_DELIM   "%04d-%02d-%02d"
#define DATE_FMT_NO_DELIM     "%04d%02d%02d"
#define TIME_FMT_WITH_DELIM   "%02d:%02d:%02d"
#define TIME_FMT_NO_DELIM     "%02d%02d%02d"
static const char *USEC_FMT[7] = {
  ".%d",
  ".%01d",
  ".%02d",
  ".%03d",
  ".%04d",
  ".%05d",
  ".%06d"
};

int ObTimeConverter::ob_time_to_str(const ObTime &ob_time, ObDTMode mode, int16_t scale,
                                    char *buf, int64_t buf_len, int64_t &pos, bool with_delim)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!(0 < mode && mode <= DT_TYPE_CNT && scale <= 6
                    && NULL != buf && buf_len > 0 && pos >= 0))) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const int32_t *parts = ob_time.parts_;
    const char *FMT = NULL;
    if (HAS_TYPE_DATE(mode)) {
      FMT = with_delim ? DATE_FMT_WITH_DELIM : DATE_FMT_NO_DELIM;
      ret = databuff_printf(buf, buf_len, pos, FMT, parts[DT_YEAR], parts[DT_MON], parts[DT_MDAY]);
    }
    if (OB_SUCC(ret)) {
      if (IS_TYPE_DATETIME(mode) && with_delim) {
        ret = databuff_printf(buf, buf_len, pos, " ");
      } else if (IS_TYPE_TIME(mode) && IS_NEG_TIME(ob_time.mode_)) {
        ret = databuff_printf(buf, buf_len, pos, "-");
      }
    }
    if (OB_SUCC(ret) && HAS_TYPE_TIME(mode)) {
      FMT = with_delim ? TIME_FMT_WITH_DELIM : TIME_FMT_NO_DELIM;
      ret = databuff_printf(buf, buf_len, pos, FMT, parts[DT_HOUR], parts[DT_MIN], parts[DT_SEC]);
      if (scale < 0 ) {
        scale = parts[DT_USEC] > 0 ? 6 : 0;
      }
      if (OB_SUCC(ret) && scale > 0) {
        ret = databuff_printf(buf, buf_len, pos, USEC_FMT[scale], parts[DT_USEC] / power_of_10[6 - scale]);
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to snprintf datetime string", K(ret));
  }
  return ret;
}

int ObTimeConverter::data_fmt_nd(char *buffer, int64_t buf_len, int64_t &pos, const int64_t n, int64_t target)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(n <= 0 || target < 0 || target > 999999)) {
    ret = OB_ERR_UNEXPECTED;
    LIB_TIME_LOG(ERROR, "invalid argument", K(ret), K(n), K(target));
  } else if (OB_UNLIKELY(n > buf_len - pos)) {
    ret = OB_SIZE_OVERFLOW;
    LIB_TIME_LOG(WARN, "no enough space for buffer", K(ret), K(n), K(buf_len), K(pos));
  } else {
    int64_t idx = pos + n - 1;
    int64_t i = 0;
    //BTW, when loop size is small, maybe, we can use loop unrolling to get better performance.
    while (i < n) {
      buffer[idx--] = static_cast<char>(target % 10 + '0');
      target /= 10;
      ++i;
    }
    pos += i;
  }
  return ret;
}

int ObTimeConverter::data_fmt_d(char *buffer, int64_t buf_len, int64_t &pos, int64_t target)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(target < 0 || target >= 100)) {
    ret = OB_ERR_UNEXPECTED;
    LIB_TIME_LOG(ERROR, "invalid argument", K(ret), K(target));
  } else {
    //buffer_size_need will be 1 or 2
    int64_t buffer_size_need = 1 + (target >= 10);
    if (OB_UNLIKELY(buffer_size_need > buf_len - pos)) {
      ret = OB_SIZE_OVERFLOW;
      LIB_TIME_LOG(WARN, "no enough space for buffer", K(ret), K(buffer_size_need), K(buf_len), K(pos));
    } else {
      int64_t idx = pos + buffer_size_need - 1;
      int64_t i = 0;
      //BTW, when loop size is small, maybe, we can use loop unrolling to get better performance.
      while (i < buffer_size_need) {
        buffer[idx--] = static_cast<char>(target % 10 + '0');
        target /= 10;
        ++i;
      }
      pos += i;
    }
  }
  return ret;
}

int ObTimeConverter::data_fmt_s(char *buffer, int64_t buf_len, int64_t &pos, const char *ptr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LIB_TIME_LOG(ERROR, "invalid argument", K(ret), K(ptr));
  } else {
    int64_t buffer_size_need = strlen(ptr);
    if (OB_UNLIKELY(buffer_size_need > buf_len - pos)) {
      ret = OB_SIZE_OVERFLOW;
      LIB_TIME_LOG(WARN, "no enough space for buffer", K(ret), K(buffer_size_need), K(buf_len), K(pos));
    } else {
      //will not copy the '\0' in ptr
      MEMCPY(buffer + pos, ptr, buffer_size_need);
      pos += buffer_size_need;
    }
  }
  return ret;
}

int ObTimeConverter::ob_time_to_str_format(const ObTime &ob_time, const ObString &format,
                                           char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(format.ptr()) || OB_ISNULL(buf) || OB_UNLIKELY(format.length() <= 0 || buf_len <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("format or output string is invalid", K(ret), K(format), K(buf), K(buf_len));
  } else {
    const char *format_ptr = format.ptr();
    const char *end_ptr = format.ptr() + format.length();
    const int32_t *parts = ob_time.parts_;
    int32_t week_sunday = -1;
    int32_t week_monday = -1;
    int32_t delta_sunday = -2;
    int32_t delta_monday = -2;
    //used for am/pm conversation in order to avoid if-else tests.
    const int hour_converter[24] = {12, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
                                    12, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
    while (format_ptr < end_ptr && OB_SUCCESS == ret) {
      if ('%' == *format_ptr) {
        format_ptr++;
        if (format_ptr >= end_ptr) {
          ret = OB_INVALID_ARGUMENT;
          break;
        }
        switch (*format_ptr) {
         /*The cases are not ordered alphabetically since Y y m d D are used frequently
         *in order to get better performance, we locate them in front of others
         */
          case 'Y': { //Year, numeric, four digits
            ret = data_fmt_nd(buf, buf_len, pos, 4, ob_time.parts_[DT_YEAR]);
            break;
          }
          case 'y': { //Year, numeric (two digits)
            int year = (ob_time.parts_[DT_YEAR]) % 100;
            ret = data_fmt_nd(buf, buf_len, pos, 2, year);
            break;
          }
          case 'M': { //Month name (January..December)
            ret = data_fmt_s(buf, buf_len, pos, MON_NAMES[parts[DT_MON]].ptr_);
            break;
          }
          case 'm': { //Month, numeric (00..12)
            ret = data_fmt_nd(buf, buf_len, pos, 2, parts[DT_MON]);
            break;
          }
          case 'D': { //Day of the month with English suffix (0th, 1st, 2nd, 3rd...)
            ret = data_fmt_s(buf, buf_len, pos, DAY_NAME[parts[DT_MDAY]]);
            break;
          }
          case 'd': { //Day of the month, numeric (00..31)
            ret = data_fmt_nd(buf, buf_len, pos, 2, parts[DT_MDAY]);
            break;
          }
          case 'a': { //Abbreviated weekday name (Sun..Sat)
            ret = data_fmt_s(buf, buf_len, pos, WDAY_ABBR_NAMES[parts[DT_WDAY]].ptr_);
            break;
          }
          case 'b': { //Abbreviated month name (Jan..Dec)
            ret = data_fmt_s(buf, buf_len, pos, MON_ABBR_NAMES[parts[DT_MON]].ptr_);
            break;
          }
          case 'c': { //Month, numeric (0..12)
            ret = data_fmt_d(buf, buf_len, pos, parts[DT_MON]);
            break;
          }
          case 'e': { //Day of the month, numeric (0..31)
            ret = data_fmt_d(buf, buf_len, pos, parts[DT_MDAY]);
            break;
          }
          case 'f': { //Microseconds (000000..999999)
            ret = data_fmt_nd(buf, buf_len, pos, 6, parts[DT_USEC]);
            break;
          }
          case 'H': { //Hour (00..23)
            ret = data_fmt_nd(buf, buf_len, pos, 2, parts[DT_HOUR]);
            break;
          }
          case 'h': //Hour (01..12)
          case 'I': { //Hour (01..12)
            int hour = hour_converter[parts[DT_HOUR]];
            ret = data_fmt_nd(buf, buf_len, pos, 2, hour);
            break;
          }
          case 'i': { //Minutes, numeric (00..59)
            ret = data_fmt_nd(buf, buf_len, pos, 2, parts[DT_MIN]);
            break;
          }
          case 'j': { //Day of year (001..366)
            ret = data_fmt_nd(buf, buf_len, pos, 3, parts[DT_YDAY]);
            break;
          }
          case 'k': { //Hour (0..23)
            ret = data_fmt_d(buf, buf_len, pos, parts[DT_HOUR]);
            break;
          }
          case 'l': { //Hour (1..12)
            int hour = hour_converter[parts[DT_HOUR]];
            ret = data_fmt_d(buf, buf_len, pos, hour);
            break;
          }
          case 'p': { //AM or PM
            const char *ptr = parts[DT_HOUR] < 12 ? "AM" : "PM";
            ret = data_fmt_s(buf, buf_len, pos, ptr);
            break;
          }
          case 'r': { //Time, 12-hour (hh:mm:ss followed by AM or PM)
            int hour = hour_converter[parts[DT_HOUR]];
            const char *ptr = parts[DT_HOUR] < 12 ? "AM" : "PM";
            ret = databuff_printf(buf, buf_len, pos, "%02d:%02d:%02d %s", hour, parts[DT_MIN], parts[DT_SEC], ptr);
            break;
          }
          case 'S': //Seconds (00..59)
          case 's': { //Seconds (00..59)
            ret = data_fmt_nd(buf, buf_len, pos, 2, parts[DT_SEC]);
            break;
          }
          case 'T': { //Time, 24-hour (hh:mm:ss)
            ret = databuff_printf(buf, buf_len, pos, "%02d:%02d:%02d", parts[DT_HOUR], parts[DT_MIN], parts[DT_SEC]);
            break;
          }
          case 'U': { //Week (00..53), where Sunday is the first day of the week
            ret = data_fmt_nd(buf, buf_len, pos, 2, ob_time_to_week(ob_time, WEEK_MODE[0]));
            break;
          }
          case 'u': { //Week (00..53), where Monday is the first day of the week
            ret = data_fmt_nd(buf, buf_len, pos, 2, ob_time_to_week(ob_time, WEEK_MODE[1]));
            break;
          }
          case 'V': { //Week (01..53), where Sunday is the first day of the week; used with %X
            //due to the face that V is often used with X.
            // In order to optimize the implementation, we set the right delta value which will possibly be used for %X case latter.
            // week_sunday != -1 means that its value has been computed in %X case ever.
            ret = data_fmt_nd(buf, buf_len, pos, 2, (-1 == week_sunday) ? ob_time_to_week(ob_time, WEEK_MODE[2], delta_sunday) : week_sunday);
            break;
          }
          case 'v': { //Week (01..53), where Monday is the first day of the week; used with %x
            ret = data_fmt_nd(buf, buf_len, pos, 2, (-1 == week_monday) ? ob_time_to_week(ob_time, WEEK_MODE[3], delta_monday) : week_monday);
            break;
          }
          case 'W': { //Weekday name (Sunday..Saturday)
            ret = data_fmt_s(buf, buf_len, pos, WDAY_NAMES[parts[DT_WDAY]].ptr_);
            break;
          }
          case 'w': { //Day of the week (0=Sunday..6=Saturday)
            ret = data_fmt_d(buf, buf_len, pos, parts[DT_WDAY] % DAYS_PER_WEEK);
            break;
          }
          case 'X': { //Year for the week where Sunday is the first day of the week, numeric, four digits; used with %V
            //due to the face that %X is often used with %V.
            // In order to optimize the implementation, we set the right week_sunday value which will possibly be used for %V case latter.
            if (-2 == delta_sunday) {
              week_sunday = ob_time_to_week(ob_time, WEEK_MODE[2], delta_sunday);
            }
            ret = data_fmt_nd(buf, buf_len, pos, 4, ob_time.parts_[DT_YEAR] + delta_sunday);
            break;
          }
          case 'x': { //Year for the week, where Monday is the first day of the week, numeric, four digits; used with %v
            if (-2 == delta_monday) {
              week_monday = ob_time_to_week(ob_time, WEEK_MODE[3], delta_monday);
            }
            ret = data_fmt_nd(buf, buf_len, pos, 4, ob_time.parts_[DT_YEAR] + delta_monday);
            break;
          }
          case '%': { //A literal "%" character
            if (pos >= buf_len) {
              ret = OB_SIZE_OVERFLOW;
              break;
            }
            buf[pos++] = '%';
            break;
          }
          default: {
            ret = OB_NOT_SUPPORTED;
            break;
          }
        }
        if (OB_SUCC(ret)) {
          format_ptr++;
        }
      } else if (pos >= buf_len) {
        ret = OB_SIZE_OVERFLOW;
        break;
      } else {
        buf[pos++] = *(format_ptr++);
      }
    }
  }
  return ret;
}

int ObTimeConverter::ob_time_to_datetime(ObTime &ob_time, const ObTimeZoneInfo *tz_info, int64_t &value)
{
  int ret = OB_SUCCESS;
  if (ZERO_DATE == ob_time.parts_[DT_DATE]) {
	  value = ZERO_DATETIME;
  } else {
    // there is no leap second, and the valid range of datetime and timestamp is not same as mysql.
    // so we don't handle leap second and shift things, delete all related codes.
    int64_t usec = ob_time.parts_[DT_DATE] * USECS_PER_DAY + ob_time_to_time(ob_time);
    if (OB_FAIL(sub_timezone_offset(tz_info, usec))) {
      LOG_WARN("failed to adjust value with time zone offset", K(ret));
    }
    value = usec;
  }
  return ret;
}

/*
 * +--------+--------+--------+--------+--------+
 * +  1968  |  1969  |  1970  |  1971  |  1972  +
 * +--------+--------+--------+--------+--------+
 * |<-D->|  |        |<-------A------->|<-B->|
 * |<-------C------->|
 *  A and C is the whole year, with A >= 0 and C < 0.
 *  B and D is the rest part (month and day), both >= 0.
 */

int32_t ObTimeConverter::ob_time_to_date(ObTime &ob_time)
{
  int32_t value = ZERO_DATE;
  if (ZERO_DATE == ob_time.parts_[DT_DATE]) {
    value = ZERO_DATE;
  } else {
    int32_t *parts = ob_time.parts_;
    parts[DT_YDAY] = DAYS_UNTIL_MON[IS_LEAP_YEAR(parts[DT_YEAR])][parts[DT_MON] - 1] + parts[DT_MDAY];
    int32_t days_of_years = (parts[DT_YEAR] - EPOCH_YEAR4) * DAYS_PER_NYEAR;
    int32_t leap_year_count = LEAP_YEAR_COUNT(parts[DT_YEAR] - 1) - LEAP_YEAR_COUNT(EPOCH_YEAR4 - 1);
    value = static_cast<int32_t>(days_of_years + leap_year_count + parts[DT_YDAY] - 1);
    parts[DT_WDAY] = WDAY_OFFSET[value % DAYS_PER_WEEK ][EPOCH_WDAY];
  }
  return value;
}

int64_t ObTimeConverter::ob_time_to_time(const ObTime &ob_time)
{
  return ((ob_time.parts_[DT_HOUR] * static_cast<int64_t>(MINS_PER_HOUR) + ob_time.parts_[DT_MIN])
         * SECS_PER_MIN + ob_time.parts_[DT_SEC]) * USECS_PER_SEC + ob_time.parts_[DT_USEC];
}

int ObTimeConverter::ob_interval_to_interval(const ObInterval &ob_interval, int64_t &value)
{
  int ret = OB_SUCCESS;
  if (ob_interval.parts_[DT_YEAR] > 0 || ob_interval.parts_[DT_MON] > 0) {
    ret = OB_INTERVAL_WITH_MONTH;
    LOG_WARN("Interval with year or month can't be converted to useconds", K(ret));
  } else {
    const int32_t *parts = ob_interval.parts_;
    value = 0;
    for (int32_t i = DT_MDAY; i <= DT_USEC; ++i) {
      value *= DT_PART_BASE[i];
      value += parts[i];
    }
    if (DT_MODE_NEG & ob_interval.mode_) {
      value = -value;
    }
  }
  return ret;
}

int32_t ObTimeConverter::ob_time_to_week(const ObTime &ob_time, ObDTMode mode)
{
  int32_t temp = 0; //trivial parameter.adapted for ob_time_to_week(const ObTime &, ObDTMode , int32_t)
  return ob_time_to_week(ob_time, mode, temp);
}

int32_t ObTimeConverter::ob_time_to_week(const ObTime &ob_time, ObDTMode mode, int32_t &delta)
{
  const int32_t *parts = ob_time.parts_;
  int32_t is_sun_begin = IS_SUN_BEGIN(mode);
  int32_t is_zero_begin = IS_ZERO_BEGIN(mode);
  int32_t is_ge_4_begin = IS_GE_4_BEGIN(mode);
  int32_t week = 0;
  if (parts[DT_YDAY] > DAYS_PER_NYEAR - 3 && is_ge_4_begin && !is_zero_begin) {
    // maybe the day is in week 1 of next year.
    int32_t days_cur_year = DAYS_PER_YEAR[IS_LEAP_YEAR(parts[DT_YEAR])];
    int32_t wday_next_yday1 = WDAY_OFFSET[days_cur_year - parts[DT_YDAY] + 1][parts[DT_WDAY]];
    int32_t yday_next_week1 = YDAY_WEEK1[wday_next_yday1][is_sun_begin][is_ge_4_begin];
    if (parts[DT_YDAY] >= days_cur_year + yday_next_week1) {
      week = 1;
      delta = 1;
    }
  }
  if (0 == week) {
    int32_t wday_cur_yday1 = WDAY_OFFSET[(1 - parts[DT_YDAY]) % DAYS_PER_WEEK][parts[DT_WDAY]];
    int32_t yday_cur_week1 = YDAY_WEEK1[wday_cur_yday1][is_sun_begin][is_ge_4_begin];
    if (parts[DT_YDAY] < yday_cur_week1 && !is_zero_begin) {
      // the day is in last week of prev year.
      int32_t days_prev_year = DAYS_PER_YEAR[IS_LEAP_YEAR(parts[DT_YEAR] - 1)];
      int32_t wday_prev_yday1 = WDAY_OFFSET[(1 - days_prev_year - parts[DT_YDAY]) % DAYS_PER_WEEK][parts[DT_WDAY]];
      int32_t yday_prev_week1 = YDAY_WEEK1[wday_prev_yday1][is_sun_begin][is_ge_4_begin];
      week = (days_prev_year + parts[DT_YDAY] - yday_prev_week1 + DAYS_PER_WEEK) / DAYS_PER_WEEK;
      delta = -1;
    } else {
      week = (parts[DT_YDAY] - yday_cur_week1 + DAYS_PER_WEEK) / DAYS_PER_WEEK;
      delta = 0;
    }
  }
  return week;
}
////////////////////////////////
// below are other utility functions:

int ObTimeConverter::validate_datetime(ObTime &ob_time)
{
  int32_t *parts = ob_time.parts_;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == parts[DT_MON] && 0 == parts[DT_MDAY])) {
    if (!(0 == parts[DT_YEAR] && 0 == parts[DT_HOUR] && 0 == parts[DT_MIN]
        && 0 == parts[DT_SEC] && 0 == parts[DT_USEC])) {
      ret = OB_INVALID_DATE_VALUE;
    } else {
      ob_time.parts_[DT_DATE] = ZERO_DATE;
    }
  } else {
    for (int i = 0; OB_SUCC(ret) && i < DATETIME_PART_CNT; ++i) {
      if (!(DT_PART_MIN[i] <= parts[i] && parts[i] <= DT_PART_MAX[i])) {
        ret = OB_INVALID_DATE_VALUE;
      }
    }
    if (OB_SUCC(ret)) {
      int is_leap = IS_LEAP_YEAR(parts[DT_YEAR]);
      if (parts[DT_MDAY] > DAYS_PER_MON[is_leap][parts[DT_MON]]) {
        ret = OB_INVALID_DATE_VALUE;
      }
    }
  }
  return ret;
}

int ObTimeConverter::validate_time(ObTime &ob_time)
{
  int ret = OB_SUCCESS;
  int32_t *parts = ob_time.parts_;
  if (parts[DT_MDAY] > 0) {
    if (parts[DT_MDAY] * static_cast<int64_t>(HOURS_PER_DAY) + parts[DT_HOUR] > UINT32_MAX) {
      parts[DT_HOUR] = TIME_MAX_HOUR + 1;
    } else {
      parts[DT_HOUR] += (parts[DT_MDAY] * HOURS_PER_DAY);
    }
    parts[DT_MDAY] = 0;
  }
  if (OB_SUCC(ret)) {
    if (parts[DT_MIN] > DT_PART_MAX[DT_MIN] || parts[DT_SEC] > DT_PART_MAX[DT_SEC]) {
      ret = OB_INVALID_DATE_VALUE;
    } else if (parts[DT_HOUR] > TIME_MAX_HOUR) {
      parts[DT_HOUR] = TIME_MAX_HOUR + 1;
    }
  }
  int32_t flag = parts[DT_HOUR] | parts[DT_MIN] | parts[DT_SEC];
  if (0 == flag) {
    ret = OB_SUCCESS; //all are zero...valid !
  }
  return ret;
}

OB_INLINE int ObTimeConverter::validate_year(int64_t year)
{
  int ret = OB_SUCCESS;
  if (0 != year && (year < YEAR_MIN_YEAR || year > YEAR_MAX_YEAR)) {
    ret = OB_DATA_OUT_OF_RANGE;
    LOG_WARN("year is invalid out of range", K(ret));
  }
  return ret;
}

int ObTimeConverter::time_overflow_trunc(int64_t &value)
{
  int ret = OB_SUCCESS;
  if (value > TIME_MAX_VAL) {
    // we need some ob error codes that map to ER_TRUNCATED_WRONG_VALUE,
    // so we get OB_INVALID_DATE_FORMAT / OB_INVALID_DATE_VALUE / OB_ERR_TRUNCATED_WRONG_VALUE.
    // another requirement comes from cast function in ob_obj_cast.cpp is this time object will be
    // set to TIME_MAX_VAL (838:59:59), rather than ZERO_VAL (00:00:00),
    // so we get the ONLY one: OB_ERR_TRUNCATED_WRONG_VALUE, because the other two will direct to
    // ZERO_VAL of the temporal types, like cast 'abc' or '1998-76-54' to date.
    ret = OB_ERR_TRUNCATED_WRONG_VALUE;
    value = TIME_MAX_VAL;
  }
  return ret;
}

// jiuren: DO NOT remove these functions!

//int ObTimeConverter::find_time_range(int64_t t, const int64_t *range_boundaries,
//                                     uint64_t higher_bound, uint64_t& result)
//{
//  int ret = OB_SUCCESS;
//  uint64_t i, lower_bound= 0;
//  /*
//    Function will work without this assertion but result would be meaningless.
//  */
//  if(!(higher_bound > 0 && t >= range_boundaries[0]))
//  {
//    _OB_LOG(ERROR, "Invalid higher_bound = %lu or t = %ld", higher_bound, t);
//    ret = OB_INVALID_ARGUMENT_FOR_USEC_TO_TIME;
//    result = -1;
//  } else {
//    /*
//    Do binary search for minimal interval which contain t. We preserve:
//    range_boundaries[lower_bound] <= t < range_boundaries[higher_bound]
//    invariant and decrease this higher_bound - lower_bound gap twice
//    times on each step.
//    */
//    while (higher_bound - lower_bound > 1)
//    {
//      i= (lower_bound + higher_bound) >> 1;
//      if (range_boundaries[i] <= t)
//        lower_bound= i;
//      else
//        higher_bound= i;
//    }
//    result = lower_bound;
//  }
//  return ret;
//}
//
//int ObTimeConverter::find_transition_type(int64_t t, const ObTimeZoneInfo *sp, TRAN_TYPE_INFO *&result)
//{
//  int ret = OB_SUCCESS;
//  if (OB_UNLIKELY(sp->timecnt == 0 || t < sp->ats[0])){
//    /*
//      If we have not any transitions or t is before first transition let
//      us use fallback time type.
//    */
//    result = sp->fallback_tti;
//  } else {
//    /*
//    Do binary search for minimal interval between transitions which
//    contain t. With this localtime_r on real data may takes less
//    time than with linear search (I've seen 30% speed up).
//    */
//    uint64_t index = 0;
//    if(OB_SUCCESS == find_time_range(t, sp->ats, sp->timecnt, index)){
//      result = &(sp->ttis[sp->types[index]]);
//    } else {
//      ret = OB_INVALID_ARGUMENT_FOR_USEC_TO_TIME;
//    }
//  }
//  return ret;
//}

int ObTimeConverter::get_datetime_digits(const char *&str, const char *end, int32_t max_len, ObTimeDigits &digits)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str) || OB_ISNULL(end) || OB_UNLIKELY(str > end || max_len <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("str or end or max_len is invalid", K(ret), K(str), K(end), K(max_len));
  } else {
    const char *pos = str;
    const char *digit_end = str + max_len < end ? str + max_len : end;
    int32_t value = 0;
    for (; OB_SUCC(ret) && pos < digit_end && isdigit(*pos); ++pos) {
      if (value * 10LL > INT32_MAX - (*pos - '0')) {
        ret = OB_OPERATE_OVERFLOW;
        LOG_WARN("datetime part value is out of range", K(ret));
      } else {
        value = value * 10 + *pos - '0';
      }
    }
    digits.ptr_ = str;
    digits.len_ = static_cast<int32_t>(pos - str);
    digits.value_ = value;
    str = pos;
  }
  return ret;
}

int ObTimeConverter::get_datetime_delims(const char *&str, const char *end, ObTimeDelims &delims)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str) || OB_ISNULL(end) || OB_UNLIKELY(str > end)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("str or end or max_len is invalid", K(ret), K(str), K(end));
  } else {
    const char *pos = str;
    for (; pos < end && !isdigit(*pos); ++pos) {}
    delims.ptr_ = str;
    delims.len_ = static_cast<int32_t>(pos - str);
    str = pos;
  }
  return ret;
}

OB_INLINE int ObTimeConverter::get_datetime_digits_delims(const char *&str, const char *end,
                                                          int32_t max_len, ObTimeDigits &digits,
                                                          ObTimeDelims &delims)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_datetime_digits(str, end, max_len, digits))) {
    LOG_WARN("failed to get digits from datetime string", K(ret));
  } else if (OB_FAIL(get_datetime_delims(str, end, delims))) {
    LOG_WARN("failed to get delims from datetime string", K(ret));
  }
  return ret;
}

OB_INLINE void ObTimeConverter::skip_delims(const char *&str, const char *end)
{
  // str < end best come before any other condition using *str, because end maybe points to memory
  // that can't be access, of course the chance for this situation is very very small.
  for (; str < end && !isdigit(*str); ++str) {}
}

OB_INLINE bool ObTimeConverter::is_year4(int64_t first_token_len)
{
  return (4 == first_token_len || 8 == first_token_len || first_token_len >= 14);
}

OB_INLINE bool ObTimeConverter::is_single_colon(const ObTimeDelims &delims)
{
  return (NULL != delims.ptr_ && ':' == *delims.ptr_ && 1 == delims.len_);
}

OB_INLINE bool ObTimeConverter::is_space_end_with_single_colon(const ObTimeDelims &delims)
{
  bool ret = false;
  if (NULL != delims.ptr_ && delims.len_ > 0) {
    const char *pos = delims.ptr_;
    const char *end = delims.ptr_ + delims.len_;
    for (; pos < end && isspace(*pos); ++pos) {}
    ret = (pos + 1 == end && ':' == *pos);
  }
  return ret;
}

OB_INLINE bool ObTimeConverter::is_single_dot(const ObTimeDelims &delims)
{
  return (1 == delims.len_ && NULL != delims.ptr_ && '.' == *delims.ptr_);
}

OB_INLINE bool ObTimeConverter::is_all_spaces(const ObTimeDelims &delims)
{
  bool ret = false;
  if (NULL != delims.ptr_ && delims.len_ > 0) {
    const char *pos = delims.ptr_;
    const char *end = delims.ptr_ + delims.len_;
    for (; pos < end && isspace(*pos); ++pos) {}
    ret = (pos == end);
  }
  return ret;
}

OB_INLINE bool ObTimeConverter::has_any_space(const ObTimeDelims &delims)
{
  bool ret = false;
  if (NULL != delims.ptr_ && delims.len_ > 0) {
    const char *pos = delims.ptr_;
    const char *end = delims.ptr_ + delims.len_;
    for (; pos < end && !isspace(*pos); ++pos) {}
    ret = (pos < end);
  }
  return ret;
}

OB_INLINE bool ObTimeConverter::is_negative(const char *&str, const char *end)
{
  // date_add('2015-12-31', interval ' +-@-=  1 - 2' day_hour) => 2016-01-01 02:00:00 .
  // date_add('2015-12-31', interval '  -@-=  1 - 2' day_hour) => 2015-12-29 22:00:00 .
  // date_add('2015-12-31', interval '  -@-=+ 1 - 2' day_hour) => 2015-12-29 22:00:00 .
  // it is negative only when minus sign appear before plus sign,
  // or there is minus sign but no plus sign.
  if (!OB_ISNULL(str)) {
    for (; str < end && !isdigit(*str) && '+' != *str && '-' != *str; ++str) {}
  }
  bool ret = (str < end) && ('-' == *str);
  skip_delims(str, end);
  return ret;
}

OB_INLINE int ObTimeConverter::normalize_usecond_round(ObTimeDigits &digits)
{
  int ret = OB_SUCCESS;
  if (digits.value_ < 0 || digits.len_ < 0 || digits.len_ > INT32_MAX_DIGITS_LEN) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("digtis is not invalid", K(ret), K(digits.value_), K(digits.len_));
  } else {
    if (digits.len_ < 6) {
      // .123 means 123000.
      digits.value_ *= power_of_10[6 - digits.len_];
    } else if (digits.len_ > 6) {
      // .1234567 will round to 123457.
      digits.value_ /= power_of_10[digits.len_ - 6];
      if (digits.ptr_[6] >= '5') {
        ++digits.value_;
      }
    }
  }
  return ret;
}

OB_INLINE int ObTimeConverter::normalize_usecond_trunc(ObTimeDigits &digits, bool need_trunc)
{
  int ret = OB_SUCCESS;
  if (digits.value_ < 0 || digits.len_ < 0 || digits.len_ > INT32_MAX_DIGITS_LEN) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("digtis is not invalid", K(ret), K(digits.value_), K(digits.len_));
  } else {
    if (digits.len_ < 6) {
      // .123 means 123000.
      digits.value_ *= power_of_10[6 - digits.len_];
    } else if (digits.len_ > 6 && need_trunc) {
      // .1234567 will trunc to 123456.
      digits.value_ /= power_of_10[digits.len_ - 6];
    }
  }
  return ret;
}

OB_INLINE int ObTimeConverter::apply_date_space_rule(const ObTimeDelims *delims)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(delims)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("delims is null", K(ret));
  } else if (has_any_space(delims[DT_YEAR]) || has_any_space(delims[DT_MON])
             || has_any_space(delims[DT_HOUR]) || has_any_space(delims[DT_MIN])
             || has_any_space(delims[DT_SEC])) {
    ret = OB_INVALID_DATE_FORMAT;
    LOG_WARN("invalid datetime string", K(ret));
  }
  return ret;
}

OB_INLINE void ObTimeConverter::apply_date_year2_rule(ObTimeDigits &year)
{
  if (year.len_ <= 2) {
    year.value_ += (year.value_ < EPOCH_YEAR2 ? 2000 : 1900);
  }
}

OB_INLINE void ObTimeConverter::apply_date_year2_rule(int32_t &year)
{
  if (year < 100) {
    year += (year < EPOCH_YEAR2 ? 2000 : 1900);
  }
}

OB_INLINE void ObTimeConverter::apply_date_year2_rule(int64_t &year)
{
  if (year < 100) {
    year += (year < EPOCH_YEAR2 ? 2000 : 1900);
  }
}

OB_INLINE int ObTimeConverter::apply_usecond_delim_rule(ObTimeDelims &second, ObTimeDigits &usecond)
{
  int ret = OB_SUCCESS;
  if (is_single_dot(second) && usecond.value_ > 0) {
    ret = normalize_usecond_round(usecond);
  } else {
    usecond.value_ = 0;
  }
  return ret;
}

int ObTimeConverter::apply_datetime_for_time_rule(ObTime &ob_time, const ObTimeDigits *digits, const ObTimeDelims *delims)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(digits) || OB_ISNULL(delims)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("digits or delims is null", K(ret));
  } else {
    int32_t delim_cnt = 0;
    bool has_space = false;
    int32_t i = 0;
    // '1011121314.15' => NULL.
    // '101112131415.16' => 13:14:15.160000 .
    // '101112131415.16.17' => NULL.
    // '101112131415..16' => 13:14:15 .
    // '101112131415..16.17' => 13:14:15 .
    // '101112131415.1611112.17' => 13:14:15.161111 .
    // delims between second and usecond, delims after usecond should be handled separately, so sub 2.
    for (; i < DATETIME_PART_CNT - 2; ++i) {
      delim_cnt += delims[i].len_;
      if (has_any_space(delims[i])) {
        has_space = true;
      }
    }
    if (is_single_dot(delims[DT_SEC]) && digits[DT_USEC].len_ <= 6) {
      delim_cnt += delims[DT_USEC].len_;
    }
    if (delim_cnt > 0 && !has_space) {
      ob_time.mode_ |= DT_TYPE_NONE;
    }
  }
  return ret;
}

OB_INLINE int ObTimeConverter::add_timezone_offset(const ObTimeZoneInfo *tz_info, int64_t &value)
{
  int ret = OB_SUCCESS;
  if (NULL != tz_info && ZERO_DATETIME != value) {
    int32_t offset = 0;
    if (OB_FAIL(tz_info->get_timezone_offset(USEC_TO_SEC(value), offset))) {
      LOG_WARN("failed to get offset between utc and local", K(ret));
    } else {
      value += SEC_TO_USEC(offset);
    }
  }
  return ret;
}

OB_INLINE int ObTimeConverter::sub_timezone_offset(const ObTimeZoneInfo *tz_info, int64_t &value)
{
  int ret = OB_SUCCESS;
  if (NULL != tz_info && ZERO_DATETIME != value) {
    int32_t offset = 0;
    if (OB_FAIL(tz_info->get_timezone_offset(USEC_TO_SEC(value), offset))) {
      LOG_WARN("failed to get offset between utc and local", K(ret));
    } else {
      value -= SEC_TO_USEC(offset);
    }
  }
  return ret;
}

int ObTimeConverter::get_str_array_idx(const ObString &str, const ObTimeConstStr *str_arr, int32_t count, int32_t &idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str.ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get index from array by string", K(ret));
  } else {
    int32_t i = 1;
    for (; i <= count; i++) {
      if (str.length() >= str_arr[i].len_ && 0 == STRNCASECMP(str.ptr(), str_arr[i].ptr_, str_arr[i].len_)) {
        break;
      }
    }
    if (i > count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get index from array by string", K(ret));
    } else {
      idx = i;
    }
  }
  return ret;
}

void ObTimeConverter::get_first_day_of_isoyear(ObTime &ob_time)
{
  int32_t wday = ob_time.parts_[DT_WDAY];
  int32_t week = ob_time_to_week(ob_time, WEEK_MODE[3]);
  int32_t offset = ((week - 1) * 7 + (wday - 1));
  ob_time.parts_[DT_DATE] -= offset;
}

} // namesapce common
} // namespace oceanbase
