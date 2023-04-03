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
#include "lib/utility/ob_template_utils.h"
#include "lib/timezone/ob_oracle_format_models.h"
#include "lib/ob_proxy_worker.h"

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

const int32_t DT_PART_BASE[DATETIME_PART_CNT]  = { 100, 12, -1, 24, 60, 60, 1000000};
const int32_t DT_PART_MIN[DATETIME_PART_CNT]   = {   0,  1,  1,  0,  0,  0, 0};
const int32_t DT_PART_MAX[DATETIME_PART_CNT]   = {9999, 12, 31, 23, 59, 59, 1000000};
// 1000000 for usecond, because sometimes we round .9999999 to  .1000000

const int64_t TZ_PART_BASE[DATETIME_PART_CNT] = {100, 12, -1, 24, 60, 60, 1000000000};
const int64_t TZ_PART_MIN[DATETIME_PART_CNT] = {1, 1, 1, 0, 0, 0, 0};
const int64_t TZ_PART_MAX[DATETIME_PART_CNT] = {9999, 12, 31, 23, 59, 59, 1000000000};

static const int8_t DAYS_PER_MON[2][12 + 1]    = {
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

static const ObTimeConstStr MDAY_NAMES[31 + 1] = {
  ObTimeConstStr("null", 4),
  ObTimeConstStr("1st", 3),
  ObTimeConstStr("2nd", 3),
  ObTimeConstStr("3rd", 3),
  ObTimeConstStr("4th", 3),
  ObTimeConstStr("5th", 3),
  ObTimeConstStr("6th", 3),
  ObTimeConstStr("7th", 3),
  ObTimeConstStr("8th", 3),
  ObTimeConstStr("9th", 3),
  ObTimeConstStr("10th", 4),
  ObTimeConstStr("11th", 4), 
  ObTimeConstStr("12th", 4), 
  ObTimeConstStr("13th", 4),
  ObTimeConstStr("14th", 4),
  ObTimeConstStr("15th", 4),
  ObTimeConstStr("16th", 4),
  ObTimeConstStr("17th", 4),
  ObTimeConstStr("18th", 4),
  ObTimeConstStr("19th", 4),
  ObTimeConstStr("20th", 4),
  ObTimeConstStr("21st", 4),
  ObTimeConstStr("22nd", 4),
  ObTimeConstStr("23rd", 4),
  ObTimeConstStr("24th", 4),
  ObTimeConstStr("25th", 4),
  ObTimeConstStr("26th", 4),
  ObTimeConstStr("27th", 4),
  ObTimeConstStr("28th", 4),
  ObTimeConstStr("29th", 4),
  ObTimeConstStr("30th", 4),
  ObTimeConstStr("31st", 4)
};

static const char *DAY_NAME[31 + 1] = {
  "0th", "1st", "2nd", "3rd", "4th", "5th", "6th", "7th", "8th", "9th", "10th",
  "11th", "12th", "13th", "14th", "15th", "16th", "17th", "18th", "19th", "20th",
  "21st", "22nd", "23rd", "24th", "25th", "26th", "27th", "28th", "29th", "30th",
  "31st"
};

static const ObTimeConstStr WDAY_NAMES[DAYS_PER_WEEK + 1] = {
  ObTimeConstStr("null", 4),
  ObTimeConstStr("Monday", 6),
  ObTimeConstStr("Tuesday", 7),
  ObTimeConstStr("Wednesday", 9),
  ObTimeConstStr("Thursday", 8),
  ObTimeConstStr("Friday", 6),
  ObTimeConstStr("Saturday", 8),
  ObTimeConstStr("Sunday", 6)
};

static const int32_t MAX_WDAY_NAME_LENGTH = ObTimeConverter::calc_max_name_length(WDAY_NAMES, DAYS_PER_WEEK);


static const ObTimeConstStr WDAY_ABBR_NAMES[DAYS_PER_WEEK + 1] = {
  ObTimeConstStr("null", 4),
  ObTimeConstStr("Mon", 3),
  ObTimeConstStr("Tue", 3),
  ObTimeConstStr("Wed", 3),
  ObTimeConstStr("Thu", 3),
  ObTimeConstStr("Fri", 3),
  ObTimeConstStr("Sat", 3),
  ObTimeConstStr("Sun", 3)
};

static const ObTimeConstStr MON_NAMES[12 + 1] = {
  ObTimeConstStr("null", 4),
  ObTimeConstStr("January", 7),
  ObTimeConstStr("February", 8),
  ObTimeConstStr("March", 5),
  ObTimeConstStr("April", 5),
  ObTimeConstStr("May", 3),
  ObTimeConstStr("June", 4),
  ObTimeConstStr("July", 4),
  ObTimeConstStr("August", 6),
  ObTimeConstStr("September", 9),
  ObTimeConstStr("October", 7),
  ObTimeConstStr("November", 8),
  ObTimeConstStr("December", 8)
};

static const int32_t MAX_MON_NAME_LENGTH = ObTimeConverter::calc_max_name_length(MON_NAMES, 12);
static const int32_t MAX_MON_NAME_LENGTH_ORACLE = 36;

static const ObTimeConstStr MON_ABBR_NAMES[12 + 1] = {
  ObTimeConstStr("null", 4),
  ObTimeConstStr("Jan", 3),
  ObTimeConstStr("Feb", 3),
  ObTimeConstStr("Mar", 3),
  ObTimeConstStr("Apr", 3),
  ObTimeConstStr("May", 3),
  ObTimeConstStr("Jun", 3),
  ObTimeConstStr("Jul", 3),
  ObTimeConstStr("Aug", 3),
  ObTimeConstStr("Sep", 3),
  ObTimeConstStr("Oct", 3),
  ObTimeConstStr("Nov", 3),
  ObTimeConstStr("Dec", 3)
};

#define START_WITH_SUNDAY       0x04
#define WEEK_FIRST_WEEKDAY      0x02
#define INCLUDE_CRITICAL_WEEK   0x01

const int64_t SECS_PER_HOUR = (SECS_PER_MIN * MINS_PER_HOUR);
const int64_t SECS_PER_DAY = (SECS_PER_HOUR * HOURS_PER_DAY);
const int64_t USECS_PER_DAY = (USECS_PER_SEC * SECS_PER_DAY);
const int64_t NSECS_PER_DAY = (NSECS_PER_SEC * SECS_PER_DAY);
const int64_t USECS_PER_MIN = (USECS_PER_SEC * SECS_PER_MIN);

const ObString ObTimeConverter::DEFAULT_NLS_DATE_FORMAT("DD-MON-RR");
const ObString ObTimeConverter::DEFAULT_NLS_TIMESTAMP_FORMAT("DD-MON-RR HH.MI.SSXFF AM");
const ObString ObTimeConverter::DEFAULT_NLS_TIMESTAMP_TZ_FORMAT("DD-MON-RR HH.MI.SSXFF AM TZR");
const ObString ObTimeConverter::COMPAT_OLD_NLS_DATE_FORMAT("YYYY-MM-DD HH24:MI:SS");
const ObString ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_FORMAT("YYYY-MM-DD HH24:MI:SS.FF");
const ObString ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT("YYYY-MM-DD HH24:MI:SS.FF TZR TZD");


int ObTime::set_tz_name(const ObString &tz_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tz_name.empty()) || OB_UNLIKELY(tz_name.length() >= OB_MAX_TZ_NAME_LEN)) {
    ret = OB_INVALID_DATE_FORMAT;
    LOG_WARN("invalid tz_name", "length", tz_name.length(), "expect_len", OB_MAX_TZ_NAME_LEN, K(ret));
  } else {
    MEMCPY(tz_name_, tz_name.ptr(), tz_name.length());
    tz_name_[tz_name.length()] = '\0';
    is_tz_name_valid_ = true;
  }

  if (OB_FAIL(ret)) {
    tz_name_[0] = '\0';
    is_tz_name_valid_ = false;
  }
  return ret;
}

int ObTime::set_tzd_abbr(const ObString &tzd_abbr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tzd_abbr.empty()) || OB_UNLIKELY(tzd_abbr.length() >= OB_MAX_TZ_ABBR_LEN)) {
    ret = OB_INVALID_DATE_FORMAT;
    LOG_WARN("invalid tz_name", "length", tzd_abbr.length(), "expect_len", OB_MAX_TZ_ABBR_LEN, K(ret));
  } else {
    MEMCPY(tzd_abbr_, tzd_abbr.ptr(), tzd_abbr.length());
    tzd_abbr_[tzd_abbr.length()] = '\0';
  }

  if (OB_FAIL(ret)) {
    tzd_abbr_[0] = '\0';
  }
  return ret;
}

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

/**
 * @brief cast str to oracle date with format
 *
 * @param str input string of date
 * @param cvrt_ctx
 * @param value out param, oracle DATE result
 * @return int
 */
int ObTimeConverter::str_to_date_oracle(const ObString &str,
                                        const ObTimeConvertCtx &cvrt_ctx,
                                        ObDateTime &value)
{
  int ret = OB_SUCCESS;
  ObTime ob_time;
  ObDateTime result_value = 0;
  ObScale scale = 0;
  if (OB_FAIL(str_to_ob_time_oracle_dfm(str, cvrt_ctx, ObDateTimeType, ob_time, scale))) {
    LOG_WARN("failed to convert str to ob_time", K(str), K(cvrt_ctx.oracle_nls_format_));
  } else if (OB_FAIL(ob_time_to_datetime(ob_time, cvrt_ctx.tz_info_, result_value))) {
    LOG_WARN("convert ob_time to datetime failed", K(ret), K(ob_time));
  } else {
    value = result_value;
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

/**
 * @brief cast str to oracle timestamp(3 possible types)
 * @param in:     str           input string
 * @param in:     cvrt_ctx
 * @param in:     target_type   target types, includes ObTimestampTZType ObTimestampNanoType ObTimestampLTZType
 * @param out:    value         result ObOTimestampData
 * @param out:    scale         scale of fractional seconds
 * @return
 */
int ObTimeConverter::str_to_otimestamp(const ObString &str,
                                       const ObTimeConvertCtx &cvrt_ctx,
                                       const ObObjType target_type,
                                       ObOTimestampData &value,
                                       ObScale &scale)
{
  int ret = OB_SUCCESS;
  value.reset();

  // NOTE::current format is fixed like "2012-12-02 12:00:00.123456".
  //      it is not enough for oracle when format variables supported.
  // UPDATE: complex format has supported.
  if (OB_UNLIKELY(!ob_is_otimestamp_type(target_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("it is not otimestamp type", K(target_type), K(ret));
  } else if (str.empty()) {
    value.set_null_value();
    scale = OB_MAX_TIMESTAMP_TZ_PRECISION;
    LOG_DEBUG("succ to convert null str to otimestamp", K(target_type), K(str), K(value), K(scale), K(lbt()));
  } else {
    ObTime ob_time;
    if (OB_FAIL(str_to_ob_time_oracle_dfm(str, cvrt_ctx, target_type, ob_time, scale))) {
      LOG_WARN("failed to convert to ob_time by dfm", "format_str", cvrt_ctx.oracle_nls_format_, K(ret));
    } else if (OB_FAIL(ob_time_to_utc(target_type, cvrt_ctx, ob_time))) {
      LOG_WARN("failed to convert ob_time to utc", K(ret));
    } else if (OB_FAIL(ob_time_to_otimestamp(ob_time, value))) {
      LOG_WARN("failed to convert obtime to timestamp_tz", K(ret));
    } else {
      LOG_DEBUG("succ to convert str to otimestamp",
          K(target_type),
          K(str),
          K(ob_time),
          K(value),
          K(scale),
          "format_str",
          cvrt_ctx.oracle_nls_format_,
          K(lbt()));
    }
  }

  return ret;
}

////////////////////////////////
// int / double / uint / string <- datetime / date / time / year
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

// date(local) --> timestamp tz(utc, offset)
//                timestamp ltz(utc)
//                timestamp (local)
int ObTimeConverter::odate_to_otimestamp(int64_t in_value_us,
                                         const ObTimeZoneInfo *tz_info,
                                         const ObObjType out_type,
                                         ObOTimestampData &out_value)
{
  int ret = OB_SUCCESS;
  if (ZERO_DATETIME == in_value_us) {
    out_value.set_null_value();
    LOG_DEBUG("null odate_to_otimestamp", K(ret), K(in_value_us), K(out_type), K(lbt()));
  } else if (OB_ISNULL(tz_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tz_info is null, it should not happened", K(ret));
  } else if (ObTimestampTZType == out_type) {
    int32_t offset_min = 0;
    int32_t tz_id = OB_INVALID_INDEX;
    int32_t tran_type_id = OB_INVALID_INDEX;
    if (OB_FAIL(sub_timezone_offset(*tz_info, ObString(), in_value_us, offset_min, tz_id, tran_type_id))) {
      LOG_WARN("failed to adjust value with time zone offset", K(ret));
    } else {
      out_value.time_us_ = in_value_us;
      out_value.time_ctx_.tail_nsec_ = 0;
      if (OB_INVALID_INDEX == tz_id) {
        out_value.time_ctx_.store_tz_id_ = 0;
        out_value.time_ctx_.set_offset_min(offset_min);
      } else {
        out_value.time_ctx_.store_tz_id_ = 1;
        out_value.time_ctx_.set_tz_id(tz_id);
        out_value.time_ctx_.set_tran_type_id(tran_type_id);
      }
    }
  } else if (ObTimestampLTZType == out_type) {
    if (OB_FAIL(sub_timezone_offset(tz_info, true, ObString(), in_value_us, true))) {
      LOG_WARN("failed to adjust value with time zone offset", K(ret));
    } else {
      out_value.time_us_ = in_value_us;
      out_value.time_ctx_.tail_nsec_ = 0;
    }
  } else if (ObTimestampNanoType == out_type) {
    out_value.time_us_ = in_value_us;
    out_value.time_ctx_.tail_nsec_ = 0;
  } else {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

// timestamp tz(utc, offset) --> date(local)
// timestamp ltz(utc)        --> date(local)
// timestamp (local)         --> date(local)
int ObTimeConverter::otimestamp_to_odate(const ObObjType in_type,
                                         const ObOTimestampData &in_value,
                                         const ObTimeZoneInfo *tz_info,
                                         int64_t &out_usec)
{
  int ret = OB_SUCCESS;
  if (in_value.is_null_value()) {
    out_usec = ZERO_DATETIME;
  } else if (OB_ISNULL(tz_info)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("tz_info is null", K(ret));
  } else if (ObTimestampTZType == in_type) {
    int32_t offset_min = 0;
    ObTime ob_time(DT_TYPE_ORACLE_TIMESTAMP);
    if (OB_FAIL(extract_offset_from_otimestamp(in_value, tz_info, offset_min, ob_time))) {
      LOG_WARN("failed to extract_offset_from_otimestamp", K(ret));
    } else {
      out_usec = in_value.time_us_ + MIN_TO_USEC(offset_min);
    }
  } else if (ObTimestampLTZType == in_type) {
    out_usec = in_value.time_us_;
    if (OB_FAIL(add_timezone_offset(tz_info, out_usec))) {
      LOG_WARN("failed to adjust value with time zone offset", K(ret));
    }
  } else if (ObTimestampNanoType == in_type) {
    out_usec = in_value.time_us_;
  } else {
    ret = OB_ERR_UNEXPECTED;
  }
  LOG_DEBUG("succ otimestamp_to_odate", K(ret), K(in_type), K(in_value), K(out_usec));
  return ret;
}

int ObTimeConverter::otimestamp_to_otimestamp(const ObObjType in_type,
                                              const ObOTimestampData &in_value,
                                              const ObTimeZoneInfo *tz_info,
                                              const ObObjType out_type,
                                              ObOTimestampData &out_value)
{
  int ret = OB_SUCCESS;
  out_value.reset();
  if (in_value.is_null_value()) {
    out_value.set_null_value();
  } else if (out_type == in_type) {
    out_value = in_value;
  } else if (ObTimestampLTZType == in_type && ObTimestampTZType == out_type) {
    int64_t in_value_us = in_value.time_us_;
    ObString tz_abbr_str;
    int32_t offset_sec = 0;
    int32_t tz_id = OB_INVALID_INDEX;
    int32_t tran_type_id = OB_INVALID_INDEX;
    if (OB_ISNULL(tz_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tz_info should not be null", K(ret), K(lbt()));
    } else if (OB_FAIL(tz_info->get_timezone_offset(in_value_us, offset_sec, tz_abbr_str, tran_type_id))) {
      LOG_WARN("failed to adjust value with time zone offset", K(ret));
    } else {
      out_value.time_us_ = in_value_us;
      out_value.time_ctx_.tail_nsec_ = in_value.time_ctx_.tail_nsec_;
      if (common::OB_INVALID_TZ_ID == tz_info->get_tz_id()) {
        out_value.time_ctx_.store_tz_id_ = 0;
        out_value.time_ctx_.set_offset_min(static_cast<int32_t>(SEC_TO_MIN(offset_sec)));
      } else {
        out_value.time_ctx_.store_tz_id_ = 1;
        out_value.time_ctx_.set_tz_id(tz_id);
        out_value.time_ctx_.set_tran_type_id(tran_type_id);
      }
    }
  } else if (ObTimestampTZType == in_type && ObTimestampLTZType == out_type) {
    out_value.time_us_ = in_value.time_us_;
    out_value.time_ctx_.tail_nsec_ = in_value.time_ctx_.tail_nsec_;
  }
  LOG_DEBUG("succ otimestamp_to_otimestamp", K(ret), K(in_type), K(in_value), K(out_type), K(out_value));
  return ret;
}

int ObTimeConverter::otimestamp_to_str(const ObOTimestampData &ot_data,
                                       const ObDataTypeCastParams &dtc_params,
                                       const int16_t scale,
                                       const ObObjType type,
                                       char *buf,
                                       int64_t buf_len,
                                       int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (ot_data.is_null_value()) {
    LOG_DEBUG("succ to null otimestamp_to_str", K(ot_data), K(type), K(scale), K(lbt()));
  } else {
    const ObOTimestampData &tmp_ot_data = round_otimestamp(scale, ot_data);
    const bool store_utc_time = false;
    ObTime ob_time(DT_TYPE_ORACLE_TIMESTAMP);
    const int64_t old_pos = pos;
    const ObString format_str = dtc_params.get_nls_format(type);
    if (OB_FAIL(otimestamp_to_ob_time(type, tmp_ot_data, dtc_params.tz_info_, ob_time, store_utc_time))) {
      LOG_WARN("failed to convert otimestamp to ob time", K(ret), K(lbt()));
    } else if (OB_FAIL(dtc_params.force_use_standard_format_
                       ? ob_time_to_str(ob_time, DT_TYPE_DATETIME, scale, buf, buf_len, pos, true)
                       : ob_time_to_str_oracle_dfm(ob_time, scale, format_str, buf, buf_len, pos))) {
      LOG_WARN("failed to convert ob time to string", K(format_str), K(ob_time), K(ret));
    } else {
      ObString tmp(pos - old_pos, buf + old_pos);
      LOG_DEBUG("succ to otimestamp_to_str",
        K(ot_data), K(type), K(scale), K(tmp_ot_data), K(tmp), K(format_str), K(lbt()));
    }
  }
  return ret;
}

bool ObTimeConverter::is_valid_otimestamp(const int64_t time_us, const int32_t tail_nsec)
{
  return ((lib::is_oracle_mode() ? ORACLE_DATETIME_MIN_VAL : DATETIME_MIN_VAL) <= time_us
          && time_us <= DATETIME_MAX_VAL
          && ObOTimestampData::MIN_TAIL_NSEC <= tail_nsec
          && tail_nsec <= ObOTimestampData::MAX_TAIL_NSEC);
}

int ObTimeConverter::extract_offset_from_otimestamp(const ObOTimestampData &in_value,
                                                    const ObTimeZoneInfo *tz_info,
                                                    int32_t &offset_min,
                                                    ObTime &ob_time)
{
  int ret = OB_SUCCESS;
  if (in_value.time_ctx_.store_tz_id_) {
    ObTZInfoMap *tz_info_map = NULL;
    ObTimeZoneInfoPos *literal_tz_info = NULL;
    ObString tz_name_str;
    ObString tz_abbr_str;
    int32_t offset_sec = 0;
    if (OB_ISNULL(tz_info)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("tz_info is null", K(ret));
    } else if (OB_ISNULL(tz_info_map = const_cast<ObTZInfoMap*>(tz_info->get_tz_info_map()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tz_info_map is NULL", K(ret));
    } else if (OB_FAIL(tz_info_map->get_tz_info_by_id(in_value.time_ctx_.tz_id_, literal_tz_info))) {
      LOG_WARN("fail to get_tz_info_by_id", "tz_id", in_value.time_ctx_.tz_id_, K(ret));
      ret = OB_ERR_INVALID_TIMEZONE_REGION_ID;
    } else if (OB_FAIL(literal_tz_info->get_timezone_offset(in_value.time_ctx_.tran_type_id_, tz_abbr_str, offset_sec))) {
      LOG_WARN("fail to get_timezone_offset", K(in_value), K(ret));
      ret = OB_ERR_INVALID_TIMEZONE_REGION_ID;
    } else if (OB_FAIL(literal_tz_info->get_tz_name(tz_name_str))) {
      LOG_WARN("fail to get_tz_name", K(tz_name_str), K(ret));
    } else if (OB_FAIL(ob_time.set_tz_name(tz_name_str))) {
      LOG_WARN("fail to set_tz_name", K(tz_name_str), K(ret));
    } else if (OB_FAIL(ob_time.set_tzd_abbr(tz_abbr_str))) {
      LOG_WARN("fail to set_tz_abbr", K(tz_abbr_str), K(ret));
    } else {
      offset_min = static_cast<int32_t>(SEC_TO_MIN(offset_sec));
      ob_time.time_zone_id_ = in_value.time_ctx_.tz_id_;
      ob_time.transition_type_id_ = in_value.time_ctx_.tran_type_id_;
      ob_time.parts_[DT_OFFSET_MIN] = offset_min;
    }
    LOG_DEBUG("extract_offset_from_otimestamp", K(ob_time), K(offset_min), K(offset_sec), K(ret));

    if (NULL != tz_info_map && NULL != literal_tz_info) {
      tz_info_map->free_tz_info_pos(literal_tz_info);
    }
  } else {
    offset_min = in_value.time_ctx_.get_offset_min();
    ob_time.parts_[DT_OFFSET_MIN] = offset_min;
  }
  LOG_DEBUG("extract_offset_from_otimestamp", K(ob_time), K(offset_min), K(ret));
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

int ObTimeConverter::check_leading_precision(const ObTimeDigits &digits)
{
  int ret = OB_SUCCESS;
  const int64_t oracle_max_leading_precision = 9;
  if (digits.value_ != 0 && digits.len_ > oracle_max_leading_precision) {
    int64_t leading_zero_count = 0;
    for (int64_t i = 0; i < digits.len_ && '0' == digits.ptr_[i]; i++) {
      leading_zero_count++;
    }
    if (leading_zero_count >= oracle_max_leading_precision) {
      ret = OB_ERR_THE_LEADING_PRECISION_OF_THE_INTERVAL_IS_TOO_SMALL;
    }
  }
  return ret;
}


////////////////////////////////
// string -> offset.

#define OFFSET_MIN  static_cast<int32_t>(-(12 * MINS_PER_HOUR + 59) * SECS_PER_MIN)        // -12:59 .
#define OFFSET_MAX  static_cast<int32_t>(13 * SECS_PER_HOUR)                               // +13:00 .
#define ORACLE_OFFSET_MIN static_cast<int32_t>(-(15 * MINS_PER_HOUR + 59) * SECS_PER_MIN)  // -15:59 .
#define ORACLE_OFFSET_MAX static_cast<int32_t>(15 * SECS_PER_HOUR)                         // +15:00 .
#define ORACLE_OFFSET_MAX_HOUR 15

/* str_to_offset usually return OB_ERR_UNKNOWN_TIME_ZONE when failed.
 * Then str is treated as a position and searched in time_zone_map, such 'Asia/Shanghai'.
 * If search failed, mysql reports OB_ERR_UNKNOWN_TIME_ZONE while oracle may return different error code.
 * Such as OB_ERR_INVALID_TIME_ZONE_HOUR when hour is greater than 15,
 * and OB_ERR_INVALID_TIME_ZONE_MINUTE when minute is greater than 59.
 * ret_more records this error code, and if search time_zone_map failed, we choose whether
 * set ret = ret_more according to compatible mode.
 */
int ObTimeConverter::str_to_offset(const ObString &str,
                                   int32_t &value,
                                   int &ret_more,
                                   const bool is_oracle_mode,
                                   const bool need_check_valid /* false */)
{
  int ret = OB_SUCCESS;
  ret_more = OB_SUCCESS;
  const ObString tmp_str = (is_oracle_mode ? const_cast<ObString&>(str).trim() : str);
  if (OB_ISNULL(tmp_str.ptr()) || OB_UNLIKELY(tmp_str.length() <= 0)) {
    ret = OB_ERR_UNKNOWN_TIME_ZONE;
    LOG_WARN("invalid time zone offset", K(ret), K(str), K(tmp_str));
  } else {
    const char *pos = tmp_str.ptr();
    const char *end = pos + tmp_str.length();
    char sign = *pos;
    if (is_oracle_mode) {
      // default is +
      if ('+' != sign && '-' != sign) {
        sign = '+';
      } else {
        pos++;
      }
    } else {
      pos++;
    }

    ObTimeDigits hour;
    ObTimeDigits minute;
    ObTimeDelims colon;
    ObTimeDelims none;
    if (OB_FAIL(get_datetime_digits_delims(pos, end, INT32_MAX, hour, colon))) {
      LOG_WARN("failed to get offset", K(ret), K(str));
    } else if (OB_FAIL(get_datetime_digits_delims(pos, end, INT32_MAX, minute, none))) {
      LOG_WARN("failed to get offset", K(ret), K(str));
    } else if (!('+' == sign || '-' == sign)
               || 0 == hour.len_
               || 0 == minute.len_
               // oracle just ignore decimal part and invalid char in the end.
               || !is_single_colon(colon)
               || (none.len_ > 0 && !is_oracle_mode)) {
      ret = OB_ERR_UNKNOWN_TIME_ZONE;
    } else if (!need_check_valid) {
      /* sometimes no need check validation, such as session deserialization
       * and load time_zone system variable to session.
       * We only need check validation when set sys variable.
       */
      value = static_cast<int32_t>(((hour.value_ * MINS_PER_HOUR) + minute.value_) * SECS_PER_MIN);
      if ('-' == sign) {
        value = -value;
      }
    } else if (is_oracle_mode && OB_FAIL(check_leading_precision(hour))) {
      LOG_WARN("check hour leading precision failed", K(ret), K(hour));
    } else if (is_oracle_mode && OB_FAIL(check_leading_precision(minute))) {
      LOG_WARN("check minute leading precision failed", K(ret), K(minute));
    } else if (OB_UNLIKELY(minute.value_ >= MINS_PER_HOUR || minute.value_ < 0)) {
      ret = OB_ERR_UNKNOWN_TIME_ZONE;
      // Oracle reports hour out of range when both hour and minute out of range.
      ret_more = is_oracle_mode ? (hour.value_ > ORACLE_OFFSET_MAX_HOUR ? OB_ERR_INVALID_TIME_ZONE_HOUR
                                                                        : OB_ERR_INVALID_TIME_ZONE_MINUTE)
                                : ret_more;
    } else {
      value = static_cast<int32_t>(((hour.value_ * MINS_PER_HOUR) + minute.value_) * SECS_PER_MIN);
      if ('-' == sign) {
        value = -value;
      }

      if (is_oracle_mode) {
        if (OB_UNLIKELY(!(ORACLE_OFFSET_MIN <= value && value <= ORACLE_OFFSET_MAX))) {
          ret_more =
              hour.value_ > ORACLE_OFFSET_MAX_HOUR ? OB_ERR_INVALID_TIME_ZONE_HOUR : OB_ERR_INVALID_TIME_ZONE_MINUTE;
          ret = OB_ERR_UNKNOWN_TIME_ZONE;
          LOG_WARN("invalid time zone offset", K(ret), K(minute.value_), K(str));
        }
        LOG_DEBUG("finish str_to_offset", K(str), K(value), K(ret), K(lbt()));
      } else {
        if (OB_UNLIKELY(!(OFFSET_MIN <= value && value <= OFFSET_MAX))) {
          ret_more =
              (minute.value_ >= DT_PART_BASE[DT_MIN] ? OB_ERR_INVALID_TIME_ZONE_MINUTE : OB_ERR_INVALID_TIME_ZONE_HOUR);
          ret = OB_ERR_UNKNOWN_TIME_ZONE;
          LOG_WARN("invalid time zone offset", K(ret), K(minute.value_), K(str));
        }
      }
    }
  }
  LOG_DEBUG("finish str_to_offset", K(ret), K(ret_more), K(str), K(value));
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

int ObTimeConverter::get_oracle_err_when_datetime_parts_conflict(int64_t part_idx)
{
  int ret = OB_SUCCESS;
  switch (part_idx) {
    case DT_YEAR:
      ret = OB_ERR_UNEXPECTED;  // never goes here for now
      break;
    case DT_MON:
      ret = OB_ERR_MONTH_CONFLICTS_WITH_JULIAN_DATE;  // ORA-01833: month conflicts with Julian date
      break;
    case DT_MDAY:
      ret = OB_ERR_DAY_OF_MONTH_CONFLICTS_WITH_JULIAN_DATE;  // ORA-01834: day of month conflicts with Julian date
      break;
    case DT_HOUR:
      ret = OB_ERR_HOUR_CONFLICTS_WITH_SECONDS_IN_DAY;  // ORA-01836: hour conflicts with seconds in day
      break;
    case DT_MIN:
      ret = OB_ERR_MINUTES_OF_HOUR_CONFLICTS_WITH_SECONDS_IN_DAY;  // ORA-01837: minutes of hour conflicts with seconds
                                                                   // in day
      break;
    case DT_SEC:
      ret = OB_ERR_SECONDS_OF_MINUTE_CONFLICTS_WITH_SECONDS_IN_DAY;  // ORA-01838: seconds of minute conflicts with
                                                                     // seconds in day
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
  }
  return ret;
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

static const int32_t DATETIME_PART_LENS_MAX[]   = {
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

/**
 * @brief convert string to ob_time struct according to oracle datetime format model
 * @param in:   str         input string
 * @param in:   format      format string
 * @param in:   cvrt_ctx
 * @param in:   target_type includes ObDateTimeType, ObOTimestampTZType, ObOTimestampLTZType, ObOTimestampType
 * @param out:  ob_time     memory struct of datetime
 * @param out:  scale       scale of fractional seconds
 */
int ObTimeConverter::str_to_ob_time_oracle_dfm(const ObString &str,
                                               const ObTimeConvertCtx &cvrt_ctx,
                                               const ObObjType target_type,
                                               ObTime &ob_time,
                                               ObScale &scale)
{
  int ret = OB_SUCCESS;
  const ObString &format = cvrt_ctx.oracle_nls_format_;
  if (OB_UNLIKELY(str.empty()
                  || format.empty()
                  || (!ob_is_otimestamp_type(target_type)
                      && !ob_is_datetime_tc(target_type)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(str), K(format), K(ob_time.mode_));
  } else {
    ob_time.mode_ |= DT_TYPE_DATETIME;
    if (ob_is_otimestamp_type(target_type)) {
      ob_time.mode_ |= DT_TYPE_ORACLE;
    }
    if (ob_is_timestamp_tz(target_type)) {
      ob_time.mode_ |= DT_TYPE_TIMEZONE;
    }
  }

  if (OB_SUCC(ret)) {
    ObSEArray<ObDFMElem, ObDFMUtil::COMMON_ELEMENT_NUMBER> dfm_elems;
    ObBitSet<ObDFMFlag::MAX_FLAG_NUMBER> elem_flags;

    // 1. parse and check semantic of format string
    if (OB_FAIL(ObDFMUtil::parse_datetime_format_string(format, dfm_elems))) {
      LOG_WARN("fail to parse oracle datetime format string", K(ret), K(format));
    } else if (OB_FAIL(ObDFMUtil::check_semantic(dfm_elems, elem_flags, ob_time.mode_))) {
      LOG_WARN("check semantic of format string failed", K(ret), K(format));
    }

    int32_t temp_tzh_value = -1;  // positive value is legal
    int32_t temp_tzm_value = -1;  // positive value is legal
    int32_t temp_tz_factor = 0;

    int32_t tz_hour = 0;  // will be negetive when time zone offset < 0
    int32_t tz_min = 0;   // will be negetive when time zone offset < 0
    int32_t session_tz_id = 0;
    int32_t session_tran_type_id = 0;

    int32_t session_tz_offset = 0;
    // 2. set default value for ob_time
    if (OB_SUCC(ret)) {
      int64_t utc_curr_time = ObTimeUtility::current_time();
      int64_t utc_curr_time_copy = utc_curr_time;
      int32_t cur_date = 0;
      if (OB_ISNULL(cvrt_ctx.tz_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session timezone info is null", K(ret));
      } else if (sub_timezone_offset(*(cvrt_ctx.tz_info_),
                                     ObString(),
                                     utc_curr_time_copy,
                                     session_tz_offset,
                                     session_tz_id,
                                     session_tran_type_id)) {
        LOG_WARN("get session timezone offset failed", K(ret));
      } else if (OB_FAIL(datetime_to_date(utc_curr_time, cvrt_ctx.tz_info_, cur_date))) {
        LOG_WARN("timestamp to date failed", K(ret));
      } else if (OB_FAIL(time_to_ob_time(0, ob_time))) {
        LOG_WARN("time to ob_time failed", K(ret));
      } else if (OB_FAIL(date_to_ob_time(cur_date, ob_time))) {
        LOG_WARN("date to ob_time failed", K(ret), K(cur_date));
      } else {
        if (OB_INVALID_TZ_ID != session_tz_id && ob_is_timestamp_tz(target_type)) {
          ob_time.is_tz_name_valid_ = true;
        }
        ob_time.parts_[DT_MDAY] = 1;  // oracle default value is the first day of current month
        ob_time.parts_[DT_DATE] = 0;  // will be recalculated
        ob_time.parts_[DT_YDAY] = 0;  // doesn't matter
        ob_time.parts_[DT_WDAY] = 0;  // doesn't matter
        tz_hour = static_cast<int32_t>(session_tz_offset / MINS_PER_HOUR);
        tz_min = static_cast<int32_t>(session_tz_offset - MINS_PER_HOUR * tz_hour);
        if (OB_FAIL(ObDFMLimit::TIMEZONE_HOUR_ABS.validate(abs(tz_hour)))) {
          LOG_WARN("invalid session timezone hour", K(ret), K(tz_hour));
        } else if (OB_FAIL(ObDFMLimit::TIMEZONE_MIN_ABS.validate(abs(tz_min)))) {
          LOG_WARN("invali d session timezone minutes", K(ret), K(tz_min));
        }
      }
    }

    // 3. go through each element, set and check conflict for ob_time parts: Year, Month, Day, Hour, Minute, Second
    if (OB_SUCC(ret)) {
      ObDFMParseCtx ctx(str.ptr(), str.length());
      int64_t last_elem_end_pos = 0;
      int64_t conflict_part_bitset = 0;
      int64_t elem_idx = 0;
      int64_t input_sep_len = 0;
      int64_t part_blank1_len = 0;
      int64_t part_sep_len = 0;
      int64_t part_blank2_len = 0;
      int64_t format_sep_len = 0;
      STATIC_ASSERT((1 << TOTAL_PART_CNT) < INT64_MAX, "for time_part_conflict_bitset");
      int32_t yday_temp_value = ZERO_DATE;  // as invalid value
      int32_t wday_temp_value = ZERO_DATE;  // as invalid value
      int32_t date_temp_value = ZERO_DATE;  // as invalid value
      int32_t hh12_temp_value = ZERO_TIME;
      int32_t julian_year_value = ZERO_DATE;  // as invalid value
      bool is_after_noon = false;
      bool is_before_christ = false;
      bool has_digit_tz_in_TZR = false;
      int64_t first_non_space_sep_char = INT64_MAX;
      int64_t ignore_fs_flag = false;

      for (elem_idx = 0; OB_SUCC(ret) && elem_idx < dfm_elems.count(); ++elem_idx) {
        ObDFMElem &elem = dfm_elems.at(elem_idx);
        if (OB_UNLIKELY(!elem.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("element is invalid", K(ret), K(elem));
        } else {
          LOG_DEBUG("DFM DEBUG: start element", K(elem), K(ctx));
        }

        // 1. check separate chars and skip blank chars first
        if (OB_SUCC(ret)) {
          // get format sep chars len
          format_sep_len = elem.offset_ - last_elem_end_pos;
          last_elem_end_pos = elem.offset_ + ObDFMFlag::PATTERN[elem.elem_flag_].len_;
          // parse input string and skip them
          part_blank1_len = ObDFMUtil::skip_blank_chars(ctx);
          // The # of skipped non-blank chars is according to format_str
          first_non_space_sep_char = ctx.is_parse_finish() ? INT64_MAX : ctx.cur_ch_[0];
          if (ObDFMFlag::X == elem.elem_flag_) {
            part_sep_len = 0;
          } else {
            part_sep_len = ObDFMUtil::skip_separate_chars(ctx, format_sep_len);
          }
          part_blank2_len = ObDFMUtil::skip_blank_chars(ctx);
          input_sep_len = part_blank1_len + part_sep_len + part_blank2_len;
          LOG_DEBUG("DFM DEBUG: skip blank and serarate chars",
              K(part_blank1_len),
              K(part_sep_len),
              K(part_blank2_len),
              K(format_sep_len),
              K(ctx));
        }

        if (OB_SUCC(ret) && ctx.is_parse_finish()) {
          break;  // if all the input chars has beeen processed, break this loop
        }

        // 2. next, parse the current element
        if (OB_SUCC(ret)) {
          int64_t parsed_elem_len = 0;
          const int64_t expected_elem_len = ObDFMFlag::EXPECTED_MATCHING_LENGTH[elem.elem_flag_];
          ctx.set_next_expected_elem(elem.elem_flag_, format_sep_len > 0 && input_sep_len == 0);
          switch (elem.elem_flag_) {
            case ObDFMFlag::AD:
            case ObDFMFlag::BC:
            case ObDFMFlag::AD2:
            case ObDFMFlag::BC2: {  // TODO : NLS_LANGUAGE
              bool is_with_dot = (ObDFMFlag::AD2 == elem.elem_flag_ || ObDFMFlag::BC2 == elem.elem_flag_);
              parsed_elem_len =
                  is_with_dot ? ObDFMFlag::PATTERN[ObDFMFlag::AD2].len_ : ObDFMFlag::PATTERN[ObDFMFlag::AD].len_;
              bool is_ad = ObDFMUtil::match_pattern_ignore_case(
                  ctx, is_with_dot ? ObDFMFlag::PATTERN[ObDFMFlag::AD2] : ObDFMFlag::PATTERN[ObDFMFlag::AD]);
              bool is_bc = ObDFMUtil::match_pattern_ignore_case(
                  ctx, is_with_dot ? ObDFMFlag::PATTERN[ObDFMFlag::BC2] : ObDFMFlag::PATTERN[ObDFMFlag::BC]);
              if (!is_ad && !is_bc) {
                ret = OB_ERR_BC_OR_AD_REQUIRED;
              } else {
                is_before_christ = is_bc;
              }
              break;
            }
            case ObDFMFlag::D: {
              int32_t wday = 0;
              if (OB_FAIL(ObDFMUtil::match_int_value(ctx, expected_elem_len, parsed_elem_len, wday))) {
                LOG_WARN("failed to match int value", K(ret));
              } else if (OB_FAIL(ObDFMLimit::WEEK_DAY.validate(wday))) {
                LOG_WARN("not a valid day of the week", K(ret), K(wday));
              } else {
                // oracle numbered sunday as 1 in territory of CHINA
                // TODO : hard code for now, need look up NLS_TERRITORIES
                wday_temp_value = (wday + 5) % 7 + 1;
              }
              break;
            }
            case ObDFMFlag::DY:
            case ObDFMFlag::DAY: {  // TODO : NLS_LANGUAGE NLS_TERRITORIES
              int32_t wday = 0;
              for (wday = 1; wday <= DAYS_PER_WEEK; ++wday) {
                const ObTimeConstStr &day_str =
                    (elem.elem_flag_ == ObDFMFlag::DAY) ? WDAY_NAMES[wday] : WDAY_ABBR_NAMES[wday];
                if (ObDFMUtil::match_pattern_ignore_case(ctx, day_str)) {
                  parsed_elem_len = day_str.len_;
                  break;
                }
              }
              if (OB_FAIL(ObDFMLimit::WEEK_DAY.validate(wday))) {
                LOG_WARN("validate week day failed", K(ret), K(wday));
              } else {
                wday_temp_value = wday;
              }
              break;
            }
            case ObDFMFlag::DD: {
              int32_t mday = 0;
              if (OB_FAIL(ObDFMUtil::match_int_value(ctx, expected_elem_len, parsed_elem_len, mday))) {
                LOG_WARN("failed to match int value", K(ret));
              } else if (OB_FAIL(ObDFMLimit::MONTH_DAY.validate(mday))) {
                LOG_WARN("day of month must be between 1 and last day of month", K(ret), K(mday));
              } else {
                // may conflict with DDD
                ret = set_ob_time_part_directly(ob_time, conflict_part_bitset, DT_MDAY, mday);
              }
              break;
            }
            case ObDFMFlag::DDD: {
              int32_t yday = 0;
              if (OB_FAIL(ObDFMUtil::match_int_value(ctx, expected_elem_len, parsed_elem_len, yday))) {
                LOG_WARN("failed to match int value", K(ret));
              } else if (OB_FAIL(ObDFMLimit::YEAR_DAY.validate(yday))) {
                LOG_WARN("day of year must be between 1 and 365 (366 for leap year)", K(ret), K(yday));
              } else {
                yday_temp_value = yday;
              }
              break;
            }
            case ObDFMFlag::DS: {  // TODO : impl it NEED NLS_DATE_FORMAT NLS_TERRITORY NLS_LANGUAGE
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("DS is not supported now", K(ret));
              break;
            }
            case ObDFMFlag::DL: {  // TODO : impl it NEED NLS_DATE_FORMAT NLS_TERRITORY NLS_LANGUAGE
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("DL is not supported now", K(ret));
              break;
            }
            case ObDFMFlag::FF:
            case ObDFMFlag::FF1:
            case ObDFMFlag::FF2:
            case ObDFMFlag::FF3:
            case ObDFMFlag::FF4:
            case ObDFMFlag::FF5:
            case ObDFMFlag::FF6:
            case ObDFMFlag::FF7:
            case ObDFMFlag::FF8:
            case ObDFMFlag::FF9: {
              int32_t usec = 0;
              // format string has '.' or 'X', but input string does not contain '.'
              // do nothing, skip element FF and revert ctx by the length of parsed chars
              if (ignore_fs_flag) {
                ctx.revert(part_blank1_len + part_sep_len + part_blank2_len);
              } else if (elem.is_single_dot_before_ && '.' != first_non_space_sep_char) {
                ctx.revert(part_sep_len + part_blank2_len);
              } else if (OB_FAIL(ObDFMUtil::match_int_value(ctx, expected_elem_len, parsed_elem_len, usec))) {
                LOG_WARN("failed to match usecs", K(ret), K(ctx));
              } else {
                scale = static_cast<ObScale>(parsed_elem_len);
                usec = static_cast<int32_t>(usec * power_of_10[MAX_SCALE_FOR_ORACLE_TEMPORAL - parsed_elem_len]);
                ob_time.parts_[DT_USEC] = usec;
              }
              break;
            }

            case ObDFMFlag::TZH:
            case ObDFMFlag::TZM: {
              if (OB_UNLIKELY(elem_flags.has_member(ObDFMFlag::TZR) || elem_flags.has_member(ObDFMFlag::TZD))) {
                ret = OB_ERR_NOT_A_VALID_TIME_ZONE;
                LOG_WARN("tzh tzm and tzr tzd can not appear at the same time", K(ret));
              } else {
                // SQL> alter session set NLS_TIMESTAMP_TZ_FORMAT='DD-MON-RR HH.MI.SS AM TZH:TZM';
                // SQL> alter session set time_zone='Asia/Shanghai';
                // SQL> select cast('01-SEP-20 11.11.11' as timestamp with time zone) from dual;
                // 01-SEP-20 11.11.11 AM +08:00
                // If format contains TZH and time_zone is a position, need to convert first.
                ob_time.is_tz_name_valid_ = false;
                int32_t value = 0;
                int32_t local_tz_factor = 1;
                if (ObDFMFlag::TZH == elem.elem_flag_) {
                  if (ObDFMUtil::is_sign_char(ctx.cur_ch_[0])) {
                    local_tz_factor = ('-' == ctx.cur_ch_[0] ? -1 : 1);
                    ctx.update(1);
                  } else {
                    if (ctx.get_parsed_len() > 0 && input_sep_len > format_sep_len) {
                      // if the input valid separate chars > format separate chars
                      // the superfluous '-' will be regarded as minus sign
                      local_tz_factor = (static_cast<int64_t>('-') == ctx.cur_ch_[-1] ? -1 : 1);
                    }
                  }
                  temp_tz_factor = local_tz_factor;  // 1 or -1, but never be 0
                }

                if (ctx.is_parse_finish()) {
                  // do nothing
                } else if (OB_FAIL(ObDFMUtil::match_int_value(ctx, expected_elem_len, parsed_elem_len, value))) {
                  LOG_WARN("failed to match usecs", K(ret), K(ctx));
                } else if (OB_FAIL((elem.elem_flag_ == ObDFMFlag::TZH)
                                       ? ObDFMLimit::TIMEZONE_HOUR_ABS.validate(value)
                                       : ObDFMLimit::TIMEZONE_MIN_ABS.validate(value))) {
                  LOG_WARN("failed to validate timezone value", K(value), K(ret));
                } else {
                  if (elem.elem_flag_ == ObDFMFlag::TZH) {
                    temp_tzh_value = value;
                  } else {
                    temp_tzm_value = value;
                  }
                }
              }
              break;
            }

            case ObDFMFlag::TZR: {
              if (OB_UNLIKELY(elem_flags.has_member(ObDFMFlag::TZH) || elem_flags.has_member(ObDFMFlag::TZM))) {
                ret = OB_ERR_NOT_A_VALID_TIME_ZONE;
                LOG_WARN("tzh tzm and tzr tzd can not appear at the same time", K(ret));
              } else {
                int32_t local_tz_factor = 1;
                if (isdigit(ctx.cur_ch_[0]) || ObDFMUtil::is_sign_char(ctx.cur_ch_[0])) {  // case1: digits
                  int32_t tmp_tz_hour = 0;
                  int32_t tmp_tz_min = 0;
                  if (ObDFMUtil::is_sign_char(ctx.cur_ch_[0])) {
                    local_tz_factor = ('-' == ctx.cur_ch_[0] ? -1 : 1);
                    ctx.update(1);
                  } else if (part_blank1_len + part_sep_len > format_sep_len &&
                             ObDFMUtil::is_sign_char(ctx.cur_ch_[-1])) {
                    local_tz_factor = ('-' == ctx.cur_ch_[-1] ? -1 : 1);
                  }

                  ObString digits_timezone;
                  parsed_elem_len = ObDFMUtil::UNKNOWN_LENGTH_OF_ELEMENT;
                  if (OB_LIKELY(!ctx.is_parse_finish())) {
                    // If tz str is not just character '+' or '-', result of convert should be
                    // offset instead of tz_id/name
                    ob_time.is_tz_name_valid_ = false;
                  }
                  if (OB_UNLIKELY(ctx.is_parse_finish())) {
                    // do nothing
                  } else if (OB_FAIL(ObDFMUtil::match_chars_until_space(ctx, digits_timezone, parsed_elem_len))) {
                    // do nothing
                  } else {
                    ObDFMParseCtx local_ctx(digits_timezone.ptr(), digits_timezone.length());
                    const char *local_sep = ObDFMUtil::find_first_separator(local_ctx);

                    if (OB_ISNULL(local_sep)) {
                      // timezone offset string is allowd to contain only hour part.
                    } else {
                      int64_t hour_expected_len = local_sep - digits_timezone.ptr();
                      int64_t local_parsed_len = 0;
                      if (OB_FAIL(ObDFMUtil::match_int_value(
                              local_ctx, hour_expected_len, local_parsed_len, tmp_tz_hour, local_tz_factor))) {
                        LOG_WARN("matching timezone hour failed, for 'TZR'. the error is ignored.",
                            K(ret),
                            K(hour_expected_len));
                      } else if (OB_FAIL(ObDFMLimit::TIMEZONE_HOUR_ABS.validate(abs(tmp_tz_hour)))) {
                        LOG_WARN("not valid timezone hour", K(ret), K(tmp_tz_hour));
                      } else if (OB_UNLIKELY(hour_expected_len != local_parsed_len)) {
                        ret = OB_INVALID_DATE_VALUE;  // invalid time zone
                      } else if (FALSE_IT(local_ctx.update(hour_expected_len + 1))) {
                      } else if (OB_UNLIKELY(local_ctx.is_parse_finish())) {
                        // When format contains TZR, tz str is allowed to be hour, which equals to hour:00.
                        has_digit_tz_in_TZR = true;
                        tz_hour = tmp_tz_hour;
                        tz_min = tmp_tz_min;
                      } else if (OB_FAIL(ObDFMUtil::match_int_value(local_ctx,
                                                                    parsed_elem_len - hour_expected_len - 1,
                                                                    local_parsed_len,
                                                                    tmp_tz_min,
                                                                    local_tz_factor))) {
                        LOG_WARN("matching timezone hour failed, for 'TZR'. the error is ignored.",
                            K(ret),
                            K(hour_expected_len));
                      } else if (OB_FAIL(ObDFMLimit::TIMEZONE_MIN_ABS.validate(abs(tmp_tz_min)))) {
                        LOG_WARN("not valid timezone hour", K(ret), K(tmp_tz_min));
                      } else if (OB_UNLIKELY(parsed_elem_len != hour_expected_len + local_parsed_len + 1)) {
                        ret = OB_INVALID_DATE_VALUE;  // invalid time zone
                      } else {
                        has_digit_tz_in_TZR = true;
                        tz_hour = tmp_tz_hour;
                        tz_min = tmp_tz_min;
                      }
                    }
                  }
                } else {  // case2: strings
                  ObString tzr_str;
                  parsed_elem_len = OB_MAX_TZ_NAME_LEN - 1;
                  if (OB_FAIL(ObDFMUtil::match_chars_until_space(ctx, tzr_str, parsed_elem_len))) {
                    LOG_WARN("failed to match tzr", K(ret));
                  } else {
                    MEMCPY(ob_time.tz_name_, tzr_str.ptr(), tzr_str.length());
                    ob_time.tz_name_[tzr_str.length()] = '\0';
                    ob_time.is_tz_name_valid_ = true;
                  }
                }
              }
              break;
            }

            case ObDFMFlag::TZD: {
              if (OB_UNLIKELY(elem_flags.has_member(ObDFMFlag::TZH) || elem_flags.has_member(ObDFMFlag::TZM))) {
                ret = OB_ERR_NOT_A_VALID_TIME_ZONE;
                LOG_WARN("tzh tzm and tzr tzd can not appear at the same time", K(ret));
              } else if (OB_UNLIKELY(has_digit_tz_in_TZR)) {
                ret = OB_INVALID_DATE_FORMAT;
                LOG_WARN("digit TZR with TZD is invalid", K(ret));
              } else {
                ObString tzd_str;
                parsed_elem_len = OB_MAX_TZ_ABBR_LEN - 1;
                if (OB_FAIL(ObDFMUtil::match_chars_until_space(ctx, tzd_str, parsed_elem_len))) {
                  LOG_WARN("failed to match tzd", K(ret));
                } else {
                  MEMCPY(ob_time.tzd_abbr_, tzd_str.ptr(), tzd_str.length());
                  ob_time.tzd_abbr_[tzd_str.length()] = '\0';
                  // ob_time.is_tz_name_valid_ = true;
                }
              }
              break;
            }

            case ObDFMFlag::J: {
              const int32_t base_julian_day = 2378497;  // julian day of 1800-01-01
              const int32_t base_date = -62091;         // ob_time.parts_[DT_DATE] of 1800-01-01
              int32_t julian_day = 0;
              int32_t ob_time_date = 0;
              ObTime tmp_ob_time;
              if (OB_FAIL(ObDFMUtil::match_int_value(ctx, expected_elem_len, parsed_elem_len, julian_day))) {
                LOG_WARN("failed to match int value", K(ret));
              } else if (OB_FAIL(ObDFMLimit::JULIAN_DATE.validate(julian_day))) {
                LOG_WARN("julian_day must between 1 and 5373484", K(ret), K(julian_day));
              } else if (julian_day < base_julian_day) {
                ret = OB_NOT_SUPPORTED;
                LOG_WARN("julian day of date before 1800-01-01 not supported", K(ret));
              } else if (FALSE_IT(ob_time_date = julian_day - base_julian_day + base_date)) {
              } else if (OB_FAIL(date_to_ob_time(ob_time_date, tmp_ob_time))) {
                LOG_WARN("date to ob time failed", K(ret));
              } else if (OB_FAIL(set_ob_time_year_may_conflict(ob_time,
                             julian_year_value,
                             tmp_ob_time.parts_[DT_YEAR],
                             tmp_ob_time.parts_[DT_YEAR],
                             true /* overwrite */))) {
                LOG_WARN("set ob_time_year conflict", K(ret));
              } else {
                yday_temp_value = tmp_ob_time.parts_[DT_YDAY];
              }
              break;
            }

            case ObDFMFlag::HH:
            case ObDFMFlag::HH12: {
              int32_t hour = 0;
              if (OB_FAIL(ObDFMUtil::match_int_value(ctx, expected_elem_len, parsed_elem_len, hour))) {
                LOG_WARN("failed to match int value", K(ret));
              } else if (OB_FAIL(ObDFMLimit::HOUR12.validate(hour))) {
                LOG_WARN("hour must be between 1 and 12", K(ret), K(hour));
              } else if (!ObDFMUtil::elem_has_meridian_indicator(elem_flags)) {
                ret = set_ob_time_part_may_conflict(ob_time, conflict_part_bitset, DT_HOUR, hour);
              } else {
                hh12_temp_value = hour;
              }
              break;
            }
            case ObDFMFlag::HH24: {
              int32_t hour = 0;
              if (OB_FAIL(ObDFMUtil::match_int_value(ctx, expected_elem_len, parsed_elem_len, hour))) {
                LOG_WARN("failed to match int value", K(ret));
              } else if (OB_FAIL(ObDFMLimit::HOUR24.validate(hour))) {
                LOG_WARN("hour must be between 0 and 23", K(ret), K(hour));
              } else if (OB_UNLIKELY(ObDFMUtil::elem_has_meridian_indicator(elem_flags))) {
                ret = OB_INVALID_MERIDIAN_INDICATOR_USE;
                LOG_WARN("HH24 appears with meridian indicator", K(ret), K(format));
              } else {
                // may conflict with SSSSS
                ret = set_ob_time_part_may_conflict(ob_time, conflict_part_bitset, DT_HOUR, hour);
              }
              break;
            }
            case ObDFMFlag::MI: {
              int32_t min = 0;
              if (OB_FAIL(ObDFMUtil::match_int_value(ctx, expected_elem_len, parsed_elem_len, min))) {
                LOG_WARN("failed to match int value", K(ret));
              } else if (OB_FAIL(ObDFMLimit::MINUTE.validate(min))) {
                LOG_WARN("minutes must be between 0 and 59", K(ret), K(min));
              } else {
                // may conflict with SSSSS
                ret = set_ob_time_part_may_conflict(ob_time, conflict_part_bitset, DT_MIN, min);
              }
              break;
            }
            case ObDFMFlag::MM: {
              int32_t mon = 0;
              if (OB_FAIL(ObDFMUtil::match_int_value(ctx, expected_elem_len, parsed_elem_len, mon))) {
                LOG_WARN("failed to match int value", K(ret));
              } else if (OB_FAIL(ObDFMLimit::MONTH_DAY.validate(mon))) {
                LOG_WARN("not a valid month", K(ret), K(mon));
              } else {
                // may conflict with DDD
                ret = set_ob_time_part_directly(ob_time, conflict_part_bitset, DT_MON, mon);
              }
              break;
            }
            case ObDFMFlag::MON:
            case ObDFMFlag::MONTH: {
              int32_t mon = 0;
              for (mon = ObDFMLimit::MONTH.min_val_; mon <= ObDFMLimit::MONTH.max_val_; ++mon) {
                const ObTimeConstStr& mon_str =
                    (elem.elem_flag_ == ObDFMFlag::MONTH) ? MON_NAMES[mon] : MON_ABBR_NAMES[mon];
                if (ObDFMUtil::match_pattern_ignore_case(ctx, mon_str)) {
                  parsed_elem_len = mon_str.len_;
                  break;
                }
              }
              if (OB_FAIL(ObDFMLimit::MONTH.validate(mon))) {
                LOG_WARN("not a valid month", K(ret), K(mon));
              } else {
                // may conflict with DDD
                ret = set_ob_time_part_directly(ob_time, conflict_part_bitset, DT_MON, mon);
              }
              break;
            }
            case ObDFMFlag::AM:
            case ObDFMFlag::PM:
            case ObDFMFlag::AM2:
            case ObDFMFlag::PM2: {
              bool is_with_dot = (ObDFMFlag::AM2 == elem.elem_flag_ || ObDFMFlag::PM2 == elem.elem_flag_);
              parsed_elem_len =
                  is_with_dot ? ObDFMFlag::PATTERN[ObDFMFlag::AM2].len_ : ObDFMFlag::PATTERN[ObDFMFlag::AM].len_;
              bool is_am = ObDFMUtil::match_pattern_ignore_case(
                  ctx, is_with_dot ? ObDFMFlag::PATTERN[ObDFMFlag::AM2] : ObDFMFlag::PATTERN[ObDFMFlag::AM]);
              bool is_pm = ObDFMUtil::match_pattern_ignore_case(
                  ctx, is_with_dot ? ObDFMFlag::PATTERN[ObDFMFlag::PM2] : ObDFMFlag::PATTERN[ObDFMFlag::PM]);
              if (OB_UNLIKELY(!is_am && !is_pm)) {
                ret = OB_ERR_AM_OR_PM_REQUIRED;
                LOG_WARN("AM/A.M. or PM/P.M. required", K(ret));
              } else {
                is_after_noon = is_pm;
              }
              break;
            }
            case ObDFMFlag::RR:
            case ObDFMFlag::RRRR: {
              int32_t round_year = 0;
              int32_t conflict_check_year = 0;
              if (OB_FAIL(ObDFMUtil::match_int_value(ctx, 4, parsed_elem_len, round_year))) {
                LOG_WARN("failed to match int value", K(ret));
              } else if (parsed_elem_len > 2) {
                conflict_check_year = round_year;
                // do nothing
              } else {
                conflict_check_year = round_year;
                int32_t first_two_digits_of_current_year = (ob_time.parts_[DT_YEAR] / 100) % 100;
                int32_t last_two_digits_of_current_year = ob_time.parts_[DT_YEAR] % 100;
                if (round_year < 50) {  // 0~49
                  if (last_two_digits_of_current_year < 50) {
                    round_year += first_two_digits_of_current_year * 100;
                  } else {
                    round_year += (first_two_digits_of_current_year + 1) * 100;
                  }
                } else if (round_year < 100) {  // 50~99
                  if (last_two_digits_of_current_year < 50) {
                    round_year += (first_two_digits_of_current_year - 1) * 100;
                  } else {
                    round_year += first_two_digits_of_current_year * 100;
                  }
                }
              }
              if (OB_SUCC(ret)) {
                if (OB_FAIL(ObDFMLimit::YEAR.validate(round_year))) {  // TODO : negetive year number
                  LOG_WARN("(full) year must be between -4713 and +9999, and not be 0", K(ret), K(round_year));
                } else if (OB_FAIL(set_ob_time_year_may_conflict(ob_time,
                                                                 julian_year_value,
                                                                 conflict_check_year,
                                                                 round_year,
                                                                 false /* overwrite */))) {
                  LOG_WARN("set ob_time_year conflict", K(ret));
                }
              }
              break;
            }
            case ObDFMFlag::SS: {
              int32_t sec = 0;
              if (OB_FAIL(ObDFMUtil::match_int_value(ctx, expected_elem_len, parsed_elem_len, sec))) {
                LOG_WARN("failed to match int value", K(ret));
              } else if (OB_FAIL(ObDFMLimit::SECOND.validate(sec))) {
                LOG_WARN("seconds must be between 0 and 59", K(ret), K(sec));
              } else {
                ret = set_ob_time_part_may_conflict(ob_time, conflict_part_bitset, DT_SEC, sec);
              }
              break;
            }
            case ObDFMFlag::SSSSS: {
              int32_t sec_past_midnight = 0;
              if (OB_FAIL(ObDFMUtil::match_int_value(ctx, expected_elem_len, parsed_elem_len, sec_past_midnight))) {
                LOG_WARN("failed to match int value", K(ret));
              } else if (OB_FAIL(ObDFMLimit::SECS_PAST_MIDNIGHT.validate(sec_past_midnight))) {
                LOG_WARN("seconds in day must be between 0 and 86399", K(ret), K(sec_past_midnight));
              } else {
                int32_t secs = sec_past_midnight % static_cast<int32_t>(SECS_PER_MIN);
                int32_t mins =
                    (sec_past_midnight / static_cast<int32_t>(SECS_PER_MIN)) % static_cast<int32_t>(MINS_PER_HOUR);
                int32_t hours =
                    sec_past_midnight / static_cast<int32_t>(SECS_PER_MIN) / static_cast<int32_t>(MINS_PER_HOUR);
                if (OB_FAIL(set_ob_time_part_may_conflict(ob_time, conflict_part_bitset, DT_SEC, secs))) {
                  LOG_WARN("set ob time conflict", K(ret), K(secs), K(ob_time));
                } else if (OB_FAIL(set_ob_time_part_may_conflict(ob_time, conflict_part_bitset, DT_MIN, mins))) {
                  LOG_WARN("set ob time conflict", K(ret), K(mins), K(ob_time));
                } else if (OB_FAIL(set_ob_time_part_may_conflict(ob_time, conflict_part_bitset, DT_HOUR, hours))) {
                  LOG_WARN("set ob time conflict", K(ret), K(hours), K(ob_time));
                }
              }
              break;
            }
            case ObDFMFlag::YGYYY: {
              int32_t years = 0;
              if (OB_FAIL(ObDFMUtil::match_int_value_with_comma(ctx, expected_elem_len, parsed_elem_len, years))) {
                LOG_WARN("failed to match int value", K(ret));
              } else if (OB_FAIL(ObDFMLimit::YEAR.validate(years))) {
                LOG_WARN("(full) year must be between -4713 and +9999, and not be 0", K(ret), K(years));
              } else if (OB_FAIL(set_ob_time_year_may_conflict(ob_time,
                                                               julian_year_value,
                                                               years,
                                                               years,
                                                               false /* overwrite */))) {
                LOG_WARN("set ob_time_year conflict", K(ret));
              }
              break;
            }
            case ObDFMFlag::SYYYY:
            case ObDFMFlag::YYYY:
            case ObDFMFlag::YYY:
            case ObDFMFlag::YY:
            case ObDFMFlag::Y: {
              int32_t years = 0;
              int32_t conflict_check_year = 0;
              int32_t sign = 1;
              if (ObDFMFlag::SYYYY == elem.elem_flag_) {
                if (ObDFMUtil::is_sign_char(ctx.cur_ch_[0])) {
                  sign = ('-' == ctx.cur_ch_[0] ? -1 : 1);
                  ctx.update(1);
                } else if (part_blank1_len + part_sep_len > format_sep_len &&
                           ObDFMUtil::is_sign_char(ctx.cur_ch_[-1])) {
                  sign = ('-' == ctx.cur_ch_[-1] ? -1 : 1);
                }
              }
              if (OB_UNLIKELY(!ctx.is_valid())) {
              } else if (OB_FAIL(ObDFMUtil::match_int_value(ctx, expected_elem_len, parsed_elem_len, years, sign))) {
                LOG_WARN("failed to match int value", K(ret));
              }
              if (OB_SUCC(ret)) {
                conflict_check_year = years;
                if (expected_elem_len < 4) {
                  years += (ob_time.parts_[DT_YEAR] / static_cast<int32_t>(power_of_10[parsed_elem_len])) *
                           static_cast<int32_t>(power_of_10[parsed_elem_len]);
                }
                if (OB_FAIL(ObDFMLimit::YEAR.validate(years))) {
                  LOG_WARN("(full) year must be between -4713 and +9999, and not be 0", K(ret), K(years));
                } else if (OB_FAIL(set_ob_time_year_may_conflict(ob_time,
                                                                 julian_year_value,
                                                                 conflict_check_year,
                                                                 years,
                                                                 false /* overwrite */))) {
                  LOG_WARN("set ob_time_year conflict", K(ret));
                }
              }
              break;
            }
            case ObDFMFlag::X: {
              if (OB_UNLIKELY('.' != ctx.cur_ch_[0])) {
                ignore_fs_flag = true;
                // revert equal to 'X' not exist
                ctx.revert(part_blank1_len + part_sep_len + part_blank2_len);
              } else {
                parsed_elem_len = 1;
              }
              break;
            }

            case ObDFMFlag::CC:
            case ObDFMFlag::SCC:
            case ObDFMFlag::IW:
            case ObDFMFlag::W:
            case ObDFMFlag::WW:
            case ObDFMFlag::YEAR:
            case ObDFMFlag::SYEAR:
            case ObDFMFlag::Q:
            case ObDFMFlag::I:
            case ObDFMFlag::IY:
            case ObDFMFlag::IYY:
            case ObDFMFlag::IYYY: {
              ret = OB_ERR_FORMAT_CODE_CANNOT_APPEAR;
              LOG_WARN("element can not appear", K(ret), K(elem));
              break;
            }
            default: {
              ret = OB_INVALID_DATE_FORMAT;
              LOG_WARN("unsupport element", K(ret), K(elem));
              break;
            }
          }  // end switch

          if (OB_SUCC(ret)) {
            ctx.update(parsed_elem_len);
          }
        }  // end if

        if (OB_FAIL(ret)) {
          LOG_WARN("failed to convert string to ob time by oracle dfm", K(ret), K(elem), K(ctx));
        } else {
          LOG_DEBUG("DFM DEBUG: finish element", K(elem), K(ctx));
        }
      }  // end for

      // check if the unprocessed elems has permission to be omitted
      if (OB_SUCC(ret)) {
        for (; elem_idx < dfm_elems.count(); ++elem_idx) {
          if (OB_UNLIKELY(!ObDFMUtil::is_element_can_omit(dfm_elems[elem_idx]))) {
            ret = OB_ERR_INPUT_VALUE_NOT_LONG_ENOUGH;
            LOG_WARN("input value not long enough for date format", K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        // all elems has finished, is there anything else in str? the rest must be separators, do check.
        int64_t str_remain_sep_len = 0;
        while (str_remain_sep_len < ctx.remain_len_ && ObDFMUtil::is_split_char(ctx.cur_ch_[str_remain_sep_len])) {
          str_remain_sep_len++;
        }
        ctx.update(str_remain_sep_len);
        if (ctx.remain_len_ > 0) {
          ret = OB_INVALID_DATE_FORMAT_END;
          LOG_WARN("input value has not finished yet", K(ctx.remain_len_), K(format), K(str), K(ret));
        }
      }

      // after noon conflict: AM PM vs HH12 HH
      if (OB_SUCC(ret)) {
        if (hh12_temp_value != ZERO_TIME) {
          // when  hour value, varied by meridian indicators
          // if HH12 = 12, when meridian indicator 'AM' exists, the real time is hour = 0
          // if HH12 = 12, when meridian indicator 'PM' exists, the real time is hour = 12
          if (is_after_noon) {
            ret = set_ob_time_part_may_conflict(ob_time, conflict_part_bitset, DT_HOUR, hh12_temp_value % 12 + 12);
          } else {
            ret = set_ob_time_part_may_conflict(ob_time, conflict_part_bitset, DT_HOUR, hh12_temp_value % 12);
          }
        }
      }

      // before christ conflict: BC AD vs YEAR   //TODO : change this when ob_time support negetive years
      if (OB_SUCC(ret)) {
        if (is_before_christ) {
          ret = OB_ERR_INVALID_YEAR_VALUE;
          LOG_WARN("before christ is not supported now!", K(ret));
        }
      }

      // year cannot changed after this line
      // feed/validate yday + YEAR to MON and DAY
      if (OB_SUCC(ret)) {
        if (yday_temp_value != ZERO_DATE) {
          int32_t month = 0;
          int32_t day = 0;
          if (OB_FAIL(get_day_and_month_from_year_day(yday_temp_value, ob_time.parts_[DT_YEAR], month, day))) {
            LOG_WARN("failed to get day and month from year day", K(ret), K(yday_temp_value), K(ob_time));
          } else if (OB_FAIL(set_ob_time_part_may_conflict(ob_time, conflict_part_bitset, DT_MON, month))) {
            LOG_WARN("failed to set ob time part with conflict", K(ret), K(month), K(ob_time));
          } else if (OB_FAIL(set_ob_time_part_may_conflict(ob_time, conflict_part_bitset, DT_MDAY, day))) {
            LOG_WARN("failed to set ob time part with conflict", K(ret), K(day), K(ob_time));
          }
        }
      }

      // calc and validate: YDAY WDAY vs YEAR MON DAY
      if (OB_SUCC(ret)) {
        if (OB_FAIL(validate_oracle_date(ob_time))) {
          LOG_WARN("date is invalid or out of range", K(ret), K(str));
        } else {
          // ob_time_to_date func is to calc YDAY and WDAY and return DATE
          date_temp_value = ob_time_to_date(ob_time);  // TODO: shanting
          if (yday_temp_value != ZERO_DATE && OB_UNLIKELY(ob_time.parts_[DT_YDAY] != yday_temp_value)) {
            ret = OB_ERR_DAY_OF_YEAR_CONFLICTS_WITH_JULIAN_DATE;
          } else if (wday_temp_value != ZERO_DATE && OB_UNLIKELY(ob_time.parts_[DT_WDAY] != wday_temp_value)) {
            ret = OB_ERR_DAY_OF_WEEK_CONFLICTS_WITH_JULIAN_DATE;
          } else {
            ob_time.parts_[DT_DATE] = date_temp_value;
          }
        }
      }

      // for time zone info
      if (OB_SUCC(ret) && ob_is_timestamp_tz(target_type)) {
        if (ob_time.is_tz_name_valid_) {  // A. timezone defined by names
          if (ob_time.get_tz_name_str().empty()) {
            // string not contains TZR, use tz_name of sessiontimezone as default value.
            int64_t pos = 0;
            if (OB_FAIL(cvrt_ctx.tz_info_->timezone_to_str(ob_time.tz_name_, OB_MAX_TZ_NAME_LEN, pos))) {
              LOG_WARN("print tz name failed", K(ret));
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(calc_tz_offset_by_tz_name(cvrt_ctx, ob_time))) {
            LOG_WARN("calc timezone offset failed", K(ret));
          }
        } else {  // B. timezone defined by time zone hour and minute
          int32_t tz_offset_value = 0;

          if (elem_flags.has_member(ObDFMFlag::TZH)) {
            bool has_tzh_value = (temp_tzh_value >= 0);
            bool has_tzm_value = (temp_tzm_value >= 0);
            if (OB_UNLIKELY(!has_tzh_value && has_tzm_value)) {
              ret = OB_INVALID_DATE_VALUE;
              LOG_WARN("only TZM match, TZH is not found", K(ret));
            } else if (!has_tzh_value && !has_tzm_value) {
              // do nothing
            } else if (has_tzh_value && !has_tzm_value) {
              tz_hour = temp_tz_factor * temp_tzh_value;
              tz_min = temp_tz_factor * abs(tz_min);
            } else {
              tz_hour = temp_tz_factor * temp_tzh_value;
              tz_min = temp_tz_factor * temp_tzm_value;
            }
          } else if (elem_flags.has_member(ObDFMFlag::TZR)) {
            // do nothing
          } else {
            // no time zone info in elem_flags
          }

          // calc offset
          if (OB_SUCC(ret)) {
            if (OB_UNLIKELY(tz_hour * tz_min < 0)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("tz_hour and tz_min cantains counter arithmetic symbols", K(ret));
            } else {
              tz_offset_value = static_cast<int32_t>(tz_hour * MINS_PER_HOUR + tz_min);
            }
          }

          // final validate
          if (OB_SUCC(ret)) {
            if (OB_UNLIKELY(!ObOTimestampData::is_valid_offset_min(tz_offset_value))) {
              ret = OB_INVALID_DATE_VALUE;
              LOG_WARN("validate timezone offset failed", K(ret), K(tz_offset_value));
            } else {
              ob_time.parts_[DT_OFFSET_MIN] = tz_offset_value;
            }
          }
        }
      }

      if (OB_FAIL(ret)) {
        LOG_WARN("failed to convert string to ob_time",
            K(format),
            K(ob_time),
            K(conflict_part_bitset),
            K(yday_temp_value),
            K(wday_temp_value),
            K(date_temp_value),
            K(hh12_temp_value),
            K(tz_hour),
            K(tz_min),
            K(is_after_noon),
            K(is_before_christ));
      } else {
        LOG_DEBUG("convert from string to ob_time",
            K(format),
            K(ob_time),
            K(conflict_part_bitset),
            K(yday_temp_value),
            K(wday_temp_value),
            K(date_temp_value),
            K(hh12_temp_value),
            K(tz_hour),
            K(tz_min),
            K(is_after_noon),
            K(is_before_christ));
      }
    }  // end if
  }

  return ret;
}


int ObTimeConverter::usec_to_ob_time(int64_t usec, ObTime &ob_time)
{
  int ret = OB_SUCCESS;
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

int ObTimeConverter::otimestamp_to_ob_time(const ObObjType type,
                                           const ObOTimestampData &ot_data,
                                           const ObTimeZoneInfo *tz_info,
                                           ObTime &ob_time,
                                           const bool store_utc_time /*true*/)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ob_is_otimestamp_type(type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("it is not otimestamp type", K(type), K(ret));
  } else if (ot_data.is_null_value()) {
    // NOTE: Any arithmetic expression containing a null always evaluates to null.
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("it is null otimestamp, should not arrive here", K(type), K(ot_data), K(ret));
  } else if (ObTimestampTZType == type) {
    int32_t offset_min = 0;
    int64_t usec = ot_data.time_us_;
    if (OB_FAIL(extract_offset_from_otimestamp(ot_data, tz_info, offset_min, ob_time))) {
      LOG_WARN("failed to extract_offset_from_otimestamp", K(ret));
    } else {
      usec += (store_utc_time ? 0 : offset_min * SECS_PER_MIN * USECS_PER_SEC);
      int64_t nsec = 0;
      if (OB_FAIL(usec_to_ob_time(usec, ob_time))) {
        LOG_WARN("failed to convert usec part to obtime", K(ret), K(usec));
      } else if (OB_UNLIKELY(
                   (nsec = ob_time.parts_[DT_USEC] * NSECS_PER_USEC + ot_data.time_ctx_.tail_nsec_) > INT32_MAX)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("nsec is overflow", K(nsec), K(ret));
      } else {
        ob_time.parts_[DT_USEC] = static_cast<int32_t>(nsec);
        ob_time.mode_ |= DT_TYPE_ORACLE;
        ob_time.mode_ |= DT_TYPE_TIMEZONE;
        if (store_utc_time) {
          ob_time.mode_ |= DT_TYPE_STORE_UTC;
        } else {
          ob_time.mode_ &= ~(DT_TYPE_STORE_UTC);
        }
      }
    }
  } else {
    int64_t usec = ot_data.time_us_;
    int64_t nsec = 0;
    if (ObTimestampLTZType == type && !store_utc_time) {
      if (OB_FAIL(add_timezone_offset(tz_info, usec))) {
        LOG_WARN("failed to adjust value with time zone offset", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(usec_to_ob_time(usec, ob_time))) {
      LOG_WARN("failed to convert usec part to obtime", K(ret), K(usec));
    } else if (OB_UNLIKELY(
                 (nsec = ob_time.parts_[DT_USEC] * NSECS_PER_USEC + ot_data.time_ctx_.tail_nsec_) > INT32_MAX)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("nsec is overflow", K(nsec), K(ret));
    } else {
      ob_time.parts_[DT_USEC] = static_cast<int32_t>(nsec);
      ob_time.mode_ |= DT_TYPE_ORACLE;
      if (ObTimestampNanoType == type || !store_utc_time) {
        ob_time.mode_ &= ~(DT_TYPE_STORE_UTC);
      } else {
        ob_time.mode_ |= DT_TYPE_STORE_UTC;
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_DEBUG("succ to otimestamp_to_ob_time", K(ret), K(ot_data), K(type), K(ob_time), K(lbt()));
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
  if (OB_UNLIKELY(n <= 0 || target < 0 || target > 999999999)) {
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

int ObTimeConverter::get_day_and_month_from_year_day(const int32_t yday,
                                                     const int32_t year,
                                                     int32_t &month,
                                                     int32_t &day)
{
  int ret = OB_SUCCESS;
  int32_t leap_year = IS_LEAP_YEAR(year);
  if (OB_UNLIKELY(yday > DAYS_UNTIL_MON[leap_year][12])) {
    ret = OB_ERR_INVALID_DAY_OF_YEAR_VALUE;
  } else {
    bool stop_flag = false;
    for (int32_t i = ObDFMLimit::MONTH.min_val_; !stop_flag && i <= ObDFMLimit::MONTH.max_val_; ++i) {
      if (yday <= DAYS_UNTIL_MON[leap_year][i]) {
        month = i;
        day = yday - DAYS_UNTIL_MON[leap_year][i - 1];
        stop_flag = true;
      }
    }
  }
  return ret;
}

int ObTimeConverter::set_ob_time_year_may_conflict(ObTime &ob_time,
                                                   int32_t &julian_year_value,
                                                   int32_t check_year,
                                                   int32_t set_year,
                                                   bool overwrite)
{
  int ret = OB_SUCCESS;
  if (ZERO_DATE != julian_year_value) {
    if (julian_year_value != check_year) {
      ret = OB_ERR_YEAR_CONFLICTS_WITH_JULIAN_DATE;
      LOG_WARN("year conflicts with Julian date", K(ret), K(julian_year_value), K(check_year));
    } else if (overwrite) {
      ob_time.parts_[DT_YEAR] = set_year;
    }
  } else {
    ob_time.parts_[DT_YEAR] = set_year;
    julian_year_value = check_year;
  }
  return ret;
}

int ObTimeConverter::ob_time_to_str_format(const ObTime &ob_time,
                                           const ObString &format,
                                           char *buf,
                                           int64_t buf_len,
                                           int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(format.ptr())
      || OB_ISNULL(buf)
      || OB_UNLIKELY(format.length() <= 0
      || buf_len <= 0)) {
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

int check_and_get_tz_info(ObTime &ob_time,
                          const ObTimeConvertCtx &cvrt_ctx,
                          const ObTimeZoneInfo *&tz_info,
                          ObTimeZoneInfoPos *&literal_tz_info,
                          ObTZInfoIDPosMap *&tz_id_pos_map)
{
  int ret = OB_SUCCESS;
  ObTZInfoMap *tz_info_map = NULL;
  if (OB_UNLIKELY(ob_time.is_tz_name_valid_)) {  // use string literal tz_inifo
    // tz_info_ is expected to be not null, but sometimes we don't set it.
    if (NULL == cvrt_ctx.tz_info_) {
      if (HAS_TYPE_ORACLE(ob_time.mode_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("tz_info_ is NULL", K(ret));
      }
    } else if (OB_ISNULL(tz_info_map = const_cast<ObTZInfoMap*>(cvrt_ctx.tz_info_->get_tz_info_map()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tz_info_map is NULL", K(ret));
    } else if (OB_FAIL(tz_info_map->get_tz_info_by_name(ob_time.get_tz_name_str(), literal_tz_info))) {
      LOG_WARN("fail to get_tz_info_by_name", K(ob_time), K(ret));
      tz_info_map->id_map_.revert(literal_tz_info);
      literal_tz_info = NULL;
    } else {
      literal_tz_info->set_error_on_overlap_time(cvrt_ctx.tz_info_->is_error_on_overlap_time());
      tz_info = literal_tz_info;
      tz_id_pos_map = &(tz_info_map->id_map_);
    }
  } else {  // use session tz_info
    tz_info = cvrt_ctx.tz_info_;
  }
  return ret;
}

/**
 * @brief calcs tz offset value by time zone name from the input ob_time, fills the result back to ob_time
 * @param in:         cvrt_ctx
 * @param in, out:    ob_time
 * @return
 */
int ObTimeConverter::calc_tz_offset_by_tz_name(const ObTimeConvertCtx &cvrt_ctx, ObTime &ob_time)
{
  int ret = OB_SUCCESS;
  int64_t usec = ob_time.parts_[DT_DATE] * USECS_PER_DAY + ob_time_to_time(ob_time);
  const ObTimeZoneInfo* tz_info = NULL;
  ObTimeZoneInfoPos* literal_tz_info = NULL;
  ObTZInfoIDPosMap* tz_id_pos_map = NULL;
  int32_t tz_id = OB_INVALID_INDEX;
  int32_t tran_type_id = OB_INVALID_INDEX;
  int32_t offset_min = 0;
  if (OB_FAIL(check_and_get_tz_info(ob_time, cvrt_ctx, tz_info, literal_tz_info, tz_id_pos_map))) {
    LOG_WARN("fail to check time zone info", K(ob_time));
  } else if (OB_ISNULL(tz_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tz_info shoule not be null", K(ret));
  } else if (OB_FAIL(sub_timezone_offset(*tz_info,
                                         ob_time.get_tzd_abbr_str(),
                                         usec,
                                         offset_min,
                                         tz_id,
                                         tran_type_id))) {
    LOG_WARN("failed to adjust value with time zone offset", K(ret));
  } else {
    ob_time.parts_[DT_OFFSET_MIN] = offset_min;
    ob_time.time_zone_id_ = tz_id;
    ob_time.transition_type_id_ = tran_type_id;
  }

  if (NULL != literal_tz_info && NULL != tz_id_pos_map) {
    tz_id_pos_map->revert(literal_tz_info);
    tz_id_pos_map = NULL;
    literal_tz_info = NULL;
  }
  return ret;
}   

int ObTimeConverter::ob_time_to_utc(const ObObjType obj_type, const ObTimeConvertCtx &cvrt_ctx, ObTime &ob_time)
{
  int ret = OB_SUCCESS;
  if (ObTimestampNanoType == obj_type) {
    // ignore offset
  } else if (HAS_TYPE_STORE_UTC(ob_time.mode_)) {
    // has store utc time
  } else {
    int64_t usec = ob_time.parts_[DT_DATE] * USECS_PER_DAY + ob_time_to_time(ob_time);
    const int64_t old_usec = usec;
    if (HAS_TYPE_TIMEZONE(ob_time.mode_)) {
      usec -= (ob_time.parts_[DT_OFFSET_MIN] * USECS_PER_MIN);
    } else {
      int32_t offset_min = 0;
      int32_t tz_id = OB_INVALID_INDEX;
      int32_t tran_type_id = OB_INVALID_INDEX;
      if (OB_ISNULL(cvrt_ctx.tz_info_)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("tz_info is null", K(ret));
      } else if (OB_FAIL(sub_timezone_offset(*cvrt_ctx.tz_info_, ObString(), usec, offset_min, tz_id, tran_type_id))) {
        LOG_WARN("failed to adjust value with time zone offset", K(ret));
      } else {
        ob_time.parts_[DT_OFFSET_MIN] = offset_min;
        ob_time.time_zone_id_ = tz_id;
        ob_time.transition_type_id_ = tran_type_id;
      }
    }

    if (OB_SUCC(ret)) {
      if (old_usec == usec) {
        // no need convert
      } else {
        const int32_t nanosecond = ob_time.parts_[DT_USEC];
        if (OB_FAIL(usec_to_ob_time(usec, ob_time))) {
          LOG_WARN("failed to usec_to_ob_time", K(ret));
        } else {
          ob_time.parts_[DT_USEC] = nanosecond;
          ob_time.parts_[DT_DATE] = ob_time_to_date(ob_time);
        }
      }
    }

    if (OB_SUCC(ret)) {
      ob_time.mode_ |= DT_TYPE_STORE_UTC;
    }
  }
  return ret;
}

// check parts of obtime before print timestamp in oracle mode
bool ObTimeConverter::valid_oracle_year(const ObTime &ob_time)
{
  int ret = true;
  if (ob_time.parts_[DT_YEAR] < 0 || ob_time.parts_[DT_YEAR] > 9999) {
    ret = false;
  }
  return ret;
}

void ObTimeConverter::calc_iso_week(bool &is_iso_week_calced,
                                    int32_t &iso_week,
                                    const ObTime &ob_time,
                                    ObDTMode mode,
                                    int32_t &delta)
{
  if (!is_iso_week_calced) {
    iso_week = ob_time_to_week(ob_time, mode, delta);
    is_iso_week_calced = true;
  }
  return;
}

int ObTimeConverter::ob_time_to_str_oracle_dfm(const ObTime &ob_time,
                                               ObScale scale,
                                               const ObString &format,
                                               char *buf,
                                               int64_t buf_len,
                                               int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0) || OB_UNLIKELY(format.empty()) ||
      OB_UNLIKELY(scale > MAX_SCALE_FOR_ORACLE_TEMPORAL)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(format), K(buf), K(buf_len), K(ob_time), K(scale));
  } else if (!valid_oracle_year(ob_time)) {
    ret = OB_ERR_DATETIME_INTERVAL_INTERNAL_ERROR;
    LOG_WARN("invalid oracle timestamp", K(ret), K(ob_time));
  } else {
    if (scale < 0) {
      scale = DEFAULT_SCALE_FOR_ORACLE_FRACTIONAL_SECONDS;
    }

    int32_t iso_week = 0;
    int32_t iso_year_delta = 0;
    bool is_iso_week_calced = false;
    // avoid repeat calc iso_week. use inner func instead of c++11 lambda

    const char *const format_begin_ptr = format.ptr();
    int64_t last_elem_end_pos = 0;
    ObSEArray<ObDFMElem, ObDFMUtil::COMMON_ELEMENT_NUMBER> dfm_elems;

    // 1. parse element from format string
    if (OB_FAIL(ObDFMUtil::parse_datetime_format_string(format, dfm_elems))) {
      LOG_WARN("fail to parse oracle datetime format string", K(ret), K(format));
    }

    // 2. print each element
    for (int64_t i = 0; OB_SUCC(ret) && i < dfm_elems.count(); ++i) {
      ObDFMElem& elem = dfm_elems.at(i);

      // element is valid
      if (OB_UNLIKELY(!elem.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("element is invalid", K(ret), K(elem));
      }

      // print separate chars between elements
      if (OB_SUCC(ret)) {
        int64_t separate_chars_len = elem.offset_ - last_elem_end_pos;
        if (separate_chars_len > 0) {
          ret = databuff_printf(buf,
              buf_len,
              pos,
              "%.*s",
              static_cast<int32_t>(separate_chars_len),
              format_begin_ptr + last_elem_end_pos);
        }
        last_elem_end_pos = elem.offset_ + ObDFMFlag::PATTERN[elem.elem_flag_].len_;
      }

      // print current elem
      if (OB_SUCC(ret)) {
        switch (elem.elem_flag_) {
          case ObDFMFlag::AD:
          case ObDFMFlag::BC: {  // TODO : NLS_LANGUAGE
            const ObTimeConstStr &target_str =
                ob_time.parts_[DT_YEAR] > 0 ? ObDFMFlag::PATTERN[ObDFMFlag::AD] : ObDFMFlag::PATTERN[ObDFMFlag::BC];
            ret = ObDFMUtil::special_mode_sprintf(buf, buf_len, pos, target_str, elem.upper_case_mode_);
            break;
          }
          case ObDFMFlag::AD2:
          case ObDFMFlag::BC2: {  // TODO : NLS_LANGUAGE
            const ObTimeConstStr &str =
                ob_time.parts_[DT_YEAR] > 0 ? ObDFMFlag::PATTERN[ObDFMFlag::AD2] : ObDFMFlag::PATTERN[ObDFMFlag::BC2];
            ret = ObDFMUtil::special_mode_sprintf(buf, buf_len, pos, str, elem.upper_case_mode_);
            break;
          }
          case ObDFMFlag::CC: {
            ret = databuff_printf(buf, buf_len, pos, "%02d", (abs(ob_time.parts_[DT_YEAR]) + 99) / 100);
            break;
          }
          case ObDFMFlag::SCC: {
            char symbol = ob_time.parts_[DT_YEAR] > 0 ? ' ' : '-';
            ret = databuff_printf(buf, buf_len, pos, "%c%02d", symbol, (abs(ob_time.parts_[DT_YEAR]) + 99) / 100);
            break;
          }
          case ObDFMFlag::D: {
            ret = databuff_printf(buf, buf_len, pos, "%d", ob_time.parts_[DT_WDAY] % 7 + 1);
            break;
          }
          case ObDFMFlag::DAY: {  // TODO : NLS_LANGUAGE
            const ObTimeConstStr &day_str = WDAY_NAMES[ob_time.parts_[DT_WDAY]];
            ret = ObDFMUtil::special_mode_sprintf(
                buf, buf_len, pos, day_str, elem.upper_case_mode_, MAX_WDAY_NAME_LENGTH);
            break;
          }
          case ObDFMFlag::DD: {
            ret = databuff_printf(buf, buf_len, pos, "%02d", ob_time.parts_[DT_MDAY]);
            break;
          }
          case ObDFMFlag::DDD: {
            ret = databuff_printf(buf, buf_len, pos, "%03d", ob_time.parts_[DT_YDAY]);
            break;
          }
          case ObDFMFlag::DY: {  // TODO: 1. NLS_LANGUAGE
            const ObTimeConstStr &day_str = WDAY_ABBR_NAMES[ob_time.parts_[DT_WDAY]];
            ret = ObDFMUtil::special_mode_sprintf(buf, buf_len, pos, day_str, elem.upper_case_mode_);
            break;
          }
          case ObDFMFlag::DS: {  // TODO: 1. NLS_TERRITORY 2. NLS_LANGUAGE
            ret = databuff_printf(buf,
                                  buf_len,
                                  pos,
                                  "%02d/%02d/%d",
                                  ob_time.parts_[DT_MON],
                                  ob_time.parts_[DT_MDAY],
                                  ob_time.parts_[DT_YEAR]);
            break;
          }
          case ObDFMFlag::DL: {  // TODO: 1. NLS_DATE_FORMAT 2. NLS_TERRITORY 3. NLS_LANGUAGE
            const ObTimeConstStr &wday_str = WDAY_NAMES[ob_time.parts_[DT_WDAY]];
            const ObTimeConstStr &mon_str = MON_NAMES[ob_time.parts_[DT_MON]];
            ret = databuff_printf(buf,
                                  buf_len,
                                  pos,
                                  "%.*s, %.*s %02d, %d",
                                  wday_str.len_,
                                  wday_str.ptr_,
                                  mon_str.len_,
                                  mon_str.ptr_,
                                  ob_time.parts_[DT_MDAY],
                                  ob_time.parts_[DT_YEAR]);
            break;
          }
          case ObDFMFlag::FF: {
            // FF default FF9 in oracle
            int64_t scale = ObDFMFlag::FF9 - ObDFMFlag::FF1 + 1;
            if (OB_UNLIKELY(!HAS_TYPE_ORACLE(ob_time.mode_))) {
              ret = OB_INVALID_DATE_FORMAT;
            } else if (0 == scale) {
              // print nothing
            } else {
              ret = data_fmt_nd(buf,
                                buf_len,
                                pos,
                                scale,
                                ob_time.parts_[DT_USEC] / power_of_10[MAX_SCALE_FOR_ORACLE_TEMPORAL - scale]);
            }
            break;
          }
          case ObDFMFlag::FF1:
          case ObDFMFlag::FF2:
          case ObDFMFlag::FF3:
          case ObDFMFlag::FF4:
          case ObDFMFlag::FF5:
          case ObDFMFlag::FF6:
          case ObDFMFlag::FF7:
          case ObDFMFlag::FF8:
          case ObDFMFlag::FF9: {
            int64_t scale = elem.elem_flag_ - ObDFMFlag::FF1 + 1;
            if (OB_UNLIKELY(!HAS_TYPE_ORACLE(ob_time.mode_))) {
              ret = OB_INVALID_DATE_FORMAT;
            } else {
              ret = data_fmt_nd(buf,
                                buf_len,
                                pos,
                                scale,
                                ob_time.parts_[DT_USEC] / power_of_10[MAX_SCALE_FOR_ORACLE_TEMPORAL - scale]);
            }
            break;
          }
          case ObDFMFlag::HH:
          case ObDFMFlag::HH12: {
            int32_t h = ob_time.parts_[DT_HOUR] % 12;
            if (0 == h) {
              h = 12;
            }
            ret = databuff_printf(buf, buf_len, pos, "%02d", h);
            break;
          }
          case ObDFMFlag::HH24: {
            int32_t h = ob_time.parts_[DT_HOUR];
            ret = databuff_printf(buf, buf_len, pos, "%02d", h);
            break;
          }
          case ObDFMFlag::IW: {
            calc_iso_week(is_iso_week_calced, iso_week, ob_time, WEEK_MODE[3], iso_year_delta);
            ret = databuff_printf(buf, buf_len, pos, "%02d", iso_week);
            break;
          }
          case ObDFMFlag::J: {
            const int32_t base_julian_day = 2378497;  // julian day of 1800-01-01
            const int32_t base_date = -62091;         // ob_time.parts_[DT_DATE] of 1800-01-01
            if (ob_time.parts_[DT_DATE] < base_date) {
              ret = OB_NOT_SUPPORTED;
            } else {
              int32_t julian_day = base_julian_day + ob_time.parts_[DT_DATE] - base_date;
              ret = databuff_printf(buf, buf_len, pos, "%07d", julian_day);
            }
            break;
          }
          case ObDFMFlag::MI: {
            ret = databuff_printf(buf, buf_len, pos, "%02d", ob_time.parts_[DT_MIN]);
            break;
          }
          case ObDFMFlag::MM: {
            ret = databuff_printf(buf, buf_len, pos, "%02d", ob_time.parts_[DT_MON]);
            break;
          }
          case ObDFMFlag::MONTH: {
            const ObTimeConstStr &mon_str = MON_NAMES[ob_time.parts_[DT_MON]];
            ret =
                ObDFMUtil::special_mode_sprintf(buf, buf_len, pos, mon_str, elem.upper_case_mode_, MAX_MON_NAME_LENGTH);
            break;
          }
          case ObDFMFlag::MON: {
            const ObTimeConstStr &mon_str = MON_ABBR_NAMES[ob_time.parts_[DT_MON]];
            ret = ObDFMUtil::special_mode_sprintf(buf, buf_len, pos, mon_str, elem.upper_case_mode_);
            break;
          }
          case ObDFMFlag::AM:
          case ObDFMFlag::PM: {
            const ObTimeConstStr &str =
                ob_time.parts_[DT_HOUR] >= 12 ? ObDFMFlag::PATTERN[ObDFMFlag::PM] : ObDFMFlag::PATTERN[ObDFMFlag::AM];
            ret = ObDFMUtil::special_mode_sprintf(buf, buf_len, pos, str, elem.upper_case_mode_);
            break;
          }
          case ObDFMFlag::AM2:
          case ObDFMFlag::PM2: {
            const ObTimeConstStr &str =
                ob_time.parts_[DT_HOUR] >= 12 ? ObDFMFlag::PATTERN[ObDFMFlag::PM2] : ObDFMFlag::PATTERN[ObDFMFlag::AM2];
            ret = ObDFMUtil::special_mode_sprintf(buf, buf_len, pos, str, elem.upper_case_mode_);
            break;
          }
          case ObDFMFlag::Q: {
            ret = databuff_printf(buf, buf_len, pos, "%d", (ob_time.parts_[DT_MON] + 2) / 3);
            break;
          }
          case ObDFMFlag::RR: {
            ret = databuff_printf(buf, buf_len, pos, "%02d", (ob_time.parts_[DT_YEAR]) % 100);
            break;
          }
          case ObDFMFlag::RRRR: {
            ret = databuff_printf(buf, buf_len, pos, "%04d", ob_time.parts_[DT_YEAR]);
            break;
          }
          case ObDFMFlag::SS: {
            ret = databuff_printf(buf, buf_len, pos, "%02d", ob_time.parts_[DT_SEC]);
            break;
          }
          case ObDFMFlag::SSSSS: {
            ret = databuff_printf(buf,
                                  buf_len,
                                  pos,
                                  "%05d",
                                  ob_time.parts_[DT_HOUR] * 3600 + ob_time.parts_[DT_MIN] * 60 + ob_time.parts_[DT_SEC]);
            break;
          }
          case ObDFMFlag::WW: {  // the first complete week of a year
            ret = databuff_printf(buf, buf_len, pos, "%02d", (ob_time.parts_[DT_YDAY] - 1) / 7 + 1);
            break;
          }
          case ObDFMFlag::W: {  // the first complete week of a month
            ret = databuff_printf(buf, buf_len, pos, "%d", (ob_time.parts_[DT_MDAY] - 1) / 7 + 1);
            break;
          }
          case ObDFMFlag::YGYYY: {
            ret = databuff_printf(
                buf, buf_len, pos, "%d,%03d", abs(ob_time.parts_[DT_YEAR]) / 1000, abs(ob_time.parts_[DT_YEAR]) % 1000);
            break;
          }
          case ObDFMFlag::YEAR: {
            ret = OB_NOT_SUPPORTED;
            break;
          }
          case ObDFMFlag::SYEAR: {
            ret = OB_NOT_SUPPORTED;
            break;
          }
          case ObDFMFlag::SYYYY: {
            const char *fmt_str = ob_time.parts_[DT_YEAR] < 0 ? "-%04d" : " %04d";
            ret = databuff_printf(buf, buf_len, pos, fmt_str, abs(ob_time.parts_[DT_YEAR]));
            break;
          }
          case ObDFMFlag::YYYY: {
            ret = databuff_printf(buf, buf_len, pos, "%04d", abs(ob_time.parts_[DT_YEAR]));
            break;
          }
          case ObDFMFlag::YYY: {
            ret = databuff_printf(buf, buf_len, pos, "%03d", abs(ob_time.parts_[DT_YEAR] % 1000));
            break;
          }
          case ObDFMFlag::YY: {
            ret = databuff_printf(buf, buf_len, pos, "%02d", abs(ob_time.parts_[DT_YEAR] % 100));
            break;
          }
          case ObDFMFlag::Y: {
            ret = databuff_printf(buf, buf_len, pos, "%01d", abs(ob_time.parts_[DT_YEAR] % 10));
            break;
          }
          case ObDFMFlag::IYYY: {
            calc_iso_week(is_iso_week_calced, iso_week, ob_time, WEEK_MODE[3], iso_year_delta);
            ret = databuff_printf(buf, buf_len, pos, "%04d", abs(ob_time.parts_[DT_YEAR] + iso_year_delta));
            break;
          }
          case ObDFMFlag::IYY: {
            calc_iso_week(is_iso_week_calced, iso_week, ob_time, WEEK_MODE[3], iso_year_delta);
            ret = databuff_printf(buf, buf_len, pos, "%03d", abs((ob_time.parts_[DT_YEAR] + iso_year_delta) % 1000));
            break;
          }
          case ObDFMFlag::IY: {
            calc_iso_week(is_iso_week_calced, iso_week, ob_time, WEEK_MODE[3], iso_year_delta);
            ret = databuff_printf(buf, buf_len, pos, "%02d", abs((ob_time.parts_[DT_YEAR] + iso_year_delta) % 100));
            break;
          }
          case ObDFMFlag::I: {
            calc_iso_week(is_iso_week_calced, iso_week, ob_time, WEEK_MODE[3], iso_year_delta);
            ret = databuff_printf(buf, buf_len, pos, "%01d", abs((ob_time.parts_[DT_YEAR] + iso_year_delta) % 10));
            break;
          }
          case ObDFMFlag::TZD: {
            if (OB_UNLIKELY(!HAS_TYPE_ORACLE(ob_time.mode_))) {
              ret = OB_INVALID_DATE_FORMAT;
            } else if (OB_LIKELY(ob_time.time_zone_id_ != OB_INVALID_INDEX)) {
              ret = databuff_printf(
                  buf, buf_len, pos, "%.*s", ob_time.get_tzd_abbr_str().length(), ob_time.get_tzd_abbr_str().ptr());
            } else {
              // do nothing
            }
            break;
          }
          case ObDFMFlag::TZR: {
            if (OB_UNLIKELY(!HAS_TYPE_ORACLE(ob_time.mode_))) {
              ret = OB_INVALID_DATE_FORMAT;
            } else if (OB_LIKELY(ob_time.time_zone_id_ != OB_INVALID_INDEX)) {
              ret = databuff_printf(
                  buf, buf_len, pos, "%.*s", ob_time.get_tz_name_str().length(), ob_time.get_tz_name_str().ptr());
            } else {
              const char* fmt_str = ob_time.parts_[DT_OFFSET_MIN] < 0 ? "-%02d:%02d" : "+%02d:%02d";
              ret = databuff_printf(buf,
                  buf_len,
                  pos,
                  fmt_str,
                  abs(ob_time.parts_[DT_OFFSET_MIN]) / 60,
                  abs(ob_time.parts_[DT_OFFSET_MIN]) % 60);
            }
            break;
          }
          case ObDFMFlag::TZH: {
            if (OB_UNLIKELY(!HAS_TYPE_TIMEZONE(ob_time.mode_))) {
              ret = OB_INVALID_DATE_FORMAT;
            } else {
              const char *fmt_str = ob_time.parts_[DT_OFFSET_MIN] < 0 ? "-%02d" : "+%02d";
              ret = databuff_printf(buf, buf_len, pos, fmt_str, abs(ob_time.parts_[DT_OFFSET_MIN]) / 60);
            }
            break;
          }
          case ObDFMFlag::TZM: {
            if (OB_UNLIKELY(!HAS_TYPE_TIMEZONE(ob_time.mode_))) {
              ret = OB_INVALID_DATE_FORMAT;
            } else {
              ret = databuff_printf(buf, buf_len, pos, "%02d", abs(ob_time.parts_[DT_OFFSET_MIN]) % 60);
            }
            break;
          }
          case ObDFMFlag::X: {
            ret = databuff_printf(buf, buf_len, pos, ".");
            break;
          }

          default: {
            ret = OB_INVALID_DATE_FORMAT;
            LOG_WARN("unknown elem", K(ret), K(elem));
            break;
          }
        }  // end switch
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to print buf", K(elem), K(ret));
        }
      }  // end if
    }    // end for

    if (OB_SUCC(ret)) {
      // print the rest separate chars
      int64_t separate_chars_len = format.length() - last_elem_end_pos;
      if (separate_chars_len > 0) {
        if (OB_FAIL(databuff_printf(buf,
                                    buf_len,
                                    pos,
                                    "%.*s",
                                    static_cast<int32_t>(separate_chars_len),
                                    format_begin_ptr + last_elem_end_pos))) {
          LOG_WARN("failed to print otimestamp", "buf", ObString(pos, buf), K(ret));
        }
      }
      LOG_DEBUG("succ to print otimestamp", "buf", ObString(pos, buf), K(ret));
    }

    if (OB_UNLIKELY(OB_SIZE_OVERFLOW == ret)) {
      int ori_ret = ret;
      ret = OB_ERR_DATE_FORMAT_IS_TOO_LONG_FOR_INTERNAL_BUFFER;
      LOG_WARN("data format is to long for internal buffer", K(ret), K(ori_ret));
    }
  }  // end if
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
  if (ZERO_DATE == ob_time.parts_[DT_DATE] && !HAS_TYPE_ORACLE(ob_time.mode_)) {
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
  return ((static_cast<int64_t>(ob_time.parts_[DT_HOUR]) * MINS_PER_HOUR + ob_time.parts_[DT_MIN]) * SECS_PER_MIN
          + ob_time.parts_[DT_SEC]) * USECS_PER_SEC
         + (HAS_TYPE_ORACLE(ob_time.mode_) ? ob_time.parts_[DT_USEC] / NSECS_PER_USEC : ob_time.parts_[DT_USEC]);
}

int ObTimeConverter::ob_time_to_otimestamp(ObTime &ob_time, ObOTimestampData &value)
{
  int ret = OB_SUCCESS;
  if (!HAS_TYPE_ORACLE(ob_time.mode_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("it is not oracle type", K(ob_time), K(ret));
  } else if (OB_FAIL(validate_oracle_timestamp(ob_time))) {
    LOG_WARN("fail to validate_oracle_timestamp", K(ob_time), K(ret));
  } else {
    int64_t usec = ob_time.parts_[DT_DATE] * USECS_PER_DAY + ob_time_to_time(ob_time);
    value.time_us_ = usec;
    value.time_ctx_.set_tail_nsec(ob_time.parts_[DT_USEC] % NSECS_PER_USEC);
    if (!HAS_TYPE_TIMEZONE(ob_time.mode_)) {
      // do nothing
    } else if (OB_INVALID_INDEX == ob_time.time_zone_id_) {
      value.time_ctx_.store_tz_id_ = 0;
      value.time_ctx_.set_offset_min(ob_time.parts_[DT_OFFSET_MIN]);
    } else {
      value.time_ctx_.store_tz_id_ = 1;
      value.time_ctx_.set_tz_id(ob_time.time_zone_id_);
      value.time_ctx_.set_tran_type_id(ob_time.transition_type_id_);
    }
  }
  return ret;
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
    if (!(0 == parts[DT_YEAR]
          && 0 == parts[DT_HOUR]
          && 0 == parts[DT_MIN]
          && 0 == parts[DT_SEC]
          && 0 == parts[DT_USEC])) {
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

int ObTimeConverter::validate_oracle_timestamp(const ObTime &ob_time)
{
  const int32_t* parts = ob_time.parts_;
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < DATETIME_PART_CNT; ++i) {
    if (parts[i] < TZ_PART_MIN[i] || parts[i] > TZ_PART_MAX[i]) {
      ret = OB_INVALID_DATE_VALUE;
    }
  }
  if (OB_SUCC(ret)) {
    int is_leap = IS_LEAP_YEAR(parts[DT_YEAR]);
    if (parts[DT_MDAY] > DAYS_PER_MON[is_leap][parts[DT_MON]]) {
      ret = OB_ERR_DATE_NOT_VALID_FOR_MONTH_SPECIFIED;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_INVALID_INDEX != ob_time.time_zone_id_) {
      if (OB_UNLIKELY(!ObOTimestampData::is_valid_tz_id(ob_time.time_zone_id_))
          || OB_UNLIKELY(!ObOTimestampData::is_valid_tran_type_id(ob_time.transition_type_id_))) {
        ret = OB_INVALID_DATE_VALUE;
      }
    } else if (OB_UNLIKELY(!ObOTimestampData::is_valid_offset_min(parts[DT_OFFSET_MIN]))) {
      ret = OB_INVALID_DATE_VALUE;
    }
  }
  return ret;
}

int ObTimeConverter::set_ob_time_part_directly(ObTime &ob_time,
                                               int64_t &conflict_bitset,
                                               const int64_t part_offset,
                                               const int32_t part_value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(part_offset >= TOTAL_PART_CNT)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ob_time.parts_[part_offset] = part_value;
    conflict_bitset |= (1 << part_offset);
  }
  return ret;
}

/*
 * element group may cause conflict on parts in ob_time
 * 1. SSSSS vs HH, HH24, HH12, MI, SS
 * 2. DDD vs DD MM/Mon/Month
 *
 * while call this function, the part_value must be the final value
 */
int ObTimeConverter::set_ob_time_part_may_conflict(ObTime &ob_time,
                                                   int64_t &conflict_bitset,
                                                   const int64_t part_offset,
                                                   const int32_t part_value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(part_offset >= TOTAL_PART_CNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(part_offset));
  } else {
    if (0 != (conflict_bitset & (1 << part_offset))) {
      // already has data in ob_time.part_[part_name], validate it
      if (OB_UNLIKELY(part_value != ob_time.parts_[part_offset])) {
        ret = get_oracle_err_when_datetime_parts_conflict(part_offset);
        LOG_WARN("set time conflict", K(ret), K(part_offset), K(part_value), K(ob_time));
      }
    } else {
      conflict_bitset |= (1 << part_offset);
      ob_time.parts_[part_offset] = part_value;
    }
  }

  return ret;
}

int32_t ObTimeConverter::calc_max_name_length(const ObTimeConstStr names[], const int64_t size)
{
  int32_t res = 0;
  for (int64_t i = 1; i <= size; ++i) {
    if (res < names[i].len_) {
      res = names[i].len_;
    }
  }
  return res;
}

ObOTimestampData ObTimeConverter::round_otimestamp(const int16_t scale, const ObOTimestampData &in_ot_data)
{
  ObOTimestampData ret_ot_data = in_ot_data;
  const int16_t MIN_SCALE = 0;
  const int16_t MAX_US_SCALE = 6;
  const int16_t MAX_NS_SCALE = 9;
  if (MIN_SCALE <= scale && scale <= MAX_NS_SCALE) {
    if (scale < MAX_US_SCALE) {
      round_datetime(scale, *(int64_t*)&ret_ot_data.time_us_);
      ret_ot_data.time_ctx_.tail_nsec_ = 0;
    } else if (MAX_US_SCALE == scale) {
      if (ret_ot_data.time_ctx_.tail_nsec_ >= power_of_10[MAX_NS_SCALE - MAX_US_SCALE] / 2) {
        ++ret_ot_data.time_us_;
      }
      ret_ot_data.time_ctx_.tail_nsec_ = 0;
    } else if (scale < MAX_NS_SCALE) {
      int32_t power_of_precision =
          static_cast<int32_t>(power_of_10[MAX_NS_SCALE - MAX_US_SCALE - (scale - MAX_US_SCALE)]);
      int32_t residue = ret_ot_data.time_ctx_.tail_nsec_ % power_of_precision;
      if (residue >= power_of_precision / 2) {
        ret_ot_data.time_ctx_.set_tail_nsec(ret_ot_data.time_ctx_.tail_nsec_ + power_of_precision - residue);
      } else {
        ret_ot_data.time_ctx_.set_tail_nsec(ret_ot_data.time_ctx_.tail_nsec_ - residue);
      }
      if (ret_ot_data.time_ctx_.tail_nsec_ >= power_of_10[MAX_NS_SCALE - MAX_US_SCALE]) {
        ++ret_ot_data.time_us_;
        ret_ot_data.time_ctx_.set_tail_nsec(
            ret_ot_data.time_ctx_.tail_nsec_ - static_cast<int32_t>(power_of_10[MAX_NS_SCALE - MAX_US_SCALE]));
      }
    }
  }  // others, just return the original value.
  return ret_ot_data;
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

OB_INLINE int ObTimeConverter::sub_timezone_offset(const ObTimeZoneInfo *tz_info,
                                                  bool is_timestamp,
                                                  const ObString &tz_abbr_str,
                                                  int64_t &value,
                                                  const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  if (tz_info != NULL
      && (ZERO_DATETIME != value || is_oracle_mode)) {
    if (is_timestamp) {
      int32_t offset_sec = 0;
      int32_t tz_id = OB_INVALID_INDEX;
      int32_t tran_type_id = OB_INVALID_INDEX;
      if (OB_FAIL(tz_info->get_timezone_sub_offset(USEC_TO_SEC(value), tz_abbr_str, offset_sec, tz_id, tran_type_id))) {
        LOG_WARN("failed to get offset between utc and local", K(ret));
      } else {
        value -= SEC_TO_USEC(offset_sec);
      }
    }
  }
  return ret;
}

OB_INLINE int ObTimeConverter::sub_timezone_offset(const ObTimeZoneInfo &tz_info,
                                                   const ObString &tz_abbr_str,
                                                   int64_t &value_us,
                                                   int32_t &offset_min,
                                                   int32_t &tz_id,
                                                   int32_t &tran_type_id)
{
  int ret = OB_SUCCESS;
  int32_t offset_sec = 0;
  tran_type_id = OB_INVALID_INDEX;
  if (OB_FAIL(tz_info.get_timezone_sub_offset(USEC_TO_SEC(value_us), tz_abbr_str, offset_sec, tz_id, tran_type_id))) {
    LOG_WARN("failed to get offset between utc and local", K(ret));
  } else if (OB_INVALID_INDEX == tz_id) {
    if (OB_UNLIKELY(!ObOTimestampData::is_valid_offset_min(static_cast<int32_t>(SEC_TO_MIN(offset_sec))))) {
      ret = OB_INVALID_DATE_VALUE;
      LOG_WARN("invalid offset_sec", K(offset_sec), K(ret));
    }
  } else {
    if (OB_UNLIKELY(!ObOTimestampData::is_valid_tz_id(tz_id))
        || OB_UNLIKELY(!ObOTimestampData::is_valid_tran_type_id(tran_type_id))) {
      ret = OB_INVALID_DATE_VALUE;
      LOG_WARN("invalid tz_id", K(tz_id), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    value_us -= SEC_TO_USEC(offset_sec);
    offset_min = static_cast<int32_t>(SEC_TO_MIN(offset_sec));
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

int ObTimeConverter::validate_oracle_date(const ObTime &ob_time)
{
  const int32_t *parts = ob_time.parts_;
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < ORACLE_DATE_PART_CNT; ++i) {
    if (parts[i] < TZ_PART_MIN[i] || parts[i] > TZ_PART_MAX[i]) {
      ret = OB_INVALID_DATE_VALUE;
    }
  }
  if (OB_SUCC(ret)) {
    int is_leap = IS_LEAP_YEAR(parts[DT_YEAR]);
    if (parts[DT_MDAY] > DAYS_PER_MON[is_leap][parts[DT_MON]]) {
      ret = OB_INVALID_DATE_VALUE;
    }
  }
  return ret;
}


ObString ObDataTypeCastParams::get_nls_format(const ObObjType input_type) const
{
  ObString format_str;
  switch (input_type) {
    case ObDateTimeType:
      format_str = (force_use_standard_format_
                    ? ObTimeConverter::COMPAT_OLD_NLS_DATE_FORMAT
                    : (session_nls_formats_[NLS_DATE].empty()
                       ? ObTimeConverter::DEFAULT_NLS_DATE_FORMAT
                       : session_nls_formats_[NLS_DATE]));
      break;
    case ObTimestampNanoType:
    case ObTimestampLTZType:
      format_str = (force_use_standard_format_
                    ? ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_FORMAT
                    : (session_nls_formats_[NLS_TIMESTAMP].empty()
                       ? ObTimeConverter::DEFAULT_NLS_TIMESTAMP_FORMAT
                       : session_nls_formats_[NLS_TIMESTAMP]));
      break;
    case ObTimestampTZType:
      format_str = (force_use_standard_format_
                    ? ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT
                    : (session_nls_formats_[NLS_TIMESTAMP_TZ].empty()
                       ? ObTimeConverter::DEFAULT_NLS_TIMESTAMP_TZ_FORMAT
                       : session_nls_formats_[NLS_TIMESTAMP_TZ]));
      break;
    default:
      break;
  }
  return format_str;
}

void ObDataTypeCastParams::set_nls_date_format(ObString str)
{
  session_nls_formats_[NLS_DATE] = str;
}

void ObDataTypeCastParams::set_nls_timestamp_format(ObString str)
{
  session_nls_formats_[NLS_TIMESTAMP] = str;
}

void ObDataTypeCastParams::set_nls_timestamp_tz_format(ObString str)
{
  session_nls_formats_[NLS_TIMESTAMP_TZ] = str;
}

int ObDataTypeCastParams::set_nls_format_by_type(ObObjType type, ObString str)
{
  int ret = OB_SUCCESS;

  switch (type) {
    case ObDateTimeType:
      set_nls_date_format(str);
      break;
    case ObTimestampNanoType:
    case ObTimestampLTZType:
      set_nls_timestamp_format(str);
      break;
    case ObTimestampTZType:
      set_nls_timestamp_tz_format(str);
      break;
    default:
      ret = OB_INVALID_ARGUMENT;
      break;
  }

  return ret;
}

} // namesapce common
} // namespace oceanbase
