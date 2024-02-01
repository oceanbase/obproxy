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

#ifndef _OCEANBASE_COMMON_OB_TIME_UTILITY_H_
#define _OCEANBASE_COMMON_OB_TIME_UTILITY_H_
#include <stdio.h>
#include <time.h>
#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/ob_date_unit_type.h"

#define START_WITH_SUNDAY       0x04
#define WEEK_FIRST_WEEKDAY      0x02
#define INCLUDE_CRITICAL_WEEK   0x01

namespace oceanbase
{
namespace common
{
class ObTimeUtility
{
  enum ObHourFlag
  {
    HOUR_UNUSE,
    HOUR_AM,
    HOUR_PM
  };
public:
  enum DecimalDigts
  {
    DIGTS_INSENSITIVE,
    DIGTS_SENSITIVE
  };
  static const char *STD_TS_FORMAT_WITH_USEC;
  static const char *STD_TS_FORMAT_WITHOUT_USEC;
public:
  static int64_t current_time();
  static int64_t current_time_coarse();
  //call mktime() to make seconds from strcut tm, and check seconds whether valid
  static int make_second(struct tm &t, time_t &second);
  static time_t extract_second(int64_t usec);
  static int str_to_timestamp(const ObString &date, struct tm &t, int64_t &usec);
  static int str_to_usec(const ObString &date, int64_t &usec);
  static int str_to_time(const ObString &date, int64_t &usec, DecimalDigts num_flag = DIGTS_INSENSITIVE);
  static int extract_usec(const ObString &str, int64_t &pos, int64_t &usec, DecimalDigts num_flag);
  static int extract_date(const ObString &str, int n, int64_t &pos, int64_t &value);
  /**
   * @brief calculate weeks of the year use struct member tm_yday and tm_wday, and alter tm_year
   * @param flag_mask: START_WITH_SUNDAY, if set, the week will use Sunday as the first weekday
   * WEEK_FIRST_WEEKDAY, if set, use the first unbroken week as the first week in this year,
   * otherwise, use ISO 8601:1988 to decide the belongs of the critical week
   * INCLUDE_CRITICAL_WEEK, if set, use WEEK_FIRST_WEEKDAY or ISO 8601:1988 to decide the
   * critical week, otherwise, discard it in this year
   */
  static int get_weeks_of_year(struct tm &t, uint8_t flag_mask);
  static int timestamp_format_to_str(const struct tm &t, int64_t usec, const ObString &format,
                                     char *buf, int64_t buf_len, int64_t &pos);
  static int usec_format_to_str(int64_t usec, const ObString &format, char *buf, int64_t buf_len,
                                int64_t &pos);
  /**
   * @brief format usec to str with default format,
   * if the time has no microsecond, the format is 'YY-MM-DD HH:MM:SS.U'
   * otherwise, the format is 'YY-MM-DD HH:MM:SS'
   */
  static int usec_to_str(int64_t usec, char *buf, int64_t buf_len, int64_t &pos);
  static bool is_valid_date(int year, int month, int mday);
  static bool is_valid_time(int hour, int minute, int second, int usec = 0);
  static bool is_leap_year(int year);
private:
  static bool is_valid_year(int year);
  static bool is_valid_month(int month);
  //day of the month
  static bool is_valid_mday(int year, int month, int mday);
  //24-Hour
  static bool is_valid_hour(int hour);
  static bool is_valid_minute(int minute);
  static bool is_valid_second(int second);
  static bool is_valid_usec(int usec);
  /**
   * @brief calculate the start weekday of the year, and return the start weekday
   * @param year_day, the days of the specified date in year
   * @param week_day, the days of the specified date in it's week
   * @param week_first_weekday, if true, use WEEK_FIRST_WEEKDAY, otherwise, use ISO 8601:1988
   *        to decide the start weekday
   */
  static int get_start_weekday(int year_day, int week_day, bool week_first_weekday);
  /**
   * @brief check the date whether in the next year's first week
   */
  static bool check_in_next_critical_week(int yday, int wday, int year, uint8_t flag_mask);
private:
  static const char *mday_name_[31];
  static const char *weekday_name_[7];
  static const char *weekday_abbr_name_[7];
  static const char *month_name_[12];
  static const char *month_abbr_name_[12];
private:
  ~ObTimeUtility() {};
  ObTimeUtility() {};
};

inline int64_t ObTimeUtility::current_time()
{
  struct timeval t;
  if (gettimeofday(&t, NULL) < 0) {
    LIB_LOG(EDIAG, "get time of day failed");
  }
  return (static_cast<int64_t>(t.tv_sec) * static_cast<int64_t>(1000000) + static_cast<int64_t>(t.tv_usec));
}

} //common
} //oceanbase

#endif //_OCEANBASE_COMMON_OB_TIME_UTILITY_H_
