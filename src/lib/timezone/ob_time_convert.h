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

#ifndef OCEANBASE_LIB_TIMEZONE_OB_TIME_CONVERT_
#define OCEANBASE_LIB_TIMEZONE_OB_TIME_CONVERT_

//#include "lib/timezone/ob_timezone_util.h"
#include "lib/timezone/ob_timezone_info.h"
#include "lib/string/ob_string.h"
#include "lib/ob_date_unit_type.h"
#include "common/ob_obj_type.h"
#include "share/part/ob_part_mgr_util.h"
#include "common/ob_sql_mode.h"

namespace oceanbase
{
namespace common
{
class ObTimeZoneInfoPos;
class ObTimeZoneInfo;
class ObOTimestampData;
struct ObTimeConstStr;
struct ObDataTypeCastParams;
class ObObj;

#define DT_TYPE_DATE        (1UL << 0)
#define DT_TYPE_TIME        (1UL << 1)
#define DT_TYPE_NONE        (1UL << 2)  // like MYSQL_TIMESTAMP_NONE, set when DT_TYPE_DATETIME is set
                                        // and the string is not in a good format, such as has delimiters
                                        // but has no space in delimiters.
#define DT_MODE_DST_GAP     (1UL << 3)
#define DT_MODE_NEG         (1UL << 4)
#define DT_WEEK_SUN_BEGIN   (1UL << 5)  // sunday is the first day of week, otherwise monday.
#define DT_WEEK_ZERO_BEGIN  (1UL << 6)  // week num will begin with 0, otherwise 1.
#define DT_WEEK_GE_4_BEGIN  (1UL << 7)  // week which has 4 or more days is week 1, otherwise has
                                        // the first sunday of monday.

#define DT_TYPE_ORACLE (1UL << 8)     // oracle timestamp to nanosecond (nano, tz, ltz)
#define DT_TYPE_STORE_UTC (1UL << 9)  // store utc  (tz, ltz)
#define DT_TYPE_TIMEZONE (1UL << 10)  // oracle timestamp with time zone (tz)
#define DT_MODE_MYSQL_DATES (1UL << 11) // mysql compatible dates

typedef uint64_t ObDTMode;

#define DT_TYPE_DATETIME  (DT_TYPE_DATE | DT_TYPE_TIME)
#define DT_TYPE_ORACLE_TIMESTAMP (DT_TYPE_DATETIME | DT_TYPE_ORACLE)
#define DT_TYPE_ORACLE_TTZ (DT_TYPE_DATETIME | DT_TYPE_ORACLE | DT_TYPE_TIMEZONE)
#define DT_TYPE_MYSQL_DATE        (DT_TYPE_DATE | DT_MODE_MYSQL_DATES)
#define DT_TYPE_MYSQL_DATETIME    (DT_TYPE_DATETIME | DT_MODE_MYSQL_DATES)
#define DT_TYPE_CNT       (3)

#define HAS_TYPE_DATE(mode)     (DT_TYPE_DATE & (mode))
#define HAS_TYPE_TIME(mode)     (DT_TYPE_TIME & (mode))
#define IS_TYPE_DATE(mode)      (DT_TYPE_DATE == (mode))
#define IS_TYPE_TIME(mode)      (DT_TYPE_TIME == (mode))
#define IS_TYPE_DATETIME(mode)  (DT_TYPE_DATETIME == (mode) || DT_TYPE_MYSQL_DATETIME == (mode))
#define IS_NEG_TIME(mode)       (DT_MODE_NEG & (mode))
#define IS_SUN_BEGIN(mode)      ((DT_WEEK_SUN_BEGIN & (mode)) ? 1 : 0)
#define IS_ZERO_BEGIN(mode)     ((DT_WEEK_ZERO_BEGIN & (mode)) ? 1 : 0)
#define IS_GE_4_BEGIN(mode)     ((DT_WEEK_GE_4_BEGIN & (mode)) ? 1 : 0)
#define HAS_TYPE_ORACLE(mode) ((DT_TYPE_ORACLE & (mode)) ? 1 : 0)
#define HAS_TYPE_TIMEZONE(mode) ((DT_TYPE_TIMEZONE & (mode)) ? 1 : 0)
#define HAS_TYPE_STORE_UTC(mode) ((DT_TYPE_STORE_UTC & (mode)) ? 1 : 0)
#define IS_MYSQL_COMPAT_DATES(mode) ((DT_MODE_MYSQL_DATES & (mode)) ? 1 : 0)

#define DATE_PART_CNT   3
#define TIME_PART_CNT   4
#define OTHER_PART_CNT  4
#define DATETIME_PART_CNT   (DATE_PART_CNT + TIME_PART_CNT)
#define ORACLE_DATE_PART_CNT (DATE_PART_CNT + TIME_PART_CNT - 1)
#define TOTAL_PART_CNT      (DATETIME_PART_CNT + OTHER_PART_CNT)

#define DT_YEAR   0
#define DT_MON    1
#define DT_MDAY   2
#define DT_HOUR   3
#define DT_MIN    4
#define DT_SEC    5
#define DT_USEC   6
#define DT_DATE   7
#define DT_YDAY   8
#define DT_WDAY   9
#define DT_OFFSET_MIN 10

#define DT_MON_NAME \
  11  // monthname doesn't contains real data by using month directly
      // put it after DT_OFFSET_MIN will be fine

extern const int64_t DT_PART_BASE[DATETIME_PART_CNT];
extern const int64_t DT_PART_MIN[DATETIME_PART_CNT];
extern const int64_t DT_PART_MAX[DATETIME_PART_CNT];

#define MONS_PER_YEAR   DT_PART_BASE[DT_MON]
#define HOURS_PER_DAY   DT_PART_BASE[DT_HOUR]
#define MINS_PER_HOUR   DT_PART_BASE[DT_MIN]
#define SECS_PER_MIN    DT_PART_BASE[DT_SEC]
#define USECS_PER_SEC   DT_PART_BASE[DT_USEC]
#define NSECS_PER_SEC   1000000000LL
#define NSECS_PER_USEC   1000LL
#define MONS_PER_QUAR   3
#define DAYS_PER_WEEK   7
#define DAYS_PER_NYEAR  365
#define DAYS_PER_LYEAR  366
#define YEARS_PER_CENTURY 100
//in order to optimized perf
//the following literals are defined by const not macro since
//they will be used many times.
extern const int64_t SECS_PER_HOUR;
extern const int64_t SECS_PER_DAY;
extern const int64_t USECS_PER_DAY;
// days from 0000-00-00 to 1970-01-01
#define DAYS_FROM_ZERO_TO_BASE 719528
#define MAX_DAYS_OF_DATE 3652424
#define MIN_DAYS_OF_DATE 366
#define TIMESTAMP_MIN_LENGTH 19
#define DATETIME_MIN_LENGTH 19
#define DATETIME_MAX_LENGTH 26
#define TIME_MIN_LENGTH 10
#define DATE_MIN_LENGTH 10
//max timestamp is 253402272000
#define TIMESTAMP_VALUE_LENGTH 12
#define SEC_TO_USEC(secs)   ((secs) * static_cast<int64_t>(USECS_PER_SEC))
#define USEC_TO_SEC(usec)   ((usec) / USECS_PER_SEC)
#define SEC_TO_MIN(secs) ((secs) / SECS_PER_MIN)
#define MIN_TO_USEC(min) ((min)*SECS_PER_MIN * USECS_PER_SEC)
#define TIMESTAMP_MAX_VAL 253402272000
#define DATETIME_MAX_VAL    253402300799999999 // '9999-12-31 23:59:59.999999'
#define MYSQL_DATETIME_MAX_VAL    9147936188962652735 // '9999-12-31 23:59:59.999999'
#define DATE_MAX_VAL        2932896 // '9999-12-31'
#define DATETIME_MIN_VAL    -62167132800000000 // '0000-01-01 00:00:00.000000'
#define MYSQL_DATETIME_MIN_VAL  0 // '0000-00-00 00:00:00.000000'
#define ORACLE_DATETIME_MIN_VAL -62135596800000000 //start from '0001-1-1 00:00:00'

struct ObDateSqlMode {
  union {
    uint64_t date_sql_mode_;
    struct {
      uint64_t allow_invalid_dates_:1;
      uint64_t no_zero_date_:1;
      uint64_t no_zero_in_date_:1;
      // For dayofmonth, year, month, day allow incomplete dates such as '2001-11-00', and not
      // affected by sqlmode NO_ZERO_IN_DATE, you can learn more from the below link by searching
      // the key words "SELECT DAYOFMONTH('2001-11-00'), MONTH('2005-00-00');"
      // https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html
      uint64_t allow_incomplete_dates_:1;
      uint64_t reserved_:28;
    };
  };
  ObDateSqlMode() : date_sql_mode_(0) {};
  ObDateSqlMode(const int64_t date_sql_mode) {
    allow_invalid_dates_ = (date_sql_mode & (1LL << 0)) ? 1 : 0;
    no_zero_date_ = (date_sql_mode & (1LL << 1)) ? 1 : 0;
    // no_zero_in_date_ = date_sql_mode & (1ULL << 2);
  };
  void init(const ObSQLMode sql_mode) {
    allow_invalid_dates_ = (bool)(SMO_ALLOW_INVALID_DATES & sql_mode);
    no_zero_date_ = (bool)(SMO_NO_ZERO_DATE & sql_mode);
    no_zero_in_date_ = (bool)(SMO_NO_ZERO_IN_DATE & sql_mode);
  };
  // There are two situations where zero in date is allowed. The first is allow_incomplete_dates_,
  // and the second is when configure `enable_mysql_compatible_dates_` is turned on and
  // sql mode `no_zero_in_date` is not set.
  bool allow_zero_in_date(const bool is_mysql_compat_dates) const
  { return allow_incomplete_dates_ || (is_mysql_compat_dates && !no_zero_in_date_); }
  TO_STRING_KV(K_(allow_invalid_dates), K_(no_zero_date), K_(allow_incomplete_dates),
               K_(no_zero_in_date));
};

class ObTime
{
public:
  ObTime()
      : mode_(0),
        time_zone_id_(common::OB_INVALID_INDEX),
        transition_type_id_(common::OB_INVALID_INDEX),
        is_tz_name_valid_(false)
  {
    MEMSET(parts_, 0, sizeof(parts_));
    MEMSET(tz_name_, 0, common::OB_MAX_TZ_NAME_LEN);
    MEMSET(tzd_abbr_, 0, common::OB_MAX_TZ_ABBR_LEN);
  }
  explicit ObTime(ObDTMode mode)
      : mode_(mode),
        time_zone_id_(common::OB_INVALID_INDEX),
        transition_type_id_(common::OB_INVALID_INDEX),
        is_tz_name_valid_(false)
  {
    MEMSET(parts_, 0, sizeof(parts_));
    MEMSET(tz_name_, 0, common::OB_MAX_TZ_NAME_LEN);
    MEMSET(tzd_abbr_, 0, common::OB_MAX_TZ_ABBR_LEN);
  }
  ~ObTime()
  {}
  ObString get_tz_name_str() const
  {
    return ObString(strlen(tz_name_), tz_name_);
  }
  ObString get_tzd_abbr_str() const
  {
    return ObString(strlen(tzd_abbr_), tzd_abbr_);
  }
  int set_tz_name(const ObString &tz_name);
  int set_tzd_abbr(const ObString &tz_abbr);
  
  TO_STRING_KV(K(mode_), "parts", ObArrayWrap<int32_t>(parts_, TOTAL_PART_CNT), "tz_name",
      ObString(OB_MAX_TZ_NAME_LEN, tz_name_), "tzd_abbr", ObString(OB_MAX_TZ_ABBR_LEN, tzd_abbr_), K_(time_zone_id),
      K_(transition_type_id), K_(is_tz_name_valid));

  ObDTMode  mode_;
  int32_t   parts_[TOTAL_PART_CNT];
  // year:    [1000, 9999].
  // month:   [1, 12].
  // day:     [1, 31].
  // hour:    [0, 23] or [0, 838] if it is a time.
  // minute:  [0, 59].
  // second:  [0, 59].
  // usecond: [0, 1000000], 1000000 can only valid after str_to_ob_time, for round.
  // date: date value, day count since 1970-1-1.
  // year day: [1, 366].
  // week day: [1, 7], 1 means monday, 7 means sunday.
  // offset minute:  [-12*60, 14*60].

  char tz_name_[common::OB_MAX_TZ_NAME_LEN];
  char tzd_abbr_[common::OB_MAX_TZ_ABBR_LEN];  // the abbr of time zone region with Daylight Saving Time
  int32_t time_zone_id_;
  int32_t transition_type_id_;
  bool is_tz_name_valid_;
};

typedef ObTime ObInterval;

struct ObTimeConstStr {
  ObTimeConstStr() : ptr_(NULL), len_(0)
  {}
  ObTimeConstStr(const char *str) : ptr_(str), len_(static_cast<int32_t>(strlen(str)))
  {}
  ObTimeConstStr(const char *str, int32_t len) : ptr_(str), len_(len)
  {}
  inline ObString to_obstring() const
  {
    return ObString(len_, ptr_);
  }
  const char *ptr_;
  int32_t len_;
  TO_STRING_KV("value", ObString(len_, ptr_), K_(len));
};

struct ObTimeConvertCtx
{
  ObTimeConvertCtx(const ObTimeZoneInfo *tz_info, const bool is_timestamp, const bool &need_truncate = false)
     :tz_info_(tz_info),
      oracle_nls_format_(),
      is_timestamp_(is_timestamp),
      need_truncate_(need_truncate) {}
  ObTimeConvertCtx(const ObTimeZoneInfo *tz_info, const ObString &oracle_nls_format, const bool is_timestamp)
     :tz_info_(tz_info),
      oracle_nls_format_(oracle_nls_format),
      is_timestamp_(is_timestamp) {}
  const ObTimeZoneInfo *tz_info_;
  ObString oracle_nls_format_;
  bool is_timestamp_; //means mysql timestamp?
  bool need_truncate_;
};

struct ObMySQLDate
{
  ObMySQLDate() : date_(0) {}
  ObMySQLDate(int32_t date) : date_(date) {}
  inline bool operator==(const ObMySQLDate &other) const { return date_ == other.date_; }
  inline bool operator!=(const ObMySQLDate &other) const { return date_ != other.date_; }
  inline bool operator>(const ObMySQLDate &other) const { return date_ > other.date_; }
  inline bool operator<(const ObMySQLDate &other) const { return date_ < other.date_; }
  inline bool operator>=(const ObMySQLDate &other) const { return date_ >= other.date_; }
  inline bool operator<=(const ObMySQLDate &other) const { return date_ <= other.date_; }
  TO_STRING_KV(K_(date), K_(year), K_(month), K_(day));
  union {
    struct {
      uint32_t day_ : 5;
      uint32_t month_ : 4;
      uint32_t year_ : 14;
      uint32_t reserved_ : 9;
    };
    int32_t date_;
  };
};

struct ObMySQLDateTime
{
private:
  static const int32_t DATETIME_YEAR_OFFSET = 13;
public:
  ObMySQLDateTime() : datetime_(0) {}
  ObMySQLDateTime(int64_t datetime) : datetime_(datetime) {}
  inline bool operator==(const ObMySQLDateTime &other) const { return datetime_ == other.datetime_; }
  inline bool operator!=(const ObMySQLDateTime &other) const { return datetime_ != other.datetime_; }
  inline bool operator>(const ObMySQLDateTime &other) const { return datetime_ > other.datetime_; }
  inline bool operator<(const ObMySQLDateTime &other) const { return datetime_ < other.datetime_; }
  inline bool operator>=(const ObMySQLDateTime &other) const { return datetime_ >= other.datetime_; }
  inline bool operator<=(const ObMySQLDateTime &other) const { return datetime_ <= other.datetime_; }
  inline int32_t year() const { return year_month_ / DATETIME_YEAR_OFFSET; }
  inline int32_t month() const { return year_month_ % DATETIME_YEAR_OFFSET; }
  inline static uint64_t year_month(uint64_t year, uint64_t month)
  { return year * DATETIME_YEAR_OFFSET + month; }
  TO_STRING_KV(K_(datetime), "year", year(), "month", month(), K_(day), K_(hour), K_(minute),
               K_(second), K_(microseconds));
  union {
    struct {
      uint64_t microseconds_ : 24;
      uint64_t second_ : 6;
      uint64_t minute_ : 6;
      uint64_t hour_ : 5;
      uint64_t day_ : 5;
      uint64_t year_month_: 17;
      uint64_t sign_ : 1;
    };
    int64_t datetime_;
  };
};

class ObTimeConverter
{
public:
  // ZERO_DATETIME is the minimal value that satisfied: 0 == value % USECS_PER_DAY.
  static const int64_t ZERO_DATETIME = static_cast<int64_t>(-9223372022400000000); // 0-0-0 0:0:0
  static const int64_t MYSQL_ZERO_DATETIME = 0; // 0-0-0 0:0:0

  // ZERO_DATE is ZERO_DATETIME / USECS_PER_DAY
  static const int32_t ZERO_DATE = static_cast<int32_t>(-106751991); // 0-0-0
  static const int32_t MYSQL_ZERO_DATE = 0; // 0-0-0
  static const int64_t ZERO_TIME = 0;
  static const uint8_t ZERO_YEAR = 0;
  static const ObString DEFAULT_NLS_DATE_FORMAT;
  static const ObString DEFAULT_NLS_TIMESTAMP_FORMAT;
  static const ObString DEFAULT_NLS_TIMESTAMP_TZ_FORMAT;
  static const ObString COMPAT_OLD_NLS_DATE_FORMAT;
  static const ObString COMPAT_OLD_NLS_TIMESTAMP_FORMAT;
  static const ObString COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT;
  
public:
  // int / double / string -> datetime(timestamp) / interval / date / time / year.
  static int int_to_datetime(int64_t int_part, int64_t dec_part, const ObTimeZoneInfo *tz_info,
                             int64_t &value,  const ObDateSqlMode date_sql_mode = 0);
  static int int_to_mdatetime(int64_t int_part, int64_t dec_part, const ObTimeConvertCtx &cvrt_ctx,
                              ObMySQLDateTime &value, const ObDateSqlMode date_sql_mode = 0);
  static int int_to_date(int64_t int64, int32_t &value, const ObDateSqlMode date_sql_mode = 0);
  static int int_to_mdate(int64_t int64, ObMySQLDate &value, const ObDateSqlMode date_sql_mode = 0);
  static int int_to_time(int64_t int64, int64_t &value);
  static int int_to_year(int64_t int64, uint8_t &value);
  static int str_to_datetime(const ObString &str, const ObTimeZoneInfo *tz_info, int64_t &value, int16_t *scale = NULL);
  static int str_to_mdatetime(const ObString &str, const ObTimeConvertCtx &cvrt_ctx,
                            ObMySQLDateTime &value, int16_t *scale = NULL,
                            const ObDateSqlMode date_sql_mode = 0);
  static int str_to_date_oracle(const ObString &str, const ObTimeConvertCtx &cvrt_ctx, ObDateTime &value);
  static int str_to_datetime_format(const ObString &str, const ObString &fmt,
                                    const ObTimeZoneInfo *tz_info, int64_t &value, int16_t *scale = NULL,
                                    const ObDateSqlMode date_sql_mode = 0);
  static int str_to_mdatetime_format(const ObString &str, const ObString &fmt,
                                    const ObTimeConvertCtx &cvrt_ctx, ObMySQLDateTime &value,
                                    int16_t *scale, const ObDateSqlMode date_sql_mode = 0);
  static int str_to_otimestamp(const ObString &str, const ObTimeConvertCtx &cvrt_ctx,
                               const ObObjType target_type, ObOTimestampData &value,
                               ObScale &scale);
  static int str_is_date_format(const ObString &str, bool &date_flag);
  static int str_to_date(const ObString &str, int32_t &value, const ObDateSqlMode date_sql_mode = 0);
  static int str_to_mdate(const ObString &str, ObMySQLDate &value, const ObDateSqlMode date_sql_mode = 0);
  static int str_to_time(const ObString &str, int64_t &value, int16_t *scale = NULL);
  static int str_to_year(const ObString &str, uint8_t &value);
  static int str_to_interval(const ObString &str, ObDateUnitType unit_type, int64_t &value);
  // int / double / string <- datetime(timestamp) / date / time / year.
  static int datetime_to_int(int64_t value, const ObTimeZoneInfo *tz_info, int64_t &int64);
  static int mdatetime_to_int(ObMySQLDateTime value, int64_t &int64);
  static int datetime_to_double(int64_t value, const ObTimeZoneInfo *tz_info, double &dbl);
  static int mdatetime_to_double(ObMySQLDateTime value, double &dbl);
  static int datetime_to_str(int64_t value, const ObTimeZoneInfo *tz_info, int16_t scale,
                             char *buf, int64_t buf_len, int64_t &pos, bool with_delim = true);
  static int mdatetime_to_str(ObMySQLDateTime value, const ObTimeZoneInfo *tz_info,
                              const ObString &nls_format, int16_t scale, char *buf, int64_t buf_len,
                              int64_t &pos, bool with_delim = true);
  static int otimestamp_to_str(const ObOTimestampData &value, const ObDataTypeCastParams &dtc_params,
                               const int16_t scale, const ObObjType type, char *buf, int64_t buf_len, int64_t &pos);
  static int date_to_int(int32_t value, int64_t &int64);
  static int mdate_to_int(ObMySQLDate value, int64_t &int64);
  static int date_to_str(int32_t value, char *buf, int64_t buf_len, int64_t &pos);
  static int mdate_to_str(ObMySQLDate value, char *buf, int64_t buf_len, int64_t &pos);
  static int time_to_int(int64_t value, int64_t &int64);
  static int time_to_double(int64_t value, double &dbl);
  static int time_to_str(int64_t value, int16_t scale,
                         char *buf, int64_t buf_len, int64_t &pos, bool with_delim = true);
  static int time_to_datetime(int64_t t_value, int64_t cur_dt_value,
                              const ObTimeZoneInfo *tz_info, int64_t &dt_value, const ObObjType expect_type);
  static int time_to_mdatetime(int64_t t_value, int64_t cur_dt_value,
                               const ObTimeZoneInfo *tz_info, ObMySQLDateTime &mdt_value);
  static int year_to_int(uint8_t value, int64_t &int64);
  static int year_to_str(uint8_t value, char *buf, int64_t buf_len, int64_t &pos);
  // inner cast between datetime, timestamp, date, time, year.
  static int datetime_to_timestamp(int64_t dt_value, const ObTimeZoneInfo *tz_info, int64_t &ts_value);
  static int mdatetime_to_timestamp(ObMySQLDateTime mdt_value, const ObTimeZoneInfo *tz_info, int64_t &ts_value);
  static int timestamp_to_datetime(int64_t ts_value, const ObTimeZoneInfo *tz_info, int64_t &dt_value);
  static int timestamp_to_mdatetime(int64_t ts_value, const ObTimeZoneInfo *tz_info, ObMySQLDateTime &mdt_value);
  static int mdatetime_to_datetime(ObMySQLDateTime mdt_value, int64_t &dt_value, const ObDateSqlMode date_sql_mode = 0);
  static int datetime_to_mdatetime(int64_t dt_value, ObMySQLDateTime &mdt_value);
  static inline void datetime_to_odate(int64_t dt_value, int64_t &odate_value) { odate_value = dt_value; }
  static int odate_to_otimestamp(int64_t in_value_us, const ObTimeZoneInfo *tz_info, const ObObjType out_type,
                                 ObOTimestampData &out_value);
  static int otimestamp_to_odate(const ObObjType in_type, const ObOTimestampData &in_value,
                                 const ObTimeZoneInfo *tz_info, int64_t &out_value_us);
  static int otimestamp_to_otimestamp(const ObObjType in_type, const ObOTimestampData &in_value,
                                      const ObTimeZoneInfo *tz_info, const ObObjType out_type,
                                      ObOTimestampData &out_value);
  static int extract_offset_from_otimestamp(const ObOTimestampData &in_value, const ObTimeZoneInfo *tz_info,
                                            int32_t &offset_min, ObTime &ob_time);
  static int datetime_to_date(int64_t dt_value, const ObTimeZoneInfo *tz_info, int32_t &d_value);
  static int datetime_to_mdate(int64_t dt_value, const ObTimeZoneInfo *tz_info, ObMySQLDate &md_value);
  static int mdatetime_to_date(ObMySQLDateTime mdt_value, int32_t &d_value, const ObDateSqlMode date_sql_mode = 0);
  static int mdatetime_to_mdate(ObMySQLDateTime mdt_value, ObMySQLDate &md_value);
  static int datetime_to_time(int64_t dt_value, const ObTimeZoneInfo *tz_info, int64_t &t_value);
  static int mdatetime_to_time(ObMySQLDateTime mdt_value, int64_t &t_value);
  static int datetime_to_year(int64_t dt_value, const ObTimeZoneInfo *tz_info, uint8_t &y_value);
  static int mdatetime_to_year(ObMySQLDateTime mdt_value, uint8_t &y_value);
  static int date_to_datetime(int32_t d_value, const ObTimeZoneInfo *tz_info, int64_t &dt_value);
  static int date_to_mdatetime(int32_t d_value, ObMySQLDateTime &mdt_value);
  static int mdate_to_datetime(ObMySQLDate md_value, const ObTimeConvertCtx &cvrt_ctx, int64_t &dt_value, const ObDateSqlMode date_sql_mode = 0);
  static int mdate_to_mdatetime(ObMySQLDate md_value, ObMySQLDateTime &mdt_value);
  static int mdate_to_date(ObMySQLDate md_value, int32_t &d_value, const ObDateSqlMode date_sql_mode = 0);
  static int date_to_mdate(int32_t d_value, ObMySQLDate &md_value);
  static int date_to_year(int32_t d_value, uint8_t &y_value);
  static int mdate_to_year(ObMySQLDate md_value, uint8_t &y_value);
  // string -> offset. value: seconds, not useconds.
  static int str_to_offset(const ObString &str, int32_t &value, int &ret_more,
                           const bool is_oracle_mode, const bool need_check_valid = false);
  // year / month / day / quarter / week / hour / minite / second / microsecond.
  static int int_to_week(int64_t uint64, int64_t mode, int32_t &value);
  // date add / sub / diff.
  static int date_adjust(const int64_t base_value, const ObString &interval_str,
                         ObDateUnitType unit_type, int64_t &value, bool is_add);
  static int date_adjust(const ObString &base_str, const ObString &interval_str,
                         ObDateUnitType unit_type, int64_t &value, bool is_add);
  static bool is_valid_datetime(const int64_t usec);
  static bool is_valid_mdatetime(const ObMySQLDateTime usec);
  static bool is_valid_otimestamp(const int64_t time_us, const int32_t tail_nsec);
  static int date_add_nmonth(const int64_t ori_date_value, const int64_t nmonth,
                             int64_t &result_date_value, bool auto_adjust_mday = false);
  static int date_add_nsecond(const int64_t ori_date_value, const int64_t nsecond,
                              const int32_t fractional_second, int64_t &result_date_value);
  static int otimestamp_add_nmonth(const ObObjType type, const ObOTimestampData ori_value, const ObTimeZoneInfo *tz_info,
                                   const int64_t nmonth, ObOTimestampData &result_value);
  static int otimestamp_add_nsecond(const ObOTimestampData ori_value, const int64_t nsecond,
                                    const int32_t fractional_second,
                                    ObOTimestampData &result_value);
  static int calc_last_date_of_the_month(const int64_t ori_date_value, int64_t &result_date_value,
                                         const ObObjType dest_type, const ObDateSqlMode date_sql_mode = 0);
  static int calc_last_mdate_of_the_month(const ObMySQLDateTime mdatetime, ObMySQLDate &mdate,
                                          const ObDateSqlMode date_sql_mode = 0);
  static int calc_next_date_of_the_wday(const int64_t ori_date_value, const ObString &wday_name, const int64_t week_count, int64_t &result_date_value);
  static int calc_days_and_months_between_dates(const int64_t date_value1, const int64_t date_value2, int64_t &months_diff, int64_t &rest_utc_diff);

public:
  // int / string -> ObTime / ObInterval <- datetime(timestamp) / date / time / year.
  static int int_to_ob_time_with_date(int64_t int64, ObTime &ob_time,
                                      const ObDateSqlMode date_sql_mode = 0);
  static int int_to_ob_time_without_date(int64_t int64, ObTime &ob_time);
  static int str_to_ob_time_with_date(const ObString &str, ObTime &ob_time, int16_t *scale = NULL,
                                      const ObDateSqlMode date_sql_mode = 0);
  static int str_to_ob_time_without_date(const ObString &str, ObTime &ob_time, int16_t *scale = NULL);
  static int str_to_ob_time_format(const ObString &str, const ObString &fmt, ObTime &ob_time, int16_t *scale = NULL);
  static int str_to_ob_interval(const ObString &str, ObDateUnitType unit_type, ObInterval &ob_interval);
  static int str_to_ob_time_oracle_dfm(const ObString &str, const ObTimeConvertCtx &cvrt_ctx,
                                       const ObObjType target_type, ObTime &ob_time, ObScale &scale);
  static int usec_to_ob_time(int64_t usecs, ObTime &ob_time);
  static int datetime_to_ob_time(int64_t value, const ObTimeZoneInfo *tz_info, ObTime &ob_time);
  template <bool calc_date = false>
  static int mdatetime_to_ob_time(const ObMySQLDateTime &value, ObTime &ob_time);
  static int otimestamp_to_ob_time(const ObObjType type, const ObOTimestampData &ot_data, const ObTimeZoneInfo *tz_info,
                                   ObTime &ob_time, const bool store_utc_time = true);
  static int date_to_ob_time(int32_t value, ObTime &ob_time);
  template <bool calc_date = false>
  static int mdate_to_ob_time(ObMySQLDate value, ObTime &ob_time);
  static int time_to_ob_time(int64_t value, ObTime &ob_time);
  // int / string <- ObTime -> datetime(timestamp) / date / time.
  static int64_t ob_time_to_int(const ObTime &ob_time, ObDTMode mode);
  static int64_t ob_time_to_int_extract(const ObTime &ob_time, ObDateUnitType unit_type);
  static int ob_time_to_str(const ObTime &ob_time, ObDTMode mode, int16_t scale,
                            char *buf, int64_t buf_len, int64_t &pos, bool with_delim);
  static int ob_time_to_str_format(const ObTime &ob_time, const ObString &format,
                                   char *buf, int64_t buf_len, int64_t &pos);
  static int calc_tz_offset_by_tz_name(const ObTimeConvertCtx &cvrt_ctx, ObTime &ob_time);
  static int ob_time_to_utc(const ObObjType obj_type, const ObTimeConvertCtx &cvrt_ctx, ObTime &ob_time);
  static bool valid_oracle_year(const ObTime &ob_time);
  static void calc_iso_week(bool &is_iso_week_calced, int32_t &iso_week, const ObTime &ob_time,
                            ObDTMode mode, int32_t &delta);
  static int ob_time_to_str_oracle_dfm(const ObTime &ob_time, ObScale scale, const ObString &format,
                                       char *buf, int64_t buf_len, int64_t &pos);
  static int ob_time_to_datetime(ObTime &ob_time, const ObTimeZoneInfo *tz_info, int64_t &value);
  static int ob_time_to_mdatetime(ObTime &ob_time, ObMySQLDateTime &value);
  static int ob_time_to_otimestamp(ObTime &ob_time, ObOTimestampData &value);
  static int32_t ob_time_to_date(ObTime &ob_time);
  static ObMySQLDate ob_time_to_mdate(ObTime &ob_time);
  static int32_t calc_date(int64_t year, int64_t month, int64_t day);
  static int32_t calc_date(const ObMySQLDate mdate)
  { return calc_date(mdate.year_, mdate.month_, mdate.day_); }
  static int64_t ob_time_to_time(const ObTime &ob_time);
  static int ob_interval_to_interval(const ObInterval &ob_interval, int64_t &value);
  // year / month / day / quarter / week / hour / minite / second / microsecond.
  static int32_t ob_time_to_week(const ObTime &ob_time, ObDTMode mode);
  static int32_t ob_time_to_week(const ObTime &ob_time, ObDTMode mode, int32_t &delta);
  static void get_first_day_of_isoyear(ObTime &ob_time);
  static int get_round_day_of_isoyear(ObTime &ob_time);
  static int validate_oracle_date(const ObTime &ob_time);
  
public:
  // other functions.
  static int set_ob_time_part_directly(
      ObTime &ob_time, int64_t &conflict_bitset, const int64_t part_offset, const int32_t part_value);
  static int set_ob_time_part_may_conflict(
      ObTime &ob_time, int64_t &conflict_bitset, const int64_t part_offset, const int32_t part_value);
  static int32_t calc_max_name_length(const ObTimeConstStr names[], const int64_t size);
  static ObOTimestampData round_otimestamp(const int16_t scale, const ObOTimestampData &in_ot_data);
  static int time_overflow_trunc(int64_t &value);
  static void round_datetime(int16_t scale, int64_t &value);
  static void round_mdatetime(int16_t scale, ObMySQLDateTime &value);
  static void trunc_datetime(int16_t scale, int64_t &value);
  static void trunc_mdatetime(int16_t scale, ObMySQLDateTime &value);
  static bool ob_is_date_datetime_all_parts_zero(const int64_t &value)
  {
    return (ZERO_DATE == value) || (ZERO_DATETIME == value);
  }
  static int get_oracle_err_when_datetime_parts_conflict(int64_t part_idx);
  
  struct ObTimeDigits {
    ObTimeDigits() : ptr_(NULL), len_(0), value_(0)
    {}
    VIRTUAL_TO_STRING_KV(K(ptr_), K(len_), K(value_));
    const char *ptr_;
    int32_t len_;
    int32_t value_;
  };
  struct ObTimeDelims {
    ObTimeDelims() : ptr_(NULL), len_(0)
    {}
    VIRTUAL_TO_STRING_KV(K(ptr_), K(len_));
    const char *ptr_;
    int32_t len_;
  };
  enum ObHourFlag
  {
    HOUR_UNUSE,
    HOUR_AM,
    HOUR_PM
  };
private:
  // date add / sub / diff.
  static int merge_date_interval(int64_t base_value, const ObString &interval_str,
                                 ObDateUnitType unit_type, int64_t &value, bool is_add);
  static int merge_date_interval(/*const*/ ObTime &base_time, const ObString &interval_str,
                                 ObDateUnitType unit_type, int64_t &value, bool is_add);
  // other utility functions.
  static int validate_datetime(ObTime &ob_time, const ObDateSqlMode date_sql_mode = 0);
  static int validate_time(ObTime &ob_time);
  static int validate_year(int64_t year);
  static int validate_oracle_timestamp(const ObTime &ob_time);
  static int check_leading_precision(const ObTimeDigits &digits);
  static int get_datetime_digits(const char *&str, const char *end, int32_t max_len, ObTimeDigits &digits);
  static int get_datetime_delims(const char *&str, const char *end, ObTimeDelims &delims);
  static int get_datetime_digits_delims(const char *&str, const char *end,
                                        int32_t max_len, ObTimeDigits &digits, ObTimeDelims &delims);
  static int str_to_digit_with_date(const ObString &str, ObTimeDigits *digits, ObTime &obtime);
  static void skip_delims(const char *&str, const char *end);
  static bool is_year4(int64_t first_token_len);
  static bool is_single_colon(const ObTimeDelims &delims);
  static bool is_space_end_with_single_colon(const ObTimeDelims &delims);
  static bool is_single_dot(const ObTimeDelims &delims);
  static bool is_all_spaces(const ObTimeDelims &delims);
  static bool has_any_space(const ObTimeDelims &delims);
  static bool is_negative(const char *&str, const char *end);
  static int normalize_usecond_round(ObTimeDigits &digits);
  static int normalize_usecond_trunc(ObTimeDigits &digits, bool need_trunc);
  static int apply_date_space_rule(const ObTimeDelims *delims);
  static void apply_date_year2_rule(ObTimeDigits &year);
  static void apply_date_year2_rule(int32_t &year);
  static void apply_date_year2_rule(int64_t &year);
  static int apply_usecond_delim_rule(ObTimeDelims &second, ObTimeDigits &usecond);
  static int apply_datetime_for_time_rule(ObTime &ob_time, const ObTimeDigits *digits, const ObTimeDelims *delims);
//  static int find_time_range(int64_t t, const int64_t *range_boundaries, uint64_t higher_bound, uint64_t& result);
//  static int find_transition_type(int64_t t, const ObTimeZoneInfo *sp, TRAN_TYPE_INFO *& result);
  static int add_timezone_offset(const ObTimeZoneInfo *tz_info, int64_t &value);
  static int sub_timezone_offset(const ObTimeZoneInfo *tz_info, int64_t &value);
  static int sub_timezone_offset(const ObTimeZoneInfo *tz_info, bool is_timestamp, const ObString &tz_abbr_str,
      int64_t &value, const bool is_oracle_mode = false);
  static int sub_timezone_offset(const ObTimeZoneInfo &tz_info, const ObString &tz_abbr_str, int64_t &value_us,
      int32_t &offset_min, int32_t &tz_id, int32_t &tran_type_id);
  static int get_str_array_idx(const ObString &str, const ObTimeConstStr *array, int32_t count, int32_t &idx);

  static int data_fmt_nd(char *buffer, int64_t buf_len, int64_t &pos, const int64_t n, int64_t target);
  static int data_fmt_d(char *buffer, int64_t buf_len, int64_t &pos, int64_t target);
  static int data_fmt_s(char *buffer, int64_t buf_len, int64_t &pos, const char *ptr);
  static int get_day_and_month_from_year_day(const int32_t yday, const int32_t year, int32_t &month, int32_t &day);
  static int set_ob_time_year_may_conflict(ObTime &ob_time, int32_t &julian_year_value,
                                           int32_t check_year, int32_t set_year, bool overwrite);

private:
  ObTimeConverter();
  virtual ~ObTimeConverter();
  DISALLOW_COPY_AND_ASSIGN(ObTimeConverter);
};

enum ObNLSFormatEnum {
  NLS_DATE = 0,
  NLS_TIMESTAMP,
  NLS_TIMESTAMP_TZ,
  NLS_MAX,
};

/**
 * @brief The ObDataTypeCastParams struct
 * include session variables for SQL resolver
 * especially for oracle mode, including time zone, datetime format and charset
 */
struct ObDataTypeCastParams {
  ObDataTypeCastParams()
    : tz_info_(NULL),
      force_use_standard_format_(false),
      nls_collation_(CS_TYPE_INVALID),
      nls_collation_nation_(CS_TYPE_INVALID),
      connection_collation_(CS_TYPE_UTF8MB4_BIN),
      part_func_type_(share::schema::PARTITION_FUNC_TYPE_MAX)
  {}
  ObDataTypeCastParams(const ObTimeZoneInfo *tz_info, bool force_use_standard_format = true)
    : tz_info_(tz_info),
      force_use_standard_format_(force_use_standard_format),
      nls_collation_(CS_TYPE_INVALID),
      nls_collation_nation_(CS_TYPE_INVALID),
      connection_collation_(CS_TYPE_UTF8MB4_BIN),
      part_func_type_(share::schema::PARTITION_FUNC_TYPE_MAX)
  {}
  ObDataTypeCastParams(const ObTimeZoneInfo *tz_info,
                       const ObString *nls_formats,
                       const ObCollationType nls_collation,
                       const ObCollationType nls_collation_nation,
                       const ObCollationType connection_collation,
                       const bool force_use_standard_format = false)
    : tz_info_(tz_info),
      force_use_standard_format_(force_use_standard_format),
      nls_collation_(nls_collation),
      nls_collation_nation_(nls_collation_nation),
      connection_collation_(connection_collation),
      part_func_type_(share::schema::PARTITION_FUNC_TYPE_MAX)
  {
    for (int64_t i = 0; NULL != nls_formats && i < NLS_MAX; ++i) {
      session_nls_formats_[i] = nls_formats[i];
    }
  }

  ObString get_nls_format(const ObObjType input_type) const;
  void set_nls_date_format(ObString str);
  void set_nls_timestamp_format(ObString str);
  void set_nls_timestamp_tz_format(ObString str);
  int set_nls_format_by_type(ObObjType type, ObString str);

  const ObTimeZoneInfo *tz_info_;
  ObString session_nls_formats_[NLS_MAX];

  // only user related str depend nls_format. others do not care it, such as ob_print_sql...
  bool force_use_standard_format_;
  
  ObCollationType nls_collation_;
  ObCollationType nls_collation_nation_;
  ObCollationType connection_collation_;  // as client cs for now
  share::schema::ObPartitionFuncType part_func_type_;
};

template <bool calc_date>
int ObTimeConverter::mdatetime_to_ob_time(const ObMySQLDateTime &value, ObTime &ob_time)
{
  int ret = OB_SUCCESS;
  int32_t *parts = ob_time.parts_;
  if (OB_UNLIKELY(MYSQL_ZERO_DATETIME == value.datetime_)) {
    MEMSET(ob_time.parts_, 0, sizeof(*parts) * TOTAL_PART_CNT);
    parts[DT_DATE] = ZERO_DATE;
  } else {
    parts[DT_YEAR] = value.year();
    parts[DT_MON] = value.month();
    parts[DT_MDAY] = value.day_;
    parts[DT_HOUR] = value.hour_;
    parts[DT_MIN] = value.minute_;
    parts[DT_SEC] = value.second_;
    parts[DT_USEC] = value.microseconds_;
    if (calc_date) {
      parts[DT_DATE] = ob_time_to_date(ob_time);
    }
  }
  return ret;
}

template <bool calc_date>
int ObTimeConverter::mdate_to_ob_time(ObMySQLDate value, ObTime &ob_time)
{
  int ret = OB_SUCCESS;
  int32_t *parts = ob_time.parts_;
  if (OB_UNLIKELY(MYSQL_ZERO_DATE == value.date_)) {
    memset(parts, 0, sizeof(*parts) * DATETIME_PART_CNT);
    parts[DT_DATE] = ZERO_DATE;
  } else {
    parts[DT_YEAR] = value.year_;
    parts[DT_MON] = value.month_;
    parts[DT_MDAY] = value.day_;
    if (calc_date) {
      parts[DT_DATE] = ob_time_to_date(ob_time);
    }
  }
  return ret;
}

}// end of common
}// end of oceanbase

#endif
