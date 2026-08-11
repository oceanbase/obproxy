/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX COMMON

#include "lib/signal/ob_signal_utils.h"
#include <time.h>
#include <sys/time.h>
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/charset/ob_mysql_global.h"
#include "lib/signal/ob_libunwind.h"

extern "C" {
extern int ob_poll(struct pollfd *__fds, nfds_t __nfds, int __timeout);
extern int64_t get_rel_offset_c(int64_t addr);
};

namespace oceanbase
{
namespace common
{

/* From mongodb */
void safe_sleep_micros(int64_t usec)
{
  auto nsec = usec * 1000;
  constexpr static int64_t k1E9 = 1000000000;
  timespec ts{nsec / k1E9, nsec % k1E9};
  nanosleep(&ts, nullptr);
}

#define LEAPOCH (946684800LL + 86400*(31+29))
#define DAYS_PER_400Y (365*400 + 97)
#define DAYS_PER_100Y (365*100 + 24)
#define DAYS_PER_4Y   (365*4   + 1)

int safe_secs_to_tm(long long t, struct tm *tm)
{
  long long days, secs;
  int remdays, remsecs, remyears;
  int qc_cycles, c_cycles, q_cycles;
  int years, months;
  int wday, yday, leap;
  static const char days_in_month[] = {31,30,31,30,31,31,30,31,30,31,31,29};

  if (t < INT_MIN * 31622400LL || t > INT_MAX * 31622400LL)
    return -1;

  secs = t - LEAPOCH;
  days = secs / 86400;
  remsecs = secs % 86400;
  if (remsecs < 0) {
    remsecs += 86400;
    days--;
  }

  wday = (3+days)%7;
  if (wday < 0) wday += 7;

  qc_cycles = days / DAYS_PER_400Y;
  remdays = days % DAYS_PER_400Y;
  if (remdays < 0) {
    remdays += DAYS_PER_400Y;
    qc_cycles--;
  }

  c_cycles = remdays / DAYS_PER_100Y;
  if (c_cycles == 4) c_cycles--;
  remdays -= c_cycles * DAYS_PER_100Y;

  q_cycles = remdays / DAYS_PER_4Y;
  if (q_cycles == 25) q_cycles--;
  remdays -= q_cycles * DAYS_PER_4Y;

  remyears = remdays / 365;
  if (remyears == 4) remyears--;
  remdays -= remyears * 365;

  leap = !remyears && (q_cycles || !c_cycles);
  yday = remdays + 31 + 28 + leap;
  if (yday >= 365+leap) yday -= 365+leap;

  years = remyears + 4*q_cycles + 100*c_cycles + 400*qc_cycles;

  for (months=0; days_in_month[months] <= remdays; months++)
    remdays -= days_in_month[months];

  if (years+100 > INT_MAX || years+100 < INT_MIN)
    return -1;

  tm->tm_year = years + 100;
  tm->tm_mon = months + 2;
  if (tm->tm_mon >= 12) {
    tm->tm_mon -=12;
    tm->tm_year++;
  }
  tm->tm_mday = remdays + 1;
  tm->tm_wday = wday;
  tm->tm_yday = yday;

  tm->tm_hour = remsecs / 3600;
  tm->tm_min = remsecs / 60 % 60;
  tm->tm_sec = remsecs % 60;

  return 0;
}

void safe_current_datetime_str(char *buf, int64_t len, int64_t &pos)
{
  pos = 0;
  struct timespec ts = {0, 0};
  clock_gettime(CLOCK_REALTIME, &ts);
  struct tm tm;
  safe_secs_to_tm(ts.tv_sec, &tm);
  int64_t count = snprintf(buf, len, "%d%d%d%d%d%d",
                                tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                                tm.tm_hour, tm.tm_min, tm.tm_sec);
  if (count >= 0 && count < len) {
    pos = count;
  } else { pos = 0;/*overflow*/}
}

void safe_current_datetime_str_v2(char *buf, int64_t len, int64_t &pos)
{
  pos = 0;
  struct timespec ts = {0, 0};
  clock_gettime(CLOCK_REALTIME, &ts);
  struct tm tm;
  safe_secs_to_tm(ts.tv_sec, &tm);
  int64_t count = snprintf(buf, len, "%d-%d-%d %d:%d:%d.%ld",
                                tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                                tm.tm_hour, tm.tm_min, tm.tm_sec, (long)(ts.tv_nsec * 1e-3));
  if (count >= 0 && count < len) {
    pos = count;
  } else { pos = 0;/*overflow*/}
}

// [TODO] need use lnprintf
int64_t safe_parray(char *buf, int64_t len, int64_t *array, int size)
{
  int64_t pos = 0;
  if (NULL != buf && len > 0 && NULL != array) {
    int64_t count = 0;
    for (int64_t i = 0; i < size; i++) {
      int64_t addr = get_rel_offset_c(array[i]);
      if (0 == i) {
        count = snprintf(buf + pos, len - pos, "0x%lx", addr);
      } else {
        count = snprintf(buf + pos, len - pos, " 0x%lx", addr);
      }
      if (count >= 0 && pos + count < len) {
        pos += count;
      } else {
        // buf not enough
        break;
      }
    }
    buf[pos] = 0;
  }
  return pos;
}

} // namespace common
} // namespace oceanbase

extern "C" {
  int64_t safe_parray_c(char *buf, int64_t len, int64_t *array, int size)
  {
    return oceanbase::common::safe_parray(buf, len, array, size);
  }
}
