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
#include "utils/ob_proxy_monitor_utils.h"
#include "lib/time/ob_time_utility.h"
#include "lib/time/ob_hrtime.h"

namespace oceanbase
{
namespace obproxy
{

using namespace oceanbase::common;

const char* ObProxyMonitorUtils::get_database_type_name(const DBServerType type)
{
  const char* str_ret = "";
  switch (type) {
    case DB_MYSQL:
      str_ret = "MYSQL";
      break;
    case DB_OB_MYSQL:
      str_ret = "OB_MYSQL";
      break;
    case DB_OB_ORACLE:
      str_ret = "OB_ORACLE";
      break;
    default:
      str_ret = "UNKNOWN_DBTYPE";
      break;
  }
  return str_ret;
}

int ObProxyMonitorUtils::sql_escape(const char *sql, const int32_t sql_len,
                                    char *new_sql, const int32_t new_sql_size,
                                    int32_t &new_sql_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql) || OB_UNLIKELY(sql_len <= 0)
   || OB_ISNULL(new_sql) || OB_UNLIKELY(new_sql_size <= 0)) {
    new_sql_len = 0;
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid_argument", KP(sql), K(sql_len), KP(new_sql), K(new_sql_size), K(ret));
  } else {
    int32_t i = 0;
    for (i = 0, new_sql_len = 0; i < sql_len && new_sql_len < new_sql_size; i++, sql++) {
      if (*sql == '\n' || *sql == ',') {
        if (new_sql_size - new_sql_len < 3) {
          break;
        }

        if (*sql == '\n') {
          MEMCPY(new_sql, "%0A", 3);
        } else if (*sql == ',') {
          MEMCPY(new_sql, "%2C", 3);
        }

        new_sql_len+=3;
        new_sql+=3;
      } else {
        *new_sql = *sql;

        new_sql_len++;
        new_sql++;
      }
    }
  }

  return ret;
}

int64_t ObProxyMonitorUtils::get_next_schedule_time(int64_t interval_us)
{
  //每次调度到 interval 的整数倍上
  //减掉 500ms, 便于监控统计
  //这里是按照一定间隔输出, 同一次输出的不同项可能分布在两边, 比如 00:00:59.999998, 00:01:00:000028
  //监控都是直接截断到秒或者分钟, 会统计到不同的两分钟里, 但他们是同一次的输出, 理应统计到一起
  const int64_t current_time = ObTimeUtility::current_time();
  int64_t next_time = (current_time / interval_us + 1) * interval_us - msec_to_usec(500) - current_time;
  // 因为定时任务的时间不精确, 比如期望在 11:23:20.500 执行, 但是实际在 11:23:20.480 执行了, 在计算下次执行时间时, 又会期望在 11:23:20.500 执行
  // 误差在 100ms 内就调到下一次去执行
  while (next_time <= msec_to_usec(100)) {
    next_time += interval_us;
  }
  return next_time;
}

} // end of namespace obproxy
} // end of namespace oceanbase
