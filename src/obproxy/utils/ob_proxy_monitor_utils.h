/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_MONITOR_UTILS_H
#define OBPROXY_MONITOR_UTILS_H
#include "lib/ob_define.h"

namespace oceanbase
{
namespace obproxy
{

class ObProxyMonitorUtils
{
public:
  static const char* get_database_type_name(const common::DBServerType type);
  static int sql_escape(const char *sql, const int32_t sql_len,
                        char *new_sql, const int32_t new_sql_size,
                        int32_t &new_sql_len);
  static int64_t get_next_schedule_time(int64_t interval_us);
};

} // end of namespace obproxy
} // end of namespace oceanbase

#endif  // OBPROXY_MONITOR_UTILS_H
