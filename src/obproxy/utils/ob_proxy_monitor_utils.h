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
