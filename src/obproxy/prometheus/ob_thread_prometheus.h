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
 *
 * *************************************************************
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef OB_THREAD_PROMETHEUS_PROCESSOR_H_
#define OB_THREAD_PROMETHEUS_PROCESSOR_H_

#include "lib/hash/ob_hashmap.h"
#include "lib/string/ob_string.h"
#include "obutils/ob_async_common_task.h"
#include "iocore/eventsystem/ob_continuation.h"
#include "iocore/eventsystem/ob_ethread.h"

#define SQL_MONITOR_INFO_ARRAY_SIZE 4

namespace oceanbase
{
namespace obproxy
{
namespace prometheus
{

struct SQLMonitorInfo {
  int64_t request_count_;
  int64_t request_total_time_;
  int64_t server_process_request_time_;
  int64_t prepare_send_request_to_server_time_;
  common::ObString cluster_name_;
  common::ObString tenant_name_;

  char cluster_name_str_[OB_PROXY_MAX_CLUSTER_NAME_LENGTH];
  char tenant_name_str_[oceanbase::common::OB_MAX_TENANT_NAME_LENGTH];

  int64_t to_string(char *buf, const int64_t buf_len) const;
};

typedef common::hash::ObHashMap<common::ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH>, SQLMonitorInfo> MonitorInfoHashMap;

class ObSQLMonitorInfoCont : public event::ObContinuation
{
public:
  ObSQLMonitorInfoCont() : ObContinuation(NULL), is_inited_(false), report_interval_us_(0),
                           thread_(NULL), thread_prometheus_(NULL)
  {
    SET_HANDLER(&ObSQLMonitorInfoCont::main_handler);
  }
  ~ObSQLMonitorInfoCont() {}
  int init(int64_t report_interval_us, event::ObEThread *thread, ObThreadPrometheus *thread_prometheus);
  int set_report_interval(const int64_t interval);
  int schedule_report_prometheus_info();
  void kill_this();

private:
  int main_handler(int event, void *data);

private:
  bool is_inited_;
  int64_t report_interval_us_;
  event::ObEThread *thread_;
  ObThreadPrometheus *thread_prometheus_;

  DISALLOW_COPY_AND_ASSIGN(ObSQLMonitorInfoCont);
};

class ObThreadPrometheus
{
public:
  int init(event::ObEThread *thread);
  ObThreadPrometheus() : monitor_info_used_(0), monitor_info_hash_map_(), sql_monitor_info_cont_(NULL), thread_(NULL) {}
  ~ObThreadPrometheus() {}
  int set_sql_monitor_info(const common::ObString &tenant_name, const common::ObString &cluster_name, const SQLMonitorInfo &info);

private:
  bool set_sql_monitor_info_using_array(const common::ObString &tenant_name, const common::ObString &cluster_name, const SQLMonitorInfo &info);
  int set_sql_monitor_info_using_hashmap(const common::ObString &tenant_name, const common::ObString &cluster_name, const SQLMonitorInfo &info);

public:
  // For monitoring information, it is stored in the form of array + hashmap,
  // mainly because the performance of get and set operations of hashmap has a great impact.
  // About 3% loss, use hashmap when the data is full
  // If the CPU tenant isolation is implemented later,
  // the tenant corresponding to a thread is fixed, and only one position in the array is used.
  SQLMonitorInfo monitor_info_array_[SQL_MONITOR_INFO_ARRAY_SIZE];
  int64_t monitor_info_used_;
  MonitorInfoHashMap monitor_info_hash_map_;
private:
  ObSQLMonitorInfoCont *sql_monitor_info_cont_;
  event::ObEThread *thread_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObThreadPrometheus);
};

} // end of prometheus
} // end of obproxy
} // end of oceanbase

#endif
