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

#define USING_LOG_PREFIX PROXY

#include "prometheus/ob_thread_prometheus.h"
#include "obutils/ob_proxy_config.h"
#include "utils/ob_proxy_hot_upgrader.h"
#include "iocore/net/ob_net_def.h"
#include "opsql/parser/ob_proxy_parse_result.h"
#include "prometheus/ob_prometheus_info.h"
#include "prometheus/ob_sql_prometheus.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace prometheus
{

int64_t SQLMonitorInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(request_count), K_(request_total_time), K_(server_process_request_time),
       K_(prepare_send_request_to_server_time), K_(cluster_name), K_(tenant_name));
  J_OBJ_END();
  return pos;
}

int ObSQLMonitorInfoCont::init(int64_t report_interval_us, ObEThread *thread, ObThreadPrometheus *thread_prometheus)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(report_interval_us <= 0 || NULL == thread || NULL == thread_prometheus)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(report_interval_us), K(ret));
  } else {
    report_interval_us_ = report_interval_us;
    thread_ = thread;
    thread_prometheus_ = thread_prometheus;
    is_inited_ = true;
  }

  return ret;
}

int ObSQLMonitorInfoCont::set_report_interval(const int64_t interval)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(interval <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid interval value", K(interval), K(ret));
  } else {
    report_interval_us_ = interval;
  }

  return ret;
}

int ObSQLMonitorInfoCont::schedule_report_prometheus_info()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K_(is_inited), K(ret));
  } else if (get_global_hot_upgrade_info().is_graceful_exit_timeout(get_hrtime())) {
    LOG_WARN("proxy need exit now");
  } else if (OB_UNLIKELY(!thread_->is_event_thread_type(ET_NET))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("sql monitor info cont must be scheduled in net thread", K(ret));
  } else if (OB_ISNULL(thread_->schedule_in(this, HRTIME_USECONDS(report_interval_us_), EVENT_NONE))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to schedule report prometheus info", K(ret));
  }

  return ret;
}

int ObSQLMonitorInfoCont::main_handler(int event, void *data)
{
  UNUSED(event);
  UNUSED(data);
  int ret = OB_SUCCESS;

  for (int64_t i = 0; i < thread_prometheus_->monitor_info_used_; i++) {
    SQLMonitorInfo &info = thread_prometheus_->monitor_info_array_[i];
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_INVALID, PROMETHEUS_REQUEST_COUNT, false, false);
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_INVALID, PROMETHEUS_REQUEST_TOTAL_TIME, hrtime_to_usec(info.request_total_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_INVALID, PROMETHEUS_SERVER_PROCESS_REQUEST_TIME, hrtime_to_usec(info.server_process_request_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_INVALID, PROMETHEUS_PREPARE_SEND_REQUEST_TIME, hrtime_to_usec(info.prepare_send_request_to_server_time_));
  }
  thread_prometheus_->monitor_info_used_ = 0;
  memset(thread_prometheus_->monitor_info_array_, 0, sizeof(thread_prometheus_->monitor_info_array_));

  MonitorInfoHashMap::iterator iter = thread_prometheus_->monitor_info_hash_map_.begin();
  for(; iter != thread_prometheus_->monitor_info_hash_map_.end(); ++iter) {
    SQLMonitorInfo &info = iter->second;
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_INVALID, PROMETHEUS_REQUEST_COUNT, false, false);
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_INVALID, PROMETHEUS_REQUEST_TOTAL_TIME, hrtime_to_usec(info.request_total_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_INVALID, PROMETHEUS_SERVER_PROCESS_REQUEST_TIME, hrtime_to_usec(info.server_process_request_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_INVALID, PROMETHEUS_PREPARE_SEND_REQUEST_TIME, hrtime_to_usec(info.prepare_send_request_to_server_time_));
  }
  thread_prometheus_->monitor_info_hash_map_.reuse();

  if (OB_FAIL(schedule_report_prometheus_info())) {
    LOG_WARN("schedule report prometheus info failed", K(ret));
  }
  return ret;
}

void ObSQLMonitorInfoCont::kill_this()
{
  if (is_inited_) {
    LOG_INFO("ObSQLMonitorInfoCont will kill self");
    report_interval_us_ = 0;
    thread_ = NULL;
  }

  op_free(this);
}

int ObThreadPrometheus::init(ObEThread *thread)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == thread)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(monitor_info_hash_map_.create(16, ObModIds::OB_PROMETHEUS_RELATED))) {
    LOG_WARN("monitor info hash map create failed", K(ret));
  } else if (OB_ISNULL(sql_monitor_info_cont_ = op_alloc(ObSQLMonitorInfoCont))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ObSQLMonitorInfoCont", K(ret));
  } else if (OB_FAIL(sql_monitor_info_cont_->init(
              get_global_proxy_config().prometheus_sync_interval / 5, thread, this))) {
    LOG_WARN("set report interval failed", K(ret));
  } else if (OB_FAIL(sql_monitor_info_cont_->schedule_report_prometheus_info())) {
    LOG_WARN("schedule report prometheus info failed", K(ret));
  } else {
    thread_ = thread;
  }

  if (OB_FAIL(ret)) {
    if (NULL != sql_monitor_info_cont_) {
      sql_monitor_info_cont_->kill_this();
      sql_monitor_info_cont_ = NULL;
    }
    thread_ = NULL;
  }

  return ret;
}

int ObThreadPrometheus::set_sql_monitor_info(const ObString &tenant_name,
                                             const ObString &cluster_name,
                                             const SQLMonitorInfo &info)
{
  int ret = OB_SUCCESS;
  if (set_sql_monitor_info_using_array(tenant_name, cluster_name, info)) {
    LOG_DEBUG("set sql monitor info using array", K(tenant_name), K(cluster_name), K(ret));
  } else if (OB_FAIL(set_sql_monitor_info_using_hashmap(tenant_name, cluster_name, info))) {
    LOG_WARN("set sql monitor info using hashmap failed", K(tenant_name), K(cluster_name), K(info), K(ret));
  }

  return ret;
}

int ObThreadPrometheus::set_sql_monitor_info_using_hashmap(const ObString &tenant_name,
                                                           const ObString &cluster_name,
                                                           const SQLMonitorInfo &info)
{
  int ret = OB_SUCCESS;
  ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH> key_string;
  SQLMonitorInfo monitor_info;
  if (OB_UNLIKELY(tenant_name.empty() || cluster_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_name), K(cluster_name), K(ret));
  } else if (OB_FAIL(paste_tenant_and_cluster_name(tenant_name, cluster_name, key_string))) {
    LOG_WARN("paste tenant and cluster name failed", K(tenant_name), K(cluster_name), K(ret));
  } else if (OB_FAIL(monitor_info_hash_map_.get_refactored(key_string, monitor_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      memset(&monitor_info, 0, sizeof(monitor_info));
      MEMCPY(monitor_info.cluster_name_str_, cluster_name.ptr(), cluster_name.length());
      MEMCPY(monitor_info.tenant_name_str_, tenant_name.ptr(), tenant_name.length());
      ret = OB_SUCCESS;
    }
  }

  if (OB_SUCC(ret)) {
    monitor_info.request_count_ += info.request_count_;
    monitor_info.request_total_time_ += info.request_count_;
    monitor_info.server_process_request_time_ += info.server_process_request_time_;
    monitor_info.prepare_send_request_to_server_time_ += info.prepare_send_request_to_server_time_;
    if (OB_FAIL(monitor_info_hash_map_.set_refactored(key_string, monitor_info, 1))) {
      LOG_WARN("monitor info hash map set failed", K(ret));
    }
  }

  return ret;
}

bool ObThreadPrometheus::set_sql_monitor_info_using_array(const ObString &tenant_name,
                                                          const ObString &cluster_name,
                                                          const SQLMonitorInfo &info)
{
  bool bret = false;
  int64_t index = -1;
  for (int64_t i = 0; !bret && i < monitor_info_used_; i++) {
    if (monitor_info_array_[i].tenant_name_ == tenant_name && monitor_info_array_[i].cluster_name_ == cluster_name) {
      bret = true;
      index = i;
    } 
  }

  if (!bret && monitor_info_used_ < SQL_MONITOR_INFO_ARRAY_SIZE) {
    memset(&monitor_info_array_[monitor_info_used_], 0, sizeof(monitor_info_array_[monitor_info_used_]));
    MEMCPY(monitor_info_array_[monitor_info_used_].cluster_name_str_, cluster_name.ptr(), cluster_name.length());
    MEMCPY(monitor_info_array_[monitor_info_used_].tenant_name_str_, tenant_name.ptr(), tenant_name.length());
    monitor_info_array_[monitor_info_used_].tenant_name_.assign_ptr(monitor_info_array_[monitor_info_used_].tenant_name_str_, tenant_name.length());
    monitor_info_array_[monitor_info_used_].cluster_name_.assign_ptr(monitor_info_array_[monitor_info_used_].cluster_name_str_, cluster_name.length());
    index = monitor_info_used_;
    monitor_info_used_++;
    bret = true;
  }

  if (bret) {
    monitor_info_array_[index].request_count_ += info.request_count_;
    monitor_info_array_[index].request_total_time_ += info.request_count_;
    monitor_info_array_[index].server_process_request_time_ += info.server_process_request_time_;
    monitor_info_array_[index].prepare_send_request_to_server_time_ += info.prepare_send_request_to_server_time_; 
  }

  return bret;
}

} // end of prometheus
} // end of obproxy
} // end of oceanbase
