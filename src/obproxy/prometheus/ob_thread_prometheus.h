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

enum ObProxyRequestType
{
  OBPROXY_SQL_REQUEST = 0,
  OBPROXY_RPC_REQUEST,
  OBPROXY_MAX_REQUEST
};

struct RpcStatInfo {
public:
  RpcStatInfo () : obkv_login_count_(0), obkv_execute_count_(0), obkv_batch_execute_count_(0),
    obkv_batch_execute_shard_count_(0), obkv_execute_query_count_(0), obkv_execute_query_shard_count_(0),
    obkv_query_and_mutate_count_(0), obkv_query_and_mutate_shard_count_(0), obkv_execute_query_sync_count_(0),
    obkv_execute_query_sync_shard_count_(0), obkv_table_ttl_count_(0), obkv_tll_request_count_(0), obkv_direct_load_count_(0),
    obkv_ls_execute_count_(0), obkv_ls_execute_shard_count_(0), obkv_other_count_(0), obkv_other_shard_count_(0) {}
  int64_t obkv_login_count_;
  int64_t obkv_execute_count_;
  int64_t obkv_batch_execute_count_;
  int64_t obkv_batch_execute_shard_count_;
  int64_t obkv_execute_query_count_;
  int64_t obkv_execute_query_shard_count_;
  int64_t obkv_query_and_mutate_count_;
  int64_t obkv_query_and_mutate_shard_count_;
  int64_t obkv_execute_query_sync_count_;
  int64_t obkv_execute_query_sync_shard_count_;
  int64_t obkv_table_ttl_count_;
  int64_t obkv_tll_request_count_;
  int64_t obkv_direct_load_count_;
  int64_t obkv_ls_execute_count_;
  int64_t obkv_ls_execute_shard_count_;
  int64_t obkv_other_count_;       //pcode not support now
  int64_t obkv_other_shard_count_; //pcode not support now
public:
   void add_count(const RpcStatInfo &stat) {
    obkv_login_count_               += stat.obkv_login_count_;
    obkv_execute_count_             += stat.obkv_execute_count_;
    obkv_batch_execute_count_       += stat.obkv_batch_execute_count_;
    obkv_batch_execute_shard_count_ += stat.obkv_batch_execute_shard_count_;
    obkv_execute_query_count_       += stat.obkv_execute_query_count_;
    obkv_execute_query_shard_count_ += stat.obkv_execute_query_shard_count_;
    obkv_query_and_mutate_count_    += stat.obkv_query_and_mutate_count_;
    obkv_query_and_mutate_shard_count_    += stat.obkv_query_and_mutate_shard_count_;
    obkv_execute_query_sync_count_        += stat.obkv_execute_query_sync_count_;
    obkv_execute_query_sync_shard_count_  += stat.obkv_execute_query_sync_shard_count_;
    obkv_table_ttl_count_           += stat.obkv_table_ttl_count_;
    obkv_tll_request_count_         += stat.obkv_tll_request_count_;
    obkv_direct_load_count_         += stat.obkv_direct_load_count_;
    obkv_ls_execute_count_          += stat.obkv_ls_execute_count_;
    obkv_ls_execute_shard_count_    += stat.obkv_ls_execute_shard_count_;
    obkv_other_count_               += stat.obkv_other_count_; //pcode not support now
    obkv_other_shard_count_         += stat.obkv_other_shard_count_; //pcode not support now
  }
  // RpcStatInfo &operator=(const RpcStatInfo& other) {
  //   MEMCPY(this, &other, sizeof(RpcStatInfo));
  //   return *this;
  // }
};

struct SQLstatInfo {
public:
  SQLstatInfo() : select_request_total_time_(0), update_request_total_time_(0), insert_request_total_time_(0),
  delete_request_total_time_(0), other_request_total_time_(0), select_process_request_time_(0), update_process_request_time_(0),
  insert_process_request_time_(0), delete_process_request_time_(0), other_process_request_time_(0),
  select_prepare_send_request_to_server_time_(0), update_prepare_send_request_to_server_time_(0),
  insert_prepare_send_request_to_server_time_(0), delete_prepare_send_request_to_server_time_(0),
  other_prepare_send_request_to_server_time_(0) {}

  void add_count(const SQLstatInfo& other) {
    select_request_total_time_ += other.select_request_total_time_;
    update_request_total_time_ += other.update_request_total_time_;
    insert_request_total_time_ += other.insert_request_total_time_;
    delete_request_total_time_ += other.delete_request_total_time_;
    other_request_total_time_ += other.other_request_total_time_;

    select_process_request_time_ += other.select_process_request_time_;
    update_process_request_time_ += other.update_process_request_time_;
    insert_process_request_time_ += other.insert_process_request_time_;
    delete_process_request_time_ += other.delete_process_request_time_;
    other_process_request_time_ += other.other_process_request_time_;

    select_prepare_send_request_to_server_time_ += other.select_prepare_send_request_to_server_time_;
    update_prepare_send_request_to_server_time_ += other.update_prepare_send_request_to_server_time_;
    insert_prepare_send_request_to_server_time_ += other.insert_prepare_send_request_to_server_time_;
    delete_prepare_send_request_to_server_time_ += other.delete_prepare_send_request_to_server_time_;
    other_prepare_send_request_to_server_time_ += other.other_prepare_send_request_to_server_time_;
  }

public:
  int64_t select_request_total_time_;
  int64_t update_request_total_time_;
  int64_t insert_request_total_time_;
  int64_t delete_request_total_time_;
  int64_t other_request_total_time_;
  int64_t select_process_request_time_;
  int64_t update_process_request_time_;
  int64_t insert_process_request_time_;
  int64_t delete_process_request_time_;
  int64_t other_process_request_time_;
  int64_t select_prepare_send_request_to_server_time_;
  int64_t update_prepare_send_request_to_server_time_;
  int64_t insert_prepare_send_request_to_server_time_;
  int64_t delete_prepare_send_request_to_server_time_;
  int64_t other_prepare_send_request_to_server_time_;
};

struct SQLMonitorInfo {
  SQLMonitorInfo() : request_type_(OBPROXY_SQL_REQUEST), request_count_(0), select_count_(0), update_count_(0), insert_count_(0),
    delete_count_(0), other_count_(0), rpc_request_stat_count_(),
    request_total_time_(0), server_process_request_time_(0),
    prepare_send_request_to_server_time_(0), cluster_name_(0), tenant_name_(0),
    cluster_name_str_(), tenant_name_str_() {}

  SQLMonitorInfo(const SQLMonitorInfo& other) {
    *this = other;
  }

  SQLMonitorInfo &operator=(const SQLMonitorInfo& other) {
    if (OB_LIKELY(this != &other)) {
      this->request_count_ = other.request_type_;
      this->request_count_ = other.request_count_;
      this->select_count_ = other.select_count_;
      this->update_count_ = other.update_count_;
      this->insert_count_ = other.insert_count_;
      this->delete_count_ = other.delete_count_;
      this->other_count_ = other.other_count_;
      this->rpc_request_stat_count_ = other.rpc_request_stat_count_; //default copy
      this->sql_request_stat_count_ = other.sql_request_stat_count_; //default copy
      this->request_total_time_ = other.request_total_time_;
      this->server_process_request_time_ = other.server_process_request_time_;
      this->prepare_send_request_to_server_time_ = other.prepare_send_request_to_server_time_;
      MEMCPY(this->cluster_name_str_, other.cluster_name_str_, OB_PROXY_MAX_CLUSTER_NAME_LENGTH);
      MEMCPY(this->tenant_name_str_, other.tenant_name_str_, oceanbase::common::OB_MAX_TENANT_NAME_LENGTH);
      this->cluster_name_.assign_ptr(this->cluster_name_str_, other.cluster_name_.length());
      this->tenant_name_.assign_ptr(this->tenant_name_str_, other.tenant_name_.length());
    }
    return *this;
  }

  ObProxyRequestType request_type_;
  int64_t request_count_;
  int64_t select_count_;
  int64_t update_count_;
  int64_t insert_count_;
  int64_t delete_count_;
  int64_t other_count_;
  RpcStatInfo rpc_request_stat_count_; // sql has not so much item to use union, so each type use its count_ only, convert it to union if need TODO
  SQLstatInfo sql_request_stat_count_;
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
  ObThreadPrometheus() : monitor_info_used_(0), monitor_info_hash_map_(), rpc_monitor_info_used_(0), rpc_monitor_info_hash_map_(), sql_monitor_info_cont_(NULL), thread_(NULL) {}
  ~ObThreadPrometheus() {}
  int set_sql_monitor_info(const common::ObString &tenant_name, const common::ObString &cluster_name, const SQLMonitorInfo &info, const ObProxyRequestType type = OBPROXY_SQL_REQUEST);

private:
  bool set_sql_monitor_info_using_array(const common::ObString &tenant_name, const common::ObString &cluster_name, const SQLMonitorInfo &info, SQLMonitorInfo (&monitor_info_array)[SQL_MONITOR_INFO_ARRAY_SIZE], int64_t &monitor_info_used);
  int set_sql_monitor_info_using_hashmap(const common::ObString &tenant_name, const common::ObString &cluster_name, const SQLMonitorInfo &info, MonitorInfoHashMap &monitor_info_map);

public:
  // For monitoring information, it is stored in the form of array + hashmap,
  // mainly because the performance of get and set operations of hashmap has a great impact.
  // About 3% loss, use hashmap when the data is full
  // If the CPU tenant isolation is implemented later,
  // the tenant corresponding to a thread is fixed, and only one position in the array is used.
  SQLMonitorInfo monitor_info_array_[SQL_MONITOR_INFO_ARRAY_SIZE];
  int64_t monitor_info_used_;
  MonitorInfoHashMap monitor_info_hash_map_;

  SQLMonitorInfo rpc_monitor_info_array_[SQL_MONITOR_INFO_ARRAY_SIZE]; //size of array must be same with monitor_info_array_, used by set_sql_monitor_info_using_array
  int64_t rpc_monitor_info_used_;
  MonitorInfoHashMap rpc_monitor_info_hash_map_;
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
