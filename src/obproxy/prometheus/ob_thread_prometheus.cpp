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
#include "prometheus/ob_rpc_prometheus.h"

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
    LOG_WDIAG("invalid argument", K(report_interval_us), K(ret));
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
    LOG_WDIAG("invalid interval value", K(interval), K(ret));
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
    LOG_WDIAG("not inited", K_(is_inited), K(ret));
  } else if (get_global_hot_upgrade_info().is_graceful_exit_timeout(get_hrtime())) {
    LOG_WDIAG("proxy need exit now");
  } else if (OB_UNLIKELY(!thread_->is_event_thread_type(ET_NET))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("sql monitor info cont must be scheduled in net thread", K(ret));
  } else if (OB_ISNULL(thread_->schedule_in(this, HRTIME_USECONDS(report_interval_us_), EVENT_NONE))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to schedule report prometheus info", K(ret));
  }

  return ret;
}

int ObSQLMonitorInfoCont::main_handler(int event, void *data)
{
  UNUSED(event);
  UNUSED(data);
  int ret = OB_SUCCESS;
  //handle sql monitor info
  for (int64_t i = 0; i < thread_prometheus_->monitor_info_used_; i++) {
    SQLMonitorInfo &info = thread_prometheus_->monitor_info_array_[i];
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_SELECT, PROMETHEUS_REQUEST_COUNT, false, false, true, info.select_count_);
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_UPDATE, PROMETHEUS_REQUEST_COUNT, false, false, true, info.update_count_);
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_INSERT, PROMETHEUS_REQUEST_COUNT, false, false, true, info.insert_count_);
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_DELETE, PROMETHEUS_REQUEST_COUNT, false, false, true, info.delete_count_);
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_OTHERS, PROMETHEUS_REQUEST_COUNT, false, false, true, info.other_count_);
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_INVALID, PROMETHEUS_REQUEST_TOTAL_TIME, hrtime_to_usec(info.request_total_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_SELECT, PROMETHEUS_REQUEST_TOTAL_TIME, hrtime_to_usec(info.sql_request_stat_count_.select_request_total_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_UPDATE, PROMETHEUS_REQUEST_TOTAL_TIME, hrtime_to_usec(info.sql_request_stat_count_.update_request_total_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_INSERT, PROMETHEUS_REQUEST_TOTAL_TIME, hrtime_to_usec(info.sql_request_stat_count_.insert_request_total_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_DELETE, PROMETHEUS_REQUEST_TOTAL_TIME, hrtime_to_usec(info.sql_request_stat_count_.delete_request_total_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_OTHERS, PROMETHEUS_REQUEST_TOTAL_TIME, hrtime_to_usec(info.sql_request_stat_count_.other_request_total_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_INVALID, PROMETHEUS_SERVER_PROCESS_REQUEST_TIME, hrtime_to_usec(info.server_process_request_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_SELECT, PROMETHEUS_SERVER_PROCESS_REQUEST_TIME, hrtime_to_usec(info.sql_request_stat_count_.select_process_request_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_UPDATE, PROMETHEUS_SERVER_PROCESS_REQUEST_TIME, hrtime_to_usec(info.sql_request_stat_count_.update_process_request_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_INSERT, PROMETHEUS_SERVER_PROCESS_REQUEST_TIME, hrtime_to_usec(info.sql_request_stat_count_.insert_process_request_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_DELETE, PROMETHEUS_SERVER_PROCESS_REQUEST_TIME, hrtime_to_usec(info.sql_request_stat_count_.delete_process_request_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_OTHERS, PROMETHEUS_SERVER_PROCESS_REQUEST_TIME, hrtime_to_usec(info.sql_request_stat_count_.other_process_request_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_INVALID, PROMETHEUS_PREPARE_SEND_REQUEST_TIME, hrtime_to_usec(info.prepare_send_request_to_server_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_SELECT, PROMETHEUS_PREPARE_SEND_REQUEST_TIME, hrtime_to_usec(info.sql_request_stat_count_.select_prepare_send_request_to_server_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_UPDATE, PROMETHEUS_PREPARE_SEND_REQUEST_TIME, hrtime_to_usec(info.sql_request_stat_count_.update_prepare_send_request_to_server_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_INSERT, PROMETHEUS_PREPARE_SEND_REQUEST_TIME, hrtime_to_usec(info.sql_request_stat_count_.insert_prepare_send_request_to_server_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_DELETE, PROMETHEUS_PREPARE_SEND_REQUEST_TIME, hrtime_to_usec(info.sql_request_stat_count_.delete_prepare_send_request_to_server_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_OTHERS, PROMETHEUS_PREPARE_SEND_REQUEST_TIME, hrtime_to_usec(info.sql_request_stat_count_.other_prepare_send_request_to_server_time_));
  }
  thread_prometheus_->monitor_info_used_ = 0;
  memset(thread_prometheus_->monitor_info_array_, 0, sizeof(thread_prometheus_->monitor_info_array_));

  MonitorInfoHashMap::iterator iter = thread_prometheus_->monitor_info_hash_map_.begin();
  for(; iter != thread_prometheus_->monitor_info_hash_map_.end(); ++iter) {
    SQLMonitorInfo &info = iter->second;
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_SELECT, PROMETHEUS_REQUEST_COUNT, false, false, true, info.select_count_);
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_UPDATE, PROMETHEUS_REQUEST_COUNT, false, false, true, info.update_count_);
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_INSERT, PROMETHEUS_REQUEST_COUNT, false, false, true, info.insert_count_);
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_DELETE, PROMETHEUS_REQUEST_COUNT, false, false, true, info.delete_count_);
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_OTHERS, PROMETHEUS_REQUEST_COUNT, false, false, true, info.other_count_);
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_INVALID, PROMETHEUS_REQUEST_TOTAL_TIME, hrtime_to_usec(info.request_total_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_SELECT, PROMETHEUS_REQUEST_TOTAL_TIME, hrtime_to_usec(info.sql_request_stat_count_.select_request_total_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_UPDATE, PROMETHEUS_REQUEST_TOTAL_TIME, hrtime_to_usec(info.sql_request_stat_count_.update_request_total_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_INSERT, PROMETHEUS_REQUEST_TOTAL_TIME, hrtime_to_usec(info.sql_request_stat_count_.insert_request_total_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_DELETE, PROMETHEUS_REQUEST_TOTAL_TIME, hrtime_to_usec(info.sql_request_stat_count_.delete_request_total_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_OTHERS, PROMETHEUS_REQUEST_TOTAL_TIME, hrtime_to_usec(info.sql_request_stat_count_.other_request_total_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_INVALID, PROMETHEUS_SERVER_PROCESS_REQUEST_TIME, hrtime_to_usec(info.server_process_request_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_SELECT, PROMETHEUS_SERVER_PROCESS_REQUEST_TIME, hrtime_to_usec(info.sql_request_stat_count_.select_process_request_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_UPDATE, PROMETHEUS_SERVER_PROCESS_REQUEST_TIME, hrtime_to_usec(info.sql_request_stat_count_.update_process_request_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_INSERT, PROMETHEUS_SERVER_PROCESS_REQUEST_TIME, hrtime_to_usec(info.sql_request_stat_count_.insert_process_request_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_DELETE, PROMETHEUS_SERVER_PROCESS_REQUEST_TIME, hrtime_to_usec(info.sql_request_stat_count_.delete_process_request_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_OTHERS, PROMETHEUS_SERVER_PROCESS_REQUEST_TIME, hrtime_to_usec(info.sql_request_stat_count_.other_process_request_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_INVALID, PROMETHEUS_PREPARE_SEND_REQUEST_TIME, hrtime_to_usec(info.prepare_send_request_to_server_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_SELECT, PROMETHEUS_PREPARE_SEND_REQUEST_TIME, hrtime_to_usec(info.sql_request_stat_count_.select_prepare_send_request_to_server_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_UPDATE, PROMETHEUS_PREPARE_SEND_REQUEST_TIME, hrtime_to_usec(info.sql_request_stat_count_.update_prepare_send_request_to_server_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_INSERT, PROMETHEUS_PREPARE_SEND_REQUEST_TIME, hrtime_to_usec(info.sql_request_stat_count_.insert_prepare_send_request_to_server_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_DELETE, PROMETHEUS_PREPARE_SEND_REQUEST_TIME, hrtime_to_usec(info.sql_request_stat_count_.delete_prepare_send_request_to_server_time_));
    SQL_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", OBPROXY_T_OTHERS, PROMETHEUS_PREPARE_SEND_REQUEST_TIME, hrtime_to_usec(info.sql_request_stat_count_.other_prepare_send_request_to_server_time_));
  }
  thread_prometheus_->monitor_info_hash_map_.reuse();

  //handle rpc monitor info
  for (int64_t i = 0; i < thread_prometheus_->rpc_monitor_info_used_; i++) {
    SQLMonitorInfo &info = thread_prometheus_->rpc_monitor_info_array_[i];
    //single request stat
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_TABLE_API_LOGIN, PROMETHEUS_REQUEST_COUNT, false, false, false, info.rpc_request_stat_count_.obkv_login_count_);
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_TABLE_API_EXECUTE, PROMETHEUS_REQUEST_COUNT, false, false, false, info.rpc_request_stat_count_.obkv_execute_count_);
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_TABLE_API_BATCH_EXECUTE, PROMETHEUS_REQUEST_COUNT, false, false, false, info.rpc_request_stat_count_.obkv_batch_execute_count_);
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_TABLE_API_EXECUTE_QUERY, PROMETHEUS_REQUEST_COUNT, false, false, false, info.rpc_request_stat_count_.obkv_execute_query_count_);
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_TABLE_API_QUERY_AND_MUTATE, PROMETHEUS_REQUEST_COUNT, false, false, false, info.rpc_request_stat_count_.obkv_query_and_mutate_count_);
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_TABLE_API_EXECUTE_QUERY_SYNC, PROMETHEUS_REQUEST_COUNT, false, false, false, info.rpc_request_stat_count_.obkv_execute_query_sync_count_);
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_TABLE_TTL, PROMETHEUS_REQUEST_COUNT, false, false, false, info.rpc_request_stat_count_.obkv_table_ttl_count_);
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_TTL_REQUEST, PROMETHEUS_REQUEST_COUNT, false, false, false, info.rpc_request_stat_count_.obkv_tll_request_count_);
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_TABLE_API_DIRECT_LOAD, PROMETHEUS_REQUEST_COUNT, false, false, false, info.rpc_request_stat_count_.obkv_direct_load_count_);
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_TABLE_API_LS_EXECUTE, PROMETHEUS_REQUEST_COUNT, false, false, false, info.rpc_request_stat_count_.obkv_ls_execute_count_);
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_INVALID_RPC_CODE, PROMETHEUS_REQUEST_COUNT, false, false, false, info.rpc_request_stat_count_.obkv_other_count_);
    //shard request stat
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_TABLE_API_BATCH_EXECUTE, PROMETHEUS_REQUEST_COUNT, false, false, true, info.rpc_request_stat_count_.obkv_batch_execute_shard_count_);
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_TABLE_API_EXECUTE_QUERY, PROMETHEUS_REQUEST_COUNT, false, false, true, info.rpc_request_stat_count_.obkv_execute_query_shard_count_);
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_TABLE_API_QUERY_AND_MUTATE, PROMETHEUS_REQUEST_COUNT, false, false, true, info.rpc_request_stat_count_.obkv_query_and_mutate_shard_count_);
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_TABLE_API_EXECUTE_QUERY_SYNC, PROMETHEUS_REQUEST_COUNT, false, false, true, info.rpc_request_stat_count_.obkv_execute_query_sync_shard_count_);
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_TABLE_API_LS_EXECUTE, PROMETHEUS_REQUEST_COUNT, false, false, true, info.rpc_request_stat_count_.obkv_ls_execute_shard_count_);
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_INVALID_RPC_CODE, PROMETHEUS_REQUEST_COUNT, false, false, true, info.rpc_request_stat_count_.obkv_other_shard_count_);

    // RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
    //     "", obrpc::OB_INVALID_RPC_CODE, PROMETHEUS_REQUEST_COUNT, false, false, info.request_count_);
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_INVALID_RPC_CODE, PROMETHEUS_REQUEST_TOTAL_TIME, hrtime_to_usec(info.request_total_time_));
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_INVALID_RPC_CODE, PROMETHEUS_SERVER_PROCESS_REQUEST_TIME, hrtime_to_usec(info.server_process_request_time_));
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_INVALID_RPC_CODE, PROMETHEUS_PREPARE_SEND_REQUEST_TIME, hrtime_to_usec(info.prepare_send_request_to_server_time_));
  }
  thread_prometheus_->rpc_monitor_info_used_ = 0;
  memset(thread_prometheus_->rpc_monitor_info_array_, 0, sizeof(thread_prometheus_->rpc_monitor_info_array_));

  MonitorInfoHashMap::iterator rpc_iter = thread_prometheus_->rpc_monitor_info_hash_map_.begin();
  for(; rpc_iter != thread_prometheus_->rpc_monitor_info_hash_map_.end(); ++rpc_iter) {
    SQLMonitorInfo &info = rpc_iter->second;
    //single request stat
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_TABLE_API_LOGIN, PROMETHEUS_REQUEST_COUNT, false, false, false, info.rpc_request_stat_count_.obkv_login_count_);
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_TABLE_API_EXECUTE, PROMETHEUS_REQUEST_COUNT, false, false, false, info.rpc_request_stat_count_.obkv_execute_count_);
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_TABLE_API_BATCH_EXECUTE, PROMETHEUS_REQUEST_COUNT, false, false, false, info.rpc_request_stat_count_.obkv_batch_execute_count_);
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_TABLE_API_EXECUTE_QUERY, PROMETHEUS_REQUEST_COUNT, false, false, false, info.rpc_request_stat_count_.obkv_execute_query_count_);
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_TABLE_API_QUERY_AND_MUTATE, PROMETHEUS_REQUEST_COUNT, false, false, false, info.rpc_request_stat_count_.obkv_query_and_mutate_count_);
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_TABLE_API_EXECUTE_QUERY_SYNC, PROMETHEUS_REQUEST_COUNT, false, false, false, info.rpc_request_stat_count_.obkv_execute_query_sync_count_);
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_TABLE_TTL, PROMETHEUS_REQUEST_COUNT, false, false, false, info.rpc_request_stat_count_.obkv_table_ttl_count_);
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_TTL_REQUEST, PROMETHEUS_REQUEST_COUNT, false, false, false, info.rpc_request_stat_count_.obkv_tll_request_count_);
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_TABLE_API_DIRECT_LOAD, PROMETHEUS_REQUEST_COUNT, false, false, false, info.rpc_request_stat_count_.obkv_direct_load_count_);
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_TABLE_API_LS_EXECUTE, PROMETHEUS_REQUEST_COUNT, false, false, false, info.rpc_request_stat_count_.obkv_ls_execute_count_);
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_INVALID_RPC_CODE, PROMETHEUS_REQUEST_COUNT, false, false, false, info.rpc_request_stat_count_.obkv_other_count_);
    //shard request stat
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_TABLE_API_BATCH_EXECUTE, PROMETHEUS_REQUEST_COUNT, false, false, true, info.rpc_request_stat_count_.obkv_batch_execute_shard_count_);
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_TABLE_API_EXECUTE_QUERY, PROMETHEUS_REQUEST_COUNT, false, false, true, info.rpc_request_stat_count_.obkv_execute_query_shard_count_);
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_TABLE_API_QUERY_AND_MUTATE, PROMETHEUS_REQUEST_COUNT, false, false, true, info.rpc_request_stat_count_.obkv_query_and_mutate_shard_count_);
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_TABLE_API_EXECUTE_QUERY_SYNC, PROMETHEUS_REQUEST_COUNT, false, false, true, info.rpc_request_stat_count_.obkv_execute_query_sync_shard_count_);
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_TABLE_API_LS_EXECUTE, PROMETHEUS_REQUEST_COUNT, false, false, true, info.rpc_request_stat_count_.obkv_ls_execute_shard_count_);
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_INVALID_RPC_CODE, PROMETHEUS_REQUEST_COUNT, false, false, true, info.rpc_request_stat_count_.obkv_other_shard_count_);

    // RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
    //     "", obrpc::OB_INVALID_RPC_CODE, PROMETHEUS_REQUEST_COUNT, false, false, info.request_count_);
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_INVALID_RPC_CODE, PROMETHEUS_REQUEST_TOTAL_TIME, hrtime_to_usec(info.request_total_time_));
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_INVALID_RPC_CODE, PROMETHEUS_SERVER_PROCESS_REQUEST_TIME, hrtime_to_usec(info.server_process_request_time_));
    RPC_PROMETHEUS_STAT("", "", info.cluster_name_, info.tenant_name_,
        "", obrpc::OB_INVALID_RPC_CODE, PROMETHEUS_PREPARE_SEND_REQUEST_TIME, hrtime_to_usec(info.prepare_send_request_to_server_time_));
  }
  thread_prometheus_->rpc_monitor_info_hash_map_.reuse();

  if (OB_FAIL(schedule_report_prometheus_info())) {
    LOG_WDIAG("schedule report prometheus info failed", K(ret));
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
  } else if (OB_FAIL(monitor_info_hash_map_.create(16, ObModIds::OB_PROMETHEUS_RELATED, ObModIds::OB_PROMETHEUS_RELATED))) {
    LOG_WDIAG("monitor info hash map create failed", K(ret));
  } else if (OB_FAIL(rpc_monitor_info_hash_map_.create(16, ObModIds::OB_PROMETHEUS_RELATED))) {
    LOG_WDIAG("monitor info rpc hash map create failed", K(ret));
  } else if (OB_ISNULL(sql_monitor_info_cont_ = op_alloc(ObSQLMonitorInfoCont))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc ObSQLMonitorInfoCont", K(ret));
  } else if (OB_FAIL(sql_monitor_info_cont_->init(
              get_global_proxy_config().prometheus_sync_interval / 5, thread, this))) {
    LOG_WDIAG("set report interval failed", K(ret));
  } else if (OB_FAIL(sql_monitor_info_cont_->schedule_report_prometheus_info())) {
    LOG_WDIAG("schedule report prometheus info failed", K(ret));
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
                                             const SQLMonitorInfo &info,
                                             const ObProxyRequestType type)
{
  int ret = OB_SUCCESS;
  if (type == OBPROXY_SQL_REQUEST) {
    if (set_sql_monitor_info_using_array(tenant_name, cluster_name, info, monitor_info_array_, monitor_info_used_)) {
      LOG_DEBUG("set sql monitor info using array", K(tenant_name), K(cluster_name), K(ret));
    } else if (OB_FAIL(set_sql_monitor_info_using_hashmap(tenant_name, cluster_name, info, monitor_info_hash_map_))) {
      LOG_WDIAG("set sql monitor info using hashmap failed", K(tenant_name), K(cluster_name), K(info), K(ret));
    }
  } else if (type == OBPROXY_RPC_REQUEST) {
    if (set_sql_monitor_info_using_array(tenant_name, cluster_name, info, rpc_monitor_info_array_, rpc_monitor_info_used_)) {
      LOG_DEBUG("set rpc monitor info using array", K(tenant_name), K(cluster_name), K(ret));
    } else if (OB_FAIL(set_sql_monitor_info_using_hashmap(tenant_name, cluster_name, info, rpc_monitor_info_hash_map_))) {
      LOG_WDIAG("set rpc monitor info using hashmap failed", K(tenant_name), K(cluster_name), K(info), K(ret));
    }
  }

  return ret;
}

int ObThreadPrometheus::set_sql_monitor_info_using_hashmap(const ObString &tenant_name,
                                                           const ObString &cluster_name,
                                                           const SQLMonitorInfo &info,
                                                           MonitorInfoHashMap &monitor_info_hash_map)
{
  int ret = OB_SUCCESS;
  ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH> key_string;
  SQLMonitorInfo monitor_info;
  if (OB_UNLIKELY(tenant_name.empty() || cluster_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(tenant_name), K(cluster_name), K(ret));
  } else if (OB_FAIL(paste_tenant_and_cluster_name(tenant_name, cluster_name, key_string))) {
    LOG_WDIAG("paste tenant and cluster name failed", K(tenant_name), K(cluster_name), K(ret));
  } else if (OB_FAIL(monitor_info_hash_map.get_refactored(key_string, monitor_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      memset(&monitor_info, 0, sizeof(monitor_info));
      MEMCPY(monitor_info.cluster_name_str_, cluster_name.ptr(), cluster_name.length());
      MEMCPY(monitor_info.tenant_name_str_, tenant_name.ptr(), tenant_name.length());
      monitor_info.cluster_name_.assign_ptr(monitor_info.cluster_name_str_, cluster_name.length());
      monitor_info.tenant_name_.assign_ptr(monitor_info.tenant_name_str_, tenant_name.length());
      ret = OB_SUCCESS;
    }
  }

  if (OB_SUCC(ret)) {
    if (info.request_type_ == OBPROXY_RPC_REQUEST) {
      monitor_info.rpc_request_stat_count_.add_count(info.rpc_request_stat_count_);
    } else {
      monitor_info.request_count_ += info.request_count_;
      monitor_info.select_count_ += info.select_count_;
      monitor_info.update_count_ += info.update_count_;
      monitor_info.insert_count_ += info.insert_count_;
      monitor_info.delete_count_ += info.delete_count_;
      monitor_info.other_count_ += info.other_count_;
      monitor_info.sql_request_stat_count_.add_count(info.sql_request_stat_count_);
    }

    monitor_info.request_total_time_ += info.request_total_time_;
    monitor_info.server_process_request_time_ += info.server_process_request_time_;
    monitor_info.prepare_send_request_to_server_time_ += info.prepare_send_request_to_server_time_;
    if (OB_FAIL(monitor_info_hash_map.set_refactored(key_string, monitor_info, 1))) {
      LOG_WDIAG("monitor info hash map set failed", K(ret));
    }
  }

  return ret;
}

bool ObThreadPrometheus::set_sql_monitor_info_using_array(const ObString &tenant_name,
                                                          const ObString &cluster_name,
                                                          const SQLMonitorInfo &info,
                                                          SQLMonitorInfo (&monitor_info_array)[SQL_MONITOR_INFO_ARRAY_SIZE],
                                                          int64_t &monitor_info_used)
{
  bool bret = false;
  int64_t index = -1;
  for (int64_t i = 0; !bret && i < monitor_info_used; i++) {
    if (monitor_info_array[i].tenant_name_ == tenant_name && monitor_info_array[i].cluster_name_ == cluster_name) {
      bret = true;
      index = i;
    } 
  }

  if (!bret && monitor_info_used < SQL_MONITOR_INFO_ARRAY_SIZE) {
    memset(&monitor_info_array[monitor_info_used], 0, sizeof(monitor_info_array[monitor_info_used]));
    MEMCPY(monitor_info_array[monitor_info_used].cluster_name_str_, cluster_name.ptr(), cluster_name.length());
    MEMCPY(monitor_info_array[monitor_info_used].tenant_name_str_, tenant_name.ptr(), tenant_name.length());
    monitor_info_array[monitor_info_used].tenant_name_.assign_ptr(monitor_info_array[monitor_info_used].tenant_name_str_, tenant_name.length());
    monitor_info_array[monitor_info_used].cluster_name_.assign_ptr(monitor_info_array[monitor_info_used].cluster_name_str_, cluster_name.length());
    index = monitor_info_used;
    monitor_info_used++;
    bret = true;
  }

  if (bret) {
    if (info.request_type_ == OBPROXY_RPC_REQUEST) {
      monitor_info_array[index].rpc_request_stat_count_.add_count(info.rpc_request_stat_count_);
    } else {
      monitor_info_array[index].request_count_ += info.request_count_;
      monitor_info_array[index].select_count_ += info.select_count_;
      monitor_info_array[index].update_count_ += info.update_count_;
      monitor_info_array[index].insert_count_ += info.insert_count_;
      monitor_info_array[index].delete_count_ += info.delete_count_;
      monitor_info_array[index].other_count_ += info.other_count_;
      monitor_info_array[index].sql_request_stat_count_.add_count(info.sql_request_stat_count_);
    }
    monitor_info_array[index].request_total_time_ += info.request_total_time_;
    monitor_info_array[index].server_process_request_time_ += info.server_process_request_time_;
    monitor_info_array[index].prepare_send_request_to_server_time_ += info.prepare_send_request_to_server_time_;
  }

  return bret;
}

} // end of prometheus
} // end of obproxy
} // end of oceanbase
