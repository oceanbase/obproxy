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
#include "obproxy/prometheus/ob_net_prometheus.h"

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

int64_t SQLMonitorInfo::MonitorInfoKey::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(hash), K_(flag_info_.flag_value),
       K_(request_type), K_(stmt_type), K_(rpc_pkt_code),
       K_(route_type), K_(route_policy),
       K_(cluster_name), K_(tenant_name), K_(database_name));
  J_OBJ_END();
  return pos;
}

int64_t SQLMonitorInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(monitor_info_key), K_(request_count), K_(request_total_time),
       K_(server_process_request_time), K_(prepare_send_request_to_server_time), K_(client_request_bytes),
       K_(server_request_bytes), K_(server_response_bytes), K_(client_response_bytes));
  J_OBJ_END();
  return pos;
}

void SQLMonitorInfo::set_key(const MonitorInfoKey& key)
{
  monitor_info_key_.hash_ = key.hash_;
  monitor_info_key_.flag_info_.flag_value_ = key.flag_info_.flag_value_;
  monitor_info_key_.request_type_ = key.request_type_;
  monitor_info_key_.stmt_type_ = key.stmt_type_;
  monitor_info_key_.rpc_pkt_code_ = key.rpc_pkt_code_;
  monitor_info_key_.route_type_ = key.route_type_;
  monitor_info_key_.route_policy_ = key.route_policy_;
  cluster_name_str_.rewrite(key.cluster_name_);
  tenant_name_str_.rewrite(key.tenant_name_);
  database_name_str_.rewrite(key.database_name_);
  monitor_info_key_.cluster_name_ = cluster_name_str_;
  monitor_info_key_.tenant_name_ = tenant_name_str_;
  monitor_info_key_.database_name_ = database_name_str_;
  monitor_info_key_.rpc_entity_type_ = key.rpc_entity_type_;
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

  ObThreadPrometheus::MonitorInfoBuiltinHashMap::iterator iter =
                                                 thread_prometheus_->monitor_info_hashmap_.begin();
  for (;iter != thread_prometheus_->monitor_info_hashmap_.end(); ++iter) {
    SQLMonitorInfo& info = *iter;
    const ObString& logic_tenant_name ="";
    const ObString& logic_database_name = "";
    const ObString& cluster_name = info.key().cluster_name_;
    const ObString& tenant_name = info.key().tenant_name_;
    const ObString& database_name = info.key().database_name_;
    ObProxyBasicStmtType stmt_type = info.key().stmt_type_;
    proxy::ObRouteInfoType route_type = info.key().route_type_;
    proxy::ObRoutePolicyEnum route_policy = info.key().route_policy_;
    obkv::ObTableEntityType table_type = info.key().rpc_entity_type_;
    bool is_slow_query = info.key().is_slow_query();
    bool is_error_resp = info.key().is_error_resp();
    bool is_partition_hit = info.key().is_partition_hit();
    bool is_shard = info.key().is_shard();
    bool is_rerouted = info.key().is_rerouted();
    bool is_partition_calc_fail = info.key().is_partition_calc_fail();
    bool is_trans_internal_routing = info.key().is_trans_internal_routing();
    int64_t request_total_time = hrtime_to_usec(info.request_total_time_);
    int64_t server_process_request_time = hrtime_to_usec(info.server_process_request_time_);
    int64_t prepare_send_request_to_server_time = hrtime_to_usec(info.prepare_send_request_to_server_time_);
    if (OBPROXY_SQL_REQUEST == info.key().request_type_) {
      SQL_PROMETHEUS_STAT(logic_tenant_name, logic_database_name, cluster_name,
                          tenant_name, database_name, stmt_type, PROMETHEUS_REQUEST_COUNT,
                          is_slow_query, is_error_resp, is_partition_hit,
                          is_rerouted, is_partition_calc_fail, is_trans_internal_routing,
                          route_type, route_policy, info.request_count_);

      SQL_PROMETHEUS_STAT(logic_tenant_name, logic_database_name, cluster_name,
                          tenant_name, database_name, stmt_type, PROMETHEUS_REQUEST_TOTAL_TIME,
                          is_slow_query, is_error_resp, is_partition_hit,
                          is_rerouted, is_partition_calc_fail, is_trans_internal_routing,
                          route_type, route_policy, request_total_time);

      SQL_PROMETHEUS_STAT(logic_tenant_name, logic_database_name, cluster_name,
                          tenant_name, database_name, stmt_type, PROMETHEUS_SERVER_PROCESS_REQUEST_TIME,
                          is_slow_query, is_error_resp, is_partition_hit,
                          is_rerouted, is_partition_calc_fail, is_trans_internal_routing,
                          route_type, route_policy, server_process_request_time);

      SQL_PROMETHEUS_STAT(logic_tenant_name, logic_database_name, cluster_name,
                          tenant_name, database_name, stmt_type, PROMETHEUS_PREPARE_SEND_REQUEST_TIME,
                          is_slow_query, is_error_resp, is_partition_hit,
                          is_rerouted, is_partition_calc_fail, is_trans_internal_routing,
                          route_type, route_policy, prepare_send_request_to_server_time);

      NET_PROMETHEUS_STAT(logic_tenant_name, logic_database_name, cluster_name, tenant_name, database_name,
                          PROMETHEUS_REQUEST_BYTE, true, true, info.client_request_bytes_);

      NET_PROMETHEUS_STAT(logic_tenant_name, logic_database_name, cluster_name, tenant_name, database_name,
                          PROMETHEUS_REQUEST_BYTE, true, false, info.server_request_bytes_);

      NET_PROMETHEUS_STAT(logic_tenant_name, logic_database_name, cluster_name, tenant_name, database_name,
                          PROMETHEUS_REQUEST_BYTE, false, true, info.client_response_bytes_);

      NET_PROMETHEUS_STAT(logic_tenant_name, logic_database_name, cluster_name, tenant_name, database_name,
                          PROMETHEUS_REQUEST_BYTE, false, false, info.server_response_bytes_);
    } else if (OBPROXY_RPC_REQUEST == info.key().request_type_) {
      obrpc::ObRpcPacketCode rpc_pkt_code = info.key().rpc_pkt_code_;
      RPC_PROMETHEUS_STAT("", "", cluster_name, tenant_name, "", rpc_pkt_code,
                          PROMETHEUS_REQUEST_COUNT, is_slow_query, is_error_resp, is_shard, table_type, info.request_count_);

      RPC_PROMETHEUS_STAT("", "", cluster_name, tenant_name, "", rpc_pkt_code,
                          PROMETHEUS_REQUEST_TOTAL_TIME, is_slow_query, is_error_resp, is_shard, table_type, request_total_time);

      RPC_PROMETHEUS_STAT("", "", cluster_name, tenant_name, "", rpc_pkt_code,
                          PROMETHEUS_SERVER_PROCESS_REQUEST_TIME, is_slow_query, is_error_resp, is_shard, table_type, server_process_request_time);

      RPC_PROMETHEUS_STAT("", "", cluster_name, tenant_name, "", rpc_pkt_code,
                          PROMETHEUS_PREPARE_SEND_REQUEST_TIME, is_slow_query, is_error_resp, is_shard, table_type, prepare_send_request_to_server_time);

      NET_PROMETHEUS_STAT("", "", cluster_name, tenant_name, database_name,
                          PROMETHEUS_RPC_REQUEST_BYTE, true, true, info.client_request_bytes_);

      NET_PROMETHEUS_STAT("", "", cluster_name, tenant_name, database_name,
                          PROMETHEUS_RPC_REQUEST_BYTE, true, false, info.server_request_bytes_);

      NET_PROMETHEUS_STAT("", "", cluster_name, tenant_name, database_name,
                          PROMETHEUS_RPC_REQUEST_BYTE, false, true, info.client_response_bytes_);

      NET_PROMETHEUS_STAT("", "", cluster_name, tenant_name, database_name,
                          PROMETHEUS_RPC_REQUEST_BYTE, false, false, info.server_response_bytes_);

    }
    info.reuse();
  }


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

int ObThreadPrometheus::set_sql_monitor_info(SQLMonitorInfo::MonitorInfoKey& tmp_info_key,
                                             const int64_t request_count,
                                             const int64_t request_total_time,
                                             const int64_t server_process_request_time,
                                             const int64_t prepare_send_request_to_server_time,
                                             const int64_t client_request_bytes,
                                             const int64_t server_request_bytes,
                                             const int64_t server_response_bytes,
                                             const int64_t client_response_bytes)
{
  int ret = OB_SUCCESS;

  SQLMonitorInfo* info_item = NULL;
  tmp_info_key.set_hash();
  if (OB_FAIL(monitor_info_hashmap_.get_refactored(tmp_info_key, info_item))) {
    if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
      LOG_WDIAG("fail to get info item", K(ret));
    } else if (OB_UNLIKELY(monitor_info_hashmap_.count() > get_global_proxy_config().monitor_item_limit)) {
      ret = OB_ERR_LIMIT;
      LOG_WDIAG("reach limit, will not set new prometheus info", K(ret));
    } else if (monitor_info_used_ < SQL_MONITOR_INFO_ARRAY_SIZE) {
      info_item = &monitor_info_array_[monitor_info_used_];
      monitor_info_used_++;
    } else if (OB_ISNULL(info_item = op_alloc(SQLMonitorInfo))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc SQLMonitorInfo", K(ret));
    }
    if (OB_ISNULL(info_item)) {
      // nothing
    } else if (FALSE_IT(info_item->set_key(tmp_info_key))) {
      // impossible
    } else if (OB_FAIL(monitor_info_hashmap_.unique_set(info_item))) {
      LOG_WDIAG("fail to inser into map", K(ret));
    } else {
      LOG_DEBUG("alloc new monitor info and insert into map");
    }
  } else {
    LOG_DEBUG("get monitor info from map");
  }

  if (OB_SUCC(ret)) {
    info_item->request_count_ += request_count;
    info_item->request_total_time_ += request_total_time;
    info_item->server_process_request_time_ += server_process_request_time;
    info_item->prepare_send_request_to_server_time_ += prepare_send_request_to_server_time;
    info_item->client_request_bytes_ += client_request_bytes;
    info_item->server_request_bytes_ += server_request_bytes;
    info_item->server_response_bytes_ += server_response_bytes;
    info_item->client_response_bytes_ += client_response_bytes;
  } else if (OB_NOT_NULL(info_item)) {
    op_free(info_item);
    info_item = NULL;
  }

  return ret;
}

} // end of prometheus
} // end of obproxy
} // end of oceanbase
