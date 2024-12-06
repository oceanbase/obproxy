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
#include "obproxy/opsql/parser/ob_proxy_parse_result.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "share/config/ob_config_helper.h"
#include "proxy/route/ob_route_diagnosis.h"
#include "proxy/route/ob_ldc_struct.h"
#include "lib/hash_func/murmur_hash.h"

#define SQL_MONITOR_INFO_ARRAY_SIZE 10

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

class SQLMonitorInfo {
public:
  SQLMonitorInfo() : monitor_info_key_(), cluster_name_str_(), tenant_name_str_(), database_name_str_(),
                     request_count_(0), request_total_time_(0),
                     server_process_request_time_(0), prepare_send_request_to_server_time_(0),
                     client_request_bytes_(0), server_request_bytes_(0), server_response_bytes_(0),
                     client_response_bytes_(0) {
  }

  ~SQLMonitorInfo() {}

  inline void reuse() {
    request_count_ = 0;
    request_total_time_ = 0;
    server_process_request_time_ = 0;
    prepare_send_request_to_server_time_ = 0;
    client_request_bytes_ = 0;
    server_request_bytes_ = 0;
    server_response_bytes_ = 0;
    client_response_bytes_ = 0;
  }

  static ObProxyBasicStmtType inline get_prometheus_output_type(ObProxyBasicStmtType stmt_type) {

    switch(stmt_type) {
      case OBPROXY_T_SELECT:
      case OBPROXY_T_UPDATE:
      case OBPROXY_T_INSERT:
      case OBPROXY_T_REPLACE:
      case OBPROXY_T_DELETE:
        return stmt_type;
      default:
        return OBPROXY_T_INVALID;
    }
  };

  SQLMonitorInfo(const SQLMonitorInfo& other) {
    if (OB_LIKELY(this != &other)) {
      *this = other;
    }
  }

  SQLMonitorInfo &operator=(const SQLMonitorInfo& other) {
    if (OB_LIKELY(this != &other)) {
      monitor_info_key_ = other.monitor_info_key_;
      request_count_ = other.request_count_;
      request_total_time_ = other.request_total_time_;
      server_process_request_time_ = other.server_process_request_time_;
      prepare_send_request_to_server_time_ = other.prepare_send_request_to_server_time_;
      client_request_bytes_ = other.client_request_bytes_;
      server_request_bytes_ = other.server_request_bytes_;
      server_response_bytes_ = other.server_response_bytes_;
      client_response_bytes_ = other.client_response_bytes_;
    }
    return *this;
  }

  struct MonitorInfoKey;
  inline const MonitorInfoKey& key() const {
    return monitor_info_key_;
  }

  void set_key(const MonitorInfoKey& key);

  int64_t to_string(char *buf, const int64_t buf_len) const;

  struct MonitorInfoKey {
    MonitorInfoKey() : request_type_(), stmt_type_(), rpc_pkt_code_(),
                       route_type_(proxy::ObRouteInfoType::INVALID),
                       route_policy_(proxy::ObRoutePolicyEnum::MERGE_IDC_ORDER),
                       cluster_name_(), tenant_name_(), database_name_() {
      flag_info_.flag_value_ = 0;
    }
    ~MonitorInfoKey() {}
    inline uint64_t hash() const {
      return hash_;
    }
    void set_hash() {
      uint64_t len = reinterpret_cast<uint64_t>(&route_policy_) - reinterpret_cast<uint64_t>(this)
                     + sizeof(route_policy_);
      uint64_t seed = murmurhash(this, static_cast<int32_t>(len), 0);
      seed = cluster_name_.hash(seed);
      seed = tenant_name_.hash(seed);
      seed = database_name_.hash(seed);
      hash_ = seed;
    }


    void set_is_slow_query(bool is_slow_query) { flag_info_.flag_.IS_SLOW_QUERY = is_slow_query; }
    void set_is_error_resp(bool is_err_resp) { flag_info_.flag_.IS_ERROR_RESP = is_err_resp; }
    void set_is_partition_hit(bool is_partition_hit) { flag_info_.flag_.IS_PARTITION_HIT = is_partition_hit; }
    void set_is_shard(bool is_shard) { flag_info_.flag_.IS_SHARD = is_shard; }
    void set_is_rerouted(bool is_rerouted) { flag_info_.flag_.IS_REROUTED = is_rerouted; }
    void set_is_partition_calc_fail(bool is_partition_calc_fail) { flag_info_.flag_.IS_PARTITION_CALC_FAIL = is_partition_calc_fail; }
    void set_is_trans_internal_routing(bool is_trans_internal_routing) { flag_info_.flag_.IS_TRANS_INTERNAL_ROUTING = is_trans_internal_routing; }

    bool is_slow_query() const { return flag_info_.flag_.IS_SLOW_QUERY; }
    bool is_error_resp() const { return flag_info_.flag_.IS_ERROR_RESP; }
    bool is_partition_hit() const { return flag_info_.flag_.IS_PARTITION_HIT; }
    bool is_shard() const { return flag_info_.flag_.IS_SHARD; }
    bool is_rerouted() const { return flag_info_.flag_.IS_REROUTED; }
    bool is_partition_calc_fail() const { return flag_info_.flag_.IS_PARTITION_CALC_FAIL; }
    bool is_trans_internal_routing() const { return flag_info_.flag_.IS_TRANS_INTERNAL_ROUTING; }
    int64_t to_string(char *buf, const int64_t buf_len) const;

  public:
    union {
      uint32_t flag_value_;
      struct {
        uint32_t IS_SLOW_QUERY:                         1;
        uint32_t IS_ERROR_RESP:                         1;
        uint32_t IS_PARTITION_HIT:                      1;
        uint32_t IS_SHARD:                              1; // used by obkv
        uint32_t IS_REROUTED:                           1;
        uint32_t IS_PARTITION_CALC_FAIL:                1;
        uint32_t IS_TRANS_INTERNAL_ROUTING:             1;
        uint32_t :                                      0;
      } flag_;
    } flag_info_;

  public:
    ObProxyRequestType request_type_;
    ObProxyBasicStmtType stmt_type_;
    obrpc::ObRpcPacketCode rpc_pkt_code_;
    proxy::ObRouteInfoType route_type_;
    proxy::ObRoutePolicyEnum route_policy_;

    // value before cluster_name_ in MonitorInfoKey will be used to calc seed
    common::ObString cluster_name_;
    common::ObString tenant_name_;
    common::ObString database_name_;
    uint64_t hash_;
  };

private:
  MonitorInfoKey monitor_info_key_;
  common::ObConfigVariableString cluster_name_str_;
  common::ObConfigVariableString  tenant_name_str_;
  common::ObConfigVariableString database_name_str_;
public:
  int64_t request_count_;
  int64_t request_total_time_;
  int64_t server_process_request_time_;
  int64_t prepare_send_request_to_server_time_;
  int64_t client_request_bytes_;
  int64_t server_request_bytes_;
  int64_t server_response_bytes_;
  int64_t client_response_bytes_;
public:
  LINK(SQLMonitorInfo, sql_monotor_info_v2_link_);
};

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
  ObThreadPrometheus() : monitor_info_used_(0), monitor_info_array_(), monitor_info_hashmap_(),
                         sql_monitor_info_cont_(NULL), thread_(NULL) {}
  ~ObThreadPrometheus() {}

  int set_sql_monitor_info(SQLMonitorInfo::MonitorInfoKey& tmp_info_key,
                           const int64_t request_count,
                           const int64_t request_total_time,
                           const int64_t server_process_request_time,
                           const int64_t prepare_send_request_to_server_time,
                           const int64_t client_request_bytes,
                           const int64_t server_request_bytes,
                           const int64_t server_response_bytes,
                           const int64_t client_response_bytes);

private:
  struct SQLMonitorInfoV2Hashing
  {
    typedef const SQLMonitorInfo::MonitorInfoKey& Key;
    typedef SQLMonitorInfo Value;
    typedef ObDLList(SQLMonitorInfo, sql_monotor_info_v2_link_) ListHead;

    static uint64_t hash(Key key) { return key.hash(); }
    static Key key(Value *value) { return value->key(); }
    static bool equal(Key lhs, Key rhs) {
      return lhs.hash_ == rhs.hash_
             && lhs.stmt_type_ == rhs.stmt_type_
             && lhs.flag_info_.flag_value_ == rhs.flag_info_.flag_value_
             && lhs.database_name_ == rhs.database_name_
             && lhs.route_type_ == rhs.route_type_
             && lhs.route_policy_ == rhs.route_policy_
             && lhs.tenant_name_ == rhs.tenant_name_
             && lhs.cluster_name_ == rhs.cluster_name_
             && lhs.request_type_ == rhs.request_type_
             && lhs.rpc_pkt_code_ == rhs.rpc_pkt_code_;
    }
  };
  static const int64_t MONITOR_INFO_HASH_BUCKET_SIZE = 1024;

public:
  typedef common::hash::ObBuildInHashMap<SQLMonitorInfoV2Hashing, MONITOR_INFO_HASH_BUCKET_SIZE> MonitorInfoBuiltinHashMap;
  int64_t monitor_info_used_;
  SQLMonitorInfo monitor_info_array_[SQL_MONITOR_INFO_ARRAY_SIZE];

  MonitorInfoBuiltinHashMap monitor_info_hashmap_;
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