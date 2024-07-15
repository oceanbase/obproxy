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

#ifndef OB_MYSQL_CONFIG_PROCESSOR
#define OB_MYSQL_CONFIG_PROCESSOR
#include "lib/ob_define.h"
#include "lib/ptr/ob_ptr.h"
#include "iocore/net/ob_inet.h"
#include "utils/ob_proxy_utils.h"
#include "obutils/ob_proxy_config.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

typedef common::ObSharedRefCount ObConfigInfo;

typedef bool CfgBool;
typedef int64_t CfgInt;
typedef int64_t CfgTime; // ns, but time in ObProxyConfig is us
typedef net::ObIpAddr CfgIp;
typedef int64_t CfgPort;
typedef net::ObIpEndpoint CfgIpPort;
typedef common::ObSEArray<net::ObIpEndpoint, 4> CfgIpPortList;

// Attention!!! if you add one config item here, you must
// add the config item in assign_config method and constructor method.
// if not, believe me you will regret.
struct ObMysqlConfigParams : public ObConfigInfo
{
public:
  static const int64_t MAX_TEST_SERVER_NUM = 4;
  ObMysqlConfigParams();
  virtual ~ObMysqlConfigParams() {}
  virtual void free() { delete this; }

  int assign_config(const obutils::ObProxyConfig &proxy_config);
  bool is_standard_routing_mode() const { return obutils::OB_STANDARD_ROUTING_MODE == server_routing_mode_; }
  bool is_random_routing_mode() const { return obutils::OB_RANDOM_ROUTING_MODE == server_routing_mode_; }
  bool is_mock_routing_mode() const { return obutils::OB_MOCK_ROUTING_MODE == server_routing_mode_; }
  bool is_mysql_routing_mode() const { return obutils::OB_MYSQL_ROUTING_MODE == server_routing_mode_; }
  int get_one_test_server_addr(net::ObIpEndpoint &addr) const;

  DECLARE_TO_STRING;

public:
  CfgTime stat_table_sync_interval_;
  CfgTime server_state_refresh_interval_;
  CfgTime stat_dump_interval_;

  CfgBool enable_flow_control_;
  CfgInt flow_high_water_mark_;
  CfgInt flow_low_water_mark_;
  CfgInt flow_consumer_reenable_threshold_;
  CfgInt flow_event_queue_threshold_;

  CfgInt default_buffer_water_mark_;
  CfgInt tunnel_request_size_threshold_;
  CfgInt request_buffer_length_;

  CfgInt sock_recv_buffer_size_out_;
  CfgInt sock_send_buffer_size_out_;
  CfgInt server_tcp_keepidle_;
  CfgInt server_tcp_keepintvl_;
  CfgInt server_tcp_keepcnt_;
  CfgInt server_tcp_user_timeout_;
  CfgInt sock_option_flag_out_;
  CfgInt sock_packet_mark_out_;
  CfgInt sock_packet_tos_out_;
  CfgInt server_tcp_init_cwnd_;

  CfgInt client_tcp_keepidle_;
  CfgInt client_tcp_keepintvl_;
  CfgInt client_tcp_keepcnt_;
  CfgInt client_tcp_user_timeout_;
  CfgInt client_sock_option_flag_out_;

  CfgBool frequent_accept_;
  CfgInt net_accept_threads_;
  CfgTime default_inactivity_timeout_;
  CfgTime observer_query_timeout_delta_;
  CfgTime short_async_task_timeout_;
  CfgTime min_congested_connect_timeout_;
  CfgTime tenant_location_valid_time_;

  CfgIp local_bound_ip_;
  CfgPort listen_port_;
  CfgPort rpc_listen_port_;
  CfgInt stack_size_;
  CfgInt work_thread_num_;
  CfgInt task_thread_num_;
  CfgInt block_thread_num_;
  CfgInt grpc_thread_num_;
  CfgInt shard_scan_thread_num_;
  CfgBool automatic_match_work_thread_;
  CfgBool enable_congestion_;
  CfgBool enable_bad_route_reject_;
  CfgBool enable_cluster_checkout_;
  CfgBool enable_client_ip_checkout_;
  CfgBool enable_proxy_scramble_;
  CfgBool enable_compression_protocol_;
  CfgBool enable_ob_protocol_v2_;
  CfgBool enable_reroute_;
  CfgBool enable_weak_reroute_;
  CfgBool enable_index_route_;
  CfgBool enable_causal_order_read_;
  CfgBool enable_transaction_internal_routing_;
  CfgIpPortList test_server_addr_;

  CfgInt sqlaudit_mem_limited_;
  CfgInt internal_cmd_mem_limited_;
  CfgInt max_connections_;

  CfgInt client_max_connections_;
  CfgBool enable_client_connection_lru_disconnect_;
  CfgInt connect_observer_max_retries_;

  CfgTime monitor_stat_low_threshold_;
  CfgTime monitor_stat_middle_threshold_;
  CfgTime monitor_stat_high_threshold_;

  CfgBool enable_trans_detail_stats_;
  CfgBool enable_mysqlsm_info_;
  CfgBool enable_report_session_stats_;
  CfgBool enable_strict_stat_time_;
  CfgBool enable_cpu_topology_;
  CfgBool enable_trace_stats_;
  CfgBool enable_partition_table_route_;
  CfgBool enable_pl_route_;
  CfgBool enable_obproxy_rpc_service_;
  CfgTime slow_transaction_time_threshold_;
  CfgTime slow_proxy_process_time_threshold_;
  CfgTime query_digest_time_threshold_;
  CfgTime slow_query_time_threshold_;
  obutils::ObProxyServiceMode proxy_service_mode_;
  obutils::ObServerRoutingMode server_routing_mode_;
  CfgInt proxy_id_;
  CfgInt client_max_memory_size_;
  CfgBool enable_cpu_isolate_;
  CfgBool enable_primary_zone_;
  CfgInt ip_listen_mode_;
  CfgIp local_bound_ipv6_ip_;
  CfgTime read_stale_retry_interval_;
  CfgTime ob_max_read_stale_time_;
  CfgInt rpc_request_max_retries_;
  CfgTime rpc_srv_session_pool_inactive_timeout_;
  CfgInt client_session_id_version_ ;
  char proxy_idc_name_[OB_PROXY_MAX_IDC_NAME_LENGTH + 1];
  char proxy_primary_zone_name_[common::MAX_ZONE_LENGTH + 1];
};

// param for performance
struct ObPerformanceParams
{
public:
  ObPerformanceParams();
  virtual ~ObPerformanceParams() {}
  virtual void free() { delete this; }

  int assign_config(const obutils::ObProxyConfig &proxy_config);
  DECLARE_TO_STRING;
public:
  CfgBool enable_performance_mode_;
  CfgBool enable_trace_;
  CfgBool enable_stat_;
  bool is_inited_;
};

class ObMysqlConfigProcessor
{
public:
  ObMysqlConfigProcessor() : params_(NULL) {}
  int reconfigure(const obutils::ObProxyConfig &proxy_config);

  // acquire and release must be used in pairs
  ObMysqlConfigParams *acquire();
  int release(ObMysqlConfigParams *params);
  ObMysqlConfigParams *get_config() const { return params_; }

private:
  // add new config_params
  int set(ObMysqlConfigParams *params);

private:
  mutable obsys::CRWLock params_lock_;
  ObMysqlConfigParams *params_;
  DISALLOW_COPY_AND_ASSIGN(ObMysqlConfigProcessor);
};

extern ObMysqlConfigProcessor mysql_config_processor;
inline ObMysqlConfigProcessor &get_global_mysql_config_processor() { return mysql_config_processor; }
extern ObPerformanceParams performance_params;
inline ObPerformanceParams get_global_performance_params() { return performance_params; }

inline int ObMysqlConfigParams::get_one_test_server_addr(net::ObIpEndpoint &addr) const
{
  static uint32_t idx = 0;
  int64_t count = test_server_addr_.count();
  return test_server_addr_.at(0 == count ? 0 : idx++ % count, addr);
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OB_MYSQL_CONFIG_PROCESSOR */
