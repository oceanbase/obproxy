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
#include "proxy/mysqllib/ob_mysql_config_processor.h"
#include "utils/ob_proxy_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

#define CONFIG_ITEM_ASSIGN(x) (this->x##_ = (proxy_config.x))
// Time in ObMysqlConfigParam is ns, but Time in ObProxyConfig is us,
// so we should covert us to ns;
#define CONFIG_TIME_ASSIGN(x) (this->x##_ = (proxy_config.x * 1000))
#define CONFIG_IP_ASSIGN(ret, x) (ret = (this->x##_.load(proxy_config.x)))

ObMysqlConfigProcessor mysql_config_processor;

ObMysqlConfigParams::ObMysqlConfigParams()
  : stat_table_sync_interval_(0),
    server_state_refresh_interval_(0),
    stat_dump_interval_(0),

    enable_flow_control_(false),
    flow_high_water_mark_(0),
    flow_low_water_mark_(0),
    flow_consumer_reenable_threshold_(0),
    flow_event_queue_threshold_(0),

    default_buffer_water_mark_(0),
    tunnel_request_size_threshold_(0),
    request_buffer_length_(4096),

    sock_recv_buffer_size_out_(0),
    sock_send_buffer_size_out_(0),
    server_tcp_keepidle_(0),
    server_tcp_keepintvl_(0),
    server_tcp_keepcnt_(0),
    server_tcp_user_timeout_(0),
    sock_option_flag_out_(0),
    sock_packet_mark_out_(0),
    sock_packet_tos_out_(0),
    server_tcp_init_cwnd_(0),

    frequent_accept_(false),
    net_accept_threads_(0),
    default_inactivity_timeout_(0),
    observer_query_timeout_delta_(0),
    short_async_task_timeout_(0),
    min_congested_connect_timeout_(0),
    tenant_location_valid_time_(0),

    local_bound_ip_(),
    listen_port_(0),
    stack_size_(0),
    work_thread_num_(0),
    task_thread_num_(0),
    block_thread_num_(0),
    grpc_thread_num_(0),
    automatic_match_work_thread_(true),

    enable_congestion_(false),
    enable_bad_route_reject_(false),
    enable_cluster_checkout_(true),
    enable_client_ip_checkout_(true),
    enable_force_request_follower_(false),
    enable_proxy_scramble_(false),
    enable_compression_protocol_(false),
    enable_ob_protocol_v2_(false),
    enable_reroute_(false),
    enable_index_route_(false),
    enable_causal_order_read_(true),

    sqlaudit_mem_limited_(0),
    internal_cmd_mem_limited_(0),
    max_connections_(0),

    client_max_connections_(0),
    enable_client_connection_lru_disconnect_(false),
    connect_observer_max_retries_(0),

    monitor_stat_low_threshold_(0),
    monitor_stat_middle_threshold_(0),
    monitor_stat_high_threshold_(0),

    enable_trans_detail_stats_(false),
    enable_mysqlsm_info_(false),
    enable_report_session_stats_(false),
    enable_strict_stat_time_(true),
    enable_cpu_topology_(true),
    enable_trace_stats_(false),
    enable_partition_table_route_(false),
    enable_pl_route_(false),
    slow_transaction_time_threshold_(0),
    slow_proxy_process_time_threshold_(0),
    query_digest_time_threshold_(0),
    slow_query_time_threshold_(0),
    proxy_service_mode_(OB_MAX_SERVICE_MODE),
    server_routing_mode_(OB_MAX_ROUTING_MODE),
    proxy_id_(0),
    client_max_memory_size_(0)
{
  proxy_idc_name_[0] = '\0';
}

int ObMysqlConfigParams::assign_config(const ObProxyConfig &proxy_config)
{
  int ret = OB_SUCCESS;
  CONFIG_TIME_ASSIGN(stat_table_sync_interval);
  CONFIG_TIME_ASSIGN(server_state_refresh_interval);
  CONFIG_TIME_ASSIGN(stat_dump_interval);

  CONFIG_ITEM_ASSIGN(enable_flow_control);
  CONFIG_ITEM_ASSIGN(flow_high_water_mark);
  CONFIG_ITEM_ASSIGN(flow_low_water_mark);
  CONFIG_ITEM_ASSIGN(flow_consumer_reenable_threshold);
  CONFIG_ITEM_ASSIGN(flow_event_queue_threshold);

  CONFIG_ITEM_ASSIGN(default_buffer_water_mark);
  CONFIG_ITEM_ASSIGN(tunnel_request_size_threshold);
  CONFIG_ITEM_ASSIGN(request_buffer_length);

  CONFIG_ITEM_ASSIGN(sock_recv_buffer_size_out);
  CONFIG_ITEM_ASSIGN(sock_send_buffer_size_out);
  CONFIG_ITEM_ASSIGN(server_tcp_keepidle);
  CONFIG_ITEM_ASSIGN(server_tcp_keepintvl);
  CONFIG_ITEM_ASSIGN(server_tcp_keepcnt);
  CONFIG_ITEM_ASSIGN(server_tcp_user_timeout);
  CONFIG_ITEM_ASSIGN(sock_option_flag_out);
  CONFIG_ITEM_ASSIGN(sock_packet_mark_out);
  CONFIG_ITEM_ASSIGN(sock_packet_tos_out);
  CONFIG_ITEM_ASSIGN(server_tcp_init_cwnd);

  CONFIG_ITEM_ASSIGN(client_sock_option_flag_out);
  CONFIG_ITEM_ASSIGN(client_tcp_keepidle);
  CONFIG_ITEM_ASSIGN(client_tcp_keepintvl);
  CONFIG_ITEM_ASSIGN(client_tcp_keepcnt);
  CONFIG_ITEM_ASSIGN(client_tcp_user_timeout);

  CONFIG_ITEM_ASSIGN(frequent_accept);
  CONFIG_ITEM_ASSIGN(net_accept_threads);
  CONFIG_TIME_ASSIGN(default_inactivity_timeout);
  CONFIG_TIME_ASSIGN(observer_query_timeout_delta);
  CONFIG_TIME_ASSIGN(short_async_task_timeout);
  CONFIG_TIME_ASSIGN(min_congested_connect_timeout);
  CONFIG_TIME_ASSIGN(tenant_location_valid_time);

  CONFIG_ITEM_ASSIGN(listen_port);
  CONFIG_ITEM_ASSIGN(stack_size);
  CONFIG_ITEM_ASSIGN(work_thread_num);
  CONFIG_ITEM_ASSIGN(task_thread_num);
  CONFIG_ITEM_ASSIGN(block_thread_num);
  CONFIG_ITEM_ASSIGN(grpc_thread_num);
  CONFIG_ITEM_ASSIGN(automatic_match_work_thread);

  CONFIG_ITEM_ASSIGN(enable_congestion);
  CONFIG_ITEM_ASSIGN(enable_bad_route_reject);
  CONFIG_ITEM_ASSIGN(enable_cluster_checkout);
  CONFIG_ITEM_ASSIGN(enable_client_ip_checkout);
  CONFIG_ITEM_ASSIGN(enable_force_request_follower);
  CONFIG_ITEM_ASSIGN(enable_proxy_scramble);
  CONFIG_ITEM_ASSIGN(enable_compression_protocol);
  CONFIG_ITEM_ASSIGN(enable_ob_protocol_v2);
  CONFIG_ITEM_ASSIGN(enable_reroute);
  CONFIG_ITEM_ASSIGN(enable_index_route);
  CONFIG_ITEM_ASSIGN(enable_causal_order_read);

  CONFIG_ITEM_ASSIGN(sqlaudit_mem_limited);
  CONFIG_ITEM_ASSIGN(internal_cmd_mem_limited);
  CONFIG_ITEM_ASSIGN(max_connections);

  CONFIG_ITEM_ASSIGN(client_max_connections);
  CONFIG_ITEM_ASSIGN(enable_client_connection_lru_disconnect);
  CONFIG_ITEM_ASSIGN(connect_observer_max_retries);

  CONFIG_TIME_ASSIGN(monitor_stat_low_threshold);
  CONFIG_TIME_ASSIGN(monitor_stat_middle_threshold);
  CONFIG_TIME_ASSIGN(monitor_stat_high_threshold);

  CONFIG_ITEM_ASSIGN(enable_trans_detail_stats);
  CONFIG_ITEM_ASSIGN(enable_mysqlsm_info);
  CONFIG_ITEM_ASSIGN(enable_report_session_stats);
  CONFIG_ITEM_ASSIGN(enable_strict_stat_time);
  CONFIG_ITEM_ASSIGN(enable_cpu_topology);
  CONFIG_ITEM_ASSIGN(enable_trace_stats);
  CONFIG_ITEM_ASSIGN(enable_partition_table_route);
  CONFIG_ITEM_ASSIGN(enable_pl_route);
  CONFIG_TIME_ASSIGN(slow_transaction_time_threshold);
  CONFIG_TIME_ASSIGN(slow_proxy_process_time_threshold);
  CONFIG_TIME_ASSIGN(query_digest_time_threshold);
  CONFIG_TIME_ASSIGN(slow_query_time_threshold);
  CONFIG_ITEM_ASSIGN(proxy_id);
  CONFIG_ITEM_ASSIGN(client_max_memory_size);

  if (OB_SUCC(ret)) {
    obsys::CRLockGuard guard(proxy_config.rwlock_);
    proxy_service_mode_ = proxy_config.get_service_mode(proxy_config.proxy_service_mode);
    server_routing_mode_ = proxy_config.get_routing_mode(proxy_config.server_routing_mode);
    if (!proxy_config.is_routing_mode_available(server_routing_mode_) || OB_MAX_ROUTING_MODE == server_routing_mode_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("routing mode is not available", K(server_routing_mode_),
               "server_routing_mode", proxy_config.server_routing_mode.str(), K(ret));
    }
  }


  if (OB_SUCC(ret)) {
    obsys::CRLockGuard guard(proxy_config.rwlock_);
    const int64_t len = static_cast<int64_t>(strlen(proxy_config.proxy_idc_name.str()));
    if (len < 0 || len > OB_PROXY_MAX_IDC_NAME_LENGTH) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("proxy_idc_name's length is over size", K(len),
               "proxy_idc_name", proxy_config.proxy_idc_name.str(), K(ret));
    } else {
      memcpy(proxy_idc_name_, proxy_config.proxy_idc_name.str(), len);
      proxy_idc_name_[len] = '\0';
    }
  }

  if (OB_SUCC(ret)) {
    obsys::CRLockGuard guard(proxy_config.rwlock_);
    const int64_t len = static_cast<int64_t>(strlen(proxy_config.proxy_primary_zone_name.str()));
    if (len < 0 || len > MAX_ZONE_LENGTH) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("proxy_primary_zone_name's length is over size", K(len),
               "proxy_primary_zone_name", proxy_config.proxy_primary_zone_name.str(), K(ret));
    } else {
      memcpy(proxy_primary_zone_name_, proxy_config.proxy_primary_zone_name.str(), len);
      proxy_primary_zone_name_[len] = '\0';
    }
  }


  if (OB_SUCC(ret)) {
    obsys::CRLockGuard guard(proxy_config.rwlock_);
    if(OB_UNLIKELY(0 != local_bound_ip_.load(proxy_config.local_bound_ip))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to assign ip value", K(proxy_config.local_bound_ip.str()), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    obsys::CRLockGuard guard(proxy_config.rwlock_);
    if (STRLEN(proxy_config.test_server_addr) > 0) {
      ObString test_server_addr(STRLEN(proxy_config.test_server_addr),
                                proxy_config.test_server_addr);
      bool is_finished = false;
      ObString addr_str;
      net::ObIpEndpoint tmp_addr;
      while (!test_server_addr.empty() && !is_finished) {
        // split by ';'
        addr_str = test_server_addr.split_on(';');
        if (addr_str.empty()) {
          addr_str.assign(test_server_addr.ptr(), test_server_addr.length()); // the last addr
          is_finished = true;
        }
        // parse addr
        if (!addr_str.empty()) {
          if (OB_FAIL(ops_ip_pton(addr_str, tmp_addr))) {
            ret = OB_BAD_ADDRESS;
            LOG_WARN("fail to assign server addr", K(addr_str), K(ret));
          } else if (OB_FAIL(test_server_addr_.push_back(tmp_addr))) {
            LOG_WARN("fail push back addr", K(addr_str), K(ret));
          } else {
            LOG_DEBUG("load addr succ", K(tmp_addr));
          }
        }
      }
    }
  }

  return ret;
}

DEF_TO_STRING(ObMysqlConfigParams)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(stat_table_sync_interval), K_(server_state_refresh_interval),
       K_(stat_dump_interval), K_(enable_flow_control), K_(flow_high_water_mark),
       K_(flow_low_water_mark), K_(flow_consumer_reenable_threshold),
       K_(flow_event_queue_threshold), K_(default_buffer_water_mark),
       K_(tunnel_request_size_threshold), K_(request_buffer_length),
       K_(sock_recv_buffer_size_out), K_(sock_send_buffer_size_out),
       K_(server_tcp_keepidle), K_(server_tcp_keepintvl),
       K_(server_tcp_keepcnt), K_(server_tcp_user_timeout),
       K_(sock_option_flag_out), K_(sock_packet_mark_out), K_(sock_packet_tos_out),
       K_(server_tcp_init_cwnd), K_(frequent_accept), K_(net_accept_threads));
  J_COMMA();
  J_KV(K_(short_async_task_timeout), K_(short_async_task_timeout), K_(min_congested_connect_timeout),
       K_(tenant_location_valid_time), K_(local_bound_ip), K_(listen_port), K_(stack_size), K_(work_thread_num),
       K_(task_thread_num), K_(block_thread_num), K_(grpc_thread_num), K_(automatic_match_work_thread),
       K_(enable_congestion), K_(enable_bad_route_reject), K_(test_server_addr),
       K_(sqlaudit_mem_limited), K_(max_connections), K_(client_max_connections),
       K_(enable_client_connection_lru_disconnect), K_(connect_observer_max_retries),
       K_(monitor_stat_low_threshold), K_(monitor_stat_middle_threshold), K_(monitor_stat_high_threshold));
  J_COMMA();
  J_KV(K_(enable_trans_detail_stats), K_(enable_mysqlsm_info),
       K_(enable_report_session_stats), K_(enable_strict_stat_time),
       K_(enable_cpu_topology), K_(internal_cmd_mem_limited), K_(enable_trace_stats),
       K_(slow_transaction_time_threshold), K_(slow_proxy_process_time_threshold),
       K_(query_digest_time_threshold), K_(slow_query_time_threshold),
       K_(proxy_service_mode), K_(server_routing_mode), K_(proxy_id), K_(proxy_idc_name),
       K_(client_max_memory_size),
       K_(default_inactivity_timeout), K_(enable_partition_table_route), K_(enable_pl_route),
       K_(enable_cluster_checkout), K_(enable_client_ip_checkout), K_(enable_force_request_follower),
       K_(enable_proxy_scramble),
       K_(enable_compression_protocol), K_(enable_ob_protocol_v2), K_(enable_reroute), K_(enable_index_route),
       K_(enable_causal_order_read));
  J_OBJ_END();
  return pos;
}

int ObMysqlConfigProcessor::reconfigure(const ObProxyConfig &proxy_config)
{
  int ret = OB_SUCCESS;
  ObMysqlConfigParams *params = new (std::nothrow) ObMysqlConfigParams();
  if (OB_ISNULL(params)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem for ObMysqlConfigParams", K(ret));
  } else {
    params->inc_ref();
    if (OB_FAIL(params->assign_config(proxy_config))) {
      LOG_WARN("fail to assign_config", K(ret));
    } else if (OB_FAIL(set(params))) {// add new config params
      LOG_WARN("fail to set new config params", K(ret));
    } else  {
      LOG_DEBUG("succ to set new config params", KPC(params));
    }
    // no matter succ or not
    params->dec_ref();
    params = NULL;
  }
  return ret;
}

ObMysqlConfigParams *ObMysqlConfigProcessor::acquire()
{
  // hand out a refcount to the caller. We should still have out
  // own refcount, so it should be at least 2.
  ObMysqlConfigParams *params = NULL;
  obsys::CRLockGuard lock(params_lock_);
  if (OB_ISNULL(params = params_)) {
    LOG_ERROR("current params_ is NULL");
  } else {
    params->inc_ref();
  }

  return params;
}

int ObMysqlConfigProcessor::release(ObMysqlConfigParams *params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(params)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("params is null", K(params), K(ret));
  } else {
    params->dec_ref();
  }
  return ret;
}

int ObMysqlConfigProcessor::set(ObMysqlConfigParams *params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(params)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(params), K(ret));
  } else {
    // new objects *must* start with a zero refcount. The mysql config
    // processor holds it's own refcount. We should be the only
    // refcount holder at this point.
    params->inc_ref();
    ObMysqlConfigParams *old_params = NULL;
    {
      obsys::CWLockGuard lock(params_lock_);
      old_params = params_;
      params_ = params;
    }

    if (OB_LIKELY(NULL != old_params)) {
      release(old_params);
      old_params = NULL;
    }
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
