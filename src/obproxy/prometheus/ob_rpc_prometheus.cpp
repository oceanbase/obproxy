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

#include "prometheus/ob_rpc_prometheus.h"
#include "prometheus/ob_prometheus_utils.h"
#include "obutils/ob_proxy_config.h"

using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obrpc;

namespace oceanbase
{
namespace obproxy
{
namespace prometheus
{

int ObRPCPrometheus::handle_prometheus(const ObString &logic_tenant_name,
                                       const ObString &logic_database_name,
                                       const ObString &cluster_name,
                                       const ObString &tenant_name,
                                       const ObString &database_name,
                                       const ObRpcPacketCode pcode,
                                      //  const ObProxyBasicStmtType stmt_type,
                                       const ObPrometheusMetrics metric, ...)
{
  int ret = OB_SUCCESS;

  va_list args;
  va_start(args, metric);
  const ObString vip_addr_name;
  if (OB_FAIL(handle_prometheus(logic_tenant_name, logic_database_name, cluster_name,
                                tenant_name, vip_addr_name, database_name, pcode, metric, args))) {
    LOG_WDIAG("fail to handle_prometheus with ObClientSessionInfo", K(logic_tenant_name), K(logic_database_name),
             K(cluster_name), K(tenant_name), K(vip_addr_name), K(database_name), K(metric), K(ret));
  }

  va_end(args);

  return ret;
}

int ObRPCPrometheus::handle_net_prometheus(const ObRpcClientNetSessionInfo &cs_info,
                                       const ObPrometheusMetrics metric, ...)
{
  int ret = OB_SUCCESS;

  ObString logic_tenant_name;
  ObString logic_database_name;
  ObString cluster_name;
  ObString tenant_name;
  ObString database_name;
  ObString vip_addr_name;

  cs_info.get_logic_tenant_name(logic_tenant_name);
  if (OB_UNLIKELY(logic_tenant_name.empty())) {
    logic_tenant_name.assign_ptr(get_global_proxy_config().app_name_str_,
                                 static_cast<int32_t>(STRLEN(get_global_proxy_config().app_name_str_)));
  }

  if (cs_info.is_sharding_user()) {
    cs_info.get_logic_database_name(logic_database_name);
  } else {
    //这个函数只会处理 session 链接数, 在 sharding 模式下就不需要物理数据源了
    cs_info.get_cluster_name(cluster_name);
    cs_info.get_tenant_name(tenant_name);
    cs_info.get_database_name(database_name);
    if (get_global_proxy_config().need_convert_vip_to_tname) {
      cs_info.get_vip_addr_name(vip_addr_name);
    }
  }

  va_list args;
  va_start(args, metric);

  if (OB_FAIL(handle_prometheus(logic_tenant_name, logic_database_name, cluster_name,
                                tenant_name, vip_addr_name, database_name, OB_PACKET_NUM, metric, args))) {
    LOG_WDIAG("fail to handle_prometheus with ObClientSessionInfo", K(logic_tenant_name), K(logic_database_name),
             K(cluster_name), K(tenant_name), K(vip_addr_name), K(database_name), K(metric), K(ret));
  }

  va_end(args);

  return ret;
}

int ObRPCPrometheus::handle_prometheus(const ObString &logic_tenant_name,
                                       const ObString &logic_database_name,
                                       const ObString &cluster_name,
                                       const ObString &tenant_name,
                                       const ObString &vip_addr_name,
                                       const ObString &database_name,
                                       const ObRpcPacketCode pcode,
                                       const ObPrometheusMetrics metric,
                                       va_list args)
{
  int ret = OB_SUCCESS;

  ObVector<ObPrometheusLabel> &label_vector = ObProxyPrometheusUtils::get_thread_label_vector();
  label_vector.reset();
  ObProxyPrometheusUtils::build_label(label_vector, LABEL_LOGIC_TENANT, logic_tenant_name);
  ObProxyPrometheusUtils::build_label(label_vector, LABEL_LOGIC_SCHEMA, logic_database_name);
  ObProxyPrometheusUtils::build_label(label_vector, LABEL_CLUSTER, cluster_name);
  ObProxyPrometheusUtils::build_label(label_vector, LABEL_TENANT, tenant_name);

  switch(metric) {
  // case PROMETHEUS_TRANSACTION_COUNT:
  // {
  //   ObProxyPrometheusUtils::build_label(label_vector, LABEL_SCHEMA, database_name);
  //   if (OB_FAIL(g_ob_prometheus_processor.handle_counter(TRANSACTION_TOTAL, REQUEST_RPC_TOTAL_HELP, label_vector))) {
  //     LOG_WDIAG("fail to handle counter with TRANSACTION_TOTAL", K(ret));
  //   }
  //   break;
  // }
  case PROMETHEUS_REQUEST_COUNT:
  {
    bool is_slow = (bool)va_arg(args, int);
    bool is_error = (bool)va_arg(args, int);
    bool is_shard = (bool)va_arg(args, int);
    int64_t value = va_arg(args, int64_t);
    ObProxyPrometheusUtils::build_label(label_vector, LABEL_SCHEMA, database_name);
    ObProxyPrometheusUtils::build_label(label_vector, LABEL_RPC_TYPE, ObRpcPacketSet::name_of_pcode(pcode), false);
    ObProxyPrometheusUtils::build_label(label_vector, LABEL_RPC_SLOW, is_slow ? LABEL_TRUE : LABEL_FALSE, false);
    ObProxyPrometheusUtils::build_label(label_vector, LABEL_RPC_RESULT, is_error ? LABEL_FAIL : LABEL_SUCC, false);
    ObProxyPrometheusUtils::build_label(label_vector, LABEL_RPC_SHARD, is_shard ? LABEL_TRUE : LABEL_FALSE, false);

    if (OB_FAIL(g_ob_prometheus_processor.handle_counter(REQUEST_RPC_TOTAL, REQUEST_RPC_TOTAL_HELP, label_vector, value))) {
      LOG_WDIAG("fail to handle counter with REQUEST_RPC_TOTAL", K(ret));
    }
    break;
  }
  case PROMETHEUS_PREPARE_SEND_REQUEST_TIME:
  case PROMETHEUS_SERVER_PROCESS_REQUEST_TIME:
  case PROMETHEUS_REQUEST_TOTAL_TIME:
  {
    int64_t value = va_arg(args, int64_t);

    ObProxyPrometheusUtils::build_label(label_vector, LABEL_SCHEMA, database_name);
    ObProxyPrometheusUtils::build_label(label_vector, LABEL_RPC_TYPE, ObRpcPacketSet::name_of_pcode(pcode), false);
    ObProxyPrometheusUtils::build_label(label_vector, LABEL_TIME_TYPE, ObProxyPrometheusUtils::get_metric_lable(metric), false);

    if (OB_FAIL(g_ob_prometheus_processor.handle_gauge(COST_RPC_TOTAL, COST_RPC_TOTAL_HELP, label_vector, value))) {
      LOG_WDIAG("fail to handle gauge with COST_RPC_TOTAL", K(ret));
    }

    /*
    ObSortedVector<int64_t> buckets;
    buckets.push_back(get_global_proxy_config().monitor_stat_low_threshold);
    buckets.push_back(get_global_proxy_config().monitor_stat_middle_threshold);
    buckets.push_back(get_global_proxy_config().monitor_stat_high_threshold);
    if (OB_FAIL(g_ob_prometheus_processor.handle_histogram(COST_TOTAL, COST_TOTAL_HELP, label_vector, value, buckets))) {
      LOG_WDIAG("fail to handle counter with COST_TOTAL", K(ret));
    }
    */
    break;
  }
  case PROMETHEUS_CURRENT_SESSION:
  {
    bool is_client = va_arg(args, int);
    int32_t value = va_arg(args, int32_t);

    ObProxyPrometheusUtils::build_label(label_vector, LABEL_SESSION_TYPE, is_client ? LABEL_SESSION_CLIENT : LABEL_SESSION_SERVER, false);
    ObProxyPrometheusUtils::build_label(label_vector, LABEL_VIP, vip_addr_name, true);
    
    if (OB_FAIL(g_ob_prometheus_processor.handle_gauge(CURRENT_RPC_SESSION, CURRENT_RPC_SESSION_HELP,
                                                       label_vector, value, false))) {
      LOG_WDIAG("fail to handle counter with CURRENT_RPC_SESSION", K(ret));
    }
    break;
  }
  case PROMETHEUS_NEW_CLIENT_CONNECTIONS:
  {
    int32_t value = va_arg(args, int32_t);

    ObProxyPrometheusUtils::build_label(label_vector, LABEL_VIP, vip_addr_name, true);

    if (OB_FAIL(g_ob_prometheus_processor.handle_counter(NEW_RPC_CLIENT_CONNECTIONS, NEW_RPC_CLIENT_CONNECTIONS_HELP,
                                                         label_vector, value))) {
      LOG_WDIAG("fail to handle counter with NEW_RPC_CLIENT_CONNECTIONS", K(ret));
    }
    break;
  }
  default:
    break;
  }

  return ret;
}

} // end of namespace prometheus
} // end of namespace obproxy
} // end of namespace oceanbase
