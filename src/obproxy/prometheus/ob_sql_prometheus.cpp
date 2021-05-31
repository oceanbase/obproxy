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

#include "prometheus/ob_sql_prometheus.h"
#include "prometheus/ob_prometheus_utils.h"
#include "obutils/ob_proxy_config.h"

using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace prometheus
{

int ObSQLPrometheus::handle_prometheus(const ObString &logic_tenant_name,
                                       const ObString &logic_database_name,
                                       const ObString &cluster_name,
                                       const ObString &tenant_name,
                                       const ObString &database_name,
                                       const ObProxyBasicStmtType stmt_type,
                                       const ObPrometheusMetrics metric, ...)
{
  int ret = OB_SUCCESS;

  va_list args;
  va_start(args, metric);

  if (OB_FAIL(handle_prometheus(logic_tenant_name, logic_database_name, cluster_name,
                                tenant_name, database_name, stmt_type, metric, args))) {
    LOG_WARN("fail to handle_prometheus with ObClientSessionInfo", K(logic_tenant_name), K(logic_database_name),
             K(cluster_name), K(tenant_name), K(database_name), K(metric), K(ret));
  }

  va_end(args);

  return ret;
}

int ObSQLPrometheus::handle_prometheus(const ObClientSessionInfo &cs_info,
                                       const ObPrometheusMetrics metric, ...)
{
  int ret = OB_SUCCESS;

  ObString logic_tenant_name;
  ObString logic_database_name;
  ObString cluster_name;
  ObString tenant_name;
  ObString database_name;

  cs_info.get_logic_tenant_name(logic_tenant_name);
  if (OB_UNLIKELY(logic_tenant_name.empty())) {
    logic_tenant_name.assign_ptr(get_global_proxy_config().app_name_str_,
                                 static_cast<int32_t>(STRLEN(get_global_proxy_config().app_name_str_)));
  }

  if (cs_info.is_sharding_user()) {
    cs_info.get_logic_database_name(logic_database_name);
  } else {
    //shardng mode no need physic datasource
    cs_info.get_cluster_name(cluster_name);
    cs_info.get_tenant_name(tenant_name);
    cs_info.get_database_name(database_name);
  }

  va_list args;
  va_start(args, metric);

  if (OB_FAIL(handle_prometheus(logic_tenant_name, logic_database_name, cluster_name,
                                tenant_name, database_name, OBPROXY_T_MAX, metric, args))) {
    LOG_WARN("fail to handle_prometheus with ObClientSessionInfo", K(logic_tenant_name), K(logic_database_name),
             K(cluster_name), K(tenant_name), K(database_name), K(metric), K(ret));
  }

  va_end(args);

  return ret;
}

int ObSQLPrometheus::handle_prometheus(const ObString &logic_tenant_name,
                                       const ObString &logic_database_name,
                                       const ObString &cluster_name,
                                       const ObString &tenant_name,
                                       const ObString &database_name,
                                       const ObProxyBasicStmtType stmt_type,
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
  case PROMETHEUS_TRANSACTION_COUNT:
  {
    ObProxyPrometheusUtils::build_label(label_vector, LABEL_SCHEMA, database_name);
    if (OB_FAIL(g_ob_prometheus_processor.handle_counter(TRANSACTION_TOTAL, TRANSACTION_TOTAL_HELP, label_vector))) {
      LOG_WARN("fail to handle counter with TRANSACTION_TOTAL", K(ret));
    }
    break;
  }
  case PROMETHEUS_REQUEST_COUNT:
  {
    bool is_slow = (bool)va_arg(args, int);
    bool is_error = (bool)va_arg(args, int);
    ObProxyPrometheusUtils::build_label(label_vector, LABEL_SCHEMA, database_name);
    ObProxyPrometheusUtils::build_label(label_vector, LABEL_SQL_TYPE, get_print_stmt_name(stmt_type), false);
    ObProxyPrometheusUtils::build_label(label_vector, LABEL_SQL_SLOW, is_slow ? LABEL_TRUE : LABEL_FALSE, false);
    ObProxyPrometheusUtils::build_label(label_vector, LABEL_SQL_RESULT, is_error ? LABEL_FAIL : LABEL_SUCC, false);

    if (OB_FAIL(g_ob_prometheus_processor.handle_counter(REQUEST_TOTAL, REQUEST_TOTAL_HELP, label_vector))) {
      LOG_WARN("fail to handle counter with REQUEST_TOTAL", K(ret));
    }
    break;
  }
  case PROMETHEUS_PREPARE_SEND_REQUEST_TIME:
  case PROMETHEUS_SERVER_PROCESS_REQUEST_TIME:
  case PROMETHEUS_REQUEST_TOTAL_TIME:
  {
    int64_t value = va_arg(args, int64_t);

    ObProxyPrometheusUtils::build_label(label_vector, LABEL_SCHEMA, database_name);
    ObProxyPrometheusUtils::build_label(label_vector, LABEL_SQL_TYPE, get_print_stmt_name(stmt_type), false);
    ObProxyPrometheusUtils::build_label(label_vector, LABEL_TIME_TYPE, ObProxyPrometheusUtils::get_metric_lable(metric), false);

    if (OB_FAIL(g_ob_prometheus_processor.handle_gauge(COST_TOTAL, COST_TOTAL_HELP, label_vector, value))) {
      LOG_WARN("fail to handle gauge with COST_TOTAL", K(ret));
    }

    /*
    ObSortedVector<int64_t> buckets;
    buckets.push_back(get_global_proxy_config().monitor_stat_low_threshold);
    buckets.push_back(get_global_proxy_config().monitor_stat_middle_threshold);
    buckets.push_back(get_global_proxy_config().monitor_stat_high_threshold);
    if (OB_FAIL(g_ob_prometheus_processor.handle_histogram(COST_TOTAL, COST_TOTAL_HELP, label_vector, value, buckets))) {
      LOG_WARN("fail to handle counter with COST_TOTAL", K(ret));
    }
    */
    break;
  }
  case PROMETHEUS_CURRENT_SESSION:
  {
    bool is_client = va_arg(args, int);
    int32_t value = va_arg(args, int32_t);

    ObProxyPrometheusUtils::build_label(label_vector, LABEL_SESSION_TYPE, is_client ? LABEL_SESSION_CLIENT : LABEL_SESSION_SERVER, false);

    if (OB_FAIL(g_ob_prometheus_processor.handle_gauge(CURRENT_SESSION, CURRENT_SESSION_HELP,
                                                       label_vector, value, false))) {
      LOG_WARN("fail to handle counter with CURRENT_SESSION", K(ret));
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
