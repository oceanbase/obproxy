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

#include "prometheus/ob_net_prometheus.h"
#include "prometheus/ob_prometheus_utils.h"

using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace prometheus
{

int ObNetPrometheus::handle_prometheus(const ObString &logic_tenant_name,
                                       const ObString &logic_database_name,
                                       const ObString &cluster_name,
                                       const ObString &tenant_name,
                                       const ObString &database_name,
                                       const ObPrometheusMetrics metric, ...)
{
  int ret = OB_SUCCESS;

  va_list args;
  va_start(args, metric);

  if (OB_FAIL(handle_prometheus(logic_tenant_name, logic_database_name, cluster_name,
                                tenant_name, database_name, metric, args))) {
    LOG_WDIAG("fail to handle_prometheus with ObClientSessionInfo", K(logic_tenant_name), K(logic_database_name),
             K(cluster_name), K(tenant_name), K(database_name), K(metric), K(ret));
  }

  va_end(args);

  return ret;
}

int ObNetPrometheus::handle_prometheus(const ObString &logic_tenant_name,
                                       const ObString &logic_database_name,
                                       const ObString &cluster_name,
                                       const ObString &tenant_name,
                                       const ObString &database_name,
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
  ObProxyPrometheusUtils::build_label(label_vector, LABEL_SCHEMA, database_name);

  switch(metric) {
  case PROMETHEUS_REQUEST_BYTE:
  {
    bool is_request = va_arg(args, int);
    bool is_client = va_arg(args, int);
    int64_t value = va_arg(args, int64_t);

    ObProxyPrometheusUtils::build_label(label_vector, LABEL_TRANS_TYPE, is_request ? LABEL_TRANS_REQUEST : LABEL_TRANS_RESPONSE, false);
    ObProxyPrometheusUtils::build_label(label_vector, LABEL_SESSION_TYPE, is_client ? LABEL_SESSION_CLIENT : LABEL_SESSION_SERVER, false);

    if (OB_FAIL(g_ob_prometheus_processor.handle_counter(REQUEST_BYTE, REQUEST_BYTE_HELP, label_vector, value))) {
      LOG_WDIAG("fail to handle counter with REQUEST_BYTE", K(value), K(ret));
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
