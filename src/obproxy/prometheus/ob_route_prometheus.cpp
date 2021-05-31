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

#include "prometheus/ob_route_prometheus.h"
#include "prometheus/ob_prometheus_utils.h"
#include "obutils/ob_proxy_config.h"

using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace prometheus
{

int ObRoutePrometheus::handle_prometheus(const ObTableEntryName &name, const ObPrometheusMetrics metric, ...)
{
  int ret = OB_SUCCESS;

  va_list args;
  va_start(args, metric);
  if (OB_FAIL(handle_prometheus(name.cluster_name_, name.tenant_name_, name.database_name_, metric, args))) {
    LOG_WARN("fail to handle_prometheus with ObTableEntryName", K(name.cluster_name_), K(name.tenant_name_),
             K(name.database_name_), K(metric), K(ret));
  }

  va_end(args);

  return ret;
}

int ObRoutePrometheus::handle_prometheus(const ObString &cluster_name,
                                         const ObString &tenant_name,
                                         const ObString &database_name,
                                         const ObPrometheusMetrics metric,
                                         va_list args)
{
  int ret = OB_SUCCESS;

  ObVector<ObPrometheusLabel> &label_vector = ObProxyPrometheusUtils::get_thread_label_vector();
  label_vector.reset();
  ObProxyPrometheusUtils::build_label(label_vector, LABEL_LOGIC_TENANT, obutils::get_global_proxy_config().app_name_str_, false);
  ObProxyPrometheusUtils::build_label(label_vector, LABEL_LOGIC_SCHEMA, "", false);
  ObProxyPrometheusUtils::build_label(label_vector, LABEL_CLUSTER, cluster_name);
  ObProxyPrometheusUtils::build_label(label_vector, LABEL_TENANT, tenant_name);
  ObProxyPrometheusUtils::build_label(label_vector, LABEL_SCHEMA, database_name);

  switch(metric) {
  case PROMETHEUS_ENTRY_LOOKUP_COUNT:
  {
    ObPrometheusEntryType entryType = (ObPrometheusEntryType)va_arg(args,int);
    bool route_hit = va_arg(args, int);
    bool result = va_arg(args, int);

    ObProxyPrometheusUtils::build_label(label_vector, LABEL_ENTRY_TYPE, ObProxyPrometheusUtils::get_type_lable(entryType), false);
    ObProxyPrometheusUtils::build_label(label_vector, LABEL_ROUTE_HIT, route_hit ? LABEL_TRUE : LABEL_FALSE, false);
    ObProxyPrometheusUtils::build_label(label_vector, LABEL_ROUTE_RESULT, result ? LABEL_TRUE : LABEL_FALSE, false);

    if (OB_FAIL(g_ob_prometheus_processor.handle_counter(ENTRY_TOTAL, ENTRY_TOTAL_HELP, label_vector))) {
      LOG_WARN("fail to handle counter with ENTRY_TOTAL", K(ret));
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
