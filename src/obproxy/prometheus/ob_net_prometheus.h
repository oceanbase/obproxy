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

#ifndef OBPROXY_NET_PROMETHEUS_H
#define OBPROXY_NET_PROMETHEUS_H

#include "proxy/mysqllib/ob_proxy_session_info.h"
#include "prometheus/ob_prometheus_processor.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace obproxy
{
namespace prometheus
{

class ObNetPrometheus
{
public:
  static int handle_prometheus(const common::ObString &logic_tenant_name,
                               const common::ObString &logic_database_name,
                               const common::ObString &cluster_name,
                               const common::ObString &tenant_name,
                               const common::ObString &database_name,
                               const ObPrometheusMetrics metric, ...);
private:
  static int handle_prometheus(const common::ObString &logic_tenant_name,
                               const common::ObString &logic_database_name,
                               const common::ObString &cluster_name,
                               const common::ObString &tenant_name,
                               const common::ObString &database_name,
                               const ObPrometheusMetrics metric, va_list args);
};

#define NET_PROMETHEUS_STAT(logic_tenant_name, logic_database_name, cluster_name, \
                            tenant_name, database_name, metric, ...) \
  if (obutils::get_global_proxy_config().enable_prometheus \
      && obutils::get_global_proxy_config().enable_extra_prometheus_metric \
      && g_ob_prometheus_processor.is_inited()) { \
    ObNetPrometheus::handle_prometheus(logic_tenant_name, logic_database_name, cluster_name, \
                                       tenant_name, database_name, metric, ##__VA_ARGS__); \
  } else {}

} // end of namespace prometheus
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_NET_PROMETHEUS_H
