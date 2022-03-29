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

#ifndef OBPROXY_SQL_PROMETHEUS_H
#define OBPROXY_SQL_PROMETHEUS_H

#include "proxy/mysqllib/ob_proxy_session_info.h"
#include "prometheus/ob_prometheus_processor.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace obproxy
{
namespace prometheus
{

class ObSQLPrometheus
{
public:
  static int handle_prometheus(const common::ObString &logic_tenant_name,
                               const common::ObString &logic_database_name,
                               const common::ObString &cluster_name,
                               const common::ObString &tenant_name,
                               const common::ObString &database_name,
                               const ObProxyBasicStmtType stmt_type,
                               const ObPrometheusMetrics metric, ...);

  static int handle_prometheus(const proxy::ObClientSessionInfo &cs_info,
                               const ObPrometheusMetrics metric, ...);
private:
  static int handle_prometheus(const common::ObString &logic_tenant_name,
                               const common::ObString &logic_database_name,
                               const common::ObString &cluster_name,
                               const common::ObString &tenant_name,
                               const common::ObString &vip_addr_name,
                               const common::ObString &database_name,
                               const ObProxyBasicStmtType stmt_type,
                               const ObPrometheusMetrics metric,
                               va_list args);
};

#define SQL_PROMETHEUS_STAT(logic_tenant_name, logic_database_name, cluster_name, \
                            tenant_name, database_name, stmt_type, metric, ...) \
  if (obutils::get_global_proxy_config().enable_prometheus && g_ob_prometheus_processor.is_inited()) { \
    ObSQLPrometheus::handle_prometheus(logic_tenant_name, logic_database_name, cluster_name, \
                                       tenant_name, database_name, stmt_type, metric, ##__VA_ARGS__); \
  } else {}

#define SESSION_PROMETHEUS_STAT(cs_info, metric, ...) \
  if (obutils::get_global_proxy_config().enable_prometheus && g_ob_prometheus_processor.is_inited()) { \
    ObSQLPrometheus::handle_prometheus(cs_info, metric, ##__VA_ARGS__); \
  } else {}

} // end of namespace prometheus
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_SQL_PROMETHEUS_H
