// Copyright (c) 2014-2016 Alibaba Inc. All Rights Reserved.
// Author:
// Normalizer:

#ifndef OBPROXY_RPC_PROMETHEUS_H
#define OBPROXY_RPC_PROMETHEUS_H

#include "proxy/rpc_optimize/net/ob_proxy_rpc_session_info.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "prometheus/ob_prometheus_processor.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace obproxy
{
namespace prometheus
{

class ObRPCPrometheus
{
public:
  static int handle_prometheus(const common::ObString &logic_tenant_name,
                               const common::ObString &logic_database_name,
                               const common::ObString &cluster_name,
                               const common::ObString &tenant_name,
                               const common::ObString &database_name,
                               const obrpc::ObRpcPacketCode pcode,
                               const ObPrometheusMetrics metric, ...);

  static int handle_net_prometheus(const proxy::ObRpcClientNetSessionInfo &cs_info,
                               const ObPrometheusMetrics metric, ...);
private:
  static int handle_prometheus(const common::ObString &logic_tenant_name,
                               const common::ObString &logic_database_name,
                               const common::ObString &cluster_name,
                               const common::ObString &tenant_name,
                               const common::ObString &vip_addr_name,
                               const common::ObString &database_name,
                               const obrpc::ObRpcPacketCode pcode,
                               const ObPrometheusMetrics metric,
                               va_list args);
};

#define RPC_PROMETHEUS_STAT(logic_tenant_name, logic_database_name, cluster_name, \
                            tenant_name, database_name, pcode, metric, ...) \
  if (obutils::get_global_proxy_config().enable_prometheus && g_ob_prometheus_processor.is_inited()) { \
    ObRPCPrometheus::handle_prometheus(logic_tenant_name, logic_database_name, cluster_name, \
                                       tenant_name, database_name, pcode, metric, ##__VA_ARGS__); \
  } else {}

#define RPC_SESSION_PROMETHEUS_STAT(cs_info, metric, ...) \
  if (obutils::get_global_proxy_config().enable_prometheus && g_ob_prometheus_processor.is_inited()) { \
    ObRPCPrometheus::handle_prometheus(cs_info, metric, ##__VA_ARGS__); \
  } else {}

#define RPC_NET_SESSION_PROMETHEUS_STAT(cs_info, metric, ...) \
  if (obutils::get_global_proxy_config().enable_prometheus && g_ob_prometheus_processor.is_inited()) { \
    ObRPCPrometheus::handle_net_prometheus(cs_info, metric, ##__VA_ARGS__); \
  } else {}

} // end of namespace prometheus
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_RPC_PROMETHEUS_H
