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

#ifndef OBPROXY_MEMORY_PROMETHEUS_H
#define OBPROXY_MEMORY_PROMETHEUS_H

#include "proxy/mysqllib/ob_proxy_session_info.h"
#include "prometheus/ob_prometheus_processor.h"

namespace oceanbase
{
namespace obproxy
{
namespace prometheus
{

class ObMemoryPrometheus
{
public:

  static int handle_prometheus(const ObPrometheusMetrics metric, ...);
};


#define MEMORY_PROMETHEUS_STAT(metric, ...) \
  if (obutils::get_global_proxy_config().enable_prometheus && g_ob_prometheus_processor.is_inited()) { \
    ObMemoryPrometheus::handle_prometheus(metric, ##__VA_ARGS__); \
  } else {}
} // end of namespace prometheus
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_SQL_PROMETHEUS_H
