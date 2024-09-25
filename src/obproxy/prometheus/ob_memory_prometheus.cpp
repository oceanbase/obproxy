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

#include "prometheus/ob_memory_prometheus.h"
#include "prometheus/ob_prometheus_utils.h"

using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace prometheus
{

int ObMemoryPrometheus::handle_prometheus(const ObPrometheusMetrics metric,
                                          ...)
{
  int ret = common::OB_SUCCESS;
  va_list args;
  va_start(args, metric);
  ObVector<ObPrometheusLabel> label_vector(10);
  switch(metric) {
    case PROMETHEUS_MEMORY_HOLD:
    case PROMETHEUS_MEMORY_USED: {
      int64_t value = va_arg(args, int64_t);
      LOG_DEBUG("set prometheus mem", K(metric), K(value));
      ObProxyPrometheusUtils::build_label(label_vector, LABLE_MEMORY_TYPE,
                                          ObProxyPrometheusUtils::get_mem_type_lable(metric), false);

      if (OB_FAIL(g_ob_prometheus_processor.set_gauge(ODP_MEMORY, ODP_MEMORY_HELP,
                                                        label_vector, value))) {
        LOG_WDIAG("fail to accumulate counter with NEW_CLIENT_CONNECTIONS", K(ret));
      }
      break;
    }
    default:
      break;
  }
  va_end(args);
  return ret;
}

} // end of namespace prometheus
} // end of namespace obproxy
} // end of namespace oceanbase
