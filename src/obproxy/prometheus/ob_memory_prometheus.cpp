/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
    case PROMETHEUS_PS_COUNT:
    case PROMETHEUS_PS_MEMORY_USED: {
      int64_t value = va_arg(args, int64_t);
      LOG_DEBUG("set prometheus mem", K(metric), K(value));
      ObProxyPrometheusUtils::build_label(label_vector, LABLE_PS_TYPE,
                                          ObProxyPrometheusUtils::get_ps_type_lable(metric), false);

      if (OB_FAIL(g_ob_prometheus_processor.set_gauge(PS_CACHE, PS_CACHE_HELP,
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
