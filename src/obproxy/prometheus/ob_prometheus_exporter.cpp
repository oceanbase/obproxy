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

#include "prometheus/ob_prometheus_exporter.h"

using namespace prometheus;
using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace prometheus
{

int ObProxyPrometheusExporter::init(int32_t listen_port)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (!(registry_ = std::make_shared<Registry>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    try {
      if (!(exposer_ = new Exposer(std::to_string(listen_port)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        listen_port_ = listen_port;

        exposer_->RegisterCollectable(registry_);
        is_inited_ = true;
      }
    } catch (...) {
      ret = OB_INVALID_ARGUMENT;
    }
  }

  return ret;
}

#define GET_OR_CREATE_FAMILY_FUNC(func_name, type) \
int ObProxyPrometheusExporter::get_or_create_##func_name##_family(const std::string& name, const std::string& help, \
                                                          const std::map<std::string, std::string>& labels, \
                                                          void *&family) \
{ \
  int ret = OB_SUCCESS; \
  family = NULL; \
  Family<type>* exporter_family = NULL; \
  if (OB_FAIL(get_family(name, exporter_family))) { \
    if (OB_ENTRY_NOT_EXIST == ret) { \
      exporter_family = &Build##type().Name(name) \
                                   .Help(help) \
                                   .Labels(labels) \
                                   .Register(*registry_); \
      std::pair<std::string, Collectable*> temp_pair(name, exporter_family); \
      family_map_.insert(temp_pair); \
      ret = OB_SUCCESS; \
    } \
  } \
\
  if (OB_SUCC(ret)) { \
    family = static_cast<void*>(exporter_family); \
  } \
  return ret; \
}

GET_OR_CREATE_FAMILY_FUNC(counter, Counter)
GET_OR_CREATE_FAMILY_FUNC(gauge, Gauge)
GET_OR_CREATE_FAMILY_FUNC(histogram, Histogram)

int ObProxyPrometheusExporter::handle_counter(void *metric, const double value)
{
  int ret = OB_SUCCESS;

  Counter* counter = static_cast<Counter*>(metric);
  counter->Increment(value);

  return ret;
}

int ObProxyPrometheusExporter::handle_gauge(void *metric, const double value)
{
  int ret = OB_SUCCESS;

  Gauge* gauge = static_cast<Gauge*>(metric);
  if (value >= 0) {
      gauge->Increment(value);
  } else {
      gauge->Decrement(-1.0 * value);
  }

  return ret;
}

int ObProxyPrometheusExporter::handle_histogram(void *metric, const double sum,
                                        std::vector<double> &bucket_counts)
{
  int ret = OB_SUCCESS;

  Histogram* histogram = static_cast<Histogram*>(metric);
  histogram->ObserveMultiple(bucket_counts, sum);

  return ret;
}

ObProxyPrometheusExporter& get_obproxy_prometheus_exporter()
{
  static ObProxyPrometheusExporter obproxy_prometheus_exporter;
  return obproxy_prometheus_exporter;
}

int init_prometheus(int32_t listen_port)
{
  return get_obproxy_prometheus_exporter().init(listen_port);
}

} // end of namespace prometheus
} // end of namespace obproxy
} // end of namespace oceanbase
