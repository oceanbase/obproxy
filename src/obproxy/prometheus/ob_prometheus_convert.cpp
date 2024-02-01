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

#include "prometheus/ob_prometheus_convert.h"
#include "prometheus/ob_prometheus_exporter.h"

using namespace prometheus;
using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace prometheus
{

void ObProxyPrometheusConvert::build_label_map(std::map<std::string, std::string>& label_map,
                                               const ObVector<ObPrometheusLabel> &label_array)
{
  for (int i = 0; i< label_array.size(); i++) {
    ObPrometheusLabel &label = label_array[i];
    std::string key(label.get_key().ptr(), label.get_key().length());
    std::string value(label.get_value().ptr(), label.get_value().length());
    label_map.insert(std::pair<std::string, std::string>(key, value));
  }
}

int ObProxyPrometheusConvert::get_or_create_exporter_family(const ObString &name, const ObString &help,
                                                            const ObVector<ObPrometheusLabel> &constant_label_array,
                                                            const ObPrometheusMetricType metric_type, void *&family)
{
  int ret = OB_SUCCESS;

  std::string name_str(name.ptr(), name.length());
  std::string help_str(help.ptr(), help.length());

  std::map<std::string, std::string> constant_label_map;
  build_label_map(constant_label_map, constant_label_array);

  switch (metric_type) {
    case PROMETHEUS_TYPE_COUNTER:
      ret = get_obproxy_prometheus_exporter().get_or_create_counter_family(name_str, help_str, constant_label_map, family);
      break;
    case PROMETHEUS_TYPE_GAUGE:
      ret = get_obproxy_prometheus_exporter().get_or_create_gauge_family(name_str, help_str, constant_label_map, family);
      break;
    case PROMETHEUS_TYPE_HISTOGRAM:
      ret = get_obproxy_prometheus_exporter().get_or_create_histogram_family(name_str, help_str, constant_label_map, family);
      break;
    default:
      break;
  }

  if (OB_FAIL(ret)) {
    LOG_WDIAG("fail to get or create faimyl", K(name), K(metric_type), K(ret));
  }

  return ret;
}

int ObProxyPrometheusConvert::create_counter_metric(void *family,
                                                    const ObVector<ObPrometheusLabel> &label_array,
                                                    void *&metric)
{
  int ret = OB_SUCCESS;

  std::map<std::string, std::string> label_map;
  build_label_map(label_map, label_array);

  if (OB_FAIL(get_obproxy_prometheus_exporter().create_metric<Counter>(family, label_map, metric))) {
    LOG_WDIAG("fail to create metric", KP(family), K(label_array), K(ret));
  }

  return ret;
}

int ObProxyPrometheusConvert::create_gauge_metric(void *family,
                                                  const ObVector<ObPrometheusLabel> &label_array,
                                                  void *&metric)
{
  int ret = OB_SUCCESS;

  std::map<std::string, std::string> label_map;
  build_label_map(label_map, label_array);

  if (OB_FAIL(get_obproxy_prometheus_exporter().create_metric<Gauge>(family, label_map, metric))) {
    LOG_WDIAG("fail to create metric", KP(family), K(label_array), K(ret));
  }

  return ret;
}

int ObProxyPrometheusConvert::create_histogram_metric(void *family,
                                                      const ObVector<ObPrometheusLabel> &label_array,
                                                      const ObSortedVector<int64_t> &ob_bucket_boundaries,
                                                      void *&metric)
{
  int ret = OB_SUCCESS;

  std::map<std::string, std::string> label_map;
  build_label_map(label_map, label_array);

  std::vector<double> bucket_boundaries;
  for (int i = 0; i< ob_bucket_boundaries.size(); i++) {
    bucket_boundaries.push_back(static_cast<double>(ob_bucket_boundaries[i]));
  }

  if (OB_FAIL(get_obproxy_prometheus_exporter().create_metric<Histogram>(family, label_map, metric, bucket_boundaries))) {
    LOG_WDIAG("fail to create metric", KP(family), K(label_array), K(ret));
  }

  return ret;
}

int ObProxyPrometheusConvert::remove_metric(void* family, void* metric,
                                            const ObPrometheusMetricType metric_type)
{
  int ret = OB_SUCCESS;

  switch (metric_type) {
    case PROMETHEUS_TYPE_COUNTER:
      ret = get_obproxy_prometheus_exporter().remove_metric<Counter>(family, metric);
      break;
    case PROMETHEUS_TYPE_GAUGE:
      ret = get_obproxy_prometheus_exporter().remove_metric<Gauge>(family, metric);
      break;
    case PROMETHEUS_TYPE_HISTOGRAM:
      ret = get_obproxy_prometheus_exporter().remove_metric<Histogram>(family, metric);
      break;
    default:
      break;
  }

  if (OB_FAIL(ret)) {
    LOG_WDIAG("fail to remove metric", KP(family), KP(metric), K(metric_type), K(ret));
  }

  return ret;
}

int ObProxyPrometheusConvert::handle_counter(void *metric, const int64_t value)
{
  return get_obproxy_prometheus_exporter().handle_counter(metric, static_cast<double>(value));
}

int ObProxyPrometheusConvert::handle_gauge(void *metric, const double value)
{
  return get_obproxy_prometheus_exporter().handle_gauge(metric, value);
}

int ObProxyPrometheusConvert::handle_gauge(void *metric, const int64_t value)
{
  return get_obproxy_prometheus_exporter().handle_gauge(metric, static_cast<double>(value));
}

int ObProxyPrometheusConvert::handle_histogram(void *metric, const int64_t sum,
                                               const ObVector<int64_t> &ob_bucket_counts)
{
  std::vector<double> bucket_counts;
  for (int i = 0; i< ob_bucket_counts.size(); i++) {
    bucket_counts.push_back(static_cast<double>(ob_bucket_counts[i]));
  }

  return get_obproxy_prometheus_exporter().handle_histogram(metric, static_cast<double>(sum), bucket_counts);
}

} // end of namespace prometheus
} // end of namespace obproxy
} // end of namespace oceanbase
