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

#ifndef OBPROXY_PROMETHEUS_CONVERT_H
#define OBPROXY_PROMETHEUS_CONVERT_H

#include "prometheus/ob_prometheus_processor.h"
#include "lib/string/ob_string.h"
#include <map>

namespace oceanbase
{
namespace obproxy
{
namespace prometheus
{

class ObProxyPrometheusConvert
{
public:
  static int get_or_create_exporter_family(const common::ObString &name, const common::ObString &help,
                                           const common::ObVector<ObPrometheusLabel> &constant_label_array,
                                           const ObPrometheusMetricType metric_type, void *&family);
  static int create_counter_metric(void *family,
                                   const common::ObVector<ObPrometheusLabel> &label_array,
                                   void *&metric);
  static int create_gauge_metric(void *family,
                                 const common::ObVector<ObPrometheusLabel> &label_array,
                                 void *&metric);
  static int create_histogram_metric(void *family,
                                     const common::ObVector<ObPrometheusLabel> &label_array,
                                     const common::ObSortedVector<int64_t> &ob_bucket_boundaries,
                                     void *&metric);

  static int remove_metric(void* family, void* metric,
                           const ObPrometheusMetricType metric_type);

  static int handle_counter(void *metric, const int64_t value);
  static int handle_gauge(void *metric, const int64_t value);
  static int handle_gauge(void *metric, const double value);
  static int handle_histogram(void *metric, const int64_t sum,
                              const common::ObVector<int64_t> &ob_bucket_counts);
private:
  static void build_label_map(std::map<std::string, std::string>& label_map,
                              const common::ObVector<ObPrometheusLabel> &labels);
};

} // end of namespace prometheus
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_PROMETHEUS_CONVERT_H
