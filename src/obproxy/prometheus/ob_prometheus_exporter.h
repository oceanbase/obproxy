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

#ifndef OBPROXY_PROMETHEUS_EXPORTER_H
#define OBPROXY_PROMETHEUS_EXPORTER_H

#include <prometheus/registry.h>
#include <prometheus/counter.h>
#include <prometheus/histogram.h>
#include <prometheus/gauge.h>
#include <prometheus/exposer.h>
#include "lib/ob_define.h"

namespace oceanbase
{
namespace obproxy
{
namespace prometheus
{

class ObProxyPrometheusExporter
{
public:
  ObProxyPrometheusExporter():is_inited_(false), listen_port_(0), exposer_(NULL) {};
  ~ObProxyPrometheusExporter() {};

  int init(int32_t listen_port);
  int create_exposer();
  void destroy_exposer();

  int get_or_create_counter_family(const std::string& name, const std::string& help,
                                   const std::map<std::string, std::string>& labels,
                                   void *&family);
  int get_or_create_gauge_family(const std::string& name, const std::string& help,
                                 const std::map<std::string, std::string>& labels,
                                 void *&family);
  int get_or_create_histogram_family(const std::string& name, const std::string& help,
                                     const std::map<std::string, std::string>& labels,
                                     void *&family);

  template <typename T, typename... Args>
  int create_metric(void *family, const std::map<std::string, std::string>& labels,
                    void *&metric, Args&&... args);
  template <typename T>
  int remove_metric(void *family, void *metric);

  int handle_counter(void *metric, const double value);
  int handle_gauge(void *metric, const double value);
  int handle_histogram(void *metric, const double sum,
                       std::vector<double> &buckets_count);

private:
  template <typename T>
  int get_family(const std::string& name, ::prometheus::Family<T>*& family);

private:
  bool is_inited_;
  int32_t listen_port_;
  ::prometheus::Exposer *exposer_;
  std::shared_ptr<::prometheus::Registry> registry_;
  std::unordered_map<std::string, ::prometheus::Collectable*> family_map_;
};

template <typename T>
int ObProxyPrometheusExporter::get_family(const std::string& name, ::prometheus::Family<T>*& family)
{
  int ret = common::OB_SUCCESS;

  auto family_iter = family_map_.find(name);
  if (family_iter == family_map_.end()) {
    ret = common::OB_ENTRY_NOT_EXIST;
  } else {
    family = dynamic_cast<::prometheus::Family<T>*>(family_iter->second);
  }

  return ret;
}


template <typename T, typename... Args>
int ObProxyPrometheusExporter::create_metric(void *family, const std::map<std::string, std::string>& labels,
                                     void *&metric, Args&&... args)
{
  int ret = common::OB_SUCCESS;

  ::prometheus::Family<T>* exporter_family = static_cast<::prometheus::Family<T>*>(family);
  auto& exporter_metric = exporter_family->Add(labels, args...);
  metric = static_cast<void *>(&exporter_metric);

  return ret;
}

template <typename T>
int ObProxyPrometheusExporter::remove_metric(void *family, void *metric)
{
  int ret = common::OB_SUCCESS;
  ::prometheus::Family<T> *exporter_family = static_cast<::prometheus::Family<T>*>(family);
  exporter_family->Remove(static_cast<T*>(metric));

  return ret;
}

ObProxyPrometheusExporter& get_obproxy_prometheus_exporter();

} // end of namespace prometheus
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_PROMETHEUS_EXPORTER_H
