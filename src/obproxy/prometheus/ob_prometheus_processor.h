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

#ifndef OBPROXY_PROMETHEUS_PROCESSOR_H
#define OBPROXY_PROMETHEUS_PROCESSOR_H

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_vector.h"
#include "lib/hash/ob_build_in_hashmap.h"
#include "prometheus/ob_prometheus_info.h"
#include "obutils/ob_async_common_task.h"

namespace oceanbase
{
namespace obproxy
{
namespace prometheus
{

class ObPrometheusProcessor
{
public:
  ObPrometheusProcessor();
  ~ObPrometheusProcessor();

  int init();
  bool is_inited() { return is_inited_; }

  int start_prometheus_task();

  int handle_counter(const char *name_ptr, const char *help_ptr,
                     common::ObVector<ObPrometheusLabel> &label_array,
                     int64_t value = 1);

  int handle_gauge(const char *name_ptr, const char *help_ptr,
                   common::ObVector<ObPrometheusLabel> &label_array,
                   int64_t value, bool allow_delete = true);

  int handle_histogram(const char *name_ptr, const char *help_ptr,
                       common::ObVector<ObPrometheusLabel> &label_array,
                       int64_t value, common::ObSortedVector<int64_t>& buckets);
private:
  static int prometheus_sync_task();
  static void update_prometheus_sync_interval();
  obutils::ObAsyncCommonTask *get_prometheus_sync_cont() { return prometheus_sync_cont_; }

  int sync_to_exporter(ObPrometheusFamilyHashTable::iterator &family_iter,
                       ObPrometheusMetricHashTable::iterator &metric_iter);
  int do_prometheus_sync_task();

  int get_or_create_exporter_metric(ObPrometheusFamilyHashTable::iterator &family_iter,
                                    ObPrometheusMetricHashTable::iterator &metric_iter,
                                    void *&exporter_metric);

  template <typename TA, typename T>
  int get_or_create_metric(ObPrometheusFamily *family,
                           common::ObVector<ObPrometheusLabel> &label_array,
                           TA& args, T *&metric, bool allow_delete = true);

  int get_family(const common::ObString &name, ObPrometheusFamily *&family);

  int create_family(const common::ObString &name, const common::ObString &help,
                    const ObPrometheusMetricType metric_type,
                    const common::ObVector<ObPrometheusLabel> &label_array,
                    ObPrometheusFamily *&family);

  int get_or_create_family(const common::ObString &name, const common::ObString &help,
                           const ObPrometheusMetricType metric_type,
                           const common::ObVector<ObPrometheusLabel> &label_array,
                           ObPrometheusFamily *&family);

private:
  bool is_inited_;
  obutils::ObAsyncCommonTask *prometheus_sync_cont_;

  char version_[100];
  char proxy_ip_[common::OB_IP_STR_BUFF]; // ip primary key
  common::ObVector<ObPrometheusLabel> default_constant_labels_;

  ObPrometheusFamilyHashTable family_hash_;
  bool need_expire_metric_;
  int32_t metric_num_;

  common::DRWLock lock_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPrometheusProcessor);
};

extern ObPrometheusProcessor g_ob_prometheus_processor;

} // end of namespace prometheus
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_PROMETHEUS_PROCESSOR_H
