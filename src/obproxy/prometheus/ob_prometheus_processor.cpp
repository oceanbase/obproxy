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

#include "prometheus/ob_prometheus_processor.h"
#include "prometheus/ob_prometheus_utils.h"
#include "prometheus/ob_prometheus_convert.h"
#include "obutils/ob_proxy_table_processor_utils.h"
#include "obutils/ob_proxy_config.h"
#include "utils/ob_proxy_monitor_utils.h"
#include "prometheus/ob_thread_prometheus.h"
#include "iocore/net/ob_net_def.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace prometheus
{
int init_prometheus(int32_t listen_port);
void destory_prometheus_exposer();
int create_prometheus_exposer();

ObPrometheusProcessor g_ob_prometheus_processor;

int ObPrometheusProcessor::start_prometheus()
{
  int ret = OB_SUCCESS;
  // Initialize thread-level prometheus statistics
  int64_t net_thread_count = g_event_processor.thread_count_for_type_[ET_NET];
  ObEThread **ethreads = g_event_processor.event_thread_[ET_NET];
  for (int64_t i = 0; OB_SUCC(ret) && i < net_thread_count; i++) {
    if (OB_ISNULL(ethreads[i]->thread_prometheus_ = new(std::nothrow) ObThreadPrometheus())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to new ObThreadPrometheus", K(i), K(ret));
    } else if (OB_FAIL(ethreads[i]->thread_prometheus_->init(ethreads[i]))) {
      LOG_WDIAG("fail to init thread prometheus", K(i), K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(start_prometheus_task())) {
    LOG_WDIAG("start prometheus failed", K(ret));
  }

  return ret;
}

int ObPrometheusProcessor::start_one_prometheus(int64_t index)
{
  int ret = OB_SUCCESS;
  ObEThread **ethreads = g_event_processor.event_thread_[ET_NET];
  if (OB_ISNULL(ethreads[index]->thread_prometheus_ = new(std::nothrow) ObThreadPrometheus())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to new ObThreadPrometheus", K(index), K(ret));
  } else if (OB_FAIL(ethreads[index]->thread_prometheus_->init(ethreads[index]))) {
    LOG_WDIAG("fail to init thread prometheus", K(index), K(ret));
  }
  return ret;
}

int ObPrometheusProcessor::start_prometheus_task()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K(ret));
  } else if (OB_UNLIKELY(NULL != prometheus_sync_cont_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("prometheus_sync_cont should be null here", K_(prometheus_sync_cont), K(ret));
  }

  if (OB_SUCC(ret)) {
    int64_t interval_us = ObProxyMonitorUtils::get_next_schedule_time(get_global_proxy_config().prometheus_sync_interval);

    if (OB_ISNULL(prometheus_sync_cont_ = ObAsyncCommonTask::create_and_start_repeat_task(interval_us,
                  "prometheus_sync_task",
                  ObPrometheusProcessor::prometheus_sync_task,
                  ObPrometheusProcessor::update_prometheus_sync_interval, false, event::ET_TASK))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to create and start prometheus_sync_task task", K(ret));
    } else {
      LOG_INFO("succ to start prometheus sync task", K(interval_us));
    }
  }

  return ret;
}

int ObPrometheusProcessor::prometheus_sync_task()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(g_ob_prometheus_processor.do_prometheus_sync_task())) {
    LOG_WDIAG("fail to do prometheus sync task", K(ret));
  } else {
    ObAsyncCommonTask *cont = g_ob_prometheus_processor.get_prometheus_sync_cont();
    if (OB_LIKELY(cont)) {
      int64_t interval_us = ObProxyMonitorUtils::get_next_schedule_time(get_global_proxy_config().prometheus_sync_interval);
      cont->set_interval(interval_us);
    }
  }

  return ret;
}

void ObPrometheusProcessor::update_prometheus_sync_interval()
{
  ObAsyncCommonTask *cont = g_ob_prometheus_processor.get_prometheus_sync_cont();
  if (OB_LIKELY(cont)) {
    int64_t interval_us = ObProxyMonitorUtils::get_next_schedule_time(get_global_proxy_config().prometheus_sync_interval);
    cont->set_interval(interval_us);
  }
}

int ObPrometheusProcessor::get_or_create_exporter_metric(ObPrometheusFamilyHashTable::iterator &family_iter,
                                                         ObPrometheusMetricHashTable::iterator &metric_iter,
                                                         void *&exporter_metric)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(exporter_metric = metric_iter->get_exporter_metric())) {
    switch (family_iter->get_metric_type()) {
      case PROMETHEUS_TYPE_COUNTER:
        ret = ObProxyPrometheusConvert::create_counter_metric(family_iter->get_exporter_family(),
                                                              metric_iter->get_labels(),
                                                              exporter_metric);
        break;
      case PROMETHEUS_TYPE_GAUGE:
        ret = ObProxyPrometheusConvert::create_gauge_metric(family_iter->get_exporter_family(),
                                                            metric_iter->get_labels(),
                                                            exporter_metric);
        break;
      case PROMETHEUS_TYPE_HISTOGRAM:
      {
        ObPrometheusHistogram *histogram = (ObPrometheusHistogram *)metric_iter.value_;
        ret = ObProxyPrometheusConvert::create_histogram_metric(family_iter->get_exporter_family(),
                                                                metric_iter->get_labels(),
                                                                histogram->get_bucket_boundaries(),
                                                                exporter_metric);
        break;
      }
      default:
        break;
    }

    if (OB_SUCC(ret)) {
      metric_iter->set_exporter_metric(exporter_metric);
    } else {
      LOG_WDIAG("fail to get or create exporter metric", "metric_type", family_iter->get_metric_type(), K(ret));
    }
  }

  return ret;
}

int ObPrometheusProcessor::sync_to_exporter(ObPrometheusFamilyHashTable::iterator &family_iter,
                                            ObPrometheusMetricHashTable::iterator &metric_iter)
{
  int ret = OB_SUCCESS;
  void *exporter_metric = NULL;
  if (OB_FAIL(get_or_create_exporter_metric(family_iter, metric_iter, exporter_metric))) {
    LOG_WDIAG("fail to get or create exporter metric", K(ret));
  } else  {
    switch (family_iter->get_metric_type()) {
      case PROMETHEUS_TYPE_COUNTER:
      {
        ObPrometheusCounter *counter = (ObPrometheusGauge *)metric_iter.value_;
        ret = ObProxyPrometheusConvert::handle_counter(exporter_metric,
                                                       counter->atomic_get_and_reset_value());
        break;
      }
      case PROMETHEUS_TYPE_GAUGE:
      {
        ObPrometheusGauge *gauge = (ObPrometheusGauge *)metric_iter.value_;
        if (get_global_proxy_config().prometheus_cost_ms_unit && family_iter->get_name() == COST_TOTAL) {
          ret = ObProxyPrometheusConvert::handle_gauge(exporter_metric,
                                                       static_cast<double>(gauge->atomic_get_and_reset_value()) / 1000);
        } else {
          ret = ObProxyPrometheusConvert::handle_gauge(exporter_metric,
                                                       gauge->atomic_get_and_reset_value());
        }
        break;
      }
      case PROMETHEUS_TYPE_HISTOGRAM:
      {
        ObPrometheusHistogram *histogram = (ObPrometheusHistogram *)metric_iter.value_;
        DRWLock::RDLockGuard lock(histogram->get_lock());
        ret = ObProxyPrometheusConvert::handle_histogram(exporter_metric,
                                                         histogram->get_sum(),
                                                         histogram->get_bucket_counts());
        histogram->reset();
        break;
      }
      default:
        break;
    }

    if (OB_FAIL(ret)) {
      LOG_WDIAG("fail to sync to exporter", "metric_type", family_iter->get_metric_type());
    }
  }

  return ret;
}

int ObPrometheusProcessor::do_prometheus_sync_task()
{
  int ret = OB_SUCCESS;

  int64_t max_idle_period = get_global_proxy_config().monitor_item_max_idle_period / get_global_proxy_config().prometheus_sync_interval;
  max_idle_period = max_idle_period == 0 ? 1 : max_idle_period;
  bool need_expire_metric = need_expire_metric_;

  ObPrometheusFamilyHashTable::iterator family_iter = family_hash_.begin();
  ObPrometheusFamilyHashTable::iterator family_last = family_hash_.end();
  for (; family_iter != family_last; ++family_iter) {
    ObPrometheusMetricHashTable::iterator metric_iter = family_iter->get_metrics().begin();
    ObPrometheusMetricHashTable::iterator metric_last = family_iter->get_metrics().end();

    for (; metric_iter != metric_last; ) {
      if (metric_iter->is_active()) {
        sync_to_exporter(family_iter, metric_iter);
        metric_iter->reset_idle_period_count();
        ++metric_iter;
      } else if (metric_iter->is_allow_delete()
                 && metric_iter->inc_and_fetch_idle_period_count() > max_idle_period
                 && need_expire_metric) {
        LOG_DEBUG("will to clean metric", K(max_idle_period), "metric", *metric_iter.value_);
        ObPrometheusMetricHashTable::iterator tmp_iter = metric_iter;
        ++metric_iter;
        if (OB_FAIL(ObProxyPrometheusConvert::remove_metric(family_iter->get_exporter_family(),
                                                            tmp_iter->get_exporter_metric(),
                                                            family_iter->get_metric_type()))) {
          LOG_WDIAG("fail to remove exporter metric", K(ret));
        } else {
          tmp_iter->set_exporter_metric(NULL);
          if (OB_FAIL(family_iter->remove_metric(tmp_iter.value_))) {
            LOG_WDIAG("fail to remove metric", K(ret));
          } else {
            ATOMIC_AAF(&metric_num_, -1);
            need_expire_metric_ = false;
          }
        }
      } else {
        ++metric_iter;
      }
    }
  }

  return ret;
}

int ObPrometheusProcessor::handle_counter(const char *name_ptr, const char *help_ptr,
                                          ObVector<ObPrometheusLabel> &label_array,
                                          int64_t value)
{
  int ret = OB_SUCCESS;

  ObPrometheusFamily *family = NULL;
  ObPrometheusCounter *counter = NULL;
  void* args = NULL;

  if (OB_FAIL(get_or_create_family(name_ptr, help_ptr, PROMETHEUS_TYPE_COUNTER,
                                   default_constant_labels_, family))) {
    LOG_WDIAG("fail to get or create family", K(name_ptr), K(ret));
  } else if (OB_FAIL(get_or_create_metric(family, label_array, args, counter))) {
    if (OB_EXCEED_MEM_LIMIT == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WDIAG("fail to get or create metric", K(label_array), K(ret));
    }
  } else {
    counter->atomic_add(value);
    family->dec_ref();
    counter->dec_ref();
  }

  return ret;
}

int ObPrometheusProcessor::handle_gauge(const char *name_ptr, const char *help_ptr,
                                        ObVector<ObPrometheusLabel> &label_array,
                                        int64_t value, bool allow_delete)
{
  int ret = OB_SUCCESS;

  ObPrometheusFamily *family = NULL;
  ObPrometheusGauge *gauge = NULL;
  void* args = NULL;

  if (OB_FAIL(get_or_create_family(name_ptr, help_ptr, PROMETHEUS_TYPE_GAUGE,
                                   default_constant_labels_, family))) {
    LOG_WDIAG("fail to get or create family", K(name_ptr), K(ret));
  } else if (OB_FAIL(get_or_create_metric(family, label_array, args, gauge, allow_delete))) {
    if (OB_EXCEED_MEM_LIMIT == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WDIAG("fail to get or create metric", K(label_array), K(ret));
    }
  } else {
    gauge->atomic_add(value);
    family->dec_ref();
    gauge->dec_ref();
  }

  return ret;
}

int ObPrometheusProcessor::handle_histogram(const char *name_ptr, const char *help_ptr,
                                            ObVector<ObPrometheusLabel> &label_array,
                                            int64_t value, ObSortedVector<int64_t>& buckets)
{
  int ret = OB_SUCCESS;

  ObPrometheusFamily *family = NULL;
  ObPrometheusHistogram *histogram = NULL;

  if (OB_FAIL(get_or_create_family(name_ptr, help_ptr, PROMETHEUS_TYPE_HISTOGRAM,
                                   default_constant_labels_, family))) {
    LOG_WDIAG("fail to get or create family", K(name_ptr), K(ret));
  } else if (OB_FAIL(get_or_create_metric(family, label_array, buckets, histogram))) {
    if (OB_EXCEED_MEM_LIMIT == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WDIAG("fail to get or create metric", K(label_array), K(ret));
    }
  } else {
    histogram->atomic_add(value);
    family->dec_ref();
    histogram->dec_ref();
  }

  return ret;
}

template <typename TA, typename T>
int ObPrometheusProcessor::get_or_create_metric(ObPrometheusFamily *family,
                                                ObVector<ObPrometheusLabel> &label_array,
                                                TA& args, T *&metric, bool allow_delete)
{
  int ret = OB_SUCCESS;

  uint64_t hash = 0;
  ObPrometheusMetricHashKey key;
  key.labels_ = &label_array;

  for (int64_t i = 0; i < label_array.size() && OB_SUCC(ret); ++i) {
    ObPrometheusLabel &label = label_array.at(i);
    hash = label.hash(hash);
  }

  key.hash_ = hash;

  metric = NULL;
  if (OB_FAIL(family->get_metric(key, metric))) {
    if (OB_HASH_NOT_EXIST == ret) {
      bool is_new = false;
      if (ATOMIC_AAF(&metric_num_, 1) <= get_global_proxy_config().monitor_item_limit) {
        if (OB_FAIL(family->create_metric(key, args, metric, is_new, allow_delete))) {
          LOG_WDIAG("create or get gobal metric failed", K(key), K(ret));
        }

        if (OB_FAIL(ret) || !is_new) {
          ATOMIC_AAF(&metric_num_, -1);
        } else {
          LOG_DEBUG("create metric success", K(key), K_(metric_num));
        }
      } else {
        ATOMIC_AAF(&metric_num_, -1);
        need_expire_metric_ = true;
        ret = OB_EXCEED_MEM_LIMIT;
        LOG_WDIAG("metric num reach limit, will discard and expire metric", K_(metric_num));
      }
    } else {
      LOG_WDIAG("get global metric from buildhash failed", K(key), K(ret));
    }
  }

  return ret;
}

int ObPrometheusProcessor::get_family(const ObString &name,
                                      ObPrometheusFamily *&family)
{
  int ret = OB_SUCCESS;

  DRWLock::RDLockGuard lock(lock_);
  if (OB_FAIL(family_hash_.get_refactored(name, family))) {
  } else {
    family->inc_ref();
  }

  return ret;
}

int ObPrometheusProcessor::create_family(const ObString &name, const ObString &help,
                                         const ObPrometheusMetricType metric_type,
                                         const ObVector<ObPrometheusLabel> &label_array,
                                         ObPrometheusFamily *&family)
{
  int ret = OB_SUCCESS;

  DRWLock::WRLockGuard lock(lock_);
  if (OB_FAIL(family_hash_.get_refactored(name, family))) {
    if (OB_HASH_NOT_EXIST == ret) {
      void *exporter_family = NULL;
      if (OB_FAIL(ObProxyPrometheusConvert::get_or_create_exporter_family(name, help, label_array, metric_type, exporter_family))) {
        LOG_WDIAG("fail to get or create exporter family", K(name), K(metric_type), K(ret));
      } else if (OB_ISNULL(family = op_alloc_args(ObPrometheusFamily, name, help, metric_type, exporter_family))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to allocate memory", K(ret));
      } else {
        family->inc_ref();
        if (OB_FAIL(family_hash_.unique_set(family))) {
          LOG_WDIAG("fail to set family into hashmap", KPC(family), K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        family->inc_ref();
      } else if (OB_NOT_NULL(family)) {
        family->dec_ref();
        family = NULL;
      }
    }
  } else {
    family->inc_ref();
  }

  return ret;
}

int ObPrometheusProcessor::get_or_create_family(const ObString &name, const ObString &help,
                                                const ObPrometheusMetricType metric_type,
                                                const ObVector<ObPrometheusLabel> &label_array,
                                                ObPrometheusFamily *&family)
{
  int ret = OB_SUCCESS;

  family = NULL;
  if (OB_FAIL(get_family(name, family))) {
    if (OB_HASH_NOT_EXIST == ret) {
      if (OB_FAIL(create_family(name, help, metric_type, label_array, family))) {
        LOG_WDIAG("fail to create family", K(name), K(ret));
      }
    } else {
      LOG_WDIAG("fail to get family", K(name), K(ret));
    }
  }

  return ret;
}

void ObPrometheusProcessor::destroy_exposer()
{
  destory_prometheus_exposer();
}

int ObPrometheusProcessor::create_exposer()
{
  return create_prometheus_exposer();
}

int ObPrometheusProcessor::init()
{
  int ret = OB_SUCCESS;
  ObAddr local_addr;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WDIAG("init twice", K(ret));
  } else if (OB_FAIL(ObProxyTableProcessorUtils::get_proxy_local_addr(local_addr))) {
    LOG_WDIAG("fail to get proxy local addr", K(local_addr), K(ret));
  } else if (OB_UNLIKELY(!local_addr.ip_to_string(proxy_ip_, static_cast<int32_t>(sizeof(proxy_ip_))))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to covert ip to string", K(local_addr), K(ret));
  } else if (OB_FAIL(init_prometheus(static_cast<int32_t>(get_global_proxy_config().prometheus_listen_port.get())))) {
    LOG_WDIAG("fail to init prometheus", "prometheus_listen_port", get_global_proxy_config().prometheus_listen_port.get(), K(ret));
  } else  {
    uint64_t offset = 0;
    const char *package = PACKAGE_STRING;
    const char *version = build_version();

    MEMCPY(version_ + offset, package, strlen(package));
    offset += strlen(package);
    MEMCPY(version_ + offset, "_", 1);
    offset ++;

    static const size_t minimumVersionLength = 17;
    size_t version_len = strlen(version);
    if (version_len > minimumVersionLength) {
        version_len = minimumVersionLength;
    }
    MEMCPY(version_ + offset, version, version_len);

    ObProxyPrometheusUtils::build_label(default_constant_labels_, "ip", proxy_ip_, false);
    ObProxyPrometheusUtils::build_label(default_constant_labels_, "namespace", "ODP", false);
    ObProxyPrometheusUtils::build_label(default_constant_labels_, "odpVersion", version_, false);
    is_inited_ = true;
  }

  return ret;
}

ObPrometheusProcessor::~ObPrometheusProcessor()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObAsyncCommonTask::destroy_repeat_task(prometheus_sync_cont_))) {
    LOG_WDIAG("fail to destroy prometheus syc task", K(ret));
  } else {
    prometheus_sync_cont_ = NULL;
  }

  ObPrometheusFamilyHashTable::iterator family_iter = family_hash_.begin();
  ObPrometheusFamilyHashTable::iterator family_last = family_hash_.end();
  for (; family_iter != family_last; ) {
    ObPrometheusFamilyHashTable::iterator tmp = family_iter;
    ++family_iter;
    family_hash_.remove(ObPrometheusFamilyHashing::key(tmp.value_));
    tmp->dec_ref();
  }
  family_hash_.reset();

  need_expire_metric_ = false;
  metric_num_ = 0;

  default_constant_labels_.clear();
  is_inited_ = false;
}

ObPrometheusProcessor::ObPrometheusProcessor()
  : is_inited_(false), prometheus_sync_cont_(NULL),
    need_expire_metric_(false), metric_num_(0)
{
  MEMSET(version_, 0, sizeof(version_));
  MEMSET(proxy_ip_, 0, sizeof(proxy_ip_));
}

} // end of namespace prometheus
} // end of namespace obproxy
} // end of namespace oceanbase
