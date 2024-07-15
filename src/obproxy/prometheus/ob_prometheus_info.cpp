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

#include "prometheus/ob_prometheus_info.h"
#include "prometheus/ob_prometheus_utils.h"
#include "iocore/eventsystem/ob_buf_allocator.h"

namespace oceanbase
{
namespace obproxy
{
namespace prometheus
{
using namespace oceanbase::common;

/* --------ObPrometheusMetric----------- */
ObPrometheusMetric::~ObPrometheusMetric()
{
  hash_ = 0;
  exporter_metric_ = NULL;

  if (buf_ != NULL) {
    op_fixed_mem_free(buf_, buf_len_);
    buf_ = NULL;
    buf_len_ = 0;
  }
  labels_.reset();
  idle_period_count_ = 0;
}

int ObPrometheusMetric::init(const ObPrometheusMetricHashKey &key, bool allow_delete)
{
  int ret = OB_SUCCESS;

  uint32_t buf_size = 0;
  unsigned char *ptr = NULL;

  allow_delete_ = allow_delete;
  hash_ = key.hash_;
  if (OB_FAIL(ObProxyPrometheusUtils::calc_buf_size(key.labels_, buf_size))) {
    LOG_WDIAG("fail to calc buf size", KPC(key.labels_));
  } else if (buf_size > 0) {
    if (OB_ISNULL(ptr = (unsigned char *)op_fixed_mem_alloc(buf_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("allocate memory failed", "size", buf_size, K(ret));
    } else {
      buf_ = ptr;
      buf_len_ = buf_size;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(0 == buf_size)) {
      LOG_DEBUG("empty lables, no need copy");
    } else if (OB_FAIL(ObProxyPrometheusUtils::copy_label_hash(key.labels_, labels_, buf_, buf_len_))) {
      LOG_WDIAG("fail to copy label hash", K(ret));
    }
  }

  return ret;
}

/* --------ObPrometheusGauge----------- */
int64_t ObPrometheusGauge::atomic_get_and_reset_value()
{
  int64_t old_value = -1;
  old_value = ATOMIC_LOAD(&value_);
  const int64_t new_value = 0;
  while (false == ATOMIC_BCAS(&value_, old_value, new_value)) {
    old_value = ATOMIC_LOAD(&value_);
  }
  return old_value;
}

/* --------ObPrometheusHistogram----------- */
ObPrometheusHistogram::ObPrometheusHistogram(const ObSortedVector<int64_t>& buckets)
  : bucket_boundaries_(buckets), bucket_counts_(buckets.size() + 1), sum_()
{
  bucket_boundaries_.sort();
  for (int64_t i = 0; i< buckets.size() + 1; i++) {
    bucket_counts_.push_back(0);
  }
}

void ObPrometheusHistogram::reset()
{
  sum_ = 0;
  for (int64_t i = 0; i< bucket_counts_.size(); i++) {
    bucket_counts_[i] = 0;
  }
}

void ObPrometheusHistogram::atomic_add(const int64_t value)
{
  int32_t bucket_index = static_cast<int32_t>(bucket_boundaries_.lower_bound(value) - bucket_boundaries_.begin());
  if (bucket_index >= bucket_counts_.size()) {
    LOG_WDIAG("bucket index is invalid", K(value), K(bucket_index), "bucket_counts size", bucket_counts_.size());
  } else {
    DRWLock::RDLockGuard lock(lock_);
    ATOMIC_AAF(&sum_, value);
    ATOMIC_AAF(&bucket_counts_[bucket_index], 1);
  }
}

//------------------- ObPrometheusFamily ------------------
ObPrometheusFamily::~ObPrometheusFamily()
{
  name_.reset();
  help_.reset();
  metric_type_ = PROMETHEUS_TYPE_COUNT;
  exporter_family_ = NULL;

  ObPrometheusMetricHashTable::iterator metric_iter = metrics_.begin();
  ObPrometheusMetricHashTable::iterator metric_last = metrics_.end();
  for (; metric_iter != metric_last; ) {
    ObPrometheusMetricHashTable::iterator tmp = metric_iter;
    ++metric_iter;
    metrics_.remove(ObPrometheusMetricHashing::key(tmp.value_));
    tmp->dec_ref();
  }
  metrics_.reset();
}

int ObPrometheusFamily::remove_metric(ObPrometheusMetric *metric)
{
  int ret = OB_SUCCESS;

  DRWLock::WRLockGuard lock(lock_);

  if (OB_FAIL(remove_metric_without_lock(metric))) {
    LOG_WDIAG("remove metric without lock failed", K(ret));
  }

  return ret;
}

int ObPrometheusFamily::remove_metric_without_lock(ObPrometheusMetric *metric)
{
  int ret = OB_SUCCESS;

  ObPrometheusMetricHashKey key = ObPrometheusMetricHashing::key(metric);
  if (OB_FAIL(metrics_.erase_refactored(key))) {
    LOG_WDIAG("remove remtric failed", K(ret));
  } else {
    metric->dec_ref();
    metric = NULL;
  }

  return ret;
}

} // end of namespace prometheus
} // end of namespace obproxy
} // end of namespace oceanbase
