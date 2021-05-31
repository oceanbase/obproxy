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

#ifndef OBPROXY_PROMETHEUS_INFO_H
#define OBPROXY_PROMETHEUS_INFO_H

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/hash/ob_build_in_hashmap.h"
#include "lib/lock/ob_drw_lock.h"
#include "lib/ptr/ob_ptr.h"
#include "lib/objectpool/ob_concurrency_objpool.h"

namespace oceanbase
{
namespace obproxy
{
namespace prometheus
{

static const int64_t LABEL_HASH_BUCKET_SIZE = 8;
static const int64_t METRIC_HASH_BUCKET_SIZE = 32;
static const int64_t FAMILY_HASH_BUCKET_SIZE = 8;

enum ObPrometheusMetricType
{
  PROMETHEUS_TYPE_COUNTER = 0,
  PROMETHEUS_TYPE_GAUGE,
  PROMETHEUS_TYPE_HISTOGRAM,
  PROMETHEUS_TYPE_COUNT
};

enum ObPrometheusMetrics
{
  PROMETHEUS_TRANSACTION_COUNT = 0,
  PROMETHEUS_REQUEST_COUNT,

  PROMETHEUS_PREPARE_SEND_REQUEST_TIME,
  PROMETHEUS_SERVER_PROCESS_REQUEST_TIME,
  PROMETHEUS_REQUEST_TOTAL_TIME,

  PROMETHEUS_CURRENT_SESSION,

  PROMETHEUS_ENTRY_LOOKUP_COUNT,
  PROMETHEUS_REQUEST_BYTE,
  PROMETHEUS_METRIC_COUNT
};

enum ObPrometheusEntryType
{
  TBALE_ENTRY = 0,
  PARTITION_INFO,
  PARTITION_ENTRY,
  ROUTE_ENTRY
};

class ObPrometheusLabel
{
public:
  ObPrometheusLabel(const common::ObString &key,
                    const common::ObString &value,
                    const bool is_value_need_alloc)
    : key_(key), value_(value), is_value_need_alloc_(is_value_need_alloc) {}
  ObPrometheusLabel() : key_(), value_(), is_value_need_alloc_(true) {}

  inline bool operator!=(const ObPrometheusLabel &label) const
  {
    return key_ != label.key_ || value_ != label.value_;
  }

  inline uint64_t hash(uint64_t seed = 0) const
  {
    seed = key_.hash(seed);
    seed = value_.hash(seed);
    return seed;
  };

  const common::ObString &get_key() const { return key_; }
  void set_key(const common::ObString &key) { key_ = key; }

  const common::ObString &get_value() const { return value_; }
  void set_value(const common::ObString &value) { value_ = value; }

  bool is_value_need_alloc() { return is_value_need_alloc_; }
  void set_value_need_alloc(const bool is_value_need_alloc) { is_value_need_alloc_ = is_value_need_alloc; }

  TO_STRING_KV(K_(key), K_(value), K_(is_value_need_alloc));

private:
  common::ObString key_;
  common::ObString value_;
  bool is_value_need_alloc_;
};

/* --------ObPrometheusMetric----------- */
class ObPrometheusMetricHashKey;

class ObPrometheusMetric : public common::ObSharedRefCount
{
public:
  ObPrometheusMetric()
    : hash_(0), exporter_metric_(NULL),
      buf_(NULL), buf_len_(0),
      allow_delete_(true),
      idle_period_count_(0) {}
  ~ObPrometheusMetric();

  int init(const ObPrometheusMetricHashKey &key, bool allow_delete);
  virtual void free() { op_free(this); }

  virtual bool is_active() const { return false; }

  bool is_allow_delete() { return allow_delete_; }
  uint64_t get_hash() { return hash_; }
  void set_exporter_metric(void *exporter_metric) { exporter_metric_ = exporter_metric; }
  void *get_exporter_metric() { return exporter_metric_; }
  common::ObVector<ObPrometheusLabel> &get_labels() { return labels_; }

  int64_t inc_and_fetch_idle_period_count() { return ATOMIC_AAF(&idle_period_count_, 1); }
  void reset_idle_period_count() { idle_period_count_ = 0; }

  TO_STRING_KV(K_(hash), K_(idle_period_count), K_(labels));

public:
  LINK(ObPrometheusMetric, metric_link_);

private:
  uint64_t hash_;

  void *exporter_metric_;

  common::ObVector<ObPrometheusLabel> labels_;
  unsigned char *buf_;
  uint32_t buf_len_;

  bool allow_delete_;
  int64_t idle_period_count_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObPrometheusMetric);
};

class ObPrometheusGauge : public ObPrometheusMetric
{
public:
  ObPrometheusGauge(const void *args) : value_(0) { UNUSED(args); }
  virtual void free() { op_free(this); }

  int64_t atomic_get_and_reset_value();
  void atomic_add(const int64_t value) { ATOMIC_AAF(&value_, value); }
  virtual bool is_active() const { return ATOMIC_LOAD(&value_); }

  int64_t get_value() { return value_; }

private:
  int64_t value_;
};

typedef ObPrometheusGauge ObPrometheusCounter;

class ObPrometheusHistogram : public ObPrometheusMetric
{
public:
  ObPrometheusHistogram(const common::ObSortedVector<int64_t>& buckets);
  virtual void free() { op_free(this); }

  void reset();
  void atomic_add(const int64_t value);
  virtual bool is_active() const { return ATOMIC_LOAD(&sum_); }

  common::DRWLock &get_lock() { return lock_; }
  const common::ObSortedVector<int64_t> &get_bucket_boundaries() { return bucket_boundaries_; }
  const common::ObVector<int64_t> &get_bucket_counts() { return bucket_counts_; }
  int64_t get_sum() { return sum_; }

private:
  common::DRWLock lock_;
  common::ObSortedVector<int64_t> bucket_boundaries_;
  common::ObVector<int64_t> bucket_counts_;
  int64_t sum_;
};

class ObPrometheusMetricHashKey
{
public:
  TO_STRING_KV(K_(hash), KPC_(labels));

public:
  uint64_t hash_;
  common::ObVector<ObPrometheusLabel> *labels_;
};

class ObPrometheusMetricHashing
{
public:
  typedef ObPrometheusMetricHashKey Key;
  typedef ObPrometheusMetric Value;
  typedef ObDLList(ObPrometheusMetric, metric_link_) ListHead;

  static uint64_t hash(Key key) { return key.hash_; }
  static Key key(Value *value)
  {
    ObPrometheusMetricHashKey key;
    key.hash_ = value->get_hash();
    key.labels_ = &value->get_labels();
    return key;
  }

  static bool equal(Key lhs, Key rhs)
  {
    bool is_equal = true;
    if (lhs.hash_ == rhs.hash_ && lhs.labels_->size() == rhs.labels_->size()) {
      for (int i = 0; i< lhs.labels_->size(); i++) {
        if (lhs.labels_->at(i) != rhs.labels_->at(i)) {
          is_equal = false;
          break;
        }
      }
    } else {
      is_equal = false;
    }

    return is_equal;
  }
};

typedef common::hash::ObBuildInHashMap<ObPrometheusMetricHashing, METRIC_HASH_BUCKET_SIZE> ObPrometheusMetricHashTable;

/* --------ObPrometheusFamily----------- */
class ObPrometheusFamily : public common::ObSharedRefCount
{
public:
  ObPrometheusFamily(const common::ObString &name, const common::ObString &help,
                     const ObPrometheusMetricType metric_type, void *exporter_family)
    : name_(name), help_(help), metric_type_(metric_type), exporter_family_(exporter_family){}
  ~ObPrometheusFamily();

  virtual void free() { op_free(this); }

  template <typename T>
  int get_metric(ObPrometheusMetricHashKey &key, T *&metric);

  template <typename TA, typename T>
  int create_metric(const ObPrometheusMetricHashKey &key, TA &args, T *&metric,
                    bool &is_new, bool allow_delete = true);

  int remove_metric(ObPrometheusMetric *metric);

  template <typename TA, typename T>
  int get_or_create_metric(const common::ObVector<ObPrometheusLabel> &label_array,
                           TA& args, T *&metric, bool allow_delete = true);

  const common::ObString &get_name() { return name_; }
  const common::ObString &get_help() { return help_; }
  ObPrometheusMetricType get_metric_type() { return metric_type_; }
  void *get_exporter_family() { return exporter_family_; }
  ObPrometheusMetricHashTable &get_metrics() { return metrics_; }

  TO_STRING_KV(K_(name), K_(help), K_(metric_type));

public:
  common::DRWLock lock_;
  LINK(ObPrometheusFamily, family_link_);

private:
  common::ObString name_;
  common::ObString help_;
  ObPrometheusMetricType metric_type_;
  void *exporter_family_;

  ObPrometheusMetricHashTable metrics_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPrometheusFamily);
};

// Interface class for IP map.
struct ObPrometheusFamilyHashing
{
  typedef const common::ObString &Key;
  typedef ObPrometheusFamily Value;
  typedef ObDLList(ObPrometheusFamily, family_link_) ListHead;

  static uint64_t hash(Key key) { return key.hash(); }
  static Key key(Value *value) { return value->get_name(); }
  static bool equal(Key lhs, Key rhs) { return lhs == rhs; }
};

typedef common::hash::ObBuildInHashMap<ObPrometheusFamilyHashing, FAMILY_HASH_BUCKET_SIZE> ObPrometheusFamilyHashTable;

template <typename T>
int ObPrometheusFamily::get_metric(ObPrometheusMetricHashKey &key, T *&metric)
{
  int ret = common::OB_SUCCESS;

  common::DRWLock::RDLockGuard lock(lock_);
  ObPrometheusMetric *tmp_metric = NULL;
  if (OB_FAIL(metrics_.get_refactored(key, tmp_metric))) {
  } else {
    metric = (T *)tmp_metric;
    metric->inc_ref();
  }

  return ret;
}

template <typename TA, typename T>
int ObPrometheusFamily::create_metric(const ObPrometheusMetricHashKey &key, TA &args, T *&metric,
                                      bool &is_new, bool allow_delete)
{
  int ret = common::OB_SUCCESS;

  common::DRWLock::WRLockGuard lock(lock_);
  ObPrometheusMetric *tmp_metric = NULL;
  if (OB_FAIL(metrics_.get_refactored(key, tmp_metric))) {
    if (common::OB_HASH_NOT_EXIST == ret) {
      if (OB_ISNULL(metric = op_alloc_args(T, args))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        PROXY_LOG(WARN, "fail to allocate memory", K(ret));
      } else {
        is_new = true;
        metric->inc_ref();
        if (OB_FAIL(metric->init(key, allow_delete))) {
          PROXY_LOG(WARN, "fail to init metric", K(key), K(ret));
        } else if (OB_FAIL(metrics_.unique_set(metric))) {
          PROXY_LOG(WARN, "fail to set metric into hashmap", KPC(metric), K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        metric->inc_ref();
      } else if (OB_NOT_NULL(metric)) {
        metric->dec_ref();
        metric = NULL;
      }
    }
  } else {
    metric = (T *)tmp_metric;
    metric->inc_ref();
  }

  return ret;
}

template <typename TA, typename T>
int ObPrometheusFamily::get_or_create_metric(const common::ObVector<ObPrometheusLabel> &label_array,
                                             TA& args, T *&metric, bool allow_delete)
{
  int ret = common::OB_SUCCESS;

  metric = NULL;
  uint64_t hash = 0;
  ObPrometheusMetricHashKey key;
  key.labels_ = &label_array;

  for (int64_t i = 0; i < label_array.size() && OB_SUCC(ret); ++i) {
    ObPrometheusLabel &label = label_array.at(i);
    hash = label.hash(hash);
  }

  key.hash_ = hash;

  metric = NULL;
  if (OB_FAIL(get_metric(key, metric))) {
    if (common::OB_HASH_NOT_EXIST == ret) {
      if (OB_FAIL(create_metric(key, args, metric))) {
        PROXY_LOG(WARN, "fail to create metric", K(key), K(ret));
      }
    } else {
      PROXY_LOG(WARN, "fail to get metric", K(key), K(ret));
    }
  }

  return ret;
}

} // end of namespace prometheus
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_PROMETHEUS_INFO_H
