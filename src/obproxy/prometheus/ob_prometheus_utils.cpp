/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PROXY

#include "prometheus/ob_prometheus_utils.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace obproxy
{
namespace prometheus
{
using namespace oceanbase::common;

const char* ObProxyPrometheusUtils::get_metric_lable(ObPrometheusMetrics metric)
{
  const char* str_ret = NULL;

  switch(metric) {
  case PROMETHEUS_PREPARE_SEND_REQUEST_TIME:
    str_ret = "prepare";
    break;
  case PROMETHEUS_SERVER_PROCESS_REQUEST_TIME:
    str_ret = "server";
    break;
  case PROMETHEUS_REQUEST_TOTAL_TIME:
    str_ret = "total";
    break;
  default:
    str_ret = "UNKNOWN";
  }

  return str_ret;
}

const char* ObProxyPrometheusUtils::get_type_lable(ObPrometheusEntryType type)
{
  const char* str_ret = NULL;

  switch(type) {
  case TBALE_ENTRY:
    str_ret = "table_entry";
    break;
  case PARTITION_INFO:
    str_ret = "partition_info";
    break;
  case PARTITION_ENTRY:
    str_ret = "partition_entry";
    break;
  case ROUTE_ENTRY:
    str_ret = "route_entry";
    break;
  default:
    str_ret = "UNKNOWN";
  }

  return str_ret;
}

const char* ObProxyPrometheusUtils::get_mem_type_lable(ObPrometheusMetrics metric)
{
  const char* str_ret = NULL;

  switch(metric) {
    case PROMETHEUS_MEMORY_HOLD:
      str_ret = "HOLD";
      break;
    case PROMETHEUS_MEMORY_USED:
      str_ret = "USED";
      break;
    default:
      str_ret = "UNKNOWN";
  }

  return str_ret;
}

const char* ObProxyPrometheusUtils::get_ps_type_lable(ObPrometheusMetrics metric)
{
  const char* str_ret = NULL;

  switch(metric) {
    case PROMETHEUS_PS_COUNT:
      str_ret = "PS_COUNT";
      break;
    case PROMETHEUS_PS_MEMORY_USED:
      str_ret = "PS_MEM";
      break;
    default:
      str_ret = "UNKNOWN";
  }

  return str_ret;
}

int ObProxyPrometheusUtils::calc_buf_size(ObVector<ObPrometheusLabel> *labels, uint32_t &buf_size)
{
  int ret = OB_SUCCESS;

  buf_size = 0;
  for (int i = 0; i < labels->size(); i++) {
    ObPrometheusLabel &label = labels->at(i);
    if (label.is_value_need_alloc()) {
      buf_size += label.get_value().length();
    }
  }

  return ret;
}

int ObProxyPrometheusUtils::copy_label_hash(ObVector<ObPrometheusLabel> *labels,
                                            ObVector<ObPrometheusLabel> &dst_labels,
                                            unsigned char *buf, uint32_t buf_len)
{
  int ret = OB_SUCCESS;

  uint64_t offset = 0;
  for (int i = 0; i < labels->size() && OB_SUCC(ret); i++) {
    ObPrometheusLabel &label = labels->at(i);

    ObPrometheusLabel new_label;
    new_label.set_key(label.get_key());

    if (label.is_value_need_alloc()) {
      if (buf != NULL && offset + label.get_value().length() <= buf_len) {
        ObString value;
        MEMCPY(buf + offset, label.get_value().ptr(), label.get_value().length());
        value.assign_ptr(reinterpret_cast<char *>(buf + offset), label.get_value().length());
        offset += label.get_value().length();
        new_label.set_value(value);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("copy label meet unexpected error", K(offset),
                 "value len", label.get_value().length(), K(buf_len));
      }
    } else {
      new_label.set_value(label.get_value());
    }

    if (OB_FAIL(dst_labels.push_back(new_label))) {
      LOG_WDIAG("put label into metric failed", K(new_label), K(ret));
    }
  }

  return ret;
}

ObVector<ObPrometheusLabel>& ObProxyPrometheusUtils::get_thread_label_vector()
{
  static thread_local ObVector<ObPrometheusLabel> prometheus_thread_labels(12);
  return prometheus_thread_labels;
}

} // end of namespace prometheus
} // end of namespace obproxy
} // end of namespace oceanbase
