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
  switch(metric) {
  case PROMETHEUS_PREPARE_SEND_REQUEST_TIME:
    return "prepare";
  case PROMETHEUS_SERVER_PROCESS_REQUEST_TIME:
    return "server";
  case PROMETHEUS_REQUEST_TOTAL_TIME:
    return "total";
  default:
    return "UNKNOWN";
  }
}

const char* ObProxyPrometheusUtils::get_type_lable(ObPrometheusEntryType type)
{
  switch(type) {
  case TBALE_ENTRY:
    return "table_entry";
  case PARTITION_ENTRY:
    return "partition_entry";
  case ROUTE_ENTRY:
    return "route_entry";
  default:
    return "UNKNOWN";
  }
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
  static __thread ObVector<ObPrometheusLabel> prometheus_thread_labels(10);
  return prometheus_thread_labels;
}

} // end of namespace prometheus
} // end of namespace obproxy
} // end of namespace oceanbase
