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

#ifndef OBPROXY_PROMETHEUS_UTILS_H
#define OBPROXY_PROMETHEUS_UTILS_H

#include "opsql/parser/ob_proxy_parse_result.h"
#include "lib/container/ob_vector.h"
#include "prometheus/ob_prometheus_info.h"

namespace oceanbase
{
namespace obproxy
{
namespace prometheus
{

#define TRANSACTION_TOTAL "odp_transaction_total"
#define TRANSACTION_TOTAL_HELP "The num of user transaction"
#define REQUEST_TOTAL "odp_sql_request_total"
#define REQUEST_TOTAL_HELP "The num of user request"
#define COST_TOTAL "odp_sql_cost_total"
#define COST_TOTAL_HELP "user cost total"
#define CURRENT_SESSION "odp_current_session"
#define CURRENT_SESSION_HELP "The num of current session"

#define REQUEST_BYTE "odp_request_byte"
#define REQUEST_BYTE_HELP "The num of request byte"

#define ENTRY_TOTAL "odp_entry_total"
#define ENTRY_TOTAL_HELP "The num of entry lookup"

#define LABEL_LOGIC_TENANT "logicTenant"
#define LABEL_LOGIC_SCHEMA "logicSchema"
#define LABEL_CLUSTER "cluster"
#define LABEL_TENANT "tenant"
#define LABEL_SCHEMA "schema"
#define LABEL_SQL_TYPE "sqlType"
#define LABEL_SQL_RESULT "sqlResult"
#define LABEL_SQL_SLOW "slowQuery"
#define LABEL_TIME_TYPE "timeType"
#define LABEL_SESSION_TYPE "sessionType"
#define LABEL_SESSION_CLIENT "client"
#define LABEL_SESSION_SERVER "server"
#define LABEL_TRANS_TYPE "transType"
#define LABEL_TRANS_REQUEST "request"
#define LABEL_TRANS_RESPONSE "response"
#define LABEL_ENTRY_TYPE "entryType"
#define LABEL_ROUTE_HIT "routeHit"
#define LABEL_ROUTE_RESULT "routeResult"
#define LABEL_FAIL "fail"
#define LABEL_SUCC "success"
#define LABEL_FALSE "false"
#define LABEL_TRUE "true"

class ObProxyPrometheusUtils
{
public:
  ObProxyPrometheusUtils() {};
  ~ObProxyPrometheusUtils() {};

  static const char* get_metric_lable(ObPrometheusMetrics metric);
  static const char* get_type_lable(ObPrometheusEntryType type);

  static int calc_buf_size(common::ObVector<ObPrometheusLabel> *labels, uint32_t &buf_size);
  static int copy_label_hash(common::ObVector<ObPrometheusLabel> *labels,
                             common::ObVector<ObPrometheusLabel> &dst_labels,
                             unsigned char *buf, uint32_t buf_len);
  static void build_label(common::ObVector<ObPrometheusLabel> &labels,
                          const char *key_ptr, const char *value_ptr,
                          bool is_value_need_alloc = true);
  static void build_label(common::ObVector<ObPrometheusLabel> &labels,
                          const char *key_ptr, const common::ObString &value,
                          bool is_value_need_alloc = true);
  static void build_label(common::ObVector<ObPrometheusLabel> &labels,
                          const common::ObString &key, const common::ObString &value,
                          bool is_value_need_alloc = true);
  static common::ObVector<ObPrometheusLabel> &get_thread_label_vector();
};

inline void ObProxyPrometheusUtils::build_label(common::ObVector<ObPrometheusLabel> &labels,
                                                const char *key_ptr, const char *value_ptr,
                                                bool is_value_need_alloc)
{
  common::ObString key;
  common::ObString value;

  key.assign_ptr(key_ptr, static_cast<common::ObString::obstr_size_t>(strlen(key_ptr)));
  value.assign_ptr(value_ptr, static_cast<common::ObString::obstr_size_t>(strlen(value_ptr)));

  build_label(labels, key, value, is_value_need_alloc);
}

inline void ObProxyPrometheusUtils::build_label(common::ObVector<ObPrometheusLabel> &labels,
                                                const char *key_ptr, const common::ObString &value,
                                                bool is_value_need_alloc)
{
  common::ObString key;
  key.assign_ptr(key_ptr, static_cast<common::ObString::obstr_size_t>(strlen(key_ptr)));
  build_label(labels, key, value, is_value_need_alloc);
}

inline void ObProxyPrometheusUtils::build_label(common::ObVector<ObPrometheusLabel> &labels,
                                                const common::ObString &key, const common::ObString &value,
                                                bool is_value_need_alloc)
{
  ObPrometheusLabel label(key, value, is_value_need_alloc);
  labels.push_back(label);
}


} // end of namespace prometheus
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_PROMETHEUS_UTILS_H
