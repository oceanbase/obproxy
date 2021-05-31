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

#ifndef OBPROXY_XFLUSH_DEF_H
#define OBPROXY_XFLUSH_DEF_H

namespace oceanbase
{
namespace obproxy
{

#define OBPROXY_XF_LOG(args...)                                                               \
  do {                                                                                        \
    const ObHotUpgraderInfo &hu_info = get_global_hot_upgrade_info();                         \
    const bool is_in_single_service = hu_info.is_in_single_service();                         \
    if (OB_LIKELY(is_in_single_service) || (!is_in_single_service && hu_info.is_parent())) {  \
      OBPROXY_XFLUSH_LOG(args);                                                               \
    }                                                                                         \
  } while(0)

#define _OBPROXY_XF_LOG(args...)                                                              \
  do {                                                                                        \
    const ObHotUpgraderInfo &hu_info = get_global_hot_upgrade_info();                         \
    const bool is_in_single_service = hu_info.is_in_single_service();                         \
    if (OB_LIKELY(is_in_single_service) || (!is_in_single_service && hu_info.is_parent())) {  \
      _OBPROXY_XFLUSH_LOG(args);                                                              \
    }                                                                                         \
  } while(0)

inline const char *get_xflush_error_name(const int error)
{
  const char *ret = NULL;
  switch (error) {
    case 4030:
      ret = "OB_TENANT_OUT_OF_MEM";
      break;

    case 4012:
      ret = "OB_TIMEOUT";
      break;

    case 4654:
      ret = "OB_LOCATION_LEADER_NOT_EXIST";
      break;

    case 6002:
      ret = "OB_TRANS_ROLLBACKED";
      break;

    case 6211:
      ret = "OB_TRANS_KILLED";
      break;

    case 6214:
      ret = "OB_PARTITION_IS_FROZEN";
      break;

    case 6224:
      ret = "OB_TRANS_NEED_ROLLBACK";
      break;

    default:
      ret = "OB_UNCOMMON_ERROR";
      break;
  }
  return ret;
}

// xflush log head
// SQL
#define XFH_SQL_SLOW_TRX        "SQL_SLOW_TRX"
#define XFH_SQL_SLOW_QUERY      "SQL_SLOW_QUERY"
#define XFH_SQL_SLOW_PROCESS    "SQL_SLOW_PROCESS"
#define XFH_SQL_NOT_SUPPORT     "SQL_NOT_SUPPORT"
#define XFH_SQL_ERROR_RESP      "SQL_ERROR_RESP"
#define XFH_SQL_FLOW_CTL        "SQL_FLOW_CTL"
#define XFH_SQL_PARTITION_MISS  "SQL_PARTITION_MISS"

// CONNECTION
#define XFH_CONNECTION_CLIENT_ABORT      "CONNECTION_CLIENT_ABORT"
#define XFH_CONNECTION_SERVER_ABORT      "CONNECTION_SERVER_ABORT"
#define XFH_CONNECTION_ACTIVE_TIMEOUT    "CONNECTION_ACTIVE_TIMEOUT"
#define XFH_CONNECTION_INACTIVE_TIMEOUT  "CONNECTION_INACTIVE_TIMEOUT"
#define XFH_CONNECTION_ERROR             "CONNECTION_ERROR"
#define XFH_CONNECTION_CLOSED            "CONNECTION_CLOSED"

// STATISTICAL
#define XFH_NET_STATE               "NET_STATE"
#define XFH_PROCESSOR_STATE         "PROCESSOR_STATE"
#define XFH_CONGESTION_STATE        "CONGESTION_STATE"
#define XFH_MYSQL_STATE             "MYSQL_STATE"
#define XFH_API_STATE               "API_STATE"
#define XFH_CLUSTER_RESOURCE_STATE  "CLUSTER_RESOURCE_STATE"
#define XFH_LOCK_STATE              "LOCK_STATE"
#define XFH_WARNING_STATE           "WARNING_STATE"

} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_XFLUSH_DEF_H
