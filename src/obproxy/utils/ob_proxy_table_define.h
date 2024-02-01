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

#ifndef OBPROXY_TABLE_DEFINE_H
#define OBPROXY_TABLE_DEFINE_H
#include "utils/ob_proxy_lib.h"

namespace oceanbase
{
namespace obproxy
{

enum ObProxyKernelRelease
{
  //Linux server release version
  RELEASE_5U = 0,
  RELEASE_6U,
  RELEASE_7U,
  RELEASE_UNKNOWN,
  RELEASE_MAX,
};

common::ObString get_kernel_release_string(const ObProxyKernelRelease release);

class ObProxyTableInfo
{
public:
  static int get_create_proxy_table_sql(char *buf, const int64_t len);
  static int get_create_proxy_config_table_sql(char *buf, const int64_t len);
  static int get_create_proxy_stat_table_sql(char *buf, const int64_t len);
  static int get_create_proxy_kv_table_sql(char *buf, const int64_t len);
  static int get_create_proxy_vip_tenant_table_sql(char *buf, const int64_t len);

  static const char *CREATE_PROXY_TABLE_SQL;
  static const char *CREATE_PROXY_CONFIG_TABLE_SQL;
  static const char *CREATE_PROXY_STAT_TABLE_SQL;
  static const char *CREATE_PROXY_KV_TABLE_SQL;
  static const char *CREATE_PROXY_VIP_TENANT_TABLE_SQL;

  static const char *PROXY_INFO_TABLE_NAME;
  static const char *PROXY_CONFIG_TABLE_NAME;
  static const char *PROXY_STAT_TABLE_NAME;
  static const char *PROXY_KV_TABLE_NAME;
  static const char *PROXY_VIP_TENANT_TABLE_NAME;

  static const char *PROXY_VIP_TENANT_VERSION_NAME;
  static const int64_t PROXY_VIP_TENANT_VERSION_DEFAULT_VALUE = 100;
  static const int64_t PROXY_UPGRADE_DURATION_MINUTES_DEFAULT_VALUE = 0;

  static const common::ObString PROXY_UPGRADE_SWITCH_NAME;
  static const common::ObString PROXY_UPGRADE_START_TIME_NAME;
  static const common::ObString PROXY_UPGRADE_DURATION_MINUTES_NAME;
  static const common::ObString PROXY_UPGRADE_NEW_BINARY_NAME[RELEASE_MAX];
  static const common::ObString PROXY_UPGRADE_NEW_BINARY_MD5[RELEASE_MAX];
  static const common::ObString PROXY_ALL_PROXY_HEADER;
  static const common::ObString PROXY_CONFIG_VERSION_NAME;

  static const char *READ_ONLY_USERNAME_USER;
  static const char *READ_ONLY_USERNAME;
  static const char *READ_ONLY_DATABASE;
  static const char *READ_ONLY_PASSWD_STAGED1;

  static const char *OBSERVER_SYS_PASSWORD;
  static const char *OBSERVER_SYS_PASSWORD1;

  static const char *TEST_MODE_USERNAME;
  static const char *TEST_MODE_PASSWORD;
  static const char *TEST_MODE_DATABASE;

  static const char *DETECT_USERNAME_USER;
  static const char *BINLOG_USERNAME_USER;
};

} // end of namespace obproxy
} // end of namespace oceanbase

#endif  // OBPROXY_TABLE_DEFINE_H
