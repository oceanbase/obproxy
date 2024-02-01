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
#include "utils/ob_proxy_table_define.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
const char *ObProxyTableInfo::CREATE_PROXY_TABLE_SQL                        =
    "CREATE TABLE IF NOT EXISTS %s (           \n"
    "  proxy_ip                   varchar(%ld),\n"
    "  proxy_port                 bigint,      \n"
    "  regist_time                datetime,    \n"
    "  proxy_id                   bigint,      \n"
    "  app_name                   varchar(%ld),\n"
    "  binary_version             varchar(%ld),\n"
    "  config_version             bigint,      \n"
    "  system_info                varchar(%ld),\n"
    "  current_pid                bigint,      \n"
    "  update_time                timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6), \n"
    "  hot_upgrade_cmd            varchar(%ld),\n"
    "  parent_hot_upgrade_status  varchar(%ld),\n"
    "  sub_hot_upgrade_status     varchar(%ld),\n"
    "  new_binary_version         varchar(%ld),\n"
    "  new_binary_5u_md5          varchar(%ld),\n"
    "  new_binary_6u_md5          varchar(%ld),\n"
    "  new_binary_7u_md5          varchar(%ld),\n"
    "  reload_config              varchar(%ld),\n"
    "  info                       varchar(%ld),\n"
    "  PRIMARY KEY(proxy_ip, proxy_port)       \n"
    ");\n";

const char *ObProxyTableInfo:: CREATE_PROXY_CONFIG_TABLE_SQL                =
    "CREATE TABLE IF NOT EXISTS %s (\n"
    "  app_name     varchar(%ld),   \n"
    "  name         varchar(%ld),   \n"
    "  value        varchar(%ld),   \n"
    "  info         varchar(%ld),   \n"
    "  need_reboot  varchar(%ld),   \n"
    "  PRIMARY KEY(app_name, name)  \n"
    ");\n";

const char *ObProxyTableInfo::CREATE_PROXY_STAT_TABLE_SQL                   =
    "CREATE TABLE IF NOT EXISTS %s (         \n"
    "  cluster_name varchar(%ld),            \n"
    "  proxy_ip     varchar(%ld),            \n"
    "  proxy_port   bigint,                  \n"
    "  session_id   bigint,                  \n"
    "  stat_name    varchar(%ld),            \n"
    "  value        bigint DEFAULT 0,        \n"
    "  gmt_modified timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6), \n"
    "  info         varchar(%ld) DEFAULT '', \n"
    "  PRIMARY KEY(cluster_name, proxy_ip, proxy_port, session_id, stat_name)"
    ");\n";

const char *ObProxyTableInfo::CREATE_PROXY_KV_TABLE_SQL                     =
    "CREATE TABLE IF NOT EXISTS %s (\n"
    "  name         varchar(%ld),   \n"
    "  datatype     bigint,         \n"
    "  value        varchar(%ld),   \n"
    "  info         varchar(%ld),   \n"
    "  PRIMARY KEY(name)            \n"
    ");\n";

const char *ObProxyTableInfo::CREATE_PROXY_VIP_TENANT_TABLE_SQL             =
    "CREATE TABLE IF NOT EXISTS %s (       \n"
    "  vid          bigint DEFAULT 0,      \n"
    "  vip          varchar(%ld),          \n"
    "  vport        bigint,                \n"
    "  tenant_name  varchar(%ld) NOT NULL, \n"
    "  cluster_name varchar(%ld) NOT NULL, \n"
    "  info         varchar(%ld),          \n"
    "  PRIMARY KEY(vid, vip, vport)        \n"
    ");\n";

const char *ObProxyTableInfo::PROXY_INFO_TABLE_NAME                         = "ob_all_proxy";
const char *ObProxyTableInfo::PROXY_CONFIG_TABLE_NAME                       = "ob_all_proxy_app_config";
const char *ObProxyTableInfo::PROXY_STAT_TABLE_NAME                         = "ob_all_proxy_stat";
const char *ObProxyTableInfo::PROXY_KV_TABLE_NAME                           = "ob_all_proxy_kv_table";
const char *ObProxyTableInfo::PROXY_VIP_TENANT_TABLE_NAME                   = "ob_all_proxy_vip_tenant";

const char *ObProxyTableInfo::PROXY_VIP_TENANT_VERSION_NAME                 = "ob_proxy_vip_tenant_version";


const char *ObProxyTableInfo::READ_ONLY_USERNAME_USER                       = "proxyro";
const char *ObProxyTableInfo::READ_ONLY_USERNAME                            = "proxyro@sys";
const char *ObProxyTableInfo::READ_ONLY_DATABASE                            = "oceanbase";
const char *ObProxyTableInfo::OBSERVER_SYS_PASSWORD                         = "observer_sys_password";
const char *ObProxyTableInfo::OBSERVER_SYS_PASSWORD1                        = "observer_sys_password1";

const char *ObProxyTableInfo::TEST_MODE_USERNAME                            = "root@sys";
const char *ObProxyTableInfo::TEST_MODE_PASSWORD                            = "";
const char *ObProxyTableInfo::TEST_MODE_DATABASE                            = "oceanbase";

const char *ObProxyTableInfo::DETECT_USERNAME_USER                          = "detect_user";
const char *ObProxyTableInfo::BINLOG_USERNAME_USER   = "binlog_user";

const ObString ObProxyTableInfo::PROXY_ALL_PROXY_HEADER                     = ObString::make_string("all_proxy");
const ObString ObProxyTableInfo::PROXY_CONFIG_VERSION_NAME                  = ObString::make_string(".config_version");
const ObString ObProxyTableInfo::PROXY_UPGRADE_SWITCH_NAME                  = ObString::make_string(".upgrade_switch");
const ObString ObProxyTableInfo::PROXY_UPGRADE_START_TIME_NAME              = ObString::make_string(".upgrade_start_time");
const ObString ObProxyTableInfo::PROXY_UPGRADE_DURATION_MINUTES_NAME        = ObString::make_string(".upgrade_duration_minutes");
const ObString ObProxyTableInfo::PROXY_UPGRADE_NEW_BINARY_NAME[RELEASE_MAX] = {
    ObString::make_string(".new_binary_name_5u"),
    ObString::make_string(".new_binary_name_6u"),
    ObString::make_string(".new_binary_name_7u"),
    ObString::make_string(".new_binary_name_unknown"),
};

const ObString ObProxyTableInfo::PROXY_UPGRADE_NEW_BINARY_MD5[RELEASE_MAX]  = {
    ObString::make_string(".new_binary_md5_5u"),
    ObString::make_string(".new_binary_md5_6u"),
    ObString::make_string(".new_binary_md5_7u"),
    ObString::make_string(".new_binary_md5_unknown"),
};


int ObProxyTableInfo::get_create_proxy_table_sql(char *buf, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(buf), K(len), K(ret));
  } else {
    int64_t w_len = snprintf(buf, len, CREATE_PROXY_TABLE_SQL,
                             PROXY_INFO_TABLE_NAME,
                             MAX_IP_ADDR_LENGTH,                         // ip
                             OB_MAX_APP_NAME_LENGTH,                 // app_name
                             OB_MAX_PROXY_BINARY_VERSION_LEN,        // binary_version
                             OB_MAX_UNAME_INFO_LEN,                  // system_info
                             OB_MAX_PROXY_HOT_UPGRADE_CMD_LEN,       // hot_upgrade_cmd
                             OB_MAX_PROXY_HOT_UPGRADE_STATUS_LEN,    // parent_hot_upgrade_status
                             OB_MAX_PROXY_HOT_UPGRADE_STATUS_LEN,    // sub_hot_upgrade_status
                             OB_MAX_PROXY_BINARY_VERSION_LEN,        // new_binary_version
                             OB_MAX_PROXY_MD5_LEN,                   // new_binary_5u_md5
                             OB_MAX_PROXY_MD5_LEN,                   // new_binary_6u_md5
                             OB_MAX_PROXY_MD5_LEN,                   // new_binary_7u_md5
                             OB_MAX_PROXY_RELOAD_CONFIG_STATUS_LEN,  // reload_config
                             OB_MAX_PROXY_INFO_LEN                   // info
                             );
    if (OB_UNLIKELY(w_len >= len) || OB_UNLIKELY(w_len <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("fail to fill create sql", K(PROXY_INFO_TABLE_NAME),
               K(buf), K(len), K(w_len), K(ret));
    }
  }

  return ret;
}

int ObProxyTableInfo::get_create_proxy_config_table_sql(char *buf, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(buf), K(len), K(ret));
  } else {
    // as the ObConfigItem use OB_MAX_CONFIG_NAME_LEN and sprintf,
    // we have to set length = OB_MAX_CONFIG_NAME_LEN - 1 ;
    int64_t w_len = snprintf(buf, len, CREATE_PROXY_CONFIG_TABLE_SQL,
                             PROXY_CONFIG_TABLE_NAME ,        // table name
                             OB_MAX_APP_NAME_LENGTH,          // app name
                             OB_MAX_CONFIG_NAME_LEN - 1,      // name
                             OB_MAX_CONFIG_VALUE_LEN - 1,     // value
                             OB_MAX_CONFIG_INFO_LEN - 1,      // info
                             OB_MAX_CONFIG_NEED_REBOOT_LEN -1 // need_reboot
                             );
    if (OB_UNLIKELY(w_len >= len) || OB_UNLIKELY(w_len <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("fail to fill create sql", K(CREATE_PROXY_CONFIG_TABLE_SQL),
               K(buf), K(len), K(w_len), K(ret));
    }
  }

  return ret;
}

int ObProxyTableInfo::get_create_proxy_stat_table_sql(char *buf, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(buf), K(len), K(ret));
  } else {
    int64_t w_len = snprintf(buf, len, CREATE_PROXY_STAT_TABLE_SQL,
                             PROXY_STAT_TABLE_NAME ,        // table name
                             OB_MAX_APP_NAME_LENGTH,        // cluster_name
                             OB_MAX_CONFIG_NAME_LEN,        // proxy_ip
                             OB_MAX_CONFIG_NAME_LEN,        // stat_name
                             OB_MAX_CONFIG_INFO_LEN         // info
                             );
    if (OB_UNLIKELY(w_len >= len) || OB_UNLIKELY(w_len <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("fail to fill create sql", K(PROXY_STAT_TABLE_NAME),
               K(buf), K(len), K(w_len), K(ret));
    }
  }

  return ret;
}

int ObProxyTableInfo::get_create_proxy_kv_table_sql(char *buf, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(buf), K(len), K(ret));
  } else {
    int64_t w_len = snprintf(buf, len, CREATE_PROXY_KV_TABLE_SQL,
                             PROXY_KV_TABLE_NAME,           // table name
                             OB_MAX_CONFIG_NAME_LEN,        // name
                             OB_MAX_CONFIG_VALUE_LEN,       // value
                             OB_MAX_CONFIG_INFO_LEN         // info
                             );
    if (OB_UNLIKELY(w_len >= len) || OB_UNLIKELY(w_len <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("fail to fill create sql", K(CREATE_PROXY_KV_TABLE_SQL),
               K(buf), K(len), K(w_len), K(ret));
    }
  }

  return ret;
}

int ObProxyTableInfo::get_create_proxy_vip_tenant_table_sql(char *buf, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(buf), K(len), K(ret));
  } else {
    int64_t w_len = snprintf(buf, len, CREATE_PROXY_VIP_TENANT_TABLE_SQL,
                             PROXY_VIP_TENANT_TABLE_NAME,           // table name
                             MAX_IP_ADDR_LENGTH,                        // vip
                             OB_MAX_TENANT_NAME_LENGTH,             // tenant_name length
                             OB_PROXY_MAX_CLUSTER_NAME_LENGTH,      // cluster_name length
                             OB_MAX_CONFIG_INFO_LEN                 // info length
                             );
    if (OB_UNLIKELY(w_len >= len) || OB_UNLIKELY(w_len <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("fail to fill create sql", K(CREATE_PROXY_VIP_TENANT_TABLE_SQL),
               K(buf), K(len), K(w_len), K(ret));
    }
  }

  return ret;
}

// !! note that you cannot modify the kernel_release name,
// which may cause the OCP hot upgrade obproxy incompatibility.
ObString get_kernel_release_string(const ObProxyKernelRelease release)
{
  static const ObString release_string_array[RELEASE_MAX] =
  {
    ObString::make_string("RELEASE_5U"),
    ObString::make_string("RELEASE_6U"),
    ObString::make_string("RELEASE_7U"),
    ObString::make_string("RELEASE_UNKNOWN")
  };

  ObString string;
  if (OB_LIKELY(release >= RELEASE_5U) && OB_LIKELY(release < RELEASE_MAX)) {
    string = release_string_array[release];
  }
  return string;
}

} // end of namespace obproxy
} // end of namespace oceanbase
