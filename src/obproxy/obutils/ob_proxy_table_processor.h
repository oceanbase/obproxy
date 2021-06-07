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

#ifndef OBPROXY_PROXY_TABLE_PROCESSOR_H
#define OBPROXY_PROXY_TABLE_PROCESSOR_H

#include "lib/ob_define.h"
#include "lib/net/ob_addr.h"
#include "lib/utility/ob_print_utils.h"
#include "utils/ob_proxy_hot_upgrader.h"
#include "obutils/ob_proxy_config_manager.h"
#include "obutils/ob_config_server_processor.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObMysqlProxy;
}
namespace obutils
{
class ObHotUpgradeProcessor;
class ObVipTenantProcessor;
class ObAsyncCommonTask;

enum ObProxyRowStatus
{
  PRS_NONE = 0,
  PRS_AVAILABLE,
  PRS_UNAVAILABLE,
  PRS_NOT_EXSIT,
};

//.upgrade_start_time | .upgrade_duration_time
const int64_t OB_MAX_PROXY_UPGRADEN_TIME_LEN   = 24;
//upgrade_off | upgrade_part | upgrade_on | upgrade_bin
const int64_t OB_MAX_PROXY_UPGRADEN_SWITCH_LEN = 16;
//all_proxy
const int64_t OB_ALL_PROXY_HEADER_LEN          = sizeof("all_proxy");
const int64_t OB_PROXY_CONFIG_VERSION_LEN      = sizeof(".config_version");

class ObProxyServerInfo
{
public:
  ObProxyServerInfo() { reset(); }
  ~ObProxyServerInfo() { }
  void reset() { memset(this, 0, sizeof(ObProxyServerInfo)); }
  static const char *get_new_binary_md5_str(const ObProxyKernelRelease version);

  int add_app_name(const char *app_name);
  int add_binary_version(const char *binary_version);
  int add_new_binary_version(const char *version);
  int add_new_binary_md5(const char *md5);
  int add_info(const char *info);
  int parse_hu_cmd(const common::ObString &cmd_str);

  DECLARE_TO_STRING;

public:
  char proxy_ip_[common::OB_IP_STR_BUFF];                       // ip primary key
  char regist_time_str_[common::OB_MAX_TIME_STR_LENGTH + 1];    // the time when one obproxy register to observer
  char app_name_[common::OB_MAX_APP_NAME_LENGTH + 1];           // application proxy used for
  char binary_version_[OB_MAX_PROXY_BINARY_VERSION_LEN + 1];    // man readable version
  char uname_info_[OB_MAX_UNAME_INFO_LEN + 1];                  // system info
  char new_binary_version_[OB_MAX_PROXY_BINARY_VERSION_LEN + 1];// new version for hot upgrade
  char new_binary_md5_[OB_MAX_PROXY_MD5_LEN];                   // new version md5 for hot upgrade
  char info_[OB_MAX_PROXY_INFO_LEN + 1];                        // extra info

  int32_t proxy_port_;                  // port primary key
  int64_t config_version_;              // current config_version
  int64_t current_pid_;                 // proxy pid
  int64_t proxy_id_;                    // proxy id, only used for server service mode
  ObReloadConfigStatus rc_status_;      // reload config status, whether reload proxy config
  ObHotUpgradeCmd hu_cmd_;              // hot upgrade cmd, main used for controlling proxy
  ObHotUpgradeStatus parent_hu_status_; // parent process hot upgrade status,
  ObHotUpgradeStatus sub_hu_status_;    // sub process hot upgrade status,
  ObProxyRowStatus row_status_;         // identity proxy info row status

private:
  int add_str_info(char *buf, const char *src, const int64_t len);
  DISALLOW_COPY_AND_ASSIGN(ObProxyServerInfo);
};

enum ObProxyKVRowsIndex
{
  INDEX_CONFIG_VERSION = 0,
  INDEX_VIP_TENANT_VERSION,

  INDEX_APP_UPGRADE_SWITCH,
  INDEX_APP_UPGRADE_START_TIME,
  INDEX_APP_UPGRADE_DURATION_MINUTES,
  INDEX_APP_UPGRADE_NEW_BINARY_NAME,
  INDEX_APP_UPGRADE_NEW_BINARY_MD5,

  INDEX_ALL_UPGRADE_SWITCH,
  INDEX_ALL_UPGRADE_START_TIME,
  INDEX_ALL_UPGRADE_DURATION_MINUTES,
  INDEX_ALL_UPGRADE_NEW_BINARY_NAME,
  INDEX_ALL_UPGRADE_NEW_BINARY_MD5,
  INDEX_MAX,
};

class ObProxyKVRowsName
{
public:
  ObProxyKVRowsName() { MEMSET(this, 0, sizeof(ObProxyKVRowsName)); }
  ~ObProxyKVRowsName() { }
  int reuse();
  TO_STRING_KV(K(name_[INDEX_APP_UPGRADE_SWITCH]));

public:
  common::ObString name_[INDEX_MAX];

  char app_config_version_[common::OB_MAX_APP_NAME_LENGTH + OB_PROXY_CONFIG_VERSION_LEN];
  char app_upgrade_switch_[common::OB_MAX_APP_NAME_LENGTH + OB_MAX_PROXY_UPGRADEN_SWITCH_LEN];
  char app_upgrade_start_time_[common::OB_MAX_APP_NAME_LENGTH  + OB_MAX_PROXY_UPGRADEN_TIME_LEN];
  char app_upgrade_duration_minutes_[common::OB_MAX_APP_NAME_LENGTH  + OB_MAX_PROXY_UPGRADEN_TIME_LEN];
  char app_upgrade_new_binary_name_[common::OB_MAX_APP_NAME_LENGTH + OB_MAX_PROXY_BINARY_VERSION_LEN];
  char app_upgrade_new_binary_md5_[common::OB_MAX_APP_NAME_LENGTH + OB_MAX_PROXY_MD5_LEN];

  char all_upgrade_switch_[OB_ALL_PROXY_HEADER_LEN + OB_MAX_PROXY_UPGRADEN_SWITCH_LEN];
  char all_upgrade_start_time_[OB_ALL_PROXY_HEADER_LEN  + OB_MAX_PROXY_UPGRADEN_TIME_LEN];
  char all_upgrade_duration_minutes_[OB_ALL_PROXY_HEADER_LEN  + OB_MAX_PROXY_UPGRADEN_TIME_LEN];
  char all_upgrade_new_binary_name_[OB_ALL_PROXY_HEADER_LEN + OB_MAX_PROXY_BINARY_VERSION_LEN];
  char all_upgrade_new_binary_md5_[OB_ALL_PROXY_HEADER_LEN + OB_MAX_PROXY_MD5_LEN];

private:
  DISALLOW_COPY_AND_ASSIGN(ObProxyKVRowsName);
};

class ObProxyKVTableInfo
{
public:
  ObProxyKVTableInfo() { reset(); }
  ~ObProxyKVTableInfo() { }
  void reset()
  {
    memset(this, 0, sizeof(ObProxyKVTableInfo));
    app_start_minutes_ = -1;
    app_duration_minutes_ = -1;
    all_start_minutes_ = -1;
    all_duration_minutes_ = -1;
    config_version_ = common::OB_INVALID_VERSION;
    vip_tenant_version_ = common::OB_INVALID_VERSION;
  }

  void reset_result();

  void fill_constant_info();
  int fill_rows(const ObProxyKVRowsName &keys,
                const common::ObString &kv_name,
                const common::ObString &kv_value);
  int parse_upgrade_start_time(const common::ObString &time_string, int64_t &minutes);
  void update_upgrade_switch(ObBatchUpgradeStatus &status, int64_t &start_minutes, int64_t &duration_minutes);

  void recheck_upgrade_switch()
  {
    update_upgrade_switch(app_status_, app_start_minutes_, app_duration_minutes_);
    update_upgrade_switch(all_status_, all_start_minutes_, all_duration_minutes_);
  }

  DECLARE_TO_STRING;

public:
  int64_t config_version_;
  int64_t vip_tenant_version_;

  ObBatchUpgradeStatus app_status_;
  int64_t app_start_minutes_;
  int64_t app_duration_minutes_;
  char app_binary_name_[OB_MAX_PROXY_BINARY_VERSION_LEN + 1];
  char app_binary_md5_[OB_MAX_PROXY_MD5_LEN];

  ObBatchUpgradeStatus all_status_;
  int64_t all_start_minutes_;
  int64_t all_duration_minutes_;
  char all_binary_name_[OB_MAX_PROXY_BINARY_VERSION_LEN + 1];
  char all_binary_md5_[OB_MAX_PROXY_MD5_LEN];

  bool is_found_[INDEX_MAX];

private:
  DISALLOW_COPY_AND_ASSIGN(ObProxyKVTableInfo);
};

class ObProxyTableProcessor
{
public:
  ObProxyTableProcessor();
  ~ObProxyTableProcessor() { destroy(); }
  void destroy();

  int init(proxy::ObMysqlProxy &mysql_proxy, ObAppVersionInfo &proxy_version, ObProxyReloadConfig &reload_config);
  int init_kv_rows_name();
  int start_check_table_task();
  int set_check_interval();

  enum RegisterType
  {
    RT_NONE = 0,
    RT_INIT,
    RT_FORCE,
    RT_CHECK,
  };
  int register_proxy(const RegisterType type);
  int update_local_config(int64_t new_config_version);
  int load_remote_config();
  int update_vip_tenant_cache();
  bool is_scheduled() const { return NULL != table_check_cont_; }

  static int do_repeat_task();
  static void update_interval();

  ObAsyncCommonTask *get_table_check_cont() { return table_check_cont_; }
  int do_check_work();

  DECLARE_TO_STRING;

private:
  int get_proxy_info(ObProxyServerInfo &proxy_info);
  int check_proxy_kv_info(ObProxyKVTableInfo &kv_info);
  int check_proxy_info(const ObProxyKVTableInfo &kv_info);
  int check_reload_config(const int64_t new_config_version);
  int check_update_vip_tenant_cache(const int64_t new_vt_cache_version);
  int check_do_batch_upgrade(const ObProxyKVTableInfo &kv_info, ObProxyServerInfo &proxy_info);
  int check_upgrade_state(const ObProxyServerInfo &proxy_info);
  int check_update_proxy_table(const ObProxyServerInfo &proxy_info);
  int check_stop_time_task();

  int build_proxy_info(ObProxyServerInfo &proxy_info);
  int update_hu_status_and_cmd(const ObProxyServerInfo &proxy_info);
  bool need_do_batch_upgrade(const ObProxyKVTableInfo &kv_info, ObProxyServerInfo &proxy_info);
  bool need_check_proxy_info_table(const ObProxyKVTableInfo &kv_info);

  bool is_meta_cluster_available();

private:
  bool is_inited_;
  bool is_registered_;
  bool is_last_report_succeed_;
  ObAsyncCommonTask *table_check_cont_;
  proxy::ObMysqlProxy *mysql_proxy_;
  ObAppVersionInfo *proxy_version_;
  ObHotUpgradeProcessor &hot_upgrade_processor_;
  ObProxyConfigManager config_manager_;
  ObVipTenantProcessor &vt_processor_;
  ObProxyKVRowsName kv_rows_name_;

  DISALLOW_COPY_AND_ASSIGN(ObProxyTableProcessor);
};

ObProxyTableProcessor &get_global_proxy_table_processor();

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OBPROXY_PROXY_TABLE_PROCESSOR_H */
