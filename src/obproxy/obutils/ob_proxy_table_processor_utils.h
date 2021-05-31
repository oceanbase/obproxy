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

#ifndef OBPROXY_TABLE_PROCESSOR_UTILS_H
#define OBPROXY_TABLE_PROCESSOR_UTILS_H

#include "lib/net/ob_addr.h"
#include "utils/ob_proxy_hot_upgrader.h"
#include "obutils/ob_vip_tenant_cache.h"

namespace oceanbase
{
namespace obproxy
{
class ObResultSetFetcher;
namespace proxy
{
class ObMysqlProxy;
class ObMysqlResultHandler;
}
namespace obutils
{
class ObProxyServerInfo;
class ObProxyKVRowsName;
class ObProxyKVTableInfo;
class ObVipTenantCache;
class ObProxyTableProcessorUtils
{
public:
  // get proxy info from ObProxyTableInfo::PROXY_INFO_TABLE_NAME
  static int get_proxy_info(proxy::ObMysqlProxy &mysql_proxy,
                            const char *proxy_ip,
                            const int32_t proxy_port,
                            ObProxyServerInfo &proxy_info);

  // fill proxy info according to mysql result
  static int fill_proxy_info(proxy::ObMysqlResultHandler &result_handler,
                             ObProxyServerInfo &proxy_info);

  // get proxy kv table info from ObProxyTableInfo::PROXY_KV_TABLE_NAME
  static int get_proxy_kv_table_info(proxy::ObMysqlProxy &mysql_proxy,
                                     const ObProxyKVRowsName &rows,
                                     ObProxyKVTableInfo &kv_info);

  // fill proxy kv table info according to mysql result
  static int fill_kv_table_info(proxy::ObMysqlResultHandler &result_handler,
                                const ObProxyKVRowsName &rows,
                                ObProxyKVTableInfo &kv_info);

  // insert into ... on duplicate key update ..., add or modify proxy info(like ob052's replace)
  static int add_or_modify_proxy_info(proxy::ObMysqlProxy &mysql_proxy,
                                      ObProxyServerInfo &proxy_info);

  static int add_proxy_kv_table_info(proxy::ObMysqlProxy &mysql_proxy, const char *appname);

  // get config version from observer
  static int get_config_version(proxy::ObMysqlProxy &mysql_proxy, int64_t &version);

  // get proxy info from ObProxyTableInfo::PROXY_VIP_TENANT_TABLE_NAME
  static int get_vip_tenant_info(proxy::ObMysqlProxy &mysql_proxy,
                                 ObVipTenantCache::VTHashMap &cache_map);
  static int fill_local_vt_cache(proxy::ObMysqlResultHandler &result_handler,
                                 ObVipTenantCache::VTHashMap &cache_map);


  static int update_proxy_table_config(proxy::ObMysqlProxy &mysql_proxy,
                                       const char *proxy_ip,
                                       const int32_t proxy_port,
                                       const int64_t config_version,
                                       const ObReloadConfigStatus rc_status);

  static int update_proxy_hu_cmd(proxy::ObMysqlProxy &mysql_proxy,
                                 const char *proxy_ip,
                                 const int32_t proxy_port,
                                 const ObHotUpgradeCmd target_cmd,
                                 const ObHotUpgradeCmd orig_cmd,
                                 const ObHotUpgradeCmd orig_cmd_either,
                                 const ObHotUpgradeCmd orig_cmd_another = HUC_MAX);

  static int get_proxy_update_hu_cmd_sql(char *sql_buf,
                                         const int64_t buf_len,
                                         const char *proxy_ip,
                                         const int32_t proxy_port,
                                         const ObHotUpgradeCmd target_cmd,
                                         const ObHotUpgradeCmd orig_cmd,
                                         const ObHotUpgradeCmd orig_cmd_either,
                                         const ObHotUpgradeCmd orig_cmd_another = HUC_MAX);

  static int update_proxy_status_and_cmd(proxy::ObMysqlProxy &mysql_proxy,
                                         const char *proxy_ip,
                                         const int32_t proxy_port,
                                         const ObHotUpgradeStatus parent_status,
                                         const ObHotUpgradeStatus sub_status,
                                         const ObHotUpgradeCmd cmd = HUC_MAX,
                                         const bool retries_too_many = false);

  static int update_proxy_exited_status(proxy::ObMysqlProxy &mysql_proxy,
                                        const char *proxy_ip,
                                        const int32_t proxy_port,
                                        const ObHotUpgradeStatus parent_status,
                                        const ObHotUpgradeCmd target_cmd);

  static int update_proxy_update_time(proxy::ObMysqlProxy &mysql_proxy,
                                      const char *proxy_ip,
                                      const int32_t proxy_port);

  static int update_proxy_info(proxy::ObMysqlProxy &mysql_proxy,
                               ObProxyServerInfo &proxy_info,
                               const bool set_rollback = false);

  static int get_proxy_local_addr(common::ObAddr &addr);

  static int get_one_local_addr(char *ip, const int64_t len);

  static int get_uname_info(char *info, const int64_t len);
};

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OBPROXY_TABLE_PROCESSOR_UTILS_H */
