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

#ifndef OBPROXY_CONFIG_MANAGER_H
#define OBPROXY_CONFIG_MANAGER_H

#include "obutils/ob_proxy_config.h"

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
class ObProxyConfig;
class ObProxyReloadConfig;
class ObProxyConfigManager
{
public:
  ObProxyConfigManager();
  ~ObProxyConfigManager() { destroy(); }

  int init(proxy::ObMysqlProxy &mysql_proxy, ObProxyReloadConfig &reload_config);
  void destroy();

  int update_proxy_config(const int64_t new_config_version);
  int64_t get_config_version() const { return proxy_config_.current_local_config_version; }
  ObProxyConfig &get_proxy_config() { return proxy_config_; }
  DECLARE_TO_STRING;

private:
  // really reload proxy config
  int reload_proxy_config();
  // really update local config
  int update_local_config(const int64_t new_config_version, proxy::ObMysqlResultHandler &result_handler);

private:
  static const char *GET_PROXY_CONFIG_SQL;

  bool is_inited_;
  proxy::ObMysqlProxy *mysql_proxy_;
  ObProxyConfig &proxy_config_;
  ObProxyReloadConfig *reload_config_func_;
  DISALLOW_COPY_AND_ASSIGN(ObProxyConfigManager);
};

} //end namespace obutils
} //end namespace obproxy
} //end namespace oceanbase
#endif /* OBPROXY_CONFIG_MANAGER_H */
