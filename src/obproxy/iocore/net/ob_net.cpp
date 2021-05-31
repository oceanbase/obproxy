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

#include "iocore/net/ob_net.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace obproxy
{
namespace net
{

#define NET_REGISTER_RAW_STAT(rsb, rec_type, name, data_type, id, sync_type, persist_type)  \
  if (OB_SUCC(ret)) { \
    ret = g_stat_processor.register_raw_stat(rsb, rec_type, name, data_type, id, sync_type, persist_type); \
  }

// This will get set via either command line or ObProxyConfig.
// epoll timeout
int net_config_poll_timeout = -1;

int init_net(ObModuleVersion version, const ObNetOptions &net_options)
{
  static bool inited = false;
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_module_version(version, NET_SYSTEM_MODULE_VERSION))) {
    PROXY_NET_LOG(WARN, "failed to check module version", K(version), K(ret));
  } else if (!inited) {
    // do one time stuff
    if (OB_FAIL(update_net_options(net_options))) {
      PROXY_NET_LOG(WARN, "fail to update_net_options", K(ret));
    } else {
      inited = true;
    }
  }
  return ret;
}

int update_net_options(const ObNetOptions &net_options)
{
  int ret = OB_SUCCESS;
  net_config_poll_timeout = static_cast<int32_t>(net_options.poll_timeout_);
  if (OB_FAIL(update_cop_config(net_options.default_inactivity_timeout_, net_options.max_client_connections_))) {
    PROXY_NET_LOG(WARN, "fail to update_cop_config",
                  K(net_options.default_inactivity_timeout_),
                  K(net_options.max_client_connections_), K(ret));
  }
  return ret;
}

} // end of namespace net
} // end of namespace obproxy
} // end of namespace oceanbase
