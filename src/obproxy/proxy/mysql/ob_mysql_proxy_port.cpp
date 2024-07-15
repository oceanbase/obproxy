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
#include "proxy/mysql/ob_mysql_proxy_port.h"
#include "iocore/net/ob_net.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::net;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
ObMysqlProxyPort &get_global_proxy_ipv4_port()
{
  static ObMysqlProxyPort g_proxy_ipv4_port;
  return g_proxy_ipv4_port;
}

ObMysqlProxyPort &get_global_proxy_ipv6_port()
{
  static ObMysqlProxyPort g_proxy_ipv6_port;
  return g_proxy_ipv6_port;
}

/* RPC Service */
ObMysqlProxyPort &get_global_rpc_proxy_ipv4_port()
{
  static ObMysqlProxyPort g_rpc_proxy_ipv4_port;
  return g_rpc_proxy_ipv4_port;
}

ObMysqlProxyPort &get_global_rpc_proxy_ipv6_port()
{
  static ObMysqlProxyPort g_rpc_proxy_ipv6_port;
  return g_rpc_proxy_ipv6_port;
}
/* END RPC service */

void ObMysqlProxyPort::reset()
{
  fd_ = NO_FD;
  port_ = 0;
  family_ = AF_INET;
}

int64_t ObMysqlProxyPort::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  if (inbound_ip_.is_valid()) {
    pos = inbound_ip_.to_string(buf, buf_len);
  }

  databuff_printf(buf, buf_len, pos, ", port=%u", port_);
  if (NO_FD != fd_) {
    databuff_printf(buf, buf_len, pos, ", fd=%d", fd_);
  }
  J_OBJ_END();
  return pos;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
