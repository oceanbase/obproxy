/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
