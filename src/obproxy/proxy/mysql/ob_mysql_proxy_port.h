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

#ifndef OBPROXY_MYSQL_PORT_H
#define OBPROXY_MYSQL_PORT_H

#include "iocore/net/ob_inet.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

// Description of an proxy port.
//
// This consolidates the options needed for proxy ports, both data
// and parsing.
//
// Options are described by a colon separated list of keywords
// without spaces. The options are applied in left to right order. If
// options do not conflict the order is irrelevant.
//
// IPv6 addresses must be enclosed by brackets. Unfortunate but colon is
// so overloaded there's no other option.

struct ObMysqlProxyPort
{
public:
  ObMysqlProxyPort() { reset(); }
  ~ObMysqlProxyPort() { reset(); }
  void reset();

  // create text description to be used for inter-process access
  // prints the file descriptor and then any options
  // @return The number of characters used for the description
  int64_t to_string(char *buf, const int64_t buf_len) const;

public:
  uint8_t family_;       // IP address family
  in_port_t port_;       // port on which to listen
  int fd_;               // pre-opened file descriptor if present

  // local address for inbound connections (listen address)
  net::ObIpAddr inbound_ip_;
};

ObMysqlProxyPort &get_global_proxy_ipv4_port();
ObMysqlProxyPort &get_global_proxy_ipv6_port();

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_MYSQL_PORT_H
