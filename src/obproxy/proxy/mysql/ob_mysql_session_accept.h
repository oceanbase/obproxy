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

#ifndef OBPROXY_MYSQL_SESSION_ACCEPT_H
#define OBPROXY_MYSQL_SESSION_ACCEPT_H

#include "utils/ob_proxy_lib.h"
#include "iocore/net/ob_inet.h"
#include "iocore/net/ob_session_accept.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{
class ObMIOBuffer;
class ObIOBufferReader;
}
namespace net
{
class ObIpAddr;
union ObIpEndpoint;
}
namespace proxy
{

// The continuation mutex is NULL to allow parallel accepts. No
// state is recorded by the handler and values are required to be set
// during construction via the Options struct and never changed. So
// a NULL mutex is safe.
//
// Most of the state is simply passed on to the ObClientSession
// after an accept. It is done here because this is the least bad
// pathway from the top level configuration to the mysql session.
class ObMysqlSessionAccept : public net::ObSessionAccept
{
public:
  ObMysqlSessionAccept()
      : net::ObSessionAccept(NULL)
  {
    SET_HANDLER(&ObMysqlSessionAccept::main_event);
  }

  virtual ~ObMysqlSessionAccept() { }

  virtual int accept(net::ObNetVConnection *netvc, event::ObMIOBuffer *iobuf, event::ObIOBufferReader *reader);
  virtual int main_event(int event, void *netvc);

private:
  DISALLOW_COPY_AND_ASSIGN(ObMysqlSessionAccept);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_MYSQL_SESSION_ACCEPT_H
