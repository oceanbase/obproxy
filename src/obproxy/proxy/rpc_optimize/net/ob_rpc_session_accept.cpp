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
#include "obutils/ob_proxy_config.h"
#include "proxy/rpc_optimize/net/ob_rpc_client_net_handler.h"
#include "proxy/rpc_optimize/net/ob_rpc_session_accept.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

int ObRpcSessionAccept::accept(ObNetVConnection *netvc, ObMIOBuffer *iobuf, ObIOBufferReader *reader)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(netvc)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_NET_LOG(WDIAG, "invalid argument", K(netvc), K(ret));
  } else {
    const sockaddr &client_ip = netvc->get_remote_addr();
    PROXY_NET_LOG(INFO, "[ObRpcSessionAccept:main_event] accepted connection",
                  K(netvc), "client_ip", ObIpEndpoint(client_ip));

    ObRpcClientNetHandler *new_session = op_reclaim_alloc(ObRpcClientNetHandler);
    if (OB_ISNULL(new_session)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      PROXY_NET_LOG(EDIAG, "failed to allocate memory for ObRpcClientNetHandler", K(ret));
    } else {
      if (OB_FAIL(new_session->new_connection(netvc, iobuf, reader))) {
        PROXY_NET_LOG(EDIAG, "fail to new_connection", K(ret));
      }
    }
  }
  return ret;
}

int ObRpcSessionAccept::main_event(int event, void *data)
{
  int ret = OB_SUCCESS;
  switch (event) {
    case NET_EVENT_ACCEPT_SUCCEED: {
      PROXY_NET_LOG(DEBUG, "ObNetAccept do listen succ");
      break;
    }
    case NET_EVENT_ACCEPT_FAILED: {
      ret = static_cast<int>(reinterpret_cast<uintptr_t>(data));
      PROXY_NET_LOG(EDIAG, "ObNetAccept fail to do listen", K(event), K(ret));
      break;
    }
    case NET_EVENT_ACCEPT: {
      if (OB_ISNULL(data)) {
        ret = OB_INVALID_ARGUMENT;
        PROXY_NET_LOG(EDIAG, "invalid argument", K(data), K(event), K(ret));
      } else if (OB_FAIL(accept(static_cast<ObNetVConnection*>(data), NULL, NULL))) {
        PROXY_NET_LOG(EDIAG, "fail to accept", K(ret));
      }
      break;
    }
    case EVENT_ERROR: {
     if (OB_UNLIKELY(-ECONNABORTED == static_cast<long>(reinterpret_cast<uintptr_t>(data)))) {
        // Under Solaris, when accept() fails and sets
        // errno to EPROTO, it means the client has
        // sent a TCP reset before the connection has
        // been accepted by the server...  Note that in
        // 2.5.1 with the Internet Server Supplement
        // and also in 2.6 the errno for this case has
        // changed from EPROTO to ECONNABORTED.

        PROXY_NET_LOG(EDIAG, "client hang, accept failed", K(data));
      }
      ret = static_cast<int>(reinterpret_cast<uintptr_t>(data));
      PROXY_NET_LOG(EDIAG, "rpc service accept received fatal error", K(event), K(ret));
      break;
    }
    default: {
      ret = OB_ERROR;
      PROXY_NET_LOG(EDIAG, "error, never run here!", K(event), K(ret));
      break;
    }
  }

  return ((OB_SUCCESS == ret) ? (EVENT_CONT) : (EVENT_ERROR));
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
