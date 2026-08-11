/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <sys/types.h>
#include "iocore/net/ob_vtoa_user.h"
#include "lib/ob_define.h"
#include "iocore/net/ob_net.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace net
{

int get_vip4rds(int sockfd, struct vtoa_get_vs4rds *vs, int *len)
{
  int ret = OB_SUCCESS;
  int ret_getsockopt = OB_SUCCESS;

  struct sockaddr_in saddr;
  int64_t saddrlen = sizeof(saddr);
  struct sockaddr_in daddr;
  int64_t daddrlen = sizeof(daddr);

  if (OB_ISNULL(vs) || OB_ISNULL(len) || OB_UNLIKELY(*len != sizeof(struct vtoa_get_vs4rds))) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_NET_LOG(WDIAG, "invalid argument", K(sockfd), K(vs), K(len), K(ret));
  } else if (OB_FALSE_IT(ret_getsockopt = ObSocketManager::getsockopt(sockfd, IPPROTO_IP, VTOA_SO_GET_VS4RDS, vs, len))) {
  } else if (OB_FAIL(ObSocketManager::getpeername(sockfd,
                                                  reinterpret_cast<struct sockaddr *>(&saddr),
                                                  &saddrlen))) {
    PROXY_NET_LOG(WDIAG, "fail to getpeername", K(sockfd), KERRMSGS, K(ret));
  } else if (OB_FAIL(ObSocketManager::getsockname(sockfd,
                                                  reinterpret_cast<struct sockaddr *>(&daddr),
                                                  &daddrlen))) {
    PROXY_NET_LOG(WDIAG, "fail to getsockname", K(sockfd), KERRMSGS, K(ret));
  } else {
    vs->protocol = IPPROTO_TCP;
    vs->caddr = saddr.sin_addr.s_addr;
    vs->cport = saddr.sin_port;
    vs->daddr = daddr.sin_addr.s_addr;
    vs->dport = daddr.sin_port;
  }

  if (OB_FAIL(ret)) {
    // do nothing just return
  } else if (OB_FAIL(ret_getsockopt)) {
    PROXY_NET_LOG(DEBUG, "fail to getsockopt VTOA_SO_GET_VS4RDS", K(sockfd), KERRMSGS, K(ret_getsockopt));
    ret = ret_getsockopt;
  } else {
    // succ
  }

  return ret;
}

int get_vip(int sockfd, struct vtoa_get_vs *vs, int *len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(vs) || OB_ISNULL(len) || OB_UNLIKELY (*len != sizeof(struct vtoa_get_vs))) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_NET_LOG(WDIAG, "invalid argument", K(sockfd), K(vs), K(len), K(ret));
  } else {
    if (OB_FAIL(ObSocketManager::getsockopt(sockfd, IPPROTO_IP, VTOA_SO_GET_VS, vs, len))) {
      PROXY_NET_LOG(WDIAG, "fail to getsockopt", K(sockfd), KERRMSGS, K(ret));
    }
  }
  return ret;
}

} // end of namespace net
} // end of namespace obproxy
} // end of namespace oceanbase