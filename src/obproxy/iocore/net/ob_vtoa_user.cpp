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

  struct sockaddr_in saddr;
  int64_t saddrlen = sizeof(saddr);
  struct sockaddr_in daddr;
  int64_t daddrlen = sizeof(daddr);

  if (OB_ISNULL(vs) || OB_ISNULL(len) || OB_UNLIKELY(*len != sizeof(struct vtoa_get_vs4rds))) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_NET_LOG(WARN, "invalid argument", K(sockfd), K(vs), K(len), K(ret));
  } else if (OB_FAIL(ObSocketManager::getpeername(sockfd,
                                                  reinterpret_cast<struct sockaddr *>(&saddr),
                                                  &saddrlen))) {
    PROXY_NET_LOG(WARN, "fail to getpeername", K(sockfd), KERRMSGS, K(ret));
  } else if (OB_FAIL(ObSocketManager::getsockname(sockfd,
                                                  reinterpret_cast<struct sockaddr *>(&daddr),
                                                  &daddrlen))) {
    PROXY_NET_LOG(WARN, "fail to getsockname", K(sockfd), KERRMSGS, K(ret));
  } else {
    vs->protocol = IPPROTO_TCP;
    vs->caddr = saddr.sin_addr.s_addr;
    vs->cport = saddr.sin_port;
    vs->daddr = daddr.sin_addr.s_addr;
    vs->dport = daddr.sin_port;

    if (OB_FAIL(ObSocketManager::getsockopt(sockfd, IPPROTO_IP, VTOA_SO_GET_VS4RDS, vs, len))) {
      PROXY_NET_LOG(DEBUG, "fail to getsockopt VTOA_SO_GET_VS4RDS", K(sockfd), KERRMSGS,
                          "client", ObIpEndpoint(ops_ip_sa_cast(saddr)),
                          "destination", ObIpEndpoint(ops_ip_sa_cast(daddr)),
                          K(ret));
    }
  }
  return ret;
}

int get_vip(int sockfd, struct vtoa_get_vs *vs, int *len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(vs) || OB_ISNULL(len) || OB_UNLIKELY (*len != sizeof(struct vtoa_get_vs))) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_NET_LOG(WARN, "invalid argument", K(sockfd), K(vs), K(len), K(ret));
  } else {
    if (OB_FAIL(ObSocketManager::getsockopt(sockfd, IPPROTO_IP, VTOA_SO_GET_VS, vs, len))) {
      PROXY_NET_LOG(WARN, "fail to getsockopt", K(sockfd), KERRMSGS, K(ret));
    }
  }
  return ret;
}

} // end of namespace net
} // end of namespace obproxy
} // end of namespace oceanbase
