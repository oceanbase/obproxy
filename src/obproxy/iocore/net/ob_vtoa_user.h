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

#ifndef OBPROXY_VTOA_USER_H
#define OBPROXY_VTOA_USER_H

#include <linux/types.h>

namespace oceanbase
{
namespace obproxy
{
namespace net
{

#define VTOA_BASE_CTL       (64+1024+64+64+64+64)   // base
#define VTOA_SO_GET_VS      (VTOA_BASE_CTL+1)
#define VTOA_SO_GET_VS4RDS  (VTOA_BASE_CTL+2)

struct vtoa_vs
{
  __u32   vid;    // VPC ID
  __be32  vaddr;  // vip
  __be16  vport;  // vport
};

struct vtoa_get_vs
{
  struct vtoa_vs vs;
};

struct vtoa_get_vs4rds
{
  // which connection
  __u16 protocol;
  __be32 caddr;           // client address
  __be16 cport;
  __be32 daddr;           // destination address
  __be16 dport;

  // the virtual server
  struct vtoa_vs entrytable;
};

int get_vip4rds(int sockfd, struct vtoa_get_vs4rds *vs, int *len);
int get_vip(int sockfd, struct vtoa_get_vs *vs, int *len);

} // end of namespace net
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_VTOA_USER_H
