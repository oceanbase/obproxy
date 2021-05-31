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
#include "packet/ob_proxy_cached_packets.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace packet
{
int ObProxyCachedPackets::get_ok_packet(ObProxyOKPacket *&ok_packet, ObCachedOKType type)
{
  int ret = common::OB_SUCCESS;
  // for safe, write three NULL there
  static __thread ObProxyOKPacket *ok_pkts[OB_OK_MAX] = {NULL};

  // can not use refrence here, so we use pointer to pointer
  if (OB_LIKELY(type < OB_OK_MAX)) {
    if (OB_ISNULL(ok_pkts[type])) {
      if (OB_ISNULL((ok_pkts[type] = new (std::nothrow) ObProxyOKPacket()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc ok_packet", K(ret));
      } else {
        ok_packet = ok_pkts[type];
      }
    } else {
      ok_packet = ok_pkts[type];
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to alloc ok_packet", K(ret));
  }

  return ret;
}
} // end of packet
} // end of obproxy
} // end of oceanbase
