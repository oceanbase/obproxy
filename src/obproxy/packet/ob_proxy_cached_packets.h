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

#ifndef OBPROXY_CACHED_PACKETS_H
#define OBPROXY_CACHED_PACKETS_H
#include "packet/ob_proxy_ok_packet.h"

namespace oceanbase
{
namespace obproxy
{
namespace packet
{
enum ObCachedOKType
{
  OB_OK_INTERNAL = 0, // internal command, for set @@autocommit = 0, begin
  OB_OK_MAX,
};

class ObProxyCachedPackets
{
public:
  static int get_ok_packet(ObProxyOKPacket *&ok_packet, ObCachedOKType type);
};

} // end of packet
} // end of obproxy
} // end of oceanbase
#endif // end of OBPROXY_CACHED_PACKETS_H
