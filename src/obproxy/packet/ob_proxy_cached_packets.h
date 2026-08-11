/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
