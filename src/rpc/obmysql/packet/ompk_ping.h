/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OMPK_PING_H_
#define _OMPK_PING_H_

#include "rpc/obmysql/ob_mysql_packet.h"

namespace oceanbase
{
namespace obmysql
{

class OMPKPing
    : public ObMySQLRawPacket
{
}; // end of class OMPKPing

} // end of namespace obmysql
} // end of namespace oceanbase

#endif /* _OMPK_PING_H_ */
