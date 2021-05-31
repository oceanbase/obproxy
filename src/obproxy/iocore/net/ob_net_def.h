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
 *
 * *************************************************************
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * *************************************************************
 *
 * Net subsystem is a layer on top the operations sytem network apis. It
 * provides an interface for accepting/creating new connection oriented
 * (TCP) and connection less (UDP) connetions and for reading/writing
 * data through these. The net system can manage 1000s of connections
 * very efficiently. Another advantage of using the net system is that
 * the SMs dont have be concerned about differences in the net apis of
 * various operations systems.
 *
 * SMs use the netProcessor global object of the Net System to create new
 * connections or to accept incoming connections. When a new connection
 * is created the SM gets a NetVConnection which is a handle for the
 * underlying connections. The SM can then use the NetVConnection to get
 * properties of the connection, read and write data. Net system also
 * has socks and ssl support.
 */

#ifndef OBPROXY_NET_DEF_H
#define OBPROXY_NET_DEF_H

#include <errno.h>
#include <netinet/in.h>
#include "iocore/eventsystem/ob_event_system.h"
#include "iocore/net/ob_net_vconnection.h"
#include "iocore/net/ob_net_processor.h"

namespace oceanbase
{
namespace obproxy
{
namespace net
{

// Net system uses event threads
// so, the net thread group id is the event thread group id
#define ET_NET ET_CALL

static const int NO_FD = -1;

#define NET_EVENT_OPEN                    (NET_EVENT_EVENTS_START)
#define NET_EVENT_OPEN_FAILED             (NET_EVENT_EVENTS_START+1)
#define NET_EVENT_ACCEPT                  (NET_EVENT_EVENTS_START+2)
#define NET_EVENT_ACCEPT_SUCCEED          (NET_EVENT_EVENTS_START+3)
#define NET_EVENT_ACCEPT_FAILED           (NET_EVENT_EVENTS_START+4)
#define NET_EVENT_CANCEL                  (NET_EVENT_EVENTS_START+5)
#define NET_EVENT_DATAGRAM_READ_COMPLETE  (NET_EVENT_EVENTS_START+6)
#define NET_EVENT_DATAGRAM_READ_ERROR     (NET_EVENT_EVENTS_START+7)
#define NET_EVENT_DATAGRAM_WRITE_COMPLETE (NET_EVENT_EVENTS_START+8)
#define NET_EVENT_DATAGRAM_WRITE_ERROR    (NET_EVENT_EVENTS_START+9)
#define NET_EVENT_DATAGRAM_READ_READY     (NET_EVENT_EVENTS_START+10)
#define NET_EVENT_DATAGRAM_OPEN           (NET_EVENT_EVENTS_START+11)
#define NET_EVENT_DATAGRAM_ERROR          (NET_EVENT_EVENTS_START+12)
#define NET_EVENT_ACCEPT_INTERNAL         (NET_EVENT_EVENTS_START+22)
#define NET_EVENT_CONNECT_INTERNAL        (NET_EVENT_EVENTS_START+23)

} // end of namespace net
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_NET_DEF_H
