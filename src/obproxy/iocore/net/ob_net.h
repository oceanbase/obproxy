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
 */

#ifndef OBPROXY_NET_H
#define OBPROXY_NET_H

// Net subsystem
//
// Net subsystem is a layer on top the operations sytem network apis. It
// provides an interface for accepting/creating new connection oriented
// (TCP) and connection less (UDP) connetions and for reading/writing
// data through these. The net system can manage 1000s of connections
// very efficiently. Another advantage of using the net system is that
// the SMs dont have be concerned about differences in the net apis of
// various operations systems.
//
// SMs use the netProcessor global object of the Net System to create new
// connections or to accept incoming connections. When a new connection
// is created the SM gets a ObNetVConnection which is a handle for the
// underlying connections. The SM can then use the ObNetVConnection to get
// properties of the connection, read and write data. Net system also
// has socks and ssl support.

#include <netinet/in.h>
#include "lib/ob_define.h"
#include "stat/ob_net_stats.h"
#include "iocore/net/ob_net_def.h"
#include "iocore/net/ob_net_vconnection.h"
#include "iocore/net/ob_unix_net.h"
#include "iocore/net/ob_unix_net_processor.h"
#include "iocore/net/ob_net_accept.h"
#include "iocore/net/ob_unix_net_vconnection.h"
#include "iocore/net/ob_poll_descriptor.h"
#include "iocore/net/ob_inet.h"
#include "iocore/net/ob_socket_manager.h"
#include "obproxy/stat/ob_stat_processor.h"

namespace oceanbase
{
namespace obproxy
{
namespace net
{

#define NET_SYSTEM_MODULE_MAJOR_VERSION 1
#define NET_SYSTEM_MODULE_MINOR_VERSION 0
#define NET_SYSTEM_MODULE_VERSION make_module_version(                    \
                                       NET_SYSTEM_MODULE_MAJOR_VERSION,   \
                                       NET_SYSTEM_MODULE_MINOR_VERSION,   \
                                       PRIVATE_MODULE_HEADER)

struct ObNetOptions
{
  int64_t poll_timeout_;
  int64_t max_connections_;
  int64_t default_inactivity_timeout_;
  int64_t max_client_connections_;
};

int init_net(ObModuleVersion version, const ObNetOptions &net_options);
int update_net_options(const ObNetOptions &net_options);

} // end of namespace net
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_NET_H
