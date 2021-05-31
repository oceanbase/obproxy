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
 * State information for a particular channel of a ObNetVConnection
 * This information is private to the Net module. It is only here
 * because of the the C++ compiler needs it to define ObNetVConnection.
 */

#ifndef OBPROXY_NET_STATE_H
#define OBPROXY_NET_STATE_H

#include "lib/list/ob_intrusive_list.h"

namespace oceanbase
{
namespace obproxy
{
namespace net
{

class ObUnixNetVConnection;

struct ObNetState
{
  ObNetState()
    : enabled_(false),
    vio_(event::ObVIO::NONE),
    active_count_(0),
    in_enabled_list_(false),
    triggered_(false)
  { }
  ~ObNetState() { }

  volatile bool enabled_;
  event::ObVIO vio_;
  common::Link<ObUnixNetVConnection> ready_link_;
  common::SLink<ObUnixNetVConnection> enable_link_;
  int32_t active_count_;
  bool in_enabled_list_;
  bool triggered_;
};

} // end of namespace net
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_NET_STATE_H
