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

#ifndef OBPROXY_UNIX_NET_H
#define OBPROXY_UNIX_NET_H

#include "iocore/net/ob_poll_descriptor.h"
#include "iocore/net/ob_unix_net_processor.h"
#include "iocore/net/ob_unix_net_vconnection.h"

namespace oceanbase
{
namespace obproxy
{
namespace net
{

extern ObHRTime last_transient_accept_error;

class ObNetHandler;
typedef int (ObNetHandler::*NetContHandler)(int event, void *data);

#define TRANSIENT_ACCEPT_ERROR_MESSAGE_EVERY      HRTIME_HOURS(24)
#define NET_RETRY_DELAY                           HRTIME_MSECONDS(1)
#define NET_PERIOD                               -HRTIME_MSECONDS(1)
#define ACCEPT_PERIOD                            -HRTIME_MSECONDS(1)

class ObNetPoll
{
public:
  explicit ObNetPoll(ObNetHandler &nh);
  ~ObNetPoll();
  int init();
  ObPollDescriptor &get_poll_descriptor() { return *poll_descriptor_; }

public:
  ObPollDescriptor *poll_descriptor_;

private:
  ObNetHandler &nh_;
  int poll_timeout_;

  DISALLOW_COPY_AND_ASSIGN(ObNetPoll);
};

// One Inactivity cop runs on each thread once every second and
// loops through the list of NetVCs and calls the timeouts
class ObInactivityCop : public event::ObContinuation
{
public:
  explicit ObInactivityCop(event::ObProxyMutex *m);
  virtual ~ObInactivityCop() {}

  int check_inactivity(int event, event::ObEvent *e);

  void set_max_connections(const int64_t x) { max_connections_in_ = x; }
  void set_connections_per_thread(const int64_t x) { connections_per_thread_in_ = x; }
  void set_default_timeout(const int64_t x) { default_inactivity_timeout_ = x; }

private:
  int keep_alive_lru(ObNetHandler &nh, ObHRTime now, event::ObEvent *e);

private:
  int64_t default_inactivity_timeout_;  // only used when one is not set for some bad reason
  int64_t total_connections_in_;
  int64_t max_connections_in_;
  int64_t connections_per_thread_in_;

  DISALLOW_COPY_AND_ASSIGN(ObInactivityCop);
};

// A ObNetHandler handles the Network IO operations. It maintains
// lists of operations at multiples of it's periodicity.
class ObNetHandler : public event::ObContinuation
{
public:
  ObNetHandler();
  virtual ~ObNetHandler() {}

  int start_net_event(int event, event::ObEvent *data);

private:
  int main_net_event(int event, event::ObEvent *data);
  void process_enabled_list();

public:
  event::ObEvent *trigger_event_;
  QueM(ObUnixNetVConnection, ObNetState, read_, ready_link_) read_ready_list_;
  QueM(ObUnixNetVConnection, ObNetState, write_, ready_link_) write_ready_list_;
  Que(ObUnixNetVConnection, link_) open_list_;
  ObDLList(ObUnixNetVConnection, cop_link_) cop_list_;
  ASLLM(ObUnixNetVConnection, ObNetState, read_, enable_link_) read_enable_list_;
  ASLLM(ObUnixNetVConnection, ObNetState, write_, enable_link_) write_enable_list_;
  Que(ObUnixNetVConnection, keep_alive_link_) keep_alive_list_;

  int64_t keep_alive_lru_size_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObNetHandler);
};

int update_cop_config(const int64_t default_inactivity_timeout, const int64_t max_client_connections);

// 1  - transient
// 0  - report as warning
// -1 - fatal
inline int accept_error_seriousness(int res)
{
  int ret = 0;
  switch (res) {
    case OB_SYS_EAGAIN:
    case OB_SYS_ECONNABORTED:
    case OB_SYS_ECONNRESET:      // for Linux
    case OB_SYS_EPIPE:           // also for Linux
      ret = 1;
      break;

    case OB_SYS_EMFILE:
    case OB_SYS_ENOMEM:
#if defined(ENOSR) && !defined(freebsd)
    case OB_SYS_ENOSR:
#endif
      PROXY_NET_LOG(WARN, "throttling misconfigured: set too high", K(res));
      ret = 0;
      break;

#ifdef ENOBUFS
    case OB_SYS_ENOBUFS:
#endif
#ifdef ENFILE
    case OB_SYS_ENFILE:
#endif
      ret = 0;
      break;

    case OB_SYS_EINTR:
      PROXY_NET_LOG(WARN, "should be handled at a lower level", K(res));
      ret = 0;
      break;

#if defined(EPROTO) && !defined(freebsd)
    case OB_SYS_EPROTO:
#endif
    case OB_SYS_EOPNOTSUPP:
    case OB_SYS_ENOTSOCK:
    case OB_SYS_ENODEV:
    case OB_SYS_EBADFD:
    default:
      ret = -1;
      break;
  }
  return ret;
}

inline void check_transient_accept_error(const int res)
{
  ObHRTime t = event::get_hrtime();
  if (0 == last_transient_accept_error
      || t - last_transient_accept_error > TRANSIENT_ACCEPT_ERROR_MESSAGE_EVERY) {
    last_transient_accept_error = t;
    PROXY_NET_LOG(WARN, "accept thread received transient error", K(res));
#if defined(linux)
    if (OB_SYS_ENOBUFS == res || OB_SYS_ENFILE == res) {
      PROXY_NET_LOG(WARN, "consider a memory upgrade", K(res));
    }
#endif
  }
}

} // end of namespace net
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_UNIX_NET_H
