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
 * ObNetAccept is a generalized facility which allows
 * Connections of different classes to be accepted either
 * from a blockable thread or by adaptive polling.
 *
 * It is used by the ObNetProcessor and the ClusterProcessor
 * and should be considered PRIVATE to processor implementations.
 */

#ifndef OBPROXY_NET_ACCEPT_H
#define OBPROXY_NET_ACCEPT_H

#include "utils/ob_proxy_lib.h"
#include "utils/ob_proxy_hot_upgrader.h"
#include "iocore/net/ob_unix_net.h"
#include "iocore/net/ob_connection.h"

namespace oceanbase
{
namespace obproxy
{
namespace net
{

struct ObNetAccept;
class ObUnixNetVConnection;
class ObNetProcessor;
struct ObEventIO;

typedef int (AcceptFunction)(ObNetAccept *na, void *e, const bool blockable);
typedef AcceptFunction *AcceptFunctionPtr;
AcceptFunction net_accept;

typedef int (ObNetAccept::*NetAcceptHandler)(int event, void *data);

// TODO fix race between cancel accept and call back
class ObNetAcceptAction : public event::ObAction, public common::ObRefCountObj
{
public:
  ObNetAcceptAction() : server_(NULL) {};
  virtual ~ObNetAcceptAction() { server_ = NULL; };

  virtual int cancel(const event::ObContinuation *cont = NULL)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(event::ObAction::cancel(cont))) {
      PROXY_NET_LOG(WARN, "fail to cancel action", K(cont), K(ret));
    } else if (OB_FAIL(server_->close())) {
      PROXY_NET_LOG(WARN, "fail to do server close", K(server_), K(ret));
    } else {/*do nothing*/}
    return ret;
  }

public:
  ObServerConnection *server_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObNetAcceptAction);
};

// Handles accepting connections.
class ObNetAccept : public event::ObContinuation
{
public:
  ObNetAccept();
  virtual ~ObNetAccept();

  int do_listen(const bool non_blocking);
  int deep_copy(const ObNetAccept &na);
  int init_accept_loop(const char *thread_name, const int64_t stacksize);
  int init_accept_per_thread();

  int init_accept();

  ObNetProcessor *get_net_processor() const;
  int set_sock_buf_size(const int fd);

  int init_unix_net_vconnection(const ObConnection &con, ObUnixNetVConnection *vc);
  int init();

private:
  int accept_loop_event(int event, event::ObEvent *e);
  int do_blocking_accept();

  int accept_fast_event(int event, void *e);
  int accept_event(int event, void *e);

  void cancel();
  // for loading balance, get the ethread which has minimal client connections
  event::ObEThread *get_schedule_ethread();
  // for connection balance in each ethread,
  // only the ethread which has minimal client connections will do accept
  bool accept_balance(event::ObEThread *ethread);

public:
  ObServerConnection server_;
  void *alloc_cache_;
  AcceptFunctionPtr accept_fn_;
  bool callback_on_open_;
  bool backdoor_;
  common::ObPtr<ObNetAcceptAction> action_;
  int32_t recv_bufsize_;
  int32_t send_bufsize_;
  uint32_t sockopt_flags_;
  uint32_t packet_mark_;
  uint32_t packet_tos_;
  event::ObEventThreadType etype_;

private:
  bool is_inited_;
  ObHRTime period_;
  ObUnixNetVConnection *epoll_vc_; //only storage for epoll events, !!not used
  ObEventIO *ep_;
};

} // end of namespace net
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_NET_ACCEPT_H
