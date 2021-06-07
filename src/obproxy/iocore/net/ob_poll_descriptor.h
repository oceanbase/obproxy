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

#ifndef OBPROXY_POLL_DESCRIPTOR_H
#define OBPROXY_POLL_DESCRIPTOR_H

#include "utils/ob_proxy_lib.h"
#include "iocore/net/ob_socket_manager.h"

namespace oceanbase
{
namespace obproxy
{
namespace net
{

typedef struct pollfd ObPollfd;

class ObPollDescriptor
{

public:
  ObPollDescriptor()
    : result_(0),
      epoll_fd_(common::OB_INVALID_INDEX)
  {
    memset(epoll_triggered_events_, 0, sizeof(epoll_triggered_events_));
    memset(pfd_, 0, sizeof(pfd_));
  }
  ~ObPollDescriptor() { }

  int init()
  {
    int ret = common::OB_SUCCESS;
    int result = -1;
    if (OB_FAIL(ObSocketManager::epoll_create(POLL_DESCRIPTOR_SIZE, epoll_fd_))) {
      PROXY_SOCK_LOG(WARN, "fail to epoll_create epoll",
                     K(epoll_fd_), KERRMSGS, K(ret));
    } else if (OB_FAIL(ObSocketManager::fcntl(epoll_fd_, F_SETFD, FD_CLOEXEC, result))) {
      PROXY_SOCK_LOG(WARN, "fail to set FD_CLOSEXEC", KERRMSGS, K(ret));
      if (0 != close(epoll_fd_)) {
        PROXY_SOCK_LOG(WARN, "fail to close epoll fd", K_(epoll_fd), KERRMSGS);
      }
      epoll_fd_ = common::OB_INVALID_INDEX;
    }
    return ret;
  }

  int get_ev_events(const int64_t index, uint32_t &events) const
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(index < 0) || OB_UNLIKELY(index >= POLL_DESCRIPTOR_SIZE)) {
      ret = common::OB_INVALID_ARGUMENT;
      PROXY_SOCK_LOG(WARN, "invalid argument", K(index), K(ret));
    } else {
      events = epoll_triggered_events_[index].events;
    }
    return ret;
  }

  int get_ev_data(const int64_t index, void *&data) const
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(index < 0) || OB_UNLIKELY(index >= POLL_DESCRIPTOR_SIZE)) {
      ret = common::OB_INVALID_ARGUMENT;
      PROXY_SOCK_LOG(WARN, "invalid argument", K(index), K(ret));
    } else {
      data = epoll_triggered_events_[index].data.ptr;
    }
    return ret;
  }

public:
  static const int64_t POLL_DESCRIPTOR_SIZE = 4096;
  // result of poll
  int64_t result_;
  int epoll_fd_;
  // epoll_event.event shows the listen events, if have multiple events, use bit | to set
  struct epoll_event epoll_triggered_events_[POLL_DESCRIPTOR_SIZE];

private:
  ObPollfd pfd_[POLL_DESCRIPTOR_SIZE];

  DISALLOW_COPY_AND_ASSIGN(ObPollDescriptor);
};

} // end of namespace net
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_POLL_DESCRIPTOR_H
