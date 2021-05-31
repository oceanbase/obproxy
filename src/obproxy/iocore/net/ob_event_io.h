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

#ifndef OBPROXY_EVENT_IO_H
#define OBPROXY_EVENT_IO_H

#include "iocore/net/ob_net_def.h"
#include "iocore/net/ob_net_accept.h"
#include "iocore/net/ob_poll_descriptor.h"
#include "iocore/net/ob_unix_net_vconnection.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{
class ObContinuation;
}
namespace net
{

#define USE_EDGE_TRIGGER_EPOLL  1

#define EVENTIO_NETACCEPT       1
#define EVENTIO_READWRITE_VC    2
#define EVENTIO_ASYNC_SIGNAL    3

#ifdef USE_EDGE_TRIGGER_EPOLL
#define USE_EDGE_TRIGGER 1
#define EVENTIO_READ (EPOLLIN|EPOLLET)
#define EVENTIO_WRITE (EPOLLOUT|EPOLLET)
#else
#define EVENTIO_READ EPOLLIN
#define EVENTIO_WRITE EPOLLOUT
#endif
#define EVENTIO_ERROR (EPOLLERR|EPOLLPRI|EPOLLHUP)

struct ObPollDescriptor;
class ObUnixNetVConnection;
struct ObNetAccept;

class ObEventIO
{
public:
  ObEventIO()
  {
    type_ = 0;
    data_.c_ = NULL,
    fd_ = NO_FD;
    event_loop_ = NULL;
#if !defined(USE_EDGE_TRIGGER)
    events_ = 0;
#endif
  }
  ~ObEventIO() { }

  int start(ObPollDescriptor &loop, int fd, event::ObContinuation &c, const int events);
  int start(ObPollDescriptor &loop, ObNetAccept &vc, const int events);
  int start(ObPollDescriptor &loop, ObUnixNetVConnection &vc, const int events);
  int start(ObPollDescriptor &loop, int fd, const int events);

  // Change the existing events by adding modify(EVENTIO_READ)
  // or removing modify(-EVENTIO_READ), for level triggered I/O
  int modify(const int events);

  int stop();
  int close();

public:
  int type_;
  union
  {
    event::ObContinuation *c_;
    ObUnixNetVConnection *vc_;
    ObNetAccept *na_;
  } data_;

private:
  int fd_;
#if !defined(USE_EDGE_TRIGGER)
  int events_;
#endif
  ObPollDescriptor *event_loop_;

  DISALLOW_COPY_AND_ASSIGN(ObEventIO);
};

inline int ObEventIO::start(ObPollDescriptor &loop, int fd, event::ObContinuation &c, const int events)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(fd < 3)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_NET_LOG(WARN, "invalid argument", K(fd), K(ret));
  } else {
    event_loop_ = &loop;
    fd_ = fd;
    data_.c_ = &c;

    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events = events;
    ev.data.ptr = this;
#if !defined(USE_EDGE_TRIGGER)
    events_ = events;
#endif
    if (OB_FAIL(ObSocketManager::epoll_ctl(event_loop_->epoll_fd_, EPOLL_CTL_ADD, fd_, &ev))) {
      PROXY_NET_LOG(WARN, "fail to epoll_ctl, op is EPOLL_CTL_ADD", K(fd), K(ret));
    }
  }
  return ret;
}

inline int ObEventIO::start(ObPollDescriptor &loop, ObNetAccept &na, const int events)
{
  int ret = common::OB_SUCCESS;
  type_ = EVENTIO_NETACCEPT;
  if (OB_FAIL(start(loop, na.server_.fd_, reinterpret_cast<event::ObContinuation &>(na), events))) {
    PROXY_NET_LOG(WARN, "fail to start event io", K(na.server_.fd_), K(ret));
  }
  return ret;
}

inline int ObEventIO::start(ObPollDescriptor &loop, ObUnixNetVConnection &vc, const int events)
{
  int ret = common::OB_SUCCESS;
  type_ = EVENTIO_READWRITE_VC;
  if (OB_FAIL(start(loop, vc.con_.fd_, reinterpret_cast<event::ObContinuation &>(vc), events))) {
    PROXY_NET_LOG(WARN, "fail to start event io", K(vc.con_.fd_), K(ret));
  }
  return ret;
}

inline int ObEventIO::start(ObPollDescriptor &loop, int fd, const int events)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(fd < 3)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_NET_LOG(WARN, "invalid argument", K(fd), K(ret));
  } else {
    event_loop_ = &loop;
    fd_ = fd;
    data_.c_ = NULL;

    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events = events;
    ev.data.ptr = this;
#if !defined(USE_EDGE_TRIGGER)
    events_ = events;
#endif
    if (OB_FAIL(ObSocketManager::epoll_ctl(event_loop_->epoll_fd_, EPOLL_CTL_ADD, fd_, &ev))) {
      PROXY_NET_LOG(WARN, "fail to epoll_ctl, op is EPOLL_CTL_ADD", K(fd), K(ret));
    }
  }
  return ret;
}

inline int ObEventIO::modify(const int events)
{
  UNUSED(events);
  int ret = common::OB_SUCCESS;
#if !defined(USE_EDGE_TRIGGER)
  struct epoll_event ev;
  memset(&ev, 0, sizeof(ev));

  int new_events = events_;
  int old_events = events_;

  if (events < 0) {
    new_events &= ~(-events);
  } else {
    new_events |= events;
  }

  events_ = new_events;
  ev.events_ = new_events;
  ev.data_.ptr_ = this;
  if (0 == new_events) {
    ret = ObSocketManager::epoll_ctl(event_loop_->epoll_fd_, EPOLL_CTL_DEL, fd, &ev);
  } else if (0 == old_events) {
    ret = ObSocketManager::epoll_ctl(event_loop_->epoll_fd_, EPOLL_CTL_ADD, fd, &ev);
  } else {
    ret = ObSocketManager::epoll_ctl(event_loop_->epoll_fd_, EPOLL_CTL_MOD, fd, &ev);
  }
#endif
  return ret;
}

inline int ObEventIO::stop()
{
  int ret = common::OB_SUCCESS;
  if (NULL != event_loop_) {
    struct epoll_event ev;
    memset(&ev, 0, sizeof(struct epoll_event));
    ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
    if (OB_FAIL(ObSocketManager::epoll_ctl(event_loop_->epoll_fd_, EPOLL_CTL_DEL, fd_, &ev))) {
      PROXY_NET_LOG(WARN, "fail to epoll_ctl, op is EPOLL_CTL_DEL", K(event_loop_->epoll_fd_),
                    K(fd_), K(ret));
    }
  }
  return ret;
}

inline int ObEventIO::close()
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(stop())) {
    switch (type_) {
      case EVENTIO_NETACCEPT:
        ret = data_.na_->server_.close();
        break;

      case EVENTIO_READWRITE_VC:
        ret = data_.vc_->con_.close();
        break;

      default:
        ret = common::OB_ERR_UNEXPECTED;
        PROXY_NET_LOG(WARN, "can't reach here", K(type_), K(ret));
        break;
    }
  } else {
    PROXY_NET_LOG(WARN, "fail to stop", K(type_), K(ret));
  }
  return ret;
}

} // end of namespace net
} // end of namespace obproxy
} // end of namespace oceanbase

#endif
