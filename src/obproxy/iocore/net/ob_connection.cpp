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
 */

#include "iocore/net/ob_connection.h"
#include "utils/ob_proxy_hot_upgrader.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace net
{

#define SET_TCP_NO_DELAY
#define SET_NO_LINGER

#ifndef FD_CLOEXEC
#define FD_CLOEXEC 1
#endif

/**
 * Struct to make cleaning up resources easier.
 *
 * By default, the method is invoked on the object when
 * this object is destructed. This can be prevented by calling
 * the reset method.
 *
 * This is not overly useful in the allocate, check, return case
 * but very handy if there are
 *   - multiple resources (each can have its own ObCleaner)
 *   - multiple checks against the resource
 * In such cases, rather than trying to track all the resources
 * that might need cleaned up, you can set up a ObCleaner at allocation
 * and only have to deal with them on success, which is generally
 * singular.
 *
 * @code
 * self::some_method (...) {
 *  // allocate resource
 *  ObCleaner<self> clean_up(this, &self::cleanup);
 *  // modify or check the resource
 *  if (fail) return FAILURE; // cleanup() is called
 *  // success!
 *  clean_up.reset(); // cleanup() not called after this
 *  return SUCCESS;
 *  }
 * @endcode
 */
template <typename T>
struct ObCleaner
{
  typedef void (T::*method)(); // Method signature.

  ObCleaner(T *obj, method method) : obj_(obj), m_(method) { }

  ~ObCleaner()
  {
    if (NULL != obj_) {
     (obj_->*m_)();
    }
  }

  void reset() { obj_ = NULL; }

  T *obj_; // Object instance.
  method m_;
};

ObConnection::ObConnection()
    : fd_(NO_FD), is_bound_(false), is_connected_(false), sock_type_(0)
{
  memset(&addr_, 0, sizeof(addr_));
}

ObConnection::~ObConnection()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(close()) && (NO_FD != fd_)) {
    PROXY_SOCK_LOG(WARN, "fail to close", K(fd_), K(ret));
  }
}

/**
 * Default options.
 *
 * This structure is used to reduce the number of places in
 * which the defaults are set. Originally the argument defaulted to
 * NULL which meant that the defaults had to be encoded in any
 * methods that used it as well as the ObNetVCOptions
 * constructor. Now they are controlled only in the latter and not in
 * any of the methods. This makes handling global default values
 * (such as RECV_BUF_SIZE) more robust. It doesn't have to be
 * checked in the method, only in the ObNetVCOptions constructor.
 *
 * The methods are simpler because they never have to check for the
 * presence of the options, yet the clients aren't inconvenienced
 * because a default value for the argument is provided. Further,
 * clients can pass temporaries and not have to declare a variable in
 * order to tweak options.
 */
const ObNetVCOptions ObConnection::DEFAULT_OPTIONS;
const int32_t ObConnection::SOCKOPT_ON             = 1;
const int32_t ObConnection::SOCKOPT_OFF            = 0;
const int32_t ObConnection::SNDBUF_AND_RCVBUF_PREC = 1024;

const int32_t ObServerConnection::LISTEN_BACKLOG   = 1024;

int ObConnection::open(const ObNetVCOptions &opt)
{
  int ret = OB_SUCCESS;
  int res = -1;
  int family = 0;

  // Need to do address calculations first, so we can determine the
  // address family for socket creation.
  ObIpEndpoint local_addr;

  sock_type_ = SOCK_STREAM;

  // No local address specified, so use family option if possible.
  family = ops_is_ip(opt.ip_family_) ? opt.ip_family_ : AF_INET;
  local_addr.set_to_any_addr(family);

  if (OB_FAIL(ObSocketManager::socket(family, sock_type_, 0, fd_))) {
    PROXY_SOCK_LOG(WARN, "fail to create socket", K(fd_), K(ret));
  } else {
    ObCleaner<ObConnection> cleanup(this, &ObConnection::cleanup);

    if (OB_FAIL(ObSocketManager::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR,
                                            reinterpret_cast<const void *>(&SOCKOPT_ON),
                                            sizeof(SOCKOPT_ON)))) {
      PROXY_SOCK_LOG(WARN, "fail to set socket opt enable_reuseaddr", K(fd_), K(local_addr), K(ret));
    } else if (!opt.f_blocking_connect_ && OB_FAIL(ObSocketManager::nonblocking(fd_))) {
      PROXY_SOCK_LOG(WARN, "fail to set socket nonblocking", K(fd_), K(local_addr), K(ret));
    } else if (OB_FAIL(ObSocketManager::fcntl(fd_, F_SETFD, FD_CLOEXEC, res))) {
      PROXY_SOCK_LOG(WARN, "fail to set socket FD_CLOEXEC", K(fd_), K(local_addr), K(ret));
    } else if (OB_FAIL(ObSocketManager::set_sndbuf_and_rcvbuf_size(fd_,
        opt.socket_send_bufsize_,
        opt.socket_recv_bufsize_,
        SNDBUF_AND_RCVBUF_PREC))){
      PROXY_SOCK_LOG(WARN, "fail to set_sndbuf_and_rcvbuf_size", K(fd_),
                           K(opt.socket_send_bufsize_),
                           K(opt.socket_recv_bufsize_),
                           K(local_addr),
                           K(ret));
    } else if(OB_FAIL(apply_options(opt))) {
      PROXY_SOCK_LOG(WARN, "fail to apply options", K(local_addr), K(ret));
    } else {
      cleanup.reset();
      is_bound_ = true;
    }
  }
  return ret;
}

int ObConnection::connect(const sockaddr &target, const ObNetVCOptions &opt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NO_FD == fd_)
             || OB_UNLIKELY(!is_bound_)
             || OB_UNLIKELY(is_connected_)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_SOCK_LOG(WARN, "invalid member variable",
                   K(fd_), K(is_bound_), K(is_connected_), K(ret));
  } else if (OB_FAIL(set_remote(target))){
    PROXY_SOCK_LOG(WARN, "fail to set remote", K(ret));
  } else {
    ObCleaner<ObConnection> cleanup(this, &ObConnection::cleanup);

    if (OB_FAIL(ObSocketManager::connect(fd_, &target, static_cast<int64_t>(ops_ip_size(target))))) {
      // It's only really an error if either the connect was blocking
      // or it wasn't blocking and the error was other than EINPROGRESS.
      // (Is EWOULDBLOCK ok? Does that start the connect?)
      // We also want to handle the cases where the connect blocking
      // and IO blocking differ, by turning it on or off as needed.
      if (!opt.f_blocking_connect_
          && (OB_SYS_EINPROGRESS == ret || OB_SYS_EWOULDBLOCK == ret)) {
        ret = OB_SUCCESS;
      } else {
        PROXY_SOCK_LOG(WARN, "fail to connect", K(fd_), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (opt.f_blocking_connect_ && !opt.f_blocking_) {
        if (OB_FAIL(ObSocketManager::nonblocking(fd_))) {
          PROXY_SOCK_LOG(WARN, "fail to set nonblocking", K(ret));
        }
      } else if (!opt.f_blocking_connect_ && opt.f_blocking_) {
        if (OB_FAIL(ObSocketManager::blocking(fd_))) {
          PROXY_SOCK_LOG(WARN, "fail to set blocking", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      cleanup.reset();
      is_connected_ = true;
    }
  }
  return ret;
}

int ObConnection::close()
{
  int ret = OB_SUCCESS;
  is_connected_ = false;
  is_bound_ = false;
  if (OB_UNLIKELY(fd_ < 3)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObSocketManager::close(fd_))) {
  }
  fd_ = NO_FD;
  return ret;
}

int ObConnection::apply_options(const ObNetVCOptions &opt)
{
  int ret = OB_SUCCESS;
  // Set options which can be changed after a connection is established
  // ignore other changes
  if (SOCK_STREAM == sock_type_) {
    if (opt.sockopt_flags_ & ObNetVCOptions::SOCK_OPT_NO_DELAY) {
      if (OB_FAIL(ObSocketManager::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY,
          reinterpret_cast<const void *>(&SOCKOPT_ON), sizeof(SOCKOPT_ON)))) {
        PROXY_SOCK_LOG(WARN, "fail to set socket opt TCP_NODELAY", K(fd_), K(ret));
      }
    }

    if (OB_SUCC(ret) && (opt.sockopt_flags_ & ObNetVCOptions::SOCK_OPT_KEEP_ALIVE)) {

#ifndef TCP_USER_TIMEOUT
#define TCP_USER_TIMEOUT 18
#endif
      if (OB_FAIL(ObSocketManager::setsockopt(fd_, SOL_SOCKET, SO_KEEPALIVE,
          reinterpret_cast<const void *>(&SOCKOPT_ON), sizeof(SOCKOPT_ON)))) {
        PROXY_SOCK_LOG(WARN, "fail to set socket opt SO_KEEPALIVE", K(fd_), K(ret));
      }
      if (OB_SUCC(ret) && opt.tcp_keepidle_s_ > 0) {
        if (OB_FAIL(ObSocketManager::setsockopt(fd_, IPPROTO_TCP, TCP_KEEPIDLE, &opt.tcp_keepidle_s_, sizeof(opt.tcp_keepidle_s_)))) {
          PROXY_SOCK_LOG(WARN, "fail to set socket opt TCP_KEEPIDLE", K(fd_), K(opt.tcp_keepidle_s_), K(ret));
        }
      }
      if (OB_SUCC(ret) && opt.tcp_keepintvl_s_ > 0) {
        if (OB_FAIL(ObSocketManager::setsockopt(fd_, IPPROTO_TCP, TCP_KEEPINTVL, &opt.tcp_keepintvl_s_, sizeof(opt.tcp_keepintvl_s_)))) {
          PROXY_SOCK_LOG(WARN, "fail to set socket opt TCP_KEEPINTVL", K(fd_), K(opt.tcp_keepintvl_s_), K(ret));
        }
      }
      if (OB_SUCC(ret) && opt.tcp_keepcnt_ > 0) {
        if (OB_FAIL(ObSocketManager::setsockopt(fd_, IPPROTO_TCP, TCP_KEEPCNT, &opt.tcp_keepcnt_, sizeof(opt.tcp_keepcnt_)))) {
          PROXY_SOCK_LOG(WARN, "fail to set socket opt TCP_KEEPCNT", K(fd_), K(opt.tcp_keepcnt_), K(ret));
        }
      }
      if (OB_SUCC(ret) && opt.tcp_user_timeout_s_ > 0) {
        int32_t user_timeout_ms = 1000 * opt.tcp_user_timeout_s_;
        if (OB_FAIL(ObSocketManager::setsockopt(fd_, IPPROTO_TCP, TCP_USER_TIMEOUT, &user_timeout_ms, sizeof(user_timeout_ms)))) {
          PROXY_SOCK_LOG(WARN, "fail to set socket opt TCP_USER_TIMEOUT", K(fd_), K(user_timeout_ms), K(ret));
          // ignore TCP_USER_TIMEOUT, depends on linux kernel
          ret = OB_SUCCESS;
        }
      }
      if (OB_SUCC(ret)) {
        PROXY_SOCK_LOG(DEBUG, "succ to set sock opt KEEPALIVE", K(opt.tcp_keepidle_s_), K(opt.tcp_keepintvl_s_),
                       K(opt.tcp_keepcnt_), K(opt.tcp_user_timeout_s_));
      }
    }

    if (OB_SUCC(ret) && (opt.sockopt_flags_ & ObNetVCOptions::SOCK_OPT_LINGER_ON)) {
      struct linger l;
      l.l_onoff = 1;
      l.l_linger = 0;
      if (OB_FAIL(ObSocketManager::setsockopt(fd_, SOL_SOCKET, SO_LINGER,
          reinterpret_cast<const void *>(&l), sizeof(l)))) {
        PROXY_SOCK_LOG(WARN, "fail to set socket opt SO_LINGER", K(fd_), K(ret));
      }
    }
  }

#if OB_HAS_SO_MARK
  if (OB_SUCC(ret) && OB_FAIL(ObSocketManager::setsockopt(fd_, SOL_SOCKET, SO_MARK,
      reinterpret_cast<const void *>(&opt.packet_mark_), sizeof(uint32_t)))) {
    PROXY_SOCK_LOG(WARN, "fail to set socket opt SO_MARK", K(fd_), K(ret));
  }
#endif

#if OB_HAS_IP_TOS
  if (OB_SUCC(ret)) {
    uint32_t tos = opt.packet_tos_;
    if (addr_.is_ip4()) {
      if (OB_FAIL(ObSocketManager::setsockopt(fd_, IPPROTO_IP, IP_TOS,
          reinterpret_cast<const void *>(&tos), sizeof(uint32_t)))) {
        PROXY_SOCK_LOG(WARN, "fail to set socket opt IP_TOS", K(fd_), K(ret));
      }
    } else if (addr_.is_ip6()) {
      if (OB_FAIL(ObSocketManager::setsockopt(fd_, IPPROTO_IPV6, IPV6_TCLASS,
          reinterpret_cast<const void *>(&tos), sizeof(uint32_t)))) {
        PROXY_SOCK_LOG(WARN, "fail to set socket opt IPV6_TCLASS", K(fd_), K(ret));
      }
    }
  }
#endif
  return ret;
}

int ObConnection::set_remote(const sockaddr &remote_addr)
{
  int ret = OB_SUCCESS;
  if (!ops_ip_copy(addr_, remote_addr)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_SOCK_LOG(WARN, "copy the address from src to dst", K(ret));
  }
  return ret;
}

void ObConnection::cleanup()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(close())) {
    PROXY_SOCK_LOG(WARN, "fail to close", K(ret));
  }
}

int ObServerConnection::accept(ObConnection *c)
{
  int ret = OB_SUCCESS;
  int res = -1;
  if (OB_ISNULL(c)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_SOCK_LOG(WARN, "invalid argument conn", K(c), K(ret));
  } else {
    int64_t sz = sizeof(c->addr_);
    if (OB_FAIL(ObSocketManager::accept(fd_, &c->addr_.sa_, &sz, c->fd_))) {
      if (OB_SYS_EAGAIN != ret) {
        PROXY_SOCK_LOG(WARN, "fail to accept", K(fd_), K(ret));
      }
    } else {
      PROXY_SOCK_LOG(INFO, "connection accepted", "client", c->addr_, "server", addr_, "accepted_fd", c->fd_, "listen_fd", fd_);

      if (OB_FAIL(ObSocketManager::nonblocking(c->fd_))) {
        PROXY_SOCK_LOG(WARN, "fail to set nonblocking", K(c->fd_), K(ret));
      } else if (OB_FAIL(ObSocketManager::fcntl(c->fd_, F_SETFD, FD_CLOEXEC, res))) {
        PROXY_SOCK_LOG(WARN, "fail to set FD_CLOEXEC", K(c->fd_), K(ret));
      } else {
        c->sock_type_ = SOCK_STREAM;
      }
    }
    if (OB_FAIL(ret)) {
      int close_ret = OB_SUCCESS;
      if (NO_FD != c->fd_ && OB_UNLIKELY(OB_SUCCESS != (close_ret = c->close()))) {
        PROXY_SOCK_LOG(WARN, "fail to close ObConnection", K(c->fd_), K(close_ret));
      }
    }
  }
  return ret;
}

int ObServerConnection::setup_fd_for_listen(
    const bool non_blocking,
    const int32_t recv_bufsize,
    const int32_t send_bufsize)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObSocketManager::set_sndbuf_and_rcvbuf_size(fd_,
      send_bufsize, recv_bufsize, SNDBUF_AND_RCVBUF_PREC))) {
    PROXY_SOCK_LOG(WARN, "fail to set_sndbuf_and_rcvbuf_size",
                   K(send_bufsize), K(recv_bufsize), K(ret));
  }

#ifdef SET_CLOSE_ON_EXEC_LISTEN
  int res = -1;
  if (OB_SUCC(ret) && OB_FAIL(ObSocketManager::fcntl(fd_, F_SETFD, FD_CLOEXEC, res))) {
    PROXY_SOCK_LOG(WARN, "fail to set fd FD_CLOEXEC", K(fd_), K(ret));
  }
#endif

#ifdef SET_NO_LINGER
  struct linger l;
  l.l_onoff = 0;
  l.l_linger = 0;
  if (OB_SUCC(ret) && OB_FAIL(ObSocketManager::setsockopt(fd_, SOL_SOCKET, SO_LINGER,
      reinterpret_cast<const void *>(&l), sizeof(l)))) {
    PROXY_SOCK_LOG(WARN, "fail to set sockopt SO_LINGER", K(fd_), K(ret));
  }
#endif

  if (OB_SUCC(ret) && ops_is_ip6(addr_)
      && OB_FAIL(ObSocketManager::setsockopt(fd_, IPPROTO_IPV6, IPV6_V6ONLY,
      reinterpret_cast<const void *>(&SOCKOPT_ON), sizeof(SOCKOPT_ON)))) {
    PROXY_SOCK_LOG(WARN, "fail to set sockopt IPV6_V6ONLY", K(fd_), K(ret));
  }

  if (OB_SUCC(ret) && OB_FAIL(ObSocketManager::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR,
      reinterpret_cast<const void *>(&SOCKOPT_ON), sizeof(SOCKOPT_ON)))) {
    PROXY_SOCK_LOG(WARN, "fail to set sockopt SO_REUSEADDR", K(fd_), K(ret));
  }

#ifdef SET_TCP_NO_DELAY
  if (OB_SUCC(ret) && OB_FAIL(ObSocketManager::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY,
      reinterpret_cast<const void *>(&SOCKOPT_ON), sizeof(SOCKOPT_ON)))) {
    PROXY_SOCK_LOG(WARN, "fail to set sockopt TCP_NODELAY", K(fd_), K(ret));
  }
#endif

#ifdef SET_SO_KEEPALIVE
  // enables 2 hour inactivity probes, also may fix IRIX FIN_WAIT_2 leak
  if (OB_SUCC(ret) && OB_FAIL(ObSocketManager::setsockopt(fd_, SOL_SOCKET, SO_KEEPALIVE,
      reinterpret_cast<const void *>(&SOCKOPT_ON), sizeof(SOCKOPT_ON)))) {
    PROXY_SOCK_LOG(WARN, "fail to set sockopt SO_KEEPALIVE", K(fd_), K(ret));
  }
#endif

#if defined(TCP_MAXSEG)
  if (OB_SUCC(ret) && ObNetProcessor::accept_mss_ > 0
      && OB_FAIL(ObSocketManager::setsockopt(fd_, IPPROTO_TCP, TCP_MAXSEG,
      reinterpret_cast<const void *>(&ObNetProcessor::accept_mss_),
      sizeof(int32_t)))) {
    PROXY_SOCK_LOG(WARN, "fail to set sockopt TCP_MAXSEG", K(fd_), K(ret));
  }
#endif

  if (OB_SUCC(ret) && non_blocking
      && OB_FAIL(ObSocketManager::nonblocking(fd_))) {
    PROXY_SOCK_LOG(WARN, "fail to set fd nonblocking", K(fd_), K(ret));
  }

  if (OB_FAIL(ret)) {
    // make coverity happy
    int tmp_ret = ret;
    if (OB_FAIL(close())) {
      PROXY_SOCK_LOG(WARN, "fail to close server connection", K(tmp_ret));
    }
    ret = tmp_ret;
  }
  return ret;
}

int ObServerConnection::listen(const bool non_blocking, const int32_t recv_bufsize,
                               const int32_t send_bufsize)
{
  int ret = OB_SUCCESS;
  if (!ops_is_ip(accept_addr_)) {
    ops_ip4_set(addr_, INADDR_ANY, 0);
  } else {
    ops_ip_copy(addr_, accept_addr_);
  }

  ObHotUpgraderInfo &info = get_global_hot_upgrade_info();

  if (OB_FAIL(ObSocketManager::socket(addr_.sa_.sa_family, SOCK_STREAM, IPPROTO_TCP, fd_))) {
    PROXY_SOCK_LOG(WARN, "failed to create socket", K(addr_), KERRMSGS, K(ret));
  } else if (OB_FAIL(setup_fd_for_listen(non_blocking, recv_bufsize, send_bufsize))) {
    PROXY_SOCK_LOG(WARN, "failed to setup_fd_for_listen", K(addr_), K(ret));
  } else if (OB_FAIL(ObSocketManager::bind(fd_, &addr_.sa_,
      static_cast<int64_t>(ops_ip_size(addr_.sa_))))) {
    PROXY_SOCK_LOG(WARN, "failed to bind", K(addr_), KERRMSGS, K(ret));
  } else if (OB_FAIL(ObSocketManager::listen(fd_, LISTEN_BACKLOG))) {
    PROXY_SOCK_LOG(WARN, "failed to listen", K(addr_), KERRMSGS, K(ret));
  } else {
    // Original just did this on port == 0.
    int64_t namelen = sizeof(addr_);
    if (OB_FAIL(ObSocketManager::getsockname(fd_, &addr_.sa_, &namelen))) {
      PROXY_SOCK_LOG(WARN, "failed to getsockname", K(addr_), KERRMSGS, K(ret));
    } else {
      info.fd_ = fd_;
    }
  }

  if (OB_FAIL(ret)) {
    // make coverity happy
    int tmp_ret = ret;
    if (OB_FAIL(close())) {
      PROXY_SOCK_LOG(WARN, "fail to close server connection", K(tmp_ret));
    }
    ret = tmp_ret;
  }
  return ret;
}

} // end of namespace net
} // end of namespace obproxy
} // end of namespace oceanbase
