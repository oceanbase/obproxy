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

#ifndef OBPROXY_SOCKET_MANAGER_H
#define OBPROXY_SOCKET_MANAGER_H

#include <openssl/ssl.h>
#include <openssl/err.h>

#include "iocore/net/ob_net_def.h"

namespace oceanbase
{
namespace obproxy
{
namespace net
{

class ObSocketManager
{
  ObSocketManager() { };
  ~ObSocketManager() { };

  public:
  static int poll(struct pollfd *pfd, const unsigned long npfd,
                  const int timeout, int64_t &count);

  static int epoll_create(const int size, int &epfd);
  static int epoll_ctl(int epfd, const int op, int fd, struct epoll_event *event);
  static int epoll_wait(int epfd, struct epoll_event *events,
                        const int maxevents, const int timeout, int64_t &count);

  static int socket(const int domain, const int type, const int protocol, int &sockfd);
  static int accept(int sockfd, struct sockaddr *addr, int64_t *addrlen, int &fd, bool need_return_eintr = false);
  static int bind(int sockfd, const struct sockaddr *name, const int64_t namelen);
  static int listen(int sockfd, const int backlog);
  static int connect(int sockfd, const struct sockaddr *addr, const int64_t addrlen);
  static int shutdown(int sockfd, const int how);
  static int close(int sockfd);

  static int read(int sockfd, void *buf, const int64_t size, int64_t &count);
  static int readv(int sockfd, struct iovec *vector, const int size, int64_t &count);

  static int write(int sockfd, const void *buf, const int64_t size, int64_t &count);
  static int writev(int sockfd, const struct iovec *vector, const int size, int64_t &count);


  static int fcntl(int sockfd, const int cmd, const int arg, int &result);
  static int set_fl(int sockfd, const int arg, int &result);
  static int clr_fl(int sockfd, const int arg, int &result);

  static int setsockopt(int sockfd, const int level, const int optname,
                        const void *optval, const int optlen);
  static int getsockopt(int sockfd, const int level, const int optname,
                        void *optval, int *optlen);

  static int blocking(int sockfd);
  static int nonblocking(int sockfd);

  static int getsockname(int sockfd, struct sockaddr *name, int64_t *namelen);

  static int get_sndbuf_size(int sockfd, int64_t &size);
  static int get_rcvbuf_size(int sockfd, int64_t &size);
  static int set_sndbuf_size(int sockfd, const int size);
  static int set_rcvbuf_size(int sockfd, const int size);
  static int set_sndbuf_and_rcvbuf_size(int sockfd, const int sndbuf_size, const int rcvbuf_size,
                                        const int prec);

  static int getpeername(int sockfd, struct sockaddr *addr, int64_t *addrlen);

  static int ssl_accept(SSL *ssl, bool &is_connected, int &tmp_code);
  static int ssl_connect(SSL *ssl, bool &is_connected, int &tmp_code);
  static int ssl_read(SSL *ssl, void *buf, const size_t size, int64_t &count, int &tmp_code);
  static int ssl_write(SSL *ssl, const void *buf, const size_t size, int64_t &count, int &tmp_code);
  static int handle_ssl_error(int tmp_code);

private:
  DISALLOW_COPY_AND_ASSIGN(ObSocketManager);
};

inline int ObSocketManager::poll(struct pollfd *pfd, const unsigned long npfd,
                                 const int timeout, int64_t &count)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(pfd)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    do {
      count = ::poll(pfd, npfd, timeout);
    } while (count < 0 && EINTR == errno);
    if (OB_UNLIKELY(count < 0)) {
      ret = ob_get_sys_errno();
    }
  }
  return ret;
}

inline int ObSocketManager::epoll_create(const int size, int &epfd)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(size <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    epfd = ::epoll_create(size);
    if (OB_UNLIKELY(epfd < 0)) {
      ret = ob_get_sys_errno();
    }
  }
  return ret;
}

inline int ObSocketManager::epoll_ctl(int epfd, const int op, int fd, struct epoll_event *event)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(epfd < 3) || OB_UNLIKELY(fd < 3) || (EPOLL_CTL_DEL != op && OB_ISNULL(event))) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    ret = ::epoll_ctl(epfd, op, fd, event);
    if (OB_UNLIKELY(ret < 0)) {
      ret = ob_get_sys_errno();
    }
  }
  return ret;
}

inline int ObSocketManager::epoll_wait(int epfd, struct epoll_event *events,
                                         const int maxevents, const int timeout, int64_t &count)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(epfd < 3) || OB_ISNULL(events) || OB_UNLIKELY(maxevents < 0)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    do {
      count = ::epoll_wait(epfd, events, maxevents, timeout);
    } while (count < 0 && EINTR == errno);
    if (OB_UNLIKELY(count < 0)) {
      ret = ob_get_sys_errno();
    }
  }
  return ret;
}

inline int ObSocketManager::socket(const int domain, const int type, const int protocol, int &sockfd)
{
  int ret = common::OB_SUCCESS;
  sockfd = ::socket(domain, type, protocol);
  if (OB_UNLIKELY(sockfd < 0)) {
    ret = ob_get_sys_errno();
  }
  return ret;
}

inline int ObSocketManager::accept(int sockfd, struct sockaddr *addr, int64_t *addrlen, int &fd, bool need_return_eintr /* false */)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(sockfd < 3) || OB_ISNULL(addr) || OB_ISNULL(addrlen)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    do {
      fd = ::accept(sockfd, addr, reinterpret_cast<socklen_t *>(addrlen));
    } while (fd < 0 && EINTR == errno && !need_return_eintr);
    if (OB_UNLIKELY(fd < 0)) {
      ret = ob_get_sys_errno();
    }
  }
  return ret;
}

inline int ObSocketManager::bind(int sockfd, const struct sockaddr *name, const int64_t namelen)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(name) || OB_UNLIKELY(namelen <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    ret = ::bind(sockfd, name, static_cast<socklen_t>(namelen));
    if (OB_UNLIKELY(ret < 0)) {
      ret = ob_get_sys_errno();
    }
  }
  return ret;
}

inline int ObSocketManager::listen(int sockfd, const int backlog)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(sockfd < 3) || OB_UNLIKELY(backlog < 0)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    ret = ::listen(sockfd, backlog);
    if (OB_UNLIKELY(ret < 0)) {
      ret = ob_get_sys_errno();
    }
  }
  return ret;
}

inline int ObSocketManager::connect(int sockfd, const struct sockaddr *addr, const int64_t addrlen)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(sockfd < 3) || OB_ISNULL(addr) || OB_UNLIKELY(addrlen < 0)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    ret = ::connect(sockfd, addr, static_cast<socklen_t>(addrlen));
    if (OB_UNLIKELY(ret < 0)) {
      ret = ob_get_sys_errno();
    }
  }
  return ret;
}

inline int ObSocketManager::shutdown(int sockfd, const int how)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(sockfd < 3)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    ret = ::shutdown(sockfd, how);
    if (OB_UNLIKELY(ret < 0)) {
      ret = ob_get_sys_errno();
    }
  }
  return ret;
}


inline int ObSocketManager::close(int sockfd)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(sockfd < 3)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    do {
      ret = ::close(sockfd);
    } while (ret < 0 && EINTR == errno);
    if (OB_UNLIKELY(ret < 0)) {
      ret = ob_get_sys_errno();
    }
  }
  return ret;
}

inline int ObSocketManager::read(int sockfd, void *buf, const int64_t size, int64_t &count)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(sockfd < 3) || OB_ISNULL(buf) || OB_UNLIKELY(size < 0)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    count = ::read(sockfd, buf, size);
    if (OB_UNLIKELY(count < 0)) {
      ret = ob_get_sys_errno();
    }
  }
  return ret;
}

inline int ObSocketManager::readv(int sockfd, struct iovec *vector, const int size, int64_t &count)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(sockfd < 3) || OB_ISNULL(vector) || OB_UNLIKELY(size < 0)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    count = ::readv(sockfd, vector, size);
    if (OB_UNLIKELY(count < 0)) {
      ret = ob_get_sys_errno();
    }
  }
  return ret;
}

inline int ObSocketManager::write(int sockfd, const void *buf, const int64_t size, int64_t &count)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(sockfd < 3) || OB_ISNULL(buf) || OB_UNLIKELY(size < 0)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    count = ::write(sockfd, buf, size);
    if (OB_UNLIKELY(count < 0)) {
      ret = ob_get_sys_errno();
    }
  }
  return ret;
}

inline int ObSocketManager::writev(int sockfd, const struct iovec *vector, const int size, int64_t &count)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(sockfd < 3) || OB_ISNULL(vector) || OB_UNLIKELY(size < 0)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    count = ::writev(sockfd, vector, size);
    if (OB_UNLIKELY(count < 0)) {
      ret = ob_get_sys_errno();
    }
  }
  return ret;
}

inline int ObSocketManager::fcntl(int sockfd, const int cmd, const int arg, int &result)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(sockfd < 3)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    do {
      result = ::fcntl(sockfd, cmd, arg);
    } while (result < 0 && (EAGAIN == errno || EINTR == errno));
    if (OB_UNLIKELY(result < 0)) {
      ret = ob_get_sys_errno();
    }
  }
  return ret;
}

inline int ObSocketManager::set_fl(int sockfd, const int arg, int &result)
{
  int ret = common::OB_SUCCESS;
  int old_flags = -1;
  if (OB_UNLIKELY(sockfd < 3)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_NET_LOG(WARN, "invalid argument", K(sockfd), K(ret));
  } else {
    if (OB_FAIL(fcntl(sockfd, F_GETFL, 0, old_flags))) {
      PROXY_NET_LOG(ERROR, "fail to fcntl", K(sockfd), K(ret), KERRMSGS);
    } else {
      old_flags |= arg;
      if (OB_FAIL(fcntl(sockfd, F_SETFL, old_flags, result))) {
        PROXY_NET_LOG(ERROR, "fail to fcntl", K(sockfd), K(ret), KERRMSGS);
      }
    }
  }
  return ret;
}

inline int ObSocketManager::clr_fl(int sockfd, const int arg, int &result)
{
  int ret = common::OB_SUCCESS;
  int old_flags = -1;
  if (OB_UNLIKELY(sockfd < 3)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_NET_LOG(WARN, "invalid argument", K(sockfd), K(ret));
  } else {
    if (OB_FAIL(fcntl(sockfd, F_GETFL, 0, old_flags))) {
      PROXY_NET_LOG(ERROR, "fail to fcntl", K(sockfd), K(ret), KERRMSGS);
    } else {
      old_flags &= ~arg;
      if (OB_FAIL(fcntl(sockfd, F_SETFL, old_flags, result))) {
        PROXY_NET_LOG(ERROR, "fail to fcntl", K(sockfd), K(ret), KERRMSGS);
      }
    }
  }
  return ret;
}

inline int ObSocketManager::setsockopt(int sockfd, const int level, const int optname,
                                         const void *optval, const int optlen)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(sockfd < 3) || OB_ISNULL(optval)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    ret = ::setsockopt(sockfd, level, optname, optval, optlen);
    if (OB_UNLIKELY(ret < 0)) {
      ret = ob_get_sys_errno();
    }
  }
  return ret;
}

inline int ObSocketManager::getsockopt(int sockfd, const int level, const int optname,
                                         void *optval, int *optlen)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(sockfd < 3) || OB_ISNULL(optval) || OB_ISNULL(optlen)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    ret = ::getsockopt(sockfd, level, optname, optval, (socklen_t *)optlen);
    if (OB_UNLIKELY(ret < 0)) {
      ret = ob_get_sys_errno();
    }
  }
  return ret;
}

inline int ObSocketManager::blocking(int sockfd)
{
  int ret = common::OB_SUCCESS;
  int flags = -1;
  if (OB_UNLIKELY(sockfd < 3)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_NET_LOG(WARN, "invalid argument", K(sockfd), K(ret));
  } else if (OB_FAIL(clr_fl(sockfd, O_NONBLOCK, flags))) {
    PROXY_NET_LOG(ERROR, "fail to clr_fl", K(sockfd), K(ret), KERRMSGS);
  }
  return ret;
}

inline int ObSocketManager::nonblocking(int sockfd)
{
  int ret = common::OB_SUCCESS;
  int flags = -1;
  if (OB_UNLIKELY(sockfd < 3)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_NET_LOG(WARN, "invalid argument", K(sockfd), K(ret));
  } else if (OB_FAIL(set_fl(sockfd, O_NONBLOCK, flags))) {
    PROXY_NET_LOG(ERROR, "fail to set_fl", K(sockfd), K(ret), KERRMSGS);
  }
  return ret;
}

inline int ObSocketManager::getsockname(int sockfd, struct sockaddr *name, int64_t *namelen)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(sockfd < 3) || OB_ISNULL(name) || OB_ISNULL(namelen)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    ret = ::getsockname(sockfd, name, reinterpret_cast<socklen_t *>(namelen));
    if (OB_UNLIKELY(ret < 0)) {
      ret = ob_get_sys_errno();
    }
  }
  return ret;
}

inline int ObSocketManager::get_sndbuf_size(int sockfd, int64_t &size)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(sockfd < 3)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_NET_LOG(WARN, "invalid argument", K(sockfd), K(ret));
  } else {
    int optval = 0;
    int optlen = sizeof(optval);
    ret = getsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<void *>(&optval), &optlen);
    if (OB_UNLIKELY(ret < 0)) {
      ret = ob_get_sys_errno();
      PROXY_NET_LOG(ERROR, "fail to getsockopt", K(sockfd), K(ret), KERRMSGS);
    } else {
      size = optval;
    }
  }
  return ret;
}

inline int ObSocketManager::get_rcvbuf_size(int sockfd, int64_t &size)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(sockfd < 3)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_NET_LOG(WARN, "invalid argument", K(sockfd), K(ret));
  } else {
    int optval = 0;
    int optlen = sizeof(optval);
    ret = getsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<void *>(&optval), &optlen);
    if (OB_UNLIKELY(ret < 0)) {
      ret = ob_get_sys_errno();
      PROXY_NET_LOG(ERROR, "fail to getsockopt", K(sockfd), K(ret), KERRMSGS);
    } else {
      size = optval;
    }
  }
  return ret;
}

inline int ObSocketManager::set_sndbuf_size(int sockfd, const int size)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(sockfd < 3) || OB_UNLIKELY(size < 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_NET_LOG(WARN, "invalid argument", K(sockfd), K(ret));
  } else {
    ret = setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<const void *>(&size), sizeof(size));
    if (OB_UNLIKELY(ret < 0)) {
      ret = ob_get_sys_errno();
      PROXY_NET_LOG(ERROR, "fail to setsockopt", K(sockfd), K(ret), KERRMSGS);
    }
  }
  return ret;
}

inline int ObSocketManager::set_rcvbuf_size(int sockfd, const int size)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(sockfd < 3) || OB_UNLIKELY(size < 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_NET_LOG(WARN, "invalid argument", K(sockfd), K(ret));
  } else {
    ret = setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<const void *>(&size), sizeof(size));
    if (OB_UNLIKELY(ret < 0)) {
      ret = ob_get_sys_errno();
      PROXY_NET_LOG(ERROR, "fail to setsockopt", K(sockfd), K(ret), KERRMSGS);
    }
  }
  return ret;

}

inline int ObSocketManager::set_sndbuf_and_rcvbuf_size(int sockfd, const int sndbuf_size, const int rcvbuf_size,
                                                         const int prec)
{
  int ret = common::OB_SUCCESS;
  int64_t bufsz = 0;
  if (OB_UNLIKELY(prec <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_NET_LOG(WARN, "invalid argument", K(sockfd), K(ret));
  } else {
    if (OB_LIKELY(sndbuf_size > 0)) {
      bufsz = ob_roundup(sndbuf_size, prec);
      while (bufsz > 0 && OB_FAIL(set_sndbuf_size(sockfd, sndbuf_size))) {
        bufsz -= 1024;
      }
    }
    bufsz = 0;
    if (OB_SUCC(ret)) {
      if (OB_LIKELY(rcvbuf_size > 0)) {
        bufsz = ob_roundup(rcvbuf_size, prec);
        while (bufsz > 0 && OB_FAIL(set_rcvbuf_size(sockfd, rcvbuf_size))) {
          bufsz -= 1024;
        }
      }
    }
  }
  return ret;
}

inline int ObSocketManager::getpeername(int sockfd, struct sockaddr *addr, int64_t *addrlen)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(sockfd < 3) || OB_ISNULL(addr) || OB_ISNULL(addrlen)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    ret = ::getpeername(sockfd, addr, reinterpret_cast<socklen_t *>(addrlen));
    if (OB_UNLIKELY(ret < 0)) {
      ret = ob_get_sys_errno();
    }
  }
  return ret;
}

int ObSocketManager::ssl_accept(SSL *ssl, bool &is_connected, int &tmp_code)
{
  int ret = common::OB_SUCCESS;

  if (OB_ISNULL(ssl) || is_connected) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_NET_LOG(WARN, "invalid argument", K(ret), K(is_connected));
  } else {
    int tmp_ret = SSL_accept(ssl);
    if (1 != tmp_ret) {
      tmp_code = SSL_get_error(ssl, tmp_ret);
      if (SSL_ERROR_NONE == tmp_code) {
        PROXY_NET_LOG(DEBUG, "ssl server accept succ", K(ret));
        is_connected = true;
      } else {
        ret = handle_ssl_error(tmp_code);
      }
    } else {
      is_connected = true;
      PROXY_NET_LOG(DEBUG, "ssl server accept succ", K(ret));
    }
  }

  return ret;
}

int ObSocketManager::ssl_connect(SSL *ssl, bool &is_connected, int &tmp_code)
{
  int ret = common::OB_SUCCESS;
  
  if (OB_ISNULL(ssl) || is_connected) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_NET_LOG(WARN, "invalid argument", K(ret), K(is_connected));
  } else {
    int tmp_ret = SSL_connect(ssl);
    if (1 != tmp_ret) {
      tmp_code = SSL_get_error(ssl, tmp_ret);
      if (SSL_ERROR_NONE == tmp_code) {
        PROXY_NET_LOG(DEBUG, "ssl client connect succ", K(ret));
        is_connected = true;
      } else {
        ret = handle_ssl_error(tmp_code);
      }
    } else {
      is_connected = true;
      PROXY_NET_LOG(DEBUG, "ssl client connect succ", K(ret));
    }
  }

  return ret;
}

int ObSocketManager::ssl_read(SSL *ssl, void *buf, const size_t size,
                              int64_t &count, int &tmp_code)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(ssl) || OB_ISNULL(buf)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (0 == size) {
    //do nothing
  } else {
    bool last_succ = count > 0 ? true : false;
    count = SSL_read(ssl, buf, static_cast<int>(size));
    if (count <= 0 && !last_succ) {
      tmp_code = SSL_get_error(ssl, static_cast<int>(count));
      ret = handle_ssl_error(tmp_code);
      count = 0;
    } else {
      PROXY_NET_LOG(DEBUG, "ssl read succ", K(count));
    }
  }
  return ret;
}

int ObSocketManager::ssl_write(SSL *ssl, const void *buf, const size_t size,
                               int64_t &count, int &tmp_code)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(ssl) || OB_ISNULL(buf)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (0 == size) {
    //do nothing
  } else {
    bool last_succ  = count > 0 ? true : false;
    count = SSL_write(ssl, buf, static_cast<int>(size));
    if (count <= 0 && !last_succ) {
      tmp_code = SSL_get_error(ssl, static_cast<int>(count));
      ret = handle_ssl_error(tmp_code);
      count = 0;
    } else {
      PROXY_NET_LOG(DEBUG, "ssl write succ", K(count));
    }
  }
  return ret;
}

inline int ObSocketManager::handle_ssl_error(int tmp_code)
{
  int ret = common::OB_SUCCESS;
  switch(tmp_code) {
    case SSL_ERROR_WANT_WRITE:
    case SSL_ERROR_WANT_READ:
      break;
    case SSL_ERROR_ZERO_RETURN:
      ret = common::OB_SSL_ERROR;
      PROXY_NET_LOG(WARN, "the ssl peer has closed", K(ret));
      break;
    default:
      ret = common::OB_SSL_ERROR;
      char temp_buf[512];
      unsigned long e = ERR_peek_last_error();
      ERR_error_string_n(e, temp_buf, sizeof(temp_buf));
      PROXY_NET_LOG(WARN, "SSL error", K(e), K(temp_buf), K(errno), K(strerror(errno)));
      break;
  }

  return ret;
}

} // end of namespace net
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_SOCKET_MANAGER_H
