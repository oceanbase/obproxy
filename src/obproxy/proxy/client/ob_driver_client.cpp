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

#include "proxy/client/ob_driver_client.h"
#include "proxy/mysql/ob_mysql_client_session.h"
#include "iocore/eventsystem/ob_action.h"
#include "iocore/net/ob_socket_manager.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;

#define SET_NONBLOCKING(fd) {           \
        int tmp_ret = OB_SUCCESS;      \
        int flags = fcntl(fd, F_GETFL); \
        int res = -1;                   \
        flags |= O_NONBLOCK;            \
        if (OB_SUCCESS != (tmp_ret = ObSocketManager::fcntl(fd, F_SETFL, flags, res))) { \
          OBPROXY_DRIVER_CLIENT_LOG(WARN, "fail to set nonblocking", K(tmp_ret), K(errno), K(strerror(errno))); \
          if (OB_SUCCESS == ret) {  \
            ret = tmp_ret;  \
          } \
        }      \
}

#define SET_BLOCKING(fd) {              \
        int tmp_ret = OB_SUCCESS;       \
        int flags = fcntl(fd, F_GETFL); \
        int res = -1;                   \
        flags &= ~O_NONBLOCK;           \
        if (OB_SUCCESS != (tmp_ret = ObSocketManager::fcntl(fd, F_SETFL, flags, res))) { \
          OBPROXY_DRIVER_CLIENT_LOG(WARN, "fail to set blocking", K(ret), K(errno), K(strerror(errno))); \
          if (OB_SUCCESS == ret) { \
            ret = tmp_ret; \
          } \
        }      \
}

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

ObDriverClient::ObDriverClient() : is_inited_(false), unix_fd_(-1), connect_timeout_(0), recv_timeout_(0), send_timeout_(0), cs_id_(-1)
{
}

ObDriverClient::~ObDriverClient()
{
  destroy();
}

void ObDriverClient::destroy()
{
  is_inited_ = false;
  if (unix_fd_ != -1) {
    close(unix_fd_);
    unix_fd_ = -1;
  }
  connect_timeout_ = 0;
  send_timeout_ = 0;
  recv_timeout_ = 0;
  cs_id_ = -1;
}

int ObDriverClient::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    OBPROXY_DRIVER_CLIENT_LOG(WARN, "driver client init twice", K(this), K(ret));
  } else if (OB_FAIL(ObSocketManager::socket(AF_UNIX, SOCK_STREAM, 0, unix_fd_))) {
    OBPROXY_DRIVER_CLIENT_LOG(ERROR, "dirver client create socket failed", K(this), K(ret));
  } else {
    OBPROXY_DRIVER_CLIENT_LOG(DEBUG, "init driver client", K(this), K_(cs_id));
    is_inited_ = true;
  }
  return ret;
}

int ObDriverClient::sync_connect()
{
  int ret = OB_SUCCESS;
  struct sockaddr_un addr;
  addr.sun_family = AF_UNIX;
  memset(addr.sun_path, 0, sizeof(addr.sun_path));
  strncpy(addr.sun_path, get_global_layout().get_unix_domain_path(), sizeof(addr.sun_path) - 1);
  if (OB_FAIL(ObSocketManager::connect(unix_fd_, (struct sockaddr*)&addr, sizeof(addr)))) {
    OBPROXY_DRIVER_CLIENT_LOG(WARN, "driver client connect unix domain failed", K(this), K(errno), K(ret));
  } else {
    OBPROXY_DRIVER_CLIENT_LOG(DEBUG, "driver client connect success" , K(this));
  }
  return ret;
}

int ObDriverClient::sync_connect_with_timeout()
{
  int ret = OB_SUCCESS;
  if (connect_timeout_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    OBPROXY_DRIVER_CLIENT_LOG(WARN, "connect timeout value unexpected", K(connect_timeout_), K(ret));
  } else if (connect_timeout_ == 0) {
    ret = sync_connect();
  } else {
    int64_t timeout = connect_timeout_;
    struct sockaddr_un addr;
    addr.sun_family = AF_UNIX;
    memset(addr.sun_path, 0, sizeof(addr.sun_path));
    strncpy(addr.sun_path, get_global_layout().get_unix_domain_path(), sizeof(addr.sun_path) - 1);
    SET_NONBLOCKING(unix_fd_);

    int connect_rv = 0;
    while (OB_SUCC(ret)) {
      connect_rv = connect(unix_fd_, (struct sockaddr*)&addr, sizeof(addr));
      if (connect_rv < 0) {
        int64_t prev_time = get_current_time();
        if (errno != EINPROGRESS && errno != EWOULDBLOCK && errno != EAGAIN) {
          ret = ob_get_sys_errno();
          OBPROXY_DRIVER_CLIENT_LOG(WARN, "connect failed", K(ret));
        } else if (errno == EWOULDBLOCK || errno == EAGAIN) {
          // The unix socket encounters the above socket sleep 1ms to prevent it from being called all the time.
          // Refer to https://stackoverflow.com/questions/48222690/set-connect-timeout-on-unix-domain-socket
          usleep(1000);
          int64_t new_time = get_current_time();
          timeout -= (new_time - prev_time);
          prev_time = new_time;
          if (timeout <= 0) {
            ret = OB_TIMEOUT;
            OBPROXY_DRIVER_CLIENT_LOG(WARN ,"connect timeout", K_(connect_timeout), K(ret));
          }
        } else if (errno == EINPROGRESS) {
          while (OB_SUCC(ret)) {
            struct pollfd pfd;
            pfd.fd = unix_fd_;
            pfd.events = POLLOUT;
            connect_rv = poll(&pfd, 1, static_cast<int>(timeout));
            if (connect_rv > 0) {
              break;
            } else if (connect_rv == 0) {
              ret = OB_TIMEOUT;
              OBPROXY_DRIVER_CLIENT_LOG(WARN ,"connect timeout", K_(connect_timeout), K(ret));
            } else {
              if (errno != EINTR) {
                ret = ob_get_sys_errno();
                OBPROXY_DRIVER_CLIENT_LOG(WARN, "poll failed", K(ret));
              } else {
                int64_t new_time = get_current_time();
                timeout -= (new_time - prev_time);
                prev_time = new_time;
                if (timeout <= 0) {
                  ret = OB_TIMEOUT;
                  OBPROXY_DRIVER_CLIENT_LOG(WARN ,"connect timeout", K_(connect_timeout), K(ret));
                }
              }
            }
          }
          if (OB_SUCC(ret) && connect_rv > 0) {
            OBPROXY_DRIVER_CLIENT_LOG(DEBUG, "connect with timeout success", K_(connect_timeout), K(ret));
            break;
          }
        }
      } else {
        OBPROXY_DRIVER_CLIENT_LOG(DEBUG, "connect with timeout success", K_(connect_timeout), K(ret));
        break;
      }
    }
    SET_BLOCKING(unix_fd_);
  }

  return ret;
}

int ObDriverClient::sync_recv_internal(char *buf, int64_t buf_len, int64_t &recv_len, int recv_flags)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(NULL == buf || 0 >= buf_len)) {
    ret = OB_ERR_UNEXPECTED;
    OBPROXY_DRIVER_CLIENT_LOG(WARN, "buf is null or len is invalid", K(buf_len), K(this), K_(cs_id), K(ret));
  } else {

    do {
      recv_len = recv(unix_fd_, buf, buf_len, recv_flags);
    } while (recv_len == -1 && EINTR == errno);

    OBPROXY_DRIVER_CLIENT_LOG(DEBUG, "sync_recv_internal recv finish", K(recv_len), K(errno));

    if (EAGAIN == errno || EWOULDBLOCK == errno) {
      // will call this func again, do nothing
    } else if (-1 == recv_len) {
      ret = ob_get_sys_errno();
      OBPROXY_DRIVER_CLIENT_LOG(WARN, "recv failed", K(ret), KP(buf), K(buf_len), K(recv_len), K(recv_flags));
    } else {
      // success, do nothing
    }
  }

  return ret;
}

int ObDriverClient::sync_wait_io_or_timeout(bool is_recv)
{
  int ret = OB_SUCCESS;
  struct pollfd pfd;
  int rv = 0;
  int64_t timeout = 0;
  int64_t prev_time = get_current_time();
  int64_t new_time = 0;
    
  memset(&pfd, 0, sizeof(pfd));
  pfd.fd = unix_fd_;
  if (is_recv) {
    timeout = recv_timeout_ <= 0 ? -1 : recv_timeout_;
    pfd.events = POLLIN | POLLERR;
  } else {
    timeout = send_timeout_ <= 0 ? -1 : send_timeout_;
    pfd.events = POLLOUT;
  }

  OBPROXY_DRIVER_CLIENT_LOG(DEBUG, "sync_wait_io_or_timeout call poll", K(is_recv), K(timeout));

  do {
    // Prevent continuous interruptions, resulting in waiting
    if (timeout > 0) {
      new_time = get_current_time();
      timeout -= new_time - prev_time;
      if (timeout < 0) {
        timeout = 0;
      }
      prev_time = new_time;
    }
    rv = poll(&pfd, 1, static_cast<int>(timeout));
  } while (rv == -1 && errno == EINTR);

  if (rv < 0) {
    ret = ob_get_sys_errno();
    OBPROXY_DRIVER_CLIENT_LOG(WARN, "poll error", K(ret));
  } else if (rv == 0) {
    ret = OB_TIMEOUT;
    OBPROXY_DRIVER_CLIENT_LOG(WARN, "poll timeout", K(timeout), K(ret));
  } else {
    // recv >= 0 , do nothing
  }

  return ret;
}

int ObDriverClient::sync_recv_with_timeout(char *buf, int64_t buf_len, int64_t &recv_len)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(NULL == buf || 0 >= buf_len)) {
    ret = OB_ERR_UNEXPECTED;
    OBPROXY_DRIVER_CLIENT_LOG(WARN, "buf is null or len is invalid", K(buf_len), K(this), K_(cs_id), K(ret));
  } else {
    int recv_flags= MSG_DONTWAIT;
    recv_len = -1;

    while (OB_SUCC(ret) && -1 == recv_len) {
      if (OB_FAIL(sync_recv_internal(buf, buf_len, recv_len, recv_flags))) {
        OBPROXY_DRIVER_CLIENT_LOG(WARN, "fail to call sync_recv_internal", K(buf_len), K(recv_len), K(ret));
      } else if (-1 == recv_len && OB_FAIL(sync_wait_io_or_timeout(true))) {
        OBPROXY_DRIVER_CLIENT_LOG(WARN, "fail to call sync_wait_io_or_timeout", K(ret));
      } else {
        // do nothing
      }
    }

    if (OB_FAIL(ret)) {
      OBPROXY_DRIVER_CLIENT_LOG(WARN, "driver client recv failed", K(this), K(recv_len), K(recv_flags), K_(cs_id), K_(unix_fd), K(ret));
    }

  }

  return ret;
}

int ObDriverClient::sync_send_internal(const char *buf, int64_t buf_len, int64_t &send_len, int send_flags)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(NULL == buf || 0 >= buf_len)) {
    ret = OB_ERR_UNEXPECTED;
    OBPROXY_DRIVER_CLIENT_LOG(WARN, "buf is null or len is invalid", K(buf_len), K(this), K_(cs_id), K(ret));
  } else {

    do {
      send_len = send(unix_fd_, buf, buf_len, send_flags);
    } while (send_len == -1 && EINTR == errno);

    OBPROXY_DRIVER_CLIENT_LOG(DEBUG, "sync_send_internal send finish", K(send_len), K(errno));

    if (EAGAIN == errno || EWOULDBLOCK == errno) {
      // will call this func again, do nothing
    } else if (-1 == send_len) {
      ret = ob_get_sys_errno();
      OBPROXY_DRIVER_CLIENT_LOG(WARN, "send failed", K(ret), KP(buf), K(buf_len), K(send_len), K(send_flags));
    } else {
      // success, do nothing
    }
  }

  return ret;
}

int ObDriverClient::sync_send_with_timeout(const char *buf, int64_t buf_len, int64_t &send_len)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(NULL == buf || 0 >= buf_len)) {
    ret = OB_ERR_UNEXPECTED;
    OBPROXY_DRIVER_CLIENT_LOG(WARN, "buf is null or len is invalid", K(buf_len), K(this), K_(cs_id), K(ret));
  } else {
    int send_flags = MSG_DONTWAIT | MSG_NOSIGNAL;
    send_len = -1;

    while (OB_SUCC(ret) && -1 == send_len) {
      if (OB_FAIL(sync_send_internal(buf, buf_len, send_len, send_flags))) {
        OBPROXY_DRIVER_CLIENT_LOG(WARN, "fail to call sync_send_internal", K(buf_len), K(send_len), K(ret));
      } else if (-1 == send_len && OB_FAIL(sync_wait_io_or_timeout(false))) {
        OBPROXY_DRIVER_CLIENT_LOG(WARN, "fail to call sync_wait_io_or_timeout", K(ret));
      } else {
        // do nothing
      }
    }

    if (OB_FAIL(ret)) {
      OBPROXY_DRIVER_CLIENT_LOG(WARN, "driver client send failed", K(this), K(send_len), K(send_flags), K_(cs_id), K_(unix_fd), K(ret));
    }

  }

  return ret;
}

int64_t ObDriverClient::get_current_time()
{
  timeval tv;
  gettimeofday(&tv, NULL);
  int64_t current_time = tv.tv_sec * 1000 + tv.tv_usec / 1000;
  return current_time;
}

} // end of proxy
} // end of obproxy
} // end of oceanbase
