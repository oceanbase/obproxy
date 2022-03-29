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

#ifndef OBPROXY_UNIX_NET_VCONNECTION_H
#define OBPROXY_UNIX_NET_VCONNECTION_H

#include "iocore/net/ob_net_def.h"
#include "iocore/net/ob_net_vconnection.h"
#include "iocore/net/ob_net_state.h"
#include "iocore/net/ob_connection.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace obproxy
{
namespace net
{

class ObNetHandler;
struct ObEventIO;

class ObUnixNetVConnection : public ObNetVConnection
{
public:
  // instances of ObUnixNetVConnection should be allocated
  // only from the free list using ObUnixNetVConnection::alloc().
  // The constructor is public just to avoid compile errors.
  ObUnixNetVConnection();
  virtual ~ObUnixNetVConnection() { };

  // The public interface is ObVIO::reenable()
  virtual void reenable(event::ObVIO *vio);
  virtual void reenable_re(event::ObVIO *vio);
  // Only for read vio
  // if atimeout_in == 0, this function behave like reenable(event::ObVIO *vio)
  // else if atimeout_in > 0, it will be schedule_in,
  // then do reenable(event::ObVIO *vio)
  virtual void reenable_in(event::ObVIO *vio, const ObHRTime atimeout_in);


  virtual event::ObVIO *do_io_read(event::ObContinuation *c,
                                   const int64_t nbytes,
                                   event::ObMIOBuffer *buf);
  virtual event::ObVIO *do_io_write(event::ObContinuation *c,
                                    const int64_t nbytes,
                                    event::ObIOBufferReader *buf);

  virtual void do_io_close(const int lerrno = -1);
  virtual void do_io_shutdown(const event::ShutdownHowToType howto);

  // Set the timeouts associated with this connection.
  // active_timeout is for the total elapsed time of
  // the connection.
  // inactivity_timeout is the elapsed time from the time
  // a read or a write was scheduled during which the
  // connection  was unable to sink/provide data.
  // calling these functions repeatedly resets the timeout.
  // These functions are NOT THREAD-SAFE, and may only be
  // called when handing an  event from this ObNetVConnection,
  // or the ObNetVConnection creation callback.
  virtual ObHRTime get_active_timeout() const;
  virtual int set_active_timeout(const ObHRTime timeout_in);
  virtual void cancel_active_timeout();
  virtual ObHRTime get_inactivity_timeout() const;
  virtual void set_inactivity_timeout(const ObHRTime timeout_in);
  virtual void cancel_inactivity_timeout();

  virtual void add_to_keep_alive_lru();
  virtual void remove_from_keep_alive_lru();

  virtual int set_local_addr();
  virtual bool set_remote_addr();
  virtual int set_virtual_addr();

  virtual int apply_options();

  virtual int get_conn_fd() { return con_.fd_; }


private:
  virtual bool get_data(const int32_t id, void *data); // unused !!!
  virtual int get_socket();
  virtual int set_tcp_init_cwnd(const int32_t init_cwnd);

public:
  int init();
  int close();
  void free();

  int accept_event(int event, event::ObEvent *e);
  int main_event(int event, event::ObEvent *e);

  // sync connect and notify caller the result
  int connect_up(event::ObEThread &t, int fd);

  void set_read_trigger();
  void reset_read_trigger();

  ObEventIO &get_event_io() { return *ep_; }

  void write_to_net(event::ObEThread &thread);
  void read_from_net(event::ObEThread &thread);

private:
  int start_event(int event, event::ObEvent *e);

  void read_disable();
  void write_disable();
  void read_reschedule();
  void write_reschedule();

  int read_signal_and_update(const int event);
  int write_signal_and_update(const int event);
  int read_signal_done(const int event);
  int write_signal_done(const int event);
  int read_signal_error(const int lerrno);
  int write_signal_error(const int lerrno);
  void net_activity();

  bool check_read_state();
  bool check_write_state();
  bool calculate_towrite_size(int64_t &towrite, bool &signalled);

  bool handle_read_from_net_error(event::ObEThread &thread, const int64_t total_read, const int error, const int tmp_code);
  bool handle_write_to_net_error(event::ObEThread &thread, const int64_t total_write, const int error, const int tmp_code);
  bool handle_read_from_net_success(event::ObEThread &thread, const event::ObProxyMutex *mutex,
                                    const int64_t total_read);
  bool handle_write_to_net_success(event::ObEThread &thread, const event::ObProxyMutex *mutex,
                                   const int64_t total_write, const bool signalled);

  int read_from_net_internal(event::ObMIOBuffer &iobuf, const int64_t toread, int64_t &total_read, int &tmp_code);
  int write_to_net_internal(event::ObIOBufferReader &reader, const int64_t towrite, int64_t &total_write, int &tmp_code);

public:
  enum ObVCSourceType {
    VC_ACCEPT = 0,
    VC_CONNECT,
    VC_INNER_CONNECT,
    VC_MAX_TYPE,
  };

  event::ObAction action_;
  // states:
  // 1. if  0 == closed_, mean not closed or reuse
  // 2. if -1 == closed_, mean close by do_io_close()
  // 3. if  1 == closed_, mean net_signal_and_update and 0 != recursion
  volatile int closed_;

  ObNetState read_;
  LINKM(ObUnixNetVConnection, read_, ready_link_)
  SLINKM(ObUnixNetVConnection, read_, enable_link_)

  ObNetState write_;
  LINKM(ObUnixNetVConnection, write_, ready_link_)
  SLINKM(ObUnixNetVConnection, write_, enable_link_)

  LINK(ObUnixNetVConnection, cop_link_);
  LINK(ObUnixNetVConnection, keep_alive_link_);

  ObHRTime active_timeout_in_;
  event::ObEvent *active_timeout_action_;

  ObHRTime inactivity_timeout_in_;
  ObHRTime next_inactivity_timeout_at_;

  ObHRTime reenable_read_time_at_;

  ObEventIO *ep_;
  ObNetHandler *nh_;

  uint32_t id_;

  ObIpEndpoint server_addr_; // used for Server address and port.

  union
  {
    uint32_t flags_;
#define NET_VC_SHUTDOWN_READ  1
#define NET_VC_SHUTDOWN_WRITE 2
    struct
    {
      uint32_t got_local_addr_:1;
      uint32_t shutdown_:2;
    } f_;
  };

  ObConnection con_;
  int32_t recursion_;
  ObHRTime submit_time_;
  ObVCSourceType source_type_;

public:
  enum SSLType
  {
    SSL_NONE = 0,
    SSL_CLIENT,
    SSL_SERVER,
    SSL_MAX_TYPE,
  };

  enum IOType
  {
    IO_NONE = 0,
    IO_READ,
    IO_WRITE,
  };

  int ssl_init(const SSLType ssL_type, const common::ObString &cluster_name, const common::ObString &tenant_name);
  inline bool using_ssl() const { return using_ssl_; }
  inline bool ssl_connected() const { return ssl_connected_; }
  void do_ssl_io(event::ObEThread &thread);
  void close_ssl();

private:
  int ssl_start_handshake(event::ObEThread &thread);
  int ssl_server_handshake(event::ObEThread &thread);
  int ssl_client_handshake(event::ObEThread &thread);
  void handle_ssl_err_code(const int err_code);
  void handle_ssl_want_read();
  void handle_ssl_want_write();
private:
  bool using_ssl_;
  bool ssl_connected_;
  SSLType ssl_type_;
  SSL *ssl_;
  bool can_shutdown_ssl_;
  IOType io_type_;

private:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObUnixNetVConnection);
};

typedef int (ObUnixNetVConnection::*NetVConnHandler)(int event, void *data);

inline ObHRTime ObUnixNetVConnection::get_active_timeout() const
{
  return active_timeout_in_;
}

inline void ObUnixNetVConnection::cancel_active_timeout()
{
  active_timeout_in_ = 0;
  if (NULL != active_timeout_action_) {
    PROXY_NET_LOG(DEBUG, "cancel active timeout", K(this));
    if (common::OB_SUCCESS != active_timeout_action_->cancel(this)) {
      PROXY_NET_LOG(ERROR, "fail to cancel active timeout");
    }
    active_timeout_action_ = NULL;
  }
}

inline ObHRTime ObUnixNetVConnection::get_inactivity_timeout() const
{
  return inactivity_timeout_in_;
}

inline void ObUnixNetVConnection::set_inactivity_timeout(const ObHRTime timeout)
{
  PROXY_NET_LOG(DEBUG, "set inactive timeout", K(timeout), K(this));
  inactivity_timeout_in_ = timeout;
  next_inactivity_timeout_at_ = event::get_hrtime() + timeout;
}

inline void ObUnixNetVConnection::cancel_inactivity_timeout()
{
  PROXY_NET_LOG(DEBUG, "cancel inactive timeout", K(this));
  inactivity_timeout_in_ = 0;
  next_inactivity_timeout_at_ = 0;
}

inline int ObUnixNetVConnection::set_local_addr()
{
  int ret = common::OB_SUCCESS;
  int64_t namelen = sizeof(local_addr_);
  if (OB_FAIL(ObSocketManager::getsockname(con_.fd_, &local_addr_.sa_, &namelen))) {
    PROXY_NET_LOG(ERROR, "fail to getsocketname", K(local_addr_), K(ret));
  } else {
    PROXY_NET_LOG(DEBUG, "set local addr succ", "fd", con_.fd_, K_(local_addr));
  }
  return ret;
}

inline bool ObUnixNetVConnection::set_remote_addr()
{
  bool bret = true;
  bret = ops_ip_copy(remote_addr_, con_.addr_);
  PROXY_NET_LOG(DEBUG, "set remote addr succ", "fd", con_.fd_, K_(remote_addr), K(bret));
  return bret;

}

inline int ObUnixNetVConnection::get_socket()
{
  return con_.fd_;
}

} // end of namespace net
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_UNIX_NET_VCONNECTION_H
