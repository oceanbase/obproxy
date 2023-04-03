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
 * **************************************************************
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

#ifndef OBPROXY_NET_VCONNECTION_H
#define OBPROXY_NET_VCONNECTION_H

#include "iocore/eventsystem/ob_vconnection.h"
#include "iocore/net/ob_inet.h"

namespace oceanbase
{
namespace obproxy
{
namespace net
{
/**
 * Holds client options for ObNetVConnection.
 * This class holds various options a user can specify for
 * ObNetVConnection. Various clients need many slightly different
 * features. This is an attempt to prevent out of control growth of
 * the connection method signatures. Only options of interest need to
 * be explicitly set -- the rest get sensible default values.
 *
 * @note Binding addresses is a bit complex. It is not currently
 * possible to bind indiscriminately across protocols, which means
 * any connection must commit to IPv4 or IPv6. For this reason the
 * connection logic will look at the address family of local_addr
 * even if addr_binding is ANY_ADDR and bind to any address in
 * that protocol. If it's not an IP protocol, IPv4 will be used.
 */

class ObNetVCOptions
{
public:
  ObNetVCOptions() { reset(); }
  ~ObNetVCOptions() { }

  ObNetVCOptions &operator=(const ObNetVCOptions &opt)
  {
    if (&opt != this) {
      MEMCPY(this, &opt, sizeof(opt));
    }
    return *this;
  }

  static const char *get_bind_style()
  {
    static const char *bind_style = "any";
    return bind_style;
  }

  // Reset all values to defaults.
  void reset();
  void set_sock_param(const int32_t recv_bufsize,
                      const int32_t send_bufsize,
                      const uint32_t opt_flags,
                      const uint32_t packet_mark = 0,
                      const uint32_t packet_tos = 0);
  void set_keepalive_param(const int32_t keepidle,
                           const int32_t keepintvl,
                           const int32_t keepcnt,
                           const int32_t user_timeout);
public:

  /**
   * IP address family.
   * This is used for inbound connections only if local_ip is not
   * set, which is sometimes more convenient for the client. This
   * defaults to AF_INET so if the client sets neither this nor
   * local_ip then IPv4 is used.
   * For outbound connections this is ignored and the family of the
   * remote address used.
   *
   * @note This is (inconsistently) called "domain" and "protocol" in
   * other places. "family" is used here because that's what the
   * standard IP data structures use.
   */
  uint16_t ip_family_;

  // Make the socket blocking on I/O (default: false)
  bool f_blocking_;

  // Make socket block on connect (default: false)
  bool f_blocking_connect_;

  // when use proxy mocked client, it is true (default: false)
  bool is_inner_connect_;

  int32_t socket_recv_bufsize_;

  int32_t socket_send_bufsize_;

  // tcp keep alive and user timeout
  int32_t tcp_keepidle_s_;
  int32_t tcp_keepintvl_s_;
  int32_t tcp_keepcnt_;
  int32_t tcp_user_timeout_s_;

  /**
   * Configuration options for sockets.
   * @note These are not identical to internal socket options but
   * specifically defined for configuration. These are mask values
   * and so must be powers of 2.
   */
  uint32_t sockopt_flags_;

  uint32_t packet_mark_;

  uint32_t packet_tos_;

  event::ObEventThreadType etype_;

  event::ObEThread *ethread_;

  // Value for TCP no delay for sockopt_flags.
  static const uint32_t SOCK_OPT_NO_DELAY = 1;

  // Value for keep alive for sockopt_flags.
  static const uint32_t SOCK_OPT_KEEP_ALIVE = 2;

  // Value for linger on for sockopt_flags
  static uint32_t const SOCK_OPT_LINGER_ON = 4;

private:
  ObNetVCOptions(const ObNetVCOptions &opt);
};

inline void ObNetVCOptions::reset()
{
  ip_family_ = AF_INET;
  f_blocking_ = false;
  f_blocking_connect_ = false;
  is_inner_connect_ = false;
#if defined(RECV_BUF_SIZE)
  socket_recv_bufsize_ = RECV_BUF_SIZE;
#else
  socket_recv_bufsize_ = 0;
#endif
  socket_send_bufsize_ = 0;
  tcp_keepidle_s_ = 0;
  tcp_keepintvl_s_ = 0;
  tcp_keepcnt_ = 0;
  tcp_user_timeout_s_ = 0;
  sockopt_flags_ = 0;
  packet_mark_ = 0;
  packet_tos_ = 0;

  etype_ = 0; //ET_NET; //FIXME
  ethread_ = NULL;
}

inline void ObNetVCOptions::set_sock_param(
    const int32_t recv_bufsize,
    const int32_t send_bufsize,
    const uint32_t opt_flags,
    const uint32_t packet_mark,
    const uint32_t packet_tos)
{
  socket_recv_bufsize_ = recv_bufsize;
  socket_send_bufsize_ = send_bufsize;
  sockopt_flags_ = opt_flags;
  packet_mark_ = packet_mark;
  packet_tos_ = packet_tos;
}

inline void ObNetVCOptions::set_keepalive_param(
    const int32_t keepidle,
    const int32_t keepintvl,
    const int32_t keepcnt,
    const int32_t user_timeout)
{
  tcp_keepidle_s_ = keepidle;
  tcp_keepintvl_s_ = keepintvl;
  tcp_keepcnt_ = keepcnt;
  tcp_user_timeout_s_ = user_timeout;
}

/**
 * A ObVConnection for a network socket. Abstraction for a net connection.
 * Similar to a socket descriptor VConnections are IO handles to
 * streams. In one sense, they serve a purpose similar to file
 * descriptors. Unlike file descriptors, VConnections allow for a
 * stream IO to be done based on a single read or write call.
 */
class ObNetVConnection : public event::ObVConnection
{
public:
  /**
   * PRIVATE: instances of ObNetVConnection cannot be created directly
   * by the state machines. The objects are created by ObNetProcessor
   * calls like accept connect_re() etc. The constructor is public
   * just to avoid compile errors.
   */
  ObNetVConnection();
  virtual ~ObNetVConnection() { }

  // PRIVATE: The public interface is ObVIO::reenable()
  virtual void reenable(event::ObVIO *vio) = 0;
  virtual void reenable_re(event::ObVIO *vio) = 0;

  /**
   *  Initiates read. ObThread safe, may be called when not handling
   *  an event from the ObNetVConnection, or the ObNetVConnection creation
   *  callback.
   * Callbacks: non-reentrant, c's lock taken during callbacks.
   *   c->handle_event(VC_EVENT_READ_READY, vio) data added to buffer
   *   c->handle_event(VC_EVENT_READ_COMPLETE, vio) finished reading nbytes of data
   *   c->handle_event(VC_EVENT_EOS, vio) the stream has been shutdown
   *   c->handle_event(VC_EVENT_ERROR, vio) error
   *
   * The vio returned during callbacks is the same as the one returned
   * by do_io_read(). The vio can be changed only during call backs
   * from the vconnection.
   *
   * @param c      continuation to be called back after (partial) read
   * @param nbytes no of bytes to read, if unknown set to INT64_MAX
   * @param buf    buffer to put the data into
   *
   * @return vio
   */
  virtual event::ObVIO *do_io_read(event::ObContinuation *c,
                                   const int64_t nbytes,
                                   event::ObMIOBuffer *buf) = 0;

  /**
   * Initiates write. ObThread-safe, may be called when not handling
   * an event from the ObNetVConnection, or the ObNetVConnection creation
   * callback.
   * Callbacks: non-reentrant, c's lock taken during callbacks.
   *   c->handle_event(VC_EVENT_WRITE_READY, vio)
   *     signifies data has written from the reader or there are no bytes
   *     available for the reader to write.
   *   c->handle_event(VC_EVENT_WRITE_COMPLETE, vio)
   *     signifies the amount of data indicated by nbytes has been read
   *     from the buffer
   *   c->handle_event(VC_EVENT_ERROR, vio)
   *     signified that error occurred during write.
   * The vio returned during callbacks is the same as the one returned
   * by do_io_write(). The vio can be changed only during call backs
   * from the vconnection. The vconnection deallocates the reader
   * when it is destroyed.
   *
   * @param c      continuation to be called back after (partial) write
   * @param nbytes no of bytes to write, if unknown must      be set to INT64_MAX
   * @param buf    source of data
   *
   * @return vio pointer
   */
  virtual event::ObVIO *do_io_write(event::ObContinuation *c,
                                    const int64_t nbytes,
                                    event::ObIOBufferReader *buf) = 0;

  /**
   * Closes the vconnection. A state machine MUST call do_io_close()
   * when it has finished with a VConenction. do_io_close() indicates
   * that the ObVConnection can be deallocated. After a close has been
   * called, the ObVConnection and underlying processor must NOT send
   * any more events related to this ObVConnection to the state machine.
   * Likewise, state machine must not access the VConnectuion or
   * any returned VIOs after calling close. lerrno indicates whether
   * a close is a normal close or an abort. The difference between
   * a normal close and an abort depends on the underlying type of
   * the ObVConnection. Passing ObVIO::CLOSE for lerrno indicates a
   * normal close while passing ObVIO::ABORT indicates an abort.
   *
   * @param lerrno ObVIO:CLOSE for regular close or ObVIO::ABORT for aborts
   */
  virtual void do_io_close(const int lerrno = -1) = 0;

  /**
   * Shuts down read side, write side, or both. do_io_shutdown() can
   * be used to terminate one or both sides of the ObVConnection. The
   * howto is one of IO_SHUTDOWN_READ, IO_SHUTDOWN_WRITE,
   * IO_SHUTDOWN_READWRITE. Once a side of a ObVConnection is shutdown,
   * no further I/O can be done on that side of the connections and
   * the underlying processor MUST NOT send any further events
   * (INCLUDING TIMEOUT EVENTS) to the state machine. The state machine
   * MUST NOT use any VIOs from a shutdown side of a connection.
   * Even if both sides of a connection are shutdown, the state
   * machine MUST still call do_io_close() when it wishes the
   * ObVConnection to be deallocated.
   *
   * @param howto  IO_SHUTDOWN_READ, IO_SHUTDOWN_WRITE, IO_SHUTDOWN_READWRITE
   */
  virtual void do_io_shutdown(const event::ShutdownHowToType howto) = 0;

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
  // @return the current active_timeout value in nanosecs
  virtual ObHRTime get_active_timeout() const = 0;

  /**
   * Sets time after which SM should be notified.
   *
   * Sets the amount of time (in nanoseconds) after which the state
   * machine using the ObNetVConnection should receive a
   * VC_EVENT_ACTIVE_TIMEOUT event. The timeout is value is ignored
   * if neither the read side nor the write side of the connection
   * is currently active. The timer is reset if the function is
   * called repeatedly This call can be used by SMs to make sure
   * that it does not keep any connections open for a really long
   * time.
   *
   * Timeout semantics:
   *
   * Should a timeout occur, the state machine for the read side of
   * the ObNetVConnection is signaled first assuming that a read has
   * been initiated on the ObNetVConnection and that the read side of
   * the ObNetVConnection has not been shutdown. Should either of the
   * two conditions not be met, the ObNetProcessor will attempt to
   * signal the write side. If a timeout is sent to the read side
   * state machine and its handler, return EVENT_DONE, a timeout
   * will not be sent to the write side. Should the return from the
   * handler not be EVENT_DONE and the write side state machine is
   * different (in terms of pointer comparison) from the read side
   * state machine, the ObNetProcessor will try to signal the write
   * side state machine as well. To signal write side, a write must
   * have been initiated on it and the write must not have been
   * shutdown.
   *
   * Receiving a timeout is only a notification that the timer has
   * expired. The ObNetVConnection is still usable. Further timeouts
   * of the type signaled will not be generated unless the timeout
   * is reset via the set_active_timeout() or set_inactivity_timeout()
   * interfaces.
   *
   * @param timeout_in
   */
  virtual int set_active_timeout(const ObHRTime timeout_in) = 0;

 /**
   * Clears the active timeout. No active timeouts will be sent until
   * set_active_timeout() is used to reset the active timeout.
   */
  virtual void cancel_active_timeout() = 0;

  // @return current inactivity_timeout value in nanosecs
  virtual ObHRTime get_inactivity_timeout() const = 0;

  /**
   * Sets time after which SM should be notified if the requested
   * IO could not be performed. Sets the amount of time (in nanoseconds),
   * if the ObNetVConnection is idle on both the read or write side,
   * after which the state machine using the ObNetVConnection should
   * receive a VC_EVENT_INACTIVITY_TIMEOUT event. Either read or
   * write traffic will cause timer to be reset. Calling this function
   * again also resets the timer. The timeout is value is ignored
   * if neither the read side nor the write side of the connection
   * is currently active. See section on timeout semantics above.
   *
   * @param timeout_in
   */
  virtual void set_inactivity_timeout(const ObHRTime timeout_in) = 0;

   /**
   * Clears the inactivity timeout. No inactivity timeouts will be
   * sent until set_inactivity_timeout() is used to reset the
   * inactivity timeout.
   */
  virtual void cancel_inactivity_timeout() = 0;


  virtual void add_to_keep_alive_lru() = 0;
  virtual void remove_from_keep_alive_lru() = 0;

  // Set local sock addr struct
  virtual int set_local_addr() = 0;

  // Set remote sock addr struct
  virtual bool set_remote_addr() = 0;

  // Set virtual sock addr struct
  virtual int set_virtual_addr() = 0;

  // Attempt to push any changed options down
  virtual int apply_options() = 0;

  virtual int get_conn_fd() = 0;

  // Set the TCP initial congestion window
  virtual int set_tcp_init_cwnd(const int32_t init_cwnd) = 0;

protected:
  virtual int get_socket() = 0;

public:
  // Returns local port
  uint16_t get_local_port();
  // Returns local sockaddr storage
  const sockaddr &get_local_addr();

  // Returns remote port
  uint16_t get_remote_port();
  // Returns remote sockaddr storage
  const sockaddr &get_remote_addr();

  // Returns virtual port
  uint16_t get_virtual_port();
  // Returns virtual vid(vpc id)
  uint32_t get_virtual_vid();
  // Returns virtual sockaddr storage
  const sockaddr &get_virtual_addr();
  const sockaddr &get_real_client_addr();

  // Force an event if a write operation empties the write buffer.
  //
  // This event will be sent to the VIO, the same place as other IO events.
  // Use an event value of 0 to cancel the trap.
  //
  // The event is sent only the next time the write buffer is emptied, not
  // every future time. The event is sent only if otherwise no event would
  // be generated.
  void trap_write_buffer_empty(int event = VC_EVENT_WRITE_READY);

  // for OBAPI
  bool get_is_force_timeout() const { return is_force_timeout_; }
  void set_is_force_timeout(const bool force_timeout) { is_force_timeout_ = force_timeout;}

public:
  // Structure holding user options
  ObNetVCOptions options_;
  event::ObEThread *thread_;

  // Set if the next write IO that empties the write buffer
  // should generate an event.
  int write_buffer_empty_event_;

protected:
  ObIpEndpoint local_addr_;   // it is local proxy addr
  ObIpEndpoint remote_addr_;  // if from accept, it is client addr, otherwise it is server addr
  ObIpEndpoint real_client_addr_;
  ObIpEndpoint virtual_addr_; // vip addr
  uint32_t virtual_vid_;

  bool got_local_addr_;
  bool got_remote_addr_;
  bool got_virtual_addr_;
  bool is_force_timeout_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObNetVConnection);
};

inline ObNetVConnection::ObNetVConnection()
    : event::ObVConnection(NULL),
      thread_(NULL),
      write_buffer_empty_event_(0),
      virtual_vid_(0),
      got_local_addr_(false),
      got_remote_addr_(false),
      got_virtual_addr_(false),
      is_force_timeout_(false)
{
  ob_zero(local_addr_);
  ob_zero(remote_addr_);
  ob_zero(virtual_addr_);
  ob_zero(real_client_addr_);
}

//@return The local port in host order.
inline uint16_t ObNetVConnection::get_local_port()
{
  return ops_ip_port_host_order(get_local_addr());
}

inline const sockaddr &ObNetVConnection::get_local_addr()
{
  if (!got_local_addr_) {
    set_local_addr();
    if ((ops_is_ip(local_addr_) && ops_ip_port_cast(local_addr_)) // IP and has a port.
        || (ops_is_ip4(local_addr_) && INADDR_ANY != ops_ip4_addr_cast(local_addr_)) // IPv4
        || (ops_is_ip6(local_addr_) && !IN6_IS_ADDR_UNSPECIFIED(&local_addr_.sin6_.sin6_addr))) {
      got_local_addr_ = 1;
    }
  }
  return local_addr_.sa_;
}

// @return The remote port in host order.
inline uint16_t ObNetVConnection::get_remote_port()
{
  return ops_ip_port_host_order(get_remote_addr());
}

inline const sockaddr &ObNetVConnection::get_remote_addr()
{
  if (!got_remote_addr_) {
    set_remote_addr();
    got_remote_addr_ = true;
  }
  return remote_addr_.sa_;
}

inline const sockaddr &ObNetVConnection::get_real_client_addr()
{
  if (!got_virtual_addr_) {
    set_virtual_addr();
    got_virtual_addr_ = true;
  }
  return real_client_addr_.sa_;
}

// virtual
inline const sockaddr &ObNetVConnection::get_virtual_addr()
{
  if (!got_virtual_addr_) {
    set_virtual_addr();
    got_virtual_addr_ = true;
  }
  return virtual_addr_.sa_;
}

inline uint16_t ObNetVConnection::get_virtual_port()
{
  return ops_ip_port_host_order(get_virtual_addr());
}

inline uint32_t ObNetVConnection::get_virtual_vid()
{
  if (!got_virtual_addr_) {
    set_virtual_addr();
    got_virtual_addr_ = true;
  }
  return virtual_vid_;
}

inline void ObNetVConnection::trap_write_buffer_empty(int event)
{
  write_buffer_empty_event_ = event;
}

} // end of namespace net
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_NET_VCONNECTION_H
