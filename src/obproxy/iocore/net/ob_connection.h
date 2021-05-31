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
 * Description:
 * struct ObConnection
 * struct ObServerConnection
 *
 * The accept call is a blocking call while connect is non-blocking. They
 * returns a new ObConnection instance which is an handle to the newly created
 * connection. The connection `q instance can be used later for read/writes
 * using an instance of IO Processor class.
 */

#ifndef OBPROXY_CONNECTION_H
#define OBPROXY_CONNECTION_H

#include "utils/ob_proxy_lib.h"
#include "iocore/net/ob_inet.h"
#include "iocore/net/ob_socket_manager.h"

namespace oceanbase
{
namespace obproxy
{
namespace net
{

struct ObNetVCOptions;

#define NON_BLOCKING             true
#define BLOCKING                 false

class ObConnection
{
public:
  ObConnection();
  virtual ~ObConnection();

  /**
   * Create and initialize the socket for this connection.
   * A socket is created and the options specified by opt are
   * set. The socket is not connected.
   *
   * @note It is important to pass the same opt to this method and
   * connect.
   * @param opt    Socket options.
   *
   * @return 0 on success, -ERRNO on failure.
   * @see connect
   */
  int open(const ObNetVCOptions &opt = DEFAULT_OPTIONS);

  /**
   * Connect the socket.
   *    The socket is connected to the remote addr and port. The
   *
   * opt structure is used to control blocking on the socket. All
   * other options are set via open. It is important to pass the
   * same opt to this method as was passed to open.
   * @param to     Remote address and port.
   * @param opt    Socket options.
   *
   * @return 0 on success.
   * @see open
   */
  int connect(const sockaddr &target, const ObNetVCOptions &opt = DEFAULT_OPTIONS);

  int close();

 /**
   * Set the internal socket address struct.
   * @param remote_addr Address and port.
   * @param remote_addr
   */
  int apply_options(const ObNetVCOptions &opt);

protected:
  int set_remote(const sockaddr &remote_addr);
  void cleanup();


public:
  int fd_; // Socket for connection.
  ObIpEndpoint addr_; // Associated address.
  bool is_bound_; // Flag for already bound to a local address.
  bool is_connected_; // Flag for already connected.
  int sock_type_;

protected:
  // Default options.
  static const ObNetVCOptions DEFAULT_OPTIONS;
  static const int32_t SOCKOPT_ON;
  static const int32_t SOCKOPT_OFF;
  static const int32_t SNDBUF_AND_RCVBUF_PREC;

};

class ObServerConnection : public ObConnection
{
public:
  ObServerConnection()
      : ObConnection()
  {
    ob_zero(accept_addr_);
  }
  virtual ~ObServerConnection() {};

  int accept(ObConnection *c);

  int setup_fd_for_listen(
      const bool non_blocking = false,
      const int32_t recv_bufsize = 0,
      const int32_t send_bufsize = 0);

  /**
   * Listen on a socket. We assume the port is in host by order, but
   * that the IP address (specified by accept_addr) has already been
   * converted into network byte order
   *
   * @param non_blocking
   * @param recv_bufsize
   * @param send_bufsize
   *
   * @return
   */
  int listen(const bool non_blocking = false,
             const int32_t recv_bufsize = 0,
             const int32_t send_bufsize = 0);

public:
  // Client side (inbound) local IP address.
  ObIpEndpoint accept_addr_;

private:
  static const int32_t LISTEN_BACKLOG;
};

} // end of namespace net
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_CONNECTION_H
