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

#ifndef OBPROXY_NET_PROCESSOR_H
#define OBPROXY_NET_PROCESSOR_H

#include "iocore/eventsystem/ob_event_system.h"
#include "iocore/net/ob_inet.h"

namespace oceanbase
{
namespace obproxy
{
namespace net
{

struct ObNetVCOptions;
class ObNetVConnection;

/**
 * This is the heart of the Net system. Provides common network APIs,
 * like accept, connect etc. It performs network I/O on behalf of a
 * state machine.
 */
class ObNetProcessor : public event::ObProcessor
{
public:
  // Options for accept.
  struct ObAcceptOptions
  {
    // Default constructor.
    // Instance is constructed with default values.
    ObAcceptOptions() { reset(); }
    ~ObAcceptOptions() { }
    // Reset all values to defaults.
    void reset();

    // Port on which to listen.
    // 0 => don't care, which is useful if the socket is already bound.
    int32_t local_port_;

    // Local address to bind for accept.
    // If not set -> any address.
    ObIpAddr local_ip_;

    // IP address family.
    // @note Ignored if an explicit incoming address is set in the
    // the configuration (local_ip). If neither is set IPv4 is used.
    int32_t ip_family_;

    // Should we use accept threads? If so, how many?
    int64_t accept_threads_;

    // if use accept thread, thread stacksize
    int64_t stacksize_;

    // tcp congestion window size
    int64_t tcp_init_cwnd_;

    // ObEvent type to generate on accept.
    event::ObEventThreadType etype_;

    // If true, the continuation is called back with
    // NET_EVENT_ACCEPT_SUCCEED
    // or NET_EVENT_ACCEPT_FAILED on success and failure resp.
    bool f_callback_on_open_;

    // Accept only on the loopback address.
    // Default: false.
    bool localhost_only_;

    // Are frequent accepts expected?
    // Default: true.
    bool frequent_accept_;
    bool backdoor_;

    // tcp defer accept timeout, if it set, accept until there is
    // data on the socket ready to be read. unit second.
    int64_t defer_accept_timeout_;

    // Socket receive buffer size.
    // 0 => OS default.
    int32_t recv_bufsize_;

    // Socket transmit buffer size.
    // 0 => OS default.
    int32_t send_bufsize_;

    // Socket options for sockopt.
    // 0 => do not set options.
    uint32_t sockopt_flags_;
    uint32_t packet_mark_;
    uint32_t packet_tos_;
 };

public:
  ObNetProcessor() { }
  virtual ~ObNetProcessor() { }

  /**
   * Accept connections on a port.
   * Callbacks:
   *   - cont->handle_event(NET_EVENT_ACCEPT, ObNetVConnection *) is
   *     called for each new connection
   *   - cont->handle_event(EVENT_ERROR,-errno) on a bad error
   * Re-entrant callbacks (based on callback_on_open flag):
   *   - cont->handle_event(NET_EVENT_ACCEPT_SUCCEED, 0) on successful
   *     accept init
   *   - cont->handle_event(NET_EVENT_ACCEPT_FAILED, 0) on accept
   *     init failure
   *
   * @param cont   event::ObContinuation to be called back with events this
   *               continuation is not locked on callbacks and so the handler must
   *               be re-entrant.
   * @param opt    Accept options.
   *
   * @return event::ObAction, that can be cancelled to cancel the accept. The
   *         port becomes free immediately.
   */
  virtual event::ObAction *accept(
      event::ObContinuation &cont,
      const ObAcceptOptions &opt = DEFAULT_ACCEPT_OPTIONS);

  /**
   * Accepts incoming connections on port. Accept connections on port.
   * Accept is done on all net threads and throttle limit is imposed
   * if frequent_accept flag is true. This is similar to the accept
   * method described above. The only difference is that the list
   * of parameter that is takes is limited.
   * Callbacks:
   *   - cont->handle_event(NET_EVENT_ACCEPT, ObNetVConnection *) is called for each new connection
   *   - cont->handle_event(EVENT_ERROR, -errno) on a bad error
   * Re-entrant callbacks (based on callback_on_open flag):
   *   - cont->handle_event(NET_EVENT_ACCEPT_SUCCEED, 0) on successful accept init
   *   - cont->handle_event(NET_EVENT_ACCEPT_FAILED, 0) on accept init failure
   *
   * @param cont   event::ObContinuation to be called back with events this
   *               continuation is not locked on callbacks and so the handler must
   *               be re-entrant.
   * @param listen_socket_in
   *               if passed, used for listening.
   * @param opt    Accept options.
   *
   * @return event::ObAction, that can be cancelled to cancel the accept. The
   *         port becomes free immediately.
   */
  virtual event::ObAction *main_accept(
      event::ObContinuation &cont,
      int listen_fd,
      const ObAcceptOptions &opt = DEFAULT_ACCEPT_OPTIONS);

  /**
   * Open a ObNetVConnection for connection oriented I/O. Connects
   * through sockserver if netprocessor is configured to use socks
   * or is socks parameters to the call are set.
   * Re-entrant callbacks:
   *   - On success calls: c->handle_event(NET_EVENT_OPEN, ObNetVConnection *)
   *   - On failure calls: c->handle_event(NET_EVENT_OPEN_FAILED, -errno)
   *
   * @note Connection may not have been established when cont is
   *   call back with success. If this behaviour is desired use
   *   synchronous connect connect method.
   * @param cont    event::ObContinuation to be called back with events.
   * @param addr    target address and port to connect to.
   * @param options @see ObNetVCOptions.
   *
   * @return
   * @see connect()
   */
  virtual int connect_re(
      event::ObContinuation &cont,
      const sockaddr &addr,
      ObNetVCOptions *options = NULL);

  /**
   * Open a ObNetVConnection for connection oriented I/O. This call
   * is similar to connect method except that the cont is called
   * back only after the connections has been established. In the
   * case of connect the cont could be called back with NET_EVENT_OPEN
   * event and OS could still be in the process of establishing the
   * connection. Re-entrant Callbacks: same as connect. If unix
   * asynchronous type connect is desired use connect_re().
   *
   * @param cont    event::ObContinuation to be called back with events.
   * @param addr    Address to which to connect (includes port).
   * @param timeout for connect, the cont will get NET_EVENT_OPEN_FAILED
   *                if connection could not be established for timeout msecs. The
   *                default is 30 secs.
   * @param opts
   *
   * @return
   * @see connect_re()
   */
  virtual int connect(
      event::ObContinuation &cont,
      const sockaddr &addr,
      event::ObAction *&action,
      const int64_t timeout = NET_CONNECT_TIMEOUT,
      ObNetVCOptions *opts = NULL);

  /**
   * Starts the ObNetProcessor. This has to be called before doing any
   * other net call.
   *
   * @param number_of_net_threads
   *                  is not used. The net processor
   *                  uses the ObEvent ObProcessor threads for its activity.
   * @param stacksize
   *
   * @return
   */
  virtual int start() = 0;

  virtual ObNetVConnection *allocate_vc() = 0;

public:
  // This is MSS for connections we accept (client connections).
  static int accept_mss_;

private:
  // Default options instance.
  static const ObAcceptOptions DEFAULT_ACCEPT_OPTIONS;
  static const int64_t NET_CONNECT_TIMEOUT = (30 * HRTIME_SECOND);

  DISALLOW_COPY_AND_ASSIGN(ObNetProcessor);
};

/**
 * Global ObNetProcessor singleton object for making net calls. All
 * net processor calls like connect, accept, etc are made using this
 * object.
 *
 * @code
 *   g_net_processor.accept(my_cont, ...);
 *   g_net_processor.connect_re(my_cont, ...);
 * @endcode
 */
extern ObNetProcessor &g_net_processor;

} // end of namespace net
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_NET_PROCESSOR_H
