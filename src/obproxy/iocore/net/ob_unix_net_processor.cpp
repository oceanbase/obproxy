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

#include "iocore/net/ob_unix_net_processor.h"
#include "iocore/net/ob_net.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;


namespace oceanbase
{
namespace obproxy
{
namespace net
{

ObUnixNetProcessor g_unix_net_processor;
ObNetProcessor &g_net_processor = g_unix_net_processor;

int ObNetProcessor::accept_mss_ = 0;
const ObNetProcessor::ObAcceptOptions ObNetProcessor::DEFAULT_ACCEPT_OPTIONS;


uint32_t net_next_connection_number()
{
  static uint32_t net_connection_number = 1;

  uint32_t ret = 0;
  do {
    ret = ATOMIC_FAA(&net_connection_number, 1);
  } while (0 == ret);
  return ret;
}

void ObNetProcessor::ObAcceptOptions::reset()
{
  local_port_ = 0;
  local_ip_.invalidate();
  accept_threads_ = -1;
  stacksize_ = 0;
  tcp_init_cwnd_ = 0;
  ip_family_ = AF_INET;
  etype_ = ET_NET;
  f_callback_on_open_ = false;
  localhost_only_ = false;
  frequent_accept_ = true;
  backdoor_ = false;
  defer_accept_timeout_ = 0;
  recv_bufsize_ = 0;
  send_bufsize_ = 0;
  sockopt_flags_ = 0;
  packet_mark_ = 0;
  packet_tos_ = 0;
}

ObAction *ObNetProcessor::accept(ObContinuation &cont, const ObAcceptOptions &opt)
{
  PROXY_NET_LOG(DEBUG, "ObNetProcessor::accept",
                "port", opt.local_port_,
                "recv_bufsize", opt.recv_bufsize_,
                "send_bufsize", opt.send_bufsize_,
                "sockopt", opt.sockopt_flags_);
  return static_cast<ObUnixNetProcessor *>(this)->accept_internal(cont, NO_FD, opt);
}

ObAction *ObNetProcessor::main_accept(ObContinuation &cont, int fd, const ObAcceptOptions &opt)
{
  PROXY_NET_LOG(DEBUG, "ObNetProcessor::accept",
                "port", opt.local_port_,
                "recv_bufsize", opt.recv_bufsize_,
                "send_bufsize", opt.send_bufsize_,
                "sockopt", opt.sockopt_flags_);

  return static_cast<ObUnixNetProcessor *>(this)->accept_internal(cont, fd, opt);
}

inline ObAction *ObUnixNetProcessor::accept_internal(ObContinuation &cont, int fd,
                                                     const ObAcceptOptions &opt)
{
  int ret = OB_SUCCESS;
  ObEventThreadType upgraded_etype = opt.etype_; // set etype requires non-const ref.
  int64_t accept_threads_ = opt.accept_threads_; // might be changed.
  ObIpEndpoint accept_ip; // local binding address.
  char thr_name[MAX_THREAD_NAME_LENGTH];
  ObNetAccept *na = NULL;
  ObAction *action_ret = NULL;

  if (OB_ISNULL(na = create_net_accept())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_NET_LOG(ERROR, "fail to create_net_accept", K(ret));
  } else if (OB_UNLIKELY(opt.local_port_ < 0) || OB_UNLIKELY(opt.local_port_ > 65536)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_NET_LOG(ERROR, "invalid argument", K(opt.local_port_));
  } else if (OB_ISNULL(na->action_ = new(std::nothrow) ObNetAcceptAction())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_NET_LOG(ERROR, "fail to new ObNetAcceptAction", K(ret));
  } else {
    // Potentially upgrade to SSL.
    upgrade_etype(upgraded_etype);

    // We've handled the config stuff at start up, but there are a few cases
    // we must handle at this point.
    if (opt.localhost_only_) {
      accept_ip.set_to_loopback(opt.ip_family_);
    } else if (opt.local_ip_.is_valid()) {
      accept_ip.assign(opt.local_ip_);
    } else {
      accept_ip.set_to_any_addr(opt.ip_family_);
    }
    accept_ip.port() = ops_port_net_order(opt.local_port_);

    na->accept_fn_ = net_accept;
    na->server_.fd_ = fd;
    ops_ip_copy(na->server_.accept_addr_, accept_ip);

    na->action_->set_continuation(&cont);
    na->action_->server_ = &na->server_;
    na->callback_on_open_ = opt.f_callback_on_open_;
    na->recv_bufsize_ = opt.recv_bufsize_;
    na->send_bufsize_ = opt.send_bufsize_;
    na->sockopt_flags_ = opt.sockopt_flags_;
    na->packet_mark_ = opt.packet_mark_;
    na->packet_tos_ = opt.packet_tos_;
    na->etype_ = upgraded_etype;
    na->backdoor_ = opt.backdoor_;
    if (na->callback_on_open_) {
      na->mutex_ = cont.mutex_;
    }

    ObNetAccept *net_accept = NULL;
    int64_t ret_len = 0;
    if (opt.frequent_accept_) {
      if (accept_threads_ > 0) {
        if (OB_FAIL(na->do_listen(BLOCKING))) {
          PROXY_NET_LOG(ERROR, "fail to do_listen BLOCKING", K(ret));
        } else {
          for (int64_t i = 1; (i < accept_threads_) && OB_SUCC(ret); ++i) {
            if (OB_ISNULL(net_accept = new (std::nothrow) ObNetAccept())){
              ret = OB_ALLOCATE_MEMORY_FAILED;
              PROXY_NET_LOG(ERROR, "fail to new ObNetAccept", K(ret));
            } else if (OB_FAIL(net_accept->deep_copy(*na))) {
              PROXY_NET_LOG(ERROR, "fail to deep_copy ObNetAccept", K(i), K(ret));
            } else {
              ret_len = snprintf(thr_name, MAX_THREAD_NAME_LENGTH, "[ACCEPT %ld:%d]", i - 1,
                                 ops_ip_port_host_order(accept_ip));
              if (OB_UNLIKELY(ret_len <= 0) || OB_UNLIKELY(ret_len >= MAX_THREAD_NAME_LENGTH)) {
                ret = OB_SIZE_OVERFLOW;
                PROXY_NET_LOG(ERROR, "fail to snprintf thr_name", K(ret));
              } else {
                if (OB_FAIL(net_accept->init_accept_loop(thr_name, opt.stacksize_))) {
                  PROXY_NET_LOG(ERROR, "fail to init_accept_loop", K(i), K(accept_ip));
                } else {
                  PROXY_NET_LOG(DEBUG, "created accept thread", K(i), K(accept_ip));
                }
              }
            }

            if (OB_FAIL(ret)) {
              if (NULL != net_accept) {
                delete net_accept;
                net_accept = NULL;
              }
            }
          }

          if (OB_SUCC(ret)) {
            ret_len = snprintf(thr_name, MAX_THREAD_NAME_LENGTH, "[ACCEPT %ld:%d]", accept_threads_ - 1,
                               ops_ip_port_host_order(accept_ip));
            if (OB_UNLIKELY(ret_len <= 0) || OB_UNLIKELY(ret_len >= MAX_THREAD_NAME_LENGTH)) {
              ret = OB_SIZE_OVERFLOW;
              PROXY_NET_LOG(ERROR, "fail to snprintf thr_name", K(ret));
            } else {
              if (OB_FAIL(na->init_accept_loop(thr_name, opt.stacksize_))) {
                PROXY_NET_LOG(ERROR, "fail to init_accept_loop", K(accept_ip));
                delete na;
                na = NULL;
              } else {
                PROXY_NET_LOG(DEBUG, "created accept thread", K(accept_ip));
              }
            }
          }
        } // end na->do_listen(BLOCKING)
      } else { // true == opt.frequent_accept_ && 0 == accept_threads_
        if(OB_FAIL(na->init_accept_per_thread())) {
          PROXY_NET_LOG(ERROR, "fail to init_accept_per_thread", K(ret));
        }
      }
    } else { // false == opt.frequent_accept_
      if (OB_FAIL(na->init_accept())) {
        PROXY_NET_LOG(ERROR, "fail to init_accept", K(ret));
      }
    }

#ifdef TCP_DEFER_ACCEPT
    // set tcp defer accept timeout if it is configured, this will not trigger an accept
    // until there is data on the socket ready to be read
    int32_t defer_accept_timeout = static_cast<int32_t>(opt.defer_accept_timeout_);
    if (OB_SUCC(ret) && (defer_accept_timeout > 0)
        && OB_FAIL(ObSocketManager::setsockopt(na->server_.fd_,
                                               IPPROTO_TCP,
                                               TCP_DEFER_ACCEPT,
                                               &defer_accept_timeout,
                                               sizeof(int32_t)))) {
      PROXY_NET_LOG(ERROR, "can't set defer accept timeout", K(defer_accept_timeout), K(ret));
    }
#endif

#ifdef TCP_INIT_CWND
    int32_t tcp_init_cwnd = static_cast<int32_t>(opt.tcp_init_cwnd_);
    if (OB_SUCC(ret) && (tcp_init_cwnd > 0)
        && OB_FAIL(ObSocketManager::setsockopt(na->server_.fd_,
                                               IPPROTO_TCP,
                                               TCP_INIT_CWND,
                                               &tcp_init_cwnd,
                                               sizeof(int32_t)))) {
      PROXY_NET_LOG(ERROR, "can't set initial congestion window", K(tcp_init_cwnd), K(ret));
    }
#endif
  }

  if (OB_FAIL(ret)) {
    if (NULL != na) {
      if (NULL != na->action_) {
        // na->action is of type ObPtr
        na->action_ = NULL;
      } else {
        delete na;
        na = NULL;
      }
    }
  } else if (NULL != na) {
    action_ret = na->action_;
  }
  return action_ret;
}

inline int ObUnixNetProcessor::connect_internal(
    ObContinuation &cont, const sockaddr &target, ObNetVCOptions *opt)
{
  int ret = OB_SUCCESS;
  ObEThread *ethread = NULL;
  ObUnixNetVConnection *vc = NULL;

  if (OB_ISNULL(cont.mutex_) || OB_ISNULL(ethread = cont.mutex_->thread_holding_)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_NET_LOG(ERROR, "invalid argument", K(ret));
  } else if (OB_ISNULL(vc = reinterpret_cast<ObUnixNetVConnection *>(allocate_vc()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_NET_LOG(ERROR, "fail to allocate_vc", K(ret));
  } else {
    if (NULL != opt) {
      vc->options_ = *opt;
    } else {
      opt = &vc->options_;
    }
    // virtual function used to upgrade etype_ to ET_SSL for SSLNetProcessor.
    upgrade_etype(opt->etype_);

    NET_SUM_GLOBAL_DYN_STAT(NET_GLOBAL_CONNECTIONS_CURRENTLY_OPEN, 1);
    vc->id_ = net_next_connection_number();
    vc->submit_time_ = get_hrtime();
    vc->mutex_ = cont.mutex_;
    vc->action_.set_continuation(&cont);
    vc->source_type_ = (opt->is_inner_connect_ ? ObUnixNetVConnection::VC_INNER_CONNECT : ObUnixNetVConnection::VC_CONNECT);
    if (OB_UNLIKELY(!ops_ip_copy(vc->server_addr_, target))) {
      ret = OB_ERR_UNEXPECTED;
      PROXY_NET_LOG(ERROR, "fail to ops_ip_copy vc->server_addr_", K(vc), K(ret));
    } else {
      //opt->ethread_ direct to client vc created thread
      //ethread direct to client session->mutex_->thread_holding_ thread
      if (NULL != opt->ethread_ && ethread != opt->ethread_) {
        //when using proxy client vc, we may connect up in different thread, reschedule
        PROXY_NET_LOG(DEBUG, "current thread is not the same with expected thread, need schedule",
                      KPC(ethread), "expected thread", *(opt->ethread_));
        if (OB_ISNULL(opt->ethread_->schedule_imm(vc))) {
          ret = OB_ERR_UNEXPECTED;
          PROXY_NET_LOG(WARN, "fail to schedule switch thread", "ethread", *(opt->ethread_), K(ret));
        }
      } else if (OB_UNLIKELY(!ethread->is_event_thread_type(opt->etype_))) {
        //we need use ET_CALL thread for connect_up.
        //we will never arrive here now, here it's just for defense
        if (OB_ISNULL(g_event_processor.schedule_imm(vc, opt->etype_))) {
          ret = OB_ERR_UNEXPECTED;
          PROXY_NET_LOG(WARN, "fail to schedule switch thread", "type", opt->etype_, K(ret));
        }
      } else {
        MUTEX_TRY_LOCK(cont_lock, cont.mutex_, ethread);
        //the cont_lock must be locked, as ethread == cont.mutex_->thread_holding_
        if (cont_lock.is_locked()) {
          MUTEX_TRY_LOCK(nh_lock, ethread->get_net_handler().mutex_, ethread);
          if (nh_lock.is_locked()) {
            //the nh_lock maybe lock failed, because the ObUnixNetVConnection::reenable(vio)
            //may be lock nh at the same time
            ret = vc->connect_up(*ethread, NO_FD);
            // some error happen, has callout in connect_up, just free vc
            if (OB_FAIL(ret)) {
              PROXY_NET_LOG(WARN, "fail to connect_up, vc will be free", K(ret));
              vc->free();
              vc = NULL;
              ret = OB_SUCCESS;
            }
          } else {
            PROXY_NET_LOG(DEBUG, "lock net_handler failed, need retry", KPC(ethread));
            if (OB_ISNULL(ethread->schedule_imm(vc))) {
              ret = OB_ERR_UNEXPECTED;
              PROXY_NET_LOG(WARN, "fail to schedule retry connect", KPC(opt->ethread_), K(ret));
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          PROXY_NET_LOG(ERROR, "lock failed, it should not happened", KPC(ethread), K(ret));
        }
      }
    }
  }

  if (OB_FAIL(ret) && NULL != vc) {
    vc->free();
    vc = NULL;
  }

  return ret;
}

inline int ObNetProcessor::connect_re(ObContinuation &cont, const sockaddr &addr, ObNetVCOptions *opts)
{
  return static_cast<ObUnixNetProcessor *>(this)->connect_internal(cont, addr, opts);
}

class ObCheckConnect : public ObContinuation
{
public:
  explicit ObCheckConnect(ObProxyMutex &m)
      : ObContinuation(&m), vc_(NULL), connect_status_(-1),
        recursion_(0), timeout_(0), opt_()
  {
    MEMSET(&target_, 0, sizeof(sockaddr));
    SET_HANDLER(&ObCheckConnect::handle_connect);
  }

  virtual ~ObCheckConnect()
  {
  }

  int connect(ObContinuation &cont, const sockaddr &target,
              ObAction *&action, const int64_t timeout, ObNetVCOptions *opt)
  {
    int ret = OB_SUCCESS;
    action = NULL;
    action_.set_continuation(&cont);
    timeout_ = timeout;
    ++recursion_;
    ret = g_net_processor.connect_re(*this, target, opt);
    --recursion_;
    if (OB_SUCC(ret) && (NET_EVENT_OPEN_FAILED != connect_status_)) {
      action = &action_;
    } else {
      delete this;
    }
    return ret;
  }

private:
  int handle_connect(int event, ObEvent *e)
  {
    int event_ret = EVENT_DONE;
    int ret = OB_SUCCESS;
    connect_status_ = event;

    switch (event) {
      case NET_EVENT_OPEN:
        vc_ = reinterpret_cast<ObUnixNetVConnection *>(e);
        // some non-zero number just to get the poll going
        vc_->do_io_write(this, 1, ObCheckConnect::GLOBAL_READER);
        // don't wait for more than timeout secs
        vc_->set_inactivity_timeout(timeout_);
        event_ret = EVENT_CONT;
        break;

      case NET_EVENT_OPEN_FAILED:
        PROXY_NET_LOG(WARN, "connect net open failed");
        if (!action_.cancelled_) {
          action_.continuation_->handle_event(NET_EVENT_OPEN_FAILED, reinterpret_cast<void *>(e));
        }
        break;

      case VC_EVENT_WRITE_READY:
      {
        PROXY_NET_LOG(DEBUG, "net connect received event", K(VC_EVENT_WRITE_READY));
        int32_t optval = -1;
        int32_t optlen = sizeof(int32_t);

        if (!action_.cancelled_) {
          if (OB_FAIL(ObSocketManager::getsockopt(vc_->con_.fd_, SOL_SOCKET, SO_ERROR,
              reinterpret_cast<void *>(&optval), &optlen))) {
            PROXY_NET_LOG(WARN, "fail to getsockopt", "addr", vc_->con_.addr_, K(ret));
          } else if (0 != optval) {
            ret = OB_ERR_SYS;
            PROXY_NET_LOG(WARN, "detect socket error", K(optval), "addr", vc_->con_.addr_, K(ret));
          } else {
            PROXY_NET_LOG(DEBUG, "connection established");
            // disable write on vc
            vc_->write_.enabled_ = false;
            vc_->cancel_inactivity_timeout();
            // clean up vc fields
            vc_->write_.vio_.nbytes_ = 0;
            vc_->write_.vio_.op_ = ObVIO::NONE;
            vc_->write_.vio_.buffer_.destroy();
          }

          if (OB_SUCC(ret)) {
            action_.continuation_->handle_event(NET_EVENT_OPEN, vc_);
          } else {
            vc_->do_io_close();
            action_.continuation_->handle_event(NET_EVENT_OPEN_FAILED,
                                                reinterpret_cast<void *>(-ENET_CONNECT_FAILED));
          }
        } else {
          vc_->do_io_close();
        }
        break;
      }

      case VC_EVENT_INACTIVITY_TIMEOUT:
        PROXY_NET_LOG(WARN, "connect timed out", "addr", vc_->con_.addr_, K(this), K_(timeout));
        vc_->do_io_close();
        if (!action_.cancelled_) {
          action_.continuation_->handle_event(NET_EVENT_OPEN_FAILED,
                                              reinterpret_cast<void *>(-ENET_CONNECT_TIMEOUT));
        }
        break;

      case VC_EVENT_DETECT_SERVER_DEAD:
        PROXY_NET_LOG(WARN, "detect server addr dead", "addr", vc_->con_.addr_, K(this));
        vc_->do_io_close();
        if (!action_.cancelled_) {
          action_.continuation_->handle_event(NET_EVENT_OPEN_FAILED,
                                              reinterpret_cast<void *>(-ENET_CONNECT_FAILED));
        }
        break;

      default:
        PROXY_NET_LOG(WARN, "unknown connect event");
        if (!action_.cancelled_) {
          action_.continuation_->handle_event(NET_EVENT_OPEN_FAILED,
                                             reinterpret_cast<void *>(-ENET_CONNECT_FAILED));
        }
        break;
    }

    if (0 == recursion_ && EVENT_DONE == event_ret) {
      delete this;
    }
    return event_ret;
  }

private:
  // if handle_connect handle event NET_EVENT_OPEN,
  // vc need use do_io_write to make the poll going.
  // but do_io_write need param ObIOBufferReader,
  // here use GLOBAL_READER instead.
  // no one will use the buf from GLOBAL_READER,
  // so here is no problem about multithreading.
  static ObMIOBuffer GLOBAL_BUFFER;
  static ObIOBufferReader *GLOBAL_READER;

private:
  ObUnixNetVConnection *vc_;
  ObAction action_;
  int32_t connect_status_;
  int32_t recursion_;
  ObHRTime timeout_;
  ObNetVCOptions opt_;
  sockaddr target_;

  DISALLOW_COPY_AND_ASSIGN(ObCheckConnect);
};

ObMIOBuffer ObCheckConnect::GLOBAL_BUFFER(1);
ObIOBufferReader *ObCheckConnect::GLOBAL_READER = ObCheckConnect::GLOBAL_BUFFER.alloc_reader();

int ObNetProcessor::connect(ObContinuation &cont, const sockaddr &target,
                            ObAction *&action, const int64_t timeout, ObNetVCOptions *opt)
{
  int ret = OB_SUCCESS;
  ObCheckConnect *c = NULL;
  action = NULL;

  if (OB_ISNULL(cont.mutex_)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_NET_LOG(ERROR, "fail to get cont mutex, it's null", K(ret));
  } else if (OB_ISNULL(c = new(std::nothrow) ObCheckConnect(*cont.mutex_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_NET_LOG(ERROR, "fail to new ObCheckConnect", K(ret));
  } else if (OB_FAIL(c->connect(cont, target, action, timeout, opt))) {
    PROXY_NET_LOG(WARN, "fail to connect with timeout", K(ret));
  }
  return ret;
}

// This is a little odd, in that the actual threads are created before calling the processor.
int ObUnixNetProcessor::start()
{
  int ret = OB_SUCCESS;
  ObEventThreadType etype = ET_NET;

  // etype is ET_NET for netProcessor
  upgrade_etype(etype);

  int64_t net_thread_count = g_event_processor.thread_count_for_type_[etype];
  ObEThread **ethreads = g_event_processor.event_thread_[etype];
  for (int64_t i = 0; i < net_thread_count && OB_SUCC(ret); ++i) {
    if (OB_FAIL(initialize_thread_for_net(ethreads[i]))) {
      PROXY_NET_LOG(ERROR, "fail to initialize thread for net", K(i), K(ret));
    }
  }
  return ret;
}

// Virtual function allows creation of an
// ObNetAccept transparent to ObNetProcessor.
inline ObNetAccept *ObUnixNetProcessor::create_net_accept()
{
  int ret = OB_SUCCESS;
  ObNetAccept *na = NULL;
  if (OB_ISNULL(na = new(std::nothrow) ObNetAccept())) {
    PROXY_NET_LOG(ERROR, "fail to allocate memory for create_net_accept an ObNetAccept");
  } else if (OB_FAIL(na->init())) {
    PROXY_NET_LOG(ERROR, "fail to init ObNetAccept", K(ret));
    delete na;
    na = NULL;
  }
  return na;
}

inline ObNetVConnection *ObUnixNetProcessor::allocate_vc()
{
  int ret = OB_SUCCESS;
  ObUnixNetVConnection *conn = NULL;
  if (OB_ISNULL(conn = op_reclaim_alloc(ObUnixNetVConnection))) {
    PROXY_NET_LOG(ERROR, "fail to allocate memory for allocate_vc an ObUnixNetVConnection");
  } else if (OB_FAIL(conn->init())) {
    PROXY_NET_LOG(ERROR, "fail to init ObUnixNetVConnection", K(ret));
    op_reclaim_free(conn);
    conn = NULL;
  }
  return conn;
}

} // end of namespace net
} // end of namespace obproxy
} // end of namespace oceanbase
