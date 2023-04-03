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

#include <pthread.h>
#include "iocore/net/ob_net.h"
#include "iocore/net/ob_unix_net.h"
#include "iocore/net/ob_event_io.h"
#include "iocore/net/ob_timerfd_manager.h"
#include "obutils/ob_resource_pool_processor.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace net
{

ObHRTime last_transient_accept_error;
pthread_once_t g_exit_once = PTHREAD_ONCE_INIT;

static inline int net_signal_hook_callback(ObEThread &thread)
{
  int ret = OB_SUCCESS;
  uint64_t counter = 0;
  int64_t count = -1;
#if OB_HAVE_EVENTFD
  ret = ObSocketManager::read(thread.evfd_, &counter, sizeof(uint64_t), count);
#else
  ret = ObSocketManager::read(thread.evpipe_[0], &counter, sizeof(uint64_t), count);
#endif
  if (OB_FAIL(ret)) {
    PROXY_NET_LOG(WARN, "fail to read from net", K(counter), K(count), K(ret));
  }
  return ret;
}

static inline int net_signal_hook_function(ObEThread &thread)
{
  int ret = 0;
  uint64_t counter = 1;
  int64_t count = -1;
#if OB_HAVE_EVENTFD
  ret = ObSocketManager::write(thread.evfd_, &counter, sizeof(uint64_t), count);
#else
  ret = ObSocketManager::write(thread.evpipe_[1], &counter, sizeof(uint64_t), count);
#endif
  if (OB_FAIL(ret)) {
    PROXY_NET_LOG(WARN, "fail to write ro net", K(counter), K(count), K(ret));
  }
  return ret;
}

void send_signal_when_exit_normally()
{
  if (g_proxy_fatal_errcode == OB_SUCCESS) {
    pid_t parent_pid= getppid();
    if (parent_pid != 1) {
      PROXY_NET_LOG(INFO, "obproxy child will send SIGUSR2 to parent", K(parent_pid));
      kill(parent_pid, SIGUSR2);
    }
  }
}

void proxy_exit_once()
{
  // no connection opened, graceful exit flag is true, graceful exit timeout, exit!!
  PROXY_NET_LOG(INFO, "graceful exit, it will cancel DEDICATED thread and exit",
                "dedicate_thread_count_", g_event_processor.dedicate_thread_count_, K(g_proxy_fatal_errcode));
  int ret = OB_SUCCESS;
  ObThreadId tid = 0;
  for (int64_t i = 0; i < g_event_processor.dedicate_thread_count_ && OB_SUCC(ret); ++i) {
    tid = g_event_processor.all_dedicate_threads_[i]->tid_;
    if (OB_FAIL(thread_cancel(tid))) {
      PROXY_NET_LOG(WARN, "fail to do thread_cancel", K(tid), K(ret));
    } else if (OB_FAIL(thread_join(tid))) {
      PROXY_NET_LOG(WARN, "fail to do thread_join", K(tid), K(ret));
    } else {
      PROXY_NET_LOG(INFO, "graceful exit, dedicated thread exited", K(tid));
    }
    ret = OB_SUCCESS;//ignore error
  }
  OB_LOGGER.destroy_async_log_thread();
  _exit(1);
}

int update_cop_config(const int64_t default_inactivity_timeout, const int64_t max_client_connections)
{
  int ret = OB_SUCCESS;
  ObEventThreadType etype = ET_NET;
  int64_t net_thread_count = g_event_processor.thread_count_for_type_[etype];
  ObEThread **netthreads = g_event_processor.event_thread_[etype];

  if (OB_ISNULL(netthreads) || OB_UNLIKELY(net_thread_count < 0)
      || OB_UNLIKELY(default_inactivity_timeout < 0)
      || OB_UNLIKELY(max_client_connections < 0)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_NET_LOG(WARN, "invalid argument", K(netthreads), K(net_thread_count),
                  K(default_inactivity_timeout),
                  K(max_client_connections), K(ret));
  } else {
    ObInactivityCop *inactivity_cop = NULL;
    for (int64_t i = 0; i < net_thread_count && OB_SUCC(ret); ++i) {
      if (OB_ISNULL(netthreads[i])) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_NET_LOG(WARN, "get netthread from netthreads is null",
                      K(netthreads[i]), K(i), K(ret));
      } else {
        inactivity_cop = &netthreads[i]->get_inactivity_cop();
        if (OB_LIKELY(default_inactivity_timeout > 0)) {
          inactivity_cop->set_default_timeout(default_inactivity_timeout);
        }
        if (OB_LIKELY(max_client_connections >= 0)) {
          inactivity_cop->set_max_connections(max_client_connections);
          inactivity_cop->set_connections_per_thread(0);
        }
      }
    }
  }
  return ret;
}

int initialize_thread_for_net(ObEThread *thread)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(thread)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_NET_LOG(WARN, "invalid argument", K(thread), K(ret));
  } else if (OB_ISNULL(thread->net_handler_ = new (std::nothrow) ObNetHandler())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_NET_LOG(WARN, "fail to new ObNetHandler", K(thread), K(ret));
  } else if (OB_ISNULL(thread->net_poll_ = new (std::nothrow) ObNetPoll(thread->get_net_handler()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_NET_LOG(WARN, "fail to new ObNetPoll", K(thread), K(ret));
  } else if (OB_FAIL(thread->net_poll_->init())) {
      PROXY_NET_LOG(WARN, "fail to init ob_poll",K(thread), K(ret));
  } else if (OB_ISNULL(thread->net_handler_->mutex_ = new_proxy_mutex(NET_HANDLER_LOCK))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_NET_LOG(WARN, "fail to allocate proxy mutex for net handler", K(thread), K(ret));
  } else if (OB_ISNULL(thread->ep_ = new (std::nothrow) ObEventIO())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_NET_LOG(WARN, "fail to allocate thread event IO", K(thread), K(ret));
  } else if (OB_ISNULL(thread->schedule_imm(thread->net_handler_))) {
    ret = OB_ERR_SYS;
    PROXY_NET_LOG(WARN, "fail to schedule net handler", K(thread), K(ret));
  } else if (OB_ISNULL(thread->inactivity_cop_ = new (std::nothrow) ObInactivityCop(thread->net_handler_->mutex_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_NET_LOG(WARN, "fail to new ObInactivityCop", K(thread), K(ret));
  } else if (OB_ISNULL(thread->schedule_every(thread->inactivity_cop_, HRTIME_SECONDS(1)))) {
    ret = OB_ERR_SYS;
    PROXY_NET_LOG(WARN, "fail to schedule_every inactivity cop", K(thread), K(ret));
  } else {
    thread->signal_hook_ = net_signal_hook_function;
    thread->ep_->type_ = EVENTIO_ASYNC_SIGNAL;
#if OB_HAVE_EVENTFD
    ret = thread->ep_->start(thread->get_net_poll().get_poll_descriptor(), thread->evfd_, EVENTIO_READ);
#else
    ret = thread->ep_->start(thread->get_net_poll().get_poll_descriptor(), thread->evpipe_[0], EVENTIO_READ);
#endif
    if (OB_FAIL(ret)) {
      PROXY_NET_LOG(WARN, "fail to start event IO", K(thread), K(ret));
    }
  }

  if ((NULL != thread) && OB_FAIL(ret)) {
    if (NULL != thread->inactivity_cop_) {
      delete thread->inactivity_cop_;
      thread->inactivity_cop_ = NULL;
    }
    if (NULL != thread->ep_) {
      delete thread->ep_;
      thread->ep_ = NULL;
    }
    if (NULL != thread->net_poll_) {
      delete thread->net_poll_;
      thread->net_poll_ = NULL;
    }
    if (NULL != thread->net_handler_) {
      delete thread->net_handler_;
      thread->net_handler_ = NULL;
    }
  }

  return ret;
}

ObNetPoll::ObNetPoll(ObNetHandler &nh)
    : poll_descriptor_(NULL),
      nh_(nh),
      timer_fd_(OB_INVALID_INDEX),
      ep_(NULL)
{
}

ObNetPoll::~ObNetPoll()
{
  delete poll_descriptor_;
  poll_descriptor_ = NULL;

  if (NULL != ep_) {
    op_reclaim_free(ep_);
    ep_ = NULL;
  }

  ObTimerFdManager::timerfd_close(timer_fd_);
}

int ObNetPoll::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL != poll_descriptor_)) {
    ret = OB_INIT_TWICE;
    PROXY_NET_LOG(WARN, "init twice", K(poll_descriptor_), K(ret));
  } else {
    poll_descriptor_ = new(std::nothrow) ObPollDescriptor();
    if (OB_ISNULL(poll_descriptor_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      PROXY_NET_LOG(WARN, "fail to new ObPollDescriptor", K(ret));
    } else if (OB_FAIL(poll_descriptor_->init())) {
      PROXY_NET_LOG(WARN, "fail to init poll_descriptor");
      delete poll_descriptor_;
      poll_descriptor_ = NULL;
    } else if (OB_FAIL(ObTimerFdManager::timerfd_create(CLOCK_REALTIME, TFD_NONBLOCK, timer_fd_))) {
      PROXY_NET_LOG(WARN, "fail to create timerfd", K(timer_fd_), KERRMSGS, K(ret));
    } else if (OB_FAIL(ObTimerFdManager::timerfd_settime(timer_fd_, 0, 0, 0))) {
      PROXY_NET_LOG(WARN, "fail to set timerfd time", K(timer_fd_), KERRMSGS, K(ret));
    } else if (OB_ISNULL(ep_ = op_reclaim_alloc(ObEventIO))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      PROXY_NET_LOG(ERROR, "fail to new ObEventIO", K(ret));
    } else {
      struct epoll_event ev;
      memset(&ev, 0, sizeof(ev));
      ev.events = EVENTIO_READ;
      ev.data.ptr = ep_;
      ep_->type_ = EVENTIO_TIMER;
      if (OB_FAIL(ObSocketManager::epoll_ctl(poll_descriptor_->epoll_fd_, EPOLL_CTL_ADD, timer_fd_, &ev))) {
        PROXY_NET_LOG(WARN, "fail to epoll_ctl, op is EPOLL_CTL_ADD", K(poll_descriptor_->epoll_fd_), K_(timer_fd), K(ret));
      }
    }
  }
  return ret;
}

int ObNetPoll::timerfd_settime()
{
  int ret = OB_SUCCESS;
  if (timer_fd_ > 0 && OB_FAIL(ObTimerFdManager::timerfd_settime(timer_fd_, 0, 0, 1))) {
    PROXY_NET_LOG(WARN, "fail to set timer time, it should not happened", K(timer_fd_), K(ret));
  }
  return ret;
}

ObInactivityCop::ObInactivityCop(ObProxyMutex *m)
    : ObContinuation(m), default_inactivity_timeout_(1800),
      total_connections_in_(0), max_connections_in_(0), connections_per_thread_in_(0)
{
  SET_HANDLER(&ObInactivityCop::check_inactivity);
  PROXY_NET_LOG(DEBUG, "new ObInactivityCop", K(default_inactivity_timeout_));
}

int ObInactivityCop::check_inactivity(int event, ObEvent *e)
{
  UNUSED(event);
  int ret = OB_SUCCESS;
  ObHRTime now = get_hrtime();
  ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
  total_connections_in_ = 0;
  ObEThread *ethread = &self_ethread();
  ObNetHandler &nh = ethread->get_net_handler();

  if (OB_ISNULL(e)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_NET_LOG(WARN, "ObEvent is NULL", K(e), K(ret));
  } else {
    if (OB_UNLIKELY(!info.need_conn_accept_ || OB_SUCCESS != g_proxy_fatal_errcode) && OB_LIKELY(info.graceful_exit_end_time_ > 0)) {
      int64_t global_connections = 0;
      NET_READ_GLOBAL_DYN_SUM(NET_GLOBAL_CLIENT_CONNECTIONS_CURRENTLY_OPEN, global_connections);
      if (0 == ethread->id_ && -1 == info.active_client_vc_count_) {
        info.active_client_vc_count_ = global_connections;
        const int64_t remain_time = hrtime_to_sec(info.graceful_exit_end_time_ - now);
        PROXY_NET_LOG(INFO, "begin orderly close", "active_client_vc_count", global_connections,
                      "remain_time(s)", remain_time, K(g_proxy_fatal_errcode));

        if (OB_GOT_SIGNAL_ABORTING == g_proxy_fatal_errcode) {
          PROXY_NET_LOG(INFO, "receive signal", K(info.received_sig_));
        }
      }

      if (global_connections > 0) {
        if (info.graceful_exit_end_time_ >= info.graceful_exit_start_time_ && info.graceful_exit_end_time_ <= now) {
          send_signal_when_exit_normally();
          pthread_once(&g_exit_once, proxy_exit_once);
        } else {
          int64_t thread_local_client_connections = 0;
          NET_THREAD_READ_DYN_SUM(ethread, NET_CLIENT_CONNECTIONS_CURRENTLY_OPEN, thread_local_client_connections);
          PROXY_NET_LOG(INFO, "wait for net_global_connections_currently_open_stat down to zero",
                        K(global_connections), K(thread_local_client_connections), K(g_proxy_fatal_errcode));
        }
      } else {
        send_signal_when_exit_normally();
        pthread_once(&g_exit_once, proxy_exit_once);
      }
    } else {
      info.active_client_vc_count_ = 0;
      info.graceful_exit_start_time_ = 0;
      info.graceful_exit_end_time_ = 0;
    }

    // Copy the list and use pop() to catch any closes caused by callbacks.
    forl_LL(ObUnixNetVConnection, vc, nh.open_list_) {
      if (vc->thread_ == ethread) {
        if (ObUnixNetVConnection::VC_ACCEPT == vc->source_type_) {
          ++total_connections_in_;
        }
        nh.cop_list_.push(vc);
      }
    }

    ObUnixNetVConnection *vc = NULL;
    ObHRTime diff = 0;

    int close_ret = OB_SUCCESS;
    while ((NULL != (vc = nh.cop_list_.pop())) && OB_SUCC(ret)) {
      // If we cannot get the lock don't stop just keep cleaning
      MUTEX_TRY_LOCK(lock, vc->mutex_, ethread);
      if (!lock.is_locked()) {
        NET_INCREMENT_DYN_STAT(INACTIVITY_COP_LOCK_ACQUIRE_FAILURE);
      } else if (vc->closed_) {
        if (OB_UNLIKELY(OB_SUCCESS != (close_ret = vc->close()))) {
          PROXY_NET_LOG(WARN, "fail to close unix net vconnection", K(vc), K(close_ret));
        }
      } else {
        int32_t event = EVENT_NONE;
        if (vc->get_is_force_timeout()
            || (info.graceful_exit_end_time_ >= info.graceful_exit_start_time_
                && info.graceful_exit_end_time_ > 0
                && info.graceful_exit_end_time_ < now)) {
          vc->next_inactivity_timeout_at_ = now; // force the connection timeout
        }

        if (now != vc->next_inactivity_timeout_at_) {
          ObIpEndpoint ip(vc->get_remote_addr());
          if (0 < get_global_proxy_config().server_detect_mode
              && vc->source_type_ == ObUnixNetVConnection::VC_CONNECT
              && OB_HASH_EXIST == get_global_resource_pool_processor().ip_set_.exist_refactored(ip)) {
            PROXY_NET_LOG(WARN, "detect server dead, close connection", K(ip));
            event = EVENT_ERROR;
          }
        }

        // set a default inactivity timeout if one is not set
        if (0 == vc->next_inactivity_timeout_at_ && default_inactivity_timeout_ > 0) {
          PROXY_NET_LOG(DEBUG, "inactivity timeout not set, setting a default",
                        K(vc), K(vc->source_type_), K(default_inactivity_timeout_));
          vc->set_inactivity_timeout(HRTIME_SECONDS(default_inactivity_timeout_));
          NET_INCREMENT_DYN_STAT(DEFAULT_INACTIVITY_TIMEOUT);
        } else {
          PROXY_NET_LOG(DEBUG, "inactivity timeout state", K(vc), K(vc->source_type_),
                        "next_inactivity_timeout_at", hrtime_to_sec(vc->next_inactivity_timeout_at_),
                        "inactivity_timeout_in", hrtime_to_sec(vc->inactivity_timeout_in_));
        }

        if (vc->next_inactivity_timeout_at_ > 0 && vc->next_inactivity_timeout_at_ <= now) {
          if (nh.keep_alive_list_.in(vc)) {
            // only stat if the connection is in keep-alive, there can be other inactivity timeouts
            diff = (now - (vc->next_inactivity_timeout_at_ - vc->inactivity_timeout_in_)) / HRTIME_SECOND;
            NET_SUM_DYN_STAT(KEEP_ALIVE_LRU_TIMEOUT_TOTAL, diff);
            NET_INCREMENT_DYN_STAT(KEEP_ALIVE_LRU_TIMEOUT_COUNT);
          }
          PROXY_NET_LOG(DEBUG, "inactivity timeout state", K(vc), K(vc->source_type_), K(now),
                        "next_inactivity_timeout_at", hrtime_to_sec(vc->next_inactivity_timeout_at_),
                        "inactivity_timeout_in", hrtime_to_sec(vc->inactivity_timeout_in_));
          vc->handle_event(EVENT_IMMEDIATE, e);
        } else if (EVENT_NONE != event) {
          vc->handle_event(event, e);
        }
      }
    }

    // Keep-alive LRU for incoming connections
    if (OB_SUCC(ret) && OB_FAIL(keep_alive_lru(nh, now, e))) {
      PROXY_NET_LOG(WARN, "fail to keep_alive_lru", K(e), K(ret));
    }
  }
  return (OB_SUCCESS == ret) ? EVENT_DONE : EVENT_ERROR;
}

int ObInactivityCop::keep_alive_lru(ObNetHandler &nh, const ObHRTime now, ObEvent *e)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(e)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_NET_LOG(WARN, "invalid argument", K(e), K(ret));
  } else {
    // maximum incoming connections is set to 0 then the feature is disabled
    if (OB_LIKELY(max_connections_in_ > 0)) {
      if (0 == connections_per_thread_in_) {
        // figure out the number of threads and calculate the number of connections per thread
        // if connections_per_thread_in_ is 0 ,
        // then it will be set to 1 in order that each net thread can hold at least 1 connection,
        // otherwise proxy will disconnect all connections in keep alive list
        if (0 == (connections_per_thread_in_ = max_connections_in_ / g_event_processor.thread_count_for_type_[ET_NET])) {
          connections_per_thread_in_ = 1;
        }
      }

      // calculate how many connections to close
      int64_t to_process = total_connections_in_ - connections_per_thread_in_;
      if (to_process > 0) {
        int64_t closed = 0;
        int64_t handle_event = 0;
        int64_t total_idle_time = 0;
        int64_t total_idle_count = 0;

        to_process = std::min(nh.keep_alive_lru_size_, to_process);

        PROXY_NET_LOG(DEBUG, "keep alive lru state", K(connections_per_thread_in_),
                      "active_count", (total_connections_in_ - nh.keep_alive_lru_size_),
                      K(nh.keep_alive_lru_size_), K(to_process), K(ET_NET));

        ObEThread *ethread = &self_ethread();
        ObHRTime diff = -1;

        // loop over the non-active connections and try to close them
        ObUnixNetVConnection *vc = nh.keep_alive_list_.head_;
        ObUnixNetVConnection *vc_next = NULL;
        int close_ret = OB_SUCCESS;
        for (int32_t i = 0; i < to_process && (NULL != vc) && OB_SUCC(ret); ++i, vc = vc_next) {
          vc_next = vc->keep_alive_link_.next_;
          if (vc->thread_ == ethread) {
            MUTEX_TRY_LOCK(lock, vc->mutex_, ethread);
            if (lock.is_locked()) {
              diff = (now - (vc->next_inactivity_timeout_at_ - vc->inactivity_timeout_in_)) / HRTIME_SECOND;
              if (diff > 0) {
                total_idle_time += diff;
                ++total_idle_count;
                NET_SUM_DYN_STAT(KEEP_ALIVE_LRU_TIMEOUT_TOTAL, diff);
                NET_INCREMENT_DYN_STAT(KEEP_ALIVE_LRU_TIMEOUT_COUNT);
              }

              PROXY_NET_LOG(INFO, "closing connection", K(vc), K(nh.keep_alive_lru_size_), K(now),
                            "next_inactivity_timeout_at", hrtime_to_sec(vc->next_inactivity_timeout_at_),
                            "inactivity_timeout_in", hrtime_to_sec(vc->inactivity_timeout_in_),
                            K(diff));

              if (vc->closed_) {
                if (OB_UNLIKELY(OB_SUCCESS != (close_ret = vc->close()))) {
                  PROXY_NET_LOG(WARN, "fail to close unix net vconnection", K(vc), K(close_ret));
                }
                ++closed;
              } else {
                vc->next_inactivity_timeout_at_ = now;
                nh.keep_alive_list_.head_->handle_event(EVENT_IMMEDIATE, e);
                ++handle_event;
              }
            }
          }
        }

        if (total_idle_count > 0) {
          PROXY_NET_LOG(DEBUG, "keep alive lru state", K(connections_per_thread_in_),
                        "active_count", (total_connections_in_ - nh.keep_alive_lru_size_ - closed - handle_event),
                        K(nh.keep_alive_lru_size_), K(closed), K(handle_event),
                        "mean_idle", (total_idle_time / total_idle_count));
        }
      }
    }
  }
  return ret;
}


ObNetHandler::ObNetHandler()
    : ObContinuation(NULL),
      trigger_event_(NULL),
      keep_alive_lru_size_(0)
{
  SET_HANDLER(reinterpret_cast<NetContHandler>(&ObNetHandler::start_net_event));
}

// Initialization here
// in the thread in which we will be executing from now on.
int ObNetHandler::start_net_event(int event, ObEvent *e)
{
  UNUSED(event);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(e)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_NET_LOG(WARN, "ObEvent is NULL", K(e), K(ret));
  } else {
    SET_HANDLER(reinterpret_cast<NetContHandler>(&ObNetHandler::main_net_event));
    e->schedule_every(NET_PERIOD);
    trigger_event_ = e;
  }
  return (OB_SUCCESS == ret) ? EVENT_CONT : EVENT_ERROR;
}

// Move VC's enabled on a different thread to the ready list
inline void ObNetHandler::process_enabled_list()
{
  ObUnixNetVConnection *vc = NULL;

  SListM(ObUnixNetVConnection, ObNetState, read_, enable_link_) rq(read_enable_list_.popall());
  while (NULL != (vc = rq.pop())) {
    vc->ep_->modify(EVENTIO_READ);
    vc->read_.in_enabled_list_ = false;
    if ((vc->reenable_read_time_at_ > 0) && vc->reenable_read_time_at_ > get_hrtime()) {
      vc->read_.in_enabled_list_ = true;
      read_enable_list_.push(vc);
    } else if ((vc->read_.enabled_ && vc->read_.triggered_) || vc->closed_) {
      read_ready_list_.in_or_enqueue(vc);
    }
  }

  SListM(ObUnixNetVConnection, ObNetState, write_, enable_link_) wq(write_enable_list_.popall());
  while (NULL != (vc = wq.pop())) {
    vc->ep_->modify(EVENTIO_WRITE);
    vc->write_.in_enabled_list_ = false;
    if ((vc->write_.enabled_ && vc->write_.triggered_) || vc->closed_) {
      write_ready_list_.in_or_enqueue(vc);
    }
  }
}

// The main event for ObNetHandler
// This is called every NET_PERIOD, and handles all IO operations scheduled
// for this period.
int ObNetHandler::main_net_event(int event, ObEvent *e)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(trigger_event_ != e)
      || OB_UNLIKELY(EVENT_INTERVAL != event && EVENT_POLL != event)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    process_enabled_list();
    int32_t poll_timeout = 0;
    ObUnixNetVConnection *vc = NULL;
    ObEThread *ethread = NULL;
    uint32_t epoll_events = 0;
    ObEventIO *epd = NULL;

    NET_INCREMENT_DYN_STAT(NET_HANDLER_RUN);

    if(OB_ISNULL(ethread = trigger_event_->ethread_)) {
      ret = OB_ERR_UNEXPECTED;
      PROXY_NET_LOG(WARN, "fail to get trigger_event_'s ethread", K(trigger_event_), K(ret));
    } else {
      if (OB_LIKELY(!read_ready_list_.empty() || !write_ready_list_.empty()
            || !read_enable_list_.empty() || !write_enable_list_.empty())) {
        poll_timeout = 0; // poll immediately returns -- we have triggered stuff to process right now
      } else {
        poll_timeout = (int32_t)(hrtime_to_msec(ethread->sleep_time_));
      }

      ObPollDescriptor &pd = ethread->get_net_poll().get_poll_descriptor();
      if (OB_FAIL(ObSocketManager::epoll_wait(pd.epoll_fd_,
          pd.epoll_triggered_events_,
          ObPollDescriptor::POLL_DESCRIPTOR_SIZE,
          poll_timeout, pd.result_))) {
      PROXY_NET_LOG(WARN, "fail to epoll_wait", K(pd.epoll_fd_),
                    K(pd.epoll_triggered_events_),
                    K(poll_timeout), K(ret));
      } else {
        bool in_list = false;
        for (int64_t i = 0; (i < pd.result_) && OB_SUCC(ret); ++i) {
          if (OB_FAIL(pd.get_ev_events(i, epoll_events))) {
            PROXY_NET_LOG(WARN, "fail to get_ev_events", K(pd.result_), K(i), K(ret));
          } else if (OB_FAIL(pd.get_ev_data(i, reinterpret_cast<void *&>(epd)))) {
            PROXY_NET_LOG(WARN, "fail to get_ev_data", K(pd.result_), K(i), K(ret));
          } else {
            if (EVENTIO_READWRITE_VC == epd->type_) {
              if (OB_ISNULL(vc = epd->data_.vc_)) {
                ret = OB_ERR_UNEXPECTED;
                PROXY_NET_LOG(WARN, "fail to get ObUnixNetVConnection from epd->data_", K(ret));
              } else {
                if (epoll_events & (EVENTIO_READ | EVENTIO_ERROR)) {
                  vc->read_.triggered_ = true;
                  if (!read_ready_list_.in(vc)) {
                    read_ready_list_.enqueue(vc);
                  } else if (epoll_events & EVENTIO_ERROR) {
                    // check for unhandled epoll events that should be handled
                    in_list = read_ready_list_.in(vc);
                    PROXY_NET_LOG(DEBUG, "unhandled epoll event on read", K(epoll_events),
                                  K(vc->read_.enabled_), K(vc->closed_), K(in_list));
                  }
                }
                if (epoll_events & (EVENTIO_WRITE | EVENTIO_ERROR)) {
                  vc->write_.triggered_ = true;
                  if (!write_ready_list_.in(vc)) {
                    write_ready_list_.enqueue(vc);
                  } else if (epoll_events & EVENTIO_ERROR) {
                    // check for unhandled epoll events that should be handled
                    in_list = write_ready_list_.in(vc);
                    PROXY_NET_LOG(DEBUG, "unhandled epoll event on write", K(epoll_events),
                                  K(vc->write_.enabled_), K(vc->closed_), K(in_list));
                  }
                } else if (!(epoll_events & EVENTIO_ERROR)) {
                  PROXY_NET_LOG(DEBUG, "unhandled epoll event", K(epoll_events));
                }
              }
            } else if (EVENTIO_ASYNC_SIGNAL == epd->type_) {
              net_signal_hook_callback(*ethread);
            } else if (EVENTIO_TIMER == epd->type_) {
              uint64_t exp;
              read(ethread->get_net_poll().get_timer_fd(), &exp, sizeof(uint64_t)); //Need to read uint64_t size, otherwise an error will occur
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      int close_ret = OB_SUCCESS;
#if defined(USE_EDGE_TRIGGER)
      // ObUnixNetVConnection *
      while (NULL != (vc = read_ready_list_.dequeue())) {
        if (vc->closed_) {
          if (OB_UNLIKELY(OB_SUCCESS != (close_ret = vc->close()))) {
            PROXY_NET_LOG(WARN, "fail to close unix net vconnection", K(vc), K(close_ret));
          }
        } else if (vc->using_ssl() && (vc->read_.enabled_ || vc->write_.enabled_)) {
          vc->do_ssl_io(*ethread);
        } else if (vc->read_.enabled_ && vc->read_.triggered_ && !vc->using_ssl()) {
          vc->read_from_net(*ethread);
        } else if (!vc->read_.enabled_) {
          read_ready_list_.remove(vc);
        }
      }

      while (NULL != (vc = write_ready_list_.dequeue())) {
        if (vc->closed_) {
          if (OB_UNLIKELY(OB_SUCCESS != (close_ret = vc->close()))) {
            PROXY_NET_LOG(WARN, "fail to close unix net vconnection", K(vc), K(close_ret));
          }
        } else if (vc->using_ssl() && (vc->read_.enabled_ || vc->write_.enabled_)) {
          vc->do_ssl_io(*ethread);
        } else if (vc->write_.enabled_ && vc->write_.triggered_ && !vc->using_ssl()) {
          vc->write_to_net(*ethread);
        } else if (!vc->write_.enabled_) {
          write_ready_list_.remove(vc);
        }
      }

#else // USE_EDGE_TRIGGER
      while (NULL != (vc = read_ready_list_.dequeue())) {
        if (vc->closed_) {
          if (OB_UNLIKELY(OB_SUCCESS != (close_ret = vc->close()))) {
            PROXY_NET_LOG(WARN, "fail to close unix net vconnection", K(vc), K(close_ret));
          }
        } else if (vc->using_ssl() || (vc->read_.enabled_ || vc->write_.enabled_)) {
          vc->do_ssl_io(*ethread);
        } else if (vc->read_.enabled_ && vc->read_.triggered_ && !vc->using_ssl()) {
          vc->read_from_net(*ethread);
        } else if (!vc->read_.enabled_) {
          vc->ep_->modify(-EVENTIO_READ);
        }
      }

      while (NULL != (vc = write_ready_list_.dequeue())) {
        if (vc->closed_) {
          if (OB_UNLIKELY(OB_SUCCESS != (close_ret = vc->close()))) {
            PROXY_NET_LOG(WARN, "fail to close unix net vconnection", K(vc), K(close_ret));
          }
        } else if (vc->using_ssl() && (vc->read_.enabled_ || vc->write_.enabled_)) {
          vc->do_ssl_io(*ethread);
        } else if (vc->write_.enabled_ && vc->write_.triggered_ && !vc->using_ssl()) {
          vc->write_to_net(*ethread);
        } else if (!vc->write_.enabled_) {
          vc->ep_->modify(-EVENTIO_WRITE);
        }
      }
#endif // !USE_EDGE_TRIGGER
    }
  }
  return (OB_SUCCESS == ret) ? EVENT_CONT : EVENT_ERROR;
}

} // end of namespace net
} // end of namespace obproxy
} // end of namespace oceanbase
