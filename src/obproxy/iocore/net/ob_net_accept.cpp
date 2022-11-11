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

#include "iocore/net/ob_net_accept.h"
#include "iocore/net/ob_net.h"
#include "iocore/net/ob_event_io.h"
#include "omt/ob_resource_unit_table_processor.h"
#include "proxy/mysql/ob_mysql_client_session.h"
#include "proxy/route/ob_table_cache.h"
#include "obutils/ob_congestion_manager.h"
#include "proxy/route/ob_partition_cache.h"
#include "proxy/route/ob_routine_cache.h"
#include "proxy/route/ob_sql_table_cache.h"
#include "prometheus/ob_prometheus_processor.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::omt;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::prometheus;

namespace oceanbase
{
namespace obproxy
{
namespace net
{

static const bool accept_till_done = true;

inline int do_net_accept(ObNetAccept *na, void *ep, const bool blockable)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(na) || OB_ISNULL(ep)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_NET_LOG(WARN, "invalid argument", K(na), K(ep), K(ret));
  } else {
    ObEvent *e = reinterpret_cast<ObEvent *>(ep);
    bool loop = accept_till_done;
    bool is_vc_from_cache = true;
    ObUnixNetVConnection *vc = NULL;
    ObConnection con;

    //do-while for accepting all the connections
    do {
      vc = reinterpret_cast<ObUnixNetVConnection *>(na->alloc_cache_);
      if (NULL == vc) {
        vc = reinterpret_cast<ObUnixNetVConnection *>(na->get_net_processor()->allocate_vc());
        if (OB_ISNULL(vc)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          PROXY_NET_LOG(ERROR, "failed to allocate ObUnixNetVConnection", K(ret));
        } else {
          is_vc_from_cache = false;
          NET_SUM_GLOBAL_DYN_STAT(NET_GLOBAL_CONNECTIONS_CURRENTLY_OPEN, 1);
          NET_SUM_GLOBAL_DYN_STAT(NET_GLOBAL_CLIENT_CONNECTIONS_CURRENTLY_OPEN, 1);
          na->alloc_cache_ = vc;
        }
      }

      if(OB_SUCC(ret)) {
        if (OB_SUCCESS != (ret = na->server_.accept(&con))) {
          if (OB_SYS_EAGAIN == ret
              || OB_UNLIKELY(OB_SYS_ECONNABORTED == ret)
              || OB_UNLIKELY(OB_SYS_EPIPE == ret)) {
          } else {
            if (NO_FD != na->server_.fd_ && !na->action_->cancelled_) {
              if (!blockable) {
                na->action_->continuation_->handle_event(EVENT_ERROR,
                  reinterpret_cast<void *>(static_cast<uintptr_t>(ret)));
              } else {
                MUTEX_LOCK(lock, na->action_->mutex_, e->ethread_);
                na->action_->continuation_->handle_event(EVENT_ERROR,
                  reinterpret_cast<void *>(static_cast<uintptr_t>(ret)));
              }
            }
          }
        } else if (OB_FAIL(na->set_sock_buf_size(na->server_.fd_))) {
          PROXY_NET_LOG(ERROR, "fail to set_sock_buf_size", K(ret));
        } else if (OB_FAIL(na->init_unix_net_vconnection(con, vc))) {
          PROXY_NET_LOG(ERROR, "fail to init_unix_net_vconnection", K(ret));
        } else {
          na->alloc_cache_ = NULL;
          vc->action_.copy(*na->action_);
          if (!is_vc_from_cache) {
            NET_ATOMIC_INCREMENT_DYN_STAT(e->ethread_, NET_CLIENT_CONNECTIONS_CURRENTLY_OPEN);
          }
          SET_CONTINUATION_HANDLER(vc, reinterpret_cast<NetVConnHandler>(&ObUnixNetVConnection::accept_event));

          if (e->ethread_->is_event_thread_type(na->etype_)) {
            vc->handle_event(EVENT_NONE, e);
          } else {
            if(OB_ISNULL(g_event_processor.schedule_imm(vc, na->etype_))) {
              ret = OB_ERR_UNEXPECTED;
              PROXY_NET_LOG(ERROR, "g_event_processor fail to schedule_imm", K(ret));
            }
          }
        }
      }
    } while (loop && OB_SUCC(ret));
  }
  return ret;
}

// General case network connection accept code
int net_accept(ObNetAccept *na, void *ep, const bool blockable)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(na) || OB_ISNULL(ep)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_NET_LOG(WARN, "invalid argument", K(na), K(ep), K(ret));
  } else {
    if (blockable) {
      if (OB_FAIL(do_net_accept(na, ep, blockable))
          && OB_SYS_EAGAIN != ret) {
        PROXY_NET_LOG(WARN, "fail to do net accept", K(na), K(ep), K(ret));
      }
    } else {
      ObEvent *e = reinterpret_cast<ObEvent *>(ep);
      MUTEX_TRY_LOCK(accept_lock, na->action_->mutex_, e->ethread_);
      if (accept_lock.is_locked()) {
        if (OB_FAIL(do_net_accept(na, ep, blockable))
            && OB_SYS_EAGAIN != ret) {
          PROXY_NET_LOG(WARN, "fail to do net accept", K(na), K(ep), K(ret));
        }
      }//end of locked
    }
  }
  return ret;
}

// Initialize the ObNetAccept for execution in its own thread.
// This should be done for low latency, high connection rate sockets.
int ObNetAccept::init_accept_loop(const char *thread_name, const int64_t stacksize)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(thread_name) || OB_UNLIKELY(stacksize < 0)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_NET_LOG(WARN, "invalid argument", K(thread_name), K(stacksize), K(ret));
  } else {
    NET_SUM_GLOBAL_DYN_STAT(NET_GLOBAL_ACCEPTS_CURRENTLY_OPEN, 1);
    SET_CONTINUATION_HANDLER(this, &ObNetAccept::accept_loop_event);
    if (OB_ISNULL(g_event_processor.spawn_thread(this, thread_name, stacksize, DEDICATE_THREAD_ACCEPT))) {
      ret = OB_ERR_UNEXPECTED;
      PROXY_NET_LOG(ERROR, "g_event_processor fail to spawn_thread", K(ret));
    }
  }
  return ret;
}

// Initialize the ObNetAccept for execution in a etype thread.
// This should be done for low connection rate sockets.
// (Management, Cluster, etc.)  Also, since it adapts to the
// number of connections arriving, it should be reasonable to
// use it for high connection rates as well.
int ObNetAccept::init_accept()
{
  int ret = OB_SUCCESS;
  ObEThread *t = g_event_processor.assign_thread(etype_);
  if (OB_ISNULL(t)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_NET_LOG(ERROR, "g_event_processor fail to assign_thread", K(ret));
  } else {
    if (NULL == action_->continuation_->mutex_) {
      action_->continuation_->mutex_ = t->mutex_;
      action_->mutex_ = t->mutex_;
    }

    if (OB_FAIL(do_listen(NON_BLOCKING))) {
      PROXY_NET_LOG(ERROR, "fail to listen", K(ret));
    } else {
      SET_HANDLER((NetAcceptHandler)&ObNetAccept::accept_event);
      period_ = ACCEPT_PERIOD;
      if (OB_ISNULL(t->schedule_every(this, period_, etype_))) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_NET_LOG(ERROR, "fail to schedule_every", K(ret));
      } else {
        NET_SUM_GLOBAL_DYN_STAT(NET_GLOBAL_ACCEPTS_CURRENTLY_OPEN, 1);
      }
    }
  }
  return ret;
}

int ObNetAccept::init_accept_per_thread()
{
  int ret = OB_SUCCESS;
  ObEThread *t = NULL;

  if (OB_FAIL(do_listen(NON_BLOCKING))) {
    PROXY_NET_LOG(ERROR, "fail to listen", K(ret));
  } else {
    if (accept_fn_ == net_accept) {
      SET_HANDLER((NetAcceptHandler)&ObNetAccept::accept_fast_event);
    } else {
      SET_HANDLER((NetAcceptHandler)&ObNetAccept::accept_event);
    }
    period_ = ACCEPT_PERIOD;
    ObNetAccept *na = NULL;
    int64_t n = g_event_processor.thread_count_for_type_[ET_NET];
    NET_SUM_GLOBAL_DYN_STAT(NET_GLOBAL_ACCEPTS_CURRENTLY_OPEN, n);

    for (int64_t i = 0; i < n && OB_SUCC(ret); ++i) {
      if (i < n - 1) {
        if (OB_ISNULL(na = new (std::nothrow) ObNetAccept())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          PROXY_NET_LOG(ERROR, "fail to new ObNetAccept");
        } else if (OB_FAIL(na->deep_copy(*this))) {
          NET_SUM_GLOBAL_DYN_STAT(NET_GLOBAL_ACCEPTS_CURRENTLY_OPEN, -1);
          PROXY_NET_LOG(ERROR, "fail to deep_copy", K(i), K(ret));
        }
      } else {
        na = this;
      }

      if (OB_SUCC(ret)) {
        t = g_event_processor.event_thread_[ET_NET][i];
        if (OB_ISNULL(t)) {
          ret = OB_ERR_UNEXPECTED;
          PROXY_NET_LOG(ERROR, "g_event_processor fail to get ET_NET ObEThread", K(ret));
        } else {
          na->mutex_ = t->get_net_handler().mutex_;
          if(OB_FAIL(na->ep_->start(t->get_net_poll().get_poll_descriptor(),
                                    *na, EVENTIO_READ))) {
            PROXY_NET_LOG(ERROR, "fail to start ObEventIO", K(ret));
          } else if (OB_ISNULL(t->schedule_every(na, period_, etype_))) {
            ret = OB_ERR_UNEXPECTED;
            PROXY_NET_LOG(ERROR, "fail to schedule_every", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObNetAccept::do_listen(const bool non_blocking)
{

  int ret = OB_SUCCESS;
  PROXY_NET_LOG(DEBUG, "ObNetAccept::do_listen", K(server_.fd_), K(non_blocking));
  const ObHotUpgraderInfo &info = get_global_hot_upgrade_info();

  if (NO_FD != server_.fd_) {
    if (!info.is_inherited_) {
      if (OB_FAIL(server_.setup_fd_for_listen(non_blocking, recv_bufsize_, send_bufsize_))) {
        PROXY_NET_LOG(INFO, "fail to setup_fd_for_listen on server",
                      K(server_.accept_addr_), K(ret));
        // retry
        if (OB_FAIL(server_.listen(non_blocking, recv_bufsize_, send_bufsize_))) {
          PROXY_NET_LOG(ERROR, "fail to setup_fd_for_listen on server",
                        K(server_.accept_addr_), KERRMSGS, K(ret));
        }
      }
    } else {
      // come from inherited, we should fill ObServerConnection's addr_
      int64_t name_len = sizeof(server_.addr_);
      if (OB_FAIL(ObSocketManager::getsockname(server_.fd_, &server_.addr_.sa_, &name_len))) {
        PROXY_NET_LOG(ERROR, "fail to get socket name",
                      K(server_.fd_), K(server_.accept_addr_), KERRMSGS, K(ret));
      } else {
        PROXY_NET_LOG(DEBUG, "succ to get socket name",
                      K(server_.fd_), K(server_.accept_addr_), K(ret));

      }
    }
  } else {
    if (OB_FAIL(server_.listen(non_blocking, recv_bufsize_, send_bufsize_))) {
      PROXY_NET_LOG(ERROR, "fail to listen", K(server_.accept_addr_), KERRMSGS, K(ret));
    } else {
      PROXY_NET_LOG(DEBUG, "succ to listen", K(server_.accept_addr_), K(ret));
    }
  }

  if (OB_ISNULL(action_->continuation_)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_NET_LOG(ERROR, "fail to get action_->continuation_", K(ret));
  } else {
    if (callback_on_open_ && !action_->cancelled_) {
      if (OB_FAIL(ret)) {
        // NET_EVENT_ACCEPT_FAILED indicates do listen failed, maybe fail to bind,
        // fail to setsockopt, fail to listen, etc.;
        // NET_EVENT_ACCEPT_SUCCEED indicates do listen succ;
        action_->continuation_->handle_event(NET_EVENT_ACCEPT_FAILED, reinterpret_cast<void *>(static_cast<uintptr_t>(ret)));
      } else {
        action_->continuation_->handle_event(NET_EVENT_ACCEPT_SUCCEED, this);
      }
      mutex_.release();
    }
  }

  return ret;
}

int ObNetAccept::fetch_vip_tenant(ObUnixNetVConnection* vc, ObVipTenant& vip_tenant, bool& lookup_success)
{
  int ret = OB_SUCCESS;
  int32_t ip;
  int32_t port;
  int64_t vid;
  if (OB_ISNULL(vc)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_NET_LOG(WARN, "vc pointer is null", K(ret));
  } else {
    ip = ntohl(vc->get_virtual_ip());
    port = static_cast<int32_t>(vc->get_virtual_port());
    vid = static_cast<int64_t>(vc->get_virtual_vid());
    vip_tenant.vip_addr_.set(ip, port, vid);
    if (OB_FAIL(get_global_vip_tenant_processor().get_vip_tenant(vip_tenant))) {
      PROXY_NET_LOG(DEBUG, "fail to get vip tenant", K(ret));
      ret = OB_SUCCESS;
    } else {
      lookup_success = true;
    }
  }
  return ret;
}

int ObNetAccept::fetch_tenant_cpu(ObVipTenant& vip_tenant, ObTenantCpu*& tenant_cpu, bool& lookup_success)
{
  int ret = OB_SUCCESS;
  ObString key_name;
  char vip_name[OB_IP_STR_BUFF];
  common::ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH + OB_IP_STR_BUFF> key_string;
  if (OB_UNLIKELY(!vip_tenant.vip_addr_.addr_.ip_to_string(vip_name, static_cast<int32_t>(sizeof(vip_name))))) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_NET_LOG(WARN, "fail to covert ip to string", K(vip_name), K(ret));
  } else if (OB_FAIL(build_tenant_cluster_vip_name(vip_tenant.tenant_name_, vip_tenant.cluster_name_, vip_name, key_string))) {
    PROXY_NET_LOG(WARN, "build tenant cluser vip name failed", K(vip_tenant), K(ret));
  } else if (FALSE_IT(key_name = ObString::make_string(key_string.ptr()))) {
  } else if (OB_FAIL(get_global_cpu_table_processor().get_tenant_cpu(key_name, tenant_cpu))) {
    PROXY_NET_LOG(DEBUG, "get tenant cpu failed", K(key_name), K(ret));
  } else {
    lookup_success = true;
  }
  return ret;
}

int ObNetAccept::handle_tenant_cpu_isolated(ObTenantCpu* tenant_cpu, ObEThread*& ethread)
{
  int ret = OB_SUCCESS;
  // 1. Assign as many threads as there are max_thread_num, and then save the thread id
  // 2. Create the tenant's cgroup filesystem
  //    2.1 Write cfs_period_us and cfs_quota_us to the cgroup filesystem
  //    2.2 tasks that write the thread id to the cgroup filesystem
  if (OB_ISNULL(tenant_cpu)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_NET_LOG(WARN, "tenant cpu point is null", K(ret));
  } else {
    if (tenant_cpu->thread_array_.count() < tenant_cpu->max_thread_num_) {
      DRWLock::WRLockGuard guard(g_event_processor.lock_);
      if (tenant_cpu->thread_array_.count() < tenant_cpu->max_thread_num_) {
        ethread = get_schedule_vip_ethread();
        if (OB_ISNULL(ethread)) {
          ret = OB_ERR_UNEXPECTED;
          PROXY_NET_LOG(ERROR, "fail to get_shedule_ethread", K(ret));
        } else if (OB_FAIL(tenant_cpu->acquire_more_worker(ethread->thread_id_))) {
          PROXY_NET_LOG(WARN, "acquire more worker failed", K(ret));
        } else {
          ethread->use_status_ = true;
          tenant_cpu->thread_array_.push_back(ethread);
        }
      } else {
        ethread = tenant_cpu->get_tenant_schedule_ethread();
      }
    } else {
      ethread = tenant_cpu->get_tenant_schedule_ethread();
    }
  }

  // for debug
  if (OB_NOT_NULL(ethread)) {
    PROXY_NET_LOG(DEBUG, "has acquire thread", K(ethread->thread_id_), KPC(tenant_cpu), KPC(ethread));
  }

  return ret;
}

int ObNetAccept::do_blocking_accept()
{
  int ret = OB_SUCCESS;
  bool loop = accept_till_done;
  ObConnection con;
  ObUnixNetVConnection *vc = NULL;
  ObEThread *ethread = NULL;
  const ObHotUpgraderInfo &info = get_global_hot_upgrade_info();

  do {
    // Although need_conn_accept_ has disenabled, it can still accept one connection
    // as it is blocking accept.
    // If proxy is pthread_cancel this thread but accept a new connection at the same time,
    // the connect would not be handled. It is small probability event and inescapability.
    if (OB_UNLIKELY(!info.need_conn_accept_)) {
      (void)sleep(1);
      continue;
    }

    if (OB_FAIL(server_.accept(&con, true))) {
      if (OB_LIKELY(OB_SYS_EINTR == ret && !info.need_conn_accept_)) {
        PROXY_NET_LOG(DEBUG, "accept interrupted when need_conn_accept is false", K(ret), "need_conn_accept", info.need_conn_accept_);
      } else {
        PROXY_NET_LOG(ERROR, "failed to accept con", K(ret));
      }
    } else {
      NET_SUM_GLOBAL_DYN_STAT(NET_GLOBAL_CONNECTIONS_CURRENTLY_OPEN, 1);
      NET_SUM_GLOBAL_DYN_STAT(NET_GLOBAL_CLIENT_CONNECTIONS_CURRENTLY_OPEN, 1);

      // Use 'NULL' to Bypass thread allocator
      vc = static_cast<ObUnixNetVConnection *>(get_net_processor()->allocate_vc());
      if (OB_ISNULL(vc)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        PROXY_NET_LOG(ERROR ,"failed to allocate_vc, current accepted connection will be closed",
                      K(con.addr_), K(con.fd_));
        if (OB_SUCCESS != con.close()) {
          PROXY_NET_LOG(WARN, "failed to close connection");
        }
        NET_SUM_GLOBAL_DYN_STAT(NET_GLOBAL_CONNECTIONS_CURRENTLY_OPEN, -1);
        NET_SUM_GLOBAL_DYN_STAT(NET_GLOBAL_CLIENT_CONNECTIONS_CURRENTLY_OPEN, -1);
      } else if (OB_FAIL(init_unix_net_vconnection(con, vc))) {
        PROXY_NET_LOG(ERROR, "fail to init_unix_net_vconnection", K(ret));
        if (OB_SUCCESS != con.close()) {
          PROXY_NET_LOG(WARN, "failed to close connection");
        }
        vc->free();
      }
    }

    if(OB_SUCC(ret)) {
      alloc_cache_ = NULL;
      vc->action_.copy(*action_);

      SET_CONTINUATION_HANDLER(vc, reinterpret_cast<NetVConnHandler>(&ObUnixNetVConnection::accept_event));

      ObVipTenant vip_tenant;
      ObTenantCpu* tenant_cpu = NULL;
      bool lookup_vip_tenant_success = false;
      bool lookup_tenant_cpu_success = false;
      if (get_global_proxy_config().enable_cpu_isolate) {
        fetch_vip_tenant(vc, vip_tenant, lookup_vip_tenant_success);
        if (lookup_vip_tenant_success) {
          fetch_tenant_cpu(vip_tenant, tenant_cpu, lookup_tenant_cpu_success);
        }
        if (lookup_vip_tenant_success && lookup_tenant_cpu_success) {
          if (OB_FAIL(handle_tenant_cpu_isolated(tenant_cpu, ethread))) {
            PROXY_NET_LOG(ERROR, "handle tenant cpu isolate failed", K(ret));
          }
          tenant_cpu->dec_ref();
        } else {
          ethread = get_schedule_other_ethread();
        }
      } else {
        ethread = get_schedule_ethread();
      }

      if (OB_ISNULL(ethread)) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_NET_LOG(ERROR, "fail to get_shedule_ethread", K(ret));
      } else {
        NET_ATOMIC_INCREMENT_DYN_STAT(ethread, NET_CLIENT_CONNECTIONS_CURRENTLY_OPEN);
        if (OB_ISNULL(ethread->schedule_imm_signal(vc))) {
          ret = OB_ERR_UNEXPECTED;
          PROXY_NET_LOG(ERROR, "fail to schedule_imm_signal vc", K(ret));
        }
      }
    }
    // if OB_SUCCESS != ret, we can't break loop here,
    // it will cause this thread to exit, even obproxy will core.
    // so just print ERROR log.
  } while (loop);
  return ret;
}

inline ObEThread *ObNetAccept::get_schedule_ethread()
{
  ObEventThreadType etype = ET_NET;
  int64_t net_thread_count = g_event_processor.thread_count_for_type_[etype];
  ObEThread **netthreads = g_event_processor.event_thread_[etype];
  ObEThread *target_ethread = NULL;
  int64_t min_conn_cnt = -1;
  int64_t tmp_cnt = -1;

  for (int64_t i = 0; i < net_thread_count; ++i) {
    NET_THREAD_READ_DYN_SUM(netthreads[i], NET_CLIENT_CONNECTIONS_CURRENTLY_OPEN, tmp_cnt);
    if (0 == i || tmp_cnt < min_conn_cnt) {
      min_conn_cnt = tmp_cnt;
      target_ethread = netthreads[i];
    }
    tmp_cnt = -1;
  }
  return target_ethread;
}

inline ObEThread *ObNetAccept::get_schedule_other_ethread()
{
  ObEventThreadType etype = ET_NET;
  ObEThread **netthreads = g_event_processor.event_thread_[etype];
  ObEThread *target_ethread = NULL;
  int64_t min_conn_cnt = -1;
  int64_t tmp_cnt = -1;

  for (int64_t i = 0; i < MAX_OTHER_GROUP_NET_THREADS; ++i) {
    NET_THREAD_READ_DYN_SUM(netthreads[i], NET_CLIENT_CONNECTIONS_CURRENTLY_OPEN, tmp_cnt);
    if (0 == i || tmp_cnt < min_conn_cnt) {
      min_conn_cnt = tmp_cnt;
      target_ethread = netthreads[i];
    }
    tmp_cnt = -1;
  }
  return target_ethread;
}

inline ObEThread *ObNetAccept::get_schedule_vip_ethread()
{
  int ret = OB_SUCCESS;
  ObEventThreadType etype = ET_NET;
  int64_t net_thread_count = g_event_processor.thread_count_for_type_[etype];
  ObEThread **netthreads = g_event_processor.event_thread_[etype];
  ObEThread *target_ethread = NULL;

  for (int64_t i = MAX_OTHER_GROUP_NET_THREADS; i < net_thread_count; ++i) {
    if (!netthreads[i]->use_status_) {
      target_ethread = netthreads[i];
      break;
    }
  }

  if (OB_ISNULL(target_ethread)) {
    if (OB_FAIL(create_one_net_ethread(target_ethread))) {
      PROXY_NET_LOG(WARN, "create net thread failed", K(ret));
    }
  }

  return target_ethread;
}

int ObNetAccept::create_one_net_ethread(ObEThread*& target_ethread)
{
  int ret = OB_SUCCESS;
  target_ethread = NULL;
  bool is_new_net_thread = false;
  int64_t event_thread_count = 0;
  int64_t net_thread_count = g_event_processor.thread_count_for_type_[ET_CALL];
  int64_t stack_size = get_global_proxy_config().stack_size;

  if (OB_FAIL(g_event_processor.spawn_net_threads(1, "ET_NET", stack_size))) {
    PROXY_NET_LOG(ERROR, "fail to spawn event threads for ET_NET", K(ret));
  } else {
    is_new_net_thread = true;
    event_thread_count = g_event_processor.event_thread_count_;
    net_thread_count = g_event_processor.thread_count_for_type_[ET_CALL];
    target_ethread = g_event_processor.event_thread_[ET_CALL][net_thread_count - 1];
    if (OB_FAIL(initialize_thread_for_net(target_ethread))) {
      PROXY_NET_LOG(ERROR, "fail to initialize thread for net", K(ret));
    } else if (OB_FAIL(init_cs_map_for_one_thread(net_thread_count - 1))) {
      PROXY_NET_LOG(ERROR, "fail to init cs_map for thread", K(net_thread_count), K(ret));
    } else if (OB_FAIL(init_table_map_for_one_thread(net_thread_count - 1))) {
      PROXY_NET_LOG(ERROR, "fail to init table_map for thread", K(net_thread_count), K(ret));
    } else if (OB_FAIL(init_congestion_map_for_one_thread(net_thread_count - 1))) {
      PROXY_NET_LOG(ERROR, "fail to init congestion_map for thread", K(net_thread_count), K(ret));
    } else if (OB_FAIL(init_partition_map_for_one_thread(net_thread_count - 1))) {
      PROXY_NET_LOG(ERROR, "fail to init partition_map for thread", K(net_thread_count), K(ret));
    } else if (OB_FAIL(init_routine_map_for_one_thread(net_thread_count - 1))) {
      PROXY_NET_LOG(ERROR, "fail to init routine_map for thread", K(net_thread_count), K(ret));
    } else if (OB_FAIL(init_sql_table_map_for_one_thread(net_thread_count - 1))) {
      PROXY_NET_LOG(ERROR, "fail to init sql_table_map for thread", K(net_thread_count), K(ret));
    } else if (OB_FAIL(init_ps_entry_cache_for_one_thread(net_thread_count - 1))) {
      PROXY_NET_LOG(ERROR, "fail to init ps entry cache for thread", K(net_thread_count), K(ret));
    } else if (OB_FAIL(init_text_ps_entry_cache_for_one_thread(net_thread_count - 1))) {
      PROXY_NET_LOG(ERROR, "fail to init text ps entry cache for thread", K(net_thread_count), K(ret));
    } else if (OB_FAIL(init_random_seed_for_one_thread(net_thread_count - 1))) {
      PROXY_NET_LOG(ERROR, "fail to init random seed for thread", K(net_thread_count), K(ret));
    } else if (OB_FAIL(ObCacheCleaner::schedule_one_cache_cleaner(net_thread_count - 1))) {
      PROXY_NET_LOG(ERROR, "fail to init cleaner cache for thread", K(net_thread_count), K(ret));
    } else if (OB_FAIL(g_ob_prometheus_processor.start_one_prometheus(net_thread_count - 1))) {
      PROXY_NET_LOG(ERROR, "fail to start one prometheus", K(net_thread_count), K(ret));
    } else {
      PROXY_NET_LOG(INFO, "new thread", K(ET_CALL), K(net_thread_count), KPC(target_ethread));
    }
  }

  if (OB_FAIL(ret) && is_new_net_thread) {
    delete target_ethread;
    target_ethread = NULL;
    g_event_processor.all_event_threads_[event_thread_count - 1] = NULL;
    g_event_processor.event_thread_[ET_CALL][net_thread_count -1] = NULL;
    g_event_processor.thread_count_for_type_[ET_CALL] -= 1;
    g_event_processor.event_thread_count_ -= 1;
  }
  return ret;
}

inline bool ObNetAccept::accept_balance(ObEThread *ethread)
{
  ObEThread *scheduled_ethread = get_schedule_ethread();
  return (NULL == ethread)
         || (NULL == scheduled_ethread)
         || (ethread == scheduled_ethread);
}

int ObNetAccept::accept_event(int event, void *ep)
{
  UNUSED(event);
  int ret = OB_SUCCESS;
  int event_ret = EVENT_CONT;
  if (OB_ISNULL(ep)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_NET_LOG(WARN, "invalid argument", K(ep), K(ret));
  } else {
    ObEvent *e = reinterpret_cast<ObEvent *>(ep);
    ObProxyMutex *m = NULL;
    const ObHotUpgraderInfo &info = get_global_hot_upgrade_info();

    if (OB_LIKELY(info.need_conn_accept_)) {
      if (NULL != action_->mutex_) {
        m = action_->mutex_;
      } else {
        m = mutex_;
      }

      MUTEX_TRY_LOCK(lock, m, e->ethread_);
      if (lock.is_locked()) {
        if (action_->cancelled_) {
          ret = OB_CANCELED;
        } else if (OB_FAIL(accept_fn_(this, e, false))) {
          if (OB_SYS_EAGAIN == ret
              || OB_UNLIKELY(OB_SYS_ECONNABORTED == ret)
              || OB_UNLIKELY(OB_SYS_EPIPE == ret)) {
            ret = OB_SUCCESS;
          } else {
            PROXY_NET_LOG(ERROR, "fail to do accept_fn_", K(&server_.addr_), K(ret));
          }
        }

        if (OB_FAIL(ret)) {
          NET_SUM_GLOBAL_DYN_STAT(NET_GLOBAL_ACCEPTS_CURRENTLY_OPEN, -1);
          if (OB_FAIL(e->cancel())) {
            PROXY_NET_LOG(WARN, "fail to cancel event", K(ret));
          }
          delete this;
          event_ret = EVENT_DONE;
        }
      }
    }
  }
  return event_ret;
}

inline int ObNetAccept::set_sock_buf_size(const int fd)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(fd < 0)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_NET_LOG(WARN, "invalid argument", K(fd), K(ret));
  } else if (OB_FAIL(ObSocketManager::set_sndbuf_and_rcvbuf_size(
      fd, send_bufsize_, recv_bufsize_, 1024))){
    PROXY_NET_LOG(WARN, "fail to set_sndbuf_and_rcvbuf_size",
                  K(send_bufsize_), K(recv_bufsize_), K(ret));
  }
  return ret;
}

int ObNetAccept::accept_fast_event(int event, void *ep)
{
  UNUSED(event);
  int ret = OB_SUCCESS;
  int net_ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS; // tmp use
  int event_ret = EVENT_CONT;
  ObConnection con;
  ObUnixNetVConnection *vc = NULL;
  ObHotUpgraderInfo &info = get_global_hot_upgrade_info();

  if (OB_ISNULL(ep)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_NET_LOG(WARN, "invalid argument", K(ep), K(ret));
  } else if (OB_UNLIKELY(!info.need_conn_accept_)) {
    //do nothing
  } else {
    ObEvent *e = reinterpret_cast<ObEvent *>(ep);
    bool need_close_vc = false;
    bool loop = accept_till_done;

    if (OB_ISNULL(e->ethread_)) {
      ret = OB_ERR_UNEXPECTED;
      PROXY_NET_LOG(ERROR, "fail to get ethread", K(ret));
    } else {
      while (loop && OB_SUCC(ret)) {
        if (!accept_balance(e->ethread_)) { // for balance
          ret = OB_SYS_EAGAIN;
          net_ret = ret;
          con.fd_ = NO_FD;
        } else if (OB_FAIL(server_.accept(&con))) {
          net_ret = ret;
          con.fd_ = NO_FD;
          if (OB_SYS_EAGAIN != ret) {
            PROXY_NET_LOG(ERROR, "fail to accept", K(server_.accept_addr_), K(server_.fd_), K(ret));
          }
        } else if (OB_FAIL(set_sock_buf_size(con.fd_))) {
          PROXY_NET_LOG(ERROR, "fail to set_sock_buf_size", K(con.addr_), K(con.fd_), K(ret));
        } else if (OB_ISNULL(vc = static_cast<ObUnixNetVConnection *>(get_net_processor()->allocate_vc()))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          PROXY_NET_LOG(ERROR, "fail to allocate_vc", K(con.addr_), K(con.fd_), K(ret));
        } else if (OB_FAIL(init_unix_net_vconnection(con, vc))) {
          PROXY_NET_LOG(ERROR, "fail to init_unix_net_vconnection", K(ret));
        } else {
          need_close_vc = false;
          vc->nh_ = &(e->ethread_->get_net_handler());
          vc->thread_ = e->ethread_;

          NET_SUM_GLOBAL_DYN_STAT(NET_GLOBAL_CONNECTIONS_CURRENTLY_OPEN, 1);
          NET_SUM_GLOBAL_DYN_STAT(NET_GLOBAL_CLIENT_CONNECTIONS_CURRENTLY_OPEN, 1);

          SET_CONTINUATION_HANDLER(vc, reinterpret_cast<NetVConnHandler>(&ObUnixNetVConnection::main_event));

          if (OB_FAIL(vc->ep_->start(e->ethread_->get_net_poll().get_poll_descriptor(),
                                     *vc, EVENTIO_READ | EVENTIO_WRITE))) {
            PROXY_NET_LOG(ERROR, "fail to start ObEventIO", K(con.addr_), K(con.fd_), K(ret));
          } else {
            vc->nh_->open_list_.enqueue(vc);
#ifdef USE_EDGE_TRIGGER
            // Set the vc as triggered and place it in the read ready queue in case
            // there is already data on the socket.
            PROXY_NET_LOG(DEBUG, "Setting triggered and adding to the read ready queue", K(con.addr_), K(con.fd_));
            vc->read_.triggered_ = true;
            vc->nh_->read_ready_list_.enqueue(vc);
#endif
            if (!action_->cancelled_) {
              // We must be holding the lock already to do later do_io_read's
              MUTEX_LOCK(lock, vc->mutex_, e->ethread_);
              if (OB_ISNULL(action_->continuation_)) {
                ret = OB_ERR_UNEXPECTED;
                PROXY_NET_LOG(ERROR, "fail to get action_->continuation_", K(con.addr_), K(con.fd_), K(ret));
              } else {
                NET_ATOMIC_INCREMENT_DYN_STAT(e->ethread_, NET_CLIENT_CONNECTIONS_CURRENTLY_OPEN);
                action_->continuation_->handle_event(NET_EVENT_ACCEPT, vc);
              }
            } else {
              need_close_vc = true;
            }
          }
          // if failed or need close vc, close vc and dec stat
          if (OB_FAIL(ret) || need_close_vc) {
            if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = vc->close()))) {
              PROXY_NET_LOG(WARN, "fail to close unix net vconnection", K(vc), K(tmp_ret));
            }
            vc = NULL;
          }
        }

        // here only handle net_ret
        if (OB_SUCCESS != net_ret) {
          if (OB_SYS_EAGAIN == net_ret || OB_SYS_ECONNABORTED == net_ret
#if defined(linux)
              || OB_SYS_EPIPE == net_ret
#endif
             ) {
          // do nothing
          } else if (accept_error_seriousness(net_ret) >= 0) {
            check_transient_accept_error(net_ret);
          } else {
            // fatal error, this ObNetAccept will stop accept and delete self
            if (!action_->cancelled_) {
              action_->continuation_->handle_event(EVENT_ERROR, reinterpret_cast<void *>(static_cast<uintptr_t>(net_ret)));
            }
            if (OB_SUCCESS != (tmp_ret = server_.close())) {
              PROXY_NET_LOG(WARN, "failed to close server connection", K(tmp_ret));
            }
            if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = e->cancel()))) {
              PROXY_NET_LOG(WARN, "fail to cancel ObEvent", K(e), K(tmp_ret));
            }
            if (NULL != vc) {
              vc->free();
              vc = NULL;
            }
            event_ret = EVENT_DONE;
            NET_SUM_GLOBAL_DYN_STAT(NET_GLOBAL_ACCEPTS_CURRENTLY_OPEN, -1);
            PROXY_NET_LOG(ERROR, "fatal error, the ObNetAccept will stop "
                          "accept and delete self", K(e), K(net_ret));
            delete this;
          }
        }
      }

      // if failed, close the connect at last
      if (OB_FAIL(ret)) {
        if ((NO_FD != con.fd_) && OB_UNLIKELY(OB_SUCCESS != (tmp_ret = con.close()))) {
          PROXY_NET_LOG(WARN, "failed to close connection", K(tmp_ret), K(ret));
        }
      }
    }
  }

  return event_ret;
}

void free_netaccept(void *param)
{
  if (OB_ISNULL(param)) {
    PROXY_NET_LOG(WARN ,"invalid argument", K(param));
  } else {
    ObNetAccept *accept = static_cast<ObNetAccept *>(param);
    delete accept;
  }
}

int ObNetAccept::init_unix_net_vconnection(const ObConnection &con, ObUnixNetVConnection *vc)
{
  int ret = OB_SUCCESS;
  ObProxyMutex *mutex = NULL;

  if (OB_ISNULL(vc)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_NET_LOG(WARN, "invalid argument", K(vc), K(ret));
  } else if (OB_ISNULL(mutex = new_proxy_mutex(NET_VC_LOCK))) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_NET_LOG(ERROR, "fail to new_proxy_mutex", K(ret));
  } else if (OB_FAIL(vc->apply_options())) {
    PROXY_NET_LOG(ERROR, "fail to apply_options", K(ret));
  } else {
    vc->con_ = con;
    vc->mutex_ = mutex;
    vc->options_.sockopt_flags_ = sockopt_flags_;
    vc->options_.packet_mark_ = packet_mark_;
    vc->options_.packet_tos_ = packet_tos_;
    vc->source_type_ = ObUnixNetVConnection::VC_ACCEPT;
    vc->id_ = net_next_connection_number();
    vc->submit_time_ = get_hrtime();
    vc->closed_ = 0;
  }
  return ret;
}

int ObNetAccept::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    PROXY_NET_LOG(ERROR, "init ObNetAccept twice", K(this), K(ret));
  } else if (OB_ISNULL(ep_ = new (std::nothrow) ObEventIO())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_NET_LOG(ERROR, "fail to new ObEventIO", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObNetAccept::accept_loop_event(int event, ObEvent *e)
{
  UNUSED(event);
  UNUSED(e);
  int ret = OB_SUCCESS;

  //open the cancel  switch
  if (OB_FAIL(pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL))) {
    PROXY_NET_LOG(WARN, "fail to do pthread_setcancelstate PTHREAD_CANCEL_ENABLE", K(ret));
  } else if (OB_FAIL(pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL))) {
    PROXY_NET_LOG(WARN, "fail to do pthread_setcanceltype PTHREAD_CANCEL_ASYNCHRONOUS", K(ret));
  } else {
    // current thread maybe pthread_cancel in other place, we need register its free func
    pthread_cleanup_push(free_netaccept, static_cast<void *>(this));
    while (do_blocking_accept() >= 0) { }
    pthread_cleanup_pop(1);
  }
  return ret;
}

// Accept ObEvent handler
ObNetAccept::ObNetAccept()
    : ObContinuation(NULL),
      alloc_cache_(NULL),
      accept_fn_(NULL),
      callback_on_open_(false),
      backdoor_(false),
      recv_bufsize_(0),
      send_bufsize_(0),
      sockopt_flags_(0),
      packet_mark_(0),
      packet_tos_(0),
      etype_(ET_CALL),
      is_inited_(false),
      period_(0),
      epoll_vc_(NULL),
      ep_(NULL)
{
}

ObNetAccept::~ObNetAccept()
{
  action_ = NULL;
  if (NULL != ep_) {
    delete ep_;
    ep_ = NULL;
  }
  is_inited_ = false;
  if (NULL != alloc_cache_) {
    reinterpret_cast<ObUnixNetVConnection *>(alloc_cache_)->free();
    alloc_cache_ = NULL;
  }
};

// Stop listening. When the next poll takes place, an error will result.
// THIS ONLY WORKS WITH POLLING STYLE ACCEPTS!
void ObNetAccept::cancel()
{
  action_->cancel();
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = server_.close())) {
    PROXY_NET_LOG(WARN, "server close failed", K(tmp_ret));
  }
}

int ObNetAccept::deep_copy(const ObNetAccept &na)
{
  int ret = OB_SUCCESS;
  MEMCPY(this, &na, sizeof(na));
  if (OB_ISNULL(ep_ = new (std::nothrow) ObEventIO())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_NET_LOG(ERROR, "fail to new ObEventIO", K(ret));
  }
  return ret;
}

ObNetProcessor *ObNetAccept::get_net_processor() const
{
  return &g_net_processor;
}

} // end of namespace net
} // end of namespace obproxy
} // end of namespace oceanbase
