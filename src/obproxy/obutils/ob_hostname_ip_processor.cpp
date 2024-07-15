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

#define USING_LOG_PREFIX PROXY

#include "obproxy/obutils/ob_hostname_ip_processor.h"
#include "lib/lock/ob_drw_lock.h"
#include "iocore/eventsystem/ob_blocking_task.h"

using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{



ObHostnameIpRefreshProcessor& get_binlog_service_hostname_ip_processor()
{
  static ObHostnameIpRefreshProcessor g_binlog_service_hostname_ip_processor;
  return g_binlog_service_hostname_ip_processor;
}

int ObHostnameIpRefreshCont::init(const ObString& target_hostname)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(hostname_.assign(target_hostname))) {
    LOG_WDIAG("fail to assign hostname", K(target_hostname), K(ret));
  }

  return ret;
}

int ObHostnameIpRefreshCont::init_task()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(refresh_processor_.refresh_hostname_ip_map(hostname_.get_obstring()))) {
    LOG_WDIAG("fail to do refresh hostname ip map", K(ret));
  } else {
    LOG_DEBUG("succ to async refresh hostname", K_(hostname));
  }

  if (OB_LIKELY(OB_SUCCESS == async_task_ret_)) {
    async_task_ret_ = ret;
  }

  need_callback_ = true;

  return ret;
}

int ObHostnameIpRefreshCont::finish_task(void *data)
{
  int ret = OB_SUCCESS;

  UNUSED(data);
  LOG_DEBUG("finish_task", KP(this), KP_(cb_cont), K(ret));

  return ret;
}

void ObHostnameIpRefreshCont::destroy()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("hostname ip refresh cont will be destroyed", KP(this));

  if (OB_FAIL(cancel_timeout_action())) {
    LOG_WDIAG("fail to cancel timeout action", K(ret));
  }
  if (OB_FAIL(cancel_pending_action())) {
    LOG_WDIAG("fail to cancel pending action", K(ret));
  }

  // 父类里最后是用的 delete, 但是本 Cont 是用的 op_alloc 分配出来的,
  // 所以只能把父类里的 destroy 方法拷贝到这里
  cb_cont_ = NULL;
  submit_thread_ = NULL;
  mutex_.release();
  action_.mutex_.release();

  op_free(this);
}

ObHostnameIpRefreshProcessor::ObHostnameIpRefreshProcessor() : hostname_ip_map_(), hostname_ip_lock_()
{

}

ObHostnameIpRefreshProcessor::~ObHostnameIpRefreshProcessor()
{

}

int ObHostnameIpRefreshProcessor::sync_get_ip_by_hostname(const ObString& target_hostname, ObIArray<net::ObIpAddr>& ret_ip_list)
{
  int ret = OB_EAGAIN;

  DRWLock::RDLockGuard lock(hostname_ip_lock_);
  typename HostnameIPHashMap::iterator end = hostname_ip_map_.end();
  for (typename HostnameIPHashMap::iterator it = hostname_ip_map_.begin();
       (OB_SUCC(ret) || OB_EAGAIN == ret) && it != end; ++it) {
    ObString hostname(it->hostname_.size(), it->hostname_.ptr());
    ObIArray<net::ObIpAddr>& ip_list = it->ip_list_;

    if (0 == target_hostname.case_compare(hostname)) {
      ret = OB_SUCCESS;
      for (int64_t i = 0; OB_SUCC(ret) && i < ip_list.count(); ++i) {
        if (OB_FAIL(ret_ip_list.push_back(ip_list.at(i)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("fail to push back ip", K(i), K(ret));
        }
      }
      it->renew_last_access_time();

      break;
    }
  }

  if (OB_FAIL(ret)) {
    LOG_DEBUG("fail to find hostname", "hostname_ip_map", *this, K(target_hostname), K(ret));
  }

  return ret;
}

int ObHostnameIpRefreshProcessor::async_refresh_ip_by_hostname(const ObString& target_hostname,
                                                               event::ObContinuation * cb_cont,
                                                               event::ObAction *&action)
{
  int ret = OB_SUCCESS;

  ObHostnameIpRefreshCont* refresh_cont = NULL;
  ObProxyMutex *mutex = NULL;

  if (OB_ISNULL(this_ethread())
      || !this_ethread()->is_event_thread_type(ET_CALL)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("should only call this func on ET_CALL thread", "cur thread type",
              this_ethread()->event_types_, "ET_CALL val", ET_CALL);
  } else if (OB_ISNULL(mutex = new_proxy_mutex())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_EDIAG("fail to alloc memory for mutex", K(ret));
  } else if (OB_ISNULL(refresh_cont = op_alloc_args(ObHostnameIpRefreshCont, mutex, cb_cont, this_ethread(), *this))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc ObHostnameIpRefreshCont", K(ret));
  } else if (OB_FAIL(refresh_cont->init(target_hostname))) {
    LOG_WDIAG("fail to init ObHostnameIpRefreshCont", K(ret));
  } else if (OB_ISNULL(g_event_processor.schedule_imm(refresh_cont, ET_BLOCKING))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to schedule hostname refresh cont", K(ret));
  } else {
    action = &(refresh_cont->get_action());
  }
  
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(refresh_cont)) {
      refresh_cont->destroy();
      refresh_cont = NULL;
    }
    if (OB_NOT_NULL(mutex)) {
      mutex->free();
      mutex = NULL;
    }
  }
    
  return ret;
}

int ObHostnameIpRefreshProcessor::refresh_hostname_ip_map(const ObString& target_hostname)
{
  int ret = OB_SUCCESS;

  bool find = false;
  DRWLock::WRLockGuard lock(hostname_ip_lock_);
  typename HostnameIPHashMap::iterator end = hostname_ip_map_.end();
  for (typename HostnameIPHashMap::iterator it = hostname_ip_map_.begin();
       OB_SUCC(ret) && !find && (it != end); ++it) {
    ObString hostname(it->hostname_.size(), it->hostname_.ptr());
    ObIArray<net::ObIpAddr>& ip_list = it->ip_list_;

    if (0 == target_hostname.case_compare(hostname)) {
      find = true;
      if (OB_FAIL(do_refresh_hostname_ip(hostname, ip_list))) {
        LOG_WDIAG("fail to refresh host ip", K(hostname), K(ret));
      } else {
        it->renew_last_access_time();
      }
    } else {
      // nothing
    }
  }

  if (OB_UNLIKELY(!find)) {
    ObHostnameIpItem* tmp_item;
    if (OB_ISNULL(tmp_item = op_alloc(ObHostnameIpItem))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc ObHostnameIpItem", K(ret));
    } else if (OB_FAIL(tmp_item->hostname_.assign(target_hostname))) {
      LOG_WDIAG("fail to assign hostname", K(ret));
    } else if (OB_FAIL(do_refresh_hostname_ip(target_hostname, tmp_item->ip_list_))) {
      LOG_WDIAG("fail to refresh host ip", K(target_hostname), K(ret));
    } else if (OB_FAIL(hostname_ip_map_.unique_set(tmp_item))) {
      LOG_WDIAG("fail to insert into hostname_ip_map", K(ret));
    } else {
      tmp_item->renew_last_access_time();
      LOG_DEBUG("succ to get new hostname", "item", *tmp_item, K(target_hostname), K(ret));
    }

    if (OB_FAIL(ret)
        && OB_NOT_NULL(tmp_item)) {
      op_free(tmp_item);
    }
  }

  return ret;
}

int ObHostnameIpRefreshProcessor::refresh_hostname_ip_map_all()
{
  int ret = OB_SUCCESS;

  bool find = false;
  int64_t expired_time_us = HRTIME_USECONDS(get_global_proxy_config().cluster_expire_time);
  DRWLock::WRLockGuard lock(hostname_ip_lock_);
  typename HostnameIPHashMap::iterator end = hostname_ip_map_.end();
  for (typename HostnameIPHashMap::iterator it = hostname_ip_map_.begin();
       OB_SUCC(ret) && !find && (it != end); ++it) {
    ObString hostname(it->hostname_.size(), it->hostname_.ptr());
    ObIArray<net::ObIpAddr>& ip_list = it->ip_list_;

    if (OB_UNLIKELY(it->is_expired(expired_time_us))) {
      hostname_ip_map_.remove(&(*it));
    } else if (OB_FAIL(do_refresh_hostname_ip(hostname, ip_list))) {
      LOG_WDIAG("fail to refresh host ip", K(hostname), K(ret));
    } else {
      // nothing
    }
  }

  return ret;
}

// blocking func, call in ET_BLOCK not in other threads
int ObHostnameIpRefreshProcessor::do_refresh_hostname_ip(const ObString& hostname, ObIArray<net::ObIpAddr>& ip_list)
{
  int ret = OB_SUCCESS;

  struct addrinfo *addrs = NULL;
  struct addrinfo hints;
  net::ObIpAddr ip_addr;
  bzero(&hints, sizeof(hints));
  hints.ai_family = AF_INET;        // 允许IPv4
  hints.ai_socktype = SOCK_STREAM;  // 使用TCP流式套接字

  if (OB_ISNULL(this_ethread())
      || !this_ethread()->is_event_thread_type(ET_BLOCKING)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("error shedule blocking task on non-blocking thread", "cur thread type",
              this_ethread()->event_types_, "ET_BLOCKING val", ET_BLOCKING);
  } else if (OB_UNLIKELY(hostname.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected empty hostname", K(ret));
  } else if (OB_FAIL(::getaddrinfo(hostname.ptr(), NULL, &hints, &addrs))) {
    ObString real_get_hostname; // for debug
    real_get_hostname.assign_ptr(hostname.ptr(), strlen(hostname.ptr()));
    ret = ob_get_sys_errno(-1 * ret);
    addrs = NULL;
    LOG_WDIAG("fail to get hostname", K(hostname), K(real_get_hostname), K(ret));
  } else {
    struct addrinfo *curr = NULL;
    struct in_addr *addr = NULL;
    for (curr = addrs; OB_SUCC(ret) && NULL != curr; curr = curr->ai_next) {
      if (NULL != curr->ai_addr) {
        addr = &((reinterpret_cast<struct sockaddr_in *>(curr->ai_addr))->sin_addr);
        if ((NULL != addr) && (addr->s_addr != htonl(INADDR_LOOPBACK) && addr->s_addr != htonl(INADDR_ANY))) {
          ip_addr = addr->s_addr;
          if (OB_FAIL(ip_list.push_back(ip_addr))) {
            LOG_WDIAG("fail to push new ip_addr", K(hostname));
          } else {
            LOG_DEBUG("succ to get ip_addr for hostname", K(ip_addr), K(hostname), K(ret));
          }
        }
      }
    }
  }

  if (NULL != addrs) {
    freeaddrinfo(addrs);
  }

  return ret;
}

int64_t ObHostnameIpRefreshProcessor::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  J_OBJ_START();
  HostnameIPHashMap& map = const_cast<HostnameIPHashMap&> (hostname_ip_map_);
  typename HostnameIPHashMap::iterator end = map.end();
  for (typename HostnameIPHashMap::iterator it = map.begin();
       it != end; ++it) {
    const ObHostnameIpItem& hostname_ip = *it;
    J_COMMA();
    J_KV(K(hostname_ip));
  }
  J_OBJ_END();

  return pos;
}

int64_t ObHostnameIpRefreshProcessor::ObHostnameIpItem::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K(hostname_));
  for (int64_t i = 0; i < ip_list_.count(); ++i) {
    J_COMMA();
    J_KV(K(i));
    J_COMMA();
    J_KV("ip", ip_list_.at(i));
  }
  J_OBJ_END();
  return pos;
}

}//end of namespace obutils
}//end of namespace obproxy
}//end of namespace oceanbase