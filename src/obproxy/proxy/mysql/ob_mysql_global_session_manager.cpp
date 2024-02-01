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
#include "proxy/mysql/ob_mysql_global_session_manager.h"
#include "lib/ob_define.h"
#include "obutils/ob_config_server_processor.h"
#include "obutils/ob_proxy_config.h"
#include "proxy/mysql/ob_mysql_debug_names.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::obutils;
namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
ObMysqlServerSessionList::ObMysqlServerSessionList() : ObContinuation(NULL)
{
  reset();
  SET_HANDLER(&ObMysqlServerSessionList::main_handler);
}

void ObMysqlServerSessionList::reset()
{
  total_count_ = 0;
  free_count_ = 0;
  using_count_ = 0;
  max_used_ = 0;
  create_count_ = 0;
  destroy_count_ = 0;
  last_log_time_ = 0;
}
ObMysqlServerSessionList::~ObMysqlServerSessionList() {
  local_ip_pool_.reset();
}
int ObMysqlServerSessionList::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(server_session_list_.init("ObMysqlServerSessionList list",
                                        reinterpret_cast<int64_t>(&(reinterpret_cast<ObMysqlServerSession*>(0))->ip_list_link_)))) {
    LOG_WDIAG("fail to init server_session_list_", K(ret));
  }
  ObProxyMutex *mutex = NULL;
  if (OB_ISNULL(mutex = new_proxy_mutex(CLIENT_VC_LOCK))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to allocate mutex", K(ret));
  } else {
    mutex_ = mutex;
  }
  return ret;
}
int ObMysqlServerSessionList::main_handler(int event, void *data)
{
  ObNetVConnection *net_vc = NULL;
  ObMysqlServerSession *ss = NULL;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("data is null", K(ret));
  } else {
    switch (event) {
    case VC_EVENT_READ_READY:
    // The server sent us data. This is unexpected so
    // close the connection
    // fallthrough
    case VC_EVENT_EOS:
    case VC_EVENT_ERROR:
    case VC_EVENT_INACTIVITY_TIMEOUT:
    case VC_EVENT_ACTIVE_TIMEOUT:
    case VC_EVENT_DETECT_SERVER_DEAD:
      net_vc = static_cast<ObNetVConnection*>((static_cast<ObVIO*>(data))->vc_server_);
      break;

    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("invalid event", K(event), K(ret));
      break;
    }
  }
  if (OB_LIKELY(NULL != net_vc)) {
    ObMysqlServerSessionHashKey hash_key;
    ObIpEndpoint local_ip(net_vc->get_local_addr());
    ObIpEndpoint server_ip(net_vc->get_remote_addr());
    hash_key.local_ip_ = &local_ip;
    hash_key.server_ip_ = &server_ip;
    LOG_DEBUG("Enter main_handler", K(event), "event:", ObMysqlDebugNames::get_event_name(event), K(local_ip), K(server_ip));
    bool found = false;
    {
      //code block for lock
      DRWLock::WRLockGuard guard(rwlock_);
      if (OB_LIKELY(NULL != (ss = local_ip_pool_.get(hash_key)))
          && OB_LIKELY(ss->get_netvc() == net_vc)) {
        // We've found our server session. Remove it from
        // our lists and close it down
        found = true;
        LOG_DEBUG("[session_pool] session received io notice ", K(event), "ss_id", ss->ss_id_, K(ss));
        if (OB_LIKELY(MSS_KA_SHARED == ss->state_)) {
          // Out of the pool! Now!
          if (OB_FAIL(remove_from_list(ss))) {
            //impossible happen here
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("no server_session found in shared pool", K(ret));
          }
          // Drop connection on this end.
          //mark has lock to prevent double lock in remove
          ss->has_global_session_lock_ = true;
          ss->do_io_close();
        } else {
          LOG_DEBUG("not expected state", K(ss->state_));
        }
      }
    }

    if (OB_UNLIKELY(!found)) {
      // We failed to find our session.  This can only be the result
      // of a programming flaw
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("Connection leak from mysql keep-alive system", K(ret));
    }
  }
  return VC_EVENT_NONE;
}
void ObMysqlServerSessionList::purge_session_list()
{
  DRWLock::WRLockGuard guard(rwlock_);
  while (!server_session_list_.empty()) {
    ObMysqlServerSession* session = (ObMysqlServerSession*)server_session_list_.pop();
    if (OB_ISNULL(session)) {
      LOG_WDIAG("unexpected session is NULL");
    } else {
      // will remove from local_ip_pool when close
      session->has_global_session_lock_ = true;
      session->do_io_close();
    }
  }
  local_ip_pool_.reset();
}
void ObMysqlServerSessionList::do_kill_session()
{
  LOG_DEBUG("do_kill_session", K(common_addr_));
  DRWLock::WRLockGuard guard(rwlock_);
  LocalIPHashTable::iterator spot = local_ip_pool_.begin();
  LocalIPHashTable::iterator last = local_ip_pool_.end();
  net::ObIpEndpoint local_ip;
  ObMysqlServerSession* session = NULL;
  for (; spot != last; ++spot) {
    session = &(*spot);
    if (OB_ISNULL(session)) {
      LOG_WDIAG("unexpected session is NULL", K(common_addr_));
    } else {
      local_ip.assign(session->get_netvc()->get_local_addr());
      LOG_DEBUG("kill sesion", K(local_ip), K(common_addr_));
      session->get_netvc()->set_is_force_timeout(true);
    }
  }
}
/*
 * add when server_session create
 * add to local_ip_pool and incr total_count
 */
int ObMysqlServerSessionList::add_server_session(ObMysqlServerSession* server_session)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(server_session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("server_session is null, invalid argument");
  } else {
    net::ObIpEndpoint local_ip;
    local_ip.assign(server_session->get_netvc()->get_local_addr());
    DRWLock::WRLockGuard guard(rwlock_);
    ret = local_ip_pool_.set_refactored(server_session);
    if (ret != OB_SUCCESS && ret != OB_HASH_EXIST) {
      LOG_WDIAG("add to local_ip_pool_ failed", K(ret), K(local_ip));
    } else {
      ret = OB_SUCCESS;
      ATOMIC_INC(&total_count_);
      ATOMIC_INC(&create_count_);
      LOG_DEBUG("succ add to local_ip_pool_", K(total_count_), K(server_session->ss_id_),
                K(server_session->auth_user_), K(server_session->server_ip_), K(local_ip));
    }
  }
  return ret;
}
/*
 * remove when server_ession do_io_close()
 * remove from local_ip_pool
 */
int ObMysqlServerSessionList::remove_server_session(const ObMysqlServerSession* server_session)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(server_session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("server_session is null, invalid argument");
  } else if (server_session->has_global_session_lock_) {
    ret =  remove_server_session_internal(server_session);
  } else {
    DRWLock::WRLockGuard guard(rwlock_);
    ret =  remove_server_session_internal(server_session);
  }
  return ret;
}
int ObMysqlServerSessionList::remove_server_session_internal(const ObMysqlServerSession* server_session)
{
  int ret = OB_SUCCESS;
  ObMysqlServerSession* ss_to_remove = NULL;
  if (OB_ISNULL(ss_to_remove = local_ip_pool_.remove(ObLocalIPHashing::key(server_session)))) {
    LOG_INFO("remove failed", K(server_session->ss_id_), K(server_session->auth_user_),
             K(server_session->server_ip_));
  } else {
    ATOMIC_DEC(&total_count_);
    ATOMIC_INC(&destroy_count_);
    LOG_DEBUG("succ removed",  K(total_count_), K(destroy_count_), K(server_session->ss_id_), K(server_session->auth_user_),
              K(server_session->server_ip_));
  }
  return ret;
}

ObMysqlServerSession* ObMysqlServerSessionList::acquire_from_list()
{
  DRWLock::RDLockGuard guard(rwlock_);
  ObMysqlServerSession* ss = (ObMysqlServerSession*)server_session_list_.pop();
  if (ss != NULL) {
    ATOMIC_DEC(&free_count_);
    using_count_ = total_count_ - free_count_;
    if (using_count_ > max_used_) {
      max_used_ = using_count_;
    }
    ss->cancel_inactivity_timeout();
    ss->state_ = MSS_ACTIVE;
    LOG_DEBUG("acquire_from_list", K(ss->server_ip_), K(ss->auth_user_), K(free_count_), K(using_count_), K(max_used_));
  } else {
    LOG_DEBUG("acquire_from_list is null", K(free_count_));
  }
  return ss;
}

// just release, if fail outer will close the session, do not close here
int ObMysqlServerSessionList::release_to_list(ObMysqlServerSession& server_session)
{
  int ret = OB_SUCCESS;
  ObUnixNetVConnection *server_vc = static_cast<ObUnixNetVConnection*>(server_session.get_netvc());
  // Now we need to issue a read on the connection to detect
  // if it closes on us. We will get called back in the
  // continuation for this bucket, ensuring we have the lock
  if (OB_ISNULL(server_vc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("server vc is null", K(ret));
  } else if (!server_vc->read_.enabled_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("not expected state");
  } else if (OB_ISNULL(server_session.do_io_read(this, INT64_MAX, server_session.read_buffer_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("do_io_read error", K(ret));
  } else if (OB_ISNULL(server_session.do_io_write(this, 0, NULL))) {
    // Transfer control of the write side as well
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("do_io_write error", K(ret));
  } else {
    // we probably don't need the active timeout set, but will leave it for now
    if (total_count_ > ObMysqlSessionUtils::get_session_max_conn(server_session.schema_key_)) {
      ret = OB_SESSION_POOL_FULL_ERROR;
      LOG_DEBUG("session pool is full", K(server_session.ss_id_), K(server_session.auth_user_),
                K(server_session.server_ip_), K(total_count_), K(free_count_));
    } else {
      server_vc->set_inactivity_timeout(ObMysqlSessionUtils::get_session_idle_timeout_ms(server_session.schema_key_));
      server_vc->set_active_timeout(server_vc->get_active_timeout());
      server_session.clear_client_session();
      server_session_list_.push(&server_session);
      int64_t old_count = free_count_;
      int64_t new_count = ATOMIC_AAF(&free_count_, 1);
      LOG_DEBUG("release_to_list succ", K(server_session.ss_id_), K(server_session.auth_user_),
                K(server_session.server_ip_), K(old_count), K(new_count), K(total_count_));
    }
  }
  return ret;
}

int ObMysqlServerSessionList::remove_from_list(ObMysqlServerSession* server_session)
{
  int ret = OB_SUCCESS;
  // is locked in main_handler
  ObMysqlServerSession* ss_to_remove = NULL;
  if (OB_ISNULL(ss_to_remove = (ObMysqlServerSession*)server_session_list_.remove(server_session))) {
    LOG_WDIAG("should not null here", K(server_session->ss_id_), K(server_session->auth_user_),
             K(server_session->server_ip_));
  } else {
    int64_t old_count = free_count_;
    int64_t new_count = ATOMIC_SAF(&free_count_, 1);
    LOG_DEBUG("remove from list succ", K(server_session->ss_id_), K(server_session->auth_user_),
              K(server_session->server_ip_), K(old_count), K(new_count));
  }
  return ret;
}
int ObMysqlServerSessionList::do_pool_log(const ObProxySchemaKey& schema_key, bool force_log)
{
  int ret = OB_SUCCESS;
  int64_t max_conn = ObMysqlSessionUtils::get_session_max_conn(schema_key);
  //log when reach ratio
  int64_t ratio = get_global_proxy_config().session_pool_stat_log_ratio;
  int64_t used_conn = total_count_ - free_count_;
  const ObString& dbkey = schema_key.dbkey_.config_string_;
  int64_t min_conn = ObMysqlSessionUtils::get_session_min_conn(schema_key);
  int64_t total_count = total_count_;
  int64_t free_count = free_count_;
  int64_t using_count = total_count - free_count;
  int64_t max_used = max_used_;
  int64_t create_count = create_count_;
  int64_t destroy_count = destroy_count_;
  if (force_log) {
    int64_t now_time = event::get_hrtime();
    last_log_time_ = now_time;
    OBPROXY_POOL_STAT_LOG(INFO, "session_pool_stat:", K(dbkey), K(max_conn), K(min_conn), K(total_count), K(free_count),
        K(using_count), K(max_used), K(create_count),K(destroy_count), K(common_addr_), K(force_log));
  } else if (used_conn >= max_conn * ratio / 10000) {
    int64_t now_time = event::get_hrtime();
    int64_t interval_time = HRTIME_USECONDS(get_global_proxy_config().session_pool_stat_log_interval);
    if (now_time - last_log_time_ >= interval_time) {
      last_log_time_ = now_time;
      OBPROXY_POOL_STAT_LOG(INFO, "session_pool_stat:", K(dbkey), K(max_conn), K(min_conn), K(total_count), K(free_count),
        K(using_count), K(max_used), K(create_count),K(destroy_count), K(common_addr_), K(force_log));
    } else {
      LOG_DEBUG("reach ratio and no need log", K(interval_time), K(last_log_time_),
        K(now_time), K(used_conn), K(max_conn), K(min_conn), K(schema_key));
    }
  }
  return ret;
}

ObMysqlServerSessionListPool::ObMysqlServerSessionListPool()
  : client_session_count_(0), schema_server_addr_info_(NULL)
{
}

ObMysqlServerSessionListPool::~ObMysqlServerSessionListPool()
{
  purge_session_list_pool();
  if (NULL != schema_server_addr_info_) {
    schema_server_addr_info_->dec_ref();
    schema_server_addr_info_ = NULL;
  }
}
int ObMysqlServerSessionListPool::init(const ObProxySchemaKey& schema_key)
{
  int ret = OB_SUCCESS;
  schema_key_ = schema_key;
  if (NULL == schema_server_addr_info_) {
    schema_server_addr_info_ = op_alloc_args(ObMysqlSchemaServerAddrInfo, schema_key);
    schema_server_addr_info_->inc_ref();
  }
  return ret;
}

int64_t ObMysqlServerSessionListPool::get_current_session_conn_count(
  const ObCommonAddr& key)
{
  int64_t conn_count = 0;
  int ret = OB_SUCCESS;
  ObMysqlServerSessionList* ss_list = NULL;
  if (OB_FAIL(accquire_server_seession_list(key, ss_list))) {
  } else {
    conn_count = ss_list->total_count_;
    ss_list->dec_ref();
  }
  return conn_count;
}

int64_t ObMysqlServerSessionListPool::incr_client_session_count()
{
  int64_t old_count = client_session_count_;
  int64_t new_count = ATOMIC_AAF(&client_session_count_, 1);
  LOG_DEBUG("after incr now_count", K(old_count), K(new_count), K(schema_key_));
  return new_count;
}
int64_t ObMysqlServerSessionListPool::decr_client_session_count()
{
  int64_t old_count = client_session_count_;
  int64_t new_count = ATOMIC_SAF(&client_session_count_, 1);
  LOG_DEBUG("after decr now_count", K(old_count), K(new_count), K(schema_key_));
  return new_count;
}
int ObMysqlServerSessionListPool::do_close_extra_session_conn(const ObCommonAddr &key,
    int64_t need_close_num)
{
  int ret = OB_SUCCESS;
  int64_t close_num = 0;
  ObMysqlServerSession* ss = NULL;
  for (; OB_SUCC(ret) && close_num < need_close_num; ++close_num) {
    if (OB_FAIL(acquire_server_session(key, ss, false))) {
    } else {
      if (ss != NULL) {
        //need set to shared as aquire set it active
        ss->state_ = MSS_KA_SHARED;
        ss->do_io_close();
        ss = NULL;
      }
    }
  }
  LOG_INFO("do_close_extra_session_conn", K(key),  K(need_close_num), K(close_num));
  return ret;
}
int ObMysqlServerSessionListPool::accquire_server_seession_list(const ObCommonAddr& key,
  ObMysqlServerSessionList* &ss_list)
{
  int ret = OB_SUCCESS;
  DRWLock::RDLockGuard guard(rwlock_);
  if (OB_FAIL(server_session_list_pool_.get_refactored(key, ss_list))) {
  } else if (OB_ISNULL(ss_list)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected null", K(key), K(ret));
  } else {
    ss_list->inc_ref();
  }
  return ret;
}

 int ObMysqlServerSessionListPool::acquire_server_session(
  const ObCommonAddr &key,
  ObMysqlServerSession* &server_session,
  bool new_client)
{
  int ret = OB_SUCCESS;
  ObMysqlServerSessionList* ss_list = NULL;
  if (new_client) {
    // here inc is to avoid concurrent acquire
    int64_t new_count = incr_client_session_count();
    int64_t max_conn = ObMysqlSessionUtils::get_session_max_conn(schema_key_);
    if (new_count > max_conn) {
      decr_client_session_count();
      ret = OB_SESSION_POOL_FULL_ERROR;
      LOG_INFO("reach_client_session_max_count", K(schema_key_.dbkey_), K(max_conn), K(client_session_count_));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(accquire_server_seession_list(key, ss_list))) {
    } else if (NULL != (server_session = (ObMysqlServerSession*)ss_list->acquire_from_list())) {
      LOG_DEBUG("acquire_session succ", K(schema_key_.dbkey_),
                K(key), K(client_session_count_), KP(server_session));
    }
    if (new_client && (OB_FAIL(ret) || NULL == server_session)) {
      ret = OB_SUCCESS;
      decr_client_session_count(); //before has inc, when get fail or null should decr
      LOG_DEBUG("acquire null session", K(schema_key_.dbkey_), K(key));
    }
    if (ss_list != NULL) {
      ss_list->do_pool_log(schema_key_);
      ss_list->dec_ref();
    }
  }
  return ret;
}

int ObMysqlServerSessionListPool::acquire_server_session(const ObCommonAddr &addr,
    const ObString &auth_user,
    ObMysqlServerSession* &server_session,
    bool new_client)
{
  UNUSED(auth_user);
  return acquire_server_session(addr, server_session, new_client);
}

int ObMysqlServerSessionListPool::release_session(ObMysqlServerSession &ss)
{
  int ret = OB_SUCCESS;
  const ObString& dbkey = ss.schema_key_.dbkey_.config_string_;
  ss.last_active_time_ = ObTimeUtility::current_time();
  LOG_DEBUG("release_session", K(ss.ss_id_), K(ss.auth_user_),
            K(ss.server_ip_), K(dbkey), K(ss.last_active_time_));
  ObCommonAddr& key = ss.common_addr_;
  ObMysqlServerSessionList* ss_list = NULL;
  ss.state_ = MSS_KA_SHARED;
  decr_client_session_count();
  if (OB_FAIL(accquire_server_seession_list(key, ss_list))) {
  } else {
    int64_t max_count = ObMysqlSessionUtils::get_session_max_conn(schema_key_);
    if (ss_list->total_count_ <= max_count) {
      ret = ss_list->release_to_list(ss);
    } else {
      ret = OB_SESSION_POOL_FULL_ERROR;
      LOG_INFO("session pool is full", K(dbkey), K(ss_list->total_count_), K(max_count));
    }
    LOG_DEBUG("after release_session", K(ss_list->total_count_), K(ss_list->free_count_), K(dbkey), K(ret));
    ss_list->dec_ref();
  }
  return ret;
}

int ObMysqlServerSessionListPool::purge_session_list_pool()
{
  int ret = OB_SUCCESS;
  DRWLock::WRLockGuard guard(rwlock_);
  IPHashTable::iterator last = server_session_list_pool_.end();
  for (IPHashTable::iterator spot = server_session_list_pool_.begin(); spot != last; ++spot) {
    spot->purge_session_list();
    ObMysqlServerSessionList* server_session_list = &(*spot);
    op_free(server_session_list);
  }
  server_session_list_pool_.reset();
  return ret;
}

int ObMysqlServerSessionListPool::do_kill_session()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("do_kill_session", K(schema_key_));
  DRWLock::WRLockGuard guard(rwlock_);
  IPHashTable::iterator last = server_session_list_pool_.end();
  for (IPHashTable::iterator spot = server_session_list_pool_.begin(); spot != last; ++spot) {
    // just do kill, ignore ret
    spot->do_kill_session();
  }
  return ret;
}

int  ObMysqlServerSessionListPool::do_kill_session_by_ssid(int64_t ss_id)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("do_kill_session_by_ssid", K(schema_key_), K(ss_id));
  bool found = false;
  DRWLock::WRLockGuard guard(rwlock_);
  IPHashTable::iterator last = server_session_list_pool_.end();
  for (IPHashTable::iterator spot = server_session_list_pool_.begin(); spot != last; ++spot) {
    ObMysqlServerSessionList* server_session_list = &(*spot);
    ObMysqlServerSessionList::LocalIPHashTable::iterator ss_spot = server_session_list->local_ip_pool_.begin();
    ObMysqlServerSessionList::LocalIPHashTable::iterator ss_last = server_session_list->local_ip_pool_.end();
    for (; !found && ss_spot != ss_last; ++ss_spot) {
      ObMysqlServerSession* session = &(*ss_spot);
      if (OB_ISNULL(session)) {
        LOG_WDIAG("unexpected session is NULL", K(server_session_list->server_ip_));
      } else if (session->ss_id_ == ss_id){
        found = true;
        net::ObIpEndpoint local_ip;
        local_ip.assign(session->get_netvc()->get_local_addr());
        LOG_DEBUG("kill sesion", K(local_ip), K(server_session_list->common_addr_));
        session->get_netvc()->set_is_force_timeout(true);
      }
    }
  }
  if (found == false) {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}


//add when server_session create
int ObMysqlServerSessionListPool::add_server_session(ObMysqlServerSession& ss)
{
  int ret = OB_SUCCESS;
  const ObString& dbkey = ss.schema_key_.dbkey_.config_string_;
  ObCommonAddr& common_addr = ss.common_addr_;
  LOG_DEBUG("add_server_session", K(ss.ss_id_), K(ss.auth_user_),
            K(ss.server_ip_), K(dbkey));
  ObMysqlServerSessionList* ss_list = NULL;
  if (OB_FAIL(accquire_server_seession_list(common_addr, ss_list))) {
    LOG_DEBUG("not exist in map", K(ret), K(dbkey));
    DRWLock::WRLockGuard guard(rwlock_);
    if (OB_FAIL(server_session_list_pool_.get_refactored(common_addr, ss_list))) {
      if (OB_ISNULL(ss_list = op_alloc(ObMysqlServerSessionList))) {
        LOG_EDIAG("fail to allocate ", K(dbkey));
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(ss_list->init())) {
        LOG_EDIAG("fail to init ss_list", K(dbkey));
        ret = OB_ERR_UNEXPECTED;
        op_free(ss_list);
        ss_list = NULL;
      } else {
        ss_list->auth_user_.set_value(ss.auth_user_);
        ss_list->server_ip_ = ss.server_ip_;
        ss_list->common_addr_ = ss.common_addr_;
        ss_list->inc_ref();
        if (OB_FAIL(server_session_list_pool_.unique_set(ss_list))) {
          LOG_WDIAG("add to map failed", K(common_addr), K(ret));
          ss_list->dec_ref();
          ss_list = NULL;
          ret = OB_ERR_UNEXPECTED;
        } else {
          ss_list->inc_ref();
          LOG_DEBUG("add to session list succ", K(schema_key_), K(common_addr), K(client_session_count_));
        }
      }
    }
  } else if (OB_ISNULL(ss_list)) {
    LOG_WDIAG("ss_list should not null here", K(dbkey));
    ret = OB_ERR_UNEXPECTED;
  }
  if (OB_SUCC(ret)) {
    add_server_addr_if_not_exist(ss.common_addr_);
    if (OB_SUCC(ss_list->add_server_session(&ss))) {
      incr_client_session_count();
    }
  }
  if (OB_NOT_NULL(ss_list)) {
    ss_list->dec_ref();
    ss_list = NULL;
  }
  LOG_DEBUG("add_server_session", K(dbkey), K(ret));
  return ret;
}
// remove when server_ession do_io_close()
int ObMysqlServerSessionListPool::remove_server_session(const ObMysqlServerSession& ss)
{
  int ret = OB_SUCCESS;
  const ObString& dbkey = ss.schema_key_.dbkey_.config_string_;
  const ObCommonAddr& key = ss.common_addr_;
  ObMysqlServerSessionList* ss_list = NULL;
  if (OB_FAIL(accquire_server_seession_list(key, ss_list))) {
    LOG_WDIAG("not in map", K(dbkey));
  } else if (OB_ISNULL(ss_list)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("session_list is null", K(dbkey));
  } else {
    if (ss.state_ == MSS_ACTIVE || ss.state_ == MSS_KA_CLIENT_SLAVE) {
      decr_client_session_count();
    }
    ret = ss_list->remove_server_session(&ss);
    ss_list->dec_ref();
    LOG_DEBUG("remove_server_session from ss_list", K(dbkey));
  }
  return ret;
}

int ObMysqlServerSessionListPool::add_server_addr_if_not_exist(const common::ObString& server_ip,
    int32_t server_port, bool is_physical)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_server_addr_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("schema_server_addr_info_ is null", K(schema_key_.dbkey_), K(server_ip), K(server_port));
  } else {
    ret = schema_server_addr_info_->add_server_addr_if_not_exist(server_ip, server_port, is_physical);
  }
  return ret;
}
int ObMysqlServerSessionListPool::add_server_addr_if_not_exist(const ObCommonAddr& common_addr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_server_addr_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("schema_server_addr_info_ is null", K(schema_key_.dbkey_), K(common_addr));
  } else {
    ret = schema_server_addr_info_->add_server_addr_if_not_exist(common_addr);
  }
  return ret;
}

int ObMysqlServerSessionListPool::remove_server_addr_if_exist(const common::ObString& server_ip,
    int32_t server_port, bool is_physical)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_server_addr_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("schema_server_addr_info_ is null", K(schema_key_.dbkey_), K(server_ip), K(server_port));
  } else {
    ret = schema_server_addr_info_->remove_server_addr_if_exist(server_ip, server_port, is_physical);
  }
  return ret;
}

int ObMysqlServerSessionListPool::remove_server_addr_if_exist(const ObCommonAddr& common_addr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_server_addr_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("schema_server_addr_info_ is null", K(schema_key_.dbkey_), K(common_addr));
  } else {
    ret = schema_server_addr_info_->remove_server_addr_if_exist(common_addr);
  }
  return ret;
}

int ObMysqlServerSessionListPool::incr_fail_count(const ObCommonAddr& addr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_server_addr_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("schema_server_addr_info_ is null", K(schema_key_.dbkey_), K(addr));
  } else {
    ret = schema_server_addr_info_->incr_fail_count(addr);
  }
  return ret;
}
void ObMysqlServerSessionListPool::reset_fail_count(const ObCommonAddr& addr)
{
  if (OB_ISNULL(schema_server_addr_info_)) {
    LOG_WDIAG("schema_server_addr_info_ is null", K(schema_key_.dbkey_), K(addr));
  } else {
    schema_server_addr_info_->reset_fail_count(addr);
  }
}
int32_t ObMysqlServerSessionListPool::get_fail_count(const ObCommonAddr& addr)
{
  int32_t fail_count = 0;
  if (OB_ISNULL(schema_server_addr_info_)) {
    LOG_WDIAG("schema_server_addr_info_ is null", K(schema_key_.dbkey_), K(addr));
  } else {
    fail_count = schema_server_addr_info_->get_fail_count(addr);
  }
  return fail_count;
}

ObMysqlGlobalSessionManager::~ObMysqlGlobalSessionManager()
{
  DRWLock::WRLockGuard guard(rwlock_);
  SessionPoolListHashTable::iterator last = global_session_pool_.end();
  for (SessionPoolListHashTable::iterator spot = global_session_pool_.begin(); spot != last; ++spot) {
    spot->purge_session_list_pool();
    ObMysqlServerSessionListPool* server_session_list_pool = &(*spot);
    server_session_list_pool->dec_ref();
  }
  global_session_pool_.reset();
}
int ObMysqlGlobalSessionManager::purge_session_manager_keepalives(const ObString& dbkey)
{
  int ret = OB_SUCCESS;
  ObMysqlServerSessionListPool* server_session_list_pool = NULL;
  // code block for lock
  DRWLock::WRLockGuard guard(rwlock_);
  if (OB_ISNULL(server_session_list_pool = global_session_pool_.remove(dbkey))) {
    LOG_WDIAG("get_refactored failed", K(dbkey));
    ret = OB_ERR_UNEXPECTED;
  } else {
    server_session_list_pool->dec_ref();
  }
  return ret;
}

ObMysqlServerSessionListPool* ObMysqlGlobalSessionManager::get_server_session_list_pool(const ObString& dbkey)
{
  int ret = OB_SUCCESS;
  ObMysqlServerSessionListPool* server_session_list_pool = NULL;
  DRWLock::RDLockGuard guard(rwlock_);
  if (OB_FAIL(global_session_pool_.get_refactored(dbkey, server_session_list_pool))) {
    LOG_DEBUG("not in map", K(dbkey));
  } else if (OB_ISNULL(server_session_list_pool)) {
    LOG_WDIAG("can not null here", K(dbkey));
    ret = OB_ERR_UNEXPECTED;
  } else {
    server_session_list_pool->inc_ref();
  }
  return server_session_list_pool;
}

int ObMysqlGlobalSessionManager::do_close_extra_session_conn(const ObProxySchemaKey& schema_key,
    const ObCommonAddr& common_addr_,
    int64_t need_close_num)
{
  int ret = OB_SUCCESS;
  const common::ObString& dbkey = schema_key.dbkey_.config_string_;
  ObMysqlServerSessionListPool* server_session_list_pool = get_server_session_list_pool(dbkey);
  if (OB_ISNULL(server_session_list_pool)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("can not be this, in map can not null", K(dbkey));
  } else {
    ret = server_session_list_pool->do_close_extra_session_conn(common_addr_, need_close_num);
    server_session_list_pool->dec_ref();
  }
  return ret;
}

int ObMysqlGlobalSessionManager::add_schema_if_not_exist(const ObProxySchemaKey& schema_key,
    ObMysqlServerSessionListPool* &server_session_list_pool)
{
  int ret = OB_SUCCESS;
  const ObString& dbkey = schema_key.dbkey_.config_string_;
  DRWLock::WRLockGuard guard(rwlock_);
  if (OB_FAIL(global_session_pool_.get_refactored(dbkey, server_session_list_pool))) {
    LOG_DEBUG("not in map, will alloc now", K(dbkey));
    if (OB_ISNULL(server_session_list_pool = op_alloc(ObMysqlServerSessionListPool))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_EDIAG("allocate fail", K(dbkey));
    } else if (OB_FAIL(server_session_list_pool->init(schema_key))) {
      op_free(server_session_list_pool);
      server_session_list_pool = NULL;
      LOG_WDIAG("fail to init server_session_list_pool", K(dbkey));
    } else {
      server_session_list_pool->inc_ref();
      if (OB_FAIL(global_session_pool_.unique_set(server_session_list_pool))) {
        LOG_WDIAG("add to map failed", K(ret), K(dbkey));
        server_session_list_pool->dec_ref();
        server_session_list_pool = NULL;
      } else {
        LOG_DEBUG("add listpool to map succ", K(dbkey));
      }
    }
  }
  if (OB_SUCC(ret)) {
    server_session_list_pool->inc_ref(); // incr ref for return
    LOG_DEBUG("already in map", K(dbkey));
  }
  return ret;
}

int ObMysqlGlobalSessionManager::remove_schema_if_exist(const ObProxySchemaKey& schema_key)
{
  int ret = OB_SUCCESS;
  const ObString& dbkey = schema_key.dbkey_;
  DRWLock::WRLockGuard guard(rwlock_);
  ObMysqlServerSessionListPool* server_session_list_pool;
  if (OB_ISNULL(server_session_list_pool = global_session_pool_.remove(dbkey))) {
    LOG_INFO("not in map", K(dbkey));
    ret = OB_ERR_UNEXPECTED;
  } else {
    server_session_list_pool->dec_ref();
    server_session_list_pool = NULL;
    LOG_DEBUG("remove_schema_if_exist succ", K(dbkey));
  }
  return ret;
}

//add when server_session create
int ObMysqlGlobalSessionManager::add_server_session(ObMysqlServerSession& server_session)
{
  int ret = OB_SUCCESS;
  const ObString& dbkey = server_session.schema_key_.dbkey_.config_string_;
  if (dbkey.empty()) {
    LOG_WDIAG("dbkey should not empty");
    ret = OB_ERR_UNEXPECTED;
    return ret;
  }
  ObMysqlServerSessionListPool* server_session_list_pool = get_server_session_list_pool(dbkey);
  if (OB_ISNULL(server_session_list_pool)) {
    // add when not exists
    ret = add_schema_if_not_exist(server_session.schema_key_, server_session_list_pool);
    LOG_DEBUG("server_session_list_pool is null, add now ", K(dbkey), K(ret));
  }
  if (OB_SUCC(ret)) {
    // add ito pool
    LOG_DEBUG("add server_session to server_session_list_pool", K(dbkey));
    ret = server_session_list_pool->add_server_session(server_session);
    server_session_list_pool->dec_ref();
  }
  return ret;
}
// remove when server_session do_io_close()
int ObMysqlGlobalSessionManager::remove_server_session(const ObMysqlServerSession& server_session)
{
  int ret = OB_SUCCESS;
  const ObString& dbkey = server_session.schema_key_.dbkey_.config_string_;
  ObMysqlServerSessionListPool* server_session_list_pool = get_server_session_list_pool(dbkey);
  if (OB_ISNULL(server_session_list_pool)) {
    LOG_WDIAG("invalid, should not null here", K(dbkey));
  } else {
    LOG_DEBUG("remove_server_session ", K(dbkey));
    ret = server_session_list_pool->remove_server_session(server_session);
  }
  return ret;
}
int ObMysqlGlobalSessionManager::acquire_server_session(const ObProxySchemaKey& schema_key,
    const ObCommonAddr &addr,
    const ObString &auth_user,
    ObMysqlServerSession *&server_session,
    bool new_client)
{
  int ret = OB_SUCCESS;
  const common::ObString& dbkey = schema_key.dbkey_.config_string_;
  ObMysqlServerSessionListPool* server_session_list_pool = get_server_session_list_pool(dbkey);
  if (OB_ISNULL(server_session_list_pool)) {
    if (OB_FAIL(add_schema_if_not_exist(schema_key, server_session_list_pool))) {
      LOG_WDIAG("add schema failed when not exist", K(dbkey), K(auth_user));
    }
  }
  if (OB_ISNULL(server_session_list_pool)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("should not null here", K(dbkey), K(auth_user));
  } else {
    ret = server_session_list_pool->acquire_server_session(addr, auth_user, server_session, new_client);
    server_session_list_pool->dec_ref();
  }
  return ret;
}
// will handle fail and close ,always return succ
int ObMysqlGlobalSessionManager::release_session(ObMysqlServerSession &to_release)
{
  int ret = OB_SUCCESS;
  const ObString& dbkey = to_release.schema_key_.dbkey_.config_string_;
  ObMysqlServerSessionListPool* server_session_list_pool = NULL;
  LOG_DEBUG("release_session", K(dbkey));
  if (dbkey.empty()) {
    LOG_WDIAG("dbkey should not empty");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(server_session_list_pool = get_server_session_list_pool(dbkey))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("server_session_list_pool is null, should not here", K(dbkey));
  } else {
    ret = server_session_list_pool->release_session(to_release);
    server_session_list_pool->dec_ref();
  }
  if (OB_FAIL(ret)) {
    to_release.do_io_close();
    ret = OB_SUCCESS;
  }
  return ret;
}

int64_t ObMysqlGlobalSessionManager::get_current_session_conn_count(const common::ObString& dbkey,
    const ObCommonAddr& common_addr)
{
  int64_t conn_count = 0;
  ObMysqlServerSessionListPool* server_session_list_pool = get_server_session_list_pool(dbkey);
  if (OB_ISNULL(server_session_list_pool)) {
    LOG_WDIAG("server_session_list_pool is null, should not here", K(dbkey));
  } else {
    conn_count = server_session_list_pool->get_current_session_conn_count(common_addr);
    server_session_list_pool->dec_ref();
  }
  return conn_count;
}
int ObMysqlGlobalSessionManager::add_server_addr_if_not_exist(const ObProxySchemaKey& schema_key,
    const common::ObString& server_ip,
    int32_t server_port,
    bool is_physical)
{
  int ret = OB_SUCCESS;
  if (TYPE_SHARD_CONNECTOR != schema_key.get_connector_type()) {
    // no sharding no need add
  } else {
    const ObString& dbkey = schema_key.dbkey_.config_string_;
    ObMysqlServerSessionListPool* server_session_list_pool = get_server_session_list_pool(dbkey);
    if (OB_ISNULL(server_session_list_pool)) {
      if (OB_FAIL(add_schema_if_not_exist(schema_key, server_session_list_pool))) {
        LOG_WDIAG("add_schema_if_not_exist fail", K(schema_key.dbkey_), K(server_ip), K(server_port));
      }
    }
    if (OB_ISNULL(server_session_list_pool)) {
      LOG_WDIAG("here should not null", K(dbkey), K(server_ip), K(server_port));
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret = server_session_list_pool->add_server_addr_if_not_exist(server_ip, server_port, is_physical);
      server_session_list_pool->dec_ref();
      LOG_DEBUG("add_server_addr_if_not_exist ", K(ret), K(dbkey), K(server_port));
    }
  }
  return ret;
}


int ObMysqlGlobalSessionManager::incr_fail_count(const common::ObString& dbkey, const ObCommonAddr& addr)
{
  int ret = OB_SUCCESS;
  ObMysqlServerSessionListPool* server_session_list_pool = get_server_session_list_pool(dbkey);
  if (OB_ISNULL(server_session_list_pool)) {
    LOG_WDIAG("can not be this, in map is null", K(dbkey), K(addr));
    ret = OB_ERR_UNEXPECTED;
  } else {
    ret = server_session_list_pool->incr_fail_count(addr);
    server_session_list_pool->dec_ref();
    LOG_DEBUG("incr_fail_count ", K(dbkey), K(addr));
  }
  return ret;
}

void ObMysqlGlobalSessionManager::reset_fail_count(const common::ObString& dbkey, const ObCommonAddr& addr)
{
  ObMysqlServerSessionListPool* server_session_list_pool = get_server_session_list_pool(dbkey);
  if (OB_ISNULL(server_session_list_pool)) {
    LOG_WDIAG("can not be this, in map is null", K(dbkey), K(addr));
  } else {
    server_session_list_pool->reset_fail_count(addr);
    server_session_list_pool->dec_ref();
    LOG_DEBUG("reset_fail_count", K(dbkey), K(addr));
  }
}

int32_t ObMysqlGlobalSessionManager::get_fail_count(const common::ObString& dbkey, const ObCommonAddr& addr)
{
  int32_t fail_count = 0;
  ObMysqlServerSessionListPool* server_session_list_pool = get_server_session_list_pool(dbkey);
  if (OB_ISNULL(server_session_list_pool)) {
    LOG_WDIAG("can not be this, in map is null", K(dbkey), K(addr));
  } else {
    fail_count = server_session_list_pool->get_fail_count(addr);
    server_session_list_pool->dec_ref();
  }
  LOG_DEBUG("get_fail_count ", K(dbkey), K(addr), K(fail_count));
  return fail_count;
}

ObMysqlSchemaServerAddrInfo* ObMysqlGlobalSessionManager::acquire_scheme_server_addr_info(const ObProxySchemaKey& schema_key)
{
  ObMysqlSchemaServerAddrInfo* server_addr_info = NULL;
  const common::ObString& dbkey = schema_key.dbkey_.config_string_;
  ObMysqlServerSessionListPool* server_session_list_pool = get_server_session_list_pool(dbkey);
  if (OB_ISNULL(server_session_list_pool)) {
    LOG_WDIAG("can not be this, in map is null", K(dbkey));
  } else {
    server_addr_info = server_session_list_pool->schema_server_addr_info_;
    server_addr_info->inc_ref();
    server_session_list_pool->dec_ref();
  }
  return server_addr_info;
}
int ObMysqlGlobalSessionManager::get_all_session_list_pool(common::ObIArray<ObMysqlServerSessionListPool*> &all_session_list_pool)
{
  int ret = OB_SUCCESS;
  DRWLock::RDLockGuard guard(rwlock_);
  SessionPoolListHashTable::iterator last = global_session_pool_.end();
  for (SessionPoolListHashTable::iterator spot = global_session_pool_.begin(); spot != last; ++spot) {
    ObMysqlServerSessionListPool* server_session_list_pool = &(*spot);
    server_session_list_pool->inc_ref();
    all_session_list_pool.push_back(server_session_list_pool);
  }
  return ret;
}

ObMysqlGlobalSessionManager& get_global_session_manager()
{
  static ObMysqlGlobalSessionManager g_mysql_global_session_manager;
  return g_mysql_global_session_manager;
}
DEF_TO_STRING(SchemaKeyConnInfo)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(schema_key), K_(addr),K_(conn_count));
  J_OBJ_END();
  return pos;
}

ObMysqlContJobList<ObProxySchemaKey>& get_global_schema_key_job_list()
{
  static ObMysqlContJobList<ObProxySchemaKey> g_schema_key_list;
  return g_schema_key_list;
}
ObMysqlContJobList<SchemaKeyConnInfo>& get_global_server_conn_job_list()
{
  static ObMysqlContJobList<SchemaKeyConnInfo> g_schema_key_conn_list;
  return g_schema_key_conn_list;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
