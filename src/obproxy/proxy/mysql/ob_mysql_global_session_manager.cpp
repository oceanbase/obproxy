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
  idle_count_ = 0;
  last_log_time_ = 0;
  pool_ = NULL;
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
    case VC_EVENT_WRITE_COMPLETE:
    case VC_EVENT_WRITE_READY:
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
    SESSION_POOL_LOG(DEBUG, "session pool main handler",
                            "ethread", this_ethread(),
                            "vc", net_vc,
                            "event", ObMysqlDebugNames::get_event_name(event), K(local_ip), K(server_ip));
    bool found = false;
    bool destroy = false;
    {
      //code block for lock
      DRWLock::WRLockGuard guard(get_ss_list_rwlock());
      if (OB_LIKELY(NULL != (ss = local_ip_pool_.get(hash_key)))
          && OB_LIKELY(ss->get_netvc() == net_vc)) {
        // We've found our server session. Remove it from
        // our lists and close it down
        found = true;
        if (OB_LIKELY(KEEP_ALIVE_GLOBAL_SHARED_IN_RESET == ss->state_)) {
          if (VC_EVENT_READ_READY == event) {
            ss->state_ = KEEP_ALIVE_GLOBAL_SHARED;
            if (ss->get_reader() != NULL) {
              // consume resp packet of com_reset_conection
              SESSION_POOL_LOG(DEBUG, "receive reset connection resp from server ingore it",
                               K(ss->get_reader()->read_avail()),
                               K(server_ip), K(local_ip));
              ss->get_reader()->consume_all();
              ss->get_reset_conn_reader()->consume_all();
              OBPROXY_POOL_LOG(TRACE, "reset_session: ok", "server_event", ObMysqlDebugNames::get_event_name(event),
                                      "server_sessid", ss->server_sessid_, "ss_id", ss->ss_id_,
                                      "server_addr", ss->server_ip_, "session_state", ss->get_state_str(), "local_addr", ss->local_ip_,
                                      "dbkey", ss->schema_key_.dbkey_.config_string_, "cur_request_id", ss->get_server_request_id(),
                                      "cur_compressed_seq", ss->get_cur_compressed_seq(),
                                      "ob_capability", ss->get_session_info().get_server_ob_capability(),
                                      "server_vc", ss->get_netvc());
            } else {
              destroy = true;
              SESSION_POOL_LOG(DEBUG, "receive resp from server ingore it but reader is null", K(*ss));
            }
          } else {
            // do nothing for VC_EVENT_WRITE_COMPLETE and VC_EVENT_WRITE_READY:
          }
        } else if (OB_LIKELY(KEEP_ALIVE_GLOBAL_SHARED == ss->state_
                             || KEEP_ALIVE_GLOBAL_SHARED_NOT_RESET == ss->state_)) {
          SESSION_POOL_LOG(DEBUG, "session idle timeout", "ss_id", ss->ss_id_, "event", ObMysqlDebugNames::get_event_name(event),
                                  "server_sessid", ss->server_sessid_, "server_ip", ss->server_ip_);
          destroy = true;
        } else {
          SESSION_POOL_LOG(WDIAG, "unexpected session state", "ss_id", ss->ss_id_,
                                  "server_sessid", ss->server_sessid_, "state", ss->state_);
          destroy = true;
        }

        if (destroy) {
          // Out of the pool! Now!
          remove_from_list_and_pool(ss);
          // Drop connection on this end.
          //mark has lock to prevent double lock in remove
          OBPROXY_POOL_LOG(TRACE, "close_session", "server_event", ObMysqlDebugNames::get_event_name(event),
                                  "server_sessid", ss->server_sessid_, "ss_id", ss->ss_id_,
                                  "server_addr", ss->server_ip_, "session_state", ss->get_state_str(), "local_addr", ss->local_ip_,
                                  "dbkey", ss->schema_key_.dbkey_.config_string_, "cur_request_id", ss->get_server_request_id(),
                                  "cur_compressed_seq", ss->get_cur_compressed_seq(), "ob_capability", ss->get_session_info().get_server_ob_capability(),
                                  "server_buffer_read", ss->get_reader(),
                                  "server_vc", ss->get_netvc());
          ss->has_global_session_lock_ = true;
          close_and_destroy_session(ss);
        }
      }

    }

    if (OB_UNLIKELY(!found)) {
      SESSION_POOL_LOG(INFO, "ignore event, server session has been acquired",
                             "event", ObMysqlDebugNames::get_event_name(event),
                             "server_ip", server_ip, "local_ip", local_ip);
    }
  }
  return VC_EVENT_NONE;
}
void ObMysqlServerSessionList::purge_session_list()
{
  DRWLock::WRLockGuard guard(get_ss_list_rwlock());
  while (!server_session_list_.empty()) {
    ObMysqlServerSession* session = (ObMysqlServerSession*)server_session_list_.pop();
    session->has_global_session_lock_ = true;
    close_and_destroy_session(session);
  }
  local_ip_pool_.reset();
}
void ObMysqlServerSessionList::do_kill_session()
{
  LOG_DEBUG("do_kill_session", K(common_addr_));
  DRWLock::WRLockGuard guard(get_ss_list_rwlock());
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

// 从原子链表 server_session_list_ 获取 server session
ObMysqlServerSession* ObMysqlServerSessionList::acquire_first_from_list()
{
  DRWLock::WRLockGuard guard(get_ss_list_rwlock());
  ObMysqlServerSession* ss = (ObMysqlServerSession*)server_session_list_.pop();
  if (ss != NULL) {
    //ATOMIC_DEC(&free_count_);
    //using_count_ = total_count_ - free_count_;
    //if (using_count_ > max_used_) {
    //  max_used_ = using_count_;
    //}
    local_ip_pool_.remove(ss);
    ss->cancel_inactivity_timeout();
    ss->state_ = KEEP_ALIVE_ACTIVE;
    LOG_DEBUG("acquire_first_from_list", K(ss->server_ip_), K(ss->auth_user_));
  } else {
    LOG_DEBUG("acquire_first_from_list is null");
  }
  return ss;
}

ObMysqlServerSession* ObMysqlServerSessionList::acquire_matched_from_list(const ObServerSessionMatchRules &rules)
{
  ObMysqlServerSession *matched_ss = NULL;
  {
    DRWLock::WRLockGuard guard(get_ss_list_rwlock());
    ObMysqlServerSession *cur_ss = (ObMysqlServerSession*)server_session_list_.head();
    while (OB_NOT_NULL(cur_ss)) {
      if (rules.is_matched(cur_ss)) {
        matched_ss = cur_ss;
        server_session_list_.remove(matched_ss);
        local_ip_pool_.remove(matched_ss);
        break;
      } else {
        cur_ss = static_cast<ObMysqlServerSession *>(server_session_list_.next(cur_ss));
      }
    }
  }

  if (matched_ss != NULL) {
    int ret = OB_SUCCESS;
    ObEThread *session_pool_thread = matched_ss->get_netvc()->thread_;
    if (OB_FAIL(matched_ss->migrate_from_session_pool_thread()))  {
      SESSION_POOL_LOG(EDIAG, "fail to migrate vc from ET_SESS_POOL to ET_NET", K(ret));
      close_and_destroy_session(matched_ss);
    } else {
      ATOMIC_DEC(&idle_count_);
      OBPROXY_POOL_LOG(TRACE, "acquire_session",
        "ss_id", matched_ss->ss_id_,
        "server_sessid", matched_ss->server_sessid_,
        "server_addr", matched_ss->server_ip_,
        "session_state", matched_ss->get_state_str(),
        "local_addr", matched_ss->local_ip_,
        "dbkey", matched_ss->schema_key_.dbkey_.config_string_,
        "cur_request_id", matched_ss->get_server_request_id(),
        "cur_compressed_seq", matched_ss->get_cur_compressed_seq(),
        "ob_capability", matched_ss->get_session_info().get_server_ob_capability(),
        "server_vc", matched_ss->get_netvc(),
        KP(this_ethread()), KP(session_pool_thread));
      matched_ss->cancel_inactivity_timeout();
      matched_ss->state_ = KEEP_ALIVE_ACTIVE;
      LOG_DEBUG("acquire_first_from_list", K(matched_ss),
                K(matched_ss->server_ip_), K(matched_ss->auth_user_));
    }
  } else {
    LOG_DEBUG("acquire_first_from_list is null");
  }
  return matched_ss;
}

// 放入原子链表 server_session_list_
int ObMysqlServerSessionList::release_to_list(ObMysqlServerSession& server_session)
{
  int ret = OB_SUCCESS;

  ObUnixNetVConnection *server_vc = static_cast<ObUnixNetVConnection*>(server_session.get_netvc());
  // Now we need to issue a read on the connection to detect
  // if it closes on us. We will get called back in the
  // continuation for this bucket, ensuring we have the lock

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(server_vc)) {
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
    server_vc->set_inactivity_timeout(HRTIME_USECONDS(get_global_proxy_config().session_pool_idle_timeout));
    server_vc->set_active_timeout(server_vc->get_active_timeout());
    server_session.clear_client_session();
    net::ObIpEndpoint local_ip;
    local_ip.assign(server_session.get_netvc()->get_local_addr());
    server_session.state_ = KEEP_ALIVE_GLOBAL_SHARED_NOT_RESET;
    ObEThread *session_pool_thread = NULL;
    // 先迁移 vc 到 ET_SESS_POOL 线程, 再将 vc 放入 local_ip_pool_
    // 如果先放入 local_ip_pool_ 就有可能在迁移 vc 之前就被获取出去了
    if (OB_FAIL(server_session.migrate_to_session_pool_thread())) {
        LOG_WDIAG("fail to migrate server session", K(ret));
    } else {
      if (server_session.get_netvc() != NULL) {
        session_pool_thread = server_session.get_netvc()->thread_;
      }
      {
        DRWLock::WRLockGuard guard(get_ss_list_rwlock());
        if (OB_FAIL(local_ip_pool_.set_refactored(&server_session))) {
          server_session_list_.remove(&server_session);
          LOG_WDIAG("add to local_ip_pool_ failed", K(ret), K(local_ip));
        } else {
          server_session_list_.push(&server_session);
        }
      }
      if (OB_SUCC(ret)) {
        ATOMIC_INC(&idle_count_);
        OBPROXY_POOL_LOG(TRACE, "release_session", "ss_id", server_session.ss_id_, "server_sessid", server_session.server_sessid_,
                                "server_addr", server_session.server_ip_, "session_state", server_session.get_state_str(),
                                "local_addr", server_session.local_ip_, "dbkey", server_session.schema_key_.dbkey_.config_string_,
                                "cur_request_id", server_session.get_server_request_id(), "cur_compressed_seq", server_session.get_cur_compressed_seq(),
                                "ob_capability", server_session.get_session_info().get_server_ob_capability(),
                                "server_vc", server_session.get_netvc(),
                                KP(this_ethread()), KP(session_pool_thread));
        LOG_DEBUG("succ add to local_ip_pool_", K(idle_count_), K(server_session.ss_id_), K(server_session.auth_user_),
                                                K(server_session.server_ip_), K(local_ip));
      }
    }

  }
  return ret;
}

void ObMysqlServerSessionList::remove_from_list_and_pool(ObMysqlServerSession* server_session)
{
  server_session_list_.remove(server_session);
  local_ip_pool_.remove(ObLocalIPHashing::key(server_session));
  pool_->decr_idle_session_count();
}

int ObMysqlServerSessionList::do_pool_log(const ObProxySchemaKey& schema_key, bool force_log)
{
  UNUSED(force_log);
  int ret = OB_SUCCESS;
  //log when reach ratio
  const ObString& dbkey = schema_key.dbkey_.config_string_;
  int64_t now_time = event::get_hrtime();
  last_log_time_ = now_time;
  OBPROXY_POOL_STAT_LOG(INFO, "session_pool_stat", K(dbkey),  "server_addr", common_addr_.ip_endpoint_, K_(idle_count));
  return ret;
}

ObMysqlServerSessionListPool::ObMysqlServerSessionListPool()
  : idle_session_count_(0), schema_server_addr_info_(NULL)
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
  if (OB_FAIL(acquire_ss_list(key, ss_list))) {
  } else {
    conn_count = ss_list->idle_count_;
    ss_list->dec_ref();
  }
  return conn_count;
}

int64_t ObMysqlServerSessionListPool::incr_idle_session_count()
{
  int64_t new_count = ATOMIC_AAF(&idle_session_count_, 1);
  SESSION_POOL_LOG(DEBUG, "incr idle session", "idle_session_count", new_count, K(schema_key_));
  return new_count;
}
int64_t ObMysqlServerSessionListPool::decr_idle_session_count()
{
  int64_t new_count = ATOMIC_SAF(&idle_session_count_, 1);
  SESSION_POOL_LOG(DEBUG, "decr idle session", "idle_session_count", new_count, K(schema_key_));
  return new_count;
}

int ObMysqlServerSessionListPool::acquire_ss_list(const ObCommonAddr& key,
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
  ObServerSessionMatchRules *rules)
{
  int ret = OB_SUCCESS;
  ObMysqlServerSessionList* ss_list = NULL;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(acquire_ss_list(key, ss_list))) {
    } else if (rules != NULL && NULL != (server_session = (ObMysqlServerSession*)ss_list->acquire_matched_from_list(*rules))) {
      LOG_DEBUG("acquire_matched_from_list succ", K(schema_key_.dbkey_),
                K(key), K(idle_session_count_), KP(server_session));
    } else if (rules == NULL && NULL != (server_session = (ObMysqlServerSession*)ss_list->acquire_first_from_list())) {
      LOG_DEBUG("acquire_first_from_list succ", K(schema_key_.dbkey_),
                K(key), K(idle_session_count_), KP(server_session));
    }
    if (server_session != NULL) {
      decr_idle_session_count();
      if (ss_list != NULL) {
        ss_list->dec_ref();
      }
    }
  }
  return ret;
}

int ObMysqlServerSessionListPool::release_server_session(ObMysqlServerSession &ss)
{
  int ret = OB_SUCCESS;
  const ObString& dbkey = ss.schema_key_.dbkey_.config_string_;
  ss.last_active_time_ = ObTimeUtility::current_time();
  LOG_DEBUG("[ObMysqlServerSessionListPool::release_session]", K(ss.ss_id_), K(ss.auth_user_),
            K(ss.server_ip_), K(dbkey), K(ss.last_active_time_));
  ObCommonAddr& key = ss.common_addr_;
  ObMysqlServerSessionList* ss_list = NULL;
  if (OB_FAIL(acquire_ss_list(key, ss_list))) {
    LOG_DEBUG("not exist in map", K(ret), K(dbkey));
    DRWLock::WRLockGuard guard(rwlock_);
    if (OB_FAIL(server_session_list_pool_.get_refactored(ss.common_addr_, ss_list))) {
      if (OB_ISNULL(ss_list = op_alloc(ObMysqlServerSessionList))) {
        LOG_EDIAG("fail to allocate ", K(dbkey));
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(ss_list->init())) {
        LOG_EDIAG("fail to init ss_list", K(dbkey));
        ret = OB_ERR_UNEXPECTED;
        op_free(ss_list);
        ss_list = NULL;
      } else {
        ss_list->pool_ = this;
        ss_list->auth_user_.set_value(ss.auth_user_);
        ss_list->server_ip_ = ss.server_ip_;
        ss_list->common_addr_ = ss.common_addr_;
        ss_list->inc_ref();
        if (OB_FAIL(server_session_list_pool_.unique_set(ss_list))) {
          LOG_WDIAG("add to map failed", K(ss.common_addr_), K(ret));
          ss_list->dec_ref();
          ss_list = NULL;
          ret = OB_ERR_UNEXPECTED;
        } else {
          ss_list->inc_ref();
          LOG_DEBUG("add to session list succ", K(schema_key_), K(ss.common_addr_), K(idle_session_count_));
        }
      }
    }
  } else if (OB_ISNULL(ss_list)) {
    LOG_WDIAG("ss_list should not null here", K(dbkey));
    ret = OB_ERR_UNEXPECTED;
  }

  if (OB_SUCC(ret)) {
    add_server_addr_if_not_exist(ss.common_addr_);
    if (OB_FAIL(ss_list->release_to_list(ss))) {
      LOG_WDIAG("fail to release server session to ss_list", K(ret));
    } else {
      incr_idle_session_count();
    }
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
  UNUSED(ss_id);

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

// 获取会话连接池中的 server session
int ObMysqlGlobalSessionManager::acquire_server_session(
    const ObProxySchemaKey& schema_key,
    const ObCommonAddr &addr,
    ObMysqlServerSession *&server_session,
    ObServerSessionMatchRules *rules)
{
  int ret = OB_SUCCESS;
  const common::ObString& dbkey = schema_key.dbkey_.config_string_;
  ObMysqlServerSessionListPool* server_session_list_pool = get_server_session_list_pool(dbkey);
  if (OB_ISNULL(server_session_list_pool)) {
    if (OB_FAIL(add_schema_if_not_exist(schema_key, server_session_list_pool))) {
      LOG_WDIAG("add schema failed when not exist", K(dbkey));
    }
  }
  if (OB_ISNULL(server_session_list_pool)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("should not null here", K(dbkey));
  } else {
    ret = server_session_list_pool->acquire_server_session(addr, server_session, rules);
    server_session_list_pool->dec_ref();
  }

  if (OB_NOT_NULL(server_session)) {
    server_session->set_need_reset_by_change_user(true);
  }
  return ret;
}

// 将 server session 放回会话连接池
int ObMysqlGlobalSessionManager::release_server_session(ObMysqlServerSession &to_release)
{
  int ret = OB_SUCCESS;
  const ObString& dbkey = to_release.schema_key_.dbkey_.config_string_;
  ObMysqlServerSessionListPool* server_session_list_pool = NULL;
  LOG_DEBUG("[ObMysqlGlobalSessionManager::release_session]", K(dbkey));
  if (dbkey.empty()) {
    LOG_WDIAG("dbkey should not empty");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(server_session_list_pool = get_server_session_list_pool(dbkey))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("server_session_list_pool is null, should not here", K(dbkey));
  } else {
    ret = server_session_list_pool->release_server_session(to_release);
    server_session_list_pool->dec_ref();
  }
  if (OB_FAIL(ret)) {
    LOG_WDIAG("[ObMysqlGlobalSessionManager::release_session] fail to release session to global pool", K(ret), K(to_release));
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
  for (SessionPoolListHashTable::iterator spot = global_session_pool_.begin(); OB_SUCC(ret) && spot != last; ++spot) {
    ObMysqlServerSessionListPool* server_session_list_pool = &(*spot);
    server_session_list_pool->inc_ref();
    if (OB_FAIL(all_session_list_pool.push_back(server_session_list_pool))) {
      SESSION_POOL_LOG(EDIAG, "fail to push back server_session_list_pool", K(ret));
    }
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
