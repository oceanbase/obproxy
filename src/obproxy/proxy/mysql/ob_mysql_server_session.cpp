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

#include "proxy/mysql/ob_mysql_server_session.h"
#include "proxy/mysql/ob_mysql_sm.h"
#include "prometheus/ob_sql_prometheus.h"
#include "proxy/mysql/ob_mysql_global_session_manager.h"
#include "obutils/ob_proxy_config.h"
#include "utils/ob_proxy_table_define.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::prometheus;
using namespace oceanbase::obproxy::dbconfig;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
void ObMysqlServerSession::destroy()
{
  PROXY_SS_LOG(DEBUG, "ObMysqlServerSession::destroy()", K(ss_id_), K(server_sessid_));
  if (OB_UNLIKELY(NULL != server_vc_) || OB_UNLIKELY(NULL == read_buffer_)
      || OB_UNLIKELY(0 != server_trans_stat_)) {
    PROXY_SS_LOG(WARN, "the server session cannot be destroyed", K(server_vc_),
                 K(read_buffer_), K(server_trans_stat_));
  }
  is_inited_ = false;
  magic_ = MYSQL_SS_MAGIC_DEAD;
  if (NULL != buf_reader_) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(buf_reader_->consume(buf_reader_->read_avail()))) {
      PROXY_SS_LOG(WARN, "fail to consume ", K(ret));
    }
  }
  if (OB_LIKELY(NULL != read_buffer_)) {
    free_miobuffer(read_buffer_);
    read_buffer_ = NULL;
  }
  session_info_.reset();
  buf_reader_ = NULL;
  mutex_.release();
  schema_key_.reset();
  op_reclaim_free(this);
}

int ObMysqlServerSession::new_connection(ObMysqlClientSession &client_session, ObNetVConnection &new_vc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    PROXY_SS_LOG(WARN, "init twice", K(is_inited_), K(ret));
  } else {
    client_session_ = &client_session;
    server_vc_ = &new_vc;
    mutex_ = new_vc.mutex_;

    if (client_session.is_session_pool_client()) {
      is_pool_session_ = true;
      if (OB_FAIL(ObMysqlSessionUtils::init_schema_key_with_client_session(schema_key_, client_session_))) {
        PROXY_SS_LOG(WARN, "init_schema_key_with_client_session failed", K(ret));
      } else if (OB_FAIL(ObMysqlSessionUtils::init_common_addr_with_client_session(common_addr_, server_ip_, client_session_))) {
        PROXY_SS_LOG(WARN, "init_common_addr_with_client_session failed", K(ret));
      } else {
        OBPROXY_POOL_LOG(INFO, "new_connection", K(schema_key_), K(local_ip_), K(server_ip_));
      }
    }

    if (OB_SUCC(ret)) {
      // Unique server session identifier.
      ss_id_ = get_next_ss_id();

      magic_ = MYSQL_SS_MAGIC_ALIVE;
      MYSQL_SUM_GLOBAL_DYN_STAT(CURRENT_SERVER_CONNECTIONS, 1); // Update the true global stat
      MYSQL_INCREMENT_DYN_STAT(TOTAL_SERVER_CONNECTIONS);
      SESSION_PROMETHEUS_STAT(client_session.get_session_info(), PROMETHEUS_CURRENT_SESSION, false, 1);

      read_buffer_ = new_empty_miobuffer(MYSQL_BUFFER_SIZE);
      if (OB_LIKELY(NULL != read_buffer_)) {
        buf_reader_ = read_buffer_->alloc_reader();
        /**
         * we cache the response in the read io buffer, so the water mark of
         * read io buffer must be larger than the response packet size. we set
         * MYSQL_NET_META_LENGTH as the default water mark, when we read
         * the header of response, we reset the water mark.
         */
        read_buffer_->water_mark_ = MYSQL_NET_META_LENGTH;
        if (OB_FAIL(session_info_.init())) {
          PROXY_SS_LOG(WARN, "fail to init session_info", K_(ss_id), K(ret));
        } else {
          DBServerType server_type = client_session.get_session_info().get_server_type();
          session_info_.set_server_type(server_type);
          PROXY_SS_LOG(INFO, "server session born", K_(ss_id), K_(server_ip),
                       "cs_id", client_session.get_cs_id(),
                       "proxy_sessid", client_session.get_proxy_sessid(),
                       K(server_type));
          state_ = MSS_INIT;
          is_inited_ = true;
          if (is_pool_session_) {
            ret = get_global_session_manager().add_server_session(*this);
            create_time_ = ObTimeUtility::current_time();
          }
        }
      } else {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        PROXY_SS_LOG(WARN, "alloc mem for read_buffer_ error", K(ret));
      }
    }
  }
  return ret;
}

ObVIO *ObMysqlServerSession::do_io_read(
    ObContinuation *c, const int64_t nbytes, ObMIOBuffer *buf)
{
  return server_vc_->do_io_read(c, nbytes, buf);
}

ObVIO *ObMysqlServerSession::do_io_write(
    ObContinuation *c, const int64_t nbytes, ObIOBufferReader *buf)
{
  return server_vc_->do_io_write(c, nbytes, buf);
}

void ObMysqlServerSession::do_io_shutdown(const ShutdownHowToType howto)
{
  PROXY_SS_LOG(INFO, "ObMysqlServerSession::do_io_shutdown", K(*this));
  server_vc_->do_io_shutdown(howto);
}

void ObMysqlServerSession::do_io_close(const int alerrno)
{
  bool need_print = true;
  if(NULL != client_session_ &&
     client_session_->get_session_info().get_priv_info().user_name_ == ObProxyTableInfo::DETECT_USERNAME_USER) {
    need_print = false;
  }

  if (need_print) {
    PROXY_SS_LOG(INFO, "server session do_io_close", K(*this), KP(server_vc_), KP(this));
  }
  if (MSS_ACTIVE == state_) {
    MYSQL_DECREMENT_DYN_STAT(CURRENT_SERVER_TRANSACTIONS);
    --server_trans_stat_;
  }

  if (NULL != server_vc_) {
    if (is_pool_session_) {
      get_global_session_manager().remove_server_session(*this);
      OBPROXY_POOL_LOG(INFO, "close_session", K(schema_key_), K(local_ip_), K(server_ip_));
    }
    server_vc_->do_io_close(alerrno);
    server_vc_ = NULL;
  }

  MYSQL_SUM_GLOBAL_DYN_STAT(CURRENT_SERVER_CONNECTIONS, -1); // Make sure to work on the global stat
  MYSQL_SUM_DYN_STAT(TRANSACTIONS_PER_SERVER_CON, transact_count_);

  if (OB_LIKELY(NULL != client_session_)) {
    SESSION_PROMETHEUS_STAT(client_session_->get_session_info(), PROMETHEUS_CURRENT_SESSION, false, -1);
    if (this == client_session_->get_cur_server_session()) {
      client_session_->set_cur_server_session(NULL);
    }
    if (this == client_session_->get_lii_server_session()) {
      client_session_->set_lii_server_session(NULL);
    }
    if (this == client_session_->get_server_session()) {
      client_session_->set_server_session(NULL);
    }
    if (this == client_session_->get_last_bound_server_session()) {
      client_session_->set_last_bound_server_session(NULL);
    }
    if (server_ip_ == client_session_->get_trans_coordinator_ss_addr()) {
      client_session_->get_trans_coordinator_ss_addr().reset();
    }
    if (need_print) {
      PROXY_SS_LOG(INFO, "server session is closing", K_(ss_id), K_(server_sessid), K_(server_ip),
          "cs_id", client_session_->get_cs_id(),
          "proxy_sessid", client_session_->get_proxy_sessid());
    }
  } else {
    PROXY_SS_LOG(INFO, "server session has not bound to client session", K(*this));
  }

  destroy();
}

void ObMysqlServerSession::reenable(ObVIO *vio)
{
  server_vc_->reenable(vio);
}

int ObMysqlServerSession::release()
{
  int ret = OB_SUCCESS;
  PROXY_SS_LOG(DEBUG, "Releasing server session", K(server_trans_stat_));
  // Set our state to KA for stat issues
  state_ = MSS_KA_SHARED;
  if (is_pool_session_) {
    if (OB_NOT_NULL(client_session_)
        && !client_session_->get_session_info().is_sharding_user()
        && get_global_proxy_config().enable_no_sharding_skip_real_conn) {
      // for v1 user save password for check
      const ObString& password = client_session_->get_session_info().get_login_req().get_hsr_result().response_.get_auth_response();
      if (OB_ISNULL(schema_key_.shard_conn_)) {
        PROXY_SS_LOG(WARN, "shard_conn_ is null, unexpected", K(schema_key_));
      } else if (schema_key_.shard_conn_->password_.empty()) {
        PROXY_SS_LOG(DEBUG, "save password", K(password.hash()));
        schema_key_.shard_conn_->password_.set_value(password);
      } else if (schema_key_.shard_conn_->password_.config_string_.compare(password) != 0) {
        PROXY_SS_LOG(DEBUG, "has password but not equal, update now",
                     K(schema_key_.shard_conn_->password_.config_string_.hash()), K(password.hash()));
        schema_key_.shard_conn_->password_.set_value(password);
      }
    }
    if (OB_FAIL(get_global_session_manager().release_session(*this))) {
      PROXY_SS_LOG(WARN, "fail to release server session to global, it will be closed", K(ret));
      do_io_close();
    }
  } else  {
    // if hava shard_connector, should push into new session pool, even if is not shardingUser
    ObShardConnector *shard_conn = session_info_.get_shard_connector();
    if (OB_NOT_NULL(shard_conn)) {
      if (OB_FAIL(client_session_->get_session_manager_new().release_session(
                  shard_conn->shard_name_.config_string_, *this))) {
        PROXY_SS_LOG(WARN, "fail to release server session to new session manager, it will be closed", K(ret));
      }
    } else {
      if (OB_FAIL(client_session_->get_session_manager().release_session(*this))) {
        PROXY_SS_LOG(WARN, "fail to release server session, it will be closed", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
      do_io_close();
    }
  }
  return ret;
}

const char *ObMysqlServerSession::get_state_str() const
{
  const char *ret = "MSS_INVALID";
  static const char *state[MSS_MAX] = {"MSS_INIT",
    "MSS_ACTIVE",
    "MSS_KA_CLIENT_SLAVE",
    "MSS_KA_SHARED"};
  if (OB_LIKELY(state_ < MSS_MAX)) {
    ret = state[state_];
  }
  return ret;
}

DEF_TO_STRING(ObMysqlServerSession)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(ss_id),
       K_(server_sessid),
       K_(server_ip),
       K_(is_inited),
       K_(magic),
       K_(state),
       KP_(server_vc),
       KPC_(client_session),
       K_(transact_count),
       K_(server_trans_stat),
       K_(session_info));
  J_OBJ_END();
  return pos;
}

ObServerAddrLookupHandler::ObServerAddrLookupHandler(ObProxyMutex &m,
                                                     ObContinuation &cont,
                                                     const ObProxyKillQueryInfo &query_info)
  : ObContinuation(&m), priv_info_(), query_info_(), cs_id_array_()
{
  query_info_.is_kill_query_ = true;
  query_info_.cs_id_ = query_info.cs_id_;

  action_.set_continuation(&cont);
  saved_event_ = -1;
  submit_thread_ = (OB_LIKELY(NULL != cont.mutex_) ? cont.mutex_->thread_holding_ : NULL);
  SET_HANDLER(&ObServerAddrLookupHandler::main_handler);
}

int ObServerAddrLookupHandler::main_handler(int event, void *data)
{
  UNUSED(data);
  UNUSED(event);
  PROXY_SS_LOG(ERROR, "it should not arrive here", K(event), KP(data));
  return EVENT_DONE;
}

int ObServerAddrLookupHandler::handle_lookup_with_proxy_conn_id(int event, void *data)
{
  UNUSED(data);
  UNUSED(event);
  int ret = OB_SUCCESS;
  int event_ret = EVENT_DONE;
  ObEThread &ethread = self_ethread();
  bool need_callback = true;
  if (OB_FAIL(lookup_server_addr_with_proxy_conn_id(ethread, priv_info_, query_info_))) {
    if (OB_NEED_RETRY == ret) {
      if (OB_ISNULL(ethread.schedule_in(this, HRTIME_MSECONDS(1)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        PROXY_SS_LOG(ERROR, "fail to schedule self", K(ethread.id_), K(ret));
      } else {
        need_callback = false;
        PROXY_SS_LOG(DEBUG, "fail to do lookup_server_addr_with_proxy_conn_id, need reschedule", K(ethread.id_), K(ret));
      }
    } else {
      PROXY_SS_LOG(WARN, "fail to do lookup_server_addr_with_proxy_conn_id", K(ethread.id_), K(ret));
    }
  }

  if (need_callback) {
    event_ret = handle_callback(SERVER_ADDR_LOOKUP_EVENT_DONE, static_cast<void *>(&query_info_));
  }
  return event_ret;
}

int ObServerAddrLookupHandler::handle_lookup_with_server_conn_id(int event, void *data)
{
  UNUSED(data);
  UNUSED(event);
  int ret = OB_SUCCESS;
  int event_ret = EVENT_DONE;
  bool need_callback = true;
  bool is_finished = false;
  ObEThread &ethread = self_ethread();
  if (OB_FAIL(lookup_server_addr_with_server_conn_id(ethread, is_finished))) {
    if (OB_NEED_RETRY == ret) {
      if (OB_ISNULL(ethread.schedule_in(this, HRTIME_MSECONDS(1)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        PROXY_SS_LOG(ERROR, "fail to schedule self", K(ethread.id_), K(ret));
      } else {
        need_callback = false;
        PROXY_SS_LOG(DEBUG, "fail to do lookup_server_addr_with_server_conn_id, need reschedule", K(ethread.id_), K(ret));
      }
    } else {
      PROXY_SS_LOG(WARN, "fail to do lookup_server_addr_with_server_conn_id", K(is_finished), K(ethread.id_), K(ret));
    }
  } else if (!is_finished) {
    const int64_t next_id = ((ethread.id_ + 1) % g_event_processor.thread_count_for_type_[ET_NET]);
    if (NULL != submit_thread_ && next_id != submit_thread_->id_) {
      need_callback = false;
      if (OB_ISNULL(g_event_processor.event_thread_[ET_NET][next_id]->schedule_imm(this))) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_SS_LOG(WARN, "schedule event error, event is null", K(ret));
      }
    } else {
      query_info_.errcode_ = OB_UNKNOWN_CONNECTION; //not found the specific session
      PROXY_SS_LOG(DEBUG, "not found the specific session", K(query_info_));
    }
  } else {
    PROXY_SS_LOG(DEBUG, "succ to do lookup_server_addr_with_server_conn_id", K(ethread.id_));
  }

  if (need_callback) {
    event_ret = handle_callback(SERVER_ADDR_LOOKUP_EVENT_DONE, static_cast<void *>(&query_info_));
  }
  return event_ret;
}

int ObServerAddrLookupHandler::lookup_server_addr_with_server_conn_id(const ObEThread &ethread,
    bool &is_finished)
{
  int ret = OB_SUCCESS;
  const uint32_t cs_id = static_cast<uint32_t>(query_info_.cs_id_);
  is_finished = false;
  ObMysqlClientSessionMap &cs_map = get_client_session_map(ethread);
  if(cs_id_array_.empty()) {
    // when we first traverse this thread, we cs_id_array_ must be empty, we need init it
    ObMysqlClientSessionMap::IDHashMap &id_map = cs_map.id_map_;
    cs_id_array_.reuse();
    if (OB_FAIL(cs_id_array_.reserve(id_map.count()))) {
      PROXY_SS_LOG(WARN, "fail to reserve cs_id_array", K(ethread.id_), "cs count", id_map.count(), K(ret));
    } else {
      ObMysqlClientSessionMap::IDHashMap::iterator spot = id_map.begin();
      ObMysqlClientSessionMap::IDHashMap::iterator end = id_map.end();
      ObCSIDHandler cs_id_handler;
      for (; OB_SUCC(ret) && spot != end; ++spot) {
        cs_id_handler.cs_id_ = spot->get_cs_id();
        if (OB_FAIL(cs_id_array_.push_back(cs_id_handler))) {
          PROXY_SS_LOG(WARN, "fail to push_back cs_id_array", K(cs_id_handler), K(ethread.id_), K(ret));
        }
      }
    }
  }

  ObMysqlClientSession *cs = NULL;
  int64_t lock_fail_count = 0;
  uint32_t curr_cs_id = 0;
  // traverse all the cs from cs map, if lock failed, inc the lock_fail_count and try next.
  // when finish one traverse, check is_finished and lock_fail_count, do next traverse
  for (int64_t i = 0; OB_SUCC(ret) && !is_finished && i < cs_id_array_.count(); ++i) {
    curr_cs_id = cs_id_array_.at(i).cs_id_;
    if (cs_id_array_.at(i).is_used_) {
      //do nothing
    } else if (OB_FAIL(cs_map.get(curr_cs_id, cs))) {
      if (OB_HASH_NOT_EXIST == ret) {
        //if cs has gone, just treat it as used
        cs_id_array_.at(i).is_used_ = true;
        ret = OB_SUCCESS;
      } else {
        PROXY_SS_LOG(WARN, "fail to get cs from cs map ", K(curr_cs_id), K(ret));
      }
    } else if (OB_ISNULL(cs)) {
      ret = OB_ERR_NULL_VALUE;
      PROXY_SS_LOG(WARN, "cs is null", K(curr_cs_id), K(ret));
    } else {
      MUTEX_TRY_LOCK(lock, cs->mutex_, this_ethread());
      if (OB_UNLIKELY(!lock.is_locked())) {
        ++lock_fail_count;
        PROXY_SS_LOG(DEBUG, "fail to try lock cs in cs_map, need retry latter", K(curr_cs_id), K(ethread.id_), K(lock_fail_count));
      } else {
        cs_id_array_.at(i).is_used_ = true;
        if (cs->is_hold_conn_id(cs_id)) {
          if (OB_FAIL(lookup_server_addr(*cs, priv_info_, query_info_))) {
            PROXY_SS_LOG(WARN, "fail to lookup_server_addr", K(cs_id), K(ret));
          } else {
            is_finished = true;
            PROXY_SS_LOG(DEBUG, "succ to lookup_server_addr", K(cs_id));
          }
        }//end of is_hold_conn_id
      }//end of locked
    }//end of else
  }//end of for

  if (OB_SUCC(ret) && !is_finished) {
    if (0 == lock_fail_count) {
      cs_id_array_.reuse();
    } else {
      ret = OB_NEED_RETRY;
      PROXY_SS_LOG(DEBUG, "fail to try lock cs in cs_map, need retry", K(cs_id), K(ethread.id_), K(lock_fail_count), K(ret));
    }
  }
  return ret;
}

int ObServerAddrLookupHandler::handle_callback(int event, void *data)
{
  UNUSED(data);
  int ret = OB_SUCCESS;
  if (saved_event_ < 0) {
    saved_event_ = event;
  }

  if (this_ethread() != submit_thread_) {
    PROXY_SS_LOG(DEBUG, "Not the same thread, do scheduling");
    SET_HANDLER(&ObServerAddrLookupHandler::handle_callback);
    if (OB_ISNULL(submit_thread_->schedule_imm(this))) {
      ret = OB_ERR_UNEXPECTED;
      PROXY_SS_LOG(WARN, "schedule event error, event is null", K(ret));
    }
  } else {
    PROXY_SS_LOG(DEBUG, "The same thread, directly execute", K(query_info_));
    MUTEX_TRY_LOCK(lock, action_.mutex_, submit_thread_);
    if (lock.is_locked()) {
      if (!action_.cancelled_) {
        action_.continuation_->handle_event(saved_event_, static_cast<void *>(&query_info_));
      } else {
        PROXY_SS_LOG(DEBUG, "action is cancelled");
      }
      delete this;
    } else if (OB_ISNULL(submit_thread_->schedule_in(this, HRTIME_MSECONDS(1)))) {
      ret = OB_ERR_UNEXPECTED;
      PROXY_SS_LOG(WARN, "schedule event error, event is null", K(ret));
    }
  }
  return EVENT_DONE;
}

int ObServerAddrLookupHandler::create_continuation(ObContinuation &cont,
    const ObProxySessionPrivInfo &priv_info, const ObProxyKillQueryInfo &query_info,
    ObServerAddrLookupHandler *&handler, event::ObProxyMutex *&mutex)
{
  int ret = OB_SUCCESS;
  handler = NULL;
  mutex = NULL;
  if (OB_ISNULL(mutex = new_proxy_mutex())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_SM_LOG(ERROR, "fail to new ObProxyMutex", K(ret));
  } else if (OB_ISNULL(handler = new(std::nothrow) ObServerAddrLookupHandler(*mutex, cont, query_info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_SM_LOG(ERROR, "fail to new ObServerAddrLookupHandler", K(ret));
  } else if (OB_FAIL(handler->priv_info_.deep_copy(&priv_info))) {
    PROXY_SS_LOG(WARN, "fail to deep copy session priv", K(priv_info), K(ret));
  } else {
    //do noting
  }

  if (OB_FAIL(ret)) {
    if (OB_LIKELY(NULL != handler)) {
      delete handler;
      handler = NULL;
    }
    if (OB_LIKELY(NULL != mutex)) {
      mutex->free();
      mutex = NULL;
    }
  }
  return ret;
}

int ObServerAddrLookupHandler::lookup_server_addr(ObContinuation &cont,
    const ObProxySessionPrivInfo &priv_info, ObProxyKillQueryInfo &query_info,
    ObAction *&addr_lookup_action_handle)
{
  int ret = OB_SUCCESS;
  addr_lookup_action_handle = NULL;
  ObEThread &ethread = self_ethread();
  int64_t thread_id = -1;
  bool is_proxy_conn_id = true;
  ObServerAddrLookupHandler *handler = NULL;
  ObProxyMutex *mutex = NULL;

  if (OB_FAIL(query_info.do_privilege_check(priv_info))) {
    PROXY_SS_LOG(WARN, "fail to do privilege check", K(query_info), K(ret));
  } else if (OB_UNLIKELY(!is_conn_id_avail(query_info.cs_id_, is_proxy_conn_id))) {
    ret = OB_ERR_UNEXPECTED;
    query_info.errcode_ = OB_UNKNOWN_CONNECTION;
    PROXY_SS_LOG(WARN, "cs_id is not avail", K(query_info));
  } else {
    if (is_proxy_conn_id) {
      //session id got from obproxy
      if (OB_FAIL(extract_thread_id(static_cast<uint32_t>(query_info.cs_id_), thread_id))) {
        query_info.errcode_ = OB_RESULT_UNKNOWN;
        PROXY_SS_LOG(WARN, "fail to extract thread id, it should not happen", K(query_info), K(ret));
      } else {
        //the same thread
        if (thread_id == ethread.id_
            && OB_FAIL(lookup_server_addr_with_proxy_conn_id(ethread, priv_info, query_info))
            && OB_NEED_RETRY != ret) {
          PROXY_SS_LOG(WARN, "fail to do lookup_server_addr_with_proxy_conn_id", K(ethread.id_), K(ret));
        } else {
          //not the same thread or need retry
          if (thread_id == ethread.id_  && OB_SUCC(ret)) {
            //do nothing
          } else if (OB_SUCC(create_continuation(cont, priv_info, query_info, handler, mutex))) {
            SET_CONTINUATION_HANDLER(handler, &ObServerAddrLookupHandler::handle_lookup_with_proxy_conn_id);
            addr_lookup_action_handle = &(handler->get_action());
            if (OB_ISNULL(g_event_processor.event_thread_[ET_NET][thread_id]->schedule_imm(handler))) {
              query_info.errcode_ = OB_RESULT_UNKNOWN;
              ret = OB_ERR_UNEXPECTED;
              PROXY_SS_LOG(WARN, "schedule event error, event is null", K(ret));
              addr_lookup_action_handle = NULL;
            } else {
              PROXY_SS_LOG(INFO, "succ to schedule ObServerAddrLookupHandler with proxy conn_id", K(query_info));
            }
          }//end of OB_SUCC
        }
      }
    } else {
      if (OB_SUCC(create_continuation(cont, priv_info, query_info, handler, mutex))) {
        SET_CONTINUATION_HANDLER(handler, &ObServerAddrLookupHandler::handle_lookup_with_server_conn_id);
        addr_lookup_action_handle = &(handler->get_action());
        if (OB_ISNULL(ethread.schedule_imm(handler))) {
          query_info.errcode_ = OB_RESULT_UNKNOWN;
          ret = OB_ERR_UNEXPECTED;
          PROXY_SS_LOG(WARN, "schedule event error, event is null", K(ret));
          addr_lookup_action_handle = NULL;
        } else {
          PROXY_SS_LOG(INFO, "succ to schedule ObServerAddrLookupHandler with server conn_id", K(query_info));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (NULL != handler) {
      delete handler;
      handler = NULL;
    }
    if (NULL != mutex) {
      mutex->free();
      mutex = NULL;
    }
  }
  return ret;
}

int ObServerAddrLookupHandler::lookup_server_addr(const ObMysqlClientSession &cs,
    const ObProxySessionPrivInfo &priv_info, ObProxyKillQueryInfo &query_info)
{
  int ret = OB_SUCCESS;
  bool has_privilege = false;
  const ObProxySessionPrivInfo &target_priv_info = cs.get_session_info().get_priv_info();
  if (priv_info.has_all_privilege_) {
    has_privilege = true;
  } else if (priv_info.is_same_tenant(target_priv_info)) {
    if (priv_info.has_super_privilege() || priv_info.is_same_user(target_priv_info)) {
      has_privilege = true;
    } else {
      query_info.errcode_ = OB_ERR_KILL_DENIED;
      PROXY_SS_LOG(WARN, "same cluster.tenant, but different user, not the owner to others",
                   K(query_info.errcode_));
    }
  } else {
    query_info.errcode_ = OB_UNKNOWN_CONNECTION;
    PROXY_SS_LOG(WARN, "curr use has no privilege to kill query others",
            K(query_info.errcode_));
  }

  if (has_privilege) {
    ObMysqlServerSession *ss = cs.get_cur_server_session();
    if (NULL != ss && NULL != ss->get_client_session()
        && cs.get_cs_id() == ss->get_client_session()->get_cs_id()) {
      query_info.server_addr_.assign(cs.get_cur_server_session()->get_netvc()->get_remote_addr());
      query_info.real_conn_id_ = ss->get_server_sessid();
      query_info.errcode_ = OB_ENTRY_EXIST;
      PROXY_SS_LOG(DEBUG, "target used server session is existed");
    } else {
      query_info.errcode_ = OB_SUCCESS;
      PROXY_SS_LOG(DEBUG, "target used server session is not existed, response ok packet");
    }
  } //has_privilege
  return ret;
}


int ObServerAddrLookupHandler::lookup_server_addr_with_proxy_conn_id(const ObEThread &ethread,
    const ObProxySessionPrivInfo &priv_info, ObProxyKillQueryInfo &query_info)
{
  int ret = OB_SUCCESS;
  const uint32_t cs_id = static_cast<uint32_t>(query_info.cs_id_);
  ObMysqlClientSessionMap &cs_map = get_client_session_map(ethread);
  ObMysqlClientSession *cs = NULL;
  if (OB_SUCC(cs_map.get(cs_id, cs)) && OB_LIKELY(NULL != cs)) {
    //found the specific session
    MUTEX_TRY_LOCK(lock, cs->mutex_, this_ethread());
    if (OB_UNLIKELY(!lock.is_locked())) {
      ret = OB_NEED_RETRY;
      PROXY_SS_LOG(WARN, "fail to try lock cs in cs_map, need retry", K(cs_id), K(ethread.id_), K(ret));
    } else if (OB_FAIL(lookup_server_addr(*cs, priv_info, query_info))) {
      PROXY_SS_LOG(WARN, "fail to lookup_server_addr", K(cs->get_cs_id()), K(ret));
    } else {
      PROXY_SS_LOG(DEBUG, "succ to lookup_server_addr", K(cs->get_cs_id()));
    }
  } else {
    query_info.errcode_ = OB_UNKNOWN_CONNECTION; //not found the specific session
    PROXY_SS_LOG(DEBUG, "not found the specific session", K(query_info));
  }
  return ret;
}

int ObServerAddrLookupHandler::build_kill_query_sql(const uint32_t conn_id, common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.append_fmt("KILL QUERY %u", conn_id))) {
    PROXY_SS_LOG(WARN, "fail to build_kill_query_sql ", K(conn_id), K(ret));
  } else {
    PROXY_SS_LOG(DEBUG, "succ to build_kill_query_sql", K(sql), "length", sql.length());
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
