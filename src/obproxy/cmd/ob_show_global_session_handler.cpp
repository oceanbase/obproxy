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

#define USING_LOG_PREFIX PROXY_ICMD

#include "cmd/ob_show_global_session_handler.h"
#include "proxy/mysql/ob_mysql_sm.h"
#include "proxy/mysql/ob_mysql_global_session_manager.h"
#include "lib/lock/ob_drw_lock.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::opsql;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
enum
{
  OB_IC_GLOBAL_SESSION_NAME = 0,
  OB_IC_GLOBAL_POOL_MAX_COUNT,
  OB_IC_GLOBAL_POOL_MIN_COUNT,
  OB_IC_GLOBAL_POOL_LIVE_COUNT,
  OB_IC_GLOBAL_POOL_IDLE_TIMEOUT,
  OB_IC_GLOBAL_POOL_BLOCK_TIMEOUT,
  OB_IC_GLOBAL_POOL_PREFILL,
  OB_IC_GLOBAL_MAX_COLUMN_ID,
};

const ObProxyColumnSchema LIST_COLUMN_ARRAY[OB_IC_GLOBAL_MAX_COLUMN_ID] = {
  ObProxyColumnSchema::make_schema(OB_IC_GLOBAL_SESSION_NAME,     "dbkey_name",        obmysql::OB_MYSQL_TYPE_VARCHAR),
  ObProxyColumnSchema::make_schema(OB_IC_GLOBAL_POOL_MAX_COUNT,   "max_client_count",  obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_IC_GLOBAL_POOL_MIN_COUNT,  "min_client_count",   obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_IC_GLOBAL_POOL_LIVE_COUNT,  "live_client_count", obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_IC_GLOBAL_POOL_IDLE_TIMEOUT,  "idle_timeout",    obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_IC_GLOBAL_POOL_BLOCK_TIMEOUT,  "block_timeout",    obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_IC_GLOBAL_POOL_PREFILL,  "need_prefill",         obmysql::OB_MYSQL_TYPE_LONGLONG),

};

enum
{
  OB_IC_GLOBAL_SESSION_INFO_SESSION_NAME = 0,
  OB_IC_GLOBAL_SESSION_INFO_SERVER_IP,
  OB_IC_GLOBAL_SESSION_INFO_LOCAL_IP,
  OB_IC_GLOBAL_SESSION_INFO_AUTH_USER,
  OB_IC_GLOBAL_SESSION_INFO_TOTAL_COUNT,
  OB_IC_GLOBAL_SESSION_INFO_FREE_COUNT,
  OB_IC_GLOBAL_SESSION_INFO_SESSION_STATE,
  OB_IC_GLOBAL_SESSION_INFO_SESSION_ID,
  OB_IC_GLOBAL_SESSION_INFO_SESSION_CREATE_TIME,
  OB_IC_GLOBAL_SESSION_INFO_SESSION_LAST_RELEASE_TIME,
  OB_IC_GLOBAL_SESSION_INFO_MAX_COLUMN_ID,
};
const ObProxyColumnSchema LIST_INFO_COLUMN_ARRAY[OB_IC_GLOBAL_SESSION_INFO_MAX_COLUMN_ID] = {
  ObProxyColumnSchema::make_schema(OB_IC_GLOBAL_SESSION_INFO_SESSION_NAME,  "dbkey_name",     obmysql::OB_MYSQL_TYPE_VARCHAR),
  ObProxyColumnSchema::make_schema(OB_IC_GLOBAL_SESSION_INFO_SERVER_IP,     "server_ip",      obmysql::OB_MYSQL_TYPE_VARCHAR),
  ObProxyColumnSchema::make_schema(OB_IC_GLOBAL_SESSION_INFO_LOCAL_IP,      "local_ip",       obmysql::OB_MYSQL_TYPE_VARCHAR),
  ObProxyColumnSchema::make_schema(OB_IC_GLOBAL_SESSION_INFO_AUTH_USER,     "auth_user",      obmysql::OB_MYSQL_TYPE_VARCHAR),
  ObProxyColumnSchema::make_schema(OB_IC_GLOBAL_SESSION_INFO_TOTAL_COUNT,   "total_count",    obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_IC_GLOBAL_SESSION_INFO_FREE_COUNT,    "free_count",     obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_IC_GLOBAL_SESSION_INFO_SESSION_STATE, "session_state",  obmysql::OB_MYSQL_TYPE_VARCHAR),
  ObProxyColumnSchema::make_schema(OB_IC_GLOBAL_SESSION_INFO_SESSION_ID,    "session_id",     obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_IC_GLOBAL_SESSION_INFO_SESSION_CREATE_TIME, "create_time",  obmysql::OB_MYSQL_TYPE_VARCHAR),
  ObProxyColumnSchema::make_schema(OB_IC_GLOBAL_SESSION_INFO_SESSION_LAST_RELEASE_TIME, "last_release_time",  obmysql::OB_MYSQL_TYPE_VARCHAR),
};

ObShowGlobalSessionHandler::ObShowGlobalSessionHandler(event::ObContinuation *cont, event::ObMIOBuffer *buf,
    const ObInternalCmdInfo &info)
  : ObInternalCmdHandler(cont, buf, info), sub_type_(info.get_sub_cmd_type()), cmd_info_(&info)
{
  SET_HANDLER(&ObShowGlobalSessionHandler::handle_show_global_session_info);
}
int ObShowGlobalSessionHandler::handle_show_global_session_info(int event, void *data)
{
  int event_ret = EVENT_DONE;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("invalid argument, it should not happen", K(event), K(data), K_(is_inited), K(ret));
  } else if (OB_FAIL(dump_header())) {
    WDIAG_ICMD("fail to dump_header", K(ret));
  } else if (OB_FAIL(dump_body())) {
    WDIAG_ICMD("fail to dump_body", K(ret));
  } else if (OB_FAIL(encode_eof_packet())) {
    WDIAG_ICMD("fail to encode eof packet", K(ret));
  } else {
    INFO_ICMD("succ to dump config", K_(like_name));
    event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
  }
  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  }
  return event_ret;
}
int ObShowGlobalSessionHandler::dump_session_header()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(encode_header(LIST_COLUMN_ARRAY, OB_IC_GLOBAL_MAX_COLUMN_ID))) {
    WDIAG_ICMD("fail to dump_session_header", K(ret));
  }
  return ret;
}
int ObShowGlobalSessionHandler::dump_session_body() {
  int ret = OB_SUCCESS;
  // here lock to avoid other remove
  DRWLock::RDLockGuard guard(get_global_session_manager().rwlock_);
  ObMysqlGlobalSessionManager::SessionPoolListHashTable& session_pool_list_table = get_global_session_manager().global_session_pool_;
  ObMysqlGlobalSessionManager::SessionPoolListHashTable::iterator spot = session_pool_list_table.begin();
  ObMysqlGlobalSessionManager::SessionPoolListHashTable::iterator last = session_pool_list_table.end();
  for (int64_t i = 0; OB_SUCC(ret) && spot != last; ++spot, ++i) {
    if (like_name_.empty() || common::match_like(spot->schema_key_.dbkey_.config_string_, like_name_)) {
      ObNewRow row;
      ObObj cells[OB_IC_GLOBAL_MAX_COLUMN_ID];
      row.cells_ = cells;
      row.count_ = OB_IC_GLOBAL_MAX_COLUMN_ID;
      cells[OB_IC_GLOBAL_SESSION_NAME].set_varchar(spot->schema_key_.dbkey_.config_string_);
      cells[OB_IC_GLOBAL_POOL_MAX_COUNT].set_int(ObMysqlSessionUtils::get_session_max_conn(spot->schema_key_));
      cells[OB_IC_GLOBAL_POOL_MIN_COUNT].set_int(ObMysqlSessionUtils::get_session_min_conn(spot->schema_key_));
      cells[OB_IC_GLOBAL_POOL_IDLE_TIMEOUT].set_int(ObMysqlSessionUtils::get_session_idle_timeout_ms(spot->schema_key_));
      cells[OB_IC_GLOBAL_POOL_BLOCK_TIMEOUT].set_int(ObMysqlSessionUtils::get_session_blocking_timeout_ms(spot->schema_key_));
      cells[OB_IC_GLOBAL_POOL_PREFILL].set_int(ObMysqlSessionUtils::get_session_prefill(spot->schema_key_));
      cells[OB_IC_GLOBAL_POOL_LIVE_COUNT].set_int(spot->client_session_count_);
      if (OB_FAIL(encode_row_packet(row))) {
        WDIAG_ICMD("fail to encode row packet", K(row), K(ret));
      }
    }
  }
  return ret;
}

int ObShowGlobalSessionHandler::dump_session_info_header()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(encode_header(LIST_INFO_COLUMN_ARRAY, OB_IC_GLOBAL_SESSION_INFO_MAX_COLUMN_ID))) {
    WDIAG_ICMD("fail to dump_session_info_header header", K(ret));
  }
  return ret;
}

int ObShowGlobalSessionHandler::dump_session_info_body(const common::ObString& dbkey)
{
  int ret = OB_SUCCESS;
  if (dbkey.empty()) {
    WDIAG_ICMD("invalid: dbkey is empty");
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObMysqlServerSessionListPool* session_list_pool = get_global_session_manager().get_server_session_list_pool(dbkey);
    if (OB_ISNULL(session_list_pool)) {
      WDIAG_ICMD("invalid: session_list_pool is null", K(dbkey));
      ret = OB_INVALID_ARGUMENT;
    } else {
      DRWLock::RDLockGuard  guard(session_list_pool->rwlock_);
      ObMysqlServerSessionListPool::IPHashTable& session_pool = session_list_pool->server_session_list_pool_;
      ObMysqlServerSessionListPool::IPHashTable::iterator spot = session_pool.begin();
      ObMysqlServerSessionListPool::IPHashTable::iterator last = session_pool.end();
      for (int64_t i = 0; OB_SUCC(ret) && spot != last; ++spot, ++i) {
        ObMysqlServerSessionList::LocalIPHashTable& local_ip_pool = spot->local_ip_pool_;
        ObMysqlServerSessionList::LocalIPHashTable::iterator local_spot = local_ip_pool.begin();
        ObMysqlServerSessionList::LocalIPHashTable::iterator local_last = local_ip_pool.end();
        for (int64_t j = 0; OB_SUCC(ret) && local_spot != local_last; ++local_spot, ++j) {
          ObNewRow row;
          ObObj cells[OB_IC_GLOBAL_SESSION_INFO_MAX_COLUMN_ID];
          row.cells_ = cells;
          row.count_ = OB_IC_GLOBAL_SESSION_INFO_MAX_COLUMN_ID;
          net::ObIpEndpoint& server_ip = spot->server_ip_;
          oceanbase::obproxy::obutils::ObProxyConfigString& auth_user = spot->auth_user_;
          const int TMP_BUF_SIZE = 128;
          char ip_buf[TMP_BUF_SIZE];
          char local_ip_buf[TMP_BUF_SIZE];
          server_ip.to_string(ip_buf, TMP_BUF_SIZE);
          cells[OB_IC_GLOBAL_SESSION_INFO_SESSION_NAME].set_varchar(local_spot->schema_key_.dbkey_.config_string_);
          cells[OB_IC_GLOBAL_SESSION_INFO_SERVER_IP].set_varchar(ip_buf);
          cells[OB_IC_GLOBAL_SESSION_INFO_AUTH_USER].set_varchar(auth_user.config_string_);
          cells[OB_IC_GLOBAL_SESSION_INFO_TOTAL_COUNT].set_int(spot->total_count_);
          cells[OB_IC_GLOBAL_SESSION_INFO_FREE_COUNT].set_int(spot->free_count_);
          cells[OB_IC_GLOBAL_SESSION_INFO_SESSION_STATE].set_varchar(ObString::make_string(local_spot->get_state_str()));
          cells[OB_IC_GLOBAL_SESSION_INFO_SESSION_ID].set_int(local_spot->ss_id_);
          net::ObNetVConnection* net_vc = local_spot->get_netvc();
          ObIpEndpoint local_addr;
          if (net_vc == NULL) {
            snprintf(local_ip_buf, TMP_BUF_SIZE, "null");
          } else {
            local_addr.assign(net_vc->get_local_addr());
            local_addr.to_string(local_ip_buf, TMP_BUF_SIZE);
          }
          cells[OB_IC_GLOBAL_SESSION_INFO_LOCAL_IP].set_varchar(local_ip_buf);
          char create_time_buf[TMP_BUF_SIZE];
          char last_release_time_buf[TMP_BUF_SIZE];
          int64_t pos = 0;
          ObTimeUtility::usec_to_str(local_spot->create_time_, create_time_buf, TMP_BUF_SIZE, pos);
          cells[OB_IC_GLOBAL_SESSION_INFO_SESSION_CREATE_TIME].set_varchar(create_time_buf);
          pos = 0;
          if (local_spot->state_ == MSS_KA_SHARED) {
            ObTimeUtility::usec_to_str(local_spot->last_active_time_, last_release_time_buf, TMP_BUF_SIZE, pos);
          } else {
            snprintf(last_release_time_buf, TMP_BUF_SIZE, "null");
          }
          cells[OB_IC_GLOBAL_SESSION_INFO_SESSION_LAST_RELEASE_TIME].set_varchar(last_release_time_buf);
          if (OB_FAIL(encode_row_packet(row))) {
            WDIAG_ICMD("fail to encode row packet", K(row), K(ret));
          }
        }
      }
      session_list_pool->dec_ref();
      session_list_pool = NULL;
    }
  }
  return ret;
}
int ObShowGlobalSessionHandler::dump_session_info_body_all()
{
  int ret = OB_SUCCESS;
  DRWLock::RDLockGuard guard(get_global_session_manager().rwlock_);
  ObMysqlGlobalSessionManager::SessionPoolListHashTable& session_pool_list_table = get_global_session_manager().global_session_pool_;
  ObMysqlGlobalSessionManager::SessionPoolListHashTable::iterator spot = session_pool_list_table.begin();
  ObMysqlGlobalSessionManager::SessionPoolListHashTable::iterator last = session_pool_list_table.end();
  for (int64_t i = 0; OB_SUCC(ret) && spot != last; ++spot, ++i) {
    ret = dump_session_info_body(spot->schema_key_.dbkey_.config_string_);
  }
  return ret;
}

int ObShowGlobalSessionHandler::dump_header()
{
  int ret = OB_SUCCESS;
  if (sub_type_ == OBPROXY_T_SUB_GLOBAL_SESSION_LIST ||
    sub_type_ == OBPROXY_T_SUB_INVALID) {
    ret = dump_session_header();
  } else if (sub_type_ == OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO ||
    sub_type_ == OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO_ALL ||
    sub_type_ == OBPROXY_T_SUB_GLOBAL_SESSION_LIST_LIKE) {
    ret = dump_session_info_header();
  }
  return ret;
}
int ObShowGlobalSessionHandler::dump_body()
{
  int ret = OB_SUCCESS;
  if (sub_type_ == OBPROXY_T_SUB_GLOBAL_SESSION_LIST) {
    ret = dump_session_body();
  } else if (sub_type_ == OBPROXY_T_SUB_INVALID) {
    DEBUG_ICMD("show global session like", K(like_name_));
    ret = dump_session_body();
  } else if (sub_type_ == OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO) {
    const ObString& dbkey = cmd_info_->get_key_string();
    ret = dump_session_info_body(dbkey);
  } else if (sub_type_ == OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO_ALL) {
    ret = dump_session_info_body_all();
  } else if (sub_type_ == OBPROXY_T_SUB_GLOBAL_SESSION_LIST_LIKE) {
    DEBUG_ICMD("show global session_list like", K(like_name_));
    ObMysqlGlobalSessionManager::SessionPoolListHashTable& session_pool_list_table = get_global_session_manager().global_session_pool_;
    ObMysqlGlobalSessionManager::SessionPoolListHashTable::iterator spot = session_pool_list_table.begin();
    ObMysqlGlobalSessionManager::SessionPoolListHashTable::iterator last = session_pool_list_table.end();
    for (int64_t i = 0; OB_SUCC(ret) && spot != last; ++spot, ++i) {
      if (common::match_like(spot->schema_key_.dbkey_.config_string_, like_name_)) {
        ret = dump_session_info_body(spot->schema_key_.dbkey_.config_string_);
      }
    }
  }
  return ret;
}

static int show_global_session_info_cmd_callback(ObContinuation * cont, ObInternalCmdInfo & info,
    ObMIOBuffer * buf, ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;
  ObShowGlobalSessionHandler *handler = NULL;
  if (OB_UNLIKELY(!ObInternalCmdHandler::is_constructor_argument_valid(cont, buf))) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("constructor argument is invalid", K(cont), K(buf), K(ret));
  } else if (OB_ISNULL(handler = new(std::nothrow) ObShowGlobalSessionHandler(cont, buf, info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    EDIAG_ICMD("fail to new ObShowGlobalSessionHandler", K(ret));
  } else if (OB_FAIL(handler->init())) {
    WDIAG_ICMD("fail to init for ObShowGlobalSessionHandler");
  } else {
    action = &handler->get_action();
    if (OB_ISNULL(self_ethread().schedule_imm(handler, ET_TASK))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      EDIAG_ICMD("fail to schedule handler", K(ret));
      action = NULL;
    } else {
      DEBUG_ICMD("succ to schedule handler");
    }
  }

  if (OB_FAIL(ret) && OB_LIKELY(NULL != handler)) {
    delete handler;
    handler = NULL;
  }
  return ret;
}
int show_global_session_info_cmd_init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_global_internal_cmd_processor().register_cmd(OBPROXY_T_ICMD_SHOW_GLOBAL_SESSION,
              &show_global_session_info_cmd_callback))) {
    WDIAG_ICMD("fail to register OBPROXY_T_ICMD_SHOW_GLOBAL_SESSION CMD", K(ret));
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
