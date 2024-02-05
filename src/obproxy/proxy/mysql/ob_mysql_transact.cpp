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

#define USING_LOG_PREFIX PROXY_TXN
#include "proxy/mysql/ob_mysql_transact.h"
#include "packet/ob_mysql_packet_reader.h"
#include "stat/ob_processor_stats.h"
#include "stat/ob_congestion_stats.h"
#include "opsql/dual_parser/ob_dual_parser.h"
#include "obutils/ob_resource_pool_processor.h"
#include "obutils/ob_task_flow_controller.h"
#include "obutils/ob_proxy_sequence_entry_cont.h"
#include "obutils/ob_proxy_table_processor_utils.h"
#include "utils/ob_proxy_utils.h"
#include "obutils/ob_proxy_config.h"
#include "utils/ob_proxy_utils.h"
#include "proxy/mysqllib/ob_proxy_session_info_handler.h"
#include "proxy/mysqllib/ob_mysql_request_builder.h"
#include "proxy/mysqllib/ob_mysql_analyzer_utils.h"
#include "proxy/mysql/ob_mysql_global_session_manager.h"
#include "proxy/mysqllib/ob_2_0_protocol_utils.h"
#include "proxy/mysql/ob_mysql_sm.h"
#include "proxy/route/ob_route_struct.h"
#include "proxy/route/ob_sql_table_cache.h"
#include "prometheus/ob_sql_prometheus.h"
#include "rpc/obmysql/packet/ompk_prepare.h"
#include "proxy/mysql/ob_cursor_struct.h"
#include "omt/ob_ssl_config_table_processor.h"
#include "dbconfig/ob_proxy_db_config_info.h"
#include "lib/encrypt/ob_encrypted_helper.h"
#include "proxy/shard/obproxy_shard_utils.h"
#include "rpc/obmysql/packet/ompk_change_user.h"
#include "rpc/obmysql/packet/ompk_prepare_execute.h"
#include "omt/ob_proxy_config_table_processor.h"
#include "obutils/ob_config_server_processor.h"
#include "lib/utility/ob_2_0_sess_veri.h"
#include "obutils/ob_read_stale_processor.h"
#include "proxy/mysqllib/ob_mysql_response_builder.h"
#include "obutils/ob_connection_diagnosis_trace.h"
#include "lib/utility/ob_tracepoint.h"
#include "proxy/mysqllib/ob_mysql_packet_rewriter.h"

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::packet;
using namespace oceanbase::sql;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::prometheus;
using namespace oceanbase::obproxy::dbconfig;
using namespace oceanbase::obproxy::omt;
namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

#define MYSQL_INCREMENT_TRANS_STAT(X) update_stat(s, X, 1);
#define MYSQL_SUM_TIME_STAT(X, cost) update_stat(s, X, cost);

bool ObMysqlTransact::is_in_trans(ObTransState &s)
{
  // if a trans is commit, the state will be set to TRANSACTION_COMPLETE,
  // so if current state is CMD_COMPLETE, it means that we have send a sql successfully and
  // the trans has not commit, that is to say "in trans"
  // so far, there are three cases NOT in trans:
  // 1. handshake response (login packet) need pl lookup
  // 2. the first sql of one transaction
  // 3. the second sql of one transaction, if the first sql is 'begin' or 'start transaction'

  // s.current_.state_ may changed to CONNECTION_ALIVE on state_server_response_read,
  // here add last_request_in_trans to correct transaction state

  bool is_in_trans = false;
  if (!s.is_auth_request_ && !s.is_hold_start_trans_ && !s.is_hold_xa_start_) {
    if (ObMysqlTransact::CMD_COMPLETE == s.current_.state_) {
      is_in_trans = true;
    } else if (ObMysqlTransact::TRANSACTION_COMPLETE != s.current_.state_
               && s.sm_->client_session_ != NULL
               && s.sm_->client_session_->is_last_request_in_trans()) {
      is_in_trans = true;
    }
  }
  return is_in_trans;
}

void ObMysqlTransact::record_trans_state(ObTransState &s, bool is_in_trans)
{
  ObMysqlClientSession *client_session = s.sm_->get_client_session();
  bool last_request_in_trans = client_session->is_last_request_in_trans();
  if (client_session->is_proxy_enable_trans_internal_routing()) {
    // set distributed transaction route flag
    bool server_trans_internal_routing = s.trans_info_.server_response_.get_analyze_result().is_server_trans_internal_routing();
    bool is_trans_internal_routing = ObMysqlTransact::handle_set_trans_internal_routing(s, server_trans_internal_routing);

    if (!last_request_in_trans && is_in_trans) {
      client_session->set_trans_coordinator_ss_addr(s.server_info_.addr_.sa_);
      LOG_DEBUG("start internal routing transaction", "coordinator addr", client_session->get_trans_coordinator_ss_addr());
      s.trace_log_.set_need_print(is_trans_internal_routing);
      s.trace_log_.log_it("[trans_start]",
                          "proxy_sessid", static_cast<int64_t>(client_session->get_proxy_sessid()),
                          "coordinator", s.server_info_.addr_,
                          "sql_cmd", static_cast<int64_t>(s.trans_info_.sql_cmd_),
                          "stmt_type", static_cast<int64_t>(s.trans_info_.client_request_.get_parse_result().get_stmt_type()));
    } else if (last_request_in_trans && !is_in_trans) {
      // close txn, refresh enable_transaction_internal_routing_
      LOG_DEBUG("internal routing transaction close", "coordinator addr", client_session->get_trans_coordinator_ss_addr());
      client_session->get_trans_coordinator_ss_addr().reset();
    }

    client_session->set_trans_internal_routing(is_trans_internal_routing);
    LOG_DEBUG("set transaction internal routing flag", "internal routing state", is_trans_internal_routing);
  }
  client_session->set_last_request_in_trans(is_in_trans);
}

void ObMysqlTransact::handle_error_jump(ObTransState &s)
{
  LOG_WDIAG("[ObMysqlTransact::handle_error_jump]");

  TRANSACT_RETURN(SM_ACTION_SEND_ERROR_NOOP, NULL);
}

// ObMysqlTransact State Machine Handlers
//
// What follow from here on are the state machine handlers - the code
// which is called from MysqlSM::set_next_state to specify
// what action the state machine needs to execute next. These ftns
// take as input just the state and set the next_action variable.
void ObMysqlTransact::bad_request(ObTransState &s)
{
  LOG_INFO("[ObMysqlTransact::bad_request] parser marked request bad");
  // no error message send, just close client session and bound server session
  TRANSACT_RETURN_WITH_MSG(SM_ACTION_SEND_ERROR_NOOP, NULL);
}

void ObMysqlTransact::update_sql_cmd(ObTransState &s)
{
  switch (s.current_.send_action_) {
    case SERVER_SEND_HANDSHAKE:
      s.trans_info_.sql_cmd_ = obmysql::OB_MYSQL_COM_HANDSHAKE;
      break;

    case SERVER_SEND_SSL_REQUEST:
      s.trans_info_.sql_cmd_ = obmysql::OB_MYSQL_COM_LOGIN;
      break;

    case SERVER_SEND_LOGIN:
      s.trans_info_.sql_cmd_ = obmysql::OB_MYSQL_COM_LOGIN;
      break;

    case SERVER_SEND_SAVED_LOGIN:
      s.trans_info_.sql_cmd_ = obmysql::OB_MYSQL_COM_LOGIN;
      break;

    case SERVER_SEND_USE_DATABASE:
      s.trans_info_.sql_cmd_ = obmysql::OB_MYSQL_COM_INIT_DB;
      break;

    case SERVER_SEND_ALL_SESSION_VARS:
    case SERVER_SEND_SESSION_VARS:
    case SERVER_SEND_SESSION_USER_VARS:
    case SERVER_SEND_START_TRANS:
    case SERVER_SEND_TEXT_PS_PREPARE:
    case SERVER_SEND_INIT_SQL:
      s.trans_info_.sql_cmd_ = obmysql::OB_MYSQL_COM_QUERY;
      break;

    case SERVER_SEND_XA_START:
      s.trans_info_.sql_cmd_ = obmysql::OB_MYSQL_COM_STMT_PREPARE_EXECUTE;
      break;

    case SERVER_SEND_PREPARE:
      s.trans_info_.sql_cmd_ = obmysql::OB_MYSQL_COM_STMT_PREPARE;
      break;

    case SERVER_SEND_REQUEST:
      if (OB_UNLIKELY(obmysql::OB_MYSQL_COM_AUTH_SWITCH_RESP == s.trans_info_.sql_cmd_ ||
                      obmysql::OB_MYSQL_COM_LOAD_DATA_TRANSFER_CONTENT == s.trans_info_.sql_cmd_)) {
        /* not update */
      } else if (OB_UNLIKELY(s.is_auth_request_)) {
        s.trans_info_.sql_cmd_ = obmysql::OB_MYSQL_COM_LOGIN;
      } else {
        s.trans_info_.sql_cmd_ = s.trans_info_.client_request_.get_packet_meta().cmd_;
      }
      break;

    case SERVER_SEND_NONE:
    default:
      PROXY_TXN_LOG(EDIAG, "Unknown server send next action", K(s.current_.send_action_));
      break;
  }

  // reuse the protocol diagnosis request analzyer to prepare to record request packets
  if (s.sm_->protocol_diagnosis_ != NULL) {
    s.sm_->protocol_diagnosis_->reuse_req_analyzer();
    PROXY_TXN_LOG(DEBUG, "[ObMysqlTransact::update_sql_cmd] reuse protocol diagnosis request analyzer");
  }

  PROXY_TXN_LOG(DEBUG, "[ObMysqlTransact::update_sql_cmd]",
                "send_action", ObMysqlTransact::get_send_action_name(s.current_.send_action_),
                "sql_cmd", ObProxyParserUtils::get_sql_cmd_name(s.trans_info_.sql_cmd_));
}

inline bool ObMysqlTransact::is_session_memory_overflow(ObTransState &s)
{
  bool bret = false;
  if (NULL != s.sm_->client_session_
      && s.sm_->client_session_->get_session_info().get_memory_size()
         > s.mysql_config_params_->client_max_memory_size_) {
    bret = true;
  }
  return bret;
}

bool ObMysqlTransact::ObTransState::is_for_update_sql(common::ObString src_sql)
{
  bool bret = false;
  const char FOR_STRING_BUF[] = "for";
  const ObString FOR_STRING(FOR_STRING_BUF);
  const ObString UPDATE_STRING("update");
  //' for update'
  if (src_sql.length() > (FOR_STRING.length() + UPDATE_STRING.length() + 2)
      && '\0' == src_sql[src_sql.length()]) {
    char *ptr = src_sql.ptr();
    char *last_pos  = NULL;
    char *pos = ptr;
    const char *end = src_sql.ptr() + src_sql.length();
    while (!bret && NULL != (pos = strcasestr(pos, FOR_STRING_BUF))) {
      last_pos = pos;
      pos += 3;

      if (NULL != last_pos
          && last_pos > ptr
          && IS_SPACE(*(last_pos-1))
          && IS_SPACE(*(last_pos+3))) {
        last_pos = last_pos + 3;
        while (last_pos < end && IS_SPACE(*last_pos)) {
          last_pos++;
        }
        if (0 == strncasecmp(last_pos, UPDATE_STRING.ptr(), UPDATE_STRING.length())
            && ('\0' == last_pos[UPDATE_STRING.length()] || ';' == last_pos[UPDATE_STRING.length()])) {
          bret = true;
        }
      }
    }
  }
  return bret;
}

ObConsistencyLevel ObMysqlTransact::ObTransState::get_read_write_consistency_level(ObClientSessionInfo &session_info)
{
  ObConsistencyLevel ret_level = common::STRONG;
  bool enable_weak_read = false;
  if (ObMysqlTransact::is_in_trans(sm_->trans_state_)
      || 0 == session_info.get_cached_variables().get_autocommit()
      || is_hold_start_trans_
      || is_hold_xa_start_) {
    if (ObMysqlTransact::is_in_trans(sm_->trans_state_)
      || is_hold_start_trans_
      || is_hold_xa_start_) {
      enable_weak_read = false;
    } else {
      enable_weak_read = sm_->enable_read_write_split_ && sm_->enable_transaction_split_;
    }
  } else {
    enable_weak_read = sm_->enable_read_write_split_;
  }

  if (enable_weak_read) {
    ret_level = common::WEAK;
  }
  return ret_level;
}

ObConsistencyLevel ObMysqlTransact::ObTransState::get_trans_consistency_level(
    ObClientSessionInfo &cs_info)
{
  /*
   * when chose read_consistency, we use the follower rules:
   * 1. if it is inner connection or non select_read_only_stmt, use strong
   * 2. else get result by sql_hint and sys_var, like the followers
   *
   *     sql_hint       sys_var     result
   *     ---------------------------------
   *     strong         *           strong
   *     weak           *           weak
   *     NULL/others    strong      strong
   *     NULL/others    weak        weak
   *     NULL/others    other       strong
   *
   * */
  ObConsistencyLevel ret_level = common::STRONG;
  if (INVALID_CONSISTENCY != pll_info_.route_.consistency_level_) {
    ret_level = pll_info_.route_.consistency_level_;
  } else {
    if (trans_info_.client_request_.get_parse_result().is_select_stmt() ||
      trans_info_.client_request_.get_parse_result().is_text_ps_select_stmt()) {
      const ObConsistencyLevel sql_hint = trans_info_.client_request_.get_parse_result().get_hint_consistency_level();
      const ObConsistencyLevel sys_var = static_cast<ObConsistencyLevel>(cs_info.get_read_consistency());
      const ObConsistencyLevel read_write_consistence_level = get_read_write_consistency_level(cs_info);
      if (common::STRONG == sql_hint || common::WEAK == sql_hint) {
        ret_level = sql_hint;
      } else {
        if (common::WEAK == sys_var) {
          ret_level = sys_var;
        } else {
          if (common::STRONG == read_write_consistence_level || common::WEAK == read_write_consistence_level) {
            ret_level = read_write_consistence_level;
          }
          if (common::STRONG != sys_var) {
            PROXY_LOG(DEBUG, "unsupport ob_read_consistency vars, maybe proxy is old, use strong read "
                    "instead", "sys_var", get_consistency_level_str(sys_var));
          }
        }
      }
      if (common::WEAK == ret_level) {
        ObString sql;
        if (obmysql::OB_MYSQL_COM_STMT_EXECUTE == trans_info_.client_request_.get_packet_meta().cmd_) {
          cs_info.get_ps_sql(sql);
        } else {
          sql = trans_info_.client_request_.get_sql();
        }
        if (OB_UNLIKELY(!cs_info.is_server_support_read_weak())) {
          ret_level = common::STRONG;
          PROXY_LOG(DEBUG, "ObServer do not support read weak, treat it as strong read",
              "sql_hint", get_consistency_level_str(sql_hint),
              "sys_var", get_consistency_level_str(sys_var),
              "ret_level", get_consistency_level_str(ret_level),
              "sql", trans_info_.client_request_.get_sql());
        } else if (OB_UNLIKELY(is_for_update_sql(sql))) {
          ret_level = common::STRONG;
          PROXY_LOG(DEBUG, "For select .. for update sql, treat it as strong read",
              "sql_hint", get_consistency_level_str(sql_hint),
              "sys_var", get_consistency_level_str(sys_var),
              "ret_level", get_consistency_level_str(ret_level),
              "sql", sql);
        } else {
          PROXY_LOG(DEBUG, "current use weak read",
              "sql_hint", get_consistency_level_str(sql_hint),
              "sys_var", get_consistency_level_str(sys_var),
              "ret_level", get_consistency_level_str(ret_level),
              "sql", sql);
        }
      }
    }
    pll_info_.route_.consistency_level_ = ret_level;
  }
  return ret_level;
}

bool ObMysqlTransact::ObTransState::is_request_readonly_zone_support(ObClientSessionInfo &cs_info)
{
  bool bret = false;
  const ObProxyBasicStmtType type = trans_info_.client_request_.get_parse_result().get_stmt_type();
  //the follower request is readonly zone avail
  //1. weak read select
  //2. set stmt
  //3. select stmt without table name
  if (obmysql::OB_MYSQL_COM_QUERY == trans_info_.sql_cmd_) {
    switch (type) {
      case OBPROXY_T_SELECT: {
        if (common::WEAK == get_trans_consistency_level(cs_info)
            || trans_info_.client_request_.get_parse_result().get_table_name().empty()) {
          bret = true;
        }
        break;
      }
      case OBPROXY_T_SET:
      case OBPROXY_T_SET_NAMES:
      case OBPROXY_T_USE_DB:
      case OBPROXY_T_HELP:
      case OBPROXY_T_SHOW:
      case OBPROXY_T_SHOW_WARNINGS:
      case OBPROXY_T_SHOW_ERRORS:
      case OBPROXY_T_SHOW_TRACE: {
        bret = true;
        break;
      }
      default: {
        break;
      }
    }
  } else {
    bret = true;
  }
  return bret;
}

void ObMysqlTransact::modify_request(ObTransState &s)
{
  if (OB_ISNULL(s.sm_->sm_cluster_resource_)
      && OB_UNLIKELY(s.sm_->client_session_->get_session_info().is_oceanbase_server()
      && !s.sm_->client_session_->is_proxysys_tenant()
      && (s.mysql_config_params_->is_mysql_routing_mode()
          || (OB_MYSQL_COM_HANDSHAKE != s.trans_info_.sql_cmd_)))) {
    LOG_WDIAG("[modify_request] cluster resource is NULL, will disconnect");
    TRANSACT_RETURN_WITH_MSG(SM_ACTION_SEND_ERROR_NOOP, NULL);
  } else {
    TRANSACT_RETURN(SM_ACTION_API_READ_REQUEST, ObMysqlTransact::handle_request);
    // do not skip plugin when not use tunnel
    if (!s.trans_info_.client_request_.is_large_request() &&
        !ObMysqlTransact::need_use_tunnel(s)) {
      s.sm_->set_skip_plugin(true);
    }
  }
}

bool ObMysqlTransact::need_use_dup_replica(const ObConsistencyLevel level, ObTransState &s)
{
  return (STRONG == level)
          && (s.trans_info_.client_request_.get_parse_result().is_select_stmt())
          && (!ObTransState::is_for_update_sql(s.trans_info_.client_request_.get_sql()))
          && (s.pll_info_.route_.has_dup_replica_);
}

bool ObMysqlTransact::need_disable_merge_status_check(ObTransState &s)
{
  bool bret = false;
  if (OB_NOT_NULL(s.sm_) && OB_NOT_NULL(s.sm_->client_session_)) {
    if (!s.sm_->client_session_->is_first_dml_sql_got()
        && s.trans_info_.client_request_.is_real_dml_sql()) {
      s.sm_->client_session_->set_first_dml_sql_got();
      LOG_INFO("user first dml got", "cs_id", s.sm_->client_session_->get_cs_id(),
               "sql", s.trans_info_.client_request_.get_print_sql());
    }

    //only readonly deploy can disable merge status check
    if (s.sm_->client_session_->dummy_ldc_.is_readonly_zone_exist()) {
      bret = !s.sm_->client_session_->is_first_dml_sql_got();
    }
  }
  return bret;
}

void ObMysqlTransact::acquire_cached_server_session(ObTransState &s)
{
  ObMysqlServerSession *last_session = s.sm_->client_session_->get_server_session();
  ObMysqlServerSession *lii_session = s.sm_->client_session_->get_lii_server_session();
  ObMysqlServerSession *selected_session = NULL;
  ObSqlParseResult &sql_result = s.trans_info_.client_request_.get_parse_result();
  //some optimization strategy
  //1.if sql result have last_insert_id, we try to use last_insert_id server session
  //sharding not support laster insert id
  if (!s.sm_->client_session_->get_session_info().is_sharding_user()
      && NULL != lii_session && sql_result.has_last_insert_id()) {
    selected_session = lii_session;
    LOG_DEBUG("[ObMysqlTransact::acquire_cached_server_session] last_insert_id_session is alive, pick it");

  } else if (get_global_proxy_config().enable_cached_server
             && !s.sm_->client_session_->is_proxy_mysql_client_
             && NULL != last_session && OB_LIKELY(!s.mysql_config_params_->is_random_routing_mode())) {
    ObAddr last_addr;
    last_addr.set_sockaddr(last_session->get_netvc()->get_remote_addr());
#if OB_DETAILED_SLOW_QUERY
    ObHRTime t1 = common::get_hrtime_internal();
#endif
    if (OB_LIKELY(OB_SUCCESS == s.sm_->client_session_->check_update_ldc())) {
#if OB_DETAILED_SLOW_QUERY
      ObHRTime t2 = common::get_hrtime_internal();
      s.sm_->cmd_time_stats_.debug_assign_time_ += (t2 - t1);
#endif

      const bool disable_merge_status_check = need_disable_merge_status_check(s);
      ObZoneType except_zone_type = ZONE_TYPE_INVALID;
      if (s.sm_->client_session_->dummy_ldc_.is_readonly_zone_exist()) {
        ObClientSessionInfo &cs_info = s.sm_->client_session_->get_session_info();
        if (common::WEAK == s.get_trans_consistency_level(cs_info)) {
          except_zone_type = ZONE_TYPE_READONLY;
        } else if (s.is_request_readonly_zone_support(cs_info)) {
          if (cs_info.is_read_consistency_set()) {
            if (common::WEAK == static_cast<ObConsistencyLevel>(cs_info.get_read_consistency())) {
              except_zone_type = ZONE_TYPE_READONLY;
            } else {//strong
              except_zone_type = ZONE_TYPE_READWRITE;
            }
          } else {
            except_zone_type = ZONE_TYPE_INVALID;
          }
        } else {
          except_zone_type = ZONE_TYPE_READWRITE;
        }
      }

      if (s.sm_->client_session_->dummy_ldc_.is_in_same_region_unmerging(
          s.pll_info_.route_.cur_chosen_route_type_,
          s.pll_info_.route_.cur_chosen_server_.zone_type_,
          last_addr,
          except_zone_type,
          disable_merge_status_check)) {
        selected_session = last_session;
        ObProxyMutex *mutex_ = s.sm_->mutex_; // for stat
        PROCESSOR_INCREMENT_DYN_STAT(GET_PL_BY_LAST_SESSION_SUCC);
        LOG_DEBUG("[ObMysqlTransact::acquire_cached_server_session] last server session is alive, pick it");
      } else {
        LOG_DEBUG("[ObMysqlTransact::acquire_cached_server_session] last server session is not in same "
                  "region unmerging, do not use", K(last_addr), "route", s.pll_info_.route_);
      }
    }
  }

  if (NULL != selected_session) {
    s.server_info_.set_addr(selected_session->get_netvc()->get_remote_addr());
    s.pll_info_.lookup_success_ = true;
    MYSQL_INCREMENT_TRANS_STAT(CLIENT_MISSING_PK_REQUESTS);
  }
}

bool ObMysqlTransact::is_dbmesh_pool_user(ObTransState &s)
{
  if (s.sm_->client_session_->get_session_info().is_sharding_user() &&
        s.sm_->client_session_->is_session_pool_client()) {
    return true;
  }
  return false;
}

bool ObMysqlTransact::can_direct_ok_for_login(ObTransState &s)
{
  bool bret = false;
  ObClientSessionInfo &cs_info = s.sm_->client_session_->get_session_info();
  if (!(obmysql::OB_MYSQL_COM_LOGIN == s.trans_info_.sql_cmd_ && s.is_auth_request_)) {
    // should only LOGIN for auth
  } else if (s.sm_->client_session_->can_direct_send_request_) {
    //scan all 直接回ok
    bret = true;
  } else if (!s.sm_->client_session_->is_session_pool_client()) {
    // should only session_pool_client
  } else if (cs_info.is_sharding_user()) {
    //V2 sharding_user 直接回ok
    bret = true;
  } else if (!s.sm_->client_session_->is_proxy_mysql_client_ && get_global_proxy_config().enable_no_sharding_skip_real_conn) {
    //v1Login时连接池有保存密码并且密码一致，直接回ok, 密码不一致还是走server认证判断
    char dbkey_buf[1024];
    ObString& user_name = cs_info.get_login_req().get_hsr_result().user_name_;
    ObString& tenant_name = cs_info.get_login_req().get_hsr_result().tenant_name_;
    ObString& cluster_name = cs_info.get_login_req().get_hsr_result().cluster_name_;
    const ObString& password = cs_info.get_login_req().get_hsr_result().response_.get_auth_response();
    ObMysqlSessionUtils::make_full_username(dbkey_buf, 1024, user_name, tenant_name, cluster_name);
    ObString dbkey = ObString::make_string(dbkey_buf);
    ObMysqlServerSessionListPool* server_session_list_pool = get_global_session_manager().get_server_session_list_pool(dbkey);
    if (NULL != server_session_list_pool) {
      if (OB_ISNULL(server_session_list_pool->schema_key_.shard_conn_)) {
        LOG_WDIAG("shard conn is null", K(server_session_list_pool->schema_key_));
      } else if (server_session_list_pool->schema_key_.shard_conn_->password_.config_string_.compare(password) == 0){
        LOG_DEBUG("same password", K(user_name), K(tenant_name), K(cluster_name));
        bret = true;
      } else {
        LOG_DEBUG("not same password", K(user_name), K(tenant_name), K(cluster_name),
          K(password.length()), K(password.hash()),
          K(server_session_list_pool->schema_key_.shard_conn_->password_.config_string_.hash()),
          K(server_session_list_pool->schema_key_.shard_conn_->password_.config_string_.length()));
        bret = false;
      }
      server_session_list_pool->dec_ref();
      server_session_list_pool = NULL;
    }
  }
  if (bret) {
    s.sm_->client_session_->set_can_direct_ok(true);
  }
  return bret;
}

bool ObMysqlTransact::is_sequence_request(ObTransState &s) {
  bool is_sequence_request = false;
  int ret = OB_SUCCESS;
  if (s.trans_info_.client_request_.get_parse_result().is_dual_request()) {
    ObMysqlClientSession *client_session = s.sm_->client_session_;
    if (OB_ISNULL(client_session)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("invalid client_session argument");
    } else if (client_session->get_session_info().is_sharding_user()) {
      ObClientSessionInfo &session_info = client_session->get_session_info();
      ObProxyMysqlRequest &client_request = s.trans_info_.client_request_;
      ObString sql = client_request.get_sql();
      oceanbase::obproxy::opsql::ObProxyDualParser parser;
      ObSqlParseResult& parse_result = client_request.get_parse_result();
      ObProxyDualParseResult& dual_result = parse_result.get_dual_result();
      if (OB_FAIL(parser.parse(sql, dual_result, static_cast<ObCollationType>(session_info.get_collation_connection())))) {
        LOG_DEBUG("parse sequence_sql_ fail", K(sql)); // ignore parse fail, maybe a db dual request
      } else if (OB_UNLIKELY(!parser.is_valid_result())) {
        LOG_DEBUG("not a senquence sql", K(ret));
      } else {
        SqlFieldResult& sql_result = parse_result.get_sql_filed_result();
        ObMysqlRequestAnalyzer::extract_fileds(dual_result.expr_result_,  sql_result);
        ObDbConfigLogicDb *logic_db_info = NULL;
        if (OB_FAIL(ObProxyShardUtils::get_logic_db_info(s, session_info, logic_db_info))) {
          LOG_DEBUG("fail to get logic_db_info", K(ret));
        } else {
          ObSequenceParam sequence_param;
          if (OB_FAIL(logic_db_info->get_sequence_param(sequence_param))) {
            LOG_WDIAG("fail to get_sequence_param", K(ret));
          } else {
            is_sequence_request = sequence_param.is_sequence_enable_;
            LOG_DEBUG("is_sequence_request", K(is_sequence_request));
          }
          if (NULL != logic_db_info) {
            logic_db_info->dec_ref();
            logic_db_info = NULL;
          }
        }
      }
    }
  }
  return is_sequence_request;
}

inline bool ObMysqlTransact::is_single_shard_db_table(ObTransState &s)
{
  bool bret = false;
  ObClientSessionInfo &cs_info = s.sm_->client_session_->get_session_info();
  ObString logic_tenant_name;
  ObString logic_database_name;
  ObDbConfigLogicDb *db_info = NULL;
  int ret = OB_SUCCESS;
  if (OB_FAIL(cs_info.get_logic_tenant_name(logic_tenant_name))) {
    LOG_WDIAG("fail to get_logic_tenant_name", K(ret));
  } else if (OB_FAIL(cs_info.get_logic_database_name(logic_database_name))) {
    LOG_WDIAG("fail to get_logic_database_name, maybe no database selected", K(ret));
  } else if (OB_ISNULL(db_info = get_global_dbconfig_cache().get_exist_db_info(logic_tenant_name, logic_database_name))) {
    LOG_WDIAG("unknown logic db info", K(logic_database_name));
  } else {
    bret = db_info->is_single_shard_db_table();
  }
  if (NULL != db_info) {
    db_info->dec_ref();
    db_info = NULL;
  }
  return bret;
}

int ObMysqlTransact::set_server_ip_by_shard_conn(ObTransState &s, ObShardConnector* shard_conn)
{
  int ret = OB_SUCCESS;
  sockaddr sa;
  int64_t port = 0;
  if (OB_FAIL(shard_conn->get_physic_ip(sa))) {
    LOG_WDIAG("fail to get ip", "physic_addr", shard_conn->physic_addr_.config_string_, K(ret));
  } else if (OB_FAIL(get_int_value(shard_conn->physic_port_.config_string_, port))) {
    LOG_WDIAG("fail to get port", "physic_port", shard_conn->physic_port_.config_string_, K(ret));
  } else {
    ops_ip_port_cast(sa) = (htons)(static_cast<uint16_t>(port));
    s.server_info_.set_addr(sa);
    s.pll_info_.lookup_success_ = true;
    LOG_DEBUG("target server addr is set", "physic_addr", shard_conn->physic_addr_.config_string_,
              "physic_port", shard_conn->physic_port_.config_string_,
              "port", port, "address", s.server_info_.addr_);
  }
  return ret;
}

void ObMysqlTransact::handle_mysql_request(ObTransState &s)
{
  int ret = OB_SUCCESS;
  sockaddr sa;
  memset(&sa, 0, sizeof(sa));
  bool need_pl_lookup = (ObMysqlTransact::need_pl_lookup(s) == NEED_PL_LOOKUP);
  ObMysqlServerSession *last_session = s.sm_->client_session_->get_server_session();
  if (need_pl_lookup) {
    ObMysqlServerSession *svr_session = NULL;
    ObShardConnector *shard_conn = s.sm_->client_session_->get_session_info().get_shard_connector();
    if (OB_ISNULL(shard_conn)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("shard conn is NULL", K(ret));
    } else if (OB_NOT_NULL(last_session)
               && last_session->get_session_info().get_shard_connector()->shard_name_ == shard_conn->shard_name_) {
      s.server_info_.set_addr(last_session->get_netvc()->get_remote_addr());
      s.pll_info_.lookup_success_ = true;
      LOG_DEBUG("[ObMysqlTransact::handle mysql request] direct use last server session");
    } else if (shard_conn->is_physic_ip_) {
      //非域名模式，直接使用地址
      ret = set_server_ip_by_shard_conn(s, shard_conn);
    } else if (s.sm_->client_session_->is_session_pool_client()) {
      if (s.sm_->client_session_->is_proxy_mysql_client_) {
        ret = set_server_ip_by_shard_conn(s, shard_conn);
      } else {
        //这里sa参数仅为了匹配函数参数，不会真正使用,连接池根据shard_conn的类型来设置从连接池获取的key信息。
        if (OB_FAIL(s.sm_->client_session_->init_session_pool_info()) ||
              OB_FAIL(s.sm_->client_session_->acquire_svr_session_in_session_pool(sa, svr_session))) {
          ret = set_server_ip_by_shard_conn(s, shard_conn);
        } else if (NULL == svr_session) {
          ret = set_server_ip_by_shard_conn(s, shard_conn);
        } else {
          s.server_info_.set_addr(svr_session->get_netvc()->get_remote_addr());
          s.pll_info_.lookup_success_ = true;
          svr_session->release();
        }
      }
    } else {
      if (OB_FAIL(s.sm_->client_session_->get_session_manager_new().acquire_random_session(
                                   shard_conn->shard_name_.config_string_, svr_session))) {
        ret = set_server_ip_by_shard_conn(s, shard_conn);
      } else {
        LOG_DEBUG("[ObMysqlTransact::handle mysql request] get server session from session pool");
        if (OB_FAIL(s.sm_->client_session_->get_session_manager_new().release_session(
                                         shard_conn->shard_name_.config_string_, *svr_session))) {
          PROXY_SS_LOG(WDIAG, "fail to release server session to new session manager, it will be closed", K(ret));
        } else {
          s.server_info_.set_addr(svr_session->get_netvc()->get_remote_addr());
          s.pll_info_.lookup_success_ = true;
        }
      }
    }
  } else {
    // !need_pl_lookup
    LOG_DEBUG("[ObMysqlTransact::handle mysql request] force to use last server session");
    if (NULL != last_session) {
      s.server_info_.set_addr(last_session->get_netvc()->get_remote_addr());
      s.pll_info_.lookup_success_ = true;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("[ObMysqlTransact::handle request] last session is NULL, we have to disconnect");
    }
  }

  if (OB_FAIL(ret)) {
    s.inner_errcode_ = ret;
    TRANSACT_RETURN_WITH_MSG(SM_ACTION_SEND_ERROR_NOOP, NULL);
  } else {
    TRANSACT_RETURN(SM_ACTION_OBSERVER_OPEN, ObMysqlTransact::handle_response);
  }
}

void ObMysqlTransact::handle_ps_close_reset(ObTransState &s)
{
  int ret = OB_SUCCESS;
  ObMysqlClientSession *client_session = s.sm_->get_client_session();

  ObIArray<ObIpEndpoint> &remove_addrs = client_session->get_session_info().get_request_send_addrs();
  /* 每次请求结束都会 放 server_entry_ 和 server_session_, 包括 close 命令的内部多次跳转到这里*/
  if (!client_session->is_first_handle_request()) {
    s.sm_->release_server_session();
    /* 如果不是第一次进来, 则还要根据第一次进来时的事务状态进行判断 */
    bool need_pl_lookup = s.is_need_pl_lookup() && !client_session->is_in_trans_for_close_request();
    // sync pl_lookup_state_
    if (!need_pl_lookup) {
      if (client_session->is_proxy_enable_trans_internal_routing()) {
        s.pl_lookup_state_ = USE_COORDINATOR_SESSION;
      } else {
        s.pl_lookup_state_ = USE_LAST_SERVER_SESSION;
      }
    }
  } else {
    /* 第一次进来, 记录下之前的事务状态 */
    client_session->set_in_trans_for_close_request(is_in_trans(s));

    remove_addrs.reuse();
    bool found = false;
    uint32_t client_ps_id = client_session->get_session_info().get_client_ps_id();
    /* 如果大于 1 << 31L, 表示是 cursor_id; 否则是 ps_id */
    if (client_ps_id >= (CURSOR_ID_START)) {
      ObCursorIdAddr *cursor_id_addr = client_session->get_session_info().get_cursor_id_addr(client_ps_id);
      if (NULL != cursor_id_addr) {
        for (int64_t i = 0; !found && i< remove_addrs.count(); i++) {
          if (remove_addrs.at(i) == cursor_id_addr->get_addr()) {
            found = true;
          }
        }
        if (!found) {
          if (OB_FAIL(remove_addrs.push_back(cursor_id_addr->get_addr()))) {
            LOG_WDIAG("fail to push back addr", K(ret));
          }
        }
      }
    } else {
      ObPsIdAddrs *ps_id_addrs = client_session->get_session_info().get_ps_id_addrs(client_ps_id);
      if (NULL != ps_id_addrs && 0 != ps_id_addrs->get_addrs().count()) {
        ObIArray<ObIpEndpoint> &array = ps_id_addrs->get_addrs();
        for (int64_t i = 0; i < array.count(); i++) {
          found = false;
          for (int64_t j = 0; !found && j < remove_addrs.count(); j++) {
            if (remove_addrs.at(j) == array.at(i)) {
              found = true;
            }
          }
          if (!found) {
            if (OB_FAIL(remove_addrs.push_back(array.at(i)))) {
              LOG_WDIAG("fail to push back addr", K(ret));
            }
          }
        }
      }
    }
  }

  ObMysqlServerSession *last_session = client_session->get_server_session();
  ObMysqlServerSession *last_bound_session = client_session->get_last_bound_server_session();

  /* 如果不需要路由, 并且是第一次进来, 则以下几种情况需要断连接:
    *  1. last_session 不存在.
    *  2. last_bound_session 不为空.
    */
  if (!s.is_need_pl_lookup() && client_session->is_first_handle_request()
      && (NULL == last_session || NULL != last_bound_session)) {
    LOG_EDIAG("[ObMysqlTransact::handle request] something is wrong, we have to disconnect",
        "is_first_handle_close_request_", client_session->is_first_handle_request(),
        KP(last_session), KP(last_bound_session));
    TRANSACT_RETURN_WITH_MSG(SM_ACTION_SEND_ERROR_NOOP, NULL);
  } else {
    bool is_need_send_cmd = false;
    bool is_need_send_to_bound_ss = false;
    ObIpEndpoint addr;

    if (0 != remove_addrs.count()) {
      is_need_send_cmd = true;
      /* 如果不需要路由, 并且是第一次进来
       * 那检查是否有需要发送到 bound_ss, 如果有, 则最先发送, 否则还要迁移连接, 再迁移回来
       */
      if (!s.is_need_pl_lookup()
          && client_session->is_first_handle_request()) {
        for (int64_t i = 0; i < remove_addrs.count(); i++) {
          if (remove_addrs.at(i) == ObIpEndpoint(last_session->get_netvc()->get_remote_addr())) {
            is_need_send_to_bound_ss = true;
            addr = remove_addrs.at(i);
            break;
          }
        }
      }

      if (!is_need_send_to_bound_ss) {
        addr = remove_addrs.at(0);
      }
    }

    if (is_need_send_cmd) {
      /* 如果不需要路由, 并且要发送的 server 里没有 last server sesion, 则迁移 */
      ObMysqlServerSession *last_bound_session = client_session->get_last_bound_server_session();
      if (!s.is_need_pl_lookup()
          && !is_need_send_to_bound_ss
          && NULL == last_bound_session) {
        client_session->attach_server_session(NULL);
        last_session->do_io_read(client_session, 0, NULL);
        client_session->set_last_bound_server_session(last_session);
        client_session->set_need_return_last_bound_ss(true);
      }

      if (OB_SUCC(ret)) {
        client_session->set_first_handle_request(false);
        s.server_info_.set_addr(addr);
        s.pll_info_.lookup_success_ = true;
        start_access_control(s);
      }
    } else {
      LOG_DEBUG("handle_ps_close_reset don't need send cmd");
      /* 这说明发生过 session 迁移, 要迁移回去 */
      if (client_session->is_need_return_last_bound_ss()) {
        if (NULL != last_bound_session) {
          if (OB_FAIL(return_last_bound_server_session(client_session))) {
            LOG_WDIAG("fail to return last bond server session", K(ret));
          }
        } else {
          LOG_WDIAG("[ObMysqlTransact::handle request] last bound session is NULL, we have to disconnect");
          ret = OB_ERR_UNEXPECTED;
        }
      }

      if (OB_SUCC(ret)) {
        if (!client_session->is_in_trans_for_close_request()) {
          s.sm_->trans_state_.current_.state_ = ObMysqlTransact::TRANSACTION_COMPLETE;
        } else {
          s.sm_->trans_state_.current_.state_ = ObMysqlTransact::CMD_COMPLETE;
        }

        /* 如果后端没执行过或者全部 close 了, 转内部请求, 把 client session 上的相关缓存清掉 */
        TRANSACT_RETURN(SM_ACTION_INTERNAL_REQUEST, handle_internal_request);
      } else {
    TRANSACT_RETURN_WITH_MSG(SM_ACTION_SEND_ERROR_NOOP, NULL);
      }
    }
  }
}

void ObMysqlTransact::handle_oceanbase_request(ObTransState &s)
{
  int ret = OB_SUCCESS;
  ObClientSessionInfo &cs_info = get_client_session_info(s);
  ObProxyMysqlRequest &client_request = s.trans_info_.client_request_;
  ObPLLookupState state = need_pl_lookup(s);
  // if target db server set
  s.use_cmnt_target_db_server_ = OB_NOT_NULL(client_request.get_parse_result().get_target_db_server())
                                && !client_request.get_parse_result().get_target_db_server()->is_empty()
                                && !s.sm_->client_session_->is_proxy_mysql_client_;
  s.use_conf_target_db_server_ = OB_NOT_NULL(s.sm_->target_db_server_)
                                && !s.sm_->target_db_server_->is_empty()
                                && !s.sm_->client_session_->is_proxy_mysql_client_;
  s.pl_lookup_state_ = state;
  if (OB_UNLIKELY(!cs_info.is_allow_use_last_session())) {
    s.pl_lookup_state_ = NEED_PL_LOOKUP;
  }
  // generate full link trace span before each request
  if (s.sm_->get_client_session()->is_first_handle_request()) {
    if (OB_FAIL(s.sm_->handle_req_for_begin_proxy_root_span())) {
      LOG_WDIAG("fail to handle req for begin proxy root span", K(ret));
    }
  }
  #ifdef ERRSIM
  if (OB_SUCC(ret) && OB_FAIL(OB_E(EventTable::EN_HANDLE_REQUEST_FAIL) OB_SUCCESS)) {
    s.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
    COLLECT_INTERNAL_DIAGNOSIS(
        s.sm_->connection_diagnosis_trace_, obutils::OB_PROXY_INTERNAL_TRACE, ret, "errsim diagnosis");
    TRANSACT_RETURN_WITH_MSG(SM_ACTION_SEND_ERROR_NOOP, NULL);
    return;
  }
  #endif

  obmysql::ObMySQLCmd cmd = s.trans_info_.sql_cmd_;
  ObDiagnosisRouteInfo::RouteInfoType route_info_type = ObDiagnosisRouteInfo::INVALID;

  const bool in_trans = is_in_trans(s);
  #ifdef ERRSIM
  if (OB_SUCC(ret) && OB_FAIL(OB_E(EventTable::EN_TRANSACTION_COORDINATOR_INVALID) OB_SUCCESS)) {
    ret = OB_SUCCESS;
    s.sm_->get_client_session()->get_trans_coordinator_ss_addr().reset();
  }
  #endif
  if (in_trans
      && s.sm_->get_client_session()->is_proxy_enable_trans_internal_routing()
      && OB_UNLIKELY(!s.sm_->get_client_session()->get_trans_coordinator_ss_addr().is_valid())) {
    // if coordinator closed, transaction is not available, disconnect directly
    ret = OB_ERR_UNEXPECTED;
    s.internal_error_op_for_diagnosis_ = ObMysqlTransact::PROXY_INTERNAL_ERROR_TRANSFER_DISCONNECT;
    LOG_WDIAG("coordinator session addr is invalid, we have to disconnect", K(ret));
    TRANSACT_RETURN_WITH_MSG(SM_ACTION_SEND_ERROR_NOOP, NULL); // disconnect
  } else if (OB_UNLIKELY(cs_info.is_sharding_user()) &&
    OB_FAIL(ObProxyShardUtils::update_sys_read_consistency_if_need(cs_info))) {
    LOG_WDIAG("fail to update_sys_read_consistency_if_need", K(ret));
    TRANSACT_RETURN_WITH_MSG(SM_ACTION_SEND_ERROR_NOOP, NULL); // disconnect
  } else if (s.sm_->get_client_session()->get_session_info().get_priv_info().user_name_ == ObProxyTableInfo::DETECT_USERNAME_USER && s.server_info_.addr_.is_valid()) {
    lookup_skip_open_server(s);
  } else if (OB_UNLIKELY(need_server_session_lookup(s))) {
    TRANSACT_RETURN(SM_ACTION_SERVER_ADDR_LOOKUP, handle_server_addr_lookup);
  } else if (OB_UNLIKELY(obmysql::OB_MYSQL_COM_STMT_CLOSE == cmd || obmysql::OB_MYSQL_COM_STMT_RESET == cmd)) {
    handle_ps_close_reset(s);
  } else if (client_request.get_parse_result().is_text_ps_drop_stmt()) {
    handle_ps_close_reset(s);
  } else if (OB_LIKELY(s.is_need_pl_lookup())) {
    route_info_type = ObDiagnosisRouteInfo::USE_PARTITION_LOCATION_LOOKUP;
    // if need pl lookup, we should extract pl info first
    if (OB_FAIL(extract_partition_info(s, (int32_t &) route_info_type))) {
      LOG_WDIAG("fail to extract partition info", K(ret));
      s.inner_errcode_ = ret;
      TRANSACT_RETURN_WITH_MSG(SM_ACTION_SEND_ERROR_NOOP, NULL); // disconnect
    } else {
      int64_t addr = cs_info.get_obproxy_route_addr();
      if (OB_UNLIKELY(0 != addr)) {
        uint32_t ip = 0;
        uint16_t port = 0;
        get_ip_port_from_addr(addr, ip, port);
        s.server_info_.set_addr(ip, port);
        LOG_DEBUG("set addr here", K(s.server_info_.addr_));
        s.pll_info_.lookup_success_ = true;
        LOG_DEBUG("@obproxy_route_addr is set", "address", s.server_info_.addr_, K(addr));
        route_info_type = ObDiagnosisRouteInfo::USE_OBPROXY_ROUTE_ADDR;
      } else if (obmysql::OB_MYSQL_COM_STMT_FETCH == cmd
                 || obmysql::OB_MYSQL_COM_STMT_GET_PIECE_DATA == cmd) {
        ObCursorIdAddr *cursor_id_addr = NULL;
        if (OB_FAIL(cs_info.get_cursor_id_addr(cursor_id_addr))) {
          LOG_WDIAG("fail to get client cursor id addr", K(ret));
          if (OB_HASH_NOT_EXIST == ret) {
            handle_fetch_request(s);
          } else {
            TRANSACT_RETURN_WITH_MSG(SM_ACTION_SEND_ERROR_NOOP, NULL);
          }
        } else {
          s.server_info_.set_addr(cursor_id_addr->get_addr());
          s.pll_info_.lookup_success_ = true;
          LOG_DEBUG("succ to set cursor target addr", "address", s.server_info_.addr_, KPC(cursor_id_addr));
          route_info_type = ObDiagnosisRouteInfo::USE_CURSOR;
        }
      } else if (obmysql::OB_MYSQL_COM_STMT_SEND_PIECE_DATA == cmd
                 || obmysql::OB_MYSQL_COM_STMT_PREPARE_EXECUTE == cmd
                 || obmysql::OB_MYSQL_COM_STMT_SEND_LONG_DATA == cmd
                 || obmysql::OB_MYSQL_COM_STMT_EXECUTE == cmd) {
        /* use piece info to record the last server addr */
        /* send piece data/send long data/prepare execute/execute should send to the same server as chosen before */
        ObPieceInfo *info = NULL;
        if (OB_FAIL(cs_info.get_piece_info(info))) {
          // maybe the first time to send, ignore not exist err, choose path by pl lookup
          if (OB_HASH_NOT_EXIST == ret) {
            LOG_DEBUG("fail to get piece info from hash map", K(ret));
            ret = OB_SUCCESS;
            // do nothing
          } else {
            LOG_WDIAG("fail to get piece info", K(ret));
            TRANSACT_RETURN_WITH_MSG(SM_ACTION_SEND_ERROR_NOOP, NULL);
          }
        } else if (OB_ISNULL(info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("info is null", K(ret));
          TRANSACT_RETURN_WITH_MSG(SM_ACTION_SEND_ERROR_NOOP, NULL);
        } else if (!info->is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("info is not valid", K(ret));
          TRANSACT_RETURN_WITH_MSG(SM_ACTION_SEND_ERROR_NOOP, NULL);
        } else {
          s.server_info_.set_addr(info->get_addr());
          s.pll_info_.lookup_success_ = true;
          LOG_DEBUG("succ to set target addr for send piece/prepare execute/send long data", "address",
                    s.server_info_.addr_, KPC(info));
          route_info_type = ObDiagnosisRouteInfo::USE_PIECES_DATA;
        }
      } else { /* do nothing */}

      // Specified server addr
      // 1. target_db_server (comment & multi levle config)
      // 2. test_server_addr & server_routing_mode (deprecated global config)

      // target db server (sql comment & multi level config)
      if (OB_SUCC(ret) && !s.pll_info_.lookup_success_) {
        // Use target_db_server
        // exsit in sql comment parsed result
        if (s.use_cmnt_target_db_server_) {
          client_request.get_parse_result().get_target_db_server()->set_obproxy_addr(s.sm_->client_session_->get_netvc()->get_local_addr());
          if (OB_FAIL(client_request.get_parse_result().get_target_db_server()->get(s.server_info_.addr_))) {
            LOG_WDIAG("fail to get target db server from sql comment", K(ret));
            handle_target_db_not_allow(s);
          } else {
            s.pll_info_.lookup_success_ = true;
            // in case of dummy entry is NULL
            if (NULL == s.sm_->client_session_->dummy_entry_ || s.sm_->is_cached_dummy_entry_expired()) {
              s.pll_info_.lookup_success_ = false;
              s.pll_info_.set_cached_dummy_force_renew();
              s.sm_->client_session_->is_need_update_dummy_entry_ = true;
              LOG_WDIAG("set lookup_success_=false since dummy entry is NULL or expired");
            }
            route_info_type = ObDiagnosisRouteInfo::USE_COMMENT_TARGET_DB;
            LOG_DEBUG("succ to get target db server from sql comment",
                    "address", s.server_info_.addr_);
          }
        // exist in multi level config target_db_server
        } else if (s.use_conf_target_db_server_) {
          s.sm_->target_db_server_->set_obproxy_addr(s.sm_->client_session_->get_netvc()->get_local_addr());
          s.sm_->target_db_server_->reuse();
          if (OB_FAIL(s.sm_->target_db_server_->get(s.server_info_.addr_))) {
            LOG_WDIAG("fail to get target db server from config", K(ret));
            handle_target_db_not_allow(s);
          } else {
            s.pll_info_.lookup_success_ = true;
            // in case of dummy entry is NULL
            if (NULL == s.sm_->client_session_->dummy_entry_ || s.sm_->is_cached_dummy_entry_expired()) {
              s.pll_info_.lookup_success_ = false;
              s.pll_info_.set_cached_dummy_force_renew();
              s.sm_->client_session_->is_need_update_dummy_entry_ = true;
            }
            LOG_DEBUG("succ to get target db server from multi level config",
                      "address", s.server_info_.addr_);
            route_info_type = ObDiagnosisRouteInfo::USE_CONFIG_TARGET_DB;
          }
        } else { /* do nothing */}
      } else if (OB_SUCC(ret)) {
        if ((s.use_cmnt_target_db_server_ && !client_request.get_parse_result().get_target_db_server()->contains(s.server_info_.addr_)) ||
            (!s.use_cmnt_target_db_server_ && s.use_conf_target_db_server_ && !s.sm_->target_db_server_->contains(s.server_info_.addr_))) {
          LOG_WDIAG("Not allow to route to target db server", "proxy current choosen", s.server_info_.addr_, K(ret));
          ret = OB_OP_NOT_ALLOW;
          handle_target_db_not_allow(s);
        }
      }
      // deprecated global config
      if (OB_SUCC(ret) && !s.pll_info_.lookup_success_ && !s.use_cmnt_target_db_server_ && !s.use_conf_target_db_server_) {
        if ((s.mysql_config_params_->is_mock_routing_mode() && !s.sm_->client_session_->is_proxy_mysql_client_)
                   || s.mysql_config_params_->is_mysql_routing_mode()) {
          if (OB_FAIL(s.mysql_config_params_->get_one_test_server_addr(s.server_info_.addr_))) {
            LOG_INFO("mysql or mock mode, but test server addr in not set, do normal pl lookup", K(ret));
            if (NULL == s.sm_->client_session_->dummy_entry_) {
              s.sm_->client_session_->is_need_update_dummy_entry_ = true;
            }
            ret = OB_SUCCESS;
          } else {
            s.sm_->client_session_->test_server_addr_ = s.server_info_.addr_;
            s.pll_info_.lookup_success_ = true;
            LOG_DEBUG("mysql mode, test server is valid, just use it and skip pl lookup",
                      "address", s.server_info_.addr_);
            route_info_type = ObDiagnosisRouteInfo::USE_TEST_SVR_ADDR;
          }
        } else { /* do nothing */}
      }

      if (OB_SUCC(ret) && !s.pll_info_.lookup_success_) {
        if (OB_UNLIKELY(!s.mysql_config_params_->is_random_routing_mode()
                             && !s.api_server_addr_set_
                             && s.pll_info_.te_name_.is_all_dummy_table()
                             && cs_info.is_allow_use_last_session()
                             && s.sm_->proxy_primary_zone_name_[0] == '\0'
                             && !s.use_cmnt_target_db_server_
                             && !s.use_conf_target_db_server_)) {
          acquire_cached_server_session(s);
          if (s.server_info_.addr_.is_valid()) {
            route_info_type = ObDiagnosisRouteInfo::USE_CACHED_SESSION;
          }
        }
      }

      if (OB_SUCC(ret)) {
        TRANSACT_RETURN(SM_ACTION_PARTITION_LOCATION_LOOKUP, handle_pl_lookup);
      }
    }
    ROUTE_DIAGNOSIS(s.sm_->route_diagnosis_,
                    ROUTE_INFO,
                    route_info,
                    ret,
                    route_info_type,
                    s.server_info_.addr_,
                    in_trans,
                    has_dependent_func(s),
                    is_trans_specified(s));
  } else {
    route_info_type = ObDiagnosisRouteInfo::INVALID;
    // !need_pl_lookup
    LOG_DEBUG("[ObMysqlTransact::handle request] force to use last server session/coordinator server session", K(get_pl_lookup_state_string(s.pl_lookup_state_)));
    ObMysqlClientSession *client_session = s.sm_->get_client_session();
    ObMysqlServerSession *last_session = client_session->get_server_session();
    sockaddr target_addr;

    if (s.pl_lookup_state_ == USE_COORDINATOR_SESSION) {
      LOG_DEBUG("use coordinator session");
      target_addr = client_session->get_trans_coordinator_ss_addr().sa_;
      route_info_type = ObDiagnosisRouteInfo::USE_COORDINATOR_SESSION;
    } else if (s.pl_lookup_state_ == USE_LAST_SERVER_SESSION) {
      LOG_DEBUG("use last server session");
      if (OB_UNLIKELY(NULL == last_session )) {
        TRANSACT_RETURN_WITH_MSG(SM_ACTION_SEND_ERROR_NOOP, NULL);
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("[ObMysqlTransact::handle request] last server session is invalid, we have to disconnect", K(ret));
      } else {
        target_addr = last_session->get_netvc()->get_remote_addr();
        route_info_type = ObDiagnosisRouteInfo::USE_LAST_SESSION;
      }
    } else {
      TRANSACT_RETURN_WITH_MSG(SM_ACTION_SEND_ERROR_NOOP, NULL);
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("[ObMysqlTransact::handle request] unexpected pl lookup state, we have to disconnect", K(s.pl_lookup_state_), K(ret));
    }

    if (OB_SUCC(ret)) {
      if (obmysql::OB_MYSQL_COM_STMT_FETCH == cmd) {
        ObCursorIdAddr *cursor_id_addr = NULL;
        if (OB_FAIL(cs_info.get_cursor_id_addr(cursor_id_addr))) {
          LOG_WDIAG("fail to get client cursor id addr", K(ret));
          if (OB_HASH_NOT_EXIST == ret) {
            handle_fetch_request(s);
          } else {
            TRANSACT_RETURN_WITH_MSG(SM_ACTION_SEND_ERROR_NOOP, NULL);
          }
        } else {
          if (OB_SUCC(ret) && OB_UNLIKELY(!ops_ip_addr_port_eq(cursor_id_addr->get_addr(), target_addr))) {
            if (USE_COORDINATOR_SESSION == s.pl_lookup_state_) {
              // under internal routing transaction, OB_MYSQL_COM_STMT_FETCH may be routed to participant
              // don't need compare target server
            } else {
              client_session->attach_server_session(NULL);
              last_session->do_io_read(client_session, 0, NULL);
              client_session->set_last_bound_server_session(last_session);
              client_session->set_need_return_last_bound_ss(true);
            }
          }

          s.server_info_.set_addr(cursor_id_addr->get_addr());
          s.pll_info_.lookup_success_ = true;
          route_info_type = ObDiagnosisRouteInfo::USE_CURSOR;
        }
      } else if (obmysql::OB_MYSQL_COM_STMT_GET_PIECE_DATA == cmd) {
        ObCursorIdAddr *cursor_id_addr = NULL;
        if (OB_FAIL(cs_info.get_cursor_id_addr(cursor_id_addr))) {
          LOG_WDIAG("fail to get client cursor id addr", K(ret));
          if (OB_HASH_NOT_EXIST == ret) {
            handle_fetch_request(s);
          } else {
            TRANSACT_RETURN_WITH_MSG(SM_ACTION_SEND_ERROR_NOOP, NULL);
          }
        } else if (OB_UNLIKELY(!ops_ip_addr_port_eq(cursor_id_addr->get_addr(), target_addr))) {
          if (USE_COORDINATOR_SESSION == s.pl_lookup_state_) {
            // under internal routing transaction, OB_MYSQL_COM_STMT_GET_PIECE_DATA may be routed to participant
            // don't need compare target server
            target_addr = cursor_id_addr->get_addr().sa_;
          } else {
            s.mysql_errcode_ = OB_ERR_DISTRIBUTED_NOT_SUPPORTED;
            s.mysql_errmsg_ = "fetch cursor target server is not the trans server";
            int tmp_ret = OB_SUCCESS;
            if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = encode_error_message(s)))) {
              LOG_WDIAG("fail to build err packet", K(tmp_ret));
            } else {
              LOG_WDIAG("fetch cursor target server is not the trans server",
                      "fetch cursor target server", cursor_id_addr->get_addr(),
                      "trans server", ObIpEndpoint(target_addr));
            }

            ret = OB_ERR_DISTRIBUTED_NOT_SUPPORTED;
            s.inner_errcode_ = ret;
            s.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
            TRANSACT_RETURN(SM_ACTION_INTERNAL_NOOP, NULL);
          }
        }
        if (OB_SUCC(ret)) {
          s.server_info_.set_addr(target_addr);
          s.pll_info_.lookup_success_ = true;
          route_info_type = ObDiagnosisRouteInfo::USE_CURSOR;
        }
      } else if (obmysql::OB_MYSQL_COM_STMT_SEND_PIECE_DATA == cmd
                 || obmysql::OB_MYSQL_COM_STMT_SEND_LONG_DATA == cmd) {
        ObPieceInfo *info = NULL;
        if (OB_FAIL(cs_info.get_piece_info(info))) {
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            // do nothing
          } else {
            TRANSACT_RETURN_WITH_MSG(SM_ACTION_SEND_ERROR_NOOP, NULL);
          }
        } else if (NULL == info) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("info is null unexpected", K(ret));
          TRANSACT_RETURN_WITH_MSG(SM_ACTION_SEND_ERROR_NOOP, NULL);
        } else if (!info->is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("info is invalid", K(ret));
          TRANSACT_RETURN_WITH_MSG(SM_ACTION_SEND_ERROR_NOOP, NULL);
        } else if (OB_UNLIKELY(!ops_ip_addr_port_eq(info->get_addr(), target_addr))) {
          s.mysql_errcode_ = OB_ERR_DISTRIBUTED_NOT_SUPPORTED;
          s.mysql_errmsg_ = "send piece info target server is not the trans server";
          int tmp_ret = OB_SUCCESS;
          if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = encode_error_message(s)))) {
            LOG_WDIAG("fail to build err packet", K(tmp_ret));
          } else {
            LOG_WDIAG("send piece/long data target server is not the trans server",
                     "target server", info->get_addr(),
                     "trans server", ObIpEndpoint(target_addr));
          }

          ret = OB_ERR_DISTRIBUTED_NOT_SUPPORTED;
          s.inner_errcode_ = ret;
          s.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
          TRANSACT_RETURN(SM_ACTION_INTERNAL_NOOP, NULL);
        }

        if (OB_SUCC(ret)) {
          s.server_info_.set_addr(target_addr);
          s.pll_info_.lookup_success_ = true;
          route_info_type = ObDiagnosisRouteInfo::USE_PIECES_DATA;
        }
      } else {
        s.server_info_.set_addr(target_addr);
        s.pll_info_.lookup_success_ = true;
      }
      ROUTE_DIAGNOSIS(s.sm_->route_diagnosis_,
                      ROUTE_INFO,
                      route_info,
                      ret,
                      route_info_type,
                      s.server_info_.addr_,
                      in_trans,
                      has_dependent_func(s),
                      is_trans_specified(s));
      if (OB_SUCC(ret)) {
        // if target db server set, then make sure target db server is the choosen server
        if (((s.use_cmnt_target_db_server_ && !client_request.get_parse_result().get_target_db_server()->contains(s.server_info_.addr_)) ||
            (!s.use_cmnt_target_db_server_ && s.use_conf_target_db_server_ && !s.sm_->target_db_server_->contains(s.server_info_.addr_))) &&
            !client_request.get_parse_result().has_explain_route()) {
          LOG_WDIAG("Not allow to route to target db server", "proxy current choosen", s.server_info_.addr_, K(ret));
          ret = OB_OP_NOT_ALLOW;
          handle_target_db_not_allow(s);
        } else {
          start_access_control(s);
        }
      }
    } else {
      ROUTE_DIAGNOSIS(s.sm_->route_diagnosis_,
                      ROUTE_INFO,
                      route_info,
                      ret,
                      route_info_type,
                      s.server_info_.addr_,
                      in_trans,
                      has_dependent_func(s),
                      is_trans_specified(s));
    }
  } // end of !s.need_pl_lookup
  LOG_DEBUG("handle oceanbase request, is addr valid:", K(s.server_info_.addr_.is_valid()));
}

void ObMysqlTransact::handle_fetch_request(ObTransState &s)
{
  int ret = OB_SUCCESS;

  int tmp_ret = OB_SUCCESS;
  if (s.sm_->client_session_->get_session_info().is_oracle_mode()) {
    s.mysql_errcode_ = OB_ERR_FETCH_OUT_SEQUENCE;
    tmp_ret = OB_ERR_FETCH_OUT_SEQUENCE;
  } else {
    s.mysql_errcode_ = OB_CURSOR_NOT_EXIST;
    tmp_ret = OB_CURSOR_NOT_EXIST;
  }

  if (OB_FAIL(s.sm_->get_client_buffer_reader()->consume_all())) {
    LOG_WDIAG("client buffer reader fail to consume all", K(ret));
  } else if (OB_FAIL(ObMysqlTransact::encode_error_message(s))) {
    LOG_WDIAG("fail to build err packet", K(ret));
  }

  if (OB_SUCC(ret)) {
    ret = tmp_ret;
  } else {
    /* if something error, disconnect */
    s.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
  }

  s.inner_errcode_ = ret;
  TRANSACT_RETURN_WITH_MSG(SM_ACTION_SEND_ERROR_NOOP, NULL);
}

void ObMysqlTransact::handle_target_db_not_allow(ObTransState &s)
{
  int ret = OB_SUCCESS;

  int tmp_ret = OB_OP_NOT_ALLOW;
  s.mysql_errcode_ = OB_OP_NOT_ALLOW;
  s.mysql_errmsg_ = "Not allow to route to target db server in the hint or configure";
  if (OB_FAIL(ObMysqlTransact::encode_error_message(s))) {
    LOG_WDIAG("fail to build err packet", K(ret));
  }

  if (OB_SUCC(ret)) {
    ret = tmp_ret;
  } else {
    /* if something error, disconnect */
    s.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
  }

  s.inner_errcode_ = ret;
  TRANSACT_RETURN_WITH_MSG(SM_ACTION_SEND_ERROR_NOOP, NULL);
}

void ObMysqlTransact::handle_explain_route(ObTransState &s)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(s.sm_->get_client_buffer_reader()->consume_all())) {
    LOG_WDIAG("client buffer reader fail to consume all for target db", K(ret));
  } else if (OB_FAIL(ObMysqlResponseBuilder::build_explain_route_resp(*s.internal_buffer_,
                                                                      s.trans_info_.client_request_,
                                                                      *s.sm_->client_session_,
                                                                      s.sm_->route_diagnosis_,
                                                                      s.sm_->get_client_session_protocol(),
                                                                      is_in_trans(s)))) {
    LOG_WDIAG("fail to build err packet", K(ret));
    /* if something error, disconnect */
    s.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
  }
  s.inner_errcode_ = ret;
  TRANSACT_RETURN(SM_ACTION_SEND_ERROR_NOOP, NULL);
}
int ObMysqlTransact::return_last_bound_server_session(ObMysqlClientSession *client_session)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(client_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid param, client session is NULL", K(ret));
  } else {
    ObMysqlServerSession *last_session = client_session->get_server_session();
    ObMysqlServerSession *last_bound_session = client_session->get_last_bound_server_session();
    /* 如果新的 bound_ss 不为空, 需要先放回连接池 */
    if (NULL != last_session) {
      last_session->release();
      client_session->attach_server_session(NULL);
    }
    /* 为什么要先设置 cur server session, 是因为 attach_server_session 有个检查 session != cur_ss_ */
    client_session->set_cur_server_session(last_bound_session);
    if (OB_FAIL(client_session->attach_server_session(last_bound_session))) {
      LOG_WDIAG("fail to attach server session", K(ret));
    } else {
      client_session->set_last_bound_server_session(NULL);
      client_session->set_need_return_last_bound_ss(false);
    }
  }

  return ret;
}

void ObMysqlTransact::handle_request(ObTransState &s)
{
  s.sm_->trans_stats_.client_requests_ += 1;
  if (OB_UNLIKELY(MYSQL_PLUGIN_AS_INTERCEPT == s.sm_->api_.plugin_tunnel_type_)) {
    setup_plugin_request_intercept(s);
  } else if (OB_UNLIKELY(s.trans_info_.client_request_.is_sharding_user() && is_sequence_request(s))) {
    LOG_DEBUG("is a sequence request");
    int ret = OB_SUCCESS;
    s.trans_info_.client_request_.get_parse_result().set_stmt_type(OBPROXY_T_ICMD_DUAL);
    ObProxyMysqlRequest &client_request = s.trans_info_.client_request_;
    if (OB_FAIL(ObMysqlRequestAnalyzer::init_cmd_info(client_request))) {
      LOG_WDIAG("[ObMysqlTransact::handle request] something wrong before, we have to disconnect");
      TRANSACT_RETURN_WITH_MSG(SM_ACTION_SEND_ERROR_NOOP, NULL);
    } else {
      TRANSACT_RETURN(SM_ACTION_INTERNAL_REQUEST, handle_internal_request);
    }
  } else if (OB_UNLIKELY(is_internal_request(s))) {
    if (s.trans_info_.client_request_.get_parse_result().is_internal_select()) {
      MYSQL_INCREMENT_TRANS_STAT(CLIENT_USE_LOCAL_SESSION_STATE_REQUESTS);
    }
    // so it's an internal request
    TRANSACT_RETURN(SM_ACTION_INTERNAL_REQUEST, handle_internal_request);
  } else if (OB_UNLIKELY(is_binlog_request(s))) {
    handle_binlog_request(s);
  } else {
    if (OB_UNLIKELY(get_global_performance_params().enable_stat_)) {
      if (obmysql::OB_MYSQL_COM_QUERY == s.trans_info_.sql_cmd_) {
        ObProxyBasicStmtType stmt_type = s.trans_info_.client_request_.get_parse_result().get_stmt_type();
        switch (stmt_type) {
          case OBPROXY_T_SELECT:
            MYSQL_INCREMENT_TRANS_STAT(CLIENT_SELECT_REQUESTS);
            break;
          case OBPROXY_T_DELETE:
            MYSQL_INCREMENT_TRANS_STAT(CLIENT_DELETE_REQUESTS);
            break;
          case OBPROXY_T_INSERT:
          case OBPROXY_T_MERGE:
            // fall through
          case OBPROXY_T_REPLACE:
            MYSQL_INCREMENT_TRANS_STAT(CLIENT_INSERT_REQUESTS);
            break;
          case OBPROXY_T_UPDATE:
            MYSQL_INCREMENT_TRANS_STAT(CLIENT_UPDATE_REQUESTS);
            break;
          default:
            MYSQL_INCREMENT_TRANS_STAT(CLIENT_OTHER_REQUESTS);
            break;
        }
      }
    }
    ROUTE_DIAGNOSIS(s.sm_->route_diagnosis_,
                    SQL_PARSE,
                    sql_parse,
                    OB_SUCCESS,
                    s.trans_info_.client_request_.get_print_sql(),
                    s.trans_info_.client_request_.get_parse_result().get_table_name(),
                    s.trans_info_.sql_cmd_);
    ObMysqlClientSession *client_session = s.sm_->get_client_session();
    if (OB_ISNULL(client_session)) {
      LOG_WDIAG("[ObMysqlTransact::handle request] client session is NULL, we have to disconnect");
      TRANSACT_RETURN_WITH_MSG(SM_ACTION_SEND_ERROR_NOOP, NULL);
    } else if (OB_UNLIKELY(is_session_memory_overflow(s))) {
      s.mysql_errcode_ = OB_EXCEED_MEM_LIMIT;
      int tmp_ret = OB_SUCCESS;
      COLLECT_INTERNAL_DIAGNOSIS(
          s.sm_->connection_diagnosis_trace_, obutils::OB_PROXY_INTERNAL_TRACE,
          OB_EXCEED_MEM_LIMIT, "client session exceed memory limit: %ld",
          s.mysql_config_params_->client_max_memory_size_);
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = encode_error_message(s)))) {
        LOG_WDIAG("fail to build err packet", K(tmp_ret));
      } else {
        LOG_WDIAG("client memory exceed memory limit",
                 "client mem size", client_session->get_session_info().get_memory_size(),
                 "config mem size", s.mysql_config_params_->client_max_memory_size_);
      }
      s.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
      TRANSACT_RETURN(SM_ACTION_INTERNAL_NOOP, NULL);
    } else if (OB_LIKELY(client_session->get_session_info().is_oceanbase_server())) {
      handle_oceanbase_request(s);
    } else {
      handle_mysql_request(s);
    }// end of NULL != s.sm_->client_session_
  }
}

inline bool ObMysqlTransact::need_use_coordinator_session(ObTransState &s) {
  // session in trans internal routing state
  bool use_coordinator = false;
  int ret = OB_SUCCESS;
  ObSqlParseResult &parser_result = s.trans_info_.client_request_.get_parse_result();
  ObClientSessionInfo &session_info = s.sm_->get_client_session()->get_session_info();
  if (ObMysqlTransact::is_trans_specified(s)) {
    if (s.sm_->get_client_session() != NULL
        && s.sm_->get_client_session()->get_server_session() != NULL
        && ops_ip_addr_port_eq (s.sm_->get_client_session()->get_trans_coordinator_ss_addr(),
                                s.sm_->get_client_session()->get_server_session()->get_netvc()->get_remote_addr())) {
      LOG_WDIAG("internal routing transaction specified, coordinator session not equal with last session", K(ret));
    }
    use_coordinator = true;
  } else if (ObMysqlTransact::need_use_tunnel(s)) {
    // large request, route to coordinator session
    use_coordinator = true;
  } else if (s.sm_->get_client_session()->is_trans_internal_routing()
      && is_sql_able_to_route_participant_in_trans(parser_result, s.trans_info_.sql_cmd_)) {
    use_coordinator = false;
  } else if (s.sm_->get_client_session()->is_trans_internal_routing() && parser_result.is_text_ps_execute_stmt()) {
    ObTextPsEntry* text_ps_entry = NULL;
    ObTextPsNameEntry* text_ps_name_entry = session_info.get_text_ps_name_entry();
    if (OB_ISNULL(text_ps_name_entry)) {
      use_coordinator = true;
      ret = OB_ERR_PREPARE_STMT_NOT_FOUND;
      LOG_WDIAG("need use coordinator on text ps execute stmt, get text ps name entry failed", K(ret));
    } else if (OB_ISNULL(text_ps_entry = text_ps_name_entry->text_ps_entry_) || !text_ps_entry->is_valid()) {
      use_coordinator = true;
      ret = OB_INVALID_ERROR;
      LOG_WDIAG("need use coordinator on text ps execute stmt, get text ps entry failed", K(ret));
    } else {
      use_coordinator = !is_sql_able_to_route_participant_in_trans(text_ps_entry->get_base_ps_parse_result(), s.trans_info_.sql_cmd_);
    }
  } else {
    // the rest of sql, use coordinator session
    use_coordinator = true;
  }
  return use_coordinator;
}

bool ObMysqlTransact::is_sql_able_to_route_participant_in_trans(obutils::ObSqlParseResult& base_sql_parse_result, obmysql::ObMySQLCmd  sql_cmd) {
  // transaction internal routing rule
  // (1) route dml with table name to participant
  // (2) route OB_MYSQL_COM_STMT_EXECUTE or text ps execute with inner sql like (1) to participant
  // (3) route sql command except OB_MYSQL_COM_QUERY/OB_MYSQL_COM_STMT_PREPARE/OB_MYSQL_COM_STMT_EXECUTE to coordinator
  // (4) route dual request to coordinator, in case of sql like `select DBMS_XA.XA_START(DBMS_XA_XID(?,?,?), 65536) from dual;` start xa transaction
  // (5) route multi-stmt to coordinator

  bool able_to_route = false;
  switch (sql_cmd) {
    case obmysql::OB_MYSQL_COM_QUERY:
    case obmysql::OB_MYSQL_COM_STMT_PREPARE:
    case obmysql::OB_MYSQL_COM_STMT_EXECUTE:
      able_to_route = true;
      break;

    default:
      able_to_route = false;
      break;
  }
  if (able_to_route) {
    if (base_sql_parse_result.is_dual_request()) {
      able_to_route = false;
    } else {
      able_to_route = (base_sql_parse_result.is_text_ps_prepare_stmt()
                      && base_sql_parse_result.is_text_ps_inner_dml_stmt()
                      && !base_sql_parse_result.is_text_ps_call_stmt()
                      && !base_sql_parse_result.is_multi_semicolon_in_stmt()
                      && base_sql_parse_result.get_table_name_length() > 0)
                    || (base_sql_parse_result.is_dml_stmt()
                        && !base_sql_parse_result.is_multi_stmt()
                        && !base_sql_parse_result.is_multi_semicolon_in_stmt()
                        && base_sql_parse_result.get_table_name_length() > 0);
    }
  }
  return able_to_route;
}

inline ObMysqlTransact::ObPLLookupState ObMysqlTransact::need_pl_lookup(ObTransState &s)
{
  ObPLLookupState state = NEED_PL_LOOKUP;
  bool trans_specified = is_trans_specified(s);
  bool is_dep_func = has_dependent_func(s);
  bool in_trans = is_in_trans(s);
  // 如果 OB_MYSQL_COM_LOAD
  if (OB_UNLIKELY(ObMysqlTransact::is_transfer_content_of_file(s))) {
    state = USE_LAST_SERVER_SESSION;
  } else if (in_trans && s.sm_->get_client_session()->is_proxy_enable_trans_internal_routing()) {
    if (is_dep_func) {
      state = USE_LAST_SERVER_SESSION;
    } else {
      state = need_use_coordinator_session(s) ? USE_COORDINATOR_SESSION : NEED_PL_LOOKUP;
    }
  } else {
    state = (in_trans || trans_specified || is_dep_func || OB_MYSQL_COM_AUTH_SWITCH_RESP == s.trans_info_.sql_cmd_) ?
             USE_LAST_SERVER_SESSION : NEED_PL_LOOKUP;
  }
  // if we don't use last server session/coordinator session, we must do pl lookup
  LOG_DEBUG("need pl lookup", "state", get_pl_lookup_state_string(state),
            "sql_cmd", ObProxyParserUtils::get_sql_cmd_name(s.trans_info_.sql_cmd_),
            "is trans specified", trans_specified,
            "has dependent func", is_dep_func,
            "is in trans", in_trans);

  return state;
}

bool ObMysqlTransact::is_trans_specified(ObTransState &s)
{
  return OB_UNLIKELY(NULL != s.sm_->client_session_
                     && !s.sm_->client_session_->is_session_pool_client()
                     && s.sm_->client_session_->get_session_info().is_trans_specified());
}

inline bool ObMysqlTransact::has_dependent_func(ObTransState &s)
{
  return OB_UNLIKELY(NULL != s.sm_->client_session_
                     && !s.sm_->client_session_->is_session_pool_client()
                     && (s.trans_info_.client_request_.get_parse_result().has_dependent_func()));
}

//二路路由条件：
// 1. 事务第一条SQL/事务自由路由的状态
// 2. 没有发生过二次路由
// 3. 不是大请求, 且请求小于 4K (因为重新这里会把 read_buffer 里的数据 consume 掉, 二次路由时会从 4K Buffer 里 copy 出来)
// 4. 如果是 EXECUTE 或 PREPARE_EXECUTE 请求, 不存在 pieceInfo 结构
// 5. 非指定 primary zone/IP 路由
inline bool ObMysqlTransact::is_need_reroute(ObMysqlTransact::ObTransState &s)
{
  int ret = OB_SUCCESS;
  int64_t total_request_packet_len = s.trans_info_.client_request_.get_packet_len();
  int64_t cached_request_packet_len = s.trans_info_.client_request_.get_req_pkt().length();
  bool is_need_reroute = false;
  bool is_weak_read = (WEAK == s.sm_->trans_state_.get_trans_consistency_level(s.sm_->get_client_session()->get_session_info()));

  // if enable_reroute, strong read req allowed to reroute
  // if enable_weak_reroute, weak read allowed to reroute
  is_need_reroute = ((!is_weak_read && s.mysql_config_params_->enable_reroute_) || (is_weak_read && s.mysql_config_params_->enable_weak_reroute_))
                    && '\0' == s.sm_->proxy_primary_zone_name_[0]
                    && !s.is_rerouted_ && s.is_need_pl_lookup()
                    && (s.is_trans_first_request_
                        || (ObMysqlTransact::is_in_trans(s)
                            && s.sm_->client_session_->is_trans_internal_routing()))
                    && !is_large_request(s)
                    && total_request_packet_len == cached_request_packet_len
                    && !s.use_cmnt_target_db_server_
                    && !s.use_conf_target_db_server_;


  if (is_need_reroute
      && (obmysql::OB_MYSQL_COM_STMT_PREPARE_EXECUTE == s.trans_info_.client_request_.get_packet_meta().cmd_
          || obmysql::OB_MYSQL_COM_STMT_EXECUTE == s.trans_info_.client_request_.get_packet_meta().cmd_)) {
    ObPieceInfo *info = NULL;
    ObClientSessionInfo &cs_info = s.sm_->client_session_->get_session_info();
    if (OB_FAIL(cs_info.get_piece_info(info))) {
      if (OB_HASH_NOT_EXIST != ret) {
        is_need_reroute = false;
      }
    } else {
      is_need_reroute = false;
    }
  }

  return is_need_reroute;
}

int ObMysqlTransact::extract_partition_info(ObTransState &s, int32_t &type)
{
  int ret = OB_SUCCESS;
  bool is_table_name_from_parser = false;
  bool is_package_name_from_parser = false;
  bool is_database_name_from_parser = false;
  ObClientSessionInfo &cs_info = get_client_session_info(s);

  // 1. get cluster_name
  ObString cluster_name = cs_info.get_priv_info().cluster_name_;
  // 2. get tenant_name
  ObString tenant_name = cs_info.get_priv_info().tenant_name_;
  // 3. get database_name and table_name;
  ObString table_name;
  ObString package_name;
  ObString database_name;

  // sys tenant just use __all_dummy entry, must not fetch table entry from remote
  if (OB_UNLIKELY(s.mysql_config_params_->is_mysql_routing_mode())) {
    table_name = OB_ALL_DUMMY_TNAME;
    database_name = OB_SYS_DATABASE_NAME;
  } else if (OB_UNLIKELY(s.sm_->client_session_->is_proxy_mysql_client_)) {
    if (tenant_name == OB_SYS_TENANT_NAME) {
      table_name = OB_ALL_DUMMY_TNAME;
      database_name = OB_SYS_DATABASE_NAME;
    } else {
      if ((NULL != s.sm_->sm_cluster_resource_)
          && !s.sm_->sm_cluster_resource_->is_default_cluster_resource()) {
        // normal case, fetch database and table from parse result
      } else {
        // default cluster resource can only used by sys tenant to init
        // real cluster resource, can not used by other tenant
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("default cluster resource can only used by sys tenant",
            K(tenant_name), KPC(s.sm_->sm_cluster_resource_));
      }
    }
  }

  if (OB_SUCC(ret) && OB_LIKELY(table_name.empty()) && OB_LIKELY(database_name.empty())) {
    ObSqlParseResult &parse_result = s.trans_info_.client_request_.get_parse_result();
    table_name = parse_result.get_table_name();

    if (OB_UNLIKELY(table_name.empty())) {
      // if table is empty , use it __all_dummy
      table_name = OB_ALL_DUMMY_TNAME;
      database_name = OB_SYS_DATABASE_NAME;

    } else if (OB_UNLIKELY(table_name[0] == '_' &&
               table_name == OB_ALL_DUMMY_TNAME)) {
      // if table is __all_dummy, just set the db to oceanbase
      database_name = OB_SYS_DATABASE_NAME;

    } else {
      is_table_name_from_parser = true;
      if (OB_LIKELY(parse_result.get_database_name().empty())) {
        if (OB_SUCCESS != cs_info.get_database_name(database_name)) {
          database_name = OB_SYS_DATABASE_NAME;
        }
        ObProxyParseString tmp_database_name;
        tmp_database_name.str_ = database_name.ptr();
        tmp_database_name.end_ptr_ = database_name.ptr() + database_name.length() - 1;
        tmp_database_name.str_len_ = database_name.length();
        tmp_database_name.quote_type_ = OBPROXY_QUOTE_T_INVALID;
        if (OB_FAIL(parse_result.set_db_name(tmp_database_name))) {
          LOG_WDIAG("parse result set db name failed", K(ret));
        }
      } else {
        is_database_name_from_parser = true;
      }

      if (OB_SUCC(ret)) {
        database_name = parse_result.get_database_name();
        package_name = parse_result.get_package_name();
        if (OB_UNLIKELY(!package_name.empty())) {
          is_package_name_from_parser = true;
        }

        // if run here, means table name and db name all come from parse result
        if (OB_UNLIKELY(s.sm_->client_session_->get_session_info().is_oracle_mode())) {
          if (is_table_name_from_parser && OBPROXY_QUOTE_T_INVALID == parse_result.get_table_name_quote()) {
            string_to_upper_case(table_name.ptr(), table_name.length());
          }
          if (is_database_name_from_parser && OBPROXY_QUOTE_T_INVALID == parse_result.get_database_name_quote()) {
            string_to_upper_case(database_name.ptr(), database_name.length());
          }
          if (is_package_name_from_parser && OBPROXY_QUOTE_T_INVALID == parse_result.get_package_name_quote()) {
            string_to_upper_case(package_name.ptr(), package_name.length());
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (IS_CLUSTER_VERSION_LESS_THAN_V4(s.sm_->sm_cluster_resource_->cluster_version_)
          && tenant_name == OB_SYS_TENANT_NAME && database_name == OB_SYS_DATABASE_NAME
          && !get_global_proxy_config().enable_qa_mode) {
        table_name = OB_ALL_DUMMY_TNAME;
      }
    }

    if (OB_UNLIKELY(s.mysql_config_params_->enable_index_route_
        && is_need_use_sql_table_cache(s)
        && table_name != OB_ALL_DUMMY_TNAME)) {
      ObString sql_id;
      ObString user_sql;

      ObProxyMysqlRequest &client_request = s.trans_info_.client_request_;
      ObClientSessionInfo &client_info = s.sm_->client_session_->get_session_info();
      if (obmysql::OB_MYSQL_COM_STMT_EXECUTE == client_request.get_packet_meta().cmd_
          || obmysql::OB_MYSQL_COM_STMT_SEND_LONG_DATA == client_request.get_packet_meta().cmd_) {
        if (OB_FAIL(client_info.get_ps_sql(user_sql))) {
          LOG_WDIAG("fail to get ps sql", K(ret));
          ret = OB_SUCCESS;
        }
      } else if (parse_result.is_text_ps_execute_stmt()) {
        if (OB_FAIL(client_info.get_text_ps_sql(user_sql))) {
          LOG_WDIAG("fail to get text ps sql", K(ret));
          ret = OB_SUCCESS;
        }
      } else {
        user_sql = client_request.get_sql();
      }

      // parse sql id
      if (OB_FAIL(ObMysqlRequestAnalyzer::analyze_sql_id(user_sql, s.trans_info_.client_request_, sql_id))) {
        LOG_WDIAG("fail to analyze sql id", K(ret));
      } else if (!sql_id.empty()) {
        ObSqlTableCache &sql_table_cache = get_global_sql_table_cache();
        // get real table name from ObSqlTableCache
        ObSqlTableEntryKey key;
        key.cr_version_ = s.sm_->sm_cluster_resource_->version_;
        key.cr_id_ = s.sm_->sm_cluster_resource_->get_cluster_id();
        key.sql_id_ = sql_id;
        key.cluster_name_ = cluster_name;
        key.tenant_name_ = tenant_name;
        key.database_name_ = database_name;
        char table_name_buf[OB_MAX_TABLE_NAME_LENGTH + 1];
        table_name_buf[0] = '\0';
        if (OB_FAIL(sql_table_cache.get_table_name(key, table_name_buf, OB_MAX_TABLE_NAME_LENGTH + 1))) {
          if (OB_ENTRY_NOT_EXIST == ret && is_table_name_from_parser) {
            LOG_DEBUG("first sql got, will add sql table cache", K(key),
                      "sql", s.trans_info_.client_request_.get_print_sql());
            ObSqlTableEntry *entry = NULL;
            if (OB_FAIL(ObSqlTableEntry::alloc_and_init_sql_table_entry(key, table_name, entry))) {
              LOG_WDIAG("fail to alloc ObSqlTableEntry", K(key), K(table_name), K(ret));
            } else if (OB_FAIL(sql_table_cache.add_sql_table_entry(entry))) {
              LOG_WDIAG("fail to add ObSqlTableEntry", KPC(entry), K(ret));
            } else {
              LOG_INFO("succ to add sql table entry", K(key), KPC(entry), K(table_name));
            }
            if (NULL != entry) {
              // entry->inc_ref will been called if succ to add
              entry->dec_ref();
              entry = NULL;
            }
          } else {
            LOG_WDIAG("fail to get table name from sql table cache", K(key), K(ret),
                     "sql", s.trans_info_.client_request_.get_print_sql());
          }
        } else if (OB_FAIL(parse_result.set_real_table_name(table_name_buf, strlen(table_name_buf)))) {
          LOG_WDIAG("fail to set real table name", K(table_name_buf), K(ret));
        } else {
          table_name = parse_result.get_table_name();
          LOG_DEBUG("succ to get real table name", K(table_name));
          type = ObDiagnosisRouteInfo::USE_GLOBAL_INDEX;
          if (OB_NOT_NULL(s.sm_->route_diagnosis_) && s.sm_->route_diagnosis_->is_diagnostic(SQL_PARSE)) {
            ObDiagnosisSqlParse *sql_parse = (ObDiagnosisSqlParse *) s.sm_->route_diagnosis_->get_last_pushed_diagnosis_point(SQL_PARSE);
            if (OB_NOT_NULL(sql_parse)) {
              sql_parse->table_ = table_name;
            }
          }
        }
      }
      if (OB_FAIL(ret)) {
        ret = OB_SUCCESS; // ignore ret, just use table name from sql parser
      }
    } // end if need_reroute
  }

  ObTableEntryName &te_name = s.pll_info_.te_name_;
  if (OB_SUCC(ret)) {
    te_name.cluster_name_ = cluster_name;
    te_name.tenant_name_ = tenant_name;
    te_name.database_name_ = database_name;
    te_name.package_name_ = package_name;
    te_name.table_name_ = table_name;
  }

  // print info
  LOG_DEBUG("current extracted table entry name", "table_entry_name", te_name, K(ret));
  if (OB_FAIL(ret) || OB_UNLIKELY(!te_name.is_valid())) {
    LOG_WDIAG("fail to extract_partition_info", K(te_name), K(ret));
    te_name.reset();
  }
  if (table_name.empty() || te_name.is_all_dummy_table()) {
    type = ObDiagnosisRouteInfo::USE_ROUTE_POLICY;
  }
  return ret;
}

inline void ObMysqlTransact::setup_plugin_request_intercept(ObTransState &s)
{
  if (OB_ISNULL(s.sm_->api_.plugin_tunnel_)) {
    LOG_EDIAG("invalid internal state", "plugin_tunnel", s.sm_->api_.plugin_tunnel_);
    TRANSACT_RETURN_WITH_MSG(SM_ACTION_SEND_ERROR_NOOP, NULL);
  } else {
    // Plugin is intercepting the request which means
    // that we don't do partition location lookup.
    // We just want to write the request straight to the plugin

    s.current_.attempts_ = 1;

    TRANSACT_RETURN(SM_ACTION_OBSERVER_OPEN, NULL);
  }
}

// Called after an API function indicates it wished to send
// an error to the client
void ObMysqlTransact::handle_api_error_jump(ObTransState &s)
{
  LOG_DEBUG("[ObMysqlTransact::handle_api_error_jump]");
  s.source_ = SOURCE_INTERNAL;

  TRANSACT_RETURN(SM_ACTION_SEND_ERROR_NOOP, NULL);
}

void ObMysqlTransact::handle_pl_update(ObTransState &s)
{
  s.pll_info_.pl_update_if_necessary(s);
}

inline bool ObMysqlTransact::ObPartitionLookupInfo::need_update_entry_by_partition_hit()
{
  return (need_update_entry()
          && !route_.is_dummy_table()
          && !route_.no_need_pl_update_);
}

int64_t ObMysqlTransact::ObPartitionLookupInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(pl_attempts),
       K_(lookup_success),
       K_(force_renew_state),
       K_(cached_dummy_entry_renew_state),
       K_(te_name),
       K_(route));
  J_OBJ_END();
  return pos;
}

inline void ObMysqlTransact::ObPartitionLookupInfo::pl_update_for_reroute(const ObTransState &s)
{
  if (OB_LIKELY(NULL != s.mysql_config_params_)) {
    const bool need_update = need_update_entry_by_partition_hit(); // 如果是分区表，需要 part_entry != null 这个条件
    const bool is_empty_entry_allowed = route_.is_empty_entry_allowed();
    const bool has_table_but_no_leader = (!route_.is_no_route_info_found() && !is_leader_existent());
    const bool is_oracle_mode = s.sm_->client_session_->get_session_info().is_oracle_mode();
    ObTableEntry *table_entry = route_.get_table_entry();
    bool is_table_name_changed = false;
    bool is_schema_version_changed = false;
    if (NULL != table_entry) {
      if (is_oracle_mode) {
        is_table_name_changed = table_entry->get_table_name() != s.reroute_info_.table_name_buf_;
      } else {
        is_table_name_changed = table_entry->get_table_name().case_compare(s.reroute_info_.table_name_buf_) != 0;
      }
      is_schema_version_changed = !is_table_name_changed
                                  && table_entry->get_schema_version() != s.reroute_info_.schema_version_;
    }
    // 1. current table entry schema version changed
    if (is_schema_version_changed) {
      LOG_INFO("schema version changed, will set table entry dirty",
               "origin_name", te_name_, "route_info", route_,
               "old schema version", table_entry->get_schema_version(),
               "new schema version", s.reroute_info_.schema_version_);
      if (route_.set_table_entry_dirty()) {
        get_pl_task_flow_controller().handle_new_task();
      }
    } else if ((s.mysql_config_params_->is_standard_routing_mode()) && need_update && !is_empty_entry_allowed) {
      // we will update pl for reroute error when:
      // 1. strong read, but current table leader return reroute
      //or
      // 2. strong read, but leader do not exist
      if (route_.is_strong_read()) {
        const bool is_leader = is_leader_server();
        if ((is_leader && !is_table_name_changed)
            || has_table_but_no_leader) {
          LOG_INFO("will set strong read route dirty", K(is_leader), K(has_table_but_no_leader),
                   "origin_name", te_name_, "route_info", route_);
          if (route_.set_target_dirty()) {
            get_pl_task_flow_controller().handle_new_task();
          }
        }
      } else if (route_.is_weak_read()) {
        // do nothing, weak read reroute will be updated by sys variable _ob_proxy_weakread_feedback
      }
    }
  }
}

inline void ObMysqlTransact::ObPartitionLookupInfo::pl_update_if_necessary(const ObTransState &s)
{
  if (OB_LIKELY(NULL != s.mysql_config_params_)) {
    const bool is_partition_hit = s.trans_info_.server_response_.get_analyze_result().is_partition_hit();
    const bool need_update = need_update_entry_by_partition_hit();
    const bool is_empty_entry_allowed = route_.is_empty_entry_allowed();

    bool is_read_weak_supported = true;
    if (NULL == s.sm_->client_session_) {
      // if client session disconnect, can not access te_name_
      te_name_.reset();
    } else {
      is_read_weak_supported = s.sm_->client_session_->get_session_info().is_server_support_read_weak();
    }

    // print log
    LOG_DEBUG("[ObMysqlTransact::ObPartitionLookupInfo::pl_update_if_necessary]",
              K(is_partition_hit),
              "state", ObMysqlTransact::get_server_state_name(s.current_.state_),
              "origin_name", te_name_,
              "is_in_standard_routing_mode", s.mysql_config_params_->is_standard_routing_mode(),
               K(need_update),
               K(is_empty_entry_allowed),
              "route_info", route_);

    if ((s.mysql_config_params_->is_standard_routing_mode()) && need_update && !is_empty_entry_allowed) {
      // we will update pl when:
      // 1. strong read, but leader return miss, or follower return hit
      //or
      // 2. strong read, but leader do not exist
      //or
      // 3. weak read, but calculate server return miss or tenant server return hit
      // besides, if we go with route optimize procedure, do not need pl update here
      if (route_.is_strong_read()) {
        const bool is_leader = is_leader_server();
        const bool has_table_but_no_leader = (!route_.is_no_route_info_found() && !is_leader_existent());
        if (is_leader != is_partition_hit || has_table_but_no_leader) {
          LOG_INFO("will set strong read route dirty", K(is_leader), K(is_partition_hit), K(has_table_but_no_leader),
                "origin_name", te_name_, "route_info", route_);
          //NOTE:: if leader was congested from server, it will set_dirty in ObMysqlTransact::handle_congestion_control_lookup
          //       here we only care response
          if (route_.set_target_dirty()) {
            get_pl_task_flow_controller().handle_new_task();
          }
        } else if (is_leader && is_partition_hit) {
          // this route entry is valid, renew last_valid_time;
          route_.renew_last_valid_time();
        }
      } else if (route_.is_weak_read()) {
        const bool is_target_server = is_target_location_server();
        const ObWeakReadHitReplica hit_replica = s.trans_info_.server_response_.get_analyze_result().get_weak_read_hit_replica();
        //TODO::here proxy only calculate one table entry, it is not accuracy.
        //      it may partition miss when use target_server under multi table. @gujian
        const bool update_by_not_hit = (is_target_server && !is_partition_hit && is_read_weak_supported);
        const bool update_by_not_miss = (!is_target_server && is_partition_hit);
        const bool update_by_leader_dead = (route_.is_follower_first_policy() && route_.is_leader_force_congested());
        const bool update_by_hit_all_leader = (is_target_server && !s.pll_info_.route_.is_leader_server() && hit_replica == ALL_LEADER_REPLICA);
        if (update_by_hit_all_leader || update_by_not_hit || update_by_not_miss || update_by_leader_dead) {

          bool is_need_force_flush = update_by_not_hit && route_.is_remote_readonly();

          LOG_INFO("will set weak read route dirty", K(is_target_server),
                   K(is_partition_hit), K(is_read_weak_supported),
                   K(update_by_leader_dead), K(update_by_hit_all_leader),
                   K(is_need_force_flush), "origin_name", te_name_,
                   "route_info", route_, "hit_replica", hit_replica);

          if (route_.set_target_dirty(is_need_force_flush)) {
            get_pl_task_flow_controller().handle_new_task();
          }
        }
      } else {
        LOG_EDIAG("it should never arriver here", "origin_name", te_name_, "route_info", route_);
      }
    }
  }
}

int ObMysqlTransact::ObPartitionLookupInfo::get_next_avail_replica(const bool is_force_retry,
                                                                    int32_t &attempt_count,
                                                                    bool &found_leader_force_congested,
                                                                    ObReadStaleParam &read_stale_param,
                                                                    bool &is_all_stale,
                                                                    const ObProxyReplicaLocation *&replica)

{
  int ret = OB_SUCCESS;
  attempt_count = 0;
  found_leader_force_congested = false;
  is_all_stale = false;
  int weak_read_count = 0;
  bool need_try_next = true;
  const ObProxyReplicaLocation *ret_replica = NULL;

  while (need_try_next) {
    ret_replica = route_.get_next_avail_replica();
    bool is_replica_avail = true;
    if (!is_force_retry && is_weak_read() && NULL != ret_replica && read_stale_param.enable_read_stale_feedback_) {
      // read staled and congested replica treaded as a weak read unavailable replica
      read_stale_param.server_addr_.assign(ret_replica->server_.get_sockaddr());
      ObReadStaleProcessor &read_stale_processor = get_global_read_stale_processor();
      bool is_stale = false;
      if (OB_FAIL(read_stale_processor.check_read_stale_state(read_stale_param, is_stale))){
        need_try_next = false;
        is_replica_avail = false;
        LOG_WDIAG("fail to check replica read stale stale", K(ret));
      } else if (is_stale) {
        PROXY_TXN_LOG(DEBUG, "the replica is stale on weak read, do not use it this time",
                      "server", route_.cur_chosen_server_, K(attempt_count));
        ret_replica = NULL;
        is_replica_avail = false;
        ++weak_read_count;
        ++attempt_count;
      } else if (route_.cur_chosen_server_.is_force_congested_) {
        PROXY_TXN_LOG(DEBUG, "the replica is congested on weak read request, do not use it this time",
                      "server", route_.cur_chosen_server_, K(attempt_count));
        ret_replica = NULL;
        is_replica_avail = false;
        ++weak_read_count;
        ++attempt_count;
      }
    } else if (!is_force_retry
        && NULL != ret_replica
        && route_.cur_chosen_server_.is_force_congested_) {
      if (ret_replica->is_leader() && is_strong_read()) {
        found_leader_force_congested = true;
      }
      PROXY_TXN_LOG(DEBUG, "this is force congested server, do not use it this time",
                    "server", route_.cur_chosen_server_, K(attempt_count));
      //if not force retry, we will do not use dead congested server
      ret_replica = NULL;
      is_replica_avail = false;
      ++attempt_count;
    }

    if (is_replica_avail) {
      need_try_next = false;
    }
  }
  if (OB_SUCC(ret)) {
    replica = ret_replica;
    if (NULL == ret_replica && is_all_iterate_once()) {
      is_all_stale = (0 != attempt_count && weak_read_count == attempt_count);
    }
  } else {
    ret_replica = NULL;
  }

  return ret;
}

inline void ObMysqlTransact::handle_internal_request(ObTransState &s)
{
  LOG_DEBUG("[ObMysqlTransact::handle_internal_request] "
            "fail to do internal request, will disconnect");
  s.free_internal_buffer();
  TRANSACT_RETURN_WITH_MSG(SM_ACTION_SEND_ERROR_NOOP, NULL);
}

void ObMysqlTransact::handle_congestion_control_lookup(ObTransState &s)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
    s.sm_->milestones_.congestion_control_end_ = get_based_hrtime(s);
    s.sm_->cmd_time_stats_.congestion_control_time_ +=
      milestone_diff(s.sm_->milestones_.congestion_control_begin_, s.sm_->milestones_.congestion_control_end_);

    s.sm_->milestones_.congestion_process_begin_ = s.sm_->milestones_.congestion_control_end_;
    s.sm_->milestones_.congestion_process_end_ = 0;
  }

  if (OB_LIKELY(!s.need_congestion_lookup_)) { // no need congestion lookup
    start_access_control(s); // continue
    ROUTE_DIAGNOSIS(s.sm_->route_diagnosis_,
                    CONGESTION_CONTROL,
                    congestion_control,
                    ret, false, false, false,
                    s.force_retry_congested_,
                    s.need_congestion_lookup_,
                    s.congestion_lookup_success_,
                    false,
                    s.server_info_.addr_);
  } else {
    if (!s.congestion_lookup_success_) { // congestion entry lookup failed
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("[ObMysqlTransact::handle_congestion_control_lookup] "
               "fail to lookup congestion entry, will disconnect");
      ROUTE_DIAGNOSIS(s.sm_->route_diagnosis_,
                      CONGESTION_CONTROL,
                      congestion_control,
                      ret, false, false, false,
                      s.force_retry_congested_,
                      s.need_congestion_lookup_,
                      s.congestion_lookup_success_,
                      false,
                      s.server_info_.addr_);
    } else {
      bool is_in_alive_congested = false;
      bool is_in_dead_congested = false;
      bool is_in_detect_congested = false;
      ObCongestionEntry *cgt_entry= s.congestion_entry_;
      if (NULL == cgt_entry) {
        ROUTE_DIAGNOSIS(s.sm_->route_diagnosis_,
                        CONGESTION_CONTROL,
                        congestion_control,
                        ret, false, false, false,
                        s.force_retry_congested_,
                        s.need_congestion_lookup_,
                        s.congestion_lookup_success_,
                        false,
                        s.server_info_.addr_);
        ObProxyMutex *mutex_ = s.sm_->mutex_;
        CONGEST_INCREMENT_DYN_STAT(dead_congested_stat);
        s.current_.state_ = ObMysqlTransact::DEAD_CONGESTED;
        handle_congestion_entry_not_exist(s);
        handle_response(s);

      } else {
        is_in_dead_congested = cgt_entry->is_dead_congested();
        is_in_detect_congested = cgt_entry->is_detect_congested();
        if (!s.force_retry_congested_
            && is_in_dead_congested
            && s.pll_info_.is_strong_read()
            && s.pll_info_.is_leader_server()) {
          //only table entry is newer then congestion entry revalidate_time, we can treat it force_not_dead_congested
          if (cgt_entry->is_server_replaying()
              && cgt_entry->get_last_revalidate_time_us() <= s.pll_info_.get_last_valid_time_us()) {
            LOG_DEBUG("leader server is in REPLAY state, treat it not dead congested",
                      KPC(cgt_entry), "pll info", s.pll_info_);
            is_in_dead_congested = false;
          } else {
            ObProxyMutex *mutex_ = s.sm_->mutex_; // for stat
            PROCESSOR_INCREMENT_DYN_STAT(UPDATE_ROUTE_ENTRY_BY_CONGESTION);
            // when leader first router && (leader was congested from server or dead congested),
            // we need set forceUpdateTableEntry to avoid senseless of leader migrate
            if (s.pll_info_.set_target_dirty()) {
              LOG_INFO("leader is force_congested in strong read, set it to dirty "
                       "and wait for updating", "addr", s.server_info_.addr_,
                       "route", s.pll_info_.route_);
            }
          }
        }

        if (is_in_detect_congested) {
          get_global_resource_pool_processor().ip_set_.set_refactored(cgt_entry->server_ip_);
        } else if (is_in_dead_congested) {
          //do nothing
        } else if (cgt_entry->is_force_alive_congested()) {
          is_in_alive_congested = true;
        } else if (cgt_entry->is_alive_congested()) {
          // Attention: if server is alive congested and need retry, we can retry it for this connection
          // and should update last congested time as punishment, otherwise leader
          // may be retried too many times in a short time if it is really blocked.
          // no need set is_congestion_entry_updated_ = true here,
          // if it is really alive, we will set entry alive after transaction is finished
          if (cgt_entry->alive_need_retry(get_hrtime())) {
            LOG_INFO("we can congested server for this connection,"
                     "and will expand its retry interval to avoid other connections use this server",
                     KPC(cgt_entry));
            cgt_entry->set_alive_failed_at(get_hrtime());
            if (get_global_proxy_config().server_detect_mode != 0) {
              if (OB_LIKELY(NULL != s.sm_->sm_cluster_resource_)) {
                s.sm_->sm_cluster_resource_->alive_addr_set_.set_refactored(cgt_entry->server_ip_);
              }
              is_in_alive_congested = true;
            }
          } else {
            is_in_alive_congested = true;
          }
        }

        if (is_in_alive_congested || is_in_dead_congested || is_in_detect_congested) {
          LOG_DEBUG("target server is in congested",
                    "addr", s.server_info_.addr_,
                    K(is_in_dead_congested),
                    K(is_in_alive_congested),
                    K(is_in_detect_congested),
                    "force_retry_congested", s.force_retry_congested_,
                    KPC(cgt_entry),
                    "pll info", s.pll_info_);
        }
        ROUTE_DIAGNOSIS(s.sm_->route_diagnosis_,
                        CONGESTION_CONTROL,
                        congestion_control,
                        ret, is_in_alive_congested, is_in_dead_congested, is_in_detect_congested,
                        s.force_retry_congested_, s.need_congestion_lookup_, s.congestion_lookup_success_,
                        true, s.server_info_.addr_);
        // 1. if the server is in congestion list and dead, we treat it as server_dead_fail
        // 2. if the server we chosen is not in congestion list, we also treat it as server_dead_fail
        if (!s.force_retry_congested_ && is_in_dead_congested) {
          ObProxyMutex *mutex_ = s.sm_->mutex_;
          CONGEST_INCREMENT_DYN_STAT(dead_congested_stat);
          s.current_.state_ = ObMysqlTransact::DEAD_CONGESTED;
          handle_response(s);
        } else if (!s.force_retry_congested_ && is_in_alive_congested) {
          ObProxyMutex *mutex_ = s.sm_->mutex_;
          CONGEST_INCREMENT_DYN_STAT(alive_congested_stat);
          s.current_.state_ = ObMysqlTransact::ALIVE_CONGESTED;
          handle_response(s);
        } else if (!s.force_retry_congested_ && is_in_detect_congested) {
          s.current_.state_ = ObMysqlTransact::DETECT_CONGESTED;
          handle_response(s);
        } else {
          start_access_control(s); // continue
        }
      }
    }
  }

  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
    s.sm_->milestones_.congestion_process_end_ = get_based_hrtime(s);
    s.sm_->cmd_time_stats_.congestion_process_time_ +=
      milestone_diff(s.sm_->milestones_.congestion_process_begin_, s.sm_->milestones_.congestion_process_end_);
  }

  if (OB_FAIL(ret)) {
    s.inner_errcode_ = ret;
    // disconnect
    TRANSACT_RETURN_WITH_MSG(SM_ACTION_SEND_ERROR_NOOP, NULL);
  }
}

void ObMysqlTransact::handle_congestion_entry_not_exist(ObTransState &s)
{
  if (NULL == s.congestion_entry_) { // not in server list
    LOG_INFO("congestion entry does not exist", "addr", s.server_info_.addr_, K_(s.pll_info));
    s.congestion_entry_not_exist_count_++;
    // 1. mark entry dirty
    if (s.pll_info_.set_dirty_all()) {
      //if dummy/target entry has clipped, we can update it here
      LOG_INFO("congestion entry doesn't exist, some route entry is expired, set it to dirty,"
               " and wait for updating", "congestion_entry_not_exist_count",
               s.congestion_entry_not_exist_count_,
               "addr", s.server_info_.addr_,
               "pll_info", s.pll_info_);
    }

    // 2. if all servers of the table entry are not in server list, just force renew
    // if dummy entry from rslist, no need force renew, will disconnect and delete cluster resource
    int64_t replica_count = s.pll_info_.replica_size();
    if ((replica_count > 0)
        && (s.congestion_entry_not_exist_count_ == replica_count)
        && !s.pll_info_.is_server_from_rslist()) {
      LOG_INFO("all servers of the route are not in server list, will force renew",
                K(replica_count), "congestion_entry_not_exist_count",
                s.congestion_entry_not_exist_count_, "route info", s.pll_info_.route_);
      s.pll_info_.set_force_renew(); // force renew
    }
  }
}

void ObMysqlTransact::modify_pl_lookup(ObTransState &s)
{
  if (OB_UNLIKELY(!s.pll_info_.is_cached_dummy_force_renew())) {
    LOG_WDIAG("[modify_pl_lookup] it should arrive here, will disconnect",
             "origin_name", s.pll_info_.te_name_, "route info", s.pll_info_.route_);
    TRANSACT_RETURN_WITH_MSG(SM_ACTION_SEND_ERROR_NOOP, NULL);
  } else {
    LOG_DEBUG("[modify_pl_lookup] we need do pl lookup for partition table",
             "origin_name", s.pll_info_.te_name_);
    TRANSACT_RETURN(SM_ACTION_PARTITION_LOCATION_LOOKUP, ObMysqlTransact::handle_pl_lookup);
  }
}

void ObMysqlTransact::check_safe_read_snapshot(ObTransState &s)
{
#if OB_DETAILED_SLOW_QUERY
  ObHRTime t1 = common::get_hrtime_internal();
#endif
  // s.sm_->sm_cluster_resource_ and s.sm_->client_session_ is not null
  int64_t max_read_snapshot = 0;
  ObAddr max_read_snapshot_addr;
  ObSafeSnapshotManager &safe_snapshot_manager =
    s.sm_->sm_cluster_resource_->safe_snapshot_mgr_;
  ObSafeSnapshotEntry *entry = NULL;
  const ObProxyPartitionLocation *pl = s.pll_info_.route_.cur_chosen_pl_;
  ObAddr addr;
  addr.set_sockaddr(s.server_info_.addr_.sa_);
  if (OB_ISNULL(pl) || pl->is_server_changed()) {
    // get the max snapshot server of all server
    int64_t count = s.sm_->client_session_->dummy_ldc_.count();
    const ObLDCItem *item_array = s.sm_->client_session_->dummy_ldc_.get_item_array();
    for (int64_t i = 0; i < count; ++i) {
      if (OB_ISNULL(item_array[i].replica_)) {
        LOG_WDIAG("item_array[i].replica_ is null, ignore it");
      } else {
        entry = safe_snapshot_manager.get(item_array[i].replica_->server_);
        if (OB_ISNULL(entry)) {
          LOG_INFO("entry is null, maybe new server added", K(item_array[i].replica_->server_));
        } else {
          if (max_read_snapshot < entry->get_safe_read_snapshot()) {
            max_read_snapshot = entry->get_safe_read_snapshot();
            max_read_snapshot_addr = entry->get_addr();
          }
        }
      }
    }
  } else {
    // get the max snapshot server of partition server
    ObProxyReplicaLocation *tmp_replica = NULL;
    for (int64_t i = 0; i < pl->replica_count(); ++i) {
      tmp_replica = pl->get_replica(i);
      if (OB_ISNULL(tmp_replica)) {
        LOG_WDIAG("replica is null, unexpected", K(i));
      } else {
        entry = safe_snapshot_manager.get(tmp_replica->server_);
        if (OB_ISNULL(entry)) {
          LOG_INFO("entry is null, maybe new server added", K(tmp_replica->server_));
        } else {
          if (max_read_snapshot < entry->get_safe_read_snapshot()) {
            max_read_snapshot = entry->get_safe_read_snapshot();
            max_read_snapshot_addr = entry->get_addr();
          }
        }
      }
    }
  }

  // if choosen server is not the max safe read snapshot addr, we should sync safe read snapshot
  entry = safe_snapshot_manager.get(addr);
  bool need_sync_safe_snapshot_addr = true;
  if (OB_ISNULL(entry)) {
    LOG_INFO("entry is null, maybe new server added, will sync safe_snapshot", K(addr));
    need_sync_safe_snapshot_addr = true;
  } else if (max_read_snapshot == 0) {
    // if max_read_snapshot == 0
    // we should not sync the variables
    need_sync_safe_snapshot_addr = false;
  } else if (max_read_snapshot == entry->get_safe_read_snapshot()
             && !entry->need_force_sync()) {
    // if current is the max safe snapshot and we need not force sync
    // we should not sync the variables
    need_sync_safe_snapshot_addr = false;
  } else {
    LOG_DEBUG("session switched or force sync, will sync safe_snapshot",
              K(max_read_snapshot), K(addr), KPC(entry));
    need_sync_safe_snapshot_addr = true;
  }

  ObClientSessionInfo &client_info = s.sm_->client_session_->get_session_info();
  if (need_sync_safe_snapshot_addr) {
    client_info.set_syncing_safe_read_snapshot(max_read_snapshot);
  } else {
    // will not sync
    client_info.set_syncing_safe_read_snapshot(0);
  }
#if OB_DETAILED_SLOW_QUERY
  ObHRTime t2 = common::get_hrtime_internal();
  s.sm_->cmd_time_stats_.debug_check_safe_snapshot_time_ += (t2 -t1);
#endif
}

void ObMysqlTransact::get_region_name_and_server_info(ObTransState &s,
                                                      ObIArray<ObServerStateSimpleInfo> &simple_servers_info,
                                                      ObIArray<ObString> &region_names)
{
  int ret = OB_SUCCESS;

  const uint64_t ss_version = s.sm_->sm_cluster_resource_->server_state_version_;
  ObIArray<ObServerStateSimpleInfo> &server_state_info = s.sm_->sm_cluster_resource_->get_server_state_info(ss_version);
  common::DRWLock &server_state_lock = s.sm_->sm_cluster_resource_->get_server_state_lock(ss_version);
  int err_no = 0;
  if (0 != (err_no = server_state_lock.try_rdlock())) {
    LOG_WDIAG("fail to tryrdlock server_state_lock, ignore", K(ss_version), K(err_no));
  } else {
    if (OB_FAIL(simple_servers_info.assign(server_state_info))) {
      LOG_WDIAG("fail to assign simple_servers_info", K(ret));
    }
    server_state_lock.rdunlock();

    if (OB_SUCC(ret)) {
      const ObString &idc_name = s.sm_->client_session_->get_current_idc_name();
      const ObString &cluster_name = s.sm_->sm_cluster_resource_->get_cluster_name();
      const int64_t cluster_id = s.sm_->sm_cluster_resource_->get_cluster_id();
      ObProxyNameString region_name_from_idc_list;
      ObLDCLocation::ObRegionMatchedType match_type = ObLDCLocation::MATCHED_BY_NONE;

      if (OB_FAIL(ObLDCLocation::get_region_name(simple_servers_info, idc_name,
                                                 cluster_name, cluster_id,
                                                 region_name_from_idc_list,
                                                 match_type, region_names))) {
        LOG_WDIAG("fail to get region name", K(idc_name), K(ret));
      }
    }
  }
}

void ObMysqlTransact::handle_pl_lookup(ObTransState &s)
{
  int ret = OB_SUCCESS;
  ++s.pll_info_.pl_attempts_;
  #ifdef ERRSIM
  if (OB_FAIL(OB_E(EventTable::EN_HANDLE_PL_LOOKUP_FAIL) OB_SUCCESS)) {
    TRANSACT_RETURN_WITH_MSG(SM_ACTION_SEND_ERROR_NOOP, NULL);
    return;
  }
  #endif

  if (s.sm_->enable_record_full_link_trace_info()) {
    trace::ObSpanCtx *ctx = s.sm_->flt_.trace_log_info_.partition_location_lookup_ctx_;
    if (OB_NOT_NULL(ctx)) {
      // set show trace buffer before flush trace
      if (s.sm_->flt_.control_info_.is_show_trace_enable()) {
        SET_SHOW_TRACE_INFO(&s.sm_->flt_.show_trace_json_info_.curr_sql_json_span_array_);
      }
      LOG_DEBUG("end span ob_proxy_partition_location_lookup", K(ctx->span_id_), K(s.sm_->flt_.span_info_.trace_id_),
                K(s.sm_->flt_.control_info_.is_show_trace_enable()));
      SET_TRACE_BUFFER(s.sm_->flt_trace_buffer_, MAX_TRACE_LOG_SIZE);
      FLT_END_SPAN(ctx);
      s.sm_->flt_.trace_log_info_.partition_location_lookup_ctx_ = NULL;
    }
  }

  if (OB_UNLIKELY(!s.pll_info_.lookup_success_)) { // partition location lookup failed
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("[ObMysqlTransact::handle_pl_lookup] "
             "fail to lookup partition location, will disconnect", K(ret));
  } else {
    // ok, and the partition location lookup succeeded
    LOG_DEBUG("[ObMysqlTransact::handle_pl_lookup] Partition location Lookup successful",
              "pl_attempts", s.pll_info_.pl_attempts_);
    const bool is_server_addr_set = s.server_info_.addr_.is_valid();
    if (OB_UNLIKELY(s.api_server_addr_set_ && !is_server_addr_set)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("[ObMysqlTransact::handle_pl_lookup] "
               "observer ip/port doesn't set correctly, will disconnect");
    } else if (OB_ISNULL(s.sm_->client_session_)
          || OB_ISNULL(s.mysql_config_params_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("[ObMysqlTransact::handle_pl_lookup] "
               "it should not arrive here, will disconnect", K(ret));
    } else if (OB_UNLIKELY(is_server_addr_set
                           && (s.mysql_config_params_->is_mysql_routing_mode()
                               || (s.mysql_config_params_->is_mock_routing_mode()
                                   && !s.sm_->client_session_->is_proxy_mysql_client_)))) {
      LOG_DEBUG("[ObMysqlTransact::handle_pl_lookup] use mysql route mode or mock mode and server addr is set, "
                "use cached session",
                "addr", s.server_info_.addr_,
                "attempts", s.current_.attempts_,
                "sm_id", s.sm_->sm_id_);
    } else if (OB_UNLIKELY(is_server_addr_set
                           && (s.use_cmnt_target_db_server_ || s.use_conf_target_db_server_))) {
      if (OB_NOT_NULL(s.sm_->client_session_->dummy_entry_)
          && s.sm_->client_session_->dummy_entry_->is_avail_state()
          && s.sm_->client_session_->dummy_entry_->is_valid()
          && is_addr_logonly(s.server_info_.addr_, s.sm_->client_session_->dummy_entry_->get_tenant_servers())) {
        LOG_WDIAG("Not allow to route to target db server", "which is logonly replica", s.server_info_.addr_, K(ret));
        ret = OB_OP_NOT_ALLOW;
        handle_target_db_not_allow(s);
      } else {
        LOG_DEBUG("[ObMysqlTransact::handle_pl_lookup] use target db server and server addr is set, "
                "use cached session",
                "addr", s.server_info_.addr_,
                "attempts", s.current_.attempts_,
                "sm_id", s.sm_->sm_id_);
      }
    } else if (is_server_addr_set) {
      LOG_DEBUG("server addr is set, use cached session",
                "addr", s.server_info_.addr_,
                "attempts", s.current_.attempts_,
                "sm_id", s.sm_->sm_id_);
    } else {
      const ObProxyReplicaLocation *replica = NULL;
#if OB_DETAILED_SLOW_QUERY
      ObHRTime t1 = get_based_hrtime(s);
#endif
      const ObConsistencyLevel consistency_level = s.get_trans_consistency_level(s.sm_->client_session_->get_session_info());
#if OB_DETAILED_SLOW_QUERY
      ObHRTime t2 = get_based_hrtime(s);
      s.sm_->cmd_time_stats_.debug_consistency_time_ += milestone_diff(t1, t2);
      t1 = t2;
#endif
      bool use_dup_replica = need_use_dup_replica(consistency_level, s);
      bool fill_addr = false;
      ObString pz_str;
      ObLocationTenantInfo *info = NULL;
      if (OB_NOT_NULL(s.sm_->route_diagnosis_) &&
          s.sm_->route_diagnosis_->is_diagnostic(ROUTE_POLICY) &&
          OB_NOT_NULL(s.sm_->sm_cluster_resource_)) {
        int tmp_ret = ret;
        ObString &tenant_name = s.sm_->client_session_->get_session_info().get_priv_info().tenant_name_;
        if (OB_SUCCESS != (tmp_ret = s.sm_->sm_cluster_resource_->get_location_tenant_info(tenant_name, info))) {
          LOG_WDIAG("fail to get primary zone", K(ret));
        } else if (OB_NOT_NULL(info)) {
          pz_str = info->primary_zone_;
        }
      }

      ObString zone(s.sm_->proxy_primary_zone_name_);
      // fill part of diagnositc info of ldc route
      ROUTE_DIAGNOSIS(s.sm_->route_diagnosis_,
                      ROUTE_POLICY,
                      route_policy,
                      ret,
                      use_dup_replica, false, false, false,
                      s.mysql_config_params_->proxy_idc_name_,
                      pz_str,
                      s.sm_->proxy_primary_zone_name_,
                      INVALID_CONSISTENCY, consistency_level,
                      MAX_ROUTE_POLICY_COUNT, MAX_ROUTE_POLICY_COUNT,
                      MAX_ROUTE_POLICY_COUNT, MAX_ROUTE_POLICY_COUNT,
                      s.pll_info_.route_.cur_chosen_server_,
                      s.pll_info_.route_.cur_chosen_route_type_);
      ObDiagnosisRoutePolicy *diagnosis_route_policy = OB_NOT_NULL(s.sm_->route_diagnosis_) ?
        reinterpret_cast<ObDiagnosisRoutePolicy*>(s.sm_->route_diagnosis_->get_last_pushed_diagnosis_point(ROUTE_POLICY)) : NULL;
      // if not support safe_weak_read snapshot version, we should disable sort by priority
      if (OB_UNLIKELY(common::WEAK == consistency_level)) {
        if (!s.sm_->is_causal_order_read_enabled()) {
          LOG_DEBUG("safe weak read is disabled",
                    "proxy_enable_causal_order_read",
                    s.sm_->trans_state_.mysql_config_params_->enable_causal_order_read_,
                    "server_enable_causal_order_read",
                    s.sm_->client_session_->get_session_info().is_server_support_safe_read_weak());
          s.sm_->client_session_->dummy_ldc_.set_safe_snapshot_manager(NULL);
        } else {
          LOG_DEBUG("safe weak read is enabled");
        }
      } else {
        if (common::STRONG == consistency_level && 1 == s.pll_info_.pl_attempts_
          && NULL != s.pll_info_.route_.table_entry_
          && !s.pll_info_.route_.table_entry_->is_dummy_entry()
          && !use_dup_replica
          && '\0' == s.sm_->proxy_primary_zone_name_[0]) {
          const ObProxyReplicaLocation *replica = NULL;
          set_route_leader_replica(s, replica);
          fill_addr = (replica != NULL);
        }
      }
      if (!fill_addr) {
        s.pll_info_.route_.need_use_dup_replica_ = use_dup_replica;
        const bool disable_merge_status_check = need_disable_merge_status_check(s);
        if (OB_NOT_NULL(diagnosis_route_policy)) {
          diagnosis_route_policy->need_check_merge_status_ = !disable_merge_status_check;
        }
        const ObRoutePolicyEnum route_policy = s.get_route_policy(*s.sm_->client_session_, use_dup_replica, diagnosis_route_policy);
        bool is_random_routing_mode = s.mysql_config_params_->is_random_routing_mode();

        ModulePageAllocator *allocator = NULL;
        ObLDCLocation::get_thread_allocator(allocator);

        ObSEArray<ObString, 5> region_names(5, *allocator);
        ObSEArray<ObServerStateSimpleInfo, ObServerStateRefreshCont::DEFAULT_SERVER_COUNT> simple_servers_info(
            ObServerStateRefreshCont::DEFAULT_SERVER_COUNT, *allocator);
        get_region_name_and_server_info(s, simple_servers_info, region_names);
        ObString proxy_primary_zone_name(s.sm_->proxy_primary_zone_name_);

        if (OB_FAIL(s.pll_info_.route_.fill_replicas(
                consistency_level,
                route_policy,
                is_random_routing_mode,
                disable_merge_status_check,
                s.sm_->client_session_,
                s.sm_->sm_cluster_resource_,
                simple_servers_info,
                region_names,
                proxy_primary_zone_name
#if OB_DETAILED_SLOW_QUERY
                ,
                s.sm_->cmd_time_stats_.debug_random_time_,
                s.sm_->cmd_time_stats_.debug_fill_time_
#endif
                ))) {
          LOG_WDIAG("fail to fill replicas", K(ret));
        } else {
#if OB_DETAILED_SLOW_QUERY
          t2 = get_based_hrtime(s);
          s.sm_->cmd_time_stats_.debug_total_fill_time_ += milestone_diff(t1, t2);
          t1 = t2;
#endif

          int32_t attempt_count = 0;
          bool found_leader_force_congested = false;
          bool is_all_stale = false;
          ObReadStaleParam param;
          build_read_stale_param(s, param);
          if (OB_FAIL(s.pll_info_.get_next_avail_replica(s.force_retry_congested_, attempt_count, found_leader_force_congested, param, is_all_stale, replica))) {
            LOG_WDIAG("fail to get next avail replica", K(ret));
          } else {
            s.current_.attempts_ += attempt_count;
            if ((NULL == replica) && s.pll_info_.is_all_iterate_once()) { // next round
              if (is_all_stale && common::WEAK == consistency_level) {
                LOG_INFO("all available replica staled or congested, route weak read request to leader", "route", s.pll_info_.route_);
                s.pll_info_.reset_cursor();
                if (NULL != s.pll_info_.route_.table_entry_) {
                  set_route_leader_replica(s, replica);
                }
                if (replica == NULL) {
                  s.force_retry_congested_ = true;
                  LOG_INFO("no avail replica found, try again");
                  replica = s.pll_info_.get_next_avail_replica();
                }
              } else {
                // if we has tried all servers, do force retry in next round
                LOG_INFO("all replica is force_congested, force retry congested now",
                          "route", s.pll_info_.route_);
                s.pll_info_.reset_cursor();
                s.force_retry_congested_ = true;
                // get replica again
                replica = s.pll_info_.get_next_avail_replica();
              }
            }
          }

#if OB_DETAILED_SLOW_QUERY
          t2 = get_based_hrtime(s);
          s.sm_->cmd_time_stats_.debug_get_next_time_ += milestone_diff(t1, t2);
          t1 = t2;
#endif
          if (OB_ISNULL(replica)) {
            COLLECT_INTERNAL_DIAGNOSIS(
                s.sm_->connection_diagnosis_trace_, OB_PROXY_INTERNAL_TRACE,
                OB_PROXY_INTERNAL_ERROR, "proxy routing find no avail replicas");
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("no replica avail", K(replica), K(ret));
          } else {
            s.server_info_.set_addr(ops_ip_sa_cast(replica->server_.get_sockaddr()));
            LOG_DEBUG("get replica by pl lookup, set addr", K(s.server_info_.addr_));
          }

          if (found_leader_force_congested) {
            ObProxyMutex *mutex_ = s.sm_->mutex_; // for stat
            PROCESSOR_INCREMENT_DYN_STAT(UPDATE_ROUTE_ENTRY_BY_CONGESTION);
            // when leader first router && (leader was congested from server or dead congested),
            // we need set forceUpdateTableEntry to avoid senseless of leader migrate
            if (s.pll_info_.set_target_dirty()) {
              LOG_INFO("leader is force_congested in strong read, set it to dirty "
                       "and wait for updating",
                       "addr", s.server_info_.addr_,
                       "route", s.pll_info_.route_);
            }
          }
        }
      } else {
      }
      if (OB_NOT_NULL(diagnosis_route_policy)) {
        diagnosis_route_policy->opt_route_policy_ = s.pll_info_.route_.ldc_route_.policy_;
        diagnosis_route_policy->chosen_route_type_ = s.pll_info_.route_.cur_chosen_route_type_;
        diagnosis_route_policy->chosen_server_ = s.pll_info_.route_.cur_chosen_server_;
        if (OB_NOT_NULL(s.pll_info_.route_.cur_chosen_server_.replica_)) {
          diagnosis_route_policy->replica_ = *s.pll_info_.route_.cur_chosen_server_.replica_;
        }
      }
      if (OB_NOT_NULL(info)) {
        info->dec_ref();
      }
    }

    if (OB_SUCC(ret)) {
      LOG_DEBUG("[ObMysqlTransact::handle_pl_lookup] chosen server, and begin to congestion lookup",
                "addr", s.server_info_.addr_,
                "attempts", s.current_.attempts_,
                "sm_id", s.sm_->sm_id_);
      if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
        s.sm_->milestones_.pl_process_end_ = get_based_hrtime(s);
        s.sm_->cmd_time_stats_.pl_process_time_ +=
          milestone_diff(s.sm_->milestones_.pl_process_begin_, s.sm_->milestones_.pl_process_end_);
      }
      TRANSACT_RETURN(SM_ACTION_CONGESTION_CONTROL_LOOKUP, handle_congestion_control_lookup);
    }
  }

  if (OB_FAIL(ret)) {
    s.inner_errcode_ = ret;
    // disconnect
    TRANSACT_RETURN_WITH_MSG(SM_ACTION_SEND_ERROR_NOOP, NULL); // disconnect
  }
}

void ObMysqlTransact::set_route_leader_replica(ObTransState &s, const ObProxyReplicaLocation *&replica)
{
  replica = NULL;
  const ObProxyPartitionLocation *pl = NULL;
  if (s.pll_info_.route_.is_non_partition_table()) {
    if (s.pll_info_.route_.table_entry_ != NULL) {
      pl = s.pll_info_.route_.table_entry_->get_first_pl();
    }
  } else {
    if (s.pll_info_.route_.part_entry_ != NULL) {
      pl = &s.pll_info_.route_.part_entry_->get_pl();
    }
  }
  if (pl != NULL){
    for (int64_t i = 0; NULL != pl && i < pl->replica_count(); i++) {
      const ObProxyReplicaLocation *tmp_replica = pl->get_replica(i);
      if (NULL != tmp_replica && tmp_replica->is_leader()) {
        s.server_info_.set_addr(ops_ip_sa_cast(tmp_replica->server_.get_sockaddr()));
        s.pll_info_.route_.cur_chosen_server_.replica_ = tmp_replica;
        s.pll_info_.route_.leader_item_.is_used_ = true;
        s.pll_info_.route_.skip_leader_item_ = true;
        s.pll_info_.route_.cur_chosen_route_type_ = ROUTE_TYPE_LEADER;
        replica = tmp_replica;
        break;
      }
    }
  }
}

bool ObMysqlTransact::need_use_tunnel(ObTransState &s)
{
    // if we use tunnel to transfer request then wouldn't call build_xxx_request()
    // 1. for large request, and we can not receive complete at once,
    //    here no need compress, and if needed, tunnel's plugin will compress
    //    and here no need rewrite stmt id, it will be rewritten in setup_client_transfer
    // 2. OB_MYSQL_COM_LOAD_DATA_TRANSFER_CONTENT means there are a lot of packets need to be transferred, and we
    //    use tunnel to transfer:
    //      a.OB20/Compression: call etup_client_transfer(MYSQL_TRANSFORM_VC)
    //        then setup_transform_to_server_transfer()
    //      b.MySQL: call setup_client_transfer(MYSQL_SERVER_VC)
  return s.trans_info_.request_content_length_ > 0 || OB_MYSQL_COM_LOAD_DATA_TRANSFER_CONTENT == s.trans_info_.sql_cmd_;
}
void ObMysqlTransact::handle_server_addr_lookup(ObTransState &s)
{
  int ret = OB_SUCCESS;
  ObProxyKillQueryInfo *query_info = s.trans_info_.client_request_.query_info_;

  if (OB_ISNULL(query_info)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WDIAG("[ObMysqlTransact::handle_server_addr_lookup] query_info should not be null, will disconnect", K(ret));
  } else if (!query_info->is_lookup_succ()) {
    // server session lookup failed, response err/ok packet
    LOG_DEBUG("[ObMysqlTransact::handle_server_addr_lookup] server addr lookup "
              "failed, response err/ok packet", "errcode", query_info->errcode_);

    ObMIOBuffer *buf = NULL;
    ObMysqlSM *sm = s.sm_;

    // consume data in client buffer reader
    if (OB_ISNULL(sm)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unexpect empty sm", K(ret));
    } else if (OB_FAIL(sm->get_client_buffer_reader()->consume_all())) {
      LOG_WDIAG("[ObMysqlTransact::handle_server_addr_lookup] fail to consume client_buffer_reader_", K(ret));
    } else if (OB_FAIL(s.alloc_internal_buffer(MYSQL_BUFFER_SIZE))) {
      LOG_WDIAG("[ObMysqlTransact::handle_server_addr_lookup] fail to allocate internal miobuffer", K(ret));
    } else {
      buf = s.internal_buffer_;
      uint8_t seq = static_cast<uint8_t>(s.trans_info_.client_request_.get_packet_meta().pkt_seq_ + 1);
      ObMysqlClientSession *client_session = sm->get_client_session();
      ObProxyProtocol client_protocol = sm->get_client_session_protocol();

      switch (query_info->errcode_) {
        case OB_SUCCESS: {
          if (OB_ISNULL(client_session)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("[ObMysqlTransact::handle_server_addr_lookup] client session is NULL, will disconnect", K(ret));
          } else {
            const ObMySQLCapabilityFlags &capability = get_client_session_info(s).get_orig_capability_flags();
            if (ObProxyPacketWriter::write_ok_packet(*buf, *client_session, client_protocol, seq, 0, capability)) {
              LOG_WDIAG("[ObMysqlTransact::handle_server_addr_lookup] fail to build ok resp "
                       "packet", K(ret));
            }
          }
          break;
        }
        case OB_UNKNOWN_CONNECTION:
        case OB_ERR_KILL_DENIED: {
          char *err_msg = NULL;
          if (OB_FAIL(ObProxyPacketWriter::get_user_err_buf(query_info->errcode_, err_msg, query_info->cs_id_))) {
            LOG_WDIAG("fail to get user err buf", K(ret));
          } else if (OB_FAIL(ObProxyPacketWriter::write_error_packet(*buf, client_session, client_protocol,
                                                                     seq, query_info->errcode_, err_msg))) {
            LOG_WDIAG("[ObMysqlTransact::build_error_packet] fail to build not supported err resp", K(ret));
          }
          break;
        }
        case OB_ERR_NO_PRIVILEGE: {
          char *err_msg = NULL;
          if (OB_FAIL(ObProxyPacketWriter::get_user_err_buf(query_info->errcode_, err_msg, query_info->priv_name_))) {
            LOG_WDIAG("fail to get user err buf", K(ret));
          } else if (OB_FAIL(ObProxyPacketWriter::write_error_packet(*buf, client_session, client_protocol,
                                                                     seq, query_info->errcode_, err_msg))) {
            LOG_WDIAG("[ObMysqlTransact::handle_server_addr_lookup] fail to build no privilege err resp packet",
                     K(ret), "priv_name", query_info->priv_name_);
          }
          break;
        }
        default: {
          char *err_msg = NULL;
          if (OB_FAIL(ObProxyPacketWriter::get_err_buf(query_info->errcode_, err_msg))) {
            LOG_WDIAG("fail to get err buf", K(ret));
          } else if (OB_FAIL(ObProxyPacketWriter::write_error_packet(*buf, client_session, client_protocol,
                                                                     seq, query_info->errcode_, err_msg))) {
            LOG_WDIAG("[ObMysqlTransact::build_error_packet] fail to build err resp packet", K(ret));
          }
          break;
        }
      }
    }

    if (OB_SUCC(ret)) {
      LOG_DEBUG("[ObMysqlTransact::handle_server_addr_lookup] succ to encode err packet, "
                "will send response");
      TRANSACT_RETURN(SM_ACTION_INTERNAL_NOOP, NULL);
    } else {
      s.free_internal_buffer();
    }

  } else { // server session lookup succeed
    if (s.pll_info_.lookup_success_) {
      LOG_DEBUG("[ObMysqlTransact::handle_server_addr_lookup] server session lookup succeed");
      if (!s.server_info_.addr_.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("[ObMysqlTransact::handle_server_addr_lookup] observer ip/port doesn't set "
                 "correctly, will disconnect", K(ret));
      } else {
        LOG_DEBUG("[ObMysqlTransact::handle_server_addr_lookup] choosen server",
                  K(s.server_info_.addr_));
        start_access_control(s);
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("[ObMysqlTransact::handle_server_addr_lookup] it should not come here, will disconnect", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    TRANSACT_RETURN_WITH_MSG(SM_ACTION_SEND_ERROR_NOOP, NULL);
  }
}

inline void ObMysqlTransact::start_access_control(ObTransState &s)
{
  if (s.trans_info_.client_request_.get_parse_result().has_explain_route()) {
    handle_explain_route(s);
  } else if (s.is_need_pl_lookup()) {
    TRANSACT_RETURN(SM_ACTION_API_OBSERVER_PL, ObMysqlTransact::lookup_skip_open_server);
    s.sm_->set_skip_plugin(true);
  } else {
    lookup_skip_open_server(s);
  }
}

inline void ObMysqlTransact::lookup_skip_open_server(ObTransState &s)
{
  TRANSACT_RETURN(SM_ACTION_OBSERVER_OPEN, ObMysqlTransact::handle_response);
}

int ObMysqlTransact::rewrite_stmt_id(ObTransState &s, ObIOBufferReader *client_buffer_reader)
{
  int ret = OB_SUCCESS;
  obmysql::ObMySQLCmd cmd = s.trans_info_.client_request_.get_packet_meta().cmd_;

  // rewrite stmt id for ps execute/send piece/send long data
  if (obmysql::OB_MYSQL_COM_STMT_EXECUTE == cmd
      || obmysql::OB_MYSQL_COM_STMT_SEND_PIECE_DATA == cmd
      || obmysql::OB_MYSQL_COM_STMT_SEND_LONG_DATA == cmd) {
    ObServerSessionInfo &ss_info = get_server_session_info(s);
    ObClientSessionInfo &cs_info = get_client_session_info(s);
    uint32_t client_ps_id = cs_info.get_client_ps_id();
    // 从当前映射关系中取出server_ps_id
    uint32_t server_ps_id = ss_info.get_server_ps_id(client_ps_id);
    client_buffer_reader->replace(reinterpret_cast<const char*>(&server_ps_id), sizeof(server_ps_id), MYSQL_NET_META_LENGTH);
  } else if (obmysql::OB_MYSQL_COM_STMT_FETCH == cmd
             || obmysql::OB_MYSQL_COM_STMT_GET_PIECE_DATA == cmd) {
    ObServerSessionInfo &ss_info = get_server_session_info(s);
    ObClientSessionInfo &cs_info = get_client_session_info(s);
    uint32_t client_cursor_id = cs_info.get_client_cursor_id();
    uint32_t server_cursor_id = 0;
    // 从当前映射关系中取出server_cursor_id
    // 如果没有, 可能链接断开过, 如果还继续用 client_cursor_id 可能会导致数据出错
    if (OB_FAIL(ss_info.get_server_cursor_id(client_cursor_id, server_cursor_id))) {
      LOG_WDIAG("fail to get server cursor id", K(client_cursor_id), K(ret));
    } else {
      client_buffer_reader->replace(reinterpret_cast<const char*>(&server_cursor_id), sizeof(server_cursor_id), MYSQL_NET_META_LENGTH);
    }
  } else if (obmysql::OB_MYSQL_COM_STMT_CLOSE == cmd || obmysql::OB_MYSQL_COM_STMT_RESET == cmd) {
    ObServerSessionInfo &ss_info = get_server_session_info(s);
    ObClientSessionInfo &cs_info = get_client_session_info(s);
    uint32_t client_ps_id = cs_info.get_client_ps_id();
    uint32_t server_ps_id = 0;
    // 从当前映射关系中取出 server_ps_id 或者 server_cursor_id
    if (client_ps_id >= (CURSOR_ID_START)) {
      if (OB_FAIL(ss_info.get_server_cursor_id(client_ps_id, server_ps_id))) {
        LOG_WDIAG("fail to get server cursor id", "client_cursor_id", client_ps_id, K(ret));
      }
    } else {
      server_ps_id = ss_info.get_server_ps_id(client_ps_id);
    }

    client_buffer_reader->replace(reinterpret_cast<const char*>(&server_ps_id), sizeof(server_ps_id), MYSQL_NET_META_LENGTH);
  } else if (obmysql::OB_MYSQL_COM_STMT_PREPARE_EXECUTE == cmd) {
    ObClientSessionInfo &cs_info = get_client_session_info(s);
    uint32_t recv_client_ps_id = cs_info.get_recv_client_ps_id();
    uint32_t client_ps_id = cs_info.get_client_ps_id();

    /* 如果收到的 recv_client_ps_id 为 0, 则表示第一次发送，可以直接把包透传过去
     * 如果收到的 recv_client_ps_id 不为 0, 则要区分两种情况:
     *   1. 如果这个后端 Server 已经发送过一次, 则要替换 Server Ps Id
     *   2. 如果这个后端 Server 没有发送过, 需要把包里的 stmt id 置为 0
     */
    if (0 != recv_client_ps_id) {
      ObServerSessionInfo &ss_info = get_server_session_info(s);
      /* 如果这个 Server 已经发送过一次，则拿到的是真实的 Server Ps Id
       * 如果这个 Server 还没发送过，返回的则是 0
       */
      uint32_t server_ps_id = ss_info.get_server_ps_id(client_ps_id);
      client_buffer_reader->replace(reinterpret_cast<const char*>(&server_ps_id), sizeof(server_ps_id), MYSQL_NET_META_LENGTH);
    }
  }
  return ret;
}

inline int ObMysqlTransact::build_user_request(
    ObTransState &s, ObIOBufferReader *client_buffer_reader,
    ObIOBufferReader *&reader, int64_t &request_len)
{
  int ret = OB_SUCCESS;

  if (s.sm_->client_session_->get_session_info().is_oceanbase_server()) {
    if (OB_FAIL(build_oceanbase_user_request(s, client_buffer_reader, reader, request_len))) {
      LOG_WDIAG("fail to build oceanbase user request", K(ret));
    }
  } else {
    // no need compress, send directly
    int64_t client_request_len = s.trans_info_.client_request_.get_packet_len();
    request_len = client_request_len;
    reader = client_buffer_reader;
  }

  return ret;
}

inline int ObMysqlTransact::build_oceanbase_user_request(
    ObTransState &s, ObIOBufferReader *client_buffer_reader,
    ObIOBufferReader *&reader, int64_t &request_len)
{
  int ret = OB_SUCCESS;
  ObProxyProtocol server_protocol = s.sm_->get_server_session_protocol();
  int64_t client_request_len = s.trans_info_.client_request_.get_packet_len();
  // obproxy only send one mysql request packet at once;
  // if received multi request packets, we will send it one by one;
  // request_len include header and paylaod
  // in mysql mode, we send auth request directly to mysql server
  if (OB_MYSQL_COM_LOGIN == s.trans_info_.sql_cmd_ && s.mysql_config_params_->is_mysql_routing_mode()) {
    request_len = s.sm_->get_client_session()->get_session_info().get_login_req().get_packet_len();
    client_request_len = request_len;
    reader = client_buffer_reader;
  } else {
    if (need_use_tunnel(s)) {
      reader = client_buffer_reader;
      request_len = client_request_len;
    } else if (OB_FAIL(rewrite_stmt_id(s, client_buffer_reader))) {
      LOG_WDIAG("rewrite stmt id failed", K(ret));
    } else {
      obmysql::ObMySQLCmd req_cmd_type = s.trans_info_.client_request_.get_packet_meta().cmd_;
      ObIOBufferReader *request_buffer_reader = client_buffer_reader;
      ObMIOBuffer *write_buffer = NULL;
      if (ObProxyProtocol::PROTOCOL_OB20 == server_protocol
          || ObProxyProtocol::PROTOCOL_CHECKSUM == server_protocol) { // convert standard mysql protocol to compression protocol
        uint8_t compress_seq = 0;
        if (OB_ISNULL(write_buffer = new_miobuffer(MYSQL_BUFFER_SIZE))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WDIAG("fail to alloc mio_buffer", K(ret));
        } else if (OB_ISNULL(reader = write_buffer->alloc_reader())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("[ObMysqlTransact::build_user_request] failed to allocate iobuffer reader", K(ret));
        } else if ((obmysql::OB_MYSQL_COM_STMT_CLOSE == req_cmd_type
                    || obmysql::OB_MYSQL_COM_STMT_RESET == req_cmd_type
                    || s.trans_info_.client_request_.get_parse_result().is_text_ps_drop_stmt())
            && OB_ISNULL(request_buffer_reader = client_buffer_reader->clone())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("[ObMysqlTransact::build_user_request] failed to clone client buffer reader", K(ret));
        } else {
          if (ObProxyProtocol::PROTOCOL_OB20 == server_protocol) {
            if (OB_FAIL(build_oceanbase_ob20_user_request(s, *write_buffer, *request_buffer_reader,
                                                          client_request_len, compress_seq))) {
              LOG_WDIAG("fail to build oceanbase ob20 user request", K(ret), K(client_request_len), K(compress_seq));
            } else {
              // handle tail crc for other cmd type, close will send to every server, do not clear buf here
              if (obmysql::OB_MYSQL_COM_STMT_CLOSE != req_cmd_type
                  && obmysql::OB_MYSQL_COM_STMT_RESET != req_cmd_type
                  && !s.trans_info_.client_request_.get_parse_result().is_text_ps_drop_stmt()) {
                ObProxyProtocol client_protocol = s.sm_->get_client_session_protocol();
                if (client_protocol == ObProxyProtocol::PROTOCOL_OB20) {
                  ObClientSessionInfo &cs_info = s.sm_->get_client_session()->get_session_info();
                  const bool ob20_req_received_done = cs_info.ob20_request_.ob20_request_received_done_;
                  const int64_t ob20_req_remain_payload_len = cs_info.ob20_request_.remain_payload_len_;
                  if (ob20_req_received_done
                      && ob20_req_remain_payload_len == 0) {
                    cs_info.ob20_request_.ob20_request_received_done_ = false;
                    int64_t read_avail = client_buffer_reader->read_avail();
                    LOG_DEBUG("after build user ob req, handle ob20 tail crc in buffer",
                              K(ob20_req_received_done), K(ob20_req_remain_payload_len), K(read_avail));
                    if (read_avail >= OB20_PROTOCOL_TAILER_LENGTH) {
                      if (OB_FAIL(client_buffer_reader->consume(OB20_PROTOCOL_TAILER_LENGTH))) {
                        LOG_WDIAG("fail to consume the last crc buffer in client request buffer", K(ret));
                      }
                    } else {
                      // nothing
                      // cause buffer could be consumed all before, eg: handle internal request
                    }
                  }
                }
              }
            } // else
          } else {
            ObCmpHeaderParam param(compress_seq, s.sm_->is_checksum_on(), s.sm_->compression_algorithm_.level_);
            INC_SHARED_REF(param.get_protocol_diagnosis_ref(), s.sm_->protocol_diagnosis_);
            if (OB_FAIL(ObMysqlAnalyzerUtils::consume_and_compress_data(request_buffer_reader, write_buffer,
                                                                        client_request_len, param))) {
              LOG_WDIAG("fail to consume and compress mysql compress data", K(ret));
            } else {
              compress_seq = param.get_compressed_seq();
            }
          }

          if (obmysql::OB_MYSQL_COM_STMT_CLOSE == req_cmd_type
              || obmysql::OB_MYSQL_COM_STMT_RESET == req_cmd_type
              || s.trans_info_.client_request_.get_parse_result().is_text_ps_drop_stmt()) {
            request_buffer_reader->dealloc();
            request_buffer_reader = NULL;
          }

          if (OB_SUCC(ret)) {
            s.sm_->get_server_session()->set_compressed_seq(compress_seq);
            request_len = reader->read_avail();
            LOG_DEBUG("build user compressed request succ", K(server_protocol),
                      "origin len", client_request_len, "compress len", request_len, "compress seq", compress_seq);
          }
        }
      } else {
        // no need compress, send directly
        request_len = client_request_len;
        if (obmysql::OB_MYSQL_COM_STMT_CLOSE == req_cmd_type
            || obmysql::OB_MYSQL_COM_STMT_RESET == req_cmd_type
            || s.trans_info_.client_request_.get_parse_result().is_text_ps_drop_stmt()) {
          int64_t written_len = 0;
          if (OB_ISNULL(write_buffer = new_miobuffer(MYSQL_BUFFER_SIZE))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WDIAG("fail to alloc mio_buffer", K(ret));
          } else if (OB_ISNULL(request_buffer_reader = write_buffer->alloc_reader())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("[ObMysqlTransact::build_user_request] failed to allocate iobuffer reader", K(ret));
          } else if (OB_FAIL(write_buffer->write(client_buffer_reader, client_request_len, written_len))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("[ObMysqlTransact::build_user_request] fail to write com_stmt_close packet into new iobuffer", K(client_request_len), K(ret));
          } else if (OB_UNLIKELY(client_request_len != written_len)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("[ObMysqlTransact::build_user_request] written_len dismatch", K(client_request_len), K(written_len), K(ret));
          }
        }
        reader = request_buffer_reader;
        PROTOCOL_DIAGNOSIS(MULTI_MYSQL, send, s.sm_->protocol_diagnosis_, *reader, reader->read_avail());
        LOG_DEBUG("protocol diagnosis", K(reader->read_avail()));
      }

      if (OB_FAIL(ret)) {
        reader = NULL;
        if (NULL != write_buffer) {
          free_miobuffer(write_buffer);
          write_buffer = NULL;
        }
      }
    }
  }

  LOG_DEBUG("[ObMysqlTransact::build_user_request] send request to observer",
            "sm_id", s.sm_->sm_id_, K(server_protocol), K(client_request_len),
            "total_client_request_len", (reader != NULL) ? reader->read_avail() : 0,
            "request len send to server", request_len,
            K(s.trans_info_.request_content_length_),
            K(s.trans_info_.sql_cmd_),
            "is_in_auth", s.is_auth_request_, K(ret));
  return ret;
}

int ObMysqlTransact::build_oceanbase_ob20_user_request(ObTransState &s, ObMIOBuffer &write_buffer,
                                                       ObIOBufferReader &request_buffer_reader,
                                                       int64_t client_request_len,
                                                       uint8_t &compress_seq)
{
  int ret = OB_SUCCESS;
  int64_t req_buf_avail = request_buffer_reader.read_avail();
  if (OB_UNLIKELY(req_buf_avail < client_request_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected buffer len check", K(ret), K(req_buf_avail), K(client_request_len));
  } else {
    const int64_t split_request_len = 1 << 23L;     // 8MB split each mysql packet
    int64_t remain_req_len = client_request_len;
    int64_t curr_req_len = 0;
    bool generated_extra_info = false;
    bool is_last_packet = false;
    const bool need_reroute = is_need_reroute(s);
    const bool is_weak_read = (WEAK == s.sm_->trans_state_.get_trans_consistency_level(s.sm_->get_client_session()->get_session_info()));
    ObSEArray<ObObJKV, 3> extra_info;
    ObSqlString sess_info_value;
    char client_ip_buf[MAX_IP_BUFFER_LEN] = "\0";
    char flt_info_buf[SERVER_FLT_INFO_BUF_MAX_LEN] = "\0";
    char sess_info_veri_buf[OB_SESS_INFO_VERI_BUF_MAX] = "\0";
    ObMysqlServerSession *server_session = s.sm_->get_server_session();
    ObMysqlClientSession *client_session = s.sm_->get_client_session();
    uint32_t req_id = server_session->get_next_server_request_id();
    const bool is_proxy_switch_route = s.sm_->is_proxy_switch_route();

    // buf to fill extra info
    char *total_flt_info_buf = flt_info_buf;
    int64_t total_flt_info_buf_len = SERVER_FLT_INFO_BUF_MAX_LEN;

    while (OB_SUCC(ret) && remain_req_len > 0) {
      curr_req_len = remain_req_len > split_request_len ? split_request_len : remain_req_len;
      remain_req_len -= curr_req_len;
      if (remain_req_len == 0) {
        is_last_packet = true;
      }

      if (!generated_extra_info) {
        // cause observer could only resolve extra info in the first ob20 packet
        // alloc show trace buffer
        if (OB_FAIL(ObProxyTraceUtils::build_show_trace_info_buffer(s.sm_, true, total_flt_info_buf,
                                                                    total_flt_info_buf_len))) {
          LOG_WDIAG("fail to build show trace info buffer", K(ret));
        } else if (OB_FAIL(ObProxyTraceUtils::build_related_extra_info_all(extra_info, s.sm_,
                      client_ip_buf, MAX_IP_BUFFER_LEN,
                      total_flt_info_buf, total_flt_info_buf_len,
                      sess_info_veri_buf, OB_SESS_INFO_VERI_BUF_MAX,
                      sess_info_value, true, is_proxy_switch_route))) {
          LOG_WDIAG("fail to build related extra info", K(ret));
        } else {
          generated_extra_info = true;
        }
      } else {
        extra_info.reset();
      }

      if (OB_SUCC(ret)) {
        const int64_t zlib_compression_level = s.sm_->compression_algorithm_.level_;
        const bool is_compressed_ob20 = (zlib_compression_level != 0 && server_session->get_session_info().is_server_ob20_compress_supported());
        Ob20HeaderParam ob20_head_param(server_session->get_server_sessid(),
                                        req_id, compress_seq, compress_seq,
                                        is_last_packet, is_weak_read, need_reroute,
                                        server_session->get_session_info().is_new_extra_info_supported(),
                                        client_session->is_trans_internal_routing(), is_proxy_switch_route,
                                        is_compressed_ob20, zlib_compression_level);
        INC_SHARED_REF(ob20_head_param.get_protocol_diagnosis_ref(), s.sm_->protocol_diagnosis_);
        if (OB_FAIL(ObProto20Utils::consume_and_compress_data(&request_buffer_reader, &write_buffer,
                                                              curr_req_len, ob20_head_param, &extra_info))) {
          LOG_WDIAG("fail to consume and compress ob20 data", K(ret));
        } else {
          LOG_DEBUG("succ to consume and compress in ob20", K(curr_req_len), K(compress_seq), K(is_last_packet),
                    K(req_id));
          compress_seq++;
        }
      }
    } // while
    compress_seq--;

    // free show trace buffer after use, in both succ and fail
    if (total_flt_info_buf_len > SERVER_FLT_INFO_BUF_MAX_LEN
        && total_flt_info_buf != NULL) {
      ob_free(total_flt_info_buf);
      total_flt_info_buf = NULL;
    }
  } // else

  return ret;
}

bool ObMysqlTransact::handle_set_trans_internal_routing(ObMysqlTransact::ObTransState &s, bool server_transaction_routing_flag)
{
  // used to set the header flag
  // if session in a transaction, return the config of enable_internal_transaction_routing in server
  // else return the config enable_internal_transaction_routing
  bool internal_routing_flag = false;
  if (is_trans_specified(s)) {
    // trans_spcified, disable trans internal routing
    internal_routing_flag = false;
  } else if (!is_in_trans(s)) {
    // do nothing, return config of enable_internal_transaction
    internal_routing_flag = s.sm_->get_client_session()->is_proxy_enable_trans_internal_routing();
  } else {
    internal_routing_flag = s.sm_->get_client_session()->is_proxy_enable_trans_internal_routing() && server_transaction_routing_flag;
  }
  return internal_routing_flag;
}

inline int ObMysqlTransact::build_normal_login_request(
    ObTransState &s, ObIOBufferReader *&reader, int64_t &request_len)
{
  int ret = OB_SUCCESS;

  ObMIOBuffer *write_buffer = NULL;
  ObClientSessionInfo &client_info = s.sm_->get_client_session()->get_session_info();
  ObShardConnector *shard_conn = client_info.get_shard_connector();
  if (OB_ISNULL(shard_conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("[ObMysqlTransact::build_normal_login_request] shard conn is null");
  } else if (OB_ISNULL(write_buffer = new_miobuffer(MYSQL_BUFFER_SIZE))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("[ObMysqlTransact::build_normal_login_request] write_buffer is null");
  } else if (OB_ISNULL(reader = write_buffer->alloc_reader())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("[ObMysqlTransact::build_normal_login_request] failed to allocate iobuffer reader", K(ret));
  } else {
    ObRespAnalyzeResult &result = s.trans_info_.server_response_.get_analyze_result();
    const ObString &server_scramble = result.get_scramble_string();

    const ObString &username = shard_conn->full_username_.config_string_;
    const ObString &password = shard_conn->password_.config_string_;
    const ObString &database = shard_conn->database_name_.config_string_;

    OMPKHandshakeResponse tg_hsr = client_info.get_login_req().get_hsr_result().response_;

    // 0. assign seq num
    tg_hsr.set_seq(static_cast<int8_t>(client_info.get_login_req().get_packet_meta().pkt_seq_));

    // 1. assign user name
    tg_hsr.set_username(username);

    // 2. add CLIENT_CONNECT_ATTRS and CLIENT_SESSION_TRACK flags
    ObMySQLCapabilityFlags cap_flag = tg_hsr.get_capability_flags();
    cap_flag.cap_flags_.OB_CLIENT_CONNECT_WITH_DB = 1;
    cap_flag.cap_flags_.OB_CLIENT_PLUGIN_AUTH = 0;
    cap_flag.cap_flags_.OB_CLIENT_CONNECT_ATTRS = 0;
    cap_flag.cap_flags_.OB_CLIENT_SESSION_TRACK = 1;
    cap_flag.cap_flags_.OB_CLIENT_DEPRECATE_EOF = 0;
    tg_hsr.set_capability_flags(cap_flag);

    // 3. change auth_response
    const int64_t pwd_buf_len = SHA1_HASH_SIZE + 1;
    char pwd_buf[pwd_buf_len] = {0};
    int64_t actual_len = 0;
    if (OB_FAIL(ObEncryptedHelper::encrypt_password(password, server_scramble,
                pwd_buf, pwd_buf_len, actual_len))) {
      LOG_WDIAG("fail to encrypt password", K(ret));
    } else {
      ObString auth_str(actual_len, pwd_buf);
      tg_hsr.set_auth_response(auth_str);

      tg_hsr.set_database(database);

      // 5. reset before add
      tg_hsr.reset_connect_attr();

      if (OB_FAIL(ObMysqlPacketWriter::write_packet(*write_buffer, tg_hsr))) {
        LOG_WDIAG("fail to write packet", K(ret));
      } else {
        ObServerSessionInfo &ss_info = get_server_session_info(s);
        ObMySQLCapabilityFlags capability(ss_info.get_compatible_capability_flags().capability_ & tg_hsr.get_capability_flags().capability_);
        ss_info.save_compatible_capability_flags(capability);
        request_len = reader->read_avail();
        LOG_DEBUG("build normal login request succ");
      }
    }
  }

  return ret;
}

int ObMysqlTransact::build_server_request(ObTransState &s, ObIOBufferReader *&reader, int64_t &request_len)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("[ObMysqlTransact::build_server_request] checking sending sql",
            "cur_send_action", ObMysqlTransact::get_send_action_name(s.current_.send_action_), K(s.trans_info_.get_print_sql()), K(is_in_trans(s)),
            K(s.sm_->get_client_session()->get_cs_id()));
  ObIOBufferReader *client_buffer_reader = NULL;
  BuildFunc build_func = NULL;
  reader = NULL;
  request_len = 0;

  if (OB_ISNULL(client_buffer_reader = s.sm_->get_client_buffer_reader())) {
    LOG_WDIAG("[ObMysqlTransact::build_server_request] client buffer reader in sm is NULL");
    ret = OB_INVALID_ARGUMENT;
  } else {
    switch (s.current_.send_action_) {
      case SERVER_SEND_REQUEST: {
        if (OB_FAIL(build_user_request(s, client_buffer_reader, reader, request_len))) {
          LOG_WDIAG("fail to build user request", K(ret));
        }
        break;
      }

      case SERVER_SEND_SSL_REQUEST: {
        build_func = ObMysqlRequestBuilder::build_ssl_request_packet;
        break;
      }

      case SERVER_SEND_INIT_SQL: {
        build_func = ObMysqlRequestBuilder::build_init_sql_request_packet;
        break;
      }

      case SERVER_SEND_LOGIN: {
        // before send first login packet, we must consume current buffer
        if (OB_FAIL(client_buffer_reader->consume_all())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("[ObMysqlTransact::build_server_request] "
                   "failed to consume all data in client buffer reader", K(ret));
        } else {
          if (s.sm_->client_session_->get_session_info().is_oceanbase_server()) {
            if (s.mysql_config_params_->is_mysql_routing_mode()) {
              // write the buffer using orig login packet directly
              build_func = ObMysqlRequestBuilder::build_orig_login_packet;
            } else {
              // write the buffor using first login packet directly
              build_func = ObMysqlRequestBuilder::build_first_login_packet;
            }
          } else {
            if (OB_FAIL(build_normal_login_request(s, reader, request_len))) {
              LOG_WDIAG("fail to build normal login reuest", K(ret));
            }
          }
        }
        break;
      }

      case SERVER_SEND_SAVED_LOGIN:
        if (is_binlog_request(s)) {
          build_func = ObMysqlRequestBuilder::build_binlog_login_packet;
        } else if (s.sm_->client_session_->get_session_info().is_oceanbase_server()) {
          // write the buffor using saved login packet directly
          build_func = ObMysqlRequestBuilder::build_saved_login_packet;
        } else {
          if (OB_FAIL(build_normal_login_request(s, reader, request_len))) {
            LOG_WDIAG("fail to build normal login reuest", K(ret));
          }
        }
        break;

      case SERVER_SEND_ALL_SESSION_VARS:
        build_func = ObMysqlRequestBuilder::build_all_session_vars_sync_packet;
        break;

      case SERVER_SEND_USE_DATABASE:
        build_func = ObMysqlRequestBuilder::build_database_sync_packet;
        break;

      case SERVER_SEND_SESSION_VARS:
        build_func = ObMysqlRequestBuilder::build_session_vars_sync_packet;
        break;

      case SERVER_SEND_SESSION_USER_VARS:
        build_func = ObMysqlRequestBuilder::build_session_user_vars_sync_packet;
        break;

      case SERVER_SEND_START_TRANS:
        build_func = ObMysqlRequestBuilder::build_start_trans_request;
        break;

      case SERVER_SEND_XA_START:
        build_func = ObMysqlRequestBuilder::build_xa_start_request;
        break;

      case SERVER_SEND_PREPARE:
        build_func = ObMysqlRequestBuilder::build_prepare_request;
        break;

      case SERVER_SEND_TEXT_PS_PREPARE:
        build_func = ObMysqlRequestBuilder::build_text_ps_prepare_request;
        break;

      case SERVER_SEND_NONE:
      case SERVER_SEND_HANDSHAKE:
        break;

      default:
        ret = OB_INNER_STAT_ERROR;
        LOG_EDIAG("Unknown send next action type", K(s.current_.send_action_));
        break;
    }

    if (OB_UNLIKELY(OB_SUCCESS == ret && NULL != build_func)) {
      ObMIOBuffer *write_buffer = NULL;

      if (OB_ISNULL(write_buffer = new_miobuffer(MYSQL_BUFFER_SIZE))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("[ObMysqlTransact::build_server_request] write_buffer is null");
      } else if (OB_ISNULL(reader = write_buffer->alloc_reader())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("[ObMysqlTransact::build_server_request] failed to allocate iobuffer reader", K(ret));
      } else {
        ObMysqlClientSession *client_session = s.sm_->get_client_session();
        ObMysqlServerSession *server_session = s.sm_->get_server_session();
        ObProxyProtocol ob_proxy_protocol = s.sm_->get_server_session_protocol();
        if (OB_ISNULL(client_session) || OB_ISNULL(server_session)) {
          ret = OB_ERR_SYS;
          LOG_WDIAG("[ObMysqlTransact::build_server_request] invalid internal variables",
                   K(client_session), K(server_session));
        } else if (OB_FAIL(build_func(s.sm_, *write_buffer, client_session->get_session_info(),
                                      server_session, ob_proxy_protocol))) {
          LOG_WDIAG("[ObMysqlTransact::build_server_request] fail to build packet,"
                   " will disconnect", "sm_id", s.sm_->sm_id_,
                   "action_name", get_send_action_name(s.current_.send_action_), K(ret));
        } else {
          request_len = reader->read_avail();
          LOG_DEBUG("[ObMysqlTransact::build_server_request]"
                    " success to build packet", "sm_id", s.sm_->sm_id_, K(ob_proxy_protocol),
                    "is_checksum_on", server_session->get_session_info().is_checksum_on(),
                    "action_name", get_send_action_name(s.current_.send_action_),
                    K(request_len), K(ret));
        }
      }

      if (OB_FAIL(ret)) {
        reader = NULL;
        if (NULL != write_buffer) {
          free_miobuffer(write_buffer);
          write_buffer = NULL;
        }
      }
    } else {
      // do nothing
    }
  }

  return ret;
}

void ObMysqlTransact::handle_response(ObTransState &s)
{
  s.source_ = SOURCE_OBSERVER;
  s.sm_->trans_stats_.server_responses_ += 1;

  handle_response_from_server(s);
}

inline int ObMysqlTransact::check_global_vars_version(ObTransState &s, const ObStringKV &str_kv)
{
  int ret = OB_SUCCESS;
  ObClientSessionInfo &client_info = get_client_session_info(s);
  int64_t snapshot_global_vars_version = client_info.get_global_vars_version();
  int64_t cur_global_vars_version = 0;
  if (OB_FAIL(get_int_value(str_kv.value_, cur_global_vars_version))) {
    LOG_WDIAG("fail to get int from string", "string value", str_kv.value_, K(ret));
  } else if (cur_global_vars_version > snapshot_global_vars_version) {
    client_info.set_global_vars_changed_flag();
    LOG_INFO("global vars changed",
             K(cur_global_vars_version), K(snapshot_global_vars_version));
  } else if (cur_global_vars_version == snapshot_global_vars_version) {
    LOG_DEBUG("global vars stay unchanged",
              K(cur_global_vars_version), K(snapshot_global_vars_version));
  } else {
    client_info.set_global_vars_changed_flag();
    LOG_WDIAG("cur should not smaller than snapshot"
             "(cur == 0 means server use default sys var set)",
             K(cur_global_vars_version), K(snapshot_global_vars_version));
  }
  return ret;
}

inline int ObMysqlTransact::do_handle_prepare_response(ObTransState &s, ObIOBufferReader *&buf_reader)
{
  int ret = OB_SUCCESS;

  //into here, the first packet must received completely
  ObProxyProtocol ob_proxy_protocol = s.sm_->get_server_session_protocol();
  ObMysqlResp &server_response = s.trans_info_.server_response_;
  if (!server_response.get_analyze_result().is_decompressed()
      && (ObProxyProtocol::PROTOCOL_CHECKSUM == ob_proxy_protocol
          || ObProxyProtocol::PROTOCOL_OB20 == ob_proxy_protocol)) {
    ObMysqlCompressedOB20AnalyzeResult result;
    ObMysqlCompressAnalyzer *compress_analyzer = &s.sm_->get_compress_analyzer();
    ObMysqlServerSession *server_session = s.sm_->get_server_session();
    compress_analyzer->reset();
    ObProtocolDiagnosis *&protocol_diagnosis = compress_analyzer->get_protocol_diagnosis_ref();
    TMP_DISABLE_PROTOCOL_DIAGNOSIS(protocol_diagnosis);
    const uint8_t req_seq = s.sm_->get_compressed_or_ob20_request_seq();
    const ObMySQLCmd cmd = s.sm_->get_request_cmd();
    const ObMysqlProtocolMode mysql_mode = s.sm_->client_session_->get_session_info().is_oracle_mode() ? OCEANBASE_ORACLE_PROTOCOL_MODE : OCEANBASE_MYSQL_PROTOCOL_MODE;
    const bool enable_extra_ok_packet_for_stats = s.sm_->is_extra_ok_packet_for_stats_enabled();
    const bool is_analyze_compressed_ob20 =
        server_session->get_session_info().is_server_ob20_compress_supported() && s.sm_->compression_algorithm_.level_ != 0;
    if (OB_FAIL(compress_analyzer->init(req_seq, ObMysqlCompressAnalyzer::DECOMPRESS_MODE,
                                        cmd, mysql_mode, enable_extra_ok_packet_for_stats, req_seq,
                                        server_session->get_server_request_id(),
                                        server_session->get_server_sessid(),
                                        is_analyze_compressed_ob20))) {
      LOG_WDIAG("fail to init compress analyzer", K(req_seq), K(cmd), K(enable_extra_ok_packet_for_stats),
               "server_request_id", server_session->get_server_request_id(),
               "server_sessid", server_session->get_server_sessid(), K(ret));
    } else if (OB_ISNULL(buf_reader = compress_analyzer->get_transfer_reader())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to get transfer reader", K(buf_reader), K(ret));
    } else if (OB_FAIL(compress_analyzer->analyze_first_response(*s.sm_->get_server_buffer_reader(), result))) {
      LOG_WDIAG("fail to analyze first response", K(s.sm_->get_server_buffer_reader()), K(ret));
    } else if (ANALYZE_DONE != result.status_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("analyze status is wrong", K(result.status_), K(ret));
    }
    REENABLE_PROTOCOL_DIAGNOSIS(protocol_diagnosis);
  } else {
    ObMysqlAnalyzeResult result;
    if (OB_FAIL(ObProxyParserUtils::analyze_one_packet(*s.sm_->get_server_buffer_reader(), result))) {
      LOG_WDIAG("fail to analyze packet", K(buf_reader), K(ret));
    } else if (ANALYZE_DONE != result.status_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("analyze status is wrong", K(result.status_), K(ret));
    } else {
      buf_reader = s.sm_->get_server_buffer_reader();
    }
  }

  return ret;
}

inline int ObMysqlTransact::do_handle_prepare_succ(ObTransState &s, uint32_t server_ps_id)
{
  int ret = OB_SUCCESS;

  ObClientSessionInfo &cs_info = get_client_session_info(s);
  ObServerSessionInfo &ss_info = get_server_session_info(s);

  uint32_t client_ps_id = s.is_hold_xa_start_? cs_info.get_xa_start_ps_id() : cs_info.get_client_ps_id();
  ObPsIdPair *ps_id_pair = ss_info.get_ps_id_pair(client_ps_id);
  /* 走到这里有两种情况:
   *   如果是第一次, 如果是 Prepare 请求或者是 PrepareExecute 请求，则 client_ps_id 是新生成的, 那肯定没有 ps_id_pair
   *   如果是第二次:
   *     1. 如果是 Execute 请求时, 同步 Prepare 请求, 虽然是一个老的 client_ps_id, 但 need_do_prepare 判断没有 ps_id_pair 时才会同步 Prepare 请求, 所以没有 ps_id_pair
   *     2. 如果是 PrepareExecute 请求:
   *       2.1. 可能发给一个新的 server, 则没有 ps_id_pair
   *       2.2. 发给一个已经发过的 server, 则有 ps_id_pair, 但是不会生成新的 server ps id, 可以比较下 server ps id
   */
  if (NULL == ps_id_pair) {
    if (OB_FAIL(ObPsIdPair::alloc_ps_id_pair(client_ps_id, server_ps_id, ps_id_pair))) {
      LOG_WDIAG("fail to alloc ps id pair", "client_ps_id", cs_info.get_client_ps_id(),
          "server_ps_id", server_ps_id, K(ret));
    } else if (OB_ISNULL(ps_id_pair)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("ps_id_pair is null", K(ps_id_pair), K(ret));
    } else if (OB_FAIL(ss_info.add_ps_id_pair(ps_id_pair))) {
      LOG_WDIAG("fail to add ps_id_pair", KPC(ps_id_pair), K(ret));
      ps_id_pair->destroy();
      ps_id_pair = NULL;
    }
  } else if (OB_UNLIKELY(server_ps_id != ps_id_pair->get_server_ps_id())) {
    ObPsEntry *ps_entry = cs_info.get_ps_entry(client_ps_id);
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("same client_ps_id, but server returns different stmt id",
        K(ret), K(client_ps_id), K(server_ps_id), KPC(ps_id_pair), KPC(ps_entry));
  } else {
    ss_info.set_server_ps_id(ps_id_pair->server_ps_id_);
  }

  if (OB_SUCC(ret)) {
    ObMysqlServerSession *ss = s.sm_->get_server_session();
    const sockaddr &addr = ss->get_netvc()->get_remote_addr();
    ObPsIdAddrs *ps_id_addrs = cs_info.get_ps_id_addrs(client_ps_id);
    if (NULL == ps_id_addrs) {
      if (OB_FAIL(ObPsIdAddrs::alloc_ps_id_addrs(client_ps_id, addr, ps_id_addrs))) {
        PROXY_API_LOG(WDIAG, "fail to alloc ps id addrs", K(client_ps_id), K(ret));
      } else if (OB_ISNULL(ps_id_addrs)) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_API_LOG(WDIAG, "ps_id_addrs is null", K(ps_id_addrs), K(ret));
      } else if (OB_FAIL(cs_info.add_ps_id_addrs(ps_id_addrs))) {
        PROXY_API_LOG(WDIAG, "fail to add ps_id_addrs", KPC(ps_id_addrs), K(ret));
        ps_id_addrs->destroy();
      }
    } else {
      if (OB_FAIL(ps_id_addrs->add_addr(addr))) {
        PROXY_API_LOG(WDIAG, "fail to add addr into ps_id_addrs", "addr", ObIpEndpoint(addr), K(ret));
      }
    }
  }

  return ret;
}

inline void ObMysqlTransact::handle_prepare_succ(ObTransState &s)
{
  int ret = OB_SUCCESS;
  if (NULL != s.sm_->client_session_) {
    ObIOBufferReader *buf_reader = NULL;
    ObMysqlPacketReader pkt_reader;
    OMPKPrepare prepare_packet;
    uint32_t server_ps_id = 0;
    if (OB_FAIL(do_handle_prepare_response(s, buf_reader))) {
      LOG_WDIAG("fail to handle prepare response", K(ret));
    } else if (OB_ISNULL(buf_reader)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("reader is null, which is unexpected", K(ret));
    } else if (OB_FAIL(pkt_reader.get_packet(*buf_reader, prepare_packet))) {
      LOG_WDIAG("fail to get ok packet from server buffer reader", K(ret));
    } else if (FALSE_IT(server_ps_id = prepare_packet.get_statement_id())) {
    } else if (OB_FAIL(do_handle_prepare_succ(s, server_ps_id))) {
      LOG_WDIAG("fail to do handle prepare succ", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    s.current_.state_ = INTERNAL_ERROR;
  }
}

inline int ObMysqlTransact::do_handle_execute_succ(ObTransState &s)
{
  int ret = OB_SUCCESS;

  ObClientSessionInfo &cs_info = get_client_session_info(s);
  ObServerSessionInfo &ss_info = get_server_session_info(s);

  uint32_t client_ps_id = s.is_hold_xa_start_? cs_info.get_xa_start_ps_id() : cs_info.get_client_ps_id();
  ObPsIdPair *ps_id_pair = ss_info.get_ps_id_pair(client_ps_id);
  ObCursorIdPair *cursor_id_pair = ss_info.get_curosr_id_pair(client_ps_id);
  if (OB_ISNULL(ps_id_pair)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_API_LOG(WDIAG, "ps id pair is null", K(client_ps_id), K(ret));
  } else if (NULL == cursor_id_pair) {
    /* 隐私 cursor 用 ps id 作为 cursor id */
    if (OB_FAIL(ObCursorIdPair::alloc_cursor_id_pair(client_ps_id, ps_id_pair->get_server_ps_id(), cursor_id_pair))) {
      PROXY_API_LOG(WDIAG, "fail to alloc cursor id pair", K(client_ps_id), "server_ps_id", ps_id_pair->get_server_ps_id(), K(ret));
    } else if (OB_ISNULL(cursor_id_pair)) {
      ret = OB_ERR_UNEXPECTED;
      PROXY_API_LOG(WDIAG, "cursor_id_pair is null", K(cursor_id_pair), K(ret));
    } else if (OB_FAIL(ss_info.add_cursor_id_pair(cursor_id_pair))) {
      PROXY_API_LOG(WDIAG, "fail to add cursor_id_pair", KPC(cursor_id_pair), K(ret));
      cursor_id_pair->destroy();
    }
  } else if (OB_UNLIKELY(cursor_id_pair->get_server_cursor_id() != ps_id_pair->get_server_ps_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("same client_ps_id, but server ps id and server cursor id id different",
        K(ret), K(client_ps_id), KPC(cursor_id_pair), KPC(ps_id_pair));
  }

  if (OB_SUCC(ret)) {
    ObMysqlServerSession *ss = s.sm_->get_server_session();
    const sockaddr &addr = ss->get_netvc()->get_remote_addr();
    ObCursorIdAddr *cursor_id_addr = cs_info.get_cursor_id_addr(client_ps_id);
    if (NULL == cursor_id_addr) {
      if (OB_FAIL(ObCursorIdAddr::alloc_cursor_id_addr(client_ps_id, addr, cursor_id_addr))) {
        PROXY_API_LOG(WDIAG, "fail to alloc cursor id addr", K(client_ps_id), K(ret));
      } else if (OB_ISNULL(cursor_id_addr)) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_API_LOG(WDIAG, "cursor_id_addr is null", K(cursor_id_addr), K(ret));
      } else if (OB_FAIL(cs_info.add_cursor_id_addr(cursor_id_addr))) {
        PROXY_API_LOG(WDIAG, "fail to add cursor_id_addr", KPC(cursor_id_addr), K(ret));
        cursor_id_addr->destroy();
      }
    } else {
      /* 同一个 ps id 可能多次执行, 每次执行可能会换不同的 server, 所以需要重新保存目的 server 地址 */
      cursor_id_addr->set_addr(addr);
    }
  }

  if (OB_SUCC(ret)) {
    ObClientSessionInfo &cs_info = get_client_session_info(s);
    cs_info.remove_piece_info(cs_info.get_client_ps_id());
  }
  return ret;
}

inline int ObMysqlTransact::do_handle_prepare_execute_xa_succ(ObIOBufferReader &buf_reader) {
  int ret = OB_SUCCESS;
  int64_t offset = 0;
  int64_t content_len = 0;
  uint8_t seq = 0;
  ObMysqlPacketReader pkt_reader;
  ObNewRow row;
  ObSMRow sm_row(BINARY, row);
  OMPKRow row_pkt(sm_row);
  OMPKPrepareExecute prepare_execute_pkt;
  int32_t eof_count_before_result_set = 0;
  // read prepare execute resp
  if (OB_FAIL(pkt_reader.get_packet(buf_reader, prepare_execute_pkt))) {
    LOG_WDIAG("fail to get prepare execute resp of xa start", K(ret));
  } else if (prepare_execute_pkt.get_column_num() > 0 && FALSE_IT(++eof_count_before_result_set)) {
  } else if (prepare_execute_pkt.get_param_num() > 0 && FALSE_IT(++eof_count_before_result_set)) {
  } else {
    LOG_DEBUG("succ to get eof count of resp of xa start sync", K(eof_count_before_result_set));
  }

  int32_t eof_count = 0;
  // num-param * coldef & eof & one row coldef & eof
   while (OB_SUCC(ret) && eof_count < eof_count_before_result_set) {
    bool is_eof = false;
    if (OB_FAIL(pkt_reader.is_eof_packet(buf_reader, offset, is_eof))) {
      LOG_WDIAG("fail test eof of resp of xa start sync", K(ret));
    } else if (OB_FAIL(pkt_reader.get_content_len_and_seq(buf_reader, offset, content_len, seq))) {
      LOG_WDIAG("fail to read packet len and seq of resp of xa start sync", K(ret));
    } else {
      offset += content_len + MYSQL_NET_HEADER_LENGTH;
    }
    if (is_eof) {
      ++eof_count;
      LOG_DEBUG("succ to read eof of resp of xa start sync", K(eof_count));
    }
  }

  // read ProtocolBinary::ResultsetRow
  if (OB_SUCC(ret)) {
    if (OB_FAIL(pkt_reader.get_packet(buf_reader, offset, row_pkt))) {
      LOG_WDIAG("fail to get result set row packet of resp of xa start sync", K(ret));
    } else {
      // 0a 00 00 08 00 00 00 00 00 00 00 00 00 00
      // ^^^^^^^^^^^ header
      //             ^^ packet header
      //                ^^ bitmap
      //                   ^^^^^^^^^^^^^^^^^^^^^^^ long/int(8/4bytes)
      const char *content_buf = row_pkt.get_cdata();
      int64_t content_len = row_pkt.get_clen();
      int8_t pkt_hdr = -1;
      int8_t bitmap = -1;
      int32_t xa_result = -1;
      if (OB_FAIL(ObMysqlPacketUtil::get_int1(content_buf, content_len, pkt_hdr))) {
        LOG_WDIAG("fail to get pkt hdr from row packet of resp of xa start sync", K(ret));
      } else if (OB_FAIL(ObMysqlPacketUtil::get_int1(content_buf, content_len, bitmap))) {
        LOG_WDIAG("fail to get bitmap from row packet of resp of xa start sync", K(ret));
      } else if (OB_UNLIKELY(pkt_hdr != 0x00)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("wrong result set row pkt hdr of resp of xa start sync", K(ret), K(pkt_hdr));
      } else if (OB_FAIL(ObMysqlPacketUtil::get_int4(content_buf, content_len, xa_result))) {
        LOG_WDIAG("fail to get xa result of resp of xa start sync", K(ret));
      } else if (OB_UNLIKELY(xa_result != 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to sync xa start", K(xa_result), K(ret));
      } else {
        LOG_DEBUG("succ to sync xa start");
      }
    }
  }

  return ret;
}

inline void ObMysqlTransact::handle_execute_succ(ObTransState &s)
{
  int ret = OB_SUCCESS;
  ObProxyMysqlRequest &client_request = s.trans_info_.client_request_;
  ObString data = client_request.get_req_pkt();
  if (OB_UNLIKELY(data.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("com_stmt_execute packet is empty", K(ret));
  } else {
    const char *pos = data.ptr() + MYSQL_NET_META_LENGTH + 4;
    int8_t flags = 0;
    ObMySQLUtil::get_int1(pos, flags);
    /* 如果请求中带了 Cursor 标志, 并且 response 是 resultSet, 则认为是 Cursor
     * 上面的判断条件不一定准确, 准确是要判断 EOF/OK 包的 status. 但比较麻烦.
     * 简化可能会有误判, 但没关系
     */
    if (flags > 0 && s.trans_info_.server_response_.get_analyze_result().is_resultset_resp()) {
      if (OB_FAIL(do_handle_execute_succ(s))) {
        LOG_WDIAG("fail to do handle execute succ", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
    s.current_.state_ = INTERNAL_ERROR;
  }
}

inline void ObMysqlTransact::handle_prepare_execute_succ(ObTransState &s)
{
  int ret = OB_SUCCESS;

  if (NULL != s.sm_->client_session_) {
    ObIOBufferReader *buf_reader = NULL;
    ObMysqlPacketReader pkt_reader;
    OMPKPrepare prepare_packet;
    uint32_t server_ps_id = 0;
    if (OB_FAIL(do_handle_prepare_response(s, buf_reader))) {
      LOG_WDIAG("fail to handle prepare response", K(ret));
    } else if (OB_ISNULL(buf_reader)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("reader is null, which is unexpected", K(ret));
    } else if (OB_FAIL(pkt_reader.get_packet(*buf_reader, prepare_packet))) {
      LOG_WDIAG("fail to get ok packet from server buffer reader", K(ret));
    } else if (FALSE_IT(server_ps_id = prepare_packet.get_statement_id())) {
    } else if (OB_FAIL(do_handle_prepare_succ(s, server_ps_id))) {
      LOG_WDIAG("fail to do handle prepare succ", K(ret));
    }
    if (OB_SUCC(ret) && s.trans_info_.server_response_.get_analyze_result().is_resultset_resp()) {
      if (OB_FAIL(do_handle_execute_succ(s))) {
        LOG_WDIAG("fail to do handle execute succ", K(ret));
      } else if (s.is_hold_xa_start_ && OB_FAIL(do_handle_prepare_execute_xa_succ(*buf_reader))) {
        LOG_WDIAG("fail to do handle prepare execute xa succ", K(ret));
      } else {}
    }
  }

  if (OB_FAIL(ret)) {
    s.current_.state_ = INTERNAL_ERROR;
  }
}

void ObMysqlTransact::handle_xa_start_sync_succ(ObTransState &s) {
  if (obmysql::OB_MYSQL_COM_STMT_PREPARE_EXECUTE == s.trans_info_.sql_cmd_) {
    handle_prepare_execute_succ(s);
  } else {
    s.current_.state_ = INTERNAL_ERROR;
    LOG_EDIAG("unexpected sql cmd for sync xa start", K(s.trans_info_.sql_cmd_));
  }
  // consume response of non-user request
  LOG_DEBUG("[ObMysqlTransact::handle_resultset_resp] reset xa start flag s.is_hold_xa_start_");
  s.is_hold_xa_start_ = false;
}

void ObMysqlTransact::handle_text_ps_prepare_succ(ObTransState &s)
{
  int ret = OB_SUCCESS;
  ObMysqlServerSession *ss = s.sm_->get_server_session();
  ObClientSessionInfo &cs_info = get_client_session_info(s);
  ObServerSessionInfo &ss_info = get_server_session_info(s);
  uint32_t ps_id = cs_info.get_client_ps_id();
  if (OB_FAIL(ss_info.set_text_ps_version(ps_id))) {
    LOG_WDIAG("add text ps name failed", K(ret));
  } else {
    const sockaddr &addr = ss->get_netvc()->get_remote_addr();
    ObPsIdAddrs *ps_id_addrs = cs_info.get_ps_id_addrs(ps_id);
    if (NULL == ps_id_addrs) {
      if (OB_FAIL(ObPsIdAddrs::alloc_ps_id_addrs(ps_id, addr, ps_id_addrs))) {
        LOG_WDIAG("fail to alloc ps id addrs", K(ps_id), K(ret));
      } else if (OB_ISNULL(ps_id_addrs)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("ps_id_addrs is null", K(ps_id_addrs), K(ret));
      } else if (OB_FAIL(cs_info.add_ps_id_addrs(ps_id_addrs))) {
        LOG_WDIAG("fail to add ps_id_addrs", KPC(ps_id_addrs), K(ret));
        ps_id_addrs->destroy();
      }
    } else {
      if (OB_FAIL(ps_id_addrs->add_addr(addr))) {
        LOG_WDIAG("fail to add addr into ps_id_addrs", "addr", ObIpEndpoint(addr), K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
    s.current_.state_ = INTERNAL_ERROR;
  }
}

void ObMysqlTransact::handle_send_init_sql_succ(ObTransState &s)
{
  int ret = OB_SUCCESS;
  get_client_session_info(s).clear_init_sql();
  int64_t pkt_len = s.trans_info_.server_response_.get_analyze_result().get_last_ok_pkt_len();
  if (pkt_len > ObProxySessionInfoHandler::OB_SIMPLE_OK_PKT_LEN) {
    ObMysqlPacketReader pkt_reader;
    OMPKOK ok_packet;
    ObIOBufferReader *buf_reader = s.sm_->get_server_buffer_reader();
    const ObMySQLCapabilityFlags cap = s.sm_->get_server_session()->get_session_info().get_compatible_capability_flags();
    int64_t offset = buf_reader->read_avail() - pkt_len;
    if (OB_ISNULL(buf_reader)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("reader is null, which is unexpected", K(ret));
    } else if (OB_FAIL(pkt_reader.get_ok_packet(*buf_reader, offset, cap, ok_packet))) {
      LOG_WDIAG("fail to get ok packet from server buffer reader", K(ret));
    } else if (OB_FAIL(ObProxySessionInfoHandler::save_changed_session_info(
                                        s.sm_->get_client_session()->get_session_info(),
                                        s.sm_->get_server_session()->get_session_info(),
                                        false,
                                        true,
                                        ok_packet,
                                        s.trans_info_.server_response_.get_analyze_result(),
                                        s.trace_log_,
                                        false))) {
      s.current_.state_ = INTERNAL_ERROR;
      LOG_WDIAG("fail to save changed session info", K(ret));
    }
  }
}

int ObMysqlTransact::handle_ps_reset_succ(ObTransState &s, bool &is_user_request)
{
  int ret = OB_SUCCESS;
  is_user_request = false;

  /* 把 cursor_id_pair 清除掉 */
  ObClientSessionInfo &cs_info = get_client_session_info(s);
  ObMysqlServerSession *ss = s.sm_->get_server_session();
  ObServerSessionInfo &ss_info = get_server_session_info(s);
  uint32_t client_ps_id = cs_info.get_client_ps_id();
  /* 可以直接删除, 有就删除, 没有就算了 */
  ss_info.remove_cursor_id_pair(client_ps_id);
  cs_info.remove_cursor_id_addr(client_ps_id);
  cs_info.remove_piece_info(client_ps_id);
  if (OB_FAIL(cs_info.remove_request_send_addr(ss->get_netvc()->get_remote_addr()))) {
    LOG_WDIAG("fail to erase server addr", K(ret));
  }

  return ret;
}

int ObMysqlTransact::handle_text_ps_drop_succ(ObTransState &s, bool &is_user_request)
{
  int ret = OB_SUCCESS;
  is_user_request = false;

  // text_ps_drop can not handle session info in trim_ok_packet, handle here
  ObClientSessionInfo &cs_info = get_client_session_info(s);
  ObServerSessionInfo &ss_info = get_server_session_info(s);
  uint32_t ps_id = cs_info.get_client_ps_id();
  ObMysqlServerSession *ss = s.sm_->get_server_session();
  if (OB_FAIL(cs_info.remove_request_send_addr(ss->get_netvc()->get_remote_addr()))) {
    LOG_WDIAG("fail to erase server addr", K(ret));
  } else if (OB_FAIL(ss_info.remove_text_ps_version(ps_id))) {
    LOG_WDIAG("fail to remove text ps id", K(ret));
  } else if (OB_FAIL(ObProxySessionInfoHandler::save_changed_sess_info(s.sm_->get_client_session()->get_session_info(),
                                                                       s.sm_->get_server_session()->get_session_info(),
                                                                       s.trans_info_.server_response_.get_analyze_result().get_extra_info(),
                                                                       s.trace_log_,
                                                                       false))) {
      LOG_WDIAG("fail to save changed session info", K(get_send_action_name(s.current_.send_action_)));
      s.current_.state_ = INTERNAL_ERROR;
  }
  return ret;
}

/* Oceanbase:
 *   用户变量: 无需处理, 一定是用户 set 设置的, 在 save_changed_session_info 中处理即可
 *   系统变量: 在处理之前需要先把用户设置的部分放到 common_sys 中,
 *             在 save_changed_session_info 中先检查 comomn_sys 中有没有,
 *             如果有的话，更新 common_sys, 如果没有, 再更新 sys
 * Mysql:
 *   用户变量: 不会通过 OK 包返回, 收到 OK 包后, 从请求中取出保存
 *   系统变量：在处理之前需要先把用户设置的部分放到 common_sys 中,
 *             然后从响应中取出保存, 保存时先检查 common_sys 中有没有,
 *             如果有的话, 更新 common_sys, 如果没有, 再更新 sys
 *
 */
inline void ObMysqlTransact::handle_user_request_succ(ObTransState &s, bool &is_user_request)
{
  int ret = OB_SUCCESS;
  is_user_request = true;

  ObClientSessionInfo& client_info = get_client_session_info(s);
  ObProxyMysqlRequest& client_request = s.trans_info_.client_request_;

  if (client_info.is_sharding_user() && !client_info.is_oceanbase_server()) {
    const ObProxyBasicStmtType type = client_request.get_parse_result().get_stmt_type();

    //如果是 SET 命令, 系统变量需要先保存, 这样才能知道用户发过来是的什么类型
    //需要排除 SET NAMES 和 SET CHARSET 命令, 因为这两个命令的系统变量名不是 NAMES 和 CHARSET,
    //  但他们的值是字符串, 所以这里可以不用先保存
    if (OBPROXY_T_SET == type && OB_FAIL(handle_user_set_request_succ(s))) {
      LOG_WDIAG("fail to handle user set request", K(ret));
    } else if (OB_FAIL(handle_normal_user_request_succ(s))) {
      LOG_WDIAG("fail to handle nomal user request", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (obmysql::OB_MYSQL_COM_CHANGE_USER == s.trans_info_.sql_cmd_) {
      if (OB_FAIL(handle_change_user_request_succ(s))) {
        LOG_WDIAG("fail to handle change user request", K(ret));
      }
    } else if (obmysql::OB_MYSQL_COM_RESET_CONNECTION == s.trans_info_.sql_cmd_) {
      if (OB_FAIL(handle_reset_connection_request_succ(s))) {
        LOG_WDIAG("fail to handle reset connection request on success", K(ret));
      }
    } else if (obmysql::OB_MYSQL_COM_STMT_RESET == s.trans_info_.sql_cmd_) {
      if (OB_FAIL(handle_ps_reset_succ(s, is_user_request))) {
        LOG_WDIAG("fail to handle ps reset request", K(ret));
      }
    } else if (client_request.get_parse_result().is_text_ps_drop_stmt()) {
      if (OB_FAIL(handle_text_ps_drop_succ(s, is_user_request))) {
        LOG_WDIAG("fail to handle text ps drop request", K(ret));
      }
    } else if (client_request.get_parse_result().is_text_ps_prepare_stmt()) {
      handle_text_ps_prepare_succ(s);
    } else if (OB_MYSQL_COM_LOAD_DATA_TRANSFER_CONTENT == s.trans_info_.sql_cmd_) {
      LOG_DEBUG("[handle_user_request_succ] sm ends to transfer content of file request");
    }
  }

  if (OB_FAIL(ret)) {
    s.current_.state_ = INTERNAL_ERROR;
  }
}

inline int ObMysqlTransact::handle_change_user_request_succ(ObTransState &s)
{
  int ret = OB_SUCCESS;
  ObClientSessionInfo& client_info = get_client_session_info(s);
  ObProxyMysqlRequest& client_request = s.trans_info_.client_request_;
  ObString change_user = client_request.get_req_pkt();
  OMPKChangeUser result;
  if (OB_UNLIKELY(change_user.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("client request is empty", K(change_user), K(ret));
  } else {
    const char *start = change_user.ptr() + MYSQL_NET_META_LENGTH;
    int32_t len = static_cast<uint32_t>(change_user.length() - MYSQL_NET_META_LENGTH);
    if (len <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("client request packet is error", K(ret));
    } else {
      result.set_content(start, len);
      const ObMySQLCapabilityFlags &capability = client_info.get_orig_capability_flags();
      result.set_capability_flag(capability);
      if (OB_FAIL(result.decode())) {
        LOG_WDIAG("fail to decode change user packet", K(ret));
      } else {
        // 1. 保存 username 和 auth_response
        if (OB_FAIL(ObProxySessionInfoHandler::rewrite_change_user_login_req(
          client_info, result.get_username(), result.get_auth_response()))) {
          LOG_WDIAG("rewrite change user login req failed", K(ret));
        } else {
          /*
           * change_user API 里面会填写一个database进来。所以observer端会根据这个database做switch
           * server 会清理 session var
           * 事务中允许执行change_user，执行后会强行rollback当前事务
           * 上面三个版本号同步已经在save_changed_session_info里面处理过，不需要再处理
           */

          // 2. update auth_str
          if (OB_SUCC(ret)) {
            const ObString full_username = client_info.get_full_username();
            ObMysqlServerSession* server_session = s.sm_->get_server_session();
            if (NULL != server_session && full_username.length() < OB_PROXY_FULL_USER_NAME_MAX_LEN) {
              MEMCPY(server_session->full_name_buf_, full_username.ptr(), full_username.length());
              server_session->auth_user_.assign_ptr(server_session->full_name_buf_, full_username.length());
              LOG_DEBUG("handle normal change user request succ", K(server_session->auth_user_));
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WDIAG("username length is error", K(full_username.length()), K(ret));
            }
          }

          // 3. clear session related source
          // 4. clear all server session, reconnect if needed
          if (OB_SUCC(ret)
              && OB_FAIL(clear_session_related_source(s))) {
            LOG_WDIAG("fail to clear session related source", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObMysqlTransact::handle_reset_connection_request_succ(ObTransState &s)
{
  int ret = OB_SUCCESS;

  // clear session related source
  // clear all server session, reconnect if needed
  if (OB_FAIL(clear_session_related_source(s))) {
    LOG_WDIAG("fail to clear session related source", K(ret));
  }

  return ret;
}

// clear after OB_MYSQL_COM_RESET_CONNECTION / OB_MYSQL_COM_CHANGE_USR
int ObMysqlTransact::clear_session_related_source(ObTransState &s)
{
  int ret = OB_SUCCESS;

  ObClientSessionInfo &client_info = get_client_session_info(s);
  ObServerSessionInfo &server_info = get_server_session_info(s);

  // reset user variable
  if (OB_FAIL(client_info.remove_all_user_variable())) {
    LOG_WDIAG("fail to remove all user variables", K(ret));
  } else {
    // reset session state, including rollback any open transaction, reset transaction isolation level
    // reset session variables, delete user variables, remove prepare statement
    client_info.destroy_ps_id_entry_map();
    client_info.destroy_ps_id_addrs_map();
    client_info.destroy_cursor_id_addr_map();
    client_info.destroy_piece_info_map();
    client_info.destroy_text_ps_name_entry_map();

    // server session
    server_info.destroy_ps_id_pair_map();
    server_info.destroy_cursor_id_pair_map();
    server_info.reuse_text_ps_version_set();

    // close other server sessions, avoid to send every server session, reconnect if needed
    ObMysqlClientSession *client_session = s.sm_->get_client_session();
    if (OB_NOT_NULL(client_session)) {
      client_session->get_session_manager().purge_keepalives();
    }

    LOG_DEBUG("succ to clear session related source");
  }

  return ret;
}

inline int ObMysqlTransact::handle_user_set_request_succ(ObTransState &s)
{
  int ret = OB_SUCCESS;

  ObSqlParseResult &sql_result = s.trans_info_.client_request_.get_parse_result();
  ObProxySetInfo &set_info = sql_result.get_set_info();
  if (OB_ISNULL(s.sm_->client_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid session, client session is NULL");
  } else if (OB_LIKELY(set_info.node_count_ > 0)) {
    ObClientSessionInfo &client_info = get_client_session_info(s);
    for (int i = 0; OB_SUCC(ret) && i < set_info.node_count_; i++) {
      SetVarNode* var_node = set_info.var_nodes_.at(i);
      ObObj value;
      if (SET_VALUE_TYPE_INT == var_node->value_type_) {
        value.set_int(var_node->int_value_);
      } else if (SET_VALUE_TYPE_NUMBER == var_node->value_type_) {
        const char *nptr = var_node->str_value_.ptr();
        char *end_ptr = NULL;
        double double_val = strtod(nptr, &end_ptr);
        if (*nptr != '\0' && *end_ptr == '\0') {
          value.set_double(double_val);
        } else {
          OBLOG_LOG(WDIAG, "invalid double value", "str", var_node->str_value_);
          value.set_varchar(var_node->str_value_.config_string_);
        }
      } else {
        if (0 == var_node->var_name_.config_string_.case_compare("character_set_results")
            && 0 == var_node->str_value_.config_string_.case_compare("NULL")) {
          value.set_varchar(ObString::make_empty_string());
        } else {
          value.set_varchar(var_node->str_value_.config_string_);
        }
      }

      // 这里可以优化下, 如果是没有, 则直接插入, 如果有的话, 则对比再更新
      // 虽然 set 了, 但是实际没变化, 也不用更新
      if (SET_VAR_USER == var_node->var_type_) {
        if (OB_FAIL(client_info.replace_user_variable(var_node->var_name_.config_string_,
                                                      value))) {
          LOG_WDIAG("fail to replace user variable", KPC(var_node), K(ret));
        } else {
          LOG_DEBUG("succ to update user variables", "key", var_node->var_name_, K(value));
        }
      } else if (SET_VAR_SYS == var_node->var_type_) {
        if (OB_FAIL(client_info.update_common_sys_variable(var_node->var_name_.config_string_,
                                                           value, true, false))) {
          LOG_WDIAG("fail to update common sys variable", KPC(var_node), K(ret));
        } else {
          LOG_DEBUG("succ to update common sys variables", "key", var_node->var_name_.config_string_, K(value));
        }
      }
    }
  }

  return ret;
}

inline int ObMysqlTransact::handle_normal_user_request_succ(ObTransState &s)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(s.sm_->client_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid session, client session is NULL");
  } else {
    ObServerSessionInfo &server_info = get_server_session_info(s);
    ObClientSessionInfo &client_info = get_client_session_info(s);
    ObIOBufferReader *buf_reader = s.sm_->get_server_buffer_reader();
    ObMysqlPacketReader pkt_reader;
    OMPKOK ok_packet;
    const ObMySQLCapabilityFlags cap = server_info.get_compatible_capability_flags();

    if (OB_ISNULL(buf_reader)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("reader is null, which is unexpected", K(ret));
    } else if (OB_FAIL(pkt_reader.get_ok_packet(*buf_reader, 0, cap, ok_packet))) {
      LOG_WDIAG("fail to get ok packet from server buffer reader", K(ret));
    } else {
      if (ok_packet.is_schema_changed()) {
        const ObString &db_name = ok_packet.get_changed_schema();
        if (!db_name.empty()) {
          bool is_string_to_lower_case = client_info.is_oracle_mode() ? false : client_info.need_use_lower_case_names();
          if (OB_FAIL(client_info.set_database_name(db_name))) {
            LOG_WDIAG("fail to set changed database name", K(db_name), K(ret));
          } else if (OB_FAIL(server_info.set_database_name(db_name, is_string_to_lower_case))) {
            LOG_WDIAG("fail to set changed database name", K(db_name), K(ret));
          }
        }

        LOG_DEBUG("succ to update schema", K(db_name));
      }

      ObSqlParseResult &sql_result = s.trans_info_.client_request_.get_parse_result();
      const ObProxyBasicStmtType type = sql_result.get_stmt_type();
      bool is_save_to_common_sys = (OBPROXY_T_SET == type || OBPROXY_T_SET_NAMES == type || OBPROXY_T_SET_CHARSET == type);

      const ObIArray<ObStringKV> &sys_var = ok_packet.get_system_vars();
      for (int64_t i = 0; OB_SUCC(ret) && i < sys_var.count(); ++i) {
        const ObStringKV &str_kv = sys_var.at(i);
        if (is_save_to_common_sys) {
          ret = client_info.update_common_sys_variable(str_kv.key_, str_kv.value_, true, false);
        } else {
          ret = client_info.update_mysql_sys_variable(str_kv.key_, str_kv.value_);
        }

        if (OB_FAIL(ret)) {
          LOG_WDIAG("fail to update mysql sys variable", K(str_kv), K(ret));
        } else {
          LOG_DEBUG("succ to update mysql sys variables", "key", str_kv.key_, "value", str_kv.value_);
        }
      }

      if (OB_SUCC(ret)) {
        // all server version expect last_insert_id will be set to client version
        ObProxySessionInfoHandler::assign_session_vars_version(client_info, server_info);
        ObProxySessionInfoHandler::assign_database_version(client_info, server_info);
      }
    }
  }

  return ret;
}

inline void ObMysqlTransact::handle_saved_login_succ(ObTransState &s)
{
  int ret = OB_SUCCESS;

  if (s.sm_->client_session_->get_session_info().is_oceanbase_server()) {
    if (OB_FAIL(handle_oceanbase_saved_login_succ(s))) {
      LOG_WDIAG("fail to handle oceanbase saved login succ", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    s.current_.state_ = INTERNAL_ERROR;
  }
}

inline int ObMysqlTransact::handle_oceanbase_saved_login_succ(ObTransState &s)
{
  int ret = OB_SUCCESS;

  if (NULL != s.sm_->client_session_) {
    ObClientSessionInfo &client_info = get_client_session_info(s);
    ObServerSessionInfo &server_info = get_server_session_info(s);
    ObIOBufferReader *buf_reader = s.sm_->get_server_buffer_reader();
    ObMysqlPacketReader pkt_reader;
    OMPKOK ok_packet;
    const ObMySQLCapabilityFlags cap = server_info.get_compatible_capability_flags();
    bool need_save = false;

    if (OB_ISNULL(buf_reader)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("reader is null, which is unexpected", K(ret));
    } else if (OB_FAIL(pkt_reader.get_ok_packet(*buf_reader, 0, cap, ok_packet))) {
      LOG_WDIAG("fail to get ok packet from server buffer reader", K(ret));
    } else {
      const ObIArray<ObStringKV> &sys_var = ok_packet.get_system_vars();
      // current, we only care about OB_SV_PROXY_GLOBAL_VARIABLES_VERSION and OB_SV_CAPABILITY_FLAG
      for (int64_t i = 0; i < sys_var.count() && OB_SUCC(ret); ++i) {
        const ObStringKV &str_kv = sys_var.at(i);
        // check global vars version, if the global vars has changed, we no need to check again
        if (str_kv.key_ == OB_SV_PROXY_GLOBAL_VARIABLES_VERSION
            && !client_info.is_global_vars_changed()) {
          if (OB_FAIL(check_global_vars_version(s, str_kv))) {
            LOG_WDIAG("fail to check global vars version", K(str_kv), K(ret));
          }
        } else if (str_kv.key_ == OB_SV_CAPABILITY_FLAG) {
          if (OB_FAIL(ObProxySessionInfoHandler::handle_capability_flag_var(
                  client_info, server_info, str_kv.value_, false, need_save))) {
            LOG_WDIAG("fail to handle capability flag var", K(str_kv), K(ret));
          }
        } else {} // do not handle other vars
      } //  end of for
    }
  }

  return ret;
}

inline void ObMysqlTransact::handle_use_db_succ(ObTransState &s)
{
  int ret = OB_SUCCESS;

  ObClientSessionInfo &client_info = get_client_session_info(s);
  ObServerSessionInfo &server_info = get_server_session_info(s);

  ObProxySessionInfoHandler::assign_database_version(client_info, server_info);

  if (client_info.is_sharding_user()) {
    ObString db_name;
    bool is_string_to_lower_case = client_info.is_oracle_mode() ? false : client_info.need_use_lower_case_names();
    if (OB_FAIL(client_info.get_database_name(db_name))) {
      LOG_WDIAG("fail to get client info database name", K(db_name), K(ret));
    } else if (OB_FAIL(server_info.set_database_name(db_name, is_string_to_lower_case))) {
      LOG_WDIAG("fail to set changed database name", K(db_name), K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    s.current_.state_ = INTERNAL_ERROR;
  }
}

void ObMysqlTransact::handle_error_resp(ObTransState &s, bool &is_user_request)
{
  ObRespAnalyzeResult &resp = s.trans_info_.server_response_.get_analyze_result();
  ObProxyMysqlRequest& client_request = s.trans_info_.client_request_;
  is_user_request = false;
  int64_t max_connect_attempts = get_max_connect_attempts(s);
  ObServerStateType pre_state = s.current_.state_;
  s.current_.state_ = RESPONSE_ERROR;

  s.trace_log_.log_it("[get_error]", "code", static_cast<int64_t>(resp.error_pkt_.get_err_code()), "trace_id", resp.server_trace_id_);
  if (resp.is_mysql_wrong_arguments_error()
      || resp.is_trans_free_route_not_supported_error()
      || resp.is_internal_error()) {
    LOG_WDIAG("trace_log", K(s.trace_log_));
  }

  if (resp.is_client_session_killed_error()) {
    COLLECT_INTERNAL_DIAGNOSIS(
        s.sm_->connection_diagnosis_trace_, obutils::OB_SERVER_INTERNAL_TRACE,
        resp.error_pkt_.get_err_code(),
        "this session was killed by other user session, error_code: %d, "
        "error_msg: %.*s",
        resp.error_pkt_.get_err_code(), resp.error_pkt_.get_message().length(), resp.error_pkt_.get_message().ptr());
  }

  switch(s.current_.send_action_) {
    case SERVER_SEND_HANDSHAKE:
      if (!s.sm_->client_session_->get_session_info().is_oceanbase_server()) {
        if (OB_SUCCESS != s.sm_->get_client_buffer_reader()->consume_all()) {
          s.current_.state_ = INTERNAL_ERROR;
          LOG_WDIAG("fail to consume client buffer reader", "state", s.current_.state_);
        } else {
          is_user_request = true;
        }
      } else {
        s.current_.error_type_ = HANDSHAKE_COMMON_ERROR;
      }
      break;

    case SERVER_SEND_LOGIN:
      if (s.current_.attempts_ < max_connect_attempts) {
        if (resp.is_server_init_error()) {
          // this error will not return to user, we will retry
          s.current_.error_type_ = LOGIN_SERVER_INIT_ERROR;
        } else if (resp.is_server_stopping_error()) {
          s.current_.error_type_ = LOGIN_SERVER_STOPPING_ERROR;
        } else if (resp.is_tenant_not_in_server_error()) {
          s.current_.error_type_ = LOGIN_TENANT_NOT_IN_SERVER_ERROR;
        } else if (resp.is_connect_error()) {
          s.current_.error_type_ = LOGIN_CONNECT_ERROR;
        } else if (resp.is_cluster_not_match_error()) {
          s.current_.error_type_ = LOGIN_CLUSTER_NOT_MATCH_ERROR;
          LOG_WDIAG("cluster is not matched, maybe server had been migrated to other cluster",
                   "send_action", get_send_action_name(s.current_.send_action_),
                   "state", get_server_state_name(s.current_.state_),
                   "proxy_id", s.mysql_config_params_->proxy_id_,
                   "server_sessid", s.sm_->get_server_session()->get_server_sessid(),
                   "cluster_name", s.sm_->get_client_session()->get_session_info().get_priv_info().cluster_name_,
                   K(resp));
          if (s.sm_->get_client_session()->get_session_info().get_priv_info().cluster_name_ == OB_META_DB_CLUSTER_NAME) {
            LOG_WDIAG("this is MetaDataBase, proxy need rebuild this cluster resource later",
                     "real_cluster_id", s.sm_->get_client_session()->get_cluster_id(),
                     "real_cluster_name", s.sm_->get_client_session()->get_real_cluster_name());
            s.sm_->get_client_session()->set_need_delete_cluster();
            s.current_.state_ = INTERNAL_ERROR;
          }
        } else if (resp.is_session_entry_exist()) {
          if(OB_CLIENT_SERVICE_MODE == s.mysql_config_params_->proxy_service_mode_) {
            // if use OB_CLIENT_SERVICE_MODE, session id from observer maybe repetitive,
            // obproxy should hide it, and clear the saved conn id
            s.current_.error_type_ = LOGIN_SESSION_ENTRY_EXIST_ERROR;
          } else {
            // if use OB_SERVER_SERVICE_MODE, session id from obproxy maybe repetitive,
            // it will happen when two proxy use the same proxy_id, obproxy should waring it
            LOG_EDIAG("connection id is repetitive, current proxy_id maybe already "
                      "used by others, need DBA interference. ",
                      "send_action", get_send_action_name(s.current_.send_action_),
                      "state", get_server_state_name(s.current_.state_),
                      "proxy_id", s.mysql_config_params_->proxy_id_,
                      "server_sessid", s.sm_->get_server_session()->get_server_sessid(),
                      K(resp));
            is_user_request = true;
          }
        } else {
          is_user_request = true;

          // case above may retry OB_MYSQL_COM_LOGIN
          // record login diagnosis log here because client will disconnect while receive error packet
          char diagnosis_msg[MAX_MSG_BUF_LEN] = "\0";
          int new_msg_len = 0;
          ObProxyMonitorUtils::sql_escape(
              resp.get_error_message().ptr(), resp.get_error_message().length(),
              diagnosis_msg, MAX_MSG_BUF_LEN, new_msg_len);
          COLLECT_LOGIN_DIAGNOSIS(
              s.sm_->connection_diagnosis_trace_, OB_LOGIN_DISCONNECT_TRACE, "",
              resp.get_error_pkt().get_err_code(), "%.*s", MAX_MSG_BUF_LEN, diagnosis_msg);
        }
      } else {
        is_user_request = true;
        char diagnosis_msg[MAX_MSG_BUF_LEN] = "\0";
        int new_msg_len = 0;
        ObProxyMonitorUtils::sql_escape(
            resp.get_error_message().ptr(), resp.get_error_message().length(),
            diagnosis_msg, MAX_MSG_BUF_LEN, new_msg_len);
        COLLECT_LOGIN_DIAGNOSIS(
            s.sm_->connection_diagnosis_trace_, OB_LOGIN_DISCONNECT_TRACE, "",
            resp.get_error_pkt().get_err_code(), "%.*s", MAX_MSG_BUF_LEN, diagnosis_msg);
      }
      break;

    case SERVER_SEND_SAVED_LOGIN:
      if (resp.is_tenant_not_in_server_error()) {
        s.current_.error_type_ = LOGIN_TENANT_NOT_IN_SERVER_ERROR;
      } else if (resp.is_connect_error()) {
        s.current_.error_type_ = LOGIN_CONNECT_ERROR;
      } else if (resp.is_cluster_not_match_error()) {
        s.current_.error_type_ = LOGIN_CLUSTER_NOT_MATCH_ERROR;
        s.current_.state_ = INTERNAL_ERROR;
        LOG_WDIAG("cluster is not matched, maybe server had been migrated to other cluser",
                 "send_action", get_send_action_name(s.current_.send_action_),
                 "state", get_server_state_name(s.current_.state_),
                 "proxy_id", s.mysql_config_params_->proxy_id_,
                 "server_sessid", s.sm_->get_server_session()->get_server_sessid(),
                 "cluster_name", s.sm_->get_client_session()->get_session_info().get_priv_info().cluster_name_,
                 K(resp));
        if (s.sm_->get_client_session()->get_session_info().get_priv_info().cluster_name_ == OB_META_DB_CLUSTER_NAME) {
          LOG_WDIAG("this is MetaDataBase, proxy need rebuild this cluster resource later",
                   "real_cluster_id", s.sm_->get_client_session()->get_cluster_id(),
                   "real_cluster_name", s.sm_->get_client_session()->get_real_cluster_name());
          s.sm_->get_client_session()->set_need_delete_cluster();
          s.current_.state_ = INTERNAL_ERROR;
        }
      } else if (resp.is_session_entry_exist()) {
        s.current_.error_type_ = LOGIN_SESSION_ENTRY_EXIST_ERROR;

        //if this is first server retry, no need close connect, delay this event
        if(OB_SERVER_SERVICE_MODE == s.mysql_config_params_->proxy_service_mode_) {
          s.current_.state_ = INTERNAL_ERROR;
          LOG_EDIAG("connection id is repetitive, current proxy_id maybe already "
                    "used by others, need DBA interference. ",
                    "send_action", get_send_action_name(s.current_.send_action_),
                    "state", get_server_state_name(s.current_.state_),
                    "proxy_id", s.mysql_config_params_->proxy_id_,
                    "proxy_sessid", s.sm_->get_client_session()->get_proxy_sessid(),
                    "server_sessid", s.sm_->get_server_session()->get_server_sessid(),
                    K(resp));
        }
      } else if (!s.sm_->client_session_->get_session_info().is_oceanbase_server()) {
        is_user_request = true;
      } else {
        s.current_.error_type_ = SAVED_LOGIN_COMMON_ERROR;
      }
      break;

    case SERVER_SEND_REQUEST: {
      // if we get an OB_TENANT_NOT_IN_SERVER or OB_SERVER_IS_INIT or OB_SERVER_IS_STOPPING
      // in send request process not in auth process,
      // that means the tenant's resource in this observer has been migrated to another observer,
      // so we should retry other observers;
      //
      // other conditions:
      //  1. must be the first request in one transaction;
      //  2. not the large request(request len > 8KB)
      //  3. cached request packet(add_reqeust()) == total request len, all request packet has been cached
      int64_t total_request_packet_len = s.trans_info_.client_request_.get_packet_len();
      int64_t cached_request_packet_len = s.trans_info_.client_request_.get_req_pkt().length();

      if (resp.is_client_session_killed_error()) {
        // client session killed, disconnect
        s.current_.error_type_ = CLIENT_SESSION_KILLED_ERROR;
        is_user_request = false;
      } else if (obmysql::OB_MYSQL_COM_STMT_RESET == s.trans_info_.sql_cmd_ ||
        s.trans_info_.client_request_.get_parse_result().is_text_ps_drop_stmt()) {
        // RESET 请求因为要发送给指定 Server, 如果该 Server 执行发生错误，直接返回错误包
        if (OB_SUCCESS != s.sm_->get_client_buffer_reader()->consume_all()) {
          s.current_.state_ = INTERNAL_ERROR;
          LOG_WDIAG("fail to consume client buffer reader", "state", s.current_.state_);
        } else {
          is_user_request = true;
        }
      } else if (resp.is_trans_free_route_not_supported_error()) {
        s.current_.error_type_ = TRANS_FREE_ROUTE_NOT_SUPPORTED_ERROR;
      } else if (resp.is_ora_fatal_error()) {
        s.current_.error_type_ = ORA_FATAL_ERROR;
      } else if (resp.is_standby_weak_readonly_error()
                 && s.sm_->sm_cluster_resource_->is_deleting()
                 && OB_DEFAULT_CLUSTER_ID == s.sm_->sm_cluster_resource_->get_cluster_id()) {
        // proxy感知到主集群switchover 之后,需要对STANDBY_WEAK_READONLY_ERROR 做断连接处理
        s.current_.error_type_ = STANDBY_WEAK_READONLY_ERROR;
      } else if ((s.is_trans_first_request_
                    || (ObMysqlTransact::is_in_trans(s) && s.sm_->get_client_session()->is_trans_internal_routing()))
                  && !s.trans_info_.client_request_.is_large_request()
                  && (total_request_packet_len == cached_request_packet_len)) {
        if (resp.is_reroute_error()) {
          s.current_.error_type_ = REQUEST_REROUTE_ERROR;
        // if all attempts were failed, we should transfer the error pkt to client finally
        } else if (s.current_.attempts_ < max_connect_attempts) {
          if (resp.is_tenant_not_in_server_error()) {
            s.current_.error_type_ = REQUEST_TENANT_NOT_IN_SERVER_ERROR;
          } else if (resp.is_server_stopping_error()) {
            s.current_.error_type_ = REQUEST_SERVER_STOPPING_ERROR;
          } else if (resp.is_server_init_error()) {
            s.current_.error_type_ = REQUEST_SERVER_INIT_ERROR;
          } else if (resp.is_connect_error()) {
            s.current_.error_type_ = REQUEST_CONNECT_ERROR;
          } else if (resp.is_readonly_error()) {
            s.current_.error_type_ = REQUEST_READ_ONLY_ERROR;
          } else {
            is_user_request = true;
          }
        } else {
          // transfer the final response to client
          is_user_request = true;
        }
      } else {
        is_user_request = true;
      }

      /* 如果是要转发给用户的 error 请求, 则说明该 Prepare 请求执行失败, 需要清理掉之前创建的 ps_id_entry */
      if (is_user_request
          && obmysql::OB_MYSQL_COM_STMT_PREPARE == s.trans_info_.sql_cmd_) {
        ObClientSessionInfo &cs_info = get_client_session_info(s);
        uint32_t client_ps_id = cs_info.get_client_ps_id();
        cs_info.remove_ps_id_entry(client_ps_id);
      }
      if (is_user_request && client_request.get_parse_result().is_text_ps_prepare_stmt()) {
        ObClientSessionInfo &cs_info = get_client_session_info(s);
        ObString text_ps_name = client_request.get_parse_result().get_text_ps_name();
        cs_info.delete_text_ps_name_entry(text_ps_name);
      }
      break;
    }
    case SERVER_SEND_ALL_SESSION_VARS:
    case SERVER_SEND_SESSION_VARS:
    case SERVER_SEND_SESSION_USER_VARS:
      s.current_.error_type_ = RESET_SESSION_VARS_COMMON_ERROR;
      break;

    case SERVER_SEND_USE_DATABASE:
      s.current_.error_type_ = SYNC_DATABASE_COMMON_ERROR;
      break;

    case SERVER_SEND_START_TRANS:
      s.current_.error_type_ = START_TRANS_COMMON_ERROR;
      break;

    case SERVER_SEND_XA_START:
      LOG_DEBUG("[ObMysqlTransact::handle_error_resp] xa start resp error");
      s.current_.error_type_ = START_XA_START_ERROR;
      break;

    case SERVER_SEND_PREPARE:
      s.current_.error_type_ = SYNC_PREPARE_COMMON_ERROR;
      break;

    case SERVER_SEND_TEXT_PS_PREPARE:
      s.current_.error_type_ = SYNC_TEXT_PS_PREPARE_COMMON_ERROR;
      break;

    case SERVER_SEND_INIT_SQL: {
      int ret = OB_SUCCESS;
      s.current_.error_type_ = SEND_INIT_SQL_ERROR;
      char err_msg_buf[OB_MAX_ERROR_MSG_LEN] = "\0";
      int64_t pos = 0;
      ObString init_sql = get_client_session_info(s).get_init_sql();
      databuff_printf(err_msg_buf, OB_MAX_ERROR_MSG_LEN, pos, ob_str_user_error(OB_SEND_INIT_SQL_ERROR),
                      init_sql.length(), init_sql.ptr());
      s.mysql_errcode_ = OB_SEND_INIT_SQL_ERROR;
      s.mysql_errmsg_ = err_msg_buf;
      COLLECT_INTERNAL_DIAGNOSIS(
          s.sm_->connection_diagnosis_trace_, obutils::OB_SERVER_INTERNAL_TRACE,
          s.mysql_errcode_, "%.*s", static_cast<int>(OB_MAX_ERROR_MSG_LEN), s.mysql_errmsg_);
      if (OB_FAIL(ObMysqlTransact::encode_error_message(s))) {
        LOG_WDIAG("fail to build err resp", K(s.current_.error_type_), K(ret));
      } else {
        s.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
      }
      break;
    }

    case SERVER_SEND_NONE:
    default:
      s.current_.state_ = INTERNAL_ERROR;
      LOG_EDIAG("unknown send action", K(s.current_.send_action_));
      break;
  }
  // if tenant no in server or cluster not match, update pl
  if (REQUEST_TENANT_NOT_IN_SERVER_ERROR == s.current_.error_type_
      || LOGIN_TENANT_NOT_IN_SERVER_ERROR == s.current_.error_type_
      || LOGIN_CLUSTER_NOT_MATCH_ERROR == s.current_.error_type_) {
    if (s.pll_info_.set_dirty_all(s.pll_info_.is_remote_readonly())) {
      //if dummy/target entry has clipped, we can update it here
      LOG_INFO("received login error, the route entry is expired, "
               "set it to dirty, and wait for updating",
               "addr", s.server_info_.addr_,
               "error_type", get_server_resp_error_name(s.current_.error_type_),
               "pll_info", s.pll_info_);
    }
  } else if (LOGIN_CONNECT_ERROR == s.current_.error_type_
             || REQUEST_CONNECT_ERROR == s.current_.error_type_) {
    if (s.pll_info_.set_target_dirty()) {
      // this error means maybe server is electing a new leader
      // so we need update table entry
      LOG_INFO("received connect error, the route entry is expired, "
               "set it to dirty, and wait for updating",
               "addr", s.server_info_.addr_,
               "error_type", get_server_resp_error_name(s.current_.error_type_),
               "pll_info", s.pll_info_);
    }
  } else if (REQUEST_REROUTE_ERROR == s.current_.error_type_) {
    if (s.is_rerouted_) {
      s.current_.state_ = INTERNAL_ERROR;
    } else  {
      ObMySQLCapabilityFlags cap = get_server_session_info(s).get_compatible_capability_flags();
      ObProto20Utils::analyze_ok_packet_and_get_reroute_info(s.sm_->get_server_buffer_reader(),
                                                             s.trans_info_.server_response_.get_analyze_result().get_last_ok_pkt_len(),
                                                             cap, s.reroute_info_);
      LOG_INFO("received reroute error, mayby need reroute",
               "addr", s.server_info_.addr_,
               "pll_info", s.pll_info_,
               "state", get_server_state_name(s.current_.state_),
               "error_type", get_server_resp_error_name(s.current_.error_type_),
               "reroute_info", s.reroute_info_);
      int64_t table_name_len = strlen(s.reroute_info_.table_name_buf_);
      ObString sql_id = s.trans_info_.client_request_.get_sql_id();
      ObSqlParseResult &sql_result = s.trans_info_.client_request_.get_parse_result();
      if (is_need_use_sql_table_cache(s)
          && !sql_id.empty()
          && table_name_len > 0
          && sql_result.get_table_name() != s.reroute_info_.table_name_buf_) {
        // 大小写敏感，server返回的表名是真实的物理表名
        // update sql table cache
        ObClientSessionInfo &cs_info = get_client_session_info(s);
        ObString cluster_name = cs_info.get_priv_info().cluster_name_;
        ObString tenant_name = cs_info.get_priv_info().tenant_name_;
        ObString database_name = sql_result.get_database_name();
        if (database_name.empty()) {
          if (OB_SUCCESS != cs_info.get_database_name(database_name)) {
            database_name = OB_SYS_DATABASE_NAME;
          }
        }
        ObSqlTableEntryKey key;
        key.cr_version_ = s.sm_->sm_cluster_resource_->version_;
        key.cr_id_ = s.sm_->sm_cluster_resource_->get_cluster_id();
        key.sql_id_ = sql_id;
        key.cluster_name_ = cluster_name;
        key.tenant_name_ = tenant_name;
        key.database_name_ = database_name;
        if (OB_SUCCESS != get_global_sql_table_cache().update_table_name(key, ObString::make_string(s.reroute_info_.table_name_buf_))) {
          LOG_WDIAG("fail to update real table name", K(s.reroute_info_), K(key),
                    "sql", s.trans_info_.client_request_.get_print_sql());
        } else {
          LOG_INFO("succ to update real table name", K(s.reroute_info_), K(key),
          "sql", s.trans_info_.client_request_.get_print_sql());
        }
      }

      if (s.pll_info_.is_weak_read()) {
        ObReadStaleParam param;
        build_read_stale_param(s, param);
        ObReadStaleProcessor &read_stale_processor = get_global_read_stale_processor();
        if (OB_SUCCESS != read_stale_processor.create_or_update_vip_feedback_record(param)) {
          LOG_WDIAG("fail to record read stale feedback", K(param));
        }
      }

      s.is_rerouted_ = true;
    }
  } else if (TRANS_FREE_ROUTE_NOT_SUPPORTED_ERROR == s.current_.error_type_) {
    s.is_rerouted_ = true;
  }


  if (s.mysql_config_params_->is_mysql_routing_mode()) {
    // in mysql mode, we should forward all errors to client directly expect SERVER_SEND_HANDSHAKE
    // and SERVER_SEND_SAVED_LOGIN, if get error, will retry then disconnect
    if (SERVER_SEND_HANDSHAKE == s.current_.send_action_
        || SERVER_SEND_SAVED_LOGIN == s.current_.send_action_) {
      s.current_.state_ = INTERNAL_ERROR;
      is_user_request = false;
    }
  }

  if (!is_user_request) {
    if (REQUEST_TENANT_NOT_IN_SERVER_ERROR == s.current_.error_type_
        || LOGIN_TENANT_NOT_IN_SERVER_ERROR == s.current_.error_type_
        || REQUEST_REROUTE_ERROR == s.current_.error_type_) {
      LOG_INFO("get err pkt from observer, not forward to client, "
               "will be consumed soon, and maybe need retry",
               "send_action", get_send_action_name(s.current_.send_action_),
               "state", get_server_state_name(s.current_.state_),
               "error_type", get_server_resp_error_name(s.current_.error_type_),
               K(resp));
    } else {
      LOG_WDIAG("get err pkt from observer, not forward to client, "
               "will be consumed soon, and maybe need retry",
               "send_action", get_send_action_name(s.current_.send_action_),
               "state", get_server_state_name(s.current_.state_),
               "error_type", get_server_resp_error_name(s.current_.error_type_),
               K(resp));
    }
    consume_response_packet(s);
  } else {
    // if forward to client, recover the current.state_ and error_type_
    s.current_.state_ = pre_state;
    s.current_.error_type_ = MIN_RESP_ERROR;
    // will rewrite the packet in tunnel
  }
}

inline void ObMysqlTransact::handle_resultset_resp(ObTransState &s, bool &is_user_request)
{
  if (OB_UNLIKELY(obmysql::OB_MYSQL_COM_STMT_PREPARE == s.trans_info_.sql_cmd_
               || SERVER_SEND_PREPARE == s.current_.send_action_)) {
    handle_prepare_succ(s);
  } else if (OB_UNLIKELY(obmysql::OB_MYSQL_COM_STMT_EXECUTE == s.trans_info_.sql_cmd_)) {
    handle_execute_succ(s);
  } else if (OB_UNLIKELY(obmysql::OB_MYSQL_COM_STMT_PREPARE_EXECUTE == s.trans_info_.sql_cmd_)) {
    handle_prepare_execute_succ(s);
  }

  // consume response of non-user request
  if (OB_UNLIKELY(SERVER_SEND_XA_START == s.current_.send_action_)) {
    if (s.sm_->get_client_session()->is_proxy_enable_trans_internal_routing()) {
      record_trans_state(s, true);
    }
    LOG_DEBUG("[ObMysqlTransact::handle_resultset_resp] reset xa start flag s.is_hold_xa_start_");
    s.is_hold_xa_start_ = false;
  }

  if (OB_UNLIKELY(SERVER_SEND_INIT_SQL == s.current_.send_action_)) {
    LOG_WDIAG("init sql should not get resultset resp, just do defence");
    get_client_session_info(s).clear_init_sql();
  }

  if (OB_UNLIKELY(SERVER_SEND_REQUEST != s.current_.send_action_)) {
    //LOG_WDIAG("handle_resultset_resp, not expected");
    consume_response_packet(s);
    is_user_request = false;
  } else {
    is_user_request = true;
  }

}

inline void ObMysqlTransact::handle_ok_resp(ObTransState &s, bool &is_user_request)
{
  int ret = OB_SUCCESS;
  // in fact, we handle ok, eof, resultset and other responses in this function
  is_user_request = false;

  if (NULL != s.sm_->client_session_ && NULL != s.sm_->get_server_session()) {
    ObClientSessionInfo &client_info = get_client_session_info(s);
    ObServerSessionInfo &server_info = get_server_session_info(s);

    switch(s.current_.send_action_) {
      case SERVER_SEND_LOGIN:
        is_user_request = true;
        break;
      case SERVER_SEND_REQUEST:
        handle_user_request_succ(s, is_user_request);
        break;

      case SERVER_SEND_SAVED_LOGIN:
        handle_saved_login_succ(s);
        break;

      case SERVER_SEND_ALL_SESSION_VARS:
      case SERVER_SEND_SESSION_VARS:
      case SERVER_SEND_SESSION_USER_VARS:
        ObProxySessionInfoHandler::assign_session_vars_version(client_info, server_info);
        // send session var function will send last_insert_id in passing
        ObProxySessionInfoHandler::assign_last_insert_id_version(client_info, server_info);
        // send session var function will send safe_read_snapshot if need
        client_info.set_syncing_safe_read_snapshot(0);
        break;

      case SERVER_SEND_USE_DATABASE:
        handle_use_db_succ(s);
        break;

      case SERVER_SEND_START_TRANS:
        LOG_DEBUG("sync session info from the ok packet of the sql `begin`");
        s.is_hold_start_trans_ = false;
        break;

      case SERVER_SEND_TEXT_PS_PREPARE:
        handle_text_ps_prepare_succ(s);
        break;

      case SERVER_SEND_INIT_SQL:
        handle_send_init_sql_succ(s);
        break;

      case SERVER_SEND_XA_START:
        handle_xa_start_sync_succ(s);
        break;

      case SERVER_SEND_HANDSHAKE:
      case SERVER_SEND_NONE:
      default:
        s.current_.state_ = INTERNAL_ERROR;
        LOG_EDIAG("unknown send action", "action", s.current_.send_action_,
                  "request_sql", s.trans_info_.get_print_sql(),
                  "result", s.trans_info_.server_response_.get_analyze_result(),
                  K(client_info), K(server_info));
        break;
    }

    // due to server may retrun reroute error in internal routing trans
    // internal routing trans allow to record transaction state after begin statement
    if (s.current_.send_action_ == SERVER_SEND_START_TRANS && s.sm_->get_client_session()->is_proxy_enable_trans_internal_routing()) {
      record_trans_state(s, true);
    }
  }

  if (OB_SUCC(ret)) {
    if (obmysql::OB_MYSQL_COM_STMT_SEND_PIECE_DATA == s.trans_info_.sql_cmd_) {
      ObPieceInfo *info = NULL;
      ObClientSessionInfo &cs_info = get_client_session_info(s);
      if (OB_FAIL(cs_info.get_piece_info(info))) {
        if (OB_HASH_NOT_EXIST == ret) {
          if (OB_ISNULL(info = op_alloc(ObPieceInfo))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            PROXY_API_LOG(EDIAG, "fail to allocate memory for piece info", K(ret));
          } else {
            info->set_ps_id(cs_info.get_client_ps_id());
            info->set_addr(s.sm_->get_server_session()->get_netvc()->get_remote_addr());
            if (OB_FAIL(cs_info.add_piece_info(info))) {
              PROXY_API_LOG(WDIAG, "fail to add piece info", K(ret));
              op_free(info);
              info = NULL;
            }
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
      s.current_.state_ = INTERNAL_ERROR;
      LOG_WDIAG("add piece info failed", K(ret));
    }
  }

  if (!is_user_request) {
    consume_response_packet(s);
  } else {
    // will rewrite the packet in tunnel
  }
}

inline void ObMysqlTransact::handle_handshake_pkt(ObTransState &s)
{
  int ret = OB_SUCCESS;

  ObMysqlClientSession *client_session = s.sm_->client_session_;
  ObMysqlServerSession *server_session = s.sm_->get_server_session();
  if (OB_LIKELY(NULL != client_session)
      && OB_LIKELY(SERVER_SEND_HANDSHAKE == s.current_.send_action_)
      && OB_LIKELY(NULL != server_session)) {
    // 为探测 SQL
    if (client_session->get_session_info().get_priv_info().user_name_ == ObProxyTableInfo::DETECT_USERNAME_USER) {
      // 模拟返回 ERROR 报文情况
      LOG_DEBUG("get handshake from detect server success");
      s.mysql_errcode_ = OB_NOT_SUPPORTED;
      s.mysql_errmsg_ = "not supported, bad route request";
      if (OB_FAIL(ObMysqlTransact::encode_error_message(s))) {
        LOG_WDIAG("fail to encode err packet buf", K(ret));
      }
      ret = OB_NOT_SUPPORTED;
    } else {
      ObRespAnalyzeResult &result = s.trans_info_.server_response_.get_analyze_result();

      uint32_t conn_id = 0;
      //use connection id from OMPKHandshakeResponse Packet when the follow happened
      //1. proxy_mysql_client
      //2. client service mode
      //3. mysql routing mode
      if (client_session->is_proxy_mysql_client_
          || OB_CLIENT_SERVICE_MODE == s.mysql_config_params_->proxy_service_mode_
          || s.mysql_config_params_->is_mysql_routing_mode()) {
        conn_id = result.get_connection_id();
      } else {
        //if use server service mode, connection id is equal to cs_id
        conn_id = client_session->get_cs_id();
      }
      server_session->set_server_sessid(conn_id);
      ObMySQLCapabilityFlags capability(result.get_server_capability());
      server_session->get_session_info().save_compatible_capability_flags(capability);
      ObAddr client_addr = client_session->get_real_client_addr(const_cast<net::ObNetVConnection *>(server_session->get_netvc()));

      //we need update proxy_sessid when first connect server
      if (0 == client_session->get_proxy_sessid()) {
        client_session->set_proxy_sessid(ObMysqlClientSession::get_next_proxy_sessid());
        LOG_INFO("succ to set proxy_sessid", "cs_id", client_session->get_cs_id(),
            "proxy_sessid", client_session->get_proxy_sessid(),
            "server_ip", server_session->server_ip_,
            "ss_id", server_session->ss_id_,
            "server_sessid", conn_id,
            "is_proxy_mysql_client", client_session->is_proxy_mysql_client_,
            "ss_fd", server_session->get_netvc()->get_conn_fd(),
            K(client_addr));
      }
      LOG_DEBUG("succ to fill conn id for server session", K(conn_id),
          "ss_id", server_session->ss_id_,
          "cs_id", client_session->get_cs_id(),
          "proxy_sessid", client_session->get_proxy_sessid(),
          "is_proxy_mysql_client", client_session->is_proxy_mysql_client_,
          K(client_addr));
      s.trace_log_.log_it("[handshake]", "svr", s.server_info_.addr_, "svr_sessid", static_cast<int64_t>(server_session->get_server_sessid()));

      if (s.mysql_config_params_->enable_client_ip_checkout_) {
        if (OB_UNLIKELY(!client_addr.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("real client add is invalid, it should not happened", K(client_addr), K(ret));
        }
      } else {
        client_addr.reset();
      }
      if (OB_SUCC(ret)) {
        if (s.sm_->client_session_->get_session_info().is_oceanbase_server()) {
          if (OB_FAIL(handle_oceanbase_handshake_pkt(s, conn_id, client_addr))) {
            LOG_WDIAG("fail to handle oceanbase handshke pkt", K(conn_id), K(client_addr), K(ret));
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    s.current_.state_ = INTERNAL_ERROR;
  } else if (!s.mysql_config_params_->is_mysql_routing_mode()) {
    consume_response_packet(s);
  }
}

inline int ObMysqlTransact::handle_oceanbase_handshake_pkt(ObTransState &s, uint32_t conn_id, ObAddr &client_addr)
{
  int ret = OB_SUCCESS;
  ObMysqlClientSession *client_session = s.sm_->client_session_;

  // current proxy support use compress, and inner request also use compress
  bool use_compress = (s.sm_->get_server_protocol() == ObProxyProtocol::PROTOCOL_CHECKSUM);
  bool use_ob_protocol_v2 = (s.sm_->get_server_protocol() == ObProxyProtocol::PROTOCOL_OB20);
  bool use_ssl = s.sm_->enable_server_ssl_
                 && !s.sm_->client_session_->is_proxy_mysql_client_
                 && s.trans_info_.server_response_.get_analyze_result().support_ssl()
                 && get_global_ssl_config_table_processor().is_ssl_key_info_valid(
                    s.sm_->client_session_->get_vip_cluster_name(),
                    s.sm_->client_session_->get_vip_tenant_name());
  bool enable_client_ip_checkout = s.mysql_config_params_->enable_client_ip_checkout_;
  ObRespAnalyzeResult &result = s.trans_info_.server_response_.get_analyze_result();
  const ObString &server_scramble = result.get_scramble_string();
  const ObString &proxy_scramble = client_session->get_scramble_string();
  if (!s.mysql_config_params_->is_mysql_routing_mode()) {
    //if not mysql routing mode, we need rewrite the first login request with newest connection id
    ObString user_cluster_name;
    ObString cluster_name = client_session->get_real_cluster_name();
    int64_t cluster_id = OB_DEFAULT_CLUSTER_ID;
    // LOGIN 时，如果cluster name或者cluster id校验失败，OB 会返回 OB_CLUSTER_NO_MATCH
    // 集群别名由公有云生成，observer只记录主集群名，该场景下不需要校验
    ObConfigServerProcessor &cs_processor = get_global_config_server_processor();
    if (OB_LIKELY(s.mysql_config_params_->enable_cluster_checkout_ && !cs_processor.is_cluster_name_alias(cluster_name))) {
      user_cluster_name = cluster_name;
      if (OB_NOT_NULL(s.sm_->sm_cluster_resource_)) {
        cluster_id = s.sm_->sm_cluster_resource_->get_cluster_id();
      }
    }
    if (OB_SUCC(ret)) {
      ObHandshakeResponseParam param;
      param.use_compress_ = use_compress;
      param.use_ob_protocol_v2_ = use_ob_protocol_v2;
      param.use_ob_protocol_v2_compress_ = (s.sm_->compression_algorithm_.level_ != 0 && use_ob_protocol_v2);
      param.use_ssl_ = use_ssl;
      param.enable_client_ip_checkout_ = enable_client_ip_checkout;
      param.cs_id_version_ = client_session->get_cs_id_version();
      //if global_vars_version was inited, this must saved login
      if (client_session->get_session_info().is_first_login_succ()) {
        // if client vc create new connection, will change current conn id to new conn id
        // from the newest handshake packet, mainly to avoid Session Already Exist

        // saved login packet, which will be sent to observer later
        // (include modified packet data and analyzed result, without -D database)
        if (OB_FAIL(ObProxySessionInfoHandler::rewrite_saved_login_req(
                    param,
                    client_session->get_session_info(),
                    user_cluster_name,
                    cluster_id,
                    conn_id,
                    client_session->get_proxy_sessid(),
                    server_scramble,
                    proxy_scramble,
                    client_addr,
                    client_session->get_cs_id(),
                    client_session->get_connected_time()))) {
          LOG_WDIAG("fail to handle rewrite saved hrs pkt, INTERNAL_ERROR", K(ret));
        } else {
          LOG_DEBUG("succ to rewrite saved hrs pkt");
        }
      } else {
        // first login packet, which will be sent to observer at first auth
        // (include modified packet data and analyzed result, with -D database)
        if (OB_FAIL(ObProxySessionInfoHandler::rewrite_first_login_req(
                    param,
                    client_session->get_session_info(),
                    user_cluster_name,
                    cluster_id,
                    conn_id,
                    client_session->get_proxy_sessid(),
                    server_scramble,
                    proxy_scramble,
                    client_addr,
                    client_session->get_cs_id(),
                    client_session->get_connected_time()))) {
          LOG_WDIAG("fail to handle rewrite first hrs pkt, INTERNAL_ERROR", K(ret));
        } else {
          LOG_DEBUG("succ to rewrite first hrs pkt");
        }
      }
    } else {
      //use orig login req, no need to rewrite it
    }
  }

  return ret;
}

void ObMysqlTransact::handle_db_reset(ObTransState &s)
{
  LOG_DEBUG("[ObMysqlTransact::handle_db_reset] database in observer was reset "
            "to empty, disconnect all server sessions in session pool");

  int ret = OB_SUCCESS;
  ObMysqlClientSession *client_session = s.sm_->get_client_session();

  if (NULL != client_session && NULL != s.sm_->get_server_session()) {
    ObClientSessionInfo &client_info = client_session->get_session_info();
    ObServerSessionInfo &server_info = get_server_session_info(s);

    // 1. disconnect all server session in session pool
    if (client_session->is_session_pool_client()) {
      get_global_session_manager().purge_session_manager_keepalives(
      client_session->schema_key_.dbkey_.config_string_);
    }

    // although Sharing Mode include MySQL Server, but can not distinguish. so, purge direct
    client_session->get_session_manager_new().purge_keepalives();
    client_session->get_session_manager().purge_keepalives();
    if (NULL != client_session->get_server_session()) {
      ret = OB_INNER_STAT_ERROR;
      LOG_EDIAG("[ObMysqlTransact::handle_db_reset]",
                "bound_server_session", client_session->get_server_session());
    } else if ((client_session->get_cur_server_session() != client_session->get_lii_server_session())
               && NULL != client_session->get_lii_server_session()) {
      ret = OB_INNER_STAT_ERROR;
      LOG_EDIAG("[ObMysqlTransact::handle_db_reset]",
                "bound_server_session", client_session->get_server_session(),
                "lii_server_session", client_session->get_lii_server_session());
    } else {
      // 2. reset db name version
      client_info.set_db_name_version(0);
      server_info.set_db_name_version(0);
      server_info.remove_database_name();

      // 3. remove data_base_name
      // in some special cases, like CHANGE EFFECTIVE tenant,
      // there may be no database name in obproxy here, which will return OB_ENTRY_NOT_EXIST
      ret = client_info.remove_database_name();
      if (OB_SUCCESS != ret && OB_ENTRY_NOT_EXIST != ret) {
        LOG_EDIAG("[ObMysqlTransact::handle_db_reset] failed to remove database", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }

    if (OB_FAIL(ret)) {
      s.current_.state_ = INTERNAL_ERROR;
    }
  }
}

inline void ObMysqlTransact::handle_first_response_packet(ObTransState &s)
{
  ObRespAnalyzeResult &resp = s.trans_info_.server_response_.get_analyze_result();
  bool is_user_request = false;
  // handle resp
  if (resp.is_error_resp()) {
    MYSQL_INCREMENT_TRANS_STAT(SERVER_ERROR_RESPONSES);
    handle_error_resp(s, is_user_request);
  } else if (resp.is_ok_resp()) {
    MYSQL_INCREMENT_TRANS_STAT(SERVER_OK_RESPONSES);
    handle_ok_resp(s, is_user_request);
  } else if (resp.is_resultset_resp()) {
    MYSQL_INCREMENT_TRANS_STAT(SERVER_RESULTSET_RESPONSES);
    handle_resultset_resp(s, is_user_request);
  } else if (resp.is_handshake_pkt()) {
    MYSQL_INCREMENT_TRANS_STAT(SERVER_OTHER_RESPONSES);
    handle_handshake_pkt(s);
  } else {
    MYSQL_INCREMENT_TRANS_STAT(SERVER_OTHER_RESPONSES);
    handle_ok_resp(s, is_user_request);
  }

  if (s.current_.send_action_ != SERVER_SEND_SAVED_LOGIN
      && s.current_.send_action_ != SERVER_SEND_HANDSHAKE
      && !is_user_request) {
    bool is_only_sync_trans_sess = resp.is_error_resp();
    if (OB_SUCCESS != ObProxySessionInfoHandler::save_changed_sess_info(s.sm_->get_client_session()->get_session_info(),
                                                                        s.sm_->get_server_session()->get_session_info(),
                                                                        resp.get_extra_info(),
                                                                        s.trace_log_,
                                                                        is_only_sync_trans_sess)) {
      s.current_.state_ = INTERNAL_ERROR;
      LOG_WDIAG("fail to save changed session info", K(get_send_action_name(s.current_.send_action_)));
    }
  }

}

inline ObServerSessionInfo &ObMysqlTransact::get_server_session_info(ObTransState &s)
{
  return s.sm_->get_server_session()->get_session_info();
}

inline ObClientSessionInfo &ObMysqlTransact::get_client_session_info(ObTransState &s)
{
  return s.sm_->get_client_session()->get_session_info();
}

inline void ObMysqlTransact::consume_response_packet(ObTransState &s)
{
  int ret = OB_SUCCESS;
  ObIOBufferReader *reader = s.sm_->get_server_buffer_reader();
  LOG_DEBUG("[ObMysqlTransact::consume_response_packet]"
            " no need forward to client, and will consume soon",
            "send_action", get_send_action_name(s.current_.send_action_),
            "len", reader->read_avail());

  if (OB_FAIL(reader->consume_all())) {
    s.current_.state_ = INTERNAL_ERROR;
  }
}

int ObMysqlTransact::build_no_privilege_message(ObTransState &trans_state,
                                                ObMysqlClientSession &client_session,
                                                const ObString &database)
{
  int ret = OB_SUCCESS;
  ObClientSessionInfo &session_info = client_session.get_session_info();
  // access denied for  database from remote addr
  trans_state.mysql_errcode_ = OB_ERR_NO_DB_PRIVILEGE;
  char err_msg[OB_MAX_ERROR_MSG_LEN];
  const ObString &user = session_info.get_origin_username();
  char client_ip[OB_MAX_SERVER_ADDR_SIZE];
  client_ip[0] = '\0';
  client_session.get_real_client_addr().ip_to_string(client_ip, OB_MAX_SERVER_ADDR_SIZE);
  if (!database.empty()) {
    snprintf(err_msg, OB_MAX_ERROR_MSG_LEN, "Access denied for user '%.*s'@'%s' to database '%.*s'",
             static_cast<int32_t>(user.length()), user.ptr(), client_ip,
             static_cast<int32_t>(database.length()), database.ptr());
  } else {
    snprintf(err_msg, OB_MAX_ERROR_MSG_LEN, "Access denied for user '%.*s'@'%s'",
             static_cast<int32_t>(user.length()), user.ptr(), client_ip);
  }
  trans_state.mysql_errmsg_ = err_msg;
  if (OB_FAIL(ObMysqlTransact::encode_error_message(trans_state))) {
    LOG_WDIAG("fail to build err packet", K(ret));
  }
  return ret;
}

inline bool ObMysqlTransact::need_refresh_trace_stats(ObTransState &s)
{
  bool bret = false;
  //we need refresh trace stats when all the follows happen
  //1. enable_trace_stats_ is true
  //2. looked up pl before found target addr
  //3. this is first_request for trans
  //4. this is not proxy_mysql_client
  if (OB_LIKELY(NULL != s.mysql_config_params_)
      && OB_UNLIKELY(s.mysql_config_params_->enable_trace_stats_)
      && s.is_trans_first_request_
      && 0 != s.pll_info_.pl_attempts_
      && OB_LIKELY(NULL != s.sm_)
      && OB_LIKELY(NULL != s.sm_->get_client_session())
      && !s.sm_->get_client_session()->is_proxy_mysql_client_) {
    bret = true;
  }
  return bret;
}

inline void ObMysqlTransact::update_trace_stat(ObTransState &s)
{
  int ret = OB_SUCCESS;
  if (need_refresh_trace_stats(s)) {
    ObMysqlClientSession &cs = *s.sm_->get_client_session();
    ObTraceStats *&trace_stats = cs.get_trace_stats();
    if (NULL == trace_stats) {
      char *buf = NULL;
      if (OB_ISNULL(buf = static_cast<char *>(op_fixed_mem_alloc(sizeof(ObTraceStats))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("fail to alloc mem for ObTraceStats", "alloc_size", sizeof(ObTraceStats), K(ret));
      } else if (OB_ISNULL(trace_stats = new (buf) ObTraceStats())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("failed to placement new for ObTraceStats", K(ret));
        op_fixed_mem_free(buf, sizeof(ObTraceStats));
        buf = NULL;
      }
    }

    if (OB_LIKELY(NULL != trace_stats)) {
      ObTraceRecord *trace_record = NULL;
      if (OB_FAIL(trace_stats->get_current_record(trace_record))) {
        PROXY_TXN_LOG(WDIAG, "fail to get current record", KPC(trace_stats),
                      "cs_id", cs.get_cs_id(), "proxy_sessid", cs.get_proxy_sessid(), K(ret));
      } else {
        trace_record->attempts_ = static_cast<int8_t>(s.current_.attempts_);
        trace_record->pl_attempts_ = static_cast<int8_t>(s.pll_info_.pl_attempts_);
        trace_record->server_state_ = static_cast<int8_t>(s.current_.state_);
        trace_record->send_action_ = static_cast<int8_t>(s.current_.send_action_);
        trace_record->resp_error_ = static_cast<int8_t>(s.current_.error_type_);
        trace_record->addr_.set_sockaddr(s.server_info_.addr_.sa_);
        if (0 == trace_stats->last_trace_end_time_) {
          trace_record->cost_time_us_ = static_cast<int32_t>(hrtime_to_usec(
                milestone_diff(s.sm_->milestones_.pl_lookup_begin_, get_based_hrtime(s))));
        } else {
          trace_record->cost_time_us_ = static_cast<int32_t>(hrtime_to_usec(
                milestone_diff(trace_stats->last_trace_end_time_, get_based_hrtime(s))));
        }
        trace_stats->last_trace_end_time_ = get_based_hrtime(s);

        PROXY_TXN_LOG(DEBUG, "succ to update trace stat", KPC(trace_record));
      }
    }
  }
}

void ObMysqlTransact::handle_oceanbase_server_resp_error(ObTransState &s, ObMySQLCmd request_cmd, ObMySQLCmd current_cmd)
{
  int ret = OB_SUCCESS;

  switch (s.current_.error_type_) {
    case REQUEST_READ_ONLY_ERROR:
    case REQUEST_TENANT_NOT_IN_SERVER_ERROR:
    case REQUEST_CONNECT_ERROR:
    case REQUEST_SERVER_INIT_ERROR:
    case REQUEST_REROUTE_ERROR:
    case REQUEST_SERVER_STOPPING_ERROR:
    case TRANS_FREE_ROUTE_NOT_SUPPORTED_ERROR: {
      // before retry send request, write the req packet back to client buffer
      ObIOBufferReader *client_reader = s.sm_->get_client_buffer_reader();
      if (OB_ISNULL(client_reader)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("[ObMysqlTransact::handle_server_resp_error] client reader is NULL");
      } else {
        // rewrite request to client buffer in order to retry
        ObString req_pkt = s.trans_info_.client_request_.get_req_pkt();
        int64_t pkt_len = req_pkt.length();
        int64_t written_len = 0;
        if (0 != client_reader->read_avail() || req_pkt.empty()) {
          ret = OB_INNER_STAT_ERROR;
          LOG_EDIAG("[ObMysqlTransact::handle_server_resp_error] invalid internal state",
                    "read_avail", client_reader->read_avail(), K(req_pkt));
        } else if (OB_FAIL(client_reader->mbuf_->write(req_pkt.ptr(), pkt_len, written_len))) {
          LOG_WDIAG("[ObMysqlTransact::handle_server_resp_error] "
                   "fail to write login packet into client iobuffer",
                   "request_length", pkt_len, K(written_len), K(ret));
        } else if (OB_UNLIKELY(pkt_len != written_len)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("request packet length must be equal with written len",
                   "request_length", pkt_len,  K(written_len), K(ret));
        } else {
          if (REQUEST_TENANT_NOT_IN_SERVER_ERROR == s.current_.error_type_
              || REQUEST_CONNECT_ERROR == s.current_.error_type_) {
            // if pl has renewed, don't renew twice
            s.pll_info_.set_force_renew();
            s.pll_info_.set_need_force_flush(s.pll_info_.is_remote_readonly());
          }
          if (REQUEST_REROUTE_ERROR == s.current_.error_type_) {
            s.pll_info_.pl_update_for_reroute(s);
          }
          handle_retry_server_connection(s);
        }
      }
      break;
    }

    case LOGIN_CLUSTER_NOT_MATCH_ERROR:
    case LOGIN_CONNECT_ERROR:
    case LOGIN_TENANT_NOT_IN_SERVER_ERROR: {
      s.pll_info_.set_force_renew();
      s.pll_info_.set_need_force_flush(s.pll_info_.is_remote_readonly());
      handle_retry_server_connection(s);
      break;
    }

    case HANDSHAKE_COMMON_ERROR:
    case LOGIN_SERVER_INIT_ERROR:
    case LOGIN_SERVER_STOPPING_ERROR:
    case LOGIN_SESSION_ENTRY_EXIST_ERROR:
    case SAVED_LOGIN_COMMON_ERROR:
    case RESET_SESSION_VARS_COMMON_ERROR:
    case SYNC_DATABASE_COMMON_ERROR: {
      //current server session is still ok
      s.sm_->clear_server_entry();
      handle_retry_server_connection(s);
      break;
    }
    case START_TRANS_COMMON_ERROR:
    case START_XA_START_ERROR:
    case SYNC_PREPARE_COMMON_ERROR:
    case SYNC_TEXT_PS_PREPARE_COMMON_ERROR:
    case SEND_INIT_SQL_ERROR:
      // no need retry, just disconnect connection for sync_prepare_error, start_trans_error, send_init_sql
      COLLECT_INTERNAL_DIAGNOSIS(s.sm_->connection_diagnosis_trace_,
                                 obutils::OB_PROXY_INTERNAL_TRACE,
                                 OB_PROXY_INTERNAL_REQUEST_FAIL,
                                 "proxy execute internal request failed, received error resp, error_type: %s", get_server_resp_error_name(s.current_.error_type_));
      handle_server_connection_break(s);
      break;
    case ORA_FATAL_ERROR:
      LOG_EDIAG("ob ora fatal error",
                "sql", s.trans_info_.client_request_.get_print_sql(),
                "origin_sql_cmd", get_mysql_cmd_str(request_cmd),
                "current_sql_cmd", get_mysql_cmd_str(current_cmd));
      if (obmysql::OB_MYSQL_COM_STMT_EXECUTE == request_cmd) {
        ObClientSessionInfo &cs_info = s.sm_->get_client_session()->get_session_info();
        ObServerSessionInfo &ss_info = s.sm_->get_server_session()->get_session_info();
        ObPsEntry *ps_entry = cs_info.get_ps_entry();
        uint32_t client_ps_id = cs_info.get_client_ps_id();
        uint32_t server_ps_id = ss_info.get_server_ps_id();
        ObPsIdPair *ps_id_pair = ss_info.get_ps_id_pair(client_ps_id);
        LOG_EDIAG("ora fatal error", K(client_ps_id), K(server_ps_id), KPC(ps_id_pair), KPC(ps_entry));
      }
      COLLECT_INTERNAL_DIAGNOSIS(s.sm_->connection_diagnosis_trace_,
                                 obutils::OB_SERVER_INTERNAL_TRACE,
                                 OB_PROXY_INTERNAL_ERROR, "ora fatal error");
      handle_server_connection_break(s);
      break;
    case STANDBY_WEAK_READONLY_ERROR:
      COLLECT_INTERNAL_DIAGNOSIS(
          s.sm_->connection_diagnosis_trace_, obutils::OB_SERVER_INTERNAL_TRACE,
          OB_PROXY_INTERNAL_ERROR,
          "primary cluster switchover to standby, disconnect");
      LOG_WDIAG("primary cluster has switchover to standby, client session will be destroyed");
      handle_server_connection_break(s);
      break;
    case CLIENT_SESSION_KILLED_ERROR:
      handle_server_connection_break(s);
      LOG_INFO("client session was killed by user", "cs_id", s.sm_->client_session_->get_cs_id());
      break;
    default: {
      ret = OB_INNER_STAT_ERROR;
      LOG_EDIAG("unexpect error!, unknown error type", K(s.current_.error_type_));
      break;
    }
  }

  if (OB_FAIL(ret)) {
    COLLECT_INTERNAL_DIAGNOSIS(
    s.sm_->connection_diagnosis_trace_, obutils::OB_PROXY_INTERNAL_TRACE,
    OB_PROXY_INTERNAL_ERROR, "fail to handle error resp from server, error type: %s",
    get_server_resp_error_name(s.current_.error_type_));
    handle_server_connection_break(s);
  }
}

void ObMysqlTransact::handle_server_resp_error(ObTransState &s)
{
  // if resp error, set sql_cmd_ to original request_cmd
  ObMySQLCmd request_cmd = s.trans_info_.client_request_.get_packet_meta().cmd_;
  ObMySQLCmd current_cmd = s.trans_info_.sql_cmd_;
  s.trans_info_.sql_cmd_ = request_cmd;
  LOG_DEBUG("[ObMysqlTransact::handle_server_resp_error] begin to handle server error, "
            "and will retry", "error_type", get_server_resp_error_name(s.current_.error_type_),
            K(current_cmd), K(request_cmd));

  if (s.sm_->client_session_->get_session_info().is_oceanbase_server()) {
    handle_oceanbase_server_resp_error(s, request_cmd, current_cmd);
  } else {
    handle_server_connection_break(s);
  }
}

// response from observer. one of two things can happen now. if the
// response is bad, then we can give up. the latter case is handled by
// handle_server_connection_break and close the client session
// and server session. if the response is good
// handle_forward_server_connection_open is called.
inline void ObMysqlTransact::handle_response_from_server(ObTransState &s)
{
  LOG_DEBUG("[ObMysqlTransact::handle_response_from_server]",
            "cur_state", get_server_state_name(s.current_.state_),
            "error_type", get_server_resp_error_name(s.current_.error_type_),
            "server_ip", s.server_info_.addr_);
  if (OB_LIKELY(CONNECTION_ALIVE == s.current_.state_)) {
    handle_first_response_packet(s);
  }

  s.server_info_.state_ = s.current_.state_;

  if (OB_UNLIKELY(s.mysql_config_params_->enable_trans_detail_stats_)) {
    update_sync_session_stat(s);
  }

  handle_server_failed(s);

  // after handle response, before cmd complete, record current server session addr and sess id for next sql
  if (OB_NOT_NULL(s.sm_->get_client_session())
      && !s.sm_->client_session_->is_proxy_mysql_client_
      && OB_NOT_NULL(s.sm_->get_server_session())) {
    ObClientSessionInfo &cs_info = s.sm_->client_session_->get_session_info();
    if (cs_info.get_last_server_addr() != s.sm_->get_server_session()->server_ip_
        && cs_info.get_last_server_sess_id() != s.sm_->get_server_session()->get_server_sessid()) {
      cs_info.set_last_server_addr(s.sm_->get_server_session()->server_ip_);
      cs_info.set_last_server_sess_id(s.sm_->get_server_session()->get_server_sessid());
      LOG_DEBUG("record server session info as last", "last_addr", cs_info.get_last_server_addr(),
                "sess_id", cs_info.get_last_server_sess_id());
    }
  }

  if (OB_UNLIKELY(get_global_performance_params().enable_stat_)) {
    update_trace_stat(s);
  }
  switch (s.current_.state_) {
    case CONNECTION_ALIVE:
      LOG_DEBUG("[ObMysqlTransact::handle_response_from_server] connection alive");
      handle_on_forward_server_response(s);
      break;

    case RESPONSE_ERROR: {
      handle_server_resp_error(s);
      break;
    }
    case ALIVE_CONGESTED:
      // fall through
    case DEAD_CONGESTED:
      // fall through
    case DETECT_CONGESTED:
      // fall through
    case CONNECT_ERROR: {
      handle_retry_server_connection(s);
      break;
    }
    case ACTIVE_TIMEOUT: {
      LOG_INFO("[ObMysqlTransact::handle_response_from_server] connection not alive");
      handle_server_connection_break(s);
      break;
    }
    case CONNECTION_ERROR:
      // fall through
    case CONNECTION_CLOSED:
      // fall through
    case STATE_UNDEFINED:
      // fall through
    case INACTIVE_TIMEOUT:
      // fall through
    case ANALYZE_ERROR:
      // fall through
    case INTERNAL_ERROR: {
      if (s.current_.state_ == CONNECTION_ERROR || s.current_.state_ == CONNECTION_CLOSED) {
        // server close connection
        s.internal_error_op_for_diagnosis_ = ObMysqlTransact::PROXY_INTERNAL_ERROR_TRANSFER_DISCONNECT;
      }
      ObMysqlServerSession *ss = s.sm_->get_server_session();
      ObMySQLCmd request_cmd = s.trans_info_.client_request_.get_packet_meta().cmd_;
      if (NULL != ss) {
        if ((INACTIVE_TIMEOUT == s.current_.state_)
            && (obmysql::OB_MYSQL_COM_QUIT == request_cmd)) {
          LOG_INFO("INACTIVE_TIMEOUT caused by OB_MYSQL_COM_QUIT, which is a normal condition",
                   "server_state", ObMysqlTransact::get_server_state_name(s.current_.state_),
                   "addr", ObIpEndpoint(ss->get_netvc()->get_remote_addr()),
                   "request_cmd", get_mysql_cmd_str(s.trans_info_.client_request_.get_packet_meta().cmd_),
                   "sql_cmd", get_mysql_cmd_str(s.trans_info_.sql_cmd_),
                   "sql", s.trans_info_.get_print_sql());
        } else if (s.need_retry_) {
          LOG_WDIAG("connection error",
                   "server_state", ObMysqlTransact::get_server_state_name(s.current_.state_),
                   "request_cmd", get_mysql_cmd_str(s.trans_info_.client_request_.get_packet_meta().cmd_),
                   "sql_cmd", get_mysql_cmd_str(s.trans_info_.sql_cmd_),
                   "sql", s.trans_info_.get_print_sql());
        }
      }
      COLLECT_INTERNAL_DIAGNOSIS(s.sm_->connection_diagnosis_trace_,
                        obutils::OB_PROXY_INTERNAL_TRACE,
                        OB_PROXY_INTERNAL_ERROR,
                        "unexpected proxy internal state: %s", get_server_state_name(s.current_.state_));
      handle_server_connection_break(s);
      break;
    }
      // not happen
    case CMD_COMPLETE:
    case TRANSACTION_COMPLETE:
    default:
      handle_server_connection_break(s);
      LOG_EDIAG("s.current_.state_ is set to something unsupported", K(s.current_.state_));
      break;
  }
}

void ObMysqlTransact::handle_oceanbase_retry_server_connection(ObTransState &s)
{
  int ret = OB_SUCCESS;
  bool second_in = false;
  ObPieceInfo *info = NULL;
  ObSSRetryStatus retry_status = NO_NEED_RETRY;
  ObRetryType retry_type = ObRetryType::INVALID;
  const int64_t max_connect_attempts = get_max_connect_attempts(s);
  bool is_reroute_to_coordinator = false;

  ObIpEndpoint old_target_server;
  net::ops_ip_copy(old_target_server, s.server_info_.addr_.sa_);

  int64_t obproxy_route_addr = 0;
  net::ObIpEndpoint coordinator = s.sm_->get_client_session()->get_trans_coordinator_ss_addr();
  if (NULL != s.sm_ && NULL != s.sm_->client_session_) {
    obproxy_route_addr = s.sm_->client_session_->get_session_info().get_obproxy_route_addr();
  }

  if (OB_FAIL(s.sm_->get_client_session()->get_session_info().get_piece_info(info))) {
    // reset to succ
    ret = OB_SUCCESS;
  } else {
    second_in = true;
  }
  // in mysql mode, no need retry
  if (OB_UNLIKELY(s.mysql_config_params_->is_mysql_routing_mode())) {
    LOG_DEBUG("in mysql mode, no need retry, will disconnect");
    retry_status = NO_NEED_RETRY;
    retry_type = NOT_RETRY;
  } else if (OB_UNLIKELY(is_in_trans(s)
                         && s.sm_->get_client_session()->is_proxy_enable_trans_internal_routing()
                         && (!coordinator.is_valid()
                         || need_use_tunnel(s)))) {
    // check coordinator addr in transaction
    if (!coordinator.is_valid()) {
      ret = OB_PROXY_INVALID_COORDINATOR;
    } else if (need_use_tunnel(s)) {
      ret = OB_ERR_UNEXPECTED;
    }
    s.inner_errcode_ = ret;
    LOG_WDIAG("coordinator addr is invalid in transaction, no need retry", K(ret));
    retry_status = NO_NEED_RETRY;
    retry_type = NOT_RETRY;


  // we will update retry_status only the follow all happened
  // 1. not in transaction / in internal routing tranaction
  // 2. attempts_ is less then max_connect_attempts
  // 3. is not kill query
  // 4. is not piece info
  } else if (s.need_retry_
             && s.current_.attempts_ < max_connect_attempts
             && 0 == obproxy_route_addr
             && !s.trans_info_.client_request_.is_kill_query()
             && obmysql::OB_MYSQL_COM_STMT_FETCH != s.trans_info_.sql_cmd_
             && obmysql::OB_MYSQL_COM_STMT_GET_PIECE_DATA != s.trans_info_.sql_cmd_
             && !second_in) {
    ++s.current_.attempts_;
    LOG_DEBUG("start next retry", "is in trans:", is_in_trans(s));
    // if use target db server then no need renew, will retry server from target db server
    if ((OB_NOT_NULL(s.trans_info_.client_request_.get_parse_result().get_target_db_server())
          && !s.trans_info_.client_request_.get_parse_result().get_target_db_server()->is_empty())
        || (OB_NOT_NULL(s.sm_->target_db_server_) && !s.sm_->target_db_server_->is_empty())) {
      s.pll_info_.force_renew_state_ = ObPartitionLookupInfo::ObForceRenewState::NO_NEED_RENEW;
      s.pll_info_.is_need_force_flush_ = false;
    }
    if (s.current_.error_type_ == TRANS_FREE_ROUTE_NOT_SUPPORTED_ERROR) {
      LOG_INFO("transaction internal routing retry", K(s.trans_info_.sql_cmd_));
      retry_status = FOUND_EXISTING_ADDR;
      s.server_info_.set_addr(coordinator);
      is_reroute_to_coordinator = true;
      retry_type = TRANS_INTERNAL_ROUTING;
    } else if (is_in_trans(s)) {
      if (s.sm_->get_client_session()->is_trans_internal_routing()) {
        if (REQUEST_REROUTE_ERROR == s.current_.error_type_) {
          retry_status = FOUND_EXISTING_ADDR;
          s.server_info_.set_addr(ops_ip_sa_cast(s.reroute_info_.replica_.server_.get_sockaddr()));
          retry_type = REROUTE;
        } else if (NOT_FOUND_EXISTING_ADDR ==  (retry_status = retry_server_connection_not_open(s, (int32_t &) retry_type))) {
          // proxy will clear server session while NOT_FOUND_EXISTING_ADDR
          // to avoid coordinator closed in transaction, forced to use coordinator while no available replica in internal routing transaction
          retry_status = FOUND_EXISTING_ADDR;
          s.server_info_.set_addr(coordinator);
          is_reroute_to_coordinator = true;
          LOG_DEBUG("not available replica in transaction internal routing, reroute to coordinator",
                    "coordinator", coordinator);
          retry_type = TRANS_INTERNAL_ROUTING;
        }
      } else {
        // if in normal transaction , close connection
        retry_status = NO_NEED_RETRY;
        retry_type = NOT_RETRY;
      }
    } else {
      if (s.pll_info_.is_force_renew()) { // force pl lookup
        retry_status = NOT_FOUND_EXISTING_ADDR;
        retry_type = NOT_RETRY;
      } else if (REQUEST_REROUTE_ERROR == s.current_.error_type_) {
        retry_status = FOUND_EXISTING_ADDR;
        s.server_info_.set_addr(ops_ip_sa_cast(s.reroute_info_.replica_.server_.get_sockaddr()));
        retry_type = REROUTE;
      } else {
        retry_status = retry_server_connection_not_open(s, (int32_t &) retry_type);
      }
    }
  }

  if (NULL == s.congestion_entry_ || !s.congestion_entry_->is_congested()) {
    LOG_INFO("pre try failed, will retry",
             "retry_status", get_retry_status_string(retry_status),
             "attempts now", s.current_.attempts_,
             K(max_connect_attempts),
             "old_target_server", old_target_server,
             "retry server", s.server_info_.addr_,
             "is_first_request", s.is_trans_first_request_,
             "is_auth_request", s.is_auth_request_,
             "is_force_renew", s.pll_info_.is_force_renew(),
             "force_retry_congested", s.force_retry_congested_,
             "send_action", get_send_action_name(s.current_.send_action_),
             "state", get_server_state_name(s.current_.state_),
             "route", s.pll_info_.route_,
             K(is_reroute_to_coordinator),
             "is_in_trans", is_in_trans(s),
             "is_internal_routing", s.sm_->get_client_session()->is_trans_internal_routing(),
             KPC_(s.congestion_entry));
  } else {
    LOG_DEBUG("pre try failed, will retry",
              "retry_status", get_retry_status_string(retry_status),
              "attempts now", s.current_.attempts_,
              K(max_connect_attempts),
              "old_target_server", old_target_server,
              "retry server", s.server_info_.addr_,
              "is_first_request", s.is_trans_first_request_,
              "is_auth_request", s.is_auth_request_,
              "is_force_renew", s.pll_info_.is_force_renew(),
              "force_retry_congested", s.force_retry_congested_,
              "send_action", get_send_action_name(s.current_.send_action_),
              "state", get_server_state_name(s.current_.state_),
              "route", s.pll_info_.route_,
              K(is_reroute_to_coordinator),
              "is_in_trans", is_in_trans(s),
              "is_internal_routing", s.sm_->get_client_session()->is_trans_internal_routing(),
              KPC_(s.congestion_entry));
  }
  ROUTE_DIAGNOSIS(s.sm_->route_diagnosis_,
                  RETRY,
                  retry_connection,
                  ret,
                  s.current_.attempts_,
                  retry_status,
                  retry_type,
                  s.server_info_.addr_);

  if (FOUND_EXISTING_ADDR == retry_status) {
    // before retry another server, reset analyze result
    s.trans_info_.server_response_.reset();

    s.sm_->trans_stats_.server_retries_ += 1;
    // reset error_type
    s.current_.error_type_ = MIN_RESP_ERROR;
    if (NULL != s.congestion_entry_) {
      s.congestion_entry_->dec_ref();
      s.congestion_entry_ = NULL;
    }
    if (is_reroute_to_coordinator) {
      s.congestion_lookup_success_ = true;
      s.need_congestion_lookup_ = false;
      s.pl_lookup_state_ = USE_COORDINATOR_SESSION;
    } else {
      s.congestion_lookup_success_ = false;
      s.need_congestion_lookup_ = true;
    }

    LOG_DEBUG("FOUND_EXISTING_ADDR, Retrying...",
              "attempts now", s.current_.attempts_,
              K(max_connect_attempts),
              "retry observer", s.server_info_.addr_,
              "force_retry_congested", s.force_retry_congested_,
              "is reroute to coordinaotr", is_reroute_to_coordinator);
    TRANSACT_RETURN(SM_ACTION_CONGESTION_CONTROL_LOOKUP, handle_congestion_control_lookup);
  } else if (NOT_FOUND_EXISTING_ADDR == retry_status) {
    // before retry another server, reset analyze result
    s.trans_info_.server_response_.reset();
    // before lookup pl, clear current server entry(close current server session)
    s.sm_->clear_server_entry();

    s.pll_info_.reset_pl();
    s.server_info_.reset();
    s.sm_->trans_stats_.server_retries_ += 1;
    s.sm_->trans_stats_.pl_lookup_retries_ += 1;
    // reset error_type
    s.current_.error_type_ = MIN_RESP_ERROR;
    if (NULL != s.congestion_entry_) {
      s.congestion_entry_->dec_ref();
      s.congestion_entry_ = NULL;
    }
    s.congestion_lookup_success_ = false;
    s.need_congestion_lookup_ = true;
    s.congestion_entry_not_exist_count_ = 0; // every time lookup pl, set it to zero

    LOG_DEBUG("NOT_FOUND_EXISTING_ADDR, Retrying..., "
              "retry to choose exist observer addr failed, and will retry partition lookup",
              "attempts now", s.current_.attempts_,
              K(max_connect_attempts),
              "force_retry_congested", s.force_retry_congested_);
    TRANSACT_RETURN(SM_ACTION_PARTITION_LOCATION_LOOKUP, handle_pl_lookup);
  } else if (NO_NEED_RETRY == retry_status) {
    if (OB_NOT_NULL(s.sm_->route_diagnosis_) && s.sm_->route_diagnosis_->is_support_explain_route()) {
      LOG_DEBUG("NO_NEED_RETRY, No more retries, but respond to explain route.");
      handle_explain_route(s);
    } else if (ObRetryType::CMNT_TARGET_DB_SERVER == retry_type ||
               ObRetryType::CONF_TARGET_DB_SERVER == retry_type) {
      LOG_DEBUG("NO_NEED_RETRY, no more target_db_server to choose", K(retry_type));
      handle_target_db_not_allow(s);
    } else {
      LOG_DEBUG("NO_NEED_RETRY, No more retries, will disconnect soon.");
      if (s.current_.state_ == RESPONSE_ERROR) {
        ObRespAnalyzeResult &result = s.trans_info_.server_response_.get_analyze_result();
        COLLECT_INTERNAL_DIAGNOSIS(
            s.sm_->connection_diagnosis_trace_,
            obutils::OB_SERVER_INTERNAL_TRACE, OB_PROXY_NO_NEED_RETRY,
            "proxy fail to retry internal request, attempts: %d, error_type: "
            "%s, error_code: %d, error_msg:%.*s",
            s.current_.attempts_,
            get_server_resp_error_name(s.current_.error_type_),
            result.get_error_pkt().get_err_code(),
            result.get_error_pkt().get_message().length(),
            result.get_error_pkt().get_message().ptr());

      } else {
        COLLECT_INTERNAL_DIAGNOSIS(
            s.sm_->connection_diagnosis_trace_,
            obutils::OB_SERVER_INTERNAL_TRACE, OB_PROXY_NO_NEED_RETRY,
            "proxy fail to retry to connect server, attempts: %d, current_state: %s",
            s.current_.attempts_, get_server_state_name(s.current_.state_));
      }
      s.internal_error_op_for_diagnosis_ = PROXY_INTERNAL_ERROR_TRANSFER_DISCONNECT;
      handle_server_connection_break(s);
    }
  } else {
    LOG_EDIAG("error, never run here");
    handle_server_connection_break(s);
  }
}

void ObMysqlTransact::handle_retry_server_connection(ObTransState &s)
{
  /*
   * reset response transform hook to keep the plugin created as the correct order
   * otherwise, the packet handled by plugin will be err, eg, reroute err packet
   */
  s.sm_->api_.txn_destroy_hook(OB_MYSQL_RESPONSE_TRANSFORM_HOOK);

  // binlog 请求也不支持重试
  if (s.sm_->client_session_->get_session_info().is_oceanbase_server()
      && !is_binlog_request(s)) {
    handle_oceanbase_retry_server_connection(s);
  } else {
    COLLECT_INTERNAL_DIAGNOSIS(
        s.sm_->connection_diagnosis_trace_, obutils::OB_PROXY_INTERNAL_TRACE,
        OB_PROXY_NO_NEED_RETRY, "binlog request is unable to retry");
    handle_server_connection_break(s);
  }
}

int ObMysqlTransact::attach_cached_dummy_entry(ObTransState &s, const ObAttachDummyEntryType type)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(NULL != s.sm_)
      && OB_LIKELY(NULL != s.sm_->client_session_)
      && OB_LIKELY(NULL != s.sm_->client_session_->dummy_entry_)
      && OB_LIKELY(s.sm_->client_session_->dummy_entry_->is_valid())) {
    const uint32_t sm_id = s.sm_->sm_id_;
    ObMysqlClientSession *client_session = s.sm_->client_session_;

    switch (type) {
      case NO_TABLE_ENTRY_FOUND_ATTACH_TYPE: {
        acquire_cached_server_session(s);
        if (s.server_info_.addr_.is_valid()) {
          ROUTE_DIAGNOSIS(s.sm_->route_diagnosis_,
                          ROUTE_INFO, route_info,
                          ret,
                          ObDiagnosisRouteInfo::USE_CACHED_SESSION,
                          s.server_info_.addr_,
                          ObMysqlTransact::is_in_trans(s),
                          false,
                          false)
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unknown type", K(type), KPC_(client_session->dummy_entry), K(sm_id), K(ret));
      }
    }
  } else {
    ret = OB_INVALID_ERROR;
    LOG_WDIAG("invalid dummy entry", "pll_info", s.pll_info_, K(ret));
  }
  return ret;
}

void ObMysqlTransact::handle_retry_last_time(ObTransState &s)
{
  // retry so many times, maybe the entry was expired, mark dirty
  if (s.pll_info_.set_dirty_all()) {
    LOG_INFO("the last retry chance, maybe the entry was expired"
             " set it to dirty, and wait for updating",
             "pll_info", s.pll_info_);
  }
}

ObMysqlTransact::ObSSRetryStatus ObMysqlTransact::retry_server_connection_not_open(ObTransState &s,
                                                                                   int32_t &retry_type)
{
  int ret = OB_SUCCESS;
  ObSSRetryStatus ret_status = NO_NEED_RETRY;

  if (OB_UNLIKELY(CONNECTION_ALIVE == s.current_.state_)
      || OB_UNLIKELY(ACTIVE_TIMEOUT == s.current_.state_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_EDIAG("[ObMysqlTransact::retry_server_connection_not_open] unexpected current server state",
              "server_state", ObMysqlTransact::get_server_state_name(s.current_.state_));
  } else {
    ObRespAnalyzeResult &resp = s.trans_info_.server_response_.get_analyze_result();
    // when use database error(ERROR 1049 (42000): Unknown database 'xxx') in innner request,
    // do not retry build new server session.
    if ((SERVER_SEND_USE_DATABASE == s.current_.send_action_) && resp.is_bad_db_error()) {
      ret_status = NO_NEED_RETRY;
      retry_type = NOT_RETRY;
      LOG_INFO("[ObMysqlTransact::retry_server_connection_not_open]"
               "database was dropped by other client session, no need to build new "
               "server session, ret_status = NO_NEED_RETRY");
    } else {
      // handle last chance retry
      if (s.current_.attempts_ == get_max_connect_attempts(s)) {
        handle_retry_last_time(s);
      }
      // target db server (sql comment & multi level config)
      if (s.use_cmnt_target_db_server_) {
        retry_type = ObRetryType::CMNT_TARGET_DB_SERVER;
        if (OB_FAIL(s.trans_info_.client_request_.get_parse_result().get_target_db_server()->get_next(s.server_info_.addr_))) {
          if (OB_NOT_NULL(s.sm_->route_diagnosis_) && s.sm_->route_diagnosis_->is_support_explain_route()) {
            // will transfer explain route packet
          } else {
            LOG_WDIAG("fail to get retry target db server from sql comment", K(ret));
          }
          ret = OB_SUCCESS;
        } else {
          ret_status = FOUND_EXISTING_ADDR;
          LOG_DEBUG("succ to get target db server from sql comment, will retry",
                  "address", s.server_info_.addr_);
        }
      } else if (s.use_conf_target_db_server_) {
        retry_type = ObRetryType::CONF_TARGET_DB_SERVER;
        if (OB_FAIL(s.sm_->target_db_server_->get_next(s.server_info_.addr_))) {
          if (OB_NOT_NULL(s.sm_->route_diagnosis_) && s.sm_->route_diagnosis_->is_support_explain_route()) {
            // will transfer explain route packet
          } else {
            LOG_WDIAG("fail to get retry target db server from config", K(ret));
          }
          ret = OB_SUCCESS;
        } else {
          ret_status = FOUND_EXISTING_ADDR;
          LOG_DEBUG("succ to get target db server from multi level config, will retry",
                "address", s.server_info_.addr_);
        }
      // config: server_routing_mode & test_server_addr
      } else if ((s.mysql_config_params_->is_mock_routing_mode() && !s.sm_->client_session_->is_proxy_mysql_client_)
          || s.mysql_config_params_->is_mysql_routing_mode()) {
        if (OB_FAIL(s.mysql_config_params_->get_one_test_server_addr(s.server_info_.addr_))) {
          LOG_WDIAG("mysql or mock mode, but test server addr in not set, disconnect", K(ret));
        } else {
          s.sm_->client_session_->test_server_addr_ = s.server_info_.addr_;
          ret_status = FOUND_EXISTING_ADDR;
          retry_type = ObRetryType::TEST_SERVER_ADDR;
          LOG_DEBUG("mysql mode, test server is valid, just use it to retry",
                    "address", s.server_info_.addr_);
        }
      } else if (s.pll_info_.replica_size() > 0) {
        int32_t attempt_count = 0;
        //as leader must first used, here found_leader_force_congested is usefull
        bool found_leader_force_congested = false;

        bool is_all_stale = false;
        const ObProxyReplicaLocation *replica = NULL;
        ObReadStaleParam param;
        build_read_stale_param(s, param);

        if (OB_FAIL(s.pll_info_.get_next_avail_replica(s.force_retry_congested_, attempt_count, found_leader_force_congested, param, is_all_stale, replica))) {
          LOG_WDIAG("fail to get next avail replica", K(ret));
        } else {
          s.current_.attempts_ += attempt_count;
          if ((NULL == replica) && !s.pll_info_.is_all_iterate_once()) { // just defense
            LOG_EDIAG("expected error", K_(s.pll_info));
            ret_status = NO_NEED_RETRY;
          } else {
            if ((NULL == replica) && s.pll_info_.is_all_iterate_once()) { // next round
              s.pll_info_.reset_cursor();
              if (s.pll_info_.is_weak_read() && is_all_stale && NULL != s.pll_info_.route_.table_entry_) {
                LOG_INFO("all replica staled, no avail replica found, try again");
                set_route_leader_replica(s, replica);
                if (replica == NULL) {
                  replica = s.pll_info_.get_next_avail_replica();
                }
              } else {
                // if we has tried all servers, do force retry in next round
                s.force_retry_congested_ = true;

                // get replica again
                replica = s.pll_info_.get_next_avail_replica();
              }
            }

            if ((NULL != replica) && replica->is_valid()) {
              struct sockaddr_storage sockaddr = replica->server_.get_sockaddr();
              s.server_info_.set_addr(*(struct sockaddr*)(&(sockaddr)));
              LOG_DEBUG("set addr here", K(s.server_info_.addr_));
              ret_status = FOUND_EXISTING_ADDR;
            } else {
              // unavailalbe replica, just defense
              LOG_EDIAG("can not found avail replica, unexpected branch", KPC(replica));
              replica = NULL;
              s.force_retry_congested_ = true;
              ret_status = NOT_FOUND_EXISTING_ADDR;
            }

            // if table entry is all dummy entry and comes from rslist
            // and all of servers are not in server list, we will disconnect and delete this cluster resource
            if (s.congestion_entry_not_exist_count_ == s.pll_info_.replica_size()
                && s.pll_info_.is_server_from_rslist()) {
              LOG_WDIAG("all servers of route which comes from rslist are not in congestion list,"
                        "will disconnect and delete this cluster resource ",
                        "route_info", s.pll_info_.route_);
              s.sm_->client_session_->set_need_delete_cluster();
              ret_status = NO_NEED_RETRY;
            }
          }
        }
        retry_type = USE_PARTITION_LOCATION_LOOKUP;
      } else {
        ObTableEntryName &te_name = s.pll_info_.te_name_;
        ret_status = NOT_FOUND_EXISTING_ADDR;
        retry_type = NOT_RETRY;
        LOG_INFO("no replica, retry to get table location again, "
                 "maybe used last session, ret_status = NOT_FOUND_EXISTING_ADDR", K(te_name));
      }
    }
  }

  if (OB_FAIL(ret)) {
    ret_status = NO_NEED_RETRY;
    retry_type = NOT_RETRY;
  }

  return ret_status;
}

inline void ObMysqlTransact::handle_server_connection_break(ObTransState &s)
{
  if (obmysql::OB_MYSQL_COM_QUIT == s.trans_info_.sql_cmd_) {
    s.current_.state_ = CONNECTION_CLOSED;
  }
  if (NULL != s.sm_->client_session_) {
    if (s.sm_->client_session_->get_session_info().get_priv_info().user_name_ == ObProxyTableInfo::DETECT_USERNAME_USER) {
      s.internal_error_op_for_diagnosis_ = PROXY_INTERNAL_ERROR_TRANSFER_DISCONNECT;
    } else {
      int64_t ss_id = (OB_ISNULL(s.sm_->get_server_session()) ? 0 : s.sm_->get_server_session()->ss_id_);
      uint32_t server_sessid = (OB_ISNULL(s.sm_->get_server_session()) ? 0 : s.sm_->get_server_session()->get_server_sessid());
      LOG_INFO("[ObMysqlTransact::handle_server_connection_break]",
          "client_ip", s.client_info_.addr_,
          "server_ip", s.server_info_.addr_,
          "cs_id", s.sm_->client_session_->get_cs_id(),
          "proxy_sessid", s.sm_->client_session_->get_proxy_sessid(),
          K(ss_id),
          K(server_sessid),
          "sm_id", s.sm_->sm_id_,
          "proxy_user_name", s.sm_->client_session_->get_session_info().get_priv_info().get_proxy_user_name(),
          "database_name", s.sm_->client_session_->get_session_info().get_database_name(),
          "server_state", ObMysqlTransact::get_server_state_name(s.current_.state_),
          "request_cmd", get_mysql_cmd_str(s.trans_info_.client_request_.get_packet_meta().cmd_),
          "sql_cmd", get_mysql_cmd_str(s.trans_info_.sql_cmd_),
          "sql", s.trans_info_.get_print_sql());

      if (obmysql::OB_MYSQL_COM_QUIT != s.trans_info_.sql_cmd_) {
        OBPROXY_XF_LOG(INFO, XFH_CONNECTION_SERVER_ABORT,
            "client_ip", s.client_info_.addr_,
            "server_ip", s.server_info_.addr_,
            "cluster_name", s.sm_->client_session_->get_session_info().get_priv_info().cluster_name_,
            "tenant_name", s.sm_->client_session_->get_session_info().get_priv_info().tenant_name_,
            "user_name", s.sm_->client_session_->get_session_info().get_priv_info().user_name_,
            "db", s.sm_->client_session_->get_session_info().get_database_name(),
            "server_state", ObMysqlTransact::get_server_state_name(s.current_.state_),
            "sql", s.trans_info_.client_request_.get_print_sql(),
            "request_cmd", get_mysql_cmd_str(s.trans_info_.sql_cmd_));

        s.trace_log_.log_it("[svr_connection_break]",
            "cli", s.client_info_.addr_,
            "svr", s.server_info_.addr_,
            "cluster", s.sm_->client_session_->get_session_info().get_priv_info().cluster_name_,
            "tenant", s.sm_->client_session_->get_session_info().get_priv_info().tenant_name_,
            "user", s.sm_->client_session_->get_session_info().get_priv_info().user_name_,
            "db", s.sm_->client_session_->get_session_info().get_database_name(),
            "svr_state", ObString(ObMysqlTransact::get_server_state_name(s.current_.state_)),
            "sql", s.trans_info_.client_request_.get_print_sql(),
            "sql_cmd", ObString(get_mysql_cmd_str(s.trans_info_.sql_cmd_)),
            "coord", s.sm_->client_session_->get_trans_coordinator_ss_addr());
        LOG_WDIAG("trace_log", K(s.trace_log_));
      }
    }
  } else {
    LOG_INFO("[ObMysqlTransact::handle_server_connection_break]",
             "client_ip", s.client_info_.addr_,
             "server_ip", s.server_info_.addr_,
             "server_state", ObMysqlTransact::get_server_state_name(s.current_.state_),
             "request_cmd", get_mysql_cmd_str(s.trans_info_.client_request_.get_packet_meta().cmd_),
             "sql_cmd", get_mysql_cmd_str(s.trans_info_.sql_cmd_),
             "sql", s.trans_info_.get_print_sql());

    if (obmysql::OB_MYSQL_COM_QUIT != s.trans_info_.sql_cmd_) {
      OBPROXY_XF_LOG(INFO, XFH_CONNECTION_SERVER_ABORT,
                     "client_ip", s.client_info_.addr_,
                     "server_ip", s.server_info_.addr_,
                     "server_state", ObMysqlTransact::get_server_state_name(s.current_.state_),
                     "sql", s.trans_info_.client_request_.get_print_sql(),
                     "request_cmd", get_mysql_cmd_str(s.trans_info_.sql_cmd_));

      s.trace_log_.log_it("[svr_connection_break]",
                     "cli", s.client_info_.addr_,
                     "svr", s.server_info_.addr_,
                     "svr_state", ObString(ObMysqlTransact::get_server_state_name(s.current_.state_)),
                     "sql", s.trans_info_.client_request_.get_print_sql(),
                     "sql_cmd", ObString(get_mysql_cmd_str(s.trans_info_.sql_cmd_)));
      LOG_WDIAG("trace_log", K(s.trace_log_));
    }
  }

  if (OB_UNLIKELY(CONNECTION_ALIVE == s.current_.state_)) {
    LOG_EDIAG("[ObMysqlTransact::handle_server_connection_break] unexpected current server state",
              "server_state", ObMysqlTransact::get_server_state_name(s.current_.state_));
  }

  MYSQL_INCREMENT_TRANS_STAT(BROKEN_SERVER_CONNECTIONS);
  // don't need record connection diagnosis info
  // most disconnection case already covered before
  s.next_action_ = SM_ACTION_SEND_ERROR_NOOP;
}

void ObMysqlTransact::handle_on_forward_server_response(ObTransState &s)
{
  LOG_DEBUG("[ObMysqlTransact::handle_on_forward_server_response]",
            "cur_send_action", ObMysqlTransact::get_send_action_name(s.current_.send_action_),"startm trans", s.is_hold_start_trans_);

  switch (s.current_.send_action_) {
    case SERVER_SEND_HANDSHAKE: {
      ObRespAnalyzeResult &resp = s.trans_info_.server_response_.get_analyze_result();
      if (resp.is_error_resp() || s.mysql_config_params_->is_mysql_routing_mode()) {
        s.next_action_ = SM_ACTION_SERVER_READ;
        if (get_client_session_info(s).is_oceanbase_server()) {
          s.sm_->api_.do_response_transform_open();
        }
      } else {
        bool server_support_ssl = resp.support_ssl();
        bool is_server_ssl_supported = s.sm_->enable_server_ssl_ &&
                      get_global_ssl_config_table_processor().is_ssl_key_info_valid(
                                       s.sm_->client_session_->get_vip_cluster_name(),
                                       s.sm_->client_session_->get_vip_tenant_name());
        LOG_DEBUG("ssl support", K(is_server_ssl_supported), K(server_support_ssl));
        if (is_server_ssl_supported && server_support_ssl
            && !s.sm_->client_session_->is_proxy_mysql_client_) {
          s.current_.send_action_ = SERVER_SEND_SSL_REQUEST;
          s.next_action_ = SM_ACTION_API_SEND_REQUEST;
          LOG_DEBUG("next senc action", K(s.current_.send_action_));
        } else if (s.is_auth_request_) {
          s.current_.send_action_ = SERVER_SEND_LOGIN;
          s.next_action_ = SM_ACTION_API_SEND_REQUEST;
        } else {
          if (OB_ISNULL(s.sm_->client_session_)) {
            s.current_.state_ = INTERNAL_ERROR;
            handle_server_connection_break(s);
            LOG_WDIAG("[ObMysqlTransact::handle_on_forward_server_response], "
                     "client session is NULL",
                     "next_action", ObMysqlTransact::get_action_name(s.next_action_),
                     K(s.sm_->client_session_), "server_session", s.sm_->get_server_session());
          } else {
            s.current_.send_action_ = SERVER_SEND_SAVED_LOGIN;
            s.next_action_ = SM_ACTION_API_SEND_REQUEST;
          }
        }
      }
      break;
    }

    case SERVER_SEND_SAVED_LOGIN: {
      ObClientSessionInfo &client_info = get_client_session_info(s);
      ObServerSessionInfo &server_info = get_server_session_info(s);
      ObRespAnalyzeResult &resp = s.trans_info_.server_response_.get_analyze_result();
      if (resp.is_error_resp()) {
        s.next_action_ = SM_ACTION_SERVER_READ;
        if (client_info.is_oceanbase_server()) {
          s.sm_->api_.do_response_transform_open();
        }
        break;
      } else if (client_info.is_oceanbase_server()) {
        bool is_proxy_mysql_client_ = s.sm_->client_session_->is_proxy_mysql_client_;
        if (is_binlog_request(s)) {
          if (client_info.need_reset_session_vars(server_info)) {
            s.current_.send_action_ = SERVER_SEND_SESSION_VARS;
            s.next_action_ = SM_ACTION_API_SEND_REQUEST;
          } else {
            s.current_.send_action_ = SERVER_SEND_REQUEST;
            s.next_action_ = SM_ACTION_API_SEND_REQUEST;
          }
          break;
        } else if (client_info.need_reset_all_session_vars() && !is_proxy_mysql_client_) {
          // 1.if global vars changed, we need to sync all session vars to keep
          //   server session vars is equal to the snapshot of client session
          // 2.proxy mysql client no need sync all session vars
          s.current_.send_action_ = SERVER_SEND_ALL_SESSION_VARS;
          s.next_action_ = SM_ACTION_API_SEND_REQUEST;
          break;
        } else {
          // we don't need to sync all session, check if we need to sync others
          // fall through:
        }
      } else {
        // we don't need to sync all session, check if we need to sync others
        // fall through:
      }
    }
    __attribute__ ((fallthrough));

    case SERVER_SEND_ALL_SESSION_VARS:
      if (OB_LIKELY(NULL != s.sm_->client_session_) && OB_LIKELY(NULL != s.sm_->get_server_session())) {
        ObClientSessionInfo &client_info = get_client_session_info(s);
        ObServerSessionInfo &server_info = get_server_session_info(s);
        //obutils::ObSqlParseResult &sql_result = s.trans_info_.client_request_.get_parse_result();
        obmysql::ObMySQLCmd cmd = s.trans_info_.client_request_.get_packet_meta().cmd_;
        if (obmysql::OB_MYSQL_COM_STMT_CLOSE == cmd
            || obmysql::OB_MYSQL_COM_STMT_RESET == cmd
            || is_binlog_request(s)
            || s.sm_->client_session_->can_direct_send_request_) {
          /* CLOSE 请求不同步任何东西, 直接发送出去 */
          s.current_.send_action_ = SERVER_SEND_REQUEST;
          s.next_action_ = SM_ACTION_API_SEND_REQUEST;
          break;
        } else if (client_info.need_reset_database(server_info)) {
          s.current_.send_action_ = SERVER_SEND_USE_DATABASE;
          s.next_action_ = SM_ACTION_API_SEND_REQUEST;
          break;
        } else {
          // fall through:
        }
      } else {
        s.current_.state_ = INTERNAL_ERROR;
        handle_server_connection_break(s);
        LOG_WDIAG("[ObMysqlTransact::handle_on_forward_server_response], "
                 "client session or server session is NULL",
                 "next_action", ObMysqlTransact::get_action_name(s.next_action_),
                 K(s.sm_->client_session_), "server_session", s.sm_->get_server_session());
        break;
      }
      __attribute__ ((fallthrough));
    case SERVER_SEND_USE_DATABASE:
      if (OB_LIKELY(NULL != s.sm_->client_session_) && OB_LIKELY(NULL != s.sm_->get_server_session())) {
        ObClientSessionInfo &client_info = get_client_session_info(s);
        ObServerSessionInfo &server_info = get_server_session_info(s);
        //obutils::ObSqlParseResult &sql_result = s.trans_info_.client_request_.get_parse_result();
        // sync the changed sys vars by config
        if ((!client_info.is_server_support_session_var_sync() ||
            client_info.need_reset_conf_sys_vars()) &&
            client_info.need_reset_session_vars(server_info)) {
          s.current_.send_action_ = SERVER_SEND_SESSION_VARS;
          s.next_action_ = SM_ACTION_API_SEND_REQUEST;
          break;
        } else if (client_info.is_server_support_session_var_sync() &&
            client_info.need_reset_user_session_vars(server_info)) {
          s.current_.send_action_ = SERVER_SEND_SESSION_USER_VARS;
          s.next_action_ = SM_ACTION_API_SEND_REQUEST;
          break;
        } else { /* fall through */ }
      } else {
        s.current_.state_ = INTERNAL_ERROR;
        handle_server_connection_break(s);
        LOG_WDIAG("[ObMysqlTransact::handle_on_forward_server_response], "
                 "client session or server session is NULL",
                 "next_action", ObMysqlTransact::get_action_name(s.next_action_),
                 K(s.sm_->client_session_), "server_session", s.sm_->get_server_session());
        break;
      }
      __attribute__ ((fallthrough));
    case SERVER_SEND_SESSION_VARS:
    case SERVER_SEND_SESSION_USER_VARS:
    case SERVER_SEND_PREPARE:
    case SERVER_SEND_TEXT_PS_PREPARE:
    case SERVER_SEND_XA_START:
    case SERVER_SEND_START_TRANS:
    case SERVER_SEND_INIT_SQL: {
      if (is_binlog_request(s)) {
        s.current_.send_action_ = SERVER_SEND_REQUEST;
        if (is_large_request(s)) { // large request
          // after sync all session variables, we need send user request.
          // and we call back to do_observer_open to send it, for maybe the
          // user request >8KB, and need transform to compress.
          s.next_action_ = SM_ACTION_OBSERVER_OPEN;
          s.send_reqeust_direct_ = true;
        } else {
          s.next_action_ = SM_ACTION_API_SEND_REQUEST;
        }
      } else if (OB_LIKELY(NULL != s.sm_->client_session_) && OB_LIKELY(NULL != s.sm_->get_server_session())) {
        ObClientSessionInfo &client_info = get_client_session_info(s);
        ObServerSessionInfo &server_info = get_server_session_info(s);
        obmysql::ObMySQLCmd cmd = s.trans_info_.client_request_.get_packet_meta().cmd_;
        ObProxyMysqlRequest &client_request = s.trans_info_.client_request_;
        if (s.is_hold_start_trans_) {
          s.current_.send_action_ = SERVER_SEND_START_TRANS;
        } else if (s.is_hold_xa_start_) {
          LOG_DEBUG("[ObMysqlTransact::handle_on_forward_server_response] set send action SERVER_SEND_XA_START for sync xa start");
          s.current_.send_action_ = SERVER_SEND_XA_START;
        } else if ((obmysql::OB_MYSQL_COM_STMT_EXECUTE == cmd
                    || obmysql::OB_MYSQL_COM_STMT_SEND_PIECE_DATA == cmd
                    || obmysql::OB_MYSQL_COM_STMT_SEND_LONG_DATA == cmd)
                   && client_info.need_do_prepare(server_info)) {
          s.current_.send_action_ = SERVER_SEND_PREPARE;
        } else if (client_request.get_parse_result().is_text_ps_execute_stmt() &&
          client_info.need_do_text_ps_prepare(server_info)) {
          s.current_.send_action_ = SERVER_SEND_TEXT_PS_PREPARE;
        } else if (!s.is_proxysys_tenant_
            && !s.sm_->client_session_->is_proxy_mysql_client_
            && !client_info.get_init_sql().empty()) {
            s.current_.send_action_ = SERVER_SEND_INIT_SQL;
          } else {
          s.current_.send_action_ = SERVER_SEND_REQUEST;
        }

        if (SERVER_SEND_REQUEST == s.current_.send_action_ &&
            ObMysqlTransact::need_use_tunnel(s)) {
          // after sync all session variables, we need send user request.
          // and we call back to do_observer_open to send it, for maybe the
          // user request >8KB, and need transform to compress.
          s.next_action_ = SM_ACTION_OBSERVER_OPEN;
          s.send_reqeust_direct_ = true;
        } else {
          s.next_action_ = SM_ACTION_API_SEND_REQUEST;
        }
      } else {
        s.current_.state_ = INTERNAL_ERROR;
        handle_server_connection_break(s);
        LOG_WDIAG("[ObMysqlTransact::handle_on_forward_server_response], "
                 "client session or server session is NULL",
                 "next_action", ObMysqlTransact::get_action_name(s.next_action_),
                 K(s.sm_->client_session_), "server_session", s.sm_->get_server_session());
      }
      break;
    }
    case SERVER_SEND_LOGIN: {
      s.next_action_ = SM_ACTION_SERVER_READ;
      if (get_client_session_info(s).is_oceanbase_server()) {
        s.sm_->api_.do_response_transform_open();
      }
      break;
    }
    case SERVER_SEND_REQUEST: {
      ObRespAnalyzeResult &resp = s.trans_info_.server_response_.get_analyze_result();
      obmysql::ObMySQLCmd cmd = s.trans_info_.client_request_.get_packet_meta().cmd_;
      ObProxyMysqlRequest &client_request = s.trans_info_.client_request_;
      // OB_MYSQL_COM_STMT_RESET 请求, 如果执行正确, 跳转; 否则返回 errro 包给 client
      if ((obmysql::OB_MYSQL_COM_STMT_RESET == cmd || client_request.get_parse_result().is_text_ps_drop_stmt())
        && resp.is_ok_resp()) {
        TRANSACT_RETURN(SM_ACTION_API_READ_REQUEST, ObMysqlTransact::handle_request);
      } else {
        s.next_action_ = SM_ACTION_SERVER_READ;
        if (get_client_session_info(s).is_oceanbase_server()) {
          s.sm_->api_.do_response_transform_open();
        }
      }
      break;
    }
    case SERVER_SEND_NONE:
    default:
      s.current_.state_ = INTERNAL_ERROR;
      handle_server_connection_break(s);
      LOG_EDIAG("Unknown server send next action", K(s.current_.send_action_));
      break;
  }

  // just print info
  if (OB_LIKELY(SM_ACTION_SERVER_READ == s.next_action_)) {
    LOG_DEBUG("[ObMysqlTransact::handle_on_forward_server_response]"
              "start send response, wait for reading request",
              "next_action", ObMysqlTransact::get_action_name(s.next_action_));
  } else {
    LOG_DEBUG("[ObMysqlTransact::handle_on_forward_server_response]",
              "next_send_action", ObMysqlTransact::get_send_action_name(s.current_.send_action_));
  }
  if (s.current_.state_ == INTERNAL_ERROR) {
    COLLECT_INTERNAL_DIAGNOSIS(
        s.sm_->connection_diagnosis_trace_, OB_PROXY_INTERNAL_TRACE,
        OB_PROXY_INTERNAL_ERROR,
        "an internal error ocurred while proxy forward server resp");
  }
  update_sql_cmd(s);
}

inline void ObMysqlTransact::update_sync_session_stat(ObTransState &s)
{
  LOG_DEBUG("[ObMysqlTransact::update_sync_session_stat]",
            "cur_send_action", ObMysqlTransact::get_send_action_name(s.current_.send_action_));

  ObHRTime start = s.sm_->milestones_.server_.server_write_begin_;
  ObHRTime end = s.sm_->milestones_.server_.server_read_end_;
  ObHRTime cost = milestone_diff(start, end)
      + s.sm_->cmd_time_stats_.plugin_decompress_response_time_
      + s.sm_->cmd_time_stats_.server_response_analyze_time_
      + s.sm_->cmd_time_stats_.ok_packet_trim_time_;
  switch (s.current_.send_action_) {
    case SERVER_SEND_SAVED_LOGIN:
      s.sm_->cmd_time_stats_.server_send_saved_login_time_ += cost;
      s.sm_->cmd_time_stats_.server_sync_session_variable_time_ += cost;
      MYSQL_SUM_TIME_STAT(TOTAL_SEND_SAVED_LOGIN_TIME, cost);
      MYSQL_INCREMENT_TRANS_STAT(SEND_SAVED_LOGIN_REQUESTS);
      break;

    case SERVER_SEND_ALL_SESSION_VARS:
      s.sm_->cmd_time_stats_.server_send_all_session_variable_time_ += cost;
      s.sm_->cmd_time_stats_.server_sync_session_variable_time_ += cost;
      MYSQL_SUM_TIME_STAT(TOTAL_SEND_ALL_SESSION_VARS_TIME, cost);
      MYSQL_INCREMENT_TRANS_STAT(SEND_ALL_SESSION_VARS_REQUESTS);
      break;

    case SERVER_SEND_USE_DATABASE:
      s.sm_->cmd_time_stats_.server_send_use_database_time_ += cost;
      s.sm_->cmd_time_stats_.server_sync_session_variable_time_ += cost;
      MYSQL_SUM_TIME_STAT(TOTAL_SEND_USE_DATABASE_TIME, cost);
      MYSQL_INCREMENT_TRANS_STAT(SEND_USE_DATABASE_REQUESTS);
      break;

    case SERVER_SEND_SESSION_VARS:
      s.sm_->cmd_time_stats_.server_send_session_variable_time_ += cost;
      s.sm_->cmd_time_stats_.server_sync_session_variable_time_ += cost;
      MYSQL_SUM_TIME_STAT(TOTAL_SEND_CHANGED_SESSION_VARS_TIME, cost);
      MYSQL_INCREMENT_TRANS_STAT(SEND_CHANGED_SESSION_VARS_REQUESTS);
      break;

    case SERVER_SEND_SESSION_USER_VARS:
      s.sm_->cmd_time_stats_.server_send_session_user_variable_time_ += cost;
      s.sm_->cmd_time_stats_.server_sync_session_variable_time_ += cost;
      MYSQL_SUM_TIME_STAT(TOTAL_SEND_CHANGED_SESSION_USER_VARS_TIME, cost);
      MYSQL_INCREMENT_TRANS_STAT(SEND_CHANGED_SESSION_USER_VARS_REQUESTS);
      break;

    case SERVER_SEND_START_TRANS:
      s.sm_->cmd_time_stats_.server_send_start_trans_time_ += cost;
      s.sm_->cmd_time_stats_.server_sync_session_variable_time_ += cost;
      MYSQL_SUM_TIME_STAT(TOTAL_SEND_START_TRANS_TIME, cost);
      MYSQL_INCREMENT_TRANS_STAT(SEND_START_TRANS_REQUESTS);
      break;

    case SERVER_SEND_XA_START:
      s.sm_->cmd_time_stats_.server_send_xa_start_time_ += cost;
      s.sm_->cmd_time_stats_.server_sync_session_variable_time_ += cost;
      MYSQL_SUM_TIME_STAT(TOTAL_SEND_XA_START_TIME, cost);
      MYSQL_INCREMENT_TRANS_STAT(SEND_XA_START_REQUESTS);
      break;

    case SERVER_SEND_INIT_SQL:
      s.sm_->cmd_time_stats_.server_send_init_sql_time_ += cost;
      break;

    case SERVER_SEND_HANDSHAKE:
    case SERVER_SEND_LOGIN:
    case SERVER_SEND_REQUEST:
    case SERVER_SEND_PREPARE:
    case SERVER_SEND_TEXT_PS_PREPARE:
    case SERVER_SEND_NONE:
      break;

    default:
      LOG_EDIAG("Unknown server send next action", K(s.current_.send_action_));
      break;
  }
}

void ObMysqlTransact::handle_transform_ready(ObTransState &s)
{
  s.pre_transform_source_ = s.source_;
  s.source_ = SOURCE_TRANSFORM;

  TRANSACT_RETURN(SM_ACTION_TRANSFORM_READ, NULL);
}

inline void ObMysqlTransact::handle_server_failed(ObTransState &s)
{
  switch (s.current_.state_) {
    case ALIVE_CONGESTED:
    case DEAD_CONGESTED:
    case DETECT_CONGESTED:
    case INTERNAL_ERROR:
      break;

    case CONNECTION_ALIVE:
    case RESPONSE_ERROR: {
      ObRespAnalyzeResult &resp = s.trans_info_.server_response_.get_analyze_result();
      if (resp.is_error_resp()) {

        switch (resp.error_pkt_.get_err_code()) {
          case -OB_SERVER_IS_INIT:
          case -OB_SERVER_IS_STOPPING:
          case -OB_PACKET_CHECKSUM_ERROR:
          case -OB_ALLOCATE_MEMORY_FAILED:
            // congestion control
            LOG_INFO("ObMysqlTransact::handle_server_failed", "err code",
                      resp.error_pkt_.get_err_code(), KPC(s.congestion_entry_));
            if (s.sm_->client_session_->get_session_info().is_oceanbase_server()) {
              s.set_alive_failed();
            }
            break;
          case -OB_NOT_MASTER: {
            // if received not master error, it means the partition locations of
            // the certain table entry has expired, so we need delay to update it;
            if (s.pll_info_.set_delay_update()) {
              LOG_INFO("received not master error, the route entry is expired, "
                       "set it to dirty, and wait for updating",
                       "origin_name", s.pll_info_.te_name_,
                       "server_ip", s.server_info_.addr_,
                       "route info", s.pll_info_.route_);
            }
            break;
          }
          case -OB_ERR_READ_ONLY: {
            if (ZONE_TYPE_READONLY == s.pll_info_.route_.cur_chosen_server_.zone_type_
                || ZONE_TYPE_ENCRYPTION == s.pll_info_.route_.cur_chosen_server_.zone_type_) {
              LOG_WDIAG("zone is readonly or encryption, but server tell error response, "
                       "maybe this new server do not support old agreement, try next server",
                       "zone_type", zone_type_to_str(s.pll_info_.route_.cur_chosen_server_.zone_type_),
                       "origin_name", s.pll_info_.te_name_,
                       "sql_cmd", get_mysql_cmd_str(s.trans_info_.sql_cmd_),
                       "sql", s.trans_info_.client_request_.get_print_sql(),
                       "route info", s.pll_info_.route_);
            } else {
              LOG_WDIAG("zone is readonly, proxy should not send request to it, "
                       "maybe zone type has been changed, try next server",
                       "origin_name", s.pll_info_.te_name_,
                       "sql_cmd", get_mysql_cmd_str(s.trans_info_.sql_cmd_),
                       "sql", s.trans_info_.client_request_.get_print_sql(),
                       "route info", s.pll_info_.route_);
            }
            break;
          }
          default:
            break;
        }
      }
      break;
    }

    case INACTIVE_TIMEOUT:
      if (obmysql::OB_MYSQL_COM_QUIT == s.trans_info_.sql_cmd_) {
        break;
      }
      // fall through

    case ACTIVE_TIMEOUT:
    case CONNECTION_ERROR:
    case STATE_UNDEFINED:
    case ANALYZE_ERROR:
    case CONNECT_ERROR:
    case CONNECTION_CLOSED:
      // As dead_congested is only be controlled by server state refresh
      // we need set it alive congested here when failed to connect
      LOG_INFO("ObMysqlTransact::handle_server_failed", "current state",
              get_server_state_name(s.current_.state_), "attempts", s.current_.attempts_,
              KPC(s.congestion_entry_));
      if (s.sm_->client_session_->get_session_info().is_oceanbase_server()) {
        s.set_alive_failed();
      }
      break;

    // can not happen
    case CMD_COMPLETE:
    case TRANSACTION_COMPLETE:
    default:
      LOG_EDIAG("s.current_.state_ is set to something unsupported", K(s.current_.state_));
      break;
  }
}

void ObMysqlTransact::histogram_response_size(ObTransState &s, int64_t response_size)
{
  if (response_size >= 0 && response_size <= 100) {
    MYSQL_INCREMENT_TRANS_STAT(RESPONSE_SIZE_100_COUNT);
  } else if (response_size <= 1024) {
    MYSQL_INCREMENT_TRANS_STAT(RESPONSE_SIZE_1K_COUNT);
  } else if (response_size <= 3072) {
    MYSQL_INCREMENT_TRANS_STAT(RESPONSE_SIZE_3K_COUNT);
  } else if (response_size <= 5120) {
    MYSQL_INCREMENT_TRANS_STAT(RESPONSE_SIZE_5K_COUNT);
  } else if (response_size <= 10240) {
    MYSQL_INCREMENT_TRANS_STAT(RESPONSE_SIZE_10K_COUNT);
  } else if (response_size <= 1048576) {
    MYSQL_INCREMENT_TRANS_STAT(RESPONSE_SIZE_1M_COUNT);
  } else {
    MYSQL_INCREMENT_TRANS_STAT(RESPONSE_SIZE_INF_COUNT);
  }
}

void ObMysqlTransact::histogram_request_size(ObTransState &s, int64_t request_size)
{
  if (request_size >= 0 && request_size <= 100) {
    MYSQL_INCREMENT_TRANS_STAT(REQUEST_SIZE_100_COUNT);
  } else if (request_size <= 1024) {
    MYSQL_INCREMENT_TRANS_STAT(REQUEST_SIZE_1K_COUNT);
  } else if (request_size <= 3072) {
    MYSQL_INCREMENT_TRANS_STAT(REQUEST_SIZE_3K_COUNT);
  } else if (request_size <= 5120) {
    MYSQL_INCREMENT_TRANS_STAT(REQUEST_SIZE_5K_COUNT);
  } else if (request_size <= 10240) {
    MYSQL_INCREMENT_TRANS_STAT(REQUEST_SIZE_10K_COUNT);
  } else if (request_size <= 1048576) {
    MYSQL_INCREMENT_TRANS_STAT(REQUEST_SIZE_1M_COUNT);
  } else {
    MYSQL_INCREMENT_TRANS_STAT(REQUEST_SIZE_INF_COUNT);
  }
}

void ObMysqlTransact::client_connection_speed(ObTransState &s, ObHRTime transfer_time, int64_t nbytes)
{
  double bytes_per_hrtime = (0 == transfer_time) ? (static_cast<double>(nbytes))
      : (static_cast<double>(nbytes) / static_cast<double>(transfer_time));
  int64_t bytes_per_sec = static_cast<int64_t>(bytes_per_hrtime * HRTIME_SECOND);

  if (bytes_per_sec <= 100) {
    MYSQL_INCREMENT_TRANS_STAT(CLIENT_SPEED_BYTES_PER_SEC_100);
  } else if (bytes_per_sec <= 1024) {
    MYSQL_INCREMENT_TRANS_STAT(CLIENT_SPEED_BYTES_PER_SEC_1K);
  } else if (bytes_per_sec <= 10240) {
    MYSQL_INCREMENT_TRANS_STAT(CLIENT_SPEED_BYTES_PER_SEC_10K);
  } else if (bytes_per_sec <= 102400) {
    MYSQL_INCREMENT_TRANS_STAT(CLIENT_SPEED_BYTES_PER_SEC_100K);
  } else if (bytes_per_sec <= 1048576) {
    MYSQL_INCREMENT_TRANS_STAT(CLIENT_SPEED_BYTES_PER_SEC_1M);
  } else if (bytes_per_sec <= 10485760) {
    MYSQL_INCREMENT_TRANS_STAT(CLIENT_SPEED_BYTES_PER_SEC_10M);
  } else {
    MYSQL_INCREMENT_TRANS_STAT(CLIENT_SPEED_BYTES_PER_SEC_100M);
  }
}

void ObMysqlTransact::server_connection_speed(ObTransState &s, ObHRTime transfer_time, int64_t nbytes)
{
  double bytes_per_hrtime = (0 == transfer_time) ? (static_cast<double>(nbytes))
      : (static_cast<double>(nbytes) / static_cast<double>(transfer_time));
  int64_t bytes_per_sec = static_cast<int64_t>(bytes_per_hrtime * HRTIME_SECOND);

  if (bytes_per_sec <= 100) {
    MYSQL_INCREMENT_TRANS_STAT(SERVER_SPEED_BYTES_PER_SEC_100);
  } else if (bytes_per_sec <= 1024) {
    MYSQL_INCREMENT_TRANS_STAT(SERVER_SPEED_BYTES_PER_SEC_1K);
  } else if (bytes_per_sec <= 10240) {
    MYSQL_INCREMENT_TRANS_STAT(SERVER_SPEED_BYTES_PER_SEC_10K);
  } else if (bytes_per_sec <= 102400) {
    MYSQL_INCREMENT_TRANS_STAT(SERVER_SPEED_BYTES_PER_SEC_100K);
  } else if (bytes_per_sec <= 1048576) {
    MYSQL_INCREMENT_TRANS_STAT(SERVER_SPEED_BYTES_PER_SEC_1M);
  } else if (bytes_per_sec <= 10485760) {
    MYSQL_INCREMENT_TRANS_STAT(SERVER_SPEED_BYTES_PER_SEC_10M);
  } else {
    MYSQL_INCREMENT_TRANS_STAT(SERVER_SPEED_BYTES_PER_SEC_100M);
  }
}

void ObMysqlTransact::client_result_stat(ObTransState &s)
{
  if (ABORTED == s.client_info_.abort_) {
    MYSQL_INCREMENT_TRANS_STAT(CLIENT_CONNECTION_ABORT_COUNT);
  } else if (DIDNOT_ABORT == s.client_info_.abort_) {
    MYSQL_INCREMENT_TRANS_STAT(CLIENT_COMPLETED_REQUESTS);
  }
  // TODO: add more client stats by client's point of view
}

int ObMysqlTransact::add_new_stat_block(ObTransState &s)
{
  int ret = OB_SUCCESS;
  ObStatBlock *new_block = NULL;

  // We keep the block around till the end of transaction
  // We don't need explicitly deallocate it later since
  // when the transaction is over, the arena will be destroyed
  if (OB_ISNULL(new_block = reinterpret_cast<ObStatBlock *>(s.arena_.alloc(sizeof(ObStatBlock))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_EDIAG("failed to allocate stat block", K(ret));
  } else {
    new_block->reset();
    s.current_stats_->next_ = new_block;
    s.current_stats_ = new_block;
    LOG_DEBUG("Adding new large stat block");
  }
  return ret;
}

int ObMysqlTransact::encode_error_message(ObTransState &s, int err_code)
{
  int ret = OB_SUCCESS;

  if (s.sm_ != NULL) {
    if (s.sm_->get_client_buffer_reader() != NULL && OB_FAIL(s.sm_->get_client_buffer_reader()->consume_all())) {
      LOG_WDIAG("client buffer reader fail to consume all", K(ret));
    } else {
      ObMysqlClientSession *client_session = s.sm_->get_client_session();
      if (err_code != 0) {
        s.mysql_errcode_ = err_code;
      }
      if (OB_FAIL(ObMysqlTransact::build_error_packet(s, client_session))) {
        LOG_WDIAG("[ObMysqlTransact::encode_error_message] fail to build error packet", "sm_id", s.sm_->sm_id_, "errcode", s.mysql_errcode_, K(ret));
      }
    }
  }
  return ret;
}

// !!!ATTENTION, if use build_error_packet directly, have to check if client_buffer_readr_ consumed correctly
int ObMysqlTransact::build_error_packet(ObTransState &s, ObMysqlClientSession *client_session)
{
  int ret = OB_SUCCESS;
  if (0 == s.mysql_errcode_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("no mysql errcode", K(ret), K_(s.mysql_errcode));
  }

  ObMIOBuffer *buf = NULL;
  if (OB_SUCC(ret)) {
    if (NULL != s.internal_buffer_) {
      buf = s.internal_buffer_;
    } else {
      if (OB_FAIL(s.alloc_internal_buffer(MYSQL_BUFFER_SIZE))) {
        LOG_EDIAG("[ObMysqlTransact::build_error_packet] fail to allocate internal buffer", K(ret));
      } else {
        buf = s.internal_buffer_;
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObMysqlSM *sm = s.sm_;
    ObProxyProtocol client_protocol = ObProxyProtocol::PROTOCOL_NORMAL;
    if (sm != NULL) {
      client_protocol = sm->get_client_session_protocol();
    }

    int errcode = s.mysql_errcode_;
    s.inner_errcode_ = s.mysql_errcode_;
    uint8_t next_seq = 0;
    if (s.is_auth_request_) {
      if (client_session != NULL) {
        ObMysqlAuthRequest &auth_req = client_session->get_session_info().get_login_req();
        next_seq = static_cast<uint8_t>(auth_req.get_packet_meta().pkt_seq_ + 1);
      }
    } else {
      ObProxyMysqlRequest &client_request = s.trans_info_.client_request_;
      next_seq = static_cast<uint8_t>(client_request.get_packet_meta().pkt_seq_ + 1);
    }

    if (NULL != s.mysql_errmsg_) {
      const char *errmsg = s.mysql_errmsg_;
      if (OB_FAIL(ObProxyPacketWriter::write_error_packet(*buf, client_session, client_protocol,
                                                          next_seq, errcode, errmsg))) {
        LOG_WDIAG("[ObMysqlTransact::build_error_packet] fail to encode err pacekt buf",
                 K(next_seq), K(errmsg), K(errcode), K(ret));
      }
    } else {
      switch (errcode) {
        case OB_PASSWORD_WRONG: {
          const int64_t BUF_LEN = OB_MAX_ERROR_MSG_LEN;
          char err_msg_buf[BUF_LEN];
          err_msg_buf[0] = '\0';
          char *errmsg = err_msg_buf;
          int64_t pos = 0;
          int64_t name_len = 0;
          const char *name_str = NULL;
          bool has_auth_resp = false;
          if (client_session != NULL) {
            ObClientSessionInfo &client_info = client_session->get_session_info();
            ObMysqlAuthRequest &auth_req = client_info.get_login_req();
            ObHSRResult &result = auth_req.get_hsr_result();
            if (!result.is_clustername_from_default_) {
              name_len = client_info.get_full_username().length();
              name_str = client_info.get_full_username().ptr();
            } else {
              name_len = result.user_tenant_name_.length();
              name_str = result.user_tenant_name_.ptr();
            }
            has_auth_resp = !result.response_.get_auth_response().empty();
          }
          char ip_buff[INET6_ADDRSTRLEN];
          ip_buff[0] = '\0';
          ops_ip_ntop(s.client_info_.addr_, ip_buff, sizeof(ip_buff));
          if (OB_FAIL(databuff_printf(errmsg, BUF_LEN, pos, ob_str_user_error(errcode),
                  name_len, name_str, strlen(ip_buff), ip_buff, (has_auth_resp ? "YES" : "NO")))) {
            LOG_WDIAG("fail to fill err_msg", K(ret));
          } else if (OB_FAIL(ObProxyPacketWriter::write_error_packet(*buf, client_session, client_protocol,
                                                                     next_seq, errcode, errmsg))) {
            LOG_WDIAG("[ObMysqlTransact::build_error_packet] fail to encode err pacekt buf",
                     K(next_seq), K(errcode), K(ret));
          } else {
            // nothing
          }
          break;
        }
        case OB_ERR_ABORTING_CONNECTION: {
          const int64_t BUF_LEN = OB_MAX_ERROR_MSG_LEN;
          char err_msg_buf[BUF_LEN];
          err_msg_buf[0] = '\0';
          char *errmsg = err_msg_buf;
          int64_t pos = 0;
          char ip_buff[MAX_IP_ADDR_LENGTH];
          ip_buff[0] = '\0';
          ObAddr local_addr;

          if (OB_FAIL(ObProxyTableProcessorUtils::get_proxy_local_addr(local_addr))) {
            LOG_WDIAG("fail to get proxy local addr", K(local_addr), K(ret));
          } else if (OB_UNLIKELY(!local_addr.ip_to_string(ip_buff, MAX_IP_ADDR_LENGTH))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("fail to covert ip to string", K(local_addr), K(ret));
          } else if (OB_FAIL(databuff_printf(errmsg, BUF_LEN, pos, ob_str_user_error(errcode), strlen(ip_buff), ip_buff))) {
            LOG_WDIAG("fail to fill err_msg", K(ret));
          } else if (OB_FAIL(ObProxyPacketWriter::write_error_packet(*buf, client_session, client_protocol,
                                                                     next_seq, errcode, errmsg))) {
            LOG_WDIAG("[ObMysqlTransact::build_error_packet] fail to build not supported err resp", K(ret));
          }
          break;
        }
        case OB_GET_LOCATION_TIME_OUT:
        case OB_CLUSTER_NOT_EXIST:
        case OB_ERR_OPERATOR_UNKNOWN:
        case OB_ERR_TOO_MANY_SESSIONS:
        case OB_EXCEED_MEM_LIMIT:
        case OB_UNSUPPORTED_PS:
        case OB_ERR_FETCH_OUT_SEQUENCE:
        case OB_CURSOR_NOT_EXIST:
        case OB_ERR_CON_COUNT_ERROR:
        case OB_NOT_SUPPORTED:
        case OB_ERR_PREPARE_STMT_NOT_FOUND:
        case OB_PROXY_HOLD_CONNECTION:
        case OB_PROXY_INTERNAL_ERROR:
        case OB_SERVER_BUILD_CONNECTION_ERROR:
        case OB_SERVER_RECEIVING_PACKET_CONNECTION_ERROR:
        case OB_SERVER_CONNECT_TIMEOUT:
        case OB_SERVER_TRANSFERRING_PACKET_CONNECTION_ERROR:
        case OB_PROXY_INACTIVITY_TIMEOUT:
        case OB_PROXY_INTERNAL_REQUEST_FAIL:
        case OB_PROXY_NO_NEED_RETRY:
        case OB_CONNECT_BINLOG_ERROR: {
          char *err_msg = NULL;
          if (OB_FAIL(ObProxyPacketWriter::get_err_buf(errcode, err_msg))) {
            LOG_WDIAG("fail to get err buf", K(ret));
          } else if (OB_FAIL(ObProxyPacketWriter::write_error_packet(*buf, client_session, client_protocol,
                                                                     next_seq, errcode, err_msg))) {
            LOG_WDIAG("[ObMysqlTransact::build_error_packet] fail to build not supported err resp", K(ret));
          } else {
            // nothing
          }
          break;
        }
        // add others...
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("unhandled errcode", K(errcode), K(ret));
          break;
        }
      }
    }
  }

  // clear
  s.mysql_errcode_ = 0;
  s.mysql_errmsg_ = NULL;

  return ret;
}

void ObMysqlTransact::handle_new_config_acquired(ObTransState &s)
{
  // update cached dummy entry valid time
  if ((NULL != s.sm_) && (NULL != s.sm_->client_session_) && (NULL != s.mysql_config_params_)) {
    int64_t valid_ns = ObRandomNumUtils::get_random_half_to_full(
        s.mysql_config_params_->tenant_location_valid_time_);
    s.sm_->client_session_->dummy_entry_valid_time_ns_ = valid_ns;
    LOG_DEBUG("now cached dummy entry valid time", K(valid_ns));
  }
}

void ObMysqlTransact::ObTransState::refresh_mysql_config()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(mysql_config_params_ != get_global_mysql_config_processor().get_config())) {
    ObMysqlConfigParams *config = NULL;
    if (OB_ISNULL(config = get_global_mysql_config_processor().acquire())) {
      PROXY_TXN_LOG(WDIAG, "fail to acquire mysql config");
    } else {
      if (OB_LIKELY(NULL != mysql_config_params_)) {
        mysql_config_params_->dec_ref();
        mysql_config_params_ = NULL;
      }
      mysql_config_params_ = config;
    }
    ObMysqlTransact::handle_new_config_acquired(*this);
  }

  if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
    if (NULL != sqlaudit_record_queue_) {
      sqlaudit_record_queue_->refcount_dec();
      sqlaudit_record_queue_ = NULL;
    }

    if (mysql_config_params_->sqlaudit_mem_limited_ > 0) {
      sqlaudit_record_queue_ = get_global_sqlaudit_processor().acquire();
    }
  }

  // record v2 config
  if (is_auth_request_) {
    if (mysql_config_params_->enable_ob_protocol_v2_) {
      sm_->set_server_protocol(ObProxyProtocol::PROTOCOL_OB20);
    } else if (mysql_config_params_->enable_compression_protocol_) {
      sm_->set_server_protocol(ObProxyProtocol::PROTOCOL_CHECKSUM);
    } else {
      sm_->set_server_protocol(ObProxyProtocol::PROTOCOL_NORMAL);
    }
    PROXY_CS_LOG(DEBUG, "client session start with ", "cs_id", sm_->get_client_session()->get_cs_id(),
                        "server_protocol", sm_->get_server_protocol());
  }

  // 开始获取版本号，如果有并发修改，会推高版本，下次执行到这还会继续更新
  ObNetVConnection *client_vc = static_cast<ObNetVConnection*>(sm_->get_client_session()->get_netvc());
  ObMysqlClientSession *client_session = sm_->get_client_session();
  ObClientSessionInfo &session_info = sm_->get_client_session()->get_session_info();
  uint64_t global_version = get_global_proxy_config_table_processor().get_config_version();
  if (sm_->config_version_ != global_version && NULL != client_vc) {
    ObVipAddr addr = client_session->get_ct_info().vip_tenant_.vip_addr_;

    ObString cluster_name;
    ObString tenant_name;
    if (OB_FAIL(session_info.get_cluster_name(cluster_name))) {
      if (ret == OB_ENTRY_NOT_EXIST) {
        ret = OB_SUCCESS;
      } else {
        LOG_WDIAG("get cluster name failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(session_info.get_tenant_name(tenant_name))) {
      if (ret == OB_ENTRY_NOT_EXIST) {
        ret = OB_SUCCESS;
      } else {
        LOG_WDIAG("get tenant name failed", K(ret));
      }
    }
    // init route diagnosis at first trans request
    // route diagnosis only works when the request is from user client
    if (!client_session->is_proxy_mysql_client_ &&
        !client_session->is_proxyro_user()) {
      if (OB_SUCC(ret)) {
        // route diagnosis
        int64_t level = get_global_proxy_config().route_diagnosis_level.get_value();
        if (level == 0) {
          if (OB_NOT_NULL(sm_->route_diagnosis_)) {
            sm_->route_diagnosis_->dec_ref();
            sm_->route_diagnosis_ = NULL;
          }
          LOG_DEBUG("route diagnosis disabled");
        } else if (OB_ISNULL(sm_->route_diagnosis_)) {
          if (OB_ISNULL(sm_->route_diagnosis_ = op_alloc(ObRouteDiagnosis))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WDIAG("fail to alloc memory to for route diagnosis", K(ret));
          } else {
            sm_->route_diagnosis_->inc_ref();
            sm_->route_diagnosis_->set_level((ObDiagnosisLevel) level);
            LOG_DEBUG("succ to init route diagnosis", K(level));
          }
        } else {
          sm_->route_diagnosis_->set_level((ObDiagnosisLevel) level);
          LOG_DEBUG("succ to set route diagnosis level", K(level));
        }
      }

      if (OB_SUCC(ret)) {
        if (0 < get_global_proxy_config().protocol_diagnosis_level.get_value()) {
          if (OB_NOT_NULL(sm_->protocol_diagnosis_)) {
            /* do nothing */
          } else if (OB_ISNULL(sm_->protocol_diagnosis_) && OB_FAIL(ObProtocolDiagnosis::alloc(sm_->protocol_diagnosis_))) {
            LOG_WDIAG("fail to alloc memory for protocol diagnosis", K(ret));
          } else {
            INC_SHARED_REF(sm_->analyzer_.get_protocol_diagnosis_ref(), sm_->protocol_diagnosis_);
            INC_SHARED_REF(sm_->compress_ob20_analyzer_.get_protocol_diagnosis_ref(), sm_->protocol_diagnosis_);
            INC_SHARED_REF(sm_->compress_analyzer_.get_protocol_diagnosis_ref(), sm_->protocol_diagnosis_);
            LOG_DEBUG("succ to enable protocol diagnosis");
          }
        } else {
          /* disable protocol diagnosis */
          DEC_SHARED_REF(sm_->protocol_diagnosis_);
        }
      }
    }

    if (OB_SUCC(ret)) {
      // if user login with vip and specifying username, tenant name, cluster name
      // we use vip clustet name and tenant name before OB_MYSQL_COM_LOGIN(clsuter name and tenant name empty) to get config while login info user specified not received
      if (client_session->is_need_convert_vip_to_tname()
          && client_session->is_vip_lookup_success()
          && cluster_name.empty()
          && tenant_name.empty()) {
        cluster_name = sm_->get_client_session()->get_vip_cluster_name();
        tenant_name = sm_->get_client_session()->get_vip_tenant_name();
        if (OB_FAIL(get_config_item(cluster_name, tenant_name, addr, global_version))) {
          LOG_WDIAG("get config item failed", K(cluster_name), K(tenant_name), K(addr), K(ret));
        }
      } else {
        if (OB_SUCC(ret) || is_auth_request_) {
          if (OB_FAIL(get_config_item(cluster_name, tenant_name, addr, global_version))) {
            LOG_WDIAG("get config item failed", K(cluster_name), K(tenant_name), K(addr), K(ret));
          }
        }
      }
    }
    LOG_DEBUG("get config succ", K(addr), K(cluster_name), K(tenant_name),
        K_(sm_->proxy_route_policy), K_(sm_->proxy_idc_name), K_(sm_->proxy_primary_zone_name),
        K_(sm_->enable_cloud_full_username), K_(sm_->enable_client_ssl),
        K_(sm_->enable_server_ssl), K_(sm_->enable_transaction_split), K_(sm_->enable_transaction_split),
        K(session_info.is_request_follower_user()), K(session_info.is_read_only_user()),
        K_(sm_->target_db_server), K(ret));
  }
}

int ObMysqlTransact::ObTransState::get_config_item(const ObString& cluster_name,
                                                   const ObString &tenant_name,
                                                   const ObVipAddr &addr,
                                                   const int64_t global_version)
{
  int ret = OB_SUCCESS;
  ObClientSessionInfo &session_info = sm_->get_client_session()->get_session_info();
  bool sync_conf_sys_var = false;

  if (is_auth_request_) {
    if (OB_SUCC(ret)) {
      ObConfigBoolItem bool_item;
      if (OB_FAIL(get_global_config_processor().get_proxy_config_bool_item(
              addr, cluster_name, tenant_name, "enable_cloud_full_username", bool_item))) {
        LOG_WDIAG("get enable_cloud_full_username failed", K(addr), K(cluster_name), K(tenant_name), K(ret));
      } else {
        sm_->enable_cloud_full_username_ = bool_item.get_value();
      }
    }

    if (OB_SUCC(ret)) {
      ObConfigBoolItem bool_item;
      if (OB_FAIL(get_global_config_processor().get_proxy_config_bool_item(
              addr, cluster_name, tenant_name, "enable_client_ssl", bool_item))) {
        LOG_WDIAG("get enable_client_ssl failed", K(addr), K(cluster_name), K(tenant_name), K(ret));
      } else {
        sm_->enable_client_ssl_ = bool_item.get_value();
      }
    }

    if (OB_SUCC(ret)) {
      ObConfigBoolItem bool_item;
      if (OB_FAIL(get_global_config_processor().get_proxy_config_bool_item(
              addr, cluster_name, tenant_name, "enable_server_ssl", bool_item))) {
        LOG_WDIAG("get enable_server_ssl failed", K(addr), K(cluster_name), K(tenant_name), K(ret));
      } else {
        sm_->enable_server_ssl_ = bool_item.get_value();
      }
    }

    if (OB_SUCC(ret)) {
      ObConfigItem item;
      ObProxyConfigTableProcessor::SSLAttributes ssl_attributes;
      if (OB_FAIL(get_global_config_processor().get_proxy_config(
              addr, cluster_name, tenant_name, "ssl_attributes", item))) {
        LOG_WDIAG("get ssl attributes failed", K(addr), K(cluster_name), K(tenant_name), K(ret));
      } else if (0 != strlen(item.str())) {
        if (OB_FAIL(ObProxyConfigTableProcessor::parse_ssl_attributes(item, ssl_attributes))) {
          LOG_WDIAG("fail to parse ssl attributes", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        sm_->ssl_attributes_ = ssl_attributes;
      }
    }

    if (OB_SUCC(ret)) {
      ObConfigItem item;
      if (OB_FAIL(get_global_config_processor().get_proxy_config(
              addr, cluster_name, tenant_name, "compression_algorithm", item))) {
        LOG_WDIAG("get compression algorithm config failed", K(addr), K(cluster_name), K(tenant_name), K(ret));
      } else {
        ObString comp_algor(item.str());
        ObCompressionAlgorithm &algor = sm_->compression_algorithm_.algorithm_;
        int64_t &level = sm_->compression_algorithm_.level_;
        if (comp_algor.empty()) {
          algor = OB_COMPRESSION_ALGORITHM_NONE;
          level = 0;
        } else {
          comp_algor = comp_algor.trim();
          ObString algo_str = comp_algor.split_on(':').trim();
          if (OB_COMPRESSION_ALGORITHM_NONE == (algor = get_compression_algorithm_by_name(algo_str))) {
          } else {
            level = atoi(comp_algor.trim().ptr());
            if (level > get_max_compression_level(algor) ||
                level < get_min_compression_level(algor)) {
              level = 0;
            }
          }
        }
        LOG_DEBUG("succ to config compression algorithm", K(algor), K(level));
      }
    }

    if (OB_SUCC(ret) && OB_MYSQL_COM_LOGIN == sm_->trans_state_.trans_info_.sql_cmd_) {
      ObConfigItem item;
      if (OB_FAIL(get_global_config_processor().get_proxy_config(addr, cluster_name, tenant_name, "init_sql", item))) {
        LOG_WDIAG("get init sql config failed", K(addr), K(cluster_name), K(tenant_name), K(ret));
      } else {
        LOG_DEBUG("get init sql config item success", K(item));
        int32_t init_sql_len = static_cast<int32_t>(strlen(item.str()));
        if (init_sql_len > 0) {
          char *buf = NULL;
          if (OB_ISNULL(buf = static_cast<char*>(ob_malloc(init_sql_len)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WDIAG("fail to alloc mem for init sql", K(init_sql_len), K(ret));
          } else {
            MEMCPY(buf, item.str(), init_sql_len);
            session_info.set_init_sql(buf, init_sql_len);
          }
        }
      }
    }
  } else {
    if (OB_SUCC(ret)) {
      ObConfigItem item;
      if (OB_FAIL(get_global_config_processor().get_proxy_config(
              addr, cluster_name, tenant_name, "proxy_route_policy", item))) {
        LOG_WDIAG("get proxy route policy config failed", K(addr), K(cluster_name), K(tenant_name), K(ret));
      } else {
        memset(sm_->proxy_route_policy_, 0, sizeof(sm_->proxy_route_policy_));
        MEMCPY(sm_->proxy_route_policy_, item.str(), strlen(item.str()));
      }
    }

    if (OB_SUCC(ret)) {
      ObConfigItem item;
      if (OB_FAIL(get_global_config_processor().get_proxy_config(
              addr, cluster_name, tenant_name, "proxy_idc_name", item))) {
        LOG_WDIAG("get proxy idc name config failed", K(addr), K(cluster_name), K(tenant_name), K(ret));
      } else {
        memset(sm_->proxy_idc_name_, 0, sizeof(sm_->proxy_idc_name_));
        MEMCPY(sm_->proxy_idc_name_, item.str(), strlen(item.str()));
      }
    }

    if (OB_SUCC(ret)) {
      ObConfigIntItem int_item;
      if (OB_FAIL(get_global_config_processor().get_proxy_config_int_item(
              addr, cluster_name, tenant_name, "obproxy_read_only", int_item))) {
        LOG_WDIAG("get vip obproxy_read_only failed", K(addr), K(cluster_name), K(tenant_name), K(ret));
      } else {
        bool is_read_only = (ReadOnly == int_item.get_value());
        bool is_sys_var_update = (session_info.is_read_only_user() != is_read_only);
        if (is_sys_var_update) {
          ObString tx_read_only("tx_read_only");
          ObString tx_read_only_true;
          if (is_read_only) {
            tx_read_only_true = "1";
          } else {
            tx_read_only_true = "0";
          }
          if (OB_FAIL(session_info.update_sys_variable(tx_read_only, tx_read_only_true))) {
            LOG_WDIAG("replace user variables failed", K(tx_read_only_true), K(ret));
          } else {
            sync_conf_sys_var = true;
          }
        }
        if (OB_SUCC(ret)) {
          session_info.set_is_read_only_user(is_read_only);
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObConfigIntItem int_item;
      if (OB_FAIL(get_global_config_processor().get_proxy_config_int_item(
              addr, cluster_name, tenant_name, "obproxy_read_consistency", int_item))) {
        LOG_WDIAG("get vip obproxy_read_consistency failed", K(addr), K(cluster_name), K(tenant_name), K(ret));
      } else {
        bool is_request_follower = (RequestFollower == int_item.get_value());
        bool is_sys_var_update = (session_info.is_request_follower_user() != is_request_follower);
        if (is_sys_var_update) {
          ObString ob_read_consistency("ob_read_consistency");
          ObString weak;
          if (is_request_follower) {
            weak = "2";
          } else {
            weak = "3";
          }
          if (OB_FAIL(session_info.update_sys_variable(ob_read_consistency, weak))) {
            LOG_WDIAG("replace user variables failed", K(weak), K(ret));
          } else {
            session_info.set_read_consistency_set_flag(true);
            sync_conf_sys_var = true;
          }
        }
        if (OB_SUCC(ret)) {
          session_info.set_is_request_follower_user(is_request_follower);
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObConfigBoolItem bool_item;
      if (OB_FAIL(get_global_config_processor().get_proxy_config_bool_item(
              addr, cluster_name, tenant_name, "enable_read_write_split", bool_item))) {
        LOG_WDIAG("get enable_read_write_split failed", K(addr), K(cluster_name), K(tenant_name), K(ret));
      } else {
        sm_->enable_read_write_split_ = bool_item.get_value();
      }
    }

    if (OB_SUCC(ret)) {
      ObConfigBoolItem bool_item;
      if (OB_FAIL(get_global_config_processor().get_proxy_config_bool_item(
              addr, cluster_name, tenant_name, "enable_transaction_split", bool_item))) {
        LOG_WDIAG("get enable_transaction_split failed", K(addr), K(cluster_name), K(tenant_name), K(ret));
      } else {
        sm_->enable_transaction_split_ = bool_item.get_value();
      }
    }

    if (OB_SUCC(ret)) {
      ObConfigIntItem int_item;
      if (OB_FAIL(get_global_config_processor().get_proxy_config_int_item(
           addr, cluster_name, tenant_name, "obproxy_force_parallel_query_dop", int_item))) {
        LOG_WDIAG("get vip obproxy_force_parallel_query_dop failed", K(addr), K(cluster_name), K(tenant_name), K(ret));
      } else {
        int64_t obproxy_force_parallel_query_dop = int_item.get_value();
        bool is_query_dop_update = (session_info.get_obproxy_force_parallel_query_dop() != obproxy_force_parallel_query_dop);
        if (is_query_dop_update) {
          char value_str[32];
          int64_t len = snprintf(value_str, sizeof(value_str), "%ld", obproxy_force_parallel_query_dop);
          if (OB_UNLIKELY(len <=0 || len >= 32)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("fail to convert value to string", K(obproxy_force_parallel_query_dop), K(len), K(ret));
          } else if (OB_FAIL(session_info.update_sys_variable("_force_parallel_query_dop", value_str))) {
            LOG_WDIAG("replace sys variables _force_parallel_query_dop failed", K(value_str), K(ret));
          } else {
            sync_conf_sys_var = true;
          }
        }

        if (OB_SUCC(ret)) {
           session_info.set_obproxy_force_parallel_query_dop(obproxy_force_parallel_query_dop);
          LOG_DEBUG("replace sys variables _force_parallel_query_dop success", K(obproxy_force_parallel_query_dop), K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObConfigIntItem item;
      bool is_variable_unknown = false;
      ObString max_stale_time("ob_max_read_stale_time");
      if (OB_FAIL(get_global_config_processor().get_proxy_config(
              addr, cluster_name, tenant_name, max_stale_time, item))) {
        LOG_WDIAG("get ob_max_read_stale_time failed", K(addr), K(cluster_name), K(tenant_name), K(ret));
      } else {
        if (session_info.get_ob_max_read_stale_time() != item.get_value()) {
          // update ob_max_read_stale_time
          ObObj stale_time(item.get_value());
          if (OB_FAIL(session_info.update_sys_variable(max_stale_time, stale_time))) {
            // ObServer version <= 4.0.0.0 not exist var ob_max_read_stale_time
            if (ret == OB_ERR_SYS_VARIABLE_UNKNOWN) {
              LOG_DEBUG("unknown sys vartiable ob_max_read_stale_time", K(stale_time), K(ret));
              is_variable_unknown = true;
              ret = OB_SUCCESS;
            } else {
              LOG_WDIAG("replace sys variables ob_max_read_stale_time failed", K(stale_time), K(ret));
            }
          } else {
            sync_conf_sys_var = true;
          }
        }
      }
      if (OB_SUCC(ret) && !is_variable_unknown) {
        session_info.set_ob_max_read_stale_time(item.get_value());
        sm_->enable_read_stale_feedback_ = mysql_config_params_->enable_weak_reroute_;
        LOG_DEBUG("replace sys variables ob_max_read_stale_time success", K(item.get_value()), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ObConfigTimeItem item;
      ObString stale_retry_interval("read_stale_retry_interval");
      if (OB_FAIL(get_global_config_processor().get_proxy_config(
              addr, cluster_name, tenant_name, stale_retry_interval, item))) {
        LOG_WDIAG("get ob_max_read_stale_time failed", K(addr), K(cluster_name), K(tenant_name), K(ret));
      }
      sm_->read_stale_retry_interval_ = item.get_value();
    }

    if (OB_SUCC(ret)) {
      ObConfigItem item;
      if (OB_FAIL(get_global_config_processor().get_proxy_config(
              addr, cluster_name, tenant_name, "binlog_service_ip", item))) {
        LOG_WDIAG("get proxy route policy config failed", K(addr), K(cluster_name), K(tenant_name), K(ret));
      } else {
        memset(sm_->binlog_service_ip_, 0, sizeof(sm_->binlog_service_ip_));
        MEMCPY(sm_->binlog_service_ip_, item.str(), strlen(item.str()));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObConfigItem item;
    if (OB_FAIL(get_global_config_processor().get_proxy_config(
            addr, cluster_name, tenant_name, "target_db_server", item))) {
      LOG_WDIAG("fail to get target_db_server", K(addr), K(cluster_name), K(tenant_name), K(ret));
      // if config empty
    } else if (OB_ISNULL(item.str()) || strlen(item.str()) == 0 ) {
      // target db server not null, reset it
      if (OB_NOT_NULL(sm_->target_db_server_)) {
        op_free(sm_->target_db_server_);
        sm_->target_db_server_ = NULL;
        // target db server is null, do nothing
      } else { /* do nothing */}
      // if config not empty
      // target db server is null, allocate and init one
    } else if (OB_ISNULL(sm_->target_db_server_) && OB_ISNULL(sm_->target_db_server_ = op_alloc(ObTargetDbServer))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc memory to for target db server", K(ret));
    } else if (OB_FAIL(sm_->target_db_server_->init(item.str(), strlen(item.str())))) {
      LOG_WDIAG("fail to load target db server from multi level config", K(ret));
    }
  }

    if (OB_SUCC(ret)) {
      ObConfigItem item;
      if (OB_FAIL(get_global_config_processor().get_proxy_config(addr, cluster_name, tenant_name,
                                                       "proxy_primary_zone_name", item))) {
        LOG_WDIAG("get proxy primary zone name failed", K(addr), K(cluster_name), K(tenant_name), K(ret));
      } else {
        memset(sm_->proxy_primary_zone_name_, 0, sizeof(sm_->proxy_primary_zone_name_));
        MEMCPY(sm_->proxy_primary_zone_name_, item.str(), strlen(item.str()));
      }
    }

  if (OB_SUCC(ret)) {
    if (sync_conf_sys_var) {
      session_info.set_sync_conf_sys_var();
    }
    if (global_version > 0 && !is_auth_request_) {
      sm_->config_version_ = global_version;
    }
  }
  return ret;
}

inline void ObMysqlTransact::ObTransState::get_route_policy(ObProxyRoutePolicyEnum policy,
                                                            ObRoutePolicyEnum& ret_policy)
{
  if (FOLLOWER_FIRST_ENUM == policy) {
    ret_policy = FOLLOWER_FIRST;
  } else if (UNMERGE_FOLLOWER_FIRST_ENUM == policy) {
    ret_policy = UNMERGE_FOLLOWER_FIRST;
  } else if (FOLLOWER_ONLY_ENUM == policy) {
    ret_policy = FOLLOWER_ONLY;
  }
}
inline ObRoutePolicyEnum ObMysqlTransact::ObTransState::get_route_policy(ObMysqlClientSession &cs,
                                                                         const bool need_use_dup_replica,
                                                                         ObDiagnosisRoutePolicy *diagnosis_route_policy)
{
  ObRoutePolicyEnum ret_policy = READONLY_ZONE_FIRST;
  ObRoutePolicyEnum session_route_policy = READONLY_ZONE_FIRST;
  ObRoutePolicyEnum proxy_route_policy = MAX_ROUTE_POLICY_COUNT;
  bool readonly_zone_exist = cs.dummy_ldc_.is_readonly_zone_exist();
  bool request_support_readonly_zone = is_request_readonly_zone_support(cs.get_session_info());
  ObConsistencyLevel session_consistency_level = static_cast<ObConsistencyLevel>(cs.get_session_info().get_read_consistency());
  ObConsistencyLevel trans_consistency_level = get_trans_consistency_level(cs.get_session_info());
  switch (cs.get_session_info().get_route_policy()) {
    case 0:
      session_route_policy = MERGE_IDC_ORDER;
      break;
    case 1:
      session_route_policy = READONLY_ZONE_FIRST;
      break;
    case 2:
      session_route_policy = ONLY_READONLY_ZONE;
      break;
    case 3:
      session_route_policy = UNMERGE_ZONE_FIRST;
      break;
    default:
      PROXY_TXN_LOG(WDIAG, "unknown route policy, use default policy",
                    "ob_route_policy", cs.get_session_info().get_route_policy(),
                    "session_route_policy", get_route_policy_enum_string(session_route_policy));
  }
  if (need_use_dup_replica) {
    //if dup_replica read, use DUP_REPLICA_FIRST, no need care about zone type
    ret_policy = DUP_REPLICA_FIRST;
  } else if (readonly_zone_exist) {
    if (common::WEAK == trans_consistency_level) {
      //if wead read, use session_route_policy
      ret_policy = session_route_policy;
    } else if (request_support_readonly_zone) {
      if (cs.get_session_info().is_read_consistency_set()) {
        if (common::WEAK == session_consistency_level) {
          ret_policy = session_route_policy;
        } else {//strong
          ret_policy = ONLY_READWRITE_ZONE;
        }
      } else {
        ret_policy = MERGE_IDC_ORDER;
      }
    } else {
      //if readonly zone not support, only use readwrite zone
      ret_policy = ONLY_READWRITE_ZONE;
    }
  } else {
    //if no readonly zone exist, use orig policy
    ret_policy = MERGE_IDC_ORDER;
    if (common::WEAK == trans_consistency_level) {
      if (is_valid_proxy_route_policy(cs.get_session_info().get_proxy_route_policy())
        && cs.get_session_info().is_proxy_route_policy_set()) {
        get_route_policy(cs.get_session_info().get_proxy_route_policy(), proxy_route_policy);
      } else {
        ObProxyRoutePolicyEnum policy = get_proxy_route_policy(sm_->proxy_route_policy_);
        PROXY_CS_LOG(DEBUG, "succ to global variable proxy_route_policy",
                 "policy", get_proxy_route_policy_enum_string(policy));
        get_route_policy(policy, proxy_route_policy);
      }
      if (proxy_route_policy != MAX_ROUTE_POLICY_COUNT) {
        ret_policy = proxy_route_policy;
      }
    }
  }
  PROXY_TXN_LOG(DEBUG, "succ to get route_policy",
                "session_route_policy", get_route_policy_enum_string(session_route_policy),
                "ret_policy", get_route_policy_enum_string(ret_policy));

  if (OB_NOT_NULL(diagnosis_route_policy)) {
    diagnosis_route_policy->proxy_route_policy_ = proxy_route_policy;
    diagnosis_route_policy->session_route_policy_ = session_route_policy;
    diagnosis_route_policy->route_policy_ = ret_policy;
    diagnosis_route_policy->session_consistency_level_ = session_consistency_level;
    diagnosis_route_policy->trans_consistency_level_ = trans_consistency_level;
    diagnosis_route_policy->need_use_dup_replica_ = need_use_dup_replica;
    diagnosis_route_policy->readonly_zone_exsits_ = readonly_zone_exist;
    diagnosis_route_policy->request_support_readonly_zone_ = request_support_readonly_zone;
  }
  return ret_policy;
}

const char *ObMysqlTransact::get_action_name(ObMysqlTransact::ObStateMachineActionType e)
{
  const char *ret = NULL;
  switch (e) {
    case ObMysqlTransact::SM_ACTION_UNDEFINED:
      ret = "SM_ACTION_UNDEFINED";
      break;

    case ObMysqlTransact::SM_ACTION_SERVER_ADDR_LOOKUP:
      ret = "SM_ACTION_SERVER_ADDR_LOOKUP";
      break;

    case ObMysqlTransact::SM_ACTION_PARTITION_LOCATION_LOOKUP:
      ret = "SM_ACTION_PARTITION_OBSERVER_LOOKUP";
      break;

    case ObMysqlTransact::SM_ACTION_CONGESTION_CONTROL_LOOKUP:
      ret = "SM_ACTION_CONGESTION_CONTROL_LOOKUP";
      break;

    case ObMysqlTransact::SM_ACTION_OBSERVER_OPEN:
      ret = "SM_ACTION_OBSERVER_OPEN";
      break;

    case ObMysqlTransact::SM_ACTION_INTERNAL_NOOP:
      ret = "SM_ACTION_INTERNAL_NOOP";
      break;

    case ObMysqlTransact::SM_ACTION_INTERNAL_REQUEST:
      ret = "SM_ACTION_INTERNAL_REQUEST";
      break;

    case ObMysqlTransact::SM_ACTION_SEND_ERROR_NOOP:
      ret = "SM_ACTION_SEND_ERROR_NOOP";
      break;

    case ObMysqlTransact::SM_ACTION_SERVER_READ:
      ret = "SM_ACTION_SERVER_READ";
      break;

    case ObMysqlTransact::SM_ACTION_API_READ_REQUEST:
      ret = "SM_ACTION_API_READ_REQUEST";
      break;

    case ObMysqlTransact::SM_ACTION_API_OBSERVER_PL:
      ret = "SM_ACTION_API_OBSERVER_PL";
      break;

    case ObMysqlTransact::SM_ACTION_API_SEND_REQUEST:
      ret = "SM_ACTION_API_SEND_REQUEST";
      break;

    case ObMysqlTransact::SM_ACTION_API_READ_RESPONSE:
      ret = "SM_ACTION_API_READ_RESPONSE";
      break;

    case ObMysqlTransact::SM_ACTION_API_SEND_RESPONSE:
      ret = "SM_ACTION_API_SEND_RESPONSE";
      break;

    case ObMysqlTransact::SM_ACTION_TRANSFORM_READ:
      ret = "SM_ACTION_TRANSFORM_READ";
      break;

    case ObMysqlTransact::SM_ACTION_API_SM_START:
      ret = "SM_ACTION_API_SM_START";
      break;

    case ObMysqlTransact::SM_ACTION_API_CMD_COMPLETE:
      ret = "SM_ACTION_API_CMD_COMPLETE";
      break;

    case ObMysqlTransact::SM_ACTION_API_SM_SHUTDOWN:
      ret = "SM_ACTION_API_SM_SHUTDOWN";
      break;

    case ObMysqlTransact::SM_ACTION_BINLOG_LOCATION_LOOKUP:
      ret = "SM_ACTION_BINLOG_LOCATION_LOOKUP";
      break;

    default:
      ret = "unknown state name";
      break;
  }

  return ret;
}

const char *ObMysqlTransact::get_server_resp_error_name(ObMysqlTransact::ObServerRespErrorType type)
{
  const char *ret = NULL;
  switch (type) {
    case ObMysqlTransact::MIN_RESP_ERROR:
      ret = "MIN_RESP_ERR";
      break;

    case ObMysqlTransact::LOGIN_SERVER_INIT_ERROR:
      ret = "LOGIN_SERVER_INIT_ERROR";
      break;

    case ObMysqlTransact::LOGIN_SERVER_STOPPING_ERROR:
      ret = "LOGIN_SERVER_STOPPING_ERROR";
      break;

    case ObMysqlTransact::LOGIN_TENANT_NOT_IN_SERVER_ERROR:
      ret = "LOGIN_TENANT_NOT_IN_SERVER_ERROR";
      break;

    case ObMysqlTransact::LOGIN_SESSION_ENTRY_EXIST_ERROR:
      ret = "LOGIN_SESSION_ENTRY_EXIST_ERROR";
      break;

    case ObMysqlTransact::LOGIN_CONNECT_ERROR:
      ret = "LOGIN_CONNECT_ERROR";
      break;

    case ObMysqlTransact::HANDSHAKE_COMMON_ERROR:
      ret = "HANDSHAKE_COMMON_ERROR";
      break;

    case ObMysqlTransact::LOGIN_CLUSTER_NOT_MATCH_ERROR:
      ret = "LOGIN_CLUSTER_NOT_MATCH_ERROR";
      break;

    case ObMysqlTransact::SAVED_LOGIN_COMMON_ERROR:
      ret = "SAVED_LOGIN_COMMON_ERROR";
      break;

    case ObMysqlTransact::RESET_SESSION_VARS_COMMON_ERROR:
      ret = "RESET_SESSION_VARS_COMMON_ERROR";
      break;

    case ObMysqlTransact::START_TRANS_COMMON_ERROR:
      ret = "START_TRANS_COMMON_ERROR";
      break;

    case ObMysqlTransact::START_XA_START_ERROR:
      ret = "START_XA_START_ERROR";
      break;

    case ObMysqlTransact::SYNC_DATABASE_COMMON_ERROR:
      ret = "SYNC_DATABASE_COMMON_ERROR";
      break;

    case ObMysqlTransact::SYNC_PREPARE_COMMON_ERROR:
      ret = "SYNC_PREPARE_COMMON_ERROR";
      break;

    case ObMysqlTransact::SYNC_TEXT_PS_PREPARE_COMMON_ERROR:
      ret = "SYNC_TEXT_PS_PREPARE_COMMON_ERROR";
      break;

    case ObMysqlTransact::ORA_FATAL_ERROR:
      ret = "ORA_FATAL_ERROR";
      break;

    case ObMysqlTransact::REQUEST_TENANT_NOT_IN_SERVER_ERROR:
      ret = "REQUEST_TENANT_NOT_IN_SERVER_ERROR";
      break;

    case ObMysqlTransact::REQUEST_SERVER_INIT_ERROR:
      ret = "REQUEST_SERVER_INIT_ERROR";
      break;

    case ObMysqlTransact::REQUEST_SERVER_STOPPING_ERROR:
      ret = "REQUEST_SERVER_STOPPING_ERROR";
      break;

    case ObMysqlTransact::REQUEST_CONNECT_ERROR:
      ret = "REQUEST_CONNECT_ERROR";
      break;

    case ObMysqlTransact::REQUEST_READ_ONLY_ERROR:
      ret = "REQUEST_READ_ONLY_ERROR";
      break;

    case ObMysqlTransact::REQUEST_REROUTE_ERROR:
      ret = "REQUEST_REROUTE_ERROR";
      break;

    case ObMysqlTransact::STANDBY_WEAK_READONLY_ERROR:
      ret = "SYANDBY_WEAK_READONLY_ERROR";
      break;

    case ObMysqlTransact::TRANS_FREE_ROUTE_NOT_SUPPORTED_ERROR:
      ret = "TRANS_FREE_ROUTE_NOT_SUPPORTED_ERROR";
      break;

    case ObMysqlTransact::SEND_INIT_SQL_ERROR:
      ret = "SEND_INIT_SQL_ERROR";
      break;

    case ObMysqlTransact::CLIENT_SESSION_KILLED_ERROR:
      ret = "CLIENT_SESSION_KILLED_ERROR";
      break;

    case ObMysqlTransact::MAX_RESP_ERROR:
      ret = "MAX_RESP_ERR";
      break;

    default:
      ret = "unknown resp error name";
      break;
  }
  return ret;
}

const char *ObMysqlTransact::get_server_state_name(ObMysqlTransact::ObServerStateType state)
{
  const char *ret = NULL;
  switch (state) {
    case ObMysqlTransact::STATE_UNDEFINED:
      ret = "STATE_UNDEFINED";
      break;

    case ObMysqlTransact::ACTIVE_TIMEOUT:
      ret = "ACTIVE_TIMEOUT";
      break;

    case ObMysqlTransact::RESPONSE_ERROR:
      ret = "RESPONSE_ERROR";
      break;

    case ObMysqlTransact::CONNECTION_ALIVE:
      ret = "CONNECTION_ALIVE";
      break;

    case ObMysqlTransact::CONNECTION_CLOSED:
      ret = "CONNECTION_CLOSED";
      break;

    case ObMysqlTransact::CONNECTION_ERROR:
      ret = "CONNECTION_ERROR";
      break;

    case ObMysqlTransact::CONNECT_ERROR:
      ret = "CONNECT_ERROR";
      break;

    case ObMysqlTransact::INACTIVE_TIMEOUT:
      ret = "INACTIVE_TIMEOUT";
      break;

    case ObMysqlTransact::ANALYZE_ERROR:
      ret = "ANALYZE_ERROR";
      break;

    case ObMysqlTransact::CMD_COMPLETE:
      ret = "CMD_COMPLETE";
      break;

    case ObMysqlTransact::TRANSACTION_COMPLETE:
      ret = "TRANSACTION_COMPLETE";
      break;

    case ObMysqlTransact::DEAD_CONGESTED:
      ret = "DEAD_CONGESTED";
      break;

    case ObMysqlTransact::ALIVE_CONGESTED:
      ret = "ALIVE_CONGESTED";
      break;

    case ObMysqlTransact::DETECT_CONGESTED:
      ret = "DETECT_CONGESTED";
      break;

    case ObMysqlTransact::INTERNAL_ERROR:
      ret = "INTERNAL_ERROR";
      break;

    default:
      ret = "unknown state name";
      break;
  }

  return ret;
}

const char *ObMysqlTransact::get_send_action_name(
    ObMysqlTransact::ObServerSendActionType type)
{
  const char *ret = NULL;
  switch (type) {
    case ObMysqlTransact::SERVER_SEND_HANDSHAKE:
      ret = "SERVER_SEND_HANDSHAKE";
      break;

    case ObMysqlTransact::SERVER_SEND_SSL_REQUEST:
      ret = "SERVER_SEND_SSL_REQUEST";
      break;

    case ObMysqlTransact::SERVER_SEND_LOGIN:
      ret = "SERVER_SEND_LOGIN";
      break;

    case ObMysqlTransact::SERVER_SEND_INIT_SQL:
      ret = "SERVER_SEND_INIT_SQL";
      break;

    case ObMysqlTransact::SERVER_SEND_SAVED_LOGIN:
      ret = "SERVER_SEND_SAVED_LOGIN";
      break;

    case ObMysqlTransact::SERVER_SEND_ALL_SESSION_VARS:
      ret = "SERVER_SEND_ALL_SESSION_VARS";
      break;

    case ObMysqlTransact::SERVER_SEND_USE_DATABASE:
      ret = "SERVER_SEND_USE_DATABASE";
      break;

    case ObMysqlTransact::SERVER_SEND_SESSION_VARS:
      ret = "SERVER_SEND_SESSION_VARS";
      break;

    case ObMysqlTransact::SERVER_SEND_SESSION_USER_VARS:
      ret = "SERVER_SEND_SESSION_USER_VARS";
      break;

    case ObMysqlTransact::SERVER_SEND_START_TRANS:
      ret = "SERVER_SEND_START_TRANS";
      break;

    case ObMysqlTransact::SERVER_SEND_XA_START:
      ret = "SERVER_SEND_XA_START";
      break;

    case ObMysqlTransact::SERVER_SEND_REQUEST:
      ret = "SERVER_SEND_REQUEST";
      break;

    case ObMysqlTransact::SERVER_SEND_NONE:
      ret = "SERVER_SEND_NONE";
      break;

    case ObMysqlTransact::SERVER_SEND_PREPARE:
      ret = "SERVER_SEND_PREPARE";
      break;

    case ObMysqlTransact::SERVER_SEND_TEXT_PS_PREPARE:
      ret = "SERVER_SEND_TEXT_PS_PREPARE";
      break;

    default:
      ret = "unknown server send action name";
      break;
  }
  return ret;
}

inline ObHRTime ObMysqlTransact::get_based_hrtime(ObTransState &s)
{
  return (NULL == s.sm_) ? (0) : (s.sm_->get_based_hrtime());
}

bool ObMysqlTransact::is_internal_request(ObTransState &s)
{
  // now internal request contains these types:
  // 1. OB_MYSQL_COM_QUERY, obproxy execute internal cmd
  // 2. xx@proxysys, obproxy will treat all request to internal cmd
  // 3. OB_MYSQL_COM_HANDSHAKE request, obproxy build the handshake packet and return to client directly
  // 4. OB_MYSQL_COM_PING request, obproxy build OK packet and return to client directly
  // 5. the first sql statement to start one transaction, like 'begin' or 'start transaction',
  //    obproxy will build an ok packet and return client directly
  // 6. parse result indicate internal request
  // 7. if it's a bad route request, like the first statement after begin(or start transaction)
  //    don't has valid table name;
  // 8. not supported mysql cmd
  // 9. public cloud read only vip
  return (s.trans_info_.client_request_.is_internal_cmd()
          || s.is_proxysys_tenant_
          || (obmysql::OB_MYSQL_COM_HANDSHAKE == s.trans_info_.sql_cmd_ && !s.mysql_config_params_->is_mysql_routing_mode())
          || can_direct_ok_for_login(s)
          || obmysql::OB_MYSQL_COM_PING == s.trans_info_.sql_cmd_
          || (s.trans_info_.client_request_.is_sharding_user()
              && (s.trans_info_.client_request_.get_parse_result().is_shard_special_cmd()
                  || obmysql::OB_MYSQL_COM_INIT_DB == s.trans_info_.sql_cmd_
                  || ((s.trans_info_.client_request_.get_parse_result().is_show_tables_stmt()
                       || s.trans_info_.client_request_.get_parse_result().is_show_full_tables_stmt()
                       || s.trans_info_.client_request_.get_parse_result().is_show_table_status_stmt()
                       || s.trans_info_.client_request_.get_parse_result().is_show_create_table_stmt())
                      && !is_single_shard_db_table(s))))
          || (s.is_trans_first_request_
              && !s.is_hold_xa_start_
              && s.trans_info_.client_request_.get_parse_result().need_hold_start_trans())
          || (s.is_trans_first_request_
              && !s.is_hold_start_trans_
              && !s.is_hold_xa_start_
              && s.trans_info_.client_request_.get_parse_result().need_hold_xa_start()
              && get_global_proxy_config().enable_xa_route)
          || s.trans_info_.client_request_.get_parse_result().is_internal_request()
          || is_bad_route_request(s))
          || !is_supported_mysql_cmd(s.trans_info_.sql_cmd_)
          || (get_client_session_info(s).is_read_only_user()
              && s.trans_info_.client_request_.get_parse_result().is_set_tx_read_only())
          || (get_client_session_info(s).is_request_follower_user()
              &&s.trans_info_.client_request_.get_parse_result().is_set_ob_read_consistency());
}

int64_t ObMysqlTransact::get_max_connect_attempts(ObTransState &s)
{
  int64_t target_db_server_num = 0;
  if (s.use_cmnt_target_db_server_) {
    target_db_server_num = s.trans_info_.client_request_.get_parse_result().get_target_db_server()->num();
  } else if (s.use_conf_target_db_server_) {
    target_db_server_num = s.sm_->target_db_server_->num();
  }
  return std::max(target_db_server_num, std::max(s.mysql_config_params_->connect_observer_max_retries_,
                  get_max_connect_attempts_from_replica(s.pll_info_.replica_size())));
}

bool ObMysqlTransact::is_binlog_request(const ObTransState &s)
{
  bool bret = false;
  if ((obmysql::OB_MYSQL_COM_REGISTER_SLAVE == s.trans_info_.client_request_.get_packet_meta().cmd_
      || obmysql::OB_MYSQL_COM_BINLOG_DUMP == s.trans_info_.client_request_.get_packet_meta().cmd_
      || obmysql::OB_MYSQL_COM_BINLOG_DUMP_GTID == s.trans_info_.client_request_.get_packet_meta().cmd_
      || s.trans_info_.client_request_.get_parse_result().is_binlog_related())
      && 0 != strlen(s.sm_->binlog_service_ip_)) {
    bret = true;
  }

  return bret;
}

void ObMysqlTransact::handle_binlog_request(ObTransState &s)
{
  if (is_in_trans(s)) {
    ObMysqlClientSession *client_session = s.sm_->get_client_session();
    ObMysqlServerSession *last_session = client_session->get_server_session();
    client_session->attach_server_session(NULL);
    last_session->do_io_read(client_session, 0, NULL);
    client_session->set_last_bound_server_session(last_session);
    client_session->set_need_return_last_bound_ss(true);
  }
  ObClientSessionInfo &cs_info = get_client_session_info(s);
  ObString cluster_name = cs_info.get_priv_info().cluster_name_;
  ObString tenant_name = cs_info.get_priv_info().tenant_name_;

  ObTableEntryName &te_name = s.pll_info_.te_name_;
  te_name.cluster_name_ = cluster_name;
  te_name.tenant_name_ = tenant_name;
  // mock database name
  te_name.database_name_ = OB_SYS_DATABASE_NAME;
  te_name.table_name_ = OB_ALL_BINLOG_DUMMY_TNAME;
  LOG_DEBUG("handle binlog request", K(te_name));
  TRANSACT_RETURN(SM_ACTION_BINLOG_LOCATION_LOOKUP, handle_bl_lookup);
}

void ObMysqlTransact::handle_bl_lookup(ObTransState &s)
{
  int ret = OB_SUCCESS;
  ++s.pll_info_.pl_attempts_;

  if (OB_UNLIKELY(!s.pll_info_.lookup_success_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("[ObMysqlTransact::handle_bl_lookup] "
             "fail to lookup binlog location, will disconnect", K(ret));
  } else if (s.trans_info_.client_request_.get_parse_result().is_show_binlog_server_for_tenant_stmt()
             && s.server_info_.addr_.is_valid()) {
    // do nothing
  } else {
    LOG_DEBUG("ObMysqlTransact::handle_bl_lookup] binlog location lookup successful",
        "pl_attempts", s.pll_info_.pl_attempts_);
    const ObProxyPartitionLocation *pl = s.pll_info_.route_.table_entry_->get_first_pl();
    if (NULL != pl && pl->replica_count() > 0) {
      s.server_info_.set_addr(ops_ip_sa_cast(pl->get_replica(0)->server_.get_sockaddr()));
      s.pll_info_.route_.cur_chosen_server_.replica_ = pl->get_replica(0);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("not found binlog server replica", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    LOG_DEBUG("[ObMysqlTransact::handle_bl_lookup] chosen server, and begin to congestion lookup",
                "addr", s.server_info_.addr_,
                "attempts", s.current_.attempts_,
                "sm_id", s.sm_->sm_id_);
    if (OB_UNLIKELY(get_global_performance_params().enable_trace_)) {
      s.sm_->milestones_.bl_process_end_ = get_based_hrtime(s);
      s.sm_->cmd_time_stats_.bl_process_time_ +=
        milestone_diff(s.sm_->milestones_.bl_process_begin_, s.sm_->milestones_.bl_process_end_);
    }
    // 目前binlog没有高可用特性，路由后直接走转发，不走容灾管理流程
    TRANSACT_RETURN(SM_ACTION_OBSERVER_OPEN, ObMysqlTransact::handle_response);
  }

  if (OB_FAIL(ret)) {
    s.inner_errcode_ = ret;
    // disconnect
    TRANSACT_RETURN_WITH_MSG(SM_ACTION_SEND_ERROR_NOOP, NULL);
  }
}

void ObMysqlTransact::build_read_stale_param(const ObTransState &s, ObReadStaleParam &param)
{
  ObVipAddr vip_addr = s.sm_->get_client_session()->get_ct_info().vip_tenant_.vip_addr_;
  param.vip_addr_ = vip_addr;
  param.cluster_name_ = s.sm_->get_client_session()->get_session_info().get_priv_info().cluster_name_;
  param.tenant_name_ = s.sm_->get_client_session()->get_session_info().get_priv_info().tenant_name_;
  param.enable_read_stale_feedback_ = s.sm_->enable_read_stale_feedback_;
  param.retry_interval_ = s.sm_->read_stale_retry_interval_;
  param.table_id_ = OB_INVALID_INDEX;
  param.partition_id_ = OB_INVALID_INDEX;
  param.server_addr_ = s.server_info_.addr_;
  if (s.pll_info_.route_.is_non_partition_table() ) {
    if (s.pll_info_.route_.table_entry_ != NULL) {
      param.table_id_ = s.pll_info_.route_.table_entry_->get_table_id();
    } else {
      // invalid table entry
      param.enable_read_stale_feedback_ = false;
    }
  } else {
    if (s.pll_info_.route_.part_entry_ != NULL) {
      param.table_id_ = s.pll_info_.route_.part_entry_->get_table_id();
      param.partition_id_ = s.pll_info_.route_.part_entry_->get_partition_id();
    } else {
      // invalid partition entry
      param.enable_read_stale_feedback_ = false;
    }
  }
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
