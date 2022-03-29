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

void ObMysqlTransact::handle_error_jump(ObTransState &s)
{
  LOG_WARN("[ObMysqlTransact::handle_error_jump]");

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
  TRANSACT_RETURN(SM_ACTION_SEND_ERROR_NOOP, NULL);
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
    if (trans_info_.client_request_.get_parse_result().is_select_stmt()) {
      const ObConsistencyLevel sql_hint = trans_info_.client_request_.get_parse_result().get_hint_consistency_level();
      const ObConsistencyLevel sys_var = static_cast<ObConsistencyLevel>(cs_info.get_read_consistency());
      if (common::STRONG == sql_hint || common::WEAK == sql_hint) {
        ret_level = sql_hint;
      } else {
        if (common::STRONG == sys_var || common::WEAK == sys_var) {
          ret_level = sys_var;
        } else {
          PROXY_LOG(DEBUG, "unsupport ob_read_consistency vars, maybe proxy is old, use strong read "
                    "instead", "sys_var", get_consistency_level_str(sys_var));
        }
      }
      if (common::WEAK == ret_level) {
        ObString sql;
        if (obmysql::OB_MYSQL_COM_STMT_EXECUTE == trans_info_.client_request_.get_packet_meta().cmd_) {
          cs_info.get_ps_sql(sql);
        } else {
          sql = trans_info_.client_request_.get_sql();
        }
        if (OB_UNLIKELY(!cs_info.is_read_weak_supported())) {
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
    LOG_WARN("[modify_request] cluster resource is NULL, will disconnect");
    TRANSACT_RETURN(SM_ACTION_SEND_ERROR_NOOP, NULL);
  } else {
    TRANSACT_RETURN(SM_ACTION_API_READ_REQUEST, ObMysqlTransact::handle_request);
    if (!s.trans_info_.client_request_.is_large_request()) {
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
             && NULL != last_session && OB_LIKELY(!s.mysql_config_params_->is_random_routing_mode())) {
    const int32_t ip = ops_ip4_addr_host_order(last_session->get_netvc()->get_remote_addr());
    const int32_t port = static_cast<int32_t>(ops_ip_port_host_order(last_session->get_netvc()->get_remote_addr()));
    ObAddr last_addr;
    last_addr.set_ipv4_addr(ip, port);
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
  if (!(obmysql::OB_MYSQL_COM_LOGIN == s.trans_info_.sql_cmd_ && s.is_auth_request_ && s.sm_->client_session_->is_session_pool_client())) {
    // should only session_pool_client and LOGIN for auth
  } else if (cs_info.is_sharding_user()) {
    bret = true;
  } else if (!s.sm_->client_session_->is_proxy_mysql_client_ && get_global_proxy_config().enable_no_sharding_skip_real_conn) {
    // if have same password in connection pool mode, can return ok directly.
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
        LOG_WARN("shard conn is null", K(server_session_list_pool->schema_key_));
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
      LOG_WARN("invalid client_session argument");
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
            LOG_WARN("fail to get_sequence_param", K(ret));
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
    LOG_WARN("fail to get_logic_tenant_name", K(ret));
  } else if (OB_FAIL(cs_info.get_logic_database_name(logic_database_name))) {
    LOG_WARN("fail to get_logic_database_name, maybe no database selected", K(ret));
  } else if (OB_ISNULL(db_info = get_global_dbconfig_cache().get_exist_db_info(logic_tenant_name, logic_database_name))) {
    LOG_WARN("unknown logic db info", K(logic_database_name));
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
    LOG_WARN("fail to get ip", "physic_addr", shard_conn->physic_addr_.config_string_, K(ret));
  } else if (OB_FAIL(get_int_value(shard_conn->physic_port_.config_string_, port))) {
    LOG_WARN("fail to get port", "physic_port", shard_conn->physic_port_.config_string_, K(ret));
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
  bool need_pl_lookup = ObMysqlTransact::need_pl_lookup(s);
  ObMysqlServerSession *last_session = s.sm_->client_session_->get_server_session();
  if (need_pl_lookup) {
    ObMysqlServerSession *svr_session = NULL;
    ObShardConnector *shard_conn = s.sm_->client_session_->get_session_info().get_shard_connector();
    if (OB_ISNULL(shard_conn)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("shard conn is NULL", K(ret));
    } else if (OB_NOT_NULL(last_session)
               && last_session->get_session_info().get_shard_connector()->shard_name_ == shard_conn->shard_name_) {
      s.server_info_.set_addr(last_session->get_netvc()->get_remote_addr());
      s.pll_info_.lookup_success_ = true;
      LOG_DEBUG("[ObMysqlTransact::handle mysql request] direct use last server session");
    } else if (shard_conn->is_physic_ip_) {
      ret = set_server_ip_by_shard_conn(s, shard_conn);
    } else if (s.sm_->client_session_->is_session_pool_client()) {
      if (s.sm_->client_session_->is_proxy_mysql_client_) {
        ret = set_server_ip_by_shard_conn(s, shard_conn);
      } else {
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
          PROXY_SS_LOG(WARN, "fail to release server session to new session manager, it will be closed", K(ret));
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
      LOG_WARN("[ObMysqlTransact::handle request] last session is NULL, we have to disconnect");
    }
  }

  if (OB_FAIL(ret)) {
    s.inner_errcode_ = ret;
    TRANSACT_RETURN(SM_ACTION_SEND_ERROR_NOOP, NULL);
  } else {
    TRANSACT_RETURN(SM_ACTION_OBSERVER_OPEN, ObMysqlTransact::handle_response);
  }
}

void ObMysqlTransact::handle_ps_close(ObTransState &s)
{
  int ret = OB_SUCCESS;
  ObMysqlClientSession *client_session = s.sm_->get_client_session();

  // At the end of each request, server_entry_ and server_session_ will be placed,
  // including the internal jump of the close command multiple times.
  if (!client_session->is_first_handle_close_request()) {
    s.sm_->release_server_session();
    // If it is not the first time to come in, it will also be judged according to the transaction status of the first time.
    s.need_pl_lookup_ = s.need_pl_lookup_ && !client_session->is_in_trans_for_close_request();
  } else {
    // The first time you come in, record the previous transaction status
    client_session->set_in_trans_for_close_request(is_in_trans(s));
  }

  ObMysqlServerSession *last_session = client_session->get_server_session();
  ObMysqlServerSession *last_bound_session = client_session->get_last_bound_server_session();

  /* If no routing is required, and it is the first time to come in, the following situations need to be disconnected:
   *  1. last_session does not exist.
   *  2. last_bound_session is not null.
   */
  if (!s.need_pl_lookup_ && client_session->is_first_handle_close_request()
      && (NULL == last_session || NULL != last_bound_session)) {
    LOG_ERROR("[ObMysqlTransact::handle request] something is wrong, we have to disconnect",
        "is_first_handle_close_request_", client_session->is_first_handle_close_request(),
        KP(last_session), KP(last_bound_session));
    TRANSACT_RETURN(SM_ACTION_SEND_ERROR_NOOP, NULL);
  } else {
    uint32_t client_ps_id = client_session->get_session_info().get_client_ps_id();
    bool is_need_send_close_cmd = false;
    bool is_need_send_to_bound_ss = false;
    ObIpEndpoint addr;

    // If greater than 1 << 31L, it means cursor_id; otherwise, it is ps_id
    if (client_ps_id >= (CURSOR_ID_START)) {
      ObCursorIdAddr *cursor_id_addr = client_session->get_session_info().get_cursor_id_addr(client_ps_id);
      if (NULL != cursor_id_addr) {
        is_need_send_close_cmd = true;
        addr = cursor_id_addr->get_addr();
        // If no routing is required, and it is the first time, the bound_ss is sent first.
        // If the server to be sent is bound_ss, the flag is set, and no migration is required later.
        if (!s.need_pl_lookup_
            && client_session->is_first_handle_close_request()
            && addr == ObIpEndpoint(last_session->get_netvc()->get_remote_addr())) {
          is_need_send_to_bound_ss = true;
        }
      }
    } else {
      ObPsIdAddrs *ps_id_addrs = client_session->get_session_info().get_ps_id_addrs(client_ps_id);
      if (NULL != ps_id_addrs && 0 != ps_id_addrs->get_addrs().size()) {
        is_need_send_close_cmd = true;
        /* If no routing is required, and it is the first time to come in
         * Then check if there is any need to send to bound_ss, if so, send it first,
         * otherwise migrate the connection, and then migrate back
         */
        if (!s.need_pl_lookup_
            && client_session->is_first_handle_close_request()) {
          ObPsIdAddrs::ADDR_HASH_SET::iterator iter = ps_id_addrs->get_addrs().begin();
          ObPsIdAddrs::ADDR_HASH_SET::iterator iter_end = ps_id_addrs->get_addrs().end();

          for (; iter != iter_end; iter++) {
            if (iter->first == ObIpEndpoint(last_session->get_netvc()->get_remote_addr())) {
              is_need_send_to_bound_ss = true;
              addr = iter->first;
              break;
            }
          }
        }

        if (!is_need_send_to_bound_ss) {
          addr = ps_id_addrs->get_addrs().begin()->first;
        }
      }
    }

    if (is_need_send_close_cmd) {
      // If no routing is required, and there is no last server session in the server to be sent, migrate
      ObMysqlServerSession *last_bound_session = client_session->get_last_bound_server_session();
      if (!s.need_pl_lookup_
          && !is_need_send_to_bound_ss
          && NULL == last_bound_session) {
        client_session->attach_server_session(NULL);
        last_session->do_io_read(client_session, 0, NULL);
        client_session->set_last_bound_server_session(last_session);
        client_session->set_need_return_last_bound_ss(true);
      }

      if (OB_SUCC(ret)) {
        client_session->set_first_handle_close_request(false);
        s.server_info_.set_addr(addr);
        s.pll_info_.lookup_success_ = true;
        start_access_control(s);
      }
    } else {
      // This indicates that session migration has occurred and needs to be migrated back
      if (client_session->is_need_return_last_bound_ss()) {
        if (NULL != last_bound_session) {
          if (OB_FAIL(return_last_bound_server_session(client_session))) {
            LOG_WARN("fail to return last bond server session", K(ret));
          }
        } else {
          LOG_WARN("[ObMysqlTransact::handle request] last bound session is NULL, we have to disconnect");
          ret = OB_ERR_UNEXPECTED;
        }
      }

      if (OB_SUCC(ret)) {
        if (!client_session->is_in_trans_for_close_request()) {
          s.sm_->trans_state_.current_.state_ = ObMysqlTransact::TRANSACTION_COMPLETE;
        } else {
          s.sm_->trans_state_.current_.state_ = ObMysqlTransact::CMD_COMPLETE;
        }

        // If the backend has not been executed or all closed, transfer the internal request and clear the relevant cache on the client session
        TRANSACT_RETURN(SM_ACTION_INTERNAL_REQUEST, handle_internal_request);
      } else {
        TRANSACT_RETURN(SM_ACTION_SEND_ERROR_NOOP, NULL);
      }
    }
  }
}

void ObMysqlTransact::handle_oceanbase_request(ObTransState &s)
{
  int ret = OB_SUCCESS;
  ObClientSessionInfo &cs_info = get_client_session_info(s);
  if (OB_LIKELY(cs_info.is_allow_use_last_session())) {
    s.need_pl_lookup_ = need_pl_lookup(s);
  } else {
    s.need_pl_lookup_ = true;
  }

  obmysql::ObMySQLCmd cmd = s.trans_info_.sql_cmd_;

  if (OB_UNLIKELY(cs_info.is_sharding_user())
      && OB_FAIL(ObProxyShardUtils::update_sys_read_consistency_if_need(cs_info))) {
    LOG_WARN("fail to update_sys_read_consistency_if_need", K(ret));
    TRANSACT_RETURN(SM_ACTION_SEND_ERROR_NOOP, NULL); // disconnect
  } else if (OB_UNLIKELY(need_server_session_lookup(s))) {
    TRANSACT_RETURN(SM_ACTION_SERVER_ADDR_LOOKUP, handle_server_addr_lookup);
  } else if (OB_UNLIKELY(obmysql::OB_MYSQL_COM_STMT_CLOSE == cmd)) {
    handle_ps_close(s);
  } else if (OB_LIKELY(s.need_pl_lookup_)) {
    // if need pl lookup, we should extract pl info first
    if (OB_FAIL(extract_partition_info(s))) {
      LOG_WARN("fail to extract partition info", K(ret));
      s.inner_errcode_ = ret;
      TRANSACT_RETURN(SM_ACTION_SEND_ERROR_NOOP, NULL); // disconnect
    } else {
      int64_t addr = cs_info.get_obproxy_route_addr();
      if (OB_UNLIKELY(0 != addr)) {
        uint32_t ip = 0;
        uint16_t port = 0;
        get_ip_port_from_addr(addr, ip, port);
        s.server_info_.set_addr(ip, port);
        s.pll_info_.lookup_success_ = true;
        LOG_DEBUG("@obproxy_route_addr is set", "address", s.server_info_.addr_, K(addr));
      } else if (obmysql::OB_MYSQL_COM_STMT_FETCH == cmd
                 || obmysql::OB_MYSQL_COM_STMT_GET_PIECE_DATA == cmd) {
        ObCursorIdAddr *cursor_id_addr = NULL;
        if (OB_FAIL(cs_info.get_cursor_id_addr(cursor_id_addr))) {
          LOG_WARN("fail to get client cursor id addr", K(ret));
          if (OB_HASH_NOT_EXIST == ret) {
            handle_fetch_request(s);
          } else {
            TRANSACT_RETURN(SM_ACTION_SEND_ERROR_NOOP, NULL);
          }
        } else {
          s.server_info_.set_addr(cursor_id_addr->get_addr());
          s.pll_info_.lookup_success_ = true;
          LOG_DEBUG("succ to set cursor target addr", "address", s.server_info_.addr_, KPC(cursor_id_addr));
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
            LOG_WARN("fail to get piece info", K(ret));
            TRANSACT_RETURN(SM_ACTION_SEND_ERROR_NOOP, NULL);
          }
        } else if (OB_ISNULL(info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("info is null", K(ret));
          TRANSACT_RETURN(SM_ACTION_SEND_ERROR_NOOP, NULL);
        } else if (!info->is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("info is not valid", K(ret));
          TRANSACT_RETURN(SM_ACTION_SEND_ERROR_NOOP, NULL);
        } else {
          s.server_info_.set_addr(info->get_addr());
          s.pll_info_.lookup_success_ = true;
          LOG_DEBUG("succ to set target addr for send piece/prepare execute/send long data", "address",
                    s.server_info_.addr_, KPC(info));
        }
      } else if ((s.mysql_config_params_->is_mock_routing_mode() && !s.sm_->client_session_->is_proxy_mysql_client_)
                 || s.mysql_config_params_->is_mysql_routing_mode()) {
        if (OB_FAIL(s.mysql_config_params_->get_one_test_server_addr(s.server_info_.addr_))) {
          LOG_INFO("mysql or mock mode, but test server addr in not set, do normal pl lookup", K(ret));
          ret = OB_SUCCESS;
        } else {
          s.sm_->client_session_->test_server_addr_ = s.server_info_.addr_;
          s.pll_info_.lookup_success_ = true;
          LOG_DEBUG("mysql mode, test server is valid, just use it and skip pl lookup",
                    "address", s.server_info_.addr_);
        }
      } else if (OB_UNLIKELY(!s.mysql_config_params_->is_random_routing_mode()
                             && !s.api_server_addr_set_
                             && s.pll_info_.te_name_.is_all_dummy_table()
                             && cs_info.is_allow_use_last_session())) {
        acquire_cached_server_session(s);
      } // end of !is_in_test_mode

      if (OB_SUCC(ret)) {
        TRANSACT_RETURN(SM_ACTION_PARTITION_LOCATION_LOOKUP, handle_pl_lookup);
      }
    }
  } else {
    // !need_pl_lookup
    LOG_DEBUG("[ObMysqlTransact::handle request] force to use last server session");
    ObMysqlServerSession *last_session = s.sm_->client_session_->get_server_session();

    if (OB_LIKELY(NULL != last_session)) {
      if (obmysql::OB_MYSQL_COM_STMT_FETCH == cmd) {
        ObCursorIdAddr *cursor_id_addr = NULL;
        if (OB_FAIL(cs_info.get_cursor_id_addr(cursor_id_addr))) {
          LOG_WARN("fail to get client cursor id addr", K(ret));
          if (OB_HASH_NOT_EXIST == ret) {
            handle_fetch_request(s);
          } else {
            TRANSACT_RETURN(SM_ACTION_SEND_ERROR_NOOP, NULL);
          }
        } else {
          if (OB_UNLIKELY(cursor_id_addr->get_addr() != last_session->get_netvc()->get_remote_addr())) {
            ObMysqlClientSession *client_session = s.sm_->get_client_session();
            client_session->attach_server_session(NULL);
            last_session->do_io_read(client_session, 0, NULL);
            client_session->set_last_bound_server_session(last_session);
            client_session->set_need_return_last_bound_ss(true);
          }

          s.server_info_.set_addr(cursor_id_addr->get_addr());
          s.pll_info_.lookup_success_ = true;
        }
      } else if (obmysql::OB_MYSQL_COM_STMT_GET_PIECE_DATA == cmd) {
        ObCursorIdAddr *cursor_id_addr = NULL;
        if (OB_FAIL(cs_info.get_cursor_id_addr(cursor_id_addr))) {
          LOG_WARN("fail to get client cursor id addr", K(ret));
          if (OB_HASH_NOT_EXIST == ret) {
            handle_fetch_request(s);
          } else {
            TRANSACT_RETURN(SM_ACTION_SEND_ERROR_NOOP, NULL);
          }
        } else if (OB_UNLIKELY(cursor_id_addr->get_addr() != last_session->get_netvc()->get_remote_addr())) {
          s.mysql_errcode_ = OB_ERR_DISTRIBUTED_NOT_SUPPORTED;
          s.mysql_errmsg_ = "fetch cursor target server is not the trans server";
          int tmp_ret = OB_SUCCESS;
          if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = build_error_packet(s)))) {
            LOG_WARN("fail to build err packet", K(tmp_ret));
          } else {
            LOG_WARN("fetch cursor target server is not the trans server",
                     "fetch cursor target server", cursor_id_addr->get_addr(),
                     "trans server", ObIpEndpoint(last_session->get_netvc()->get_remote_addr()));
          }

          ret = OB_ERR_DISTRIBUTED_NOT_SUPPORTED;
          s.inner_errcode_ = ret;
          s.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
          TRANSACT_RETURN(SM_ACTION_INTERNAL_NOOP, NULL);
        }

        if (OB_SUCC(ret)) {
          s.server_info_.set_addr(last_session->get_netvc()->get_remote_addr());
          s.pll_info_.lookup_success_ = true;
        }
      } else if (obmysql::OB_MYSQL_COM_STMT_SEND_PIECE_DATA == cmd
                 || obmysql::OB_MYSQL_COM_STMT_SEND_LONG_DATA == cmd) {
        ObPieceInfo *info = NULL;
        if (OB_FAIL(cs_info.get_piece_info(info))) {
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            // do nothing
          } else {
            TRANSACT_RETURN(SM_ACTION_SEND_ERROR_NOOP, NULL);
          }
        } else if (NULL == info) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("info is null unexpected", K(ret));
          TRANSACT_RETURN(SM_ACTION_SEND_ERROR_NOOP, NULL);
        } else if (!info->is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("info is invalid", K(ret));
          TRANSACT_RETURN(SM_ACTION_SEND_ERROR_NOOP, NULL);
        } else if (OB_UNLIKELY(info->get_addr() != last_session->get_netvc()->get_remote_addr())) {
          s.mysql_errcode_ = OB_ERR_DISTRIBUTED_NOT_SUPPORTED;
          s.mysql_errmsg_ = "send piece info target server is not the trans server";
          int tmp_ret = OB_SUCCESS;
          if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = build_error_packet(s)))) {
            LOG_WARN("fail to build err packet", K(tmp_ret));
          } else {
            LOG_WARN("send piece/long data target server is not the trans server",
                     "target server", info->get_addr(),
                     "trans server", ObIpEndpoint(last_session->get_netvc()->get_remote_addr()));
          }

          ret = OB_ERR_DISTRIBUTED_NOT_SUPPORTED;
          s.inner_errcode_ = ret;
          s.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
          TRANSACT_RETURN(SM_ACTION_INTERNAL_NOOP, NULL);
        }

        if (OB_SUCC(ret)) {
          s.server_info_.set_addr(last_session->get_netvc()->get_remote_addr());
          s.pll_info_.lookup_success_ = true;
        }
      } else {
        s.server_info_.set_addr(last_session->get_netvc()->get_remote_addr());
        s.pll_info_.lookup_success_ = true;
      }

      if (OB_SUCC(ret)) {
        start_access_control(s);
      }
    } else {
      LOG_WARN("[ObMysqlTransact::handle request] last session is NULL, we have to disconnect");
      TRANSACT_RETURN(SM_ACTION_SEND_ERROR_NOOP, NULL);
    }
  } // end of !s.need_pl_lookup
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
    LOG_WARN("client buffer reader fail to consume all", K(ret));
  } else if (OB_FAIL(build_error_packet(s))) {
    LOG_WARN("fail to build err packet", K(ret));
  }

  if (OB_SUCC(ret)) {
    ret = tmp_ret;
  } else {
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
    LOG_WARN("invalid param, client session is NULL", K(ret));
  } else {
    ObMysqlServerSession *last_session = client_session->get_server_session();
    ObMysqlServerSession *last_bound_session = client_session->get_last_bound_server_session();
    if (NULL != last_session) {
      last_session->release();
      client_session->attach_server_session(NULL);
    }
    client_session->set_cur_server_session(last_bound_session);
    if (OB_FAIL(client_session->attach_server_session(last_bound_session))) {
      LOG_WARN("fail to attach server session", K(ret));
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
      LOG_WARN("[ObMysqlTransact::handle request] something wrong before, we have to disconnect");
      TRANSACT_RETURN(SM_ACTION_SEND_ERROR_NOOP, NULL);
    } else {
      TRANSACT_RETURN(SM_ACTION_INTERNAL_REQUEST, handle_internal_request);
    }
  } else if (OB_UNLIKELY(is_internal_request(s))) {
    if (s.trans_info_.client_request_.get_parse_result().is_internal_select()) {
      MYSQL_INCREMENT_TRANS_STAT(CLIENT_USE_LOCAL_SESSION_STATE_REQUESTS);
    }
    // so it's an internal request
    TRANSACT_RETURN(SM_ACTION_INTERNAL_REQUEST, handle_internal_request);
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

    if (OB_ISNULL(s.sm_->client_session_)) {
      LOG_WARN("[ObMysqlTransact::handle request] client session is NULL, we have to disconnect");
      TRANSACT_RETURN(SM_ACTION_SEND_ERROR_NOOP, NULL);
    } else if (OB_UNLIKELY(is_session_memory_overflow(s))) {
      s.mysql_errcode_ = OB_EXCEED_MEM_LIMIT;
      int tmp_ret = OB_SUCCESS;
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = build_error_packet(s)))) {
        LOG_WARN("fail to build err packet", K(tmp_ret));
      } else {
        LOG_WARN("client memory exceed memory limit",
                 "client mem size", s.sm_->client_session_->get_session_info().get_memory_size(),
                 "config mem size", s.mysql_config_params_->client_max_memory_size_);
      }
      s.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
      TRANSACT_RETURN(SM_ACTION_INTERNAL_NOOP, NULL);
    } else if (OB_LIKELY(s.sm_->client_session_->get_session_info().is_oceanbase_server())) {
      handle_oceanbase_request(s);
    } else {
      handle_mysql_request(s);
    }// end of NULL != s.sm_->client_session_
  }
}

inline bool ObMysqlTransact::need_use_last_server_session(ObTransState &s)
{
  // there are three cases we must force to use last server session
  // 1. trans has begin, other sql must send to the same server session
  // 2. a func depend on last execute sql
  // 3. has already specified transaction characteristics (set transaction xxx), not commit yet,
  return (is_in_trans(s)
          || OB_UNLIKELY(NULL != s.sm_->client_session_
              && !s.sm_->client_session_->is_session_pool_client()
              && (s.trans_info_.client_request_.get_parse_result().has_dependent_func()
                || s.sm_->client_session_->get_session_info().is_trans_specified())));
}

inline bool ObMysqlTransact::need_pl_lookup(ObTransState &s)
{
  // if we don't use last server session, we must do pl lookup
  return !need_use_last_server_session(s);
}

//reroute conditions:
// 1. Transaction first SQL
// 2. No reroute has occurred
// 3. It is not a large request, and the request is less than 4K
// (because the data in the read_buffer will be consumed again here, and the second routing will be copied from the 4K Buffer)
// 4. In case of EXECUTE or PREPARE_EXECUTE request, no pieceInfo structure exists
inline bool ObMysqlTransact::is_need_reroute(ObMysqlTransact::ObTransState &s)
{
  int ret = OB_SUCCESS;
  int64_t total_request_packet_len = s.trans_info_.client_request_.get_packet_len();
  int64_t cached_request_packet_len = s.trans_info_.client_request_.get_req_pkt().length();
  bool is_need_reroute = false;

  is_need_reroute = s.mysql_config_params_->enable_reroute_
                    && !s.is_rerouted_ && s.need_pl_lookup_
                    && s.is_trans_first_request_
                    && !s.trans_info_.client_request_.is_large_request()
                    && total_request_packet_len == cached_request_packet_len;

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

int ObMysqlTransact::extract_partition_info(ObTransState &s)
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
        LOG_WARN("default cluster resource can only used by sys tenant",
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
      database_name = parse_result.get_database_name();
      if (OB_LIKELY(database_name.empty())) {
        if (OB_SUCCESS != cs_info.get_database_name(database_name)) {
          database_name = OB_SYS_DATABASE_NAME;
        }
      } else {
        is_database_name_from_parser = true;
      }

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

    if (OB_UNLIKELY(s.mysql_config_params_->enable_index_route_
        && is_need_use_sql_table_cache(s)
        && table_name != OB_ALL_DUMMY_TNAME)) {
      ObString sql_id;
      // parse sql id
      if (OB_FAIL(ObMysqlRequestAnalyzer::analyze_sql_id(s.trans_info_.client_request_, sql_id))) {
        LOG_WARN("fail to analyze sql id", K(ret));
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
              LOG_WARN("fail to alloc ObSqlTableEntry", K(key), K(table_name), K(ret));
            } else if (OB_FAIL(sql_table_cache.add_sql_table_entry(entry))) {
              LOG_WARN("fail to add ObSqlTableEntry", KPC(entry), K(ret));
            } else {
              LOG_INFO("succ to add sql table entry", K(key), KPC(entry), K(table_name));
            }
            if (NULL != entry) {
              // entry->inc_ref will been called if succ to add
              entry->dec_ref();
              entry = NULL;
            }
          } else {
            LOG_WARN("fail to get table name from sql table cache", K(key), K(ret),
                     "sql", s.trans_info_.client_request_.get_print_sql());
          }
        } else if (OB_FAIL(parse_result.set_real_table_name(table_name_buf, strlen(table_name_buf)))) {
          LOG_WARN("fail to set real table name", K(table_name_buf), K(ret));
        } else {
          table_name = parse_result.get_table_name();
          LOG_DEBUG("succ to get real table name", K(table_name));
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
    LOG_WARN("fail to extract_partition_info", K(te_name), K(ret));
    te_name.reset();
  }

  return ret;
}

inline void ObMysqlTransact::setup_plugin_request_intercept(ObTransState &s)
{
  if (OB_ISNULL(s.sm_->api_.plugin_tunnel_)) {
    LOG_ERROR("invalid internal state", "plugin_tunnel", s.sm_->api_.plugin_tunnel_);
    TRANSACT_RETURN(SM_ACTION_SEND_ERROR_NOOP, NULL);
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
  return (need_update_entry() && !route_.is_dummy_table());
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
    const bool need_update = need_update_entry_by_partition_hit();
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
      //or
      // 3. weak read, but calculate server return reroute
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
        if (is_target_location_server()) {
          LOG_INFO("will set weak read route dirty",
                   "origin_name", te_name_, "route_info", route_);
          if (route_.set_target_dirty()) {
            get_pl_task_flow_controller().handle_new_task();
          }
        }
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
      is_read_weak_supported = s.sm_->client_session_->get_session_info().is_read_weak_supported();
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
        //TODO::here proxy only calculate one table entry, it is not accuracy.
        //      it may partition miss when use target_server under multi table.
        const bool update_by_not_hit = (is_target_server && !is_partition_hit && is_read_weak_supported);
        const bool update_by_not_miss = (!is_target_server && is_partition_hit);
        const bool update_by_leader_dead = (route_.is_follower_first_policy() && route_.is_leader_force_congested());
        if (update_by_not_hit || update_by_not_miss || update_by_leader_dead) {

          bool is_need_force_flush = update_by_not_hit && route_.is_remote_readonly();

          LOG_INFO("will set weak read route dirty", K(is_target_server), K(is_partition_hit),
                   K(is_read_weak_supported), K(update_by_leader_dead), K(is_need_force_flush),
                   "origin_name", te_name_, "route_info", route_);

          if (route_.set_target_dirty(is_need_force_flush)) {
            get_pl_task_flow_controller().handle_new_task();
          }
        }
      } else {
        LOG_ERROR("it should never arriver here", "origin_name", te_name_, "route_info", route_);
      }
    }
  }
}

inline void ObMysqlTransact::handle_internal_request(ObTransState &s)
{
  LOG_DEBUG("[ObMysqlTransact::handle_internal_request] "
            "fail to do internal request, will disconnect");
  s.free_internal_buffer();
  TRANSACT_RETURN(SM_ACTION_SEND_ERROR_NOOP, NULL);
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
  } else {
    if (!s.congestion_lookup_success_) { // congestion entry lookup failed
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[ObMysqlTransact::handle_congestion_control_lookup] "
               "fail to lookup congestion entry, will disconnect");
    } else {
      bool is_in_alive_congested = false;
      bool is_in_dead_congested = false;
      ObCongestionEntry *cgt_entry= s.congestion_entry_;
      if (NULL == cgt_entry) {
        ObProxyMutex *mutex_ = s.sm_->mutex_;
        CONGEST_INCREMENT_DYN_STAT(dead_congested_stat);
        s.current_.state_ = ObMysqlTransact::DEAD_CONGESTED;
        handle_congestion_entry_not_exist(s);
        handle_response(s);

      } else {
        is_in_dead_congested = cgt_entry->is_dead_congested();
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

        if (is_in_dead_congested) {
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
          } else {
            is_in_alive_congested = true;
          }
        }

        if (is_in_alive_congested || is_in_dead_congested) {
          LOG_DEBUG("target server is in congested",
                    "addr", s.server_info_.addr_,
                    K(is_in_dead_congested),
                    K(is_in_alive_congested),
                    "force_retry_congested", s.force_retry_congested_,
                    KPC(cgt_entry),
                    "pll info", s.pll_info_);
        }

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
    TRANSACT_RETURN(SM_ACTION_SEND_ERROR_NOOP, NULL);
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
    LOG_WARN("[modify_pl_lookup] it should arrive here, will disconnect",
             "origin_name", s.pll_info_.te_name_, "route info", s.pll_info_.route_);
    TRANSACT_RETURN(SM_ACTION_SEND_ERROR_NOOP, NULL);
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
  addr.set_ipv4_addr(s.server_info_.addr_.get_ip4_host_order(),
                     static_cast<int32_t>(s.server_info_.addr_.get_port_host_order()));
  if (OB_ISNULL(pl) || pl->is_server_changed()) {
    // get the max snapshot server of all server
    int64_t count = s.sm_->client_session_->dummy_ldc_.count();
    const ObLDCItem *item_array = s.sm_->client_session_->dummy_ldc_.get_item_array();
    for (int64_t i = 0; i < count; ++i) {
      if (OB_ISNULL(item_array[i].replica_)) {
        LOG_WARN("item_array[i].replica_ is null, ignore it");
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
        LOG_WARN("replica is null, unexpected", K(i));
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
    LOG_WARN("fail to tryrdlock server_state_lock, ignore", K(ss_version), K(err_no));
  } else {
    if (OB_FAIL(simple_servers_info.assign(server_state_info))) {
      LOG_WARN("fail to assign simple_servers_info", K(ret));
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
        LOG_WARN("fail to get region name", K(idc_name), K(ret));
      }
    }
  }
}

void ObMysqlTransact::handle_pl_lookup(ObTransState &s)
{
  int ret = OB_SUCCESS;
  ++s.pll_info_.pl_attempts_;
  if (OB_UNLIKELY(!s.pll_info_.lookup_success_)) { // partition location lookup failed
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[ObMysqlTransact::handle_pl_lookup] "
             "fail to lookup partition location, will disconnect", K(ret));
  } else {
    // ok, and the partition location lookup succeeded
    LOG_DEBUG("[ObMysqlTransact::handle_pl_lookup] Partition location Lookup successful",
              "pl_attempts", s.pll_info_.pl_attempts_);
    const bool is_server_addr_set = s.server_info_.addr_.is_valid();
    if (OB_UNLIKELY(s.api_server_addr_set_ && !is_server_addr_set)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[ObMysqlTransact::handle_pl_lookup] "
               "observer ip/port doesn't set correctly, will disconnect");
    } else if (OB_ISNULL(s.sm_->client_session_)
          || OB_ISNULL(s.mysql_config_params_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[ObMysqlTransact::handle_pl_lookup] "
               "it should not arrive here, will disconnect", K(ret));
    } else if (OB_UNLIKELY(is_server_addr_set && s.mysql_config_params_->is_mysql_routing_mode())) {
      LOG_DEBUG("[ObMysqlTransact::handle_pl_lookup] use mysql route mode and server addr is set, "
                "use cached session",
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
      bool fill_addr = false;
      bool use_dup_replica = need_use_dup_replica(consistency_level, s);
      // if not support safe_weak_read snapshot version, we should disable sort by priority
      if (OB_UNLIKELY(common::WEAK == consistency_level)) {
        if (!s.sm_->is_causal_order_read_enabled()) {
          LOG_DEBUG("safe weak read is disabled",
                    "proxy_enable_causal_order_read",
                    s.sm_->trans_state_.mysql_config_params_->enable_causal_order_read_,
                    "server_enable_causal_order_read",
                    s.sm_->client_session_->get_session_info().is_safe_read_weak_supported());
          s.sm_->client_session_->dummy_ldc_.set_safe_snapshot_manager(NULL);
        } else {
          LOG_DEBUG("safe weak read is enabled");
        }
      } else {
        if (common::STRONG == consistency_level && 1 == s.pll_info_.pl_attempts_
          && NULL != s.pll_info_.route_.table_entry_
          && !s.pll_info_.route_.table_entry_->is_dummy_entry()
          && !use_dup_replica
          && '\0' == s.mysql_config_params_->proxy_primary_zone_name_[0]) {
          const ObProxyPartitionLocation *pl = s.pll_info_.route_.table_entry_->get_first_pl();
          for (int64_t i = 0; OB_SUCC(ret) && NULL != pl && i < pl->replica_count(); i++) {
            const ObProxyReplicaLocation *replica = pl->get_replica(i);
            if (NULL != replica && replica->is_leader()) {
              fill_addr = true;
              s.server_info_.set_addr(replica->server_.get_ipv4(), static_cast<uint16_t>(replica->server_.get_port()));
              s.pll_info_.route_.cur_chosen_server_.replica_ = replica;
              s.pll_info_.route_.leader_item_.is_used_ = true;
              s.pll_info_.route_.skip_leader_item_ = true;
              break;
            }
          }
        }
      }

      if (!fill_addr) {
        s.pll_info_.route_.need_use_dup_replica_ = use_dup_replica;
        const bool disable_merge_status_check = need_disable_merge_status_check(s);
        const ObRoutePolicyEnum route_policy = s.get_route_policy(*s.sm_->client_session_, use_dup_replica);

        ModulePageAllocator *allocator = NULL;
        ObLDCLocation::get_thread_allocator(allocator);

        ObSEArray<ObString, 5> region_names(5, *allocator);
        ObSEArray<ObServerStateSimpleInfo, ObServerStateRefreshCont::DEFAULT_SERVER_COUNT> simple_servers_info(
            ObServerStateRefreshCont::DEFAULT_SERVER_COUNT, *allocator);
        get_region_name_and_server_info(s, simple_servers_info, region_names);
        ObString proxy_primary_zone_name(s.mysql_config_params_->proxy_primary_zone_name_);

        if (OB_FAIL(s.pll_info_.route_.fill_replicas(
                consistency_level,
                route_policy,
                disable_merge_status_check,
                s.sm_->client_session_->dummy_entry_,
                s.sm_->client_session_->dummy_ldc_,
                simple_servers_info,
                region_names,
                proxy_primary_zone_name
#if OB_DETAILED_SLOW_QUERY
                ,
                s.sm_->cmd_time_stats_.debug_random_time_,
                s.sm_->cmd_time_stats_.debug_fill_time_
#endif
                ))) {
          LOG_WARN("fail to fill replicas", K(ret));
        } else if (is_server_addr_set) {
          LOG_DEBUG("server addr is set, use cached session",
                    "addr", s.server_info_.addr_,
                    "attempts", s.current_.attempts_,
                    "sm_id", s.sm_->sm_id_);
        } else {
#if OB_DETAILED_SLOW_QUERY
          t2 = get_based_hrtime(s);
          s.sm_->cmd_time_stats_.debug_total_fill_time_ += milestone_diff(t1, t2);
          t1 = t2;
#endif

          int32_t attempt_count = 0;
          bool found_leader_force_congested = false;
          if (OB_UNLIKELY(s.mysql_config_params_->is_random_routing_mode()) && (!s.sm_->client_session_->is_proxy_mysql_client_)) {
            uint32_t last_ip = 0;
            uint16_t last_port = 0;
            ObNetVConnection *netvc = NULL;
            if (NULL != s.sm_->client_session_->get_server_session() && NULL != (netvc = s.sm_->client_session_->get_server_session()->get_netvc()) && s.pll_info_.replica_size() > 1) {
              last_ip = ntohl(netvc->get_remote_ip());
              last_port = netvc->get_remote_port();
            }
            replica = s.pll_info_.get_next_replica(last_ip, last_port, s.force_retry_congested_);
          } else {
            replica = s.pll_info_.get_next_avail_replica(s.force_retry_congested_, attempt_count,
                                                         found_leader_force_congested);
            s.current_.attempts_ += attempt_count;
            if ((NULL == replica) && s.pll_info_.is_all_iterate_once()) { // next round
              // if we has tried all servers, do force retry in next round
              LOG_INFO("all replica is force_congested, force retry congested now",
                       "route", s.pll_info_.route_);
              s.pll_info_.reset_cursor();
              s.force_retry_congested_ = true;

              // get replica again
              replica = s.pll_info_.get_next_avail_replica();
            }
          }

#if OB_DETAILED_SLOW_QUERY
          t2 = get_based_hrtime(s);
          s.sm_->cmd_time_stats_.debug_get_next_time_ += milestone_diff(t1, t2);
          t1 = t2;
#endif

          if (OB_ISNULL(replica)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("no replica avail", K(replica), K(ret));
          } else {
            s.server_info_.set_addr(replica->server_.get_ipv4(),
                                    static_cast<uint16_t>(replica->server_.get_port()));
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
    TRANSACT_RETURN(SM_ACTION_SEND_ERROR_NOOP, NULL);
  }
}

void ObMysqlTransact::handle_server_addr_lookup(ObTransState &s)
{
  int ret = OB_SUCCESS;
  ObProxyKillQueryInfo *query_info = s.trans_info_.client_request_.query_info_;

  if (OB_ISNULL(query_info)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("[ObMysqlTransact::handle_server_addr_lookup] query_info should not be null, will disconnect", K(ret));
  } else if (!query_info->is_lookup_succ()) {
    // server session lookup failed, response err/ok packet
    LOG_DEBUG("[ObMysqlTransact::handle_server_addr_lookup] server addr lookup "
              "failed, response err/ok packet", "errcode", query_info->errcode_);
    ObMIOBuffer *buf = NULL;
    // consume data in client buffer reader
    if (OB_FAIL(s.sm_->get_client_buffer_reader()->consume_all())) {
      LOG_WARN("[ObMysqlTransact::handle_server_addr_lookup] fail to consume client_buffer_reader_", K(ret));
    } else if (OB_FAIL(s.alloc_internal_buffer(MYSQL_BUFFER_SIZE))) {
      LOG_WARN("[ObMysqlTransact::handle_server_addr_lookup] fail to allocate internal miobuffer", K(ret));
    } else {
      buf = s.internal_buffer_;
      uint8_t seq = static_cast<uint8_t>(s.trans_info_.client_request_.get_packet_meta().pkt_seq_ + 1);

      switch (query_info->errcode_) {
        case OB_SUCCESS: {
          if (OB_ISNULL(s.sm_->client_session_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("[ObMysqlTransact::handle_server_addr_lookup] client session is NULL, will disconnect", K(ret));
          } else {
            const ObMySQLCapabilityFlags &capability = get_client_session_info(s).get_orig_capability_flags();
            if (OB_FAIL(ObMysqlPacketUtil::encode_ok_packet(*buf, seq, 0, capability))) {
              LOG_WARN("[ObMysqlTransact::handle_server_addr_lookup] fail to build ok resp "
                       "packet", K(ret));
            }
          }
          break;
        }
        case OB_UNKNOWN_CONNECTION:
        case OB_ERR_KILL_DENIED: {
          if (OB_FAIL(ObMysqlPacketUtil::encode_err_packet(*buf, seq, query_info->errcode_, query_info->cs_id_))) {
            LOG_WARN("[ObMysqlTransact::handle_server_addr_lookup] fail to build err resp packet",
                     K(ret), "cs_id", query_info->cs_id_);
          }
          break;
        }
        case OB_ERR_NO_PRIVILEGE: {
          if (OB_FAIL(ObMysqlPacketUtil::encode_err_packet(*buf, seq, query_info->errcode_,
                                                           query_info->priv_name_))) {
            LOG_WARN("[ObMysqlTransact::handle_server_addr_lookup] fail to build err resp packet",
                     K(ret), "priv_name", query_info->priv_name_);
          }
          break;
        }
        default: {
          if (OB_FAIL(ObMysqlPacketUtil::encode_err_packet(*buf, seq, query_info->errcode_))) {
            LOG_WARN("[ObMysqlTransact::handle_server_addr_lookup] fail to build err resp "
                      "packet", K(ret));
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
        LOG_WARN("[ObMysqlTransact::handle_server_addr_lookup] observer ip/port doesn't set "
                 "correctly, will disconnect", K(ret));
      } else {
        LOG_DEBUG("[ObMysqlTransact::handle_server_addr_lookup] choosen server",
                  K(s.server_info_.addr_));
        start_access_control(s);
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[ObMysqlTransact::handle_server_addr_lookup] it should not come here, will disconnect", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    TRANSACT_RETURN(SM_ACTION_SEND_ERROR_NOOP, NULL);
  }
}

inline void ObMysqlTransact::start_access_control(ObTransState &s)
{
  if (s.need_pl_lookup_) {
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

inline int ObMysqlTransact::build_user_request(
    ObTransState &s, ObIOBufferReader *client_buffer_reader,
    ObIOBufferReader *&reader, int64_t &request_len)
{
  int ret = OB_SUCCESS;

  if (s.sm_->client_session_->get_session_info().is_oceanbase_server()) {
    if (OB_FAIL(build_oceanbase_user_request(s, client_buffer_reader, reader, request_len))) {
      LOG_WARN("fail to build oceanbase user request", K(ret));
    }
  } else {
    // no need compress, send directly
    int64_t client_request_len = s.trans_info_.client_request_.get_packet_len();
    request_len = client_request_len;
    reader = client_buffer_reader;
  }

  return ret;
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
    // get server_ps_id by client_ps_id
    uint32_t server_ps_id = ss_info.get_server_ps_id(client_ps_id);
    client_buffer_reader->replace(reinterpret_cast<const char*>(&server_ps_id), sizeof(server_ps_id), MYSQL_NET_META_LENGTH);
  } else if (obmysql::OB_MYSQL_COM_STMT_FETCH == cmd
             || obmysql::OB_MYSQL_COM_STMT_GET_PIECE_DATA == cmd) {
    ObServerSessionInfo &ss_info = get_server_session_info(s);
    ObClientSessionInfo &cs_info = get_client_session_info(s);
    uint32_t client_cursor_id = cs_info.get_client_cursor_id();
    uint32_t server_cursor_id = 0;
    // get server_cursor_id by client_cursor_id
    // if no server_cursor_id, mayby server session has disconnected, disconnect client session
    if (OB_FAIL(ss_info.get_server_cursor_id(client_cursor_id, server_cursor_id))) {
      LOG_WARN("fail to get server cursor id", K(client_cursor_id), K(ret));
    } else {
      client_buffer_reader->replace(reinterpret_cast<const char*>(&server_cursor_id), sizeof(server_cursor_id), MYSQL_NET_META_LENGTH);
    }
  } else if (obmysql::OB_MYSQL_COM_STMT_CLOSE == cmd) {
    ObServerSessionInfo &ss_info = get_server_session_info(s);
    ObClientSessionInfo &cs_info = get_client_session_info(s);
    uint32_t client_ps_id = cs_info.get_client_ps_id();
    uint32_t server_ps_id = 0;
    // get server_ps_id or server_cursor_id
    if (client_ps_id >= (CURSOR_ID_START)) {
      if (OB_FAIL(ss_info.get_server_cursor_id(client_ps_id, server_ps_id))) {
        LOG_WARN("fail to get server cursor id", "client_cursor_id", client_ps_id, K(ret));
      }
    } else {
      server_ps_id = ss_info.get_server_ps_id(client_ps_id);
    }

    client_buffer_reader->replace(reinterpret_cast<const char*>(&server_ps_id), sizeof(server_ps_id), MYSQL_NET_META_LENGTH);
  } else if (obmysql::OB_MYSQL_COM_STMT_PREPARE_EXECUTE == cmd) {
    ObClientSessionInfo &cs_info = get_client_session_info(s);
    uint32_t recv_client_ps_id = cs_info.get_recv_client_ps_id();
    uint32_t client_ps_id = cs_info.get_client_ps_id();

    /* if recv_client_ps_id == 0, first request, send to server directly
     * if recv_client_ps_id ! = 0, have two case:
     *   1. if first send to this server, need set stmt_id to 0 in packet
     *   2. if not first send to this server, need replace server_ps_id
     */
    if (0 != recv_client_ps_id) {
      ObServerSessionInfo &ss_info = get_server_session_info(s);
      /* if not first send to this server, get real server_ps_id
       * if first send to this server, get 0
       */
      uint32_t server_ps_id = ss_info.get_server_ps_id(client_ps_id);
      client_buffer_reader->replace(reinterpret_cast<const char*>(&server_ps_id), sizeof(server_ps_id), MYSQL_NET_META_LENGTH);
    }
  }
  return ret;
}

inline int ObMysqlTransact::build_oceanbase_user_request(
    ObTransState &s, ObIOBufferReader *client_buffer_reader,
    ObIOBufferReader *&reader, int64_t &request_len)
{
  int ret = OB_SUCCESS;
  ObProxyProtocol ob_proxy_protocol = s.sm_->use_compression_protocol();
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
    // request_content_length_ > 0 means large request, and we can not receive complete at once,
    // here no need compress, and if needed, tunnel's plugin will compress
    // and here no need rewrite stmt id, it will be rewritten in Setup client transfer
    if (s.trans_info_.request_content_length_ > 0) {
      reader = client_buffer_reader;
      request_len = client_request_len;
    } else if (OB_FAIL(rewrite_stmt_id(s, client_buffer_reader))) {
      LOG_WARN("rewrite stmt id failed", K(ret));
    } else {
      ObIOBufferReader *request_buffer_reader = client_buffer_reader;
      if (PROTOCOL_OB20 == ob_proxy_protocol || PROTOCOL_CHECKSUM == ob_proxy_protocol) { // convert standard mysql protocol to compression protocol
        ObMIOBuffer *write_buffer = NULL;
        uint8_t compress_seq = 0;
        if (OB_ISNULL(write_buffer = new_miobuffer(MYSQL_BUFFER_SIZE))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc mio_buffer", K(ret));
        } else if (OB_ISNULL(reader = write_buffer->alloc_reader())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[ObMysqlTransact::build_user_request] failed to allocate iobuffer reader", K(ret));
        } else if (obmysql::OB_MYSQL_COM_STMT_CLOSE == s.trans_info_.client_request_.get_packet_meta().cmd_
                   && OB_ISNULL(request_buffer_reader = client_buffer_reader->clone())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[ObMysqlTransact::build_user_request] failed to clone client buffer reader", K(ret));
        } else {
          if (PROTOCOL_OB20 == ob_proxy_protocol) {
            const bool is_last_packet = true;
            // here and handle_error_resp need have same reroute conditions
            const bool need_reroute = is_need_reroute(s);
            ObSEArray<ObObJKV, 8> extro_info;
            char client_ip_buf[MAX_IP_BUFFER_LEN] = "\0";
            ObMysqlClientSession *client_session = s.sm_->get_client_session();
            if (!client_session->is_proxy_mysql_client_
                && client_session->is_need_send_trace_info()
                && is_last_packet) {
              ObAddr client_ip = client_session->get_real_client_addr();
              if (OB_FAIL(ObProxyTraceUtils::build_client_ip(extro_info, client_ip_buf, client_ip))) {
                LOG_ERROR("fail to build client ip", K(client_ip), K(ret));
              } else {
                client_session->set_already_send_trace_info(true);
              }
            }

            if (OB_SUCC(ret)) {
              if (OB_FAIL(ObProto20Utils::consume_and_compress_data(
                      request_buffer_reader, write_buffer, client_request_len, compress_seq, compress_seq,
                      s.sm_->get_server_session()->get_next_server_request_id(),
                      s.sm_->get_server_session()->get_server_sessid(),
                      is_last_packet, need_reroute, &extro_info))) {
                LOG_ERROR("fail to consume_and_compress_data", K(ret));
              }
            }
          } else {
            const bool use_fast_compress = true;
            const bool is_checksum_on = s.sm_->is_checksum_on();
            if (OB_FAIL(ObMysqlAnalyzerUtils::consume_and_compress_data(
                    request_buffer_reader, write_buffer, client_request_len, use_fast_compress,
                    compress_seq, is_checksum_on))) {
              LOG_WARN("fail to consume_and_compress_data", K(ret));
            }
          }

          if (obmysql::OB_MYSQL_COM_STMT_CLOSE == s.trans_info_.client_request_.get_packet_meta().cmd_) {
            request_buffer_reader->dealloc();
            request_buffer_reader = NULL;
          }

          if (OB_SUCC(ret)) {
            s.sm_->get_server_session()->set_compressed_seq(compress_seq);
            request_len = reader->read_avail();
            LOG_DEBUG("build user compressed request succ", K(ob_proxy_protocol),
                      "origin len", client_request_len, "compress len", request_len);
          }
        }
      } else {
        // no need compress, send directly
        request_len = client_request_len;

        ObMIOBuffer *write_buffer = NULL;
        if (obmysql::OB_MYSQL_COM_STMT_CLOSE == s.trans_info_.client_request_.get_packet_meta().cmd_) {
          int64_t written_len = 0;
          if (OB_ISNULL(write_buffer = new_miobuffer(MYSQL_BUFFER_SIZE))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc mio_buffer", K(ret));
          } else if (OB_ISNULL(request_buffer_reader = write_buffer->alloc_reader())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("[ObMysqlTransact::build_user_request] failed to allocate iobuffer reader", K(ret));
          } else if (OB_FAIL(write_buffer->write(client_buffer_reader, client_request_len, written_len))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("[ObMysqlTransact::build_user_request] fail to write com_stmt_close packet into new iobuffer", K(client_request_len), K(ret));
          } else if (OB_UNLIKELY(client_request_len != written_len)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("[ObMysqlTransact::build_user_request] written_len dismatch", K(client_request_len), K(written_len), K(ret));
          }
        }
        reader = request_buffer_reader;
      }
    }
  }

  LOG_DEBUG("[ObMysqlTransact::build_user_request] send request to observer",
            "sm_id", s.sm_->sm_id_, K(ob_proxy_protocol), K(client_request_len),
            "total_client_request_len", reader->read_avail(),
            "request len send to server", request_len,
            K(s.trans_info_.request_content_length_),
            K(s.trans_info_.sql_cmd_),
            "is_in_auth", s.is_auth_request_, K(ret));
  return ret;
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
    LOG_WARN("[ObMysqlTransact::build_normal_login_request] shard conn is null");
  } else if (OB_ISNULL(write_buffer = new_miobuffer(MYSQL_BUFFER_SIZE))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[ObMysqlTransact::build_normal_login_request] write_buffer is null");
  } else if (OB_ISNULL(reader = write_buffer->alloc_reader())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[ObMysqlTransact::build_normal_login_request] failed to allocate iobuffer reader", K(ret));
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
      LOG_WARN("fail to encrypt password", K(ret));
    } else {
      ObString auth_str(actual_len, pwd_buf);
      tg_hsr.set_auth_response(auth_str);

      tg_hsr.set_database(database);

      // 5. reset before add
      tg_hsr.reset_connect_attr();

      if (OB_FAIL(ObMysqlPacketWriter::write_packet(*write_buffer, tg_hsr))) {
        LOG_WARN("fail to write packet", K(ret));
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
  ObIOBufferReader *client_buffer_reader = NULL;
  BuildFunc build_func = NULL;
  reader = NULL;
  request_len = 0;

  if (OB_ISNULL(client_buffer_reader = s.sm_->get_client_buffer_reader())) {
    LOG_WARN("[ObMysqlTransact::build_server_request] client buffer reader in sm is NULL");
    ret = OB_INVALID_ARGUMENT;
  } else {
    switch (s.current_.send_action_) {
      case SERVER_SEND_REQUEST: {
        if (OB_FAIL(build_user_request(s, client_buffer_reader, reader, request_len))) {
          LOG_WARN("fail to build user request", K(ret));
        }
        break;
      }

      case SERVER_SEND_SSL_REQUEST: {
        build_func = ObMysqlRequestBuilder::build_ssl_request_packet;
        break;
      }

      case SERVER_SEND_LOGIN: {
        // before send first login packet, we must consume current buffer
        if (OB_FAIL(client_buffer_reader->consume_all())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[ObMysqlTransact::build_server_request] "
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
              LOG_WARN("fail to build normal login reuest", K(ret));
            }
          }
        }
        break;
      }

      case SERVER_SEND_SAVED_LOGIN:
        if (s.sm_->client_session_->get_session_info().is_oceanbase_server()) {
          // write the buffor using saved login packet directly
          build_func = ObMysqlRequestBuilder::build_saved_login_packet;
        } else {
          if (OB_FAIL(build_normal_login_request(s, reader, request_len))) {
            LOG_WARN("fail to build normal login reuest", K(ret));
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

      case SERVER_SEND_LAST_INSERT_ID:
        build_func = ObMysqlRequestBuilder::build_last_insert_id_sync_packet;
        break;

      case SERVER_SEND_START_TRANS:
        build_func = ObMysqlRequestBuilder::build_start_trans_request;
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
        LOG_ERROR("Unknown send next action type", K(s.current_.send_action_));
        break;
    }

    if (OB_UNLIKELY(OB_SUCCESS == ret && NULL != build_func)) {
      ObMIOBuffer *write_buffer = NULL;

      if (OB_ISNULL(write_buffer = new_miobuffer(MYSQL_BUFFER_SIZE))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[ObMysqlTransact::build_server_request] write_buffer is null");
      } else if (OB_ISNULL(reader = write_buffer->alloc_reader())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[ObMysqlTransact::build_server_request] failed to allocate iobuffer reader", K(ret));
      } else {
        ObMysqlClientSession *client_session = s.sm_->get_client_session();
        ObMysqlServerSession *server_session = s.sm_->get_server_session();
        ObProxyProtocol ob_proxy_protocol = s.sm_->use_compression_protocol();
        if (OB_ISNULL(client_session) || OB_ISNULL(server_session)) {
          ret = OB_ERR_SYS;
          LOG_WARN("[ObMysqlTransact::build_server_request] invalid internal variables",
                   K(client_session), K(server_session));
        } else if (OB_FAIL(build_func(*write_buffer,
                                      client_session->get_session_info(),
                                      server_session,
                                      ob_proxy_protocol))) {
          LOG_WARN("[ObMysqlTransact::build_server_request] fail to build packet,"
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
    LOG_WARN("fail to get int from string", "string value", str_kv.value_, K(ret));
  } else if (cur_global_vars_version > snapshot_global_vars_version) {
    client_info.set_global_vars_changed_flag();
    LOG_INFO("global vars changed",
             K(cur_global_vars_version), K(snapshot_global_vars_version));
  } else if (cur_global_vars_version == snapshot_global_vars_version) {
    LOG_DEBUG("global vars stay unchanged",
              K(cur_global_vars_version), K(snapshot_global_vars_version));
  } else {
    client_info.set_global_vars_changed_flag();
    LOG_WARN("cur should not smaller than snapshot"
             "(cur == 0 means server use default sys var set)",
             K(cur_global_vars_version), K(snapshot_global_vars_version));
  }
  return ret;
}

inline int ObMysqlTransact::do_handle_prepare_response(ObTransState &s, ObIOBufferReader *&buf_reader)
{
  int ret = OB_SUCCESS;

  //into here, the first packet must received completely
  ObProxyProtocol ob_proxy_protocol = s.sm_->use_compression_protocol();
  ObMysqlResp &server_response = s.trans_info_.server_response_;
  if (!server_response.get_analyze_result().is_decompressed() &&
      (PROTOCOL_CHECKSUM == ob_proxy_protocol || PROTOCOL_OB20 == ob_proxy_protocol)) {
    ObMysqlCompressedOB20AnalyzeResult result;
    ObMysqlCompressAnalyzer *compress_analyzer = &s.sm_->get_compress_analyzer();
    compress_analyzer->reset();
    const uint8_t req_seq = s.sm_->get_request_seq();
    const ObMySQLCmd cmd = s.sm_->get_request_cmd();
    const ObMysqlProtocolMode mysql_mode = s.sm_->client_session_->get_session_info().is_oracle_mode() ? OCEANBASE_ORACLE_PROTOCOL_MODE : OCEANBASE_MYSQL_PROTOCOL_MODE;
    const bool enable_extra_ok_packet_for_stats = s.sm_->is_extra_ok_packet_for_stats_enabled();
    if (OB_FAIL(compress_analyzer->init(req_seq, ObMysqlCompressAnalyzer::DECOMPRESS_MODE,
            cmd, mysql_mode, enable_extra_ok_packet_for_stats, req_seq,
            s.sm_->get_server_session()->get_server_request_id(),
            s.sm_->get_server_session()->get_server_sessid()))) {
      LOG_WARN("fail to init compress analyzer", K(req_seq), K(cmd), K(enable_extra_ok_packet_for_stats),
               "server_request_id", s.sm_->get_server_session()->get_server_request_id(),
               "server_sessid", s.sm_->get_server_session()->get_server_sessid(), K(ret));
    } else if (OB_ISNULL(buf_reader = compress_analyzer->get_transfer_reader())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get transfer reader", K(buf_reader), K(ret));
    } else if (OB_FAIL(compress_analyzer->analyze_first_response(*s.sm_->get_server_buffer_reader(), result))) {
      LOG_WARN("fail to analyze first response", K(s.sm_->get_server_buffer_reader()), K(ret));
    } else if (ANALYZE_DONE != result.status_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("analyze status is wrong", K(result.status_), K(ret));
    }
  } else {
    ObMysqlAnalyzeResult result;
    if (OB_FAIL(ObProxyParserUtils::analyze_one_packet(*s.sm_->get_server_buffer_reader(), result))) {
      LOG_WARN("fail to analyze packet", K(buf_reader), K(ret));
    } else if (ANALYZE_DONE != result.status_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("analyze status is wrong", K(result.status_), K(ret));
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

  uint32_t client_ps_id = cs_info.get_client_ps_id();
  ObPsIdPair *ps_id_pair = ss_info.get_ps_id_pair(client_ps_id);
  /* here two case:
   *  if first in:
   *    if Prepare Request and PrepareExecute Request, client_ps_id is newer and no ps_id_pair
   *  if not first in:  
   *    1. if Execute Request, sync Prepare Request, no ps_id_pair
   *    2. if PrepareExecute Request:
   *      2.1 send to new server, no ps_id_pair
   *      2.2 send to old server, have ps_id_pair, compare server ps id
   */
  if (NULL == ps_id_pair) {
    if (OB_FAIL(ObPsIdPair::alloc_ps_id_pair(client_ps_id, server_ps_id, ps_id_pair))) {
      LOG_WARN("fail to alloc ps id pair", "client_ps_id", cs_info.get_client_ps_id(),
          "server_ps_id", server_ps_id, K(ret));
    } else if (OB_ISNULL(ps_id_pair)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ps_id_pair is null", K(ps_id_pair), K(ret));
    } else if (OB_FAIL(ss_info.add_ps_id_pair(ps_id_pair))) {
      LOG_WARN("fail to add ps_id_pair", KPC(ps_id_pair), K(ret));
      ps_id_pair->destroy();
      ps_id_pair = NULL;
    }
  } else if (OB_UNLIKELY(server_ps_id != ps_id_pair->get_server_ps_id())) {
    ObPsEntry *ps_entry = cs_info.get_ps_entry(client_ps_id);
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("same client_ps_id, but server returns different stmt id",
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
        PROXY_API_LOG(WARN, "fail to alloc ps id addrs", K(client_ps_id), K(ret));
      } else if (OB_ISNULL(ps_id_addrs)) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_API_LOG(WARN, "ps_id_addrs is null", K(ps_id_addrs), K(ret));
      } else if (OB_FAIL(cs_info.add_ps_id_addrs(ps_id_addrs))) {
        PROXY_API_LOG(WARN, "fail to add ps_id_addrs", KPC(ps_id_addrs), K(ret));
        ps_id_addrs->destroy();
      }
    } else {
      if (OB_FAIL(ps_id_addrs->add_addr(addr))) {
        PROXY_API_LOG(WARN, "fail to add addr into ps_id_addrs", "addr", ObIpEndpoint(addr), K(ret));
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
      LOG_WARN("fail to handle prepare response", K(ret));
    } else if (OB_ISNULL(buf_reader)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("reader is null, which is unexpected", K(ret));
    } else if (OB_FAIL(pkt_reader.get_packet(*buf_reader, prepare_packet))) {
      LOG_WARN("fail to get ok packet from server buffer reader", K(ret));
    } else if (FALSE_IT(server_ps_id = prepare_packet.get_statement_id())) {
    } else if (OB_FAIL(do_handle_prepare_succ(s, server_ps_id))) {
      LOG_WARN("fail to do handle prepare succ", K(ret));
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

  uint32_t client_ps_id = cs_info.get_client_ps_id();
  ObPsIdPair *ps_id_pair = ss_info.get_ps_id_pair(client_ps_id);
  ObCursorIdPair *cursor_id_pair = ss_info.get_curosr_id_pair(client_ps_id);
  if (OB_ISNULL(ps_id_pair)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_API_LOG(WARN, "ps id pair is null", K(client_ps_id), K(ret));
  } else if (NULL == cursor_id_pair) {
    // ps cursor use ps_id as cursor_id
    if (OB_FAIL(ObCursorIdPair::alloc_cursor_id_pair(client_ps_id, ps_id_pair->get_server_ps_id(), cursor_id_pair))) {
      PROXY_API_LOG(WARN, "fail to alloc cursor id pair", K(client_ps_id), "server_ps_id", ps_id_pair->get_server_ps_id(), K(ret));
    } else if (OB_ISNULL(cursor_id_pair)) {
      ret = OB_ERR_UNEXPECTED;
      PROXY_API_LOG(WARN, "cursor_id_pair is null", K(cursor_id_pair), K(ret));
    } else if (OB_FAIL(ss_info.add_cursor_id_pair(cursor_id_pair))) {
      PROXY_API_LOG(WARN, "fail to add cursor_id_pair", KPC(cursor_id_pair), K(ret));
      cursor_id_pair->destroy();
    }
  } else if (OB_UNLIKELY(cursor_id_pair->get_server_cursor_id() != ps_id_pair->get_server_ps_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("same client_ps_id, but server ps id and server cursor id id different",
        K(ret), K(client_ps_id), KPC(cursor_id_pair), KPC(ps_id_pair));
  }

  if (OB_SUCC(ret)) {
    ObMysqlServerSession *ss = s.sm_->get_server_session();
    const sockaddr &addr = ss->get_netvc()->get_remote_addr();
    ObCursorIdAddr *cursor_id_addr = cs_info.get_cursor_id_addr(client_ps_id);
    if (NULL == cursor_id_addr) {
      if (OB_FAIL(ObCursorIdAddr::alloc_cursor_id_addr(client_ps_id, addr, cursor_id_addr))) {
        PROXY_API_LOG(WARN, "fail to alloc cursor id addr", K(client_ps_id), K(ret));
      } else if (OB_ISNULL(cursor_id_addr)) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_API_LOG(WARN, "cursor_id_addr is null", K(cursor_id_addr), K(ret));
      } else if (OB_FAIL(cs_info.add_cursor_id_addr(cursor_id_addr))) {
        PROXY_API_LOG(WARN, "fail to add cursor_id_addr", KPC(cursor_id_addr), K(ret));
        cursor_id_addr->destroy();
      }
    } else {
      // save server addr for follow Fetch Request
      cursor_id_addr->set_addr(addr);
    }
  }

  if (OB_SUCC(ret)) {
    ObClientSessionInfo &cs_info = get_client_session_info(s);
    cs_info.remove_piece_info(cs_info.get_client_ps_id());
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
    LOG_WARN("com_stmt_execute packet is empty", K(ret));
  } else {
    const char *pos = data.ptr() + MYSQL_NET_META_LENGTH + 4;
    int8_t flags = 0;
    ObMySQLUtil::get_int1(pos, flags);
    // if request have cursor flag and response is resultSet, think as cursor
    if (flags > 0 && s.trans_info_.server_response_.get_analyze_result().is_resultset_resp()) {
      if (OB_FAIL(do_handle_execute_succ(s))) {
        LOG_WARN("fail to do handle execute succ", K(ret));
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
      LOG_WARN("fail to handle prepare response", K(ret));
    } else if (OB_ISNULL(buf_reader)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("reader is null, which is unexpected", K(ret));
    } else if (OB_FAIL(pkt_reader.get_packet(*buf_reader, prepare_packet))) {
      LOG_WARN("fail to get ok packet from server buffer reader", K(ret));
    } else if (FALSE_IT(server_ps_id = prepare_packet.get_statement_id())) {
    } else if (OB_FAIL(do_handle_prepare_succ(s, server_ps_id))) {
      LOG_WARN("fail to do handle prepare succ", K(ret));
    }

    if (OB_SUCC(ret) && s.trans_info_.server_response_.get_analyze_result().is_resultset_resp()) {
      if (OB_FAIL(do_handle_execute_succ(s))) {
        LOG_WARN("fail to do handle execute succ", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
    s.current_.state_ = INTERNAL_ERROR;
  }
}

void ObMysqlTransact::handle_text_ps_prepare_succ(ObTransState &s)
{
  int ret = OB_SUCCESS;
  if (NULL != s.sm_->client_session_) {
    ObClientSessionInfo &cs_info = get_client_session_info(s);
    ObServerSessionInfo &ss_info = get_server_session_info(s);
    if (OB_FAIL(ss_info.add_text_ps_name(cs_info.get_text_ps_entry()->get_version()))) {
      LOG_WARN("add text ps name failed", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    s.current_.state_ = INTERNAL_ERROR;
  }
}

/* Oceanbase:
 *   user var: no need handle here. handle it in save_changed_session_info
 *   sys var: if exist in comomn_sys, update it; otherwise, update sys
 * MySQL:  
 *   use var: save it when get OK Response
 *   sys var: if exist in comomn_sys, update it; otherwise, update sys
 */
inline void ObMysqlTransact::handle_user_request_succ(ObTransState &s)
{
  int ret = OB_SUCCESS;

  ObClientSessionInfo &client_info = get_client_session_info(s);

  if (client_info.is_sharding_user() && !client_info.is_oceanbase_server()) {
    ObSqlParseResult &sql_result = s.trans_info_.client_request_.get_parse_result();
    const ObProxyBasicStmtType type = sql_result.get_stmt_type();

    if (OBPROXY_T_SET == type && OB_FAIL(handle_user_set_request_succ(s))) {
      LOG_WARN("fail to handle user set request", K(ret));
    } else if (OB_FAIL(handle_normal_user_request_succ(s))) {
      LOG_WARN("fail to handle nomal user request", K(ret));
    }
  }

  if (OB_SUCC(ret) && obmysql::OB_MYSQL_COM_CHANGE_USER == s.trans_info_.sql_cmd_) {
    if (OB_FAIL(handle_change_user_request_succ(s))) {
      LOG_WARN("fail to handle change user request", K(ret));
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
    LOG_WARN("client request is empty", K(change_user), K(ret));
  } else {
    const char *start = change_user.ptr() + MYSQL_NET_META_LENGTH;
    int32_t len = static_cast<uint32_t>(change_user.length() - MYSQL_NET_META_LENGTH);
    if (len <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("client request packet is error", K(ret));
    } else {
      result.set_content(start, len);
      const ObMySQLCapabilityFlags &capability = client_info.get_orig_capability_flags();
      result.set_capability_flag(capability);
      if (OB_FAIL(result.decode())) {
        LOG_WARN("fail to decode change user packet", K(ret));
      } else {
        // 1. Save username and auth_response
        if (OB_FAIL(ObProxySessionInfoHandler::rewrite_change_user_login_req(
          client_info, result.get_username(), result.get_auth_response()))) {
          LOG_WARN("rewrite change user login req failed", K(ret));
        } else {
          /*
           * A database will be filled in the change_user API. So the observer side will switch based on this database
           * server will clean up session var
           * Change_user is allowed to be executed in the transaction, and the current transaction will be forced to rollback after execution
           * The above three version number synchronization has been processed in save_changed_session_info, no need to process it anymore
           */

          // 2. Clean up user variables
          if (OB_FAIL(client_info.remove_all_user_variable())) {
            LOG_WARN("remove all user variable failed", K(ret));
          }

          // 3. Update auth_str
          if (OB_SUCC(ret)) {
            const ObString full_username = client_info.get_full_username();
            ObMysqlServerSession* server_session = s.sm_->get_server_session();
            if (NULL != server_session && full_username.length() < OB_PROXY_FULL_USER_NAME_MAX_LEN) {
              MEMCPY(server_session->full_name_buf_, full_username.ptr(), full_username.length());
              server_session->auth_user_.assign_ptr(server_session->full_name_buf_, full_username.length());
              LOG_DEBUG("handle normal change user request succ", K(server_session->auth_user_));
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("username length is error", K(full_username.length()), K(ret));
            }
          }

          // 4. Clear all server sessions
          if (OB_SUCC(ret)) {
            ObMysqlClientSession* client_session = s.sm_->get_client_session();
            client_session->get_session_manager().purge_keepalives();
          }
        }
      }
    }
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
    LOG_WARN("invalid session, client session is NULL");
  } else if (OB_LIKELY(set_info.node_count_ > 0)) {

    ObClientSessionInfo &client_info = get_client_session_info(s);
    for (int i = 0; OB_SUCC(ret) && i < set_info.node_count_; i++) {
      SetVarNode &var_node = set_info.var_nodes_.at(i);
      ObObj value;
      if (SET_VALUE_TYPE_INT == var_node.value_type_) {
        value.set_int(var_node.int_value_);
      } else if (SET_VALUE_TYPE_NUMBER == var_node.value_type_) {
        const char *nptr = var_node.str_value_.buf_;
        char *end_ptr = NULL;
        double double_val = strtod(nptr, &end_ptr);
        if (*nptr != '\0' && *end_ptr == '\0') {
          value.set_double(double_val);
        } else {
          OBLOG_LOG(WARN, "invalid double value", "str", var_node.str_value_.string_);
          value.set_varchar(var_node.str_value_.string_);
        }
      } else {
        if (0 == var_node.var_name_.string_.case_compare("character_set_results")
            && 0 == var_node.str_value_.string_.case_compare("NULL")) {
          value.set_varchar(ObString::make_empty_string());
        } else {
          value.set_varchar(var_node.str_value_.string_);
        }
      }

      // TODO: if not exist, insert; otherwise, cmp and update
      if (SET_VAR_USER == var_node.var_type_) {
        if (OB_FAIL(client_info.replace_user_variable(var_node.var_name_.string_,
                                                      value))) {
          LOG_WARN("fail to replace user variable", K(var_node), K(ret));
        } else {
          LOG_DEBUG("succ to update user variables", "key", var_node.var_name_.string_, K(value));
        }
      } else if (SET_VAR_SYS == var_node.var_type_) {
        if (OB_FAIL(client_info.update_common_sys_variable(var_node.var_name_.string_,
                                                           value, true, false))) {
          LOG_WARN("fail to update common sys variable", K(var_node), K(ret));
        } else {
          LOG_DEBUG("succ to update common sys variables", "key", var_node.var_name_.string_, K(value));
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
    LOG_WARN("invalid session, client session is NULL");
  } else {
    ObServerSessionInfo &server_info = get_server_session_info(s);
    ObClientSessionInfo &client_info = get_client_session_info(s);
    ObIOBufferReader *buf_reader = s.sm_->get_server_buffer_reader();
    ObMysqlPacketReader pkt_reader;
    OMPKOK ok_packet;
    const ObMySQLCapabilityFlags cap = server_info.get_compatible_capability_flags();

    if (OB_ISNULL(buf_reader)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("reader is null, which is unexpected", K(ret));
    } else if (OB_FAIL(pkt_reader.get_ok_packet(*buf_reader, 0, cap, ok_packet))) {
      LOG_WARN("fail to get ok packet from server buffer reader", K(ret));
    } else {
      if (ok_packet.is_schema_changed()) {
        const ObString &db_name = ok_packet.get_changed_schema();
        if (!db_name.empty()) {
          bool is_string_to_lower_case = client_info.is_oracle_mode() ? false : client_info.need_use_lower_case_names();
          if (OB_FAIL(client_info.set_database_name(db_name))) {
            LOG_WARN("fail to set changed database name", K(db_name), K(ret));
          } else if (OB_FAIL(server_info.set_database_name(db_name, is_string_to_lower_case))) {
            LOG_WARN("fail to set changed database name", K(db_name), K(ret));
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
          LOG_WARN("fail to update mysql sys variable", K(str_kv), K(ret));
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
      LOG_WARN("fail to handle oceanbase saved login succ", K(ret));
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
      LOG_WARN("reader is null, which is unexpected", K(ret));
    } else if (OB_FAIL(pkt_reader.get_ok_packet(*buf_reader, 0, cap, ok_packet))) {
      LOG_WARN("fail to get ok packet from server buffer reader", K(ret));
    } else {
      const ObIArray<ObStringKV> &sys_var = ok_packet.get_system_vars();
      // current, we only care about OB_SV_PROXY_GLOBAL_VARIABLES_VERSION and OB_SV_CAPABILITY_FLAG
      for (int64_t i = 0; i < sys_var.count() && OB_SUCC(ret); ++i) {
        const ObStringKV &str_kv = sys_var.at(i);
        // check global vars version, if the global vars has changed, we no need to check again
        if (str_kv.key_ == OB_SV_PROXY_GLOBAL_VARIABLES_VERSION
            && !client_info.is_global_vars_changed()) {
          if (OB_FAIL(check_global_vars_version(s, str_kv))) {
            LOG_WARN("fail to check global vars version", K(str_kv), K(ret));
          }
        } else if (str_kv.key_ == OB_SV_CAPABILITY_FLAG) {
          if (OB_FAIL(ObProxySessionInfoHandler::handle_capability_flag_var(
                  client_info, server_info, str_kv.value_, false, need_save))) {
            LOG_WARN("fail to handle capability flag var", K(str_kv), K(ret));
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
      LOG_WARN("fail to get client info database name", K(db_name), K(ret));
    } else if (OB_FAIL(server_info.set_database_name(db_name, is_string_to_lower_case))) {
      LOG_WARN("fail to set changed database name", K(db_name), K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    s.current_.state_ = INTERNAL_ERROR;
  }
}

void ObMysqlTransact::handle_error_resp(ObTransState &s)
{
  ObRespAnalyzeResult &resp = s.trans_info_.server_response_.get_analyze_result();
  bool is_user_request = false;
  int64_t max_connect_attempts = get_max_connect_attempts(s);
  ObServerStateType pre_state = s.current_.state_;
  s.current_.state_ = RESPONSE_ERROR;

  switch(s.current_.send_action_) {
    case SERVER_SEND_HANDSHAKE:
      if (!s.sm_->client_session_->get_session_info().is_oceanbase_server()) {
        if (OB_SUCCESS != s.sm_->get_client_buffer_reader()->consume_all()) {
          s.current_.state_ = INTERNAL_ERROR;
          LOG_WARN("fail to consume client buffer reader", "state", s.current_.state_);
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
          LOG_WARN("cluster is not matched, maybe server had been migrated to other cluster",
                   "send_action", get_send_action_name(s.current_.send_action_),
                   "state", get_server_state_name(s.current_.state_),
                   "proxy_id", s.mysql_config_params_->proxy_id_,
                   "server_sessid", s.sm_->get_server_session()->get_server_sessid(),
                   "cluster_name", s.sm_->get_client_session()->get_session_info().get_priv_info().cluster_name_,
                   K(resp));
          if (s.sm_->get_client_session()->get_session_info().get_priv_info().cluster_name_ == OB_META_DB_CLUSTER_NAME) {
            LOG_WARN("this is MetaDataBase, proxy need rebuild this cluster resource later",
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
            LOG_ERROR("connection id is repetitive, current proxy_id maybe already "
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
        }
      } else {
        is_user_request = true;
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
        LOG_WARN("cluster is not matched, maybe server had been migrated to other cluser",
                 "send_action", get_send_action_name(s.current_.send_action_),
                 "state", get_server_state_name(s.current_.state_),
                 "proxy_id", s.mysql_config_params_->proxy_id_,
                 "server_sessid", s.sm_->get_server_session()->get_server_sessid(),
                 "cluster_name", s.sm_->get_client_session()->get_session_info().get_priv_info().cluster_name_,
                 K(resp));
        if (s.sm_->get_client_session()->get_session_info().get_priv_info().cluster_name_ == OB_META_DB_CLUSTER_NAME) {
          LOG_WARN("this is MetaDataBase, proxy need rebuild this cluster resource later",
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
          LOG_ERROR("connection id is repetitive, current proxy_id maybe already "
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
      if (OB_UNLIKELY(cached_request_packet_len > total_request_packet_len)) { // impossible, just defense
        LOG_ERROR("unexpected branch, maybe receive two mysql packets",
                  K(cached_request_packet_len), K(total_request_packet_len),
                 "sql", s.trans_info_.client_request_.get_print_sql(), K(resp));
      }
      if (resp.is_ora_fatal_error()) {
        s.current_.error_type_ = ORA_FATAL_ERROR;
      } else if (resp.is_standby_weak_readonly_error()
                 && s.sm_->sm_cluster_resource_->is_deleting()
                 && OB_DEFAULT_CLUSTER_ID == s.sm_->sm_cluster_resource_->get_cluster_id()) {
        // if primary switchover, need disconnection when get STANDBY_WEAK_READONLY_ERROR error
        s.current_.error_type_ = STANDBY_WEAK_READONLY_ERROR;
      } else if (s.is_trans_first_request_
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

      // It means that the Prepare request failed to execute,
      // and the previously created ps_id_entry needs to be cleaned up
      if (is_user_request
          && obmysql::OB_MYSQL_COM_STMT_PREPARE == s.trans_info_.sql_cmd_) {
        ObClientSessionInfo &cs_info = get_client_session_info(s);
        uint32_t client_ps_id = cs_info.get_client_ps_id();
        cs_info.remove_ps_id_entry(client_ps_id);
      }
      break;
    }
    case SERVER_SEND_ALL_SESSION_VARS:
    case SERVER_SEND_SESSION_VARS:
    case SERVER_SEND_LAST_INSERT_ID:
      s.current_.error_type_ = RESET_SESSION_VARS_COMMON_ERROR;
      break;

    case SERVER_SEND_USE_DATABASE:
      s.current_.error_type_ = SYNC_DATABASE_COMMON_ERROR;
      break;

    case SERVER_SEND_START_TRANS:
      s.current_.error_type_ = START_TRANS_COMMON_ERROR;
      break;

    case SERVER_SEND_PREPARE:
      s.current_.error_type_ = SYNC_PREPARE_COMMON_ERROR;
      break;

    case SERVER_SEND_TEXT_PS_PREPARE:
      s.current_.error_type_ = SYNC_TEXT_PS_PREPARE_COMMON_ERROR;
      break;

    case SERVER_SEND_NONE:
    default:
      s.current_.state_ = INTERNAL_ERROR;
      LOG_ERROR("unknown send action", K(s.current_.send_action_));
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
        // real physic table name, case sensitive
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
          LOG_WARN("fail to update real table name", K(s.reroute_info_), K(key),
                    "sql", s.trans_info_.client_request_.get_print_sql());
        } else {
          LOG_INFO("succ to update real table name", K(s.reroute_info_), K(key),
          "sql", s.trans_info_.client_request_.get_print_sql());
        }
      }
      s.is_rerouted_ = true;
    }
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
      LOG_WARN("get err pkt from observer, not forward to client, "
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

inline void ObMysqlTransact::handle_resultset_resp(ObTransState &s)
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
  if (OB_UNLIKELY(SERVER_SEND_REQUEST != s.current_.send_action_)) {
    //LOG_WARN("handle_resultset_resp, not expected");
    consume_response_packet(s);
  }
}

inline void ObMysqlTransact::handle_ok_resp(ObTransState &s)
{
  int ret = OB_SUCCESS;
  // in fact, we handle ok, eof, resultset and other responses in this function
  bool is_user_request = false;

  if (NULL != s.sm_->client_session_ && NULL != s.sm_->get_server_session()) {
    ObClientSessionInfo &client_info = get_client_session_info(s);
    ObServerSessionInfo &server_info = get_server_session_info(s);

    switch(s.current_.send_action_) {
      case SERVER_SEND_LOGIN:
        is_user_request = true;
        break;
      case SERVER_SEND_REQUEST:
        handle_user_request_succ(s);
        is_user_request = true;
        break;

      case SERVER_SEND_SAVED_LOGIN:
        handle_saved_login_succ(s);
        break;

      case SERVER_SEND_ALL_SESSION_VARS:
      case SERVER_SEND_SESSION_VARS:
        ObProxySessionInfoHandler::assign_session_vars_version(client_info, server_info);
        // send session var function will send last_insert_id in passing
        ObProxySessionInfoHandler::assign_last_insert_id_version(client_info, server_info);
        // send session var function will send safe_read_snapshot if need
        client_info.set_syncing_safe_read_snapshot(0);
        break;

      case SERVER_SEND_USE_DATABASE:
        handle_use_db_succ(s);
        break;

      case SERVER_SEND_LAST_INSERT_ID:
        ObProxySessionInfoHandler::assign_last_insert_id_version(client_info, server_info);
        break;

      case SERVER_SEND_START_TRANS:
        // reset hold start trans flag if receive ok packet
        s.is_hold_start_trans_ = false;
        break;

      case SERVER_SEND_TEXT_PS_PREPARE:
        handle_text_ps_prepare_succ(s);
        break;

      case SERVER_SEND_HANDSHAKE:
      case SERVER_SEND_NONE:
      default:
        s.current_.state_ = INTERNAL_ERROR;
        LOG_ERROR("unknown send action", "action", s.current_.send_action_,
                  "request_sql", s.trans_info_.get_print_sql(),
                  "result", s.trans_info_.server_response_.get_analyze_result(),
                  K(client_info), K(server_info));
        break;
    }
  }

  if (obmysql::OB_MYSQL_COM_STMT_SEND_PIECE_DATA == s.trans_info_.sql_cmd_) {
    ObPieceInfo *info = NULL;
    ObClientSessionInfo &cs_info = get_client_session_info(s);
    if (OB_FAIL(cs_info.get_piece_info(info))) {
      if (OB_HASH_NOT_EXIST == ret) {
        if (OB_ISNULL(info = op_alloc(ObPieceInfo))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          PROXY_API_LOG(ERROR, "fail to allocate memory for piece info", K(ret));
        } else {
          info->set_ps_id(cs_info.get_client_ps_id());
          info->set_addr(s.sm_->get_server_session()->get_netvc()->get_remote_addr());
          if (OB_FAIL(cs_info.add_piece_info(info))) {
            PROXY_API_LOG(WARN, "fail to add piece info", K(ret));
            op_free(info);
            info = NULL;
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
      s.current_.state_ = INTERNAL_ERROR;
      LOG_WARN("add piece info failed", K(ret));
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

    if (s.mysql_config_params_->enable_client_ip_checkout_) {
      if (OB_UNLIKELY(!client_addr.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("real client add is invalid, it should not happened", K(client_addr), K(ret));
      }
    } else {
      client_addr.reset();
    }
    if (OB_SUCC(ret)) {
      if (s.sm_->client_session_->get_session_info().is_oceanbase_server()) {
        if (OB_FAIL(handle_oceanbase_handshake_pkt(s, conn_id, client_addr))) {
          LOG_WARN("fail to handle oceanbase handshke pkt", K(conn_id), K(client_addr), K(ret));
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
  bool use_compress = s.mysql_config_params_->enable_compression_protocol_;
  bool use_ob_protocol_v2 = s.mysql_config_params_->enable_ob_protocol_v2_;
  bool use_ssl = !s.sm_->client_session_->is_proxy_mysql_client_
                 && s.trans_info_.server_response_.get_analyze_result().support_ssl()
                 && get_global_ssl_config_table_processor().is_ssl_supported(
                    s.sm_->client_session_->get_vip_cluster_name(),
                    s.sm_->client_session_->get_vip_cluster_name(),
                    false);
  ObRespAnalyzeResult &result = s.trans_info_.server_response_.get_analyze_result();
  const ObString &server_scramble = result.get_scramble_string();
  const ObString &proxy_scramble = client_session->get_scramble_string();
  if (!s.mysql_config_params_->is_mysql_routing_mode()) {
    //if not mysql routing mode, we need rewrite the first login request with newest connection id
    ObString user_cluster_name;
    int64_t cluster_id = OB_DEFAULT_CLUSTER_ID;
    if (OB_LIKELY(s.mysql_config_params_->enable_cluster_checkout_)) {
      user_cluster_name = client_session->get_real_cluster_name();
      if (OB_NOT_NULL(s.sm_->sm_cluster_resource_)) {
        cluster_id = s.sm_->sm_cluster_resource_->get_cluster_id();
      }
    }
    //if global_vars_version was inited, this must saved login
    if (client_session->get_session_info().is_first_login_succ()) {
      // if client vc create new connection, will change current conn id to new conn id
      // from the newest handshake packet, mainly to avoid Session Already Exist
      if (OB_FAIL(ObProxySessionInfoHandler::rewrite_saved_login_req(
                  client_session->get_session_info(),
                  user_cluster_name,
                  cluster_id,
                  conn_id,
                  client_session->get_proxy_sessid(),
                  server_scramble,
                  proxy_scramble,
                  client_addr,
                  use_compress,
                  use_ob_protocol_v2,
                  use_ssl))) {
        LOG_WARN("fail to handle rewrite saved hrs pkt, INTERNAL_ERROR", K(ret));
      } else {
        LOG_DEBUG("succ to rewrite saved hrs pkt");
      }
    } else {
      if (OB_FAIL(ObProxySessionInfoHandler::rewrite_first_login_req(
                  client_session->get_session_info(),
                  user_cluster_name,
                  cluster_id,
                  conn_id,
                  client_session->get_proxy_sessid(),
                  server_scramble,
                  proxy_scramble,
                  client_addr,
                  use_compress,
                  use_ob_protocol_v2,
                  use_ssl))) {
        LOG_WARN("fail to handle rewrite first hrs pkt, INTERNAL_ERROR", K(ret));
      } else {
        LOG_DEBUG("succ to rewrite first hrs pkt");
      }
    }
  } else {
    //use orig login req, no need to rewrite it
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
      LOG_ERROR("[ObMysqlTransact::handle_db_reset]",
                "bound_server_session", client_session->get_server_session());
    } else if ((client_session->get_cur_server_session() != client_session->get_lii_server_session())
               && NULL != client_session->get_lii_server_session()) {
      ret = OB_INNER_STAT_ERROR;
      LOG_ERROR("[ObMysqlTransact::handle_db_reset]",
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
        LOG_ERROR("[ObMysqlTransact::handle_db_reset] failed to remove database", K(ret));
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

  // handle resp
  if (resp.is_resultset_resp()) {
    MYSQL_INCREMENT_TRANS_STAT(SERVER_RESULTSET_RESPONSES);
    handle_resultset_resp(s);
  } else if (resp.is_ok_resp()) {
    MYSQL_INCREMENT_TRANS_STAT(SERVER_OK_RESPONSES);
    handle_ok_resp(s);
  } else if (resp.is_error_resp()) {
    MYSQL_INCREMENT_TRANS_STAT(SERVER_ERROR_RESPONSES);
    handle_error_resp(s);
  } else if (resp.is_handshake_pkt()) {
    MYSQL_INCREMENT_TRANS_STAT(SERVER_OTHER_RESPONSES);
    handle_handshake_pkt(s);
  } else {
    MYSQL_INCREMENT_TRANS_STAT(SERVER_OTHER_RESPONSES);
    handle_ok_resp(s);
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
  if (OB_FAIL(ObMysqlTransact::build_error_packet(trans_state))) {
    LOG_WARN("fail to build err packet", K(ret));
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
        LOG_ERROR("fail to alloc mem for ObTraceStats", "alloc_size", sizeof(ObTraceStats), K(ret));
      } else if (OB_ISNULL(trace_stats = new (buf) ObTraceStats())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("failed to placement new for ObTraceStats", K(ret));
        op_fixed_mem_free(buf, sizeof(ObTraceStats));
        buf = NULL;
      }
    }

    if (OB_LIKELY(NULL != trace_stats)) {
      ObTraceRecord *trace_record = NULL;
      if (OB_FAIL(trace_stats->get_current_record(trace_record))) {
        PROXY_TXN_LOG(WARN, "fail to get current record", KPC(trace_stats),
                      "cs_id", cs.get_cs_id(), "proxy_sessid", cs.get_proxy_sessid(), K(ret));
      } else {
        trace_record->attempts_ = static_cast<int8_t>(s.current_.attempts_);
        trace_record->pl_attempts_ = static_cast<int8_t>(s.pll_info_.pl_attempts_);
        trace_record->server_state_ = static_cast<int8_t>(s.current_.state_);
        trace_record->send_action_ = static_cast<int8_t>(s.current_.send_action_);
        trace_record->resp_error_ = static_cast<int8_t>(s.current_.error_type_);
        trace_record->addr_.set_ipv4_addr(s.server_info_.get_ipv4(), s.server_info_.get_port());
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
    case REQUEST_SERVER_STOPPING_ERROR: {
      // before retry send request, write the saved packet to client buffer
      ObString req_pkt = s.trans_info_.client_request_.get_req_pkt();
      int64_t pkt_len = req_pkt.length();
      ObIOBufferReader *client_reader = s.sm_->get_client_buffer_reader();
      int64_t written_len = 0;
      if (OB_ISNULL(client_reader)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[ObMysqlTransact::handle_server_resp_error] client reader is NULL");
      } else {
        if (0 != client_reader->read_avail() || req_pkt.empty()) {
          ret = OB_INNER_STAT_ERROR;
          LOG_ERROR("[ObMysqlTransact::handle_server_resp_error] invalid internal state",
                    "read_avail", client_reader->read_avail(), K(req_pkt));
        } else if (OB_FAIL(client_reader->mbuf_->write(req_pkt.ptr(), pkt_len, written_len))) {
          LOG_WARN("[ObMysqlTransact::handle_server_resp_error] "
                   "fail to write login packet into client iobuffer",
                   "request_length", pkt_len, K(written_len), K(ret));
        } else if (OB_UNLIKELY(pkt_len != written_len)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("request packet length must be equal with written len",
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
    case START_TRANS_COMMON_ERROR:
    case SYNC_DATABASE_COMMON_ERROR: {
      //current server session is still ok
      s.sm_->clear_server_entry();
      handle_retry_server_connection(s);
      break;
    }
    case SYNC_PREPARE_COMMON_ERROR:
      // no need retry, just disconnect connection for sync_prepare_error
      handle_server_connection_break(s);
      break;
    case ORA_FATAL_ERROR:
      LOG_ERROR("ob ora fatal error",
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
        LOG_ERROR("ora fatal error", K(client_ps_id), K(server_ps_id), KPC(ps_id_pair), KPC(ps_entry));
      }
      handle_server_connection_break(s);
      break;
    case STANDBY_WEAK_READONLY_ERROR:
      LOG_WARN("primary cluster has switchover to standby, client session will be destroyed");
      handle_server_connection_break(s);
      break;
    default: {
      ret = OB_INNER_STAT_ERROR;
      LOG_ERROR("unexpect error!, unknown error type", K(s.current_.error_type_));
      break;
    }
  }

  if (OB_FAIL(ret)) {
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
    case STATE_UNDEFINED:
      // fall through
    case INACTIVE_TIMEOUT:
      // fall through
    case ANALYZE_ERROR:
      // fall through
    case INTERNAL_ERROR:
      // fall through
    case CONNECTION_CLOSED: {
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
        } else {
          LOG_WARN("connection error",
                   "server_state", ObMysqlTransact::get_server_state_name(s.current_.state_),
                   "request_cmd", get_mysql_cmd_str(s.trans_info_.client_request_.get_packet_meta().cmd_),
                   "sql_cmd", get_mysql_cmd_str(s.trans_info_.sql_cmd_),
                   "sql", s.trans_info_.get_print_sql());
        }
      }

      handle_server_connection_break(s);
      break;
    }
      // not happen
    case CMD_COMPLETE:
    case TRANSACTION_COMPLETE:
    default:
      handle_server_connection_break(s);
      LOG_ERROR("s.current_.state_ is set to something unsupported", K(s.current_.state_));
      break;
  }
}

void ObMysqlTransact::handle_oceanbase_retry_server_connection(ObTransState &s)
{
  int ret = OB_SUCCESS;
  bool second_in = false;
  ObPieceInfo *info = NULL;
  ObSSRetryStatus retry_status = NO_NEED_RETRY;
  const int64_t max_connect_attempts = get_max_connect_attempts(s);

  ObIpEndpoint old_target_server;
  net::ops_ip_copy(old_target_server, s.server_info_.addr_.sa_);

  int64_t obproxy_route_addr = 0;
  if (NULL != s.sm_ && NULL != s.sm_->client_session_) {
    obproxy_route_addr = s.sm_->client_session_->get_session_info().get_obproxy_route_addr();
  }

  if (OB_FAIL(s.sm_->get_client_session()->get_session_info().get_piece_info(info))) {
    // do nothing
  } else {
    second_in = true;
  }
  // in mysql mode, no need retry
  if (OB_UNLIKELY(s.mysql_config_params_->is_mysql_routing_mode())) {
    LOG_DEBUG("in mysql mode, no need retry, will disconnect");
    retry_status = NO_NEED_RETRY;

  // we will update retry_status only the follow all happened
  // 1. not in transaction
  // 2. attempts_ is less then max_connect_attempts
  // 3. is not kill query
  // 4. is not piece info
  } else if (!is_in_trans(s)
             && s.current_.attempts_ < max_connect_attempts
             && 0 == obproxy_route_addr
             && !s.trans_info_.client_request_.is_kill_query()
             && obmysql::OB_MYSQL_COM_STMT_FETCH != s.trans_info_.sql_cmd_
             && obmysql::OB_MYSQL_COM_STMT_GET_PIECE_DATA != s.trans_info_.sql_cmd_
             && !second_in) {
    ++s.current_.attempts_;
    LOG_DEBUG("start next retry");

    if (s.pll_info_.is_force_renew()) { // force pl lookup
      retry_status = NOT_FOUND_EXISTING_ADDR;
    } else if (REQUEST_REROUTE_ERROR == s.current_.error_type_) {
      retry_status = FOUND_EXISTING_ADDR;
      net::ops_ip_copy(s.server_info_.addr_, s.reroute_info_.replica_.server_.get_ipv4(),
                       static_cast<uint16_t>(s.reroute_info_.replica_.server_.get_port()));
    } else {
      retry_status = retry_server_connection_not_open(s);
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
              KPC_(s.congestion_entry));
  }

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
    s.congestion_lookup_success_ = false;
    s.need_congestion_lookup_ = true;

    LOG_DEBUG("FOUND_EXISTING_ADDR, Retrying...",
              "attempts now", s.current_.attempts_,
              K(max_connect_attempts),
              "retry observer", s.server_info_.addr_,
              "force_retry_congested", s.force_retry_congested_);
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
    LOG_DEBUG("NO_NEED_RETRY, No more retries, will disconnect soon.");
    handle_server_connection_break(s);
  } else {
    LOG_ERROR("error, never run here");
    handle_server_connection_break(s);
  }
}

void ObMysqlTransact::handle_retry_server_connection(ObTransState &s)
{
  if (s.sm_->client_session_->get_session_info().is_oceanbase_server()) {
    handle_oceanbase_retry_server_connection(s);
  } else {
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
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown type", K(type), KPC_(client_session->dummy_entry), K(sm_id), K(ret));
      }
    }
  } else {
    ret = OB_INVALID_ERROR;
    LOG_WARN("invalid dummy entry", "pll_info", s.pll_info_, K(ret));
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

ObMysqlTransact::ObSSRetryStatus ObMysqlTransact::retry_server_connection_not_open(ObTransState &s)
{
  int ret = OB_SUCCESS;
  ObSSRetryStatus ret_status = NO_NEED_RETRY;

  if (OB_UNLIKELY(CONNECTION_ALIVE == s.current_.state_)
      || OB_UNLIKELY(ACTIVE_TIMEOUT == s.current_.state_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_ERROR("[ObMysqlTransact::retry_server_connection_not_open] unexpected current server state",
              "server_state", ObMysqlTransact::get_server_state_name(s.current_.state_));
  } else {
    ObRespAnalyzeResult &resp = s.trans_info_.server_response_.get_analyze_result();
    // when use database error(ERROR 1049 (42000): Unknown database 'xxx') in innner request,
    // do not retry build new server session.
    if ((SERVER_SEND_USE_DATABASE == s.current_.send_action_) && resp.is_bad_db_error()) {
      ret_status = NO_NEED_RETRY;
      LOG_INFO("[ObMysqlTransact::retry_server_connection_not_open]"
               "database was dropped by other client session, no need to build new "
               "server session, ret_status = NO_NEED_RETRY");
    } else {
      // handle last chance retry
      if (s.current_.attempts_ == get_max_connect_attempts(s)) {
        handle_retry_last_time(s);
      }

      if (s.pll_info_.replica_size() > 0) {
        int32_t attempt_count = 0;
        //as leader must first used, here found_leader_force_congested is usefull
        bool found_leader_force_congested = false;
        const ObProxyReplicaLocation *replica = s.pll_info_.get_next_avail_replica(
            s.force_retry_congested_, attempt_count, found_leader_force_congested);
        s.current_.attempts_ += attempt_count;

        if ((NULL == replica) && !s.pll_info_.is_all_iterate_once()) { // just defense
          LOG_ERROR("expected error", K_(s.pll_info));
          ret_status = NO_NEED_RETRY;
        } else {
          if ((NULL == replica) && s.pll_info_.is_all_iterate_once()) { // next round
            // if we has tried all servers, do force retry in next round
            s.pll_info_.reset_cursor();
            s.force_retry_congested_ = true;

            // get replica again
            replica = s.pll_info_.get_next_avail_replica();
          }

          if ((NULL != replica) && replica->is_valid()) {
            s.server_info_.set_addr(replica->server_.get_ipv4(), static_cast<uint16_t>(replica->server_.get_port()));
            ret_status = FOUND_EXISTING_ADDR;
          } else {
            // unavailalbe replica, just defense
            LOG_ERROR("can not found avail replica, unexpected branch", KPC(replica));
            replica = NULL;
            s.force_retry_congested_ = true;
            ret_status = NOT_FOUND_EXISTING_ADDR;
          }

          // if table entry is all dummy entry and comes from rslist
          // and all of servers are not in server list, we will disconnect and delete this cluster resource
          if (s.congestion_entry_not_exist_count_ == s.pll_info_.replica_size()
              && s.pll_info_.is_server_from_rslist()) {
            LOG_WARN("all servers of route which comes from rslist are not in congestion list,"
                     "will disconnect and delete this cluster resource ",
                     "route_info", s.pll_info_.route_);
            s.sm_->client_session_->set_need_delete_cluster();
            ret_status = NO_NEED_RETRY;
          }
        }
      } else {
        ObTableEntryName &te_name = s.pll_info_.te_name_;
        ret_status = NOT_FOUND_EXISTING_ADDR;
        LOG_INFO("no replica, retry to get table location again, "
                 "maybe used last session, ret_status = NOT_FOUND_EXISTING_ADDR", K(te_name));
      }
    }
  }

  if (OB_FAIL(ret)) {
    ret_status = NO_NEED_RETRY;
  }

  return ret_status;
}

inline void ObMysqlTransact::handle_server_connection_break(ObTransState &s)
{
  if (NULL != s.sm_->client_session_) {
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
    }
  }

  if (OB_UNLIKELY(CONNECTION_ALIVE == s.current_.state_)) {
    LOG_ERROR("[ObMysqlTransact::handle_server_connection_break] unexpected current server state",
              "server_state", ObMysqlTransact::get_server_state_name(s.current_.state_));
  }

  MYSQL_INCREMENT_TRANS_STAT(BROKEN_SERVER_CONNECTIONS);

  s.next_action_ = SM_ACTION_SEND_ERROR_NOOP;
}

void ObMysqlTransact::handle_on_forward_server_response(ObTransState &s)
{
  LOG_DEBUG("[ObMysqlTransact::handle_on_forward_server_response]",
            "cur_send_action", ObMysqlTransact::get_send_action_name(s.current_.send_action_));

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
        bool is_server_ssl_supported = get_global_ssl_config_table_processor().is_ssl_supported(
          s.sm_->client_session_->get_vip_cluster_name(), s.sm_->client_session_->get_vip_tenant_name(), false);
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
            LOG_WARN("[ObMysqlTransact::handle_on_forward_server_response], "
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
      ObRespAnalyzeResult &resp = s.trans_info_.server_response_.get_analyze_result();
      if (resp.is_error_resp()) {
        s.next_action_ = SM_ACTION_SERVER_READ;
        if (client_info.is_oceanbase_server()) {
          s.sm_->api_.do_response_transform_open();
        }
        break;
      } else if (client_info.is_oceanbase_server()) {
        bool is_proxy_mysql_client_ = s.sm_->client_session_->is_proxy_mysql_client_;
        if (client_info.need_reset_all_session_vars() && !is_proxy_mysql_client_) {
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
        if (obmysql::OB_MYSQL_COM_STMT_CLOSE == s.trans_info_.client_request_.get_packet_meta().cmd_) {
          // no need sync var, send to server directly
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
        LOG_WARN("[ObMysqlTransact::handle_on_forward_server_response], "
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
        if (client_info.need_reset_session_vars(server_info)) {
          s.current_.send_action_ = SERVER_SEND_SESSION_VARS;
          s.next_action_ = SM_ACTION_API_SEND_REQUEST;
          break;
        } else {
          // fall through:
        }
      } else {
        s.current_.state_ = INTERNAL_ERROR;
        handle_server_connection_break(s);
        LOG_WARN("[ObMysqlTransact::handle_on_forward_server_response], "
                 "client session or server session is NULL",
                 "next_action", ObMysqlTransact::get_action_name(s.next_action_),
                 K(s.sm_->client_session_), "server_session", s.sm_->get_server_session());
        break;
      }
      __attribute__ ((fallthrough));
    case SERVER_SEND_SESSION_VARS:
    case SERVER_SEND_LAST_INSERT_ID:
    case SERVER_SEND_PREPARE:
    case SERVER_SEND_TEXT_PS_PREPARE:
    case SERVER_SEND_START_TRANS: {
      if (OB_LIKELY(NULL != s.sm_->client_session_) && OB_LIKELY(NULL != s.sm_->get_server_session())) {
        ObClientSessionInfo &client_info = get_client_session_info(s);
        ObServerSessionInfo &server_info = get_server_session_info(s);
        obmysql::ObMySQLCmd cmd = s.trans_info_.client_request_.get_packet_meta().cmd_;
        //obutils::ObSqlParseResult &sql_result = s.trans_info_.client_request_.get_parse_result();
        if (client_info.need_reset_last_insert_id(server_info)) {
          // TODO: current version proxy parse can't judge last_insert_id exactly,
          // so we do not judge, whether sql_reuslt has_last_insert_id here
          // if it is large request, we do not parse the sql, we don't know whether the sql contains
          // last_insert_id, in this case we will sync last_insert_id if need
          s.current_.send_action_ = SERVER_SEND_LAST_INSERT_ID;
        } else if (s.is_hold_start_trans_) {
          s.current_.send_action_ = SERVER_SEND_START_TRANS;
        } else if ((obmysql::OB_MYSQL_COM_STMT_EXECUTE == cmd
                   || obmysql::OB_MYSQL_COM_STMT_SEND_PIECE_DATA == cmd
                   || obmysql::OB_MYSQL_COM_STMT_SEND_LONG_DATA == cmd)
                   && client_info.need_do_prepare(server_info)) {
          s.current_.send_action_ = SERVER_SEND_PREPARE;
        } else if (client_info.is_text_ps_execute() && client_info.need_do_text_ps_prepare(server_info)) {
          s.current_.send_action_ = SERVER_SEND_TEXT_PS_PREPARE;
        } else {
          s.current_.send_action_ = SERVER_SEND_REQUEST;
        }

        if ((SERVER_SEND_REQUEST == s.current_.send_action_)
            && (s.trans_info_.request_content_length_ > 0)) { // large request
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
        LOG_WARN("[ObMysqlTransact::handle_on_forward_server_response], "
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
      s.next_action_ = SM_ACTION_SERVER_READ;
      if (get_client_session_info(s).is_oceanbase_server()) {
        s.sm_->api_.do_response_transform_open();
      }
      break;
    }
    case SERVER_SEND_NONE:
    default:
      s.current_.state_ = INTERNAL_ERROR;
      handle_server_connection_break(s);
      LOG_ERROR("Unknown server send next action", K(s.current_.send_action_));
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

    case SERVER_SEND_LAST_INSERT_ID:
      s.sm_->cmd_time_stats_.server_send_last_insert_id_time_ += cost;
      s.sm_->cmd_time_stats_.server_sync_session_variable_time_ += cost;
      MYSQL_SUM_TIME_STAT(TOTAL_SEND_LAST_INSERT_ID_TIME, cost);
      MYSQL_INCREMENT_TRANS_STAT(SEND_LAST_INSERT_ID_REQUESTS);
      break;

    case SERVER_SEND_START_TRANS:
      s.sm_->cmd_time_stats_.server_send_start_trans_time_ += cost;
      s.sm_->cmd_time_stats_.server_sync_session_variable_time_ += cost;
      MYSQL_SUM_TIME_STAT(TOTAL_SEND_START_TRANS_TIME, cost);
      MYSQL_INCREMENT_TRANS_STAT(SEND_START_TRANS_REQUESTS);
      break;

    case SERVER_SEND_HANDSHAKE:
    case SERVER_SEND_LOGIN:
    case SERVER_SEND_REQUEST:
    case SERVER_SEND_PREPARE:
    case SERVER_SEND_TEXT_PS_PREPARE:
    case SERVER_SEND_NONE:
      break;

    default:
      LOG_ERROR("Unknown server send next action", K(s.current_.send_action_));
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
              LOG_WARN("zone is readonly or encryption, but server tell error response, "
                       "maybe this new server do not support old agreement, try next server",
                       "zone_type", zone_type_to_str(s.pll_info_.route_.cur_chosen_server_.zone_type_),
                       "origin_name", s.pll_info_.te_name_,
                       "sql_cmd", get_mysql_cmd_str(s.trans_info_.sql_cmd_),
                       "sql", s.trans_info_.client_request_.get_print_sql(),
                       "route info", s.pll_info_.route_);
            } else {
              LOG_WARN("zone is readonly, proxy should not send request to it, "
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
      LOG_ERROR("s.current_.state_ is set to something unsupported", K(s.current_.state_));
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
    LOG_ERROR("failed to allocate stat block", K(ret));
  } else {
    new_block->reset();
    s.current_stats_->next_ = new_block;
    s.current_stats_ = new_block;
    LOG_DEBUG("Adding new large stat block");
  }
  return ret;
}

int ObMysqlTransact::build_error_packet(ObTransState &s)
{
  int ret = OB_SUCCESS;
  if (0 == s.mysql_errcode_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no mysql errcode", K(ret), K_(s.mysql_errcode));
  }

  ObMIOBuffer *buf = NULL;
  if (OB_SUCC(ret)) {
    if (NULL != s.internal_buffer_) {
      buf = s.internal_buffer_;
    } else {
      if (OB_FAIL(s.alloc_internal_buffer(MYSQL_BUFFER_SIZE))) {
        LOG_ERROR("[ObMysqlTransact::build_error_packet] fail to allocate internal buffer", K(ret));
      } else {
        buf = s.internal_buffer_;
      }
    }
  }

  if (OB_SUCC(ret)) {
    int errcode = s.mysql_errcode_;
    s.inner_errcode_ = s.mysql_errcode_;
    uint8_t next_seq = 0;
    if (s.is_auth_request_) {
      if ((NULL != s.sm_) && (NULL != s.sm_->client_session_)) {
        ObMysqlAuthRequest &auth_req = s.sm_->client_session_->get_session_info().get_login_req();
        next_seq = static_cast<uint8_t>(auth_req.get_packet_meta().pkt_seq_ + 1);
      }
    } else {
      ObProxyMysqlRequest &client_request = s.trans_info_.client_request_;
      next_seq = static_cast<uint8_t>(client_request.get_packet_meta().pkt_seq_ + 1);
    }

    if (NULL != s.mysql_errmsg_) {
      const char *errmsg = s.mysql_errmsg_;
      if (OB_FAIL(ObMysqlPacketUtil::encode_err_packet_buf(*buf, next_seq, errcode, errmsg))) {
        LOG_WARN("[ObMysqlTransact::build_error_packet] fail to encode err pacekt buf",
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
          if ((NULL != s.sm_) && (NULL != s.sm_->client_session_)) {
            ObClientSessionInfo &client_info = s.sm_->client_session_->get_session_info();
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
            LOG_WARN("fail to fill err_msg", K(ret));
          } else if (OB_FAIL(ObMysqlPacketUtil::encode_err_packet_buf(*buf, next_seq, errcode, errmsg))) {
            LOG_WARN("[ObMysqlTransact::build_error_packet] fail to encode err pacekt buf",
                     K(next_seq), K(errcode), K(ret));
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
        case OB_NOT_SUPPORTED: {
          if (OB_FAIL(ObMysqlPacketUtil::encode_err_packet(*buf, next_seq, errcode))) {
            LOG_WARN("[ObMysqlTransact::build_error_packet] fail to build not supported err resp", K(ret));
          }
          break;
        }
        // add others...
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unhandled errcode", K(errcode), K(ret));
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
inline ObRoutePolicyEnum ObMysqlTransact::ObTransState::get_route_policy(ObMysqlClientSession &cs, const bool need_use_dup_replica)
{
  ObRoutePolicyEnum ret_policy = READONLY_ZONE_FIRST;
  ObRoutePolicyEnum session_route_policy = READONLY_ZONE_FIRST;
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
      PROXY_TXN_LOG(WARN, "unknown route policy, use default policy",
                    "ob_route_policy", cs.get_session_info().get_route_policy(),
                    "session_route_policy", get_route_policy_enum_string(session_route_policy));
  }

  if (need_use_dup_replica) {
    //if dup_replica read, use DUP_REPLICA_FIRST, no need care about zone type
    ret_policy = DUP_REPLICA_FIRST;
  } else if (cs.dummy_ldc_.is_readonly_zone_exist()) {
    if (common::WEAK == get_trans_consistency_level(cs.get_session_info())) {
      //if wead read, use session_route_policy
      ret_policy = session_route_policy;
    } else if (is_request_readonly_zone_support(cs.get_session_info())) {
      if (cs.get_session_info().is_read_consistency_set()) {
        if (common::WEAK == static_cast<ObConsistencyLevel>(cs.get_session_info().get_read_consistency())) {
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
    if (common::WEAK == get_trans_consistency_level(cs.get_session_info())) {
      if (is_valid_proxy_route_policy(cs.get_session_info().get_proxy_route_policy()) 
        && cs.get_session_info().is_proxy_route_policy_set()) {
        get_route_policy(cs.get_session_info().get_proxy_route_policy(), ret_policy);
      } else {
        const ObString value = get_global_proxy_config().proxy_route_policy.str();
        ObProxyRoutePolicyEnum policy = get_proxy_route_policy(value);
        PROXY_CS_LOG(DEBUG, "succ to global variable proxy_route_policy",
                 "policy", get_proxy_route_policy_enum_string(policy));
        get_route_policy(policy, ret_policy);
      }
    }
  }
  PROXY_TXN_LOG(DEBUG, "succ to get route_policy",
                "session_route_policy", get_route_policy_enum_string(session_route_policy),
                "ret_policy", get_route_policy_enum_string(ret_policy));

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

    case ObMysqlTransact::SYNC_DATABASE_COMMON_ERROR:
      ret = "SYNC_DATABASE_COMMON_ERROR";
      break;

    case ObMysqlTransact::SYNC_PREPARE_COMMON_ERROR:
      ret = "SYNC_PREPARE_COMMON_ERROR";
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

    case ObMysqlTransact::SERVER_SEND_START_TRANS:
      ret = "SERVER_SEND_START_TRANS";
      break;

    case ObMysqlTransact::SERVER_SEND_LAST_INSERT_ID:
      ret = "SERVER_SEND_LAST_INSERT_ID";
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
                  || (s.trans_info_.client_request_.get_parse_result().is_show_tables_stmt()
                      && !is_single_shard_db_table(s))))
          || (s.is_trans_first_request_
              && s.trans_info_.client_request_.get_parse_result().need_hold_start_trans())
          || s.trans_info_.client_request_.get_parse_result().is_internal_request()
          || is_bad_route_request(s))
          || !is_supported_mysql_cmd(s.trans_info_.sql_cmd_)
          || (get_client_session_info(s).is_read_only_user()
              && s.trans_info_.client_request_.get_parse_result().is_set_tx_read_only())
          || (get_client_session_info(s).is_request_follower_user()
              &&s.trans_info_.client_request_.get_parse_result().is_set_ob_read_consistency());
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
