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

#include "proxy/shard/obproxy_shard_utils.h"
#include "proxy/mysqllib/ob_mysql_request_builder.h"
#include "obproxy/obutils/ob_resource_pool_processor.h"
#include "lib/oblog/ob_log.h"
#include "lib/encrypt/ob_encrypted_helper.h"
#include "rpc/obmysql/packet/ompk_handshake.h"
#if HAVA_BEYONDTRUST
#include "obutils/ob_beyond_trust_processor.h"
#endif
#include "dbconfig/ob_proxy_db_config_info.h"
#include "utils/ob_proxy_blowfish.h"
#include "sql/session/ob_system_variable_alias.h"
#include "obutils/ob_proxy_config.h"
#include "optimizer/ob_sharding_select_log_plan.h"
#include "obutils/ob_proxy_stmt.h"
#include "optimizer/ob_proxy_optimizer_processor.h"
#include "lib/container/ob_se_array_iterator.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::dbconfig;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::sql;
using namespace oceanbase::obproxy::optimizer;


namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
static const char* DEFAULT_READ_CONSISTENCY = "STRONG";
int ObProxyShardUtils::check_logic_database(ObMysqlTransact::ObTransState &trans_state,
                                            ObMysqlClientSession &client_session,
                                            const ObString &db_name)
{
  int ret = OB_SUCCESS;

  ObClientSessionInfo &session_info = client_session.get_session_info();
  ObShardConnector *shard_conn = NULL;
  ObShardConnector *prev_shard_conn = session_info.get_shard_connector();
  ObDbConfigLogicDb *db_info = NULL;
  ObString logic_tenant_name;
  const ObString &username = session_info.get_origin_username();
  const ObString &host = session_info.get_client_host();
  ObShardUserPrivInfo &saved_up_info = session_info.get_shard_user_priv();

  if (OB_FAIL(session_info.get_logic_tenant_name(logic_tenant_name))) {
    LOG_WDIAG("fail to get_logic_tenant_name", K(ret));
  } else if (OB_ISNULL(db_info = get_global_dbconfig_cache().get_exist_db_info(logic_tenant_name, db_name))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WDIAG("logic db does not exist", K(ret), K(db_name), K(logic_tenant_name));
  } else if (session_info.enable_shard_authority()) {
    ObShardUserPrivInfo up_info;
    if (OB_FAIL(db_info->get_user_priv_info(username, host, up_info))) {
      LOG_WDIAG("fail to get user priv info", K(db_name), K(username), K(host), K(ret));
    } else if (saved_up_info.password_stage2_ != up_info.password_stage2_) {
      ret = OB_PASSWORD_WRONG;
      LOG_WDIAG("pass word is wrong", K(saved_up_info), K(up_info), K(ret));
    } else {
      saved_up_info.assign(up_info);
    }
    if (OB_FAIL(ret)) {
      ret = OB_ERR_NO_DB_PRIVILEGE;
    }
  } else if (!client_session.is_local_connection() && !db_info->enable_remote_connection()) {
    ret = OB_ERR_NO_DB_PRIVILEGE;
    LOG_WDIAG("Access denied for database from remote addr", K(db_name), K(ret));
  }

  //有的 SQL 不需要改写, 可以直接依赖上一次的 DBKey, 切换逻辑库以后, 也随机选一个, 避免使用到上一个逻辑库的
  if (OB_SUCC(ret)) {
    if (OB_FAIL(db_info->acquire_random_shard_connector(shard_conn))) {
      LOG_WDIAG("fail to get random shard connector", K(ret), KPC(db_info));
    } else if (*prev_shard_conn != *shard_conn) {
      if (OB_FAIL(change_connector(*db_info, client_session, trans_state, prev_shard_conn, shard_conn))) {
        LOG_WDIAG("fail to change connector", KPC(prev_shard_conn), KPC(shard_conn), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    session_info.set_group_id(OBPROXY_MAX_DBMESH_ID);
    session_info.set_table_id(OBPROXY_MAX_DBMESH_ID);
    session_info.set_es_id(OBPROXY_MAX_DBMESH_ID);
    session_info.set_logic_database_name(db_name);
    LOG_DEBUG("check logic database success", K(db_name));
  }

  if (NULL != shard_conn) {
    shard_conn->dec_ref();
    shard_conn = NULL;
  }

  if (NULL != db_info) {
    db_info->dec_ref();
    db_info = NULL;
  }
  return ret;
}

int ObProxyShardUtils::check_logic_db_priv_for_cur_user(const ObString &logic_tenant_name,
                                                        ObMysqlClientSession &client_session,
                                                        const ObString &db_name)
{
  int ret = OB_SUCCESS;

  ObDbConfigLogicDb *db_info = NULL;
  ObClientSessionInfo &session_info = client_session.get_session_info();
  const ObString &username = session_info.get_origin_username();
  const ObString &host = session_info.get_client_host();
  ObShardUserPrivInfo &saved_up_info = session_info.get_shard_user_priv();

  if (OB_ISNULL(db_info = get_global_dbconfig_cache().get_exist_db_info(logic_tenant_name, db_name))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WDIAG("logic db does not exist", K(ret), K(db_name), K(logic_tenant_name));
  } else if (session_info.enable_shard_authority()) {
    ObShardUserPrivInfo new_up_info;
    if (OB_FAIL(db_info->get_user_priv_info(username, host, new_up_info))) {
      LOG_WDIAG("fail to get user priv info", K(db_name), K(username), K(host), K(ret));
    } else if (saved_up_info.password_stage2_ != new_up_info.password_stage2_) {
      ret = OB_PASSWORD_WRONG;
      LOG_WDIAG("pass word is wrong", K(saved_up_info), K(new_up_info), K(ret));
    }
  } else if (!client_session.is_local_connection() && db_info->enable_remote_connection()) {
    ret = OB_ERR_NO_DB_PRIVILEGE;
    LOG_WDIAG("Access denied for database from remote addr", K(db_name), K(ret));
  }

  if (NULL != db_info) {
    db_info->dec_ref();
    db_info = NULL;
  }
  return ret;
}
int ObProxyShardUtils::change_connector(ObDbConfigLogicDb &logic_db_info,
                                        ObMysqlClientSession &client_session,
                                        ObMysqlTransact::ObTransState &trans_state,
                                        const ObShardConnector *prev_shard_conn,
                                        ObShardConnector *shard_conn)
{
  int ret = OB_SUCCESS;

  ObClientSessionInfo &session_info = client_session.get_session_info();

  if (!shard_conn->is_same_connection(prev_shard_conn)) {
    // 这里要用 ObMysqlTransact::is_in_trans, 比如 begin 后的第一条 SQL,
    // is_sharding_in_trans 会认为在事务中, 但是这里需要认为不在事务中, 否则会认为是分布式
    if (OB_UNLIKELY(ObMysqlTransact::is_in_trans(trans_state))) {
      ret = OB_ERR_DISTRIBUTED_NOT_SUPPORTED;
      LOG_WDIAG("not support distributed transaction", K(trans_state.current_.state_),
               K(trans_state.is_auth_request_),
               K(trans_state.is_hold_start_trans_),
               K(trans_state.is_hold_xa_start_),
               KPC(prev_shard_conn),
               KPC(shard_conn), K(ret));
    } else {
      session_info.set_allow_use_last_session(false);
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(handle_sys_read_consitency_prop(logic_db_info, *shard_conn, session_info))) {
      LOG_WDIAG("fail to handle_sys_read_consitency_prop", KPC(shard_conn));
    } else {
      ObShardProp* shard_prop = NULL;
      if (OB_FAIL(logic_db_info.get_shard_prop(shard_conn->shard_name_, shard_prop))) {
        LOG_DEBUG("fail to get shard prop", "shard name", shard_conn->shard_name_, K(ret));
        ret = OB_SUCCESS;
      } else {
        session_info.set_shard_prop(shard_prop);
      }
    }
  }

  if (OB_SUCC(ret)) {
    session_info.set_shard_connector(shard_conn);
    // 虽然分库分表不会同步 database, 但是从分库分表到单库单表,
    // 由于分库分表没同步, 当切换到单库单表时, 如果 dbkey 相同, 单库单表逻辑不设置 database, 会导致设置为错误的数据库
    session_info.set_database_name(shard_conn->database_name_.config_string_);

    if (DB_OB_MYSQL == shard_conn->server_type_ || DB_OB_ORACLE == shard_conn->server_type_) {
      bool is_cluster_changed = (prev_shard_conn->cluster_name_ != shard_conn->cluster_name_);
      bool is_tenant_changed = (prev_shard_conn->tenant_name_ != shard_conn->tenant_name_);
      if (OB_FAIL(change_user_auth(client_session, *shard_conn, is_cluster_changed, is_tenant_changed))) {
        LOG_EDIAG("fail to change user auth", KPC(shard_conn), K(is_cluster_changed), K(is_tenant_changed), K(ret));
      }
    }
  }

  return ret;
}

int ObProxyShardUtils::change_user_auth(ObMysqlClientSession &client_session,
                                        const ObShardConnector &shard_conn,
                                        const bool is_cluster_changed,
                                        const bool is_tenant_changed)
{
  int ret = OB_SUCCESS;
  ObClientSessionInfo &session_info = client_session.get_session_info();
  const ObString &full_user_name = shard_conn.full_username_.config_string_;
  const ObString &database = shard_conn.database_name_.config_string_;
  ObString password = shard_conn.password_.config_string_;
  const ObString &tenant_name = shard_conn.tenant_name_;
  const ObString &cluster_name = shard_conn.cluster_name_;

  // if cluster or tenant has bean changed, it is need to build new auth packet
  if (is_cluster_changed && NULL != client_session.cluster_resource_) {
    // decrease the reference of old cluster
    client_session.cluster_resource_->dec_ref();
    client_session.cluster_resource_ = NULL;
  }

  // tenant changed or cluster changed, both need to refresh tenant dummy entry
  if (is_cluster_changed || is_tenant_changed || NULL == client_session.dummy_entry_) {
    client_session.is_need_update_dummy_entry_ = true;
  }

  if (shard_conn.is_enc_beyond_trust() && OB_UNLIKELY(password.empty())) {
#if HAVA_BEYONDTRUST
    const ObString &shard_name = shard_conn.shard_name_.config_string_;
    ObBeyondTrustProcessor &bt_processor = get_global_beyond_trust_processor();
    char pwd_buf[OB_MAX_PASSWORD_LENGTH];
    memset(pwd_buf, 0, sizeof(pwd_buf));
    if (OB_FAIL(bt_processor.get_connector_info(shard_name, pwd_buf, OB_MAX_PASSWORD_LENGTH, false))) {
      LOG_WDIAG("fail to get connector info from beyond trust cache", K(shard_name), K(ret));
    } else {
      password.assign_ptr(pwd_buf, (ObString::obstr_size_t)strlen(pwd_buf));
    }
#else
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("not support beyond trust password", K(ret));
#endif
  }
  if (OB_FAIL(ObProxySessionInfoHandler::rewrite_login_req_by_sharding(session_info, full_user_name,
              password, database, tenant_name, cluster_name))) {
    LOG_WDIAG("fail to rewrite_login_req_by_sharding", K(full_user_name), K(tenant_name), K(cluster_name), K(ret));
  }

  return ret;
}

//外部逻辑会去掉引号, 所以这里面要加上引号
void ObProxyShardUtils::replace_oracle_table(ObSqlString &new_sql, const ObString &real_name,
                                             bool &hava_quoto, bool is_single_shard_db_table,
                                             bool is_database)
{
  if (is_database) {
    new_sql.append("\"", 1);
    new_sql.append(real_name);
    new_sql.append("\"", 1);
  } else {
    // 如果有引号, 带上引号
    if (hava_quoto) {
      new_sql.append("\"", 1);
      new_sql.append(real_name);
      new_sql.append("\"", 1);
    } else {
      // 如果没有引号
      if (is_single_shard_db_table) {
        new_sql.append(real_name);
      } else {
        new_sql.append("\"", 1);
        new_sql.append(real_name);
        new_sql.append("\"", 1);
      }
    }
  }

  hava_quoto = false;
}

int ObProxyShardUtils::rewrite_shard_dml_request(const ObString &sql,
                                                 ObSqlString &new_sql,
                                                 ObSqlParseResult &parse_result,
                                                 bool is_oracle_mode,
                                                 const ObHashMap<ObString, ObString> &table_name_map,
                                                 const ObString &real_database_name,
                                                 bool is_single_shard_db_table)
{
  int ret = OB_SUCCESS;

  const char *sql_ptr = sql.ptr();
  int64_t sql_len = sql.length();
  int64_t copy_pos = 0;

  ObProxyDMLStmt *dml_stmt = static_cast<ObProxyDMLStmt*>(parse_result.get_proxy_stmt());
  if (OB_ISNULL(dml_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected null select stmt", K(ret));
  } else {
    ObProxyDMLStmt::TablePosArray &table_pos_array = dml_stmt->get_table_pos_array();
    std::sort(table_pos_array.begin(), table_pos_array.end());

    for (int64_t i = 0; OB_SUCC(ret) && i < table_pos_array.count(); i++) {
      ObProxyExprTablePos &expr_table_pos = table_pos_array.at(i);
      ObProxyExprTable *expr_table = expr_table_pos.get_table_expr();

      if (OB_ISNULL(expr_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unexpected null expr table", K(ret));
      } else {
        int64_t table_pos = expr_table_pos.get_table_pos();
        int64_t database_pos = expr_table_pos.get_database_pos();

        ObString &database_name = expr_table->get_database_name();
        ObString &table_name = expr_table->get_table_name();
        uint64_t database_len = database_name.length();
        uint64_t table_len = table_name.length();

        ObString real_table_name;
        if (OB_FAIL(table_name_map.get_refactored(table_name, real_table_name))) {
          LOG_WDIAG("fail to get real table name", K(table_name), K(ret));
        } else if (OB_FAIL(rewrite_shard_request_table(sql_ptr, database_pos, database_len,
                                                       table_pos, table_len, real_table_name, real_database_name,
                                                       is_oracle_mode, is_single_shard_db_table, new_sql, copy_pos))) {
          LOG_WDIAG("fail to rewrite table", K(ret));
        }
      }
    }
  }


  if (OB_SUCC(ret)) {
    new_sql.append(sql_ptr + copy_pos, sql_len - copy_pos);
  }

  return ret;
}


int ObProxyShardUtils::rewrite_shard_request_db(const char *sql_ptr, int64_t database_pos,
                                                int64_t table_pos, uint64_t database_len,
                                                const ObString &real_database_name, bool is_oracle_mode,
                                                bool is_single_shard_db_table, ObSqlString &new_sql, int64_t &copy_pos)
{
  int ret = OB_SUCCESS;

  bool database_hava_quoto = false;

  // 替换 database
  if (database_pos > 0) {
    // 如果 SQL 里有 database
    if (*(sql_ptr + database_pos - 1) == '`' || *(sql_ptr + database_pos - 1) == '"') {
      database_hava_quoto = true;
      database_pos -= 1;
      database_len += 2;
    }
    new_sql.append(sql_ptr + copy_pos, database_pos - copy_pos);

    if (is_oracle_mode) {
      replace_oracle_table(new_sql, real_database_name, database_hava_quoto, is_single_shard_db_table, true);
    } else {
      new_sql.append(real_database_name);
    }

    copy_pos = database_pos + database_len;
    if (database_pos < table_pos) {
      new_sql.append(sql_ptr + copy_pos, table_pos - copy_pos);
    }
  } else {
    // 如果 SQL 里没有 database, 单库单表就不加了
    // add real database name before logic table name
    new_sql.append(sql_ptr + copy_pos, table_pos - copy_pos);
    if (!is_single_shard_db_table && !real_database_name.empty()) {
      if (is_oracle_mode) {
        replace_oracle_table(new_sql, real_database_name, database_hava_quoto, is_single_shard_db_table, true);
      } else {
        new_sql.append(real_database_name);
      }
      new_sql.append(".", 1);
    }
  }

  return ret;
}

int ObProxyShardUtils::rewrite_shard_request_table_no_db(const char *sql_ptr,
                                                         int64_t table_pos, uint64_t table_len,
                                                         const ObString &real_table_name,
                                                         bool is_oracle_mode, bool is_single_shard_db_table,
                                                         ObSqlString &new_sql, int64_t &copy_pos)
{
  int ret = OB_SUCCESS;

  bool table_have_quoto = false;

  if (*(sql_ptr + table_pos - 1) == '`' || *(sql_ptr + table_pos - 1) == '"') {
    table_have_quoto = true;
    table_pos -= 1;
    table_len += 2;
  }

  new_sql.append(sql_ptr + copy_pos, table_pos - copy_pos);
  // 替换表名
  if (is_oracle_mode) {
    replace_oracle_table(new_sql, real_table_name, table_have_quoto, is_single_shard_db_table, false);
  } else {
    new_sql.append(real_table_name);
  }

  copy_pos = table_pos + table_len;

  return ret;
}


int ObProxyShardUtils::rewrite_shard_request_table(const char *sql_ptr,
                                                   int64_t database_pos, uint64_t database_len,
                                                   int64_t table_pos, uint64_t table_len,
                                                   const ObString &real_table_name, const ObString &real_database_name,
                                                   bool is_oracle_mode, bool is_single_shard_db_table,
                                                   ObSqlString &new_sql, int64_t &copy_pos)
{
  int ret = OB_SUCCESS;

  bool table_hava_quoto = false;

  if (*(sql_ptr + table_pos - 1) == '`' || *(sql_ptr + table_pos - 1) == '"') {
    table_hava_quoto = true;
    table_pos -= 1;
    table_len += 2;
  }

  if (OB_FAIL(rewrite_shard_request_db(sql_ptr, database_pos, table_pos, database_len,
                                       real_database_name, is_oracle_mode,
                                       is_single_shard_db_table, new_sql, copy_pos))) {
    LOG_WDIAG("fail to rewrite db", K(ret));
  } else {
    // 替换表名
    if (is_oracle_mode) {
      replace_oracle_table(new_sql, real_table_name, table_hava_quoto, is_single_shard_db_table, false);
    } else {
      new_sql.append(real_table_name);
    }

    copy_pos = table_pos + table_len;
  }

  return ret;
}

int ObProxyShardUtils::rewrite_shard_request_hint_table(const char *sql_ptr, int64_t index_table_pos, uint64_t index_table_len,
                                                        const ObString &real_table_name, bool is_oracle_mode,
                                                        bool is_single_shard_db_table, ObSqlString &new_sql, int64_t &copy_pos)
{
  int ret = OB_SUCCESS;

  bool table_hava_quoto = false;

  // 替换 hint 里的 index 表名
  if (index_table_pos > 0) {
    if (*(sql_ptr + index_table_pos - 1) == '`' || *(sql_ptr + index_table_pos - 1) == '"') {
      table_hava_quoto = true;
      index_table_pos -= 1;
      index_table_len += 2;
    }
    new_sql.append(sql_ptr + copy_pos, index_table_pos);

    if (is_oracle_mode) {
      replace_oracle_table(new_sql, real_table_name, table_hava_quoto, is_single_shard_db_table, false);
    } else {
      new_sql.append(real_table_name);
    }

    copy_pos = index_table_pos + index_table_len;
  }

  return ret;
}

// Mysql 表名用单引号, Oracle 表名用双引号
//  不区分大小写
// Oracle 有大小写区分, 不带引号默认是大写的
//  如果没有引号, Oralce 模式加上引号
//     如果是单表, 表名来自 SQL, 不确定是大写还是小写
//       如果有引号, 带上引号
//       如果没引号, 不带引号
//   如果是分库分表, 表名来自配置, 需要带上引号, 配置是啥就是啥
//      如果有引号, 带上引号
//      如果没引号, 带上引号
int ObProxyShardUtils::rewrite_shard_request(ObClientSessionInfo &session_info,
                                             ObSqlParseResult &parse_result,
                                             const ObString &table_name, const ObString &database_name,
                                             const ObString &real_table_name, const ObString &real_database_name,
                                             bool is_single_shard_db_table,
                                             const ObString& sql,
                                             ObSqlString& new_sql)
{
  int ret = OB_SUCCESS;

  uint64_t table_len = table_name.length();
  uint64_t database_len = database_name.length();
  uint64_t index_table_len = table_name.length();
  int64_t table_pos = parse_result.get_dbmesh_route_info().tb_pos_;
  int64_t database_pos = parse_result.get_dbmesh_route_info().db_pos_;
  int64_t index_table_pos = parse_result.get_dbmesh_route_info().index_tb_pos_;

  const char *sql_ptr = sql.ptr();
  int64_t sql_len = sql.length();

  bool is_oracle_mode = session_info.is_oracle_mode();
  int64_t copy_pos = 0;

  if (OB_FAIL(rewrite_shard_request_hint_table(sql_ptr, index_table_pos, index_table_len,
                                               real_table_name, is_oracle_mode,
                                               is_single_shard_db_table,new_sql, copy_pos))) {
    LOG_WDIAG("fail to rewrite hint table", K(ret));
  } else {
    if (OB_INVALID_INDEX == database_pos || database_pos < table_pos) {
      if (OB_FAIL(rewrite_shard_request_table(sql_ptr, database_pos, database_len,
                                              table_pos, table_len, real_table_name, real_database_name,
                                              is_oracle_mode, is_single_shard_db_table, new_sql, copy_pos))) {
        LOG_WDIAG("fail to rewrite table", K(ret));
      }
    } else {
      if (OB_FAIL(rewrite_shard_request_table(sql_ptr, 0, database_len,
                                              table_pos, table_len, real_table_name, real_database_name,
                                              is_oracle_mode, is_single_shard_db_table, new_sql, copy_pos))) {
        LOG_WDIAG("fail to rewrite table", K(ret));
      } else if (OB_FAIL(rewrite_shard_request_db(sql_ptr, database_pos, table_pos, database_len,
                                                  real_database_name, is_oracle_mode,
                                                  is_single_shard_db_table, new_sql, copy_pos))) {
        LOG_WDIAG("fail to rewrite db", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    new_sql.append(sql_ptr + copy_pos, sql_len - copy_pos);
    LOG_DEBUG("succ to rewrite sql", K(sql), K(new_sql));
  }

  return ret;
}

int ObProxyShardUtils::testload_check_obparser_node_is_valid(const ParseNode *node, const ObItemType &type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("checkout node failed: node is NULL", K(ret), K(type));
  } else if (type != node->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("checkout node failed: type is not equal", K(ret), K(type), K(node->type_));
  } else {
    switch (type) {
      case T_HINT_OPTION_LIST: {
        if (T_HINT_OPTION_LIST != node->type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("checkout node failed: node type error", K(ret), K(type), K(node->type_));
        }
        break;
      }
      case T_INDEX: {
        if (OB_T_INDEX_NUM_CHILD /* 3 */ != node->num_child_
                || OB_ISNULL(node->children_[1])
                || T_RELATION_FACTOR_IN_HINT != (node->children_[1]->type_)
                || OB_T_RELATION_FACTOR_IN_HINT_NUM_CHILD /* 2 */ != node->children_[1]->num_child_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("error T_INDEX node for sql to handle", K(ret));
        }
        break;
      }
      case T_RELATION_FACTOR: {
       if (OB_T_RELATION_FACTOR_NUM_CHILD /* 2 */ > node->num_child_
              || OB_ISNULL(node->children_[1])
              || T_IDENT != node->children_[1]->type_
              || -1 == node->children_[1]->token_len_
              || -1 == node->children_[1]->token_off_) { //must be contain table entry
         ret = OB_ERR_UNEXPECTED;
         LOG_WDIAG("unexpected T_RELATION_FACTOR node", K(ret));
       }
       break;
      }
      case T_COLUMN_REF: {
        if (OB_T_COLUMN_REF_NUM_CHILD /* 3 */ > node->num_child_
             || OB_ISNULL(node->children_[2])
             || (T_IDENT != node->children_[2]->type_ // must be contain column entry
                         && T_STAR != node->children_[2]->type_) /* T_STAR node */
             || (T_IDENT == node->children_[2]->type_
                         && -1 == node->children_[2]->token_len_)
             || (T_IDENT == node->children_[2]->type_
                         && -1 == node->children_[2]->token_off_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("unexpected T_COLUMN_REF node", K(ret));
        }
        break;
      }
      case T_IDENT: {
        if (OB_T_IDENT_NUM_CHILD /* 0 */ != node->num_child_
              || OB_ISNULL(node->str_value_)
              || 0 > node->token_off_
              || 0 > node->token_len_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("unexpected T_IDENT node", K(ret));
        }
      }
      default:
        break; // other type not to use
    }
  }
  if (OB_SUCC(ret)) {
    LOG_DEBUG("check obparser node success", K(ret), K(type));
  }
  return ret;
}

int ObProxyShardUtils::testload_get_obparser_db_and_table_node(const ParseNode *root,
                                                   ParseNode *&db_node,
                                                   ParseNode *&table_node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("checkout node failed: node is NULL", K(ret));
  } else {
    db_node = NULL;
    table_node = NULL;
    const ObItemType &type = root->type_;

    if (T_RELATION_FACTOR != type && T_COLUMN_REF != type) {
      LOG_WDIAG("not is not T_RELATION_FACTOR or T_COLUMN_REF node", K(ret), K(type));
    } else if (OB_FAIL(testload_check_obparser_node_is_valid(root, type))) {
      LOG_WDIAG("not is not invalid  T_RELATION_FACTOR or T_COLUMN_REF node", K(ret), K(type));
    } else {
      // not need to check table node and db node at here, will check it when use
      db_node = root->children_[0];  // set db_node
      table_node = root->children_[1]; // set table_node
    }
  }
  return ret;
}

int ObProxyShardUtils::testload_rewrite_name_base_on_parser_node(common::ObSqlString &new_sql,
                                                   const char *new_name,
                                                   const char *sql_ptr,
                                                   int64_t sql_len,
                                                   int &last_pos,
                                                   ParseNode *node)
{
  int ret = OB_SUCCESS;
  UNUSED(sql_len);
  if (OB_ISNULL(node) || OB_ISNULL(sql_ptr) || OB_ISNULL(new_name)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("use NULL node to rewrite sql", K(ret), K(last_pos));
  } else if (OB_FAIL(testload_check_obparser_node_is_valid(node, T_IDENT))) { //check T_IDENT node
    LOG_WDIAG("use invalid node to rewrite sql", K(ret), K(last_pos));
  } else {
    if (last_pos < node->token_off_) {
      new_sql.append(sql_ptr + last_pos, node->token_off_ - last_pos);
    }

    new_sql.append(new_name);
    last_pos = node->token_off_ + node->token_len_;
    LOG_DEBUG("ObProxyShardUtils::testload_rewrite_name_base_on_parser_node",
               K(ret), K(last_pos), K(node->token_off_), K(node->token_len_));
  }
  return ret;
}

// rewrite hint for testload such as 'SELECT \/*+ index(table_a primary) *\/ col1 FROM table_a;
int ObProxyShardUtils::testload_check_and_rewrite_testload_hint_index(common::ObSqlString &new_sql,
                                                   const char *sql_ptr,
                                                   int64_t sql_len,
                                                   int &last_pos,
                                                   ObSEArray<ObParseNode*, 1> &hint_option_list,
                                                   const ObString &hint_table,
                                                   const ObString &real_database_name,
                                                   ObDbConfigLogicDb &logic_db_info)
{
  int ret = OB_SUCCESS;
  if (hint_option_list.count() > 0) {
    LOG_DEBUG("check_and_rewrite_testload_hint_index", "hint count", hint_option_list.count());
  }
  ParseNode *node = NULL;
  bool use_hint_table_name = !hint_table.empty();

/**
---- T_HINT_OPTION_LIST (null):pos:0, text_len:0,num_child_:3, token_off:25, token_len:64
------ T_INDEX (null):pos:0, text_len:0,num_child_:3, token_off:-1, token_len:-1
-------- There is one NULL child node
-------- T_RELATION_FACTOR_IN_HINT (null):pos:0, text_len:0,num_child_:2, token_off:-1, token_len:-1
---------- T_RELATION_FACTOR ta:pos:0, text_len:0,num_child_:2, token_off:-1, token_len:-1
------------ T_IDENT db:pos:0, text_len:0,num_child_:0, token_off:33, token_len:2
------------ T_IDENT ta:pos:0, text_len:0,num_child_:0, token_off:36, token_len:2
---------- There is one NULL child node
-------- T_IDENT primary:pos:0, text_len:0,num_child_:0, token_off:39, token_len:7
------ T_READ_CONSISTENCY (null):pos:0, text_len:0,num_child_:0, token_off:-1, token_len:-1
------ T_INDEX (null):pos:0, text_len:0,num_child_:3, token_off:-1, token_len:-1
-------- There is one NULL child node
-------- T_RELATION_FACTOR_IN_HINT (null):pos:0, text_len:0,num_child_:2, token_off:-1, token_len:-1
---------- T_RELATION_FACTOR tb:pos:0, text_len:0,num_child_:2, token_off:-1, token_len:-1
---------- There is one NULL child node
-------- T_IDENT sec:pos:0, text_len:0,num_child_:0, token_off:83, token_len:3
 */

  ParseNode *relation_node = NULL;
  ParseNode *tb_node = NULL;
  ParseNode *db_node = NULL;
  for (int i = 0; OB_SUCC(ret) && i < hint_option_list.count(); i++) {
    if (OB_NOT_NULL(hint_option_list[i])
            && OB_NOT_NULL(node = hint_option_list[i]->get_node())
            && T_HINT_OPTION_LIST == node->type_) {
      ParseNode *index_node = NULL;
      for (int j= 0; OB_SUCC(ret) && j < node->num_child_; j++) {
        // handle T_INDEX node
        if (OB_NOT_NULL(index_node = node->children_[j]) && T_INDEX /* 3 */ == index_node->type_) {
          if (OB_FAIL(testload_check_obparser_node_is_valid(index_node, T_INDEX))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("error T_INDEX node for sql to handle", K(ret), K(sql_ptr));
          } else if ((OB_ISNULL(relation_node = index_node->children_[1]->children_[0]))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("error T_RELATION_FACTOR_IN_HINT node for sql to handle", K(ret), K(sql_ptr));
          // relation_node will be check when use
          } else if (OB_FAIL(testload_get_obparser_db_and_table_node(relation_node, db_node, tb_node))) {
            LOG_WDIAG("fail get table_node or db_node from T_RELATION_FACTOR", K(ret), K(relation_node),
                      K(db_node), K(tb_node));
          } else if (OB_NOT_NULL(db_node) // rewrite db node
                       && OB_FAIL(testload_rewrite_name_base_on_parser_node(new_sql, real_database_name.ptr(),
                                  sql_ptr, sql_len, last_pos, db_node))) {
            LOG_WDIAG("fail to rewrite db_node for T_RELATION_FACTOR", K(ret), K(db_node));
          } else {
            // table_name_len must be less than OB_MAX_TABLE_NAME_LENGTH
            std::string new_name;
            std::string table_name = std::string(tb_node->str_value_, tb_node->token_len_); //use origin table_name
            if (use_hint_table_name) {
              new_name.append(hint_table.ptr()); // use hint table
            } else if (OB_FAIL(logic_db_info.check_and_get_testload_table(table_name, new_name))) {
              LOG_WDIAG("check and get table name is fail", K(ret));
            }
            if (OB_SUCC(ret) // rewrite table node
                  && OB_FAIL(testload_rewrite_name_base_on_parser_node(new_sql, new_name.c_str(),
                                                             sql_ptr, sql_len, last_pos, tb_node))) {
              LOG_WDIAG("fail to rewrite db_node for T_RELATION_FACTOR", K(ret), K(tb_node));
            }
          } /* end else */
        }
      }
    }
  }
  return ret;
}

/**
 * Check and rewrite sql when with 'testload=1' in sql hint, it will to replace db_name and
 * table_name base on logic_db_info
 * Args:
 *   @param	session_info	: client session info
 *   @param	client_request	: client request
 *   @param	is_single_shard_db_table:
 *   @param	real_database_name	: real database name in Server
 *   @param	logic_db_info		: config info for database
 * */
int ObProxyShardUtils::testload_check_and_rewrite_testload_request(ObSqlParseResult &parse_result,
                                    bool is_single_shard_db_table,
                                    const ObString &hint_table,
                                    const ObString &real_database_name,
                                    ObDbConfigLogicDb &logic_db_info,
                                    const ObString& sql,
                                    common::ObSqlString& new_sql)
{
  int ret = OB_SUCCESS;
  UNUSED(is_single_shard_db_table);

  const char *sql_ptr = sql.ptr();
  int64_t sql_len = sql.length();
  ObArenaAllocator allocator;
  bool use_hint_table_name = !hint_table.empty();

  ObSEArray<ObParseNode*, 1> relation_table_node;
  ObSEArray<ObParseNode*, 1> hint_option_list;
  common::hash::ObHashMap<common::ObString, ObParseNode*> all_table_map;
  common::hash::ObHashMap<common::ObString, ObParseNode*> alias_table_map;

  if (OB_FAIL(alias_table_map.create(OB_ALIAS_TABLE_MAP_MAX_BUCKET_NUM,
                                      ObModIds::OB_HASH_ALIAS_TABLE_MAP))) {
    LOG_WDIAG("failed to create alias_table_map map");
  } else if (OB_FAIL(all_table_map.create(OB_ALIAS_TABLE_MAP_MAX_BUCKET_NUM,
                                          ObModIds::OB_HASH_ALIAS_TABLE_MAP))) {
    LOG_WDIAG("failed to create all_table_map map");
  } else if (OB_ISNULL(parse_result.get_ob_parser_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("failed to init obparser result tree:", K(ret));
  } else if (OB_FAIL(ObProxySqlParser::ob_load_testload_parse_node(
                                   parse_result.get_ob_parser_result()->result_tree_,
                                   1, /* level */
                                   relation_table_node,
                                   //ref_column_node,
                                   hint_option_list,
                                   all_table_map,
                                   alias_table_map,
                                   allocator))) {
    LOG_WDIAG("ob_load_testload_parse_node failed", K(ret));
  } else if (use_hint_table_name && 1 != (all_table_map.size())) {
    // There is more than one table in SQL, alias table node will be added in all table name
    ret = OB_ERR_MORE_TABLES_WITH_TABLE_HINT;
    LOG_WDIAG("check_and_rewrite_testload_request failed for more table in testload request with table_name hint",
             "ret", ret, "all_table_name", all_table_map.size(), "alias_table_name", alias_table_map.size());
  } else {
    LOG_DEBUG("check_and_rewrite_testload_request:", K(use_hint_table_name), K(relation_table_node),
              K(hint_option_list), K(all_table_map.size()), K(alias_table_map.size()));

    int32_t i = 0;
    ParseNode *node = NULL;   // table T_IDENT node
    ParseNode *db_node = NULL;// database T_IDENT node
    std::string new_name;
    int32_t last_pos = 0;

    if (0 < hint_option_list.count()
          && OB_FAIL(testload_check_and_rewrite_testload_hint_index(new_sql,
                                                           sql_ptr,
                                                           sql_len,
                                                           last_pos,
                                                           hint_option_list,
                                                           hint_table,
                                                           real_database_name,
                                                           logic_db_info))) { //need to check table info
      LOG_WDIAG("check_and_rewrite_testload_hint_index failed", K(ret), K(last_pos), K(new_sql));
    }

    while (OB_SUCC(ret) && i < relation_table_node.count()) {
      db_node = NULL;
      if (OB_ISNULL(node = relation_table_node[i]->get_node())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unexpected node in relation_table_node", K(ret));
      } else {
        switch (node->type_) {
          case T_RELATION_FACTOR:
            if (OB_FAIL(testload_check_obparser_node_is_valid(node, T_RELATION_FACTOR))) {
              LOG_WDIAG("unexpected node T_RELATION_FACTOR", K(ret));
            } else if (OB_FAIL(testload_get_obparser_db_and_table_node(node, db_node, node))){
              LOG_WDIAG("get tb_node or db_node fail for T_RELATION_FACTOR", K(ret));
            } else {
              LOG_DEBUG("will check the table is testload table", K(ret), "table", (node->str_value_));
            }
            break;
          case T_COLUMN_REF:
            if (OB_FAIL(testload_check_obparser_node_is_valid(node, T_COLUMN_REF))) {
              LOG_WDIAG("unexpected node T_RELATION_FACTOR", K(ret));
            } else if (OB_FAIL(testload_get_obparser_db_and_table_node(node, db_node, node))){
              LOG_WDIAG("get tb_node or db_node fail for T_RELATION_FACTOR", K(ret));
            } else {
              ObString ob_str;
              ObString table_str = ObString::make_string(node->str_value_);
              /* compare it with alias node, not need to change it to upper */
              ObParseNode *new_node = NULL;
              if (OB_SUCCESS == alias_table_map.get_refactored(table_str, new_node)) {
                i++;
                continue; //skip directly
              } else {    //not found
                //check table name is if testLoad table name
                LOG_DEBUG("will check the table is testload table", K(ret), "table", (node->str_value_));
              }
            }
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(testload_check_obparser_node_is_valid(node, T_IDENT))) { // check table node first
          //ret = OB_ERR_UNEXPECTED; // not to be here
          LOG_WDIAG("it is not valid table node", K(ret));
        } else {
          LOG_DEBUG("ObProxyShardUtils::check_and_rewrite_testload_request before rewrite",
                    K(i), K(last_pos), K(node->token_off_), K(new_sql), K(last_pos));
          std::string table_name = std::string(node->str_value_, node->token_len_);

          if (OB_NOT_NULL(db_node)
                 && OB_FAIL(testload_rewrite_name_base_on_parser_node(new_sql, real_database_name.ptr(),
                                                                      sql_ptr, sql_len, last_pos, db_node))) {
            LOG_WDIAG("check and rewrite db name is fail", K(ret), "db_name", real_database_name.ptr());
          // rewrite table name
          } else if (use_hint_table_name) {
            new_name.clear();
            new_name.append(hint_table.ptr());
          } else if (OB_FAIL(logic_db_info.check_and_get_testload_table(table_name, new_name))) {
            LOG_WDIAG("check and rewrite table name is fail", K(ret), "table_name", node->str_value_);
          }
          if (OB_SUCC(ret) && OB_FAIL(testload_rewrite_name_base_on_parser_node(new_sql, new_name.c_str(), sql_ptr,
                                                                                sql_len, last_pos, node))) {
            LOG_WDIAG("check and rewrite table name is fail", K(ret), "table_name", new_name.c_str());
          }
        }
        LOG_DEBUG("ObProxyShardUtils::check_and_rewrite_testload_request after rewrite", K(new_sql), K(last_pos));
      }
      i++;
    }
    if (sql_len >= last_pos) {
      LOG_DEBUG("TAIL SQL is", K(last_pos), K(sql_len), "sql_prt", ObString::make_string(sql_ptr));
      new_sql.append(sql_ptr + last_pos, sql_len - last_pos); //added
    }
  }
  LOG_DEBUG("ObProxyShardUtils::check_and_rewrite_testload_request all", K(ret), K(new_sql));

  return ret;
}

bool ObProxyShardUtils::is_special_db(obutils::ObSqlParseResult& parse_result)
{
  const ObString &database_name = parse_result.get_origin_database_name();
  if (!database_name.empty() && 0 == database_name.case_compare("information_schema")) {
    return true;
  }

  return false;
}

int ObProxyShardUtils::get_logic_db_info(ObMysqlTransact::ObTransState &s, ObClientSessionInfo &session_info,
  ObDbConfigLogicDb *&logic_db_info)
{
  int ret = OB_SUCCESS;
  ObString logic_tenant_name;
  ObString database_name;
  if (session_info.is_sharding_user()) {
    database_name = s.trans_info_.client_request_.get_parse_result().get_origin_database_name();
    ObString saved_database_name;
    session_info.get_logic_database_name(saved_database_name);
    // if no db name in sql and saved, just return, no rewrite sql and change user
    if (database_name.empty() && saved_database_name.empty()) {
      ret = OB_ERR_NO_DB_SELECTED;
      LOG_WDIAG("no database selected", K(ret));
    } else {
      ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();
      // 0. get db name. if there is not db name in sql, get the saved logic db name
      if (database_name.empty()) {
        database_name = saved_database_name;
      }
      // 1. get real db_name and table_name
      if (OB_FAIL(session_info.get_logic_tenant_name(logic_tenant_name))) {
        ret = OB_ERR_UNEXPECTED; // no need response, just return ret and disconnect
        LOG_EDIAG("fail to get logic tenant name", K(ret));
      } else if (OB_ISNULL(logic_db_info = dbconfig_cache.get_exist_db_info(logic_tenant_name, database_name))) {
        ret = OB_ERR_BAD_DATABASE;
        LOG_WDIAG("database not exist", K(logic_tenant_name), K(database_name));
      }
    }
  }
  return ret;
}

int ObProxyShardUtils::check_shard_request(ObMysqlClientSession &client_session,
                       ObSqlParseResult &parse_result,
                       ObDbConfigLogicDb &logic_db_info)
{
  int ret = OB_SUCCESS;

  ObClientSessionInfo &session_info = client_session.get_session_info();
  const ObString &database_name = logic_db_info.db_name_.config_string_;
  ObString saved_database_name;
  session_info.get_logic_database_name(saved_database_name);
  if (session_info.enable_shard_authority()) {
    bool sql_has_database = !parse_result.get_origin_database_name().empty();
    const ObShardUserPrivInfo &up_info = session_info.get_shard_user_priv();
    if (sql_has_database && database_name != saved_database_name) {
      const ObString &username = session_info.get_origin_username();
      const ObString &host = session_info.get_client_host();
      ObShardUserPrivInfo new_up_info;
      if (OB_FAIL(logic_db_info.get_user_priv_info(username, host, new_up_info))) {
        ret = OB_ERR_NO_DB_PRIVILEGE;
        LOG_WDIAG("fail to get user priv info for new database", K(database_name), K(username), K(host), K(ret));
      } else if (OB_UNLIKELY(up_info.password_stage2_ != new_up_info.password_stage2_)) {
        // client does not change password, so here we assume same user has same password in different logic db
        // orignal login password is not saved, we just use stage2 to check password
        ret = OB_ERR_NO_DB_PRIVILEGE;
        LOG_WDIAG("fail to check password for new database", K(new_up_info), K(up_info), K(ret));
      } else if (!check_shard_authority(new_up_info, parse_result)) {
        ret = OB_ERR_NO_PRIVILEGE;
        LOG_WDIAG("no privilege to do stmt", K(new_up_info), K(parse_result), K(ret));
      }
    } else {
      if (!check_shard_authority(up_info, parse_result)) {
        ret = OB_ERR_NO_PRIVILEGE;
        LOG_WDIAG("no privilege to do stmt", K(up_info), K(parse_result), K(ret));
      }
    }
  } else if (!client_session.is_local_connection() && !logic_db_info.enable_remote_connection()) {
    ret = OB_ERR_NO_DB_PRIVILEGE;
    LOG_WDIAG("Access denied for database from remote addr", K(database_name), K(ret));
  }
  return ret;
}

int ObProxyShardUtils::handle_possible_probing_stmt(const ObString& sql,
                                                    ObSqlParseResult &parse_result)
{
  int ret = OB_SUCCESS;

  const ObHotUpgraderInfo& info = get_global_hot_upgrade_info();
  if(OB_UNLIKELY(!info.is_active_for_rolling_upgrade_)) {
    if (OB_UNLIKELY(parse_result.get_table_name().empty()
                    && parse_result.is_select_stmt())) {
      ObProxySqlParser sql_parser;
      if (OB_FAIL(sql_parser.parse_sql_by_obparser(ObProxyMysqlRequest::get_parse_sql(sql), NORMAL_PARSE_MODE, parse_result, true))) {
        LOG_WDIAG("parse_sql_by_obparser failed", K(ret), K(sql));
      } else {
        ObProxyDMLStmt *dml_stmt = static_cast<ObProxyDMLStmt*>(parse_result.get_proxy_stmt());
        ObIArray<ObProxyExpr*> &select_expr_array = dml_stmt->select_exprs_;
        ObProxyExprConst *const_expr = NULL;
        if (OB_UNLIKELY(1 == select_expr_array.count())
            && OB_NOT_NULL(const_expr = dynamic_cast<ObProxyExprConst*>(select_expr_array.at(0)))
            && OB_UNLIKELY(const_expr->get_object().is_int())
            && OB_UNLIKELY(1 == const_expr->get_object().get_int())) {
          return OB_ERR_UNEXPECTED; //断连接; OB_NOT_SUPPORTED:返回error包
        }
      }
    }
  }

  return ret;
}

int ObProxyShardUtils::get_shard_hint(const ObString &table_name,
                                      ObClientSessionInfo &session_info,
                                      ObDbConfigLogicDb &logic_db_info,
                                      ObSqlParseResult &parse_result,
                                      int64_t &group_index, int64_t &tb_index,
                                      int64_t &es_index, ObString &hint_table_name,
                                      ObTestLoadType &testload_type)
{
  int ret = OB_SUCCESS;

  bool is_sticky_session = false;

  if (OB_FAIL(get_shard_hint(logic_db_info, parse_result,
                             group_index, tb_index, es_index,
                             hint_table_name, testload_type,
                             is_sticky_session))) {
    LOG_WDIAG("fail to get shard hint", K(ret));
  } else if (is_sticky_session) {
    group_index = session_info.get_group_id();
    tb_index = session_info.get_table_id();
    es_index = session_info.get_es_id();

    if (group_index == OBPROXY_MAX_DBMESH_ID) {
      group_index = 0;
    }

    if (tb_index == OBPROXY_MAX_DBMESH_ID) {
      tb_index = 0;
    }

    if (es_index == OBPROXY_MAX_DBMESH_ID) {
      es_index = 0;
    }

    if (tb_index == 0 && group_index > 0) {
      tb_index = group_index;
    }
  }

  if (OB_SUCC(ret) && !table_name.empty() && !logic_db_info.is_single_shard_db_table()) {
    ObShardRule *logic_tb_info = NULL;
    if (OB_FAIL(logic_db_info.get_shard_rule(logic_tb_info, table_name))) {
      ret = OB_SUCCESS;
      LOG_DEBUG("no shard rule, maybe route by hint", K(table_name), K(ret));
    } else if (group_index != OBPROXY_MAX_DBMESH_ID && group_index >= logic_tb_info->db_size_) {
      ret = OB_NOT_SUPPORTED;
      LOG_WDIAG("sql with group hint and greater than db size",
               "db_size", logic_tb_info->db_size_, K(group_index), K(ret));
    }
  }

  return ret;
}

int ObProxyShardUtils::get_shard_hint(ObDbConfigLogicDb &logic_db_info,
                                      ObSqlParseResult &parse_result,
                                      int64_t &group_index, int64_t &tb_index,
                                      int64_t &es_index, ObString &hint_table_name,
                                      ObTestLoadType &testload_type,
                                      bool &is_sticky_session)
{
  int ret = OB_SUCCESS;
  DbMeshRouteInfo &odp_route_info = parse_result.get_dbmesh_route_info();
  DbpRouteInfo &dbp_route_info = parse_result.get_dbp_route_info();
  if (odp_route_info.is_valid()) {
    group_index = parse_result.get_dbmesh_route_info().group_idx_;
    hint_table_name = parse_result.get_dbmesh_route_info().table_name_;
    if (hint_table_name.empty()) {
      tb_index =  parse_result.get_dbmesh_route_info().tb_idx_;
    }
    es_index = parse_result.get_dbmesh_route_info().es_idx_;
    testload_type = get_testload_type(parse_result.get_dbmesh_route_info().testload_);
    const ObString &disaster_status = parse_result.get_dbmesh_route_info().disaster_status_;
    // 如果 disaster status 存在且有效，使用自适应容灾配置的eid 覆盖掉hint 中的es_index
    // 如果失败了，忽略
    if (!disaster_status.empty() && OB_FAIL(logic_db_info.get_disaster_eid(disaster_status, es_index))) {
      LOG_DEBUG("fail to get disaster elastic id", K(disaster_status), K(ret));
      ret = OB_SUCCESS;
    }
  } else if (dbp_route_info.is_valid()) {
    if (parse_result.get_dbp_route_info().is_group_info_valid()) {
      group_index = dbp_route_info.group_idx_;
      hint_table_name = dbp_route_info.table_name_;
    } else if (parse_result.get_dbp_route_info().sticky_session_) {
      is_sticky_session = true;
    }
  }


  // check unsupported hint combination
  if (OB_SUCC(ret)) {
    bool has_scan_all = dbp_route_info.scan_all_;
    bool has_sticky_session = is_sticky_session;
    bool has_group_id = odp_route_info.group_idx_ != OBPROXY_MAX_DBMESH_ID
                        || dbp_route_info.has_group_info_;
    bool has_sharding_key = dbp_route_info.has_shard_key_;
    int hint_count = has_scan_all + has_sticky_session + has_group_id + has_sharding_key;
    if (hint_count >= 2) {
      ret = OB_PROXY_SHARD_HINT_NOT_SUPPORTED;
      LOG_WDIAG("unsupported hint combination", K(has_scan_all), K(has_sticky_session),
                K(has_group_id), K(has_sharding_key), K(ret));
    }
  }

  return ret;
}

bool ObProxyShardUtils::is_read_stmt(ObClientSessionInfo &session_info, ObMysqlTransact::ObTransState &trans_state,
                                     ObSqlParseResult& parse_result)
{
  // 写请求走写权重, 事务内请求都认为是写
  // 读请求走读权重
  return (!is_sharding_in_trans(session_info, trans_state)
          && (parse_result.is_show_stmt()
              || parse_result.is_desc_stmt()
              || (parse_result.is_select_stmt() && !parse_result.has_for_update())
              || parse_result.is_set_stmt() //[set ac = 0] is internal request, will not be here
              || parse_result.has_explain()));
}

bool ObProxyShardUtils::is_unsupport_type_in_multi_stmt(ObSqlParseResult& parse_result)
{
  return parse_result.is_shard_special_cmd()
         || ObProxyShardUtils::is_special_db(parse_result)
         || parse_result.is_ddl_stmt()
         || parse_result.is_multi_stmt()
         || parse_result.is_show_tables_stmt()
         || parse_result.is_show_full_tables_stmt()
         || parse_result.is_show_table_status_stmt()
         || parse_result.is_show_create_table_stmt();
}

int ObProxyShardUtils::handle_information_schema_request(ObMysqlClientSession &client_session,
                                                         ObMysqlTransact::ObTransState &trans_state,
                                                         ObIOBufferReader &client_buffer_reader)
{
  int ret = OB_SUCCESS;

  ObProxyMysqlRequest &client_request = trans_state.trans_info_.client_request_;
  ObSqlParseResult &parse_result = client_request.get_parse_result();
  SqlFieldResult& sql_result = parse_result.get_sql_filed_result();
  ObProxySqlParser sql_parser;
  ObString sql = client_request.get_parse_sql();
  ObProxyDMLStmt *dml_stmt = NULL;
  if (OB_FAIL(sql_parser.parse_sql_by_obparser(sql, NORMAL_PARSE_MODE, parse_result, true))) {
    LOG_WDIAG("parse_sql_by_obparser failed", K(ret), K(sql));
  } else if (OB_ISNULL(dml_stmt = dynamic_cast<ObProxyDMLStmt*>(parse_result.get_proxy_stmt()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("dml stmt is null, unexpected", K(ret));
  } else {
    ObClientSessionInfo &cs_info = client_session.get_session_info();
    ObDbConfigLogicDb *logic_db_info = NULL;
    ObShardConnector *shard_conn = NULL;
    bool is_rewrite_sql = false;
    bool is_single_shard_db_table = false;

    common::ObSqlString new_sql;
    const char *sql_ptr = sql.ptr();
    int64_t sql_len = sql.length();
    int64_t copy_pos = 0;

    ObString last_database_name;
    char real_database_name[OB_MAX_DATABASE_NAME_LENGTH];
    char real_table_name[OB_MAX_TABLE_NAME_LENGTH];

    ObProxyDMLStmt::DbTablePosArray &db_table_pos_array = dml_stmt->get_db_table_pos_array();
    std::sort(db_table_pos_array.begin(), db_table_pos_array.end());

    for (int64_t i = 0; OB_SUCC(ret) && i < db_table_pos_array.count(); i++) {
      ObProxyDbTablePos &db_table_pos = db_table_pos_array.at(i);
      if (SHARDING_POS_DB == db_table_pos.get_type()) {
        const ObString &database_name = db_table_pos.get_name();

        if (last_database_name.empty()) {
          last_database_name = database_name;
        } else if (0 != last_database_name.case_compare(database_name)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WDIAG("do not support two schema", K(database_name), K(last_database_name), K(ret));
        }

        if (OB_SUCC(ret) && NULL == shard_conn) {
          ObShardConnector *prev_shard_conn = cs_info.get_shard_connector();
          ObString logic_tenant_name;
          ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();
          if (OB_ISNULL(prev_shard_conn)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("prev shard connector info is null", K(ret));
          } else if (OB_FAIL(cs_info.get_logic_tenant_name(logic_tenant_name))) {
            ret = OB_ERR_UNEXPECTED; // no need response, just return ret and disconnect
            LOG_EDIAG("fail to get logic tenant name", K(ret));
          } else if (OB_ISNULL(logic_db_info = dbconfig_cache.get_exist_db_info(logic_tenant_name, database_name))) {
            ret = OB_ERR_BAD_DATABASE;
            LOG_WDIAG("database not exist", K(logic_tenant_name), K(database_name));
          } else if (OB_FAIL(logic_db_info->get_first_group_shard_connector(shard_conn, OBPROXY_MAX_DBMESH_ID, false, TESTLOAD_NON))) {
            LOG_WDIAG("fail to get random shard connector", K(ret), KPC(logic_db_info));
          } else if (*prev_shard_conn != *shard_conn) {
            if (OB_FAIL(change_connector(*logic_db_info, client_session, trans_state, prev_shard_conn, shard_conn))) {
              LOG_WDIAG("fail to change connector", KPC(prev_shard_conn), KPC(shard_conn), K(ret));
            }
          }

          if (OB_SUCC(ret)) {
            snprintf(real_database_name, OB_MAX_DATABASE_NAME_LENGTH, "%.*s",
                     shard_conn->database_name_.length(), shard_conn->database_name_.ptr());
            is_single_shard_db_table = logic_db_info->is_single_shard_db_table();
          }
        }
      }
    }

    if (OB_SUCC(ret) && last_database_name.empty()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WDIAG("do not support zero schema", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < db_table_pos_array.count(); i++) {
      ObProxyDbTablePos &db_table_pos = db_table_pos_array.at(i);
      if (SHARDING_POS_DB == db_table_pos.get_type()) {
        const ObString &database_name = db_table_pos.get_name();
        int32_t database_pos = db_table_pos.get_pos();
        if (OB_FAIL(rewrite_shard_request_db(sql_ptr, database_pos, 0, database_name.length(),
                                             real_database_name, cs_info.is_oracle_mode(),
                                             is_single_shard_db_table, new_sql, copy_pos))) {
          LOG_WDIAG("fail to rewrite db", K(ret));
        } else {
          is_rewrite_sql = true;
        }
      } else if (!is_single_shard_db_table && SHARDING_POS_TABLE == db_table_pos.get_type()) {
        int32_t table_pos = db_table_pos.get_pos();
        const ObString &table_name = db_table_pos.get_name();
        int64_t tb_index = 0;
        ObTestLoadType testload_type = TESTLOAD_NON;
        ObString hint_table;
        if (OB_FAIL(logic_db_info->get_real_table_name(table_name, sql_result,
                                                       real_table_name, OB_MAX_TABLE_NAME_LENGTH,
                                                       tb_index, hint_table, testload_type))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            // If the table does not exist, ignore the table name
            ret = OB_SUCCESS;
          } else {
            LOG_WDIAG("fail to get real table name", K(table_name), K(tb_index), K(testload_type), K(ret));
          }
        } else if (OB_FAIL(rewrite_shard_request_table_no_db(sql_ptr, table_pos, table_name.length(), real_table_name,
                                                             cs_info.is_oracle_mode(), is_single_shard_db_table,
                                                             new_sql, copy_pos))) {
          LOG_WDIAG("fail to rewrite table", K(ret));
        } else {
          is_rewrite_sql = true;
        }
      }
    }

    if (OB_SUCC(ret) && is_rewrite_sql) {
      const uint32_t PARSE_EXTRA_CHAR_NUM = 2;
      new_sql.append(sql_ptr + copy_pos, sql_len - copy_pos - PARSE_EXTRA_CHAR_NUM);

      // 4. push reader forward by consuming old buffer and write new sql into buffer
      if (OB_FAIL(client_buffer_reader.consume_all())) {
        LOG_WDIAG("fail to consume all", K(ret));
      } else {
        ObMIOBuffer *writer = client_buffer_reader.mbuf_;
        if (OB_ISNULL(writer)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("unexpected null values ", K(writer), K(ret));
          // no need compress here, if server session support compress, it will compress later
        } else if (OB_FAIL(ObMysqlRequestBuilder::build_mysql_request(*writer, obmysql::OB_MYSQL_COM_QUERY, new_sql.string(), false, false, 0))) {
          LOG_WDIAG("fail to build_mysql_request", K(new_sql), K(ret));
        } else if (OB_FAIL(ObProxySessionInfoHandler::rewrite_query_req_by_sharding(cs_info, client_request, client_buffer_reader))) {
          LOG_WDIAG("fail to rewrite_query_req_by_sharding", K(ret));
        }
      }
    }

    if (NULL != shard_conn) {
      shard_conn->dec_ref();
      shard_conn = NULL;
    }

    if (NULL != logic_db_info) {
      logic_db_info->dec_ref();
      logic_db_info = NULL;
    }
  }

  return ret;
}

/*
 *
 * 如果没有表名
 *   不处理改写及计算路由的过程，使用上一个请求计算得来的请求，与handle_shard_request对齐普通请求
 *
 * 如果没有 hint
 *   权重读写分离
 *
 * 有 hint
 *   如果有 table_id，如果为0, 则忽略。否则报错
 *   如果有 group_id，如果为0，则忽略。否则报错
 *   如果有 testload、或者指定表名
 *     如果没有表名，报错
 *     如果有表名，改写 SQL
 *   如果有 es_id (eid 可能来自disaster_status)，根据 es_id 路由。否则根据权重读写分离
 *   其他 hint 忽略
 */
int ObProxyShardUtils::do_handle_single_shard_request(ObMysqlClientSession &client_session,
                                                      ObMysqlTransact::ObTransState &trans_state,
                                                      ObDbConfigLogicDb &logic_db_info,
                                                      const ObString& sql,
                                                      ObSqlString& new_sql,
                                                      ObSqlParseResult &parse_result,
                                                      int64_t& es_index,
                                                      int64_t& group_index,
                                                      const int64_t last_es_index)
{
  int ret = OB_SUCCESS;

  ObClientSessionInfo &session_info = client_session.get_session_info();
  session_info.set_enable_reset_db(true);

  ObShardConnector *shard_conn = NULL;
  ObShardConnector *prev_shard_conn = session_info.get_shard_connector();

  ObString table_name = parse_result.get_origin_table_name();
  es_index = OBPROXY_MAX_DBMESH_ID;
  group_index = OBPROXY_MAX_DBMESH_ID;
  int64_t tb_index = OBPROXY_MAX_DBMESH_ID;
  ObTestLoadType testload_type = TESTLOAD_NON;
  ObString hint_table;
  bool is_need_rewrite_sql = false;
  bool is_skip_rewrite_check = false;
  bool is_sticky_session = false;
  if (OB_FAIL(get_shard_hint(logic_db_info, parse_result,
              group_index, tb_index, es_index, hint_table, testload_type, is_sticky_session))) {
    LOG_WDIAG("fail to get shard hint", K(ret));
  } else {
    //  可以跳过改写检查的白名单sql
    //    1.如果识别sql中有表名，不能跳过
    //    2.SQL中没有识别到表名：
    //      2.1 SHOW_TABLES/SHOW_DATABASE
    //      2.2 SELECT LAST_INSERT_ID
    //      2.3 COMMIT
    //      2.4 ROLLBACK
    is_skip_rewrite_check = table_name.empty() && (parse_result.is_show_tables_stmt()
                                                  || parse_result.is_show_full_tables_stmt()
                                                  || parse_result.is_show_table_status_stmt()
                                                  || (parse_result.is_select_stmt() && parse_result.has_last_insert_id())
                                                  || parse_result.is_commit_stmt()
                                                  || parse_result.is_rollback_stmt());
    // 有三种情况需要改写:
    //   1. SQL 里有 DB
    //   2. 有压测标志
    //   3. hint 里指定了表名
    is_need_rewrite_sql = (!is_skip_rewrite_check) && (!parse_result.get_origin_database_name().empty()
                          || TESTLOAD_NON != testload_type
                          || !hint_table.empty());

    // 如果要改写 SQL, 但是 SQL 里没有找到表名, 就报错
    if ((OBPROXY_MAX_DBMESH_ID != tb_index && 0 != tb_index)
        || (OBPROXY_MAX_DBMESH_ID != group_index && 0 != group_index)
        || (is_need_rewrite_sql && table_name.empty())) {
      ret = OB_NOT_SUPPORTED;
      LOG_WDIAG("sql with dbmesh route hint and no table name is unsupported", K(tb_index), K(group_index), K(is_need_rewrite_sql), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    char real_table_name[OB_MAX_TABLE_NAME_LENGTH];
    char real_database_name[OB_MAX_DATABASE_NAME_LENGTH];

    // 写请求走写权重, 事务内请求都认为是写
    // 读请求走读权重
    bool is_read_stmt = ObProxyShardUtils::is_read_stmt(session_info, trans_state, parse_result);

    if (OB_FAIL(logic_db_info.get_single_table_info(table_name, shard_conn,
                                                    real_database_name, OB_MAX_DATABASE_NAME_LENGTH,
                                                    real_table_name, OB_MAX_TABLE_NAME_LENGTH,
                                                    group_index, tb_index, es_index,
                                                    hint_table, testload_type, is_read_stmt, last_es_index))) {
      LOG_WDIAG("shard tpo info is null", K(ret));
    } else if (OB_ISNULL(shard_conn) || OB_ISNULL(prev_shard_conn)) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("shard connector info or prev shard connector info is null", KP(shard_conn),
               KP(prev_shard_conn), K(ret));
    } else if (*prev_shard_conn != *shard_conn) {
      if (OB_FAIL(change_connector(logic_db_info, client_session, trans_state, prev_shard_conn, shard_conn))) {
        LOG_WDIAG("fail to change connector", KPC(prev_shard_conn), KPC(shard_conn), K(ret));
      }
    }
    if (OB_NOT_NULL(shard_conn)) {
      shard_conn->dec_ref();
      shard_conn = NULL;
    }

    if (OB_SUCC(ret)) {
      if (!is_skip_rewrite_check) {
        if (TESTLOAD_ALIPAY_COMPATIBLE == testload_type && hint_table.empty()) { /* testload = 8 */
          ret = OB_ERR_TESTLOAD_ALIPAY_COMPATIBLE;
          LOG_WDIAG("not have table_name's hint for 'testload=8' (TESTLOAD_ALIPAY_COMPATIBLE)", K(ret));
        } else if (TESTLOAD_NON != testload_type) { //rewrite table name for testload
          LOG_DEBUG("check_and_rewrite_testload_request", K(testload_type), K(ret));
          ObProxySqlParser sql_parser;
          if (OB_FAIL(sql_parser.parse_sql_by_obparser(ObProxyMysqlRequest::get_parse_sql(sql),
                                                      NORMAL_PARSE_MODE, parse_result, false))) {
            LOG_WDIAG("parse_sql_by_obparser failed", K(ret), K(sql));
          } else if (OB_FAIL(testload_check_and_rewrite_testload_request(parse_result, false, hint_table,
                                                                        ObString::make_string(real_database_name),
                                                                        logic_db_info, sql, new_sql))) {
            LOG_WDIAG("fail to check and rewrite testload request");
          }
        } else if (is_need_rewrite_sql) {
          if (OB_FAIL(rewrite_shard_request(session_info, parse_result, table_name,
                                            logic_db_info.db_name_.config_string_, ObString::make_string(real_table_name),
                                            ObString::make_string(real_database_name), true, sql, new_sql))) {
              LOG_WDIAG("fail to rewrite shard request", K(ret), K(table_name),
                      K(logic_db_info.db_name_), K(real_table_name), K(real_database_name));
          }
        } else {
          //no need to rewrite sql, just append to new sql;
          ret = new_sql.append(sql);
          LOG_DEBUG("not need to rewrite sql by rules", K(real_database_name), K(real_table_name));
        }
      } else {
        //no need to rewrite sql, just append to new sql;
        ret = new_sql.append(sql);
        LOG_DEBUG("not need to rewrite sql by rules", K(real_database_name), K(real_table_name));
      }
    }
  }
  return ret;
}

int ObProxyShardUtils::handle_single_shard_request(ObMysqlClientSession &client_session,
                                                   ObMysqlTransact::ObTransState &trans_state,
                                                   ObIOBufferReader &client_buffer_reader,
                                                   ObDbConfigLogicDb &logic_db_info)
{
  int ret = OB_SUCCESS;

  ObClientSessionInfo &session_info = client_session.get_session_info();
  ObProxyMysqlRequest &client_request = trans_state.trans_info_.client_request_;
  ObSqlParseResult &origin_parse_result = client_request.get_parse_result();
  ObString origin_sql = client_request.get_sql();//should not use get_parse_sql()
  ObSqlString new_sql;
  ObSEArray<ObString, 4> sql_array;
  ObArenaAllocator &allocator = origin_parse_result.allocator_;
  char * multi_sql_buf = NULL;
  int64_t es_index = OBPROXY_MAX_DBMESH_ID;
  int64_t group_index = OBPROXY_MAX_DBMESH_ID;
  int64_t last_es_index = OBPROXY_MAX_DBMESH_ID;
  int64_t last_group_index = OBPROXY_MAX_DBMESH_ID;
  if (OB_FAIL(ObProxySqlParser::split_multiple_stmt(origin_sql, sql_array))) {
    LOG_WDIAG("fail to split sql", K(ret));
  } else if (OB_UNLIKELY(sql_array.count() <= 0)){
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WDIAG("fail to split sql and the sql num is wrong", K(ret));
  } else if (OB_UNLIKELY((sql_array.count() > 1)
             && OB_FAIL(ObProxySqlParser::preprocess_multi_stmt(allocator,
                                                                multi_sql_buf,
                                                                origin_sql.length(),
                                                                sql_array)))) {
    LOG_WDIAG("fail to preprocess multi stmt", K(ret));
  } else {
    bool is_multi_stmt = sql_array.count() > 1;
    ObSqlParseResult parse_result;
    ObSqlParseResult* real_parse_result = NULL;
    ObProxySqlParser sql_parser;
    for (int64_t i = 0; OB_SUCC(ret) && i < sql_array.count(); ++i) {
      const ObString& sql = sql_array.at(i);
      if (0 == i) {
        real_parse_result = &origin_parse_result;
      } else if (OB_FAIL(sql_parser.parse_sql(ObProxyMysqlRequest::get_parse_sql(sql),
                                      NORMAL_PARSE_MODE/*parse_mode*/, parse_result,
                                      false/*use_lower_case_name*/,
                                      static_cast<common::ObCollationType>(client_session.get_session_info().get_collation_connection()),
                                      false/*drop_origin_db_table_name*/,
                                      true /*is sharding user*/))) {
        LOG_WDIAG("fail to handle single shard request for single sql", K(ret), K(sql));
      } else {
        real_parse_result = &parse_result;
      }

      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(real_parse_result->is_invalid_stmt() && real_parse_result->has_shard_comment())) {
          ret = OB_ERR_PARSER_SYNTAX;
          LOG_WDIAG("fail to parse shard sql with shard comment","sql", client_request.get_sql(), K(ret));
        } else if (OB_UNLIKELY(is_multi_stmt && ObProxyShardUtils::is_unsupport_type_in_multi_stmt(*real_parse_result))) {
          ret = OB_NOT_SUPPORTED;
          LOG_WDIAG("unsupport type in multi stmt", K(ret), K(sql));
        } else if (OB_FAIL(do_handle_single_shard_request(client_session, trans_state, logic_db_info, sql, new_sql,
                                                          *real_parse_result, es_index, group_index, last_es_index))) {
          LOG_WDIAG("fail to handle single shard request for single sql", K(ret), K(sql), K(new_sql));
        } else if (OB_UNLIKELY((i > 0) && ((es_index != last_es_index) || (group_index != last_group_index)))) {
          ret = OB_ERR_DISTRIBUTED_NOT_SUPPORTED;
          LOG_WDIAG("different elastic index or group index for multi stmt is not supported", K(es_index), K(last_es_index), K(group_index),
                                                                                            K(last_group_index), K(sql), K(new_sql), K(ret));
        } else {
          last_es_index = es_index;
          last_group_index = group_index;
          if (i > 0) {
            real_parse_result->reset();
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(build_shard_request_packet(session_info, client_request, client_buffer_reader, new_sql.string()))) {
      LOG_WDIAG("fail to build sharding request packet", K(ret));
    }
  }

  if(NULL != multi_sql_buf) {
    allocator.free(multi_sql_buf);
  }

  return ret;
}

int ObProxyShardUtils::handle_shard_request(ObMysqlClientSession &client_session,
                                            ObMysqlTransact::ObTransState &trans_state,
                                            ObIOBufferReader &client_buffer_reader,
                                            ObDbConfigLogicDb &logic_db_info)
{
  int ret = OB_SUCCESS;

  ObClientSessionInfo &session_info = client_session.get_session_info();
  session_info.set_enable_reset_db(false);
  ObProxyMysqlRequest &client_request = trans_state.trans_info_.client_request_;
  ObSqlParseResult &origin_parse_result = client_request.get_parse_result();
  ObString first_table_name = origin_parse_result.get_origin_table_name();
  ObString origin_sql = client_request.get_sql();//should not use get_parse_sql()
  ObSqlString new_sql;
  ObSEArray<ObString, 4> sql_array;
  ObArenaAllocator &allocator = origin_parse_result.allocator_;
  char * multi_sql_buf = NULL;
  int64_t es_index = OBPROXY_MAX_DBMESH_ID;
  int64_t group_index = OBPROXY_MAX_DBMESH_ID;
  int64_t last_es_index = OBPROXY_MAX_DBMESH_ID;
  int64_t last_group_index = OBPROXY_MAX_DBMESH_ID;
  bool is_scan_all = false;

  if (OB_FAIL(ObProxySqlParser::split_multiple_stmt(origin_sql, sql_array))) {
    LOG_WDIAG("fail to split sql", K(ret));
  } else if (OB_UNLIKELY(sql_array.count() <= 0)){
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WDIAG("fail to split sql and the sql num is wrong", K(ret));
  } else if (OB_UNLIKELY((sql_array.count() > 1)
             && OB_FAIL(ObProxySqlParser::preprocess_multi_stmt(allocator,
                                                                multi_sql_buf,
                                                                origin_sql.length(),
                                                                sql_array)))) {
    LOG_WDIAG("fail to preprocess multi stmt", K(ret));
  } else {
    bool is_multi_stmt = sql_array.count() > 1;
    ObSqlParseResult parse_result;
    ObSqlParseResult* real_parse_result = NULL;
    ObProxySqlParser sql_parser;
    for (int64_t i = 0; OB_SUCC(ret) && i < sql_array.count(); ++i) {
      const ObString& sql = sql_array.at(i);
      if (0 == i) {
        real_parse_result = &origin_parse_result;
      } else if (OB_FAIL(sql_parser.parse_sql(ObProxyMysqlRequest::get_parse_sql(sql),
                                      NORMAL_PARSE_MODE/*parse_mode*/, parse_result,
                                      false/*use_lower_case_name*/,
                                      static_cast<common::ObCollationType>(client_session.get_session_info().get_collation_connection()),
                                      false/*drop_origin_db_table_name*/,
                                      true /*is sharding user*/))) {
        LOG_WDIAG("fail to handle shard request for single sql", K(ret), K(sql));
      } else {
        real_parse_result = &parse_result;
      }

      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(real_parse_result->is_invalid_stmt() && real_parse_result->has_shard_comment())) {
          ret = OB_ERR_PARSER_SYNTAX;
          LOG_WDIAG("fail to parse shard sql with shard comment","sql", client_request.get_sql(), K(ret));
        } else if (OB_UNLIKELY(is_multi_stmt && ObProxyShardUtils::is_unsupport_type_in_multi_stmt(*real_parse_result))) {
          ret = OB_NOT_SUPPORTED;
          LOG_WDIAG("unsupport type in multi stmt", K(ret), K(sql));
        } else if (OB_FAIL(do_handle_shard_request(client_session, trans_state, logic_db_info, sql, new_sql, *real_parse_result,
                                                  es_index, group_index, last_es_index, is_scan_all))) {
          LOG_WDIAG("fail to handle shard request for single sql", K(ret), K(sql), K(new_sql));
        } else if (OB_UNLIKELY((i > 0) && ((es_index != last_es_index) || (group_index != last_group_index)))) {
          ret = OB_ERR_DISTRIBUTED_NOT_SUPPORTED;
          LOG_WDIAG("different elastic index or group index for multi stmt is not supported", K(es_index), K(last_es_index), K(group_index),
                                                                                            K(last_group_index), K(sql), K(new_sql), K(ret));
        } else if (OB_UNLIKELY(is_multi_stmt && is_scan_all)) {
          ret = OB_ERR_DISTRIBUTED_NOT_SUPPORTED;
          LOG_WDIAG("scan all sql in multi stmt is not supported", K(sql), K(new_sql), K(ret));
        } else {
          last_es_index = es_index;
          last_group_index = group_index;
          if (i > 0) {
            real_parse_result->reset();
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (is_scan_all) {
        if (OB_FAIL(handle_scan_all_real_info(logic_db_info, client_session, trans_state, first_table_name))) {
          LOG_WDIAG("fail to handle scan all real info", K(first_table_name), K(ret));
        }
      } else {
        if (OB_FAIL(build_shard_request_packet(session_info, client_request, client_buffer_reader, new_sql.string()))) {
          LOG_WDIAG("fail to build sharding request packet", K(new_sql), K(ret));
        }
      }
    }
  }

  if(NULL != multi_sql_buf) {
    allocator.free(multi_sql_buf);
  }

  return ret;
}

int ObProxyShardUtils::do_handle_shard_request(ObMysqlClientSession &client_session,
                                               ObMysqlTransact::ObTransState &trans_state,
                                               ObDbConfigLogicDb &db_info,
                                               const ObString& sql,
                                               ObSqlString& new_sql,
                                               ObSqlParseResult &parse_result,
                                               int64_t& es_index,
                                               int64_t& group_index,
                                               const int64_t last_es_index,
                                               bool& is_scan_all)
{
  int ret = OB_SUCCESS;

  ObString table_name = parse_result.get_origin_table_name();
  if (OB_UNLIKELY(is_unsupport_type_in_multi_stmt(parse_result)
      || table_name.empty())) {
    //暂时先放过, 保持兼容
    new_sql.append(sql);
  } else if (parse_result.is_show_stmt() || parse_result.is_desc_table_stmt()) {
    if (OB_FAIL(handle_other_request(client_session, trans_state, table_name, db_info,
                                     sql, new_sql, parse_result, es_index, group_index, last_es_index))) {
      LOG_WDIAG("fail to handle other request", K(ret), K(sql), K(new_sql), K(table_name));
    }
  } else if (parse_result.is_select_stmt()) {
    if (OB_FAIL(handle_select_request(client_session, trans_state, table_name, db_info,
                                      sql, new_sql, parse_result, es_index, group_index, last_es_index, is_scan_all))) {
      LOG_WDIAG("fail to handle select request", K(ret), K(sql), K(new_sql), K(table_name));
    }
  } else if (parse_result.is_dml_stmt()) {
    if (OB_FAIL(handle_dml_request(client_session, trans_state, table_name, db_info,
                                   sql, new_sql, parse_result, es_index, group_index, last_es_index))) {
      LOG_WDIAG("fail to handle dml request", K(table_name), K(sql), K(new_sql), K(ret));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WDIAG("stmt is unsupported for sharding table", "stmt type", parse_result.get_stmt_type(), K(ret));
  }

  return ret;
}

int ObProxyShardUtils::build_shard_request_packet(ObClientSessionInfo &session_info,
                                                  ObProxyMysqlRequest &client_request,
                                                  ObIOBufferReader &client_buffer_reader,
                                                  const ObString& sql)
{
  int ret = OB_SUCCESS;

  // 4. push reader forward by consuming old buffer and write new sql into buffer
  if (OB_FAIL(client_buffer_reader.consume_all())) {
    LOG_WDIAG("fail to consume all", K(ret));
  } else {
    ObMIOBuffer *writer = client_buffer_reader.mbuf_;
    if (OB_ISNULL(writer)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unexpected null values ", K(writer), K(ret));
      // no need compress here, if server session support compress, it will compress later
    } else if (OB_FAIL(ObMysqlRequestBuilder::build_mysql_request(*writer, obmysql::OB_MYSQL_COM_QUERY, sql, false, false, 0))) {
      LOG_WDIAG("fail to build_mysql_request", K(sql), K(ret));
    } else if (OB_FAIL(ObProxySessionInfoHandler::rewrite_query_req_by_sharding(session_info, client_request, client_buffer_reader))) {
      LOG_WDIAG("fail to rewrite_query_req_by_sharding", K(ret));
    }
  }

  return ret;
}

int ObProxyShardUtils::handle_ddl_request(ObMysqlSM *sm,
                                          ObMysqlClientSession &client_session,
                                          ObMysqlTransact::ObTransState &trans_state,
                                          ObDbConfigLogicDb &db_info,
                                          bool &need_wait_callback)
{
  int ret = OB_SUCCESS;

  ObShardDDLCont *cont = NULL;

  ObClientSessionInfo &session_info = client_session.get_session_info();
  ObProxyMysqlRequest &client_request = trans_state.trans_info_.client_request_;
  ObString sql = client_request.get_parse_sql();
  ObSqlParseResult &parse_result = client_request.get_parse_result();
  ObString table_name = parse_result.get_origin_table_name();
  ObString logic_tenant_name;
  ObEThread *cb_thread = &self_ethread();

  int64_t group_id = OBPROXY_MAX_DBMESH_ID;
  //unused variables
  int64_t table_id = OBPROXY_MAX_DBMESH_ID;
  int64_t es_id = OBPROXY_MAX_DBMESH_ID;
  ObTestLoadType testload_type = TESTLOAD_NON;
  ObString hint_table_name;
  if (OB_FAIL(get_shard_hint(table_name, session_info, db_info, parse_result,
                             group_id, table_id, es_id, hint_table_name, testload_type))) {
    LOG_WDIAG("fail to get shard hint", K(ret));
  } else if (FALSE_IT(group_id = (group_id == OBPROXY_MAX_DBMESH_ID) ? -1 : group_id)) {
    //impossible
  } if (OB_FAIL(session_info.get_logic_tenant_name(logic_tenant_name))) {
    ret = OB_ERR_UNEXPECTED; // no need response, just return ret and disconnect
    LOG_EDIAG("fail to get logic tenant name", K(ret));
  } else if (OB_ISNULL(cont = new(std::nothrow) ObShardDDLCont(sm, cb_thread))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc memory for ObShardDDLCont", K(ret));
  } else if (OB_FAIL(cont->init(logic_tenant_name, db_info.db_name_.config_string_,
                                sql, group_id, hint_table_name, parse_result))) {
    LOG_WDIAG("fail to init ObShardDDLCont", K(ret));
  } else if (OB_ISNULL(g_event_processor.schedule_imm(cont, ET_TASK))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to schedule ob shard ddl task", K(ret));
  } else if (OB_FAIL(sm->setup_handle_shard_ddl(&cont->get_action()))) {
    LOG_WDIAG("fail to setup handle shard ddl", K(ret));
  } else {
    need_wait_callback = true;
    LOG_INFO("succ to schedule ob shard ddl task");
  }

  if (OB_FAIL(ret)) {
    if (NULL != cont) {
      cont->destroy();
      cont = NULL;
    }
  }

  return ret;
}

int ObProxyShardUtils::handle_other_request(ObMysqlClientSession &client_session,
                                            ObMysqlTransact::ObTransState &trans_state,
                                            const ObString &table_name,
                                            ObDbConfigLogicDb &db_info,
                                            const ObString& sql,
                                            ObSqlString& new_sql,
                                            ObSqlParseResult &parse_result,
                                            int64_t& es_index,
                                            int64_t& group_index,
                                            const int64_t last_es_index)
{
  int ret = OB_SUCCESS;

  ObClientSessionInfo &session_info = client_session.get_session_info();
  char real_table_name[OB_MAX_TABLE_NAME_LENGTH];
  char real_database_name[OB_MAX_DATABASE_NAME_LENGTH];

  if (OB_FAIL(handle_other_real_info(db_info, client_session, trans_state, table_name,
                                     real_database_name, OB_MAX_DATABASE_NAME_LENGTH,
                                     real_table_name, OB_MAX_TABLE_NAME_LENGTH,
                                     parse_result, es_index, group_index, last_es_index))) {
    LOG_WDIAG("fail to handle other real info", K(ret), K(session_info), K(table_name));
  } else if (OB_FAIL(rewrite_shard_request(session_info, parse_result,
                                           table_name, db_info.db_name_.config_string_,
                                           ObString::make_string(real_table_name),
                                           ObString::make_string(real_database_name), false, sql, new_sql))) {
    LOG_WDIAG("fail to rewrite shard request", K(ret), K(table_name),
             K(db_info.db_name_), K(real_table_name), K(real_database_name));
  }

  return ret;
}

int ObProxyShardUtils::handle_select_request(ObMysqlClientSession &client_session,
                                             ObMysqlTransact::ObTransState &trans_state,
                                             const ObString &table_name,
                                             ObDbConfigLogicDb &db_info,
                                             const ObString& sql,
                                             ObSqlString& new_sql,
                                             ObSqlParseResult &parse_result,
                                             int64_t& es_index,
                                             int64_t& group_index,
                                             const int64_t last_es_index,
                                             bool& is_scan_all)
{
  int ret = OB_SUCCESS;
  SqlFieldResult& sql_result = parse_result.get_sql_filed_result();
  ObProxySqlParser sql_parser;
  if (OB_FAIL(sql_parser.parse_sql_by_obparser(ObProxyMysqlRequest::get_parse_sql(sql), NORMAL_PARSE_MODE, parse_result, true))) {
    LOG_WDIAG("parse_sql_by_obparser failed", K(ret), K(sql));
  } else if (FALSE_IT(is_scan_all = need_scan_all(parse_result))) {
    // impossible
  } else if (is_scan_all) {
    if (OB_FAIL(need_scan_all_by_index(table_name, db_info, sql_result, is_scan_all))) {
      LOG_WDIAG("fail to exec scan all by index", K(table_name), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (is_scan_all) {
      //handle scan all later
    } else {
      if (OB_FAIL(handle_dml_real_info(db_info, client_session, trans_state, table_name,
                                       sql, new_sql, parse_result, es_index, group_index, last_es_index))) {
        LOG_WDIAG("fail to handle dml real info", K(table_name), K(ret));
      }
    }
  }

  return ret;
}

int ObProxyShardUtils::handle_dml_request(ObMysqlClientSession &client_session,
                                          ObMysqlTransact::ObTransState &trans_state,
                                          const ObString &table_name,
                                          ObDbConfigLogicDb &db_info,
                                          const ObString& sql,
                                          ObSqlString& new_sql,
                                          ObSqlParseResult &parse_result,
                                          int64_t& es_index,
                                          int64_t& group_index,
                                          const int64_t last_es_index)
{
  int ret = OB_SUCCESS;
  ObProxySqlParser sql_parser;
  if (OB_FAIL(sql_parser.parse_sql_by_obparser(ObProxyMysqlRequest::get_parse_sql(sql), NORMAL_PARSE_MODE, parse_result, true))) {
    LOG_WDIAG("parse_sql_by_obparser failed", K(ret), K(sql));
  } else if (OB_FAIL(handle_dml_real_info(db_info, client_session, trans_state, table_name,
                                       sql, new_sql, parse_result, es_index, group_index, last_es_index))) {
    LOG_WDIAG("fail to handle dml real info", K(table_name), K(ret));
  }

  return ret;
}

int ObProxyShardUtils::check_topology(ObSqlParseResult &parse_result,
                                      ObDbConfigLogicDb &db_info)
{
  int ret = OB_SUCCESS;

  ObProxyDMLStmt *dml_stmt = static_cast<ObProxyDMLStmt*>(parse_result.get_proxy_stmt());
  if (OB_ISNULL(dml_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("select stmt is null, unexpected", K(ret));
  } else {
    int64_t last_db_size = -1;
    int64_t last_tb_size = -1;
    ObProxyDMLStmt::ExprMap &table_exprs_map = dml_stmt->get_table_exprs_map();
    ObProxyDMLStmt::ExprMap::iterator iter = table_exprs_map.begin();
    ObProxyDMLStmt::ExprMap::iterator end = table_exprs_map.end();

    for (; OB_SUCC(ret) && iter != end; iter++) {
      ObProxyExpr *expr = iter->second;
      ObProxyExprTable *table_expr = NULL;
      ObShardRule *logic_tb_info = NULL;
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("expr is null, unexpected", K(ret));
      } else if (OB_ISNULL(table_expr = dynamic_cast<ObProxyExprTable*>(expr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to cast to table expr", K(expr), K(ret));
      } else {
        ObString &table_name = table_expr->get_table_name();
        if (OB_FAIL(db_info.get_shard_rule(logic_tb_info, table_name))) {
          LOG_WDIAG("fail to get shard rule", K(table_name), K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        int64_t current_db_size = logic_tb_info->db_size_;
        if (-1 == last_db_size) {
          last_db_size = current_db_size;
        } else if (last_db_size != current_db_size) {
          ret = OB_ERR_UNSUPPORT_DIFF_TOPOLOGY;
        }
      }

      if (OB_SUCC(ret)) {
        int64_t current_tb_size = 0;
        if (1 == logic_tb_info->tb_size_) {
          current_tb_size = logic_tb_info->db_size_;
        } else {
          current_tb_size = logic_tb_info->tb_size_;
        }

        if (-1 == last_tb_size) {
          last_tb_size = current_tb_size;
        } else if (last_tb_size != current_tb_size) {
          ret = OB_ERR_UNSUPPORT_DIFF_TOPOLOGY;
        }
      }
    }
  }

  return ret;
}

bool ObProxyShardUtils::need_scan_all(ObSqlParseResult &parse_result)
{
  if (parse_result.is_select_stmt() && !parse_result.has_for_update()
      && (parse_result.get_dbp_route_info().scan_all_
          || (!(parse_result.has_dbmesh_hint() || parse_result.is_use_dbp_hint())
                && get_global_proxy_config().auto_scan_all))) {
    return true;
  }

  return false;
}

int ObProxyShardUtils::need_scan_all_by_index(const ObString &table_name,
                                              ObDbConfigLogicDb &db_info,
                                              SqlFieldResult& sql_result,
                                              bool &is_scan_all)
{
  int ret = OB_SUCCESS;

  ObShardRule *logic_tb_info = NULL;
  ObSEArray<int64_t, 4> index_array;
  if (OB_FAIL(db_info.get_shard_rule(logic_tb_info, table_name))) {
    LOG_WDIAG("fail to get shard rule", K(table_name), K(ret));
    // 有可能不带 where 条件, 但是只有一个库一个表
  } else if (logic_tb_info->tb_size_ == 1 && logic_tb_info->db_size_ == 1) {
    is_scan_all = false;
  } else if (sql_result.field_num_ > 0) {
    if (logic_tb_info->tb_size_ == 1) {
      // 分库不分表 和 分库单表,  根据 db rule 计算
      if (OB_FAIL(ObShardRule::get_physic_index_array(sql_result, logic_tb_info->db_rules_,
                                                      logic_tb_info->db_size_,
                                                      TESTLOAD_NON, index_array))) {
        LOG_WDIAG("fail to get physic tb index", K(table_name), KPC(logic_tb_info), K(ret));
      }
    } else {
      // 分库分表, 根据 tb rule 计算
      if (OB_FAIL(ObShardRule::get_physic_index_array(sql_result, logic_tb_info->tb_rules_,
                                                      logic_tb_info->tb_size_,
                                                      TESTLOAD_NON, index_array))) {
        LOG_WDIAG("fail to get physic tb index", K(table_name), KPC(logic_tb_info), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (!index_array.empty() && index_array.count() == 1) {
        is_scan_all = false;
      }
    }
  }

  return ret;
}

int ObProxyShardUtils::update_sys_read_consistency_if_need(ObClientSessionInfo &session_info)
{
  int ret = OB_SUCCESS;
  ObConsistencyLevel level = session_info.get_consistency_level_prop();
  if (level != INVALID_CONSISTENCY) {
    LOG_DEBUG("update_sys_read_consistency_if_need", K(level));
    ObObj obj;
    if (OB_FAIL(session_info.get_sys_variable_value(ObString::make_string(OB_SV_READ_CONSISTENCY), obj))) {
      LOG_WDIAG("get_sys_variable_value fail", K(ret));
    } else {
      int64_t sys_val = obj.get_int();
      if (sys_val != level) {
        obj.set_int(level);
        if (OB_FAIL(session_info.update_sys_variable(ObString::make_string(OB_SV_READ_CONSISTENCY), obj))) {
          LOG_WDIAG("fail to update read_consistency", K(obj), K(ret));
        } else {
          LOG_DEBUG("update read_consistency succ", K(obj));
        }
      } else {
        LOG_DEBUG("same as old ,no need update", K(sys_val));
      }
    }
  } else {
    //maybe not set do nothing
  }
  return ret;
}

int ObProxyShardUtils::handle_sys_read_consitency_prop(ObDbConfigLogicDb &logic_db_info,
  ObShardConnector& shard_conn,
  ObClientSessionInfo &session_info)
{
  int ret = OB_SUCCESS;
  ObString read_consistency = shard_conn.read_consistency_.config_string_;
  ObShardProp* shard_prop = NULL;
  if (read_consistency.empty()) {
    // ignore when get fail
    if (OB_SUCC(logic_db_info.get_shard_prop(shard_conn.shard_name_.config_string_, shard_prop))) {
      read_consistency = shard_prop->read_consistency_.config_string_;
    }
  }
  if (read_consistency.empty()) {
    read_consistency = ObString::make_string(DEFAULT_READ_CONSISTENCY);
  }
  ObConsistencyLevel level = get_consistency_level_by_str(read_consistency);
  session_info.set_consistency_level_prop(level);
  if (OB_NOT_NULL(shard_prop)) {
    shard_prop->dec_ref();
    shard_prop = NULL;
  }
  return ret;
}

int ObProxyShardUtils::init_shard_common_session(ObClientSessionInfo &session_info)
{
  //todo: 这个默认值最好是可以来自一个控制台，可以设置，可以调整
  //int type
  std::map<std::string, int> int_session_map;
  int_session_map.insert(std::pair<std::string, int>("autocommit", 1));
  int_session_map.insert(std::pair<std::string, int>("interactive_timeout", 28800));
  int_session_map.insert(std::pair<std::string, int>("wait_timeout", 28800));
  int_session_map.insert(std::pair<std::string, int>("net_read_timeout", 30));
  int_session_map.insert(std::pair<std::string, int>("net_write_timeout", 60));
  int_session_map.insert(std::pair<std::string, int>("tx_read_only", 0));
  int_session_map.insert(std::pair<std::string, int>("character_set_client", 45));
  int_session_map.insert(std::pair<std::string, int>("character_set_connection", 45));
  int_session_map.insert(std::pair<std::string, int>("character_set_results", 45));
  // follower are global session, can not set now
  // int_session_map.insert(std::pair<std::string, int>("query_cache_size", 1048576));
  // int_session_map.insert(std::pair<std::string, int>("query_cache_type", 0));
  // int_session_map.insert(std::pair<std::string, int>("read_only", 0));
  // int_session_map.insert(std::pair<std::string, int>("connect_timeout", 10));


  //uinttype
  std::map<std::string, uint64_t> uint_session_map;
  uint_session_map.insert(std::pair<std::string, uint64_t>("auto_increment_increment", 1));
  uint_session_map.insert(std::pair<std::string, uint64_t>("auto_increment_offset", 1));
  uint_session_map.insert(std::pair<std::string, uint64_t>("last_insert_id", 0));


  std::map<std::string, std::string> str_session_map;
  // as following obj_cast may failed, change use int now
  // str_session_map.insert(std::pair<std::string, std::string>("character_set_client", "utf8"));
  // str_session_map.insert(std::pair<std::string, std::string>("character_set_connection", "utf8"));
  // str_session_map.insert(std::pair<std::string, std::string>("character_set_results", "utf8"));

  int ret = OB_SUCCESS;
  common::ObObj obj;
  ObString name;
  for(std::map<std::string, int>::iterator it = int_session_map.begin();
      OB_SUCC(ret) && it!= int_session_map.end(); ++it) {
    obj.reset();
    name = ObString::make_string(it->first.c_str());
    obj.set_int(it->second);
    if (OB_FAIL(session_info.update_common_sys_variable(name, obj, true, false))) {
      LOG_WDIAG("init int_sys failed", K(name), K(obj));
    }
  }
  for(std::map<std::string, uint64_t>::iterator it = uint_session_map.begin();
      OB_SUCC(ret) && it!= uint_session_map.end(); ++it) {
    obj.reset();
    name = ObString::make_string(it->first.c_str());
    obj.set_uint(ObUInt64Type, it->second);
    if (OB_FAIL(session_info.update_common_sys_variable(name, obj, true, false))) {
      LOG_WDIAG("init unt_sys failed", K(name), K(obj));
    }
  }
  if (OB_SUCC(ret)) {
    for(std::map<std::string, std::string>::iterator it = str_session_map.begin();
      OB_SUCC(ret) && it!= str_session_map.end(); ++it) {
      obj.reset();
      name = ObString::make_string(it->first.c_str());
      obj.set_varchar(ObString::make_string(it->second.c_str()));
      obj.set_collation_type(ObCharset::get_system_collation());
      if (OB_FAIL(session_info.update_common_sys_variable(name, obj, true, false))) {
        LOG_WDIAG("init str_sys failed", K(name), K(obj));
      }
    }
  }
  LOG_DEBUG("succ init_shard_common_session");
  return ret;
}

int ObProxyShardUtils::handle_shard_auth(ObMysqlClientSession &client_session, const ObHSRResult &hsr)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("will handle shard auth");
  // try to find logic db, do not use origin name, it's fake
  ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();

  ObClientSessionInfo &session_info = client_session.get_session_info();
  ObDbConfigLogicTenant *tenant_info = NULL;
  ObDbConfigLogicDb *db_info = NULL;
  ObShardConnector *shard_conn = NULL;
  const ObString &database = hsr.response_.get_database();
  const ObString &password = hsr.response_.get_auth_response();

  // check whether using shard
  if (NULL != (tenant_info = dbconfig_cache.get_exist_tenant(hsr.tenant_name_))
      && tenant_info->ld_map_.count() > 0) {
    LOG_DEBUG("succ to get logic tenant info", KPC(tenant_info));
    session_info.set_user_identity(USER_TYPE_SHARDING);
    if (get_global_proxy_config().is_pool_mode) {
      client_session.set_session_pool_client(true);
    }
    session_info.set_logic_tenant_name(hsr.tenant_name_);
    if (get_global_proxy_config().enable_shard_authority) {
      session_info.set_enable_shard_authority();
    }
    const ObString &username = hsr.user_name_;
    const ObString &host = session_info.get_client_host();
    ObShardUserPrivInfo &up_info = session_info.get_shard_user_priv();
    if (OB_FAIL(init_shard_common_session(session_info))) {
      LOG_WDIAG("init_shard_common_session failed", K(ret));
    } else if (OB_FAIL(session_info.set_origin_username(username))) {
      LOG_WDIAG("fail to set origin username", K(username), K(ret));
    } else if (session_info.enable_shard_authority()) {
      if (database.empty()) {
        if (OB_FAIL(tenant_info->acquire_random_logic_db_with_auth(username, host, up_info, db_info))) {
          ret = OB_USER_NOT_EXIST;
          LOG_WDIAG("fail to acquire random shard connector", K(username), K(host), K(ret));
        }
      } else if (NULL != (db_info = tenant_info->get_exist_db(database))) {
        if (OB_FAIL(db_info->get_user_priv_info(username, host, up_info))) {
          LOG_WDIAG("fail to get user priv info", K(username), K(host), K(database), K(ret));
          ret = OB_USER_NOT_EXIST;
        }
      }
      if (OB_SUCC(ret) && NULL != db_info) {
        if (OB_FAIL(check_login(password, client_session.get_scramble_string(),
                                up_info.password_stage2_.config_string_))) {
          LOG_WDIAG("fail to check login", K(ret));
        }
      }
    } else {
      if (database.empty()) {
        if (OB_FAIL(tenant_info->acquire_random_logic_db(db_info))) {
          LOG_WDIAG("fail to get random logic db", K(ret), KPC(tenant_info));
        }
      } else if (NULL != (db_info = tenant_info->get_exist_db(database))) {
        if (!client_session.is_local_connection() && !db_info->enable_remote_connection()) {
          ret = OB_ERR_NO_DB_PRIVILEGE;
          LOG_WDIAG("Access denied for database from remote addr", K(database), K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(db_info)) {
        ret = OB_ERR_BAD_DATABASE;
        LOG_WDIAG("database does not exist", K(database), K(ret));
      } else if (!database.empty()) {
        session_info.set_logic_database_name(database);
        LOG_DEBUG("succ to get logic db", KPC(db_info));
      } else {
        LOG_DEBUG("succ to get random logic db", KPC(db_info));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(db_info->acquire_random_shard_connector(shard_conn))) {
        LOG_WDIAG("fail to get random shard connector", K(ret), KPC(db_info));
      } else {
        if (DB_OB_MYSQL == shard_conn->server_type_ || DB_OB_ORACLE == shard_conn->server_type_) {
          if (OB_FAIL(change_user_auth(client_session, *shard_conn, true, true))) {
            LOG_WDIAG("fail to change user auth", K(ret), KPC(shard_conn));
          }
        }

        if (OB_SUCC(ret)) {
          ObShardProp* shard_prop = NULL;
          if (OB_FAIL(db_info->get_shard_prop(shard_conn->shard_name_, shard_prop))) {
            LOG_DEBUG("fail to get shard prop", "shard name", shard_conn->shard_name_, K(ret));
            ret = OB_SUCCESS;
          } else {
            session_info.set_shard_prop(shard_prop);
          }
        }

        if (OB_SUCC(ret)) {
          session_info.set_shard_connector(shard_conn);
          if (client_session.is_session_pool_client()) {
            //连接池不会真正建联，需要保存下database_name
            const ObString& real_database_name = shard_conn->database_name_.config_string_;
            session_info.set_database_name(real_database_name);
            LOG_DEBUG("set database_name for pool", K(real_database_name));
          }
        }
      }
    }
  } else {
    // not shard user
  }
  if (NULL != shard_conn) {
    shard_conn->dec_ref();
    shard_conn = NULL;
  }
  if (NULL != db_info) {
    db_info->dec_ref();
    db_info = NULL;
  }
  if (NULL != tenant_info) {
    tenant_info->dec_ref();
    tenant_info = NULL;
  }
  return ret;
}

int ObProxyShardUtils::handle_shard_use_db(ObMysqlTransact::ObTransState &trans_state,
                                           ObMysqlClientSession &client_session,
                                           const ObString &db_name)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_logic_database(trans_state, client_session, db_name))) {
    LOG_WDIAG("fail to check logic database", K(db_name), K(ret));
  }
  return ret;
}

int ObProxyShardUtils::get_db_version(const ObString &logic_tenant_name,
                                      const ObString &logic_database_name,
                                      ObString &db_version)
{
  int ret = OB_SUCCESS;
  ObDbConfigLogicDb *logic_db_info = NULL;
  ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();
  if (OB_ISNULL(logic_db_info = dbconfig_cache.get_exist_db_info(logic_tenant_name, logic_database_name))) {
    LOG_WDIAG("logic db is not exist", K(logic_tenant_name), K(logic_database_name));
  } else {
    db_version = logic_db_info->get_version();
  }
  if (NULL != logic_db_info) {
    logic_db_info->dec_ref();
    logic_db_info = NULL;
  }
  return ret;
}

int ObProxyShardUtils::get_all_database(const ObString &logic_tenant_name, ObArray<ObString> &db_names)
{
  int ret = OB_SUCCESS;
  ObDbConfigLogicTenant *tenant_info = NULL;
  ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();
  if (NULL != (tenant_info = dbconfig_cache.get_exist_tenant(logic_tenant_name))) {
    obsys::CRLockGuard guard(dbconfig_cache.rwlock_);
    ObDbConfigLogicTenant::LDHashMap &logic_db_map = tenant_info->ld_map_;
    ObDbConfigLogicTenant::LDHashMap::iterator it = logic_db_map.begin();
    ObDbConfigLogicTenant::LDHashMap::iterator it_end = logic_db_map.end();
    for (; it != it_end; ++it) {
      db_names.push_back(it->db_name_.config_string_);
    }
  }
  if (NULL != tenant_info) {
    tenant_info->dec_ref();
    tenant_info = NULL;
  }
  return ret;
}

int ObProxyShardUtils::get_all_schema_table(const ObString &logic_tenant_name, const ObString &logic_database_name, ObArray<ObString> &table_names)
{
  int ret = OB_SUCCESS;
  ObDbConfigLogicDb *logic_db_info = NULL;
  ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();
  if (NULL != (logic_db_info = dbconfig_cache.get_exist_db_info(logic_tenant_name, logic_database_name))) {
    if (OB_FAIL(logic_db_info->get_all_shard_table(table_names))) {
      LOG_WDIAG("fail to get all shard tables", K(logic_tenant_name), K(logic_database_name), K(ret));
    }
  }
  if (NULL != logic_db_info) {
    logic_db_info->dec_ref();
    logic_db_info = NULL;
  }
  return ret;
}

int ObProxyShardUtils::add_table_name_to_map(ObIAllocator &allocator,
                                             ObHashMap<ObString, ObString> &table_name_map,
                                             const ObString &table_name, const ObString &real_table_name)
{
  int ret = OB_SUCCESS;

  char *buf = NULL;
  if (OB_ISNULL(buf = (char*) allocator.alloc(real_table_name.length()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc table name buf", K(real_table_name), K(ret));
  } else {
    memcpy(buf, real_table_name.ptr(), real_table_name.length());
    ObString real_table_name_buf(real_table_name.length(), buf);
    if (OB_FAIL(table_name_map.set_refactored(table_name, real_table_name_buf))) {
      LOG_WDIAG("fail to set table name buf", K(table_name), K(ret));
    }
  }

  return ret;
}

int ObProxyShardUtils::handle_other_real_info(ObDbConfigLogicDb &logic_db_info,
                                              ObMysqlClientSession &client_session,
                                              ObMysqlTransact::ObTransState &trans_state,
                                              const ObString &table_name,
                                              char *real_database_name, int64_t db_name_len,
                                              char *real_table_name, int64_t tb_name_len,
                                              ObSqlParseResult &parse_result,
                                              int64_t& es_index, int64_t& group_index,
                                              const int64_t last_es_index)
{
  int ret = OB_SUCCESS;

  ObShardConnector *shard_conn = NULL;
  ObClientSessionInfo &cs_info = client_session.get_session_info();
  ObShardConnector *prev_shard_conn = cs_info.get_shard_connector();
  SqlFieldResult &sql_result = parse_result.get_sql_filed_result();

  ObShardRule *logic_tb_info = NULL;

  group_index = OBPROXY_MAX_DBMESH_ID;
  int64_t tb_index = OBPROXY_MAX_DBMESH_ID;
  es_index = OBPROXY_MAX_DBMESH_ID;
  ObTestLoadType testload_type = TESTLOAD_NON;
  ObString hint_table;
  bool is_read_stmt = false;
  bool need_rewrite_table_name = true;
  bool is_not_saved_database = false;

  if (OB_ISNULL(prev_shard_conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("shard connector info is null", K(ret));
  } else if (OB_FAIL(get_shard_hint(table_name, cs_info, logic_db_info, parse_result,
                                    group_index, tb_index, es_index, hint_table, testload_type))) {
    LOG_WDIAG("fail to get shard hint", K(ret));
  } else if (OBPROXY_MAX_DBMESH_ID != group_index
             && OBPROXY_MAX_DBMESH_ID == tb_index) {
    need_rewrite_table_name = false;
  } else if (OB_FAIL(logic_db_info.get_shard_rule(logic_tb_info, table_name))) {
    LOG_WDIAG("fail to get shard rule", K(table_name), K(ret));
  } else {
    int64_t last_group_id = cs_info.get_group_id();
    ObString saved_database_name;
    cs_info.get_logic_database_name(saved_database_name);
    is_not_saved_database = saved_database_name != logic_db_info.db_name_.config_string_;

    // If no group_index is specified, and the database accessed by the current SQL is the session database, the last_group_id is used
    if (!is_not_saved_database && last_group_id < logic_tb_info->db_size_) {
      group_index = last_group_id;
      es_index = cs_info.get_es_id();
    }

    if (OBPROXY_MAX_DBMESH_ID == group_index
        && OB_FAIL(ObShardRule::get_physic_index_random(logic_tb_info->db_size_, group_index))) {
      LOG_WDIAG("fail to get physic db index", "db_size", logic_tb_info->db_size_, K(group_index), K(ret));
    }
  }

  // 2.get shard_connector and real db_name
  if (OB_FAIL(ret)) {
    // nothing
  } else if (OB_FAIL(logic_db_info.get_shard_connector_by_index(shard_conn, es_index, last_es_index, group_index,
             is_read_stmt, testload_type, NULL/* logic_tb_info */, sql_result))
             || OB_ISNULL(shard_conn)) {
    LOG_WDIAG("fail to get shard connector", K(table_name), KPC(logic_tb_info), K(ret));
  } else {
    snprintf(real_database_name, db_name_len, "%.*s", shard_conn->database_name_.length(), shard_conn->database_name_.ptr());
    if (*prev_shard_conn != *shard_conn) {
      if (OB_FAIL(change_connector(logic_db_info, client_session, trans_state, prev_shard_conn, shard_conn))) {
        LOG_WDIAG("fail to change connector", KPC(prev_shard_conn), KPC(shard_conn), K(ret));
      }
    }
  }

    // 3.get real table_name
  if (OB_FAIL(ret)) {
    // nothing
  } else if (!hint_table.empty()) {
    snprintf(real_table_name, tb_name_len, "%.*s", static_cast<int>(hint_table.length()), hint_table.ptr());
  } else {
    if (!need_rewrite_table_name) {
      snprintf(real_table_name, tb_name_len, "%.*s", static_cast<int>(table_name.length()), table_name.ptr());
    } else {
      if (tb_index == OBPROXY_MAX_DBMESH_ID) {
        if (1 == logic_tb_info->tb_size_) {
          tb_index = group_index;
        } else {
          tb_index = group_index * (logic_tb_info->tb_size_ / logic_tb_info->db_size_);
        }
      }
      if (OB_FAIL(logic_tb_info->get_real_name_by_index(logic_tb_info->tb_size_, logic_tb_info->tb_suffix_len_,
                        tb_index, logic_tb_info->tb_prefix_.config_string_,
                        logic_tb_info->tb_tail_.config_string_, real_table_name, tb_name_len))) {
        LOG_WDIAG("fail to get real table name", K(tb_index), KPC(logic_tb_info), K(ret));
      }

    }

    if (OB_SUCC(ret) && TESTLOAD_NON != testload_type) {
      snprintf(real_table_name + strlen(real_table_name), tb_name_len - strlen(real_table_name), "_T");
    }
  }

  //如果是当前 session 上的逻辑库, 则下次可以复用这个 group_index
  if (OB_SUCC(ret) && !is_not_saved_database) {
    cs_info.set_group_id(group_index);
    cs_info.set_table_id(tb_index);
    cs_info.set_es_id(es_index);
  }

  if (NULL != shard_conn) {
    shard_conn->dec_ref();
    shard_conn = NULL;
  }

  return ret;
}

int ObProxyShardUtils::handle_scan_all_real_info(ObDbConfigLogicDb &logic_db_info,
                                                 ObMysqlClientSession &client_session,
                                                 ObMysqlTransact::ObTransState &trans_state,
                                                 const ObString &table_name)
{
  int ret = OB_SUCCESS;
  ObProxyMysqlRequest &client_request = trans_state.trans_info_.client_request_;
  ObSqlParseResult &parse_result = client_request.get_parse_result();

  ObSEArray<ObShardConnector*, 4> shard_connector_array;
  ObSEArray<ObShardProp*, 4> shard_prop_array;
  ObSEArray<ObHashMapWrapper<ObString, ObString>, 4> table_name_map_array;
  ObIAllocator *allocator = NULL;

  ObProxyDMLStmt *dml_stmt = static_cast<ObProxyDMLStmt*>(parse_result.get_proxy_stmt());
  if (OB_ISNULL(dml_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("dml stmt is null, unexpected", K(ret));
  } else if (dml_stmt->has_unsupport_expr_type()) {
    ret = OB_ERROR_UNSUPPORT_EXPR_TYPE;
    LOG_WDIAG("unsupport sql", K(ret));
  } else if (OB_FAIL(check_topology(parse_result, logic_db_info))) {
    if (OB_ERR_UNSUPPORT_DIFF_TOPOLOGY != ret) {
      LOG_WDIAG("fail to check topology", K(ret));
    }
  } else if (OB_FAIL(get_global_optimizer_processor().alloc_allocator(allocator))) {
    LOG_WDIAG("alloc allocator failed", K(ret));
  } else if (OB_FAIL(handle_sharding_select_real_info(logic_db_info, client_session, trans_state,
                                                      table_name, *allocator,
                                                      shard_connector_array,
                                                      shard_prop_array,
                                                      table_name_map_array))) {
    LOG_WDIAG("fail to handle sharding select real info", K(ret), K(table_name));
  } else {
    LOG_DEBUG("proxy stmt", K(dml_stmt->get_stmt_type()), K(dml_stmt->limit_offset_), K(dml_stmt->limit_size_));
    LOG_DEBUG("select expr");
    for (int64_t i = 0; i < dml_stmt->select_exprs_.count(); i++) {
      ObProxyExpr::print_proxy_expr(dml_stmt->select_exprs_.at(i));
    }
    LOG_DEBUG("group by expr");
    for (int64_t i = 0; i < dml_stmt->group_by_exprs_.count(); i++) {
      ObProxyExpr::print_proxy_expr(dml_stmt->group_by_exprs_.at(i));
    }
    for (int64_t i = 0; i < dml_stmt->order_by_exprs_.count(); i++) {
      ObProxyOrderItem *order_expr = dml_stmt->order_by_exprs_.at(i);
      LOG_DEBUG("order by expr", K(order_expr->order_direction_));
      ObProxyExpr::print_proxy_expr(order_expr);
    }
    // 处理分布式select
    ObShardingSelectLogPlan* select_plan = NULL;
    void *ptr = NULL;
    if (OB_ISNULL(ptr = allocator->alloc(sizeof(ObShardingSelectLogPlan)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc select plan buf", "size", sizeof(ObShardingSelectLogPlan), K(ret));
    } else {
      select_plan = new(ptr) ObShardingSelectLogPlan(client_request, allocator);
      client_session.set_sharding_select_log_plan(select_plan);
      if (OB_FAIL(select_plan->generate_plan(shard_connector_array, shard_prop_array, table_name_map_array))) {
        LOG_WDIAG("fail to generate plan", K(ret));
      }
    }
  }
  return ret;
}

int ObProxyShardUtils::handle_dml_real_info(ObDbConfigLogicDb &logic_db_info,
                                            ObMysqlClientSession &client_session,
                                            ObMysqlTransact::ObTransState &trans_state,
                                            const ObString &table_name,
                                            const ObString& sql,
                                            ObSqlString& new_sql,
                                            ObSqlParseResult &parse_result,
                                            int64_t& es_index,
                                            int64_t& group_index,
                                            const int64_t last_es_index)
{
  int ret = OB_SUCCESS;

  ObShardConnector *shard_conn = NULL;
  ObClientSessionInfo &session_info = client_session.get_session_info();
  ObShardConnector *prev_shard_conn = session_info.get_shard_connector();

  SqlFieldResult &sql_result = parse_result.get_sql_filed_result();
  ObProxyDMLStmt *dml_stmt = dynamic_cast<ObProxyDMLStmt*>(parse_result.get_proxy_stmt());

  group_index = OBPROXY_MAX_DBMESH_ID;
  int64_t tb_index = OBPROXY_MAX_DBMESH_ID;
  es_index = OBPROXY_MAX_DBMESH_ID;
  ObTestLoadType testload_type = TESTLOAD_NON;
  ObString hint_table;
  ObArenaAllocator allocator;

  ObHashMap<ObString, ObString> table_name_map;
  char real_table_name[OB_MAX_TABLE_NAME_LENGTH];
  char real_database_name[OB_MAX_DATABASE_NAME_LENGTH];

  // 写请求走写权重, 事务内请求都认为是写
  // 读请求走读权重
  bool is_read_stmt = ObProxyShardUtils::is_read_stmt(session_info, trans_state, parse_result);
  bool need_rewrite_table_name = true;

  if (OB_ISNULL(dml_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("dml stmt is null, unexpected", K(ret));
  } else if (OB_FAIL(table_name_map.create(OB_ALIAS_TABLE_MAP_MAX_BUCKET_NUM,
                                           ObModIds::OB_HASH_ALIAS_TABLE_MAP))) {
    LOG_WDIAG("fail to create table name map", K(ret));
  } else if (OB_FAIL(get_shard_hint(table_name, session_info, logic_db_info, parse_result,
                                    group_index, tb_index, es_index,
                                    hint_table, testload_type))) {
    LOG_WDIAG("fail to get shard hint", K(ret));
  } else if (OBPROXY_MAX_DBMESH_ID != group_index
             && OBPROXY_MAX_DBMESH_ID == tb_index) {
    // group id hint, just send direct
    need_rewrite_table_name = false;
  } else {
    // no group id hint, check dml
    if (OB_FAIL(check_dml_sql(table_name, logic_db_info, parse_result))) {
      LOG_WDIAG("fail to check dml sql", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObProxyDMLStmt::ExprMap &table_exprs_map = dml_stmt->get_table_exprs_map();
    ObProxyDMLStmt::ExprMap::iterator iter = table_exprs_map.begin();
    ObProxyDMLStmt::ExprMap::iterator end = table_exprs_map.end();

    if (!hint_table.empty() && table_exprs_map.size() > 1) {
      ret = OB_ERR_MORE_TABLES_WITH_TABLE_HINT;
      LOG_WDIAG("more table with table_name hint", "table size", table_exprs_map.size(), K(hint_table), K(ret));
    } else if (OB_FAIL(logic_db_info.get_shard_table_info(table_name, sql_result, shard_conn,
                                                          real_database_name, OB_MAX_DATABASE_NAME_LENGTH,
                                                          real_table_name, OB_MAX_TABLE_NAME_LENGTH,
                                                          group_index, tb_index, es_index,
                                                          hint_table, testload_type, is_read_stmt, last_es_index))) {
      LOG_WDIAG("fail to get real info", K(table_name), K(group_index), K(tb_index),
               K(es_index), K(hint_table), K(testload_type), K(is_read_stmt), K(ret));
    } else if (OB_ISNULL(shard_conn) || OB_ISNULL(prev_shard_conn)) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WDIAG("shard connector info or prev shard connector info is null", KP(shard_conn),
               KP(prev_shard_conn), K(ret));
    }

    for (; OB_SUCC(ret) && iter != end; iter++) {
      ObProxyExpr *expr = iter->second;
      ObProxyExprTable *table_expr = NULL;
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("expr is null, unexpected", K(ret));
      } else if (OB_ISNULL(table_expr = dynamic_cast<ObProxyExprTable*>(expr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to cast to table expr", K(expr), K(ret));
      } else {
        ObString &sql_table_name = table_expr->get_table_name();
        if (OB_FAIL(logic_db_info.get_real_table_name(sql_table_name, sql_result,
                                                      real_table_name, OB_MAX_TABLE_NAME_LENGTH,
                                                      tb_index, hint_table, testload_type,
                                                      need_rewrite_table_name))) {
          LOG_WDIAG("fail to get real table name", K(sql_table_name), K(tb_index),
                   K(hint_table), K(testload_type), K(ret));
        } else if (OB_FAIL(add_table_name_to_map(allocator, table_name_map, sql_table_name, real_table_name))) {
          LOG_WDIAG("fail to add table name to map", K(sql_table_name), K(real_table_name), K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(rewrite_shard_dml_request(sql, new_sql, parse_result,
                                          session_info.is_oracle_mode(), table_name_map,
                                          ObString::make_string(real_database_name), false))) {
      LOG_WDIAG("fail to rewrite shard request", K(real_database_name), K(ret));
    }
  }

  if (OB_SUCC(ret) && *prev_shard_conn != *shard_conn) {
    if (OB_FAIL(change_connector(logic_db_info, client_session, trans_state, prev_shard_conn, shard_conn))) {
      LOG_WDIAG("fail to change connector", KPC(prev_shard_conn), KPC(shard_conn), K(ret));
    }
  }

  //有可能 SQL 里带 database, 只有是 session 上保存的逻辑库才能保存 group_id, 下次才能复用
  if (OB_SUCC(ret)) {
    ObString saved_database_name;
    session_info.get_logic_database_name(saved_database_name);
    bool is_not_saved_database = saved_database_name != logic_db_info.db_name_.config_string_;
    if (!is_not_saved_database) {
      session_info.set_group_id(group_index);
      session_info.set_table_id(tb_index);
      session_info.set_es_id(es_index);
    }
  }

  if (NULL != shard_conn) {
    shard_conn->dec_ref();
    shard_conn = NULL;
  }
  return ret;
}
// Filter unsupported dml sqls
//
// 1. insert/replace like 'insert into t1(C1, C2) values(1, 2), (null, null), (3, 4);'
// @return the error code.
int ObProxyShardUtils::check_dml_sql(const ObString &table_name,
                                     ObDbConfigLogicDb &logic_db_info,
                                     ObSqlParseResult &parse_result)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(parse_result.is_insert_stmt()
                  || parse_result.is_replace_stmt())) {
    ObShardRule *logic_tb_info = NULL;
    ObProxyDMLStmt *dml_stmt = dynamic_cast<ObProxyDMLStmt*>(parse_result.get_proxy_stmt());
    if (table_name.empty()
        || parse_result.use_column_value_from_hint()
        || OB_ISNULL(dml_stmt)
        || dml_stmt->has_sub_select()) {
      ret = OB_SUCCESS;
      LOG_DEBUG("special insert stmt, no need check");
    } else if (OB_FAIL(logic_db_info.get_shard_rule(logic_tb_info, table_name))) {
      ret = OB_SUCCESS;
      LOG_DEBUG("fail to get shard rule, maybe route by hint", K(table_name));
    } else if (OB_UNLIKELY(parse_result.get_sql_filed_result().fields_.empty())) {
      ret = OB_SUCCESS;
      LOG_DEBUG("no sql fields find, check when calculate", "sql_field_result", parse_result.get_sql_filed_result());
    } else {
      bool need_check = true;
      SqlFieldResult &sql_result = parse_result.get_sql_filed_result();
      ObIArray<SqlField*>& sql_fields = sql_result.fields_;
      const ObIArray<ObString>& shard_key_columns = logic_tb_info->shard_key_columns_;
      int64_t column_num = sql_fields.count();
      int64_t column_value_num = 0;
      int64_t shard_key_num = shard_key_columns.count();
      int64_t shard_key_column_num = 0;
      // find shard key fields
      ObSEArray<int64_t, 4> shard_key_fields_index;
      for (int64_t i = 0; OB_SUCC(ret) && i < column_num; ++i) {
        for (int64_t j = 0; OB_SUCC(ret) && j < shard_key_num; ++j) {
          if (0 == shard_key_columns.at(j).case_compare(sql_fields.at(i)->column_name_.config_string_)) {
            if (OB_FAIL(shard_key_fields_index.push_back(i))) {
              LOG_WDIAG("fail to push back index", K(i), K(j), K(ret));
            } else {
              ++shard_key_column_num;
            }
            break;
          }
        }
      }

      // check shard key column value num
      if (OB_FAIL(ret)) {
        // nothing
      } else if (OB_UNLIKELY(shard_key_fields_index.empty())) {
        need_check = false;
        LOG_DEBUG("no shard key fields find, maybe single table", K(sql_fields), K(ret));
      } else {
        column_value_num = sql_fields.at(shard_key_fields_index[0])->column_values_.count();
        for (int64_t i = 1; OB_SUCC(ret) && i < shard_key_column_num; ++i) {
          if (OB_UNLIKELY(column_value_num
                          != sql_fields.at(shard_key_fields_index[i])->column_values_.count())) {
            ret = OB_NOT_SUPPORTED;
            LOG_WDIAG("different shard key value num, unexpect insert stmt", K(sql_fields), K(column_value_num), K(ret));
          }
        }
      }

      // check all null row
      if (OB_FAIL(ret)) {
        // nothing
      } else if (!need_check) {
        // nothing
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < column_value_num; ++j) {
          int64_t null_value_count = 0;
          for (int64_t i = 0; OB_SUCC(ret) && i < shard_key_column_num; ++i) {
            if (TOKEN_NULL == sql_fields.at(shard_key_fields_index[i])->column_values_[j].value_type_) {
              ++null_value_count;
            }
          }

          if (OB_UNLIKELY(shard_key_column_num == null_value_count)) {
            ret = OB_NOT_SUPPORTED;
            LOG_WDIAG("all shard keys are null in one row is not supported", K(shard_key_column_num),
                      K(null_value_count), K(sql_fields), K(ret));
          }
        }
      }

      // filter null
      if (OB_FAIL(ret)) {
        // nothing
      } else if (!need_check) {
        // nothing
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < shard_key_column_num; ++i) {
          int64_t index = shard_key_fields_index[i];
          ObSEArray<SqlColumnValue, 3> tmp_column_values;
          for (int64_t j = 0;  OB_SUCC(ret) && j < column_value_num; ++j) {
            if (TOKEN_NULL != sql_fields.at(index)->column_values_[j].value_type_) {
              if (OB_FAIL(tmp_column_values.push_back(sql_fields.at(index)->column_values_[j]))) {
                LOG_WDIAG("fail to push back column value", K(i), K(j), "column value", sql_fields.at(index)->column_values_[j]);
              }
            }
          }
          if (OB_FAIL(sql_fields.at(index)->column_values_.assign(tmp_column_values))) {
            LOG_WDIAG("fail to assign column values", K(ret));
          }
        }
      }
    }
  } else {
    // sql except insert/replace
    // nothing
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObProxyShardUtils::get_real_info(ObClientSessionInfo &session_info,
                                     ObDbConfigLogicDb &logic_db_info,
                                     const ObString &table_name,
                                     ObSqlParseResult &parse_result,
                                     ObShardConnector *&shard_conn,
                                     char *real_database_name, int64_t db_name_len,
                                     char *real_table_name, int64_t tb_name_len,
                                     int64_t* group_id, int64_t* table_id, int64_t* es_id, bool is_read_stmt)
{
  int ret = OB_SUCCESS;

  int64_t group_index = OBPROXY_MAX_DBMESH_ID;
  int64_t tb_index = OBPROXY_MAX_DBMESH_ID;
  int64_t es_index = OBPROXY_MAX_DBMESH_ID;
  ObTestLoadType testload_type = TESTLOAD_NON;
  ObString hint_table;
  if (OB_FAIL(get_shard_hint(table_name, session_info, logic_db_info, parse_result,
              group_index, tb_index, es_index, hint_table, testload_type))) {
    LOG_WDIAG("fail to get shard hint", K(ret));
  } else if (OB_FAIL(logic_db_info.get_real_info(table_name, parse_result, shard_conn,
                                          real_database_name, db_name_len,
                                          real_table_name, tb_name_len,
                                          group_index, tb_index, es_index,
                                          hint_table, testload_type, is_read_stmt))) {
    LOG_WDIAG("fail to get real info", K(table_name), K(group_index), K(tb_index),
             K(es_index), K(hint_table), K(testload_type), K(is_read_stmt), K(ret));
  }

  if (NULL != group_id) {
    *group_id = group_index;
  }

  if (NULL != table_id) {
    *table_id = tb_index;
  }

  if (NULL != es_id) {
    *es_id = es_index;
  }

  return ret;
}

int ObProxyShardUtils::is_sharding_in_trans(ObClientSessionInfo &session_info, ObMysqlTransact::ObTransState &trans_state)
{
  return 0 == session_info.get_cached_variables().get_autocommit()
         || trans_state.is_hold_start_trans_
         || trans_state.is_hold_xa_start_
         || ObMysqlTransact::is_in_trans(trans_state);
}

int ObProxyShardUtils::check_login(const ObString &login_reply, const ObString &scramble_str, const ObString &stored_stage2)
{
  int ret = OB_SUCCESS;
  bool pass = false;
  if (OB_FAIL(ObEncryptedHelper::check_login(login_reply, scramble_str, stored_stage2, pass))) {
    LOG_WDIAG("fail to check login", K(login_reply), K(stored_stage2), K(ret));
  } else if (!pass) {
    LOG_WDIAG("password is wrong", K(login_reply), K(stored_stage2), K(ret));
  }
  if (OB_FAIL(ret) || !pass) {
    ret = OB_PASSWORD_WRONG;
  }
  return ret;
}

bool ObProxyShardUtils::check_shard_authority(const ObShardUserPrivInfo &up_info, const ObSqlParseResult &parse_result)
{
  bool bret = true;
  if (parse_result.is_select_stmt()) {
    bret = up_info.priv_set_ & OB_PRIV_SELECT;
  } else if (parse_result.is_insert_stmt()) {
    bret = up_info.priv_set_ & OB_PRIV_INSERT;
  } else if (parse_result.is_update_stmt()) {
    bret = up_info.priv_set_ & OB_PRIV_UPDATE;
  } else if (parse_result.is_replace_stmt()) {
    bret = up_info.priv_set_ & (OB_PRIV_DELETE | OB_PRIV_INSERT);
  } else if (parse_result.is_delete_stmt()) {
    bret = up_info.priv_set_ & OB_PRIV_DELETE;
  } else if (parse_result.is_create_stmt()
             || parse_result.is_rename_stmt()
             || parse_result.is_stop_ddl_task_stmt()
             || parse_result.is_retry_ddl_task_stmt()) {
    bret = up_info.priv_set_ & OB_PRIV_CREATE;
  } else if (parse_result.is_alter_stmt()) {
    bret = up_info.priv_set_ & OB_PRIV_ALTER;
  } else if (parse_result.is_drop_stmt()) {
    bret = up_info.priv_set_ & OB_PRIV_DROP;
  } else if (parse_result.is_truncate_stmt()) {
    bret = up_info.priv_set_ & OB_PRIV_ALTER;
  } else if (parse_result.is_ddl_stmt() || parse_result.is_multi_stmt()) {
    bret = false;
  }
  return bret;
}

int ObProxyShardUtils::build_error_packet(int err_code,
                                          bool &need_response_for_dml,
                                          ObMysqlTransact::ObTransState &trans_state,
                                          ObMysqlClientSession *client_session)
{
  int ret = OB_SUCCESS;
  switch (err_code) {
    case OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR:
    case OB_NOT_SUPPORTED:
    case OB_ERR_UNEXPECTED:
    case OB_INVALID_ARGUMENT_FOR_SUBSTR:
    case OB_ERR_NO_DB_SELECTED:
    case OB_ERR_NO_PRIVILEGE:
    case OB_EXPR_CALC_ERROR:
    case OB_EXPR_COLUMN_NOT_EXIST:
    case OB_OBJ_TYPE_ERROR:
    case OB_ERR_BAD_DATABASE:
    case OB_ERR_NO_DB_PRIVILEGE:
    case OB_ERR_PARSER_SYNTAX:
    case OB_USER_NOT_EXIST:
    case OB_PASSWORD_WRONG:
    case OB_ERR_NO_ZONE_SHARD_TPO:
    case OB_ERR_NO_DEFAULT_SHARD_TPO:
    case OB_ERR_NO_TABLE_RULE:
    case OB_ERR_FORMAT_FOR_TESTLOAD_TABLE_MAP:
    case OB_ERR_MORE_TABLES_WITH_TABLE_HINT:
    case OB_ERR_GET_PHYSIC_INDEX_BY_RULE:
    case OB_ERR_TESTLOAD_ALIPAY_COMPATIBLE:
    case OB_ERR_NULL_DB_VAL_TESTLOAD_TABLE_MAP:
    case OB_ERR_BATCH_INSERT_FOUND:
    case OB_ERROR_UNSUPPORT_EXPR_TYPE:
    case OB_ERR_UNSUPPORT_DIFF_TOPOLOGY:
    case OB_ERR_BAD_FIELD_ERROR:
      need_response_for_dml = true;
      break;
    case OB_ENTRY_NOT_EXIST:
      need_response_for_dml = true;
      err_code = OB_TABLE_NOT_EXIST;
      break;
    case OB_ERR_DISTRIBUTED_NOT_SUPPORTED:
      need_response_for_dml = true;
      err_code = OB_ERR_DISTRIBUTED_TRANSACTION_NOT_SUPPORTED;
      break;
    default:
      ret = err_code;
      break;
  }

  if (need_response_for_dml) {
    trans_state.mysql_errcode_ = err_code;
    trans_state.mysql_errmsg_ = ob_strerror(err_code);
    if (OB_FAIL(ObMysqlTransact::build_error_packet(trans_state, client_session))) {
      LOG_WDIAG("fail to build err resp", K(ret));
    }
  }
  return ret;
}

int ObProxyShardUtils::handle_sharding_select_real_info(ObDbConfigLogicDb &logic_db_info,
                                                        ObMysqlClientSession &client_session,
                                                        ObMysqlTransact::ObTransState &trans_state,
                                                        const ObString table_name,
                                                        ObIAllocator &allocator,
                                                        ObIArray<ObShardConnector*> &shard_connector_array,
                                                        ObIArray<ObShardProp*> &shard_prop_array,
                                                        ObIArray<ObHashMapWrapper<ObString, ObString> > &table_name_map_array)
{
  int ret = OB_SUCCESS;
  ObClientSessionInfo &session_info = client_session.get_session_info();
  ObSqlParseResult &parse_result = trans_state.trans_info_.client_request_.get_parse_result();

  int64_t group_index = OBPROXY_MAX_DBMESH_ID;
  int64_t tb_index = OBPROXY_MAX_DBMESH_ID;
  int64_t es_index = OBPROXY_MAX_DBMESH_ID;
  ObTestLoadType testload_type = TESTLOAD_NON;
  ObString hint_table;

  bool is_read_stmt = ObProxyShardUtils::is_read_stmt(session_info, trans_state, parse_result);

  if (OB_FAIL(get_shard_hint(table_name, session_info, logic_db_info, parse_result,
                             group_index, tb_index, es_index,
                             hint_table, testload_type))) {
    LOG_WDIAG("fail to get shard hint", K(ret));
  } else if (OB_FAIL(logic_db_info.get_sharding_select_info(table_name, parse_result,
                                                            testload_type, is_read_stmt, es_index,
                                                            allocator,
                                                            shard_connector_array,
                                                            shard_prop_array,
                                                            table_name_map_array))) {
    LOG_WDIAG("fail to get sharding select info", K(table_name),
             K(testload_type), K(is_read_stmt), K(ret));
  }

  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
