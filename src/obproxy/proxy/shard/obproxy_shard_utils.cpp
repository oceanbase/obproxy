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
    LOG_WARN("fail to get_logic_tenant_name", K(ret));
  } else if (OB_ISNULL(db_info = get_global_dbconfig_cache().get_exist_db_info(logic_tenant_name, db_name))) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("logic db does not exist", K(ret), K(db_name), K(logic_tenant_name));
  } else if (session_info.enable_shard_authority()) {
    ObShardUserPrivInfo up_info;
    if (OB_FAIL(db_info->get_user_priv_info(username, host, up_info))) {
      LOG_WARN("fail to get user priv info", K(db_name), K(username), K(host), K(ret));
    } else if (saved_up_info.password_stage2_ != up_info.password_stage2_) {
      ret = OB_PASSWORD_WRONG;
      LOG_WARN("pass word is wrong", K(saved_up_info), K(up_info), K(ret));
    } else {
      saved_up_info.assign(up_info);
    }
    if (OB_FAIL(ret)) {
      ret = OB_ERR_NO_DB_PRIVILEGE;
    }
  } else if (!client_session.is_local_connection() && !db_info->enable_remote_connection()) {
    ret = OB_ERR_NO_DB_PRIVILEGE;
    LOG_WARN("Access denied for database from remote addr", K(db_name), K(ret));
  }

  // switch logic database, random get one shard connector
  if (OB_SUCC(ret)) {
    if (OB_FAIL(db_info->acquire_random_shard_connector(shard_conn))) {
      LOG_WARN("fail to get random shard connector", K(ret), KPC(db_info));
    } else if (*prev_shard_conn != *shard_conn) {
      if (OB_FAIL(change_connector(*db_info, client_session, trans_state, prev_shard_conn, shard_conn))) {
        LOG_WARN("fail to change connector", KPC(prev_shard_conn), KPC(shard_conn), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    session_info.set_group_id(OBPROXY_MAX_DBMESH_ID);
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

int ObProxyShardUtils::change_connector(ObDbConfigLogicDb &logic_db_info,
                                        ObMysqlClientSession &client_session,
                                        ObMysqlTransact::ObTransState &trans_state,
                                        const ObShardConnector *prev_shard_conn,
                                        ObShardConnector *shard_conn)
{
  int ret = OB_SUCCESS;

  ObClientSessionInfo &session_info = client_session.get_session_info();

  if (!shard_conn->is_same_connection(prev_shard_conn)) {
    // need use ObMysqlTransact::is_in_trans, not use is_sharding_in_trans
    // because first SQL should not think as in trans
    if (OB_UNLIKELY(ObMysqlTransact::is_in_trans(trans_state))) {
      ret = OB_ERR_DISTRIBUTED_NOT_SUPPORTED;
      LOG_WARN("not support distributed transaction", K(trans_state.current_.state_),
               K(trans_state.is_auth_request_),
               K(trans_state.is_hold_start_trans_),
               KPC(prev_shard_conn),
               KPC(shard_conn), K(ret));
    } else {
      session_info.set_allow_use_last_session(false);
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(handle_sys_read_consitency_prop(logic_db_info, *shard_conn, session_info))) {
      LOG_WARN("fail to handle_sys_read_consitency_prop", KPC(shard_conn));
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
    // single schema table mode, need sync schema
    session_info.set_database_name(shard_conn->database_name_.config_string_);

    if (DB_OB_MYSQL == shard_conn->server_type_ || DB_OB_ORACLE == shard_conn->server_type_) {
      bool is_cluster_changed = (prev_shard_conn->cluster_name_ != shard_conn->cluster_name_);
      bool is_tenant_changed = (prev_shard_conn->tenant_name_ != shard_conn->tenant_name_);
      if (OB_FAIL(change_user_auth(client_session, *shard_conn, is_cluster_changed, is_tenant_changed))) {
        LOG_ERROR("fail to change user auth", KPC(shard_conn), K(is_cluster_changed), K(is_tenant_changed), K(ret));
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
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("not support beyond trust password", K(ret));
  }
  if (OB_FAIL(ObProxySessionInfoHandler::rewrite_login_req_by_sharding(session_info, full_user_name,
              password, database, tenant_name, cluster_name))) {
    LOG_WARN("fail to rewrite_login_req_by_sharding", K(full_user_name), K(tenant_name), K(cluster_name), K(ret));
  }

  return ret;
}

// caller will erase remove quoto, so here need add quoto
void ObProxyShardUtils::replace_oracle_table(ObSqlString &new_sql, const ObString &real_name,
                                             bool &hava_quoto, bool is_single_shard_db_table,
                                             bool is_database)
{
  if (is_database) {
    new_sql.append("\"", 1);
    new_sql.append(real_name);
    new_sql.append("\"", 1);
  } else {
    if (hava_quoto) {
      new_sql.append("\"", 1);
      new_sql.append(real_name);
      new_sql.append("\"", 1);
    } else {
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

int ObProxyShardUtils::do_rewrite_shard_select_request(const ObString &sql,
                                                       ObSqlParseResult &parse_result,
                                                       bool is_oracle_mode,
                                                       const ObHashMap<ObString, ObString> &table_name_map,
                                                       const ObString &real_database_name,
                                                       bool is_single_shard_db_table,
                                                       ObSqlString &new_sql)
{
  int ret = OB_SUCCESS;

  const uint32_t PARSE_EXTRA_CHAR_NUM = 2;
  const char *sql_ptr = sql.ptr();
  int64_t sql_len = sql.length();
  int64_t copy_pos = 0;

  ObProxySelectStmt *select_stmt = static_cast<ObProxySelectStmt*>(parse_result.get_proxy_stmt());
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null select stmt", K(ret));
  } else {
    ObProxySelectStmt::TablePosArray &table_pos_array = select_stmt->get_table_pos_array();
    std::sort(table_pos_array.begin(), table_pos_array.end());

    for (int64_t i = 0; OB_SUCC(ret) && i < table_pos_array.count(); i++) {
      ObProxyExprTablePos &expr_table_pos = table_pos_array.at(i);
      ObProxyExprTable *expr_table = expr_table_pos.get_table_expr();

      if (OB_ISNULL(expr_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null expr table", K(ret));
      } else {
        bool database_hava_quoto = false;
        bool table_hava_quoto = false;
        int64_t table_pos = expr_table_pos.get_table_pos();
        int64_t database_pos = expr_table_pos.get_database_pos();

        ObString &database_name = expr_table->get_database_name();
        ObString &table_name = expr_table->get_table_name();
        uint64_t database_len = database_name.length();
        uint64_t table_len = table_name.length();

        ObString real_table_name;
        if (OB_FAIL(table_name_map.get_refactored(table_name, real_table_name))) {
          LOG_WARN("fail to get real table name", K(table_name), K(ret));
        } else {
          if (*(sql_ptr + table_pos - 1) == '`' || *(sql_ptr + table_pos - 1) == '"') {
            table_hava_quoto = true;
            table_pos -= 1;
            table_len += 2;
          }

          // replace database
          if (database_pos > 0) {
            // If there is database in SQL
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
            new_sql.append(sql_ptr + copy_pos, table_pos - copy_pos);
          } else {
            // If there is no database in SQL, single database and single table will not be added.
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

          // replace table name
          if (is_oracle_mode) {
            replace_oracle_table(new_sql, real_table_name, table_hava_quoto, is_single_shard_db_table, false);
          } else {
            new_sql.append(real_table_name);
          }
          copy_pos = table_pos + table_len;
        }
      }
    }
  }


  if (OB_SUCC(ret)) {
    new_sql.append(sql_ptr + copy_pos, sql_len - copy_pos - PARSE_EXTRA_CHAR_NUM);
  }

  return ret;
}

int ObProxyShardUtils::rewrite_shard_select_request(ObClientSessionInfo &session_info,
                                                    ObProxyMysqlRequest &client_request,
                                                    ObIOBufferReader &client_buffer_reader,
                                                    const ObHashMap<ObString, ObString> &table_name_map,
                                                    const ObString &real_database_name,
                                                    bool is_single_shard_db_table)
{
  int ret = OB_SUCCESS;

  ObSqlParseResult &parse_result = client_request.get_parse_result();
  ObSqlString new_sql;

  if (OB_FAIL(do_rewrite_shard_select_request(client_request.get_parse_sql(), parse_result,
                                              session_info.is_oracle_mode(), table_name_map,
                                              real_database_name, is_single_shard_db_table,
                                              new_sql))) {
  } else {
    // 4. push reader forward by consuming old buffer and write new sql into buffer
    if (OB_FAIL(client_buffer_reader.consume_all())) {
      LOG_WARN("fail to consume all", K(ret));
    } else {
      ObMIOBuffer *writer = client_buffer_reader.mbuf_;
      if (OB_ISNULL(writer)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null values ", K(writer), K(ret));
        // no need compress here, if server session support compress, it will compress later
      } else if (OB_FAIL(ObMysqlRequestBuilder::build_mysql_request(*writer, obmysql::OB_MYSQL_COM_QUERY, new_sql.string(), false, false))) {
        LOG_WARN("fail to build_mysql_request", K(new_sql), K(ret));
      } else if (OB_FAIL(ObProxySessionInfoHandler::rewrite_query_req_by_sharding(session_info, client_request, client_buffer_reader))) {
        LOG_WARN("fail to rewrite_query_req_by_sharding", K(ret));
      }
    }
  }

  return ret;
}

// MySQL use single quoto to table, case-insensitive
// Oracle use double quoto, case-sensitive, The default is uppercase
//  if single schema table mode, table from SQL:
//     if have quoto, add quoto
//     if no quoto, not add quoto
//  if sharding mode, table from config, both add quoto
int ObProxyShardUtils::rewrite_shard_request(ObClientSessionInfo &session_info,
                                             ObProxyMysqlRequest &client_request,
                                             ObIOBufferReader &client_buffer_reader,
                                             const ObString &table_name, const ObString &database_name,
                                             const ObString &real_table_name, const ObString &real_database_name,
                                             bool is_single_shard_db_table)
{
  int ret = OB_SUCCESS;

  const uint32_t PARSE_EXTRA_CHAR_NUM = 2;
  uint64_t table_len = table_name.length();
  int64_t dml_tb_pos = client_request.get_parse_result().get_dbmesh_route_info().tb_pos_;
  uint64_t index_table_len = table_name.length();
  int64_t index_table_pos = client_request.get_parse_result().get_dbmesh_route_info().index_tb_pos_;

  ObString sql = client_request.get_parse_sql();
  common::ObSqlString new_sql;
  const char *sql_ptr = sql.ptr();
  int64_t sql_len = sql.length();

  bool database_hava_quoto = false;
  bool table_hava_quoto = false;
  int64_t table_pos = dml_tb_pos;
  int64_t database_pos = -1;
  int64_t copy_pos = 0;
  ObString tmp_str;

  // replace index table name in hint
  if (index_table_pos > 0) {
    if (*(sql_ptr + index_table_pos - 1) == '`' || *(sql_ptr + index_table_pos - 1) == '"') {
      table_hava_quoto = true;
      index_table_pos -= 1;
      index_table_len += 2;
    }
    new_sql.append(sql_ptr + copy_pos, index_table_pos);

    if (session_info.is_oracle_mode()) {
      replace_oracle_table(new_sql, real_table_name, table_hava_quoto, is_single_shard_db_table, false);
    } else {
      new_sql.append(real_table_name);
    }

    copy_pos = index_table_pos + index_table_len;
  }

  if (*(sql_ptr + table_pos - 1) == '`' || *(sql_ptr + table_pos - 1) == '"') {
    table_hava_quoto = true;
    table_pos -= 1;
    table_len = table_name.length() + 2;
  }

  // get database pos, check whether have database or not
  // TODO: get database pos by parser
  bool sql_has_database = !client_request.get_parse_result().get_origin_database_name().empty();
  if (sql_has_database && *(sql_ptr + table_pos - 1) == '.') {
    database_pos = table_pos - 1 - database_name.length();
    if (*(sql_ptr + table_pos - 2) == '`' || *(sql_ptr + table_pos - 2) == '"') {
      database_hava_quoto = true;
      database_pos -= 2;
      tmp_str.assign_ptr(sql_ptr + database_pos + 1, static_cast<size_t>(database_name.length()));
    } else {
      tmp_str.assign_ptr(sql_ptr + database_pos, static_cast<size_t>(database_name.length()));
    }
    if (database_name == tmp_str) {
      // do nothing
    } else {
      database_pos = -1;
    }
  }

  // replace database
  if (database_pos > 0) {
    new_sql.append(sql_ptr + copy_pos, database_pos - copy_pos);

    if (session_info.is_oracle_mode()) {
      replace_oracle_table(new_sql, real_database_name, database_hava_quoto, is_single_shard_db_table, true);
    } else {
      new_sql.append(real_database_name);
    }

    new_sql.append(".", 1);
  } else {
    // if SQL not have database, no need add database in single mode
    // add real database name before logic table name
    new_sql.append(sql_ptr + copy_pos, table_pos - copy_pos);
    if (!is_single_shard_db_table && !database_name.empty()) {

      if (session_info.is_oracle_mode()) {
        replace_oracle_table(new_sql, real_database_name, database_hava_quoto, is_single_shard_db_table, true);
      } else {
        new_sql.append(real_database_name);
      }
      new_sql.append(".", 1);
    }
  }

  // replace table name
  if (session_info.is_oracle_mode()) {
    replace_oracle_table(new_sql, real_table_name, table_hava_quoto, is_single_shard_db_table, false);
  } else {
    new_sql.append(real_table_name);
  }

  copy_pos = table_pos + table_len;
  new_sql.append(sql_ptr + copy_pos, sql_len - copy_pos - PARSE_EXTRA_CHAR_NUM);

  // 4. push reader forward by consuming old buffer and write new sql into buffer
  if (OB_FAIL(client_buffer_reader.consume_all())) {
    LOG_WARN("fail to consume all", K(ret));
  } else {
    ObMIOBuffer *writer = client_buffer_reader.mbuf_;
    if (OB_ISNULL(writer)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null values ", K(writer), K(ret));
      // no need compress here, if server session support comrpess, it will compress later
    } else if (OB_FAIL(ObMysqlRequestBuilder::build_mysql_request(*writer, obmysql::OB_MYSQL_COM_QUERY, new_sql.string(), false, false))) {
      LOG_WARN("fail to build_mysql_request", K(new_sql), K(ret));
    } else if (OB_FAIL(ObProxySessionInfoHandler::rewrite_query_req_by_sharding(session_info, client_request, client_buffer_reader))) {
      LOG_WARN("fail to rewrite_query_req_by_sharding", K(ret));
    }
  }

  return ret;
}

int ObProxyShardUtils::testload_check_obparser_node_is_valid(const ParseNode *node, const ObItemType &type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("checkout node failed: node is NULL", K(ret), K(type));
  } else if (type != node->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("checkout node failed: type is not equal", K(ret), K(type), K(node->type_));
  } else {
    switch (type) {
      case T_HINT_OPTION_LIST: {
        if (T_HINT_OPTION_LIST != node->type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("checkout node failed: node type error", K(ret), K(type), K(node->type_));
        }
        break;
      }
      case T_INDEX: {
        if (OB_T_INDEX_NUM_CHILD /* 3 */ != node->num_child_
                || OB_ISNULL(node->children_[1])
                || T_RELATION_FACTOR_IN_HINT != (node->children_[1]->type_)
                || OB_T_RELATION_FACTOR_IN_HINT_NUM_CHILD /* 2 */ != node->children_[1]->num_child_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("error T_INDEX node for sql to handle", K(ret));
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
         LOG_WARN("unexpected T_RELATION_FACTOR node", K(ret));
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
          LOG_WARN("unexpected T_COLUMN_REF node", K(ret));
        }
        break;
      }
      case T_IDENT: {
        if (OB_T_IDENT_NUM_CHILD /* 0 */ != node->num_child_
              || OB_ISNULL(node->str_value_)
              || 0 > node->token_off_
              || 0 > node->token_len_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected T_IDENT node", K(ret));
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
    LOG_WARN("checkout node failed: node is NULL", K(ret));
  } else {
    db_node = NULL;
    table_node = NULL;
    const ObItemType &type = root->type_;
    
    if (T_RELATION_FACTOR != type && T_COLUMN_REF != type) {
      LOG_WARN("not is not T_RELATION_FACTOR or T_COLUMN_REF node", K(ret), K(type));
    } else if (OB_FAIL(testload_check_obparser_node_is_valid(root, type))) {
      LOG_WARN("not is not invalid  T_RELATION_FACTOR or T_COLUMN_REF node", K(ret), K(type));
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
    LOG_WARN("use NULL node to rewrite sql", K(ret), K(last_pos));
  } else if (OB_FAIL(testload_check_obparser_node_is_valid(node, T_IDENT))) { //check T_IDENT node
    LOG_WARN("use invalid node to rewrite sql", K(ret), K(last_pos));
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
            LOG_WARN("error T_INDEX node for sql to handle", K(ret), K(sql_ptr));
          } else if ((OB_ISNULL(relation_node = index_node->children_[1]->children_[0]))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("error T_RELATION_FACTOR_IN_HINT node for sql to handle", K(ret), K(sql_ptr));
          // relation_node will be check when use
          } else if (OB_FAIL(testload_get_obparser_db_and_table_node(relation_node, db_node, tb_node))) {
            LOG_WARN("fail get table_node or db_node from T_RELATION_FACTOR", K(ret), K(relation_node),
                      K(db_node), K(tb_node));
          } else if (OB_NOT_NULL(db_node) // rewrite db node
                       && OB_FAIL(testload_rewrite_name_base_on_parser_node(new_sql, real_database_name.ptr(),
                                  sql_ptr, sql_len, last_pos, db_node))) {
            LOG_WARN("fail to rewrite db_node for T_RELATION_FACTOR", K(ret), K(db_node));
          } else {
            // table_name_len must be less than OB_MAX_TABLE_NAME_LENGTH
            std::string new_name;
            std::string table_name = std::string(tb_node->str_value_, tb_node->token_len_); //use origin table_name
            if (use_hint_table_name) {
              new_name.append(hint_table.ptr()); // use hint table
            } else if (OB_FAIL(logic_db_info.check_and_get_testload_table(table_name, new_name))) {
              LOG_WARN("check and get table name is fail", K(ret));
            }
            if (OB_SUCC(ret) // rewrite table node
                  && OB_FAIL(testload_rewrite_name_base_on_parser_node(new_sql, new_name.c_str(),
                                                             sql_ptr, sql_len, last_pos, tb_node))) {
              LOG_WARN("fail to rewrite db_node for T_RELATION_FACTOR", K(ret), K(tb_node));
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
 *   @param	client_buffer_reader	: buffer to rewrite request 
 *   @param	is_single_shard_db_table: 
 *   @param	real_database_name	: real database name in Server
 *   @param	logic_db_info		: config info for database
 * */
int ObProxyShardUtils::testload_check_and_rewrite_testload_request(ObClientSessionInfo &session_info,
                                    ObProxyMysqlRequest &client_request,
                                    ObIOBufferReader &client_buffer_reader,
                                    bool is_single_shard_db_table,
                                    const ObString &hint_table,
                                    const ObString &real_database_name,
                                    ObDbConfigLogicDb &logic_db_info)
{
  int ret = OB_SUCCESS; 
  UNUSED(is_single_shard_db_table);
  ObSqlParseResult &parse_result = client_request.get_parse_result();
  ObString sql = client_request.get_sql();

  common::ObSqlString new_sql; //init by default
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
    LOG_WARN("failed to create alias_table_map map");
  } else if (OB_FAIL(all_table_map.create(OB_ALIAS_TABLE_MAP_MAX_BUCKET_NUM,
                                          ObModIds::OB_HASH_ALIAS_TABLE_MAP))) {
    LOG_WARN("failed to create all_table_map map");
  } else if (OB_ISNULL(parse_result.get_ob_parser_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to init obparser result tree:", K(ret));
  } else if (OB_FAIL(ObProxySqlParser::ob_load_testload_parse_node(
                                   parse_result.get_ob_parser_result()->result_tree_,
                                   1, /* level */
                                   relation_table_node,
                                   //ref_column_node,
                                   hint_option_list,
                                   all_table_map,
                                   alias_table_map,
                                   allocator))) {
    LOG_WARN("ob_load_testload_parse_node failed", K(ret));
  } else if (use_hint_table_name && 1 != (all_table_map.size())) {
    // There is more than one table in SQL, alias table node will be added in all table name
    ret = OB_ERR_MORE_TABLES_WITH_TABLE_HINT;
    LOG_WARN("check_and_rewrite_testload_request failed for more table in testload request with table_name hint",
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
      LOG_WARN("check_and_rewrite_testload_hint_index failed", K(ret), K(last_pos), K(new_sql));
    }

    while (OB_SUCC(ret) && i < relation_table_node.count()) {
      db_node = NULL;
      if (OB_ISNULL(node = relation_table_node[i]->get_node())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected node in relation_table_node", K(ret));
      } else {
        switch (node->type_) {
          case T_RELATION_FACTOR:
            if (OB_FAIL(testload_check_obparser_node_is_valid(node, T_RELATION_FACTOR))) {
              LOG_WARN("unexpected node T_RELATION_FACTOR", K(ret));
            } else if (OB_FAIL(testload_get_obparser_db_and_table_node(node, db_node, node))){
              LOG_WARN("get tb_node or db_node fail for T_RELATION_FACTOR", K(ret));
            } else {
              LOG_DEBUG("will check the table is testload table", K(ret), "table", (node->str_value_));
            }
            break;
          case T_COLUMN_REF:
            if (OB_FAIL(testload_check_obparser_node_is_valid(node, T_COLUMN_REF))) {
              LOG_WARN("unexpected node T_RELATION_FACTOR", K(ret));
            } else if (OB_FAIL(testload_get_obparser_db_and_table_node(node, db_node, node))){
              LOG_WARN("get tb_node or db_node fail for T_RELATION_FACTOR", K(ret));
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
          LOG_WARN("it is not valid table node", K(ret));
        } else {
          LOG_DEBUG("ObProxyShardUtils::check_and_rewrite_testload_request before rewrite",
                    K(i), K(last_pos), K(node->token_off_), K(new_sql), K(last_pos));
          std::string table_name = std::string(node->str_value_, node->token_len_);

          if (OB_NOT_NULL(db_node)
                 && OB_FAIL(testload_rewrite_name_base_on_parser_node(new_sql, real_database_name.ptr(),
                                                                      sql_ptr, sql_len, last_pos, db_node))) { 
            LOG_WARN("check and rewrite db name is fail", K(ret), "db_name", real_database_name.ptr());
          // rewrite table name
          } else if (use_hint_table_name) {
            new_name.clear();
            new_name.append(hint_table.ptr());
          } else if (OB_FAIL(logic_db_info.check_and_get_testload_table(table_name, new_name))) {
            LOG_WARN("check and rewrite table name is fail", K(ret), "table_name", node->str_value_);
          }
          if (OB_SUCC(ret) && OB_FAIL(testload_rewrite_name_base_on_parser_node(new_sql, new_name.c_str(), sql_ptr,
                                                                                sql_len, last_pos, node))) {
            LOG_WARN("check and rewrite table name is fail", K(ret), "table_name", new_name.c_str());
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
  //rewrite sql base on new_sql
  if (OB_SUCC(ret)) {
    if (OB_FAIL(client_buffer_reader.consume_all())) {
      LOG_WARN("fail to consume all", K(ret));
    } else {
      ObMIOBuffer *writer = client_buffer_reader.mbuf_;
      if (OB_ISNULL(writer)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null values ", K(writer), K(ret));
        // no need compress here, if server session support compress, it will compress later
      } else if (OB_FAIL(ObMysqlRequestBuilder::build_mysql_request(*writer, obmysql::OB_MYSQL_COM_QUERY, new_sql.string(), false, false))) {
        LOG_WARN("fail to build_mysql_request", K(new_sql), K(ret));
      } else if (OB_FAIL(ObProxySessionInfoHandler::rewrite_query_req_by_sharding(session_info, client_request, client_buffer_reader))) {
        LOG_WARN("fail to rewrite_query_req_by_sharding", K(ret));
      }
    } 
  }
//*/
  return ret;
}

bool ObProxyShardUtils::is_special_db(ObMysqlTransact::ObTransState &s)
{
  const ObString &database_name = s.trans_info_.client_request_.get_parse_result().get_origin_database_name();
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
      LOG_WARN("no database selected", K(ret));
    } else {
      ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();
      // 0. get db name. if there is not db name in sql, get the saved logic db name
      if (database_name.empty()) {
        database_name = saved_database_name;
      }
      // 1. get real db_name and table_name
      if (OB_FAIL(session_info.get_logic_tenant_name(logic_tenant_name))) {
        ret = OB_ERR_UNEXPECTED; // no need response, just return ret and disconnect
        LOG_ERROR("fail to get logic tenant name", K(ret));
      } else if (OB_ISNULL(logic_db_info = dbconfig_cache.get_exist_db_info(logic_tenant_name, database_name))) {
        ret = OB_ERR_BAD_DATABASE;
        LOG_WARN("database not exist", K(logic_tenant_name), K(database_name));
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
  ObHSRResult &hsr = session_info.get_login_req().get_hsr_result();
  const ObString &database_name = logic_db_info.db_name_.config_string_;
  ObString saved_database_name;
  session_info.get_logic_database_name(saved_database_name);
  if (session_info.enable_shard_authority()) {
    bool sql_has_database = !parse_result.get_origin_database_name().empty();
    const ObShardUserPrivInfo &up_info = session_info.get_shard_user_priv();
    if (sql_has_database && database_name != saved_database_name) {
      const ObString &username = hsr.user_name_;
      const ObString &host = session_info.get_client_host();
      ObShardUserPrivInfo new_up_info;
      if (OB_FAIL(logic_db_info.get_user_priv_info(username, host, new_up_info))) {
        ret = OB_ERR_NO_DB_PRIVILEGE;
        LOG_WARN("fail to get user priv info for new database", K(database_name), K(username), K(host), K(ret));
      } else if (OB_UNLIKELY(up_info.password_stage2_ != new_up_info.password_stage2_)) {
        // client does not change password, so here we assume same user has same password in different logic db
        // orignal login password is not saved, we just use stage2 to check password
        ret = OB_ERR_NO_DB_PRIVILEGE;
        LOG_WARN("fail to check password for new database", K(new_up_info), K(up_info), K(ret));
      } else if (!check_shard_authority(new_up_info, parse_result)) {
        ret = OB_ERR_NO_PRIVILEGE;
        LOG_WARN("no privilege to do stmt", K(new_up_info), K(parse_result), K(ret));
      }
    } else {
      if (!check_shard_authority(up_info, parse_result)) {
        ret = OB_ERR_NO_PRIVILEGE;
        LOG_WARN("no privilege to do stmt", K(up_info), K(parse_result), K(ret));
      }
    }
  } else if (!client_session.is_local_connection() && !logic_db_info.enable_remote_connection()) {
    ret = OB_ERR_NO_DB_PRIVILEGE;
    LOG_WARN("Access denied for database from remote addr", K(database_name), K(ret));
  }
  return ret;
}

int ObProxyShardUtils::get_shard_hint(ObDbConfigLogicDb &logic_db_info,
                                      ObSqlParseResult &parse_result,
                                      int64_t &group_index, int64_t &tb_index,
                                      int64_t &es_index, ObString &table_name,
                                      ObTestLoadType &testload_type)
{
  int ret = OB_SUCCESS;
  DbMeshRouteInfo &odp_route_info = parse_result.get_dbmesh_route_info();
  DbpRouteInfo &dbp_route_info = parse_result.get_dbp_route_info();
  if (odp_route_info.is_valid()) {
    group_index = parse_result.get_dbmesh_route_info().group_idx_;
    table_name = parse_result.get_dbmesh_route_info().table_name_;
    if (table_name.empty()) {
      tb_index =  parse_result.get_dbmesh_route_info().tb_idx_;
    }
    es_index = parse_result.get_dbmesh_route_info().es_idx_;
    testload_type = get_testload_type(parse_result.get_dbmesh_route_info().testload_);
    const ObString &disaster_status = parse_result.get_dbmesh_route_info().disaster_status_;
    // if disaster status exist and valid, use disaster_eid
    // if failed, ignore
    if (!disaster_status.empty() && OB_FAIL(logic_db_info.get_disaster_eid(disaster_status, es_index))) {
      LOG_DEBUG("fail to get disaster elastic id", K(disaster_status), K(ret));
      ret = OB_SUCCESS;
    }
  } else if (dbp_route_info.is_valid()) {
    if (parse_result.get_dbp_route_info().is_group_info_valid()) {
      group_index = dbp_route_info.group_idx_;
      table_name = dbp_route_info.table_name_;
    }
  }
  return ret;
}

bool ObProxyShardUtils::is_read_stmt(ObClientSessionInfo &session_info, ObMysqlTransact::ObTransState &trans_state,
                                     ObSqlParseResult& parse_result)
{
  // Write request using write weight
  //   All requests within the transaction are considered to be write
  // Read request using read weight
  return (!is_sharding_in_trans(session_info, trans_state)
          && (parse_result.is_show_stmt()
              || parse_result.is_desc_stmt()
              || (parse_result.is_select_stmt() && !parse_result.has_for_update())
              || parse_result.is_set_stmt() //[set ac = 0] is internal request, will not be here
              || parse_result.has_explain()));
}

/*
 * if no table name
 *   use last shard connector
 *
 * if no hint
 *   Read and write separation based on weight 
 *
 * if have hint
 *   it have table_id and 0, ignore. otherwise report an error
 *   if have group_id and 0, ignore. otherwise report an error
 *   if have testload or table name
 *     if sql do not have table name, report an error
 *     if sql have table name, rewrite sql
 *   if have es_id, route based on es_id. otherwise based on weight  
 *   ignore other hint
 */
int ObProxyShardUtils::do_handle_single_shard_request(ObMysqlClientSession &client_session,
                                                      ObMysqlTransact::ObTransState &trans_state,
                                                      ObIOBufferReader &client_buffer_reader,
                                                      ObDbConfigLogicDb &logic_db_info)
{
  int ret = OB_SUCCESS;

  ObClientSessionInfo &session_info = client_session.get_session_info();
  session_info.set_enable_reset_db(true);

  ObShardConnector *shard_conn = NULL;
  ObShardConnector *prev_shard_conn = session_info.get_shard_connector();

  ObProxyMysqlRequest &client_request = trans_state.trans_info_.client_request_;
  ObSqlParseResult &parse_result = client_request.get_parse_result();
  ObString table_name = client_request.get_parse_result().get_origin_table_name();
  int64_t group_index = OBPROXY_MAX_DBMESH_ID;
  int64_t tb_index = OBPROXY_MAX_DBMESH_ID;
  int64_t es_index = OBPROXY_MAX_DBMESH_ID;
  ObTestLoadType testload_type = TESTLOAD_NON;
  ObString hint_table;
  bool is_need_rewrite_sql = false;
  bool is_skip_rewrite_check = false;
  bool is_need_skip_router = false;
  if (OB_FAIL(get_shard_hint(logic_db_info, parse_result,
              group_index, tb_index, es_index, hint_table, testload_type))) {
    LOG_WARN("fail to get shard hint", K(ret));
  } else {
    // some case can skip route:
    //  1. if specify group_index, tb_index, es_index
    //  2. if in trans, use last shard connector
    //  3. special sql, use last shard connector
    is_need_skip_router = (!(OBPROXY_MAX_DBMESH_ID != es_index && 0 != es_index)
                             && ((is_sharding_in_trans(session_info, trans_state))
                              || parse_result.is_show_tables_stmt()
                              || (parse_result.is_select_stmt() && parse_result.has_last_insert_id())));
    
    // some case can skip rewrite check:
    //   1. if sql have table name, can not skip
    //   2. if sql have no table name
    //      2.1 SHOW_TABLES/SHOW_DATABASE
    //      2.2 SELECT LAST_INSERT_ID
    //      2.3 COMMIT
    //      2.4 ROLLBACK
    is_skip_rewrite_check = table_name.empty() && (parse_result.is_show_tables_stmt()
                                                  || (parse_result.is_select_stmt() && parse_result.has_last_insert_id())
                                                  || parse_result.is_commit_stmt()
                                                  || parse_result.is_rollback_stmt()); 
    // three case need rewrite sql:
    //   1. sql have db
    //   2. have testload flag
    //   3. specify table in hint
    is_need_rewrite_sql = (!is_skip_rewrite_check) && (!client_request.get_parse_result().get_origin_database_name().empty()
                          || TESTLOAD_NON != testload_type
                          || !hint_table.empty());

    // if need rewrite sql, but not find table name in sql, report an error
    if ((OBPROXY_MAX_DBMESH_ID != tb_index && 0 != tb_index)
        || (OBPROXY_MAX_DBMESH_ID != group_index && 0 != group_index)
        || (is_need_rewrite_sql && table_name.empty())) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("sql with dbmesh route hint and no table name is unsupported", K(tb_index), K(group_index), K(is_need_rewrite_sql), K(ret));
     }
  }

  if (OB_SUCC(ret)) {
    char real_table_name[OB_MAX_TABLE_NAME_LENGTH];
    char real_database_name[OB_MAX_DATABASE_NAME_LENGTH];

    if (!is_need_skip_router) {
      bool is_read_stmt = ObProxyShardUtils::is_read_stmt(session_info, trans_state, parse_result);

      if (OB_FAIL(logic_db_info.get_single_table_info(table_name, shard_conn,
                                                      real_database_name, OB_MAX_DATABASE_NAME_LENGTH,
                                                      real_table_name, OB_MAX_TABLE_NAME_LENGTH,
                                                      group_index, tb_index, es_index,
                                                      hint_table, testload_type, is_read_stmt))) {
        LOG_WARN("shard tpo info is null", K(ret));
      } else if (OB_ISNULL(shard_conn) || OB_ISNULL(prev_shard_conn)) {
        ret = OB_EXPR_CALC_ERROR;
        LOG_WARN("shard connector info or prev shard connector info is null", KP(shard_conn),
                 KP(prev_shard_conn), K(ret));
      } else if (*prev_shard_conn != *shard_conn) {
        if (OB_FAIL(change_connector(logic_db_info, client_session, trans_state, prev_shard_conn, shard_conn))) {
          LOG_WARN("fail to change connector", KPC(prev_shard_conn), KPC(shard_conn), K(ret));
        }
      } 
      if (OB_NOT_NULL(shard_conn)) {
        shard_conn->dec_ref();
        shard_conn = NULL;
      }
    } else { // is_need_skip_router == true, write new table_name and database_name
      if (OB_NOT_NULL(prev_shard_conn) && OB_FAIL(logic_db_info.get_single_real_table_info(table_name, *prev_shard_conn,
                                                                     real_database_name, OB_MAX_DATABASE_NAME_LENGTH,
                                                                     real_table_name, OB_MAX_TABLE_NAME_LENGTH,
                                                                     hint_table))) {
        LOG_WARN("fail to get real table info", K(table_name), K(ret));
      } else if (OB_ISNULL(prev_shard_conn)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("shard previous connection info is null to use", K(is_need_skip_router), K(ret));
      }
    }

    if (!is_skip_rewrite_check) {
      LOG_DEBUG("check_and_rewrite_testload_request", K(testload_type), K(ret));
      if (OB_SUCC(ret) && TESTLOAD_ALIPAY_COMPATIBLE == testload_type && hint_table.empty()) { /* testload = 8 */
        ret = OB_ERR_TESTLOAD_ALIPAY_COMPATIBLE;
        LOG_WARN("not have table_name's hint for 'testload=8' (TESTLOAD_ALIPAY_COMPATIBLE)", K(ret));
      } else if (OB_SUCC(ret) && TESTLOAD_NON != testload_type) { //rewrite table name for testload
        ObProxySqlParser sql_parser;
        ObSqlParseResult &sql_parse_result = client_request.get_parse_result();
        ObString sql = client_request.get_parse_sql();
        if (OB_FAIL(sql_parser.parse_sql_by_obparser(sql, NORMAL_PARSE_MODE, sql_parse_result, false))) {
          LOG_WARN("parse_sql_by_obparser failed", K(ret), K(sql));
        } else if (OB_FAIL(testload_check_and_rewrite_testload_request(session_info, client_request, client_buffer_reader,
                                                       false, hint_table, ObString::make_string(real_database_name), logic_db_info))) {
          LOG_WARN("fail to check and rewrite testload request");
        } else {
          is_need_rewrite_sql = false;
          LOG_DEBUG("check and rewrite testload data");
        }
      }

      if (OB_SUCC(ret) && is_need_rewrite_sql) {
        if (OB_FAIL(rewrite_shard_request(session_info, client_request, client_buffer_reader,
                                          table_name, logic_db_info.db_name_.config_string_, ObString::make_string(real_table_name),
                                          ObString::make_string(real_database_name), true))) {
          LOG_WARN("fail to rewrite shard request", K(ret), K(table_name),
                   K(logic_db_info.db_name_), K(real_table_name), K(real_database_name));
        }
      }
    } else {
      LOG_DEBUG("not need to rewrite sql by rules", K(real_database_name), K(real_table_name));
    }
  }

  return ret;
}

int ObProxyShardUtils::handle_single_shard_request(ObMysqlSM *sm,
                                                   ObMysqlClientSession &client_session,
                                                   ObMysqlTransact::ObTransState &trans_state,
                                                   ObIOBufferReader &client_buffer_reader,
                                                   ObDbConfigLogicDb &logic_db_info,
                                                   bool &need_wait_callback)
{
  int ret = OB_SUCCESS;

  ObProxyMysqlRequest &client_request = trans_state.trans_info_.client_request_;
  ObSqlParseResult &parse_result = client_request.get_parse_result();
  const ObString runtime_env = get_global_proxy_config().runtime_env.str();
  if (parse_result.is_ddl_stmt() && 0 == runtime_env.case_compare(OB_PROXY_DBP_RUNTIME_ENV)) {
    if (OB_FAIL(handle_ddl_request(sm, client_session, trans_state, logic_db_info, need_wait_callback))) {
      LOG_WARN("fail to handle ddl request", K(ret));
    }
  } else if (OB_FAIL(do_handle_single_shard_request(client_session, trans_state,
                                                    client_buffer_reader, logic_db_info))) {
    LOG_WARN("fail to handle single shard request", K(ret));
  }

  return ret;
}

int ObProxyShardUtils::handle_shard_request(ObMysqlSM *sm,
                       ObMysqlClientSession &client_session,
                       ObMysqlTransact::ObTransState &trans_state,
                       ObIOBufferReader &client_buffer_reader,
                       ObDbConfigLogicDb &db_info,
                       bool &need_wait_callback)
{
  int ret = OB_SUCCESS;
  ObClientSessionInfo &session_info = client_session.get_session_info();
  session_info.set_enable_reset_db(false);
  ObProxyMysqlRequest &client_request = trans_state.trans_info_.client_request_;
  ObSqlParseResult &parse_result = client_request.get_parse_result();
  ObString table_name = client_request.get_parse_result().get_origin_table_name();
  if (parse_result.is_ddl_stmt()) {
    const ObString runtime_env = get_global_proxy_config().runtime_env.str();
    if (0 == runtime_env.case_compare(OB_PROXY_DBP_RUNTIME_ENV)) {
      if (OB_FAIL(handle_ddl_request(sm, client_session, trans_state, db_info, need_wait_callback))) {
        LOG_WARN("fail to handle ddl request", K(ret));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("ddl stmt is unsupported for sharding table", K(ret));
    }
  } else if (parse_result.is_show_tables_stmt() || (table_name.empty() && !parse_result.is_dml_stmt())) {
    //do nothing
  } else if (table_name.empty()) {
    // keep compatible, skip
  } else if (parse_result.is_multi_stmt()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("multi stmt is unsupported for sharding table", "stmt type", parse_result.get_stmt_type(), K(ret));
  } else if (parse_result.is_show_create_table_stmt() || parse_result.is_desc_table_stmt()) {
    if (OB_FAIL(handle_other_request(client_session, trans_state,
                                     client_buffer_reader, table_name, db_info))) {
      LOG_WARN("fail to handle other request", K(ret), K(session_info), K(table_name));
    }
  } else if (parse_result.is_select_stmt()) {
    if (OB_FAIL(handle_select_request(client_session, trans_state,
                                      client_buffer_reader, table_name, db_info))) {
      LOG_WARN("fail to handle select request", K(ret), K(session_info), K(table_name));
    }
  } else if (parse_result.is_dml_stmt()) {
    if (OB_FAIL(handle_dml_request(client_session, trans_state,
                                   client_buffer_reader, table_name, db_info))) {
      LOG_WARN("fail to handle dml request", K(table_name), K(ret));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("stmt is unsupported for sharding table", "stmt type", parse_result.get_stmt_type(), K(ret));
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
  ObString logic_tenant_name;
  ObEThread *cb_thread = &self_ethread();
  if (OB_FAIL(session_info.get_logic_tenant_name(logic_tenant_name))) {
    ret = OB_ERR_UNEXPECTED; // no need response, just return ret and disconnect
    LOG_ERROR("fail to get logic tenant name", K(ret));
  } else if (OB_ISNULL(cont = new(std::nothrow) ObShardDDLCont(sm, cb_thread))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObShardDDLCont", K(ret));
  } else if (OB_FAIL(cont->init(logic_tenant_name, db_info.db_name_.config_string_, sql,
                                parse_result.get_stmt_type(), parse_result.get_cmd_sub_type()))) {
    LOG_WARN("fail to init ObShardDDLCont", K(ret));
  } else if (OB_ISNULL(g_event_processor.schedule_imm(cont, ET_TASK))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to schedule ob shard ddl task", K(ret));
  } else if (OB_FAIL(sm->setup_handle_shard_ddl(&cont->get_action()))) {
    LOG_WARN("fail to setup handle shard ddl", K(ret));
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
                                            ObIOBufferReader &client_buffer_reader,
                                            const ObString &table_name,
                                            ObDbConfigLogicDb &db_info)
{
  int ret = OB_SUCCESS;

  ObClientSessionInfo &session_info = client_session.get_session_info();
  ObProxyMysqlRequest &client_request = trans_state.trans_info_.client_request_;
  char real_table_name[OB_MAX_TABLE_NAME_LENGTH];
  char real_database_name[OB_MAX_DATABASE_NAME_LENGTH];

  if (OB_FAIL(handle_other_real_info(db_info, client_session, trans_state, table_name,
                                     real_database_name, OB_MAX_DATABASE_NAME_LENGTH,
                                     real_table_name, OB_MAX_TABLE_NAME_LENGTH))) {
    LOG_WARN("fail to handle other real info", K(ret), K(session_info), K(table_name));
  } else if (OB_FAIL(rewrite_shard_request(session_info, client_request, client_buffer_reader,
                                           table_name, db_info.db_name_.config_string_,
                                           ObString::make_string(real_table_name),
                                           ObString::make_string(real_database_name), false))) {
    LOG_WARN("fail to rewrite shard request", K(ret), K(table_name),
             K(db_info.db_name_), K(real_table_name), K(real_database_name));
  }

  return ret;
}

int ObProxyShardUtils::handle_select_request(ObMysqlClientSession &client_session,
                                             ObMysqlTransact::ObTransState &trans_state,
                                             ObIOBufferReader &client_buffer_reader,
                                             const ObString &table_name,
                                             ObDbConfigLogicDb &db_info)
{
  int ret = OB_SUCCESS;
  ObProxyMysqlRequest &client_request = trans_state.trans_info_.client_request_;
  ObSqlParseResult &parse_result = client_request.get_parse_result();
  SqlFieldResult& sql_result = parse_result.get_sql_filed_result();

  bool is_scan_all = false;
  ObProxySqlParser sql_parser;
  ObString sql = client_request.get_parse_sql();
  if (OB_FAIL(sql_parser.parse_sql_by_obparser(sql, NORMAL_PARSE_MODE, parse_result, true))) {
    LOG_WARN("parse_sql_by_obparser failed", K(ret), K(sql));
  } else if (FALSE_IT(is_scan_all = need_scan_all(parse_result))) {
    // impossible
  } else if (is_scan_all) {
    if (OB_FAIL(need_scan_all_by_index(table_name, db_info, sql_result, is_scan_all))) {
      LOG_WARN("fail to exec scan all by index", K(table_name), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (is_scan_all) {
      if (OB_FAIL(handle_scan_all_real_info(db_info, client_session, trans_state, table_name))) {
        LOG_WARN("fail to handle scan all real info", K(table_name), K(ret));
      }
    } else {
      if (OB_FAIL(handle_select_real_info(db_info, client_session, trans_state,
                                          table_name, client_buffer_reader))) {
        LOG_WARN("fail to handle dml real info", K(table_name), K(ret));
      }
    }
  }

  return ret;
}

int ObProxyShardUtils::handle_dml_request(ObMysqlClientSession &client_session,
                                          ObMysqlTransact::ObTransState &trans_state,
                                          ObIOBufferReader &client_buffer_reader,
                                          const ObString &table_name,
                                          ObDbConfigLogicDb &db_info)
{
  int ret = OB_SUCCESS;
  ObClientSessionInfo &session_info = client_session.get_session_info();
  ObProxyMysqlRequest &client_request = trans_state.trans_info_.client_request_;
  ObSqlParseResult &parse_result = client_request.get_parse_result();
  char real_table_name[OB_MAX_TABLE_NAME_LENGTH];
  char real_database_name[OB_MAX_DATABASE_NAME_LENGTH];

  ObCollationType connection_collation = static_cast<ObCollationType>(client_session.get_session_info().get_collation_connection());
  if (OB_FAIL(ObMysqlRequestAnalyzer::parse_sql_fileds(client_request, connection_collation))) {
    LOG_WARN("fail to extract_fileds", K(connection_collation), K(ret));
  } else if ((parse_result.is_insert_stmt() || parse_result.is_replace_stmt()
             || parse_result.is_update_stmt()) && parse_result.get_batch_insert_values_count() > 1) {
    ret = OB_ERR_BATCH_INSERT_FOUND;
    LOG_WARN("batch insert not supported in sharding sql", K(ret), K(parse_result.get_batch_insert_values_count()));
  } else if (OB_FAIL(handle_dml_real_info(db_info, client_session, trans_state, table_name,
                                          real_database_name, OB_MAX_DATABASE_NAME_LENGTH,
                                          real_table_name, OB_MAX_TABLE_NAME_LENGTH))) {
    LOG_WARN("fail to handle dml real info", K(ret), K(session_info), K(table_name));
  } else if (OB_FAIL(rewrite_shard_request(session_info, client_request, client_buffer_reader,
                                           table_name, db_info.db_name_.config_string_, ObString::make_string(real_table_name),
                                           ObString::make_string(real_database_name), false))) {
    LOG_WARN("fail to rewrite shard request", K(ret), K(table_name),
             K(db_info.db_name_), K(real_table_name), K(real_database_name));
  }

  return ret;
}

int ObProxyShardUtils::check_topology(ObSqlParseResult &parse_result,
                                      ObDbConfigLogicDb &db_info)
{
  int ret = OB_SUCCESS;

  ObProxySelectStmt *select_stmt = static_cast<ObProxySelectStmt*>(parse_result.get_proxy_stmt());
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select stmt is null, unexpected", K(ret));
  } else {
    int64_t last_db_size = -1;
    int64_t last_tb_size = -1;
    ObProxySelectStmt::ExprMap &table_exprs_map = select_stmt->get_table_exprs_map();
    ObProxySelectStmt::ExprMap::iterator iter = table_exprs_map.begin();
    ObProxySelectStmt::ExprMap::iterator end = table_exprs_map.end();

    for (; OB_SUCC(ret) && iter != end; iter++) {
      ObProxyExpr *expr = iter->second;
      ObProxyExprTable *table_expr = NULL;
      ObShardRule *logic_tb_info = NULL;
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null, unexpected", K(ret));
      } else if (OB_ISNULL(table_expr = dynamic_cast<ObProxyExprTable*>(expr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to cast to table expr", K(expr), K(ret));
      } else {
        ObString &table_name = table_expr->get_table_name();
        if (OB_FAIL(db_info.get_shard_rule(logic_tb_info, table_name))) {
          LOG_WARN("fail to get shard rule", K(table_name), K(ret));
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
          || (!parse_result.has_shard_comment() && get_global_proxy_config().auto_scan_all))) {
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
    LOG_WARN("fail to get shard rule", K(table_name), K(ret));
    // It is possible to have no where condition, but only one database and one table
  } else if (logic_tb_info->tb_size_ == 1 && logic_tb_info->db_size_ == 1) {
    is_scan_all = false;
  } else if (sql_result.field_num_ > 0) {
    if (logic_tb_info->tb_size_ == 1) {
      // Sub-library without table and sub-library single table, calculated according to db rule
      if (OB_FAIL(ObShardRule::get_physic_index_array(sql_result, logic_tb_info->db_rules_,
                                                      logic_tb_info->db_size_,
                                                      TESTLOAD_NON, index_array))) {
        LOG_WARN("fail to get physic tb index", K(table_name), KPC(logic_tb_info), K(ret));
      }
    } else {
      // Sub-library and sub-table, calculated according to tb rule
      if (OB_FAIL(ObShardRule::get_physic_index_array(sql_result, logic_tb_info->tb_rules_,
                                                      logic_tb_info->tb_size_,
                                                      TESTLOAD_NON, index_array))) {
        LOG_WARN("fail to get physic tb index", K(table_name), KPC(logic_tb_info), K(ret));
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
      LOG_WARN("get_sys_variable_value fail", K(ret));
    } else {
      int64_t sys_val = obj.get_int();
      if (sys_val != level) {
        obj.set_int(level);
        if (OB_FAIL(session_info.update_sys_variable(ObString::make_string(OB_SV_READ_CONSISTENCY), obj))) {
          LOG_WARN("fail to update read_consistency", K(obj), K(ret));
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
  //TODO: These configurations support adjustment
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
      LOG_WARN("init int_sys failed", K(name), K(obj));
    }
  }
  for(std::map<std::string, uint64_t>::iterator it = uint_session_map.begin();
      OB_SUCC(ret) && it!= uint_session_map.end(); ++it) {
    obj.reset();
    name = ObString::make_string(it->first.c_str());
    obj.set_uint(ObUInt64Type, it->second);
    if (OB_FAIL(session_info.update_common_sys_variable(name, obj, true, false))) {
      LOG_WARN("init unt_sys failed", K(name), K(obj));
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
        LOG_WARN("init str_sys failed", K(name), K(obj));
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
      LOG_WARN("init_shard_common_session failed", K(ret));
    } else if (OB_FAIL(session_info.set_origin_username(username))) {
      LOG_WARN("fail to set origin username", K(username), K(ret));
    } else if (session_info.enable_shard_authority()) {
      if (database.empty()) {
        if (OB_FAIL(tenant_info->acquire_random_logic_db_with_auth(username, host, up_info, db_info))) {
          ret = OB_USER_NOT_EXIST;
          LOG_WARN("fail to acquire random shard connector", K(username), K(host), K(ret));
        }
      } else if (NULL != (db_info = tenant_info->get_exist_db(database))) {
        if (OB_FAIL(db_info->get_user_priv_info(username, host, up_info))) {
          LOG_WARN("fail to get user priv info", K(username), K(host), K(database), K(ret));
          ret = OB_USER_NOT_EXIST;
        }
      }
      if (OB_SUCC(ret) && NULL != db_info) {
        if (OB_FAIL(check_login(password, client_session.get_scramble_string(),
                                up_info.password_stage2_.config_string_))) {
          LOG_WARN("fail to check login", K(ret));
        }
      }
    } else {
      if (database.empty()) {
        if (OB_FAIL(tenant_info->acquire_random_logic_db(db_info))) {
          LOG_WARN("fail to get random logic db", K(ret), KPC(tenant_info));
        }
      } else if (NULL != (db_info = tenant_info->get_exist_db(database))) {
        if (!client_session.is_local_connection() && !db_info->enable_remote_connection()) {
          ret = OB_ERR_NO_DB_PRIVILEGE;
          LOG_WARN("Access denied for database from remote addr", K(database), K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(db_info)) {
        ret = OB_ERR_BAD_DATABASE;
        LOG_WARN("database does not exist", K(database), K(ret));
      } else if (!database.empty()) {
        session_info.set_logic_database_name(database);
        LOG_DEBUG("succ to get logic db", KPC(db_info));
      } else {
        LOG_DEBUG("succ to get random logic db", KPC(db_info));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(db_info->acquire_random_shard_connector(shard_conn))) {
        LOG_WARN("fail to get random shard connector", K(ret), KPC(db_info));
      } else {
        if (DB_OB_MYSQL == shard_conn->server_type_ || DB_OB_ORACLE == shard_conn->server_type_) {
          if (OB_FAIL(change_user_auth(client_session, *shard_conn, true, true))) {
            LOG_WARN("fail to change user auth", K(ret), KPC(shard_conn));
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
            //need save database name in connection pool mode
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
    LOG_WARN("fail to check logic database", K(db_name), K(ret));
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
    LOG_WARN("logic db is not exist", K(logic_tenant_name), K(logic_database_name));
  } else {
    db_version = logic_db_info->get_version();
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
      LOG_WARN("fail to get all shard tables", K(logic_tenant_name), K(logic_database_name), K(ret));
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
    LOG_WARN("fail to alloc table name buf", K(real_table_name), K(ret));
  } else {
    memcpy(buf, real_table_name.ptr(), real_table_name.length());
    ObString real_table_name_buf(real_table_name.length(), buf);
    if (OB_FAIL(table_name_map.set_refactored(table_name, real_table_name_buf))) {
      LOG_WARN("fail to set table name buf", K(table_name), K(ret));
    }
  }

  return ret;
}

int ObProxyShardUtils::handle_other_real_info(ObDbConfigLogicDb &logic_db_info,
                                              ObMysqlClientSession &client_session,
                                              ObMysqlTransact::ObTransState &trans_state,
                                              const ObString &table_name,
                                              char *real_database_name, int64_t db_name_len,
                                              char *real_table_name, int64_t tb_name_len)
{
  int ret = OB_SUCCESS;
  ObShardConnector *shard_conn = NULL;
  ObClientSessionInfo &cs_info = client_session.get_session_info();
  ObShardConnector *prev_shard_conn = cs_info.get_shard_connector();
  ObSqlParseResult &parse_result = trans_state.trans_info_.client_request_.get_parse_result();

  ObShardRule *logic_tb_info = NULL;

  int64_t group_id = OBPROXY_MAX_DBMESH_ID;
  int64_t table_id = OBPROXY_MAX_DBMESH_ID;
  int64_t es_id = OBPROXY_MAX_DBMESH_ID;
  ObTestLoadType testload_type = TESTLOAD_NON;
  ObString hint_table;

  if (OB_ISNULL(prev_shard_conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shard connector info is null", K(ret));
  } else if (OB_FAIL(get_shard_hint(logic_db_info, parse_result,
                                    group_id, table_id, es_id, hint_table, testload_type))) {
    LOG_WARN("fail to get shard hint", K(ret));
  // get group_id
  } else if (OB_FAIL(logic_db_info.get_shard_rule(logic_tb_info, table_name))) {
    LOG_WARN("fail to get shard rule", K(table_name), K(ret));
  } else {
    int64_t last_group_id = cs_info.get_group_id();

    ObString saved_database_name;
    cs_info.get_logic_database_name(saved_database_name);
    bool is_not_saved_database = saved_database_name != logic_db_info.db_name_.config_string_;

    // need to re-select a shard_conn condition, and any one of the conditions can be satisfied
    // 1. SQL comes with database, and it is different from the current logic library
    // 2. The group id is specified in the hint, and it is different from the last time
    // 3. No group_id is specified, nor did the last one (for example, switch the library)
    // 4. If the last group_id is greater than the db_size of the table of this SQL
    // (for example, the table of the previous SQL is group_{00-99}, the table of this SQL is group_00)
    if (is_not_saved_database
        || (group_id != OBPROXY_MAX_DBMESH_ID && group_id != last_group_id)
        || last_group_id == OBPROXY_MAX_DBMESH_ID
        || last_group_id >= logic_tb_info->db_size_) {
      ObShardTpo *shard_tpo = NULL;
      ObGroupCluster *gc_info = NULL;
      if (OB_FAIL(logic_db_info.get_shard_tpo(shard_tpo))) {
        LOG_WARN("fail to get shard tpo info", K(ret));
      } else if (OB_ISNULL(shard_tpo)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("shard tpo info is null", K(ret));
      } else if (OB_FAIL(ObShardRule::get_physic_index_random(logic_tb_info->db_size_, group_id))) {
        LOG_WARN("fail to get physic db index", "db_size", logic_tb_info->db_size_, K(group_id), K(ret));
      // reuse real_database_name to store group name instead of using a new buffer
      // real_database_name will be set after shard connector is chosen
      } else  if (OB_FAIL(logic_tb_info->get_real_name_by_index(logic_tb_info->db_size_, logic_tb_info->db_suffix_len_, group_id,
                                                                logic_tb_info->db_prefix_.config_string_,
                                                                logic_tb_info->db_tail_.config_string_,
                                                                real_database_name, db_name_len))) {
        LOG_WARN("fail to get real group name", K(group_id), KPC(logic_tb_info), K(ret));
      } else if (OB_FAIL(shard_tpo->get_group_cluster(ObString::make_string(real_database_name), gc_info))) {
        LOG_DEBUG("group does not exist", "phy_db_name", real_database_name, K(ret));
      } else if (OB_ISNULL(gc_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("group cluster info is null", "phy_db_name", real_database_name, K(ret));
      } else {
        int64_t es_size = gc_info->get_es_size();
        bool is_read_stmt = false;
        ObString shard_name;
        if (-1 == es_id || OBPROXY_MAX_DBMESH_ID == es_id) {
          if (OB_FAIL(gc_info->get_elastic_id_by_weight(es_id, is_read_stmt))) {
            LOG_WARN("fail to get eid by read weight", KPC(gc_info));
          } else {
            LOG_DEBUG("succ to get eid by weight", K(es_id));
          }
        } else if (OB_UNLIKELY(es_id >= es_size)) {
          ret = OB_EXPR_CALC_ERROR;
          LOG_WARN("es index is larger than elastic array", K(es_id), K(es_size), K(ret));
        }

        if (OB_SUCC(ret)) {
          shard_name = gc_info->get_shard_name_by_eid(es_id);
          if (TESTLOAD_NON != testload_type) {
            if (OB_FAIL(logic_db_info.get_testload_shard_connector(shard_name, logic_db_info.testload_prefix_.config_string_, shard_conn))) {
              LOG_WARN("testload shard connector not exist", "testload_type", get_testload_type_str(testload_type),
                       K(shard_name), "testload_prefix", logic_db_info.testload_prefix_, K(ret));
            }
          } else if (OB_FAIL(logic_db_info.get_shard_connector(shard_name, shard_conn))) {
            LOG_WARN("shard connector does not exist", K(shard_name), K(ret));
          } else if (*prev_shard_conn != *shard_conn) {
            if (OB_FAIL(change_connector(logic_db_info, client_session, trans_state, prev_shard_conn, shard_conn))) {
              LOG_WARN("fail to change connector", KPC(prev_shard_conn), KPC(shard_conn), K(ret));
            }
          }
        }
      }

      //If it is the logic library on the current session, you can reuse this group_id next time
      if (OB_SUCC(ret) && !is_not_saved_database) {
        cs_info.set_group_id(group_id);
      }
    } else {
      prev_shard_conn->inc_ref();
      shard_conn = prev_shard_conn;
      group_id = last_group_id;
    }

    if (OB_SUCC(ret)) {
      snprintf(real_database_name, db_name_len, "%.*s", shard_conn->database_name_.length(), shard_conn->database_name_.ptr());

      if (!hint_table.empty()) {
        snprintf(real_table_name, tb_name_len, "%.*s", static_cast<int>(hint_table.length()), hint_table.ptr());
      } else {
        if (table_id == OBPROXY_MAX_DBMESH_ID) {
          if (1 == logic_tb_info->tb_size_) {
            table_id = group_id;
          } else {
            table_id = group_id * (logic_tb_info->tb_size_ / logic_tb_info->db_size_);
          }
        }
        if (OB_FAIL(logic_tb_info->get_real_name_by_index(logic_tb_info->tb_size_, logic_tb_info->tb_suffix_len_, table_id,
                                                        logic_tb_info->tb_prefix_.config_string_,
                                                        logic_tb_info->tb_tail_.config_string_, real_table_name, tb_name_len))) {
          LOG_WARN("fail to get real table name", KPC(logic_tb_info), K(ret));
        } else if (TESTLOAD_NON != testload_type) {
          snprintf(real_table_name + strlen(real_table_name), tb_name_len - strlen(real_table_name), "_T");
        }
      }
    }
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

  ObProxySelectStmt *select_stmt = static_cast<ObProxySelectStmt*>(parse_result.get_proxy_stmt());
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select stmt is null, unexpected", K(ret));
  } else if (select_stmt->has_unsupport_expr_type()) {
    ret = OB_ERROR_UNSUPPORT_EXPR_TYPE;
    LOG_WARN("unsupport sql", K(ret));
  } else if (OB_FAIL(check_topology(parse_result, logic_db_info))) {
    if (OB_ERR_UNSUPPORT_DIFF_TOPOLOGY != ret) {
      LOG_WARN("fail to check topology", K(ret));
    }
  } else if (OB_FAIL(get_global_optimizer_processor().alloc_allocator(allocator))) {
    LOG_WARN("alloc allocator failed", K(ret));
  } else if (OB_FAIL(handle_sharding_select_real_info(logic_db_info, client_session, trans_state,
                                                      table_name, *allocator,
                                                      shard_connector_array,
                                                      shard_prop_array,
                                                      table_name_map_array))) {
    LOG_WARN("fail to handle sharding select real info", K(ret), K(table_name));
  } else {
    LOG_DEBUG("proxy stmt", K(select_stmt->get_stmt_type()), K(select_stmt->limit_offset_), K(select_stmt->limit_size_));
    LOG_DEBUG("select expr");
    for (int64_t i = 0; i < select_stmt->select_exprs_.count(); i++) {
      ObProxyExpr::print_proxy_expr(select_stmt->select_exprs_.at(i));
    }
    LOG_DEBUG("group by expr");
    for (int64_t i = 0; i < select_stmt->group_by_exprs_.count(); i++) {
      ObProxyExpr::print_proxy_expr(select_stmt->group_by_exprs_.at(i));
    }
    for (int64_t i = 0; i < select_stmt->order_by_exprs_.count(); i++) {
      ObProxyOrderItem *order_expr = select_stmt->order_by_exprs_.at(i);
      LOG_DEBUG("order by expr", K(order_expr->order_direction_));
      ObProxyExpr::print_proxy_expr(order_expr);
    }
    // Handling distributed selects
    ObShardingSelectLogPlan* select_plan = NULL;
    void *ptr = NULL;
    if (OB_ISNULL(ptr = allocator->alloc(sizeof(ObShardingSelectLogPlan)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc select plan buf", "size", sizeof(ObShardingSelectLogPlan), K(ret));
    } else {
      select_plan = new(ptr) ObShardingSelectLogPlan(client_request, allocator);
      client_session.set_sharding_select_log_plan(select_plan);
      if (OB_FAIL(select_plan->generate_plan(shard_connector_array, shard_prop_array, table_name_map_array))) {
        LOG_WARN("fail to generate plan", K(ret));
      }
    }
  }
  return ret;
}

int ObProxyShardUtils::handle_select_real_info(ObDbConfigLogicDb &logic_db_info,
                                               ObMysqlClientSession &client_session,
                                               ObMysqlTransact::ObTransState &trans_state,
                                               const ObString &table_name,
                                               ObIOBufferReader &client_buffer_reader)
{
  int ret = OB_SUCCESS;

  ObShardConnector *shard_conn = NULL;
  ObClientSessionInfo &session_info = client_session.get_session_info();
  ObShardConnector *prev_shard_conn = session_info.get_shard_connector();

  ObProxyMysqlRequest &client_request = trans_state.trans_info_.client_request_;
  ObSqlParseResult &parse_result = client_request.get_parse_result();
  SqlFieldResult &sql_result = parse_result.get_sql_filed_result();
  ObProxySelectStmt *select_stmt = dynamic_cast<ObProxySelectStmt*>(parse_result.get_proxy_stmt());

  int64_t group_index = OBPROXY_MAX_DBMESH_ID;
  int64_t tb_index = OBPROXY_MAX_DBMESH_ID;
  int64_t es_index = OBPROXY_MAX_DBMESH_ID;
  ObTestLoadType testload_type = TESTLOAD_NON;
  ObString hint_table;
  ObArenaAllocator allocator;

  ObHashMap<ObString, ObString> table_name_map;
  char real_table_name[OB_MAX_TABLE_NAME_LENGTH];
  char real_database_name[OB_MAX_DATABASE_NAME_LENGTH];

  // The write request walks the write weight, and requests within the transaction are considered to be write
  // read request weight
  bool is_read_stmt = ObProxyShardUtils::is_read_stmt(session_info, trans_state, parse_result);

  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select stmt is null, unexpected", K(ret));
  } else if (OB_FAIL(table_name_map.create(OB_ALIAS_TABLE_MAP_MAX_BUCKET_NUM,
                                           ObModIds::OB_HASH_ALIAS_TABLE_MAP))) {
    LOG_WARN("fail to create table name map", K(ret));
  } else if (OB_FAIL(get_shard_hint(logic_db_info, parse_result,
                                    group_index, tb_index, es_index,
                                    hint_table, testload_type))) {
    LOG_WARN("fail to get shard hint", K(ret));
  }

  if (OB_SUCC(ret)) {
    ObProxySelectStmt::ExprMap &table_exprs_map = select_stmt->get_table_exprs_map();
    ObProxySelectStmt::ExprMap::iterator iter = table_exprs_map.begin();
    ObProxySelectStmt::ExprMap::iterator end = table_exprs_map.end();

    if (!hint_table.empty() && table_exprs_map.size() > 1) {
      ret = OB_ERR_MORE_TABLES_WITH_TABLE_HINT;
      LOG_WARN("more table with table_name hint", "table size", table_exprs_map.size(), K(hint_table), K(ret));
    } else if (OB_FAIL(logic_db_info.get_shard_table_info(table_name, sql_result, shard_conn,
                                                          real_database_name, OB_MAX_DATABASE_NAME_LENGTH,
                                                          real_table_name, OB_MAX_TABLE_NAME_LENGTH,
                                                          group_index, tb_index, es_index,
                                                          hint_table, testload_type, is_read_stmt))) {
      LOG_WARN("fail to get real info", K(table_name), K(group_index), K(tb_index),
               K(es_index), K(hint_table), K(testload_type), K(is_read_stmt), K(ret));
    } else if (OB_ISNULL(shard_conn) || OB_ISNULL(prev_shard_conn)) {
      ret = OB_EXPR_CALC_ERROR;
      LOG_WARN("shard connector info or prev shard connector info is null", KP(shard_conn),
               KP(prev_shard_conn), K(ret));
    }

    for (; OB_SUCC(ret) && iter != end; iter++) {
      ObProxyExpr *expr = iter->second;
      ObProxyExprTable *table_expr = NULL;
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null, unexpected", K(ret));
      } else if (OB_ISNULL(table_expr = dynamic_cast<ObProxyExprTable*>(expr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to cast to table expr", K(expr), K(ret));
      } else {
        ObString &sql_table_name = table_expr->get_table_name();
        if (OB_FAIL(logic_db_info.get_real_table_name(sql_table_name, sql_result,
                                                      real_table_name, OB_MAX_TABLE_NAME_LENGTH,
                                                      tb_index, hint_table, testload_type))) {
          LOG_WARN("fail to get real table name", K(sql_table_name), K(tb_index),
                   K(hint_table), K(testload_type), K(ret));
        } else if (OB_FAIL(add_table_name_to_map(allocator, table_name_map, sql_table_name, real_table_name))) {
          LOG_WARN("fail to add table name to map", K(sql_table_name), K(real_table_name), K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(rewrite_shard_select_request(session_info, client_request, client_buffer_reader,
                                             table_name_map, ObString::make_string(real_database_name), false))) {
      LOG_WARN("fail to rewrite shard request", K(real_database_name), K(ret));
    }
  }

  if (OB_SUCC(ret) && *prev_shard_conn != *shard_conn) {
    if (OB_FAIL(change_connector(logic_db_info, client_session, trans_state, prev_shard_conn, shard_conn))) {
      LOG_WARN("fail to change connector", KPC(prev_shard_conn), KPC(shard_conn), K(ret));
    }
  }

  //It is possible that there is a database in SQL. Only the logical library saved on the session can save the group_id and reuse it next time.
  if (OB_SUCC(ret)) {
    ObString saved_database_name;
    session_info.get_logic_database_name(saved_database_name);
    bool is_not_saved_database = saved_database_name != logic_db_info.db_name_.config_string_;
    if (!is_not_saved_database) {
      session_info.set_group_id(group_index);
    }
  }

  if (NULL != shard_conn) {
    shard_conn->dec_ref();
    shard_conn = NULL;
  }
  return ret;
}

int ObProxyShardUtils::handle_dml_real_info(ObDbConfigLogicDb &logic_db_info,
                                            ObMysqlClientSession &client_session,
                                            ObMysqlTransact::ObTransState &trans_state,
                                            const ObString &table_name,
                                            char *real_database_name, int64_t db_name_len,
                                            char *real_table_name, int64_t tb_name_len)
{
  int ret = OB_SUCCESS;

  ObShardConnector *shard_conn = NULL;
  ObClientSessionInfo &session_info = client_session.get_session_info();
  ObShardConnector *prev_shard_conn = session_info.get_shard_connector();
  ObSqlParseResult &parse_result = trans_state.trans_info_.client_request_.get_parse_result();
  int64_t group_id = OBPROXY_MAX_DBMESH_ID;

  bool is_read_stmt = ObProxyShardUtils::is_read_stmt(session_info, trans_state, parse_result);

  if (OB_FAIL(get_real_info(logic_db_info, table_name, parse_result, shard_conn,
                            real_database_name, db_name_len,
                            real_table_name, tb_name_len,
                            &group_id, NULL, NULL, is_read_stmt))) {
    LOG_WARN("fail to get real db and tb", K(table_name), K(ret));
  } else if (OB_ISNULL(shard_conn) || OB_ISNULL(prev_shard_conn)) {
    ret = OB_EXPR_CALC_ERROR;
    LOG_WARN("shard connector info or prev shard connector info is null", KP(shard_conn),
             KP(prev_shard_conn), K(ret));
  }

  if (OB_SUCC(ret) && *prev_shard_conn != *shard_conn) {
    if (OB_FAIL(change_connector(logic_db_info, client_session, trans_state, prev_shard_conn, shard_conn))) {
      LOG_WARN("fail to change connector", KPC(prev_shard_conn), KPC(shard_conn), K(ret));
    }
  }

  //There may be a database in SQL, only the logical library saved on the session can save the group_id, and it can be reused next time
  if (OB_SUCC(ret)) {
    ObString saved_database_name;
    session_info.get_logic_database_name(saved_database_name);
    bool is_not_saved_database = saved_database_name != logic_db_info.db_name_.config_string_;
    if (!is_not_saved_database) {
      session_info.set_group_id(group_id);
    }
  }

  if (NULL != shard_conn) {
    shard_conn->dec_ref();
    shard_conn = NULL;
  }
  return ret;
}

int ObProxyShardUtils::get_real_info(ObDbConfigLogicDb &logic_db_info,
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
  if (OB_FAIL(get_shard_hint(logic_db_info, parse_result,
              group_index, tb_index, es_index, hint_table, testload_type))) {
    LOG_WARN("fail to get shard hint", K(ret));
  } else if (OB_FAIL(logic_db_info.get_real_info(table_name, parse_result, shard_conn,
                                          real_database_name, db_name_len,
                                          real_table_name, tb_name_len,
                                          group_index, tb_index, es_index,
                                          hint_table, testload_type, is_read_stmt))) {
    LOG_WARN("fail to get real info", K(table_name), K(group_index), K(tb_index),
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
         || ObMysqlTransact::is_in_trans(trans_state);
}

int ObProxyShardUtils::check_login(const ObString &login_reply, const ObString &scramble_str, const ObString &stored_stage2)
{
  int ret = OB_SUCCESS;
  bool pass = false;
  if (OB_FAIL(ObEncryptedHelper::check_login(login_reply, scramble_str, stored_stage2, pass))) {
    LOG_WARN("fail to check login", K(login_reply), K(stored_stage2), K(ret));
  } else if (!pass) {
    LOG_WARN("password is wrong", K(login_reply), K(stored_stage2), K(ret));
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
  } else if (parse_result.is_create_stmt()) {
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

int ObProxyShardUtils::build_error_packet(int err_code, bool &need_response_for_dml,
                       ObMysqlTransact::ObTransState &trans_state)
{
  int ret = OB_SUCCESS;
  switch (err_code) {
    case OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR:
    case OB_NOT_SUPPORTED:
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
    if (OB_FAIL(ObMysqlTransact::build_error_packet(trans_state))) {
      LOG_WARN("fail to build err resp", K(ret));
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

  if (OB_FAIL(get_shard_hint(logic_db_info, parse_result,
                             group_index, tb_index, es_index,
                             hint_table, testload_type))) {
    LOG_WARN("fail to get shard hint", K(ret));
  } else if (OB_FAIL(logic_db_info.get_sharding_select_info(table_name, parse_result,
                                                            testload_type, is_read_stmt, es_index,
                                                            allocator,
                                                            shard_connector_array,
                                                            shard_prop_array,
                                                            table_name_map_array))) {
    LOG_WARN("fail to get sharding select info", K(table_name),
             K(testload_type), K(is_read_stmt), K(ret));
  }

  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
