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

#ifndef OBPROXY_SHARD_UTILS_H
#define OBPROXY_SHARD_UTILS_H

#include "proxy/mysqllib/ob_proxy_session_info_handler.h"
#include "proxy/mysqllib/ob_proxy_auth_parser.h"
#include "proxy/mysql/ob_mysql_sm.h"
#include "dbconfig/ob_proxy_db_config_info.h"
#include "obutils/ob_proxy_sql_parser.h"
#include "dbconfig/ob_proxy_db_config_info.h"

using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace dbconfig
{
class ObDbConfigLogicDb;
class ObShardConnector;
}
namespace proxy
{

class ObProxyShardUtils
{
public:
  static int handle_shard_use_db(ObMysqlTransact::ObTransState &trans_state,
                                 ObMysqlClientSession &client_session, const common::ObString &db_name);
  static int handle_shard_auth(ObMysqlClientSession &client_session, const ObHSRResult &hsr);
  static int init_shard_common_session(ObClientSessionInfo &session_info);
  static int get_all_database(const ObString &logic_tenant_name, ObArray<ObString> &db_names);
  static int get_all_schema_table(const ObString &logic_tenant_name, const ObString &logic_database_name, ObArray<ObString> &table_names);
  static int get_db_version(const ObString &logic_tenant_name,
                            const ObString &logic_database_name,
                            ObString &db_version);
  static bool is_special_db(ObMysqlTransact::ObTransState &s);
  static int get_logic_db_info(ObMysqlTransact::ObTransState &s,
                               ObClientSessionInfo &session_info,
                               dbconfig::ObDbConfigLogicDb *&logic_db_info);

  static int get_real_info(dbconfig::ObDbConfigLogicDb &logic_db_info,
                           const common::ObString &table_name,
                           obutils::ObSqlParseResult &parse_result,
                           dbconfig::ObShardConnector *&shard_conn,
                           char *real_database_name, int64_t db_name_len,
                           char *real_table_name, int64_t tb_name_len,
                           int64_t* group_id, int64_t* table_id, int64_t* es_id, bool is_read_stmt = false);

  static int update_sys_read_consistency_if_need(ObClientSessionInfo &session_info);
  static int check_login(const ObString &login_reply, const ObString &scramble_str, const ObString &stored_stage2);
  static bool check_shard_authority(const dbconfig::ObShardUserPrivInfo &up_info,
                                    const obutils::ObSqlParseResult &parse_result);

  static int get_shard_hint(dbconfig::ObDbConfigLogicDb &logic_db_info,
                            obutils::ObSqlParseResult &parse_result,
                            int64_t &group_index, int64_t &tb_index,
                            int64_t &es_index, ObString &table_name,
                            dbconfig::ObTestLoadType &testload_type);

  static int check_shard_request(ObMysqlClientSession &client_session,
                                 obutils::ObSqlParseResult &parse_result,
                                 dbconfig::ObDbConfigLogicDb &logic_db_info);
  static int handle_shard_request(ObMysqlClientSession &client_session,
             ObMysqlTransact::ObTransState &trans_state,
             ObIOBufferReader &client_buffer_reader,
             dbconfig::ObDbConfigLogicDb &db_info);
  static int handle_single_shard_request(ObMysqlClientSession &client_session,
             ObMysqlTransact::ObTransState &trans_state,
             ObIOBufferReader &client_buffer_reader,
             dbconfig::ObDbConfigLogicDb &db_info);
  static int build_error_packet(int err_code, bool &need_response_for_dml,
                                ObMysqlTransact::ObTransState &trans_state);

  static int handle_sharding_select_real_info(dbconfig::ObDbConfigLogicDb &logic_db_info,
                                  ObMysqlClientSession &client_session,
                                  ObMysqlTransact::ObTransState &trans_state,
                                  const ObString table_name,
                                  common::ObIAllocator &allocator,
                                  common::ObIArray<dbconfig::ObShardConnector*> &shard_connector_array,
                                  common::ObIArray<ObString> &physcial_table_name_array);

private:
  static bool is_read_stmt(ObClientSessionInfo &session_info,
                           ObMysqlTransact::ObTransState &trans_state,
                           obutils::ObSqlParseResult &parse_result);
  static int change_connector(dbconfig::ObDbConfigLogicDb &logic_db_info,
                              ObMysqlClientSession &client_session,
                              ObMysqlTransact::ObTransState &trans_state,
                              const dbconfig::ObShardConnector *prev_shard_conn,
                              dbconfig::ObShardConnector *shard_conn);

  static int handle_dml_request(ObMysqlClientSession &client_session,
                                ObMysqlTransact::ObTransState &trans_state,
                                ObIOBufferReader &client_buffer_reader,
                                const common::ObString &table_name,
                                dbconfig::ObDbConfigLogicDb &db_info);
  static int check_logic_database(ObMysqlTransact::ObTransState &trans_state,
                                  ObMysqlClientSession &client_session, const ObString &db_name);
  static void replace_oracle_table(ObSqlString &new_sql, const ObString &real_name,
                                   bool &hava_quoto, bool is_single_shard_db_table,
                                   bool is_database);
  static int rewrite_shard_request(ObClientSessionInfo &session_info,
                                   ObProxyMysqlRequest &client_request,
                                   ObIOBufferReader &client_buffer_reader,
                                   const ObString &table_name, const ObString &database_name,
                                   const ObString &real_table_name, const ObString &real_database_name,
                                   bool is_single_shard_db_table);
  static int testload_check_obparser_node_is_valid(const ParseNode *root, const ObItemType &type);
  static int testload_rewrite_name_base_on_parser_node(common::ObSqlString &new_sql,
                                   const char *new_name,
                                   const char *sql_ptr,
                                   int64_t sql_len,
                                   int &last_pos,
                                   ParseNode *node);
  static int testload_get_obparser_db_and_table_node(const ParseNode *root,
                                   ParseNode *&db_node,
                                   ParseNode *&table_node);
  static int testload_check_and_rewrite_testload_request(ObClientSessionInfo &session_info,
                                   ObProxyMysqlRequest &client_request,
                                   ObIOBufferReader &client_buffer_reader,
                                   bool is_single_shard_db_table,
                                   const ObString &hint_table,
                                   const ObString &real_database_name,
                                   dbconfig::ObDbConfigLogicDb &logic_db_info);
  static int testload_check_and_rewrite_testload_hint_index(common::ObSqlString &new_sql,
                                                   const char *sql_ptr,
                                                   int64_t sql_len,
                                                   int &last_pos,
                                                   ObSEArray<oceanbase::obproxy::obutils::ObParseNode*, 1> &hint_option_list,
                                                   const ObString &hint_table,
                                                   const ObString &real_database_name,
                                                   oceanbase::obproxy::dbconfig::ObDbConfigLogicDb &logic_db_info);

  static int testload_check_table_is_test_load_table(const char* db_name, const char* table_name, dbconfig::ObDbConfigLogicDb &logic_db_info);
  static int change_user_auth(ObMysqlClientSession &client_session, 
                              const dbconfig::ObShardConnector &physic_db_info,
                              const bool is_cluster_changed,
                              const bool is_tennat_changed);
  static int handle_other_real_info(dbconfig::ObDbConfigLogicDb &logic_db_info,
                                    ObMysqlClientSession &client_session,
                                    ObMysqlTransact::ObTransState &trans_state,
                                    const ObString &table_name,
                                    char *real_database_name, int64_t db_name_len,
                                    char *real_table_name, int64_t tb_name_len);
  static int handle_dml_real_info(dbconfig::ObDbConfigLogicDb &logic_db_info,
                                  ObMysqlClientSession &client_session,
                                  ObMysqlTransact::ObTransState &trans_state,
                                  const ObString &table_name,
                                  char *real_database_name, int64_t db_name_len,
                                  char *real_table_name, int64_t tb_name_len);
  static int handle_sys_read_consitency_prop(dbconfig::ObDbConfigLogicDb &logic_db_info,
                                             dbconfig::ObShardConnector& shard_conn,
                                             ObClientSessionInfo &session_info);
  static int is_sharding_in_trans(ObClientSessionInfo &session_info,
                                  ObMysqlTransact::ObTransState &trans_state);
};


} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_SHARD_UTILS_H */
