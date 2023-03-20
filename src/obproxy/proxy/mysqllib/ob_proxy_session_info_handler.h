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

#ifndef OBPROXY_SESSION_INFO_HANDLER_H
#define OBPROXY_SESSION_INFO_HANDLER_H
#include "rpc/obmysql/ob_mysql_packet.h"
#include "utils/ob_proxy_lib.h"
#include "proxy/mysqllib/ob_2_0_protocol_struct.h"
#include "lib/oblog/ob_simple_trace.h"

namespace oceanbase
{
namespace common
{
class ObString;
class ObAddr;
}
namespace obmysql
{
class OMPKOK;
}
namespace obproxy
{
namespace event
{
class ObIOBufferReader;
class ObMIOBuffer;
}

namespace proxy
{
class ObProxyMysqlRequest;
class ObMysqlAnalyzeResult;
class ObClientSessionInfo;
class ObServerSessionInfo;
class ObRespAnalyzeResult;
class ObMysqlAuthRequest;
struct ObHandshakeResponseParam;

// this is the enum of the vars we care about in save changed session info
enum ObProxySysVarType
{
  OBPROXY_VAR_GLOBAL_VARIABLES_VERSION = 0,
  OBPROXY_VAR_USER_PRIVILEGE,
  OBPROXY_VAR_SET_TRX_EXECUTED,
  OBPROXY_VAR_PARTITION_HIT,
  OBPROXY_VAR_LAST_INSERT_ID,
  OBPROXY_VAR_CAPABILITY_FLAG,
  OBPROXY_VAR_SAFE_READ_SNAPSHOT,
  OBPROXY_VAR_ROUTE_POLICY_FLAG,
  OBPROXY_VAR_ENABLE_TRANSMISSION_CHECKSUM_FLAG,
  OBPROXY_VAR_STATEMENT_TRACE_ID_FLAG,
  OBPROXY_VAR_READ_CONSISTENCY_FLAG,
  OBPROXY_VAR_OTHERS
};

class ObProxySessionInfoHandler
{
public:
  // simple ok packet should not contains changed info (db_name, session variables)
  static const int64_t OB_SIMPLE_OK_PKT_LEN = 12;
  static int save_changed_session_info(ObClientSessionInfo &client_info,
                                       ObServerSessionInfo &server_info,
                                       const bool is_auth_request,
                                       const bool need_handle_sysvar,
                                       obmysql::OMPKOK &ok_pkt,
                                       ObRespAnalyzeResult &resp_result,
                                       common::ObSimpleTrace<4096> &trace_log,
                                       const bool is_save_to_common_sys = false);

  // The @reader must has only one receive completed ok packet,
  // and has not forwarded out even part of this ok pacekt.
  // The ok packet in @reader may be in single buffer block or multi buffer blocks.
  // this func will consume the old ok packet and write new ok packet.
  static int rebuild_ok_packet(event::ObIOBufferReader &reader,
                               ObClientSessionInfo &client_info,
                               ObServerSessionInfo &server_info,
                               const bool is_auth_request,
                               const bool need_handle_sysvar,
                               ObRespAnalyzeResult &resp_result,
                               common::ObSimpleTrace<4096> &trace_log,
                               const bool is_save_to_common_sys);

  static int analyze_extra_ok_packet(event::ObIOBufferReader &reader,
                                     ObClientSessionInfo &client_info,
                                     ObServerSessionInfo &server_info,
                                     const bool need_handle_sysvar,
                                     ObRespAnalyzeResult &resp_result,
                                     common::ObSimpleTrace<4096> &trace_log);

  static int rewrite_query_req_by_sharding(ObClientSessionInfo &client_info,
                                           ObProxyMysqlRequest &client_request,
                                           event::ObIOBufferReader &buffer_reader);

  static int rewrite_login_req_by_sharding(ObClientSessionInfo &client_info,
                                           const common::ObString &username,
                                           const common::ObString &password,
                                           const common::ObString &database,
                                           const common::ObString &tenant_name,
                                           const common::ObString &cluster_name);

  static int rewrite_login_req(ObClientSessionInfo &client_info,
                               ObHandshakeResponseParam &param);

  static int rewrite_ldg_login_req(ObClientSessionInfo &client_info,
                                   const common::ObString &tenant_name,
                                   const common::ObString &cluster_name);

  static int rewrite_ssl_req(ObClientSessionInfo &client_info);

  static int rewrite_first_login_req(ObClientSessionInfo &client_info,
                                     const common::ObString &cluster_name,
                                     const int64_t cluster_id,
                                     const uint32_t server_sessid,
                                     const uint64_t proxy_sessid,
                                     const common::ObString &server_scramble,
                                     const common::ObString &proxy_scramble,
                                     const common::ObAddr &client_addr,
                                     const bool use_compress,
                                     const bool use_ob_protocol_v2,
                                     const bool use_ssl,
                                     const bool enable_client_ip_checkout);
  static int rewrite_saved_login_req(ObClientSessionInfo &client_info,
                                     const common::ObString &cluster_name,
                                     const int64_t cluster_id,
                                     const uint32_t server_sessid,
                                     const uint64_t proxy_sessid,
                                     const common::ObString &server_scramble,
                                     const common::ObString &proxy_scramble,
                                     const common::ObAddr &client_addr,
                                     const bool use_compress,
                                     const bool use_ob_protocol_v2,
                                     const bool use_ssl,
                                     const bool enable_client_ip_checkout);
  static int rewrite_common_login_req(ObClientSessionInfo &client_info,
                                      ObHandshakeResponseParam &param,
                                      const int64_t global_vars_version,
                                      const common::ObString &cluster_name,
                                      const int64_t cluster_id,
                                      const uint32_t server_sessid,
                                      const uint64_t proxy_sessid,
                                      const common::ObString &server_scramble,
                                      const common::ObString &proxy_scramble,
                                      const common::ObAddr &client_addr);
  
  static int rewrite_change_user_login_req(ObClientSessionInfo &client_info,
                                           const common::ObString& username,
                                           const common::ObString& auth_response);

  static void assign_database_version(ObClientSessionInfo &client_info,
                                      ObServerSessionInfo &server_info);
  static void assign_last_insert_id_version(ObClientSessionInfo &client_info,
                                            ObServerSessionInfo &server_info);
  static int assign_session_vars_version(ObClientSessionInfo &client_info,
                                         ObServerSessionInfo &server_info);

  static int handle_capability_flag_var(ObClientSessionInfo &client_info,
                                        ObServerSessionInfo &server_info,
                                        const common::ObString &value,
                                        const bool is_auth_request,
                                        bool &need_save);
  static int handle_enable_transmission_checksum_flag_var(ObClientSessionInfo &client_info,
                                                      ObServerSessionInfo &server_info,
                                                      const common::ObString &value,
                                                      const bool is_auth_request,
                                                      bool &need_save);
  static int save_changed_sess_info(ObClientSessionInfo& client_info,
                                    ObServerSessionInfo& server_info,
                                    Ob20ExtraInfo& extra_info,
                                    common::ObSimpleTrace<4096> &trace_log,
                                    bool is_only_sync_trans_sess);

private:
  static ObProxySysVarType get_sys_var_type(const common::ObString &var_name);

  static int handle_global_variables_version_var(ObClientSessionInfo &client_info,
                                                 const common::ObString &value,
                                                 const bool is_auth_request,
                                                 bool &need_save);

  static int handle_user_privilege_var(ObClientSessionInfo &client_info,
                                       const common::ObString &value,
                                       const bool is_auth_request,
                                       bool &need_save);

  static int handle_set_trx_executed_var(ObClientSessionInfo &client_info,
                                         const common::ObString &value,
                                         const bool is_auth_request,
                                         bool &need_save);

  static int handle_partition_hit_var(const common::ObString &value,
                                      const bool is_auth_request,
                                      ObRespAnalyzeResult &resp_result,
                                      bool &need_save);

  static int handle_last_insert_id_var(ObClientSessionInfo &client_info,
                                       const obmysql::ObStringKV &str_kv,
                                       const bool is_auth_request,
                                       ObRespAnalyzeResult &resp_result,
                                       bool &need_save);

  static int handle_safe_snapshot_var(ObClientSessionInfo &client_info,
                                      const common::ObString &value,
                                      bool &need_save);

  static int handle_route_policy_var(ObClientSessionInfo &client_info,
                                     const obmysql::ObStringKV &str_kv,
                                     const bool is_auth_request,
                                     ObRespAnalyzeResult &resp_result,
                                     bool &need_save);

  static int handle_statement_trace_id_var(ObRespAnalyzeResult &resp_result,
                                           const common::ObString &value,
                                           bool &need_save);

  static int handle_read_consistency_var(ObClientSessionInfo &client_info,
                                         const obmysql::ObStringKV &str_kv,
                                         const bool is_auth_request,
                                         ObRespAnalyzeResult &resp_result,
                                         bool &need_save);

  static int handle_common_var(ObClientSessionInfo &client_info,
                               const obmysql::ObStringKV &str_kv,
                               const bool is_auth_request,
                               ObRespAnalyzeResult &resp_result,
                               bool &need_save);

  static int handle_sys_var(ObClientSessionInfo &client_info,
                            ObServerSessionInfo &server_info,
                            const obmysql::ObStringKV &str_kv,
                            const bool is_auth_request,
                            ObRespAnalyzeResult &resp_result,
                            const bool is_save_to_common_sys = false);
  static int handle_necessary_sys_var(ObClientSessionInfo &client_info,
                                      ObServerSessionInfo &server_info,
                                      const obmysql::ObStringKV &str_kv,
                                      const bool is_auth_request,
                                      ObRespAnalyzeResult &resp_result);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_SESSION_INFO_HANDLER_H
