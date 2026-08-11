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

#ifndef OBPROXY_MYSQL_CSHA2_HANDLER_H
#define OBPROXY_MYSQL_CSHA2_HANDLER_H

#include "lib/utility/ob_macro_utils.h"
#include "proxy/mysql/ob_mysql_transact.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

// process_client_csha2_auth_more_resp 成功时由出参带回语义，供 ObMysqlTransact::handle_csha2_request 做状态机收尾。
enum class ObMysqlCsha2RequestOutcome {
  INVALID = 0,
  REPLIED_PUBLIC_KEY,
  REPLIED_DEFERRED_OK,
  FORWARDED_TO_SERVER,
};

class ObMysqlCsha2Handler
{
public:
  static bool is_client_ssl(ObMysqlTransact::ObTransState &s);
  static bool is_server_ssl(ObMysqlServerSession *server_session);
  static bool client_leg_allows_forced_csha2_full_auth(ObMysqlTransact::ObTransState &s);
  static bool is_fast_auth_succ_with_ok(ObMysqlTransact::ObTransState &s);
  static bool should_force_full_auth_on_login_resp(ObMysqlTransact::ObTransState &s,
                                                   ObClientSessionInfo &client_info);
  static bool should_force_full_auth_on_request_resp(ObMysqlTransact::ObTransState &s,
                                                     ObClientSessionInfo &client_info,
                                                     bool &is_change_user_resp);
  static bool is_request_public_key_packet(const ObClientSessionInfo &client_info);
  static void record_pending_auth_switch_plugin(ObClientSessionInfo &client_info,
                                                const common::ObString &auth_plugin_name);
  static int save_change_user_req_for_auth_switch(ObMysqlTransact::ObTransState &s,
                                                  const char *trace_desc);
  static int save_deferred_ok(event::ObIOBufferReader *buf_reader,
                              ObClientSessionInfo &client_info,
                              const int64_t ok_offset,
                              const int64_t ok_pkt_len);
  static int build_deferred_ok_for_mysql_client(ObMysqlTransact::ObTransState &s,
                                                ObClientSessionInfo &client_info,
                                                const char *ok_ptr,
                                                const int64_t ok_len,
                                                const uint8_t seq_for_client,
                                                char *&out_buf,
                                                int64_t &out_len);
  // 仅在外层已判定为非 TLS 且 auth_more_data_resp_raw_ 负载为 ODP 私钥对应 RSA 密文块时调用；失败即向上返回错误码。
  static int decrypt_rsa_password_and_normalize(ObClientSessionInfo &client_info);
  static int copy_tls_client_auth_more_raw_to_for_server(ObClientSessionInfo &client_info);
  static int encrypt_password_with_server_public_key(ObClientSessionInfo &client_info,
                                                     ObMysqlServerSession *server_session,
                                                     const common::ObString &server_rsa_public_key);
  static int send_internal_public_key(ObMysqlTransact::ObTransState &s,
                                      const uint8_t client_auth_more_data_resp_pkt_seq);
  static int send_internal_full_auth_required(ObMysqlTransact::ObTransState &s,
                                              const uint8_t base_req_pkt_seq,
                                              const char *trace_stage,
                                              const char *base_seq_name);
  static int send_internal_full_auth_required_after_login(ObMysqlTransact::ObTransState &s,
                                                          ObClientSessionInfo &client_info);
  static int send_internal_full_auth_required_after_request(ObMysqlTransact::ObTransState &s,
                                                            ObClientSessionInfo &client_info);
  static void reply_err_to_client_and_break(ObMysqlTransact::ObTransState &s, const int fail_ret);
  static int process_client_csha2_auth_more_resp(ObMysqlTransact::ObTransState &s,
                                                 ObMysqlCsha2RequestOutcome &outcome);
  // FORWARDED_TO_SERVER 时由 transact 调用：写 ODP->server 方向状态；随后应调用 handle_oceanbase_request。
  static int apply_forward_auth_more_to_server_transact_state(ObMysqlTransact::ObTransState &s);
  static int begin_forced_full_auth(ObMysqlTransact::ObTransState &s,
                                    ObClientSessionInfo &client_info,
                                    const char *trace_desc);
  static void handle_server_rsa_public_key_response(ObMysqlTransact::ObTransState &s);
  // 调用前须已 consume 客户端 AuthMoreData 响应，且当前包非 0x02（0x02 由 process_client_csha2_auth_more_resp 统一处理）
  static int forward_auth_more_password_to_server(ObMysqlTransact::ObTransState &s,
                                                  ObClientSessionInfo &client_info);

private:
  // 客户端 AuthMoreData 响应里发 0x02 要 proxy 公钥：alloc internal_buffer（若尚无）、发包并置 csha2_skip_phase_update_（序号取自 s.trans_info_.client_request_）
  static int reply_proxy_rsa_public_key_to_client(ObMysqlTransact::ObTransState &s);
  // need_force 且客户端已 consume、非 0x02：解密密码并回写 deferred login OK 给客户端
  static int reply_deferred_ok_for_need_force(ObMysqlTransact::ObTransState &s,
                                              ObClientSessionInfo &client_info);
  // 解析 AuthMoreData 密码：非 TLS 走 RSA 解密规范化；TLS 时仅当 is_need_copy 为 true 才拷贝到 for_server（转发 observer 需要；仅回 deferred OK 时不需要）
  static int handle_client_auth_more_password(ObClientSessionInfo &client_info,
                                               const bool is_client_ssl,
                                               const bool is_need_copy);
  // 调用解密方法之前用来检查前置条件的方法
  static int check_non_tls_auth_more_rsa_decrypt_precondition(const ObClientSessionInfo &client_info);
  ObMysqlCsha2Handler();
  DISALLOW_COPY_AND_ASSIGN(ObMysqlCsha2Handler);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_MYSQL_CSHA2_HANDLER_H
