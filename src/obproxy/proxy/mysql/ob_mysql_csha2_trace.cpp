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

#include "proxy/mysql/ob_mysql_csha2_trace.h"
#include "proxy/mysql/ob_mysql_sm.h"
#include "rpc/obmysql/packet/ompk_auth_switch_req.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

void ObMysqlCsha2Trace::trace_cached_login(const char *stage, ObMysqlClientSession *cs)
{
  if (OB_NOT_NULL(cs)) {
    ObClientSessionInfo &cs_info = cs->get_session_info();
    ObClientSessionCsha2AuthContext &csha2_ctx = cs_info.get_csha2_auth_ctx();
    OMPKHandshakeResponse &hsr = cs_info.get_login_req().get_hsr_result().response_;
    const ObString &auth_resp = hsr.get_auth_response();
    PROXY_CSHA2_LOG(DEBUG, "cached login snapshot", "stage", stage, "cs_id", cs->get_cs_id(),
              "auth_resp_len", auth_resp.length(), "pending_plugin", csha2_ctx.pending_auth_switch_plugin_);
  }
}

void ObMysqlCsha2Trace::trace_server_auth_switch_req(const char *stage, ObMysqlTransact::ObTransState &s)
{
  if (OB_NOT_NULL(s.sm_) && OB_NOT_NULL(s.sm_->get_client_session())) {
    event::ObIOBufferReader *server_buffer_reader = s.sm_->get_server_buffer_reader();
    if (OB_NOT_NULL(server_buffer_reader)) {
      ObMysqlClientSession *cs = s.sm_->get_client_session();
      const int64_t server_buffer_len = server_buffer_reader->read_avail();
      if (server_buffer_len <= MYSQL_NET_META_LENGTH) {
        PROXY_CSHA2_LOG(DEBUG, "server AuthSwitchRequest decode skipped",
                  "stage", stage,
                  "cs_id", cs->get_cs_id(),
                  "send_action", ObMysqlTransact::get_send_action_name(s.current_.send_action_),
                  "sql_cmd", s.trans_info_.sql_cmd_,
                  "server_buffer_len", server_buffer_len);
      } else {
        char *buf = static_cast<char *>(op_fixed_mem_alloc(server_buffer_len));
        if (OB_ISNULL(buf)) {
          PROXY_CSHA2_LOG(WDIAG, "fail to alloc memory for auth switch request trace", "len", server_buffer_len);
        } else {
          server_buffer_reader->copy(buf, server_buffer_len);
          OMPKAuthSwitchReq auth_switch_req;
          uint32_t content_len = 0;
          char *header = buf;
          ObMySQLUtil::get_uint3(header, content_len);
          auth_switch_req.set_content(buf + 4, content_len);
          int tmp_ret = auth_switch_req.decode();
          if (OB_SUCCESS != tmp_ret) {
            PROXY_CSHA2_LOG(WDIAG, "fail to decode server AuthSwitchRequest", K(tmp_ret),
                      "stage", stage,
                      "cs_id", cs->get_cs_id(),
                      "send_action", ObMysqlTransact::get_send_action_name(s.current_.send_action_),
                      "sql_cmd", s.trans_info_.sql_cmd_,
                      "server_buffer_len", server_buffer_len,
                      "content_len", content_len);
          } else {
            const ObString &plugin = auth_switch_req.get_auth_plugin_name();
            const ObString &auth_data = auth_switch_req.get_auth_plugin_data();
            PROXY_CSHA2_LOG(DEBUG, "decoded server AuthSwitchRequest",
                      "stage", stage, "cs_id", cs->get_cs_id(),
                      "server_plugin", plugin, "server_auth_data_len", auth_data.length());
          }
          op_fixed_mem_free(buf, server_buffer_len);
          buf = NULL;
        }
      }
    }
  }
}

void ObMysqlCsha2Trace::trace_first_response_packet(ObMysqlTransact::ObTransState &s)
{
  ObRespAnalyzeResult &resp = s.trans_info_.resp_result_;
  if (OB_NOT_NULL(s.sm_) && OB_NOT_NULL(s.sm_->get_client_session())) {
    if (ObMysqlTransact::SERVER_SEND_HANDSHAKE == s.current_.send_action_
        || ObMysqlTransact::SERVER_SEND_LOGIN == s.current_.send_action_
        || ObMysqlTransact::SERVER_SEND_SAVED_LOGIN == s.current_.send_action_
        || ObMysqlTransact::SERVER_SEND_SAVED_AUTH_SWITCH_RESP == s.current_.send_action_
        || ObMysqlTransact::SERVER_SEND_AUTH_MORE_DATA == s.current_.send_action_
        || ObMysqlTransact::SERVER_SEND_AUTH_MORE_DATA_REQUEST_PUBLIC_KEY == s.current_.send_action_
        || s.is_login_auth_switch_resp_phase()
        || s.is_login_auth_more_data_resp_phase()
        || obmysql::OB_MYSQL_COM_LOGIN == s.trans_info_.sql_cmd_
        || obmysql::OB_MYSQL_COM_AUTH_SWITCH_RESP == s.trans_info_.sql_cmd_
        || obmysql::OB_MYSQL_COM_AUTH_MORE_DATA_RESP == s.trans_info_.sql_cmd_) {
      event::ObIOBufferReader *r = s.sm_->get_server_buffer_reader();
      uint8_t pkt_type = 0;
      uint8_t more_data = 0;
      uint8_t server_pkt_seq = 0;
      if (OB_NOT_NULL(r) && r->read_avail() >= (MYSQL_NET_HEADER_LENGTH + 1)) {
        char b[MYSQL_NET_HEADER_LENGTH + 2];
        const int64_t want = (r->read_avail() >= static_cast<int64_t>(sizeof(b))
                              ? static_cast<int64_t>(sizeof(b))
                              : r->read_avail());
        (void) r->copy(b, want, 0);
        pkt_type = static_cast<uint8_t>(b[MYSQL_NET_HEADER_LENGTH]);
        if (MYSQL_AUTH_EXTRA_DATA_PACKET_TYPE == pkt_type && want >= (MYSQL_NET_HEADER_LENGTH + 2)) {
          more_data = static_cast<uint8_t>(b[MYSQL_NET_HEADER_LENGTH + 1]);
        }
        if (want >= MYSQL_NET_HEADER_LENGTH) {
          server_pkt_seq = static_cast<uint8_t>(b[3]);
        }
      }
      ObMysqlClientSession *cs = s.sm_->get_client_session();
      PROXY_CSHA2_LOG(DEBUG, "recv server response (first packet)",
                "cs_id", cs->get_cs_id(),
                "send_action", ObMysqlTransact::get_send_action_name(s.current_.send_action_),
                "pkt_type", pkt_type,
                "more_data", more_data,
                "is_auth_switch_req", resp.is_auth_switch_req(),
                "is_auth_more_data_req", resp.is_auth_more_data_req(),
                "server_pkt_seq", server_pkt_seq);
      if (resp.is_auth_switch_req()) {
        trace_cached_login("recv_server_auth_switch_req", cs);
        trace_server_auth_switch_req("handle_first_response_packet", s);
      }
    }
  }
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
