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

#ifndef OBPROXY_MYSQL_REQUEST_BUILDER_H
#define OBPROXY_MYSQL_REQUEST_BUILDER_H

#include "utils/ob_proxy_lib.h"
#include "packet/ob_mysql_packet_writer.h"
#include "proxy/mysqllib/ob_mysql_ob20_packet_write.h"
#include "proxy/mysql/ob_mysql_server_session.h"
#include "proxy/mysqllib/ob_compressed_header_param.h"
#include "rpc/obmysql/packet/ompk_change_user.h"

namespace oceanbase
{
namespace common
{
class ObSqlString;
}
namespace obproxy
{
namespace event
{
class ObMIOBuffer;
}
namespace proxy
{
class ObMysqlSM;

typedef int (*BuildFunc)(ObMysqlSM *sm, event::ObMIOBuffer &, ObClientSessionInfo &, ObMysqlServerSession *,
                         const ObProxyProtocol ob_proxy_protocol);

class ObMysqlRequestBuilder
{
public:

  static int add_connect_attr(const char *key, const char *value, OMPKChangeUser &change_user);
  static int add_connect_attr(const char *key, const common::ObString &value, OMPKChangeUser &change_user);

  static int build_request_packet(ObString sql,
                                  obmysql::ObMySQLCmd cmd,
                                  ObMysqlSM *sm,
                                  event::ObMIOBuffer &mio_buf,
                                  ObMysqlServerSession *server_session,
                                  const ObProxyProtocol ob_proxy_protocol);
  // build login packet to send first login request
  static int build_first_login_packet(ObMysqlSM *sm,
                                      event::ObMIOBuffer &mio_buf,
                                      ObClientSessionInfo &client_info,
                                      ObMysqlServerSession *server_session,
                                      const ObProxyProtocol ob_proxy_protocol);

  // build login packet to send orig login request
  static int build_orig_login_packet(ObMysqlSM *sm,
                                     event::ObMIOBuffer &mio_buf,
                                     ObClientSessionInfo &client_info,
                                     ObMysqlServerSession *server_session,
                                     const ObProxyProtocol ob_proxy_protocol);
  
  // build binlog login request
  static int build_binlog_login_packet(ObMysqlSM *sm,
                                       event::ObMIOBuffer &mio_buf,
                                       ObClientSessionInfo &client_info,
                                       ObMysqlServerSession *server_session,
                                       const ObProxyProtocol ob_proxy_protocol);

  // build saved login packet to send saved login request
  static int build_saved_login_packet(ObMysqlSM *sm,
                                      event::ObMIOBuffer &mio_buf,
                                      ObClientSessionInfo &client_info,
                                      ObMysqlServerSession *server_session,
                                      const ObProxyProtocol ob_proxy_protocol);

  // Replay cached COM_AUTH_SWITCH_RESP to server during saved-login (Observer may re-issue AuthSwitchRequest).
  static int build_saved_auth_switch_resp(ObMysqlSM *sm,
                                          event::ObMIOBuffer &mio_buf,
                                          ObClientSessionInfo &client_info,
                                          ObMysqlServerSession *server_session,
                                          const ObProxyProtocol ob_proxy_protocol);

  // build auth more data response packet (e.g. caching_sha2_password full auth password packet)
  static int build_auth_more_data_packet(ObMysqlSM *sm,
                                         event::ObMIOBuffer &mio_buf,
                                         ObClientSessionInfo &client_info,
                                         ObMysqlServerSession *server_session,
                                         const ObProxyProtocol ob_proxy_protocol);

  // BuildFunc-compatible: send 0x02 (request server RSA public key) to server.
  static int build_auth_more_data_request_public_key_for_server(ObMysqlSM *sm,
                                                               event::ObMIOBuffer &mio_buf,
                                                               ObClientSessionInfo &client_info,
                                                               ObMysqlServerSession *server_session,
                                                               const ObProxyProtocol ob_proxy_protocol);

  static int build_auth_more_data_public_key_packet(ObMysqlClientSession *client_session,
                                                    const ObProxyProtocol client_reply_proto,
                                                    event::ObMIOBuffer &mio_buf,
                                                    const common::ObString &public_key,
                                                    const uint8_t pkt_seq);

  static int build_ssl_request_packet(ObMysqlSM *sm,
                                      event::ObMIOBuffer &mio_buf,
                                      ObClientSessionInfo &client_info,
                                      ObMysqlServerSession *server_session,
                                      const ObProxyProtocol ob_proxy_protocol);

  // build packet to sync all session vars
  static int build_all_session_vars_sync_packet(ObMysqlSM *sm,
                                                event::ObMIOBuffer &mio_buf,
                                                ObClientSessionInfo &client_info,
                                                ObMysqlServerSession *server_session,
                                                const ObProxyProtocol ob_proxy_protocol);

  // build OB_MYSQL_COM_INIT_DB packet to sync database name
  static int build_database_sync_packet(ObMysqlSM *sm,
                                        event::ObMIOBuffer &mio_buf,
                                        ObClientSessionInfo &client_info,
                                        ObMysqlServerSession *server_session,
                                        const ObProxyProtocol ob_proxy_protocol);

  // build OB_MYSQL_COM_QUERY packet to sync session vars
  static int build_session_vars_sync_packet(ObMysqlSM *sm,
                                            event::ObMIOBuffer &mio_buf,
                                            ObClientSessionInfo &client_info,
                                            ObMysqlServerSession *server_session,
                                            const ObProxyProtocol ob_proxy_protocol);

  // build OB_MYSQL_COM_QUERY packet to sync session user vars
  static int build_session_user_vars_sync_packet(ObMysqlSM *sm,
                                                 event::ObMIOBuffer &mio_buf,
                                                 ObClientSessionInfo &client_info,
                                                 ObMysqlServerSession *server_session,
                                                 const ObProxyProtocol ob_proxy_protocol);

  // build start transaction request packet
  static int build_start_trans_request(ObMysqlSM *sm,
                                       event::ObMIOBuffer &mio_buf,
                                       ObClientSessionInfo &client_info,
                                       ObMysqlServerSession *server_session,
                                       const ObProxyProtocol ob_proxy_protocol);
  
  static int build_xa_start_request(ObMysqlSM *sm,
                                    event::ObMIOBuffer &mio_buf,
                                    ObClientSessionInfo &client_info,
                                    ObMysqlServerSession *server_session,
                                    const ObProxyProtocol ob_proxy_protocol);

  // build mysql request packet
  static int build_mysql_request(event::ObMIOBuffer &mio_buf,
                                 const obmysql::ObMySQLCmd cmd,
                                 const common::ObString &sql,
                                 const bool need_compress,
                                 const bool is_checksum_on, 
                                 const int64_t compression_level);

  // build mysql prepare request packet
  static int build_prepare_request(ObMysqlSM *sm,
                                   event::ObMIOBuffer &mio_buf,
                                   ObClientSessionInfo &client_info,
                                   ObMysqlServerSession *server_session,
                                   const ObProxyProtocol ob_proxy_protocol);

  // build mysql text_ps prepare request packet
  static int build_text_ps_prepare_request(ObMysqlSM *sm,
                                           event::ObMIOBuffer &mio_buf,
                                           ObClientSessionInfo &client_info,
                                           ObMysqlServerSession *server_session,
                                           const ObProxyProtocol ob_proxy_protocol);

  static int build_reset_session_request(ObMysqlSM *sm,
                                         event::ObMIOBuffer &mio_buf,
                                         ObClientSessionInfo &client_info,
                                         ObMysqlServerSession *server_session,
                                         const ObProxyProtocol ob_proxy_protocol);

  // build mysql init sql request packet
  static int build_init_sql_request_packet(ObMysqlSM *sm,
                                           event::ObMIOBuffer &mio_buf,
                                           ObClientSessionInfo &client_info,
                                           ObMysqlServerSession *server_session,
                                           const ObProxyProtocol ob_proxy_protocol);

  static int build_request_from_packet_str(ObMysqlSM *sm,
                                           const ObString &packet_str,
                                           event::ObMIOBuffer &mio_buf,
                                           ObMysqlServerSession *server_session,
                                           const ObProxyProtocol ob_proxy_protocol,
                                           const ObIArray<ObObJKV> *extra_info = NULL);
};

inline int ObMysqlRequestBuilder::build_first_login_packet(ObMysqlSM *sm,
                                                           event::ObMIOBuffer &mio_buf,
                                                           ObClientSessionInfo &client_info,
                                                           ObMysqlServerSession *server_session,
                                                           const ObProxyProtocol ob_proxy_protocol)

{
  UNUSED(sm);
  UNUSED(server_session);
  UNUSED(ob_proxy_protocol); // auth request no need compress
  ObMysqlAuthRequest &auth_req = client_info.get_login_req();
  ObHSRResult &hsr = auth_req.get_hsr_result();
  ObServerSessionInfo &ss_info = server_session->get_session_info();
  obmysql::ObMySQLCapabilityFlags capability(ss_info.get_compatible_capability_flags().capability_ & hsr.response_.get_capability_flags().capability_);
  ss_info.save_compatible_capability_flags(capability);
  common::ObString &packet_str = auth_req.get_auth_request();
  return packet::ObMysqlPacketWriter::write_raw_packet(mio_buf, packet_str);
}

inline int ObMysqlRequestBuilder::build_orig_login_packet(ObMysqlSM *sm,
                                                          event::ObMIOBuffer &mio_buf,
                                                          ObClientSessionInfo &client_info,
                                                          ObMysqlServerSession *server_session,
                                                          const ObProxyProtocol ob_proxy_protocol)

{
  UNUSED(sm);
  UNUSED(server_session);
  UNUSED(ob_proxy_protocol); // auth request no need compress
  ObMysqlAuthRequest &auth_req = client_info.get_login_req();
  ObHSRResult &hsr = auth_req.get_hsr_result();
  ObServerSessionInfo &ss_info = server_session->get_session_info();
  obmysql::ObMySQLCapabilityFlags capability(ss_info.get_compatible_capability_flags().capability_ & hsr.response_.get_capability_flags().capability_);
  ss_info.save_compatible_capability_flags(capability);
  common::ObString &packet_str = auth_req.get_auth_request();
  return packet::ObMysqlPacketWriter::write_raw_packet(mio_buf, packet_str);
}

inline int ObMysqlRequestBuilder::build_saved_login_packet(ObMysqlSM *sm,
                                                           event::ObMIOBuffer &mio_buf,
                                                           ObClientSessionInfo &client_info,
                                                           ObMysqlServerSession *server_session,
                                                           const ObProxyProtocol ob_proxy_protocol)
{
  UNUSED(sm);
  UNUSED(ob_proxy_protocol); // auth request no need compress
  ObMysqlAuthRequest &auth_req = client_info.get_login_req();
  ObHSRResult &hsr = auth_req.get_hsr_result();
  ObServerSessionInfo &ss_info = server_session->get_session_info();
  obmysql::ObMySQLCapabilityFlags capability(ss_info.get_compatible_capability_flags().capability_ & hsr.response_.get_capability_flags().capability_);
  ss_info.save_compatible_capability_flags(capability);
  common::ObString &packet_str = auth_req.get_auth_request();
  return packet::ObMysqlPacketWriter::write_raw_packet(mio_buf, packet_str);
}

inline int ObMysqlRequestBuilder::build_saved_auth_switch_resp(ObMysqlSM *sm,
                                                               event::ObMIOBuffer &mio_buf,
                                                               ObClientSessionInfo &client_info,
                                                               ObMysqlServerSession *server_session,
                                                               const ObProxyProtocol ob_proxy_protocol)
{
  int ret = common::OB_SUCCESS;
  ObClientSessionCsha2AuthContext &csha2_ctx = client_info.get_csha2_auth_ctx();
  const char *src = csha2_ctx.auth_switch_resp_.ptr();
  const int64_t total_len = csha2_ctx.auth_switch_resp_.len();
  if (OB_UNLIKELY(total_len <= MYSQL_NET_HEADER_LENGTH) || OB_ISNULL(src)) {
    ret = common::OB_ERR_UNEXPECTED;
    PROXY_CSHA2_LOG(WDIAG, "cached auth switch resp empty, cannot replay to server on saved login",
              K(ret), K(total_len));
  } else if (OB_ISNULL(sm) || OB_ISNULL(server_session)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_LOG(WDIAG, "sm or server_session is null", K(ret));
  } else {
    char *pkt = static_cast<char *>(op_fixed_mem_alloc(total_len));
    if (OB_ISNULL(pkt)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      PROXY_LOG(WDIAG, "alloc failed for saved auth switch replay", K(ret), K(total_len));
    } else {
      MEMCPY(pkt, src, total_len);
      ObMysqlAuthRequest &auth_req = client_info.get_login_req();
      const uint8_t seq = static_cast<uint8_t>(
          static_cast<uint8_t>(auth_req.get_packet_meta().pkt_seq_) + 2);
      pkt[3] = static_cast<char>(seq);
      common::ObString packet_str(static_cast<int32_t>(total_len), pkt);
      if (ObProxyProtocol::PROTOCOL_OCEANBASE_20 == ob_proxy_protocol
          || ObProxyProtocol::PROTOCOL_COMPRESSED_MYSQL == ob_proxy_protocol) {
        ret = build_request_from_packet_str(sm, packet_str, mio_buf, server_session, ob_proxy_protocol, NULL);
      } else {
        ret = packet::ObMysqlPacketWriter::write_raw_packet(mio_buf, packet_str);
      }
      op_fixed_mem_free(pkt, total_len);
    }
  }
  return ret;
}

inline int ObMysqlRequestBuilder::build_auth_more_data_packet(ObMysqlSM *sm,
                                                              event::ObMIOBuffer &mio_buf,
                                                              ObClientSessionInfo &client_info,
                                                              ObMysqlServerSession *server_session,
                                                              const ObProxyProtocol ob_proxy_protocol)
{
  // 构造auth more data resp 给server
  // 优先级是：rsa_wire → for_server → raw
  int ret = common::OB_SUCCESS;
  ObClientSessionCsha2AuthContext &csha2_ctx = client_info.get_csha2_auth_ctx();
  obutils::ObVariableLenBuffer<OB_AUTH_MORE_DATA_RESP_LEN> &rsa_wire =
      csha2_ctx.auth_more_data_resp_rsa_wire_;
  obutils::ObVariableLenBuffer<OB_AUTH_MORE_DATA_RESP_LEN> &for_server_resp =
      csha2_ctx.auth_more_data_resp_for_server_;
  obutils::ObVariableLenBuffer<OB_AUTH_MORE_DATA_RESP_LEN> &raw_resp =
      csha2_ctx.auth_more_data_resp_raw_;
  obutils::ObVariableLenBuffer<OB_AUTH_MORE_DATA_RESP_LEN> *selected_resp =
      csha2_ctx.get_auth_more_data_resp_to_server();
  if (OB_UNLIKELY(selected_resp->len() <= MYSQL_NET_HEADER_LENGTH)) {
    ret = common::OB_ERR_UNEXPECTED;
    PROXY_LOG(WDIAG, "auth more data resp is empty, can not build packet", K(ret),
              "rsa_wire_len", rsa_wire.len(),
              "for_server_len", for_server_resp.len(),
              "raw_len", raw_resp.len());
  } else {
    const char *const p = selected_resp->ptr();
    const int64_t plen = selected_resp->len();
    common::ObString packet_str(static_cast<int32_t>(plen), p);
    // OB20 / compressed-MySQL: one get_next for outer framing only (see build_request_from_packet_str).
    // Inner MySQL header (incl. pkt[3] seq) comes from client or csha2 buffers; do not send raw only.
    if (ObProxyProtocol::PROTOCOL_OCEANBASE_20 == ob_proxy_protocol
        || ObProxyProtocol::PROTOCOL_COMPRESSED_MYSQL == ob_proxy_protocol) {
      ret = build_request_from_packet_str(sm, packet_str, mio_buf, server_session, ob_proxy_protocol, NULL);
    } else {
      ret = packet::ObMysqlPacketWriter::write_raw_packet(mio_buf, packet_str);
    }
  }
  return ret;
}

inline int ObMysqlRequestBuilder::build_auth_more_data_request_public_key_for_server(
    ObMysqlSM *sm,
    event::ObMIOBuffer &mio_buf,
    ObClientSessionInfo &client_info,
    ObMysqlServerSession *server_session,
    const ObProxyProtocol ob_proxy_protocol)
{
  // 构造0x02给server
  UNUSED(client_info);
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(server_session)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_LOG(WDIAG, "server_session is null", K(ret));
  } else {
    char pkt[MYSQL_NET_HEADER_LENGTH + 1];
    int64_t pos = 0;
    if (OB_FAIL(ObMySQLUtil::store_int3(pkt, MYSQL_PAYLOAD_LENGTH_LENGTH, 1, pos))) {
      PROXY_LOG(WDIAG, "fail to store auth more data request-public-key packet len", K(ret), K(pos));
    } else {
      // pkt[3]: classic MySQL per-packet sequence inside the payload. OB20 request_id is assigned
      // only in build_request_from_packet_str (get_next_server_request_id); never use get_next here.
      pkt[3] = 0;
      pkt[4] = static_cast<char>(MYSQL_AUTH_REQUEST_PUBLIC_KEY_TYPE);
      common::ObString packet_str(static_cast<int32_t>(sizeof(pkt)), pkt);
      if (ObProxyProtocol::PROTOCOL_OCEANBASE_20 == ob_proxy_protocol
          || ObProxyProtocol::PROTOCOL_COMPRESSED_MYSQL == ob_proxy_protocol) {
        ret = build_request_from_packet_str(sm, packet_str, mio_buf, server_session, ob_proxy_protocol, NULL);
      } else {
        ret = packet::ObMysqlPacketWriter::write_raw_packet(mio_buf, packet_str);
      }
    }
  }
  return ret;
}

inline int ObMysqlRequestBuilder::build_auth_more_data_public_key_packet(
    ObMysqlClientSession *client_session,
    const ObProxyProtocol client_reply_proto,
    event::ObMIOBuffer &mio_buf,
    const common::ObString &public_key,
    const uint8_t pkt_seq)
{
  // 构造0x01 public key给client
  int ret = common::OB_SUCCESS;
  const int64_t payload_len = 1 + public_key.length();
  const int64_t packet_len = MYSQL_NET_HEADER_LENGTH + payload_len;
  char *pkt = static_cast<char *>(op_fixed_mem_alloc(packet_len));
  if (OB_ISNULL(pkt)) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    PROXY_LOG(WDIAG, "fail to allocate auth more data public key packet", K(ret), K(packet_len));
  } else {
    int64_t pos = 0;
    if (OB_FAIL(ObMySQLUtil::store_int3(pkt, MYSQL_PAYLOAD_LENGTH_LENGTH, payload_len, pos))) {
      PROXY_LOG(WDIAG, "fail to store auth more data public key packet len", K(ret), K(payload_len), K(pos));
    } else {
      pkt[3] = static_cast<char>(pkt_seq);
      pkt[4] = static_cast<char>(MYSQL_AUTH_EXTRA_DATA_PACKET_TYPE);
      if (public_key.length() > 0) {
        MEMCPY(pkt + MYSQL_NET_HEADER_LENGTH + 1, public_key.ptr(), public_key.length());
      }
      common::ObString packet_str(static_cast<int32_t>(packet_len), pkt);
      if (OB_ISNULL(client_session)) {
        ret = common::OB_ERR_UNEXPECTED;
        PROXY_LOG(WDIAG, "client session is null, cannot write auth more data public key packet",
                  K(ret), K(client_reply_proto));
      } else if (OB_FAIL(packet::ObProxyPacketWriter::write_raw_packet(mio_buf, *client_session,
                                                               client_reply_proto, packet_str))) {
        PROXY_LOG(WDIAG, "fail to write auth more data public key raw packet",
                  K(ret), K(client_reply_proto), K(packet_len), K(public_key.length()));
      }
    }
    op_fixed_mem_free(pkt, packet_len);
    pkt = NULL;
  }
  return ret;
}

inline int ObMysqlRequestBuilder::build_mysql_request(event::ObMIOBuffer &mio_buf,
                                                      const obmysql::ObMySQLCmd cmd,
                                                      const common::ObString &sql,
                                                      const bool need_compress,
                                                      const bool is_checksum_on,
                                                      const int64_t compression_level)
{
  proxy::ObCompressedHeaderParam param(0, is_checksum_on, compression_level);
  return packet::ObMysqlPacketWriter::write_request_packet(mio_buf, cmd, sql, need_compress, param);
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_MYSQL_REQUEST_BUILDER_H
