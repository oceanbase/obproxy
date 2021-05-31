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
#include "proxy/mysqllib/ob_proxy_session_info_handler.h"
#include "rpc/obmysql/packet/ompk_ok.h"
#include "packet/ob_mysql_packet_writer.h"
#include "packet/ob_mysql_packet_reader.h"
#include "proxy/mysqllib/ob_mysql_response.h"
#include "proxy/mysqllib/ob_mysql_packet_rewriter.h"
#include "proxy/mysqllib/ob_mysql_request_analyzer.h"
#include "proxy/mysqllib/ob_proxy_session_info.h"
#include "obutils/ob_proxy_config.h"
#include "proxy/client/ob_client_utils.h"
#include "lib/encrypt/ob_encrypted_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::sql;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::packet;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

const ObString PROXY_IDC_NAME_USER_SESSION_VAR = common::ObString::make_string("proxy_idc_name");
const ObString PROXY_ROUTE_POLICY_USER_SESSION_VAR = common::ObString::make_string("proxy_route_policy");

int ObProxySessionInfoHandler::analyze_extra_ok_packet(ObIOBufferReader &reader,
                                                       ObClientSessionInfo &client_info,
                                                       ObServerSessionInfo &server_info,
                                                       const bool need_handle_sysvar,
                                                       ObRespAnalyzeResult &resp_result)
{
  int ret = OB_SUCCESS;

  int64_t pkt_len = resp_result.get_last_ok_pkt_len();

  if (pkt_len > OB_SIMPLE_OK_PKT_LEN) {
    OMPKOK src_ok;
    ObMysqlPacketReader pkt_reader;
    const ObMySQLCapabilityFlags cap = server_info.get_compatible_capability_flags();
    int64_t offset = reader.read_avail() - pkt_len;
    // get ok packet
    if (OB_FAIL(pkt_reader.get_ok_packet(reader, offset, cap, src_ok))) {
      LOG_WARN("fail to get ok packet", K(ret));
    // save changed info
    } else if (OB_FAIL(save_changed_session_info(client_info, server_info, false,
                                                 need_handle_sysvar, src_ok, resp_result, false))) {
      LOG_WARN("fail to save changed session info", K(ret));
    } else {
      LOG_DEBUG("analyze_extra_ok_packet", K(src_ok));
    }
    // if failed(unexpected), we will go on anyway
    ret = OB_SUCCESS;
  } else {
    // do not analyze ok packet if the ok packet is simple
  }

  // we will trim the extra ok packet anaway
  resp_result.rewritten_last_ok_pkt_len_ = 0;
  ObMIOBuffer *writer = reader.writer();
  if (OB_ISNULL(writer)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null writer", K(writer), K(ret));
  } else if (OB_FAIL(writer->trim(reader, pkt_len))) {
    LOG_WARN("failed to trim last pkt", K(writer), K(pkt_len), K(ret));
  }

  return ret;
}

int ObProxySessionInfoHandler::rebuild_ok_packet(ObIOBufferReader &reader,
                                                 ObClientSessionInfo &client_info,
                                                 ObServerSessionInfo &server_info,
                                                 const bool is_auth_request,
                                                 const bool need_handle_sysvar,
                                                 ObRespAnalyzeResult &resp_result,
                                                 const bool is_save_to_common_sys)
{
  int ret = OB_SUCCESS;
  int64_t pkt_len = resp_result.get_last_ok_pkt_len();

  if (pkt_len > OB_SIMPLE_OK_PKT_LEN) {
    ObMysqlPacketReader pkt_reader;
    const ObMySQLCapabilityFlags cap = server_info.get_compatible_capability_flags();
    OMPKOK src_ok;
    int64_t offset = reader.read_avail() - pkt_len;
    LOG_DEBUG("rebuild_ok_packet", K(reader.read_avail()), K(pkt_len));

    // 1. get ok packet from buffer
    pkt_reader.get_ok_packet(reader, offset, cap, src_ok);
    // 2. save seesion info
    if (OB_FAIL(save_changed_session_info(client_info, server_info, is_auth_request,
                                          need_handle_sysvar, src_ok, resp_result,
                                          is_save_to_common_sys))) {
      LOG_WARN("fail to save changed session info", K(is_auth_request), K(ret));
    } else {
      // 3. rewrite ok packet
      OMPKOK des_ok;
      const ObMySQLCapabilityFlags &orig_cap = client_info.get_orig_capability_flags();
      if (OB_FAIL(ObMysqlPacketRewriter::rewrite_ok_packet(src_ok, orig_cap, des_ok,
          client_info.is_oracle_mode()))) {
        LOG_WARN("fail to rewrite ok packet", K(src_ok), K(ret));
      } else {
        LOG_DEBUG("rebuild_ok_packet succ", K(src_ok), "src_size", src_ok.get_serialize_size(),
                  K(des_ok), "dst_size", des_ok.get_serialize_size());

        // 4. trim the orig ok packet
        resp_result.rewritten_last_ok_pkt_len_ = des_ok.get_serialize_size()
                                                 + MYSQL_NET_HEADER_LENGTH;
        ObMIOBuffer *writer = reader.writer();
        if (OB_ISNULL(writer)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null writer", K(writer), K(ret));
        } else if (OB_FAIL(writer->trim(reader, pkt_len))) {
          LOG_WARN("failed to trim last pkt", K(writer), K(pkt_len), K(ret));
          // 5. write the new ok packet
        } else if (OB_FAIL(ObMysqlPacketWriter::write_packet(*writer, des_ok))) {
          LOG_WARN("fail to write ok packet", K(src_ok), K(ret));
        } else {
          // do nothing
        }
      }
    }
  } else {
    // TODO: we may trim the last byte later
    resp_result.rewritten_last_ok_pkt_len_ = pkt_len;
  }
  return ret;
}

int ObProxySessionInfoHandler::rewrite_query_req_by_sharding(ObClientSessionInfo &client_info,
        ObProxyMysqlRequest &client_request, ObIOBufferReader &buffer_reader) {
  int ret = OB_SUCCESS;
  ObMysqlAnalyzeStatus status = ANALYZE_CONT;

  ObRequestAnalyzeCtx target_ctx;
  target_ctx.is_auth_ = false;
  target_ctx.reader_ = &buffer_reader;
  target_ctx.request_buffer_length_ = 4096;
  target_ctx.cached_variables_ = &client_info.get_cached_variables();
  target_ctx.is_sharding_mode_ = true;
  target_ctx.drop_origin_db_table_name_ = true;

  ObMysqlAuthRequest tmp_auth_req;
  obmysql::ObMySQLCmd tmp_req_cmd = obmysql::OB_MYSQL_COM_MAX_NUM;

  client_request.reset(false);
  ObMysqlRequestAnalyzer::analyze_request(target_ctx, tmp_auth_req,
                                          client_request, tmp_req_cmd, status);

  if (OB_UNLIKELY(ANALYZE_DONE != status)) {
    LOG_WARN("fail to analyze request", K(status), K(ret));
    if (ANALYZE_OBPARSE_ERROR == status) {
      ret = OB_ERR_PARSER_SYNTAX;
    } else if (ANALYZE_OBUNSUPPORT_ERROR == status) {
      ret = OB_ERROR_UNSUPPORT_EXPR_TYPE;
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
  } else {
    LOG_DEBUG("succ to rewrite req packet");
  }

  return ret;
}

int ObProxySessionInfoHandler::rewrite_login_req_by_sharding(ObClientSessionInfo &client_info,
    const ObString &username, const ObString &password, const ObString &database,
    const ObString &tenant_name, const ObString &cluster_name)
{
  int ret = OB_SUCCESS;
  const int64_t BUFFER_SIZE = BUFFER_SIZE_FOR_INDEX(BUFFER_SIZE_INDEX_4K);
  ObMIOBuffer *target_hsr_buf = NULL;
  ObIOBufferReader *target_hsr_reader = NULL;

  // 1. alloc tmp buffer
  if (OB_ISNULL(target_hsr_buf = new_miobuffer(BUFFER_SIZE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for ObMIOBuffer", "size", BUFFER_SIZE, K(ret));
  } else if (OB_ISNULL(target_hsr_reader = target_hsr_buf->alloc_reader())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc reader for ObIOBufferReader", K(ret));
  } else {
    // 2. rewrite handshake response
    OMPKHandshakeResponse target_hsr;
    ObRequestAnalyzeCtx target_ctx;
    ObMysqlAuthRequest &auth_req = client_info.get_login_req();

    // 0. deep copy response
    target_hsr = auth_req.get_hsr_result().response_;

    // 1. assign seq num
    target_hsr.set_seq(static_cast<int8_t>(auth_req.get_packet_meta().pkt_seq_));

    // 2. change username
    target_hsr.set_username(username);

    // 3. change auth_response
    const int64_t pwd_buf_len = SHA1_HASH_SIZE + 1;
    char pwd_buf[pwd_buf_len] = {0};
    char passwd_staged1_buf[ENC_STRING_BUF_LEN]; // 1B '*' + 40B octal num
    ObString passwd_string(ENC_STRING_BUF_LEN, passwd_staged1_buf);
    if (OB_FAIL(ObEncryptedHelper::encrypt_passwd_to_stage1(password, passwd_string))) {
      LOG_WARN("fail to encrypt_passwd_to_stage1", K(ret));
    } else {
      passwd_string += 1;
      OMPKHandshake handshake;
      int64_t actual_len = 0;
      char scramble_buf[SCRAMBLE_LENGTH + 1] = {0};
      handshake.get_scramble(scramble_buf, SCRAMBLE_LENGTH + 1, actual_len);
      ObString scramble_string(actual_len, scramble_buf);
      if (OB_FAIL(ObClientUtils::get_auth_password_from_stage1(passwd_string,
              scramble_string, pwd_buf, pwd_buf_len, actual_len))) {
            LOG_WARN("fail to get get_auth_password_from_stage1", K(ret));
      } else {
        ObString auth_str(actual_len, pwd_buf);
        target_hsr.set_auth_response(auth_str);
      }
    }

    // 4. add OB_CLIENT_CONNECT_WITH_DB flags
    ObMySQLCapabilityFlags cap_flag = target_hsr.get_capability_flags();
    cap_flag.cap_flags_.OB_CLIENT_CONNECT_WITH_DB = 1;
    target_hsr.set_capability_flags(cap_flag);

    // 5. change database;
    target_hsr.set_database(database);

    if (OB_FAIL(ObMysqlPacketWriter::write_packet(*target_hsr_buf, target_hsr))) {
      LOG_WARN("fail to write hsr pkt", K(target_hsr), K(target_hsr_buf), K(ret));
    } else if (OB_FAIL(ObRequestAnalyzeCtx::init_auth_request_analyze_ctx(
                           target_ctx, target_hsr_reader,
                           tenant_name, cluster_name))) {
      LOG_WARN("fail to int request analyze context", K(ret));
    } else {
      ObMysqlAnalyzeStatus status = ANALYZE_CONT;
      obmysql::ObMySQLCmd tmp_req_cmd = obmysql::OB_MYSQL_COM_MAX_NUM;
      ObProxyMysqlRequest tmp_request;

      auth_req.reset();
      ObMysqlRequestAnalyzer::analyze_request(target_ctx, auth_req,
                                              tmp_request, tmp_req_cmd, status);
      if (OB_UNLIKELY(ANALYZE_DONE != status)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to analyze request", K(status), K(ret));
      } else {
        // reload priv info
        ObProxySessionPrivInfo &priv_info = client_info.get_priv_info();
        priv_info.cluster_name_ = auth_req.get_hsr_result().cluster_name_;
        priv_info.tenant_name_ = auth_req.get_hsr_result().tenant_name_;
        priv_info.user_name_ = auth_req.get_hsr_result().user_name_;
        LOG_DEBUG("succ to rewrite login packet", K(username), K(priv_info));
      }
    }
  }

  // 4. free the buffer if need
  if (OB_LIKELY(NULL != target_hsr_buf)) {
    free_miobuffer(target_hsr_buf);
    target_hsr_buf = NULL;
    target_hsr_reader = NULL;
  }
  return ret;
}

int ObProxySessionInfoHandler::rewrite_login_req(ObClientSessionInfo &client_info, ObHandshakeResponseParam &param)
{
  int ret = OB_SUCCESS;
  const int64_t BUFFER_SIZE = BUFFER_SIZE_FOR_INDEX(BUFFER_SIZE_INDEX_4K);
  ObMIOBuffer *target_hsr_buf = NULL;
  ObIOBufferReader *target_hsr_reader = NULL;

  // 1. alloc tmp buffer
  if (OB_ISNULL(target_hsr_buf = new_miobuffer(BUFFER_SIZE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for ObMIOBuffer", "size", BUFFER_SIZE, K(ret));
  } else if (OB_ISNULL(target_hsr_reader = target_hsr_buf->alloc_reader())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc reader for ObIOBufferReader", K(ret));
  } else {
    // 2. rewrite handshake response
    OMPKHandshakeResponse target_hsr;
    ObRequestAnalyzeCtx target_ctx;
    ObMysqlAuthRequest &auth_req = client_info.get_login_req();
    ObString default_tenant_name;
    ObString default_cluster_name;

    if (OB_FAIL(client_info.get_tenant_name(default_tenant_name))) {
      LOG_WARN("fail to get tenant name", K(ret));
    } else if (OB_FAIL(client_info.get_cluster_name(default_cluster_name))) {
      LOG_WARN("fail to get cluster name", K(ret));
    } else if (OB_FAIL(ObMysqlPacketRewriter::rewrite_handshake_response_packet(auth_req, param, target_hsr))) {
      LOG_WARN("fail to rewrite handshake response packet", K(ret));
      // 3. analyze the rewrited handshake response
    } else if (OB_FAIL(ObMysqlPacketWriter::write_packet(*target_hsr_buf, target_hsr))) {
      LOG_WARN("fail to write hsr pkt", K(target_hsr), K(target_hsr_buf), K(ret));
    } else if (OB_FAIL(ObRequestAnalyzeCtx::init_auth_request_analyze_ctx(
            target_ctx, target_hsr_reader,
            default_tenant_name, default_cluster_name))) {
      LOG_WARN("fail to int request analyze context", K(ret));
    } else {
      ObMysqlAnalyzeStatus status = ANALYZE_CONT;
      obmysql::ObMySQLCmd tmp_req_cmd = obmysql::OB_MYSQL_COM_MAX_NUM;
      ObProxyMysqlRequest tmp_request;

      auth_req.reset();
      ObMysqlRequestAnalyzer::analyze_request(target_ctx, auth_req,
                                              tmp_request, tmp_req_cmd, status, client_info.is_oracle_mode());
      if (OB_UNLIKELY(ANALYZE_DONE != status)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to analyze request", K(status), K(ret));
      } else {
        // reload priv info
        ObProxySessionPrivInfo &priv_info = client_info.get_priv_info();
        priv_info.cluster_name_ = auth_req.get_hsr_result().cluster_name_;
        priv_info.tenant_name_ = auth_req.get_hsr_result().tenant_name_;
        priv_info.user_name_ = auth_req.get_hsr_result().user_name_;
        LOG_DEBUG("succ to rewrite login packet", K(param), K(priv_info));
      }
    }
  }

  // 4. free the buffer if need
  if (OB_LIKELY(NULL != target_hsr_buf)) {
    free_miobuffer(target_hsr_buf);
    target_hsr_buf = NULL;
    target_hsr_reader = NULL;
  }
  return ret;
}

int  ObProxySessionInfoHandler::rewrite_ldg_login_req(ObClientSessionInfo &client_info,
                                                      const ObString &ldg_tenant_name,
                                                      const ObString &ldg_cluster_name)
{
  int ret = OB_SUCCESS;
  const int64_t BUFFER_SIZE = BUFFER_SIZE_FOR_INDEX(BUFFER_SIZE_INDEX_4K);
  ObMIOBuffer *target_hsr_buf = NULL;
  ObIOBufferReader *target_hsr_reader = NULL;
  // 1.alloc tmp buffer
  if (OB_ISNULL(target_hsr_buf = new_miobuffer(BUFFER_SIZE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_CS_LOG(WARN, "fail to allocate memory for ObMIOBuffer", "size", BUFFER_SIZE, K(ret));
  } else if (OB_ISNULL(target_hsr_reader = target_hsr_buf->alloc_reader())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_CS_LOG(WARN, "fail to alloc reader for ObIOBufferReader", K(ret));
  } else {
    OMPKHandshakeResponse target_hsr;
    ObRequestAnalyzeCtx target_ctx;
    ObMysqlAuthRequest &auth_req = client_info.get_login_req();
    const ObHSRResult &hsr = client_info.get_login_req().get_hsr_result();
    char username_buf[OB_PROXY_FULL_USER_NAME_MAX_LEN];
    int64_t pos = 0;
    memset(username_buf, 0, sizeof(username_buf));
    MEMCPY(username_buf, hsr.user_name_.ptr(), hsr.user_name_.length());
    pos += hsr.user_name_.length();
    MEMCPY(username_buf + pos, "@", 1);
    pos += 1;
    MEMCPY(username_buf + pos, ldg_tenant_name.ptr(), ldg_tenant_name.length());
    pos += ldg_tenant_name.length();
    MEMCPY(username_buf + pos, "#", 1);
    pos += 1;
    MEMCPY(username_buf + pos, ldg_cluster_name.ptr(), ldg_cluster_name.length());
    pos += ldg_cluster_name.length();
    username_buf[pos] = '\0';
    target_hsr = auth_req.get_hsr_result().response_;
    target_hsr.set_seq(static_cast<int8_t>(auth_req.get_packet_meta().pkt_seq_));
    target_hsr.set_username(username_buf);
    if (OB_FAIL(ObMysqlPacketWriter::write_packet(*target_hsr_buf, target_hsr))) {
      PROXY_CS_LOG(WARN, "fail to write hsr pkt", K(target_hsr), K(target_hsr_buf), K(ret));
    } else if (OB_FAIL(ObRequestAnalyzeCtx::init_auth_request_analyze_ctx(
                                      target_ctx, target_hsr_reader,
                                      ldg_tenant_name, ldg_cluster_name))) {
      PROXY_CS_LOG(WARN, "fail to init request analyze context", K(ret));
    } else {
      ObMysqlAnalyzeStatus status = ANALYZE_CONT;
      obmysql::ObMySQLCmd tmp_req_cmd = obmysql::OB_MYSQL_COM_MAX_NUM;
      ObProxyMysqlRequest tmp_request;
      auth_req.reset();
      ObMysqlRequestAnalyzer::analyze_request(target_ctx, auth_req,
                                              tmp_request, tmp_req_cmd, status);
      if (OB_UNLIKELY(ANALYZE_DONE != status)) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_CS_LOG(WARN, "fail to analyze request", K(status), K(ret));
      } else {
        ObProxySessionPrivInfo &priv_info = client_info.get_priv_info();
        priv_info.cluster_name_ = auth_req.get_hsr_result().cluster_name_;
        priv_info.tenant_name_ = auth_req.get_hsr_result().tenant_name_;
        priv_info.user_name_ = auth_req.get_hsr_result().user_name_;
        PROXY_CS_LOG(DEBUG, "rewrite ldg login req", K(auth_req.get_hsr_result()));
      }
    }
    if (OB_LIKELY(NULL != target_hsr_buf)) {
      free_miobuffer(target_hsr_buf);
      target_hsr_buf = NULL;
      target_hsr_reader = NULL;
    }
  }
  return ret;
}

int ObProxySessionInfoHandler::rewrite_ssl_req(ObClientSessionInfo &client_info)
{
  int ret = OB_SUCCESS;
  OMPKHandshakeResponse resp = client_info.get_login_req().get_hsr_result().response_;
  OMPKSSLRequest &ssl_req = client_info.get_ssl_req();

  // SSL Request seq is 1
  ssl_req.set_seq(1);

  ObMySQLCapabilityFlags cap_flag = resp.get_capability_flags();
  cap_flag.cap_flags_.OB_CLIENT_SSL = 1;
  cap_flag.cap_flags_.OB_CLIENT_CONNECT_ATTRS = 1;
  cap_flag.cap_flags_.OB_CLIENT_SESSION_TRACK = 1;
  ssl_req.set_capability_flags(cap_flag);

  ssl_req.set_max_packet_size(resp.get_max_packet_size());
  ssl_req.set_character_set(resp.get_char_set());

  return ret;
}

int ObProxySessionInfoHandler::rewrite_first_login_req(ObClientSessionInfo &client_info,
                                                       const ObString &cluster_name,
                                                       const int64_t cluster_id,
                                                       const uint32_t server_sessid,
                                                       const uint64_t proxy_sessid,
                                                       const ObString &server_scramble,
                                                       const ObString &proxy_scramble,
                                                       const ObAddr &client_addr,
                                                       const bool use_compress,
                                                       const bool use_ob_protocol_v2,
                                                       const bool use_ssl)
{
  int ret = OB_SUCCESS;
  // first login packet, which will be sent to observer at first auth
  // (include modified packet data and analyzed result, with -D database)
  ObHandshakeResponseParam param;
  param.is_saved_login_ = false;
  param.use_compress_ = use_compress;
  param.use_ob_protocol_v2_ = use_ob_protocol_v2;
  param.use_ssl_ = use_ssl;
  // in the first login packet the version is set to 0,
  // in the saved login packet the version is set to the global vars version
  // observer used this value to judge whether the connection is first server session
  const int64_t global_vars_version = 0;
  if (OB_FAIL(rewrite_common_login_req(client_info, param, global_vars_version, cluster_name,
      cluster_id, server_sessid, proxy_sessid, server_scramble, proxy_scramble, client_addr))) {
    LOG_WARN("fail to rewrite_common_login_req", K(param), K(ret));
  } else {
    LOG_DEBUG("succ to rewrite_first_login_req", K(param));
  }
  return ret;
}

int ObProxySessionInfoHandler::rewrite_saved_login_req(ObClientSessionInfo &client_info,
                                                       const ObString &cluster_name,
                                                       const int64_t cluster_id,
                                                       const uint32_t server_sessid,
                                                       const uint64_t proxy_sessid,
                                                       const ObString &server_scramble,
                                                       const ObString &proxy_scramble,
                                                       const ObAddr &client_addr,
                                                       const bool use_compress,
                                                       const bool use_ob_protocol_v2,
                                                       const bool use_ssl)
{
  int ret = OB_SUCCESS;
  // saved login packet, which will be sent to observer later
  // (include modified packet data and analyzed result, without -D database)
  ObHandshakeResponseParam param;
  param.is_saved_login_ = true;
  param.use_compress_ = use_compress;
  param.use_ob_protocol_v2_ = use_ob_protocol_v2;
  param.use_ssl_ = use_ssl;
  // in the first login packet the version is set to 0,
  // in the saved login packet the version is set to the global vars version
  // observer used this value to judge whether the connection is first server session
  const int64_t global_vars_version = client_info.get_global_vars_version();
  if (OB_FAIL(rewrite_common_login_req(client_info, param, global_vars_version, cluster_name,
      cluster_id, server_sessid, proxy_sessid, server_scramble, proxy_scramble, client_addr))) {
    LOG_WARN("fail to rewrite_common_login_req", K(param), K(ret));
  } else {
    LOG_DEBUG("succ to rewrite_saved_login_req", K(param));
  }
  return ret;
}

inline int ObProxySessionInfoHandler::rewrite_common_login_req(ObClientSessionInfo &client_info,
                                                               ObHandshakeResponseParam &param,
                                                               const int64_t global_vars_version,
                                                               const ObString &cluster_name,
                                                               const int64_t cluster_id,
                                                               const uint32_t server_sessid,
                                                               const uint64_t proxy_sessid,
                                                               const ObString &server_scramble,
                                                               const ObString &proxy_scramble,
                                                               const ObAddr &client_addr)
{
  int ret = OB_SUCCESS;
  uint64_t cap = OBPROXY_DEFAULT_CAPABILITY_FLAG;
  if (param.use_compress_) {
    cap |= OB_CAP_CHECKSUM;
  } else {
    cap &= ~OB_CAP_CHECKSUM;
  }

  if (param.use_ob_protocol_v2_) {
    cap |= (OB_CAP_OB_PROTOCOL_V2 | OB_CAP_PROXY_REROUTE);
  } else {
    cap &= ~(OB_CAP_OB_PROTOCOL_V2 | OB_CAP_PROXY_REROUTE);
  }

  param.cluster_name_ = cluster_name;
  const bool need_write_proxy_sramble = !proxy_scramble.empty()
      && !client_info.get_login_req().get_hsr_result().response_.get_auth_response().empty();

  if (OB_FAIL(param.write_conn_id_buf(server_sessid))) {
    LOG_WARN("fail to write connection id", K(server_sessid), K(ret));
  } else if (OB_FAIL(param.write_proxy_version_buf())) {
    LOG_WARN("fail to write proxy version", K(ret));
  } else if (OB_FAIL(param.write_proxy_conn_id_buf(proxy_sessid))) {
    LOG_WARN("fail to write proxy conn id", K(proxy_sessid), K(ret));
  } else if (OB_FAIL(param.write_global_vars_version_buf(global_vars_version))) {
    LOG_WARN("fail to write global vars version_", K(global_vars_version), K(ret));
  } else if (OB_FAIL(param.write_capability_buf(cap))) {
    LOG_WARN("fail to write capability flag", K(cap), K(ret));
  } else if (need_write_proxy_sramble && OB_FAIL(param.write_proxy_scramble(proxy_scramble, server_scramble))) {
    LOG_WARN("fail to write proxy scramble", K(ret));
  } else if (OB_FAIL(param.write_client_addr_buf(client_addr))) {
    LOG_WARN("fail to write client_addr", K(client_addr), K(ret));
  } else if (OB_FAIL(param.write_cluster_id_buf(cluster_id))) {
    LOG_WARN("fail to write cluster id", K(cluster_id), K(ret));
  } else if (OB_FAIL(ObProxySessionInfoHandler::rewrite_login_req(client_info, param))) {
    LOG_WARN("fail to rewrite_login_req", K(param), K(ret));
  } else if (OB_FAIL(ObProxySessionInfoHandler::rewrite_ssl_req(client_info))) {
    LOG_WARN("fail to rewrite_login_req", K(ret));
  }
  return ret;
}

inline ObProxySysVarType ObProxySessionInfoHandler::get_sys_var_type(
    const ObString &var_name)
{
  // TODO:
  // this function simple and inefficient,
  // use trie tree or some other data structure to improve performace later
  ObProxySysVarType type_ret = OBPROXY_VAR_OTHERS;

  if (ObSessionFieldMgr::is_global_version_variable(var_name)) {
    type_ret = OBPROXY_VAR_GLOBAL_VARIABLES_VERSION;
  } else if (ObSessionFieldMgr::is_user_privilege_variable(var_name)) {
    type_ret = OBPROXY_VAR_USER_PRIVILEGE;
  } else if (ObSessionFieldMgr::is_set_trx_executed_variable(var_name)) {
    type_ret = OBPROXY_VAR_SET_TRX_EXECUTED;
  } else if (ObSessionFieldMgr::is_partition_hit_variable(var_name)) {
    type_ret = OBPROXY_VAR_PARTITION_HIT;
  } else if (ObSessionFieldMgr::is_last_insert_id_variable(var_name)) {
    type_ret = OBPROXY_VAR_LAST_INSERT_ID;
  } else if (ObSessionFieldMgr::is_capability_flag_variable(var_name)) {
    type_ret = OBPROXY_VAR_CAPABILITY_FLAG;
  } else if (ObSessionFieldMgr::is_safe_read_snapshot_variable(var_name)) {
    type_ret = OBPROXY_VAR_SAFE_READ_SNAPSHOT;
  } else if (ObSessionFieldMgr::is_route_policy_variable(var_name)) {
    type_ret = OBPROXY_VAR_ROUTE_POLICY_FLAG;
  } else if (ObSessionFieldMgr::is_enable_transmission_checksum_variable(var_name)) {
    type_ret = OBPROXY_VAR_ENABLE_TRANSMISSION_CHECKSUM_FLAG;
  } else if (ObSessionFieldMgr::is_statement_trace_id_variable(var_name)) {
    type_ret = OBPROXY_VAR_STATEMENT_TRACE_ID_FLAG;
  } else if (ObSessionFieldMgr::is_read_consistency_variable(var_name)) {
    type_ret = OBPROXY_VAR_READ_CONSISTENCY_FLAG;
  } else {
    // do noting
  }

  return type_ret;
}

inline int ObProxySessionInfoHandler::handle_global_variables_version_var(
    ObClientSessionInfo &client_info,
    const ObString &value,
    const bool is_auth_request,
    bool &need_save)
{
  int ret = OB_SUCCESS;

  need_save = true;
  if (is_auth_request) {
    int64_t version = 0;
    if (OB_FAIL(get_int_value(value, version))) {
      LOG_WARN("fail to get int64_t value from string", K(value), K(ret));
    } else if (client_info.get_global_vars_version() > 0) {
      LOG_WARN("global vars version has been assigned, ignore it",
               K(client_info.get_global_vars_version()),
               K(value));
    } else {
      LOG_INFO("get global vars version", K(value), K(version));
      client_info.set_global_vars_version(version);
    }
  } else {
    need_save = false;
  }

  return ret;
}

int ObProxySessionInfoHandler::handle_capability_flag_var(
    ObClientSessionInfo &client_info,
    ObServerSessionInfo &server_info,
    const ObString &value,
    const bool is_auth_request,
    bool &need_save)
{
  int ret = OB_SUCCESS;

  need_save = true;
  int64_t cap = 0;
  if (OB_FAIL(get_int_value(value, cap))) {
    LOG_WARN("fail to get int from obstring", K(value), K(ret));
  } else {
    uint64_t local_cap = (0 == client_info.get_ob_capability())
                          ? OBPROXY_DEFAULT_CAPABILITY_FLAG
                          : client_info.get_ob_capability();
    uint64_t client_cap = (cap & local_cap);
    uint64_t server_cap = (cap & OBPROXY_DEFAULT_CAPABILITY_FLAG);

    client_info.set_ob_capability(client_cap);
    server_info.set_ob_capability(server_cap);
    LOG_INFO("succ to set ob_capability_flag", K(client_cap), K(server_cap), "orgin_cap", cap,
             "support_checksum", server_info.is_checksum_supported(), K(is_auth_request));
  }
  return ret;
}

int ObProxySessionInfoHandler::handle_enable_transmission_checksum_flag_var(
    ObClientSessionInfo &client_info,
    ObServerSessionInfo &server_info,
    const ObString &value,
    const bool is_auth_request,
    bool &need_save)
{
  int ret = OB_SUCCESS;

  UNUSED(client_info);
  need_save = true;
  int64_t enable_transmission_checksum = 0;
  if (OB_FAIL(get_int_value(value, enable_transmission_checksum))) {
    LOG_WARN("fail to get int from obstring", K(value), K(ret));
  } else {
    ObProxyChecksumSwitch old_switch = server_info.get_checksum_switch();
    server_info.set_checksum_switch(1 == enable_transmission_checksum);
    LOG_INFO("succ to set checksum_switch", K(enable_transmission_checksum), K(old_switch), K(is_auth_request));
  }
  return ret;
}

int ObProxySessionInfoHandler::handle_statement_trace_id_var(
    ObRespAnalyzeResult &resp_result,
    const ObString &value,
    bool &need_save)
{
  int ret = OB_SUCCESS;

  need_save = false;
  resp_result.set_server_trace_id(value);

  return ret;
}

int ObProxySessionInfoHandler::handle_read_consistency_var(
    ObClientSessionInfo &client_info,
    const obmysql::ObStringKV &str_kv,
    const bool is_auth_request,
    ObRespAnalyzeResult &resp_result,
    bool &need_save)
{
  int ret = OB_SUCCESS;
  int64_t read_consistency = 0;
  if (!is_auth_request) {
    if (OB_FAIL(get_int_value(str_kv.value_, read_consistency))) {
      LOG_WARN("fail to get int64_t value from string", K(str_kv), K(ret));
    } else {
      const ObConsistencyLevel level = static_cast<ObConsistencyLevel>(read_consistency);
      //value: "", "FROZEN", "WEAK", "STRONG"
      if (common::WEAK == level || common::STRONG == level) {
        client_info.set_read_consistency_set_flag(true);
        LOG_INFO("succ to set ob_read_consistency", K(read_consistency));
      } else {
        client_info.set_read_consistency_set_flag(false);
        LOG_INFO("unknown ob_read_consistency, treate it not set", K(read_consistency));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ret = handle_common_var(client_info, str_kv, is_auth_request, resp_result, need_save);
  }
  return ret;
}

inline int ObProxySessionInfoHandler::handle_user_privilege_var(
    ObClientSessionInfo &client_info,
    const ObString &value,
    const bool is_auth_request,
    bool &need_save)
{
  int ret = OB_SUCCESS;

  need_save = true;
  if (is_auth_request) {
    int64_t cur_priv_set = client_info.get_priv_info().user_priv_set_;
    int64_t user_priv_set = -1;

    if (ObProxySessionPrivInfo::is_user_priv_set_available(cur_priv_set)) {
      // do not save user_privilege if it has been set
      need_save = false;
    } else {
      // update user_privilege in first login succ
      if (OB_FAIL(get_int_value(value, user_priv_set))) {
        LOG_WARN("fail to get int from string", "string value", value, K(ret));
      } else if (OB_UNLIKELY(!ObProxySessionPrivInfo::is_user_priv_set_available(user_priv_set))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("user_priv_set should not be -1", "string value", value,
                 K(user_priv_set), K(ret));
      } else {
        client_info.set_user_priv_set(user_priv_set);
        LOG_DEBUG("succ to update proxy user privilege", K(user_priv_set));
      }
    }
  } else {
    // do not save user_privilege if is not in auth_request
    need_save = false;
  }

  return ret;
}

inline int ObProxySessionInfoHandler::handle_set_trx_executed_var(
    ObClientSessionInfo &client_info,
    const ObString &value,
    const bool is_auth_request,
    bool &need_save)
{
  int ret = OB_SUCCESS;

  need_save = true;
  if (is_auth_request) {
    need_save = false;
  } else {
    if (value == ObString::make_string("1")) {
      client_info.set_trans_specified_flag();
      LOG_DEBUG("set transaction specificted");
    } else {
      client_info.clear_trans_specified_flag();
      LOG_DEBUG("clear transaction specificted");
    }
  }

  return ret;
}

inline int ObProxySessionInfoHandler::handle_partition_hit_var(
    const ObString &value,
    const bool is_auth_request,
    ObRespAnalyzeResult &resp_result,
    bool &need_save)
{
  int ret = OB_SUCCESS;

  need_save = true;
  if (is_auth_request) {
    need_save = false;
  } else {
    if (value == ObString::make_string("1")) {
      resp_result.is_partition_hit_ = true;
    } else {
      resp_result.is_partition_hit_ = false;
    }
  }

  return ret;
}

inline int ObProxySessionInfoHandler::handle_last_insert_id_var(
    ObClientSessionInfo &client_info,
    const ObStringKV &str_kv,
    const bool is_auth_request,
    ObRespAnalyzeResult &resp_result,
    bool &need_save)
{
  if (!is_auth_request) {
    resp_result.is_last_insert_id_changed_ = true;
  }

  return handle_common_var(client_info, str_kv, is_auth_request, resp_result, need_save);
}

inline int ObProxySessionInfoHandler::handle_safe_snapshot_var(
    ObClientSessionInfo &client_info,
    const ObString &value,
    bool &need_save)
{
  int ret = OB_SUCCESS;
  int64_t safe_read_snapshot = 0;
  if (OB_FAIL(get_int_value(value, safe_read_snapshot))) {
    LOG_WARN("fail to get int64_t value from string", K(value), K(ret));
  } else {
    client_info.set_safe_read_snapshot(safe_read_snapshot);
  }
  need_save = false;
  return ret;
}

inline int ObProxySessionInfoHandler::handle_route_policy_var(
    ObClientSessionInfo &client_info,
    const obmysql::ObStringKV &str_kv,
    const bool is_auth_request,
    ObRespAnalyzeResult &resp_result,
    bool &need_save)
{
  int ret = OB_SUCCESS;
  int64_t route_policy = 0;
  if (OB_FAIL(get_int_value(str_kv.value_, route_policy))) {
    LOG_WARN("fail to get int64_t value from string", K(str_kv), K(ret));
  } else {
    client_info.set_route_policy(route_policy);
    ret = handle_common_var(client_info, str_kv, is_auth_request, resp_result, need_save);
  }
  return ret;
}

inline int ObProxySessionInfoHandler::handle_common_var(
    ObClientSessionInfo &client_info,
    const ObStringKV &str_kv,
    const bool is_auth_request,
    ObRespAnalyzeResult &resp_result,
    bool &need_save)
{
  int ret = OB_SUCCESS;

  need_save = true;
  if (is_auth_request) {
    bool is_equal = false;
    if (client_info.is_sharding_user() && get_global_proxy_config().is_pool_mode) {
      LOG_WARN("sharding_user with pool should not enter here");
    } else if (!client_info.is_sharding_user() && get_global_proxy_config().is_pool_mode) {
    } else if (OB_FAIL(client_info.is_equal_with_snapshot(str_kv.key_, str_kv.value_, is_equal))) {
      // maybe observer has upgraded
      if (OB_UNLIKELY(OB_ERR_SYS_VARIABLE_UNKNOWN == ret)) {
        resp_result.has_new_sys_var_ = true;
        LOG_WARN("unknown system variable, maybe observer has upgrade", K(str_kv), K(ret));
        ret = OB_SUCCESS;
        // do not save the new variable;
        need_save = false;
      } else {
        LOG_WARN("fail to judge equal with snapshot", K(ret));
      }
    } else {
      // do not save var if it is equal to snapshot
      if (is_equal) {
        need_save = false;
      } else {
        // save it
      }
    } // end of is_equal_with_snapshot
  } else {
    // save it
  }

  return OB_SUCCESS;
}

inline int ObProxySessionInfoHandler::handle_sys_var(ObClientSessionInfo &client_info,
                                                     ObServerSessionInfo &server_info,
                                                     const ObStringKV &str_kv,
                                                     const bool is_auth_request,
                                                     ObRespAnalyzeResult &resp_result,
                                                     const bool is_save_to_common_sys)
{
  // we do not return this value
  int ret = OB_SUCCESS;

  bool need_save = true;
  ObProxySysVarType type = get_sys_var_type(str_kv.key_);
  switch (type) {
    case OBPROXY_VAR_GLOBAL_VARIABLES_VERSION:
      ret = handle_global_variables_version_var(client_info, str_kv.value_,
                                                is_auth_request, need_save);
      break;
    case OBPROXY_VAR_USER_PRIVILEGE:
      ret = handle_user_privilege_var(client_info, str_kv.value_,
                                      is_auth_request, need_save);
      break;
    case OBPROXY_VAR_SET_TRX_EXECUTED:
      ret = handle_set_trx_executed_var(client_info, str_kv.value_,
                                        is_auth_request, need_save);
      break;
    case OBPROXY_VAR_PARTITION_HIT:
      ret = handle_partition_hit_var(str_kv.value_, is_auth_request,
                                     resp_result, need_save);
      break;
    case OBPROXY_VAR_LAST_INSERT_ID:
      ret = handle_last_insert_id_var(client_info, str_kv, is_auth_request,
                                      resp_result, need_save);
      break;
    case OBPROXY_VAR_CAPABILITY_FLAG:
      ret = handle_capability_flag_var(client_info, server_info, str_kv.value_,
                                       is_auth_request, need_save);
      break;
    case OBPROXY_VAR_SAFE_READ_SNAPSHOT:
      ret = handle_safe_snapshot_var(client_info, str_kv.value_, need_save);
      break;

    case OBPROXY_VAR_ROUTE_POLICY_FLAG:
      ret = handle_route_policy_var(client_info, str_kv, is_auth_request,
                                    resp_result, need_save);
      break;

    case OBPROXY_VAR_ENABLE_TRANSMISSION_CHECKSUM_FLAG:
      ret = handle_enable_transmission_checksum_flag_var(client_info, server_info, str_kv.value_,
                                                         is_auth_request, need_save);
      break;

    case OBPROXY_VAR_STATEMENT_TRACE_ID_FLAG:
      ret = handle_statement_trace_id_var(resp_result, str_kv.value_, need_save);
      break;

    case OBPROXY_VAR_READ_CONSISTENCY_FLAG:
      ret = handle_read_consistency_var(client_info, str_kv, is_auth_request, resp_result, need_save);
      break;

    case OBPROXY_VAR_OTHERS:
    default:
      ret = handle_common_var(client_info, str_kv, is_auth_request,
                              resp_result, need_save);
      break;
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("fail to process sys var", K(type), K(ret));
  } else {
    LOG_DEBUG("succ to update sys variables", "key", str_kv.key_, "value", str_kv.value_, K(need_save), K(is_save_to_common_sys));
    if (need_save) {
      // if need save, update sys variable in session field mgr
      if (is_save_to_common_sys) {
        ret = client_info.update_common_sys_variable(str_kv.key_, str_kv.value_, true, true);
      } else {
        ret = client_info.update_sys_variable(str_kv.key_, str_kv.value_);
      }

      if (OB_FAIL(ret)) {
        if (OB_ERR_SYS_VARIABLE_UNKNOWN == ret) { // maybe observer has upgraded
          resp_result.has_new_sys_var_ = true;
          LOG_WARN("unknown system variable, maybe observer has upgrade", K(str_kv), K(ret));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to update sys variable", K(str_kv), K(ret));
        }
      }

      // if last_insert_id has changed, assign version
      // NOTE: do this after update sys variable
      if (OB_SUCC(ret)) {
        if (resp_result.is_last_insert_id_changed_) {
          assign_last_insert_id_version(client_info, server_info);
        }
      }
    }
  }

  return ret;
}

inline int ObProxySessionInfoHandler::handle_necessary_sys_var(ObClientSessionInfo &client_info,
                                                               ObServerSessionInfo &server_info,
                                                               const ObStringKV &str_kv,
                                                               const bool is_auth_request,
                                                               ObRespAnalyzeResult &resp_result)
{
  int ret = OB_SUCCESS;
  UNUSED(resp_result);

  bool need_save = true;
  ObProxySysVarType type = get_sys_var_type(str_kv.key_);
  switch (type) {
    // must get cap, even cluster resource is not avail, or we will die
    case OBPROXY_VAR_CAPABILITY_FLAG:
      ret = handle_capability_flag_var(client_info, server_info, str_kv.value_,
                                       is_auth_request, need_save);
      break;

    case OBPROXY_VAR_ENABLE_TRANSMISSION_CHECKSUM_FLAG:
      ret = handle_enable_transmission_checksum_flag_var(client_info, server_info, str_kv.value_,
                                                     is_auth_request, need_save);
      break;
    default:
      break;
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("fail to process sys var", K(str_kv), K(type), K(ret));
  } else {
    LOG_DEBUG("succ to process sys var", K(str_kv));
  }
  return ret;
}

int ObProxySessionInfoHandler::save_changed_session_info(ObClientSessionInfo &client_info,
                                                         ObServerSessionInfo &server_info,
                                                         const bool is_auth_request,
                                                         const bool need_handle_sysvar,
                                                         OMPKOK &ok_pkt,
                                                         ObRespAnalyzeResult &resp_result,
                                                         const bool is_save_to_common_sys)
{
  int ret = OB_SUCCESS;
  // 1. save server status
  if (is_auth_request) {
    bool is_oracle_mode = 1 == ok_pkt.get_server_status().status_flags_.OB_SERVER_STATUS_RESERVED;
    LOG_DEBUG("will set oracle mode ", K(is_oracle_mode));
    client_info.set_oracle_mode(is_oracle_mode);
  }
  // 2. save db name
  if (ok_pkt.is_schema_changed()) {
    const ObString &db_name = ok_pkt.get_changed_schema();
    if (!db_name.empty()) {
      bool is_string_to_lower_case = client_info.is_oracle_mode() ? false : client_info.need_use_lower_case_names();
      if (OB_FAIL(client_info.set_database_name(db_name))) {
        LOG_WARN("fail to set changed database name", K(db_name), K(ret));
      } else if (OB_FAIL(server_info.set_database_name(db_name, is_string_to_lower_case))) {
        LOG_WARN("fail to set changed database name", K(db_name), K(ret));
      }
    } else {
      resp_result.is_server_db_reset_ = true;
      LOG_DEBUG("db has been reset");
    }
  }

  // 3. save sys var
  resp_result.has_new_sys_var_ = false;
  const ObIArray<ObStringKV> &sys_var = ok_pkt.get_system_vars();
  if (!sys_var.empty()) {
    for (int64_t i = 0; i < sys_var.count() && OB_SUCC(ret); ++i) {
      if (need_handle_sysvar) {
        if (OB_FAIL(handle_sys_var(client_info, server_info, sys_var.at(i),
                                   is_auth_request, resp_result, is_save_to_common_sys))) {
          LOG_WARN("fail to handle sys var", K(sys_var.at(i)), K(is_auth_request), K(ret));
        }
      } else {
        // some necessary sys var like cap, must be handled properly
        if (OB_FAIL(handle_necessary_sys_var(client_info, server_info, sys_var.at(i),
                                             is_auth_request, resp_result))) {
          LOG_WARN("fail to nessary handle sys var", K(sys_var.at(i)), K(is_auth_request), K(ret));
        }
      }
    }
  }

  // 4. save user var
  const ObIArray<ObStringKV> &user_var = ok_pkt.get_user_vars();
  if (!user_var.empty()) {
    for (int64_t i = 0; i < user_var.count() && OB_SUCC(ret); ++i) {
      const ObStringKV &str_kv = user_var.at(i);
      LOG_DEBUG("user variable will be updated", K(str_kv));
      if (OB_FAIL(client_info.replace_user_variable(str_kv.key_, str_kv.value_))) {
        LOG_WARN("fail to replace user variable", K(str_kv), K(ret));
      } else {
        if (PROXY_IDC_NAME_USER_SESSION_VAR == str_kv.key_) {
          const ObString value = trim_quote(str_kv.value_);
          resp_result.has_proxy_idc_name_user_var_ = true;
          client_info.set_idc_name(value);
          LOG_INFO("succ to update user session variable proxy_idc_name",
                   "idc_name", client_info.get_idc_name());
        } else if (PROXY_ROUTE_POLICY_USER_SESSION_VAR == str_kv.key_) {
          const ObString value = trim_quote(str_kv.value_);
          ObProxyRoutePolicyEnum policy = get_proxy_route_policy(value);
          client_info.set_proxy_route_policy(policy);
          LOG_INFO("succ to update user session variable proxy_route_policy",
                   "policy", get_proxy_route_policy_enum_string(policy));
        }
      }
    }
  }

  // 5. assign sys var version
  if (OB_SUCC(ret)) {
    // all server version expect last_insert_id will be set to client version
    ret = assign_session_vars_version(client_info, server_info);
    assign_database_version(client_info, server_info);
  }

  // 6. handle vip for fllower replica in public cloud or handle config enable read only mode
  if (OB_SUCC(ret) && is_auth_request) {
    if (client_info.is_request_follower_user()) {
      ObString ob_read_consistency("ob_read_consistency");
      // 2 means WEAK for ob_read_consistency
      ObString weak("2");
      if (OB_FAIL(client_info.update_sys_variable(ob_read_consistency, weak))) {
        LOG_WARN("replace user variables failed", K(ret));
      } else {
        client_info.set_read_consistency_set_flag(true);
      }
    }

    if (OB_SUCC(ret) && (client_info.is_read_only_user())) {
      ObString tx_read_only("tx_read_only");
      // 1 means true for tx_read_only
      ObString tx_read_only_true("1");
      if (OB_FAIL(client_info.update_sys_variable(tx_read_only, tx_read_only_true))) {
        LOG_WARN("replace user variables failed", K(ret));
      }
    }
  }

  return ret;
}

void ObProxySessionInfoHandler::assign_database_version(
    ObClientSessionInfo &client_info,
    ObServerSessionInfo &server_info)
{
  if (!client_info.get_database_name().empty()) {
    bool is_string_to_lower_case = client_info.is_oracle_mode() ? false : client_info.need_use_lower_case_names();
    server_info.set_database_name(client_info.get_database_name(), is_string_to_lower_case);
  }
  int64_t c_db_version = client_info.get_db_name_version();
  server_info.set_db_name_version(c_db_version);
}

void ObProxySessionInfoHandler::assign_last_insert_id_version(
    ObClientSessionInfo &client_info,
    ObServerSessionInfo &server_info)
{
  int64_t c_lii_version = client_info.get_last_insert_id_version();
  server_info.set_last_insert_id_version(c_lii_version);
  if (client_info.is_session_pool_client_) {
    server_info.field_mgr_.replace_last_insert_id_var(client_info.field_mgr_,
    server_info.is_oceanbase_server());
  }
}

int ObProxySessionInfoHandler::assign_session_vars_version(
    ObClientSessionInfo &client_info,
    ObServerSessionInfo &server_info)
{
  int ret = OB_SUCCESS;
  int64_t c_common_sys_version = client_info.get_common_sys_var_version();
  server_info.set_common_sys_var_version(c_common_sys_version);

  int64_t c_common_hot_sys_version = client_info.get_common_hot_sys_var_version();
  server_info.set_common_hot_sys_var_version(c_common_hot_sys_version);

  int64_t c_mysql_sys_version = client_info.get_mysql_sys_var_version();
  server_info.set_mysql_sys_var_version(c_mysql_sys_version);

  int64_t c_mysql_hot_sys_version = client_info.get_mysql_hot_sys_var_version();
  server_info.set_mysql_hot_sys_var_version(c_mysql_hot_sys_version);

  int64_t c_sys_version = client_info.get_sys_var_version();
  server_info.set_sys_var_version(c_sys_version);

  int64_t c_hot_version = client_info.get_hot_sys_var_version();
  server_info.set_hot_sys_var_version(c_hot_version);

  int64_t c_user_version = client_info.get_user_var_version();
  server_info.set_user_var_version(c_user_version);

  bool is_changed = false;
  if (client_info.is_session_pool_client_) {
    ObSessionVarValHash& client_val_hash = client_info.val_hash_;
    ObSessionVarValHash& server_val_hash = server_info.val_hash_;

    if (server_info.is_oceanbase_server()) {
      is_changed = client_info.is_sys_hot_version_changed();
      if (OB_FAIL(server_info.field_mgr_.replace_all_hot_sys_vars(client_info.field_mgr_))) {
        LOG_WARN("fail to replace_all_hot_sys_vars", K(ret));
      } else if (is_changed) {
        if (OB_FAIL(client_info.field_mgr_.calc_hot_sys_var_hash(client_val_hash.hot_sys_var_hash_))) {
          LOG_WARN("fail to calc_hot_sys_var_hash for client", K(ret));
        } else {
          client_info.hash_version_.hot_sys_var_version_ = c_hot_version;
        }
      }
      if (OB_SUCC(ret)) {
        server_val_hash.hot_sys_var_hash_ = client_val_hash.hot_sys_var_hash_;
        LOG_DEBUG("handle sys_hot_version", K(c_hot_version), K(is_changed), K(ret));
      }
      if (OB_SUCC(ret)) {
        is_changed = client_info.is_sys_cold_version_changed();
        if (is_changed) {
          if (OB_FAIL(client_info.field_mgr_.calc_cold_sys_var_hash(client_val_hash.cold_sys_var_hash_))) {
            LOG_WARN("fail to calc_cold_sys_var_hash", K(ret));
          } else {
            client_info.hash_version_.sys_var_version_ = c_sys_version;
          }
        }
        if (OB_SUCC(ret)) {
          server_val_hash.cold_sys_var_hash_ = client_val_hash.cold_sys_var_hash_;
          LOG_DEBUG("handle sys_cold",  K(c_sys_version), K(is_changed), K(ret));
        }
      }
    } else {
      is_changed = client_info.is_mysql_hot_sys_version_changed();
      if (OB_FAIL(server_info.field_mgr_.replace_all_mysql_hot_sys_vars(client_info.field_mgr_))) {
        LOG_WARN("fail to replace_all_mysql_hot_sys_vars", K(ret));
      } else if (is_changed) {
        if (OB_FAIL(client_info.field_mgr_.calc_mysql_hot_sys_var_hash(client_val_hash.mysql_hot_sys_var_hash_))) {
          LOG_WARN("fail to calc_mysql_hot_sys_var_hash for client", K(ret));
        } else {
          client_info.hash_version_.mysql_hot_sys_var_version_  = c_mysql_hot_sys_version;
        }
      }
      if (OB_SUCC(ret)) {
        server_val_hash.mysql_hot_sys_var_hash_  = client_val_hash.mysql_hot_sys_var_hash_;
        LOG_DEBUG("handle mysql_hot_sys",  K(c_mysql_hot_sys_version), K(is_changed), K(ret));
      }
      if (OB_SUCC(ret)) {
        is_changed = client_info.is_mysql_cold_sys_version_changed();
        if (is_changed) {
          if (OB_FAIL(client_info.field_mgr_.calc_mysql_cold_sys_var_hash(client_val_hash.mysql_cold_sys_var_hash_))) {
            LOG_WARN("fail to calc_mysql_cold_sys_var_hash", K(ret));
          } else {
            client_info.hash_version_.mysql_sys_var_version_ = c_mysql_sys_version;
          }
        }
        if (OB_SUCC(ret)) {
          server_val_hash.mysql_cold_sys_var_hash_ = client_val_hash.mysql_cold_sys_var_hash_;
          LOG_DEBUG("handle mysql_cold_sys",  K(c_mysql_sys_version), K(is_changed), K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      is_changed = client_info.is_common_hot_sys_version_changed();
      if (OB_FAIL(server_info.field_mgr_.replace_all_common_hot_sys_vars(client_info.field_mgr_,
        server_info.is_oceanbase_server()))) {
        LOG_WARN("fail to replace_all_common_hot_sys_vars", K(ret));
      } else if (is_changed){
        if (OB_FAIL(client_info.field_mgr_.calc_common_hot_sys_var_hash(
          client_val_hash.common_hot_sys_var_hash_))) {
          LOG_WARN("fail to calc_common_hot_sys_var_hash for client", K(ret));
        } else {
          client_info.hash_version_.common_hot_sys_var_version_ = c_common_hot_sys_version;
        }
      }
      if (OB_SUCC(ret)) {
        server_val_hash.common_hot_sys_var_hash_ = client_val_hash.common_hot_sys_var_hash_;
        LOG_DEBUG("handle common_hot_sys",  K(c_common_hot_sys_version), K(is_changed), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      is_changed = client_info.is_common_cold_sys_version_changed();
      if (is_changed) {
        if (OB_FAIL(client_info.field_mgr_.calc_common_cold_sys_var_hash(
          client_val_hash.common_cold_sys_var_hash_))) {
          LOG_WARN("fail to calc_common_cold_sys_var_hash", K(ret));
        } else {
          client_info.hash_version_.common_sys_var_version_ = c_common_sys_version;
        }
      }
      if (OB_SUCC(ret)) {
        server_val_hash.common_cold_sys_var_hash_ = client_val_hash.common_cold_sys_var_hash_;
        LOG_DEBUG("handle common_cold_sys",  K(c_common_sys_version), K(is_changed), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      is_changed = client_info.is_user_var_version_changed();
      if (is_changed) {
        if (OB_FAIL(client_info.field_mgr_.calc_user_var_hash(client_val_hash.user_var_hash_))) {
          LOG_WARN("fail to replace_all_user_vars", K(ret));
        } else {
          client_info.hash_version_.user_var_version_ = c_user_version;
        }
      }
      if (OB_SUCC(ret)) {
        server_val_hash.user_var_hash_ = client_val_hash.user_var_hash_;
        LOG_DEBUG("handle user_var",  K(c_user_version), K(is_changed), K(ret));
      }
    }
    LOG_DEBUG("assign_session_vars_version", K(client_val_hash), K(server_val_hash));
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
