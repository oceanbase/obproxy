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

#include "proxy/mysql/ob_mysql_csha2_handler.h"

#include <cstdio>
#include <cstring>

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>

#include "proxy/mysqllib/ob_proxy_session_info.h"
#include "obutils/ob_proxy_csha2_rsa_keys.h"
#include "proxy/mysqllib/ob_mysql_packet_rewriter.h"
#include "packet/ob_mysql_packet_writer.h"
#include "proxy/mysqllib/ob_mysql_request_builder.h"
#include "proxy/mysqllib/ob_proxy_session_info_handler.h"
#include "proxy/mysql/ob_mysql_sm.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "rpc/obmysql/packet/ompk_ok.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::packet;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

// Max RSA cipher body length we support (2048->256, 4096->512). Actual size from RSA_size(rsa).
static const int64_t CSHA2_RSA_MAX_ENCRYPTED_BODY_LEN = 512;
// Max plaintext password length we accept after RSA decrypt.
static const int64_t CSHA2_MAX_PLAINTEXT_PASSWORD_LEN = 256;

static int csha2_load_proxy_public_key(ObString &public_key)
{
  int ret = OB_SUCCESS;
  public_key.reset();
  static ObString cached_public_key;
  static bool is_cached = false;
  if (is_cached) {
    public_key = cached_public_key;
  } else {
    const char *wallet_public_key_path = obutils::ob_proxy_csha2_rsa_get_public_key_path();
    FILE *fp = NULL;
    char *buf = NULL;
    int64_t file_len = 0;
    if (OB_ISNULL(fp = fopen(wallet_public_key_path, "rb"))) {
      ret = OB_IO_ERROR;
      LOG_WDIAG("fail to open proxy rsa public key file (non-TLS caching_sha2_password RSA unavailable if "
                "startup key init failed or path wrong)",
                K(ret), K(wallet_public_key_path), KERRMSGS);
    } else if (0 != fseek(fp, 0, SEEK_END)) {
      ret = OB_IO_ERROR;
      LOG_WDIAG("fail to seek proxy rsa public key file", K(ret), K(wallet_public_key_path), KERRMSGS);
    } else if (OB_UNLIKELY((file_len = ftell(fp)) <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("invalid proxy rsa public key file length", K(ret), K(file_len), K(wallet_public_key_path));
    } else if (0 != fseek(fp, 0, SEEK_SET)) {
      ret = OB_IO_ERROR;
      LOG_WDIAG("fail to rewind proxy rsa public key file", K(ret), K(wallet_public_key_path), KERRMSGS);
    } else if (OB_ISNULL(buf = static_cast<char *>(common::ob_malloc(file_len + 1, ObModIds::OB_PROXY_UTILS)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc buffer for proxy rsa public key", K(ret), K(file_len));
    } else {
      const size_t read_size = fread(buf, 1, file_len, fp);
      if (OB_UNLIKELY(static_cast<int64_t>(read_size) != file_len)) {
        ret = OB_IO_ERROR;
        LOG_WDIAG("fail to read proxy rsa public key file", K(ret), K(read_size), K(file_len), KERRMSGS);
      } else {
        buf[file_len] = '\0';
        cached_public_key.assign_ptr(buf, static_cast<int32_t>(file_len));
        public_key = cached_public_key;
        is_cached = true;
      }
    }
    if (NULL != fp) {
      fclose(fp);
    }
    if (OB_FAIL(ret) && NULL != buf) {
      common::ob_free(buf);
      buf = NULL;
    }
  }
  return ret;
}

static inline common::ObString csha2_effective_proxy_scramble_for_csha2_rsa(ObClientSessionInfo &client_info)
{
  common::ObString s = client_info.get_scramble_string();
  if (s.length() != SCRAMBLE_LENGTH) {
    s = common::ObString::make_string(OB_AUTH_DATA_AB);
  }
  return s;
}

static int csha2_get_auth_target_server_session(ObMysqlTransact::ObTransState &s,
                                                ObMysqlServerSession *&server_session)
{
  int ret = OB_SUCCESS;
  server_session = s.sm_->get_server_session();
  if (OB_ISNULL(server_session) && OB_NOT_NULL(s.sm_->get_client_session())) {
    server_session = s.sm_->get_client_session()->get_last_server_session();
  }
  if (OB_ISNULL(server_session)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_CSHA2_LOG(WDIAG, "server session is null when handling csha2 auth more data", K(ret));
  }
  return ret;
}

static int csha2_load_server_rsa_public_key(const common::ObString &server_rsa_public_key, RSA *&out_rsa)
{
  int ret = OB_SUCCESS;
  out_rsa = NULL;
  RSA *rsa = NULL;
  char *pem_z = NULL;
  const char *pem_ptr = server_rsa_public_key.ptr();
  const int pem_len = static_cast<int>(server_rsa_public_key.length());
  const int64_t pem_alloc_len = static_cast<int64_t>(pem_len) + 1;

  // 1) ObString 未必以 NUL 结尾：拷到固定内存池并显式补 0，供 BIO_new_mem_buf(..., -1) 使用。
  if (OB_ISNULL(pem_z = static_cast<char *>(op_fixed_mem_alloc(pem_alloc_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_CSHA2_LOG(WDIAG, "op_fixed_mem_alloc for PEM NUL-terminated copy failed", K(ret), K(pem_len));
  } else {
    MEMCPY(pem_z, pem_ptr, pem_len);
    pem_z[pem_len] = '\0';

    // 2) 内容里若含内嵌 NUL，strlen 会短于 pem_len；继续解析只会读到截断串，直接判非法。
    const size_t pem_z_strlen = strlen(pem_z);
    if (pem_z_strlen != static_cast<size_t>(pem_len)) {
      ret = OB_INVALID_ARGUMENT;
      PROXY_CSHA2_LOG(WDIAG, "PEM has embedded NUL; refuse parse", K(ret), K(pem_len),
                "strlen_", static_cast<uint64_t>(pem_z_strlen));
    }

    // 3) OpenSSL：先按 RSA PUBLIC KEY，失败再按通用 PUBLIC KEY（SPKI）取 RSA。
    if (OB_SUCC(ret)) {
      ERR_clear_error();
      BIO *bio = BIO_new_mem_buf(pem_z, -1);
      if (OB_ISNULL(bio)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        PROXY_CSHA2_LOG(WDIAG, "BIO_new_mem_buf(pem_z,-1) failed", K(ret));
      } else {
        rsa = PEM_read_bio_RSA_PUBKEY(bio, NULL, NULL, NULL);
        BIO_free(bio);
        if (OB_ISNULL(rsa)) {
          const unsigned long err_rsa_pk = ERR_get_error();
          PROXY_CSHA2_LOG(DEBUG, "PEM_read_bio_RSA_PUBKEY failed, try SPKI PUBKEY", K(err_rsa_pk));
          ERR_clear_error();
          bio = BIO_new_mem_buf(pem_z, -1);
          if (OB_ISNULL(bio)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            PROXY_CSHA2_LOG(WDIAG, "BIO_new_mem_buf(pem_z,-1) retry failed", K(ret));
          } else {
            EVP_PKEY *tmp_pkey = PEM_read_bio_PUBKEY(bio, NULL, NULL, NULL);
            BIO_free(bio);
            if (OB_ISNULL(tmp_pkey)) {
              const unsigned long err_pub = ERR_get_error();
              PROXY_CSHA2_LOG(WDIAG, "PEM_read_bio_PUBKEY failed after RSA_PUBKEY fail",
                        K(err_rsa_pk), K(err_pub));
              ret = OB_INVALID_ARGUMENT;
            } else {
              rsa = EVP_PKEY_get1_RSA(tmp_pkey);
              if (OB_ISNULL(rsa)) {
                const unsigned long err_get1 = ERR_peek_error();
                PROXY_CSHA2_LOG(WDIAG, "EVP_PKEY_get1_RSA returned NULL (expect RSA key)",
                          K(err_get1), "pkey_type", EVP_PKEY_id(tmp_pkey));
                ret = OB_INVALID_ARGUMENT;
              }
              EVP_PKEY_free(tmp_pkey);
            }
          }
        } else {
          PROXY_CSHA2_LOG(DEBUG, "loaded server key via PEM_read_bio_RSA_PUBKEY");
        }
      }
    }
    op_fixed_mem_free(pem_z, pem_alloc_len);
    pem_z = NULL;
  }
  // 4) 成功路径必须带出 RSA*；失败则释放已分配的 RSA，避免泄漏。
  if (OB_SUCC(ret) && OB_ISNULL(rsa)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_CSHA2_LOG(WDIAG, "PEM_read server RSA public key failed (tried RSA_PUBKEY and PUBKEY/SPKI)",
              K(ret), K(ERR_get_error()));
  }
  if (OB_SUCC(ret)) {
    out_rsa = rsa;
  } else if (NULL != rsa) {
    RSA_free(rsa);
    rsa = NULL;
  }
  return ret;
}

static int csha2_fill_server_rsa_wire_packet(ObClientSessionCsha2AuthContext &csha2_ctx,
                                             const unsigned char *encrypted,
                                             const int enc_len)
{
  // 把密文封成发往 server 的 MySQL 包
  int ret = OB_SUCCESS;
  csha2_ctx.auth_more_data_resp_rsa_wire_.reset();
  if (OB_FAIL(csha2_ctx.auth_more_data_resp_rsa_wire_.init(MYSQL_NET_HEADER_LENGTH + enc_len))) {
    PROXY_CSHA2_LOG(WDIAG, "fail to init rsa_wire buffer for encrypted server auth more data", K(ret), K(enc_len));
  } else {
    char header[MYSQL_NET_HEADER_LENGTH];
    int64_t pos = 0;
    if (OB_FAIL(ObMySQLUtil::store_int3(header, MYSQL_PAYLOAD_LENGTH_LENGTH, enc_len, pos))) {
      PROXY_CSHA2_LOG(WDIAG, "fail to store encrypted packet len", K(ret), K(enc_len));
    } else {
      if (csha2_ctx.auth_more_data_resp_raw_.len() >= MYSQL_NET_HEADER_LENGTH) {
        header[3] = csha2_ctx.auth_more_data_resp_raw_.ptr()[3];
      } else if (csha2_ctx.auth_more_data_resp_for_server_.len() >= MYSQL_NET_HEADER_LENGTH) {
        header[3] = csha2_ctx.auth_more_data_resp_for_server_.ptr()[3];
      } else {
        header[3] = 0;
        PROXY_CSHA2_LOG(DEBUG, "encrypted auth more data: inner mysql seq default 0 (no raw/for_server header)");
      }
      if (OB_FAIL(csha2_ctx.auth_more_data_resp_rsa_wire_.write(header, MYSQL_NET_HEADER_LENGTH))) {
        PROXY_CSHA2_LOG(WDIAG, "fail to write encrypted header", K(ret));
      } else if (OB_FAIL(csha2_ctx.auth_more_data_resp_rsa_wire_.write(reinterpret_cast<const char *>(encrypted), enc_len))) {
        PROXY_CSHA2_LOG(WDIAG, "fail to write encrypted body", K(ret), K(enc_len));
      }
    }
  }
  return ret;
}

bool ObMysqlCsha2Handler::is_client_ssl(ObMysqlTransact::ObTransState &s)
{
  bool using_ssl = false;
  if (OB_NOT_NULL(s.sm_) && OB_NOT_NULL(s.sm_->get_client_session())
      && OB_NOT_NULL(s.sm_->get_client_session()->get_netvc())) {
    ObUnixNetVConnection *client_vc =
      static_cast<ObUnixNetVConnection *>(s.sm_->get_client_session()->get_netvc());
    using_ssl = client_vc->using_ssl();
  }
  return using_ssl;
}

bool ObMysqlCsha2Handler::is_server_ssl(ObMysqlServerSession *server_session)
{
  bool using_ssl = false;
  if (OB_NOT_NULL(server_session)) {
    net::ObNetVConnection *vc = server_session->get_netvc();
    if (OB_NOT_NULL(vc)) {
      ObUnixNetVConnection *server_vc = static_cast<ObUnixNetVConnection *>(vc);
      using_ssl = server_vc->using_ssl();
    }
  }
  return using_ssl;
}

bool ObMysqlCsha2Handler::client_leg_allows_forced_csha2_full_auth(ObMysqlTransact::ObTransState &s)
{
  return is_client_ssl(s) || (NULL != obutils::ob_proxy_csha2_rsa_get_private_key());
}

bool ObMysqlCsha2Handler::is_fast_auth_succ_with_ok(ObMysqlTransact::ObTransState &s)
{
  return s.trans_info_.resp_result_.is_fast_auth_succ()
      && s.trans_info_.resp_result_.get_deferred_ok_pkt_len() > 0;
}

bool ObMysqlCsha2Handler::should_force_full_auth_on_login_resp(ObMysqlTransact::ObTransState &s,
                                                               ObClientSessionInfo &client_info)
{
  return OB_NOT_NULL(s.sm_)
      && OB_NOT_NULL(s.sm_->get_client_session())
      && client_info.get_login_req().get_hsr_result().response_.get_auth_plugin_name()
            == OMPKHandshake::AUTH_PLUGIN_CACHING_SHA2_PASSWORD
      && client_leg_allows_forced_csha2_full_auth(s);
}

bool ObMysqlCsha2Handler::should_force_full_auth_on_request_resp(ObMysqlTransact::ObTransState &s,
                                                                 ObClientSessionInfo &client_info,
                                                                 bool &is_change_user_resp)
{
  ObClientSessionCsha2AuthContext &csha2_ctx = client_info.get_csha2_auth_ctx();
  const bool is_auth_switch_resp = (obmysql::OB_MYSQL_COM_AUTH_SWITCH_RESP == s.trans_info_.sql_cmd_
      && s.is_auth_switch_resp_phase()
      && (csha2_ctx.pending_auth_switch_plugin_ == proxy::PROXY_AUTH_PLUGIN_CACHING_SHA2_PASSWORD));
  is_change_user_resp = (obmysql::OB_MYSQL_COM_CHANGE_USER == s.trans_info_.sql_cmd_);
  const bool allow_forced = client_leg_allows_forced_csha2_full_auth(s);
  return (is_auth_switch_resp || is_change_user_resp) && allow_forced;
}

bool ObMysqlCsha2Handler::is_request_public_key_packet(const ObClientSessionInfo &client_info)
{
  const ObClientSessionCsha2AuthContext &csha2_ctx = client_info.get_csha2_auth_ctx();
  return csha2_ctx.auth_more_data_resp_raw_.len() == MYSQL_NET_HEADER_LENGTH + 1
      && NULL != csha2_ctx.auth_more_data_resp_raw_.ptr()
      && static_cast<uint8_t>(csha2_ctx.auth_more_data_resp_raw_.ptr()[MYSQL_NET_HEADER_LENGTH])
          == MYSQL_AUTH_REQUEST_PUBLIC_KEY_TYPE;
}

int ObMysqlCsha2Handler::check_non_tls_auth_more_rsa_decrypt_precondition(
    const ObClientSessionInfo &client_info)
{
  int ret = OB_SUCCESS;
  const ObClientSessionCsha2AuthContext &csha2_ctx = client_info.get_csha2_auth_ctx();
  const int64_t raw_len = csha2_ctx.auth_more_data_resp_raw_.len();
  const int64_t body_len = raw_len - MYSQL_NET_HEADER_LENGTH;
  if (body_len <= 0 || body_len > CSHA2_RSA_MAX_ENCRYPTED_BODY_LEN
      || OB_ISNULL(csha2_ctx.auth_more_data_resp_raw_.ptr())) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_CSHA2_LOG(WDIAG, "csha2 non-TLS auth more data: invalid payload length for RSA password",
              K(ret), K(raw_len), K(body_len));
  } else {
    RSA *rsa = obutils::ob_proxy_csha2_rsa_get_private_key();
    if (OB_ISNULL(rsa)) {
      if (body_len >= 128 && body_len <= CSHA2_RSA_MAX_ENCRYPTED_BODY_LEN) {
        ret = OB_NOT_INIT;
        PROXY_CSHA2_LOG(WDIAG, "obproxy RSA private key unavailable but client sent RSA-sized auth payload; "
                  "non-TLS caching_sha2_password RSA not available (startup key init likely failed)",
                  K(ret), K(raw_len), K(body_len));
      } else {
        ret = OB_ERR_UNEXPECTED;
        PROXY_CSHA2_LOG(WDIAG, "csha2 non-TLS auth more data: payload is not RSA ciphertext block for proxy key "
                  "(ODP RSA private key not loaded or length mismatch)",
                  K(ret), K(raw_len), K(body_len));
      }
    } else {
      const int rsa_size = RSA_size(rsa);
      if (body_len != static_cast<int64_t>(rsa_size)) {
        ret = OB_ERR_UNEXPECTED;
        PROXY_CSHA2_LOG(WDIAG, "csha2 non-TLS auth more data: RSA ciphertext length mismatch proxy key",
                  K(ret), K(raw_len), K(body_len), K(rsa_size));
      }
    }
  }
  return ret;
}

void ObMysqlCsha2Handler::record_pending_auth_switch_plugin(ObClientSessionInfo &client_info,
                                                            const common::ObString &auth_plugin_name)
{
  ObClientSessionCsha2AuthContext &csha2_ctx = client_info.get_csha2_auth_ctx();
  if (auth_plugin_name == OMPKHandshake::AUTH_PLUGIN_CACHING_SHA2_PASSWORD) {
    csha2_ctx.pending_auth_switch_plugin_ = proxy::PROXY_AUTH_PLUGIN_CACHING_SHA2_PASSWORD;
  } else if (auth_plugin_name == OMPKHandshake::AUTH_PLUGIN_MYSQL_NATIVE_PASSWORD) {
    csha2_ctx.pending_auth_switch_plugin_ = proxy::PROXY_AUTH_PLUGIN_MYSQL_NATIVE_PASSWORD;
  } else {
    csha2_ctx.pending_auth_switch_plugin_ = proxy::PROXY_AUTH_PLUGIN_UNKNOWN;
  }
}

int ObMysqlCsha2Handler::save_change_user_req_for_auth_switch(ObMysqlTransact::ObTransState &s,
                                                              const char *trace_desc)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(s.sm_) && OB_NOT_NULL(s.sm_->client_session_)
      && s.trans_info_.sql_cmd_ == OB_MYSQL_COM_CHANGE_USER) {
    ObClientSessionInfo &cs_info = s.sm_->client_session_->get_session_info();
    ObClientSessionCsha2AuthContext &csha2_ctx = cs_info.get_csha2_auth_ctx();
    const ObString req = s.trans_info_.client_request_.get_req_pkt();
    if (req.empty()) {
      // Session pool reset: build_reset_session_request consumed the client buffer, so there is no
      // client-side COM_CHANGE_USER copy. ObMysqlTransact::build_server_request saves the wire packet.
      if (!csha2_ctx.change_user_req_.empty()) {
        PROXY_CSHA2_LOG(DEBUG, "save_change_user_req_for_auth_switch: keep pre-saved internal packet",
                 K(trace_desc), "saved_len", csha2_ctx.change_user_req_.len());
      }
    } else if (OB_FAIL(csha2_ctx.save_change_user_req(req))) {
      LOG_WDIAG("fail to save change user request in auth switch flow", K(ret), K(trace_desc));
    }
  }
  return ret;
}

int ObMysqlCsha2Handler::save_deferred_ok(event::ObIOBufferReader *buf_reader,
                                          ObClientSessionInfo &client_info,
                                          const int64_t ok_offset,
                                          const int64_t ok_pkt_len)
{
  int ret = OB_SUCCESS;
  obutils::ObVariableLenBuffer<proxy::OB_DEFERRED_LOGIN_OK_LEN> &deferred_ok =
    client_info.get_csha2_auth_ctx().deferred_login_ok_resp_;
  deferred_ok.reset();
  if (OB_FAIL(deferred_ok.init(ok_pkt_len))) {
    LOG_WDIAG("fail to init deferred ok buffer", K(ret), K(ok_pkt_len));
  } else {
    (void) buf_reader->copy(deferred_ok.pos(), ok_pkt_len, ok_offset);
    (void) deferred_ok.consume(ok_pkt_len);
  }
  return ret;
}

int ObMysqlCsha2Handler::build_deferred_ok_for_mysql_client(ObMysqlTransact::ObTransState &s,
                                                            ObClientSessionInfo &client_info,
                                                            const char *ok_ptr,
                                                            const int64_t ok_len,
                                                            const uint8_t seq_for_client,
                                                            char *&out_buf,
                                                            int64_t &out_len)
{
  // 将「暂存的 Observer OK 包」按客户端 capability 重写后编码；任一步失败则返回错误，
  // 由调用方 reply_err_to_client_and_break，不把未重写的 Observer OK 发给客户端。
  int ret = OB_SUCCESS;
  out_buf = NULL;
  out_len = 0;
  if (OB_ISNULL(ok_ptr) || ok_len < MYSQL_NET_HEADER_LENGTH || OB_ISNULL(s.sm_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    // 1) 取 server session（当前或 last），用于 server_cap / server_info。
    ObMysqlServerSession *server_session = s.sm_->get_server_session();
    if (OB_ISNULL(server_session) && OB_NOT_NULL(s.sm_->get_client_session())) {
      server_session = s.sm_->get_client_session()->get_last_server_session();
    }
    const uint32_t payload_u = uint3korr(ok_ptr);
    const int64_t payload_len = static_cast<int64_t>(payload_u);
    if (OB_ISNULL(server_session)) {
      ret = OB_ERR_UNEXPECTED;
      PROXY_CSHA2_LOG(WDIAG, "deferred OK rewrite skipped: no server session", K(ret));
    } else if (MYSQL_NET_HEADER_LENGTH + payload_len != ok_len || payload_len < 1) {
      ret = OB_ERR_UNEXPECTED;
      PROXY_CSHA2_LOG(WDIAG, "deferred OK rewrite skipped: header/body length mismatch",
               K(ok_len), K(payload_len), K(ret));
    } else {
      // 2) 按 Observer 侧 capability 解码 OK payload。
      ObServerSessionInfo &server_info = server_session->get_session_info();
      const ObMySQLCapabilityFlags server_cap = server_info.get_compatible_capability_flags();
      OMPKOK src_ok;
      src_ok.set_capability(server_cap);
      src_ok.set_content(ok_ptr + MYSQL_NET_HEADER_LENGTH, static_cast<uint32_t>(payload_len));
      src_ok.set_seq(static_cast<uint8_t>(ok_ptr[3]));
      ret = src_ok.decode();
      if (OB_FAIL(ret)) {
        PROXY_CSHA2_LOG(WDIAG, "deferred OK rewrite skipped: decode failed", K(ret));
      } else {
        // 3) 从 OK 里抽出 session 状态（含 sysvar 等），写回 client_info。
        const bool need_handle_sysvar =
            (OB_NOT_NULL(s.sm_->sm_cluster_resource_) && s.sm_->sm_cluster_resource_->is_avail());
        if (OB_FAIL(ObProxySessionInfoHandler::save_changed_session_info(
                client_info,
                server_info,
                true,
                need_handle_sysvar,
                src_ok,
                s.trans_info_.resp_result_,
                s.trace_log_,
                false))) {
          PROXY_CSHA2_LOG(WDIAG, "deferred OK rewrite skipped: save_changed_session_info failed", K(ret));
        } else {
          // 4) 按客户端握手时的 orig_cap 重写 OK（登录路径），再设 client 方向 seq 并编码整包。
          OMPKOK des_ok;
          char cap_buf[OB_MAX_UINT64_BUF_LEN];
          const ObMySQLCapabilityFlags &orig_cap = client_info.get_orig_capability_flags();
          ret = ObMysqlPacketRewriter::rewrite_ok_packet(
              src_ok,
              orig_cap,
              des_ok,
              client_info,
              cap_buf,
              OB_MAX_UINT64_BUF_LEN,
              true /* is_auth_request */);
          if (OB_FAIL(ret)) {
            PROXY_CSHA2_LOG(WDIAG, "deferred OK rewrite skipped: rewrite_ok_packet failed", K(ret));
          } else {
            des_ok.set_seq(seq_for_client);
            const int64_t enc_need = des_ok.get_serialize_size() + MYSQL_NET_HEADER_LENGTH;
            char *enc_buf = static_cast<char *>(op_fixed_mem_alloc(enc_need));
            if (OB_ISNULL(enc_buf)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              PROXY_CSHA2_LOG(WDIAG, "deferred OK rewrite skipped: alloc encode buf failed",
                       K(enc_need), K(ret));
            } else {
              int64_t tmp_len = enc_need;
              int64_t pos = 0;
              ret = ObMySQLPacket::encode_packet(enc_buf, tmp_len, pos, des_ok);
              if (OB_FAIL(ret)) {
                PROXY_CSHA2_LOG(WDIAG, "deferred OK rewrite skipped: encode_packet failed", K(ret));
                op_fixed_mem_free(enc_buf, enc_need);
                enc_buf = NULL;
              } else {
                out_buf = enc_buf;
                out_len = pos;
                ret = OB_SUCCESS;
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObMysqlCsha2Handler::decrypt_rsa_password_and_normalize(ObClientSessionInfo &client_info)
{
  // 使用odp rsa私钥进行解密
  int ret = OB_SUCCESS;
  ObClientSessionCsha2AuthContext &csha2_ctx = client_info.get_csha2_auth_ctx();
  RSA *rsa = obutils::ob_proxy_csha2_rsa_get_private_key();
  const int64_t raw_len = csha2_ctx.auth_more_data_resp_raw_.len();
  const int64_t body_len = raw_len - MYSQL_NET_HEADER_LENGTH;
  if (OB_ISNULL(rsa)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("decrypt_rsa_password_and_normalize called without proxy RSA private key", K(ret));
  } else {
    const int rsa_size = RSA_size(rsa);
    if (body_len != static_cast<int64_t>(rsa_size) || body_len <= 0
        || body_len > CSHA2_RSA_MAX_ENCRYPTED_BODY_LEN || NULL == csha2_ctx.auth_more_data_resp_raw_.ptr()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("decrypt_rsa_password_and_normalize: raw packet length mismatch RSA key",
                K(ret), K(raw_len), K(body_len), K(rsa_size));
    } else {
      const char *body = csha2_ctx.auth_more_data_resp_raw_.ptr() + MYSQL_NET_HEADER_LENGTH;
      const common::ObString scramble = csha2_effective_proxy_scramble_for_csha2_rsa(client_info);
      if (OB_UNLIKELY(scramble.length() != SCRAMBLE_LENGTH)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WDIAG("scramble length not 20, cannot decrypt RSA auth more data", K(ret), K(scramble.length()));
      } else {
        unsigned char decrypted[CSHA2_RSA_MAX_ENCRYPTED_BODY_LEN];
        bool used_pkcs1_v15 = false;
        ERR_clear_error();
        int dec_len = RSA_private_decrypt(rsa_size,
                                          reinterpret_cast<const unsigned char *>(body),
                                          decrypted,
                                          rsa,
                                          RSA_PKCS1_OAEP_PADDING);
        if (dec_len <= 0) {
          const unsigned long err_oaep = ERR_get_error();
          PROXY_CSHA2_LOG(DEBUG, "RSA_private_decrypt OAEP failed, try PKCS#1 v1.5", K(err_oaep));
          ERR_clear_error();
          dec_len = RSA_private_decrypt(rsa_size,
                                        reinterpret_cast<const unsigned char *>(body),
                                        decrypted,
                                        rsa,
                                        RSA_PKCS1_PADDING);
          if (dec_len > 0) {
            used_pkcs1_v15 = true;
          }
        }
        if (dec_len <= 0) {
          ret = OB_DECRYPT_FAILED;
          LOG_WDIAG("RSA_private_decrypt failed after OAEP and PKCS#1 v1.5", K(ret), K(ERR_get_error()));
        } else if (dec_len > CSHA2_MAX_PLAINTEXT_PASSWORD_LEN) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WDIAG("decrypted password length exceeds sanity limit", K(ret), K(dec_len),
                    K(CSHA2_MAX_PLAINTEXT_PASSWORD_LEN));
        } else {
          char plaintext[CSHA2_RSA_MAX_ENCRYPTED_BODY_LEN];
          const int64_t xor_len = (dec_len < SCRAMBLE_LENGTH) ? dec_len : SCRAMBLE_LENGTH;
          for (int64_t i = 0; i < xor_len; ++i) {
            plaintext[i] = decrypted[i] ^ static_cast<unsigned char>(scramble.ptr()[i]);
          }
          for (int64_t i = xor_len; i < dec_len; ++i) {
            plaintext[i] = decrypted[i];
          }
          const int64_t plaintext_len = dec_len;
          csha2_ctx.auth_more_data_resp_rsa_wire_.reset();
          csha2_ctx.auth_more_data_resp_for_server_.reset();
          if (OB_FAIL(csha2_ctx.auth_more_data_resp_for_server_.init(MYSQL_NET_HEADER_LENGTH + plaintext_len))) {
            LOG_WDIAG("fail to init normalized auth more data resp for plaintext", K(ret), K(plaintext_len));
          } else {
            char header[MYSQL_NET_HEADER_LENGTH];
            int64_t pos = 0;
            if (OB_FAIL(ObMySQLUtil::store_int3(header, MYSQL_PAYLOAD_LENGTH_LENGTH,
                                                static_cast<int32_t>(plaintext_len), pos))) {
              LOG_WDIAG("fail to store normalized packet len", K(ret), K(plaintext_len));
            } else {
              header[3] = csha2_ctx.auth_more_data_resp_raw_.ptr()[3];
              if (OB_FAIL(csha2_ctx.auth_more_data_resp_for_server_.write(header, MYSQL_NET_HEADER_LENGTH))) {
                LOG_WDIAG("fail to write normalized header", K(ret));
              } else if (OB_FAIL(csha2_ctx.auth_more_data_resp_for_server_.write(plaintext, plaintext_len))) {
                LOG_WDIAG("fail to write normalized plaintext", K(ret), K(plaintext_len));
              } else {
                PROXY_CSHA2_LOG(DEBUG, "RSA decrypt auth more data ok", K(plaintext_len), K(used_pkcs1_v15));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObMysqlCsha2Handler::copy_tls_client_auth_more_raw_to_for_server(ObClientSessionInfo &client_info)
{
  int ret = OB_SUCCESS;
  ObClientSessionCsha2AuthContext &csha2_ctx = client_info.get_csha2_auth_ctx();
  const int64_t raw_len = csha2_ctx.auth_more_data_resp_raw_.len();
  const int64_t body_len = raw_len - MYSQL_NET_HEADER_LENGTH;
  if (OB_UNLIKELY(body_len <= 0 || body_len > CSHA2_MAX_PLAINTEXT_PASSWORD_LEN)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_CSHA2_LOG(WDIAG, "invalid TLS client auth more data length for server forward",
              K(ret), K(raw_len), K(body_len));
  } else {
    csha2_ctx.auth_more_data_resp_rsa_wire_.reset();
    csha2_ctx.auth_more_data_resp_for_server_.reset();
    if (OB_FAIL(csha2_ctx.auth_more_data_resp_for_server_.init(raw_len))) {
      PROXY_CSHA2_LOG(WDIAG, "fail to init for_server from TLS client auth more data", K(ret), K(raw_len));
    } else if (OB_FAIL(csha2_ctx.auth_more_data_resp_for_server_.write(csha2_ctx.auth_more_data_resp_raw_.ptr(),
                                                                       raw_len))) {
      PROXY_CSHA2_LOG(WDIAG, "fail to write TLS client auth more into for_server", K(ret), K(raw_len));
    } else {
      PROXY_CSHA2_LOG(DEBUG, "copied TLS client auth more data to for_server for ODP-server leg",
                K(raw_len), K(body_len));
    }
  }
  return ret;
}

int ObMysqlCsha2Handler::encrypt_password_with_server_public_key(ObClientSessionInfo &client_info,
                                                                 ObMysqlServerSession *server_session,
                                                                 const common::ObString &server_rsa_public_key)
{
  int ret = OB_SUCCESS;
  RSA *rsa = NULL;
  ObClientSessionCsha2AuthContext &csha2_ctx = client_info.get_csha2_auth_ctx();

  // 1) 目标：把 for_server_ 里规范化后的明文密码，用 Observer 下发的 PEM 公钥做 RSA-OAEP，
  //    结果写入 auth_more_data_resp_rsa_wire_，供后续发往 server 的 Auth More Data。
  if (OB_ISNULL(server_session)) {
    ret = OB_INVALID_ARGUMENT;
    PROXY_CSHA2_LOG(WDIAG, "server_session is null", K(ret));
  } else {
    // 2) for_server_ = MySQL 包头 + 密码体；校验体长与上限（与解密/规范化阶段一致）。
    const int64_t norm_len = csha2_ctx.auth_more_data_resp_for_server_.len();
    const int64_t body_len = norm_len - MYSQL_NET_HEADER_LENGTH;
    if (body_len <= 0 || body_len > CSHA2_MAX_PLAINTEXT_PASSWORD_LEN) {
      ret = OB_INVALID_ARGUMENT;
      PROXY_CSHA2_LOG(WDIAG, "invalid normalized plaintext length for server RSA encrypt",
                K(ret), K(norm_len), K(body_len));
    } else if (server_rsa_public_key.length() <= 0) {
      ret = OB_ENTRY_NOT_EXIST;
      PROXY_CSHA2_LOG(WDIAG, "no server RSA public key in current response", K(ret));
    } else {
      const char *plaintext = csha2_ctx.auth_more_data_resp_for_server_.ptr() + MYSQL_NET_HEADER_LENGTH;
      // 3) 与 Observer 校验侧对齐：前 min(密码长, 20) 字节与 proxy scramble XOR，其余原样（MySQL csha2 约定）。
      const common::ObString xor_scramble = csha2_effective_proxy_scramble_for_csha2_rsa(client_info);
      if (OB_UNLIKELY(xor_scramble.length() != SCRAMBLE_LENGTH)) {
        ret = OB_INVALID_ARGUMENT;
        PROXY_CSHA2_LOG(WDIAG, "proxy scramble length not 20, cannot RSA encrypt for server",
                  K(ret), K(xor_scramble.length()));
      } else {
        // 4) PEM -> RSA*，并检查模长，密文长度应等于 RSA_size。
        if (OB_FAIL(csha2_load_server_rsa_public_key(server_rsa_public_key, rsa))) {
          // ret / logs from csha2_load_server_rsa_public_key
        } else {
          const int rsa_size = RSA_size(rsa);
          if (rsa_size <= 0 || rsa_size > CSHA2_RSA_MAX_ENCRYPTED_BODY_LEN) {
            RSA_free(rsa);
            rsa = NULL;
            ret = OB_ERR_UNEXPECTED;
            PROXY_CSHA2_LOG(WDIAG, "invalid RSA_size for server key", K(ret), K(rsa_size));
          } else {
            unsigned char to_encrypt[CSHA2_RSA_MAX_ENCRYPTED_BODY_LEN];
            const int64_t xor_len = (body_len < SCRAMBLE_LENGTH) ? body_len : SCRAMBLE_LENGTH;
            for (int64_t i = 0; i < xor_len; ++i) {
              to_encrypt[i] = static_cast<unsigned char>(plaintext[i])
                  ^ static_cast<unsigned char>(xor_scramble.ptr()[i]);
            }
            for (int64_t i = xor_len; i < body_len; ++i) {
              to_encrypt[i] = static_cast<unsigned char>(plaintext[i]);
            }

            // 5) RSA_public_encrypt(..., RSA_PKCS1_OAEP_PADDING)，与 server 端解密方式配对。
            unsigned char encrypted[CSHA2_RSA_MAX_ENCRYPTED_BODY_LEN];
            ERR_clear_error();
            const int enc_len = RSA_public_encrypt(static_cast<int>(body_len),
                                                   to_encrypt,
                                                   encrypted,
                                                   rsa,
                                                   RSA_PKCS1_OAEP_PADDING);
            RSA_free(rsa);
            rsa = NULL;
            if (enc_len != rsa_size) {
              ret = OB_DECRYPT_FAILED;
              PROXY_CSHA2_LOG(WDIAG, "RSA_public_encrypt failed", K(ret), K(enc_len), K(rsa_size),
                        K(ERR_get_error()));
            } else if (OB_FAIL(csha2_fill_server_rsa_wire_packet(csha2_ctx, encrypted, enc_len))) {
              // ret set by OB_FAIL
            } else {
              PROXY_CSHA2_LOG(DEBUG, "RSA encrypt password with server public key ok", K(enc_len));
            }
          }
        }
      }
    }
  }
  // 6) 异常路径若仍持有 rsa，统一释放。
  if (NULL != rsa) {
    RSA_free(rsa);
    rsa = NULL;
  }
  return ret;
}

int ObMysqlCsha2Handler::send_internal_public_key(ObMysqlTransact::ObTransState &s,
                                                  const uint8_t client_auth_more_data_resp_pkt_seq)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(s.sm_) || OB_ISNULL(s.sm_->get_client_session()) || OB_ISNULL(s.internal_buffer_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("sm/client session/internal buffer is null", K(ret), KP(s.sm_), KP(s.internal_buffer_));
  } else {
    ObString public_key;
    if (OB_FAIL(csha2_load_proxy_public_key(public_key))) {
      LOG_WDIAG("fail to load proxy rsa public key (client requested 0x02; connection will abort)",
                K(ret));
    } else if (public_key.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("proxy public key is empty, cannot reply RSA public key request", K(ret));
    } else if (OB_FAIL(ObMysqlRequestBuilder::build_auth_more_data_public_key_packet(
                   s.sm_->get_client_session(), s.sm_->get_client_session_protocol(),
                   *s.internal_buffer_, public_key,
                   static_cast<uint8_t>(client_auth_more_data_resp_pkt_seq + 1)))) {
      LOG_WDIAG("fail to build internal auth more data public key packet", K(ret),
                K(client_auth_more_data_resp_pkt_seq));
    } else {
      PROXY_CSHA2_LOG(DEBUG, "client-proxy seq: send internal RSA public key",
                "cs_id", s.sm_->get_client_session()->get_cs_id(),
                "client_auth_more_data_resp_pkt_seq", client_auth_more_data_resp_pkt_seq,
                "public_key_pkt_seq", static_cast<uint8_t>(client_auth_more_data_resp_pkt_seq + 1),
                "public_key_len", public_key.length());
    }
  }
  return ret;
}

int ObMysqlCsha2Handler::send_internal_full_auth_required(ObMysqlTransact::ObTransState &s,
                                                          const uint8_t base_req_pkt_seq,
                                                          const char *trace_stage,
                                                          const char *base_seq_name)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(s.alloc_internal_buffer(MYSQL_BUFFER_SIZE))) {
    LOG_WDIAG("fail to alloc internal buffer for auth more data req", K(ret));
  } else if (OB_ISNULL(s.sm_) || OB_ISNULL(s.sm_->get_client_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("sm/client session is null", K(ret));
  } else {
    const uint8_t seq = static_cast<uint8_t>(base_req_pkt_seq + 1);
    char pkt[MYSQL_NET_HEADER_LENGTH + 2];
    int64_t pos = 0;
    if (OB_FAIL(ObMySQLUtil::store_int3(pkt, 3, 2, pos))) {
      LOG_WDIAG("fail to store mysql packet len", K(ret), K(pos));
    } else {
      pkt[3] = static_cast<char>(seq);
      pkt[4] = static_cast<char>(MYSQL_AUTH_EXTRA_DATA_PACKET_TYPE);
      pkt[5] = static_cast<char>(MYSQL_FULL_AUTH_REQUIRED_TYPE);
      char hex_buf[32];
      int64_t hex_pos = 0;
      (void) hex_print(pkt, 6, hex_buf, sizeof(hex_buf), hex_pos);
      hex_buf[hex_pos < static_cast<int64_t>(sizeof(hex_buf)) ? hex_pos : sizeof(hex_buf) - 1] = '\0';
      PROXY_CSHA2_LOG(DEBUG, "client-proxy seq: send internal AuthMoreData",
                "stage", trace_stage,
                "cs_id", s.sm_->get_client_session()->get_cs_id(),
                base_seq_name, base_req_pkt_seq,
                "auth_more_data_pkt_seq", seq,
                "raw_pkt_hex", hex_buf);
      ObString pkt_str(sizeof(pkt), pkt);
      ObProxyProtocol proto = s.sm_->get_client_session_protocol();
      if (OB_FAIL(ObProxyPacketWriter::write_raw_packet(*s.internal_buffer_,
                                                        *s.sm_->get_client_session(),
                                                        proto, pkt_str))) {
        LOG_WDIAG("fail to write auth more data req to internal buffer", K(ret));
      } else {
        // 发送内部 full auth required 后，必须把当前状态置为 CMD_COMPLETE，
        // 这样 tunnel 完成后 SM 才会继续等待客户端下一帧 AuthMoreDataResponse。
        s.current_.state_ = ObMysqlTransact::CMD_COMPLETE;
        s.next_action_ = ObMysqlTransact::SM_ACTION_INTERNAL_NOOP;
      }
    }
  }
  return ret;
}

int ObMysqlCsha2Handler::send_internal_full_auth_required_after_login(ObMysqlTransact::ObTransState &s,
                                                                      ObClientSessionInfo &client_info)
{
  PROXY_CSHA2_LOG(DEBUG, "send internal AuthMoreData(full auth required) to client after login",
            "cs_id", s.sm_->get_client_session()->get_cs_id(),
            "auth_plugin", client_info.get_login_req().get_hsr_result().response_.get_auth_plugin_name());
  // 登录阶段没有 client_request_，这里必须使用 login_req 的序号来构造下一帧内部包。
  const uint8_t client_login_pkt_seq = static_cast<uint8_t>(client_info.get_login_req().get_packet_meta().pkt_seq_);
  return send_internal_full_auth_required(s, client_login_pkt_seq,
                                          "after_login", "client_login_pkt_seq");
}

int ObMysqlCsha2Handler::send_internal_full_auth_required_after_request(ObMysqlTransact::ObTransState &s,
                                                                        ObClientSessionInfo &client_info)
{
  ObClientSessionCsha2AuthContext &csha2_ctx = client_info.get_csha2_auth_ctx();
  PROXY_CSHA2_LOG(DEBUG, "send internal AuthMoreData(full auth required) to client after request response",
            "cs_id", s.sm_->get_client_session()->get_cs_id(),
            "sql_cmd", s.trans_info_.sql_cmd_,
            "request_phase", s.request_phase_,
            "pending_auth_switch_plugin", csha2_ctx.pending_auth_switch_plugin_);
  const uint8_t client_req_pkt_seq = static_cast<uint8_t>(s.trans_info_.client_request_.get_packet_meta().pkt_seq_);
  return send_internal_full_auth_required(s, client_req_pkt_seq,
                                          "after_request_response", "client_req_pkt_seq");
}

void ObMysqlCsha2Handler::reply_err_to_client_and_break(ObMysqlTransact::ObTransState &s, const int fail_ret)
{
  int enc_ret = OB_SUCCESS;
  if (OB_IO_ERROR == fail_ret) {
    s.mysql_errcode_ = OB_IO_ERROR;
    s.mysql_errmsg_ =
        "OBProxy: RSA public key missing or unreadable (.conf/rsa_public.pem relative to process cwd). "
        "Non-TLS caching_sha2_password requires proxy RSA keys.";
  } else if (OB_NOT_INIT == fail_ret) {
    s.mysql_errcode_ = OB_NOT_INIT;
    s.mysql_errmsg_ =
        "OBProxy: RSA private key unavailable. Non-TLS caching_sha2_password cannot complete on this proxy.";
  } else {
    s.mysql_errcode_ = (OB_SUCCESS != fail_ret ? fail_ret : OB_ERR_UNEXPECTED);
    s.mysql_errmsg_ = ob_strerror(s.mysql_errcode_);
  }
  enc_ret = ObMysqlTransact::encode_error_message(s);
  if (OB_SUCCESS != enc_ret) {
    PROXY_CSHA2_LOG(WDIAG, "csha2: encode MySQL ERR packet failed, client may see ERROR 2013 (lost connection)",
              K(enc_ret), K(fail_ret));
  }
  s.inner_errcode_ = (OB_SUCCESS == enc_ret) ? fail_ret : enc_ret;
  s.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
  ObMysqlTransact::handle_server_connection_break(s);
}

int ObMysqlCsha2Handler::apply_forward_auth_more_to_server_transact_state(ObMysqlTransact::ObTransState &s)
{
  int ret = OB_SUCCESS;
  ObMysqlServerSession *server_session = NULL;
  if (OB_ISNULL(s.sm_) || OB_ISNULL(s.sm_->get_client_session())) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_CSHA2_LOG(WDIAG, "csha2 forward: sm/client session is null", K(ret));
  } else if (OB_FAIL(csha2_get_auth_target_server_session(s, server_session))) {
    PROXY_CSHA2_LOG(WDIAG, "csha2 forward: fail to get auth target server session", K(ret));
  } else {
    ObClientSessionCsha2AuthContext &csha2_ctx =
        s.sm_->get_client_session()->get_session_info().get_csha2_auth_ctx();
    // 这一轮已经进入 ODP->observer 方向，不应该继续沿用上一轮
    // “内部回公钥”留下的 CMD_COMPLETE 状态，否则后续容易误判成继续给客户端回包。
    s.current_.state_ = ObMysqlTransact::CONNECTION_ALIVE;
    s.pl_lookup_state_ = ObMysqlTransact::USE_LAST_SERVER_SESSION;
    if (is_server_ssl(server_session)) {
      csha2_ctx.auth_more_data_resp_rsa_wire_.reset();
      s.current_.send_action_ = ObMysqlTransact::SERVER_SEND_AUTH_MORE_DATA;
    } else {
      s.current_.send_action_ = ObMysqlTransact::SERVER_SEND_AUTH_MORE_DATA_REQUEST_PUBLIC_KEY;
    }
  }
  return ret;
}

int ObMysqlCsha2Handler::process_client_csha2_auth_more_resp(ObMysqlTransact::ObTransState &s,
                                                             ObMysqlCsha2RequestOutcome &outcome)
{
  int ret = OB_SUCCESS;
  outcome = ObMysqlCsha2RequestOutcome::INVALID;
  if (OB_ISNULL(s.sm_) || OB_ISNULL(s.sm_->client_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("sm/client session is null", K(ret));
  } else {
    ObClientSessionInfo &client_info = s.sm_->client_session_->get_session_info();
    ObClientSessionCsha2AuthContext &csha2_ctx = client_info.get_csha2_auth_ctx();

    // csha2下要么ssl要么rsa，否则报错
    if (!client_leg_allows_forced_csha2_full_auth(s)) {
      ret = OB_NOT_INIT;
      PROXY_CSHA2_LOG(WDIAG, "csha2: client leg disallows auth more data handling (need TLS or proxy RSA keys)",
                "cs_id", s.sm_->get_client_session()->get_cs_id(),
                K(s.request_phase_));
    } else {
      ObMysqlClientSession *client_session = s.sm_->get_client_session();
      ObIOBufferReader *client_buf_reader = s.sm_->get_client_buffer_reader();
      // Case1/Case2 公共：校验 client 读侧、consume 整包；0x02 公钥请求统一回 ODP 公钥
      if (OB_ISNULL(client_session) || OB_ISNULL(client_buf_reader)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("client session/buffer reader is null", K(ret));
      } else if (OB_FAIL(client_buf_reader->consume_all())) {
        LOG_WDIAG("fail to consume client auth more data resp", K(ret));
      } else if (is_request_public_key_packet(client_info)) {
        // a. 0x02公钥请求 -> 返回公钥，等待客户端下一轮回包（Case1/Case2 共用）
        if (OB_FAIL(reply_proxy_rsa_public_key_to_client(s))) {
          LOG_WDIAG("fail to reply proxy RSA public key to client after 0x02", K(ret));
        } else {
          outcome = ObMysqlCsha2RequestOutcome::REPLIED_PUBLIC_KEY;
        }
      } else if (csha2_ctx.need_force_auth_more_data_) {
        // Case 1：强制 full auth 后客户端回密码包 -> 回写 deferred login OK
        //    b. rsa密文 -> 解密并规范化，再返回deferred ok
        //    c. ssl明文密码 -> 规范化，再返回deferred ok
        if (OB_FAIL(reply_deferred_ok_for_need_force(s, client_info))) {
          LOG_WDIAG("fail to reply deferred login OK for need force", K(ret));
        } else {
          outcome = ObMysqlCsha2RequestOutcome::REPLIED_DEFERRED_OK;
        }
      } else {
        // Case 2：密码转发 server（调用方已 consume 且非 0x02）
        //    b. rsa密文 -> 解密后转发给server
        //    c. ssl明文密码 -> 转发给server
        if (OB_FAIL(forward_auth_more_password_to_server(s, client_info))) {
          LOG_WDIAG("fail to forward auth more password to server", K(ret));
        } else {
          outcome = ObMysqlCsha2RequestOutcome::FORWARDED_TO_SERVER;
        }
      }
    }
  }
  return ret;
}

int ObMysqlCsha2Handler::reply_proxy_rsa_public_key_to_client(ObMysqlTransact::ObTransState &s)
{
  int ret = OB_SUCCESS;
  const uint8_t client_auth_more_data_resp_pkt_seq =
      static_cast<uint8_t>(s.trans_info_.client_request_.get_packet_meta().pkt_seq_);
  if (OB_FAIL(s.alloc_internal_buffer(MYSQL_BUFFER_SIZE))) {
    LOG_WDIAG("fail to alloc internal buffer for RSA public key", K(ret));
  } else if (OB_FAIL(send_internal_public_key(s, client_auth_more_data_resp_pkt_seq))) {
    LOG_WDIAG("fail to send internal RSA public key", K(ret));
  } else {
    // 与 forward 路径一致：回公钥后不推进 request_phase_，等客户端下一包密码
    s.csha2_skip_phase_update_ = true;
    ObClientSessionCsha2AuthContext &csha2_ctx =
        s.sm_->get_client_session()->get_session_info().get_csha2_auth_ctx();
    PROXY_CSHA2_LOG(DEBUG, "replied proxy RSA public key to client after 0x02",
              K(s.sm_->get_client_session()->get_cs_id()),
              K(csha2_ctx.need_force_auth_more_data_));
  }
  return ret;
}

int ObMysqlCsha2Handler::handle_client_auth_more_password(ObClientSessionInfo &client_info,
                                                           const bool is_client_ssl,
                                                           const bool is_need_copy)
{
  int ret = OB_SUCCESS;
  if (is_client_ssl) {
    if (is_need_copy) {
      // 转发 observer 时拷贝到 for_server；need_force 仅回 deferred OK 时 is_need_copy=false，不拷贝
      if (OB_FAIL(copy_tls_client_auth_more_raw_to_for_server(client_info))) {
        LOG_WDIAG("fail to copy TLS client auth more data for server leg", K(ret));
      }
    }
  } else {
    if (OB_FAIL(check_non_tls_auth_more_rsa_decrypt_precondition(client_info))) {
      LOG_WDIAG("fail to check non-TLS auth more RSA decrypt precondition", K(ret));
    } else if (OB_FAIL(decrypt_rsa_password_and_normalize(client_info))) {
      LOG_WDIAG("fail to decrypt RSA auth more data and normalize", K(ret), K(is_client_ssl));
    }
  }
  return ret;
}

int ObMysqlCsha2Handler::reply_deferred_ok_for_need_force(ObMysqlTransact::ObTransState &s,
                                                          ObClientSessionInfo &client_info)
{
  ObClientSessionCsha2AuthContext &csha2_ctx = client_info.get_csha2_auth_ctx();
  int ret = OB_SUCCESS;
  const uint8_t client_auth_more_data_resp_pkt_seq =
      static_cast<uint8_t>(s.trans_info_.client_request_.get_packet_meta().pkt_seq_);
  ObMysqlClientSession *client_session = s.sm_->get_client_session();
  ObProxyProtocol client_protocol = s.sm_->get_client_session_protocol();
  if (OB_ISNULL(client_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("client session is null", K(ret));
  } else if (!csha2_ctx.has_deferred_login_ok()) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_CSHA2_LOG(WDIAG, "deferred login ok is empty", K(ret), "len", csha2_ctx.deferred_login_ok_resp_.len());
  } else if (OB_FAIL(s.alloc_internal_buffer(MYSQL_BUFFER_SIZE))) {
    LOG_WDIAG("fail to alloc internal buffer for deferred ok", K(ret));
  } else {
    if (OB_FAIL(handle_client_auth_more_password(client_info, is_client_ssl(s), false))) {
      LOG_WDIAG("fail to handle client auth more password", K(ret));
    } else  {
      // 返回deferred ok
      const int64_t ok_len = csha2_ctx.deferred_login_ok_resp_.len();
      const char *ok_ptr = csha2_ctx.deferred_login_ok_resp_.ptr();
      const uint8_t seq = static_cast<uint8_t>(client_auth_more_data_resp_pkt_seq + 1);
      char *send_buf = NULL;
      int64_t send_len = 0;
      if (OB_FAIL(build_deferred_ok_for_mysql_client(
              s, client_info, ok_ptr, ok_len, seq, send_buf, send_len))) {
        LOG_WDIAG("fail to build deferred ok for client (no raw OK fallback; connection will break)",
                  K(ret), K(ok_len));
      } else if (OB_ISNULL(send_buf) || send_len < MYSQL_NET_HEADER_LENGTH) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("deferred ok build returned empty", K(ret), KP(send_buf), K(send_len));
      } else {
        ObString pkt_str(static_cast<int32_t>(send_len), send_buf);
        if (OB_FAIL(ObProxyPacketWriter::write_raw_packet(*s.internal_buffer_,
                                                          *client_session,
                                                          client_protocol, pkt_str))) {
          LOG_WDIAG("fail to write deferred ok to internal buffer", K(ret));
        }
        op_fixed_mem_free(send_buf, send_len);
        send_buf = NULL;
      }
    }
  }

  if (OB_SUCC(ret)) {
    PROXY_CSHA2_LOG(DEBUG, "send deferred ok to client success", K(s.sm_->get_client_session()->get_cs_id()));
    csha2_ctx.need_force_auth_more_data_ = false;
    csha2_ctx.deferred_login_ok_resp_.reset();
    s.trans_info_.resp_result_.set_is_auth_more_data_req(false);
  }
  return ret;
}

int ObMysqlCsha2Handler::forward_auth_more_password_to_server(ObMysqlTransact::ObTransState &s,
                                                              ObClientSessionInfo &client_info)
{
  int ret = OB_SUCCESS;
  const bool client_ssl = is_client_ssl(s);
  if (OB_FAIL(handle_client_auth_more_password(client_info, client_ssl, true))) {
    LOG_WDIAG("fail to handle client auth more password", K(ret));
  } else {
    ObMysqlServerSession *server_session = NULL;
    if (OB_FAIL(csha2_get_auth_target_server_session(s, server_session))) {
      LOG_WDIAG("fail to get auth target server session", K(ret));
    } else {
      PROXY_CSHA2_LOG(DEBUG, "prepared client auth more password for server forward",
                K(s.sm_->get_client_session()->get_cs_id()),
                K(client_ssl));
    }
  }
  return ret;
}

int ObMysqlCsha2Handler::begin_forced_full_auth(ObMysqlTransact::ObTransState &s,
                                                ObClientSessionInfo &client_info,
                                                const char *trace_desc)
{
  int ret = OB_SUCCESS;
  ObIOBufferReader *buf_reader = s.sm_->get_server_buffer_reader();
  const int64_t ok_offset = s.trans_info_.resp_result_.get_deferred_ok_offset();
  const int64_t ok_pkt_len = s.trans_info_.resp_result_.get_deferred_ok_pkt_len();
  PROXY_CSHA2_LOG(DEBUG, "recv server response(fast auth succ + ok) -> force client full auth (use analyzer result)",
            "trace_desc", trace_desc,
            "cs_id", s.sm_->get_client_session()->get_cs_id(),
            "defer_ok_offset", ok_offset,
            "deferred_ok_len", ok_pkt_len);
  if (OB_FAIL(save_deferred_ok(buf_reader, client_info, ok_offset, ok_pkt_len))) {
    LOG_WDIAG("fail to save deferred ok", K(ret), "trace_desc", trace_desc);
  } else {
    client_info.get_csha2_auth_ctx().need_force_auth_more_data_ = true;
    s.trans_info_.resp_result_.set_is_auth_more_data_req(true);
    ObMysqlTransact::consume_response_packet(s);
    PROXY_CSHA2_LOG(DEBUG, "deferred server OK (will request client plaintext password)",
              "trace_desc", trace_desc,
              "cs_id", s.sm_->get_client_session()->get_cs_id(),
              "deferred_ok_len", ok_pkt_len);
  }
  return ret;
}

void ObMysqlCsha2Handler::handle_server_rsa_public_key_response(ObMysqlTransact::ObTransState &s)
{
  ObClientSessionInfo &client_info = ObMysqlTransact::get_client_session_info(s);
  ObRespAnalyzeResult &resp = s.trans_info_.resp_result_;
  const common::ObString &pub_key = resp.get_rsa_public_key();
  int ret = OB_SUCCESS;
  if (pub_key.length() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("server RSA public key response empty", K(ret));
  } else {
    ObMysqlServerSession *server_session = s.sm_->get_server_session();
    if (OB_ISNULL(server_session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("server session null when encrypting for server RSA", K(ret));
    } else if (OB_FAIL(encrypt_password_with_server_public_key(client_info, server_session, pub_key))) {
      LOG_WDIAG("fail to encrypt password with server RSA public key after receiving key", K(ret));
    } else {
      // server 返回 PEM 后，这一包不会继续转发给客户端，而是由 proxy 自己消费并继续发密文。
      ObMysqlTransact::consume_response_packet(s);
      if (ObMysqlTransact::INTERNAL_ERROR == s.current_.state_) {
        s.next_action_ = ObMysqlTransact::SM_ACTION_SERVER_READ;
        if (client_info.is_oceanbase_server()) {
          s.sm_->api_.do_response_transform_open();
        }
      } else {
        s.current_.send_action_ = ObMysqlTransact::SERVER_SEND_AUTH_MORE_DATA;
        s.next_action_ = ObMysqlTransact::SM_ACTION_API_SEND_REQUEST;
        PROXY_CSHA2_LOG(DEBUG, "consumed server PEM, encrypted password, will send auth more data to server",
                  K(s.sm_->get_client_session()->get_cs_id()));
      }
    }
  }
  if (OB_FAIL(ret)) {
    reply_err_to_client_and_break(s, ret);
    s.next_action_ = ObMysqlTransact::SM_ACTION_SEND_ERROR_NOOP;
  }
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
