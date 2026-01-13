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
#include "proxy/mysqllib/ob_mysql_request_builder.h"
#include "proxy/mysql/ob_mysql_sm.h"
#include "lib/utility/ob_2_0_sess_veri.h"
#include "rpc/obmysql/packet/ompk_change_user.h"
#include "obproxy/proxy/mysqllib/ob_mysql_packet_rewriter.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::packet;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

int ObMysqlRequestBuilder::build_request_packet(ObString sql,
                                                ObMySQLCmd cmd,
                                                ObMysqlSM *sm,
                                                ObMIOBuffer &mio_buf,
                                                ObMysqlServerSession *server_session,
                                                const ObProxyProtocol ob_proxy_protocol)
{
  int ret = OB_SUCCESS;
  uint8_t next_compress_seq = 0;
  ObServerSessionInfo &server_info = server_session->get_session_info();
  if (ObProxyProtocol::PROTOCOL_OCEANBASE_20 == ob_proxy_protocol) {
    ObSEArray<ObObJKV, 3> extra_info;
    ObSqlString sess_info_value;
    char client_ip_buf[MAX_IP_BUFFER_LEN] = "\0";
    char flt_info_buf[SERVER_FLT_INFO_BUF_MAX_LEN] = "\0";
    char sess_info_veri_buf[OB_SESS_INFO_VERI_BUF_MAX] = "\0";

    const bool is_last_packet = true;
    const bool is_proxy_switch_route = false;
    const int64_t compression_level = sm->compression_algorithm_.level_;
    const bool is_compressed_ob20 = (server_info.is_server_ob20_compress_supported() && compression_level !=0);
    Ob20HeaderParam ob20_head_param(server_session->get_server_sessid(), server_session->get_next_server_request_id(),
                                            next_compress_seq, next_compress_seq, is_last_packet, /* is_weak_read */ false,
                                            /* is_need_reroute */ false, server_info.is_new_extra_info_supported(),
                                            sm->get_client_session()->is_trans_internal_routing(), is_proxy_switch_route,
                                            is_compressed_ob20, compression_level);
    DEC_AND_INC_SHARED_REF(ob20_head_param.get_protocol_diagnosis_ref(), sm->protocol_diagnosis_);
    if (OB_FAIL(ObProxyTraceUtils::build_related_extra_info_all(extra_info, sm,
                                                                client_ip_buf, MAX_IP_BUFFER_LEN,
                                                                flt_info_buf, SERVER_FLT_INFO_BUF_MAX_LEN,
                                                                sess_info_veri_buf, OB_SESS_INFO_VERI_BUF_MAX,
                                                                sess_info_value, is_last_packet,
                                                                is_proxy_switch_route))) {
      LOG_WDIAG("fail to build related extra info", K(ret));
    } else if (OB_FAIL(ObMysqlOB20PacketWriter::write_request_packet(mio_buf, cmd, sql, ob20_head_param, &extra_info))) {
      LOG_WDIAG("fail to write request packet in ob20", K(ret));
    } else {
      next_compress_seq = ob20_head_param.get_compressed_seq();
    }
  } else {
    const bool need_compress = ob_proxy_protocol == ObProxyProtocol::PROTOCOL_COMPRESSED_MYSQL ? true : false;
    ObCompressedHeaderParam param(next_compress_seq, server_info.is_checksum_on(), sm->compression_algorithm_.level_);
    DEC_AND_INC_SHARED_REF(param.get_protocol_diagnosis_ref(), sm->protocol_diagnosis_);
    if (OB_FAIL(ObMysqlPacketWriter::write_request_packet(mio_buf, cmd, sql, need_compress, param))) {
      LOG_WDIAG("fail to write request packet in mysql/compressed mysql", K(ob_proxy_protocol), K(ret));
    } else {
      next_compress_seq = param.get_compressed_seq();
    }
  }

  if (OB_SUCC(ret)) {
    server_session->set_cur_compressed_seq(next_compress_seq - 1);
  }

  return ret;
}

int ObMysqlRequestBuilder::build_database_sync_packet(ObMysqlSM *sm,
                                                      ObMIOBuffer &mio_buf,
                                                      ObClientSessionInfo &client_info,
                                                      ObMysqlServerSession *server_session,
                                                      const ObProxyProtocol ob_proxy_protocol)
{
  int ret = OB_SUCCESS;
  ObServerSessionInfo &server_info = server_session->get_session_info();
  if (client_info.need_reset_database(server_info)) {
    ObMySQLCmd cmd = OB_MYSQL_COM_INIT_DB;
    ObString db_name;
    char sql[OB_SHORT_SQL_LENGTH];
    if (OB_FAIL(client_info.extract_changed_schema(server_info, db_name))) {
      LOG_WDIAG("fail to extract changed schema", K(ret));
    } else if (client_info.is_oracle_mode()) {
      // need rewrite com init db for oracle mode
      // write_request_packet will copy db_name to mio_buf, so we can use local buffer to store alter session sql
      int64_t len = snprintf(sql, OB_SHORT_SQL_LENGTH, "alter session set current_schema = \"%.*s\"",
                             db_name.length(), db_name.ptr());
      if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= OB_SHORT_SQL_LENGTH)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to fill sql set db", K(sql), K(len), K(ret));
      } else {
        cmd = OB_MYSQL_COM_QUERY;
        db_name.assign_ptr(sql, static_cast<int32_t>(len));
      }
    }
    #ifdef ERRSIM
    if (OB_SUCC(ret) && OB_FAIL(OB_E(EventTable::EN_SYNC_DATABASE_FAIL) OB_SUCCESS))  {
      ret = OB_SUCCESS;
      db_name = "use errsim_database;";
    }
    #endif
    if (OB_FAIL(build_request_packet(db_name, cmd, sm, mio_buf, server_session, ob_proxy_protocol))) {
      LOG_WDIAG("fail to build sync database packet", K(db_name), K(cmd));
    } else {
      LOG_DEBUG("will sync database", K(db_name), K(cmd));
    }
  }
  return ret;
}

int ObMysqlRequestBuilder::build_all_session_vars_sync_packet(ObMysqlSM *sm,
                                                              ObMIOBuffer &mio_buf,
                                                              ObClientSessionInfo &client_info,
                                                              ObMysqlServerSession *server_session,
                                                              const ObProxyProtocol ob_proxy_protocol)
{
  int ret = OB_SUCCESS;
  ObMySQLCmd cmd = OB_MYSQL_COM_QUERY;
  ObSqlString sql;
  if (OB_FAIL(client_info.extract_all_variable_reset_sql(sql))) {
    LOG_WDIAG("fail to extract all variable reset sql", K(ret));
  } else if (OB_FAIL(build_request_packet(sql.string(), cmd, sm, mio_buf, server_session, ob_proxy_protocol))) {
    LOG_WDIAG("fail to build sync all session vars packet", K(sql), K(cmd), K(ret));
  } else {
    LOG_DEBUG("will sync all session vars", K(sql), K(cmd));
  }

  return ret;
}


int ObMysqlRequestBuilder::build_session_vars_sync_packet(ObMysqlSM *sm,
                                                          ObMIOBuffer &mio_buf,
                                                          ObClientSessionInfo &client_info,
                                                          ObMysqlServerSession *server_session,
                                                          const ObProxyProtocol ob_proxy_protocol)
{
  int ret = OB_SUCCESS;
  ObMySQLCmd cmd = OB_MYSQL_COM_QUERY;
  ObSqlString reset_sql;
  ObServerSessionInfo &server_info = server_session->get_session_info();
  if (OB_FAIL(client_info.extract_variable_reset_sql(server_info, reset_sql))) {
    LOG_WDIAG("fail to extract variable reset sql", K(ret));
  } else {
    #ifdef ERRSIM
    if (OB_FAIL(OB_E(EventTable::EN_SYNC_SYS_VAR_FAIL) OB_SUCCESS))  {
      ret = OB_SUCCESS;
      reset_sql.reset();
      reset_sql.append("errsim sync sys var");
    }
    #endif
    if (OB_FAIL(build_request_packet(reset_sql.string(), cmd, sm, mio_buf, server_session, ob_proxy_protocol))) {
      LOG_WDIAG("fail to build sync session vars packet", K(reset_sql), K(cmd), K(ret));
    } else {
      LOG_DEBUG("will sync session vars", K(reset_sql), K(cmd));
    }
  }
  return ret;
}

int ObMysqlRequestBuilder::build_session_user_vars_sync_packet(ObMysqlSM *sm,
                                                               ObMIOBuffer &mio_buf,
                                                               ObClientSessionInfo &client_info,
                                                               ObMysqlServerSession *server_session,
                                                               const ObProxyProtocol ob_proxy_protocol)
{
  int ret = OB_SUCCESS;
  ObMySQLCmd cmd = OB_MYSQL_COM_QUERY;
  ObSqlString reset_sql;
  ObServerSessionInfo &server_info = server_session->get_session_info();
  if (OB_FAIL(client_info.extract_user_variable_reset_sql(server_info, reset_sql))) {
    LOG_WDIAG("fail to extract variable reset sql", K(ret));
  } else {
    #ifdef ERRSIM
    if (OB_FAIL(OB_E(EventTable::EN_SYNC_USER_VAR_FAIL) OB_SUCCESS))  {
      ret = OB_SUCCESS;
      reset_sql.reset();
      reset_sql.append("errsim sync user var");
    }
    #endif
    if (OB_FAIL(build_request_packet(reset_sql.string(), cmd, sm, mio_buf, server_session, ob_proxy_protocol))) {
      LOG_WDIAG("fail to build sync user session vars packet", K(reset_sql), K(cmd), K(ret));
    } else {
      LOG_DEBUG("will sync user session vars", K(reset_sql), K(cmd));
    }
  }
  return ret;
}

int ObMysqlRequestBuilder::build_start_trans_request(ObMysqlSM *sm,
                                                     ObMIOBuffer &mio_buf,
                                                     ObClientSessionInfo &client_info,
                                                     ObMysqlServerSession *server_session,
                                                     const ObProxyProtocol ob_proxy_protocol)
{
  int ret = OB_SUCCESS;
  #ifdef ERRSIM
  if (OB_FAIL(OB_E(EventTable::EN_SYNC_START_TRANS_FAIL) OB_SUCCESS)) {
    ret = OB_SUCCESS;
    client_info.set_start_trans_sql("errsim");
  }
  #endif

  ObMySQLCmd cmd = OB_MYSQL_COM_QUERY;
  ObString &sql = client_info.get_start_trans_sql();
  if (OB_FAIL(build_request_packet(sql, cmd, sm, mio_buf, server_session, ob_proxy_protocol))) {
    LOG_WDIAG("fail to build start transaction packet", K(sql), K(cmd), K(ret));
  } else {
    LOG_DEBUG("will sync start transaction", K(sql), K(cmd));
  }

  return ret;
}

int ObMysqlRequestBuilder::build_xa_start_request(ObMysqlSM *sm,
                                                  ObMIOBuffer &mio_buf,
                                                  ObClientSessionInfo &client_info,
                                                  ObMysqlServerSession *server_session,
                                                  const ObProxyProtocol ob_proxy_protocol)
{
  LOG_DEBUG("start to build xa start request");
  int ret = OB_SUCCESS;
  ObString &xa_start_req_pkt = client_info.get_start_trans_sql();
  ObString xa_pkt_payload(xa_start_req_pkt.length() - MYSQL_NET_META_LENGTH,
                          xa_start_req_pkt.ptr() + MYSQL_NET_META_LENGTH);
  const char *cmd_pos = xa_start_req_pkt.ptr() + MYSQL_NET_HEADER_LENGTH;
  uint8_t xa_cmd;
  ObMySQLUtil::get_uint1(cmd_pos, xa_cmd);
  if (OB_MYSQL_COM_STMT_PREPARE_EXECUTE == xa_cmd) {
    const char *stmt_pos = xa_pkt_payload.ptr();
    uint32_t client_ps_id = 0;
    ObMySQLUtil::get_uint4(stmt_pos, client_ps_id);
    if (0 != client_ps_id) {
      ObServerSessionInfo &ss_info =
          sm->get_server_session()->get_session_info();
      /* 如果这个 Server 已经发送过一次，则拿到的是真实的 Server Ps Id
       * 如果这个 Server 还没发送过，返回的则是 0
       */
      uint32_t server_ps_id = ss_info.get_server_ps_id(client_ps_id);
      memcpy(xa_pkt_payload.ptr(), &server_ps_id, sizeof(server_ps_id));
    }
  } else {
    LOG_EDIAG("xa start cmd is not OB_MYSQL_COM_STMT_PREPARE_EXECUTE, something wrong");
  }
  ObMySQLCmd cmd = OB_MYSQL_COM_STMT_PREPARE_EXECUTE;
  if (OB_FAIL(build_request_packet(xa_pkt_payload, cmd, sm, mio_buf, server_session, ob_proxy_protocol))) {
    LOG_WDIAG("fail to build xa start packet", K(xa_pkt_payload), K(cmd), K(ret));
  } else {
    LOG_DEBUG("will sync xa start", K(xa_pkt_payload), K(cmd));
  }
  return ret;
}

int ObMysqlRequestBuilder::build_request_from_packet_str(
    ObMysqlSM *sm,
    const ObString &packet_str,
    event::ObMIOBuffer &mio_buf,
    ObMysqlServerSession *server_session,
    const ObProxyProtocol ob_proxy_protocol,
    const ObIArray<ObObJKV> *extra_info)
{
  int ret = OB_SUCCESS;
  uint8_t next_compress_seq = 0;
  ObServerSessionInfo &server_info = server_session->get_session_info();
  if (ObProxyProtocol::PROTOCOL_OCEANBASE_20 == ob_proxy_protocol) {
    const bool is_last_packet = true;
    const bool is_proxy_switch_route = false;
    const int64_t compression_level = sm->compression_algorithm_.level_;
    const bool is_compressed_ob20 = (server_info.is_server_ob20_compress_supported() && compression_level !=0);
    Ob20HeaderParam ob20_head_param(server_session->get_server_sessid(), server_session->get_next_server_request_id(),
                                    next_compress_seq, next_compress_seq, is_last_packet, /* is_weak_read */ false,
                                    /* is_need_reroute */ false, server_info.is_new_extra_info_supported(),
                                    sm->get_client_session()->is_trans_internal_routing(), is_proxy_switch_route,
                                    is_compressed_ob20, compression_level);
    DEC_AND_INC_SHARED_REF(ob20_head_param.get_protocol_diagnosis_ref(), sm->protocol_diagnosis_);
    if (OB_FAIL(ObMysqlOB20PacketWriter::write_raw_packet(mio_buf, packet_str, ob20_head_param, extra_info))) {
      LOG_WDIAG("fail to write request packet in ob20", K(ret));
    } else {
      next_compress_seq = ob20_head_param.get_compressed_seq();
    }
  } else {
    const bool need_compress = ob_proxy_protocol == ObProxyProtocol::PROTOCOL_COMPRESSED_MYSQL ? true : false;
    ObCompressedHeaderParam param(next_compress_seq, server_info.is_checksum_on(), sm->compression_algorithm_.level_);
    DEC_AND_INC_SHARED_REF(param.get_protocol_diagnosis_ref(), sm->protocol_diagnosis_);
    if (need_compress) {
      if (OB_FAIL(ObMysqlPacketWriter::write_compressed_raw_packet(mio_buf, packet_str, param))) {
        LOG_WDIAG("fail to write request packet in compressed mysql", K(ob_proxy_protocol), K(ret));
      }
    } else {
      if (OB_FAIL(ObMysqlPacketWriter::write_raw_packet(mio_buf, packet_str))) {
        LOG_WDIAG("fail to write request packet in mysql", K(ob_proxy_protocol), K(ret));
      }
    }

    next_compress_seq = param.get_compressed_seq();
  }

  if (OB_SUCC(ret)) {
    server_session->set_cur_compressed_seq(next_compress_seq - 1);
  }

  return ret;
}
int ObMysqlRequestBuilder::build_prepare_request(ObMysqlSM *sm,
                                                 ObMIOBuffer &mio_buf,
                                                 ObClientSessionInfo &client_info,
                                                 ObMysqlServerSession *server_session,
                                                 const ObProxyProtocol ob_proxy_protocol)
{
  int ret = OB_SUCCESS;
  ObMySQLCmd cmd = OB_MYSQL_COM_STMT_PREPARE;
  ObString ps_sql;
  if (OB_FAIL(client_info.get_ps_sql(ps_sql))) {
    LOG_WDIAG("fail to get ps sql", K(ret));
  } else if (OB_FAIL(build_request_packet(ps_sql, cmd, sm, mio_buf, server_session, ob_proxy_protocol))) {
    LOG_WDIAG("fail to build prepare packet", K(ps_sql), K(cmd), K(ret));
  } else {
    LOG_DEBUG("will sync preapre", K(ps_sql), K(cmd));
  }

  return ret;
}

int ObMysqlRequestBuilder::build_ssl_request_packet(ObMysqlSM *sm,
                                                    event::ObMIOBuffer &mio_buf,
                                                    ObClientSessionInfo &client_info,
                                                    ObMysqlServerSession *server_session,
                                                    const ObProxyProtocol ob_proxy_protocol)
{
  UNUSED(sm);
  UNUSED(server_session);
  UNUSED(ob_proxy_protocol); // auth request no need compress
  obmysql::OMPKSSLRequest ssl_req = client_info.get_ssl_req();

  return packet::ObMysqlPacketWriter::write_packet(mio_buf, ssl_req);
}

int ObMysqlRequestBuilder::build_text_ps_prepare_request(ObMysqlSM *sm,
                                                         ObMIOBuffer &mio_buf,
                                                         ObClientSessionInfo &client_info,
                                                         ObMysqlServerSession *server_session,
                                                         const ObProxyProtocol ob_proxy_protocol)
{
  int ret = OB_SUCCESS;
  ObMySQLCmd cmd = OB_MYSQL_COM_QUERY;
  ObString sql;
  if (OB_FAIL(client_info.get_text_ps_sql(sql))) {
    LOG_WDIAG("fail to get ps sql", K(ret));
  } else if (OB_FAIL(build_request_packet(sql, cmd, sm, mio_buf, server_session, ob_proxy_protocol))) {
    LOG_WDIAG("fail to build text ps prepare packet", K(sql), K(cmd), K(ret));
  } else {
    LOG_DEBUG("will sync text ps prepare", K(sql), K(cmd));
  }

  return ret;
}

int ObMysqlRequestBuilder::add_connect_attr(const char *key, const char *value,
                                            OMPKChangeUser &change_user)
{
  ObStringKV str_kv;
  str_kv.key_.assign_ptr(key, static_cast<int32_t>(STRLEN(key)));
  str_kv.value_.assign_ptr(value, static_cast<int32_t>(STRLEN(value)));
  return change_user.add_connect_attr(str_kv);
}

int ObMysqlRequestBuilder::add_connect_attr(const char *key, const common::ObString &value,
                                            OMPKChangeUser &change_user)
{
  ObStringKV str_kv;
  str_kv.key_.assign_ptr(key, static_cast<int32_t>(STRLEN(key)));
  str_kv.value_.assign_ptr(value.ptr(), value.length());
  return change_user.add_connect_attr(str_kv);
}

int ObMysqlRequestBuilder::build_reset_session_request(ObMysqlSM *sm,
                                                       ObMIOBuffer &mio_buf,
                                                       ObClientSessionInfo &client_info,
                                                       ObMysqlServerSession *server_session,
                                                       const ObProxyProtocol ob_proxy_protocol)
{
  int ret = OB_SUCCESS;

  OMPKChangeUser change_user_req;
  OMPKHandshakeResponse &handshake_resp = client_info.get_login_req().get_hsr_result().response_;
  ObMySQLCapabilityFlags cap_flag = handshake_resp.get_capability_flags();
  net::ObUnixNetVConnection* unix_vc = static_cast<net::ObUnixNetVConnection *>(server_session->get_netvc());
  cap_flag.cap_flags_.OB_CLIENT_CONNECT_ATTRS = 1;
  cap_flag.cap_flags_.OB_CLIENT_SESSION_TRACK = 1;
  cap_flag.cap_flags_.OB_CLIENT_SSL = unix_vc->using_ssl();

  bool is_first_login = sm->trans_state_.is_handshake_req_phase();
  ObSEArray<ObObJKV, 3> extra_info;
  ObString database;
  if (is_first_login) {
    // 登录使用会话连接池
    // 客户端如果正在登录就直接使用 handshake response 中的 database
    database = handshake_resp.get_database();
   } else {
    // 切路由使用会话连接池
    // 客户端如果正在登录 client info 里面还没有信息
    database = client_info.get_database_name();

    // COM_CHANGE_USER 中不同步 sess info, 在用户切路由后的第一个请求中同步 sess info
    //if (ObProxyProtocol::PROTOCOL_OCEANBASE_20 == ob_proxy_protocol) {
    //  ObSqlString sess_info_value;
    //  if (OB_FAIL(ObProxyTraceUtils::build_sync_sess_info(extra_info, sess_info_value, sm, true))) {
    //    LOG_WDIAG("fail to build related extra info", K(ret));
    //  }
    //}
  }

  change_user_req.set_database(database);
  change_user_req.set_mysql_capability(cap_flag);
  change_user_req.set_username(client_info.get_priv_info().user_name_);
  change_user_req.set_character_set(client_info.get_ncharacter_set_connection());
  change_user_req.set_auth_plugin_name(handshake_resp.get_auth_plugin_name());
  change_user_req.set_auth_response(handshake_resp.get_auth_response());
  change_user_req.reset_connect_attr();

  ObHandshakeResponseParam param;
  ObMysqlClientSession *client_session = sm->client_session_;
  const ObString &proxy_scramble = client_session->get_scramble_string();
  const ObString &server_scramble = server_session->get_scramble_string();

  ObAddr client_addr = client_session->get_real_client_addr(const_cast<net::ObNetVConnection *>(server_session->get_netvc()));

  bool find_client_ip = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < handshake_resp.get_connect_attrs().count(); ++i) {
    ObStringKV kv;
    // transit conn attrs OB_MYSQL_OB_CLIENT
    if (OB_FAIL(handshake_resp.get_connect_attrs().at(i, kv))) {
      LOG_WDIAG("fail access handshake response connect attrs", K(i), K(ret));
    } else if (kv.key_.prefix_match(OB_MYSQL_OB_CLIENT)) {
      if (OB_FAIL(change_user_req.get_connect_attrs().push_back(kv))) {
        LOG_WDIAG("fail push back transparent transmit connect attrs", K(kv), K(ret));
      } else { /* succ */ }
    } else if (!find_client_ip
                && sm->trans_state_.mysql_config_params_->enable_client_ip_checkout_
                && 0 == kv.key_.case_compare(OB_MYSQL_CLIENT_IP)
                && !kv.value_.empty()){
      snprintf(param.client_ip_buf_, MAX_IP_ADDR_LENGTH, "%.*s", kv.value_.length(), kv.value_.ptr());
      find_client_ip = true;
    } else { /* do nothing */ }
  }

  // fill params
  if (OB_FAIL(param.write_proxy_conn_id_buf(client_session->get_proxy_sessid()))) {
    LOG_WDIAG("fail to write_proxy_conn_id_buf", K(ret), K(client_session->get_proxy_sessid()));
  } else if (is_first_login && OB_FAIL(param.write_global_vars_version_buf(static_cast<int64_t>(0)))) {
    LOG_WDIAG("fail to write_global_vars_version_buf", K(ret));
  } else if (!is_first_login && OB_FAIL(param.write_global_vars_version_buf(client_info.get_global_vars_version()))) {
    LOG_WDIAG("fail to write_global_vars_version_buf", K(ret), K(client_info.get_global_vars_version()));
  } else if (!proxy_scramble.empty() && OB_FAIL(param.write_proxy_scramble(proxy_scramble, server_scramble))) {
    LOG_WDIAG("fail to write_proxy_scramble", K(ret), K(proxy_scramble), K(server_scramble), K(client_info.get_global_vars_version()));
  } else if (OB_FAIL(!find_client_ip && param.write_client_addr_buf(client_addr))) {
    LOG_WDIAG("fail to write_client_addr_buf", K(ret), K(client_addr));
  } else if (OB_FAIL(param.write_client_port_buf(client_addr.get_port()))) {
    LOG_WDIAG("fail to write_client_port_buf", K(ret), K(client_addr.get_port()));
  } else if (OB_FAIL(param.write_cs_id_buf(client_session->get_cs_id()))) {
    LOG_WDIAG("fail to write_cs_id_buf", K(ret), K(client_session->get_cs_id()));
  } else if (OB_FAIL(param.write_connected_time_buf(client_session->get_connected_time()))) {
    LOG_WDIAG("fail to write_connected_time_buf", K(ret), K(client_session->get_connected_time()));
  } else if (OB_FALSE_IT(param.proxy_idc_name_ = sm->multi_level_config_->proxy_idc_name_)) {
  // add connection attributes
  } else if (OB_FAIL(add_connect_attr(OB_MYSQL_PROXY_CONNECTION_ID, param.proxy_conn_id_buf_, change_user_req))) {
    LOG_WDIAG("fail to add proxy_sessid", K(param.proxy_conn_id_buf_), K(ret));
  } else if (OB_FAIL(add_connect_attr(OB_MYSQL_GLOBAL_VARS_VERSION, param.global_vars_version_buf_, change_user_req))) {
    LOG_WDIAG("fail to add global vars version", K(param.global_vars_version_buf_), K(ret));
  } else if (param.is_proxy_scramble_valid() && OB_FAIL(add_connect_attr(OB_MYSQL_SCRAMBLE, param.proxy_scramble_, change_user_req))) {
    LOG_WDIAG("fail to add global vars version", K(param.proxy_scramble_), K(ret));
  } else if (param.is_client_ip_valid() && OB_FAIL(add_connect_attr(OB_MYSQL_CLIENT_IP, param.client_ip_buf_, change_user_req))) {
    LOG_WDIAG("fail to add client ip", K(param.client_ip_buf_), K(ret));
  } else if (OB_FAIL(add_connect_attr(OB_MYSQL_CLIENT_PORT, param.client_port_buf_, change_user_req))) {
    LOG_WDIAG("fail to add client port", K(param.client_port_buf_), K(ret));
  } else if (OB_FAIL(add_connect_attr(OB_MYSQL_CLIENT_SESSION_ID, param.cs_id_buf_, change_user_req))) {
    LOG_WDIAG("fail to add connected time", K(param.cs_id_buf_), K(ret));
  } else if (OB_FAIL(add_connect_attr(OB_MYSQL_CLIENT_CONNECT_TIME, param.connected_time_buf_, change_user_req))) {
    LOG_WDIAG("fail to add connected time", K(param.connected_time_buf_), K(ret));
  } else if (!param.proxy_idc_name_.empty() && OB_FAIL(add_connect_attr(OB_MYSQL_PROXY_IDC_NAME, param.proxy_idc_name_, change_user_req))) {
    LOG_WDIAG("fail to add proxy idc name", K(param.proxy_idc_name_), K(ret));
  } else {
    char change_user_req_buf[4096];
    int64_t pos = 0;
    int64_t buf_len = 4096;
    change_user_req.set_seq(1); // client expected to receive the ok/auth switch of seq==2
    if (OB_FAIL(ObMySQLPacket::encode_packet(change_user_req_buf, buf_len, pos, change_user_req))) {
      LOG_WDIAG("fail to serialize change user request", K(ret), K(pos));
    } else {
      LOG_DEBUG("succ to serialize change user request to reset session status", K(pos));
      ObString change_user_req_str(pos, change_user_req_buf);
      if (OB_FAIL(build_request_from_packet_str(sm, change_user_req_str, mio_buf, server_session, ob_proxy_protocol, &extra_info))) {
        LOG_WDIAG("fail to build_request_from_packet_str", K(ret));
      } else {
        SESSION_POOL_LOG(DEBUG, "succ to build com_stmt_change_user to reset session",
                                "proxy_sessid", client_session->get_proxy_sessid(),
                                "server_addr", server_session->server_ip_,
                                "client_addr", client_addr,
                                "user",client_info.get_priv_info().user_name_,
                                "db", change_user_req.get_database(),
                                "charset", change_user_req.get_character_set(),
                                K(proxy_scramble),
                                K(server_scramble),
                                "connected_time", client_session->get_connected_time());
                    }
    }
  }


  return ret;
}

int ObMysqlRequestBuilder::build_binlog_login_packet(ObMysqlSM *sm,
                                                     ObMIOBuffer &mio_buf,
                                                     ObClientSessionInfo &client_info,
                                                     ObMysqlServerSession *server_session,
                                                     const ObProxyProtocol ob_proxy_protocol)
{
  UNUSED(sm);
  UNUSED(server_session);
  UNUSED(ob_proxy_protocol);

  OMPKHandshakeResponse tg_hsr = client_info.get_login_req().get_hsr_result().response_;
  tg_hsr.set_username(client_info.get_login_req().get_hsr_result().full_name_);
  tg_hsr.set_seq(1);

  return packet::ObMysqlPacketWriter::write_packet(mio_buf, tg_hsr);
}

int ObMysqlRequestBuilder::build_init_sql_request_packet(ObMysqlSM *sm,
                                                         ObMIOBuffer &mio_buf,
                                                         ObClientSessionInfo &client_info,
                                                         ObMysqlServerSession *server_session,
                                                         const ObProxyProtocol ob_proxy_protocol)
{
  int ret = OB_SUCCESS;
  ObMySQLCmd cmd = OB_MYSQL_COM_QUERY;
  if (OB_FAIL(build_request_packet(client_info.get_init_sql(), cmd, sm, mio_buf, server_session, ob_proxy_protocol))) {
    LOG_WDIAG("fail to build init sql packet", K(client_info.get_init_sql()), K(cmd), K(ret));
  } else {
    client_info.set_has_send_init_sql(true);
    LOG_DEBUG("will sync init sql", K(client_info.get_init_sql()), K(cmd));
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
