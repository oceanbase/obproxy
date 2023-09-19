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
      LOG_WARN("fail to extract changed schema", K(ret));
    } else if (client_info.is_oracle_mode()) {
      // need rewrite com init db for oracle mode
      // write_request_packet will copy db_name to mio_buf, so we can use local buffer to store alter session sql
      int64_t len = snprintf(sql, OB_SHORT_SQL_LENGTH, "alter session set current_schema = \"%.*s\"",
                             db_name.length(), db_name.ptr());
      if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= OB_SHORT_SQL_LENGTH)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to fill sql set db", K(sql), K(len), K(ret));
      } else {
        cmd = OB_MYSQL_COM_QUERY;
        db_name.assign_ptr(sql, static_cast<int32_t>(len));
      }
    }

    if (OB_SUCC(ret)) {
      uint8_t compressed_seq = 0;
      if (ObProxyProtocol::PROTOCOL_OB20 == ob_proxy_protocol) {
        ObSEArray<ObObJKV, 3> extra_info;
        ObSqlString sess_info_value;
        char client_ip_buf[MAX_IP_BUFFER_LEN] = "\0";
        char flt_info_buf[SERVER_FLT_INFO_BUF_MAX_LEN] = "\0";
        char sess_info_veri_buf[OB_SESS_INFO_VERI_BUF_MAX] = "\0";
        const bool is_last_packet = true;
        const bool is_proxy_switch_route = false;
        if (OB_FAIL(ObProxyTraceUtils::build_related_extra_info_all(extra_info, sm,
                      client_ip_buf, MAX_IP_BUFFER_LEN,
                      flt_info_buf, SERVER_FLT_INFO_BUF_MAX_LEN,
                      sess_info_veri_buf, OB_SESS_INFO_VERI_BUF_MAX,
                      sess_info_value, is_last_packet, is_proxy_switch_route))) {
          LOG_WARN("fail to build related extra info", K(ret));
        } else if (OB_FAIL(ObMysqlOB20PacketWriter::write_request_packet(mio_buf, cmd, db_name,
                                                                         server_session->get_server_sessid(),
                                                                         server_session->get_next_server_request_id(),
                                                                         compressed_seq, compressed_seq, true, false, false,
                                                                         server_info.is_new_extra_info_supported(),
                                                                         sm->get_client_session()->is_trans_internal_routing(),
                                                                         is_proxy_switch_route, &extra_info))) {
          LOG_WARN("fail to write request packet in ob20", K(ret));
        } else { /* nothing */ }
      } else {
        const bool need_compress = ob_proxy_protocol == ObProxyProtocol::PROTOCOL_CHECKSUM ? true : false;
        ret = ObMysqlPacketWriter::write_request_packet(mio_buf, cmd, db_name, compressed_seq,
                                                        need_compress, server_info.is_checksum_on());
      }

      if (OB_SUCC(ret)) {
        server_session->set_compressed_seq(compressed_seq);
        LOG_DEBUG("will sync schema", K(db_name));
      } else {
        LOG_WARN("fail to write packet", K(cmd), K(db_name), K(ob_proxy_protocol), K(ret));
      }
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
  ObSqlString reset_sql;
  ObServerSessionInfo &server_info = server_session->get_session_info();
  if (OB_FAIL(client_info.extract_all_variable_reset_sql(reset_sql))) {
    LOG_WARN("fail to extract all variable reset sql", K(ret));
  } else {
    uint8_t compressed_seq = 0;
    if (ObProxyProtocol::PROTOCOL_OB20 == ob_proxy_protocol) {
      ObSEArray<ObObJKV, 3> extra_info;
      ObSqlString sess_info_value;
      char client_ip_buf[MAX_IP_BUFFER_LEN] = "\0";
      char flt_info_buf[SERVER_FLT_INFO_BUF_MAX_LEN] = "\0";
      char sess_info_veri_buf[OB_SESS_INFO_VERI_BUF_MAX] = "\0";
      const bool is_last_packet = true;
      const bool is_proxy_switch_route = false;
      if (OB_FAIL(ObProxyTraceUtils::build_related_extra_info_all(extra_info, sm,
                    client_ip_buf, MAX_IP_BUFFER_LEN,
                    flt_info_buf, SERVER_FLT_INFO_BUF_MAX_LEN,
                    sess_info_veri_buf, OB_SESS_INFO_VERI_BUF_MAX,
                    sess_info_value, is_last_packet, is_proxy_switch_route))) {
        LOG_WARN("fail to build related extra info", K(ret));
      } else if (OB_FAIL(ObMysqlOB20PacketWriter::write_request_packet(mio_buf, cmd, reset_sql.string(),
                                                                       server_session->get_server_sessid(),
                                                                       server_session->get_next_server_request_id(),
                                                                       compressed_seq, compressed_seq, true, false, false,
                                                                       server_info.is_new_extra_info_supported(),
                                                                       sm->get_client_session()->is_trans_internal_routing(),
                                                                       is_proxy_switch_route, &extra_info))) {
        LOG_WARN("fail to write request packet in ob20", K(ret));
      } else { /* nothing */ }
    } else {
      const bool need_compress = ob_proxy_protocol == ObProxyProtocol::PROTOCOL_CHECKSUM ? true : false;
      ret = ObMysqlPacketWriter::write_request_packet(mio_buf, cmd, reset_sql.string(), compressed_seq,
                                                      need_compress, server_info.is_checksum_on());
    }

    if (OB_SUCC(ret)) {
      server_session->set_compressed_seq(compressed_seq);
      LOG_DEBUG("will sync all session variables", K(reset_sql));
    } else {
      LOG_WARN("fail to write packet", K(cmd), K(reset_sql), K(ob_proxy_protocol), K(ret));
    }
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
    LOG_WARN("fail to extract variable reset sql", K(ret));
  } else {
    uint8_t compressed_seq = 0;
    if (ObProxyProtocol::PROTOCOL_OB20 == ob_proxy_protocol) {
        ObSEArray<ObObJKV, 3> extra_info;
        ObSqlString sess_info_value;
        char client_ip_buf[MAX_IP_BUFFER_LEN] = "\0";
        char flt_info_buf[SERVER_FLT_INFO_BUF_MAX_LEN] = "\0";
        char sess_info_veri_buf[OB_SESS_INFO_VERI_BUF_MAX] = "\0";
        const bool is_last_packet = true;
        const bool is_proxy_switch_route = false;
        if (OB_FAIL(ObProxyTraceUtils::build_related_extra_info_all(extra_info, sm,
                      client_ip_buf, MAX_IP_BUFFER_LEN,
                      flt_info_buf, SERVER_FLT_INFO_BUF_MAX_LEN,
                      sess_info_veri_buf, OB_SESS_INFO_VERI_BUF_MAX,
                      sess_info_value, is_last_packet, is_proxy_switch_route))) {
          LOG_WARN("fail to build related extra info", K(ret));
      } else if (OB_FAIL(ObMysqlOB20PacketWriter::write_request_packet(mio_buf, cmd, reset_sql.string(),
                                                                      server_session->get_server_sessid(),
                                                                      server_session->get_next_server_request_id(),
                                                                      compressed_seq, compressed_seq, true, false, false,
                                                                      server_info.is_new_extra_info_supported(), 
                                                                      sm->get_client_session()->is_trans_internal_routing(),
                                                                      is_proxy_switch_route, &extra_info))) {
        LOG_WARN("fail to write request packet in ob20", K(ret));
      } else { /* nothing */ }
    } else {
      const bool need_compress = ob_proxy_protocol == ObProxyProtocol::PROTOCOL_CHECKSUM ? true : false;
      ret = ObMysqlPacketWriter::write_request_packet(mio_buf, cmd, reset_sql.string(), compressed_seq,
                                                      need_compress, server_info.is_checksum_on());
    }

    if (OB_SUCC(ret)) {
      server_session->set_compressed_seq(compressed_seq);
      LOG_DEBUG("will sync session variables", K(reset_sql));
    } else {
      LOG_WARN("fail to write packet", K(cmd), K(reset_sql), K(ob_proxy_protocol), K(ret));
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
    LOG_WARN("fail to extract variable reset sql", K(ret));
  } else {
    uint8_t compressed_seq = 0;
    if (ObProxyProtocol::PROTOCOL_OB20 == ob_proxy_protocol) {
        ObSEArray<ObObJKV, 3> extra_info;
        ObSqlString sess_info_value;
        char client_ip_buf[MAX_IP_BUFFER_LEN] = "\0";
        char flt_info_buf[SERVER_FLT_INFO_BUF_MAX_LEN] = "\0";
        char sess_info_veri_buf[OB_SESS_INFO_VERI_BUF_MAX] = "\0";
        const bool is_last_packet = true;
        const bool is_proxy_switch_route = false;
        if (OB_FAIL(ObProxyTraceUtils::build_related_extra_info_all(extra_info, sm,
                      client_ip_buf, MAX_IP_BUFFER_LEN,
                      flt_info_buf, SERVER_FLT_INFO_BUF_MAX_LEN,
                      sess_info_veri_buf, OB_SESS_INFO_VERI_BUF_MAX,
                      sess_info_value, is_last_packet, is_proxy_switch_route))) {
        LOG_WARN("fail to build extra info for server", K(ret));
      } else if (OB_FAIL(ObMysqlOB20PacketWriter::write_request_packet(mio_buf, cmd, reset_sql.string(),
                                                                      server_session->get_server_sessid(),
                                                                      server_session->get_next_server_request_id(),
                                                                      compressed_seq, compressed_seq,  true, false, false,
                                                                      server_info.is_new_extra_info_supported(),
                                                                      sm->get_client_session()->is_trans_internal_routing(),
                                                                      is_proxy_switch_route, &extra_info))) {
        LOG_WARN("fail to write request packet in ob20", K(ret));
      } else { /* nothing */ }
    } else {
      const bool need_compress = ob_proxy_protocol == ObProxyProtocol::PROTOCOL_CHECKSUM ? true : false;
      ret = ObMysqlPacketWriter::write_request_packet(mio_buf, cmd, reset_sql.string(), compressed_seq,
                                                      need_compress, server_info.is_checksum_on());
    }

    if (OB_SUCC(ret)) {
      server_session->set_compressed_seq(compressed_seq);
      LOG_DEBUG("will sync session user variables", K(reset_sql));
    } else {
      LOG_WARN("fail to write packet", K(cmd), K(reset_sql), K(ob_proxy_protocol), K(ret));
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
  ObMySQLCmd cmd = OB_MYSQL_COM_QUERY;
  ObString &sql = client_info.get_start_trans_sql();
  ObServerSessionInfo &server_info = server_session->get_session_info();

  uint8_t compressed_seq = 0;
  if (ObProxyProtocol::PROTOCOL_OB20 == ob_proxy_protocol) {
    ObSEArray<ObObJKV, 3> extra_info;
    ObSqlString sess_info_value;
    char client_ip_buf[MAX_IP_BUFFER_LEN] = "\0";
    char flt_info_buf[SERVER_FLT_INFO_BUF_MAX_LEN] = "\0";
    char sess_info_veri_buf[OB_SESS_INFO_VERI_BUF_MAX] = "\0";
    const bool is_last_packet = true;
    const bool is_proxy_switch_route = false;
    if (OB_FAIL(ObProxyTraceUtils::build_related_extra_info_all(extra_info, sm,
                  client_ip_buf, MAX_IP_BUFFER_LEN,
                  flt_info_buf, SERVER_FLT_INFO_BUF_MAX_LEN,
                  sess_info_veri_buf, OB_SESS_INFO_VERI_BUF_MAX,
                  sess_info_value, is_last_packet, is_proxy_switch_route))) {
      LOG_WARN("fail to build related extra info", K(ret));
    } else if (OB_FAIL(ObMysqlOB20PacketWriter::write_request_packet(mio_buf, cmd, sql,
                                                                     server_session->get_server_sessid(),
                                                                     server_session->get_next_server_request_id(),
                                                                     compressed_seq, compressed_seq, true, false, false,
                                                                     server_info.is_new_extra_info_supported(),
                                                                     sm->get_client_session()->is_trans_internal_routing(),
                                                                     is_proxy_switch_route, &extra_info))) {
      LOG_WARN("fail to write request packet in ob20", K(ret));
    } else { /* nothing */ }
  } else {
    const bool need_compress = ob_proxy_protocol == ObProxyProtocol::PROTOCOL_CHECKSUM ? true : false;
    ret = ObMysqlPacketWriter::write_request_packet(mio_buf, cmd, sql, compressed_seq,
                                                    need_compress, server_info.is_checksum_on());
  }

  if (OB_SUCC(ret)) {
    server_session->set_compressed_seq(compressed_seq);
    LOG_DEBUG("will send start trans sql", K(sql));
  } else {
    LOG_WARN("fail to write packet", K(cmd), K(sql), K(ob_proxy_protocol), K(ret));
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
  ObServerSessionInfo &server_info = server_session->get_session_info();
  ObMySQLCmd cmd = OB_MYSQL_COM_STMT_PREPARE_EXECUTE; 
  uint8_t compressed_seq = 0;
  if (ObProxyProtocol::PROTOCOL_OB20 == ob_proxy_protocol) {
    ObSEArray<ObObJKV, 3> extra_info;
    ObSqlString sess_info_value;
    char client_ip_buf[MAX_IP_BUFFER_LEN] = "\0";
    char flt_info_buf[SERVER_FLT_INFO_BUF_MAX_LEN] = "\0";
    char sess_info_veri_buf[OB_SESS_INFO_VERI_BUF_MAX] = "\0";
    const bool is_last_packet = true;
    const bool is_proxy_switch_route = false;
    if (OB_FAIL(ObProxyTraceUtils::build_related_extra_info_all(extra_info, sm,
                  client_ip_buf, MAX_IP_BUFFER_LEN,
                  flt_info_buf, SERVER_FLT_INFO_BUF_MAX_LEN,
                  sess_info_veri_buf, OB_SESS_INFO_VERI_BUF_MAX,
                  sess_info_value, is_last_packet, is_proxy_switch_route))) {
      LOG_WARN("fail to build related extra info", K(ret));
    } else if (OB_FAIL(ObMysqlOB20PacketWriter::write_request_packet(mio_buf, cmd, xa_pkt_payload,
                                                                     server_session->get_server_sessid(),
                                                                     server_session->get_next_server_request_id(),
                                                                     compressed_seq, compressed_seq, true, false, false,
                                                                     server_info.is_new_extra_info_supported(),
                                                                     sm->get_client_session()->is_trans_internal_routing(),
                                                                     is_proxy_switch_route, &extra_info))) {
      LOG_WARN("fail to write request packet in ob20", K(ret));
    } else { /* nothing */ }
  } else {
    const bool need_compress = ob_proxy_protocol == ObProxyProtocol::PROTOCOL_CHECKSUM ? true : false;
    ret = ObMysqlPacketWriter::write_request_packet(mio_buf, cmd, xa_pkt_payload, compressed_seq,
                                                    need_compress, server_info.is_checksum_on());
  }

  if (OB_SUCC(ret)) {
    server_session->set_compressed_seq(compressed_seq);
    LOG_DEBUG("will send to sync xa start", K(xa_pkt_payload));
  } else {
    LOG_WARN("fail to write xa start request to buffer", K(cmd), K(xa_pkt_payload), K(ob_proxy_protocol), K(ret));
  }

  return OB_SUCCESS;
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
  ObServerSessionInfo &server_info = server_session->get_session_info();
  if (OB_FAIL(client_info.get_ps_sql(ps_sql))) {
    LOG_WARN("fail to get ps sql", K(ret));
  } else {
    uint8_t compressed_seq = 0;
    if (ObProxyProtocol::PROTOCOL_OB20 == ob_proxy_protocol) {
        ObSEArray<ObObJKV, 3> extra_info;
        ObSqlString sess_info_value;
        char client_ip_buf[MAX_IP_BUFFER_LEN] = "\0";
        char flt_info_buf[SERVER_FLT_INFO_BUF_MAX_LEN] = "\0";
        char sess_info_veri_buf[OB_SESS_INFO_VERI_BUF_MAX] = "\0";
        const bool is_last_packet = true;
        const bool is_proxy_switch_route = false;
        if (OB_FAIL(ObProxyTraceUtils::build_related_extra_info_all(extra_info, sm,
                      client_ip_buf, MAX_IP_BUFFER_LEN,
                      flt_info_buf, SERVER_FLT_INFO_BUF_MAX_LEN,
                      sess_info_veri_buf, OB_SESS_INFO_VERI_BUF_MAX,
                      sess_info_value, is_last_packet, is_proxy_switch_route))) {
          LOG_WARN("fail to build related extra info", K(ret));
      } else if (OB_FAIL(ObMysqlOB20PacketWriter::write_request_packet(mio_buf, cmd, ps_sql,
                                                                       server_session->get_server_sessid(),
                                                                       server_session->get_next_server_request_id(),
                                                                       compressed_seq, compressed_seq, true, false, false,
                                                                       server_info.is_new_extra_info_supported(), 
                                                                       sm->get_client_session()->is_trans_internal_routing(),
                                                                       is_proxy_switch_route, &extra_info))) {
        LOG_WARN("fail to write request packet in ob20", K(ret));
      } else { /* nothing */ }
    } else {
      const bool need_compress = ob_proxy_protocol == ObProxyProtocol::PROTOCOL_CHECKSUM ? true : false;
      ret = ObMysqlPacketWriter::write_request_packet(mio_buf, cmd, ps_sql, compressed_seq,
                                                      need_compress, server_info.is_checksum_on());
    }

    if (OB_SUCC(ret)) {
      server_session->set_compressed_seq(compressed_seq);
      LOG_DEBUG("will prepare sql", K(ps_sql));
    } else {
      LOG_WARN("fail to write packet", K(cmd), K(ps_sql), K(ob_proxy_protocol), K(ret));
    }
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
  ObServerSessionInfo &server_info = server_session->get_session_info();
  if (OB_FAIL(client_info.get_text_ps_sql(sql))) {
    LOG_WARN("fail to get ps sql", K(ret));
  } else {
    uint8_t compressed_seq = 0;
    if (ObProxyProtocol::PROTOCOL_OB20 == ob_proxy_protocol) {
        ObSEArray<ObObJKV, 3> extra_info;
        ObSqlString sess_info_value;
        char client_ip_buf[MAX_IP_BUFFER_LEN] = "\0";
        char flt_info_buf[SERVER_FLT_INFO_BUF_MAX_LEN] = "\0";
        char sess_info_veri_buf[OB_SESS_INFO_VERI_BUF_MAX] = "\0";
        const bool is_last_packet = true;
        const bool is_proxy_switch_route = false;
        if (OB_FAIL(ObProxyTraceUtils::build_related_extra_info_all(extra_info, sm,
                      client_ip_buf, MAX_IP_BUFFER_LEN,
                      flt_info_buf, SERVER_FLT_INFO_BUF_MAX_LEN,
                      sess_info_veri_buf, OB_SESS_INFO_VERI_BUF_MAX,
                      sess_info_value, is_last_packet, is_proxy_switch_route))) {
          LOG_WARN("fail to build related extra info", K(ret));
      } else if (OB_FAIL(ObMysqlOB20PacketWriter::write_request_packet(mio_buf, cmd, sql,
                                                                       server_session->get_server_sessid(),
                                                                       server_session->get_next_server_request_id(),
                                                                       compressed_seq, compressed_seq, true, false, false,
                                                                       server_info.is_new_extra_info_supported(), 
                                                                       sm->get_client_session()->is_trans_internal_routing(),
                                                                       is_proxy_switch_route, &extra_info))) {
        LOG_WARN("fail to write request packet in ob20", K(ret));
      } else { /* nothing */ }
    } else {
      const bool need_compress = ob_proxy_protocol == ObProxyProtocol::PROTOCOL_CHECKSUM ? true : false;
      ret = ObMysqlPacketWriter::write_request_packet(mio_buf, cmd, sql, compressed_seq,
                                                      need_compress, server_info.is_checksum_on());
    }

    if (OB_SUCC(ret)) {
      server_session->set_compressed_seq(compressed_seq);
      LOG_DEBUG("text ps sql will prepare", K(sql));
    } else {
      LOG_WARN("fal to write packet", K(cmd), K(sql), K(ob_proxy_protocol), K(ret));
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
  tg_hsr.reset_connect_attr();

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
  uint8_t compressed_seq = 0;
  ObServerSessionInfo &server_info = server_session->get_session_info();
  if (ObProxyProtocol::PROTOCOL_OB20 == ob_proxy_protocol) {
    ObSEArray<ObObJKV, 3> extra_info;
    ObSqlString sess_info_value;
    char client_ip_buf[MAX_IP_BUFFER_LEN] = "\0";
    char flt_info_buf[SERVER_FLT_INFO_BUF_MAX_LEN] = "\0";
    char sess_info_veri_buf[OB_SESS_INFO_VERI_BUF_MAX] = "\0";
    const bool is_last_packet = true;
    const bool is_proxy_switch_route = false;
    if (OB_FAIL(ObProxyTraceUtils::build_related_extra_info_all(extra_info, sm,
            client_ip_buf, MAX_IP_BUFFER_LEN,
            flt_info_buf, SERVER_FLT_INFO_BUF_MAX_LEN,
            sess_info_veri_buf, OB_SESS_INFO_VERI_BUF_MAX,
            sess_info_value, is_last_packet, is_proxy_switch_route))) {
      LOG_WARN("fail to build related extra info", K(ret));
    } else if (OB_FAIL(ObMysqlOB20PacketWriter::write_request_packet(mio_buf, cmd, client_info.get_init_sql(),
            server_session->get_server_sessid(),
            server_session->get_next_server_request_id(),
            compressed_seq, compressed_seq, true, false, false,
            server_info.is_new_extra_info_supported(), 
            sm->get_client_session()->is_trans_internal_routing(),
            is_proxy_switch_route, &extra_info))) {
      LOG_WARN("fail to write request packet in ob20", K(ret));
    } else { /* nothing */ }
  } else {
    ObServerSessionInfo &server_info = server_session->get_session_info();
    const bool need_compress = ob_proxy_protocol == ObProxyProtocol::PROTOCOL_CHECKSUM ? true : false;
    ret = ObMysqlPacketWriter::write_request_packet(mio_buf, cmd, client_info.get_init_sql(), compressed_seq,
        need_compress, server_info.is_checksum_on());
  }

  if (OB_SUCC(ret)) {
    server_session->set_compressed_seq(compressed_seq);
    LOG_DEBUG("will send init sql");
  } else {
    LOG_WARN("fal to write packet", K(cmd), K(ob_proxy_protocol), K(ret));
  }

  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
