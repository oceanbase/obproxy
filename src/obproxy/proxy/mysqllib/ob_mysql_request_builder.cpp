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

int ObMysqlRequestBuilder::build_request_packet(ObString sql,
                                                ObMySQLCmd cmd,
                                                ObMysqlSM *sm,
                                                ObMIOBuffer &mio_buf,
                                                ObMysqlServerSession *server_session,
                                                const ObProxyProtocol ob_proxy_protocol)
{
  int ret = OB_SUCCESS;
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
    const int64_t compression_level = sm->compression_algorithm_.level_;
    const bool is_compressed_ob20 = (server_info.is_server_ob20_compress_supported() && compression_level !=0);
    Ob20HeaderParam ob20_head_param(server_session->get_server_sessid(), server_session->get_next_server_request_id(),
                                            compressed_seq, compressed_seq, is_last_packet, /* is_weak_read */ false,
                                            /* is_need_reroute */ false, server_info.is_new_extra_info_supported(),
                                            sm->get_client_session()->is_trans_internal_routing(), is_proxy_switch_route,
                                            is_compressed_ob20, compression_level);
    INC_SHARED_REF(ob20_head_param.get_protocol_diagnosis_ref(), sm->protocol_diagnosis_);
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
      compressed_seq = ob20_head_param.get_compressed_seq();
    }
  } else {
    const bool need_compress = ob_proxy_protocol == ObProxyProtocol::PROTOCOL_CHECKSUM ? true : false;
    ObCmpHeaderParam param(compressed_seq, server_info.is_checksum_on(), sm->compression_algorithm_.level_);
    INC_SHARED_REF(param.get_protocol_diagnosis_ref(), sm->protocol_diagnosis_);
    if (OB_FAIL(ObMysqlPacketWriter::write_request_packet(mio_buf, cmd, sql, need_compress, param))) {
      LOG_WDIAG("fail to write request packet in mysql/compressed mysql", K(ob_proxy_protocol), K(ret));
    } else {
      compressed_seq = param.get_compressed_seq();
    }
  }

  if (OB_SUCC(ret)) {
    server_session->set_compressed_seq(compressed_seq);
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
  ObMySQLCmd cmd = OB_MYSQL_COM_STMT_PREPARE_EXECUTE;
   if (OB_FAIL(build_request_packet(xa_pkt_payload, cmd, sm, mio_buf, server_session, ob_proxy_protocol))) {
    LOG_WDIAG("fail to build xa start packet", K(xa_pkt_payload), K(cmd), K(ret));
  } else {
    LOG_DEBUG("will sync xa start", K(xa_pkt_payload), K(cmd));
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
  if (OB_FAIL(build_request_packet(client_info.get_init_sql(), cmd, sm, mio_buf, server_session, ob_proxy_protocol))) {
    LOG_WDIAG("fail to build init sql packet", K(client_info.get_init_sql()), K(cmd), K(ret));
  } else {
    LOG_DEBUG("will sync init sql", K(client_info.get_init_sql()), K(cmd));
  }

  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
