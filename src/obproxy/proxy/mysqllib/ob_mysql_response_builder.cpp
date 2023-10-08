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
#include "proxy/mysqllib/ob_mysql_response_builder.h"
#include "rpc/obmysql/packet/ompk_ok.h"
#include "rpc/obmysql/packet/ompk_prepare_execute.h"
#include "rpc/obmysql/packet/ompk_prepare_execute_req.h"
#include "rpc/obmysql/packet/ompk_eof.h"
#include "packet/ob_proxy_cached_packets.h"
#include "packet/ob_mysql_packet_writer.h"
#include "packet/ob_mysql_packet_util.h"
#include "proxy/mysqllib/ob_proxy_session_info.h"
#include "obproxy/packet/ob_proxy_packet_writer.h"
#include "obproxy/proxy/mysql/ob_mysql_client_session.h"
#include "stat/ob_net_stats.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::sql;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::packet;
using namespace oceanbase::obproxy::net;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
const ObString ObMysqlResponseBuilder::OBPROXY_ROUTE_ADDR_NAME = "@obproxy_route_addr";
const ObString ObMysqlResponseBuilder::OBPROXY_PROXY_VERSION_NAME = "proxy_version()";
const ObString ObMysqlResponseBuilder::OBPROXY_PROXY_STATUS_NAME = "proxy_status";

int ObMysqlResponseBuilder::build_ok_resp(ObMIOBuffer &mio_buf,
                                          ObProxyMysqlRequest &client_request,
                                          ObMysqlClientSession &client_session,
                                          const ObProxyProtocol protocol,
                                          const bool is_in_trans,
                                          const bool is_state_changed)
{
  int ret = OB_SUCCESS;

  ObProxyOKPacket *ok_packet = NULL;
  if (OB_FAIL(ObProxyCachedPackets::get_ok_packet(ok_packet, OB_OK_INTERNAL))) {
    LOG_WARN("fail to get thread cache ok packet", K(ret));
  } else {
    uint8_t seq = static_cast<uint8_t>(client_request.get_packet_meta().pkt_seq_ + 1);
    ok_packet->set_seq(seq);

    // default set of ok packet
    // 0x22 means OB_SERVER_STATUS_AUTOCOMMIT, OB_SERVER_STATUS_NO_INDEX_USED is set
    int64_t autocommit = client_session.get_session_info().get_cached_variables().get_autocommit();
    ObServerStatusFlags ssf(0x22);
    if (is_in_trans) {
      ssf.status_flags_.OB_SERVER_STATUS_IN_TRANS = 1;
    }
    if (is_state_changed) {
      ssf.status_flags_.OB_SERVER_SESSION_STATE_CHANGED = 1;
    }
    if (0 == autocommit) {
      ssf.status_flags_.OB_SERVER_STATUS_AUTOCOMMIT = 0;
    }
    //ok_packet.set_server_status(ssf.status_flags_);
    ok_packet->set_status_flags(ssf.flags_);

    ObString pkt_str;
    if (OB_FAIL(ok_packet->get_packet_str(pkt_str))) {
      LOG_WARN("fail to get ok packet str", K(ret));
    } else if (OB_FAIL(ObProxyPacketWriter::write_raw_packet(mio_buf, client_session, protocol, pkt_str))) {
      LOG_WARN("fail to write packet", K(ret));
    }
  }

  return ret;
}

/**
 * @brief build OB_MYSQL_COM_STMT_PREPARE_EXECUTE response of XA_START request
 *  OB_MYSQL_COM_STMT_PREPARE_EXECUTE
 *  struct of OB_MYSQL_COM_STMT_PREPARE_EXECUTE RESPONSE:
 *  Prepare
 *  param_num > 0 ? ColDef * param_num
 *  Eof
 *  col_num > 0 ? ColDef * col_num
 *  Eof
 *  Row
 *  Eof
 *  Ok
 * 
 * @param mio_buf 
 * @param client_request 
 * @param info 
 * @return int 
 */
int ObMysqlResponseBuilder::build_prepare_execute_xa_start_resp(ObMIOBuffer &mio_buf,
                                                                ObProxyMysqlRequest &client_request,
                                                                ObMysqlClientSession &client_session,
                                                                const ObProxyProtocol protocol)
{
  int ret = OB_SUCCESS;
  ObClientSessionInfo &info = client_session.get_session_info();
  OMPKPrepareExecuteReq prepare_req_pkt;
  ObIOBufferReader *tmp_mio_reader = NULL;
  ObMIOBuffer *tmp_mio_buf = &mio_buf;
  
  if (protocol == ObProxyProtocol::PROTOCOL_OB20) {
    if (OB_ISNULL(tmp_mio_buf = new_empty_miobuffer())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new miobuffer", K(ret));
    } else if (OB_ISNULL(tmp_mio_reader = tmp_mio_buf->alloc_reader())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to alloc reader", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    // decode the prepare execute request
    prepare_req_pkt.set_content(client_request.get_req_pkt().ptr() + MYSQL_NET_HEADER_LENGTH,
                                static_cast<uint32_t>(client_request.get_packet_len()));
    if (OB_FAIL(prepare_req_pkt.decode())) {
      LOG_WARN("fail to decode the xa start prepare execute", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    // get seq
    uint8_t seq = static_cast<uint8_t>(client_request.get_packet_meta().pkt_seq_ + 1);
    uint32_t sid = info.get_client_ps_id();
    int64_t autocommit = info.get_cached_variables().get_autocommit();
    // write prepare execute resp 
    OMPKPrepareExecute prepare_pkt;
    prepare_pkt.set_seq(seq++);
    prepare_pkt.set_status(0);
    prepare_pkt.set_statement_id(sid);
    prepare_pkt.set_column_num(1);
    prepare_pkt.set_param_num(static_cast<uint16_t>(prepare_req_pkt.get_param_num()));
    prepare_pkt.set_warning_count(0);
    prepare_pkt.set_extend_flag(0);
    prepare_pkt.set_has_result_set(1);
    if (OB_FAIL(ObMysqlPacketWriter::write_packet(*tmp_mio_buf, prepare_pkt))) {
      seq--;
      LOG_WARN("fail to write prepare pkt of resp of xa start hold", K(ret));
    }
    // write params Coldef 
    for (int32_t i = 0; OB_SUCC(ret) && i < prepare_req_pkt.get_param_num(); i++) {
      ObMySQLField param_field;
      param_field.cname_ = "?";
      param_field.type_ = prepare_req_pkt.get_param_types().at(i);
      param_field.type_info_ = prepare_req_pkt.get_type_infos().at(i);
      param_field.flags_ = 0;
      param_field.charsetnr_ = static_cast<uint16_t>(info.get_collation_connection());
      param_field.length_ = 0;
      if (OB_FAIL(ObMysqlPacketUtil::encode_field_packet(*tmp_mio_buf, seq, param_field))) {
        LOG_WARN("fail to write field pkt of resp of xa start hold", K(ret), K(param_field));
      }
    }
    // write Eof
    if (prepare_req_pkt.get_param_num() > 0 && OB_SUCC(ret)) {
      uint16_t status_flag_0 = 0;
      status_flag_0 |= (1 << OB_SERVER_STATUS_IN_TRANS_POS);
      if (0 != autocommit) {
        status_flag_0 |= (1 << OB_SERVER_STATUS_AUTOCOMMIT_POS);
      }
      if (OB_FAIL(ObMysqlPacketUtil::encode_eof_packet(*tmp_mio_buf, seq, status_flag_0))){
        LOG_WARN("fail to write eof pkt of resp of xa start hold", K(ret));
      }
    }    
    // ColDef: row field
    if (OB_SUCC(ret)) {
    ObMySQLField row_col_field;
    row_col_field.cname_ = client_request.get_parse_sql();
    row_col_field.org_cname_ = client_request.get_parse_sql();
    row_col_field.type_ = OB_MYSQL_TYPE_LONG;
    row_col_field.flags_ = OB_MYSQL_BINARY_FLAG;
    row_col_field.charsetnr_ = CS_TYPE_BINARY;
    row_col_field.length_ = 0;
    if (OB_FAIL(ObMysqlPacketUtil::encode_field_packet(*tmp_mio_buf, seq, row_col_field))) {
        LOG_WARN("fail to write field pkt of resp of xa start hold", K(ret));
      }
    }
    // Eof
    if (OB_SUCC(ret)) {
      uint16_t status_flag_1 = 0;
      if (0 != autocommit) {
        status_flag_1 |= (1 << OB_SERVER_STATUS_AUTOCOMMIT_POS);
      }
      status_flag_1 |= (1 << OB_SERVER_STATUS_IN_TRANS_POS);
      status_flag_1 |= (1 << OB_SERVER_STATUS_CURSOR_EXISTS_POS);
      if (OB_FAIL(ObMysqlPacketUtil::encode_eof_packet(*tmp_mio_buf, seq, status_flag_1))) {
        LOG_WARN("fail to write eof pkt of resp of xa start hold", K(ret));
      }
    }
    // Row
    if (OB_SUCC(ret)) {
      ObObj field_value;
      field_value.set_int32(0);
      ObNewRow row;
      row.cells_ = &field_value;
      row.count_ = 1;
      if (ObMysqlPacketUtil::encode_row_packet(*tmp_mio_buf, BINARY, seq, row)) {
        LOG_WARN("fail to write row pkt", K(ret));
      }
    }
    // write Eof
    if (OB_SUCC(ret)) {
      uint16_t status_flag_2 = 0;
      if (0 != autocommit) {
        status_flag_2 |= (1 << OB_SERVER_STATUS_AUTOCOMMIT_POS);
      }
      status_flag_2 |= (1 << OB_SERVER_STATUS_IN_TRANS_POS);
      status_flag_2 |= (1 << OB_SERVER_STATUS_LAST_ROW_SENT_POS);
      if (OB_FAIL(ObMysqlPacketUtil::encode_eof_packet(*tmp_mio_buf, seq, status_flag_2))) {
        LOG_WARN("fail to write eof pkt of resp of xa start hold", K(ret));
      }
    }
    // Ok
    if (OB_SUCC(ret)) {
      uint16_t status_flag_3 = 0;
      if (0 != autocommit) {
        status_flag_3 |= (1 << OB_SERVER_STATUS_AUTOCOMMIT_POS);
      }
      status_flag_3 |= (1 << OB_SERVER_STATUS_IN_TRANS_POS);
      status_flag_3 |= (1 << OB_SERVER_STATUS_NO_INDEX_USED_POS);
      status_flag_3 |= (1 << OB_SERVER_STATUS_CURSOR_EXISTS_POS);
      status_flag_3 |= (1 << OB_SERVER_STATUS_LAST_ROW_SENT_POS);
      status_flag_3 |= (1 << OB_SERVER_SESSION_STATE_CHANGED_POS);
      if (OB_FAIL(ObMysqlPacketUtil::encode_ok_packet(*tmp_mio_buf, seq, 0, info.get_orig_capability_flags(), status_flag_3))) {
        LOG_WARN("fail to write last ok pkt of resp of xa start hold", K(ret));
      } else {
        LOG_DEBUG("succ to write prepare execute packets of resp of xa start hold");
      }
    } else {
      LOG_WARN("fail to write prepare execute packets of resp of xa start hold", K(ret));
    }

    if (OB_SUCC(ret) && protocol == ObProxyProtocol::PROTOCOL_OB20) {
      Ob20ProtocolHeader &ob20_head = client_session.get_session_info().ob20_request_.ob20_header_;
      uint8_t compressed_seq = static_cast<uint8_t>(client_session.get_compressed_seq() + 1);
      Ob20ProtocolHeaderParam ob20_head_param(client_session.get_cs_id(), ob20_head.request_id_, compressed_seq,
                                              compressed_seq, true, false, false,
                                              client_session.is_client_support_new_extra_info(),
                                              client_session.is_trans_internal_routing(), false);
      if (OB_FAIL(ObProto20Utils::consume_and_compress_data(tmp_mio_reader, &mio_buf,
                                                            tmp_mio_reader->read_avail(), ob20_head_param))) {
        LOG_WARN("fail to consume and compress data for executor response packet in ob20", K(ret));
      } else {
        LOG_DEBUG("succ to executor response in ob20 packet");
      }
    }
  } 
  return ret;
}

int ObMysqlResponseBuilder::build_select_tx_ro_resp(ObMIOBuffer &mio_buf,
                                                    ObProxyMysqlRequest &client_request,
                                                    ObMysqlClientSession &client_session,
                                                    const ObProxyProtocol protocol,
                                                    const bool is_in_trans)
{
  int ret = OB_SUCCESS;
  ObClientSessionInfo &info = client_session.get_session_info();
  
  // get seq
  uint8_t seq = static_cast<uint8_t>(client_request.get_packet_meta().pkt_seq_ + 1);

  // get field
  ObMySQLField field;
  field.cname_ = client_request.get_parse_result().get_col_name();
  field.org_cname_ = client_request.get_parse_result().get_col_name();
  field.type_ = OB_MYSQL_TYPE_LONGLONG;
  field.charsetnr_ = CS_TYPE_BINARY;
  field.flags_ = OB_MYSQL_BINARY_FLAG;

  // get filed value
  ObObj field_value;
  field_value.set_varchar(info.get_cached_variables().get_tx_read_only_str());

  // get status flag
  uint16_t status_flag = 0;
  int64_t autocommit = info.get_cached_variables().get_autocommit();
  if (0 != autocommit) {
    status_flag |= (1 << OB_SERVER_STATUS_AUTOCOMMIT_POS);
  }
  if (is_in_trans) {
    status_flag |= (1 << OB_SERVER_STATUS_IN_TRANS_POS);
  }

  // encode to mio_buf
  if (OB_FAIL(ObProxyPacketWriter::write_kv_resultset(mio_buf, client_session, protocol, seq,
                                                      field, field_value, status_flag))) {
    LOG_WARN("fail to write kv resultset", K(ret));
  }
  
  return ret;
}

int ObMysqlResponseBuilder::build_explain_route_resp(ObMIOBuffer &mio_buf,
                                                    ObProxyMysqlRequest &client_request,
                                                    ObMysqlClientSession &client_session,
                                                    ObRouteDiagnosis *diagnosis,
                                                    const ObProxyProtocol protocol,
                                                    const bool is_in_trans)
{
  int ret = OB_SUCCESS;
  ObClientSessionInfo &info = client_session.get_session_info();
  
  // get seq
  uint8_t seq = static_cast<uint8_t>(client_request.get_packet_meta().pkt_seq_ + 1);

  // fill field
  ObMySQLField field;
  field.cname_ = "Route Plan";
  field.type_ = OB_MYSQL_TYPE_LONGLONG;
  field.charsetnr_ = OB_MYSQL_TYPE_VARCHAR;
  field.flags_ = OB_MYSQL_BINARY_FLAG;

  // fill filed value
  ObObj route_plan;
  char *plan_str = NULL;
  static char disable[] = "Route diagnosis disabled\0";
  static char not_support[] = "Route diagnosis not supports this query\0";
  int64_t size = 1L << 20;
  if (OB_NOT_NULL(diagnosis)) {
    if (diagnosis->get_level() == 0) {
      plan_str = disable;
    } else if (!diagnosis->is_support_explain_route()) {
      plan_str = not_support;
    } else if (OB_ISNULL(plan_str = (char*) op_fixed_mem_alloc(size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem for route plan", K(size), K(ret));
    } else {
      diagnosis->to_format_string(plan_str, size);
    }
  } else {
    plan_str = disable;
  }
  
  if (OB_SUCC(ret)) {
    route_plan.set_varchar(plan_str, (int) strlen(plan_str));
    // get status flag
    uint16_t status_flag = 0;
    int64_t autocommit = info.get_cached_variables().get_autocommit();
    if (0 != autocommit) {
      status_flag |= (1 << OB_SERVER_STATUS_AUTOCOMMIT_POS);
    }
    if (is_in_trans) {
      status_flag |= (1 << OB_SERVER_STATUS_IN_TRANS_POS);
    }

    // encode to mio_buf
    if (OB_FAIL(ObProxyPacketWriter::write_kv_resultset(mio_buf, client_session, protocol, seq,
                                                        field, route_plan, status_flag))) {
      LOG_WARN("fail to write kv resultset", K(ret));
    }
  }

  if (OB_NOT_NULL(plan_str) && 
      plan_str != disable &&
      plan_str != not_support) {
    op_fixed_mem_free(plan_str, size);
  }

  return ret;
}

int ObMysqlResponseBuilder::build_select_route_addr_resp(ObMIOBuffer &mio_buf,
                                                         ObProxyMysqlRequest &client_request,
                                                         ObMysqlClientSession &client_session,
                                                         const ObProxyProtocol protocol,
                                                         const bool is_in_trans,
                                                         const struct sockaddr &addr)
{
  int ret = OB_SUCCESS;
  ObClientSessionInfo &info = client_session.get_session_info();

  // get seq
  uint8_t seq = static_cast<uint8_t>(client_request.get_packet_meta().pkt_seq_ + 1);

  // get field
  ObMySQLField field;
  field.cname_ = OBPROXY_ROUTE_ADDR_NAME;
  field.org_cname_ = OBPROXY_ROUTE_ADDR_NAME;
  field.type_ = OB_MYSQL_TYPE_VARCHAR;
  field.charsetnr_ = CS_TYPE_BINARY;
  field.flags_ = OB_MYSQL_BINARY_FLAG;

  // get filed value
  ObObj field_value;
  char buf[MAX_IP_ADDR_LENGTH];
  memset(buf, 0, sizeof(buf));
  net::ops_ip_ntop(addr, buf, MAX_IP_ADDR_LENGTH);
  field_value.set_varchar(buf);

  // get status flag
  uint16_t status_flag = 0;
  int64_t autocommit = info.get_cached_variables().get_autocommit();
  if (0 != autocommit) {
    status_flag |= (1 << OB_SERVER_STATUS_AUTOCOMMIT_POS);
  }
  if (is_in_trans) {
    status_flag |= (1 << OB_SERVER_STATUS_IN_TRANS_POS);
  }

  // encode to mio_buf
  if (OB_FAIL(ObProxyPacketWriter::write_kv_resultset(mio_buf, client_session, protocol, seq,
                                                      field, field_value, status_flag))) {
    LOG_WARN("fail to write kv resultset", K(ret));
  }
  
  return ret;
}

int ObMysqlResponseBuilder::build_select_proxy_version_resp(ObMIOBuffer &mio_buf,
                                                            ObProxyMysqlRequest &client_request,
                                                            ObMysqlClientSession &client_session,
                                                            const ObProxyProtocol protocol,
                                                            const bool is_in_trans)
{
  int ret = OB_SUCCESS;
  ObClientSessionInfo &info = client_session.get_session_info();

  // get seq
  uint8_t seq = static_cast<uint8_t>(client_request.get_packet_meta().pkt_seq_ + 1);

  // get field
  ObMySQLField field;
  if (client_request.get_parse_result().get_col_name().empty()) {
    field.cname_ = OBPROXY_PROXY_VERSION_NAME;
    field.org_cname_ = OBPROXY_PROXY_VERSION_NAME;
  } else {
    field.cname_ = client_request.get_parse_result().get_col_name();
    field.org_cname_ = OBPROXY_PROXY_VERSION_NAME;
  }
  field.type_ = OB_MYSQL_TYPE_VARCHAR;
  field.charsetnr_ = CS_TYPE_BINARY;
  field.flags_ = OB_MYSQL_BINARY_FLAG;

  // get filed value
  ObObj field_value;
  field_value.set_varchar(PACKAGE_VERSION);

  // get status flag
  uint16_t status_flag = 0;
  int64_t autocommit = info.get_cached_variables().get_autocommit();
  if (0 != autocommit) {
    status_flag |= (1 << OB_SERVER_STATUS_AUTOCOMMIT_POS);
  }
  if (is_in_trans) {
    status_flag |= (1 << OB_SERVER_STATUS_IN_TRANS_POS);
  }

  // encode to mio_buf
  if (OB_FAIL(ObProxyPacketWriter::write_kv_resultset(mio_buf, client_session, protocol,
                                                      seq, field, field_value, status_flag))) {
    LOG_WARN("fail to write kv resultset", K(ret));
  }

  return ret;
}

int ObMysqlResponseBuilder::build_select_proxy_status_resp(ObMIOBuffer &mio_buf,
                                                            ObProxyMysqlRequest &client_request,
                                                            ObClientSessionInfo &info,
                                                            const bool is_in_trans)
{
  int ret = OB_SUCCESS;

  // get seq
  uint8_t seq = static_cast<uint8_t>(client_request.get_packet_meta().pkt_seq_ + 1);

  // get field
  ObMySQLField field;
  field.cname_ = OBPROXY_PROXY_STATUS_NAME;
  field.org_cname_ = OBPROXY_PROXY_STATUS_NAME;
  field.type_ = OB_MYSQL_TYPE_VARCHAR;
  field.charsetnr_ = CS_TYPE_BINARY;
  field.flags_ = OB_MYSQL_BINARY_FLAG;

  // get filed value
  ObString rolling_upgrade_state;
  const ObHotUpgraderInfo& hot_upgrade_info = get_global_hot_upgrade_info();
  if (OB_LIKELY(hot_upgrade_info.is_active_for_rolling_upgrade_)) {
    rolling_upgrade_state = "ACTIVE";
  } else {
    bool timeout = hot_upgrade_info.is_graceful_offline_timeout(get_hrtime());
    int64_t global_connections = 0;
    NET_READ_GLOBAL_DYN_SUM(NET_GLOBAL_CLIENT_CONNECTIONS_CURRENTLY_OPEN, global_connections);
    if ((global_connections <= 1) || timeout) {
      rolling_upgrade_state = "INACTIVE";
    } else {
      rolling_upgrade_state = "BE_INACTIVE";
    }
  }
  ObObj field_value;
  field_value.set_varchar(rolling_upgrade_state);

  // get status flag
  uint16_t status_flag = 0;
  int64_t autocommit = info.get_cached_variables().get_autocommit();
  if (0 != autocommit) {
    status_flag |= (1 << OB_SERVER_STATUS_AUTOCOMMIT_POS);
  }
  if (is_in_trans) {
    status_flag |= (1 << OB_SERVER_STATUS_IN_TRANS_POS);
  }

  // encode to mio_buf
  if (OB_FAIL(ObMysqlPacketUtil::encode_kv_resultset(mio_buf, seq,
                                                     field, field_value, status_flag))) {
    LOG_WARN("fail to encode kv resultset", K(seq), K(ret));
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
