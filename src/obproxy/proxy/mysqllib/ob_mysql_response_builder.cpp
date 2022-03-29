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
#include "packet/ob_proxy_cached_packets.h"
#include "packet/ob_mysql_packet_writer.h"
#include "packet/ob_mysql_packet_util.h"
#include "proxy/mysqllib/ob_proxy_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::sql;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::packet;


namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
const ObString ObMysqlResponseBuilder::OBPROXY_ROUTE_ADDR_NAME = "@obproxy_route_addr";
const ObString ObMysqlResponseBuilder::OBPROXY_PROXY_VERSION_NAME = "proxy_version()";

int ObMysqlResponseBuilder::build_ok_resp(ObMIOBuffer &mio_buf,
                                          ObProxyMysqlRequest &client_request,
                                          ObClientSessionInfo &info,
                                          const bool is_in_trans,
                                          const bool is_state_changed)
{
  int ret = OB_SUCCESS;

  ObProxyOKPacket *ok_packet = NULL;
  if (OB_FAIL(ObProxyCachedPackets::get_ok_packet(ok_packet, OB_OK_INTERNAL))) {
    LOG_WARN("fail to get thread cache ok packet", K(ret));
  } else {
    const uint8_t seq = static_cast<uint8_t>(client_request.get_packet_meta().pkt_seq_ + 1);
    ok_packet->set_seq(seq);

    // default set of ok packet
    // 0x22 means OB_SERVER_STATUS_AUTOCOMMIT, OB_SERVER_STATUS_NO_INDEX_USED is set
    int64_t autocommit = info.get_cached_variables().get_autocommit();
    ObServerStatusFlags ssf(0x22);
    if (is_in_trans) {
      ssf.status_flags_.OB_SERVER_STATUS_IN_TRANS = 1;
    }
    if (is_state_changed) {
      ssf.status_flags_.OB_SERVER_SESSION_STATE_CHANGED = 1;
    }
    if (0 != autocommit) {
      ssf.status_flags_.OB_SERVER_STATUS_AUTOCOMMIT = 1;
    }
    //ok_packet.set_server_status(ssf.status_flags_);
    ok_packet->set_status_flags(ssf.flags_);

    ObString pkt_str;
    if (OB_FAIL(ok_packet->get_packet_str(pkt_str))) {
      LOG_WARN("fail to get ok packet str", K(ret));
    } else if (OB_FAIL(ObMysqlPacketWriter::write_raw_packet(mio_buf, pkt_str))) {
      LOG_WARN("fail to write ok packet", K(ret));
    }
  }

  return ret;
}

int ObMysqlResponseBuilder::build_select_tx_ro_resp(ObMIOBuffer &mio_buf,
                                                    ObProxyMysqlRequest &client_request,
                                                    ObClientSessionInfo &info,
                                                    const bool is_in_trans)
{
  int ret = OB_SUCCESS;

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
  if (OB_FAIL(ObMysqlPacketUtil::encode_kv_resultset(mio_buf, seq,
                                                     field, field_value, status_flag))) {
    LOG_WARN("fail to encode kv resultset", K(seq), K(ret));
  }
  return ret;
}

int ObMysqlResponseBuilder::build_select_route_addr_resp(ObMIOBuffer &mio_buf,
                                                         ObProxyMysqlRequest &client_request,
                                                         ObClientSessionInfo &info,
                                                         const bool is_in_trans,
                                                         int64_t addr)
{
  int ret = OB_SUCCESS;

  // get seq
  uint8_t seq = static_cast<uint8_t>(client_request.get_packet_meta().pkt_seq_ + 1);

  // get field
  ObMySQLField field;
  field.cname_ = OBPROXY_ROUTE_ADDR_NAME;
  field.org_cname_ = OBPROXY_ROUTE_ADDR_NAME;
  field.type_ = OB_MYSQL_TYPE_LONGLONG;
  field.charsetnr_ = CS_TYPE_BINARY;
  field.flags_ = OB_MYSQL_BINARY_FLAG;

  // get filed value
  ObObj field_value;
  field_value.set_int(addr);

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

int ObMysqlResponseBuilder::build_select_proxy_version_resp(ObMIOBuffer &mio_buf,
                                                            ObProxyMysqlRequest &client_request,
                                                            ObClientSessionInfo &info,
                                                            const bool is_in_trans)
{
  int ret = OB_SUCCESS;

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
  if (OB_FAIL(ObMysqlPacketUtil::encode_kv_resultset(mio_buf, seq,
                                                     field, field_value, status_flag))) {
    LOG_WARN("fail to encode kv resultset", K(seq), K(ret));
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
