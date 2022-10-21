/**
 * Copyright (c) 2022 OceanBase
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

#include "lib/oblog/ob_log.h"
#include "obproxy/packet/ob_proxy_packet_writer.h"
#include "obproxy/packet/ob_mysql_packet_writer.h"
#include "obproxy/proxy/mysqllib/ob_proxy_session_info.h"
#include "obproxy/proxy/mysqllib/ob_2_0_protocol_struct.h"
#include "obproxy/proxy/mysqllib/ob_mysql_ob20_packet_write.h"
#include "rpc/obmysql/ob_mysql_field.h"
#include "obproxy/iocore/eventsystem/ob_io_buffer.h"
#include "lib/utility/ob_macro_utils.h"
#include "obproxy/proxy/mysqllib/ob_2_0_protocol_utils.h"
#include "obproxy/proxy/mysql/ob_mysql_client_session.h"


using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace packet
{

int ObProxyPacketWriter::write_packet(event::ObMIOBuffer &write_buf, ObMysqlClientSession &client_session,
                                      obmysql::ObMySQLPacket &packet)
{
  int ret = OB_SUCCESS;
  ObProxyProtocol protocol = client_session.get_client_session_protocol();
    
  if (protocol == ObProxyProtocol::PROTOCOL_OB20) {
    Ob20ProtocolHeader &ob20_head = client_session.get_session_info().ob20_request_.ob20_header_;
    uint8_t compressed_seq = static_cast<uint8_t>(client_session.get_compressed_seq() + 1);
    Ob20ProtocolHeaderParam ob20_head_param(client_session.get_cs_id(), ob20_head.request_id_, compressed_seq,
                                            compressed_seq, true, false,
                                            client_session.is_client_support_new_extra_info());
    if (OB_FAIL(ObMysqlOB20PacketWriter::write_packet(write_buf, packet, ob20_head_param))) {
      LOG_WARN("fail to write ob20 packet", K(ret));
    } else {
      client_session.set_compressed_seq(compressed_seq);
    }
  } else {
    if (OB_FAIL(ObMysqlPacketWriter::write_packet(write_buf, packet))) {
      LOG_WARN("fail to write mysql packet", K(ret));
    }
  }

  return ret;
}

int ObProxyPacketWriter::write_raw_packet(event::ObMIOBuffer &write_buf, ObMysqlClientSession &client_session,
                                          ObString &pkt_str)
{
  int ret = OB_SUCCESS;
  ObProxyProtocol client_protocol = client_session.get_client_session_protocol();

  if (client_protocol == ObProxyProtocol::PROTOCOL_OB20) {
    Ob20ProtocolHeader &ob20_head = client_session.get_session_info().ob20_request_.ob20_header_;
    uint8_t compressed_seq = static_cast<uint8_t>(client_session.get_compressed_seq() + 1);
    Ob20ProtocolHeaderParam ob20_head_param(client_session.get_cs_id(), ob20_head.request_id_,
                                            compressed_seq, compressed_seq, true, false,
                                            client_session.is_client_support_new_extra_info());
    if (OB_FAIL(ObMysqlOB20PacketWriter::write_raw_packet(write_buf, pkt_str, ob20_head_param))) {
      LOG_WARN("fail to write raw packet in ob20 format", K(ret));
    } else {
       client_session.set_compressed_seq(compressed_seq);
    }
  } else {
    if (OB_FAIL(ObMysqlPacketWriter::write_raw_packet(write_buf, pkt_str))) {
      LOG_WARN("fail to write raw ok packet", K(ret));
    }
  }

  return ret;
}

int ObProxyPacketWriter::write_kv_resultset(event::ObMIOBuffer &write_buf, ObMysqlClientSession &client_session,
                                            uint8_t &seq, const obmysql::ObMySQLField &field, ObObj &field_value,
                                            const uint16_t status_flag)
{
  int ret = OB_SUCCESS;
  ObProxyProtocol client_protocol = client_session.get_client_session_protocol();

  if (client_protocol == ObProxyProtocol::PROTOCOL_OB20) {
    Ob20ProtocolHeader &ob20_head = client_session.get_session_info().ob20_request_.ob20_header_;
    uint8_t compressed_seq = static_cast<uint8_t>(client_session.get_compressed_seq() + 1);
    Ob20ProtocolHeaderParam ob20_head_param(client_session.get_cs_id(), ob20_head.request_id_, compressed_seq,
                                            compressed_seq, true, false,
                                            client_session.is_client_support_new_extra_info());
    if (OB_FAIL(ObProto20Utils::encode_kv_resultset(write_buf, ob20_head_param, seq, field,
                                                    field_value, status_flag))) {
      LOG_WARN("fail to encode kv resultset", K(ret));
    }    
  } else {
    // mysql
    if (OB_FAIL(ObMysqlPacketUtil::encode_kv_resultset(write_buf, seq, field, field_value, status_flag))) {
      LOG_WARN("fail to encode kv resultset", K(seq), K(ret));
    }
  }

  return ret;
}

int ObProxyPacketWriter::write_ok_packet(event::ObMIOBuffer &write_buf, ObMysqlClientSession &client_session,
                                         uint8_t &seq, const int64_t affected_rows,
                                         const obmysql::ObMySQLCapabilityFlags &capability)
{
  int ret = OB_SUCCESS;
  ObProxyProtocol protocol = client_session.get_client_session_protocol();

  if (protocol == ObProxyProtocol::PROTOCOL_OB20) {
    Ob20ProtocolHeader &ob20_head = client_session.get_session_info().ob20_request_.ob20_header_;
    uint8_t compressed_seq = static_cast<uint8_t>(client_session.get_compressed_seq() + 1);
    Ob20ProtocolHeaderParam ob20_head_param(client_session.get_cs_id(), ob20_head.request_id_, compressed_seq,
                                            compressed_seq, true, false,
                                            client_session.is_client_support_new_extra_info());
    if (OB_FAIL(ObProto20Utils::encode_ok_packet(write_buf, ob20_head_param, seq, affected_rows, capability))) {
      LOG_WARN("fail to encode ok packet in ob20 format", K(ret));
    } else {
      client_session.set_compressed_seq(compressed_seq);
    }
  } else {
    if (OB_FAIL(ObMysqlPacketUtil::encode_ok_packet(write_buf, seq, affected_rows, capability))) {
      LOG_WARN("[ObMysqlSM::do_internal_request] fail to encode OB_MYSQL_COM_PING response ok packet", K(ret));
    }
  }

  return ret;
}

int ObProxyPacketWriter::write_executor_resp_packet(event::ObMIOBuffer *write_buf,
                                                    ObMysqlClientSession *client_session, uint8_t &seq,
                                                    engine::ObProxyResultResp *result_resp)
{
  int ret = OB_SUCCESS;
  ObProxyProtocol client_protocol = client_session->get_client_session_protocol();

  if (client_protocol == ObProxyProtocol::PROTOCOL_OB20) {
    Ob20ProtocolHeader &ob20_head = client_session->get_session_info().ob20_request_.ob20_header_;
    uint8_t compressed_seq = static_cast<uint8_t>(client_session->get_compressed_seq() + 1);
    Ob20ProtocolHeaderParam ob20_head_param(client_session->get_cs_id(), ob20_head.request_id_, compressed_seq,
                                            compressed_seq, true, false,
                                            client_session->is_client_support_new_extra_info());
    if (OB_FAIL(ObProto20Utils::encode_executor_response_packet(write_buf, ob20_head_param, seq, result_resp))) {
      LOG_WARN("fail to encode executor response ob20 packet", K(ret));
    }
  } else {
    if (OB_FAIL(ObMysqlPacketUtil::encode_executor_response_packet(write_buf, seq, result_resp))) {
      LOG_WARN("fail to encode executor response packet", K(ret));
    }
  }

  return ret;
}

int ObProxyPacketWriter::get_err_buf(int err_code, char *&buf)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(err_code == OB_SUCCESS)) {
    BACKTRACE(ERROR, (OB_SUCCESS == err_code), "BUG send error packet but err code is 0");
    err_code = OB_ERR_UNEXPECTED;
  }

  static __thread char msg_buffer_[MAX_MSG_BUFFER_SIZE];
  MEMSET(msg_buffer_, 0x0, sizeof(msg_buffer_));
  const char *err_msg = ob_strerror(err_code);
  int32_t length = 0;
  if (OB_ISNULL(err_msg)) {
    length = snprintf(msg_buffer_, sizeof(msg_buffer_), "Unknown user error");
  } else {
    length = snprintf(msg_buffer_, sizeof(msg_buffer_), err_msg);
  }

  if (OB_UNLIKELY(length < 0) || OB_UNLIKELY(length >= MAX_MSG_BUFFER_SIZE)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("err msg buffer not enough", K(ret), K(length), K(err_msg));
  } else {
    buf = msg_buffer_;
  }

  return ret;
}

int ObProxyPacketWriter::write_error_packet(event::ObMIOBuffer &write_buf, proxy::ObMysqlClientSession *client_session,
                                            uint8_t &seq, const int err_code, const char *msg_buf)
{
  return write_error_packet(write_buf, client_session, seq, err_code, common::ObString::make_string(msg_buf));  
}

int ObProxyPacketWriter::write_error_packet(event::ObMIOBuffer &write_buf, proxy::ObMysqlClientSession *client_session,
                                            uint8_t &seq, const int err_code, const ObString &msg_buf)
{
  int ret = OB_SUCCESS;
  ObProxyProtocol client_protocol = client_session->get_client_session_protocol();

  if (client_protocol == ObProxyProtocol::PROTOCOL_OB20) {
    if (OB_ISNULL(client_session)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid client session ptr in ob20 mode", K(ret));
    } else {
      Ob20ProtocolHeader &ob20_head = client_session->get_session_info().ob20_request_.ob20_header_;
      uint8_t compressed_seq = static_cast<uint8_t>(client_session->get_compressed_seq() + 1);
      Ob20ProtocolHeaderParam ob20_head_param(client_session->get_cs_id(), ob20_head.request_id_, 
                                              compressed_seq, compressed_seq, true, false,
                                              client_session->is_client_support_new_extra_info());
      if (OB_FAIL(ObProto20Utils::encode_err_packet(write_buf, ob20_head_param, seq, err_code, msg_buf))) {
        LOG_WARN("fail to encode err packet in ob20 format", K(ret));
      }
    }
  } else {
    // mysql
    if (OB_FAIL(ObMysqlPacketUtil::encode_err_packet(write_buf, seq, err_code, msg_buf))) {
      LOG_WARN("fail to encode err packet buf in mysql format", K(ret));
    }
  }
  
  return ret;
}


} // end of packet
} // end of obproxy
} // end of oceanbase

