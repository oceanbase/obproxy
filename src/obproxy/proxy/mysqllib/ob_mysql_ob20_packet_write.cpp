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

#include "proxy/mysqllib/ob_mysql_common_define.h"
#include "proxy/mysqllib/ob_mysql_ob20_packet_write.h"
#include "proxy/mysqllib/ob_2_0_protocol_utils.h"
#include "obproxy/packet/ob_mysql_packet_writer.h"
#include "obproxy/proxy/mysqllib/ob_mysql_analyzer_utils.h"

using namespace oceanbase::obproxy::event;
using namespace oceanbase::common;
using namespace oceanbase::obmysql;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

int ObMysqlOB20PacketWriter::write_compressed_packet(ObMIOBuffer &mio_buf,
                                                     const ObMySQLRawPacket &packet,
                                                     const Ob20ProtocolHeaderParam &ob20_head_param,
                                                     const common::ObIArray<ObObJKV> *extra_info)
{
  int ret = OB_SUCCESS;

  ObIOBufferReader *tmp_mio_reader = NULL;
  ObMIOBuffer *tmp_mio_buf = NULL;
  const int64_t packet_len = packet.get_clen() + MYSQL_NET_META_LENGTH;
  if (OB_ISNULL(tmp_mio_buf = new_miobuffer(packet_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new miobuffer", K(ret));
  } else if (OB_ISNULL(tmp_mio_reader = tmp_mio_buf->alloc_reader())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to alloc reader", K(ret));
  } else if (OB_FAIL(packet::ObMysqlPacketWriter::write_raw_packet(*tmp_mio_buf, packet))) {
    LOG_WARN("fail to write raw packet", K(ret));
  } else if (OB_FAIL(ObProto20Utils::consume_and_compress_data(tmp_mio_reader, &mio_buf,
                       tmp_mio_reader->read_avail(), ob20_head_param, extra_info))) {
    LOG_WARN("fail to consume and compress data", K(ret));
  } else {
    // nothing
  }

  if (OB_LIKELY(NULL != tmp_mio_buf)) {
    free_miobuffer(tmp_mio_buf);
    tmp_mio_buf = NULL;
    tmp_mio_reader = NULL;
  }

  return ret;
}

int ObMysqlOB20PacketWriter::write_compressed_packet(ObMIOBuffer &mio_buf,
                                                     const obmysql::ObMySQLPacket &packet,
                                                     const Ob20ProtocolHeaderParam &ob20_head_param)
{
  int ret = OB_SUCCESS;

  const int64_t serialize_size = packet.get_serialize_size();
  const int64_t packet_len = serialize_size + MYSQL_NET_HEADER_LENGTH;
  int64_t tmp_len = packet_len;
  int64_t pos = 0;
  ObMIOBuffer *tmp_mio_buf = NULL;
  ObIOBufferReader *tmp_mio_reader = NULL;

  if (OB_ISNULL(tmp_mio_buf = new_miobuffer(packet_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new miobuffer", K(ret), K(packet_len));
  } else if (OB_ISNULL(tmp_mio_reader = tmp_mio_buf->alloc_reader())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to alloc reader", K(ret));
  } else if (ObMySQLPacket::encode_packet(tmp_mio_buf->end(), tmp_len, pos, packet)) {
    LOG_WARN("fail to encode packet",  K(ret), K(tmp_len), K(pos), K(pos), K(packet_len), K(packet));
  } else if (OB_FAIL(tmp_mio_buf->fill(pos))) {
    // move start pointer
    LOG_WARN("fail to fill iobuffer", K(ret), K(pos));
  } else if (ObProto20Utils::consume_and_compress_data(tmp_mio_reader, &mio_buf,
                                                       tmp_mio_reader->read_avail(), ob20_head_param)) {
    LOG_WARN("fail to consume and compress data", K(ret));
  } else {
    // nothing
  }

  // free mio buf
  if (OB_LIKELY(tmp_mio_buf != NULL)) {
    free_miobuffer(tmp_mio_buf);
    tmp_mio_buf = NULL;
    tmp_mio_reader = NULL;
  }

  return ret;
}

int ObMysqlOB20PacketWriter::write_raw_packet(event::ObMIOBuffer &mio_buf, const common::ObString &packet_str,
                                              const Ob20ProtocolHeaderParam &ob20_head_param)
{
  int ret = OB_SUCCESS;

  // write to buffer directly
  const char *buf = packet_str.ptr();
  const int64_t buf_len = packet_str.length(); 
  if (OB_UNLIKELY(buf == NULL) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument before write raw packet", K(ret), K(buf), K(buf_len));
  } else {
    ObMIOBuffer *tmp_mio_buf = NULL;
    ObIOBufferReader *tmp_mio_reader = NULL;
    int64_t written_len = 0;
    if (OB_ISNULL(tmp_mio_buf = new_miobuffer(buf_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new miobuffer", K(ret), K(buf_len));
    } else if (OB_ISNULL(tmp_mio_reader = tmp_mio_buf->alloc_reader())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to alloc reader", K(ret));
    } else if (OB_FAIL(tmp_mio_buf->write(buf, buf_len, written_len))) {
      LOG_WARN("fail to write", K(buf_len), K(ret));
    } else if (OB_UNLIKELY(written_len != buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("written_len dismatch", K(written_len), K(buf_len), K(ret));
    } else if (OB_FAIL(ObProto20Utils::consume_and_compress_data(tmp_mio_reader, &mio_buf,
                                                                 tmp_mio_reader->read_avail(), ob20_head_param))) {
      LOG_WARN("fail to consume and compress data", K(ret));
    } else {
      LOG_DEBUG("succ to write raw packet in ob20 format");
    }

    if (OB_LIKELY(NULL != tmp_mio_buf)) {
      free_miobuffer(tmp_mio_buf);
      tmp_mio_buf = NULL;
      tmp_mio_reader = NULL;
    }
  }
  
  return ret;
}

int ObMysqlOB20PacketWriter::write_request_packet(ObMIOBuffer &mio_buf,
                                                  const ObMySQLCmd cmd,
                                                  const common::ObString &sql_str,
                                                  const uint32_t conn_id,
                                                  const uint32_t req_id,
                                                  const uint8_t compressed_seq,
                                                  const uint8_t pkt_seq,
                                                  bool is_last_packet,
                                                  bool is_need_reroute,
                                                  bool is_new_extra_info,
                                                  const common::ObIArray<ObObJKV> *extra_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(sql_str.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(sql_str), K(ret));
  } else if (OB_UNLIKELY(sql_str.length() > MYSQL_PACKET_MAX_LENGTH)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("we cannot support packet which is larger than 16MB", K(sql_str),
             K(MYSQL_PACKET_MAX_LENGTH), K(ret));
  } else {
    Ob20ProtocolHeaderParam ob20_head_param(conn_id, req_id, compressed_seq, pkt_seq, is_last_packet, is_need_reroute,
                                            is_new_extra_info);
    ObMySQLRawPacket com_pkt(cmd);
    com_pkt.set_content(sql_str.ptr(), static_cast<uint32_t>(sql_str.length()));
    if (OB_FAIL(ObMysqlOB20PacketWriter::write_compressed_packet(mio_buf, com_pkt, ob20_head_param, extra_info))) {
      LOG_WARN("write packet failed", K(sql_str), K(ret));
    }
  }
  return ret;
}

int ObMysqlOB20PacketWriter::write_packet(ObMIOBuffer &mio_buf,
                                          const char *buf,
                                          const int64_t buf_len,
                                          const Ob20ProtocolHeaderParam &ob20_head_param)
{
  int ret = OB_SUCCESS;

  obmysql::ObMySQLPacket packet;
  packet.set_content(buf, static_cast<uint32_t>(buf_len));
  ret = write_packet(mio_buf, packet, ob20_head_param);

  return ret;
}

int ObMysqlOB20PacketWriter::write_packet(ObMIOBuffer &mio_buf,
                                          const obmysql::ObMySQLPacket &packet,
                                          const Ob20ProtocolHeaderParam &ob20_head_param)
{
  int ret = OB_SUCCESS;
  
  int64_t serialize_size = packet.get_serialize_size();
  if (OB_UNLIKELY(serialize_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid serialize size", K(ret), K(serialize_size));
  } else if (OB_FAIL(write_compressed_packet(mio_buf, packet, ob20_head_param))) {
    LOG_WARN("fail to write compressed packet", K(ret), K(serialize_size));
  } else {
    // nothing
  }

  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
