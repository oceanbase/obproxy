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

using namespace oceanbase::obproxy::event;
using namespace oceanbase::common;
using namespace oceanbase::obmysql;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
// only compress ObMysqlRawPacket,
int ObMysqlOB20PacketWriter::write_compressed_packet(ObMIOBuffer &mio_buf,
                                                     const ObMySQLRawPacket &packet,
                                                     uint8_t &compressed_seq,
                                                     const uint32_t request_id,
                                                     const uint32_t sessid)
{
  int ret = OB_SUCCESS;
  char meta_buf[MYSQL_NET_META_LENGTH];
  int64_t pos = 0;
  int64_t meta_buf_len = MYSQL_NET_META_LENGTH;
  // 1. encode mysql packet meta(header + cmd)
  if (OB_FAIL(packet.encode_packet_meta(meta_buf, meta_buf_len, pos))) {
    LOG_WARN("fail to encode packet meta", K(pos), K(packet), K(meta_buf_len), K(ret));
  } else {
    ObIOBufferReader *tmp_reader = NULL;
    ObMIOBuffer *tmp_mio_buf = NULL;
    const char *buf = packet.get_cdata();
    int64_t buf_len = packet.get_clen();
    int64_t written_len = 0;
    const bool is_last_packet = true;
    const bool is_need_reroute = false;
    if (OB_ISNULL(tmp_mio_buf = new_empty_miobuffer())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new miobuffer", K(ret));
    } else if (OB_ISNULL(tmp_reader = tmp_mio_buf->alloc_reader())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to alloc reader", K(ret));
    // 2. write the meta buf, the first part of mysql packet
    } else if (OB_FAIL(tmp_mio_buf->write(meta_buf, meta_buf_len, written_len))) {
      LOG_WARN("fail to write", K(meta_buf_len), K(ret));
    } else if (OB_UNLIKELY(written_len != MYSQL_NET_META_LENGTH)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("written_len dismatch", K(written_len), K(MYSQL_NET_META_LENGTH), K(ret));
    // 3. write the request buf, the second part of mysql packet
    } else if (OB_FAIL(tmp_mio_buf->write(buf, buf_len, written_len))) {
      LOG_WARN("fail to write", K(buf_len), K(ret));
    } else if (OB_UNLIKELY(written_len != buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("written_len dismatch", K(written_len), K(buf_len), K(ret));
    // 4. make up compress packet
    } else if (OB_FAIL(ObProto20Utils::consume_and_compress_data(tmp_reader,
               &mio_buf, tmp_reader->read_avail(), compressed_seq, compressed_seq,
               request_id, sessid, is_last_packet, is_need_reroute))) {
      LOG_WARN("fail to consume_and_compress_data", K(ret));
    }

    if (NULL != tmp_mio_buf) {
      free_miobuffer(tmp_mio_buf);
      tmp_mio_buf = NULL;
      tmp_reader = NULL;
    }
  }

  return ret;
}

int ObMysqlOB20PacketWriter::write_request_packet(ObMIOBuffer &mio_buf, const ObMySQLCmd cmd,
                                                  const common::ObString &sql_str,
                                                  uint8_t &compressed_seq,
                                                  const uint32_t request_id,
                                                  const uint32_t sessid)
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
    ObMySQLRawPacket com_pkt(cmd);
    com_pkt.set_content(sql_str.ptr(), static_cast<uint32_t>(sql_str.length()));
    if (OB_FAIL(ObMysqlOB20PacketWriter::write_compressed_packet(mio_buf, com_pkt, compressed_seq, request_id, sessid))) {
      LOG_WARN("write packet failed", K(sql_str), K(ret));
    }
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
