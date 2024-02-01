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
#include "packet/ob_mysql_packet_writer.h"
#include "iocore/eventsystem/ob_io_buffer.h"
#include "proxy/mysqllib/ob_mysql_common_define.h"
#include "proxy/mysqllib/ob_mysql_compressed_packet.h"
#include "proxy/mysqllib/ob_mysql_analyzer_utils.h"
#include "utils/ob_zlib_stream_compressor.h"
#include "proxy/mysqllib/ob_protocol_diagnosis.h"

using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::common;
using namespace oceanbase::obmysql;

namespace oceanbase
{
namespace obproxy
{
namespace packet
{

int ObMysqlPacketWriter::write_packet(ObMIOBuffer &mio_buf,
                                      const ObMySQLPacket &packet,
                                      const int64_t packet_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(packet_len < MYSQL_NET_HEADER_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(packet), K(packet_len), K(ret));
  } else {
    int64_t tmp_len = packet_len;
    int64_t pos = 0;
    int64_t written_len = 0;
    if (mio_buf.block_write_avail() >= packet_len) {
      // in this case, we can write packet directly to mio_buf
      if (OB_FAIL(ObMySQLPacket::encode_packet(mio_buf.end(), tmp_len, pos, packet))) {
        LOG_WDIAG("fail to encode packet", K(tmp_len), K(pos), K(pos),
                 K(packet_len), K(packet), K(ret));
      } else if (OB_FAIL(mio_buf.fill(pos))) {
        // move start pointer
        LOG_WDIAG("fail to fill iobuffer", K(pos), K(ret));
      }
    } else {
      // in this case, we need extra buffer
      char *buf = reinterpret_cast<char *>(op_fixed_mem_alloc(packet_len));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("fail to alloc mem", K(packet_len), K(ret));
      } else if (OB_FAIL(ObMySQLPacket::encode_packet(buf, tmp_len, pos, packet))) {
        LOG_WDIAG("fail to encode packet", K(tmp_len), K(pos),
                 K(packet_len), K(packet), K(ret));
      } else {
        if (OB_FAIL(mio_buf.write(buf, pos, written_len))) {
          LOG_WDIAG("packet is not written completely", K(written_len), K(pos), K(ret));
        } else if (OB_UNLIKELY(pos != written_len)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("packet is not written completely", K(written_len), K(pos), K(ret));
        } else {} // do nothing
      }
      if (OB_LIKELY(NULL != buf)) {
        op_fixed_mem_free(buf, packet_len);
        buf = NULL;
      } else {} // do nothing
    }
  }
  return ret;

}

int ObMysqlPacketWriter::write_raw_packet(ObMIOBuffer &mio_buf, const ObString &packet_str)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(packet_str.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(packet_str), K(ret));
  } else {
    int64_t written_len = 0;
    if (OB_FAIL(mio_buf.write(packet_str.ptr(), packet_str.length(), written_len))) {
      LOG_WDIAG("not all data write to miobuffer", K(written_len), K(packet_str.length()), K(ret));
    } else if (OB_UNLIKELY(packet_str.length() != written_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("packet is not written completely", K(written_len), K(packet_str.length()), K(ret));
    } else {} // do nothing
  }
  return ret;
}

int ObMysqlPacketWriter::write_raw_packet(ObMIOBuffer &write_buf, const ObMySQLRawPacket &packet)
{
  int ret = OB_SUCCESS;

  char meta_buf[MYSQL_NET_META_LENGTH];
  int64_t pos = 0;
  int64_t meta_buf_len = MYSQL_NET_META_LENGTH;
  const char *buf = packet.get_cdata();
  int64_t buf_len = packet.get_clen();
  int64_t written_len = 0;

  // first write mysql packet meta(header + cmd)
  if (OB_FAIL(packet.encode_packet_meta(meta_buf, meta_buf_len, pos))) {
    LOG_WDIAG("fail to encode packet meta", K(pos), K(packet), K(meta_buf_len), K(ret));
  } else if (OB_FAIL(write_buf.write(meta_buf, meta_buf_len, written_len))) {
    LOG_WDIAG("fail to write meta buf to dst buf", K(ret));
  } else if (OB_UNLIKELY(written_len != MYSQL_NET_META_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("written len dismatch", K(ret), K(written_len), K(MYSQL_NET_META_LENGTH));
  } else if (OB_FAIL(write_buf.write(buf, buf_len, written_len))) {   // then write packet body
    LOG_WDIAG("fail to write packet buf to dst buf", K(ret));
  } else if (OB_UNLIKELY(written_len != buf_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("written len dismatch", K(ret), K(buf_len), K(written_len));
  } else {
    // nothing
  }

  return ret;
}

// only compress ObMysqlRawPacket
int ObMysqlPacketWriter::write_compressed_packet(ObMIOBuffer &mio_buf,
                                                 const ObMySQLRawPacket &packet,
                                                 proxy::ObCmpHeaderParam &param)
{
  int ret = OB_SUCCESS;

  ObIOBufferReader *tmp_mio_reader = NULL;
  ObMIOBuffer *tmp_mio_buf = NULL;
  if (OB_ISNULL(tmp_mio_buf = new_empty_miobuffer())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to new miobuffer", K(ret));
  } else if (OB_ISNULL(tmp_mio_reader = tmp_mio_buf->alloc_reader())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to alloc reader", K(ret));
  } else if (OB_FAIL(ObMysqlPacketWriter::write_raw_packet(*tmp_mio_buf, packet))) {
    LOG_WDIAG("fail to write raw packet", K(ret));
  } else if (OB_FAIL(ObMysqlAnalyzerUtils::consume_and_compress_data(tmp_mio_reader, &mio_buf,
                                                                     tmp_mio_reader->read_avail(),
                                                                     param))) {
    LOG_WDIAG("fail to consume and compress data", K(ret));
  } else {
    LOG_DEBUG("succ to write compressed packet");
  }

  if (NULL != tmp_mio_buf) {
    free_miobuffer(tmp_mio_buf);
    tmp_mio_buf = NULL;
    tmp_mio_reader = NULL;
  }

  return ret;
}

int ObMysqlPacketWriter::write_compressed_packet(
    ObMIOBuffer &mio_buf,
    const ObMySQLPacket &packet,
    const int64_t packet_len)
{
  int ret = OB_SUCCESS;
  int64_t tmp_len = packet_len;
  int64_t pos = 0;
  if (OB_UNLIKELY(packet_len < MYSQL_COMPRESSED_HEALDER_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(packet), K(packet_len), K(ret));
  } else {
    char *buf = reinterpret_cast<char *>(op_fixed_mem_alloc(packet_len));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_EDIAG("fail to alloc mem", K(packet_len), K(ret));
    } else if (OB_FAIL(ObMySQLPacket::encode_packet(buf, tmp_len, pos, packet))) {
      LOG_WDIAG("fail to encode packet", K(tmp_len), K(pos), K(packet_len), K(packet), K(ret));
    } else {
      char *compressed_packet_start = buf;
      int64_t compressed_packet_len = pos; // not include compressed header
      ObMysqlCompressedPacket compressed_packet;
      compressed_packet.set_seq(0);
      compressed_packet.set_content(compressed_packet_start, static_cast<int32>(compressed_packet_len));
      int64_t serialize_len = compressed_packet.get_serialize_size(); // include compressed header

      pos = 0;
      if (mio_buf.block_write_avail() >= serialize_len) {
        // in this case, we can write packet directly to mio_buf
        if (OB_FAIL(compressed_packet.encode(mio_buf.end(), serialize_len, pos))) {
          LOG_WDIAG("fail to encode compressed packet", K(serialize_len), K(pos), K(ret));
        } else if (OB_FAIL(mio_buf.fill(pos))) {
          // move start pointer
          LOG_WDIAG("fail to fill iobuffer", K(pos), K(ret));
        }
      } else {
        int64_t written_len = 0;
        // in this case, we need extra buffer
        char *compressed_buf = reinterpret_cast<char *>(op_fixed_mem_alloc(serialize_len));
        if (OB_ISNULL(compressed_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_EDIAG("fail to alloc mem", K(serialize_len), K(ret));
        } else if (OB_FAIL(compressed_packet.encode(compressed_buf, serialize_len, pos))) {
          LOG_WDIAG("fail to encode packet", K(serialize_len), K(pos), K(ret));
        } else if (OB_FAIL(mio_buf.write(compressed_buf, pos, written_len))) {
          LOG_WDIAG("packet is not written completely", K(written_len), K(pos), K(ret));
        } else if (OB_UNLIKELY(pos != written_len)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("invalid written len", K(pos), K(written_len), K(ret));
        } else {} // do nothing

        if (OB_LIKELY(NULL != compressed_buf)) {
          op_fixed_mem_free(compressed_buf, serialize_len);
          compressed_buf = NULL;
        }
      }
    }

    if (OB_LIKELY(NULL != buf)) {
      op_fixed_mem_free(buf, packet_len);
      buf = NULL;
    }
  }

  return ret;
}

int ObMysqlPacketWriter::write_request_packet(ObMIOBuffer &mio_buf, const ObMySQLCmd cmd,
                                              const common::ObString &sql_str,
                                              const bool need_compress,
                                              ObCmpHeaderParam &compressed_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(sql_str.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(sql_str), K(ret));
  } else if (OB_UNLIKELY(sql_str.length() > MYSQL_PACKET_MAX_LENGTH)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WDIAG("we cannot support packet which is larger than 16MB", K(sql_str),
             K(MYSQL_PACKET_MAX_LENGTH), K(ret));
  } else {
    ObMySQLRawPacket com_pkt(cmd);
    com_pkt.set_content(sql_str.ptr(), static_cast<uint32_t>(sql_str.length()));
    if (need_compress) {
      if (OB_FAIL(ObMysqlPacketWriter::write_compressed_packet(mio_buf, com_pkt, compressed_param))) {
        LOG_WDIAG("write packet failed", K(sql_str), K(ret));
      }
    } else {
      if (OB_FAIL(ObMysqlPacketWriter::write_packet(mio_buf, com_pkt))) {
        LOG_WDIAG("write packet failed", K(sql_str), K(ret));
      } else {
        ObProtocolDiagnosis *protocol_diagnosis = compressed_param.get_protocol_diagnosis();
        ObIOBufferReader *diagnosis_reader = NULL;
        if (OB_NOT_NULL(diagnosis_reader = mio_buf.alloc_reader())) {
          PROTOCOL_DIAGNOSIS(MULTI_MYSQL, send, protocol_diagnosis, *diagnosis_reader, diagnosis_reader->read_avail());
          diagnosis_reader->dealloc();
          diagnosis_reader = NULL;
        }
      }
    }
  }
  return ret;
}

} // end of namespace packet
} // end of namespace obproxy
} // end of namespace oceanbase
