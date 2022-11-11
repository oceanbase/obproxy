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
#include "packet/ob_mysql_packet_reader.h"
#include "rpc/obmysql/ob_mysql_util.h"
#include "rpc/obmysql/packet/ompk_ok.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;
namespace oceanbase
{
namespace obproxy
{
namespace packet
{
void ObMysqlPacketReader::reset()
{
  if (NULL != buf_) {
    op_fixed_mem_free(buf_, buf_len_);
    buf_ = NULL;
    buf_len_ = 0;
  }
  is_in_use_ = false;
}

inline int ObMysqlPacketReader::copy_to_buildin_buf(ObIOBufferReader &buf_reader,
                                                    const int64_t buf_len,
                                                    const int64_t offset,
                                                    char *&pbuf)
{
  int ret = OB_SUCCESS;
  // multiple block buffer
  if (OB_UNLIKELY(is_in_use_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("the buffer is hold by other packet", K(ret));
  } else {
    reset();
    if (buf_len < OB_SMALL_BUF_SIZE) {
      pbuf = small_buf_;
    } else {
      buf_len_ = buf_len;
      buf_ = static_cast<char *>(op_fixed_mem_alloc(buf_len_));
      pbuf = buf_;
    }
    if (OB_ISNULL(pbuf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc build-in buffer", K(ret));
      buf_len_ = 0;
    } else {
      char *written_pos = buf_reader.copy(pbuf, buf_len, offset);
      if (OB_UNLIKELY(written_pos != pbuf + buf_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not copy completely", K(written_pos), K(pbuf), K(buf_len), K(ret));
        reset();
      }
    }
  }
  return ret;
}

inline int ObMysqlPacketReader::get_buf(ObIOBufferReader &buf_reader, const int64_t buf_len,
                                        const int64_t offset, char *&pbuf)
{
  int ret = OB_SUCCESS;
  pbuf = NULL;

  int64_t all_data_size = (buf_reader.read_avail() + buf_reader.reserved_size_);
  if ((offset + buf_len) > all_data_size) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buf not enough", K(offset), K(buf_len), K(all_data_size), K(ret));
  } else {
    // skip the offset, find the right IOBufferBlock
    buf_reader.skip_empty_blocks(); // must be at the beginning
    int64_t outer_offset = offset;
    int64_t start_offset = buf_reader.start_offset_;
    int64_t total_offset = outer_offset + start_offset;
    int64_t len = -1;
    ObIOBufferBlock *target_block = buf_reader.block_;
    while ((NULL != target_block) && (len <= 0)) {
      len = target_block->read_avail();
      len -= total_offset;
      if (len <= 0) {
        total_offset = -len;
        target_block = target_block->next_;
      }
    }

    if (OB_ISNULL(target_block) || OB_UNLIKELY(total_offset < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to skip offset", K(target_block), K(total_offset), K(ret));
    } else if (OB_LIKELY((total_offset + buf_len) <= target_block->read_avail())) {
      pbuf = target_block->start() + total_offset;
    } else if (OB_FAIL(copy_to_buildin_buf(buf_reader, buf_len, offset, pbuf))) {
      LOG_WARN("fail to copy to buildin_buf", K(ret));
    } else {
      LOG_DEBUG("is in multiple buffer", K(offset), K(buf_len), K(all_data_size), K(ret));
    }
  }

  return ret;
}

inline int ObMysqlPacketReader::get_content_len_and_seq(ObIOBufferReader &buf_reader,
                             const int64_t offset, int64_t &content_len, uint8_t &seq)
{
  int ret = OB_SUCCESS;
  char *pbuf = NULL;
  if (OB_FAIL(get_buf(buf_reader, OB_MYSQL_NET_HEADER_LENGTH, offset, pbuf))) {
    LOG_WARN("fail to get header buf", K(ret));
  } else if (OB_ISNULL(pbuf)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pbuf is null, which is unexpected", K(pbuf), K(ret));
  } else {
    content_len = static_cast<int64_t>(uint3korr(pbuf));
    seq = static_cast<int64_t>(uint1korr(pbuf + 3));
  }
  return ret;
}

int ObMysqlPacketReader::get_ok_packet(ObIOBufferReader &buf_reader,
                                       const int64_t offset,
                                       const ObMySQLCapabilityFlags &cap,
                                       OMPKOK &ok_pkt,
                                       int64_t &pkt_len)
{
  int ret = OB_SUCCESS;
  char *content_buf = NULL;
  int64_t content_len = 0;
  uint8_t seq = 0;
  if (OB_FAIL(get_content_len_and_seq(buf_reader, offset, content_len, seq))) {
    LOG_WARN("fail to get content length and seq", K(ret), K(offset), K(content_len), K(seq));
  } else if (OB_FAIL(get_buf(buf_reader, content_len, offset + OB_MYSQL_NET_HEADER_LENGTH, content_buf))) {
    LOG_WARN("fail to get content buf", K(content_len), K(content_buf), K(offset), K(ret));
  } else {
    ok_pkt.set_seq(seq);
    ok_pkt.set_content(content_buf, static_cast<uint32_t>(content_len));
    ok_pkt.set_capability(cap);
    if (OB_FAIL(ok_pkt.decode())) {
      LOG_WARN("decode ok packet failed", K(ret));
    } else {
      pkt_len = content_len + OB_MYSQL_NET_HEADER_LENGTH;
      is_in_use_ = true; // if output buffer, we must mark we hold the buffer
    }
  }
  return ret;
}

int ObMysqlPacketReader::get_ok_packet_server_status(ObIOBufferReader &buf_reader,
                                                     ObServerStatusFlags &server_status)
{
  int ret = OB_SUCCESS;
  char *content_buf = NULL;
  int64_t content_len = 0;
  uint8_t seq = 0;
  int64_t offset = 0;
  if (OB_FAIL(get_content_len_and_seq(buf_reader, offset, content_len, seq))) {
    LOG_WARN("fail to get content length and seq", K(content_len), K(seq), K(offset), K(ret));
  } else if (OB_FAIL(get_buf(buf_reader, content_len, offset + OB_MYSQL_NET_HEADER_LENGTH, content_buf))) {
    LOG_WARN("fail to get content buf", K(content_len), K(content_buf), K(offset), K(ret));
  } else {
    const char *pos = content_buf;
    uint8_t field_count = 0;
    uint64_t affected_rows = 0;
    uint64_t last_insert_id = 0;
    if (FALSE_IT(ObMySQLUtil::get_uint1(pos, field_count))) {
      LOG_WARN("get field count", K(pos), K(ret));
    } else if (OB_UNLIKELY(0 != field_count)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("field count is not 0", K(pos), K(field_count), K(ret));
    } else if (OB_FAIL(ObMySQLUtil::get_length(pos, affected_rows))) {
      LOG_WARN("get affected_rows failed", K(pos), K(ret));
    } else if (OB_FAIL(ObMySQLUtil::get_length(pos, last_insert_id))) {
      LOG_WARN("get last_insert_id failed", K(pos), K(ret));
    } else if (FALSE_IT(ObMySQLUtil::get_uint2(pos, server_status.flags_))) {
      LOG_WARN("get server status failed", K(pos), K(ret));
    }
  }
  return ret;
}

int ObMysqlPacketReader::get_packet(ObIOBufferReader &buf_reader,
                                    ObMySQLPacket &packet)
{
  int ret = OB_SUCCESS;
  char *content_buf = NULL;
  int64_t content_len = 0;
  uint8_t seq = 0;
  int64_t offset = 0;
  if (OB_FAIL(get_content_len_and_seq(buf_reader, offset, content_len, seq))) {
    LOG_WARN("fail to get content length and seq", K(content_len), K(seq), K(offset), K(ret));
  } else if (OB_FAIL(get_buf(buf_reader, content_len, offset + OB_MYSQL_NET_HEADER_LENGTH, content_buf))) {
    LOG_WARN("fail to get content buf", K(content_len), K(content_buf), K(offset), K(ret));
  } else {
    packet.set_seq(seq);
    packet.set_content(content_buf, static_cast<uint32_t>(content_len));
    if (OB_FAIL(packet.decode())) {
      LOG_WARN("decode packet failed", K(ret));
    } else {
      is_in_use_ = true; // if output buffer, we must mark we hold the buffer
    }
  }
  return ret;
}

void ObMysqlPacketReader::print_reader(event::ObIOBufferReader &buf_reader)
{
  int ret = OB_SUCCESS;

  char *pkt_str = NULL;
  int64_t pkt_len = buf_reader.read_avail();
  static const int64_t BUF_LEN = 1024;
  char buf[BUF_LEN];
  buf[0] = '\0';

  int64_t pos = 0;
  if (OB_FAIL(copy_to_buildin_buf(buf_reader, pkt_len, 0, pkt_str))) {
    LOG_WARN("fail to copy buf", K(ret));
  } else if (OB_FAIL(hex_print(pkt_str, pkt_len, buf, BUF_LEN, pos))) {
    if (OB_SIZE_OVERFLOW == ret) {
      // print part
      pos = BUF_LEN - 1;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to do hex print", K(pkt_str), K(pkt_len), K(pos), K(ret));
    }
  } else { }

  // print ERROR log here
  if (OB_SUCC(ret)) {
    ObString packet_str(pos, buf);
    LOG_ERROR("print reader", K(packet_str), K(pos), K(pkt_len));
  }
}

} // end of namespace packet
} // end of namespace obproxy
} // end of namespace oceanbase

