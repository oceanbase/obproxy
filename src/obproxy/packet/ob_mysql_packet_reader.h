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

#ifndef OBPROXY_MYSQL_PACKET_READER_H
#define OBPROXY_MYSQL_PACKET_READER_H

#include "utils/ob_proxy_lib.h"
#include "iocore/eventsystem/ob_io_buffer.h"

namespace oceanbase
{
namespace obmysql
{
class OMPKOK;
class ObMySQLPacket;
union ObMySQLCapabilityFlags;
union ObServerStatusFlags;
}
namespace obproxy
{
namespace packet
{
class ObMysqlPacketReader
{
public:
  static const int64_t OB_MYSQL_CONTENT_LENGTH_ENCODE_SIZE = 3;
  static const int64_t OB_MYSQL_SEQ_ENCODE_SIZE = 1;
  static const int64_t OB_MYSQL_NET_HEADER_LENGTH = 4;
  static const int64_t OB_SMALL_BUF_SIZE = 64;

  ObMysqlPacketReader() : is_in_use_(false), buf_(NULL), buf_len_(0) { small_buf_[0] = '\0'; }
  ~ObMysqlPacketReader() { reset(); }
  void reset();

  // === GET function will not comsume the packet
  int get_content_len(event::ObIOBufferReader &buf_reader, const int64_t offset, int64_t &content_len);
  int get_seq(event::ObIOBufferReader &buf_reader, const int64_t offset, uint8_t &seq);

  // DESC: get the next ok packet from MIOBuffer
  // NOTE: 1. get function will NOT consume buffer as packet may hold some string int mio_buf
  //       2. if mio_buf.consume or reader.reset is called, do NOT use the packet any more
  // Params:
  //        @buf_reader[in], the input buffer
  //        @offset[in],     the offset to read
  //        @cap[in],        the capability flag to decode ok packet
  //        @ok_pkt[out],    the output ok packet
  //        @pkt_len[out],   the output packet length, use it to comsume buffer later
  // Usage:
  //       ObMysqlPacketReader pkt_reader;
  //       OMPKOK ok_packet;
  //       pkt_reader.get_ok_packet(buf_reader, cap, ok_packet, pkt_len);
  //       do something with ok_packet( e.g. save_sesssion_info(ok_packet); )
  //       mio_buf.consume(pkt_len);
  //       pkt_reader.reset(); // optional
  int get_ok_packet(event::ObIOBufferReader &buf_reader,
                    const int64_t offset,
                    const obmysql::ObMySQLCapabilityFlags &cap,
                    obmysql::OMPKOK &ok_pkt,
                    int64_t &pkt_len);
  int get_ok_packet(event::ObIOBufferReader &buf_reader,
                    const int64_t offset,
                    const obmysql::ObMySQLCapabilityFlags &cap,
                    obmysql::OMPKOK &ok_pkt);
  int get_packet(event::ObIOBufferReader &buf_reader,
                 obmysql::ObMySQLPacket &packet);
  int get_ok_packet_server_status(event::ObIOBufferReader &buf_reader,
                                  obmysql::ObServerStatusFlags &server_status);
  // === READ function will comsume the packet
  // DESC: read a packet to pkt_str, And comsume it
  // NOTE: 1. read function will consume the packet
  // Params:
  //        @buf_reader[in], the input buffer
  //        @cap[in], the capability flag to decode ok packet
  //        @ok_pkt[out], the output ok packet
  //        @pkt_len[out], the output packet length, use it to comsume buffer later
  int read_packet_str(event::ObIOBufferReader &buf_reader,
                      const int64_t pkt_len,
                      char *&pkt_str);
  void print_reader(event::ObIOBufferReader &buf_reader);
private:
  int64_t get_reserved_offset(event::ObIOBufferReader &buf_reader);

  int copy_to_buildin_buf(event::ObIOBufferReader &buf_reader, const int64_t buf_len,
                          const int64_t offset, char *&pbuf);
  // get the buffer from buf_reader, the buffer may be [block_buffer in buf_reader]
  //                                                   or [small buffer in small_buf_]
  //                                                   or [large buffer in the heap]
  // Params:
  //        @buf_reader[in], the input buffer
  //        @buf_len[in], the length of buffer want to get
  //        @offset[in], get the buffer from offset
  //        @pbuf[out], the pointer to the buffer
  int get_buf(event::ObIOBufferReader &buf_reader, const int64_t buf_len,
              const int64_t offset, char *&pbuf);

  bool is_in_use_;  // indicate whether output packet hold this reader
  char *buf_;       // pointer to the buf in heap(if need)
  int64_t buf_len_; // the length of buf in heap
  char small_buf_[OB_SMALL_BUF_SIZE]; // the stack small buf

  DISALLOW_COPY_AND_ASSIGN(ObMysqlPacketReader);
};

inline int ObMysqlPacketReader::get_ok_packet(event::ObIOBufferReader &buf_reader,
                                              const int64_t offset,
                                              const obmysql::ObMySQLCapabilityFlags &cap,
                                              obmysql::OMPKOK &ok_pkt)
{
  int64_t pkt_len = 0;
  return get_ok_packet(buf_reader, offset, cap, ok_pkt, pkt_len);
}

inline int ObMysqlPacketReader::read_packet_str(event::ObIOBufferReader &buf_reader,
                                                const int64_t pkt_len,
                                                char *&pkt_str)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(copy_to_buildin_buf(buf_reader, pkt_len, 0, pkt_str))) {
    PROXY_LOG(WARN, "fail to copy buf", K(ret));
  } else {
    is_in_use_ = true; // if output buffer, we must mark we hold the buffer
    if (OB_FAIL(buf_reader.consume(pkt_len))) {
      PROXY_LOG(WARN, "fail to consume ", K(pkt_len), K(ret));
    }
  }
  return ret;
}

} // end of namespace packet
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_MYSQL_PACKET_READER_H
