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

#ifndef OBPROXY_MYSQL_PACKET_WRITER_H
#define OBPROXY_MYSQL_PACKET_WRITER_H

#include "lib/string/ob_sql_string.h"
#include "rpc/obmysql/packet/ompk_field.h"
#include "rpc/obmysql/packet/ompk_row.h"
#include "utils/ob_proxy_lib.h"
#include "proxy/mysqllib/ob_mysql_analyzer_utils.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{
class ObMIOBuffer;
}
namespace packet
{
class ObMysqlPacketWriter
{
public:
  static const int64_t MYSQL_NET_HEADER_LENGTH = 4;
  static const int64_t MAX_FILED_SIZE = 1024;
  static const int64_t MAX_ROW_SIZE   = 65536;

  ObMysqlPacketWriter() {}
  ~ObMysqlPacketWriter() {}

  // write mysql packet to mio buffer
  static int write_packet(event::ObMIOBuffer &mio_buf, const obmysql::ObMySQLPacket &packet);
  static int write_packet(event::ObMIOBuffer &mio_buf,
                          const obmysql::ObMySQLPacket &packet,
                          const int64_t packet_len);
  
  static int write_field_packet(event::ObMIOBuffer &mio_buf,
                                const obmysql::OMPKField &field_packet);
  static int write_row_packet(event::ObMIOBuffer &mio_buf,
                              const obmysql::OMPKRow &row_packet);
  // write raw packet string to mio buffer
  static int write_raw_packet(event::ObMIOBuffer &mio_buf, const common::ObString &raw_packet);
  static int write_raw_packet(event::ObMIOBuffer &write_buf, const obmysql::ObMySQLRawPacket &packet);
  
  // write request packet to mio buffer
  static int write_request_packet(event::ObMIOBuffer &mio_buf,
                                  const obmysql::ObMySQLCmd cmd,
                                  const common::ObString &sql_str,
                                  const bool need_compress,
                                  proxy::ObCmpHeaderParam &compressed_param);
private:
  // @packet, normal mysql packet
  // compress the packet and write compressed packet to mio_buf
  static int write_compressed_packet(event::ObMIOBuffer &mio_buf,
                                     const obmysql::ObMySQLPacket &packet,
                                     const int64_t packet_len);

  // @packet, normal raw mysql packet
  // only compress ObMysqlRawPacket, for we can copy less than common ObMysqlPacket
  // and more efficient.
  static int write_compressed_packet(event::ObMIOBuffer &mio_buf,
                                     const obmysql::ObMySQLRawPacket &packet,
                                     proxy::ObCmpHeaderParam &compressed_param);
  
  DISALLOW_COPY_AND_ASSIGN(ObMysqlPacketWriter);
};

inline int ObMysqlPacketWriter::write_packet(event::ObMIOBuffer &mio_buf,
                                             const obmysql::ObMySQLPacket &packet)
{
  int ret = common::OB_SUCCESS;
  int64_t serialize_size = packet.get_serialize_size();
  if (OB_UNLIKELY(serialize_size < 0)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    ret = write_packet(mio_buf, packet, serialize_size + MYSQL_NET_HEADER_LENGTH);
  }
  return ret;
}

inline int ObMysqlPacketWriter::write_field_packet(event::ObMIOBuffer &mio_buf,
                                                   const obmysql::OMPKField &field_packet)
{
  return write_packet(mio_buf, field_packet, MAX_FILED_SIZE);
}

inline int ObMysqlPacketWriter::write_row_packet(event::ObMIOBuffer &mio_buf,
                                                 const obmysql::OMPKRow &row_packet)
{
  return write_packet(mio_buf, row_packet, MAX_ROW_SIZE);
}


} // end of namespace packet
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_MYSQL_PACKET_WRITER_H
