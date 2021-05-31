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

#ifndef OBPROXY_MYSQL_OB20_PACKET_WRITER_H
#define OBPROXY_MYSQL_OB20_PACKET_WRITER_H

#include "lib/string/ob_sql_string.h"
#include "iocore/eventsystem/ob_io_buffer.h"
#include "rpc/obmysql/ob_mysql_packet.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

class ObMysqlOB20PacketWriter
{
public:
  ObMysqlOB20PacketWriter() {}
  ~ObMysqlOB20PacketWriter() {}

  // write request packet to mio buffer
  static int write_request_packet(event::ObMIOBuffer &mio_buf,
                                  const obmysql::ObMySQLCmd cmd,
                                  const common::ObString &sql_str,
                                  uint8_t &compressed_seq,
                                  const uint32_t request_id,
                                  const uint32_t sessid);
private:
  static int write_compressed_packet(event::ObMIOBuffer &mio_buf,
                                     const obmysql::ObMySQLRawPacket &packet,
                                     uint8_t &compressed_seq,
                                     const uint32_t request_id,
                                     const uint32_t sessid);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_MYSQL_OB20_PACKET_WRITER_H
