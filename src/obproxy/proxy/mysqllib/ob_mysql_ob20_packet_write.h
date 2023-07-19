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
#include "obproxy/proxy/mysqllib/ob_2_0_protocol_utils.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class Ob20ProtocolHeaderParam;

class ObMysqlOB20PacketWriter
{
public:
  ObMysqlOB20PacketWriter() {}
  ~ObMysqlOB20PacketWriter() {}

  // write request packet to mio buffer
  static int write_raw_packet(event::ObMIOBuffer &mio_buf, const common::ObString &packet_str,
                              const Ob20ProtocolHeaderParam &ob20_head_param);
  static int write_request_packet(event::ObMIOBuffer &mio_buf, const obmysql::ObMySQLCmd cmd,
                                  const common::ObString &sql_str, const uint32_t conn_id,
                                  const uint32_t req_id, const uint8_t compressed_seq,
                                  const uint8_t pkt_seq, const bool is_last_packet, const bool is_weak_read,
                                  const bool is_need_reroute, const bool is_new_extra_info,
                                  const bool is_trans_internal_routing, const bool is_proxy_switch_reroute,
                                  const common::ObIArray<ObObJKV> *extra_info = NULL);
  static int write_packet(event::ObMIOBuffer &mio_buf, const char *buf, const int64_t buf_len,
                          const Ob20ProtocolHeaderParam &ob20_head_param);
  static int write_packet(event::ObMIOBuffer &mio_buf, const obmysql::ObMySQLPacket &packet,
                          const Ob20ProtocolHeaderParam &ob20_head_param);

private:
  static int write_compressed_packet(event::ObMIOBuffer &mio_buf, const obmysql::ObMySQLRawPacket &packet,
                                     const Ob20ProtocolHeaderParam &ob20_head_param,
                                     const common::ObIArray<ObObJKV> *extra_info = NULL);
  static int write_compressed_packet(event::ObMIOBuffer &mio_buf, const obmysql::ObMySQLPacket &packet,
                                     const Ob20ProtocolHeaderParam &ob20_head_param);
  DISALLOW_COPY_AND_ASSIGN(ObMysqlOB20PacketWriter);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_MYSQL_OB20_PACKET_WRITER_H
