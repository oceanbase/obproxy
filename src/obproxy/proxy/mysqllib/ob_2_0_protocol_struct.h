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

#ifndef OBPROXY_OB20_PROTOCOL_STRUCT_H
#define OBPROXY_OB20_PROTOCOL_STRUCT_H

#include "proxy/mysqllib/ob_mysql_common_define.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

// used for proxy and observer to negotiate new features
union Ob20ProtocolFlags
{
  Ob20ProtocolFlags() : flags_(0) {}
  explicit Ob20ProtocolFlags(uint32_t flag) : flags_(flag) {}

  bool is_extra_info_exist() const { return 1 == st_flags_.OB_EXTRA_INFO_EXIST; }
  bool is_last_packet() const { return 1 == st_flags_.OB_IS_LAST_PACKET; }

  uint32_t flags_;
  struct Protocol20Flags
  {
    uint32_t OB_EXTRA_INFO_EXIST:                       1;
    uint32_t OB_IS_LAST_PACKET:                         1;
    uint32_t OB_IS_PROXY_REROUTE:                       1;
    uint64_t OB_FLAG_RESERVED_NOT_USE:                 30;
  } st_flags_;
};

class Ob20ProtocolHeader
{
public:
  ObMysqlCompressedPacketHeader cp_hdr_;

  uint16_t magic_num_;
  uint16_t header_checksum_;
  uint32_t connection_id_;
  uint32_t request_id_;
  uint8_t pkt_seq_;
  uint32_t payload_len_;
  Ob20ProtocolFlags flag_;
  uint16_t version_;
  uint16_t reserved_;

public:
  Ob20ProtocolHeader()
    : cp_hdr_(), magic_num_(0), header_checksum_(0),
    connection_id_(0), request_id_(0), pkt_seq_(0), payload_len_(0),
    flag_(0), version_(0), reserved_(0) {}

  ~Ob20ProtocolHeader() {}

  void reset()
  {
    MEMSET(this, 0, sizeof(Ob20ProtocolHeader));
  }

  TO_STRING_KV("ob 20 protocol header", cp_hdr_,
               K_(magic_num),
               K_(header_checksum),
               K_(connection_id),
               K_(request_id),
               K_(pkt_seq),
               K_(payload_len),
               K_(version),
               K_(flag_.flags),
               K_(reserved));
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_OB20_PROTOCOL_STRUCT_H */
