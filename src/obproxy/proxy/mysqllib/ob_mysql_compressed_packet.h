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

#ifndef OB_MYSQL_COMPRESSED_PACKET
#define OB_MYSQL_COMPRESSED_PACKET
#include "lib/ob_define.h"
#include "rpc/ob_packet.h"
#include "proxy/mysqllib/ob_mysql_common_define.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

class ObMysqlCompressedPacket : public rpc::ObPacket
{
public:
  ObMysqlCompressedPacket() : hdr_(), cdata_(NULL) {}
  virtual ~ObMysqlCompressedPacket() {}

  inline void set_seq(uint8_t seq) { hdr_.seq_ = seq; }
  inline uint8_t get_seq() { return hdr_.seq_; }

  inline void set_content(const char *content, const uint32_t len)
  {
    hdr_.non_compressed_len_ = len;
    cdata_ = content;
  }

  static int encode_compressed_header(char *buf, const int64_t &len, int64_t &pos,
                                      const ObMysqlCompressedPacketHeader &hdr);

  virtual int64_t get_serialize_size() const;
  int encode(char *buf, int64_t &len, int64_t &pos);
  int decode(char *buf, int64_t &len, int64_t &pos);

  int64_t to_string(char *buf, const int64_t buf_len) const;

protected:
  virtual int serialize(char*, const int64_t&, int64_t&) const;

protected:
  ObMysqlCompressedPacketHeader hdr_;
  const char *cdata_;
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OB_MYSQL_COMPRESSED_PACKET */
