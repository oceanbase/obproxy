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
#include "lib/compress/zlib/ob_zlib_compressor.h"
#include "rpc/obmysql/ob_mysql_util.h"
#include "proxy/mysqllib/ob_mysql_compressed_packet.h"
using namespace oceanbase::common;
using namespace oceanbase::obmysql;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

int ObMysqlCompressedPacket::serialize(char *buf, const int64_t &length, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  ObZlibCompressor compressor;
  int64_t compressed_len = 0;
  if (OB_UNLIKELY(NULL == buf || length <= 0 || pos < 0 || NULL == cdata_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(buf), K(length), K(get_serialize_size()), K(pos), K(ret));
  } else if (OB_FAIL(compressor.compress(cdata_, hdr_.non_compressed_len_,
          buf + pos, length - pos, compressed_len))) {
    LOG_WARN("fail to compress", K(hdr_), K(compressed_len), K(pos), K(ret));
  } else {
    pos += compressed_len;
  }

  return ret;
}

int ObMysqlCompressedPacket::decode(char *buf, int64_t &len, int64_t &pos)
{
  UNUSED(buf);
  UNUSED(len);
  UNUSED(pos);
  return common::OB_NOT_SUPPORTED;
}

int ObMysqlCompressedPacket::encode(char *buf, int64_t &len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  const int64_t orig_pos = pos;
  pos += MYSQL_COMPRESSED_HEALDER_LENGTH; // reserve header

  if (OB_FAIL(serialize(buf, len, pos))) {
    LOG_WARN("encode packet data failed", K(ret));
  } else {
    int32_t compressed_len = static_cast<int32_t>(pos - orig_pos - MYSQL_COMPRESSED_HEALDER_LENGTH);
    pos = orig_pos;
    hdr_.compressed_len_ = compressed_len;
    if (OB_FAIL(encode_compressed_header(buf, len, pos, hdr_))) {
      LOG_WARN("fail to encode compressed header", K(hdr_), K(ret));
    } else {
      pos += compressed_len;
    }
  }

  if (OB_FAIL(ret)) {
    pos = orig_pos;
  }

  return ret;
}

int64_t ObMysqlCompressedPacket::get_serialize_size() const
{
  int64_t max_size = 0;
  int ret = common::OB_SUCCESS;
  ObZlibCompressor compressor;
  if (OB_FAIL(compressor.get_max_overflow_size(hdr_.non_compressed_len_, max_size))) {
    PROXY_LOG(ERROR, "fail to get max overflow size", K_(hdr), K(ret));
  } else {
    max_size += hdr_.non_compressed_len_;
    max_size += MYSQL_COMPRESSED_HEALDER_LENGTH; // do not forget hdr
  }
  return max_size;
}

int ObMysqlCompressedPacket::encode_compressed_header(
    char *buf, const int64_t &len, int64_t &pos, const ObMysqlCompressedPacketHeader &hdr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || len <= 0 || pos < 0 || pos >= len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(buf), K(len), K(pos), K(ret));
  } else {
    if (OB_FAIL(ObMySQLUtil::store_int3(buf, len, hdr.compressed_len_, pos))) {
      LOG_ERROR("fail to encode int", K(ret));
    } else if (OB_FAIL(ObMySQLUtil::store_int1(buf, len, hdr.seq_, pos))) {
      LOG_ERROR("fail to encode seq", K(ret));
    } else if (OB_FAIL(ObMySQLUtil::store_int3(buf, len, hdr.non_compressed_len_, pos))) {
      LOG_ERROR("fail to encode int", K(ret));
    }
  }
  return ret;
}

int64_t ObMysqlCompressedPacket::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(K_(hdr), KP_(cdata));
  return pos;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
