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

#ifndef OBPROXY_MYSQL_PACKET_UTIL_H
#define OBPROXY_MYSQL_PACKET_UTIL_H

#include "lib/container/ob_array.h"
#include "rpc/obmysql/ob_mysql_field.h"
#include "common/ob_field.h"
#include "rpc/obmysql/ob_mysql_packet.h"

namespace oceanbase
{
namespace common
{
class ObNewRow;
class ObObj;
}
namespace obmysql
{
class ObMySQLField;
}
namespace obproxy
{
namespace event
{
class ObMIOBuffer;
}

class ObMysqlPacketUtil
{
public:
  // encode packet utils
  static int encode_header(event::ObMIOBuffer &write_buf, uint8_t &seq,
                           common::ObIArray<obmysql::ObMySQLField> &fields,
                           uint16_t status_flag = 0);
  static int encode_row_packet(event::ObMIOBuffer &write_buf, uint8_t &seq,
                               const common::ObNewRow &row,
                               common::ObIArray<common::ObField> *fields = NULL);
  static int encode_eof_packet(event::ObMIOBuffer &write_buf, uint8_t &seq,
                               uint16_t status_flag = 0);
  static int encode_err_packet(event::ObMIOBuffer &write_buf, uint8_t &seq, int errcode);

  template<typename T>
  static int encode_err_packet(event::ObMIOBuffer &write_buf, uint8_t &seq, int errcode, T &param)
  {
    int ret = common::OB_SUCCESS;
    const int32_t MAX_MSG_BUF_SIZE = 256;
    if (OB_UNLIKELY(common::OB_SUCCESS == errcode)) {
      PROXY_LOG(WARN, "BUG send error packet but err code is 0", "Backtrace", common::lbt());
      errcode = common::OB_ERR_UNEXPECTED;
    }
    char msg_buf[MAX_MSG_BUF_SIZE];
    const char *errmsg = ob_str_user_error(errcode);
    int32_t length = 0;
    if (OB_ISNULL(errmsg)) {
      length = snprintf(msg_buf, sizeof(msg_buf), "Unknown user error, err=%d", errcode);
    } else {
      length = snprintf(msg_buf, sizeof(msg_buf), errmsg, param);
    }
    if (OB_UNLIKELY(length < 0) || OB_UNLIKELY(length > MAX_MSG_BUF_SIZE)) {
      ret = common::OB_BUF_NOT_ENOUGH;
      PROXY_LOG(WARN, "msg_buf is not enough", K(length), K(errmsg), K(ret));
    } else {
      ret = encode_err_packet_buf(write_buf, seq, errcode, msg_buf);
    }
    return ret;
  }

  static int encode_err_packet_buf(event::ObMIOBuffer &write_buf,
                                   uint8_t &seq,
                                   const int errcode,
                                   const char *msg_buf);

  static int encode_err_packet_buf(event::ObMIOBuffer &write_buf,
                                   uint8_t &seq,
                                   const int errcode,
                                   common::ObString msg);

  static int encode_ok_packet(event::ObMIOBuffer &write_buf,
                              uint8_t &seq,
                              const int64_t affected_rows,
                              const obmysql::ObMySQLCapabilityFlags &capability,
                              uint16_t status_flag = 0);
  static int encode_kv_resultset(event::ObMIOBuffer &write_buf,
                                 uint8_t &seq,
                                 const obmysql::ObMySQLField &field,
                                 common::ObObj &field_value,
                                 const uint16_t status_flag);

  static inline int get_int1(const char *&pos, int64_t &buf_len, int8_t &v);
  static inline int get_int2(const char *&pos, int64_t &buf_len, int16_t &v);
  static inline int get_int3(const char *&pos, int64_t &buf_len, int32_t &v);
  static inline int get_int4(const char *&pos, int64_t &buf_len, int32_t &v);
  static inline int get_int8(const char *&pos, int64_t &buf_len, int64_t &v);

  static inline int get_uint1(const char *&pos, int64_t &buf_len, uint8_t &v);
  static inline int get_uint2(const char *&pos, int64_t &buf_len, uint16_t &v);
  static inline int get_uint3(const char *&pos, int64_t &buf_len, uint32_t &v);
  static inline int get_uint4(const char *&pos, int64_t &buf_len, uint32_t &v);
  static inline int get_uint5(const char *&pos, int64_t &buf_len, uint64_t &v);
  static inline int get_uint6(const char *&pos, int64_t &buf_len, uint64_t &v);
  static inline int get_uint8(const char *&pos, int64_t &buf_len, uint64_t &v);

  static inline int get_float(const char *&pos, int64_t &buf_len, float &v);
  static inline int get_double(const char *&pos, int64_t &buf_len, double &v);

  static inline int get_length(const char *&pos, int64_t &buf_len, uint64_t &length);
};

int ObMysqlPacketUtil::get_int1(const char *&pos, int64_t &buf_len, int8_t &v)
{
  int ret = common::OB_SUCCESS;
  if (buf_len < 1) {
    ret = common::OB_SIZE_OVERFLOW;
  } else {
    obmysql::ObMySQLUtil::get_int1(pos, v);
    buf_len -= 1;
  }
  return ret;
}

int ObMysqlPacketUtil::get_int2(const char *&pos, int64_t &buf_len, int16_t &v)
{
  int ret = common::OB_SUCCESS;
  if (buf_len < 2) {
    ret = common::OB_SIZE_OVERFLOW;
  } else {
    obmysql::ObMySQLUtil::get_int2(pos, v);
    buf_len -= 2;
  }
  return ret;
}

int ObMysqlPacketUtil::get_int3(const char *&pos, int64_t &buf_len, int32_t &v)
{
  int ret = common::OB_SUCCESS;
  if (buf_len < 3) {
    ret = common::OB_SIZE_OVERFLOW;
  } else {
    obmysql::ObMySQLUtil::get_int3(pos, v);
    buf_len -= 3;
  }
  return ret;
}

int ObMysqlPacketUtil::get_int4(const char *&pos, int64_t &buf_len, int32_t &v)
{
  int ret = common::OB_SUCCESS;
  if (buf_len < 4) {
    ret = common::OB_SIZE_OVERFLOW;
  } else {
    obmysql::ObMySQLUtil::get_int4(pos, v);
    buf_len -= 4;
  }
  return ret;
}

int ObMysqlPacketUtil::get_int8(const char *&pos, int64_t &buf_len, int64_t &v)
{
  int ret = common::OB_SUCCESS;
  if (buf_len < 8) {
    ret = common::OB_SIZE_OVERFLOW;
  } else {
    obmysql::ObMySQLUtil::get_int8(pos, v);
    buf_len -= 8;
  }
  return ret;
}

int ObMysqlPacketUtil::get_uint1(const char *&pos, int64_t &buf_len, uint8_t &v)
{
  int ret = common::OB_SUCCESS;
  if (buf_len < 1) {
    ret = common::OB_SIZE_OVERFLOW;
  } else {
    obmysql::ObMySQLUtil::get_uint1(pos, v);
    buf_len -= 1;
  }
  return ret;
}

int ObMysqlPacketUtil::get_uint2(const char *&pos, int64_t &buf_len, uint16_t &v)
{
  int ret = common::OB_SUCCESS;
  if (buf_len < 2) {
    ret = common::OB_SIZE_OVERFLOW;
  } else {
    obmysql::ObMySQLUtil::get_uint2(pos, v);
    buf_len -= 2;
  }
  return ret;
}

int ObMysqlPacketUtil::get_uint3(const char *&pos, int64_t &buf_len, uint32_t &v)
{
  int ret = common::OB_SUCCESS;
  if (buf_len < 3) {
    ret = common::OB_SIZE_OVERFLOW;
  } else {
    obmysql::ObMySQLUtil::get_uint3(pos, v);
    buf_len -= 3;
  }
  return ret;
}

int ObMysqlPacketUtil::get_uint4(const char *&pos, int64_t &buf_len, uint32_t &v)
{
  int ret = common::OB_SUCCESS;
  if (buf_len < 4) {
    ret = common::OB_SIZE_OVERFLOW;
  } else {
    obmysql::ObMySQLUtil::get_uint4(pos, v);
    buf_len -= 4;
  }
  return ret;
}

int ObMysqlPacketUtil::get_uint5(const char *&pos, int64_t &buf_len, uint64_t &v)
{
  int ret = common::OB_SUCCESS;
  if (buf_len < 5) {
    ret = common::OB_SIZE_OVERFLOW;
  } else {
    obmysql::ObMySQLUtil::get_uint5(pos, v);
    buf_len -= 5;
  }
  return ret;
}

int ObMysqlPacketUtil::get_uint6(const char *&pos, int64_t &buf_len, uint64_t &v)
{
  int ret = common::OB_SUCCESS;
  if (buf_len < 6) {
    ret = common::OB_SIZE_OVERFLOW;
  } else {
    obmysql::ObMySQLUtil::get_uint6(pos, v);
    buf_len -= 6;
  }
  return ret;
}

int ObMysqlPacketUtil::get_uint8(const char *&pos, int64_t &buf_len, uint64_t &v)
{
  int ret = common::OB_SUCCESS;
  if (buf_len < 8) {
    ret = common::OB_SIZE_OVERFLOW;
  } else {
    obmysql::ObMySQLUtil::get_uint8(pos, v);
    buf_len -= 8;
  }
  return ret;
}

int ObMysqlPacketUtil::get_float(const char *&pos, int64_t &buf_len, float &v)
{
  int ret = common::OB_SUCCESS;
  if (buf_len < sizeof(v)) {
    ret = common::OB_SIZE_OVERFLOW;
  } else {
    MEMCPY(&v, pos, sizeof(v));
    pos += sizeof(v);
    buf_len -= sizeof(v);
  }
  return ret;
}

int ObMysqlPacketUtil::get_double(const char *&pos, int64_t &buf_len, double &v)
{
  int ret = common::OB_SUCCESS;
  if (buf_len < sizeof(v)) {
    ret = common::OB_SIZE_OVERFLOW;
  } else {
    MEMCPY(&v, pos, sizeof(v));
    pos += sizeof(v);
    buf_len -= sizeof(v);
  }
  return ret;
}

// https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::LengthEncodedInteger
int ObMysqlPacketUtil::get_length(const char *&pos, int64_t &buf_len, uint64_t &length)
{
  int ret = common::OB_SUCCESS;
  if (buf_len < 1) {
    ret = common::OB_SIZE_OVERFLOW;
  } else {
    uint16_t s2 = 0;
    uint32_t s4 = 0;
    uint8_t sentinel = 0;
    get_uint1(pos, buf_len, sentinel);
    if (sentinel < 251) {
      length = sentinel;
    } else if (sentinel == 251) {
      length = UINT64_MAX; // represents a NULL resultset
    } else if (sentinel == 252) {
      ret = get_uint2(pos, buf_len, s2);
      length = s2;
    } else if (sentinel == 253) {
      ret = get_uint3(pos, buf_len, s4);
      length = s4;
    } else if (sentinel == 254) {
      ret = get_uint8(pos, buf_len, length);
    } else {
      // 255??? won't get here.
      pos--;           // roll back
      buf_len++;
      ret = common::OB_INVALID_DATA;
    }
  }
  return ret;
}

}//end of namespace obproxy
}//end of namespace oceanbase

#endif /* OBPROXY_MYSQL_PACKET_UTIL_H */
