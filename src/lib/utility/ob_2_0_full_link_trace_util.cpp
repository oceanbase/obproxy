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

#include "lib/utility/ob_2_0_full_link_trace_util.h"
#include "rpc/obmysql/ob_mysql_util.h"
#include "lib/oblog/ob_log.h"


namespace oceanbase
{

namespace common
{

int Ob20FullLinkTraceTransUtil::store_type_and_len(char *buf, int64_t len, int64_t &pos, int16_t type, int32_t v_len)
{
  int ret = OB_SUCCESS;
  
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input", K(ret));
  } else if (len < pos + FLT_TYPE_AND_LEN) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", K(ret), K(len), K(pos));
  } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int2(buf, len, type, pos))){
    LOG_WARN("failed to store type", K(ret), K(type), K(buf), K(len));
  } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int4(buf, len, v_len, pos))) {
    LOG_WARN("failed to store v_len", K(ret), K(buf));
  } else {
    // do nothing
  }
  
  return ret;
}

int Ob20FullLinkTraceTransUtil::store_str(char *buf, int64_t len, int64_t &pos, const char *str,
                                          const int32_t str_len, int16_t type)
{
  int ret = OB_SUCCESS;
  
  if (len < pos + str_len + FLT_TYPE_AND_LEN) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", K(ret), K(len), K(pos), K(str_len));
  } else if (OB_FAIL(store_type_and_len(buf, len, pos, type, str_len))) {
    LOG_WARN("fail to store type and len", K(ret));
  } else {
    MEMCPY(buf + pos, str, str_len);
    pos += str_len;
  }
  
  return ret;
}

int Ob20FullLinkTraceTransUtil::store_int1(char *buf, int64_t len, int64_t &pos, int8_t v, int16_t type)
{
  int ret = OB_SUCCESS;
  
  int32_t v_len = 1;
  if (len < pos + FLT_TYPE_AND_LEN + v_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", K(ret), K(len), K(pos));
  } else if (OB_FAIL(store_type_and_len(buf, len, pos, type, v_len))) {
    LOG_WARN("failed to store type and len", K(v), K(buf), K(len)); 
  } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int1(buf, len, v, pos))) {
    LOG_WARN("failed to store int1", K(ret), K(buf));
  } else {
    // do nothing
  }
  
  return ret;
}

int Ob20FullLinkTraceTransUtil::store_int2(char *buf, int64_t len, int64_t &pos, int16_t v, int16_t type)
{
  int ret = OB_SUCCESS;
  
  int32_t v_len = 2;
  if (len < pos + FLT_TYPE_AND_LEN + v_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", K(ret), K(len), K(pos));
  } else if (OB_FAIL(store_type_and_len(buf, len, pos, type, v_len))) {
    LOG_WARN("failed to store type", K(v), K(buf), K(len));
  } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int2(buf, len, v, pos))) {
    LOG_WARN("failed to store int2", K(ret), K(buf));
  } else {
    // do nothing
  }
  
  return ret;
}

int Ob20FullLinkTraceTransUtil::store_int3(char *buf, int64_t len, int64_t &pos, int32_t v, int16_t type)
{
  int ret = OB_SUCCESS;
  
  int32_t v_len = 3;
  if (len < pos + FLT_TYPE_AND_LEN + v_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", K(ret), K(len), K(pos));
  } else if (OB_FAIL(store_type_and_len(buf, len, pos, type, v_len))) {
    LOG_WARN("failed to store type", K(v), K(buf), K(len));
  } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int3(buf, len, v, pos))) {
    LOG_WARN("failed to store int3", K(ret), K(buf));
  } else {
    // do nothing
  }
  
  return ret;
}

int Ob20FullLinkTraceTransUtil::store_int4(char *buf, int64_t len, int64_t &pos, int32_t v, int16_t type)
{
  int ret = OB_SUCCESS;
  
  int32_t v_len = 4;
  if (len < pos + FLT_TYPE_AND_LEN + v_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", K(ret), K(len), K(pos));
  } else if (OB_FAIL(store_type_and_len(buf, len, pos, type, v_len))) {
    LOG_WARN("failed to store type", K(v), K(buf), K(len));
  } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int4(buf, len, v, pos))) {
    LOG_WARN("failed to store int4", K(ret), K(buf));
  } else {
    // do nothing
  }
  
  return ret;
}

int Ob20FullLinkTraceTransUtil::store_int5(char *buf, int64_t len, int64_t &pos, int64_t v, int16_t type)
{
  int ret = OB_SUCCESS;
  
  int32_t v_len = 5;
  if (len < pos + FLT_TYPE_AND_LEN + v_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", K(ret), K(len), K(pos));
  } else if (OB_FAIL(store_type_and_len(buf, len, pos, type, v_len))) {
    LOG_WARN("failed to store type", K(v), K(buf), K(len));
  } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int5(buf, len, v, pos))) {
    LOG_WARN("failed to store int5", K(ret), K(buf));
  } else {
    // do nothing
  }
  
  return ret;
}

int Ob20FullLinkTraceTransUtil::store_int6(char *buf, int64_t len, int64_t &pos, int64_t v, int16_t type)
{
  int ret = OB_SUCCESS;
  
  int32_t v_len = 6;
  if (len < pos + FLT_TYPE_AND_LEN + v_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", K(ret), K(len), K(pos));
  } else if (OB_FAIL(store_type_and_len(buf, len, pos, type, v_len))) {
    LOG_WARN("failed to store type", K(v), K(buf), K(len));
  } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int6(buf, len, v, pos))) {
    LOG_WARN("failed to store int6", K(ret), K(buf));
  } else {
    // do nothing
  }
  
  return ret;
}

int Ob20FullLinkTraceTransUtil::store_int8(char *buf, int64_t len, int64_t &pos, int64_t v, int16_t type)
{
  int ret = OB_SUCCESS;
  
  int32_t v_len = 8;
  if (len < pos + FLT_TYPE_AND_LEN + v_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", K(ret), K(len), K(pos));
  } else if (OB_FAIL(store_type_and_len(buf, len, pos, type, v_len))) {
    LOG_WARN("failed to store type", K(v), K(buf), K(len));
  } else if (OB_FAIL(obmysql::ObMySQLUtil::store_int8(buf, len, v, pos))) {
    LOG_WARN("failed to store int8", K(ret), K(buf));
  } else {
    // do nothing
  }
  
  return ret;
}

int Ob20FullLinkTraceTransUtil::store_double(char *buf, const int64_t len, int64_t &pos, double val, int16_t type)
{
  int ret = OB_SUCCESS;
  
  int32_t v_len = sizeof(double);
  if (len < pos + FLT_TYPE_AND_LEN + v_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", K(ret), K(len), K(pos), K(v_len));
  } else if (OB_FAIL(store_type_and_len(buf, len, pos, type, v_len))) {
    LOG_WARN("failed to store type", K(type), K(buf), K(len));
  } else {
    MEMCPY(buf + pos, &val, v_len);
    pos += v_len;
  }
  
  return ret;
}

int Ob20FullLinkTraceTransUtil::store_float(char *buf, const int64_t len, int64_t &pos, float val, int16_t type)
{
  int ret = OB_SUCCESS;
  
  int32_t v_len = sizeof(float);
  if (len < pos + FLT_TYPE_AND_LEN + v_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", K(ret), K(len), K(pos), K(v_len));
  } else if (OB_FAIL(store_type_and_len(buf, len, pos, type, v_len))) {
    LOG_WARN("failed to store type", K(type), K(buf), K(len));
  } else {
    MEMCPY(buf + pos, &val, v_len);
    pos += v_len;
  }
  
  return ret;
}

int Ob20FullLinkTraceTransUtil::store_uuid(char *buf, const int64_t len, int64_t &pos, trace::UUID uuid, int16_t type)
{
  int ret = OB_SUCCESS;

  int32_t v_len = static_cast<int32_t>(uuid.get_serialize_size());
  if (len < pos + FLT_TYPE_AND_LEN + v_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", K(ret), K(len), K(pos));
  } else if (OB_FAIL(store_type_and_len(buf, len, pos, type, v_len))) {
    LOG_WARN("fail to store type", K(ret));
  } else if (OB_FAIL(uuid.serialize(buf, len, pos))) {
    LOG_WARN("fail to serialize UUID", K(ret));
  } else {
    //nothing
  }
    
  return ret;
}

int Ob20FullLinkTraceTransUtil::resolve_type_and_len(const char *buf,
                                                     int64_t len,
                                                     int64_t &pos, 
                                                     int16_t &type,
                                                     int32_t &v_len)
{
  int ret = OB_SUCCESS;
  
  if (OB_ISNULL(buf)){
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (len < pos + FLT_TYPE_AND_LEN) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", K(ret), K(pos), K(len));
  } else {
    const char *tmp_buf = buf + pos;
    obmysql::ObMySQLUtil::get_int2(tmp_buf, type);
    pos += 2;
    obmysql::ObMySQLUtil::get_int4(tmp_buf, v_len);
    pos += 4;
  }

  return ret;
}

int Ob20FullLinkTraceTransUtil::get_str(const char *buf, int64_t len, int64_t &pos, int64_t str_len, char *&str)
{
  int ret = OB_SUCCESS;
  
  if (pos + str_len > len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size overflow", K(ret), K(pos), K(str_len), K(len));
  } else {
    str = (char *)(buf + pos);
    pos += str_len;
  }

  return ret;
}

int Ob20FullLinkTraceTransUtil::get_int1(const char *buf, int64_t len, int64_t &pos, int64_t v_len, int8_t &val)
{
  int ret = OB_SUCCESS;
  
  if (pos + v_len > len || v_len != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input", K(ret), K(pos), K(v_len), K(len));
  } else {
    const char *data = buf + pos;
    obmysql::ObMySQLUtil::get_int1(data, val);
    pos += 1;
  }
  
  return ret;
}

int Ob20FullLinkTraceTransUtil::get_int2(const char *buf, int64_t len, int64_t &pos, int64_t v_len, int16_t &val)
{
  int ret = OB_SUCCESS;
  
  if (pos + v_len > len || v_len != 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input", K(ret), K(pos), K(v_len), K(len));
  } else {
    const char *data = buf + pos;
    obmysql::ObMySQLUtil::get_int2(data, val);
    pos += 2;
  }
  
  return ret;
}

int Ob20FullLinkTraceTransUtil::get_int3(const char *buf, int64_t len, int64_t &pos, int64_t v_len, int32_t &val)
{
  int ret = OB_SUCCESS;
  
  if (pos + v_len > len || v_len != 3) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input", K(ret), K(pos), K(v_len), K(len));
  } else {
    const char *data = buf + pos;
    obmysql::ObMySQLUtil::get_int3(data, val);
    pos += 3;
  }
  
  return ret;
}

int Ob20FullLinkTraceTransUtil::get_int4(const char *buf, int64_t len, int64_t &pos, int64_t v_len, int32_t &val)
{
  int ret = OB_SUCCESS;

  if (pos + v_len > len || v_len != 4) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input", K(ret), K(pos), K(v_len), K(len));
  } else {
    const char *data = buf + pos;
    obmysql::ObMySQLUtil::get_int4(data, val);
    pos += 4;
  }
  
  return ret;
}

int Ob20FullLinkTraceTransUtil::get_int8(const char *buf, int64_t len, int64_t &pos, int64_t v_len, int64_t &val)
{
  int ret = OB_SUCCESS;
  
  if (pos + v_len > len || v_len != 8) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input", K(ret), K(pos), K(v_len), K(len));
  } else {
    const char *data = buf + pos;
    obmysql::ObMySQLUtil::get_int8(data, val);
    pos += 8;
  }
  
  return ret;
}

int Ob20FullLinkTraceTransUtil::get_double(const char *buf, int64_t len, int64_t &pos, int64_t v_len, double &val)
{
  int ret = OB_SUCCESS;

  int64_t d_len = sizeof(double);
  if (pos + v_len > len || v_len != d_len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input", K(pos), K(v_len), K(len), K(d_len));
  } else {
    val = (*((double *)(buf + pos)));
    pos += d_len;
  }
  
  return ret;
}

int Ob20FullLinkTraceTransUtil::get_float(const char *buf, int64_t len, int64_t &pos, int64_t v_len, float &val)
{
  int ret = OB_SUCCESS;

  int64_t f_len = sizeof(float);
  if (pos + v_len > len || v_len != f_len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input", K(pos), K(v_len), K(len), K(f_len));
  } else {
    val = (*((float *)(buf + pos)));
    pos += f_len;
  }
  
  return ret;
}

int Ob20FullLinkTraceTransUtil::get_uuid(const char *buf, int64_t len, int64_t &pos, int64_t v_len, trace::UUID &uuid)
{
  int ret = OB_SUCCESS;

  int64_t uuid_len = sizeof(trace::UUID);
  if (pos + v_len > len || v_len != uuid_len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input", K(ret), K(pos), K(v_len), K(len), K(uuid_len));
  } else if (OB_FAIL(uuid.deserialize(buf, len, pos))) {
    LOG_WARN("fail to deserialize uuid", K(ret));
  } else {
    //nothing
  }

  return ret;
}

// total size
int64_t Ob20FullLinkTraceTransUtil::get_serialize_size(int64_t seri_size)
{
  return FLT_TYPE_AND_LEN + seri_size;
}

int Ob20FullLinkTraceTransUtil::resolve_type(const char *buf, int64_t len, int64_t &pos, int16_t &type)
{
  int ret = OB_SUCCESS;
  
  if (OB_ISNULL(buf)){
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, null buf ptr.");
  } else if (pos + 2 > len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Failed to check pos len", K(pos), K(len));
  } else {
    const char *data_type = buf + pos;
    obmysql::ObMySQLUtil::get_int2(data_type, type);
    pos += 2;
  }

  return ret;
}


} // common

} // oceanbase

