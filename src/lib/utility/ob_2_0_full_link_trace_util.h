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

#ifndef _OBPROXY_OB_2_0_FULL_LINK_TRACE_UTIL_H_
#define _OBPROXY_OB_2_0_FULL_LINK_TRACE_UTIL_H_

#include "lib/trace/ob_trace.h"

namespace oceanbase
{

namespace common
{

/*
 * serialize or deserialize the buf as full link trace format in ob20 protocol payload
 * buf stored in extra info in ob20 payload data
 * format:
 * key: "full_trc"
 * value: "[type1][len1]
           [key_id1][v_len1][value1]
           [key_id2][v_len2][value2]
           ...
           [type2][len2]
           [key_id3][v_len3][value3]
           [key_id4][v_len4][value4]
           ..."
 * byte size: type(2byte), len(4byte), key_id(2byte), v_len(4byte), value(accord to v_len) 
 * attention: 
   1: each type+len define the range of data it covered, check the boundary by len
   2: there could be more than one type+len in each ob20 payload data
   3: the key_id is defined previously, which is the same for client & proxy & server
 *
 */

#define FLT_TYPE_LEN 2
#define FLT_TOTAL_LEN 4
#define FLT_TYPE_AND_LEN (FLT_TYPE_LEN + FLT_TOTAL_LEN)

class Ob20FullLinkTraceTransUtil
{
public:
  // save type+len
  static int store_type_and_len(char *buf, int64_t len, int64_t &pos, int16_t type, int32_t v_len);
  static int store_str(char *buf, int64_t len, int64_t &pos, const char *str, const int32_t str_len, int16_t type);

  // serialize data
  static int store_int1(char *buf, int64_t len, int64_t &pos, int8_t v, int16_t type);
  static int store_int2(char *buf, int64_t len, int64_t &pos, int16_t v, int16_t type);
  static int store_int3(char *buf, int64_t len, int64_t &pos, int32_t v, int16_t type);
  static int store_int4(char *buf, int64_t len, int64_t &pos, int32_t v, int16_t type);
  static int store_int5(char *buf, int64_t len, int64_t &pos, int64_t v, int16_t type);
  static int store_int6(char *buf, int64_t len, int64_t &pos, int64_t v, int16_t type);
  static int store_int8(char *buf, int64_t len, int64_t &pos, int64_t v, int16_t type);

  static int store_double(char *buf, const int64_t len, int64_t &pos, double val, int16_t type);
  static int store_float(char *buf, const int64_t len, int64_t &pos, float val, int16_t type);
  static int store_uuid(char *buf, const int64_t len, int64_t &pos, trace::UUID uuid, int16_t type);
      
  // decode type+len
  static int resolve_type_and_len(const char *buf, int64_t len, int64_t &pos, int16_t &type, int32_t &v_len);
  static int get_str(const char *buf, int64_t len, int64_t &pos, int64_t str_len, char *&str);

  // deserialize data
  static int get_int1(const char *buf, int64_t len, int64_t &pos, int64_t v_len, int8_t &val);
  static int get_int2(const char *buf, int64_t len, int64_t &pos, int64_t v_len, int16_t &val);
  static int get_int3(const char *buf, int64_t len, int64_t &pos, int64_t v_len, int32_t &val);
  static int get_int4(const char *buf, int64_t len, int64_t &pos, int64_t v_len, int32_t &val);
  static int get_int8(const char *buf, int64_t len, int64_t &pos, int64_t v_len, int64_t &val);

  static int get_double(const char *buf, int64_t len, int64_t &pos, int64_t v_len, double &val);
  static int get_float(const char *buf, int64_t len, int64_t &pos, int64_t v_len, float &val);
  static int get_uuid(const char *buf, int64_t len, int64_t &pos, int64_t v_len, trace::UUID &uuid);
  
  // total size
  static int64_t get_serialize_size(int64_t seri_size);

  // resolve new extra info protocol, extra id(type)
  static int resolve_type(const char *buf, int64_t len, int64_t &pos, int16_t &type);

private:
  DISALLOW_COPY_AND_ASSIGN(Ob20FullLinkTraceTransUtil);
};


} // common

} // oceanbase

#endif  // _OBPROXY_OB_2_0_FULL_LINK_TRACE_UTIL_H_

