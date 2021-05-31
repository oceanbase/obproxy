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
#include <gtest/gtest.h>
#include "lib/ob_define.h"
#include "obproxy/utils/ob_zlib_stream_compressor.h"
#include "obproxy/proxy/mysqllib/ob_mysql_common_define.h"
#include "lib/oblog/ob_log.h"
#include "rpc/obmysql/ob_mysql_util.h"

namespace oceanbase
{
namespace obproxy
{
using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::proxy;

// test code
class ObMysqlTestUtils
{
public:
  static int covert_hex_to_string(const char *hex_str, int64_t len, char *str);

  static int64_t get_compressed_len(int64_t origin_len) { return origin_len * 2 + 20; }

  static int build_mysql_compress_packet(const char *data, const int64_t data_len,
                                         const uint8_t seq, char *dest, const int64_t dest_len,
                                         int64_t &total_packet_len, const bool is_checksum_on = true);
  static char *get_random_str(char *str, size_t len);

  static int64_t get_us();
};

int64_t ObMysqlTestUtils::get_us()
{
  struct timeval time_val;
  gettimeofday(&time_val, NULL);
  return time_val.tv_sec * 1000000 + time_val.tv_usec;
}

char *ObMysqlTestUtils::get_random_str(char *str, size_t len)
{
  int i;
  for(i=0;i<len;++i)
    str[i]=(char)('A'+(char)(rand()%26));
  str[++i]='\0';
  return str;
}

int ObMysqlTestUtils::covert_hex_to_string(const char *hex_str, int64_t len, char *str)
{
  int ret = OB_SUCCESS;
  if (NULL == hex_str || len <= 0 || NULL == str || len%2 != 0) {
    ret = OB_INVALID_ARGUMENT;

  } else {
    for (int64_t i = 0; i < len/2; i++) {
      char tmp[3];
      tmp[0] = hex_str[2*i];
      tmp[1] = hex_str[2*i + 1];
      tmp[2] = '\0';
      sscanf(tmp, "%x", (unsigned int*)(&str[i]));
    }
    str[len/2] = '\0';
  }
  return ret;
}

int ObMysqlTestUtils::build_mysql_compress_packet(
    const char *data, const int64_t data_len, const uint8_t seq,
    char *dest, const int64_t dest_len, int64_t &total_packet_len, const bool is_checksum_on/*true*/)
{
  int ret = OB_SUCCESS;
  ObZlibStreamCompressor compressor;
  char *body_ptr = dest + MYSQL_COMPRESSED_HEALDER_LENGTH;
  int64_t compress_len = 0;
  int64_t non_compress_len = 0;
  if (is_checksum_on) {
    int64_t body_len = dest_len - MYSQL_COMPRESSED_HEALDER_LENGTH;
    int64_t filled_len = 0;
    if (OB_FAIL(compressor.add_compress_data(data, data_len, true))) {
      PROXY_LOG(WARN, "fail to add compress data", KP(data), K(data_len), K(ret));
    } else if (OB_FAIL(compressor.compress(body_ptr, body_len, filled_len))) {
      PROXY_LOG(WARN, "fail to compress", KP(body_ptr), K(body_len), K(ret));
    } else if (filled_len >= body_len) {
      ret = OB_ERR_UNEXPECTED;
      PROXY_LOG(WARN, "body_len must > filled_len", K(filled_len), K(body_len), K(ret));
    } else {
      compress_len = filled_len;
      non_compress_len = data_len;
    }
  } else {
    MEMCPY(body_ptr, data, data_len);
    compress_len = data_len;
    non_compress_len = 0;
  }
  if (OB_SUCC(ret)) {
    int64_t pos = 0;
    int64_t header_buf_len = MYSQL_COMPRESSED_HEALDER_LENGTH;
    if (OB_FAIL(ObMySQLUtil::store_int3(dest, header_buf_len, static_cast<int32_t>(compress_len), pos))) {
      PROXY_LOG(WARN, "fail to store int3", K(ret));
    } else if (OB_FAIL(ObMySQLUtil::store_int1(dest, header_buf_len, seq, pos))) {
      PROXY_LOG(WARN, "fail to store int", K(ret));
    } else if (OB_FAIL(ObMySQLUtil::store_int3(dest, header_buf_len, static_cast<int32_t>(non_compress_len), pos))) {
      PROXY_LOG(WARN, "fail to store int3", K(ret));
    } else {
      total_packet_len = compress_len + header_buf_len;
    }
  }
  return ret;
}

}
}
