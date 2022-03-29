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

#define USING_LOG_PREFIX LIB
#include "lib/utility/ob_print_utils.h"
#include "lib/string/ob_string.h"
namespace oceanbase
{
namespace common
{

char get_xdigit(const char c1)
{
  char ret_char = 0;
  if (c1 >= 'a' && c1 <= 'f') {
    ret_char = (char)(c1 - 'a' + 10);
  } else if (c1 >= 'A' && c1 <= 'F') {
    ret_char = (char)(c1 - 'A' + 10);
  } else {
    ret_char = (char)(c1 - '0');
  }
  return (ret_char & 0x0F);
}

char *hex2str(const void *data, const int32_t size)
{
  //TODO: change the log buffer
  static const int32_t BUFFER_SIZE = 1 << 16;
  static __thread char BUFFER[2][BUFFER_SIZE];
  static __thread uint64_t i = 0;
  hex_to_str(data, size, BUFFER[i % 2], BUFFER_SIZE);
  return BUFFER[i++ % 2];
}

int32_t hex_to_str(const void *in_data, const int32_t data_length, void *buff,
                   const int32_t buff_size)
{
  unsigned const char *p = NULL;
  int len = 0;
  int32_t i = 0;
  if (NULL != in_data && NULL != buff && buff_size >= data_length * 2) {
    p = (unsigned const char *)in_data;
    for (; i < data_length; ++i) {
      if (0 > (len = sprintf((char *)buff + i * 2, "%02X", *(p + i)))) {
        // this fun general used by to_string which will ignore return value. so here not print log
      }
    }
  } else {}
  return i;
}

int hex_print(const char* in_buf, int64_t in_len,
              char *buffer, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(in_buf)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < in_len; ++i) {
      if (OB_FAIL(databuff_printf(buffer, buf_len, pos, "%02X",  *(in_buf + i)))) {
      } else {}
    } // end for
  }
  return ret;
}

int32_t str_to_hex(const void *in_data, const int32_t data_length, void *buff,
                   const int32_t buff_size)
{
  unsigned const char *p = NULL;
  unsigned char *o = NULL;
  unsigned char c = 0;
  int32_t i = 0;
  if (NULL != in_data && NULL != buff && buff_size >= data_length / 2) {
    p = (unsigned const char *)in_data;
    o = (unsigned char *)buff;
    c = 0;
    for (i = 0; i < data_length; i++) {
      c = static_cast<unsigned char>(c << 4);
      if (*(p + i) > 'F' ||
          (*(p + i) < 'A' && *(p + i) > '9') ||
          *(p + i) < '0') {
        break;
      }
      if (*(p + i) >= 'A') {
        c = static_cast<unsigned char>(c + (*(p + i) - 'A' + 10));
      } else {
        c = static_cast<unsigned char>(c + (*(p + i) - '0'));
      }
      if (i % 2 == 1) {
        *(o + i / 2) = c;
        c = 0;
      }
    }
  } else {}
  return i;
}
////////////////////////////////////////////////////////////////
template <>
int64_t to_string<int64_t>(const int64_t &v, char *buffer, const int64_t buffer_size)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(databuff_printf(buffer, buffer_size, pos, "%ld", v))) {
  } else {}
  return pos;
}
template <>
int64_t to_string<uint64_t>(const uint64_t &v, char *buffer, const int64_t buffer_size)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(databuff_printf(buffer, buffer_size, pos, "%lu", v))) {
  } else {}
  return pos;
}

template <>
int64_t to_string<double>(const double &v, char *buffer, const int64_t buffer_size)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(databuff_printf(buffer, buffer_size, pos, "%f", v))) {
  } else {}
  return pos;
}

template <>
const char *to_cstring<const char *>(const char *const &str)
{
  return str;
}

template <>
const char *to_cstring<int64_t>(const int64_t &v)
{
  return to_cstring<int64_t, 5>(v);
}

////////////////////////////////////////////////////////////////
int databuff_printf(char *buf, const int64_t buf_len, int64_t &pos, const char *fmt, ...)
{
  int ret = OB_SUCCESS;
  va_list args;
  va_start(args, fmt);
  if (OB_FAIL(databuff_vprintf(buf, buf_len, pos, fmt, args))) {
  } else {}
  va_end(args);
  return ret;
}

int databuff_vprintf(char *buf, const int64_t buf_len, int64_t &pos, const char *fmt, va_list args)
{
  int ret = OB_SUCCESS;
  if (NULL != buf && 0 <= pos && pos < buf_len) {
    int len = vsnprintf(buf + pos, buf_len - pos, fmt, args);
    if (len < 0) {
      ret = OB_ERR_UNEXPECTED;
    } else if (len < buf_len - pos) {
      pos += len;
    } else {
      pos = buf_len - 1;  //skip '\0' written by vsnprintf
      ret = OB_SIZE_OVERFLOW;
    }
  } else {
    ret = OB_SIZE_OVERFLOW;
  }
  return ret;
}

} // end namespace common
} // end namespace oceanbase
