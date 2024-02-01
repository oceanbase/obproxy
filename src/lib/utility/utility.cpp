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

#include "lib/utility/utility.h"
#include "lib/ob_define.h"
#include "easy_inet.h"
#include "common/ob_rowkey.h"
#include "lib/time/ob_time_utility.h"
#include "lib/file/file_directory_utils.h"
#include "common/ob_range2.h"
#include "lib/string/ob_sql_string.h"
namespace oceanbase
{
namespace common
{
extern "C"
{
  void do_breakpad_init() __attribute__((weak));
  void do_breakpad_init()
  {}
}

void hex_dump(const void *data, const int32_t size,
              const bool char_type /*= true*/, const int32_t log_level /*= OB_LOG_LEVEL_DEBUG*/)
{
  if (OB_LOGGER.get_log_level() < log_level) { return; }
  /* dumps size bytes of *data to stdout. Looks like:
   * [0000] 75 6E 6B 6E 6F 77 6E 20
   * 30 FF 00 00 00 00 39 00 unknown 0.....9.
   * (in a single line of course)
   */

  unsigned const char *p = (unsigned char *)data;
  unsigned char c = 0;
  int n = 0;
  char bytestr[4] = {0};
  char addrstr[10] = {0};
  char hexstr[ 16 * 3 + 5] = {0};
  char charstr[16 * 1 + 5] = {0};

  for (n = 1; n <= size; n++) {
    if (n % 16 == 1) {
      /* store address for this line */
      IGNORE_RETURN snprintf(addrstr, sizeof(addrstr), "%.4x",
                             (int)((unsigned long)p - (unsigned long)data));
    }

    c = *p;
    if (isprint(c) == 0) {
      c = '.';
    }

    /* store hex str (for left side) */
    IGNORE_RETURN snprintf(bytestr, sizeof(bytestr), "%02X ", *p);
    strncat(hexstr, bytestr, sizeof(hexstr) - strlen(hexstr) - 1);

    /* store char str (for right side) */
    IGNORE_RETURN snprintf(bytestr, sizeof(bytestr), "%c", c);
    strncat(charstr, bytestr, sizeof(charstr) - strlen(charstr) - 1);

    if (n % 16 == 0) {
      /* line completed */
      if (char_type)
        _OB_NUM_LEVEL_LOG(log_level, "[%ld] [%4.4s]   %-50.50s  %s\n",
                          pthread_self(), addrstr, hexstr, charstr);
      else
        _OB_NUM_LEVEL_LOG(log_level, "[%ld] [%4.4s]   %-50.50s\n",
                          pthread_self(), addrstr, hexstr);
      hexstr[0] = 0;
      charstr[0] = 0;
    } else if (n % 8 == 0) {
      /* half line: add whitespaces */
      strncat(hexstr, "  ", sizeof(hexstr) - strlen(hexstr) - 1);
      strncat(charstr, " ", sizeof(charstr) - strlen(charstr) - 1);
    }
    p++; /* next byte */
  }

  if (strlen(hexstr) > 0) {
    /* print rest of buffer if not empty */
    if (char_type)
      _OB_NUM_LEVEL_LOG(log_level, "[%ld] [%4.4s]   %-50.50s  %s\n",
                        pthread_self(), addrstr, hexstr, charstr);
    else
      _OB_NUM_LEVEL_LOG(log_level, "[%ld] [%4.4s]   %-50.50s\n",
                        pthread_self(), addrstr, hexstr);
  }
}
int32_t parse_string_to_int_array(const char *line,
                                  const char del, int32_t *array, int32_t &size)
{
  int ret = 0;
  const char *start = line;
  const char *p = NULL;
  char buffer[OB_MAX_ROW_KEY_LENGTH];

  if (NULL == line || NULL == array || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
  }

  int32_t idx = 0;
  if (OB_SUCC(ret)) {
    while (OB_SUCC(ret) && NULL != start) {
      p = strchr(start, del);
      if (NULL != p) {
        memset(buffer, 0, OB_MAX_ROW_KEY_LENGTH);
        strncpy(buffer, start, p - start);
        if (strlen(buffer) > 0) {
          if (idx >= size) {
            ret = OB_SIZE_OVERFLOW;
            break;
          } else {
            array[idx++] = static_cast<int32_t>(strtol(buffer, NULL, 10));
          }
        }
        start = p + 1;
      } else {
        if (strlen(start) > OB_MAX_ROW_KEY_LENGTH - 1) {
          ret = OB_SIZE_OVERFLOW;
          break;
        } else {
          memset(buffer, 0, OB_MAX_ROW_KEY_LENGTH);
          strcpy(buffer, start);
          if (strlen(buffer) > 0) {
            if (idx >= size) {
              ret = OB_SIZE_OVERFLOW;
              break;
            } else {
              array[idx++] = static_cast<int32_t>(strtol(buffer, NULL, 10));
            }
          }
          break;
        }
      }
    }

    if (OB_SUCC(ret)) { size = idx; }
  }
  return ret;
}

int escape_enter_symbol(char *buffer, const int64_t length, int64_t &pos, const char *src)
{
  int ret = OB_SUCCESS;
  int64_t copy_size = pos;
  int64_t src_len = strlen(src);
  if (pos + src_len >= length || NULL == buffer) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    char escape = 0;;
    for (int i = 0; OB_SUCC(ret) && i < src_len; ++i) {
      escape = 0;
      switch (src[i]) {
        case '\n':
          escape = 'n';
          break;
        default:
          break;
      }
      if (escape) {
        if (copy_size >= length - 2) {
          ret = OB_BUF_NOT_ENOUGH;
          break;
        }
        buffer[pos++] = '\\';
        buffer[pos++] = escape;
        copy_size += 2;
      } else {
        if (copy_size >= length - 1) {
          ret = OB_BUF_NOT_ENOUGH;
          break;
        }
        buffer[pos++] = src[i];
        copy_size += 1;
      }
    }
    if (copy_size < length) {
      buffer[pos] = '\0';
    } else {
      ret = OB_BUF_NOT_ENOUGH;
    }
  }
  return ret;
}
/**
 * only escape " and \, used to stringify range2str
 */
int escape_range_string(char *buffer, const int64_t length, int64_t &pos, const ObString &in)
{
  int ret = OB_SUCCESS;
  int64_t copy_size = 0;
  if (pos + in.length() >= length || NULL == buffer) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    char escape = 0;;
    for (int i = 0; OB_SUCC(ret) && i < in.length(); ++i) {
      escape = 0;
      switch (in.ptr()[i]) {
        case '"':
          escape = '"';
          break;
        case '\\':
          escape = '\\';
          break;
        default:
          break;
      }
      if (escape) {
        if (copy_size >= length - 2) {
          ret = OB_BUF_NOT_ENOUGH;
          break;
        }
        buffer[pos++] = '\\';
        buffer[pos++] = escape;
        copy_size += 2;
      } else {
        if (copy_size >= length - 1) {
          ret = OB_BUF_NOT_ENOUGH;
          break;
        }
        buffer[pos++] = in.ptr()[i];
        copy_size += 1;
      }
    }
  }
  return ret;
}

int convert_comment_str(char *comment_str)
{
  int ret = OB_SUCCESS;
  if (comment_str == NULL) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (ret = replace_str(comment_str, strlen(comment_str), "\\n", "\n"))) {
    _OB_LOG(WDIAG, "replace \\n to enter failed, src_str=%s, ret=%d", comment_str, ret);
  }
  return ret;
}

int64_t lower_align(int64_t input, int64_t align)
{
  int64_t ret = input;
  ret = (input + align - 1) & ~(align - 1);
  ret = ret - ((ret - input + align - 1) & ~(align - 1));
  return ret;
};

int64_t upper_align(int64_t input, int64_t align)
{
  int64_t ret = input;
  ret = (input + align - 1) & ~(align - 1);
  return ret;
};

bool is2n(int64_t input)
{
  return (((~input + 1) & input) == input);
};

int64_t next_pow2(const int64_t x)
{
  return x ? (1ULL << (8 * sizeof(int64_t) - __builtin_clzll(x - 1))) : 1;
}

bool all_zero(const char *buffer, const int64_t size)
{
  bool bret = true;
  const char *buffer_end = buffer + size;
  const char *start = (char *)upper_align((int64_t)buffer, sizeof(int64_t));
  start = std::min(start, buffer_end);
  const char *end = (char *)lower_align((int64_t)(buffer + size), sizeof(int64_t));
  end = std::max(end, buffer);

  bret = all_zero_small(buffer, start - buffer);
  if (bret) {
    bret = all_zero_small(end, buffer + size - end);
  }
  if (bret) {
    const char *iter = start;
    while (iter < end) {
      if (0 != *((int64_t *)iter)) {
        bret = false;
        break;
      }
      iter += sizeof(int64_t);
    }
  }
  return bret;
};

bool all_zero_small(const char *buffer, const int64_t size)
{
  bool bret = true;
  if (NULL != buffer) {
    for (int64_t i = 0; i < size; i++) {
      if (0 != *(buffer + i)) {
        bret = false;
        break;
      }
    }
  }
  return bret;
}

const char *get_file_path(const char *file_dir, const char *file_name)
{
  static __thread char file_path[OB_MAX_FILE_NAME_LENGTH];
  file_path[0] = '\0';
  if (NULL != file_dir) {
    IGNORE_RETURN snprintf(file_path, sizeof(file_path), "%s/%s", file_dir, file_name);
  }
  return file_path;
}

char *str_trim(char *str)
{
  char *p = str, *sa = str;
  while (*p) {
    if (*p != ' ') {
      *str++ = *p;
    }
    p++;
  }
  *str = 0;
  return sa;
}

char *ltrim(char *str)
{
  char *p = str;
  while (p != NULL && *p != '\0' && isspace(*p)) {
    ++p;
  }
  return p;
}

char *rtrim(char *str)
{
  if ((str != NULL) && *str != '\0') {
    char *p = str + strlen(str) - 1;
    while ((p > str) && isspace(*p)) {
      --p;
    }
    *(p + 1) = '\0';
  }
  return str;
}
const char *inet_ntoa_r(const uint64_t ipport)
{
  static const int64_t BUFFER_SIZE = 32;
  static __thread char buffers[2][BUFFER_SIZE];
  static __thread uint64_t i = 0;
  char *buffer = buffers[i++ % 2];
  buffer[0] = '\0';

  uint32_t ip = (uint32_t)(ipport & 0xffffffff);
  int port = (int)((ipport >> 32) & 0xffff);
  unsigned char *bytes = (unsigned char *) &ip;
  if (port > 0) {
    IGNORE_RETURN snprintf(buffer, BUFFER_SIZE, "%d.%d.%d.%d:%d", bytes[0], bytes[1], bytes[2], bytes[3], port);
  } else {
    IGNORE_RETURN snprintf(buffer, BUFFER_SIZE, "%d.%d.%d.%d:-1", bytes[0], bytes[1], bytes[2], bytes[3]);
  }

  return buffer;
}

const char *inet_ntoa_r(const uint32_t ip)
{
  static const int64_t BUFFER_SIZE = 32;
  static __thread char buffers[2][BUFFER_SIZE];
  static __thread uint64_t i = 0;
  char *buffer = buffers[i++ % 2];
  buffer[0] = '\0';

  unsigned char *bytes = (unsigned char *) &ip;
  IGNORE_RETURN snprintf(buffer, BUFFER_SIZE, "%d.%d.%d.%d", bytes[0], bytes[1], bytes[2], bytes[3]);

  return buffer;
}

/**
 * sockaddr_in to string
 */
char *easy_inet_addr_to_str(easy_addr_t *addr, char *buffer, int len)
{
  unsigned char *b = NULL;
  int64_t ret_len = 0;
  if (AF_INET6 == addr->family) {
    char tmp[INET6_ADDRSTRLEN];
    if (NULL != inet_ntop(AF_INET6, addr->u.addr6, tmp, INET6_ADDRSTRLEN)) {
      if (addr->port) {
        ret_len = snprintf(buffer, len, "[%s]:%d", tmp, (ntohs)(addr->port));
        if (OB_UNLIKELY(ret_len <= 0) || OB_UNLIKELY(ret_len >= len)) {
          // do nothing
        }
      } else {
        ret_len = snprintf(buffer, len, "%s", tmp);
        if (OB_UNLIKELY(ret_len <= 0) || OB_UNLIKELY(ret_len >= len)) {
          // do nothing
        }
      }
    }
  } else {
    b = reinterpret_cast<unsigned char *>(&addr->u.addr);
    if (addr->port) {
      ret_len = snprintf(buffer, len, "%d.%d.%d.%d:%d", b[0], b[1], b[2], b[3], (ntohs)(addr->port));
      if (OB_UNLIKELY(ret_len <= 0) || OB_UNLIKELY(ret_len >= len)) {
        // do nothing
      }
    } else {
      ret_len = snprintf(buffer, len, "%d.%d.%d.%d", b[0], b[1], b[2], b[3]);
      if (OB_UNLIKELY(ret_len <= 0) || OB_UNLIKELY(ret_len >= len)) {
        // do nothing
      }
    }
  }
  return buffer;
}

const char *inet_ntoa_r(easy_addr_t addr)
{
  static const int64_t BUFFER_SIZE = 64;
  static __thread char buffers[2][BUFFER_SIZE];
  static __thread uint64_t i = 0;
  char *buffer = buffers[i++ % 2];
  buffer[0] = '\0';
  return oceanbase::common::easy_inet_addr_to_str(&addr, buffer, BUFFER_SIZE);
}

const char *time2str(const int64_t time_us, const char *format)
{
  static const int32_t BUFFER_SIZE = 1024;
  static __thread char buffer[10][BUFFER_SIZE];
  static __thread uint64_t i = 0;
  buffer[i % 10][0] = '\0';
  struct tm time_struct;
  int64_t time_s = time_us / 1000000;
  int64_t cur_second_time_us = time_us % 1000000;
  if (NULL != localtime_r(&time_s, &time_struct)) {
    int64_t pos = strftime(buffer[i % 10], BUFFER_SIZE, format, &time_struct);
    if (pos < BUFFER_SIZE) {
      IGNORE_RETURN snprintf(&buffer[i % 10][pos], BUFFER_SIZE - pos, ".%ld %ld", cur_second_time_us, time_us);
    }
  }
  return buffer[i++ % 10];
}

const char *obj_time2str(const int64_t time_us)
{
  const char *format = DEFAULT_TIME_FORMAT;
  static const int32_t BUFFER_SIZE = 1024;
  static __thread char buffer[BUFFER_SIZE];
  buffer[0] = '\0';
  struct tm time_struct;
  int64_t time_s = time_us / 1000000;
  int64_t cur_second_time_us = time_us % 1000000;
  if (NULL != localtime_r(&time_s, &time_struct)) {
    int64_t pos = strftime(buffer, BUFFER_SIZE, format, &time_struct);
    if (pos < BUFFER_SIZE) {
      IGNORE_RETURN snprintf(&buffer[pos], BUFFER_SIZE - pos, ".%06ld", cur_second_time_us);
    }
  }
  return buffer;
}

void print_rowkey(FILE *fd, ObString &rowkey)
{
  char *pchar = rowkey.ptr();
  for (int i = 0; i < rowkey.length(); ++i, pchar++) {
    if (isprint(*pchar)) {
      fprintf(fd, "%c", *pchar);
    } else {
      fprintf(fd, "\\%hhu", *pchar);
    }
  }
}

int mem_chunk_serialize(char *buf, int64_t len, int64_t &pos, const char *data, int64_t data_len)
{
  int err = OB_SUCCESS;
  int64_t tmp_pos = pos;
  if (NULL == buf || len <= 0 || pos < 0 || pos > len || NULL == data || 0 > data_len) {
    err = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (err = serialization::encode_i64(buf, len, tmp_pos, data_len))) {
    _OB_LOG(EDIAG, "encode_i64(buf=%p, len=%ld, pos=%ld, i=%ld)=>%d", buf, len, tmp_pos, data_len,
            err);
  } else if (tmp_pos + data_len > len) {
    err = OB_SERIALIZE_ERROR;
  } else {
    MEMCPY(buf + tmp_pos, data, data_len);
    tmp_pos += data_len;
    pos = tmp_pos;
  }
  return err;
}

int mem_chunk_deserialize(const char *buf, int64_t len, int64_t &pos, char *data, int64_t data_len,
                          int64_t &real_len)
{
  int err = OB_SUCCESS;
  int64_t tmp_pos = pos;
  if (NULL == buf || len <= 0 || pos < 0 || pos > len || NULL == data || data_len < 0) {
    err = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (err = serialization::decode_i64(buf, len, tmp_pos, &real_len))) {
    _OB_LOG(EDIAG, "decode_i64(buf=%p, len=%ld, pos=%ld, i=%ld)=>%d", buf, len, tmp_pos, real_len,
            err);
  } else if (real_len > data_len || tmp_pos + real_len > len) {
    err = OB_DESERIALIZE_ERROR;
  } else {
    MEMCPY(data, buf + tmp_pos, real_len);
    tmp_pos += real_len;
    pos = tmp_pos;
  }
  return err;
}

int64_t min(const int64_t x, const int64_t y)
{
  return x > y ? y : x;
}

int64_t max(const int64_t x, const int64_t y)
{
  return x < y ? y : x;
}

int get_double_expand_size(int64_t &current_size, const int64_t limit_size)
{
  int ret = OB_SUCCESS;
  int64_t new_size = current_size << 1;
  if (current_size > limit_size) {
    ret = OB_SIZE_OVERFLOW;
  } else if (new_size > limit_size) {
    current_size = limit_size;
  } else {
    current_size = new_size;
  }
  return ret;
}

uint16_t bswap16(uint16_t a)
{
  return static_cast<uint16_t>(((a >> 8) & 0xFFU) | ((a << 8) & 0xFF00U));
}

bool str_isprint(const char *str, const int64_t length)
{
  bool bret = false;
  if (NULL != str
      && 0 != length) {
    bret = true;
    for (int64_t i = 0; i < length; i++) {
      if (!isprint(str[i])) {
        bret = false;
        break;
      }
    }
  }
  return bret;
}


int replace_str(char *src_str, const int64_t src_str_buf_size,
                const char *match_str, const char *replace_str)
{
  int ret                 = OB_SUCCESS;
  int64_t str_len         = 0;
  int64_t match_str_len   = 0;
  int64_t replace_str_len = 0;
  const char *find_pos    = NULL;
  char new_str[OB_MAX_EXPIRE_INFO_STRING_LENGTH];

  if (NULL == src_str || src_str_buf_size <= 0
      || NULL == match_str || NULL == replace_str) {
    _OB_LOG(WDIAG, "invalid param, src_str=%p, src_str_buf_size=%ld, "
            "match_str=%p, replace_str=%p",
            src_str, src_str_buf_size, match_str, replace_str);
    ret = OB_ERROR;
  } else if (NULL != (find_pos = strstr(src_str, match_str))) {
    match_str_len = strlen(match_str);
    replace_str_len = strlen(replace_str);
    while (OB_SUCC(ret) && NULL != find_pos) {
      str_len = find_pos - src_str + replace_str_len
          + strlen(find_pos + match_str_len);
      if (str_len >= OB_MAX_EXPIRE_INFO_STRING_LENGTH
          || str_len >= src_str_buf_size) {
        _OB_LOG(WDIAG, "str after replace is too large, new_size=%ld, "
                "new_buf_size=%ld, src_str_buf_size=%ld",
                str_len, OB_MAX_EXPIRE_INFO_STRING_LENGTH, src_str_buf_size);
        ret = OB_ERROR;
        break;
      } else {
        memset(new_str, 0, OB_MAX_EXPIRE_INFO_STRING_LENGTH);
        strncpy(new_str, src_str, find_pos - src_str);
        strcat(new_str, replace_str);
        strcat(new_str, find_pos + match_str_len);
        strcpy(src_str, new_str);
      }

      find_pos = strstr(src_str, match_str);
    }
  }

  return ret;
}

int get_ethernet_speed(const char *devname, int64_t &speed)
{
  int rc = OB_SUCCESS;
  if (NULL == devname) {
    _OB_LOG(WDIAG, "invalid devname %p", devname);
    rc = OB_INVALID_ARGUMENT;
  } else {
    rc = get_ethernet_speed(ObString::make_string(devname), speed);
  }
  return rc;
}

int get_ethernet_speed(const ObString &devname, int64_t &speed)
{
  int rc = OB_SUCCESS;
  bool exist = false;
  char path[OB_MAX_FILE_NAME_LENGTH];
  if (0 == devname.length()) {
    _OB_LOG(WDIAG, "empty devname");
    rc = OB_INVALID_ARGUMENT;
  } else {
    IGNORE_RETURN snprintf(path, sizeof(path), "/sys/class/net/%.*s", devname.length(), devname.ptr());
    if (OB_SUCCESS != (rc = FileDirectoryUtils::is_exists(path, exist)) || !exist) {
      _OB_LOG(WDIAG, "path %s not exist", path);
      rc = OB_FILE_NOT_EXIST;
    }
  }
  if (OB_SUCCESS != rc)
  {}
  else {
    CharArena alloc;
    ObString str;
    IGNORE_RETURN snprintf(path, sizeof(path), "/sys/class/net/%.*s/bonding/",
                           devname.length(), devname.ptr());
    if (OB_SUCCESS != (rc = FileDirectoryUtils::is_exists(path, exist))) {
      LIB_LOG(WDIAG, "check net file if exists failed.", K(rc));
    } else if (exist) {
      IGNORE_RETURN snprintf(path, sizeof(path), "/sys/class/net/%.*s/bonding/slaves",
                             devname.length(), devname.ptr());
      if (OB_SUCCESS != (rc = load_file_to_string(path, alloc, str))) {
        _OB_LOG(WDIAG, "load file %s failed, rc %d", path, rc);
      } else if (0 == str.length()) {
        _OB_LOG(WDIAG, "can't get slave ethernet");
        rc = OB_ERROR;
      } else {
        int len = 0;
        while (len < str.length() && !isspace(str.ptr()[len])) {
          len++;
        }
        IGNORE_RETURN snprintf(path, sizeof(path), "/sys/class/net/%.*s/speed", len, str.ptr());
      }
    } else {
      IGNORE_RETURN snprintf(path, sizeof(path), "/sys/class/net/%.*s/speed",
                             devname.length(), devname.ptr());
    }
    if (OB_SUCCESS == rc) {
      if (OB_SUCCESS != (rc = load_file_to_string(path, alloc, str))) {
        _OB_LOG(WDIAG, "load file %s failed, rc %d", path, rc);
      } else {
        speed = atoll(str.ptr());
        speed = speed * 1024 * 1024 / 8;
      }
    }
  }
  return rc;
}

bool is_case_space_equal(const char *s1, int64_t s1_len, const char *s2, int64_t s2_len)
{
  int result = true;

  //Check input
  if (NULL == s1 || NULL == s2 || s1_len < 0 || s2_len < 0) {
    result = false;
    _OB_LOG(EDIAG,
            "Invalid argument, input arguments include NULL pointer or string length is less than zero.");
  } else if (s1 != s2) { //If s1 == s2,return 1
    while (1) {
      //Left trim
      while (s1_len != 0 && isspace(*s1)) {
        --s1_len;
        ++s1;
      }

      while (s2_len != 0 && isspace(*s2)) {
        --s2_len;
        ++s2;
      }

      //To stop while(1).
      if (0 == s1_len && 0 == s2_len) {
        result = true;
        break;
      } else if (0 == s1_len || 0 == s2_len) {
        result = false;
        break;
      }

      if (false == result) {
        break;
      }

      //Compare chars bettween s1 and s2
      while (s1_len != 0 && s2_len != 0) {
        if (tolower(*s1) != tolower(*s2)) {
          result = false;
          break;
        }

        int s1_is_space = 0;
        int s2_is_space = 0;

        if (--s1_len != 0) {
          s1_is_space = isspace(*(++s1));
        }

        if (--s2_len != 0) {
          s2_is_space = isspace(*(++s2));
        }

        if (s1_is_space != s2_is_space) {
          result = false;
          break;
        } else if (s1_is_space && s2_is_space) {
          break;
        }
      } // end of while (s1_len != 0 && s2_len != 0)
    } // end of while(1)
  } // end of else if (s1 != s2)

  return result;
}

bool is_n_case_space_equal(const char *s1, int64_t s1_len, const char *s2, int64_t s2_len,
                           int64_t cmp_len)
{
  bool result = true;

  //Check input
  if (NULL == s1 || NULL == s2 || s1_len < 0 || s2_len < 0) {
    result = false;
    _OB_LOG(EDIAG,
            "Invalid argument, input arguments include NULL pointer or string length is less than zero.");
  } else if (s1 != s2) { //If s1 == s2,return 1
    while (1) {
      //Left trim
      while (s1_len != 0 && isspace(*s1)) {
        --s1_len;
        ++s1;
      }

      while (s2_len != 0 && isspace(*s2)) {
        --s2_len;
        ++s2;
      }

      //To stop while(1).
      if (cmp_len <= 0 || (0 == s1_len && 0 == s2_len)) {
        result = true;
        break;
      } else if (0 == s1_len || 0 == s2_len) {
        result = false;
        break;
      }

      if (false == result) {
        break;
      }

      //Compare chars bettween s1 and s2
      while (cmp_len > 0 && s1_len != 0 && s2_len != 0) {
        if (tolower(*s1) != tolower(*s2)) {
          result = false;
          break;
        }

        --cmp_len;

        int s1_is_space = 0;
        int s2_is_space = 0;

        if (--s1_len != 0) {
          s1_is_space = isspace(*(++s1));
        }

        if (--s2_len != 0) {
          s2_is_space = isspace(*(++s2));
        }

        if (s1_is_space != s2_is_space) {
          result = false;
          break;
        } else if (s1_is_space && s2_is_space) {
          --cmp_len;
          break;
        }
      } // end of while (cmp_len > 0 && s1_len != 0 && s2_len != 0)
    } // end of while(1)
  } // end of if (s1 != s2)

  return result;
}

int wild_compare(const char *str, const char *wild_str, const bool str_is_pattern)
{
  const char wild_many = '%';
  const char wild_one = '_';
  const char wild_prefix = '\\';
  char cmp = 0;
  const int EQUAL = 0;
  const int UNEQUAL = 1;
  const int UNKOWN = -1;
  int ret = UNKOWN;
  if (NULL != str && NULL != wild_str) {
    while (*wild_str) {
      while (*wild_str && *wild_str != wild_many && *wild_str != wild_one) {
        if (*wild_str == wild_prefix && wild_str[1]) {
          wild_str++;
          if (str_is_pattern && *str++ != wild_prefix) {
            ret = UNEQUAL;
            break;
          }
        }
        if (*wild_str++ != *str++) {
          ret = UNEQUAL;
          break;
        }
      }
      if (UNKOWN != ret) {
        break;
      }
      if (! *wild_str ) {
        ret = (*str != 0) ? UNEQUAL : EQUAL;
        break;
      }
      if (*wild_str++ == wild_one) {
        if (! *str || (str_is_pattern && *str == wild_many)) {
          ret = UNEQUAL;                     // One char; skip
          break;
        }
        if (*str++ == wild_prefix && str_is_pattern && *str) {
          str++;
        }
      } else {						// Found wild_many
        while (str_is_pattern && *str == wild_many) {
          str++;
        }
        for (; *wild_str ==  wild_many || *wild_str == wild_one; wild_str++) {
          if (*wild_str == wild_many) {
            while (str_is_pattern && *str == wild_many)
              str++;
          }
          else {
            if (str_is_pattern && *str == wild_prefix && str[1]) {
              str+=2;
            } else if (! *str++) {
              ret = UNEQUAL;
              break;
            }
          }
        }
        if (!*wild_str) {
          ret = EQUAL;		// wild_many as last char: OK
          break;
        }
        if ((cmp= *wild_str) == wild_prefix && wild_str[1] && !str_is_pattern) {
          cmp=wild_str[1];
        }
        for (;;str++) {
          while (*str && *str != cmp) {
            str++;
          }
          if (!*str) {
            ret = UNEQUAL;
            break;
          }
          if (wild_compare(str,wild_str,str_is_pattern) == 0) {
            ret = EQUAL;
            break;
          }
        }
      }
    }
    if (UNKOWN == ret) {
      ret = (*str != 0) ? UNEQUAL : EQUAL;
    }
  }
  return ret;
}

int wild_compare(const ObString &str, const ObString &wild_str, const bool str_is_pattern)
{
  const char wild_many = '%';
  const char wild_one = '_';
  const char wild_prefix = '\\';
  char cmp = 0;
  const int EQUAL = 0;
  const int UNEQUAL = 1;
  const int UNKOWN = -1;
  int ret = UNKOWN;
  const char *str_ptr = str.ptr();
  const char *wild_ptr = wild_str.ptr();
  const ObString::obstr_size_t str_length = str.length();
  const ObString::obstr_size_t wild_length = wild_str.length();
  ObString::obstr_size_t str_pos = 0;
  ObString::obstr_size_t wild_pos = 0;
#define INC_STR()                               \
  str_ptr++;                                    \
  str_pos++;

#define INC_WILD()                              \
  wild_ptr++;                                   \
  wild_pos++;

  if (NULL != str_ptr && NULL != wild_ptr) {
    while (wild_pos < wild_length) {
      //check char without wild_many or wild_one.
      while (wild_pos < wild_length && *wild_ptr != wild_many && *wild_ptr != wild_one) {
        if (*wild_ptr == wild_prefix && (wild_pos+1 < wild_length)) {
          INC_WILD();
          if (str_is_pattern) {
            if (str_pos == str_length || (str_pos < str_length && *str_ptr != wild_prefix)) {
              ret = UNEQUAL;
              break;
            }
            INC_STR();
          }
        }
        if ((str_pos >= str_length
             || (str_pos < str_length && wild_pos < wild_length && *wild_ptr != *str_ptr))) {
          ret = UNEQUAL;
          break;
        }
        INC_STR();
        INC_WILD();
      }

      if (UNKOWN != ret) {
        break;
      }
      if (wild_pos >= wild_length) {
        ret = (str_pos < str_length) ? UNEQUAL : EQUAL;
        break;
      }
      if (wild_pos < wild_length && *wild_ptr == wild_one) {
        INC_WILD();
        if (str_pos >= str_length
            || (str_is_pattern && (str_pos < str_length && *str_ptr == wild_many))) {
          ret = UNEQUAL;       // One char; skip
          break;
        }
        if (str_pos + 1 < str_length && *str_ptr == wild_prefix && str_is_pattern) {
          INC_STR();
        }
        INC_STR();
      } else {
        INC_WILD();
        while (str_is_pattern && str_pos < str_length && *str_ptr == wild_many) {
          INC_STR();
        }
        for (; wild_pos < wild_length && (*wild_ptr == wild_many || *wild_ptr == wild_one);
             wild_ptr++, wild_pos++) {
          if (*wild_ptr == wild_many) {
            while (str_is_pattern && str_pos < str_length && *str_ptr == wild_many) {
              INC_STR();
            }
          } else {
            if (str_is_pattern && str_pos + 1 < str_length && *str_ptr == wild_prefix) {
              INC_STR();
            } else if (str_pos >= str_length) {
              ret = UNEQUAL;
              break;
            }
            INC_STR();
          }
        }
        if (wild_pos >= wild_length) {
          ret = EQUAL;      // wild_many as last char: OK
          break;
        }
        if ((cmp = *wild_ptr) == wild_prefix && wild_pos + 1 < wild_length && !str_is_pattern) {
          cmp = wild_ptr[1];
        }
        for (;; str_pos++, str_ptr++) {
          while (str_pos < str_length && *str_ptr != cmp) {
            INC_STR();
          }
          if (str_pos >= str_length) {
            ret = EQUAL;
            break;
          }
          if (wild_compare(ObString(str_length - str_pos, str_ptr),
                           ObString(wild_length - wild_pos, wild_ptr), str_is_pattern) == 0) {
            ret = EQUAL;
            break;
          }
        }
      }
    }
    if (UNKOWN == ret) {
      ret = (str_pos < str_length) ? UNEQUAL : EQUAL;
    }
  }
#undef INC_STR
#undef INC_WILD
  return ret;
}

uint64_t get_sort(uint count, ...)
{
  const char wild_many = '%';
  const char wild_one = '_';
  const char wild_prefix = '\\';
  va_list args;
  va_start(args,count);
  ulong sort=0;

  // Should not use this function with more than 8 arguments for compare.
  if (count <= 8) {
    while (count--) {
      char *start = NULL;
      char *str= va_arg(args,char*);
      uint chars= 0;
      uint wild_pos= 0;           // first wildcard position

      if ((start = str)) {
        for (; *str ; str++) {
          if (*str == wild_prefix && str[1]) {
            str++;
          } else if (*str == wild_many || *str == wild_one) {
            wild_pos= (uint) (str - start) + 1;
            break;
          }
          chars= 128;                             // Marker that chars existed
        }
      }
      sort= (sort << 8) + (wild_pos ? min(wild_pos, 127) : chars);
    }
  }
  va_end(args);
  return sort;
}

uint64_t get_sort(const ObString &str)
{
  const char wild_many = '%';
  const char wild_one = '_';
  const char wild_prefix = '\\';
  uint64_t sort = 0;
  if (str.length() > 0) {
    const char *str_ptr = str.ptr();
    ObString::obstr_size_t str_pos = 0;
    const ObString::obstr_size_t str_length = str.length();
    uint32_t specific_chars = 128;
    uint32_t wild_pos = 0; // first wildcard position

    for (; str_pos < str_length; str_ptr++, str_pos++) {
      if (str_pos + 1 < str_length && *str_ptr == wild_prefix) {
        str_ptr++;
        str_pos++;
      } else if (str_pos < str_length && (*str_ptr == wild_many || *str_ptr == wild_one)) {
        wild_pos = str_pos + 1;
        break;
      }
    }
    sort = wild_pos ? min(wild_pos, 127): specific_chars;
  }
  return sort;
}

bool prefix_match(const char *prefix, const char *str)
{
  bool b_ret = false;
  if (NULL == prefix || NULL == str) {

  } else {
    size_t prefix_len = strlen(prefix);
    size_t str_len = strlen(str);
    if (prefix_len > str_len) {

    } else if (0 == MEMCMP(prefix, str, prefix_len)) {
      b_ret = true;
    }
  }
  return b_ret;
}

int str_cmp(const void *v1, const void *v2)
{
  int ret = 0;
  if (NULL == v1) {
    ret = -1;
  } else if (NULL == v2) {
    ret = 1;
  } else {
    ret = strcmp((const char*)v1, (const char*)v2);
  }
  return ret;
}

const char* get_default_if()
{
  static char ifname[128] = {};
  int found = 0;
  FILE *file = NULL;
  file = fopen("/proc/net/route", "r");
  if (file) {
    char dest[16] = {};
    char gw[16] = {};
    char remain[1024] = {};
    if (1 == fscanf(file, "%1024[^\n]\n", remain)) {
      while (1) {
        int r = fscanf(file,
                       "%127s\t%15s\t%15s\t%1023[^\n]\n",
                       ifname, dest, gw, remain);
        if (r < 4) {
          break;
        }
        if (MEMCMP(gw, "00000000", 8) != 0) {
          found = 1;
          break;
        }
      }
    }
    fclose(file);
  }
  if (!found) {
    ifname[0] = '\0';
  }
  return ifname;
}

int sql_append_hex_escape_str(const ObString &str, ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  const int64_t need_len = sql.length() + str.length() * 2 + 3; // X''

  if (OB_FAIL(sql.reserve(need_len))) {
    LOG_WDIAG("reserve sql failed, ", K(ret));
  } else if (OB_FAIL(sql.append("X'"))) {
    LOG_WDIAG("append string failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < str.length(); ++i) {
      if (OB_FAIL(sql.append_fmt("%02X", static_cast<uint8_t>(str.ptr()[i])))) {
        LOG_WDIAG("append string failed", K(ret), K(i));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql.append("'"))) {
      LOG_WDIAG("append string failed", K(ret));
    }
  }
  return ret;
}

static int pidfile_test(const char *pidfile)
{
  int ret = OB_SUCCESS;
  int fd = open(pidfile, O_RDONLY);

  if (fd < 0) {
    LOG_EDIAG("fid file doesn't exist", K(pidfile));
    ret = OB_FILE_NOT_EXIST;
  } else {
    if (lockf(fd, F_TEST, 0) != 0) {
      ret = OB_ERROR;
    }
    close(fd);
  }

  return ret;
}

static int read_pid(const char *pidfile, long &pid)
{
  int ret = OB_SUCCESS;
  char buf[32] = {};
  int fd = open(pidfile, O_RDONLY);

  if (fd < 0) {
    LOG_EDIAG("can't open pid file", K(pidfile), K(errno));
    ret = OB_FILE_NOT_EXIST;
  } else if (read(fd, buf, sizeof(buf)) <= 0) {
    LOG_EDIAG("fail to read pid from file", K(pidfile), K(errno));
    ret = OB_IO_ERROR;
  } else {
    pid = strtol(buf, NULL, 10);
  }

  if (fd >= 0) {
    close(fd);
  }

  return ret;
}

static int use_daemon()
{
  int ret = OB_SUCCESS;
  const int nochdir = 1;
  const int noclose = 0;
  if (daemon(nochdir, noclose) < 0) {
    LOG_EDIAG("create daemon process fail", K(errno));
    ret = OB_ERR_SYS;
  }
  reset_tid_cache();
  // bt("enable_preload_bt") = 1;
  return ret;
}

int start_daemon(const char *pidfile)
{
  int ret = OB_SUCCESS;

  ret = pidfile_test(pidfile);

  if (ret != OB_SUCCESS && ret != OB_FILE_NOT_EXIST) {
    LOG_EDIAG("pid already exists");
  } else {
    ret = OB_SUCCESS;
  }

  // start daemon
  if (OB_SUCC(ret) && OB_FAIL(use_daemon())) {
    LOG_EDIAG("create daemon process fail", K(ret));
  }

  if (OB_SUCC(ret)) {
    int fd = open(pidfile, O_RDWR|O_CREAT, 0600);

    if (fd < 0) {  // open pidfile fail
      LOG_EDIAG("can't open pid file", K(pidfile), K(fd), K(errno));
      ret = OB_IO_ERROR;
    } else if (lockf(fd, F_TLOCK, 0) < 0) {  // other process has locked it
      long pid = 0;
      if (OB_FAIL(read_pid(pidfile, pid))) {
        LOG_EDIAG("read pid fail", K(pidfile), K(ret));
      } else {
        LOG_EDIAG("process is running", K(pid));
      }
      close(fd);
    } else {  // I hold the lock, won't close this fd.
      if (ftruncate(fd, 0) < 0) {
        LOG_EDIAG("ftruncate pid file fail", K(pidfile), K(errno));
        ret = OB_IO_ERROR;
      } else {
        char buf[32] = {};
        IGNORE_RETURN snprintf(buf, sizeof(buf), "%d", getpid());
        ssize_t len = strlen(buf);
        ssize_t nwrite = write(fd, buf, len);
        if (len != nwrite) {
          LOG_EDIAG("write pid file fail", K(pidfile), K(errno));
          ret = OB_IO_ERROR;
        }
      }
    }
  }

  return ret;
}

int ob_alloc_printf(ObString &result, ObIAllocator &alloc, const char* fmt, va_list ap)
{
  int ret = OB_SUCCESS;
  va_list ap2;
  va_copy(ap2, ap);
  int64_t n = vsnprintf(NULL, 0, fmt, ap);
  if (n < 0) {
    LOG_EDIAG("vsnprintf failed", K(n), K(errno));
    ret = OB_ERR_SYS;
  } else {
    char* buf = static_cast<char*>(alloc.alloc(n+1));
    if (NULL == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("no memory");
    } else {
      int64_t n2 = vsnprintf(buf, n+1, fmt, ap2);
      if (n2 < 0) {
        LOG_EDIAG("vsnprintf failed", K(n), K(errno));
        ret = OB_ERR_SYS;
      } else if (n != n2) {
        LOG_EDIAG("vsnprintf failed", K(n), K(n2));
        ret = OB_ERR_SYS;
      } else {
        result.assign_ptr(buf, static_cast<int32_t>(n));
      }
    }
  }
  return ret;
}

int ob_alloc_printf(ObString &result, ObIAllocator &alloc, const char* fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  int ret = ob_alloc_printf(result, alloc, fmt, ap);
  va_end(ap);
  return ret;
}

int64_t ObTimeGuard::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int64_t i = 0;

  while ((i + 8) <= click_count_) {
    databuff_printf(buf, buf_len, pos, "%d,%d,%d,%d,%d,%d,%d,%d,",
                    click_[i], click_[i+1], click_[i+2], click_[i+3], click_[i+4],
                    click_[i+5], click_[i+6], click_[i+7]);
    i = i + 8;
  }
  switch (click_count_ - i) {
    case 0: {
      break;
    }
    case 1: {
      databuff_printf(buf, buf_len, pos, "%d,", click_[i]);
      break;
    }
    case 2: {
      databuff_printf(buf, buf_len, pos, "%d,%d,", click_[i], click_[i+1]);
      break;
    }
    case 3: {
      databuff_printf(buf, buf_len, pos, "%d,%d,%d,", click_[i], click_[i+1], click_[i+2]);
      break;
    }
    case 4: {
      databuff_printf(buf, buf_len, pos, "%d,%d,%d,%d,",
                      click_[i], click_[i+1], click_[i+2], click_[i+3]);
      break;
    }
    case 5: {
      databuff_printf(buf, buf_len, pos, "%d,%d,%d,%d,%d,",
                      click_[i], click_[i+1], click_[i+2], click_[i+3], click_[i+4]);
      break;
    }
    case 6: {
      databuff_printf(buf, buf_len, pos, "%d,%d,%d,%d,%d,%d,",
                      click_[i], click_[i+1], click_[i+2], click_[i+3], click_[i+4], click_[i+5]);
      break;
    }
    case 7: {
      databuff_printf(buf, buf_len, pos, "%d,%d,%d,%d,%d,%d,%d,",
                      click_[i], click_[i+1], click_[i+2], click_[i+3], click_[i+4], click_[i+5], click_[i+6]);
      break;
    }
    default: {
      // do nothing
    }
  }

  databuff_printf(buf, buf_len, pos, "used=%ld owner=%s", common::ObTimeUtility::current_time() - start_ts_, owner_);
  return pos;
}

////////////////////////////////////////////////////////////////////
// BandwidthThrottle

ObBandwidthThrottle::ObBandwidthThrottle()
    : start_time_(0), rate_(0), threshold_(0),
      lamt_(0), buf_len_(0), inited_(false)
{
}


ObBandwidthThrottle::~ObBandwidthThrottle()
{
}

int ObBandwidthThrottle::init(const int64_t rate, const int64_t buflen)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WDIAG, "init throttle twice.", K(ret));
  } else if (rate <= 0 || buflen <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "invalid arguments.", K(ret), K(rate), K(buflen));
  } else {
    rate_ = rate;
    buf_len_ = buflen;
    start_time_ = 0;
    inited_ = true;
  }
  return ret;
}

void ObBandwidthThrottle::destroy()
{
  inited_ = false;
}
int ObBandwidthThrottle::start_task()
{
  int ret = OB_SUCCESS;
  int64_t current_time = ObTimeUtility::current_time();
  if (!inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WDIAG, "throttle is not initialized.", K(ret));
  } else {
    ObSpinLockGuard guard(lock_);
    if (0 == start_time_) {
      start_time_ = current_time;
    } else {
      int64_t elapse_time = current_time - start_time_;
      if (0 == elapse_time) elapse_time = 1;     // avoid divide 0 error.
      int64_t current_speed = lamt_ * 1000 * 1000 / elapse_time;
      if (current_speed < buf_len_ / 4) {
        // restart stat.
        start_time_ = current_time;
        lamt_ = 0;
      }
    }
  }
  return ret;
}

int ObBandwidthThrottle::limit(const int64_t bytes, int64_t &sleep_time)
{
  int ret = OB_SUCCESS;
  int64_t current_time = ObTimeUtility::current_time();
  sleep_time = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WDIAG, "throttle is not initialized.", K(ret));
  } else if (bytes <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "invalid input bytes.", K(ret), K(bytes));
  } else {
    ObSpinLockGuard guard(lock_);
    lamt_ += bytes;
    int64_t elapse_time = current_time - start_time_;
    int64_t max_band_time = lamt_ * 1000 * 1000 / rate_;
    if (elapse_time < max_band_time) {
      sleep_time = max_band_time - elapse_time;
    }
  }
  return ret;
}


//ref: https://www.cnblogs.com/westfly/p/5139645.html
struct tm *ob_localtime(const time_t *unix_sec, struct tm *result)
{
  static const int HOURS_IN_DAY = 24;
  static const int MINUTES_IN_HOUR = 60;
  static const int DAYS_FROM_UNIX_TIME = 2472632;
  static const int DAYS_FROM_YEAR = 153;
  static const int MAGIC_UNKONWN_FIRST = 146097;
  static const int MAGIC_UNKONWN_SEC = 1461;
  // use __timezone from glibc/time/tzset.c, default value is -480 for china
  const int32_t timezone = static_cast<int32_t>(__daylight ? __timezone - 3600 : __timezone);
  const int32_t tz_minutes = static_cast<int32_t>(timezone / 60);

//only support time > 1970/1/1 8:0:0
  if (OB_LIKELY(NULL != result) && OB_LIKELY(NULL != unix_sec) && OB_LIKELY(*unix_sec > 0)) {
    result->tm_sec  = static_cast<int>((*unix_sec) % MINUTES_IN_HOUR);
    int tmp_i       = static_cast<int>((*unix_sec) / MINUTES_IN_HOUR) - tz_minutes;
    result->tm_min  = tmp_i % MINUTES_IN_HOUR;
    tmp_i          /= MINUTES_IN_HOUR;
    result->tm_hour = tmp_i % HOURS_IN_DAY;
    result->tm_mday = tmp_i / HOURS_IN_DAY;
    int tmp_a       = result->tm_mday + DAYS_FROM_UNIX_TIME;
    int tmp_b       = (tmp_a * 4 + 3) / MAGIC_UNKONWN_FIRST;
    int tmp_c       = tmp_a - (tmp_b * MAGIC_UNKONWN_FIRST) / 4;
    int tmp_d       = ((tmp_c * 4 + 3) / MAGIC_UNKONWN_SEC);
    int tmp_e       = tmp_c - (tmp_d * MAGIC_UNKONWN_SEC) / 4;
    int tmp_m       = (5 * tmp_e + 2) / DAYS_FROM_YEAR;
    result->tm_mday = tmp_e + 1 - (DAYS_FROM_YEAR * tmp_m + 2) / 5;
    result->tm_mon  = tmp_m + 2 - (tmp_m / 10) * 12;
    result->tm_year = tmp_b * 100 + tmp_d - 6700 + (tmp_m / 10);
  }
  return result;
}

void ob_fast_localtime(time_t &cached_unix_sec, struct tm &cached_localtime,
    const time_t &unix_sec, struct tm *result)
{
  if (OB_LIKELY(NULL != result)) {
    if (cached_unix_sec == unix_sec) {
      *result = cached_localtime;
    } else {
      cached_unix_sec = unix_sec;
      cached_localtime = *ob_localtime(&unix_sec, result);
    }
  }
}


////////////////////////////////////////////////////////////////////////////////////////////////////

} // end namespace common
} // end namespace oceanbase
