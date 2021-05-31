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

#define USING_LOG_PREFIX RPC_OBMYSQL

#include "rpc/obmysql/ob_mysql_util.h"

#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/charset/ob_dtoa.h"
#include "lib/ob_proxy_worker.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obmysql
{
const uint64_t ObMySQLUtil::NULL_ = UINT64_MAX;
// @todo
//TODO avoid coredump if field_index is too large
//http://dev.mysql.com/doc/internals/en/prepared-statements.html#null-bitmap
//offset is 2
void ObMySQLUtil::update_null_bitmap(char *&bitmap, int64_t field_index)
{
  int byte_pos = static_cast<int>((field_index + 2) / 8);
  int bit_pos  = static_cast<int>((field_index + 2) % 8);
  int8_t result = static_cast<int8_t>(bitmap[byte_pos]);
  result |= static_cast<int8_t>(1 << bit_pos);
  bitmap[byte_pos] = static_cast<char>(result);
  //bitmap[byte_pos] |= static_cast<char>(1 << bit_pos);
}

//called by handle OB_MYSQL_COM_STMT_EXECUTE offset is 0
int ObMySQLUtil::store_length(char *buf, int64_t len, uint64_t length, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (len < 0 || pos < 0 || len <= pos) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("Store length fail, buffer over flow!",
        K(len), K(pos), K(length), K(ret));
  } else if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input buf", K(ret), K(buf));
  } else {
    int64_t remain = len - pos;
    if (OB_SUCC(ret)) {
      if (length < (uint64_t) 251 && remain >= 1) {
        ret = store_int1(buf, len, (uint8_t) length, pos);
      }
      /* 251 is reserved for NULL */
      else if (length < (uint64_t) 0X10000 && remain >= 3) {
        ret = store_int1(buf, len, static_cast<int8_t>(252), pos);
        if (OB_SUCC(ret)) {
          ret = store_int2(buf, len, (uint16_t) length, pos);
          if (OB_FAIL(ret)) {
            pos--;
          }
        }
      } else if (length < (uint64_t) 0X1000000 && remain >= 4) {
        ret = store_int1(buf, len, (uint8_t) 253, pos);
        if (OB_SUCC(ret)) {
          ret = store_int3(buf, len, (uint32_t) length, pos);
          if (OB_FAIL(ret)) {
            pos--;
          }
        }
      } else if (length < UINT64_MAX && remain >= 9) {
        ret = store_int1(buf, len, (uint8_t) 254, pos);
        if (OB_SUCC(ret)) {
          ret = store_int8(buf, len, (uint64_t) length, pos);
          if (OB_FAIL(ret)) {
            pos--;
          }
        }
      } else if (length == UINT64_MAX) { /* NULL_ == UINT64_MAX */
        ret = store_null(buf, len, pos);
      } else {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("Store length fail, buffer over flow!",
            K(len), K(pos), K(length), K(ret));
      }
    }
  }
  return ret;
}

int ObMySQLUtil::get_length(const char *&pos, uint64_t &length)
{
  uint8_t sentinel = 0;
  uint16_t s2 = 0;
  uint32_t s4 = 0;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input buf", K(pos), K(ret));
  } else {
    get_uint1(pos, sentinel);
    if (sentinel < 251) {
      length = sentinel;
    } else if (sentinel == 251) {
      length = NULL_;
    } else if (sentinel == 252) {
      get_uint2(pos, s2);
      length = s2;
    } else if (sentinel == 253) {
      get_uint3(pos, s4);
      length = s4;
    } else if (sentinel == 254) {
      get_uint8(pos, length);
    } else {
      // 255??? won't get here.
      pos--;                  // roll back
      ret = OB_INVALID_DATA;
    }
  }
  return ret;
}

int ObMySQLUtil::get_length(const char *&pos, uint64_t &length, uint64_t &pos_inc_len)
{
  int ret = OB_SUCCESS;
  const char *tmp_pos = pos;
  if (OB_FAIL(get_length(pos, length))) {
    LOG_WARN("fail to get length", K(ret));
  } else {
    pos_inc_len = pos - tmp_pos;
  }
  return ret;
}

uint64_t ObMySQLUtil::get_number_store_len(const uint64_t num)
{
  uint64_t len = 0;
  if (num < 251) {
    len = 1;
  } else if (num < (1 << 16)) {
    len = 3;
  } else if (num < (1 << 24)) {
    len = 4;
  } else if (num < UINT64_MAX) {
    len = 9;
  } else if (num == UINT64_MAX) {
    // NULL_ == UINT64_MAX
    //it is represents a NULL in a ProtocolText::ResultsetRow.
    len = 1;
  }
  return len;
}

int ObMySQLUtil::store_str(char *buf, int64_t len, const char *str, int64_t &pos)
{
  uint64_t length = strlen(str);
  return store_str_v(buf, len, str, length, pos);
}

int ObMySQLUtil::store_str_v(char *buf, int64_t len, const char *str,
                             const uint64_t length, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t pos_bk = pos;

  if (OB_ISNULL(buf)) { // str could be null
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input args", K(buf), K(ret));
  } else {
    if (OB_FAIL(store_length(buf, len, length, pos))) {
      LOG_WARN("Store length fail!",
          K(len), K(pos), K(length), K(ret));
    } else if (len >= pos && length <= static_cast<uint64_t>(len - pos)) {
      if ((0 == length ) || (length > 0 && NULL != str)) {
        MEMCPY(buf + pos, str, length);
        pos += length;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid args", K(str), K(length));
      }
    } else {
      pos = pos_bk;        // roll back
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("Store string fail, buffer over flow!", K(len), K(pos), K(length), K(ret));
    }
  }
  return ret;
}

int ObMySQLUtil::store_obstr(char *buf, int64_t len, ObString str, int64_t &pos)
{
  return store_str_v(buf, len, str.ptr(), str.length(), pos);
}

int ObMySQLUtil::store_str_zt(char *buf, int64_t len, const char *str, int64_t &pos)
{
  uint64_t length = strlen(str);
  return store_str_vzt(buf, len, str, length, pos);
}

int ObMySQLUtil::store_str_nzt(char *buf, int64_t len, const char *str, int64_t &pos)
{
  return store_str_vnzt(buf, len, str, strlen(str), pos);
}

int ObMySQLUtil::store_str_vnzt(char *buf, int64_t len, const char *str, int64_t length, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input args", K(buf));
  } else {
    if (length >= 0) {
      if (len > 0 && pos >= 0 && len >= pos && len - pos >= length) {
        if ((0 == length) || (length > 0 && NULL != str)) {
          MEMCPY(buf + pos, str, length);
          pos += length;
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid args", K(str), K(length));
        }
      } else {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("Store string fail, buffer over flow!", K(len), K(pos), K(length), K(ret));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid input length", K(ret), K(length));
    }
  }
  return ret;
}


int ObMySQLUtil::store_str_vzt(char *buf, int64_t len, const char *str,
                               const uint64_t length, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf))  {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input args", K(ret), K(buf));
  } else {
    if (len > 0 && pos >= 0 && len > pos && static_cast<uint64_t>(len - pos) > length) {
      if ((0 == length) || (length > 0 && NULL != str)) {
        MEMCPY(buf + pos, str, length);
        pos += length;
        buf[pos++] = '\0';
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid args", K(str), K(length));
      }
    } else {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("Store string fail, buffer over flow!", K(len), K(pos), K(length), K(ret));
    }
  }
  return ret;
}

int ObMySQLUtil::store_obstr_zt(char *buf, int64_t len, ObString str, int64_t &pos)
{
  return store_str_vzt(buf, len, str.ptr(), str.length(), pos);
}

int ObMySQLUtil::store_obstr_nzt(char *buf, int64_t len, ObString str, int64_t &pos)
{
  return store_str_vnzt(buf, len, str.ptr(), str.length(), pos);
}


void ObMySQLUtil::prepend_zeros(char *buf, int64_t org_char_size, int64_t offset) {
  // memmove(buf + offset, buf, org_char_size);
  if (OB_ISNULL(buf)) {
    LOG_WARN("invalid buf input", K(buf));
  } else {
    char *src_last = buf + org_char_size;
    char *dst_last = src_last + offset;
    while (org_char_size-- > 0) {
      *--dst_last = *--src_last;
    }
    while (offset-- > 0) {
      buf[offset] = '0';
    }
  }
}


int ObMySQLUtil::int_cell_str(
    char *buf, const int64_t len, int64_t val, bool is_unsigned,
    MYSQL_PROTOCOL_TYPE type, int64_t &pos,
    bool zerofill, int32_t zflength)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input buf", K(buf));
  } else {
    uint64_t length = 0;
    // OB_MYSQL_TYPE_LONGLONG format
    if (OB_UNLIKELY(zerofill && (pos + zflength + 1 > len))) {
      ret = OB_SIZE_OVERFLOW;
    } else if (OB_UNLIKELY(len - pos < 29)) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      if (OB_SUCC(ret)) {
        if (TEXT == type) {
          const char *fmt = is_unsigned ? "%lu" : "%ld";
          const int64_t MAX_INTEGER_LEN = 30;
          char tmp_buff[MAX_INTEGER_LEN];
          //you know, if val = 1234 then len_raw will be 4
          int64_t len_raw = snprintf(tmp_buff, MAX_INTEGER_LEN, fmt, val);
          length = len_raw;
          int64_t zero_cnt = 0;
          if (zerofill && (zero_cnt = zflength - length) > 0) {
            length = zflength;
          }
          /* skip bytes_to_store_len bytes to store length */
          int64_t bytes_to_store_len = get_number_store_len(length);
          MEMCPY(buf + pos + bytes_to_store_len, tmp_buff, len_raw);
          /*zero_cnt > 0 indicates that zerofill is true */
          if (zero_cnt > 0) {
            ObMySQLUtil::prepend_zeros(buf + pos + bytes_to_store_len, length, zero_cnt);
          }
          ret = ObMySQLUtil::store_length(buf, pos + bytes_to_store_len, length, pos);
          pos += length;
        } else if (BINARY == type) {
          ret = ObMySQLUtil::store_int8(buf, len, val, pos);
        }
      } else {
        LOG_ERROR("size over flow", K(ret), K(length), K(len), K(pos), K(zflength));
      }
    }
  }
  return ret;
}

int ObMySQLUtil::null_cell_str(char *buf, const int64_t len, MYSQL_PROTOCOL_TYPE type,
                               int64_t &pos, int64_t cell_index, char *bitmap)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input buf", K(ret), K(buf));
  } else {
    if (len - pos <= 0) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      if (BINARY == type) {
        ObMySQLUtil::update_null_bitmap(bitmap, cell_index);
      } else {
        ret = ObMySQLUtil::store_null(buf, len, pos);
      }
    }
  }
  return ret;
}

int ObMySQLUtil::number_cell_str(
    char *buf, const int64_t len, const number::ObNumber &val, int64_t &pos, int16_t scale,
    bool zerofill, int32_t zflength)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid buf input", K(buf), K(ret));
  } else {
    /* skip 1 byte to store length */
    int64_t length = 0;
    if (OB_UNLIKELY(zerofill && (pos + zflength + 1 > len))) {
      ret = OB_SIZE_OVERFLOW;
    } else if (OB_FAIL(val.format(buf + pos + 1, len - pos - 1, length, scale))) {

    } else {
      int64_t zero_cnt = 0;
      if (zerofill && (zero_cnt = zflength - length) > 0) {
        ObMySQLUtil::prepend_zeros(buf + pos + 1, length, zero_cnt);
        length = zflength;
      }

      ret = ObMySQLUtil::store_length(buf, len, length, pos);
      pos += length;
    }
  }
  return ret;
}

int ObMySQLUtil::datetime_cell_str(
    char *buf, const int64_t len,
    int64_t val, MYSQL_PROTOCOL_TYPE type, int64_t &pos,
    const ObTimeZoneInfo *tz_info, int16_t scale)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input args", K(ret), K(buf));
  } else {
    if (type == BINARY) {
      ObTime ob_time;
      uint8_t timelen = 0;
      if (OB_FAIL(ObTimeConverter::datetime_to_ob_time(val, tz_info, ob_time))) {
        LOG_WARN("convert usec ", K(ret));
      } else {
        timelen = 11;
        if (OB_FAIL(ObMySQLUtil::store_int1(buf, len, timelen, pos))) {
          LOG_WARN("failed to store int", K(buf), K(len), K(timelen), K(pos), K(ret));
        } else if (OB_FAIL(
            ObMySQLUtil::store_int2(buf, len, static_cast<int16_t>(ob_time.parts_[DT_YEAR]), pos))) {
          LOG_WARN("failed to store int", K(buf), K(len), K(timelen), K(pos), K(ret));
        } else if (OB_FAIL(
            ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(ob_time.parts_[DT_MON]), pos))) {
          LOG_WARN("failed to store int", K(buf), K(len), K(timelen), K(pos), K(ret));
        } else if (OB_FAIL(
            ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(ob_time.parts_[DT_MDAY]), pos))) {
          LOG_WARN("failed to store int", K(buf), K(len), K(timelen), K(pos), K(ret));
        } else if (OB_FAIL(
            ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(ob_time.parts_[DT_HOUR]), pos))) {
          LOG_WARN("failed to store int", K(buf), K(len), K(timelen), K(pos), K(ret));
        } else if (OB_FAIL(
            ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(ob_time.parts_[DT_MIN]), pos))) {
          LOG_WARN("failed to store int", K(buf), K(len), K(timelen), K(pos), K(ret));
        } else if (OB_FAIL(
            ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(ob_time.parts_[DT_SEC]), pos))) {
          LOG_WARN("failed to store int", K(buf), K(len), K(timelen), K(pos), K(ret));
        } else if (OB_FAIL(
            ObMySQLUtil::store_int4(buf, len, static_cast<int32_t>(ob_time.parts_[DT_USEC]), pos))) {
          LOG_WARN("failed to store int", K(buf), K(len), K(timelen), K(pos), K(ret));
        }
      }
    } else {
      /* skip 1 byte to store length */
      int64_t pos_begin = pos++;
      if (len - pos <= 1) {
        ret = OB_SIZE_OVERFLOW;
      } else {
        if (OB_FAIL(ObTimeConverter::datetime_to_str(val, tz_info, scale, buf, len, pos))) {
          LOG_WARN("failed to convert ob time to string", K(ret));
          pos = pos_begin + 1;
        } else {
          // store length as beginning
          int64_t timelen = pos - pos_begin - 1;
          ret = ObMySQLUtil::store_length(buf, len, timelen, pos_begin);
        }
      }
    }
  }
  return ret;
}

int ObMySQLUtil::date_cell_str(
    char *buf, const int64_t len,
    int32_t val, MYSQL_PROTOCOL_TYPE type, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input args", K(ret), K(buf));
  } else {
    if (type == BINARY) {
      ObTime ob_time;
      uint8_t timelen = 0;
      if (OB_FAIL(ObTimeConverter::date_to_ob_time(val, ob_time))) {
        LOG_WARN("convert day to date failed", K(ret));
      } else if (0 == ob_time.parts_[DT_YEAR] && 0 == ob_time.parts_[DT_MON] && 0 == ob_time.parts_[DT_MDAY]) {
        timelen = 0;
        ret = ObMySQLUtil::store_int1(buf, len, timelen, pos);
      } else {
        timelen = 4;
        if (OB_FAIL(ObMySQLUtil::store_int1(buf, len, timelen, pos))) {
          LOG_WARN("failed to store int", K(buf), K(len), K(timelen), K(pos), K(ret));
        } else if (OB_FAIL(
            ObMySQLUtil::store_int2(buf, len, static_cast<int16_t>(ob_time.parts_[DT_YEAR]), pos))) {
          LOG_WARN("failed to store int", K(buf), K(len), K(timelen), K(pos), K(ret));
        } else if (OB_FAIL(
            ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(ob_time.parts_[DT_MON]), pos))) {
          LOG_WARN("failed to store int", K(buf), K(len), K(timelen), K(pos), K(ret));
        } else if (OB_FAIL(
            ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(ob_time.parts_[DT_MDAY]), pos))) {
          LOG_WARN("failed to store int", K(buf), K(len), K(timelen), K(pos), K(ret));
        }
      }
    } else {
      /* skip 1 byte to store length */
      int64_t pos_begin = pos++;
      if (len - pos <= 1) {
        ret = OB_SIZE_OVERFLOW;
      } else {
        if (OB_FAIL(ObTimeConverter::date_to_str(val, buf, len, pos))) {
          LOG_WARN("failed to convert ob time to string", K(ret));
          pos = pos_begin + 1;
        } else {
          // store length as beginning
          int64_t timelen = pos - pos_begin - 1;
          ret = ObMySQLUtil::store_length(buf, len, timelen, pos_begin);
        }
      }
    }
  }
  return ret;
}

int ObMySQLUtil::time_cell_str(
    char *buf, const int64_t len,
    int64_t val, MYSQL_PROTOCOL_TYPE type, int64_t &pos, int16_t scale)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input args", K(ret), K(buf));
  } else {
    if (type == BINARY) {
      ObTime ob_time;
      uint8_t timelen = 0;
      if (OB_FAIL(ObTimeConverter::time_to_ob_time(val, ob_time))) {
        LOG_WARN("convert usec to timestamp failed", K(ret));
      } else {
        timelen = 8;
        if (OB_FAIL(ObMySQLUtil::store_int1(buf, len, timelen, pos))) {
        } else if (
            OB_FAIL(ObMySQLUtil::store_int2(buf, len, static_cast<int16_t>(ob_time.parts_[DT_HOUR]), pos))) {
        } else if (
            OB_FAIL(ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(ob_time.parts_[DT_MIN]), pos))) {
        } else if (
            OB_FAIL(ObMySQLUtil::store_int1(buf, len, static_cast<int8_t>(ob_time.parts_[DT_SEC]), pos))) {
        } else if (
            OB_FAIL(ObMySQLUtil::store_int4(buf, len, static_cast<int32_t>(ob_time.parts_[DT_USEC]), pos))) {
        }
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to store int", K(buf), K(len), K(timelen), K(pos), K(ret));
        }
      }
    } else {
      /* skip 1 byte to store length */
      int64_t pos_begin = pos++;
      if (len - pos <= 1) {
        ret = OB_SIZE_OVERFLOW;
      } else {
        if (OB_FAIL(ObTimeConverter::time_to_str(val, scale, buf, len, pos))) {
          LOG_WARN("failed to convert ob time to string", K(ret));
          pos = pos_begin + 1;
        } else {
          // store length as beginning
          int64_t timelen = pos - pos_begin - 1;
          ret = ObMySQLUtil::store_length(buf, len, timelen, pos_begin);
        }
      }
    }
  }
  return ret;
}

int ObMySQLUtil::year_cell_str(
    char *buf, const int64_t len,
    uint8_t val, MYSQL_PROTOCOL_TYPE type, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input args", K(ret), K(buf));
  } else {
    if (type == BINARY) {
      int64_t year = 0;
      uint8_t yearlen = 0;
      if (OB_FAIL(ObTimeConverter::year_to_int(val, year))) {
        LOG_WARN("failed to convert year to integer", K(ret));
      } else {
        yearlen = 2;
        if (OB_FAIL(ObMySQLUtil::store_int1(buf, len, yearlen, pos))) {
          LOG_WARN("failed to store int", K(buf), K(len), K(pos), K(ret));
        } else if (OB_FAIL(ObMySQLUtil::store_int2(buf, len, static_cast<int16_t>(year), pos))) {
          LOG_WARN("failed to store int", K(buf), K(len), K(pos), K(ret));
        }
      }
    } else {
      /* skip 1 byte to store length */
      int64_t pos_begin = pos++;
      if (len - pos <= 1) {
        ret = OB_SIZE_OVERFLOW;
      } else {
        if (OB_FAIL(ObTimeConverter::year_to_str(val, buf, len, pos))) {
          LOG_WARN("failed to convert year to string", K(ret));
          pos = pos_begin + 1;
        } else {
          // store length as beginning
          uint64_t yearlen = pos - pos_begin - 1;
          ret = ObMySQLUtil::store_length(buf, len, yearlen, pos_begin);
        }
      }
    }
  }
  return ret;
}

int ObMySQLUtil::varchar_cell_str(char *buf, const int64_t len, const ObString &val, int64_t &pos)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input args", K(ret), K(buf));
  } else {
    int64_t length = val.length();

    if (length < len - pos) {
      int64_t pos_bk = pos;
      if (OB_FAIL(ObMySQLUtil::store_length(buf, len, length, pos))) {
        LOG_WARN("store length fail", K(ret));
      } else {
        if (length <= len - pos) {
          MEMCPY(buf + pos, val.ptr(), length);
          pos += length;
        } else {
          LOG_WARN("store string fail", K(ret));
          pos = pos_bk;
          ret = OB_SIZE_OVERFLOW;
        }
      }
    } else {
      LOG_WARN("store varchar cell fail", K(length), K(len), K(pos));
      ret = OB_SIZE_OVERFLOW;
    }
  }
  return ret;
}

int ObMySQLUtil::float_cell_str(char *buf, const int64_t len, float val,
                                MYSQL_PROTOCOL_TYPE type, int64_t &pos, int16_t scale,
                                bool zerofill, int32_t zflength)
{
  static const int FLT_LEN =  FLOAT_TO_STRING_CONVERSION_BUFFER_SIZE;
  static const int FLT_SIZE = sizeof (float);

  LOG_DEBUG("float_cell_str", K(val));

  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input args", K(ret), K(buf));
  } else {
    if (BINARY == type) {
      if (len - pos > FLT_SIZE) {
        MEMCPY(buf + pos, &val, FLT_SIZE);
        pos += FLT_SIZE;
      } else {
        ret = OB_SIZE_OVERFLOW;
      }
    } else {
      if (OB_UNLIKELY(zerofill && (pos + zflength + 1 > len))) {
        ret = OB_SIZE_OVERFLOW;
      } else if (OB_UNLIKELY(len - pos <= FLT_LEN)) {
        ret = OB_SIZE_OVERFLOW;
      } else if (len - pos > FLT_LEN) {
        int64_t length;
        if (0 <= scale) {
          length = ob_fcvt(val, scale, FLT_LEN - 1, buf + pos + 1, NULL);
        } else {
          length = ob_gcvt_opt(val, OB_GCVT_ARG_FLOAT, FLT_LEN - 1, buf + pos + 1, NULL, lib::is_oracle_mode());
        }
        if (length < 251) {
          int64_t zero_cnt = 0;
          if (zerofill && (zero_cnt = zflength - length) > 0) {
            ObMySQLUtil::prepend_zeros(buf + pos + 1, length, zero_cnt);
            length = zflength;
          }
          ret = ObMySQLUtil::store_length(buf, len, length, pos);
          pos += length;
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_ERROR("invalid length", K(length)); //OB_ASSERT(length < 251);
        }
      }
    }
  }

  return ret;
}

int ObMySQLUtil::double_cell_str(char *buf, const int64_t len, double val,
                                 MYSQL_PROTOCOL_TYPE type, int64_t &pos, int16_t scale,
                                 bool zerofill, int32_t zflength)
{
  static const int DBL_LEN =  DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE;
  static const int DBL_SIZE = sizeof (double);

  LOG_DEBUG("double_cell_str", K(val));
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input", K(buf));
  } else {
    if (BINARY == type) {
      if (len - pos > DBL_SIZE) {
        MEMCPY(buf + pos, &val, DBL_SIZE);
        pos += DBL_SIZE;
      } else {
        ret = OB_SIZE_OVERFLOW;
      }
    } else {
      // max size: DOUBLE_MAX + 3 bytes to store its length
      if (OB_UNLIKELY(zerofill && (pos + zflength + 3 > len))) {
        ret = OB_SIZE_OVERFLOW;
      } else if (OB_UNLIKELY(len - pos < DBL_LEN + 3)) {
        ret = OB_SIZE_OVERFLOW;
      } else {
        // we skip 1 bytes to store length for most cases
        int64_t length;
        if (0 <= scale) {
          length = ob_fcvt(val, scale, DBL_LEN - 1, buf + pos + 1, NULL);
        } else {
          length = ob_gcvt_opt(val, OB_GCVT_ARG_DOUBLE, DBL_LEN - 1, buf + pos + 1, NULL, lib::is_oracle_mode());
        }
        if (length <= DBL_LEN) { //OB_ASSERT(length <= DBL_LEN);
          int64_t zero_cnt = 0;
          if (zerofill && (zero_cnt = zflength - length) > 0) {
            ObMySQLUtil::prepend_zeros(buf + pos + 1, length, zero_cnt);
            length = zflength;
          }

          // According to the agreement: If the length of the double is less than 251 bytes,
          // the length header only needs 1 byte, otherwise the length header needs 3 bytes
          if (length < 251) {
            ret = ObMySQLUtil::store_length(buf, len, length, pos);
            pos += length;
          } else if (length >= 251) {
            // we need 3 btyes to hold length of double (maybe)
            memmove(buf + pos + 3, buf + pos + 1, length);
            ret = ObMySQLUtil::store_length(buf, len, length, pos);
            pos += length;
          }
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_ERROR("invalid length", K(length));
        }
      }
    }
  }
  return ret;
}

} // namespace obmysql
} // namespace oceanbase
