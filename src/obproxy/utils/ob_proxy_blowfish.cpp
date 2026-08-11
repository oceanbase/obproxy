/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * *************************************************************
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX PROXY
#include <openssl/blowfish.h>
#include "utils/ob_proxy_blowfish.h"
#include "utils/ob_proxy_utils.h"
#include "lib/container/ob_se_array.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
const char *ObBlowFish::ENC_KEY_BYTES_PROD_STR = "";

int ObBlowFish::encode(char *in, const int64_t in_len, char *out, const int64_t out_len, const char *secret_key)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(in) || OB_ISNULL(out)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(in), K(out), K(in_len), K(out_len), K(ret));
  } else {
    char tmp_out[OB_MAX_PASSWORD_LENGTH];
    memset(tmp_out, 0, sizeof(tmp_out));
    int64_t in_str_len = strlen(in);
    int64_t padding_len = BF_BLOCK - in_str_len % BF_BLOCK;
    if (OB_UNLIKELY(in_str_len + padding_len >= in_len)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WDIAG("in buffer size is not enough", K(padding_len), K(in), K(in_len), K(ret));
    } else {
      for (int64_t i = in_str_len; i < in_str_len + padding_len; ++i) {
        in[i] = static_cast<char>(padding_len);
      }
      if (OB_FAIL(do_bf_ecb_encrypt(reinterpret_cast<const unsigned char *>(in), in_str_len + padding_len,
                                    reinterpret_cast<unsigned char *>(tmp_out), OB_MAX_PASSWORD_LENGTH, BF_ENCRYPT, secret_key))) {
        LOG_WDIAG("fail to do bf ecb encrypt", K(in), K(padding_len), K(ret));
      } else if (OB_FAIL(covert_string_to_hex(tmp_out, strlen(tmp_out), out, out_len))) {
        LOG_WDIAG("fail to convert str to hex", K(ret));
      }
    }
  }
  return ret;
}

int ObBlowFish::decode(const char *in, const int64_t in_str_len, char *out, const int64_t out_len, const char *secret_key)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(in) || OB_ISNULL(out)
      || in_str_len < 0
      || in_str_len >= OB_MAX_PASSWORD_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(in), K(out), K(in_str_len), K(out_len), K(ret));
  } else {
    char tmp_out[OB_MAX_PASSWORD_LENGTH];
    memset(tmp_out, 0, sizeof(tmp_out));
    int64_t tmp_out_len = 0;
    if (OB_FAIL(convert_large_str_to_hex_v2(in, in_str_len, tmp_out, OB_MAX_PASSWORD_LENGTH, tmp_out_len))) {
    } else if (OB_FAIL(do_bf_ecb_encrypt(reinterpret_cast<const unsigned char *>(tmp_out), tmp_out_len,
                                         reinterpret_cast<unsigned char *>(out), out_len, BF_DECRYPT, secret_key))) {
      LOG_WDIAG("fail to do bf ecn encrypt", K(ret));
    } else {
      // trim padding number
      int64_t result_len = strlen(out);
      if (out[result_len - 1] >= 1 && out[result_len - 1] <= 8) {
        int64_t padding_len = out[result_len - 1];
        result_len = result_len - padding_len;
        memset(out + result_len, 0, padding_len);
      }
    }
  }
  return ret;
}

int ObBlowFish::do_bf_ecb_encrypt(const unsigned char *in, const int64_t in_str_len,
                                  unsigned char *out, const int64_t out_len,
                                  const int enc_mode, const char* secret_key)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(in) || OB_ISNULL(out)
      || OB_UNLIKELY(in_str_len % BF_BLOCK != 0)
      || OB_UNLIKELY(in_str_len > out_len)
      || OB_UNLIKELY(BF_ENCRYPT != enc_mode && BF_DECRYPT != enc_mode)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(in), K(out), K(in_str_len), K(out_len), K(enc_mode), K(ret));
  } else {
    BF_KEY bf_key;
    const char* real_bf_key = (NULL == secret_key) ? ENC_KEY_BYTES_PROD_STR : secret_key;
    BF_set_key(&bf_key, static_cast<int>(strlen(real_bf_key)), reinterpret_cast<const unsigned char *>(real_bf_key));
    int pos = 0;
    while (pos != in_str_len) {
      BF_ecb_encrypt(reinterpret_cast<const unsigned char *>(in + pos), reinterpret_cast<unsigned char *>(out + pos), &bf_key, enc_mode);
      pos += BF_BLOCK;
    }
  }
  return ret;
}

int ObBlowFish::covert_hex_to_string(const char *hex_str, const int64_t hex_len,
                                     char *str, const int64_t str_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(hex_str) || OB_UNLIKELY(hex_len <= 0)
      || OB_ISNULL(str) || OB_UNLIKELY(hex_len % 2 != 0)
      || OB_UNLIKELY(hex_len / 2 >= str_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(hex_str), K(hex_len), K(str), K(str_len), K(ret));
  } else {
    char tmp[3];
    int64_t tmp_len = hex_len / 2;
    for (int64_t i = 0; i < tmp_len; ++i) {
      tmp[0] = hex_str[2 * i];
      tmp[1] = hex_str[2 * i + 1];
      tmp[2] = '\0';
      sscanf(tmp, "%x", (unsigned int*)(&str[i]));
    }
    str[tmp_len] = '\0';
  }
  return ret;
}

int ObBlowFish::covert_string_to_hex(const char *str, const int64_t str_len,
                                     char *hex_str, const int64_t hex_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str) || OB_ISNULL(hex_str)
      || OB_UNLIKELY(hex_len <= str_len * 2)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < str_len; ++i) {
      if (OB_UNLIKELY(-1 == sprintf(hex_str, "%.2x", static_cast<unsigned char>(str[i])))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to call sprintf", KERRMSGS, K(ret));
      } else {
        hex_str += 2;
      }
    }
    *hex_str = '\0';
  }
  return ret;
}

int ObBlowFish::convert_large_str_to_hex_v2(const char *str, const int64_t str_len,
                                            char *hex_str, const int64_t hex_len, int64_t &hex_str_len)
{
  int ret = 0;

  char tmp_in[OB_MAX_PASSWORD_LENGTH] = {0};

  const char *cursor = str;
  const char *last_minus_pos = NULL;
  bool is_negative = false;
  int64_t padding_zero_num = 0;
  int64_t digit_num = 0;
  const int64_t base = 0xff;
  ObString input_str(str_len, str);
  ObSEArray<int64_t, OB_MAX_PASSWORD_LENGTH> byte_value_array;

  while (cursor != str + str_len) {
    cursor = static_cast<const char *>(memchr(reinterpret_cast<const void *>(cursor), '-', static_cast<int32_t>(str_len - (cursor - str))));
    if (NULL != cursor) {
      last_minus_pos = cursor;
      ++cursor;
    } else {
      break;
    }
  }

  if (NULL != last_minus_pos && last_minus_pos != str) {
    ret = OB_INVALID_ARGUMENT;
  } else {

    if (NULL == last_minus_pos) {
      cursor = str;
    } else {
      cursor = last_minus_pos + 1;
      is_negative = true;
    }

    digit_num = str_len - (cursor - str);
    for (int64_t i = 0; OB_SUCC(ret) && i < digit_num; ++i) {
      char c = cursor[i];
      if (('0' <= c && c <= '9')
          ||('a' <= c && c <= 'f')
          ||('A' <= c && c <= 'F')) {
        continue;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WDIAG("invalid hex str", K(c), K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
    // nothing
  } else {
    // JAVA BigInteger.toString(16) may loss front '0'
    // not support loss '0' more than 8
    if (digit_num % BF_BLOCK != 0) {
      padding_zero_num = BF_BLOCK - (digit_num % BF_BLOCK);
      MEMSET(tmp_in, '0', static_cast<size_t>(padding_zero_num));
    }
    MEMCPY(tmp_in + padding_zero_num, cursor, static_cast<size_t>(digit_num));
    digit_num += padding_zero_num;

    if (digit_num > hex_len) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("invalid hex str", K(digit_num), K(hex_len), K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < digit_num; i+=2) {
      int64_t part_value;
      ObString tmp_str(2, tmp_in + i);
      if (OB_FAIL(get_int_value(tmp_str, part_value, 16))) {
        LOG_WDIAG("fail to get int", K(ret));
      } else if (OB_FAIL(byte_value_array.push_back(part_value))) {
        LOG_WDIAG("fail to push back", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (is_negative) {
        int over_flow = 1;
        for (int64_t i = byte_value_array.count() - 1; i >= 0; --i) {
          byte_value_array.at(i) = (~byte_value_array.at(i)) & base;
        }

        for (int64_t i = byte_value_array.count() - 1; i >= 0 && over_flow; --i) {
          byte_value_array.at(i) += over_flow;
          over_flow = (byte_value_array.at(i) & (~base))? 1 : 0;
          byte_value_array.at(i) &= base;
        }

        if (over_flow) {
          LOG_WDIAG("maybe get password str:'-0', treat as success", K(input_str), K(ret));
        }
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < byte_value_array.count(); ++i) {
        if ((~base) & byte_value_array.at(i)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("unexpected byte value", K(base), K(byte_value_array.at(i)), K(ret));
        } else {
          hex_str[i] = static_cast<char>(byte_value_array.at(i));
          hex_str_len++;
        }
      }
    }
  }

  if (IS_DEBUG_ENABLED()) {
    ObString input_debug_string(digit_num, tmp_in);
    ObString output_debug_string(hex_str_len, hex_str);
    char buf[1024] = {};
    buf[0] = '0';
    buf[1] = 'x';
    int64_t pos = 2;
    for (int64_t i = 0; OB_SUCC(ret) && i < hex_str_len; ++i) {
      int64_t tmp_len = snprintf(buf + pos, static_cast<size_t>(1024 - pos), "%02x", static_cast<uint8_t>(output_debug_string[i]));
      if (tmp_len != 2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to snprintf", K(tmp_len), K(ret));
      } else {
        pos += tmp_len;
      }
    }
    ObString output_debug_value_string(pos, buf);
    LOG_DEBUG("debug preprocess output for blowfish", K(input_debug_string), K(str_len), K(output_debug_string),
            K(hex_str_len), K(output_debug_value_string), K(pos));
  }

  return ret;
}

int ObBlowFish::convert_large_str_to_hex_v1(const char *str, const int64_t str_len,
                                            char *hex_str, const int64_t hex_len, int64_t &hex_str_len)
{
  int ret = OB_SUCCESS;
  UNUSED(hex_len);
  const char *cursor = str;
  const char *last_minus_pos = NULL;
  bool is_negative = false;
  while (cursor != str + str_len) {
    cursor = static_cast<const char *>(memchr(reinterpret_cast<const void *>(cursor), '-', static_cast<int32_t>(str_len - (cursor - str))));
    if (NULL != cursor) {
      last_minus_pos = cursor;
      ++cursor;
    } else {
      break;
    }
  }
  if (NULL != last_minus_pos && last_minus_pos != str) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (NULL == last_minus_pos) {
      cursor = str;
    } else {
      cursor = last_minus_pos + 1;
      is_negative = true;
    }
    // trim leading zero
    while (cursor < str + str_len && *cursor == '0') {
      ++cursor;
    }
    if (cursor < str + str_len) {
      int64_t digit_num = str_len - (cursor - str);
      int64_t bit_num = digit_num * 4 + 1;
      int64_t int_num = static_cast<uint64_t>(bit_num + 31) >> 5;
      int64_t first_part_len = digit_num % 7;
      if (0 == first_part_len) {
        first_part_len = 7;
      }
      int64_t mag[64] = {0};
      memset(mag, 0, 64 * sizeof(int64_t));
      ObString tmp_str(static_cast<int32_t>(first_part_len), cursor);
      cursor += first_part_len;
      get_int_value(tmp_str, mag[int_num - 1], 16);
      int64_t radix = 0x10000000;
      int64_t part_value = 0;
      while (cursor < str + str_len) {
        tmp_str.assign_ptr(cursor, 7);
        cursor += 7;
        get_int_value(tmp_str, part_value, 16);
        destructive_multi_add(mag, int_num, radix, part_value);
      }
      int64_t start_pos = 0;
      for (; start_pos < int_num && mag[start_pos] == 0; ++start_pos);

      int64_t out_len = get_bit_len(mag, start_pos, int_num, is_negative) / 8 + 1;
      int64_t padding_len = 0;
      if (out_len % 8 != 0) {
        // hex_str buf len is 128, so we can use memmove safely
        padding_len = 8 - out_len % 8;
        out_len += padding_len;
        memset(hex_str, 0, padding_len);
      }
      hex_str_len = out_len;
      int64_t out_idx = out_len - 1;
      int64_t byte_pos = 4;
      int tmp_value = 0;
      for(int64_t i = start_pos; out_idx >= padding_len; --out_idx) {
        if (4 == byte_pos) {
          if (i < 0) {
            tmp_value = 0;
          } else if (i >= int_num) {
            tmp_value = is_negative ? -1 : 0;
          } else {
            int32_t var2 = static_cast<int32_t>(mag[int_num - i - 1 + start_pos]);
            tmp_value = is_negative ? (i <= start_pos ? -var2 : ~var2) : var2;
            ++i;
          }
          byte_pos = 1;
        } else {
          tmp_value = static_cast<int>(static_cast<uint32_t>(tmp_value) >> 8);
          ++byte_pos;
        }
        hex_str[out_idx] = static_cast<uint8_t>(tmp_value);
      }
    }
  }
  return ret;
}

void ObBlowFish::destructive_multi_add(int64_t *x, const int64_t int_num, int64_t y, int64_t z)
{
  int64_t ylong = y & 0xffffffffL;
  int64_t zlong = z & 0xffffffffL;
  int64_t product = 0;
  int64_t carry = 0;
  for (int64_t i = int_num - 1; i >= 0; --i) {
    product = ylong * (x[i] & 0xffffffffL) + carry;
    x[i] = product;
    carry = (static_cast<uint64_t>(product)) >> 32;
  }
  int64_t sum = (x[int_num - 1] & 0xffffffffL) + zlong;
  x[int_num -1] = sum;
  carry = static_cast<uint64_t>(sum) >> 32;
  for (int64_t i = int_num - 2; i >= 0; --i) {
    sum = (x[i] & 0xffffffffL) + carry;
    x[i] = sum;
    carry = (static_cast<uint64_t>(sum)) >> 32;
  }
}

int ObBlowFish::get_leading_zeros(int value)
{
  int ret = 0;
  if (value == 0) {
    ret =  32;
  } else {
    ret = 1;
    if (value >> 16 == 0) {
      ret += 16;
      value <<= 16;
    }
    if (value >> 24 == 0) {
      ret += 8;
      value <<= 8;
    }
    if (value >> 28 == 0) {
      ret += 4;
      value <<= 4;
    }
    if (value >> 30 == 0) {
      ret += 2;
      value <<= 2;
    }
    ret -= value >> 31;
  }
  return ret;
}

int ObBlowFish::get_bit_count(int var)
{
  var -= (-1431655766 & var) >> 1;
  var = (var & 858993459) + ((var >> 2) & 858993459);
  var = (var + (var >> 4)) & 252645135;
  var += var >> 8;
  var += var >> 16;
  return var & 255;
}

int ObBlowFish::get_bit_len(int64_t *mag, int64_t start_idx, int64_t int_num, const bool is_negative)
{
  int ret = -1;
  int mag_len = static_cast<int>(int_num - start_idx);
  int len = ((mag_len - 1) << 5) + 32 - get_leading_zeros(static_cast<int>(mag[start_idx]));
  if (0 == mag_len) {
    ret = 0;
  } else if (!is_negative) {
    ret = len;
  } else {
    // Check if value is a power of two
    bool is_pow = get_bit_count(static_cast<int>(mag[start_idx])) == 1;
    for(int64_t i = start_idx + 1; i < int_num && is_pow; ++i) {
      is_pow = mag[i] == 0;
    }
    ret = is_pow ? len - 1 : len;
  }
  return ret;
}

} // end namespace obproxy
} // end namespace oceanbase
