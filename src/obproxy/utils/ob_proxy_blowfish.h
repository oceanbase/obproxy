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

#ifndef OBPROXY_BLOWFISH_H
#define OBPROXY_BLOWFISH_H

/* use openssl's blowfish algorithm to Encrypt/Decrypt
 * the use of interface of openssl, refer to:
 * https://www.openssl.org/docs/man1.0.2/man3/blowfish.html
 */

#include "utils/ob_proxy_lib.h"

namespace oceanbase
{
namespace obproxy
{

class ObBlowFish
{
public:
  static int encode(char *in, const int64_t in_len, char *out, const int64_t out_len);
  static int decode(const char *in, const int64_t in_len, char *out, const int64_t out_len);

private:
  static int covert_hex_to_string(const char *hex_str, const int64_t hex_len,
                                  char *str, const int64_t str_len);
  static int covert_string_to_hex(const char *str, const int64_t str_len,
                                  char *hex_str, const int64_t hex_len);
  static int convert_large_str_to_hex(const char *str, const int64_t str_len,
                                      char *hex_str, const int64_t hex_len, int64_t &out_len);
  static int do_bf_ecb_encrypt(const unsigned char *in, const int64_t in_str_len,
                               unsigned char *out, const int64_t out_len,
                               const int enc_mode);
  static void destructive_multi_add(int64_t *x, const int64_t int_num, int64_t y, int64_t z);

  static int get_leading_zeros(int value);
  static int get_bit_count(int var0);
  static int get_bit_len(int64_t *mag, int64_t start_idx, int64_t int_num, const bool is_negative);

public:
  static const char *ENC_KEY_BYTES_PROD_STR;
};

} // end obproxy
} // end oceanbase

#endif
