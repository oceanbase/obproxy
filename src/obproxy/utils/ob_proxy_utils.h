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

#ifndef OBPROXY_UTILS_H
#define OBPROXY_UTILS_H
#include "lib/ob_define.h"
#include "common/ob_object.h"
#include "lib/string/ob_fixed_length_string.h"
#include "utils/ob_proxy_lib.h"

namespace oceanbase
{
namespace common
{
class ObString;
} // end of namespace common

namespace obproxy
{
class ObRandomNumUtils
{
public:
  static int get_random_num(const int64_t min, const int64_t max, int64_t &random_num);
  // get the random value in the range [full_num/2, full_num]
  static int64_t get_random_half_to_full(const int64_t full_num);

  static int init_seed();
private:
  static int get_random_seed(int64_t &random_seed);
  static bool is_seed_inited_;
  static int64_t seed_;
};

// get int64_t value from ObString
int get_int_value(const common::ObString &str, int64_t &value, const int radix = 10);
int get_double_value(const common::ObString &str, double &value);

// Numerical operation
inline int64_t round(const int64_t input, const int64_t align)
{
  return (input + align - 1L) & ~(align - 1L);
}

inline int64_t next_pow2(const int64_t input)
{
  return (input > 0) ? (1ULL << (8 * sizeof(int64_t) - __builtin_clzll(input - 1))) : 1;
}

bool has_upper_case_letter(common::ObString &string);

void string_to_lower_case(char *str, const int32_t str_len);

void string_to_upper_case(char *str, const int32_t str_len);

bool is_available_md5(const common::ObString &string);

common::ObString trim_header_space(const common::ObString &input);

common::ObString trim_tailer_space(const common::ObString &input);

//trim ' or "
common::ObString trim_quote(const common::ObString &input);

int str_replace(char *input_buf, const int32_t input_size,
                char *output_buf, const int32_t output_size,
                const char *target_key, const int32_t target_key_len,
                const common::ObString &target_value, int32_t &output_pos);

int convert_timestamp_to_version(int64_t time_us, char *buf, int64_t len);
int paste_tenant_and_cluster_name(const common::ObString &tenant_name, const common::ObString &cluster_name,
                                  common::ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH> &key_string);

} // end of namespace obproxy
} // end of namespace oceanbase

#endif  // OBPROXY_UTILS_H
