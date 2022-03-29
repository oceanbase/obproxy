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
#include "utils/ob_proxy_utils.h"
#include "utils/ob_proxy_lib.h"
#include "lib/file/ob_file.h"
#include "lib/time/ob_time_utility.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
const char *OBPROXY_TIMESTAMP_VERSION_FORMAT = "%Y%m%d%H%i%s%f";

bool ObRandomNumUtils::is_seed_inited_ = false;
int64_t ObRandomNumUtils::seed_ = -1;

int ObRandomNumUtils::get_random_num(const int64_t min, const int64_t max, int64_t &random_num)
{
  int ret = OB_SUCCESS;
  random_num = 0;
  if (OB_UNLIKELY(min > max)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(min), K(max), K(ret));
  } else if (!is_seed_inited_) {
    if (OB_FAIL(init_seed())) {
      LOG_WARN("fail to init random seed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    random_num = min + random() % (max - min + 1);
  }
  return ret;
}

int64_t ObRandomNumUtils::get_random_half_to_full(const int64_t full_num)
{
  int ret = OB_SUCCESS;
  int64_t ret_value = 0;
  if (OB_UNLIKELY(full_num < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(full_num), K(ret));
  } else if (OB_UNLIKELY(0 == full_num)) {
    LOG_INFO("full_num is zero, will return 0 as random num");
  } else if (OB_FAIL(get_random_num(full_num / 2, full_num, ret_value))) {
    LOG_WARN("fail to get random num, will return full_num as random num", K(ret));
    ret_value = full_num;
  } else {/*do nothing*/}
  return ret_value;
}

int ObRandomNumUtils::init_seed()
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(!is_seed_inited_)) {
    if (OB_FAIL(get_random_seed(seed_))) {
      LOG_WARN("fail to get random seed", K(ret));
    } else {
      srandom(static_cast<uint32_t>(seed_));
      is_seed_inited_ = true;
    }
  }
  return ret;
}

int ObRandomNumUtils::get_random_seed(int64_t &seed)
{
  int ret = OB_SUCCESS;
  ObFileReader file_reader;
  const bool dio = false;
  if (OB_FAIL(file_reader.open(ObString::make_string("/dev/urandom"), dio))) {
    LOG_WARN("fail to open /dev/urandom", KERRMSGS, K(ret));
  } else {
    int64_t random_value = 0;
    int64_t ret_size = 0;
    int64_t read_size = sizeof(random_value);
    int64_t offset = 0;
    char *buf = reinterpret_cast<char *>(&random_value);
    if (OB_FAIL(file_reader.pread(buf, read_size, offset, ret_size))) {
      LOG_WARN("fail to read", KERRMSGS, K(ret));
    } else if (OB_UNLIKELY(read_size != ret_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("readed data is not enough", K(read_size), K(ret_size), K(ret));
    } else {
      seed = labs(random_value);
    }
    file_reader.close();
  }
  return ret;
}

int get_int_value(const ObString &str, int64_t &value, const int radix /*10*/)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(str.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("str is empty", K(str), K(ret));
  } else {
    static const int32_t MAX_INT64_STORE_LEN = 31;
    char int_buf[MAX_INT64_STORE_LEN + 1];
    int64_t len = std::min(str.length(), MAX_INT64_STORE_LEN);
    MEMCPY(int_buf, str.ptr(), len);
    int_buf[len] = '\0';
    char *end_ptr = NULL;
    value = strtoll(int_buf, &end_ptr, radix);
    if (('\0' != *int_buf ) && ('\0' == *end_ptr)) {
      // succ, do nothing
    } else {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid int value", K(value), K(ret), K(str));
    }
  }
  return ret;
}

int get_double_value(const ObString &str, double &value)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(str.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("str is empty", K(str), K(ret));
  } else {
    double ret_val = 0.0;
    static const int32_t MAX_UINT64_STORE_LEN = 32;
    char double_buf[MAX_UINT64_STORE_LEN + 1];
    int64_t len = std::min(str.length(),
                           static_cast<ObString::obstr_size_t>(MAX_UINT64_STORE_LEN));
    MEMCPY(double_buf, str.ptr(), len);
    double_buf[len] = '\0';
    char *end_ptr = NULL;
    ret_val = strtod(double_buf, &end_ptr);
    if (('\0' != *double_buf ) && ('\0' == *end_ptr)) {
      value = ret_val;
    } else {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid dobule value", K(value), K(str), K(ret));
    }
  }

  return ret;
}

bool has_upper_case_letter(common::ObString &string)
{
  bool bret = false;
  if (OB_LIKELY(NULL != string.ptr())) {
    int64_t length = static_cast<int64_t>(string.length());
    for (int64_t i = 0; !bret && i < length; ++i) {
      if (string[i] >= 'A' && string[i] <= 'Z') {
        bret = true;
      }
    }
  }
  return bret;
}

void string_to_lower_case(char *str, const int32_t str_len)
{
  if (OB_LIKELY(NULL != str) && OB_LIKELY(str_len > 0)) {
    for (int32_t i = 0; NULL != str && i < str_len; ++i, ++str) {
      if ((*str) >= 'A' && (*str) <= 'Z') {
        (*str) = static_cast<char>((*str) + 32);
      }
    }
  }
}

void string_to_upper_case(char *str, const int32_t str_len)
{
  if (OB_LIKELY(NULL != str) && OB_LIKELY(str_len > 0)) {
    for (int32_t i = 0; NULL != str && i < str_len; ++i, ++str) {
      if ((*str) >= 'a' && (*str) <= 'z') {
        (*str) = static_cast<char>((*str) - 32);
      }
    }
  }
}

bool is_available_md5(const ObString &string)
{
  bool bret = true;
  if (!string.empty() && OB_DEFAULT_PROXY_MD5_LEN == string.length()) {
    for (int64_t i = 0; bret && i < OB_DEFAULT_PROXY_MD5_LEN; ++i) {
      if (string[i] < '0'
          || ('9' < string[i] && string[i] < 'a')
          || string[i] > 'z') {
        bret = false;
      }
    }
  } else {
    bret = false;
  }
  return bret;
}

ObString trim_header_space(const ObString &input)
{
  ObString ret;
  if (NULL != input.ptr()) {
    const char *ptr = input.ptr();
    ObString::obstr_size_t start = 0;
    ObString::obstr_size_t end = input.length();
    while (start < end && IS_SPACE(ptr[start])) {
      start++;
    }
    ret.assign_ptr(input.ptr() + start, end - start);
  }
  return ret;
}

ObString trim_tailer_space(const ObString &input)
{
  ObString ret;
  if (NULL != input.ptr()) {
    const char *ptr = input.ptr();
    ObString::obstr_size_t start = 0;
    ObString::obstr_size_t end = input.length();
    while (start < end && IS_SPACE(ptr[end - 1])) {
      end--;
    }
    ret.assign_ptr(input.ptr() + start, end - start);
  }
  return ret;
}

common::ObString trim_quote(const common::ObString &input)
{
  ObString ret;
  //trim single quotes
  if (!input.empty() && input.length() > 1) {
    if ('\'' == input[0] && '\'' == input[input.length() - 1]) {
      ret.assign_ptr(input.ptr() + 1, input.length() - 2);
    }
  } else {
    ret.assign_ptr(input.ptr(), input.length());
  }
  return ret;
}

int str_replace(char *input_buf, const int32_t input_size,
                char *output_buf, const int32_t output_size,
                const char *target_key, const int32_t target_key_len,
                const ObString &target_value, int32_t &output_pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(input_buf) || OB_UNLIKELY(input_size <= 0)
      || OB_ISNULL(output_buf) || OB_UNLIKELY(output_size <= 0)
      || OB_ISNULL(target_key) || OB_UNLIKELY(target_key_len <= 0)
      || OB_UNLIKELY(output_pos < 0) || OB_UNLIKELY(output_pos >= output_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid_argument", KP(input_buf), K(input_size), KP(output_buf), K(output_size),
             KP(target_key), K(target_key_len), K(output_pos), K(ret));
  } else {
    char *found_ptr = NULL;
    int32_t ipos = 0;
    int32_t copy_len = 0;
    while (OB_SUCC(ret)
           && ipos < input_size
           && NULL != (found_ptr = strstr(input_buf + ipos, target_key))) {
      copy_len = static_cast<int32_t>(found_ptr - input_buf - ipos);
      if (output_pos + copy_len < output_size) {
        memcpy(output_buf + output_pos, input_buf + ipos, copy_len);
        output_pos += copy_len;
        ipos += (copy_len + target_key_len);
        if (!target_value.empty()) {
          copy_len = target_value.length();
          if (output_pos + copy_len < output_size) {
            memcpy(output_buf + output_pos, target_value.ptr(), copy_len);
            output_pos += copy_len;
          } else {
            ret = OB_SIZE_OVERFLOW;
            LOG_INFO("size is overflow", K(output_pos), K(copy_len), K(output_size), K(ret));
          }
        }
      } else {
        ret = OB_SIZE_OVERFLOW;
        LOG_INFO("size is overflow", K(output_pos), K(copy_len), K(output_size), K(ret));
      }
    }

    if (OB_SUCC(ret) && ipos < input_size) {
      copy_len = input_size - ipos;
      if (output_pos < output_size && output_pos + copy_len < output_size) {
        memcpy(output_buf + output_pos, input_buf + ipos, copy_len);
        output_pos += copy_len;
      } else {
        ret = OB_SIZE_OVERFLOW;
        LOG_INFO("size is overflow", K(output_pos), K(copy_len), K(output_size), K(ret));
      }
    }

    ObString key(target_key_len, target_key);
    ObString input(input_size, input_buf);
    ObString output(output_pos, output_buf);
    LOG_DEBUG("finish str_replace", K(key), K(target_value), K(input), K(output), K(ret));
  }
  return ret;
}

int convert_timestamp_to_version(int64_t time_us, char *buf, int64_t len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(ObTimeUtility::usec_format_to_str(time_us, ObString(OBPROXY_TIMESTAMP_VERSION_FORMAT),
                                                buf, len, pos))) {
    LOG_WARN("fail to format timestamp  to str", K(time_us), K(ret));
  } else if (OB_UNLIKELY(pos < 3)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid timestamp", K(time_us), K(pos), K(ret));
  } else {
    buf[pos - 3] = '\0'; // ms
  }
  return ret;
}

int paste_tenant_and_cluster_name(const ObString &tenant_name, const ObString &cluster_name,
                                  ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH> &key_string)
{
  int ret = OB_SUCCESS;
  char buf[OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH];
  int64_t len = 0;
  len = static_cast<int64_t>(snprintf(buf, OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH, 
    "%.*s#%.*s", tenant_name.length(), tenant_name.ptr(), cluster_name.length(), cluster_name.ptr()));
  if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to fill buf", K(ret), K(tenant_name), K(cluster_name));
  } else if (OB_FAIL(key_string.assign(buf))) {
    LOG_WARN("assign failed", K(ret));
  }

  return ret;
}

} // end of namespace obproxy
} // end of namespace oceanbase
