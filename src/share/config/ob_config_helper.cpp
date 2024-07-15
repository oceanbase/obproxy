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

#include "share/config/ob_config_helper.h"
#include "share/config/ob_config.h"
#include "obproxy/iocore/eventsystem/ob_buf_allocator.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/alloc/alloc_assist.h"
#include "lib/hash_func/murmur_hash.h"

namespace oceanbase
{
namespace common
{

bool ObConfigIpChecker::check(const ObConfigItem &t) const
{
  struct in_addr in;
  int result = inet_pton(AF_INET, t.str(), &in);
  if (result != 1) {
    struct in6_addr in6;
    result = inet_pton(AF_INET6, t.str(), &in6);
  }
  return result == 1;
}

ObConfigConsChecker:: ~ObConfigConsChecker()
{
  if (NULL != left_) {
    delete left_;
  }
  if (NULL != right_) {
    delete right_;
  }
}
bool ObConfigConsChecker::check(const ObConfigItem &t) const
{
  return (NULL == left_ ? true : left_->check(t))
         && (NULL == right_ ? true : right_->check(t));
}

bool ObConfigGreaterThan::check(const ObConfigItem &t) const
{
  return t > val_;
}

bool ObConfigGreaterEqual::check(const ObConfigItem &t) const
{
  return t >= val_;
}

bool ObConfigLessThan::check(const ObConfigItem &t) const
{
  return t < val_;
}

bool ObConfigLessEqual::check(const ObConfigItem &t) const
{
  return t <= val_;
}

bool ObConfigLogLevelChecker::check(const ObConfigItem &t) const
{
  return OB_SUCCESS == OB_LOGGER.parse_check(t.str(), static_cast<int32_t>(STRLEN(t.str())));
}

int64_t ObConfigCapacityParser::get(const char *str, bool &valid)
{
  char *p_unit = NULL;
  int64_t value = 0;

  if (OB_ISNULL(str) || '\0' == str[0]) {
    valid = false;
  } else {
    valid = true;
    value = std::max(0L, strtol(str, &p_unit, 0));

    if (0 == STRCASECMP("b", p_unit)
        || 0 == STRCASECMP("byte", p_unit)) {
      // do nothing
    } else if (0 == STRCASECMP("kb", p_unit)
               || 0 == STRCASECMP("k", p_unit)) {
      value <<= CAP_KB;
    } else if ('\0' == *p_unit
               || 0 == STRCASECMP("mb", p_unit)
               || 0 == STRCASECMP("m", p_unit)) {
      // default is metabyte
      value <<= CAP_MB;
    } else if (0 == STRCASECMP("gb", p_unit)
               || 0 == STRCASECMP("g", p_unit)) {
      value <<= CAP_GB;
    } else if (0 == STRCASECMP("tb", p_unit)
               || 0 == STRCASECMP("t", p_unit)) {
      value <<= CAP_TB;
    } else if (0 == STRCASECMP("pb", p_unit)
               || 0 == STRCASECMP("p", p_unit)) {
      value <<= CAP_PB;
    } else {
      valid = false;
      OB_LOG(EDIAG, "set capacity error", K(str), K(p_unit));
    }
  }

  return value;
}

// ObConfigVariableString
ObConfigVariableString::ObConfigVariableString(const ObConfigVariableString& other): used_len_(0)
{
  rewrite(other.ptr(), other.size());
}

void ObConfigVariableString::reset()
{
  if (used_len_ > VARIABLE_BUF_LEN) {
    obproxy::op_fixed_mem_free(data_union_.ptr_, used_len_ + 1);
  }
  MEMSET(static_cast<void*>(&data_union_), 0, sizeof(data_union_));
  used_len_ = 0;
}

ObConfigVariableString& ObConfigVariableString::operator=(const ObConfigVariableString& other)
{
  int ret = OB_SUCCESS;
  if (this != &other && OB_FAIL(rewrite(other.ptr(), other.size()))) {
    OB_LOG(WDIAG, "fail to alloc rewrite ObConfigVariableString", K(ret));
  }
  return *this;
}

uint64_t ObConfigVariableString::hash(uint64_t seed) const
{
  seed = murmurhash(ptr(), static_cast<int32_t>(used_len_), seed);
  return seed;
}

bool ObConfigVariableString::operator==(const ObConfigVariableString& other) const
{
  bool bret = true;
  if (this != &other) {
    if (other.used_len_ != used_len_) {
      bret = false;
    } else {
      bret = (0 == STRNCMP(this->ptr(), other.ptr(), used_len_));
    }
  }
  return bret;
}

int ObConfigVariableString::alloc_mem(int64_t len)
{
  int ret = OB_SUCCESS;
  reset();
  if (len > VARIABLE_BUF_LEN
      && OB_ISNULL(data_union_.ptr_ = static_cast<char*>(
                                                obproxy::op_fixed_mem_alloc(len + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WDIAG, "fail to alloc data_info_ mem", K(len), K(ret));
  } else {
    used_len_ = len;
  }
  return ret;
}

int ObConfigVariableString::rewrite(const char *ptr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ptr)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WDIAG, "unexcepted to input null pointer", K(ptr), K(ret));
  } else {
    int64_t len = static_cast<int64_t>(strlen(ptr));
    if (OB_FAIL(rewrite(ptr, len))) {
      OB_LOG(WDIAG, "fail to rewrite ptr", K(ptr), K(len), K(ret));
    }
  }
  return ret;
}

int ObConfigVariableString::rewrite(const ObString &str)
{
  int ret = OB_SUCCESS;
  const char* ptr = str.ptr();
  int64_t len = str.length();
  if (OB_FAIL(rewrite(ptr, len))) {
    OB_LOG(WDIAG, "fail to rewrite ObString", K(ptr), K(len), K(ret));
  }
  return ret;
}

int ObConfigVariableString::rewrite(const char *ptr, const int64_t len)
{
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  // 1. 扩缩容
  if (OB_UNLIKELY(len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WDIAG, "write length can't less than 0", K(ptr), K(len), K(ret));
  } else if (used_len_ != len && OB_FAIL(alloc_mem(len))) {
      OB_LOG(WDIAG, "fail to alloc_mem", K(len), K(ret));
  }
  // 2. 写入数据
  if (OB_SUCC(ret)
      && OB_NOT_NULL(ptr)
      && OB_LIKELY(len > 0)
      && OB_FAIL(databuff_printf(used_len_ > VARIABLE_BUF_LEN ? 
                                        data_union_.ptr_ : data_union_.buf_,
                                 len > VARIABLE_BUF_LEN ? 
                                      used_len_ + 1 : VARIABLE_BUF_LEN + 1,
                                  pos, "%.*s", static_cast<int32_t>(len), ptr))) {
    OB_LOG(WDIAG, "fail to databuff_printf ptr to union buffer", K(ptr), K(len),
           K(ret));
  }
  return ret;
}

int64_t ObConfigVariableString::to_string(char *buf, const int64_t len) const
{
  int64_t pos = 0;
  if (OB_LIKELY(NULL != buf) && OB_LIKELY(len > 0)) {
    if (NULL != ptr()) {
      pos = snprintf(buf, len, "%.*s", static_cast<int>(used_len_), ptr());
      if (pos < 0) {
        pos = 0;
      } else if (pos >= len) {
        pos = len - 1;
      }
    }
  }
  return pos;
}

ObConfigVariableString::operator const ObString() const
{
  return ObString(used_len_, ptr());
}

} // end of namepace common
} // end of namespace oceanbase
