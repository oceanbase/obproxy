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

#include "obutils/ob_proxy_string_utils.h"
#include "lib/allocator/ob_malloc.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
ObProxyVariantString::ObProxyVariantString(): config_string_(), data_(NULL), data_size_(0)
{

}
ObProxyVariantString::~ObProxyVariantString()
{
  reset();
}
ObProxyVariantString::ObProxyVariantString(const ObProxyVariantString& vstr): data_(NULL), data_size_(0)
{
  if (&vstr != this) {
    reset();
    set_value(vstr.config_string_);
  }
}
bool ObProxyVariantString::equals(const ObProxyVariantString& vstr) const
{
  return config_string_.compare(vstr.config_string_) == 0;
}

ObProxyVariantString& ObProxyVariantString::operator=(const ObProxyVariantString& vstr)
{
  if (&vstr != this) {
    reset();
    set_value(vstr.config_string_.length(), vstr.config_string_.ptr());
  }
  return *this;
}

void ObProxyVariantString::reset()
{
  if (!OB_ISNULL(data_))
  {
    ob_free(data_);
    data_ = NULL;
    data_size_ = 0;
  }
  config_string_.reset();
}
DEF_TO_STRING(ObProxyVariantString)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(config_string));
  J_OBJ_END();
  return pos;
}

// To simplify, free mem everytime and alloc mem. so best save config which will not change after init
bool ObProxyVariantString::set_value(const common::ObString &value)
{
  return set_value(value.length(), value.ptr());
}

bool ObProxyVariantString::set_value(const int32_t len, const char *value)
{
  reset();
  data_size_ = len + 1;
  data_ = (char*)ob_malloc(data_size_);
  if (OB_ISNULL(data_)) {
    LOG_WDIAG("ob_malloc failed", K(data_size_));
    return false;
  }
  MEMCPY(data_, value, len);
  data_[len] = '\0';
  config_string_.assign_ptr(data_, len);
  return true;
}

bool ObProxyVariantString::set_value_with_quote(const common::ObString &value, const char quote)
{
  return set_value_with_quote(value.length(), value.ptr(), quote);
}

bool ObProxyVariantString::set_value_with_quote(const int32_t len, const char *value, const char quote)
{
  reset();
  data_size_ = len + 3;
  data_ = (char*)ob_malloc(data_size_);
  if (OB_ISNULL(data_)) {
    LOG_WDIAG("ob_malloc failed", K_(data_size));
    return false;
  } else {
    data_[0] = quote;
    MEMCPY(data_ + 1, value, len);
    data_[len + 1] = quote;
    data_[len + 2] = '\0';
    config_string_.assign_ptr(data_, len + 2);
  }

  return true;
}

void ObProxyVariantString::set_integer(const int64_t other)
{
  char buf[1024];
  const int32_t len = snprintf(buf, 1024, "%ld", other);
  if (len <= 0 || len >= 1024) {
    LOG_EDIAG("snprintf failed", K(other));
  } else {
    set_value(len, buf);
  }
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
