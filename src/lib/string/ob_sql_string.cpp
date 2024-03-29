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

#include "lib/string/ob_sql_string.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace common
{

ObSqlString::ObSqlString(const int64_t mod_id /* = ObModIds::OB_SQL_STRING */)
    : data_(NULL), data_size_(0), len_(0), allocator_(mod_id)
{
}

ObSqlString::~ObSqlString()
{
  reset();
}

void ObSqlString::reset()
{
  if (NULL != data_) {
    allocator_.free(data_);
    data_ = NULL;
  }

  data_size_ = 0;
  len_ = 0;
}

int ObSqlString::append(const char *str)
{
  return append(str, NULL == str ? 0 : strlen(str));
}

int ObSqlString::append(const char *str, const int64_t len)
{
  int ret = OB_SUCCESS;
  // %str can be NULL
  if (len < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(ret), K(len));
  } else {
    if (NULL != str && len >= 0) {
      const int64_t need_len = len_ + len;
      if (OB_FAIL(reserve(need_len))) {
        LOG_WDIAG("reserve data failed", K(ret), K(need_len));
      } else {
        MEMCPY(data_ + len_, str, len);
        len_ += len;
        data_[len_] = '\0';
      }
    }
  }

  return ret;
}

int ObSqlString::append(const ObString &str)
{
  return append(str.ptr(), str.length());
}

int ObSqlString::append_fmt(const char *fmt, ...)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(fmt)) {
    // do nothing
  } else {
    va_list ap;
    va_start(ap, fmt);
    if (OB_FAIL(vappend(fmt, ap))) {
      LOG_WDIAG("append failed", K(ret), K(fmt));
    }
    va_end(ap);
  }
  return ret;
}

int ObSqlString::assign(const char *str)
{
  reuse();
  // %str can be NULL
  return append(str);
}

int ObSqlString::assign(const char *str, const int64_t len)
{
  reuse();
  int ret = OB_SUCCESS;
  // %str can be NULL
  if (len < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(ret), K(len));
  } else if (OB_FAIL(append(str, len))) {
    LOG_WDIAG("append string failed", K(ret), K(str), K(len));
  }
  return ret;
}

int ObSqlString::assign(const ObString &str)
{
  reuse();
  return append(str);
}

int ObSqlString::assign_fmt(const char *fmt, ...)
{
  reuse();
  int ret = OB_SUCCESS;
  if (NULL == fmt) {
    // do nothing
  } else {
    va_list ap;
    va_start(ap, fmt);
    if (OB_FAIL(vappend(fmt, ap))) {
      LOG_WDIAG("append failed", K(ret), K(fmt));
    }
    va_end(ap);
  }
  return ret;
}

const ObString ObSqlString::string() const
{
  return ObString(0, static_cast<int32_t>(len_), data_);
}

int ObSqlString::set_length(const int64_t len)
{
  int ret = OB_SUCCESS;
  if (len < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(ret), K(len));
  } else if (len > capacity()) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_EDIAG("try set too long length, buffer maybe overflow",
        K(ret), "capacity", capacity(), K(len));
  } else {
    len_ = len;
    if (data_size_ > 0) {
      data_[len_] = '\0';
    }
  }
  return ret;
}

int64_t ObSqlString::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || 0 == buf_len) {
    // do nothing
  } else {
    const ObString s = string();
    pos = s.to_string(buf, buf_len);
  }
  return pos;
}

int ObSqlString::vappend(const char *fmt, va_list ap)
{
  int ret = OB_SUCCESS;
  va_list ap2;
  va_copy(ap2, ap);

  if (NULL == fmt) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(ret), KP(fmt));
  } else {
    int64_t n = vsnprintf(data_ + len_, data_size_ - len_, fmt, ap);
    if (n < 0) {
      ret = OB_ERR_SYS;
      LOG_WDIAG("vsnprintf failed", K(ret), K(n), K(errno));
    } else if (n >= data_size_ - len_) {
      if (OB_FAIL(reserve(n + len_))) {
        LOG_WDIAG("reserve data failed", K(ret), "size", n + len_);
      } else {
        n = vsnprintf(data_ + len_, data_size_ - len_, fmt, ap2);
        if (n < 0) {
          ret = OB_ERR_SYS;
          LOG_WDIAG("vsnprintf failed", K(ret), K(n), K(errno));
        } else {
          if (n >= data_size_ - len_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("unexpected value returned", K(ret),
                K(n), "buff size", data_size_ - len_);
          } else {
            len_ += n;
          }
        }
      }
    } else {
      len_ += n;
    }
  }

  va_end(ap2);
  return ret;
}

void ObSqlString::reuse()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(set_length(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("set zero length will always success", K(ret));
  }
}

int ObSqlString::reserve(const int64_t size)
{
  int ret = OB_SUCCESS;
  const int64_t need_size = size + 1; // 1 more byte for C terminating null byte ('\0')
  static const int64_t BIT_PER_BYTE = 8;
  if (size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(ret), K(size));
  } else {
    if (data_size_ < need_size) {
      int64_t extend_to = data_size_ > MAX_SQL_STRING_LEN ? data_size_ : MAX_SQL_STRING_LEN;
      for (int64_t i = 0; i < static_cast<int64_t>(sizeof(extend_to)) * BIT_PER_BYTE
          && extend_to < need_size; ++i) {
        extend_to = extend_to << 1;
      }
      if (extend_to < need_size) {
        ret = OB_SIZE_OVERFLOW;
        LOG_EDIAG("size overflow", K(ret), K(extend_to), K(need_size));
      } else if (OB_FAIL(extend(extend_to))) {
        LOG_WDIAG("extend failed", K(ret), K(extend_to));
      }
    }
  }
  return ret;
}

int ObSqlString::extend(const int64_t size)
{
  int ret = OB_SUCCESS;
  char *new_data = NULL;
  if (size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(ret), K(size));
  } else if (NULL == (new_data = (static_cast<char *>(allocator_.alloc(size))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("allocate memory failed", K(ret), K(size));
  } else {
    if (NULL != data_) {
      MEMCPY(new_data, data_, len_ + 1);
      allocator_.free(data_);
      data_ = NULL;
    }
    data_ = new_data;
    data_size_ = size;
  }
  return ret;
}

} // end namespace common
} // end namespace oceanbase
