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

#ifndef OBPROXY_BUF_H
#define OBPROXY_BUF_H
#include "lib/ob_define.h"
#include "iocore/eventsystem/ob_buf_allocator.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

template <int64_t BUF_LEN>
class ObVariableLenBuffer
{
public:
  ObVariableLenBuffer() : is_inited_(false), buf_(NULL), total_len_(0), valid_len_(0) { }
  ~ObVariableLenBuffer() { destroy(); }

  int init(const int64_t mem_len);
  bool is_inited() const { return is_inited_; }
  void reset();
  void destroy() { reset(); }

  bool is_full_filled() const { return valid_len_ == total_len_; }
  bool empty() const { return 0 == valid_len_; }
  int64_t len() const { return valid_len_; }
  int64_t total_len() const { return total_len_; }
  int64_t remain() const;
  int write(const char *data, const int64_t len);

  char *pos();
  int consume(const int64_t consume_len);
  const char *ptr() const { return buf_; }
  bool operator==(const ObVariableLenBuffer<BUF_LEN> &tmp_len_buffer) const
  {
    bool bret = false;

    if (valid_len_ == tmp_len_buffer.valid_len_
        && 0 == strncmp(buf_, tmp_len_buffer.buf_, valid_len_)) {
      bret = true;
    }

    return bret;
  }

    int64_t to_string(char *buf, const int64_t len) const
    {
      int64_t pos = 0;
      if (OB_LIKELY(NULL != buf) && OB_LIKELY(len > 0)) {
        if (NULL != ptr()) {
          pos = snprintf(buf, len, "%.*s", static_cast<int>(valid_len_), buf_);
          if (pos < 0) {
            pos = 0;
          } else if (pos >= len) {
            pos = len - 1;
          }
        }
      }
      return pos;
    }

private:
  bool is_inited_;
  char *buf_;
  int64_t total_len_;
  int64_t valid_len_;
  char fix_buf_[BUF_LEN];
  DISALLOW_COPY_AND_ASSIGN(ObVariableLenBuffer);
};

template <int64_t BUF_LEN>
inline void ObVariableLenBuffer<BUF_LEN>::reset()
{
  if (OB_LIKELY(is_inited_)) {
    if (total_len_ > BUF_LEN && NULL != buf_) {
      op_fixed_mem_free(buf_, total_len_);
    }
    buf_ = NULL;
    total_len_ = 0;
    valid_len_ = 0;
    is_inited_ = false;
  }
}

template <int64_t BUF_LEN>
inline int ObVariableLenBuffer<BUF_LEN>::init(const int64_t mem_len)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = common::OB_INIT_TWICE;
    _PROXY_LOG(WDIAG, "has already inited, mem_len=%ld, inited=%d, ret=%d", mem_len, is_inited_, ret);
  } else if (OB_UNLIKELY(mem_len <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    _PROXY_LOG(WDIAG, "invalid input valid, mem_len=%ld, ret=%d", mem_len, ret);
  } else if (mem_len <= BUF_LEN) {
    buf_ = fix_buf_;
  } else if (OB_ISNULL(buf_ = static_cast<char *>(op_fixed_mem_alloc(mem_len)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    _PROXY_LOG(EDIAG, "fail to alloc mem, mem_len=%ld, ret=%d", mem_len, ret);
  }
  if (OB_SUCC(ret)) {
    total_len_ = mem_len;
    valid_len_ = 0;
    is_inited_ = true;
  }

  return ret;
}

template <int64_t BUF_LEN>
inline char *ObVariableLenBuffer<BUF_LEN>::pos()
{
  char *pos = NULL;
  if (OB_LIKELY(is_inited_)) {
    if (OB_LIKELY(valid_len_ >= 0) && OB_LIKELY(valid_len_ < total_len_)) {
      pos = buf_ + valid_len_;
    }
  }
  return pos;
}

template <int64_t BUF_LEN>
inline int ObVariableLenBuffer<BUF_LEN>::consume(const int64_t consume_len)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    _PROXY_LOG(WDIAG, "not init, ret=%d", ret);
  } else if (OB_UNLIKELY(consume_len < 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    _PROXY_LOG(WDIAG, "consume_len must >= 0, consume_len=%ld, ret=%d",
               consume_len, ret);
  } else if (OB_UNLIKELY(valid_len_ + consume_len > total_len_)) {
    ret = common::OB_ERR_UNEXPECTED;
    _PROXY_LOG(WDIAG, "valid_len_ + consume_len must <= %ld, valid_len = %ld, "
               "consume_len = %ld, ret = %d", total_len_, valid_len_,
               consume_len, ret);
  } else {
    valid_len_ += consume_len;
  }
  return ret;
}

template <int64_t BUF_LEN>
inline int64_t ObVariableLenBuffer<BUF_LEN>::remain() const
{
  int64_t len = -1;
  if (OB_LIKELY(is_inited_)) {
    len = total_len_ - valid_len_;
  }
  return len;
}

template <int64_t BUF_LEN>
inline int ObVariableLenBuffer<BUF_LEN>::write(const char *data, const int64_t len)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    PROXY_LOG(WDIAG, "not init", K(ret));
  } else if (OB_ISNULL(data) || OB_UNLIKELY(len <= 0) || OB_UNLIKELY(valid_len_ + len > total_len_)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_LOG(WDIAG, "invalid arguments", K(data), K(len), K_(valid_len), K_(total_len), K(ret));
  } else {
    MEMCPY(buf_ + valid_len_, data, len);
    valid_len_ += len;
  }
  return ret;
}

//////////////////////////////////////////////////////////////////////////////////////////////////

template <int64_t BUF_LEN>
class ObFixedLenBuffer
{
public:
  ObFixedLenBuffer() { reset(); }
  ~ObFixedLenBuffer() { }

  bool is_full_filled() const { return valid_len_ == BUF_LEN; }
  bool empty() const { return 0 == valid_len_; }
  int64_t remain() const;
  int consume(const int64_t consume_len);
  int write(const char *data, const int64_t len);
  char *pos();
  const char *ptr() const { return buf_; }
  int64_t len() const { return valid_len_; }
  int64_t total_len() const { return BUF_LEN; }

  inline void reset() { valid_len_ = 0; }

private:
  char buf_[BUF_LEN];
  int64_t valid_len_;
  DISALLOW_COPY_AND_ASSIGN(ObFixedLenBuffer);
};

template <int64_t BUF_LEN>
inline int64_t ObFixedLenBuffer<BUF_LEN>::remain() const
{
  return BUF_LEN - valid_len_;
}

template <int64_t BUF_LEN>
inline int ObFixedLenBuffer<BUF_LEN>::consume(const int64_t consume_len)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(consume_len < 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_LOG(WDIAG, "consume_len must >= 0", K(consume_len), K(ret));
  } else if (OB_UNLIKELY(valid_len_ + consume_len > BUF_LEN)) {
    ret = common::OB_ERR_UNEXPECTED;
    _PROXY_LOG(WDIAG, "valid_len_ + consume_len must <= %ld, valid_len = %ld, "
               "consume_len = %ld, ret = %d", BUF_LEN, valid_len_, consume_len, ret);
  } else {
    valid_len_ += consume_len;
  }
  return ret;
}

template <int64_t BUF_LEN>
inline char *ObFixedLenBuffer<BUF_LEN>::pos()
{
  char *pos = NULL;
  if (OB_LIKELY(valid_len_ >= 0) && OB_LIKELY(valid_len_ < BUF_LEN)) {
    pos = buf_ + valid_len_;
  }
  return pos;
}

template <int64_t BUF_LEN>
inline int ObFixedLenBuffer<BUF_LEN>::write(const char *data, const int64_t len)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(data) || OB_UNLIKELY(len <= 0) || OB_UNLIKELY(valid_len_ + len > BUF_LEN)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_LOG(WDIAG, "invalid arguments", K(data), K(len), K_(valid_len), K(ret));
  } else {
    MEMCPY(buf_ + valid_len_, data, len);
    valid_len_ += len;
  }
  return ret;
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_BUF_H
