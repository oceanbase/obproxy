/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OB_SIMPLE_TRACE_H
#define _OB_SIMPLE_TRACE_H

#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace common
{

template<int64_t BUFFER_SIZE>
class ObSimpleTraceBase
{
public:
  ObSimpleTraceBase() { reset(); }
  ~ObSimpleTraceBase() { }
  void reset()
  {
    pos_ = 0;
    buf_[0] = '\0';
  }
  template <typename ... Args>
  int log_it(const char *info, Args const & ... args)
  {
    int ret = common::OB_SUCCESS;
    const int64_t saved_pos = pos_;
    const char delimiter1[] = " | ";
    const char delimiter2[] = "... | ";
    if (OB_FAIL(fill_buffer(buf_, BUFFER_SIZE - sizeof(delimiter2), pos_, info, args...))) {
      if ((BUFFER_SIZE - saved_pos) >= BUFFER_SIZE / 2) {
        // rewrite ret
        ret = OB_SUCCESS;
        (void)databuff_printf(buf_, BUFFER_SIZE, pos_, delimiter2);
      }
    } else {
      (void)databuff_printf(buf_, BUFFER_SIZE, pos_, delimiter1);
    }
    if (OB_FAIL(ret)) {
      pos_ = saved_pos;
      buf_[pos_] = '\0';
    }
    return ret;
  }
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "%s", buf_);
    return pos;
  }
private:
  int fill_kv(char *buf, const int64_t buf_len, int64_t &pos,
              const bool with_comma)
  {
    UNUSED(buf);
    UNUSED(buf_len);
    UNUSED(pos);
    UNUSED(with_comma);
    return common::OB_SUCCESS;
  }

  int fill_kv(char *buf, const int64_t buf_len, int64_t &pos,
              const bool with_comma,
              const ObILogKV &kv)
  {
    return kv.print(buf, buf_len, pos, with_comma);
  }

  template <typename Key, typename Value, typename ... Args>
  int fill_kv(char *buf, const int64_t buf_len, int64_t &pos,
              const bool with_comma,
              const Key &key,
              const Value &value,
              Args const & ... args)
  {
    int ret = OB_SUCCESS;
    ret = fill_kv(buf, buf_len, pos, with_comma, LOG_KV(key, value));
    if (OB_SUCC(ret)) {
      ret = fill_kv(buf, buf_len, pos, 1, args...);
    }
    return ret;
  }
  template <typename ... Args>
  int fill_buffer(char *data, const int64_t buf_len,
                  int64_t &pos,
                  const char *info,
                  Args const & ... args)
  {
    int ret = OB_SUCCESS;
    int64_t tmp_pos = pos;
    ret = databuff_printf(data, buf_len, pos, "%s ", info);
    if (OB_SUCC(ret)) {
      ret = fill_kv(data, buf_len, pos, 0, args...);
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(OB_LOGGER.get_log_level() >= OB_LOG_LEVEL_TRACE)) {
      char tmp_buf[1024];
      memset(tmp_buf, 0, sizeof(tmp_buf));
      int64_t copy_len = pos - tmp_pos > 1023 ? 1023 : pos - tmp_pos; 
      MEMCPY(tmp_buf, data + tmp_pos, copy_len);
      OB_LOG(TRACE, "trace_log", K(tmp_buf));
    }
    return ret;
  }
private:
  char buf_[BUFFER_SIZE];
  int64_t pos_;
};

template<int64_t TRACE_SIZE>
class ObSimpleTrace
{
public:
  ObSimpleTrace() { reset(); }
  ~ObSimpleTrace() { }
  void reset()
  {
    first_idx_ = 0;
    cur_idx_ = 0;
    need_print_ = false;
    for (int64_t i = 0; i < BASE_TRACE_CNT; i++) {
      traces_[i].reset();
    }
  }
  template <typename ... Args>
  void log_it(const char *info, Args const & ... args)
  {
    if (need_print_) {
      int ret = common::OB_SUCCESS;
      for (int64_t i = 0; i < 2; i++) {
        cur_idx_ = (cur_idx_ + i) % BASE_TRACE_CNT;
        if (1 == i) {
          if (cur_idx_ == first_idx_) {
            if (OB_UNLIKELY(OB_LOGGER.get_log_level() >= OB_LOG_LEVEL_TRACE)) {
              // _LOG(TRACE, "print_trace_log", K(*this));
            } else {
              first_idx_ = (first_idx_ + 1) % BASE_TRACE_CNT;
            }
          }
          traces_[cur_idx_].reset();
        }
        if (OB_SUCC(traces_[cur_idx_].log_it(info, args...))) {
          break;
        }
      }
      (void)ret;
    }
  }
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    for (int64_t i = 0; i < BASE_TRACE_CNT; i++) {
      if (pos < buf_len) {
        const int64_t idx = (first_idx_ + i) % BASE_TRACE_CNT;
        const int64_t size = traces_[idx].to_string(buf + pos, buf_len - pos);
        pos = pos + size;
        if (idx == cur_idx_) {
          break;
        }
      } else {
        break;
      }
    }
    return pos;
  }
  void set_need_print(const bool &need_print) { need_print_ = need_print; }
private:
  static const int64_t BASE_TRACE_CNT = 4;
  static const int64_t BUFFER_SIZE = TRACE_SIZE / BASE_TRACE_CNT;
private:
  ObSimpleTraceBase<BUFFER_SIZE> traces_[BASE_TRACE_CNT];
  int64_t first_idx_;
  int64_t cur_idx_;
  bool need_print_;
};

} // end namespace common
} // end namespace oceanbase

#endif /* _OB_TRACE_EVENT_H */
