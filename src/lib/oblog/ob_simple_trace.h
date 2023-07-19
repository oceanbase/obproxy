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

template<int64_t BASE_TRACE_SIZE>
class ObSimpleTraceBase
{
public:
  class Item
  {
  public:
    const char *key;
    int64_t value;
    int flag;
  };
  enum flag
  {
    ST_INFO,
    ST_INT_TYPE,
    ST_STRING_TYPE
  };
public:
  ObSimpleTraceBase() { reset(); }
  ~ObSimpleTraceBase() { }
  void reset()
  {
    buf_pos_ = 0;
    item_idx_ = 0;
    log_count_ = 0;
  }
  template <typename ... Args>
  int log_it(const char *info, Args const & ... args)
  {
    int ret = common::OB_SUCCESS;
    const int64_t saved_buf_pos = buf_pos_;
    const int64_t saved_item_idx = item_idx_;
    Item *item = NULL;
    if (NULL == (item = get_item())) {
      ret = common::OB_SIZE_OVERFLOW;
    } else {
      item->key = info;
      item->value = 0;
      item->flag = ST_INFO;
      ret = fill_kv(args...);
    }
    if (OB_FAIL(ret)) {
      buf_pos_ = saved_buf_pos;
      item_idx_ = saved_item_idx;
    } else {
      log_count_++;
    }
    return ret;
  }
  int64_t get_log_count() const { return log_count_; }
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int ret = common::OB_SUCCESS;
    int64_t pos = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < item_idx_; i++) {
      const Item *item = &(items_[i]);
      if (ST_INFO == item->flag) {
        if (i > 0) {
          ret = databuff_printf(buf, buf_len, pos, "%s", " | ");
        }
        if (OB_SUCC(ret)) {
          ret = databuff_printf(buf, buf_len, pos, "%s", item->key);
        }
      } else if (ST_INT_TYPE == item->flag) {
        ret = databuff_printf(buf, buf_len, pos, ", %s:%ld", item->key, item->value);
      } else if (ST_STRING_TYPE == item->flag) {
        ret = databuff_printf(buf, buf_len, pos, ", %s:%s", item->key, (char *)(item->value));
      } else {
        // do nothing
      }
    }
    if (OB_SUCC(ret)) {
      (void)databuff_printf(buf, buf_len, pos, "%s", " | ");
    }
    return pos;
  }
private:
  int fill_kv()
  {
    return common::OB_SUCCESS;
  }

  template <typename Value>
  int fill_kv(const char *key, const Value &value)
  {
    int ret = common::OB_SUCCESS;
    Item *item = NULL;
    if (NULL == (item = get_item())) {
      ret = common::OB_SIZE_OVERFLOW;
    } else {
      int64_t out = 0;
      int flag = 0;
      if (OB_SUCC(transform(buf_, BUFFER_SIZE, buf_pos_, value, out, flag))) {
        item->key = key;
        item->value = out;
        item->flag = flag;
      }
    }
    return ret;
  }

  template <typename Value, typename ... Args>
  int fill_kv(const char *key, const Value &value, Args const & ... args)
  {
    int ret = OB_SUCCESS;
    ret = fill_kv(key, value);
    if (OB_SUCC(ret)) {
      ret = fill_kv(args...);
    }
    return ret;
  }
private:
  template<typename T>
  int transform(char *buf, const int64_t buf_len, int64_t &pos,
      const T &value, int64_t &out, int &flag)
  {
    int ret = common::OB_SUCCESS;
    const int64_t saved_pos = pos;
    const int64_t size = value.to_string(buf + pos, buf_len - pos);
    pos = pos + size;
    if (pos >= buf_len - 1) {
      ret = amend(buf, buf_len - 1, pos, size);
    }
    if (OB_FAIL(ret)) {
      pos = saved_pos;
    } else {
      buf[pos++] = '\0';
      out = (int64_t)(buf + saved_pos);
      flag = ST_STRING_TYPE;
    }
    return ret;
  }
  int transform(char *buf, const int64_t buf_len, int64_t &pos,
      const int64_t &value, int64_t &out, int &flag)
  {
    UNUSED(buf);
    UNUSED(buf_len);
    UNUSED(pos);
    out = value;
    flag = ST_INT_TYPE;
    return common::OB_SUCCESS;
  }
  int transform(char *buf, const int64_t buf_len, int64_t &pos,
      const bool &value, int64_t &out, int &flag)
  {
    UNUSED(buf);
    UNUSED(buf_len);
    UNUSED(pos);
    out = value;
    flag = ST_INT_TYPE;
    return common::OB_SUCCESS;
  }
  int amend(char *buf, const int64_t buf_len, int64_t &pos, const int64_t size)
  {
    int ret = common::OB_SUCCESS;
    static const char ellipsis[] = "...";
    if (size >= BUFFER_SIZE / 2) {
      pos = pos - sizeof(ellipsis) - 1;
      ret = databuff_printf(buf, buf_len, pos, "%s", ellipsis);
    } else {
      ret = common::OB_SIZE_OVERFLOW;
    }
    return ret;
  }
  Item *get_item()
  {
    Item *item = NULL;
    if (item_idx_ < ITEM_COUNT) {
      item = &(items_[item_idx_]);
      item_idx_++;
    }
    return item;
  }
private:
  static const int64_t BUFFER_SIZE = BASE_TRACE_SIZE / 4;
  static const int64_t ITEM_COUNT = (BASE_TRACE_SIZE - BUFFER_SIZE) / sizeof(Item);
private:
  char buf_[BUFFER_SIZE];
  Item items_[ITEM_COUNT];
  int64_t buf_pos_;
  int64_t item_idx_;
  int64_t log_count_;
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
    dropped_ = 0;
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
            dropped_ = dropped_ + traces_[first_idx_].get_log_count();
            first_idx_ = (first_idx_ + 1) % BASE_TRACE_CNT;
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
    buf[pos] = '\0';
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
    if (pos < buf_len) {
      (void)databuff_printf(buf, buf_len, pos, "dropped=%ld", dropped_);
    }
    return pos;
  }

  void set_need_print(const bool &need_print) { need_print_ = need_print; }

private:
  static const int64_t BASE_TRACE_CNT = 4;
  static const int64_t BASE_TRACE_SIZE = TRACE_SIZE / BASE_TRACE_CNT;
private:
  ObSimpleTraceBase<BASE_TRACE_SIZE> traces_[BASE_TRACE_CNT];
  int64_t first_idx_;
  int64_t cur_idx_;
  int64_t dropped_;
  bool need_print_;
};

} // end namespace common
} // end namespace oceanbase

#endif /* _OB_TRACE_EVENT_H */
