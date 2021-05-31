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

#ifndef OCEANBASE_COMMON_OB_RANGE2_H_
#define OCEANBASE_COMMON_OB_RANGE2_H_

#include "lib/tbsys.h"
#include "lib/ob_define.h"
#include "common/ob_rowkey.h"
#include "common/ob_range.h"
#include "lib/utility/utility.h"
#include "lib/regex/ob_regex.h"


namespace oceanbase
{
namespace common
{

struct ObNewRange : public RowkeyInfoHolder
{

public:
  uint64_t table_id_;
  ObBorderFlag border_flag_;
  ObRowkey start_key_;
  ObRowkey end_key_;


  ObNewRange()
  {
    reset();
  }

  ~ObNewRange()
  {
    reset();
  }

  inline void reset()
  {
    table_id_ = OB_INVALID_ID;
    border_flag_.set_data(0);
    start_key_.assign(NULL, 0);
    end_key_.assign(NULL, 0);
  }

  inline const ObRowkey &get_start_key()
  {
    return start_key_;
  }
  inline const ObRowkey &get_start_key() const
  {
    return start_key_;
  }


  inline const ObRowkey &get_end_key()
  {
    return end_key_;
  }
  inline const ObRowkey &get_end_key() const
  {
    return end_key_;
  }

  inline const ObRowkeyInfo *fetch_rowkey_info(const ObNewRange &l, const ObNewRange &r) const
  {
    const ObRowkeyInfo *ri = NULL;
    if (NULL != l.rowkey_info_) {
      ri = l.rowkey_info_;
    } else if (NULL != r.rowkey_info_) {
      ri = r.rowkey_info_;
    }
    return ri;
  }

  int build_range(uint64_t table_id, ObRowkey rowkey)
  {
    int ret = OB_SUCCESS;
    if (OB_INVALID_ID == table_id || !rowkey.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid arguments.", K(table_id), K(rowkey), K(ret));
    } else {
      table_id_ = table_id;
      start_key_ = rowkey;
      end_key_ = rowkey;
      border_flag_.set_inclusive_start();
      border_flag_.set_inclusive_end();
    }
    return ret;
  }

  // new compare func for tablet.range and scan_param.range
  inline int compare_with_endkey2(const ObNewRange &r) const
  {
    int cmp = 0;
    ObRowkeyLess less(fetch_rowkey_info(*this, r));
    if (end_key_.is_max_row()) {
      if (!r.end_key_.is_max_row()) {
        cmp = 1;
      }
    } else if (r.end_key_.is_max_row()) {
      cmp = -1;
    } else {
      cmp = less.compare(end_key_, r.end_key_);
      if (0 == cmp) {
        if (border_flag_.inclusive_end() && !r.border_flag_.inclusive_end()) {
          cmp = 1;
        } else if (!border_flag_.inclusive_end() && r.border_flag_.inclusive_end()) {
          cmp = -1;
        }
      }
    }
    return cmp;
  }

  inline int compare_with_startkey2(const ObNewRange &r) const
  {
    int cmp = 0;
    ObRowkeyLess less(fetch_rowkey_info(*this, r));
    if (start_key_.is_min_row()) {
      if (!r.start_key_.is_min_row()) {
        cmp = -1;
      }
    } else if (r.start_key_.is_min_row()) {
      cmp = 1;
    } else {
      cmp = less.compare(start_key_, r.start_key_);
      if (0 == cmp) {
        if (border_flag_.inclusive_start() && !r.border_flag_.inclusive_start()) {
          cmp = -1;
        } else if (!border_flag_.inclusive_start() && r.border_flag_.inclusive_start()) {
          cmp = 1;
        }
      }
    }
    return cmp;
  }

  // TODO: this func cost mush, need modify this logic
  inline bool is_valid() const { return !empty(); }

  inline bool empty() const
  {
    bool ret = false;
    if (start_key_.is_min_row() || end_key_.is_max_row()) {
      ret = false;
    } else {
      ret  = end_key_ < start_key_
             || ((end_key_ == start_key_)
                 && !((border_flag_.inclusive_end())
                      && border_flag_.inclusive_start()));
    }
    return ret;
  }

  // from MIN to MAX, complete set.
  inline void set_whole_range()
  {
    start_key_.set_min_row();
    end_key_.set_max_row();
    border_flag_.unset_inclusive_start();
    border_flag_.unset_inclusive_end();
  }

  // from MIN to MAX, complete set.
  inline bool is_whole_range() const
  {
    return (start_key_.is_min_row()) && (end_key_.is_max_row());
  }

  /*
  inline bool is_close_range() const
  {
    if (start_key_.length() <= 0 || end_key_.length() <= 0) {
      COMMON_LOG(ERROR, "invalid range keys", K_(start_key),
                K_(end_key));
    }
    return !((start_key_.length() > 0 && start_key_.ptr()[0].is_min_value()) ||
             (end_key_.length() > 0 && end_key_.ptr()[0].is_max_value()));
  }
  */

  inline bool is_left_open_right_closed() const
  {
    return (!border_flag_.inclusive_start() && border_flag_.inclusive_end()) || end_key_.is_max_row();
  }

  // return true if the range is a single key value(but not min or max); false otherwise
  inline bool is_single_rowkey() const
  {
    int ret = false;
    if (start_key_.is_min_row() || start_key_.is_max_row()
        || end_key_.is_min_row() || end_key_.is_max_row()) {
      ret = false;
    } else if (start_key_ == end_key_ && border_flag_.inclusive_start()
               && border_flag_.inclusive_end()) {
      ret = true;
    }
    return ret;
  }

  inline bool equal(const ObNewRange &r) const
  {
    return equal2(r);
  }

  inline bool equal2(const ObNewRange &r) const
  {
    return (table_id_ == r.table_id_) && (compare_with_startkey2(r) == 0) &&
           (compare_with_endkey2(r) == 0);
  }

  inline bool operator == (const ObNewRange &other) const
  {
    return equal(other);
  };

  int64_t to_string(char *buffer, const int64_t length) const;
  int64_t to_simple_string(char *buffer, const int64_t length) const;
  int64_t to_plain_string(char *buffer, const int64_t length) const;
  uint64_t hash() const;


  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size(void) const;

  template <typename Allocator>
  int deserialize(Allocator &allocator, const char *buf, const int64_t data_len, int64_t &pos);
};

template <typename Allocator>
int ObNewRange::deserialize(Allocator &allocator, const char *buf, const int64_t data_len,
                            int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObObj array[OB_MAX_ROWKEY_COLUMN_NUMBER * 2];
  ObNewRange copy_range;
  copy_range.start_key_.assign(array, OB_MAX_ROWKEY_COLUMN_NUMBER);
  copy_range.end_key_.assign(array + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);
  if (OB_FAIL(copy_range.deserialize(buf, data_len, pos))) {
    COMMON_LOG(WARN, "deserialize range to shallow copy object failed.",
               KP(buf), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(deep_copy_range(allocator, copy_range, *this))) {
    COMMON_LOG(WARN, "deep_copy_range failed.",
               KP(buf), K(data_len), K(pos), K(copy_range), K(ret));
  }

  return ret;
}

template <typename Allocator>
inline int deep_copy_range(Allocator &allocator, const ObNewRange &src, ObNewRange &dst)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(src.start_key_.deep_copy(dst.start_key_, allocator))) {
    COMMON_LOG(WARN, "deep copy start key failed.", K(src.start_key_), K(ret));
  } else if (OB_FAIL(src.end_key_.deep_copy(dst.end_key_, allocator))) {
    COMMON_LOG(WARN, "deep copy end key failed.", K(src.end_key_), K(ret));
  } else {
    dst.table_id_ = src.table_id_;
    dst.border_flag_ = src.border_flag_;
  }
  return ret;
}

template <typename Allocator>
inline int deep_copy_range(Allocator &allocator, const ObNewRange &src, ObNewRange *&dst)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  if (NULL == (ptr = allocator.alloc(sizeof(ObNewRange)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "allocate new range failed", K(ret));
  } else {
    dst = new(ptr) ObNewRange();
    if (OB_FAIL(src.start_key_.deep_copy(dst->start_key_, allocator))) {
      COMMON_LOG(WARN, "deep copy start key failed.", K(src.start_key_), K(ret));
    } else if (OB_FAIL(src.end_key_.deep_copy(dst->end_key_, allocator))) {
      COMMON_LOG(WARN, "deep copy end key failed.", K(src.end_key_), K(ret));
    } else {
      dst->table_id_ = src.table_id_;
      dst->border_flag_ = src.border_flag_;
    }
    if (OB_FAIL(ret) && NULL != ptr) {
      allocator.free(ptr);
      ptr = NULL;
    }
  }

  return ret;
}
} // end namespace common
} // end namespace oceanbase

#endif //OCEANBASE_COMMON_OB_RANGE_H_
