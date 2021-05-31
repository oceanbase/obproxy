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

#ifndef OCEANBASE_COMMON_OB_RANGE_H_
#define OCEANBASE_COMMON_OB_RANGE_H_

#include "lib/tbsys.h"
#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "common/ob_string_buf.h"

namespace oceanbase
{
namespace common
{
class ObBorderFlag
{
public:
  static const int8_t INCLUSIVE_START = 0x1;
  static const int8_t INCLUSIVE_END = 0x2;
  static const int8_t MIN_VALUE = 0x4;
  static const int8_t MAX_VALUE = 0x8;

public:
  ObBorderFlag() : data_(0) {}
  virtual ~ObBorderFlag() {}

  inline void set_inclusive_start() { data_ |= INCLUSIVE_START; }

  inline void unset_inclusive_start() { data_ &= (~INCLUSIVE_START); }

  inline bool inclusive_start() const { return (data_ & INCLUSIVE_START) == INCLUSIVE_START; }

  inline void set_inclusive_end() { data_ |= INCLUSIVE_END; }

  inline void unset_inclusive_end() { data_ &= (~INCLUSIVE_END); }

  inline bool inclusive_end() const { return (data_ & INCLUSIVE_END) == INCLUSIVE_END; }

  inline void set_min_value() { data_ |= MIN_VALUE; }
  inline void unset_min_value() { data_ &= (~MIN_VALUE); }
  inline bool is_min_value() const { return (data_ & MIN_VALUE) == MIN_VALUE; }

  inline void set_max_value() { data_ |= MAX_VALUE; }
  inline void unset_max_value() { data_ &= (~MAX_VALUE); }
  inline bool is_max_value() const { return (data_ & MAX_VALUE) == MAX_VALUE; }

  inline void set_data(const int8_t data) { data_ = data; }
  inline int8_t get_data() const { return data_; }

  TO_STRING_KV(N_FLAG, data_);
private:
  int8_t data_;
};

struct ObVersion
{
  const static int16_t START_MINOR_VERSION = 1;
  const static int16_t MAX_MINOR_VERSION = INT16_MAX;
  const static int32_t START_MAJOR_VERSION = 2;
  const static int32_t MAX_MAJOR_VERSION = INT32_MAX;
  const static int32_t DEFAULT_MAJOR_VERSION = 1;

  ObVersion() : version_(0) {}
  explicit ObVersion(int64_t version) : version_(version) {}
  ObVersion(const int64_t major, const int64_t minor)
      : major_(static_cast<int32_t>(major)),
        minor_(static_cast<int16_t>(minor)),
        is_final_minor_(0)
  {
  }

  union
  {
    int64_t version_;
    struct
    {
      int32_t major_           : 32;
      int16_t minor_           : 16;
      int16_t is_final_minor_  : 16;
    };
  };
  static ObVersion MIN_VERSION;
  static ObVersion MAX_VERSION;

  bool is_valid() const
  {
    return version_ > 0;
  }

  void reset()
  {
    major_ = 0;
    minor_ = 0;
    is_final_minor_ = 0;
  }

  int64_t operator=(int64_t version)
  {
    version_ = version;
    return version_;
  }

  operator int64_t() const
  {
    return version_;
  }

  static int64_t get_version(int64_t major, int64_t minor, bool is_final_minor)
  {
    ObVersion v;
    v.major_          = static_cast<int32_t>(major);
    v.minor_          = static_cast<int16_t>(minor);
    v.is_final_minor_ = is_final_minor ? 1 : 0;
    return v.version_;
  }

  static int64_t get_major(int64_t version)
  {
    ObVersion v;
    v.version_ = version;
    return v.major_;
  }

  static int64_t get_minor(int64_t version)
  {
    ObVersion v;
    v.version_ = version;
    return v.minor_;
  }

  static bool is_final_minor(int64_t version)
  {
    ObVersion v;
    v.version_ = version;
    return v.is_final_minor_ != 0;
  }

  static ObVersion get_version_with_max_minor(const int64_t version)
  {
    ObVersion v;
    v.version_ = version;
    v.minor_ = INT16_MAX;
    return v;
  }

  static ObVersion get_version_with_min_minor(const int64_t version)
  {
    ObVersion v;
    v.version_ = version;
    v.minor_ = 0;
    return v;
  }

  static int compare(int64_t l, int64_t r)
  {
    int ret = 0;
    ObVersion lv(l);
    ObVersion rv(r);

    //ignore is_final_minor
    if ((lv.major_ == rv.major_) && (lv.minor_ == rv.minor_)) {
      ret = 0;
    } else if ((lv.major_ < rv.major_) ||
               ((lv.major_ == rv.major_) && lv.minor_ < rv.minor_)) {
      ret = -1;
    } else {
      ret = 1;
    }
    return ret;
  }

  inline int compare(const ObVersion &rhs) const
  {
    int ret = 0;

    //ignore is_final_minor
    if ((major_ == rhs.major_) && (minor_ == rhs.minor_)) {
      ret = 0;
    } else if ((major_ < rhs.major_) ||
               ((major_ == rhs.major_) && minor_ < rhs.minor_)) {
      ret = -1;
    } else {
      ret = 1;
    }

    return ret;
  }

  inline bool operator<(const ObVersion &rhs) const
  {
    return compare(rhs) < 0;
  }

  inline bool operator<=(const ObVersion &rhs) const
  {
    return compare(rhs) <= 0;
  }

  inline bool operator>(const ObVersion &rhs) const
  {
    return compare(rhs) > 0;
  }

  inline bool operator>=(const ObVersion &rhs) const
  {
    return compare(rhs) >= 0;
  }

  inline bool operator==(const ObVersion &rhs) const
  {
    return compare(rhs) == 0;
  }

  inline bool operator!=(const ObVersion &rhs) const
  {
    return compare(rhs) != 0;
  }

  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "\"%d-%hd-%hd\"",
                    major_, minor_, is_final_minor_);
    return pos;
  }

  int version_to_string(char *buf, const int64_t buf_len) const
  {
    int ret = OB_SUCCESS;
    if (NULL != buf && buf_len > 0) {
      if (0 > snprintf(buf, buf_len, "%d-%d-%d",
                       major_, minor_, is_final_minor_)) {
        ret = OB_ERR_UNEXPECTED;
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
    }
    return ret;
  }
  int fixed_length_encode(char *buf, const int64_t buf_len, int64_t &pos) const;
  int fixed_length_decode(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_fixed_length_encoded_size() const;
  TO_YSON_KV(Y_(version));
  OB_UNIS_VERSION(1);
};

class ObVersionProvider
{
public:
  virtual ~ObVersionProvider() {}
  virtual const ObVersion get_frozen_version() const = 0;
  virtual const ObVersion get_merged_version() const = 0;
};

struct ObVersionRange
{
  ObBorderFlag border_flag_;
  ObVersion start_version_;
  ObVersion end_version_;

  inline void set_whole_range()
  {
    memset(this, 0, sizeof(ObVersionRange));
    border_flag_.set_min_value();
    border_flag_.set_max_value();
  }

  // from MIN to MAX, complete set.
  inline bool is_whole_range() const
  {
    return (border_flag_.is_min_value()) && (border_flag_.is_max_value());
  }

  inline bool is_inclusive_end() const
  {
    return !border_flag_.is_max_value() && border_flag_.inclusive_end();
  }

  inline int64_t get_query_version() const
  {
    int64_t query_version = 0;

    /**
     * query_version = 0 means read the data of serving version
     * query_version > 0 means read the data of specified version
     * query_version = -1 means invalid version
     * query_version = -2 means read the newest version
     */
    if (border_flag_.is_max_value()) {
      query_version = OB_NEWEST_DATA_VERSION;
    } else if (end_version_.major_ > 0) {
      if (border_flag_.inclusive_end()) {
        query_version = end_version_.major_;
      } else {
        if (end_version_.major_ > start_version_.major_ + 1) {
          query_version = end_version_.major_ - 1;
        } else if (end_version_.major_ == start_version_.major_ + 1 && border_flag_.inclusive_start()) {
          query_version = start_version_.major_;
        } else {
          query_version = -1;
        }
      }
    }

    return query_version;
  }

  inline bool empty() const
  {
    bool ret = false;
    if (border_flag_.is_min_value() || border_flag_.is_max_value()) {
      ret = false;
    } else {
      ret  = end_version_ < start_version_
             || ((end_version_ == start_version_)
                 && !((border_flag_.inclusive_end())
                      && border_flag_.inclusive_start()));
    }
    return ret;
  }

  inline bool intersect(const ObVersionRange &r) const
  {
    bool ret = false;

    if (empty() || r.empty()) {
      ret = false;
    } else if (is_whole_range() || r.is_whole_range()) {
      ret = true;
    } else {
      ObVersion lver, rver;
      int8_t include_lborder = 0;
      int8_t include_rborder = 0;
      bool min_value = false;
      int cmp = end_version_.compare(r.end_version_);
      if (cmp < 0) {
        lver = end_version_;
        rver = r.start_version_;
        include_lborder = (border_flag_.inclusive_end());
        include_rborder = (r.border_flag_.inclusive_start());
        min_value = (r.border_flag_.is_min_value());
      } else if (cmp > 0) {
        lver = r.end_version_;
        rver = start_version_;
        include_lborder = (r.border_flag_.inclusive_end());
        include_rborder = (border_flag_.inclusive_start());
        min_value = (border_flag_.is_min_value());
      } else {
        ret = true;
      }

      if (cmp != 0) {
        if (min_value) {
          ret = true;
        } else if (lver < rver) {
          ret = false;
        } else if (lver > rver) {
          ret = true;
        } else {
          ret = (include_lborder != 0 && include_rborder != 0);
        }
      }
    }

    return ret;
  }

  int64_t to_string(char *buf, int64_t buf_len) const
  {
    int64_t pos = 0;
    const char *lb = NULL;
    const char *rb = NULL;
    if (buf != NULL && buf_len > 0) {
      if (border_flag_.is_min_value()) {
        lb = "(MIN";
      } else if (border_flag_.inclusive_start()) {
        lb = "[";
      } else {
        lb = "(";
      }

      if (border_flag_.is_max_value()) {
        rb = "MAX)";
      } else if (border_flag_.inclusive_end()) {
        rb = "]";
      } else {
        rb = ")";
      }

      if (is_whole_range()) {
        databuff_printf(buf, buf_len, pos, "\"%s,%s\"", lb, rb);
      } else if (border_flag_.is_min_value()) {
        databuff_printf(buf, buf_len, pos, "\"%s,%d-%hd-%hd%s\"", lb,
                        end_version_.major_, end_version_.minor_, end_version_.is_final_minor_, rb);
      } else if (border_flag_.is_max_value()) {
        databuff_printf(buf, buf_len, pos, "\"%s%d-%hd-%hd, %s\"", lb,
                        start_version_.major_, start_version_.minor_, start_version_.is_final_minor_, rb);
      } else {
        databuff_printf(buf, buf_len, pos, "\"%s %d-%hd-%hd,%d-%hd-%hd %s\"", lb,
                        start_version_.major_, start_version_.minor_, start_version_.is_final_minor_,
                        end_version_.major_, end_version_.minor_, end_version_.is_final_minor_, rb);
      }
    }
    return pos;
  }
};
} // end namespace common
} // end namespace oceanbase

#endif //OCEANBASE_COMMON_OB_RANGE_H_
