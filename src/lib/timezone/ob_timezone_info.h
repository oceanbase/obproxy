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

#ifndef OCEANBASE_LIB_TIMEZONE_INFO_
#define OCEANBASE_LIB_TIMEZONE_INFO_

#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/number/ob_number_v2.h"
#include "lib/alloc/alloc_struct.h"

namespace oceanbase
{
namespace common
{

class ObOTimestampData {
public:
  static const int32_t MIN_OFFSET_MINUTES = -15 * 60 - 59;
  static const int32_t MAX_OFFSET_MINUTES = 15 * 60 + 59;
  static const int32_t MAX_OFFSET_MINUTES_STRICT = 15 * 60;
  static const int32_t MIN_TAIL_NSEC = 0;
  static const int32_t MAX_TAIL_NSEC = 999;
  static const int32_t BASE_TAIL_NSEC = 1000;
  static const int32_t MAX_BIT_OF_TAIL_NSEC = ((1 << 10) - 1);
  static const int32_t MAX_BIT_OF_OFFSET_MIN = ((1 << 11) - 1);
  static const int32_t MAX_BIT_OF_TZ_ID = ((1 << 11) - 1);
  static const int32_t MAX_BIT_OF_TRAN_TYPE_ID = ((1 << 5) - 1);
  static const int32_t MIN_TZ_ID = 1;
  static const int32_t MAX_TZ_ID = MAX_BIT_OF_TZ_ID;
  static const int32_t MIN_TRAN_TYPE_ID = 0;
  static const int32_t MAX_TRAN_TYPE_ID = MAX_BIT_OF_TRAN_TYPE_ID;

  struct UnionTZCtx {
    void set_tail_nsec(const int32_t value)
    {
      tail_nsec_ = static_cast<uint16_t>(value & MAX_BIT_OF_TAIL_NSEC);
    }
    void set_tz_id(const int32_t value)
    {
      tz_id_ = static_cast<uint16_t>(value & MAX_BIT_OF_TZ_ID);
    }
    void set_tran_type_id(const int32_t value)
    {
      tran_type_id_ = static_cast<uint16_t>(value & MAX_BIT_OF_TRAN_TYPE_ID);
    }
    int32_t get_offset_min() const
    {
      return (is_neg_offset_ ? (0 - static_cast<int32_t>(offset_min_)) : static_cast<int32_t>(offset_min_));
    }
    void set_offset_min(const int32_t value)
    {
      int32_t tmp_value = value;
      if (value < 0) {
        tmp_value = 0 - value;
        is_neg_offset_ = 1;
      } else {
        is_neg_offset_ = 0;
      }
      offset_min_ = static_cast<int16_t>(tmp_value & MAX_BIT_OF_OFFSET_MIN);
    }

    union {
      uint32_t desc_;
      struct {
        union {
          uint16_t time_desc_;
          struct {
            uint16_t tail_nsec_ : 10;     // append nanosecond to the tailer, [0, 999]
            uint16_t version_ : 2;        // default 0
            uint16_t store_tz_id_ : 1;    // true mean store tz_id
            uint16_t is_null_ : 1;        // oracle null timestamp
            uint16_t time_reserved_ : 2;  // reserved
          };
        };
        union {
          uint16_t tz_desc_;
          struct {
            uint16_t is_neg_offset_ : 1;    // reserved
            uint16_t offset_min_ : 11;      // tz offset min
            uint16_t offset_reserved_ : 4;  // reserved
          };
          struct {
            uint16_t tz_id_ : 11;        // Time_zone_id of oceanbase.__all_time_zone_transition_type, [1, 2047]
            uint16_t tran_type_id_ : 5;  // Transition_type_id of oceanbase.__all_time_zone_transition_type, [0,31]
          };
        };
      };
    };
  } __attribute__((packed));

public:
  ObOTimestampData() : time_ctx_(), time_us_(0)
  {}
  ObOTimestampData(const int64_t time_us, const UnionTZCtx tz_ctx) : time_ctx_(tz_ctx), time_us_(time_us)
  {}
  ~ObOTimestampData()
  {}
  void reset()
  {
    memset(this, 0, sizeof(ObOTimestampData));
  }
  bool is_null_value() const
  {
    return time_ctx_.is_null_;
  }
  void set_null_value()
  {
    time_ctx_.is_null_ = 1;
  }
  static bool is_valid_offset_min(const int32_t offset_min)
  {
    return MIN_OFFSET_MINUTES <= offset_min && offset_min <= MAX_OFFSET_MINUTES;
  }
  static bool is_valid_offset_min_strict(const int32_t offset_min)
  {
    return MIN_OFFSET_MINUTES <= offset_min && offset_min <= MAX_OFFSET_MINUTES_STRICT;
  }
  static bool is_valid_tz_id(const int32_t tz_id)
  {
    return (MIN_TZ_ID <= tz_id && tz_id <= MAX_TZ_ID);
  }
  static bool is_valid_tran_type_id(const int32_t tran_type_id)
  {
    return (MIN_TRAN_TYPE_ID <= tran_type_id && tran_type_id <= MAX_TRAN_TYPE_ID);
  }
  inline int compare(const ObOTimestampData &other) const
  {
    int result = 0;
    if (time_us_ > other.time_us_) {
      result = 1;
    } else if (time_us_ < other.time_us_) {
      result = -1;
    } else if (time_ctx_.tail_nsec_ > other.time_ctx_.tail_nsec_) {
      result = 1;
    } else if (time_ctx_.tail_nsec_ < other.time_ctx_.tail_nsec_) {
      result = -1;
    } else {
      result = 0;
    }
    return result;
  }

  inline bool operator<(const ObOTimestampData &other) const
  {
    return compare(other) < 0;
  }

  inline bool operator<=(const ObOTimestampData &other) const
  {
    return compare(other) <= 0;
  }

  inline bool operator>(const ObOTimestampData &other) const
  {
    return compare(other) > 0;
  }

  inline bool operator>=(const ObOTimestampData &other) const
  {
    return compare(other) >= 0;
  }

  inline bool operator==(const ObOTimestampData &other) const
  {
    return compare(other) == 0;
  }

  inline bool operator!=(const ObOTimestampData &other) const
  {
    return compare(other) != 0;
  }

  DECLARE_TO_STRING;

public:
  UnionTZCtx time_ctx_;  // time ctx, such as version, tail_ns, tz_id...
  int64_t time_us_;      // full time with usec, same as timestamp
} __attribute__((packed));


struct ObTimeZoneName
{
  ObString  name_;
  int32_t   lower_idx_;   // index of ObTimeZoneTrans array.
  int32_t   upper_idx_;   // index of ObTimeZoneTrans array.
  int32_t   tz_id;        // tmp members, will be removed later.
};

struct ObTimeZoneTrans
{
  int64_t   trans_;
  int32_t   offset_;
  int32_t   tz_id;        // tmp members, will be removed later.
};

static const int32_t INVALID_TZ_OFF = INT32_MAX;

struct ObTZTransitionStruct {
  ObTZTransitionStruct() : offset_sec_(0), tran_type_id_(common::OB_INVALID_INDEX), is_dst_(false)
  {
    MEMSET(abbr_, 0, common::OB_MAX_TZ_ABBR_LEN);
  }
  ~ObTZTransitionStruct()
  {}
  int assign(const ObTZTransitionStruct &src)
  {
    int ret = OB_SUCCESS;
    if (OB_LIKELY(this != &src)) {
      MEMCPY(this, &src, sizeof(ObTZTransitionStruct));
    }
    return ret;
  }
  void reset()
  {
    offset_sec_ = 0;
    tran_type_id_ = common::OB_INVALID_INDEX;
    is_dst_ = false;
    MEMSET(abbr_, 0, common::OB_MAX_TZ_ABBR_LEN);
  }
  void set_tz_abbr(const common::ObString &abbr)
  {
    MEMSET(abbr_, 0, common::OB_MAX_TZ_ABBR_LEN);
    if (!abbr.empty()) {
      const int64_t min_len = std::min(static_cast<int64_t>(abbr.length()), common::OB_MAX_TZ_ABBR_LEN - 1);
      MEMCPY(abbr_, abbr.ptr(), min_len);
    }
  }
  bool operator==(const ObTZTransitionStruct &other) const
  {
    return (offset_sec_ == other.offset_sec_
            && tran_type_id_ == other.tran_type_id_
            && is_dst_ == other.is_dst_
            && 0 == MEMCMP(abbr_, other.abbr_, common::OB_MAX_TZ_ABBR_LEN));
  }
  bool operator!=(const ObTZTransitionStruct &other) const
  {
    return !(*this == other);
  }

  TO_STRING_KV(K_(offset_sec), K_(tran_type_id), K_(is_dst), "abbr", common::ObString(abbr_));

  int32_t offset_sec_;
  int32_t tran_type_id_;
  bool is_dst_;
  char abbr_[common::OB_MAX_TZ_ABBR_LEN];
};

class ObTZTransitionTypeInfo {
  OB_UNIS_VERSION(1);

public:
  ObTZTransitionTypeInfo() : lower_time_(common::OB_INVALID_TZ_TRAN_TIME), info_()
  {}
  virtual ~ObTZTransitionTypeInfo()
  {}
  int assign(const ObTZTransitionTypeInfo &src);
  void reset();

  void set_tz_abbr(const common::ObString &abbr)
  {
    info_.set_tz_abbr(abbr);
  }
  OB_INLINE bool is_dst() const
  {
    return info_.is_dst_;
  }
  bool operator==(const ObTZTransitionTypeInfo &other) const
  {
    return (lower_time_ == other.lower_time_ && info_ == other.info_);
  }
  bool operator!=(const ObTZTransitionTypeInfo &other) const
  {
    return !(*this == other);
  }
  virtual int get_offset_according_abbr(
      const common::ObString &tz_abbr_str, int32_t &offset_sec, int32_t &tran_type_id) const;
  VIRTUAL_TO_STRING_KV(K_(lower_time), K_(info));

private:
  DISALLOW_COPY_AND_ASSIGN(ObTZTransitionTypeInfo);

public:
  int64_t lower_time_;
  ObTZTransitionStruct info_;
};

class ObTZRevertTypeInfo : public ObTZTransitionTypeInfo {
  OB_UNIS_VERSION(1);

public:
  enum TypeInfoClass { NONE = 0, NORMAL, OVERLAP, GAP };

public:
  ObTZRevertTypeInfo() : ObTZTransitionTypeInfo(), type_class_(NONE), extra_info_()
  {}
  virtual ~ObTZRevertTypeInfo()
  {}
  void reset();
  int assign_normal(const ObTZTransitionTypeInfo &src);
  int assign_extra(const ObTZTransitionTypeInfo &src);
  int assign(const ObTZRevertTypeInfo &src);
  OB_INLINE bool is_normal() const
  {
    return NORMAL == type_class_;
  }
  OB_INLINE bool is_gap() const
  {
    return GAP == type_class_;
  }
  OB_INLINE bool is_overlap() const
  {
    return OVERLAP == type_class_;
  }
  int get_offset_according_abbr(const common::ObString &tz_abbr_str, int32_t &offset_sec, int32_t &tran_type_id) const;
  bool operator==(const ObTZRevertTypeInfo &other) const
  {
    return (type_class_ == other.type_class_
            && extra_info_ == other.extra_info_
            && lower_time_ == other.lower_time_
            && info_ == other.info_);
  }
  bool operator!=(const ObTZRevertTypeInfo &other) const
  {
    return !(*this == other);
  }

  VIRTUAL_TO_STRING_KV(K_(lower_time), K_(info), K_(type_class), K_(extra_info));

private:
  DISALLOW_COPY_AND_ASSIGN(ObTZRevertTypeInfo);

public:
  TypeInfoClass type_class_;
  ObTZTransitionStruct extra_info_;
};

class ObTZIDKey {
public:
  ObTZIDKey() : tz_id_(0)
  {}
  ObTZIDKey(const int64_t tz_id) : tz_id_(tz_id)
  {}
  uint64_t hash() const
  {
    return common::murmurhash(&tz_id_, sizeof(tz_id_), 0);
  };
  int compare(const ObTZIDKey& r)
  {
    int cmp = 0;
    if (tz_id_ < r.tz_id_) {
      cmp = -1;
    } else if (tz_id_ == r.tz_id_) {
      cmp = 0;
    } else {
      cmp = 1;
    }
    return cmp;
  }
  TO_STRING_KV(K_(tz_id));

public:
  int64_t tz_id_;
};

class ObTZNameKey {
public:
  ObTZNameKey(const common::ObString &tz_key_str);
  ObTZNameKey(const ObTZNameKey &key);
  ObTZNameKey()
  {
    MEMSET(tz_name_, 0, common::OB_MAX_TZ_NAME_LEN);
  }
  ~ObTZNameKey()
  {}
  void reset();
  int assign(const ObTZNameKey &key);
  void operator=(const ObTZNameKey &key);
  int compare(const ObTZNameKey &key) const
  {
    ObString self_str(strlen(tz_name_), tz_name_);
    return self_str.case_compare(key.tz_name_);
  }
  bool operator==(const ObTZNameKey &key) const
  {
    return 0 == compare(key);
  }

  uint64_t hash(uint64_t seed = 0) const;
  TO_STRING_KV("tz_name", common::ObString(common::OB_MAX_TZ_NAME_LEN, tz_name_));

private:
  char tz_name_[common::OB_MAX_TZ_NAME_LEN];
};


class ObTZInfoMap;

class ObTZMapWrap {
public:
  ObTZMapWrap() : tz_info_map_(NULL)
  {}
  ~ObTZMapWrap();
  const ObTZInfoMap* get_tz_map() const
  {
    return tz_info_map_;
  }
  void set_tz_map(const common::ObTZInfoMap *tz_info_map);

private:
  ObTZInfoMap *tz_info_map_;
};

class ObTimeZoneInfo
{
  OB_UNIS_VERSION(1);
public:
  ObTimeZoneInfo()
    : error_on_overlap_time_(false),
      tz_map_wrap_(),
      tz_id_(common::OB_INVALID_TZ_ID),
      offset_(0)
  {}
  int assign(const ObTimeZoneInfo &src);
  int set_timezone(const ObString &str);
  int32_t get_tz_id() const
  {
    return tz_id_;
  }
  void set_offset(int32_t offset)
  {
    offset_ = offset;
  }
  virtual int get_timezone_offset(int64_t value, int32_t &offset_sec) const;
  virtual int get_timezone_sub_offset(
      int64_t value, const ObString &tz_abbr_str, int32_t &offset_sec, int32_t &tz_id, int32_t &tran_type_id) const;
  virtual int get_timezone_offset(
      int64_t value, int32_t &offset_sec, common::ObString &tz_abbr_str, int32_t &tran_type_id) const;
  virtual int timezone_to_str(char *buf, const int64_t len, int64_t &pos) const;

  void set_error_on_overlap_time(bool is_error)
  {
    error_on_overlap_time_ = is_error;
  }
    bool is_error_on_overlap_time() const
  {
    return error_on_overlap_time_;
  }
  void set_tz_info_map(const ObTZInfoMap *tz_info_map)
  {
    tz_map_wrap_.set_tz_map(tz_info_map);
  }
  ObTZMapWrap& get_tz_map_wrap()
  {
    return tz_map_wrap_;
  }
  const ObTZInfoMap* get_tz_info_map() const
  {
    return tz_map_wrap_.get_tz_map();
  }

  void reset()
  {
    tz_id_ = common::OB_INVALID_TZ_ID;
    offset_ = 0;
    error_on_overlap_time_ = false;
    tz_map_wrap_.set_tz_map(NULL);
  }
  TO_STRING_KV(N_ID, tz_id_, N_OFFSET, offset_, N_ERROR_ON_OVERLAP_TIME, error_on_overlap_time_);

private:
  static ObTimeZoneName TIME_ZONE_NAMES[];
  static ObTimeZoneTrans TIME_ZONE_TRANS[];

protected:
  bool error_on_overlap_time_;
  
  // do not serialize it
  ObTZMapWrap tz_map_wrap_;

private:
  int32_t tz_id_;
  int32_t offset_;
};

typedef common::LinkHashNode<ObTZIDKey> ObTZIDHashNode;
typedef common::LinkHashNode<ObTZNameKey> ObTZNameHashNode;
typedef common::LinkHashValue<ObTZIDKey> ObTZIDHashValue;
typedef common::LinkHashValue<ObTZNameKey> ObTZNameHashValue;

class ObTimeZoneInfo;
class ObTimeZoneInfoPos : public ObTimeZoneInfo, public ObTZIDHashValue {
  OB_UNIS_VERSION(1);

public:
  ObTimeZoneInfoPos()
      : tz_id_(common::OB_INVALID_TZ_ID),
        default_type_(),
        tz_tran_types_(),
        tz_revt_types_(),
        curr_idx_(0)
  {
    MEMSET(tz_name_, 0, common::OB_MAX_TZ_NAME_LEN);
  }
  virtual ~ObTimeZoneInfoPos()
  {}
  int assign(const ObTimeZoneInfoPos &src);
  void reset();
  bool is_valid() const
  {
    return tz_id_ != common::OB_INVALID_TZ_ID;
  }
  int compare_upgrade(const ObTimeZoneInfoPos &other, bool &is_equal) const;

  virtual int get_timezone_offset(
      int64_t value, int32_t &offset_sec, common::ObString &tz_abbr_str, int32_t &tran_type_id) const;
  int get_timezone_offset(const int32_t tran_type_id, common::ObString &tz_abbr_str, int32_t &offset_sec) const;
  virtual int get_timezone_offset(int64_t value, int32_t &offset_sec) const;
  virtual int get_timezone_sub_offset(int64_t value, const common::ObString &tz_abbr_str, int32_t &offset_sec,
      int32_t &tz_id, int32_t &tran_type_id) const;

  inline void set_tz_id(int64_t tz_id)
  {
    tz_id_ = tz_id;
  }
  inline int64_t get_tz_id() const
  {
    return tz_id_;
  }
  int set_tz_name(const char *name, int64_t name_len);
  int get_tz_name(common::ObString &tz_name) const;
  common::ObString get_tz_name() const
  {
    return common::ObString(tz_name_);
  }
  int add_tran_type_info(const ObTZTransitionTypeInfo &type_info);
  int set_default_tran_type(const ObTZTransitionTypeInfo &tran_type);
  inline const ObTZTransitionTypeInfo &get_default_trans_type() const
  {
    return default_type_;
  }
  inline int32_t get_curr_idx() const
  {
    return curr_idx_;
  }
  inline int32_t get_next_idx() const
  {
    return curr_idx_ + 1;
  }
  inline void inc_curr_idx()
  {
    ++curr_idx_;
  }
  const common::ObSArray<ObTZTransitionTypeInfo, ObMalloc>& get_tz_tran_types() const
  {
    return tz_tran_types_[get_curr_idx() % 2];
  }
  const common::ObSArray<ObTZRevertTypeInfo, ObMalloc>& get_tz_revt_types() const
  {
    return tz_revt_types_[get_curr_idx() % 2];
  }
  const common::ObSArray<ObTZTransitionTypeInfo, ObMalloc>& get_next_tz_tran_types() const
  {
    return tz_tran_types_[get_next_idx() % 2];
  }
  const common::ObSArray<ObTZRevertTypeInfo, ObMalloc>& get_next_tz_revt_types() const
  {
    return tz_revt_types_[get_next_idx() % 2];
  }
  common::ObSArray<ObTZTransitionTypeInfo, ObMalloc>& get_next_tz_tran_types()
  {
    return tz_tran_types_[get_next_idx() % 2];
  }
  common::ObSArray<ObTZRevertTypeInfo, ObMalloc>& get_next_tz_revt_types()
  {
    return tz_revt_types_[get_next_idx() % 2];
  }
  int calc_revt_types();
  virtual int timezone_to_str(char *buf, const int64_t len, int64_t &pos) const;
  VIRTUAL_TO_STRING_KV("tz_name", common::ObString(common::OB_MAX_TZ_NAME_LEN, tz_name_), "tz_id", tz_id_,
      "default_transition_type", default_type_, "tz_transition_types", get_tz_tran_types(), "tz_revert_types",
      get_tz_revt_types(), K_(curr_idx), "next_tz_transition_types", get_next_tz_tran_types(), "next_tz_revert_types",
      get_next_tz_revt_types());

private:
  int find_time_range(int64_t value,
                      const common::ObIArray<ObTZTransitionTypeInfo> &tz_tran_types,
                      int64_t &type_idx) const;
  int find_offset_range(const int32_t tran_type_id,
                        const common::ObIArray<ObTZTransitionTypeInfo> &tz_tran_types,
                        int64_t &type_idx) const;
  int find_revt_time_range(int64_t value,
                           const common::ObIArray<ObTZRevertTypeInfo> &tz_revt_types,
                           int64_t &type_idx) const;

private:
  int64_t tz_id_;
  /*default_type_ is used for times smaller than first transition or if
    there are no transitions at all.*/
  ObTZTransitionTypeInfo default_type_;
  // used for utc time -> local time
  common::ObSArray<ObTZTransitionTypeInfo, ObMalloc> tz_tran_types_[2];
  // used for local time -> utc time
  common::ObSArray<ObTZRevertTypeInfo, ObMalloc> tz_revt_types_[2];
  uint32_t curr_idx_;
  char tz_name_[common::OB_MAX_TZ_NAME_LEN];
};

class ObTZNameIDInfo : public ObTZNameHashValue {
public:
  ObTZNameIDInfo() : tz_id_(common::OB_INVALID_TZ_ID)
  {
    MEMSET(tz_name_, 0, common::OB_MAX_TZ_NAME_LEN);
  }
  void set(const int64_t tz_id, const common::ObString &tz_name)
  {
    tz_id_ = tz_id;
    if (tz_name.empty()) {
      MEMSET(tz_name_, 0, common::OB_MAX_TZ_NAME_LEN);
    } else {
      const int64_t min_len = std::min(static_cast<int64_t>(tz_name.length()), common::OB_MAX_TZ_ABBR_LEN - 1);
      MEMCPY(tz_name_, tz_name.ptr(), min_len);
      tz_name_[min_len] = '\0';
    }
  }

  ObTZNameIDInfo(const int64_t tz_id, const common::ObString &tz_name) : tz_id_(tz_id)
  {
    set(tz_id, tz_name);
  }
  ~ObTZNameIDInfo()
  {}
  TO_STRING_KV(K_(tz_id), K_(tz_name));

public:
  int64_t tz_id_;
  char tz_name_[common::OB_MAX_TZ_NAME_LEN];
};

class ObTZIDPosAlloc {
public:
  ObTZIDPosAlloc()
  {}
  ~ObTZIDPosAlloc()
  {}
  ObTimeZoneInfoPos* alloc_value();
  void free_value(ObTimeZoneInfoPos *tz_info);

  ObTZIDHashNode* alloc_node(ObTimeZoneInfoPos *value);
  void free_node(ObTZIDHashNode *node);
};

class ObTZNameIDAlloc {
public:
  ObTZNameIDAlloc()
  {}
  ~ObTZNameIDAlloc()
  {}
  ObTZNameIDInfo* alloc_value();
  void free_value(ObTZNameIDInfo *info);

  ObTZNameHashNode* alloc_node(ObTZNameIDInfo *value);
  void free_node(ObTZNameHashNode *node);
};

typedef common::ObLinkHashMap<ObTZIDKey, ObTimeZoneInfoPos, ObTZIDPosAlloc> ObTZInfoIDPosMap;
typedef common::ObLinkHashMap<ObTZNameKey, ObTZNameIDInfo, ObTZNameIDAlloc> ObTZInfoNameIDMap;

class ObTZInfoMap {
public:
  ObTZInfoMap() : inited_(false), id_map_(), name_map_(), ref_count_(0)
  {}
  ~ObTZInfoMap()
  {}
  //int init(const lib::ObLabel& label);
  int reset();
  int print_tz_info_map();
  bool is_inited()
  {
    return inited_;
  }
  int get_tz_info_by_id(const int64_t tz_id, ObTimeZoneInfoPos *&tz_info_by_id);
  int get_tz_info_by_name(const common::ObString &tz_name, ObTimeZoneInfoPos *&tz_info_by_name);
  void free_tz_info_pos(ObTimeZoneInfoPos *&tz_info)
  {
    id_map_.revert(tz_info);
    tz_info = NULL;
  }
  void inc_ref_count()
  {
    ATOMIC_INC(&ref_count_);
  }
  void dec_ref_count()
  {
    ATOMIC_DEC(&ref_count_);
  }
  int64_t get_ref_count()
  {
    return ATOMIC_LOAD64(&ref_count_);
  }

public:
  bool inited_;
  ObTZInfoIDPosMap id_map_;
  ObTZInfoNameIDMap name_map_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTZInfoMap);
  int64_t ref_count_;
};

class ObTimeZoneInfoWrap {
  enum ObTZInfoClass { NONE = 0, POSITION = 1, OFFSET = 2 };
  OB_UNIS_VERSION(1);

public:
  ObTimeZoneInfoWrap()
      : tz_info_pos_(),
        tz_info_offset_(),
        tz_info_(NULL),
        class_(NONE),
        cur_version_(0),
        error_on_overlap_time_(false)
  {}
  ~ObTimeZoneInfoWrap()
  {}
  void reset();
  const ObTimeZoneInfo* get_time_zone_info() const
  {
    return tz_info_;
  }
  ObTimeZoneInfoPos& get_tz_info_pos()
  {
    return tz_info_pos_;
  }
  const ObTimeZoneInfo& get_tz_info_offset() const
  {
    return tz_info_offset_;
  }
  int64_t get_cur_version() const
  {
    return cur_version_;
  }
  void set_cur_version(const int64_t version)
  {
    cur_version_ = version;
  }
  ObTZInfoClass get_tz_info_class() const
  {
    return class_;
  }
  bool is_position_class() const
  {
    return POSITION == class_;
  }
  bool is_error_on_overlap_time() const
  {
    return error_on_overlap_time_;
  }
  int set_error_on_overlap_time(bool is_error);
  int init_time_zone(const ObString &str_val, const int64_t curr_version, ObTZInfoMap &tz_info_map);
  void set_tz_info_map(const ObTZInfoMap *tz_info_map);
  int deep_copy(const ObTimeZoneInfoWrap &tz_inf_wrap);
  void set_tz_info_offset(const int32_t offset)
  {
    tz_info_offset_.set_offset(offset);
    tz_info_ = &tz_info_offset_;
    tz_info_->set_error_on_overlap_time(error_on_overlap_time_);
    class_ = OFFSET;
  }
  void set_tz_info_position()
  {
    tz_info_ = &tz_info_pos_;
    tz_info_->set_error_on_overlap_time(error_on_overlap_time_);
    class_ = POSITION;
  }
  VIRTUAL_TO_STRING_KV(
      K_(cur_version), "class", class_, KP_(tz_info), K_(error_on_overlap_time), K_(tz_info_pos), K_(tz_info_offset));

private:
  common::ObTimeZoneInfoPos tz_info_pos_;
  common::ObTimeZoneInfo tz_info_offset_;
  common::ObTimeZoneInfo *tz_info_;
  ObTZInfoClass class_;
  int64_t cur_version_;
  bool error_on_overlap_time_;
};



} // end of common
} // end of oceanbase

#endif // OCEANBASE_LIB_TIMEZONE_INFO_
