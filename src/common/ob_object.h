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

#ifndef OCEANBASE_COMMON_OB_OBJECT_H_
#define OCEANBASE_COMMON_OB_OBJECT_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/tbsys.h"
#include "common/ob_action_flag.h"
#include "common/ob_obj_type.h"
#include "common/ob_accuracy.h"
#include "lib/number/ob_number_v2.h"
#include "lib/charset/ob_charset.h"
#include "lib/timezone/ob_timezone_info.h"
#include "lib/hash/ob_hashutils.h"
#include "lib/ob_proxy_worker.h"
#include "lib/rowid/ob_urowid.h"

namespace oceanbase
{
namespace tests
{
namespace common
{
class ObjTest;
}
}
namespace common
{
struct ObObjMeta
{
public:
  ObObjMeta()
      :type_(ObNullType),
       cs_level_(CS_LEVEL_INVALID),
       cs_type_(CS_TYPE_INVALID),
       scale_(-1)
  {}

  OB_INLINE bool operator ==(const ObObjMeta &other) const
  {
    return (type_ == other.type_ && cs_level_ == other.cs_level_&& cs_type_ == other.cs_type_);
  }
  OB_INLINE bool operator !=(const ObObjMeta &other) const { return !this->operator ==(other); }
  // this method is inefficient, you'd better use set_tinyint() etc. instead
  OB_INLINE void set_type(const ObObjType &type)
  {
    type_ = static_cast<uint8_t>(type);
    if (ObNullType == type_) {
      set_collation_level(CS_LEVEL_IGNORABLE);
      set_collation_type(CS_TYPE_BINARY);
    } else if (ObUnknownType == type_
               || ObExtendType == type_) {
      set_collation_level(CS_LEVEL_INVALID);
      set_collation_type(CS_TYPE_INVALID);
    } else if (ObHexStringType == type_) {
      set_collation_type(CS_TYPE_BINARY);
    } else if (!ob_is_string_type(static_cast<ObObjType>(type_))) {
      set_collation_level(CS_LEVEL_NUMERIC);
      set_collation_type(CS_TYPE_BINARY);
    }
  }
  // in greatest case need manually set numeric collation,
  // e.g greatest(2, 'x') => type=varchar,collation=binary,cmp_collation=utf8
  OB_INLINE void set_numeric_collation() {
    set_collation_level(CS_LEVEL_NUMERIC);
    set_collation_type(CS_TYPE_BINARY);
  }
  OB_INLINE void set_meta(const ObObjMeta &meta)
  {
    type_ = meta.type_;
    cs_level_ = meta.cs_level_;
    cs_type_ = meta.cs_type_;
    scale_ = meta.scale_;
  }
  OB_INLINE void reset()
  {
    type_ = ObNullType;
    cs_level_ = CS_LEVEL_INVALID;
    cs_type_ = CS_TYPE_INVALID;
    scale_ = -1;
  }

  OB_INLINE ObObjType get_type() const { return static_cast<ObObjType>(type_); }
  OB_INLINE ObObjTypeClass get_type_class() const { return ob_obj_type_class(get_type()); }

  OB_INLINE void set_null() { type_ = static_cast<uint8_t>(ObNullType); set_collation_level(CS_LEVEL_IGNORABLE); set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_tinyint() { type_ = static_cast<uint8_t>(ObTinyIntType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_smallint() { type_ = static_cast<uint8_t>(ObSmallIntType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_mediumint() { type_ = static_cast<uint8_t>(ObMediumIntType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_int32() { type_ = static_cast<uint8_t>(ObInt32Type); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_int() { type_ = static_cast<uint8_t>(ObIntType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_utinyint() { type_ = static_cast<uint8_t>(ObUTinyIntType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_usmallint() { type_ = static_cast<uint8_t>(ObUSmallIntType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_umediumint() { type_ = static_cast<uint8_t>(ObUMediumIntType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_uint32() { type_ = static_cast<uint8_t>(ObUInt32Type); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_uint64() { type_ = static_cast<uint8_t>(ObUInt64Type); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_float() { type_ = static_cast<uint8_t>(ObFloatType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_double() { type_ = static_cast<uint8_t>(ObDoubleType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_ufloat() { type_ = static_cast<uint8_t>(ObUFloatType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_udouble() { type_ = static_cast<uint8_t>(ObUDoubleType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_number() { type_ = static_cast<uint8_t>(ObNumberType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_unumber() { type_ = static_cast<uint8_t>(ObUNumberType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_number_float() { type_ = static_cast<uint8_t>(ObNumberFloatType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_datetime() { type_ = static_cast<uint8_t>(ObDateTimeType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_timestamp() { type_ = static_cast<uint8_t>(ObTimestampType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_year() { type_ = static_cast<uint8_t>(ObYearType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_date() { type_ = static_cast<uint8_t>(ObDateType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_time() { type_ = static_cast<uint8_t>(ObTimeType); set_collation_level(CS_LEVEL_NUMERIC);set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_varchar() { type_ = static_cast<uint8_t>(ObVarcharType); }
  OB_INLINE void set_char() { type_ = static_cast<uint8_t>(ObCharType); }
  OB_INLINE void set_varbinary() { type_ = static_cast<uint8_t>(ObVarcharType); set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_binary() { type_ = static_cast<uint8_t>(ObCharType); set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_hex_string() { type_ = static_cast<uint8_t>(ObHexStringType); set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_raw() { type_ = static_cast<uint8_t>(ObRawType); set_collation_type(CS_TYPE_BINARY); }
  OB_INLINE void set_ext() { type_ = static_cast<uint8_t>(ObExtendType); set_collation_level(CS_LEVEL_INVALID); set_collation_type(CS_TYPE_INVALID);}
  OB_INLINE void set_unknown() { type_ = static_cast<uint8_t>(ObUnknownType); set_collation_level(CS_LEVEL_INVALID); set_collation_type(CS_TYPE_INVALID); }
  OB_INLINE void set_nvarchar2() { type_ = static_cast<uint8_t>(ObNVarchar2Type); }
  OB_INLINE void set_nchar() { type_ = static_cast<uint8_t>(ObNCharType); }
  OB_INLINE void set_urowid()
  {
    type_ = static_cast<uint8_t>(ObURowIDType);
    set_collation_level(CS_LEVEL_INVALID);
    set_collation_type(CS_TYPE_BINARY);
  }
  OB_INLINE void set_otimestamp_type(const ObObjType type)
  {
    type_ = static_cast<uint8_t>(type);
    set_collation_level(CS_LEVEL_NUMERIC);
    set_collation_type(CS_TYPE_BINARY);
  }

  OB_INLINE bool is_valid() const { return ob_is_valid_obj_type(static_cast<ObObjType>(type_)); }
  OB_INLINE bool is_invalid() const { return !ob_is_valid_obj_type(static_cast<ObObjType>(type_)); }

  OB_INLINE bool is_null() const { return type_ == static_cast<uint8_t>(ObNullType); }
  OB_INLINE bool is_tinyint() const { return type_ == static_cast<uint8_t>(ObTinyIntType); }
  OB_INLINE bool is_smallint() const { return type_ == static_cast<uint8_t>(ObSmallIntType); }
  OB_INLINE bool is_mediumint() const { return type_ == static_cast<uint8_t>(ObMediumIntType); }
  OB_INLINE bool is_int32() const { return type_ == static_cast<uint8_t>(ObInt32Type); }
  OB_INLINE bool is_int() const { return type_ == static_cast<uint8_t>(ObIntType); }
  OB_INLINE bool is_utinyint() const { return type_ == static_cast<uint8_t>(ObUTinyIntType); }
  OB_INLINE bool is_usmallint() const { return type_ == static_cast<uint8_t>(ObUSmallIntType); }
  OB_INLINE bool is_umediumint() const { return type_ == static_cast<uint8_t>(ObUMediumIntType); }
  OB_INLINE bool is_uint32() const { return type_ == static_cast<uint8_t>(ObUInt32Type); }
  OB_INLINE bool is_uint64() const { return type_ == static_cast<uint8_t>(ObUInt64Type); }
  OB_INLINE bool is_float() const { return type_ == static_cast<uint8_t>(ObFloatType); }
  OB_INLINE bool is_double() const { return type_ == static_cast<uint8_t>(ObDoubleType); }
  OB_INLINE bool is_ufloat() const { return type_ == static_cast<uint8_t>(ObUFloatType); }
  OB_INLINE bool is_udouble() const { return type_ == static_cast<uint8_t>(ObUDoubleType); }
  OB_INLINE bool is_number() const { return type_ == static_cast<uint8_t>(ObNumberType); }
  OB_INLINE bool is_unumber() const { return type_ == static_cast<uint8_t>(ObUNumberType); }
  OB_INLINE bool is_number_float() const { return type_ == static_cast<uint8_t>(ObNumberFloatType); }
  OB_INLINE bool is_datetime() const { return type_ == static_cast<uint8_t>(ObDateTimeType); }
  OB_INLINE bool is_timestamp() const { return type_ == static_cast<uint8_t>(ObTimestampType); }
  OB_INLINE bool is_year() const { return type_ == static_cast<uint8_t>(ObYearType); }
  OB_INLINE bool is_date() const { return type_ == static_cast<uint8_t>(ObDateType); }
  OB_INLINE bool is_time() const { return type_ == static_cast<uint8_t>(ObTimeType); }
  OB_INLINE bool is_varchar() const
  {
    return ((type_ == static_cast<uint8_t>(ObVarcharType)) && (CS_TYPE_BINARY != cs_type_));
  }
  OB_INLINE bool is_char() const
  {
    return ((type_ == static_cast<uint8_t>(ObCharType)) && (CS_TYPE_BINARY != cs_type_));
  }
  OB_INLINE bool is_varbinary() const
  {
    return (type_ == static_cast<uint8_t>(ObVarcharType) && CS_TYPE_BINARY == cs_type_);
  }
  OB_INLINE bool is_binary() const
  {
    return (type_ == static_cast<uint8_t>(ObCharType) && CS_TYPE_BINARY == cs_type_);
  }
  OB_INLINE bool is_hex_string() const { return type_ == static_cast<uint8_t>(ObHexStringType); }
  OB_INLINE bool is_raw() const { return type_ == static_cast<uint8_t>(ObRawType); }
  OB_INLINE bool is_ext() const { return type_ == static_cast<uint8_t>(ObExtendType); }
  OB_INLINE bool is_unknown() const { return type_ == static_cast<uint8_t>(ObUnknownType); }
  // combination of above functions.
  OB_INLINE bool is_varbinary_or_binary() const { return is_varbinary() || is_binary(); }
  OB_INLINE bool is_varchar_or_char() const { return is_varchar() || is_char(); }
  OB_INLINE bool is_varying_len_char_type() const { return is_varchar(); }
  OB_INLINE bool is_numeric_type() const { return ob_is_numeric_type(get_type()); }
  OB_INLINE bool is_integer_type() const { return ob_is_integer_type(get_type()); }
  OB_INLINE bool is_string_type() const { return ob_is_string_tc(get_type()); }
  OB_INLINE bool is_temporal_type() const { return ob_is_temporal_type(get_type()); }
  OB_INLINE bool is_nchar() const { return type_ == static_cast<uint8_t>(ObNCharType); }
  OB_INLINE bool is_nvarchar2() const { return type_ == static_cast<uint8_t>(ObNVarchar2Type); }
  OB_INLINE bool is_nstring() const { return is_nvarchar2() || is_nchar(); }
  OB_INLINE bool is_blob() const { return (ob_is_text_tc(get_type()) && CS_TYPE_BINARY == cs_type_); }
  OB_INLINE bool is_character_type() const { return is_nstring() || is_varchar_or_char(); }
  OB_INLINE bool is_timestamp_tz() const { return type_ == static_cast<uint8_t>(ObTimestampTZType); }
  OB_INLINE bool is_timestamp_ltz() const { return type_ == static_cast<uint8_t>(ObTimestampLTZType); }
  OB_INLINE bool is_timestamp_nano() const { return type_ == static_cast<uint8_t>(ObTimestampNanoType); }
  OB_INLINE bool is_unsigned_integer() const
  {
    return (static_cast<uint8_t>(ObUTinyIntType) <= type_
         && static_cast<uint8_t>(ObUInt64Type)   >= type_);
  }

  OB_INLINE void set_collation_level(ObCollationLevel cs_level) { cs_level_ = cs_level; }
  OB_INLINE void set_collation_type(ObCollationType cs_type) { cs_type_ = cs_type; }
  OB_INLINE void set_default_collation_type() { set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset())); }
  OB_INLINE ObCollationLevel get_collation_level() const { return static_cast<ObCollationLevel>(cs_level_); }
  OB_INLINE ObCollationType get_collation_type() const { return static_cast<ObCollationType>(cs_type_); }
  OB_INLINE bool is_collation_invalid() const
  {
    return CS_LEVEL_INVALID == cs_level_ && CS_TYPE_INVALID == cs_type_;
  }
  OB_INLINE void set_collation(const ObObjMeta &other)
  {
    cs_level_ = other.cs_level_;
    cs_type_ = other.cs_type_;
  }
  OB_INLINE void set_scale(ObScale scale) { scale_ = static_cast<int8_t>(scale); }
  OB_INLINE ObScale get_scale() const { return static_cast<ObScale>(scale_); }

  TO_STRING_KV(N_TYPE, ob_obj_type_str(static_cast<ObObjType>(type_)),
               N_COLLATION, ObCharset::collation_name(get_collation_type()),
               N_COERCIBILITY, ObCharset::collation_level(get_collation_level()));
  NEED_SERIALIZE_AND_DESERIALIZE;
private:
  uint8_t type_;
  uint8_t cs_level_;    // collation level
  uint8_t cs_type_;     // collation type
  int8_t scale_;        // scale
};

struct ObObjPrintParams {
  ObObjPrintParams(const ObTimeZoneInfo* tz_info, ObCollationType cs_type)
      : tz_info_(tz_info), cs_type_(cs_type), print_flags_(0)
  {}
  ObObjPrintParams(const ObTimeZoneInfo* tz_info)
      : tz_info_(tz_info), cs_type_(CS_TYPE_UTF8MB4_GENERAL_CI), print_flags_(0)
  {}
  ObObjPrintParams() : tz_info_(NULL), cs_type_(CS_TYPE_UTF8MB4_GENERAL_CI), print_flags_(0)
  {}
  TO_STRING_KV(K_(tz_info), K_(cs_type));
  const ObTimeZoneInfo* tz_info_;
  ObCollationType cs_type_;
  union {
    uint32_t print_flags_;
    struct {
      uint32_t need_cast_expr_ : 1;
      uint32_t is_show_create_view_ : 1;
      uint32_t reserved_ : 30;
    };
  };
};

// sizeof(ObObjValue)=8
union ObObjValue
{
  int64_t int64_;
  uint64_t uint64_;

  float float_;
  double double_;

  const char* string_;

  uint32_t *nmb_digits_;

  int64_t datetime_;
  int32_t date_;
  int64_t time_;
  uint8_t year_;

  int64_t ext_;
  int64_t unknown_;
};

class ObBatchChecksum;

class ObObj
{
public:
  // min, max extend value
  static const int64_t MIN_OBJECT_VALUE         = UINT64_MAX - 2;
  static const int64_t MAX_OBJECT_VALUE         = UINT64_MAX - 1;
  // WARNING:used only in RootTable, other user should not use this
  // to represent a (min, max) range in Roottable impl, we need this
  static const char *MIN_OBJECT_VALUE_STR;
  static const char *MAX_OBJECT_VALUE_STR;
  static const char *NOP_VALUE_STR;
public:
  ObObj();
  ObObj(const ObObj &other);
  explicit ObObj(bool val);
  explicit ObObj(int64_t val);
  explicit ObObj(ObObjType type);
  inline void reset();
  //when in not strict sql mode, build default value refer to data type
  int build_not_strict_default_value();
  static ObObj make_min_obj();
  static ObObj make_max_obj();
  static ObObj make_nop_obj();

  //@{ setters
  void set_type(const ObObjType &type)
  {
    if (OB_UNLIKELY(ObNullType > type || ObMaxType < type)) {
      COMMON_LOG(ERROR, "invalid type", K(type));
      meta_.set_type(ObUnknownType);
    } else {
      meta_.set_type(type);
    }
  }
  void set_collation_level(const ObCollationLevel &cs_level) { meta_.set_collation_level(cs_level); }
  void set_collation_type(const ObCollationType &cs_type) { meta_.set_collation_type(cs_type); }
  void set_default_collation_type() { meta_.set_default_collation_type(); }
  void set_meta_type(const ObObjMeta &type) { meta_ = type; }
  void set_collation(const ObObjMeta &type) { meta_.set_collation(type); }
  void set_scale(ObScale scale) { meta_.set_scale(scale); }

  void set_int(const ObObjType type, const int64_t value);
  void set_tinyint(const int8_t value);
  void set_smallint(const int16_t value);
  void set_mediumint(const int32_t value);
  void set_int32(const int32_t value);
  void set_int(const int64_t value);  // aka bigint

  void set_tinyint_value(const int8_t value);
  void set_smallint_value(const int16_t value);
  void set_mediumint_value(const int32_t value);
  void set_int32_value(const int32_t value);
  void set_int_value(const int64_t value);  // aka bigint

  void set_uint(const ObObjType type, const uint64_t value);
  void set_utinyint(const uint8_t value);
  void set_usmallint(const uint16_t value);
  void set_umediumint(const uint32_t value);
  void set_uint32(const uint32_t value);
  void set_uint64(const uint64_t value);

  void set_float(const ObObjType type, const float value);
  void set_float(const float value);
  void set_float_value(const float value);
  void set_ufloat(const float value);

  void set_double(const ObObjType type, const double value);
  void set_double(const double value);
  void set_double_value(const double value);
  void set_udouble(const double value);

  void set_number(const ObObjType type, const number::ObNumber &num);
  void set_number(const number::ObNumber &num);
  void set_number_value(const number::ObNumber &num);
  void set_number(const number::ObNumber::Desc nmb_desc, uint32_t *nmb_digits);

  void set_unumber(const number::ObNumber &num);
  void set_unumber(const number::ObNumber::Desc nmb_desc, uint32_t *nmb_digits);
  void set_unumber_value(const number::ObNumber::Desc nmb_desc, uint32_t *nmb_digits);

  void set_number_float(const number::ObNumber &num);
  void set_number_float(const number::ObNumber::Desc nmb_desc, uint32_t *nmb_digits);
  void set_number_float_value(const number::ObNumber::Desc nmb_desc, uint32_t *nmb_digits);

  void set_datetime(const ObObjType type, const int64_t value);
  void set_datetime(const int64_t value);
  void set_timestamp(const int64_t value);
  void set_date(const int32_t value);
  void set_time(const int64_t value);
  void set_year(const uint8_t value);

  void set_datetime_value(const int64_t value);
  void set_timestamp_value(const int64_t value);
  void set_otimestamp_value(const ObObjType type, const ObOTimestampData &value);
  void set_otimestamp_value(const ObObjType type, const int64_t time_us, const uint32_t time_ctx_desc);
  void set_otimestamp_value(const ObObjType type, const int64_t time_us, const uint16_t time_desc);
  void set_otimestamp_null(const ObObjType type);
  void set_date_value(const int32_t value);
  void set_time_value(const int64_t value);
  void set_year_value(const uint8_t value);

  void set_timestamp_tz(const int64_t time_us, const uint32_t time_ctx_desc) { set_otimestamp_value(ObTimestampTZType, time_us, time_ctx_desc); }
  void set_timestamp_ltz(const int64_t time_us, const uint16_t time_desc) { set_otimestamp_value(ObTimestampLTZType, time_us, time_desc); }
  void set_timestamp_nano(const int64_t time_us, const uint16_t time_desc) { set_otimestamp_value(ObTimestampNanoType, time_us, time_desc); }
  void set_timestamp_tz(const ObOTimestampData &value) { set_otimestamp_value(ObTimestampTZType, value); }
  void set_timestamp_ltz(const ObOTimestampData &value) { set_otimestamp_value(ObTimestampLTZType, value); }
  void set_timestamp_nano(const ObOTimestampData &value) { set_otimestamp_value(ObTimestampNanoType, value); }

  void set_string(const ObObjType type, const char *ptr, const ObString::obstr_size_t size);
  void set_string(const ObObjType type, const ObString &value);
  void set_varchar(const ObString &value);
  void set_varchar(const char *ptr, const ObString::obstr_size_t size);
  void set_varchar(const char *cstr);
  void set_char(const ObString &value);
  void set_varbinary(const ObString &value);
  void set_binary(const ObString &value);
  void set_raw(const ObString &value);
  void set_raw(const char *ptr, const ObString::obstr_size_t size);
  void set_raw_value(const char *ptr, const ObString::obstr_size_t size);
  void set_hex_string(const ObString &value);
  void set_hex_string_value(const ObString &value);

  inline void set_bool(const bool value);
  inline void set_ext(const int64_t value);
  inline void set_unknown(const int64_t value);

  inline void set_null();
  inline void set_min_value();
  inline void set_max_value();
  inline void set_nop_value();

  void set_val_len(const int32_t val_len);

  void set_nvarchar2(const ObString &value);
  void set_nvarchar2_value(const char *ptr, const ObString::obstr_size_t size);
  void set_nchar(const ObString &value);
  void set_nchar_value(const char *ptr, const ObString::obstr_size_t size);
  void set_urowid(const ObURowIDData &urowid)
  {
    meta_.set_urowid();
    v_.string_ = (const char *)urowid.rowid_content_;
    val_len_ = static_cast<int32_t>(urowid.rowid_len_);
  }
  void set_urowid(const char *ptr, const int64_t size)
  {
    meta_.set_urowid();
    v_.string_ = ptr;
    val_len_ = static_cast<int32_t>(size);
  }
  
  //@}

  //@{ getters
  OB_INLINE ObObjType get_type() const { return meta_.get_type(); }
  OB_INLINE ObObjTypeClass get_type_class() const { return meta_.get_type_class(); }
  OB_INLINE ObCollationLevel get_collation_level() const { return meta_.get_collation_level(); }
  OB_INLINE ObCollationType get_collation_type() const { return meta_.get_collation_type(); }
  OB_INLINE ObScale get_scale() const { return meta_.get_scale(); }
  inline const ObObjMeta& get_meta() const { return meta_; }

  inline int get_tinyint(int8_t &value) const;
  inline int get_smallint(int16_t &value) const;
  inline int get_mediumint(int32_t &value) const;
  inline int get_int32(int32_t &value) const;
  inline int get_int(int64_t &value) const;

  inline int get_utinyint(uint8_t &value) const;
  inline int get_usmallint(uint16_t &value) const;
  inline int get_umediumint(uint32_t &value) const;
  inline int get_uint32(uint32_t &value) const;
  inline int get_uint64(uint64_t &value) const;

  inline int get_float(float &value) const;
  inline int get_double(double &value) const;
  inline int get_ufloat(float &value) const;
  inline int get_udouble(double &value) const;

  inline int get_number(number::ObNumber &num) const;
  inline int get_unumber(number::ObNumber &num) const;
  inline int get_number_float(number::ObNumber &num) const;

  inline int get_datetime(int64_t &value) const;
  inline int get_timestamp(int64_t &value) const;
  inline int get_date(int32_t &value) const;
  inline int get_time(int64_t &value) const;
  inline int get_year(uint8_t &value) const;

  inline int get_string(ObString &value) const;
  inline int get_varchar(ObString &value) const;
  inline int get_char(ObString &value) const;
  inline int get_nvarchar2(ObString &value) const;
  inline int get_nchar(ObString &value) const;
  inline int get_varbinary(ObString &value) const;
  inline int get_raw(ObString &value) const;
  inline int get_binary(ObString &value) const;
  inline int get_hex_string(ObString &value) const;

  inline int get_bool(bool &value) const;
  inline int get_ext(int64_t &value) const;
  inline int get_unknown(int64_t &value) const;

  inline int32_t get_val_len() const { return val_len_; }

  /// the follow getters do not check type, use them when you already known the type
  OB_INLINE int8_t get_tinyint() const { return static_cast<int8_t>(v_.int64_); }
  OB_INLINE int16_t get_smallint() const { return static_cast<int16_t>(v_.int64_); }
  OB_INLINE int32_t get_mediumint() const { return static_cast<int32_t>(v_.int64_); }
  OB_INLINE int32_t get_int32() const { return static_cast<int32_t>(v_.int64_); }
  OB_INLINE int64_t get_int() const { return static_cast<int64_t>(v_.int64_); }

  OB_INLINE uint8_t get_utinyint() const { return static_cast<uint8_t>(v_.uint64_); }
  OB_INLINE uint16_t get_usmallint() const { return static_cast<uint16_t>(v_.uint64_); }
  OB_INLINE uint32_t get_umediumint() const { return static_cast<uint32_t>(v_.uint64_); }
  OB_INLINE uint32_t get_uint32() const { return static_cast<uint32_t>(v_.uint64_); }
  OB_INLINE uint64_t get_uint64() const { return static_cast<uint64_t>(v_.uint64_); }

  OB_INLINE float get_float() const { return v_.float_; }
  OB_INLINE double get_double() const { return v_.double_; }
  OB_INLINE float get_ufloat() const { return v_.float_; }
  OB_INLINE double get_udouble() const { return v_.double_; }

  OB_INLINE number::ObNumber get_number() const { return number::ObNumber(nmb_desc_.desc_, v_.nmb_digits_); }
  OB_INLINE number::ObNumber get_unumber() const { return number::ObNumber(nmb_desc_.desc_, v_.nmb_digits_); }
  OB_INLINE number::ObNumber get_number_float() const { return number::ObNumber(nmb_desc_.desc_, v_.nmb_digits_); }
  OB_INLINE int64_t get_number_digit_length() const { return nmb_desc_.len_; }

  OB_INLINE int64_t get_datetime() const { return v_.datetime_; }
  OB_INLINE int64_t get_timestamp() const { return v_.datetime_; }
  OB_INLINE int32_t get_date() const { return v_.date_; }
  OB_INLINE int64_t get_time() const { return v_.time_; }
  OB_INLINE uint8_t get_year() const { return v_.year_; }

  OB_INLINE ObString get_string() const { return ObString(val_len_, v_.string_); }
  OB_INLINE ObString get_varchar() const { return ObString(val_len_, v_.string_); }
  OB_INLINE ObString get_char() const { return ObString(val_len_, v_.string_); }
  OB_INLINE ObString get_varbinary() const { return ObString(val_len_, v_.string_); }
  OB_INLINE ObString get_raw() const { return ObString(val_len_, v_.string_); }
  OB_INLINE ObString get_binary() const { return ObString(val_len_, v_.string_); }
  OB_INLINE ObString get_hex_string() const { return ObString(val_len_, v_.string_); }
  OB_INLINE int64_t get_number_byte_length() const { return nmb_desc_.len_ * sizeof(uint32_t); }

  OB_INLINE bool get_bool() const { return (0 != v_.int64_); }
  inline int64_t get_ext() const;
  OB_INLINE int64_t get_unknown() const { return v_.unknown_; }

  inline const number::ObNumber::Desc& get_number_desc() const { return nmb_desc_; }
  inline const uint32_t *get_number_digits() const { return v_.nmb_digits_; }

  inline const char* get_string_ptr() const { return v_.string_; }
  inline int32_t get_string_len() const { return val_len_; }

  inline ObString get_nvarchar2() const { return ObString(val_len_, v_.string_); }
  inline ObString get_nchar() const { return ObString(val_len_, v_.string_); }
  
  inline ObOTimestampData::UnionTZCtx get_tz_desc() const
  {
    return time_ctx_;
  }
  inline ObOTimestampData get_otimestamp_value() const
  {
    return ObOTimestampData(v_.datetime_, time_ctx_);
  }
  static int64_t get_otimestamp_store_size(const bool is_timestamp_tz)
  {
    return static_cast<int64_t>(sizeof(int64_t) + (is_timestamp_tz ? sizeof(uint32_t) : sizeof(uint16_t)));
  }
  inline int64_t get_otimestamp_store_size() const
  {
    return get_otimestamp_store_size(is_timestamp_tz());
  }
  
  //@}

  //@{ test functions
  OB_INLINE bool is_valid_type() const { return meta_.is_valid(); }
  OB_INLINE bool is_invalid_type() const { return meta_.is_invalid(); }

  OB_INLINE bool is_null() const { return meta_.is_null(); }
  OB_INLINE bool is_tinyint() const { return meta_.is_tinyint(); }
  OB_INLINE bool is_smallint() const { return meta_.is_smallint(); }
  OB_INLINE bool is_mediumint() const { return meta_.is_mediumint(); }
  OB_INLINE bool is_int32() const { return meta_.is_int32(); }
  OB_INLINE bool is_int() const { return meta_.is_int(); }
  OB_INLINE bool is_utinyint() const { return meta_.is_utinyint(); }
  OB_INLINE bool is_usmallint() const { return meta_.is_usmallint(); }
  OB_INLINE bool is_umediumint() const { return meta_.is_umediumint(); }
  OB_INLINE bool is_uint32() const { return meta_.is_uint32(); }
  OB_INLINE bool is_uint64() const { return meta_.is_uint64(); }
  OB_INLINE bool is_float() const { return meta_.is_float(); }
  OB_INLINE bool is_double() const { return meta_.is_double(); }
  OB_INLINE bool is_ufloat() const { return meta_.is_ufloat(); }
  OB_INLINE bool is_udouble() const { return meta_.is_udouble(); }
  OB_INLINE bool is_number() const { return meta_.is_number(); }
  OB_INLINE bool is_unumber() const { return meta_.is_unumber(); }
  OB_INLINE bool is_number_float() const { return meta_.is_number_float(); }
  OB_INLINE bool is_datetime() const { return meta_.is_datetime(); }
  OB_INLINE bool is_timestamp() const { return meta_.is_timestamp(); }
  OB_INLINE bool is_year() const { return meta_.is_year(); }
  OB_INLINE bool is_date() const { return meta_.is_date(); }
  OB_INLINE bool is_time() const { return meta_.is_time(); }
  OB_INLINE bool is_varchar() const { return meta_.is_varchar(); }
  OB_INLINE bool is_char() const { return meta_.is_char(); }
  OB_INLINE bool is_varbinary() const { return meta_.is_varbinary(); }
  OB_INLINE bool is_raw() const { return meta_.is_raw(); }
  OB_INLINE bool is_binary() const { return meta_.is_binary(); }
  OB_INLINE bool is_hex_string() const { return meta_.is_hex_string(); }
  OB_INLINE bool is_ext() const { return meta_.is_ext(); }
  OB_INLINE bool is_unknown() const { return meta_.is_unknown(); }

  OB_INLINE bool is_integer_type() const { return meta_.is_integer_type(); }
  OB_INLINE bool is_numeric_type() const { return meta_.is_numeric_type(); }
  OB_INLINE bool is_string_type() const { return meta_.is_string_type(); }
  OB_INLINE bool is_temporal_type() const { return meta_.is_temporal_type(); }
  OB_INLINE bool is_varchar_or_char() const { return meta_.is_varchar_or_char(); }
  OB_INLINE bool is_varying_len_char_type() const { return meta_.is_varying_len_char_type(); }
  OB_INLINE bool is_character_type() const { return meta_.is_character_type(); }
  OB_INLINE bool is_blob() const { return meta_.is_blob(); }
  OB_INLINE bool is_timestamp_tz() const { return meta_.is_timestamp_tz(); }
  OB_INLINE bool is_timestamp_nano() const { return meta_.is_timestamp_nano(); }
  OB_INLINE bool is_varbinary_or_binary() const { return meta_.is_varbinary_or_binary(); }

  inline bool is_min_value() const;
  inline bool is_max_value() const;
  inline bool is_nop_value() const;
  inline bool is_true() const;
  inline bool is_false() const;

  // check if the object is true with implicit type casting
  int is_true(bool range_check, bool &is_true) const;

  bool is_zero() const;
  //@}

  /// apply mutation to this obj
  int apply(const ObObj &mutation);

  //@{ comparison
  bool operator<(const ObObj &that_obj) const;
  bool operator>(const ObObj &that_obj) const;
  bool operator<=(const ObObj &that_obj) const;
  bool operator>=(const ObObj &that_obj) const;
  bool operator==(const ObObj &that_obj) const;
  bool operator!=(const ObObj &that_obj) const;
  int compare(const ObObj &other, ObCollationType cs_type = CS_TYPE_INVALID) const;
  bool is_equal(const ObObj &other, ObCollationType cs_type = CS_TYPE_INVALID) const;
  //@}

  //@{ print utilities
  /// print as JSON style
  int64_t to_string(char *buffer, const int64_t length, const ObTimeZoneInfo *tz_info = NULL) const;
  /// print as SQL literal style, e.g. used to show column default value
  int print_sql_literal(char *buffer, int64_t length, int64_t &pos, const ObTimeZoneInfo *tz_info = NULL) const;
  /// print as SQL VARCHAR literal
  int print_varchar_literal(char *buffer, int64_t length, int64_t &pos, const ObTimeZoneInfo *tz_info = NULL) const;
  /// print as plain string
  int print_plain_str_literal(char *buffer, int64_t length, int64_t &pos, const ObTimeZoneInfo *tz_info = NULL) const;

  void print_range_value(char *buffer, int64_t length, int64_t &pos) const;
  void print_str_with_repeat(char *buffer, int64_t length, int64_t &pos) const;
  /// dump into log
  void dump(const int32_t log_level = OB_LOG_LEVEL_DEBUG) const;
  //@}

  //@{  deep copy
  bool need_deep_copy()const;
  int64_t get_deep_copy_size() const;
  int deep_copy(const ObObj &src, char *buf, const int64_t size, int64_t &pos);

  const void *get_data_ptr() const;
  int64_t get_data_length() const;
  //@}

  //@{ checksum
  // CRC64
  int64_t checksum(const int64_t current) const;
  void checksum(ObBatchChecksum &bc) const;
  // murmurhash
  uint64_t hash(uint64_t seed = 0) const;
  uint64_t hash_murmur(uint64_t seed = 0) const;
  uint64_t varchar_hash(ObCollationType cs_type, uint64_t seed = 0) const;
  uint64_t hash_v1(uint64_t seed = 0) const;  // for compatible purpose, use hash() instead
  bool can_compare(const ObObj &other) const;
  bool check_collation_integrity() const;
  //@}

  NEED_SERIALIZE_AND_DESERIALIZE;
private:
  friend class tests::common::ObjTest;
  friend class ObCompactCellWriter;
  friend class ObCompactCellIterator;
  uint64_t murmurhash(const uint64_t hash) const;
  uint64_t murmurhash(const ObCollationType cs_type, const uint64_t hash) const;
  // murmurhash which do not consider ObObjType
  uint64_t murmurhash_v2(const uint64_t hash) const;
public:
  ObObjMeta meta_;  // sizeof = 4
  union
  {
    int32_t val_len_;
    number::ObNumber::Desc nmb_desc_;
    ObOTimestampData::UnionTZCtx time_ctx_;
  };  // sizeof = 4
  ObObjValue v_;  // sizeof = 8
};

struct ObjHashBase
{
  static const bool is_varchar_hash = true;
};

// default hash method: same with ObObj::hash())
//  murmurhash for non string types.
//  mysql string hash for string types.
struct ObDefaultHash : public ObjHashBase
{
  static const bool is_varchar_hash = false;
  static uint64_t hash(const void *data, uint64_t len, uint64_t seed)
  {
    return murmurhash64A(data, static_cast<int32_t>(len), seed);
  }
};

struct ObMurmurHash : public ObjHashBase
{
  static uint64_t hash(const void *data, uint64_t len, uint64_t seed)
  {
    return murmurhash64A(data, static_cast<int32_t>(len), seed);
  }
};

template <ObObjType type, typename T, typename P>
struct ObjHashCalculator
{
  static uint64_t calc_hash_value(const P &param, const uint64_t hash) {
    UNUSED(param);
    UNUSED(hash);
    return 0;
  }
};

inline ObObj::ObObj()
{
  meta_.set_null();
  meta_.set_collation_type(CS_TYPE_INVALID);
  meta_.set_collation_level(CS_LEVEL_INVALID);
}

inline ObObj::ObObj(bool val)
{
  set_bool(val);
}

inline ObObj::ObObj(int64_t val)
{
  set_int(val);
}

inline ObObj::ObObj(ObObjType type)
{
  meta_.set_type(type);
}

inline ObObj::ObObj(const ObObj &other)
{
  *this = other;
}

inline void ObObj::reset()
{
  meta_.set_null();
  meta_.set_collation_type(CS_TYPE_INVALID);
  meta_.set_collation_level(CS_LEVEL_INVALID);
}

inline ObObj ObObj::make_min_obj()
{
  ObObj obj;
  obj.set_min_value();
  return obj;
}

inline ObObj ObObj::make_max_obj()
{
  ObObj obj;
  obj.set_max_value();
  return obj;
}

inline ObObj ObObj::make_nop_obj()
{
  ObObj obj;
  obj.set_nop_value();
  return obj;
}

inline void ObObj::set_int(const ObObjType type, const int64_t value)
{
  meta_.set_type(type);
  meta_.set_collation_level(CS_LEVEL_NUMERIC);
  v_.int64_ = value;
}

inline void ObObj::set_tinyint(const int8_t value)
{
  meta_.set_tinyint();
  v_.int64_ = static_cast<int64_t>(value);
}

inline void ObObj::set_tinyint_value(const int8_t value)
{
//  meta_.set_tinyint();
  v_.int64_ = static_cast<int64_t>(value);
}

inline void ObObj::set_smallint(const int16_t value)
{
  meta_.set_smallint();
  v_.int64_ = static_cast<int64_t>(value);
}

inline void ObObj::set_smallint_value(const int16_t value)
{
  v_.int64_ = static_cast<int64_t>(value);
}

inline void ObObj::set_mediumint(const int32_t value)
{
  meta_.set_mediumint();
  v_.int64_ = static_cast<int64_t>(value);
}

inline void ObObj::set_int32(const int32_t value)
{
  meta_.set_int32();
  v_.int64_ = static_cast<int64_t>(value);
}

inline void ObObj::set_int32_value(const int32_t value)
{
//  meta_.set_int32();
  v_.int64_ = static_cast<int64_t>(value);
}

inline void ObObj::set_int(const int64_t value)
{
  meta_.set_int();
  v_.int64_ = value;
}

inline void ObObj::set_int_value(const int64_t value)
{
  v_.int64_ = value;
}

inline void ObObj::set_uint(const ObObjType type, const uint64_t value)
{
  meta_.set_type(type);
  meta_.set_collation_level(CS_LEVEL_NUMERIC);
  v_.uint64_ = value;
}

inline void ObObj::set_utinyint(const uint8_t value)
{
  meta_.set_utinyint();
  v_.uint64_ = static_cast<uint64_t>(value);
}

inline void ObObj::set_usmallint(const uint16_t value)
{
  meta_.set_usmallint();
  v_.uint64_ = static_cast<uint64_t>(value);
}

inline void ObObj::set_umediumint(const uint32_t value)
{
  meta_.set_umediumint();
  v_.uint64_ = static_cast<uint64_t>(value);
}

inline void ObObj::set_uint32(const uint32_t value)
{
  meta_.set_uint32();
  v_.uint64_ = static_cast<uint64_t>(value);
}

inline void ObObj::set_uint64(const uint64_t value)
{
  meta_.set_uint64();
  v_.uint64_ = value;
}

inline void ObObj::set_float(const ObObjType type, const float value)
{
  meta_.set_type(type);
  meta_.set_collation_level(CS_LEVEL_NUMERIC);
  v_.float_ = value;
}

inline void ObObj::set_float(const float value)
{
  meta_.set_float();
  v_.float_ = value;
}

inline void ObObj::set_float_value(const float value)
{
//  meta_.set_float();
  v_.float_ = value;
}

inline void ObObj::set_ufloat(const float value)
{
  meta_.set_ufloat();
  v_.float_ = value;
}

inline void ObObj::set_double(const ObObjType type, const double value)
{
  meta_.set_type(type);
  meta_.set_collation_level(CS_LEVEL_NUMERIC);
  v_.double_ = value;
}

inline void ObObj::set_double(const double value)
{
  meta_.set_double();
  v_.double_ = value;
}

inline void ObObj::set_double_value(const double value)
{
  v_.double_ = value;
}

inline void ObObj::set_udouble(const double value)
{
  meta_.set_udouble();
  v_.double_ = value;
}

inline void ObObj::set_number(const ObObjType type, const number::ObNumber &num)
{
  meta_.set_type(type);
  meta_.set_collation_level(CS_LEVEL_NUMERIC);
  nmb_desc_.desc_ = num.get_desc();
  v_.nmb_digits_ = num.get_digits();
}

inline void ObObj::set_number(const number::ObNumber &num)
{
  meta_.set_number();
  nmb_desc_.desc_ = num.get_desc();
  v_.nmb_digits_ = num.get_digits();
}

inline void ObObj::set_number_value(const number::ObNumber &num)
{
  nmb_desc_.desc_ = num.get_desc();
  v_.nmb_digits_ = num.get_digits();
}

inline void ObObj::set_number(const number::ObNumber::Desc nmb_desc, uint32_t *nmb_digits)
{
  meta_.set_number();
  nmb_desc_ = nmb_desc;
  v_.nmb_digits_ = nmb_digits;
}

inline void ObObj::set_unumber(const number::ObNumber &num)
{
  meta_.set_unumber();
  nmb_desc_.desc_ = num.get_desc();
  v_.nmb_digits_ = num.get_digits();
}

inline void ObObj::set_unumber(const number::ObNumber::Desc nmb_desc, uint32_t *nmb_digits)
{
  meta_.set_unumber();
  nmb_desc_ = nmb_desc;
  v_.nmb_digits_ = nmb_digits;
}

inline void ObObj::set_unumber_value(const number::ObNumber::Desc nmb_desc, uint32_t *nmb_digits)
{
  nmb_desc_ = nmb_desc;
  v_.nmb_digits_ = nmb_digits;
}

inline void ObObj::set_number_float(const number::ObNumber &num)
{
  meta_.set_number_float();
  nmb_desc_.desc_ = num.get_desc_value();
  v_.nmb_digits_ = num.get_digits();
}

inline void ObObj::set_number_float(const number::ObNumber::Desc nmb_desc, uint32_t *nmb_digits)
{
  meta_.set_number_float();
  nmb_desc_ = nmb_desc;
  v_.nmb_digits_ = nmb_digits;
}

inline void ObObj::set_number_float_value(const number::ObNumber::Desc nmb_desc, uint32_t *nmb_digits)
{
  nmb_desc_ = nmb_desc;
  v_.nmb_digits_ = nmb_digits;
}

inline void ObObj::set_datetime(const ObObjType type, const int64_t value)
{
  meta_.set_type(type);
  meta_.set_collation_level(CS_LEVEL_NUMERIC);
  v_.datetime_ = value;
}

inline void ObObj::set_datetime(const int64_t value)
{
  meta_.set_datetime();
  v_.datetime_ = value;
}
inline void ObObj::set_datetime_value(const int64_t value)
{

  v_.datetime_ = value;
}

inline void ObObj::set_timestamp(const int64_t value)
{
  meta_.set_timestamp();
  v_.datetime_ = value;
}

inline void ObObj::set_timestamp_value(const int64_t value)
{
  v_.datetime_ = value;
}

inline void ObObj::set_otimestamp_value(const ObObjType type, const ObOTimestampData &value)
{
  meta_.set_otimestamp_type(type);
  time_ctx_ = value.time_ctx_;
  v_.datetime_ = value.time_us_;
}

inline void ObObj::set_otimestamp_value(const ObObjType type, const int64_t time_us, const uint32_t time_ctx_desc)
{
  meta_.set_otimestamp_type(type);
  time_ctx_.desc_ = time_ctx_desc;
  v_.datetime_ = time_us;
}

inline void ObObj::set_otimestamp_value(const ObObjType type, const int64_t time_us, const uint16_t time_desc)
{
  meta_.set_otimestamp_type(type);
  time_ctx_.tz_desc_ = 0;
  time_ctx_.time_desc_ = time_desc;
  v_.datetime_ = time_us;
}

inline void ObObj::set_otimestamp_null(const ObObjType type)
{
  meta_.set_otimestamp_type(type);
  time_ctx_.tz_desc_ = 0;
  time_ctx_.time_desc_ = 0;
  time_ctx_.is_null_ = 1;
}

inline void ObObj::set_date(const int32_t value)
{
  meta_.set_date();
  v_.date_ = value;
}

inline void ObObj::set_time(const int64_t value)
{
  meta_.set_time();
  v_.time_ = value;
}
inline void ObObj::set_date_value(const int32_t value)
{
  v_.date_ = value;
}

inline void ObObj::set_time_value(const int64_t value)
{
  v_.time_ = value;
}

inline void ObObj::set_year(const uint8_t value)
{
  meta_.set_year();
  v_.year_ = value;
}

inline void ObObj::set_year_value(const uint8_t value)
{
  v_.year_ = value;
}

inline void ObObj::set_string(const ObObjType type, const char *ptr, const ObString::obstr_size_t size)
{
  meta_.set_type(type);
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = ptr;
  val_len_ = size;
}

inline void ObObj::set_string(const ObObjType type, const ObString &value)
{
  meta_.set_type(type);
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = value.ptr();
  val_len_ = value.length();
}

inline void ObObj::set_varchar(const ObString &value)
{
  meta_.set_varchar();
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = value.ptr();
  val_len_ = value.length();
}

inline void ObObj::set_varchar(const char *ptr, const ObString::obstr_size_t size)
{
  meta_.set_varchar();
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = ptr;
  val_len_ = size;
}

inline void ObObj::set_varchar(const char *cstr)
{
  meta_.set_varchar();
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = cstr;
  val_len_ = static_cast<int32_t>(strlen(cstr));
}

inline void ObObj::set_char(const ObString &value)
{
  meta_.set_char();
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = value.ptr();
  val_len_ = value.length();
}

inline void ObObj::set_varbinary(const ObString &value)
{
  meta_.set_varchar();
  meta_.set_collation_type(CS_TYPE_BINARY);
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = value.ptr();
  val_len_ = value.length();
}

inline void ObObj::set_binary(const ObString &value)
{
  meta_.set_char();
  meta_.set_collation_type(CS_TYPE_BINARY);
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = value.ptr();
  val_len_ = value.length();
}

inline void ObObj::set_raw(const ObString &value)
{
  meta_.set_raw();
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = value.ptr();
  val_len_ = value.length();
}

inline void ObObj::set_raw(const char *ptr, const ObString::obstr_size_t size)
{
  meta_.set_raw();
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = ptr;
  val_len_ = size;
}

inline void ObObj::set_raw_value(const char *ptr, const ObString::obstr_size_t size)
{
  meta_.set_collation_type(CS_TYPE_BINARY);
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = ptr;
  val_len_ = size;
}

inline void ObObj::set_hex_string(const ObString &value)
{
  meta_.set_hex_string();
  v_.string_ = value.ptr();
  val_len_ = value.length();
}

inline void ObObj::set_hex_string_value(const ObString &value)
{
  v_.string_ = value.ptr();
  val_len_ = value.length();
}

inline void ObObj::set_nvarchar2(const ObString &value)
{
  meta_.set_nvarchar2();
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = value.ptr();
  val_len_ = value.length();
}

inline void ObObj::set_nvarchar2_value(const char *ptr, const ObString::obstr_size_t size)
{
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = ptr;
  val_len_ = size;
}

inline void ObObj::set_nchar(const ObString &value)
{
  meta_.set_nchar();
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = value.ptr();
  val_len_ = value.length();
}

inline void ObObj::set_nchar_value(const char *ptr, const ObString::obstr_size_t size)
{
  meta_.set_collation_level(CS_LEVEL_IMPLICIT);
  v_.string_ = ptr;
  val_len_ = size;
}

inline void ObObj::set_null()
{
  meta_.set_null();
}

inline void ObObj::set_bool(const bool value)
{
  meta_.set_tinyint();
  v_.int64_ = static_cast<int64_t>(value);
}

inline void ObObj::set_ext(const int64_t value)
{
  meta_.set_ext();
  v_.ext_ = value;
}

inline void ObObj::set_unknown(const int64_t value)
{
  meta_.set_unknown();
  v_.unknown_ = value;
}

inline void ObObj::set_min_value()
{
  set_ext(MIN_OBJECT_VALUE);
}

inline void ObObj::set_max_value()
{
  set_ext(MAX_OBJECT_VALUE);
}

inline void ObObj::set_nop_value()
{
  set_ext(ObActionFlag::OP_NOP);
}

inline bool ObObj::is_min_value() const
{
  return meta_.get_type() == ObExtendType && v_.ext_ == MIN_OBJECT_VALUE;
}

inline bool ObObj::is_max_value() const
{
  return meta_.get_type() == ObExtendType && v_.ext_ == MAX_OBJECT_VALUE;
}

inline bool ObObj::is_nop_value() const
{
  return meta_.get_type() == ObExtendType && v_.ext_ == ObActionFlag::OP_NOP;
}

inline bool ObObj::is_true() const
{
  return meta_.is_tinyint() && 0 != v_.int64_;
}

inline bool ObObj::is_false() const
{
  return meta_.is_tinyint() && 0 == v_.int64_;
}

inline bool ObObj::need_deep_copy()const
{
  return ((is_string_type() && 0 != get_val_len())
          || ob_is_number_tc(meta_.get_type()));
}

inline int64_t ObObj::get_ext() const
{
  int64_t res = 0;
  if (ObExtendType == meta_.get_type()) {
    res = v_.ext_;
  }
  return res;
}

inline void ObObj::set_val_len(const int32_t val_len)
{
  val_len_ = val_len;
}

////////////////////////////////////////////////////////////////
inline int ObObj::get_tinyint(int8_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_tinyint()) {
    v = static_cast<int8_t>(v_.int64_);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_smallint(int16_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_smallint()) {
    v = static_cast<int16_t>(v_.int64_);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_mediumint(int32_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_mediumint()) {
    v = static_cast<int32_t>(v_.int64_);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_int32(int32_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_int32()) {
    v = static_cast<int32_t>(v_.int64_);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_int(int64_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_int()) {
    v = v_.int64_;
    ret = OB_SUCCESS;
  }
  return ret;
}


inline int ObObj::get_utinyint(uint8_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_utinyint()) {
    v = static_cast<uint8_t>(v_.uint64_);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_usmallint(uint16_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_usmallint()) {
    v = static_cast<uint16_t>(v_.uint64_);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_umediumint(uint32_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_umediumint()) {
    v = static_cast<uint32_t>(v_.uint64_);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_uint32(uint32_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_uint32()) {
    v = static_cast<uint32_t>(v_.uint64_);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_uint64(uint64_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_uint64()) {
    v = v_.uint64_;
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_float(float &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_float()) {
    v = v_.float_;
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_double(double &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_double()) {
    v = v_.double_;
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_ufloat(float &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_ufloat()) {
    v = v_.float_;
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_udouble(double &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_udouble()) {
    v = v_.double_;
    ret = OB_SUCCESS;
  }
  return ret;
}


inline int ObObj::get_number(number::ObNumber &num) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_number()) {
    num.assign(nmb_desc_.desc_, v_.nmb_digits_);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_unumber(number::ObNumber &num) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_unumber()) {
    num.assign(nmb_desc_.desc_, v_.nmb_digits_);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_number_float(number::ObNumber &num) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_number_float()) {
    num.assign(nmb_desc_.desc_, v_.nmb_digits_);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_datetime(int64_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_datetime()) {
    v = v_.datetime_;
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_timestamp(int64_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_timestamp()) {
    v = v_.datetime_;
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_date(int32_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_date()) {
    v = v_.date_;
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_time(int64_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_time()) {
    v = v_.time_;
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_year(uint8_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_year()) {
    v = v_.year_;
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_string(ObString &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_string_type()) {
    v.assign_ptr(v_.string_, val_len_);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_varchar(ObString &v) const
{
  return get_string(v);
}

inline int ObObj::get_char(ObString &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_char()) {
    v.assign_ptr(v_.string_, val_len_);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_nvarchar2(ObString &v) const
{
  return get_string(v);
}

inline int ObObj::get_nchar(ObString &v) const
{
  return get_string(v);
}

inline int ObObj::get_varbinary(ObString &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_varbinary()) {
    v.assign_ptr(v_.string_, val_len_);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_raw(ObString &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_raw()) {
    v.assign_ptr(v_.string_, val_len_);
    ret = OB_SUCCESS;
  } else if (meta_.is_null()) {
    v.assign_ptr(NULL, 0);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_binary(ObString &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_binary()) {
    v.assign_ptr(v_.string_, val_len_);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_hex_string(ObString &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_hex_string()) {
    v.assign_ptr(v_.string_, val_len_);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_bool(bool &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_tinyint()) {
    v = (0 != v_.int64_);
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_ext(int64_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_ext()) {
    v = v_.ext_;
    ret = OB_SUCCESS;
  }
  return ret;
}

inline int ObObj::get_unknown(int64_t &v) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (meta_.is_unknown()) {
    v = v_.unknown_;
    ret = OB_SUCCESS;
  }
  return ret;
}

inline static uint64_t varchar_hash_with_collation(const ObObj &obj,
                                                   const ObCollationType cs_type,
                                                   const uint64_t hash, hash_algo hash_al)
{
  return ObCharset::hash(cs_type, obj.get_string_ptr(), obj.get_string_len(), hash,
                         obj.is_varying_len_char_type() && lib::is_oracle_mode(), hash_al);
}

inline uint64_t ObObj::varchar_hash(ObCollationType cs_type, uint64_t seed) const
{
  check_collation_integrity();
  return varchar_hash_with_collation(*this, cs_type, seed, NULL);
}

inline bool ObObj::can_compare(const ObObj &other) const
{
  bool ret = false;
  if (get_type_class() == other.get_type_class()
      || (ObIntTC == get_type_class() && ObUIntTC == other.get_type_class())
      || (ObUIntTC == get_type_class() && ObIntTC == other.get_type_class())
      || get_type() == ObNullType
      || other.get_type() == ObNullType
      || is_min_value()
      || is_max_value()
      || other.is_min_value()
      || other.is_max_value()
      || (is_datetime() && other.is_datetime())) {
    ret = true;
  }
  return ret;
}

inline const void *ObObj::get_data_ptr() const
{
  const void *ret = NULL;
  if (ob_is_string_tc(get_type())) {
    ret = const_cast<char *>(v_.string_);
  } else if (ObNumberType == get_type()) {
    ret = const_cast<uint32_t *>(v_.nmb_digits_);
  } else {
    ret = &v_;
  }
  return ret;
};


inline int64_t ObObj::get_data_length() const
{
  int64_t ret = sizeof(v_);
  if (ob_is_string_tc(get_type())) {
    ret = val_len_;
  } else if (ObNumberType == get_type()) {
    ret = nmb_desc_.len_;
  }
  return ret;
};

template <typename AllocatorT>
    int ob_write_obj(AllocatorT &allocator, const ObObj &src, ObObj &dst)
{
  int ret = OB_SUCCESS;
  if (ob_is_string_tc(src.get_type())) {
    ObString str = src.get_string();
    ObString str_clone;
    if (OB_SUCCESS == (ret = ob_write_string(allocator, str, str_clone))) {
      dst.set_string(src.get_type(), str_clone);
      dst.set_collation_level(src.get_collation_level());
      dst.set_collation_type(src.get_collation_type());
    }
  } else if (ob_is_number_tc(src.get_type())) {
    number::ObNumber nmb = src.get_number();
    number::ObNumber nmb_clone;
    if (OB_SUCCESS == (ret = nmb_clone.from(nmb, allocator))) {
      dst.set_number(src.get_type(), nmb_clone);
    }
  } else {
    dst = src;
  }
  return ret;
}

class ObObjParam : public ObObj
{
public:

  struct ParamFlag
  {
    ParamFlag() : need_to_check_type_(true), need_to_check_bool_value_(false), expected_bool_value_(false)
    { }
    TO_STRING_KV(K_(need_to_check_type), K_(need_to_check_bool_value), K_(expected_bool_value));
    void reset();

    uint8_t need_to_check_type_: 1; //TRUE if the type need to be checked by plan cache, FALSE otherwise
    uint8_t need_to_check_bool_value_ : 1;//TRUE if the bool value need to be checked by plan cache, FALSE otherwise
    uint8_t expected_bool_value_ : 1;//bool value, effective only when need_to_check_bool_value_ is true
  };

  ObObjParam() : ObObj(), accuracy_(), res_flags_(0)
  {
  }
public:
  void reset();
  // accuracy.
  OB_INLINE void set_accuracy(const common::ObAccuracy &accuracy) { accuracy_.set_accuracy(accuracy); }
  OB_INLINE void set_length(common::ObLength length) { accuracy_.set_length(length); }
  OB_INLINE void set_precision(common::ObPrecision precision) { accuracy_.set_precision(precision); }
  OB_INLINE void set_scale(common::ObScale scale) { accuracy_.set_scale(scale); }
  OB_INLINE const common::ObAccuracy &get_accuracy() const { return accuracy_; }
  OB_INLINE common::ObLength get_length() const { return accuracy_.get_length(); }
  OB_INLINE common::ObPrecision get_precision() const { return accuracy_.get_precision(); }
  OB_INLINE common::ObScale get_scale() const { return accuracy_.get_scale(); }
  OB_INLINE void set_result_flag(uint32_t flag) { res_flags_ |= flag; }
  OB_INLINE void unset_result_flag(uint32_t flag) { res_flags_ &= (~flag); }
  OB_INLINE bool has_result_flag(uint32_t flag) const { return res_flags_ & flag; }
  OB_INLINE uint32_t get_result_flag() const { return res_flags_; }

  OB_INLINE const ParamFlag &get_param_flag() const { return flag_; }
  OB_INLINE void set_need_to_check_type(bool flag) { flag_.need_to_check_type_ = flag; }
  OB_INLINE bool need_to_check_type() const { return flag_.need_to_check_type_; }

  OB_INLINE void set_need_to_check_bool_value(bool flag) { flag_.need_to_check_bool_value_ = flag; }
  OB_INLINE bool need_to_check_bool_value() const { return flag_.need_to_check_bool_value_; }

  OB_INLINE void set_expected_bool_value(bool b_value) { flag_.expected_bool_value_ = b_value; }
  OB_INLINE bool expected_bool_value() const { return flag_.expected_bool_value_; }

  // others.
  INHERIT_TO_STRING_KV(N_OBJ, ObObj, N_ACCURACY, accuracy_, N_FLAG, res_flags_);
  NEED_SERIALIZE_AND_DESERIALIZE;
private:
  ObAccuracy accuracy_;
  uint32_t res_flags_;  // BINARY, NUM, NOT_NULL, TIMESTAMP, etc
                        // reference: src/lib/regex/include/mysql_com.h
  ParamFlag flag_;
};

typedef int (*ob_obj_print)(const ObObj &obj, char *buffer, int64_t length, int64_t &pos, const ObObjPrintParams &params);
typedef int64_t (*ob_obj_crc64)(const ObObj &obj, const int64_t current);
typedef void (*ob_obj_batch_checksum)(const ObObj &obj, ObBatchChecksum &bc);
typedef uint64_t (*ob_obj_hash)(const ObObj &obj, const uint64_t hash);
typedef int (*ob_obj_value_serialize)(const ObObj &obj, char* buf, const int64_t buf_len, int64_t& pos);
typedef int (*ob_obj_value_deserialize)(ObObj &obj, const char* buf, const int64_t data_len, int64_t& pos);
typedef int64_t (*ob_obj_value_get_serialize_size)(const ObObj &obj);

class ObHexEscapeSqlStr
{
public:
  ObHexEscapeSqlStr(const common::ObString &str) : str_(str) { }
  ObString str() const { return str_; }
  int64_t get_extra_length() const;
  DECLARE_TO_STRING;
private:
  ObString str_;
};

}
}

#endif //
