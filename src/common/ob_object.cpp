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

#include <string.h>
#include <algorithm>
#include <math.h>  // for fabs, fabsf
#define USING_LOG_PREFIX COMMON
#include "common/ob_object.h"
#include "lib/utility/serialization.h"
#include "lib/tbsys.h"
#include "common/ob_action_flag.h"
#include "lib/utility/utility.h"
#include "lib/checksum/ob_crc64.h"
#include "common/ob_obj_cast.h"
#include "common/ob_obj_compare.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/allocator/ob_stack_allocator.h"
#include "lib/number/ob_number_v2.h"
#include "common/ob_obj_funcs.h"

using namespace oceanbase;
using namespace oceanbase::common;

#define PRINT_META()
//#define PRINT_META() BUF_PRINTO(obj.get_meta()); J_COLON();

const char *ObObj::MIN_OBJECT_VALUE_STR       = "__OB__MIN__";
const char *ObObj::MAX_OBJECT_VALUE_STR       = "__OB__MAX__";
const char *ObObj::NOP_VALUE_STR = "__OB__NOP__";

DEFINE_SERIALIZE(ObObjMeta)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(type_);
  OB_UNIS_ENCODE(cs_level_);
  OB_UNIS_ENCODE(cs_type_);
  OB_UNIS_ENCODE(scale_);
  return ret;
}

DEFINE_DESERIALIZE(ObObjMeta)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(type_);
  OB_UNIS_DECODE(cs_level_);
  OB_UNIS_DECODE(cs_type_);
  OB_UNIS_DECODE(scale_);
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObObjMeta)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(type_);
  OB_UNIS_ADD_LEN(cs_level_);
  OB_UNIS_ADD_LEN(cs_type_);
  OB_UNIS_ADD_LEN(scale_);
  return len;
}

////////////////////////////////////////////////////////////////

int ObObj::is_true(bool /*range_check*/, bool &is_true) const
{
  int ret = OB_SUCCESS;
  if (ObTinyIntType == get_type()) {
    is_true = ObObj::is_true();
  } else if (is_zero() || is_null()) {
    is_true = false;
  } else {
    StackAllocator allocator;
    ObCastMode cast_mode = CM_WARN_ON_FAIL /*| (range_check ? CM_NONE : CM_NO_RANGE_CHECK)*/;
    ObCastCtx cast_ctx(&allocator, NULL, cast_mode, CS_TYPE_INVALID);
    ObObj buf_obj;
    const ObObj *res_obj = NULL;
    if (OB_FAIL(ObObjCasterV2::to_type(ObTinyIntType, cast_ctx, *this, buf_obj, res_obj))) {
      _OB_LOG(WARN, "failed to cast object to tinyint");
    } else {
      is_true = !res_obj->is_zero();
    }
  }
  return ret;
}

bool ObObj::is_zero() const
{
  bool ret = is_numeric_type();
  if (ret) {
    switch(meta_.get_type()) {
      case ObTinyIntType:
        // fall through
      case ObSmallIntType:
        // fall through
      case ObMediumIntType:
        // fall through
      case ObInt32Type:
        // fall through
      case ObIntType:
        ret = (0 == v_.int64_);
        break;
      case ObUTinyIntType:
        // fall through
      case ObUSmallIntType:
        // fall through
      case ObUMediumIntType:
        // fall through
      case ObUInt32Type:
        // fall through
      case ObUInt64Type:
        ret = (0 == v_.uint64_);
        break;
      case ObFloatType:
        ret = (fabsf(v_.float_) < OB_FLOAT_EPSINON);
        break;
      case ObDoubleType:
        ret = (fabs(v_.double_) < OB_DOUBLE_EPSINON);
        break;
      case ObUFloatType:
        ret = (v_.float_ < OB_FLOAT_EPSINON);
        break;
      case ObUDoubleType:
        ret = (v_.double_ < OB_DOUBLE_EPSINON);
        break;
      case ObNumberType:
        // fall through
      case ObUNumberType: {
        number::ObNumber nmb = get_number();
        ret = nmb.is_zero();
        break;
      }
      default:
        BACKTRACE(ERROR, true, "unexpected numeric type=%hhd", meta_.get_type());
        right_to_die_or_duty_to_live();
    }
  }
  return ret;
}

int ObObj::build_not_strict_default_value()
{
  int ret = OB_SUCCESS;
  const ObObjType &data_type = meta_.get_type();
  switch(data_type) {
    case ObTinyIntType:
      set_tinyint(0);
      break;
    case ObSmallIntType:
      set_smallint(0);
      break;
    case ObMediumIntType:
      set_mediumint(0);
      break;
    case ObInt32Type:
      set_int32(0);
      break;
    case ObIntType:
      set_int(0);
      break;
    case ObUTinyIntType:
      set_utinyint(0);
      break;
    case ObUSmallIntType:
      set_usmallint(0);
      break;
    case ObUMediumIntType:
      set_umediumint(0);
      break;
    case ObUInt32Type:
      set_uint32(0);
      break;
    case ObUInt64Type:
      set_uint64(0);
      break;
    case ObFloatType:
      set_float(0);
      break;
    case ObDoubleType:
      set_double(0);
      break;
    case ObUFloatType:
      set_ufloat(0);
      break;
    case ObUDoubleType:
      set_udouble(0);
      break;
    case ObNumberType: {
      number::ObNumber zero;
      zero.set_zero();
      set_number(zero);
    }
      break;
    case ObUNumberType: {
      number::ObNumber zero;
      zero.set_zero();
      set_unumber(zero);
    }
      break;
    case ObDateTimeType:
      set_datetime(ObTimeConverter::ZERO_DATETIME);
      break;
    case ObTimestampType:
      set_timestamp(ObTimeConverter::ZERO_DATETIME);
      break;
    case ObDateType:
      set_date(ObTimeConverter::ZERO_DATE);
      break;
    case ObTimeType:
      set_time(0);
      break;
    case ObYearType:
      set_year(0);
      break;
    case ObVarcharType: {
        ObString null_str;
        set_varchar(null_str);
      }
      break;
    case ObCharType: {
        ObString null_str;
        set_char(null_str);
      }
      break;
    case ObTimestampTZType:
    case ObTimestampLTZType:
    case ObTimestampNanoType: {
        set_otimestamp_null(data_type);
        break;
    }
    default:
      ret = OB_INVALID_ARGUMENT;
      _OB_LOG(WARN, "unexpected data type=%hhd", data_type);
  }
  return ret;
}

int64_t ObObj::get_deep_copy_size() const
{
  int64_t ret = 0;
  if (is_string_type()) {
    ret += val_len_;
  } else if (ob_is_number_tc(get_type())) {
    ret += (sizeof(uint32_t) * nmb_desc_.len_);
  }
  return ret;
}

int ObObj::deep_copy(const ObObj &src, char *buf, const int64_t size, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (ob_is_string_tc(src.get_type())) {
    ObString src_str = src.get_string();
    if (size < (pos + src_str.length())) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      MEMCPY(buf + pos, src_str.ptr(), src_str.length());
      ObString dest_str;
      dest_str.assign_ptr(buf + pos, src_str.length());
      this->set_varchar(dest_str);
      this->set_type(src.get_type());
//      this->set_collation_level(src.get_collation_level());
      this->set_collation_type(src.get_collation_type());
      pos += src_str.length();
    }
  } else if (ObNumberTC == src.get_type_class()) {
    number::ObNumber src_nmb;
    src_nmb = src.get_number();
    if (size < (int64_t)(pos + sizeof(uint32_t) * src_nmb.get_length())) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      MEMCPY(buf + pos, src_nmb.get_digits(), sizeof(uint32_t) * src_nmb.get_length());
      number::ObNumber dest_nmb;
      dest_nmb.assign(src_nmb.get_desc(), (uint32_t *)(buf + pos));
      *this = src;
      this->set_number(src.get_type(), dest_nmb);
      pos += (sizeof(uint32_t) * src_nmb.get_length());
    }
  } else {
    *this = src;
  }
  return ret;
}

int ObObj::compare(const ObObj &other, ObCollationType cs_type/*COLLATION_TYPE_MAX*/) const
{
  return ObObjCmpFuncs::compare_nullsafe(*this, other, cs_type);
}

bool ObObj::is_equal(const ObObj &other, ObCollationType cs_type) const
{
  return ObObjCmpFuncs::compare_oper_nullsafe(*this, other, cs_type, CO_EQ);
}

bool ObObj::operator<(const ObObj &other) const
{
  return ObObjCmpFuncs::compare_oper_nullsafe(*this, other, CS_TYPE_INVALID, CO_LT);
}

bool ObObj::operator>(const ObObj &other) const
{
  return ObObjCmpFuncs::compare_oper_nullsafe(*this, other, CS_TYPE_INVALID, CO_GT);
}

bool ObObj::operator>=(const ObObj &other) const
{
  return ObObjCmpFuncs::compare_oper_nullsafe(*this, other, CS_TYPE_INVALID, CO_GE);
}

bool ObObj::operator<=(const ObObj &other) const
{
  return ObObjCmpFuncs::compare_oper_nullsafe(*this, other, CS_TYPE_INVALID, CO_LE);
}

bool ObObj::operator==(const ObObj &other) const
{
  return ObObjCmpFuncs::compare_oper_nullsafe(*this, other, CS_TYPE_INVALID, CO_EQ);
}

bool ObObj::operator!=(const ObObj &other) const
{
  return ObObjCmpFuncs::compare_oper_nullsafe(*this, other, CS_TYPE_INVALID, CO_NE);
}

int ObObj::apply(const ObObj &mutation)
{
  int ret = OB_SUCCESS;
  int org_type = get_type();
  int mut_type = mutation.get_type();
  if (OB_UNLIKELY(ObMaxType <= mut_type
                  || (ObExtendType != org_type
                      && ObNullType != org_type
                      && ObExtendType != mut_type
                      && ObNullType != mut_type
                      && org_type != mut_type))) {
    _OB_LOG(WARN, "type not coincident or invalid type[this->type:%d,mutation.type:%d]",
              org_type, mut_type);
    ret = OB_INVALID_ARGUMENT;
  } else {
    switch (mut_type) {
      case ObNullType:
        set_null();
        break;
      case ObExtendType: {
        int64_t org_ext = get_ext();
        switch (mutation.get_ext()) {
          case ObActionFlag::OP_DEL_ROW:
          case ObActionFlag::OP_DEL_TABLE:
            /// used for join, if right row was deleted, set the cell to null
            set_null();
            break;
          case ObActionFlag::OP_ROW_DOES_NOT_EXIST:
            /// do nothing
            break;
          case ObActionFlag::OP_NOP:
            if (org_ext == ObActionFlag::OP_ROW_DOES_NOT_EXIST
                || org_ext == ObActionFlag::OP_DEL_ROW) {
              set_null();
            }
            break;
          default:
            ret = OB_INVALID_ARGUMENT;
            _OB_LOG(ERROR, "unsupported ext value [value:%ld]", mutation.get_ext());
            break;
        }  // end switch
        break;
      }
      default:
        *this = mutation;
        break;
    }  // end switch
  }
  return ret;
}

////////////////////////////////////////////////////////////////
#define DEF_FUNC_ENTRY(OBJTYPE)                 \
  {                                             \
      obj_print_sql<OBJTYPE>,                   \
      obj_print_str<OBJTYPE>,                   \
      obj_print_plain_str<OBJTYPE>,             \
      obj_print_json<OBJTYPE>,                  \
      obj_crc64<OBJTYPE>,                       \
      obj_batch_checksum<OBJTYPE>,              \
      obj_murmurhash<OBJTYPE>,                  \
      ObjHashCalculator<OBJTYPE, ObDefaultHash, ObObj>::calc_hash_value,  \
      obj_val_serialize<OBJTYPE>,               \
      obj_val_deserialize<OBJTYPE>,             \
      obj_val_get_serialize_size<OBJTYPE>,      \
      ObjHashCalculator<OBJTYPE, ObMurmurHash, ObObj>::calc_hash_value,  \
  }

ObObjTypeFuncs OBJ_FUNCS[ObMaxType] =
{
  DEF_FUNC_ENTRY(ObNullType),  // 0
  DEF_FUNC_ENTRY(ObTinyIntType),  // 1
  DEF_FUNC_ENTRY(ObSmallIntType), // 2
  DEF_FUNC_ENTRY(ObMediumIntType),  // 3
  DEF_FUNC_ENTRY(ObInt32Type),      // 4
  DEF_FUNC_ENTRY(ObIntType),        // 5
  DEF_FUNC_ENTRY(ObUTinyIntType),   // 6
  DEF_FUNC_ENTRY(ObUSmallIntType),  // 7
  DEF_FUNC_ENTRY(ObUMediumIntType), // 8
  DEF_FUNC_ENTRY(ObUInt32Type),     // 9
  DEF_FUNC_ENTRY(ObUInt64Type),     // 10
  DEF_FUNC_ENTRY(ObFloatType),      // 11
  DEF_FUNC_ENTRY(ObDoubleType),     // 12
  DEF_FUNC_ENTRY(ObUFloatType),     // 13
  DEF_FUNC_ENTRY(ObUDoubleType),    // 14
  DEF_FUNC_ENTRY(ObNumberType),     // 15
  DEF_FUNC_ENTRY(ObUNumberType),  // 16: unumber is the same as number
  DEF_FUNC_ENTRY(ObDateTimeType),  // 17
  DEF_FUNC_ENTRY(ObTimestampType), // 18
  DEF_FUNC_ENTRY(ObDateType),  // 19
  DEF_FUNC_ENTRY(ObTimeType),  // 20
  DEF_FUNC_ENTRY(ObYearType),  // 21
  DEF_FUNC_ENTRY(ObVarcharType),  // 22, varchar
  DEF_FUNC_ENTRY(ObCharType),     // 23, char
  DEF_FUNC_ENTRY(ObHexStringType),  // 24, hex_string
  DEF_FUNC_ENTRY(ObExtendType),  // 25, ext
  DEF_FUNC_ENTRY(ObUnknownType),  // 26, unknown
  DEF_FUNC_ENTRY(ObNullType),          // 27
  DEF_FUNC_ENTRY(ObNullType),          // 28
  DEF_FUNC_ENTRY(ObNullType),          // 29
  DEF_FUNC_ENTRY(ObNullType),          // 30
  DEF_FUNC_ENTRY(ObNullType),          // 31
  DEF_FUNC_ENTRY(ObNullType),          // 32
  DEF_FUNC_ENTRY(ObNullType),          // 33
  DEF_FUNC_ENTRY(ObNullType),          // 34
  DEF_FUNC_ENTRY(ObNullType),          // 35
  DEF_FUNC_ENTRY(ObTimestampTZType),   // 36, timestamp with time zone
  DEF_FUNC_ENTRY(ObTimestampLTZType),  // 37, timestamp with local time zone
  DEF_FUNC_ENTRY(ObTimestampNanoType), // 38, timestamp (9)
  DEF_FUNC_ENTRY(ObRawType),           // 39, raw
  DEF_FUNC_ENTRY(ObNullType),          // 40
  DEF_FUNC_ENTRY(ObNullType),          // 41
  DEF_FUNC_ENTRY(ObNumberFloatType),   // 42
  DEF_FUNC_ENTRY(ObNVarchar2Type),     // 43, nvarchar2
  DEF_FUNC_ENTRY(ObNCharType),         // 44, nchar
};

////////////////////////////////////////////////////////////////
int ObObj::print_sql_literal(char *buffer, int64_t length, int64_t &pos,const ObTimeZoneInfo *tz_info) const
{
  return OBJ_FUNCS[meta_.get_type()].print_sql(*this, buffer, length, pos, tz_info);
}

//used for show create table default value
//for example:
// `a` int(11) NOT NULL DEFAULT '0'  (with '')
//always with ''
int ObObj::print_varchar_literal(char *buffer, int64_t length, int64_t &pos, const ObTimeZoneInfo *tz_info) const
{
  return OBJ_FUNCS[meta_.get_type()].print_str(*this, buffer, length, pos, tz_info);
}

int ObObj::print_plain_str_literal(char *buffer, int64_t length, int64_t &pos, const ObTimeZoneInfo *tz_info) const
{
  return OBJ_FUNCS[meta_.get_type()].print_plain_str(*this, buffer, length, pos, tz_info);
}

void ObObj::print_str_with_repeat(char *buf, int64_t buf_len, int64_t &pos) const
{
  const unsigned char *uptr = reinterpret_cast<const unsigned char*>(v_.string_);
  int32_t real_len = val_len_;
  int32_t repeats = 0;
  int8_t cnt_space = 0;//There is no space for whole multibyte character, then add trailing spaces.
  if (NULL != uptr) {
    while (' ' == uptr[real_len - 1]) {
      --real_len;
      ++cnt_space;
    }
    // for utf-8 character set, pad BFBFEF as the tailing characters in a loop
    while (real_len - 2 > 0 && 0xBF == uptr[real_len - 1]  && 0xBF == uptr[real_len - 2]  && 0xEF == uptr[real_len - 3]) {
      real_len -= 3;
      ++repeats;
    }
  }
  if (0 == repeats) {
    real_len = val_len_;
  }
  BUF_PRINTO(ObString(0, real_len, v_.string_));
  if (repeats > 0) {
    BUF_PRINTF(" \'<%X%X%X><repeat %d times>\' ", uptr[real_len], uptr[real_len + 1], uptr[real_len + 2], repeats);
    //There is no space for whole multibyte character, then add trailing spaces.
    if (1 == cnt_space) {
      BUF_PRINTO(" ");
    } else if (2 == cnt_space){
      BUF_PRINTO("  ");
    }
  }
}

void ObObj::print_range_value(char *buf, int64_t buf_len, int64_t &pos) const
{
  if (is_string_type()) {
    J_OBJ_START();
    BUF_PRINTO(ob_obj_type_str(this->get_type()));
    J_COLON();
    //for Unicode character set
    print_str_with_repeat(buf, buf_len, pos);
    J_COMMA();
    J_KV(N_COLLATION, ObCharset::collation_name(this->get_collation_type()));
    J_OBJ_END();
  } else {
    (void)databuff_print_obj(buf, buf_len, pos, *this);
  }
}

int64_t ObObj::to_string(char *buf, const int64_t buf_len, const ObTimeZoneInfo *tz_info) const
{
  int64_t pos = 0;
  if (get_type() < ObMaxType && get_type() >= ObNullType) {
    (void)OBJ_FUNCS[meta_.get_type()].print_json(*this, buf, buf_len, pos, tz_info);
  }
  return pos;
}

bool ObObj::check_collation_integrity() const
{
  bool is_ok = true;
  if (ObNullType == get_type()) {
    // ignore null
    //is_ok = (CS_TYPE_BINARY == get_collation_type() && CS_LEVEL_IGNORABLE == get_collation_level());
  } else if (ob_is_numeric_type(get_type()) || ob_is_temporal_type(get_type())){
    is_ok = (CS_TYPE_BINARY == get_collation_type() && CS_LEVEL_NUMERIC == get_collation_level());
  } else {
    // ignore: varchar, char, binary, varbinary, unknown, ext
  }
  if (!is_ok) {
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      BACKTRACE(WARN, true, "unexpected collation type: %s", to_cstring(get_meta()));
    }
  }
  return is_ok;
}

uint64_t ObObj::hash_v1(uint64_t seed) const
{
  check_collation_integrity();
  return OBJ_FUNCS[meta_.get_type()].murmurhash(*this, seed);
}

uint64_t ObObj::hash(uint64_t seed) const
{
  check_collation_integrity();
  return OBJ_FUNCS[meta_.get_type()].murmurhash_v2(*this, seed);
}

uint64_t ObObj::hash_murmur(uint64_t seed) const
{
  check_collation_integrity();
  return OBJ_FUNCS[meta_.get_type()].murmurhash_v3(*this, seed);
}


int64_t ObObj::checksum(const int64_t current) const
{
  check_collation_integrity();
  return OBJ_FUNCS[meta_.get_type()].crc64(*this, current);
}

void ObObj::checksum(ObBatchChecksum &bc) const
{
  check_collation_integrity();
  OBJ_FUNCS[meta_.get_type()].batch_checksum(*this, bc);
}

void ObObj::dump(const int32_t log_level /*= OB_LOG_LEVEL_DEBUG*/) const
{
  _OB_NUM_LEVEL_LOG(log_level, "%s", S(*this));
}

////////////////////////////////////////////////////////////////
DEFINE_SERIALIZE(ObObj)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(meta_);
  if (OB_SUCC(ret)) {
    if (meta_.is_invalid()) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret = OBJ_FUNCS[meta_.get_type()].serialize(*this, buf, buf_len, pos);
    }
  }
  return ret;
}

DEFINE_DESERIALIZE(ObObj)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(meta_);
  if (OB_SUCC(ret)) {
    if (meta_.is_invalid()) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret = OBJ_FUNCS[meta_.get_type()].deserialize(*this, buf, data_len, pos);
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObObj)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(meta_);
  len += OBJ_FUNCS[meta_.get_type()].get_serialize_size(*this);
  return len;
}

OB_SERIALIZE_MEMBER_INHERIT(ObObjParam, ObObj, accuracy_, res_flags_);

void ObObjParam::reset()
{
  accuracy_.reset();
  res_flags_ = 0;
  flag_.reset();
}

void ObObjParam::ParamFlag::reset()
{
  need_to_check_type_ = true;
  need_to_check_bool_value_ = false;
  expected_bool_value_ = false;
}

DEF_TO_STRING(ObHexEscapeSqlStr)
{
  int64_t buf_pos = 0;
  if (buf != NULL && buf_len > 0 && !str_.empty()) {
    const char *end = str_.ptr() + str_.length();
    if (lib::is_oracle_mode()) {
      for (const char *cur = str_.ptr(); cur < end && buf_pos < buf_len; ++cur) {
        if ('\'' == *cur) {
          buf[buf_pos++] = '\'';
          if (buf_pos < buf_len) {
            buf[buf_pos++] = *cur;
          }
        } else {
          buf[buf_pos++] = *cur;
        }
      }
    } else {
      for (const char *cur = str_.ptr(); cur < end && buf_pos < buf_len; ++cur) {
        switch (*cur) {
          case '\\': {
            buf[buf_pos++] = '\\';
            if (buf_pos < buf_len) {
              buf[buf_pos++] = '\\';
            }
            break;
          }
          case '\0': {
            buf[buf_pos++] = '\\';
            if (buf_pos < buf_len) {
              buf[buf_pos++] = '0';
            }
            break;
          }
          case '\'':
          case '\"': {
            buf[buf_pos++] = '\\';
            if (buf_pos < buf_len) {
              buf[buf_pos++] = *cur;
            }
            break;
          }
          case '\n': {
            buf[buf_pos++] = '\\';
            if (buf_pos < buf_len) {
              buf[buf_pos++] = 'n';
            }
            break;
          }
          case '\r': {
            buf[buf_pos++] = '\\';
            if (buf_pos < buf_len) {
              buf[buf_pos++] = 'r';
            }
            break;
          }
          case '\t': {
            buf[buf_pos++] = '\\';
            if (buf_pos < buf_len) {
              buf[buf_pos++] = 't';
            }
            break;
          }
          default: {
            buf[buf_pos++] = *cur;
            break;
          }
        }
      }
    }
  }
  return buf_pos;
}

int64_t ObHexEscapeSqlStr::get_extra_length() const
{
  int64_t ret_length = 0;
  if (!str_.empty()) {
    const char *end = str_.ptr() + str_.length();
    if (lib::is_oracle_mode()) {
      for (const char *cur = str_.ptr(); cur < end; ++cur) {
        if ('\'' == *cur) {
          ++ret_length;
        }
      }
    } else {
      for (const char *cur = str_.ptr(); cur < end; ++cur) {
        switch (*cur) {
          case '\\':
          case '\0':
          case '\'':
          case '\"':
          case '\n':
          case '\r':
          case '\t': {
            ++ret_length;
            break;
          }
          default: {
            //do nothing
          }
        }
      }
    }
  }
  return ret_length;
}
