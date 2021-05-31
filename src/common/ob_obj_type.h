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

#ifndef OCEANBASE_COMMON_OB_OBJECT_TYPE_H_
#define OCEANBASE_COMMON_OB_OBJECT_TYPE_H_
#include "lib/utility/ob_print_utils.h"
#include "lib/charset/ob_charset.h"
namespace oceanbase
{
namespace common
{
// we can append new type only, do NOT delete nor change order,
// modify ObObjTypeClass and ob_obj_type_class when append new object type.
enum ObObjType
{
  ObNullType = 0,

  ObTinyIntType=1,                // int8, aka mysql boolean type
  ObSmallIntType=2,               // int16
  ObMediumIntType=3,              // int24
  ObInt32Type=4,                 // int32
  ObIntType=5,                    // int64, aka bigint

  ObUTinyIntType=6,                // uint8
  ObUSmallIntType=7,               // uint16
  ObUMediumIntType=8,              // uint24
  ObUInt32Type=9,                    // uint32
  ObUInt64Type=10,                 // uint64

  ObFloatType=11,                  // single-precision floating point
  ObDoubleType=12,                 // double-precision floating point

  ObUFloatType=13,            // unsigned single-precision floating point
  ObUDoubleType=14,           // unsigned double-precision floating point

  ObNumberType=15, // aka decimal/numeric
  ObUNumberType=16,

  ObDateTimeType=17,
  ObTimestampType=18,
  ObDateType=19,
  ObTimeType=20,
  ObYearType=21,

  ObVarcharType=22,  // charset: utf8mb4 or binary
  ObCharType=23,     // charset: utf8mb4 or binary

  ObHexStringType=24,  // hexadecimal literal, e.g. X'42', 0x42, b'1001', 0b1001

  ObExtendType=25,                 // Min, Max, NOP etc.
  ObUnknownType=26,                // For question mark(?) in prepared statement, no need to serialize
  // @note future new types to be defined here !!!
  ObTinyTextType    = 27,
  ObTextType        = 28,
  ObMediumTextType  = 29,
  ObLongTextType    = 30,
  ObBitType         = 31,
  ObEnumType        = 32,
  ObSetType         = 33,
  ObEnumInnerType   = 34,
  ObSetInnerType    = 35,

  /*
  ObTinyBlobType = 31,
  ObMediumBlobType = 32,
  ObBlobType = 33,
  ObLongBlobType = 34,

  ObBitType=35
  ObEnumType=36
  ObSetType=37
  */
  ObTimestampTZType   = 36, // timestamp with time zone for oracle
  ObTimestampLTZType  = 37, // timestamp with local time zone for oracle
  ObTimestampNanoType = 38, // timestamp nanosecond for oracle
  ObRawType           = 39, // raw type for oracle
  ObIntervalYMType    = 40, // interval year to month
  ObIntervalDSType    = 41, // interval day to second
  ObNumberFloatType   = 42, // oracle float, subtype of NUMBER
  ObNVarchar2Type     = 43, // nvarchar2
  ObNCharType         = 44, // nchar
  ObURowIDType        = 45, // UROWID
  ObMaxType                    // invalid type, or count of obj type
};

enum ObObjTypeClass
{
  ObNullTC      = 0,    // null
  ObIntTC       = 1,    // int8, int16, int24, int32, int64.
  ObUIntTC      = 2,    // uint8, uint16, uint24, uint32, uint64.
  ObFloatTC     = 3,    // float, ufloat.
  ObDoubleTC    = 4,    // double, udouble.
  ObNumberTC    = 5,    // number, unumber.
  ObDateTimeTC  = 6,    // datetime, timestamp.
  ObDateTC      = 7,    // date
  ObTimeTC      = 8,    // time
  ObYearTC      = 9,    // year
  ObStringTC    = 10,   // varchar, char, varbinary, binary.
  ObExtendTC    = 11,   // extend
  ObUnknownTC   = 12,   // unknown
  ObTextTC = 13, //TinyText,MediumText, Text ,LongText
  ObMaxTC,
  // invalid type classes are below, only used as the result of XXXX_type_promotion()
  // to indicate that the two obj can't be promoted to the same type.
  ObIntUintTC,
  ObLeftTypeTC,
  ObRightTypeTC,
  ObActualMaxTC
};

static ObObjTypeClass OBJ_TYPE_TO_CLASS[ObMaxType] =
{
  ObNullTC,         // null
  ObIntTC,          // int8
  ObIntTC,          // int16
  ObIntTC,          // int24
  ObIntTC,          // int32
  ObIntTC,          // int64
  ObUIntTC,         // uint8
  ObUIntTC,         // uint16
  ObUIntTC,         // uint24
  ObUIntTC,         // uint32
  ObUIntTC,         // uint64
  ObFloatTC,        // float
  ObDoubleTC,       // double
  ObFloatTC,        // ufloat
  ObDoubleTC,       // udouble
  ObNumberTC,       // number
  ObNumberTC,       // unumber
  ObDateTimeTC,     // datetime
  ObDateTimeTC,     // timestamp
  ObDateTC,         // date
  ObTimeTC,         // time
  ObYearTC,         // year
  ObStringTC,       // varchar
  ObStringTC,       // char
  ObStringTC,       // hexadecimal literal
  ObExtendTC,       // extend
  ObUnknownTC,      // unknown
  ObTextTC,         // TinyText
  ObTextTC,         // MediumText
  ObTextTC,         // Text
  ObTextTC          // LongText
};

static ObObjType OBJ_DEFAULT_TYPE[ObActualMaxTC] =
{
  ObNullType,       // null
  ObIntType,        // int
  ObUInt64Type,     // uint
  ObFloatType,      // float
  ObDoubleType,     // double
  ObNumberType,     // number
  ObDateTimeType,   // datetime
  ObDateType,       // date
  ObTimeType,       // time
  ObYearType,       // year
  ObVarcharType,    // varchar
  ObExtendType,     // extend
  ObUnknownType,    // unknown
  ObLongTextType,       // text
  ObMaxType,        // maxtype
  ObUInt64Type,     // int&uint
};

OB_INLINE ObObjTypeClass ob_obj_type_class(const ObObjType type)
{
  return type < ObMaxType ? OBJ_TYPE_TO_CLASS[type] : ObMaxTC;
}

OB_INLINE ObObjType ob_obj_default_type(const ObObjTypeClass tc)
{
  return tc < ObActualMaxTC ? OBJ_DEFAULT_TYPE[tc] : ObMaxType;
}

extern const int64_t INT_MIN_VAL[ObMaxType];
extern const int64_t INT_MAX_VAL[ObMaxType];
extern const uint64_t UINT_MAX_VAL[ObMaxType];
extern const double REAL_MIN_VAL[ObMaxType];
extern const double REAL_MAX_VAL[ObMaxType];

OB_INLINE bool is_valid_obj_type(const ObObjType type)
{
  return ObNullType <= type && type < ObMaxType;
}

OB_INLINE bool ob_is_castable_type_class(ObObjTypeClass tc)
{
  return (ObIntTC <= tc && tc <= ObStringTC) || ObLeftTypeTC == tc || ObRightTypeTC == tc;
}

//used for arithmetic
OB_INLINE bool ob_is_int_uint(ObObjTypeClass left_tc, ObObjTypeClass right_tc)
{
  return (ObIntTC == left_tc && ObUIntTC == right_tc) || (ObIntTC == right_tc && ObUIntTC == left_tc);
}

// print obj type string
const char *ob_obj_type_str(ObObjType type);
const char *ob_sql_type_str(ObObjType type);
int ob_sql_type_str(char *buff, int64_t buff_length, ObObjType type, int64_t length, int64_t precision, int64_t scale, ObCollationType coll_type);

// print obj type class string
const char *ob_obj_tc_str(ObObjTypeClass tc);
const char *ob_sql_tc_str(ObObjTypeClass tc);

// get obj type size for fixed length type
int32_t ob_obj_type_size(ObObjType type);

inline bool ob_is_valid_obj_type(ObObjType type) { return 0 <= type && type < ObMaxType; }
inline bool ob_is_invalid_obj_type(ObObjType type) { return !ob_is_valid_obj_type(type); }
inline bool ob_is_valid_obj_tc(ObObjTypeClass tc) { return 0 <= tc && tc < ObMaxTC; }
inline bool ob_is_invalid_obj_tc(ObObjTypeClass tc) { return !ob_is_valid_obj_tc(tc); }

// test type catalog
inline bool ob_is_integer_type(ObObjType type) { return type >= ObTinyIntType && type <= ObUInt64Type; }
inline bool ob_is_numeric_type(ObObjType type) { return type >= ObTinyIntType && type <= ObUNumberType; }
inline bool ob_is_real_type(ObObjType type) { return type >= ObFloatType && type <= ObUDoubleType;}

inline bool ob_is_string_type(ObObjType type) { return type >= ObVarcharType && type <= ObHexStringType; }
inline bool ob_is_temporal_type(ObObjType type) { return type >= ObDateTimeType && type <= ObYearType; }

inline bool ob_is_int_tc(ObObjType type) { return ObIntTC == ob_obj_type_class(type); }
inline bool ob_is_uint_tc(ObObjType type) { return ObUIntTC == ob_obj_type_class(type); }
inline bool ob_is_float_tc(ObObjType type) { return ObFloatTC == ob_obj_type_class(type); }
inline bool ob_is_double_tc(ObObjType type) { return ObDoubleTC == ob_obj_type_class(type); }
inline bool ob_is_number_tc(ObObjType type) { return ObNumberTC == ob_obj_type_class(type); }
inline bool ob_is_datetime_tc(ObObjType type) { return ObDateTimeTC == ob_obj_type_class(type); }
inline bool ob_is_time_tc(ObObjType type) { return ObTimeTC == ob_obj_type_class(type); }
inline bool ob_is_string_tc(ObObjType type) { return ObStringTC == ob_obj_type_class(type); }
inline bool ob_is_nvarchar2(const ObObjType type) { return ObNVarchar2Type == type; }
inline bool ob_is_nchar(const ObObjType type) { return ObNCharType == type; }
inline bool ob_is_nstring_type(const ObObjType type)
{
  return ob_is_nchar(type) || ob_is_nvarchar2(type);
}

inline bool is_obj_type_supported(ObObjType type)
{
  return type > ObNullType && type < ObUnknownType;
}

// to_string adapter
template<>
inline int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const ObObjType &t)
{
  return databuff_printf(buf, buf_len, pos, "\"%s\"", ob_obj_type_str(t));
}

template<>
inline int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                  const bool with_comma, const ObObjType &t)
{
  return databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:\"%s\""), key, ob_obj_type_str(t));
}

bool ob_can_static_cast(const ObObjType src, const ObObjType dst);
}  // end namespace common
}  // end namespace oceanbase

#endif //OCEANBASE_COMMON_OB_OBJECT_TYPE_H_
