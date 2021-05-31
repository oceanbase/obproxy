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

#include <float.h>
#include "lib/ob_define.h"
#include "common/ob_obj_type.h"
#include "lib/charset/ob_charset.h"
namespace oceanbase
{
namespace common
{
const char *ob_obj_type_str(ObObjType type)
{
  return ob_sql_type_str(type);
}

const char *ob_obj_tc_str(ObObjTypeClass tc)
{
  return ob_sql_tc_str(tc);
}

const char *ob_sql_type_str(ObObjType type)
{
  static const char *sql_type_name[ObMaxType+1] =
  {
    "NULL",

    "TINYINT",
    "SMALLINT",
    "MEDIUMINT",
    "INT",
    "BIGINT",

    "TINYINT UNSIGNED",
    "SMALLINT UNSIGNED",
    "MEDIUMINT UNSIGNED",
    "INT UNSIGNED",
    "BIGINT UNSIGNED",

    "FLOAT",
    "DOUBLE",

    "FLOAT UNSIGNED",
    "DOUBLE UNSIGNED",

    "DECIMAL",
    "DECIMAL UNSIGNED",

    "DATETIME",
    "TIMESTAMP",
    "DATE",
    "TIME",
    "YEAR",

    "VARCHAR",
    "CHAR",
    "HEX_STRING",

    "EXT",
    "UNKNOWN",

    "TINYTEXT",
    "TEXT",
    "MEDIUMTEXT",
    "LONGTEXT",
    ""
  };
  return sql_type_name[OB_LIKELY(type < ObMaxType) ? type : ObMaxType];
}

typedef int (*obSqlTypeStrFunc)(char *buff, int64_t buff_length, int64_t length, int64_t precision, int64_t scale, ObCollationType coll_type);

//For date, null/unkown/max/extend
#define DEF_TYPE_STR_FUNCS(TYPE, STYPE1, STYPE2)                                                        \
  int ob_##TYPE##_str(char *buff, int64_t buff_length, int64_t length, int64_t precision, int64_t scale, ObCollationType coll_type)\
  {                                                                                                     \
    int ret = OB_SUCCESS;                                                                               \
    UNUSED(length);                                                                                     \
    UNUSED(precision);                                                                                  \
    UNUSED(scale);                                                                                      \
    UNUSED(coll_type);                                                  \
    int64_t pos = 0;                                                                                    \
    ret = databuff_printf(buff, buff_length, pos, STYPE1 STYPE2);                                       \
    return ret;                                                                                         \
  }

//For tinyint/smallint/mediumint/int/bigint (unsigned), year
#define DEF_TYPE_STR_FUNCS_PRECISION(TYPE, STYPE1, STYPE2)                                              \
  int ob_##TYPE##_str(char *buff, int64_t buff_length, int64_t length, int64_t precision, int64_t scale, ObCollationType coll_type)\
  {                                                                                                     \
    int ret = OB_SUCCESS;                                                                               \
    UNUSED(length);                                                                                     \
    UNUSED(scale);                                                                                      \
    UNUSED(coll_type);                                                  \
    int64_t pos = 0;                                                                                    \
    if (precision < 0) {                                                                                \
      ret = databuff_printf(buff, buff_length, pos, STYPE1 STYPE2);                                     \
    } else {                                                                                            \
      ret = databuff_printf(buff, buff_length, pos, STYPE1 "(%ld)" STYPE2, precision);                  \
    }                                                                                                   \
    return ret;                                                                                         \
  }

//For datetime/timestamp/time
#define DEF_TYPE_STR_FUNCS_SCALE(TYPE, STYPE1, STYPE2)                                                  \
  int ob_##TYPE##_str(char *buff, int64_t buff_length, int64_t length, int64_t precision, int64_t scale, ObCollationType coll_type) \
  {                                                                                                     \
    int ret = OB_SUCCESS;                                                                               \
    UNUSED(length);                                                                                     \
    UNUSED(precision);                                                                                  \
    UNUSED(coll_type);                                                  \
    int64_t pos = 0;                                                                                    \
    if (scale <= 0) {                                                                                   \
      ret = databuff_printf(buff, buff_length, pos, STYPE1 STYPE2);                                     \
    } else {                                                                                            \
      ret = databuff_printf(buff, buff_length, pos, STYPE1 "(%ld)" STYPE2, scale);                      \
    }                                                                                                   \
    return ret;                                                                                         \
  }

//For number/float/double (unsigned)
#define DEF_TYPE_STR_FUNCS_PRECISION_SCALE(TYPE, STYPE1, STYPE2)                                                       \
  int ob_##TYPE##_str(char *buff, int64_t buff_length, int64_t length, int64_t precision, int64_t scale, ObCollationType coll_type) \
  {                                                                                                     \
    int ret = OB_SUCCESS;                                                                               \
    UNUSED(length);                                                                                     \
    UNUSED(coll_type);                                                  \
    int64_t pos = 0;                                                                                    \
    if (precision < 0 && scale < 0) {                                                                   \
      ret = databuff_printf(buff, buff_length, pos, STYPE1 STYPE2);                                     \
    } else {                                                                                            \
      ret = databuff_printf(buff, buff_length, pos, STYPE1 "(%ld,%ld)" STYPE2, precision, scale);       \
    }                                                                                                   \
    return ret;                                                                                         \
}

//For char/varchar
#define DEF_TYPE_STR_FUNCS_LENGTH(TYPE, STYPE1, STYPE2)                 \
  int ob_##TYPE##_str(char *buff, int64_t buff_length, int64_t length, int64_t precision, int64_t scale, ObCollationType coll_type) \
  {                                                                     \
    int ret = OB_SUCCESS;                                               \
    UNUSED(precision);                                                  \
    UNUSED(scale) ;                                                     \
    int64_t pos = 0;                                                    \
    if (CS_TYPE_BINARY == coll_type) {                                  \
      ret = databuff_printf(buff, buff_length, pos, STYPE2 "(%ld)", length); \
    } else {                                                            \
      ret = databuff_printf(buff, buff_length, pos, STYPE1 "(%ld)", length); \
    }                                                                   \
    return ret;                                                         \
  }


DEF_TYPE_STR_FUNCS(null, "null", "");
DEF_TYPE_STR_FUNCS_PRECISION(tinyint, "tinyint", "");
DEF_TYPE_STR_FUNCS_PRECISION(smallint, "smallint", "");
DEF_TYPE_STR_FUNCS_PRECISION(mediumint, "mediumint", "");
DEF_TYPE_STR_FUNCS_PRECISION(int, "int", "");
DEF_TYPE_STR_FUNCS_PRECISION(bigint, "bigint", "");
DEF_TYPE_STR_FUNCS_PRECISION(utinyint, "tinyint", " unsigned");
DEF_TYPE_STR_FUNCS_PRECISION(usmallint, "smallint", " unsigned");
DEF_TYPE_STR_FUNCS_PRECISION(umediumint, "mediumint", " unsigned");
DEF_TYPE_STR_FUNCS_PRECISION(uint, "int", " unsigned");
DEF_TYPE_STR_FUNCS_PRECISION(ubigint, "bigint", " unsigned");
DEF_TYPE_STR_FUNCS_PRECISION_SCALE(float, "float", "");
DEF_TYPE_STR_FUNCS_PRECISION_SCALE(double, "double", "");
DEF_TYPE_STR_FUNCS_PRECISION_SCALE(ufloat, "float", " unsigned");
DEF_TYPE_STR_FUNCS_PRECISION_SCALE(udouble, "double", " unsigned");
DEF_TYPE_STR_FUNCS_PRECISION_SCALE(number, "decimal", "");
DEF_TYPE_STR_FUNCS_PRECISION_SCALE(unumber, "decimal", " unsigned");
DEF_TYPE_STR_FUNCS_SCALE(datetime, "datetime", "");
DEF_TYPE_STR_FUNCS_SCALE(timestamp, "timestamp", "");
DEF_TYPE_STR_FUNCS(date, "date", "");
DEF_TYPE_STR_FUNCS_SCALE(time, "time", "");
DEF_TYPE_STR_FUNCS_PRECISION(year, "year", "");
DEF_TYPE_STR_FUNCS_LENGTH(varchar, "varchar", "varbinary");
DEF_TYPE_STR_FUNCS_LENGTH(char, "char", "binary");
DEF_TYPE_STR_FUNCS_LENGTH(hex_string, "hex_string", "hex_string");
DEF_TYPE_STR_FUNCS(extend, "ext", "");
DEF_TYPE_STR_FUNCS(unknown, "unknown", "");
DEF_TYPE_STR_FUNCS_LENGTH(tinytext, "tinytext", "tinyblob");
DEF_TYPE_STR_FUNCS_LENGTH(text, "text", "blob");
DEF_TYPE_STR_FUNCS_LENGTH(mediumtext, "mediumtext", "mediumblob");
DEF_TYPE_STR_FUNCS_LENGTH(longtext, "longtext", "longblob");

int ob_empty_str(char *buff, int64_t buff_length, int64_t length, int64_t precision, int64_t scale, ObCollationType coll_type)
{
  int ret = OB_SUCCESS;
  UNUSED(length);
  UNUSED(scale) ;
  UNUSED(precision);
  UNUSED(coll_type);
  if (buff_length > 0) {
    buff[0] = '\0';
  }
  return ret;
}


int ob_sql_type_str(char *buff, int64_t buff_length, ObObjType type, int64_t length, int64_t precision, int64_t scale, ObCollationType coll_type)
{
  static obSqlTypeStrFunc sql_type_name[ObMaxType+1] =
  {
    ob_null_str,  // NULL

    ob_tinyint_str,  // TINYINT
    ob_smallint_str, // SAMLLINT
    ob_mediumint_str,   // MEDIUM
    ob_int_str,      // INT
    ob_bigint_str,   // BIGINT

    ob_utinyint_str,  // TYIYINT UNSIGNED
    ob_usmallint_str, // SMALLINT UNSIGNED
    ob_umediumint_str,   // MEDIUM UNSIGNED
    ob_uint_str,      // INT UNSIGNED
    ob_ubigint_str,   // BIGINT UNSIGNED

    ob_float_str,  // FLOAT
    ob_double_str, // DOUBLE

    ob_ufloat_str,  // FLOAT UNSIGNED
    ob_udouble_str, // DOUBLE UNSIGNED

    ob_number_str,  // DECIMAL
    ob_unumber_str,  // DECIMAL UNSIGNED

    ob_datetime_str,  // DATETIME
    ob_timestamp_str, // TIMESTAMP
    ob_date_str,      // DATE
    ob_time_str,      // TIME
    ob_year_str,      // YEAR

    ob_varchar_str,  // VARCHAR
    ob_char_str,     // CHAR
    ob_hex_string_str,  // HEX_STRING

    ob_extend_str,  // EXT
    ob_unknown_str,  // UNKNOWN
    ob_tinytext_str, //TINYTEXT
    ob_text_str, //TEXT
    ob_mediumtext_str, //MEDIUMTEXT
    ob_longtext_str, //LONGTEXT
    ob_empty_str             // MAX
  };
  return sql_type_name[OB_LIKELY(type < ObMaxType) ? type : ObMaxType](buff, buff_length, length, precision, scale, coll_type);
}

//DEF_TYPE_STR_FUNCS(number, "decimal", "");
const char *ob_sql_tc_str(ObObjTypeClass tc)
{
  static const char *sql_tc_name[] =
  {
    "NULL",
    "INT",
    "UINT",
    "FLOAT",
    "DOUBLE",
    "DECIMAL",
    "DATETIME",
    "DATE",
    "TIME",
    "YEAR",
    "STRING",
    "EXT",
    "UNKNOWN",
    "TEXT",
    ""
  };
  return sql_tc_name[OB_LIKELY(tc < ObMaxTC) ? tc : ObMaxTC];
}

int32_t ob_obj_type_size(ObObjType type)
{
  int32_t size = 0;
  UNUSED(type);
  // @todo
  return size;
}

#ifndef INT24_MIN
#define INT24_MIN     (-8388607 - 1)
#endif
#ifndef INT24_MAX
#define INT24_MAX     (8388607)
#endif
#ifndef UINT24_MAX
#define UINT24_MAX    (16777215U)
#endif

const int64_t INT_MIN_VAL[ObMaxType] =
{
  0,        // null.
  INT8_MIN,
  INT16_MIN,
  INT24_MIN,
  INT32_MIN,
  INT64_MIN
};

const int64_t INT_MAX_VAL[ObMaxType] =
{
  0,        // null.
  INT8_MAX,
  INT16_MAX,
  INT24_MAX,
  INT32_MAX,
  INT64_MAX
};

const uint64_t UINT_MAX_VAL[ObMaxType] =
{
  0,              // null.
  0, 0, 0, 0, 0,  // int8, int16, int24, int32, int64.
  UINT8_MAX,
  UINT16_MAX,
  UINT24_MAX,
  UINT32_MAX,
  UINT64_MAX
};

const double REAL_MIN_VAL[ObMaxType] =
{
  0.0,                      // null.
  0.0, 0.0, 0.0, 0.0, 0.0,  // int8, int16, int24, int32, int64.
  0.0, 0.0, 0.0, 0.0, 0.0,  // uint8, uint16, uint24, uint32, uint64.
  -FLT_MAX,
  -DBL_MAX,
  0.0,
  0.0
};

const double REAL_MAX_VAL[ObMaxType] =
{
  0.0,                      // null.
  0.0, 0.0, 0.0, 0.0, 0.0,  // int8, int16, int24, int32, int64.
  0.0, 0.0, 0.0, 0.0, 0.0,  // uint8, uint16, uint24, uint32, uint64.
  FLT_MAX,
  DBL_MAX,
  FLT_MAX,
  DBL_MAX
};

} // common
} // oceanbase
