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

#ifndef OCEANBASE_RPC_OBMYSQL_OB_MYSQL_GLOBAL_
#define OCEANBASE_RPC_OBMYSQL_OB_MYSQL_GLOBAL_

#include "easy_define.h"  // for conflict of macro likely
#include "lib/charset/ob_mysql_global.h"

#define float4get(V,M)   MEMCPY(&V, (M), sizeof(float))
#define float4store(V,M) MEMCPY(V, (&M), sizeof(float))

#define float8get(V,M)   doubleget((V),(M))
#define float8store(V,M) doublestore((V),(M))

#define SIZEOF_CHARP 8
#define NOT_FIXED_DEC 31

/*
  The longest string ob_fcvt can return is 311 + "precision" bytes.
  Here we assume that we never cal ob_fcvt() with precision >= NOT_FIXED_DEC
  (+ 1 byte for the terminating '\0').
*/
#define FLOATING_POINT_BUFFER (311 + NOT_FIXED_DEC)

#define DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE FLOATING_POINT_BUFFER
/* The fabs(float) < 10^39 */
#define FLOAT_TO_STRING_CONVERSION_BUFFER_SIZE (39 + NOT_FIXED_DEC)

#define MAX_DECPT_FOR_F_FORMAT DBL_DIG
#define MAX_DATETIME_WIDTH  19  /* YYYY-MM-DD HH:MM:SS */

/* -[digits].E+## */
#define MAX_FLOAT_STR_LENGTH (FLT_DIG + 6)
/* -[digits].E+### */
#define MAX_DOUBLE_STR_LENGTH (DBL_DIG + 7)

namespace oceanbase
{
namespace obmysql
{

// compat with mariadb_com
#define OB_MYSQL_NOT_NULL_FLAG (1 << 0)
#define OB_MYSQL_PRI_KEY_FLAG (1 << 1)
#define OB_MYSQL_UNIQUE_KEY_FLAG (1 << 2)
#define OB_MYSQL_MULTIPLE_KEY_FLAG (1 << 3)
#define OB_MYSQL_BLOB_FLAG (1 << 4)
#define OB_MYSQL_UNSIGNED_FLAG (1 << 5)
#define OB_MYSQL_ZEROFILL_FLAG (1 << 6)
#define OB_MYSQL_BINARY_FLAG (1 << 7)

// following are only sent to new clients
#define OB_MYSQL_ENUM_FLAG (1 << 8)
#define OB_MYSQL_AUTO_INCREMENT_FLAG (1 << 9)
#define OB_MYSQL_TIMESTAMP_FLAG (1 << 10)
#define OB_MYSQL_SET_FLAG (1 << 11)
#define OB_MYSQL_NO_DEFAULT_VALUE_FLAG (1 << 12)
#define OB_MYSQL_ON_UPDATE_NOW_FLAG (1 << 13)
#define OB_MYSQL_PART_KEY_FLAG (1 << 14)
#define OB_MYSQL_NUM_FLAG (1 << 15) //32768
#define OB_MYSQL_GROUP_FLAG (1 << 15) //32768
#define OB_MYSQL_UNIQUE_FLAG (1 << 16)
#define OB_MYSQL_BINCMP_FLAG (1 << 17)
#define OB_MYSQL_GET_FIXED_FIELDS_FLAG (1 << 18)
#define OB_MYSQL_FIELD_IN_PART_FUNC_FLAG (1 << 19)

enum EMySQLFieldType
{
  OB_MYSQL_TYPE_DECIMAL,
  OB_MYSQL_TYPE_TINY,
  OB_MYSQL_TYPE_SHORT,
  OB_MYSQL_TYPE_LONG,
  OB_MYSQL_TYPE_FLOAT,
  OB_MYSQL_TYPE_DOUBLE,
  OB_MYSQL_TYPE_NULL,
  OB_MYSQL_TYPE_TIMESTAMP,
  OB_MYSQL_TYPE_LONGLONG,
  OB_MYSQL_TYPE_INT24,
  OB_MYSQL_TYPE_DATE,
  OB_MYSQL_TYPE_TIME,
  OB_MYSQL_TYPE_DATETIME,
  OB_MYSQL_TYPE_YEAR,
  OB_MYSQL_TYPE_NEWDATE,
  OB_MYSQL_TYPE_VARCHAR,
  OB_MYSQL_TYPE_BIT,
  OB_MYSQL_TYPE_COMPLEX = 160,
  OB_MYSQL_TYPE_CURSOR = 163,
  OB_MYSQL_TYPE_OB_TIMESTAMP_WITH_TIME_ZONE = 200,
  OB_MYSQL_TYPE_OB_TIMESTAMP_WITH_LOCAL_TIME_ZONE = 201,
  OB_MYSQL_TYPE_OB_TIMESTAMP_NANO = 202,
  OB_MYSQL_TYPE_OB_RAW = 203,
  OB_MYSQL_TYPE_NEWDECIMAL = 246,
  OB_MYSQL_TYPE_ENUM = 247,
  OB_MYSQL_TYPE_SET = 248,
  OB_MYSQL_TYPE_TINY_BLOB = 249,
  OB_MYSQL_TYPE_MEDIUM_BLOB = 250,
  OB_MYSQL_TYPE_LONG_BLOB = 251,
  OB_MYSQL_TYPE_BLOB = 252,
  OB_MYSQL_TYPE_VAR_STRING = 253,
  OB_MYSQL_TYPE_STRING = 254,
  OB_MYSQL_TYPE_GEOMETRY = 255,
  OB_MYSQL_TYPE_NOT_DEFINED = 65535
};

} // end of namespace obmysql
} // end of namespace oceanbase

#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{
const char *get_emysql_field_type_str(const obmysql::EMySQLFieldType &type);

inline const char *get_emysql_field_type_str(const obmysql::EMySQLFieldType &type)
{
  const char *str = "UNKNOWN_TYPE";

  switch (type) {
    case obmysql::OB_MYSQL_TYPE_DECIMAL:
      str = "OB_MYSQL_TYPE_DECIMAL";
      break;
    case obmysql::OB_MYSQL_TYPE_TINY:
      str = "OB_MYSQL_TYPE_TINY";
      break;
    case obmysql::OB_MYSQL_TYPE_SHORT:
      str = "OB_MYSQL_TYPE_SHORT";
      break;
    case obmysql::OB_MYSQL_TYPE_LONG:
      str = "OB_MYSQL_TYPE_LONG";
      break;
    case obmysql::OB_MYSQL_TYPE_FLOAT:
      str = "OB_MYSQL_TYPE_FLOAT";
      break;
    case obmysql::OB_MYSQL_TYPE_DOUBLE:
      str = "OB_MYSQL_TYPE_DOUBLE";
      break;
    case obmysql::OB_MYSQL_TYPE_NULL:
      str = "OB_MYSQL_TYPE_NULL";
      break;
    case obmysql::OB_MYSQL_TYPE_TIMESTAMP:
      str = "OB_MYSQL_TYPE_TIMESTAMP";
      break;
    case obmysql::OB_MYSQL_TYPE_LONGLONG:
      str = "OB_MYSQL_TYPE_LONGLONG";
      break;
    case obmysql::OB_MYSQL_TYPE_INT24:
      str = "OB_MYSQL_TYPE_INT24";
      break;
    case obmysql::OB_MYSQL_TYPE_DATE:
      str = "OB_MYSQL_TYPE_DATE";
      break;
    case obmysql::OB_MYSQL_TYPE_TIME:
      str = "OB_MYSQL_TYPE_TIME";
      break;
    case obmysql::OB_MYSQL_TYPE_DATETIME:
      str = "OB_MYSQL_TYPE_DATETIME";
      break;
    case obmysql::OB_MYSQL_TYPE_YEAR:
      str = "OB_MYSQL_TYPE_YEAR";
      break;
    case obmysql::OB_MYSQL_TYPE_NEWDATE:
      str = "OB_MYSQL_TYPE_NEWDATE";
      break;
    case obmysql::OB_MYSQL_TYPE_VARCHAR:
      str = "OB_MYSQL_TYPE_VARCHAR";
      break;
    case obmysql::OB_MYSQL_TYPE_BIT:
      str = "OB_MYSQL_TYPE_BIT";
      break;
    case obmysql::OB_MYSQL_TYPE_OB_TIMESTAMP_WITH_TIME_ZONE:
      str = "OB_MYSQL_TYPE_OB_TIMESTAMP_WITH_TIME_ZONE";
      break;
    case obmysql::OB_MYSQL_TYPE_OB_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      str = "OB_MYSQL_TYPE_OB_TIMESTAMP_WITH_LOCAL_TIME_ZONE";
      break;
    case obmysql::OB_MYSQL_TYPE_OB_TIMESTAMP_NANO:
      str = "OB_MYSQL_TYPE_OB_TIMESTAMP_NANO";
      break;
    case obmysql::OB_MYSQL_TYPE_OB_RAW:
      str = "OB_MYSQL_TYPE_OB_RAW";
      break;
    case obmysql::OB_MYSQL_TYPE_NEWDECIMAL:
      str = "OB_MYSQL_TYPE_NEWDECIMAL";
      break;
    case obmysql::OB_MYSQL_TYPE_ENUM:
      str = "OB_MYSQL_TYPE_ENUM";
      break;
    case obmysql::OB_MYSQL_TYPE_SET:
      str = "OB_MYSQL_TYPE_SET";
      break;
    case obmysql::OB_MYSQL_TYPE_TINY_BLOB:
      str = "OB_MYSQL_TYPE_TINYBLOB";
      break;
    case obmysql::OB_MYSQL_TYPE_MEDIUM_BLOB:
      str = "OB_MYSQL_TYPE_MEDIUMBLOB";
      break;
    case obmysql::OB_MYSQL_TYPE_LONG_BLOB:
      str = "OB_MYSQL_TYPE_LONGBLOB";
      break;
    case obmysql::OB_MYSQL_TYPE_BLOB:
      str = "OB_MYSQL_TYPE_BLOB";
      break;
    case obmysql::OB_MYSQL_TYPE_VAR_STRING:
      str = "OB_MYSQL_TYPE_VAR_STRING";
      break;
    case obmysql::OB_MYSQL_TYPE_STRING:
      str = "OB_MYSQL_TYPE_STRING";
      break;
    case obmysql::OB_MYSQL_TYPE_GEOMETRY:
      str = "OB_MYSQL_TYPE_GEOMETRY";
      break;
    case obmysql::OB_MYSQL_TYPE_NOT_DEFINED:
      str = "OB_MYSQL_TYPE_NOTDEFINED";
      break;
    default:
      str = "UNKNOWN_TYPE";
      break;
  }

  return str;
}

inline int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos,
                               const obmysql::EMySQLFieldType &type)
{
  return common::databuff_printf(buf, buf_len, pos, "%s", get_emysql_field_type_str(type));
}

inline int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                  const bool with_comma, const obmysql::EMySQLFieldType &type)
{
  return databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:\"%s\""), key, get_emysql_field_type_str(type));
}


} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_RPC_OBMYSQL_OB_MYSQL_GLOBAL_
