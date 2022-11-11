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

#include "obsm_utils.h"

#include "lib/time/ob_time_utility.h"
#include "lib/charset/ob_dtoa.h"
#include "common/ob_field.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;

struct ObMySQLTypeMap
{
  /* oceanbase::common::ObObjType ob_type; */
  EMySQLFieldType mysql_type;
  uint16_t flags;         /* flags if Field */
  uint64_t length;        /* other than varchar type */
};

// @todo
// reference: https://dev.mysql.com/doc/refman/5.6/en/c-api-data-structures.html
// reference: http://dev.mysql.com/doc/internals/en/client-server-protocol.html
static const ObMySQLTypeMap type_maps_[ObMaxType] =
{
  /* ObMinType */
  {OB_MYSQL_TYPE_NULL,      OB_MYSQL_BINARY_FLAG, 0},                        /* ObNullType */
  {OB_MYSQL_TYPE_TINY,      OB_MYSQL_BINARY_FLAG, 0},                        /* ObTinyIntType */
  {OB_MYSQL_TYPE_SHORT,     OB_MYSQL_BINARY_FLAG, 0},                        /* ObSmallIntType */
  {OB_MYSQL_TYPE_INT24,     OB_MYSQL_BINARY_FLAG, 0},                        /* ObMediumIntType */
  {OB_MYSQL_TYPE_LONG,      OB_MYSQL_BINARY_FLAG, 0},                        /* ObInt32Type */
  {OB_MYSQL_TYPE_LONGLONG,  OB_MYSQL_BINARY_FLAG, 0},                        /* ObIntType */
  {OB_MYSQL_TYPE_TINY,      OB_MYSQL_BINARY_FLAG | OB_MYSQL_UNSIGNED_FLAG, 0},        /* ObUTinyIntType */
  {OB_MYSQL_TYPE_SHORT,     OB_MYSQL_BINARY_FLAG | OB_MYSQL_UNSIGNED_FLAG, 0},        /* ObUSmallIntType */
  {OB_MYSQL_TYPE_INT24,     OB_MYSQL_BINARY_FLAG | OB_MYSQL_UNSIGNED_FLAG, 0},        /* ObUMediumIntType */
  {OB_MYSQL_TYPE_LONG,      OB_MYSQL_BINARY_FLAG | OB_MYSQL_UNSIGNED_FLAG, 0},        /* ObUInt32Type */
  {OB_MYSQL_TYPE_LONGLONG,  OB_MYSQL_BINARY_FLAG | OB_MYSQL_UNSIGNED_FLAG, 0},        /* ObUInt64Type */
  {OB_MYSQL_TYPE_FLOAT,     OB_MYSQL_BINARY_FLAG, 0},                        /* ObFloatType */
  {OB_MYSQL_TYPE_DOUBLE,    OB_MYSQL_BINARY_FLAG, 0},                        /* ObDoubleType */
  {OB_MYSQL_TYPE_FLOAT,     OB_MYSQL_BINARY_FLAG | OB_MYSQL_UNSIGNED_FLAG, 0},        /* ObUFloatType */
  {OB_MYSQL_TYPE_DOUBLE,    OB_MYSQL_BINARY_FLAG | OB_MYSQL_UNSIGNED_FLAG, 0},        /* ObUDoubleType */
  {OB_MYSQL_TYPE_NEWDECIMAL,OB_MYSQL_BINARY_FLAG, 0},                        /* ObNumberType */
  {OB_MYSQL_TYPE_NEWDECIMAL,OB_MYSQL_BINARY_FLAG | OB_MYSQL_UNSIGNED_FLAG, 0},        /* ObUNumberType */
  {OB_MYSQL_TYPE_DATETIME,  OB_MYSQL_BINARY_FLAG, 0},                        /* ObDateTimeType */
  {OB_MYSQL_TYPE_TIMESTAMP, OB_MYSQL_BINARY_FLAG | OB_MYSQL_TIMESTAMP_FLAG, 0},       /* ObTimestampType */
  {OB_MYSQL_TYPE_DATE,   OB_MYSQL_BINARY_FLAG, 0},                        /* ObDateType */
  {OB_MYSQL_TYPE_TIME,      OB_MYSQL_BINARY_FLAG, 0},                        /* ObTimeType */
  {OB_MYSQL_TYPE_YEAR,      OB_MYSQL_UNSIGNED_FLAG | OB_MYSQL_ZEROFILL_FLAG, 0},      /* ObYearType */
  {OB_MYSQL_TYPE_VAR_STRING, 0, 0},              /* ObVarcharType */
  {OB_MYSQL_TYPE_STRING, 0, 0},                  /* ObCharType */
  {OB_MYSQL_TYPE_VAR_STRING,OB_MYSQL_BINARY_FLAG, 0},     /* ObHexStringType */
  {OB_MYSQL_TYPE_NOT_DEFINED, 0, 0},             /* ObExtendType */
  {OB_MYSQL_TYPE_NOT_DEFINED, 0, 0},             /* ObUnknownType */
  /* ObMaxType */
};

//called by handle OB_MYSQL_COM_STMT_EXECUTE offset is 0
bool ObSMUtils::update_from_bitmap(ObObj &param, const char *bitmap, int64_t field_index)
{
  bool ret = false;
  int byte_pos = static_cast<int>(field_index / 8);
  int bit_pos  = static_cast<int>(field_index % 8);
  char value = bitmap[byte_pos];
  if (value & (1 << bit_pos)) {
    ret = true;
    param.set_null();
  }
  return ret;
}

int ObSMUtils::cell_str(
    char *buf, const int64_t len,
    const ObObj &obj,
    MYSQL_PROTOCOL_TYPE type, int64_t &pos,
    int64_t cell_idx, char *bitmap,
    const ObTimeZoneInfo *tz_info,
    const ObField *field)
{
  int ret = OB_SUCCESS;

  ObScale scale = 0;
  bool zerofill = false;
  int32_t zflength = 0;
  if (NULL == field) {
    if (OB_UNLIKELY(obj.is_invalid_type())) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      scale = ObAccuracy::DML_DEFAULT_ACCURACY[obj.get_type()].get_scale();
    }
  } else {
    scale = field->accuracy_.get_scale();
    zerofill = field->flags_ & OB_MYSQL_ZEROFILL_FLAG;
    zflength = field->length_;
  }
  if (OB_SUCC(ret)) {
    switch (obj.get_type_class()) {
      case ObNullTC:
        ret = ObMySQLUtil::null_cell_str(buf, len, type, pos, cell_idx, bitmap);
        break;
      case ObIntTC:
        ret = ObMySQLUtil::int_cell_str(buf, len, obj.get_int(), false, type, pos, zerofill, zflength);
        break;
      case ObUIntTC:
        ret = ObMySQLUtil::int_cell_str(buf, len, obj.get_int(), true, type, pos, zerofill, zflength);
        break;
      case ObFloatTC:
        ret = ObMySQLUtil::float_cell_str(buf, len, obj.get_float(), type, pos, scale, zerofill, zflength);
        break;
      case ObDoubleTC:
        ret = ObMySQLUtil::double_cell_str(buf, len, obj.get_double(), type, pos, scale, zerofill, zflength);
        break;
      case ObNumberTC:
        ret = ObMySQLUtil::number_cell_str(buf, len, obj.get_number(), pos, scale, zerofill, zflength);
        break;
      case ObDateTimeTC:
        ret = ObMySQLUtil::datetime_cell_str(buf, len, obj.get_datetime(), type, pos,
                                             obj.is_timestamp() ? tz_info : NULL, scale);
        break;
      case ObDateTC:
        ret = ObMySQLUtil::date_cell_str(buf, len, obj.get_date(), type, pos);
        break;
      case ObTimeTC:
        ret = ObMySQLUtil::time_cell_str(buf, len, obj.get_time(), type, pos, scale);
        break;
      case ObYearTC:
        ret = ObMySQLUtil::year_cell_str(buf, len, obj.get_year(), type, pos);
        break;
      case ObRawTC:
      case ObTextTC: // TODO@hanhui texttc share the stringtc temporarily
      case ObStringTC:
      case ObLobTC: {
        ret = ObMySQLUtil::varchar_cell_str(buf, len, obj.get_string(), pos);
        break;
      }
      default:
        _OB_LOG(ERROR, "invalid ob type=%d", obj.get_type());
        ret = OB_ERROR;
        break;
    }
  }
  return ret;
}

int get_map(ObObjType ob_type, const ObMySQLTypeMap *&map)
{
  int ret = OB_SUCCESS;
  if (ob_type >= ObMaxType) {
    ret = OB_OBJ_TYPE_ERROR;
  }

  if (OB_SUCC(ret)) {
    map = type_maps_ + ob_type;
  }

  return ret;
}

int ObSMUtils::get_type_length(ObObjType ob_type, int64_t &length)
{
  const ObMySQLTypeMap *map = NULL;
  int ret = OB_SUCCESS;

  if ((ret = get_map(ob_type, map)) == OB_SUCCESS) {
    length = map->length;
  }
  return ret;
}

int ObSMUtils::get_mysql_type(ObObjType ob_type, EMySQLFieldType &mysql_type,
                              uint16_t &flags, ObScale &num_decimals)
{
  const ObMySQLTypeMap *map = NULL;
  int ret = OB_SUCCESS;

  if ((ret = get_map(ob_type, map)) == OB_SUCCESS) {
    mysql_type = map->mysql_type;
    flags |= map->flags;
    // batch fixup num_decimal values
    // so as to be compatible with mysql metainfo
    switch (mysql_type) {
      case OB_MYSQL_TYPE_LONGLONG:
      case OB_MYSQL_TYPE_LONG:
      case OB_MYSQL_TYPE_INT24:
      case OB_MYSQL_TYPE_SHORT:
      case OB_MYSQL_TYPE_TINY:
      case OB_MYSQL_TYPE_NULL:
      case OB_MYSQL_TYPE_DATE:
      case OB_MYSQL_TYPE_YEAR:
        num_decimals = 0;
        break;
      case OB_MYSQL_TYPE_VAR_STRING:
      case OB_MYSQL_TYPE_STRING:
        // for compatible with MySQL, ugly convention.
        num_decimals = static_cast<ObScale>(NOT_FIXED_DEC);
        break;
      case OB_MYSQL_TYPE_TIMESTAMP:
      case OB_MYSQL_TYPE_DATETIME:
      case OB_MYSQL_TYPE_TIME:
      case OB_MYSQL_TYPE_FLOAT:
      case OB_MYSQL_TYPE_DOUBLE:
      case OB_MYSQL_TYPE_NEWDECIMAL:
        num_decimals = static_cast<ObScale>((num_decimals == -1) ? NOT_FIXED_DEC : num_decimals);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        _OB_LOG(WARN, "unexpected mysql_type=%d", mysql_type);
        break;
    } // end switch
  }
  return ret;
}

int ObSMUtils::get_ob_type(ObObjType &ob_type, EMySQLFieldType mysql_type)
{
  int ret = OB_SUCCESS;
  switch (mysql_type) {
    case OB_MYSQL_TYPE_NULL:
      ob_type = ObNullType;
      break;
    case OB_MYSQL_TYPE_TINY:
      ob_type = ObTinyIntType;
      break;
    case OB_MYSQL_TYPE_SHORT:
      ob_type = ObSmallIntType;
      break;
    case OB_MYSQL_TYPE_INT24:
      ob_type = ObMediumIntType;
      break;
    case OB_MYSQL_TYPE_LONG:
      ob_type = ObInt32Type;
      break;
    case OB_MYSQL_TYPE_LONGLONG:
      ob_type = ObIntType;
      break;
    case OB_MYSQL_TYPE_FLOAT:
      ob_type = ObFloatType;
      break;
    case OB_MYSQL_TYPE_DOUBLE:
      ob_type = ObDoubleType;
      break;
    case OB_MYSQL_TYPE_TIMESTAMP:
      ob_type = ObTimestampType;
      break;
    case OB_MYSQL_TYPE_DATETIME:
      ob_type = ObDateTimeType;
      break;
    case OB_MYSQL_TYPE_TIME:
      ob_type = ObTimeType;
      break;
    case OB_MYSQL_TYPE_DATE:
      ob_type = ObDateType;
      break;
    case OB_MYSQL_TYPE_YEAR:
      ob_type = ObYearType;
      break;
    case OB_MYSQL_TYPE_VARCHAR:
    case OB_MYSQL_TYPE_STRING:
    case OB_MYSQL_TYPE_VAR_STRING:
      ob_type = ObVarcharType;
      break;
    case OB_MYSQL_TYPE_TINY_BLOB:
      ob_type = ObTinyTextType;
      break;
    case OB_MYSQL_TYPE_BLOB:
      ob_type = ObTextType;
      break;
    case OB_MYSQL_TYPE_MEDIUM_BLOB:
      ob_type = ObMediumTextType;
      break;
    case OB_MYSQL_TYPE_LONG_BLOB:
      ob_type = ObLongTextType;
      break;
    case OB_MYSQL_TYPE_OB_TIMESTAMP_WITH_TIME_ZONE:
      ob_type = ObTimestampTZType;
      break;
    case OB_MYSQL_TYPE_OB_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      ob_type = ObTimestampLTZType;
      break;
    case OB_MYSQL_TYPE_OB_TIMESTAMP_NANO:
      ob_type = ObTimestampNanoType;
      break;
    case OB_MYSQL_TYPE_OB_RAW:
      ob_type = ObRawType;
      break;
    case OB_MYSQL_TYPE_DECIMAL:
    case OB_MYSQL_TYPE_NEWDECIMAL:
      ob_type = ObNumberType;
      break;
    case OB_MYSQL_TYPE_BIT:
      ob_type = ObBitType;
      break;
    case OB_MYSQL_TYPE_ENUM:
      ob_type = ObEnumType;
      break;
    case OB_MYSQL_TYPE_SET:
      ob_type = ObSetType;
      break;
    case OB_MYSQL_TYPE_COMPLEX:
      ob_type = ObExtendType;
      break;
    case OB_MYSQL_TYPE_OB_UROWID:
      ob_type = ObURowIDType;
      break;
    default:
      _OB_LOG(WARN, "unsupport MySQL type %d", mysql_type);
      ret = OB_OBJ_TYPE_ERROR;
  }
  return ret;
}

int ObSMUtils::to_ob_field(const ObMySQLField &field, ObField &mfield)
{
  int ret = OB_SUCCESS;
  mfield.dname_ = field.dname_;
  mfield.tname_ = field.tname_;
  mfield.org_tname_ = field.org_tname_;
  mfield.cname_ = field.cname_;
  mfield.org_cname_ = field.org_cname_;

  mfield.accuracy_ = field.accuracy_;

  // To distinguish between binary and nonbinary data for string data types,
  // check whether the charsetnr value is 63. Also, flag must be set to binary accordingly
  mfield.charsetnr_ = field.charsetnr_;
  mfield.flags_ = field.flags_;
  mfield.length_ = field.length_;

  // For Varchar class, check charset:
  // TODO:Handling incorrect character sets
  if (ObCharset::is_valid_collation(static_cast<ObCollationType>(field.charsetnr_))
      && ObCharset::is_bin_sort(static_cast<ObCollationType>(field.charsetnr_))) {
    mfield.flags_ |= OB_MYSQL_BINARY_FLAG;
  }

  ObObjType ob_type;
  ret = ObSMUtils::get_ob_type(ob_type, field.type_);
  mfield.type_.set_type(ob_type);

  OB_LOG(DEBUG, "to ob field", K(ret), K(mfield), K(field));
  return ret;
}
