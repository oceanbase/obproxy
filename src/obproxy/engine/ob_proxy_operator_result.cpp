/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PROXY
#include "obproxy/engine/ob_proxy_operator_result.h"

namespace oceanbase{
namespace obproxy{
namespace engine{

const int64_t ENGINE_ARRAY_NEW_ALLOC_SIZE = 1;
const int16_t OP_DEFAULT_ERROR_NO = 8001;
const char* OP_DEFAULT_ERROR_MSG = "Inner error occured in Operator and not have any other info";


int change_sql_field(const ObMysqlField *src_field, obmysql::ObMySQLField *&dst_field,
                     common::ObIAllocator &allocator)
{
  int ret = common::OB_SUCCESS;
  dst_field = NULL;
  void *tmp_buf = NULL;
  char *buf = NULL;
  if (OB_ISNULL(src_field)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (OB_NOT_NULL(tmp_buf = allocator.alloc(sizeof(obmysql::ObMySQLField)))){
    dst_field = new (tmp_buf) obmysql::ObMySQLField();

    buf = static_cast<char*>(allocator.alloc(src_field->db_.length()));
    MEMCPY(buf, src_field->db_.ptr(), src_field->db_.length());
    dst_field->dname_.assign_ptr(buf, src_field->db_.length());

    buf = static_cast<char*>(allocator.alloc(src_field->table_.length()));
    MEMCPY(buf, src_field->table_.ptr(), src_field->table_.length());
    dst_field->tname_.assign_ptr(buf, src_field->table_.length());

    buf = static_cast<char*>(allocator.alloc(src_field->org_table_.length()));
    MEMCPY(buf, src_field->org_table_.ptr(), src_field->org_table_.length());
    dst_field->org_tname_.assign_ptr(buf, src_field->org_table_.length());


    buf = static_cast<char*>(allocator.alloc(src_field->name_.length()));
    MEMCPY(buf, src_field->name_.ptr(), src_field->name_.length());
    dst_field->cname_.assign_ptr(buf, src_field->name_.length());

    buf = static_cast<char*>(allocator.alloc(src_field->org_name_.length()));
    MEMCPY(buf, src_field->org_name_.ptr(), src_field->org_name_.length());
    dst_field->org_cname_.assign_ptr(buf, src_field->org_name_.length());

    if (obmysql::OB_MYSQL_TYPE_FLOAT == src_field->type_
        || obmysql::OB_MYSQL_TYPE_DOUBLE == src_field->type_) {
      if (0x1f == src_field->decimals_) {
        ObObjType ob_type;
        if (OB_SUCCESS != ObSMUtils::get_ob_type(ob_type, src_field->type_)) {
          ob_type = ObDoubleType;
        }
        dst_field->accuracy_ = ObAccuracy::DML_DEFAULT_ACCURACY[ob_type];
      } else {
        dst_field->accuracy_.set_scale(static_cast<ObScale>(src_field->decimals_));
      }
    } else if(obmysql::OB_MYSQL_TYPE_NEWDECIMAL == src_field->type_
              || obmysql::OB_MYSQL_TYPE_DECIMAL == src_field->type_
              || obmysql::OB_MYSQL_TYPE_TIMESTAMP == src_field->type_
              || obmysql::OB_MYSQL_TYPE_DATETIME == src_field->type_
              || obmysql::OB_MYSQL_TYPE_TIME == src_field->type_) {
      if (src_field->decimals_ > number::ObNumber::MAX_SCALE) {
        ObObjType ob_type;
        if (OB_SUCCESS != ObSMUtils::get_ob_type(ob_type, src_field->type_)) {
          ob_type = ObNumberType;
        }
        dst_field->accuracy_ = ObAccuracy::DML_DEFAULT_ACCURACY[ob_type];
      } else {
        dst_field->accuracy_.set_scale(static_cast<ObScale>(src_field->decimals_));
      }
    } else {
      dst_field->accuracy_.set_accuracy(static_cast<int64_t>(src_field->decimals_));
    }

    dst_field->type_ = src_field->type_;
    dst_field->flags_ = static_cast<uint16_t>(src_field->flags_);
    dst_field->set_charset_number(static_cast<uint16_t>(src_field->charsetnr_));
    dst_field->length_ = static_cast<uint32_t>(src_field->length_);
  }
  return ret;
}

int change_sql_value(ObObj &value, obmysql::ObMySQLField &field, ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;

  if (value.is_varchar()) {
    ObObjType ob_type;
    ObCollationType cs_type = static_cast<ObCollationType>(field.charsetnr_);
    // utf8_general_ci => CS_TYPE_UTF8MB4_GENERAL_CI
    if (33 == field.charsetnr_) {
      cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
      // utf8_bin => CS_TYPE_UTF8MB4_BIN
    } else if (83 == field.charsetnr_) {
      cs_type = CS_TYPE_UTF8MB4_BIN;
    }

    value.set_collation_type(cs_type);

    if (0 != value.get_string_len()) {
      // 把列转成具体的类型, 如果转换不了就保持 varchar
      if (OB_FAIL(ObSMUtils::get_ob_type(ob_type, field.type_))) {
        COMMON_LOG(INFO, "cast ob type from mysql type failed", K(ob_type), "elem_type", field.type_, K(ret));
        ret = OB_SUCCESS;
      } else if (ObTimestampType == ob_type || ObTimeType == ob_type
                 || ObDateType == ob_type || ObDateTimeType == ob_type) {
        //do nothing
      } else {
        ObCastCtx cast_ctx(allocator, NULL, CM_NULL_ON_WARN, cs_type);
        // use src_obj as buf_obj
        if (OB_FAIL(ObObjCasterV2::to_type(ob_type, cs_type, cast_ctx, value, value))) {
          COMMON_LOG(WDIAG, "failed to cast obj", "row", value, K(ob_type), K(cs_type), K(ret));
        }
      }
    }
  }

  return ret;
}

}
}
}