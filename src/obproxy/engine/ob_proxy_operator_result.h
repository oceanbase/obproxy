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

#ifndef OBPROXY_OB_PROXY_RESULT_RESP_H
#define OBPROXY_OB_PROXY_RESULT_RESP_H
#include "common/ob_row.h"
//#include "common/ob_field.h"
#include "common/obsm_utils.h"
#include "common/ob_obj_cast.h"
#include "rpc/obmysql/ob_mysql_field.h"
#include "proxy/mysqllib/ob_resultset_fetcher.h"
#include "lib/container/ob_se_array.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include "executor/ob_proxy_parallel_execute_cont.h"

namespace oceanbase{
namespace obproxy{
namespace engine{


const int64_t ENGINE_ARRAY_NEW_ALLOC_SIZE = 1;

typedef common::ObSEArray<common::ObObj*, 4, common::ObIAllocator&> ResultRow;
typedef common::ObSEArray<ResultRow *, 4, common::ObIAllocator&> ResultRows;
typedef common::ObSEArray<int64_t, 4, common::ObIAllocator&> ResultRowsIndex;
typedef common::ObSEArray<obmysql::ObMySQLField, 1, common::ObIAllocator&> ResultFields;

enum PacketType {
  PCK_DEFAULT = 0,
  PCK_OK_RESPONSE,
  PCK_ERR_RESPONSE,
  PCK_RESULTSET_RESPONSE,
  PCK_RESULTSET_EOF_RESPONSE,
  PCK_MAX
};

typedef struct PacketErrInfo {
  uint16_t error_code_;
  common::ObString error_msg_;
public:
  TO_STRING_KV(K(error_code_), K(error_msg_));
} PacketErrorInfo;

const int16_t OP_DEFAULT_ERROR_NO = 8001;
const char* OP_DEFAULT_ERROR_MSG  = "Inner error occured in Operator and not have any other info";

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
      // Convert the column to a specific type, if it cannot be converted, keep varchar
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
          COMMON_LOG(WARN, "failed to cast obj", "row", value, K(ob_type), K(cs_type), K(ret));
        }
      }
    }
  }

  return ret;
}

class ObProxyResultResp : public executor::ObProxyParallelResp
{
public:
  ObProxyResultResp(common::ObIAllocator &allocator, int64_t cont_index)
      : ObProxyParallelResp(cont_index),
        packet_flag_(PCK_DEFAULT),
        err_info_(NULL),
        result_rows_(NULL),
        result_fields_(NULL),
        column_count_(0),
        cur_row_index_(0),
        result_idx_(0),
        result_sum_(0),
        allocator_(allocator) {}

  ~ObProxyResultResp();

  int init_result(ResultRows *rows, ResultFields *fields);
  int next(ResultRow *&row);
  int get_fields(ResultFields *&fields);
  ResultFields* get_fields() { return result_fields_; }
  bool is_error_resp() const { return packet_flag_ == PCK_ERR_RESPONSE;}
  bool is_ok_resp() const { return packet_flag_ == PCK_OK_RESPONSE; }
  bool is_resultset_resp() const { return packet_flag_ == PCK_RESULTSET_RESPONSE;  }
  bool is_resultset_resp_eof() const { return packet_flag_ == PCK_RESULTSET_EOF_RESPONSE; }
  uint16_t get_err_code() const {
      return static_cast<uint16_t>(err_info_ == NULL ? OP_DEFAULT_ERROR_NO: err_info_->error_code_); }
  common::ObString get_err_msg() {
      return err_info_ == NULL ? common::ObString(OP_DEFAULT_ERROR_MSG):err_info_->error_msg_; }
  ResultRows& get_result_rows() { return *result_rows_; }

  int64_t get_column_count() { return column_count_; }
  void set_column_count(int64_t count) { column_count_ = count; }
  void set_packet_flag(PacketType type) { packet_flag_ = type; }
  PacketType get_packet_flag() { return packet_flag_; }
  void set_err_info(PacketErrInfo *err_info) { err_info_ = err_info; }

  void set_result_sum(int64_t sum) { result_sum_ = sum; }
  void set_result_idx(int64_t idx) { result_idx_ = idx; }
  int64_t get_result_sum() { return result_sum_; }
  int64_t get_result_idx() { return result_idx_; }
  void set_has_calc_exprs(bool has_calc_exprs) { has_calc_exprs_ = has_calc_exprs; }
  bool get_has_calc_exprs() { return has_calc_exprs_; }
  TO_STRING_KV(K(packet_flag_), K(err_info_), K(column_count_));
private:
  PacketType packet_flag_;
  bool has_calc_exprs_;
  PacketErrInfo *err_info_;
  ResultRows *result_rows_;
  ResultFields *result_fields_;
  int64_t column_count_;
  int64_t cur_row_index_;
  int64_t result_idx_; // which server from
  int64_t result_sum_; // sum of server from
  common::ObIAllocator &allocator_;

};

/* checkout result_rows which call it */
int ObProxyResultResp::init_result(ResultRows *result_rows, ResultFields *fields)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(result_rows) || OB_ISNULL(fields)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    result_rows_ = result_rows;
    result_fields_ = fields;
    set_packet_flag(PCK_RESULTSET_RESPONSE);
    cur_row_index_ = 0;
  }
  return ret;
}

int ObProxyResultResp::next(ResultRow *&row)
{
  int ret = common::OB_SUCCESS;
  row = NULL; /* if not have any rows, it is NULL */
  if (OB_ISNULL(result_rows_)) {
    ret = common::OB_ERROR;
  } else if (cur_row_index_ < result_rows_->count()) {
    row = result_rows_->at(cur_row_index_++);
  } else if (cur_row_index_ == result_rows_->count()) {
    ret = common::OB_ITER_END;
  }
  return ret;
}

int ObProxyResultResp::get_fields(ResultFields *&fields)
{
  int ret = common::OB_SUCCESS;
  fields = NULL; /* if not have any rows, it is NULL */
  if (OB_ISNULL(result_rows_)) {
    ret = common::OB_ERROR;
  } else if (result_fields_->count() > 0) {
    fields = result_fields_;
  }
  return ret;
}

}
}
}
#endif /* OBPROXY_OB_PROXY_RESULT_RESP_H */
