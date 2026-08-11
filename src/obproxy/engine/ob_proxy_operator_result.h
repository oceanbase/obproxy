/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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

extern const int64_t ENGINE_ARRAY_NEW_ALLOC_SIZE;
extern const int16_t OP_DEFAULT_ERROR_NO;
extern const char* OP_DEFAULT_ERROR_MSG;

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


extern int change_sql_field(const ObMysqlField *src_field, obmysql::ObMySQLField *&dst_field,
                            common::ObIAllocator &allocator);

extern int change_sql_value(ObObj &value, obmysql::ObMySQLField &field, ObIAllocator *allocator);

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
