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

#ifndef OBPROXY_OB_PROXY_OPERATOR_H
#define OBPROXY_OB_PROXY_OPERATOR_H

#include "opsql/func_expr_resolver/proxy_expr/ob_proxy_expr.h"
#include "common/ob_row.h"
#include "lib/container/ob_se_array.h"
#include "lib/ob_errno.h"
#include "dbconfig/ob_proxy_db_config_info.h"
#include "proxy/api/ob_api_defs.h"
#include "obutils/ob_async_common_task.h"
#include "proxy/mysqllib/ob_field_heap.h"

#include "ob_proxy_operator_result.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace obproxy {
namespace engine {

enum ObPhyOperatorType
{
  PHY_INVALID = 0,
  PHY_SORT,
  PHY_AGG,
  PHY_TABLE_SCAN,
  PHY_PROJECTION,
  PHY_MERGE_AGG,
  PHY_HASH_AGG,
  PHY_STREAM_AGG,
  PHY_MEM_MERGE_AGG,
  PHY_MERGE_SORT,
  PHY_TOPK,
  PHY_STREAM_SORT,
  PHY_MEM_MERGE_SORT,
  PHY_MAX
};

const char* get_op_name(ObPhyOperatorType type)
{
  const char *char_ret = NULL;
  switch (type) {
    case PHY_SORT:
      char_ret = "PHY_SORT";
      break;
    case PHY_AGG:
      char_ret = "PHY_AGG";
      break;
    case PHY_TABLE_SCAN:
      char_ret = "PHY_TABLE_SCAN";
      break;
    case PHY_PROJECTION:
      char_ret = "PHY_PROJECTION";
      break;
    case PHY_MERGE_AGG:
      char_ret = "PHY_MERGE_AGG";
      break;
    case PHY_HASH_AGG:
      char_ret = "PHY_HASH_AGG";
      break;
    case PHY_MERGE_SORT:
      char_ret = "PHY_MERGE_SORT";
      break;
    case PHY_TOPK:
      char_ret = "PHY_TOPK";
      break;
    case PHY_STREAM_AGG:
      char_ret = "PHY_STREAM_AGG";
      break;
    case PHY_MEM_MERGE_AGG:
      char_ret = "PHY_MEM_MERGE_AGG";
      break;
    case PHY_STREAM_SORT:
      char_ret = "PHY_STREAM_SORT";
      break;
    case PHY_MEM_MERGE_SORT:
      char_ret = "PHY_MEM_MERGE_SORT";
      break;
    default:
      char_ret = "UNKOWN_OPERATOR";
      break;
  }
  return char_ret;
}

/* Default child_cnt_ for each Operator */
const uint32_t OP_MAX_CHILDREN_NUM = 10;
const uint32_t OP_HANDLER_TIMEOUT_MS = 5000 * 1000;

class ObProxyOpInput;
class ObProxyOpCtx;
class ObOperatorAsyncCommonTask;

class ObProxyOperator
{
public:
  ObProxyOperator(ObProxyOpInput *input, common::ObIAllocator &allocator);

  virtual ~ObProxyOperator();

  virtual ObProxyOpInput *get_input() const { return input_; }
  void set_input(ObProxyOpInput *input) { input_ = input; }

  int set_children_pointer(ObProxyOperator **children, uint32_t child_cnt); 
  int set_child(const uint32_t idx, ObProxyOperator *child);
  virtual ObProxyOperator* get_child(const uint32_t idx); //TableScan Operator not have any chilren
  int32_t get_child_cnt() { return child_cnt_; }
  virtual int init();
  virtual const char* op_name();

  static void print_execute_plan(ObProxyOperator *op, const int level, char* buf, 
          int& pos, int length) 
  {
    if (NULL == op || NULL == buf || length <= 0) {
      return;
    }
    for (int i = 0; i < level * 2; i++) {
      pos += snprintf(buf + pos, length - pos, "-");
    }

    pos += snprintf(buf + pos, length - pos, " OPERATER:%s, TYPE:%d\n", 
      get_op_name(op->get_op_type()), op->get_op_type());

    for (uint32_t i = 0; i < op->get_child_cnt(); i++) {
      print_execute_plan(op->get_child(i), level + 1, buf, pos, length - pos);
    }
  }

  static void print_execute_plan_info(ObProxyOperator *op);

  // Open the operator, cascading open children's Operators.
  virtual int open(event::ObContinuation *cont, event::ObAction *&action, const int64_t timeout_ms = 0);

  virtual int get_next_row();
  virtual void close();
  virtual void destruct_children();
  virtual void destruct_input();

  virtual int process_ready_data(void *data, int &event);
  virtual int process_complete_data(void *data);
  virtual int handle_result(void *data, bool &is_final, ObProxyResultResp *&result);
  virtual int handle_response_result(void *data, bool &is_final, ObProxyResultResp *&result);

  virtual int calc_result(ResultRow &row, ResultRow &result, common::ObIArray<ObProxyExpr*> &exprs,
                             const int64_t first_res_offset);
  virtual int put_row(ResultRow *&row);
  virtual int init_row(ResultRow *&row);
  virtual int init_row_set(ResultRows *&rows);

  virtual int packet_result_set(ObProxyResultResp *&res, ResultRows *rows, ResultFields *fields);
  virtual int packet_result_set_eof(ObProxyResultResp *&res);
  virtual int packet_error_info(ObProxyResultResp *&res, PacketErrInfo *err);
  virtual int packet_error_info(ObProxyResultResp *&res, char *err_msg, int64_t err_msg_len,
                   uint16_t error_no);
  virtual int build_ok_packet(ObProxyResultResp *&res);

  void set_result_fields(ResultFields *result_fields) {
      result_fields_ =  result_fields;
  }
  ResultFields *get_result_fields() { return result_fields_; }

  virtual void set_phy_operator(bool is_phy) { is_phy_operator_ = is_phy; }
  virtual bool get_phy_operator() const { return is_phy_operator_;}

  void set_op_type(ObPhyOperatorType type);
  ObPhyOperatorType get_op_type() { return type_; }

  int put_result_row(ResultRow *row);

  int64_t get_cont_index() { return cont_index_; }
  void set_cont_index(int64_t cont_index) { cont_index_ = cont_index; }
  void* get_operator_result() { return result_; }
  void set_operator_async_task(ObOperatorAsyncCommonTask *operator_async_task) { operator_async_task_ = operator_async_task; }

protected:
  ObProxyOpInput *input_;
  common::ObIAllocator &allocator_;
  ObProxyOperator *parent_;
  ObProxyOperator **children_;
  uint32_t child_cnt_;
  uint32_t child_max_cnt_;
  union {
    ObProxyOperator *child_;
    ObProxyOperator *left_;
  };
  ObProxyOperator *right_;

  bool opened_;
  // is physical operater or not, not phy operator put data as stream.
  bool is_phy_operator_;

  int64_t column_count_;
  int64_t row_count_;
  int32_t *projector_;
  ObPhyOperatorType type_;
  ResultRows *cur_result_rows_;
  ResultFields *result_fields_;

  ObOperatorAsyncCommonTask *operator_async_task_;
  int64_t cont_index_;
  int64_t timeout_ms_;
  bool expr_has_calced_;
  void *result_;
  DISALLOW_COPY_AND_ASSIGN(ObProxyOperator);
};

class ObProxyOpInput
{
public:
  ObProxyOpInput() : limit_offset_(0), limit_size_(-1),
                     select_exprs_(ObModIds::OB_SE_ARRAY_ENGINE, ENGINE_ARRAY_NEW_ALLOC_SIZE) {}
  virtual ~ObProxyOpInput() {}

  int set_select_exprs(const common::ObIArray<ObProxyExpr*> &select_exprs) {
    return select_exprs_.assign(select_exprs);
  }

  void set_limit_offset(int64_t limit_offset) { limit_offset_ = limit_offset; }
  int64_t get_limit_offset() { return limit_offset_; }
  void set_limit_size(int64_t limit_size) { limit_size_ = limit_size; }
  int64_t get_limit_size() { return limit_size_; }

  common::ObSEArray<ObProxyExpr*, 4>& get_select_exprs() { return select_exprs_; }

  int64_t get_op_top_value() { return 0; }
protected:
  int64_t limit_offset_;
  int64_t limit_size_;
  /* All operations need the select_expr, so need put it inito here. */
  common::ObSEArray<ObProxyExpr*, 4> select_exprs_;
};

}
}
}
#endif //OBPROXY_OB_PROXY_OPERATOR_H
