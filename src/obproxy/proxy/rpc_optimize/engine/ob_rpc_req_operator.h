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

#ifndef OBPROXY_RPC_REQ_OB_PROXY_OPERATOR_H
#define OBPROXY_RPC_REQ_OB_PROXY_OPERATOR_H

#include "common/ob_row.h"
#include "lib/container/ob_se_array.h"
#include "lib/ob_errno.h"
#include "proxy/api/ob_api_defs.h"
#include "obutils/ob_async_common_task.h"
#include "proxy/rpc_optimize/ob_rpc_req_trace.h"
#include "proxy/rpc_optimize/ob_rpc_req.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace obproxy {

namespace proxy {
class ObRpcReq;
}

namespace executor {
class ObProxyRpcParallelResp;
}
namespace engine {

class ObProxyRpcOpParam;

typedef common::ObSEArray<proxy::ObRpcReq *, 4> RpcParallelRespArray;
typedef common::ObSEArray<ObProxyRpcOpParam, 4> RpcOpParamArray;

enum ObPhyRpcReqOperatorType
{
  PHY_RPC_INVALID = 0,
  PHY_RPC_PROJECTION,
  PHY_RPC_TABLE_SCAN,
  PHY_RPC_RANGE_SCAN,
  PHY_RPC_BATCH_API,
  PHY_RPC_STREAM_SCAN,
  PHY_RPC_MAX
};

const char* get_rpc_op_name(ObPhyRpcReqOperatorType type)
{
  const char *char_ret = NULL;
  switch (type) {
    case PHY_RPC_TABLE_SCAN:
      char_ret = "PHY_RPC_TABLE_SCAN";
      break;
    case PHY_RPC_PROJECTION:
      char_ret = "PHY_RPC_PROJECTION";
      break;
    case PHY_RPC_RANGE_SCAN:
      char_ret = "PHY_RPC_RANGE_SCAN";
      break;
    case PHY_RPC_BATCH_API:
      char_ret = "PHY_RPC_BATCH_API";
      break;
    case PHY_RPC_STREAM_SCAN:
      char_ret = "PHY_RPC_STREAM_SCAN";
      break;
    default:
      char_ret = "UNKOWN_RPC_OPERATOR";
      break;
  }
  return char_ret;
}

/* Default child_cnt_ for each Operator */
const uint32_t OP_RPC_MAX_CHILDREN_NUM = 10;
const uint32_t OP_RPC_HANDLER_TIMEOUT_MS = 5000 * 1000;

// class ObProxyOpCtx;
class ObRpcReqOperatorAsyncCommonTask;

class ObProxyRpcOpParam
{
public:
  ObProxyRpcOpParam()
    : cont_index_(0), session_info_(NULL), rpc_request_(NULL) {}
  ObProxyRpcOpParam(int cont_index, void *session_info, proxy::ObRpcReq *request)
    : cont_index_(cont_index), session_info_(session_info), rpc_request_(request) {}
  ~ObProxyRpcOpParam() {}
  int64_t to_string(char *buf, const int64_t buf_len) const {
    int64_t pos = 0;
    J_OBJ_START();
    J_KV(K_(cont_index),
         KP_(session_info),
         K_(rpc_request));
    J_OBJ_END();
    return pos;
  }

public:
  int cont_index_;
  void *session_info_;
  proxy::ObRpcReq *rpc_request_;
};

class ObProxyRpcReqOperator
{
public:
  ObProxyRpcReqOperator(proxy::ObRpcReq *input, common::ObIAllocator &allocator);

  virtual ~ObProxyRpcReqOperator();

  virtual proxy::ObRpcReq *get_input() const { return input_; }
  void set_input(proxy::ObRpcReq *input) { input_ = input; }

  int set_children_pointer(ObProxyRpcReqOperator **children, uint32_t child_cnt);
  int set_child(const uint32_t idx, ObProxyRpcReqOperator *child);
  virtual ObProxyRpcReqOperator* get_child(const uint32_t idx); //TableScan Operator not have any chilren
  int32_t get_child_cnt() { return child_cnt_; }
  virtual int init();
  virtual const char* op_name();

  static void print_execute_plan(ObProxyRpcReqOperator *op, const int level, char* buf,
          int& pos, int length)
  {
    if (NULL == op || NULL == buf || length <= 0) {
      return;
    }
    for (int i = 0; i < level * 2; i++) {
      pos += snprintf(buf + pos, length - pos, "-");
    }

    pos += snprintf(buf + pos, length - pos, " OPERATER:%s, TYPE:%d\n",
      get_rpc_op_name(op->get_rpc_op_type()), op->get_rpc_op_type());

    for (uint32_t i = 0; i < op->get_child_cnt(); i++) {
      print_execute_plan(op->get_child(i), level + 1, buf, pos, length - pos);
    }
  }

  static void print_execute_plan_info(ObProxyRpcReqOperator *op);

  // Open the operator, cascading open children's Operators.
  virtual int open(event::ObContinuation *cont, event::ObAction *&action, const int64_t timeout_ms = 0);

  virtual int execute_rpc_request();
  virtual void close();
  virtual void destruct_children();
  virtual void destruct_input();

  virtual int process_ready_data(void *data, int &event);
  virtual int process_complete_data(void *data);
  virtual int handle_result(void *data, bool &is_final, proxy::ObRpcReq *&result);
  virtual int handle_response_result(void *data, bool &is_final, proxy::ObRpcReq *&result);

  virtual int packet_error_info(proxy::ObRpcReq *&res, char *err_msg, int64_t err_msg_len,
                   uint16_t error_no);

  virtual int build_empty_packet(proxy::ObRpcReq *&res);
  virtual int build_error_packet(proxy::ObRpcReq *&rest, int error_code);

  virtual void set_phy_operator(bool is_phy) { is_phy_operator_ = is_phy; }
  virtual bool get_phy_operator() const { return is_phy_operator_;}
  virtual void set_stream_fetch_request(proxy::ObRpcReq *stream_request) { stream_request_ = stream_request; }
  virtual int fetch_stream_response(int64_t cont_index = 0);// { /* do nothing */ UNUSED(cont_index); return 0; }
  virtual void set_is_in_fetching(bool flag) { is_in_fetching_ = flag; }
  virtual bool get_is_in_fetching() { return is_in_fetching_; }

  void set_rpc_op_type(ObPhyRpcReqOperatorType type);
  ObPhyRpcReqOperatorType get_rpc_op_type() { return type_; }

  int64_t get_cont_index() { return cont_index_; }
  void set_cont_index(int64_t cont_index) { cont_index_ = cont_index; }
  void* get_operator_result() { return result_; }
  void set_operator_async_task(ObRpcReqOperatorAsyncCommonTask *operator_async_task) { operator_async_task_ = operator_async_task; }

  void set_request_param_array(RpcOpParamArray *arr) { request_param_arr_ = arr; }
  void reset_build_packet(proxy::ObRpcReq *&res);

protected:
  proxy::ObRpcReq *input_;
  proxy::ObRpcReq *stream_request_;
  common::ObIAllocator &allocator_;
  ObProxyRpcReqOperator *parent_;
  ObProxyRpcReqOperator **children_;
  uint32_t child_cnt_;
  uint32_t child_max_cnt_;
  union {
    ObProxyRpcReqOperator *child_;
    ObProxyRpcReqOperator *left_;
  };
  ObProxyRpcReqOperator *right_;

  bool opened_;
  // is physical operater or not, not phy operator put data as stream.
  bool is_phy_operator_;
  bool is_in_fetching_;

  int32_t *projector_;
  ObPhyRpcReqOperatorType type_;

  ObRpcReqOperatorAsyncCommonTask *operator_async_task_;
  int64_t cont_index_;
  int64_t timeout_ms_;
  void *result_;
  RpcOpParamArray *request_param_arr_;
  proxy::ObRpcReq *build_result_;
  proxy::ObRpcReqTraceId rpc_trace_id_;
  DISALLOW_COPY_AND_ASSIGN(ObProxyRpcReqOperator);
};

}
}
}
#endif //OBPROXY_RPC_REQ_OB_PROXY_OPERATOR_H
