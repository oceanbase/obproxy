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

#define USING_LOG_PREFIX PROXY

#include "lib/oblog/ob_log_module.h"
#include "iocore/eventsystem/ob_vconnection.h"
#include "iocore/eventsystem/ob_ethread.h"
#include "ob_rpc_req_operator.h"
#include "ob_rpc_req_operator_table_scan.h"
#include "ob_rpc_req_operator_async_task.h"
#include "proxy/rpc_optimize/executor/ob_rpc_req_parallel_execute_cont.h" //proxy::ObRpcReq

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::proxy;

namespace oceanbase
{
namespace obproxy
{
namespace engine
{

ObProxyRpcReqOperator::ObProxyRpcReqOperator(proxy::ObRpcReq *input, common::ObIAllocator &allocator)
          : input_(input), stream_request_(NULL), allocator_(allocator), parent_(NULL), children_(NULL),
            child_cnt_(0), child_max_cnt_(0), left_(NULL), right_(NULL),
            is_phy_operator_(false),
            is_in_fetching_(false),
            projector_(NULL), type_(PHY_RPC_INVALID),
            operator_async_task_(NULL), cont_index_(0), timeout_ms_(0),
            result_(NULL), request_param_arr_(NULL), build_result_(NULL), rpc_trace_id_()
{
  if (OB_NOT_NULL(input)) {
    uint64_t id, ipport = 0;
    const ObRpcReqTraceId &trace_id = input->get_trace_id();
    trace_id.get_rpc_trace_id(id, ipport);
    rpc_trace_id_.set_rpc_trace_id(id, ipport);
  }
}

ObProxyRpcReqOperator::~ObProxyRpcReqOperator()
{
  LOG_DEBUG("Will Destruct Operator", K(op_name()), K_(rpc_trace_id));
  destruct_children();
  destruct_input();

  if (OB_NOT_NULL(build_result_)) {
    reset_build_packet(build_result_);
    build_result_ = NULL;
  }

  if (OB_NOT_NULL(operator_async_task_)) {
    operator_async_task_->destroy();
    operator_async_task_ = NULL;
  }
}

void ObProxyRpcReqOperator::destruct_children()
{
  if (get_rpc_op_type() != PHY_RPC_TABLE_SCAN) { //skip TABLE_SCAN
    for (int64_t i = 0; i < child_cnt_; ++i) {
      if (OB_NOT_NULL(children_[i])) {
        children_[i]->~ObProxyRpcReqOperator();
        children_[i] = NULL;
      }
    }
    child_cnt_ = 0;
  }
}

void ObProxyRpcReqOperator::destruct_input()
{
  if (OB_NOT_NULL(input_)) {
    // input_->~proxy::ObRpcReq();
    input_ = NULL;
  }
}

void ObProxyRpcReqOperator::print_execute_plan_info(ObProxyRpcReqOperator *op)
{
  char buf[1024 * 256];
  int32_t pos = 0;
  ObProxyRpcReqOperator::print_execute_plan(op, 0, buf, pos, 256 * 1024);
  ObString trace_info(16 * 1024, buf);
  LOG_DEBUG("execute_plan_info: \n", K(trace_info));
}

int ObProxyRpcReqOperator::set_children_pointer(ObProxyRpcReqOperator **children, uint32_t child_cnt)
{
  int ret = common::OB_SUCCESS;
  if (child_cnt > 0 && NULL == children) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(ret), K(op_name()), K(child_cnt), KP(children), K_(rpc_trace_id));
  } else {
    children_ = children;
    child_cnt_ = child_cnt;
    child_max_cnt_ = child_cnt;
    if (child_cnt > 0) {
      child_ = children[0];
    } else {
      child_ = NULL;
    }
    if (child_cnt > 1) {
      right_ = children[1];
    } else {
      right_ = NULL;
    }
  }
  return ret;
}

int ObProxyRpcReqOperator::set_child(const uint32_t idx, ObProxyRpcReqOperator *child)
{
  int ret = common::OB_SUCCESS;
  if (idx >= child_max_cnt_ || OB_ISNULL(child)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(ret), K(op_name()), K(idx), K(child_max_cnt_), KP(child), K_(rpc_trace_id));
  } else if (idx > child_cnt_) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(ret), K(op_name()), K(idx), K(child_cnt_), KP(child), K_(rpc_trace_id));
  } else {
    children_[idx] = child;
    if (0 == idx) {
      child_ = child;
    }
    if (1 == idx) {
      right_ = child;
    }
    child_->parent_ = this;
    child_cnt_ = (idx == child_cnt_) ? child_cnt_ + 1 : child_cnt_;
  }
  return ret;
}

ObProxyRpcReqOperator* ObProxyRpcReqOperator::get_child(const uint32_t idx)
{
  ObProxyRpcReqOperator *op = NULL;
  int ret = common::OB_SUCCESS;
  if (idx >= child_cnt_ &&  PHY_RPC_TABLE_SCAN != type_) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(ret), K(op_name()), K(idx), K(child_cnt_), K_(rpc_trace_id));
  } else if (PHY_RPC_TABLE_SCAN != type_) { //TableScan not has valid child
    op = children_[idx];
  }
  return op;
}

int ObProxyRpcReqOperator::init()
{
  LOG_DEBUG("ObProxyRpcReqOperator::init start", K(op_name()), K_(rpc_trace_id));
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(children_)) {
    if (0 >= child_max_cnt_) {
      child_max_cnt_ = OP_RPC_MAX_CHILDREN_NUM;
    }
    const int64_t alloc_size = child_max_cnt_ * sizeof(ObProxyRpcReqOperator*);
    children_ = reinterpret_cast<ObProxyRpcReqOperator **>(allocator_.alloc(alloc_size));
    if (OB_ISNULL(children_)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("no have enough memory to init", K(ret), K(op_name()), K(child_max_cnt_),
          K(alloc_size), K_(rpc_trace_id));
    }
  }

  // if (OB_SUCC(ret) && OB_FAIL(init_row_set(cur_result_rows_))) {
  //   ret = common::OB_ERR_UNEXPECTED;
  //   LOG_WDIAG("init result_rows error", K(ret), K(op_name()));
  // }

  LOG_DEBUG("ObProxyRpcReqOperator::init end", K(ret), K(op_name()), K_(rpc_trace_id));
  return ret;
}

const char* ObProxyRpcReqOperator::op_name()
{
  return get_rpc_op_name(get_rpc_op_type());
}

int ObProxyRpcReqOperator::open(event::ObContinuation *cont, event::ObAction *&action, const int64_t timeout_ms)
{
  LOG_DEBUG("ObProxyRpcReqOperator::open enter", K(op_name()), K(cont), K(timeout_ms), K_(rpc_trace_id));
  int ret = common::OB_SUCCESS;
  void *tmp_buf = NULL;
  action  = NULL;
  if (OB_ISNULL(cont)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input", K(ret), K_(rpc_trace_id));
  } else if (OB_ISNULL(tmp_buf = static_cast<char *>(allocator_.alloc(sizeof(ObRpcReqOperatorAsyncCommonTask))))){
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc mem", K(ret));
  } else if (OB_ISNULL(operator_async_task_ = new (tmp_buf) ObRpcReqOperatorAsyncCommonTask(child_cnt_, this))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("failed to init ObRpcReqOperatorAsyncCommonTask object", K(ret), K_(rpc_trace_id));
  } else {
    action = &operator_async_task_->get_action();
    if (OB_FAIL(operator_async_task_->init_async_task(cont, &self_ethread()))) {
      LOG_WDIAG("failed to init init ObRpcReqOperatorAsyncCommonTask task", K(ret), K_(rpc_trace_id));
    } else if (get_rpc_op_type() != PHY_RPC_TABLE_SCAN) { //TABLE_SCAN operator not need to init child
      for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt_; i++) {
        if (OB_ISNULL(children_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("invalid child to open", K(ret), K_(rpc_trace_id));
        } else if (OB_FAIL(children_[i]->open(operator_async_task_, operator_async_task_->parallel_action_array_[i], timeout_ms))) {
          LOG_WDIAG("open child operator failed", K(ret), K(children_[i]), K_(rpc_trace_id));
        } else if (OB_ISNULL(operator_async_task_->parallel_action_array_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("not get the action from child", K(ret), K(children_[i]), K(i));
        } else {
          operator_async_task_->add_target_task_count();
        }
      }
    }
  }
  timeout_ms_ = timeout_ms;
  return ret;
}

int ObProxyRpcReqOperator::fetch_stream_response(int64_t cont_index)
{
  int ret = common::OB_SUCCESS;
  LOG_DEBUG("ObProxyRpcReqOperator::fetch_stream_response", K(op_name()), K_(stream_request), K_(operator_async_task));
  if (get_rpc_op_type() != PHY_RPC_TABLE_SCAN) {
    for (int64_t i = 0; i < child_cnt_; i++) {
      if (OB_ISNULL(children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("invalid child to open", K(ret));
      } else {
        children_[i]->set_stream_fetch_request(stream_request_);
        if (OB_FAIL(children_[i]->fetch_stream_response(cont_index))) {
          LOG_WDIAG("open child operator failed", K(ret), K(children_[i]));
        }
      }
    }
  }
  return ret;
}

void ObProxyRpcReqOperator::close()
{
  LOG_DEBUG("ObProxyRpcReqOperator::close enter", K(op_name()));

  if (get_rpc_op_type() != PHY_RPC_TABLE_SCAN) { //skip PHY_RPC_TABLE_SCAN
    for (int64_t i = 0; i < child_cnt_; ++i) {
       if (OB_NOT_NULL(children_[i])) {
         children_[i]->close();
       }
    }
  }

  if (OB_NOT_NULL(operator_async_task_)) {
    operator_async_task_->cancel_all_pending_action();
  }

  LOG_DEBUG("ObProxyRpcReqOperator::close exit", K(op_name()));
}

int ObProxyRpcReqOperator::execute_rpc_request()
{
  LOG_DEBUG("ObProxyRpcReqOperator::execute_rpc_request enter", K(op_name()));
  int ret = common::OB_SUCCESS;
  /* children_ pointer is checked bofore operator open, no need check it again. */

  for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt_; ++i) {
    if (OB_FAIL(children_[i]->execute_rpc_request())) {
      LOG_WDIAG("ObProxyProOp::execute_rpc_request failed", K(ret));
      break;
    }
  }
  LOG_DEBUG("ObProxyRpcReqOperator::execute_rpc_request exit", K(ret), K(op_name()));
  return ret;
}

int ObProxyRpcReqOperator::process_ready_data(void *data, int &event)
{
  int ret = OB_SUCCESS;
  bool is_final = false;
  proxy::ObRpcReq *result = NULL;
  LOG_DEBUG("ObProxyRpcReqOperator::process_ready_data ", K(data), K(event), "operator_name", op_name());
  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid param, data is null", "op_name", op_name(), K(ret));
  } else if (OB_FAIL(handle_result(data, is_final, result))) {
    LOG_WDIAG("fail to handle result", "op_name", op_name(), K(ret));
  } else if (!is_final && OB_ISNULL(result)) {
    event = VC_EVENT_CONT;
  } else if (is_final) {
    result_ = result;
    event = VC_EVENT_READ_COMPLETE;
  } else {
    result_ = result;
    event = VC_EVENT_READ_READY;
  }

  LOG_DEBUG("finish process_ready_data", K(event), K(ret), "rpc_operator_name", op_name());
  return ret;
}

int ObProxyRpcReqOperator::handle_result(void *data, bool &is_final, proxy::ObRpcReq *&result)
{
  int ret = OB_SUCCESS;

  proxy::ObRpcReq *request = NULL;
  proxy::ObRpcReq *pres = NULL;
  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input, data is NULL", K(ret));
  } else if (OB_ISNULL(request = reinterpret_cast<proxy::ObRpcReq*>(data)) || OB_ISNULL(pres = request)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input, pres type is not match", K(ret), K(request), K(pres));
  } else {
    proxy::ObRpcOBKVInfo &obkv_info = pres->get_obkv_info();
    if (obkv_info.is_resp()) {
      if (OB_FAIL(handle_response_result(data, is_final, result))) {
        LOG_WDIAG("failed to handle resultset_resp", K(ret));
      }
    } else if (obkv_info.is_error()) {
      result = pres;
    }
    LOG_DEBUG("handle_result success", K(ret), K(pres), K(is_final), K(result), K(obkv_info), "is_resp", obkv_info.is_resp(), "is_error", obkv_info.is_error(), K_(rpc_trace_id));
  }
  return ret;
}

int ObProxyRpcReqOperator::handle_response_result(void *src, bool &is_final, proxy::ObRpcReq *&result)
{
  UNUSED(src);
  UNUSED(is_final);
  // UNUSED(result);
  result = (proxy::ObRpcReq *)src;
  // result_ = result;
  return OB_SUCCESS;
}

int ObProxyRpcReqOperator::process_complete_data(void *data)
{
  int ret = OB_SUCCESS;
  bool is_final = true;
  proxy::ObRpcReq *result = NULL;
  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid param, data is null", K(ret), K_(rpc_trace_id));
  } else if (OB_FAIL(handle_result(data, is_final, result))) {
    LOG_WDIAG("fail to handle result", K(ret), K_(rpc_trace_id));
  } else if (OB_ISNULL(result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("result is NULL, something error", K(ret), K_(rpc_trace_id));
  } else {
    result_ = result;
  }

  LOG_DEBUG("finish process_complete_data", K(ret), K_(rpc_trace_id));
  return ret;
}

int ObProxyRpcReqOperator::packet_error_info(proxy::ObRpcReq *&res, char *err_msg, int64_t err_msg_len,
                     uint16_t error_no)
{
 int ret = common::OB_SUCCESS;
 if (OB_ISNULL(err_msg)) {
   ret = common::OB_INVALID_ARGUMENT;
   LOG_WDIAG("invalid argument", K(ret), K(err_msg), K(err_msg_len), K(error_no), K_(rpc_trace_id));
 } else {
   LOG_DEBUG("will be packet error packet:", K(err_msg), K(err_msg_len), K(error_no), K_(rpc_trace_id));
   res = NULL;
   void *err_buf = NULL;
   char *info_buf = NULL;
   err_buf = allocator_.alloc(err_msg_len);
   info_buf = static_cast<char*>(allocator_.alloc(err_msg_len + 1));

   if (OB_ISNULL(err_buf) || OB_ISNULL(info_buf)) {
     ret = common::OB_ALLOCATE_MEMORY_FAILED;
     LOG_WDIAG("not allocat enougth memeory",
              K(err_msg_len + 1), K_(rpc_trace_id));
   } else {
    //TODO build rpc error response
    LOG_WDIAG("rpc error response", K(res), K(error_no), K(err_msg), K(err_msg_len));
     //PacketErrorInfo *err_info = new (err_buf) PacketErrorInfo();
     //err_info->error_code_ = error_no;

     //MEMCPY(info_buf, err_msg, err_msg_len);
     //info_buf[err_msg_len] = '\0';
     //err_info->error_msg_.assign(info_buf, static_cast<ObString::obstr_size_t>(err_msg_len));
     //ret = packet_error_info(res, err_info);
   }
 }
 return ret;
}

int ObProxyRpcReqOperator::build_empty_packet(proxy::ObRpcReq *&res)
{
  int ret = OB_SUCCESS;
  void *tmp_buf = NULL;
  res = NULL;
  if(OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(proxy::ObRpcReq)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("no have enough memory to init", K(ret), "op_name", op_name(), K_(rpc_trace_id));
  } else {
    res = new (tmp_buf) proxy::ObRpcReq();
    res->set_cont_index(get_cont_index());
    tmp_buf = NULL;
    // if (OB_FAIL(res->alloc_req_info())) {
    //   LOG_WDIAG("failed to init rep buf for res", K(res));
    // }
   //  if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(proxy::ObProxyRpcResponse)))) {
   //    ret =  OB_ALLOCATE_MEMORY_FAILED;
   //    LOG_WDIAG("no have enough memory to init", K(ret), "op_name", op_name());
   //  } else {
   //   proxy::ObProxyRpcResponse *proxy_res = new (tmp_buf) proxy::ObProxyRpcResponse;
   //   res->set_response(proxy_res);
   //   if (OB_NOT_NULL(build_result_)) {
   //     reset_build_packet(build_result_); //other memory will be free by allocator
   //     build_result_ = NULL;
   //   }
     build_result_ = res;
   //  }
  }
  //proxy::ObProxyRpcResponse
  LOG_DEBUG("ObProxyRpcReqOperator::packet_empty_packet",
            "op_name", op_name(), K(ret), K_(rpc_trace_id));
  return ret;
}

int ObProxyRpcReqOperator::build_error_packet(proxy::ObRpcReq *&res, int error_code)
{
  int ret = OB_SUCCESS;
  UNUSED(error_code);
  if (OB_FAIL(build_empty_packet(res))) {
    LOG_WDIAG("build empty packet failed", K(ret), K_(rpc_trace_id));
  } else {
    //TODO build response base on rpc request(meta) and error_code
  }

  return ret;
}

void ObProxyRpcReqOperator::reset_build_packet(proxy::ObRpcReq *&res)
{
  UNUSED(res);
  // if (OB_NOT_NULL(res) && OB_NOT_NULL(res->get_rpc_response())) {
  //   res->get_rpc_response()->reset(); //free memory of response allocted
  // }
}

void ObProxyRpcReqOperator::set_rpc_op_type(ObPhyRpcReqOperatorType type)
{
  type_ = type;
}

}
}
}
