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

#include "ob_rpc_req_sharding_plan.h"
#include "ob_rpc_req_optimizer_processor.h"
#include "proxy/rpc_optimize/engine/ob_rpc_req_operator_batch.h"
#include "proxy/rpc_optimize/engine/ob_rpc_req_operator_ls.h"
#include "proxy/rpc_optimize/engine/ob_rpc_req_operator_projection.h"
#include "proxy/rpc_optimize/engine/ob_rpc_req_operator_query.h"
#include "proxy/rpc_optimize/engine/ob_rpc_req_operator_table_scan.h"
#include "obproxy/obkv/table/ob_table_rpc_request.h"

using namespace oceanbase::obproxy::engine;
using namespace oceanbase::common;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obrpc;

namespace oceanbase
{
namespace obproxy
{
namespace optimizer
{


//template <typename O, typename I>
//int create_operator(ObIAllocator *allocator, O *&op, I *&input)
template <typename O, typename I>
int create_operator(ObIAllocator *allocator, O *&op, I *input)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  op = NULL;

  if (OB_SUCC(ret) && (NULL == (ptr = allocator->alloc(sizeof(O))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("alloc operator failed", K(ret));
  } else {
    op = new (ptr) O(input, *allocator);
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(op->init())) {
      LOG_WDIAG("op init failed", K(ret));
    }
  }
  return ret;
}

ObProxyShardRpcReqRequestPlan::ObProxyShardRpcReqRequestPlan(proxy::ObRpcReq *client_request,
                                                       ObIAllocator *allocator)
       : client_request_(client_request), allocator_(allocator), plan_root_(NULL)
{
}

ObProxyShardRpcReqRequestPlan::~ObProxyShardRpcReqRequestPlan()
{
  if (NULL != plan_root_) {
    plan_root_->~ObProxyRpcReqOperator();
  }

  plan_root_ = NULL;
}

int ObProxyShardRpcReqRequestPlan::handle_shard_rpc_request(proxy::ObRpcReq *client_request)
{
  int ret = OB_SUCCESS;
  ObProxyShardRpcReqRequestPlan* execute_plan = NULL;
  ObIAllocator *allocator = NULL;
  void *ptr = NULL;
  if (OB_ISNULL(client_request)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument to handle", K(client_request));
  } else if (OB_FAIL(get_global_optimizer_rpc_req_processor().alloc_allocator(allocator))) {
    LOG_WDIAG("alloc allocator failed", K(ret));
  } else if (OB_ISNULL(ptr = allocator->alloc(sizeof(ObProxyShardRpcReqRequestPlan)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc select plan buf", "size", sizeof(ObProxyShardRpcReqRequestPlan), K(ret));
  } else {
    execute_plan = new(ptr) ObProxyShardRpcReqRequestPlan(client_request, allocator);
    if (OB_FAIL(execute_plan->generate_plan())) {
      LOG_WDIAG("fail to generate plan", K(ret));
    } else {
      client_request->execute_plan_ = execute_plan;
    }
  }
  // trans_state.
  return ret;
}

int ObProxyShardRpcReqRequestPlan::handle_shard_rpc_fetch_request(proxy::ObRpcReq *client_request)
{
  int ret = OB_SUCCESS;
  UNUSED(client_request);
  return ret;
}

int ObProxyShardRpcReqRequestPlan::generate_plan()
{
  int ret = OB_SUCCESS;
  ObRpcOBKVInfo &obkv_info = client_request_->get_obkv_info();
  LOG_DEBUG("begin to generate plan");

  obrpc::ObRpcPacketCode pcode = obkv_info.pcode_;
  bool is_stream = false;
  bool is_query = (pcode == obrpc::OB_TABLE_API_EXECUTE_QUERY || pcode == obrpc::OB_TABLE_API_QUERY_AND_MUTATE || pcode == obrpc::OB_TABLE_API_EXECUTE_QUERY_SYNC);

  if (OB_FAIL(add_rpc_table_scan_operator())) {
    LOG_WDIAG("fail to add rpc table scan operator", K(ret));
  } else if (pcode == obrpc::OB_TABLE_API_BATCH_EXECUTE && OB_FAIL(add_rpc_batch_table_operator())) {
    LOG_WDIAG("fail to add rpc batch operator", K(ret));
  } else if (pcode == obrpc::OB_TABLE_API_LS_EXECUTE && OB_FAIL(add_rpc_ls_table_operator())) {
    LOG_WDIAG("fail to add rpc ls operator", K(ret));
  } else if (is_query && is_stream) {
    ret = OB_NOT_SUPPORTED;
    LOG_WDIAG("ODP not support stream query", K(ret));
  } else if (is_query && !is_stream && OB_FAIL(add_rpc_common_table_query_operator())) {
    LOG_WDIAG("fail to add rpc table query operator", K(ret));
  } else if (OB_FAIL(add_rpc_projection_operator())) {
    LOG_WDIAG("fail to add projection operator", K(ret));
  } else {
    print_plan_info();
  }

  return ret;
}

int ObProxyShardRpcReqRequestPlan::add_rpc_table_scan_operator()
{
  int ret = OB_SUCCESS;

  ObProxyRpcReqTableScanOp *table_scan_op = NULL;
  // char *buf = NULL;
  if (OB_FAIL(create_operator(allocator_, table_scan_op, client_request_))) {
    LOG_WDIAG("create operator and input failed", K(ret));
  } else {
    plan_root_ = table_scan_op;
  }

  return ret;
}

int ObProxyShardRpcReqRequestPlan::add_rpc_projection_operator()
{
  int ret = OB_SUCCESS;

  ObProxyRpcReqProOp *op = NULL;

  if (OB_FAIL(create_operator(allocator_, op, client_request_))) {
    LOG_WDIAG("create projection failed", K(ret));
  } else {
    if (OB_FAIL(op->set_child(0, plan_root_))) {
      LOG_WDIAG("set child failed", K(ret));
    } else {
      plan_root_ = op;
    }
  }

  return ret;
}

int ObProxyShardRpcReqRequestPlan::add_rpc_common_table_query_operator()
{
  int ret = OB_SUCCESS;

  ObProxyRpcReqQueryOp *op = NULL;

  if (OB_FAIL(create_operator(allocator_, op, client_request_))) {
    LOG_WDIAG("create projection failed", K(ret));
  } else {
    if (OB_FAIL(op->set_child(0, plan_root_))) {
      LOG_WDIAG("set child failed", K(ret));
    } else {
      plan_root_ = op;
    }
  }

  return ret;
}

// int ObProxyShardRpcReqRequestPlan::add_rpc_stream_table_query_operator()
// {
//   int ret = OB_SUCCESS;

//   // ObProxyRpcReqQueryStreamOp *op = NULL;
//   ObProxyRpcReqQueryOp *op = NULL;

//   if (OB_FAIL(create_operator(allocator_, op, client_request_))) {
//     LOG_WDIAG("create projection failed", K(ret));
//   } else {
//     if (OB_FAIL(op->set_child(0, plan_root_))) {
//       LOG_WDIAG("set child failed", K(ret));
//     } else {
//       plan_root_ = op;
//     }
//   }

//   return ret;
// }

int ObProxyShardRpcReqRequestPlan::add_rpc_batch_table_operator()
{
  int ret = OB_SUCCESS;

  ObProxyRpcReqBatchOp *op = NULL;

  if (OB_FAIL(create_operator(allocator_, op, client_request_))) {
    LOG_WDIAG("create projection failed", K(ret));
  } else {
    if (OB_FAIL(op->set_child(0, plan_root_))) {
      LOG_WDIAG("set child failed", K(ret));
    } else {
      plan_root_ = op;
    }
  }
  return ret;
}

int ObProxyShardRpcReqRequestPlan::add_rpc_ls_table_operator()
{
  int ret = OB_SUCCESS;
  ObProxyRpcReqLSOp *op = NULL;
  if (OB_FAIL(create_operator(allocator_, op, client_request_))) {
    LOG_WDIAG("create projection failed", K(ret));
  } else {
    if (OB_FAIL(op->set_child(0, plan_root_))) {
      LOG_WDIAG("set child failed", K(ret));
    } else {
      plan_root_ = op;
    }
  }
  return ret;
}

void ObProxyShardRpcReqRequestPlan::print_plan_info()
{
  ObProxyRpcReqOperator::print_execute_plan_info(plan_root_);
}

} // end optimizer
} // end obproxy
} // end oceanbase
