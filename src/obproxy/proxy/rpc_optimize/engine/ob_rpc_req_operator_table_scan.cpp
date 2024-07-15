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
#include "ob_rpc_req_operator_table_scan.h"
#include "lib/string/ob_sql_string.h"
#include "utils/ob_proxy_utils.h" /* string_to_upper_case */
#include "iocore/eventsystem/ob_ethread.h"
#include "proxy/rpc_optimize/executor/ob_rpc_req_parallel_cont.h"
#include "proxy/rpc_optimize/executor/ob_rpc_req_parallel_execute_cont.h" //proxy::ObRpcReq
#include "proxy/rpc_optimize/net/ob_rpc_client_net_handler.h"
#include "proxy/rpc_optimize/ob_rpc_request_sm.h"
#include "proxy/route/ob_index_entry.h"
#include "obproxy/obkv/table/ob_rpc_struct.h"
#include "obproxy/obkv/table/ob_table_rpc_request.h"
#include "lib/utility/ob_ls_id.h"

using namespace oceanbase::obproxy::executor;
using namespace oceanbase::common;
using namespace oceanbase::obproxy::obkv;
using namespace oceanbase::obproxy::proxy;

namespace oceanbase {
namespace obproxy {
namespace engine {

int ObProxyRpcReqTableScanOp::open(event::ObContinuation *cont, event::ObAction *&action, const int64_t timeout_ms)
{
  child_cnt_ = 1; //fake child
  int ret = ObProxyRpcReqOperator::open(cont, action, timeout_ms);
  return ret;
}

ObProxyRpcReqOperator* ObProxyRpcReqTableScanOp::get_child(const uint32_t idx)
{
  int ret = common::OB_SUCCESS;
  if (child_cnt_ > 0) {
    ret = common::OB_ERROR;
    LOG_WDIAG("invalid TABLE_SCAN operator which has children operator", K(ret), K(idx),
        K(child_cnt_), K_(rpc_trace_id));
  }
  return NULL;
}

int ObProxyRpcReqTableScanOp::handle_shard_rpc_request(proxy::ObRpcReq* rpc_req,
                           common::ObSEArray<executor::ObProxyRpcParallelParam, 4> &parallel_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rpc_req)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid rpc request to handle", KP(rpc_req), K(ret), K_(rpc_trace_id));
  } else {
    ObRpcOBKVInfo &obkv_info = rpc_req->get_obkv_info();
    obrpc::ObRpcPacketCode pcode = obkv_info.get_pcode();
    LOG_DEBUG("ObProxyRpcReqTableScanOp::handle_shard_rpc_request code info", "code", pcode, K_(rpc_trace_id));
    switch (pcode) {
      case obrpc::OB_TABLE_API_BATCH_EXECUTE:
      {
        ret = handle_shard_rpc_obkv_batch_request(rpc_req, parallel_param);
        break;
      }
      case obrpc::OB_TABLE_API_LS_EXECUTE:
      {
        ret = handle_shard_rpc_ls_request(rpc_req, parallel_param);
        break;
      }
      case obrpc::OB_TABLE_API_EXECUTE_QUERY:
      case obrpc::OB_TABLE_API_QUERY_AND_MUTATE:
      {
        ret = handle_shard_rpc_obkv_query_request(rpc_req, parallel_param);
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("invalid pcode for init", K(pcode), K(ret), K_(rpc_trace_id));
        break;
    }
  }
  return ret;
}

int ObProxyRpcReqTableScanOp::handle_shard_rpc_obkv_batch_request(proxy::ObRpcReq* rpc_req,
                  common::ObSEArray<executor::ObProxyRpcParallelParam, 4> &parallel_param)
{
  int ret = OB_SUCCESS;
  ObRpcTableBatchOperationRequest *batch_request = NULL;
  int64_t batch_request_len = sizeof(ObRpcTableBatchOperationRequest);
  if (OB_ISNULL(rpc_req)
      || OB_ISNULL(batch_request = dynamic_cast<ObRpcTableBatchOperationRequest *>(rpc_req->get_rpc_request()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invlid argument to handle", K(ret), K_(rpc_trace_id));
  } else {
    ObRpcOBKVInfo &obkv_info = rpc_req->get_obkv_info();
    LOG_DEBUG("ObProxyRpcReqTableScanOp::handle_shard_rpc_obkv_batch_request", K(parallel_param), K(rpc_req), "table_id", obkv_info.get_table_id(), K_(rpc_trace_id));
    PARTITION_ID_MAP &partid_to_index_map = batch_request->get_partition_id_map();
    PARTITION_ID_MAP::iterator it = partid_to_index_map.begin();
    PARTITION_ID_MAP::iterator end = partid_to_index_map.end();
    ObSEArray<ObRpcReq *, 4> rpc_reqs;
    for (; OB_SUCC(ret) && it != end; it++) {
      int64_t partition_id = it->first;
      ObSEArray<int64_t, 4> &index = it->second;
      ObRpcTableBatchOperationRequest *sub_batch_request = NULL;
      proxy::ObRpcReq *sub_rpc_req = NULL;
      ObRpcRequestSM *request_sm = NULL;
      LOG_DEBUG("handle_shard_rpc_obkv_batch_request handle sub index", K(index), K(partition_id), K_(rpc_trace_id));
      if (index.count() > 0) {
        void *buf = NULL;
        if (OB_ISNULL(buf = op_fixed_mem_alloc(batch_request_len))) {
          ret = common::OB_ALLOCATE_MEMORY_FAILED;
          LOG_WDIAG("memory not enougth to init rpc request", K(ret), K_(rpc_trace_id));
        } else {
          sub_batch_request = new (buf) ObRpcTableBatchOperationRequest();
          if (OB_ISNULL(sub_rpc_req = ObRpcReq::allocate())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("could not allocate request, abort connection if need", K(ret), K_(rpc_trace_id));
          } else if (OB_ISNULL(request_sm = ObRpcRequestSM::allocate())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("could not allocate request sm, net need abort connection", K(ret), K_(rpc_trace_id));
          }
        }

        if (OB_SUCC(ret) && OB_NOT_NULL(sub_batch_request)) {
          ObRpcOBKVInfo &sub_obkv_info = sub_rpc_req->get_obkv_info();
          sub_batch_request->set_packet_meta(batch_request->get_packet_meta());
          sub_batch_request->set_partition_id(partition_id);

          if (OB_FAIL(sub_batch_request->init_as_sub_batch_operation_request(*batch_request, index))) {
            LOG_WDIAG("invalid to get sub opertion in batch request", K(ret), K_(rpc_trace_id));
          } else if (OB_FAIL(sub_rpc_req->sub_rpc_req_init(rpc_req, request_sm, sub_batch_request, batch_request_len, cont_index_ + 1, partition_id))) {
            LOG_WDIAG("fail to init sub rpc req for batch request", K(ret), K_(rpc_trace_id));
          } else if (request_sm->init(sub_rpc_req)) {
            LOG_WDIAG("fail to init sub request_sm for batch request", K(ret), K_(rpc_trace_id));
          } else {
            executor::ObProxyRpcParallelParam param;

            cont_index_++;
            param.partition_id_ = partition_id; //not have any used
            param.request_ = sub_rpc_req;
            LOG_DEBUG("handle_shard_rpc_obkv_batch_request ", K(sub_rpc_req), "table_id", sub_obkv_info.get_table_id(), "partition_id", sub_obkv_info.get_partition_id(), K_(rpc_trace_id));
            parallel_param.push_back(param);
            rpc_reqs.push_back(sub_rpc_req);
          }
        }
      }

      if (OB_FAIL(ret)) {
        LOG_DEBUG("clean data in ObProxyRpcReqTableScanOp::handle_shard_rpc_obkv_batch_request", K(ret), K_(rpc_trace_id));
        if (OB_NOT_NULL(sub_rpc_req)) {
          sub_rpc_req->destroy();
        }
        if (OB_NOT_NULL(request_sm)) {
          request_sm->destroy();
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(rpc_req->alloc_sub_rpc_req_array(rpc_reqs.count()))) {
        LOG_WDIAG("fail to call alloc_sub_rpc_req_array", K(ret), K_(rpc_trace_id));
      } else if (OB_FAIL(rpc_req->set_sub_rpc_req(rpc_reqs))) {
        LOG_WDIAG("fail to call set_sub_rpc_req", K(ret), K_(rpc_trace_id));
      } else {
        LOG_DEBUG("succ to set_sub_rpc_req", K(ret), K_(rpc_trace_id));
      }
    } else {
      for (int64_t i = 0; i < rpc_reqs.count(); ++i) {
        ObRpcReq *sub_rpc_req = rpc_reqs.at(i);
        ObRpcRequestSM *request_sm = NULL;
        if (OB_NOT_NULL(sub_rpc_req)) {
          sub_rpc_req->destroy();
          request_sm = reinterpret_cast<ObRpcRequestSM *>(sub_rpc_req->sm_);
        }
        if (OB_NOT_NULL(request_sm)) {
          request_sm->destroy();
        }
      }
    }
  }
  LOG_DEBUG("ObProxyRpcReqTableScanOp::handle_shard_rpc_obkv_batch_request end", K(parallel_param), K_(rpc_trace_id));
  return ret;
}

int ObProxyRpcReqTableScanOp::handle_shard_rpc_ls_request(
    proxy::ObRpcReq *rpc_req,
    common::ObSEArray<executor::ObProxyRpcParallelParam, 4> &parallel_param)
{
  int ret = OB_SUCCESS;
  ObRpcTableLSOperationRequest *ls_request = NULL;
  int64_t ls_request_len = sizeof(ObRpcTableLSOperationRequest);
  if (OB_ISNULL(rpc_req)
      || OB_ISNULL(ls_request = dynamic_cast<ObRpcTableLSOperationRequest *>(rpc_req->get_rpc_request()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invlid argument to handle", K(ret), K_(rpc_trace_id));
  } else {
    ObRpcOBKVInfo &obkv_info = rpc_req->get_obkv_info();
    LOG_DEBUG("ObProxyRpcReqTableScanOp::handle_shard_rpc_ls_request", K(parallel_param), K(rpc_req),
              "table_id", obkv_info.get_table_id(), K_(rpc_trace_id));
    LS_TABLET_ID_MAP::iterator ls_id_iter = ls_request->get_ls_id_tablet_id_map().begin(); 
    LS_TABLET_ID_MAP::iterator ls_id_end = ls_request->get_ls_id_tablet_id_map().end(); 
    ObSEArray<ObRpcReq *, 4> rpc_reqs;
    for (; OB_SUCC(ret) && ls_id_iter != ls_id_end; ls_id_iter++) {
      ObRpcTableLSOperationRequest *sub_ls_req = NULL;
      proxy::ObRpcReq *sub_rpc_req = NULL;
      ObSEArray<int64_t, 4> tablet_ids = ls_id_iter->second;
      int64_t ls_id = ls_id_iter->first; 
      ObRpcRequestSM *request_sm = NULL;

      void *buf = NULL;
      if (OB_ISNULL(buf = op_fixed_mem_alloc(sizeof(ObRpcTableLSOperationRequest)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("memory not enougth to init rpc request", K(ret), K_(rpc_trace_id));
      } else {
        sub_ls_req = new (buf) ObRpcTableLSOperationRequest();
        if (OB_ISNULL(sub_rpc_req = ObRpcReq::allocate())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("could not allocate request, abort connection if need", K(ret), K_(rpc_trace_id));
        } else if (OB_ISNULL(request_sm = ObRpcRequestSM::allocate())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("could not allocate request sm, net need abort connection", K(ret), K_(rpc_trace_id));
        }
      }
      
      if (OB_SUCC(ret)) {
        int64_t tablet_id = tablet_ids.at(0);
        LOG_DEBUG("handle_shard_rpc_ls_batch_request handle sub index", K(tablet_ids), K(ls_id),K_(rpc_trace_id));
        ObRpcOBKVInfo &sub_obkv_info = sub_rpc_req->get_obkv_info();
        if (OB_FAIL(sub_ls_req->init_as_sub_ls_operation_request(*ls_request, ls_id, tablet_ids))) {
          LOG_WDIAG("invalid to get sub opertion in batch request", K(ret), K_(rpc_trace_id));
        } else if (OB_FAIL(sub_rpc_req->sub_rpc_req_init(rpc_req, request_sm, sub_ls_req, ls_request_len, cont_index_ + 1, tablet_id, ls_id))) {
          LOG_WDIAG("fail to init sub rpc req for ls request", K(ret), K_(rpc_trace_id));
        } else if (request_sm->init(sub_rpc_req)) {
          LOG_WDIAG("fail to init sub request_sm for ls request", K(ret), K_(rpc_trace_id));
        } else {
          executor::ObProxyRpcParallelParam param;
          
          cont_index_++;
          param.partition_id_ = tablet_id; //not have any used
          param.request_ = sub_rpc_req;
          LOG_DEBUG("handle_shard_rpc_obkv_ls_request ", K(*sub_ls_req), K(sub_rpc_req), "table_id", sub_obkv_info.get_table_id(),
                    "partition_id", sub_obkv_info.get_partition_id(), K_(rpc_trace_id));
          parallel_param.push_back(param);
          rpc_reqs.push_back(sub_rpc_req);
        }
      }

      if (OB_FAIL(ret)) {
        LOG_DEBUG("clean data in ObProxyRpcReqTableScanOp::handle_shard_rpc_ls_request", K(ret), K_(rpc_trace_id));
        if (OB_NOT_NULL(sub_rpc_req)) {
          sub_rpc_req->destroy();
        }
        if (OB_NOT_NULL(request_sm)) {
          request_sm->destroy();
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(rpc_req->alloc_sub_rpc_req_array(rpc_reqs.count()))) {
        LOG_WDIAG("fail to call alloc_sub_rpc_req_array", K(ret), K_(rpc_trace_id));
      } else if (OB_FAIL(rpc_req->set_sub_rpc_req(rpc_reqs))) {
        LOG_WDIAG("fail to call set_sub_rpc_req", K(ret), K_(rpc_trace_id));
      } else {
        LOG_DEBUG("succ to set_sub_rpc_req", K(ret), K_(rpc_trace_id));
      }
    } else {
      for (int64_t i = 0; i < rpc_reqs.count(); ++i) {
        ObRpcReq *sub_rpc_req = rpc_reqs.at(i);
        ObRpcRequestSM *request_sm = NULL;
        if (OB_NOT_NULL(sub_rpc_req)) {
          sub_rpc_req->destroy();
          request_sm = reinterpret_cast<ObRpcRequestSM *>(sub_rpc_req->sm_);
        }
        if (OB_NOT_NULL(request_sm)) {
          request_sm->destroy();
        }
      }
    }
  }
  LOG_DEBUG("ObProxyRpcReqTableScanOp::handle_shard_rpc_obkv_ls_request end", K(parallel_param), K_(rpc_trace_id));
  return ret;
}

int ObProxyRpcReqTableScanOp::handle_shard_rpc_obkv_query_request(
    proxy::ObRpcReq *rpc_req,
    common::ObSEArray<executor::ObProxyRpcParallelParam, 4> &parallel_param)
{
  int ret = OB_SUCCESS;
  ObRpcTableQueryRequest *query_request = NULL;
  int64_t query_request_len = sizeof(ObRpcTableQueryRequest);
  if (OB_ISNULL(rpc_req)
   || OB_ISNULL(query_request = dynamic_cast<ObRpcTableQueryRequest *>(rpc_req->get_rpc_request()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invlid argument to handle", KP(rpc_req), KP(query_request), K(ret), K_(rpc_trace_id));
  } else {
    const ObSEArray<int64_t, 1> &partition_ids = query_request->get_partition_ids();
    ObSEArray<ObRpcReq *, 4> rpc_reqs;
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_ids.count(); i++) {
      int64_t partition_id = partition_ids.at(i);
      LOG_DEBUG("handle_shard_rpc_obkv_query_request handle sub index", K(i), K(partition_id), K_(rpc_trace_id));
      ObRpcTableQueryRequest *sub_query_request = NULL;
      proxy::ObRpcReq *sub_rpc_req = NULL;
      ObRpcRequestSM *request_sm = NULL;
      void *buf = NULL;
      if (OB_ISNULL(buf = op_fixed_mem_alloc(sizeof(ObRpcTableQueryRequest)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("memory not enougth to init rpc sub_query_request", K(ret), K_(rpc_trace_id));
      } else {
        sub_query_request = new (buf) ObRpcTableQueryRequest(*query_request);
        if (OB_ISNULL(sub_rpc_req = ObRpcReq::allocate())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("could not allocate request, abort connection if need", K(ret), K_(rpc_trace_id));
        } else if (OB_ISNULL(request_sm =  ObRpcRequestSM::allocate())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("could not allocate request sm, net need abort connection", K(ret), K_(rpc_trace_id));
        }
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(sub_query_request)) {
        sub_query_request->set_packet_meta(query_request->get_packet_meta());
        sub_query_request->set_table_operation(query_request->get_query());
        if (OB_FAIL(sub_rpc_req->sub_rpc_req_init(rpc_req, request_sm, sub_query_request, query_request_len, cont_index_ + 1, partition_id))) {
          LOG_WDIAG("fail to init sub rpc req for query request", K(ret), K_(rpc_trace_id));
        } else if (request_sm->init(sub_rpc_req)) {
          LOG_WDIAG("fail to init sub request_sm for query request", K(ret), K_(rpc_trace_id));
        } else {
          executor::ObProxyRpcParallelParam param;
          param.partition_id_ = partition_id;
          param.request_ = sub_rpc_req;

          LOG_DEBUG("sub rpc_req init done", KPC(rpc_req), KPC(sub_rpc_req), K_(rpc_trace_id));
          cont_index_++;
          parallel_param.push_back(param);
          rpc_reqs.push_back(sub_rpc_req);
        }
      }

      if (OB_FAIL(ret)) {
        LOG_DEBUG("clean data in ObProxyRpcReqTableScanOp::handle_shard_rpc_obkv_batch_request", K(ret), K_(rpc_trace_id));
        if (OB_NOT_NULL(sub_rpc_req)) {
          sub_rpc_req->destroy();
        }
        if (OB_NOT_NULL(request_sm)) {
          request_sm->destroy();
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(rpc_req->alloc_sub_rpc_req_array(rpc_reqs.count()))) {
        LOG_WDIAG("fail to call alloc_sub_rpc_req_array", K(ret), K_(rpc_trace_id));
      } else if (OB_FAIL(rpc_req->set_sub_rpc_req(rpc_reqs))) {
        LOG_WDIAG("fail to call set_sub_rpc_req", K(ret), K_(rpc_trace_id));
      } else {
        LOG_DEBUG("succ to set_sub_rpc_req", K(ret), K_(rpc_trace_id));
      }
    } else {
      for (int64_t i = 0; i < rpc_reqs.count(); ++i) {
        ObRpcReq *sub_rpc_req = rpc_reqs.at(i);
        ObRpcRequestSM *request_sm = NULL;
        if (OB_NOT_NULL(sub_rpc_req)) {
          sub_rpc_req->destroy();
          request_sm = reinterpret_cast<ObRpcRequestSM *>(sub_rpc_req->sm_);
        }
        if (OB_NOT_NULL(request_sm)) {
          request_sm->destroy();
        }
      }
    }
  }
  return ret;
}

int ObProxyRpcReqTableScanOp::execute_rpc_request()
{
  int ret = OB_SUCCESS;

  proxy::ObRpcReq* input = NULL;

  if (OB_ISNULL(input = ObProxyRpcReqOperator::get_input())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("not have any input for table_scan", K(ret), KP(ObProxyRpcReqOperator::get_input()), K_(rpc_trace_id));
  } else if (OB_ISNULL(operator_async_task_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input for ObProxyRpcReqTableScanOp", K(ret), K_(rpc_trace_id));
  } else if (OB_FAIL(handle_shard_rpc_request(input, parallel_param_))) {
    LOG_WDIAG("handle shard rpc request failed", K(ret), K_(rpc_trace_id));
  } else {
    input->set_snet_state(proxy::ObRpcReq::ServerNetState::RPC_REQ_SERVER_SHARDING_REQUEST_HANDLING);
  }

  if (OB_SUCC(ret)) {
    bool is_stream_fetch = false;//input->is_stream_fetch_request();
    for (int64_t i = 0; i < parallel_param_.count(); i++) {
      //LOG_DEBUG("sub_sql before send", K(i), "sql", parallel_param.at(i).request_sql_,
      //          "database", parallel_param.at(i).shard_conn_->database_name_);
      if (OB_NOT_NULL(parallel_param_.at(i).request_)) {
        LOG_DEBUG("init sub req", "req", parallel_param_.at(i).request_, K_(rpc_trace_id));
        parallel_param_.at(i).request_->set_sub_req_inited(true);
      }
      LOG_DEBUG("sub_rpc before send", K(i), "rpc request", parallel_param_.at(i).request_,
                "info", parallel_param_.at(i).request_->get_rpc_request()->get_packet_meta(), K_(timeout_ms), K_(rpc_trace_id));
    }
    if (OB_FAIL(get_global_rpc_parallel_processor().open(*operator_async_task_, operator_async_task_->parallel_action_array_[0],
                                                     parallel_param_, &allocator_, is_stream_fetch, rpc_parallel_cont_ptr_, timeout_ms_))) {
      LOG_WDIAG("fail to op parallel processor", K(parallel_param_), K_(rpc_trace_id), K(ret));
    } else {
    }
  }

  return ret;
}

int ObProxyRpcReqTableScanOp::fetch_stream_response(int64_t cont_index) {
  int ret = OB_SUCCESS;
  UNUSED(cont_index);
  return ret;
}
}
}
}
