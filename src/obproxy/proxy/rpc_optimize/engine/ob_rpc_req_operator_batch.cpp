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
#include "common/ob_obj_compare.h"
#include "ob_rpc_req_operator_batch.h"
#include "lib/container/ob_se_array_iterator.h"
#include "proxy/rpc_optimize/executor/ob_rpc_req_parallel_execute_cont.h" //proxy::ObRpcReq
#include "obproxy/obkv/table/ob_table_rpc_request.h"
#include "obproxy/obkv/table/ob_table_rpc_response.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;

namespace oceanbase {
namespace obproxy {
namespace engine {
using namespace oceanbase::obproxy::obkv;

ObProxyRpcReqBatchOp::~ObProxyRpcReqBatchOp()
{
  release_cache_resultset(); // release cached result which has been returned by timeout/error resp,
                             // which not need fetch all resultset
}

void ObProxyRpcReqBatchOp::release_cache_resultset()
{
  resp_map_.reuse();
}

int ObProxyRpcReqBatchOp::handle_result(void *data, bool &is_final, proxy::ObRpcReq *&result)
{
  int ret = OB_SUCCESS;

  proxy::ObRpcReq *pres = NULL;
  // proxy::ObRpcReq *res_pres = NULL;
  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input, data is NULL", K(ret), K_(rpc_trace_id));
  } else if (OB_ISNULL(pres = reinterpret_cast<proxy::ObRpcReq*>(data))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input, pres type is not match", K(ret), K(pres), K_(rpc_trace_id));
  } else {
    if (pres->has_resultset_resp() || pres->has_error_resp()) {
      if (pres->has_error_resp()) {
        error_resp_count_++;
        if (OB_ISNULL(first_error_result_)) {
          first_error_result_ = pres;
          // first_error_result_ = res_pres;
        }
        LOG_INFO("obproxy rpc batch operation sub request meet error", K_(error_resp_count),
                 "cont_index", pres->get_cont_index(), "error_code", pres->get_error_code(), K_(rpc_trace_id));
      }
      if (OB_FAIL(handle_response_result(pres, is_final, result))) {
        LOG_WDIAG("failed to handle resultset_resp", K(ret), K_(rpc_trace_id));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("handle_result get response without resultset and is not error", KPC(pres), K(ret), K_(rpc_trace_id));
    }
    LOG_DEBUG("handle_result success", K(ret), K(pres), K_(rpc_trace_id));
  }
  return ret;
}

int ObProxyRpcReqBatchOp::handle_response_result(void *data, bool &is_final,
                                              proxy::ObRpcReq *&result)
{
  int ret = OB_SUCCESS;
  // resp_array_->push_back(reinterpret_cast<proxy::ObRpcReq*>(data));
  obkv::ObRpcResponse *last_response = NULL;
  proxy::ObRpcReq *pres = reinterpret_cast<proxy::ObRpcReq*>(data);

  if (OB_ISNULL(pres)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("data is NULL", K(ret));
  } else {
    proxy::ObRpcReq *rpc_req = get_input();
    resp_map_.set_refactored(pres->get_cont_index(), pres);
    LOG_DEBUG("ObProxyRpcReqBatchOp::handle_response_result", KPC(pres), "index", pres->get_cont_index(), "has_response", pres->has_resultset_resp(),
                        "partition id", pres->get_obkv_info().get_partition_id(), K(is_final), KPC(rpc_req), K_(rpc_trace_id));
  }

  if (OB_SUCC(ret) && is_final) {
    /* has received all response from server */
    proxy::ObRpcReq *rpc_req = get_input();//get_rpc_request();
    ObRpcTableBatchOperationRequest *request = NULL;//reinterpret_cast<ObRpcTableBatchOperationRequest *>(get_input()->rpc_request_);
    if (OB_ISNULL(rpc_req) 
      || OB_ISNULL(request = reinterpret_cast<ObRpcTableBatchOperationRequest *>(rpc_req->get_rpc_request()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("invalid shard request has set in", K(request), K(ret), K_(rpc_trace_id));
    } else {
      char *buf = NULL;
      obkv::ObRpcTableBatchOperationResponse *rpc_response = NULL;
      int64_t all_operation_count = request->get_table_operation().count();
      int64_t sub_request_count = request->get_partition_id_map().size();
      bool is_return_one_result = request->return_one_result();
      proxy::ObRpcOBKVInfo &obkv_info = rpc_req->get_obkv_info();

      LOG_DEBUG("need alloc memory", K(&allocator_), "size", sizeof(obkv::ObRpcTableBatchOperationResponse), K(buf),
                K_(rpc_trace_id));

      if (OB_SUCC(ret)) {
        if (OB_ISNULL(buf = rpc_req->alloc_rpc_response(sizeof(obkv::ObRpcTableBatchOperationResponse)))) {
          ret = common::OB_ALLOCATE_MEMORY_FAILED;
          LOG_WDIAG("not enougth alloc memory", K_(rpc_trace_id));
        } else {
          rpc_response = new (buf) obkv::ObRpcTableBatchOperationResponse();
          //rpc_response->get_batch_result().set_entity_factory(&(rpc_response->get_table_entity_factory()));
          rpc_req->set_rpc_response_len(sizeof(obkv::ObRpcTableBatchOperationResponse));

          LOG_DEBUG("result is", K(result), K(all_operation_count), K(sub_request_count), K_(rpc_trace_id));

          if (error_resp_count_ > 0) {
            proxy::ObRpcReq *res_error_result = NULL;// = first_error_result_->get_response_req();
            LOG_DEBUG("obproxy rpc batch handle not all sub requests succ", K(all_operation_count),
                      K_(error_resp_count), K_(first_error_result), K(res_error_result), K_(rpc_trace_id));
            if (OB_NOT_NULL(first_error_result_)
                    // && OB_NOT_NULL(res_error_result = first_error_result_->get_response_req())
                    && OB_NOT_NULL(res_error_result = first_error_result_)
                    && 0 != res_error_result->get_error_code()
                    // && OB_NOT_NULL(first_error_result_->get_rpc_response()) /* maybe it's NULL */
                    ) {
              rpc_response->get_result_code().rcode_ = res_error_result->get_error_code();//first_error_result_->get_rpc_response()->get_result_code().rcode_;
              last_response = first_error_result_->get_rpc_response();
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WDIAG("error unexpected to get null error first_error_result for batch rpc request handle, could not be here", K(ret),
                      "error_code", res_error_result->get_error_code(), "response", first_error_result_->get_rpc_response(), K(res_error_result), K_(rpc_trace_id));
            }
          } else if (is_return_one_result) {
            if (OB_FAIL(generate_one_result_resp(rpc_response->get_batch_result(), last_response))) {
              LOG_WDIAG("fail to generate one result resp for batch operation", K(ret));
            }
          } else {
            if (OB_FAIL(generate_normal_resp(rpc_response->get_batch_result(), *request, last_response))) {
              LOG_WDIAG("fail to generate response for batch operation", K(ret), K_(rpc_trace_id));
            }
          } /* if (error_resp_count_ > 0) */
        } /* if (OB_ISNULL(buf = (char*)op_fixed_mem_alloc(sizeof(obkv::ObRpcTableBatchOperationResponse)))) */
      } /* if (OB_SUCC(ret)) */
      if (OB_SUCC(ret)) {
        LOG_DEBUG("batch operation get result", "batch_result", rpc_response->get_batch_result(), K_(rpc_trace_id));
        // need to update sum of the length in meta
        if (OB_ISNULL(last_response)) {
          /* just used rpc_request meta info when meet error */
          rpc_response->set_packet_meta(rpc_req->get_rpc_request()->get_packet_meta());
          rpc_response->get_packet_meta().rpc_header_.flags_ |= ObRpcPacketHeader::RESP_FLAG;
        } else {
          rpc_response->set_packet_meta(last_response->get_packet_meta());
        }
        rpc_response->get_packet_meta().ez_header_.ez_payload_size_ = (uint32_t)(rpc_response->get_encode_size() - 16); //EZ_HEADER_LEN
        obkv_info.set_resp(true);
        rpc_req->set_rpc_response(rpc_response);
        result_ = rpc_req;
        result = rpc_req;
        LOG_DEBUG("rpc response", K(rpc_response), K(*rpc_response), K(result), K_(rpc_trace_id));
      }

      release_cache_resultset();
    }
  } else {
    result = NULL;
  }
  return ret;
}

int ObProxyRpcReqBatchOp::generate_one_result_resp(obkv::ObTableBatchOperationResult &batch_result,
                                                   obkv::ObRpcResponse *&last_response)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("generate one result resp", K_(rpc_trace_id));
  ObTableOperationResult table_operation_res;
  RPC_BATCH_RESP_MAP::iterator it = resp_map_.begin();
  RPC_BATCH_RESP_MAP::iterator end = resp_map_.end();
  for (int i = 0; it != end && OB_SUCC(ret); it++, i++) {
    proxy::ObRpcReq *sub_res_rpc_req = it->second;
    ObRpcTableBatchOperationResponse *sub_batch_operation_response = NULL;
    int64_t table_result_count = 0;
    // resp_map_.get_refactored(j);
    if (OB_ISNULL(sub_res_rpc_req)
        || OB_ISNULL(sub_batch_operation_response
                     = dynamic_cast<obkv::ObRpcTableBatchOperationResponse *>(sub_res_rpc_req->get_rpc_response()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("invalid response to handle", K(sub_res_rpc_req), "has_response",
                sub_res_rpc_req == NULL ? NULL : sub_res_rpc_req->get_rpc_response(), K(ret), K_(rpc_trace_id));
      break;
    } else if (OB_FALSE_IT(table_result_count
                           = sub_batch_operation_response->get_batch_result().get_table_operation_result_count())) {
    } else if (table_result_count != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unexpected table operation count in returning_one_result mode", K(ret), K_(rpc_trace_id), "count",
                table_result_count);
    } else {
      const ObTableOperationResult &mmres = sub_batch_operation_response->get_batch_result().at(0);
      last_response = sub_res_rpc_req->get_rpc_response();
      if (i == 0) {
        table_operation_res = mmres;
      } else {
        table_operation_res.set_affected_rows(mmres.get_affected_rows() + table_operation_res.get_affected_rows());
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(batch_result.push_back(table_operation_res))) {
    LOG_WDIAG("fail to push back table operation result", K(ret), K_(rpc_trace_id));
  }
  return ret;
}

int ObProxyRpcReqBatchOp::generate_normal_resp(obkv::ObTableBatchOperationResult &batch_result,
                                               obkv::ObRpcTableBatchOperationRequest &batch_request,
                                               obkv::ObRpcResponse *&last_response)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<int64_t, 4> sub_request_index_arr;
  common::ObSEArray<int64_t, 4> all_operation_index_arr;
  PARTITION_ID_MAP &partid_to_index_map = batch_request.get_partition_id_map();
  // int64_t count = partid_to_index_map.size();
  int64_t all_operation_count = batch_request.get_sub_req_count();
  int64_t sub_request_count = partid_to_index_map.size();

  for (int64_t i = 0; OB_SUCC(ret) && i < sub_request_count; i++) {
    if (OB_FAIL(sub_request_index_arr.push_back(0))) {
      LOG_WDIAG("fail to call push_back for sub_request_index_arr", K(ret), K(i), K(sub_request_count),
                K_(rpc_trace_id));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_operation_count; i++) {
    if (OB_FAIL(all_operation_index_arr.push_back(0))) {
      LOG_WDIAG("fail to call push_back for all_operation_index_arr", K(ret), K(i), K(all_operation_count),
                K_(rpc_trace_id));
    }
  }

  PARTITION_ID_MAP::iterator it = partid_to_index_map.begin();
  PARTITION_ID_MAP::iterator end = partid_to_index_map.end();
  int index = 0;
  for (; it != end && OB_SUCC(ret); it++, index++) {
    ObSEArray<int64_t, 4> &index_arr = it->second;
    for (int64_t i = 0; i < index_arr.count(); i++) {
      all_operation_index_arr.at(index_arr[i]) = index;
      LOG_DEBUG("batch index info", "pos", index_arr[i], K(index), "partition id", it->first, K_(rpc_trace_id));
    }
  }

  for (int64_t i = 0; i < all_operation_count && OB_SUCC(ret); i++) {
    proxy::ObRpcReq *sub_res_rpc_req = NULL;
    ObRpcTableBatchOperationResponse *sub_batch_operation_response = NULL;
    // resp_map_.get_refactored(j);
    int64_t index = all_operation_index_arr[i];
    if (OB_FAIL(resp_map_.get_refactored(index, sub_res_rpc_req))
        || OB_ISNULL(sub_res_rpc_req)
        || OB_ISNULL(sub_batch_operation_response = dynamic_cast<obkv::ObRpcTableBatchOperationResponse *>(sub_res_rpc_req->get_rpc_response()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("invalid response to handle", K(sub_res_rpc_req), "has_response",
                sub_res_rpc_req == NULL ? NULL : sub_res_rpc_req->get_rpc_response(), K(ret), K(index),
                K_(rpc_trace_id));
      break;
    } else {
      int64_t sub_request_index = sub_request_index_arr[index];
      LOG_DEBUG("batch operation get result1", K(sub_batch_operation_response), K(i), K(sub_request_index),
                K_(rpc_trace_id));
      if (sub_request_index >= sub_batch_operation_response->get_batch_result().get_table_operation_result_count()) {
        if (1 == sub_batch_operation_response->get_batch_result().get_table_operation_result_count()
            && ObTableEntityType::ET_HKV == batch_request.get_entity_type()) {
          sub_request_index = 0; // 对于hbase请求，并且response为1个情况情况下，如果request
                                 // index大于response结果，取第一个response覆盖所有的response即可
          LOG_DEBUG("Hbase request result set is 1, and the first result set is used by default",
                    KPC(sub_batch_operation_response), K_(rpc_trace_id));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("get a invalid sub_request_index", K(sub_request_index), KPC(sub_batch_operation_response),
                    KPC(sub_res_rpc_req), K(index), K_(rpc_trace_id));
        }
      } else {
        // do nothing
      }
      if (OB_SUCC(ret)) {
        LOG_DEBUG("batch operation get result2", "batch", sub_batch_operation_response->get_batch_result(), "result",
                  sub_batch_operation_response->get_batch_result().at(sub_request_index), K_(rpc_trace_id));
        const ObTableOperationResult &mmres = sub_batch_operation_response->get_batch_result().at(sub_request_index);
        LOG_DEBUG("batch operation get result3", K(sub_batch_operation_response), K(i), K(mmres), K_(rpc_trace_id));
        batch_result.push_back(mmres);
        last_response = sub_res_rpc_req->get_rpc_response();
        sub_request_index_arr.at(index) += 1;
      }
    }
  }
  sub_request_index_arr.reset();
  all_operation_index_arr.reset();

  return ret;
}

} // namespace engine
}
}
