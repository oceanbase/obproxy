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
#include "ob_rpc_req_operator_ls.h"
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

ObProxyRpcReqLSOp::~ObProxyRpcReqLSOp() {
  release_cache_resultset();
}

void ObProxyRpcReqLSOp::release_cache_resultset() {
  resp_map_.reuse();
}

int ObProxyRpcReqLSOp::handle_result(void *data, bool &is_final, proxy::ObRpcReq *&result)
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
        }
        LOG_INFO("obproxy rpc ls operation sub request meet error", K_(error_resp_count),
                 "cont_index", pres->get_cont_index(), "error_code", pres->get_error_code(),
                 K_(rpc_trace_id));
      }
      if (OB_FAIL(handle_response_result(pres, is_final, result))) {
        LOG_WDIAG("failed to handle resultset_resp", K(ret), K_(rpc_trace_id));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("handle_result get response without resultset and is not error", KPC(pres),
                K(ret), K_(rpc_trace_id));
    }
    LOG_DEBUG("handle_result success", K(ret), K(pres), K_(rpc_trace_id));
  }
  return ret;
}

int ObProxyRpcReqLSOp::handle_response_result(void *data, bool &is_final, proxy::ObRpcReq *&result) 
{
  int ret = OB_SUCCESS;
  // resp_array_->push_back(reinterpret_cast<proxy::ObRpcReq*>(data));
  obkv::ObRpcResponse *last_response = NULL;
  proxy::ObRpcReq *pres = reinterpret_cast<proxy::ObRpcReq*>(data);

  if (OB_ISNULL(pres)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("data is NULL", K(ret), K_(rpc_trace_id));
  } else {
    proxy::ObRpcReq *rpc_req = get_input();
    resp_map_.set_refactored(pres->get_cont_index(), pres);
    LOG_DEBUG("ObProxyRpcReqLSOp::handle_response_result", KPC(pres), "index", pres->get_cont_index(),
              "has_response", pres->has_resultset_resp(), K(is_final), KPC(rpc_req), K_(rpc_trace_id));
  }

  if (OB_SUCC(ret) && is_final) {
    proxy::ObRpcReq *rpc_req = get_input();
    ObRpcTableLSOperationRequest *request = NULL;
    if (OB_ISNULL(rpc_req)
        || OB_ISNULL(request = reinterpret_cast<ObRpcTableLSOperationRequest *>(rpc_req->get_rpc_request()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("invalid shard request has set in", K(request), K(ret), K_(rpc_trace_id));
    } else {
      proxy::ObRpcOBKVInfo &obkv_info = rpc_req->get_obkv_info();
      bool return_one_result = request->get_operation().return_one_result();
      char *buf = NULL;
      obkv::ObRpcTableLSOperationResponse *rpc_response = NULL;
      LOG_DEBUG("need alloc memory", K(&allocator_), "size", sizeof(obkv::ObRpcTableLSOperationResponse), K(buf), K_(rpc_trace_id));

      if (OB_ISNULL(buf = rpc_req->alloc_rpc_response(sizeof(obkv::ObRpcTableLSOperationResponse)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("not enougth alloc memory", K_(rpc_trace_id));
      } else {
        // init rpc response 
        rpc_response = new (buf) obkv::ObRpcTableLSOperationResponse();
        //rpc_response->get_ls_result().set_entity_factory(&(rpc_response->get_table_entity_factory()));
        //rpc_response->get_ls_result().set_allocator(&rpc_response->get_allocator());
        rpc_response->get_ls_result().set_all_properties_names(request->get_operation().get_all_properties_names());
        rpc_response->get_ls_result().set_all_rowkey_names(request->get_operation().get_all_rowkey_names());
        rpc_req->set_rpc_response_len(sizeof(obkv::ObRpcTableLSOperationResponse));

        // handle error resp
        if (error_resp_count_ > 0) {
          proxy::ObRpcReq *res_error_result = NULL;
          LOG_DEBUG("obproxy rpc ls handle not all sub requests succ", K_(error_resp_count), K_(first_error_result),
                    K(res_error_result), K_(rpc_trace_id));
          if (OB_NOT_NULL(first_error_result_) &&
              OB_NOT_NULL(res_error_result = first_error_result_) &&
              0 != res_error_result->get_error_code()) {
            rpc_response->get_result_code().rcode_ = res_error_result->get_error_code();
            last_response = first_error_result_->get_rpc_response();
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("error unexpected to get null error first_error_result for ls rpc request handle, could not be here", K(ret),
                    "error_code", res_error_result->get_error_code(), "response", first_error_result_->get_rpc_response(),
                    K(res_error_result), K_(rpc_trace_id));
          }
        } else {
          if (return_one_result) {
            if (OB_FAIL(generate_one_result_resp(rpc_response->get_ls_result(), last_response))) {
              LOG_WDIAG("fail to generate one result resp", K(ret));
            }
          } else {
            if (OB_FAIL(generate_normal_resp(rpc_response->get_ls_result(), *request, last_response))) {
              LOG_WDIAG("fail to generate resp for ls operation", K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        LOG_DEBUG("ls operation get result", "ls_result", rpc_response->get_ls_result(), K_(rpc_trace_id));
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

int ObProxyRpcReqLSOp::generate_one_result_resp(ObTableLSOpResult &ls_op_result,
                                                ObRpcResponse *& last_response)
{
  int ret = OB_SUCCESS;
  ObTableSingleOpResult single_op_result;
  LOG_DEBUG("generate one result resp", K_(rpc_trace_id));
  RPC_LS_RESP_MAP::iterator it = resp_map_.begin();
  RPC_LS_RESP_MAP::iterator end = resp_map_.end();
  ObTableTabletOpResult tablet_op_result;
  tablet_op_result.set_all_rowkey_names(&ls_op_result.get_rowkey_names());
  tablet_op_result.set_all_properties_names(&ls_op_result.get_properties_names());

  for (int64_t i = 0; it != end && OB_SUCC(ret); it++, i++) {
    proxy::ObRpcReq *sub_res_rpc_req = it->second;
    ObRpcTableLSOperationResponse *sub_ls_operation_response = NULL;
    // i means sub request order not origin request order
    // i == 0 maybe not the resposne of the first sub request
    if (OB_ISNULL(sub_res_rpc_req) ||
        OB_ISNULL(sub_ls_operation_response =
                      dynamic_cast<obkv::ObRpcTableLSOperationResponse *>(sub_res_rpc_req->get_rpc_response()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("invalid response to handle", K(sub_res_rpc_req), "has_response",
                sub_res_rpc_req == NULL ? NULL : sub_res_rpc_req->get_rpc_response(), K(ret), "sub_request_index", i,
                K_(rpc_trace_id));
    } else {
      ObTableLSOpResult &lso_mmres = sub_ls_operation_response->get_ls_result();
      int tablet_res_count = lso_mmres.get_tablet_op_result().count();
      for (int j = 0; j < tablet_res_count && OB_SUCC(ret); j ++ ) {
        int single_res_count = lso_mmres.get_tablet_op_result().at(j).get_single_op_result().count();
        if (single_res_count != 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("unexpected single result count in returning_one_result mode", K(ret), K(single_res_count));
        } else {
          ObTableTabletOpResult &tablet_mmres = lso_mmres.get_tablet_op_result().at(j);
          ObTableSingleOpResult &mmres = tablet_mmres.get_single_op_result().at(0);
          if (i == 0 && j == 0) {
            single_op_result = mmres;
          } else {
            single_op_result.set_affected_rows(mmres.get_affected_rows() + single_op_result.get_affected_rows());
          }
          LOG_DEBUG("ls operation get result succ", K(sub_ls_operation_response), K(mmres), K(i), K(j),
                    K_(rpc_trace_id));
        }
      }
      if (OB_SUCC(ret)) {
        last_response = sub_res_rpc_req->get_rpc_response();
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(tablet_op_result.get_single_op_result().push_back(single_op_result))) {
      LOG_WDIAG("fail to push back single operation", K(ret));
    } else if (OB_FAIL(ls_op_result.get_tablet_op_result().push_back(tablet_op_result))) {
      LOG_WDIAG("fail to push back tablet operation", K(ret));
    }
  }
  return ret;
}

int ObProxyRpcReqLSOp::generate_normal_resp(ObTableLSOpResult &ls_op_result,
                                            ObRpcTableLSOperationRequest &ls_request,
                                            ObRpcResponse *&last_response)
{
  int ret = OB_SUCCESS;
  int64_t *all_ls_op_index_arr;                                           // record which ls resp belongs
  int64_t *all_tablet_op_index_arr;                                       // record which tablet resp belongs
  int64_t *all_single_op_index_arr;                                       // record which single resp belongs
  LS_TABLET_ID_MAP &ls_id_tablet_id_map = ls_request.get_ls_id_tablet_id_map(); // the map record ls id of all tabelt_id
  TABLET_ID_INDEX_MAP &tablet_id_map = ls_request.get_tablet_id_index_map(); // the map record original index of all  single op

  int64_t all_operation_count = ls_request.get_sub_req_count();
  int64_t all_tablet_op_count = ls_request.get_operation().get_tablet_ops().count();
  int64_t sub_tablet_request_count = tablet_id_map.size();
  int64_t sub_ls_request_count = ls_id_tablet_id_map.size();

  LOG_DEBUG("received ls shard resp completly", K(sub_ls_request_count), K(sub_tablet_request_count),
            K(all_operation_count), K_(rpc_trace_id));
  if (OB_FAIL(init_index_arr(all_ls_op_index_arr, all_operation_count))) {
    LOG_WDIAG("fail to init all ls operation index", K(ret), K_(rpc_trace_id));
  } else if (OB_FAIL(init_index_arr(all_tablet_op_index_arr, all_operation_count))) {
    LOG_WDIAG("fail to init ls operation index", K(ret), K_(rpc_trace_id));
  } else if (OB_FAIL(init_index_arr(all_single_op_index_arr, all_operation_count))) {
    LOG_WDIAG("fail to init sginle operation index", K(ret), K_(rpc_trace_id));
  }
  LS_TABLET_ID_MAP::iterator ls_it = ls_id_tablet_id_map.begin();
  LS_TABLET_ID_MAP::iterator ls_end = ls_id_tablet_id_map.end();

  // calc sub response index of all single operation:
  // we build the parallel async task in the same way, so the index of iterator is the index of sub response of resp_map
  for (int ls_index = 0; ls_it != ls_end && OB_SUCC(ret); ls_it++, ls_index++) {
    ObSEArray<int64_t, 4> &tablet_id_arr = ls_it->second;
    ObSEArray<int64_t, 4>::iterator tablet_it = tablet_id_arr.begin();
    ObSEArray<int64_t, 4>::iterator tablet_end = tablet_id_arr.end();
    for (int tablet_index = 0; tablet_it != tablet_end && OB_SUCC(ret); tablet_it++, tablet_index++) {
      int64_t tablet_id = *tablet_it;
      ObSEArray<int64_t, 4> index_arr;
      if (OB_FAIL(tablet_id_map.get_refactored(tablet_id, index_arr))) {
        LOG_WDIAG("fail to get index arr", K(tablet_id), K(ret), K_(rpc_trace_id));
      } else {
        ObSEArray<int64_t, 4>::iterator single_it = index_arr.begin();
        ObSEArray<int64_t, 4>::iterator singel_end = index_arr.end();

        for (int single_index = 0; single_it != singel_end; single_it++, single_index++) {
          int64_t origin_location = index_arr[single_index];
          if (origin_location >= all_operation_count) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("unexpected single op index", K(origin_location), K(all_operation_count), K(ret),
                      K_(rpc_trace_id));
          } else {
            all_ls_op_index_arr[origin_location] = ls_index;
            all_tablet_op_index_arr[origin_location] = tablet_index;
            all_single_op_index_arr[origin_location] = single_index;
          }
        }
      } // loop original single op index
    }   // loop tablet id
  }     // loop ls id
  
  int offset = 0;
  for (int i = 0; i < all_tablet_op_count && OB_SUCC(ret); i++) {
    ObTableTabletOpResult tablet_op_result;
    tablet_op_result.set_all_rowkey_names(&ls_op_result.get_rowkey_names());
    tablet_op_result.set_all_properties_names(&ls_op_result.get_properties_names());
    // push back a default tablet op
    if (OB_FAIL(ls_op_result.get_tablet_op_result().push_back(tablet_op_result))) {
      LOG_WDIAG("fail to push back tablet op resutl", K(ret));
    }
    int64_t all_single_op_count = ls_request.get_operation().get_tablet_ops().at(i).count();
    for (int j = 0; j < all_single_op_count && OB_SUCC(ret); j++) {
      proxy::ObRpcReq *sub_res_rpc_req = NULL;
      ObRpcTableLSOperationResponse *sub_ls_operation_response = NULL;
      int64_t ls_index = all_ls_op_index_arr[offset];
      int64_t tablet_index = all_tablet_op_index_arr[offset];
      int64_t single_index = all_single_op_index_arr[offset];
      if (OB_FAIL(resp_map_.get_refactored(ls_index, sub_res_rpc_req)) || OB_ISNULL(sub_res_rpc_req)
          || OB_ISNULL(sub_ls_operation_response
                       = dynamic_cast<obkv::ObRpcTableLSOperationResponse *>(sub_res_rpc_req->get_rpc_response()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("invalid response to handle", K(sub_res_rpc_req), "has_response",
                  sub_res_rpc_req == NULL ? NULL : sub_res_rpc_req->get_rpc_response(), K(ret), K(ls_index),
                K(tablet_index), K(single_index), "request_offset", offset, K_(rpc_trace_id));
      } else {
        ObTableLSOpResult &lso_mmres = sub_ls_operation_response->get_ls_result();
        if (OB_UNLIKELY(lso_mmres.get_tablet_op_result().count() <= tablet_index
                        || lso_mmres.get_tablet_op_result().at(tablet_index).get_single_op_result().count()
                               <= single_index)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("unexpected tablet result index or single result index", K(ls_index), K(tablet_index),
                    K(single_index), K(ret), K_(rpc_trace_id));
        } else if (lso_mmres.get_tablet_op_result().at(tablet_index).get_single_op_result().count() == 1
                   && ObTableEntityType::ET_HKV == ls_request.get_entity_type()) {
          single_index = 0;
        } else {
          ObTableTabletOpResult &tablet_mmres = lso_mmres.get_tablet_op_result().at(tablet_index);
          ObTableSingleOpResult &mmres = tablet_mmres.get_single_op_result().at(single_index);
          if (OB_FAIL(ls_op_result.get_tablet_op_result().at(i).get_single_op_result().push_back(mmres))) {
            LOG_WDIAG("fail to push single op result", K(ret));
          } else {
            last_response = sub_res_rpc_req->get_rpc_response();
            LOG_DEBUG("ls operation get result succ", K(sub_ls_operation_response), K(mmres), K(offset), K(ls_index),
                      K(tablet_index), K(single_index), K_(rpc_trace_id));
          }
        }
        offset++;
      }
    }
  }
  free_index_arr(all_ls_op_index_arr, all_operation_count);
  free_index_arr(all_tablet_op_index_arr, all_operation_count);
  free_index_arr(all_single_op_index_arr, all_operation_count);
  return ret;
}

int ObProxyRpcReqLSOp::init_index_arr(int64_t *&arr, const int64_t count)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  if (OB_ISNULL(buf = op_fixed_mem_alloc(sizeof(int64_t) * count))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMSET(buf, 0, sizeof(int64_t) * count);
    arr = static_cast<int64_t*>(buf);
  }
  return ret;
}

void ObProxyRpcReqLSOp::free_index_arr(int64_t *&arr, const int64_t count)
{
  if (OB_NOT_NULL(arr)) {
    op_fixed_mem_free(arr, sizeof(int64_t) * count);
  }
}

}
}
}
