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
#include "ob_rpc_req_operator_query.h"
#include "lib/container/ob_se_array_iterator.h"
#include "proxy/rpc_optimize/executor/ob_rpc_req_parallel_execute_cont.h" //proxy::ObRpcReq
#include "obproxy/obkv/table/ob_table_rpc_response.h"
#include "ob_rpc_req_operator_async_task.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::obkv;
using namespace oceanbase::obproxy::proxy;

namespace oceanbase {
namespace obproxy {
namespace engine {

ObProxyRpcReqQueryOp::~ObProxyRpcReqQueryOp()
{
  // resp_array_.reset();
  release_cache_resultset();
}

void ObProxyRpcReqQueryOp::release_cache_resultset()
{
  resp_array_.reset();
}

int ObProxyRpcReqQueryOp::handle_result(void *data, bool &is_final, proxy::ObRpcReq *&result)
{
  int ret = OB_SUCCESS;

  proxy::ObRpcReq *pres = NULL;
  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input, data is NULL", K(ret), K_(rpc_trace_id));
  } else if (OB_ISNULL(pres = reinterpret_cast<proxy::ObRpcReq*>(data))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input, pres type is not match", K(ret), K_(rpc_trace_id));
  } else {
    if (pres->has_resultset_resp() || pres->has_error_resp()) {
      if (pres->has_error_resp()) {
        LOG_INFO("obproxy rpc table query operation sub request meet error",
                 "cont_index", pres->get_cont_index(), "error_code", pres->get_error_code(), K_(rpc_trace_id));
      }
      if (OB_FAIL(handle_response_result(pres, is_final, result))) {
        LOG_WDIAG("failed to handle resultset_resp", K(ret), K_(rpc_trace_id));
      }
    } else {
      LOG_WDIAG("handle_result get response without resultset", KPC(pres), K_(rpc_trace_id));
    }
    LOG_DEBUG("handle_result success", K(ret), K(pres), K_(rpc_trace_id));
  }
  return ret;
}

int ObProxyRpcReqQueryOp::handle_response_result(void *data, bool &is_final,
                                              proxy::ObRpcReq *&result)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(data)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("data is NULL", K(ret), KP(data));
  } else {
    resp_array_.push_back(reinterpret_cast<proxy::ObRpcReq*>(data));
  }

  if (OB_SUCC(ret) && is_final) {
    /* has received all response from server */
    int64_t count = resp_array_.count();
    char *buf = NULL;
    bool has_inited_meta = false;
    int64_t i = 0;
    ObRpcReq *rpc_req = get_input();
    LOG_DEBUG("ObProxyRpcReqQueryOp::handle_response_result", K(count), K_(rpc_trace_id));
    obkv::ObRpcTableQueryResponse *rpc_response = NULL;
    ObRpcOBKVInfo &obkv_info = rpc_req->get_obkv_info();

    if (OB_ISNULL(buf = rpc_req->alloc_rpc_response(sizeof(obkv::ObRpcTableQueryResponse)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("invalid alloc memory", K(ret));
    } else {
      result = rpc_req;
      rpc_response = new (buf) obkv::ObRpcTableQueryResponse();
      rpc_req->set_rpc_response(rpc_response);
      rpc_req->set_rpc_response_len(sizeof(obkv::ObRpcTableQueryResponse));
    }
    for (i = 0; i < count && OB_SUCC(ret); i++) {
      obkv::ObRpcTableQueryResponse *res_imp = NULL;
      proxy::ObRpcReq *res = resp_array_.at(i);

      if (OB_ISNULL(res)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("invalid response to handle, rpc_rpc is NULL", K_(rpc_trace_id), KP(res));
      } else if (res->has_error_resp()) {
        ObRpcResultCode &result_code = rpc_response->get_result_code();
        result_code.rcode_ = res->get_error_code();
        LOG_WDIAG("handle shard rpc request meet error code", K(ret), K_(rpc_trace_id));
        res = NULL;
        break;
      } else if (OB_ISNULL(res_imp = dynamic_cast<obkv::ObRpcTableQueryResponse *>(res->get_rpc_response()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("invalid response to handle, cannot cast to ObRpcTableQueryResponse", K_(rpc_trace_id));
      } else {
        if (!has_inited_meta) {
          int tmp_ret = OB_SUCCESS;
          rpc_response->set_packet_meta(res_imp->get_packet_meta());
          tmp_ret = rpc_response->get_query_result().add_all_property_shallow_copy(res_imp->get_query_result());
          if (OB_SUCC(tmp_ret)) {
            has_inited_meta = true;
          } else {
            LOG_WDIAG("invalid properity get from server", K(tmp_ret), K_(rpc_trace_id));
          }
        }
        int tmp = rpc_response->get_query_result().add_all_row_shallow_copy(res_imp->get_query_result());

        LOG_DEBUG("ObProxyRpcReqQueryOp::handle_response_result", KPC(res_imp),
                  "row_count", res_imp->get_query_result().get_row_count(),
                  "buf", res_imp->get_query_result().get_buf(), 
                  "return", tmp, K_(rpc_trace_id));
        res = NULL;
      }
    }

    if (OB_SUCC(ret)) {
      if (!has_inited_meta) {
        ObRpcRequest *request = NULL;
        if (OB_ISNULL(request = rpc_req->get_rpc_request())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("shard query meet error, not init meta and get NULL request", KPC(rpc_req));
        } else {
          ObRpcPacketMeta &meta = rpc_response->get_packet_meta();
          memcpy(&meta.ez_header_, &(request->get_packet_meta().ez_header_), sizeof(meta.ez_header_));
          memcpy(&meta.rpc_header_, &(request->get_packet_meta().rpc_header_), sizeof(meta.rpc_header_));
          meta.rpc_header_.flags_ &= (uint16_t)~(ObRpcPacketHeader::REQUIRE_REROUTING_FLAG);  // clear reroute flag
          meta.rpc_header_.flags_ |= obrpc::ObRpcPacketHeader::RESP_FLAG;
        }
      }
      rpc_response->get_packet_meta().ez_header_.ez_payload_size_ = (uint32_t)rpc_response->get_packet_meta().rpc_header_.hlen_
          + (int32_t)(rpc_response->get_query_result().get_result_size());
      LOG_DEBUG("ObProxyRpcReqQueryOp::handle_response_result final info", "row_count",
                rpc_response->get_query_result().get_row_count(),
                "field_count", rpc_response->get_query_result().get_property_count(),
                "new size",  rpc_response->get_packet_meta().ez_header_.ez_payload_size_, K_(rpc_trace_id));
      obkv_info.set_resp(true);
      rpc_req->set_rpc_response(rpc_response);
      result = rpc_req;
      result_ = rpc_req;
    }
  }
  return ret;
}

} //end of engine
} //end of obproxy
} //end of oceanbase
