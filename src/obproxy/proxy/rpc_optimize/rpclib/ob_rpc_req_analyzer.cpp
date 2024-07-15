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
#include "ob_rpc_req_analyzer.h"
#include "rpc/obmysql/ob_mysql_global.h"
#include "obkv/table/ob_table_rpc_request.h"
#include "obkv/table/ob_table_rpc_response.h"
#include "obkv/table/ob_rpc_struct.h"
#include "obutils/ob_config_server_processor.h"
#include "obutils/ob_proxy_config.h"
#include "utils/ob_proxy_utils.h"
#include "proxy/mysqllib/ob_mysql_common_define.h"
#include "proxy/rpc_optimize/ob_rpc_req.h"
#include "proxy/rpc_optimize/rpclib/ob_table_query_async_entry.h"
#include "proxy/rpc_optimize/rpclib/ob_rpc_req_ctx.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
using namespace common;
using namespace event;
using namespace obmysql;
using namespace obkv;
using namespace obutils;

int ObProxyRpcReqAnalyzer::analyze_rpc_req(ObProxyRpcReqAnalyzeCtx &ctx, ObRpcReqAnalyzeNewStatus &status, ObRpcReq &ob_rpc_req)
{
  int ret = OB_SUCCESS;
  status = RPC_ANALYZE_NEW_CONT;
  obkv::ObProxyRpcType rpc_type = ob_rpc_req.get_rpc_type();
  const ObRpcReqTraceId &rpc_trace_id = ob_rpc_req.get_trace_id();

  switch (rpc_type)
  {
  case OBPROXY_RPC_OBRPC: {
    if (OB_FAIL(analyze_obkv_req(ctx, status, ob_rpc_req))) {
      LOG_WDIAG("fail to call analyze_obkv_req", K(ret), K(status), K(ob_rpc_req), K(rpc_trace_id));
    }
    break;
  }
  case OBPROXY_RPC_HBASE:
  case OBPROXY_RPC_UNKOWN:
  default:
    ret = OB_ERR_UNEXPECTED;
    status = RPC_ANALYZE_NEW_ERROR;
    LOG_WDIAG("ObProxyRpcReqAnalyzer::analyze_rpc_req get an wrong rpc type", K(rpc_type), K(status), K(ret));
    break;
  }
  // }

  return ret;
}

int ObProxyRpcReqAnalyzer::handle_req_header(ObProxyRpcReqAnalyzeCtx &ctx, ObRpcReqAnalyzeNewStatus &status, ObRpcReq &ob_rpc_req)
{
  int ret = OB_SUCCESS;
  status = RPC_ANALYZE_NEW_CONT;
  char *req_buf = NULL;
  int64_t req_buf_len = 0;
  const ObRpcReqTraceId &rpc_trace_id = ob_rpc_req.get_trace_id();
  if (!ctx.is_response_) {
    req_buf = ob_rpc_req.get_request_buf();
    req_buf_len = ob_rpc_req.get_request_buf_len();
  } else {
    req_buf = ob_rpc_req.get_response_buf();
    req_buf_len = ob_rpc_req.get_response_buf_len();
  }

  LOG_DEBUG("ObProxyRpcReqAnalyzer::handle_request_header get request", K(ob_rpc_req), K(rpc_trace_id));

  if (req_buf_len < ObProxyRpcReqAnalyzer::RPC_NET_HEADER) {
    ret = OB_ERR_UNEXPECTED;
    status = RPC_ANALYZE_NEW_ERROR;
    LOG_WDIAG("ob_rpc_req buffer length less than RPC_NET_HEADER", K(req_buf_len), K(rpc_trace_id));
  } else {
    obkv::ObRpcEzHeader ezhdr;
    ezhdr.ez_payload_size_ = 0;

    for (int i = 0; i < 4; i++) {
      ezhdr.magic_header_flag_[i] = uint1korr(&req_buf[i]);
    }
    for (int i = 0; i < 4; i++) {
      ezhdr.ez_payload_size_ <<= 8;
      ezhdr.ez_payload_size_ += uint1korr(&req_buf[4 + i]);
    }
    ezhdr.chid_ = uint4korr(&req_buf[8]);
    ezhdr.reserved_ = uint4korr(&req_buf[12]);

    if (0 == memcmp(ezhdr.magic_header_flag_, obkv::ObRpcEzHeader::MAGIC_HEADER_FLAG, sizeof(obkv::ObRpcEzHeader::MAGIC_HEADER_FLAG))) {
      ob_rpc_req.set_rpc_type(obkv::OBPROXY_RPC_OBRPC);

      if (req_buf_len < ezhdr.ez_payload_size_ + ObProxyRpcReqAnalyzer::RPC_NET_HEADER) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("the buffer size is smaller than the packet size", K(req_buf_len), "packet size", ezhdr.ez_payload_size_ + ObProxyRpcReqAnalyzer::RPC_NET_HEADER, K(rpc_trace_id));
      } else {
        LOG_DEBUG("ObProxyRpcReqAnalyzer::handle_request_header get an rpc type", K(ezhdr), K(status), K(ret), "partition_id", ob_rpc_req.get_obkv_info().get_partition_id(), K(rpc_trace_id));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      status = RPC_ANALYZE_NEW_ERROR;
      LOG_WDIAG("ObProxyRpcReqAnalyzer::handle_request_header get an wrong rpc type", K(ezhdr), K(status), K(ret), K(rpc_trace_id));
    }
  }

  return ret;
}

int ObProxyRpcReqAnalyzer::analyze_obkv_req(ObProxyRpcReqAnalyzeCtx &ctx, ObRpcReqAnalyzeNewStatus &status, ObRpcReq &ob_rpc_req)
{
  int ret = OB_SUCCESS;
  int64_t analyze_pos = 0;
  char *req_buf = NULL;
  int64_t req_buf_len = 0;
  ObRpcOBKVInfo &obkv_info = ob_rpc_req.get_obkv_info();
  const ObRpcReqTraceId &rpc_trace_id = ob_rpc_req.get_trace_id();
  obkv::ObRpcPacketMeta meta;
  if (!ctx.is_response_) {
    req_buf = ob_rpc_req.get_request_buf();
    req_buf_len = ob_rpc_req.get_request_buf_len();
  } else {
    req_buf = ob_rpc_req.get_response_buf();
    req_buf_len = ob_rpc_req.get_response_buf_len();
  }

  if (OB_FAIL(meta.deserialize(req_buf, req_buf_len, analyze_pos))) {
    status = RPC_ANALYZE_NEW_ERROR;
    LOG_WDIAG("fail to deserialize ObRpcPacketMeta", K(ob_rpc_req), K(ret), K(status), K(rpc_trace_id));
  } else {
    if ((meta.rpc_header_.flags_ & ObRpcPacketHeader::RESP_FLAG) > 0) {
      //reset response flag before set
      obkv_info.reset_odp_resp_flag();
    }
    obkv_info.set_pcode(meta.rpc_header_.pcode_);
    obkv_info.set_meta_flag(meta.rpc_header_.flags_);
    ctx.analyze_pos_ = meta.rpc_header_.hlen_ + RPC_NET_HEADER;

    if (obkv_info.is_resp()) {
      ob_rpc_req.set_response(true);
      // obkv_info.reset_odp_resp_flag();
      if (OB_FAIL(analyze_obkv_req_response(ctx, status, ob_rpc_req, meta))) {
        LOG_WDIAG("fail to call analyze_obkv_req_response", K(ret), K(status), K(ob_rpc_req), K(meta), K(rpc_trace_id));
      } else {
        obkv_info.set_resp_completed(true);
      }
    } else {
      // only request keep tenant id
      obkv_info.tenant_id_ = meta.rpc_header_.tenant_id_;   // set tenant id
      if (OB_FAIL(analyze_obkv_req_request(ctx, status, ob_rpc_req, meta))) {
        LOG_WDIAG("fail to call analyze_obkv_req_response", K(ret), K(status), K(ob_rpc_req), K(meta), K(rpc_trace_id));
      }
    }

  }

  if (OB_SUCC(ret)) {
    status = RPC_ANALYZE_NEW_DONE;
    LOG_DEBUG("analyze_obrpc_req done", K(ob_rpc_req), "partition_id", ob_rpc_req.get_obkv_info().get_partition_id(), K(rpc_trace_id));
  }

  return ret;
}

int ObProxyRpcReqAnalyzer::analyze_obkv_req_request(ObProxyRpcReqAnalyzeCtx &ctx, ObRpcReqAnalyzeNewStatus &status, ObRpcReq &ob_rpc_req, obkv::ObRpcPacketMeta &meta)
{
  int ret = OB_SUCCESS;
  int64_t analyze_pos = ctx.analyze_pos_;
  char *req_buf = NULL;
  int64_t req_buf_len = 0;
  if (!ctx.is_response_) {
    req_buf = ob_rpc_req.get_request_buf();
    req_buf_len = ob_rpc_req.get_request_buf_len();
  } else {
    req_buf = ob_rpc_req.get_response_buf();
    req_buf_len = ob_rpc_req.get_response_buf_len();
  }
  int64_t rpc_request_size = 0;
  void *buf = NULL;
  ObRpcOBKVInfo &obkv_info = ob_rpc_req.get_obkv_info();
  ObRpcPacketCode pcode = obkv_info.pcode_;
  ObRpcRequest *rpc_request = NULL;
  const ObRpcReqTraceId &rpc_trace_id = ob_rpc_req.get_trace_id();

  if (OB_FAIL(get_rpc_request_size(pcode, rpc_request_size))) {
    status = RPC_ANALYZE_NEW_ERROR;
    LOG_WDIAG("fail to get rpc request size", K(pcode), K(rpc_request_size), K(ret), K(rpc_trace_id));
  } else {
    if (OB_FAIL(ob_rpc_req.free_rpc_request())) {
      LOG_WDIAG("fail to call free_rpc_request", K(ret), K(pcode), K(rpc_trace_id));
    } else if (OB_ISNULL(ob_rpc_req.get_rpc_request())) {
      if (OB_ISNULL(buf = op_fixed_mem_alloc(rpc_request_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("analyze_obrpc_req alloc memory failed", K(ret), K(pcode), K(rpc_request_size), K(rpc_trace_id));
      } else {
        ob_rpc_req.set_rpc_request_len(rpc_request_size);
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(buf)) {
      switch (pcode) {
      case obrpc::OB_TABLE_API_LOGIN: {
        rpc_request = new (buf) ObRpcTableLoginRequest;
        obkv_info.set_auth(true);
        break;
      }
      case obrpc::OB_TABLE_API_EXECUTE: {
        rpc_request = new (buf) ObRpcTableOperationRequest;
        break;
      }
      case obrpc::OB_TABLE_API_BATCH_EXECUTE: {
        rpc_request = new (buf) ObRpcTableBatchOperationRequest;
        obkv_info.set_batch(true);
        break;
      }
      case obrpc::OB_TABLE_API_EXECUTE_QUERY: {
        rpc_request = new (buf) ObRpcTableQueryRequest;
        break;
      }
      case obrpc::OB_TABLE_API_QUERY_AND_MUTATE: {
        rpc_request = new (buf) ObRpcTableQueryAndMutateRequest;
        break;
      }
      case obrpc::OB_TABLE_API_EXECUTE_QUERY_SYNC: {
        rpc_request = new (buf) ObRpcTableQuerySyncRequest;
        break;
      }
      case obrpc::OB_TABLE_API_DIRECT_LOAD: {
        rpc_request = new (buf) ObRpcTableDirectLoadRequest;
        break;
      }
      case obrpc::OB_TABLE_API_LS_EXECUTE: {
        rpc_request = new (buf) ObRpcTableLSOperationRequest;
        break;
      }
      default:
        status = RPC_ANALYZE_NEW_ERROR;
        rpc_request = NULL;
        LOG_WDIAG("invalid rpc pcode", K(pcode), K(ret), K(status), K(rpc_trace_id));
        break;
      }
    }

    if (OB_SUCC(ret)) {
      // set rpc meta
      rpc_request->set_packet_meta(meta);
      rpc_request->set_cluster_version(ctx.cluster_version_);
       
      if (OB_FAIL(rpc_request->analyze_request(req_buf, req_buf_len, analyze_pos))) {
        LOG_WDIAG("fail to call analyze_request", K(ret), KP(req_buf), K(req_buf_len), K(analyze_pos), K(rpc_trace_id), K(pcode));
      } else {
        // set obkv_info meta
        obkv_info.set_hbase_request(rpc_request->is_hbase_request());
        obkv_info.set_read_weak(rpc_request->is_read_weak());
        obkv_info.set_query_with_index(rpc_request->is_query_with_index());
        obkv_info.set_stream_query(rpc_request->is_stream_query());
        obkv_info.table_name_ = rpc_request->get_table_name();
        obkv_info.index_name_ = rpc_request->get_index_name();
        // TODO 不同请求后续单独加函数处理, rpc_requset中加入handle_rpc_reuqest处理login/batch/query/ls_load等请求，共性的属性获取提取到基类
        if (obrpc::OB_TABLE_API_LS_EXECUTE == pcode) {
          ObRpcTableDirectLoadRequest *direct_load_request = dynamic_cast<ObRpcTableDirectLoadRequest *>(rpc_request);
          if (OB_NOT_NULL(direct_load_request)) {
            obkv_info.set_first_direct_load_request(direct_load_request->is_begin_request());
          }
        }
        // decode credential
        int64_t pos = 0;
        const ObString &credential = rpc_request->get_credential();
        if (!credential.empty() && OB_FAIL(serialization::decode(credential.ptr(), credential.length(), pos, obkv_info.credential_))) {
          status = RPC_ANALYZE_NEW_ERROR;
          LOG_WDIAG("failed to serialize credential", K(ret), K(pos));
        } else {
          // success
          ob_rpc_req.set_rpc_request(rpc_request);
        }
      }
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(buf) && OB_ISNULL(ob_rpc_req.get_rpc_request())) {
      //free buf, if not analyze request succed
      if (OB_NOT_NULL(rpc_request)) {
        rpc_request->~ObRpcRequest();
      }
      op_fixed_mem_free(buf, rpc_request_size);
      ob_rpc_req.set_rpc_request_len(0);
    }
  }

  return ret;
}

int ObProxyRpcReqAnalyzer::analyze_obkv_req_response(ObProxyRpcReqAnalyzeCtx &ctx, ObRpcReqAnalyzeNewStatus &status, ObRpcReq &ob_rpc_req, obkv::ObRpcPacketMeta &meta)
{
  int ret = OB_SUCCESS;
  int64_t analyze_pos = ctx.analyze_pos_;
  char *req_buf = NULL;
  int64_t req_buf_len = 0;
  if (!ctx.is_response_) {
    req_buf = ob_rpc_req.get_request_buf();
    req_buf_len = ob_rpc_req.get_request_buf_len();
  } else {
    req_buf = ob_rpc_req.get_response_buf();
    req_buf_len = ob_rpc_req.get_response_buf_len();
  }
  int64_t rpc_response_size = 0;
  void *buf = NULL;
  ObRpcOBKVInfo &obkv_info = ob_rpc_req.get_obkv_info();
  ObRpcResponse *rpc_response = NULL;
  ObRpcPacketCode pcode = obkv_info.pcode_;
  ObRpcResultCode result_code;
  const ObRpcReqTraceId &rpc_trace_id = ob_rpc_req.get_trace_id();

  if (OB_FAIL(get_rpc_response_size(pcode, rpc_response_size))) {
    status = RPC_ANALYZE_NEW_ERROR;
    LOG_WDIAG("fail to get response size", K(ret), K(pcode));
  } else {
    bool is_compress_response = (meta.get_rpc_header().compressor_type_ > NONE_COMPRESSOR);
    if (is_compress_response) {
      if (pcode == OB_TABLE_API_MOVE || ctx.is_inner_request_) {
        ret = OB_ERR_UNEXPECTED;
        status = RPC_ANALYZE_NEW_ERROR;
        LOG_WDIAG("received a compressed response from server", K(pcode), "is_shard_req", ctx.is_inner_request_, K(ret));
      } else {
        status = RPC_ANALYZE_NEW_DONE;
      }
    } else {
      if (OB_FAIL(result_code.deserialize(req_buf, req_buf_len, analyze_pos))) {
        LOG_WDIAG("fail to call deserialize for result_code", K(ret), KP(req_buf), K(req_buf_len), K(analyze_pos));
        status = RPC_ANALYZE_NEW_ERROR;
      } else if (0 != result_code.rcode_ || obkv_info.is_bad_routing()) {
        // 记录错误，是否需要重传
        obkv_info.set_error_resp(true);
        obkv_info.rpc_origin_error_code_ = result_code.rcode_;
        LOG_INFO("rpc response is error", K(pcode),
                  "error_code", result_code.rcode_, "rpc_trace_id", obkv_info.rpc_trace_id_,
                  "need_reroute", obkv_info.is_bad_routing(), "error_msg", result_code.msg_,
                  "cur_serve_ip", obkv_info.server_info_.addr_);
        status = RPC_ANALYZE_NEW_DONE;
      } else {
        // reset error code
        obkv_info.rpc_origin_error_code_ = 0;
        status = RPC_ANALYZE_NEW_DONE;
      }

      /**
       * @brief
       *   1. need_parse_response_fully
       *     1.1. not error
       *     1.2. shard request
       *     1.3. async query request
       *   2. OB_TABLE_API_MOVE
       */
      if (OB_SUCC(ret) && (obkv_info.need_parse_response_fully() || (OB_TABLE_API_MOVE == pcode))) {
        if (OB_FAIL(ob_rpc_req.free_rpc_response())) {
          LOG_WDIAG("failed to free_rpc_response", K(ret));
        } else if (OB_ISNULL(buf = ob_rpc_req.alloc_rpc_response(rpc_response_size))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WDIAG("analyze_obrpc_req alloc memory failed", K(ret), K(pcode), K(rpc_response_size));
        }

        if (OB_SUCC(ret)) {
          switch (pcode) {
          case obrpc::OB_TABLE_API_LOGIN: {
            rpc_response = new (buf) ObRpcTableLoginResponse;
            break;
          }
          case obrpc::OB_TABLE_API_EXECUTE: {
            rpc_response = new (buf) ObRpcTableOperationResponse;
            break;
          }
          case obrpc::OB_TABLE_API_BATCH_EXECUTE: {
            rpc_response = new (buf) ObRpcTableBatchOperationResponse;
            break;
          }
          case obrpc::OB_TABLE_API_EXECUTE_QUERY: {
            rpc_response = new (buf) ObRpcTableQueryResponse;
            break;
          }
          case obrpc::OB_TABLE_API_QUERY_AND_MUTATE: {
            rpc_response = new (buf) ObRpcTableQueryAndMutateResponse;
            break;
          }
          case obrpc::OB_TABLE_API_EXECUTE_QUERY_SYNC: {
            rpc_response = new (buf) ObRpcTableQuerySyncResponse;
            break;
          }
          case obrpc::OB_TABLE_API_MOVE : {
            rpc_response = new (buf) ObRpcTableMoveResponse;
            obkv_info.set_resp_reroute_info(true); //handle the response
            break;
          }
          case obrpc::OB_TABLE_API_LS_EXECUTE : {
            rpc_response = new (buf) ObRpcTableLSOperationResponse;
            break;
          }
          default:
            status = RPC_ANALYZE_NEW_ERROR;
            LOG_WDIAG("invalid rpc pcode", K(pcode), K(ret), K(status), K(rpc_trace_id));
            break;
          }
        }
        if (OB_SUCC(ret)) {
          rpc_response->set_packet_meta(meta);
          rpc_response->set_result_code(result_code);
          rpc_response->set_cluster_version(ctx.cluster_version_);
          if (OB_FAIL(rpc_response->analyze_response(req_buf, req_buf_len, analyze_pos))) {
            LOG_WDIAG("fail to call analyze_response", K(ret), KP(req_buf), K(req_buf_len), K(analyze_pos), K(rpc_trace_id), K(pcode));
          } else {
            ob_rpc_req.set_rpc_response(rpc_response);
            ob_rpc_req.set_rpc_response_len(rpc_response_size);
          }
        }
        if (OB_FAIL(ret) && OB_NOT_NULL(buf) && OB_ISNULL(ob_rpc_req.get_rpc_response())) {
          //free memory if failed to deserialize
          if (OB_NOT_NULL(rpc_response)) {
            rpc_response->~ObRpcResponse();
          }
          if (ob_rpc_req.is_inner_request()) {
            if (OB_ISNULL(ob_rpc_req.get_inner_request_allocator())) {
              // not change ret
              LOG_WDIAG("inner request allocator is NULL", K(ret));
            } else {
              ob_rpc_req.get_inner_request_allocator()->free(buf);
            }
          } else {
            op_fixed_mem_free(buf, rpc_response_size);
          }
          ob_rpc_req.set_rpc_response_len(0);
        }
      }
    }
  }

  return ret;
}

int ObProxyRpcReqAnalyzer::get_parse_allocator(ObArenaAllocator *&allocator)
{
  int ret = OB_SUCCESS;
  static __thread ObArenaAllocator *arena_allocator = NULL;
  if (OB_UNLIKELY(NULL == arena_allocator)) {
    if (NULL == (arena_allocator = new (std::nothrow) ObArenaAllocator(common::ObModIds::OB_PROXY_RPC_PARSE))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc arena allocator", K(ret));
    } else {
      allocator = arena_allocator;
    }
  } else {
    allocator = arena_allocator;
  }

  return ret;
}

int ObProxyRpcReqAnalyzer::handle_obkv_login_rewrite(ObRpcReq &ob_rpc_req)
{
  int ret = OB_SUCCESS;
  obkv::ObRpcTableLoginRequest *orig_auth_req = NULL;
  ObRpcOBKVInfo &obkv_info = ob_rpc_req.get_obkv_info();
  ObRpcReqCtx *rpc_ctx = NULL;
  const ObRpcReqTraceId &rpc_trace_id = ob_rpc_req.get_trace_id();

  if (obkv_info.is_auth()
    && OB_NOT_NULL(orig_auth_req = dynamic_cast<obkv::ObRpcTableLoginRequest *>(ob_rpc_req.get_rpc_request()))
    && OB_NOT_NULL(rpc_ctx = ob_rpc_req.get_rpc_ctx())) {

    orig_auth_req->set_tenant_name(rpc_ctx->get_tenant_name());
    orig_auth_req->set_user_name(rpc_ctx->get_user_name());
    int64_t request_len = orig_auth_req->get_encode_size();
    int64_t pos = 0;
    int64_t buf_len = 0;
    char *buf = NULL;

    //Need to use an inner buffer here because the OBString in the login request uses the data in the original buffer.
    if (OB_FAIL(ob_rpc_req.alloc_request_inner_buf(request_len + ObProxyRpcReqAnalyzer::OB_RPC_ANALYZE_MORE_BUFF_LEN))) {
      LOG_WDIAG("fail to allocate rpc request req info object", K(ret), K(rpc_trace_id));
    } else {
      buf = ob_rpc_req.get_request_inner_buf();
      buf_len = ob_rpc_req.get_request_inner_buf_len();
      ob_rpc_req.set_use_request_inner_buf(true);

      if (OB_FAIL(orig_auth_req->encode(buf, buf_len, pos))) {
        LOG_WDIAG("fail to encode table login request", K(request_len), K(pos), K(buf_len), K(rpc_trace_id));
      } else {
        buf[pos++] = '\0';
        // ob_rpc_req.set_request_buf_len(request_len);
        ob_rpc_req.set_request_len(request_len);
      }
    }
  } else {
    //invalid request to rewrite
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid login request to rewrite", K(ob_rpc_req), K(ret), K(rpc_trace_id));
  }

  return ret;
}

inline bool ObProxyRpcReqAnalyzer::obkv_execute_could_rewrite(int64_t partition_id_len, int64_t table_id_len, int64_t cluster_version)
{
  bool ret = false;

  if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
    if (TABLE_ID_MAX_LEN == table_id_len && PARTITION_ID_MAX_LEN == partition_id_len) {
      ret = true;
    }
  } else {
    if (TABLE_ID_MAX_LEN == table_id_len && TABLET_ID_LEN == partition_id_len) {
      ret = true;
    }
  }

  return ret;
}

int ObProxyRpcReqAnalyzer::handle_obkv_execute_rewrite(ObRpcReq &ob_rpc_req)
{
  int ret = OB_SUCCESS;
  ObRpcOBKVInfo &obkv_info = ob_rpc_req.get_obkv_info();
  const ObRpcReqTraceId &rpc_trace_id = ob_rpc_req.get_trace_id();

  if (ob_rpc_req.canceled()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid rpc req, use default policy", K(ob_rpc_req), K(rpc_trace_id));
  } else {
    int64_t partition_id = obkv_info.partition_id_;
    int64_t table_id = obkv_info.table_id_;
    int64_t ls_id = obkv_info.ls_id_;
    ObRpcRequest *rpc_request = ob_rpc_req.get_rpc_request();
    int64_t partition_id_position = rpc_request->get_part_id_position();
    int64_t partition_id_len = rpc_request->get_part_id_len();
    int64_t table_id_position = rpc_request->get_table_id_position();
    int64_t table_id_len = rpc_request->get_table_id_len();
    int64_t ls_id_postition = rpc_request->get_ls_id_position();
    int64_t ls_id_len = rpc_request->get_ls_id_len();
    int64_t cluster_version = ob_rpc_req.get_cluster_version();
    LOG_DEBUG("ObProxyRpcReqAnalyzer:: handle_obkv_execute_rewrite to rewrite buffer", K(table_id_len),
              K(partition_id_len), K(partition_id), K(table_id), K(ls_id), K(ls_id_len), K(rpc_trace_id));
    if (!obkv_execute_could_rewrite(partition_id_len, table_id_len, cluster_version)) {
      if (OB_FAIL(handle_obkv_serialize_request(ob_rpc_req))) {
        LOG_WDIAG("fail to encode ob rpc request", K(ret), K(rpc_trace_id));
      }
    } else  if (OB_LIKELY(partition_id_position > 0 && table_id_position > 0)) {
      int64_t crc_position = RPC_NET_HEADER_LENGTH + ObRpcPacketHeader::get_checksum_position();
      int64_t checksum_begin_position = RPC_NET_HEADER_LENGTH + rpc_request->get_packet_meta().rpc_header_.hlen_;
      int64_t flags_position = RPC_NET_HEADER_LENGTH + ObRpcPacketHeader::get_flags_position();
      char *rpc_request_buffer = ob_rpc_req.get_request_buf();
      int64_t rpc_request_len = ob_rpc_req.get_request_len();
      int64_t crc_result = 0;
      uint16_t flag = rpc_request->get_packet_meta().get_rpc_header().flags_;

      LOG_DEBUG("before rewrite buffer", K(partition_id), K(table_id), K(partition_id_position), K(table_id_position),
        K(crc_position), K(checksum_begin_position), K(rpc_request_len), K(crc_result), K_(obkv_info.pcode), K(rpc_trace_id));

      if (OB_FAIL(common::serialization::encode_i16(rpc_request_buffer, rpc_request_len, flags_position, flag))) {
        LOG_WDIAG("fail to call encode_i16 for flag", K(ret), K(rpc_trace_id));
      } else {
        LOG_DEBUG("succ to covert flag to support reroute in rpc request", K(flag), K(rpc_trace_id));
      }

      if (OB_SUCC(ret)) {
        if (IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)) {
          if (OB_FAIL(common::serialization::encode_ten_bytes_i64(rpc_request_buffer, rpc_request_len, partition_id_position, partition_id))) {
            LOG_WDIAG("fail to call encode_ten_bytes_i64", K(ret), K(rpc_trace_id));
          } else if (OB_FAIL(common::serialization::encode_ten_bytes_i64(rpc_request_buffer, rpc_request_len, table_id_position, table_id))) {
            LOG_WDIAG("fail to call encode_ten_bytes_i64", K(ret), K(rpc_trace_id));
          } else {
            // success
          }
        } else {
          // for v4, partition id to tablet id
          // for lsop, rewrite ls id
          if (OB_FAIL(common::serialization::encode_i64(rpc_request_buffer, rpc_request_len, partition_id_position, partition_id))) {
            LOG_WDIAG("fail to call encode_i64", K(ret), K(rpc_trace_id));
          } else if (OB_FAIL(common::serialization::encode_ten_bytes_i64(rpc_request_buffer, rpc_request_len, table_id_position, table_id))) {
            LOG_WDIAG("fail to call encode_i64", K(ret), K(rpc_trace_id));
          } else if (obkv_info.pcode_ == obrpc::OB_TABLE_API_LS_EXECUTE &&
                     OB_FAIL(common::serialization::encode_i64(rpc_request_buffer, rpc_request_len, ls_id_postition, ls_id))) {
            LOG_WDIAG("fail to call encode_i64", K(ret), K(rpc_trace_id));
          }
        }
      }

      if (OB_SUCC(ret)) {
        // cale crc 64
        crc_result = ob_crc64(rpc_request_buffer + checksum_begin_position, rpc_request_len - checksum_begin_position);

        if (OB_FAIL(common::serialization::encode_i64(rpc_request_buffer, rpc_request_len, crc_position, crc_result))) {
          LOG_WDIAG("fail to call encode_i64", K(ret), K(rpc_trace_id));
        } else {
          LOG_DEBUG("succ to rewrite table api execute request", K(ob_rpc_req), "request_len", ob_rpc_req.get_request_len(), K(rpc_trace_id));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("invalid to rewrite obkv execute request", K(ob_rpc_req), K(table_id_position), K(partition_id_position), K(rpc_trace_id));
    }
  }
  return ret;
}

void inline shrink_copy_char_buf(char *dst, char *src, int64_t n)
{
  int64_t i = 0;
  for(i = 0; i < n; i++) {
    dst[i] = src[i];
  }
}

int ObProxyRpcReqAnalyzer::handle_obkv_serialize_request(ObRpcReq &ob_rpc_req)
{
  int ret = OB_SUCCESS;
  ObRpcRequest *request = NULL;
  char *buf = NULL;
  int64_t buf_len = 0;
  int64_t pos = 0;
  int64_t request_len = 0;
  const ObRpcReqTraceId &rpc_trace_id = ob_rpc_req.get_trace_id();

  if (OB_ISNULL(request = ob_rpc_req.get_rpc_request())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid request", K(ret), K(rpc_trace_id));
  } else {
    request_len = request->get_encode_size();
    LOG_DEBUG("ObProxyRpcReqAnalyzer::handle_obkv_serialize_request", "inner_request", ob_rpc_req.is_inner_request(), K(ob_rpc_req), K(request_len), K(rpc_trace_id));

    if (ob_rpc_req.is_inner_request() || OB_ISNULL(ob_rpc_req.get_request_buf())) { //need realloc req_buf, and set it to buf
      if (OB_FAIL(ob_rpc_req.alloc_request_buf(request_len + ObProxyRpcReqAnalyzer::OB_RPC_ANALYZE_MORE_BUFF_LEN))) {
        LOG_WDIAG("fail to allocate rpc request buf", K(ret), K(rpc_trace_id));
      } else {
        buf = ob_rpc_req.get_request_buf();
        buf_len = ob_rpc_req.get_request_buf_len();
      }
    } else {
      //need use ob_rpc_req.request_inner_buf_
      if (OB_FAIL(ob_rpc_req.alloc_request_inner_buf(request_len + ObProxyRpcReqAnalyzer::OB_RPC_ANALYZE_MORE_BUFF_LEN))) {
        LOG_WDIAG("fail to allocate rpc request inner buf", K(ret), K(rpc_trace_id));
      } else {
        buf = ob_rpc_req.get_request_inner_buf();
        buf_len = ob_rpc_req.get_request_inner_buf_len();
        ob_rpc_req.set_use_request_inner_buf(true);
      }
    }

    if (OB_SUCC(ret)) {
      request->set_cluster_version(ob_rpc_req.get_cluster_version());
      if (OB_FAIL(request->encode(buf, buf_len, pos))) {
        LOG_WDIAG("fail to encode the request to buf", K(ret), K(ob_rpc_req), K(rpc_trace_id));
      } else {
        //set pos to new_request_len
        ob_rpc_req.set_request_len(pos);
        LOG_DEBUG("succ to encode the request to buf", K(pos), K(buf), K(buf_len), K(rpc_trace_id));
      }
    }
  }

  return ret;
}

int ObProxyRpcReqAnalyzer::reset_obkv_request_before_send(ObRpcReq &ob_rpc_req)
{
  int ret = OB_SUCCESS;
  ObRpcOBKVInfo &obkv_info = ob_rpc_req.get_obkv_info();
  ObRpcRequest *request = NULL;
  const ObRpcReqTraceId &rpc_trace_id = ob_rpc_req.get_trace_id();

  if (OB_ISNULL(request = ob_rpc_req.get_rpc_request())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("ObProxyRpcReqAnalyzer::reset_obkv_request_before_send get invalid rpc_req", K(ret), K(ob_rpc_req), K(rpc_trace_id));
  } else {
    bool enable_reroute = ob_rpc_req.get_rpc_request_config_info().rpc_enable_reroute_;//get_global_proxy_config().rpc_enable_reroute;
    LOG_DEBUG("ObProxyRpcReqAnalyzer::reset_obkv_request_before_send", "pcode", request->get_packet_meta().get_pcode(),
              "table_id", obkv_info.get_table_id(), "partition_id", obkv_info.get_partition_id(), K(enable_reroute), K(rpc_trace_id));
    if (obkv_info.get_partition_id() == OB_INVALID_INDEX && obkv_info.is_definitely_single()) {
      //set single partition id to 0
      obkv_info.set_partition_id(OB_FIRST_PARTTITION_ID);
    }
    request->set_reroute_flag(enable_reroute);
    request->set_table_id(obkv_info.get_table_id());
    request->set_partition_id(obkv_info.get_partition_id());
    request->set_cluster_version(ob_rpc_req.get_cluster_version());

    if (obrpc::OB_TABLE_API_EXECUTE_QUERY_SYNC == obkv_info.pcode_) {
      LOG_DEBUG("reset obkv request for OB_TABLE_API_EXECUTE_QUERY_SYNC");
      // For Async Query，Allocate or get ObTableQueryAsyncInfo
      ObRpcTableQuerySyncRequest *sync_query_request = NULL;
      ObTableQueryAsyncEntry *query_async_entry = NULL;
      if (OB_ISNULL(sync_query_request = dynamic_cast<ObRpcTableQuerySyncRequest *>(request))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("pcode is OB_TABLE_API_EXECUTE_QUERY_SYNC, but can not cast to ObTableQuerySyncRequest", K(ret), K(request));
      } else if (OB_ISNULL(query_async_entry = obkv_info.query_async_entry_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_DEBUG("failed to get query_async_entry", KPC(query_async_entry), K(ret));
      } else {
        LOG_DEBUG("reset_obkv_request_before_send for query_async", KPC(query_async_entry));
        ObTableQuerySyncRequest &sync_query = const_cast<ObTableQuerySyncRequest &>(sync_query_request->get_query_request());
        sync_query.query_session_id_ = query_async_entry->get_server_query_session_id();
        if (query_async_entry->is_first_query()) {
          sync_query.query_type_ = ObQueryOperationType::QUERY_START;
        } else {
          sync_query.query_type_ = ObQueryOperationType::QUERY_NEXT;
        }
      }
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObProxyRpcReqAnalyzer::handle_obkv_request_rewrite(ObRpcReq &ob_rpc_req)
{
  int ret = OB_SUCCESS;
  const ObRpcReqTraceId &rpc_trace_id = ob_rpc_req.get_trace_id();

  if (OB_NOT_NULL(ob_rpc_req.get_rpc_request())) {
    ObRpcOBKVInfo &obkv_info = ob_rpc_req.get_obkv_info();
    obrpc::ObRpcPacketCode pcode = obkv_info.pcode_;
    LOG_DEBUG("ObProxyRpcReqAnalyzer::handle_obkv_request_rewrite", K(ob_rpc_req), K(obkv_info), "rpc_request",
              ob_rpc_req.get_rpc_request(), K(pcode), "is_proxy_rpc", obkv_info.is_inner_request_, K(rpc_trace_id));
    if (obkv_info.is_respo_reroute_info()) {
      //do nothing
      LOG_DEBUG("not need to rewrite request for retry caused by reroute info from server", K(pcode), K(ob_rpc_req), K(rpc_trace_id));
    // need set enable reroute flag for rpc_req_->request;
    } else if (OB_FAIL(reset_obkv_request_before_send(ob_rpc_req))) {
      // need set reroute flag for rpc_req_->request;
      LOG_WDIAG("fail to reset table_id or partition_id for rpc request", K(ret), K(ob_rpc_req), K(rpc_trace_id));
    } else {
      if (obkv_info.is_inner_request_) {
        ret = handle_obkv_serialize_request(ob_rpc_req);
      } else {
        switch(pcode) {
          case obrpc::OB_TABLE_API_LOGIN:
            ret = handle_obkv_login_rewrite(ob_rpc_req);
            break;
          case obrpc::OB_TABLE_API_EXECUTE:
          case obrpc::OB_TABLE_API_BATCH_EXECUTE:
          case obrpc::OB_TABLE_API_EXECUTE_QUERY:
          case obrpc::OB_TABLE_API_QUERY_AND_MUTATE:
          case obrpc::OB_TABLE_API_LS_EXECUTE:
            ret = handle_obkv_execute_rewrite(ob_rpc_req);
            break;
          case obrpc::OB_TABLE_API_EXECUTE_QUERY_SYNC:
            ret = handle_obkv_serialize_request(ob_rpc_req);
            break;
          case obrpc::OB_TABLE_API_DIRECT_LOAD:
            //do nothing
            break;
          default:
            LOG_DEBUG("invalid pcode to rewrite", K(pcode), K(rpc_trace_id));
            break;
        }
      }
    }
  }

  return ret;
}

int ObProxyRpcReqAnalyzer::handle_obkv_response_rewrite(ObRpcReq &ob_rpc_req)
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(ob_rpc_req.get_rpc_request())) {
    ObRpcOBKVInfo &obkv_info = ob_rpc_req.get_obkv_info();
    obrpc::ObRpcPacketCode pcode = obkv_info.pcode_;
    LOG_DEBUG("ObProxyRpcReqAnalyzer::handle_obkv_response_rewrite", K(ob_rpc_req), K(obkv_info), "rpc_response",
              ob_rpc_req.get_rpc_response(), K(pcode), "is_proxy_rpc", obkv_info.is_inner_request_);
    
    if (obrpc::OB_TABLE_API_EXECUTE_QUERY_SYNC != pcode) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("response not support to rewrite", K(pcode));
    } else {
      if (OB_FAIL(handle_obkv_serialize_response(ob_rpc_req))) {
        LOG_WDIAG("fail to call handle_obkv_serialize_response", K(ret), K(pcode));
      }
    }
  }

  return ret;
}

int ObProxyRpcReqAnalyzer::handle_obkv_serialize_response(ObRpcReq &ob_rpc_req)
{
  int ret = OB_SUCCESS;
  ObRpcResponse *response = NULL;
  ObRpcOBKVInfo &obkv_info = ob_rpc_req.get_obkv_info();
  char *buf = NULL;
  int64_t buf_len = 0;
  int64_t pos = 0;
  const ObRpcReqTraceId &rpc_trace_id = ob_rpc_req.get_trace_id();

  if (OB_ISNULL(response = ob_rpc_req.get_rpc_response())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument for handle_obkv_serialize_response", K(ret), K(ob_rpc_req), K(obkv_info), K(response), K(rpc_trace_id));
  } else {
    int64_t response_len = response->get_encode_size();
    LOG_DEBUG("ObProxyRpcReqAnalyzer::handle_obkv_serialize_response", "inner_response", ob_rpc_req.is_inner_request(), K(ob_rpc_req), K(rpc_trace_id));
    if (OB_ISNULL(ob_rpc_req.get_response_buf())) { //need realloc req_buf, and set it to buf
      if (OB_FAIL(ob_rpc_req.alloc_response_buf(response_len + ObProxyRpcReqAnalyzer::OB_RPC_ANALYZE_MORE_BUFF_LEN))) {
        LOG_WDIAG("fail to allocate response buf", K(ret));
        LOG_WDIAG("fail to allocate rpc response buf", K(ret), K(rpc_trace_id));
      } else {
        buf = ob_rpc_req.get_response_buf();
        buf_len = ob_rpc_req.get_response_buf_len();
      }
    } else {
      //need use ob_rpc_req.response_inner_buf_
      if (OB_FAIL(ob_rpc_req.alloc_response_inner_buf(response_len + ObProxyRpcReqAnalyzer::OB_RPC_ANALYZE_MORE_BUFF_LEN))) {
        LOG_WDIAG("fail to allocate response inner buf", K(ret));
      } else {
        buf = ob_rpc_req.get_response_inner_buf();
        buf_len = ob_rpc_req.get_response_inner_buf_len();
        ob_rpc_req.set_use_response_inner_buf(true);
      }
    }

    if (OB_SUCC(ret)) {
      response->set_cluster_version(ob_rpc_req.get_cluster_version());
      if (OB_FAIL(response->encode(buf, buf_len, pos))) {
        LOG_WDIAG("fail to encode the response to buf", K(ret), K(ob_rpc_req), K(rpc_trace_id));
      } else {
        //set pos to new_request_len
        ob_rpc_req.set_response_len(pos);
        LOG_DEBUG("succ to encode the response to buf", K(pos), K(buf), K(buf_len), K(rpc_trace_id));
      }
    }
  }

  return ret;
}

int ObProxyRpcReqAnalyzer::build_empty_query_response(ObRpcReq &ob_rpc_req)
{
  int ret = OB_SUCCESS;
  void *tmp_buf = NULL;
  ObRpcOBKVInfo &obkv_info = ob_rpc_req.get_obkv_info();
  ObRpcRequest *request = NULL;
  ObRpcResponse *response = NULL;
  int64_t response_len = 0;

  if (obrpc::OB_TABLE_API_EXECUTE_QUERY == obkv_info.pcode_) {
    response_len = sizeof(ObRpcTableQueryResponse);
  } else if (obrpc::OB_TABLE_API_EXECUTE_QUERY_SYNC == obkv_info.pcode_) {
    response_len = sizeof(ObRpcTableQuerySyncResponse);
  } else if (obrpc::OB_TABLE_API_QUERY_AND_MUTATE == obkv_info.pcode_) {
    response_len = sizeof(ObRpcTableQueryAndMutateResponse);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("ObProxyRpcReqAnalyzer::build_empty_query_response get a wrong pcode", K(obrpc::OB_TABLE_API_EXECUTE_QUERY_SYNC));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ob_rpc_req.free_rpc_response())) {
      LOG_WDIAG("failed to free_rpc_response", K(ret));
    } else if (OB_ISNULL(tmp_buf = ob_rpc_req.alloc_rpc_response(response_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("analyze_obrpc_req alloc memory failed", K(ret), K(response_len));
    }

    if (OB_SUCC(ret)) {
      if (obrpc::OB_TABLE_API_EXECUTE_QUERY == obkv_info.pcode_) {
        response =  new (tmp_buf) ObRpcTableQueryResponse();
      } else if (obrpc::OB_TABLE_API_EXECUTE_QUERY_SYNC == obkv_info.pcode_) {
        // Sync Query需要设置end
        ObRpcTableQuerySyncResponse *sync_response =  new (tmp_buf) ObRpcTableQuerySyncResponse();
        sync_response->get_query_result().is_end_ = true;
        response = sync_response;
      } else if (obrpc::OB_TABLE_API_QUERY_AND_MUTATE == obkv_info.pcode_) {
        response =  new (tmp_buf) ObRpcTableQueryAndMutateResponse();
      }
      ObRpcPacketMeta &meta = response->get_packet_meta();

      if (OB_ISNULL(request = ob_rpc_req.get_rpc_request())) {
        memcpy(&meta.ez_header_.magic_header_flag_, obkv::ObRpcEzHeader::MAGIC_HEADER_FLAG, sizeof(obkv::ObRpcEzHeader::MAGIC_HEADER_FLAG));
        meta.rpc_header_.pcode_ = obkv_info.pcode_;
      } else {
        memcpy(&meta.ez_header_, &(request->get_packet_meta().ez_header_), sizeof(meta.ez_header_));
        memcpy(&meta.rpc_header_, &(request->get_packet_meta().rpc_header_), sizeof(meta.rpc_header_));
        meta.rpc_header_.flags_ &= (uint16_t)~(ObRpcPacketHeader::REQUIRE_REROUTING_FLAG);  // clear reroute flag
      }

      meta.rpc_header_.flags_ |= obrpc::ObRpcPacketHeader::RESP_FLAG;
      obkv_info.set_resp_completed(true);
      ob_rpc_req.set_rpc_response(response);
      ob_rpc_req.set_rpc_response_len(response_len);
    }
  }
  return ret;
}

int ObProxyRpcReqAnalyzer::build_error_response(ObRpcReq &ob_rpc_req, int err_code)
{
  int ret = OB_SUCCESS;
  void *tmp_buf = NULL;
  ObRpcOBKVInfo &obkv_info = ob_rpc_req.get_obkv_info();
  ObRpcRequest *request = NULL;
  ObRpcResponse *response = NULL;
  int64_t response_len = sizeof(ObRpcResponse);
  const ObRpcReqTraceId &rpc_trace_id = ob_rpc_req.get_trace_id();

  if (OB_FAIL(ob_rpc_req.free_rpc_response())) {
    LOG_WDIAG("failed to free_rpc_response", K(ret), K(rpc_trace_id));
  } else if (OB_ISNULL(tmp_buf = ob_rpc_req.alloc_rpc_response(response_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("analyze_obrpc_req alloc memory failed", K(ret), K(response_len), K(rpc_trace_id));
  }

  if (OB_SUCC(ret)) {
    response =  new (tmp_buf) ObRpcResponse();
    ObRpcPacketMeta &meta = response->get_packet_meta();
    ObRpcResultCode &result_code = response->get_result_code();

    if (OB_ISNULL(request = ob_rpc_req.get_rpc_request())) {
      const ObRpcReqTraceId &trace_id = ob_rpc_req.get_trace_id();
      trace_id.get_rpc_trace_id(meta.rpc_header_.trace_id_[1], meta.rpc_header_.trace_id_[0]);
      memcpy(&meta.ez_header_.magic_header_flag_, obkv::ObRpcEzHeader::MAGIC_HEADER_FLAG, sizeof(obkv::ObRpcEzHeader::MAGIC_HEADER_FLAG));
      meta.rpc_header_.pcode_ = obkv_info.pcode_;
    } else {
      memcpy(&meta.ez_header_, &(request->get_packet_meta().ez_header_), sizeof(meta.ez_header_));
      memcpy(&meta.rpc_header_, &(request->get_packet_meta().rpc_header_), sizeof(meta.rpc_header_));
      meta.rpc_header_.flags_ &= (uint16_t)~(ObRpcPacketHeader::REQUIRE_REROUTING_FLAG);  // clear reroute flag
    }

    meta.rpc_header_.flags_ |= obrpc::ObRpcPacketHeader::RESP_FLAG;
    result_code.rcode_ = err_code;
    obkv_info.set_resp_completed(true);
    ob_rpc_req.set_rpc_response(response);
    ob_rpc_req.set_rpc_response_len(response_len);
  }
  return ret;
}

int ObProxyRpcReqAnalyzer::get_rpc_request_size(const ObRpcPacketCode pcode, int64_t &size)  
{
  int ret = OB_SUCCESS;
  switch (pcode)
  {
  case obrpc::OB_TABLE_API_LOGIN:
    size = sizeof(ObRpcTableLoginRequest);
    break;
  case obrpc::OB_TABLE_API_EXECUTE:
    size = sizeof(ObRpcTableOperationRequest);
    break;
  case obrpc::OB_TABLE_API_BATCH_EXECUTE:
    size = sizeof(ObRpcTableBatchOperationRequest);
    break;
  case obrpc::OB_TABLE_API_EXECUTE_QUERY:
    size = sizeof(ObRpcTableQueryRequest);
    break;
  case obrpc::OB_TABLE_API_QUERY_AND_MUTATE:
    size = sizeof(ObRpcTableQueryAndMutateRequest);
    break;
  case obrpc::OB_TABLE_API_EXECUTE_QUERY_SYNC:
    size = sizeof(ObRpcTableQuerySyncRequest);
    break;
  case obrpc::OB_TABLE_API_DIRECT_LOAD:
    size = sizeof(ObRpcTableDirectLoadRequest);
    break;
  case obrpc::OB_TABLE_API_LS_EXECUTE:
    size = sizeof(ObRpcTableLSOperationRequest);
    break;
  default:
    size = 0;
    ret = OB_NOT_SUPPORTED;
    LOG_WDIAG("not supported operation", K(pcode), K(ret));
    break;
  } 
  return ret;
}

int ObProxyRpcReqAnalyzer::get_rpc_response_size(const ObRpcPacketCode pcode, int64_t &size)
{
  int ret = OB_SUCCESS;
  switch (pcode)
  {
  case obrpc::OB_TABLE_API_LOGIN:
    size = sizeof(ObRpcTableLoginResponse);
    break;
  case obrpc::OB_TABLE_API_EXECUTE:
    size = sizeof(ObRpcTableOperationResponse);
    break;
  case obrpc::OB_TABLE_API_BATCH_EXECUTE:
    size = sizeof(ObRpcTableBatchOperationResponse);
    break;
  case obrpc::OB_TABLE_API_EXECUTE_QUERY:
    size = sizeof(ObRpcTableQueryResponse);
    break;
  case obrpc::OB_TABLE_API_QUERY_AND_MUTATE:
    size = sizeof(ObRpcTableQueryAndMutateResponse);
    break;
  case obrpc::OB_TABLE_API_EXECUTE_QUERY_SYNC:
    size = sizeof(ObRpcTableQuerySyncResponse);
    break;
  case obrpc::OB_TABLE_API_DIRECT_LOAD:
    size = sizeof(ObRpcTableDirectLoadRequest);
    break;
  case obrpc::OB_TABLE_API_LS_EXECUTE:
    size = sizeof(ObRpcTableLSOperationResponse);
    break;
  case obrpc::OB_TABLE_API_MOVE:
    size = sizeof(ObRpcTableMoveResponse);
    break;
  default:
    size = 0;
    ret = OB_NOT_SUPPORTED;
    LOG_WDIAG("not supported operation", K(pcode), K(ret));
    break;
  } 
  return ret;
}

int ObProxyRpcReqAnalyzer::do_parse_full_user_name(ObRpcReqCtx &rpc_ctx,
    const ObString &user_name, const char separator,
    ObProxyRpcReqAnalyzeCtx &ctx)
{
  int ret = OB_SUCCESS;
  const char *tenant_pos = NULL;
  const char *user_cluster_pos = NULL;
  const char *cluster_id_pos = NULL;
  ObString user;
  ObString tenant;
  ObString cluster;
  ObString name_id_str;
  ObString cluster_id_str;
  char tenant_str[OB_MAX_TENANT_NAME_LENGTH];
  char cluster_str[OB_PROXY_MAX_CLUSTER_NAME_LENGTH];

  if (user_name.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("no login request for auth", K(ret));
  } else {
    ObString full_user_name = user_name;
    LOG_DEBUG("full user_name", K(full_user_name), K(user_name));
    if ('\0' == separator) {
      //standard full username: user@tenant#cluster:cluster_id
      tenant_pos = full_user_name.find(FORMAL_USER_TENANT_SEPARATOR);
      user_cluster_pos = full_user_name.find(FORMAL_TENANT_CLUSTER_SEPARATOR);
      if (NULL != tenant_pos && NULL != user_cluster_pos) {
        user = full_user_name.split_on(tenant_pos);
        tenant = full_user_name.split_on(user_cluster_pos);
        cluster = full_user_name;
      } else if (NULL != tenant_pos) {
        user = full_user_name.split_on(tenant_pos);
        tenant = full_user_name;
      } else if (NULL != user_cluster_pos) {
        user = full_user_name.split_on(user_cluster_pos);
        cluster = full_user_name;
      } else {
        user = full_user_name;
      }
      if (!cluster.empty()) {
        if (NULL != (cluster_id_pos = cluster.find(CLUSTER_ID_SEPARATOR))) {
          name_id_str = cluster;
          cluster = name_id_str.split_on(cluster_id_pos);
          cluster_id_str = name_id_str;
        }
      }
    } else {
      //unstandard full user name:ClusterSeparatorTenantSeparatorUserSeparatorClusterID
      tenant_pos = full_user_name.find(separator);
      cluster = full_user_name.split_on(tenant_pos);
      user_cluster_pos = full_user_name.find(separator);
      tenant = full_user_name.split_on(separator);
      user = full_user_name;
      if (NULL != (cluster_id_pos = user.find(CLUSTER_ID_SEPARATOR))) {
        name_id_str = user;
        user = name_id_str.split_on(cluster_id_pos);
        cluster_id_str = name_id_str;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (tenant.empty() && cluster.empty()) {
      // if proxy start with specified tenant and cluster, just use them
      obutils::ObProxyConfig &proxy_config = obutils::get_global_proxy_config();
      obsys::CRLockGuard guard(proxy_config.rwlock_);
      int64_t proxy_tenant_len = strlen(proxy_config.proxy_tenant_name.str());
      int64_t proxy_cluster_len = strlen(proxy_config.rootservice_cluster_name.str());
      if (proxy_tenant_len > 0 && proxy_cluster_len > 0) {
        if (OB_UNLIKELY(proxy_tenant_len > OB_MAX_TENANT_NAME_LENGTH)
            || OB_UNLIKELY(proxy_cluster_len > OB_PROXY_MAX_CLUSTER_NAME_LENGTH)) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WDIAG("proxy_tenant or proxy_cluster is too long", K(proxy_tenant_len), K(proxy_cluster_len), K(ret));
        } else {
          memcpy(tenant_str, proxy_config.proxy_tenant_name.str(), proxy_tenant_len);
          memcpy(cluster_str, proxy_config.rootservice_cluster_name.str(), proxy_cluster_len);
          tenant.assign_ptr(tenant_str, static_cast<int32_t>(proxy_tenant_len));
          cluster.assign_ptr(cluster_str, static_cast<int32_t>(proxy_cluster_len));
        }
      }
    } else {
      if (!tenant.empty()) {
        ctx.has_tenant_username_ = true;
      }
      if (!cluster.empty()) {
        ctx.has_cluster_username_ = true;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (tenant.empty()) {
      tenant = ctx.vip_tenant_name_;
    }
    if (cluster.empty()) {
      rpc_ctx.set_clustername_from_default(true);
      cluster = ctx.vip_cluster_name_;
    }

    if (OB_FAIL(ObProxyRpcReqAnalyzer::do_parse_auth_result(rpc_ctx,
                                         FORMAL_USER_TENANT_SEPARATOR,
                                         FORMAL_TENANT_CLUSTER_SEPARATOR,
                                         CLUSTER_ID_SEPARATOR,
                                         user, tenant, cluster, cluster_id_str))) {
      LOG_WDIAG("fail to do parse auth result", K(rpc_ctx), K(ret));
    }

  }
  return ret;
}

int ObProxyRpcReqAnalyzer::do_parse_auth_result(ObRpcReqCtx &rpc_ctx,
                                      const char ut_separator,
                                      const char tc_separator,
                                      const char cluster_id_separator,
                                      const ObString &user,
                                      const ObString &tenant,
                                      const ObString &cluster,
                                      const ObString &cluster_id_str)
{
  int ret = OB_SUCCESS;
  int64_t cluster_id;
  char *buf_start = NULL;
  int64_t len = user.length() + tenant.length() + cluster.length() + 2; // separators('@','#')
  if (!cluster_id_str.empty()) {
    len = len + cluster_id_str.length() + 1; // separator ':'
  }

  if (OB_UNLIKELY(user.empty()) || OB_UNLIKELY(tenant.empty()) || OB_UNLIKELY(cluster.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid full user name", K(user), K(tenant), K(cluster), K(ret));
  } else if (rpc_ctx.get_analyze_name_buf(buf_start, len)) {
    LOG_WDIAG("fail to get_analyze_name_buf from rpc_ctx", K(user), K(tenant), K(cluster), K(ret));
  } else {
    int64_t pos = 0;
    MEMCPY(buf_start, user.ptr(), user.length());
    rpc_ctx.assign_user_name(buf_start, user.length());
    pos += user.length();
    buf_start[pos++] = ut_separator;
    MEMCPY(buf_start + pos, tenant.ptr(), tenant.length());
    rpc_ctx.assign_tenant_name(buf_start + pos, tenant.length());
    pos += tenant.length();
    buf_start[pos++] = tc_separator;
    MEMCPY(buf_start + pos, cluster.ptr(), cluster.length());
    rpc_ctx.assign_cluster_name(buf_start + pos, cluster.length());
    pos += cluster.length();
    if (!cluster_id_str.empty()) {
      if (OB_FAIL(get_int_value(cluster_id_str, cluster_id))) {
        LOG_WDIAG("fail to get int value for cluster id", K(cluster_id_str), K(ret));
      } else {
        rpc_ctx.set_cluster_id(cluster_id);
        buf_start[pos++] = cluster_id_separator;
        MEMCPY(buf_start + pos, cluster_id_str.ptr(), cluster_id_str.length());
        pos += cluster_id_str.length();
      }
    }
    rpc_ctx.assign_full_name(buf_start, static_cast<int32_t>(pos));
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
