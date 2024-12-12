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
#include "share/part/ob_part_desc.h"
#include "share/part/ob_part_desc_hash.h"
#include "share/part/ob_part_desc_key.h"
#include "share/part/ob_part_desc_range.h"
#include "share/part/ob_part_desc_list.h"
#include "proxy/mysqllib/ob_mysql_common_define.h"
#include "proxy/route/ob_table_entry.h"
#include "proxy/rpc/ob_rpc_req.h"
#include "proxy/rpc/rpclib/ob_table_query_async_entry.h"
#include "proxy/rpc/rpclib/ob_rpc_req_ctx.h"
#include "obkv/redis/ob_redis_rpc_response.h"
#include "proxy/rpc/redis/ob_rpc_redis_analyzer.h"
#include "proxy/rpc/rpclib/ob_rpc_req_ctx_processor.h"
#include "proxy/route/ob_index_entry.h"

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

int ObProxyRpcReqAnalyzer::analyze_rpc_packet_meta(ObProxyRpcReqAnalyzeCtx &ctx, ObRpcReq &ob_rpc_req)
{
  int ret = OB_SUCCESS;
  int64_t &analyze_pos = ctx.analyze_pos_;
  char *req_buf = NULL;
  int64_t req_buf_len = 0;
  ObRpcOBKVInfo &obkv_info = ob_rpc_req.get_obkv_info();
  const ObRpcReqTraceId &rpc_trace_id = ob_rpc_req.get_trace_id();
  ObRpcReqAnalyzeNewStatus &status = ctx.status_;
  obkv::ObRpcPacketMeta meta;
  status = RPC_ANALYZE_NEW_CONT;
  if (!ctx.is_response_) {
    req_buf = ob_rpc_req.get_request_buf();
    req_buf_len = ob_rpc_req.get_request_buf_len();
  } else {
    req_buf = ob_rpc_req.get_response_buf();
    req_buf_len = ob_rpc_req.get_response_buf_len();
  }

  if (OB_FAIL(meta.deserialize(req_buf, req_buf_len, analyze_pos))) {
    LOG_WDIAG("fail to deserialize ObRpcPacketMeta", K(ob_rpc_req), K(ret), K(rpc_trace_id));
  } else {
    obkv_info.set_pcode(meta.rpc_header_.pcode_);
    obkv_info.set_meta_flag(meta.rpc_header_.flags_);
    obkv_info.tenant_id_ = meta.rpc_header_.tenant_id_;   // set tenant id
    analyze_pos = meta.rpc_header_.hlen_ + RPC_NET_HEADER;

    if (OB_UNLIKELY(obkv_info.is_resp() != ctx.is_response_)) {
      ret = OB_ERR_UNEXPECTED;
      //maybe server return response without RESP flag for response
      LOG_WDIAG("invalid request or response to handle", K(ob_rpc_req), K(ret), K(rpc_trace_id),
                "request_resp_flag", obkv_info.is_resp(), "ctx_is_response", ctx.is_response_);
    } else {
      // analyze response meta
      if (obkv_info.is_resp()) {
        ObRpcResultCode result_code;
        ob_rpc_req.set_response(true);
        obkv_info.reset_odp_resp_flag();

        bool is_compress_response = (meta.get_rpc_header().compressor_type_ > NONE_COMPRESSOR);
        if (is_compress_response) {
          if (obkv_info.pcode_ == OB_TABLE_API_MOVE || ctx.is_inner_request_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("received a compressed response from server", "pcode", obkv_info.pcode_, "is_shard_req", ctx.is_inner_request_, K(ret));
          } else {
            status = RPC_ANALYZE_NEW_DONE;
          }
        } else {
          if (OB_FAIL(result_code.deserialize(req_buf, req_buf_len, analyze_pos))) {
            LOG_WDIAG("fail to call deserialize for result_code", K(ret), KP(req_buf), K(req_buf_len), K(analyze_pos));
          } else if (0 != result_code.rcode_ || obkv_info.is_bad_routing()) {
            // 记录错误，是否需要重传
            obkv_info.set_error_resp(true);
            obkv_info.rpc_origin_error_code_ = result_code.rcode_;
            LOG_INFO("rpc response is error", "pcode", obkv_info.pcode_,
                      "error_code", result_code.rcode_, "rpc_trace_id", obkv_info.rpc_trace_id_,
                      "need_reroute", obkv_info.is_bad_routing(), "error_msg", result_code.msg_,
                      "cur_serve_ip", obkv_info.server_info_.addr_);
          } else {
            // reset error code
            obkv_info.rpc_origin_error_code_ = 0;
          }

          /**
           * @brief
           *   1. need_parse_response_fully
           *     1.1. not error
           *     1.2. shard request
           *     1.3. async query request
           *   2. OB_TABLE_API_MOVE
           *   3. OB_REDIS_EXECUTE
           */
          if (OB_SUCC(ret) && (obkv_info.need_parse_response_fully() || (OB_TABLE_API_MOVE == obkv_info.pcode_) || (OB_REDIS_EXECUTE == obkv_info.pcode_ && obkv_info.rpc_origin_error_code_ == 0))) {
            // alloc response and full parse
            if (OB_FAIL(ob_rpc_req.alloc_rpc_response())) {
              LOG_WDIAG("fail to call alloc_rpc_response", K(ob_rpc_req), K(ret), K(rpc_trace_id));
            } else {
              // set rpc meta
              ObRpcResponse *rpc_response = ob_rpc_req.get_rpc_response();
              rpc_response->set_packet_meta(meta);
              rpc_response->set_result_code(result_code);
              rpc_response->set_cluster_version(ctx.cluster_version_);
            }
            status = RPC_ANALYZE_NEW_CONT;
          } else {
            status = RPC_ANALYZE_NEW_DONE;
          }
        }
      } else {
        // alloc rpc request
        if (OB_FAIL(ob_rpc_req.alloc_rpc_request())) {
          LOG_WDIAG("fail to call alloc_rpc_request", K(ob_rpc_req), K(ret), K(rpc_trace_id));
        } else {
          // set rpc meta
          ObRpcRequest *rpc_request = ob_rpc_req.get_rpc_request();
          rpc_request->set_packet_meta(meta);
          rpc_request->set_cluster_version(ctx.cluster_version_);
        }
      }
    }
  }

  return ret;
}

int ObProxyRpcReqAnalyzer::analyze_rpc_request(ObProxyRpcReqAnalyzeCtx &ctx, ObRpcReq &ob_rpc_req)
{
  int ret = OB_SUCCESS;
  int64_t &analyze_pos = ctx.analyze_pos_;
  char *req_buf = ob_rpc_req.get_request_buf();
  int64_t req_buf_len = ob_rpc_req.get_request_buf_len();
  const ObRpcReqTraceId &rpc_trace_id = ob_rpc_req.get_trace_id();
  ObRpcOBKVInfo &obkv_info = ob_rpc_req.get_obkv_info();
  ObRpcRequest *rpc_request = ob_rpc_req.get_rpc_request();
  ObRpcReqAnalyzeNewStatus &status = ctx.status_;

  if (OB_FAIL(rpc_request->analyze_request(req_buf, req_buf_len, analyze_pos))) {
    LOG_WDIAG("fail to call analyze_request", K(ret), KP(req_buf), K(req_buf_len), K(analyze_pos), K(rpc_trace_id), "pcode", obkv_info.pcode_);
  } else {
    // set obkv_info meta
    obkv_info.set_hbase_request(rpc_request->is_hbase_request());
    obkv_info.set_read_weak(rpc_request->is_read_weak());
    obkv_info.set_query_with_index(rpc_request->is_query_with_index());
    obkv_info.set_stream_query(rpc_request->is_stream_query());
    obkv_info.table_name_ = rpc_request->get_table_name();
    obkv_info.index_name_ = rpc_request->get_index_name();
    int64_t partition_id = rpc_request->get_partition_id();
    int64_t ls_id = rpc_request->get_ls_id();
    if ((partition_id != 0 && partition_id != -1) || (ls_id != ObLSID::INVALID_LS_ID)) {
      obkv_info.set_partition_id(partition_id);
      obkv_info.set_ls_id(ls_id);
      obkv_info.is_rpc_request_with_partition_id_ = true;
    }

    // decode credential
    int64_t pos = 0;
    const ObString &credential = rpc_request->get_credential();
    if (!credential.empty() && OB_FAIL(serialization::decode(credential.ptr(), credential.length(), pos, obkv_info.credential_))) {
      status = RPC_ANALYZE_NEW_ERROR;
      LOG_WDIAG("failed to serialize credential", K(ret), K(pos));
    } else {
      if (obrpc::OB_TABLE_API_LS_EXECUTE == obkv_info.pcode_) {
        ObRpcTableDirectLoadRequest *direct_load_request = dynamic_cast<ObRpcTableDirectLoadRequest *>(rpc_request);
        if (OB_NOT_NULL(direct_load_request)) {
          obkv_info.set_first_direct_load_request(direct_load_request->is_begin_request());
        }
      } else if (obrpc::OB_GET_PARTITIONS == obkv_info.pcode_) {
        obkv_info.is_internal_rpc_request_ = true;
      }
    }

    if (OB_SUCC(ret)) {
      status = RPC_ANALYZE_NEW_DONE;
    }
  }

  return ret;
}

int ObProxyRpcReqAnalyzer::analyze_rpc_response(ObProxyRpcReqAnalyzeCtx &ctx, ObRpcReq &ob_rpc_req)
{
  int ret = OB_SUCCESS;
  int64_t &analyze_pos = ctx.analyze_pos_;
  char *req_buf = ob_rpc_req.get_response_buf();
  int64_t req_buf_len = ob_rpc_req.get_response_buf_len();
  const ObRpcReqTraceId &rpc_trace_id = ob_rpc_req.get_trace_id();
  ObRpcOBKVInfo &obkv_info = ob_rpc_req.get_obkv_info();
  ObRpcResponse *rpc_response = ob_rpc_req.get_rpc_response();
  ObRpcReqAnalyzeNewStatus &status = ctx.status_;

  if (OB_FAIL(rpc_response->analyze_response(req_buf, req_buf_len, analyze_pos))) {
    LOG_WDIAG("fail to call analyze_response", K(ret), KP(req_buf), K(req_buf_len), K(analyze_pos), K(rpc_trace_id), "pcode", obkv_info.pcode_);
  } else {
    status = RPC_ANALYZE_NEW_DONE;
    obkv_info.set_resp_completed(true);
  }

  if (OB_SUCC(ret)) {
    status = RPC_ANALYZE_NEW_DONE;
  }

  return ret;
}

bool ObProxyRpcReqAnalyzer::required_async_analyze(ObProxyRpcReqAnalyzeCtx &ctx, ObRpcReq &ob_rpc_req)
{
  bool async_analyze = false;

  if (ctx.is_response_) {
    // do nothing
    // 这里由上层调用保证，传入的ob_rpc_req一定是需要全量解析的
    ObRpcResponse *rpc_response = ob_rpc_req.get_rpc_response();

    if (OB_ISNULL(rpc_response)) {
      // do nothing
    } else {
      const ObRpcPacketMeta &meta = rpc_response->get_packet_meta();
      if (meta.ez_header_.ez_payload_size_ > OB_RPC_BIG_PACKET_LEN) {
        async_analyze = true;
        LOG_DEBUG("rpc response require async analyze", K(ob_rpc_req), "payload_size", meta.ez_header_.ez_payload_size_);
      }
    }
  } else {
    // for request analyze
    ObRpcRequest *rpc_request = ob_rpc_req.get_rpc_request();

    if (OB_ISNULL(rpc_request)) {
      // do nothing
    } else {
      const ObRpcPacketMeta &meta = rpc_request->get_packet_meta();
      if (meta.ez_header_.ez_payload_size_ > OB_RPC_BIG_PACKET_LEN) {
        async_analyze = true;
        LOG_DEBUG("rpc request require async analyze", K(ob_rpc_req), "payload_size", meta.ez_header_.ez_payload_size_);
      }
    }
  }

  return async_analyze;
}

int ObProxyRpcReqAnalyzer::handle_server_failed(ObProxyRpcReqAnalyzeCtx &ctx, ObRpcReq &ob_rpc_req)
{
  int ret = OB_SUCCESS;

  ObRpcOBKVInfo &obkv_info = ob_rpc_req.get_obkv_info();
  const ObRpcReqTraceId &rpc_trace_id = ob_rpc_req.get_trace_id();

  #ifdef ERRSIM
  if (OB_SUCC(ret) && OB_FAIL(OB_E(EventTable::EN_RPC_NO_MASTER) OB_SUCCESS)) {
    ret = OB_SUCCESS;
    obkv_info.set_error_resp(true);
    obkv_info.rpc_origin_error_code_ = OB_NOT_MASTER;
  }
  #endif
  if (obkv_info.is_error() && obkv_info.is_resp()) {
    LOG_DEBUG("ObRpcRequestSM::handle_server_failed", "error_code", obkv_info.rpc_origin_error_code_, K(rpc_trace_id));

    if (obkv_info.pcode_ == obrpc::OB_TABLE_API_DIRECT_LOAD) {
      LOG_INFO("ObRpcRequestSM::handle_server_failed direct load request receive server failed", "error_code",
                obkv_info.rpc_origin_error_code_, K(rpc_trace_id));
      //don't do any retry for OB_TABLE_API_DIRECT_LOAD request
      obkv_info.set_resp_reroute_info(false);
      obkv_info.set_need_retry(false);
      obkv_info.set_need_retry_with_global_index(false);
    }
    // For the -10500 error, there are two main situations:
    //  1. Use the main table routing to report error -10500, splice it into a global index table, and try again.
    //  2. Using global index table routing, error -10500 is reported. This situation is usually caused by using the old cache. In this case, normal retry logic is used, and the main table routing is used.
    else if (OB_ERR_KV_GLOBAL_INDEX_ROUTE == obkv_info.rpc_origin_error_code_) {
      if (!ob_rpc_req.get_rpc_request_config_info().rpc_enable_global_index_) {
        ret = OB_ERR_KV_GLOBAL_INDEX_ROUTE;
        LOG_WDIAG("Currently a global index error is returned but ODP disables global indexing", "error_code", obkv_info.rpc_origin_error_code_, K(rpc_trace_id));
      } else if (!obkv_info.is_query_with_index()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WDIAG("Currently it is not an index query request but a related error is returned", "error_code", obkv_info.rpc_origin_error_code_, K(rpc_trace_id));
      } else {
        LOG_INFO("ObRpcRequestSM::handle_server_failed get global index error", "error_code", obkv_info.rpc_origin_error_code_,
                  "data_table_id", obkv_info.data_table_id_, "table_id", obkv_info.table_id_, "idx_name", obkv_info.index_name_,
                  "is_global_index_route", obkv_info.is_global_index_route(), K(rpc_trace_id));
        if (obkv_info.is_global_index_route()) {
          // dirtry index entry
          if (OB_NOT_NULL(obkv_info.index_entry_)) {
            obkv_info.index_entry_->cas_set_dirty_state();
            obkv_info.index_entry_->dec_ref();
            obkv_info.index_entry_ = NULL;
          }
          obkv_info.table_id_ = 0;
          obkv_info.data_table_id_ = 0;
          obkv_info.index_table_name_.reset();
          obkv_info.set_need_retry(true);

          ctx.dirty_partition_entry_ = true;
        } else {
          // With index table name, try again
          obkv_info.set_need_retry_with_global_index(true);
        }
      }
    // TODO: There may be many different errors in the future. The reroute flag is not set, but you need to update the routing information and try again.
    //  1. table level.  2. partition level.
    } else if (OB_SCHEMA_ERROR == obkv_info.rpc_origin_error_code_
                || OB_TABLE_NOT_EXIST == obkv_info.rpc_origin_error_code_
                || OB_TABLET_NOT_EXIST == obkv_info.rpc_origin_error_code_
                || OB_LS_NOT_EXIST == obkv_info.rpc_origin_error_code_
                || (obrpc::OB_TABLE_API_LS_EXECUTE == obkv_info.pcode_
                    && OB_NOT_MASTER == obkv_info.rpc_origin_error_code_)) {
      if (obkv_info.is_rpc_request_with_partition_id_) {
        ret = OB_ERR_KV_ROUTE_ENTRY_EXPIRE;
        LOG_INFO("ObRpcRequestSM::handle_server_failed get OB_SCHEMA_ERROR/OB_TABLE_NOT_EXIST "
                  "with set partition id, return OB_ERR_KV_ROUTE_ENTRY_EXPIRE", "error_code",
                obkv_info.rpc_origin_error_code_, K(rpc_trace_id));
      } else {
        LOG_INFO("ObRpcRequestSM::handle_server_failed get OB_SCHEMA_ERROR/OB_TABLE_NOT_EXIST", "error_code",
                obkv_info.rpc_origin_error_code_, K(rpc_trace_id));
        obkv_info.set_need_retry(true);
        obkv_info.set_route_entry_dirty();
      }

      ctx.dirty_table_entry_ = true;
    } else if (obkv_info.is_bad_routing()) {
      obkv_info.set_need_retry(true);
      obkv_info.set_route_entry_dirty();
      LOG_INFO("ObRpcRequestSM::handle_server_failed ", "error_code", obkv_info.rpc_origin_error_code_,
                "is_inner_request", obkv_info.is_inner_request_,
                "retry_count", obkv_info.rpc_request_retry_times_, K(rpc_trace_id));
      // if received not master error, it means the partition locations of
      // the certain table entry has expired, so we need delay to update it;
      ctx.dirty_partition_entry_ = true;
    } else {
      switch (obkv_info.rpc_origin_error_code_)
      {
      case OB_LOCATION_LEADER_NOT_EXIST:
      case OB_NOT_MASTER:
      case OB_RS_NOT_MASTER:
      case OB_RS_SHUTDOWN:
      case OB_RPC_SEND_ERROR:
      case OB_RPC_POST_ERROR:
      case OB_PARTITION_NOT_EXIST:
      case OB_LOCATION_NOT_EXIST:
      case OB_PARTITION_IS_STOPPED:
      case OB_PARTITION_IS_BLOCKED:
      case OB_SERVER_IS_INIT:
      case OB_SERVER_IS_STOPPING:
      // To avoid frequent changes in the tenant ID, return 5150(OB_TENANT_NOT_IN_SERVER) directly and client client recalculate tenant id.
      // case OB_TENANT_NOT_IN_SERVER:
      case OB_TRANS_RPC_TIMEOUT:
      case OB_MAPPING_BETWEEN_TABLET_AND_LS_NOT_EXIST:
        obkv_info.set_need_retry(true);
        obkv_info.set_route_entry_dirty();
        LOG_INFO("ObRpcRequestSM::handle_server_failed get route error_code", "error_code", obkv_info.rpc_origin_error_code_,
                  "is_inner_request", obkv_info.is_inner_request_,
                  "retry_count", obkv_info.rpc_request_retry_times_, K(rpc_trace_id));
        // if received not master error, it means the partition locations of
        // the certain table entry has expired, so we need delay to update it;
        ctx.dirty_partition_entry_ = true;
        break;
      case OB_TENANT_NOT_IN_SERVER: // -5150
        ctx.dirty_table_entry_ = true;
        LOG_INFO("ObRpcRequestSM::handle_server_failed get route error_code, just dirty table entry not retry",
                  "error_code", obkv_info.rpc_origin_error_code_,
                  "is_inner_request", obkv_info.is_inner_request_,
                  "retry_count", obkv_info.rpc_request_retry_times_, K(rpc_trace_id));
        break;
      default:
        //do nothing
        LOG_DEBUG("ObRpcRequestSM::handle_server_failed get route error_code, do nothing",
                  "error_code", obkv_info.rpc_origin_error_code_,
                  "is_inner_request", obkv_info.is_inner_request_,
                  "retry_count", obkv_info.rpc_request_retry_times_, K(rpc_trace_id));
        break;
      }
    }
  }

  return ret;
}

int ObProxyRpcReqAnalyzer::handle_query_async_response(ObProxyRpcReqAnalyzeCtx &ctx, ObRpcReq &ob_rpc_req)
{
  int ret = OB_SUCCESS;
  ObRpcTableQuerySyncResponse *query_response = NULL;
  ObRpcOBKVInfo &obkv_info = ob_rpc_req.get_obkv_info();
  const ObRpcReqTraceId &rpc_trace_id = ob_rpc_req.get_trace_id();

  if (OB_ISNULL(query_response = dynamic_cast<ObRpcTableQuerySyncResponse *>(ob_rpc_req.get_rpc_response()))) {
    LOG_DEBUG("direct return response to client, no need to handle response", K(rpc_trace_id));
  } else {
    LOG_DEBUG("handle_obkv_table_query_async_response for OB_TABLE_API_EXECUTE_QUERY_SYNC", K(rpc_trace_id));

    // 处理跨分区SyncQuery的标记，session id处理，标记处理
    ObTableQueryAsyncEntry *query_async_entry = NULL;
    if (OB_ISNULL(query_async_entry = obkv_info.query_async_entry_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_DEBUG("process ObRpcTableQuerySyncResponse get query_async_entry is NULL", K(ret), K(obkv_info), KPC(query_async_entry), K(rpc_trace_id));
    } else {
      LOG_DEBUG("begin to process ObRpcTableQuerySyncResponse", KPC(query_async_entry), K(rpc_trace_id));

      if (OB_SUCC(ret)) {
        /**
         * @brief
         * 1. If there is data, directly modify the flag to not end and return
         *  1.1 If it is the last partition with end, clean query_async_entry
         * 2. If there is no data
         *  2.1 If it is the last partition, set client_session_id and return, NOTICE：clean query_async_entry
         *  2.2 If it is not the last partition, you need to try again with the next partition.
         */
        bool is_data = 0 != query_response->get_query_result().get_row_count();
        bool is_end = query_response->get_query_result().is_end_;
        bool need_clean_query_info = false;

        // reset is_first
        query_async_entry->set_first_query(false);

        if (query_async_entry->is_single_query_request()) {
          // single
          if (is_end) {
            need_clean_query_info = true;   // clean query_async_entry
          } else {
            LOG_DEBUG("single async query result", "current server session id", query_async_entry->get_server_query_session_id(),
                        "response server session id", query_response->get_query_result().query_session_id_, K(rpc_trace_id));
            query_async_entry->set_server_query_session_id(query_response->get_query_result().query_session_id_);
          }
        } else {
          // sharding
          if (is_data) {
            if (is_end) {
              if (query_async_entry->is_last_tablet()) {
                need_clean_query_info = true;   // clean query_async_entry
              } else {
                query_response->get_query_result().is_end_ = false;  // set not end
                query_async_entry->add_current_position();
                query_async_entry->set_server_query_session_id(0);
                query_async_entry->set_first_query(true);
                query_async_entry->reset_server_info();
                LOG_DEBUG("handle async response with", K(is_data), K(is_end), K(rpc_trace_id));
              }
            } else {
              LOG_DEBUG("shard async query result", "current server session id", query_async_entry->get_server_query_session_id(),
                        "response server session id", query_response->get_query_result().query_session_id_, K(rpc_trace_id));
              query_async_entry->set_server_query_session_id(query_response->get_query_result().query_session_id_);
            }
          } else {
            if (!is_end) {
              // This situation does not exist. The default is is_end to prevent the server from returning an exception and causing an obproxy exception.
              LOG_WDIAG("Async query get no data but response flag is not end", K(is_end), K(is_data), K(query_response), K(rpc_trace_id));
              is_end = true;
            }
            if (query_async_entry->is_last_tablet()) {
              need_clean_query_info = true;
            } else {
              query_async_entry->add_current_position();
              query_async_entry->set_server_query_session_id(0);
              query_async_entry->set_first_query(true);
              query_async_entry->reset_server_info();
              query_async_entry->set_need_retry(true);
              ctx.need_retry_ = true;
            }
          }
        }

        query_response->get_query_result().query_session_id_ = query_async_entry->get_client_query_session_id();
        ctx.need_rewrite_ = true;

        if (need_clean_query_info) {
          query_async_entry->set_need_terminal(true);
          query_async_entry->set_deleted_state();
        }
        LOG_DEBUG("process ObRpcTableQuerySyncResponse done", KPC(query_async_entry), K(is_data), K(is_end), K(need_clean_query_info), K(ctx), K(rpc_trace_id));
      }
    }
  }

  return ret;
}

int ObProxyRpcReqAnalyzer::handle_login_response(ObProxyRpcReqAnalyzeCtx &ctx, ObRpcReq &ob_rpc_req)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);

  ObRpcReqCtx *rpc_ctx = NULL;
  ObRpcOBKVInfo &obkv_info = ob_rpc_req.get_obkv_info();
  const ObRpcReqTraceId &rpc_trace_id = ob_rpc_req.get_trace_id();
  ObRpcTableLoginResponse *login_response = NULL;

  if (OB_ISNULL(rpc_ctx = obkv_info.rpc_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("handle login result but rpc_ctx is NULL", K(ret), K(rpc_trace_id));
  } else if (OB_ISNULL(login_response = dynamic_cast<ObRpcTableLoginResponse *>(ob_rpc_req.get_rpc_response()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("handle login result but login_response is NULL", K(ret), K(rpc_trace_id));
  } else {
    // decode and store credential
    int64_t pos = 0;

    const ObString &credential = login_response->get_credential();
    if (OB_FAIL(serialization::decode(credential.ptr(), credential.length(), pos, obkv_info.credential_))) {
      LOG_WDIAG("failed to serialize credential", K(ret), K(pos));
    } else {
      // set credential
      rpc_ctx->set_credential(obkv_info.credential_);
      // add in global cache
      rpc_ctx->inc_ref();   //inc before add to cache
      if (OB_FAIL(get_global_rpc_req_ctx_cache().add_rpc_req_ctx_if_not_exist(*rpc_ctx, false))) {
        LOG_WDIAG("fail to add rpc ctx", KPC(rpc_ctx), K(ret));
        rpc_ctx->dec_ref();
      } else {
        LOG_DEBUG("succ to add rpc ctx into global cache", KPC(rpc_ctx), K(rpc_trace_id));
        //set credential value for redis
        if (ob_rpc_req.get_rpc_type() == OBPROXY_RPC_REDIS) {
          ObRpcRedisInfo *redis_info = ob_rpc_req.get_redis_info();
          if (OB_NOT_NULL(redis_info)) {
            redis_info->set_rpc_credential(credential);
          }
        }
      }
    }
  }

  return ret;
}

int ObProxyRpcReqAnalyzer::handle_rpc_response(ObProxyRpcReqAnalyzeCtx &ctx, ObRpcReq &ob_rpc_req)
{
  int ret = OB_SUCCESS;

  ObRpcOBKVInfo &obkv_info = ob_rpc_req.get_obkv_info();
  const ObRpcReqTraceId &rpc_trace_id = ob_rpc_req.get_trace_id();

  if (obkv_info.is_respo_reroute_info()) {
    //need reroute base on server reroute info
    ctx.need_reroute_ = true;
  } else if (OB_FAIL(ObProxyRpcReqAnalyzer::handle_server_failed(ctx, ob_rpc_req))) {
    LOG_WDIAG("fail to call handle_server_failed", K(ret), K(rpc_trace_id));
  } else if (!obkv_info.is_rpc_req_can_retry()) {
    // if cannot retry, handle special rpc response for additional information
    if (ob_rpc_req.is_inner_request()) {
      // do nothing, inner request just callback to operation
    } else {
      if (obrpc::OB_TABLE_API_EXECUTE_QUERY_SYNC == obkv_info.pcode_) {
        LOG_DEBUG("handle_obkv_response for OB_TABLE_API_EXECUTE_QUERY_SYNC", K(rpc_trace_id));
        if (OB_FAIL(ObProxyRpcReqAnalyzer::handle_query_async_response(ctx, ob_rpc_req))) {
          LOG_WDIAG("fail to call handle_query_async_response", K(ret), K(rpc_trace_id));
        }
      } else if (obrpc::OB_TABLE_API_LOGIN == obkv_info.pcode_) {
        LOG_DEBUG("handle_obkv_response for OB_TABLE_API_LOGIN", K(rpc_trace_id));
        if (OB_FAIL(ObProxyRpcReqAnalyzer::handle_login_response(ctx, ob_rpc_req))) {
          LOG_WDIAG("fail to call handle_login_response", K(ret), K(rpc_trace_id));
        }  else if (ob_rpc_req.get_rpc_type() == OBPROXY_RPC_REDIS) { //TODO
          LOG_DEBUG("handle_obkv_response for OBPROXY_RPC_REDIS", K(rpc_trace_id));
          //need build ok packet for auth request
          if (OB_FAIL(ObRpcRedisAnalyzer::build_ok_resp(ob_rpc_req))) {
            LOG_WDIAG("fail to build ok response for auth login request", K(ret), K(rpc_trace_id));
          } else if (OB_FAIL(ObRpcRedisAnalyzer::handle_redis_serialize_response(ob_rpc_req))) {
            LOG_WDIAG("invalid to serialize inner error response", K(ret), K(rpc_trace_id));
          } else {
          //save credential info
          }
        }
      } else if (obrpc::OB_TABLE_API_DIRECT_LOAD == obkv_info.pcode_) {
        ctx.need_retry_ = false;
        obkv_info.set_need_retry(false); //not do any retry for direct_load request(it will be errored if retry)
      } else if (obrpc::OB_REDIS_EXECUTE == obkv_info.pcode_) {
        if (OB_UNLIKELY(obkv_info.rpc_origin_error_code_ != 0)) {
          ObString err_content;
          bool is_error_from_server = true;
          if (OB_FAIL(ObRpcRedisAnalyzer::build_err_msg(ob_rpc_req, err_content, is_error_from_server))) {
            LOG_WDIAG("failed to fmt error msg from server", K(ret), K(ob_rpc_req));
          } else if (OB_FAIL(ObRpcRedisAnalyzer::build_err_resp(ob_rpc_req, err_content))) {
            LOG_WDIAG("failed to build error pkt for redis response", K(ret), K(ob_rpc_req));
          } else if(OB_FAIL(ObRpcRedisAnalyzer::handle_redis_serialize_response(ob_rpc_req))) {
            LOG_WDIAG("invalid to serialize redis server error response", K(ret), K(ob_rpc_req));
          }
          LOG_DEBUG("get an error response from server, maybe need retry or directly to return error",
                    "error_code", obkv_info.rpc_origin_error_code_, K(rpc_trace_id), "can_retry", obkv_info.is_rpc_req_can_retry());
        } else {
          //it is redis request has handled
          ObRpcRedisInfo *redis_info = ob_rpc_req.get_redis_info();
          int64_t response_len = 0;
          char *real_response = NULL;
          if (OB_ISNULL(redis_info)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("invalid redis request to handle", K(ret), K(redis_info), K(rpc_trace_id));
          } else if (OB_FAIL(ObRpcRedisAnalyzer::get_real_redis_response(ob_rpc_req, real_response, response_len)) || OB_ISNULL(real_response)) {
            LOG_WDIAG("fail to get real response from server", K(ret), K(redis_info), K(rpc_trace_id));
          } else {
            redis_info->set_response_server_ptr(real_response);
            redis_info->set_response_len(response_len);
            // LOG_DEBUG("redis to encode get real response", K(real_response), K(response_len));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (ctx.need_retry_ || obkv_info.is_rpc_req_can_retry()) {
      // need retry in handle_server_failed or index or async query
      ctx.need_retry_ = true;
      LOG_DEBUG("[ObProxyRpcReqAnalyzer::handle_rpc_response] need retry rpc req",
          "error_code", obkv_info.rpc_origin_error_code_,
          "global index retry", obkv_info.is_need_retry_with_global_index(),
          "async retry", obkv_info.is_need_retry_with_query_async(),
          "retry_times", obkv_info.rpc_request_retry_times_, K(rpc_trace_id));
    } else if (ctx.need_rewrite_) {
      // rewrite response and return to client
      if (OBPROXY_RPC_REDIS == ob_rpc_req.get_rpc_type()) { //TODO rpc_type_ and ctx.need_rewrite_
        if (OB_FAIL(ObRpcRedisAnalyzer::handle_redis_response_rewrite(ob_rpc_req))) {
          LOG_WDIAG("fail to call handle_obkv_response_rewrite", K(ret), K(rpc_trace_id));
        }
      } else {
       if (OB_FAIL(ObProxyRpcReqAnalyzer::handle_obkv_response_rewrite(ob_rpc_req))) {
          LOG_WDIAG("fail to call handle_obkv_response_rewrite", K(ret), K(rpc_trace_id));
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
    // need set enable reroute flag for ob_rpc_req.request;
    } else if (OB_FAIL(reset_obkv_request_before_send(ob_rpc_req))) {
      // need set reroute flag for ob_rpc_req.request;
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
  ObRpcOBKVInfo &obkv_info = ob_rpc_req.get_obkv_info();

  if (obrpc::OB_TABLE_API_EXECUTE_QUERY != obkv_info.pcode_
    && obrpc::OB_TABLE_API_EXECUTE_QUERY_SYNC != obkv_info.pcode_
    && obrpc::OB_TABLE_API_QUERY_AND_MUTATE != obkv_info.pcode_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("ObProxyRpcReqAnalyzer::build_empty_query_response get a wrong pcode", K(obrpc::OB_TABLE_API_EXECUTE_QUERY_SYNC));
  } else {
    if (OB_FAIL(ob_rpc_req.alloc_rpc_response())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to call  alloc_rpc_response", K(ret));
    } else {
      ObRpcRequest *request = NULL;
      ObRpcResponse *response = ob_rpc_req.get_rpc_response();
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
    }
  }
  return ret;
}

int ObProxyRpcReqAnalyzer::build_error_response(ObRpcReq &ob_rpc_req, int err_code)
{
  int ret = OB_SUCCESS;
  ObRpcOBKVInfo &obkv_info = ob_rpc_req.get_obkv_info();
  const ObRpcReqTraceId &rpc_trace_id = ob_rpc_req.get_trace_id();

  if (OB_FAIL(ob_rpc_req.alloc_rpc_response())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("alloc memory failed", K(ret), K(rpc_trace_id));
  } else {
    ObRpcRequest *request = NULL;
    ObRpcResponse *response = ob_rpc_req.get_rpc_response();
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
  case obrpc::OB_GET_PARTITIONS:
    size = sizeof(ObRpcTableGetRouteRequest);
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
  case obrpc::OB_REDIS_EXECUTE:
    size = sizeof(ObRpcRedisOperationResponse); //todo need replace it to  Redis response
    break;
  case obrpc::OB_GET_PARTITIONS:
    size = sizeof(ObRpcTableGetRouteResponse);
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

int ObProxyRpcReqAnalyzer::build_get_partition_response(ObRpcReq &ob_rpc_req, ObTableEntry &table_entry)
{
  int ret = OB_SUCCESS;
  ObRpcOBKVInfo &obkv_info = ob_rpc_req.get_obkv_info();
  ObRpcRequest *request = NULL;
  ObRpcTableGetRouteResponse *response = NULL;
  int64_t response_len = sizeof(ObRpcTableGetRouteResponse);

  if (OB_FAIL(ob_rpc_req.free_rpc_response())) {
    LOG_WDIAG("failed to free_rpc_response", K(ret));
  } else if (OB_FAIL(ob_rpc_req.alloc_rpc_response()) || OB_ISNULL(response = dynamic_cast<ObRpcTableGetRouteResponse *>(ob_rpc_req.get_rpc_response()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("analyze_obrpc_req alloc memory failed", K(ret), K(response_len));
  } else {
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

    // build 完毕 开始解析
    // table level
    ObObkvRouteResult &ObkvRouteResult = response->get_route_result();
    ObkvRouteResult.create_time_us_ = table_entry.get_create_time_us();
    ObkvRouteResult.table_id_ = table_entry.get_table_id();
    ObkvRouteResult.part_num_ = table_entry.get_part_num();
    // partinfo
    if (table_entry.is_partition_table()) {
      ObProxyPartInfo &part_info = *table_entry.get_part_info();
      ObObkvPartitionInfo &result_part_info = ObkvRouteResult.part_info_;
      result_part_info.part_level_ = static_cast<int64_t>(part_info.get_part_level());

      ObProxyPartOption &first_part_option = part_info.get_first_part_option();
      result_part_info.part_num_ = first_part_option.part_num_;
      result_part_info.part_space_ = first_part_option.part_space_;
      result_part_info.part_type_ = static_cast<int64_t>(first_part_option.part_func_type_);
      result_part_info.part_expr_ = part_info.get_part_expr();
      result_part_info.part_range_type_ = part_info.get_part_range_type();
      if (result_part_info.part_level_ == share::schema::ObPartitionLevel::PARTITION_LEVEL_TWO) {
        ObProxyPartOption &sub_part_option = part_info.get_sub_part_option();
        result_part_info.sub_part_num_ = sub_part_option.part_num_;
        result_part_info.sub_part_space_ = sub_part_option.part_space_;
        result_part_info.sub_part_type_ = static_cast<int64_t>(sub_part_option.part_func_type_);
        result_part_info.sub_part_expr_ = part_info.get_sub_part_expr();
        result_part_info.sub_part_range_type_ = part_info.get_sub_part_range_type();
      }
      // part key
      ObProxyPartKeyInfo &part_key_info = part_info.get_part_key_info();
      for (int i = 0; i < part_key_info.key_num_; ++i) {
        ObProxyPartKey &part_key = part_key_info.part_keys_[i];
        ObObkvPartKey result_part_key;
        if (0 != part_key.func_type_) {
          // 生成列、do nothing
        } else {
          result_part_key.part_key_cs_type_ = part_key.cs_type_;
          result_part_key.part_key_idx_ = part_key.idx_;
          result_part_key.part_key_level_ = static_cast<int64_t>(part_key.level_);
          result_part_key.part_key_name_.assign(part_key.name_.str_, part_key.name_.str_len_);
          result_part_key.part_key_type_ = static_cast<int64_t>(part_key.obj_type_);
          result_part_key.part_key_extra_.assign(part_key.part_key_extra_.str_, part_key.part_key_extra_.str_len_);
          result_part_info.part_keys_.push_back(result_part_key);
        }
      }
      // first part
      const common::ObPartDesc *first_part_desc = part_info.get_part_mgr().get_first_part_desc();
      if (OB_ISNULL(first_part_desc) || OB_FAIL(first_part_desc->build_obkv_part_array(ObkvRouteResult.first_parts_))) {
        LOG_WDIAG("fail to build obkv first part array", KP(first_part_desc), K(ret));
      } else {
        // sub part
        if (result_part_info.part_level_ == share::schema::ObPartitionLevel::PARTITION_LEVEL_TWO) {
          int64_t cluster_version = part_info.get_cluster_version();
          common::ObPartDesc *current_sub_desc = NULL;
          for (int i = 0; OB_SUCC(ret) && i < ObkvRouteResult.first_parts_.count(); ++i) {
            int64_t first_part_id = ObkvRouteResult.first_parts_[i].part_id_;
            int64_t sub_part_num;
            if (OB_FAIL(part_info.get_part_mgr().get_sub_part_num_by_first_part_id(part_info,
                                                                                   first_part_id,
                                                                                   sub_part_num))) {
              LOG_WDIAG("fail to get sub part num", K(ret));
            } else if (OB_FAIL(part_info.get_part_mgr().get_sub_part_desc_by_first_part_id(false,
                                                                                    first_part_id,
                                                                                    current_sub_desc,
                                                                                    cluster_version))) {
              LOG_WDIAG("fail to get sub part desc", K(ret));
            } else if (OB_FAIL(current_sub_desc->build_obkv_part_array(ObkvRouteResult.sub_parts_))) {
              LOG_WDIAG("fail to build obkv sub part array", K(ret));
            } else {
              ObkvRouteResult.first_parts_.at(i).sub_part_num_ = sub_part_num;
            }
          }
        }
      }
    }

    LOG_DEBUG("build_get_partition_response done", K(ObkvRouteResult));
  }

  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
