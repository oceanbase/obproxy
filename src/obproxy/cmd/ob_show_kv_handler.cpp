/*
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

#define USING_LOG_PREFIX PROXY_ICMD

#include "cmd/ob_show_kv_handler.h"
#include "iocore/eventsystem/ob_continuation.h"
#include "stat/ob_rpc_req_stats.h"
#include "iocore/net/ob_net_def.h"
#include "iocore/eventsystem/ob_kv_task.h"


using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
// RPC_THREAD_COLUMN
enum
{
  OB_KV_THREAD_ID = 0,
  OB_KV_THREAD_ETYPE,
  OB_KV_CURRENTLY_RPC_REQ,
  OB_KV_CURRENTLY_SUB_RPC_REQ,
  OB_KV_REQ_QPS,
  OB_KV_SINGLE_REQ_QPS,
  OB_KV_SHARD_REQ_QPS,
  OB_KV_MAX_COLUMN_ID
};

const ObProxyColumnSchema OB_KV_THREAD_COLUMN_ARR[OB_KV_MAX_COLUMN_ID] = {
  ObProxyColumnSchema::make_schema(OB_KV_THREAD_ID, "ThreadID", obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_KV_THREAD_ETYPE, "ThreadType", obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_KV_CURRENTLY_RPC_REQ, "ClientHandlingReqCount", obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_KV_CURRENTLY_SUB_RPC_REQ, "ThreadHandlingSubReqCount", obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_KV_REQ_QPS, "ClientReqQps", obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_KV_SINGLE_REQ_QPS, "SingleReqQps", obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_KV_SHARD_REQ_QPS, "ShardReqQps", obmysql::OB_MYSQL_TYPE_LONGLONG),
};

// RPC_REQUESTSTAT_COLUMN
enum
{
  OB_RPC_THREAD_ID = 0,
  OB_RPC_THREAD_ETYPE,
  OB_RPC_ANALYZE_REQUEST,
  OB_RPC_BUILDING_CLUSTER,
  OB_RPC_GET_GLOBAL_INDEX,
  OB_RPC_GET_PARTITION_ID,
  OB_RPC_GET_LS_ID,
  OB_RPC_IN_SHARDING,
  OB_RPC_SENDING_TO_SERVER,
  OB_RPC_ANALYZE_RESPONSE,
  OB_RPC_SENDING_TO_CLIENT,
  OB_RPC_MAX_REQUESTSTAT_COLUMN_ID
};

const ObProxyColumnSchema OB_KV_REQUESTSTAT_COLUMN_ARR[OB_RPC_MAX_REQUESTSTAT_COLUMN_ID] = {
  ObProxyColumnSchema::make_schema(OB_RPC_THREAD_ID, "ThreadID", obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_RPC_THREAD_ETYPE, "ThreadType", obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_RPC_ANALYZE_REQUEST, "AnalyzeRequestCount", obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_RPC_BUILDING_CLUSTER, "BuildClusterCount", obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_RPC_GET_GLOBAL_INDEX, "GetGlobalindexCount", obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_RPC_GET_PARTITION_ID, "GetPartitionIDCount", obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_RPC_GET_LS_ID, "GetLSIDCount", obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_RPC_IN_SHARDING, "InShardingCount", obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_RPC_SENDING_TO_SERVER, "SendToServerCount", obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_RPC_ANALYZE_RESPONSE, "AnalyzeResponseCount", obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_RPC_SENDING_TO_CLIENT, "SendToClientCount", obmysql::OB_MYSQL_TYPE_LONGLONG),
};

ObShowKvHandler::ObShowKvHandler(ObContinuation *cont, event::ObMIOBuffer *buf, const ObInternalCmdInfo &info)
  : oceanbase::obproxy::ObInternalCmdHandler(cont, buf, info), sub_type_(info.get_sub_cmd_type()),
  start_worker_thread_id_(-1), start_async_thread_id_(-1), cur_async_thread_id_(-1), is_worker_thread_finished_(false),
  is_async_thread_finished_(false)
{
  if (!info.get_rpc_req_type_string().empty()) {
    int32_t min_len =std::min(info.get_rpc_req_type_string().length(), OB_PROXY_MAX_RPC_REQ_TYPE_LENGTH);
    MEMCPY(rpc_req_type_str_, info.get_rpc_req_type_string().ptr(), min_len);
    rpc_req_type_str_[min_len] = '\0';
  } else {
    rpc_req_type_str_[0] = '\0';
  }
  SET_HANDLER(&ObShowKvHandler::handle_kv_thread_cmd);
}

int ObShowKvHandler::get_rpc_req_type(int64_t &type)
{
  int ret = OB_SUCCESS;
  if (0 == strcasecmp(rpc_req_type_str_ , "OB_TABLE_API_LOGIN")) {
    type = ObRpcReqType::OB_RPC_LOGIN;
  } else if (0 == strcasecmp(rpc_req_type_str_, "OB_TABLE_API_EXECUTE")) {
    type = ObRpcReqType::OB_RPC_EXECUTE;
  } else if (0 == strcasecmp(rpc_req_type_str_, "OB_TABLE_API_BATCH_EXECUTE")) {
    type = ObRpcReqType::OB_RPC_BATCH_EXECUTE;
  } else if (0 == strcasecmp(rpc_req_type_str_, "OB_TABLE_API_EXECUTE_QUERY")) {
    type = ObRpcReqType::OB_RPC_EXECUTE_QUERY;
  } else if (0 == strcasecmp(rpc_req_type_str_, "OB_TABLE_API_QUERY_AND_MUTATE")) {
    type = ObRpcReqType::OB_RPC_QUERY_AND_MUTATE;
  } else if (0 == strcasecmp(rpc_req_type_str_, "OB_TABLE_API_EXECUTE_QUERY_SYNC")) {
    type = ObRpcReqType::OB_RPC_EXECUTE_QUERY_SYNC;
  } else if (0 == strcasecmp(rpc_req_type_str_, "OB_TABLE_API_DIRECT_LOAD")) {
    type = ObRpcReqType::OB_RPC_DIRECT_LOAD;
  } else if (0 == strcasecmp(rpc_req_type_str_, "OB_TABLE_API_LS_EXECUTE")) {
    type = ObRpcReqType::OB_RPC_LS_EXECUTE;
  } else {
    type = ObRpcReqType::OB_MAX_TABLE_API_TYPE;
  }
  return ret;
}

int ObShowKvHandler::dump_header()
{
  int ret = OB_SUCCESS;
  if (header_encoded_) {
    DEBUG_ICMD("header is already encoded, skpi this");
  } else {
    switch(sub_type_) {
      case OBPROXY_T_SUB_KV_THREAD: {
        if (OB_FAIL(encode_header(OB_KV_THREAD_COLUMN_ARR, OB_KV_MAX_COLUMN_ID))) {
          WDIAG_ICMD("fail to encoder header", K(ret), K(sub_type_));
        }
        break;
      }
      case OBPROXY_T_SUB_KV_REQUESTSTAT: {
        if (OB_FAIL(encode_header(OB_KV_REQUESTSTAT_COLUMN_ARR, OB_RPC_MAX_REQUESTSTAT_COLUMN_ID))) {
          WDIAG_ICMD("fail to encoder header", K(ret), K(sub_type_));
        }
        break;
      }
      default: {
        if (OB_FAIL(encode_header(OB_KV_THREAD_COLUMN_ARR, OB_KV_MAX_COLUMN_ID))) {
          WDIAG_ICMD("fail to encoder header", K(ret), K(sub_type_));
        }
      }
    }
    if (OB_SUCC(ret)) {
      header_encoded_ = true;
    }
  }
  return ret;
}

int ObShowKvHandler::dump_kv_thread_one_thread(ObEThread &thread)
{
  int ret = OB_SUCCESS;
  DEBUG_ICMD("show kv thread in thread", K(&thread), K(thread.event_types_));
  int64_t thread_id = thread.id_;
  ObEventThreadType thread_etype = 0;
  int64_t current_req = 0;
  int64_t current_sub_req = 0;
  RPC_REQ_THREAD_READ_DYN_STAT(&thread, CURRENTLY_HANDLING_RPC_REQ, current_req);
  RPC_REQ_THREAD_READ_DYN_STAT(&thread, CURRENTLY_HANDLING_SHARD_RPC_REQ, current_sub_req);
  if (OB_FAIL(thread.get_origin_etype(thread_etype))) {
    WDIAG_ICMD("fail to get origin etype", K(thread_etype));
  } else {
    int64_t rpc_req_qps = ObRpcReqThreadQpsStat::get_last_sec_rpc_req();
    int64_t single_rpc_req_qps = ObRpcReqThreadQpsStat::get_last_sec_single_rpc_req();
    int64_t sub_rpc_req_qps = ObRpcReqThreadQpsStat::get_last_sec_shard_rpc_req();
    ObNewRow row;
    ObObj cells[OB_KV_MAX_COLUMN_ID];
    cells[OB_KV_THREAD_ID].set_int(thread_id);
    cells[OB_KV_THREAD_ETYPE].set_int(thread_etype);
    cells[OB_KV_CURRENTLY_RPC_REQ].set_int(current_req);
    cells[OB_KV_CURRENTLY_SUB_RPC_REQ].set_int(current_sub_req);
    cells[OB_KV_REQ_QPS].set_int(rpc_req_qps);
    cells[OB_KV_SINGLE_REQ_QPS].set_int(single_rpc_req_qps);
    cells[OB_KV_SHARD_REQ_QPS].set_int(sub_rpc_req_qps);
    row.cells_ = cells;
    row.count_ = OB_KV_MAX_COLUMN_ID;
    if (OB_FAIL(encode_row_packet(row))) {
      WDIAG_ICMD("fail to encode row packet", K(ret));
    }
  }
  return ret;
}

int ObShowKvHandler::dump_kv_requeststat_one_thread(ObEThread &thread)
{
  int ret = OB_SUCCESS;
  DEBUG_ICMD("show kv requeststat in thread", K(&thread), K(thread.event_types_));
  int64_t thread_id = thread.id_;
  int64_t type = ObRpcReqType::OB_MAX_TABLE_API_TYPE;
  ObEventThreadType thread_etype = 0;

  if (OB_FAIL(thread.get_origin_etype(thread_etype))) {
    WDIAG_ICMD("fail to get origin etype", K(thread_etype));
  } else if (OB_FAIL(get_rpc_req_type(type))) {
    WDIAG_ICMD("fail to get rpc_req type", K(type));
  } else {
    ObNewRow row;
    ObObj cells[OB_RPC_MAX_REQUESTSTAT_COLUMN_ID];
    cells[OB_RPC_THREAD_ID].set_int(thread_id);
    cells[OB_RPC_THREAD_ETYPE].set_int(thread_etype);
    cells[OB_RPC_ANALYZE_REQUEST].set_int(ObRpcReqThreadStat::get_current_rpc_req_in_analyze_request(static_cast<ObRpcReqType>(type)));
    cells[OB_RPC_BUILDING_CLUSTER].set_int(ObRpcReqThreadStat::get_current_rpc_req_in_building_cluster(static_cast<ObRpcReqType>(type)));
    cells[OB_RPC_GET_GLOBAL_INDEX].set_int(ObRpcReqThreadStat::get_current_rpc_req_in_get_globalindex(static_cast<ObRpcReqType>(type)));
    cells[OB_RPC_GET_PARTITION_ID].set_int(ObRpcReqThreadStat::get_current_rpc_req_in_get_partition_id(static_cast<ObRpcReqType>(type)));
    cells[OB_RPC_GET_LS_ID].set_int(ObRpcReqThreadStat::get_current_rpc_req_in_get_ls_id(static_cast<ObRpcReqType>(type)));
    cells[OB_RPC_IN_SHARDING].set_int(ObRpcReqThreadStat::get_current_rpc_req_in_sharding(static_cast<ObRpcReqType>(type)));
    cells[OB_RPC_SENDING_TO_SERVER].set_int(ObRpcReqThreadStat::get_current_rpc_req_in_send_server(static_cast<ObRpcReqType>(type)));
    cells[OB_RPC_ANALYZE_RESPONSE].set_int(ObRpcReqThreadStat::get_current_rpc_req_in_analyze_response(static_cast<ObRpcReqType>(type)));
    cells[OB_RPC_SENDING_TO_CLIENT].set_int(ObRpcReqThreadStat::get_current_rpc_req_in_send_client(static_cast<ObRpcReqType>(type)));
    row.cells_= cells;
    row.count_ = OB_RPC_MAX_REQUESTSTAT_COLUMN_ID;
    if (OB_FAIL(encode_row_packet(row))) {
      WDIAG_ICMD("fail to encode row packet", K(ret));
    }
  }
  return ret;
}

int ObShowKvHandler::dump_kv_thread_item(int event, void *data)
{
  int ret = OB_SUCCESS;
  int event_ret = EVENT_DONE;
  ObEThread *ethread = NULL;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("invalid argument", K(ret), K(event), K(data), K_(is_inited));
  } else if (OB_ISNULL(ethread = this_ethread())) {
    ret = OB_ERR_UNEXPECTED;
    WDIAG_ICMD("unexpected thread", K(ret));
  } else if (OB_FAIL(dump_kv_thread_one_thread(*ethread))) {
    WDIAG_ICMD("unexpected thread", K(ret));
  } else {
    ObRpcReqThreadQpsStat::update_last_sec_rpc_req_stat();
  }
  if (start_worker_thread_id_ == -1) {
    // submit worker thread
    start_worker_thread_id_ = ethread->id_;
  }

  if (OB_SUCC(ret) && !is_worker_thread_finished_) {
    int64_t next_id = ((ethread->id_ + 1) % g_event_processor.thread_count_for_type_[ET_NET]);
    if (next_id == start_worker_thread_id_) {
      // worker thread iterate finished
      is_worker_thread_finished_ = true;
    } else {
      if (OB_ISNULL(g_event_processor.event_thread_[ET_NET][next_id])) {
        ret = OB_ERR_UNEXPECTED;
        WDIAG_ICMD("unexpected event thread", K(ret));
      } else if (OB_ISNULL(g_event_processor.event_thread_[ET_NET][next_id]->schedule_imm(this))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        WDIAG_ICMD("fail to schedule ObShowSessionHandler", K(ret));
      } else {
        DEBUG_ICMD("success to schedule to worker thread", K(next_id));
      }
    }
  }

  if (OB_SUCC(ret) && is_worker_thread_finished_ && !is_async_thread_finished_) {
    int64_t async_thread_count = g_event_processor.thread_count_for_type_[ET_OBKV];
    DEBUG_ICMD("try to schedule rpc cmd to async thread", K(async_thread_count), K(cur_async_thread_id_));
    if (async_thread_count == 0) {
      is_async_thread_finished_ = true;
    } else if (cur_async_thread_id_ == async_thread_count - 1) {
      is_async_thread_finished_ = true;
    } else if (start_async_thread_id_ == -1) {
      // submit thread is worker thread, schedule to the first async thread
      start_async_thread_id_ = 0;
      cur_async_thread_id_ = 0;
    } else if ((cur_async_thread_id_ ++) >= async_thread_count) {
      is_async_thread_finished_ = true;
    }

    if (!is_async_thread_finished_) {
      if (cur_async_thread_id_ >= async_thread_count || cur_async_thread_id_ < 0) {
        ret = OB_ERR_UNEXPECTED;
        WDIAG_ICMD("unexpected asynec thread id", K(cur_async_thread_id_), K(async_thread_count));
      } else if (OB_ISNULL(g_event_processor.event_thread_[ET_OBKV][cur_async_thread_id_])) {
        ret = OB_ERR_UNEXPECTED;
        WDIAG_ICMD("unexpected obkv thread", K(ret));
      } else if (OB_ISNULL(g_event_processor.event_thread_[ET_OBKV][cur_async_thread_id_]->schedule_imm(this))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        WDIAG_ICMD("fail to schedule ObShowSessionHandler", K(ret));
      } else {
        DEBUG_ICMD("success to schedule to async thread", K(cur_async_thread_id_));
      }
    }
  }

  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  } else if (is_async_thread_finished_ && is_worker_thread_finished_) {
    if (OB_FAIL(encode_eof_packet())) {
      WDIAG_ICMD("fail to encode eof packet", K(ret));
    }
    DEBUG_ICMD("show proxyrpc cmd, complete all thread schedule, callback");
    event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
  }
  return event_ret;
}

int ObShowKvHandler::dump_kv_requeststat_item(int event, void *data)
{
  int ret = OB_SUCCESS;
  int event_ret = EVENT_DONE;
  ObEThread *ethread = NULL;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("invalid argument", K(ret), K(event), K(data), K_(is_inited));
  } else if (OB_ISNULL(ethread = this_ethread())) {
    ret = OB_ERR_UNEXPECTED;
    WDIAG_ICMD("unexpected thread", K(ret));
  } else if (OB_FAIL(dump_kv_requeststat_one_thread(*ethread))) {
    WDIAG_ICMD("unexpected thread", K(ret));
  } else {
  }
  if (start_worker_thread_id_ == -1) {
    // submit worker thread
    start_worker_thread_id_ = ethread->id_;
  }

  if (OB_SUCC(ret) && !is_worker_thread_finished_) {
    int64_t next_id = ((ethread->id_ + 1) % g_event_processor.thread_count_for_type_[ET_NET]);
    if (next_id == start_worker_thread_id_) {
      // worker thread iterate finished
      is_worker_thread_finished_ = true;
    } else {
      if (OB_ISNULL(g_event_processor.event_thread_[ET_NET][next_id])) {
        ret = OB_ERR_UNEXPECTED;
        WDIAG_ICMD("unexpected event thread", K(ret));
      } else if (OB_ISNULL(g_event_processor.event_thread_[ET_NET][next_id]->schedule_imm(this))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        WDIAG_ICMD("fail to schedule ObShowKvHandler", K(ret));
      } else {
        DEBUG_ICMD("success to schedule to worker thread", K(next_id));
      }
    }
  }

  if (OB_SUCC(ret) && is_worker_thread_finished_ && !is_async_thread_finished_) {
    int64_t async_thread_count = g_event_processor.thread_count_for_type_[ET_OBKV];
    DEBUG_ICMD("try to schedule rpc cmd to async thread", K(async_thread_count), K(cur_async_thread_id_));
    if (async_thread_count == 0) {
      is_async_thread_finished_ = true;
    } else if (cur_async_thread_id_ == async_thread_count - 1) {
      is_async_thread_finished_ = true;
    } else if (start_async_thread_id_ == -1) {
      // submit thread is worker thread, schedule to the first async thread
      start_async_thread_id_ = 0;
      cur_async_thread_id_ = 0;
    } else if ((cur_async_thread_id_ ++) >= async_thread_count) {
      is_async_thread_finished_ = true;
    }

    if (!is_async_thread_finished_) {
      if (cur_async_thread_id_ >= async_thread_count || cur_async_thread_id_ < 0) {
        ret = OB_ERR_UNEXPECTED;
        WDIAG_ICMD("unexpected asynec thread id", K(cur_async_thread_id_), K(async_thread_count));
      } else if (OB_ISNULL(g_event_processor.event_thread_[ET_OBKV][cur_async_thread_id_])) {
        ret = OB_ERR_UNEXPECTED;
        WDIAG_ICMD("unexpected obkv thread", K(ret));
      } else if (OB_ISNULL(g_event_processor.event_thread_[ET_OBKV][cur_async_thread_id_]->schedule_imm(this))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        WDIAG_ICMD("fail to schedule ObShowKvHandler", K(ret));
      } else {
        DEBUG_ICMD("success to schedule to async thread", K(cur_async_thread_id_));
      }
    }
  }

  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  } else if (is_async_thread_finished_ && is_worker_thread_finished_) {
    if (OB_FAIL(encode_eof_packet())) {
      WDIAG_ICMD("fail to encode eof packet", K(ret));
    }
    DEBUG_ICMD("show proxyrpc cmd, complete all thread schedule, callback");
    event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
  }
  return event_ret;
}

int ObShowKvHandler::handle_kv_thread_cmd(int event, void *data)
{
  int ret = OB_SUCCESS;
  int event_ret = EVENT_DONE;
  bool need_callback = true;
  ObEThread *ethread = NULL;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("invalid argumnet, it should not happen", K(event), K(data), K(is_inited_), K(ret));
  } else if (OB_FAIL(dump_header())) {
    WDIAG_ICMD("fail to dump list header", K(ret));
  } else if (OB_ISNULL(ethread = this_ethread())) {
    ret = OB_ERR_UNEXPECTED;
    WDIAG_ICMD("unexpected ethread", K(ret));
  } else {
    SET_HANDLER(&ObShowKvHandler::dump_kv_thread_item);
    if (OB_ISNULL(ethread->schedule_imm(this))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      WDIAG_ICMD("fail to schedule self", K(ret));
    } else {
      need_callback = false;
    }
  }
  if (need_callback) {
    if (OB_FAIL(ret)) {
      event_ret = internal_error_callback(ret);
    } else {
      event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
    }
  }
  return event_ret;
}

int ObShowKvHandler::handle_kv_requeststat_cmd(int event, void *data)
{
  int ret = OB_SUCCESS;
  int event_ret = EVENT_DONE;
  bool need_callback = true;
  ObEThread *ethread = NULL;
  if (OB_UNLIKELY(!is_argument_valid(event, data))) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("invalid argumnet, it should not happen", K(event), K(data), K(is_inited_), K(ret));
  } else if (OB_FAIL(dump_header())) {
    WDIAG_ICMD("fail to dump list header", K(ret));
  } else if (OB_ISNULL(ethread = this_ethread())) {
    ret = OB_ERR_UNEXPECTED;
    WDIAG_ICMD("unexpected ethread", K(ret));
  } else {
    SET_HANDLER(&ObShowKvHandler::dump_kv_requeststat_item);
    if (OB_ISNULL(ethread->schedule_imm(this))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      WDIAG_ICMD("fail to schedule self", K(ret));
    } else {
      need_callback = false;
    }
  }
  if (need_callback) {
    if (OB_FAIL(ret)) {
      event_ret = internal_error_callback(ret);
    } else {
      event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
    }
  }
  return event_ret;
}

static int show_kv_cmd_callback(ObContinuation *cont, ObInternalCmdInfo &info, ObMIOBuffer *buf, ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;
  ObEThread *ethread = NULL;
  ObShowKvHandler *handler = NULL;

  if (OB_UNLIKELY(!ObInternalCmdHandler::is_constructor_argument_valid(cont, buf))) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("constructor argument is invalid", K(cont), K(buf), K(ret));
  } else if (OB_ISNULL(handler = new (std::nothrow) ObShowKvHandler(cont, buf, info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    WDIAG_ICMD("fail to alloc mem for ObShowKvHandler", K(ret));
  } else if (OB_FAIL(handler->init())) {
    WDIAG_ICMD("fail to init ObShowKvHandler", K(ret));
  } else if (OB_ISNULL(ethread = this_ethread())) {
    ret = OB_ERR_UNEXPECTED;
    WDIAG_ICMD("cur ethread is null, it should not happened", K(ret));
  } else {
    DEBUG_ICMD("succ to schedule ObShowKvHandler cont");
    if (OBPROXY_T_SUB_KV_THREAD == info.get_sub_cmd_type()) {
      SET_CONTINUATION_HANDLER(handler, &ObShowKvHandler::handle_kv_thread_cmd);
    } else if (OBPROXY_T_SUB_KV_REQUESTSTAT == info.get_sub_cmd_type()) {
      SET_CONTINUATION_HANDLER(handler, &ObShowKvHandler::handle_kv_requeststat_cmd);
    } else {
      ret = OB_NOT_SUPPORTED;
      WDIAG_ICMD("unsupported cmd type", "cmd_type", info.get_sub_cmd_type());
    }
    if (OB_SUCC(ret)) {
      action = &handler->get_action();
      if (OB_ISNULL(ethread->schedule_imm(handler))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        WDIAG_ICMD("fail to schedule ObShowKvHandler", K(ret));
      }
    }
  }
  if (OB_FAIL(ret) && OB_LIKELY(NULL != handler)) {
    delete handler;
    handler = NULL;
  }
  return ret;
}

int show_kv_cmd_init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_global_internal_cmd_processor().register_cmd(OBPROXY_T_ICMD_SHOW_RPC, &show_kv_cmd_callback))) {
    WDIAG_ICMD("fail to register OBPROXY_T_ICMD_SHOW_RPC", K(ret));
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
