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

#include "proxy/rpc_optimize/ob_rpc_req_debug_names.h"
#include "proxy/rpc_optimize/ob_rpc_request_sm.h"
#include "proxy/rpc_optimize/net/ob_rpc_client_net_handler.h"
#include "proxy/rpc_optimize/net/ob_rpc_server_net_handler.h"
#include "proxy/rpc_optimize/rpclib/ob_tablegroup_entry.h"
#include "proxy/rpc_optimize/rpclib/ob_table_query_async_entry.h"
#include "proxy/rpc_optimize/rpclib/ob_rpc_req_ctx.h"
#include "proxy/rpc_optimize/net/ob_rpc_server_net_handler.h"
#include "proxy/client/ob_client_vc.h"
#include "obutils/ob_resource_pool_processor.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
const char *ObRpcReqDebugNames::get_action_name(enum ObRpcRequestSMActionType event)
{
  const char *ret = NULL;

  switch (event) {
    case RPC_REQ_NEW_REQUEST: {
      ret = "RPC_REQ_NEW_REQUEST";
      break;
    }
    case RPC_REQ_IN_CLUSTER_BUILD: {
      ret = "RPC_REQ_IN_CLUSTER_BUILD";
      break;
    }
    case RPC_REQ_QUERY_ASYNC_INFO_GET: {
      ret = "RPC_REQ_QUERY_ASYNC_INFO_GET";
      break;
    }
    case RPC_REQ_INNER_INFO_GET: {
      ret = "RPC_REQ_INNER_INFO_GET";
      break;
    }
    case RPC_REQ_IN_TABLEGROUP_LOOKUP: {
      ret = "RPC_REQ_IN_TABLEGROUP_LOOKUP";
      break;
    }
    case RPC_REQ_IN_PARTITION_LOOKUP: {
      ret = "RPC_REQ_IN_PARTITION_LOOKUP";
      break;
    }
    case RPC_REQ_PARTITION_LOOKUP_DONE: {
      ret = "RPC_REQ_PARTITION_LOOKUP_DONE";
      break;
    }
    case RPC_REQ_IN_INDEX_LOOKUP: {
      ret = "RPC_REQ_IN_INDEX_LOOKUP";
      break;
    }
    case RPC_REQ_SERVER_ADDR_SEARCHING: {
      ret = "RPC_REQ_SERVER_ADDR_SEARCHING";
      break;
    }
    case RPC_REQ_SERVER_ADDR_SEARCHED: {
      ret = "RPC_REQ_SERVER_ADDR_SEARCHED";
      break;
    }
    case RPC_REQ_IN_CONGESTION_CONTROL_LOOKUP: {
      ret = "RPC_REQ_IN_CONGESTION_CONTROL_LOOKUP";
      break;
    }
    case RPC_REQ_CONGESTION_CONTROL_LOOKUP_DONE: {
      ret = "RPC_REQ_CONGESTION_CONTROL_LOOKUP_DONE";
      break;
    }
    case RPC_REQ_REQUEST_SERVER_SENDING: {
      ret = "RPC_REQ_REQUEST_SERVER_SENDING";
      break;
    }
    case RPC_REQ_REQUEST_REWRITE: {
      ret = "RPC_REQ_REQUEST_REWRITE";
      break;
    }
    case RPC_REQ_PROCESS_RESPONSE: {
      ret = "RPC_REQ_PROCESS_RESPONSE";
      break;
    }
    case RPC_REQ_RESPONSE_REWRITE: {
      ret = "RPC_REQ_RESPONSE_REWRITE";
      break;
    }
    case RPC_REQ_RESPONSE_CLIENT_SENDING: {
      ret = "RPC_REQ_RESPONSE_CLIENT_SENDING";
      break;
    }
    case RPC_REQ_INTERNAL_BUILD_RESPONSE: {
      ret = "RPC_REQ_INTERNAL_BUILD_RESPONSE";
      break;
    }
    case RPC_REQ_REQUEST_INNER_REQUEST_CLEANUP: {
      ret = "RPC_REQ_REQUEST_INNER_REQUEST_CLEANUP";
      break;
    }
    case RPC_REQ_REQUEST_CLEANUP: {
      ret = "RPC_REQ_REQUEST_CLEANUP";
      break;
    }
    case RPC_REQ_REQUEST_DONE: {
      ret = "RPC_REQ_REQUEST_DONE";
      break;
    }
    case RPC_REQ_REQUEST_RETRY: {
      ret = "RPC_REQ_REQUEST_RETRY";
      break;
    }
    case RPC_REQ_HANDLE_SHARD_REQUEST: {
      ret = "RPC_REQ_HANDLE_SHARD_REQUEST";
      break;
    }
    case RPC_REQ_HANDLE_SHARD_REQUEST_DONE: {
      ret = "RPC_REQ_HANDLE_SHARD_REQUEST_DONE";
      break;
    }
    case RPC_REQ_REQUEST_ERROR: {
      ret = "RPC_REQ_REQUEST_ERROR";
      break;
    }
    case RPC_REQ_REQUEST_TIMEOUT: {
      ret = "RPC_REQ_REQUEST_TIMEOUT";
      break;
    } //later than server
    case RPC_REQ_REQUEST_NEED_BE_CLEAN: {
      ret = "RPC_REQ_REQUEST_NEED_BE_CLEAN";
      break;
    }
    default:
      ret = "unknown event";
      break;
  }
  return ret;
}

const char *ObRpcReqDebugNames::get_event_name(const int event)
{
  const char *ret = NULL;

  switch (event) {
    // Vconnection Events
    case VC_EVENT_NONE:
      ret = "VC_EVENT_NONE";
      break;

    case VC_EVENT_IMMEDIATE:
      ret = "VC_EVENT_IMMEDIATE";
      break;

    case VC_EVENT_READ_READY:
      ret = "VC_EVENT_READ_READY";
      break;

    case VC_EVENT_WRITE_READY:
      ret = "VC_EVENT_WRITE_READY";
      break;

    case VC_EVENT_READ_COMPLETE:
      ret = "VC_EVENT_READ_COMPLETE";
      break;

    case VC_EVENT_WRITE_COMPLETE:
      ret = "VC_EVENT_WRITE_COMPLETE";
      break;

    case VC_EVENT_EOS:
      ret = "VC_EVENT_EOS";
      break;

    case VC_EVENT_ERROR:
      ret = "VC_EVENT_ERROR";
      break;

    case VC_EVENT_INACTIVITY_TIMEOUT:
      ret = "VC_EVENT_INACTIVITY_TIMEOUT";
      break;

    case VC_EVENT_ACTIVE_TIMEOUT:
      ret = "VC_EVENT_ACTIVE_TIMEOUT";
      break;

    case EVENT_INTERVAL:
      ret = "VC_EVENT_INTERVAL";
      break;

    // Net Events
    case NET_EVENT_OPEN:
      ret = "NET_EVENT_OPEN";
      break;

    case NET_EVENT_ACCEPT:
      ret = "NET_EVENT_ACCEPT";
      break;

    case NET_EVENT_OPEN_FAILED:
      ret = "NET_EVENT_OPEN_FAILED";
      break;

    //case TRANSFORM_READ_READY:
    //  ret = "TRANSFORM_READ_READY";
    //  break;

    case ASYNC_PROCESS_INVALID_EVENT:
      ret = "ASYNC_PROCESS_INVALID_EVENT";
      break;

    case CLUSTER_RESOURCE_CREATE_COMPLETE_EVENT:
      ret = "CLUSTER_RESOURCE_CREATE_COMPLETE_EVENT";
      break;

    case ASYNC_PROCESS_DONE_EVENT:
      ret = "ASYNC_PROCESS_DONE_EVENT";
      break;

    // client vc swap mutex
    case CLIENT_VC_SWAP_MUTEX_EVENT:
      ret = "CLIENT_VS_SWAP_MUTEX_EVENT";
      break;

    case CLIENT_VC_DISCONNECT_LAST_USED_SS_EVENT:
      ret = "CLIENT_VC_DISCONNECT_LAST_USED_SS_EVENT";
      break;

    case CLIENT_SESSION_ERASE_FROM_MAP_EVENT:
      ret = "CLIENT_SESSION_ERASE_FROM_MAP__EVENT";
      break;
    // Internal Cmd
    case INTERNAL_CMD_EVENTS_SUCCESS:
      ret = "INTERNAL_CMD_EVENTS_SUCCESS";
      break;

    case INTERNAL_CMD_EVENTS_FAILED:
      ret = "INTERNAL_CMD_EVENTS_FAILED";
      break;

    // kill query
    case SERVER_ADDR_LOOKUP_EVENT_DONE:
      ret = "SERVER_ADDR_LOOKUP_EVENT_DONE";
      break;

      // Congestion
    case CONGESTION_EVENT_ALIVE_CONGESTED:
      ret = "CONGESTION_EVENT_ALIVE_CONGESTED";
      break;

    case CONGESTION_EVENT_DEAD_CONGESTED:
      ret = "CONGESTION_EVENT_DEAD_CONGESTED";
      break;

    case CONGESTION_EVENT_CONGESTED_LIST_DONE:
      ret = "CONGESTION_EVENT_CONGESTED_LIST_DONE";
      break;

    case CONGESTION_EVENT_CONTROL_LOOKUP_DONE:
      ret = "CONGESTION_EVENT_CONTROL_LOOKUP_DONE";
      break;

    // table entry
    case TABLE_ENTRY_EVENT_LOOKUP_DONE:
      ret = "TABLE_ENTRY_EVENT_LOOKUP_DONE";
      break;

    case RPC_REQUEST_SM_DONE:
      ret = "RPC_REQUEST_SM_DONE";
      break;
    
    case RPC_REQUEST_SM_CLEANUP:
      ret = "RPC_REQUEST_SM_CLEANUP";
      break;

    case RPC_REQUEST_SM_CALL_NEXT:
      ret = "RPC_REQUEST_SM_CALL_NEXT";
      break;

    // client net
    case RPC_CLIENT_NET_PERIOD_TASK:
      ret = "RPC_CLIENT_NET_PERIOD_TASK";
      break;
    case RPC_CLIENT_NET_SEND_RESPONSE:
      ret = "RPC_CLIENT_NET_SEND_RESPONSE";
      break;

    // server net
    case RPC_SERVER_NET_REQUEST_SEND:
      ret = "RPC_SERVER_NET_REQUEST_SEND";
      break;
    case RPC_SERVER_NET_RESPONSE_RECVD:
      ret = "RPC_SERVER_NET_RESPONSE_RECVD";
      break;
    case RPC_SERVER_NET_PERIOD_TASK:
      ret = "RPC_SERVER_NET_PERIOD_TASK";
      break;
    case RPC_SERVER_NET_TABLE_ENTRY_SEND_REQUEST:
      ret = "RPC_SERVER_NET_TABLE_ENTRY_SEND_REQUEST";
      break;
    case RPC_SERVER_NET_TABLE_ENTRY_DESTROY:
      ret = "RPC_SERVER_NET_TABLE_ENTRY_DESTROY";
      break;
    case RPC_SERVER_NET_TABLE_ENTRY_PERIOD_TASK:
      ret = "RPC_SERVER_NET_TABLE_ENTRY_PERIOD_TASK";
      break;
    
    // tablegroup
    case TABLEGROUP_ENTRY_LOOKUP_CACHE_DONE:
      ret = "TABLEGROUP_ENTRY_LOOKUP_CACHE_DONE";
      break;
    case TABLEGROUP_ENTRY_LOOKUP_START_EVENT:
      ret = "TABLEGROUP_ENTRY_LOOKUP_START_EVENT";
      break;
    case TABLEGROUP_ENTRY_LOOKUP_CACHE_EVENT:
      ret = "TABLEGROUP_ENTRY_LOOKUP_CACHE_EVENT";
      break;
    case TABLEGROUP_ENTRY_LOOKUP_REMOTE_EVENT:
      ret = "TABLEGROUP_ENTRY_LOOKUP_REMOTE_EVENT";
      break;
    case TABLEGROUP_ENTRY_FAIL_SCHEDULE_LOOKUP_REMOTE_EVENT:
      ret = "TABLEGROUP_ENTRY_FAIL_SCHEDULE_LOOKUP_REMOTE_EVENT";
      break;

    // asyncquery
    case RPC_QUERY_ASYNC_ENTRY_LOOKUP_CACHE_DONE:
      ret = "RPC_QUERY_ASYNC_ENTRY_LOOKUP_CACHE_DONE";
      break;
    case RPC_QUERY_ASYNC_ENTRY_LOOKUP_START_EVENT:
      ret = "RPC_QUERY_ASYNC_ENTRY_LOOKUP_START_EVENT";
      break;
    case RPC_QUERY_ASYNC_ENTRY_LOOKUP_CACHE_EVENT:
      ret = "RPC_QUERY_ASYNC_ENTRY_LOOKUP_CACHE_EVENT";
      break;
    
    // rpc_ctx
    case RPC_REQ_CTX_LOOKUP_CACHE_DONE:
      ret = "RPC_REQ_CTX_LOOKUP_CACHE_DONE";
      break;
    case RPC_REQ_CTX_LOOKUP_START_EVENT:
      ret = "RPC_REQ_CTX_LOOKUP_START_EVENT";
      break;
    case RPC_REQ_CTX_LOOKUP_CACHE_EVENT:
      ret = "RPC_REQ_CTX_LOOKUP_CACHE_EVENT";
      break;
 
    default:
      ret = "unknown event";
      break;
  }
  return ret;
}

const char *ObRpcReqDebugNames::get_client_state_name(enum ObRpcReq::ClientNetState event)
{
  const char *ret = NULL;
  switch (event)
  {
  case ObRpcReq::ClientNetState::RPC_REQ_CLIENT_INIT:
    ret = "RPC_REQ_CLIENT_INIT";
    break;
  case ObRpcReq::ClientNetState::RPC_REQ_CLIENT_REQUEST_READ:
    ret = "RPC_REQ_CLIENT_REQUEST_READ";
    break;
  case ObRpcReq::ClientNetState::RPC_REQ_CLIENT_REQUEST_HANDLING:
    ret = "RPC_REQ_CLIENT_REQUEST_HANDLING";
    break;
  case ObRpcReq::ClientNetState::RPC_REQ_CLIENT_RESPONSE_HANDLING:
    ret = "RPC_REQ_CLIENT_RESPONSE_HANDLING";
    break;
  case ObRpcReq::ClientNetState::RPC_REQ_CLIENT_RESPONSE_SEND:
    ret = "RPC_REQ_CLIENT_RESPONSE_SEND";
    break;
  case ObRpcReq::ClientNetState::RPC_REQ_CLIENT_INNER_REQUEST:
    ret = "RPC_REQ_CLIENT_INNER_REQUEST";
    break;
  case ObRpcReq::ClientNetState::RPC_REQ_CLIENT_DONE:
    ret = "RPC_REQ_CLIENT_DONE";
    break;
  case ObRpcReq::ClientNetState::RPC_REQ_CLIENT_INNER_REQUEST_DONE:
    ret = "RPC_REQ_CLIENT_INNER_REQUEST_DONE";
    break;
  case ObRpcReq::ClientNetState::RPC_REQ_CLIENT_DESTROY:
    ret = "RPC_REQ_CLIENT_DESTROY";
    break;
  case ObRpcReq::ClientNetState::RPC_REQ_CLIENT_CANCLED:
    ret = "RPC_REQ_CLIENT_CANCLED";
    break;
  default:
    ret = "unknown state";
    break;
  }

  return ret;
}

const char *ObRpcReqDebugNames::get_server_state_name(enum ObRpcReq::ServerNetState event)
{
  const char *ret = NULL;
  switch (event)
  {
  case ObRpcReq::ServerNetState::RPC_REQ_SERVER_INIT:
    ret = "RPC_REQ_SERVER_INIT";
    break;
  case ObRpcReq::ServerNetState::RPC_REQ_SERVER_ENTRY_LOOKUP:
    ret = "RPC_REQ_SERVER_ENTRY_LOOKUP";
    break;
  case ObRpcReq::ServerNetState::RPC_REQ_SERVER_ENTRY_WARTING:
    ret = "RPC_REQ_SERVER_ENTRY_WARTING";
    break;
  case ObRpcReq::ServerNetState::RPC_REQ_SERVER_ENTRY_LOOKUP_DONE:
    ret = "RPC_REQ_SERVER_ENTRY_LOOKUP_DONE";
    break;
  case ObRpcReq::ServerNetState::RPC_REQ_SERVER_REQUST_SENDING:
    ret = "RPC_REQ_SERVER_REQUST_SENDING";
    break;
  case ObRpcReq::ServerNetState::RPC_REQ_SERVER_REQUST_SENDED:
    ret = "RPC_REQ_SERVER_REQUST_SENDED";
    break;
  case ObRpcReq::ServerNetState::RPC_REQ_SERVER_RESPONSE_READING:
    ret = "RPC_REQ_SERVER_RESPONSE_READING";
    break;
  case ObRpcReq::ServerNetState::RPC_REQ_SERVER_RESPONSE_READED:
    ret = "RPC_REQ_SERVER_RESPONSE_READED";
    break;
  case ObRpcReq::ServerNetState::RPC_REQ_SERVER_SHARDING_REQUEST_HANDLING:
    ret = "RPC_REQ_SERVER_SHARDING_REQUEST_HANDLING";
    break;
  case ObRpcReq::ServerNetState::RPC_REQ_SERVER_DONE:
    ret = "RPC_REQ_SERVER_DONE";
    break;
  case ObRpcReq::ServerNetState::RPC_REQ_SERVER_SHARDING_REQUEST_DONE:
    ret = "RPC_REQ_SERVER_SHARDING_REQUEST_DONE";
    break;
  case ObRpcReq::ServerNetState::RPC_REQ_SERVER_DESTROY:
    ret = "RPC_REQ_SERVER_DESTROY";
    break;
  case ObRpcReq::ServerNetState::RPC_REQ_SERVER_CANCLED:
    ret = "RPC_REQ_SERVER_CANCLED";
    break;
  default:
    ret = "unknown state";
    break;
  }

  return ret;
}

const char *ObRpcReqDebugNames::get_rpc_sm_state_name(enum ObRpcReq::RpcReqSmState event)
{
  const char *ret = NULL;
  switch (event)
  {
  case ObRpcReq::RpcReqSmState::RPC_REQ_SM_INIT:
    ret = "RPC_REQ_SM_INIT";
    break;
  case ObRpcReq::RpcReqSmState::RPC_REQ_SM_REQUEST_ANALYZE:
    ret = "RPC_REQ_SM_REQUEST_ANALYZE";
    break;
  case ObRpcReq::RpcReqSmState::RPC_REQ_SM_CLUSTER_BUILD:
    ret = "RPC_REQ_SM_CLUSTER_BUILD";
    break;
  case ObRpcReq::RpcReqSmState::RPC_REQ_SM_PARTITION_LOOKUP:
    ret = "RPC_REQ_SM_PARTITION_LOOKUP";
    break;
  case ObRpcReq::RpcReqSmState::RPC_REQ_SM_ADDR_SEARCH:
    ret = "RPC_REQ_SM_ADDR_SEARCH";
    break;
  case ObRpcReq::RpcReqSmState::RPC_REQ_SM_SHARDING_HANDLE:
    ret = "RPC_REQ_SM_SHARDING_HANDLE";
    break;
  case ObRpcReq::RpcReqSmState::RPC_REQ_SM_SERVER_HANDLE:
    ret = "RPC_REQ_SM_SERVER_HANDLE";
    break;
  case ObRpcReq::RpcReqSmState::RPC_REQ_SM_RESPONE_RETURN:
    ret = "RPC_REQ_SM_RESPONE_RETURN";
    break;
  case ObRpcReq::RpcReqSmState::RPC_REQ_SM_REQUEST_DONE:
    ret = "RPC_REQ_SM_REQUEST_DONE";
    break;
  case ObRpcReq::RpcReqSmState::RPC_REQ_SM_INNER_ERROR:
    ret = "RPC_REQ_SM_INNER_ERROR";
    break;
  case ObRpcReq::RpcReqSmState::RPC_REQ_SM_TIMEOUT:
    ret = "RPC_REQ_SM_TIMEOUT";
    break;
  case ObRpcReq::RpcReqSmState::RPC_REQ_SM_INNER_REQUEST_CLEANUP:
    ret = "RPC_REQ_SM_INNER_REQUEST_CLEANUP";
    break;
  case ObRpcReq::RpcReqSmState::RPC_REQ_SM_REQUEST_CLEANUP:
    ret = "RPC_REQ_SM_REQUEST_CLEANUP";
    break;
  case ObRpcReq::RpcReqSmState::RPC_REQ_SM_DESTROYED:
    ret = "RPC_REQ_SM_DESTROYED";
    break;
  default:
    ret = "unknown state";
    break;
  }

  return ret;
}

const char *ObRpcReqDebugNames::get_rpc_clean_module_name(enum ObRpcReq::RpcReqCleanModule event)
{
  const char *ret = NULL;
  switch (event)
  {
  case ObRpcReq::RpcReqCleanModule::RPC_REQ_CLEAN_MODULE_CLIENT_NET:
    ret = "RPC_REQ_CLEAN_MODULE_CLIENT_NET";
    break;
  case ObRpcReq::RpcReqCleanModule::RPC_REQ_CLEAN_MODULE_SERVER_NET:
    ret = "RPC_REQ_CLEAN_MODULE_SERVER_NET";
    break;
  case ObRpcReq::RpcReqCleanModule::RPC_REQ_CLEAN_MODULE_REQUEST_SM:
    ret = "RPC_REQ_CLEAN_MODULE_REQUEST_SM";
    break;
  case ObRpcReq::RpcReqCleanModule::RPC_REQ_CLEAN_MODULE_MAX:
    ret = "RPC_REQ_CLEAN_MODULE_MAX";
    break;
  default:
    ret = "unknown state";
    break;
  }

  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
