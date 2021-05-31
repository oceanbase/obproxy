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

#include "proxy/mysql/ob_mysql_debug_names.h"
#include "proxy/mysql/ob_mysql_client_session.h"
#include "proxy/api/ob_mysql_sm_api.h"
#include "proxy/client/ob_client_vc.h"
#include "obutils/ob_resource_pool_processor.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
const char *ObMysqlDebugNames::get_event_name(const int event)
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

    case TRANSFORM_READ_READY:
      ret = "TRANSFORM_READ_READY";
      break;

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

    //  MysqlTunnel Events
    case MYSQL_TUNNEL_EVENT_DONE:
      ret = "MYSQL_TUNNEL_EVENT_DONE";
      break;

    case MYSQL_TUNNEL_EVENT_PRECOMPLETE:
      ret = "MYSQL_TUNNEL_EVENT_PRECOMPLETE";
      break;

    case MYSQL_TUNNEL_EVENT_CONSUMER_DETACH:
      ret = "MYSQL_TUNNEL_EVENT_CONSUMER_DETACH";
      break;

    case MYSQL_TUNNEL_EVENT_CMD_COMPLETE:
      ret = "MYSQL_TUNNEL_EVENT_CMD_COMPLETE";
      break;

    // Plugin Events
    case MYSQL_API_CONTINUE:
      ret = "MYSQL_API_CONTINUE";
      break;

    case MYSQL_API_ERROR:
      ret = "MYSQL_API_ERROR";
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

    default:
      ret = "unknown event";
      break;
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
