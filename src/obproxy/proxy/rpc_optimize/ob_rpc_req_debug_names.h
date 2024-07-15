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

#ifndef OBPROXY_RPC_REQ_DEBUG_NAMES_H
#define OBPROXY_RPC_REQ_DEBUG_NAMES_H
#include "proxy/rpc_optimize/ob_rpc_request_sm.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

class ObRpcReqDebugNames
{
public:
  static const char *get_action_name(enum ObRpcRequestSMActionType event);
  static const char *get_event_name(int event);
  static const char *get_client_state_name(enum ObRpcReq::ClientNetState event);
  static const char *get_server_state_name(enum ObRpcReq::ServerNetState event);
  static const char *get_rpc_sm_state_name(enum ObRpcReq::RpcReqSmState event);
  static const char *get_rpc_clean_module_name(enum ObRpcReq::RpcReqCleanModule event);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_MYSQL_DEBUG_NAME_H
