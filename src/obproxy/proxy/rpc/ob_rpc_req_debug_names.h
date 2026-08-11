/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_RPC_REQ_DEBUG_NAMES_H
#define OBPROXY_RPC_REQ_DEBUG_NAMES_H
#include "proxy/rpc/ob_rpc_request_sm.h"

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
