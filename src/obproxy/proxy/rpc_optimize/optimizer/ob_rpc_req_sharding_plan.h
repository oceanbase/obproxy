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

#ifndef OBPROXY_SHARDING_RPC_REQ_REQUEST_PLAN_H_
#define OBPROXY_SHARDING_RPC_REQ_REQUEST_PLAN_H_

#include "proxy/rpc_optimize/engine/ob_rpc_req_operator.h"
#include "proxy/rpc_optimize/engine/ob_rpc_req_operator_table_scan.h"
#include "proxy/rpc_optimize/ob_rpc_req.h"
// #include "proxy/rpc/ob_rpc_transact.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObRpcClientSession; //RPC
// class ObRpcTransact;
}

namespace optimizer
{

class ObProxyShardRpcReqRequestPlan
{
public:
  ObProxyShardRpcReqRequestPlan(proxy::ObRpcReq *client_request,
                          common::ObIAllocator *allocator);
  // ObProxyShardRpcReqRequestPlan(proxy::ObRpcReq *client_request,
  //                         common::ObIAllocator *allocator);
  ~ObProxyShardRpcReqRequestPlan();

  int generate_plan();

  int add_rpc_table_scan_operator();

  const proxy::ObRpcReq *get_client_request() const { return client_request_; }

  static int handle_shard_rpc_request(proxy::ObRpcReq *client_request);
  static int handle_shard_rpc_fetch_request(proxy::ObRpcReq *client_request);

  common::ObIAllocator* get_allocator() const { return allocator_; }
  engine::ObProxyRpcReqOperator* get_plan_root() const { return plan_root_; }

private:
  int add_rpc_projection_operator();
  int add_rpc_common_table_query_operator();
  // int add_rpc_stream_table_query_operator();
  int add_rpc_batch_table_operator();
  int add_rpc_ls_table_operator();
  void print_plan_info();

private:
  proxy::ObRpcReq *client_request_;
  // proxy::ObProxyRpcAuthRequest *client_auth_request_;
  common::ObIAllocator *allocator_;
  engine::ObProxyRpcReqOperator *plan_root_;
};

} // end optimizer
} // end obproxy
} // end oceanbase

#endif /* OBPROXY_SHARDING_RPC_REQ_REQUEST_PLAN_H_ */
