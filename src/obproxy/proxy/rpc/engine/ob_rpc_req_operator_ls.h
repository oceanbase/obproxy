
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

#ifndef OBPROXY_OB_PROXY_RPC_REQ_OPERATOR_LS_H
#define OBPROXY_OB_PROXY_RPC_REQ_OPERATOR_LS_H

#include "ob_rpc_req_operator.h"
#include "lib/hash/ob_hashmap.h"
#include "obkv/table/ob_table.h"
#include "obkv/table/ob_table_rpc_request.h"

namespace oceanbase {
namespace obproxy {
namespace executor {
class ObProxyRpcParallelResp;
}
namespace engine {



class ObProxyRpcReqLSOp : public ObProxyRpcReqOperator 
{
public:
  ObProxyRpcReqLSOp(proxy::ObRpcReq *input, common::ObIAllocator &allocator)
  : ObProxyRpcReqOperator(input, allocator), error_resp_count_(0), first_error_result_(NULL),
    batch_resp_(NULL), resp_map_()
  {
    set_rpc_op_type(PHY_RPC_BATCH_API);
    resp_map_.create(OB_ALIAS_TABLE_MAP_MAX_BUCKET_NUM,
                     ObModIds::OB_HASH_ALIAS_TABLE_MAP);
  }
  virtual ~ObProxyRpcReqLSOp();
  virtual int handle_result(void *data, bool &is_final, proxy::ObRpcReq *&result);

  virtual int handle_response_result(void *data, bool &is_final, proxy::ObRpcReq *&result);

  virtual void release_cache_resultset();

private:
  int generate_one_result_resp(obkv::ObTableLSOpResult &tablet_op_result,
                               obkv::ObRpcResponse *&last_repsone);
  int generate_normal_resp(obkv::ObTableLSOpResult &ls_op_result, 
                           obkv::ObRpcTableLSOperationRequest &ls_request,
                           obkv::ObRpcResponse *&last_response);

  static int init_index_arr(int64_t *&arr, const int64_t count);
  static void free_index_arr(int64_t *&arr, const int64_t count);
protected:
  int64_t error_resp_count_;
  proxy::ObRpcReq *first_error_result_;
  proxy::ObRpcReq *batch_resp_;
  typedef hash::ObHashMap<int64_t, proxy::ObRpcReq *> RPC_LS_RESP_MAP;
  RPC_LS_RESP_MAP resp_map_; 
};

}
}
}

#endif
