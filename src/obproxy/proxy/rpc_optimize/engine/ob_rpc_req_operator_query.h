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

#ifndef OBPROXY_OB_PROXY_RPC_REQ_OPERATOR_QUERY_H
#define OBPROXY_OB_PROXY_RPC_REQ_OPERATOR_QUERY_H

#include "ob_rpc_req_operator.h"
#include "common/ob_row.h"
#include "obproxy/obkv/table/ob_table_rpc_response.h"
#include "lib/hash/ob_hashset.h"

namespace oceanbase {
namespace obproxy {
namespace executor {
class ObProxyRpcParallelResp;
}
namespace engine {

typedef common::ObSEArray<int32_t, 4> RpcParallelRespIndexArray;
typedef common::hash::ObHashSet<int64_t> RpcParallelRespIndexSet;
typedef common::ObSEArray<obkv::ObRpcTableQueryResponse *, 4> RpcQueryespArray;

class ObProxyRpcReqQueryOp : public ObProxyRpcReqOperator
{
public:
  ObProxyRpcReqQueryOp(proxy::ObRpcReq *input, common::ObIAllocator &allocator)
    : ObProxyRpcReqOperator(input, allocator), query_resp_(NULL), resp_array_()
  {
    set_rpc_op_type(PHY_RPC_RANGE_SCAN);
  }

  virtual ~ObProxyRpcReqQueryOp();

  virtual int handle_result(void *data, bool &is_final, proxy::ObRpcReq *&result);

  virtual int handle_response_result(void *data, bool &is_final, proxy::ObRpcReq *&result);

  virtual void release_cache_resultset();

protected:
  proxy::ObRpcReq *query_resp_;
  RpcParallelRespArray resp_array_;
};

//class ObProxyRpcQueryStreamOp : public ObProxyRpcReqQueryOp
//{
//public:
  //ObProxyRpcQueryStreamOp(proxy::ObRpcReq *input, common::ObIAllocator &allocator)
    //: ObProxyRpcReqQueryOp(input, allocator), resp_array_(), query_resp_array_(), stream_index_set_(), query_resp_cached_(NULL),
      //partition_ids_count_(0), batch_count_(0), handle_stream_request_(false)
  //{
    //set_rpc_op_type(PHY_RPC_STREAM_SCAN);
  //}

  //virtual ~ObProxyRpcQueryStreamOp()
  //{
  //}

  //void set_batch_size(int32_t count) { batch_count_ = count; }
  //int32_t get_batch_size() { return batch_count_; }

  //void set_partition_ids_count(int64_t count) { partition_ids_count_ = count; }
  //int64_t get_partition_ids_count() { return partition_ids_count_; }

  //// virtual int handle_result(void *data, bool &is_final, proxy::ObRpcReq *&result);
  //virtual int handle_response_result(void *data, bool &is_final, proxy::ObRpcReq *&result);

  //[> same as execute_rpc_request <]
  //virtual int fetch_stream_response(int64_t cont_index);

  //[> get cached the result for last record. <]
  //int get_resultset_from_cached_resp(proxy::ObRpcReq *&result);

  //[> cache full batch size resultset in OP <]
  //int init_query_result(obkv::ObRpcTableQueryResponse *&query_result, obkv::ObRpcTableQueryResponse *source_result);

//protected:
  //// proxy::ObRpcReq *stream_request_;
  //RpcParallelRespArray resp_array_;
  //RpcQueryespArray query_resp_array_;
  //RpcParallelRespIndexSet stream_index_set_; //not useful
  //obkv::ObRpcTableQueryResponse *query_resp_cached_;
  //int64_t partition_ids_count_;
  //int64_t fetching_count_;
  //int64_t cur_fetch_returned_;
  //int32_t batch_count_;
  //bool handle_stream_request_;
//};

}
}
}

#endif //OBPROXY_OB_PROXY_RPC_REQ_OPERATOR_QUERY_H
