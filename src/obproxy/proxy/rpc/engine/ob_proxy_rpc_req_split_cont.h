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

#ifndef OBPROXY_RPC_REQ_SPLIT_CONT_H
#define OBPROXY_RPC_REQ_SPLIT_CONT_H

#include "iocore/eventsystem/ob_continuation.h"
#include "proxy/rpc/ob_rpc_req.h"
#include "proxy/rpc/engine/ob_rpc_req_operator.h"
#include "lib/hash/ob_hashmap.h"
#include "obkv/table/ob_table.h"
#include "obkv/table/ob_table_rpc_request.h"
#include "obkv/table/ob_table_rpc_response.h"
#include "proxy/rpc/executor/ob_rpc_req_parallel_processor.h"

namespace oceanbase
{
namespace obproxy
{
namespace engine
{

/**
 * @brief rpc request split controller
 */
class ObProxyRpcReqSplitCont : public event::ObContinuation
{
public:
  ObProxyRpcReqSplitCont(proxy::ObRpcReq *root_rpc_req, common::ObIAllocator *allocator); // ObArenaAllocator
  ~ObProxyRpcReqSplitCont();

  int init_cont(event::ObContinuation *cont); // cont: root sm
  void cleanup();
  void detach();
  int try_free_self();
  void inc_pending_cb() { ATOMIC_INC(&pending_cb_count_); }
  void dec_pending_cb() { ATOMIC_DEC(&pending_cb_count_); }
  void cancel_recursively();
  int dispatch_cancel_to_child_request_only();
  proxy::ObRpcReq *get_root_rpc_req() const { return root_rpc_req_; }
  void set_need_cancel(int64_t index, bool need_cancel);
  bool get_need_cancel(int64_t index) const;
  void set_canceled(bool is_canceled) { is_canceled_ = is_canceled; }
  bool is_canceled() const { return is_canceled_; }

  int main_handler(int event, void *data);

  int execute_rpc_request();

private:
  static constexpr uint8_t DEFAULT_PARALLEL_SUB_REQUEST_COUNT = 4;
  typedef common::ObSEArray<executor::ObProxyRpcParallelParam, DEFAULT_PARALLEL_SUB_REQUEST_COUNT> ParallelParamArray;
  typedef common::ObSEArray<uintptr_t, DEFAULT_PARALLEL_SUB_REQUEST_COUNT> FALLBACK_ALLOCATOR_ARRAY;
  typedef hash::ObHashMap<int64_t, oceanbase::obproxy::obkv::ObRpcResponse *> RESP_MAP;
  typedef hash::ObHashMap<int64_t, common::ObIAllocator *> ALLOCATOR_MAP;

  event::ObContinuation *cb_cont_;
  event::ObProxyMutex *mutex_;
  common::ObIAllocator *allocator_;
  event::ObEThread *execute_thread_;
  ALLOCATOR_MAP allocator_map_;
  proxy::ObRpcReq *root_rpc_req_;
  proxy::ObRpcReqTraceId rpc_trace_id_;
  obrpc::ObRpcPacketCode request_pcode_;

  ParallelParamArray parallel_param_;
  FALLBACK_ALLOCATOR_ARRAY fallback_allocator_array_;
  int64_t sub_req_index_;
  int64_t completed_sub_req_count_;

  proxy::ObRpcReq *result_;
  RESP_MAP resp_map_;
  bool received_error_event_;
  bool is_canceled_;
  bool detached_;
  volatile int64_t pending_cb_count_;
  // === split and schedule sub request methods ===
  int init_and_schedule_sub_sm(proxy::ObRpcReq *sub_rpc_req, int64_t index);
  int dispatch_request_by_type(proxy::ObRpcReq* rpc_req,
                               ParallelParamArray &parallel_param);
  int handle_shard_rpc_obkv_batch_request(proxy::ObRpcReq* rpc_req,
                                          ParallelParamArray &parallel_param);
  int handle_shard_rpc_ls_request(proxy::ObRpcReq* rpc_req,
                                  ParallelParamArray &parallel_param);
  int handle_shard_rpc_obkv_query_request(proxy::ObRpcReq* rpc_req,
                                          ParallelParamArray &parallel_param);

  // === receive results methods ===
  virtual int handle_result(void *data, bool &is_final);
  int dispatch_result_by_type(proxy::ObRpcReq *sub_rpc_req, bool &is_final);
  int handle_query_result(proxy::ObRpcReq *sub_rpc_req, bool &is_final);
  int handle_batch_result(proxy::ObRpcReq *sub_rpc_req, bool &is_final);
  int handle_ls_result(proxy::ObRpcReq *sub_rpc_req, bool &is_final);
  int generate_batch_one_result_resp(obkv::ObTableBatchOperationResult &batch_result, obkv::ObRpcResponse *&last_response);
  int generate_batch_normal_resp(obkv::ObTableBatchOperationResult &batch_result, obkv::ObRpcTableBatchOperationRequest &batch_request, obkv::ObRpcResponse *&last_response);
  int generate_ls_one_result_resp(obkv::ObTableLSOpResult &ls_op_result, obkv::ObRpcResponse *&last_response);
  int generate_ls_normal_resp(obkv::ObTableLSOpResult &ls_op_result,
                             obkv::ObRpcTableLSOperationRequest &ls_request,
                             obkv::ObRpcResponse *&last_response);
  int clone_ls_response(obkv::ObRpcTableLSOperationResponse &ls_resp);
  int init_index_arr(int64_t *&arr, const int64_t count);
  void free_index_arr(int64_t *&arr, const int64_t count);
  int generate_error_resp(proxy::ObRpcReq *error_sub_req);

  int count_inner_callback_nums(bool &if_final);
  int cleanup_completed_sub_request(proxy::ObRpcReq *sub_rpc_req,
                                   proxy::ObRpcReq::ClientNetState cleanup_state, bool need_clean_sub_req_allocator);

};

} // end of namespace engine
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_RPC_REQ_SPLIT_CONT_H