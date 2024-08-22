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
#ifndef OBPROXY_RPC_REQ_PARALLEL_EXECUTE_CONT_H
#define OBPROXY_RPC_REQ_PARALLEL_EXECUTE_CONT_H

#include "obutils/ob_async_common_task.h"
#include "ob_rpc_req_parallel_processor.h"

namespace oceanbase
{
namespace obproxy
{
namespace executor
{

class ObProxyRpcReqParallelExecuteCont : public obutils::ObAsyncCommonTask
{
public:
  ObProxyRpcReqParallelExecuteCont(event::ObProxyMutex *m, event::ObContinuation *cb_cont, event::ObEThread *submit_thread)
      : ObAsyncCommonTask(m, "rpc parallel execute cont", cb_cont, submit_thread),
        is_save_session_mode_(false), use_last_rpc_proxy_(false), rpc_request_(NULL),
        cont_index_(-1), allocator_(NULL) {}
  ~ObProxyRpcReqParallelExecuteCont() {}

  int init(const ObProxyRpcParallelParam &parallel_param, const int64_t cont_index,
           ObIAllocator *allocator, const int64_t timeout_ms);
  void destroy();
  virtual int init_task();
  virtual int finish_task(void *data);
  virtual void *get_callback_data() {
    return static_cast<void *>(rpc_request_);
  };

private:
  bool is_save_session_mode_;
  bool use_last_rpc_proxy_;
  proxy::ObRpcReq *rpc_request_;
  int64_t cont_index_;
  common::ObIAllocator *allocator_;
};

} // end of namespace executor
} // end of namespace obproxy
} // end of namespace oceanbase

#endif //OBPROXY_RPC_REQ_PARALLEL_EXECUTE_CONT_H
