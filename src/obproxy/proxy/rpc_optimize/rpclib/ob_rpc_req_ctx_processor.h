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

#ifndef OB_RPC_REQ_CTX_PROCESSOR_H
#define OB_RPC_REQ_CTX_PROCESSOR_H

#include "proxy/rpc_optimize/rpclib/ob_rpc_req_ctx.h"
#include "proxy/rpc_optimize/rpclib/ob_rpc_req_ctx_cache.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

struct ObRpcReqCtxResult
{
  ObRpcReqCtxResult() : target_ctx_(NULL) {}
  ~ObRpcReqCtxResult() {}
  void reset() { target_ctx_ = NULL; }

  TO_STRING_KV(K_(target_ctx));

  ObRpcReqCtx *target_ctx_;
};

class ObRpcReqCtxParam
{
public:
  ObRpcReqCtxParam()
    : cont_(NULL), key_(), result_() {}
  ~ObRpcReqCtxParam() { reset(); }

  void reset();
  bool is_valid() const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void deep_copy(ObRpcReqCtxParam &other);

  event::ObContinuation *cont_;
  obkv::ObTableApiCredential key_;
  ObRpcReqCtxResult result_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcReqCtxParam);
};

inline bool ObRpcReqCtxParam::is_valid() const
{
  return NULL != cont_;
}

inline void ObRpcReqCtxParam::reset()
{
  cont_ = NULL;
  result_.reset();
}

class ObRpcReqCtxProcessor
{
public:
  ObRpcReqCtxProcessor() {}
  ~ObRpcReqCtxProcessor() {}

  static int get_rpc_req_ctx(ObRpcReqCtxParam &param, event::ObAction *&action);

private:
  static int get_rpc_req_ctx_from_thread_cache(ObRpcReqCtxParam &param,
                                               ObRpcReqCtx *&ctx);
  DISALLOW_COPY_AND_ASSIGN(ObRpcReqCtxProcessor);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OB_RPC_REQ_CTX_PROCESSOR_H