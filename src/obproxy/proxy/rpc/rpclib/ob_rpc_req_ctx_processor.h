/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_RPC_REQ_CTX_PROCESSOR_H
#define OB_RPC_REQ_CTX_PROCESSOR_H

#include "proxy/rpc/rpclib/ob_rpc_req_ctx.h"
#include "proxy/rpc/rpclib/ob_rpc_req_ctx_cache.h"

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