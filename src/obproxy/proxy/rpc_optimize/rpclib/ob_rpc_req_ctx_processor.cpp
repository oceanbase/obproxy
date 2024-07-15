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

#define USING_LOG_PREFIX PROXY

#include "proxy/rpc_optimize/rpclib/ob_rpc_req_ctx_processor.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

class ObRpcReqCtxCont : public ObContinuation
{
public:
  ObRpcReqCtxCont();
  virtual ~ObRpcReqCtxCont() {}

  int main_handler(int event, void *data);
  int init(ObRpcReqCtxParam &param);
  ObAction *get_action() { return &action_; }
  static const char *get_event_name(const int64_t event);
  void kill_this();
  void set_need_notify(const bool need_notify) { need_notify_ = need_notify; }

private:
  int start_lookup_rpc_req_ctx();
  int lookup_ctx_in_cache();
  int handle_lookup_cache_done();
  int notify_caller();

private:
  uint32_t magic_;
  ObRpcReqCtxParam param_;

  ObAction *pending_action_;
  ObAction action_;

  ObRpcReqCtx *gcached_ctx_; // the ctx from global cache

  bool kill_self_;
  bool need_notify_;
  DISALLOW_COPY_AND_ASSIGN(ObRpcReqCtxCont);
};

ObRpcReqCtxCont::ObRpcReqCtxCont()
  : ObContinuation(), magic_(OB_CONT_MAGIC_ALIVE),
    param_(), pending_action_(NULL), action_(), gcached_ctx_(NULL),
    kill_self_(false), need_notify_(true)
{
  SET_HANDLER(&ObRpcReqCtxCont::main_handler);
}

inline int ObRpcReqCtxCont::init(ObRpcReqCtxParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid rpc ctx param", K(param), K(ret));
  } else {
    param_.deep_copy(param);
    action_.set_continuation(param.cont_);
    mutex_ = param.cont_->mutex_;
  }

  return ret;
}

void ObRpcReqCtxCont::kill_this()
{
  param_.reset();
  int ret = OB_SUCCESS;
  if (NULL != pending_action_) {
    if (OB_FAIL(pending_action_->cancel())) {
      LOG_WDIAG("fail to cancel pending action", K_(pending_action), K(ret));
    } else {
      pending_action_ = NULL;
    }
  }

  if (NULL != gcached_ctx_) {
    gcached_ctx_->dec_ref();
    gcached_ctx_ = NULL;
  }

  action_.set_continuation(NULL);
  magic_ = OB_CONT_MAGIC_DEAD;
  mutex_.release();

  op_free(this);
}

const char *ObRpcReqCtxCont::get_event_name(const int64_t event)
{
  const char *name = NULL;
  switch (event) {
    case RPC_REQ_CTX_LOOKUP_START_EVENT: {
      name = "RPC_REQ_CTX_LOOKUP_START_EVENT";
      break;
    }
    case RPC_REQ_CTX_LOOKUP_CACHE_DONE: {
      name = "RPC_REQ_CTX_LOOKUP_CACHE_DONE";
      break;
    }
    case RPC_REQ_CTX_LOOKUP_CACHE_EVENT: {
      name = "RPC_REQ_CTX_LOOKUP_CACHE_EVENT";
      break;
    }
    default: {
      name = "unknown event name";
      break;
    }
  }
  return name;
}

int ObRpcReqCtxCont::main_handler(int event, void *data)
{
  int he_ret = EVENT_CONT;
  int ret = OB_SUCCESS;
  LOG_DEBUG("ObRpcReqCtxCont::main_handler, received event",
            "event", get_event_name(event), K(data));
  if (OB_CONT_MAGIC_ALIVE != magic_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("this rpc ctx cont is dead", K_(magic), K(ret));
  } else if (this_ethread() != mutex_->thread_holding_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("this_ethread must be equal with thread_holding", "this_ethread",
              this_ethread(), "thread_holding", mutex_->thread_holding_, K(ret));
  } else {
    pending_action_ = NULL;
    switch (event) {
      case RPC_REQ_CTX_LOOKUP_START_EVENT: {
        if (OB_FAIL(start_lookup_rpc_req_ctx())) {
          LOG_WDIAG("fail to start lookup rpc ctx", K(ret));
        }
        break;
      }
      case RPC_REQ_CTX_LOOKUP_CACHE_EVENT: {
        if (OB_FAIL(lookup_ctx_in_cache())) {
          LOG_WDIAG("fail to lookup enty in cache", K(ret));
        }
        break;
      }
      case RPC_REQ_CTX_LOOKUP_CACHE_DONE: {
        if (OB_FAIL(handle_lookup_cache_done())) {
          LOG_WDIAG("fail to handle lookup cache done", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unknow event", K(event), K(data), K(ret));
        break;
      }
    }
  }

  // if failed, just inform out
  if (OB_FAIL(ret)) {
    if (NULL != (param_.result_.target_ctx_)) {
      param_.result_.target_ctx_->dec_ref();
      param_.result_.target_ctx_ = NULL;
    }
    if (OB_FAIL(notify_caller())) {
      LOG_WDIAG("fail to notify caller result", K(ret));
    }
  }

  if (kill_self_) {
    kill_this();
    he_ret = EVENT_DONE;
  }
  return he_ret;
}

int ObRpcReqCtxCont::start_lookup_rpc_req_ctx()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(lookup_ctx_in_cache())) {
    LOG_WDIAG("fail to lookup enty in cache", K(ret));
  }

  return ret;
}

int ObRpcReqCtxCont::handle_lookup_cache_done()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gcached_ctx_)) {
    // do nothing
    LOG_DEBUG("handle_lookup_cache_done get nothing", KP_(gcached_ctx));
  }
  //  else if (!gcached_ctx_->is_avail_state()) {
  //   LOG_INFO("this rpc ctx is not in avail state", KPC_(gcached_ctx));
  //   gcached_ctx_->dec_ref();
  //   gcached_ctx_ = NULL;
  // }

  if (OB_SUCC(ret)) {
    param_.result_.target_ctx_ = gcached_ctx_;
    gcached_ctx_ = NULL;
    if (OB_FAIL(notify_caller())) { // notify_caller
      LOG_WDIAG("fail to notify caller", K(ret));
    }
  }

  return ret;
}

int ObRpcReqCtxCont::lookup_ctx_in_cache()
{
  int ret = OB_SUCCESS;
  obkv::ObTableApiCredential key = param_.key_;
  ObAction *action = NULL;
  ret = get_global_rpc_req_ctx_cache().get_rpc_req_ctx(this, key, &gcached_ctx_, action);
  if (OB_SUCC(ret)) {
    if (NULL != action) {
      pending_action_ = action;
    } else if (OB_FAIL(handle_lookup_cache_done())) {
      LOG_WDIAG("fail to handle lookup cache done", K(ret));
    }
  } else {
    LOG_WDIAG("fail to get rpc ctx loaction ctx", K_(param), K(ret));
  }

  return ret;
}

int ObRpcReqCtxCont::notify_caller()
{
  int ret = OB_SUCCESS;
  ObRpcReqCtx *&ctx = param_.result_.target_ctx_;
  if (NULL != ctx /*&& ctx->is_avail_state()*/) {
    ObRpcReqCtxRefHashMap &rpc_ctx_map = self_ethread().get_rpc_req_ctx_map();
    if (OB_FAIL(rpc_ctx_map.set(ctx))) {
      LOG_WDIAG("fail to set rpc ctx map", KPC(ctx), K(ret));
      ret = OB_SUCCESS; // ignore ret
    }
  }

  if (action_.cancelled_) {
    // when cancelled, do no forget free the table ctx
    if (NULL != ctx) {
      ctx->dec_ref();
      ctx = NULL;
    }
    LOG_INFO("ObRpcReqCtxCont has been cancelled", K_(param));
  } else if (need_notify_) {
    action_.continuation_->handle_event(RPC_REQ_CTX_LOOKUP_CACHE_DONE, &param_.result_);
    param_.result_.reset();
  } else {
    if (NULL != ctx) {
      ctx->dec_ref();
      ctx = NULL;
    }
    param_.result_.reset();
  }

  if (OB_SUCC(ret)) {
    kill_self_ = true;
  }
  return ret;
}

int64_t ObRpcReqCtxParam::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP_(cont),
       K_(key),
       K_(result));
  J_OBJ_END();
  return pos;
}

inline void ObRpcReqCtxParam::deep_copy(ObRpcReqCtxParam &other)
{
  if (this != &other) {
    cont_ = other.cont_;
    key_ = other.key_;
  }
}

int ObRpcReqCtxProcessor::get_rpc_req_ctx(ObRpcReqCtxParam &param, ObAction *&action)
{
  int ret = OB_SUCCESS;
  ObRpcReqCtx *tmp_ctx = NULL;
  ObRpcReqCtxCont *cont = NULL;
  if (OB_FAIL(get_rpc_req_ctx_from_thread_cache(param, tmp_ctx))) {
    LOG_WDIAG("fail to get rpc req ctx in thread cache", K(param), K(ret));
  } else if (NULL != tmp_ctx) { // thread cache hit
    // hand over ref
    param.result_.target_ctx_ = tmp_ctx;
    tmp_ctx = NULL;
  } else {
    // 2. find rpc ctx global cache
    cont = op_alloc(ObRpcReqCtxCont);
    if (OB_ISNULL(cont)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc ObRpcReqCtxCont", K(ret));
    } else if (OB_FAIL(cont->init(param))) {
      LOG_WDIAG("fail to init rpc ctx cont", K(ret));
    } else {
      action = cont->get_action();
      if (OB_ISNULL(self_ethread().schedule_imm(cont, RPC_REQ_CTX_LOOKUP_START_EVENT))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to schedule imm", K(ret));
      }
    }

    if (OB_FAIL(ret) && (NULL != cont)) {
      cont->kill_this();
      cont = NULL;
    }
  }

  if (OB_FAIL(ret)) {
    action = NULL;
  }

  return ret;
}

int ObRpcReqCtxProcessor::get_rpc_req_ctx_from_thread_cache(
    ObRpcReqCtxParam &param,
    ObRpcReqCtx *&ctx)
{
  int ret = OB_SUCCESS;
  ctx = NULL;
  ObRpcReqCtxRefHashMap &rpc_req_ctx_map = self_ethread().get_rpc_req_ctx_map();
  ObRpcReqCtx *tmp_ctx = NULL;
  obkv::ObTableApiCredential key = param.key_;

  tmp_ctx = rpc_req_ctx_map.get(key); // get will inc ctx's ref
  if (NULL != tmp_ctx) {
    bool find_succ = false;
    if (tmp_ctx->is_deleting()) {
      LOG_DEBUG("this rpc ctx has deleted", KPC(tmp_ctx));
    } else if (get_global_rpc_req_ctx_cache().is_rpc_req_ctx_expired(*tmp_ctx)) {
      // rpc ctx has expired
      LOG_DEBUG("the rpc ctx is expired",
                "expire_time_us", get_global_rpc_req_ctx_cache().get_cache_expire_time_us(),
                KPC(tmp_ctx), K(param));
    } else {
      find_succ = true;
    }

    if (!find_succ) {
      tmp_ctx->dec_ref();
      tmp_ctx = NULL;
    } else {
      LOG_DEBUG("get rpc ctx from thread cache succ", KPC(tmp_ctx));
    }
  }
  ctx = tmp_ctx;

  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
