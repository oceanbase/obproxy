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

#include "ob_rpc_req_parallel_execute_cont.h"
#include "lib/encrypt/ob_encrypted_helper.h"
#include "common/obsm_utils.h"
#include "common/ob_obj_cast.h"
#include "proxy/rpc_optimize/net/ob_rpc_client_net_handler.h"
#include "proxy/rpc_optimize/ob_rpc_req.h"
#include "proxy/rpc_optimize/ob_rpc_request_sm.h"

using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace executor
{

int ObProxyRpcReqParallelExecuteCont::init(const ObProxyRpcParallelParam &parallel_param, const int64_t cont_index, ObIAllocator *allocator, const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  UNUSED(timeout_ms);

  cont_index_ = cont_index;
  allocator_ = allocator;
  rpc_request_ = parallel_param.request_;
  rpc_request_->set_cont_index(cont_index);

  return ret;
}


int ObProxyRpcReqParallelExecuteCont::init_task()
{
  int ret = OB_SUCCESS;

  LOG_DEBUG("ObProxyRpcReqParallelExecuteCont init_task", KP(this), K(rpc_request_), KPC(rpc_request_));
  if (OB_NOT_NULL(rpc_request_) && OB_NOT_NULL(rpc_request_->cnet_sm_) && OB_NOT_NULL(rpc_request_->sm_)) {
    proxy::ObRpcRequestSM *request_sm = rpc_request_->get_request_sm();
    // init inner request sm
    if (OB_FAIL(request_sm->init_inner_request(this, mutex_))) {
      LOG_WDIAG("fail to init inner rpc request", K(ret));
    } else {
      if (OB_FAIL(request_sm->setup_rpc_get_cluster())) { //skip parser rpc request
        // todo: setup_rpc_get_cluster in async task to avoid ObProxyRpcReqParallelExecuteCont init fail.
        LOG_WDIAG("ObProxyRpcReqParallelExecuteCont invalid to handle rpc request");
      } else {
        LOG_DEBUG("ObProxyRpcReqParallelExecuteCont success to handle the inner rpc request", K(rpc_request_), K(request_sm));
      }
    }
    if (OB_FAIL(ret)) {
      request_sm->set_inner_cont(NULL);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid rpc_request to handle");
  }

  return ret;
}

int ObProxyRpcReqParallelExecuteCont::finish_task(void *data)
{
  int ret = OB_SUCCESS;

  LOG_DEBUG("ObProxyRpcReqParallelExecuteCont finish_task", KP(this), KP(data), K_(cont_index),
            KP_(cb_cont), K(ret), K_(rpc_request));

  if (NULL != data && data == rpc_request_) {
    proxy::ObRpcReq *resp = rpc_request_;
    resp->set_cont_index(cont_index_); //set final cont_index
    LOG_DEBUG("ObProxyRpcReqParallelExecuteCont finish task", K(data), K(resp));
  }

  return ret;
}


void ObProxyRpcReqParallelExecuteCont::destroy()
{
  LOG_DEBUG("parallel execute cont will be destroyed", KP(this));

  cancel_pending_action();

  // 父类里最后是用的 delete, 但是本 Cont 是用的 op_alloc 分配出来的,
  // 所以只能把父类里的 destroy 方法拷贝到这里
  cb_cont_ = NULL;
  allocator_ = NULL;
  submit_thread_ = NULL;
  mutex_.release();
  action_.mutex_.release();

  op_free(this);
}

} // end of namespace executor
} // end of namespace obproxy
} // end of namespace oceanbase
