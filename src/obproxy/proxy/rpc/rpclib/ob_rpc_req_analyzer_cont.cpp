#define USING_LOG_PREFIX PROXY

#include "proxy/rpc/rpclib/ob_rpc_req_analyzer_cont.h"
#include "proxy/rpc/ob_rpc_req_trace.h"
#include "proxy/rpc/ob_rpc_req.h"
#include "iocore/eventsystem/ob_vconnection.h"
#include "lib/oblog/ob_log_module.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

int ObRpcReqAnalyzerCont::main_handler(int event, void *data)
{
  int ret = OB_SUCCESS;
  UNUSED(data);

  if (NULL == execute_thread_) {
    execute_thread_ = &self_ethread();
  }

  switch (event) {
    case EVENT_IMMEDIATE: {
      pending_action_ = NULL;
      if (OB_FAIL(handle_event_start())) {
        LOG_INFO("fail to handle event start", K(ret));
      }
      break;
    }
    case ASYNC_PROCESS_INFORM_OUT_EVENT: {
      pending_action_ = NULL;
      if (OB_FAIL(handle_event_inform_out())) {
        LOG_WDIAG("fail to handle inform out event", K(ret));
      }
      break;
    }
    case VC_EVENT_ACTIVE_TIMEOUT:
    case EVENT_ERROR:
    default: {
      pending_action_ = NULL;
      if (OB_FAIL(cancel_timeout_action())) {
        LOG_WDIAG("fail to cancel_timeout_action", K(ret));
      } else if (action_.cancelled_) {
        terminate_ = true;
        LOG_INFO("async task has been cancelled, will kill itself", K(ret));
      } else if (NULL != cb_cont_) {
        need_callback_ = true;
      } else {
        terminate_ = true;
      }

      if (EVENT_ERROR != event || VC_EVENT_ACTIVE_TIMEOUT != event) {
        LOG_WDIAG("error state, nerver run here", K(event), K(ret));
      } else {
        LOG_INFO("error state", K(event));
      }
      break;
    }
  }

  if (!terminate_ && (need_callback_ || OB_FAIL(ret))) {
    if (execute_thread_ == &self_ethread()) {
      execute_thread_->is_need_thread_pool_event_ = true;
    }
    if (OB_FAIL(handle_callback())) {
      LOG_WDIAG("fail to handle callback", K(ret));
    }
  }

  if (terminate_) {
    if (execute_thread_ == &self_ethread()) {
      execute_thread_->is_need_thread_pool_event_ = true;
    }
    destroy();
  }

  return EVENT_DONE;
}

int ObRpcReqAnalyzerCont::init_task()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(rpc_req_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", KP_(rpc_req), K(ret));
  } else {
    const ObRpcReqTraceId &rpc_trace_id = rpc_req_->get_trace_id();

    LOG_DEBUG("ObRpcReqAnalyzerCont::init_task", K_(ctx), K(rpc_trace_id));

    if (ctx_.is_response_) {
      if (OB_FAIL(ObProxyRpcReqAnalyzer::analyze_rpc_response(ctx_, *rpc_req_))) {
        LOG_WDIAG("fail to call analyze_rpc_response", KP_(rpc_req), K(ret), K(rpc_trace_id));
      } else if (OB_FAIL(ObProxyRpcReqAnalyzer::handle_rpc_response(ctx_, *rpc_req_))) {
        LOG_WDIAG("fail to call handle_rpc_response", K(ret), K(rpc_trace_id));
      } else {
        need_callback_ = true;
      }
    } else {
      if (OB_FAIL(ObProxyRpcReqAnalyzer::analyze_rpc_request(ctx_, *rpc_req_))) {
        LOG_WDIAG("fail to call analyze_rpc_request", KP_(rpc_req), K(ret), K(rpc_trace_id));
      } else {
        need_callback_ = true;
      }
    }
  }

  return ret;
}

void ObRpcReqAnalyzerCont::destroy()
{
  LOG_DEBUG("ObRpcReqAnalyzerCont will be destroyed", KP(this));
  ObAsyncCommonTask::destroy();
}

}
}
}