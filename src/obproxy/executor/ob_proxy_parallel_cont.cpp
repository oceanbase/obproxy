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

#include "ob_proxy_parallel_cont.h"
#include "ob_proxy_parallel_execute_cont.h"
#include "obutils/ob_proxy_config.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace executor
{

int ObProxyParallelCont::do_open(ObAction *&action, ObIArray<ObProxyParallelParam> &parallel_param, ObIAllocator *allocator, const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;

  action = NULL;
  char *buf = NULL;
  parallel_task_count_ = parallel_param.count();
  buf_size_ = sizeof(ObAction *) * parallel_task_count_;
  timeout_ms_ = timeout_ms > 0 ? timeout_ms : usec_to_msec(get_global_proxy_config().short_async_task_timeout);

  if (OB_UNLIKELY(NULL != parallel_action_array_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("array buf is not null", K_(parallel_action_array), K(ret));
  } else if (OB_ISNULL(buf = static_cast<char *>(op_fixed_mem_alloc(buf_size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc mem", K_(buf_size), K(ret));
  } else if (FALSE_IT(MEMSET(buf, 0, buf_size_))) {
    // nerver here
  } else if (OB_ISNULL(parallel_action_array_ = new (buf) ObAction *[parallel_task_count_])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to init parallel action array", K_(parallel_task_count), K(ret));
  } else if (OB_FAIL(schedule_timeout())) {
    LOG_WDIAG("fail to schedule timeout action", K(ret));
  } else if (OB_FAIL(handle_parallel_task(parallel_param, allocator))) {
    LOG_WDIAG("fail to handle parallel task", K(ret));
  } else {
    action = &get_action();
    LOG_DEBUG("succ to schedule parallel task", KP(this));
  }

  // if failed, no need cancel timeout and task which are canceled by call's destroy func
  return ret;
}

int ObProxyParallelCont::main_handler(int event, void *data)
{
  int event_ret = EVENT_CONT;
  int ret = OB_SUCCESS;
  bool is_need_free_data = ASYNC_PROCESS_DONE_EVENT == event;

  LOG_INFO("ObProxyParallelCont::main_handler", K(event), K(this));

  if (OB_UNLIKELY(this_ethread() != mutex_->thread_holding_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("this_ethread must be equal with thread_holding", "this_ethread",
              this_ethread(), "thread_holding", mutex_->thread_holding_, K(ret));
  } else {
    switch (event) {
      case ASYNC_PROCESS_DONE_EVENT: {
        if (OB_FAIL(handle_parallel_task_complete(data, is_need_free_data))) {
          LOG_WDIAG("fail to handle parallel task complete", K(ret));
        }
        break;
      }
      case EVENT_INTERVAL: {
        if (OB_FAIL(handle_timeout())) {
          LOG_WDIAG("fail to handle timeout event", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unexpected event", K(event), K(ret));
        break;
      }
    }
  }

  if (OB_UNLIKELY(is_need_free_data) && OB_NOT_NULL(data)) {
    ObProxyParallelResp *result = static_cast<ObProxyParallelResp *>(data);
    op_free(result);
  }

  if (OB_FAIL(ret)) {
    notify_caller_error();
  }

  if (terminate_) {
    destroy();
    event_ret = EVENT_DONE;
  }

  return event_ret;
}

int ObProxyParallelCont::handle_parallel_task(ObIArray<ObProxyParallelParam> &parallel_param, ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;

  ObProxyParallelExecuteCont *execute_cont = NULL;
  ObProxyMutex *mutex = NULL;

  for (int64_t i = 0; OB_SUCC(ret) && i < parallel_task_count_; ++i) {
    execute_cont = NULL;
    mutex = NULL;
    if (OB_ISNULL(mutex = new_proxy_mutex())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_EDIAG("fail to alloc memory for mutex", K(ret));
    // child task's submit thread same as this cont's submit thread. so here no need to switch thread
    } else if (OB_ISNULL(execute_cont = op_alloc_args(ObProxyParallelExecuteCont, mutex, this, submit_thread_))) {
      LOG_WDIAG("fail to alloc parallel execute cont", K(ret));
    } else if (OB_FAIL(execute_cont->init(parallel_param.at(i), i, allocator, timeout_ms_))) {
      LOG_WDIAG("fail to init execute cont", K(ret));
    } else if (OB_ISNULL(g_event_processor.schedule_imm(execute_cont, ET_CALL))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to schedule parallel execute cont", K(ret));
    } else {
      ++target_task_count_;
      parallel_action_array_[i] = &execute_cont->get_action();
      LOG_DEBUG("succ to schedule parallel execute task", "cont index", i);
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(execute_cont)) {
      execute_cont->destroy();
      execute_cont = NULL;
    }
  }

  return ret;
}

int ObProxyParallelCont::handle_parallel_task_complete(void *data, bool &is_need_free_data)
{
  int ret = OB_SUCCESS;
  --target_task_count_;

  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("fetch result is null", K(ret));
  } else {
    if (action_.cancelled_) {
      terminate_ = true;
      LOG_INFO("ObProxyParallelCont async task has been cancelled", K(ret));
    } else {
      ObProxyParallelResp *result = static_cast<ObProxyParallelResp *>(data);
      int64_t cont_index = result->get_cont_index();

      LOG_DEBUG("ObProxyParallelCont handle_parallel_task_complete", "cont index", cont_index);

      if (OB_UNLIKELY(cont_index < 0) || OB_UNLIKELY(cont_index >= parallel_task_count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unexpected cont result", K(cont_index), K(ret));
      } else {
        parallel_action_array_[cont_index] = NULL;

        if (target_task_count_ > 0) {
          cb_cont_->handle_event(VC_EVENT_READ_READY, data);
        } else {
          cb_cont_->handle_event(VC_EVENT_READ_COMPLETE, data);
          terminate_ = true;
        }
        is_need_free_data = false;
      }
    }
  }

  return ret;
}

int ObProxyParallelCont::schedule_timeout()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(NULL != timeout_action_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("timeout action must be NULL", K_(timeout_action), K(ret));
  // no need switch thread
  } else if (OB_ISNULL(timeout_action_ = submit_thread_->schedule_in(this, HRTIME_MSECONDS(timeout_ms_)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to schedule timeout", K_(timeout_action), K(ret));
  }

  return ret;
}

int ObProxyParallelCont::handle_timeout()
{
  int ret = OB_SUCCESS;
  LOG_INFO("timeout to execute parallel task", KP(this));

  timeout_action_ = NULL;
  cancel_all_pending_action();

  if (action_.cancelled_) {
    LOG_INFO("async task has been cancelled", K(ret));
  } else {
    cb_cont_->handle_event(VC_EVENT_ACTIVE_TIMEOUT);
  }

  terminate_ = true;

  return ret;
}

int ObProxyParallelCont::notify_caller_error()
{
  int ret = OB_SUCCESS;

  cancel_timeout_action();
  cancel_all_pending_action();

  if (action_.cancelled_) {
    LOG_INFO("async task has been cancelled", K(ret));
  } else {
    cb_cont_->handle_event(VC_EVENT_ERROR);
  }

  terminate_ = true;

  return ret;
}

void ObProxyParallelCont::cancel_timeout_action()
{
  if (NULL != timeout_action_) {
    timeout_action_->cancel();
    timeout_action_ = NULL;
  }
}

void ObProxyParallelCont::cancel_all_pending_action()
{
  if (NULL != parallel_action_array_) {
    for (int64_t i = 0; i < parallel_task_count_; ++i) {
      if (NULL != parallel_action_array_[i]) {
        parallel_action_array_[i]->cancel();
        parallel_action_array_[i] = NULL;
      }
    }
  }
}

void ObProxyParallelCont::destroy()
{
  LOG_DEBUG("obproxy parallel cont will be destroyed", KP(this));

  cancel_timeout_action();
  cancel_all_pending_action();

  if (NULL != parallel_action_array_ && buf_size_ > 0) {
    op_fixed_mem_free(parallel_action_array_, buf_size_);
  }
  parallel_action_array_ = NULL;
  buf_size_ = 0;

  parallel_task_count_ = 0;
  target_task_count_ = 0;

  cb_cont_ = NULL;
  submit_thread_ = NULL;
  mutex_.release();
  action_.mutex_.release();

  op_free(this);
}

} // end of namespace executor
} // end of namespace obproxy
} // end of namespace oceanbase
