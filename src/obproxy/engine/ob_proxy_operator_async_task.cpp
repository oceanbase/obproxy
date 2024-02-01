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

#include "lib/oblog/ob_log_module.h"
#include "iocore/eventsystem/ob_vconnection.h"
#include "iocore/eventsystem/ob_ethread.h"

#include "ob_proxy_operator_async_task.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace engine
{

ObOperatorAsyncCommonTask::ObOperatorAsyncCommonTask(int64_t target_task_count, ObProxyOperator *ob_operator)
     : obutils::ObAsyncCommonTask(NULL, "operator task"), buf_size_(0), target_task_count_(target_task_count),
       parallel_task_count_(target_task_count), parallel_action_array_(NULL), cont_index_(0), timeout_ms_(0),
       ob_operator_(ob_operator)
{}

int ObOperatorAsyncCommonTask::init_async_task(event::ObContinuation *cont, event::ObEThread *submit_thread)
{
  int ret = common::OB_SUCCESS;
  void *tmp_buf = NULL;
  if (OB_ISNULL(cont)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input", K(ret), KP(this));
  } else {
    /* init the parament of ObAsyncCommonTask */
    buf_size_ = sizeof(event::ObAction) * parallel_task_count_;
    cb_cont_ = cont;
    submit_thread_ = submit_thread;
    mutex_ = cont->mutex_;
    if (parallel_task_count_ > 0) {
      if (OB_ISNULL(tmp_buf = static_cast<char *>(op_fixed_mem_alloc(buf_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to alloc mem", K_(buf_size), K(ret));
      } else if (FALSE_IT(MEMSET(tmp_buf, 0, buf_size_))) {
        // nerver here
      } else if (OB_ISNULL(parallel_action_array_
                    = new (tmp_buf) event::ObAction *[parallel_task_count_])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to init parallel action array", K(parallel_task_count_), K(ret));
      } 
    }
  }
  return ret;
}

void ObOperatorAsyncCommonTask::destroy()
{
  LOG_DEBUG("oboperator async common task will be destroyed", KP(this));

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
  if (NULL != ob_operator_) {
    ob_operator_->set_operator_async_task(NULL);
    ob_operator_ = NULL;
  }
  submit_thread_ = NULL;
  timeout_ms_ = 0;
  cont_index_ = 0;
  mutex_.release();
  action_.mutex_.release();
}

int ObOperatorAsyncCommonTask::main_handler(int event, void *data)
{
  int ret = common::OB_SUCCESS;
  int event_ret = VC_EVENT_CONT;

  if (OB_ISNULL(ob_operator_)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WDIAG("ObOperatorAsyncCommonTask::main_handler invilid operator object", K(ret));
  } else if (OB_UNLIKELY(this_ethread() != mutex_->thread_holding_)) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_EDIAG("this_ethread must be equal with thread_holding", "this_ethread",
              this_ethread(), "thread_holding", mutex_->thread_holding_, K(ret));
  } else if (action_.cancelled_) {
    LOG_INFO("ObOperatorAsyncCommonTask::main_handler action has canceled()", K(event), K(ret),
             KP_(ob_operator), "op_name", ob_operator_->op_name());
    terminate_ = true;
  } else {
    switch (event) {
      case VC_EVENT_READ_COMPLETE: {
        --target_task_count_;
        if (OB_FAIL(ob_operator_->process_complete_data(data))) {
          LOG_WDIAG("fail to handle parallel task complete", K(ret), K(ob_operator_->op_name()));
        } else {
          int64_t cont_index = 0; //TODO need to update pres->get_cont_index();
          if (OB_UNLIKELY(cont_index < 0) || OB_UNLIKELY(cont_index >= parallel_task_count_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("unexpected cont result", K(ob_operator_->op_name()), K(cont_index), K(parallel_task_count_), K(ret));
          } else {
            parallel_action_array_[cont_index] = NULL;
          }
          if (target_task_count_ > 0) {
            event_ret = VC_EVENT_READ_READY;
          } else {
            event_ret = VC_EVENT_READ_COMPLETE;
          }
        }
        break;
      }
      case VC_EVENT_READ_READY:
        if (OB_FAIL(ob_operator_->process_ready_data(data, event_ret))) {
          LOG_WDIAG("fail to handle ready data", K(ret), K(ob_operator_->op_name()));
        }
        break;
      case VC_EVENT_ACTIVE_TIMEOUT: {
        if (OB_FAIL(handle_timeout())) {
          LOG_WDIAG("fail to handle timeout event", K(ret), K(ob_operator_->op_name()));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unexpected event", K(event), K(ret), K(ob_operator_->op_name()));
        break;
      }
    }
  }

  if (!terminate_) {
    if (OB_SUCC(ret)) {
      if (event_ret == VC_EVENT_READ_READY || event_ret == VC_EVENT_READ_COMPLETE) {
        cb_cont_->handle_event(event_ret, ob_operator_->get_operator_result());
      }

      if (event_ret == VC_EVENT_READ_COMPLETE) {
        terminate_ = true;
      }
    } else {
      notify_caller_error();
    }
  }

  if (terminate_) {
    destroy();
    event_ret = EVENT_DONE;
  }

  return event_ret;
}

int ObOperatorAsyncCommonTask::handle_timeout()
{
  int ret = OB_SUCCESS;
  LOG_INFO("timeout to execute parallel task", KP_(ob_operator), "op_name", ob_operator_->op_name(), KP(this));

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

void ObOperatorAsyncCommonTask::cancel_timeout_action()
{
  if (NULL != timeout_action_) {
    timeout_action_->cancel();
    timeout_action_ = NULL;
  }
}

int ObOperatorAsyncCommonTask::notify_caller_error()
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

void ObOperatorAsyncCommonTask::cancel_all_pending_action()
{
  if (NULL != parallel_action_array_) {
    for (int64_t i = 0; i < parallel_task_count_; ++i) {
      if (NULL != parallel_action_array_[i]) {
        parallel_action_array_[i]->cancel();
        parallel_action_array_[i] = NULL;
        LOG_DEBUG("ObProxyOperator::cancel_all_pending_action canceled", K(ob_operator_),
                K(parallel_task_count_), K(i));
      }
    }
  }
}

} /* END:engine */
} /* END:obproxy */
} /* END:oceanbase */
