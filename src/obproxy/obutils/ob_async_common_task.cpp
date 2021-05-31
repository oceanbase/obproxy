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

#include "obutils/ob_async_common_task.h"
#include "lib/profile/ob_trace_id.h"
#include "proxy/client/ob_client_vc.h"
#include "utils/ob_proxy_hot_upgrader.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::proxy;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
static const int EVENT_COMPLETE = CLIENT_TRANSPORT_MYSQL_RESP_EVENT;
static const int EVENT_INFORM_OUT = ASYNC_PROCESS_INFORM_OUT_EVENT;

int ObAsyncCommonTask::main_handler(int event, void *data)
{
  int ret = OB_SUCCESS;
  switch (event) {
    case EVENT_INTERVAL: {
      timeout_action_ = NULL;
      if (OB_FAIL(handle_timeout())) {
        LOG_WARN("fail to handle timeout");
      }
      break;
    }
    case EVENT_IMMEDIATE: {
      pending_action_ = NULL;
      if (OB_FAIL(cancel_timeout_action())) {
        LOG_WARN("fail to cancel_timeout_action", K(ret));
      } else if (OB_FAIL(handle_event_start())) {
        LOG_INFO("fail to handle event start", K(ret));
      }
      break;
    }
    case ASYNC_PROCESS_DONE_EVENT:
    case EVENT_COMPLETE: {
      pending_action_ = NULL;
      if (OB_FAIL(cancel_timeout_action())) {
        LOG_WARN("fail to cancel_timeout_action", K(ret));
      } else if (OB_FAIL(handle_event_complete(data))) {
        LOG_WARN("fail to handle event complete", K(ret));
      }
      break;
    }
    case EVENT_INFORM_OUT: {
      pending_action_ = NULL;
      if (OB_FAIL(cancel_timeout_action())) {
        LOG_WARN("fail to cancel_timeout_action", K(ret));
      } else if (OB_FAIL(handle_event_inform_out())) {
        LOG_WARN("fail to handle inform out event", K(ret));
      }
      break;
    }
    case ASYNC_PROCESS_START_REPEAT_TASK_EVENT: {
      if (OB_FAIL(schedule_repeat_task())) {
        LOG_WARN("fail to schedule repeat task", K(ret));
      }
      break;
    }
    case ASYNC_PROCESS_DO_REPEAT_TASK_EVENT: {
      if (OB_FAIL(handle_repeat_task())) {
        LOG_WARN("fail to handle repeat task", K(ret));
      }
      break;
    }
    case ASYNC_PROCESS_SET_INTERVAL_EVENT: {
      if (OB_ISNULL(update_interval_func_)) {
        // no need to update interval
      } else if (OB_FAIL(cancel_pending_action())) {
        LOG_WARN("fail to cancel pending action", K(ret));
      } else if (FALSE_IT(update_interval_func_())) {
        // impossible
      } else if (OB_FAIL(schedule_repeat_task())) {
        LOG_WARN("fail to schedule repeat task", K(ret));
      }
      break;
    }
    case ASYNC_PROCESS_DESTROY_SELF_EVENT: {
      terminate_ = true;
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error state, nerver run here", K(event), K(ret));
      break;
    }
  }

  if (!terminate_ && need_callback_) {
    if (OB_FAIL(handle_callback())) {
      LOG_WARN("fail to handle callback", K(ret));
    }
  }

  if (terminate_) {
    destroy();
  }

  return EVENT_DONE;
}

inline int ObAsyncCommonTask::handle_event_start()
{
  int ret = OB_SUCCESS;
  if (action_.cancelled_) {
    terminate_ = true;
    LOG_INFO("async task has been cancelled, will kill itself", K(ret));
  } else if (OB_FAIL(init_task())) {
    LOG_INFO("fail to init async task", "task_name", task_name_, K(ret));
    if (NULL != cb_cont_) {
      need_callback_ = true;
    } else {
      terminate_ = true;
    }
  }
  return ret;
}

inline int ObAsyncCommonTask::handle_event_complete(void *data)
{
  int ret = OB_SUCCESS;
  if (action_.cancelled_) {
    terminate_ = true;
    LOG_INFO("async task has been cancelled, will kill itself", K(ret));
  } else if (NULL != cb_cont_) {
    need_callback_ = true;
  } else {
    terminate_ = true;
  }

  if (OB_FAIL(finish_task(data))) {
    LOG_WARN("fail to do finish task", "task_name", task_name_,K(ret));
  }

  return ret;
}

int ObAsyncCommonTask::handle_timeout()
{
  int ret = OB_SUCCESS;
  LOG_INFO("async task timeout", "task_name", task_name_);
  if (NULL != cb_cont_) {
    need_callback_ = true;
  } else {
    terminate_ = true;
  }
  return ret;
}


int ObAsyncCommonTask::handle_callback()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cb_cont_) || OB_ISNULL(submit_thread_)) {
    // no need to callback, will destroy itself
    terminate_ = true;
  } else if (&self_ethread() == submit_thread_) {
    // the same thread, inform out directly
    if (OB_FAIL(handle_event_inform_out())) {
      LOG_WARN("fail to handle inform out event", K(ret));
    }
  } else if (OB_ISNULL(submit_thread_->schedule_imm(this, EVENT_INFORM_OUT))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to schedule back to main handler, will destroy itself", K(ret));
  }
  if (OB_FAIL(ret)) {
    terminate_ = true;
  }
  return ret;
}

int ObAsyncCommonTask::handle_event_inform_out()
{
  int ret = OB_SUCCESS;
  need_callback_ = false;
  terminate_ = true;

  if (&self_ethread() != submit_thread_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("this thread must be equal with submit_thread",
              "this ethread", &self_ethread(), K_(submit_thread), K(ret));
  } else if (OB_ISNULL(cb_cont_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cb_cont can not be null here", K_(cb_cont), K(ret));
  } else {
    // 1. should acquire action_.mutex_
    //   1.1. if mutex == action_.mutex_, also can lock success
    MUTEX_TRY_LOCK(lock, action_.mutex_, this_ethread());
    if (OB_LIKELY(lock.is_locked())) {
      if (action_.cancelled_) {
        LOG_INFO("async task has been cancelled", K(ret));
      } else {
        if (mutex_ != action_.mutex_) {
          ObCurTraceId::set((uint64_t)(action_.mutex_.ptr_));
        }
        cb_cont_->handle_event(ASYNC_PROCESS_DONE_EVENT, get_callback_data());
      }
    } else if (OB_ISNULL(submit_thread_->schedule_imm(this, EVENT_INFORM_OUT))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to do schedule imm", K_(submit_thread), K(ret));
    } else {
      terminate_ = false;
    }
  }

  return ret;
}

void ObAsyncCommonTask::destroy()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("async task will be destroyed", KPC(this));
  if (OB_FAIL(cancel_timeout_action())) {
    LOG_WARN("fail to cancel timeout action", K(ret));
  }
  if (OB_FAIL(cancel_pending_action())) {
    LOG_WARN("fail to cancel pending action", K(ret));
  }
  cb_cont_ = NULL;
  submit_thread_ = NULL;
  mutex_.release();
  action_.mutex_.release();
  delete this;
}

DEF_TO_STRING(ObAsyncCommonTask)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this), K_(task_name), K_(terminate), K_(need_callback), K_(is_repeat), K_(is_stop),
       K_(interval_us), KP_(timeout_action), KP_(pending_action),
       KP_(cb_cont), KPC_(submit_thread));
  J_OBJ_END();
  return pos;
}

inline int ObAsyncCommonTask::schedule_repeat_task()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL != pending_action_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pending_action should be null here", K_(pending_action), K(ret));
  } else if (OB_LIKELY(!is_stop_)) {
    if (OB_LIKELY(interval_us_ > 0)) {
      if (is_repeat_) {
        if (OB_ISNULL(pending_action_ = self_ethread().schedule_every(this,
                      HRTIME_USECONDS(interval_us_), ASYNC_PROCESS_DO_REPEAT_TASK_EVENT))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to schedule repeat task", KPC(this), K(ret));
        }
      } else if (OB_ISNULL(pending_action_ = self_ethread().schedule_in(this,
                    HRTIME_USECONDS(interval_us_), ASYNC_PROCESS_DO_REPEAT_TASK_EVENT))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to schedule repeat task", KPC(this), K(ret));
      }
    }
  }
  return ret;
}

inline int ObAsyncCommonTask::handle_repeat_task()
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ::oceanbase::common::ObTimeUtility::current_time();
  if (OB_LIKELY(NULL != process_func_) && OB_FAIL(process_func_())) {
    LOG_WARN("fail to do repeat task", KPC(this), K(ret));
  }

  check_stop_repeat_task();

  if (!is_repeat_) {
    pending_action_ = NULL;
    if (OB_FAIL(schedule_repeat_task())) {
      LOG_WARN("fail to schedule repeat task", KPC(this), K(ret));
    }
  }
  const int64_t end_time = ::oceanbase::common::ObTimeUtility::current_time();
  if (end_time - start_time > 1000 * 1000) {
    LOG_WARN("repeat task cost too much time", "task name", task_name_,
             K(start_time), K(end_time), KPC(this),
             "cost time(us)", end_time - start_time);
  }

  return ret;
}

inline void ObAsyncCommonTask::check_stop_repeat_task()
{
  const ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
  if (OB_UNLIKELY(!info.need_conn_accept_)) {
    // no need do repeat task again
    is_stop_ = true;; // for schedule_in task
    int ret = OB_SUCCESS;
    if (OB_FAIL(cancel_pending_action())) { // for schedule_every task
      LOG_WARN("fail to cancel pending action", K(ret));
    }
  }
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
