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

#ifndef OBPROXY_ASYNC_COMMON_TASK_H
#define OBPROXY_ASYNC_COMMON_TASK_H
#include "iocore/eventsystem/ob_action.h"
#include "iocore/eventsystem/ob_ethread.h"
#include "iocore/eventsystem/ob_event_system.h"
#include "iocore/eventsystem/ob_blocking_task.h"
#include "stat/ob_resource_pool_stats.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
#define ASYNC_PROCESS_INVALID_EVENT (EVENT_ASYNC_PROCESS_START + 0)
#define ASYNC_PROCESS_DONE_EVENT (EVENT_ASYNC_PROCESS_START + 1)
#define ASYNC_PROCESS_INFORM_OUT_EVENT (EVENT_ASYNC_PROCESS_START + 2)
#define ASYNC_PROCESS_DO_REPEAT_TASK_EVENT (EVENT_ASYNC_PROCESS_START + 3)
#define ASYNC_PROCESS_START_REPEAT_TASK_EVENT (EVENT_ASYNC_PROCESS_START + 4)
#define ASYNC_PROCESS_SET_INTERVAL_EVENT (EVENT_ASYNC_PROCESS_START + 5)
#define ASYNC_PROCESS_DESTROY_SELF_EVENT (EVENT_ASYNC_PROCESS_START + 6)

// callback method for anync task data
typedef int (event::ObContinuation::*process_async_task_pfn) (void *data);
typedef int (*ProcessFunc) ();
typedef void (*UpdateIntervalFunc) ();
class ObAsyncCommonTask : public event::ObContinuation
{
public:
  ObAsyncCommonTask(event::ObProxyMutex *m, const char *name, event::ObContinuation *cb_cont = NULL,
                    event::ObEThread *submit_thread = NULL, event::ObEventThreadType etype = event::ET_BLOCKING)
    : ObContinuation(m), terminate_(false), need_callback_(false), is_repeat_(false), is_stop_(false), interval_us_(-1),
      etype_(etype), task_name_(name), timeout_action_(NULL), pending_action_(NULL), cb_cont_(cb_cont),
      submit_thread_(submit_thread), process_func_(NULL), update_interval_func_(NULL), action_()
  {
    action_.set_continuation(cb_cont);
    ASYNC_COMMON_TASK_INCREMENT_DYN_STAT(CREATE_ASYNC_COMMON_TASK_COUNT);
    SET_HANDLER(&ObAsyncCommonTask::main_handler);
  }

  ObAsyncCommonTask(event::ObProxyMutex *m, const char *name, ProcessFunc process_func, UpdateIntervalFunc update_func,
                    const bool is_repeat = false, event::ObEventThreadType etype = event::ET_BLOCKING)
    : ObContinuation(m), terminate_(false), need_callback_(false), is_repeat_(is_repeat), is_stop_(false), interval_us_(-1),
      etype_(etype), task_name_(name), timeout_action_(NULL), pending_action_(NULL),
      cb_cont_(NULL), submit_thread_(NULL), process_func_(process_func),
      update_interval_func_(update_func), action_()
  {
    SET_HANDLER(&ObAsyncCommonTask::main_handler);
  }
  virtual ~ObAsyncCommonTask()
  {
    ASYNC_COMMON_TASK_INCREMENT_DYN_STAT(DESTROY_ASYNC_COMMON_TASK_COUNT);
  }

  virtual int main_handler(int event, void *data);

  event::ObAction &get_action() { return action_; }
  int cancel_pending_action()
  {
    int ret = common::OB_SUCCESS;
    if (NULL != pending_action_) {
      if (OB_FAIL(pending_action_->cancel())) {
        PROXY_LOG(WARN, "fail to cancel pending action", K_(pending_action), K(ret));
      } else {
        pending_action_ = NULL;
      }
    }
    return ret;
  }
  int cancel_timeout_action()
  {
    int ret = common::OB_SUCCESS;
    if (NULL != timeout_action_) {
      if (OB_FAIL(timeout_action_->cancel())) {
        PROXY_LOG(WARN, "fail to cancel timeout action", K_(timeout_action), K(ret));
      } else {
        timeout_action_ = NULL;
      }
    }
    return ret;
  }
  virtual void destroy();

  virtual int init_task() { return common::OB_NOT_IMPLEMENT; }
  virtual int finish_task(void *data) { UNUSED(data); return common::OB_NOT_IMPLEMENT; }
  virtual void *get_callback_data() { return NULL; }
  virtual int schedule_timeout() { return common::OB_SUCCESS; }
  virtual int handle_timeout();

  int handle_event_start();
  int handle_event_complete(void *data);
  int handle_callback();
  int handle_event_inform_out();
  int handle_repeat_task();
  int schedule_repeat_task();
  void check_stop_repeat_task();

  void set_interval(const int64_t interval_us) { interval_us_ = interval_us; }
  void set_task_name(const char *name) { task_name_ = name; }
  void set_terminate() { terminate_ = true; }

  static int destroy_repeat_task(ObAsyncCommonTask *&cont);
  static int update_task_interval(ObAsyncCommonTask *cont);
  static ObAsyncCommonTask *create_and_start_repeat_task(const int64_t interval_us, const char *task_name,
                                                         ProcessFunc process_func, UpdateIntervalFunc update_func,
                                                         const bool is_repeat = false,
                                                         event::ObEventThreadType etype = event::ET_BLOCKING);
  virtual DECLARE_TO_STRING;

protected:
  bool terminate_;
  bool need_callback_;
  bool is_repeat_;
  bool is_stop_;
  int64_t interval_us_;
  event::ObEventThreadType etype_;
  const char *task_name_;
  event::ObAction *timeout_action_;
  event::ObAction *pending_action_;
  event::ObContinuation *cb_cont_;
  event::ObEThread *submit_thread_;
  ProcessFunc process_func_;
  UpdateIntervalFunc update_interval_func_;
  event::ObAction action_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAsyncCommonTask);
};

inline int ObAsyncCommonTask::destroy_repeat_task(ObAsyncCommonTask *&cont)
{
  int ret = common::OB_SUCCESS;
  if (OB_LIKELY(NULL != cont)) {
    if (OB_ISNULL(event::g_event_processor.schedule_imm(cont, cont->etype_,
                  ASYNC_PROCESS_DESTROY_SELF_EVENT))) {
      ret = common::OB_ERR_UNEXPECTED;
      PROXY_LOG(WARN, "fail to schedule destroy repeat task cont", K(ret));
    } else {
      cont = NULL;
    }
  }
  return ret;
}

inline int ObAsyncCommonTask::update_task_interval(ObAsyncCommonTask *cont)
{
  int ret = common::OB_SUCCESS;
  if (OB_LIKELY(NULL != cont)) {
    if (OB_ISNULL(event::g_event_processor.schedule_imm(cont, cont->etype_,
                  ASYNC_PROCESS_SET_INTERVAL_EVENT))) {
      ret = common::OB_ERR_UNEXPECTED;
      PROXY_LOG(WARN, "fail to schedule set interval event", K(cont));
    }
  }
  return ret;
}

inline ObAsyncCommonTask *ObAsyncCommonTask::create_and_start_repeat_task(
                                             const int64_t interval_us,
                                             const char *task_name,
                                             ProcessFunc process_func,
                                             UpdateIntervalFunc update_func,
                                             const bool is_repeat /*false*/,
                                             event::ObEventThreadType etype /* ET_BLOCKING */)
{
  ObAsyncCommonTask *cont = NULL;
  int ret = common::OB_SUCCESS;
  event::ObProxyMutex *mutex = NULL;
  if (OB_ISNULL(process_func)) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_LOG(WARN, "process func is null", K(ret));
  } else if (OB_ISNULL(mutex = event::new_proxy_mutex())) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    PROXY_LOG(ERROR, "fail to alloc memory for mutex", K(ret));
  } else if (OB_ISNULL(cont = new(std::nothrow) ObAsyncCommonTask(mutex, task_name, process_func, update_func, is_repeat, etype))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    PROXY_LOG(ERROR, "fail to alloc memory for mutex", K(ret));
    if (OB_LIKELY(NULL != mutex)) {
      mutex->free();
      mutex = NULL;
    }
  } else {
    cont->set_interval(interval_us);
    if (is_repeat) {
      if (OB_ISNULL(event::g_event_processor.schedule_imm(cont, etype,
                    ASYNC_PROCESS_START_REPEAT_TASK_EVENT))) {
        ret = common::OB_ERR_UNEXPECTED;
        PROXY_LOG(WARN, "fail to schedule repeat task", K(interval_us), K(ret));
      }
    } else if (OB_ISNULL(event::g_event_processor.schedule_in(cont, HRTIME_USECONDS(interval_us),
               etype, ASYNC_PROCESS_DO_REPEAT_TASK_EVENT))) {
      ret = common::OB_ERR_UNEXPECTED;
      PROXY_LOG(WARN, "fail to schedule repeat task", K(interval_us), K(ret));
    }
  }

  if (OB_FAIL(ret) && OB_LIKELY(NULL != cont)) {
    cont->destroy();
    cont = NULL;
  }
  return cont;
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_ASYNC_COMMON_TASK_H */
