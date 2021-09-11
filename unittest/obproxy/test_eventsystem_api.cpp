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

#define USING_LOG_PREFIX PROXY_EVENT

#include "test_eventsystem_api.h"
#include "stat/ob_net_stats.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;

namespace oceanbase
{
namespace obproxy
{
bool g_signal_hook_success = true;
TestWaitCond g_wait_cond;
int32_t g_event_start_threads = 2; // >1
char g_output[TEST_STRING_GROUP_COUNT][TEST_MAX_STRING_LENGTH] = {};
char g_input[TEST_STRING_GROUP_COUNT][TEST_MAX_STRING_LENGTH] ={
    "OceanBase was originally designed to solve the problem of ",
    "large-scale data of Taobao in ALIBABA GROUP. "
};

TestFuncParam *create_funcparam_test(bool create_cont,
    ObEventFunc funcp, ObProxyMutex *mutexp)
{
  TestFuncParam *func_param = new(std::nothrow) TestFuncParam();
  if (NULL == func_param) {
    OB_ASSERT(!"failed to allocate TestFuncParam instance");
  }

  TestWaitCond *wait_cond = new(std::nothrow) TestWaitCond();
  if (NULL == wait_cond) {
    OB_ASSERT(!"failed to allocate TestWaitCond instance");
  }
  func_param->wait_cond_ = wait_cond;

  if (create_cont) {
    ObContInternal *contp = ObContInternal::alloc(funcp, mutexp);
    if (NULL == contp) {
      OB_ASSERT(!"failed to allocate ObContInternal instance");
    }
    func_param->cont_ = (ObContinuation *)contp;
  }

  return func_param;
}

bool destroy_funcparam_test(TestFuncParam *param)
{
  bool ret = false;
  if (NULL == param) {
    LOG_WARN("Plugin tries to destroy a TestFuncParam which is  already deleted");
  } else {
    if (NULL != param->cont_) {
      if (NULL != ((ObContInternal *)(param->cont_))->event_func_) {
        ((ObContInternal *)(param->cont_))->destroy();
      } else {
        delete param->cont_;
      }
    } else if (NULL != param->ptr_mutex_) {
      param->ptr_mutex_.release();
    } else if (NULL != param->processor_) {
      delete param->processor_;
    } else if (NULL != param->action_) {
      delete param->action_;
    } else if (NULL != param->test_event_) {
      param->test_event_->free();
    } else if (NULL != param->thread_) {
      delete param->thread_;
    }

    pthread_mutex_destroy(&param->wait_cond_->mutex_);
    pthread_cond_destroy(&param->wait_cond_->cond_);
    delete param->wait_cond_;

    ret = param->test_ok_;
    delete param;
    param = NULL;
  }
  return ret;
}

void wait_condition(TestWaitCond *wait_cond)
{
  pthread_mutex_lock(&wait_cond->mutex_);
  while (wait_cond->condition_is_false_) { //prevent spurious wakeup in multi-core processors
    pthread_cond_wait(&wait_cond->cond_, &wait_cond->mutex_);
  }
  pthread_mutex_unlock(&wait_cond->mutex_);
}

void signal_condition(TestWaitCond *wait_cond)
{
  wait_cond->condition_is_false_ = false;
  pthread_mutex_lock(&wait_cond->mutex_);
  pthread_cond_signal(&wait_cond->cond_);
  pthread_mutex_unlock(&wait_cond->mutex_);
}

int handle_hook_test(ObEThread &ethread)
{
  UNUSED(ethread);
  g_signal_hook_success = true;
  return 0;
}

void print_process(const unsigned int seq_no, const ObHRTime cur_time,
                   const ObHRTime last_time)
{
  ObHRTime tmp_diff = cur_time - last_time;
  printf("seq_no = %d\n", seq_no);
  (1 == seq_no) ? printf("init time = ") : printf("last time = ");
  printf("%lds.%ldms.%ldus.%ldns\n",
         hrtime_to_sec(last_time),
         hrtime_to_msec(last_time % HRTIME_SECOND),
         hrtime_to_usec(last_time % HRTIME_MSECOND),
         hrtime_to_nsec(last_time % HRTIME_USECOND));
  printf("cur  time = %lds.%ldms.%ldus.%ldns\n",
         hrtime_to_sec(cur_time),
         hrtime_to_msec(cur_time % HRTIME_SECOND),
         hrtime_to_usec(cur_time % HRTIME_MSECOND),
         hrtime_to_nsec(cur_time % HRTIME_USECOND));
  printf("diff time = %lds.%ldms.%ldus.%ldns\n",
         hrtime_to_sec(tmp_diff),
         hrtime_to_msec(tmp_diff % HRTIME_SECOND),
         hrtime_to_usec(tmp_diff % HRTIME_MSECOND),
         hrtime_to_nsec(tmp_diff % HRTIME_USECOND));
}

void check_schedule_result(TestFuncParam *param, SCHEDULE_X check_imm,
                           SCHEDULE_X check_at, SCHEDULE_X check_in, SCHEDULE_X check_every)
{
  if (EVENT_IMMEDIATE == param->event_->callback_event_) {
    check_imm(param);
  } else if (EVENT_INTERVAL == param->event_->callback_event_
             && 0 != param->event_->period_) {
    check_every(param);
  } else if (EVENT_INTERVAL == param->event_->callback_event_
             && 0 == param->event_->period_
             && param->atimeout_ == param->event_->timeout_at_) {
    check_at(param);
  } else if (EVENT_INTERVAL == param->event_->callback_event_
             && 0 == param->event_->period_
             && param->atimeout_ != param->event_->timeout_at_) {
    check_in(param);
  } else {
    LOG_ERROR("check_schedule: unknown schedule_x");
  }
}

int check_imm_test_ok(ObHRTime diff_time, ObHRTime sleep_time)
{
  bool test_ok = false;

  if (diff_time < 0) {
    diff_time = 0 - diff_time;
  }
  if (diff_time <= (TEST_THRESHOLD_MSECONDS + sleep_time)) {
    test_ok = true;
  } else {
    test_ok = false;
  }
  return test_ok;
}

int check_test_ok(TestFuncParam *param, const ObHRTime timeout)
{
  ObHRTime diff_time = hrtime_diff(param->cur_time_, param->last_time_);
  if (diff_time < 0)
  {
    diff_time = 0 - diff_time;
  }
  if (timeout <= 0)
  {
    param->test_ok_ = check_imm_test_ok(diff_time);
  } else {
    if (diff_time <= (timeout + TEST_THRESHOLD_MSECONDS)
    && diff_time >= (timeout - TEST_THRESHOLD_MSECONDS))
    {
      param->test_ok_ = true;
    } else {
      param->test_ok_ = false;
    }
  }
  return param->test_ok_;
}

void *thread_g_net_processor_start(void *data)
{
  UNUSED(data);
  int ret = OB_SUCCESS;
  net::ObNetOptions net_options;
  net_options.max_connections_ = 8192;
  net_options.default_inactivity_timeout_ = 180000;
  net_options.max_client_connections_ = 0;
  if (OB_FAIL(init_event_system(EVENT_SYSTEM_MODULE_VERSION))) {
    ERROR_NET("failed to init event_system, ret=%d", ret);
  } else if (OB_FAIL(init_mysql_stats())) {
    ERROR_NET("failed to init mysql_stats, ret=%d", ret);
  } else if (OB_FAIL(init_net(NET_SYSTEM_MODULE_VERSION, net_options))) {
    ERROR_NET("failed to init net, ret=%d", ret);
  } else if (OB_FAIL(init_net_stats())) {
    ERROR_NET("failed to init net stats, ret=%d", ret);
  } else if (OB_FAIL(g_event_processor.start(g_event_start_threads, DEFAULT_STACKSIZE, false, false))) {
    ERROR_NET("failed to START g_event_processor, ret=%d", ret);
  } else if (OB_FAIL(g_net_processor.start())) {
    ERROR_NET("failed to START g_net_processor, ret=%d", ret);
  } else {}

  if (OB_SUCC(ret)) {
    INFO_NET("TEST", "start g_net_processor success");
    signal_condition(&g_wait_cond);
    this_ethread()->execute();
  } else {
    ERROR_NET("do exit, ret=%d", ret);
    exit(1);
  }
  return NULL;
}

void init_g_net_processor()
{
  ObThreadId tid;
  tid = thread_create(thread_g_net_processor_start, NULL, true, 0);
  if (tid <= 0) {
    ERROR_NET("failed to create thread_g_net_processor_start");
  } else {
    wait_condition(&g_wait_cond);
    sleep(1);// wait for execute() start, simple way
    INFO_NET("TEST", "start thread_g_net_processor_start success");
  }
}

bool test_expect(const int32_t type, const int64_t a,
    const char astr[], const int64_t b, const char bstr[])
{
  bool ret = true;
  switch (type) {
    case TEST_EXPECT_TYPE_EQ: {
      if(OB_UNLIKELY(!(a == b))) {
        ret = false;
      }
      break;
    }
    case TEST_EXPECT_TYPE_LE: {
      if(OB_UNLIKELY(!(a <= b))) {
        ret = false;
      }
      break;
    }
    case TEST_EXPECT_TYPE_GE: {
      if(OB_UNLIKELY(!(a >= b))) {
        ret = false;
      }
      break;
    }
    case TEST_EXPECT_TYPE_TRUE: {
      if(OB_UNLIKELY(!(a))) {
        ret = false;
      }
      break;
    }
    default:
      ob_release_assert(!"unknown event");
  }
  if (!ret) {
    if (TEST_EXPECT_TYPE_TRUE != type) {
      ERROR_NET("fail, %s:%s %ld:%ld", astr, bstr, a, b);
    } else {
      ERROR_NET("fail, %s %ld", astr, a);
    }
    BACKTRACE(ERROR, 1, "fail");
  }

  return ret;
}

} // end of namespace obproxy
} // end of namespace oceanbase

