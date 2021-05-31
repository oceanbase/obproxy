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

#define private public
#define protected public
#include <gtest/gtest.h>
#include <pthread.h>
#include "test_eventsystem_api.h"
#include "obproxy/iocore/eventsystem/ob_ethread.h"

namespace oceanbase
{
namespace obproxy
{
using namespace proxy;
using namespace common;
using namespace event;

#define TEST_LOCK_SLEEP_TIME            HRTIME_SECONDS(1)
#define MAX_PARAM_ARRAY_SIZE            7

#define SCHEDULE_INIT_COMMON(callback_event)                                            \
{                                                                                       \
  param->started_ = true;                                                               \
  param->last_time_ = get_hrtime_internal();                                      \
  if (param->common_ethread_) {                                                         \
    int64_t cur_thread = (test_last_thread + 1) % g_event_processor.thread_count_for_type_[ET_CALL];\
    param->ethread_ = g_event_processor.event_thread_[ET_CALL][cur_thread];              \
  } else {                                                                              \
    test_last_thread = g_event_processor.next_thread_for_type_[ET_CALL];                \
    param->ethread_ = g_event_processor.assign_thread(ET_CALL);                         \
  }                                                                                     \
  ASSERT_TRUE(NULL != param->ethread_);                                                 \
  param->callback_event_ = callback_event;                                              \
  param->cookie_ = param;                                                               \
}

enum TestEThreadFuncType {
  TEST_NULL = 0,
  TEST_SCHEDULE_IMM = 1,
  TEST_SCHEDULE_IMM_SIGNAL,
  TEST_SCHEDULE_AT,
  TEST_SCHEDULE_IN ,
  TEST_SCHEDULE_EVERY,
  TEST_SCHEDULE,
  TEST_SCHEDULE_IMM_LOCAL,
  TEST_SCHEDULE_AT_LOCAL,
  TEST_SCHEDULE_IN_LOCAL ,
  TEST_SCHEDULE_EVERY_LOCAL,
  TEST_SCHEDULE_LOCAL,
  TEST_OBETHREAD_NULL,
  TEST_OBETHREAD_REGULAR,
  TEST_OBETHREAD_DEDICATED,
  TEST_INIT,
  TEST_THREAD_SPAWN_THREAD_INTERNAL,
};

int32_t test_last_thread = 0;

class TestEThread : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown();
  static void check_thread_spawn_thread_internal(TestFuncParam *param);
  static void check_obethread_common(ObEThread *ethread);
  static void check_obethread_null(TestFuncParam *param);
  static void check_obethread_regular(TestFuncParam *param);
  static void check_obethread_dedicated(TestFuncParam *param);
  static void check_init(TestFuncParam *param);
  static void check_schedule_common(TestFuncParam *param);
  static void check_schedule_imm(TestFuncParam *param);
  static void check_schedule_imm_signal(TestFuncParam *param);
  static void check_schedule_at(TestFuncParam *param);
  static void check_schedule_in(TestFuncParam *param);
  static void check_schedule_every(TestFuncParam *param);
  static void check_schedule(TestFuncParam *param);
  static void check_schedule_imm_local(TestFuncParam *param);
  static void check_schedule_at_local(TestFuncParam *param);
  static void check_schedule_in_local(TestFuncParam *param);
  static void check_schedule_every_local(TestFuncParam *param);
  static void check_schedule_local(TestFuncParam *param);
public:
  TestFuncParam *test_param[MAX_PARAM_ARRAY_SIZE];
};

void *thread_spawn_thread_internal(void *data);
void start_obethred_common(ObEThread *ethread, int64_t event_thread_count, TestFuncParam *param);

void TestEThread::SetUp()
{
  for (int i = 0; i < MAX_PARAM_ARRAY_SIZE; ++i) {
    test_param[i] = NULL;
  }
}

void TestEThread::TearDown()
{
  for (int i = 0; i < MAX_PARAM_ARRAY_SIZE; ++i) {
    if (NULL != test_param[i]) {
      destroy_funcparam_test(test_param[i]);
    } else {}
  }
}

void TestEThread::check_thread_spawn_thread_internal(TestFuncParam *param)
{
  ObThread *thread = NULL;
  char thr_name[MAX_THREAD_NAME_LENGTH];
  if (NULL == (thread = new(std::nothrow) ObThread())) {
    LOG_ERROR("failed to create ObThread");
    param->test_ok_ = false;
  } else {
    param->thread_ = thread;
    ASSERT_TRUE(0 == thread->tid_);
    ASSERT_TRUE(NULL == thread->mutex_);
    ASSERT_TRUE(NULL == thread->mutex_ptr_);
    thread->execute();//null

    snprintf(thr_name, MAX_THREAD_NAME_LENGTH, "TEST_thread_func");
    if (OB_SUCCESS != thread->start(thr_name, DEFAULT_STACKSIZE,
        (ThreadFunction)thread_spawn_thread_internal, (void *)param)) {
      LOG_ERROR("failed to start event thread");
    } else {
      wait_condition(param->wait_cond_);
    }
    thread_join(thread->tid_);
    param->thread_ = NULL;
  }
}

inline void TestEThread::check_obethread_common(ObEThread *ethread)
{
  ASSERT_TRUE(NULL ==  ethread->ethreads_to_be_signalled_);
  ASSERT_TRUE(0 ==  ethread->ethreads_to_be_signalled_count_);
  ASSERT_TRUE(0 ==  ethread->event_types_);
  ASSERT_TRUE(NULL ==  ethread->signal_hook_);
  ASSERT_TRUE(NULL ==  ethread->ep_);
  for (int i = 0; i < ObEThread::MAX_THREAD_DATA_SIZE; ++i) {
    ASSERT_TRUE(0 ==  ethread->thread_private_[i]);
  }
}

void TestEThread::check_obethread_null(TestFuncParam *param)
{
  if (!param->started_) {
    param->started_ = true;
    param->func_type_ = TEST_OBETHREAD_NULL;
    if (OB_ISNULL(param->ethread_ = new(std::nothrow) ObEThread())) {
      LOG_ERROR("failed to allocator memory for event thread, null");
      param->test_ok_ = false;
      ((ObContInternal *)(param->cont_))->deletable_ = true;
      signal_condition(param->wait_cond_);
    } else {
      check_obethread_common(param->ethread_);
      ASSERT_TRUE(REGULAR ==  param->ethread_->tt_);
      ASSERT_TRUE(ObEThread::NO_ETHREAD_ID ==  param->ethread_->id_);
      ASSERT_TRUE(NULL ==  param->ethread_->pending_event_);
      param->ethread_->id_ = g_event_processor.event_thread_count_;
      start_obethred_common(param->ethread_, g_event_processor.event_thread_count_, param);
    }
  } else {
    check_schedule_result(param, check_schedule_imm, check_schedule_at,
            check_schedule_in, check_schedule_every);
  }
}

void TestEThread::check_obethread_regular(TestFuncParam *param)
{
  if (!param->started_) {
    param->started_ = true;
    param->func_type_ = TEST_OBETHREAD_REGULAR;
    if (NULL == (param->ethread_ = new(std::nothrow) ObEThread(REGULAR,
        g_event_processor.event_thread_count_))) {
      LOG_ERROR("failed to allocator memory for event thread, REGULAR");
      param->test_ok_ = false;
      ((ObContInternal *)(param->cont_))->deletable_ = true;
      signal_condition(param->wait_cond_);
    } else {
      check_obethread_common(param->ethread_);
      ASSERT_TRUE(REGULAR ==  param->ethread_->tt_);
      ASSERT_TRUE(g_event_processor.event_thread_count_ ==  param->ethread_->id_);
      ASSERT_TRUE(NULL ==  param->ethread_->pending_event_);
      start_obethred_common(param->ethread_, g_event_processor.event_thread_count_, param);
    }
  } else {
    check_schedule_result(param, check_schedule_imm, check_schedule_at,
        check_schedule_in, check_schedule_every);
  }
}

void TestEThread::check_obethread_dedicated(TestFuncParam *param)
{
  if (!param->started_) {
    param->started_ = true;
    param->func_type_ = TEST_OBETHREAD_DEDICATED;
    int ret = OB_SUCCESS;
    if (NULL == (param->event_ = op_reclaim_alloc(ObEvent))) {
      LOG_ERROR("failed to allocate memory for event");
      ret = OB_ERROR;
    } else if (OB_FAIL(param->event_->init(*param->cont_, 0, 0))){
      LOG_ERROR("failed to init for event");
      param->event_->free();
    } else if (NULL == (param->ethread_ = new(std::nothrow) ObEThread(DEDICATED,
        param->event_))) {
      LOG_ERROR("failed to allocator memory for event thread, DEDICATED");
      param->event_->free();
      ret = OB_ERROR;
    } else if (OB_FAIL(param->ethread_->init())) {
      LOG_ERROR("failed to allocator memory for event thread, DEDICATED");
      param->event_->free();
      delete param->ethread_;
      param->ethread_ = NULL;
    } else {
      char thr_name[MAX_THREAD_NAME_LENGTH];
      ObEThread *ethread = param->ethread_;
      int64_t &dedicate_thread_count = g_event_processor.dedicate_thread_count_;
      // add to the g_event_processor.all_event_threads_
      g_event_processor.all_dedicate_threads_[dedicate_thread_count] = ethread;

      check_obethread_common(ethread);
      ASSERT_TRUE(DEDICATED ==  ethread->tt_);
      ASSERT_TRUE(ObEThread::NO_ETHREAD_ID ==  ethread->id_);
      ASSERT_TRUE(param->event_ ==  ethread->pending_event_);

      ethread->set_event_thread_type((ObEventThreadType)ET_CALL);
      param->callback_event_ = EVENT_IMMEDIATE;
      param->cookie_ = param;
      param->event_->callback_event_ = EVENT_IMMEDIATE;
      param->event_->cookie_ = param;
      ((ObContInternal *)param->cont_)->event_count_++;
      param->last_time_ = get_hrtime_internal();
      snprintf(thr_name, MAX_THREAD_NAME_LENGTH, "[ET_NET %ld]", dedicate_thread_count);
      if (OB_SUCCESS != (ret = ethread->start(thr_name, DEFAULT_STACKSIZE))) {
        LOG_ERROR("failed to start event thread", K(dedicate_thread_count));
        ret = OB_ERROR;
      } else {
        param->event_->ethread_ = ethread;
        param->event_->continuation_->mutex_ = ethread->mutex_;
        param->event_->mutex_ = param->event_->continuation_->mutex_;
        g_event_processor.dedicate_thread_count_++;
      }
    }

    if (OB_FAIL(ret)) {
      param->test_ok_ = false;
      ((ObContInternal *)(param->cont_))->deletable_ = true;
      signal_condition(param->wait_cond_);
    }
  } else {
    check_schedule_imm(param);
  }
}

void TestEThread::check_init(TestFuncParam *param)
{
  int ret = OB_SUCCESS;
  ObEThread *ethread = NULL;

  param->func_type_ = TEST_INIT;
  if (NULL == (param->ethread_ = new(std::nothrow) ObEThread(
      REGULAR, g_event_processor.event_thread_count_))) {
    LOG_ERROR("failed to allocator memory for event thread, REGULAR");
    param->test_ok_ = false;
  } else {
    ethread = param->ethread_;
    if (OB_SUCCESS != (ret = ethread->init())) {
      LOG_ERROR("failed to init event thread", K(ret));
      param->test_ok_ = false;
      delete param->ethread_;
      param->ethread_ = NULL;
    } else {
      for (int64_t i = 0; i < MAX_EVENT_THREADS; i++) {
        ASSERT_TRUE(NULL == ethread->ethreads_to_be_signalled_[i]);
      }
#if OB_HAVE_EVENTFD
      ASSERT_TRUE(ethread->evfd_ >= 0);
      ASSERT_TRUE(FD_CLOEXEC == fcntl(ethread->evfd_, F_GETFD));
      ASSERT_TRUE(!!(O_NONBLOCK & fcntl(ethread->evfd_, F_GETFL)));
#else
      ASSERT_TRUE(FD_CLOEXEC == fcntl(ethread->evpipe_[0], F_GETFD));
      ASSERT_TRUE(FD_CLOEXEC == fcntl(ethread->evpipe_[1], F_GETFD));
      ASSERT_TRUE(!!(O_NONBLOCK & fcntl(ethread->evpipe_[0], F_GETFL)));
      ASSERT_TRUE(!!(O_NONBLOCK & fcntl(ethread->evpipe_[1], F_GETFL)));
#endif
      //TODO: test the behavior of evfd_/evpipe_[1]
      //TODO: test the behavior of ethreads_to_be_signalled_count_
      delete param->ethread_;
      param->test_ok_ = true;
    }
  }
}

inline void TestEThread::check_schedule_common(TestFuncParam *param)
{
  ASSERT_EQ(param->callback_event_, param->event_->callback_event_);
  ASSERT_TRUE(param->cookie_ == param->event_->cookie_);
  ASSERT_TRUE(param->cont_ == param->event_->continuation_);
  ASSERT_FALSE(param->event_->cancelled_);
  ASSERT_TRUE(param->event_->ethread_ == param->ethread_);
  ASSERT_TRUE(param->event_->continuation_->mutex_ == param->event_->mutex_);
}

void TestEThread::check_schedule_imm(TestFuncParam *param)
{
  if (!param->started_){
    SCHEDULE_INIT_COMMON(EVENT_IMMEDIATE);
    param->func_type_ = TEST_SCHEDULE_IMM;
    ((ObContInternal *)param->cont_)->event_count_++;
    param->event_ = param->ethread_->schedule_imm(param->cont_,
                                                  param->callback_event_,
                                                  param->cookie_);
    ASSERT_TRUE(NULL != param->event_);
  } else {
    check_schedule_common(param);
    ASSERT_EQ(0, param->event_->timeout_at_);
    ASSERT_EQ(0, param->event_->period_);
    param->test_ok_ = check_imm_test_ok(hrtime_diff(param->cur_time_, param->last_time_));
  }
}

void TestEThread::check_schedule_imm_signal(TestFuncParam *param)
{
  if (!param->started_) {
    SCHEDULE_INIT_COMMON(EVENT_IMMEDIATE);
    param->func_type_ = TEST_SCHEDULE_IMM_SIGNAL;
    ((ObContInternal *)param->cont_)->event_count_++;
    g_signal_hook_success = false;
    param->ethread_->signal_hook_ = handle_hook_test;
    param->event_ = param->ethread_->schedule_imm_signal(param->cont_,
                                                         param->callback_event_,
                                                         param->cookie_);
    ASSERT_TRUE(NULL != param->event_);
  } else {
    check_schedule_imm(param);
    param->test_ok_ = param->test_ok_ && g_signal_hook_success;
  }
}

void TestEThread::check_schedule_at(TestFuncParam *param)
{
  if (!param->started_) {
    SCHEDULE_INIT_COMMON(EVENT_INTERVAL);
    param->func_type_ = TEST_SCHEDULE_AT;
    param->atimeout_ = TEST_TIME_SECOND_AT(param);
    ((ObContInternal *)param->cont_)->event_count_++;
    param->event_ = param->ethread_->schedule_at(param->cont_,
                                                 param->atimeout_,
                                                 param->callback_event_,
                                                 param->cookie_);
    ASSERT_TRUE(NULL != param->event_);
    ASSERT_TRUE(param->at_delta_ > 0 ? 1 == param->event_->in_the_prot_queue_ : true);
  } else {
    check_schedule_common(param);
    ASSERT_GE(TEST_TIME_SECOND_AT(param), param->event_->timeout_at_);
    ASSERT_EQ(0, param->event_->period_);
    check_test_ok(param, param->at_delta_);
    //  param->at_delta_ <= 0,//at the past point,equal to imm
    //  param->at_delta_ >  0,//at the future point, equal to in
  }
}

void TestEThread::check_schedule_in(TestFuncParam *param)
{
  if (!param->started_) {
    SCHEDULE_INIT_COMMON(EVENT_INTERVAL);
    param->func_type_ = TEST_SCHEDULE_IN;
    ((ObContInternal *)param->cont_)->event_count_++;
    param->event_ = param->ethread_->schedule_in(param->cont_,
                                                 param->atimeout_,
                                                 param->callback_event_,
                                                 param->cookie_);
    ASSERT_TRUE(NULL != param->event_);
    ASSERT_TRUE(param->atimeout_ > 0 ? 1 == param->event_->in_the_prot_queue_ : true);
    if (param->event_->timeout_at_ < 0) {//drop into poll queue, can not update deletable_
      ((ObContInternal *)(param->cont_))->deletable_ = true;
    } else {}
  } else {
    check_schedule_common(param);
    ASSERT_LT(param->atimeout_, param->event_->timeout_at_);
    ASSERT_GE(get_hrtime_internal() + param->atimeout_, param->event_->timeout_at_);
    ASSERT_EQ(0, param->event_->period_);
    check_test_ok(param, param->atimeout_);
    //param->atimeout_ <= 0 //in the past point,equal to imm
  }
}

void TestEThread::check_schedule_every(TestFuncParam *param)
{
  if (!param->started_) {
    SCHEDULE_INIT_COMMON(EVENT_INTERVAL);
    param->func_type_ = TEST_SCHEDULE_EVERY;
    ((ObContInternal *)param->cont_)->event_count_ += TEST_DEFAULT_PERIOD_COUNT;
    param->event_ = param->ethread_->schedule_every(param->cont_,
                                                    param->aperiod_,
                                                    param->callback_event_,
                                                    param->cookie_);
    ASSERT_TRUE(NULL != param->event_);
    if (param->aperiod_ < 0) {//drop into poll queue, can not update deletable_
      ((ObContInternal *)(param->cont_))->deletable_ = true;
    } else {
      ASSERT_TRUE(1 == param->event_->in_the_prot_queue_);
    }
  } else {
    SCHEDULE_EVERY_FIRST_CHECK;
    param->test_ok_ = param->test_ok_ && check_test_ok(param, param->aperiod_);
    //param->aperiod_ < 0 ;//equal to schedule at, will fall into negative_queue
  }
}

void TestEThread::check_schedule(TestFuncParam *param)
{
  if (!param->started_) {
    SCHEDULE_INIT_COMMON(param->event_->callback_event_);
    param->func_type_ = TEST_SCHEDULE;
    param->event_->cookie_ = param;
    param->ethread_->signal_hook_ = handle_hook_test;
    ASSERT_EQ(OB_SUCCESS, param->ethread_->schedule(*param->event_, param->fast_signal_));
    ASSERT_TRUE(NULL != param->event_);
  } else {
    check_schedule_result(param, check_schedule_imm, check_schedule_at,
        check_schedule_in, check_schedule_every);
    if (param->fast_signal_) {
      param->test_ok_ = param->test_ok_ && g_signal_hook_success;
    }
  }
}

void TestEThread::check_schedule_imm_local(TestFuncParam *param)
{
  if (!param->started_){
    SCHEDULE_INIT_COMMON(EVENT_IMMEDIATE);
    param->func_type_ = TEST_SCHEDULE_IMM_LOCAL;
    ((ObContInternal *)param->cont_)->event_count_++;
    param->event_ = param->ethread_->schedule_imm_local(param->cont_,
                                                        param->callback_event_,
                                                        param->cookie_);
    ASSERT_TRUE(NULL != param->event_);
  } else {
    check_schedule_common(param);
    ASSERT_EQ(0, param->event_->period_);
    if (!param->locked_) {
      ASSERT_EQ(0, param->event_->timeout_at_);
      param->test_ok_ = check_imm_test_ok(hrtime_diff(param->cur_time_, param->last_time_));
    } else {
      ASSERT_NE(0, param->event_->timeout_at_);
      param->test_ok_ = check_imm_test_ok(hrtime_diff(param->cur_time_, param->last_time_),
          TEST_LOCK_SLEEP_TIME);
    }
  }
}

void TestEThread::check_schedule_at_local(TestFuncParam *param)
{
  if (!param->started_) {
    param->started_ = true;
    param->func_type_ = TEST_SCHEDULE_AT_LOCAL;
    ((ObContInternal *)param->cont_)->event_count_++;
    SCHEDULE_INIT_COMMON(EVENT_INTERVAL);
    param->atimeout_ = TEST_TIME_SECOND_AT(param);
    param->event_ = param->ethread_->schedule_at_local(param->cont_,
                                                       param->atimeout_,
                                                       param->callback_event_,
                                                       param->cookie_);
    ASSERT_TRUE(NULL != param->event_);
    ASSERT_TRUE(param->at_delta_ > 0 ? 1 == param->event_->in_the_prot_queue_ : true);
  } else {
    check_schedule_common(param);
    ASSERT_GE(TEST_TIME_SECOND_AT(param), param->event_->timeout_at_);
    ASSERT_EQ(0, param->event_->period_);
    check_test_ok(param, param->at_delta_);
    //  param->at_delta_ <= 0,//at the past point,equal to imm
    //  param->at_delta_ > 0,//at the future point, equal to in
  }
}

void TestEThread::check_schedule_in_local(TestFuncParam *param)
{
  if (!param->started_) {
    SCHEDULE_INIT_COMMON(EVENT_INTERVAL);
    param->func_type_ = TEST_SCHEDULE_IN_LOCAL;
    ((ObContInternal *)param->cont_)->event_count_++;
    param->event_ = param->ethread_->schedule_in_local(param->cont_,
                                                       param->atimeout_,
                                                       param->callback_event_,
                                                       param->cookie_);
    ASSERT_TRUE(NULL != param->event_);
    ASSERT_TRUE(param->atimeout_ > 0 ? 1 == param->event_->in_the_prot_queue_ : true);
    if (param->event_->timeout_at_ < 0) {//drop into poll queue, can not update deletable_
      ((ObContInternal *)(param->cont_))->deletable_ = true;
    } else {}
  } else {
    check_schedule_common(param);
    ASSERT_LT(param->atimeout_, param->event_->timeout_at_);
    ASSERT_GE(get_hrtime_internal() + param->atimeout_, param->event_->timeout_at_);
    ASSERT_EQ(0, param->event_->period_);
    check_test_ok(param, param->atimeout_);
    //param->atimeout_ <= 0 //in the past point,equal to imm
  }
}

void TestEThread::check_schedule_every_local(TestFuncParam *param)
{
  if (!param->started_) {
    SCHEDULE_INIT_COMMON(EVENT_INTERVAL);
    param->func_type_ = TEST_SCHEDULE_EVERY_LOCAL;
    ((ObContInternal *)param->cont_)->event_count_ += TEST_DEFAULT_PERIOD_COUNT;
    param->event_ = param->ethread_->schedule_every_local(param->cont_,
                                                          param->aperiod_,
                                                          param->callback_event_,
                                                          param->cookie_);
    ASSERT_TRUE(NULL != param->event_);
    if (param->aperiod_ < 0) {//drop into poll queue, can not update deletable_
      ((ObContInternal *)(param->cont_))->deletable_ = true;
    } else {
      ASSERT_TRUE(1 == param->event_->in_the_prot_queue_);
    }
  } else {
    SCHEDULE_EVERY_FIRST_CHECK;
    param->test_ok_ = param->test_ok_ && check_test_ok(param, param->aperiod_);
    //param->aperiod_ < 0 ;//equal to schedule at, will fall into negative_queue
  }
}

void TestEThread::check_schedule_local(TestFuncParam *param)
{
  if (!param->started_) {
    param->started_ = true;
    param->func_type_ = TEST_SCHEDULE_LOCAL;
    param->cookie_ = param;
    param->event_->cookie_ = param;
    ASSERT_TRUE(NULL != param->ethread_);
    param->last_time_ = get_hrtime_internal();
    ASSERT_EQ(OB_SUCCESS, param->ethread_->schedule_local(*param->event_));
    ASSERT_TRUE(NULL != param->event_);
  } else {
    if (REGULAR == param->ethread_->tt_) {
      check_schedule_result(param, check_schedule_imm_local, check_schedule_at_local,
          check_schedule_in_local, check_schedule_every_local);
      ASSERT_EQ(test_last_thread + 1, g_event_processor.next_thread_for_type_[ET_CALL]);
    } else if (DEDICATED == param->ethread_->tt_) {
      ASSERT_EQ(test_last_thread + 2, g_event_processor.next_thread_for_type_[ET_CALL]);
      int64_t cur_thread = (test_last_thread + 1) % g_event_processor.thread_count_for_type_[ET_CALL];
      ASSERT_TRUE(param->ethread_ !=  g_event_processor.event_thread_[ET_CALL][cur_thread]);
      //just for reusing check_schedule_result and passing test below,
      //in fact, param->ethread_ != param->event_->ethread_
      param->ethread_ = g_event_processor.event_thread_[ET_CALL][cur_thread];
      check_schedule_result(param, check_schedule_imm, check_schedule_at,
          check_schedule_in, check_schedule_every);
      param->ethread_ = g_event_processor.all_dedicate_threads_[0];//reset
    } else {}
  }
}

void start_obethred_common(ObEThread *ethread, int64_t event_thread_count, TestFuncParam *param)
{
  int ret = OB_SUCCESS;
  char thr_name[MAX_THREAD_NAME_LENGTH];

  g_event_processor.all_event_threads_[event_thread_count] = ethread;
  ethread->set_event_thread_type((ObEventThreadType)ET_CALL);
  g_event_processor.event_thread_[ET_CALL][event_thread_count] = ethread;
  g_event_processor.thread_count_for_type_[ET_CALL]++;

  if (OB_SUCCESS != (ret = ethread->init())) {
    LOG_ERROR("failed to init event thread", K(event_thread_count), K(ret));
  } else {
    snprintf(thr_name, MAX_THREAD_NAME_LENGTH, "[ET_NET %ld]", event_thread_count);
    if (OB_SUCCESS != (ret = ethread->start(thr_name, DEFAULT_STACKSIZE))) {
      LOG_ERROR("failed to start event thread", K(event_thread_count));
    } else {
      g_event_processor.event_thread_count_++;
      param->last_time_ = get_hrtime_internal();

      switch (param->cons_type_) {
      case TEST_SCHEDULE_IMM: {
          param->callback_event_ = EVENT_IMMEDIATE;
          param->cookie_ = param;
          ((ObContInternal *)param->cont_)->event_count_++;
          param->event_ = ethread->schedule_imm(param->cont_, EVENT_IMMEDIATE, (void *)param);
          break;
        }
      case TEST_SCHEDULE_AT: {
          param->callback_event_ = EVENT_INTERVAL;
          param->cookie_ = param;
          param->at_delta_ = HRTIME_SECONDS(2);
          param->atimeout_ = TEST_TIME_SECOND_AT(param);
          ((ObContInternal *)param->cont_)->event_count_++;
          param->event_ = ethread->schedule_at(param->cont_, param->atimeout_ ,
              EVENT_INTERVAL, (void *)param);
          ASSERT_TRUE(1 == param->event_->in_the_prot_queue_);
          break;
        }
      case TEST_SCHEDULE_IN: {
          param->callback_event_ = EVENT_INTERVAL;
          param->cookie_ = param;
          param->atimeout_ = TEST_TIME_SECOND_IN;
          ((ObContInternal *)param->cont_)->event_count_++;
          param->event_ = ethread->schedule_in(param->cont_, param->atimeout_ ,
              EVENT_INTERVAL, (void *)param);
          ASSERT_TRUE(1 == param->event_->in_the_prot_queue_);
          break;
        }
      case TEST_SCHEDULE_EVERY: {
          param->callback_event_ = EVENT_INTERVAL;
          param->cookie_ = param;
          param->aperiod_ = TEST_TIME_SECOND_EVERY;
          ((ObContInternal *)param->cont_)->event_count_ += TEST_DEFAULT_PERIOD_COUNT;
          param->event_ = ethread->schedule_every(param->cont_, param->aperiod_ ,
              EVENT_INTERVAL, (void *)param);
          ASSERT_TRUE(1 == param->event_->in_the_prot_queue_);
          break;
        }
      default: {
          LOG_ERROR("param->cons_type_, invalid event type");
          break;
        }
      }
    }
  }
}

void *thread_spawn_thread_internal(void *data)
{
  TestFuncParam *param = (TestFuncParam *)(data);
  printf("call spawn_thread_internal success\n");
  param->test_ok_ = true;
  signal_condition(param->wait_cond_);
  pthread_exit(NULL);
  return param;
}

int handle_schedule_test(ObContInternal *contp, ObEventType event, void* edata)
{
  UNUSED(event);
  UNUSED(contp);
  TestFuncParam *param;
  param = static_cast<TestFuncParam *>((static_cast<ObEvent *>(edata))->cookie_);
  ++param->seq_no_;
  param->cur_time_ = get_hrtime_internal();

  if (param->change2external_al_) {
    TestEThread::check_schedule_imm(static_cast<TestFuncParam *>(param->change_cookie_[0]));
  }
  if (param->change2external_local_) {
    TestEThread::check_schedule_in(static_cast<TestFuncParam *>(param->change_cookie_[1]));
    TestEThread::check_schedule_in(static_cast<TestFuncParam *>(param->change_cookie_[2]));
    TestEThread::check_schedule_in(static_cast<TestFuncParam *>(param->change_cookie_[3]));
  }
  if (param->test_lock_) {
    TestFuncParam *tmp_param = static_cast<TestFuncParam *>(param->change_cookie_[4]);
    tmp_param->cont_ = param->cont_;
    tmp_param->locked_ = true;
    TestEThread::check_schedule_imm_local(tmp_param);
    sleep(TEST_LOCK_SLEEP_TIME/HRTIME_SECOND);
  }

  switch (param->func_type_) {
  case TEST_OBETHREAD_NULL: {
      TestEThread::check_obethread_null(param);
      break;
    }
  case TEST_OBETHREAD_REGULAR: {
      TestEThread::check_obethread_regular(param);
      break;
    }
  case TEST_OBETHREAD_DEDICATED: {
      TestEThread::check_obethread_dedicated(param);
      break;
    }
  case TEST_SCHEDULE_IMM: {
      TestEThread::check_schedule_imm(param);
      break;
    }
  case TEST_SCHEDULE_IN: {
      TestEThread::check_schedule_in(param);
      break;
    }
  case TEST_SCHEDULE_AT: {
      TestEThread::check_schedule_at(param);
      break;
    }
  case TEST_SCHEDULE_EVERY: {
      TestEThread::check_schedule_every(param);
      break;
    }
  case TEST_SCHEDULE_IMM_SIGNAL: {
      TestEThread::check_schedule_imm_signal(param);
      break;
    }
  case TEST_SCHEDULE: {
      TestEThread::check_schedule(param);
      break;
    }
  case TEST_SCHEDULE_IMM_LOCAL: {
      TestEThread::check_schedule_imm_local(param);
      break;
    }
  case TEST_SCHEDULE_IN_LOCAL: {
      TestEThread::check_schedule_in_local(param);
      break;
    }
  case TEST_SCHEDULE_AT_LOCAL: {
      TestEThread::check_schedule_at_local(param);
      break;
    }
  case TEST_SCHEDULE_EVERY_LOCAL: {
      TestEThread::check_schedule_every_local(param);
      break;
    }
  case TEST_SCHEDULE_LOCAL: {
      TestEThread::check_schedule_local(param);
      break;
    }
  default: {
      LOG_ERROR("handle_schedule_test, invalid event type");
      break;
    }
  }

#ifdef DEBUG_TEST // print the process
  printf("func_type:%u; ", param->func_type_);
  print_process(param->seq_no_, param->cur_time_, param->last_time_);
#endif

  int ret = OB_ERROR;
  if (TEST_SCHEDULE_EVERY == param->func_type_
      ||TEST_SCHEDULE_EVERY_LOCAL == param->func_type_
      || (EVENT_INTERVAL == param->event_->callback_event_ && 0 != param->event_->period_)) {
    param->last_time_ = get_hrtime_internal();
    if (param->period_count_ > 1) {
      --param->period_count_;
    } else {
      ret = OB_SUCCESS;
      param->event_->cancelled_ = true;
    }
  } else {
    ret = OB_SUCCESS;
  }

  if (OB_SUCC(ret)) {
    signal_condition(param->wait_cond_);
  }
  return ret;
}

void *thread_main_start_test(void *func_param)
{
  TestFuncParam *param = (TestFuncParam *)func_param;
  init_event_system(EVENT_SYSTEM_MODULE_VERSION);
  g_event_processor.start(TEST_ET_CALL_THREADS_NUM, DEFAULT_STACKSIZE, false, false);
  param->test_ok_ = true;

  signal_condition(param->wait_cond_);
  this_ethread()->execute();
  return NULL;
}

TEST_F(TestEThread, g_event_processor_start)
{
  LOG_DEBUG("g_event_processor START");
  pthread_t thread;
  test_param[0] = create_funcparam_test();
  pthread_attr_t *attr_null = NULL;

  if (0 != pthread_create(&thread, attr_null, thread_main_start_test, (void *)test_param[0])) {
    LOG_ERROR("failed to create thread_main_start_test");
  } else {
    wait_condition(test_param[0]->wait_cond_);
  }

  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEThread, Thread_spawn_thread_internal)
{
  LOG_DEBUG("Thread spawn_thread_internal");
  test_param[0] = create_funcparam_test();

  TestEThread::check_thread_spawn_thread_internal(test_param[0]);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEThread, ObEThread_null1_imm)
{
  LOG_DEBUG("ObEThread_null imm");
  test_param[0] = create_funcparam_test(true, handle_schedule_test);
  test_param[0]->cons_type_ = TEST_SCHEDULE_IMM;

  TestEThread::check_obethread_null(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEThread, ObEThread_null2_at)
{
  LOG_DEBUG("ObEThread_null at");
  test_param[0] = create_funcparam_test(true, handle_schedule_test);
  test_param[0]->cons_type_ = TEST_SCHEDULE_AT;

  TestEThread::check_obethread_null(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEThread, ObEThread_REGULAR1_in)
{
  LOG_DEBUG("ObEThread_REGULAR");
  test_param[0] = create_funcparam_test(true, handle_schedule_test);
  test_param[0]->cons_type_ = TEST_SCHEDULE_IN;

  TestEThread::check_obethread_regular(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEThread, ObEThread_REGULAR2_every)
{
  LOG_DEBUG("ObEThread_REGULAR every");
  test_param[0] = create_funcparam_test(true, handle_schedule_test);
  test_param[0]->cons_type_ = TEST_SCHEDULE_EVERY;

  TestEThread::check_obethread_regular(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEThread, ObEThread_DEDICATED)
{
  LOG_DEBUG("ObEThread_DEDICATED");
  test_param[0] = create_funcparam_test(true, handle_schedule_test, new_proxy_mutex());

  TestEThread::check_obethread_dedicated(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEThread, init)
{
  LOG_DEBUG("init");
  test_param[0] = create_funcparam_test();

  TestEThread::check_init(test_param[0]);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEThread, free_event)
{
  LOG_DEBUG("free_event");
  test_param[0] = create_funcparam_test();

  if (NULL == (test_param[0]->event_ = op_reclaim_alloc(ObEvent))) {
    LOG_ERROR("fail to alloc mem for this_ethread_and_free_event");
  } else {
    test_param[0]->ptr_mutex_ = new_proxy_mutex();
    test_param[0]->event_->mutex_ = test_param[0]->ptr_mutex_;
    g_event_processor.all_event_threads_[0]->free_event(*test_param[0]->event_);
    ASSERT_TRUE(NULL == test_param[0]->event_->mutex_);
  }
}

TEST_F(TestEThread, is_or_set_event_thread_type)
{
  LOG_DEBUG("is_or_set event_thread_type");

  ObEThread *ethread = g_event_processor.all_event_threads_[0];
  ASSERT_TRUE(ethread->is_event_thread_type((ObEventThreadType)ET_CALL));
  ASSERT_FALSE(ethread->is_event_thread_type((ObEventThreadType)1));
  ethread->set_event_thread_type((ObEventThreadType)1);
  ASSERT_TRUE(ethread->is_event_thread_type((ObEventThreadType)1));
  ethread->set_event_thread_type((ObEventThreadType)ET_CALL);
}

TEST_F(TestEThread, schedule_imm)
{
  LOG_DEBUG("schedule_imm");
  test_param[0] = create_funcparam_test(true, handle_schedule_test);

  TestEThread::check_schedule_imm(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEThread, schedule_imm_signal)
{
  LOG_DEBUG("schedule_imm_signal");
  test_param[0] = create_funcparam_test(true, handle_schedule_test);

  TestEThread::check_schedule_imm_signal(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEThread, schedule_at1)
{
  LOG_DEBUG("schedule_at");
  test_param[0] = create_funcparam_test(true, handle_schedule_test,
      new_proxy_mutex());
  test_param[0]->at_delta_ = HRTIME_SECONDS(2);

  TestEThread::check_schedule_at(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEThread, schedule_at2_minus_negative_queue)
{
  LOG_DEBUG("schedule_at -negative_queue");
  test_param[0] = create_funcparam_test(true, handle_schedule_test);
  test_param[0]->at_delta_ = HRTIME_SECONDS(-2);

  TestEThread::check_schedule_at(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEThread, schedule_in1)
{
  LOG_DEBUG("schedule_in");
  test_param[0] = create_funcparam_test(true, handle_schedule_test);
  test_param[0]->atimeout_ = TEST_TIME_SECOND_IN;

  TestEThread::check_schedule_in(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEThread, schedule_in2_minus)
{
  LOG_DEBUG("schedule_in -");
  test_param[0] = create_funcparam_test(true, handle_schedule_test,
      new_proxy_mutex());
  test_param[0]->atimeout_ = 0 - TEST_TIME_SECOND_IN;

  TestEThread::check_schedule_in(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEThread, schedule_in3_minus_negative_queue)
{
  LOG_DEBUG("schedule_in -negative_queue");
  test_param[0] = create_funcparam_test(true, handle_schedule_test);
  test_param[0]->atimeout_ = 0 - TEST_TIME_SECOND_IN - get_hrtime_internal();

  TestEThread::check_schedule_in(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEThread, schedule_in4_multi_negative_queue)
{
  LOG_DEBUG("schedule_in multi -negative_queue");
  for (int i = 0; i < 3; i++) {
    test_param[i] = create_funcparam_test(true, handle_schedule_test, new_proxy_mutex());
    test_param[i]->common_ethread_ =  true;
  }
  test_param[0]->atimeout_ = 0 - HRTIME_SECONDS(2) - get_hrtime_internal();//poll insert
  test_param[1]->atimeout_ = 0 - HRTIME_SECONDS(4) - get_hrtime_internal();//poll insert
  test_param[2]->atimeout_ = 0 - HRTIME_SECONDS(3) - get_hrtime_internal();//poll insert

  for (int i = 0; i < 3; i++) {
    TestEThread::check_schedule_in(test_param[i]);
  }

  for (int i = 0; i < 3; i++) {
    wait_condition(test_param[i]->wait_cond_);
    ASSERT_TRUE(test_param[i]->test_ok_);
  }
}

TEST_F(TestEThread, schedule_every1)
{
  LOG_DEBUG("schedule_every");
  test_param[0] = create_funcparam_test(true, handle_schedule_test);
  test_param[0]->aperiod_ = TEST_TIME_SECOND_EVERY;

  TestEThread::check_schedule_every(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEThread, schedule_every2_minus_negative_queue)
{
  LOG_DEBUG("schedule_every - negative_queue");
  test_param[0] = create_funcparam_test(true, handle_schedule_test, new_proxy_mutex());
  test_param[0]->aperiod_ = 0 - TEST_TIME_SECOND_EVERY;

  TestEThread::check_schedule_every(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
}


TEST_F(TestEThread, schedule1_in)
{
  LOG_DEBUG("schedule (in)");
  test_param[0] = create_funcparam_test(true, handle_schedule_test,
      new_proxy_mutex());

  if (NULL == (test_param[0]->event_ = op_reclaim_alloc(ObEvent))) {
    LOG_ERROR("fail to alloc mem for schedule");
  } else {
    test_param[0]->event_->callback_event_ = EVENT_INTERVAL;
    test_param[0]->atimeout_ = TEST_TIME_SECOND_IN;
    ASSERT_EQ(common::OB_SUCCESS, test_param[0]->event_->init(*test_param[0]->cont_,
        get_hrtime_internal() + test_param[0]->atimeout_, 0));
    test_param[0]->fast_signal_ = false;
    ((ObContInternal *)test_param[0]->cont_)->event_count_++;

    TestEThread::check_schedule(test_param[0]);
    wait_condition(test_param[0]->wait_cond_);
    ASSERT_TRUE(test_param[0]->test_ok_);
  }
}

TEST_F(TestEThread, schedule2_at)
{
  LOG_DEBUG("schedule (at)");
  test_param[0] = create_funcparam_test(true, handle_schedule_test);

  if (NULL == (test_param[0]->event_ = op_reclaim_alloc(ObEvent))) {
    LOG_ERROR("fail to alloc mem for schedule");
  } else {
    test_param[0]->callback_event_ = EVENT_INTERVAL;
    test_param[0]->event_->callback_event_ = EVENT_INTERVAL;
    test_param[0]->at_delta_ = HRTIME_SECONDS(2);
    test_param[0]->atimeout_ = TEST_TIME_SECOND_AT(test_param[0]);
    ASSERT_EQ(common::OB_SUCCESS, test_param[0]->event_->init(*test_param[0]->cont_, test_param[0]->atimeout_, 0));
    test_param[0]->fast_signal_ = true;
    ((ObContInternal *)test_param[0]->cont_)->event_count_++;

    TestEThread::check_schedule(test_param[0]);
    wait_condition(test_param[0]->wait_cond_);
    ASSERT_TRUE(test_param[0]->test_ok_);
  }
}

TEST_F(TestEThread, schedule_imm_local)
{
  LOG_DEBUG("schedule_imm_local");
  test_param[0] = create_funcparam_test(true, handle_schedule_test, new_proxy_mutex());

  TestEThread::check_schedule_imm_local(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEThread, schedule_at_local1)
{
  LOG_DEBUG("schedule_at_local");
  test_param[0] = create_funcparam_test(true, handle_schedule_test, new_proxy_mutex());
  test_param[0]->at_delta_ = HRTIME_SECONDS(2);

  TestEThread::check_schedule_at_local(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEThread, schedule_at_local2_minus_negative_queue)
{
  LOG_DEBUG("schedule_at_local -negative_queue");
  test_param[0] = create_funcparam_test(true, handle_schedule_test, new_proxy_mutex());
  test_param[0]->at_delta_ = HRTIME_SECONDS(-2);

  TestEThread::check_schedule_at_local(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEThread, schedule_in_local1)
{
  LOG_DEBUG("schedule_in_local");
  test_param[0] = create_funcparam_test(true, handle_schedule_test, new_proxy_mutex());
  test_param[0]->atimeout_ = TEST_TIME_SECOND_IN;

  TestEThread::check_schedule_in_local(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEThread, schedule_in_local2_minus)
{
  LOG_DEBUG("schedule_in_local -");
  test_param[0] = create_funcparam_test(true, handle_schedule_test, new_proxy_mutex());
  test_param[0]->atimeout_ = 0 - TEST_TIME_SECOND_IN;

  TestEThread::check_schedule_in_local(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEThread, schedule_in_local3_minus_negative_queue)
{
  LOG_DEBUG("schedule_in_local -negative_queue");
  test_param[0] = create_funcparam_test(true, handle_schedule_test, new_proxy_mutex());
  test_param[0]->atimeout_ = 0 - TEST_TIME_SECOND_IN - get_hrtime_internal();

  TestEThread::check_schedule_in_local(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEThread, schedule_every_local1)
{
  LOG_DEBUG("schedule_every_local");
  test_param[0] = create_funcparam_test(true, handle_schedule_test,
      new_proxy_mutex());
  test_param[0]->aperiod_ = TEST_TIME_SECOND_EVERY;

  TestEThread::check_schedule_every_local(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEThread, schedule_every_local2_minus_negative_queue)
{
  LOG_DEBUG("schedule_every_local - negative_queue");
  test_param[0] = create_funcparam_test(true, handle_schedule_test,
      new_proxy_mutex());
  test_param[0]->aperiod_ = 0 - TEST_TIME_SECOND_EVERY;

  TestEThread::check_schedule_every_local(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEThread, schedule_local1_imm)
{
  LOG_DEBUG("schedule local (imm)");
  test_param[0] = create_funcparam_test(true, handle_schedule_test,
      new_proxy_mutex());

  if (NULL == (test_param[0]->event_ = op_reclaim_alloc(ObEvent))) {
    LOG_ERROR("fail to alloc mem for schedule local ");
  } else {
    test_param[0]->callback_event_ = EVENT_IMMEDIATE;
    test_param[0]->event_->callback_event_ = EVENT_IMMEDIATE;
    ASSERT_EQ(common::OB_SUCCESS, test_param[0]->event_->init(*test_param[0]->cont_, 0, 0));
    test_last_thread = g_event_processor.next_thread_for_type_[ET_CALL];
    test_param[0]->ethread_ = g_event_processor.assign_thread(ET_CALL);
    ((ObContInternal *)test_param[0]->cont_)->event_count_++;

    TestEThread::check_schedule_local(test_param[0]);
    wait_condition(test_param[0]->wait_cond_);
    ASSERT_TRUE(test_param[0]->test_ok_);
  }
}

TEST_F(TestEThread, schedule_local2_every)
{
  LOG_DEBUG("schedule local (every)");
  test_param[0] = create_funcparam_test(true, handle_schedule_test,
      new_proxy_mutex());

  if (NULL == (test_param[0]->event_ = op_reclaim_alloc(ObEvent))) {
    LOG_ERROR("fail to alloc mem for schedule local ");
  } else {
    test_param[0]->callback_event_ = EVENT_INTERVAL;
    test_param[0]->event_->callback_event_ = EVENT_INTERVAL;
    test_param[0]->aperiod_ = TEST_TIME_SECOND_EVERY;
    ASSERT_EQ(common::OB_SUCCESS, test_param[0]->event_->init(*test_param[0]->cont_,
        get_hrtime_internal() + test_param[0]->aperiod_, test_param[0]->aperiod_));
    test_last_thread = g_event_processor.next_thread_for_type_[ET_CALL];
    test_param[0]->ethread_ = g_event_processor.assign_thread(ET_CALL);
    ((ObContInternal *)test_param[0]->cont_)->event_count_ += TEST_DEFAULT_PERIOD_COUNT;

    TestEThread::check_schedule_local(test_param[0]);
    wait_condition(test_param[0]->wait_cond_);
    ASSERT_TRUE(test_param[0]->test_ok_);
  }
}

TEST_F(TestEThread, schedule_process_event_lock)
{
  LOG_DEBUG("schedule process_event lock");
  test_param[0] = create_funcparam_test(true, handle_schedule_test, new_proxy_mutex());
  test_param[0]->test_lock_ = true;
  test_param[1] = create_funcparam_test();
  test_param[0]->change_cookie_[4] = test_param[1];

  TestEThread::check_schedule_imm(test_param[0]);

  for (int i = 0; i < 2; i++) {
    wait_condition(test_param[i]->wait_cond_);
    ASSERT_TRUE(test_param[i]->test_ok_);
  }

  pthread_mutex_destroy(&test_param[1]->wait_cond_->mutex_);
  pthread_cond_destroy(&test_param[1]->wait_cond_->cond_);
  delete test_param[1]->wait_cond_;
  delete test_param[1];
  test_param[1] = NULL;
}

TEST_F(TestEThread, schedule_multi_execute)
{
  LOG_DEBUG("schedule multi execute");

  for (int i = 0; i < 7; i++) {
    test_param[i] = create_funcparam_test(true, handle_schedule_test, new_proxy_mutex());
    test_param[i]->common_ethread_ =  true;
  }
  //event_queue event,when process_event,make other events into event_queue_external_
  test_param[0]->atimeout_ = 0 - HRTIME_SECONDS(2);
  test_param[0]->change2external_al_ = true;
  test_param[0]->change2external_local_ = true;
  test_param[0]->change_cookie_[0] = test_param[2];
  test_param[0]->change_cookie_[1] = test_param[3];
  test_param[0]->change_cookie_[2] = test_param[4];
  test_param[0]->change_cookie_[3] = test_param[5];
  //negative_queue event,make sure can execute the negative_queue after finishing event_queue
  test_param[1]->atimeout_ = 0 - HRTIME_SECONDS(2) - get_hrtime_internal();

  test_param[3]->atimeout_ = HRTIME_SECONDS(2);//event_queue_.enqueue
  test_param[4]->atimeout_ = 0 - HRTIME_SECONDS(2) - get_hrtime_internal();//poll insert
  test_param[5]->atimeout_ = 0 - HRTIME_SECONDS(3) - get_hrtime_internal();//poll enqueue
  test_param[5]->change2external_al_ = true;
  test_param[5]->change_cookie_[0] = test_param[6];

  TestEThread::check_schedule_in_local(test_param[0]);
  TestEThread::check_schedule_in_local(test_param[1]);

  for (int i = 0; i < 7; i++) {
    wait_condition(test_param[i]->wait_cond_);
    ASSERT_TRUE(test_param[i]->test_ok_);
  }
}

} // end of namespace obproxy
} // end of namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  OB_LOGGER.set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
