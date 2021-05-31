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

namespace oceanbase
{
namespace obproxy
{
using namespace proxy;
using namespace common;
using namespace event;

#define TEST_SPAWN_THREADS_NUM          1
#define TEST_DEFAULT_NEXT_THREAD        0
#define TEST_DEFAULT_DTHREAD_NUM        0
#define TEST_ET_SPAWN                   (ET_CALL + 1)

#define OB_ALIGN(size, boundary)    (((size) + ((boundary) - 1)) & ~((boundary) - 1))

#define INVALID_SPAWN_EVENT_THREADS(...)                                                   \
  {                                                                                          \
    ASSERT_TRUE(OB_INVALID_ARGUMENT == g_event_processor.spawn_event_threads(__VA_ARGS__));\
  }

#define NULL_SCHEDULE_IMM(...)                                          \
  {                                                                       \
    ASSERT_TRUE(NULL == g_event_processor.schedule_imm(__VA_ARGS__));   \
  }

#define NULL_SCHEDULE_AT(...)                                          \
  {                                                                      \
    ASSERT_TRUE(NULL == g_event_processor.schedule_at(__VA_ARGS__));   \
  }

#define NULL_SCHEDULE_IN(...)                                          \
  {                                                                      \
    ASSERT_TRUE(NULL == g_event_processor.schedule_in(__VA_ARGS__));   \
  }

#define NULL_SCHEDULE_EVERY(...)                                          \
  {                                                                      \
    ASSERT_TRUE(NULL == g_event_processor.schedule_every(__VA_ARGS__));   \
  }

#define NULL_SCHEDULE_IMM_SIGNAL(...)                                           \
  {                                                                               \
    ASSERT_TRUE(NULL == g_event_processor.schedule_imm_signal(__VA_ARGS__));    \
  }

#define NULL_SCHEDULE(...)                                                  \
  {                                                                           \
    ASSERT_TRUE(NULL == g_event_processor.schedule(__VA_ARGS__)->ethread_); \
  }

#define NULL_PREPARE_SCHEDULE_IMM(...)                                          \
  {                                                                               \
    ASSERT_TRUE(NULL == g_event_processor.prepare_schedule_imm(__VA_ARGS__));   \
  }

enum TestProcessorFuncType
{
  TEST_NULL = 0,
  TEST_SCHEDULE_IMM = 1,
  TEST_SCHEDULE_IMM_SIGNAL,
  TEST_SCHEDULE_AT,
  TEST_SCHEDULE_IN ,
  TEST_SCHEDULE_EVERY,
  TEST_PREPARE_SCHEDULE_IMM,
  TEST_DO_SCHEDULE,
  TEST_SCHEDULE,
  TEST_SPAWN_THREAD
};

unsigned int next_thread[2] = {TEST_DEFAULT_NEXT_THREAD, TEST_DEFAULT_NEXT_THREAD};
unsigned int event_thread_count[2] = {TEST_ET_CALL_THREADS_NUM, TEST_SPAWN_THREADS_NUM};
unsigned int event_type_count = 0;
int64_t dthread_num = TEST_DEFAULT_DTHREAD_NUM;

int handle_schedule_test(ObContInternal *contp, ObEventType event, void *edata);
void *thread_main_start_test(void *func_param);
void set_fast_signal_true(TestFuncParam *param);

class TestEventProcessor : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown();
  static void check_start(TestFuncParam *param);
  static void check_spawn_event_threads(TestFuncParam *param);
  static void check_schedule_common(TestFuncParam *param);
  static void check_schedule_imm(TestFuncParam *param);
  static void check_schedule_imm_signal(TestFuncParam *param);
  static void check_schedule_at(TestFuncParam *param);
  static void check_schedule_in(TestFuncParam *param);
  static void check_schedule_every(TestFuncParam *param);
  static void check_schedule(TestFuncParam *param);
  static void check_prepare_schedule_imm(TestFuncParam *param);
  static void check_do_schedule(TestFuncParam *param);
  static void check_allocate(TestFuncParam *param);
  static void check_assign_thread(TestFuncParam *param);
  static void check_spawn_thread(TestFuncParam *param);

public:
  TestFuncParam *test_param[MAX_PARAM_ARRAY_SIZE];
};

void TestEventProcessor::SetUp()
{
  for (int i = 0; i < MAX_PARAM_ARRAY_SIZE; ++i) {
    test_param[i] = NULL;
  }
}

void TestEventProcessor::TearDown()
{
  for (int i = 0; i < MAX_PARAM_ARRAY_SIZE; ++i) {
    if (NULL != test_param[i]) {
      destroy_funcparam_test(test_param[i]);
    } else {}
  }
}

void TestEventProcessor::check_start(TestFuncParam *param)
{
  init_event_system(EVENT_SYSTEM_MODULE_VERSION);

  ASSERT_EQ(0, g_event_processor.event_thread_count_);
  ASSERT_EQ(0, g_event_processor.thread_group_count_);
  ASSERT_EQ(0, g_event_processor.dedicate_thread_count_);
  ASSERT_EQ(0, g_event_processor.thread_data_used_);
  ASSERT_FALSE(g_event_processor.started_);
  for (int i = 0; i < MAX_EVENT_TYPES; ++i) {
    ASSERT_EQ(0, g_event_processor.thread_count_for_type_[i]);
    ASSERT_EQ(0U, g_event_processor.next_thread_for_type_[i]);
    for (int64_t j = 0; j < MAX_THREADS_IN_EACH_TYPE; ++j) {
      ASSERT_TRUE(NULL == g_event_processor.event_thread_[i][j]);
    }
  }
  for (int64_t i = 0; i < MAX_EVENT_THREADS; ++i) {
    ASSERT_TRUE(NULL == g_event_processor.all_event_threads_[i]);
    ASSERT_TRUE(NULL == g_event_processor.all_dedicate_threads_[i]);
  }

  ASSERT_EQ(OB_INVALID_ARGUMENT, g_event_processor.start(-100));
  ASSERT_EQ(OB_INVALID_ARGUMENT, g_event_processor.start(-1));
  ASSERT_EQ(OB_INVALID_ARGUMENT, g_event_processor.start(0));
  ASSERT_EQ(OB_INVALID_ARGUMENT, g_event_processor.start(MAX_EVENT_THREADS + 1));
  ASSERT_EQ(OB_INVALID_ARGUMENT, g_event_processor.start(MAX_EVENT_THREADS + 100));
  ASSERT_EQ(OB_INVALID_ARGUMENT, g_event_processor.start(1, -100));
  ASSERT_EQ(OB_INVALID_ARGUMENT, g_event_processor.start(1, -1));
  ASSERT_EQ(OB_SUCCESS, g_event_processor.start(TEST_ET_CALL_THREADS_NUM, DEFAULT_STACKSIZE, false, false));
  ASSERT_EQ(OB_INIT_TWICE, g_event_processor.start(MAX_EVENT_THREADS));
  ASSERT_EQ(OB_INIT_TWICE, g_event_processor.start(0));
  ASSERT_EQ(OB_INIT_TWICE, g_event_processor.start(-1));
  ASSERT_EQ(OB_INIT_TWICE, g_event_processor.start(-1, 0));

  ASSERT_EQ(TEST_ET_CALL_THREADS_NUM, g_event_processor.event_thread_count_);
  ASSERT_EQ(TEST_ET_CALL_THREADS_NUM, g_event_processor.thread_count_for_type_[ET_CALL]);
  ASSERT_EQ(1, g_event_processor.thread_group_count_);
  ASSERT_EQ(0, g_event_processor.dedicate_thread_count_);
  ASSERT_TRUE(g_event_processor.started_);
  for (int64_t i = 0; i < TEST_ET_CALL_THREADS_NUM; ++i) {
    ASSERT_TRUE(g_event_processor.all_event_threads_[i] != NULL);
    ASSERT_TRUE(g_event_processor.event_thread_[ET_CALL][i]
                == g_event_processor.all_event_threads_[i]);
    ASSERT_TRUE(REGULAR == g_event_processor.all_event_threads_[i]->tt_);
    ASSERT_TRUE(g_event_processor.all_event_threads_[i]->is_event_thread_type(ET_CALL));
  }

  next_thread[ET_CALL] = TEST_DEFAULT_NEXT_THREAD;
  ++event_type_count;
  param->test_ok_ = true;
}

void TestEventProcessor::check_spawn_event_threads(TestFuncParam *param)
{
  int64_t stacksize = DEFAULT_STACKSIZE;
  ObEventThreadType etype = 0;
  char thr_name[MAX_THREAD_NAME_LENGTH];
  snprintf(thr_name, MAX_THREAD_NAME_LENGTH, "[T_ET_SPAWN]");
  char *thr_null = NULL;

  INVALID_SPAWN_EVENT_THREADS(-100, thr_name, stacksize, etype);
  INVALID_SPAWN_EVENT_THREADS(-1, thr_name, stacksize, etype);
  INVALID_SPAWN_EVENT_THREADS(0, thr_name, stacksize, etype);
  INVALID_SPAWN_EVENT_THREADS(MAX_EVENT_THREADS - TEST_ET_CALL_THREADS_NUM + 1,
                              thr_name, stacksize, etype);

  int64_t thread_group_count = g_event_processor.thread_group_count_;//save
  g_event_processor.thread_group_count_ = MAX_EVENT_TYPES;//update error key
  INVALID_SPAWN_EVENT_THREADS(TEST_SPAWN_THREADS_NUM, thr_name, stacksize, etype);
  g_event_processor.thread_group_count_ = MAX_EVENT_TYPES + 1;//update error key
  INVALID_SPAWN_EVENT_THREADS(TEST_SPAWN_THREADS_NUM, thr_name, stacksize, etype);
  g_event_processor.thread_group_count_ = thread_group_count;//reset

  INVALID_SPAWN_EVENT_THREADS(TEST_SPAWN_THREADS_NUM, thr_null, stacksize, etype);
  INVALID_SPAWN_EVENT_THREADS(TEST_SPAWN_THREADS_NUM, thr_name, -100, etype);
  INVALID_SPAWN_EVENT_THREADS(TEST_SPAWN_THREADS_NUM, thr_name, -1, etype);

  ASSERT_TRUE(OB_SUCCESS == g_event_processor.spawn_event_threads(TEST_SPAWN_THREADS_NUM,
                                                                  thr_name,
                                                                  stacksize,
                                                                  etype));

  ASSERT_EQ(TEST_ET_SPAWN, etype);
  ASSERT_EQ(2, g_event_processor.thread_group_count_);
  ASSERT_EQ(TEST_SPAWN_THREADS_NUM, g_event_processor.thread_count_for_type_[TEST_ET_SPAWN]);

  int64_t tmp_n = g_event_processor.event_thread_count_ - TEST_SPAWN_THREADS_NUM;
  for (int64_t i = tmp_n; i < g_event_processor.event_thread_count_; ++i) {
    ASSERT_TRUE(NULL != g_event_processor.all_event_threads_[i]);
    ASSERT_TRUE(g_event_processor.event_thread_[TEST_ET_SPAWN][i - tmp_n]
                == g_event_processor.all_event_threads_[i]);
    ASSERT_TRUE(REGULAR == g_event_processor.all_event_threads_[i]->tt_);
    ASSERT_TRUE(g_event_processor.all_event_threads_[i]->is_event_thread_type(TEST_ET_SPAWN));
  }
  next_thread[TEST_ET_SPAWN] = TEST_DEFAULT_NEXT_THREAD;
  ++event_type_count;
  param->test_ok_ = true;
}

void TestEventProcessor::check_schedule_common(TestFuncParam *param)
{
  ASSERT_EQ(param->callback_event_, param->event_->callback_event_);
  ASSERT_TRUE(param->cookie_ == param->event_->cookie_);
  ASSERT_TRUE(param->cont_ == param->event_->continuation_);
  ASSERT_FALSE(param->event_->cancelled_);
  ASSERT_TRUE(param->event_->continuation_->mutex_ == param->event_->mutex_);
  if (event_thread_count[param->event_type_] > 1)
    ASSERT_EQ(++next_thread[param->event_type_],
              g_event_processor.next_thread_for_type_[param->event_type_]);
  else {
    ASSERT_EQ(0U, g_event_processor.next_thread_for_type_[param->event_type_]);
  }
}

void TestEventProcessor::check_schedule_imm(TestFuncParam *param)
{
  if (!param->started_) {
    param->started_ = true;
    param->func_type_ = TEST_SCHEDULE_IMM;
    param->cookie_ = param;
    param->callback_event_ = EVENT_IMMEDIATE;
    ObContinuation *cont_null = NULL;
    NULL_SCHEDULE_IMM(cont_null, param->event_type_, param->callback_event_, param->cookie_);
    NULL_SCHEDULE_IMM(param->cont_, MAX_EVENT_TYPES, param->callback_event_, param->cookie_);
    NULL_SCHEDULE_IMM(param->cont_, MAX_EVENT_TYPES + 1, param->callback_event_, param->cookie_);
    NULL_SCHEDULE_IMM(param->cont_, -10, param->callback_event_, param->cookie_);
    NULL_SCHEDULE_IMM(param->cont_, -1, param->callback_event_, param->cookie_);
    NULL_SCHEDULE_IMM(param->cont_, event_type_count + 1, param->callback_event_, param->cookie_);

    ((ObContInternal *)param->cont_)->event_count_++;
    param->last_time_ = get_hrtime_internal();
    param->event_ = g_event_processor.schedule_imm(param->cont_,
                                                   param->event_type_,
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

void TestEventProcessor::check_schedule_at(TestFuncParam *param)
{
  if (!param->started_) {
    param->started_ = true;
    param->func_type_ = TEST_SCHEDULE_AT;
    param->cookie_ = param;
    param->callback_event_ = EVENT_INTERVAL;
    param->atimeout_ = TEST_TIME_SECOND_AT(param);
    ObContinuation *cont_null = NULL;
    NULL_SCHEDULE_AT(cont_null, param->atimeout_, param->event_type_,
                     param->callback_event_, param->cookie_);
    NULL_SCHEDULE_AT(param->cont_, param->atimeout_, MAX_EVENT_TYPES,
                     param->callback_event_, param->cookie_);
    NULL_SCHEDULE_AT(param->cont_, param->atimeout_, MAX_EVENT_TYPES + 1,
                     param->callback_event_, param->cookie_);
    NULL_SCHEDULE_AT(param->cont_, param->atimeout_, -10, param->callback_event_,
                     param->cookie_);
    NULL_SCHEDULE_AT(param->cont_, param->atimeout_, -1, param->callback_event_,
                     param->cookie_);
    NULL_SCHEDULE_AT(param->cont_, param->atimeout_, event_type_count + 1,
                     param->callback_event_, param->cookie_);
    NULL_SCHEDULE_AT(param->cont_, HRTIME_SECONDS(0), param->event_type_,
                     param->callback_event_, param->cookie_);
    NULL_SCHEDULE_AT(param->cont_, HRTIME_SECONDS(-1), param->event_type_,
                     param->callback_event_, param->cookie_);

    ((ObContInternal *)param->cont_)->event_count_++;
    param->last_time_ = get_hrtime_internal();
    param->event_ = g_event_processor.schedule_at(param->cont_,
                                                  param->atimeout_,
                                                  param->event_type_,
                                                  param->callback_event_,
                                                  param->cookie_);
    ASSERT_TRUE(NULL != param->event_);
    ASSERT_TRUE(param->at_delta_ > 0 ? 1 == param->event_->in_the_prot_queue_ : true);
  } else {
    check_schedule_common(param);
    ASSERT_GE(TEST_TIME_SECOND_AT(param), param->event_->timeout_at_);
    ASSERT_EQ(0, param->event_->period_);

    check_test_ok(param, param->at_delta_);
    //  param->at_delta_ <= 0,//at the past point, equal to imm
    //  param->at_delta_ > 0,//at the future point, equal to in
  }
}

void TestEventProcessor::check_schedule_in(TestFuncParam *param)
{
  if (!param->started_) {
    param->started_ = true;
    param->func_type_ = TEST_SCHEDULE_IN;
    param->cookie_ = param;
    param->callback_event_ = EVENT_INTERVAL;
    ObContinuation *cont_null = NULL;
    NULL_SCHEDULE_IN(cont_null, param->atimeout_, param->event_type_,
                     param->callback_event_, param->cookie_);
    NULL_SCHEDULE_IN(param->cont_, param->atimeout_, MAX_EVENT_TYPES,
                     param->callback_event_, param->cookie_);
    NULL_SCHEDULE_IN(param->cont_, param->atimeout_, MAX_EVENT_TYPES + 1,
                     param->callback_event_, param->cookie_);
    NULL_SCHEDULE_IN(param->cont_, param->atimeout_, -10, param->callback_event_,
                     param->cookie_);
    NULL_SCHEDULE_IN(param->cont_, param->atimeout_, -1, param->callback_event_,
                     param->cookie_);
    NULL_SCHEDULE_IN(param->cont_, param->atimeout_, event_type_count + 1,
                     param->callback_event_, param->cookie_);

    ((ObContInternal *)param->cont_)->event_count_++;
    param->last_time_ = get_hrtime_internal();
    param->event_ = g_event_processor.schedule_in(param->cont_,
                                                  param->atimeout_,
                                                  param->event_type_,
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

void TestEventProcessor::check_schedule_every(TestFuncParam *param)
{
  if (!param->started_) {
    param->started_ = true;
    param->func_type_ = TEST_SCHEDULE_EVERY;
    param->cookie_ = param;
    param->callback_event_ = EVENT_INTERVAL;
    ObContinuation *cont_null = NULL;
    NULL_SCHEDULE_EVERY(cont_null, param->aperiod_, param->event_type_,
                        param->callback_event_, param->cookie_);
    NULL_SCHEDULE_EVERY(param->cont_, param->aperiod_, MAX_EVENT_TYPES,
                        param->callback_event_, param->cookie_);
    NULL_SCHEDULE_EVERY(param->cont_, param->aperiod_, MAX_EVENT_TYPES + 1,
                        param->callback_event_, param->cookie_);
    NULL_SCHEDULE_EVERY(param->cont_, param->aperiod_, -10, param->callback_event_,
                        param->cookie_);
    NULL_SCHEDULE_EVERY(param->cont_, param->aperiod_, -1, param->callback_event_,
                        param->cookie_);
    NULL_SCHEDULE_EVERY(param->cont_, param->aperiod_, event_type_count + 1,
                        param->callback_event_, param->cookie_);
    NULL_SCHEDULE_EVERY(param->cont_, HRTIME_SECONDS(0), param->event_type_,
                        param->callback_event_, param->cookie_);

    ((ObContInternal *)param->cont_)->event_count_ += TEST_DEFAULT_PERIOD_COUNT;
    param->last_time_ = get_hrtime_internal();
    param->event_ = g_event_processor.schedule_every(param->cont_,
                                                     param->aperiod_,
                                                     param->event_type_,
                                                     param->callback_event_,
                                                     param->cookie_);
    ASSERT_TRUE(NULL != param->event_);
    ASSERT_TRUE(param->aperiod_ > 0 ? 1 == param->event_->in_the_prot_queue_ : true);
    if (param->aperiod_ < 0) {//drop into poll queue, can not update deletable_
      ((ObContInternal *)(param->cont_))->deletable_ = true;
    } else {}
  } else {
    SCHEDULE_EVERY_FIRST_CHECK;
    param->test_ok_ = param->test_ok_ && check_test_ok(param, param->aperiod_);
    //param->aperiod_ < 0 ;//equal to schedule at, will fall into negative_queue
  }
}

void TestEventProcessor::check_schedule(TestFuncParam *param)
{
  if (!param->started_) {
    param->started_ = true;
    param->func_type_ = TEST_SCHEDULE;
    param->callback_event_ = param->event_->callback_event_;
    param->cookie_ = param;
    param->event_->cookie_ = param;
    param->last_time_ = get_hrtime_internal();
    param->event_ = g_event_processor.schedule(param->event_,
                                               param->event_type_,
                                               param->fast_signal_);
    ASSERT_TRUE(NULL != param->event_);
    ASSERT_TRUE(NULL != param->event_->ethread_);
  } else {
    check_schedule_result(param, check_schedule_imm, check_schedule_at,
                          check_schedule_in, check_schedule_every);
    if (param->fast_signal_) {
      param->test_ok_ = param->test_ok_ && g_signal_hook_success;
    }
  }
}

void TestEventProcessor::check_schedule_imm_signal(TestFuncParam *param)
{
  if (!param->started_) {
    param->started_ = true;
    param->func_type_ = TEST_SCHEDULE_IMM_SIGNAL;
    param->cookie_ = param;
    param->callback_event_ = EVENT_IMMEDIATE;
    set_fast_signal_true(param);
    ObContinuation *cont_null = NULL;
    NULL_SCHEDULE_IMM_SIGNAL(cont_null, param->event_type_, param->callback_event_,
                             param->cookie_);
    NULL_SCHEDULE_IMM_SIGNAL(param->cont_, MAX_EVENT_TYPES, param->callback_event_,
                             param->cookie_);
    NULL_SCHEDULE_IMM_SIGNAL(param->cont_, MAX_EVENT_TYPES + 1, param->callback_event_,
                             param->cookie_);
    NULL_SCHEDULE_IMM_SIGNAL(param->cont_, -10, param->callback_event_, param->cookie_);
    NULL_SCHEDULE_IMM_SIGNAL(param->cont_, -1, param->callback_event_, param->cookie_);
    NULL_SCHEDULE_IMM_SIGNAL(param->cont_, event_type_count + 1, param->callback_event_,
                             param->cookie_);

    ((ObContInternal *)param->cont_)->event_count_++;
    param->last_time_ = get_hrtime_internal();
    param->event_ = g_event_processor.schedule_imm_signal(param->cont_,
                                                          param->event_type_,
                                                          param->callback_event_,
                                                          param->cookie_);
    ASSERT_TRUE(NULL != param->event_);
  } else {
    check_schedule_imm(param);
    param->test_ok_ = param->test_ok_ && g_signal_hook_success;
  }
}

void TestEventProcessor::check_prepare_schedule_imm(TestFuncParam *param)
{
  if (!param->started_) {
    param->started_ = true;
    param->func_type_ = TEST_PREPARE_SCHEDULE_IMM;
    param->callback_event_ = EVENT_IMMEDIATE;
    param->cookie_ = param;
    ObContinuation *cont_null = NULL;
    NULL_PREPARE_SCHEDULE_IMM(cont_null, param->event_type_, param->callback_event_,
                              param->cookie_);
    NULL_PREPARE_SCHEDULE_IMM(param->cont_, MAX_EVENT_TYPES, param->callback_event_,
                              param->cookie_);
    NULL_PREPARE_SCHEDULE_IMM(param->cont_, MAX_EVENT_TYPES + 1, param->callback_event_,
                              param->cookie_);
    NULL_PREPARE_SCHEDULE_IMM(param->cont_, -10, param->callback_event_, param->cookie_);
    NULL_PREPARE_SCHEDULE_IMM(param->cont_, -1, param->callback_event_, param->cookie_);
    NULL_PREPARE_SCHEDULE_IMM(param->cont_, event_type_count + 1, param->callback_event_,
                              param->cookie_);

    ((ObContInternal *)param->cont_)->event_count_++;
    param->last_time_ = get_hrtime_internal();
    param->event_ = g_event_processor.prepare_schedule_imm(param->cont_,
                                                           param->event_type_,
                                                           param->callback_event_,
                                                           param->cookie_);
    ASSERT_TRUE(NULL != param->event_);
    param->event_->ethread_->event_queue_external_.enqueue(param->event_);
  } else {
    check_schedule_imm(param);
  }
}

void TestEventProcessor::check_do_schedule(TestFuncParam *param)
{
  if (!param->started_) {
    param->started_ = true;
    param->func_type_ = TEST_DO_SCHEDULE;
    param->callback_event_ = param->event_->callback_event_;
    param->cookie_ = param;
    param->event_->cookie_ = param;
    ObEvent *event_null = NULL;
    g_event_processor.do_schedule(event_null, param->fast_signal_);
    param->last_time_ = get_hrtime_internal();
    g_event_processor.do_schedule(param->event_, param->fast_signal_);
  } else {
    check_schedule(param);
  }
}

void TestEventProcessor::check_allocate(TestFuncParam *param)
{
  int64_t start = OB_ALIGN(offsetof(ObEThread, thread_private_), 16);
  int64_t old = g_event_processor.thread_data_used_;
  int64_t loss = start - offsetof(ObEThread, thread_private_);

  ASSERT_TRUE(-1 == g_event_processor.allocate(-10));
  ASSERT_TRUE((old + start) == g_event_processor.allocate(0));
  ASSERT_TRUE(-1 == g_event_processor.allocate(ObEThread::MAX_THREAD_DATA_SIZE - old - loss + 1));
  ASSERT_TRUE((start + old) == g_event_processor.allocate(1));
  ASSERT_TRUE(g_event_processor.thread_data_used_ == old + 16);
  old =  old + 16;
  ASSERT_TRUE((start + old) == g_event_processor.allocate(17));
  ASSERT_TRUE(g_event_processor.thread_data_used_ == old + 32);
  old =  old + 32;
  ASSERT_TRUE((start + old) == g_event_processor.allocate(16));
  ASSERT_TRUE(g_event_processor.thread_data_used_ == old + 16);
  param->test_ok_ = true;
}

void TestEventProcessor::check_assign_thread(TestFuncParam *param)
{
  ObEventThreadType event_type = ET_CALL;
  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(NULL != g_event_processor.assign_thread(event_type));
    if (g_event_processor.next_thread_for_type_[event_type] > 1) {
      ASSERT_EQ(g_event_processor.next_thread_for_type_[event_type],
                ++next_thread[event_type]);
    } else {
      ASSERT_EQ(g_event_processor.next_thread_for_type_[event_type], 0U);
    }
  }

  event_type = TEST_ET_SPAWN;
  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(NULL != g_event_processor.assign_thread(event_type));
    if (g_event_processor.next_thread_for_type_[event_type] > 1) {
      ASSERT_EQ(g_event_processor.next_thread_for_type_[event_type],
                ++next_thread[event_type]);
    } else {
      ASSERT_EQ(g_event_processor.next_thread_for_type_[event_type], 0U);
    }
  }
  param->test_ok_ = true;
}

void TestEventProcessor::check_spawn_thread(TestFuncParam *param)
{
  if (!param->started_) {
    param->started_ = true;
    param->func_type_ = TEST_SPAWN_THREAD;
    snprintf(param->thr_name_, MAX_THREAD_NAME_LENGTH, "[T_ET_SPAWN_D]");
    ((ObContInternal *)param->cont_)->event_count_++;
    char *thr_null = NULL;
    ObContinuation *cont_null = NULL;
    int64_t dedicate_thread_count = g_event_processor.dedicate_thread_count_;

    ASSERT_TRUE(NULL == g_event_processor.spawn_thread(cont_null,
                                                       param->thr_name_,
                                                       param->stacksize_));
    g_event_processor.dedicate_thread_count_ = MAX_EVENT_THREADS;
    ASSERT_TRUE(NULL == g_event_processor.spawn_thread(param->cont_,
                                                       param->thr_name_,
                                                       param->stacksize_));
    g_event_processor.dedicate_thread_count_ = MAX_EVENT_THREADS + 10;
    ASSERT_TRUE(NULL == g_event_processor.spawn_thread(param->cont_,
                                                       param->thr_name_,
                                                       param->stacksize_));
    g_event_processor.dedicate_thread_count_ = dedicate_thread_count;

    ASSERT_TRUE(NULL == g_event_processor.spawn_thread(param->cont_,
                                                       thr_null,
                                                       param->stacksize_));
    ASSERT_TRUE(NULL == g_event_processor.spawn_thread(param->cont_,
                                                       param->thr_name_,
                                                       -100));
    param->last_time_ = get_hrtime_internal();
    param->event_ = g_event_processor.spawn_thread(param->cont_,
                                                   param->thr_name_,
                                                   param->stacksize_);
    param->event_->cookie_ = param;
    ASSERT_TRUE(NULL != param->event_);
  } else {
    ASSERT_TRUE(param->cont_ == param->event_->continuation_);
    ASSERT_EQ(0, param->event_->timeout_at_);
    ASSERT_EQ(0, param->event_->period_);
    ASSERT_FALSE(param->event_->cancelled_);
    ASSERT_TRUE(param->event_->continuation_->mutex_ == param->event_->mutex_);
    ASSERT_TRUE(DEDICATED == g_event_processor.all_dedicate_threads_[dthread_num]->tt_);
    ASSERT_TRUE(++dthread_num == g_event_processor.dedicate_thread_count_);

    param->test_ok_ = check_imm_test_ok(hrtime_diff(param->cur_time_, param->last_time_));
  }
}

int handle_schedule_test(ObContInternal *contp, ObEventType event, void *edata)
{
  UNUSED(event);
  UNUSED(contp);
  TestFuncParam *param;
  param = static_cast<TestFuncParam *>((static_cast<ObEvent *>(edata))->cookie_);
  ++param->seq_no_;
  param->cur_time_ = get_hrtime_internal();

  switch (param->func_type_) {
    case TEST_SCHEDULE_IMM:
    case TEST_PREPARE_SCHEDULE_IMM: {
      TestEventProcessor::check_schedule_imm(param);
      break;
    }
    case TEST_SCHEDULE_IN: {
      TestEventProcessor::check_schedule_in(param);
      break;
    }
    case TEST_SCHEDULE_AT: {
      TestEventProcessor::check_schedule_at(param);
      break;
    }
    case TEST_SCHEDULE_EVERY: {
      TestEventProcessor::check_schedule_every(param);
      break;
    }
    case TEST_SCHEDULE_IMM_SIGNAL: {
      TestEventProcessor::check_schedule_imm_signal(param);
      break;
    }
    case TEST_SCHEDULE: {
      TestEventProcessor::check_schedule(param);
      break;
    }
    case TEST_DO_SCHEDULE: {
      TestEventProcessor::check_do_schedule(param);
      break;
    }
    case TEST_SPAWN_THREAD: {
      TestEventProcessor::check_spawn_thread(param);
      break;
    }
    default: {
      LOG_ERROR("handle_schedule_test, invalid event type");
      break;
    }
  }

#ifdef DEBUG_TEST // print the process
  print_process(param->seq_no_, param->cur_time_, param->last_time_);
#endif

  int ret = OB_ERROR;
  if (TEST_SCHEDULE_EVERY == param->func_type_
      || (EVENT_INTERVAL == param->event_->callback_event_ && 0 != param->event_->period_)) {
    param->last_time_ = param->cur_time_;
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
  TestEventProcessor::check_start((TestFuncParam *)func_param);
  signal_condition(((TestFuncParam *)func_param)->wait_cond_);

  this_ethread()->execute();
  return NULL;
}

void set_fast_signal_true(TestFuncParam *param)
{
  int next = 0;
  param->fast_signal_ = true;
  g_signal_hook_success = false;

  if (event_thread_count[param->event_type_] > 1) {
    next = next_thread[param->event_type_] % event_thread_count[param->event_type_];
  } else {
    next = 0;
  }
  ObEThread *tmp_ethread = g_event_processor.event_thread_[param->event_type_][next];
  tmp_ethread->signal_hook_ = handle_hook_test;
}

TEST_F(TestEventProcessor, eventprocessor_start)
{
  LOG_DEBUG("eventprocessor start");
  pthread_t thread;
  test_param[0] = create_funcparam_test();
  pthread_attr_t *attr_null = NULL;

  if (0 != pthread_create(&thread, attr_null, thread_main_start_test, (void *)test_param[0])) {
    LOG_ERROR("failed to create processor_start");
  } else {
    wait_condition(test_param[0]->wait_cond_);
    ASSERT_TRUE(test_param[0]->test_ok_);
  }
}

TEST_F(TestEventProcessor, eventprocessor_spawn_event_threads)
{
  LOG_DEBUG("eventprocessor spawn_event_threads");
  test_param[0] = create_funcparam_test();

  TestEventProcessor::check_spawn_event_threads(test_param[0]);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEventProcessor, eventprocessor_schedule_imm1)
{
  LOG_DEBUG("eventprocessor schedule_imm");
  test_param[0] = create_funcparam_test(true, handle_schedule_test);
  test_param[0]->event_type_ = ET_CALL;

  TestEventProcessor::check_schedule_imm(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEventProcessor, eventprocessor_schedule_imm2_spawn)
{
  LOG_DEBUG("eventprocessor schedule_imm (spawn)");
  test_param[0] = create_funcparam_test(true, handle_schedule_test, new_proxy_mutex());
  test_param[0]->event_type_ = TEST_ET_SPAWN;

  TestEventProcessor::check_schedule_imm(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEventProcessor, eventprocessor_schedule_at1)
{
  LOG_DEBUG("eventprocessor schedule_at");
  test_param[0] = create_funcparam_test(true, handle_schedule_test);
  test_param[0]->at_delta_ = HRTIME_SECONDS(2);
  test_param[0]->event_type_ = ET_CALL;

  TestEventProcessor::check_schedule_at(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
};

TEST_F(TestEventProcessor, eventprocessor_schedule_at2_minus_spawn)
{
  LOG_DEBUG("eventprocessor schedule_at (-spawn)");
  test_param[0] = create_funcparam_test(true, handle_schedule_test, new_proxy_mutex());
  test_param[0]->at_delta_ = HRTIME_SECONDS(-2);
  test_param[0]->event_type_ = TEST_ET_SPAWN;

  TestEventProcessor::check_schedule_at(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
};

TEST_F(TestEventProcessor, eventprocessor_schedule_in1)
{
  LOG_DEBUG("eventprocessor schedule_in");
  test_param[0] = create_funcparam_test(true, handle_schedule_test);
  test_param[0]->event_type_ = ET_CALL;
  test_param[0]->atimeout_ = TEST_TIME_SECOND_IN;

  TestEventProcessor::check_schedule_in(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
};

TEST_F(TestEventProcessor, eventprocessor_schedule_in2_minus_spawn)
{
  LOG_DEBUG("eventprocessor schedule_in (-spawn)");
  test_param[0] = create_funcparam_test(true, handle_schedule_test, new_proxy_mutex());
  test_param[0]->event_type_ = TEST_ET_SPAWN;
  test_param[0]->atimeout_ = 0 - TEST_TIME_SECOND_IN;

  TestEventProcessor::check_schedule_in(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
};

TEST_F(TestEventProcessor, eventprocessor_schedule_in3_minus_negative_queue)
{
  LOG_DEBUG("eventprocessor schedule_in (-negative_queue)");
  test_param[0] = create_funcparam_test(true, handle_schedule_test);
  test_param[0]->event_type_ = ET_CALL;
  test_param[0]->atimeout_ = 0 - TEST_TIME_SECOND_IN - get_hrtime_internal();

  TestEventProcessor::check_schedule_in(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
};

TEST_F(TestEventProcessor, eventprocessor_schedule_every1)
{
  LOG_DEBUG("eventprocessor schedule_every");
  test_param[0] = create_funcparam_test(true, handle_schedule_test);
  test_param[0]->event_type_ = ET_CALL;
  test_param[0]->aperiod_ = TEST_TIME_SECOND_EVERY;

  TestEventProcessor::check_schedule_every(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
};

TEST_F(TestEventProcessor, eventprocessor_schedule_every2_minus_spawn_negative_queue)
{
  LOG_DEBUG("eventprocessor schedule_every (-spawn negative_queue)");
  test_param[0] = create_funcparam_test(true, handle_schedule_test, new_proxy_mutex());
  test_param[0]->event_type_ = TEST_ET_SPAWN;
  test_param[0]->aperiod_ = 0 - TEST_TIME_SECOND_EVERY;

  TestEventProcessor::check_schedule_every(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
};

TEST_F(TestEventProcessor, eventprocessor_schedule1_in)
{
  LOG_DEBUG("eventprocessor schedule (ET_CALL in)");
  test_param[0] = create_funcparam_test(true, handle_schedule_test, new_proxy_mutex());
  ASSERT_TRUE(NULL != test_param[0]->cont_->mutex_);

  if (NULL == (test_param[0]->event_ = op_reclaim_alloc(ObEvent))) {
    LOG_ERROR("fail to alloc mem for processor_schedule test");
  } else {
    test_param[0]->event_type_ = ET_CALL;
    test_param[0]->event_->callback_event_ = EVENT_INTERVAL;
    test_param[0]->atimeout_ = TEST_TIME_SECOND_IN;
    ASSERT_EQ(common::OB_SUCCESS, test_param[0]->event_->init(*test_param[0]->cont_,
        get_hrtime_internal() + test_param[0]->atimeout_, 0));
    ASSERT_TRUE(test_param[0]->event_->is_inited_);
    test_param[0]->fast_signal_ = false;
    ((ObContInternal *)test_param[0]->cont_)->event_count_++;

    TestEventProcessor::check_schedule(test_param[0]);
    wait_condition(test_param[0]->wait_cond_);
    ASSERT_TRUE(test_param[0]->test_ok_);
  }
};

TEST_F(TestEventProcessor, eventprocessor_schedule2_at)
{
  LOG_DEBUG("eventprocessor schedule (spawn at)");
  test_param[0] = create_funcparam_test(true, handle_schedule_test);

  if (NULL == (test_param[0]->event_ = op_reclaim_alloc(ObEvent))) {
    LOG_ERROR("fail to alloc mem for processor_schedule test");
  } else {
    test_param[0]->event_type_ = TEST_ET_SPAWN;
    test_param[0]->event_->callback_event_ = EVENT_INTERVAL;
    test_param[0]->at_delta_ = HRTIME_SECONDS(2);
    test_param[0]->atimeout_ = TEST_TIME_SECOND_AT(test_param[0]);
    ASSERT_EQ(common::OB_SUCCESS, test_param[0]->event_->init(*test_param[0]->cont_, test_param[0]->atimeout_, 0));
    ASSERT_TRUE(test_param[0]->event_->is_inited_);
    ((ObContInternal *)test_param[0]->cont_)->event_count_++;
    set_fast_signal_true(test_param[0]);

    TestEventProcessor::check_schedule(test_param[0]);
    wait_condition(test_param[0]->wait_cond_);
    ASSERT_TRUE(test_param[0]->test_ok_);
  }
};

TEST_F(TestEventProcessor, eventprocessor_schedule_imm_signal1)
{
  LOG_DEBUG("eventprocessor schedule_imm_signal");
  test_param[0] = create_funcparam_test(true, handle_schedule_test);
  test_param[0]->event_type_ = ET_CALL;

  TestEventProcessor::check_schedule_imm_signal(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
};

TEST_F(TestEventProcessor, eventprocessor_schedule_imm_signal2_spawn)
{
  LOG_DEBUG("eventprocessor schedule_imm_signal (spawn)");
  test_param[0] = create_funcparam_test(true, handle_schedule_test, new_proxy_mutex());
  test_param[0]->event_type_ = TEST_ET_SPAWN;

  TestEventProcessor::check_schedule_imm_signal(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
};

TEST_F(TestEventProcessor, eventprocessor_prepare_schedule_imm1)
{
  LOG_DEBUG("eventprocessor prepare_schedule_imm");
  test_param[0] = create_funcparam_test(true, handle_schedule_test);
  test_param[0]->event_type_ = ET_CALL;

  TestEventProcessor::check_prepare_schedule_imm(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
};

TEST_F(TestEventProcessor, eventprocessor_prepare_schedule_imm2_spawn)
{
  LOG_DEBUG("eventprocessor prepare_schedule_imm (spawn)");
  test_param[0] = create_funcparam_test(true, handle_schedule_test, new_proxy_mutex());
  test_param[0]->event_type_ = TEST_ET_SPAWN;

  TestEventProcessor::check_prepare_schedule_imm(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
};

TEST_F(TestEventProcessor, eventprocessor_do_schedule1_every)
{
  LOG_DEBUG("eventprocessor do_schedule (ET_CALL every)");
  test_param[0] = create_funcparam_test(true, handle_schedule_test);

  //  test_param[0]->func_type_ = TEST_DO_SCHEDULE;
  if (NULL == (test_param[0]->event_ = op_reclaim_alloc(ObEvent))) {
    LOG_ERROR("fail to alloc mem for do_schedule");
  } else {
    test_param[0]->event_->callback_event_ = EVENT_INTERVAL;
    test_param[0]->aperiod_ = TEST_TIME_SECOND_EVERY;
    ((ObContInternal *)test_param[0]->cont_)->event_count_ += TEST_DEFAULT_PERIOD_COUNT;
    ASSERT_EQ(common::OB_SUCCESS, test_param[0]->event_->init(*test_param[0]->cont_,
        get_hrtime_internal() + test_param[0]->aperiod_, test_param[0]->aperiod_));
    ASSERT_TRUE(test_param[0]->event_->is_inited_);
    test_param[0]->event_type_ = ET_CALL;
    test_param[0]->event_->ethread_ = g_event_processor.assign_thread(
        test_param[0]->event_type_);
    if (NULL != test_param[0]->event_->continuation_->mutex_) {
      test_param[0]->event_->mutex_ = test_param[0]->event_->continuation_->mutex_;
    } else {
      test_param[0]->event_->continuation_->mutex_ = test_param[0]->event_->ethread_->mutex_;
      test_param[0]->event_->mutex_ = test_param[0]->event_->continuation_->mutex_;
    }

    TestEventProcessor::check_do_schedule(test_param[0]);
    wait_condition(test_param[0]->wait_cond_);
    ASSERT_TRUE(test_param[0]->test_ok_);
  }
};

TEST_F(TestEventProcessor, eventprocessor_do_schedule2_minus_every)
{
  LOG_DEBUG("eventprocessor do_schedule (spawn every-)");
  test_param[0] = create_funcparam_test(true, handle_schedule_test, new_proxy_mutex());
  ASSERT_TRUE(NULL != test_param[0]->cont_->mutex_);

  if (NULL == (test_param[0]->event_ = op_reclaim_alloc(ObEvent))) {
    LOG_ERROR("fail to alloc mem for do_schedule");
  } else {
    test_param[0]->event_->callback_event_ = EVENT_INTERVAL;
    test_param[0]->aperiod_ = 0 - TEST_TIME_SECOND_EVERY;
    ((ObContInternal *)test_param[0]->cont_)->event_count_ += TEST_DEFAULT_PERIOD_COUNT;
    ASSERT_EQ(common::OB_SUCCESS, test_param[0]->event_->init(*test_param[0]->cont_,
        test_param[0]->aperiod_, test_param[0]->aperiod_));
    ASSERT_TRUE(test_param[0]->event_->is_inited_);
    test_param[0]->event_type_ = TEST_ET_SPAWN;
    test_param[0]->event_->ethread_ = g_event_processor.assign_thread(
        test_param[0]->event_type_);
    if (NULL != test_param[0]->event_->continuation_->mutex_) {
      test_param[0]->event_->mutex_ = test_param[0]->event_->continuation_->mutex_;
    } else {
      test_param[0]->event_->continuation_->mutex_ = test_param[0]->event_->ethread_->mutex_;
      test_param[0]->event_->mutex_ = test_param[0]->event_->continuation_->mutex_;
    }

    set_fast_signal_true(test_param[0]);
    ((ObContInternal *)(test_param[0]->cont_))->deletable_ = true;
    TestEventProcessor::check_do_schedule(test_param[0]);
    wait_condition(test_param[0]->wait_cond_);
    ASSERT_TRUE(test_param[0]->test_ok_);
  }
};

TEST_F(TestEventProcessor, eventprocessor_assign_thread)
{
  LOG_DEBUG("eventprocessor _assign_thread");
  test_param[0] = create_funcparam_test();

  TestEventProcessor::check_assign_thread(test_param[0]);
  ASSERT_TRUE(test_param[0]->test_ok_);
};

TEST_F(TestEventProcessor, eventprocessor_allocate)
{
  LOG_DEBUG("eventprocessor allocate");
  test_param[0] = create_funcparam_test();

  TestEventProcessor::check_allocate(test_param[0]);
  ASSERT_TRUE(test_param[0]->test_ok_);
};

TEST_F(TestEventProcessor, eventprocessor_spawn_thread1_0)
{
  LOG_DEBUG("eventprocessor spawn_thread (0)");
  test_param[0] = create_funcparam_test(true, handle_schedule_test);
  test_param[0]->stacksize_ = 0;

  TestEventProcessor::check_spawn_thread(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
};

TEST_F(TestEventProcessor, eventprocessor_spawn_thread2_default)
{
  LOG_DEBUG("eventprocessor spawn_thread (default)");
  test_param[0] = create_funcparam_test(true, handle_schedule_test, new_proxy_mutex());
  test_param[0]->stacksize_ = DEFAULT_STACKSIZE;

  TestEventProcessor::check_spawn_thread(test_param[0]);
  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
};

TEST_F(TestEventProcessor, processor)
{
  LOG_DEBUG("processor");
  ObTasksProcessor *task_processor = NULL;
  test_param[0] = create_funcparam_test();

  if (NULL == (test_param[0]->processor_ = new(std::nothrow) ObProcessor())) {
    LOG_ERROR("failed to create ObProcessor");
  } else {
    ASSERT_TRUE(0 == test_param[0]->processor_->get_thread_count());
    ASSERT_TRUE(0 == test_param[0]->processor_->start(0));
    test_param[0]->processor_->shutdown();
    delete test_param[0]->processor_;
    test_param[0]->processor_ = NULL;
  }

  if (NULL == (task_processor = new(std::nothrow) ObTasksProcessor())) {
    LOG_ERROR("failed to create ObTasksProcessor");
  } else {
    test_param[0]->processor_ = static_cast<ObProcessor *>(task_processor);
    ASSERT_TRUE(OB_INVALID_ARGUMENT == task_processor->start(-10));
    ASSERT_TRUE(OB_INVALID_ARGUMENT == task_processor->start(0));
    ASSERT_TRUE(OB_SUCCESS == task_processor->start(1));
    delete task_processor;
    test_param[0]->processor_ = NULL;
  }

  g_event_processor.shutdown();//do nothing
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

