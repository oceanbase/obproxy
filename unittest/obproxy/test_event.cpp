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
#include "obproxy/iocore/eventsystem/ob_event.h"
#include "obproxy/iocore/eventsystem/ob_action.h"

namespace oceanbase
{
namespace obproxy
{
using namespace proxy;
using namespace common;
using namespace event;

#define SCHEDULE_MULTI                                                              \
{                                                                                   \
  test_param[0] = create_funcparam_test(true, handle_schedule_test, new_proxy_mutex());  \
  test_param[0]->common_ethread_ = true;                                                 \
  test_param[1] = create_funcparam_test(true, handle_schedule_test, new_proxy_mutex());  \
  test_param[1]->common_ethread_ = true;                                                 \
  schedule_init_common(test_param[0], HRTIME_SECONDS(1));                                \
  schedule_init_common(test_param[1], HRTIME_SECONDS(2));                                \
  test_param[0]->event_neighbor_ = test_param[1]->event_;                                     \
}

enum TestEventFuncType {
  TEST_NULL = 0,
  TEST_SCHEDULE_IMM = 1,
  TEST_SCHEDULE_AT,
  TEST_SCHEDULE_IN ,
  TEST_SCHEDULE_EVERY,
  TEST_SCHEDULE,
  TEST_OBEVENT,
  TEST_FREE,
  TEST_INIT,
};

unsigned int schedule_type = TEST_SCHEDULE_IMM;
int32_t test_last_thread = 0;

class TestEvent : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown();
  static void check_schedule_common(TestFuncParam *param,
      int32_t acallback_event = EVENT_INTERVAL);
  static void check_schedule_imm(TestFuncParam *param);
  static void check_schedule_at(TestFuncParam *param);
  static void check_schedule_in(TestFuncParam *param);
  static void check_schedule_every(TestFuncParam *param);

public:
  TestFuncParam *test_param[MAX_PARAM_ARRAY_SIZE];
};

void TestEvent::SetUp()
{
  for (int i = 0; i < MAX_PARAM_ARRAY_SIZE; ++i) {
    test_param[i] = NULL;
  }
}

void TestEvent::TearDown()
{
  for (int i = 0; i < MAX_PARAM_ARRAY_SIZE; ++i) {
    if (NULL != test_param[i]) {
      destroy_funcparam_test(test_param[i]);
    } else {}
  }
}

void schedule_init_common(TestFuncParam *param, ObHRTime atimeout = TEST_TIME_SECOND_IN);

inline void TestEvent::check_schedule_common(TestFuncParam *param, int32_t acallback_event)
{
  ASSERT_TRUE(acallback_event== param->event_->callback_event_);
  ASSERT_TRUE(0 == param->event_->in_the_priority_queue_);
  ASSERT_TRUE(param->cont_->mutex_ == param->event_->mutex_);
  ASSERT_TRUE(1 == param->event_->in_the_prot_queue_);
}

void TestEvent::check_schedule_imm(TestFuncParam *param)
{
  if (!param->started_){
    param->started_ = true;
    ((ObContInternal *)param->cont_)->event_count_ = 1;
    param->last_time_ = get_hrtime_internal();
    param->event_->schedule_imm(EVENT_IMMEDIATE);

    ASSERT_TRUE(0 == param->event_->timeout_at_);
    ASSERT_TRUE(0 == param->event_->period_);
    ASSERT_TRUE(EVENT_IMMEDIATE== param->event_->callback_event_);
    ASSERT_TRUE(0 == param->event_->in_the_priority_queue_);
    ASSERT_TRUE(param->cont_->mutex_ == param->event_->mutex_);
  } else {
    param->test_ok_ = check_imm_test_ok(hrtime_diff(param->cur_time_, param->last_time_));
  }
}

void TestEvent::check_schedule_at(TestFuncParam *param)
{
  if (!param->started_) {
    param->started_ = true;
    ((ObContInternal *)param->cont_)->event_count_ = 1;
    param->last_time_ = get_hrtime_internal();
    param->event_->schedule_at(TEST_TIME_SECOND_AT(param), EVENT_INTERVAL);

    ASSERT_TRUE(param->event_->timeout_at_ <= TEST_TIME_SECOND_AT(param));
    ASSERT_TRUE(0 == param->event_->period_);
    check_schedule_common(param);
  } else {
    check_test_ok(param, param->at_delta_);
    //  param->at_delta_ <= 0,//at the past point,equal to imm
    //  param->at_delta_ >  0,//at the future point, equal to in
  }
}

void TestEvent::check_schedule_in(TestFuncParam *param)
{
  if (!param->started_) {
    param->started_ = true;
    ((ObContInternal *)param->cont_)->event_count_ = 1;
    param->last_time_ = get_hrtime_internal();
    param->event_->schedule_in(param->atimeout_, EVENT_INTERVAL);

    ASSERT_TRUE(param->event_->timeout_at_ > param->atimeout_);
    ASSERT_TRUE(param->event_->timeout_at_ <= (param->atimeout_ + get_hrtime_internal()));
    ASSERT_TRUE(0 == param->event_->period_);
    check_schedule_common(param);

    if (param->event_->timeout_at_ < 0) {//drop into poll queue, can not update deletable_
      ((ObContInternal *)(param->cont_))->deletable_ = true;
    } else {}
  } else {
    check_test_ok(param, param->atimeout_);
    //param->atimeout_ <= 0 //in the past point,equal to imm
  }
}

void TestEvent::check_schedule_every(TestFuncParam *param)
{
  if (!param->started_) {
    param->started_ = true;
    ((ObContInternal *)param->cont_)->event_count_ = TEST_DEFAULT_PERIOD_COUNT;
    param->last_time_ = get_hrtime_internal();
    param->period_count_ = TEST_DEFAULT_PERIOD_COUNT;
    param->event_->schedule_every(param->aperiod_, EVENT_INTERVAL);

    SCHEDULE_EVERY_FIRST_CHECK;
    if (param->aperiod_ < 0) {//drop into poll queue, can not update deletable_
      ((ObContInternal *)(param->cont_))->deletable_ = true;
    } else {}
  } else {
    param->test_ok_ = param->test_ok_ && check_test_ok(param, param->aperiod_);
    //param->aperiod_ < 0 ;//equal to schedule at, will fall into negative_queue
  }
}

int handle_schedule_test(ObContInternal *contp, ObEventType event, void* edata)
{
  UNUSED(event);
  UNUSED(contp);
  TestFuncParam *param = NULL;
  TestFuncParam *param2 = NULL;
  param = static_cast<TestFuncParam *>((static_cast<ObEvent *>(edata))->cookie_);
  ++param->seq_no_;
  param->cur_time_ = get_hrtime_internal();

#ifdef DEBUG_TEST // print the process
  printf("func_type:%u; ", param->func_type_);
  print_process(param->seq_no_, param->cur_time_, param->last_time_);
#endif

  if (TEST_NULL == param->func_type_) {
    if (param->common_ethread_) {
      param->test_ok_ = true;
      param2 = static_cast<TestFuncParam *>(param->event_neighbor_->cookie_);
      signal_condition(param->wait_cond_);
      param = param2;
    }
    param->func_type_ = schedule_type;
    param->started_ = false;
    param->seq_no_ = 0;
  } else {}

  switch (param->func_type_) {
  case TEST_NULL: {
      param->test_ok_ = true;
      break;
    }
  case TEST_SCHEDULE_IMM: {
      TestEvent::check_schedule_imm(param);
      break;
    }
  case TEST_SCHEDULE_IN: {
      TestEvent::check_schedule_in(param);
      break;
    }
  case TEST_SCHEDULE_AT: {
      TestEvent::check_schedule_at(param);
      break;
    }
  case TEST_SCHEDULE_EVERY: {
      TestEvent::check_schedule_every(param);
      break;
    }
  default: {
      LOG_ERROR("handle_schedule_test, invalid event type");
      break;
    }
  }


  int ret = OB_ERROR;

  if (0 != param->seq_no_) {
    if (TEST_SCHEDULE_EVERY == param->func_type_) {
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
  } else {}

  return ret;
}

void schedule_init_common(TestFuncParam *param, ObHRTime atimeout)
{
  param->started_ = true;
  param->func_type_ = TEST_NULL;
  if (param->common_ethread_) {
    int64_t cur_thread = (test_last_thread + 1) % g_event_processor.thread_count_for_type_[ET_CALL];
    param->ethread_ = g_event_processor.event_thread_[ET_CALL][cur_thread];
  } else {
    test_last_thread = g_event_processor.next_thread_for_type_[ET_CALL];
    param->ethread_ = g_event_processor.assign_thread(ET_CALL);
  }
  ASSERT_TRUE(NULL != param->ethread_);
  param->cookie_ = param;
  ((ObContInternal *)param->cont_)->event_count_ = 1;
  param->last_time_ = get_hrtime_internal();
  param->event_ = param->ethread_->schedule_in_local(param->cont_,
      atimeout, EVENT_INTERVAL, param->cookie_);
  ASSERT_TRUE(NULL != param->event_);
  ASSERT_TRUE(1 == param->event_->in_the_prot_queue_);
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

TEST_F(TestEvent, ObAction_test)
{
  LOG_DEBUG("ObAction_test");
  ObAction *action = NULL;
  ObContinuation *cont = NULL;
  test_param[0] = create_funcparam_test();

  if (NULL == (action = new(std::nothrow) ObAction)) {
    LOG_ERROR("fail to alloc mem for ObAction");
  } else {
    test_param[0]->action_ = action;
    ASSERT_TRUE(NULL == action->continuation_);
    ASSERT_TRUE(NULL == action->mutex_);
    ASSERT_FALSE(action->cancelled_);

    if (NULL == (cont = new(std::nothrow) ObContinuation(new_proxy_mutex()))) {
      LOG_ERROR("fail to alloc mem for ObContinuation");
    } else {
      test_param[0]->cont_ = cont;
      action->set_continuation(NULL);
      ASSERT_TRUE(NULL == action->continuation_);
      ASSERT_TRUE(NULL == action->mutex_);

      action->set_continuation(cont);
      ASSERT_TRUE(cont == action->continuation_);
      ASSERT_TRUE(cont->mutex_ == action->mutex_);

      action->cancelled_ = false;
      action->cancel();
      ASSERT_TRUE(action->cancelled_);
      delete cont;
      test_param[0]->cont_ = NULL;
      delete action;
      test_param[0]->action_ = NULL;
      action = NULL;
    }
  }
}

TEST_F(TestEvent, g_event_processor_start)
{
  LOG_DEBUG("g_event_processor START");
  pthread_t thread;
  test_param[0] = create_funcparam_test();
  pthread_attr_t *attr_null = NULL;

  if (0 != pthread_create(&thread, attr_null, thread_main_start_test, (void *)test_param[0])) {
    LOG_ERROR("failed to create thread_main_start_test");
  } else {
    wait_condition(test_param[0]->wait_cond_);
    ASSERT_TRUE(test_param[0]->test_ok_);
  }
}

TEST_F(TestEvent, ObEvent_null_init_free)
{
  LOG_DEBUG("ObEvent_null_init_free");
  bool test_ok = false;
  ObEvent *event = NULL;
  ObHRTime atimeout_at = HRTIME_SECONDS(2);
  ObHRTime aperiod = HRTIME_SECONDS(2);
  ObContinuation *cont = NULL;
  test_param[0] = create_funcparam_test();

  if (NULL == (event = op_reclaim_alloc(ObEvent))) {
    LOG_ERROR("fail to alloc mem for ObEvent");
  } else if (NULL == (cont = new(std::nothrow) ObContinuation)) {
    LOG_ERROR("fail to alloc mem for ObContinuation");
    event->free();
  } else {
    test_param[0]->test_event_ = event;
    test_param[0]->cont_ = cont;
    //test constructor
    ASSERT_TRUE(NULL == event->ethread_);
    ASSERT_FALSE(event->in_the_prot_queue_);
    ASSERT_FALSE(event->in_the_priority_queue_);
    ASSERT_FALSE(event->in_heap_);
    ASSERT_TRUE(EVENT_NONE == event->callback_event_);
    ASSERT_TRUE(0 == event->timeout_at_);
    ASSERT_TRUE(0 == event->period_);
    ASSERT_TRUE(NULL == event->cookie_);
#ifdef ENABLE_TIME_TRACE
    ASSERT_TRUE(0 == event->start_time_);
#endif
    //test init
    event->cancelled_ = true;
    ASSERT_EQ(common::OB_SUCCESS, event->init(*cont, atimeout_at, aperiod));
    ASSERT_TRUE(cont == event->continuation_);
    ASSERT_TRUE(atimeout_at == event->timeout_at_);
    ASSERT_TRUE(aperiod == event->period_);
    ASSERT_FALSE(event->cancelled_);
    delete cont;
    test_param[0]->cont_ = NULL;

    //test free
    event->mutex_ = new_proxy_mutex();
    ASSERT_TRUE(NULL != event->mutex_);
    event->free();
    test_param[0]->test_event_ = NULL;
    ASSERT_TRUE(NULL == event->mutex_);
    test_ok = true;
  }
  ASSERT_TRUE(test_ok);
}

TEST_F(TestEvent, schedule_imm1)
{
  LOG_DEBUG("schedule_imm");
  test_param[0] = create_funcparam_test(true, handle_schedule_test, new_proxy_mutex());
  schedule_init_common(test_param[0]);
  schedule_type = TEST_SCHEDULE_IMM;

  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEvent, schedule_imm2_multi)
{
  LOG_DEBUG("schedule_imm multi");
  SCHEDULE_MULTI;
  schedule_type = TEST_SCHEDULE_IMM;

  for (int i = 0; i < 2; ++i) {
    wait_condition(test_param[i]->wait_cond_);
    ASSERT_TRUE(test_param[i]->test_ok_);
  }
}

TEST_F(TestEvent, schedule_at1)
{
  LOG_DEBUG("schedule_at");
  test_param[0] = create_funcparam_test(true, handle_schedule_test, new_proxy_mutex());
  test_param[0]->at_delta_ = HRTIME_SECONDS(2);
  schedule_type = TEST_SCHEDULE_AT;
  schedule_init_common(test_param[0]);

  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEvent, schedule_at2_minus_negative_queue)
{
  LOG_DEBUG("schedule_at -negative_queue");
  test_param[0] = create_funcparam_test(true, handle_schedule_test, new_proxy_mutex());
  test_param[0]->at_delta_ = HRTIME_SECONDS(-2);
  schedule_type = TEST_SCHEDULE_AT;
  schedule_init_common(test_param[0]);

  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEvent, schedule_at3_multi)
{
  LOG_DEBUG("schedule_at multi");
  SCHEDULE_MULTI;
  schedule_type = TEST_SCHEDULE_AT;
  test_param[1]->at_delta_ = HRTIME_SECONDS(2);

  for (int i = 0; i < 2; ++i) {
    wait_condition(test_param[i]->wait_cond_);
    ASSERT_TRUE(test_param[i]->test_ok_);
  }
}

TEST_F(TestEvent, schedule_in1)
{
  LOG_DEBUG("schedule_in");
  test_param[0] = create_funcparam_test(true, handle_schedule_test, new_proxy_mutex());
  test_param[0]->atimeout_ = TEST_TIME_SECOND_IN;
  schedule_type = TEST_SCHEDULE_IN;
  schedule_init_common(test_param[0]);

  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEvent, schedule_in2_minus)
{
  LOG_DEBUG("schedule_in -");
  test_param[0] = create_funcparam_test(true, handle_schedule_test, new_proxy_mutex());
  test_param[0]->atimeout_ = 0 - TEST_TIME_SECOND_IN;
  schedule_type = TEST_SCHEDULE_IN;
  schedule_init_common(test_param[0]);

  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEvent, schedule_in3_minus_negative_queue)
{
  LOG_DEBUG("schedule_in -negative_queue");
  test_param[0] = create_funcparam_test(true, handle_schedule_test, new_proxy_mutex());
  test_param[0]->atimeout_ = 0 - TEST_TIME_SECOND_IN - get_hrtime_internal();
  schedule_type = TEST_SCHEDULE_IN;
  schedule_init_common(test_param[0]);

  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEvent, schedule_in4_multi)
{
  LOG_DEBUG("schedule_in multi");
  SCHEDULE_MULTI;
  schedule_type = TEST_SCHEDULE_IN;
  test_param[1]->atimeout_ = TEST_TIME_SECOND_IN;

  for (int i = 0; i < 2; ++i) {
    wait_condition(test_param[i]->wait_cond_);
    ASSERT_TRUE(test_param[i]->test_ok_);
  }
}

TEST_F(TestEvent, schedule_every1)
{
  LOG_DEBUG("schedule_every");
  test_param[0] = create_funcparam_test(true, handle_schedule_test, new_proxy_mutex());
  test_param[0]->aperiod_ = TEST_TIME_SECOND_EVERY;
  schedule_type = TEST_SCHEDULE_EVERY;
  schedule_init_common(test_param[0]);

  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEvent, schedule_every2_minus_negative_queue)
{
  LOG_DEBUG("schedule_every - negative_queue");
  test_param[0] = create_funcparam_test(true, handle_schedule_test, new_proxy_mutex());
  test_param[0]->aperiod_ = 0 - TEST_TIME_SECOND_EVERY;
  schedule_type = TEST_SCHEDULE_EVERY;
  schedule_init_common(test_param[0]);

  wait_condition(test_param[0]->wait_cond_);
  ASSERT_TRUE(test_param[0]->test_ok_);
}

TEST_F(TestEvent, schedule_every3_multi)
{
  LOG_DEBUG("schedule_every multi");
  SCHEDULE_MULTI;
  schedule_type = TEST_SCHEDULE_EVERY;
  test_param[1]->aperiod_ = TEST_TIME_SECOND_EVERY;

  for (int i = 0; i < 2; ++i) {
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
