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

#include <gtest/gtest.h>
#include <pthread.h>
#define private public
#define protected public
#include "test_eventsystem_api.h"
#include "ob_protected_queue.h"

namespace oceanbase
{
namespace obproxy
{
using namespace proxy;
using namespace common;
using namespace event;

int64_t signal_hook_result = 0;

#define MAX_EVENT_CREATE_NUM        2*DEFALUT_CHANGE_COOKIE_SIZE
#define DEFALLT_ETHREAD_NUM         4

class TestProtectedQueue : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown();
  static void check_dequeue_al(ObEThread *cur_ethread, ObEvent *event = NULL);
  static void check_al_is_empty(ObProtectedQueue *tmp_queue);
  static void check_null_inserting_thread_empty(TestFuncParam *param);
  static void check_null_inserting_thread_not_empty(TestFuncParam *param);
  static void check_enqueue_not_regular(ObEThread *e_ethread, TestFuncParam *tmp_param);
  static void check_enqueue_regular(ObEvent *event, ObEThread *inserting_thread,
      int64_t seq_no);
  static void check_flush_signals_handle(ObEThread *cur_ethread, bool test_ok);
  void fill_ethreads_zero_queue(ObEvent **array_event, int64_t event_seq[]);
  void common_enqueue_not_regular(int64_t event_seq[]);
  void common_enqueue_regular(int64_t e_ethread_seq[]);
  void common_flush_signals(int64_t array_seq[], bool check_one = false);
  void check_dequeue_timed(ObProtectedQueue *tmp_queue);
  void dequeue_timed_exception(ObHRTime timeout);

public:
  TestFuncParam *test_param_;
  ObProtectedQueue *protected_queue_;
  ObEvent *event_array_[MAX_EVENT_CREATE_NUM];
};

int signal_hook_enqueue(ObEThread &ethread);
int signal_hook_simple(ObEThread &ethread);
void *thread_enqueue_cont_wait(void *func_param);
void *thread_null_inserting_thread(void *data);
void *thread_fill_al_by_event_array(void *this_test);
void *thread_fill_al_cancelled(void *protected_queue);
void fill_al_by_event_array(TestProtectedQueue *this_test);
TestFuncParam *common_handle_continuation(void* edata);
int handle_enqueue_not_regular_continuation(ObContInternal *contp, ObEventType aevent,
    void* edata);
int handle_enqueue_regular_continuation(ObContInternal *contp, ObEventType aevent,
    void* edata);
int handle_flush_signals_one_continuation(ObContInternal *contp, ObEventType aevent,
    void* edata);
int handle_flush_signals_continuation(ObContInternal *contp, ObEventType aevent,
    void* edata);
void init_g_event_processor();

void TestProtectedQueue::SetUp()
{
  test_param_ = NULL;
  if (NULL == (protected_queue_ = new(std::nothrow) ObProtectedQueue())) {
      LOG_ERROR("fail to alloc mem for ObProtectedQueue");
      ASSERT_TRUE(false);
    } else if (OB_SUCCESS != protected_queue_->init()) {
      LOG_ERROR("fail to init ObProtectedQueue");
      ASSERT_TRUE(false);
    }
  for (int64_t i = 0; i < MAX_EVENT_CREATE_NUM; ++i) {
    if (NULL == (event_array_[i] = op_reclaim_alloc(ObEvent))) {
      LOG_ERROR("fail to alloc mem for ObEvent");
      ASSERT_TRUE(false);
    }
  }
}

void TestProtectedQueue::TearDown()
{
  if (NULL != test_param_) {
    destroy_funcparam_test(test_param_);
  } else {}

  if (NULL != protected_queue_) {
    ObEvent *e = (ObEvent *)protected_queue_->atomic_list_.popall();
    SLL<ObEvent, ObEvent::Link_link_> t;
    t.head_ = e;
    while (NULL != (e = t.pop())) {
      e->mutex_ = NULL;
      op_reclaim_free(e);
    }
    while (NULL != (e = protected_queue_->local_queue_.dequeue())) {
      e->mutex_ = NULL;
      op_reclaim_free(e);
    }
    mutex_destroy(&protected_queue_->lock_);
    cond_destroy(&protected_queue_->might_have_data_);
    delete protected_queue_;
  } else {}

  for (int64_t i = 0; i < MAX_EVENT_CREATE_NUM; ++i) {
    if (NULL != (event_array_[i])) {
      event_array_[i]->free();
      event_array_[i] = NULL;
    }
  }
  for (int64_t i = 0; i < g_event_processor.event_thread_count_; ++i) {
    g_event_processor.all_event_threads_[i]->signal_hook_ = NULL;
  }
}

void TestProtectedQueue::check_al_is_empty(ObProtectedQueue *tmp_queue)
{
  ASSERT_TRUE(tmp_queue->atomic_list_.empty());
}

void TestProtectedQueue::check_dequeue_al(ObEThread *cur_ethread, ObEvent *event)
{
  ObEvent *tmp_event = (ObEvent *)(cur_ethread->event_queue_external_.atomic_list_.pop());
  if (NULL != tmp_event) {
    ASSERT_TRUE(tmp_event == event);
    ASSERT_EQ(1, event->in_the_prot_queue_);
    event->in_the_prot_queue_ = 0;
  } else {}
}

void TestProtectedQueue::check_dequeue_timed(ObProtectedQueue *tmp_queue)
{
  ASSERT_TRUE(tmp_queue->atomic_list_.empty());
  ASSERT_FALSE(tmp_queue->local_queue_.empty());
  for (int64_t i = 0; i < MAX_EVENT_CREATE_NUM; ++i) {
    ASSERT_EQ(1, event_array_[i]->in_the_prot_queue_);
    ObEvent *tmp_event = tmp_queue->dequeue_local();
    ASSERT_TRUE(event_array_[i] == tmp_event);
    ASSERT_EQ(0, event_array_[i]->in_the_prot_queue_);
    ASSERT_TRUE(NULL != event_array_[i]->mutex_);
  }
  ASSERT_TRUE(tmp_queue->local_queue_.empty());
}

void TestProtectedQueue::fill_ethreads_zero_queue(ObEvent **array_event,
    int64_t event_seq[])
{
//  make all_event_threads_[0]->event_queue_external_.al_ have events
  ObProtectedQueue *tmp_queue = &(g_event_processor.all_event_threads_[0]->event_queue_external_);
  ASSERT_TRUE(tmp_queue->atomic_list_.empty());
  for (int i = 1 ; i < DEFALUT_CHANGE_COOKIE_SIZE; ++i) {
    tmp_queue->enqueue(array_event[event_seq[i]]);
    ASSERT_EQ(1, array_event[event_seq[i]]->in_the_prot_queue_);
  }
  ASSERT_FALSE(tmp_queue->atomic_list_.empty());
}

void TestProtectedQueue::check_null_inserting_thread_empty(TestFuncParam *param)
{
  ASSERT_EQ(1, param->event_->in_the_prot_queue_);//check enqueue
  ASSERT_TRUE(g_signal_hook_success);
  ASSERT_TRUE(param->test_ok_);
}

void TestProtectedQueue::check_null_inserting_thread_not_empty(TestFuncParam *param)
{
  ASSERT_EQ(1, param->test_event_->in_the_prot_queue_);
  ASSERT_FALSE(g_signal_hook_success);
}

void TestProtectedQueue::common_enqueue_not_regular(int64_t event_seq[])
{
//  inserting_thread != e_ethread, NULL != inserting_thread,
//  NULL == ethreads_to_be_signalled_
  char thr_name[MAX_THREAD_NAME_LENGTH];
  test_param_ = create_funcparam_test(true, handle_enqueue_not_regular_continuation,
      new_proxy_mutex());
  for (int i = 0 ; i < DEFALUT_CHANGE_COOKIE_SIZE; ++i) {
    test_param_->array_seq_[i] = event_seq[i];
    test_param_->change_cookie_[i] = (void *)(event_array_[event_seq[i]]);
  }
  fill_ethreads_zero_queue(event_array_, event_seq);
  snprintf(thr_name, MAX_THREAD_NAME_LENGTH, "[TEST_ENQUEUE]");
  ((ObContInternal *)test_param_->cont_)->event_count_++;
//  dedicated ethread, so ethreads_to_be_signalled_ is null
  test_param_->event_ = g_event_processor.spawn_thread(test_param_->cont_, thr_name);
  test_param_->event_->cookie_ = test_param_;

  wait_condition(test_param_->wait_cond_);
}

void TestProtectedQueue::check_enqueue_not_regular(ObEThread *e_ethread,
    TestFuncParam *tmp_param)
{
  ASSERT_TRUE(tmp_param->test_ok_);
  ObEThread *inserting_thread = this_ethread();//not_regular
  ASSERT_TRUE(e_ethread->event_queue_external_.atomic_list_.empty());
  ASSERT_FALSE(inserting_thread->event_queue_external_.atomic_list_.empty());
  for (int i = 1; i < DEFALUT_CHANGE_COOKIE_SIZE; ++i) {
    TestProtectedQueue::check_dequeue_al(inserting_thread,
        (ObEvent *)(tmp_param->change_cookie_[i]));
  }
  TestProtectedQueue::check_dequeue_al(inserting_thread,
      (ObEvent *)(tmp_param->change_cookie_[0]));
  ASSERT_TRUE(inserting_thread->event_queue_external_.atomic_list_.empty());
}

void TestProtectedQueue::common_enqueue_regular(int64_t e_ethread_seq[])
{
//  inserting_thread != e_ethread, NULL != inserting_thread,
//  NULL != ethreads_to_be_signalled_, so it is neighbor ethread
  ObEThread *cur_ethread = NULL;
  cur_ethread = g_event_processor.all_event_threads_[g_event_processor.event_thread_count_ - 1];
  test_param_ = create_funcparam_test(true, handle_enqueue_regular_continuation,
      new_proxy_mutex());
  for (int i = 0; i < DEFALUT_CHANGE_COOKIE_SIZE ; ++i) {//set different e_ethread
    test_param_->change_cookie_[i] = (void *)(event_array_[i]);
    test_param_->array_seq_[i] = e_ethread_seq[i];
  }
  test_param_->ethread_ = cur_ethread;
  ((ObContInternal *)test_param_->cont_)->event_count_++;
  test_param_->ethread_->schedule_imm_local(test_param_->cont_, EVENT_IMMEDIATE,
      test_param_);
  wait_condition(test_param_->wait_cond_);
}

void TestProtectedQueue::check_enqueue_regular(ObEvent *event,
    ObEThread *inserting_thread, int64_t seq_no)
{
  ObEThread * e_ethread = event->ethread_;
  ASSERT_TRUE(g_event_processor.all_event_threads_[g_event_processor.event_thread_count_ - 1]
      == inserting_thread);
  ASSERT_EQ(1, event->in_the_prot_queue_);
  ASSERT_TRUE(g_signal_hook_success);

  if (inserting_thread->ethreads_to_be_signalled_count_ < g_event_processor.event_thread_count_) {
    ASSERT_EQ(seq_no, inserting_thread->ethreads_to_be_signalled_count_);
    ASSERT_TRUE(e_ethread == inserting_thread->ethreads_to_be_signalled_[seq_no - 1]);
  } else {
    ASSERT_EQ(g_event_processor.event_thread_count_, inserting_thread->ethreads_to_be_signalled_count_);
    ASSERT_TRUE(e_ethread == inserting_thread->ethreads_to_be_signalled_[e_ethread->id_]);
    ASSERT_TRUE(NULL == inserting_thread->ethreads_to_be_signalled_[inserting_thread->id_]);
  }
}

void TestProtectedQueue::common_flush_signals(int64_t array_seq[], bool check_one)
{
  ObEThread *cur_ethread = NULL;
  cur_ethread = g_event_processor.all_event_threads_[g_event_processor.event_thread_count_ - 1];
  if (check_one) {
    test_param_ = create_funcparam_test(true, handle_flush_signals_one_continuation,
        new_proxy_mutex());
    for (int i = 0 ; i < DEFALUT_CHANGE_COOKIE_SIZE; ++i) {
      test_param_->array_seq_[i] = array_seq[i];
      test_param_->change_cookie_[i] = (void *)(event_array_[array_seq[i]]);
    }
    fill_ethreads_zero_queue(event_array_, array_seq);
  } else {
    test_param_ = create_funcparam_test(true, handle_flush_signals_continuation,
        new_proxy_mutex());
    for (int i = 0; i < DEFALUT_CHANGE_COOKIE_SIZE ; ++i) {
      test_param_->change_cookie_[i] = (void *)(event_array_[i]);
      test_param_->array_seq_[i] = array_seq[i];
    }
  }
  test_param_->ethread_ = cur_ethread;
  ((ObContInternal *)test_param_->cont_)->event_count_++;
  test_param_->ethread_->schedule_imm_local(test_param_->cont_, EVENT_IMMEDIATE,
      test_param_);
  wait_condition(test_param_->wait_cond_);
  sleep(1);
}

void TestProtectedQueue::check_flush_signals_handle(ObEThread *cur_ethread, bool test_ok)
{
  ASSERT_TRUE(test_ok);
  ASSERT_EQ(0, cur_ethread->ethreads_to_be_signalled_count_);
  for (int i = 0; i < g_event_processor.event_thread_count_; ++i) {
    ASSERT_TRUE(NULL == cur_ethread->ethreads_to_be_signalled_[i]);
  }
  ASSERT_TRUE(g_event_processor.all_event_threads_[0]->event_queue_external_.atomic_list_.empty());
}

void TestProtectedQueue::dequeue_timed_exception(ObHRTime timeout)
{
  ASSERT_TRUE(protected_queue_->atomic_list_.empty());
  ASSERT_TRUE(protected_queue_->local_queue_.empty());
  ObHRTime last_time = get_hrtime_internal();
  protected_queue_->dequeue_timed(timeout, true);
  ASSERT_TRUE(check_imm_test_ok(get_hrtime_internal() - last_time));
}

void init_g_event_processor()
{
  init_event_system(EVENT_SYSTEM_MODULE_VERSION);
  if (OB_SUCCESS != g_event_processor.start(DEFALLT_ETHREAD_NUM, DEFAULT_STACKSIZE, false, false)) {
    LOG_ERROR("failed to START g_event_processor");
  } else {
    //do not go into execute() loop
    LOG_ERROR("g_event_processor START success");
  }
}

int signal_hook_enqueue(ObEThread &ethread)
{
  ObEThread *inserting_thread = this_ethread();
  if (NULL != inserting_thread && !ethread.event_queue_external_.atomic_list_.empty()) {
    ObEvent *event = NULL;
    while (NULL != (event= (ObEvent *)(ethread.event_queue_external_.atomic_list_.pop()))) {
      event->in_the_prot_queue_ = 0;
      inserting_thread->event_queue_external_.enqueue(event);
    }
  } else {}
  LOG_DEBUG("signal_hook_enqueue success");
  return 0;
}

int signal_hook_simple(ObEThread &ethread)
{
  UNUSED(ethread);
  g_signal_hook_success = true;
  return 0;
}

void *thread_enqueue_cont_wait(void *func_param)
{
  int ret = 0;
  TestFuncParam *tmp_param = static_cast<TestFuncParam *>(func_param);
  ObProtectedQueue *tmp_queue = NULL;
  tmp_queue = &(tmp_param->ethread_->event_queue_external_);
  tmp_param->test_ok_ = false;

  if (OB_FAIL(mutex_acquire(&tmp_queue->lock_))) {
    LOG_ERROR("fail to acquire mutex");
  } else {
    timespec ts = hrtime_to_timespec(get_hrtime_internal() + HRTIME_SECONDS(2));
    cond_timedwait(&tmp_queue->might_have_data_, &tmp_queue->lock_, &ts);
    tmp_param->test_ok_ = true;
    if (OB_FAIL(mutex_release(&tmp_queue->lock_))) {
      LOG_ERROR("fail to release mutex");
    }
  }
  return NULL;
}

void *thread_null_inserting_thread(void *data)
{
  ObThreadId tid;
  ObEThread *cur_ethread = g_event_processor.all_event_threads_[0];
  cur_ethread->signal_hook_ = signal_hook_simple;
  TestFuncParam *param = (TestFuncParam *)data;
  param->event_->ethread_ = cur_ethread;
  TestFuncParam *tmp_param = create_funcparam_test();//destroyed in teardown
  tmp_param->ethread_ = cur_ethread;

// enqueue: empty, inserting_thread != e_ethread, NULL == inserting_thread
  tid = thread_create(thread_enqueue_cont_wait, (void *)tmp_param, 0, 0);
  if (tid <= 0) {//check signal() whether if execute
    LOG_ERROR("failed to create thread_enqueue_cont_wait");
    destroy_funcparam_test(tmp_param);
  } else {
    sleep(1);//in case of too fast
    g_signal_hook_success = false;
    cur_ethread->event_queue_external_.enqueue(param->event_, true);
    thread_join(tid);
    param->test_ok_ = tmp_param->test_ok_;
    destroy_funcparam_test(tmp_param);
    TestProtectedQueue::check_null_inserting_thread_empty(param);

    g_signal_hook_success = false;//not empty
    param->test_event_->ethread_ = cur_ethread;
    cur_ethread->event_queue_external_.enqueue(param->test_event_, true);
    TestProtectedQueue::check_null_inserting_thread_not_empty(param);

    TestProtectedQueue::check_dequeue_al(cur_ethread, param->test_event_);//dequeue from al
    TestProtectedQueue::check_dequeue_al(cur_ethread, param->event_);
    param->test_event_ = NULL;
  }
  return NULL;
}

void *thread_fill_al_by_event_array(void *this_test)
{
  ObProtectedQueue *protected_queue = ((TestProtectedQueue *)this_test)->protected_queue_;
  ObEvent **event_array = ((TestProtectedQueue *)this_test)->event_array_;
  for (int64_t i = 0; i < MAX_EVENT_CREATE_NUM; ++i) {
    event_array[i]->mutex_ = new_proxy_mutex();
    protected_queue->enqueue(event_array[i], false);
  }
  return NULL;
}

void *thread_fill_al_cancelled(void *protected_queue)
{
  ObEvent *tmp_event = NULL;
  ObProtectedQueue *tmp_queue = (ObProtectedQueue *)protected_queue;
  for (int64_t i = 0; i < MAX_EVENT_CREATE_NUM; ++i) {
    if (NULL == (tmp_event = op_reclaim_alloc(ObEvent))) {//destroyed in teardown
      LOG_ERROR("fail to alloc mem for ObEvent");
    } else {
      tmp_event->cancelled_ = true;
      tmp_queue->enqueue(tmp_event, false);
    }
  }
  return NULL;
}

void fill_al_by_event_array(TestProtectedQueue *this_test)
{
  ObThreadId tid;
  tid = thread_create(thread_fill_al_by_event_array, (void *)this_test, 0, 0);
  if (tid <= 0) {
    LOG_ERROR("failed to create thread_null_inserting_thread");
  } else {
    thread_join(tid);
  }
}

int handle_enqueue_not_regular_continuation(ObContInternal *contp, ObEventType aevent,
    void* edata)
{
  UNUSED(aevent);
  UNUSED(contp);
  ObThreadId tid;
  ObEvent *tmp_event = NULL;
  TestFuncParam *param;
  param = static_cast<TestFuncParam *>((static_cast<ObEvent *>(edata))->cookie_);
  ObEThread *inserting_thread = this_ethread();//not_regular

  tmp_event = (ObEvent *)(param->change_cookie_[0]);
  tmp_event->ethread_ = g_event_processor.all_event_threads_[0];
  tmp_event->ethread_->signal_hook_ = signal_hook_enqueue;

  TestFuncParam *tmp_param = create_funcparam_test();
  tmp_param->ethread_ = tmp_event->ethread_;
  tid = thread_create(thread_enqueue_cont_wait, (void *)tmp_param, 0, 0);
  if (tid <= 0) {//check signal
    LOG_ERROR("failed to create thread_enqueue_cont_wait");
  } else {
    sleep(1);
    inserting_thread->event_queue_external_.enqueue(tmp_event, true);
    thread_join(tid);
    param->test_ok_ = tmp_param->test_ok_;
  }
  destroy_funcparam_test(tmp_param);

  TestProtectedQueue::check_enqueue_not_regular(tmp_event->ethread_, param);
  signal_condition(param->wait_cond_);
  return 1;
}

TestFuncParam *common_handle_continuation(void* edata)
{
  ObEvent *tmp_event = NULL;
  TestFuncParam *param = NULL;
  param = static_cast<TestFuncParam *>((static_cast<ObEvent *>(edata))->cookie_);
  ObEThread *inserting_thread = this_ethread();//g_event_processor.all_event_threads_[last];

  for (int64_t i = 0; i < DEFALUT_CHANGE_COOKIE_SIZE; ++i) {
    // sig_e[] : null -> full -> overflow
    tmp_event = (ObEvent *)(param->change_cookie_[i]);
    tmp_event->ethread_ = g_event_processor.all_event_threads_[param->array_seq_[i]];
    tmp_event->ethread_->signal_hook_ = signal_hook_simple;
    g_signal_hook_success = false;
    inserting_thread->event_queue_external_.enqueue(tmp_event, true);
    TestProtectedQueue::check_enqueue_regular(tmp_event, inserting_thread, i + 1);
    TestProtectedQueue::check_dequeue_al(inserting_thread, tmp_event);
  }
  return param;
}

int handle_enqueue_regular_continuation(ObContInternal *contp, ObEventType aevent,
    void* edata)
{
  UNUSED(aevent);
  UNUSED(contp);
  TestFuncParam *param = common_handle_continuation(edata);
  param->test_ok_ = true;
  signal_condition(param->wait_cond_);
  return 1;
}

int handle_flush_signals_one_continuation(ObContInternal *contp, ObEventType aevent,
    void* edata)
{
  UNUSED(aevent);
  UNUSED(contp);
  ObThreadId tid;
  ObEvent *tmp_event = NULL;
  TestFuncParam *tmp_param = NULL;
  TestFuncParam *param = NULL;
  param = static_cast<TestFuncParam *>((static_cast<ObEvent *>(edata))->cookie_);
  ObEThread *inserting_thread = this_ethread();//g_event_processor.all_event_threads_[last];

  tmp_event = (ObEvent *)(param->change_cookie_[0]);
  tmp_event->ethread_ = g_event_processor.all_event_threads_[0];
  tmp_event->ethread_->signal_hook_ = signal_hook_enqueue;
  inserting_thread->event_queue_external_.enqueue(tmp_event, false);//do nothing first time
  TestProtectedQueue::check_enqueue_regular(tmp_event, inserting_thread, 1);

  //check flush_signals()
  tmp_param = create_funcparam_test();
  tmp_param->ethread_ = g_event_processor.all_event_threads_[0];
  tid = thread_create(thread_enqueue_cont_wait, (void *)tmp_param, 0, 0);
  if (tid <= 0) {
    LOG_ERROR("failed to create thread_enqueue_cont_wait");
    destroy_funcparam_test(tmp_param);
  } else {
    sleep(1);//in case of too fast
    flush_signals(inserting_thread);
    thread_join(tid);
    param->test_ok_ = tmp_param->test_ok_;
    destroy_funcparam_test(tmp_param);
    TestProtectedQueue::check_flush_signals_handle(inserting_thread, param->test_ok_);
  }
  TestProtectedQueue::check_enqueue_not_regular(tmp_event->ethread_, param);

  signal_condition(param->wait_cond_);
  return 1;
}

int handle_flush_signals_continuation(ObContInternal *contp, ObEventType aevent,
    void* edata)
{
  UNUSED(aevent);
  UNUSED(contp);
  ObThreadId tid[DEFALUT_CHANGE_COOKIE_SIZE];
  TestFuncParam *tmp_param[DEFALUT_CHANGE_COOKIE_SIZE] = {};
  ObEThread *inserting_thread = this_ethread();//g_event_processor.all_event_threads_[last];
  TestFuncParam *param = common_handle_continuation(edata);

  //too many signal_hook to check, so it only checks signal, without checking signal_hook
  for (int64_t i = 0; i < g_event_processor.event_thread_count_; ++i) {
    tmp_param[i] = create_funcparam_test();
    tmp_param[i]->ethread_ = g_event_processor.all_event_threads_[param->array_seq_[i]];
    tid[i] = thread_create(thread_enqueue_cont_wait, (void *)tmp_param[i], 0, 0);
    if (tid[i] <= 0) {
      LOG_ERROR("failed to create thread_enqueue_cont_wait");
      destroy_funcparam_test(tmp_param[i]);
    } else {}
  }
  sleep(1);//in case of too fast
  flush_signals(inserting_thread);
  param->test_ok_ = true;
  for (int64_t i = 0; i < g_event_processor.event_thread_count_; ++i) {
    if (tid[i] > 0) {
      thread_join(tid[i]);
      param->test_ok_ = param->test_ok_ & tmp_param[i]->test_ok_;
      destroy_funcparam_test(tmp_param[i]);
    } else {}
  }
  TestProtectedQueue::check_flush_signals_handle(inserting_thread, param->test_ok_);

  signal_condition(param->wait_cond_);
  return 1;
}

TEST_F(TestProtectedQueue, ObProtectedQueue)
{
  LOG_DEBUG("ObProtectedQueue");//check ObProtectedQueue()
  ObEvent obevent;
  ASSERT_TRUE(NULL != protected_queue_);
  ASSERT_EQ(0, strcmp(protected_queue_->atomic_list_.name_,"ObProtectedQueue"));
  ASSERT_EQ(protected_queue_->atomic_list_.offset_, (char *)&obevent.link_.next_ - (char *)&obevent);
  ASSERT_TRUE(protected_queue_->atomic_list_.empty());
  ASSERT_TRUE(protected_queue_->local_queue_.empty());
}

TEST_F(TestProtectedQueue, enqueue_local)
{
  LOG_DEBUG("enqueue_local");
  ObEvent *tmp_event = NULL;
  ASSERT_TRUE(NULL != protected_queue_);
  for (int64_t i = 0; i < MAX_EVENT_CREATE_NUM; ++i) {
    if (NULL == (tmp_event = op_reclaim_alloc(ObEvent))) {//destroyed in teardown
      LOG_ERROR("fail to alloc mem for ObEvent");
      ASSERT_TRUE(false);
    } else {
      ASSERT_EQ(0, tmp_event->in_the_prot_queue_);//check enqueue_local()
      protected_queue_->enqueue_local(tmp_event);
      ASSERT_EQ(1, tmp_event->in_the_prot_queue_);
      ASSERT_FALSE(protected_queue_->local_queue_.empty());
    }
  }
}

TEST_F(TestProtectedQueue, dequeue_local)
{
  LOG_DEBUG("dequeue_local");
  ObEvent *tmp_event = NULL;
  ASSERT_TRUE(NULL != protected_queue_);
  for (int64_t i = 0; i < MAX_EVENT_CREATE_NUM; ++i) {
    protected_queue_->enqueue_local(event_array_[i]);//destroyed in teardown
    ASSERT_EQ(1, event_array_[i]->in_the_prot_queue_);
  }
  for (int64_t i = 0; i < MAX_EVENT_CREATE_NUM; ++i) {
    tmp_event = protected_queue_->dequeue_local();//check dequeue_local()
    ASSERT_TRUE(event_array_[i] == tmp_event);
    ASSERT_EQ(0, tmp_event->in_the_prot_queue_);
  }
  tmp_event = protected_queue_->dequeue_local();
  ASSERT_TRUE(NULL == tmp_event);
}

TEST_F(TestProtectedQueue, enqueue_null_inserting_thread)
{
  LOG_DEBUG("enqueue NULL == inserting_thread");
  ObThreadId tid;
  test_param_ = create_funcparam_test();//destroyed in teardown
  test_param_->event_ = event_array_[0];
  test_param_->test_event_ = event_array_[1];
  tid = thread_create(thread_null_inserting_thread, (void *)test_param_, 0, 0);
  if (tid <= 0) {
    LOG_ERROR("failed to create thread_null_inserting_thread");
  } else {
    thread_join(tid);
  }

  test_param_->event_ = event_array_[2];
  test_param_->test_event_ = event_array_[3];
  tid = thread_create(thread_null_inserting_thread, (void *)test_param_, 0, 0);
  if (tid <= 0) {
    LOG_ERROR("failed to create thread_null_inserting_thread");
  } else {
    thread_join(tid);
  }
}

TEST_F(TestProtectedQueue, enqueue_null_ethreads_to_be_signalled_1)
{
  LOG_DEBUG("enqueue NULL == ethreads_to_be_signalled_1");
  int64_t event_seq[DEFALUT_CHANGE_COOKIE_SIZE] = {0, 1, 2, 3, 4};//e_ethread_seq[5] is 0
  TestProtectedQueue::common_enqueue_not_regular(event_seq);
  ASSERT_TRUE(test_param_->test_ok_);
}

TEST_F(TestProtectedQueue, enqueue_null_ethreads_to_be_signalled_2)
{
  LOG_DEBUG("enqueue NULL == ethreads_to_be_signalled_2");
  int64_t event_seq[DEFALUT_CHANGE_COOKIE_SIZE] = {4, 3, 2, 1, 0};//e_ethread_seq[5] is 0
  TestProtectedQueue::common_enqueue_not_regular(event_seq);
  ASSERT_TRUE(test_param_->test_ok_);
}

TEST_F(TestProtectedQueue, enqueue_regular_ethread_1)
{
  LOG_DEBUG("enqueue NULL != ethreads_to_be_signalled_1");
  int64_t e_ethread_seq[DEFALUT_CHANGE_COOKIE_SIZE] = {1, 1, 0, 2, 0};
  TestProtectedQueue::common_enqueue_regular(e_ethread_seq);
  ASSERT_TRUE(test_param_->test_ok_);
}

TEST_F(TestProtectedQueue, enqueue_regular_ethread_2)
{
  LOG_DEBUG("enqueue NULL != ethreads_to_be_signalled_2");
  int64_t e_ethread_seq[DEFALUT_CHANGE_COOKIE_SIZE] = {0, 1, 2, 1, 0};
  TestProtectedQueue::common_enqueue_regular(e_ethread_seq);
  ASSERT_TRUE(test_param_->test_ok_);
}

TEST_F(TestProtectedQueue, enqueue_regular_ethread_3)
{
  LOG_DEBUG("enqueue NULL != ethreads_to_be_signalled_3");
  int64_t e_ethread_seq[DEFALUT_CHANGE_COOKIE_SIZE] = {1, 0, 2, 0, 0};
  TestProtectedQueue::common_enqueue_regular(e_ethread_seq);
  ASSERT_TRUE(test_param_->test_ok_);
}

TEST_F(TestProtectedQueue, flush_signals_one)
{
  LOG_DEBUG("flush_signals");
  int64_t event_seq[DEFALUT_CHANGE_COOKIE_SIZE] = {1, 3, 5, 7, 9};
  TestProtectedQueue::common_flush_signals(event_seq, true);
  ASSERT_TRUE(test_param_->test_ok_);
}

TEST_F(TestProtectedQueue, flush_signals)
{
  LOG_DEBUG("flush_signals");
  int64_t e_ethread_seq[DEFALUT_CHANGE_COOKIE_SIZE] = {1, 0, 2, 0, 0};
  TestProtectedQueue::common_flush_signals(e_ethread_seq);
  ASSERT_TRUE(test_param_->test_ok_);
}

TEST_F(TestProtectedQueue, dequeue_timed)
{
  LOG_DEBUG("dequeue_timed");
  fill_al_by_event_array(this);
  ASSERT_FALSE(protected_queue_->atomic_list_.empty());
  ASSERT_TRUE(protected_queue_->local_queue_.empty());
  protected_queue_->dequeue_timed(0, false); //no sleep and no cancelled_
  check_dequeue_timed(protected_queue_);
}

TEST_F(TestProtectedQueue, dequeue_timed_cancelled)
{
  LOG_DEBUG("dequeue_timed cancelled");
  ObThreadId tid;
  tid = thread_create(thread_fill_al_cancelled, (void *)protected_queue_, 0, 0);
  if (tid <= 0) {
    LOG_ERROR("failed to create thread_null_inserting_thread");
  } else {
    thread_join(tid);
  }
  ASSERT_FALSE(protected_queue_->atomic_list_.empty());
  ASSERT_TRUE(protected_queue_->local_queue_.empty());
  protected_queue_->dequeue_timed(0, false);//no sleep, but set cancelled_
  ASSERT_TRUE(protected_queue_->atomic_list_.empty());
  ASSERT_TRUE(protected_queue_->local_queue_.empty());
}

TEST_F(TestProtectedQueue, dequeue_timed_sleep_not_empty)
{
  LOG_DEBUG("dequeue_timed sleep not empty");
  ObHRTime last_time = 0;
  fill_al_by_event_array(this);
  last_time = get_hrtime_internal();//sleep, but atomic_list_.empty() == false
  protected_queue_->dequeue_timed(last_time + HRTIME_SECONDS(2), true);
  ASSERT_TRUE(check_imm_test_ok(get_hrtime_internal() - last_time));
  check_dequeue_timed(protected_queue_);
}

TEST_F(TestProtectedQueue, dequeue_timed_sleep_empty)
{
  LOG_DEBUG("dequeue_timed sleep empty");
  ObHRTime last_time = 0;
  ObHRTime diff_time = 0;
  ASSERT_TRUE(protected_queue_->atomic_list_.empty());
  ASSERT_TRUE(protected_queue_->local_queue_.empty());
  last_time = get_hrtime_internal();//sleep and atomic_list_.empty() == true
  protected_queue_->dequeue_timed(last_time + HRTIME_SECONDS(2), true);
  diff_time = get_hrtime_internal() - last_time;
  ASSERT_TRUE(diff_time <= (HRTIME_SECONDS(2) + TEST_THRESHOLD_MSECONDS)
      && diff_time >= (HRTIME_SECONDS(2) - TEST_THRESHOLD_MSECONDS));
  ASSERT_TRUE(protected_queue_->atomic_list_.empty());
  ASSERT_TRUE(protected_queue_->local_queue_.empty());
}

TEST_F(TestProtectedQueue, dequeue_timed_sleep_empty_exception)
{
  LOG_DEBUG("dequeue_timed sleep empty exception"); //sleep, al_.empty() == true
  dequeue_timed_exception(HRTIME_SECONDS(2)); //but 0 < timeout < cur
  dequeue_timed_exception(0);                 //but timeout == 0
  dequeue_timed_exception(HRTIME_SECONDS(-2));//but timeout < 0
  ASSERT_TRUE(protected_queue_->atomic_list_.empty());
  ASSERT_TRUE(protected_queue_->local_queue_.empty());
}

} // end of namespace obproxy
} // end of namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::obproxy::init_g_event_processor();
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  OB_LOGGER.set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
