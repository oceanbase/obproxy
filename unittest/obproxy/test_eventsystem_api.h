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

#ifndef UNITTEST_OBPROXY_EVENTSYSTEM_API_H
#define UNITTEST_OBPROXY_EVENTSYSTEM_API_H

#include "lib/ob_define.h"
#include "obproxy/proxy/api/ob_iocore_api.h"
#include "obproxy/stat/ob_mysql_stats.h"

namespace oceanbase
{
namespace obproxy
{
using namespace proxy;
using namespace common;
using namespace event;
struct TestWaitCond;

#define TEST_ET_CALL_THREADS_NUM        2
#define TEST_TIME_SECOND_IN             HRTIME_SECONDS(2)
#define TEST_TIME_SECOND_AT(param)      (get_hrtime_internal() + param->at_delta_)
#define TEST_TIME_SECOND_EVERY          HRTIME_SECONDS(2)
#define TEST_THRESHOLD_MSECONDS         HRTIME_MSECONDS(100)
#define TEST_DEFAULT_PERIOD_COUNT       3
#define TEST_MULTI_SCHEDULE             7
#define DEBUG_TEST
#define MAX_PARAM_ARRAY_SIZE            7
#define DEFALUT_CHANGE_COOKIE_SIZE      5

#define DEBUG_SOCK(tag, fmt...) _PROXY_SOCK_LOG(DEBUG, ##fmt)
#define WARN_SOCK(tag, fmt...) _PROXY_SOCK_LOG(WARN, ##fmt)
#define DEBUG_NET(tag, fmt...) _PROXY_NET_LOG(DEBUG, ##fmt)
#define INFO_NET(tag, fmt...) _PROXY_NET_LOG(INFO, ##fmt)
#define WARN_NET(fmt...) _PROXY_NET_LOG(WARN, ##fmt)
#define ERROR_NET(fmt...) _PROXY_NET_LOG(ERROR, ##fmt)

#define SCHEDULE_EVERY_FIRST_CHECK                                              \
  {                                                                               \
    if (param->period_count_ == TEST_DEFAULT_PERIOD_COUNT) {                      \
      check_schedule_common(param);                                               \
      if (param->aperiod_ < 0) {                                                  \
        ASSERT_EQ(param->event_->period_, param->event_->timeout_at_);            \
      } else {                                                                    \
        ASSERT_LE(param->event_->period_, param->event_->timeout_at_);            \
      }                                                                           \
      ASSERT_EQ(param->aperiod_, param->event_->period_);                         \
      param->test_ok_ = true;                                                     \
    }                                                                             \
  }

#define TEST_MAX_STRING_LENGTH       70
#define TEST_STRING_GROUP_COUNT      2
#define TEST_G_BUFF_SIZE             TEST_MAX_STRING_LENGTH * TEST_STRING_GROUP_COUNT

#define TEST_EXPECT_EQ(A, B)  \
{\
  DEBUG_NET("CHECK", "TEST_EXPECT_EQ"); \
  check_ok_ &= test_expect(TEST_EXPECT_TYPE_EQ, A, #A, B, #B);\
}

#define TEST_EXPECT_LE(A, B)  \
{\
  DEBUG_NET("CHECK", "TEST_EXPECT_LE"); \
  check_ok_ &= test_expect(TEST_EXPECT_TYPE_LE, A, #A, B, #B); \
}

#define TEST_EXPECT_GE(A, B)  \
{\
  DEBUG_NET("CHECK", "TEST_EXPECT_GE"); \
  check_ok_ &= test_expect(TEST_EXPECT_TYPE_GE, A, #A, B, #B); \
}

#define TEST_EXPECT_TRUE(A)   \
{\
  DEBUG_NET("CHECK", "TEST_EXPECT_TRUE"); \
  check_ok_ &= test_expect(TEST_EXPECT_TYPE_TRUE, A, #A); \
}

extern bool g_signal_hook_success;
extern TestWaitCond g_wait_cond;
extern int32_t g_event_start_threads;
extern char g_output[TEST_STRING_GROUP_COUNT][TEST_MAX_STRING_LENGTH];
extern char g_input[TEST_STRING_GROUP_COUNT][TEST_MAX_STRING_LENGTH];

enum {
  TEST_EXPECT_TYPE_EQ = 0,
  TEST_EXPECT_TYPE_LE,
  TEST_EXPECT_TYPE_GE,
  TEST_EXPECT_TYPE_TRUE,
};


enum {
  RESULT_DEFAULT = 0,
  RESULT_NET_EVENT_ERROR,
  RESULT_NET_EVENT_LISTEN_FAILED,
  RESULT_NET_EVENT_LISTEN_SUCCEED,
  RESULT_NET_EVENT_ACCEPT_SUCCEED,
  RESULT_NET_EVENT_IO_SUCCEED,
  RESULT_NET_WRITE_UNFINISH,
  RESULT_NET_EVENT_ERROR_NO_MEMORY,
  RESULT_NET_EVENT_ERROR_TOO_MANY,
  RESULT_NET_ERROR,

  RESULT_NET_EVENT_IO_READ_SUCCEED,
  RESULT_NET_EVENT_IO_WRITE_SUCCEED,

  RESULT_NET_CALL_CONNECT_SUCCEED,
  RESULT_NET_CALL_CONNECT_FAILED,
  RESULT_NET_CONNECT_OPEN,
  RESULT_NET_CONNECT_OPEN_FAILED,
  RESULT_NET_EVENT_READ_FINISH,
};

struct TestWaitCond
{
  TestWaitCond()
  {
    pthread_mutex_init(&mutex_, NULL);
    pthread_cond_init(&cond_, NULL);
    condition_is_false_ = true;
  }
  pthread_mutex_t mutex_;
  pthread_cond_t  cond_;
  bool condition_is_false_;
};

struct TestFuncParam
{
  TestFuncParam()
  {
    func_type_ = 0;
    aperiod_ = 0;
    period_count_ = TEST_DEFAULT_PERIOD_COUNT;
    atimeout_ = 0;
    at_delta_ = HRTIME_SECONDS(2);
    event_type_ = ET_CALL;
    callback_event_ = EVENT_NONE;
    cookie_ = NULL;
    fast_signal_ = false;
    cont_ = NULL;
    event_ = NULL;
    ethread_ = NULL;
    ptr_mutex_ = NULL;
    processor_ = NULL;
    action_ = NULL;
    test_event_ = NULL;
    thread_ = NULL;
    cons_type_ = 0;
    memset(thr_name_, 0, sizeof(thr_name_));
    stacksize_ = 0;
    allocate_size_ = 0;
    started_ = false;
    wait_cond_ = NULL;
    test_ok_ = false;
    seq_no_ = 0;
    cur_time_ = 0;
    last_time_ = 0;
    test_lock_ = false;
    locked_ = false;
    common_ethread_ = false;
    change2external_local_ = false;
    change2external_al_ = false;
    memset(change_cookie_, 0, DEFALUT_CHANGE_COOKIE_SIZE);
    event_neighbor_ = NULL;
    memset(array_seq_, 0, DEFALUT_CHANGE_COOKIE_SIZE);
  }

  unsigned int func_type_;
  ObHRTime aperiod_;
  int32_t period_count_;
  ObHRTime atimeout_;
  ObHRTime at_delta_;
  ObEventThreadType event_type_;
  int callback_event_;
  void *cookie_;//it is used for transferring parameters to handle_schedule_test()
  bool fast_signal_;
  ObContinuation *cont_;
  ObEvent *event_;
  ObEThread *ethread_;
  common::ObPtr<ObProxyMutex> ptr_mutex_;
  ObProcessor *processor_;
  ObAction *action_;
  ObEvent *test_event_;
  ObThread *thread_;
  unsigned int cons_type_;
  char thr_name_[MAX_THREAD_NAME_LENGTH];
  int64_t stacksize_;
  int64_t allocate_size_;
  bool started_;
  TestWaitCond *wait_cond_;
  bool test_ok_;
  unsigned int seq_no_;
  ObHRTime last_time_;
  ObHRTime cur_time_;
  bool test_lock_;
  bool locked_;
  bool common_ethread_;
  bool change2external_local_;
  bool change2external_al_;
  void *change_cookie_[DEFALUT_CHANGE_COOKIE_SIZE];
  ObEvent *event_neighbor_;
  int64_t array_seq_[DEFALUT_CHANGE_COOKIE_SIZE];
};

typedef void (*SCHEDULE_X)(TestFuncParam *param);

TestFuncParam *create_funcparam_test(bool create_cont = false, ObEventFunc funcp = NULL,
                                     ObProxyMutex *mutexp = NULL);
bool destroy_funcparam_test(TestFuncParam *func_param);
void wait_condition(TestWaitCond *wait_cond);
void signal_condition(TestWaitCond *wait_cond);
int handle_hook_test(ObEThread &ethread);
void print_process(const unsigned int seq_no,
                   const ObHRTime cur_time, const ObHRTime last_time);
void check_schedule_result(TestFuncParam *param, SCHEDULE_X check_imm,
                           SCHEDULE_X check_at, SCHEDULE_X check_in, SCHEDULE_X check_every);
int check_imm_test_ok(ObHRTime diff_time, ObHRTime sleep_time = 0);
int check_test_ok(TestFuncParam *param, const ObHRTime timeout) ;
void *thread_g_net_processor_start(void *data);
void init_g_net_processor();

bool test_expect(const int32_t type, const int64_t a,  const char astr[],
    const int64_t b = 0, const char bstr[] = NULL);

} // end of namespace obproxy
} // end of namespace oceanbase

#endif //UNITTEST_OBPROXY_EVENTSYSTEM_API_H
