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

#include <gtest/gtest.h>
#include <pthread.h>
#define private public
#define protected public
#include "test_eventsystem_api.h"
#include "ob_mysql_stats.h"
#include "stat/ob_net_stats.h"

namespace oceanbase
{
namespace obproxy
{
using namespace proxy;
using namespace common;
using namespace event;
using namespace net;

#define UNUSED_PARAMER                      1000
#define TEST_EVENT_START_THREADS_NUM        2
#define TEST_MAX_STRING_LENGTH              70
#define TEST_STRING_GROUP_COUNT             5

char g_output[TEST_STRING_GROUP_COUNT][TEST_MAX_STRING_LENGTH] = {};
TestWaitCond g_wait_cond;

struct TestNetSMCont : public ObContinuation
{
  ObVIO *read_vio_;
  ObIOBufferReader *reader_;
  ObNetVConnection *vc_;
  ObMIOBuffer *buf_;

  TestNetSMCont(ObProxyMutex * mutex, ObNetVConnection * vc)
  : ObContinuation(mutex)
  {
    MUTEX_TRY_LOCK(lock, mutex_, vc->thread_);
    ob_release_assert(lock.is_locked());
    vc_ = vc;
    SET_HANDLER(&TestNetSMCont::handle_read);
    buf_ = new_miobuffer(32 * 1024);
    reader_ = buf_->alloc_reader();
    read_vio_ = vc->do_io_read(this, INT64_MAX, buf_);
    memset(g_output, 0 , TEST_STRING_GROUP_COUNT * TEST_MAX_STRING_LENGTH);
    INFO_NET("TEST", "TestNetSMCont : create test sm");
  }

  int handle_read(int event, void *data)
  {
    UNUSED(data);
    int size;
    static int i = 0;
    switch (event) {
    case VC_EVENT_READ_READY: {
      size = (int)reader_->read_avail();
      ob_assert(size <= TEST_MAX_STRING_LENGTH);
      reader_->read(g_output[i++], size);
      printf("output:%s\n", g_output[i - 1]);
      fflush(stdout);
      break;
    }
    case VC_EVENT_READ_COMPLETE:
    case VC_EVENT_EOS: {
      size = (int)reader_->read_avail();
      reader_->read(g_output[i++], size);
      printf("output:%s\n", g_output[i - 1]);
      fflush(stdout);
      vc_->do_io_close();
      break;
    }
    case VC_EVENT_ERROR: {
      vc_->do_io_close();
      break;
    }
    default:
      ob_release_assert(!"unknown event");
    }
    return EVENT_CONT;
  }
};

struct TestNetAcceptCont : public event::ObContinuation
{
  TestNetAcceptCont(ObProxyMutex * mutex)
  : ObContinuation(mutex)
  {
    SET_HANDLER(&TestNetAcceptCont::handle_accept);
  }

  int handle_accept(int event, void *data)
  {
    UNUSED(event);
    INFO_NET("TEST", "TestNetAcceptCont : Accepted a connection");
     ObNetVConnection *vc = static_cast<ObNetVConnection *>(data);
    new TestNetSMCont(new_proxy_mutex(), vc);
    signal_condition(&g_wait_cond);
    return EVENT_CONT;
  }
};

void *thread_net_processor_start(void *data);
void init_net_processor();

class TestNetSystem : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown();

public:
  FILE *test_fp_;
};

void TestNetSystem::SetUp()
{
  test_fp_ = NULL;
}

void TestNetSystem::TearDown()
{
  if (NULL != test_fp_) {
    pclose(test_fp_);
  } else {}
}

void *thread_net_processor_start(void *data)
{
  UNUSED(data);
  init_event_system(EVENT_SYSTEM_MODULE_VERSION);
  ObNetProcessor::ObAcceptOptions options;
  options.local_port_ = 9876;
  options.accept_threads_ = 1;
  options.frequent_accept_ = true;

  init_mysql_stats();
  ObNetOptions net_options;
  if (OB_SUCCESS != init_net(NET_SYSTEM_MODULE_VERSION, net_options)) {
    ERROR_NET("failed to init net");
  } else if (OB_SUCCESS != init_net_stats()) {
    ERROR_NET("failed to init net stats");
  } else if (OB_SUCCESS != g_event_processor.start(TEST_EVENT_START_THREADS_NUM, DEFAULT_STACKSIZE, false, false)) {
    DEBUG_NET("TEST", "failed to START g_event_processor");
  } else if (OB_SUCCESS != g_net_processor.start(UNUSED_PARAMER, UNUSED_PARAMER)){
    DEBUG_NET("TEST", "failed to START g_net_processor");
  } else {
    INFO_NET("TEST", "g_net_processor START success");
    g_net_processor.accept(new TestNetAcceptCont(new_proxy_mutex()), options);
    this_ethread()->execute();
  }
  return NULL;
}

void init_net_processor()
{
  ObThreadId tid;
  tid = thread_create(thread_net_processor_start, NULL, true, 0);
  if (tid <= 0) {
    LOG_ERROR("failed to create thread_net_processor_start");
  } else {}
}

TEST_F(TestNetSystem, test_start)
{
  INFO_NET("TEST", "test_start");
  char buffer[TEST_STRING_GROUP_COUNT][TEST_MAX_STRING_LENGTH] ={
      "OceanBase was originally designed to solve the problem of ",
      "large-scale data of Taobao in ALIBABA GROUP. ",
      "OceanBase experiences these versions such as 0.3, 0.4, 0.5. ",
      "You can refer to \"OceanBase 0.5 System Overview\" for system ",
      "overview of version 0.5."
  };

  if (NULL == (test_fp_ = popen("telnet 127.0.0.1 9876", "w"))) {
    DEBUG_NET("TEST", "failed to popen test_fp_");
  } else {
    wait_condition(&g_wait_cond);

    for (int64_t i = 0; i < TEST_STRING_GROUP_COUNT; ++i) {
      fputs(buffer[i], test_fp_);
      fflush(test_fp_);
      printf("input :%s\n", buffer[i]);
      fflush(stdout);
      sleep(1);
    }

    for (int64_t i = 0; i < TEST_STRING_GROUP_COUNT; ++i) {
      EXPECT_EQ(0, memcmp(buffer[i], g_output[i], TEST_MAX_STRING_LENGTH));
    }
  }
}


} // end of namespace obproxy
} // end of namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  oceanbase::obproxy::init_net_processor();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

