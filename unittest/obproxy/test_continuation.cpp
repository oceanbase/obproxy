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
#include "obproxy/iocore/eventsystem/ob_continuation.h"

namespace oceanbase
{
namespace obproxy
{
using namespace proxy;
using namespace common;
using namespace event;

class TestChildContinuation : public ObContinuation
{
public:
  TestChildContinuation() {}
  virtual ~TestChildContinuation() { }

  void init()
  {
    SET_HANDLER(&TestChildContinuation::handle_continuation);
#ifdef DEBUG
    printf("TestChildContinuation's name : %s\n", this->handle_name_);
#endif
  }

  int handle_continuation(int event, void *data)
  {
    UNUSED(event);
    UNUSED(data);
    printf("call TestChildContinuation::handle_continuation ok\n");
    return true;
  }

  int handle_continuation2(int event, void *data)
  {
    UNUSED(event);
    UNUSED(data);
    printf("call TestChildContinuation::handle_continuation2 ok\n");
    return true;
  }
};

class TestContinuation : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown();

public:
  TestFuncParam *test_param;
};

void TestContinuation::SetUp()
{
    test_param = NULL;
}

void TestContinuation::TearDown()
{
  if (NULL != test_param) {
    destroy_funcparam_test(test_param);
  } else {}
}

int handle_continuation(int event, void *data)
{
  UNUSED(event);
  UNUSED(data);
  printf("call handle_continuation ok\n");
  return true;
}

TEST_F(TestContinuation, ObContinuation_all)
{
  LOG_DEBUG("ObContinuation_all");
  ObContinuation *cont = NULL;
  TestChildContinuation *child_cont = NULL;
  test_param = create_funcparam_test();
  ObProxyMutex *mutex = new_proxy_mutex();
  int handle_continuation(int event, void *data);

  if (NULL == (cont = new(std::nothrow) ObContinuation(mutex))) {
    LOG_ERROR("fail to alloc mem for ObContinuation");
  } else {
    test_param->cont_ = cont;
    ASSERT_TRUE(NULL == cont->handler_);
    ASSERT_TRUE(mutex == cont->mutex_);
    delete cont;
    test_param->cont_ = NULL;
  }

  if (NULL == (child_cont = new(std::nothrow) TestChildContinuation)) {
    LOG_ERROR("fail to alloc mem for TestChildContinuation");
  } else {
    test_param->cont_ = static_cast<ObContinuation *>(child_cont);
    ASSERT_TRUE(NULL == child_cont->mutex_);
    ASSERT_TRUE(NULL == child_cont->handler_);
    child_cont->init();
    ASSERT_TRUE(child_cont->handle_event());

    SET_CONTINUATION_HANDLER(child_cont, &TestChildContinuation::handle_continuation2);
    ASSERT_TRUE(child_cont->handle_event());
    delete child_cont;
    test_param->cont_ = NULL;
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
