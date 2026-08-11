/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PROXY
#include <gtest/gtest.h>
#include "lib/oblog/ob_log.h"
#include <sys/resource.h>
#include "common/ob_common_utility.h"

namespace oceanbase
{
using namespace common;
namespace obproxy
{

class TestStackOverflow : public ::testing::Test
{ };


bool test_func()
{
  int ret = OB_SUCCESS;
  bool is_over_flow = false;
  ret = check_stack_overflow(is_over_flow);
  if (is_over_flow || ret != OB_SUCCESS) {
    // do nothing
  } else {
    is_over_flow = test_func();
  }
  return is_over_flow;
}

TEST_F(TestStackOverflow, test_delete_tenant)
{
  // limit stack size to 1MB
  const rlim_t stack_size = 1 * 1024 * 1024;
  struct rlimit rl;
  rl.rlim_cur = stack_size;

  int result = getrlimit(RLIMIT_STACK, &rl);
  ASSERT_EQ(result, 0);
  if (rl.rlim_cur > stack_size) {
    rl.rlim_cur = stack_size;
  }

  result = setrlimit(RLIMIT_STACK, &rl);
  ASSERT_EQ(result, 0);


  bool is_over_flow = test_func();
  LOG_INFO("check over flow", K(is_over_flow));
  ASSERT_EQ(is_over_flow, true);
}


}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
