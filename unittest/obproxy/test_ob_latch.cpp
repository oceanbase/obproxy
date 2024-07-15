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
#define USING_LOG_PREFIX PROXY
#include <gtest/gtest.h>
#include <pthread.h>
#include <thread>
#include <iostream>
#include <cstdlib>
#include <ctime>
#include "lib/utility/ob_macro_utils.h"
#include "lib/ob_errno.h"
#include "lib/lock/ob_latch.h"



#define NUM_THREADS 128
#define DEFAULT_CYCLE_NUM 1000000

using namespace oceanbase::common;
namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
static int g_var_a CACHE_ALIGNED = 0;

static ObLatch g_latch;

int do_read()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(g_latch.rdlock(ObLatchIds::DEFAULT_DRW_LOCK))) {
    LOG_EDIAG("fail to rdlock");
  } else {
    int tmp = g_var_a;
    UNUSED(tmp); 
  }

  if (OB_FAIL(g_latch.unlock())) {
    LOG_EDIAG("fail to unlock rdlock");
  }

  return ret;
}

int do_write()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(g_latch.wrlock(ObLatchIds::DEFAULT_DRW_LOCK))) {
    LOG_EDIAG("fail to wrlock");
  } else {
    g_var_a++;
  }

  if (OB_FAIL(g_latch.unlock())) {
    LOG_EDIAG("fail to unlock rdlock");
  }

  return ret;
}


void *op(void *arg) {
  long long cycles = *((long long*) arg);
  int ret = OB_SUCCESS;
  int64_t i = 0;
  for (; i < cycles; i++) {
    if (0 == (i % 2)) {
      if (OB_FAIL(do_write())) {
        std::abort();
      }
    } else {
      if (OB_FAIL(do_read())) {
        std::abort();
      }
    }
  }
  UNUSED(ret);
  pthread_exit(NULL);
  return NULL;
}

}
}
}

/* Usage:
*
*  'nohup ./test_ob_latch 1000000 > test_ob_latch.log&'
*/
int main(int argc, char **argv) {
  long long cycles = 0;
  long long expect_value = 0;
  if (1 == argc) {
    cycles = DEFAULT_CYCLE_NUM;
  } else {
    cycles = atoll(argv[1]);
  }
  expect_value = cycles * NUM_THREADS / 2;
  pthread_t tids_op[NUM_THREADS];
  for(int i = 0; i < NUM_THREADS; ++i) {
    pthread_create(&tids_op[i], NULL, oceanbase::obproxy::proxy::op, &cycles);
  }

  for (int i = 0; i < NUM_THREADS; i++) {
      pthread_join(tids_op[i], NULL);
  }

  std::cout << "total cycles:" << cycles << std::endl;
  std::cout << "expected value:" << expect_value << std::endl;
  std::cout << "real value:" << oceanbase::obproxy::proxy::g_var_a << std::endl;
  std::cout << (expect_value == oceanbase::obproxy::proxy::g_var_a ? "succ" : "fail") << std::endl;

  return 0;
}