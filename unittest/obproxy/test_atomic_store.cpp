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
#include "lib/atomic/ob_atomic.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/ob_errno.h"

#define ATOMIC_LOAD_OLD(x) ({__COMPILER_BARRIER(); *(x);})
#define ATOMIC_STORE_OLD(x, v) ({__COMPILER_BARRIER(); *(x) = v; __sync_synchronize(); })

#define NUM_THREADS 128
#define DEFAULT_CYCLE_NUM 1000000

using namespace oceanbase::common;
namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
static volatile int g_lock_v CACHE_ALIGNED = 0;
static volatile int g_var_a CACHE_ALIGNED = 0;
static volatile int g_reorder_count CACHE_ALIGNED = 0;

void inline lock_old()
{
  while (!ATOMIC_BCAS(&g_lock_v, 0, 1)) {
  #if defined(__aarch64__)
      asm("yield");
  #else
      asm("pause");
  #endif
  }
}

void inline unlock_old()
{
  // #define ATOMIC_STORE_OLD(x, v) ({__COMPILER_BARRIER(); *(x) = v; __sync_synchronize(); })
  ATOMIC_STORE_OLD(&g_lock_v, 0);
}

void inline lock()
{
  while (!ATOMIC_BCAS(&g_lock_v, 0, 1)) {
  #if defined(__aarch64__)
      asm("yield");
  #else
      asm("pause");
  #endif
  }
}

void inline unlock()
{
  // #define ATOMIC_STORE(x, v) ({ __atomic_store_n((x), (v), __ATOMIC_SEQ_CST);})
  ATOMIC_STORE(&g_lock_v, 0);
}


void *op_old(void *arg) {
  int ret = OB_SUCCESS;
  long long cycles = *((long long*) arg);
  int64_t i = 0;
  for (; i < cycles; i++) {
    lock_old();
    if (0 != g_var_a) {
      ATOMIC_INC(&g_reorder_count);
    }
    g_var_a++;
    MEM_BARRIER();
    g_var_a--;
    unlock_old();
  }
  UNUSED(ret);
  pthread_exit(NULL);
  return NULL;
}

void *op(void *arg) {
  long long cycles = *((long long*) arg);
  int ret = OB_SUCCESS;
  int64_t i = 0;
  for (; i < cycles; i++) {
    lock();
    if (0 != g_var_a) {
      ATOMIC_INC(&g_reorder_count);
    }
    g_var_a++;
    MEM_BARRIER();
    g_var_a--;
    unlock();
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
*  old atomic_store mod:
*   'nohup ./test_atomic_store 1000000 > output.log&'
*  expect:
*   (ARM) g_reorder_count > 0
*   (x86) (always) g_reorder_count == 0
*
*  new atomic_store mod:
*   'nohup ./test_atomic_store 1000000 a > output.log&'
*  expect:
*   (always) g_reorder_count == 0
*
*/
int main(int argc, char **argv) {
  bool use_old_atomic = argc <= 2;
  long long cycles = 0;
  if (1 == argc) {
    cycles = DEFAULT_CYCLE_NUM;
  } else {
    cycles = atoll(argv[1]);
  }

  std::cout << "use_old_atomic:" << use_old_atomic << std::endl;
  pthread_t tids_op[NUM_THREADS];
  for(int i = 0; i < NUM_THREADS; ++i) {
    pthread_create(&tids_op[i], NULL, use_old_atomic ? oceanbase::obproxy::proxy::op_old : oceanbase::obproxy::proxy::op, &cycles);
  }

  for (int i = 0; i < NUM_THREADS; i++) {
      pthread_join(tids_op[i], NULL);
  }

  std::cout << "g_reorder_count:" << oceanbase::obproxy::proxy::g_reorder_count << std::endl;
  return 0;
}