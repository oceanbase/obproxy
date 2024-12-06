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
#include "lib/lock/tbrwlock.h"
#include "lib/lock/ob_mutex.h"

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

pthread_barrier_t barrier CACHE_ALIGNED;
obsys::CRWLock pthread_lock CACHE_ALIGNED;
lib::ObMutex ob_lock CACHE_ALIGNED;

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
  // 所有线程在此处等待
  pthread_barrier_wait(&barrier);
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

void *op_new(void *arg) {
  // 所有线程在此处等待
  pthread_barrier_wait(&barrier);
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

void *op_with_pthread_lock(void *arg) {
  // 所有线程在此处等待
  pthread_barrier_wait(&barrier);
  long long cycles = *((long long*) arg);
  int ret = OB_SUCCESS;
  int64_t i = 0;
  for (; i < cycles; i++) {
    obsys::CWLockGuard wlock(pthread_lock);
    if (0 != g_var_a) {
      ATOMIC_INC(&g_reorder_count);
    }
    g_var_a++;
    MEM_BARRIER();
    g_var_a--;
  }
  UNUSED(ret);
  pthread_exit(NULL);
  return NULL;
}

void *op_with_ob_latch(void *arg) {
  // 所有线程在此处等待
  pthread_barrier_wait(&barrier);
  long long cycles = *((long long*) arg);
  int ret = OB_SUCCESS;
  int64_t i = 0;
  for (; i < cycles; i++) {
    lib::ObMutexGuard guard(ob_lock);
    if (0 != g_var_a) {
      ATOMIC_INC(&g_reorder_count);
    }
    g_var_a++;
    MEM_BARRIER();
    g_var_a--;
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
*   'nohup ./test_atomic_store 1000000 o > output.log&'
*  expect:
*   (ARM) g_reorder_count > 0
*   (x86) (always) g_reorder_count == 0
*
*  new atomic_store mod:
*   'nohup ./test_atomic_store 1000000 n > output.log&'
*  expect:
*   (always) g_reorder_count == 0
*
*/
int main(int argc, char **argv) {
  typedef void* (*opFunc)(void *arg);
  opFunc op =  oceanbase::obproxy::proxy::op_old;
  long long cycles = 0;
  if (1 == argc) {
    cycles = DEFAULT_CYCLE_NUM;
  } else if (2 == argc) {
    cycles = atoll(argv[1]);
  } else {
    cycles = atoll(argv[1]);
    switch(argv[2][0]) {
      case 'o': {
        op =  oceanbase::obproxy::proxy::op_old;
        std::cout << "use old atomic operation" << std::endl;
        break;
      }
      case 'n': {
        op =  oceanbase::obproxy::proxy::op_new;
        std::cout << "use new atomic operation" << std::endl;
        break;
      }
      case 'p': {
        op =  oceanbase::obproxy::proxy::op_with_pthread_lock;
        std::cout << "use pthread lock operation" << std::endl;
        break;
      }
      case 'l': {
        op =  oceanbase::obproxy::proxy::op_with_ob_latch;
        std::cout << "use oblatch operation" << std::endl;
        break;
      }
      default:
        break;
    }
  }

  pthread_t tids_op[NUM_THREADS];

  // 初始化屏障
  if (pthread_barrier_init(&oceanbase::obproxy::proxy::barrier, NULL, NUM_THREADS + 1) != 0) {
      fprintf(stderr, "Error initializing barrier\n");
      exit(1);
  }

  for(int i = 0; i < NUM_THREADS; ++i) {
    pthread_create(&tids_op[i], NULL, op, &cycles);
  }

  // 主线程也加入屏障
  pthread_barrier_wait(&oceanbase::obproxy::proxy::barrier);

  for (int i = 0; i < NUM_THREADS; i++) {
      pthread_join(tids_op[i], NULL);
  }

  std::cout << "g_reorder_count:" << oceanbase::obproxy::proxy::g_reorder_count << std::endl;
  return 0;
}