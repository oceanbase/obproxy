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

#include "lib/thread/ob_simple_thread_pool.h"

namespace oceanbase
{
namespace common
{
ObSimpleThreadPool::ObSimpleThreadPool()
    : is_inited_(false),
      is_stopped_(false),
      thread_num_(0),
      task_num_limit_(0)
{
}

ObSimpleThreadPool::~ObSimpleThreadPool()
{
  destroy();
}

int ObSimpleThreadPool::init(const int64_t thread_num, const int64_t task_num_limit)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (thread_num <= 0 || task_num_limit <= 0 || thread_num > MAX_THREAD_NUM) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (ret = queue_.init(task_num_limit))) {
    COMMON_LOG(WARN, "task queue init failed", K(ret), K(task_num_limit));
  } else if (OB_SUCCESS != (ret = launch_thread_(thread_num))) {
    COMMON_LOG(WARN, "launch_thread_ failed", K(ret), K(thread_num));
  } else {
    is_inited_ = true;
    is_stopped_ = false;
    thread_num_ = thread_num;
    task_num_limit_ = task_num_limit;
  }

  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

void ObSimpleThreadPool::destroy()
{
  is_inited_ = false;
  ATOMIC_STORE(&is_stopped_, true);
  for (int64_t i = 0; i < thread_num_; i++) {
    ThreadConf &tc = thread_conf_array_[i];
    queue_cond_.signal();
    int pthread_ret = pthread_join(tc.pd, NULL);
    if (0 != pthread_ret) {
      COMMON_LOG(ERROR, "pthread_join failed", K(pthread_ret));
    } else {
      // do nothing
    }
  }
  thread_num_ = 0;
  task_num_limit_ = 0;
  queue_.destroy();
}

int ObSimpleThreadPool::push(void *task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (NULL == task) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ret = queue_.push(task);
    if (OB_SUCC(ret)) {
      queue_cond_.signal();
    }
    if (OB_SIZE_OVERFLOW == ret) {
      ret = OB_EAGAIN;
    }
  }
  return ret;
}

void *ObSimpleThreadPool::thread_func_(void *data)
{
  ThreadConf *const tc = (ThreadConf *)data;
  if (NULL == tc || NULL == tc->host) {
    COMMON_LOG(WARN, "thread_func param null pointer");
  } else {
    while (!ATOMIC_LOAD(&tc->host->is_stopped_)) {
      void *task = NULL;
      tc->host->queue_.pop(task);
      if (NULL != task) {
        tc->host->handle(task);
      } else {
        tc->host->queue_cond_.timedwait(QUEUE_WAIT_TIME);
      }
    }
  }
  return NULL;
}

int ObSimpleThreadPool::launch_thread_(const int64_t thread_num)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCCESS == ret && i < thread_num; i++) {
    ThreadConf &tc = thread_conf_array_[i];
    tc.host = this;
    int tmp_ret = 0;
    if (0 != (tmp_ret = pthread_create(&(tc.pd), NULL, thread_func_, &tc))) {
      COMMON_LOG(WARN, "pthread_create failed", "ret", tmp_ret);
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    COMMON_LOG(INFO, "create thread succ", "index", i, "ret", tmp_ret);
  }
  return ret;
}
} // namespace common
} // namespace oceanbase
