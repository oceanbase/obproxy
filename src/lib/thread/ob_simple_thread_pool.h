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

#ifndef OCEANBASE_COMMON_OB_SIMPLE_THREAD_POOL_H_
#define OCEANBASE_COMMON_OB_SIMPLE_THREAD_POOL_H_
#include "common/ob_queue_thread.h"

namespace oceanbase
{
namespace common
{
class ObSimpleThreadPool
{
public:
  ObSimpleThreadPool();
  virtual ~ObSimpleThreadPool();
public:
  int init(const int64_t thread_num, const int64_t task_num_limit);
  void destroy();
  int push(void *task);
  virtual void handle(void *task) = 0;
  int64_t get_queue_num() const { return queue_.get_total(); }
private:
  static void *thread_func_(void *data);
  int launch_thread_(const int64_t thread_num);
  static const int64_t QUEUE_WAIT_TIME = 100 * 1000;
  static const int64_t MAX_THREAD_NUM = 256;
private:
  bool is_inited_;
  bool is_stopped_;
  int64_t thread_num_;
  int64_t task_num_limit_;
  ObCond queue_cond_;
  ObFixedQueue<void> queue_;
  struct ThreadConf
  {
    pthread_t pd;
    ObSimpleThreadPool *host;
    ThreadConf() : pd(0), host(NULL) {}
  };
  ThreadConf thread_conf_array_[MAX_THREAD_NUM];
};
} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_COMMON_OB_SIMPLE_THREAD_POOL_H_
