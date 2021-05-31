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

#ifndef OBPROXY_OB_PROXY_ASYNC_TASK_H
#define OBPROXY_OB_PROXY_ASYNC_TASK_H

#include "obutils/ob_async_common_task.h"
#include "ob_proxy_operator.h"

namespace oceanbase
{
namespace obproxy
{
namespace engine
{

class ObOperatorAsyncCommonTask : public obutils::ObAsyncCommonTask
{
public:
  ObOperatorAsyncCommonTask(int64_t target_task_count, ObProxyOperator *ob_operator); 
  /* START:callback function for ObAsyncCommonTask */
  virtual int main_handler(int event, void *data);
  virtual int handle_timeout();

  int64_t get_cont_index() { return cont_index_; }
  void set_cont_index(int64_t cont_index) { cont_index_ = cont_index; }

  int64_t get_parallel_task_count() { return parallel_task_count_; }
  int set_parallel_task_action(int64_t cont_index, event::ObAction *action);
  int init_async_task(event::ObContinuation *cont, event::ObEThread *submit_thread);
  void add_target_task_count() {} //TODO need to implement
  event::ObContinuation *get_cb_cont() { return cb_cont_; }
  void destroy();

  //int handle_parallel_task();
//  int handle_parallel_task_complete(void *data);
  int notify_caller_error();
  void cancel_timeout_action();
  void cancel_all_pending_action();
protected:
  int64_t buf_size_;
  int64_t target_task_count_;
  int64_t parallel_task_count_;
public:
  event::ObAction **parallel_action_array_;
  int64_t cont_index_;
  int64_t timeout_ms_;
  ObProxyOperator *ob_operator_;
  DISALLOW_COPY_AND_ASSIGN(ObOperatorAsyncCommonTask);
  /* END:callback function for ObAsyncCommonTask */

};

} /* END:engine */
} /* END:obproxy */
} /* END:oceanbase */

#endif /* OBPROXY_OB_PROXY_ASYNC_TASK_H */
