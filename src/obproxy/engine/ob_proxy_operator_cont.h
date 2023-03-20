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

#ifndef OBPROXY_OPERATOR_CONT_H
#define OBPROXY_OPERATOR_CONT_H

#include "obutils/ob_async_common_task.h"
#include "ob_proxy_operator.h"

namespace oceanbase
{
namespace obproxy
{
namespace engine
{

class ObProxyOperatorCont : public obutils::ObAsyncCommonTask
{
public:
  ObProxyOperatorCont(event::ObContinuation *cb_cont, event::ObEThread *submit_thread)
      : ObAsyncCommonTask(cb_cont->mutex_, "operator cont", cb_cont, submit_thread),
        seq_(0), timeout_ms_(0), operator_root_(NULL), execute_thread_(NULL), data_(NULL),
        buf_(NULL), buf_reader_(NULL)
  {
    SET_HANDLER(&ObProxyOperatorCont::main_handler);
  }

  ~ObProxyOperatorCont() {}

  int main_handler(int event, void *data);
  int init(ObProxyOperator* operator_root, uint8_t seq, const int64_t timeout_ms = 0);
  virtual int init_task();
  virtual int finish_task(void *data);
  virtual void *get_callback_data() { return buf_reader_; }
  virtual int schedule_timeout();
  virtual void destroy();

private:
  int build_executor_resp(event::ObMIOBuffer *write_buf, uint8_t &seq, ObProxyResultResp *result_resp);

private:
  uint8_t seq_;
  int64_t timeout_ms_;
  ObProxyOperator* operator_root_;
  event::ObEThread *execute_thread_;
  void *data_;
  event::ObMIOBuffer *buf_;
  event::ObIOBufferReader *buf_reader_;
  DISALLOW_COPY_AND_ASSIGN(ObProxyOperatorCont);
};

} // end of namespace engine
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_OPERATOR_CONT_H
