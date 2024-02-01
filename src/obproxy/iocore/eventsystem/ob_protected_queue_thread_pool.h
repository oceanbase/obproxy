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

#ifndef OBPROXY_PROTECTED_QUEUE_THREAD_POOL_H
#define OBPROXY_PROTECTED_QUEUE_THREAD_POOL_H

#include "iocore/eventsystem/ob_event.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{

class ObProtectedQueueThreadPool
{
public:
  ObProtectedQueueThreadPool() : is_inited_(false) {}
  ~ObProtectedQueueThreadPool() { }

  int init();
  void enqueue(ObEvent *e);
  int dequeue_timed(const ObHRTime timeout, ObEvent *&event);
  int signal();

public:
  bool is_inited_;
  common::ObAtomicList atomic_list_;
  ObMutex lock_;
  ObProxyThreadCond might_have_data_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObProtectedQueueThreadPool);
};

inline int ObProtectedQueueThreadPool::init()
{
  int ret = common::OB_SUCCESS;
  ObEvent e;
  if (OB_FAIL(common::mutex_init(&lock_))) {
    PROXY_EVENT_LOG(WDIAG, "fail to init mutex", K(ret));
  } else if (OB_FAIL(atomic_list_.init("ObProtectedQueueThreadPool",
                                       reinterpret_cast<char *>(&e.link_.next_) - reinterpret_cast<char *>(&e)))) {
    PROXY_EVENT_LOG(WDIAG, "fail to init atomic_list_", K(ret));
  } else if (OB_FAIL(cond_init(&might_have_data_))) {
    PROXY_EVENT_LOG(WDIAG, "fail to init ObProxyThreadCond", K(ret));
  } else {
    is_inited_ = 0;
  }
  return ret;
}

inline int ObProtectedQueueThreadPool::signal()
{
  int ret = common::OB_SUCCESS;
  // Need to get the lock before you can signal the thread
  if (OB_FAIL(common::mutex_acquire(&lock_))) {
    PROXY_EVENT_LOG(EDIAG, "fail to acquire lock", K(ret));
  } else {
    if (OB_FAIL(cond_signal(&might_have_data_))) {
      PROXY_EVENT_LOG(WDIAG, "fail to call cond_signal", K(ret));
    }

    int tmp_ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(common::OB_SUCCESS != ( tmp_ret = common::mutex_release(&lock_)))) {
      PROXY_EVENT_LOG(WDIAG, "fail to release mutex", K(tmp_ret));
    }
  }
  return ret;
}

} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_PROTECTED_QUEUE_THREAD_POOL_H
