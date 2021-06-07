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
 *
 * **************************************************************
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef OBPROXY_THREAD_INTERFACE_H
#define OBPROXY_THREAD_INTERFACE_H

#include "iocore/eventsystem/ob_thread_impl.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{

class ObThread;
class ObProxyMutex;

typedef void *(*ThreadFunction)(void *arg);

extern ObProxyMutex *global_mutex;

static const int MAX_THREAD_NAME_LENGTH = 32;
static const int DEFAULT_STACKSIZE      = 1048576; // 1MB

/**
 * Base class for the threads in the ObEvent System. ObThread is the base
 * class for all the thread classes in the ObEvent System. Objects of the
 * ObThread class represent spawned or running threads and provide minimal
 * information for its derived classes. ObThread objects have a reference
 * to a ObProxyMutex, that is used for atomic operations internally, and
 * an ink_thread member that is used to identify the thread in the system.
 *
 * You should not create an object of the ObThread class, they are typically
 * instantiated after some thread startup mechanism exposed by a processor,
 * but even then you would probably deal with processor functions and
 * not the ObThread object itself.
 */
class ObThread
{
public:
  ObThread() : tid_(0), mutex_(NULL), mutex_ptr_(NULL) {}

  virtual ~ObThread() { }

  int start(const char *name, const int64_t stacksize = DEFAULT_STACKSIZE,
            ThreadFunction f = NULL, void *a = NULL);

  int set_specific();
  virtual void execute() { }

public:
  /**
   * System-wide thread identifier. The thread identifier is represented
   * by the platform independent type ink_thread and it is the system-wide
   * value assigned to each thread. It is exposed as a convenience for
   * processors and you should not modify it directly.
   */
  ObThreadId tid_;

  /**
   * ObThread lock to ensure atomic operations. The thread lock available
   * to derived classes to ensure atomic operations and protect critical
   * regions. Do not modify this member directly.
   */
  ObProxyMutex *mutex_;

  // in multi threads, it can not guarantee cur_time_ to be monotonically increasing
  static ObHRTime cur_time_;

  common::ObPtr<ObProxyMutex> mutex_ptr_;

private:
  // prevent unauthorized copies (Not implemented)
  DISALLOW_COPY_AND_ASSIGN(ObThread);
};

inline ObThread *&this_thread()
{
  static __thread ObThread *thread = NULL;
  return thread;
}

inline int ObThread::set_specific()
{
  this_thread() = this;
  return common::OB_SUCCESS;
}

inline ObHRTime get_hrtime()
{
  return ObThread::cur_time_;
}

} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_THREAD_INTERFACE_H
