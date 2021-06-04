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
 * *************************************************************
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#ifndef OBPROXY_PROCESSOR_H
#define OBPROXY_PROCESSOR_H

#include "iocore/eventsystem/ob_thread.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{

class ObThread;

/**
 * Base class for all of the IO Core processors.
 *
 * The ObProcessor class defines a common interface for all the
 * processors in the IO Core. A processor is multithreaded subsystem
 * specialized in some type of task or application. For example,
 * the ObEvent System module includes the ObEventProcessor which provides
 * scheduling services, the Net module includes the ObNetProcessor
 * which provides networking services, etc.
 *
 * You cannot create objects of the ObProcessor class and its methods
 * have no implementation. Therefore, you are expected to use objects
 * of a derived type.
 *
 * Most of such derived classes, provide a singleton object and is
 * common case to have a single instance in that application scope.
 *
 * ObProcessor objects process requests which are placed in the ObProcessor's
 * input queue. A ObProcessor can contain multiple threads to process
 * requests in the queue.  Requests in the queue are Continuations, which
 * describe functions to run, and what to do when the function is complete
 * (if anything).
 *
 * Basically, Processors should be viewed as multi-threaded schedulers which
 * process request Continuations from their queue.  Requests can be made of
 * a ObProcessor either by directly adding a request ObContinuation to the queue,
 * or more conveniently, by calling a method service call which synthesizes
 * the appropriate request ObContinuation and places it in the queue.
 */
class ObProcessor
{
protected:
  ObProcessor() { }

public:
  virtual ~ObProcessor() { }

  /**
   * Returns a ObThread appropriate for the processor.
   * Returns a new instance of a ObThread or ObThread derived class of
   * a thread which is the thread class for the processor.
   *
   * @param thread_index
   *               reserved for future use.
   *
   * @return
   */
  virtual ObThread *create_thread(const int64_t thread_index)
  {
    UNUSED(thread_index);
    return NULL;
  }

  /**
   * Returns the number of threads required for this processor. If
   * the number is not defined or not used, it is equal to 0.
   *
   * @return
   */
  virtual int64_t get_thread_count() { return 0; }

  /**
   * This function attempts to stop the processor. Please refer to
   * the documentation on each processor to determine if it is
   * supported.
   */
  virtual void shutdown() { }

  /**
   * Starts execution of the processor.
   * Attempts to start the number of threads specified for the
   * processor, initializes their states and sets them running. On
   * failure it returns a negative value.
   *
   * @param number_of_threads
   *                  Positive value indicating the number of
   *                  threads to spawn for the processor.
   * @param stacksize The thread stack size to use for this processor.
   *
   * @return
   */
  virtual int start(const int64_t number_of_threads,
                    const int64_t stacksize = DEFAULT_STACKSIZE)
  {
    UNUSED(number_of_threads);
    UNUSED(stacksize);
    return 0;
  }

private:
  // prevent unauthorized copies (Not implemented)
  DISALLOW_COPY_AND_ASSIGN(ObProcessor);
};

} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase

#endif //OBPROXY_PROCESSOR_H
