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

#define USING_LOG_PREFIX PROXY_EVENT

#include "iocore/eventsystem/ob_blocking_task.h"
#include "iocore/eventsystem/ob_event_system.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace event
{
ObEventThreadType ET_BLOCKING = ET_CALL;
ObBlockingTaskProcessor g_blocking_task_processor;

// Note that if the number of task_threads is 0, all continuations scheduled for
// ET_BLOCKING ends up running on ET_CALL (which is the net-threads).
int ObBlockingTaskProcessor::start(const int64_t blocking_threads, const int64_t stacksize)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(g_event_processor.spawn_event_threads(blocking_threads, "ET_BLOCKING", stacksize, ET_BLOCKING))) {
    LOG_WARN("fail to spawn event threads for ET_BLOCKING", K(ret));
  }
  return ret;
}
} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase
