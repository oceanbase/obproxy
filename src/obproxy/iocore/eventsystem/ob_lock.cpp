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

#include "iocore/eventsystem/ob_lock.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace event
{

void lock_waiting(const ObSrcLoc &loc, const char *handler)
{
   LOG_DEBUG("WARNING: waiting on lock", K(loc), K(handler));
}

void lock_holding(const ObSrcLoc &loc, const char *handler)
{
   LOG_DEBUG("WARNING: holding too long", K(loc), K(handler));
}

void lock_taken(const ObSrcLoc &loc, const char *handler)
{
  LOG_DEBUG("WARNING: lock taken too many times",  K(loc), K(handler));
}

#ifdef OB_HAS_LOCK_CONTENTION_PROFILING

void ObProxyMutex::print_lock_stats(bool flag)
{
  if (flag) {
    if (total_acquires_ >= 10) {
      _LOG_DEBUG("Lock Stats (Dying):successful %d (%.2f), unsuccessful %d (%.2f) blocking %d",
                  successful_nonblocking_acquires_,
                  (nonblocking_acquires_ > 0 ?
                   successful_nonblocking_acquires_ * 100.0 / nonblocking_acquires_ : 0.0),
                  unsuccessful_nonblocking_acquires_,
                  (nonblocking_acquires_ > 0 ?
                   unsuccessful_nonblocking_acquires_ * 100.0 / nonblocking_acquires_ : 0.0),
                  blocking_acquires_);
    }
  } else {
    if (0 == (total_acquires_ % 100)) {
       _LOG_DEBUG("Lock Stats (Alive):successful %d (%.2f), unsuccessful %d (%.2f) blocking %d",
                  successful_nonblocking_acquires_,
                  (nonblocking_acquires_ > 0 ?
                   successful_nonblocking_acquires_ * 100.0 / nonblocking_acquires_ : 0.0),
                  unsuccessful_nonblocking_acquires_,
                  (nonblocking_acquires_ > 0 ?
                   unsuccessful_nonblocking_acquires_ * 100.0 / nonblocking_acquires_ : 0.0),
                  blocking_acquires_);
    }
  }
}
#endif //OB_HAS_LOCK_CONTENTION_PROFILING

} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase
