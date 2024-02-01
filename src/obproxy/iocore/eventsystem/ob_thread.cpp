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

#include "iocore/eventsystem/ob_event_system.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace event
{
ObProxyMutex *global_mutex = NULL;
ObHRTime ObThread::cur_time_ = 0;

struct ObThreadDataInternal
{
  ThreadFunction f_;
  void *a_;
  ObThread *me_;
  char name_[MAX_THREAD_NAME_LENGTH];
};

static void *spawn_thread_internal(void *a)
{
  if (OB_UNLIKELY(NULL != a)) {
    int ret = OB_SUCCESS;
    ObThreadDataInternal *p = reinterpret_cast<ObThreadDataInternal *>(a);

    if(OB_FAIL(p->me_->set_specific())) {
      LOG_EDIAG("failed to set_specific for thread", "name", p->name_, K(ret));
    } else if (OB_FAIL(set_thread_name(p->name_))) {
      LOG_EDIAG("failed to set_thread_name", "name", p->name_, K(ret));
    } else {
      if (NULL != p->f_) {
        p->f_(p->a_);
      } else {
        p->me_->execute();
      }
    }
    delete p;
  }
  return NULL;
}

int ObThread::start(const char *name, const int64_t stacksize, ThreadFunction f, void *a)
{
  int ret = OB_SUCCESS;
  ObThreadDataInternal *p = NULL;

  if (OB_ISNULL(p = new(std::nothrow) ObThreadDataInternal())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_EDIAG("failed to allocate memory for thread data", K(ret));
  } else {
    p->f_ = f;
    p->a_ = a;
    p->me_ = this;
    int32_t len = snprintf(p->name_, MAX_THREAD_NAME_LENGTH, "%s", name);
    if (len <= 0 || len >= MAX_THREAD_NAME_LENGTH) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WDIAG("failed format thread name", K(len), K(MAX_THREAD_NAME_LENGTH), K(ret));
    } else {
      if ((tid_ = thread_create(spawn_thread_internal, (void *)p, 0, stacksize)) <= 0) {
        ret = OB_ERR_SYS;
        LOG_EDIAG("failed to create thread", K(ret));
      }
    }
  }

  if (OB_FAIL(ret) && NULL != p) {
    delete p;
    p = NULL;
  }
  return ret;
}

} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase
