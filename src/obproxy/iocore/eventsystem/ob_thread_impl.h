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

#ifndef OBPROXY_EVENT_THREAD_H
#define OBPROXY_EVENT_THREAD_H

#include <sched.h>
#include <pthread.h>
#include <signal.h>
#include "lib/time/ob_hrtime.h"
#include "lib/lock/ob_mutex.h"
#include "lib/oblog/ob_log.h"
#include "utils/ob_proxy_lib.h"

typedef pthread_t ObThreadId;
typedef pthread_cond_t ObProxyThreadCond;
typedef pthread_key_t ObThreadKey;

namespace oceanbase
{
namespace obproxy
{
namespace event
{
// The POSIX threads interface
static inline int thread_key_create(ObThreadKey *key, void (*destructor)(void *value))
{
  int ret = common::OB_SUCCESS;
  int err_code = 0;
  if (OB_UNLIKELY(0 != (err_code = pthread_key_create(key, destructor)))) {
    ret = ob_get_sys_errno(err_code);
    PROXY_EVENT_LOG(ERROR, "failed to pthread_key_create", KERRNOMSGS(err_code), K(ret));
  }
  return ret;
}

static inline int thread_setspecific(ObThreadKey key, void *value)
{
  int ret = common::OB_SUCCESS;
  int err_code = 0;
  if (OB_UNLIKELY(0 != (err_code = pthread_setspecific(key, value)))) {
    ret = ob_get_sys_errno(err_code);
    PROXY_EVENT_LOG(ERROR, "failed to pthread_setspecific", KERRNOMSGS(err_code), K(ret));
  }
  return ret;
}

static inline void *thread_getspecific(ObThreadKey key)
{
  return pthread_getspecific(key);
}

static inline int thread_key_delete(ObThreadKey key)
{
  int ret = common::OB_SUCCESS;
  int err_code = 0;
  if (OB_UNLIKELY(0 != (err_code = pthread_key_delete(key)))) {
    ret = ob_get_sys_errno(err_code);
    PROXY_EVENT_LOG(ERROR, "failed to pthread_key_delete", KERRNOMSGS(err_code), K(ret));
  }
  return ret;
}

static inline ObThreadId thread_create(void * (*f)(void *), void *a,
                                       const bool detached = 0, const int64_t stacksize = 0)
{
  ObThreadId ret_threadid = 0;
  int err_code = 0;
  pthread_attr_t attr;
  if (OB_UNLIKELY(0 != (err_code = pthread_attr_init(&attr)))) {
    PROXY_EVENT_LOG(ERROR, "failed to pthread_attr_init", KERRNOMSGS(err_code));
  } else if (OB_UNLIKELY(0 != (err_code = pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM)))) {
    PROXY_EVENT_LOG(ERROR, "failed to pthread_attr_setscope", KERRNOMSGS(err_code));
  } else if (stacksize > 0 && OB_UNLIKELY(0 != (err_code = pthread_attr_setstacksize(&attr, stacksize)))) {
    PROXY_EVENT_LOG(ERROR, "failed to pthread_attr_setstacksize", KERRNOMSGS(err_code));
  } else if (0 != detached && OB_UNLIKELY(0 != (err_code = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED)))) {
    PROXY_EVENT_LOG(ERROR, "failed to pthread_attr_setdetachstate", KERRNOMSGS(err_code));
  } else if (OB_UNLIKELY(0 != (err_code = pthread_create(&ret_threadid, &attr, f, a)))) {
    PROXY_EVENT_LOG(ERROR, "failed to pthread_create", KERRNOMSGS(err_code));
    ret_threadid = 0;
  } else {
    //do nothing
  }

  if (OB_UNLIKELY(0 != (err_code = pthread_attr_destroy(&attr)))) {
    PROXY_EVENT_LOG(ERROR, "failed to pthread_attr_destroy", KERRNOMSGS(err_code));
  }
  /**
   * If the thread has not been created successfully return 0.
   */
  return ret_threadid;
}

static inline int thread_cancel(ObThreadId who)
{
  int ret = common::OB_SUCCESS;
  int err_code = 0;
  if (OB_UNLIKELY(0 != (err_code = pthread_cancel(who)))) {
    ret = ob_get_sys_errno(err_code);
    PROXY_EVENT_LOG(ERROR, "failed to pthread_cancel", KERRNOMSGS(err_code), K(ret));
  }
  return ret;
}

static inline int thread_join(ObThreadId t)
{
  int ret = common::OB_SUCCESS;
  int err_code = 0;
  void *void_tmp = NULL;
  if (OB_UNLIKELY(0 != (err_code = pthread_join(t, &void_tmp)))) {
    ret = ob_get_sys_errno(err_code);
    PROXY_EVENT_LOG(ERROR, "failed to pthread_join", KERRNOMSGS(err_code), K(ret));
  }
  return ret;
}

static inline ObThreadId thread_self()
{
  return (pthread_self());
}

static inline int thread_get_priority(ObThreadId t, int *priority)
{
  int ret = common::OB_SUCCESS;
  int err_code = 0;
  int policy = 0;
  struct sched_param param;
  if(OB_UNLIKELY(0 != (err_code = pthread_getschedparam(t, &policy, &param)))) {
    ret = ob_get_sys_errno(err_code);
    PROXY_EVENT_LOG(ERROR, "failed to pthread_getschedparam", KERRNOMSGS(err_code), K(ret));
  } else {
    *priority = param.sched_priority;
  }
  return ret;
}

static inline int thread_sigsetmask(int how, const sigset_t *set, sigset_t *oset)
{
  int ret = common::OB_SUCCESS;
  int err_code = 0;
  if (OB_UNLIKELY(0 != (err_code = pthread_sigmask(how, set, oset)))) {
    ret = ob_get_sys_errno(err_code);
    PROXY_EVENT_LOG(ERROR, "failed to pthread_sigmask", KERRNOMSGS(err_code), K(ret));
  }
  return ret;
}

// Posix Condition Variables
static inline int cond_init(ObProxyThreadCond *cp)
{
  int ret = common::OB_SUCCESS;
  int err_code = 0;
  if (OB_UNLIKELY(0 != (err_code = pthread_cond_init(cp, NULL)))) {
    ret = ob_get_sys_errno(err_code);
    PROXY_EVENT_LOG(ERROR, "failed to pthread_cond_init", KERRNOMSGS(err_code), K(ret));
  }
  return ret;
}

static inline int cond_destroy(ObProxyThreadCond *cp)
{
  int ret = common::OB_SUCCESS;
  int err_code = 0;
  if (OB_UNLIKELY(0 != (err_code = pthread_cond_destroy(cp)))) {
    ret = ob_get_sys_errno(err_code);
    PROXY_EVENT_LOG(ERROR, "failed to pthread_cond_destroy", KERRNOMSGS(err_code), K(ret));
  }
  return ret;
}

static inline int cond_wait(ObProxyThreadCond *cp, ObMutex *mp)
{
  int ret = common::OB_SUCCESS;
  int err_code = 0;
  if (OB_UNLIKELY(0 != (err_code = pthread_cond_wait(cp, mp)))) {
    ret = ob_get_sys_errno(err_code);
    PROXY_EVENT_LOG(ERROR, "failed to pthread_cond_wait", KERRNOMSGS(err_code), K(ret));
  }
  return ret;
}

static inline int cond_timedwait(ObProxyThreadCond *cp, ObMutex *mp, struct timespec *t)
{
  int ret = common::OB_SUCCESS;
  int err_code = 0;
  while (EINTR == (err_code = pthread_cond_timedwait(cp, mp, t)));
  if (OB_UNLIKELY(0 != err_code) && OB_UNLIKELY(ETIME != err_code) && OB_UNLIKELY(ETIMEDOUT != err_code)) {
    ret = ob_get_sys_errno(err_code);
    PROXY_EVENT_LOG(ERROR, "failed to pthread_cond_timedwait", KERRNOMSGS(err_code), K(ret));
  } else {
    if (0 != err_code) {
      ret = ob_get_sys_errno(err_code);
    }
  }
  return ret;
}

static inline int cond_signal(ObProxyThreadCond *cp)
{
  int ret = common::OB_SUCCESS;
  int err_code = 0;
  if (OB_UNLIKELY(0 != (err_code = pthread_cond_signal(cp)))) {
    ret = ob_get_sys_errno(err_code);
    PROXY_EVENT_LOG(ERROR, "failed to pthread_cond_signal", KERRNOMSGS(err_code), K(ret));
  }
  return ret;
}

static inline int cond_broadcast(ObProxyThreadCond *cp)
{
  int ret = common::OB_SUCCESS;
  int err_code = 0;
  if (OB_UNLIKELY(0 != (err_code = pthread_cond_broadcast(cp)))) {
    ret = ob_get_sys_errno(err_code);
    PROXY_EVENT_LOG(ERROR, "failed to pthread_cond_broadcast", KERRNOMSGS(err_code), K(ret));
  }
  return ret;
}

static inline int thread_yield()
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(0 != sched_yield())) {
    ret = ob_get_sys_errno();
    PROXY_EVENT_LOG(ERROR, "failed to sched_yield", KERRMSGS, K(ret));
  }
  return ret;
}

static inline void thread_exit(void *status)
{
  pthread_exit(status);
}

// This define is from Linux's <sys/prctl.h> and is most likely very
// Linux specific...
static inline int set_thread_name(const char *name)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(0 != prctl(PR_SET_NAME, name, 0, 0, 0))) {
    ret = ob_get_sys_errno();
    PROXY_EVENT_LOG(ERROR, "failed to prctl PR_SET_NAME", KERRMSGS, K(ret));
  }
  return ret;
}

} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_EVENT_THREAD_H
