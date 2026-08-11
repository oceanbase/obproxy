/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PROXY

#include "lib/utility/ob_hook.h"

#include <dlfcn.h>
#include <exception>
#include <utility>
#include <cstdlib>

#include "lib/ob_define.h"
#include "lib/alloc/alloc_assist.h"
#include "lib/oblog/ob_log_module.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace common
{


typedef void* (*start_routine_t)(void*);
typedef int (*pthread_create_t)(pthread_t*, const pthread_attr_t*, start_routine_t, void*);

// 线程本地存储记录异常
static thread_local std::exception_ptr thread_exception = nullptr;
static pthread_create_t org_pthread_create = NULL;

void* wrapped_routine(void* arg)
{
  void* ret = NULL;

  start_routine_t start_routine = NULL;
  void* org_arg = NULL;
  std::pair<start_routine_t, void*>* pair_args = static_cast<std::pair<start_routine_t, void*>*>(arg);

  if (OB_ISNULL(pair_args)
      || OB_ISNULL(pair_args->first)) {
    LOG_ERROR("unexpected arg for wrapped_routine", K(lbt()));
    if (OB_NOT_NULL(pair_args)) {
      delete pair_args;
    }
  } else {
    start_routine = pair_args->first;
    org_arg = pair_args->second;
    delete pair_args;
    try {
      ret = start_routine(org_arg);
    } catch (const std::exception& e) {
      LOG_ERROR("Caught std::exception", KP(e.what()), K(e.what()), K(lbt()));
    } catch (int e) {
      LOG_ERROR("Caught int exception", K(e), K(lbt()));
    } catch (double e) {
      LOG_ERROR("Caught double exception", K(e), K(lbt()));
    } catch (void* e) {
      LOG_ERROR("Caught pointer exception", KP(e), K(lbt()));
    } catch (...) {
      // 某些异常不能被忽略，可能是 GLIBC（操作系统） 的特定功能 feature，必须 throw 到外层函数!!!
      thread_exception = std::current_exception();
      LOG_INFO("Caught unexpected, maybe OS feature", "exception name", typeid(thread_exception).name(), K(lbt()));
      throw;
    }
  }

  return ret;
}

extern "C" int pthread_create(pthread_t *thread, const pthread_attr_t *attr,
                              void *(*start_routine)(void*), void *arg) noexcept
{
  int ret = common::OB_SUCCESS;
  const char *err = NULL;

  if (OB_ISNULL(org_pthread_create)) {
    dlerror();  // 清除之前的错误
    org_pthread_create = (pthread_create_t)dlsym(RTLD_NEXT, "pthread_create");
    err = dlerror();
  }

  if (OB_NOT_NULL(err)) {
    LOG_ERROR("fail to dlsym", KP(err), K(err));
    ob_abort();
  } else {
    std::pair<start_routine_t, void*> * wrapped_args = new std::pair<start_routine_t, void*>(start_routine, arg);
    if (OB_FAIL(org_pthread_create(thread, attr, wrapped_routine, wrapped_args))) {
      delete wrapped_args; // 创建线程失败时清理
    }
  }

  return ret;
}

} // end of namespace common
} // end of namespace oceanbase