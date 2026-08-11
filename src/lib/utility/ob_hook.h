/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OBPROXY_OB_HOOK_H_
#define _OBPROXY_OB_HOOK_H_

#include <pthread.h>

namespace oceanbase
{
namespace common
{

extern "C" {
  int pthread_create(pthread_t *thread, const pthread_attr_t *attr, void *(*start_routine)(void*), void *arg) noexcept;
}

} // end of namespace common
} // end of namespace oceanbase

#endif