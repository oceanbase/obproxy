/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <sys/time.h>
#include "Cond.h"
#include "lib/oblog/ob_log.h"
namespace tbutil
{
Cond::Cond()
{
    int rt = pthread_cond_init(&_cond, NULL);
    if (0 != rt) {
      _OB_LOG(WDIAG, "Failed to init cond, err=%d", rt);
    }
}

Cond::~Cond()
{
  int ret = pthread_cond_destroy(&_cond);
  if (0 != ret) {
    _OB_LOG(WDIAG, "Failed to destroy cond, err=%d", ret);
  }
}

void Cond::signal()
{
    const int rt = pthread_cond_signal(&_cond);
    if (0 != rt) {
      _OB_LOG(WDIAG, "Failed to signal condition, err=%d", rt);
    }
}

void Cond::broadcast()
{
    const int rt = pthread_cond_broadcast(&_cond);
    if (0 != rt) {
      _OB_LOG(WDIAG, "Failed to broadcast condition, err=%d", rt);
    }
}
}//end namespace tbutil
