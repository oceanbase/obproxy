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

#ifndef OBPROXY_TIMERFD_MANAGER_H
#define OBPROXY_TIMERFD_MANAGER_H

#include <sys/timerfd.h>

namespace oceanbase
{
namespace obproxy
{
namespace net
{

class ObTimerFdManager
{
  ObTimerFdManager() { };
  ~ObTimerFdManager() { };
public:
  static int timerfd_create(int clockid, int flags, int &timerfd);
  static int timerfd_settime(int timerfd, int flags, int64_t new_value_sec, int64_t new_value_nsec);
  static void timerfd_close(int timerfd);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTimerFdManager);
};

inline int ObTimerFdManager::timerfd_create(int clockid, int flags, int &timerfd)
{
  int ret = common::OB_SUCCESS;
  timerfd = ::timerfd_create(clockid, flags);
  if (OB_UNLIKELY(timerfd < 0)) {
    ret = ob_get_sys_errno();
  }
  return ret;
}

inline int ObTimerFdManager::timerfd_settime(int timerfd, int flags, int64_t new_value_sec, int64_t new_value_nsec)
{
  int ret = common::OB_SUCCESS;

  struct itimerspec new_value;
  new_value.it_value.tv_sec = new_value_sec; //Turn off the timer during initialization
  new_value.it_value.tv_nsec = new_value_nsec;

  new_value.it_interval.tv_sec = 0;
  new_value.it_interval.tv_nsec = 0;

  // Set the timer
  ret = ::timerfd_settime(timerfd, flags, &new_value, NULL);
  if (OB_UNLIKELY(ret < 0)) {
   ret = ob_get_sys_errno();
  }
  return ret;
}

inline void ObTimerFdManager::timerfd_close(int timerfd)
{
  if (timerfd >= 0) {
    ::close(timerfd);
  }
}

} // end of namespace net
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_TIMERFD_MANAGER_H
