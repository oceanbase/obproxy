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

#include "lib/queue/ob_lighty_queue.h"
#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>
#include "lib/utility/utility.h"

namespace oceanbase
{
namespace common
{
static int64_t get_us() { return ::oceanbase::common::ObTimeUtility::current_time(); }

int LightyQueue::init(const uint64_t capacity, const uint32_t mod_id)
{
  return queue_.init(capacity, global_default_allocator, mod_id);
}

int LightyQueue::push(void *data, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  int64_t abs_timeout = get_us() + timeout;
  int64_t wait_timeout = 0;
  while (true) { // WHITESCAN: OB_CUSTOM_ERROR_COVERED
    uint32_t seq = cond_.get_seq();
    if (OB_SUCCESS == (ret = queue_.push(data))) {
      break;
    } else if ((wait_timeout = abs_timeout - get_us()) <= 0) {
      break;
    } else {
      cond_.wait(seq, wait_timeout);
    }
  }
  if (OB_SUCCESS == ret) {
    cond_.signal();
  }
  return ret;
}

int LightyQueue::pop(void *&data, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  int64_t abs_timeout = get_us() + timeout;
  int64_t wait_timeout = 0;
  while (true) { // WHITESCAN: OB_CUSTOM_ERROR_COVERED
    uint32_t seq = cond_.get_seq();
    if (OB_SUCCESS == (ret = queue_.pop(data))) {
      break;
    } else if ((wait_timeout = abs_timeout - get_us()) <= 0) {
      break;
    } else {
      cond_.wait(seq, wait_timeout);
    }
  }
  if (OB_SUCCESS == ret) {
    cond_.signal();
  }
  return ret;
}

int LightyQueue::pop_with_priority(void *&data, const int64_t timeout_us,
    uint32_t remain_limit)
{
  int ret = OB_SUCCESS;
  remain_limit = remain_limit % 8;
  if (0 == remain_limit || size() > static_cast<int64_t>((max_size() >> 3) * remain_limit)) {
    int64_t abs_timeout = get_us() + timeout_us;
    int64_t wait_timeout = 0;
    while (true) { // WHITESCAN: OB_CUSTOM_ERROR_COVERED
      uint32_t seq = cond_.get_seq();
      if (OB_SUCCESS == (ret = queue_.pop(data))) {
        break;
      } else if ((wait_timeout = abs_timeout - get_us()) <= 0) {
        break;
      } else {
        cond_.wait(seq, wait_timeout);
      }
    }
    if (OB_SUCCESS == ret) {
      cond_.signal();
    }
  } else {
    ret = OB_BUF_NOT_ENOUGH;
  }
  return ret;
}

void LightyQueue::reset()
{
  void *p = NULL;
  while (0 == pop(p, 0))
    ;
}
}; // end namespace common
}; // end namespace oceanbase
