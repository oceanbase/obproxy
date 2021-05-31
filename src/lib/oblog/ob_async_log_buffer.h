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

#ifndef OCEANBASE_COMMON_OB_ASYNC_LOG_BUFFER_
#define OCEANBASE_COMMON_OB_ASYNC_LOG_BUFFER_

#include "lib/oblog/ob_async_log_struct.h"
#include "lib/lock/Mutex.h"
#include "lib/lock/Monitor.h"

namespace oceanbase
{
namespace common
{

template<typename Type, int size>
class ObLogRingBuffer
{
public:
  ObLogRingBuffer() : destroyed_(false), size_(size), start_(0), end_(0)
  { memset(&elems_, 0, sizeof(elems_)); }
  virtual ~ObLogRingBuffer() { destroy(); }
public:
  bool is_full() const { return (end_ + 1) % size_ == start_; }
  bool is_empty() const { return end_ == start_; }
  int push(const Type &elem, Type &old_elem, bool &overwrite = true);
  int pop(Type &elem);
  void destroy();
private:
  bool destroyed_;
  int size_;
  int start_;
  int end_;
  Type elems_[size];
  tbutil::Monitor<tbutil::Mutex> monitor_;
};

template<typename Type, int size>
int ObLogRingBuffer<Type, size>::push(const Type &elem, Type &old_elem, bool &overwrite)
{
  int ret = common::OB_SUCCESS;

  tbutil::Monitor<tbutil::Mutex>::Lock guard(monitor_);
  if (destroyed_) {
    ret = common::OB_NOT_INIT;
  } else if (is_full() && !overwrite) {
    ret = common::OB_ELECTION_WARN_LOGBUF_FULL;
  } else {
    const bool empty = is_empty();
    elems_[end_] = elem;
    end_ = (end_ + 1) % size_;
    // full, overwrite
    if (end_ == start_) {
      old_elem = elems_[start_];
      start_ = (start_ + 1) % size_;
      overwrite = true;
    } else {
      overwrite = false;
    }
    if (empty) {
      monitor_.notify();
    }
    ret = common::OB_SUCCESS;
  }

  return ret;
}

template<typename Type, int size>
int ObLogRingBuffer<Type, size>::pop(Type &elem)
{
  int ret = common::OB_SUCCESS;

  tbutil::Monitor<tbutil::Mutex>::Lock guard(monitor_);
  while (!destroyed_ && is_empty()) {
    monitor_.wait();
  }
  if (destroyed_) {
    ret = common::OB_NOT_INIT;
  } else {
    elem = elems_[start_];
    start_ = (start_ + 1) % size_;
  }

  return ret;
}

template<typename Type, int size>
void ObLogRingBuffer<Type, size>::destroy()
{
  tbutil::Monitor<tbutil::Mutex>::Lock guard(monitor_);
  if (!destroyed_) {
    monitor_.notifyAll();
    destroyed_ = true;
  }
}
} // common
} // oceanbase
#endif //OCEANBASE_COMMON_OB_ASYNC_LOG_BUFFER_
