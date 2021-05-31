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

#ifndef OCEANBASE_LIB_OB_QSYNC_
#define OCEANBASE_LIB_OB_QSYNC_

#include "lib/ob_define.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/thread_local/ob_tsi_utils.h"

namespace oceanbase
{
namespace common
{
class ObQSync
{
public:
  enum { MAX_REF_CNT = OB_MAX_CPU_NUM };
  struct Ref
  {
    Ref(): ref_(0) {}
    ~Ref() {}
    int64_t ref_ CACHE_ALIGNED;
  };
  ObQSync() {}
  virtual ~ObQSync() {}
  int64_t acquire_ref()
  {
    int64_t idx = icpu_id();
    ref(idx, 1);
    return idx;
  }
  void release_ref(int64_t idx)
  {
    ref(idx, -1);
  }
  void sync()
  {
    for(int64_t i = 0; i < MAX_REF_CNT; i++) {
      Ref* ref = ref_array_ + i;
      if (NULL != ref) {
        while(ATOMIC_LOAD(&ref->ref_) != 0)
          ;
      }
    }
  }

private:
  void ref(int64_t idx, int64_t x)
  {
    Ref* ref = ref_array_ + idx;
    if (NULL != ref) {
      (void)ATOMIC_FAA(&ref->ref_, x);
    }
  }

private:
  Ref ref_array_[MAX_REF_CNT];
};

struct QSyncCriticalGuard
{
  explicit QSyncCriticalGuard(ObQSync& qsync): qsync_(qsync), ref_(qsync.acquire_ref()) {}
  ~QSyncCriticalGuard() { qsync_.release_ref(ref_); }
  ObQSync& qsync_;
  int64_t ref_;
};

#define CriticalGuard(qs) common::QSyncCriticalGuard critical_guard((qs))
#define WaitQuiescent(qs) (qs).sync()

}; // end namespace common
}; // end namespace oceanbase

#endif
