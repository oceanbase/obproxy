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

#ifndef OB_BUCKET_LOCK_H_
#define OB_BUCKET_LOCK_H_
#include "lib/lock/ob_latch.h"
#include "lib/stat/ob_latch_define.h"
#include "lib/allocator/ob_mod_define.h"

namespace oceanbase
{
namespace common
{
class ObBucketLock
{
public:
  ObBucketLock();
  virtual ~ObBucketLock();
  int init(
      const uint64_t bucket_cnt,
      const uint32_t latch_id = ObLatchIds::DEFAULT_BUCKET_LOCK,
      const int64_t mod_id = ObModIds::BUCKET_LOCK,
      const uint64_t tenant_id = OB_SERVER_TENANT_ID);
  void destroy();
  int try_rdlock(const uint64_t bucket_idx);
  int try_wrlock(const uint64_t bucket_idx);
  int rdlock(const uint64_t bucket_idx);
  int wrlock(const uint64_t bucket_idx);
  int unlock(const uint64_t bucket_idx);
private:
  uint64_t bucket_cnt_;
  uint64_t latch_cnt_;
  ObLatch *latches_;
  uint32_t latch_id_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBucketLock);
};

class ObBucketRLockGuard
{
public:
  ObBucketRLockGuard(ObBucketLock &lock, const uint64_t index)
     : lock_(lock),
       index_(index),
       ret_(OB_SUCCESS)
  {
    if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.rdlock(index_)))) {
      COMMON_LOG(WARN, "Fail to read lock bucket, ", K_(index), K_(ret));
    }
  };
  ~ObBucketRLockGuard()
  {
    if (OB_LIKELY(OB_SUCCESS == ret_)) {
      if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.unlock(index_)))) {
        COMMON_LOG(WARN, "Fail to unlock bucket, ", K_(ret));
      }
    }
  };
  inline int get_ret() const { return ret_; }
private:
  ObBucketLock &lock_;
  const uint64_t index_;
  int ret_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBucketRLockGuard);
};

class ObBucketWLockGuard
{
public:
  ObBucketWLockGuard(ObBucketLock &lock, const uint64_t index)
     : lock_(lock),
       index_(index),
       ret_(OB_SUCCESS)
  {
    if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.wrlock(index_)))) {
      COMMON_LOG(WARN, "Fail to write lock bucket, ", K_(index), K_(ret));
    }
  };
  ~ObBucketWLockGuard()
  {
    if (OB_LIKELY(OB_SUCCESS == ret_)) {
      if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.unlock(index_)))) {
        COMMON_LOG(WARN, "Fail to unlock bucket, ", K_(index), K_(ret));
      }
    }
  };
  inline int get_ret() const { return ret_; }
private:
  ObBucketLock &lock_;
  const uint64_t index_;
  int ret_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBucketWLockGuard);
};
} /* namespace common */
} /* namespace oceanbase */

#endif /* OB_BUCKET_LOCK_H_ */
