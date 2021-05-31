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

#ifndef  OCEANBASE_LOCK_LATCH_H_
#define  OCEANBASE_LOCK_LATCH_H_

#include "lib/ob_define.h"
#include "lib/list/ob_dlist.h"
#include "lib/atomic/ob_atomic.h"
namespace oceanbase
{
namespace common
{
struct ObLatchWaitMode
{
  enum ObLatchWaitModeEnum
  {
    NOWAIT = 0,
    READ_WAIT = 1,
    WRITE_WAIT = 2
  };
};

class ObLatch;
typedef int (ObLatch::*low_try_lock)(const uint32_t lock, const uint32_t uid, bool &conflict);

struct ObWaitProc : public ObDLinkBase<ObWaitProc>
{
  ObWaitProc(ObLatch &latch, const uint32_t wait_mode)
    : addr_(&latch),
      mode_(wait_mode),
      wait_(0)
  {
  }
  virtual ~ObWaitProc()
  {
  }
  bool is_valid() const
  {
    return OB_LIKELY(NULL != addr_)
              && OB_LIKELY(ObLatchWaitMode::READ_WAIT == mode_
                  || ObLatchWaitMode::WRITE_WAIT == mode_);
  }
  int64_t to_string(char* buf, const int64_t buf_len) const;
  ObLatch *addr_;
  int32_t mode_;
  volatile int32_t wait_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObWaitProc);
};

class ObLatchWaitQueue
{
public:
  static ObLatchWaitQueue &get_instance();
  int wait(
      ObWaitProc &proc,
      const uint32_t latch_id,
      const uint32_t uid,
      low_try_lock lock_func,
      low_try_lock lock_func_ignore,
      const int64_t abs_timeout_us);
  int wake_up(ObLatch &latch, const bool only_rd_wait = false);
private:
  struct ObLatchBucket
  {
    ObDList<ObWaitProc> wait_list_;
    volatile int32_t lock_;
    ObLatchBucket() : wait_list_(), lock_(0)
    {
    }
  };

  ObLatchWaitQueue();
  virtual ~ObLatchWaitQueue();
  void lock_bucket(ObLatchBucket &bucket);
  void unlock_bucket(ObLatchBucket &bucket);
  int try_lock(
      ObLatchBucket &bucket,
      ObWaitProc &proc,
      const uint32_t latch_id,
      const uint32_t uid,
      low_try_lock lock_func);
private:
  static const uint64_t LATCH_MAP_BUCKET_CNT = 3079;
  ObLatchBucket wait_map_[LATCH_MAP_BUCKET_CNT];
private:
  DISALLOW_COPY_AND_ASSIGN(ObLatchWaitQueue);
};

class ObLatch
{
public:
  ObLatch();
  ~ObLatch();
  int try_rdlock(const uint32_t latch_id);
  int try_wrlock(const uint32_t latch_id, const uint32_t *puid = NULL);
  int rdlock(
      const uint32_t latch_id,
      const int64_t abs_timeout_us = INT64_MAX);
  int wrlock(
      const uint32_t latch_id,
      const int64_t abs_timeout_us = INT64_MAX,
      const uint32_t *puid = NULL);
  int wr2rdlock(const uint32_t *puid = NULL);
  int unlock(const uint32_t *puid = NULL);
  inline bool is_locked() const;
  inline bool is_rdlocked() const;
  inline bool is_wrlocked_by(const uint32_t *puid = NULL) const;
  inline uint32_t get_wid() const;
  int64_t to_string(char* buf, const int64_t buf_len) const;
private:
  int low_lock(
      const uint32_t latch_id,
      const int64_t abs_timeout_us,
      const uint32_t uid,
      const uint32_t wait_mode,
      low_try_lock lock_func,
      low_try_lock lock_func_ignore);
  inline int low_try_rdlock(const uint32_t lock, const uint32_t uid, bool &conflict);
  inline int low_try_wrlock(const uint32_t lock, const uint32_t uid, bool &conflict);
  inline int low_try_rdlock_ignore(const uint32_t lock, const uint32_t uid, bool &conflict);
  inline int low_try_wrlock_ignore(const uint32_t lock, const uint32_t uid, bool &conflict);
  friend class ObLatchWaitQueue;
  static const uint32_t WRITE_MASK = 1<<30;
  static const uint32_t WAIT_MASK = 1<<31;
  volatile uint32_t lock_;
};

class ObLatchRGuard
{
public:
  ObLatchRGuard(ObLatch &lock, const uint32_t latch_id)
      : lock_(lock),
        ret_(OB_SUCCESS)
  {
    if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.rdlock(latch_id)))) {
      COMMON_LOG(ERROR, "lock error, ", K(latch_id), K(ret_));
    }
  }
  ~ObLatchRGuard()
  {
    if (OB_LIKELY(OB_SUCCESS == ret_)) {
      if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.unlock()))) {
        COMMON_LOG(ERROR, "unlock error, ", K(ret_));
      }
    }
  }
  int get_ret() const { return ret_; }
private:
  ObLatch &lock_;
  int ret_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLatchRGuard);
};

class ObLatchWGuard
{
public:
  ObLatchWGuard(ObLatch &lock, const uint32_t latch_id)
      : lock_(lock),
        ret_(OB_SUCCESS)
  {
    if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.wrlock(latch_id)))) {
      COMMON_LOG(ERROR, "lock error, ", K(latch_id), K(ret_));
    }
  }
  ~ObLatchWGuard()
  {
    if (OB_LIKELY(OB_SUCCESS == ret_)) {
      if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.unlock()))) {
        COMMON_LOG(ERROR, "unlock error, ", K(ret_));
      }
    }
  }
  int get_ret() const { return ret_; }
private:
  ObLatch &lock_;
  int ret_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLatchWGuard);
};

/**
 * --------------------------------------------------------Inline methods---------------------------------------------------------
 */
inline bool ObLatch::is_locked() const
{
  return 0 != ATOMIC_LOAD(&lock_);
}

inline bool ObLatch::is_rdlocked() const
{
  uint32_t lock = ATOMIC_LOAD(&lock_);
  return 0 == (lock & WRITE_MASK) && (lock & (~WAIT_MASK)) > 0;
}

inline bool ObLatch::is_wrlocked_by(const uint32_t *puid) const
{
  uint32_t uid = (NULL == puid) ? static_cast<uint32_t>(GETTID()) : *puid;
  uint32_t lock = ATOMIC_LOAD(&lock_);
  return 0 != (lock & WRITE_MASK) && uid == (lock & ~(WAIT_MASK | WRITE_MASK));
}

inline uint32_t ObLatch::get_wid() const
{
  uint32_t lock = ATOMIC_LOAD(&lock_);
  return (0 == (lock & WRITE_MASK)) ? 0 : (lock & ~(WAIT_MASK | WRITE_MASK));
}

inline int ObLatch::low_try_rdlock(const uint32_t lock, const uint32_t uid, bool &conflict)
{
  UNUSED(uid);
  int ret = OB_EAGAIN;
  if (0 == (lock & WAIT_MASK) && 0 == (lock & WRITE_MASK)) {
    conflict = false;
    if (ATOMIC_BCAS(&lock_, lock, lock + 1)) {
      ret = OB_SUCCESS;
    }
  } else {
    conflict = true;
  }
  return ret;
}

inline int ObLatch::low_try_wrlock(const uint32_t lock, const uint32_t uid, bool &conflict)
{
  int ret = OB_EAGAIN;
  if (0 == lock) {
    conflict = false;
    if (ATOMIC_BCAS(&lock_, 0, (WRITE_MASK | uid))) {
      ret = OB_SUCCESS;
    }
  } else {
    conflict = true;
  }
  return ret;
}

inline int ObLatch::low_try_rdlock_ignore(const uint32_t lock, const uint32_t uid, bool &conflict)
{
  int ret = OB_EAGAIN;
  UNUSED(uid);
  if (0 == (lock & WRITE_MASK)) {
    conflict = false;
    if (ATOMIC_BCAS(&lock_, lock, lock + 1)) {
      ret = OB_SUCCESS;
    }
  } else {
    conflict = true;
  }
  return ret;
}

inline int ObLatch::low_try_wrlock_ignore(const uint32_t lock, const uint32_t uid, bool &conflict)
{
  int ret = OB_EAGAIN;
  if (0 == lock || WAIT_MASK == lock) {
    conflict = false;
    if (ATOMIC_BCAS(&lock_, lock, (lock | (WRITE_MASK | uid)))) {
      ret = OB_SUCCESS;
    }
  } else {
    conflict = true;
  }
  return ret;
}

}
}

#endif //OCEANBASE_COMMON_SPIN_RWLOCK_H_
