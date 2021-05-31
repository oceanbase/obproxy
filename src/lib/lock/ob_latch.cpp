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

#include "ob_latch.h"
#include "lib/time/ob_time_utility.h"
#include "lib/stat/ob_latch_define.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/ob_worker.h"

namespace oceanbase
{
namespace common
{
/**
 * -------------------------------------------------------ObLatchWaitQueue---------------------------------------------------------------
 */
ObLatchWaitQueue::ObLatchWaitQueue()
  : wait_map_()
{
}

ObLatchWaitQueue::~ObLatchWaitQueue()
{
}

ObLatchWaitQueue &ObLatchWaitQueue::get_instance()
{
  static ObLatchWaitQueue instance_;
  return instance_;
}

int ObLatchWaitQueue::wait(
    ObWaitProc &proc,
    const uint32_t latch_id,
    const uint32_t uid,
    low_try_lock lock_func,
    low_try_lock lock_func_ignore,
    const int64_t abs_timeout_us)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_UNLIKELY(!proc.is_valid())
      || OB_UNLIKELY(latch_id >= ObLatchIds::LATCH_END)
      || OB_UNLIKELY(0 == uid)
      || OB_UNLIKELY(uid >= ObLatch::WRITE_MASK)
      || OB_UNLIKELY(NULL == lock_func)
      || OB_UNLIKELY(NULL == lock_func_ignore)
      || OB_UNLIKELY(abs_timeout_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", K(proc), K(uid), K(abs_timeout_us), K(ret));
  } else {
    ObLatch &latch = *proc.addr_;
    uint64_t pos = reinterpret_cast<uint64_t>((&latch)) % LATCH_MAP_BUCKET_CNT;
    ObLatchBucket &bucket = wait_map_[pos];
    int64_t timeout = 0;
    bool conflict = false;
    struct timespec ts;

    //check if need wait
    if (OB_FAIL(try_lock(bucket, proc, latch_id, uid, lock_func))) {
      if (OB_EAGAIN != ret) {
        COMMON_LOG(ERROR, "Fail to try lock, ", K(ret));
      }
    }

    //wait signal or timeout
    while (OB_EAGAIN == ret) {
      timeout = abs_timeout_us - ObTimeUtility::current_time();
      if (OB_UNLIKELY(timeout <= 0)) {
        ret = OB_TIMEOUT;
        COMMON_LOG(DEBUG, "Wait latch timeout, ", K(abs_timeout_us), K(lbt()), K(latch.get_wid()), K(ret));
      } else {
        ts.tv_sec = timeout / 1000000;
        ts.tv_nsec = 1000 * (timeout % 1000000);
        tmp_ret = futex_wait(&proc.wait_, 1, &ts);
        if (ETIMEDOUT != tmp_ret) {
          //try lock
          conflict = false;
          while(!conflict) {
            if (OB_SUCC((latch.*lock_func_ignore)(latch.lock_, uid, conflict))) {
              break;
            }

#if defined(__aarch64__)
            asm("yield");
#else
            asm("pause");
#endif
          }

          if (OB_EAGAIN == ret) {
            if (OB_FAIL(try_lock(bucket, proc, latch_id, uid, lock_func_ignore))) {
              if (OB_EAGAIN != ret) {
                COMMON_LOG(ERROR, "Fail to try lock, ", K(ret));
              }
            }
          }
        } else {
          ret = OB_TIMEOUT;
          COMMON_LOG(DEBUG, "Wait latch timeout, ", K(abs_timeout_us), K(lbt()),
            K(latch.get_wid()), K(ret));
        }
      }
    }

    //remove proc from wait list if it is necessary, in the common case, we do not need remove
    if (OB_UNLIKELY(1 == proc.wait_)) {
      lock_bucket(bucket);
      if (1 == proc.wait_) {
        if (OB_ISNULL(bucket.wait_list_.remove(&proc))) {
          //should not happen
          tmp_ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(ERROR, "Fail to remove proc, ", K(tmp_ret));
        } else {
          //if there is not any wait proc, clear the wait mask
          bool has_wait = false;
          for (ObWaitProc *iter = bucket.wait_list_.get_first();
              iter != bucket.wait_list_.get_header();
              iter = iter->get_next()) {
            if (iter->addr_ == proc.addr_) {
              has_wait = true;
              break;
            }
          }

          if (!has_wait) {
            __sync_and_and_fetch(&latch.lock_, ~latch.WAIT_MASK);
          }
        }
      }
      unlock_bucket(bucket);
    }
  }

  return ret;
}

int ObLatchWaitQueue::wake_up(ObLatch &latch, const bool only_rd_wait)
{
  int ret = OB_SUCCESS;
  uint64_t pos = reinterpret_cast<uint64_t>((&latch)) % LATCH_MAP_BUCKET_CNT;
  ObLatchBucket &bucket = wait_map_[pos];
  ObWaitProc *iter = NULL;
  ObWaitProc *tmp = NULL;
  int32_t wait_mode = ObLatchWaitMode::NOWAIT;
  volatile int32_t *pwait = NULL;
  uint32_t wake_cnt = 0;

  lock_bucket(bucket);
  for (iter = bucket.wait_list_.get_first(); OB_SUCC(ret) && iter != bucket.wait_list_.get_header(); ) {
    if (iter->addr_ == &latch) {
      wait_mode = iter->mode_;
      if (ObLatchWaitMode::READ_WAIT == wait_mode
          || (ObLatchWaitMode::WRITE_WAIT == wait_mode && 0 == wake_cnt && !only_rd_wait)) {
        tmp = iter->get_next();
        if (OB_ISNULL(bucket.wait_list_.remove(iter))) {
          //should not happen
          ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(ERROR, "Fail to remove iter from wait list, ", K(ret));
        } else {
          pwait = &iter->wait_;
          //the proc.wait_ must be set to 0 at last, once the 0 is set, the *iter may be not valid any more
          MEM_BARRIER();
          *pwait = 0;
          if (1 == futex_wake(pwait, 1)) {
            ++wake_cnt;
          }
        }
        iter = tmp;
      }

      if (ObLatchWaitMode::WRITE_WAIT == wait_mode && (wake_cnt > 0 || only_rd_wait)) {
        break;
      }
    } else {
      //other latch in same bucket, just ignore
      iter = iter->get_next();
    }
  }

  //check if there is other wait in the waiting list
  bool has_wait = false;
  for (; iter != bucket.wait_list_.get_header(); iter = iter->get_next()) {
    if (iter->addr_ == &latch) {
      has_wait = true;
      break;
    }
  }
  if (!has_wait) {
    //no wait, clear the wait mask
    __sync_and_and_fetch(&latch.lock_, ~latch.WAIT_MASK);
  }

  unlock_bucket(bucket);
  return ret;
}

void ObLatchWaitQueue::lock_bucket(ObLatchBucket &bucket)
{
  ObDiagnoseTenantInfo *di = ObDiagnoseTenantInfo::get_local_diagnose_info();
  uint64_t i = 1;
  while (!ATOMIC_BCAS(&bucket.lock_, 0, 1)) {
    ++i;
#if defined(__aarch64__)
    asm("yield");
#else
    asm("pause");
#endif
  }
  if (OB_UNLIKELY(NULL != di)) {
    ObLatchStat &latch_stat = di->get_latch_stats().items_[ObLatchIds::LATCH_WAIT_QUEUE_LOCK];
    ++latch_stat.gets_;
    latch_stat.spin_gets_ += i;
  }
}

void ObLatchWaitQueue::unlock_bucket(ObLatchBucket &bucket)
{
  ATOMIC_STORE(&bucket.lock_, 0);
}

int ObLatchWaitQueue::try_lock(
    ObLatchBucket &bucket,
    ObWaitProc &proc,
    const uint32_t latch_id,
    const uint32_t uid,
    low_try_lock lock_func)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!proc.is_valid())
      || OB_UNLIKELY(latch_id >= ObLatchIds::LATCH_END)
      || OB_UNLIKELY(NULL == lock_func)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", K(proc), K(uid), K(ret));
  } else {
    uint32_t lock = 0;
    bool conflict = false;
    ObLatch &latch = *(proc.addr_);

    lock_bucket(bucket);
    while (true) {
      lock = latch.lock_;
      if (OB_SUCC((latch.*lock_func)(lock, uid, conflict))) {
        break;
      } else if (conflict) {
        if (ATOMIC_BCAS(&latch.lock_, lock, lock | latch.WAIT_MASK)) {
          break;
        }
      }
#if defined(__aarch64__)
      asm("yield");
#else
      asm("pause");
#endif
    }

    if (OB_EAGAIN == ret) {
      //fail to lock, add the proc to wait list
      if (ObLatchPolicy::LATCH_READ_PREFER == OB_LATCHES[latch_id].policy_
          && ObLatchWaitMode::READ_WAIT == proc.mode_) {
        if (!bucket.wait_list_.add_first(&proc)) {
          ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(ERROR, "Fail to add proc to wait list, ", K(ret));
        }
      } else {
        if (!bucket.wait_list_.add_last(&proc)) {
          ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(ERROR, "Fail to add proc to wait list, ", K(ret));
        }
      }
      proc.wait_ = 1;
    }

    unlock_bucket(bucket);
  }

  return ret;
}


/**
 * -------------------------------------------------------ObLatch---------------------------------------------------------------
 */
ObLatch::ObLatch()
  : lock_(0)
{
}

ObLatch::~ObLatch()
{
  if (0 != lock_) {
    COMMON_LOG(DEBUG, "invalid lock,", K(lock_), K(lbt()));
  }
}

int ObLatch::try_rdlock(const uint32_t latch_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(latch_id >= ObLatchIds::LATCH_END)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument", K(latch_id), K(ret));
  } else {
    ret = OB_EAGAIN;
    uint64_t i = 0;
    uint32_t lock = 0;
    do {
      lock = lock_;
      if (0 == (lock & WRITE_MASK)) {
        ++i;
        if (ATOMIC_BCAS(&lock_, lock, lock + 1)) {
          ret = OB_SUCCESS;
          break;
        }
      } else {
        break;
      }

#if defined(__aarch64__)
      asm("yield");
#else
      asm("pause");
#endif
    } while (true);

    ObDiagnoseTenantInfo *di = ObDiagnoseTenantInfo::get_local_diagnose_info();
    if (NULL != di) {
      ObLatchStat &latch_stat = di->get_latch_stats().items_[latch_id];
      if (OB_SUCC(ret)) {
        ++latch_stat.immediate_gets_;
      } else {
        ++latch_stat.immediate_misses_;
      }
      latch_stat.spin_gets_ += i;
    }
  }
  return ret;
}

int ObLatch::try_wrlock(const uint32_t latch_id, const uint32_t *puid)
{
  int ret = OB_SUCCESS;
  uint32_t uid = (NULL == puid) ? static_cast<uint32_t>(GETTID()) : *puid;
  if (OB_UNLIKELY(latch_id >= ObLatchIds::LATCH_END)
      || OB_UNLIKELY(0 == uid)
      || OB_UNLIKELY(uid >= WRITE_MASK)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument", K(latch_id), K(uid), K(ret));
  } else {
    if (!ATOMIC_BCAS(&lock_, 0, (WRITE_MASK | uid))) {
      ret = OB_EAGAIN;
    }

    ObDiagnoseTenantInfo *di = ObDiagnoseTenantInfo::get_local_diagnose_info();
    if (NULL != di) {
      ObLatchStat &latch_stat = di->get_latch_stats().items_[latch_id];
      if (OB_SUCC(ret)) {
        ++latch_stat.immediate_gets_;
      } else {
        ++latch_stat.immediate_misses_;
      }
      ++latch_stat.spin_gets_;
    }
  }
  return ret;
}

int ObLatch::rdlock(
    const uint32_t latch_id,
    const int64_t abs_timeout_us)
{
  int ret = OB_SUCCESS;
  //the uid is unused concurrently
  uint32_t uid = 1;
  if (OB_UNLIKELY(latch_id >= ObLatchIds::LATCH_END)
      || OB_UNLIKELY(abs_timeout_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", K(latch_id), K(abs_timeout_us), K(ret));
  } else if (OB_FAIL(low_lock(
      latch_id,
      abs_timeout_us,
      uid,
      ObLatchWaitMode::READ_WAIT,
      ObLatchPolicy::LATCH_FIFO == OB_LATCHES[latch_id].policy_ ? &ObLatch::low_try_rdlock : &ObLatch::low_try_rdlock_ignore,
      &ObLatch::low_try_rdlock_ignore))) {
    if (OB_TIMEOUT != ret) {
      COMMON_LOG(WARN, "Fail to low lock, ", K(ret));
    }
  }
  return ret;
}


int ObLatch::wrlock(
    const uint32_t latch_id,
    const int64_t abs_timeout_us,
    const uint32_t *puid)
{
  int ret = OB_SUCCESS;
  uint32_t uid = (NULL == puid) ? static_cast<uint32_t>(GETTID()) : *puid;
  if (OB_UNLIKELY(latch_id >= ObLatchIds::LATCH_END)
      || OB_UNLIKELY(abs_timeout_us <= 0)
      || OB_UNLIKELY(0 == uid)
      || OB_UNLIKELY(uid >= WRITE_MASK)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument, ", K(latch_id), K(abs_timeout_us), K(uid), K(lbt()), K(ret));
  } else if (OB_FAIL(low_lock(
      latch_id,
      abs_timeout_us,
      uid,
      ObLatchWaitMode::WRITE_WAIT,
      &ObLatch::low_try_wrlock,
      &ObLatch::low_try_wrlock_ignore))) {
    if (OB_TIMEOUT != ret) {
      COMMON_LOG(WARN, "Fail to low lock, ", K(ret));
    }
  }
  return ret;
}

int ObLatch::wr2rdlock(const uint32_t *puid)
{
  int ret = OB_SUCCESS;
  if (!is_wrlocked_by(puid)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "The latch is not write locked, ", K(ret));
  } else {
    uint32_t lock = lock_;
    while (!ATOMIC_BCAS(&lock_, lock, (lock & WAIT_MASK) + 1)) {
      lock = lock_;
#if defined(__aarch64__)
      asm("yield");
#else
      asm("pause");
#endif
    }
    bool only_rd_wait = true;
    if (OB_FAIL(ObLatchWaitQueue::get_instance().wake_up(*this, only_rd_wait))) {
      COMMON_LOG(ERROR, "Fail to wake up latch wait queue, ", K(this), K(ret));
    }
  }
  return ret;
}

int ObLatch::unlock(const uint32_t *puid)
{
  int ret = OB_SUCCESS;
  uint32_t lock = ATOMIC_LOAD(&lock_);

  if (0 != (lock & WRITE_MASK)) {
    uint32_t uid = (NULL == puid) ? static_cast<uint32_t>(GETTID()) : *puid;
    uint32_t wid = (lock & ~(WAIT_MASK | WRITE_MASK));
    if (NULL != puid && uid != wid) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "The latch is not write locked by the uid, ", K(uid), K(wid), K(lbt()), K(ret));
    } else {
      lock = __sync_and_and_fetch(&lock_, WAIT_MASK);
    }
  } else if ((lock & (~WAIT_MASK)) > 0) {
    lock = ATOMIC_AAF(&lock_, -1);
  } else {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "invalid lock,", K(lock), K(ret));
  }
  if (OB_SUCCESS == ret && WAIT_MASK == lock) {
    if (OB_FAIL(ObLatchWaitQueue::get_instance().wake_up(*this))) {
      COMMON_LOG(ERROR, "Fail to wake up latch wait queue, ", K(this), K(ret));
    }
  }
  return ret;
}

int ObLatch::low_lock(
    const uint32_t latch_id,
    const int64_t abs_timeout_us,
    const uint32_t uid,
    const uint32_t wait_mode,
    low_try_lock lock_func,
    low_try_lock lock_func_ignore)
{
  int ret = OB_SUCCESS;
  uint64_t i = 0;
  uint32_t lock = 0;
  uint64_t spin_cnt = 0;
  uint64_t yield_cnt = 0;
  bool waited = false;
  bool conflict = false;

  if (OB_UNLIKELY(latch_id >= ObLatchIds::LATCH_END)
      || OB_UNLIKELY(abs_timeout_us <= 0)
      || OB_UNLIKELY(0 == uid)
      || OB_UNLIKELY(uid >= WRITE_MASK)
      || OB_UNLIKELY(ObLatchWaitMode::READ_WAIT != wait_mode
          && ObLatchWaitMode::WRITE_WAIT != wait_mode)
      || OB_UNLIKELY(NULL == lock_func)
      || OB_UNLIKELY(NULL == lock_func_ignore)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument", K(latch_id), K(uid), K(ret));
  } else {
    while (OB_SUCC(ret)) {
      //spin
      for (i = 0; i < OB_LATCHES[latch_id].max_spin_cnt_; ++i) {
        lock = lock_;
        if (OB_SUCCESS == (this->*lock_func)(lock, uid, conflict)) {
          break;
        }
#if defined(__aarch64__)
        asm("yield");
#else
        asm("pause");
#endif
      }
      spin_cnt += i;

      if (i < OB_LATCHES[latch_id].max_spin_cnt_) {
        //success lock
        ++spin_cnt;
        break;
      } else if (yield_cnt < OB_LATCHES[latch_id].max_yield_cnt_) {
        //yield and retry
        sched_yield();
        ++yield_cnt;
        continue;
      } else {
        //wait
        waited = true;
        ObWaitEventGuard wait_guard(
          OB_LATCHES[latch_id].wait_event_idx_,
          abs_timeout_us / 1000,
          reinterpret_cast<uint64_t>(this),
          latch_id,
          1);
        ObWaitProc proc(*this, wait_mode);
        if (OB_FAIL(ObLatchWaitQueue::get_instance().wait(
            proc,
            latch_id,
            uid,
            lock_func,
            lock_func_ignore,
            abs_timeout_us))) {
          if (OB_TIMEOUT != ret) {
            COMMON_LOG(WARN, "Fail to wait the latch, ", K(ret));
          }
        } else {
          break;
        }
      }
    }

    ObDiagnoseTenantInfo *di = ObDiagnoseTenantInfo::get_local_diagnose_info();
    if (NULL != di) {
      ObLatchStat &latch_stat = di->get_latch_stats().items_[latch_id];
      ++latch_stat.gets_;
      latch_stat.spin_gets_ += spin_cnt;
      latch_stat.sleeps_ += yield_cnt;
      if (waited) {
        ++latch_stat.misses_;
        if (NULL != ObDiagnoseSessionInfo::get_local_diagnose_info()) {
          latch_stat.wait_time_ += ObDiagnoseSessionInfo::get_local_diagnose_info()->get_curr_wait().wait_time_;
        }
      }
    }
  }
  return ret;
}


int64_t ObWaitProc::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_print_kv(buf, buf_len, pos, KP_(addr), K_(mode));
  return pos;
}

int64_t ObLatch::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_print_kv(buf, buf_len, pos, "lock_", static_cast<uint32_t>(lock_));
  return pos;
}
}
}
