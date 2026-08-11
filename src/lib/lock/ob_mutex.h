/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_MUTEX_H_
#define OB_MUTEX_H_

#include "lib/ob_define.h"
#include "lib/stat/ob_latch_define.h"
#include "lib/lock/ob_lock_guard.h"
#include "lib/lock/ob_latch.h"

namespace oceanbase
{
namespace lib
{
class ObMutex {
public:
  explicit ObMutex(uint32_t latch_id = common::ObLatchIds::DEFAULT_MUTEX)
      : latch_(), latch_id_(latch_id)
  {
  }
  ~ObMutex() { }
  inline int lock() { return latch_.wrlock(latch_id_); }
  inline int trylock() { return latch_.try_wrlock(latch_id_); }
  inline int unlock() { return latch_.unlock(); }
private:
  common::ObLatch latch_;
  uint32_t latch_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMutex);
};

static inline int mutex_acquire(ObMutex *m)
{
  return m->lock();
}

static inline bool mutex_try_acquire(ObMutex *m)
{
  return common::OB_SUCCESS == m->trylock();
}
static inline int mutex_release(ObMutex *m)
{
  return m->unlock();
}

class ObDummyMutex : public ObMutex
{
public:
  inline int lock() { return common::OB_SUCCESS; }
  inline int trylock() { return common::OB_SUCCESS; }
  inline int unlock() { return common::OB_SUCCESS; }
};

typedef ObLockGuard<ObMutex> ObMutexGuard;

} // end of namespace lib
} // end of namespace oceanbase


// belows for proxy
typedef pthread_mutex_t ObMutex;

namespace oceanbase
{
namespace common
{

static inline int mutex_init(ObMutex *m)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 != pthread_mutex_init(m, NULL))) {
    ret = OB_ERR_SYS;
    LIB_LOG(EDIAG, "mutex init fail", K(ret));
  }
  return ret;
}

static inline int mutex_destroy(ObMutex *m)
{
  return (0 == pthread_mutex_destroy(m)) ? OB_SUCCESS : OB_ERR_SYS;
}

static inline int mutex_acquire(ObMutex *m)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 != pthread_mutex_lock(m))) {
    ret = OB_ERR_SYS;
    LIB_LOG(EDIAG, "mutex acquire fail", K(ret));
  }
  return ret;
}

static inline int mutex_release(ObMutex *m)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 != pthread_mutex_unlock(m))) {
    ret = OB_ERR_SYS;
    LIB_LOG(EDIAG, "mutex release fail", K(ret));
  }
  return ret;
}

static inline bool mutex_try_acquire(ObMutex *m)
{
  return (0 == pthread_mutex_trylock(m));
}

} // end of namespace common
} // end of namespace oceanbase

#endif // OB_MUTEX_H_
