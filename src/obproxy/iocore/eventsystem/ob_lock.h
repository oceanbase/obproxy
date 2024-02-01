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
 *
 * *************************************************************
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#ifndef OBPROXY_LOCK_H
#define OBPROXY_LOCK_H

#include "iocore/eventsystem/ob_thread.h"
#include "iocore/eventsystem/ob_resource_tracker.h"
#include "stat/ob_lock_stats.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{

#define OB_PROXY_MAX_LOCK_TIME                      HRTIME_MSECONDS(200) //200ms
#define OB_PROXY_INIT_MUTEX_THREAD_HOLDING_COUNT    (-1024 * 1024)
#define OB_PROXY_MAX_LOCK_TAKEN                     (1024L * 1024L)

/**
 * Blocks until the lock to the ObProxyMutex is acquired.
 * This macro performs a blocking call until the lock to the ObProxyMutex
 * is acquired. This call allocates a special object that holds the
 * lock to the ObProxyMutex only for the scope of the function or
 * region. It is a good practice to delimit such scope explicitly
 * with '&#123;' and '&#125;'.
 *
 * @param l Arbitrary name for the lock to use in this call
 * @param m A pointer to (or address of) a ObProxyMutex object
 * @param t The current ObEThread executing your code.
 */
#ifdef OB_HAS_EVENT_DEBUG
#define MUTEX_LOCK(l, m, t) oceanbase::obproxy::event::ObMutexLock \
  l(MAKE_LOCATION(), NULL, m, t)
#else
#define MUTEX_LOCK(l, m, t) oceanbase::obproxy::event::ObMutexLock l(m, t)
#endif //OB_HAS_EVENT_DEBUG


#ifdef OB_HAS_EVENT_DEBUG
/**
 * Attempts to acquire the lock to the ObProxyMutex.
 * This macro attempts to acquire the lock to the specified ObProxyMutex
 * object in a non-blocking manner. After using the macro you can
 * see if it was successful by comparing the lock variable with true
 * or false (the variable name passed in the l parameter).
 *
 * @param l Arbitrary name for the lock to use in this call (lock
 *          variable)
 * @param m A pointer to (or address of) a ObProxyMutex object
 * @param t The current ObEThread executing your code.
 */
#define MUTEX_TRY_LOCK(l, m, t) \
  oceanbase::obproxy::event::ObMutexTryLock \
  l(MAKE_LOCATION(), reinterpret_cast<char*>(NULL), m, t)

/**
 * Attempts to acquire the lock to the ObProxyMutex.
 * This macro performs up to the specified number of attempts to
 * acquire the lock on the ObProxyMutex object. It does so by running
 * a busy loop (busy wait) 'sc' times. You should use it with care
 * since it blocks the thread during that time and wastes CPU time.
 *
 * @param l Arbitrary name for the lock to use in this call (lock
 *          variable)
 * @param m A pointer to (or address of) a ObProxyMutex object
 * @param t The current ObEThread executing your code.
 * @param sc The number of attempts or spin count. It must be a
 *           positive value.
 */
#define MUTEX_TRY_LOCK_SPIN(l, m, t, sc) \
  oceanbase::obproxy::event::ObMutexTryLock \
  l(MAKE_LOCATION(), reinterpret_cast<char*>(NULL), m, t, sc)


/**
 * Attempts to acquire the lock to the ObProxyMutex.
 * This macro attempts to acquire the lock to the specified ObProxyMutex
 * object in a non-blocking manner. After using the macro you can
 * see if it was successful by comparing the lock variable with true
 * or false (the variable name passed in the l parameter).
 *
 * @param l Arbitrary name for the lock to use in this call (lock
 *          variable)
 * @param m A pointer to (or address of) a ObProxyMutex object
 * @param t The current ObEThread executing your code.
 * @param c ObContinuation whose mutex will be attempted to lock.
 */
#else //OB_HAS_EVENT_DEBUG
#define MUTEX_TRY_LOCK(l, m, t) ObMutexTryLock l(m, t)
#define MUTEX_TRY_LOCK_SPIN(l, m, t, sc) ObMutexTryLock l(m, t, sc)
#endif //OB_HAS_EVENT_DEBUG

/**
 * Releases the lock on a ObProxyMutex.
 * This macro releases the lock on the ObProxyMutex, provided it is
 * currently held. The lock must have been successfully acquired
 * with one of the MUTEX macros.
 *
 * @param l Arbitrary name for the lock to use in this call (lock
 *   variable) It must be the same name as the one used to acquire the
 *   lock.
 */
#define MUTEX_RELEASE(l) (l).release()

class ObEThread;
typedef ObEThread *ObEThreadPtr;

extern void lock_waiting(const ObSrcLoc &loc, const char *handler);
extern void lock_holding(const ObSrcLoc &loc, const char *handler);
extern void lock_taken(const ObSrcLoc &loc, const char *handler);

/**
 * Lock object used in continuations and threads.
 *
 * The ObProxyMutex class is the main synchronization object used
 * throughout the ObEvent System. It is a reference counted object
 * that provides mutually exclusive access to a resource. Since the
 * ObEvent System is multithreaded by design, the ObProxyMutex is required
 * to protect data structures and state information that could
 * otherwise be affected by the action of concurrent threads.
 *
 * A ObProxyMutex object has an ObMutex member (defined in ob_mutex.h)
 * which is a wrapper around the platform dependent mutex type. This
 * member allows the ObProxyMutex to provide the functionality required
 * by the users of the class without the burden of platform specific
 * function calls.
 *
 * The ObProxyMutex also has a reference to the current ObEThread holding
 * the lock as a back pointer for verifying that it is released
 * correctly.
 *
 * Acquiring/Releasing locks:
 *
 * Included with the ObProxyMutex class, there are several macros that
 * allow you to lock/unlock the underlying mutex object.
 */
class ObProxyMutex : public common::ObRefCountObj
{
public:
  /**
   * Constructor - use new_ProxyMutex() instead.
   *
   * The constructor of a ObProxyMutex object. Initializes the state
   * of the object but leaves the initialization of the mutex member
   * until it is needed (through init()). Do not use this constructor,
   * the preferred mechanism for creating a ObProxyMutex is via the
   * new_ProxyMutex function, which provides a faster allocation.
   */
  ObProxyMutex()
  {
    thread_holding_ = NULL;
    thread_holding_count_ = 0;
    lockstat_ = COMMON_LOCK;
#ifdef OB_HAS_EVENT_DEBUG
    hold_time_ = 0;
    handler_ = NULL;
#ifdef OB_PROXY_MAX_LOCK_TAKEN
    lock_taken_count_ = 0;
#endif //OB_PROXY_MAX_LOCK_TAKEN
#ifdef OB_HAS_LOCK_CONTENTION_PROFILING
    total_acquires_ = 0;
    blocking_acquires_ = 0;
    nonblocking_acquires_ = 0;
    successful_nonblocking_acquires_ = 0;
    unsuccessful_nonblocking_acquires_ = 0;
#endif //OB_HAS_LOCK_CONTENTION_PROFILING
#endif //OB_HAS_EVENT_DEBUG
  }

  virtual ~ObProxyMutex() {}

  /**
   * Initializes the underlying mutex object.
   * After constructing your ObProxyMutex object, use this function
   * to initialize the underlying mutex object with an optional name.
   */
  int init() { return common::OB_SUCCESS; }

  void free()
  {
#ifdef OB_HAS_EVENT_DEBUG
#ifdef OB_HAS_LOCK_CONTENTION_PROFILING
    print_lock_stats(1);
#endif //OB_HAS_LOCK_CONTENTION_PROFILING
#endif //OB_HAS_EVENT_DEBUG
    op_reclaim_free(this);
  }

#ifdef OB_HAS_EVENT_DEBUG
#ifdef OB_HAS_LOCK_CONTENTION_PROFILING
  void print_lock_stats(bool flag);
#endif //OB_HAS_LOCK_CONTENTION_PROFILING
#endif //OB_HAS_EVENT_DEBUG

public:
  /**
   * Underlying mutex object.
   *
   * The platform independent mutex for the ObProxyMutex class. You
   * must not modify or set it directly.
   */
  lib::ObMutex the_mutex_;

  /**
   * Backpointer to owning thread.
   *
   * This is a pointer to the thread currently holding the mutex
   * lock.  You must not modify or set this value directly.
   */
  volatile ObEThreadPtr thread_holding_;

  int32_t thread_holding_count_;

  ObLockStats lockstat_;

#ifdef OB_HAS_EVENT_DEBUG
  ObHRTime hold_time_;
  ObSrcLoc srcloc_;
  const char *handler_;

#ifdef OB_PROXY_MAX_LOCK_TAKEN
  int32_t lock_taken_count_;
#endif //OB_PROXY_MAX_LOCK_TAKEN

#ifdef OB_HAS_LOCK_CONTENTION_PROFILING
  int32_t total_acquires_;
  int32_t blocking_acquires_;
  int32_t nonblocking_acquires_;
  int32_t successful_nonblocking_acquires_;
  int32_t unsuccessful_nonblocking_acquires_;
#endif //OB_HAS_LOCK_CONTENTION_PROFILING
#endif //OB_HAS_EVENT_DEBUG
};

inline bool mutex_trylock(
#ifdef OB_HAS_EVENT_DEBUG
    const ObSrcLoc &location, const char *ahandler,
#endif
    ObProxyMutex *m, ObEThread *t)
{
  bool bret = true;
  if (OB_ISNULL(t) || OB_UNLIKELY(reinterpret_cast<ObThread *>(t) != this_thread()) || OB_ISNULL(m)) {
    bret = false;
    PROXY_EVENT_LOG(WDIAG, "argument is error", K(t), K(m));
  } else {
    if (m->thread_holding_ != t) {
      if (OB_UNLIKELY(!lib::mutex_try_acquire(&m->the_mutex_))) {
#ifdef OB_HAS_EVENT_DEBUG
        lock_waiting(m->srcloc_, m->handler_);
#ifdef OB_HAS_LOCK_CONTENTION_PROFILING
        ++(m->unsuccessful_nonblocking_acquires_);
        ++(m->nonblocking_acquires_);
        ++(m->total_acquires_);
        m->print_lock_stats(0);
#endif //OB_HAS_LOCK_CONTENTION_PROFILING
#endif //OB_HAS_EVENT_DEBUG
        bret = false;
        LOCK_INCREMENT_DYN_STAT(m->lockstat_);
      } else {
        m->thread_holding_ = t;
#ifdef OB_HAS_EVENT_DEBUG
        m->srcloc_ = location;
        m->handler_ = ahandler;
        m->hold_time_ = get_hrtime();
#ifdef OB_PROXY_MAX_LOCK_TAKEN
        ++(m->lock_taken_count_);
#endif //OB_PROXY_MAX_LOCK_TAKEN
#endif //OB_HAS_EVENT_DEBUG
      }
    }

    if (OB_LIKELY(bret)) {
#ifdef OB_HAS_EVENT_DEBUG
#ifdef OB_HAS_LOCK_CONTENTION_PROFILING
      ++(m->successful_nonblocking_acquires_);
      ++(m->nonblocking_acquires_);
      ++(m->total_acquires_);
      m->print_lock_stats(0);
#endif //OB_HAS_LOCK_CONTENTION_PROFILING
#endif //OB_HAS_EVENT_DEBUG
      ++(m->thread_holding_count_);
    }
  }
  return bret;
}

inline bool mutex_trylock_spin(
#ifdef OB_HAS_EVENT_DEBUG
    const ObSrcLoc &location, const char *ahandler,
#endif
    ObProxyMutex *m, ObEThread *t, int spincnt = 1)
{
  bool bret = true;
  if (OB_ISNULL(t) || OB_ISNULL(m)) {
    bret = false;
    PROXY_EVENT_LOG(WDIAG, "argument is error", K(t), K(m));
  } else {
    if (m->thread_holding_ != t) {
      do {
        bret = lib::mutex_try_acquire(&m->the_mutex_);
      } while (--spincnt && OB_UNLIKELY(!bret));

      if (OB_UNLIKELY(!bret)) {
#ifdef OB_HAS_EVENT_DEBUG
        lock_waiting(m->srcloc_, m->handler_);
#ifdef OB_HAS_LOCK_CONTENTION_PROFILING
        ++(m->unsuccessful_nonblocking_acquires_);
        ++(m->nonblocking_acquires_);
        ++(m->total_acquires_);
        m->print_lock_stats(0);
#endif //OB_HAS_LOCK_CONTENTION_PROFILING
#endif //OB_HAS_EVENT_DEBUG
        LOCK_INCREMENT_DYN_STAT(m->lockstat_);
      } else {
        m->thread_holding_ = t;
#ifdef OB_HAS_EVENT_DEBUG
        m->srcloc_ = location;
        m->handler_ = ahandler;
        m->hold_time_ = get_hrtime();
#ifdef OB_PROXY_MAX_LOCK_TAKEN
        ++(m->lock_taken_count_);
#endif //OB_PROXY_MAX_LOCK_TAKEN
#endif //OB_HAS_EVENT_DEBUG
      }
    }

    if (OB_LIKELY(bret)) {
#ifdef OB_HAS_EVENT_DEBUG
#ifdef OB_HAS_LOCK_CONTENTION_PROFILING
      ++(m->successful_nonblocking_acquires_);
      ++(m->nonblocking_acquires_);
      ++(m->total_acquires_);
      m->print_lock_stats(0);
#endif //OB_HAS_LOCK_CONTENTION_PROFILING
#endif //OB_HAS_EVENT_DEBUG
      ++(m->thread_holding_count_);
    }
  }
  return bret;
}

inline bool mutex_lock(
#ifdef OB_HAS_EVENT_DEBUG
    const ObSrcLoc &location, const char *ahandler,
#endif
    ObProxyMutex *m, ObEThread *t)
{
  bool bret = true;
  if (OB_ISNULL(t) || OB_ISNULL(m)) {
    bret = false;
    PROXY_EVENT_LOG(WDIAG, "argument is error", K(t), K(m));
  } else {
    if (m->thread_holding_ != t) {
      if (OB_UNLIKELY(common::OB_SUCCESS != lib::mutex_acquire(&m->the_mutex_))) {
        bret = false;
        PROXY_EVENT_LOG(EDIAG, "fail to acquire mutex");
      } else {
        m->thread_holding_ = t;
#ifdef OB_HAS_EVENT_DEBUG
        m->srcloc_ = location;
        m->handler_ = ahandler;
        m->hold_time_ = get_hrtime();
#ifdef OB_PROXY_MAX_LOCK_TAKEN
        ++(m->lock_taken_count_);
#endif //OB_PROXY_MAX_LOCK_TAKEN
#endif //OB_HAS_EVENT_DEBUG
      }
    }

    if (OB_LIKELY(bret)) {
#ifdef OB_HAS_EVENT_DEBUG
#ifdef OB_HAS_LOCK_CONTENTION_PROFILING
      ++(m->blocking_acquires_);
      ++(m->total_acquires_);
      m->print_lock_stats(0);
#endif //OB_HAS_LOCK_CONTENTION_PROFILING
#endif //OB_HAS_EVENT_DEBUG
      ++(m->thread_holding_count_);
    }
  }
  return bret;
}

inline void mutex_unlock(ObProxyMutex *m, ObEThread *t)
{
  if (OB_ISNULL(t) || OB_ISNULL(m)) {
    PROXY_EVENT_LOG(WDIAG, "argument is error", K(t), K(m));
  } else {
    if (OB_LIKELY(m->thread_holding_count_ > 0) && OB_LIKELY(t == m->thread_holding_)) {
      --(m->thread_holding_count_);
      if (0 == m->thread_holding_count_) {
#ifdef OB_HAS_EVENT_DEBUG
        if (OB_UNLIKELY(get_hrtime() - m->hold_time_ > OB_PROXY_MAX_LOCK_TIME)) {
          lock_holding(m->srcloc_, m->handler_);
        }
#ifdef OB_PROXY_MAX_LOCK_TAKEN
        if (OB_UNLIKELY(m->lock_taken_count_ > OB_PROXY_MAX_LOCK_TAKEN)) {
          lock_taken(m->srcloc_, m->handler_);
        }
#endif //OB_PROXY_MAX_LOCK_TAKEN
        m->srcloc_ = ObSrcLoc(NULL, NULL, 0);
        m->handler_ = NULL;
#endif //OB_HAS_EVENT_DEBUG
        m->thread_holding_ = NULL;
        if (OB_UNLIKELY(common::OB_SUCCESS != lib::mutex_release(&m->the_mutex_))) {
          PROXY_EVENT_LOG(EDIAG, "fail to release mutex");
        }
      }
    }
  }
}

class ObMutexLock
{
public:
  ObMutexLock() : m_(NULL) { };
  ObMutexLock(
#ifdef OB_HAS_EVENT_DEBUG
      const ObSrcLoc &location, const char *ahandler,
#endif //OB_HAS_EVENT_DEBUG
      ObProxyMutex *am, ObEThread *t) : m_(am)
  {
    mutex_lock(
#ifdef OB_HAS_EVENT_DEBUG
        location, ahandler,
#endif //OB_HAS_EVENT_DEBUG
        m_, t);
  }

  void set_and_take(
#ifdef OB_HAS_EVENT_DEBUG
      const ObSrcLoc &location, const char *ahandler,
#endif //OB_HAS_EVENT_DEBUG
      ObProxyMutex *am, ObEThread *t)
  {
    m_ = am;
    mutex_lock(
#ifdef OB_HAS_EVENT_DEBUG
        location, ahandler,
#endif //OB_HAS_EVENT_DEBUG
        m_, t);
  }

  void release()
  {
    if (OB_LIKELY(NULL != m_)) {
      mutex_unlock(m_, m_->thread_holding_);
    }
    m_.release();
  }

  ~ObMutexLock()
  {
    if (OB_LIKELY(NULL != m_)) {
      mutex_unlock(m_, m_->thread_holding_);
    }
  }

public:
  common::ObPtr<ObProxyMutex> m_;
};

class ObMutexTryLock
{
public:
  ObMutexTryLock(
#ifdef OB_HAS_EVENT_DEBUG
      const ObSrcLoc &location, const char *ahandler,
#endif //OB_HAS_EVENT_DEBUG
      ObProxyMutex *am, ObEThread *t)
  {
    lock_acquired_ = mutex_trylock(
#ifdef OB_HAS_EVENT_DEBUG
        location, ahandler,
#endif //OB_HAS_EVENT_DEBUG
        am, t);
    if (lock_acquired_) {
      m_ = am;
    }
  }

  ObMutexTryLock(
#ifdef OB_HAS_EVENT_DEBUG
      const ObSrcLoc &location, const char *ahandler,
#endif //OB_HAS_EVENT_DEBUG
      ObProxyMutex *am, ObEThread *t, int sp)
  {
    lock_acquired_ = mutex_trylock_spin(
#ifdef OB_HAS_EVENT_DEBUG
        location, ahandler,
#endif //OB_HAS_EVENT_DEBUG
        am, t, sp);
    if (lock_acquired_) {
      m_ = am;
    }
  }

  ~ObMutexTryLock()
  {
    if (OB_LIKELY(lock_acquired_)) {
      mutex_unlock(m_.ptr_, m_.ptr_->thread_holding_);
    }
  }

  void release()
  {
    if (OB_LIKELY(lock_acquired_)) {
      mutex_unlock(m_.ptr_, m_.ptr_->thread_holding_);
      m_.release();
      lock_acquired_ = false;
    } else {
      // generate a warning because it shouldn't be done.
      PROXY_EVENT_LOG(WDIAG, "it had not acquired lock");
    }
  }

  bool is_locked() const { return lock_acquired_; }
  const ObProxyMutex *get_mutex() { return m_.ptr_; }

public:
  common::ObPtr<ObProxyMutex> m_;
  volatile bool lock_acquired_;
};

/**
 * Creates a new ObProxyMutex object.
 * This is the preferred mechanism for constructing objects of the
 * ObProxyMutex class. It provides you with faster allocation than
 * that of the normal constructor.
 *
 * @return A pointer to a ObProxyMutex object appropriate for the build
 *         environment.
 */
inline ObProxyMutex *new_proxy_mutex(ObLockStats lock_stat = COMMON_LOCK)
{
  ObProxyMutex *ret = NULL;
  if (OB_ISNULL(ret = op_reclaim_alloc(ObProxyMutex))) {
    PROXY_EVENT_LOG(EDIAG, "fail to alloc memory for ObProxyMutex");
  } else if (OB_UNLIKELY(common::OB_SUCCESS != ret->init())) {
    PROXY_EVENT_LOG(WDIAG, "fail to init proxy mutex");
  } else {
    ret->lockstat_ = lock_stat;
  }
  return ret;
}

} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_LOCK_H
