/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef TBSYS_LOCK_GUARD_H_
#define TBSYS_LOCK_GUARD_H_

namespace obsys
{
    /** 
     * @brief  CLockGuard is a template class, it needs CThreadMutex as its template parameter
     * Constructor calls the lock method passed in parameters, and destructor calls the unlock method
     */
    template <class T>
    class CLockGuard
    {
    public:
        CLockGuard(const T& lock, bool block = true) : _lock(lock)
        {
            _acquired = !(block ? _lock.lock() : _lock.tryLock());
        }

        ~CLockGuard()
        {
            if (_acquired) _lock.unlock();
        }

        bool acquired() const
        {
            return _acquired;
        }
        
    private:
        const T& _lock;
        mutable bool _acquired;
    };
}

#endif
