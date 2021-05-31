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
