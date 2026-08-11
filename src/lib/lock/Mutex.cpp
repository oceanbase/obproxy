/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "Mutex.h"
#include "lib/oblog/ob_log.h"
namespace tbutil
{
Mutex::Mutex()
{
    const int rt = pthread_mutex_init(&_mutex, NULL);
#ifdef _NO_EXCEPTION
    assert( rt == 0 );
    if ( rt != 0 )
    {
        _OB_LOG(EDIAG,"%s","ThreadSyscallException");
    }
#else
    if ( rt != 0 )
    {
        throw ThreadSyscallException(__FILE__, __LINE__, rt);
    }
#endif
}

Mutex::~Mutex()
{
    const int rt = pthread_mutex_destroy(&_mutex);
    assert(rt == 0);
    if ( rt != 0 )
    {
        _OB_LOG(EDIAG,"%s","ThreadSyscallException");
    }
}

void Mutex::lock() const
{
    const int rt = pthread_mutex_lock(&_mutex);
#ifdef _NO_EXCEPTION
    assert( rt == 0 );
    if ( rt != 0 )
    {
        if ( rt == EDEADLK )
        {
            _OB_LOG(EDIAG,"%s","ThreadLockedException ");
        }
        else
        {
            _OB_LOG(EDIAG,"%s","ThreadSyscallException");
        }
    }
#else
    if( rt != 0 )
    {
        if(rt == EDEADLK)
        {
            throw ThreadLockedException(__FILE__, __LINE__);
        }
        else
        {
            throw ThreadSyscallException(__FILE__, __LINE__, rt);
        }
    }
#endif
}

bool Mutex::tryLock() const
{
    const int rt = pthread_mutex_trylock(&_mutex);
#ifdef _NO_EXCEPTION
    if ( rt != 0 && rt !=EBUSY )
    {
        if ( rt == EDEADLK )
        {
            _OB_LOG(EDIAG,"%s","ThreadLockedException ");
        }
        else
        {
            _OB_LOG(EDIAG,"%s","ThreadSyscallException");
        }
        return false;
    }
#else
    if(rt != 0 && rt != EBUSY)
    {
        if(rt == EDEADLK)
        {
            throw ThreadLockedException(__FILE__, __LINE__);
        }
        else
        {
            throw ThreadSyscallException(__FILE__, __LINE__, rt);
        }
    }
#endif
    return (rt == 0);
}

void Mutex::unlock() const
{
    const int rt = pthread_mutex_unlock(&_mutex);
#ifdef _NO_EXCEPTION
    assert( rt == 0 );
    if ( rt != 0 )
    {
        _OB_LOG(EDIAG,"%s","ThreadSyscallException");
    }
#else
    if ( rt != 0 )
    {
        throw ThreadSyscallException(__FILE__, __LINE__, rt);
    }
#endif
}

void Mutex::unlock(LockState& state) const
{
    state.mutex = &_mutex;
}

void Mutex::lock(LockState&) const
{
}

bool Mutex::willUnlock() const
{
    return true;
}
}//end namespace tbutil
