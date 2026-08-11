/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef TBSYS_MUTEX_H_
#define TBSYS_MUTEX_H_

#include <assert.h>
#include <pthread.h>
namespace obsys {

/*
 * author cjxrobot
 *
 * Linux thread-lock
 */

/**
* @brief Simple encapsulation of linux thread-lock and mutex-lock
*/
class CThreadMutex {

public:
    /*
     * Constructor
     */
    CThreadMutex() {
        //assert(pthread_mutex_init(&_mutex, NULL) == 0);
        const int iRet = pthread_mutex_init(&_mutex, NULL);
        (void) iRet;
        assert( iRet == 0 );
    }

    /*
     * Destructor
     */
    ~CThreadMutex() {
        pthread_mutex_destroy(&_mutex);
    }

    /**
     * Lock
     */

    void lock () {
        pthread_mutex_lock(&_mutex);
    }

    /**
     * trylock
     */

    int trylock () {
        return pthread_mutex_trylock(&_mutex);
    }

    /**
     * Unlock
     */
    void unlock() {
        pthread_mutex_unlock(&_mutex);
    }

protected:

    pthread_mutex_t _mutex;
};

/**
 * @brief Thread guard
 */
class CThreadGuard
{
public:
    CThreadGuard(CThreadMutex *mutex)
    {
      _mutex = NULL;
        if (mutex) {
            _mutex = mutex;
            _mutex->lock();
        }
    }
    ~CThreadGuard()
    {
        if (_mutex) {
            _mutex->unlock();
        }
    }
private:
    CThreadMutex *_mutex;
};

}

#endif /*MUTEX_H_*/
