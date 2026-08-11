/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef TBSYS_DEFAULT_RUNNABLE_H_
#define TBSYS_DEFAULT_RUNNABLE_H_
#include "lib/thread/runnable.h"
namespace obsys {

class CDefaultRunnable : public Runnable {

public:
    CDefaultRunnable(int threadCount = 1);

    virtual ~CDefaultRunnable();

    void setThreadCount(int threadCount);

    /**
     * create %_threadCount threads
     * @return started thread count;
     */
    int start();

    /**
     * stop
     */
    void stop();

    /**
     * wait
     */
    void wait();

protected:
    CThread *_thread;
    int _threadCount;
    bool _stop;
};

}

#endif /*RUNNABLE_H_*/
