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
