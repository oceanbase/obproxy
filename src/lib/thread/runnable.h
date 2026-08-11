/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef TBSYS_RUNNABLE_H_
#define TBSYS_RUNNABLE_H_

namespace obsys {
class CThread;
class Runnable {

public:
    virtual ~Runnable() {
    }
    virtual void run(CThread *thread, void *arg) = 0;
};

}

#endif /*RUNNABLE_H_*/
