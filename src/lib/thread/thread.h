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

#ifndef TBSYS_THREAD_H_
#define TBSYS_THREAD_H_
#include <sys/types.h>
#include <linux/unistd.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/syscall.h>   /* For SYS_xxx definitions */

#include "lib/thread/runnable.h"
namespace obsys {

class CThread {

public:
    CThread() {
        tid = 0;
        pid = 0;
    }

    bool start(Runnable *r, void *a) {
        runnable = r;
        args = a;
        return 0 == pthread_create(&tid, NULL, CThread::hook, this);
    }

    void join() {
        if (tid) {
            (void)pthread_join(tid, NULL);
            tid = 0;
            pid = 0;
        }
    }

    Runnable *getRunnable() {
        return runnable;
    }

    void *getArgs() {
        return args;
    }

    int getpid() {
        return pid;
    }

    static void *hook(void *arg) {
        CThread *thread = (CThread*) arg;
        thread->pid = gettid();

        if (thread->getRunnable()) {
            thread->getRunnable()->run(thread, thread->getArgs());
        }

        return (void*) NULL;
    }

private:
    #ifdef _syscall0
    static _syscall0(pid_t,gettid)
    #else
    static pid_t gettid() { return static_cast<pid_t>(syscall(__NR_gettid));}
    #endif

private:
    pthread_t tid;      // pthread_self() id
    int pid;            // process id of pthread
    Runnable *runnable;
    void *args;
};

}

#endif /*THREAD_H_*/
