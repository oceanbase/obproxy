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

#include "defaultrunnable.h"
#include "lib/thread/thread.h"
#include "lib/oblog/ob_log.h"
namespace obsys {

CDefaultRunnable::CDefaultRunnable(int threadCount) {
    _stop = false;
    _threadCount = threadCount;
    _thread = NULL;
}
CDefaultRunnable::~CDefaultRunnable() {
    if (_thread) {
        delete[] _thread;
        _thread = NULL;
    }
}

void CDefaultRunnable::setThreadCount(int threadCount)
{
    if (_thread != NULL) {
        _OB_LOG(ERROR, "already running, can not set threadCount");
        return;
    }
    _threadCount = threadCount;
}

// start
int CDefaultRunnable::start() {
    if (_thread != NULL || _threadCount < 1) {
        _OB_LOG(ERROR, "start failure, _thread: %p, threadCount: %d", _thread, _threadCount);
        return 0;
    }
    _thread = new CThread[_threadCount];
    if (NULL == _thread)
    {
        _OB_LOG(ERROR, "create _thread object failed, threadCount: %d", _threadCount);
        return 0;
    }
    int i = 0;
    for (; i<_threadCount; i++)
    {
        if (!_thread[i].start(this, (void*)((long)i)))
        {
          return i;
        }
    }
    return i;
}

// stop
void CDefaultRunnable::stop() {
    _stop = true;
}

// wait
void CDefaultRunnable::wait() {
    if (_thread != NULL)
    {
        for (int i=0; i<_threadCount; i++)
        {
            _thread[i].join();
        }
    }
}

}

////////END
