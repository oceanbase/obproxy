/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_SESSION_POOL_TASK_H
#define OBPROXY_SESSION_POOL_TASK_H

#include "iocore/eventsystem/ob_processor.h"
#include "iocore/eventsystem/ob_event.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{
extern ObEventThreadType ET_SESS_POOL;

class ObSessionPoolEventProcessor : public ObProcessor
{
public:
  int start(const int64_t threads, const int64_t stacksize = DEFAULT_STACKSIZE);
};

extern ObSessionPoolEventProcessor g_session_pool_event_processor;
} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_GRPC_PARENT_TASK_H
