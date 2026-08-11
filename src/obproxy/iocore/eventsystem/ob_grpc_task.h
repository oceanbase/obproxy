/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_GRPC_TASK_H
#define OBPROXY_GRPC_TASK_H

#include "iocore/eventsystem/ob_processor.h"
#include "iocore/eventsystem/ob_event.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{
extern ObEventThreadType ET_GRPC;

class ObGrpcTaskProcessor : public ObProcessor
{
public:
  int start(const int64_t grpc_threads, const int64_t stacksize = DEFAULT_STACKSIZE);
};

extern ObGrpcTaskProcessor g_grpc_task_processor;
} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_GRPC_TASK_H
