/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_OPTIMIZER_PROCESSOR_H
#define OBPROXY_OPTIMIZER_PROCESSOR_H

#include "lib/allocator/page_arena.h"

namespace oceanbase
{
namespace obproxy
{
namespace optimizer
{

class ObProxyOptimizerProcessor
{
public:
  ObProxyOptimizerProcessor() {}
  ~ObProxyOptimizerProcessor() {}

  int alloc_allocator(common::ObIAllocator *&allocator);
  int free_allocator(common::ObIAllocator *allocator);

private:
  common::ObArenaAllocator allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObProxyOptimizerProcessor);
};

extern ObProxyOptimizerProcessor g_ob_proxy_optimizer_processor;
inline ObProxyOptimizerProcessor &get_global_optimizer_processor()
{
  return g_ob_proxy_optimizer_processor;
}

} // end of namespace optimizer
} // end of namespace obproxy
} // end of namespace oceanbase

#endif //OBPROXY_OPTIMIZER_PROCESSOR_H
