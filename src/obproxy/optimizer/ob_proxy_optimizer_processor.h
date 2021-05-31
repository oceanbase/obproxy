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
