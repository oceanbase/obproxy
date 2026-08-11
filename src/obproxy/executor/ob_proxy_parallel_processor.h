/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_PARALLEL_PROCESSOR_H
#define OBPROXY_PARALLEL_PROCESSOR_H

#include "dbconfig/ob_proxy_db_config_info.h"
#include "iocore/eventsystem/ob_action.h"
#include "lib/container/ob_iarray.h"

namespace oceanbase
{
namespace obproxy
{
namespace executor
{

class ObProxyParallelParam
{
public:    
  ObProxyParallelParam() : shard_conn_(NULL), shard_prop_(NULL), request_sql_() {}
  ~ObProxyParallelParam() {}

  TO_STRING_KV(KPC_(shard_conn), K_(request_sql));

public:    
  dbconfig::ObShardConnector* shard_conn_;
  dbconfig::ObShardProp* shard_prop_;
  common::ObString request_sql_;
};

class ObProxyParallelProcessor
{
public:
  ObProxyParallelProcessor() {}
  ~ObProxyParallelProcessor() {}

  int open(event::ObContinuation &cont, event::ObAction *&action,
           common::ObIArray<ObProxyParallelParam> &parallel_param,
           common::ObIAllocator *allocator,
           const int64_t timeout_ms = 0);

private:
  DISALLOW_COPY_AND_ASSIGN(ObProxyParallelProcessor);
};

extern ObProxyParallelProcessor g_ob_proxy_parallel_processor;
inline ObProxyParallelProcessor &get_global_parallel_processor()
{
  return g_ob_proxy_parallel_processor;
}

} // end of namespace executor
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_PARALLEL_PROCESSOR_H
