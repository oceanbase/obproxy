/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_GRPC_CLIENT_POOL_H
#define OBPROXY_GRPC_CLIENT_POOL_H

#include "lib/atomic/ob_atomic.h"
#include "lib/list/ob_atomic_list.h"

namespace oceanbase
{
namespace obproxy
{
namespace dbconfig
{

class ObGrpcClient;
class ObGrpcClientPool
{
public:
  ObGrpcClientPool() : is_inited_(false),
                       client_count_(0),
                       free_gc_list_() {}
  ~ObGrpcClientPool() {}
  int init(int64_t client_count, bool &is_client_valid);

  ObGrpcClient *acquire_grpc_client();
  void release_grpc_client(ObGrpcClient *client);

private:
  bool is_inited_;
  int64_t client_count_;
  common::ObAtomicList free_gc_list_;
};

}
}
}
#endif /* OBPROXY_GRPC_CLIENT_POOL_H */
