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
