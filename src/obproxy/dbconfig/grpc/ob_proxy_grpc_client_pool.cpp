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

#define USING_LOG_PREFIX PROXY
#include "dbconfig/grpc/ob_proxy_grpc_client_pool.h"
#include "utils/ob_proxy_lib.h"
#include "lib/list/ob_atomic_list.h"
#include "dbconfig/grpc/ob_proxy_grpc_client.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace dbconfig
{

int ObGrpcClientPool::init(int64_t client_count, bool &is_client_avail)
{
  int ret = OB_SUCCESS;
  is_client_avail = false;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K_(is_inited), K(ret));
  } else if (OB_FAIL(free_gc_list_.init("grpc_client_list",
             reinterpret_cast<int64_t>(&(reinterpret_cast<ObGrpcClient *>(0))->link_)))) {
    LOG_WARN("fail to init gc_list", K(ret));
  } else {
    ObGrpcClient *grpc_client = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < client_count; ++i) {
      if (OB_FAIL(ObGrpcClient::alloc(grpc_client))) {
        LOG_WARN("fail to alloc grpc client", K(ret));
      } else if (OB_ISNULL(grpc_client)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("grpc_client is null", K(ret));
      } else {
        if (grpc_client->is_avail()) {
          is_client_avail = true;
        }
        free_gc_list_.push(grpc_client);
      }
    }
    if (OB_SUCC(ret)) {
      client_count_ = client_count;
      is_inited_ = true;
      LOG_INFO("succ to init grpc client pool", K(client_count), K(is_client_avail));
    }
  }
  return ret;
}

ObGrpcClient *ObGrpcClientPool::acquire_grpc_client()
{
  return reinterpret_cast<ObGrpcClient *>(free_gc_list_.pop());
}

void ObGrpcClientPool::release_grpc_client(ObGrpcClient *client)
{
  if (NULL != client) {
    free_gc_list_.push(client);
  }
}

} // end dbconfig
} // end obproxy
} // end oceanbase
