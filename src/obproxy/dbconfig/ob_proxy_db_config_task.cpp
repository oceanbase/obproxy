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
#include "dbconfig/ob_proxy_db_config_task.h"
#include "dbconfig/ob_proxy_db_config_processor.h"
#include "dbconfig/grpc/ob_proxy_grpc_client.h"
#include "dbconfig/grpc/ob_proxy_grpc_utils.h"
#include "dbconfig/ob_proxy_pb_utils.h"
#include "dbconfig/ob_proxy_db_config_info.h"

using namespace grpc;
using namespace google::protobuf;
using namespace envoy::service::discovery::v2;
using namespace envoy::api::v2;
using namespace envoy::api::v2::core;
using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace dbconfig
{

//-------ObWatchParentCont------
int ObWatchParentCont::alloc_watch_parent_cont(ObWatchParentCont *&cont, const ObDDSCrdType type)
{
  int ret = OB_SUCCESS;
  ObProxyMutex *mutex = NULL;
  cont = NULL;
  if (OB_ISNULL(mutex = new_proxy_mutex())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_EDIAG("fail to alloc memory for mutex", K(ret));
  } else if (OB_ISNULL(cont = new(std::nothrow) ObWatchParentCont(mutex, type))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_EDIAG("fail to alloc memory for ObWatchParentCont", K(ret));
    if (OB_LIKELY(NULL != mutex)) {
      mutex->free();
      mutex = NULL;
    }
  }
  return ret;
}

void ObWatchParentCont::destroy()
{
  ObAsyncCommonTask::destroy();
}

int ObWatchParentCont::init_task()
{
  int ret = OB_SUCCESS;
  ObGrpcClient *grpc_client = get_global_db_config_processor().get_grpc_client_pool().acquire_grpc_client();
  if (NULL != grpc_client) {
    LOG_DEBUG("begin to do dbconfig fetch task", "task_name", get_type_task_name(type_));
    // send fetch crd request
    DiscoveryRequest request;
    DiscoveryResponse response;
    request.set_version_info("");
    request.set_type_url(get_type_url(type_));
    if (grpc_client->sync_write(request)) {
      while (grpc_client->sync_read(response)) {
        const std::string& type_url = response.type_url();
        LOG_INFO("Received gRPC message", "type_url", type_url.c_str(),
                 "version", response.version_info().c_str(),
                 "resource size", response.resources_size());
        if (OB_FAIL(get_global_db_config_processor().sync_fetch_tenant_config(response))) {
          LOG_WDIAG("fail to fetch tenant config", K(ret));
        }
      } // end while
    } // end write
    LOG_DEBUG("finish watch parent cr task", "type", get_type_task_name(type_));
  }

  if (NULL != grpc_client) {
    get_global_db_config_processor().get_grpc_client_pool().release_grpc_client(grpc_client);
    grpc_client = NULL;
  }

  self_ethread().schedule_in(this, HRTIME_MSECONDS(RETRY_INTERVAL_MS), EVENT_IMMEDIATE);
  LOG_DEBUG("succ to reschedule task", "type", get_type_task_name(type_));
  terminate_ = false;
  // remember to reset ret, or terminate will be set to true if async common task failed in ini_task()
  ret = OB_SUCCESS;
  return ret;
}

} // end namespace dbconfig
} // end namespace proxy
} // end namespace oceanbase
