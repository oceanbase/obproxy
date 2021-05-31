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
#include "dbconfig/ob_dbconfig_child_cont.h"
#include "dbconfig/ob_dbconfig_db_cont.h"
#include "dbconfig/ob_dbconfig_tenant_cont.h"
#include "dbconfig/ob_proxy_db_config_processor.h"
#include "dbconfig/grpc/ob_proxy_grpc_client.h"
#include "dbconfig/grpc/ob_proxy_grpc_utils.h"
#include "dbconfig/ob_proxy_pb_utils.h"
#include "dbconfig/ob_proxy_db_config_info.h"

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

//-------ObDbConfigFetchCont------
int ObDbConfigFetchCont::alloc_fetch_cont(ObDbConfigFetchCont *&cont, ObContinuation *cb_cont, const int64_t index, const ObDDSCrdType type)
{
  int ret = OB_SUCCESS;
  ObProxyMutex *mutex = NULL;
  cont = NULL;
  ObEThread *cb_thread = &self_ethread();
  if (OB_ISNULL(mutex = new_proxy_mutex())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc memory for mutex", K(ret));
  } else {
    switch (type) {
      case TYPE_TENANT:
        cont = new(std::nothrow) ObDbConfigTenantCont(mutex, cb_cont, cb_thread, type);
        break;
      case TYPE_DATABASE:
        cont = new(std::nothrow) ObDbConfigDbCont(mutex, cb_cont, cb_thread, type);
        break;
      case TYPE_DATABASE_AUTH:
      case TYPE_DATABASE_VAR:
      case TYPE_DATABASE_PROP:
      case TYPE_SHARDS_TPO:
      case TYPE_SHARDS_ROUTER:
      case TYPE_SHARDS_DIST:
      case TYPE_SHARDS_CONNECTOR:
      case TYPE_SHARDS_PROP:
        cont = new(std::nothrow) ObDbConfigChildCont(mutex, cb_cont, cb_thread, type);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown type", K(type));
    }
  }
  if (OB_ISNULL(cont)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc memory for ObDbConfigFetchCont", K(ret));
    if (OB_LIKELY(NULL != mutex)) {
      mutex->free();
      mutex = NULL;
    }
  } else {
    cont->result_.cont_index_ = index;
    LOG_DEBUG("succ to alloc fetch cont", "type", get_type_task_name(type), KP(cont));
  }
  return ret;
}

ObDbConfigFetchCont::ObDbConfigFetchCont(ObProxyMutex *m, ObContinuation *cb_cont, ObEThread *cb_thread, const ObDDSCrdType type)
  : ObAsyncCommonTask(m, get_type_task_name(type), cb_cont, cb_thread),
    type_(type), result_()
{
}

void ObDbConfigFetchCont::destroy()
{
  LOG_DEBUG("fetch cont will be destroyed", "type", get_type_task_name(type_), KP(this));
  ObAsyncCommonTask::destroy();
}


} // end namespace dbconfig
} // end namespace proxy
} // end namespace oceanbase
