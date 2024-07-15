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

#define USING_LOG_PREFIX PROXY_ICMD

#include "cmd/ob_alter_resource_handler.h"
#include "iocore/eventsystem/ob_event_processor.h"
#include "iocore/eventsystem/ob_task.h"
#include "obutils/ob_resource_pool_processor.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
ObAlterResourceHandler::ObAlterResourceHandler(ObContinuation *cont, ObMIOBuffer *buf, const ObInternalCmdInfo &info)
  : ObInternalCmdHandler(cont, buf, info), capability_(info.get_capability())
{
  SET_HANDLER(&ObAlterResourceHandler::handle_delete_cluster);
  if (!info.get_cluster_string().empty()) {
    int32_t min_len =std::min(info.get_cluster_string().length(), static_cast<int32_t>(OB_PROXY_MAX_CLUSTER_NAME_LENGTH));
    MEMCPY(cluster_str_, info.get_cluster_string().ptr(), min_len);
    cluster_str_[min_len] = '\0';
  } else {
    cluster_str_[0] = '\0';
  }
}

int ObAlterResourceHandler::handle_delete_cluster(int event, void *data)
{
  int event_ret = EVENT_DONE;
  int ret = OB_SUCCESS;
  UNUSED(event);
  UNUSED(data);
  ObString cluster_name(cluster_str_);
  if (cluster_name.empty()) {
    ret = OB_INNER_STAT_ERROR;
    WDIAG_ICMD("cluster name can not be empty", K(cluster_name), K(ret));
  } else {
    ObResourcePoolProcessor &rpp = get_global_resource_pool_processor();
    if (ObString::make_string(OB_META_DB_CLUSTER_NAME) == cluster_name) {
      if (OB_FAIL(rpp.rebuild_metadb())) { // metadb can not delete, but rebuilding is allowed
        LOG_WDIAG("fail to rebuild metadb", K(ret));
      }
    } else if (OB_FAIL(rpp.delete_cluster_resource(cluster_name))) {
      LOG_WDIAG("fail to delete cluster resource", K(cluster_name), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(encode_ok_packet(0, capability_))) {
      WDIAG_ICMD("fail to encode ok packet", K(ret));
    } else {
      INFO_ICMD("succ to delete cluster resource", K(cluster_name));
      event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
    }
  } else {
    int errcode = ret;
    if (OB_FAIL(encode_err_packet(errcode))) {
      WDIAG_ICMD("fail to encode err resp packet", K(errcode), K(ret));
    } else {
      INFO_ICMD("succ to encode err resp packet", K(errcode));
      event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
    }
  }

  if (OB_FAIL(ret)) {
    event_ret = internal_error_callback(ret);
  }
  return event_ret;
}

static int delete_cluster_cmd_callback(ObContinuation *cont, ObInternalCmdInfo &info,
    ObMIOBuffer *buf, ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;
  ObAlterResourceHandler *handler = NULL;
  const bool is_query_cmd = false;

  if (OB_UNLIKELY(!ObInternalCmdHandler::is_constructor_argument_valid(cont, buf))) {
    ret = OB_INVALID_ARGUMENT;
    WDIAG_ICMD("constructor argument is invalid", K(cont), K(buf), K(ret));
  } else if (OB_ISNULL(handler = new(std::nothrow) ObAlterResourceHandler(cont, buf, info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    EDIAG_ICMD("fail to new ObAlterResourceHandler", K(ret));
  } else if (OB_FAIL(handler->init(is_query_cmd))) {
    WDIAG_ICMD("fail to init for ObAlterResourceHandler", K(ret));
  } else {
    action = &handler->get_action();
    if (OB_ISNULL(g_event_processor.schedule_imm(handler, ET_CALL))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      EDIAG_ICMD("fail to schedule ObAlterResourceHandler", K(ret));
      action = NULL;
    } else {
      DEBUG_ICMD("succ to schedule ObAlterResourceHandler");
    }
  }

  if (OB_FAIL(ret) && OB_LIKELY(NULL != handler)) {
    delete handler;
    handler = NULL;
  }
  return ret;
}

int alter_resource_delete_cmd_init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_global_internal_cmd_processor().register_cmd(OBPROXY_T_ICMD_ALTER_RESOURCE,
                                                               &delete_cluster_cmd_callback))) {
    WDIAG_ICMD("fail to register CMD_TYPE_DELETE_CLUSTER", K(ret));
  }
  return ret;
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
