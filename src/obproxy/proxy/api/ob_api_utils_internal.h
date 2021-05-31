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

#ifndef OBPROXY_API_UTILS_INTERNAL_H
#define OBPROXY_API_UTILS_INTERNAL_H

#include "proxy/api/ob_global_plugin.h"
#include "proxy/api/ob_transaction_plugin.h"
#include "proxy/api/ob_transformation_plugin.h"
#include "proxy/api/ob_api_plugin.h"
#include "proxy/api/ob_api_transaction.h"
#include "proxy/api/ob_intercept_plugin.h"
#include "proxy/api/ob_session_plugin.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObProxyClientSession;

class ObApiUtilsInternal
{
public:
  static ObMysqlHookID convert_internal_hook(ObApiPlugin::ObHookType type);
  static ObMysqlHookID convert_internal_transformation_type(ObTransformationPlugin::ObTransformType type);

  static void invoke_plugin_for_event(ObTransactionPlugin *transaction_plugin, ObMysqlSM *sm, ObEventType event);
  static void invoke_plugin_for_event(ObSessionPlugin *session_plugin, ObProxyClientSession *ssn, ObEventType event);
  static void invoke_plugin_for_event(ObGlobalPlugin *global_plugin, void *data, ObEventType event);

  static common::ObPtr<event::ObProxyMutex> &get_transaction_plugin_mutex(ObTransactionPlugin &transaction_plugin);
  static ObApiTransaction *get_api_transaction(ObMysqlSM *sm, const bool create = true);

  static event::ObAction *cont_schedule(ObContInternal *cont, ObHRTime timeout, ObThreadPool tp);
  static event::ObAction *cont_schedule_every(ObContInternal *cont, ObHRTime every, ObThreadPool tp);
  static event::ObAction *mysql_schedule(ObContInternal *cont, ObMysqlSM *sm, ObHRTime timeout);
  static void action_cancel(event::ObAction *action);
  static void mysqlsm_reenable(ObMysqlSM *sm, ObEventType event);
  static void ssn_reenable(ObProxyClientSession *ssn, ObEventType event);

  static void dispatch_intercept_event(ObInterceptPlugin *plugin, ObEventType event, void *edata)
  {
    plugin->handle_event(event, edata);
  }

  static void dispatch_transform_event(ObTransformationPlugin *plugin, ObEventType event, void *edata)
  {
    plugin->handle_event(event, edata);
  }
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_API_UTILS_INTERNAL_H
