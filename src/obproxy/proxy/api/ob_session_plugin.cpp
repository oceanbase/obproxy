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

#include "proxy/api/ob_session_plugin.h"
#include "proxy/api/ob_api_utils_internal.h"
#include "proxy/mysql/ob_proxy_client_session.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
using namespace event;

int handle_session_plugin_events(ObContInternal *cont, ObEventType event, void *edata)
{
  ObProxyClientSession *ssn = static_cast<ObProxyClientSession *>(edata);
  ObSessionPlugin *session_plugin = static_cast<ObSessionPlugin *>(cont->data_);
  DEBUG_API("Invoking session plugin %p for event %d on session %p",
            session_plugin, event, edata);
  ObApiUtilsInternal::invoke_plugin_for_event(session_plugin, ssn, event);

  return 0;
}

ObSessionPlugin::ObSessionPlugin(ObProxyClientSession *session)
{
  ob_assert(NULL != session);
  session_ = session;
  cont_ = ObContInternal::alloc(handle_session_plugin_events, new_proxy_mutex());
  cont_->data_ = static_cast<void *>(this);
}

void ObSessionPlugin::destroy()
{
  ObApiPlugin::destroy();
  if (NULL != cont_) {
    cont_->destroy();
    cont_ = NULL;
  }
}

void ObSessionPlugin::register_hook(const ObApiPlugin::ObHookType hook_type)
{
  ObMysqlHookID hook_id = ObApiUtilsInternal::convert_internal_hook(hook_type);
  if (hook_id < OB_MYSQL_LAST_HOOK) {
    session_->ssn_hook_append(hook_id, cont_);
  }
  DEBUG_API("Registered session plugin %p for hook %s, hook_id=%s",
             this, HOOK_TYPE_STRINGS[hook_type],
             ObMysqlApiDebugNames::get_api_hook_name(hook_id));
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
