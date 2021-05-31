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

#include "proxy/api/ob_global_plugin.h"
#include "proxy/api/ob_api_utils_internal.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
using namespace event;

int handle_global_plugin_events(ObContInternal *cont, ObEventType event, void *edata)
{
  ObGlobalPlugin *global_plugin = static_cast<ObGlobalPlugin *>(cont->data_);
  DEBUG_API("Invoking global plugin %p for event %d on transaction %p",
            global_plugin, event, edata);
  ObApiUtilsInternal::invoke_plugin_for_event(global_plugin, edata, event);

  return 0;
}

ObGlobalPlugin::ObGlobalPlugin()
{
  cont_ = ObContInternal::alloc(handle_global_plugin_events, NULL);
  cont_->data_ = static_cast<void *>(this);
}

ObGlobalPlugin::ObGlobalPlugin(const ObGlobalPlugin &other)
    : cont_(NULL)
{
  *this = other;
}

ObGlobalPlugin &ObGlobalPlugin::operator =(const ObGlobalPlugin &other)
{
  if (this != &other) {
    cont_ = ObContInternal::alloc(handle_global_plugin_events, new_proxy_mutex());
    cont_->data_ = static_cast<void *>(this);
  }

  return *this;
}

void ObGlobalPlugin::destroy()
{
  ObApiPlugin::destroy();
  if (NULL != cont_) {
    cont_->destroy();
    cont_ = NULL;
  }
}

void ObGlobalPlugin::register_hook(const ObApiPlugin::ObHookType hook_type)
{
  ObMysqlHookID hook_id = ObApiUtilsInternal::convert_internal_hook(hook_type);
  if (hook_id < OB_MYSQL_LAST_HOOK) {
    mysql_global_hooks->append(hook_id, cont_);
  }
  DEBUG_API("Registered global plugin %p for hook %s, hook_id=%s",
             this, HOOK_TYPE_STRINGS[hook_type],
             ObMysqlApiDebugNames::get_api_hook_name(hook_id));
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
