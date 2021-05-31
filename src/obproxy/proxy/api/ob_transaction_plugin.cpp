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

#include "proxy/api/ob_api_utils_internal.h"
#include "proxy/api/ob_transaction_plugin.h"
#include "proxy/mysql/ob_mysql_sm.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
using namespace common;
using namespace event;

static int handle_transaction_plugin_events(ObContInternal *cont, ObEventType event, void *edata)
{
  ObMysqlSM *sm = static_cast<ObMysqlSM *>(edata);
  ObTransactionPlugin *plugin = static_cast<ObTransactionPlugin *>(cont->data_);
  DEBUG_API("cont=%p, event=%d, mysqlsm=%p, plugin=%p", cont, event, edata, plugin);
  ObApiUtilsInternal::invoke_plugin_for_event(plugin, sm, event);
  return OB_SUCCESS;
}

ObTransactionPlugin::ObTransactionPlugin(ObApiTransaction &transaction)
{
  cont_ = ObContInternal::alloc(handle_transaction_plugin_events, NULL);
  cont_->data_ = static_cast<void *>(this);
  mutex_ = new_proxy_mutex();
  sm_ = transaction.get_sm();
  DEBUG_API("Creating new ObTransactionPlugin=%p", this);
}

void ObTransactionPlugin::destroy()
{
  DEBUG_API("Destroying ObTransactionPlugin=%p", this);
  ObApiPlugin::destroy();
  cont_->destroy();
  mutex_.release();
}

void ObTransactionPlugin::register_hook(ObApiPlugin::ObHookType hook_type)
{
  ObMysqlHookID hook_id = ObApiUtilsInternal::convert_internal_hook(hook_type);
  if (hook_id < OB_MYSQL_LAST_HOOK) {
    sm_->txn_hook_append(hook_id, cont_);
  }
  DEBUG_API("register hook, ObTransactionPlugin=%p mysqlsm=%p registering "
             "hook_type=%d [%s] hook_id=%s",
             this, sm_, hook_type, HOOK_TYPE_STRINGS[hook_type],
             ObMysqlApiDebugNames::get_api_hook_name(hook_id));
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
