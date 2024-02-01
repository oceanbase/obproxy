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
#include "proxy/mysql/ob_proxy_client_session.h"
#include "proxy/mysqllib/ob_proxy_parser_utils.h"
#include "proxy/mysql/ob_mysql_debug_names.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

ObProxyClientSession::ObProxyClientSession()
    : ObVConnection(NULL), hooks_on_(true),
      cs_default_handler_(NULL), api_scope_(API_HOOK_SCOPE_NONE),
      api_hookid_(OB_MYSQL_LAST_HOOK), api_current_(NULL)
{
  memset(&user_args_, 0, sizeof(user_args_));
}

static const ObEventType eventmap[OB_MYSQL_LAST_HOOK + 1] = {
  OB_EVENT_MYSQL_READ_REQUEST,            // OB_MYSQL_READ_REQUEST_HOOK
  OB_EVENT_MYSQL_OBSERVER_PL,             // OB_MYSQL_OBSERVER_PL_HOOK
  OB_EVENT_MYSQL_SEND_REQUEST,            // OB_MYSQL_SEND_REQUEST_HOOK
  OB_EVENT_MYSQL_READ_RESPONSE,           // OB_MYSQL_READ_RESPONSE_HOOK
  OB_EVENT_MYSQL_SEND_RESPONSE,           // OB_MYSQL_SEND_RESPONSE_HOOK
  OB_EVENT_MYSQL_REQUEST_TRANSFORM,       // OB_MYSQL_REQUEST_TRANSFORM_HOOK
  OB_EVENT_MYSQL_RESPONSE_TRANSFORM,      // OB_MYSQL_RESPONSE_TRANSFORM_HOOK
  OB_EVENT_MYSQL_TXN_START,               // OB_MYSQL_TXN_START_HOOK
  OB_EVENT_MYSQL_TXN_CLOSE,               // OB_MYSQL_TXN_CLOSE_HOOK
  OB_EVENT_MYSQL_SSN_START,               // OB_MYSQL_SSN_START_HOOK
  OB_EVENT_MYSQL_SSN_CLOSE,               // OB_MYSQL_SSN_CLOSE_HOOK
  OB_EVENT_NONE,                          // OB_MYSQL_RESPONSE_CLIENT_HOOK
  OB_EVENT_MYSQL_CMD_COMPLETE,            // OB_MYSQL_CMD_COMPLETE_HOOK
  OB_EVENT_NONE,                          // OB_MYSQL_LAST_HOOK
};

static bool is_valid_hook(ObMysqlHookID hookid)
{
  return (hookid >= 0) && (hookid < OB_MYSQL_LAST_HOOK);
}

void ObProxyClientSession::cleanup()
{
  DEBUG_API("client_session=%p destroying session plugin", this);
  ObSessionPlugin *tmp_plugin = NULL;
  for (ObSessionPlugin *plugin = plugins_.head_; NULL != plugin;) {
    // get next first, then destroy current plugin
    tmp_plugin = plugin;
    plugin = plugins_.next(plugin);
    tmp_plugin->destroy();
  }
  plugins_.reset();
  api_hooks_.destroy();
  mutex_.release();
}

int ObProxyClientSession::state_api_callout(int event, void *data)
{
  UNUSED(data);
  switch (event) {
    case EVENT_NONE:
    case EVENT_INTERVAL:
    case OB_EVENT_MYSQL_CONTINUE:
      if (OB_LIKELY(is_valid_hook(api_hookid_))) {
        if (NULL == api_current_ && API_HOOK_SCOPE_GLOBAL == api_scope_) {
          api_current_ = mysql_global_hooks->get(api_hookid_);
          api_scope_ = API_HOOK_SCOPE_LOCAL;
        }

        if (NULL == api_current_ && API_HOOK_SCOPE_LOCAL == api_scope_) {
          api_current_ = ssn_hook_get(api_hookid_);
          api_scope_ = API_HOOK_SCOPE_NONE;
        }

        if (NULL != api_current_) {
          ObAPIHook *hook = api_current_;
          ObPtr<ObProxyMutex> plugin_mutex;

          if (NULL != hook->cont_->mutex_) {
            plugin_mutex = hook->cont_->mutex_;
            MUTEX_TRY_LOCK(plugin_lock, hook->cont_->mutex_, mutex_->thread_holding_);
            if (!plugin_lock.is_locked()) {
              CLIENT_SESSION_SET_DEFAULT_HANDLER(&ObProxyClientSession::state_api_callout);
              if (OB_ISNULL(mutex_->thread_holding_->schedule_in(this, HRTIME_MSECONDS(1)))) {
                LOG_WDIAG("schedule in error");
              }
            } else {
              api_current_ = api_current_->next();
              hook->invoke(eventmap[api_hookid_], this);
            }
          } else {
            api_current_ = api_current_->next();
            hook->invoke(eventmap[api_hookid_], this);
          }
          break;
        }
      }

      handle_api_return(event);
      break;

    case OB_EVENT_MYSQL_ERROR:
      handle_api_return(event);
      break;

    default:
      LOG_WDIAG("invalid event, event", K(event));
      break;
  }

  return EVENT_NONE;
}

int ObProxyClientSession::do_api_callout(ObMysqlHookID id)
{
  int ret = OB_SUCCESS;
  if (OB_MYSQL_SSN_START_HOOK != id && OB_MYSQL_SSN_CLOSE_HOOK != id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid hook id", K(id), K(ret));
  } else {
    api_hookid_ = id;
    api_scope_ = API_HOOK_SCOPE_GLOBAL;
    api_current_ = NULL;

    if (hooks_on_ && has_hooks()) {
      CLIENT_SESSION_SET_DEFAULT_HANDLER(&ObProxyClientSession::state_api_callout);
      state_api_callout(EVENT_NONE, NULL);
    } else {
      handle_api_return(OB_EVENT_MYSQL_CONTINUE);
    }
  }
  return ret;
}

void ObProxyClientSession::handle_api_return(int event)
{
  int ret = OB_SUCCESS;
  ObMysqlHookID hookid = api_hookid_;

  CLIENT_SESSION_SET_DEFAULT_HANDLER(&ObProxyClientSession::state_api_callout);

  api_hookid_ = OB_MYSQL_LAST_HOOK;
  api_scope_ = API_HOOK_SCOPE_NONE;
  api_current_ = NULL;

  switch (hookid) {
    case OB_MYSQL_SSN_START_HOOK:
      if (OB_EVENT_MYSQL_ERROR == event) {
        do_io_close();
      } else if (OB_FAIL(start())) {
        LOG_WDIAG("start client session failed, connection will be closed", K(ret));
      }
      break;

    case OB_MYSQL_SSN_CLOSE_HOOK:
      destroy();
      break;

    default:
      LOG_WDIAG("received invalid session hook", "hook name",
               ObMysqlApiDebugNames::get_api_hook_name(hookid), K(hookid));
      break;
  }
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
