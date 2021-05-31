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

#ifndef OBPROXY_CLIENT_SESSION_H
#define OBPROXY_CLIENT_SESSION_H

#include "utils/ob_proxy_lib.h"
#include "iocore/net/ob_net.h"
#include "proxy/api/ob_api.h"
#include "proxy/api/ob_session_plugin.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

typedef int (ObProxyClientSession::*ClientSessionHandler)(int event, void *data);

#define CLIENT_SESSION_SET_DEFAULT_HANDLER(h) {        \
     cs_default_handler_ = (ClientSessionHandler)h;    \
}

class ObProxyClientSession : public event::ObVConnection
{
public:
  ObProxyClientSession();
  virtual ~ObProxyClientSession() {}

  virtual void destroy() = 0;
  virtual int start() = 0;

  virtual int new_connection(net::ObNetVConnection *new_vc, event::ObMIOBuffer *iobuf,
                             event::ObIOBufferReader *reader) = 0;

  virtual int ssn_hook_append(const ObMysqlHookID id, ObContInternal *cont)
  {
    return api_hooks_.append(id, cont);
  }

  virtual int ssn_hook_prepend(const ObMysqlHookID id, ObContInternal *cont)
  {
    return api_hooks_.prepend(id, cont);
  }

  ObAPIHook *ssn_hook_get(const ObMysqlHookID id) const { return api_hooks_.get(id); }

  void add_plugin(ObSessionPlugin *plugin)
  {
    plugins_.push(plugin);
  }

  void *get_user_arg(unsigned ix) const
  {
    void *ret = NULL;
    if (OB_LIKELY(ix < countof(user_args_))) {
      ret = user_args_[ix];
    }
    return ret;
  }

  void set_user_arg(unsigned ix, void *arg)
  {
    if (OB_LIKELY(ix < countof(user_args_))) {
      user_args_[ix] = arg;
    }
  }

  bool hooks_enabled() const { return hooks_on_; }

  bool has_hooks() const { return api_hooks_.has_hooks() || mysql_global_hooks->has_hooks(); }

  // Initiate an API hook invocation.
  // if fail to do_api_callout, the caller should handle error itself
  int do_api_callout(ObMysqlHookID id);

  void cleanup();

protected:
  bool hooks_on_;
  ClientSessionHandler cs_default_handler_;

private:
  ObAPIHookScope api_scope_;
  ObMysqlHookID api_hookid_;
  ObAPIHook *api_current_;
  ObMysqlAPIHooks api_hooks_;
  SList(ObSessionPlugin, link_) plugins_;
  void *user_args_[MYSQL_SSN_TXN_MAX_USER_ARG];

  int state_api_callout(int event, void *data);
  void handle_api_return(int event);
  DISALLOW_COPY_AND_ASSIGN(ObProxyClientSession);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_CLIENT_SESSION_H
