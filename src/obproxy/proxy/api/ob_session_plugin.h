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

#ifndef OBPROXY_SESSION_PLUGIN_H
#define OBPROXY_SESSION_PLUGIN_H

#include "utils/ob_proxy_lib.h"
#include "proxy/api/ob_api_plugin.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObContInternal;
class ObProxyClientSession;

/**
 * @brief The interface used when creating a ObSessionPlugin.
 *
 * A ObSessionPlugin is a plugin that will fire only for the specific client session it
 * was bound to. When you create a ObSessionPlugin you call the parent constructor with
 * the associated client session and it will become automatically bound. This means that your
 * ObSessionPlugin will automatically be destroyed when the client session dies.
 *
 * Depending on the type of hook you choose to build you will implement
 * one or more callback methods.
 * Here is a simple example of a ObSessionPlugin:
 * @code
 * class ObSessionHookPlugin : public ObSessionPlugin
 * {
 * public:
 *   static ObSessionHookPlugin *alloc(ObProxyClientSession *ssn)
 *   {
 *     return op_reclaim_alloc_args(ObSessionHookPlugin, ssn);
 *   }
 *   ObSessionHookPlugin(ObProxyClientSession *ssn)
 *      : ObSessionPlugin(ssn)
 *   {
 *     register_hook(HOOK_SSN_START);
 *   }
 *   void destroy()
 *   {
 *     ObSessionPlugin::destroy();
 *   }
 *   virtual void handle_ssn_start(ObProxyClientSession *ssn)
 *   {
 *     std::cout << "Hello from handle_ssn_start!" << std::endl;
 *     ObApiPlugin::handle_ssn_start(ssn);
 *   }
 * };
 * @endcode
 * @see ObApiPlugin
 */
class ObSessionPlugin : public ObApiPlugin
{
public:
  virtual void destroy();

  /**
   * register_hook is the mechanism used to attach a session hook.
   *
   * @note Whenever you register a hook you must have the appropriate callback
   *  definied in your ObSessionPlugin see ObHookType and ObApiPlugin for the
   *  correspond ObHookTypes and callback methods. If you fail to implement the
   *  callback, a default implmentation will be used that will only resume the
   *  ObApiTransaction.
   * @see ObHookType
   * @see ObApiPlugin
   */
  void register_hook(const ObApiPlugin::ObHookType);

  SLINK(ObSessionPlugin, link_);

protected:
  explicit ObSessionPlugin(ObProxyClientSession *session);
  virtual ~ObSessionPlugin() { }

private:
  DISALLOW_COPY_AND_ASSIGN(ObSessionPlugin);
  ObContInternal *cont_;
  ObProxyClientSession *session_;
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase


#endif // OBPROXY_SESSION_PLUGIN_H
