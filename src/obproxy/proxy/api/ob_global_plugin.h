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

#ifndef OBPROXY_GLOBAL_PLUGIN_H
#define OBPROXY_GLOBAL_PLUGIN_H

#include "utils/ob_proxy_lib.h"
#include "proxy/api/ob_api_plugin.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObContInternal;

/**
 *
 *
 * @brief The interface used when creating a ObGlobalPlugin.
 *
 * A ObGlobalPlugin is a ObApiPlugin that will fire for a given hook
 * on all ObApiTransactions. In otherwords, a ObGlobalPlugin is not
 * tied to a specific plugin, a ObApiTransaction specific plugin would
 * be a ObTransactionPlugin.
 *
 * Depending on the type of hook you choose to build you will implement
 * one or more callback methods.
 * Here is a simple example of a ObGlobalPlugin:
 * @code
 * class ObGlobalHookPlugin : public ObGlobalPlugin
 * {
 * public:
 *  ObGlobalHookPlugin()
 *  {
 *    register_hook(HOOK_READ_REQUEST);
 *  }
 *  void destroy()
 *  {
 *    ObGlobalPlugin::destroy();
 *  }
 *  virtual void handle_read_request(ObApiTransaction &transaction)
 *  {
 *    std::cout << "Hello from handle_read_request!" << std::endl;
 *    transaction.resume();
 *  }
 * };
 * @endcode
 * @see ObApiPlugin
 */
class ObGlobalPlugin : public ObApiPlugin
{
public:
  virtual void destroy();

  /**
   * register_hook is the mechanism used to attach a global hook.
   *
   * @note Whenever you register a hook you must have the appropriate callback
   *  definied in your ObGlobalPlugin see ObHookType and ObApiPlugin for the
   *  correspond ObHookTypes and callback methods. If you fail to implement the
   *  callback, a default implmentation will be used that will only resume the
   *  ObApiTransaction.
   * @see ObHookType
   * @see ObApiPlugin
   */
  void register_hook(const ObApiPlugin::ObHookType);

protected:
  ObGlobalPlugin();
  virtual ~ObGlobalPlugin() { }
  ObGlobalPlugin(const ObGlobalPlugin &other);
  ObGlobalPlugin &operator =(const ObGlobalPlugin &other);

  ObContInternal *cont_;
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase


#endif // OBPROXY_GLOBAL_PLUGIN_H
