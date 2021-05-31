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

#ifndef OBPROXY_TRANSACTION_PLUGIN_H
#define OBPROXY_TRANSACTION_PLUGIN_H

#include "iocore/eventsystem/ob_lock.h"
#include "proxy/api/ob_api_plugin.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

class ObApiUtilsInternal;
class ObApiTransaction;
class ObMysqlSM;

/**
 * @brief The interface used when creating a ObTransactionPlugin.
 *
 * A ObApiTransaction is a plugin that will fire only for the specific ObApiTransaction it
 * was bound to. When you create a ObTransactionPlugin you call the parent constructor with
 * the associated ObApiTransaction and it will become automatically bound. This means that your
 * ObTransactionPlugin will automatically be destroyed when the ObApiTransaction dies.
 *
 * Implications of this are that you can easily add ObApiTransaction scoped storage by adding
 * a member to a ObTransactionPlugin since the destructor will be called of your ObTransactionPlugin
 * any cleanup that needs to happen can happen in your destructor as you normally would.
 *
 * You must always be sure to implement the appropriate callback for the type of hook you register.
 * @code
 * // For a more detailed example see examples/transactionhook/
 * class ObTransactionHookPlugin : public ObTransactionPlugin
 * {
 * public:
 *   static ObTransactionHookPlugin *alloc(ObApiTransaction &transaction)
 *   {
 *     return op_reclaim_alloc_args(ObTransactionHookPlugin, transaction);
 *   }
 *   virtual void destroy()
 *   {
 *     ObTransactionPlugin::destroy();
 *     delete[] char_ptr_; // cleanup
 *     op_reclaim_free(this);
 *   }
 *   void handle_send_response(ObApiTransaction &transaction)
 *   {
 *     transaction.resume();
 *   }
 * private:
 *   ObTransactionHookPlugin(ObApiTransaction &transaction) : ObTransactionPlugin(transaction)
 *   {
 *     char_ptr_ = new char[100]; // ObApiTransaction scoped storage
 *     register_hook(HOOK_SEND_RESPONSE);
 *   }
 *
 *   char *char_ptr_;
 * };
 * @endcode
 * @see ObApiPlugin
 * @see ObHookType
 */
class ObTransactionPlugin : public ObApiPlugin
{
  friend class ObApiUtilsInternal;
  friend class ObApiTransaction;

public:
  virtual void destroy();

  /**
   * register_hook is the mechanism used to attach a transaction hook.
   *
   * @note Whenever you register a hook you must have the appropriate callback definied
   * in your ObTransactionPlugin see ObHookType and ObApiPlugin for the correspond HookTypes
   * and callback methods. If you fail to implement the callback, a default implmentation
   * will be used that will only resume the ObApiTransaction.
   * @param hook_type
   *
   * @see ObHookType
   * @see ObApiPlugin
   */
  void register_hook(ObApiPlugin::ObHookType hook_type);

  SLINK(ObTransactionPlugin, link_);

protected:
  explicit ObTransactionPlugin(ObApiTransaction &transaction);

  /**
   * This method will return a ObPtr to a ObProxyMutex that can be used for ObAsyncProvider
   * and ObAsyncReceiver operations.
   *
   * If another thread wanted to stop this transaction from dispatching an event it
   * could be passed this mutex and it would be able to lock it and prevent another
   * thread from dispatching back into this ObTransactionPlugin.
   *
   * @return
   */
  common::ObPtr<event::ObProxyMutex> &get_mutex() { return mutex_; }

  ObContInternal *cont_;
  common::ObPtr<event::ObProxyMutex> mutex_;
  ObMysqlSM *sm_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTransactionPlugin);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_TRANSACTION_PLUGIN_H
