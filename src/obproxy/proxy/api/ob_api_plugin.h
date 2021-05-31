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
 *
 * **************************************************************
 *
 * @brief Contains the base interface used in creating Global and Transaciton plugins.
 * @note This interface can never be implemented directly, it should be implemented
 *   through extending ObGlobalPlugin, ObTransactionPlugin, or ObTransformationPlugin.
 */

#ifndef OBPROXY_API_PLUGIN_H
#define OBPROXY_API_PLUGIN_H

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObApiTransaction;
class ObProxyClientSession;

/**
 * @brief The base interface used when creating a ObApiPlugin.
 * @note This interface can never be implemented directly, it should be implemented
 *   through extending ObGlobalPlugin, ObTransactionPlugin, or ObTransformationPlugin.
 * @see ObTransactionPlugin
 * @see ObGlobalPlugin
 * @see ObTransformationPlugin
 */
class ObApiPlugin
{
public:
  virtual void destroy() { }

  /**
   * A enumeration of the available types of Hooks. These are used with
   * ObGlobalPlugin::register_hook() and ObTransactionPlugin::register_hook().
   */
  enum ObHookType
  {
    HOOK_SSN_START = 0,   // fired when client session start, only support global plugin
    HOOK_TXN_START,       // fired when transaction start
    HOOK_READ_REQUEST,    // fired directly after reading request.
    HOOK_SEND_REQUEST,    // fired right before request are sent to the origin
    HOOK_READ_RESPONSE,   // fired right after response have been read from the origin
    HOOK_SEND_RESPONSE,   // fired right before the response are sent to the client
    HOOK_OBSERVER_PL,     // fired right after the observer partition location lookup
    HOOK_CMD_COMPLETE,    // fired after one command of transaction complete
    HOOK_TXN_CLOSE,       // fired when transaction close
    HOOK_SSN_CLOSE        // fired when client session close, only support global plugin
  };

  /**
   * This method must be implemented when you hook HOOK_SSN_START
   */
  virtual void handle_ssn_start(ObProxyClientSession *ssn);

  /**
   * This method must be implemented when you hook HOOK_TXN_START
   */
  virtual void handle_txn_start(ObApiTransaction &transaction);

  /**
   * This method must be implemented when you hook HOOK_READ_REQUEST
   */
  virtual void handle_read_request(ObApiTransaction &transaction);

  /**
   * This method must be implemented when you hook HOOK_SEND_REQUEST
   */
  virtual void handle_send_request(ObApiTransaction &transaction);

  /**
   * This method must be implemented when you hook HOOK_READ_RESPONSE
   */
  virtual void handle_read_response(ObApiTransaction &transaction);

  /**
   * This method must be implemented when you hook HOOK_SEND_RESPONSE
   */
  virtual void handle_send_response(ObApiTransaction &transaction);

  /**
   * This method must be implemented when you hook HOOK_OBSERVER_PL
   */
  virtual void handle_observer_partition_location(ObApiTransaction &transaction);

  /**
   * This method must be implemented when you hook HOOK_CMD_COMPLETE
   */
  virtual void handle_cmd_complete(ObApiTransaction &transaction);

  /**
   * This method must be implemented when you hook HOOK_TXN_CLOSE
   */
  virtual void handle_txn_close(ObApiTransaction &transaction);

  /**
   * This method must be implemented when you hook HOOK_SSN_CLOSE
   */
  virtual void handle_ssn_close(ObProxyClientSession *ssn);

protected:
  /**
   * @note This interface can never be implemented directly, it should be implemented
   *   through extending ObGlobalPlugin, ObTransactionPlugin, or ObTransformationPlugin.
   * @private
   */
  ObApiPlugin() { };
  virtual ~ObApiPlugin() { }
};

/**
 * Human readable strings for each ObHookType, you can access them as
 * HOOK_TYPE_STRINGS[HOOK_OBSERVER_PL] for example.
 */
extern const char *HOOK_TYPE_STRINGS[];

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_API_PLUGIN_H
