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

#ifndef OBPROXY_API_TRANSACTION_H
#define OBPROXY_API_TRANSACTION_H

#include "utils/ob_proxy_lib.h"
#include "proxy/api/ob_api.h"
#include "proxy/api/ob_transaction_plugin.h"
#include "proxy/mysqllib/ob_mysql_request_analyzer.h"
#include "proxy/mysqllib/ob_mysql_response.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

// forward declarations
class ObApiUtilsInternal;
class ObMysqlSM;

/**
 * @brief ObApiTransaction are the object containing all the state related to a Mysql Transaction
 * @warning Transactions should never be directly created by the user, they will always be automatically
 * created and destroyed as they are needed. Transactions should never be saved beyond the
 * scope of the function in which they are delivered otherwise undefined behaviour will result.
 */
class ObApiTransaction
{
  friend class ObTransactionPlugin; // ObTransactionPlugin is a friend so it can call add_plugin()
  friend class ObTransformationPlugin; // ObTransformationPlugin is a friend so it can call add_plugin()
  friend class ObApiUtilsInternal;

public:
  static ObApiTransaction *alloc(ObMysqlSM *sm) { return op_reclaim_alloc_args(ObApiTransaction, sm); }

  /**
   * destroy all plugins and itself
   *
   * @param
   */
  void destroy();

  /**
   * clear all plugin in the transaction
   *
   * @param
   */
  void reset();

  /**
   * Causes the ObApiTransaction to continue on to other states in the mysql state machine
   * If you do not call resume() on a ObApiTransaction it will remain in that state until
   * it's advanced out by a call to resume() or error().
   */
  void resume();

  /**
   * Causes the ObApiTransaction to advance to the error state in the mysql state machine.
   * @see error(const std::string &)
   */
  void error();

  /**
   * Causes the ObApiTransaction to advance to the error state in the mysql state machine with
   * a specific error message packet displayed. This is functionally equivalent to the following:
   *
   * @code
   * set_error_body(content);
   * error();
   * @endcode
   * @param error_packet the error packet.
   */
  void error(event::ObMIOBuffer *error_packet);

  /**
   * Sets the error packet but this method does not advance the state machine to the error state.
   * To do that you must explicitly call error().
   *
   * @param error_packet the error packet.
   */
  void set_error_body(event::ObMIOBuffer *error_packet);

  /**
   * Get the clients address
   *
   * @return The sockaddr structure representing the client's address
   */
  //const sockaddr *get_client_address() const;

  /**
   * Get the incoming address
   *
   * @return The sockaddr structure representing the incoming address
   */
  //const sockaddr *get_incoming_address() const;

  /**
   * Get the server address
   *
   * @return The sockaddr structure representing the server's address
   */
  //const sockaddr *get_server_address() const;

  /**
   * Set the incoming port on the ObApiTransaction
   *
   * @param port   is the port to set as the incoming port on the transaction
   *
   * @return
   */
  bool set_incoming_port(uint16_t port);

  /**
   * Sets the server address on the ObApiTransaction to a populated sockaddr
   *
   * @return
   */
  //bool set_server_address(const sockaddr *);

  /**
   * Returns a boolean value if the request is an internal cmd.
   *
   * @return boolean value specifying if the request was an internal request.
   */
  bool is_internal_cmd() const;

  /**
   * Returns the ObProxyMysqlRequest object for the incoming request from the client.
   *
   * @return Request object that can be used to manipulate the incoming request from the client.
   */
  ObProxyMysqlRequest &get_client_request() { return *client_request_; }

  /**
   * Returns a ObProxyMysqlRequest object which is the request from obproxy to the observer.
   *
   * @return Request object that can be used to manipulate the outgoing request to the observer.
   */
  ObProxyMysqlRequest &get_server_request() { return *server_request_; }

  /**
   * Returns a ObMysqlResp object which is the response coming from the observer
   *
   * @return Response object that can be used to manipulate the incoming response from the observer.
   */
  ObMysqlResp &get_server_response() { return *server_response_; }

  /**
   * Returns a ObMysqlResp object which is the response going to the client
   *
   * @return Response object that can be used to manipulate the outgoing response from the client.
   */
  ObMysqlResp &get_client_response() { return *client_response_; }

  /**
   * The available types of timeouts you can set on a ObApiTransaction.
   */
  enum ObTimeoutType
  {
    TIMEOUT_CONNECT,      // Timeout on Connect
    TIMEOUT_NO_ACTIVITY,  // Timeout on No Activity
    TIMEOUT_ACTIVE        // Timeout with Activity
  };

  /**
   * Allows you to set various types of timeouts on a ObApiTransaction
   *
   * @param type    The type of timeout
   * @param time_ms The timeout time in milliseconds
   *
   * @see ObTimeoutType
   */
  void set_timeout(ObTimeoutType type, int32_t time_ms);

  /**
   * Returns the ObMysqlSM * related to the current ObApiTransaction
   *
   * @return a void * which can be cast back to a ObMysqlSM *.
   */
  ObMysqlSM *get_sm() const { return sm_; }

  /**
   * Adds a ObTransactionPlugin to the current ObApiTransaction. This effectively
   * transfers ownership and the
   * ObApiTransaction is now responsible for cleaning it up.
   */
  void add_plugin(ObTransactionPlugin *plugin);

private:
  explicit ObApiTransaction(ObMysqlSM *sm);

private:
  DISALLOW_COPY_AND_ASSIGN(ObApiTransaction);
  ObMysqlSM *sm_;
  SList(ObTransactionPlugin, link_) plugins_;
  ObProxyMysqlRequest *client_request_; // for other packets
  ObProxyMysqlRequest *server_request_;
  ObMysqlResp *server_response_;
  ObMysqlResp *client_response_;
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_API_TRANSACTION_H
