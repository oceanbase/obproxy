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

#ifndef OBPROXY_MYSQL_PROXY_SERVER_MAIN_H
#define OBPROXY_MYSQL_PROXY_SERVER_MAIN_H

#include "iocore/net/ob_net_processor.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObMysqlProxy;
}
namespace event
{
class ObContinuation;
}
namespace proxy
{
struct ObMysqlProxyPort;
class ObMysqlConfigParams;

// data about an acceptor
// this is used to separate setting up the proxy ports and
// starting to accept on them
struct ObMysqlProxyAcceptor
{
  // default constructor.
  ObMysqlProxyAcceptor() : accept_(NULL) {}
  ~ObMysqlProxyAcceptor() { accept_ = NULL; }

  // accept continuation.
  event::ObContinuation* accept_;
  // options for ObNetProcessor.
  net::ObNetProcessor::ObAcceptOptions net_opt_;
};

// data about an acceptor
// this is used to separate setting up the rpc proxy ports and
// starting to accept on them
struct ObRpcProxyAcceptor
{
  // default constructor.
  ObRpcProxyAcceptor() : accept_(NULL) {}
  ~ObRpcProxyAcceptor() { accept_ = NULL; }

  // accept continuation.
  event::ObContinuation* accept_;
  // options for ObNetProcessor.
  net::ObNetProcessor::ObAcceptOptions net_opt_;
};

class ObMysqlProxyServerMain
{
public:
  // initialize all obproxy port data structures needed to run
  static int init_mysql_proxy_server(const ObMysqlConfigParams &config_params);

  // start the proxy server
  // the port data should have been created by
  // init_mysql_proxy_server()
  static int start_mysql_proxy_server(const ObMysqlConfigParams &config_params);
  static int start_mysql_proxy_acceptor();


  // initialize rpc obproxy port data structures needed to run
  static int init_rpc_proxy_server(const ObMysqlConfigParams &config_params);
  static int start_rpc_proxy_server(const ObMysqlConfigParams &config_params);
  static int start_rpc_proxy_acceptor();
  static int close_listen_fd(const int32_t listen_fd);

private:
  static int make_net_accept_options(
      const ObMysqlConfigParams &config_params,
      const ObMysqlProxyPort &port,
      net::ObNetProcessor::ObAcceptOptions &accept_options);
  static int make_mysql_proxy_acceptor(const ObMysqlConfigParams &config_params,
                                       const ObMysqlProxyPort &port,
                                       ObMysqlProxyAcceptor &acceptor);
  static int start_processor_threads(const ObMysqlConfigParams &config_params);

  static int init_mysql_proxy_port(const ObMysqlConfigParams &config_params);
  static int init_inherited_info(ObMysqlProxyPort &proxy_port, const int fd);

  /* RPC Service */
  static int make_rpc_proxy_acceptor(const ObMysqlConfigParams &config_params,
                                       const ObMysqlProxyPort &port,
                                       ObRpcProxyAcceptor &acceptor);

  static int init_rpc_proxy_port(const ObMysqlConfigParams &config_params);
  /* END RPC Service */
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_MYSQL_PROXY_SERVER_MAIN_H
