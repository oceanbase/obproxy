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
#include "proxy/mysql/ob_mysql_proxy_server_main.h"
#include "proxy/api/ob_plugin.h"
#include "proxy/mysql/ob_mysql_sm.h"
#include "proxy/mysql/ob_mysql_session_accept.h"
#include "proxy/route/ob_table_cache.h"
#include "proxy/route/ob_partition_cache.h"
#include "proxy/route/ob_routine_cache.h"
#include "proxy/route/ob_sql_table_cache.h"
#include "iocore/eventsystem/ob_blocking_task.h"
#include "iocore/eventsystem/ob_grpc_task.h"
#include "iocore/eventsystem/ob_shard_watch_task.h"
#include "iocore/eventsystem/ob_shard_scan_all_task.h"
#include "obutils/ob_congestion_manager.h"
#include "obutils/ob_proxy_config.h"
#include "dbconfig/ob_proxy_db_config_processor.h"
#include "ob_proxy_init.h"
#include "opsql/func_expr_resolver/proxy_expr/ob_proxy_expr_factory.h"
//RPC
#include "obproxy/stat/ob_rpc_stats.h"
#include "proxy/rpc_optimize/net/ob_rpc_session_accept.h"
#include "proxy/rpc_optimize/net/ob_rpc_client_net_handler.h"
#include "proxy/rpc_optimize/net/ob_rpc_server_net_handler.h"
#include "proxy/route/ob_index_cache.h"
#include "proxy/rpc_optimize/rpclib/ob_table_query_async_cache.h"
#include "proxy/rpc_optimize/rpclib/ob_tablegroup_cache.h"
#include "proxy/rpc_optimize/rpclib/ob_rpc_req_ctx_cache.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::dbconfig;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

// global acceptor, ObMysqlProxyAccept
ObMysqlProxyAcceptor g_mysql_proxy_ipv4_acceptor;
ObMysqlProxyAcceptor g_mysql_proxy_ipv6_acceptor;
// global rpc acceptor, ObRpcProxyAcceptor (RPC service)
ObRpcProxyAcceptor g_rpc_proxy_ipv4_acceptor;
ObRpcProxyAcceptor g_rpc_proxy_ipv6_acceptor;

// called from ob_api.cpp
int ObMysqlProxyServerMain::make_net_accept_options(
    const ObMysqlConfigParams &config_params,
    const ObMysqlProxyPort &port,
    ObNetProcessor::ObAcceptOptions &accept_options)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(config_params.net_accept_threads_ < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(config_params.net_accept_threads_), K(ret));
  } else {
    ObNetProcessor::ObAcceptOptions net_opt;

    net_opt.accept_threads_ = config_params.net_accept_threads_;
    net_opt.frequent_accept_ = config_params.frequent_accept_;
    net_opt.ip_family_ = port.family_;
    net_opt.local_port_ = port.port_;
    net_opt.stacksize_ = config_params.stack_size_;
    net_opt.tcp_init_cwnd_ = config_params.server_tcp_init_cwnd_;
    net_opt.f_callback_on_open_ = true;
    // need to set 0, will be used in state_client_request_read
    net_opt.sockopt_flags_ = 0;
    if (port.inbound_ip_.is_valid()) {
      net_opt.local_ip_ = port.inbound_ip_;
    }

    accept_options = net_opt;
  }
  return ret;
}

int ObMysqlProxyServerMain::make_mysql_proxy_acceptor(
  const ObMysqlConfigParams &config_params,
  const ObMysqlProxyPort &port,
  ObMysqlProxyAcceptor &acceptor)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(make_net_accept_options(config_params, port, acceptor.net_opt_))) {
    LOG_EDIAG("fail to make_net_accept_options", K(ret));
  } else {
    ObMysqlSessionAccept *mysql_accept = new(std::nothrow) ObMysqlSessionAccept();
    if (OB_ISNULL(mysql_accept)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_EDIAG("fail to new ObMysqlSessionAccept", K(ret));
    } else {
      acceptor.accept_ = mysql_accept;
    }
  }
  return ret;
}

// set up all the accepts and sockets
int ObMysqlProxyServerMain::init_mysql_proxy_server(const ObMysqlConfigParams &config_params)
{
  int ret = OB_SUCCESS;

  //We don't have to care about whether or not the plugin initial success now
  plugin_init();

  // enable reclaim mysql sm
  op_reclaim_sparse_opt(ObMysqlSM, ObMysqlSM::instantiate_func, ENABLE_RECLAIM, 1);

  if (RUN_MODE_PROXY == g_run_mode && OB_FAIL(init_mysql_proxy_port(config_params))) {
    LOG_EDIAG("fail to init mysql proxy port", K(ret));
  } else if (OB_FAIL(init_mysql_stats())) {
    LOG_EDIAG("fail to init_mysql_stats", K(ret));
  } else if (OB_FAIL(mutex_init(&g_debug_sm_list_mutex))) {
    LOG_EDIAG("fail to init g_debug_sm_list_mutex", K(ret));
  } else if (OB_FAIL(ObProxyExprFactory::register_proxy_expr())) {
    LOG_EDIAG("fail to init proxy expr", K(ret));
  }

#ifdef USE_MYSQL_DEBUG_LISTS
  if (OB_SUCC(ret) && OB_FAIL(mutex_init(&g_debug_cs_list_mutex))) {
    LOG_EDIAG("fail to init g_debug_cs_list_mutex", K(ret));
  }
#endif

  int64_t ip_mode = config_params.ip_listen_mode_;
  bool enable_ipv4 = (ip_mode == 1 || ip_mode == 3);
  bool enable_ipv6 = (ip_mode == 2 || ip_mode == 3);

  if (OB_SUCC(ret) && enable_ipv4) {
    // do the configuration defined ports
    if (OB_FAIL(make_mysql_proxy_acceptor(config_params,
                                          get_global_proxy_ipv4_port(),
                                          g_mysql_proxy_ipv4_acceptor))) {
      LOG_EDIAG("fail to make mysql proxy acceptor", K(ret));
    }
  }

  if (OB_SUCC(ret) && enable_ipv6) {
    if (OB_FAIL(make_mysql_proxy_acceptor(config_params,
                                          get_global_proxy_ipv6_port(),
                                          g_mysql_proxy_ipv6_acceptor))) {
      LOG_EDIAG("fail to make mysql proxy acceptor", K(ret));
    }
  }
  return ret;
}

int ObMysqlProxyServerMain::start_mysql_proxy_acceptor()
{
  int ret = OB_SUCCESS;
  int64_t ip_mode = get_global_proxy_config().ip_listen_mode;
  bool enable_ipv4 = (ip_mode == 1 || ip_mode == 3);
  bool enable_ipv6 = (ip_mode == 2 || ip_mode == 3);
  // start accepting connections
  // although we make a good pretence here, I don't believe that ObNetProcessor::main_accept()
  // ever actually returns NULL. It would be useful to be able to detect errors
  // and spew them here though.
  ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
  info.port_state_ = OB_PROXY_PORT_SQL_SERVICE;
  if (enable_ipv4 && OB_ISNULL(g_net_processor.main_accept(*(g_mysql_proxy_ipv4_acceptor.accept_),
                                                           get_global_proxy_ipv4_port().fd_,
                                                           g_mysql_proxy_ipv4_acceptor.net_opt_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("fail to execute ipv4 main accept", K(ret));
  } else if (enable_ipv6 && OB_ISNULL(g_net_processor.main_accept(*(g_mysql_proxy_ipv6_acceptor.accept_),
                                                            get_global_proxy_ipv6_port().fd_,
                                                            g_mysql_proxy_ipv6_acceptor.net_opt_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("fail to execute ipv6 main accept", K(ret));
  }
  info.port_state_ = OB_PROXY_PORT_DEFAULT;

  return ret;
}

int ObMysqlProxyServerMain::start_mysql_proxy_server(const ObMysqlConfigParams &config_params)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(start_processor_threads(config_params))) {
    LOG_EDIAG("fail to start processor threads", K(ret));
  }
  return ret;
}

int ObMysqlProxyServerMain::start_processor_threads(const ObMysqlConfigParams &config_params)
{
  int ret = OB_SUCCESS;
  int64_t stack_size = config_params.stack_size_;
  int64_t event_threads = config_params.work_thread_num_;
  int64_t shard_scan_threads = config_params.shard_scan_thread_num_;
  int64_t task_threads = config_params.task_thread_num_;
  bool enable_cpu_topology = config_params.enable_cpu_topology_;
  bool automatic_match_work_thread = config_params.automatic_match_work_thread_;
  int64_t blocking_threads = config_params.block_thread_num_; //thread for blocking task
  int64_t grpc_threads = config_params.grpc_thread_num_;
  int64_t grpc_watch_threads = 1;
  bool enable_cpu_isolate = config_params.enable_cpu_isolate_;
  if (OB_UNLIKELY(stack_size <= 0) || OB_UNLIKELY(event_threads <= 0)
      || OB_UNLIKELY(task_threads <= 0)) {
    ret = OB_INVALID_CONFIG;
    LOG_WDIAG("invalid variable", K(stack_size), K(event_threads), K(task_threads), K(ret));
  } else if (OB_FAIL(g_event_processor.start(static_cast<int>(event_threads), stack_size,
                                             enable_cpu_topology, automatic_match_work_thread, enable_cpu_isolate))) {
    LOG_EDIAG("fail to start event processor", K(stack_size), K(event_threads), K(ret));
  } else if (OB_FAIL(g_net_processor.start())) {
    LOG_EDIAG("fail to start net processor", K(ret));
  } else if (OB_FAIL(g_task_processor.start(task_threads, stack_size))) {
    LOG_EDIAG("fail to start task processor", K(stack_size), K(ret));
  } else if (OB_FAIL(g_blocking_task_processor.start(blocking_threads, stack_size))) {
    LOG_EDIAG("fail to start blocking task processor", K(stack_size), K(ret));
  } else if (get_global_proxy_config().enable_sharding && !get_global_proxy_config().use_local_dbconfig
             && OB_FAIL(g_grpc_task_processor.start(grpc_threads, stack_size))) {
    // if use local config, no need start grpc threads
    LOG_EDIAG("fail to start grpc task processor", K(stack_size), K(ret));
  } else if (get_global_proxy_config().enable_sharding
      && OB_FAIL(g_shard_watch_task_processor.start(grpc_watch_threads, stack_size))) {
    LOG_EDIAG("fail to start grpc parent task processor", K(stack_size), K(ret));
  } else if (get_global_proxy_config().enable_sharding
      && OB_FAIL(g_shard_scan_all_task_processor.start(shard_scan_threads > 0 ? shard_scan_threads
                                                       : g_event_processor.thread_count_for_type_[ET_CALL] / 2,
                                                       stack_size))) {
    LOG_EDIAG("fail to start grpc parent task processor", K(stack_size), K(ret));
  } else if (OB_FAIL(init_cs_map_for_thread())) {
    LOG_EDIAG("fail to init cs_map for thread", K(ret));
  } else if (OB_FAIL(init_rpc_net_cs_map_for_thread())) {
    LOG_EDIAG("fail to init net_cs_map for thread", K(ret));
  } else if (OB_FAIL(init_rpc_net_ss_map_for_thread())) {
    LOG_EDIAG("fail to init net_ss_map for thread", K(ret));
  } else if (OB_FAIL(init_cs_id_list_for_thread())) {
    LOG_EDIAG("failt to init cs_id_list for thread", K(ret));
  } else if (OB_FAIL(init_table_map_for_thread())) {
    LOG_EDIAG("fail to init table_map for thread", K(ret));
  } else if (OB_FAIL(init_congestion_map_for_thread())) {
    LOG_EDIAG("fail to init congestion_map for thread", K(ret));
  } else if (OB_FAIL(init_partition_map_for_thread())) {
    LOG_EDIAG("fail to init partition_map for thread", K(ret));
  } else if (OB_FAIL(init_index_map_for_thread())) {
    LOG_EDIAG("fail to init index_map for thread", K(ret));
  } else if (OB_FAIL(init_tablegroup_map_for_thread())) {
    LOG_EDIAG("fail to init tablegroup for thread", K(ret));
  } else if (OB_FAIL(init_table_query_async_map_for_thread())) {
    LOG_EDIAG("fail to init table_query_async_map for thread", K(ret));
  } else if (OB_FAIL(init_rpc_req_ctx_map_for_thread())) {
    LOG_EDIAG("fail to init rpc_req_ctx_map for thread", K(ret));
  } else if (OB_FAIL(init_routine_map_for_thread())) {
    LOG_EDIAG("fail to init routine_map for thread", K(ret));
  } else if (OB_FAIL(init_sql_table_map_for_thread())) {
    LOG_EDIAG("fail to init sql_table_map for thread", K(ret));
  } else if (OB_FAIL(init_ps_entry_cache_for_thread())) {
    LOG_EDIAG("fail to init ps entry cache for thread", K(ret));
  } else if (OB_FAIL(init_text_ps_entry_cache_for_thread())) {
    LOG_EDIAG("fail to init text ps entry cache for thread", K(ret));
  } else if (OB_FAIL(init_random_seed_for_thread())) {
    LOG_EDIAG("fail to init random seed for thread", K(ret));
  } else {}
  return ret;
}

int ObMysqlProxyServerMain::init_inherited_info(ObMysqlProxyPort &proxy_port, const int fd)
{
  int ret = OB_SUCCESS;
  proxy_port.fd_ = fd;
  struct sockaddr_storage sock_addr;
  int64_t namelen = sizeof(sock_addr);
  memset(&sock_addr, 0, namelen);
  if (OB_FAIL(ObSocketManager::getsockname(proxy_port.fd_, (struct sockaddr*)(&sock_addr), &namelen))) {
    LOG_EDIAG("fail to get sock name", K(ret));
  } else {
    // This step of conversion is mainly to obtain the port number, sockaddr_in and sockaddr_in6 port number positions are compatible
    struct sockaddr_in *ain = (sockaddr_in *)&sock_addr;
    proxy_port.inbound_ip_.assign(*(struct sockaddr*)(&sock_addr));
    proxy_port.port_ = static_cast<in_port_t>((ntohs)(ain->sin_port));
    LOG_INFO("succ init mysql proxy port by inherited fd", K(proxy_port));
  }
  return ret;
}

int ObMysqlProxyServerMain::init_mysql_proxy_port(const ObMysqlConfigParams &config_params)
{
  int ret = OB_SUCCESS;
  ObMysqlProxyPort &proxy_ipv4_port = get_global_proxy_ipv4_port();
  ObMysqlProxyPort &proxy_ipv6_port = get_global_proxy_ipv6_port();
  const ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
  int64_t ip_mode = config_params.ip_listen_mode_;
  bool enable_ipv4 = (ip_mode == 1 || ip_mode == 3);
  bool enable_ipv6 = (ip_mode == 2 || ip_mode == 3);

  // init from inherited fd
  if (info.is_inherited_) {
    if (enable_ipv4) {
      if (OB_FAIL(init_inherited_info(proxy_ipv4_port, info.ipv4_fd_))) {
        LOG_WDIAG("fail to init inherited info for proxy ipv4 port", K(ret));
      }
    }

    if (OB_SUCC(ret) && enable_ipv6) {
      if (OB_FAIL(init_inherited_info(proxy_ipv6_port, info.ipv6_fd_))) {
        LOG_WDIAG("fail to init inherited info for proxy ipv4 port", K(ret));
      }
    }
  } else { // init from config
    if (enable_ipv4) {
      proxy_ipv4_port.port_ = static_cast<in_port_t>(config_params.listen_port_);
      proxy_ipv4_port.inbound_ip_ = config_params.local_bound_ip_;
      LOG_INFO("succ init mysql proxy ipv4 port by config", K(proxy_ipv4_port));
    }

    if (enable_ipv6) {
      proxy_ipv6_port.port_ = static_cast<in_port_t>(config_params.listen_port_);
      proxy_ipv6_port.inbound_ip_ = config_params.local_bound_ipv6_ip_;
      LOG_INFO("succ init mysql proxy ipv6 port by config", K(proxy_ipv6_port));
    }
  }
  return ret;
}

int ObMysqlProxyServerMain::make_rpc_proxy_acceptor(
  const ObMysqlConfigParams &config_params,
  const ObMysqlProxyPort &port,
  ObRpcProxyAcceptor &acceptor)
{
  int ret = OB_SUCCESS;
  // init rpc port info
  if (OB_FAIL(make_net_accept_options(config_params, port, acceptor.net_opt_))) {
    LOG_EDIAG("fail to make_net_accept_options", K(ret));
  } else {
    // /*
    ObRpcSessionAccept *rpc_accept = new(std::nothrow) ObRpcSessionAccept();
    if (OB_ISNULL(rpc_accept)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_EDIAG("fail to new ObMysqlSessionAccept", K(ret));
    } else {
      acceptor.accept_ = rpc_accept;
    }
    // */
  }
  return ret;
}

// set up all the accepts and sockets
int ObMysqlProxyServerMain::init_rpc_proxy_server(const ObMysqlConfigParams &config_params)
{
  int ret = OB_SUCCESS;
  UNUSED(config_params);

  //We don't have to care about whether or not the plugin initial success now
  //plugin_init(); //TODO, ZDW， not do anything

  if (OB_FAIL(init_rpc_proxy_port(config_params))) {
    LOG_EDIAG("fail to init rpc proxy port", K(ret));
  } else if (OB_FAIL(init_rpc_stats())) {
    LOG_EDIAG("fail to init_rpc_stats", K(ret));
  }

#ifdef USE_RPC_DEBUG_LISTS
  if (OB_SUCC(ret) && OB_FAIL(mutex_init(&g_debug_rpc_cs_list_mutex))) {
    LOG_EDIAG("fail to init g_debug_rpc_cs_list_mutex", K(ret));
  }
#endif

  if (OB_SUCC(ret)) {
    // do the configuration defined ports
    int64_t ip_mode = config_params.ip_listen_mode_;
    bool enable_ipv4 = (ip_mode == 1 || ip_mode == 3);
    bool enable_ipv6 = (ip_mode == 2 || ip_mode == 3);

    if (OB_SUCC(ret) && enable_ipv4) {
      // do the configuration defined ports
      if (OB_FAIL(make_rpc_proxy_acceptor(config_params,
                                            get_global_rpc_proxy_ipv4_port(),
                                            g_rpc_proxy_ipv4_acceptor))) {
        LOG_EDIAG("fail to make rpc ipv4 proxy acceptor", K(ret));
      }
    }

    if (OB_SUCC(ret) && enable_ipv6) {
      if (OB_FAIL(make_rpc_proxy_acceptor(config_params,
                                            get_global_rpc_proxy_ipv6_port(),
                                            g_rpc_proxy_ipv6_acceptor))) {
        LOG_EDIAG("fail to make rpc ipv6 proxy acceptor", K(ret));
      }
    }
  }
  //*/
  return ret;
}

int ObMysqlProxyServerMain::init_rpc_proxy_port(const ObMysqlConfigParams &config_params)
{
  int ret = OB_SUCCESS;
  ObMysqlProxyPort &rpc_proxy_ipv4_port = get_global_rpc_proxy_ipv4_port();
  ObMysqlProxyPort &rpc_proxy_ipv6_port = get_global_rpc_proxy_ipv6_port();
  const ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
  int64_t ip_mode = config_params.ip_listen_mode_;
  bool enable_ipv4 = (ip_mode == 1 || ip_mode == 3);
  bool enable_ipv6 = (ip_mode == 2 || ip_mode == 3);
  bool rpc_is_inherited_ = (info.is_inherited_
    && (OB_INVALID_INDEX != info.rpc_ipv4_fd_ || OB_INVALID_INDEX != info.rpc_ipv6_fd_));

  // init from inherited fd
  if (rpc_is_inherited_) {
    if (enable_ipv4) {
      if (OB_FAIL(init_inherited_info(rpc_proxy_ipv4_port, info.rpc_ipv4_fd_))) {
        LOG_WDIAG("fail to init inherited info for rpc proxy ipv4 port", K(ret));
      }
    }

    if (OB_SUCC(ret) && enable_ipv6) {
      if (OB_FAIL(init_inherited_info(rpc_proxy_ipv6_port, info.rpc_ipv6_fd_))) {
        LOG_WDIAG("fail to init inherited info for proxy ipv6 port", K(ret));
      }
    }
  } else { // init from config
    if (enable_ipv4) {
      rpc_proxy_ipv4_port.port_ = static_cast<in_port_t>(config_params.rpc_listen_port_);
      rpc_proxy_ipv4_port.inbound_ip_ = config_params.local_bound_ip_;
      LOG_INFO("succ init rpc proxy ipv4 port by config", K(rpc_proxy_ipv4_port));
    }

    if (enable_ipv6) {
      rpc_proxy_ipv6_port.port_ = static_cast<in_port_t>(config_params.rpc_listen_port_);
      rpc_proxy_ipv6_port.inbound_ip_ = config_params.local_bound_ipv6_ip_;
      LOG_INFO("succ init rpc proxy ipv6 port by config", K(rpc_proxy_ipv6_port));
    }
  }
  return ret;
}

//TODO need check remove this function
int ObMysqlProxyServerMain::start_rpc_proxy_server(const ObMysqlConfigParams &config_params)
{
  int ret = OB_SUCCESS;
  UNUSED(config_params);

  return ret;
}

int ObMysqlProxyServerMain::start_rpc_proxy_acceptor()
{
  int ret = OB_SUCCESS;
  int64_t ip_mode = get_global_proxy_config().ip_listen_mode;
  bool enable_ipv4 = (ip_mode == 1 || ip_mode == 3);
  bool enable_ipv6 = (ip_mode == 2 || ip_mode == 3);
  // start accepting connections
  // although we make a good pretence here, I don't believe that ObNetProcessor::main_accept()
  // ever actually returns NULL. It would be useful to be able to detect errors
  // and spew them here though.
  ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
  info.port_state_ = OB_PROXY_PORT_RPC_SERVICE;
  if (get_global_proxy_config().enable_obproxy_rpc_service) {
    if (enable_ipv4 && OB_ISNULL(g_net_processor.main_accept(*(g_rpc_proxy_ipv4_acceptor.accept_),
                                                             get_global_rpc_proxy_ipv4_port().fd_,
                                                             g_rpc_proxy_ipv4_acceptor.net_opt_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("fail to execute rpc ipv4 main accept", K(ret));
    } else if (enable_ipv6 && OB_ISNULL(g_net_processor.main_accept(*(g_rpc_proxy_ipv6_acceptor.accept_),
                                                              get_global_rpc_proxy_ipv6_port().fd_,
                                                              g_rpc_proxy_ipv6_acceptor.net_opt_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("fail to execute rpc ipv6 main accept", K(ret));
    }
  } else if (info.is_inherited_ && (info.rpc_ipv4_fd_ != OB_INVALID_INDEX || info.rpc_ipv6_fd_ != OB_INVALID_INDEX)) {
    int tmp_ret = ret; //close error not to impact startup for obproxy process.
    if (OB_INVALID_INDEX != info.rpc_ipv4_fd_ && OB_FAIL(close_listen_fd(info.rpc_ipv4_fd_))) {
      LOG_EDIAG("fail to close listen rpc v4 fd", K(ret));
    }
    if (OB_INVALID_INDEX != info.rpc_ipv6_fd_ && OB_FAIL(close_listen_fd(info.rpc_ipv6_fd_))) {
      LOG_EDIAG("fail to close listen rpc v6 fd", K(ret));
    }
    info.rpc_ipv4_fd_ = OB_INVALID_INDEX;
    info.rpc_ipv6_fd_ = OB_INVALID_INDEX;
    ret = tmp_ret;
  }
  info.port_state_ = OB_PROXY_PORT_DEFAULT;
  return ret;
}

int ObMysqlProxyServerMain::close_listen_fd(const int32_t listen_fd)
{
  int ret = OB_SUCCESS;
  int fd = -1;
  pid_t pid = getpid();
  DIR *fd_dir = NULL;
  char fd_dir_path [OB_MAX_FILE_NAME_LENGTH];

  if (OB_UNLIKELY(listen_fd < 3)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int n = snprintf(fd_dir_path, OB_MAX_FILE_NAME_LENGTH, "/proc/%d/fd", pid);
    if (OB_UNLIKELY(n < 0) || OB_UNLIKELY(n >= OB_MAX_FILE_NAME_LENGTH)) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      fd_dir = opendir(fd_dir_path);
      if (OB_ISNULL(fd_dir)) {
        ret = OB_FILE_NOT_OPENED;
      } else {
        struct dirent *de = NULL;
        while (OB_SUCC(ret) && (NULL != (de = readdir(fd_dir)))) {
          errno = 0;
          if ('.' == de->d_name[0]) {
            continue;
          } else {
            fd = static_cast<int32_t>(strtol(de->d_name, NULL, 10));
            if (fd == listen_fd) {
              if (OB_UNLIKELY(0 != close(fd))) {
                // do nothing
              }
            }
          }
        }
        if (OB_UNLIKELY(0 != closedir(fd_dir))) {
          // do nothing
        }
      }
    }
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
