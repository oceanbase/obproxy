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

#ifndef OBPROXY_RPC_STATS_H
#define OBPROXY_RPC_STATS_H

#include "iocore/eventsystem/ob_event_system.h"
#include "stat/ob_stat_processor.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
// some defines that might be candidates for configurable settings later.
// max number of user arguments for transactions and sessions
#define RPC_SSN_TXN_MAX_USER_ARG         16

// Instead of enumerating the stats in a common file, each module
// needs to enumerate its stats separately and register them
enum ObRpcStats
{
  RPC_TOTAL_TRANSACTION_COUNT = 0,
  RPC_TOTAL_USER_TRANSACTION_COUNT,
  RPC_TOTAL_QUERY_COUNT,

  RPC_TOTAL_CLIENT_REQUEST_REREAD_COUNT,
  RPC_TOTAL_SERVER_RESPONSE_REREAD_COUNT,

  // size stats
  RPC_CLIENT_REQUEST_TOTAL_SIZE,
  RPC_CLIENT_RESPONSE_TOTAL_SIZE,
  RPC_SERVER_REQUEST_TOTAL_SIZE,
  RPC_SERVER_RESPONSE_TOTAL_SIZE,

  // time stats
  RPC_TOTAL_TRANSACTIONS_TIME,
  RPC_TOTAL_USER_TRANSACTIONS_TIME,
  RPC_TOTAL_CLIENT_REQUEST_READ_TIME,
  RPC_TOTAL_CLIENT_RESPONSE_WRITE_TIME,
  RPC_TOTAL_CLIENT_REQUEST_ANALYZE_TIME,
  RPC_TOTAL_CLIENT_TRANSACTION_IDLE_TIME,

  RPC_TOTAL_OK_PACKET_TRIM_TIME,

  RPC_TOTAL_SERVER_PROCESS_REQUEST_TIME,
  RPC_TOTAL_SERVER_RESPONSE_READ_TIME,
  RPC_TOTAL_SERVER_RESPONSE_ANALYZE_TIME,
  RPC_TOTAL_SEND_SAVED_LOGIN_TIME,
//  RPC_TOTAL_SEND_ALL_SESSION_VARS_TIME,
//  RPC_TOTAL_SEND_USE_DATABASE_TIME,
//  RPC_TOTAL_SEND_CHANGED_SESSION_VARS_TIME,
//  RPC_TOTAL_SEND_LAST_INSERT_ID_TIME,
//  RPC_TOTAL_SEND_START_TRANS_TIME,

  RPC_TOTAL_PL_LOOKUP_TIME,
  RPC_TOTAL_CONGESTION_CONTROL_LOOKUP_TIME,
  RPC_TOTAL_SERVER_CONNECT_TIME,

  // Transactiona stats
  RPC_CLIENT_REQUESTS,
  RPC_CLIENT_LARGE_REQUESTS,
  RPC_CLIENT_INTERNAL_REQUESTS,
  // the request proxy will use local session state and responce packet directly
  // e.g. select @@tx_read_only
  // RPC_CLIENT_USE_LOCAL_SESSION_STATE_REQUESTS,
  RPC_CLIENT_MISSING_PK_REQUESTS,
  RPC_CLIENT_COMPLETED_REQUESTS,
  RPC_CLIENT_CONNECTION_ABORT_COUNT,
  RPC_CLIENT_SELECT_REQUESTS,
  RPC_CLIENT_INSERT_REQUESTS,
  RPC_CLIENT_UPDATE_REQUESTS,
  RPC_CLIENT_DELETE_REQUESTS,
  RPC_CLIENT_OTHER_REQUESTS,

  RPC_REQUEST_SIZE_100_COUNT,
  RPC_REQUEST_SIZE_1K_COUNT,
  RPC_REQUEST_SIZE_3K_COUNT,
  RPC_REQUEST_SIZE_5K_COUNT,
  RPC_REQUEST_SIZE_10K_COUNT,
  RPC_REQUEST_SIZE_1M_COUNT,
  RPC_REQUEST_SIZE_INF_COUNT,

  RPC_RESPONSE_SIZE_100_COUNT,
  RPC_RESPONSE_SIZE_1K_COUNT,
  RPC_RESPONSE_SIZE_3K_COUNT,
  RPC_RESPONSE_SIZE_5K_COUNT,
  RPC_RESPONSE_SIZE_10K_COUNT,
  RPC_RESPONSE_SIZE_1M_COUNT,
  RPC_RESPONSE_SIZE_INF_COUNT,

  // connection speed stats
  RPC_CLIENT_SPEED_BYTES_PER_SEC_100,
  RPC_CLIENT_SPEED_BYTES_PER_SEC_1K,
  RPC_CLIENT_SPEED_BYTES_PER_SEC_10K,
  RPC_CLIENT_SPEED_BYTES_PER_SEC_100K,
  RPC_CLIENT_SPEED_BYTES_PER_SEC_1M,
  RPC_CLIENT_SPEED_BYTES_PER_SEC_10M,
  RPC_CLIENT_SPEED_BYTES_PER_SEC_100M,

  RPC_SERVER_SPEED_BYTES_PER_SEC_100,
  RPC_SERVER_SPEED_BYTES_PER_SEC_1K,
  RPC_SERVER_SPEED_BYTES_PER_SEC_10K,
  RPC_SERVER_SPEED_BYTES_PER_SEC_100K,
  RPC_SERVER_SPEED_BYTES_PER_SEC_1M,
  RPC_SERVER_SPEED_BYTES_PER_SEC_10M,
  RPC_SERVER_SPEED_BYTES_PER_SEC_100M,

  RPC_SERVER_CONNECT_COUNT,
  RPC_SERVER_CONNECT_RETRIES,
  RPC_SERVER_PL_LOOKUP_COUNT,
  RPC_SERVER_PL_LOOKUP_RETRIES,
  RPC_BROKEN_SERVER_CONNECTIONS,

  RPC_SERVER_REQUESTS,
  RPC_SERVER_RESPONSES,
  RPC_SERVER_ERROR_RESPONSES,
  RPC_SERVER_RESULTSET_RESPONSES,
  RPC_SERVER_OK_RESPONSES,
  RPC_SERVER_OTHER_RESPONSES,
  RPC_SEND_SAVED_LOGIN_REQUESTS,
  // RPC_SEND_ALL_SESSION_VARS_REQUESTS,
  // RPC_SEND_USE_DATABASE_REQUESTS,
  // RPC_SEND_CHANGED_SESSION_VARS_REQUESTS,
  // RPC_SEND_LAST_INSERT_ID_REQUESTS,
  // RPC_SEND_START_TRANS_REQUESTS,

  RPC_VIP_TO_TENANT_CACHE_HIT,
  RPC_VIP_TO_TENANT_CACHE_MISS,

  RPC_SESSION_STAT_COUNT, // max session stat count

  // connnection stats
  RPC_CURRENT_CLIENT_CONNECTIONS,
  RPC_CURRENT_ACTIVE_CLIENT_CONNECTIONS,
  RPC_CURRENT_CLIENT_TRANSACTIONS,
  RPC_CURRENT_SERVER_TRANSACTIONS,

  RPC_DUMMY_ENTRY_EXPIRED_COUNT,

  // Mysql Total Connections Stats
  //
  // it is assumed that this inequality will always be satisfied:
  //   total_client_connections_stat >=
  //     total_client_connections_ipv4_stat +
  //     total_client_connections_ipv6_stat
  RPC_TOTAL_CLIENT_CONNECTIONS,
  RPC_TOTAL_INTERNAL_CLIENT_CONNECTIONS,
  RPC_TOTAL_CLIENT_CONNECTIONS_IPV4,
  RPC_TOTAL_CLIENT_CONNECTIONS_IPV6,
  RPC_TOTAL_SERVER_CONNECTIONS,
  RPC_CURRENT_SERVER_CONNECTIONS, // global

  // Mysql K-A Stats
  RPC_TRANSACTIONS_PER_CLIENT_CON,
  RPC_TRANSACTIONS_PER_SERVER_CON,

  RPC_STAT_COUNT
};

extern ObRecRawStatBlock *rpc_rsb;
extern const char *g_rpc_stat_name[];

#define RPC_INCREMENT_DYN_STAT(x)     \
  (void)ObStatProcessor::incr_raw_stat_sum(rpc_rsb, mutex_->thread_holding_, x, 1)
#define RPC_DECREMENT_DYN_STAT(x)     \
  (void)ObStatProcessor::incr_raw_stat_sum(rpc_rsb, mutex_->thread_holding_, x, -1)
#define RPC_SUM_DYN_STAT(x, y)        \
  (void)ObStatProcessor::incr_raw_stat(rpc_rsb, mutex_->thread_holding_, x, y)
#define RPC_SUM_GLOBAL_DYN_STAT(x, y) \
  (void)ObStatProcessor::incr_global_raw_stat_sum(rpc_rsb, x, y)

#define RPC_READ_DYN_SUM(x, S)        \
  ObStatProcessor::get_raw_stat_sum(rpc_rsb, x, S) // This aggregates threads too
#define RPC_READ_GLOBAL_DYN_SUM(x, S) \
  ObStatProcessor::get_global_raw_stat_sum(rpc_rsb, x, S)

//TODO RPC need add it next like init_mysql_stats
int init_rpc_stats();
//int init_rpc_stats() { return 0; }

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_RPC_STATS_H
