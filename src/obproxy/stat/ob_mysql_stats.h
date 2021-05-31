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

#ifndef OBPROXY_MYSQL_STATS_H
#define OBPROXY_MYSQL_STATS_H

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
#define MYSQL_SSN_TXN_MAX_USER_ARG         16

// Instead of enumerating the stats in a common file, each module
// needs to enumerate its stats separately and register them
enum ObMysqlStats
{
  TOTAL_TRANSACTION_COUNT = 0,
  TOTAL_USER_TRANSACTION_COUNT,
  TOTAL_QUERY_COUNT,

  TOTAL_CLIENT_REQUEST_REREAD_COUNT,
  TOTAL_SERVER_RESPONSE_REREAD_COUNT,

  // size stats
  CLIENT_REQUEST_TOTAL_SIZE,
  CLIENT_RESPONSE_TOTAL_SIZE,
  SERVER_REQUEST_TOTAL_SIZE,
  SERVER_RESPONSE_TOTAL_SIZE,

  // time stats
  TOTAL_TRANSACTIONS_TIME,
  TOTAL_USER_TRANSACTIONS_TIME,
  TOTAL_CLIENT_REQUEST_READ_TIME,
  TOTAL_CLIENT_RESPONSE_WRITE_TIME,
  TOTAL_CLIENT_REQUEST_ANALYZE_TIME,
  TOTAL_CLIENT_TRANSACTION_IDLE_TIME,

  TOTAL_OK_PACKET_TRIM_TIME,

  TOTAL_SERVER_PROCESS_REQUEST_TIME,
  TOTAL_SERVER_RESPONSE_READ_TIME,
  TOTAL_SERVER_RESPONSE_ANALYZE_TIME,
  TOTAL_SEND_SAVED_LOGIN_TIME,
  TOTAL_SEND_ALL_SESSION_VARS_TIME,
  TOTAL_SEND_USE_DATABASE_TIME,
  TOTAL_SEND_CHANGED_SESSION_VARS_TIME,
  TOTAL_SEND_LAST_INSERT_ID_TIME,
  TOTAL_SEND_START_TRANS_TIME,

  TOTAL_PL_LOOKUP_TIME,
  TOTAL_CONGESTION_CONTROL_LOOKUP_TIME,
  TOTAL_SERVER_CONNECT_TIME,

  // Transactiona stats
  CLIENT_REQUESTS,
  CLIENT_LARGE_REQUESTS,
  CLIENT_INTERNAL_REQUESTS,
  // the request proxy will use local session state and responce packet directly
  // e.g. select @@tx_read_only
  CLIENT_USE_LOCAL_SESSION_STATE_REQUESTS,
  CLIENT_MISSING_PK_REQUESTS,
  CLIENT_COMPLETED_REQUESTS,
  CLIENT_CONNECTION_ABORT_COUNT,
  CLIENT_SELECT_REQUESTS,
  CLIENT_INSERT_REQUESTS,
  CLIENT_UPDATE_REQUESTS,
  CLIENT_DELETE_REQUESTS,
  CLIENT_OTHER_REQUESTS,

  REQUEST_SIZE_100_COUNT,
  REQUEST_SIZE_1K_COUNT,
  REQUEST_SIZE_3K_COUNT,
  REQUEST_SIZE_5K_COUNT,
  REQUEST_SIZE_10K_COUNT,
  REQUEST_SIZE_1M_COUNT,
  REQUEST_SIZE_INF_COUNT,

  RESPONSE_SIZE_100_COUNT,
  RESPONSE_SIZE_1K_COUNT,
  RESPONSE_SIZE_3K_COUNT,
  RESPONSE_SIZE_5K_COUNT,
  RESPONSE_SIZE_10K_COUNT,
  RESPONSE_SIZE_1M_COUNT,
  RESPONSE_SIZE_INF_COUNT,

  // connection speed stats
  CLIENT_SPEED_BYTES_PER_SEC_100,
  CLIENT_SPEED_BYTES_PER_SEC_1K,
  CLIENT_SPEED_BYTES_PER_SEC_10K,
  CLIENT_SPEED_BYTES_PER_SEC_100K,
  CLIENT_SPEED_BYTES_PER_SEC_1M,
  CLIENT_SPEED_BYTES_PER_SEC_10M,
  CLIENT_SPEED_BYTES_PER_SEC_100M,

  SERVER_SPEED_BYTES_PER_SEC_100,
  SERVER_SPEED_BYTES_PER_SEC_1K,
  SERVER_SPEED_BYTES_PER_SEC_10K,
  SERVER_SPEED_BYTES_PER_SEC_100K,
  SERVER_SPEED_BYTES_PER_SEC_1M,
  SERVER_SPEED_BYTES_PER_SEC_10M,
  SERVER_SPEED_BYTES_PER_SEC_100M,

  SERVER_CONNECT_COUNT,
  SERVER_CONNECT_RETRIES,
  SERVER_PL_LOOKUP_COUNT,
  SERVER_PL_LOOKUP_RETRIES,
  BROKEN_SERVER_CONNECTIONS,

  SERVER_REQUESTS,
  SERVER_RESPONSES,
  SERVER_ERROR_RESPONSES,
  SERVER_RESULTSET_RESPONSES,
  SERVER_OK_RESPONSES,
  SERVER_OTHER_RESPONSES,
  SEND_SAVED_LOGIN_REQUESTS,
  SEND_ALL_SESSION_VARS_REQUESTS,
  SEND_USE_DATABASE_REQUESTS,
  SEND_CHANGED_SESSION_VARS_REQUESTS,
  SEND_LAST_INSERT_ID_REQUESTS,
  SEND_START_TRANS_REQUESTS,

  VIP_TO_TENANT_CACHE_HIT,
  VIP_TO_TENANT_CACHE_MISS,

  SESSION_STAT_COUNT, // max session stat count

  // connnection stats
  CURRENT_CLIENT_CONNECTIONS,
  CURRENT_ACTIVE_CLIENT_CONNECTIONS,
  CURRENT_CLIENT_TRANSACTIONS,
  CURRENT_SERVER_TRANSACTIONS,

  DUMMY_ENTRY_EXPIRED_COUNT,

  // Mysql Total Connections Stats
  //
  // it is assumed that this inequality will always be satisfied:
  //   total_client_connections_stat >=
  //     total_client_connections_ipv4_stat +
  //     total_client_connections_ipv6_stat
  TOTAL_CLIENT_CONNECTIONS,
  TOTAL_INTERNAL_CLIENT_CONNECTIONS,
  TOTAL_CLIENT_CONNECTIONS_IPV4,
  TOTAL_CLIENT_CONNECTIONS_IPV6,
  TOTAL_SERVER_CONNECTIONS,
  CURRENT_SERVER_CONNECTIONS, // global

  // Mysql K-A Stats
  TRANSACTIONS_PER_CLIENT_CON,
  TRANSACTIONS_PER_SERVER_CON,

  MYSQL_STAT_COUNT
};

extern ObRecRawStatBlock *mysql_rsb;
extern const char *g_mysql_stat_name[];

#define MYSQL_INCREMENT_DYN_STAT(x)     \
  (void)ObStatProcessor::incr_raw_stat_sum(mysql_rsb, mutex_->thread_holding_, x, 1)
#define MYSQL_DECREMENT_DYN_STAT(x)     \
  (void)ObStatProcessor::incr_raw_stat_sum(mysql_rsb, mutex_->thread_holding_, x, -1)
#define MYSQL_SUM_DYN_STAT(x, y)        \
  (void)ObStatProcessor::incr_raw_stat(mysql_rsb, mutex_->thread_holding_, x, y)
#define MYSQL_SUM_GLOBAL_DYN_STAT(x, y) \
  (void)ObStatProcessor::incr_global_raw_stat_sum(mysql_rsb, x, y)

#define MYSQL_READ_DYN_SUM(x, S)        \
  ObStatProcessor::get_raw_stat_sum(mysql_rsb, x, S) // This aggregates threads too
#define MYSQL_READ_GLOBAL_DYN_SUM(x, S) \
  ObStatProcessor::get_global_raw_stat_sum(mysql_rsb, x, S)

int init_mysql_stats();

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_MYSQL_STATS_H
