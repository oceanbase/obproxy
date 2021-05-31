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

#include "stat/ob_mysql_stats.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

#define MYSQL_REGISTER_RAW_STAT(rsb, rec_type, name, data_type, id, sync_type, persist_type)  \
  if (OB_SUCC(ret)) { \
    ret = g_stat_processor.register_raw_stat(rsb, rec_type, name, data_type, id, sync_type, persist_type); \
    g_mysql_stat_name[id] = name; \
  }

ObRecRawStatBlock *mysql_rsb;
const char *g_mysql_stat_name[MYSQL_STAT_COUNT];

int init_mysql_stats()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(mysql_rsb = g_stat_processor.allocate_raw_stat_block(MYSQL_STAT_COUNT, XFH_MYSQL_STATE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_LOG(WARN, "fail to alloc mem for mysql_rsb", K(ret));
  } else {
    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "total_transaction_count",
                            RECD_INT, TOTAL_TRANSACTION_COUNT, SYNC_SUM, RECP_PERSISTENT);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "total_user_transaction_count",
                            RECD_INT, TOTAL_USER_TRANSACTION_COUNT, SYNC_SUM, RECP_PERSISTENT);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "total_query_count",
                            RECD_INT, TOTAL_QUERY_COUNT, SYNC_SUM, RECP_PERSISTENT);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "total_client_request_reread_count",
                            RECD_INT, TOTAL_CLIENT_REQUEST_REREAD_COUNT, SYNC_SUM, RECP_PERSISTENT);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "total_server_response_reread_count",
                            RECD_INT, TOTAL_SERVER_RESPONSE_REREAD_COUNT, SYNC_SUM, RECP_PERSISTENT);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "current_client_connections",
                            RECD_INT, CURRENT_CLIENT_CONNECTIONS, SYNC_SUM, RECP_PERSISTENT);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "current_active_client_connections",
                            RECD_INT, CURRENT_ACTIVE_CLIENT_CONNECTIONS, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "current_client_transactions",
                            RECD_INT, CURRENT_CLIENT_TRANSACTIONS, SYNC_SUM, RECP_PERSISTENT);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "current_server_transactions",
                            RECD_INT, CURRENT_SERVER_TRANSACTIONS, SYNC_SUM, RECP_PERSISTENT);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "dummy_entry_expired_count",
                            RECD_INT, DUMMY_ENTRY_EXPIRED_COUNT, SYNC_SUM, RECP_PERSISTENT);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "total_client_connections",
                            RECD_INT, TOTAL_CLIENT_CONNECTIONS, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "total_internal_client_connections",
                            RECD_INT, TOTAL_INTERNAL_CLIENT_CONNECTIONS, SYNC_SUM, RECP_PERSISTENT);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "total_client_connections_ipv4",
                            RECD_INT, TOTAL_CLIENT_CONNECTIONS_IPV4, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "total_client_connections_ipv6",
                            RECD_INT, TOTAL_CLIENT_CONNECTIONS_IPV6, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "total_server_connections",
                            RECD_INT, TOTAL_SERVER_CONNECTIONS, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "current_server_connections",
                            RECD_INT, CURRENT_SERVER_CONNECTIONS, SYNC_SUM, RECP_PERSISTENT);

    // cache stats
    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "vip_to_tenant_cache_hit",
                            RECD_INT, VIP_TO_TENANT_CACHE_HIT, SYNC_SUM, RECP_PERSISTENT);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "vip_to_tenant_cache_miss",
                            RECD_INT, VIP_TO_TENANT_CACHE_MISS, SYNC_SUM, RECP_PERSISTENT);

    // Mysql K-A Stats
    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "avg_transactions_per_client_connection",
                            RECD_FLOAT, TRANSACTIONS_PER_CLIENT_CON, SYNC_AVG, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "avg_transactions_per_server_connection",
                            RECD_FLOAT, TRANSACTIONS_PER_SERVER_CON, SYNC_AVG, RECP_NULL);

    // client stats
    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "client_requests",
                            RECD_INT, CLIENT_REQUESTS, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "client_large_requests",
                            RECD_INT, CLIENT_LARGE_REQUESTS, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "client_internal_requests",
                            RECD_INT, CLIENT_INTERNAL_REQUESTS, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "local_session_state_requests",
                            RECD_INT, CLIENT_USE_LOCAL_SESSION_STATE_REQUESTS, SYNC_SUM, RECP_PERSISTENT);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "client_missing_pk_requests",
                            RECD_INT, CLIENT_MISSING_PK_REQUESTS, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "client_completed_requests",
                            RECD_INT, CLIENT_COMPLETED_REQUESTS, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "client_connection_abort_count",
                            RECD_INT, CLIENT_CONNECTION_ABORT_COUNT, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "client_select_requests",
                            RECD_INT, CLIENT_SELECT_REQUESTS, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "client_insert_requests",
                            RECD_INT, CLIENT_INSERT_REQUESTS, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "client_update_requests",
                            RECD_INT, CLIENT_UPDATE_REQUESTS, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "client_delete_requests",
                            RECD_INT, CLIENT_DELETE_REQUESTS, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "client_other_requests",
                            RECD_INT, CLIENT_OTHER_REQUESTS, SYNC_SUM, RECP_NULL);

    // request size histogram
    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "request_size_100_count",
                            RECD_COUNTER, REQUEST_SIZE_100_COUNT, SYNC_COUNT, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "request_size_1K_count",
                            RECD_COUNTER, REQUEST_SIZE_1K_COUNT, SYNC_COUNT, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "request_size_3K_count",
                            RECD_COUNTER, REQUEST_SIZE_3K_COUNT, SYNC_COUNT, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "request_size_5K_count",
                            RECD_COUNTER, REQUEST_SIZE_5K_COUNT, SYNC_COUNT, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "request_size_10K_count",
                            RECD_COUNTER, REQUEST_SIZE_10K_COUNT, SYNC_COUNT, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "request_size_1M_count",
                            RECD_COUNTER, REQUEST_SIZE_1M_COUNT, SYNC_COUNT, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "request_size_inf_count",
                            RECD_COUNTER, REQUEST_SIZE_INF_COUNT, SYNC_COUNT, RECP_NULL);

    // response size histogram
    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "response_size_100_count",
                            RECD_COUNTER, RESPONSE_SIZE_100_COUNT, SYNC_COUNT, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "response_size_1K_count",
                            RECD_COUNTER, RESPONSE_SIZE_1K_COUNT, SYNC_COUNT, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "response_size_3K_count",
                            RECD_COUNTER, RESPONSE_SIZE_3K_COUNT, SYNC_COUNT, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "response_size_5K_count",
                            RECD_COUNTER, RESPONSE_SIZE_5K_COUNT, SYNC_COUNT, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "response_size_10K_count",
                            RECD_COUNTER, RESPONSE_SIZE_10K_COUNT, SYNC_COUNT, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "response_size_1M_count",
                            RECD_COUNTER, RESPONSE_SIZE_1M_COUNT, SYNC_COUNT, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "response_size_inf_count",
                            RECD_COUNTER, RESPONSE_SIZE_INF_COUNT, SYNC_COUNT, RECP_NULL);

    // client connection speed stats
    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "client_speed_bytes_per_sec_100",
                            RECD_COUNTER, CLIENT_SPEED_BYTES_PER_SEC_100, SYNC_COUNT, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "client_speed_bytes_per_sec_1K",
                            RECD_COUNTER, CLIENT_SPEED_BYTES_PER_SEC_1K, SYNC_COUNT, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "client_speed_bytes_per_sec_10K",
                            RECD_COUNTER, CLIENT_SPEED_BYTES_PER_SEC_10K, SYNC_COUNT, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "client_speed_bytes_per_sec_100K",
                            RECD_COUNTER, CLIENT_SPEED_BYTES_PER_SEC_100K, SYNC_COUNT, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "client_speed_bytes_per_sec_1M",
                            RECD_COUNTER, CLIENT_SPEED_BYTES_PER_SEC_1M, SYNC_COUNT, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "client_speed_bytes_per_sec_10M",
                            RECD_COUNTER, CLIENT_SPEED_BYTES_PER_SEC_10M, SYNC_COUNT, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "client_speed_bytes_per_sec_100M",
                            RECD_COUNTER, CLIENT_SPEED_BYTES_PER_SEC_100M, SYNC_COUNT, RECP_NULL);

    // server connection speed stats
    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "server_speed_bytes_per_sec_100",
                            RECD_COUNTER, SERVER_SPEED_BYTES_PER_SEC_100, SYNC_COUNT, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "server_speed_bytes_per_sec_1K",
                            RECD_COUNTER, SERVER_SPEED_BYTES_PER_SEC_1K, SYNC_COUNT, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "server_speed_bytes_per_sec_10K",
                            RECD_COUNTER, SERVER_SPEED_BYTES_PER_SEC_10K, SYNC_COUNT, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "server_speed_bytes_per_sec_100K",
                            RECD_COUNTER, SERVER_SPEED_BYTES_PER_SEC_100K, SYNC_COUNT, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "server_speed_bytes_per_sec_1M",
                            RECD_COUNTER, SERVER_SPEED_BYTES_PER_SEC_1M, SYNC_COUNT, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "server_speed_bytes_per_sec_10M",
                            RECD_COUNTER, SERVER_SPEED_BYTES_PER_SEC_10M, SYNC_COUNT, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "server_speed_bytes_per_sec_100M",
                            RECD_COUNTER, SERVER_SPEED_BYTES_PER_SEC_100M, SYNC_COUNT, RECP_NULL);

    // server stats
    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "server_connect_count",
                            RECD_INT, SERVER_CONNECT_COUNT, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "server_connect_retries",
                            RECD_INT, SERVER_CONNECT_RETRIES, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "server_pl_lookup_count",
                            RECD_INT, SERVER_PL_LOOKUP_COUNT, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "server_pl_lookup_retries",
                            RECD_INT, SERVER_PL_LOOKUP_RETRIES, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "broken_server_connections",
                            RECD_INT, BROKEN_SERVER_CONNECTIONS, SYNC_SUM, RECP_PERSISTENT);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "server_responses",
                            RECD_INT, SERVER_RESPONSES, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "server_requests",
                            RECD_INT, SERVER_REQUESTS, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "server_error_responses",
                            RECD_INT, SERVER_ERROR_RESPONSES, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "server_resultset_responses",
                            RECD_INT, SERVER_RESULTSET_RESPONSES, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "server_ok_responses",
                            RECD_INT, SERVER_OK_RESPONSES, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "server_other_responses",
                            RECD_INT, SERVER_OTHER_RESPONSES, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "send_saved_login_requests",
                            RECD_INT, SEND_SAVED_LOGIN_REQUESTS, SYNC_SUM, RECP_PERSISTENT);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "send_all_session_vars_requests",
                            RECD_INT, SEND_ALL_SESSION_VARS_REQUESTS, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "send_use_database_requests",
                            RECD_INT, SEND_USE_DATABASE_REQUESTS, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "send_changed_session_vars_requests",
                            RECD_INT, SEND_CHANGED_SESSION_VARS_REQUESTS, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "send_last_insert_id_requests",
                            RECD_INT, SEND_LAST_INSERT_ID_REQUESTS, SYNC_SUM, RECP_PERSISTENT);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "send_start_trans_requests",
                            RECD_INT, SEND_START_TRANS_REQUESTS, SYNC_SUM, RECP_NULL);

    // size stats
    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "client_request_total_size",
                            RECD_INT, CLIENT_REQUEST_TOTAL_SIZE, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "client_response_total_size",
                            RECD_INT, CLIENT_RESPONSE_TOTAL_SIZE, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "server_request_total_size",
                            RECD_INT, SERVER_REQUEST_TOTAL_SIZE, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "server_response_total_size",
                            RECD_INT, SERVER_RESPONSE_TOTAL_SIZE, SYNC_SUM, RECP_NULL);

    // times
    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "total_transactions_time",
                            RECD_INT, TOTAL_TRANSACTIONS_TIME, SYNC_SUM, RECP_PERSISTENT);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "total_user_transactions_time",
                            RECD_INT, TOTAL_USER_TRANSACTIONS_TIME, SYNC_SUM, RECP_PERSISTENT);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "total_client_request_read_time",
                            RECD_INT, TOTAL_CLIENT_REQUEST_READ_TIME, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "total_client_response_write_time",
                            RECD_INT, TOTAL_CLIENT_RESPONSE_WRITE_TIME, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "total_client_request_analyze_time",
                            RECD_INT, TOTAL_CLIENT_REQUEST_ANALYZE_TIME, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "total_client_transaction_idle_time",
                            RECD_INT, TOTAL_CLIENT_TRANSACTION_IDLE_TIME, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "total_ok_packet_trim_time",
                            RECD_INT, TOTAL_OK_PACKET_TRIM_TIME, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "total_server_process_request_time",
                            RECD_INT, TOTAL_SERVER_PROCESS_REQUEST_TIME, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "total_server_response_read_time",
                            RECD_INT, TOTAL_SERVER_RESPONSE_READ_TIME, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "total_server_response_analyze_time",
                            RECD_INT, TOTAL_SERVER_RESPONSE_ANALYZE_TIME, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "total_send_saved_login_time",
                            RECD_INT, TOTAL_SEND_SAVED_LOGIN_TIME, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "total_send_all_session_vars_time",
                            RECD_INT, TOTAL_SEND_ALL_SESSION_VARS_TIME, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "total_send_use_database_time",
                            RECD_INT, TOTAL_SEND_USE_DATABASE_TIME, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "total_send_changed_session_vars_time",
                            RECD_INT, TOTAL_SEND_CHANGED_SESSION_VARS_TIME, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "total_send_last_insert_id_time",
                            RECD_INT, TOTAL_SEND_LAST_INSERT_ID_TIME, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "total_send_start_trans_time",
                            RECD_INT, TOTAL_SEND_START_TRANS_TIME, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "total_pl_lookup_time",
                            RECD_INT, TOTAL_PL_LOOKUP_TIME, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "total_congestion_control_time",
                            RECD_INT, TOTAL_CONGESTION_CONTROL_LOOKUP_TIME, SYNC_SUM, RECP_NULL);

    MYSQL_REGISTER_RAW_STAT(mysql_rsb, RECT_PROCESS, "total_server_connect_time",
                            RECD_INT, TOTAL_SERVER_CONNECT_TIME, SYNC_SUM, RECP_NULL);

  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
