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

#include "proxy/mysql/ob_mysql_sm_time_stat.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
#define TO_STRING_TIME_US(name) \
  databuff_printf(buf, buf_len, pos, #name"us=%ld, ", hrtime_to_usec(name));

#define TO_STRING_TIME_US_END(name) \
  databuff_printf(buf, buf_len, pos, #name"us=%ld", hrtime_to_usec(name));

int64_t ObCmdTimeStat::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  TO_STRING_TIME_US(client_transaction_idle_time_);
  TO_STRING_TIME_US(client_request_read_time_);
  TO_STRING_TIME_US(client_request_analyze_time_);
  TO_STRING_TIME_US(cluster_resource_create_time_);
  TO_STRING_TIME_US(pl_lookup_time_);
  TO_STRING_TIME_US(pl_process_time_);

  TO_STRING_TIME_US(bl_lookup_time_);
  TO_STRING_TIME_US(bl_process_time_);

#if OB_DETAILED_SLOW_QUERY
  TO_STRING_TIME_US(debug_assign_time_);
  TO_STRING_TIME_US(debug_consistency_time_);
  TO_STRING_TIME_US(debug_random_time_);
  TO_STRING_TIME_US(debug_fill_time_);
  TO_STRING_TIME_US(debug_total_fill_time_);
  TO_STRING_TIME_US(debug_get_next_time_);
#endif

  TO_STRING_TIME_US(congestion_control_time_);
  TO_STRING_TIME_US(congestion_process_time_);
  TO_STRING_TIME_US(do_observer_open_time_);
#if OB_DETAILED_SLOW_QUERY
  TO_STRING_TIME_US(debug_check_safe_snapshot_time_);
#endif

  TO_STRING_TIME_US(server_connect_time_);
  TO_STRING_TIME_US(server_sync_session_variable_time_);
  TO_STRING_TIME_US(server_send_saved_login_time_);
  TO_STRING_TIME_US(server_send_use_database_time_);
  TO_STRING_TIME_US(server_send_session_variable_time_);
  TO_STRING_TIME_US(server_send_session_user_variable_time_);
  TO_STRING_TIME_US(server_send_all_session_variable_time_);
  TO_STRING_TIME_US(server_send_start_trans_time_);
  TO_STRING_TIME_US(server_send_xa_start_time_);
  TO_STRING_TIME_US(server_send_init_sql_time_);
  TO_STRING_TIME_US(build_server_request_time_);
  TO_STRING_TIME_US(plugin_compress_request_time_);
  TO_STRING_TIME_US(prepare_send_request_to_server_time_);
  TO_STRING_TIME_US(server_request_write_time_);
  TO_STRING_TIME_US(server_process_request_time_);
  TO_STRING_TIME_US(server_response_read_time_);
  TO_STRING_TIME_US(plugin_decompress_response_time_);
  TO_STRING_TIME_US(server_response_analyze_time_);
  TO_STRING_TIME_US(ok_packet_trim_time_);
  TO_STRING_TIME_US(client_response_write_time_);
  TO_STRING_TIME_US_END(request_total_time_);
  J_OBJ_END();
  return pos;
}


int64_t ObTransactionStat::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(client_requests), K_(server_responses), K_(pl_lookup_retries), K_(server_retries),
       K_(client_request_bytes), K_(server_request_bytes), K_(server_response_bytes),
       K_(client_response_bytes));
  J_COMMA();
  TO_STRING_TIME_US(client_transaction_idle_time_);
  TO_STRING_TIME_US(client_process_request_time_);
  TO_STRING_TIME_US(client_request_read_time_);
  TO_STRING_TIME_US(client_request_analyze_time_);
  TO_STRING_TIME_US(cluster_resource_create_time_);
  TO_STRING_TIME_US(pl_lookup_time_);
  TO_STRING_TIME_US(pl_process_time_);
  TO_STRING_TIME_US(congestion_control_time_);
  TO_STRING_TIME_US(congestion_process_time_);
  TO_STRING_TIME_US(do_observer_open_time_);
  TO_STRING_TIME_US(server_connect_time_);
  TO_STRING_TIME_US(sync_session_variable_time_);
  TO_STRING_TIME_US(send_saved_login_time_);
  TO_STRING_TIME_US(send_use_database_time_);
  TO_STRING_TIME_US(send_session_vars_time_);
  TO_STRING_TIME_US(send_session_user_vars_time_);
  TO_STRING_TIME_US(send_all_session_vars_time_);
  TO_STRING_TIME_US(send_start_trans_time_);
  TO_STRING_TIME_US(send_xa_start_time_);
  TO_STRING_TIME_US(build_server_request_time_);
  TO_STRING_TIME_US(plugin_compress_request_time_);
  TO_STRING_TIME_US(prepare_send_request_to_server_time_);
  TO_STRING_TIME_US(server_request_write_time_);
  TO_STRING_TIME_US(server_process_request_time_);
  TO_STRING_TIME_US(server_response_read_time_);
  TO_STRING_TIME_US(plugin_decompress_response_time_);
  TO_STRING_TIME_US(server_response_analyze_time_);
  TO_STRING_TIME_US(ok_packet_trim_time_);
  TO_STRING_TIME_US(client_response_write_time_);
  TO_STRING_TIME_US_END(trans_time_);
  J_OBJ_END();
  return pos;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
