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

#ifndef OBPROXY_MYSQL_SM_TIME_STAT_H
#define OBPROXY_MYSQL_SM_TIME_STAT_H

#include "lib/time/ob_hrtime.h"
#include "utils/ob_proxy_lib.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

struct ObClientMilestones
{
  ObClientMilestones() { reset(); }
  ~ObClientMilestones() { }

  void reset()
  {
    MEMSET(this, 0, sizeof(ObClientMilestones));
  }

  // client :
  // client_begin represents the time this transaction started.
  // If this is the first transaction in a connection, then client_begin is set to accept time.
  // otherwise it is set to first read time.
  ObHRTime client_begin_;
  ObHRTime client_read_end_;
  ObHRTime client_write_begin_;
  ObHRTime client_end_;

  //analyzer
  ObHRTime analyze_request_begin_;
  ObHRTime analyze_request_end_;
};

struct ObServerMilestones
{
  ObServerMilestones() { reset(); }
  ~ObServerMilestones() { }

  void reset()
  {
    MEMSET(this, 0, sizeof(ObServerMilestones));
  }

  // server
  ObHRTime server_write_begin_;
  ObHRTime server_write_end_;
  ObHRTime server_read_begin_;
  ObHRTime server_read_end_;
};

struct ObTransactionMilestones
{
  ObTransactionMilestones()
      : last_client_cmd_end_(0), server_connect_begin_(0), server_connect_end_(0),
        server_first_write_begin_(0), pl_lookup_begin_(0), pl_lookup_end_(0),
        pl_process_begin_(0), pl_process_end_(0), bl_lookup_begin_(0), bl_lookup_end_(0),
        bl_process_begin_(0), bl_process_end_(0), congestion_control_begin_(0),
        congestion_control_end_(0), congestion_process_begin_(0), congestion_process_end_(0),
        cluster_resource_create_begin_(0), cluster_resource_create_end_(0),
        trans_start_(0), trans_finish_(0), do_observer_open_begin_(0), do_observer_open_end_(0)
  { }
  ~ObTransactionMilestones() { }

  void trans_reset()
  {
    MEMSET(this, 0, sizeof(ObTransactionMilestones));
  }

  void cmd_reset()
  {
    client_.reset();
    server_.reset();
    server_first_write_begin_ = 0;
  }

  ObClientMilestones client_;

  // last_client_cmd_end_:
  // it record last request complete time,
  // cannot reset to 0 by last request,
  // because it will be used in the current request.
  ObHRTime last_client_cmd_end_;

  // server
  ObHRTime server_connect_begin_;
  ObHRTime server_connect_end_;
  ObServerMilestones server_;

  ObHRTime server_first_write_begin_;

  // partition location lookup
  ObHRTime pl_lookup_begin_;
  ObHRTime pl_lookup_end_;

  // pl process
  ObHRTime pl_process_begin_;
  ObHRTime pl_process_end_;

  // binlog location lookup
  ObHRTime bl_lookup_begin_;
  ObHRTime bl_lookup_end_;

  // binlog location process
  ObHRTime bl_process_begin_;
  ObHRTime bl_process_end_;

  // congestion_control_lookup
  ObHRTime congestion_control_begin_;
  ObHRTime congestion_control_end_;

  // congestion process
  ObHRTime congestion_process_begin_;
  ObHRTime congestion_process_end_;

  // init cluster resource
  ObHRTime cluster_resource_create_begin_;
  ObHRTime cluster_resource_create_end_;

  // state machine
  ObHRTime trans_start_;
  ObHRTime trans_finish_;

  // do observer open
  ObHRTime do_observer_open_begin_;
  ObHRTime do_observer_open_end_;
};

struct ObCmdTimeStat
{
  ObCmdTimeStat()
  {
    reset();
  }
  ~ObCmdTimeStat () { }

  void reset()
  {
    MEMSET(this, 0, sizeof(ObCmdTimeStat));
  }

  int64_t to_string(char *buf, const int64_t buf_len) const;

  ObHRTime client_transaction_idle_time_;
  ObHRTime client_request_read_time_;

  ObHRTime client_request_analyze_time_;
  ObHRTime cluster_resource_create_time_;
  ObHRTime pl_lookup_time_;
  ObHRTime pl_process_time_;
  ObHRTime bl_lookup_time_;
  ObHRTime bl_process_time_;

#if OB_DETAILED_SLOW_QUERY
  ObHRTime debug_assign_time_;
  ObHRTime debug_consistency_time_;
  ObHRTime debug_random_time_;
  ObHRTime debug_fill_time_;
  ObHRTime debug_total_fill_time_;
  ObHRTime debug_get_next_time_;
#endif

  ObHRTime congestion_control_time_;
  ObHRTime congestion_process_time_;

  ObHRTime do_observer_open_time_;
#if OB_DETAILED_SLOW_QUERY
  ObHRTime debug_check_safe_snapshot_time_;
#endif
  ObHRTime server_connect_time_;

  ObHRTime server_sync_session_variable_time_;//write_time + process_time + read_time + decompress + analyze + trim
  ObHRTime server_send_saved_login_time_;
  ObHRTime server_send_use_database_time_;
  ObHRTime server_send_session_variable_time_;
  ObHRTime server_send_session_user_variable_time_;
  ObHRTime server_send_all_session_variable_time_;
  ObHRTime server_send_start_trans_time_;
  ObHRTime server_send_xa_start_time_;

  ObHRTime build_server_request_time_;
  ObHRTime plugin_compress_request_time_;

  ObHRTime prepare_send_request_to_server_time_;
  ObHRTime server_request_write_time_;

  ObHRTime server_process_request_time_;
  ObHRTime server_response_read_time_;
  ObHRTime plugin_decompress_response_time_;
  ObHRTime server_response_analyze_time_;
  ObHRTime ok_packet_trim_time_;

  ObHRTime client_response_write_time_;

  ObHRTime request_total_time_;
};

struct ObTransactionStat
{
  ObTransactionStat()
  {
    reset();
  }
  ~ObTransactionStat() { }

  void reset()
  {
    MEMSET(this, 0, sizeof(ObTransactionStat));
  }

  int64_t to_string(char *buf, const int64_t buf_len) const;

  int32_t client_requests_;
  int32_t server_responses_;
  int32_t pl_lookup_retries_;
  int32_t server_retries_;

  int64_t client_request_bytes_;
  int64_t server_request_bytes_;
  int64_t server_response_bytes_;
  int64_t client_response_bytes_;

  ObHRTime client_transaction_idle_time_;
  ObHRTime client_process_request_time_;//client_request_read+server_request_write
  ObHRTime client_request_read_time_;

  ObHRTime client_request_analyze_time_;
  ObHRTime cluster_resource_create_time_;
  ObHRTime pl_lookup_time_;
  ObHRTime pl_process_time_;
  ObHRTime congestion_control_time_;
  ObHRTime congestion_process_time_;

  ObHRTime do_observer_open_time_;
  ObHRTime server_connect_time_;

  ObHRTime sync_session_variable_time_;
  ObHRTime send_saved_login_time_;
  ObHRTime send_use_database_time_;
  ObHRTime send_session_vars_time_;
  ObHRTime send_session_user_vars_time_;
  ObHRTime send_all_session_vars_time_;
  ObHRTime send_start_trans_time_;
  ObHRTime send_xa_start_time_;

  ObHRTime build_server_request_time_;
  ObHRTime plugin_compress_request_time_;
  ObHRTime prepare_send_request_to_server_time_;
  ObHRTime server_request_write_time_;

  ObHRTime server_process_request_time_;
  ObHRTime server_response_read_time_;
  ObHRTime plugin_decompress_response_time_;
  ObHRTime server_response_analyze_time_;
  ObHRTime ok_packet_trim_time_;
  ObHRTime client_response_write_time_;

  ObHRTime trans_time_;
  bool is_in_testload_trans_;
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_MYSQL_SM_TIME_STAT_H
