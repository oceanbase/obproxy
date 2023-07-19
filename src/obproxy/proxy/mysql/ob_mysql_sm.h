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
 * ************************************************************
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef OBPROXY_MYSQL_SM_H
#define OBPROXY_MYSQL_SM_H

#include "utils/ob_proxy_lib.h"
#include "utils/ob_proxy_monitor_utils.h"
#include "utils/ob_target_db_server.h"
#include "iocore/eventsystem/ob_event_system.h"
#include "proxy/mysqllib/ob_mysql_request_analyzer.h"
#include "proxy/mysqllib/ob_mysql_compress_analyzer.h"
#include "proxy/mysqllib/ob_mysql_compress_ob20_analyzer.h"
#include "proxy/api/ob_api_internal.h"
#include "proxy/api/ob_mysql_sm_api.h"
#include "proxy/mysql/ob_mysql_transact.h"
#include "proxy/mysql/ob_mysql_vctable.h"
#include "proxy/mysql/ob_mysql_tunnel.h"
#include "proxy/mysql/ob_mysql_client_session.h"
#include "proxy/mysql/ob_mysql_sm_time_stat.h"
#include "proxy/shard/obproxy_shard_ddl_cont.h"
#include "obutils/ob_tenant_stat_struct.h"
#include "engine/ob_proxy_operator_result.h"
#include "lib/utility/ob_2_0_full_link_trace_info.h"
#include "rpc/proxy_protocol/proxy_protocol_v2.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

// The default size for mysql packet buffers
// We want to use a larger buffer size when reading response
// from the observer since we want to get as much of the
// document as possible on the first read Marco benchmarked
// about 3% ops/second improvement using the larger buffer size
static int64_t const MYSQL_BUFFER_SIZE     = BUFFER_SIZE_FOR_INDEX(BUFFER_SIZE_INDEX_8K);
static const int64_t MYSQL_SM_LIST_BUCKETS = 64;

class ObMysqlServerSession;

enum ObMysqlSMMagic
{
  MYSQL_SM_MAGIC_ALIVE = 0x0000FEED,
  MYSQL_SM_MAGIC_DEAD  = 0xDEADFEED
};

struct ObStreamSizeStat
{
  ObStreamSizeStat()
  {
    reset();
  }
  ~ObStreamSizeStat() { }

  void reset()
  {
    memset(this, 0, sizeof(ObStreamSizeStat));
  }

  int64_t to_string(char *buf, const int64_t buf_len) const;

  int64_t client_request_bytes_;
  int64_t server_request_bytes_;
  int64_t server_response_bytes_;
  int64_t client_response_bytes_;
};

extern ObMutex g_debug_sm_list_mutex;

/* Attention!!!
 * The sm object does not call the constructor when allocate, but directly copies the memory.
 * Therefore, when adding member variables to sm or adding sub-members to sm member variables,
 * if the new member contains pointer variables, it must be displayed Initialize in the init function,
 * otherwise the new object member pointer may point to the memory in the old sm object;
 */
class ObMysqlSM : public event::ObContinuation
{
  friend class ObMysqlSMApi;
  friend class ObShowSMHandler;
public:
  ObMysqlSM();
  virtual ~ObMysqlSM() {}

  void cleanup();
  void destroy();

  static ObMysqlSM *allocate();
  static void instantiate_func(ObMysqlSM &prototype, ObMysqlSM &new_instance);
  static void make_scatter_list(ObMysqlSM &prototype);

  int init();
  int attach_client_session(ObMysqlClientSession &client_vc_arg,
                            event::ObIOBufferReader &buffer_reader,
                            const bool is_new_conn = false);

  // Called by ObMysqlSessionManager so that we can
  // reset the session timeouts and initiate a read
  // while holding the lock for the server session
  int attach_server_session(ObMysqlServerSession &s);

  // Called by either state_partition_location_lookup() or directly by the
  // partitionLocationProcessor in the case of inline completion.
  // Handles the setting of all state necessary before calling transact to
  // process the partition location lookup.
  // A NULL result indicates the partition location lookup failed
  int process_partition_location(ObMysqlRouteResult &result);

  // Called from ob_api.cpp which acquires the state machine lock
  // before calling
  int state_api_callback(int event, void *data);
  int state_api_callout(int event, void *data);

  // Functions for manipulating api hooks
  void txn_hook_append(const ObMysqlHookID id, ObContInternal *cont);
  void txn_hook_prepend(const ObMysqlHookID id, ObContInternal *cont);
  ObAPIHook *txn_hook_get(const ObMysqlHookID id);

  ObMysqlTunnel &get_tunnel() { return tunnel_; };
  const ObMysqlTunnel &get_tunnel() const { return tunnel_; };
  ObMysqlVCTableEntry *get_client_entry() { return client_entry_; }

  // Debugging routines to dump the SM history
  void dump_history_state();
  void add_history_entry(const char *fileline, const int event, const int32_t reentrant);

  int get_mysql_schedule(int event, void *data);
  int setup_handle_shard_ddl(event::ObAction *action);
  int state_handle_shard_ddl(int event, void *data);
  int process_shard_ddl_result(ObShardDDLStatus *ddl_status);
  int setup_handle_execute_plan();
  int state_handle_execute_plan(int event, void *data);
  int process_executor_result(event::ObIOBufferReader *resp_reader);

  int handle_shard_request(bool &need_response_for_stmt, bool &need_wait_callback);

  int check_user_identity(const ObString &user_name, const ObString &tenant_name, const ObString &cluster_name);
  int save_user_login_info(ObClientSessionInfo &session_info, ObHSRResult &hsr_result);
  void analyze_mysql_request(ObMysqlAnalyzeStatus &status, const bool is_mysql_req_in_ob20 = false);
  int analyze_login_request(ObRequestAnalyzeCtx &ctx, ObMysqlAnalyzeStatus &status);
  int analyze_change_user_request();
  int analyze_ps_prepare_request();
  int do_analyze_ps_prepare_request(const ObString &ps_sql);
  int analyze_ps_execute_request(bool is_large_request = false);
  int do_analyze_ps_execute_request(ObPsIdEntry *entry, bool is_large_request);
  int do_analyze_ps_execute_request_with_flag(ObPsIdEntry *entry);
  int do_analyze_ps_execute_request_without_flag(ObPsIdEntry *entry);
  int analyze_text_ps_prepare_request(const ObRequestAnalyzeCtx &ctx);
  int do_parse_text_ps_prepare_sql(char*& text_ps_prepare_buf, int64_t& text_ps_prepare_buf_len,
      ObString& text_ps_sql, const ObRequestAnalyzeCtx& ctx);
  int do_analyze_text_ps_prepare_request(const ObString& text_ps_sql);
  int analyze_text_ps_execute_request();
  int analyze_text_ps_drop_request();
  int analyze_fetch_request();
  int analyze_close_reset_request();
  int analyze_ps_prepare_execute_request();
  bool need_setup_client_transfer();
  bool check_connection_throttle();
  bool can_pass_white_list();
  int analyze_capacity_flag_from_client();
  bool check_vt_connection_throttle();
  bool is_partition_table_route_supported();
  bool is_pl_route_supported();
  int encode_error_message(int err_code);
  int handle_saved_session_variables();
  void print_mysql_complete_log(ObMysqlTunnelProducer *p);

  event::ObIOBufferReader *get_client_buffer_reader() { return client_buffer_reader_; }
  event::ObIOBufferReader *get_server_buffer_reader() { return server_buffer_reader_; }

  ObMysqlServerSession *get_server_session() { return server_session_; }
  ObMysqlClientSession *get_client_session() { return client_session_; }
  ObMysqlClientSession *get_client_session() const { return client_session_; }

  ObMysqlTransactionAnalyzer &get_trans_analyzer() { return analyzer_; }

  int64_t get_query_timeout();
  ObHRTime get_based_hrtime();
  bool is_causal_order_read_enabled();

  // set and cancel timmer
  void set_client_connect_timeout();
  void set_client_wait_timeout();
  void set_client_net_read_timeout();
  void set_server_query_timeout();
  void cancel_server_query_timeout();
  void set_server_trx_timeout();
  void set_server_quit_timeout();
  void set_internal_cmd_timeout(const ObHRTime timeout);

  // record the transaction stats into client session stats
  void update_session_stats(int64_t *stats, const int64_t stats_size);

  void get_server_session_ids(uint32_t &server_sessid, int64_t &ss_id);

  const common::ObString &get_server_trace_id();

  void clear_client_entry();
  void clear_server_entry();
  bool can_server_session_release();
  void release_server_session_to_pool();
  void release_server_session();
  bool need_close_last_used_ss();

  ObProxyProtocol get_server_session_protocol() const;
  ObProxyProtocol get_client_session_protocol() const;
  bool is_checksum_on() const;
  bool is_extra_ok_packet_for_stats_enabled() const;
  uint8_t get_request_seq();
  obmysql::ObMySQLCmd get_request_cmd();

  ObMysqlCompressAnalyzer &get_compress_analyzer();

  int swap_mutex(event::ObProxyMutex *mutex);

  int trim_ok_packet(event::ObIOBufferReader &reader);
  int use_set_pool_addr();

  bool is_cloud_user() const;
  bool need_reject_user_login(const common::ObString &user,
                              const common::ObString &tenant,
                              const bool has_tenant_username,
                              const bool has_cluster_username,
                              const bool is_cloud_user) const;
  inline void set_skip_plugin(const bool bvalue) { skip_plugin_ = bvalue; }
  void set_detect_server_info(net::ObIpEndpoint target_addr, int cnt, int64_t time);
public:
  static const int64_t OP_LOCAL_NUM = 32;
  static const int64_t MAX_SCATTER_LEN;

  uint32_t sm_id_;
  ObMysqlSMMagic magic_;

  ObMysqlTransact::ObTransState trans_state_;
  ObMysqlClientSession *client_session_;
  obutils::ObClusterResource *sm_cluster_resource_; // point to client_session's cluster_resource

  LINK(ObMysqlSM, stat_link_);

#ifdef USE_MYSQL_DEBUG_LISTS
  LINK(ObMysqlSM, link_);
#endif

  // Stats
  ObStreamSizeStat cmd_size_stats_;
  ObCmdTimeStat cmd_time_stats_;
  ObTransactionStat trans_stats_;
  ObTransactionMilestones milestones_;
  bool is_updated_stat_;
  bool is_in_list_;

  // hooks_set records whether there are any hooks relevant
  // to this transaction. Used to avoid costly calls
  // do_api_callout_internal()
  bool hooks_set_;
  ObMysqlSMApi api_;

  ObMysqlTransactionAnalyzer analyzer_;
  ObMysqlCompressAnalyzer compress_analyzer_;
  ObMysqlCompressOB20Analyzer compress_ob20_analyzer_;
  ObMysqlRequestAnalyzer request_analyzer_;

public:
  common::FLTObjManage flt_;     // ob20 full link trace obj
  char flt_trace_buffer_[MAX_TRACE_LOG_SIZE];     // buffer for full link trace with each sm
  
public:
  static uint32_t get_next_sm_id();
  void remove_client_entry();
  void remove_server_entry();

  void refresh_cluster_resource();

  int main_handler(int event, void *data);
  int tunnel_handler_response_transfered(int event, void *data);
  int tunnel_handler_request_transfered(int event, void *data);
  int mysql_client_event_handler(int event, void *data);

  int state_client_request_read(int event, void *data);
  int state_watch_for_client_abort(int event, void *data);
  int state_server_addr_lookup(int event, void *data);
  int state_partition_location_lookup(int event, void *data);
  int state_binlog_location_lookup(int event, void *data);
  int state_add_to_list(int event, void *data);
  int state_remove_from_list(int event, void *data);

  //internal request: execute_internal_cmd
  int state_execute_internal_cmd(int event, void *data);

  int handle_retry_acquire_svr_session();
  int do_internal_observer_open_event(int event, void *data);

  // OB Server Handlers
  int state_observer_open(int event, void *data);
  int state_congestion_control_lookup(int event, void *data);
  int state_server_request_send(int event, void *data);
  int state_server_response_read(int event, void *data);

  // API
  int state_request_wait_for_transform_read(int event, void *data);
  int state_response_wait_for_transform_read(int event, void *data);

  // Tunnel event handlers
  int tunnel_handler_server(int event, ObMysqlTunnelProducer &p);
  int tunnel_handler_server_cmd_complete(ObMysqlTunnelProducer &p);
  int tunnel_handler_client(int event, ObMysqlTunnelConsumer &c);
  int tunnel_handler_request_transfer_client(int event, ObMysqlTunnelProducer &c);
  int tunnel_handler_request_transfer_server(int event, ObMysqlTunnelConsumer &c);
  int tunnel_handler_transform_write(int event, ObMysqlTunnelConsumer &c);
  int tunnel_handler_transform_read(int event, ObMysqlTunnelProducer &p);
  int tunnel_handler_plugin_client(int event, ObMysqlTunnelConsumer &c);

  void do_partition_location_lookup();
  void do_binlog_location_lookup();
  void do_congestion_control_lookup();
  void do_server_addr_lookup();
  int do_observer_open();
  int do_oceanbase_internal_observer_open(ObMysqlServerSession *&selected_session);
  int do_normal_internal_observer_open(ObMysqlServerSession *&selected_session);
  int do_internal_observer_open();
  void do_internal_request();
  int do_internal_request_for_sharding_init_db(event::ObMIOBuffer *buf);
  int do_internal_request_for_sharding_show_db_version(event::ObMIOBuffer *buf);
  int do_internal_request_for_sharding_show_db(event::ObMIOBuffer *buf);
  int do_internal_request_for_sharding_show_table(event::ObMIOBuffer *buf);
  int do_internal_request_for_sharding_show_table_status(event::ObMIOBuffer *buf);
  int do_internal_request_for_sharding_show_elastic_id(event::ObMIOBuffer *buf);
  int do_internal_request_for_sharding_show_topology(event::ObMIOBuffer *buf);
  int do_internal_request_for_sharding_select_db(event::ObMIOBuffer *buf);
  int connect_observer();
  int setup_client_transfer(ObMysqlVCType to_vc_type);
  void handle_api_return();
  void callout_api_and_start_next_action(const ObMysqlTransact::ObStateMachineActionType api_next_action);

  void handle_server_setup_error(int event, void *data);
  void handle_observer_open();
  void handle_request_transfer_failure();
  void set_client_abort(const ObMysqlTransact::ObAbortStateType client_abort, int event);
  int setup_client_request_read();
  int setup_server_response_read();
  int setup_server_request_send();
  int setup_server_transfer();
  int setup_internal_transfer(MysqlSMHandler handler);
  void setup_error_transfer();
  int setup_cmd_complete();

  void set_next_state();
  void call_transact_and_set_next_state(TransactEntryFunc f);

  int init_request_content(ObRequestAnalyzeCtx &ctx, const bool is_mysql_req_in_ob20);
  int server_transfer_init(event::ObMIOBuffer *buf, int64_t &nbytes);

  void setup_set_cached_variables();
  void setup_get_cluster_resource();
  int state_get_cluster_resource(int event, void *data);
  int process_cluster_resource(void *data);
  int process_server_addr_lookup(const ObProxyKillQueryInfo *query_info);

  int handle_first_response_packet(ObMysqlAnalyzeStatus &state, int64_t &first_pkt_len, bool need_receive_completed);
  int handle_oceanbase_first_response_packet(ObMysqlAnalyzeStatus &state,
                                             const bool need_receive_completed,
                                             int64_t &first_pkt_len);
  int handle_first_compress_response_packet(ObMysqlAnalyzeStatus &state,
    const bool need_receive_completed, int64_t &first_pkt_len);
  // handle first standard mysql packet
  int handle_first_normal_response_packet(ObMysqlAnalyzeStatus &state,
                                          const bool need_receive_completed,
                                          int64_t &first_pkt_len);

  int handle_first_request_packet(ObMysqlAnalyzeStatus &status, int64_t &first_packet_len);
  int handle_first_compress_request_packet(ObMysqlAnalyzeStatus &state, int64_t &first_packet_len);

  void check_update_checksum_switch(const bool is_compressed_payload);

  void clear_entries();
  void consume_all_internal_data();

  void update_safe_read_snapshot();

  void update_congestion_entry(const int event);
  bool is_cached_dummy_entry_expired();
  void update_cached_dummy_entry(ObMysqlRouteResult &result);

  bool need_print_trace_stat() const;

  // terminate sm
  int kill_this_async_hook(int event, void *data);
  void kill_this();

  void update_stats();
  void update_cmd_stats();
  void update_monitor_log();
  void get_monitor_error_info(int32_t &error_code,
                              ObString &error_msg,
                              bool &is_error_resp);
  void update_monitor_stats(const ObString &logic_tenant_name,
                            const ObString &logic_database_name,
                            const ObString &cluster,
                            const ObString &tenant,
                            const ObString &database,
                            const common::DBServerType database_type,
                            const ObProxyBasicStmtType stmt_type,
                            char *error_code_str);

  int handle_limit(bool &need_direct_response_for_client);
  int handle_ldg(bool &need_direct_response_for_client);

  void save_response_flt_result_to_sm(common::FLTObjManage &flt);
  int save_request_flt_result_to_sm(common::FLTObjManage &flt);
  bool enable_record_full_link_trace_info();
  bool is_proxy_init_trace_log_info();
  int handle_resp_for_end_proxy_root_span(trace::UUID &trace_id, bool is_in_trans);
  int handle_req_for_begin_proxy_root_span();
  void handle_req_to_generate_root_span_from_client();
  int handle_req_to_generate_root_span_by_proxy();
  int handle_for_end_proxy_trace(trace::UUID &trace_id);
  int handle_resp_for_end_flt_trace(bool is_trans_completed);
  void set_enable_ob_protocol_v2(const bool enable_ob_protocol_v2) { enable_ob_protocol_v2_ = enable_ob_protocol_v2; }
  bool is_enable_ob_protocol_v2() const { return enable_ob_protocol_v2_; }

  bool is_proxy_switch_route() const;
private:
  // private functions
  int handle_server_request_send_long_data();
  int do_analyze_ps_execute_request_with_remain_value(event::ObMIOBuffer *writer, int64_t read_avail,
                                                      int64_t param_type_pos);
  int handle_compress_request_analyze_done(ObMysqlCompressedOB20AnalyzeResult &ob20_result, int64_t &first_packet_len,
                                           ObMysqlAnalyzeStatus &status);
  void analyze_status_after_analyze_mysql_in_ob20_payload(ObMysqlAnalyzeStatus &status,
                                                          ObClientSessionInfo &client_session_info);
  int analyze_ob20_remain_after_analyze_mysql_request_done(ObClientSessionInfo &client_session_info);
  int handle_proxy_protocol_v2_request(proxy_protocol_v2::ProxyProtocolV2 &v2, ObMysqlAnalyzeStatus &status);

private:
  static const int64_t HISTORY_SIZE = 32;

  struct ObHistory
  {
    const char *file_line_;
    int event_;
    int32_t reentrancy_;
  };
  ObHistory history_[HISTORY_SIZE];
  uint32_t history_pos_;

  ObMysqlTunnel tunnel_;
  ObMysqlVCTable vc_table_;

  ObMysqlVCTableEntry *client_entry_;
  event::ObIOBufferReader *client_buffer_reader_;

  ObMysqlVCTableEntry *server_entry_;
  ObMysqlServerSession *server_session_;
  event::ObIOBufferReader *server_buffer_reader_;

  MysqlSMHandler default_handler_;
  event::ObAction *pending_action_;

  int32_t reentrancy_count_;

  // The terminate flag is set by handlers and checked by the
  // main handler who will terminate the state machine
  // when the flag is set
  bool terminate_sm_;
  bool kill_this_async_done_;
  bool handling_ssl_request_;
  bool need_renew_cluster_resource_;
  bool is_in_trans_;
  int32_t retry_acquire_server_session_count_;
  int64_t start_acquire_server_session_time_;
  bool skip_plugin_;
  bool add_detect_server_cnt_;
  proxy_protocol_v2::ProxyProtocolV2 proxy_protocol_v2_;
public:
  // Multi-level configuration items: Because the most fine-grained configuration items
  // can take effect at the VIP level, each SM may need to be different
  char proxy_route_policy_[OB_MAX_CONFIG_VALUE_LEN];
  char proxy_idc_name_[OB_MAX_CONFIG_VALUE_LEN];
  bool enable_cloud_full_username_;
  bool enable_client_ssl_;
  bool enable_server_ssl_;
  bool enable_read_write_split_;
  bool enable_transaction_split_;
  bool enable_ob_protocol_v2_; // limit the scope of changing enable_protocol_v2_ to client session level 
  bool enable_read_stale_feedback_;
  int64_t read_stale_retry_interval_;
  uint64_t config_version_;
  ObTargetDbServer *target_db_server_;
};

inline ObMysqlSM *ObMysqlSM::allocate()
{
  return op_thread_alloc_init(ObMysqlSM, event::get_sm_allocator(), ObMysqlSM::instantiate_func);
}

inline uint32_t ObMysqlSM::get_next_sm_id()
{
  static volatile uint32_t next_sm_id = 0;
  return ATOMIC_AAF((&next_sm_id), 1);
}

inline void ObMysqlSM::remove_client_entry()
{
  if (OB_LIKELY(NULL != client_entry_)) {
    vc_table_.remove_entry(client_entry_);
    client_entry_ = NULL;
  }
}

inline void ObMysqlSM::remove_server_entry()
{
  if (OB_LIKELY(NULL != server_entry_)) {
    vc_table_.remove_entry(server_entry_);
    server_entry_ = NULL;
  }
}

inline void ObMysqlSM::add_history_entry(const char *fileline, const int event, const int32_t reentrant)
{
  int64_t pos = history_pos_++ % HISTORY_SIZE;

  history_[pos].file_line_ = fileline;
  history_[pos].event_ = event;
  history_[pos].reentrancy_ = reentrant;
}

inline void ObMysqlSM::txn_hook_append(const ObMysqlHookID id, ObContInternal *cont)
{
  api_.txn_hook_append(id, cont);
  hooks_set_ = true;
}

inline void ObMysqlSM::txn_hook_prepend(const ObMysqlHookID id, ObContInternal *cont)
{
  api_.txn_hook_prepend(id, cont);
  hooks_set_ = true;
}

inline ObAPIHook *ObMysqlSM::txn_hook_get(const ObMysqlHookID id)
{
  return api_.txn_hook_get(id);
}

inline int ObMysqlSM::get_mysql_schedule(int event, void *data)
{
  return api_.get_mysql_schedule(event, data);
}

inline void ObMysqlSM::set_client_connect_timeout()
{
  if (OB_LIKELY(NULL != client_session_)) {
    client_session_->set_connect_timeout();
  }
}

inline void ObMysqlSM::set_client_wait_timeout()
{
  if (OB_LIKELY(NULL != client_session_)) {
    client_session_->set_wait_timeout();
  }
}

inline void ObMysqlSM::set_client_net_read_timeout()
{
  if (OB_LIKELY(NULL != client_session_)) {
    client_session_->set_net_read_timeout();
  }
}

inline void ObMysqlSM::set_internal_cmd_timeout(const ObHRTime timeout)
{
  if (OB_LIKELY(NULL != client_session_)) {
    client_session_->set_inactivity_timeout(timeout);
  }
}

inline int64_t ObMysqlSM::get_query_timeout()
{
  int64_t timeout = HRTIME_NSECONDS(trans_state_.mysql_config_params_->observer_query_timeout_delta_);
  if (OB_LIKELY(NULL != client_session_)) {
    int64_t hint_query_timeout = trans_state_.trans_info_.client_request_.get_parse_result().get_hint_query_timeout();
    // if the request contains query_timeout in hint, we use it
    if (hint_query_timeout > 0) {
      // the query timeout in hint is in microseconds(us), so convert it into nanoseconds
      timeout += HRTIME_USECONDS(hint_query_timeout);
    } else {
      dbconfig::ObShardProp *shard_prop = client_session_->get_session_info().get_shard_prop();
      if (OB_NOT_NULL(shard_prop)) {
        timeout = HRTIME_MSECONDS(shard_prop->get_socket_timeout());
      } else {
        // we do parse in trans now, so we can use query_timeout in anycase
        timeout += client_session_->get_session_info().get_query_timeout();
      }
    }
  } else {}
  return timeout;
}

// the timeout of server processing request
inline void ObMysqlSM::set_server_query_timeout()
{
  if (OB_LIKELY(NULL != server_session_)) {
    server_session_->set_inactivity_timeout(get_query_timeout());
  }
}

inline void ObMysqlSM::cancel_server_query_timeout()
{
  if (OB_LIKELY(NULL != server_session_)) {
    server_session_->cancel_inactivity_timeout();
  }
}

inline void ObMysqlSM::set_server_trx_timeout()
{
  if (OB_LIKELY(NULL != server_session_) && OB_LIKELY(NULL != client_session_)) {
    server_session_->set_inactivity_timeout(client_session_->get_session_info().get_trx_timeout());
  }
}

inline void ObMysqlSM::set_server_quit_timeout()
{
  static const int64_t QUIT_TIMEOUT = HRTIME_MSECONDS(1);
  if (OB_LIKELY(NULL != server_session_)) {
    server_session_->set_inactivity_timeout(QUIT_TIMEOUT);
  }
}

inline void ObMysqlSM::clear_client_entry()
{
  if (NULL != client_session_) {
    vc_table_.cleanup_entry(client_entry_);
    client_entry_ = NULL;
    client_session_ = NULL;
  }
}

inline void ObMysqlSM::clear_server_entry()
{
  if (NULL != server_session_) {
    vc_table_.cleanup_entry(server_entry_);
    server_entry_ = NULL;
    server_session_ = NULL;
  }
}

inline void ObMysqlSM::clear_entries()
{
  PROXY_LOG(INFO, "ObMysqlSM::clear_entries", K_(client_entry), K(server_entry_), K_(sm_id));
  if (OB_LIKELY(NULL != client_entry_)) {
    if (OB_SUCCESS != vc_table_.cleanup_entry(client_entry_)) {
      PROXY_LOG(WARN, "vc_table failed to cleanup client entry", K_(client_entry), K_(sm_id));
    }
    if (OB_LIKELY(client_entry_->vc_ == NULL)) {
      client_session_ = NULL;       // vc is equivalent to session
    }
    client_entry_ = NULL;
  }

  if (OB_LIKELY(NULL != server_entry_)) {
    if (OB_SUCCESS != vc_table_.cleanup_entry(server_entry_)) {
      PROXY_LOG(WARN, "vc_table failed to cleanup client entry", K_(server_entry), K_(sm_id));
    }
    if (OB_LIKELY(server_entry_->vc_ == NULL)) {
      server_session_ = NULL;       // vc is equivalent to session
    }
    server_entry_ = NULL;
  }
}

inline void ObMysqlSM::consume_all_internal_data()
{
  int ret = OB_SUCCESS;
  // consume all data in internal reader, make sure will disconnect directly
  if (NULL != trans_state_.internal_reader_) {
    if (OB_FAIL(trans_state_.internal_reader_->consume_all())) {
      PROXY_LOG(WARN, "fail to consume all", K(ret));
    }
  }
}

inline void ObMysqlSM::callout_api_and_start_next_action(
    const ObMysqlTransact::ObStateMachineActionType api_next_action)
{
  trans_state_.api_next_action_ = api_next_action;
  if (hooks_set_ && !skip_plugin_) {
    api_.do_api_callout_internal();
  } else {
    handle_api_return();
  }
}

inline ObHRTime ObMysqlSM::get_based_hrtime()
{
  ObHRTime time = 0;
  if ((NULL != trans_state_.mysql_config_params_)
      && trans_state_.mysql_config_params_->enable_strict_stat_time_) {
    time = get_hrtime_internal();
  } else {
    time = event::get_hrtime();
  }
  return time;
}

inline bool ObMysqlSM::is_causal_order_read_enabled()
{
  return trans_state_.mysql_config_params_->enable_causal_order_read_
         && NULL != client_session_
         && client_session_->get_session_info().is_server_support_safe_read_weak();
}

inline bool ObMysqlSM::need_print_trace_stat() const
{
  //we may failed to parse or resolver, so it should not print when partition miss
  return (NULL != client_session_ && client_session_->need_print_trace_stat());
}

inline const common::ObString &ObMysqlSM::get_server_trace_id()
{
  ObMysqlResp &server_response = trans_state_.trans_info_.server_response_;
  return server_response.get_analyze_result().server_trace_id_;
}

struct ObMysqlSMListBucket
{
  common::ObPtr<event::ObProxyMutex> mutex_;
  ObDLList(ObMysqlSM, stat_link_) sm_list_;
};

extern ObMysqlSMListBucket g_mysqlsm_list[];

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_MYSQL_SM_H
