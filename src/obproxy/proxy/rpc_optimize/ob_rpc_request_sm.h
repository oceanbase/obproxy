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
#ifndef OBPROXY_RPC_REQUEST_HADNLE_H
#define OBPROXY_RPC_REQUEST_HADNLE_H

#include "iocore/eventsystem/ob_event_system.h"
#include "proxy/rpc_optimize/ob_rpc_req.h"
#include "proxy/mysqllib/ob_mysql_config_processor.h"
#include "proxy/rpc_optimize/ob_rpc_req_trace.h"
#include "proxy/rpc_optimize/rpclib/ob_rpc_req_analyzer.h"
#include "proxy/rpc_optimize/rpclib/ob_rpc_time_stat.h"
#include "proxy/route/ob_route_struct.h"
#include "proxy/route/ob_server_route.h"
#include "proxy/route/ob_ldc_location.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObAddr;
class ObString;
}
namespace obproxy
{
namespace obutils
{
class ObProxyConfigString;
class ObServerStateSimpleInfo;
class ObCongestionEntry;
}
namespace proxy
{
class ObTableEntry;
class ObRpcClientNetHandler;
struct ObProxyRpcReqAnalyzeCtx;

#define RPC_REQUEST_SM_DONE RPC_REQUEST_SM_EVENT_EVENTS_START + 1
#define RPC_REQUEST_SM_CALL_NEXT RPC_REQUEST_SM_EVENT_EVENTS_START + 2
#define RPC_REQUEST_SM_CLEANUP RPC_REQUEST_SM_EVENT_EVENTS_START + 3

enum ObRpcRequestSMMagic
{
  RPC_REQUEST_HANDLE_SM_MAGIC_ALIVE = 0x0000FEED,
  RPC_REQUEST_HANDLE_SM_MAGIC_DEAD = 0xDEADFEED
};

/**
 * @brief rpc路由策略: 当前默认为RPC_ROUTE_FIND_LEADER, 如果为弱读或者congestion流程失败，回退到RPC_ROUTE_NORMAL
 *         RPC_ROUTE_NORMAL:类似SQL流程路由方式，判断强读弱读、设置pllinfo、找可用的replica
 *         RPC_ROUTE_FIND_LEADER:由于大部分KV请求是强读并且只能发送到leader，这个mode是简化了路由流程，直接发送到leader
 */
enum ObRpcRouteMode
{
  RPC_ROUTE_NORMAL = 0,
  RPC_ROUTE_FIND_LEADER,
  RPC_ROUTE_MAX,
};

enum ObRpcRequestSMActionType
{
  RPC_REQ_NEW_REQUEST = 0,
  RPC_REQ_REQUEST_DIRECT_ROUTING,
  RPC_REQ_CTX_LOOKUP,
  RPC_REQ_IN_CLUSTER_BUILD,
  RPC_REQ_INNER_INFO_GET,
  RPC_REQ_QUERY_ASYNC_INFO_GET,
  RPC_REQ_IN_PARTITION_LOOKUP,
  RPC_REQ_PARTITION_LOOKUP_DONE,
  RPC_REQ_IN_INDEX_LOOKUP,
  RPC_REQ_IN_TABLEGROUP_LOOKUP,
  RPC_REQ_SERVER_ADDR_SEARCHING,
  RPC_REQ_SERVER_ADDR_SEARCHED,
  RPC_REQ_IN_CONGESTION_CONTROL_LOOKUP,
  RPC_REQ_CONGESTION_CONTROL_LOOKUP_DONE,
  RPC_REQ_REQUEST_REWRITE,
  RPC_REQ_REQUEST_SERVER_SENDING,
  RPC_REQ_PROCESS_RESPONSE,
  RPC_REQ_RESPONSE_REWRITE,
  RPC_REQ_INTERNAL_BUILD_RESPONSE,
  RPC_REQ_RESPONSE_CLIENT_SENDING,
  RPC_REQ_RESPONSE_SERVER_REROUTING,
  RPC_REQ_REQUEST_INNER_REQUEST_CLEANUP,
  RPC_REQ_REQUEST_CLEANUP,
  RPC_REQ_REQUEST_DONE,
  RPC_REQ_HANDLE_SHARD_REQUEST,
  RPC_REQ_HANDLE_SHARD_REQUEST_DONE,
  RPC_REQ_REQUEST_RETRY,
  RPC_REQ_REQUEST_ERROR,
  RPC_REQ_REQUEST_TIMEOUT, //later than server
  RPC_REQ_REQUEST_NEED_BE_CLEAN,
};

struct ObRpcReqStreamSizeStat
{
  ObRpcReqStreamSizeStat()
  {
    reset();
  }
  ~ObRpcReqStreamSizeStat() { }

  void reset()
  {
    memset(this, 0, sizeof(ObRpcReqStreamSizeStat));
  }

  int64_t to_string(char *buf, const int64_t buf_len) const;

  int64_t client_request_bytes_;
  int64_t server_request_bytes_;
  int64_t server_response_bytes_;
  int64_t client_response_bytes_;
};

struct ObRpcReqRetryStat
{
  ObRpcReqRetryStat()
  {
    reset();
  }
  ~ObRpcReqRetryStat() { }

  void reset()
  {
    memset(this, 0, sizeof(ObRpcReqRetryStat));
  }

  int64_t to_string(char *buf, const int64_t buf_len) const;

  int64_t request_retry_times_;
  int64_t request_move_reroute_times_;
  int64_t request_retry_consume_time_us_;
};

struct ObPartitionLookupInfo
{
public:
  enum ObForceRenewState
  {
    NO_NEED_RENEW = 0,
    PREPARE_RENEW,
    RENEW_DONE
  };

  ObPartitionLookupInfo()
      : lookup_success_(false), cached_dummy_entry_renew_state_(NO_NEED_RENEW),
        pl_attempts_(0), force_renew_state_(NO_NEED_RENEW), te_name_(), route_(),
        is_need_force_flush_(false)
  {
  }
  ~ObPartitionLookupInfo() {}

  void pl_update_if_necessary(const ObMysqlConfigParams &mysql_config_params);
  void renew_last_valid_time() { route_.renew_last_valid_time(); }
  bool need_update_entry() const { return route_.need_update_entry(); }
  bool need_update_entry_by_partition_hit() {
    return (need_update_entry() && !route_.is_dummy_table());
  }
  bool is_partition_table() const { return route_.is_partition_table(); }
  bool is_all_iterate_once() const { return route_.is_all_iterate_once(); }
  bool is_leader_existent() const { return route_.is_leader_existent(); }
  bool is_leader_server() const { return route_.is_leader_server(); }
  bool is_target_location_server() const { return route_.is_target_location_server(); }
  bool is_server_from_rslist() const { return route_.is_server_from_rslist(); }
  bool is_no_route_info_found() const { return route_.is_no_route_info_found(); }
  int64_t get_last_valid_time_us() const { return route_.get_last_valid_time_us(); }
  int64_t replica_size() const { return route_.replica_size(); }
  common::ObConsistencyLevel get_consistency_level() const { return route_.get_consistency_level(); }
  bool is_strong_read() const { return route_.is_strong_read(); }
  bool is_weak_read() const { return route_.is_weak_read(); }
  ObTableEntry *get_dummy_entry() { return route_.get_dummy_entry(); }
  void set_route_info(ObMysqlRouteResult &result) { route_.set_route_info(result); }
  bool set_target_dirty() { return route_.set_target_dirty(); }
  bool set_dirty_all(bool is_need_force_flush = false) { return route_.set_dirty_all(is_need_force_flush); }
  bool set_delay_update() { return route_.set_delay_update(); }
  bool need_update_dummy_entry() const { return route_.need_update_dummy_entry(); }
  void set_need_force_flush(bool is_need_force_flush) { is_need_force_flush_ = is_need_force_flush; }
  bool is_need_force_flush() { return is_need_force_flush_; }
  bool is_remote_readonly() { return route_.is_remote_readonly(); }

  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_OBJ_START();
    J_KV(K_(pl_attempts),
         K_(lookup_success),
         K_(force_renew_state),
         K_(cached_dummy_entry_renew_state),
         K_(te_name),
         K_(route));
    J_OBJ_END();
    return pos;
  }

  const ObProxyReplicaLocation *get_next_avail_replica(const bool is_force_retry,
      int32_t &attempt_count, bool &found_leader_force_congested)
  {
    const ObProxyReplicaLocation *ret_replica = NULL;
    attempt_count = 0;
    bool need_try_next = true;
    found_leader_force_congested = false;
    while (need_try_next) {
      ret_replica = route_.get_next_avail_replica();
      if (!is_force_retry
          && NULL != ret_replica
          && route_.cur_chosen_server_.is_force_congested_) {
        if (ret_replica->is_leader() && is_strong_read()) {
          found_leader_force_congested = true;
        }
        PROXY_TXN_LOG(DEBUG, "this is force congested server, do not use it this time",
                      "server", route_.cur_chosen_server_, K(attempt_count));
        //if not force retry, we will do not use dead congested server
        ret_replica = NULL;
        ++attempt_count;
      } else {
        need_try_next = false;
      }
    }
    return ret_replica;
  }

  const ObProxyReplicaLocation *get_next_avail_replica()
  {
    return route_.get_next_avail_replica();
  }
  const ObProxyReplicaLocation *get_next_replica(const uint32_t cur_ip, const uint16_t cur_port,
      const bool is_force_retry)
  {
    return route_.get_next_replica(cur_ip, cur_port, is_force_retry);
  }
  const ObProxyReplicaLocation *get_leader_replica_from_remote()
  {
    return route_.get_leader_replica_from_remote();
  }
  void reset_pl();
  void reset();

  // if pl has force renewed, don't renew twice
  void set_force_renew()
  {
    if (NO_NEED_RENEW == force_renew_state_) {
      force_renew_state_ = PREPARE_RENEW;
    }
  }
  bool is_force_renew() const { return PREPARE_RENEW == force_renew_state_; }
  void set_force_renew_done() { force_renew_state_ = RENEW_DONE; }

  // if cached dummy entry renewed, don't renew twice
  void set_cached_dummy_force_renew()
  {
    if (NO_NEED_RENEW == cached_dummy_entry_renew_state_) {
      cached_dummy_entry_renew_state_ = PREPARE_RENEW;
    }
  }
  bool is_cached_dummy_force_renew() const { return PREPARE_RENEW == cached_dummy_entry_renew_state_; }
  bool is_cached_dummy_avail_force_renew() const { return NO_NEED_RENEW == cached_dummy_entry_renew_state_; }
  void set_cached_dummy_force_renew_done() { cached_dummy_entry_renew_state_ = RENEW_DONE; }

public:
  bool lookup_success_;
  ObForceRenewState cached_dummy_entry_renew_state_;
  int64_t pl_attempts_;
  ObForceRenewState force_renew_state_;
  ObTableEntryName te_name_;
  ObServerRoute route_;
  bool is_need_force_flush_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPartitionLookupInfo);
};

inline void ObPartitionLookupInfo::reset()
{
  route_.reset();
  lookup_success_ = false;
  pl_attempts_= 0;
  force_renew_state_ = NO_NEED_RENEW;
  cached_dummy_entry_renew_state_ = NO_NEED_RENEW;
  te_name_.reset();
  is_need_force_flush_ = false;
}

// class ObRpcReqTraceId;
class ObRpcReq;
class ObRpcRequestSM;
class ObRpcRequestConfigInfo;
typedef int (ObRpcRequestSM::*ObRpcRequestSMHandler)(int event, void *data);

class ObRpcRequestSM : public event::ObContinuation
{
  enum ObAttachDummyEntryType
  {
    NO_TABLE_ENTRY_FOUND_ATTACH_TYPE = 0,
  };
public:
  ObRpcRequestSM();
  virtual ~ObRpcRequestSM() {}

  void cleanup();
  void inner_request_cleanup();   //Called when the subtask ends to clean up all subtask data
  void destroy();
  void kill_this();

  static ObRpcRequestSM *allocate();
  static void instantiate_func(ObRpcRequestSM &prototype, ObRpcRequestSM &new_instance);
  static void make_scatter_list(ObRpcRequestSM &prototype);

  int init(ObRpcReq *rpc_req, event::ObProxyMutex *mutex = NULL);
  int init_inner_request(event::ObContinuation *inner_cont, event::ObProxyMutex *mutex);
  static uint32_t get_next_sm_id();
  // Debugging routines to dump the SM history
  void dump_history_state();
  void add_history_entry(const char *fileline, const int event, const int32_t reentrant);
  void refresh_mysql_config();
  ObHRTime get_based_hrtime();
  static ObHRTime static_get_based_hrtime();
  int init_rpc_request_content(ObProxyRpcReqAnalyzeCtx &ctx);

  void set_rpc_req(ObRpcReq *req) { rpc_req_ = req;}
  ObRpcReq *get_rpc_req() { return rpc_req_; }

  void set_rpc_timeout_us(int64_t timeout) { timeout_us_ = timeout;}
  int64_t get_rpc_timeout_us() { return timeout_us_; }

  void set_retry_need_update_pl(bool retry_need_update_pl) { retry_need_update_pl_ = retry_need_update_pl; }
  bool get_retry_need_update_pl() { return retry_need_update_pl_; }

  int process_request(ObRpcReq *req);
  int process_response(ObRpcReq *req);

  int handle_server_failed();
  int process_partition_location(ObMysqlRouteResult &result);

  int analyze_request(ObRpcReqAnalyzeNewStatus &status);
  int analyze_response(ObRpcReqAnalyzeNewStatus &status);
  int handle_rpc_request(bool &need_response_for_stmt, bool &need_wait_callback);
  int handle_obkv_request(bool &need_response_for_stmt, bool &need_wait_callback);
  int handle_rpc_response(bool &need_rewrite, bool &need_retry);
  int handle_obkv_response(bool &need_rewrite, bool &need_retry);
  int handle_obkv_table_query_async_response(bool &need_rewrite, bool &need_retry);
  int handle_obkv_login_response(bool &need_rewrite, bool &need_retry);
  
  int setup_obrpc_req_inner_info_get();

  int setup_process_request();
  int setup_process_response();
  int setup_rpc_get_cluster();
  int setup_req_inner_info_get();
  int setup_table_query_async_info_get();
  int setup_rpc_req_ctx_lookup();
  int setup_rpc_partition_lookup();
  int setup_rpc_index_lookup();
  int setup_rpc_tablegroup_lookup();
  int setup_congestion_control_lookup();
  int setup_rpc_server_addr_search_normal();
  int setup_rpc_server_addr_direct_find_leader(bool &need_retry_with_normal_mode);
  int setup_rpc_server_addr_searching();
  int setup_rpc_request_server_reroute();
  int setup_rpc_request_direct_load_route();
  int setup_rpc_handle_shard_request();
  int setup_rpc_send_request_to_server();
  int setup_rpc_send_response_to_client();
  int setup_rpc_request_retry();
  int setup_rpc_return_error();
  int setup_rpc_internal_build_response();

  int state_rpc_get_cluster(int event, void *data);
  int state_table_query_async_info_get(int event, void *data);
  int state_rpc_partition_lookup(int event, void *data);
  int state_rpc_req_ctx_lookup(int event, void *data);
  int state_rpc_index_lookup(int event, void *data);
  int state_rpc_tablegroup_lookup(int event, void *data);
  int state_rpc_handle_shard_request(int event, void *data);
  int state_congestion_control_lookup(int event, void *data);
  int state_rpc_get_cluster_done();
  int state_rpc_index_lookup_done();
  int state_rpc_partition_lookup_done();
  int state_congestion_control_lookup_done();
  int state_rpc_server_addr_searched();
  int state_rpc_server_request_rewrite();
  int state_rpc_client_response_rewrite();
  int state_req_inner_info_getted();
  int state_rpc_send_request_done();
  int state_rpc_req_done();
  int state_rpc_req_cleanup();
  int state_rpc_req_inner_request_cleanup();

  int inner_request_callback(int event);

  int main_handler(int event, void *data);

  // cluster info
  int set_cluster_info(const bool enable_cluster_checkout,
    const ObString &cluster_name, const obutils::ObProxyConfigString &real_meta_cluster_name,
    const int64_t cluster_id, bool &need_delete_cluster);
  void free_real_meta_cluster_name();
  int process_cluster_resource(void *data);
  int fill_pll_tname();

  bool is_inner_request() const ;
  bool is_need_convert_vip_to_tname() const ;
  bool is_valid_rpc_req() const ;
  ObConsistencyLevel get_trans_consistency_level();
  ObRoutePolicyEnum get_route_policy(const bool need_use_dup_replica);
  void get_route_policy(ObProxyRoutePolicyEnum policy, ObRoutePolicyEnum& ret_policy) const;
  ObString get_current_idc_name() const;
  void get_region_name_and_server_info(ObIArray<obutils::ObServerStateSimpleInfo> &simple_servers_info, ObIArray<ObString> &region_names);
  void handle_congestion_entry_not_exist();

  void set_state_and_call_next(enum ObRpcRequestSMActionType state);
  void set_next_state(enum ObRpcRequestSMActionType state);
  void call_next_action();

  void set_execute_thread(event::ObEThread *execute_thread) { execute_thread_ = execute_thread; }

  void set_rpc_req_origin_channel_id(uint32_t id) { rpc_req_origin_channel_id_ = id; }
  uint32_t get_rpc_req_origin_channel_id() { return rpc_req_origin_channel_id_; }

  bool check_connection_throttle();
  int analyze_rpc_login_request(ObProxyRpcReqAnalyzeCtx &ctx, ObRpcReqAnalyzeNewStatus &status);
  int keep_dummy_entry_and_update_ldc(ObMysqlRouteResult &result);
  int get_cached_cluster_resource();
  int update_cached_cluster_resource();
  int get_cached_dummy_entry_and_ldc();
  int update_cached_dummy_entry_and_ldc();
  int get_cached_config_info();
  int update_cached_config_info();

  bool retry_server_connection_not_open(); //found next addr or false
  void retry_reset();

  int dirty_rpc_route_result(ObMysqlRouteResult *result);

  int calc_rpc_timeout_us();
  int schedule_timeout_action();
  int cancel_pending_action();
  int cancel_timeout_action();

  int schedule_cleanup_action();
  int cancel_cleanup_action();
  // If t is 0, call schedule_imm, if not, call schedule_in
  int schedule_call_next_action(enum ObRpcRequestSMActionType, const ObHRTime t = 0);
  int cancel_call_next_action();

  int cancel_sharding_action();

  int get_proxy_primary_zone_array(common::ObString zone, common::ObSEArray<common::ObString, 5> &zone_array);

  int handle_global_index_partition_lookup_done();
  void handle_timeout();
  void set_inner_cont(event::ObContinuation *cont) { inner_cont_ = cont; }
  ObRpcReqCmdTimeStat &get_cmd_time_stat() { return cmd_time_stats_; }
  void update_cmd_stats();
  void update_monitor_log();
  void get_monitor_error_info(int32_t &error_code, ObString &error_msg, bool &is_error_resp);
  void update_monitor_stats(const ObString &logic_tenant_name,
                            const ObString &logic_database_name,
                            const ObString &cluster,
                            const ObString &tenant,
                            const ObString &database,
                            const common::DBServerType database_type,
                            const ObProxyBasicStmtType stmt_type,
                            char *error_code_str);
  bool need_reject_user_login(const common::ObString &user,
                            const common::ObString &tenant,
                            const bool has_tenant_username,
                            const bool has_cluster_username,
                            const bool is_cloud_user) const;
  int check_user_identity(const ObString &user_name,
                          const ObString &tenant_name,
                          const ObString &cluster_name,
                          const bool has_tenant_username,
                          const bool has_cluster_username);

  int init_request_meta_info();
  void refresh_rpc_request_config();
  int get_config_item(const common::ObString& cluster_name,
                        const common::ObString &tenant_name,
                        const obutils::ObVipAddr &addr,
                        ObRpcRequestConfigInfo &req_config_info,
                        const int64_t global_version = 0);

  void init_rpc_trace_id();
  static ObProxyBasicStmtType get_rpc_basic_stmt_type(const obrpc::ObRpcPacketCode pcode);
  void set_sm_terminate() { terminate_sm_ = true; }

  event::ObProxyMutex *lock_for_inner_request()
  {
    return inner_request_cleanup_mutex_;
  }
public:
  static const int64_t MAX_SCATTER_LEN;
private:
  static const int64_t HISTORY_SIZE = 32;

  uint32_t sm_id_;
  ObRpcRequestSMMagic magic_;
  enum ObRpcRequestSMActionType next_action_;

  struct ObHistory
  {
    const char *file_line_;
    int event_;
    int32_t reentrancy_;
  };
  ObHistory history_[HISTORY_SIZE];
  uint32_t history_pos_;
  // LINK(ObRpcRequestSM, stat_link_);

  ObRpcRequestSMHandler default_handler_;
  event::ObAction *pending_action_;
  event::ObAction *timeout_action_;
  event::ObAction *cleanup_action_;
  event::ObAction *sharding_action_;
  event::ObAction *sm_next_action_;
  event::ObContinuation *inner_cont_;
  int32_t reentrancy_count_;

  bool terminate_sm_;
  ObRpcReq *rpc_req_;
  uint32_t rpc_req_origin_channel_id_;
  event::ObEThread *create_thread_;
  event::ObEThread *execute_thread_;   // sub rpc req mayce execute in another thread

  // Stats
  ObRpcReqStreamSizeStat cmd_size_stats_;
  ObRpcReqRetryStat cmd_retry_stats_;
  ObRpcReqCmdTimeStat cmd_time_stats_;
  ObRpcMilestones milestones_;
  ObMysqlConfigParams *mysql_config_params_;

  obutils::ObClusterResource *cluster_resource_;
  common::ObString real_meta_cluster_name_;
  char *real_meta_cluster_name_str_;
  int64_t cluster_id_;
  int64_t timeout_us_;
  bool retry_need_update_pl_;
  bool need_pl_lookup_;
  bool need_congestion_lookup_;
  bool force_retry_congested_;
  bool congestion_lookup_success_;
  bool is_congestion_entry_updated_;
  bool already_get_async_info_;
  bool already_get_index_entry_;
  bool already_get_tablegroup_entry_;
  int32_t congestion_entry_not_exist_count_;
  obutils::ObCongestionEntry *congestion_entry_;
  ObPartitionLookupInfo pll_info_;  // partition lookup info
  ObRpcReqTraceId rpc_trace_id_;

  ObRpcRouteMode rpc_route_mode_;

  common::ObPtr<event::ObProxyMutex> inner_request_cleanup_mutex_;
public:
  ObRouteDiagnosis *route_diagnosis_;
  obutils::ObConnectionDiagnosisTrace *connection_diagnosis_trace_;

  friend class ObRpcReq;
};

inline ObRpcRequestSM *ObRpcRequestSM::allocate()
{
  return op_thread_alloc_init(ObRpcRequestSM, event::get_rpc_request_handle_sm_allocator(), ObRpcRequestSM::instantiate_func);
}

inline uint32_t ObRpcRequestSM::get_next_sm_id()
{
  static volatile uint32_t next_sm_id = 0;
  return ATOMIC_AAF((&next_sm_id), 1);
}

inline int64_t milestone_diff_new(const ObHRTime start, const ObHRTime end)
{
  return (start > 0 && end > start) ? (end - start) : 0;
}

inline ObHRTime ObRpcRequestSM::static_get_based_hrtime()
{
  ObHRTime time = 0;
  if (obutils::get_global_proxy_config().enable_strict_stat_time) {
    time = get_hrtime_internal();
  } else {
    time = event::get_hrtime();
  }
  return time;
}

/* just used in monitor log for rpc request which add it into obproxy_stat.log */
inline ObProxyBasicStmtType ObRpcRequestSM::get_rpc_basic_stmt_type(const obrpc::ObRpcPacketCode pcode)
{
  ObProxyBasicStmtType stmt_type = OBRPC_INVALID;
  switch (pcode)
  {
  case obrpc::OB_TABLE_API_LOGIN:
    stmt_type = OBRPC_OBKV_TABLE_API_LOGIN;
    break;
  case obrpc::OB_TABLE_API_EXECUTE:
    stmt_type = OBRPC_OBKV_TABLE_API_EXECUTE;
    break;
  case obrpc::OB_TABLE_API_BATCH_EXECUTE:
    stmt_type = OBRPC_OBKV_TABLE_API_BATCH_EXECUTE;
    break;
  case obrpc::OB_TABLE_API_EXECUTE_QUERY:
    stmt_type = OBRPC_OBKV_TABLE_API_EXECUTE_QUERY;
    break;
  case obrpc::OB_TABLE_API_QUERY_AND_MUTATE:
    stmt_type = OBRPC_OBKV_TABLE_API_QUERY_AND_MUTATE;
    break;
  case obrpc::OB_TABLE_API_EXECUTE_QUERY_SYNC:
    stmt_type = OBRPC_OBKV_TABLE_API_EXECUTE_QUERY_SYNC;
    break;
  case obrpc::OB_TABLE_API_DIRECT_LOAD:
    stmt_type = OBRPC_OBKV_TABLE_API_DIRECT_LOAD;
    break;
  case obrpc::OB_TABLE_API_MOVE:
    stmt_type = OBRPC_OBKV_TABLE_API_MOVE;
    break;
  case obrpc::OB_TABLE_API_LS_EXECUTE:
    stmt_type = OBRPC_OBKV_TABLE_API_LS_EXECUTE;
    break;
  default:
    //OBRPC_INVALID
    break;
  }
  return stmt_type;
}

}
}
}

#endif