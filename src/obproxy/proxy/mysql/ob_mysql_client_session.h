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

#ifndef OBPROXY_MYSQL_CLIENT_SESSION_H
#define OBPROXY_MYSQL_CLIENT_SESSION_H

#include "stat/ob_proxy_trace_stats.h"
#include "stat/ob_mysql_stats.h"
#include "obutils/ob_proxy_sql_parser.h"
#include "obutils/ob_vip_tenant_processor.h"
#include "proxy/mysqllib/ob_mysql_config_processor.h"
#include "proxy/mysql/ob_proxy_client_session.h"
#include "proxy/mysql/ob_mysql_session_manager.h"
#include "proxy/mysql/ob_prepare_statement_struct.h"
#include "proxy/route/ob_table_entry.h"
#include "proxy/route/ob_ldc_location.h"
#include "rpc/obmysql/packet/ompk_handshake.h"
#include "optimizer/ob_sharding_select_log_plan.h"
#include "optimizer/ob_proxy_optimizer_processor.h"
#include "iocore/net/ob_unix_net_vconnection.h"
#include "ob_proxy_init.h"
#include "obutils/ob_connection_diagnosis_trace.h"
#include "rpc/proxy_protocol/proxy_protocol_v2.h"
#include "lib/hash/ob_hashset.h"
#include "lib/list//ob_list.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{
class ObEThread;
}
namespace obutils
{
class ObClusterResource;
}
namespace proxy
{
#define CLIENT_SESSION_ERASE_FROM_MAP_EVENT (CLIENT_SESSION_EVENT_EVENTS_START + 1)
#define CLIENT_SESSION_ACQUIRE_SERVER_SESSION_EVENT (CLIENT_SESSION_EVENT_EVENTS_START + 2)

extern ObMutex g_debug_cs_list_mutex;

class ObMysqlRequestParam;
class ObMysqlSM;
class ObMysqlClientSession : public ObProxyClientSession
{
public:

  struct ObConnTenantInfo
  {
    ObConnTenantInfo() : lookup_success_(false), vip_tenant_(), client_addr_() {}
    ~ObConnTenantInfo() { }

    void reset() {
      lookup_success_ = false;
      vip_tenant_.reset();
      client_addr_.reset();
      // slb_addr_.reset();
    }

    bool lookup_success_;
    obutils::ObVipTenant vip_tenant_;
    common::ObAddr client_addr_; // the client ip addr
    // common::ObAddr slb_addr_;    // SLB ip addr
  };

  ObMysqlClientSession();
  virtual ~ObMysqlClientSession() {}

  // Implement ObProxyClientSession interface.
  virtual void destroy();

  virtual int start() { return new_transaction(true); }

  int new_connection(net::ObNetVConnection *new_vc, event::ObMIOBuffer *iobuf,
                     event::ObIOBufferReader *reader);

  int new_connection(net::ObNetVConnection *new_vc, event::ObMIOBuffer *iobuf,
                     event::ObIOBufferReader *reader, obutils::ObClusterResource *cluster_resource);

  int new_connection(net::ObNetVConnection *new_vc, event::ObMIOBuffer *iobuf,
                     event::ObIOBufferReader *reader, dbconfig::ObShardConnector *shard_conn,
                     dbconfig::ObShardProp *shard_prop);

  int get_vip_info();

  // Implement ObVConnection interface.
  virtual event::ObVIO *do_io_read(event::ObContinuation *c,
                                   const int64_t nbytes = INT64_MAX,
                                   event::ObMIOBuffer *buf = 0);
  virtual event::ObVIO *do_io_write(event::ObContinuation *c = NULL,
                                    const int64_t nbytes = INT64_MAX,
                                    event::ObIOBufferReader *buf = 0);

  int main_handler(int event, void *data);

  virtual void do_io_close(const int lerrno = -1);
  virtual void do_io_shutdown(const event::ShutdownHowToType howto);
  virtual void reenable(event::ObVIO *vio);

  int new_transaction(const bool is_new_conn = false);
  void handle_transaction_complete(event::ObIOBufferReader *r, bool &close_cs);

  void set_half_close_flag() { half_close_ = true; }
  void clear_half_close_flag() { half_close_ = false; }
  bool get_half_close_flag() const { return half_close_; }
  virtual int release(event::ObIOBufferReader *r);
  net::ObNetVConnection *get_netvc() const { return client_vc_; }
  bool is_local_connection() const { return is_local_connection_; }
  void set_local_connection();
  common::ObAddr get_real_client_addr(net::ObNetVConnection *server_vc = NULL);

  virtual int attach_server_session(ObMysqlServerSession *ssession);
  int acquire_svr_session(const sockaddr &addr, const bool need_close_last_ss, ObMysqlServerSession *&svr_session);
  int init_session_pool_info();
  int acquire_svr_session_in_session_pool(const sockaddr &addr, ObMysqlServerSession *&svr_session);
  int acquire_svr_session_no_pool(const sockaddr &addr, ObMysqlServerSession *&svr_session);
  int64_t get_svr_session_count() const;

  inline ObMysqlServerSession *get_server_session() const { return bound_ss_; }
  inline ObMysqlServerSession *get_cur_server_session() const { return cur_ss_; }
  inline ObMysqlServerSession *get_lii_server_session() const { return lii_ss_; }
  inline ObMysqlServerSession *get_last_bound_server_session() const { return last_bound_ss_; }
  inline net::ObIpEndpoint &get_trans_coordinator_ss_addr() { return trans_coordinator_ss_addr_; }
  inline void set_server_session(ObMysqlServerSession *ssession) { bound_ss_ = ssession; }
  inline void set_cur_server_session(ObMysqlServerSession *ssession) { cur_ss_ = ssession; }
  inline void set_lii_server_session(ObMysqlServerSession *ssession) { lii_ss_ = ssession; }
  inline void set_last_bound_server_session(ObMysqlServerSession *ssession) { last_bound_ss_ = ssession; }
  inline void set_trans_coordinator_ss_addr(const sockaddr &addr) { trans_coordinator_ss_addr_.assign(addr); }
  bool is_hold_conn_id(const uint32_t conn_id);

  // Functions for manipulating api hooks
  int ssn_hook_append(const ObMysqlHookID id, ObContInternal *cont);
  int ssn_hook_prepend(const ObMysqlHookID id, ObContInternal *cont);

  int64_t get_transact_count() const { return session_stats_.stats_[TOTAL_TRANSACTION_COUNT]; }
  ObClientSessionInfo &get_session_info() { return session_info_; }
  const ObClientSessionInfo &get_session_info() const { return session_info_; }
  ObMysqlSessionManagerNew &get_session_manager_new() { return session_manager_new_; }
  const ObMysqlSessionManagerNew &get_session_manager_new() const { return session_manager_new_; }
  ObMysqlSessionManager &get_session_manager() { return session_manager_; }
  const ObMysqlSessionManager &get_session_manager() const { return session_manager_; }
  const char *get_read_state_str() const;
  const common::ObString &get_vip_tenant_name() { return ct_info_.vip_tenant_.tenant_name_; }
  const common::ObString &get_vip_cluster_name() { return ct_info_.vip_tenant_.cluster_name_; }
  const common::ObString get_vip_vpc_info()
  {
    ObString ret_str;
    ret_str.assign_ptr(ct_info_.vip_tenant_.vip_addr_.vpc_info_.ptr(),
            static_cast<int>(ct_info_.vip_tenant_.vip_addr_.vpc_info_.len()));
    return ret_str;
  }
  bool is_vip_lookup_success() const { return ct_info_.lookup_success_; }
  int64_t to_string(char *buf, const int64_t buf_len) const;

  //proxy inner mysql_client do need convert vip to tenant
  bool is_need_convert_vip_to_tname();

  // client cap judgement
  bool is_client_support_full_link_trace() const { return session_info_.is_client_support_full_link_trace(); }
  bool is_client_support_new_extra_info() const { return session_info_.is_client_support_new_extra_info(); }
  bool is_client_support_full_link_trace_ext() const {
    return session_info_.is_client_support_full_link_trace_ext();
  }

  int64_t get_current_tid() const { return current_tid_; }

  uint32_t &get_cs_id_ref() { return cs_id_; }
  uint32_t get_cs_id() const { return cs_id_; }
  uint64_t get_proxy_sessid() const { return proxy_sessid_; }
  void set_proxy_sessid(uint64_t sessid) { proxy_sessid_ = sessid; }
  int create_scramble();
  common::ObString &get_scramble_string() { return session_info_.get_scramble_string(); }
  ObString get_current_idc_name() const;
  int check_update_ldc();
  bool need_print_trace_stat() const;

  // generate a cs_id, depends on proxy_id
  static int get_thread_init_cs_id(const ObClientSessionIDVersion version, uint32_t &thread_init_cs_id, uint32_t &max_local_seq, const int64_t thread_id = -1);
  int acquire_client_session_id(const ObClientSessionIDVersion version);
  static uint64_t get_next_proxy_sessid();

  int add_to_list();
  void handle_new_connection();
  int fill_session_priv_info();

  bool is_authorised_proxysys(const ObProxyLoginUserType type);

  int fill_tenant_info_with_ppv2(proxy_protocol_v2::ProxyProtocolV2 &v2);

  common::ObString &get_login_packet();

  struct ObSessionStats
  {
    ObSessionStats() : modified_time_(0), reported_time_(0), is_first_register_(true)
    {
      memset(stats_, 0, sizeof(stats_));
    }
    ~ObSessionStats() { }

    int64_t to_string(char *buf, const int64_t buf_len) const;

    int64_t stats_[SESSION_STAT_COUNT];
    ObHRTime modified_time_;
    ObHRTime reported_time_;
    bool is_first_register_;//we need sync all stats when first_register_succ_ is false
  };

  enum ObInListStat
  {
    LIST_INIT = 0,
    LIST_ADDED,
    LIST_REMOVED,
  };

  ObSessionStats &get_session_stats() { return session_stats_; }
  const ObSessionStats &get_session_stats() const { return session_stats_; }
  ObTraceStats *&get_trace_stats() { return trace_stats_; }
  const ObTraceStats *get_trace_stats() const { return trace_stats_; }

  // get timeout
  // get session timeout and convert it to ns according the time_unit given
  // params: @@timeout_name :the name of timeout session variable name(wait_timeout, etc.)
  //         @@time_unit    :the time unit of the timeout (second, microsecond, etc.)
  // return: return session timeout in nanoseconds,
  //         if the timeout is 0 or we get session variable failed return default_inactivity_timeout
  int64_t get_session_timeout(const char *timeout_name, ObHRTime time_unit) const;
  int64_t get_connect_timeout() const { return HRTIME_SECONDS(10); } // defualt connect timeout is 10s

  // set timeout
  // set inactivity to the value timeout(in nanoseconds)
  void set_inactivity_timeout(const ObHRTime timeout, obutils::ObInactivityTimeoutEvent event);
  void set_connect_timeout() {
    set_inactivity_timeout(get_connect_timeout(), obutils::OB_CLIENT_CONNECT_TIMEOUT);
    #ifdef ERRSIM
    int ret = OB_SUCCESS;
    if (OB_FAIL(OB_E(EventTable::EN_CLIENT_CONNECT_TIMEOUT) OB_SUCCESS)) {
      set_inactivity_timeout(1, obutils::OB_CLIENT_CONNECT_TIMEOUT);
    }
    #endif
  }
  void set_wait_timeout()
  {
    set_inactivity_timeout(session_info_.get_wait_timeout(), obutils::OB_CLIENT_WAIT_TIMEOUT);
    #ifdef ERRSIM
    int ret = OB_SUCCESS;
    if (OB_FAIL(OB_E(EventTable::EN_CLIENT_WAIT_TIMEOUT) OB_SUCCESS)) {
      set_inactivity_timeout(1, obutils::OB_CLIENT_WAIT_TIMEOUT);
    }
    #endif
  }
  void set_net_write_timeout()
  {
    set_inactivity_timeout(session_info_.get_net_write_timeout(), obutils::OB_CLIENT_NET_WRITE_TIMEOUT);
    #ifdef ERRSIM
    int ret = OB_SUCCESS;
    if (OB_FAIL(OB_E(EventTable::EN_CLIENT_NET_WRITE_TIMEOUT) OB_SUCCESS)) {
      set_inactivity_timeout(1, obutils::OB_CLIENT_NET_WRITE_TIMEOUT);
    }
    #endif
  }
  void set_net_read_timeout()
  {
    set_inactivity_timeout(session_info_.get_net_read_timeout(),obutils::OB_CLIENT_NET_READ_TIMEOUT);
    #ifdef ERRSIM
    int ret = OB_SUCCESS;
    if (OB_FAIL(OB_E(EventTable::EN_CLIENT_NET_READ_TIMEOUT) OB_SUCCESS)) {
      set_inactivity_timeout(1, obutils::OB_CLIENT_NET_READ_TIMEOUT);
    }
    #endif
  }
  bool is_in_trans() { return !is_waiting_trans_first_request_; }

  void cancel_inactivity_timeout();

  int reset_read_buffer();
  event::ObIOBufferReader *get_reader() { return buffer_reader_; }
  event::ObEThread *get_create_thread() { return create_thread_; }

  int64_t get_cluster_id() const { return session_info_.get_cluster_id(); }
  const common::ObString &get_real_cluster_name() const
  {
    // if this is MetaDataBase cluster, return its real cluster name,
    // otherwise, return its cluster name
    return (session_info_.get_priv_info().cluster_name_ == OB_META_DB_CLUSTER_NAME
            ? session_info_.get_meta_cluster_name()
            : session_info_.get_priv_info().cluster_name_);
  }

  int swap_mutex(void *data);
  void close_last_used_ss();

  void set_need_delete_cluster() { need_delete_cluster_ = true; }
  void set_proxy_mysql_client() { is_proxy_mysql_client_ = true; }
  void set_can_send_request() { can_direct_send_request_ = true; }
  void set_session_pool_client(bool is_session_pool_client) {
    session_info_.is_session_pool_client_ = is_session_pool_client;
  }
  inline bool is_session_pool_client() { return session_info_.is_session_pool_client_; }
  void set_server_addr(proxy::ObCommonAddr addr) {common_addr_ = addr;}
  void set_first_dml_sql_got() { is_first_dml_sql_got_ = true; }
  bool is_first_dml_sql_got() const { return is_first_dml_sql_got_; }

  uint8_t get_compressed_seq() const { return compressed_seq_; }
  void set_compressed_seq(const uint8_t seq) { compressed_seq_ = seq; }

  void set_need_send_trace_info(bool is_need_send_trace_info) { is_need_send_trace_info_ = is_need_send_trace_info; }
  bool is_need_send_trace_info() const { return is_need_send_trace_info_; }
  void set_already_send_trace_info(bool is_already_send_trace_info) { is_already_send_trace_info_ = is_already_send_trace_info; }
  bool is_already_send_trace_info() const { return is_already_send_trace_info_; }

  void set_first_handle_request(bool is_first_handle_request) { is_first_handle_request_ = is_first_handle_request; }
  bool is_first_handle_request() const { return is_first_handle_request_; }
  void set_in_trans_for_close_request(bool is_in_trans_for_close_request) { is_in_trans_for_close_request_ = is_in_trans_for_close_request; }
  bool is_in_trans_for_close_request() const { return is_in_trans_for_close_request_; }
  void set_last_request_in_trans(bool is_in_trans) { is_last_request_in_trans_ = is_in_trans; }
  bool is_last_request_in_trans() { return is_last_request_in_trans_; }
  void set_trans_internal_routing(bool is_internal_routing) { is_trans_internal_routing_ = is_internal_routing; }
  bool is_trans_internal_routing() const { return is_trans_internal_routing_; }
  void set_need_return_last_bound_ss(bool is_need_return_last_bound_ss) { is_need_return_last_bound_ss_ = is_need_return_last_bound_ss; }
  bool is_need_return_last_bound_ss() const { return is_need_return_last_bound_ss_; }
  void set_proxy_enable_trans_internal_routing(bool is_enable_internal_route) { is_proxy_enable_trans_internal_routing_ = is_enable_internal_route; }
  bool is_proxy_enable_trans_internal_routing() const { return is_proxy_enable_trans_internal_routing_; }
  bool enable_analyze_internal_cmd() const { return session_info_.enable_analyze_internal_cmd(); }
  bool is_metadb_user() const { return session_info_.is_metadb_user(); }
  bool is_proxysys_user() const { return session_info_.is_proxysys_user(); }
  bool is_rootsys_user() const { return session_info_.is_rootsys_user(); }
  bool is_proxyro_user() const { return session_info_.is_proxyro_user(); }
  bool is_inspector_user() const { return session_info_.is_inspector_user(); }
  bool is_proxysys_tenant() const { return session_info_.is_proxysys_tenant(); }
  ObProxyLoginUserType get_user_identity() const { return session_info_.get_user_identity(); }
  void set_user_identity(const ObProxyLoginUserType identity) { session_info_.set_user_identity(identity); }
  void set_conn_prometheus_decrease(bool conn_prometheus_decrease) { conn_prometheus_decrease_ = conn_prometheus_decrease; }
  void set_vip_connection_decrease(bool vip_connection_decrease) { vip_connection_decrease_ = vip_connection_decrease; }
  void record_sess_killed(uint32_t cs_id);
  optimizer::ObShardingSelectLogPlan* get_sharding_select_log_plan() const { return select_plan_; }
  void set_sharding_select_log_plan(optimizer::ObShardingSelectLogPlan *plan) {
    if (NULL != select_plan_) {
      common::ObIAllocator *allocator_ = select_plan_->get_allocator();
      select_plan_->~ObShardingSelectLogPlan();
      if (NULL != allocator_) {
        optimizer::get_global_optimizer_processor().free_allocator(allocator_);
      }
      select_plan_ = NULL;
    }

    select_plan_ = plan;
  }

  bool can_direct_ok() const { return can_direct_ok_; }
  void set_can_direct_ok(bool val) { can_direct_ok_ = val; }

  // ps cache
  uint32_t inc_and_get_ps_id() {
    uint32_t ps_id = ++ps_id_;
    if (ps_id >= (CURSOR_ID_START)) {
      ps_id_ = 0;
      ps_id = ++ps_id_;
    }
    return ps_id;
  }
  uint32_t inc_and_get_cursor_id() {
    uint32_t cursor_id = ++cursor_id_;
    if (cursor_id <= (CURSOR_ID_START)) {
      cursor_id_ = CURSOR_ID_START;
      cursor_id = ++cursor_id_;
    }
    return cursor_id;
  }

  void set_using_ldg(const bool using_ldg) { using_ldg_ = using_ldg; }
  bool using_ldg() const { return using_ldg_; }
  obutils::ObInactivityTimeoutEvent get_inactivity_timeout_event() const { return timeout_event_;}
  ObHRTime get_timeout_record() const { return timeout_record_; }
  bool is_request_transferring() const { return is_request_transferring_; }
  void set_request_transferring(bool is_transferring) { is_request_transferring_ = is_transferring; }
  ObConnTenantInfo& get_ct_info() { return ct_info_; }

  void set_cs_id_version(const ObClientSessionIDVersion cs_id_version) { cs_id_version_ = cs_id_version; }
  ObClientSessionIDVersion get_cs_id_version() const { return cs_id_version_; }
  bool is_cs_id_v2() const { return cs_id_version_ == CLIENT_SESSION_ID_V2; }
  void set_connected_time(const int64_t connected_time) { connected_time_ = connected_time; }
  int64_t  get_connected_time() const { return  connected_time_; }

private:
  static uint32_t get_next_ps_stmt_id();

  int state_keep_alive(int event, void *data);
  int state_server_keep_alive(int event, void *data);
  int handle_other_event(int event, void *data);

  int handle_delete_cluster();

  void set_tcp_init_cwnd();

  int fetch_tenant_by_vip();
  int get_vip_addr();
  inline void decrease_used_connections();

  void update_session_stats();
  bool need_close() const;

public:
  static const int64_t OP_LOCAL_NUM = 32;
  static const int64_t SCRAMBLE_SIZE = 20;

  bool can_direct_ok_;
  bool is_proxy_mysql_client_; // used for ObMysqlClient
  bool can_direct_send_request_; // used for ObMysqlClient
  bool can_server_session_release_;  //used for session release
  proxy::ObCommonAddr common_addr_; // session pool server_addr

  // An active connection is one that a request has been
  // successfully parsed (PARSE_DONE) and it remains to be
  // active until the transaction goes through or the client
  // aborts.
  bool active_;

  // store tests_server_addr in client session, so we can use same test_server during the connection
  net::ObIpEndpoint test_server_addr_;
  // when kill self's session, it is true
  bool vc_ready_killed_;
  bool is_waiting_trans_first_request_;
  bool is_need_send_trace_info_;
  bool is_already_send_trace_info_;
  bool is_first_handle_request_;
  bool is_in_trans_for_close_request_;
  bool is_last_request_in_trans_;
  bool is_trans_internal_routing_;
  bool is_need_return_last_bound_ss_;
  bool need_delete_cluster_;
  bool is_first_dml_sql_got_;//default false, will route with merge status careless
                             //it is true after user first dml sql arrived.
  bool is_proxy_enable_trans_internal_routing_; // from config, update each tranasction start
  uint8_t compressed_seq_;   // seq management between client & proxy

  obutils::ObClusterResource *cluster_resource_;
  ObTableEntry *dummy_entry_; // __all_dummy's table location entry
  bool is_need_update_dummy_entry_;
  ObLDCLocation dummy_ldc_;
  // if expried, cached dummy entry will refetch;
  // every client session has random half to full of config.tenant_location_valid_time;
  int64_t dummy_entry_valid_time_ns_;
  uint64_t server_state_version_;

  //mainly used for inner cs. if not inner cs. it must null
  //when it is inner cs, it point to client_vc.info_.request_param actually and update real-time
  //the current idc name in it has higher priority than session_info_.idc_name_
  const ObMysqlRequestParam *inner_request_param_;

  bool is_request_transferring_;

  ObProxySchemaKey schema_key_;
  obutils::ObInactivityTimeoutEvent timeout_event_;     // just record timeout event for log
  ObHRTime timeout_record_;                             // just record timeout for log
  LINK(ObMysqlClientSession, stat_link_);

#ifdef USE_MYSQL_DEBUG_LISTS
  LINK(ObMysqlClientSession, link_);
#endif

private:
  enum ObClientReadState
  {
    MCS_INIT = 0,
    MCS_ACTIVE_READER,
    MCS_KEEP_ALIVE,
    MCS_HALF_CLOSED,
    MCS_CLOSED,
    MCS_MAX
  };

  bool tcp_init_cwnd_set_;
  bool half_close_;
  bool conn_decrease_;
  bool conn_prometheus_decrease_;
  bool vip_connection_decrease_;
  int magic_;

  event::ObEThread *create_thread_;
  bool is_local_connection_;
  net::ObNetVConnection *client_vc_;
  ObInListStat in_list_stat_;
  int64_t current_tid_;  // the thread id the client session bind to, just for show proxystat
  uint32_t cs_id_;//Unique client session identifier, assignment by proxy self
  uint64_t proxy_sessid_;

  // Attetion! (bound_ss_ != NULL && cur_ss_ != NULL) will never appear.
  // last used server session, which is listening by client session.
  // it's mainly used to pick server session in a transaction.
  ObMysqlServerSession *bound_ss_;
  // current used server session, which is listening by mysql sm.
  // it's mainly used to traverse all server sessions.
  // curr_ss_ is pointed to the server session used by mysql sm.
  ObMysqlServerSession *cur_ss_;
  // last_insert_id server session.
  // it's changed every time when last_insert_id is changed.
  // NOTE:: it is only appoint to server session, no hold it
  ObMysqlServerSession *lii_ss_;
  ObMysqlServerSession *last_bound_ss_;

  // coordinator session address in transaction
  net::ObIpEndpoint trans_coordinator_ss_addr_;

  event::ObMIOBuffer *read_buffer_;
  event::ObIOBufferReader *buffer_reader_;

  ObMysqlSM *mysql_sm_;

  ObClientReadState read_state_;

  event::ObVIO *ka_vio_;
  event::ObVIO *server_ka_vio_;

  ObConnTenantInfo ct_info_;

  //session info
  ObClientSessionInfo session_info_;
  ObMysqlSessionManager session_manager_; // server session manager
  ObMysqlSessionManagerNew session_manager_new_; // server session manager
  ObSessionStats session_stats_;
  ObTraceStats *trace_stats_;
  optimizer::ObShardingSelectLogPlan *select_plan_;
  uint32_t ps_id_;
  uint32_t cursor_id_;
  bool using_ldg_;
  ObClientSessionIDVersion cs_id_version_;
  int64_t connected_time_;
private:
  int acquire_client_session_id_v1();
  int acquire_client_session_id_v2();
  DISALLOW_COPY_AND_ASSIGN(ObMysqlClientSession);
};

inline void ObMysqlClientSession::set_local_connection()
{
  if (is_proxy_mysql_client_ || RUN_MODE_CLIENT == g_run_mode) {
    is_local_connection_ = true;
  } else {
    if (OB_NOT_NULL(client_vc_)) {
      is_local_connection_ = net::ops_is_ip_loopback(client_vc_->get_local_addr());
    }
  }
}

inline uint32_t ObMysqlClientSession::get_next_ps_stmt_id()
{
  static uint32_t next_stmt_id = 1;
  uint32_t ret = 0;
  do {
    ret = ATOMIC_FAA(&next_stmt_id, 1);
  } while (0 == ret);
  return ret;
}

inline common::ObAddr ObMysqlClientSession::get_real_client_addr(net::ObNetVConnection *server_vc)
{
  common::ObAddr ret_addr;
  if (is_proxy_mysql_client_ || RUN_MODE_CLIENT == g_run_mode) {
    if (OB_NOT_NULL(server_vc)) {
      ret_addr.set_sockaddr(server_vc->get_local_addr());
    }
  } else {
    if (OB_NOT_NULL(client_vc_)) {
      ret_addr.set_sockaddr(client_vc_->get_real_client_addr());
    }
  }
  PROXY_CS_LOG(DEBUG, "succ to get real client addr", K(ret_addr));
  return ret_addr;
}

inline void ObMysqlClientSession::set_inactivity_timeout(ObHRTime timeout, obutils::ObInactivityTimeoutEvent event)
{
  timeout_event_ = event;
  timeout_record_ = timeout;
  if (OB_LIKELY(NULL != client_vc_)) {
    client_vc_->set_inactivity_timeout(timeout);
  }
}

inline void ObMysqlClientSession::cancel_inactivity_timeout()
{
  timeout_event_ = obutils::OB_TIMEOUT_UNKNOWN_EVENT;
  timeout_record_ = 0;
  if (OB_LIKELY(NULL != client_vc_)) {
    client_vc_->cancel_inactivity_timeout();
  }
}

inline int ObMysqlClientSession::reset_read_buffer()
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buffer_reader_) || OB_UNLIKELY(NULL == read_buffer_)) {
    ret =common::OB_ERR_UNEXPECTED;
    PROXY_CS_LOG(WDIAG, "invalid read_buffer", K(buffer_reader_), K(read_buffer_), K(ret));
  } else {
    read_buffer_->dealloc_all_readers();
    read_buffer_->writer_ = NULL;
    buffer_reader_ = read_buffer_->alloc_reader();
  }
  return ret;
}

// A list of client sessions.
class ObMysqlClientSessionMap
{
public:
  ObMysqlClientSessionMap() {}
  virtual ~ObMysqlClientSessionMap() {}

public:
  static const int64_t HASH_BUCKET_SIZE = 64;

  // Interface class for client session connection id map.
  struct IDHashing
  {
    typedef const uint32_t &Key;
    typedef ObMysqlClientSession Value;
    typedef ObDLList(ObMysqlClientSession, stat_link_) ListHead;

    static uint64_t hash(Key key) { return common::murmurhash(&key, sizeof(uint32_t), 0); }
    static Key key(Value *value) { return value->get_cs_id_ref(); }
    static bool equal(Key lhs, Key rhs) { return lhs == rhs; }
  };

  typedef common::hash::ObBuildInHashMap<IDHashing, HASH_BUCKET_SIZE> IDHashMap; // Sessions by client session identity

public:
  int set(ObMysqlClientSession &cs);
  int get(const uint32_t &cs_id, ObMysqlClientSession *&cs);
  int erase(const uint32_t &cs_id);

  IDHashMap id_map_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMysqlClientSessionMap);
};

inline ObMysqlClientSessionMap &get_client_session_map(const event::ObEThread &t)
{
  return *(const_cast<event::ObEThread *>(&t)->cs_map_);
}

inline common::ObMysqlRandom &get_random_seed(const event::ObEThread &t)
{
  return *(const_cast<event::ObEThread *>(&t)->random_seed_);
}

int init_cs_map_for_thread();
int init_cs_id_list_for_thread();
int init_random_seed_for_thread();
int init_random_seed_for_one_thread(int64_t index);

bool is_proxy_conn_id_avail(const uint64_t conn_id);
bool is_server_conn_id_avail(const uint64_t conn_id);
bool is_conn_id_avail(const int64_t conn_id, bool &is_proxy_generated);
int extract_thread_id(const uint32_t cs_id, int64_t &thread_id);
int extract_thread_id_v1(const uint32_t cs_id, int64_t &thread_id);
int extract_thread_id_v2(const uint32_t cs_id, int64_t &thread_id);
void extract_proxy_id_v2(const uint32_t cs_id, int64_t &proxy_id);

int init_cs_map_for_one_thread(int64_t index);

class ObClientSessionIDList
{

public:
  ObClientSessionIDList() { using_cs_id_set_.create(HASH_BUCKET_SIZE, ObModIds::OB_PROXY_CLIENT_SESSION_ID, ObModIds::OB_PROXY_CLIENT_SESSION_ID); }
  virtual ~ObClientSessionIDList() { using_cs_id_set_.destroy(); }
public:
  static const int64_t HASH_BUCKET_SIZE = 64;
  int record_cs_id(const uint32_t cs_id);
  int is_cs_id_exist(const uint32_t cs_id, bool &is_exist);
  int erase_cs_id(const uint32_t cs_id);
private:
  ObSEArray<uint32_t, 32> unused_cs_id_list_;
  hash::ObHashSet<uint32_t, hash::NoPthreadDefendMode> using_cs_id_set_;
  common::DRWLock lock_;
  DISALLOW_COPY_AND_ASSIGN(ObClientSessionIDList);
};

inline ObClientSessionIDList &get_client_session_id_list(const event::ObEThread &t)
{
  return *(const_cast<event::ObEThread *>(&t)->cs_id_list_);
}



} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_MYSQL_CLIENT_SESSION_H
