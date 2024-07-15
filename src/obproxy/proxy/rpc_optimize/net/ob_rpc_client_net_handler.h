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
#ifndef OBPROXY_RPC_CLIENT_NET_HANDLER_H
#define OBPROXY_RPC_CLIENT_NET_HANDLER_H

#include "obkv/table/ob_rpc_struct.h"
#include "proxy/rpc_optimize/net/ob_rpc_net_handler.h"
#include "proxy/rpc_optimize/rpclib/ob_rpc_req_analyzer.h"
#include "proxy/rpc_optimize/net/ob_proxy_rpc_session_info.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

class ObRpcClientNetHandler;
class ObRpcClientNetHandlerMap;

#define RPC_CLIENT_NET_PERIOD_TASK RPC_CLIENT_NET_EVENT_EVENTS_START + 1
#define RPC_CLIENT_NET_SEND_RESPONSE RPC_CLIENT_NET_EVENT_EVENTS_START + 2

static const uint32_t LOCAL_IPV4_ADDR = 0x100007F;

enum ObRpcClientNetMagic
{
  RPC_C_NET_MAGIC_ALIVE = 0x0123F00D,
  RPC_C_NET_MAGIC_DEAD = 0xDEADF00D
};

typedef int (ObRpcClientNetHandler::*ClientNetHandler)(int event, void *data);

#define CLIENT_NET_SET_DEFAULT_HANDLER(h) {        \
     cs_default_handler_ = (ClientNetHandler)h;    \
}

class ObRpcClientNetHandler : public ObRpcNetHandler
{
public:
  struct ObConnTenantInfo
  {
    ObConnTenantInfo() : lookup_success_(false), vip_tenant_(), client_addr_(), slb_addr_() {}
    ~ObConnTenantInfo() { }

    bool lookup_success_;
    obutils::ObVipTenant vip_tenant_;
    common::ObAddr client_addr_; // the client ip addr
    common::ObAddr slb_addr_;    // SLB ip addr
  };

  ObRpcClientNetHandler();
  virtual ~ObRpcClientNetHandler() {}

  void destroy();
  // int new_connection(net::ObNetVConnection &new_vc);
  int main_handler(int event, void *data);
  int new_connection(net::ObNetVConnection *new_vc, event::ObMIOBuffer *iobuf,
                     event::ObIOBufferReader *reader);
  int new_connection(net::ObNetVConnection *new_vc, event::ObMIOBuffer *iobuf,
                     event::ObIOBufferReader *reader, obutils::ObClusterResource *cluster_resource);

  int handle_other_event(int event, void *data);
  int state_keep_alive(int event, void *data);

  int handle_delete_cluster();
  const char *get_read_state_str() const;

  void handle_new_connection();
  int acquire_client_session_id();
  int add_to_list();
  int get_vip_addr();
  int fetch_tenant_by_vip();
  common::ObAddr get_real_client_addr(net::ObNetVConnection *server_vc = NULL);

  int schedule_period_task();
  int handle_period_task();
  int cancel_period_task();

  int schedule_send_response_action();
  int cancel_pending_action();

  void set_local_connection();
  bool is_local_connection() const { return is_local_connection_; }

  void set_half_close_flag() { half_close_ = true; }
  void clear_half_close_flag() { half_close_ = false; }
  bool get_half_close_flag() const { return half_close_; }
  void set_tcp_init_cwnd();
  bool need_close() const;

  uint64_t get_next_proxy_sessid();

  event::ObVIO *do_io_write(
    ObContinuation *c, const int64_t nbytes, event::ObIOBufferReader *buf);
  void do_io_close(const int alerrno = 0);
  void reenable(event::ObVIO *vio);
  int release(event::ObIOBufferReader *r);
  void handle_transact_complete(event::ObIOBufferReader *r, bool &close_cs);

  ObRpcClientNetSessionInfo &get_session_info() { return session_info_; }
  const common::ObString &get_vip_tenant_name() { return ct_info_.vip_tenant_.tenant_name_; }
  const common::ObString &get_vip_cluster_name() { return ct_info_.vip_tenant_.cluster_name_; }
  ObConnTenantInfo& get_ct_info() { return ct_info_; }

  net::ObIpEndpoint &get_last_server_addr() { return last_server_ip_; }
  void set_last_server_addr(net::ObIpEndpoint &server_ip) { last_server_ip_ = server_ip; }
  int64_t get_cluster_id() const { return session_info_.get_cluster_id(); }

  bool is_vip_lookup_success() const { return ct_info_.lookup_success_; }
  bool is_need_convert_vip_to_tname();
  bool need_print_trace_stat() const;

  int64_t get_current_tid() const { return current_tid_; }
  uint32_t get_cs_id() { return cs_id_; }
  uint32_t &get_cs_id_ref() { return cs_id_; }
  uint64_t get_proxy_sessid() const { return proxy_sessid_; }
  void set_proxy_sessid(uint64_t sessid) { proxy_sessid_ = sessid; }

  void set_need_delete_cluster() { need_delete_cluster_ = true; }

  ObString get_current_idc_name() const;
  int check_update_ldc();
  int check_update_ldc(ObLDCLocation &dummy_ldc);

  // int swap_mutex(void *data);

  static int get_thread_init_cs_id(uint32_t &thread_init_cs_id, uint32_t &max_local_seq, const int64_t thread_id = -1);
  int handle_response_rewrite_channel_id(ObRpcReq *request);
  int init_request_meta_info(ObRpcReq *request);

  int64_t to_string(char *buf, const int64_t buf_len) const;

  /** handle net info*/
  int setup_client_request_read();
  int state_client_request_read(int event, void *data);

  int calc_response_need_send(int64_t &count);
  int store_rpc_req_into_response_buffer(int64_t need_send_resp_count, int64_t &send_response, int64_t &total_response_len);
  int setup_client_response_send();
  int setup_client_response_direct_send();
  int state_client_response_send(int event, void *data);

  int handle_client_entry_setup_error(int event, void *data);
  void add_client_response_request(ObRpcReq *request);
  void clean_all_pending_request();
  void clean_all_timeout_request();

  int64_t get_cluster_version() const { return cluster_version_; }
  void set_cluster_version(int64_t cluster_version) { cluster_version_ = cluster_version; }

  void set_has_tenant_username(bool flag) { session_info_.has_tenant_username_ = flag; }
  void set_has_cluster_username(bool flag) { session_info_.has_cluster_username_ = flag; }
  bool has_tenant_username() const { return session_info_.has_tenant_username_; }
  bool has_cluster_username() const { return session_info_.has_cluster_username_; }

  enum ObInListStat
  {
    LIST_INIT = 0,
    LIST_ADDED,
    LIST_REMOVED,
  };
  enum ObClientReadState
  {
    MCS_INIT = 0,
    MCS_ACTIVE_READER,
    MCS_KEEP_ALIVE,
    MCS_HALF_CLOSED,
    MCS_CLOSED,
    MCS_MAX
  };

public:
  ClientNetHandler cs_default_handler_;
  bool vc_ready_killed_;
  bool half_close_;
  obutils::ObClusterResource *cluster_resource_;
  int64_t cluster_version_;
  //TODO next need build a global cache for client_net_handler

  ObTableEntry *dummy_entry_; // __all_dummy's table location entry
  bool is_need_update_dummy_entry_;
  ObLDCLocation dummy_ldc_;
  // if expried, cached dummy entry will refetch;
  // every client session has random half to full of config.tenant_location_valid_time;
  int64_t dummy_entry_valid_time_ns_;

  // ObProxySchemaKey schema_key_;
  LINK(ObRpcClientNetHandler, stat_link_);

#ifdef USE_MYSQL_DEBUG_LISTS
  LINK(ObRpcClientNetHandler, link_);
#endif

private:


  ObRpcClientNetMagic magic_;

  event::ObEThread *create_thread_;
  bool is_local_connection_;
  ObInListStat in_list_stat_;
  int64_t current_tid_;  // the thread id the client session bind to, just for show proxystat
  uint32_t cs_id_;//Unique client session identifier, assignment by proxy self
  uint32_t atomic_channel_id_;
  uint64_t proxy_sessid_;
  bool using_ldg_;
  bool tcp_init_cwnd_set_;
  ObClientReadState read_state_;
  bool active_;
  bool is_sending_response_;
  bool need_delete_cluster_;

  uint64_t server_state_version_;

  ObConnTenantInfo ct_info_;
  net::ObIpEndpoint last_server_ip_; /* only used for weak read when not have any route info (need use dummumy entry pll)*/
  typedef hash::ObHashMap<int32_t, ObRpcReq *, hash::NoPthreadDefendMode> RPC_PKT_REQ_MAP;
  RPC_PKT_REQ_MAP cid_to_req_map_;
  ObRpcReqList need_send_response_list_;
  ObRpcReqList sending_response_list_;
  event::ObAction *period_task_action_;
  event::ObAction *pending_action_;

  int64_t current_need_read_len_;
  obkv::ObRpcEzHeader current_ez_header_;

  ObRpcClientNetSessionInfo session_info_; //use client session info first
  char net_head_buf_[ObProxyRpcReqAnalyzer::RPC_NET_HEADER];
private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcClientNetHandler);
};

inline void ObRpcClientNetHandler::set_local_connection()
{
  if (OB_NOT_NULL(rpc_net_vc_)) {
    is_local_connection_ = net::ops_is_ip_loopback(rpc_net_vc_->get_local_addr());
  }
}

inline common::ObAddr ObRpcClientNetHandler::get_real_client_addr(net::ObNetVConnection *server_vc)
{
  UNUSED(server_vc);
  common::ObAddr ret_addr;
  if (OB_NOT_NULL(rpc_net_vc_)) {
    ret_addr.set_sockaddr(rpc_net_vc_->get_real_client_addr());
  }
  PROXY_CS_LOG(DEBUG, "succ to get real client addr", K(ret_addr));
  return ret_addr;
}

inline void ObRpcClientNetHandler::add_client_response_request(ObRpcReq *request)
{
  if (OB_NOT_NULL(request)) {
    PROXY_CS_LOG(DEBUG, "ObRpcClientNetHandler::add_client_response_request", K(request));
    need_send_response_list_.push_back(request);
  }
}

// A list of client sessions.
class ObRpcClientNetHandlerMap
{
public:
  ObRpcClientNetHandlerMap() {}
  virtual ~ObRpcClientNetHandlerMap() {}

public:
  static const int64_t HASH_BUCKET_SIZE = 64;

  // Interface class for client session connection id map.
  struct IDHashing
  {
    typedef const uint32_t &Key;
    typedef ObRpcClientNetHandler Value;
    typedef ObDLList(ObRpcClientNetHandler, stat_link_) ListHead;

    static uint64_t hash(Key key) { return common::murmurhash(&key, sizeof(uint32_t), 0); }
    static Key key(Value *value) { return value->get_cs_id_ref(); }
    static bool equal(Key lhs, Key rhs) { return lhs == rhs; }
  };

  typedef common::hash::ObBuildInHashMap<IDHashing, HASH_BUCKET_SIZE> IDHashMap; // Sessions by client session identity

public:
  int set(ObRpcClientNetHandler &cs);
  int get(const uint32_t &cs_id, ObRpcClientNetHandler *&cs);
  int erase(const uint32_t &cs_id);

  IDHashMap id_map_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcClientNetHandlerMap);
};

inline ObRpcClientNetHandlerMap &get_rpc_client_net_handler_map(const event::ObEThread &t)
{
  // return *(const_cast<event::ObEThread *>(&t)->rpc_cs_map_);
  return *(const_cast<event::ObEThread *>(&t)->rpc_net_cs_map_);
}

inline common::ObMysqlRandom &get_rpc_net_random_seed(const event::ObEThread &t)
{
  return *(const_cast<event::ObEThread *>(&t)->random_seed_);
}

int init_rpc_net_cs_map_for_thread();
int init_rpc_net_random_seed_for_thread();

// bool is_rpc_proxy_conn_id_avail(const uint64_t conn_id);
// bool is_rpc_server_conn_id_avail(const uint64_t conn_id);
// bool is_rpc_conn_id_avail(const int64_t conn_id, bool &is_proxy_generated);
// int rpc_extract_thread_id(const uint32_t cs_id, int64_t &thread_id);

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_RPC_CLIENT_NET_HANDLER_H
