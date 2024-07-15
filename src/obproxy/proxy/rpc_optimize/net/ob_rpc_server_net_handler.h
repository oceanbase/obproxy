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
#ifndef OBPROXY_RPC_SERVER_NET_HANDLER_H
#define OBPROXY_RPC_SERVER_NET_HANDLER_H

#include "obkv/table/ob_rpc_struct.h"
#include "proxy/rpc_optimize/net/ob_rpc_net_handler.h"
#include "proxy/rpc_optimize/rpclib/ob_rpc_req_analyzer.h"
#include "lib/hash/ob_hashmap.h" //ObHashMap

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObRpcReq;
class ObRpcServerNetTableEntry;
class ObRpcServerNetHandleHashKey;
//RPC_SERVER_NET_EVENT_EVENTS_START

#define RPC_SERVER_NET_REQUEST_SEND RPC_SERVER_NET_EVENT_EVENTS_START + 1
#define RPC_SERVER_NET_RESPONSE_RECVD RPC_SERVER_NET_EVENT_EVENTS_START + 2
#define RPC_SERVER_NET_PERIOD_TASK RPC_SERVER_NET_EVENT_EVENTS_START + 3
#define RPC_SERVER_NET_TABLE_ENTRY_SEND_REQUEST RPC_SERVER_NET_EVENT_EVENTS_START + 80
#define RPC_SERVER_NET_TABLE_ENTRY_DESTROY RPC_SERVER_NET_EVENT_EVENTS_START + 81
#define RPC_SERVER_NET_TABLE_ENTRY_PERIOD_TASK RPC_SERVER_NET_EVENT_EVENTS_START + 82
#define RPC_SERVER_NET_TABLE_ENTRY_RETRY_PENDING_REREQUST RPC_SERVER_NET_EVENT_EVENTS_START + 83

enum ObRpcServerNetState
{
  OB_RPC_SERVER_ENTRY_INIT = 0,
  OB_RPC_SERVER_ENTRY_DESTROY,
  OB_RPC_SERVER_ENTRY_CONNECTING,
  OB_RPC_SERVER_ENTRY_ACTIVE,
  OB_RPC_SERVER_ENTRY_IDEL,
  OB_RPC_SERVER_ENTRY_REQUEST_SENDING,
  OB_RPC_SERVER_ENTRY_RESPONSE_WAITING,
  OB_RPC_SERVER_ENTRY_MAX,
};

class ObRpcServerNetHandleHashKey
{
public:
  ObRpcServerNetHandleHashKey() :
    server_ip_(NULL), auth_user_(NULL) {}
  ~ObRpcServerNetHandleHashKey() {}

  bool equal (const ObRpcServerNetHandleHashKey& rhs) const
  {
    if (!net::ops_ip_addr_port_eq(server_ip_->sa_, rhs.server_ip_->sa_)) {
      return false;
    }

    // if (auth_user_ != NULL && rhs.auth_user_ != NULL)  {
    //   if (*auth_user_ != *rhs.auth_user_)  {
    //     return false;
    //   }
    // }
    return true;
  }

  TO_STRING_KV(KPC_(server_ip), KPC_(auth_user));

  // const net::ObIpEndpoint *local_ip_;
  const net::ObIpEndpoint *server_ip_;
  const ObString *auth_user_;
};

// class ObRpcServerSession : public event::ObVConnection
class ObRpcServerNetHandler : public ObRpcNetHandler
{
public:
  ObRpcServerNetHandler();
  virtual ~ObRpcServerNetHandler() { }
  void destroy();
  void init_server_addr(ObAddr server_addr); //to init server_ip
  void init_server_ip(net::ObIpEndpoint &server_ip); //to init server_ip
  int new_connection(net::ObNetVConnection &new_vc);
  int new_connection(net::ObNetVConnection *new_vc, event::ObMIOBuffer *iobuf,
                     event::ObIOBufferReader *reader);
//  int new_connection(net::ObNetVConnection *new_vc, event::ObMIOBuffer *iobuf,
//                     event::ObIOBufferReader *reader, obutils::ObClusterResource *cluster_resource);
  int handle_new_connection();
  // int release(event::ObIOBufferReader *r);

  // virtual void do_io_shutdown(const event::ShutdownHowToType howto);

  // void set_half_close_flag() { half_close_ = true; }
  // void clear_half_close_flag() { half_close_ = false; }
  // bool get_half_close_flag() const { return half_close_; }

  int64_t get_current_tid() const { return current_tid_; }
  uint32_t &get_cs_id_ref() { return cs_id_; }
  uint64_t get_proxy_sessid() const { return proxy_sessid_; }
  void set_proxy_sessid(uint64_t sessid) { proxy_sessid_ = sessid; }
  uint64_t get_next_proxy_sessid();
  int64_t get_next_ss_id();

  ObString get_current_idc_name() const;
  // int check_update_ldc();
  void set_tcp_init_cwnd();


  // static int get_thread_init_cs_id(uint32_t &thread_init_cs_id, uint32_t &max_local_seq, const int64_t thread_id = -1);

  int64_t to_string(char *buf, const int64_t buf_len) const;

  const char *get_read_state_str() const;
  event::ObVIO *do_io_write(
    ObContinuation *c, const int64_t nbytes, event::ObIOBufferReader *buf);
  void do_io_close(const int alerrno = 0);
  void reenable(event::ObVIO *vio);
  bool need_close() const;
  bool need_print_trace_stat() const;

  ObRpcServerNetHandleHashKey get_server_handler_hash_key() const
  {
    ObRpcServerNetHandleHashKey key;
    // key.local_ip_ = &local_ip_;
    key.server_ip_ = &server_ip_;
    // key.auth_user_ = &auth_user_;
    return key;
  }

  /** handle net info*/
  int main_handler(int event, void *data);
  int handle_net_event(int event, void *data);
  int handle_common_event(int event, void *data);
  int handle_other_event(int event, void *data);
  // int setup_init_new_server_connection();
  int state_server_new_connection(int event, void *data);
  int state_server_request_send(int event, void *data);
  int setup_server_response_read();
  int state_server_response_read(int event, void *data);
  int setup_server_request_send();
  int setup_retry_request(ObRpcReqList &requests);
  // int state_server_request_send_done(int event, void *data);
  // int state_server_keep_alive(int event, void *data);
  // int state_keep_alive(int event, void *data);
  int handle_server_entry_setup_error(int event, void *data);

  int handle_request_rewrite_channel_id(ObRpcReq *request);
  int handle_response_recover_channel_id(ObRpcReq *request, uint32_t origin_id);

  int schedule_period_task();
  int handle_period_task();
  int cancel_period_task();

  int schedule_timeout_action();
  int cancel_timeout_action();
  int cancel_pending_action();
  int handle_timeout();

  void renew_first_send_time() {
    if (0 == first_send_time_us_) {
      first_send_time_us_ = common::hrtime_to_usec(event::get_hrtime());
    }
  }
  void renew_last_send_time() { last_send_time_us_ = common::hrtime_to_usec(event::get_hrtime()); }
  void renew_last_recv_time() { last_recv_time_us_ = common::hrtime_to_usec(event::get_hrtime()); }
  void renew_handler_last_active_time() { last_active_time_us_ = common::hrtime_to_usec(event::get_hrtime()); }
  bool is_invalid_server_net_handle(int64_t rpc_server_net_invalid_time_us, int64_t rpc_server_net_max_pending_request) {
    return 0 != last_recv_time_us_ && 0 != rpc_server_net_invalid_time_us && 0 != rpc_server_net_max_pending_request
          && rpc_server_net_invalid_time_us < common::hrtime_to_usec(event::get_hrtime()) - last_recv_time_us_
          && rpc_server_net_max_pending_request < request_pending_;
  }

  bool is_server_net_handler_expired(int64_t rpc_server_net_handler_expire_time_us) {
    return last_active_time_us_ != 0 &&
           need_send_req_list_.size() == 0 &&
           sending_req_list_.size() == 0 &&
           request_pending_ == 0 &&
           rpc_server_net_handler_expire_time_us != 0 &&
           rpc_server_net_handler_expire_time_us < common::hrtime_to_usec(event::get_hrtime()) - last_active_time_us_;
  }
  void clean_all_pending_request();
  void clean_all_timeout_request();

  enum ObServerReadState
  {
    MCS_INIT = 0,
    MCS_ACTIVE_READER,
    MCS_KEEP_ALIVE,
    MCS_HALF_CLOSED,
    MCS_CLOSED,
    MCS_MAX
  };


public:

  LINK(ObRpcServerNetHandler, all_server_link_);
  LINK(ObRpcServerNetHandler, free_server_link_);
  LINK(ObRpcServerNetHandler, in_connect_server_link_);


  ObServerReadState read_state_;
  bool conn_decrease_;
  bool active_;

  common::ObAddr server_addr_;
  bool vc_ready_killed_;
  ObRpcReqList need_send_req_list_;
  ObRpcReqList sending_req_list_;

public:
  bool tcp_init_cwnd_set_;
  bool half_close_;
  ObRpcServerNetState state_;
  int64_t server_connect_retry_times_;
  int64_t first_send_time_us_; //first send time after last receive time
  int64_t last_send_time_us_;
  int64_t last_recv_time_us_;
  int64_t last_active_time_us_; // used for expire control
  int64_t request_sent_;
  int64_t request_pending_;
  int64_t current_need_read_len_;
  obkv::ObRpcEzHeader current_ez_header_;
  ObRpcServerNetTableEntry *server_table_entry_; //pointer to table_entry
  event::ObEThread *create_thread_;
  int64_t current_tid_;  // the thread id the client session bind to, just for show proxystat
  uint32_t s_id_;        //Unique client session identifier, assignment by proxy self
  uint32_t cs_id_;        //Unique client session identifier, assignment by proxy self
  uint64_t proxy_sessid_;
  int32_t atomic_channel_id_; // channel id for this server
  int64_t timeout_us_;
  event::ObAction *pending_action_;
  event::ObAction *timeout_action_;
  event::ObAction *period_task_action_;
  common::hash::ObHashMap<int32_t, ObRpcReq*, hash::NoPthreadDefendMode>  cid_to_req_map_; //we could find the ObRpcReq based on channel_id
  char net_head_buf_[ObProxyRpcReqAnalyzer::RPC_NET_HEADER];

private:
  inline int calc_request_need_send(ObRpcReqList &retry_list);
  int save_rpc_response(ObRpcReq *rpc_req);
  DISALLOW_COPY_AND_ASSIGN(ObRpcServerNetHandler);
};

/* one group server  */
class ObRpcServerNetTableEntry : public event::ObContinuation
{
public:
  ObRpcServerNetTableEntry() : server_addr_(), server_ip_(), local_ip_(),
                               waiting_req_list_(), create_thread_(NULL), cur_server_entry_count_(0), max_server_entry_count_(0),
                               access_times_(0), last_access_timestamp_(0), connnectiong_server_entry_count_(0),
                               server_connect_error_conut_(0), last_build_connect_timestamp_(0),
                               last_connect_failed_timestamp_(0), last_connect_success_timestamp_(0),
                               send_request_action_(NULL), period_task_action_(NULL), retry_request_action_(NULL)
  {
    SET_HANDLER(&ObRpcServerNetTableEntry::main_handler);
  }
  virtual ~ObRpcServerNetTableEntry() {}
  int init();
  void destroy();
  //handle the request
  int add_req_to_waiting_link(ObRpcReq *ob_rpc_req); // put the waiting request
  int pop_back_req_from_waiting_link(ObRpcReq *&ob_rpc_req);                 // pop waiting request
  int64_t to_string(char *buf, const int64_t buf_len) const;

  //handle the sever entry
  ObRpcServerNetHandler* pop_free_server_entry(); // get server entry
  // int add_server_entry_to_free_link(ObRpcServerNetHandler *entry);
  void add_free_server_entry(ObRpcServerNetHandler *entry);
  // ObRpcServerNetHandler* pop_connect_server_entry(ObRpcServerNetHandler *entry);
  int add_connect_server_entry(ObRpcServerNetHandler *entry);
  int remove_server_entry(ObRpcServerNetHandler *entry);
  void do_entry_close();
  int handle_rpc_request(ObRpcReq &request);
  int handle_send_request();

  int schedule_period_task();
  int handle_period_task();
  int cancel_period_task();

  void clean_all_timeout_request();

  int connect_to_observer(); //add a conenction to observer
  int main_handler(int event, void *data);

  int schedule_send_request_action();
  int cancel_send_request_action();
  void set_server_connector_error();
  void cleanup_server_connector_error();
  void force_retry_waiting_link(bool server_failed);

  ObRpcServerNetHandleHashKey get_server_handler_hash_key() const
  {
    ObRpcServerNetHandleHashKey key;
    // key.local_ip_ = &local_ip_;
    // server_addr_.get_ipv4
    key.server_ip_ = &server_ip_;
    // key.auth_user_ = &auth_user_;
    return key;
  }
  void renew_last_access_time() { last_access_timestamp_ = common::hrtime_to_usec(event::get_hrtime()); }
  int clean_all_pending_request();
  bool is_server_table_entry_expired(int64_t rpc_server_entry_expire_time) {
    return all_server_list_.empty() &&
           cur_server_entry_count_ == 0 &&
           OB_ISNULL(send_request_action_) &&
           last_access_timestamp_ != 0 &&
           rpc_server_entry_expire_time != 0 &&
           rpc_server_entry_expire_time < common::hrtime_to_usec(event::get_hrtime()) - last_access_timestamp_;
  }
public:
  static const int64_t HASH_BUCKET_SIZE = 16;

public:
  // Interface class for IP map.
  struct ObRpcIPHashing
  {
    typedef const ObRpcServerNetHandleHashKey Key;
    typedef ObRpcServerNetHandler Value;
    // typedef ObDLList(ObRpcServerNetHandler, ip_hash_link_) ListHead;

    static uint64_t hash(Key key) { return common::murmurhash(&key.server_ip_->sa_, sizeof(sockaddr), 0); }
    static Key key(Value const *value)
    {
      ObRpcServerNetHandleHashKey key;
      if (OB_NOT_NULL(value)) {
        key = value->get_server_handler_hash_key();
      }
      return key;
    }

    static bool equal(Key lhs, Key rhs) { return lhs.equal(rhs); }
  };


public:
  LINK(ObRpcServerNetTableEntry, ip_hash_link_);
  ObAddr server_addr_;
  net::ObIpEndpoint server_ip_;
  net::ObIpEndpoint local_ip_;
  ASLL(ObRpcServerNetHandler, all_server_link_) all_server_list_; //same as write_enable_list_
  ASLL(ObRpcServerNetHandler, free_server_link_) free_server_list_;
  // ASLL(ObRpcServerNetHandler, in_ssl_server_link_) in_ssl_server_list_;
  ASLL(ObRpcServerNetHandler, in_connect_server_link_) in_connect_server_list_;
  // ASLL(ObRpcServerNetHandler, in_work_server_link_) in_work_server_list_;
  // Link<ObRpcServerNetHandler *> all_server_entry_;  // 所有的server entry
  // Link<ObRpcServerNetHandler *> free_server_entry_; // 空闲的server entry
  // Link<ObRpcServerNetHandler *> in_ssl_auth_entry_; // 进行认证阶段的entry
  // Link<ObRpcReq *> obrpc_req_link_;   //待发送链表
  // common::ObDList<ObRpcReq> writing_req_list_;

  ObRpcReqList waiting_req_list_;

  event::ObEThread *create_thread_;

  int64_t cur_server_entry_count_;
  int64_t max_server_entry_count_;
  int64_t access_times_;
  int64_t last_access_timestamp_;  //trace info
  int64_t connnectiong_server_entry_count_;  //正处于连接阶段的server entry数量；
  int64_t server_connect_error_conut_;
  int64_t last_build_connect_timestamp_;
  int64_t last_connect_failed_timestamp_;
  int64_t last_connect_success_timestamp_;
  // event::ObAction *timeout_action_;
  event::ObAction *send_request_action_;
  event::ObAction *period_task_action_;
  event::ObAction *retry_request_action_;
};

class ObRpcServerNetTableEntryPool : public event::ObContinuation
{
public:
  // Default constructor.
  // Constructs an empty pool.
  ObRpcServerNetTableEntryPool(event::ObProxyMutex *mutex = NULL);
  virtual ~ObRpcServerNetTableEntryPool() { }

  // Handle events from server sessions.
  int event_handler(int event, void *data);
  // int state_observer_open(int event, void *data);
public:
  static const int64_t HASH_BUCKET_SIZE = 16;

public:
  // Interface class for IP map.
  struct ObRpcIPHashing
  {
    typedef const ObRpcServerNetHandleHashKey Key;
    typedef ObRpcServerNetTableEntry Value;
    typedef ObDLList(ObRpcServerNetTableEntry, ip_hash_link_) ListHead;

    static uint64_t hash(Key key) { return common::murmurhash(&key.server_ip_->sa_, sizeof(sockaddr), 0); }
    static Key key(Value const *value)
    {
      ObRpcServerNetHandleHashKey key;
      if (OB_NOT_NULL(value)) {
        key = value->get_server_handler_hash_key();
      }
      return key;
    }

    static bool equal(Key lhs, Key rhs) { return lhs.equal(rhs); }
  };

  typedef common::hash::ObBuildInHashMap<ObRpcIPHashing, HASH_BUCKET_SIZE> RpcIPHashTable; // ObRpcServerNetTableEntry by IP address.

public:
  // Get a ObRpcServerNetTableEntry from the pool.
  // The ObRpcServerNetTableEntry is selected based on address. If found the session
  // is removed from the pool.
  //
  // @return A pointer to the session or NULL if not matching session was found.
  ObRpcServerNetTableEntry *acquire_server_table_entry(const ObRpcServerNetHandleHashKey &key);
  ObRpcServerNetTableEntry *acquire_server_table_entry(const sockaddr &addr, const ObString &auth_user);
  int put_server_table_entry(ObRpcServerNetTableEntry *table_entry, const ObRpcServerNetHandleHashKey &key);

  ObRpcServerNetTableEntry *acquire_server_table_entry(ObRpcReq &key);

  int release_server_entry(ObRpcServerNetTableEntry &rst);
  int handle_rpc_request(ObRpcReq &request);

  //close all server entry and clear table entry
  void purge();
  void destroy();
  // int64_t to_string(char *buf, const int64_t buf_len) const;


public:
  RpcIPHashTable ip_pool_;

  LINK(ObRpcServerNetTableEntryPool, rpc_server_entry_pool_hash_link_);

private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcServerNetTableEntryPool);

};

inline ObRpcServerNetTableEntryPool &get_rpc_server_net_handler_map(const event::ObEThread &t)
{
  return *(const_cast<event::ObEThread *>(&t)->rpc_net_ss_map_);
}

inline int check_and_clean_server_request_list(ObRpcReqList &req_list);

int init_rpc_net_ss_map_for_thread();

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_RPC_SERVER_NET_HANDLER_H