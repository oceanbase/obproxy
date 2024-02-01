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

#ifndef OBPROXY_CLIENT_VC_H
#define OBPROXY_CLIENT_VC_H

#include "proxy/client/ob_client_utils.h"
#include "iocore/net/ob_net_vconnection.h"
#include "proxy/mysql/ob_mysql_global_session_utils.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
class ObClusterResource;
}
namespace proxy
{
#define CLIENT_DESTROY_SELF_EVENT (CLIENT_EVENT_EVENTS_START + 1)
#define CLIENT_VC_DISCONNECT_EVENT (CLIENT_EVENT_EVENTS_START + 2)
#define CLIENT_TRANSPORT_MYSQL_RESP_EVENT (CLIENT_EVENT_EVENTS_START + 3)
#define CLIENT_MYSQL_RESP_TRANSFER_COMPLETE_EVENT (CLIENT_EVENT_EVENTS_START + 4)
#define CLIENT_INFORM_MYSQL_CLIENT_TRANSFER_RESP_EVENT (CLIENT_EVENT_EVENTS_START + 5)
#define CLIENT_VC_SWAP_MUTEX_EVENT (CLIENT_EVENT_EVENTS_START + 6)
#define CLIENT_VC_DISCONNECT_LAST_USED_SS_EVENT (CLIENT_EVENT_EVENTS_START + 7)

struct ObClientVCState
{
  ObClientVCState() : vio_() {};
  ~ObClientVCState() {}

  event::ObVIO vio_;
};

class ObMysqlRequestParam;
class ObMysqlClient;
class ObMysqlClientSession;
class ClientPoolOption
{
public:
  ClientPoolOption(): need_skip_stage2_(false),
    is_session_pool_client_(false) {}
  bool need_skip_stage2_;
  bool is_session_pool_client_;
  ObProxySchemaKey schema_key_;
  proxy::ObCommonAddr server_addr_;
};

class ObClientVC : public net::ObNetVConnection
{
public:
  explicit ObClientVC(ObMysqlClient &client_core);
  virtual ~ObClientVC() {}

  int main_handler(int event, void *data);

  virtual event::ObVIO *do_io_read(event::ObContinuation *c = NULL,
                                   const int64_t nbytes = INT64_MAX,
                                   event::ObMIOBuffer *buf = 0);

  virtual event::ObVIO *do_io_write(event::ObContinuation *c = NULL,
                                    const int64_t nbytes = INT64_MAX,
                                    event::ObIOBufferReader *buf = 0);

  virtual void do_io_close(const int lerrno = -1);
  virtual void do_io_shutdown(const event::ShutdownHowToType howto) { UNUSED(howto); }

  // Reenable a given vio. The public interface is through ObVIO::reenable
  virtual void reenable(event::ObVIO *vio) { reenable_re(vio); }
  virtual void reenable_re(event::ObVIO *vio);
  void reenable_read() { reenable_re(&read_state_.vio_); }
  int transfer_bytes();

  // Timeouts
  virtual int set_active_timeout(ObHRTime timeout_in) { UNUSED(timeout_in); return 0; }
  virtual void set_inactivity_timeout(ObHRTime timeout_in) { UNUSED(timeout_in); }
  virtual void cancel_active_timeout() {}
  virtual void cancel_inactivity_timeout() {}
  virtual void add_to_keep_alive_lru() {}
  virtual void remove_from_keep_alive_lru() {}
  virtual ObHRTime get_active_timeout() const { return 0; }
  virtual ObHRTime get_inactivity_timeout() const { return 0; }

  // Pure virtual functions we need to compile
  virtual int get_socket() { return 0; }
  virtual int set_local_addr() { return 0; }
  virtual bool set_remote_addr() { return true; }
  virtual int set_virtual_addr() { return 0; }
  virtual int get_conn_fd() { return 0; }
  virtual int set_tcp_init_cwnd(int init_cwnd) { UNUSED(init_cwnd); return 0; }
  virtual int apply_options() { return 0; }

  void clear_request_sent() { is_request_sent_ = false; }
  void set_addr(const common::ObAddr &addr) { addr_ = addr; }

private:
  uint32_t magic_;
  bool disconnect_by_client_; // dissconnect by received VC_EVENT_EOS, sent by ObMysqlClient
  bool is_request_sent_; // used to inform client session read request only once
  ObMysqlClient *core_client_;
  event::ObAction *pending_action_;

  ObClientVCState read_state_;
  ObClientVCState write_state_;
  common::ObAddr addr_;
  DISALLOW_COPY_AND_ASSIGN(ObClientVC);
};

class ObMysqlClientPool;
class ObMysqlClient : public event::ObContinuation
{
public:
  enum ObClientActionType
  {
    CLIENT_ACTION_UNDEFINED = 0,
    CLIENT_ACTION_CONNECT,
    CLIENT_ACTION_READ_HANDSHAKE,
    CLIENT_ACTION_SET_AUTOCOMMIT,
    CLIENT_ACTION_READ_LOGIN_RESP,
    CLIENT_ACTION_READ_NORMAL_RESP,
  };

  ObMysqlClient();
  virtual ~ObMysqlClient() {};
  int main_handler(int event, void *data);

  static int alloc(ObMysqlClientPool *pool,
                   ObMysqlClient *&client,
                   const common::ObString &user_name,
                   const common::ObString &password,
                   const common::ObString &database,
                   const bool is_meta_mysql_client,
                   const common::ObString &cluster_name = "",
                   const common::ObString &password1 = "",
                   ClientPoolOption* client_pool_option = NULL);

  int init(ObMysqlClientPool *pool,
           const common::ObString &user_name,
           const common::ObString &password,
           const common::ObString &database,
           const bool is_meta_mysql_client,
           const common::ObString &cluster_name = "",
           const common::ObString &password1 = "",
           ClientPoolOption* client_pool_option = NULL);

  int init_detect_client(obutils::ObClusterResource *cr);
  int init_binlog_client(const common::ObString cluster_name, const common::ObString tenant_name);

  // must be used under the mutex_'s lock
  bool is_avail() const { return !in_use_; }

  // Attention!! before use post request, must comfirm:
  // 1. under the mutex_'s lock
  // 2. this client is_avail();
  // 3. in ET_CALL(work) thread;
  int post_request(event::ObContinuation *cont,
                   const ObMysqlRequestParam &request_param,
                   const int64_t timeout_ms,
                   event::ObAction *&action);
  static const char *get_client_event_name(const int64_t event);

  event::ObProxyMutex *get_common_mutex() { return common_mutex_.ptr_; }
  void kill_this();
  event::ObMIOBuffer *get_client_buf() { return mysql_resp_->get_resp_miobuf(); }

private:
  static const char *get_client_action_name(const ObClientActionType type);

  int do_post_request();
  int do_next_action(void *data);
  int transfer_and_analyze_response(event::ObVIO &vio, const obmysql::ObMySQLCmd cmd);

  int notify_transfer_completed();

  int do_new_connection_with_cr(ObMysqlClientSession *client_session);
  int do_new_connection_with_shard_conn(ObMysqlClientSession *client_session);
  int setup_read_handshake();
  int setup_read_login_resp();
  int setup_read_autocommit_resp();
  int setup_read_normal_resp();
  bool is_in_auth() const;
  int transport_mysql_resp();
  int handle_client_vc_disconnect();
  int forward_mysql_request();

  int schedule_active_timeout();
  int cancel_active_timeout();
  int handle_active_timeout();
  int handle_request_complete();

  void release(bool is_need_check_reentry);

public:
  SLINK(ObMysqlClient, link_);
  SLINK(ObMysqlClient, mc_link_);

private:
  const static int64_t CLIENT_MIN_BLOCK_TRANSFER_BYTES = 512;
  const static int64_t RESCHEDULE_CLIENT_TRANSPORT_MYSQL_RESP_INTERVAL = HRTIME_MSECONDS(1); // 1ms

  uint32_t magic_;
  int32_t reentrancy_count_;
  bool terminate_;
  bool is_inited_;
  bool in_use_;
  bool is_request_complete_;
  bool use_short_connection_;
  ObClientVC *client_vc_;
  ObMysqlClientPool *pool_;
  // in some situtaion, we can create obmysql client without ClientPool
  obutils::ObClusterResource *cr_;

  event::ObAction *active_timeout_action_;
  common::ObPtr<event::ObProxyMutex> common_mutex_;
  event::ObAction action_;
  int64_t active_timeout_ms_;

  ObClientActionType next_action_;

  event::ObMIOBuffer *request_buf_;
  event::ObIOBufferReader *request_reader_;

  ObClientMysqlResp *mysql_resp_;
  ObClientRequestInfo info_;

  bool is_session_pool_client_;
  ObProxySchemaKey schema_key_;
  proxy::ObCommonAddr server_addr_;
  bool need_connect_retry_;
  int64_t retry_times_;
  DISALLOW_COPY_AND_ASSIGN(ObMysqlClient);
};

inline bool ObMysqlClient::is_in_auth() const
{
  return ((CLIENT_ACTION_READ_HANDSHAKE == next_action_)
          || (CLIENT_ACTION_READ_LOGIN_RESP == next_action_)
          || (CLIENT_ACTION_SET_AUTOCOMMIT == next_action_));
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_CLIENT_VC_H
