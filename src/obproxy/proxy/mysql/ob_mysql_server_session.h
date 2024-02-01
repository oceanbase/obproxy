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

#ifndef OBPROXY_SERVER_SESSION_H
#define OBPROXY_SERVER_SESSION_H

#include "iocore/net/ob_net.h"
#include "cmd/ob_internal_cmd_handler.h"
#include "obutils/ob_proxy_json_config_info.h"
#include "proxy/mysql/ob_mysql_global_session_utils.h"
#include "obutils/ob_connection_diagnosis_trace.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
#define  SERVER_ADDR_LOOKUP_EVENT_DONE   INTERNAL_CMD_EVENTS_START + 3
#define  UINT24_MAX (16777215U)

class ObMysqlClientSession;

enum ObMSSState
{
  MSS_INIT = 0,
  MSS_ACTIVE,
  MSS_KA_CLIENT_SLAVE,
  MSS_KA_SHARED,
  MSS_MAX
};

enum ObServerSessionMagic
{
  MYSQL_SS_MAGIC_ALIVE = 0x0123FEED,
  MYSQL_SS_MAGIC_DEAD = 0xDEADFEED
};

class ObMysqlServerSessionHashKey
{
public:
  ObMysqlServerSessionHashKey() :
    local_ip_(NULL), server_ip_(NULL), auth_user_(NULL) {}
  ~ObMysqlServerSessionHashKey() {}

  bool equal (const ObMysqlServerSessionHashKey& rhs) const
  {
    if (!net::ops_ip_addr_port_eq(server_ip_->sa_, rhs.server_ip_->sa_)) {
      return false;
    }

    if (local_ip_ != NULL && rhs.local_ip_ != NULL) {
      if (!net::ops_ip_addr_port_eq(local_ip_->sa_, rhs.local_ip_->sa_)) {
        return false;
      }
    }

    if (auth_user_ != NULL && rhs.auth_user_ != NULL)  {
      if (*auth_user_ != *rhs.auth_user_)  {
        return false;
      }
    }
    return true;
  }

  TO_STRING_KV(KPC_(local_ip), KPC_(server_ip), KPC_(auth_user));

  const net::ObIpEndpoint *local_ip_;
  const net::ObIpEndpoint *server_ip_;
  const ObString *auth_user_;
};

class ObMysqlServerSession : public event::ObVConnection
{
public:
  ObMysqlServerSession()
      : event::ObVConnection(NULL), common_addr_(), local_ip_(), server_ip_(), auth_user_(),
        server_sessid_(0), ss_id_(0), transact_count_(0), state_(MSS_INIT), compressed_seq_(0),
        server_trans_stat_(0), read_buffer_(NULL), is_from_pool_(false), is_pool_session_(false),
        has_global_session_lock_(false), create_time_(0), last_active_time_(0),
        timeout_event_(obutils::OB_TIMEOUT_UNKNOWN_EVENT), timeout_record_(0),
        is_inited_(false), magic_(MYSQL_SS_MAGIC_DEAD), server_vc_(NULL),
        buf_reader_(NULL), session_info_(), client_session_(NULL)
  {
    ObRandom rand1;
    request_id_ = rand1.get_int32(0, UINT24_MAX);
  }
  virtual ~ObMysqlServerSession() { }

  void destroy();
  int new_connection(ObMysqlClientSession &client_session, net::ObNetVConnection &new_vc);

  int reset_read_buffer()
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(NULL == buf_reader_) || OB_UNLIKELY(NULL == read_buffer_)) {
      ret = common::OB_ERR_UNEXPECTED;
      PROXY_SS_LOG(WDIAG, "invalid read_buffer", K(buf_reader_), K(read_buffer_), K(ret));
    } else {
      read_buffer_->dealloc_all_readers();
      read_buffer_->writer_ = NULL;
      buf_reader_ = read_buffer_->alloc_reader();
    }
    return ret;
  }

  event::ObIOBufferReader *get_reader() { return buf_reader_; };

  virtual event::ObVIO *do_io_read(event::ObContinuation *c,
                                   const int64_t nbytes = INT64_MAX,
                                   event::ObMIOBuffer *buf = 0);
  virtual event::ObVIO *do_io_write(event::ObContinuation *c = NULL,
                                    const int64_t nbytes = INT64_MAX,
                                    event::ObIOBufferReader *buf = 0);

  virtual void do_io_close(const int lerrno = -1);
  virtual void do_io_shutdown(const event::ShutdownHowToType howto);
  virtual void reenable(event::ObVIO *vio);

  int release();
  net::ObNetVConnection *get_netvc() const { return server_vc_; }

  inline void set_client_session(ObMysqlClientSession &client_session) { client_session_ = &client_session; }
  inline void clear_client_session() { client_session_ = NULL; }
  inline ObMysqlClientSession *get_client_session() { return client_session_; }
  inline ObServerSessionInfo &get_session_info() { return session_info_; }
  inline const ObServerSessionInfo &get_session_info() const { return session_info_; }
  const char *get_state_str() const;

  // set inactivity to the value timeout(in nanoseconds)
  void set_inactivity_timeout(ObHRTime timeout, obutils::ObInactivityTimeoutEvent event);
  // cancel the inactivity timeout
  void cancel_inactivity_timeout();

  // server cap judgement
  bool is_ob_protocol_v2_supported() const { return session_info_.is_ob_protocol_v2_supported(); }
  bool is_checksum_supported() const { return session_info_.is_checksum_supported(); }
  bool is_checksum_on() const { return session_info_.is_checksum_on(); }
  bool is_extra_ok_packet_for_stats_enabled() const { return session_info_.is_extra_ok_packet_for_stats_enabled(); }
  bool is_full_link_trace_supported() const { return session_info_.is_full_link_trace_supported(); }
  bool is_full_link_trace_ext_enabled() const {
    return session_info_.is_full_link_trace_ext_supported();
  }

  uint32_t get_server_sessid() const { return server_sessid_; }
  void set_server_sessid(const uint32_t server_sessid) { server_sessid_ = server_sessid; }

  uint32_t get_server_request_id() const { return request_id_; }
  uint32_t get_next_server_request_id()
  {
    request_id_++;
    if (request_id_ > UINT24_MAX) {
      request_id_ = 0;
    }
    return request_id_;
  }

  uint8_t get_compressed_seq() const { return compressed_seq_; }
  void set_compressed_seq(const uint8_t compressed_seq) { compressed_seq_ = compressed_seq; }
  obutils::ObInactivityTimeoutEvent get_inactivity_timeout_event() const { return timeout_event_;}
  ObHRTime get_timeout_record() const { return timeout_record_; }
  ObMysqlServerSessionHashKey get_server_session_hash_key() const
  {
    ObMysqlServerSessionHashKey key;
    key.local_ip_ = &local_ip_;
    key.server_ip_ = &server_ip_;
    key.auth_user_ = &auth_user_;
    return key;
  }

  DECLARE_TO_STRING;

public:
  // Round Trip Time between proxy and server, 200ms, choose a better later
  static const int64_t RTT_BETWEEN_PROXY_AND_SERVER = HRTIME_MSECONDS(200);
  // when receive 'quit' cmd, obproxy should disconnect(in 1ms) after sending it to observer
  static const int64_t QUIT_TIMEOUT = HRTIME_MSECONDS(1);

  //common_addr for global session pool
  proxy::ObCommonAddr common_addr_;
  // Keys for matching hostnames
  net::ObIpEndpoint local_ip_;
  net::ObIpEndpoint server_ip_;
  ObString auth_user_;
  char full_name_buf_[OB_PROXY_FULL_USER_NAME_MAX_LEN];

  uint32_t server_sessid_;
  int64_t ss_id_;
  int64_t transact_count_;
  ObMSSState state_;

  uint32_t request_id_;
  uint8_t compressed_seq_;

  // Used to verify we are recording the server
  // transaction stat properly
  int64_t server_trans_stat_;

  LINK(ObMysqlServerSession, ip_hash_link_);
  LINK(ObMysqlServerSession, dbkey_local_ip_hash_link_);
  LINK(ObMysqlServerSession, ip_list_link_);

  // The ServerSession owns the following buffer which use
  // for parsing the headers. The server session needs to
  // own the buffer so we can go from a keep-alive state
  // to being acquired and parsing the header without
  // changing the buffer we are doing I/O on.
  event::ObMIOBuffer *read_buffer_;

  bool is_from_pool_;
  bool is_pool_session_;
  bool has_global_session_lock_;
  ObProxySchemaKey schema_key_;
  int64_t create_time_;
  int64_t last_active_time_;
  obutils::ObInactivityTimeoutEvent timeout_event_;     // just record timeout event for log
  ObHRTime timeout_record_;                             // just record timeout for log

private:
  static int64_t get_next_ss_id();
  int64_t get_round_trip_time() const { return RTT_BETWEEN_PROXY_AND_SERVER; }

private:
  bool is_inited_;
  int magic_;
  net::ObNetVConnection *server_vc_;

  event::ObIOBufferReader *buf_reader_;

  ObServerSessionInfo session_info_;
  ObMysqlClientSession *client_session_;
  DISALLOW_COPY_AND_ASSIGN(ObMysqlServerSession);
};

inline int64_t ObMysqlServerSession::get_next_ss_id()
{
  static int64_t next_ss_id = 1;
  int64_t ret = 0;
  do {
    ret = ATOMIC_FAA(&next_ss_id, 1);
  } while (0 == ret);
  return ret;
}

inline void ObMysqlServerSession::set_inactivity_timeout(ObHRTime timeout, obutils::ObInactivityTimeoutEvent event)
{
  if (OB_LIKELY(NULL != server_vc_)) {
    timeout_record_ = timeout;
    timeout_event_ = event;
    // server timeout value is (session_timeout + 2 * RTT(proxy<->server))
    server_vc_->set_inactivity_timeout(timeout + (get_round_trip_time() * 2));
  }
}

inline void ObMysqlServerSession::cancel_inactivity_timeout()
{
  if (OB_LIKELY(NULL != server_vc_)) {
    server_vc_->cancel_inactivity_timeout();
  }
}

class ObServerAddrLookupHandler : public event::ObContinuation
{
public:
  ObServerAddrLookupHandler(event::ObProxyMutex &mutex, event::ObContinuation &cont, const ObProxyKillQueryInfo &query_info);
  virtual ~ObServerAddrLookupHandler() { cs_id_array_.destroy(); }

  event::ObAction &get_action() { return action_; }
  int main_handler(int event, void *data);
  int handle_lookup_with_proxy_conn_id(int event, void *data);
  int handle_lookup_with_server_conn_id(int event, void *data);
  int lookup_server_addr_with_server_conn_id(const event::ObEThread &ethread, bool &is_finished);
  int handle_callback(int event, void *data);

  static int lookup_server_addr(event::ObContinuation &cont,
                                const ObProxySessionPrivInfo &priv_info,
                                ObProxyKillQueryInfo &query_info,
                                event::ObAction *&addr_lookup_action_handle);
  static int create_continuation(ObContinuation &cont,
                                 const ObProxySessionPrivInfo &priv_info,
                                 const ObProxyKillQueryInfo &query_info,
                                 ObServerAddrLookupHandler *&handler,
                                 event::ObProxyMutex *&mutex);
  static int lookup_server_addr(const ObMysqlClientSession &cs,
                                const ObProxySessionPrivInfo &priv_info,
                                ObProxyKillQueryInfo &query_info);
  static int lookup_server_addr_with_proxy_conn_id(const event::ObEThread &ethread,
                                                   const ObProxySessionPrivInfo &priv_info,
                                                   ObProxyKillQueryInfo &query_info);
  static int build_kill_query_sql(const uint32_t conn_id, common::ObSqlString &sql);

public:
  ObProxySessionPrivInfo priv_info_;

private:
  int saved_event_;
  event::ObEThread *submit_thread_;
  ObProxyKillQueryInfo query_info_;
  event::ObAction action_;
  CSIDHanders cs_id_array_;
  DISALLOW_COPY_AND_ASSIGN(ObServerAddrLookupHandler);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_SERVER_SESSION_H
