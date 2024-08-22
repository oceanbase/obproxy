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
#ifndef OBPROXY_RPC_NET_HANDLER_H
#define OBPROXY_RPC_NET_HANDLER_H

#include "iocore/net/ob_net.h"
#include "cmd/ob_internal_cmd_handler.h"
#include "obutils/ob_proxy_json_config_info.h"
#include "proxy/mysql/ob_mysql_global_session_utils.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
#define  SERVER_ADDR_LOOKUP_EVENT_DONE   INTERNAL_CMD_EVENTS_START + 3
#define  UINT24_MAX (16777215U)

static int64_t const RPC_BUFFER_SIZE = BUFFER_SIZE_FOR_INDEX(BUFFER_SIZE_INDEX_8K);
// class ObRpcSM;
// class ObRpcClientSession;
class ObRpcReq;

enum ObRpcNetMSSState
{
  RPC_NET_MSS_INIT = 0,
  RPC_NET_MSS_ACTIVE,
  RPC_NET_MSS_KA_CLIENT_SLAVE,
  RPC_NET_MSS_KA_SHARED,
  RPC_NET_MSS_MAX
};

enum ObRpcNetHandlerMagic
{
  RPC_NET_SS_MAGIC_ALIVE = 0x0123FEED,
  RPC_NET_SS_MAGIC_DEAD = 0xDEADFEED
};

enum ObRpcReqReadStatus
{
  RPC_REQUEST_READ_ERROR = -1,
  RPC_REQUEST_READ_DONE = 0,
  RPC_REQUEST_READ_OK = 1,
  RPC_REQUEST_READ_CONT = 2
};

class ObRpcNetHandler : public event::ObVConnection
{
public:
  ObRpcNetHandler()
      : event::ObVConnection(NULL), server_sessid_(0), ss_id_(0),
        state_(RPC_NET_MSS_INIT), server_trans_stat_(0),
        read_buffer_(NULL),
        eos_(false), create_time_(0), last_active_time_(0), read_begin_(0), write_begin_(0),
        net_entry_(), is_inited_(false), magic_(RPC_NET_SS_MAGIC_ALIVE), rpc_net_vc_(NULL),
        buf_reader_(NULL), conn_prometheus_decrease_(false)
  {
    memset(&local_ip_, 0, sizeof(local_ip_));
    memset(&server_ip_, 0, sizeof(server_ip_));
    ObRandom rand1;
    request_id_ = rand1.get_int32(0, UINT24_MAX);
  }

  virtual ~ObRpcNetHandler() {}

  void destroy();
  void cleanup() {}
  int new_connection(net::ObNetVConnection &new_vc);

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
  net::ObNetVConnection *get_netvc() const { return rpc_net_vc_; }

  const char *get_state_str() const;

  // set inactivity to the value timeout(in nanoseconds)
  void set_inactivity_timeout(ObHRTime timeout);
  // cancel the inactivity timeout
  void cancel_inactivity_timeout();

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
  void set_conn_prometheus_decrease(bool conn_prometheus_decrease) { conn_prometheus_decrease_ = conn_prometheus_decrease; }

  class ObRpcNetVCTableEntry
  {
  public:
    ObRpcNetVCTableEntry() :
      eos_(false), in_tunnel_(false),
      vc_(NULL),
      read_buffer_(NULL),
      write_buffer_(NULL),
      read_vio_(NULL),
      write_vio_(NULL) {}
    virtual ~ObRpcNetVCTableEntry() {}
    bool eos_;
    bool in_tunnel_;
    event::ObVConnection *vc_;
    event::ObMIOBuffer *read_buffer_;
    event::ObMIOBuffer *write_buffer_;
    event::ObVIO *read_vio_;
    event::ObVIO *write_vio_;
  };

  DECLARE_TO_STRING;

public:
  // Round Trip Time between proxy and server, 200ms, choose a better later @gujian
  static const int64_t RTT_BETWEEN_PROXY_AND_SERVER = HRTIME_MSECONDS(200);

  net::ObIpEndpoint local_ip_;
  net::ObIpEndpoint server_ip_;

  uint32_t server_sessid_;
  int64_t ss_id_;
  // int64_t transact_count_;
  ObRpcNetMSSState state_;

  uint32_t request_id_;
  uint8_t compressed_seq_;

  // Used to verify we are recording the server
  // transaction stat properly
  int64_t server_trans_stat_;
  event::ObMIOBuffer *read_buffer_;

  bool eos_; //is broken
  int64_t create_time_;
  int64_t last_active_time_;
  ObHRTime read_begin_;
  ObHRTime write_begin_;
  ObRpcNetVCTableEntry net_entry_; //net data handler

private:
  static int64_t get_next_ss_id();
  int64_t get_round_trip_time() const { return RTT_BETWEEN_PROXY_AND_SERVER; }

public:
  bool is_inited_;
  int magic_;
  net::ObNetVConnection *rpc_net_vc_;

  event::ObIOBufferReader *buf_reader_;
    bool conn_prometheus_decrease_;
  DISALLOW_COPY_AND_ASSIGN(ObRpcNetHandler);
};

inline int64_t ObRpcNetHandler::get_next_ss_id()
{
  static int64_t next_ss_id = 1;
  int64_t ret = 0;
  do {
    ret = ATOMIC_FAA(&next_ss_id, 1);
  } while (0 == ret);
  return ret;
}

inline void ObRpcNetHandler::set_inactivity_timeout(ObHRTime timeout)
{
  if (OB_LIKELY(NULL != rpc_net_vc_)) {
    rpc_net_vc_->set_inactivity_timeout(timeout);
  }
}

inline void ObRpcNetHandler::cancel_inactivity_timeout()
{
  if (OB_LIKELY(NULL != rpc_net_vc_)) {
    rpc_net_vc_->cancel_inactivity_timeout();
  }
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_RPC_NET_HANDLER_H
