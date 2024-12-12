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
#ifndef OBPROXY_RPC_OBKV_CLIENT_NET_HANDLER_H
#define OBPROXY_RPC_OBKV_CLIENT_NET_HANDLER_H

#include "obkv/table/ob_rpc_struct.h"
#include "proxy/rpc/net/ob_rpc_net_handler.h"
#include "proxy/rpc/net/ob_rpc_client_net_handler.h"
#include "proxy/rpc/rpclib/ob_rpc_req_analyzer.h"
#include "proxy/rpc/net/ob_proxy_rpc_session_info.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

class ObRpcClientNetHandler;
class ObRpcClientNetHandlerMap;

#define CLIENT_NET_SET_DEFAULT_HANDLER(h) {        \
     cs_default_handler_ = (ClientNetHandler)h;    \
}

class ObRpcOBKVClientNetHandler : public ObRpcClientNetHandler
{
public:

  ObRpcOBKVClientNetHandler();
  virtual ~ObRpcOBKVClientNetHandler() {}

  virtual void destroy();
  // int new_connection(net::ObNetVConnection &new_vc);
  virtual int main_handler(int event, void *data);

  int handle_other_event(int event, void *data);
  // int state_keep_alive(int event, void *data);

  int schedule_period_task();
  int handle_period_task();
  int cancel_period_task();

  int schedule_send_response_action();
  int cancel_pending_action();

  // virtual event::ObVIO *do_io_write(
  //   ObContinuation *c, const int64_t nbytes, event::ObIOBufferReader *buf);
  virtual void do_io_close(const int alerrno = 0);

  int handle_response_rewrite_channel_id(ObRpcReq *request);
  int init_request_meta_info(ObRpcReq *request);

  // int64_t to_string(char *buf, const int64_t buf_len) const;

  /** handle net info*/
  int setup_client_request_read();
  int state_client_request_read(int event, void *data);

  int calc_response_need_send(int64_t &count);
  int store_rpc_req_into_response_buffer(int64_t need_send_resp_count, int64_t &send_response, int64_t &total_response_len);
  virtual int setup_client_response_send();
  // virtual int setup_client_response_direct_send();
  virtual int state_client_response_send(int event, void *data);

  int handle_client_entry_setup_error(int event, void *data);
  virtual void add_client_response_request(ObRpcReq *request);
  void clean_all_pending_request();
  void clean_all_timeout_request();

protected:
  uint32_t atomic_channel_id_;
  bool is_sending_response_;
  bool has_inited_;

  ObConnTenantInfo ct_info_;
  net::ObIpEndpoint last_server_ip_; /* only used for weak read when not have any route info (need use dummumy entry pll)*/
  typedef hash::ObHashMap<int32_t, ObRpcReq *, hash::NoPthreadDefendMode> RPC_PKT_REQ_MAP;
  RPC_PKT_REQ_MAP cid_to_req_map_;
  ObRpcReqList need_send_response_list_;
  ObRpcReqList sending_response_list_;
  event::ObAction *period_task_action_;
  // event::ObAction *pending_action_;

  int64_t current_need_read_len_;
  obkv::ObRpcEzHeader current_ez_header_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcOBKVClientNetHandler);
};

inline void ObRpcOBKVClientNetHandler::add_client_response_request(ObRpcReq *request)
{
  if (OB_NOT_NULL(request)) {
    PROXY_CS_LOG(DEBUG, "ObRpcOBKVClientNetHandler::add_client_response_request", K(request), K_(cs_id));
    need_send_response_list_.push_back(request);
  }
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_RPC_OBKV_CLIENT_NET_HANDLER_H
