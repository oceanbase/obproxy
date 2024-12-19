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
#ifndef OBPROXY_RPC_REDIS_CLIENT_NET_HANDLER_H
#define OBPROXY_RPC_REDIS_CLIENT_NET_HANDLER_H

#include "iocore/net/ob_net.h"
#include "cmd/ob_internal_cmd_handler.h"
#include "obutils/ob_proxy_json_config_info.h"
#include "proxy/mysql/ob_mysql_global_session_utils.h"
#include "proxy/rpc/net/ob_rpc_client_net_handler.h"
#include "proxy/rpc/redis/ob_rpc_redis_ctx_cache.h"
#include "proxy/rpc/redis/ob_rpc_redis_monitor_cache.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

class ObRpcRedisClientNetHandler;
class ObRpcReq;
class ObRpcRedisCtx;
class ObRpcRedisMonitorMsg;
class ObRpcRedisClientNetHandler : public ObRpcClientNetHandler
{
public:

  ObRpcRedisClientNetHandler();
  virtual ~ObRpcRedisClientNetHandler() {}

  void destroy();
  // int new_connection(net::ObNetVConnection &new_vc);
  int new_connection(net::ObNetVConnection *new_vc, event::ObMIOBuffer *iobuf,
                     event::ObIOBufferReader *reader);
  int main_handler(int event, void *data);

  // int handle_other_event(int event, void *data);
  int schedule_period_task();
  int handle_period_task();
  int cancel_period_task();

  int cancel_pending_action();

  // virtual event::ObVIO *do_io_write(
  //   ObContinuation *c, const int64_t nbytes, event::ObIOBufferReader *buf);

  virtual void do_io_close(const int alerrno = 0);


  // int64_t to_string(char *buf, const int64_t buf_len) const;

  /** handle net info*/
  virtual int setup_client_request_read();

  virtual int state_client_request_read(int event, void *data);

  virtual int setup_client_response_send();

  virtual int state_client_response_send(int event, void *data);

  virtual void add_client_response_request(ObRpcReq *request);

  virtual int schedule_send_response_action();

  int setup_monitor_msg_send();
  common::ObString get_rpc_credential() { return credential_; }

  void set_rpc_credential(const common::ObString &credential);
  ObRpcRedisCtx *get_redis_ctx() { return redis_ctx_; }
  int add_redis_monitor_msg();

protected:
  ObRpcReq *cur_rpc_request_;
  bool is_in_handling_request_;
  uint64_t redis_db_;
  int64_t last_monitor_time_us_;
  char rpc_credential_[50];
  common::ObString credential_;
  event::ObAction *period_task_action_;
  ObRpcRedisCtx *redis_ctx_;
  common::ObList<ObRpcRedisMonitorMsg*> redis_monitor_list_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcRedisClientNetHandler);
};

} /* proxy */
} /* obproxy */
} /* oceanbase */

#endif /* OBPROXY_RPC_REDIS_CLIENT_NET_HANDLER_H*/