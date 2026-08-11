/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_SHOW_KV_HANDLER_H
#define OBPROXY_SHOW_KV_HANDLER_H

#include "cmd/ob_internal_cmd_handler.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

#define OB_PROXY_MAX_RPC_REQ_TYPE_LENGTH 32

class ObShowKvHandler : public ObInternalCmdHandler
{
public:
  ObShowKvHandler(ObContinuation *cont, event::ObMIOBuffer *buf, const ObInternalCmdInfo &info);
  virtual ~ObShowKvHandler() {}

  int handle_kv_thread_cmd(int event, void *data);
  int handle_kv_requeststat_cmd(int event, void *data);
  int dump_header();

  int dump_kv_thread_item(int event, void *data);
  int dump_kv_thread_one_thread(event::ObEThread &thread);
  int dump_kv_requeststat_item(int event, void *data);
  int dump_kv_requeststat_one_thread(event::ObEThread &thread);
  int get_rpc_req_type(int64_t &type);

private:
  const ObProxyBasicStmtSubType sub_type_;
  int64_t ob_rpc_req_type_;
  int64_t start_worker_thread_id_;
  int64_t start_async_thread_id_;
  int64_t cur_async_thread_id_;
  bool is_worker_thread_finished_;
  bool is_async_thread_finished_;
  char rpc_req_type_str_[OB_PROXY_MAX_RPC_REQ_TYPE_LENGTH + 1];

  DISALLOW_COPY_AND_ASSIGN(ObShowKvHandler);
};

int show_kv_cmd_init();

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_SHOW_SESSION_HANDLER_H */
