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

#ifndef OBPROXY_SHOW_KV_HANDLER_H
#define OBPROXY_SHOW_KV_HANDLER_H

#include "cmd/ob_internal_cmd_handler.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

class ObShowKvHandler : public ObInternalCmdHandler
{
public:
  ObShowKvHandler(ObContinuation *cont, event::ObMIOBuffer *buf, const ObInternalCmdInfo &info)
      : oceanbase::obproxy::ObInternalCmdHandler(cont, buf, info), sub_type_(info.get_sub_cmd_type()),
        start_worker_thread_id_(-1), start_async_thread_id_(-1), cur_async_thread_id_(-1), is_worker_thread_finished_(false),
        is_async_thread_finished_(false)
  {
    SET_HANDLER(&ObShowKvHandler::handle_kv_thread_cmd);
  }
  virtual ~ObShowKvHandler() {}

  int handle_kv_thread_cmd(int event, void *data);
  int dump_kv_thread_cmd_header();

  int show_kv_thread_list(int event, void *data);
  int show_kv_thread_in_thread(event::ObEThread &thread);

private:
  const ObProxyBasicStmtSubType sub_type_;
  int64_t start_worker_thread_id_;
  int64_t start_async_thread_id_;
  int64_t cur_async_thread_id_;
  bool is_worker_thread_finished_;
  bool is_async_thread_finished_;

  DISALLOW_COPY_AND_ASSIGN(ObShowKvHandler);
};

int show_kv_cmd_init();

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_SHOW_SESSION_HANDLER_H */
