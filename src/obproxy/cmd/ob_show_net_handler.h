/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_SHOW_NET_HANDLER_H
#define OBPROXY_SHOW_NET_HANDLER_H

#include "cmd/ob_internal_cmd_handler.h"

namespace oceanbase
{
namespace obproxy
{
namespace net
{
class ObNetHandler;

class ObShowNetHandler : public ObInternalCmdHandler
{
public:
  ObShowNetHandler(event::ObContinuation *cont, event::ObMIOBuffer *buf, const ObInternalCmdInfo &info);
  virtual ~ObShowNetHandler() {}
  int handle_show_threads(int event, event::ObEvent *e);
  int handle_show_connections(int event, event::ObEvent *e);

private:
  int show_single_thread(int event, event::ObEvent *e);
  int dump_thread_header();
  int dump_single_thread(event::ObEThread *ethread, const ObNetHandler &nh);

  int show_connections(int event, event::ObEvent *e);
  int dump_connections_header();
  int dump_connections_on_thread(const event::ObEThread *ethread, const ObNetHandler *nh);

private:
  const int64_t thread_id_;
  const int64_t limit_rows_;
  const int64_t limit_offset_;
  int64_t next_thread_id_;

  DISALLOW_COPY_AND_ASSIGN(ObShowNetHandler);
};

int show_net_cmd_init();
} // end of namespace net
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_SHOW_NET_HANDLER_H

