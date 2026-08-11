/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_SHOW_TRACE_HANDLER_H
#define OBPROXY_SHOW_TRACE_HANDLER_H

#include "stat/ob_proxy_trace_stats.h"
#include "cmd/ob_internal_cmd_handler.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObShowTraceHandler : public ObInternalCmdHandler
{
public:
  ObShowTraceHandler(event::ObContinuation *cont, event::ObMIOBuffer *buf, const ObInternalCmdInfo &info);
  virtual ~ObShowTraceHandler() { cs_id_array_.destroy(); }
  int handle_trace(int event, void *data);
  virtual CSIDHanders *get_cs_id_array() { return &cs_id_array_; }

private:
  int dump_trace_header();
  int dump_trace(const ObMysqlClientSession &cs);
  int dump_trace_item(const ObTraceRecord &item);

public:
  const ObProxyBasicStmtSubType sub_type_;
  int64_t attempt_limit_;
  CSIDHanders cs_id_array_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObShowTraceHandler);
};

int show_trace_cmd_init();

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_SHOW_TRACE_HANDLER_H */

