/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_SHOW_GLOBAL_SESSION_HANDLER_H
#define OBPROXY_SHOW_GLOBAL_SESSION_HANDLER_H
#include "cmd/ob_internal_cmd_handler.h"
#include "proxy/mysql/ob_mysql_client_session.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObShowGlobalSessionHandler : public ObInternalCmdHandler
{
public:
  ObShowGlobalSessionHandler(event::ObContinuation *cont, event::ObMIOBuffer *buf, const ObInternalCmdInfo &info);
  virtual ~ObShowGlobalSessionHandler() {}
  int handle_show_global_session_info(int event, void *data);
  int dump_header();
  int dump_body();
private:
  int dump_session_header();
  int dump_session_body();
  int dump_session_info_header();
  int dump_session_info_body(const common::ObString& dbkey);
  int dump_session_info_body_all();
private:
  ObProxyBasicStmtSubType sub_type_;
  const ObInternalCmdInfo* cmd_info_;
  DISALLOW_COPY_AND_ASSIGN(ObShowGlobalSessionHandler);
};
int show_global_session_info_cmd_init();

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_SHOW_GLOBAL_SESSION_HANDLER_H