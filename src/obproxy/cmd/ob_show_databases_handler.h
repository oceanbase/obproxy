/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_SHOW_DATABASES_HANDLER_H
#define OBPROXY_SHOW_DATABASES_HANDLER_H

#include "cmd/ob_cmd_handler.h"

namespace oceanbase
{
namespace obproxy
{

namespace proxy
{
  class ObMysqlClientSession;
}
namespace obutils
{

class ObSqlParseResult;
class ObShardingShowDatabasesHandler : public ObCmdHandler
{
public:
  ObShardingShowDatabasesHandler(event::ObMIOBuffer *buf, ObCmdInfo &info);
  virtual ~ObShardingShowDatabasesHandler() {}
  int handle_show_databases(const ObString &logic_tenant_name, proxy::ObMysqlClientSession &client_session);

  static int show_databases_cmd_callback(event::ObMIOBuffer *buf, ObCmdInfo &info,
                                         const ObString &logic_tenant_name,
                                         proxy::ObMysqlClientSession &client_session);

private:
  int dump_database_header();
  int dump_database(const ObString &logic_tenant_name, proxy::ObMysqlClientSession &client_session);

  DISALLOW_COPY_AND_ASSIGN(ObShardingShowDatabasesHandler);
};
} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_SHOW_DATABASES_HANDLER_H */
