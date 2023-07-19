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
class ObShowDatabasesHandler : public ObCmdHandler
{
public:
  ObShowDatabasesHandler(event::ObMIOBuffer *buf, ObCmdInfo &info);
  virtual ~ObShowDatabasesHandler() {}
  int handle_show_databases(const ObString &logic_tenant_name, proxy::ObMysqlClientSession &client_session);

  static int show_databases_cmd_callback(event::ObMIOBuffer *buf, ObCmdInfo &info,
                                         const ObString &logic_tenant_name,
                                         proxy::ObMysqlClientSession &client_session);

private:
  int dump_database_header();
  int dump_database(const ObString &logic_tenant_name, proxy::ObMysqlClientSession &client_session);

  DISALLOW_COPY_AND_ASSIGN(ObShowDatabasesHandler);
};
} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_SHOW_DATABASES_HANDLER_H */
