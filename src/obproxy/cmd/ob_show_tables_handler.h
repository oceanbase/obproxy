/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_SHOW_TABLES_HANDLER_H
#define OBPROXY_SHOW_TABLES_HANDLER_H

#include "cmd/ob_cmd_handler.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
class ObShardingShowTablesHandler : public ObCmdHandler
{
public:
  ObShardingShowTablesHandler(event::ObMIOBuffer *buf, ObCmdInfo &info, ObProxyBasicStmtSubType sub_type);
  virtual ~ObShardingShowTablesHandler() {}
  int handle_show_tables(const ObString &logic_tenant_name, const ObString &logic_database_name,
                         ObString &logic_table_name);

  static int show_tables_cmd_callback(event::ObMIOBuffer *buf, ObCmdInfo &info, ObProxyBasicStmtSubType sub_type,
                                      const ObString &logic_tenant_name, const ObString &logic_database_name,
                                      ObString &logic_table_name);

private:
  int dump_table_header(const ObString &logic_database_name);
  int dump_table(const ObString &logic_tenant_name, const ObString &logic_database_name,
                 ObString &logic_table_name);

private:
  ObProxyBasicStmtSubType sub_type_;

  DISALLOW_COPY_AND_ASSIGN(ObShardingShowTablesHandler);
};
} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_SHOW_TABLES_HANDLER_H */
