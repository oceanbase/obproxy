/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_SHOW_CREATE_TABLE_HANDLER_H
#define OBPROXY_SHOW_CREATE_TABLE_HANDLER_H

#include "cmd/ob_cmd_handler.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
class ObShardingShowCreateTableHandler : public ObCmdHandler
{
public:
  ObShardingShowCreateTableHandler(event::ObMIOBuffer *buf, ObCmdInfo &info);
  virtual ~ObShardingShowCreateTableHandler() {}
  int handle_show_create_table(const ObString &logic_tenant_name, const ObString &logic_database_name,
                               const ObString &logic_table_name);
  static int show_create_table_cmd_callback(event::ObMIOBuffer *buf, ObCmdInfo &info,
                                            const ObString &logic_tenant_name, const ObString &logic_database_name,
                                            const ObString &logic_table_name);

  DISALLOW_COPY_AND_ASSIGN(ObShardingShowCreateTableHandler);
};
} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_SHOW_CREATE_TABLE_HANDLER_H */
