/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_SHOW_DB_VERSION_HANDLER_H
#define OBPROXY_SHOW_DB_VERSION_HANDLER_H

#include "cmd/ob_cmd_handler.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
class ObShowDBVersionHandler : public ObCmdHandler
{
public:
  ObShowDBVersionHandler(event::ObMIOBuffer *buf, ObCmdInfo &info);
  virtual ~ObShowDBVersionHandler() {}
  int handle_show_db_version(const ObString &logic_tenant_name, const ObString &logic_db_name);

  static int show_db_version_cmd_callback(event::ObMIOBuffer *buf, ObCmdInfo &info,
                                          const ObString &logic_tenant_name,
                                          const ObString &logic_db_name);

private:
  int dump_db_version_header();
  int dump_db_version(const ObString &logic_tenant_name, const ObString &logic_db_name);

  DISALLOW_COPY_AND_ASSIGN(ObShowDBVersionHandler);
};
} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_SHOW_DB_VERSION_HANDLER_H */
