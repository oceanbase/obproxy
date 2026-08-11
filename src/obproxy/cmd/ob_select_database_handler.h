/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_SELECT_DATABASE_HANDLER_H
#define OBPROXY_SELECT_DATABASE_HANDLER_H

#include "cmd/ob_cmd_handler.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

class ObSelectDatabaseHandler : public ObCmdHandler
{
public:
  ObSelectDatabaseHandler(event::ObMIOBuffer *buf, ObCmdInfo &info);
  virtual ~ObSelectDatabaseHandler() {}
  int handle_select_database(const ObString &logic_database_name);

  static int select_database_cmd_callback(event::ObMIOBuffer *buf, ObCmdInfo &info,
                                          const ObString &logic_database_name);

private:
  int dump_header();
  int dump_payload(const ObString &logic_database_name);

  DISALLOW_COPY_AND_ASSIGN(ObSelectDatabaseHandler);
};

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_SELECT_DATABASE_HANDLER_H */
