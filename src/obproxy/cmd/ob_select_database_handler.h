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
  ObSelectDatabaseHandler(event::ObMIOBuffer *buf, uint8_t pkg_seq, int64_t memory_limit);
  virtual ~ObSelectDatabaseHandler() {}
  int handle_select_database(const ObString &logic_database_name);

  static int select_database_cmd_callback(event::ObMIOBuffer *buf, uint8_t pkg_seq, int64_t memory_limit, const ObString &logic_database_name);

private:
  int dump_header();
  int dump_payload(const ObString &logic_database_name);

  DISALLOW_COPY_AND_ASSIGN(ObSelectDatabaseHandler);
};

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_SELECT_DATABASE_HANDLER_H */
