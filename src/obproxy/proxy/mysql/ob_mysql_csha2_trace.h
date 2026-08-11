/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_MYSQL_CSHA2_TRACE_H
#define OBPROXY_MYSQL_CSHA2_TRACE_H

#include "lib/utility/ob_macro_utils.h"
#include "proxy/mysql/ob_mysql_transact.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

class ObMysqlCsha2Trace
{
public:
  static void trace_cached_login(const char *stage, ObMysqlClientSession *cs);
  static void trace_server_auth_switch_req(const char *stage, ObMysqlTransact::ObTransState &s);
  static void trace_first_response_packet(ObMysqlTransact::ObTransState &s);

private:
  ObMysqlCsha2Trace();
  DISALLOW_COPY_AND_ASSIGN(ObMysqlCsha2Trace);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_MYSQL_CSHA2_TRACE_H
