/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_MYSQL_DEBUG_NAMES_H
#define OBPROXY_MYSQL_DEBUG_NAMES_H

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

class ObMysqlDebugNames
{
public:
  static const char *get_event_name(int const event);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_MYSQL_DEBUG_NAME_H
