/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_MYSQLCLIENT_OB_ISQL_CONNECTION_POOL_H_
#define OCEANBASE_MYSQLCLIENT_OB_ISQL_CONNECTION_POOL_H_

#include <stdint.h>

namespace oceanbase
{
namespace common
{
namespace sqlclient
{

class ObISQLConnection;

class ObISQLConnectionPool
{
public:
  ObISQLConnectionPool() {};
  virtual ~ObISQLConnectionPool() {};

  // sql string escape
  virtual int escape(const char *from, const int64_t from_size,
      char *to, const int64_t to_size, int64_t &out_size) = 0;

  // acquired connection must be released
  virtual int acquire(ObISQLConnection *&conn) = 0;
  virtual int release(ObISQLConnection *conn, const bool success) = 0;
};

} // end namespace sqlclient
} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_MYSQLCLIENT_OB_ISQL_CONNECTION_POOL_H_
