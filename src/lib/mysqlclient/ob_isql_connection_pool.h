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
