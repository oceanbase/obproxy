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

#ifndef OCEANBASE_MYSQLCLIENT_OB_ISQL_CONNECTION_H_
#define OCEANBASE_MYSQLCLIENT_OB_ISQL_CONNECTION_H_

#include <stdint.h>

namespace oceanbase
{
namespace common
{
class ObIAllocator;

namespace sqlclient
{

class ObISQLResultHandler;

// SQL client connection interface
class ObISQLConnection
{
public:
  ObISQLConnection() {}
  virtual ~ObISQLConnection() {}

  // sql execute interface
  // %res is allocated from %alloc,
  // caller's responsibility to call ~ObISQLResultHandler() and free %res
  virtual int execute_read(ObIAllocator &alloc, const uint64_t tenant_id, const char *sql,
      ObISQLResultHandler *&res) = 0;
  virtual int execute_write(const uint64_t tenant_id, const char *sql,
      int64_t &affected_rows) = 0;

  // transaction interface
  virtual int start_transaction(bool with_snap_shot = false) = 0;
  virtual int rollback() = 0;
  virtual int commit() = 0;
};

} // end namespace sqlclient
} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_MYSQLCLIENT_OB_ISQL_CONNECTION_H_
