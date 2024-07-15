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

#ifndef OBPROXY_MYSQL_VCTABLE_H
#define OBPROXY_MYSQL_VCTABLE_H

#include "iocore/eventsystem/ob_vconnection.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObMysqlSM;
typedef int (ObMysqlSM::*MysqlSMHandler)(int event, void *data);

enum ObMysqlVCType
{
  MYSQL_UNKNOWN = 0,
  MYSQL_CLIENT_VC,
  MYSQL_SERVER_VC,
  MYSQL_TRANSFORM_VC,
  MYSQL_RPC_CLIENT_VC,
  MYSQL_RPC_SERVER_VC
};

struct ObMysqlVCTableEntry
{
  bool eos_;
  bool in_tunnel_;
  ObMysqlVCType vc_type_;
  MysqlSMHandler vc_handler_;
  event::ObVConnection *vc_;
  event::ObMIOBuffer *read_buffer_;
  event::ObMIOBuffer *write_buffer_;
  event::ObVIO *read_vio_;
  event::ObVIO *write_vio_;
};

struct ObMysqlVCTable
{
  ObMysqlVCTable();
  ~ObMysqlVCTable() { }

  ObMysqlVCTableEntry *new_entry();
  ObMysqlVCTableEntry *find_entry(event::ObVConnection* vc);
  ObMysqlVCTableEntry *find_entry(event::ObVIO* vio);
  int remove_entry(ObMysqlVCTableEntry* e);
  int cleanup_entry(ObMysqlVCTableEntry* e, bool need_close = true);
  int cleanup_all();
  bool is_table_clear() const;

  static const int VC_TABLE_MAX_ENTRIES = 4;
private:
  ObMysqlVCTableEntry vc_table_[VC_TABLE_MAX_ENTRIES];
  DISALLOW_COPY_AND_ASSIGN(ObMysqlVCTable);
};

inline bool ObMysqlVCTable::is_table_clear() const
{
  bool bret = true;
  for (int64_t i = 0; bret && i < VC_TABLE_MAX_ENTRIES; ++i) {
    if (NULL != vc_table_[i].vc_) {
      bret = false;
    }
  }
  return bret;
}

inline const char *get_mysql_vc_type(const ObMysqlVCType type)
{
  const char *name = NULL;
  switch (type) {
    case MYSQL_UNKNOWN:
      name = "MYSQL_UNKNOWN";
      break;
    case MYSQL_CLIENT_VC:
      name = "MYSQL_CLIENT_VC";
      break;
    case MYSQL_SERVER_VC:
      name = "MYSQL_SERVER_VC";
      break;
    case MYSQL_TRANSFORM_VC:
      name = "MYSQL_TRANSFORM_VC";
      break;
    default:
      name = "UNKNOWN_VC";
      break;
  }
  return name;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_MYSQL_VCTABLE_H
