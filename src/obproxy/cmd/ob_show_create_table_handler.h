// Copyright 2014-2016 Alibaba Inc. All Rights Reserved.
// Author:
//    luoxiaohu.lxh <luoxiaohu.lxh@oceanbase.com>
// Normalizer:
//    luoxiaohu.lxh <luoxiaohu.lxh@oceanbase.com>
//

#ifndef OBPROXY_SHOW_CREATE_TABLE_HANDLER_H
#define OBPROXY_SHOW_CREATE_TABLE_HANDLER_H

#include "cmd/ob_cmd_handler.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
class ObShowCreateTableHandler : public ObCmdHandler
{
public:
  ObShowCreateTableHandler(event::ObMIOBuffer *buf, ObCmdInfo &info);
  virtual ~ObShowCreateTableHandler() {}
  int handle_show_create_table(const ObString &logic_tenant_name, const ObString &logic_database_name,
                               const ObString &logic_table_name);
  static int show_create_table_cmd_callback(event::ObMIOBuffer *buf, ObCmdInfo &info,
                                            const ObString &logic_tenant_name, const ObString &logic_database_name,
                                            const ObString &logic_table_name);

  DISALLOW_COPY_AND_ASSIGN(ObShowCreateTableHandler);
};
} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_SHOW_CREATE_TABLE_HANDLER_H */
