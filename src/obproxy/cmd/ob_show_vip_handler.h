/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_SHOW_VIP_HANDLER_H
#define OBPROXY_SHOW_VIP_HANDLER_H

#include "cmd/ob_internal_cmd_handler.h"
#include "obutils/ob_vip_tenant_processor.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
class ObShowVipHandler : public ObInternalCmdHandler
{
public:
  ObShowVipHandler(event::ObContinuation *cont, event::ObMIOBuffer *buf,
                   const ObInternalCmdInfo &info);
  virtual ~ObShowVipHandler() {}
  int main_handler(int event, void *data);

private:
  int dump_header();
  int dump_body();
  int dump_item(const ObVipTenant &vip_tenant);
  static int sqlite3_callback(void *data, int argc, char **argv, char **column_name);

  DISALLOW_COPY_AND_ASSIGN(ObShowVipHandler);
};

int show_vip_cmd_init();
} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_SHOW_VIP_HANDLER_H */
