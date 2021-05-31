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

  DISALLOW_COPY_AND_ASSIGN(ObShowVipHandler);
};

int show_vip_cmd_init();
} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_SHOW_VIP_HANDLER_H */
