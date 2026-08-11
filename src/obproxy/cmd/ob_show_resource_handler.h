/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_SHOW_RESOURCE_HANDLER_H
#define OBPROXY_SHOW_RESOURCE_HANDLER_H

#include "cmd/ob_internal_cmd_handler.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
class ObClusterResource;
class ObShowResourceHandler : public ObInternalCmdHandler
{
public:
  ObShowResourceHandler(event::ObContinuation *cont, event::ObMIOBuffer *buf, const ObInternalCmdInfo &info);
  virtual ~ObShowResourceHandler() {}
  int handle_show_resource(int event, void *data);

private:
  int dump_resource_header();
  int dump_resource_item(const ObClusterResource *cr);

  DISALLOW_COPY_AND_ASSIGN(ObShowResourceHandler);
};

int show_resource_cmd_init();

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_SHOW_RESOURCE_HANDLER_H */

