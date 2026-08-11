/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_DELETE_CLUSTER_HANDLER_H
#define OBPROXY_DELETE_CLUSTER_HANDLER_H

#include "cmd/ob_internal_cmd_handler.h"
#include "rpc/obmysql/ob_mysql_packet.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
class ObAlterResourceHandler : public ObInternalCmdHandler
{
public:
  ObAlterResourceHandler(event::ObContinuation *cont, event::ObMIOBuffer *buf, const ObInternalCmdInfo &info);
  virtual ~ObAlterResourceHandler() {}

  int handle_delete_cluster(int event, void *data);

private:
  char cluster_str_[OB_PROXY_MAX_CLUSTER_NAME_LENGTH + 1];
  const obmysql::ObMySQLCapabilityFlags capability_;

  DISALLOW_COPY_AND_ASSIGN(ObAlterResourceHandler);
};

int alter_resource_delete_cmd_init();
} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_DELETE_CLUSTER_HANDLER_H */
