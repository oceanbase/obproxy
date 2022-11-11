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
