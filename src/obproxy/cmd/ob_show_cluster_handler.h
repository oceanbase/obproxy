/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_SHOW_CLUSTER_HANDLER_H
#define OBPROXY_SHOW_CLUSTER_HANDLER_H

#include "cmd/ob_internal_cmd_handler.h"
#include "obutils/ob_config_server_processor.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
class ObProxyIDCInfo;
class ObShowClusterHandler : public ObInternalCmdHandler
{
public:
	ObShowClusterHandler(event::ObContinuation *cont, event::ObMIOBuffer *buf, const ObInternalCmdInfo &info);
  virtual ~ObShowClusterHandler() {}
  int handle_show_cluster(int event, void *data);

private:
  int dump_cluster_header();
  int dump_cluster_info(const ObProxyClusterInfo &cluster_info,
                        event::ObFixedArenaAllocator<ObLayout::MAX_PATH_LENGTH> &allocator);
  int dump_cluster_rslist_item(const ObProxyClusterInfo &cluster_info,
                               const proxy::ObProxyReplicaLocation &addr,
                               event::ObFixedArenaAllocator<ObLayout::MAX_PATH_LENGTH> &allocator);
  int dump_cluster_idc_list_item(const ObProxyClusterInfo &cluster_info,
                                 const ObProxyIDCInfo &idc_info,
                                 event::ObFixedArenaAllocator<ObLayout::MAX_PATH_LENGTH> &allocator);

private:
  ObProxyBasicStmtSubType sub_type_;
  DISALLOW_COPY_AND_ASSIGN(ObShowClusterHandler);
};

int show_cluster_cmd_init();
} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_SHOW_CLUSTER_HANDLER_H */
