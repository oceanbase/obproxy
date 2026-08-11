/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_SHOW_CONGESTION_HANDLER_H
#define OBPROXY_SHOW_CONGESTION_HANDLER_H

#include "cmd/ob_internal_cmd_handler.h"
#include "obutils/ob_congestion_manager.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
class ObClusterResource;
class ObShowCongestionHandler : public ObInternalCmdHandler
{
public:
  ObShowCongestionHandler(event::ObContinuation *cont, event::ObMIOBuffer *buf, const ObInternalCmdInfo &info);
  virtual ~ObShowCongestionHandler() { destroy(); }

private:
  int handle_congestion(int event, void *data);
  int fill_cluster_resource_array();
  int dump_header();
  int dump_item(const ObCongestionEntry *entry, const ObString cluster_name);
  void destroy();

private:
  const ObProxyBasicStmtSubType sub_type_;
  int64_t idx_;
  int64_t list_bucket_;
  char cluster_str_[OB_PROXY_MAX_CLUSTER_NAME_LENGTH + 1];
  ObSEArray<ObClusterResource *, OB_PROXY_CLUSTER_RESOURCE_ITEM_COUNT> cr_array_;

  DISALLOW_COPY_AND_ASSIGN(ObShowCongestionHandler);
};

int show_congestion_cmd_init();

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_SHOW_CONGESTION_HANDLER_H */
