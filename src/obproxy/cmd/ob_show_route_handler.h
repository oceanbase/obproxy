/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_SHOW_ROUTE_HANDLER_H
#define OBPROXY_SHOW_ROUTE_HANDLER_H

#include "cmd/ob_internal_cmd_handler.h"
#include "proxy/route/ob_table_cache.h"
#include "proxy/route/ob_partition_cache.h"
#include "proxy/route/ob_routine_cache.h"
#include "proxy/route/ob_index_cache.h"
#include "proxy/rpc/rpclib/ob_tablegroup_cache.h"
#include "proxy/rpc/rpclib/ob_table_query_async_cache.h"
#include "proxy/rpc/rpclib/ob_tablet_ls_cache.h"
#include "proxy/rpc/rpclib/ob_rpc_req_ctx_cache.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObShowRouteHandler : public ObInternalCmdHandler
{
public:
	ObShowRouteHandler(event::ObContinuation *cont, event::ObMIOBuffer *buf, const ObInternalCmdInfo &info);
  virtual ~ObShowRouteHandler() {}
  int handle_show_table(int event, void *data);
  int handle_show_partition(int event, void *data);
  int handle_show_routine(int event, void *data);
  int handle_show_global_index(int event, void *data);
  int handle_show_table_group(int event, void *data);
  int handle_show_query_async(int event, void *data);
  int handle_show_tablet_ls(int event, void *data);
  int handle_show_rpc_ctx(int event, void *data);

private:
  int dump_header();
  int dump_table_item(const ObTableEntry &entry);
  int dump_partition_item(const ObPartitionEntry &entry);
  int dump_routine_item(const ObRoutineEntry &entry);
  int dump_global_index_item(const ObIndexEntry &entry);
  int dump_table_group_item(const ObTableGroupEntry &entry);
  int dump_query_async_item(const ObTableQueryAsyncEntry &entry);
  int dump_tablet_ls_item(const ObTabletLsEntry &entry);
  int dump_rpc_ctx_item(const ObRpcReqCtx &entry);

  int fill_table_entry_name();
  int fill_routine_entry_name();

  const ObProxyBasicStmtSubType sub_type_;
  int64_t list_bucket_;
  int64_t tablet_ls_size_;
  ObTableEntryName entry_name_;

  char value_str_[common::OB_MAX_CONFIG_VALUE_LEN + 1];

  DISALLOW_COPY_AND_ASSIGN(ObShowRouteHandler);
};

int show_route_cmd_init();
} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_SHOW_ROUTE_HANDLER_H */
