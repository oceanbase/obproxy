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

#ifndef OBPROXY_SHOW_ROUTE_HANDLER_H
#define OBPROXY_SHOW_ROUTE_HANDLER_H

#include "cmd/ob_internal_cmd_handler.h"
#include "proxy/route/ob_table_cache.h"
#include "proxy/route/ob_partition_cache.h"
#include "proxy/route/ob_routine_cache.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObShowRouteHandler : public ObInternalCmdHandler
{
public:
	ObShowRouteHandler(event::ObContinuation *cont, event::ObMIOBuffer *buf,
	                   const ObInternalCmdInfo &info);
  virtual ~ObShowRouteHandler() {}
  int handle_show_table(int event, void *data);
  int handle_show_partition(int event, void *data);
  int handle_show_routine(int event, void *data);

private:
  int dump_header();
  int dump_table_item(const ObTableEntry &entry);
  int dump_partition_item(const ObPartitionEntry &entry);
  int dump_routine_item(const ObRoutineEntry &entry);

  int fill_table_entry_name();
  int fill_routine_entry_name();

  const ObProxyBasicStmtSubType sub_type_;
  int64_t list_bucket_;
  ObTableEntryName entry_name_;

  char value_str_[common::OB_MAX_CONFIG_VALUE_LEN + 1];

  DISALLOW_COPY_AND_ASSIGN(ObShowRouteHandler);
};

int show_route_cmd_init();
} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_SHOW_ROUTE_HANDLER_H */
