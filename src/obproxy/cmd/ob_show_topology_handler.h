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

#ifndef OBPROXY_SHOW_TOPOLOGY_HANDLER_H
#define OBPROXY_SHOW_TOPOLOGY_HANDLER_H

#include "cmd/ob_cmd_handler.h"

namespace oceanbase
{
namespace obproxy
{
namespace dbconfig
{
class ObGroupCluster;
class ObShardRule;
}

namespace obutils
{
class ObShowTopologyHandler : public ObCmdHandler
{
public:
  ObShowTopologyHandler(event::ObMIOBuffer *buf, ObCmdInfo &info);
  virtual ~ObShowTopologyHandler() {}

  int handle_show_elastic_id(const common::ObString &tenant_name,
                             const common::ObString &db_name,
                             const common::ObString &group_name);
  static int show_elastic_id_cmd_callback(event::ObMIOBuffer *buf,
                                          ObCmdInfo &info,
                                          const common::ObString &logic_tenant_name,
                                          const common::ObString &logic_database_name,
                                          const common::ObString &group_name);
  int handle_show_topology(const common::ObString &tenant_name,
                           const common::ObString &db_name,
                           const common::ObString &table_name);
  static int show_topology_cmd_callback(event::ObMIOBuffer *buf,
                                        ObCmdInfo &info,
                                        const common::ObString &logic_tenant_name,
                                        const common::ObString &logic_database_name,
                                        const common::ObString &table_name);

public:
  static const int64_t OB_MAX_ELASTIC_ID_COUNT = 256;

private:
  int dump_elastic_id_header();
  int dump_elastic_id(const int64_t eid);

  int dump_shard_topology_for_single_db(ObString table_name);
  int dump_shard_topology_for_shard_db_single_tb(ObString table_name);
  int dump_shard_topology_for_shard_db_non_shard_tb(ObString table_name, dbconfig::ObShardRule *shard_rule);
  int dump_shard_topology_for_shard_db_shard_tb(dbconfig::ObShardRule *shard_rule);

  ObString get_shard_key_str(dbconfig::ObShardRule& shard_rule);
  ObString get_shard_rule_str(dbconfig::ObShardRule& shard_rule);
private:
  DISALLOW_COPY_AND_ASSIGN(ObShowTopologyHandler);
};

int show_topology_cmd_init();
} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_SHOW_TOPOLOGY_HANDLER_H */

