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

#ifndef OB_PROXY_FRESH_SERVER_ADDR_CONT_H_
#define OB_PROXY_FRESH_SERVER_ADDR_CONT_H_
#include "iocore/eventsystem/ob_event_system.h"
#include "obutils/ob_async_common_task.h"
#include "lib/string/ob_string.h"
#include "lib/lock/tbrwlock.h"
#include "proxy/mysql/ob_mysql_global_session_utils.h"
#include "share/schema/ob_schema_struct.h"
#include "share/partition_table/ob_partition_info.h"
#include "common/ob_role.h"
#include "common/ob_zone_type.h"
#include "share/ob_server_status.h"
#include "share/ob_zone_info.h"
#include "obutils/ob_congestion_entry.h"
#include "proxy/route/ob_table_entry.h"
#include "common/ob_zone.h"
#include "share/ob_zone_status.h"

#define REFRESH_SERVER_START_CONT_EVENT (REFRESH_SERVER_ADDR_EVENT_START + 1)
#define REFRESH_SERVER_CREATE_CLUSTER_RESOURCE_EVENT (REFRESH_SERVER_ADDR_EVENT_START + 2)
#define REFRESH_SERVER_DONE_ENTRY_EVENT (REFRESH_SERVER_ADDR_EVENT_START + 3)

namespace oceanbase
{
namespace obproxy
{
class ObResultSetFetcher;
namespace proxy {
class ObProxySchemaKey;
}
namespace obutils {
class ObClusterResource;

class ObProxyRefreshServerAddrCont : public obutils::ObAsyncCommonTask
{
public:
  ObProxyRefreshServerAddrCont(event::ObProxyMutex *m, int64_t inerval_us = 5000);
  virtual ~ObProxyRefreshServerAddrCont();

  virtual int main_handler(int event, void *data);
  int schedule_refresh_server_cont(bool imm = false);
  int handle_all_schema_keys();
  int handle_get_one_schema_key();
  int handle_dummy_entry_resp(ObResultSetFetcher &rs_fetcher);
  int process_table_entry_lookup(void* data);
  int find_servers_from_dummy_entry(proxy::ObTableEntry *dummy_entry);
private:
  const char *get_event_name(int event);
  int fetch_dummy_table_entry();
  int handle_create_cluster_resource();
  int handle_create_cluster_resource_complete(void *data);

private:
  static const int64_t RETRY_INTERVAL_MS = 1000; // 1s
  int32_t reentrancy_count_;
  int32_t retry_count_; // if create fail, retry count, default is INT32_MAX;
  int32_t succ_count_;
  int32_t total_count_;
  int64_t refresh_interval_us_;
  proxy::ObProxySchemaKey* schema_key_;
  ObClusterResource* cr_;
  oceanbase::obproxy::obutils::ObProxyConfigString current_idc_name_;
  DISALLOW_COPY_AND_ASSIGN(ObProxyRefreshServerAddrCont);
};

int global_proxy_session_create_task();
} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
#endif //OB_PROXY_FRESH_SERVER_ADDR_CONT_H_
