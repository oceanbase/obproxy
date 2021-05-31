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

#ifndef OB_PROXY_CREATE_SERVER_CONN_CONT_H_
#define OB_PROXY_CREATE_SERVER_CONN_CONT_H_
#include "iocore/eventsystem/ob_event_system.h"
#include "obutils/ob_async_common_task.h"
#include "lib/string/ob_string.h"
#include "lib/lock/tbrwlock.h"
#include "lib/net/ob_addr.h"
#include "proxy/mysql/ob_mysql_global_session_utils.h"

#define CONN_ENTRY_GET_ONE_CONN_INFO_EVENT (CONN_ENTRY_EVENT_EVENTS_START + 1)
#define CONN_ENTRY_CREATE_CLUSTER_RESOURCE_EVENT (CONN_ENTRY_EVENT_EVENTS_START + 2)
#define CONN_ENTRY_CREATE_SERVER_SESSION_EVENT (CONN_ENTRY_EVENT_EVENTS_START + 3)
#define CONN_ENTRY_CREATE_DONE_EVENT (CONN_ENTRY_EVENT_EVENTS_START + 4)
namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObMysqlProxy;
class SchemaKeyConnInfo;
}
namespace obutils {
class ObClusterResource;

class ObProxyCreateServerConnCont : public obutils::ObAsyncCommonTask
{
public:
  ObProxyCreateServerConnCont(event::ObProxyMutex *m, int64_t interval_us = 5000);
  ~ObProxyCreateServerConnCont();
public:
  virtual int main_handler(int event, void *data);
  int schedule_create_conn_cont(bool imm = false);
  int handle_get_one_conn_info();
  int handle_create_cluster_resource();
  int handle_create_cluster_resource_complete(void *data);
  int do_create_server_conn();
  int handle_client_resp(void *data);
  int handle_select_value_resp(ObResultSetFetcher &rs_fetcher);
  int handle_create_session();
  const char *get_event_name(const int64_t event);

public:
  static const int64_t RETRY_INTERVAL_MS = 1000; // 1s
  int32_t reentrancy_count_;
  int32_t retry_count_;
  int64_t conn_count_;
  int64_t create_count_;
  int64_t refresh_interval_us_;
  proxy::SchemaKeyConnInfo* schema_key_conn_info_;
  oceanbase::obproxy::obutils::ObClusterResource* cr_;
  oceanbase::obproxy::proxy::ObMysqlProxy* proxy_;
  DISALLOW_COPY_AND_ASSIGN(ObProxyCreateServerConnCont);
};
} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
#endif //OB_PROXY_CREATE_SERVER_CONN_CONT_H_