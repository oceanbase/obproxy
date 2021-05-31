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

#ifndef OB_PROXY_CONN_NUM_CHECK_CONT_H_
#define OB_PROXY_CONN_NUM_CHECK_CONT_H_
#include "iocore/eventsystem/ob_event_system.h"
#include "obutils/ob_async_common_task.h"
#include "lib/string/ob_string.h"
#include "lib/lock/tbrwlock.h"
#include "proxy/mysql/ob_mysql_global_session_utils.h"

#define CONN_NUM_CHECK_ENTRY_START_EVENT (CONN_NUM_CHECK_EVENT_EVENTS_START + 1)
#define CONN_NUM_CHECK_ENTRY_DESTROY_EVENT (CONN_NUM_CHECK_EVENT_EVENTS_START + 2)

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
class ObClusterResource;

class ObProxyConnNumCheckCont : public obutils::ObAsyncCommonTask
{
public:
  ObProxyConnNumCheckCont(event::ObProxyMutex *m, int64_t interval_us = 5000);
  ~ObProxyConnNumCheckCont();
  int init();
  virtual int main_handler(int event, void *data);
  int schedule_check_conn_num_cont(bool imm = false);
  int stop_check_conn_num();
  int handle_conn_num_check();
  int handle_one_schema_key_num_check();
  int handle_detroy_self();
  const char *get_event_name(const int64_t event);

public:
  int32_t reentrancy_count_;
  int32_t retry_count_;
  int64_t refresh_interval_us_;
  obproxy::proxy::ObProxySchemaKey schema_key_;
  DISALLOW_COPY_AND_ASSIGN(ObProxyConnNumCheckCont);
};
} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
#endif //OB_PROXY_CONN_NUM_CHECK_CONT_H_