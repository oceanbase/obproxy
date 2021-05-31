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

#ifndef OB_SESSION_POOL_PROCESSOR_H_
#define OB_SESSION_POOL_PROCESSOR_H_
#include "lib/utility/ob_macro_utils.h"
#include "share/config/ob_common_config.h"
#include "iocore/eventsystem/ob_event.h"

namespace oceanbase
{
namespace obproxy
{
namespace dbconfig {
class ObDbConfigLogicTenant;
}
namespace obutils
{
extern event::ObEventThreadType ET_SPOOL;
class ObSessionPoolProcessor
{
public:
  ObSessionPoolProcessor();
  ~ObSessionPoolProcessor();
  int create_refresh_server_session_cont();
  int create_server_conn_cont();
  int create_conn_num_check_cont();
  int handle_one_logic_tenent(dbconfig::ObDbConfigLogicTenant* logic_tenant,
                                    int32_t& logic_tenant_count,
                                    int32_t& logic_db_count,
                                    int32_t& shard_conn_count);
  int start_session_pool_task();
  int start_pool_stat_dump_task();
  static int do_pool_stat_dump();
  static void update_pool_stat_dump_interval();
  ObAsyncCommonTask* get_pool_stat_dump_cont() {return pool_stat_dump_cont_;}
private:
  ObAsyncCommonTask * pool_stat_dump_cont_;
  DISALLOW_COPY_AND_ASSIGN(ObSessionPoolProcessor);
};

extern ObSessionPoolProcessor g_session_pool_processor;
inline ObSessionPoolProcessor &get_global_session_pool_processor()
{
  return g_session_pool_processor;
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
#endif //OB_SESSION_POOL_PROCESSOR_H_