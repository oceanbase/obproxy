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

#include "proxy/rpc_optimize/rpclib/ob_rpc_time_stat.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
#define TO_STRING_TIME_US(name) \
  databuff_printf(buf, buf_len, pos, #name"us=%ld, ", hrtime_to_usec(name));

#define TO_STRING_TIME_US_END(name) \
  databuff_printf(buf, buf_len, pos, #name"us=%ld", hrtime_to_usec(name));

int64_t ObRpcReqCmdTimeStat::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  TO_STRING_TIME_US(client_request_read_time_);
  TO_STRING_TIME_US(client_request_analyze_time_);
  TO_STRING_TIME_US(rpc_ctx_lookup_time_);
  TO_STRING_TIME_US(cluster_resource_create_time_);
  TO_STRING_TIME_US(query_async_lookup_time_);
  TO_STRING_TIME_US(index_entry_lookup_time_);
  TO_STRING_TIME_US(tablegroup_entry_lookup_time_);
  TO_STRING_TIME_US(pl_lookup_time_);
  TO_STRING_TIME_US(pl_process_time_);
  TO_STRING_TIME_US(congestion_control_time_);
  TO_STRING_TIME_US(congestion_process_time_);

  TO_STRING_TIME_US(build_server_request_time_);
  TO_STRING_TIME_US(prepare_send_request_to_server_time_);
  TO_STRING_TIME_US(server_request_write_time_);
  TO_STRING_TIME_US(server_process_request_time_);
  TO_STRING_TIME_US(server_response_read_time_);
  TO_STRING_TIME_US(server_response_analyze_time_);
  TO_STRING_TIME_US(client_response_write_time_);
  TO_STRING_TIME_US_END(request_total_time_);
  J_OBJ_END();
  return pos;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
