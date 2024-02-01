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

#include "stat/ob_net_stats.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace net
{

#define NET_REGISTER_RAW_STAT(rsb, rec_type, name, data_type, id, sync_type, persist_type)  \
  if (OB_SUCC(ret)) { \
    ret = g_stat_processor.register_raw_stat(rsb, rec_type, name, data_type, id, sync_type, persist_type); \
  }

ObRecRawStatBlock *net_rsb = NULL;

int init_net_stats()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(net_rsb = g_stat_processor.allocate_raw_stat_block(NET_STAT_COUNT, XFH_NET_STATE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_NET_LOG(EDIAG, "g_stat_processor fail to allocate_raw_stat_block", K(ret));
  } else {
    NET_REGISTER_RAW_STAT(net_rsb, RECT_PROCESS, "net_handler_run",
                          RECD_INT, NET_HANDLER_RUN, SYNC_SUM, RECP_NULL);

    NET_REGISTER_RAW_STAT(net_rsb, RECT_PROCESS, "read_bytes",
                          RECD_INT, NET_READ_BYTES, SYNC_SUM, RECP_PERSISTENT);

    NET_REGISTER_RAW_STAT(net_rsb, RECT_PROCESS, "write_bytes",
                          RECD_INT, NET_WRITE_BYTES, SYNC_SUM, RECP_PERSISTENT);

    NET_REGISTER_RAW_STAT(net_rsb, RECT_PROCESS, "client_connections_currently_open",
                          RECD_INT, NET_CLIENT_CONNECTIONS_CURRENTLY_OPEN, SYNC_SUM, RECP_NULL);

    NET_REGISTER_RAW_STAT(net_rsb, RECT_PROCESS, "global_client_connections_currently_open",
                          RECD_INT, NET_GLOBAL_CLIENT_CONNECTIONS_CURRENTLY_OPEN, SYNC_SUM, RECP_NULL);

    NET_REGISTER_RAW_STAT(net_rsb, RECT_PROCESS, "global_connections_currently_open",
                          RECD_INT, NET_GLOBAL_CONNECTIONS_CURRENTLY_OPEN, SYNC_SUM, RECP_NULL);

    NET_REGISTER_RAW_STAT(net_rsb, RECT_PROCESS, "global_accepts_currently_open",
                          RECD_INT, NET_GLOBAL_ACCEPTS_CURRENTLY_OPEN, SYNC_SUM, RECP_NULL);

    NET_REGISTER_RAW_STAT(net_rsb, RECT_PROCESS, "calls_to_readfromnet",
                          RECD_INT, NET_CALLS_TO_READFROMNET, SYNC_SUM, RECP_NULL);

    NET_REGISTER_RAW_STAT(net_rsb, RECT_PROCESS, "calls_to_read",
                          RECD_INT, NET_CALLS_TO_READ, SYNC_SUM, RECP_NULL);

    NET_REGISTER_RAW_STAT(net_rsb, RECT_PROCESS, "calls_to_read_nodata",
                          RECD_INT, NET_CALLS_TO_READ_NODATA, SYNC_SUM, RECP_NULL);

    NET_REGISTER_RAW_STAT(net_rsb, RECT_PROCESS, "calls_to_writetonet",
                          RECD_INT, NET_CALLS_TO_WRITETONET, SYNC_SUM, RECP_NULL);

    NET_REGISTER_RAW_STAT(net_rsb, RECT_PROCESS, "calls_to_write",
                          RECD_INT, NET_CALLS_TO_WRITE, SYNC_SUM, RECP_NULL);

    NET_REGISTER_RAW_STAT(net_rsb, RECT_PROCESS, "calls_to_write_nodata",
                          RECD_INT, NET_CALLS_TO_WRITE_NODATA, SYNC_SUM, RECP_NULL);

    NET_REGISTER_RAW_STAT(net_rsb, RECT_PROCESS, "inactivity_cop_lock_acquire_failure",
                          RECD_INT, INACTIVITY_COP_LOCK_ACQUIRE_FAILURE, SYNC_SUM, RECP_NULL);

    NET_REGISTER_RAW_STAT(net_rsb, RECT_PROCESS, "keep_alive_lru_timeout_total",
                          RECD_INT, KEEP_ALIVE_LRU_TIMEOUT_TOTAL, SYNC_SUM, RECP_NULL);

    NET_REGISTER_RAW_STAT(net_rsb, RECT_PROCESS, "keep_alive_lru_timeout_count",
                          RECD_INT, KEEP_ALIVE_LRU_TIMEOUT_COUNT, SYNC_SUM, RECP_NULL);

    NET_REGISTER_RAW_STAT(net_rsb, RECT_PROCESS, "default_inactivity_timeout",
                          RECD_INT, DEFAULT_INACTIVITY_TIMEOUT, SYNC_SUM, RECP_NULL);
  }
  return ret;
}

} // end of namespace net
} // end of namespace obproxy
} // end of namespace oceanbase
