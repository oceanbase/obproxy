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

#include "stat/ob_lock_stats.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace event
{

#define LOCK_REGISTER_RAW_STAT(rsb, rec_type, name, data_type, id, sync_type, persist_type) \
  if (OB_SUCC(ret)) { \
    ret = g_stat_processor.register_raw_stat(rsb, rec_type, name, data_type, id, sync_type, persist_type); \
  }

ObRecRawStatBlock *lock_rsb = NULL;

int init_lock_stats()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(lock_rsb = g_stat_processor.allocate_raw_stat_block(MAX_LOCK_COUNT,
      XFH_LOCK_STATE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_LOG(WARN, "fail to alloc mem for lock_rsb", K(ret));
  } else {
    LOCK_REGISTER_RAW_STAT(lock_rsb, RECT_PROCESS, "net_vc_lock_conflict_count",
                                     RECD_INT, NET_VC_LOCK, SYNC_SUM, RECP_NULL);

    LOCK_REGISTER_RAW_STAT(lock_rsb, RECT_PROCESS, "net_handler_lock_conflict_count",
                                     RECD_INT, NET_HANDLER_LOCK, SYNC_SUM, RECP_NULL);

    LOCK_REGISTER_RAW_STAT(lock_rsb, RECT_PROCESS, "ethread_lock_conflict_count",
                                     RECD_INT, ETHREAD_LOCK, SYNC_SUM, RECP_NULL);

    LOCK_REGISTER_RAW_STAT(lock_rsb, RECT_PROCESS, "congestion_entry_lock_conflict_count",
                                     RECD_INT, CONGESTION_ENTRY_LOCK, SYNC_SUM, RECP_NULL);

    LOCK_REGISTER_RAW_STAT(lock_rsb, RECT_PROCESS, "congestion_table_lock_conflict_count",
                                     RECD_INT, CONGESTION_TABLE_LOCK, SYNC_SUM, RECP_NULL);

    LOCK_REGISTER_RAW_STAT(lock_rsb, RECT_PROCESS, "mysql_proxy_lock_conflict_count",
                                     RECD_INT, MYSQL_PROXY_LOCK, SYNC_SUM, RECP_NULL);

    LOCK_REGISTER_RAW_STAT(lock_rsb, RECT_PROCESS, "client_vc_lock_conflict_count",
                                     RECD_INT, CLIENT_VC_LOCK, SYNC_SUM, RECP_NULL);

    LOCK_REGISTER_RAW_STAT(lock_rsb, RECT_PROCESS, "cache_cleaner_lock_conflict_count",
                                     RECD_INT, CACHE_CLEANER_LOCK, SYNC_SUM, RECP_NULL);

    LOCK_REGISTER_RAW_STAT(lock_rsb, RECT_PROCESS, "table_entry_map_lock_conflict_count",
                                     RECD_INT, TABLE_ENTRY_MAP_LOCK, SYNC_SUM, RECP_NULL);

    LOCK_REGISTER_RAW_STAT(lock_rsb, RECT_PROCESS, "common_lock_conflict_count",
                                     RECD_INT, COMMON_LOCK, SYNC_SUM, RECP_NULL);
  }

  return ret;
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
