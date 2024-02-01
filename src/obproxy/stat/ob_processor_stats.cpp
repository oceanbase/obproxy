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

#include "stat/ob_processor_stats.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
#define PROCESSOR_REGISTER_RAW_STAT(rsb, rec_type, name, data_type, id, sync_type, persist_type)  \
  if (OB_SUCC(ret)) { \
    ret = g_stat_processor.register_raw_stat(rsb, rec_type, name, data_type, id, sync_type, persist_type); \
  }

ObRecRawStatBlock *processor_rsb = NULL;

int init_processor_stats()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(processor_rsb = g_stat_processor.allocate_raw_stat_block(PROCESSOR_STAT_COUNT, XFH_PROCESSOR_STATE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_LOG(EDIAG, "fail to alloc memory for processor rsb", K(ret));
  } else {
    // pl related
    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_pl_total",
                      RECD_INT, GET_PL_TOTAL, SYNC_SUM, RECP_NULL);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_pl_from_thread_cache_hit",
                      RECD_INT, GET_PL_FROM_THREAD_CACHE_HIT, SYNC_SUM, RECP_PERSISTENT);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_pl_from_global_cache_hit",
                      RECD_INT, GET_PL_FROM_GLOBAL_CACHE_HIT, SYNC_SUM, RECP_PERSISTENT);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_pl_from_global_cache_dirty_stat",
                      RECD_INT, GET_PL_FROM_GLOBAL_CACHE_DIRTY, SYNC_SUM, RECP_PERSISTENT);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_pl_by_all_dummy",
                      RECD_INT, GET_PL_BY_ALL_DUMMY, SYNC_SUM,  RECP_NULL);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_pl_from_remote",
                      RECD_INT, GET_PL_FROM_REMOTE, SYNC_SUM, RECP_PERSISTENT);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_pl_from_remote_succ",
                      RECD_INT, GET_PL_FROM_REMOTE_SUCC, SYNC_SUM, RECP_NULL);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_pl_from_remote_fail",
                      RECD_INT, GET_PL_FROM_REMOTE_FAIL, SYNC_SUM, RECP_NULL);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_pl_by_last_session_succ",
                      RECD_INT, GET_PL_BY_LAST_SESSION_SUCC, SYNC_SUM, RECP_NULL);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_pl_by_rs_list_succ",
                      RECD_INT, GET_PL_BY_RS_LIST_SUCC, SYNC_SUM, RECP_NULL);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "pl_delay_update_count",
                      RECD_INT, PL_DELAY_UPDATE_COUNT, SYNC_SUM, RECP_NULL);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "set_delay_update_count",
                      RECD_INT, SET_DELAY_UPDATE_COUNT, SYNC_SUM, RECP_NULL);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "gc_table_entry_from_global_cache",
                      RECD_INT, GC_TABLE_ENTRY_FROM_GLOBAL_CACHE, SYNC_SUM, RECP_NULL);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "gc_table_entry_from_thread_cache",
                      RECD_INT, GC_TABLE_ENTRY_FROM_THREAD_CACHE, SYNC_SUM, RECP_NULL);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "kick_out_table_entry_from_global_cache",
                      RECD_INT, KICK_OUT_TABLE_ENTRY_FROM_GLOBAL_CACHE, SYNC_SUM, RECP_NULL);

    // partition info related
    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_part_info_from_remote",
                      RECD_INT, GET_PART_INFO_FROM_REMOTE, SYNC_SUM, RECP_PERSISTENT);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_part_info_from_remote_succ",
                      RECD_INT, GET_PART_INFO_FROM_REMOTE_SUCC, SYNC_SUM, RECP_PERSISTENT);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_part_info_from_remote_fail",
                      RECD_INT, GET_PART_INFO_FROM_REMOTE_FAIL, SYNC_SUM, RECP_PERSISTENT);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_first_part_from_remote",
                      RECD_INT, GET_FIRST_PART_FROM_REMOTE, SYNC_SUM, RECP_PERSISTENT);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_first_part_from_remote_succ",
                      RECD_INT, GET_FIRST_PART_FROM_REMOTE_SUCC, SYNC_SUM, RECP_PERSISTENT);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_first_part_from_remote_fail",
                      RECD_INT, GET_FIRST_PART_FROM_REMOTE_FAIL, SYNC_SUM, RECP_PERSISTENT);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_sub_part_from_remote",
                      RECD_INT, GET_SUB_PART_FROM_REMOTE, SYNC_SUM, RECP_PERSISTENT);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_sub_part_from_remote_succ",
                      RECD_INT, GET_SUB_PART_FROM_REMOTE_SUCC, SYNC_SUM, RECP_PERSISTENT);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_sub_part_from_remote_fail",
                      RECD_INT, GET_SUB_PART_FROM_REMOTE_FAIL, SYNC_SUM, RECP_PERSISTENT);

    // partition entry related
    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_partition_entry_from_thread_cache_hit",
                      RECD_INT, GET_PARTITION_ENTRY_FROM_THREAD_CACHE_HIT, SYNC_SUM, RECP_PERSISTENT);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_partition_entry_from_global_cache_hit",
                      RECD_INT, GET_PARTITION_ENTRY_FROM_GLOBAL_CACHE_HIT, SYNC_SUM, RECP_PERSISTENT);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_partition_entry_from_global_cache_dirty",
                      RECD_INT, GET_PARTITION_ENTRY_FROM_GLOBAL_CACHE_DIRTY, SYNC_SUM, RECP_PERSISTENT);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_partition_entry_from_remote",
                      RECD_INT, GET_PARTITION_ENTRY_FROM_REMOTE, SYNC_SUM, RECP_PERSISTENT);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_partition_entry_from_remote_succ",
                      RECD_INT, GET_PARTITION_ENTRY_FROM_REMOTE_SUCC, SYNC_SUM, RECP_PERSISTENT);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_partition_entry_from_remote_fail",
                      RECD_INT, GET_PARTITION_ENTRY_FROM_REMOTE_FAIL, SYNC_SUM, RECP_PERSISTENT);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "gc_partition_entry_from_global_cache",
                      RECD_INT, GC_PARTITION_ENTRY_FROM_GLOBAL_CACHE, SYNC_SUM, RECP_PERSISTENT);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "gc_partition_entry_from_thread_cache",
                      RECD_INT, GC_PARTITION_ENTRY_FROM_THREAD_CACHE, SYNC_SUM, RECP_PERSISTENT);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "kick_out_partition_entry_from_global_cache",
                      RECD_INT, KICK_OUT_PARTITION_ENTRY_FROM_GLOBAL_CACHE, SYNC_SUM, RECP_PERSISTENT);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "update_route_entry_by_congestion",
                      RECD_INT, UPDATE_ROUTE_ENTRY_BY_CONGESTION, SYNC_SUM, RECP_PERSISTENT);

    // routine entry related
    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_routine_entry_from_thread_cache_hit",
                      RECD_INT, GET_ROUTINE_ENTRY_FROM_THREAD_CACHE_HIT, SYNC_SUM, RECP_PERSISTENT);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_routine_entry_from_global_cache_hit",
                      RECD_INT, GET_ROUTINE_ENTRY_FROM_GLOBAL_CACHE_HIT, SYNC_SUM, RECP_PERSISTENT);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_routine_entry_from_global_cache_dirty",
                      RECD_INT, GET_ROUTINE_ENTRY_FROM_GLOBAL_CACHE_DIRTY, SYNC_SUM, RECP_PERSISTENT);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_routine_entry_from_remote",
                      RECD_INT, GET_ROUTINE_ENTRY_FROM_REMOTE, SYNC_SUM, RECP_PERSISTENT);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_routine_entry_from_remote_succ",
                      RECD_INT, GET_ROUTINE_ENTRY_FROM_REMOTE_SUCC, SYNC_SUM, RECP_PERSISTENT);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_routine_entry_from_remote_fail",
                      RECD_INT, GET_ROUTINE_ENTRY_FROM_REMOTE_FAIL, SYNC_SUM, RECP_PERSISTENT);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "gc_routine_entry_from_global_cache",
                      RECD_INT, GC_ROUTINE_ENTRY_FROM_GLOBAL_CACHE, SYNC_SUM, RECP_PERSISTENT);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "gc_routine_entry_from_thread_cache",
                      RECD_INT, GC_ROUTINE_ENTRY_FROM_THREAD_CACHE, SYNC_SUM, RECP_PERSISTENT);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "kick_out_routine_entry_from_global_cache",
                      RECD_INT, KICK_OUT_ROUTINE_ENTRY_FROM_GLOBAL_CACHE, SYNC_SUM, RECP_PERSISTENT);

    // congestion related
    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_congestion_total",
                      RECD_INT, GET_CONGESTION_TOTAL, SYNC_SUM, RECP_NULL);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_congestion_from_thread_cache_hit",
                      RECD_INT, GET_CONGESTION_FROM_THREAD_CACHE_HIT, SYNC_SUM, RECP_PERSISTENT);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_congestion_from_global_cache_hit",
                      RECD_INT, GET_CONGESTION_FROM_GLOBAL_CACHE_HIT, SYNC_SUM, RECP_PERSISTENT);

    PROCESSOR_REGISTER_RAW_STAT(processor_rsb, RECT_PROCESS, "get_congestion_from_global_cache_miss",
                      RECD_INT, GET_CONGESTION_FROM_GLOBAL_CACHE_MISS, SYNC_SUM, RECP_PERSISTENT);
  }

  return ret;
}

} // end of namespace obproxy
} // end of namespace oceanbase
