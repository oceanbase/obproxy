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

#define USING_LOG_PREFIX PROXY
#include "stat/ob_proxy_warning_stats.h"
#include "cmd/ob_show_warning_handler.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::proxy;

namespace oceanbase
{
namespace obproxy
{

#define PROXY_REGISTER_RAW_STAT(rsb, rec_type, name, data_type, id, sync_type, persist_type) \
  if (OB_SUCC(ret)) { \
    ret = g_stat_processor.register_raw_stat(rsb, rec_type, name, data_type, id, sync_type, persist_type); \
  }

ObRecRawStatBlock *warning_rsb = NULL;

int init_warning_stats()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(warning_rsb = g_stat_processor.allocate_raw_stat_block(MAX_WARNING_STATS_COUNT, XFH_WARNING_STATE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc mem for warning_rsb", K(ret));
  } else {
    PROXY_REGISTER_RAW_STAT(warning_rsb, RECT_PROCESS, "current_core_count",
                                         RECD_INT, CURRENT_CORE_COUNT, SYNC_SUM, RECP_PERSISTENT);

    PROXY_REGISTER_RAW_STAT(warning_rsb, RECT_PROCESS, "last_warn_log_time_us",
                                         RECD_INT, LAST_WARN_LOG_TIME_US, SYNC_SUM, RECP_PERSISTENT);

    PROXY_REGISTER_RAW_STAT(warning_rsb, RECT_PROCESS, "last_error_log_time_us",
                                         RECD_INT, LAST_ERROR_LOG_TIME_US, SYNC_SUM, RECP_PERSISTENT);

    PROXY_REGISTER_RAW_STAT(warning_rsb, RECT_PROCESS, "total_warn_log_count",
                                         RECD_INT, TOTAL_WARN_LOG_COUNT, SYNC_SUM, RECP_PERSISTENT);

    PROXY_REGISTER_RAW_STAT(warning_rsb, RECT_PROCESS, "total_error_log_count",
                                         RECD_INT, TOTAL_ERROR_LOG_COUNT, SYNC_SUM, RECP_PERSISTENT);

    PROXY_REGISTER_RAW_STAT(warning_rsb, RECT_PROCESS, "alloced_tiny_log_item_count",
                                         RECD_INT, ALLOCED_TINY_LOG_ITEM_COUNT, SYNC_SUM, RECP_PERSISTENT);

    PROXY_REGISTER_RAW_STAT(warning_rsb, RECT_PROCESS, "active_tiny_log_item_count",
                                         RECD_INT, ACTIVE_TINY_LOG_ITEM_COUNT, SYNC_SUM, RECP_PERSISTENT);

    PROXY_REGISTER_RAW_STAT(warning_rsb, RECT_PROCESS, "released_tiny_log_item_count",
                                         RECD_INT, RELEASED_TINY_LOG_ITEM_COUNT, SYNC_SUM, RECP_PERSISTENT);

    PROXY_REGISTER_RAW_STAT(warning_rsb, RECT_PROCESS, "alloced_normal_log_item_count",
                                         RECD_INT, ALLOCED_NORMAL_LOG_ITEM_COUNT, SYNC_SUM, RECP_PERSISTENT);

    PROXY_REGISTER_RAW_STAT(warning_rsb, RECT_PROCESS, "active_normal_log_item_count",
                                         RECD_INT, ACTIVE_NORMAL_LOG_ITEM_COUNT, SYNC_SUM, RECP_PERSISTENT);

    PROXY_REGISTER_RAW_STAT(warning_rsb, RECT_PROCESS, "released_normal_log_item_count",
                                         RECD_INT, RELEASED_NORMAL_LOG_ITEM_COUNT, SYNC_SUM, RECP_PERSISTENT);

    PROXY_REGISTER_RAW_STAT(warning_rsb, RECT_PROCESS, "alloced_large_log_item_count",
                                         RECD_INT, ALLOCED_LARGE_LOG_ITEM_COUNT, SYNC_SUM, RECP_PERSISTENT);

    PROXY_REGISTER_RAW_STAT(warning_rsb, RECT_PROCESS, "active_large_log_item_count",
                                         RECD_INT, ACTIVE_LARGE_LOG_ITEM_COUNT, SYNC_SUM, RECP_PERSISTENT);

    PROXY_REGISTER_RAW_STAT(warning_rsb, RECT_PROCESS, "released_large_log_item_count",
                                         RECD_INT, RELEASED_LARGE_LOG_ITEM_COUNT, SYNC_SUM, RECP_PERSISTENT);

    PROXY_REGISTER_RAW_STAT(warning_rsb, RECT_PROCESS, "default_log_write_size",
                                         RECD_INT, DEFAULT_LOG_WRITE_SIZE, SYNC_SUM, RECP_PERSISTENT);

    PROXY_REGISTER_RAW_STAT(warning_rsb, RECT_PROCESS, "default_log_write_count",
                                         RECD_INT, DEFAULT_LOG_WRITE_COUNT, SYNC_SUM, RECP_PERSISTENT);

    PROXY_REGISTER_RAW_STAT(warning_rsb, RECT_PROCESS, "default_large_log_write_count",
                                         RECD_INT, DEFAULT_LARGE_LOG_WRITE_COUNT, SYNC_SUM, RECP_PERSISTENT);

    PROXY_REGISTER_RAW_STAT(warning_rsb, RECT_PROCESS, "xflush_log_write_size",
                                         RECD_INT, XFLUSH_LOG_WRITE_SIZE, SYNC_SUM, RECP_PERSISTENT);

    PROXY_REGISTER_RAW_STAT(warning_rsb, RECT_PROCESS, "xflush_log_write_count",
                                         RECD_INT, XFLUSH_LOG_WRITE_COUNT, SYNC_SUM, RECP_PERSISTENT);

    PROXY_REGISTER_RAW_STAT(warning_rsb, RECT_PROCESS, "xflush_large_log_write_count",
                                         RECD_INT, XFLUSH_LARGE_LOG_WRITE_COUNT, SYNC_SUM, RECP_PERSISTENT);

    PROXY_REGISTER_RAW_STAT(warning_rsb, RECT_PROCESS, "dropped_error_log_count",
                                         RECD_INT, DROPPED_ERROR_LOG_COUNT, SYNC_SUM, RECP_PERSISTENT);

    PROXY_REGISTER_RAW_STAT(warning_rsb, RECT_PROCESS, "dropped_warn_log_count",
                                         RECD_INT, DROPPED_WARN_LOG_COUNT, SYNC_SUM, RECP_PERSISTENT);

    PROXY_REGISTER_RAW_STAT(warning_rsb, RECT_PROCESS, "dropped_info_log_count",
                                         RECD_INT, DROPPED_INFO_LOG_COUNT, SYNC_SUM, RECP_PERSISTENT);

    PROXY_REGISTER_RAW_STAT(warning_rsb, RECT_PROCESS, "dropped_trace_log_count",
                                         RECD_INT, DROPPED_TRACE_LOG_COUNT, SYNC_SUM, RECP_NULL);

    PROXY_REGISTER_RAW_STAT(warning_rsb, RECT_PROCESS, "dropped_debug_log_count",
                                         RECD_INT, DROPPED_DEBUG_LOG_COUNT, SYNC_SUM, RECP_NULL);

    PROXY_REGISTER_RAW_STAT(warning_rsb, RECT_PROCESS, "async_flush_log_speed",
                                         RECD_INT, ASYNC_FLUSH_LOG_SPEED, SYNC_SUM, RECP_NULL);
  }
  return ret;
}

void logger_callback(const int32_t level, const int64_t time,
                     const char *head_buf, const int64_t head_len,
                     const char *data, const int64_t data_len)
{
  ObWarningProcessor &g_warning_processor = get_global_warning_processor();
  g_warning_processor.record_log(level, time, head_buf, head_len, data, data_len);

  switch (level) {
    case OB_LOG_LEVEL_WARN:
      PROXY_INCREMENT_DYN_STAT(TOTAL_WARN_LOG_COUNT);
      PROXY_SET_GLOBAL_DYN_STAT(LAST_WARN_LOG_TIME_US, time);
      break;

    case OB_LOG_LEVEL_ERROR:
      PROXY_INCREMENT_DYN_STAT(TOTAL_ERROR_LOG_COUNT);
      PROXY_SET_GLOBAL_DYN_STAT(LAST_ERROR_LOG_TIME_US, time);
      break;

    default:
      // do nothting
      break;
  }
}

} // end of namespace obproxy
} // end of namespace oceanbase
