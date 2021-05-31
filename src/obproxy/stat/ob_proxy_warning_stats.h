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

#ifndef OBPROXY_WARNING_STATS_H
#define OBPROXY_WARNING_STATS_H

#include "stat/ob_stat_processor.h"

namespace oceanbase
{
namespace obproxy
{

enum ObWarningStats
{
  CURRENT_CORE_COUNT,
  LAST_WARN_LOG_TIME_US,
  LAST_ERROR_LOG_TIME_US,
  TOTAL_WARN_LOG_COUNT,
  TOTAL_ERROR_LOG_COUNT,

  ALLOCED_TINY_LOG_ITEM_COUNT,
  ACTIVE_TINY_LOG_ITEM_COUNT,
  RELEASED_TINY_LOG_ITEM_COUNT,

  ALLOCED_NORMAL_LOG_ITEM_COUNT,
  ACTIVE_NORMAL_LOG_ITEM_COUNT,
  RELEASED_NORMAL_LOG_ITEM_COUNT,

  ALLOCED_LARGE_LOG_ITEM_COUNT,
  ACTIVE_LARGE_LOG_ITEM_COUNT,
  RELEASED_LARGE_LOG_ITEM_COUNT,

  DEFAULT_LOG_WRITE_SIZE,
  DEFAULT_LOG_WRITE_COUNT,
  DEFAULT_LARGE_LOG_WRITE_COUNT,

  XFLUSH_LOG_WRITE_SIZE,
  XFLUSH_LOG_WRITE_COUNT,
  XFLUSH_LARGE_LOG_WRITE_COUNT,

  DROPPED_ERROR_LOG_COUNT,
  DROPPED_WARN_LOG_COUNT,
  DROPPED_INFO_LOG_COUNT,
  DROPPED_TRACE_LOG_COUNT,
  DROPPED_DEBUG_LOG_COUNT,

  ASYNC_FLUSH_LOG_SPEED,
  MAX_WARNING_STATS_COUNT
};

extern ObRecRawStatBlock *warning_rsb;

#define PROXY_INCREMENT_DYN_STAT(x) \
    (void)ObStatProcessor::incr_raw_stat_sum_no_log(warning_rsb, x, 1)

#define PROXY_SET_GLOBAL_DYN_STAT(x, y) \
    (void)ObStatProcessor::set_global_raw_stat_sum_no_log(warning_rsb, x, y)

int init_warning_stats();

// attention !!! can't print log in this func
void logger_callback(const int32_t level, const int64_t time,
                     const char *head_buf, const int64_t head_len,
                     const char *data = NULL, const int64_t data_len = 0);

} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_WARNING_STATS_H
