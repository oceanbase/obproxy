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

#ifndef OBPROXY_PROCESSOR_STATS_H
#define OBPROXY_PROCESSOR_STATS_H

#include "stat/ob_stat_processor.h"
#include "iocore/eventsystem/ob_event_system.h"

namespace oceanbase
{
namespace obproxy
{
// Instead of enumerating the stats in a common file, each module
// needs to enumerate its stats separately and register them
enum ObProcessorStats
{
  // pl related
  GET_PL_TOTAL,
  GET_PL_FROM_THREAD_CACHE_HIT,
  // the statistics below are used before request
  GET_PL_FROM_GLOBAL_CACHE_HIT,
  GET_PL_FROM_GLOBAL_CACHE_DIRTY,
  GET_PL_BY_ALL_DUMMY, // get pl by __all_dummy
  GET_PL_FROM_REMOTE, // from remote server
  GET_PL_FROM_REMOTE_SUCC,
  GET_PL_FROM_REMOTE_FAIL,
  GET_PL_BY_LAST_SESSION_SUCC,
  GET_PL_BY_SESSION_POOL_SUCC,
  GET_PL_BY_RS_LIST_SUCC,
  PL_DELAY_UPDATE_COUNT,
  SET_DELAY_UPDATE_COUNT,
  GC_TABLE_ENTRY_FROM_GLOBAL_CACHE,
  GC_TABLE_ENTRY_FROM_THREAD_CACHE,
  KICK_OUT_TABLE_ENTRY_FROM_GLOBAL_CACHE, // when table cache is full

  // partition info related
  GET_PART_INFO_FROM_REMOTE,
  GET_PART_INFO_FROM_REMOTE_SUCC,
  GET_PART_INFO_FROM_REMOTE_FAIL,
  GET_FIRST_PART_FROM_REMOTE,
  GET_FIRST_PART_FROM_REMOTE_SUCC,
  GET_FIRST_PART_FROM_REMOTE_FAIL,
  GET_SUB_PART_FROM_REMOTE,
  GET_SUB_PART_FROM_REMOTE_SUCC,
  GET_SUB_PART_FROM_REMOTE_FAIL,

  // partition entry related
  GET_PARTITION_ENTRY_FROM_THREAD_CACHE_HIT,
  GET_PARTITION_ENTRY_FROM_GLOBAL_CACHE_HIT,
  GET_PARTITION_ENTRY_FROM_GLOBAL_CACHE_DIRTY,
  GET_PARTITION_ENTRY_FROM_REMOTE,
  GET_PARTITION_ENTRY_FROM_REMOTE_SUCC,
  GET_PARTITION_ENTRY_FROM_REMOTE_FAIL,
  GC_PARTITION_ENTRY_FROM_GLOBAL_CACHE,
  GC_PARTITION_ENTRY_FROM_THREAD_CACHE,
  KICK_OUT_PARTITION_ENTRY_FROM_GLOBAL_CACHE, // when partition cache is full

  UPDATE_ROUTE_ENTRY_BY_CONGESTION,

  // routine entry related
  GET_ROUTINE_ENTRY_FROM_THREAD_CACHE_HIT,
  GET_ROUTINE_ENTRY_FROM_GLOBAL_CACHE_HIT,
  GET_ROUTINE_ENTRY_FROM_GLOBAL_CACHE_DIRTY,
  GET_ROUTINE_ENTRY_FROM_REMOTE,
  GET_ROUTINE_ENTRY_FROM_REMOTE_SUCC,
  GET_ROUTINE_ENTRY_FROM_REMOTE_FAIL,
  GC_ROUTINE_ENTRY_FROM_GLOBAL_CACHE,
  GC_ROUTINE_ENTRY_FROM_THREAD_CACHE,
  KICK_OUT_ROUTINE_ENTRY_FROM_GLOBAL_CACHE, // when routine cache is full

  // congestion related
  GET_CONGESTION_TOTAL,
  GET_CONGESTION_FROM_THREAD_CACHE_HIT,
  GET_CONGESTION_FROM_GLOBAL_CACHE_HIT,
  GET_CONGESTION_FROM_GLOBAL_CACHE_MISS,

  PROCESSOR_STAT_COUNT
};

extern ObRecRawStatBlock *processor_rsb;

#define PROCESSOR_INCREMENT_DYN_STAT(x)     \
  ObStatProcessor::incr_raw_stat_sum(processor_rsb, mutex_->thread_holding_, x, 1)
#define PROCESSOR_DECREMENT_DYN_STAT(x)     \
  ObStatProcessor::incr_raw_stat_sum(processor_rsb, mutex_->thread_holding_, x, -1)
#define PROCESSOR_SUM_DYN_STAT(x, y)        \
  ObStatProcessor::incr_raw_stat(processor_rsb, mutex_->thread_holding_, x, y)
#define PROCESSOR_READ_DYN_SUM(x, sum)        \
  ObStatProcessor::get_raw_stat_sum(processor_rsb, x, sum)

#define PROCESSOR_SUM_GLOBAL_DYN_STAT(x, y) \
  ObStatProcessor::incr_global_raw_stat_sum(processor_rsb, x, y)
#define PROCESSOR_READ_GLOBAL_DYN_SUM(x, S) \
  ObStatProcessor::get_global_raw_stat_sum(processor_rsb, x, S)

int init_processor_stats();

} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_PROCESSOR_STATS_H
