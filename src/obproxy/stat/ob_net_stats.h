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

#ifndef OBPROXY_NET_STATS_H
#define OBPROXY_NET_STATS_H

#include "stat/ob_stat_processor.h"

namespace oceanbase
{
namespace obproxy
{
namespace net
{

enum ObNetStats
{
  NET_HANDLER_RUN,
  NET_READ_BYTES,
  NET_WRITE_BYTES,
  NET_CLIENT_CONNECTIONS_CURRENTLY_OPEN,
  NET_GLOBAL_CLIENT_CONNECTIONS_CURRENTLY_OPEN, // global
  NET_GLOBAL_CONNECTIONS_CURRENTLY_OPEN, // global
  NET_GLOBAL_ACCEPTS_CURRENTLY_OPEN, // global, count of accept task
  NET_CALLS_TO_READFROMNET,
  NET_CALLS_TO_READ,
  NET_CALLS_TO_READ_NODATA,
  NET_CALLS_TO_WRITETONET,
  NET_CALLS_TO_WRITE,
  NET_CALLS_TO_WRITE_NODATA,
  INACTIVITY_COP_LOCK_ACQUIRE_FAILURE,
  KEEP_ALIVE_LRU_TIMEOUT_TOTAL,
  KEEP_ALIVE_LRU_TIMEOUT_COUNT,
  DEFAULT_INACTIVITY_TIMEOUT,
  NET_STAT_COUNT
};

extern ObRecRawStatBlock *net_rsb;

#define NET_ATOMIC_INCREMENT_DYN_STAT(thread, x) \
  (void)ObStatProcessor::atomic_incr_raw_stat_sum(net_rsb, thread, x, 1)

#define NET_ATOMIC_DECREMENT_DYN_STAT(thread, x) \
  (void)ObStatProcessor::atomic_incr_raw_stat_sum(net_rsb, thread, x, -1)

#define NET_THREAD_READ_DYN_SUM(thread, x, sum) \
  (void)ObStatProcessor::get_thread_raw_stat_sum(net_rsb, thread, x, sum)



#define NET_INCREMENT_DYN_STAT(x) \
  (void)ObStatProcessor::incr_raw_stat_sum(net_rsb, mutex_->thread_holding_, x, 1)

#define NET_DECREMENT_DYN_STAT(x) \
  (void)ObStatProcessor::incr_raw_stat_sum(net_rsb, mutex_->thread_holding_, x, -1)

#define NET_SUM_DYN_STAT(x, r) \
  (void)ObStatProcessor::incr_raw_stat_sum(net_rsb, mutex_->thread_holding_, x, r)

#define NET_READ_DYN_SUM(x, sum) \
  (void)ObStatProcessor::get_raw_stat_sum(net_rsb, x, sum)

#define NET_READ_DYN_STAT(x, count, sum) \
  do { \
    ObStatProcessor::get_raw_stat_sum(net_rsb, x, sum);     \
    ObStatProcessor::get_raw_stat_count(net_rsb, x, count); \
  } while (0)

// For global access
#define NET_SUM_GLOBAL_DYN_STAT(x, r) \
  (void)ObStatProcessor::incr_global_raw_stat_sum(net_rsb, (x), (r))
#define NET_READ_GLOBAL_DYN_SUM(x, sum) \
  (void)ObStatProcessor::get_global_raw_stat_sum(net_rsb, x, sum)

int init_net_stats();

} // end of namespace net
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_NET_STATS_H
