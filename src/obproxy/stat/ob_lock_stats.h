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

#ifndef OBPROXY_LOCK_STATS_H
#define OBPROXY_LOCK_STATS_H

#include "stat/ob_stat_processor.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{

enum ObLockStats
{
  NET_VC_LOCK = 0,
  NET_HANDLER_LOCK,
  ETHREAD_LOCK,
  CONGESTION_ENTRY_LOCK,
  CONGESTION_TABLE_LOCK,
  MYSQL_PROXY_LOCK,
  CLIENT_VC_LOCK,
  CACHE_CLEANER_LOCK,
  TABLE_ENTRY_MAP_LOCK,
  SQL_TABLE_ENTRY_MAP_LOCK,
  PARTITION_ENTRY_MAP_LOCK,
  ROUTINE_ENTRY_MAP_LOCK,
  COMMON_LOCK,
  INDEX_ENTRY_MAP_LOCK,
  TABLEGROUP_ENTRY_MAP_LOCK,
  RPC_QUERY_ASYNC_ENTRY_MAP_LOCK,
  RPC_REQ_CTX_MAP_LOCK,
  MAX_LOCK_COUNT
};

extern ObRecRawStatBlock *lock_rsb;

#define LOCK_INCREMENT_DYN_STAT(x) \
  (void)ObStatProcessor::incr_raw_stat_sum(lock_rsb, m->thread_holding_, x, 1)
#define LOCK_DECREMENT_DYN_STAT(x) \
  (void)ObStatProcessor::incr_raw_stat_sum(lock_rsb, m->thread_holding_, x, -1)
#define LOCK_SUM_DYN_STAT(x, y) \
  (void)ObStatProcessor::incr_raw_stat(lock_rsb, m->thread_holding_, x, y)
#define LOCK_READ_DYN_SUM(x, sum) \
  (void)ObStatProcessor::get_raw_stat_sum(lock_rsb, x, sum)

#define LOCK_SUM_GLOBAL_DYN_STAT(x, y) \
  (void)ObStatProcessor::incr_global_raw_stat_sum(lock_rsb, x, y)
#define LOCK_READ_GLOBAL_DYN_SUM(x, S) \
  (void)ObStatProcessor::get_global_raw_stat_sum(lock_rsb, x, S)

int init_lock_stats();

} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_LOCK_STATS_H
