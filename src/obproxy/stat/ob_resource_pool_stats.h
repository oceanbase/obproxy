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

#ifndef OBPROXY_RESOURCE_POOL_STATS_H
#define OBPROXY_RESOURCE_POOL_STATS_H

#include "stat/ob_stat_processor.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

enum ObResourcePoolStats
{
  CREATE_CLUSTER_RESOURCE_COUNT = 0,
  CREATE_CLUSTER_RESOURCE_TIME,
  GET_CLUSTER_RESOURCE_FAIL_COUNT,
  PARALLEL_CREATE_CLUSTER_RESOURCE_COUNT,

  CURRENT_CLUSTER_RESOURCE_COUNT,

  CREATE_CLUSTER_RESOURCE_SUCC_COUNT,
  DELETE_CLUSTER_RESOURCE_COUNT,
  FREE_CLUSTER_RESOURCE_COUNT,

  CREATE_CLUSTER_CLIENT_POOL_SUCC_COUNT,
  DELETE_CLUSTER_CLIENT_POOL_COUNT,
  FREE_CLUSTER_CLIENT_POOL_COUNT,

  CREATE_ASYNC_COMMON_TASK_COUNT,
  DESTROY_ASYNC_COMMON_TASK_COUNT,

  RESOURCE_POOL_STAT_COUNT
};

extern ObRecRawStatBlock *resource_pool_rsb;

#define RESOURCE_POOL_INCREMENT_DYN_STAT(x) \
  (void)ObStatProcessor::incr_raw_stat_sum(resource_pool_rsb, mutex_->thread_holding_, x, 1)
#define RESOURCE_POOL_DECREMENT_DYN_STAT(x) \
  (void)ObStatProcessor::incr_raw_stat_sum(resource_pool_rsb, mutex_->thread_holding_, x, -1)
#define RESOURCE_POOL_SUM_DYN_STAT(x, y) \
  (void)ObStatProcessor::incr_raw_stat(resource_pool_rsb, mutex_->thread_holding_, x, y)
#define RESOURCE_POOL_READ_DYN_SUM(x, sum) \
  (void)ObStatProcessor::get_raw_stat_sum(resource_pool_rsb, x, sum)

#define RESOURCE_POOL_SUM_GLOBAL_DYN_STAT(x, y) \
  (void)ObStatProcessor::incr_global_raw_stat_sum(resource_pool_rsb, x, y)
#define RESOURCE_POOL_READ_GLOBAL_DYN_SUM(x, S) \
  (void)ObStatProcessor::get_global_raw_stat_sum(resource_pool_rsb, x, S)

#define CLIENT_POOL_INCREMENT_DYN_STAT(x) \
  if (NULL != this_ethread()) { \
    (void)ObStatProcessor::incr_raw_stat_sum(resource_pool_rsb, event::this_ethread(), x, 1); \
  }

#define ASYNC_COMMON_TASK_INCREMENT_DYN_STAT(x) \
  if (NULL != event::this_ethread()) { \
    (void)ObStatProcessor::incr_raw_stat_sum(resource_pool_rsb, event::this_ethread(), x, 1); \
  }

int init_resource_pool_stats();

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_RESOURCE_POOL_STATS_H
