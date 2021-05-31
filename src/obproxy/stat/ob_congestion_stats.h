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

#ifndef OBPROXY_CONGESTION_STATS_H
#define OBPROXY_CONGESTION_STATS_H

#include "iocore/eventsystem/ob_event_system.h"
#include "stat/ob_stat_processor.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

int register_congest_stats();

extern ObRecRawStatBlock *congest_rsb;

/*
 * each module needs * to enumerate its stats separately and register them
 */
enum {
  dead_congested_stat,
  alive_congested_stat,
  congest_stat_count,
};

#define CONGEST_SUM_GLOBAL_DYN_STAT(x, y) \
  ObStatProcessor::incr_global_raw_stat_sum(congest_rsb, x, y)
#define CONGEST_INCREMENT_DYN_STAT(x) \
  ObStatProcessor::incr_raw_stat(congest_rsb, mutex_->thread_holding_, x, 1)

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_CONGESTION_STATS_H
