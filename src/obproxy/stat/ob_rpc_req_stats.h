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

#ifndef OBPROXY_RPC_REQ_STATS_H
#define OBPROXY_RPC_REQ_STATS_H

#include "obutils/ob_proxy_config.h"
#include "stat/ob_stat_processor.h"
#include "iocore/eventsystem/ob_thread.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

class ObRpcReqThreadQpsStat
{
public:
  static void update_last_sec_rpc_req_stat();
  static void inc_rpc_req_stat(bool is_shard);
  static void dec_rpc_req_stat(bool is_shard);
  static int64_t &get_sub_req_async_thread_iso_range()
  {
    static __thread int64_t thread_range = 1;
    return thread_range;
  }
  static int64_t &get_last_sec_rpc_req()
  {
    static __thread int64_t last_req_count = 0;
    return last_req_count;
  }
  static int64_t &get_last_sec_single_rpc_req()
  {
    static __thread int64_t last_req_count = 0;
    return last_req_count;
  }
  static int64_t &get_last_sec_shard_rpc_req()
  {
    static __thread int64_t last_req_count = 0;
    return last_req_count;
  }
private:
  static ObHRTime &get_last_sec_time()
  {
    static __thread ObHRTime time = 0;
    return time;
  }

  static int64_t &get_current_rpc_req()
  {
    static __thread int64_t req_count = 0;
    return req_count;
  }

  static int64_t &get_current_single_rpc_req()
  {
    static __thread int64_t req_count = 0;
    return req_count;
  }

  static int64_t &get_current_shard_rpc_req()
  {
    static __thread int64_t req_count = 0;
    return req_count;
  }
  static void update_async_thread_iso_range();
};

enum ObRpcReqStats {
  CURRENTLY_HANDLING_RPC_REQ,
  CURRENTLY_HANDLING_SINGLE_RPC_REQ,
  CURRENTLY_HANDLING_SHARD_RPC_REQ,
  REDIS_COMMAND_PROCESSED,
  REDIS_TOTAL_NET_INPUT_BYTES,
  REDIS_TOTAL_NET_OUTPUT_BYTES,
  MAX_RPC_REQ_STAT_COUNT
};

extern ObRecRawStatBlock *rpc_req_rsb;

#define RPC_REQ_INCREMENT_DYN_STAT(thread, x) (void)ObStatProcessor::incr_raw_stat_sum(rpc_req_rsb, thread, x, 1)
#define RPC_REQ_DECREMENT_DYN_STAT(thread, x) (void)ObStatProcessor::incr_raw_stat_sum(rpc_req_rsb, thread, x, -1)
#define RPC_REQ_THREAD_READ_DYN_STAT(thread, x, sum)                                                                   \
  (void)ObStatProcessor::get_thread_raw_stat_sum(rpc_req_rsb, thread, x, sum)

int init_rpc_req_stats();

} // namespace proxy
} // namespace obproxy
} // namespace oceanbase

#endif // OBPROXY_RPC_REQ_STATS_H
