/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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

enum ObRpcReqType
{
  OB_RPC_LOGIN = 0,
  OB_RPC_EXECUTE,
  OB_RPC_BATCH_EXECUTE,
  OB_RPC_EXECUTE_QUERY,
  OB_RPC_QUERY_AND_MUTATE,
  OB_RPC_EXECUTE_QUERY_SYNC,
  OB_RPC_DIRECT_LOAD,
  OB_RPC_LS_EXECUTE,
  OB_MAX_TABLE_API_TYPE
};

enum ObRpcReqState
{
  OB_RPC_REQ_IN_ANALYZE_REQUEST = 0,
  OB_RPC_REQ_IN_BUILDING_CLUSTER,
  OB_RPC_REQ_GET_GLOBAL_INDEX,
  OB_RPC_REQ_GET_PARTITION_ID,
  OB_RPC_REQ_GET_LS_ID,
  OB_RPC_REQ_IN_SHARDING,
  OB_RPC_REQ_IN_SENDING_TO_SERVER,
  OB_RPC_REQ_IN_ANALYZE_RESPONSE,
  OB_RPC_REQ_IN_SENDING_TO_CLINET,
  OB_RPC_REQ_MAX_STATE
};

class ObRpcReqThreadStat
{
public:
  static void update_rpc_req_state(ObRpcReqType type, ObRpcReqState cur_state, ObRpcReqState pre_state);
  static int64_t &get_current_rpc_req_in_analyze_request(ObRpcReqType type)
  {
   return ObRpcReqStat[type][OB_RPC_REQ_IN_ANALYZE_REQUEST];
  }
  static int64_t &get_current_rpc_req_in_building_cluster(ObRpcReqType type)
  {
    return ObRpcReqStat[type][OB_RPC_REQ_IN_BUILDING_CLUSTER];
  }
  static int64_t &get_current_rpc_req_in_get_globalindex(ObRpcReqType type)
  {
    return ObRpcReqStat[type][OB_RPC_REQ_GET_GLOBAL_INDEX];
  }
  static int64_t &get_current_rpc_req_in_get_partition_id(ObRpcReqType type)
  {
    return ObRpcReqStat[type][OB_RPC_REQ_GET_PARTITION_ID];
  }
  static int64_t &get_current_rpc_req_in_get_ls_id(ObRpcReqType type)
  {
    return ObRpcReqStat[type][OB_RPC_REQ_GET_LS_ID];
  }
  static int64_t &get_current_rpc_req_in_sharding(ObRpcReqType type)
  {
    return ObRpcReqStat[type][OB_RPC_REQ_IN_SHARDING];
  }
  static int64_t &get_current_rpc_req_in_send_server(ObRpcReqType type)
  {
    return ObRpcReqStat[type][OB_RPC_REQ_IN_SENDING_TO_SERVER];
  }
  static int64_t &get_current_rpc_req_in_analyze_response(ObRpcReqType type)
  {
    return ObRpcReqStat[type][OB_RPC_REQ_IN_ANALYZE_RESPONSE];
  }
  static int64_t &get_current_rpc_req_in_send_client(ObRpcReqType type)
  {
    return ObRpcReqStat[type][OB_RPC_REQ_IN_SENDING_TO_CLINET];
  }
  static thread_local int64_t ObRpcReqStat[OB_MAX_TABLE_API_TYPE + 1][OB_RPC_REQ_MAX_STATE + 1];
};

enum  {
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
