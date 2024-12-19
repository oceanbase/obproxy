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


#include "stat/ob_rpc_req_stats.h"
#include "iocore/eventsystem/ob_ethread.h"

namespace oceanbase
{

using namespace common;

namespace obproxy
{
namespace proxy
{


#define RPC_REGISTER_RAW_STAT(rsb, rec_type, name, data_type, id, sync_type, persist_type)                             \
  if (OB_SUCC(ret)) {                                                                                                  \
    ret = g_stat_processor.register_raw_stat(rsb, rec_type, name, data_type, id, sync_type, persist_type);             \
  }

ObRecRawStatBlock *rpc_req_rsb = NULL;

int init_rpc_req_stats()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(rpc_req_rsb = g_stat_processor.allocate_raw_stat_block(MAX_RPC_REQ_STAT_COUNT, XFH_RPC_REQ_STATE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_LOG(WDIAG, "fail to alloc mem for lock_rsb", K(ret));
  } else {
    RPC_REGISTER_RAW_STAT(rpc_req_rsb, RECT_PROCESS, "curent_handing_rpc_req", RECD_INT, CURRENTLY_HANDLING_RPC_REQ,
                          SYNC_SUM, RECP_NULL);
    RPC_REGISTER_RAW_STAT(rpc_req_rsb, RECT_PROCESS, "curent_handing_single_rpc_req", RECD_INT, CURRENTLY_HANDLING_SINGLE_RPC_REQ,
                          SYNC_SUM, RECP_NULL);
    RPC_REGISTER_RAW_STAT(rpc_req_rsb, RECT_PROCESS, "curent_handing_shard_rpc_req", RECD_INT, CURRENTLY_HANDLING_SHARD_RPC_REQ,
                          SYNC_SUM, RECP_NULL);
    RPC_REGISTER_RAW_STAT(rpc_req_rsb, RECT_PROCESS, "redis_commands_processed", RECD_INT, REDIS_COMMAND_PROCESSED,
                          SYNC_SUM, RECP_PERSISTENT);
    RPC_REGISTER_RAW_STAT(rpc_req_rsb, RECT_PROCESS, "redis_total_net_input_bytes", RECD_INT, REDIS_TOTAL_NET_INPUT_BYTES,
                          SYNC_SUM, RECP_PERSISTENT);
    RPC_REGISTER_RAW_STAT(rpc_req_rsb, RECT_PROCESS, "redis_total_net_output_bytes", RECD_INT, REDIS_TOTAL_NET_OUTPUT_BYTES,
                          SYNC_SUM, RECP_PERSISTENT);
  }

  return ret;
}

// keep work_thread * single_qps * 10 > 2 * async_thread * sub_req_weight * sub_req_qps
void ObRpcReqThreadQpsStat::update_async_thread_iso_range()
{
  int64_t work_thread_num = obutils::get_global_proxy_config().work_thread_num;
  int64_t async_thread_num = obutils::get_global_proxy_config().rpc_async_task_thread_num;
  int64_t rpc_sub_req_weight = obutils::get_global_proxy_config().rpc_sub_request_weight;
  int64_t single_rpc_req_qps = get_last_sec_single_rpc_req();
  int64_t shard_rpc_req_qps = get_last_sec_shard_rpc_req();

  async_thread_num = common::min(work_thread_num, async_thread_num);
  int64_t thread_range = 0;
  double rate = 0.0;
  if (single_rpc_req_qps <= 0) {
    thread_range = async_thread_num;
  } else if (async_thread_num > 0) {
    thread_range = 1;
    if (async_thread_num > 1) {
      rate = (((double)shard_rpc_req_qps * 2 * (double)rpc_sub_req_weight) / ((double)single_rpc_req_qps * 10));
      thread_range += rate * (async_thread_num - 1);
      thread_range = (thread_range > async_thread_num) ? async_thread_num : thread_range;
    }
  }
  get_sub_req_async_thread_iso_range() = thread_range;
  PROXY_RPC_SM_LOG(DEBUG, "update async thread isolation range", K(async_thread_num), K(work_thread_num),
                   K(single_rpc_req_qps), K(shard_rpc_req_qps), K(thread_range), K(rate), K(rpc_sub_req_weight));
}

void ObRpcReqThreadQpsStat::update_last_sec_rpc_req_stat()
{
  if (event::get_hrtime() - get_last_sec_time() > HRTIME_SECOND) {
    get_last_sec_time() = event::get_hrtime();
    get_last_sec_rpc_req() = get_current_rpc_req();
    get_last_sec_single_rpc_req() = get_current_single_rpc_req();
    get_last_sec_shard_rpc_req() = get_current_shard_rpc_req();
    get_current_rpc_req() = 0;
    get_current_single_rpc_req() = 0;
    get_current_shard_rpc_req() = 0;
    update_async_thread_iso_range();
  }
}

void ObRpcReqThreadQpsStat::inc_rpc_req_stat(bool is_shard)
{
  if (is_shard) {
    get_current_shard_rpc_req()++;
    RPC_REQ_INCREMENT_DYN_STAT(event::this_ethread(), CURRENTLY_HANDLING_SHARD_RPC_REQ);
  } else {
    get_current_single_rpc_req()++;
    RPC_REQ_INCREMENT_DYN_STAT(event::this_ethread(), CURRENTLY_HANDLING_SINGLE_RPC_REQ);
  }

  ObRpcReqThreadQpsStat::update_last_sec_rpc_req_stat();
}

void ObRpcReqThreadQpsStat::dec_rpc_req_stat(bool is_shard)
{
  if (is_shard) {
    RPC_REQ_DECREMENT_DYN_STAT(event::this_ethread(), CURRENTLY_HANDLING_SINGLE_RPC_REQ);
  } else {
    RPC_REQ_DECREMENT_DYN_STAT(event::this_ethread(), CURRENTLY_HANDLING_SHARD_RPC_REQ);
  }
}

} // namespace proxy
} // namespace obproxy
} // namespace oceanbase
