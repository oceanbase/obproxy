/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef __OB_RPC_THROTTLE_H__
#define __OB_RPC_THROTTLE_H__

#include "lib/ob_define.h"
#include "lib/time/ob_hrtime.h"
#include "lib/lock/ob_drw_lock.h"
#include "obutils/ob_proxy_config.h"

namespace oceanbase
{
namespace obproxy
{
namespace obkv
{

// 第 11 个是用来存最新的值，比如从 0 秒开始，现在是第 10秒，那使用 0~9，第10秒就存最新的，所以最少是 11 个
// +1，是为了防止时间的不准，允许多个线程间的时间误差在 1s 以内(+2 就表示允许误差在 2s 内)，比如线程 1 认为现在是第 10秒，线程2 认为现在是第 11 s，线程1推进 index 到第 10 秒，随后线程 2 会推进到 11秒。如果只有 11 个，那就会存储到 11/11 = 0，即第 0个位置上，而线程 1 认为当前是在第10s，继续使用 0 ~9 ，就会使用到已经被清空的 0 号数据
#define RPC_QPS_CALC_COUNT 10
#define RPC_QPS_VALUE_COUNT (11 + 1)

class ObRpcThrottle{
public:
  ObRpcThrottle(): is_inited_(false), is_trigger_throttle_(false),
                   is_freeze_request_(false), holding_resource_(0),
                   index_time_sec_(0),limit_qps_(0),
                   token_(0),next_free_token_micros_(0) {
    MEMSET(rpc_qps_, 0, sizeof(rpc_qps_));
  }
  ~ObRpcThrottle(){}

  int init();
  void update_holding_resource(int64_t size);
  bool is_trigger_throttle() { return is_trigger_throttle_; }
  bool is_freeze_request() { return is_freeze_request_; }
  int get_holding_resource() { return holding_resource_; }
  int get_limit_qps() { return limit_qps_; }
  void set_trigger_throttle(bool flag) { is_trigger_throttle_ = flag; }
  void set_freeze_request(bool flag) { is_freeze_request_ = flag; }

  int push_index();
  int calc(bool &is_pass);
private:
  int calc_qps(int64_t &cur_qps);
  int do_calc(bool &is_pass, int64_t limit_qps);
  int do_push_index(int64_t current_time_sec);

private:
  bool is_inited_;
  bool is_trigger_throttle_;
  bool is_freeze_request_;
  int64_t holding_resource_;
  common::DRWLock lock_;
  int64_t index_time_sec_; // 表示当前要访问的数据下标的时间
  int64_t rpc_qps_[RPC_QPS_VALUE_COUNT];
  int64_t limit_qps_;
  int64_t token_;
  int64_t next_free_token_micros_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcThrottle);
};
ObRpcThrottle &get_global_rpc_throttle();

} // end of namespace obkv
} // end of namespace obproxy
} // end of namespace oceanbase
#endif
