/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX PROXY
#include "proxy/rpc/rpclib/ob_rpc_throttle.h"
#include "proxy/rpc/ob_rpc_req.h"

using namespace oceanbase::common;


namespace oceanbase
{
namespace obproxy
{
namespace obkv
{

ObRpcThrottle &get_global_rpc_throttle()
{
  static ObRpcThrottle g_rpc_throttle;
  return g_rpc_throttle;
}

int ObRpcThrottle::init()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_EDIAG("ObRpcThrottle has been inited", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObRpcThrottle::update_holding_resource(int64_t size)
{
  ATOMIC_AAF(&holding_resource_, size);
}

int ObRpcThrottle::do_push_index(int64_t current_time_sec)
{
  int ret = OB_SUCCESS;

  if (current_time_sec > index_time_sec_) {
    // 先获取写锁，避免同时有人使用 index 时间
    if (OB_SUCC(lock_.try_wrlock())) {
      // 如果确实大于 index 时间, 则更新 index 时间, 更新时间时, 需要把推进的数组中的值清空
      if (current_time_sec > index_time_sec_) {
        if (current_time_sec - index_time_sec_ > RPC_QPS_VALUE_COUNT) {
          MEMSET(rpc_qps_, 0, sizeof(rpc_qps_));
        } else {
          int64_t index = -1;
          for (int64_t i = index_time_sec_ + 1; i <= current_time_sec; i++) {
            index = i % RPC_QPS_VALUE_COUNT;
            rpc_qps_[index] = 0;
          }
        }
        index_time_sec_ = current_time_sec;
      } else {
        // 如果当前时间小于等于 index, 说明有并发更新了这个时间, 重新进来即可
        ret = OB_EAGAIN;
      }
      lock_.wrunlock();
    } else {
      ret = OB_EAGAIN;
    }
  }
  return ret;
}

int ObRpcThrottle::push_index()
{
  int ret = OB_SUCCESS;
  int64_t current_time_sec = hrtime_to_sec(get_hrtime_internal());

  while (OB_EAGAIN == (ret = do_push_index(current_time_sec))) {
    PAUSE();
  }
  // push 之后, 当前时间肯定小于 index_time
    // 获取读锁, 避免有其他并发请求推进 index_time
  if (OB_SUCC(lock_.rdlock())) {
    int64_t index = index_time_sec_ % RPC_QPS_VALUE_COUNT;
    int64_t new_value = ATOMIC_AAF(&rpc_qps_[index], 1);
    lock_.rdunlock();
    LOG_DEBUG("rpc throttle store value", K(index), "value", new_value);
  }
  return ret;
}

int ObRpcThrottle::calc_qps(int64_t &cur_qps)
{
  int ret = OB_SUCCESS;
  int64_t index = -1;
  int64_t index_time_sec = index_time_sec_;
  for (int64_t i = index_time_sec - RPC_QPS_CALC_COUNT; i < index_time_sec; ++i) {
    index = i % RPC_QPS_VALUE_COUNT;
    cur_qps += rpc_qps_[index];
    LOG_DEBUG("rpc_qps value:",K(index), K(rpc_qps_[index]));
  }
  cur_qps /= RPC_QPS_CALC_COUNT;
  return ret;
}

int ObRpcThrottle::calc(bool &is_pass)
{
  // limit qps is cur_qps * 70%
  // sysuser can set limit_qps
  int ret = OB_SUCCESS;
  is_pass = true;
  int64_t limit_qps = obutils::get_global_proxy_config().rpc_throttle_limit_qps_qa;
  if (OB_LIKELY(limit_qps == 0)) {
    int64_t cur_qps = 0;
    if (OB_FAIL(calc_qps(cur_qps))) {
      LOG_WDIAG("fail to calc qps", K(ret));
    } else {
      limit_qps_ = cur_qps * 7 / 10;
    }
  } else {
    limit_qps_ = limit_qps;
  }
  limit_qps_ = (limit_qps_ == 0) ? 10000: limit_qps_;
  LOG_DEBUG("rpc throttle limit qps:", K(limit_qps_), K(ret));
  while (OB_EAGAIN == (ret = do_calc(is_pass, limit_qps_))) {
    PAUSE();
  }
  return ret;
}

int ObRpcThrottle::do_calc(bool &is_pass, int64_t limit_qps)
{
  int ret = OB_SUCCESS;
  int debug_value = 0;

  // 1. 先判断是不是超过 next_free_token_micros
  // 2. 如果是, 检查超过多少
  //   2.1 如果当前时间在 next_free_token_micros 和 next_free_token_micros + 1 之间, 则消耗掉新增的那个令牌
  //   2.2 如果前时间大于 next_free_token_micros + 1 , 则除了消耗掉新增的一个令牌, 还要额外多存些令牌
  // 3. 如果不是, 消耗令牌

  int64_t current_time_micros = hrtime_to_usec(get_hrtime_internal());
  int64_t next_free_token_micros = next_free_token_micros_;
  int64_t interval_per_request_micros = sec_to_usec(1) / limit_qps;
  int64_t max_token = limit_qps;
  max_token = max_token < 1 ? 1 : max_token;

  if (current_time_micros > next_free_token_micros) {
    // case 2
    if (current_time_micros < next_free_token_micros + interval_per_request_micros) {
      // case 2.1
      if (!ATOMIC_BCAS(&next_free_token_micros_, next_free_token_micros, next_free_token_micros + interval_per_request_micros)) {
        // 如果更新失败, 则可能有其他并发推进了 next_free_token_micros, 重新获取时间
        ret = OB_EAGAIN;
        debug_value = 1;
      } else {
        is_pass = true;
        debug_value = 2;
      }
    } else {
      // case 2.2
      // 因为要更新两个参数, 抢锁, 更新时间前，并发 case 只可能是 case 2.1
      if (OB_SUCC(lock_.try_wrlock())) {
        // 记录更新时间前的 token 数量, 这个时候不会有并发 case 3(只要没更新时间, 则正常 case 都会进入 case 2 的 if 语句里), 所以这个时候的 token 是准的
        int64_t first_last_token = token_;

        int64_t next_free_token_micros = next_free_token_micros_;
        if (current_time_micros >= next_free_token_micros + interval_per_request_micros) {
          int64_t current_token_total = current_time_micros / interval_per_request_micros;
          int64_t new_next_free_token_micros = (current_token_total + 1 ) * interval_per_request_micros;
          if (!ATOMIC_BCAS(&next_free_token_micros_, next_free_token_micros, new_next_free_token_micros)) {
            // 如果更新失败, 则可能有其他并发 case 2.1, 推进了 next_free_token_micros, 重新获取时间
            ret = OB_EAGAIN;
            debug_value = 3;
          } else {
            // 如果时间更新成功, 并发 case 要么是 case 2.1, 要么是 case 3. 但是 2.1 已经不影响当前 token 的更新. 只关注 case 3 即可
            // +1 是因为只要跨过 next_free_token_micros 就需要加 1
            int64_t add_token = current_token_total - next_free_token_micros / interval_per_request_micros + 1;

            // 利用 first_last_token 得到目标 token
            int64_t target_token = first_last_token + add_token;
            target_token = target_token > max_token ? max_token : target_token;
            target_token -= 1;

            int64_t new_token = 0;
            int64_t last_token = 0;

            do {
              // 如果更新失败, 可能有并发 case 3, 再次计算并更新. 因为持有锁, 就不暂停了
              last_token = token_;
              // 目标 token 减去并发消耗掉的 token
              new_token = target_token - (first_last_token - last_token);
              if (OB_UNLIKELY(new_token < 0)) {
                new_token = 0;
              }
            } while (!ATOMIC_BCAS(&token_, last_token, new_token));

            is_pass = true;
            debug_value = 4;
            LOG_DEBUG("ObRpcThrottle update token", K(new_token), K(first_last_token), K(last_token), K(ret));
          }
        } else {
          debug_value = 5;
          ret = OB_EAGAIN;
        }
        lock_.wrunlock();
      } else {
        // 有可能有并发请求在更新两个参数
        debug_value = 6;
        ret = OB_EAGAIN;
      }
    }
  } else {
    // case 3
    int64_t last_token = token_;
    // 如果 token > 0, 则一直获取 token, 直到 token 为 0
    while (last_token > 0 && !ATOMIC_BCAS(&token_, last_token, last_token - 1)) {
      PAUSE();
      last_token = token_;
    }

    // 如果 token < 0, 说明 token 被用光了
    if (last_token <= 0) {
      // 这里要加读锁, 如果加读锁成功, 说明没有并发要更新 token 的, 这时如果依然没有 token, 才能拒绝
      if (OB_SUCC(lock_.try_rdlock())) {
        last_token = token_;
        if (last_token > 0) {
          debug_value = 7;
          ret = OB_EAGAIN;
        } else {
          debug_value = 8;
          is_pass = false;
        }
        lock_.rdunlock();
      } else {
        // 如果获取失败, 则说明当前有并发准备更新 token, 等会儿再来
        debug_value = 9;
        ret = OB_EAGAIN;
      }
    } else {
      debug_value = 10;
      is_pass = true;
    }
  }

  LOG_DEBUG("ObRpcThrottle do calc end", K(debug_value), K(ret));
  return ret;
}

} // end of namespace obkv
} // end of namespace obproxy
} // end of namespace oceanbase
