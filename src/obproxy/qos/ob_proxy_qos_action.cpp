/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PROXY

#include "qos/ob_proxy_qos_action.h"
#include "lib/utility/ob_print_utils.h"
#include "qos/ob_proxy_qos_stat_processor.h"

namespace oceanbase
{
namespace obproxy
{
namespace qos
{
using namespace common;

int64_t ObProxyQosAction::to_string(char *buf, int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(type));
  J_OBJ_END();
  return pos;
}

int ObProxyQosActionBreaker::calc(bool &is_pass)
{
  int ret = OB_SUCCESS;
  is_pass = false;
  return ret;
}

int ObProxyQosActionLimit::do_calc(bool &is_pass)
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
  int64_t interval_per_request_micros = sec_to_usec(1) * LIMIT_ACTION_RATIO / limit_qps_;

  int64_t max_token = limit_qps_ / LIMIT_ACTION_RATIO;
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
            // -1 是因为要当前请求要消耗一个 token
            target_token -= 1;

            int64_t new_token = 0;
            int64_t last_token = 0;

            do {
              // 如果更新失败, 可能有并发 case 3, 再次计算并更新. 因为持有锁, 就不暂停了
              last_token = token_;
              // 目标 token 减去并发消耗掉的 token
              new_token = target_token - (first_last_token - last_token);

              // 做个防御
              if (OB_UNLIKELY(new_token < 0)) {
                new_token = 0;
              }
            } while (!ATOMIC_BCAS(&token_, last_token, new_token));

            is_pass = true;
            debug_value = 4;
            LOG_DEBUG("ObProxyQosActionLimit update token", K(new_token), K(first_last_token), K(last_token), K(ret));
          }
        } else {
          debug_value = 5;
          ret = OB_EAGAIN;
        }

        // 释放锁
        lock_.wrunlock();
      } else {
        // 有可能有并发请求在更新两个参数, 那等会儿, 然后再重新获取时间
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

  LOG_DEBUG("ObProxyQosActionLimit do calc end", K(debug_value), K(ret));

  return ret;
}

int ObProxyQosActionLimit::calc(bool &is_pass)
{
  int ret = OB_SUCCESS;
  is_pass = true;

  if (limit_qps_ <= 0) {
    is_pass = false;
  } else {
    while (OB_EAGAIN == (ret = do_calc(is_pass))) {
      PAUSE();
    }
  }

  return ret;
}

int ObProxyQosActionCircuitBreaker::calc(bool &is_pass)
{
  int ret = OB_SUCCESS;

  if (is_circuit_ && time_window_ > 0 && limit_fuse_time_ > 0) {
    ObHRTime current_time = 0;
    current_time = get_hrtime_internal();
    if (end_fuse_time_ < current_time) {
      is_circuit_ = 0;
      end_fuse_time_ = 0;
    }
  }

  if (OB_UNLIKELY(is_circuit_)) {
    is_pass = false;
  } else if (time_window_ > 0) {
    int64_t cost = 0;
    if (OB_FAIL(g_ob_qos_stat_processor.calc_cost(cluster_name_, tenant_name_, database_name_,
            user_name_, cost, time_window_))) {
      LOG_WDIAG("cacl cost failed for breaker", K(ret), K(cluster_name_), K(tenant_name_),
                   K(database_name_), K(user_name_));
    } else {
      // cost的单位是us，time_window的单位是秒
      if (cost > time_window_ * (int64_t)(limit_conn_ * 1000000)) {
        is_circuit_ = true;
        ObHRTime current_time = 0;
        current_time = get_hrtime_internal();
        ObHRTime end_fuse_time = current_time + hrtime_from_sec(limit_fuse_time_);
        end_fuse_time_ = end_fuse_time;
      }
      is_pass = !is_circuit_;
    }
  } else {
    if (OB_FAIL(g_ob_qos_stat_processor.calc_qps_and_rt(cluster_name_, tenant_name_, database_name_,
                                                        user_name_, rt_, qps_, is_circuit_))) {
      LOG_WDIAG("fail to calc qps and rt", K_(cluster_name), K_(tenant_name), K_(database_name),
               K_(user_name), K_(rt), K_(qps), K(ret));
    } else {
      is_pass = !is_circuit_;
    }
  }

  return ret;
}

} // end qos
} // end obproxy
} // end oceanbase
