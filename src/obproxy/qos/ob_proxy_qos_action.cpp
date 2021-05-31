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

  // 1. first, check whethere exceed next_free_token_micros
  // 2. if exceeded, check over how much
  //   2.1 if current time is between next_free_token_micros and next_free_token_micros + 1, consume the new token
  //   2.2 if current time greater than next_free_token_micros + 1, consume the new token and save the other token
  // 3. if not exceeded, consume one token
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
        // if update failed, maybe other update next_free_token_micros, re-call this func
        ret = OB_EAGAIN;
        debug_value = 1;
      } else {
        is_pass = true;
        debug_value = 2;
      }
    } else {
      // case 2.2
      if (OB_SUCC(lock_.try_wrlock())) {
        // record the token num before update time
        int64_t first_last_token = token_;

        int64_t next_free_token_micros = next_free_token_micros_;
        if (current_time_micros >= next_free_token_micros + interval_per_request_micros) {
          int64_t current_token_total = current_time_micros / interval_per_request_micros;
          int64_t new_next_free_token_micros = (current_token_total + 1 ) * interval_per_request_micros;
          if (!ATOMIC_BCAS(&next_free_token_micros_, next_free_token_micros, new_next_free_token_micros)) {
            // if update failed, maybe case 2.1 is happening, re-call this func
            ret = OB_EAGAIN;
            debug_value = 3;
          } else {
            // because cross next_free_token_micros, so need add 1
            int64_t add_token = current_token_total - next_free_token_micros / interval_per_request_micros + 1;

            int64_t target_token = first_last_token + add_token;
            target_token = target_token > max_token ? max_token : target_token;
            // becaure consume one token, so sub 1
            target_token -= 1;

            int64_t new_token = 0;
            int64_t last_token = 0;

            do {
              last_token = token_;
              new_token = target_token - (first_last_token - last_token);

              // just defence
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

        // release lock
        lock_.wrunlock();
      } else {
        debug_value = 6;
        ret = OB_EAGAIN;
      }
    }
  } else {
    // case 3
    int64_t last_token = token_;
    while (last_token > 0 && !ATOMIC_BCAS(&token_, last_token, last_token - 1)) {
      PAUSE();
      last_token = token_;
    }

    if (last_token <= 0) {
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
      LOG_WARN("cacl cost failed for breaker", K(ret), K(cluster_name_), K(tenant_name_),
                   K(database_name_), K(user_name_));
    } else {
      // cost unit is us, time_window unit is second
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
      LOG_WARN("fail to calc qps and rt", K_(cluster_name), K_(tenant_name), K_(database_name),
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
