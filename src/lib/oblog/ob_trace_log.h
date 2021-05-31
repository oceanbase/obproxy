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

#ifndef  OCEANBASE_LIB_OBLOG_OB_TRACE_LOG_
#define  OCEANBASE_LIB_OBLOG_OB_TRACE_LOG_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include <sys/syscall.h>
#include "lib/ob_define.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_print_kv.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/time/tbtimeutil.h"
#include "lib/trace/ob_trace_event.h"
// #if __INCLUDE_LEVEL__ > 0
// #error "ob_trace_log.h should only be included in .cpp files directly"
// #endif

#define OB_TRACE_LOGGER ::oceanbase::common::TraceLog

#define CHECK_TRACE_TIMES(times)                                                    \
    (                                                                                       \
    {                                                                                       \
      const uint64_t *cur_trace_id = ObCurTraceId::get();                                   \
      static __thread uint64_t trace_id[3] = {0, 0, 1}; /* {trace_id1, trace_id2, count} */ \
      bool printable = true;                                                                \
      if (OB_ISNULL(cur_trace_id)) {                                                        \
      } else {                                                                              \
        if (cur_trace_id[0] == 0) {                                                         \
        } else if (cur_trace_id[0] == trace_id[0] && cur_trace_id[1] == trace_id[1]) {      \
          ++trace_id[2];                                                                    \
          if (trace_id[2] > times) {                                                        \
            printable = false;                                                              \
          }                                                                                 \
        } else {                                                                            \
          trace_id[0] = cur_trace_id[0];                                                    \
          trace_id[1] = cur_trace_id[1];                                                    \
          trace_id[2] = 1;                                                                  \
        }                                                                                   \
      }                                                                                     \
      printable;                                                                            \
    })

#define NG_TRACE_EXT_TIMES(times, ...)\
  if (CHECK_TRACE_TIMES(times)) {                               \
    NG_TRACE_EXT(__VA_ARGS__);                                          \
  }

#define NG_TRACE_TIMES(times, ...)                                      \
  if (CHECK_TRACE_TIMES(times)) {                               \
    NG_TRACE(__VA_ARGS__);                                              \
  }

#define PRINT_TRACE(log_buffer)                                     \
  if (OB_TRACE_LOGGER::get_log_level() <= OB_LOGGER.get_log_level()) {  \
    OB_LOGGER.log_message_kv("[TRACE]", OB_LOGGER.get_log_level(), "", 0, "", "[normal]", "TRACE", THE_TRACE); \
  }
#define FORCE_PRINT_TRACE(log_buffer, HEAD)                                                            \
  {                                                                                                        \
    OB_LOGGER.log_message_kv("[TRACE]", OB_LOGGER.get_log_level(), "", 0, "", HEAD, "TRACE", log_buffer); \
  }

namespace oceanbase
{
namespace common
{
class TraceLog
{
  static const char *const LOG_LEVEL_ENV_KEY;
public:
  static int32_t set_log_level(const char *log_level_str);
  static int32_t set_log_level(const char *log_level_str, volatile int &log_level_);
  inline static int32_t get_log_level()
  {
    if (!got_env_) {
      const char *log_level_str = getenv(LOG_LEVEL_ENV_KEY);
      set_log_level(log_level_str);
      got_env_ = true;
    }
    return log_level_;
  }
  static int32_t up_log_level();
  static int32_t down_log_level();
private:
  static const char *const level_strs_[];
  static volatile int log_level_;
  static bool got_env_;
};
}
}

#endif //OCEANBASE_LIB_OBLOG_OB_TRACE_LOG_
