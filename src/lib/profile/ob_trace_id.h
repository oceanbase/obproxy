/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <stdint.h>
#include <pthread.h>
#include "lib/net/ob_addr.h"
#include "lib/atomic/ob_atomic.h"

#ifndef OCEANBASE_COMMON_OB_TRACE_ID_H
#define OCEANBASE_COMMON_OB_TRACE_ID_H
namespace oceanbase
{
namespace common
{

#define TRACE_ID_FORMAT "Y%lX-%016lX"

/**request_type-table_id-partition_id-absolute_net_time-wait_time
 * such as:
 *   single-11-11-10-2
 *   shard-11-0-10-2
*/
#define RPC_REQUEST_INFO_FORMAT "%s-%ld-%ld-%ld-%ld"
struct ObCurTraceId
{
  class SeqGenerator
  {
  public:
    static uint64_t seq_generator_;
  };
  class TraceId
  {
  public:
    inline TraceId() { uval_[0] = 0; uval_[1] = 0; }
    inline bool is_invalid() { return id_.seq_ == 0 ? true : false; }
    inline void init(const ObAddr &ip_port)
    {
      // TODO: Consider whether to support IPv6
      id_.seq_ = ATOMIC_AAF(&(SeqGenerator::seq_generator_), 1);
      id_.ip_ = ip_port.get_ipv4();
      id_.reserved_ = 0;
      id_.port_ = static_cast<uint16_t>(ip_port.get_port());
    }
    inline int set(const uint64_t *uval)
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(uval)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        uval_[0] = uval[0];
        uval_[1] = uval[1];
      }
      return ret;
    }
    inline const uint64_t* get() const { return uval_; }
    inline void reset() { uval_[0] = 0; uval_[1] = 0; }
    inline int64_t to_string(char *buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      common::databuff_printf(buf, buf_len, pos, TRACE_ID_FORMAT, uval_[0], uval_[1]);
      return pos;
    }

    inline int64_t safe_to_string(char *buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      // [TODO] need use lnprintf
      int len = snprintf(buf, buf_len, TRACE_ID_FORMAT, uval_[0], uval_[1]);
      if (len < 0) {
        // nothing
      } else if (len < buf_len - pos) {
        pos += len;
      } else {
        pos = buf_len - 1;  //skip '\0' written by snprintf
      }
      return pos;
    }

  private:
    union
    {
      struct
      {
        uint32_t ip_;
        uint16_t port_;
        uint16_t reserved_;
        uint64_t seq_;
      } id_;
      uint64_t uval_[2];
    };
  };

  inline static void init(const ObAddr &ip_port)
  {
    TraceId *trace_id = get_trace_id();
    if (NULL != trace_id) {
      trace_id->init(ip_port);
    }
  }

  inline static void set(const uint64_t *uval)
  {
    TraceId *trace_id = get_trace_id();
    if (NULL != trace_id) {
      trace_id->set(uval);
    }
  }

  inline static void set(const uint64_t id, const uint64_t ipport = 0)
  {
    uint64_t uval[2] = {ipport, id};
    set(uval);
  }

  inline static void reset()
  {
    TraceId *trace_id = get_trace_id();
    if (NULL != trace_id) {
      trace_id->reset();
    }
  }
  inline static const uint64_t* get()
  {
    TraceId *trace_id = get_trace_id();
    return trace_id->get();
  }

  inline static TraceId *get_trace_id()
  {
    static __thread TraceId *TRACE_ID = NULL;
    if (OB_ISNULL(TRACE_ID)) {
      TRACE_ID = new (std::nothrow) TraceId();
    }
    return TRACE_ID;
  }
};

int32_t LogExtraHeaderCallback(char *buf, int32_t buf_size,
                               int level, const char *file,
                               int line, const char *function, pthread_t tid);
}// namespace common
}// namespace oceanbase


#endif
