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
#define TRACE_ID_FORMAT "Y%lX-%lX"
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
