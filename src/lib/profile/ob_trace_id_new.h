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
#ifndef OCEANBASE_COMMON_OB_TRACE_ID_NEW_H
#define OCEANBASE_COMMON_OB_TRACE_ID_NEW_H
namespace oceanbase
{
namespace common
{
#define NEW_TRACE_ID_FORMAT "Y%lX-%016lX"
#define NEW_TRACE_ID_FORMAT_V2 "Y%lX-%016lX-%lx-%lx"
#define NEW_TRACE_ID_FORMAT_PARAM(x) x[0], x[1], x[2], x[3]
#define NEW_TRACE_ID_BUF_LENTH 80
struct ObCurNewTraceId
{
  class SeqGenerator
  {
  public:
    static uint64_t seq_generator_;
  };
  class NewTraceId
  {
  public:
    inline NewTraceId() { uval_[0] = 0; uval_[1] = 0; uval_[2] = 0; uval_[3] = 0; }
    inline bool is_invalid() { return id_.seq_ == 0 ? true : false; }
    inline void init(const ObAddr &ip_port)
    {
      id_.seq_ = ATOMIC_AAF(&(SeqGenerator::seq_generator_), 1);
      id_.is_user_request_ = 0;
      id_.is_ipv6_ = ip_port.get_ipv6_high() != 0 || ip_port.get_ipv6_low() != 0; // not have ip_port.using_ipv6();
      id_.reserved_ = 0;
      id_.port_ = static_cast<uint16_t>(ip_port.get_port());
      if (id_.is_ipv6_) {
        id_.ipv6_[0] = ip_port.get_ipv6_low();
        id_.ipv6_[1] = ip_port.get_ipv6_high();
      } else {
        id_.ip_ = ip_port.get_ipv4();
      }
    }
    void check_ipv6_valid() {
      if (id_.is_ipv6_ && id_.ipv6_[0] == 0) {
        fprintf(stderr, "ERROR trace id lost ipv6 addr: %s\n", lbt());
      }
    }
    inline int set(const uint64_t *uval)
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(uval)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        uval_[0] = uval[0];
        uval_[1] = uval[1];
        uval_[2] = uval[2];
        uval_[3] = uval[3];
        check_ipv6_valid();
      }
      return ret;
    }
    inline void set(const uint64_t id, const uint64_t ipport = 0)
    {
      uint64_t uval[4] = {ipport, id, 0, 0}; //not used
      set(uval);
    }
    inline void set(const NewTraceId &new_trace_id)
    {
      uval_[0] = new_trace_id.uval_[0];
      uval_[1] = new_trace_id.uval_[1];
      uval_[2] = new_trace_id.uval_[2];
      uval_[3] = new_trace_id.uval_[3];
      check_ipv6_valid();
    }
    inline const uint64_t* get() const { return uval_; }
    inline uint64_t get_seq() const { return id_.seq_; }
    // inline const ObAddr get_addr() const
    // {
    //   ObAddr addr(id_.ip_, id_.port_);
    //   if (id_.is_ipv6_) {
    //     addr.set_ipv6_addr(id_.ipv6_[1], id_.ipv6_[0], id_.port_);
    //   }
    //   return addr;
    // }
    inline void reset() { uval_[0] = 0; uval_[1] = 0; uval_[2] = 0; uval_[3] = 0; }

    inline int64_t to_string(char *buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      common::databuff_printf(buf, buf_len, pos, NEW_TRACE_ID_FORMAT_V2, uval_[0], uval_[1], uval_[2], uval_[3]);

      return pos;
    }
    int parse_from_buf(const char* buf) {
      int ret = OB_SUCCESS;
      if (4 != sscanf(buf, NEW_TRACE_ID_FORMAT_V2, &uval_[0], &uval_[1], &uval_[2], &uval_[3])) {
        ret = OB_INVALID_ARGUMENT;
      }
      return ret;
    }
    inline bool equals(const NewTraceId &trace_id) const
    {
      return uval_[0] == trace_id.uval_[0] && uval_[1] == trace_id.uval_[1]
          && uval_[2] == trace_id.uval_[2] && uval_[3] == trace_id.uval_[3];
    }
    inline void mark_user_request()
    {
      id_.is_user_request_ = 1;
    }

    inline bool is_user_request() const
    {
      return 0 != id_.is_user_request_;
    }
  private:
    union
    {
      struct
      {
        uint32_t ip_: 32;
        uint16_t port_: 16;
        uint8_t is_user_request_: 1;
        uint8_t is_ipv6_:1;
        uint16_t reserved_: 14;
        uint64_t seq_: 64;
        uint64_t ipv6_[2];
      } id_;
      uint64_t uval_[4];
    };
  };

  inline static void init(const ObAddr &ip_port)
  {
    NewTraceId *trace_id = get_trace_id();
    if (NULL != trace_id) {
      trace_id->init(ip_port);
    }
  }

  inline static void set(const uint64_t *uval)
  {
    NewTraceId *trace_id = get_trace_id();
    if (NULL != trace_id) {
      trace_id->set(uval);
    }
  }

  inline static void set(const uint64_t id, const uint64_t ipport = 0)
  {
    uint64_t uval[4] = {ipport, id, 0, 0}; //not used
    set(uval);
  }

  inline static void reset()
  {
    NewTraceId *trace_id = get_trace_id();
    if (NULL != trace_id) {
      trace_id->reset();
    }
  }
  inline static const uint64_t* get()
  {
    NewTraceId *trace_id = get_trace_id();
    return trace_id->get();
  }
  inline static uint64_t get_seq()
  {
    NewTraceId *trace_id = get_trace_id();
    return trace_id->get_seq();
  }

  // inline static const ObAddr get_addr()
  // {
  //   NewTraceId *trace_id = get_trace_id();
  //   return trace_id->get_addr();
  // }

  inline static NewTraceId *get_trace_id()
  {
    static __thread NewTraceId *TRACE_ID = NULL;
    if (OB_ISNULL(TRACE_ID)) {
      TRACE_ID = new (std::nothrow) NewTraceId();
    }
    return TRACE_ID;
  }

  inline static void mark_user_request()
  {
    get_trace_id()->mark_user_request();
  }

  inline static bool is_user_request()
  {
    return get_trace_id()->is_user_request();
  }
};

// int32_t LogExtraHeaderCallback(char *buf, int32_t buf_size,
//                                int level, const char *file,
//                                int line, const char *function, pthread_t tid);
}// namespace common
}// namespace oceanbase


#endif
