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
#ifndef OBPROXY_RPC_REQ_TRACE_H
#define OBPROXY_RPC_REQ_TRACE_H
#include <stdint.h>
#include "lib/string/ob_string.h"
#include "lib/profile/ob_trace_id_new.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

class ObRpcReqTraceId
{
public:
  ObRpcReqTraceId() {
    reset();
  }

  ObRpcReqTraceId(const ObRpcReqTraceId &other) {
    set_rpc_trace_id(other.trace_id_[1], other.trace_id_[0]);
    sub_index_id_ = other.sub_index_id_;
  }

  void reset() {
    memset(this, 0, sizeof(ObRpcReqTraceId));
  }

  void get_rpc_trace_id(uint64_t &id, uint64_t &ipport) const {
    id = rpc_trace_id_[1];
    ipport = rpc_trace_id_[0];
  }

  void set_rpc_trace_id(const uint64_t id, const uint64_t ipport) {
    rpc_trace_id_[0] = ipport;
    rpc_trace_id_[1] = id;

    common::ObCurNewTraceId::NewTraceId new_trace_id;
    int32_t len = 0;
    new_trace_id.set(id /*sequnence */, ipport /* ip & port */);
    len = (int32_t)new_trace_id.to_string(trace_id_buf_, NEW_TRACE_ID_BUF_LENTH);
    if (len > 0 && len < NEW_TRACE_ID_BUF_LENTH) {
      trace_id_.assign(trace_id_buf_, len);
    }
  }

  void set_rpc_sub_index_id(uint64_t index_id, uint32_t sub_req_retry_times)
  {
    if (sub_req_retry_times <=0 ) {
      sub_index_id_ = index_id;
    } else {
      uint64_t mask24 = 0xFFFFFF; // 24 位
      uint64_t origin_index_id = index_id & mask24;
      sub_index_id_ = (sub_req_retry_times << 24);
      sub_index_id_ |= origin_index_id;
    }
  }
  uint64_t get_rpc_sub_index_id() const { return sub_index_id_; }

  ObRpcReqTraceId &operator=(const ObRpcReqTraceId &other)
  {
    if (&other != this) {
      set_rpc_trace_id(other.rpc_trace_id_[1], other.rpc_trace_id_[0]);
      sub_index_id_ = other.sub_index_id_;
    }
    return *this;
  }

  int64_t to_string(char *buf, const int64_t buf_len) const {
    int64_t pos = 0;
    J_OBJ_START();
    J_KV(K_(trace_id),
         K_(sub_index_id));
    J_OBJ_END();
    return pos;
  }

private:
  uint64_t sub_index_id_;
  uint64_t rpc_trace_id_[2];
  common::ObString trace_id_;
  char trace_id_buf_[common::OB_MAX_OBPROXY_TRACE_ID_LENGTH]; //max set to 128, for new trace id's length is 80
};
}
}
}

#endif /* OBPROXY_RPC_REQ_TRACE_H */