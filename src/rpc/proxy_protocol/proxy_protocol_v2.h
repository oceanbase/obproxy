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

#ifndef _OB_PROXY_PROTOCOL_V2_PACKET_H_
#define _OB_PROXY_PROTOCOL_V2_PACKET_H_

#include "lib/net/ob_addr.h"
#include "obproxy/obutils/ob_proxy_buf.h"

namespace oceanbase
{
namespace proxy_protocol_v2
{

enum ANALYZE_STATE
{
  ANALYZE_HEADER,
  ANALYZE_BODY,
};

class ProxyProtocolV2
{
public:
  ProxyProtocolV2() : ver_cmd_(0), fam_(0), len_(0), src_addr_(), dst_addr_(), total_len_(0),
                vpc_info_(), is_finished_(false), analyze_state_(ANALYZE_HEADER) {}
  ~ProxyProtocolV2() {}
  int analyze_packet(char *buf, int64_t buf_len);
  inline int64_t get_total_len() const { return total_len_; }
  inline int64_t get_len() const { return len_; }
  int64_t to_string(char *buf, const int64_t buf_len) const;
  bool is_finished() const { return is_finished_; }
public:
  static const int64_t PROXY_PROTOCOL_V2_HEADER_LEN = 16;

  uint8_t sig_[12];
  uint8_t ver_cmd_;
  uint8_t fam_;
  uint16_t len_;
  common::ObAddr src_addr_;
  common::ObAddr dst_addr_;
  int64_t total_len_;
  obproxy::obutils::ObVariableLenBuffer<64> vpc_info_;
  bool is_finished_;
  ANALYZE_STATE analyze_state_;
};

} // end of proxy_protocol_v2
} // end of oceanbase

#endif // _OB_PROXY_PROTOCOL_V2_PACKET_H_
