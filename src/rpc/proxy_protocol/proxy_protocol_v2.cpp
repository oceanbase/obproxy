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

#include "rpc/proxy_protocol/proxy_protocol_v2.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace proxy_protocol_v2
{

int ProxyProtocolV2::analyze_packet(char *buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(buf), K(buf_len), K(ret));
  } else if (is_finished_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("analyze twice unexpected,", K(ret));
  } else {
    if (ANALYZE_HEADER == analyze_state_ && buf_len >= PROXY_PROTOCOL_V2_HEADER_LEN) {
      MEMCPY(sig_, buf, 12);
      ver_cmd_ = buf[12];
      fam_ = buf[13];
      len_ = ntohs(*(uint16_t*)(&buf[14]));
      total_len_ = len_ + PROXY_PROTOCOL_V2_HEADER_LEN;
      analyze_state_ = ANALYZE_BODY;
    }

    if (ANALYZE_BODY == analyze_state_ && buf_len >= total_len_) {
      int64_t end_pos = 0;
      if (0x11 == fam_) {
        uint32_t src_addr = ntohl(*(uint32_t*)(&buf[16]));
        uint32_t dst_addr = ntohl(*(uint32_t*)(&buf[20]));
        uint16_t src_port = ntohs(*(uint16_t*)(&buf[24]));
        uint16_t dst_port = ntohs(*(uint16_t*)(&buf[26]));
        src_addr_.set_ipv4_addr(src_addr, src_port);
        dst_addr_.set_ipv4_addr(dst_addr, dst_port);
        end_pos = 27;
      } else if (0x21 == fam_) {
        uint64_t src_high_addr = *(uint64_t*)(&buf[16]);
        uint64_t src_low_addr = *(uint64_t*)(&buf[24]);
        uint64_t dst_high_addr = *(uint64_t*)(&buf[32]);
        uint64_t dst_low_addr = *(uint64_t*)(&buf[40]);
        uint16_t src_port = ntohs(*(uint16_t*)(&buf[48]));
        uint16_t dst_port = ntohs(*(uint16_t*)(&buf[50]));
        src_addr_.set_ipv6_addr(src_high_addr, src_low_addr, src_port);
        dst_addr_.set_ipv6_addr(dst_high_addr, dst_low_addr, dst_port);
        end_pos = 51;
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WDIAG("not support situation", K(fam_), K(ret));
      }

      if (OB_SUCC(ret)) {
        end_pos++;
        while (end_pos < total_len_) {
          uint8_t type = *(uint8_t*)(&buf[end_pos]);
          uint16_t length =  ntohs(*(uint16_t*)(&buf[end_pos + 1]));
          // 0xea 的 type 是 aws 使用的，参考 https://docs.aws.amazon.com/elasticloadbalancing/latest/network/load-balancer-target-groups.html#proxy-protocol
          if (type == 0xea) {
            vpc_info_.reset();
            if (OB_FAIL(vpc_info_.init(length - 1))) {
              LOG_WDIAG("vpc info init failed", K(length - 1), K(ret));
            } else if (OB_FAIL(vpc_info_.write(&buf[end_pos + 4], length - 1))) {
              LOG_WDIAG("vpc info write failed", K(ret));
            }
            break;
          } else {
            end_pos += 3 + length;
          }
        }
      }

      is_finished_ = true;
    }
  }

  return ret;
}

int64_t ProxyProtocolV2::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(ver_cmd), K_(fam), K_(total_len), K_(src_addr), K_(dst_addr), K_(vpc_info));
  J_OBJ_END();
  return pos;
}

} // end of proxy_
} // end of oceanbase
