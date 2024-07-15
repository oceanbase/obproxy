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

#define USING_LOG_PREFIX RPC_OBRPC
#include "rpc/obrpc/ob_rpc_packet.h"
#include "lib/utility/utility.h"
#include "lib/ob_define.h"

using namespace oceanbase::common::serialization;
using namespace oceanbase::common;

namespace oceanbase
{

namespace obrpc
{

DEFINE_SERIALIZE(ObRpcPacketHeader)
{
  int ret = OB_SUCCESS;
  //TODO RPC force set dst_cluster_id_ = -1;
  if (buf_len - pos >= get_encoded_size()) {
    if (OB_FAIL(encode_i32(buf, buf_len, pos, pcode_))) {
      LOG_WDIAG("Encode error", K(ret));
    } else if (OB_FAIL(encode_i8(buf, buf_len, pos, static_cast<int8_t>(get_encoded_size())))) {
      LOG_WDIAG("Encode error", K(ret));
    } else if (OB_FAIL(encode_i8(buf, buf_len, pos, priority_))) {
      LOG_WDIAG("Encode error", K(ret));
    } else if (OB_FAIL(encode_i16(buf, buf_len, pos, flags_))) {
      LOG_WDIAG("Encode error", K(ret));
    } else if (OB_FAIL(encode_i64(buf, buf_len, pos, checksum_))) {
      LOG_WDIAG("Encode error", K(ret));
    } else if (OB_FAIL(encode_i64(buf, buf_len, pos, tenant_id_))) {
      LOG_WDIAG("Encode error", K(ret));
    } else if (OB_FAIL(encode_i64(buf, buf_len, pos, priv_tenant_id_))) {
      LOG_WDIAG("Encode error", K(ret));
    } else if (OB_FAIL(encode_i64(buf, buf_len, pos, session_id_))) {
      LOG_WDIAG("Encode error", K(ret));
    } else if (OB_FAIL(encode_i64(buf, buf_len, pos, trace_id_[0]))) {
      LOG_WDIAG("Encode error", K(ret));
    } else if (OB_FAIL(encode_i64(buf, buf_len, pos, trace_id_[1]))) {
      LOG_WDIAG("Encode error", K(ret));
    } else if (OB_FAIL(encode_i64(buf, buf_len, pos, timeout_))) {
      LOG_WDIAG("Encode error", K(ret));
    } else if (OB_FAIL(encode_i64(buf, buf_len, pos, timestamp_))) {
      LOG_WDIAG("Encode error", K(ret));
    } else if (OB_FAIL(cost_time_.serialize(buf, buf_len, pos))) {
      LOG_WDIAG("Encode error", K(ret));
    } else if (OB_FAIL(encode_i64(buf, buf_len, pos, dst_cluster_id_))) {
      LOG_WDIAG("Encode error", K(ret));
    } else if (OB_FAIL(encode_i32(buf, buf_len, pos, compressor_type_))) {
      LOG_WDIAG("Encode error", K(ret));
    } else if (OB_FAIL(encode_i32(buf, buf_len, pos, original_len_))) {
      LOG_WDIAG("Encode error", K(ret));
    } else {
      //do nothing
    }
  } else {
    ret = OB_BUF_NOT_ENOUGH;
  }

  return ret;
}

DEFINE_DESERIALIZE(ObRpcPacketHeader)
{
  int ret = OB_SUCCESS;
  if (data_len - pos >= HEADER_SIZE) {
    if (OB_FAIL(decode_i32(buf, data_len, pos, reinterpret_cast<int32_t*>(&pcode_)))) {
      LOG_WDIAG("Decode error", K(ret));
    } else if (OB_FAIL(decode_i8(buf, data_len, pos, reinterpret_cast<int8_t*>(&hlen_)))) {
      LOG_WDIAG("Decode error", K(ret));
    } else if (OB_FAIL(decode_i8(buf, data_len, pos, reinterpret_cast<int8_t*>(&priority_)))) {
      LOG_WDIAG("Decode error", K(ret));
    } else if (OB_FAIL(decode_i16(buf, data_len, pos, reinterpret_cast<int16_t*>(&flags_)))) {
      LOG_WDIAG("Decode error", K(ret));
    } else if (OB_FAIL(decode_i64(buf, data_len, pos, reinterpret_cast<int64_t*>(&checksum_)))) {
      LOG_WDIAG("Decode error", K(ret));
    } else if (OB_FAIL(decode_i64(buf, data_len, pos, reinterpret_cast<int64_t*>(&tenant_id_)))) {
      LOG_WDIAG("Decode error", K(ret));
    } else if (OB_FAIL(decode_i64(buf, data_len, pos, reinterpret_cast<int64_t*>(&priv_tenant_id_)))) {
      LOG_WDIAG("Decode error", K(ret));
    } else if (OB_FAIL(decode_i64(buf, data_len, pos, reinterpret_cast<int64_t*>(&session_id_)))) {
      LOG_WDIAG("Decode error", K(ret));
    } else if (OB_FAIL(decode_i64(buf, data_len, pos, reinterpret_cast<int64_t*>(&trace_id_[0])))) {
      LOG_WDIAG("Decode error", K(ret));
    } else if (OB_FAIL(decode_i64(buf, data_len, pos, reinterpret_cast<int64_t*>(&trace_id_[1])))) {
      LOG_WDIAG("Decode error", K(ret));
    } else if (OB_FAIL(decode_i64(buf, data_len, pos, reinterpret_cast<int64_t*>(&timeout_)))) {
      LOG_WDIAG("Decode error", K(ret));
    } else if (OB_FAIL(decode_i64(buf, data_len, pos, reinterpret_cast<int64_t*>(&timestamp_)))) {
      LOG_WDIAG("Decode error", K(ret));
    }
  } else {
    ret = OB_INVALID_DATA;
  }

  if (OB_SUCC(ret)) {
    if (hlen_ >= get_encoded_size()) {
      // new header
      if (data_len - pos >= cost_time_.get_encoded_size()) {
        if (OB_FAIL(cost_time_.deserialize(buf, data_len, pos))) {
          LOG_WDIAG("Decode error", K(ret));
        } else if (hlen_ > pos && OB_FAIL(decode_i64(buf, hlen_, pos, &dst_cluster_id_))) {
          LOG_WDIAG("Decode error", K(ret), K(hlen_), K(pos));
        } else if (hlen_ > pos &&
                   OB_FAIL(decode_i32(buf, hlen_, pos,
                                      reinterpret_cast<int32_t*>(&compressor_type_)))) {
          LOG_WDIAG("Decode error", K(ret), K_(hlen), K(pos));
        } else if (hlen_ > pos && OB_FAIL(decode_i32(buf, hlen_, pos, &original_len_))) {
          LOG_WDIAG("Decode error", K(ret), K_(hlen), K(pos));
        }
      } else {
        ret = OB_INVALID_DATA;
      }
    } else if (hlen_ == HEADER_SIZE) {
      // old header
    } else {
      ret = OB_INVALID_DATA;
    }
  }

  return ret;
}
} // ends of namespace obrpc
} // ends of namespace oceanbase
