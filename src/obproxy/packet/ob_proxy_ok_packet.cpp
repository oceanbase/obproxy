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
#include "ob_proxy_ok_packet.h"

using namespace oceanbase::obmysql;

namespace oceanbase
{
namespace obproxy
{
namespace packet
{
ObProxyOKPacket::ObProxyOKPacket()
    : is_buffer_valid_(false), seq_(1),
      status_flags_(0x22), warnings_count_(0), affected_rows_(0), last_insert_id_(0),
      status_flags_pos_(0), warnings_count_pos_(0), last_insert_id_pos_(0), pkt_len_(0)
{
  // do not reset pkt_buf as we have set pkt_len to 0
}

ObProxyOKPacket::~ObProxyOKPacket()
{
  is_buffer_valid_ = false;
  pkt_len_ = 0;
}

int ObProxyOKPacket::encode()
{
  int ret = OB_SUCCESS;
  int64_t pos = OB_SEQ_POS;
  if (OB_FAIL(ObMySQLUtil::store_int1(pkt_buf_, OB_MAX_OK_PACKET_LENGTH, seq_, pos))) {
    LOG_WARN("fail to store seq", K_(seq), K(pos), K(ret));
  } else if (OB_FAIL(ObMySQLUtil::store_int1(pkt_buf_, OB_MAX_OK_PACKET_LENGTH,
                                             OB_HEADER_OK_TYPE, pos))) {
    LOG_WARN("fail to store header", K(pos), K(ret));
  } else if (OB_FAIL(ObMySQLUtil::store_length(pkt_buf_, OB_MAX_OK_PACKET_LENGTH,
                                               affected_rows_, pos))) {
    LOG_WARN("fail to store affected_rows", K_(affected_rows), K(pos), K(ret));
  } else if (FALSE_IT(last_insert_id_pos_ = pos)) {
    // last_insert_id is behind affected_rows
  } else if (OB_FAIL(ObMySQLUtil::store_length(pkt_buf_, OB_MAX_OK_PACKET_LENGTH,
                                               last_insert_id_, pos))) {
    LOG_WARN("fail to store", K_(last_insert_id), K(pos), K(ret));
  } else if (FALSE_IT(status_flags_pos_ = pos)) {
    // status_flag is behind last_insert_id
  } else if (OB_FAIL(ObMySQLUtil::store_int2(pkt_buf_, OB_MAX_OK_PACKET_LENGTH,
                                             status_flags_, pos))) {
    LOG_WARN("fail to store status_flags", K_(status_flags), K(pos), K(ret));
  } else if (FALSE_IT(warnings_count_pos_ = pos)) {
    // warnings_count is behind status_flag
  } else if (OB_FAIL(ObMySQLUtil::store_int2(pkt_buf_, OB_MAX_OK_PACKET_LENGTH,
                                             warnings_count_, pos))) {
    LOG_WARN("fail to store warnings_count", K_(warnings_count), K(pos), K(ret));
  } else if (FALSE_IT(pkt_len_ = pos)) {
    // last content, store pkt_len
  } else if (FALSE_IT(pos = 0)) {
    // reset pos to 0, then store content length
  } else if (OB_FAIL(ObMySQLUtil::store_int3(pkt_buf_, OB_MAX_OK_PACKET_LENGTH,
                                             static_cast<int32_t>(pkt_len_ - OB_HEADER_POS),
                                             pos))) {
    LOG_WARN("fail to store length", "length", pkt_len_ - OB_HEADER_POS, K(pos), K(ret));
  } else {
    // do nothing
  }

  if (OB_SUCC(ret)) {
    is_buffer_valid_ = true;
  } else {
    is_buffer_valid_ = false;
  }

  return ret;
}

} // end of packet
} // end of obproxy
} // end of oceanbase
