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

#ifndef OBPROXY_OK_PACKET_H
#define OBPROXY_OK_PACKET_H
#include "lib/string/ob_string.h"
#include "rpc/obmysql/ob_mysql_util.h"

namespace oceanbase
{
namespace obproxy
{
namespace packet
{
class ObProxyOKPacket
{
public:
  // ref:http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
  // len(3) + seq(1) + header(1) + affected_rows(1~9) + last_insert_id(1~9)
  // + status_flags(2) + warnings(2) = 27 < 32
  static const int64_t OB_MAX_OK_PACKET_LENGTH = 32;
  static const uint8_t OB_HEADER_OK_TYPE = 0;

  static const int64_t OB_SEQ_POS = 3;
  static const int64_t OB_HEADER_POS = 4;
  static const int64_t OB_AFFECTED_ROWS_POS_ = 5;

  ObProxyOKPacket();
  ~ObProxyOKPacket();
  // set value directly
  void set_seq(const uint8_t seq);
  void set_status_flags(const uint16_t status_flags);
  void set_warnings_count(const uint16_t warnings_count);
  void set_affected_rows(const uint64_t affected_rows);
  void set_last_insert_id(const uint64_t last_insert_id);

  // rebuild buffer
  int get_packet_str(common::ObString &pkt_str);
private:
  int encode();
  // whether the buffer is valid(has been encoded)
  bool is_buffer_valid_;

  // packet content
  uint8_t seq_;
  uint16_t status_flags_;
  uint16_t warnings_count_;
  uint64_t affected_rows_;
  uint64_t last_insert_id_;

  // packet content offset
  int64_t status_flags_pos_;
  int64_t warnings_count_pos_;
  int64_t last_insert_id_pos_;

  int64_t pkt_len_;
  char pkt_buf_[OB_MAX_OK_PACKET_LENGTH];
};

inline void ObProxyOKPacket::set_seq(const uint8_t seq)
{
  if (seq == seq_) {
    // do nothing
  } else {
    // if buffer is valid, set to buffer directly
    if (is_buffer_valid_) {
      if (OB_LIKELY(OB_SEQ_POS < OB_MAX_OK_PACKET_LENGTH)) {
        int1store(pkt_buf_ + OB_SEQ_POS, seq);
      } else {
        is_buffer_valid_ = false;
      }
    }
    // set value
    seq_ = seq;
  }
}

inline void ObProxyOKPacket::set_status_flags(const uint16_t status_flags)
{
  if (status_flags == status_flags_) {
    // do nothing
  } else {
    // if buffer is valid, set to buffer directly
    if (is_buffer_valid_) {
      if (OB_LIKELY(status_flags_pos_ + 1 < OB_MAX_OK_PACKET_LENGTH)) {
        int2store(pkt_buf_ + status_flags_pos_, status_flags);
      } else {
        is_buffer_valid_ = false;
      }
    }
    // set value
    status_flags_ = status_flags;
  }
}

inline void ObProxyOKPacket::set_warnings_count(const uint16_t warnings_count)
{
  if (warnings_count == warnings_count_) {
    // do nothing
  } else {
    // if buffer is valid, set to buffer directly
    if (is_buffer_valid_) {
      if (OB_LIKELY(warnings_count_pos_ + 1 < OB_MAX_OK_PACKET_LENGTH)) {
        int2store(pkt_buf_ + warnings_count_pos_, warnings_count);
      } else {
        is_buffer_valid_ = false;
      }
    }
    // set value
    warnings_count_ = warnings_count;
  }
}

inline void ObProxyOKPacket::set_affected_rows(const uint64_t affected_rows)
{
  if (affected_rows == affected_rows_) {
    // do nothing
  } else {
    // if buffer is valid, set to buffer directly if length is not changed
    if (is_buffer_valid_) {
      uint64_t old_len = obmysql::ObMySQLUtil::get_number_store_len(affected_rows_);
      uint64_t new_len = obmysql::ObMySQLUtil::get_number_store_len(affected_rows);
      if (old_len == new_len) {
        int ret = common::OB_SUCCESS;
        int64_t pos = OB_AFFECTED_ROWS_POS_;
        if (OB_FAIL(obmysql::ObMySQLUtil::store_length(pkt_buf_, OB_MAX_OK_PACKET_LENGTH,
                                                       affected_rows, pos))) {
          is_buffer_valid_ = false;
        }
      } else {
        is_buffer_valid_ = false;
      }
    }
    // set value
    affected_rows_ = affected_rows;
  }
}

inline void ObProxyOKPacket::set_last_insert_id(const uint64_t last_insert_id)
{
  if (last_insert_id == last_insert_id_) {
    // do nothing
  } else {
    // if buffer is valid, set to buffer directly if length is not changed
    if (is_buffer_valid_) {
      uint64_t old_len = obmysql::ObMySQLUtil::get_number_store_len(last_insert_id_);
      uint64_t new_len = obmysql::ObMySQLUtil::get_number_store_len(last_insert_id);
      if (old_len == new_len) {
        int ret = common::OB_SUCCESS;
        int64_t pos = last_insert_id_pos_;
        if (OB_FAIL(obmysql::ObMySQLUtil::store_length(pkt_buf_, OB_MAX_OK_PACKET_LENGTH,
                                                       last_insert_id, pos))) {
          is_buffer_valid_ = false;
        }
      } else {
        is_buffer_valid_ = false;
      }
    }
    // set value
    last_insert_id_ = last_insert_id;
  }
}

inline int ObProxyOKPacket::get_packet_str(ObString &pkt_str)
{
  int ret = common::OB_SUCCESS;
  if (!is_buffer_valid_) {
    if (OB_FAIL(encode())) {
      // do not print here
    }
  }
  if (is_buffer_valid_) {
    pkt_str.assign_ptr(pkt_buf_, static_cast<int32_t>(pkt_len_));
  }
  return ret;
}

} // end of packet
} // end of obproxy
} // end of oceanbase
#endif // end of OBPROXY_OK_PACKET_H
