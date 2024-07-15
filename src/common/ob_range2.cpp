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

#include "common/ob_range2.h"
#include "lib/utility/utility.h"


namespace oceanbase
{
namespace common
{

int64_t ObNewRange::to_plain_string(char *buffer, const int64_t length) const
{
  int64_t pos = 0;
  if (NULL == buffer || 0 >= length) {
  } else {
    if (border_flag_.inclusive_start()) {
      databuff_printf(buffer, length, pos, "[");
    } else {
      databuff_printf(buffer, length, pos, "(");
    }
    pos += start_key_.to_plain_string(buffer + pos, length - pos);
    databuff_printf(buffer, length, pos, " ; ");
    pos += end_key_.to_plain_string(buffer + pos, length - pos);
    if (border_flag_.inclusive_end()) {
      databuff_printf(buffer, length, pos, "]");
    } else {
      databuff_printf(buffer, length, pos, ")");
    }
    if (is_whole_range()) {
      databuff_printf(buffer, length, pos, "always true");
    } else if (empty()) {
      databuff_printf(buffer, length, pos, "always false");
    }
  }
  return pos;
}

int64_t ObNewRange::to_simple_string(char *buffer, const int64_t length) const
{
  int64_t pos = 0;
  if (NULL == buffer || 0 >= length) {
  } else {
    if (table_id_ != OB_INVALID_ID) {
      databuff_printf(buffer, length, pos, "table_id:%ld,", table_id_);
    } else {
      databuff_printf(buffer, length, pos, "table_id:null,");
    }
    if (border_flag_.inclusive_start()) {
      databuff_printf(buffer, length, pos, "[");
    } else {
      databuff_printf(buffer, length, pos, "(");
    }
    pos += start_key_.to_string(buffer + pos, length - pos);
    databuff_printf(buffer, length, pos, ";");
    pos += end_key_.to_string(buffer + pos, length - pos);
    if (border_flag_.inclusive_end()) {
      databuff_printf(buffer, length, pos, "]");
    } else {
      databuff_printf(buffer, length, pos, ")");
    }
  }
  return pos;
}

int64_t ObNewRange::to_string(char *buffer, const int64_t length) const
{
  int64_t pos = 0;
  if (NULL == buffer || 0 >= length) {
  } else {
    databuff_printf(buffer, length, pos, "{\"range\":\"");
    pos += to_simple_string(buffer + pos, length - pos);
    databuff_printf(buffer, length, pos, "\"}");
  }
  return pos;
}

uint64_t ObNewRange::hash() const
{
  uint64_t hash_val = 0;
  int8_t flag = border_flag_.get_data();

  /**
   * if it's max value, the border flag maybe is right close,
   * ignore it.
   */
  if (end_key_.is_max_row()) {
    flag = ObBorderFlag::MAX_VALUE;
  }
  hash_val = murmurhash(&table_id_, sizeof(uint64_t), 0);
  hash_val = murmurhash(&flag, sizeof(int8_t), hash_val);
  if (NULL != start_key_.ptr()
      && start_key_.length() > 0) {
    hash_val = start_key_.murmurhash(hash_val);
  }
  if (NULL != end_key_.ptr()
      && end_key_.length() > 0) {
    hash_val = end_key_.murmurhash(hash_val);
  }

  return hash_val;
};

int ObNewRange::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "invalid arguments.", KP(buf), K(buf_len), K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(
      buf, buf_len, pos, static_cast<int64_t>(table_id_)))) {
    COMMON_LOG(WDIAG, "serialize table_id failed.",
               KP(buf), K(buf_len), K(pos), K_(table_id), K(ret));
  } else if (OB_FAIL(serialization::encode_i8(
      buf, buf_len, pos, border_flag_.get_data()))) {
    COMMON_LOG(WDIAG, "serialize border_flag failed.",
               KP(buf), K(buf_len), K(pos), K_(border_flag), K(ret));
  } else if (OB_FAIL(start_key_.serialize(buf, buf_len, pos))) {
    COMMON_LOG(WDIAG, "serialize start_key failed.",
               KP(buf), K(buf_len), K(pos), K_(start_key), K(ret));
  } else if (OB_FAIL(end_key_.serialize(buf, buf_len, pos))) {
    COMMON_LOG(WDIAG, "serialize end_key failed.",
               KP(buf), K(buf_len), K(pos), K_(end_key), K(ret));
  }
  return ret;
}

int ObNewRange::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int8_t flag = 0;
  if (NULL == buf || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "invalid arguments.", KP(buf), K(data_len), K(ret));
  } else if (NULL == start_key_.get_obj_ptr() || NULL == end_key_.get_obj_ptr()) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WDIAG, "start_key or end_key ptr is NULL",
               K_(start_key), K_(end_key), K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(
      buf, data_len, pos, reinterpret_cast<int64_t *>(&table_id_)))) {
    COMMON_LOG(WDIAG, "deserialize table_id failed.",
               KP(buf), K(data_len), K(pos), K_(table_id), K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf, data_len, pos, &flag))) {
    COMMON_LOG(WDIAG, "deserialize flag failed.",
               KP(buf), K(data_len), K(pos), K(flag), K(ret));
  } else if (OB_FAIL(start_key_.deserialize(buf, data_len, pos))) {
    COMMON_LOG(WDIAG, "deserialize start_key failed.",
               KP(buf), K(data_len), K(pos), K_(start_key), K(ret));
  } else if (OB_FAIL(end_key_.deserialize(buf, data_len, pos))) {
    COMMON_LOG(WDIAG, "deserialize end_key failed.",
               KP(buf), K(data_len), K(pos), K_(end_key), K(ret));
  } else {
    border_flag_.set_data(flag);
  }
  return ret;
}

int64_t ObNewRange::get_serialize_size(void) const
{
  int64_t total_size = 0;

  total_size += serialization::encoded_length_vi64(table_id_);
  total_size += serialization::encoded_length_i8(border_flag_.get_data());

  total_size += start_key_.get_serialize_size();
  total_size += end_key_.get_serialize_size();

  return total_size;
}

int ObNewRange::serialize_v4(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "invalid arguments.", KP(buf), K(buf_len), K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(
      buf, buf_len, pos, static_cast<int64_t>(table_id_)))) {
    COMMON_LOG(WDIAG, "serialize table_id failed.",
               KP(buf), K(buf_len), K(pos), K_(table_id), K(ret));
  } else if (OB_FAIL(serialization::encode_i8(
      buf, buf_len, pos, border_flag_.get_data()))) {
    COMMON_LOG(WDIAG, "serialize border_flag failed.",
               KP(buf), K(buf_len), K(pos), K_(border_flag), K(ret));
  } else if (OB_FAIL(start_key_.serialize(buf, buf_len, pos))) {
    COMMON_LOG(WDIAG, "serialize start_key failed.",
               KP(buf), K(buf_len), K(pos), K_(start_key), K(ret));
  } else if (OB_FAIL(end_key_.serialize(buf, buf_len, pos))) {
    COMMON_LOG(WDIAG, "serialize end_key failed.",
               KP(buf), K(buf_len), K(pos), K_(end_key), K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, flag_))) {
    COMMON_LOG(WDIAG, "serialize flag failed.",
               KP(buf), K(buf_len), K(pos), K_(flag), K(ret));
  }
  return ret;
}

int ObNewRange::deserialize_v4(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int8_t flag = 0;
  if (NULL == buf || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "invalid arguments.", KP(buf), K(data_len), K(ret));
  } else if (NULL == start_key_.get_obj_ptr() || NULL == end_key_.get_obj_ptr()) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WDIAG, "start_key or end_key ptr is NULL",
               K_(start_key), K_(end_key), K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(
      buf, data_len, pos, reinterpret_cast<int64_t *>(&table_id_)))) {
    COMMON_LOG(WDIAG, "deserialize table_id failed.",
               KP(buf), K(data_len), K(pos), K_(table_id), K(ret));
  } else if (OB_FAIL(serialization::decode_i8(buf, data_len, pos, &flag))) {
    COMMON_LOG(WDIAG, "deserialize flag failed.",
               KP(buf), K(data_len), K(pos), K(flag), K(ret));
  } else if (OB_FAIL(start_key_.deserialize(buf, data_len, pos))) {
    COMMON_LOG(WDIAG, "deserialize start_key failed.",
               KP(buf), K(data_len), K(pos), K_(start_key), K(ret));
  } else if (OB_FAIL(end_key_.deserialize(buf, data_len, pos))) {
    COMMON_LOG(WDIAG, "deserialize end_key failed.",
               KP(buf), K(data_len), K(pos), K_(end_key), K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &flag_))) {
    COMMON_LOG(WDIAG, "deserialize flag failed.",
               KP(buf), K(data_len), K(pos), K_(flag), K(ret));
  } else {
    border_flag_.set_data(flag);
  }
  return ret;
}

int64_t ObNewRange::get_serialize_size_v4(void) const
{
  int64_t total_size = 0;

  total_size += serialization::encoded_length_vi64(table_id_);
  total_size += serialization::encoded_length_i8(border_flag_.get_data());

  total_size += start_key_.get_serialize_size();
  total_size += end_key_.get_serialize_size();
  total_size += serialization::encoded_length_vi64(flag_);

  return total_size;
}

}
}
