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

#include "common/ob_range.h"
#include "lib/tbsys.h"
#include <string.h>
#include "lib/utility/utility.h"

namespace oceanbase
{
namespace common
{
ObVersion ObVersion::MIN_VERSION(0);
ObVersion ObVersion::MAX_VERSION(INT32_MAX, INT16_MAX);
OB_SERIALIZE_MEMBER(ObVersion,
                    version_);
int ObVersion::fixed_length_encode(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, new_pos, version_))) {
    CLOG_LOG(WARN, "serialize error", "buf", buf, "buf_len", buf_len, "pos", pos, "version_", version_);
  } else {
    pos = new_pos;
  }
  return ret;
}
int ObVersion::fixed_length_decode(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, new_pos, &version_))) {
    CLOG_LOG(WARN, "deserialize error", "buf", buf, "data_len", data_len, "pos", new_pos);
  } else {
    pos = new_pos;
  }
  return ret;
}

int64_t ObVersion::get_fixed_length_encoded_size() const
{
  int64_t size = 0;
  size += serialization::encoded_length_i64(version_);
  return size;
}

#if 0
// --------------------------------------------------------
// class ObRange implements
// --------------------------------------------------------

bool ObRange::check(void) const
{
  bool ret = true;
  if ((start_key_ > end_key_) && (!border_flag_.is_max_value()) && (!border_flag_.is_min_value())) {
    _OB_LOG(WARN, "%s", "check start key gt than end key");
    ret = false;
  } else if (start_key_ == end_key_) {
    if (!border_flag_.is_min_value() && !border_flag_.is_max_value()) {
      if (start_key_.length() == 0 || !border_flag_.inclusive_start() || !border_flag_.inclusive_end()) {
        _OB_LOG(WARN, "check border flag or length failed:length[%d], flag[%u]",
                start_key_.length(), border_flag_.get_data());
        ret = false;
      }
    }
  }
  return ret;
}

bool ObRange::equal(const ObRange &r) const
{
  return (compare_with_startkey(r) == 0) && (compare_with_endkey(r) == 0);
}

bool ObRange::intersect(const ObRange &r) const
{
  // suppose range.start_key_ <= range.end_key_
  if (table_id_ != r.table_id_) { return false; }
  if (empty() || r.empty()) { return false; }
  if (is_whole_range() || r.is_whole_range()) { return true; }

  ObString lkey, rkey;
  bool ret = false;
  int8_t include_lborder = 0;
  int8_t include_rborder = 0;
  bool min_value = false;
  int cmp = compare_with_endkey(r);
  if (cmp < 0) {
    lkey = end_key_;
    rkey = r.start_key_;
    include_lborder = (border_flag_.inclusive_end());
    include_rborder = (r.border_flag_.inclusive_start());
    min_value = (r.border_flag_.is_min_value());
  } else if (cmp > 0) {
    lkey = r.end_key_;
    rkey = start_key_;
    include_lborder = (r.border_flag_.inclusive_end());
    include_rborder = (border_flag_.inclusive_start());
    min_value = (border_flag_.is_min_value());
  } else {
    ret = true;
  }

  if (cmp != 0) {
    if (min_value) { ret = true; }
    else if (lkey < rkey) { ret = false; }
    else if (lkey > rkey) { ret = true; }
    else { ret = (include_lborder != 0 && include_rborder != 0); }
  }

  return ret;
}

void ObRange::dump() const
{
  // TODO, used in ObString is a c string end with '\0'
  // just for test.
  const int32_t MAX_LEN = OB_MAX_ROW_KEY_LENGTH;
  char sbuf[MAX_LEN];
  char ebuf[MAX_LEN];
  MEMCPY(sbuf, start_key_.ptr(), start_key_.length());
  sbuf[start_key_.length()] = 0;
  MEMCPY(ebuf, end_key_.ptr(), end_key_.length());
  ebuf[end_key_.length()] = 0;
  _OB_LOG(DEBUG,
          "table:%ld, border flag:%d, start key:%s(%d), end key:%s(%d)\n",
          table_id_, border_flag_.get_data(), sbuf,  start_key_.length(), ebuf, end_key_.length());
}

void ObRange::hex_dump(const int32_t log_level) const
{
  _OB_NUM_LEVEL_LOG(log_level,
                    "table:%ld, border flag:%d\n", table_id_, border_flag_.get_data());
  _OB_NUM_LEVEL_LOG(log_level,
                    "dump start key with hex format, length(%d)", start_key_.length());
  common::hex_dump(start_key_.ptr(), start_key_.length(), true, log_level);
  _OB_NUM_LEVEL_LOG(log_level,
                    "dump end   key with hex format, length(%d)", end_key_.length());
  common::hex_dump(end_key_.ptr(), end_key_.length(), true, log_level);
}

int ObRange::to_string(char *buffer, const int32_t length) const
{
  int ret = OB_SUCCESS;
  if (NULL == buffer || length <= 0) {
    ret = OB_INVALID_ARGUMENT;
  }

  int32_t key_length = 0;
  int32_t pos = 0;
  int32_t remain_length = 0;
  int32_t byte_len = 0;
  int32_t max_key_length = 0;

  // calc print key length;
  if (border_flag_.is_min_value() && border_flag_.is_max_value()) {
    key_length += 6; // MIN, MAX
  } else if (border_flag_.is_min_value()) {
    key_length += 3 + 2 * end_key_.length(); // MIN, XXX
  } else if (border_flag_.is_max_value()) {
    key_length += 3 + 2 * start_key_.length(); // XXX, MAX
  } else {
    key_length += 2 * end_key_.length() + 2 * start_key_.length(); // XXX, MAX
  }

  key_length += 3; // (,)

  // print table:xxx
  if (OB_SUCC(ret)) {
    pos = snprintf(buffer, length, "table:%ld, ", table_id_);
    if (pos >= length) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      remain_length = length - pos;
    }
  }

  if (OB_SUCC(ret)) {

    const char *lb = 0;
    if (border_flag_.inclusive_start()) { lb = "["; }
    else { lb = "("; }
    if (border_flag_.is_min_value()) { lb = "(MIN"; }

    byte_len = snprintf(buffer + pos, remain_length, "%s", lb);
    pos += byte_len;
    remain_length = length - pos;

    // add start_key_
    if (!border_flag_.is_min_value()) {
      // buffer not enough, store part of start key, start= "key"|".."|",]\0"
      max_key_length = (remain_length < key_length) ? ((remain_length - 3) / 2 - 2) / 2 :
                       start_key_.length();
      byte_len = hex_to_str(start_key_.ptr(), max_key_length, buffer + pos, remain_length);
      pos += byte_len * 2;
      if (remain_length < key_length) {
        buffer[pos++] = '.';
        buffer[pos++] = '.';
      }
    }

    // add ,
    buffer[pos++] = ',';
    remain_length = length - pos;

    // add end_key_
    if (!border_flag_.is_max_value()) {
      // buffer not enough, store part of end key, end key = "key"|".."|"]\0"
      max_key_length = (remain_length < end_key_.length() * 2 + 2) ? (remain_length - 4) / 2 :
                       end_key_.length();
      int byte_len = hex_to_str(end_key_.ptr(), max_key_length, buffer + pos, remain_length);
      pos += byte_len * 2;
      if (remain_length < end_key_.length() * 2 + 2) {
        buffer[pos++] = '.';
        buffer[pos++] = '.';
      }
    }

    const char *rb = 0;
    if (border_flag_.inclusive_end()) { rb = "]"; }
    else { rb = ")"; }
    if (border_flag_.is_max_value()) { rb = "MAX)"; }
    snprintf(buffer + pos, length - pos, "%s", rb);

  }

  return ret;
}

int ObRange::trim(const ObRange &r, ObStringBuf &string_buf)
{
  int ret = OB_SUCCESS;
  if (r.table_id_ != table_id_) {
    ret = OB_ERROR;
    _OB_LOG(WARN, "table id is not equal table_id_[%ld] r.table_id_[%ld]",
            table_id_, r.table_id_);
  }

  if (OB_SUCC(ret)) {
    if (0 == compare_with_startkey2(r)) { // left interval equal
      if (0 == compare_with_endkey2(r)) { // both interval equal
        ret = OB_EMPTY_RANGE;
      } else {
        if (compare_with_endkey2(r) > 0) { // right interval great
          border_flag_.unset_min_value();
          if (r.border_flag_.inclusive_end()) {
            border_flag_.unset_inclusive_start();
          } else {
            border_flag_.set_inclusive_start();
          }
          ret = string_buf.write_string(r.end_key_, &start_key_);
          if (OB_FAIL(ret)) {
            _OB_LOG(WARN, "write start key fail:ret[%d]", ret);
          }
        } else {
          ret = OB_ERROR;
          _OB_LOG(DEBUG, "dump this range");
          dump();
          _OB_LOG(DEBUG, "dump r range");
          r.dump();
          _OB_LOG(WARN, "r should be included by this range");
        }
      }
    } else if (0 == compare_with_endkey2(r)) { // right interval equal
      if (compare_with_startkey2(r) < 0) { // left interval less
        border_flag_.unset_max_value();
        if (r.border_flag_.inclusive_start()) {
          border_flag_.unset_inclusive_end();
        } else {
          border_flag_.set_inclusive_end();
        }
        ret = string_buf.write_string(r.start_key_, &end_key_);
        if (OB_FAIL(ret)) {
          _OB_LOG(WARN, "write end key fail:ret[%d]", ret);
        }
      } else {
        ret = OB_ERROR;

        _OB_LOG(DEBUG, "dump this range");
        dump();
        _OB_LOG(DEBUG, "dump r range");
        r.dump();
        _OB_LOG(WARN, "r should be included by this range");
      }
    } else {
      ret = OB_ERROR;

      _OB_LOG(DEBUG, "dump this range");
      dump();
      _OB_LOG(DEBUG, "dump r range");
      r.dump();
      _OB_LOG(WARN, "r should be included by this range");
    }
  }

  return ret;
}

int64_t ObRange::hash() const
{
  uint32_t hash_val = 0;
  int8_t flag = border_flag_.get_data();

  /**
   * if it's max value, the border flag maybe is right close,
   * ignore it.
   */
  if (border_flag_.is_max_value()) {
    flag = ObBorderFlag::MAX_VALUE;
  }
  hash_val = murmurhash(&table_id_, sizeof(uint64_t), 0);
  hash_val = murmurhash(&flag, sizeof(int8_t), hash_val);
  if (!border_flag_.is_min_value() && NULL != start_key_.ptr()
      && start_key_.length() > 0) {
    hash_val = murmurhash(start_key_.ptr(), start_key_.length(), hash_val);
  }
  if (!border_flag_.is_max_value() && NULL != end_key_.ptr()
      && end_key_.length() > 0) {
    hash_val = murmurhash(end_key_.ptr(), end_key_.length(), hash_val);
  }

  return hash_val;
};

DEFINE_SERIALIZE(ObRange)
{
  int ret = OB_ERROR;
  ret = serialization::encode_vi64(buf, buf_len, pos, static_cast<int64_t>(table_id_));

  if (OB_SUCC(ret)) {
    ret = serialization::encode_i8(buf, buf_len, pos, border_flag_.get_data());
  }

  if (OB_SUCC(ret)) {
    ret = start_key_.serialize(buf, buf_len, pos);
  }

  if (OB_SUCC(ret)) {
    ret = end_key_.serialize(buf, buf_len, pos);
  }

  return ret;
}

DEFINE_DESERIALIZE(ObRange)
{
  int ret = OB_ERROR;
  ret = serialization::decode_vi64(buf, data_len, pos, (int64_t *)&table_id_);

  if (OB_SUCC(ret)) {
    int8_t flag = 0;
    ret = serialization::decode_i8(buf, data_len, pos, &flag);
    if (OB_SUCC(ret)) {
      border_flag_.set_data(flag);
    }
  }

  if (OB_SUCC(ret)) {
    ret = start_key_.deserialize(buf, data_len, pos);
    if (OB_FAIL(ret)) {
      _OB_LOG(WARN, "failed to deserialize start key, buf=%p, data_len=%ld, pos=%ld, ret=%d",
              buf, data_len, pos, ret);
    }
  }

  if (OB_SUCC(ret)) {
    ret = end_key_.deserialize(buf, data_len, pos);
    if (OB_FAIL(ret)) {
      _OB_LOG(WARN, "failed to deserialize end key, buf=%p, data_len=%ld, pos=%ld, ret=%d",
              buf, data_len, pos, ret);
    }
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObRange)
{
  int64_t total_size = 0;

  total_size += serialization::encoded_length_vi64(table_id_);
  total_size += serialization::encoded_length_i8(border_flag_.get_data());

  total_size += start_key_.get_serialize_size();
  total_size += end_key_.get_serialize_size();

  return total_size;
}
#endif

} // end namespace common
} // end namespace oceanbase
