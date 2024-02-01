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

#include "lib/allocator/ob_malloc.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/utility/serialization.h"
#include "common/ob_rowkey.h"
#include "common/ob_rowkey_info.h"
#include "common/ob_object.h"
#include "common/ob_obj_cast.h"

namespace oceanbase
{
namespace common
{

ObObj ObRowkey::MIN_OBJECT = ObObj::make_min_obj();
ObObj ObRowkey::MAX_OBJECT = ObObj::make_max_obj();

ObRowkey ObRowkey::MIN_ROWKEY(&ObRowkey::MIN_OBJECT, 1);
ObRowkey ObRowkey::MAX_ROWKEY(&ObRowkey::MAX_OBJECT, 1);

int32_t ObRowkey::compare(const ObRowkey &rhs) const
{
  return compare(rhs, NULL);
}

int32_t ObRowkey::compare(const ObRowkey &rhs, const ObRowkeyInfo *rowkey_info) const
{
  int32_t cmp = 0;
  int32_t lv = 0;
  int32_t rv = 0;

  if (obj_ptr_ == rhs.obj_ptr_) {
    cmp = static_cast<int32_t>(obj_cnt_ - rhs.obj_cnt_);
  } else {
    if (is_min_row()) { lv = -1; }
    else if (is_max_row()) { lv = 1; }
    if (rhs.is_min_row()) { rv = -1; }
    else if (rhs.is_max_row()) { rv  = 1; }
    if (0 == lv && 0 == rv) {
      int64_t i = 0;
      int64_t cmp_cnt = std::min(obj_cnt_, rhs.obj_cnt_);
      for (; i < cmp_cnt && 0 == cmp; ++i) {
       cmp = obj_ptr_[i].compare(rhs.obj_ptr_[i], CS_TYPE_INVALID) *
              ((NULL == rowkey_info) ? 1 : rowkey_info->get_column(i)->order_);
      }

      if (0 == cmp) {
        cmp = static_cast<int32_t>(obj_cnt_ - rhs.obj_cnt_);
      }
    } else {
      cmp = lv - rv;
    }
  }

  return cmp;
}

int ObRowkey::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "invalid arguments.",
               KP(buf), K(buf_len), K(ret));
  } else if (OB_UNLIKELY(!is_legal())) {
    ret = OB_INVALID_DATA;
    COMMON_LOG(WDIAG, "illegal rowkey.",
               KP_(obj_ptr), K_(obj_cnt), K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, obj_cnt_))) {
    COMMON_LOG(WDIAG, "encode object count failed.",
               KP(buf), K(buf_len), K(pos), K_(obj_cnt), K(ret));
  } else if (OB_FAIL(serialize_objs(buf, buf_len, pos))) {
    COMMON_LOG(WDIAG, "serialize objects failed.",
               KP(buf), K(buf_len), K(pos), K_(obj_cnt), K(ret));
  }
  return ret;
}

int ObRowkey::deserialize(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t obj_cnt = 0;
  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "invalid arguments.",
               KP(buf), K(buf_len), K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, buf_len, pos, &obj_cnt))) {
    COMMON_LOG(WDIAG, "decode object count failed.",
               KP(buf), K(buf_len), K(pos), K(obj_cnt), K(ret));
  } else if (obj_cnt_ < obj_cnt) {
    ret = OB_BUF_NOT_ENOUGH;
    COMMON_LOG(EDIAG, "obj number greater than expected.",
               K_(obj_cnt), K(obj_cnt), K(ret));
  } else {
    obj_cnt_ = obj_cnt;
    if (OB_FAIL(deserialize_objs(buf, buf_len, pos))) {
      COMMON_LOG(WDIAG, "decode objects failed.",
                 KP(buf), K(buf_len), K(pos), K(obj_cnt), K(ret));
    }
  }
  return ret;
}

int64_t ObRowkey::get_serialize_size(void) const
{
  int64_t size = serialization::encoded_length_vi64(obj_cnt_);
  size += get_serialize_objs_size();
  return size;
}

int ObRowkey::serialize_objs(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "invalid arguments.", KP(buf), K(buf_len), K(ret));
  } else if (OB_UNLIKELY(!is_legal())) {
    ret = OB_INVALID_DATA;
    COMMON_LOG(WDIAG, "illegal rowkey.",
               KP_(obj_ptr), K_(obj_cnt), K(ret));
  }
  for (int64_t i = 0; i < obj_cnt_ && OB_SUCCESS == ret; ++i) {
    if (OB_FAIL(obj_ptr_[i].serialize(buf, buf_len, pos))) {
      COMMON_LOG(WDIAG, "serialize object failed.",
                 K(i), KP(buf), K(buf_len), K(pos), K(ret));
    }
  }

  return ret;
}

int64_t ObRowkey::get_serialize_objs_size(void) const
{
  int64_t total_size = 0;
  for (int64_t i = 0; i < obj_cnt_ ; ++i) {
    total_size += obj_ptr_[i].get_serialize_size();
  }
  return total_size;
}

int ObRowkey::deserialize_objs(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (NULL == obj_ptr_) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(EDIAG, "obj array is NULL", K(ret));
  } else {
    for (int64_t i = 0; i < obj_cnt_ && OB_SUCCESS == ret; ++i) {
      if (OB_FAIL(obj_ptr_[i].deserialize(buf, buf_len, pos))) {
        COMMON_LOG(WDIAG, "deserialize object failed.",
                   K(i), KP(buf), K(buf_len), K(pos), K(ret));
      }
    }
  }
  return ret;
}

int64_t ObRowkey::to_string(char *buffer, const int64_t length) const
{
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buffer) || OB_ISNULL(obj_ptr_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "arg is null", KP(buffer), KP(obj_ptr_));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < obj_cnt_; ++i) {
      if (pos < length) {
        if (!obj_ptr_[i].is_max_value() && !obj_ptr_[i].is_min_value()) {
          obj_ptr_[i].print_range_value(buffer, length, pos);
        } else if (obj_ptr_[i].is_min_value()) {
          if (OB_FAIL(databuff_printf(buffer, length, pos, "MIN"))) {
            COMMON_LOG(WDIAG, "Failed to print", K(obj_ptr_[i]), K(ret));
          }
        } else {
          if (OB_FAIL(databuff_printf(buffer, length, pos, "MAX"))) {
            COMMON_LOG(WDIAG, "Failed to print", K(obj_ptr_[i]), K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (i < obj_cnt_ - 1) {
            if (OB_FAIL(databuff_printf(buffer, length, pos, ","))) {
              COMMON_LOG(WDIAG, "Failed to print", K(ret));
            }
          }
        }
      }
    }
  }
  //no way to preserve ret code
  (void)ret;
  return pos;
}

int64_t ObRowkey::to_plain_string(char *buffer, const int64_t length) const
{
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < obj_cnt_; ++i) {
    if (pos < length) {
      if (!obj_ptr_[i].is_max_value() && !obj_ptr_[i].is_min_value()) {
        if (OB_FAIL(obj_ptr_[i].print_plain_str_literal(buffer, length, pos))) {
          COMMON_LOG(WDIAG, "Failed to print", K(obj_ptr_[i]), K(ret));
        }
      } else if (obj_ptr_[i].is_min_value()) {
        if (OB_FAIL(databuff_printf(buffer, length, pos, "MIN"))) {
          COMMON_LOG(WDIAG, "Failed to print", K(obj_ptr_[i]), K(ret));
        }
      } else {
        if (OB_FAIL(databuff_printf(buffer, length, pos, "MAX"))) {
          COMMON_LOG(WDIAG, "Failed to print", K(obj_ptr_[i]), K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (i < obj_cnt_ - 1) {
          if (OB_FAIL(databuff_printf(buffer, length, pos, ","))) {
            COMMON_LOG(WDIAG, "Failed to print", K(ret));
          }
        }
      }
    } else {
      break;
    }
  }
  //no way to preserve ret code
  (void)ret;
  return pos;
}

int ObRowkey::checksum(ObBatchChecksum &bc) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_legal())) {
    ret = OB_INVALID_DATA;
    COMMON_LOG(WDIAG, "illegal rowkey.",
               KP_(obj_ptr), K_(obj_cnt), K(ret));
  } else if (0 < obj_cnt_ && NULL != obj_ptr_) {
    for (int64_t i = 0; i < obj_cnt_; i++) {
      obj_ptr_[i].checksum(bc);
    }
  }
  return ret;
}

uint64_t ObRowkey::murmurhash(const uint64_t hash) const
{
  int tmp_ret = OB_SUCCESS;
  uint64_t ret = hash;
  if (OB_UNLIKELY(!is_legal())) {
    tmp_ret = OB_INVALID_DATA;
    COMMON_LOG(EDIAG, "illegal rowkey.",
               KP_(obj_ptr), K_(obj_cnt), K(tmp_ret));
  } else if (0 < obj_cnt_ && NULL != obj_ptr_) {
    if (is_min_row() || is_max_row()) {
      ret = obj_ptr_[0].hash(ret);
    } else {
      for (int64_t i = 0; i < obj_cnt_; i++) {
        ret = obj_ptr_[i].hash(ret);
      }
    }
  }
  return ret;
}

RowkeyInfoHolder::RowkeyInfoHolder(const ObRowkeyInfo *ri)
    : rowkey_info_(ri)
{
}

RowkeyInfoHolder::~RowkeyInfoHolder()
{
}

void RowkeyInfoHolder::set_rowkey_info(const ObRowkeyInfo *ri)
{
  rowkey_info_ = ri;
}

bool ObRowkeyLess::operator()(const ObRowkey &lhs, const ObRowkey &rhs) const
{
  return lhs.compare(rhs, rowkey_info_) < 0;
}

int ObRowkeyLess::compare(const ObRowkey &lhs, const ObRowkey &rhs) const
{
  return lhs.compare(rhs, rowkey_info_);
}

}
}
