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

#include "common/ob_record_header_v2.h"

namespace oceanbase
{
namespace common
{

const int8_t ObRecordHeaderV2::HEADER_VERSION = 0x2;

ObRecordHeaderV2::ObRecordHeaderV2()
{
  reset();
}

void ObRecordHeaderV2::set_header_checksum()
{
  header_checksum_ = 0;
  int16_t checksum = 0;

  checksum = checksum ^ magic_;
  checksum = static_cast<int16_t>(checksum ^ static_cast<int16_t>(header_length_));
  checksum = static_cast<int16_t>(checksum ^ static_cast<int16_t>(version_));
  checksum = checksum ^ reserved16_;
  format_i64(data_length_, checksum);
  format_i64(data_zlength_, checksum);
  format_i64(data_checksum_, checksum);
  header_checksum_ = checksum;
}

int ObRecordHeaderV2::check_header_checksum() const
{
  int ret = OB_SUCCESS;
  int16_t checksum = 0;

  checksum = checksum ^ magic_;
  checksum = static_cast<int16_t>(checksum ^ static_cast<int16_t>(header_length_));
  checksum = static_cast<int16_t>(checksum ^ static_cast<int16_t>(version_));
  checksum = checksum ^ header_checksum_;
  checksum = checksum ^ reserved16_;
  format_i64(data_length_, checksum);
  format_i64(data_zlength_, checksum);
  format_i64(data_checksum_, checksum);

  if (0 != checksum) {
    ret = OB_CHECKSUM_ERROR;
    COMMON_LOG(WDIAG, "record check checksum failed.", K(*this), K(ret));
  }

  return ret;
}

int ObRecordHeaderV2::check_payload_checksum(const char *buf, const int64_t len) const
{
  int ret = OB_SUCCESS;

  if (NULL == buf || len < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "invalid arguments.",
               KP(buf), K(len), K(ret));
  } else if (0 == len && (0 != data_zlength_ || 0 != data_length_ || 0 != data_checksum_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "invalid arguments.",
               KP(buf), K(len),
               K_(data_zlength), K_(data_length),
               K_(data_checksum), K(ret));
  } else if ((data_zlength_ != len)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "data length is not correct.",
               K_(data_zlength), K(len), K(ret));
  } else {
    int64_t crc_check_sum = ob_crc64_sse42(buf, len);
    if (crc_check_sum !=  data_checksum_) {
      ret = OB_CHECKSUM_ERROR;
      COMMON_LOG(WDIAG, "checksum error.",
                 K(crc_check_sum), K_(data_checksum), K(ret));
    }
  }

  return ret;
}

int ObRecordHeaderV2::check_record(
    const char *ptr,
    const int64_t size,
    const int16_t magic,
    ObRecordHeaderV2 &header,
    const char *&payload_ptr,
    int64_t &payload_size)
{
  int ret = OB_SUCCESS;
  const int64_t record_header_len = sizeof(ObRecordHeaderV2);

  if (NULL == ptr || size < 0 || magic < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "invalid arguments.",
               KP(ptr), K(size), K(magic), K(ret));
  } else if (record_header_len > size) {
    ret = OB_BUF_NOT_ENOUGH;
    COMMON_LOG(WDIAG,
              "invalid arguments, header size too small.",
              K(record_header_len), K(size), K(ret));
  } else {
    MEMCPY(&header, ptr, record_header_len);
    payload_ptr = ptr + record_header_len;
    payload_size = size - record_header_len;
  }

  if (OB_SUCC(ret)) {
    if (magic != header.magic_) {
      ret = OB_INVALID_DATA;
      COMMON_LOG(WDIAG, "record header magic is not match",
                     K(header), K(magic), K(ret));
    } else if (HEADER_VERSION != header.version_) {
      ret = OB_INVALID_DATA;
      COMMON_LOG(WDIAG, "record header version is not match",
                           K(header), K(ret));
    }else if (OB_FAIL(header.check_header_checksum())) {
      COMMON_LOG(WDIAG, "check header checksum failed.",
                 K(header), K(record_header_len), K(ret));
    } else if (OB_FAIL(header.check_payload_checksum(payload_ptr, payload_size))) {
      COMMON_LOG(WDIAG, "check data checksum failed.",
                 K(header), KP(payload_ptr), K(payload_size), K(ret));
    }
  }

  return ret;
}

int ObRecordHeaderV2::check_record(
    const char *ptr,
    const int64_t size,
    const int16_t magic)
{
  int ret = OB_SUCCESS;
  ObRecordHeaderV2 header;

  const char *payload_ptr = NULL;
  int64_t payload_size = 0;

  if (NULL == ptr || size < 0 || magic < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "invalid arguments.",
               KP(ptr), K(size), K(magic), K(ret));
  } else if (OB_FAIL(check_record(ptr,
                                  size,
                                  magic,
                                  header,
                                  payload_ptr,
                                  payload_size))) {
    COMMON_LOG(WDIAG, "check record failed.",
               KP(ptr), K(size), K(magic), K(header),
               KP(payload_ptr), K(payload_size), K(ret));
  }

  return ret;
}

int ObRecordHeaderV2::get_record_header(
    const char *buf,
    const int64_t size,
    ObRecordHeaderV2 &header,
    const char *&payload_ptr,
    int64_t &payload_size)
{
  int ret = OB_SUCCESS;

  if (NULL == buf || size < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "invalid arguments.", KP(buf), K(size), K(ret));
  } else if (static_cast<int64_t>(sizeof(ObRecordHeaderV2)) > size) {
    ret = OB_BUF_NOT_ENOUGH;
    COMMON_LOG(WDIAG, "invalid arguments, header size too small.",
               K(sizeof(ObRecordHeaderV2)), K(size), K(ret));
  } else {
    MEMCPY(&header, buf, sizeof(ObRecordHeaderV2));
    payload_ptr = buf + sizeof(ObRecordHeaderV2);
    payload_size = size - sizeof(ObRecordHeaderV2);
  }

  return ret;
}
}//end namespace common
}//end namespace oceanbase

