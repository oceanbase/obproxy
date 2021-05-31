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

#ifndef OCEANBASE_COMMON_OB_RECORD_HEADER_V2_H_
#define OCEANBASE_COMMON_OB_RECORD_HEADER_V2_H_

#include "lib/ob_define.h"
#include "lib/checksum/ob_crc64.h"
#include "common/ob_record_header.h"

namespace oceanbase
{
namespace common
{
struct ObRecordHeaderV2
{
  static const int8_t HEADER_VERSION;
  int16_t magic_;
  int8_t header_length_;
  int8_t version_;
  int16_t header_checksum_;
  int16_t reserved16_;
  int64_t data_length_;
  int64_t data_zlength_;
  int64_t data_checksum_;

  ObRecordHeaderV2();
  ~ObRecordHeaderV2() {}

  TO_STRING_KV(K_(magic),
               K_(header_length),
               K_(version),
               K_(header_checksum),
               K_(reserved16),
               K_(data_length),
               K_(data_zlength),
               K_(data_checksum));

  inline void reset();


  inline bool is_compressed_data() const;

  void set_header_checksum();
  int check_header_checksum() const;
  int check_payload_checksum(const char *buf, const int64_t len) const;

  static int check_record(
      const char *ptr,
      const int64_t size,
      const int16_t magic,
      ObRecordHeaderV2 &header,
      const char *&payload_ptr,
      int64_t &payload_size);

  static int check_record(
      const char *buf,
      const int64_t len,
      const int16_t magic);

  static int get_record_header(
      const char *buf,
      const int64_t size,
      ObRecordHeaderV2 &header,
      const char *&payload_ptr,
      int64_t &payload_size);
};


inline void ObRecordHeaderV2::reset()
{
  memset(this, 0, sizeof(ObRecordHeaderV2));
  header_length_ = static_cast<int8_t>(sizeof(ObRecordHeaderV2));
  version_ = ObRecordHeaderV2::HEADER_VERSION;
}

inline bool ObRecordHeaderV2::is_compressed_data() const
{
  return data_length_ != data_zlength_;
}
}//end namespace commmon
}//end namespace oceanbase
#endif

