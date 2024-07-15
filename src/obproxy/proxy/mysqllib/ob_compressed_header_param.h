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

#ifndef OBPROXY_COMPRESSED_HEADER_PARAM_H
#define OBPROXY_COMPRESSED_HEADER_PARAM_H

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObProtocolDiagnosis;
class ObCompressedHeaderParam
{
public:
  ObCompressedHeaderParam() :
    compressed_seq_(0),
    is_checksum_on_(false),
    compression_level_(0),
    protocol_diagnosis_(NULL) {}

  ObCompressedHeaderParam(
    uint8_t compressed_seq,
    bool is_checksum_on,
    int64_t compression_level)
    : compressed_seq_(compressed_seq),
      is_checksum_on_(is_checksum_on),
      compression_level_(compression_level),
      protocol_diagnosis_(NULL){}

  ~ObCompressedHeaderParam();
  ObCompressedHeaderParam(const ObCompressedHeaderParam &param);
  ObCompressedHeaderParam &operator=(const ObCompressedHeaderParam &param);
  inline uint8_t get_compressed_seq() { return compressed_seq_; }
  inline void set_compressed_seq(uint8_t compressed_seq) { compressed_seq_ = compressed_seq; }
  inline const bool is_checksum_on() { return is_checksum_on_; }
  inline const int64_t get_compression_level() { return compression_level_; }

  ObProtocolDiagnosis *&get_protocol_diagnosis_ref();
  ObProtocolDiagnosis *get_protocol_diagnosis();
  const ObProtocolDiagnosis *get_protocol_diagnosis() const;

  TO_STRING_KV(K_(compressed_seq), K_(is_checksum_on), K_(compression_level), KP_(protocol_diagnosis));

private:
  uint8_t compressed_seq_;
  bool is_checksum_on_;
  int64_t compression_level_;
  ObProtocolDiagnosis *protocol_diagnosis_;
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OBPROXY_COMPRESSED_HEADER_PARAM_H */
