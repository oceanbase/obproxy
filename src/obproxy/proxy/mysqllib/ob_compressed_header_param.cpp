/**
 * Copyright (c) 2024 OceanBase
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
#include "proxy/mysqllib/ob_compressed_header_param.h"
#include "proxy/mysqllib/ob_protocol_diagnosis.h"
namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
ObCompressedHeaderParam::ObCompressedHeaderParam(const ObCompressedHeaderParam &param) {
  compressed_seq_ = param.compressed_seq_;
  compression_level_ = param.compression_level_;
  is_checksum_on_ = param.is_checksum_on_;
  protocol_diagnosis_ = NULL;
  INC_SHARED_REF(protocol_diagnosis_, const_cast<ObProtocolDiagnosis*>(param.get_protocol_diagnosis()));
}

ObCompressedHeaderParam &ObCompressedHeaderParam::operator=(const ObCompressedHeaderParam &param) {
  if (this != &param) {
    compressed_seq_ = param.compressed_seq_;
    compression_level_ = param.compression_level_;
    is_checksum_on_ = param.is_checksum_on_;
    INC_SHARED_REF(protocol_diagnosis_, const_cast<ObProtocolDiagnosis*>(param.get_protocol_diagnosis()));
  }
  return *this;
}

ObCompressedHeaderParam::~ObCompressedHeaderParam() {
  DEC_SHARED_REF(protocol_diagnosis_);
}

ObProtocolDiagnosis *&ObCompressedHeaderParam::get_protocol_diagnosis_ref() {
return protocol_diagnosis_;
}

ObProtocolDiagnosis *ObCompressedHeaderParam::get_protocol_diagnosis() {
  return protocol_diagnosis_;
}

const ObProtocolDiagnosis *ObCompressedHeaderParam::get_protocol_diagnosis() const{
  return protocol_diagnosis_;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase