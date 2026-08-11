/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
  DEC_AND_INC_SHARED_REF(protocol_diagnosis_, const_cast<ObProtocolDiagnosis*>(param.get_protocol_diagnosis()));
}

ObCompressedHeaderParam &ObCompressedHeaderParam::operator=(const ObCompressedHeaderParam &param) {
  if (this != &param) {
    compressed_seq_ = param.compressed_seq_;
    compression_level_ = param.compression_level_;
    is_checksum_on_ = param.is_checksum_on_;
    DEC_AND_INC_SHARED_REF(protocol_diagnosis_, const_cast<ObProtocolDiagnosis*>(param.get_protocol_diagnosis()));
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