/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PROXY
#include "proxy/mysqllib/ob_oceanbase_20_header_param.h"
#include "proxy/mysqllib/ob_protocol_diagnosis.h"
namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
Ob20HeaderParam::Ob20HeaderParam(const Ob20HeaderParam &param) {
  connection_id_ = param.connection_id_;
  request_id_ = param.request_id_;
  compressed_seq_ = param.compressed_seq_;
  pkt_seq_ = param.pkt_seq_;
  is_last_packet_ = param.is_last_packet_;
  is_weak_read_ = param.is_weak_read_;
  is_need_reroute_ = param.is_need_reroute_;
  is_new_extra_info_ = param.is_new_extra_info_;
  is_trans_internal_routing_ = param.is_trans_internal_routing_;
  is_switch_route_ = param.is_switch_route_;
  is_compressed_ob20_ = param.is_compressed_ob20_;
  compression_level_ = param.compression_level_;
  protocol_diagnosis_ = NULL;
  DEC_AND_INC_SHARED_REF(protocol_diagnosis_, const_cast<ObProtocolDiagnosis*>(param.get_protocol_diagnosis()));
}

Ob20HeaderParam &Ob20HeaderParam::operator=(const Ob20HeaderParam &param) {
  if (this != &param) {
    connection_id_ = param.connection_id_;
    request_id_ = param.request_id_;
    compressed_seq_ = param.compressed_seq_;
    pkt_seq_ = param.pkt_seq_;
    is_last_packet_ = param.is_last_packet_;
    is_weak_read_ = param.is_weak_read_;
    is_need_reroute_ = param.is_need_reroute_;
    is_new_extra_info_ = param.is_new_extra_info_;
    is_trans_internal_routing_ = param.is_trans_internal_routing_;
    is_switch_route_ = param.is_switch_route_;
    is_compressed_ob20_ = param.is_compressed_ob20_;
    compression_level_ = param.compression_level_;
    DEC_AND_INC_SHARED_REF(protocol_diagnosis_, const_cast<ObProtocolDiagnosis*>(param.get_protocol_diagnosis()));
  }
  return *this;
}

Ob20HeaderParam::~Ob20HeaderParam() {
  DEC_SHARED_REF(protocol_diagnosis_);
}

void Ob20HeaderParam::reset() {
  DEC_SHARED_REF(protocol_diagnosis_);
  MEMSET(this, 0x0, sizeof(Ob20HeaderParam));
}

ObProtocolDiagnosis *&Ob20HeaderParam::get_protocol_diagnosis_ref() {
  return protocol_diagnosis_;
}

ObProtocolDiagnosis *Ob20HeaderParam::get_protocol_diagnosis() {
  return protocol_diagnosis_;
}

const ObProtocolDiagnosis *Ob20HeaderParam::get_protocol_diagnosis() const {
  return protocol_diagnosis_;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase