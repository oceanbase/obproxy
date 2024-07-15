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

#ifndef OBPROXY_OB20_HEADER_PARAM_H
#define OBPROXY_OB20_HEADER_PARAM_H
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

class ObProtocolDiagnosis;

// used for transfer function argument
// flag ref to Protocol20Flags
class Ob20HeaderParam {
public:
  Ob20HeaderParam() : connection_id_(0),
                      request_id_(0),
                      compressed_seq_(0),
                      pkt_seq_(0),
                      is_last_packet_(false),
                      is_weak_read_(false),
                      is_need_reroute_(false),
                      is_new_extra_info_(false),
                      is_trans_internal_routing_(false),
                      is_switch_route_(false),
                      is_compressed_ob20_(false),
                      compression_level_(0),
                      protocol_diagnosis_(NULL) {}
  Ob20HeaderParam(uint32_t conn_id, uint32_t req_id, uint8_t compressed_seq, uint8_t pkt_seq,
                  bool is_last_packet, bool is_weak_read, bool is_need_reroute,
                  bool is_new_extra_info, bool is_trans_internal_routing, bool is_switch_route,
                  bool is_compressed_ob20 = false, int64_t compression_level = 0)
    : connection_id_(conn_id), request_id_(req_id), compressed_seq_(compressed_seq), pkt_seq_(pkt_seq),
      is_last_packet_(is_last_packet), is_weak_read_(is_weak_read), is_need_reroute_(is_need_reroute),
      is_new_extra_info_(is_new_extra_info), is_trans_internal_routing_(is_trans_internal_routing),
      is_switch_route_(is_switch_route), is_compressed_ob20_(is_compressed_ob20), compression_level_(compression_level),
      protocol_diagnosis_(NULL) {}
  ~Ob20HeaderParam();
  Ob20HeaderParam(const Ob20HeaderParam &param);

  Ob20HeaderParam &operator=(const Ob20HeaderParam &param);

  inline uint32_t get_connection_id() const { return connection_id_; }
  inline uint32_t get_request_id() const { return request_id_; }
  inline uint8_t get_compressed_seq() const { return compressed_seq_; }
  inline void set_compressed_seq(uint8_t compressed_seq) { compressed_seq_ = compressed_seq; }
  inline uint8_t get_pkt_seq() const { return pkt_seq_; }
  inline bool is_last_packet() const { return is_last_packet_; }
  inline bool is_weak_read() const { return is_weak_read_; }
  inline bool is_need_reroute() const { return is_need_reroute_; }
  inline bool is_new_extra_info() const { return is_new_extra_info_; }
  inline bool is_trans_internal_routing() const { return is_trans_internal_routing_; }
  inline bool is_switch_route() const { return is_switch_route_; }
  inline bool is_compressed_ob20() const { return is_compressed_ob20_; }
  inline int64_t get_compresion_level() const { return compression_level_; }
  

  ObProtocolDiagnosis *&get_protocol_diagnosis_ref();
  ObProtocolDiagnosis *get_protocol_diagnosis();
  const ObProtocolDiagnosis *get_protocol_diagnosis() const;
  void reset();

  TO_STRING_KV(K_(connection_id), K_(request_id), K_(compressed_seq), K_(pkt_seq),
               K_(is_last_packet), K_(is_weak_read), K_(is_need_reroute), K_(is_new_extra_info),
               K_(is_trans_internal_routing), K_(is_switch_route), K_(is_compressed_ob20),
               K_(compression_level));
private:
  uint32_t connection_id_;
  uint32_t request_id_;
  uint8_t compressed_seq_;
  uint8_t pkt_seq_;
  bool is_last_packet_;
  bool is_weak_read_;
  bool is_need_reroute_;
  bool is_new_extra_info_;
  bool is_trans_internal_routing_;
  bool is_switch_route_;              // send to observer only
  bool is_compressed_ob20_;
  int64_t compression_level_;
  ObProtocolDiagnosis *protocol_diagnosis_;
};
} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OBPROXY_OB20_PROTOCOL_UTILS_H */