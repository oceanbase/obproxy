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

#ifndef OBPROXY_OB20_PROTOCOL_UTILS_H
#define OBPROXY_OB20_PROTOCOL_UTILS_H

#include "iocore/eventsystem/ob_io_buffer.h"
#include "proxy/mysqllib/ob_proxy_parser_utils.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "proxy/mysqllib/ob_mysql_response.h"
#include "proxy/route/ob_route_struct.h"
#include "obproxy/engine/ob_proxy_operator_result.h"
#include "proxy/mysqllib/ob_mysql_analyzer_utils.h"

namespace oceanbase
{
namespace obmysql
{
class ObMySQLField;
}

namespace obproxy
{
namespace proxy
{

#define SERVER_FLT_INFO_BUF_MAX_LEN (335)   // FLTSpanInfo: 75 + FLTAppInfo: 260
#define CLIENT_EXTRA_INFO_BUF_MAX_LEN (105)    // FLTControlInfo:62/69 + FLTQueryInfo: 34

typedef obmysql::ObCommonKV<common::ObObj, common::ObObj> ObObJKV;
class ObMysqlSM;
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

class ObProto20Utils
{
  static const int64_t OB_SIMPLE_OK_PKT_LEN = 12;
public:
  ObProto20Utils();
  virtual ~ObProto20Utils();

  // encode packet utils
  static int encode_ok_packet(event::ObMIOBuffer &write_buf, Ob20HeaderParam &ob20_head_param,
                              uint8_t &seq, const int64_t affected_rows,
                              const obmysql::ObMySQLCapabilityFlags &capability, const uint16_t status_flag = 0);
  static int encode_kv_resultset(event::ObMIOBuffer &write_buf, Ob20HeaderParam &ob20_head_param,
                                 uint8_t &seq, const obmysql::ObMySQLField &field, ObObj &field_value,
                                 const uint16_t status_flag = 0);
  static int encode_err_packet(event::ObMIOBuffer &write_buf, Ob20HeaderParam &ob20_head_param,
                               uint8_t &seq, const int err_code, const ObString &msg_buf);
  // analyze utils
  static int analyze_ok_packet_and_get_reroute_info(event::ObIOBufferReader *reader,
                                                    const int64_t pkt_len,
                                                    const obmysql::ObMySQLCapabilityFlags &cap,
                                                    ObProxyRerouteInfo &reroute_info);
  static int analyze_first_mysql_packet(event::ObIOBufferReader &reader,
                                        ObMysqlCompressedOB20AnalyzeResult &result,
                                        ObMysqlAnalyzeResult &mysql_result);
  static int analyze_one_compressed_packet(event::ObIOBufferReader &reader,
                                           ObMysqlCompressedOB20AnalyzeResult &result,
                                           bool is_analyze_compressed_ob20);
  static int consume_and_compress_data(event::ObIOBufferReader *reader, event::ObMIOBuffer *write_buf,
                                       const int64_t data_len, Ob20HeaderParam &ob20_head_param,
                                       const common::ObIArray<ObObJKV> *extra_info = NULL);

  // fill utils
  static int fill_proto20_extra_info(event::ObMIOBuffer *write_buf, const common::ObIArray<ObObJKV> *extra_info,
                                     const bool is_new_extra_info, int64_t &payload_len, uint64_t &crc64,
                                     bool &is_extra_info_exist);
  static int fill_proto20_payload(event::ObIOBufferReader *reader, event::ObMIOBuffer *write_buf,
                                  const int64_t data_len, int64_t &payload_len, uint64_t &crc64);
  static int fill_proto20_tailer(event::ObMIOBuffer *write_buf, const uint64_t crc64);
  static int fill_proto20_header(char *hdr_start, const int64_t payload_len,
                                 Ob20HeaderParam &ob20_head_param,
                                 const bool is_extra_info_exist);
  static int reserve_proto_hdr(event::ObMIOBuffer *write_buf, char *&hdr_start, const int64_t reserve_len);
  static bool is_trans_related_sess_info(int16_t type);
  
private:
  inline static int analyze_compressed_packet_header(const char *start, const int64_t len,
                                                     ObMysqlCompressedOB20AnalyzeResult &result,
                                                     bool is_analyze_compressed_ob20);
  static int fill_proto20_obobj_extra_info(event::ObMIOBuffer *write_buf, const common::ObIArray<ObObJKV> *extra_info,
                                           int64_t &payload_len, uint64_t &crc64, bool &is_extra_info_exist);
  static int fill_proto20_new_extra_info(event::ObMIOBuffer *write_buf, const common::ObIArray<ObObJKV> *extra_info,
                                         int64_t &payload_len, uint64_t &crc64, bool &is_extra_info_exist);

private:
  DISALLOW_COPY_AND_ASSIGN(ObProto20Utils);
};

class ObProxyTraceUtils
{
public:
  static int build_client_ip(common::ObIArray<ObObJKV> &extra_info, char *buf, int64_t buf_len, ObMysqlSM *sm,
                             const bool is_last_packet_or_segment);
  static int build_flt_info_for_server(ObMysqlSM *sm, char *buf, int64_t buf_len,
                                        common::ObIArray<ObObJKV> &extra_info, const bool is_last_packet_or_segment);
  static int build_extra_info_for_client(ObMysqlSM *sm, char *buf, const int64_t len,
                                         common::ObIArray<ObObJKV> &extra_info, const bool is_last_packet);
  static int build_sync_sess_info(common::ObIArray<ObObJKV> &extra_info,
                                  common::ObSqlString &info_value,
                                  ObMysqlSM *sm,
                                  const bool is_last_packet);
  static int build_sess_veri_for_server(ObMysqlSM *sm, common::ObIArray<ObObJKV> &extra_info,
                                        char *sess_veri_buf, const int64_t sess_veri_buf_len,
                                        const bool is_last_packet, const bool is_proxy_switch_route);
  static int build_related_extra_info_all(common::ObIArray<ObObJKV> &extra_info, ObMysqlSM *sm,
                                          char *ip_buf, const int64_t ip_buf_len,
                                          char *flt_info_buf, const int64_t flt_info_buf_len,
                                          char *sess_veri_buf, const int64_t sess_veri_buf_len,
                                          common::ObSqlString &info_value, const bool is_last_packet,
                                          const bool is_proxy_switch_route);
  static int build_show_trace_info_buffer(ObMysqlSM *sm, const bool is_last_packet, char *&buf, int64_t &buf_len);
  
private:
  DISALLOW_COPY_AND_ASSIGN(ObProxyTraceUtils);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OBPROXY_OB20_PROTOCOL_UTILS_H */
