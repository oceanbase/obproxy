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

#define SERVER_EXTRA_INFO_BUF_MAX_LEN (335)   // FLTSpanInfo: 75 + FLTAppInfo: 260
#define CLIENT_EXTRA_INFO_BUF_MAX_LEN (100)    // FLTControlInfo:62 + FLTQueryInfo: 34

typedef obmysql::ObCommonKV<common::ObObj, common::ObObj> ObObJKV;
class ObMysqlSM;

class ObProto20Utils
{
  static const int64_t OB_SIMPLE_OK_PKT_LEN = 12;
public:
  ObProto20Utils();
  virtual ~ObProto20Utils();

  // encode packet utils
  static int encode_ok_packet(event::ObMIOBuffer &write_buf, const Ob20ProtocolHeaderParam &ob20_head_param,
                              uint8_t &seq, const int64_t affected_rows,
                              const obmysql::ObMySQLCapabilityFlags &capability, const uint16_t status_flag = 0);
  static int encode_kv_resultset(event::ObMIOBuffer &write_buf, const Ob20ProtocolHeaderParam &ob20_head_param,
                                 uint8_t &seq, const obmysql::ObMySQLField &field, ObObj &field_value,
                                 const uint16_t status_flag = 0);
  static int encode_err_packet(event::ObMIOBuffer &write_buf, const Ob20ProtocolHeaderParam &ob20_head_param,
                               uint8_t &seq, const int err_code, const ObString &msg_buf);
  static int encode_executor_response_packet(event::ObMIOBuffer *write_buf,
                                             const Ob20ProtocolHeaderParam &ob20_head_param,
                                             uint8_t &seq, engine::ObProxyResultResp *result_resp);
  // analyze utils
  static int analyze_ok_packet_and_get_reroute_info(event::ObIOBufferReader *reader,
                                                    const int64_t pkt_len,
                                                    const obmysql::ObMySQLCapabilityFlags &cap,
                                                    ObProxyRerouteInfo &reroute_info);
  static int analyze_first_mysql_packet(event::ObIOBufferReader &reader,
                                        ObMysqlCompressedOB20AnalyzeResult &result,
                                        ObMysqlAnalyzeResult &mysql_result);
  static int analyze_one_compressed_packet(event::ObIOBufferReader &reader,
                                           ObMysqlCompressedOB20AnalyzeResult &result);
  static int consume_and_compress_data(event::ObIOBufferReader *reader, event::ObMIOBuffer *write_buf,
                                       const int64_t data_len, const Ob20ProtocolHeaderParam &ob20_head_param,
                                       const common::ObIArray<ObObJKV> *extra_info = NULL);

  // fill utils
  static int fill_proto20_extra_info(event::ObMIOBuffer *write_buf, const common::ObIArray<ObObJKV> *extra_info,
                                     const bool is_new_extra_info, int64_t &payload_len, uint64_t &crc64,
                                     bool &is_extra_info_exist);
  static int fill_proto20_payload(event::ObIOBufferReader *reader, event::ObMIOBuffer *write_buf,
                                  const int64_t data_len, int64_t &payload_len, uint64_t &crc64);
  static int fill_proto20_tailer(event::ObMIOBuffer *write_buf, const uint64_t crc64);
  static int fill_proto20_header(char *hdr_start, const int64_t payload_len,
                                 const uint8_t compressed_seq, const uint8_t packet_seq,
                                 const uint32_t request_id, const uint32_t connid,
                                 const bool is_last_packet, const bool is_need_reroute,
                                 const bool is_extra_info_exist, const bool is_new_extra_info);
  static int reserve_proto20_hdr(event::ObMIOBuffer *write_buf, char *&hdr_start);
  
private:
  inline static int analyze_compressed_packet_header(const char *start, const int64_t len,
                                                     ObMysqlCompressedOB20AnalyzeResult &result);
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
  static int build_extra_info_for_server(ObMysqlSM *sm, char *buf, int64_t buf_len,
                                         common::ObIArray<ObObJKV> &extra_info, const bool is_last_packet_or_segment);
  static int build_extra_info_for_client(ObMysqlSM *sm, char *buf, const int64_t len,
                                         common::ObIArray<ObObJKV> &extra_info);
  static int build_sync_sess_info(common::ObIArray<ObObJKV> &extra_info,
                                  common::hash::ObHashMap<int16_t, ObString>& sess_info_hash_map,
                                  common::ObSqlString& info_value);
  static int build_related_extra_info_all(common::ObIArray<ObObJKV> &extra_info, ObMysqlSM *sm,
                                          char *ip_buf, const int64_t ip_buf_len,
                                          char *extra_info_buf, const int64_t extra_info_buf_len,
                                          common::ObSqlString &info_value, const bool is_last_packet);
private:
  DISALLOW_COPY_AND_ASSIGN(ObProxyTraceUtils);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OBPROXY_OB20_PROTOCOL_UTILS_H */
