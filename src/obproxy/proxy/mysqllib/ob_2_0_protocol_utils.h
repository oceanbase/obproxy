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

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
typedef obmysql::ObCommonKV<common::ObObj, common::ObObj> ObObJKV;

class ObProto20Utils
{
  static const int64_t OB_SIMPLE_OK_PKT_LEN = 12;
public:
  ObProto20Utils();
  virtual ~ObProto20Utils();

  static int analyze_ok_packet_and_get_reroute_info(event::ObIOBufferReader *reader,
                                                    const int64_t pkt_len,
                                                    const obmysql::ObMySQLCapabilityFlags &cap,
                                                    ObProxyRerouteInfo &reroute_info);

  static int analyze_fisrt_mysql_packet(event::ObIOBufferReader &reader,
                                        ObMysqlCompressedOB20AnalyzeResult &result,
                                        ObMysqlAnalyzeResult &mysql_result);
  static int analyze_one_compressed_packet(event::ObIOBufferReader &reader,
                                           ObMysqlCompressedOB20AnalyzeResult &result);

  static int consume_and_compress_data(event::ObIOBufferReader *reader, event::ObMIOBuffer *write_buf,
                                       const int64_t data_len, const uint8_t compressed_seq, const uint8_t packet_seq,
                                       const uint32_t request_id, const uint32_t connid, const bool is_last_packet,
                                       const bool is_need_reroute, const common::ObIArray<ObObJKV> *extro_info = NULL);

private:
  inline static int analyze_compressed_packet_header(const char *start, const int64_t len,
                                                     ObMysqlCompressedOB20AnalyzeResult &result);

  inline static int reserve_proto20_hdr(event::ObMIOBuffer *write_buf, char *&hdr_start);
  inline static int fill_proto20_header(char *hdr_start, const int64_t payload_len,
                                        const uint8_t compressed_seq, const uint8_t packet_seq,
                                        const uint32_t request_id, const uint32_t connid,
                                        const bool is_last_packet, const bool is_need_reroute,
                                        const bool is_extro_info_exist);

  inline static int fill_proto20_extro_info(event::ObMIOBuffer *write_buf, const common::ObIArray<ObObJKV> *extro_info,
                                            int64_t &payload_len, uint64_t &crc64, bool &is_extro_info_exist);

  inline static int fill_proto20_payload(event::ObIOBufferReader *reader, event::ObMIOBuffer *write_buf,
                                         const int64_t data_len, int64_t &payload_len, uint64_t &crc64);

  inline static int fill_proto20_tailer(event::ObMIOBuffer *write_buf, const uint64_t crc64);

private:
  DISALLOW_COPY_AND_ASSIGN(ObProto20Utils);
};

class ObProxyTraceUtils
{
public:
  static int build_client_ip(common::ObIArray<ObObJKV> &extro_info, char *client_ip_buf, common::ObAddr &client_ip);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OBPROXY_OB20_PROTOCOL_UTILS_H */
