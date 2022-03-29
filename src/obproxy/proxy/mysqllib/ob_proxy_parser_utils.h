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

#ifndef OBPROXY_PARSER_UTILS_H
#define OBPROXY_PARSER_UTILS_H

#include "lib/ob_define.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "proxy/mysqllib/ob_mysql_common_define.h"
#include "proxy/mysqllib/ob_proxy_mysql_request.h"
#include "proxy/mysqllib/ob_2_0_protocol_struct.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{
class ObIOBufferReader;
}
namespace proxy
{

enum ObMysqlAnalyzeStatus
{
  ANALYZE_CAN_NOT_PASS_WHITE_LIST_ERROR = -3,
  ANALYZE_OBPARSE_ERROR = -2,
  ANALYZE_ERROR = -1,
  ANALYZE_DONE = 0,
  ANALYZE_OK = 1,
  ANALYZE_CONT = 2
};

struct ObMysqlAnalyzeResult
{

  ObMysqlAnalyzeResult() { reset(); }
  ~ObMysqlAnalyzeResult() { }
  void reset()
  {
    status_ = ANALYZE_ERROR;
    meta_.reset();
  }

  bool is_ok_packet() const { return (MYSQL_OK_PACKET_TYPE == meta_.cmd_); }
  bool is_error_packet() const { return (MYSQL_ERR_PACKET_TYPE == meta_.cmd_); }

  ObMysqlAnalyzeStatus status_;
  ObMysqlPacketMeta meta_;
};

class ObMysqlCompressedAnalyzeResult
{
public:
  ObMysqlCompressedAnalyzeResult() : status_(ANALYZE_ERROR), header_(), is_checksum_on_(true) {}
  ~ObMysqlCompressedAnalyzeResult() {}
  virtual void reset()
  {
    status_ = ANALYZE_ERROR;
    header_.reset();
    is_checksum_on_ = true;
  }

  ObMysqlAnalyzeStatus status_;
  ObMysqlCompressedPacketHeader header_;
  bool is_checksum_on_;
  TO_STRING_KV(K_(status), K_(header), K_(is_checksum_on));
};

class ObMysqlCompressedOB20AnalyzeResult : public ObMysqlCompressedAnalyzeResult
{
public:
  ObMysqlCompressedOB20AnalyzeResult() : ObMysqlCompressedAnalyzeResult(), ob20_header_() {}
  ~ObMysqlCompressedOB20AnalyzeResult() {}
  virtual void reset()
  {
    ObMysqlCompressedAnalyzeResult::reset();
    ob20_header_.reset();
  }

  Ob20ProtocolHeader ob20_header_;
  TO_STRING_KV(K_(status), K_(header), K_(is_checksum_on), K_(ob20_header));
};

class ObProxyParserUtils
{
public:
  ObProxyParserUtils() {};
  ~ObProxyParserUtils() {};
  // get Length-Encoded Integer, and increase buf pointer
  static uint64_t get_lenenc_int(const char *&buf);
  static const char *get_sql_cmd_name(const obmysql::ObMySQLCmd cmd);
  static const char *get_analyze_status_name(const ObMysqlAnalyzeStatus status);

  // judge whether one mysql packet has been received complete, and get packt len
  // if completed, return ANALYZE_DONE
  // if not,       return ANALYZE_CONT
  //
  // if is_mysql_request = true, the analyzed packet is mysql request packet
  // if is_mysql_request = false, the analyzed packet is mysql response packet
  static int analyze_one_packet(event::ObIOBufferReader &reader,
                                ObMysqlAnalyzeResult &result);


  // some mysql packet mybe only has header, without body
  // so this method only analyze header, and return whether
  // the packet received completed
  static int analyze_one_packet_only_header(event::ObIOBufferReader &reader,
                                            ObMysqlAnalyzeResult &result);

  static bool is_ok_packet(event::ObIOBufferReader &reader, ObMysqlAnalyzeResult &result);
  static bool is_error_packet(event::ObIOBufferReader &reader, ObMysqlAnalyzeResult &result);

  // judge whether one mysql compressed packet has been received complete, and get packt len
  // if completed, return ANALYZE_DONE
  // if not,       return ANALYZE_CONT
  static int analyze_one_compressed_packet(event::ObIOBufferReader &reader,
                                           ObMysqlCompressedAnalyzeResult &result);
  static int analyze_mysql_packet_meta(const char *ptr, const int64_t len, ObMysqlPacketMeta &meta);

  static int analyze_mysql_packet_header(const char *ptr, const int64_t len, obmysql::ObMySQLPacketHeader &header);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OBPROXY_PARSER_UTILS_H */
