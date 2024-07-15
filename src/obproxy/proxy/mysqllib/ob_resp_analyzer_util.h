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

#ifndef OBPROXY_ANALYER_UTIL
#define OBPROXY_ANALYER_UTIL
#include <stdint.h>
#include <stddef.h>
#include "lib/ob_define.h"
#include "proxy/mysqllib/ob_proxy_parser_utils.h"
#include "iocore/eventsystem/ob_io_buffer.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
using namespace oceanbase::obmysql;
struct ObAnalyzeHeaderResult
{
  ObAnalyzeHeaderResult() { memset(this, 0, sizeof(ObAnalyzeHeaderResult)); }
  ~ObAnalyzeHeaderResult() {}
  void reset() { memset(this, 0, sizeof(ObAnalyzeHeaderResult)); }
  ObMysqlAnalyzeStatus status_;
  ObMysqlPacketMeta mysql_header_;
  union { // both of below have compressed mysql header
    ObMysqlCompressedPacketHeader compressed_mysql_header_;
    Ob20ProtocolHeader ob20_header_;
  };
  TO_STRING_KV(K(status_), K(mysql_header_), K(ob20_header_), K(compressed_mysql_header_));
};
enum ObRespAnalyzeMode
{
  SIMPLE_MODE,
  DECOMPRESS_MODE,
};
enum ObRespPktAnalyzerState
{
  STREAM_INVALID,
  STREAM_MYSQL_HEADER,
  STREAM_MYSQL_TYPE,
  STREAM_MYSQL_BODY,
  STREAM_MYSQL_END,
  STREAM_COMPRESSED_MYSQL_HEADER,
  STREAM_COMPRESSED_MYSQL_PAYLOAD,
  STREAM_COMPRESSED_OCEANBASE_PAYLOAD,
  STREAM_COMPRESSED_END,
  STREAM_OCEANBASE20_HEADER,
  STREAM_OCEANBASE20_EXTRA_INFO_HEADER,
  STREAM_OCEANBASE20_EXTRA_INFO,
  STREAM_OCEANBASE20_MYSQL_PAYLOAD,
  STREAM_OCEANBASE20_TAILER,
  STREAM_OCEANBASE20_END,
};
class ObRespAnalyzerUtil
{
public:
  static const char *get_stream_state_str(ObRespPktAnalyzerState state);
  static inline bool is_resultset_resp(ObMySQLCmd cmd, uint8_t first_pkt_type, ObServerStatusFlags status);
  static int analyze_one_compressed_ob20_packet(event::ObIOBufferReader &reader, ObAnalyzeHeaderResult &result);
  static inline int analyze_one_mysql_packet(event::ObIOBufferReader &reader, ObAnalyzeHeaderResult &result, ObMySQLCmd cmd);
  static int receive_next_ok_packet(event::ObIOBufferReader &reader, ObAnalyzeHeaderResult &result);
  static inline int stream_analyze_data(const char *src, const int64_t src_len, int64_t &analyzed,
                                        const char *dst, char *&target, uint32_t &to_analyze,
                                        const uint32_t target_len);
  static inline int stream_read_data(const char *src, const int64_t src_len, int64_t &read, uint32_t &to_read);
  static int do_new_extra_info_decode(const char *buf, const int64_t len,
                                      Ob20ExtraInfo &extra_info,
                                      common::FLTObjManage &flt_manage);
  static int do_obobj_extra_info_decode(const char *buf, const int64_t len,
                                        Ob20ExtraInfo &extra_info,
                                        common::FLTObjManage &flt_manage);
};
int ObRespAnalyzerUtil::analyze_one_mysql_packet(
    event::ObIOBufferReader &reader,
    ObAnalyzeHeaderResult &result,
    ObMySQLCmd cmd)
{
  int ret = common::OB_SUCCESS;
  ObMysqlAnalyzeResult tmp_result;
  if (OB_LIKELY(OB_MYSQL_COM_STATISTICS != cmd)) {
    ret = ObProxyParserUtils::analyze_one_packet(reader, tmp_result);
  } else {
    ret = ObProxyParserUtils::analyze_one_packet_only_header(reader, tmp_result);
  }
  result.status_ = tmp_result.status_;
  result.mysql_header_ = tmp_result.meta_;

  return ret;
}
bool ObRespAnalyzerUtil::is_resultset_resp(
    ObMySQLCmd cmd,
    uint8_t first_pkt_type,
    ObServerStatusFlags status)
{
  bool is_query_resultset = (OB_MYSQL_COM_QUERY == cmd || OB_MYSQL_COM_STMT_EXECUTE == cmd || OB_MYSQL_COM_STMT_FETCH == cmd);

  bool is_resultset_pkts = MYSQL_OK_PACKET_TYPE != first_pkt_type &&
                           MYSQL_ERR_PACKET_TYPE != first_pkt_type &&
                           MYSQL_EOF_PACKET_TYPE != first_pkt_type &&
                           MYSQL_LOCAL_INFILE_TYPE != first_pkt_type;

  bool is_need_transform_plugin = (OB_MYSQL_COM_STMT_PREPARE == cmd ||
                                   OB_MYSQL_COM_STMT_PREPARE_EXECUTE == cmd ||
                                   OB_MYSQL_COM_STMT_FETCH == cmd) &&
                                  MYSQL_ERR_PACKET_TYPE != first_pkt_type;

  bool is_more_results_exists = status.status_flags_.OB_SERVER_MORE_RESULTS_EXISTS;

  return (is_query_resultset && (is_more_results_exists || is_resultset_pkts)) ||
         is_need_transform_plugin ||
         OB_MYSQL_COM_FIELD_LIST == cmd;
}
int ObRespAnalyzerUtil::stream_read_data(
    const char *src, const int64_t src_len,
    int64_t &read, uint32_t &to_read)
{
  int ret = common::OB_SUCCESS;
  read = 0;
  if (OB_NOT_NULL(src)) {
    read = (src_len >= to_read) ? to_read : src_len;
    to_read -= static_cast<uint32_t>(read);
  } else {
    ret = common::OB_ERR_UNEXPECTED;
  }

  return ret;
}
/*
  read `target_len` bytes of data from `src[src_len]`
  and copy the read data to `dst[target_len]`
  and return the `target` ptr point to `dst` or `src`
*/
int ObRespAnalyzerUtil::stream_analyze_data(
    const char *src, const int64_t src_len, int64_t &analyzed,
    const char *dst, char *&target, uint32_t &to_analyze,
    const uint32_t target_len)
{
  int ret = common::OB_SUCCESS;
  analyzed = 0;
  if (OB_LIKELY(src != NULL && target_len >= to_analyze)) {
    target = NULL;
    analyzed = (src_len >= to_analyze) ? to_analyze : src_len;
    if (analyzed == target_len) {
      target = const_cast<char*>(src); // no to_analyze to MEMCPY
    } else {
      if (OB_LIKELY(dst != NULL)) {
        MEMCPY(const_cast<char*>(dst + (target_len - to_analyze)), src, analyzed);
        target = const_cast<char*>(dst);
      }
    }
    to_analyze -= static_cast<uint32_t>(analyzed);
  } else {
    ret = common::OB_ERR_UNEXPECTED;
  }

  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif