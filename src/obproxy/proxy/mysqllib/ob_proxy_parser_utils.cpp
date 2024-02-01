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

#define USING_LOG_PREFIX PROXY
#include "proxy/mysqllib/ob_proxy_parser_utils.h"
#include "rpc/obmysql/ob_mysql_util.h"
#include "iocore/eventsystem/ob_io_buffer.h"

using namespace oceanbase::obproxy::event;
using namespace oceanbase::obmysql;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
uint64_t ObProxyParserUtils::get_lenenc_int(const char *&buf)
{
  uint64_t ret_value = 0;
  uint64_t int_len = 0;
  int_len = uint1korr(buf);
  buf += 1;
  if (int_len < 0xfb) {
    ret_value = int_len;
  } else if (0xfc == int_len){
    ret_value = uint2korr(buf);
    buf += 2;
  } else if (0xfd == int_len){
    ret_value = uint3korr(buf);
    buf += 3;
  } else if(0xfe == int_len){
    ret_value = uint8korr(buf);
    buf += 8;
  }
  return ret_value;
}

const char *ObProxyParserUtils::get_sql_cmd_name(const ObMySQLCmd cmd)
{
  const char *name = NULL;
  switch (cmd) {
    case OB_MYSQL_COM_SLEEP:
      name = "OB_MYSQL_COM_SLEEP";
      break;

    case OB_MYSQL_COM_QUIT:
      name = "OB_MYSQL_COM_QUIT";
      break;

    case OB_MYSQL_COM_INIT_DB:
      name = "OB_MYSQL_COM_INIT_DB";
      break;

    case OB_MYSQL_COM_QUERY:
      name = "OB_MYSQL_COM_QUERY";
      break;

    case OB_MYSQL_COM_FIELD_LIST:
      name = "OB_MYSQL_COM_FIELD_LIST";
      break;

    case OB_MYSQL_COM_CREATE_DB:
      name = "OB_MYSQL_COM_CREATE_DB";
      break;

    case OB_MYSQL_COM_DROP_DB:
      name = "OB_MYSQL_COM_DROP_DB";
      break;

    case OB_MYSQL_COM_REFRESH:
      name = "OB_MYSQL_COM_REFRESH";
      break;

    case OB_MYSQL_COM_SHUTDOWN:
      name = "OB_MYSQL_COM_SHUTDOWN";
      break;

    case OB_MYSQL_COM_STATISTICS:
      name = "OB_MYSQL_COM_STATISTICS";
      break;

    case OB_MYSQL_COM_PROCESS_INFO:
      name = "OB_MYSQL_COM_PROCESS_INFO";
      break;

    case OB_MYSQL_COM_CONNECT:
      name = "OB_MYSQL_COM_CONNECT";
      break;

    case OB_MYSQL_COM_PROCESS_KILL:
      name = "OB_MYSQL_COM_PROCESS_KILL";
      break;

    case OB_MYSQL_COM_DEBUG:
      name = "OB_MYSQL_COM_DEBUG";
      break;

    case OB_MYSQL_COM_PING:
      name = "OB_MYSQL_COM_PING";
      break;

    case OB_MYSQL_COM_TIME:
      name = "OB_MYSQL_COM_TIME";
      break;

    case OB_MYSQL_COM_DELAYED_INSERT:
      name = "OB_MYSQL_COM_DELAYED_INSERT";
      break;

    case OB_MYSQL_COM_CHANGE_USER:
      name = "OB_MYSQL_COM_CHANGE_USER";
      break;

    case OB_MYSQL_COM_DAEMON:
      name = "OB_MYSQL_COM_DAEMON";
      break;

    case OB_MYSQL_COM_HANDSHAKE:
      name = "OB_MYSQL_COM_HANDSHAKE";
      break;

    case OB_MYSQL_COM_LOGIN:
      name = "OB_MYSQL_COM_LOGIN";
      break;

    case OB_MYSQL_COM_BINLOG_DUMP:
      name = "OB_MYSQL_COM_BINLOG_DUMP";
      break;

    case OB_MYSQL_COM_RESET_CONNECTION:
      name = "OB_MYSQL_COM_RESET_CONNECTION";
      break;

    case OB_MYSQL_COM_TABLE_DUMP:
      name = "OB_MYSQL_COM_TABLE_DUMP";
      break;

    case OB_MYSQL_COM_CONNECT_OUT:
      name = "OB_MYSQL_COM_CONNECT_OUT";
      break;

    case OB_MYSQL_COM_REGISTER_SLAVE:
      name = "OB_MYSQL_COM_REGISTER_SLAVE";
      break;

    case OB_MYSQL_COM_BINLOG_DUMP_GTID:
      name = "OB_MYSQL_COM_BINLOG_DUMP_GTID";
      break;

    case OB_MYSQL_COM_STMT_PREPARE:
      name = "OB_MYSQL_COM_STMT_PREPARE";
      break;

    case OB_MYSQL_COM_STMT_EXECUTE:
      name = "OB_MYSQL_COM_STMT_EXECUTE";
      break;

    case OB_MYSQL_COM_STMT_PREPARE_EXECUTE:
      name = "OB_MYSQL_COM_STMT_PREPARE_EXECUTE";
      break;

    case OB_MYSQL_COM_STMT_SEND_LONG_DATA:
      name = "OB_MYSQL_COM_STMT_SEND_LONG_DATA";
      break;

    case OB_MYSQL_COM_STMT_CLOSE:
      name = "OB_MYSQL_COM_STMT_CLOSE";
      break;

    case OB_MYSQL_COM_STMT_RESET:
      name = "OB_MYSQL_COM_STMT_RESET";
      break;

    case OB_MYSQL_COM_SET_OPTION:
      name = "OB_MYSQL_COM_SET_OPTION";
      break;

    case OB_MYSQL_COM_STMT_FETCH:
      name = "OB_MYSQL_COM_STMT_FETCH";
      break;

    case OB_MYSQL_COM_DELETE_SESSION:
      name = "OB_MYSQL_COM_DELETE_SESSION";
      break;

    case OB_MYSQL_COM_END:
      name = "OB_MYSQL_COM_END";
      break;

    case OB_MYSQL_COM_MAX_NUM:
      name = "OB_MYSQL_COM_MAX_NUM";
      break;

    case OB_MYSQL_COM_STMT_SEND_PIECE_DATA:
      name ="OB_MYSQL_COM_STMT_SEND_PIECE_DATA";
      break;

    case OB_MYSQL_COM_STMT_GET_PIECE_DATA:
      name = "OB_MYSQL_COM_STMT_GET_PIECE_DATA";
      break;

    case OB_MYSQL_COM_LOAD_DATA_TRANSFER_CONTENT:
      name = "OB_MYSQL_COM_LOAD_DATA_TRANSFER_CONTENT";
      break;

    case OB_MYSQL_COM_AUTH_SWITCH_RESP:
      name = "OB_MYSQL_COM_AUTH_SWITCH_RESP";
      break;

    default:
      name = "unknown sql command";
      break;
  }

  return name;
}

const char *ObProxyParserUtils::get_analyze_status_name(const ObMysqlAnalyzeStatus status)
{
  const char *name = NULL;
  switch (status) {
  case ANALYZE_OBPARSE_ERROR:
    name = "ANALYZE_OBPARSE_ERROR";
    break;

  case ANALYZE_ERROR:
    name = "ANALYZE_ERROR";
    break;

  case ANALYZE_DONE:
    name = "ANALYZE_DONE";
    break;

  case ANALYZE_OK:
    name = "ANALYZE_OK";
    break;

  case ANALYZE_CONT:
    name = "ANALYZE_CONT";
    break;

  default:
    name = "unkonwn analyze status name";
    break;
  }

  return name;
}

int ObProxyParserUtils::analyze_one_packet(ObIOBufferReader &reader, ObMysqlAnalyzeResult &result)
{
  int ret = OB_SUCCESS;
  int64_t len = reader.read_avail();

  result.status_ = ANALYZE_CONT;
  // just get mysql header when header cross two buffer block
  if (OB_LIKELY(len >= MYSQL_NET_META_LENGTH)) {
    int64_t block_len = reader.block_read_avail();
    char *buf_start = reader.start();
    char mysql_hdr[MYSQL_NET_META_LENGTH];

    if (OB_UNLIKELY(block_len < MYSQL_NET_META_LENGTH)) {
      char *written_pos = reader.copy(mysql_hdr, MYSQL_NET_META_LENGTH, 0);
      if (OB_UNLIKELY(written_pos != mysql_hdr + MYSQL_NET_META_LENGTH)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("not copy completely", K(written_pos), K(mysql_hdr),
                 "meta_length", MYSQL_NET_META_LENGTH, K(ret));
      } else {
        buf_start = mysql_hdr;
      }
    }

    if (OB_SUCC(ret)) {
      result.meta_.pkt_len_ = uint3korr(buf_start) + MYSQL_NET_HEADER_LENGTH;
      result.meta_.pkt_seq_ = uint1korr(buf_start + 3);
      result.meta_.data_ = static_cast<uint8_t>(buf_start[4]);
      if (OB_LIKELY(len >= result.meta_.pkt_len_)) {
        result.status_ = ANALYZE_DONE;
      }
    }
  }
  return ret;
}

int ObProxyParserUtils::analyze_one_packet_only_header(ObIOBufferReader &reader, ObMysqlAnalyzeResult &result)
{
  int ret = OB_SUCCESS;
  const int64_t len = reader.read_avail();

  result.status_ = ANALYZE_CONT;
  // just get mysql header when header cross two buffer block
  if (OB_LIKELY(len >= MYSQL_NET_HEADER_LENGTH)) {
    int64_t block_len = reader.block_read_avail();
    char *buf_start = reader.start();
    char mysql_hdr[MYSQL_NET_HEADER_LENGTH];

    if (OB_UNLIKELY(block_len < MYSQL_NET_HEADER_LENGTH)) {
      char *written_pos = reader.copy(mysql_hdr, MYSQL_NET_HEADER_LENGTH, 0);
      if (OB_UNLIKELY(written_pos != mysql_hdr + MYSQL_NET_HEADER_LENGTH)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("not copy completely", K(written_pos), K(mysql_hdr),
                 "meta_length", MYSQL_NET_HEADER_LENGTH, K(ret));
      } else {
        buf_start = mysql_hdr;
      }
    }

    if (OB_SUCC(ret)) {
      result.meta_.pkt_len_ = uint3korr(buf_start) + MYSQL_NET_HEADER_LENGTH; // include header
      result.meta_.pkt_seq_ = uint1korr(buf_start + 3);
      result.meta_.cmd_ = OB_MYSQL_COM_MAX_NUM; // only analyze header
      if (len >= result.meta_.pkt_len_) {
        result.status_ = ANALYZE_DONE;
      }
    }
  }
  return ret;
}

int ObProxyParserUtils::analyze_mysql_packet_meta(const char *ptr, const int64_t len, ObMysqlPacketMeta &meta)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ptr) || (OB_UNLIKELY(len < MYSQL_NET_META_LENGTH))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", KP(ptr), K(len), K(ret));
  } else {
    meta.pkt_len_ = uint3korr(ptr) + MYSQL_NET_HEADER_LENGTH;
    meta.pkt_seq_ = uint1korr(ptr + 3);
    meta.data_ = static_cast<uint8_t>(ptr[4]);
  }

  return ret;
}

int ObProxyParserUtils::analyze_mysql_packet_header(const char *ptr, const int64_t len, ObMySQLPacketHeader &header)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ptr) || (len < MYSQL_NET_HEADER_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", KP(ptr), K(len), K(ret));
  } else {
    header.len_ = uint3korr(ptr);
    header.seq_ = uint1korr(ptr + 3);
  }

  return ret;
}

bool ObProxyParserUtils::is_ok_packet(ObIOBufferReader &reader, ObMysqlAnalyzeResult &result)
{
  bool bret = false;
  if (OB_LIKELY(OB_SUCCESS == analyze_one_packet(reader, result))) {
    if (ANALYZE_DONE == result.status_) {
      if (MYSQL_OK_PACKET_TYPE == result.meta_.pkt_type_) {
        bret = true;
      }
    }
  }
  return bret;
}

bool ObProxyParserUtils::is_error_packet(ObIOBufferReader &reader, ObMysqlAnalyzeResult &result)
{
  bool bret = false;
  if (OB_LIKELY(OB_SUCCESS == analyze_one_packet(reader, result))) {
    if (ANALYZE_DONE == result.status_) {
      if (MYSQL_ERR_PACKET_TYPE == result.meta_.pkt_type_) {
        bret = true;
      }
    }
  }
  return bret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
