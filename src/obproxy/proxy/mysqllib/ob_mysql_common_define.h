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

#ifndef OBPROXY_MYSQL_COMMON_DEFINE_H
#define OBPROXY_MYSQL_COMMON_DEFINE_H

#include <stdint.h>
#include "rpc/obmysql/ob_mysql_packet.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

static const uint32_t MYSQL_NET_HEADER_LENGTH             = 4;      // standard header size
static const uint32_t MYSQL_PAYLOAD_LENGTH_LENGTH         = 3;  // standard payload length size
static const int64_t MYSQL_COMP_HEADER_LENGTH             = 3;      // compression header extra size
static const int64_t MYSQL_NET_TYPE_LENGTH                = 1;         // packet type size
static const int64_t MYSQL_PS_EXECUTE_HEADER_LENGTH       = 9; // ps packet header size: stmt_id + flag + iteration-count
// mysql meta info include mysql header and mysql request type
static const int64_t MYSQL_NET_META_LENGTH                = MYSQL_NET_TYPE_LENGTH + MYSQL_NET_HEADER_LENGTH;

// compressed packet
// The header looks like:
//
//  3   length of compressed payload
//  1   compressed sequence id
//  3   length of payload before compression
static const int64_t MYSQL_COMPRESSED_HEALDER_LENGTH      = 7;

static const int64_t MYSQL_PACKET_MAX_LENGTH              = 0xFFFFFF;
static const int64_t MYSQL_PAYLOAD_MAX_LENGTH             = (MYSQL_PACKET_MAX_LENGTH - 1);
static const int64_t MYSQL_SHORT_PACKET_MAX_LENGTH        = 2048; //for ok, eof
//for hello pkt, the first pkt send by observer or mysql
static const int64_t MYSQL_HELLO_PKT_MAX_LEN              = 1024;
static const int64_t MYSQL_OK_PACKET_TYPE                 = 0x00;
static const int64_t MYSQL_ERR_PACKET_TYPE                = 0xFF;
//the EOF packet may appear in places where a Protocol::LengthEncodedInteger
//may appear. You must check whether the packet length is less than 9 to
//make sure that it is a EOF packet.
static const int64_t MYSQL_EOF_PACKET_TYPE                = 0xFE;
static const int64_t MYSQL_LOCAL_INFILE_TYPE              = 0xFB;
static const int64_t MYSQL_HANDSHAKE_PACKET_TYPE          = 0x0A;

//-----------------------------------oceanbase 2.0 c/s protocol----------------------//
static const uint16_t OB20_PROTOCOL_MAGIC_NUM             = 0x20AB;
static const int64_t OB20_PROTOCOL_HEADER_LENGTH          = 24;
static const int32_t OB20_PROTOCOL_TAILER_LENGTH          = 4;  // for CRC32
static const int64_t OB20_PROTOCOL_HEADER_TAILER_LENGTH   = OB20_PROTOCOL_HEADER_LENGTH + OB20_PROTOCOL_TAILER_LENGTH;
static const int32_t OB20_PROTOCOL_EXTRA_INFO_LENGTH      = 4;  // for the length of extra info
static const int16_t OB20_PROTOCOL_VERSION_VALUE          = 20;
static const int64_t MYSQL_COMPRESSED_OB20_HEALDER_LENGTH = MYSQL_COMPRESSED_HEALDER_LENGTH + OB20_PROTOCOL_HEADER_LENGTH;

// one of those types indicates that one mysql cmd response is finished
enum ObMysqlRespEndingType
{
  OK_PACKET_ENDING_TYPE = 0,
  EOF_PACKET_ENDING_TYPE,
  ERROR_PACKET_ENDING_TYPE,
  HANDSHAKE_PACKET_ENDING_TYPE, // OB_MYSQL_COM_HANDSHAKE packet
  STRING_EOF_ENDING_TYPE,       // OB_MYSQL_COM_STATISTICS response
  LOCAL_INFILE_ENDING_TYPE,     // LOCAL INFILE Request
  PREPARE_OK_PACKET_ENDING_TYPE, // OB_MYSQL_COM_STMT_PREPARE packet
  MAX_PACKET_ENDING_TYPE
};

static const int64_t OB_MYSQL_RESP_ENDING_TYPE_COUNT = (MAX_PACKET_ENDING_TYPE + 1);

//this is used for OB_MYSQL_COM_QUERY, OB_MYSQL_COM_PROCESS_INFO
enum ObMysqlRespType
{
  RESULT_SET_RESP_TYPE = 0, // for result set
  LOCAL_INFILE_RESP_TYPE,   // for local infile, not supported now
  OTHERS_RESP_TYPE,         // for OK or EOF or ERROR or HANDSHAKE
  MAX_RESP_TYPE
};

enum ObRespTransState
{
  IN_TRANS_STATE_BY_DEFAULT = 0,
  IN_TRANS_STATE_BY_PARSE,
  NOT_IN_TRANS_STATE_BY_PARSE
};

struct ObMysqlPacketMeta
{
  uint32_t pkt_len_;
  uint8_t pkt_seq_;
  union
  {
    obmysql::ObMySQLCmd cmd_; // mysql cmd for mysql request
    uint8_t pkt_type_; // packet type for mysql response
    uint8_t data_;
  };

  ObMysqlPacketMeta() : pkt_len_(0), pkt_seq_(0), data_(0) { }
  ~ObMysqlPacketMeta() { }
  void reset()
  {
    MEMSET(this, 0, sizeof(ObMysqlPacketMeta));
  }

  TO_STRING_KV(K_(pkt_len), K_(pkt_seq), K_(cmd), K_(pkt_type));
};

struct ObMysqlCompressedPacketHeader
{
  uint32_t compressed_len_;
  uint8_t seq_;
  uint32_t non_compressed_len_;

  ObMysqlCompressedPacketHeader() : compressed_len_(0), seq_(0), non_compressed_len_(0) {}
  ~ObMysqlCompressedPacketHeader() {}
  //according to mysql protocol, if non_compressed_len_=0, the payload is not compressed
  bool is_compressed_payload() const { return (0 != non_compressed_len_); }
  void reset()
  {
    MEMSET(this, 0, sizeof(ObMysqlCompressedPacketHeader));
  }
  TO_STRING_KV(K_(compressed_len), K_(seq), K_(non_compressed_len));
};

bool is_supported_mysql_cmd(const obmysql::ObMySQLCmd mysql_cmd)
{
  bool ret = false;
  switch (mysql_cmd) {
    case obmysql::OB_MYSQL_COM_QUERY:
    case obmysql::OB_MYSQL_COM_HANDSHAKE:
    case obmysql::OB_MYSQL_COM_LOGIN:
    case obmysql::OB_MYSQL_COM_PING:
    case obmysql::OB_MYSQL_COM_INIT_DB:
    case obmysql::OB_MYSQL_COM_QUIT:
    case obmysql::OB_MYSQL_COM_DELETE_SESSION:
    case obmysql::OB_MYSQL_COM_SLEEP:
    case obmysql::OB_MYSQL_COM_FIELD_LIST:
    case obmysql::OB_MYSQL_COM_CREATE_DB:
    case obmysql::OB_MYSQL_COM_DROP_DB:
    case obmysql::OB_MYSQL_COM_REFRESH:
    case obmysql::OB_MYSQL_COM_SHUTDOWN:
    case obmysql::OB_MYSQL_COM_STATISTICS:
    case obmysql::OB_MYSQL_COM_PROCESS_INFO:
    case obmysql::OB_MYSQL_COM_CONNECT:
    case obmysql::OB_MYSQL_COM_PROCESS_KILL:
    case obmysql::OB_MYSQL_COM_DEBUG:
    case obmysql::OB_MYSQL_COM_TIME:
    case obmysql::OB_MYSQL_COM_DELAYED_INSERT:
    case obmysql::OB_MYSQL_COM_DAEMON:
    // Prepared Statements(Binary Protocol)
    case obmysql::OB_MYSQL_COM_STMT_PREPARE:
    case obmysql::OB_MYSQL_COM_STMT_EXECUTE:
    case obmysql::OB_MYSQL_COM_STMT_PREPARE_EXECUTE:
    case obmysql::OB_MYSQL_COM_STMT_SEND_LONG_DATA:
    case obmysql::OB_MYSQL_COM_STMT_CLOSE:
    case obmysql::OB_MYSQL_COM_STMT_RESET:
    // Stored Procedures
    case obmysql::OB_MYSQL_COM_STMT_FETCH:
    // pieceinfo
    case obmysql::OB_MYSQL_COM_STMT_SEND_PIECE_DATA:
    case obmysql::OB_MYSQL_COM_STMT_GET_PIECE_DATA:
      ret = true;
      break;
    case obmysql::OB_MYSQL_COM_CHANGE_USER:
    // Replication Protocol
    case obmysql::OB_MYSQL_COM_BINLOG_DUMP:
    case obmysql::OB_MYSQL_COM_TABLE_DUMP:
    case obmysql::OB_MYSQL_COM_CONNECT_OUT:
    case obmysql::OB_MYSQL_COM_REGISTER_SLAVE:
    case obmysql::OB_MYSQL_COM_BINLOG_DUMP_GTID:
    // Stored Procedures
    case obmysql::OB_MYSQL_COM_SET_OPTION:
      ret = false;
      break;
    default:
      break;
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_MYSQL_COMMON_DEFINE_H */
