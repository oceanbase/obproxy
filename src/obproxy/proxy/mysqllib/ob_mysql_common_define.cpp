/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */


#include "proxy/mysqllib/ob_mysql_common_define.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
const common::ObString get_proxy_protocol_string(enum ObProxyProtocol protocol)
{
  common::ObString ret;
  switch (protocol) {
    case ObProxyProtocol::PROTOCOL_MYSQL:
      ret = SERVER_PROTOCOL_MYSQL;
      break;

    case ObProxyProtocol::PROTOCOL_COMPRESSED_MYSQL:
      ret = SERVER_PROTOCOL_COMPRESSED_MYSQL;
      break;

    case ObProxyProtocol::PROTOCOL_OCEANBASE_20:
      ret = SERVER_PROTOCOL_OCEANBASE_20;
      break;

    default:
      ret = "Unknown";
      break;
  }

  return ret;
}

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
    case obmysql::OB_MYSQL_COM_RESET_CONNECTION:
    // Prepared Statements(Binary Protocol)
    case obmysql::OB_MYSQL_COM_STMT_PREPARE:
    case obmysql::OB_MYSQL_COM_STMT_EXECUTE:
    case obmysql::OB_MYSQL_COM_STMT_PREPARE_EXECUTE:
    case obmysql::OB_MYSQL_COM_STMT_SEND_LONG_DATA:
    case obmysql::OB_MYSQL_COM_STMT_CLOSE:
    case obmysql::OB_MYSQL_COM_STMT_RESET:
    case obmysql::OB_MYSQL_COM_STMT_FETCH:
    case obmysql::OB_MYSQL_COM_CHANGE_USER:
    // binlog related
    case obmysql::OB_MYSQL_COM_REGISTER_SLAVE:
    case obmysql::OB_MYSQL_COM_BINLOG_DUMP:
    case obmysql::OB_MYSQL_COM_BINLOG_DUMP_GTID:
    case obmysql::OB_MYSQL_COM_CDC_DUMP:
    // pieceinfo
    case obmysql::OB_MYSQL_COM_STMT_SEND_PIECE_DATA:
    case obmysql::OB_MYSQL_COM_STMT_GET_PIECE_DATA:
    case obmysql::OB_MYSQL_COM_SET_OPTION:
    case obmysql::OB_MYSQL_COM_LOAD_DATA_TRANSFER_CONTENT:
    case obmysql::OB_MYSQL_COM_AUTH_SWITCH_RESP:
    case obmysql::OB_MYSQL_COM_AUTH_MORE_DATA_RESP:
      ret = true;
      break;
    // Replication Protocol
    case obmysql::OB_MYSQL_COM_TABLE_DUMP:
    case obmysql::OB_MYSQL_COM_CONNECT_OUT:
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