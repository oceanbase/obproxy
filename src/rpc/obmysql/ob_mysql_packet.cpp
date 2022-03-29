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

#define USING_LOG_PREFIX RPC_OBMYSQL

#include "rpc/obmysql/ob_mysql_packet.h"

#include "lib/utility/ob_macro_utils.h"
#include "rpc/obmysql/ob_mysql_util.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;

namespace oceanbase
{
namespace obmysql
{

int ObMySQLPacket::encode_packet(char *buf, int64_t &len, int64_t &pos, const ObMySQLPacket &pkt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || len <= 0 || pos < 0) {
    LOG_WARN("invalid buf or len", KP(buf), K(len), K(pos), K(ret));
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t seri_size = 0;
    if (OB_FAIL(pkt.encode(buf + pos, len, seri_size))) {
      LOG_WARN("serialize response packet fail", K(ret));
    } else {
      len -= seri_size;
      pos += seri_size;
    }
  }
  return ret;
}

int ObMySQLPacket::encode(char *buffer, int64_t length, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buffer) || 0 >= length || pos < 0) {
    LOG_WARN("invalid argument", KP(buffer), K(length), K(pos));
    ret = OB_INVALID_ARGUMENT;
  } else {
    const int64_t orig_pos = pos;
    pos += OB_MYSQL_HEADER_LENGTH;

    if (OB_FAIL(serialize(buffer, length, pos))) {
      LOG_WARN("encode packet data failed", K(ret));
    } else {
      int32_t payload = static_cast<int32_t>(pos - orig_pos - OB_MYSQL_HEADER_LENGTH);
      pos = orig_pos;
      if (OB_FAIL(ObMySQLUtil::store_int3(buffer, length, payload, pos))) {
        LOG_ERROR("failed to encode int", K(ret)); // OB_ASSERT(false);
      } else if (OB_FAIL(ObMySQLUtil::store_int1(buffer, length, hdr_.seq_, pos))) {
        LOG_ERROR("failed to encode int", K(ret)); // OB_ASSERT(false);
      } else {
        pos += payload;
      }
    }

    if (OB_FAIL(ret)) {
      pos = orig_pos;
    }
  }
  return ret;
}

int64_t ObMySQLPacket::get_serialize_size() const
{
  BACKTRACE(ERROR, 1, "not a serializiable packet");
  return -1;
}

int ObMySQLPacket::store_string_kv(char* buf, int64_t len, const ObStringKV& str, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMySQLUtil::store_obstr(buf, len, str.key_, pos))) {
    LOG_WARN("store stringkv key fail", K(ret));
  } else if (OB_FAIL(ObMySQLUtil::store_obstr(buf, len, str.value_, pos))) {
    LOG_WARN("store stringkv value fail", K(ret));
  }
  return ret;
}

uint64_t ObMySQLPacket::get_kv_encode_len(const ObStringKV& string_kv)
{
  uint64_t len = 0;
  len += ObMySQLUtil::get_number_store_len(string_kv.key_.length());
  len += string_kv.key_.length();
  len += ObMySQLUtil::get_number_store_len(string_kv.value_.length());
  len += string_kv.value_.length();
  return len;
}

ObStringKV ObMySQLPacket::get_separator_kv()
{
  static ObStringKV separator_kv;
  separator_kv.key_ = common::ObString::make_string("__NULL");
  separator_kv.value_ = common::ObString::make_string("__NULL");
  return separator_kv;
}

int64_t ObMySQLRawPacket::get_serialize_size() const
{
  return static_cast<int64_t>(get_clen()) + 1; // add 1 for cmd_
}

// serialize content in string<EOF> by default
int ObMySQLRawPacket::serialize(char *buf, const int64_t length, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || length <= 0 || pos < 0
                  || length - pos < get_serialize_size() || NULL == cdata_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(buf), K(length), K(get_serialize_size()), K(pos), K(ret));
  } else if (OB_FAIL(ObMySQLUtil::store_int1(buf, length, cmd_, pos))) {
    LOG_WARN("fail to store cmd", K(length), K(cmd_), K(pos), K(ret));
  } else if (OB_FAIL(ObMySQLUtil::store_str_vnzt(buf, length, get_cdata(), get_clen(), pos))) {
    LOG_WARN("fail to store content", K(length), K(get_cdata()), K(get_clen()), K(pos), K(ret));
  }
  return ret;
}

int ObMySQLRawPacket::encode_packet_meta(char *buf, int64_t &len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  // healder len + cmd len = 4 + 1 = 5
  if (OB_UNLIKELY(NULL == buf || len <= 0 || pos < 0 || len - pos < 5)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(buf), K(len), K(pos), K(ret));
  } else {
    int32_t payload = 1 + hdr_.len_; // payload = cmd + request = 1 + hdr_.len
    if (OB_FAIL(ObMySQLUtil::store_int3(buf, len, payload, pos))) {
      LOG_ERROR("failed to encode int", K(ret));
    } else if (OB_FAIL(ObMySQLUtil::store_int1(buf, len, hdr_.seq_, pos))) {
      LOG_ERROR("failed to encode int", K(ret));
    } else if (OB_FAIL(ObMySQLUtil::store_int1(buf, len, cmd_, pos))) {
      LOG_WARN("fail to store cmd", K(len), K(cmd_), K(pos), K(ret));
    }
  }

  return ret;
}

char const *get_info_func_name(const ObInformationFunctions func)
{
  const char *str = NULL;
  static const char *func_name_array[MAX_INFO_FUNC] =
  {
    "benchmark",
    "charset",
    "coercibility",
    "coliation",
    "connection_id",
    "current_user",
    "database",
    "found_rows",
    "last_insert_id",
    "row_count",
    "schema",
    "session_user",
    "system user",
    "user",
    "version",
  };

  if (func >= BENCHMARK_FUNC && func < MAX_INFO_FUNC) {
    str = func_name_array[func];
  }
  return str;
}

char const *get_mysql_cmd_str(ObMySQLCmd mysql_cmd)
{
  const char *str = "invalid";
  static const char *mysql_cmd_array[OB_MYSQL_COM_MAX_NUM] =
  {
    "Sleep",  // OB_MYSQL_COM_SLEEP,
    "Quit",  // OB_MYSQL_COM_QUIT,
    "Init DB",  // OB_MYSQL_COM_INIT_DB,
    "Query",  // OB_MYSQL_COM_QUERY,
    "Field List",  // OB_MYSQL_COM_FIELD_LIST,

    "Create DB",  // OB_MYSQL_COM_CREATE_DB,
    "Drop DB",  // OB_MYSQL_COM_DROP_DB,
    "Refresh",  // OB_MYSQL_COM_REFRESH,
    "Shutdown",  // OB_MYSQL_COM_SHUTDOWN,
    "Statistics",  // OB_MYSQL_COM_STATISTICS,

    "Process Info",  // OB_MYSQL_COM_PROCESS_INFO,
    "Connect",  // OB_MYSQL_COM_CONNECT,
    "Process Kill",  // OB_MYSQL_COM_PROCESS_KILL,
    "Debug",  // OB_MYSQL_COM_DEBUG,
    "Ping",  // OB_MYSQL_COM_PING,

    "Time",  // OB_MYSQL_COM_TIME,
    "Delayed insert",  // OB_MYSQL_COM_DELAYED_INSERT,
    "Change user",  // OB_MYSQL_COM_CHANGE_USER,
    "Binlog dump",  // OB_MYSQL_COM_BINLOG_DUMP,

    "Table dump",  // OB_MYSQL_COM_TABLE_DUMP,
    "Connect out",  // OB_MYSQL_COM_CONNECT_OUT,
    "Register slave",  // OB_MYSQL_COM_REGISTER_SLAVE,

    "Prepare",  // OB_MYSQL_COM_STMT_PREPARE,
    "Execute",  // OB_MYSQL_COM_STMT_EXECUTE,
    "Stmt send long data",  // OB_MYSQL_COM_STMT_SEND_LONG_DATA,
    "Close stmt",  // OB_MYSQL_COM_STMT_CLOSE,

    "Stmt reset",  // OB_MYSQL_COM_STMT_RESET,
    "Set option",  // OB_MYSQL_COM_SET_OPTION,
    "Stmt fetch",  // OB_MYSQL_COM_STMT_FETCH,
    "Daemno",  // OB_MYSQL_COM_DAEMON,

    "Binlog dump gtid",  // OB_MYSQL_COM_BINLOG_DUMP_GTID,
    // "Reset connection",  // OB_MYSQL_COM_RESET_CONNECTION,
    "Prepare Execute", // OB_MYSQL_COM_STMT_PREPARE_EXECUTE,
    "SEND PIECE DATA",
    "GET PIECE DATA",
    "End",  // OB_MYSQL_COM_END,

    "Delete session", // OB_MYSQL_COM_DELETE_SESSION
    "Handshake",  // OB_MYSQL_COM_HANDSHAKE,
    "Login"  // OB_MYSQL_COM_LOGIN,
  };

  if (mysql_cmd >= OB_MYSQL_COM_SLEEP && mysql_cmd <= OB_MYSQL_COM_BINLOG_DUMP_GTID) {
    str = mysql_cmd_array[mysql_cmd];
  } else if (mysql_cmd >= OB_MYSQL_COM_STMT_PREPARE_EXECUTE && mysql_cmd <= OB_MYSQL_COM_END) {
    int start = OB_MYSQL_COM_BINLOG_DUMP_GTID + 1;
    str = mysql_cmd_array[mysql_cmd - OBPROXY_NEW_MYSQL_CMD_START + start];
  } else if (mysql_cmd > OB_MYSQL_COM_END && mysql_cmd < OB_MYSQL_COM_MAX_NUM) {
    int start = OB_MYSQL_COM_BINLOG_DUMP_GTID + 1 + OB_MYSQL_COM_END - OB_MYSQL_COM_STMT_PREPARE_EXECUTE + 1;
    str = mysql_cmd_array[mysql_cmd - OBPROXY_MYSQL_CMD_START + start];
  }
  return str;

}

} // end of namespace obmysql
} // end of namespace oceanbase
