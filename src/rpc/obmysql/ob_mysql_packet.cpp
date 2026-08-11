/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
    LOG_WDIAG("invalid buf or len", KP(buf), K(len), K(pos), K(ret));
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t seri_size = 0;
    if (OB_FAIL(pkt.encode(buf + pos, len, seri_size))) {
      LOG_WDIAG("serialize response packet fail", K(ret));
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
    LOG_WDIAG("invalid argument", KP(buffer), K(length), K(pos));
    ret = OB_INVALID_ARGUMENT;
  } else {
    const int64_t orig_pos = pos;
    pos += OB_MYSQL_HEADER_LENGTH;

    if (OB_FAIL(serialize(buffer, length, pos))) {
      LOG_WDIAG("encode packet data failed", K(ret));
    } else {
      int32_t payload = static_cast<int32_t>(pos - orig_pos - OB_MYSQL_HEADER_LENGTH);
      pos = orig_pos;
      if (OB_FAIL(ObMySQLUtil::store_int3(buffer, length, payload, pos))) {
        LOG_EDIAG("failed to encode int", K(ret)); // OB_ASSERT(false);
      } else if (OB_FAIL(ObMySQLUtil::store_int1(buffer, length, hdr_.seq_, pos))) {
        LOG_EDIAG("failed to encode int", K(ret)); // OB_ASSERT(false);
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
  BACKTRACE(EDIAG, 1, "not a serializiable packet");
  return -1;
}

int ObMySQLPacket::store_string_kv(char* buf, int64_t len, const ObStringKV& str, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMySQLUtil::store_obstr(buf, len, str.key_, pos))) {
    LOG_WDIAG("store stringkv key fail", K(ret));
  } else if (OB_FAIL(ObMySQLUtil::store_obstr(buf, len, str.value_, pos))) {
    LOG_WDIAG("store stringkv value fail", K(ret));
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
  if (OB_UNLIKELY(NULL == buf || length <= 0 || pos < 0 || length - pos < get_serialize_size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", KP(buf), K(length), K(get_serialize_size()), K(pos), K(ret));
  } else if (OB_FAIL(ObMySQLUtil::store_int1(buf, length, cmd_, pos))) {
    LOG_WDIAG("fail to store cmd", K(length), K(cmd_), K(pos), K(ret));
  } else if (get_serialize_size() != 1  && cdata_ == NULL) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(get_serialize_size()), KP(cdata_));
  } else if (OB_FAIL(ObMySQLUtil::store_str_vnzt(buf, length, get_cdata(), get_clen(), pos))) {
    LOG_WDIAG("fail to store content", K(length), K(get_cdata()), K(get_clen()), K(pos), K(ret));
  }
  return ret;
}

int ObMySQLRawPacket::encode_packet_meta(char *buf, int64_t &len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  // healder len + cmd len = 4 + 1 = 5
  if (OB_UNLIKELY(NULL == buf || len <= 0 || pos < 0 || len - pos < 5)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", KP(buf), K(len), K(pos), K(ret));
  } else {
    int32_t payload = 1 + hdr_.len_; // payload = cmd + request = 1 + hdr_.len
    if (OB_FAIL(ObMySQLUtil::store_int3(buf, len, payload, pos))) {
      LOG_EDIAG("failed to encode int", K(ret));
    } else if (OB_FAIL(ObMySQLUtil::store_int1(buf, len, hdr_.seq_, pos))) {
      LOG_EDIAG("failed to encode int", K(ret));
    } else if (OB_FAIL(ObMySQLUtil::store_int1(buf, len, cmd_, pos))) {
      LOG_WDIAG("fail to store cmd", K(len), K(cmd_), K(pos), K(ret));
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

} // end of namespace obmysql
} // end of namespace oceanbase
