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

#include "rpc/obmysql/packet/ompk_error.h"
#include "rpc/obmysql/ob_mysql_util.h"

using namespace oceanbase::obmysql;

OMPKError::OMPKError() : field_count_(0xff)
{
  errcode_ = 2000;
  sqlstate_ = ObString::make_string("HY000");
  message_ = ObString::make_string("");
}

OMPKError::~OMPKError()
{
}

int OMPKError::serialize(char *buffer, int64_t len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  if (NULL == buffer || len - pos < static_cast<int64_t>(get_serialize_size())) {
    LOG_WDIAG("invalid argument", K(buffer), K(len), K(pos), "need_size", get_serialize_size());
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (OB_FAIL(ObMySQLUtil::store_int1(buffer, len, field_count_, pos))) {
      LOG_WDIAG("store fail", K(buffer), K(len), K(field_count_), K(ret));
    } else if (OB_FAIL(ObMySQLUtil::store_int2(buffer, len, errcode_, pos))) {
      LOG_WDIAG("store fail", K(buffer), K(len), K(errcode_), K(ret));
    } else if (OB_FAIL(ObMySQLUtil::store_int1(buffer, len, MARKER, pos))) {
      LOG_WDIAG("store fail", K(buffer), K(len), K(ret));
    } else if (OB_FAIL(ObMySQLUtil::store_obstr_nzt(buffer, len, sqlstate_, pos))) {
      LOG_WDIAG("store fail", K(buffer), K(len), K(ret));
    } else if (OB_FAIL(ObMySQLUtil::store_obstr_nzt(buffer, len, message_, pos))) {
      LOG_WDIAG("store fail", K(buffer), K(len), K(ret));
    }
  }
  return ret;
}

int64_t OMPKError::get_serialize_size() const
{
  int64_t len = 0;
  len += 9;   // 1byte field_count + 2bytes errno + 1byte sqlmarker + 5bytes sqlstate
  len += message_.length();
  return len;
}

int OMPKError::decode()
{
  int ret = OB_SUCCESS;
  const char *pos = cdata_;
  const int64_t len = hdr_.len_;
  const char *end = pos + len;

  if (OB_ISNULL(cdata_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_EDIAG("null input", K(cdata_), K(ret)); //  OB_ASSERT(NULL != cdata_);
  } else {
    ObMySQLUtil::get_uint1(pos, field_count_);
    ObMySQLUtil::get_uint2(pos, errcode_);
    pos += 1; // skip SQL State Marker '#'
    sqlstate_.assign_ptr(pos, 5);
    pos += 5;
    if (end >= pos) {
      //OB_ASSERT(end >= pos);
      message_.assign_ptr(pos, static_cast<uint32_t>(end - pos));
      pos += end - pos;
      //OB_ASSERT(pos == end);
      if (pos != end) {
        ret = OB_ERR_UNEXPECTED;
        LOG_EDIAG("packet pos not equal end", K(pos), K(end), K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("packet error", K(end), K(pos), K(ret));
    }
  }
  return ret;
}

int OMPKError::set_message(const ObString &message)
{
  int ret = OB_SUCCESS;
  if (NULL == message.ptr() || 0 > message.length()) {
    LOG_WDIAG("invalid argument message", K(message));
    ret = OB_INVALID_ARGUMENT;
  } else {
    message_.assign(const_cast<char *>(message.ptr()), message.length());
  }
  return ret;
}

int OMPKError::set_sqlstate(const char *sqlstate)
{
  int ret = common::OB_SUCCESS;
  if (SQLSTATE_SIZE == strlen(sqlstate)) {
    sqlstate_ = ObString::make_string(sqlstate);
  } else {
    ret = common::OB_INVALID_ARGUMENT;
  }
  return ret;
}

int OMPKError::set_oberrcode(int errcode)
{
  int mysql_errno = common::ob_mysql_errno(errcode);
  if (mysql_errno < 0) {
    mysql_errno = -errcode;
  }
  BACKTRACE(EDIAG, (0 == errcode || 0 == mysql_errno), "BUG!!!");
  errcode_ = static_cast<uint16_t>(mysql_errno);
  return set_sqlstate(common::ob_sqlstate(errcode));
}

int64_t OMPKError::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(K_(field_count),
       K_(errcode),
       K_(sqlstate),
       K_(message)
      );
  return pos;
}
