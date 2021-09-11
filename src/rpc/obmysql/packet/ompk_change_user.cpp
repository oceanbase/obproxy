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

#include "rpc/obmysql/packet/ompk_change_user.h"
#include "lib/charset/ob_charset.h"
#include "rpc/obmysql/ob_mysql_util.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;

OMPKChangeUser::OMPKChangeUser()
    : cmd_(OB_MYSQL_COM_CHANGE_USER),
      character_set_(CS_TYPE_UTF8MB4_BIN),
      mysql_cap_(),
      username_(),
      auth_response_(),
      auth_plugin_name_(),
      database_(),
      connect_attrs_(),
      sys_vars_(),
      user_vars_()
{}

void OMPKChangeUser::reset()
{
  mysql_cap_.capability_ = 0;
  character_set_ = 0;
  username_.reset();
  auth_response_.reset();
  database_.reset();
  auth_plugin_name_.reset();
  connect_attrs_.reset();
  sys_vars_.reset();
  user_vars_.reset();
}

// see com_change_user packet
// http://imysql.com/mysql-internal-manual/com-change-user.html
// for proxy, add session vars as connect attrs
int64_t OMPKChangeUser::get_serialize_size() const
{
  int64_t len = 0;
  len = 1;  // cmd
  len += username_.length() + 1;

  if (!!mysql_cap_.cap_flags_.OB_CLIENT_SECURE_CONNECTION) {
    len += 1;
    len += auth_response_.length();
  } else {
    len += auth_response_.length() + 1;
  }

  len += database_.length() + 1;
  len += 2;  // character set

  if (!!mysql_cap_.cap_flags_.OB_CLIENT_PLUGIN_AUTH) {
    len += auth_plugin_name_.length() + 1;
  }

  if (!!mysql_cap_.cap_flags_.OB_CLIENT_CONNECT_ATTRS) {
    len += ObMySQLUtil::get_number_store_len(get_connect_attrs_len());
    len += get_connect_attrs_len();
  }
  return len;
}

int OMPKChangeUser::decode()
{
  int ret = OB_SUCCESS;
  const char *buf = cdata_;
  const char *pos = cdata_;
  const int64_t len = hdr_.len_;
  const char *end = buf + len;

  if (NULL != cdata_) {
    // get username
    if (OB_LIKELY(pos < end)) {
      username_ = ObString::make_string(pos);
      pos += strlen(pos) + 1;
    }

    // get auth response
    if (OB_LIKELY(pos < end)) {
      if (mysql_cap_.cap_flags_.OB_CLIENT_SECURE_CONNECTION) {
        uint8_t auth_response_len = 0;
        ObMySQLUtil::get_uint1(pos, auth_response_len);
        auth_response_.assign_ptr(pos, static_cast<int32_t>(auth_response_len));
        pos += auth_response_len;
      } else {
        auth_response_.assign_ptr(pos, static_cast<int32_t>(STRLEN(pos)));
        pos += auth_response_.length() + 1;
      }
    }
    
    if (OB_LIKELY(pos < end)) {
      database_.assign_ptr(pos, static_cast<int32_t>(STRLEN(pos)));
      pos += database_.length() + 1;
    }

    if (OB_LIKELY(pos < end)) {
      ObMySQLUtil::get_uint2(pos, character_set_);
    }

    if (OB_LIKELY(pos < end)) {
      if (mysql_cap_.cap_flags_.OB_CLIENT_PLUGIN_AUTH) {
        auth_plugin_name_.assign_ptr(pos, static_cast<int32_t>(STRLEN(pos)));
        pos += auth_plugin_name_.length() + 1;
      }
    }

    if (OB_LIKELY(pos < end)) {
      if (mysql_cap_.cap_flags_.OB_CLIENT_CONNECT_ATTRS) {
        uint64_t all_attrs_len = 0;
        const char* attrs_end = NULL;
        if (OB_FAIL(ObMySQLUtil::get_length(pos, all_attrs_len))) {
          LOG_WARN("fail to get all_attrs_len", K(ret));
        } else {
          attrs_end = pos + all_attrs_len;
        }
        ObStringKV str_kv;
        while (OB_SUCC(ret) && OB_LIKELY(pos < attrs_end)) {
          if (OB_FAIL(decode_string_kv(attrs_end, pos, str_kv))) {
            OB_LOG(WARN, "fail to decode string kv", K(ret));
          } else {
            if (str_kv.key_ == OB_MYSQL_PROXY_SESSION_VARS) {
              const char* vars_start = str_kv.value_.ptr();
              if (OB_FAIL(decode_session_vars(vars_start, str_kv.value_.length()))) {
                OB_LOG(WARN, "fail to decode session vars", K(ret));
              }
            } else {
              // do not save it
            }
          }
        }
      }  // end connect attrs
    }    // end if
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("null input", K(cdata_));
  }
  return ret;
}

int OMPKChangeUser::serialize(char* buffer, const int64_t length, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buffer) || OB_UNLIKELY(length - pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(buffer), K(length), K(pos), K(ret));
  } else if (OB_UNLIKELY(length - pos < static_cast<int64_t>(get_serialize_size()))) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size is overflow", K(length), K(pos), "need_size", get_serialize_size(), K(ret));
  } else {
    if (OB_FAIL(ObMySQLUtil::store_int1(buffer, length, cmd_, pos))) {
      LOG_WARN("store fail", K(ret), K(buffer), K(length), K(pos));
    } else if (OB_FAIL(ObMySQLUtil::store_obstr_zt(buffer, length, username_, pos))) {
      LOG_WARN("store fail", K(ret), K(buffer), K(length), K(pos));
    } else {
      if (mysql_cap_.cap_flags_.OB_CLIENT_SECURE_CONNECTION) {
        if (OB_FAIL(ObMySQLUtil::store_int1(buffer, length, static_cast<uint8_t>(auth_response_.length()), pos))) {
          LOG_WARN("fail to store auth response length", K(ret));
        } else if (OB_FAIL(ObMySQLUtil::store_str_vnzt(
                       buffer, length, auth_response_.ptr(), auth_response_.length(), pos))) {
          LOG_WARN("fail to store auth response", K_(auth_response), K(ret));
        }
      } else {
        if (OB_FAIL(ObMySQLUtil::store_obstr_zt(buffer, length, auth_response_, pos))) {
          LOG_WARN("fail to store auth response", K_(auth_response), K(ret));
        }
      }
    }

    if (OB_SUCC(ret) && OB_FAIL(ObMySQLUtil::store_obstr_zt(buffer, length, database_, pos))) {
      LOG_WARN("fail to store database", K_(database), K(ret));
    }

    if (OB_SUCC(ret) && OB_FAIL(ObMySQLUtil::store_int2(buffer, length, character_set_, pos))) {
      LOG_WARN("fail to store charset", K_(character_set), K(ret));
    }

    if (OB_SUCC(ret)) {
      if (mysql_cap_.cap_flags_.OB_CLIENT_PLUGIN_AUTH) {
        if (OB_FAIL(ObMySQLUtil::store_obstr_zt(buffer, length, auth_plugin_name_, pos))) {
          LOG_WARN("fail to store auth_plugin_name", K_(auth_plugin_name), K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (mysql_cap_.cap_flags_.OB_CLIENT_CONNECT_ATTRS) {
        uint64_t all_attrs_len = get_connect_attrs_len();
        if (OB_FAIL(ObMySQLUtil::store_length(buffer, length, all_attrs_len, pos))) {
          LOG_WARN("fail to store all_attrs_len", K(all_attrs_len), K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < connect_attrs_.count(); ++i) {
            ret = ObMySQLPacket::store_string_kv(buffer, length, connect_attrs_.at(i), pos);
          }  // end store normal connect attrs

          if (OB_SUCC(ret)) {
            // store session vars
            if (sys_vars_.empty() && user_vars_.empty()) {
              // do nothing
            } else if (OB_FAIL(ObMySQLUtil::store_obstr(
                           buffer, length, ObString::make_string(OB_MYSQL_PROXY_SESSION_VARS), pos))) {
              LOG_WARN("store fail", K(ret), K(buffer), K(length), K(pos));
            } else if (OB_FAIL(serialize_session_vars(buffer, length, pos))) {
              LOG_WARN("fail to store session vars", K(ret), K(buffer), K(length), K(pos));
            }
          }  // end store session vars
        }
      }
    }
  }
  return ret;
}

uint64_t OMPKChangeUser::get_session_vars_len() const
{
  uint64_t session_vars_len = 0;
  for (int64_t i = 0; i < sys_vars_.count(); ++i) {
    session_vars_len += ObMySQLPacket::get_kv_encode_len(sys_vars_.at(i));
  }
  if (!user_vars_.empty()) {
    session_vars_len += ObMySQLPacket::get_kv_encode_len(ObMySQLPacket::get_separator_kv());
    for (int64_t i = 0; i < user_vars_.count(); ++i) {
      session_vars_len += ObMySQLPacket::get_kv_encode_len(user_vars_.at(i));
    }
  }
  return session_vars_len;
}

uint64_t OMPKChangeUser::get_connect_attrs_len() const
{
  uint64_t all_attr_len = 0;
  ObStringKV string_kv;
  if (!!mysql_cap_.cap_flags_.OB_CLIENT_CONNECT_ATTRS) {
    for (int64_t i = 0; i < connect_attrs_.count(); i++) {
      all_attr_len += ObMySQLPacket::get_kv_encode_len(connect_attrs_.at(i));
    }
    // store session vars as connect attrs
    if (!sys_vars_.empty() || !user_vars_.empty()) {
      int64_t len = STRLEN(OB_MYSQL_PROXY_SESSION_VARS);
      all_attr_len += ObMySQLUtil::get_number_store_len(len);
      all_attr_len += len;
      all_attr_len += ObMySQLUtil::get_number_store_len(get_session_vars_len());
      all_attr_len += get_session_vars_len();
    }
  }
  return all_attr_len;
}

int OMPKChangeUser::serialize_session_vars(char* buffer, const int64_t length, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMySQLUtil::store_length(buffer, length, get_session_vars_len(), pos))) {
    LOG_WARN("fail to store session vars len", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sys_vars_.count(); ++i) {
      ret = ObMySQLPacket::store_string_kv(buffer, length, sys_vars_.at(i), pos);
    }
    if (OB_SUCC(ret)) {
      if (!user_vars_.empty()) {
        ret = ObMySQLPacket::store_string_kv(buffer, length, ObMySQLPacket::get_separator_kv(), pos);
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < user_vars_.count(); ++i) {
        ret = ObMySQLPacket::store_string_kv(buffer, length, user_vars_.at(i), pos);
      }
    }
  }
  return ret;
}

int OMPKChangeUser::decode_string_kv(const char* attrs_end, const char*& pos, ObStringKV& kv)
{
  int ret = OB_SUCCESS;
  uint64_t key_len = 0;
  uint64_t value_len = 0;
  if (OB_ISNULL(pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalie input value", K(pos), K(ret));
  } else {
    if (OB_FAIL(ObMySQLUtil::get_length(pos, key_len))) {
      OB_LOG(WARN, "fail t get key len", K(pos), K(ret));
    } else {
      kv.key_.assign_ptr(pos, static_cast<uint32_t>(key_len));
      pos += key_len;
      if (pos >= attrs_end) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected key len", K(ret), K(key_len));
      } else if (OB_FAIL(ObMySQLUtil::get_length(pos, value_len))) {
        OB_LOG(WARN, "fail t get value len", K(pos), K(ret));
      } else {
        kv.value_.assign_ptr(pos, static_cast<uint32_t>(value_len));
        pos += value_len;
      }
    }
  }
  return ret;
}

int OMPKChangeUser::decode_session_vars(const char*& pos, const int64_t session_vars_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pos) || OB_UNLIKELY(session_vars_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalie input value", K(pos), K(session_vars_len), K(ret));
  } else {
    const char* end = pos + session_vars_len;
    bool found_separator = false;
    ObStringKV tmp_kv;
    while (OB_SUCC(ret) && OB_LIKELY(pos < end)) {
      if (OB_FAIL(decode_string_kv(end, pos, tmp_kv))) {
        OB_LOG(WARN, "fail to decode string kv", K(ret));
      } else {
        if (tmp_kv.key_ == ObMySQLPacket::get_separator_kv().key_ &&
            tmp_kv.value_ == ObMySQLPacket::get_separator_kv().value_) {
          found_separator = true;
          // continue
        } else {
          if (found_separator) {
            if (OB_FAIL(user_vars_.push_back(tmp_kv))) {
              OB_LOG(WARN, "fail to push back user_vars", K(tmp_kv), K(ret));
            }
          } else {
            if (OB_FAIL(sys_vars_.push_back(tmp_kv))) {
              OB_LOG(WARN, "fail to push back sys_vars", K(tmp_kv), K(ret));
            }
          }
        }
      }
    }  // end while
  }

  return ret;
}
