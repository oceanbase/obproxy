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

OMPKChangeUser::OMPKChangeUser() : status_(0xfe),
    username_(),
    auth_plugin_data_(),
    auth_plugin_name_(),
    database_(),
    character_set_(CS_TYPE_UTF8MB4_BIN)
{
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
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("null input", K(cdata_));
  }
  return ret;
}

int64_t OMPKChangeUser::get_serialize_size() const
{
  int64_t len = 0;
  len = 1; // [fe]
  len += auth_plugin_name_.length() + 1;
  len += auth_plugin_data_.length();
  return len;
}

int OMPKChangeUser::serialize(char *buffer, const int64_t length, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buffer) || OB_UNLIKELY(length - pos < static_cast<int64_t>(get_serialize_size()))) {
    LOG_WARN("invalid argument", K(buffer), K(length), K(pos), "need_size", get_serialize_size());
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (OB_FAIL(ObMySQLUtil::store_int1(buffer, length, status_, pos))) {
      LOG_WARN("store fail", K(ret), K(buffer), K(length), K(pos));
    } else if (OB_FAIL(ObMySQLUtil::store_obstr_zt(buffer, length, auth_plugin_name_, pos))) {
      LOG_WARN("store fail", K(ret), K(buffer), K(length), K(pos));
    } else {
      if (!auth_plugin_data_.empty()) {
        MEMCPY(buffer + pos, auth_plugin_data_.ptr(), auth_plugin_data_.length());
        pos += auth_plugin_data_.length();
      }
    }
  }
  return ret;
}

