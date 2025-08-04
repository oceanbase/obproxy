/**
 * Copyright (c) 2025 OceanBase
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

#include "rpc/obmysql/packet/ompk_auth_switch_req.h"
#include "rpc/obmysql/packet/ompk_handshake.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;

int OMPKAuthSwitchReq::decode()
{
  int ret = OB_SUCCESS;
  const char *buf = cdata_;
  const char *pos = cdata_;
  const int64_t len = hdr_.len_;
  const char *end = buf + len;
  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_EDIAG("null input", KP(buf), K(len), K(ret));
  } else {
    uint8_t status_flag = 0;
    ObMySQLUtil::get_uint1(pos, status_flag);
    if (status_flag != 0xFE) {
      ret = OB_INVALID_ARGUMENT;
      LOG_EDIAG("not auth switch request", K(status_flag));
    } else {
      int64_t auth_plugin_name_len = strlen(pos);  // NULL ternamite
      auth_plugin_name_.assign_ptr(pos, auth_plugin_name_len);
      pos += (auth_plugin_name_len + 1);
      auth_plugin_data_.assign_ptr(pos, end - pos + 1);
    }
  }

  return ret;
}