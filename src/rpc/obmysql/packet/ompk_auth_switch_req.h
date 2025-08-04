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

#ifndef _OMPK_AUTH_SWITCH_REQ_H_
#define _OMPK_AUTH_SWITCH_REQ_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "rpc/obmysql/ob_mysql_util.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "rpc/obmysql/packet/ompk_handshake.h"

namespace oceanbase
{
namespace obmysql
{
class OMPKAuthSwitchReq
    : public ObMySQLPacket
{
public:
  OMPKAuthSwitchReq() {};
  virtual ~OMPKAuthSwitchReq() {};
  inline const ObString& get_auth_plugin_name() const { return auth_plugin_name_; }
  inline const ObString& get_auth_plugin_data() const { return auth_plugin_data_; }
  int decode();

private:
  DISALLOW_COPY_AND_ASSIGN(OMPKAuthSwitchReq);
  const static int8_t AUTH_SWITCH_REQ_STATUS_FLAG = 0xFE;
  ObString auth_plugin_name_;
  ObString auth_plugin_data_;

}; // end of class OMPKAuthSwitchReq
} // end of namespace obmysql
} // end of namespace oceanbase

#endif