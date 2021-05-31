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

#ifndef OBPROXY_KILL_GLOBAL_SESSION_HANDLER_H
#define OBPROXY_KILL_GLOBAL_SESSION_HANDLER_H
#include "cmd/ob_internal_cmd_handler.h"
#include "proxy/mysql/ob_mysql_client_session.h"
namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObKillGlobalSessionHandler : public ObInternalCmdHandler
{
public:
  ObKillGlobalSessionHandler(event::ObContinuation *cont, event::ObMIOBuffer *buf,
                        const ObInternalCmdInfo &info);
  virtual ~ObKillGlobalSessionHandler() {}
  int handle_kill_global_session_info(int event, void *data);
  int handle_kill_session_info();
protected:
  int encode_err_packet(const int errcode);
private:
  int handle_kill_session_by_ssid(const ObString& dbkey, int64_t ssid);
  ObProxyBasicStmtSubType sub_type_;
  const ObInternalCmdInfo* cmd_info_;
  const obmysql::ObMySQLCapabilityFlags capability_;
  #define ERR_MSG_BUF_SIZE 1024
  char err_msg_[ERR_MSG_BUF_SIZE];
  DISALLOW_COPY_AND_ASSIGN(ObKillGlobalSessionHandler);
};
int kill_global_session_info_cmd_init();

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_KILL_GLOBAL_SESSION_HANDLER_H