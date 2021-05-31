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

#ifndef OBPROXY_KILL_OP_HANDLER_H
#define OBPROXY_KILL_OP_HANDLER_H

#include "cmd/ob_internal_cmd_handler.h"
#include "proxy/mysql/ob_mysql_client_session.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObKillOpHandler : public ObInternalCmdHandler
{
public:
  ObKillOpHandler(event::ObContinuation *cont, event::ObMIOBuffer *buf,
                  const ObInternalCmdInfo &info);
  virtual ~ObKillOpHandler() { cs_id_array_.destroy(); }
  int handle_kill_option(int event, void *data);
  virtual CSIDHanders *get_cs_id_array() { return &cs_id_array_; }

private:
  int kill_session(ObMysqlClientSession &cs);
  int get_server_session(const ObMysqlClientSession &cs, const ObMysqlServerSession *&server_session);

  bool need_privilege_check() const { return OBPROXY_T_SUB_KILL_CONNECTION == sub_type_; }

public:
  ObProxySessionPrivInfo session_priv_;
  share::schema::ObNeedPriv need_priv_;

private:
  const ObProxyBasicStmtSubType sub_type_;
  const int64_t ss_id_;//server session id
  const obmysql::ObMySQLCapabilityFlags capability_;
  CSIDHanders cs_id_array_;

  DISALLOW_COPY_AND_ASSIGN(ObKillOpHandler);
};

int kill_op_cmd_init();

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_KILL_OP_HANDLER_H */

