/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_ALTER_CONFIG_SET_HANDLER_H
#define OBPROXY_ALTER_CONFIG_SET_HANDLER_H

#include "cmd/ob_internal_cmd_handler.h"
#include "rpc/obmysql/ob_mysql_packet.h"

#define CLIENT_SESSION_ID_V1_PROXY_ID_LIMIT 255  // max proxy_id using CLIENT_SESSION_ID_V1
#define CLIENT_SESSION_ID_V2_PROXY_ID_LIMIT 8191 // max proxy_id using CLIENT_SESSION_ID_V2

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
class ObAlterConfigSetHandler : public ObInternalCmdHandler
{
public:
  ObAlterConfigSetHandler(event::ObContinuation *cont, event::ObMIOBuffer *buf, const ObInternalCmdInfo &info);
  virtual ~ObAlterConfigSetHandler() {}

  int handle_set_config(int event, void *data);

private:
  const obmysql::ObMySQLCapabilityFlags capability_;

  char key_str_[common::OB_MAX_CONFIG_NAME_LEN + 1];// for alter xx set
  char value_str_[common::OB_MAX_CONFIG_VALUE_LEN + 1];// for alter xx set

  DISALLOW_COPY_AND_ASSIGN(ObAlterConfigSetHandler);
};

int alter_config_set_cmd_init();
} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_ALTER_CONFIG_SET_HANDLER_H */

