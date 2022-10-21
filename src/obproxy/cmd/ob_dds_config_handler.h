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

#ifndef OBPROXY_DDS_CONFIG_HANDLER_H
#define OBPROXY_DDS_CONFIG_HANDLER_H

#include "cmd/ob_internal_cmd_handler.h"

typedef struct _ObExprParseResult ObExprParseResult;
namespace oceanbase
{
namespace common
{
class ObArenaAllocator;
class ObConfigItem;
}
namespace obproxy
{
namespace proxy {
class ObMysqlSM;
}
namespace obutils
{
class ObDdsConfigHandler : public ObInternalCmdHandler
{
public:
  ObDdsConfigHandler(event::ObContinuation *cont, event::ObMIOBuffer *buf, const ObInternalCmdInfo &info);

  virtual ~ObDdsConfigHandler() {}
  int main_handler(int event, void *data);
private:
  int handle_show_variables();
  int handle_set_variables();
  int handle_select_variables();
  int handle_select_session_variables();
  int handle_one_select_session_variable(const char* key, const char* val);
  int handle_select_sql_variables();
  int handle_update_variables();
  int dump_select_result(const common::ObString& app_name,
                         const common::ObString& app_block);

  void get_params_from_parse_result(const ObExprParseResult& expr_result,
                                    common::ObString& app_name,
                                    common::ObString& app_version,
                                    common::ObString& config_val);

  int handle_parse_where_fields(common::ObArenaAllocator* allocator,
                                ObExprParseResult& expr_result,
                                common::ObCollationType connection_collation);
  int update_dds_config_to_processor(const common::ObString& app_name,
                                     const common::ObString& app_version,
                                     const common::ObString& app_block,
                                     const common::ObString& config_val);

private:
  oceanbase::obproxy::proxy::ObMysqlSM* sm_;
  const ObProxyBasicStmtSubType sub_type_;
  const ObProxyBasicStmtType cmd_type_;
  const obmysql::ObMySQLCapabilityFlags capability_;

  DISALLOW_COPY_AND_ASSIGN(ObDdsConfigHandler);
};

int dds_config_cmd_init();
} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_DDS_CONFIG_HANDLER_H */

