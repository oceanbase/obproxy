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

#ifndef OBPROXY_SHOW_CONFIG_HANDLER_H
#define OBPROXY_SHOW_CONFIG_HANDLER_H

#include "cmd/ob_internal_cmd_handler.h"

namespace oceanbase
{
namespace common
{
class ObConfigItem;
}
namespace obproxy
{
namespace obutils
{
class ObShowConfigHandler : public ObInternalCmdHandler
{
public:
  ObShowConfigHandler(event::ObContinuation *cont, event::ObMIOBuffer *buf,
                      const ObInternalCmdInfo &info);

  virtual ~ObShowConfigHandler() {}
  int handle_show_config(int event, void *data);

private:
  int dump_config_header();
  int dump_config_item(const common::ObConfigItem &item);
  int dump_json_config_version(const common::ObString &version);
  int dump_json_config_bin_url(const common::ObString &url);
  int dump_json_config_db(const common::ObString &db);
  int dump_json_config_username(const common::ObString &username);
  int dump_json_config_password(const common::ObString &password);
  int dump_json_config_real_cluster_name(const common::ObString &cluster_name);
  int dump_json_config_cluster_count(const int64_t count);
  int dump_json_config_gmt_modified(const int64_t time);

  int dump_syslog_level();
  int dump_syslog_level_header();
  int dump_syslog_level_item(const char *level_str, const char *par_mod_name,
                             const char *sub_mod_name = NULL);

private:
  const ObProxyBasicStmtSubType sub_type_;

  DISALLOW_COPY_AND_ASSIGN(ObShowConfigHandler);
};

int show_config_cmd_init();
} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_SHOW_CONFIG_HANDLER_H */

