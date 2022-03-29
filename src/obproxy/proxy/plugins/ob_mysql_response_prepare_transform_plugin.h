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

#ifndef OBPROXY_MYSQL_RESPONSE_PREPARE_TRANSFORM_PLUGIN_H
#define OBPROXY_MYSQL_RESPONSE_PREPARE_TRANSFORM_PLUGIN_H

#include "proxy/api/ob_global_plugin.h"
#include "proxy/api/ob_transformation_plugin.h"
#include "proxy/mysql/ob_mysql_sm.h"
#include "packet/ob_mysql_packet_reader.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

enum EnumPrepareStateType
{
  PREPARE_OK,
  PREPARE_PARAM,
  PREPARE_COLUMN,
  PREPARE_END
};

class ObMysqlResponsePrepareTransformPlugin : public ObTransformationPlugin
{
public:
  static ObMysqlResponsePrepareTransformPlugin *alloc(ObApiTransaction &transaction);

  explicit ObMysqlResponsePrepareTransformPlugin(ObApiTransaction &transaction);

  virtual void destroy();

  // this func can not consume the reader, super class will do it
  virtual int consume(event::ObIOBufferReader *reader);

  virtual void handle_input_complete();

private:
  int handle_prepare_ok(event::ObIOBufferReader *reader);
  int handle_prepare_param(event::ObIOBufferReader *reader);
  int handle_prepare_column();

private:
  event::ObIOBufferReader *local_reader_;
  event::ObIOBufferReader *local_analyze_reader_;
  packet::ObMysqlPacketReader pkt_reader_;
  EnumPrepareStateType prepare_state_;
  uint16_t num_columns_;
  uint16_t num_params_;
  uint16_t pkt_count_;

  DISALLOW_COPY_AND_ASSIGN(ObMysqlResponsePrepareTransformPlugin);
};

class ObMysqlResponsePrepareGlobalPlugin : public ObGlobalPlugin
{
public:
  static ObMysqlResponsePrepareGlobalPlugin *alloc()
  {
    return op_reclaim_alloc(ObMysqlResponsePrepareGlobalPlugin);
  }

  ObMysqlResponsePrepareGlobalPlugin()
  {
    register_hook(HOOK_READ_RESPONSE);
  }

  virtual void destroy()
  {
    ObGlobalPlugin::destroy();
    op_reclaim_free(this);
  }

  virtual void handle_read_response(ObApiTransaction &transaction)
  {
    ObTransactionPlugin *plugin = NULL;

    if (need_enable_plugin(transaction.get_sm())) {
      plugin = ObMysqlResponsePrepareTransformPlugin::alloc(transaction);
      if (NULL != plugin) {
        transaction.add_plugin(plugin);
        PROXY_API_LOG(DEBUG, "add ObMysqlResponsePrepareTransformPlugin", K(plugin));
      } else {
        PROXY_API_LOG(ERROR, "fail to allocate memory for ObMysqlResponsePrepareTransformPlugin");
      }
    } else {
      PROXY_API_LOG(DEBUG, "no need setup ObMysqlResponsePrepareTransformPlugin");
    }

    transaction.resume();
  }

  inline bool need_enable_plugin(ObMysqlSM *sm) const
  {
    PROXY_API_LOG(DEBUG, "need_enable_plugin",
                  "send action", sm->trans_state_.current_.send_action_,
                  "mysql_cmd", get_mysql_cmd_str(sm->trans_state_.trans_info_.sql_cmd_));

    return (!sm->trans_state_.trans_info_.client_request_.is_internal_cmd()
            && ObMysqlTransact::SERVER_SEND_REQUEST == sm->trans_state_.current_.send_action_
            && obmysql::OB_MYSQL_COM_STMT_PREPARE == sm->trans_state_.trans_info_.sql_cmd_);
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObMysqlResponsePrepareGlobalPlugin);
};

void init_mysql_response_prepare_transform()
{
  PROXY_API_LOG(INFO, "init mysql response prepare transformation plugin");
  ObMysqlResponsePrepareGlobalPlugin *prepare_transform = ObMysqlResponsePrepareGlobalPlugin::alloc();
  UNUSED(prepare_transform);
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_MYSQL_RESPONSE_PREPARE_TRANSFORM_PLUGIN_H
