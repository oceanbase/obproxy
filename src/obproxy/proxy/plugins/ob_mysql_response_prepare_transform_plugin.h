/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
  uint32_t pkt_count_;

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
        PROXY_API_LOG(EDIAG, "fail to allocate memory for ObMysqlResponsePrepareTransformPlugin");
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
                  "mysql_cmd", ObProxyParserUtils::get_sql_cmd_name(sm->trans_state_.trans_info_.sql_cmd_));
    bool bret = (!sm->trans_state_.trans_info_.client_request_.is_internal_cmd()
                 && ObMysqlTransact::SERVER_SEND_REQUEST == sm->trans_state_.current_.send_action_
                 && obmysql::OB_MYSQL_COM_STMT_PREPARE == sm->trans_state_.trans_info_.sql_cmd_);

    if (bret && OB_NOT_NULL(sm->protocol_diagnosis_)) {
      sm->protocol_diagnosis_->record_resp_forward_ctrl_flow(ObRespForwardCtrlFlow::PLUGIN_PREPARE_WORK);
      PROTOCOL_FORWARD_LOG(TRACE, "plugin_prepare work");
    }

    return bret;
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
