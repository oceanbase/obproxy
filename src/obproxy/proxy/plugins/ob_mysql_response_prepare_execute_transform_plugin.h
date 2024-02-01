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

#ifndef OBPROXY_MYSQL_RESPONSE_PREPARE_EXECUTE_TRANSFORM_PLUGIN_H
#define OBPROXY_MYSQL_RESPONSE_PREPARE_EXECUTE_TRANSFORM_PLUGIN_H

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

enum EnumPrepareExecuteStateType
{
  PREPARE_EXECUTE_HEADER,
  PREPARE_EXECUTE_PARAM,
  PREPARE_EXECUTE_COLUMN,
  PREPARE_EXECUTE_ROW,
  PREPARE_EXECUTE_END
};

class ObMysqlResponsePrepareExecuteTransformPlugin : public ObTransformationPlugin
{
public:
  static ObMysqlResponsePrepareExecuteTransformPlugin *alloc(ObApiTransaction &transaction);

  explicit ObMysqlResponsePrepareExecuteTransformPlugin(ObApiTransaction &transaction);

  virtual void destroy();

  // this func can not consume the reader, super class will do it
  virtual int consume(event::ObIOBufferReader *reader);

  virtual void handle_input_complete();

private:
  void reset();
  int handle_prepare_execute_ok(event::ObIOBufferReader *reader);
  int handle_prepare_param();
  int handle_prepare_column(event::ObIOBufferReader *reader);
  int handle_prepare_execute_eof(event::ObIOBufferReader *reader);

private:
  event::ObIOBufferReader *local_reader_;
  event::ObIOBufferReader *local_analyze_reader_;
  packet::ObMysqlPacketReader pkt_reader_;
  EnumPrepareExecuteStateType prepare_execute_state_;
  uint16_t num_columns_;
  uint16_t num_params_;
  uint16_t pkt_count_;
  bool hava_cursor_;
  common::ObArray<obmysql::EMySQLFieldType> field_types_;

  DISALLOW_COPY_AND_ASSIGN(ObMysqlResponsePrepareExecuteTransformPlugin);
};

class ObMysqlResponsePrepareExecuteGlobalPlugin : public ObGlobalPlugin
{
public:
  static ObMysqlResponsePrepareExecuteGlobalPlugin *alloc()
  {
    return op_reclaim_alloc(ObMysqlResponsePrepareExecuteGlobalPlugin);
  }

  ObMysqlResponsePrepareExecuteGlobalPlugin()
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
      plugin = ObMysqlResponsePrepareExecuteTransformPlugin::alloc(transaction);
      if (NULL != plugin) {
        transaction.add_plugin(plugin);
        PROXY_API_LOG(DEBUG, "add ObMysqlResponsePrepareExecuteTransformPlugin", K(plugin));
      } else {
        PROXY_API_LOG(EDIAG, "fail to allocate memory for ObMysqlResponsePrepareExecuteTransformPlugin");
      }
    } else {
      PROXY_API_LOG(DEBUG, "no need setup ObMysqlResponsePrepareExecuteTransformPlugin");
    }

    transaction.resume();
  }

  inline bool need_enable_plugin(ObMysqlSM *sm) const
  {
    return (!sm->trans_state_.trans_info_.client_request_.is_internal_cmd()
            && ObMysqlTransact::SERVER_SEND_REQUEST == sm->trans_state_.current_.send_action_
            && obmysql::OB_MYSQL_COM_STMT_PREPARE_EXECUTE == sm->trans_state_.trans_info_.sql_cmd_
            && sm->trans_state_.trans_info_.server_response_.get_analyze_result().is_resultset_resp());
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObMysqlResponsePrepareExecuteGlobalPlugin);
};

void init_mysql_response_prepare_execute_transform()
{
  PROXY_API_LOG(INFO, "init mysql response prepare execute transformation plugin");
  ObMysqlResponsePrepareExecuteGlobalPlugin *prepare_execute_transform = ObMysqlResponsePrepareExecuteGlobalPlugin::alloc();
  UNUSED(prepare_execute_transform);
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_MYSQL_RESPONSE_PREPARE_EXECUTE_TRANSFORM_PLUGIN_H
