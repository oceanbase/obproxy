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

#ifndef OBPROXY_MYSQL_RESPONSE_CALL_TRANSFORM_PLUGIN_H
#define OBPROXY_MYSQL_RESPONSE_CALL_TRANSFORM_PLUGIN_H

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

enum RESULTSET_STATE
{
  RESULTSET_HEADER,
  RESULTSET_FIELD,
  RESULTSET_EOF_FIRST,
  RESULTSET_ROW
};

class ObMysqlResponseCursorTransformPlugin : public ObTransformationPlugin
{
public:
  static ObMysqlResponseCursorTransformPlugin *alloc(ObApiTransaction &transaction);

  explicit ObMysqlResponseCursorTransformPlugin(ObApiTransaction &transaction);

  virtual void destroy();

  // this func can not consume the reader, super class will do it
  virtual int consume(event::ObIOBufferReader *reader);

  virtual void handle_input_complete();

  static int handle_resultset_row(event::ObIOBufferReader *reader, ObMysqlSM *sm,
                                  common::ObArray<obmysql::EMySQLFieldType> field_types,
                                  bool hava_cursor, uint64_t column_num);
  static int add_cursor_id_pair(ObMysqlServerSession *server_session, uint32_t client_cursor_id, uint32_t server_cursor_id);
  static int add_cursor_id_addr(ObMysqlClientSession *client_session, uint32_t client_cursor_id, const sockaddr &addr);
  static int skip_field_value(const char *&data, int64_t &buf_len, obmysql::EMySQLFieldType field_type);

private:
  void reset();

  int handle_resultset_header(event::ObIOBufferReader *reader);
  int handle_resultset_field(event::ObIOBufferReader *reader);

private:
  event::ObIOBufferReader *local_reader_;
  event::ObIOBufferReader *local_analyze_reader_;
  packet::ObMysqlPacketReader pkt_reader_;
  RESULTSET_STATE resultset_state_;
  uint64_t column_num_;
  uint64_t pkt_count_;
  bool hava_cursor_;
  common::ObArray<obmysql::EMySQLFieldType> field_types_;

  DISALLOW_COPY_AND_ASSIGN(ObMysqlResponseCursorTransformPlugin);
};

class ObMysqlResponseCursorGlobalPlugin : public ObGlobalPlugin
{
public:
  static ObMysqlResponseCursorGlobalPlugin *alloc()
  {
    return op_reclaim_alloc(ObMysqlResponseCursorGlobalPlugin);
  }

  ObMysqlResponseCursorGlobalPlugin()
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
      plugin = ObMysqlResponseCursorTransformPlugin::alloc(transaction);
      if (NULL != plugin) {
        transaction.add_plugin(plugin);
        PROXY_API_LOG(DEBUG, "add ObMysqlResponseCursorTransformPlugin", K(plugin));
      } else {
        PROXY_API_LOG(EDIAG, "fail to allocate memory for ObMysqlResponseCursorTransformPlugin");
      }
    } else {
      PROXY_API_LOG(DEBUG, "no need setup ObMysqlResponseCursorTransformPlugin");
    }

    transaction.resume();
  }

  inline bool need_enable_plugin(ObMysqlSM *sm) const
  {
    return (!sm->trans_state_.trans_info_.client_request_.is_internal_cmd()
            && ObMysqlTransact::SERVER_SEND_REQUEST == sm->trans_state_.current_.send_action_
            && obmysql::OB_MYSQL_COM_STMT_EXECUTE == sm->trans_state_.trans_info_.sql_cmd_
            && !sm->trans_state_.trans_info_.client_request_.get_parse_result().is_dml_stmt()
            && sm->trans_state_.trans_info_.server_response_.get_analyze_result().is_resultset_resp());
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObMysqlResponseCursorGlobalPlugin);
};

void init_mysql_response_cursor_transform()
{
  PROXY_API_LOG(INFO, "init mysql response cursor transformation plugin");
  ObMysqlResponseCursorGlobalPlugin *cursor_transform = ObMysqlResponseCursorGlobalPlugin::alloc();
  UNUSED(cursor_transform);
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_MYSQL_RESPONSE_CALL_TRANSFORM_PLUGIN_H
