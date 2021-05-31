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

#ifndef OBPROXY_MYSQL_REQUEST_PREPARE_TRANSFORM_PLUGIN_H
#define OBPROXY_MYSQL_REQUEST_PREPARE_TRANSFORM_PLUGIN_H

#include "proxy/api/ob_global_plugin.h"
#include "proxy/api/ob_transformation_plugin.h"
#include "proxy/mysqllib/ob_proxy_session_info_handler.h"
#include "proxy/mysql/ob_mysql_client_session.h"
#include "proxy/mysql/ob_mysql_sm.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

class ObMysqlRequestPrepareTransformPlugin : public ObTransformationPlugin
{
public:
  static ObMysqlRequestPrepareTransformPlugin *alloc(ObApiTransaction &transaction, int64_t ps_pkt_len);

  ObMysqlRequestPrepareTransformPlugin(ObApiTransaction &transaction, int64_t ps_pkt_len);

  virtual void destroy();

  // this func can not consume the reader, super class will do it
  virtual int consume(event::ObIOBufferReader *reader);

  virtual void handle_input_complete();

private:
  int copy_ps_or_text_ps_sql(int64_t copy_size);
  int handle_ps_prepare();
  int handle_text_ps_prepare();
  int check_last_data_segment(event::ObIOBufferReader &reader, bool &is_last_segment);

private:

  int64_t ps_pkt_len_;
  int64_t copy_offset_;
  char *ps_or_text_ps_sql_buf_;
  event::ObIOBufferReader *local_reader_;
  DISALLOW_COPY_AND_ASSIGN(ObMysqlRequestPrepareTransformPlugin);
};

class ObMysqlRequestPrepareGlobalPlugin : public ObGlobalPlugin
{
public:
  static ObMysqlRequestPrepareGlobalPlugin *alloc()
  {
    return op_reclaim_alloc(ObMysqlRequestPrepareGlobalPlugin);
  }

  ObMysqlRequestPrepareGlobalPlugin()
  {
    register_hook(HOOK_READ_REQUEST);
  }

  virtual void destroy()
  {
    ObGlobalPlugin::destroy();
    op_reclaim_free(this);
  }

  virtual void handle_read_request(ObApiTransaction &transaction)
  {
    ObTransactionPlugin *plugin = NULL;

    if (need_enable_plugin(transaction.get_sm())) {
      int64_t req_pkt_len = transaction.get_sm()->trans_state_.trans_info_.request_content_length_;
      plugin = ObMysqlRequestPrepareTransformPlugin::alloc(transaction, req_pkt_len);
      if (NULL != plugin) {
        transaction.add_plugin(plugin);
        PROXY_API_LOG(DEBUG, "add ObMysqlRequestPrepareTransformPlugin", K(plugin), K(req_pkt_len));
      } else {
        PROXY_API_LOG(ERROR, "fail to allocate memory for ObMysqlRequestPrepareTransformPlugin");
      }
    } else {
      PROXY_API_LOG(DEBUG, "handle_read_request, no need setup ObMysqlRequestPrepareTransformPlugin");
    }

    transaction.resume();
  }

  inline bool need_enable_plugin(ObMysqlSM *sm) const
  {
    PROXY_API_LOG(DEBUG, "need_enable_plugin",
                  "request_content_length", sm->trans_state_.trans_info_.request_content_length_,
                  "mysql_cmd", get_mysql_cmd_str(sm->trans_state_.trans_info_.sql_cmd_));
    return (!sm->trans_state_.trans_info_.client_request_.is_internal_cmd()
            && sm->trans_state_.trans_info_.request_content_length_ > 0
            && (obmysql::OB_MYSQL_COM_STMT_PREPARE == sm->trans_state_.trans_info_.sql_cmd_
              || sm->trans_state_.trans_info_.client_request_.get_parse_result().is_text_ps_prepare_stmt()));
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObMysqlRequestPrepareGlobalPlugin);
};

void init_mysql_request_prepare_transform()
{
  PROXY_API_LOG(INFO, "init mysql request prepare transformation plugin");
  ObMysqlRequestPrepareGlobalPlugin *prepare_transform = ObMysqlRequestPrepareGlobalPlugin::alloc();
  UNUSED(prepare_transform);
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_MYSQL_REQUEST_PREPARE_TRANSFORM_PLUGIN_H
